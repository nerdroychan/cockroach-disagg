// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package cloud

import (
	"context"
	"io"
	"os"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/util/ioctx"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/sstable"
)

const minReadSize = 32 << 10 // 8kb

// ExternalSSTReader returns opens an SST in external storage, and returns
// a vfs.File-like wrapper.
func ExternalSSTReader(
	ctx context.Context,
	e ExternalStorage,
	basename string,
) (sstable.ReadableFile, error) {
	// Do an initial read of the file, from the beginning, to get the file size as
	// this is used e.g. to read the trailer.
	var f ioctx.ReadCloserCtx
	var sz int64

	const maxAttempts = 3
	if err := retry.WithMaxAttempts(ctx, base.DefaultRetryOptions(), maxAttempts, func() error {
		var err error
		f, sz, err = e.ReadFileAt(ctx, basename, 0)
		if err == nil {
			_ = f.Close(ctx)
		}
		return err
	}); err != nil {
		return nil, err
	}

	raw := &sstReader{
		ctx: ctx,
		sz:  sizeStat(sz),
		openAt: func(offset int64) (ioctx.ReadCloserCtx, error) {
			var reader ioctx.ReadCloserCtx
			err := retry.WithMaxAttempts(ctx, base.DefaultRetryOptions(), maxAttempts, func() error {
				var err error
				reader, _, err = e.ReadFileAt(ctx, basename, offset)
				return err
			})
			if err != nil {
				return nil, err
			}
			return reader, nil
		},
	}
	raw.cache.sem = make(chan struct{}, 1)
	raw.cache.buf = make([]byte, 0, minReadSize)

	var reader sstable.ReadableFile = raw

	// We only explicitly buffer the suffix of the file when not decrypting as
	// the decrypting reader has its own internal block buffer.
	if err := raw.readAndCacheSuffix(64 << 10); err != nil {
		return nil, err
	}

	return reader, nil
}

type sstReader struct {
	ctx    context.Context
	sz     sizeStat
	openAt func(int64) (ioctx.ReadCloserCtx, error)

	readPos int64 // readPos is used to transform Read() to ReadAt(readPos).

	// This wrapper's primary purpose is reading SSTs which often perform many
	// tiny reads in a cluster of offsets near the end of the file. If we can read
	// the whole region once and fullfil those from a cache, we can avoid repeated
	// RPCs.
	cache struct {
		sem    chan struct{}
		pos    int64
		buf    []byte
		closed bool
	}
}

// Close implements io.Closer.
func (r *sstReader) Close() error {
	r.readPos = 0
	r.cache.sem <- struct{}{}
	r.cache.closed = true
	<-r.cache.sem
	return nil
}

// Stat returns the size of the file.
func (r *sstReader) Stat() (os.FileInfo, error) {
	return r.sz, nil
}

// Read implements io.Reader.
//
// Note: Read is _not_ concurrent-safe, unlike ReadAt.
func (r *sstReader) Read(p []byte) (int, error) {
	n, err := r.ReadAt(p, r.readPos)
	r.readPos += int64(n)
	return n, err
}

// readAndCacheSuffix caches the `size` suffix of the file (which could the
// whole file) for use by later ReadAt calls to avoid making additional RPCs.
func (r *sstReader) readAndCacheSuffix(size int64) error {
	if size == 0 {
		return nil
	}
	bufSize := size
	if bufSize < minReadSize {
		bufSize = minReadSize
	}
	r.cache.buf = make([]byte, 0, bufSize)
	scratch := r.cache.buf[:size]
	r.cache.pos = int64(r.sz) - size
	if r.cache.pos <= 0 {
		r.cache.pos = 0
	}
	reader, err := r.openAt(r.cache.pos)
	if err != nil {
		return err
	}
	defer reader.Close(r.ctx)
	read := 0
	for n := 0; read < len(scratch); n, err = reader.Read(r.ctx, scratch[read:]) {
		read += n
		if err != nil {
			break
		}
	}
	if err != nil && err != io.EOF {
		return err
	}
	r.cache.buf = scratch[:read]
	return nil
}

func (r *sstReader) cacheTheRest(b ioctx.ReadCloserCtx, offset int64, read int) {
	defer func() {
		<-r.cache.sem
	}()
	defer b.Close(r.ctx)

	if r.cache.closed {
		return
	}

	if offset >= r.cache.pos && offset < r.cache.pos+int64(len(r.cache.buf)) {
		// We _could_ have read from the cache, but we likely didn't at the time
		// for some reason; either the channel was full, or some other reader
		// came around and filled the cache with this exact same block. Either way,
		// don't evict the cache.
		return
	}

	r.cache.pos = offset + int64(read)
	r.cache.buf = r.cache.buf[:0]
	excessBytesToRead := minReadSize - read
	if cap(r.cache.buf) < excessBytesToRead {
		r.cache.buf = make([]byte, 0, minReadSize)
	}

	var err error
	scratch := r.cache.buf[:excessBytesToRead]
	read2 := 0
	for n := 0; read2 < len(scratch); n, err = b.Read(r.ctx, scratch[read2:]) {
		read2 += n
		if err != nil {
			break
		}
	}
	err = nil
	r.cache.buf = scratch[:read2]
}

func (r *sstReader) readFromBody(p []byte, b ioctx.ReadCloserCtx, offset int64) (read int, err error, leaveBodyOpen bool) {
	for n := 0; read < len(p); n, err = b.Read(r.ctx, p[read:]) {
		read += n
		if err != nil {
			break
		}
	}
	// If we got an EOF after we had read enough, ignore it.
	if read == len(p) && err == io.EOF {
		return read, nil, false
	}
	if read >= minReadSize || err != nil {
		// No need to cache.
		return read, err, false
	}

	select {
	case r.cache.sem <- struct{}{}:
	default:
		return read, err, false
	}

	if r.cache.closed {
		<-r.cache.sem
		return read, err, false
	}

	go r.cacheTheRest(b, offset, read)

	return read, err, true
}

// ReadAt implements io.ReaderAt by opening a Reader at an offset before reading
// from it.
func (r *sstReader) ReadAt(p []byte, offset int64) (int, error) {
	var read int

	select {
	case r.cache.sem <- struct{}{}:
		if offset >= r.cache.pos && offset < r.cache.pos+int64(len(r.cache.buf)) {
			read += copy(p, r.cache.buf[offset-r.cache.pos:])
			if read == len(p) {
				<-r.cache.sem
				return read, nil
			}
			// Advance offset to end of what cache read.
			offset += int64(read)
		}
		<-r.cache.sem
	case <-time.After(3 * time.Millisecond):
	}

	if offset == int64(r.sz) {
		return read, io.EOF
	}

	//// Position the underlying reader at offset if needed.
	//if r.pos != offset {
	//	if err := r.Close(); err != nil {
	//		return 0, err
	//	}
	//	b, err := r.openAt(offset)
	//	if err != nil {
	//		return 0, err
	//	}
	//	r.pos = offset
	//	r.body = b
	//}

	b, err := r.openAt(offset)
	if err != nil {
		return read, err
	}

	var leaveBodyOpen bool
	var read2 int
	read2, err, leaveBodyOpen = r.readFromBody(p[read:], b, offset)
	read += read2
	if !leaveBodyOpen {
		_ = b.Close(r.ctx)
	}

	return read, err
}

type sizeStat int64

func (s sizeStat) Size() int64      { return int64(s) }
func (sizeStat) IsDir() bool        { panic(errors.AssertionFailedf("unimplemented")) }
func (sizeStat) ModTime() time.Time { panic(errors.AssertionFailedf("unimplemented")) }
func (sizeStat) Mode() os.FileMode  { panic(errors.AssertionFailedf("unimplemented")) }
func (sizeStat) Name() string       { panic(errors.AssertionFailedf("unimplemented")) }
func (sizeStat) Sys() interface{}   { panic(errors.AssertionFailedf("unimplemented")) }
