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
	"bufio"
	"context"
	"io"
	"io/fs"
	"os"
	"path"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/vfs"
)

type vfsAdapter struct {
	ExternalStorage

	ctx context.Context
	basePath string
}

type externalStorageFile struct {
	reader      sstable.ReadableFile
	writer      *bufio.Writer
	writeCloser io.WriteCloser
	v *vfsAdapter
	name string
	offset int64
}

func (e *externalStorageFile) Close() error {
	var err error
	if e.reader != nil {
		err = e.reader.Close()
		if err != nil {

			return err
		}
	}
	if e.writer != nil {
		_ = e.writer.Flush()
		e.writer = nil
		err = e.writeCloser.Close()
	}
	return err
}

func (e *externalStorageFile) Read(p []byte) (n int, err error) {
	if e.reader == nil {
		return 0, nil
	}
	n, err = e.reader.ReadAt(p, e.offset)
	e.offset += int64(n)
	return n, err
}

func (e *externalStorageFile) ReadAt(p []byte, off int64) (n int, err error) {
	if e.reader == nil {
		return 0, nil
	}
	n, err = e.reader.ReadAt(p, off)
	return n, err
}

func (e *externalStorageFile) Write(p []byte) (n int, err error) {
	if e.writer == nil {
		return 0, nil
	}
	return e.writer.Write(p)
}

type fileStat struct {
	name string
	size int64
}

func (f fileStat) Name() string {
	return f.name
}

func (f fileStat) Size() int64 {
	return f.size
}

func (f fileStat) Mode() fs.FileMode {
	return 0644
}

func (f fileStat) ModTime() time.Time {
	return time.Time{}
}

func (f fileStat) IsDir() bool {
	return false
}

func (f fileStat) Sys() interface{} {
	return nil
}

func (e *externalStorageFile) Stat() (os.FileInfo, error) {
	size, err := e.v.ExternalStorage.Size(e.v.ctx, path.Join(e.v.basePath, e.name))
	if err != nil {
		return nil, err
	}
	return fileStat{name: e.name, size: size}, nil
}

func (e *externalStorageFile) Sync() error {
	return nil
}

func (v *vfsAdapter) Create(name string) (vfs.File, error) {
	writer, err := v.ExternalStorage.Writer(v.ctx, path.Join(v.basePath, name))
	if err != nil {
		return nil, err
	}
	wrappedWriter := bufio.NewWriterSize(writer, 64 << 10)
	return &externalStorageFile{writer: wrappedWriter, writeCloser: writer}, nil
}

func (v *vfsAdapter) Link(oldname, newname string) error {
	return errors.New("unsupported")
}

func (v *vfsAdapter) Open(name string, opts ...vfs.OpenOption) (vfs.File, error) {
	f, err := ExternalSSTReader(v.ctx, v.ExternalStorage, name)
	if err != nil {
		return nil, err
	}
	return &externalStorageFile{reader: f, v: v, name: name}, nil
}

func (v *vfsAdapter) OpenDir(name string) (vfs.File, error) {
	return nil, errors.New("unsupported")
}

func (v *vfsAdapter) Remove(name string) error {
	return v.ExternalStorage.Delete(v.ctx, path.Join(v.basePath, name))
}

func (v *vfsAdapter) RemoveAll(name string) error {
	return v.ExternalStorage.Delete(v.ctx, path.Join(v.basePath, name))
}

func (v *vfsAdapter) Rename(oldname, newname string) error {
	readFile, err := v.Open(oldname)
	if err != nil {
		return err
	}
	defer func() {
		if readFile != nil {
			_ = readFile.Close()
			readFile = nil
		}
	}()
	writeFile, err := v.Create(newname)
	if err != nil {
		return err
	}
	defer writeFile.Close()
	_, err = io.Copy(writeFile, readFile)
	if err != nil {
		return err
	}
	_ = readFile.Close()
	readFile = nil
	return v.Remove(oldname)
}

func (v *vfsAdapter) ReuseForWrite(oldname, newname string) (vfs.File, error) {
	err := v.Remove(oldname)
	if err != nil {
		return nil, err
	}
	return v.Create(newname)
}

func (v *vfsAdapter) MkdirAll(dir string, perm os.FileMode) error {
	return nil
}

func (v *vfsAdapter) Lock(name string) (io.Closer, error) {
	return nil, errors.New("unsupported in external storage")
}

func (v *vfsAdapter) List(dir string) ([]string, error) {
	paths := make([]string, 0)
	err := v.ExternalStorage.List(v.ctx, path.Join(v.basePath, dir), "", func(s string) error {
		paths = append(paths, s)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return paths, err
}

func (v *vfsAdapter) Stat(name string) (os.FileInfo, error) {
	f, err := v.Open(name)
	if err != nil {
		return nil, err
	}
	return f.Stat()
}

func (v *vfsAdapter) PathBase(strPath string) string {
	return path.Base(strPath)
}

func (v *vfsAdapter) PathJoin(elem ...string) string {
	return path.Join(elem...)
}

func (v *vfsAdapter) PathDir(strPath string) string {
	return path.Dir(strPath)
}

func (v *vfsAdapter) GetDiskUsage(path string) (vfs.DiskUsage, error) {
	return vfs.DiskUsage{}, errors.New("unsupported")
}

var _ vfs.FS = &vfsAdapter{}
