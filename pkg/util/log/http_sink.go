// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package log

import (
	"bytes"
	"crypto/tls"
	"fmt"
	"net/http"
	"net/url"

	"github.com/cockroachdb/cockroach/pkg/cli/exit"
	"github.com/cockroachdb/cockroach/pkg/util/log/logconfig"
	"github.com/cockroachdb/errors"
)

// TODO: HTTP requests should be bound to context via http.NewRequestWithContext
// Proper logging context to be decided/designed.
func newHTTPSink(c logconfig.HTTPSinkConfig) (*httpSink, error) {
	transport, ok := http.DefaultTransport.(*http.Transport)
	if !ok {
		return nil, errors.AssertionFailedf("http.DefaultTransport is not a http.Transport: %T", http.DefaultTransport)
	}
	transport = transport.Clone()
	transport.DisableKeepAlives = *c.DisableKeepAlives
	hs := &httpSink{
		client: http.Client{
			Transport: transport,
			Timeout:   *c.Timeout,
		},
		address:     *c.Address,
		doRequest:   doPost,
		contentType: "application/octet-stream",
	}

	if *c.UnsafeTLS {
		transport.TLSClientConfig = &tls.Config{InsecureSkipVerify: true}
	}

	if string(*c.Method) == http.MethodGet {
		hs.doRequest = doGet
	}

	f, ok := formatters[*c.Format]
	if !ok {
		panic(errors.AssertionFailedf("unknown format: %q", *c.Format))
	}
	if f.contentType() != "" {
		hs.contentType = f.contentType()
	}

	hs.config = &c

	return hs, nil
}

type httpSink struct {
	client      http.Client
	address     string
	contentType string
	doRequest   func(sink *httpSink, logEntry []byte) (*http.Response, error)
	config      *logconfig.HTTPSinkConfig
}

// output emits some formatted bytes to this sink.
// the sink is invited to perform an extra flush if indicated
// by the argument. This is set to true for e.g. Fatal
// entries.
//
// The parent logger's outputMu is held during this operation: log
// sinks must not recursively call into logging when implementing
// this method.
func (hs *httpSink) output(b []byte, opt sinkOutputOptions) (err error) {
	resp, err := hs.doRequest(hs, b)
	if err != nil {
		return err
	}

	if resp.StatusCode >= 400 {
		return HTTPLogError{
			StatusCode: resp.StatusCode,
			Address:    hs.address,
		}
	}
	return nil
}

func doPost(hs *httpSink, b []byte) (*http.Response, error) {
	resp, err := hs.client.Post(hs.address, hs.contentType, bytes.NewReader(b))
	if err != nil {
		return nil, err
	}
	resp.Body.Close() // don't care about content
	return resp, nil
}

func doGet(hs *httpSink, b []byte) (*http.Response, error) {
	resp, err := hs.client.Get(hs.address + "?" + url.QueryEscape(string(b)))
	if err != nil {
		return nil, err
	}
	resp.Body.Close() // don't care about content
	return resp, nil
}

// active returns true if this sink is currently active.
func (*httpSink) active() bool {
	return true
}

// attachHints attaches some hints about the location of the message
// to the stack message.
func (*httpSink) attachHints(stacks []byte) []byte {
	return stacks
}

// exitCode returns the exit code to use if the logger decides
// to terminate because of an error in output().
func (*httpSink) exitCode() exit.Code {
	return exit.LoggingNetCollectorUnavailable()
}

// HTTPLogError represents an HTTP error status code from a logging request.
type HTTPLogError struct {
	StatusCode int
	Address    string
}

func (e HTTPLogError) Error() string {
	return fmt.Sprintf(
		"received %v response attempting to log to [%v]",
		e.StatusCode, e.Address)
}
