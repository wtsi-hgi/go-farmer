/*******************************************************************************
 * Copyright (c) 2026 Genome Research Ltd.
 *
 * Author: Sendu Bala <sb10@sanger.ac.uk>
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be included
 * in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY
 * CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
 * TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
 * SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 ******************************************************************************/

package elasticsearch

import (
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

const bodyCloseServerError = `{"error":"boom"}`

// closeRecordingBody is a response body that records whether it was closed.
type closeRecordingBody struct {
	io.Reader

	closed bool
}

func (b *closeRecordingBody) Close() error {
	b.closed = true

	return nil
}

func closeRecordingClient(searchStatus, scrollStatus int) (*Client, *[]*closeRecordingBody) {
	bodies := &[]*closeRecordingBody{}

	client := transportClient(closeRecordingTransport{
		searchStatus: searchStatus,
		scrollStatus: scrollStatus,
		bodies:       bodies,
	})

	return client, bodies
}

func countUnclosed(bodies []*closeRecordingBody) int {
	unclosed := 0

	for _, body := range bodies {
		if !body.closed {
			unclosed++
		}
	}

	return unclosed
}

// clearScrollFailingClient is like closeRecordingClient with successful
// statuses, except that clear scroll requests fail with errFaultConnection.
func clearScrollFailingClient() (*Client, *[]*closeRecordingBody) {
	bodies := &[]*closeRecordingBody{}

	client := transportClient(clearScrollFailingTransport{closeRecordingTransport{
		searchStatus: http.StatusOK,
		scrollStatus: http.StatusOK,
		bodies:       bodies,
	}})

	return client, bodies
}

// closeRecordingTransport is an http.RoundTripper that serves info, search,
// scroll and clear scroll requests, recording every response body it hands
// out. The initial search and each scroll page respond with the given status
// codes; a 200 search gives a page of 1 hit out of 23581, and a 200 scroll page
// has no hits.
type closeRecordingTransport struct {
	searchStatus int
	scrollStatus int
	bodies       *[]*closeRecordingBody
}

func (t closeRecordingTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	status, body := http.StatusOK, `{}`

	switch {
	case req.URL.Path == "/":
		body = mockVersionJSON
	case !strings.HasSuffix(req.URL.Path, "/scroll"):
		status, body = t.searchStatus, faultPageJSON
	case req.Method != http.MethodDelete:
		status, body = t.scrollStatus, faultEmptyPageJSON
	}

	if status != http.StatusOK {
		body = bodyCloseServerError
	}

	recorded := &closeRecordingBody{Reader: strings.NewReader(body)}
	*t.bodies = append(*t.bodies, recorded)

	resp := faultResponse(status, nil)
	resp.Body = recorded

	return resp, nil
}

// clearScrollFailingTransport is a closeRecordingTransport whose clear scroll
// (DELETE) requests fail at the transport level with errFaultConnection.
type clearScrollFailingTransport struct {
	closeRecordingTransport
}

func (t clearScrollFailingTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.Method == http.MethodDelete {
		return nil, errFaultConnection
	}

	return t.closeRecordingTransport.RoundTrip(req)
}

func TestResponseBodiesAreClosed(t *testing.T) {
	Convey("When Search gets an error status, the error keeps the server text and every response body is closed",
		t, func() {
			client, bodies := closeRecordingClient(http.StatusInternalServerError, http.StatusOK)

			_, err := client.Search(&Query{})
			shouldBeFailedQueryWithServerText(err)
			So(*bodies, ShouldNotBeEmpty)
			So(countUnclosed(*bodies), ShouldEqual, 0)
		})

	Convey("When Scroll's initial search gets an error status, the error keeps the server text and every "+
		"response body is closed", t, func() {
		client, bodies := closeRecordingClient(http.StatusInternalServerError, http.StatusOK)

		_, err := client.Scroll(&Query{Size: MaxSize}, func(*Hit) {})
		shouldBeFailedQueryWithServerText(err)
		So(*bodies, ShouldNotBeEmpty)
		So(countUnclosed(*bodies), ShouldEqual, 0)
	})

	Convey("When a scroll page gets an error status, the error keeps the server text and every response body "+
		"is closed", t, func() {
		client, bodies := closeRecordingClient(http.StatusOK, http.StatusInternalServerError)

		_, err := client.Scroll(&Query{Size: MaxSize}, func(*Hit) {})
		shouldBeFailedQueryWithServerText(err)
		So(*bodies, ShouldNotBeEmpty)
		So(countUnclosed(*bodies), ShouldEqual, 0)
	})

	Convey("After a successful Scroll, including its clear scroll request, every response body is closed", t, func() {
		client, bodies := closeRecordingClient(http.StatusOK, http.StatusOK)

		_, err := client.Scroll(&Query{Size: MaxSize}, func(*Hit) {})
		So(err, ShouldBeNil)
		So(client.Error, ShouldBeNil)
		So(*bodies, ShouldNotBeEmpty)
		So(countUnclosed(*bodies), ShouldEqual, 0)
	})

	Convey("When the clear scroll request fails with a connection error, Scroll still succeeds and records the "+
		"error without panicking", t, func() {
		client, bodies := clearScrollFailingClient()

		_, err := client.Scroll(&Query{Size: MaxSize}, func(*Hit) {})
		So(err, ShouldBeNil)
		So(client.Error, ShouldWrap, errFaultConnection)
		So(*bodies, ShouldNotBeEmpty)
		So(countUnclosed(*bodies), ShouldEqual, 0)
	})

	Convey("After Info, every response body is closed", t, func() {
		client, bodies := closeRecordingClient(http.StatusOK, http.StatusOK)

		info, err := client.Info()
		So(err, ShouldBeNil)
		So(info.Version.Number, ShouldEqual, "7.17.6")
		So(*bodies, ShouldNotBeEmpty)
		So(countUnclosed(*bodies), ShouldEqual, 0)
	})
}

func shouldBeFailedQueryWithServerText(err error) {
	So(err, ShouldNotBeNil)

	var esErr Error

	So(errors.As(err, &esErr), ShouldBeTrue)
	So(esErr.Msg, ShouldEqual, ErrFailedQuery)
	So(err.Error(), ShouldContainSubstring, bodyCloseServerError)
}
