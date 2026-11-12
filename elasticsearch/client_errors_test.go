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
	"testing/iotest"

	. "github.com/smartystreets/goconvey/convey"
)

const (
	faultEmptyPageJSON = `{"_scroll_id":"sid","hits":{"total":{"value":23581},"hits":[]}}`
	faultPageJSON      = `{"_scroll_id":"sid","hits":{"total":{"value":23581},"hits":[{"_id":"1","_source":{"USER_NAME":"u1"}}]}}` //nolint:lll
	faultTruncatedJSON = `{"_scroll_id":"sid","hits":{"total":`
)

var errFaultConnection = errors.New("connection reset")

// faultTransport is an http.RoundTripper that serves a scroll search whose
// total is 23581 but whose every page holds one hit. The initial search
// response comes from initial (nil means a connection error), and the response
// for scroll page N (counting from 1) comes from page(N). Clear scroll and info
// requests succeed.
type faultTransport struct {
	initial func() *http.Response
	page    func(n int) *http.Response
	pages   *int
}

func (f faultTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.URL.Path == "/" {
		return faultResponse(http.StatusOK, strings.NewReader(mockVersionJSON)), nil
	}

	if !strings.HasSuffix(req.URL.Path, "/scroll") {
		if resp := f.initial(); resp != nil {
			return resp, nil
		}

		return nil, errFaultConnection
	}

	if req.Method == http.MethodDelete {
		return faultResponse(http.StatusOK, strings.NewReader(`{}`)), nil
	}

	*f.pages++

	return f.page(*f.pages), nil
}

func faultResponse(status int, body io.Reader) *http.Response {
	return &http.Response{
		StatusCode: status,
		Body:       io.NopCloser(body),
		Header:     http.Header{"X-Elastic-Product": []string{"Elasticsearch"}},
	}
}

func faultClient(initial func() *http.Response, page func(int) *http.Response) *Client {
	client, err := NewClient(Config{
		Host:      "fault",
		Scheme:    "http",
		Port:      mockPort,
		Index:     "fault",
		transport: faultTransport{initial: initial, page: page, pages: new(int)},
	})
	So(err, ShouldBeNil)

	return client
}

func TestScrollErrorContext(t *testing.T) {
	failOnPage := func(failing int, fail func() *http.Response) func(int) *http.Response {
		return func(n int) *http.Response {
			if n == failing {
				return fail()
			}

			return goodPage()
		}
	}

	Convey("When the initial search response body is cut off, Scroll's error names that stage and wraps the cause",
		t, func() {
			err := scrollWithFault(truncatedPage, failOnPage(0, truncatedPage))
			So(err, ShouldWrap, io.ErrUnexpectedEOF)
			So(err.Error(), ShouldEqual, "initial search: read response body: unexpected EOF")
		})

	Convey("When the initial search request fails, Scroll's error names that stage and wraps the cause", t, func() {
		err := scrollWithFault(func() *http.Response { return nil }, failOnPage(0, truncatedPage))
		So(err, ShouldWrap, errFaultConnection)
		So(err.Error(), ShouldStartWith, "initial search: ")
	})

	Convey("When the initial search response is not valid JSON, Scroll's error names the decode stage", t, func() {
		badJSON := func() *http.Response { return faultResponse(http.StatusOK, strings.NewReader(`{"hits":`)) }

		err := scrollWithFault(badJSON, failOnPage(0, truncatedPage))
		So(err, ShouldNotBeNil)
		So(err.Error(), ShouldStartWith, "initial search: decode response: ")
	})

	Convey("When the initial search fails with an error status, Scroll's error names that stage and keeps the Error type",
		t, func() {
			serverError := func() *http.Response {
				return faultResponse(http.StatusInternalServerError, strings.NewReader(`{"error":"boom"}`))
			}

			err := scrollWithFault(serverError, failOnPage(0, truncatedPage))
			So(err, ShouldNotBeNil)
			So(err.Error(), ShouldStartWith, "initial search: "+ErrFailedQuery)

			var esErr Error

			So(errors.As(err, &esErr), ShouldBeTrue)
			So(esErr.Msg, ShouldEqual, ErrFailedQuery)
		})

	Convey("When a scroll page response body is cut off, Scroll's error names the page and wraps the cause", t, func() {
		err := scrollWithFault(goodPage, failOnPage(1, truncatedPage))
		So(err, ShouldWrap, io.ErrUnexpectedEOF)
		So(err.Error(), ShouldEqual,
			"scroll page 1 after 1 of 23581 hits: read response body: unexpected EOF")

		err = scrollWithFault(goodPage, failOnPage(3, truncatedPage))
		So(err, ShouldWrap, io.ErrUnexpectedEOF)
		So(err.Error(), ShouldEqual,
			"scroll page 3 after 3 of 23581 hits: read response body: unexpected EOF")
	})

	Convey("A scroll with no faults succeeds, ending when a page has no hits", t, func() {
		client := faultClient(goodPage, func(n int) *http.Response {
			if n == 2 {
				return faultResponse(http.StatusOK, strings.NewReader(faultEmptyPageJSON))
			}

			return goodPage()
		})

		received := 0
		_, err := client.Scroll(&Query{Size: MaxSize}, func(*Hit) { received++ })
		So(err, ShouldBeNil)
		So(received, ShouldEqual, 2)
		So(client.Error, ShouldBeNil)
	})
}

func goodPage() *http.Response {
	return faultResponse(http.StatusOK, strings.NewReader(faultPageJSON))
}

func scrollWithFault(initial func() *http.Response, page func(int) *http.Response) error {
	client := faultClient(initial, page)
	_, err := client.Scroll(&Query{Size: MaxSize}, func(*Hit) {})

	return err
}

// truncatedPage returns a response whose body ends with io.ErrUnexpectedEOF
// part way through, as when a connection closes mid-body.
func truncatedPage() *http.Response {
	return faultResponse(http.StatusOK, io.MultiReader(
		strings.NewReader(faultTruncatedJSON),
		iotest.ErrReader(io.ErrUnexpectedEOF),
	))
}
