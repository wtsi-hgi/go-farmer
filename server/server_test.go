/*******************************************************************************
 * Copyright (c) 2024 Genome Research Ltd.
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

package server

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sort"
	"strings"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
	"github.com/wtsi-hgi/go-farmer/cache"
	es "github.com/wtsi-hgi/go-farmer/elasticsearch"
)

type mockRealServer struct {
}

func (m *mockRealServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Write([]byte("a real elasticsearch response")) //nolint:errcheck
}

type mockScroller struct {
	*es.Mock
}

func newMockScroller(index string) *mockScroller {
	mock := es.NewMock(index)

	return &mockScroller{mock}
}

func (m *mockScroller) Scroll(query *es.Query) (*es.Result, error) {
	return m.Mock.Scroll(query, nil)
}

func TestServer(t *testing.T) {
	Convey("Given a server", t, func() {
		urlStr := "http://host:1234/"
		index := "some-indexes-*"

		mockReal := httptest.NewServer(&mockRealServer{})
		defer mockReal.Close()

		mock := newMockScroller(index)
		cq, err := cache.New(mock, mock, 1)
		So(err, ShouldBeNil)

		server := New(cq, index, &url.URL{Host: strings.TrimPrefix(mockReal.URL, "http://"), Scheme: "http"})

		Convey("and non-search requests, server acts as a proxy to the 'real' elasticsearch server", func() {
			req := httptest.NewRequest(http.MethodGet, urlStr, nil)
			w := httptest.NewRecorder()

			server.ServeHTTP(w, req)

			resp := w.Result()
			So(resp.StatusCode, ShouldEqual, http.StatusOK)

			var content bytes.Buffer

			_, err := content.ReadFrom(resp.Body)
			So(err, ShouldBeNil)
			So(content.String(), ShouldEqual, "a real elasticsearch response")
		})

		Convey("and an invalid search request, server returns Bad Request", func() {
			urlStr += "some-indexes-%2A/" + es.SearchPage
			req := httptest.NewRequest(http.MethodPost, urlStr, nil)
			w := httptest.NewRecorder()

			server.ServeHTTP(w, req)

			resp := w.Result()
			So(resp.StatusCode, ShouldEqual, http.StatusBadRequest)

			req = httptest.NewRequest(http.MethodGet, urlStr, strings.NewReader(`{}`))
			w = httptest.NewRecorder()

			server.ServeHTTP(w, req)

			resp = w.Result()
			So(resp.StatusCode, ShouldEqual, http.StatusBadRequest)
		})

		Convey("and a valid aggregation search request, server returns agg results", func() {
			req := mock.AggQuery()
			w := httptest.NewRecorder()

			server.ServeHTTP(w, req)

			resp := w.Result()
			So(resp.StatusCode, ShouldEqual, http.StatusOK)

			data, err := io.ReadAll(resp.Body)
			So(err, ShouldBeNil)
			So(len(data), ShouldEqual, 240)
			resp.Body.Close()

			result, err := cache.Decode(data)
			So(err, ShouldBeNil)

			So(len(result.HitSet.Hits), ShouldEqual, 0)
			So(len(result.Aggregations.Stats.Buckets), ShouldEqual, 6)
		})

		Convey("and a valid scrolling search request, server returns all scroll hits", func() {
			req, _ := mock.ScrollQuery("")
			w := httptest.NewRecorder()

			server.ServeHTTP(w, req)

			resp := w.Result()
			So(resp.StatusCode, ShouldEqual, http.StatusOK)

			data, err := io.ReadAll(resp.Body)
			So(err, ShouldBeNil)
			resp.Body.Close()

			result, err := cache.Decode(data)
			So(err, ShouldBeNil)

			So(result.Aggregations, ShouldBeNil)
			So(len(result.HitSet.Hits), ShouldEqual, 10000)

			req, expectedNumHits := mock.ScrollQuery("?scroll=1m")
			w = httptest.NewRecorder()

			server.ServeHTTP(w, req)

			resp = w.Result()
			So(resp.StatusCode, ShouldEqual, http.StatusOK)

			data, err = io.ReadAll(resp.Body)
			So(err, ShouldBeNil)
			resp.Body.Close()

			result, err = cache.Decode(data)
			So(err, ShouldBeNil)

			So(result.Aggregations, ShouldBeNil)
			So(result.HitSet.Total.Value, ShouldEqual, expectedNumHits)
			So(len(result.HitSet.Hits), ShouldEqual, expectedNumHits)
			So(result.HitSet.Hits[0].ID, ShouldNotBeBlank)
			So(result.HitSet.Hits[0].Details.ID, ShouldBeBlank)
		})

		Convey("and scroll endpoint requests, server returns pretend responses", func() {
			urlStr += es.SearchPage + "/" + scrollPage
			req := httptest.NewRequest(http.MethodPost, urlStr, nil)
			w := httptest.NewRecorder()

			server.ServeHTTP(w, req)

			resp := w.Result()
			So(resp.StatusCode, ShouldEqual, http.StatusOK)

			req = httptest.NewRequest(http.MethodDelete, urlStr, nil)
			w = httptest.NewRecorder()

			server.ServeHTTP(w, req)

			resp = w.Result()
			So(resp.StatusCode, ShouldEqual, http.StatusOK)
			So(resp.Body, ShouldNotBeNil)

			bodyBytes, err := io.ReadAll(resp.Body)
			So(err, ShouldBeNil)
			So(string(bodyBytes), ShouldEqual, `{"succeeded":true,"num_freed":0}`)
		})

		Convey("and a valid get_usernames request, server returns all users", func() {
			req, _ := mock.ScrollQuery("?scroll=1m")
			req.URL.Path = slash + getUsernamesEndpoint

			w := httptest.NewRecorder()

			server.ServeHTTP(w, req)

			resp := w.Result()
			So(resp.StatusCode, ShouldEqual, http.StatusOK)

			data, err := io.ReadAll(resp.Body)
			So(err, ShouldBeNil)
			resp.Body.Close()

			var usernames []string

			err = json.Unmarshal(data, &usernames)
			So(err, ShouldBeNil)

			sort.Strings(usernames)

			expected := []string{"u", "u1", "u2"}
			So(usernames, ShouldResemble, expected)
		})
	})
}
