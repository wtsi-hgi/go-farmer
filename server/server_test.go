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
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
	es "github.com/wtsi-hgi/go-farmer/elasticsearch"
)

func TestServer(t *testing.T) {
	Convey("Given a server", t, func() {
		url := "http://host:1234/"

		mock := es.NewMock()
		server := New(mock.Client())

		Convey("and non-search requests, server returns OK", func() {
			req := httptest.NewRequest(http.MethodGet, url, nil)
			w := httptest.NewRecorder()

			server.ServeHTTP(w, req)

			resp := w.Result()
			So(resp.StatusCode, ShouldEqual, http.StatusOK)
		})

		Convey("and an invalid search request, server returns Bad Request", func() {
			url += es.SearchPage
			req := httptest.NewRequest(http.MethodPost, url, nil)
			w := httptest.NewRecorder()

			server.ServeHTTP(w, req)

			resp := w.Result()
			So(resp.StatusCode, ShouldEqual, http.StatusBadRequest)

			req = httptest.NewRequest(http.MethodGet, url, strings.NewReader(`{}`))
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

			defer resp.Body.Close()

			data, err := io.ReadAll(resp.Body)
			So(err, ShouldBeNil)
			So(len(data), ShouldEqual, 1133)

			result := &es.Result{}
			err = json.Unmarshal(data, result)
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

			dec := json.NewDecoder(resp.Body)
			result := &es.Result{}
			err := dec.Decode(result)
			So(err, ShouldBeNil)

			So(len(result.Aggregations.Stats.Buckets), ShouldEqual, 0)
			So(len(result.HitSet.Hits), ShouldEqual, 10000)

			req, expectedNumHits := mock.ScrollQuery("?scroll=1m")
			w = httptest.NewRecorder()

			server.ServeHTTP(w, req)

			resp = w.Result()
			So(resp.StatusCode, ShouldEqual, http.StatusOK)

			dec = json.NewDecoder(resp.Body)
			result = &es.Result{}
			err = dec.Decode(result)
			So(err, ShouldBeNil)

			So(len(result.Aggregations.Stats.Buckets), ShouldEqual, 0)
			So(len(result.HitSet.Hits), ShouldEqual, expectedNumHits)
		})
	})
}
