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

package elasticsearch

import (
	"net/http"
	"strings"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestQuery(t *testing.T) {
	Convey("You can make a Query from different kinds of requests", t, func() {
		url := "http://host:1234"

		req, err := http.NewRequest(http.MethodGet, url, nil) //nolint:noctx
		So(err, ShouldBeNil)

		_, ok := NewQueryFromRequest(req)
		So(ok, ShouldBeFalse)

		body := strings.NewReader(testAggQuery)

		req, err = http.NewRequest(http.MethodPost, url, body) //nolint:noctx
		So(err, ShouldBeNil)

		_, ok = NewQueryFromRequest(req)
		So(ok, ShouldBeFalse)

		url += "/_search"

		req, err = http.NewRequest(http.MethodPost, url, nil) //nolint:noctx
		So(err, ShouldBeNil)

		_, ok = NewQueryFromRequest(req)
		So(ok, ShouldBeFalse)

		req, err = http.NewRequest(http.MethodPost, url, body) //nolint:noctx
		So(err, ShouldBeNil)

		query, ok := NewQueryFromRequest(req)
		So(ok, ShouldBeTrue)
		So(query.Aggs, ShouldNotBeNil)
		So(query.IsScroll(), ShouldBeFalse)

		req, err = http.NewRequest(http.MethodPost, url, strings.NewReader(testNonAggQuery)) //nolint:noctx
		So(err, ShouldBeNil)

		query, ok = NewQueryFromRequest(req)
		So(ok, ShouldBeTrue)
		So(query.Aggs, ShouldBeNil)
		So(query.IsScroll(), ShouldBeFalse)
		So(query.Size, ShouldEqual, 0)

		url += "?size=10000"
		req, err = http.NewRequest(http.MethodPost, url, strings.NewReader(testNonAggQuery)) //nolint:noctx
		So(err, ShouldBeNil)

		query, ok = NewQueryFromRequest(req)
		So(ok, ShouldBeTrue)
		So(query.IsScroll(), ShouldBeFalse)
		So(query.Size, ShouldEqual, 10000)
		So(len(query.Source), ShouldEqual, 0)

		url += "&_source=USER_NAME"
		req, err = http.NewRequest(http.MethodPost, url, strings.NewReader(testNonAggQuery)) //nolint:noctx
		So(err, ShouldBeNil)

		query, ok = NewQueryFromRequest(req)
		So(ok, ShouldBeTrue)
		So(query.IsScroll(), ShouldBeFalse)
		So(query.Size, ShouldEqual, 10000)
		So(query.Source, ShouldResemble, []string{"USER_NAME"})

		url += "%2CQUEUE_NAME"
		req, err = http.NewRequest(http.MethodPost, url, strings.NewReader(testNonAggQuery)) //nolint:noctx
		So(err, ShouldBeNil)

		query, ok = NewQueryFromRequest(req)
		So(ok, ShouldBeTrue)
		So(query.IsScroll(), ShouldBeFalse)
		So(query.Size, ShouldEqual, 10000)
		So(query.Source, ShouldResemble, []string{"USER_NAME", "QUEUE_NAME"})

		url += "&scroll=60000ms"
		req, err = http.NewRequest(http.MethodPost, url, strings.NewReader(testNonAggQuery)) //nolint:noctx
		So(err, ShouldBeNil)

		query, ok = NewQueryFromRequest(req)
		So(ok, ShouldBeTrue)
		So(query.IsScroll(), ShouldBeTrue)
	})
}
