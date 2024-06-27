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
	"time"

	. "github.com/smartystreets/goconvey/convey"
)

func TestQuery(t *testing.T) {
	Convey("You can make a Query from different kinds of requests, and they have unique keys", t, func() {
		url := "http://host:1234"

		req, err := http.NewRequest(http.MethodGet, url, nil) //nolint:noctx
		So(err, ShouldBeNil)

		_, madeQuery := NewQuery(req)
		So(madeQuery, ShouldBeFalse)

		body := strings.NewReader(testAggQuery)

		req, err = http.NewRequest(http.MethodPost, url, body) //nolint:noctx
		So(err, ShouldBeNil)

		_, madeQuery = NewQuery(req)
		So(madeQuery, ShouldBeFalse)

		url += "/_search"

		req, err = http.NewRequest(http.MethodPost, url, nil) //nolint:noctx
		So(err, ShouldBeNil)

		_, madeQuery = NewQuery(req)
		So(madeQuery, ShouldBeFalse)

		req, err = http.NewRequest(http.MethodPost, url, body) //nolint:noctx
		So(err, ShouldBeNil)

		query, madeQuery := NewQuery(req)
		So(madeQuery, ShouldBeTrue)

		key1 := query.Key()
		So(key1, ShouldNotBeBlank)
		So(query.Aggs, ShouldNotBeNil)
		So(query.IsScroll(), ShouldBeFalse)

		req, err = http.NewRequest(http.MethodPost, url, strings.NewReader(testNonAggQuery)) //nolint:noctx
		So(err, ShouldBeNil)

		query, madeQuery = NewQuery(req)
		So(madeQuery, ShouldBeTrue)

		key2 := query.Key()
		So(key2, ShouldNotBeBlank)
		So(key2, ShouldNotEqual, key1)
		So(query.Aggs, ShouldBeNil)
		So(query.IsScroll(), ShouldBeFalse)
		So(query.Size, ShouldEqual, 0)

		url += "?size=10000"
		req, err = http.NewRequest(http.MethodPost, url, strings.NewReader(testNonAggQuery)) //nolint:noctx
		So(err, ShouldBeNil)

		query, madeQuery = NewQuery(req)
		So(madeQuery, ShouldBeTrue)

		key3 := query.Key()
		So(key3, ShouldNotBeBlank)
		So(key3, ShouldNotEqual, key2)
		So(query.IsScroll(), ShouldBeFalse)
		So(query.Size, ShouldEqual, 10000)
		So(len(query.Source), ShouldEqual, 0)

		url += "&_source=USER_NAME"
		req, err = http.NewRequest(http.MethodPost, url, strings.NewReader(testNonAggQuery)) //nolint:noctx
		So(err, ShouldBeNil)

		query, madeQuery = NewQuery(req)
		So(madeQuery, ShouldBeTrue)

		key4 := query.Key()
		So(key4, ShouldNotBeBlank)
		So(key4, ShouldNotEqual, key3)
		So(query.IsScroll(), ShouldBeFalse)
		So(query.Size, ShouldEqual, 10000)
		So(query.Source, ShouldResemble, []string{"USER_NAME"})

		url += "%2CQUEUE_NAME"
		req, err = http.NewRequest(http.MethodPost, url, strings.NewReader(testNonAggQuery)) //nolint:noctx
		So(err, ShouldBeNil)

		query, madeQuery = NewQuery(req)
		So(madeQuery, ShouldBeTrue)

		key5 := query.Key()
		So(key5, ShouldNotBeBlank)
		So(key5, ShouldNotEqual, key4)
		So(query.IsScroll(), ShouldBeFalse)
		So(query.Size, ShouldEqual, 10000)
		So(query.Source, ShouldResemble, []string{"USER_NAME", "QUEUE_NAME"})

		url += "&scroll=60000ms"
		req, err = http.NewRequest(http.MethodPost, url, strings.NewReader(testNonAggQuery)) //nolint:noctx
		So(err, ShouldBeNil)

		query, madeQuery = NewQuery(req)
		So(madeQuery, ShouldBeTrue)

		key6 := query.Key()
		So(key6, ShouldNotBeBlank)
		So(key6, ShouldNotEqual, key5)
		So(query.IsScroll(), ShouldBeTrue)
	})

	manualQuery := &Query{
		Query: &QueryFilter{Bool: QFBool{Filter: Filter{
			{"match_phrase": map[string]interface{}{"META_CLUSTER_NAME": "farm"}},
			{"range": map[string]interface{}{
				"timestamp": map[string]string{
					"lte":    "2024-05-04T00:10:00Z",
					"gte":    "2024-05-04T00:00:00Z",
					"format": "strict_date_optional_time",
				},
			}},
			{"match_phrase": map[string]interface{}{"BOM": "Human Genetics"}},
			{"match_phrase": map[string]interface{}{"ACCOUNTING_NAME": "hgi"}},
		}}},
	}

	Convey("You can get the date range from a Query", t, func() {
		expectedLTE, err := time.Parse(time.RFC3339, "2024-05-04T00:10:00Z")
		So(err, ShouldBeNil)

		expectedGTE, err := time.Parse(time.RFC3339, "2024-05-04T00:00:00Z")
		So(err, ShouldBeNil)

		query, err := newQueryFromReader(strings.NewReader(testNonAggQuery))
		So(err, ShouldBeNil)

		lte, gte, err := query.DateRange()
		So(err, ShouldBeNil)
		So(lte, ShouldEqual, expectedLTE)
		So(gte, ShouldEqual, expectedGTE)

		lte, gte, err = manualQuery.DateRange()
		So(err, ShouldBeNil)
		So(lte, ShouldEqual, expectedLTE)
		So(gte, ShouldEqual, expectedGTE)

		noRangeQuery := `{"query":{"bool":{"filter":[{"match_phrase":{"META_CLUSTER_NAME":"farm"}}]}}}`
		query, err = newQueryFromReader(strings.NewReader(noRangeQuery))
		So(err, ShouldBeNil)

		_, _, err = query.DateRange()
		So(err, ShouldNotBeNil)
	})

	Convey("You can get the filters from a Query", t, func() {
		matchesQuery := `{"query":{"bool":{"filter":[{"match_phrase":{"META_CLUSTER_NAME":"farm"}},{"range":{"timestamp":{"lte":"2024-05-04T00:10:00Z","gte":"2024-05-04T00:00:00Z","format":"strict_date_optional_time"}}},{"prefix":{"QUEUE_NAME":"normal"}},{"match_phrase":{"ACCOUNTING_NAME":"hgi"}}]}}}` //nolint:lll
		query, err := newQueryFromReader(strings.NewReader(matchesQuery))
		So(err, ShouldBeNil)

		filters := query.Filters()
		So(filters, ShouldResemble, map[string]string{
			"META_CLUSTER_NAME": "farm",
			"ACCOUNTING_NAME":   "hgi",
			"QUEUE_NAME":        "normal",
		})

		filters = manualQuery.Filters()
		So(filters, ShouldResemble, map[string]string{
			"META_CLUSTER_NAME": "farm",
			"ACCOUNTING_NAME":   "hgi",
			"BOM":               "Human Genetics",
		})

		noMatchQuery := `{"query":{"bool":{"filter":[{"x":{"y":"z"}}]}}}`
		query, err = newQueryFromReader(strings.NewReader(noMatchQuery))
		So(err, ShouldBeNil)

		filters = query.Filters()
		So(len(filters), ShouldEqual, 0)
	})
}
