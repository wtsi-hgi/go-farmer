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

package cache

import (
	"strconv"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
	es "github.com/wtsi-hgi/go-farmer/elasticsearch"
)

const (
	index     = "mockindex"
	cacheSize = 2
)

type mockSearchScroller struct {
	searchCalls int
	scrollCalls int
}

func (m *mockSearchScroller) Search(index string, query *es.Query) (*es.Result, error) {
	m.searchCalls++

	return m.querier(index, query)
}

func (m *mockSearchScroller) querier(_ string, query *es.Query) (*es.Result, error) {
	filters := query.Filters()

	total, err := strconv.Atoi(filters["total"])
	if err != nil {
		return nil, err
	}

	return &es.Result{
		HitSet: &es.HitSet{
			Total: es.HitSetTotal{
				Value: total,
			},
		},
	}, nil
}

func (m *mockSearchScroller) Scroll(index string, query *es.Query) (*es.Result, error) {
	m.scrollCalls++

	return m.querier(index, query)
}

func TestCache(t *testing.T) {
	Convey("Given a Searcher, a Scroller, a Query and a CachedQuerier", t, func() {
		ss := &mockSearchScroller{}

		expectedTotal := 5

		query := &es.Query{
			Query: &es.QueryFilter{Bool: es.QFBool{Filter: es.Filter{
				{"match_phrase": map[string]interface{}{"total": strconv.Itoa(expectedTotal)}},
			}}},
		}

		cq, err := New(ss, ss, cacheSize)
		So(err, ShouldBeNil)

		Convey("You can get uncached, then cached Search results", func() {
			So(ss.searchCalls, ShouldEqual, 0)

			results, err := cq.Search(index, query)
			So(err, ShouldBeNil)
			So(results.HitSet.Total.Value, ShouldEqual, expectedTotal)
			So(ss.searchCalls, ShouldEqual, 1)

			results, err = cq.Search(index, query)
			So(err, ShouldBeNil)
			So(results.HitSet.Total.Value, ShouldEqual, expectedTotal)
			So(ss.searchCalls, ShouldEqual, 1)
			So(ss.scrollCalls, ShouldEqual, 0)

			Convey("Different queries get fresh results that are also cached", func() {
				expectedTotal2 := expectedTotal + 1
				query2 := &es.Query{
					Query: &es.QueryFilter{Bool: es.QFBool{Filter: es.Filter{
						{"match_phrase": map[string]interface{}{"total": strconv.Itoa(expectedTotal2)}},
					}}},
				}

				results, err := cq.Search(index, query2)
				So(err, ShouldBeNil)
				So(results.HitSet.Total.Value, ShouldEqual, expectedTotal2)
				So(ss.searchCalls, ShouldEqual, 2)

				results, err = cq.Search(index, query2)
				So(err, ShouldBeNil)
				So(results.HitSet.Total.Value, ShouldEqual, expectedTotal2)
				So(ss.searchCalls, ShouldEqual, 2)
				So(ss.scrollCalls, ShouldEqual, 0)

				results, err = cq.Search(index, query)
				So(err, ShouldBeNil)
				So(results.HitSet.Total.Value, ShouldEqual, expectedTotal)
				So(ss.searchCalls, ShouldEqual, 2)

				Convey("The least recently used item in the cache gets evicted once full", func() {
					expectedTotal3 := expectedTotal2 + 1
					query3 := &es.Query{
						Query: &es.QueryFilter{Bool: es.QFBool{Filter: es.Filter{
							{"match_phrase": map[string]interface{}{"total": strconv.Itoa(expectedTotal3)}},
						}}},
					}

					results, err := cq.Search(index, query3)
					So(err, ShouldBeNil)
					So(results.HitSet.Total.Value, ShouldEqual, expectedTotal3)
					So(ss.searchCalls, ShouldEqual, 3)

					results, err = cq.Search(index, query3)
					So(err, ShouldBeNil)
					So(results.HitSet.Total.Value, ShouldEqual, expectedTotal3)
					So(ss.searchCalls, ShouldEqual, 3)

					results, err = cq.Search(index, query)
					So(err, ShouldBeNil)
					So(results.HitSet.Total.Value, ShouldEqual, expectedTotal)
					So(ss.searchCalls, ShouldEqual, 3)

					results, err = cq.Search(index, query2)
					So(err, ShouldBeNil)
					So(results.HitSet.Total.Value, ShouldEqual, expectedTotal2)
					So(ss.searchCalls, ShouldEqual, 4)
				})
			})
		})

		Convey("You can get uncached, then cached Scroll results", func() {
			So(ss.scrollCalls, ShouldEqual, 0)

			results, err := cq.Scroll(index, query)
			So(err, ShouldBeNil)
			So(results.HitSet.Total.Value, ShouldEqual, expectedTotal)
			So(ss.scrollCalls, ShouldEqual, 1)

			results, err = cq.Scroll(index, query)
			So(err, ShouldBeNil)
			So(results.HitSet.Total.Value, ShouldEqual, expectedTotal)
			So(ss.scrollCalls, ShouldEqual, 1)
			So(ss.searchCalls, ShouldEqual, 0)
		})
	})
}
