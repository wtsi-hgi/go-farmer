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
	"encoding/json"
	"sort"
	"strconv"
	"strings"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
	es "github.com/wtsi-hgi/go-farmer/elasticsearch"
)

const (
	index     = "mockindex"
	cacheSize = 2
)

type mockSearchScroller struct {
	searchCalls   int
	scrollCalls   int
	usernameCalls int
}

func (m *mockSearchScroller) Search(query *es.Query) (*es.Result, error) {
	m.searchCalls++

	return m.querier(query)
}

func (m *mockSearchScroller) querier(query *es.Query) (*es.Result, error) {
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
			Hits: []es.Hit{
				{ID: "1", Details: &es.Details{ID: "1", UserName: "a", Job: "j", MemRequestedMB: 1}},
				{ID: "2", Details: &es.Details{ID: "2", UserName: "a"}},
				{ID: "3", Details: &es.Details{ID: "3", UserName: "b"}},
				{ID: "4", Details: &es.Details{ID: "4", UserName: "a"}},
				{ID: "5", Details: &es.Details{ID: "5", UserName: "b"}},
			},
		},
	}, nil
}

func (m *mockSearchScroller) Scroll(query *es.Query) (*es.Result, error) {
	m.scrollCalls++

	return m.querier(query)
}

func (m *mockSearchScroller) Done(query *es.Query) bool {
	return true
}

func (m *mockSearchScroller) Usernames(query *es.Query) ([]string, error) {
	m.usernameCalls++

	r, err := m.querier(query)
	if err != nil {
		return nil, err
	}

	usernamesMap := make(map[string]bool)

	for _, hit := range r.HitSet.Hits {
		usernamesMap[hit.Details.UserName] = true
	}

	usernames := make([]string, 0, len(usernamesMap))
	for username := range usernamesMap {
		usernames = append(usernames, username)
	}

	return usernames, nil
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

			data, err := cq.Search(query)
			So(err, ShouldBeNil)

			results, err := Decode(data)
			So(err, ShouldBeNil)
			So(results.HitSet.Total.Value, ShouldEqual, expectedTotal)
			So(ss.searchCalls, ShouldEqual, 1)

			data, err = cq.Search(query)
			So(err, ShouldBeNil)

			results, err = Decode(data)
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

				data, err = cq.Search(query2)
				So(err, ShouldBeNil)

				results, err = Decode(data)
				So(err, ShouldBeNil)
				So(results.HitSet.Total.Value, ShouldEqual, expectedTotal2)
				So(ss.searchCalls, ShouldEqual, 2)

				data, err = cq.Search(query2)
				So(err, ShouldBeNil)

				results, err = Decode(data)
				So(err, ShouldBeNil)
				So(results.HitSet.Total.Value, ShouldEqual, expectedTotal2)
				So(ss.searchCalls, ShouldEqual, 2)
				So(ss.scrollCalls, ShouldEqual, 0)

				data, err = cq.Search(query)
				So(err, ShouldBeNil)

				results, err = Decode(data)
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

					data, err = cq.Search(query3)
					So(err, ShouldBeNil)

					results, err = Decode(data)
					So(err, ShouldBeNil)
					So(results.HitSet.Total.Value, ShouldEqual, expectedTotal3)
					So(ss.searchCalls, ShouldEqual, 3)

					data, err = cq.Search(query3)
					So(err, ShouldBeNil)

					results, err = Decode(data)
					So(err, ShouldBeNil)
					So(results.HitSet.Total.Value, ShouldEqual, expectedTotal3)
					So(ss.searchCalls, ShouldEqual, 3)

					data, err = cq.Search(query)
					So(err, ShouldBeNil)

					results, err = Decode(data)
					So(err, ShouldBeNil)
					So(results.HitSet.Total.Value, ShouldEqual, expectedTotal)
					So(ss.searchCalls, ShouldEqual, 3)

					data, err = cq.Search(query2)
					So(err, ShouldBeNil)

					results, err = Decode(data)
					So(err, ShouldBeNil)
					So(results.HitSet.Total.Value, ShouldEqual, expectedTotal2)
					So(ss.searchCalls, ShouldEqual, 4)
				})
			})
		})

		Convey("You can get uncached, then cached Scroll results", func() {
			So(ss.scrollCalls, ShouldEqual, 0)

			data, err := cq.Scroll(query)
			So(err, ShouldBeNil)

			results, err := Decode(data)
			So(err, ShouldBeNil)
			So(results.HitSet.Total.Value, ShouldEqual, expectedTotal)
			So(ss.scrollCalls, ShouldEqual, 1)

			data, err = cq.Scroll(query)
			So(err, ShouldBeNil)

			results, err = Decode(data)
			So(err, ShouldBeNil)
			So(results.HitSet.Total.Value, ShouldEqual, expectedTotal)
			So(ss.scrollCalls, ShouldEqual, 1)
			So(ss.searchCalls, ShouldEqual, 0)
		})

		Convey("You can get uncached, then cached Usernames results", func() {
			So(ss.usernameCalls, ShouldEqual, 0)

			data, err := cq.Usernames(query)
			So(err, ShouldBeNil)

			var usernames []string

			err = json.Unmarshal(data, &usernames)
			So(err, ShouldBeNil)

			sort.Strings(usernames)

			expected := []string{"a", "b"}
			So(usernames, ShouldResemble, expected)
			So(ss.usernameCalls, ShouldEqual, 1)

			data, err = cq.Usernames(query)
			So(err, ShouldBeNil)

			usernames = nil

			err = json.Unmarshal(data, &usernames)
			So(err, ShouldBeNil)
			sort.Strings(usernames)
			So(usernames, ShouldResemble, expected)
			So(ss.usernameCalls, ShouldEqual, 1)
			So(ss.scrollCalls, ShouldEqual, 0)
			So(ss.searchCalls, ShouldEqual, 0)

			rdata, err := cq.Scroll(query)
			So(err, ShouldBeNil)

			results, err := Decode(rdata)
			So(err, ShouldBeNil)
			So(results.HitSet.Total.Value, ShouldEqual, expectedTotal)
			So(ss.scrollCalls, ShouldEqual, 1)

			data, err = cq.Usernames(query)
			So(err, ShouldBeNil)

			usernames = nil

			err = json.Unmarshal(data, &usernames)
			So(err, ShouldBeNil)

			sort.Strings(usernames)

			So(usernames, ShouldResemble, expected)
			So(ss.usernameCalls, ShouldEqual, 1)
			So(ss.scrollCalls, ShouldEqual, 1)
			So(ss.searchCalls, ShouldEqual, 0)
		})

		Convey("You can get all fields, or just the ones you want", func() {
			data, err := cq.Scroll(query)
			So(err, ShouldBeNil)

			jsonStr := string(data)
			So(strings.Count(jsonStr, `{"_id":"`), ShouldEqual, 5)
			So(strings.Count(jsonStr, `{"_source":{"_id":"`), ShouldEqual, 0)
			So(strings.Count(jsonStr, `,"MEM_REQUESTED_MB":1`), ShouldEqual, 1)
			So(strings.Count(jsonStr, `,"MEM_REQUESTED_MB":`), ShouldEqual, 5)
			So(strings.Count(jsonStr, `,"Job":"j"`), ShouldEqual, 1)
			So(strings.Count(jsonStr, `,"Job":`), ShouldEqual, 5)
			So(strings.Count(jsonStr, `"ACCOUNTING_NAME":`), ShouldEqual, 5)
			So(strings.Count(jsonStr, `,"AVAIL_CPU_TIME_SEC":`), ShouldEqual, 5)
			So(strings.Count(jsonStr, `,"BOM":`), ShouldEqual, 5)
			So(strings.Count(jsonStr, `,"Command":`), ShouldEqual, 5)
			So(strings.Count(jsonStr, `,"JOB_NAME":`), ShouldEqual, 5)
			So(strings.Count(jsonStr, `,"MEM_REQUESTED_MB_SEC":`), ShouldEqual, 5)
			So(strings.Count(jsonStr, `,"NUM_EXEC_PROCS":`), ShouldEqual, 5)
			So(strings.Count(jsonStr, `,"PENDING_TIME_SEC":`), ShouldEqual, 5)
			So(strings.Count(jsonStr, `,"QUEUE_NAME":`), ShouldEqual, 5)
			So(strings.Count(jsonStr, `,"RUN_TIME_SEC":`), ShouldEqual, 5)
			So(strings.Count(jsonStr, `,"timestamp":`), ShouldEqual, 5)
			So(strings.Count(jsonStr, `,"USER_NAME":`), ShouldEqual, 5)
			So(strings.Count(jsonStr, `,"WASTED_CPU_SECONDS":`), ShouldEqual, 5)
			So(strings.Count(jsonStr, `,"WASTED_MB_SECONDS":0`), ShouldEqual, 5)

			query.Source = []string{"MEM_REQUESTED_MB", "Job", "WASTED_MB_SECONDS"}
			data, err = cq.Scroll(query)
			So(err, ShouldBeNil)

			jsonStr = string(data)
			So(strings.Count(jsonStr, `{"_id":"`), ShouldEqual, 5)
			So(strings.Count(jsonStr, `{"_source":{"_id":"`), ShouldEqual, 0)
			So(strings.Count(jsonStr, `,"MEM_REQUESTED_MB":1`), ShouldEqual, 1)
			So(strings.Count(jsonStr, `,"MEM_REQUESTED_MB":0`), ShouldEqual, 4)
			So(strings.Count(jsonStr, `"Job":"j"`), ShouldEqual, 1)
			So(strings.Count(jsonStr, `"Job":""`), ShouldEqual, 4)
			So(strings.Count(jsonStr, `"ACCOUNTING_NAME":`), ShouldEqual, 0)
			So(strings.Count(jsonStr, `,"AVAIL_CPU_TIME_SEC":`), ShouldEqual, 0)
			So(strings.Count(jsonStr, `,"BOM":`), ShouldEqual, 0)
			So(strings.Count(jsonStr, `,"Command":`), ShouldEqual, 0)
			So(strings.Count(jsonStr, `,"JOB_NAME":`), ShouldEqual, 0)
			So(strings.Count(jsonStr, `,"MEM_REQUESTED_MB_SEC":`), ShouldEqual, 0)
			So(strings.Count(jsonStr, `,"NUM_EXEC_PROCS":`), ShouldEqual, 0)
			So(strings.Count(jsonStr, `,"PENDING_TIME_SEC":`), ShouldEqual, 0)
			So(strings.Count(jsonStr, `,"QUEUE_NAME":`), ShouldEqual, 0)
			So(strings.Count(jsonStr, `,"RUN_TIME_SEC":`), ShouldEqual, 0)
			So(strings.Count(jsonStr, `,"timestamp":`), ShouldEqual, 0)
			So(strings.Count(jsonStr, `,"USER_NAME":`), ShouldEqual, 0)
			So(strings.Count(jsonStr, `,"WASTED_CPU_SECONDS":`), ShouldEqual, 0)
			So(strings.Count(jsonStr, `,"WASTED_MB_SECONDS":0`), ShouldEqual, 5)
		})
	})
}
