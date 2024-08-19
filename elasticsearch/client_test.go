/*******************************************************************************
 * Copyright (c) 2024 Genome Research Ltd.
 *
 * Author: Sendu Bala <sb10@sanger.ac.uk>
 * Author: Iaroslav Popov <ip13@sanger.ac.uk>
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
	"os"
	"strconv"
	"strings"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestElasticSearchClientMock(t *testing.T) {
	Convey("Given some config", t, func() {
		mockTrans := mockTransport{}

		config := Config{
			Host:      "mock",
			Username:  "mock",
			Password:  "mock",
			Scheme:    "http",
			Port:      1234,
			Index:     "mock-*",
			transport: mockTrans,
		}

		doClientTests(t, config, 2)
	})
}

func doClientTests(t *testing.T, config Config, expectedNumHits int) {
	t.Helper()

	Convey("You can create an elasticsearch client", func() {
		client, err := NewClient(config)
		So(err, ShouldBeNil)
		So(client, ShouldNotBeNil)

		info, err := client.Info()
		So(err, ShouldBeNil)
		So(info.Version.Number, ShouldEqual, testExpectedVersion)

		Convey("And given an elasticsearch aggregation query json", func() {
			jsonStr := testAggQuery
			r := strings.NewReader(jsonStr)
			query, err := newQueryFromReader(r)
			So(err, ShouldBeNil)

			Convey("You can do a Search", func() {
				result, err := client.Search(query)
				So(err, ShouldBeNil)
				So(result, ShouldNotBeNil)
				So(len(result.Aggregations.Stats.Buckets), ShouldEqual, 6)
				So(len(result.HitSet.Hits), ShouldEqual, 0)
				So(result.HitSet.Total.Value, ShouldEqual, expectedNumHits)
			})
		})

		Convey("And given an elasticsearch non-aggregation query json", func() {
			jsonStr := testNonAggQuery
			r := strings.NewReader(jsonStr)
			query, err := newQueryFromReader(r)
			So(err, ShouldBeNil)

			Convey("You can do a Search", func() {
				result, err := client.Search(query)
				So(err, ShouldBeNil)
				So(result, ShouldNotBeNil)
				So(result.Aggregations, ShouldBeNil)
				So(len(result.HitSet.Hits), ShouldEqual, 0)
				So(result.HitSet.Total.Value, ShouldEqual, expectedNumHits)
			})

			Convey("You can do a Scroll which always returns all hits", func() {
				hitsReceieved := 0
				cb := func(hit *Hit) {
					hitsReceieved++
				}

				result, err := client.Scroll(query, cb)
				So(err, ShouldBeNil)
				So(client.Error, ShouldBeNil)
				So(result, ShouldNotBeNil)
				So(result.Aggregations, ShouldBeNil)
				So(len(result.HitSet.Hits), ShouldEqual, 0)
				So(hitsReceieved, ShouldEqual, expectedNumHits)
				So(result.HitSet.Total.Value, ShouldEqual, expectedNumHits)
			})

			Convey("Search results change based on size and source", func() {
				query.Size = MaxSize
				result, err := client.Search(query)
				So(err, ShouldBeNil)
				So(result, ShouldNotBeNil)
				So(result.Aggregations, ShouldBeNil)
				So(len(result.HitSet.Hits), ShouldEqual, expectedNumHits)
				So(result.HitSet.Total.Value, ShouldEqual, expectedNumHits)
				So(result.HitSet.Hits[0].Details.AccountingName, ShouldEqual, "pathdev")
				So(result.HitSet.Hits[0].Details.UserName, ShouldEqual, "pathpipe")
				So(result.HitSet.Hits[0].Details.QueueName, ShouldEqual, "transfer")

				result.HitSet.Hits = []Hit{result.HitSet.Hits[0]}

				jsonBytes, err := result.MarshalFields(query.DesiredFields())
				So(err, ShouldBeNil)
				So(string(jsonBytes), ShouldContainSubstring, "ACCOUNTING_NAME")
				So(string(jsonBytes), ShouldContainSubstring, "USER_NAME")

				query.Source = []string{"USER_NAME", "QUEUE_NAME"}
				result, err = client.Search(query)
				So(err, ShouldBeNil)
				So(result.Aggregations, ShouldBeNil)
				So(len(result.HitSet.Hits), ShouldEqual, expectedNumHits)
				So(result.HitSet.Hits[0].Details.AccountingName, ShouldBeBlank)
				So(result.HitSet.Hits[0].Details.UserName, ShouldNotBeBlank)
				So(result.HitSet.Hits[0].Details.QueueName, ShouldNotBeBlank)

				result.HitSet.Hits = []Hit{result.HitSet.Hits[0]}

				jsonBytes, err = result.MarshalFields(query.DesiredFields())
				So(err, ShouldBeNil)
				So(string(jsonBytes), ShouldNotContainSubstring, "ACCOUNTING_NAME")
				So(string(jsonBytes), ShouldContainSubstring, "USER_NAME")
			})
		})

		Convey("And given an elasticsearch non-aggregation query json with more hits than max", func() {
			jsonStr := testScollQueryManyHits
			r := strings.NewReader(jsonStr)
			query, err := newQueryFromReader(r)
			So(err, ShouldBeNil)

			Convey("You can do a Scroll which auto-scrolls", func() {
				hitsReceieved := 0
				cb := func(hit *Hit) {
					hitsReceieved++
				}

				result, err := client.Scroll(query, cb)
				So(err, ShouldBeNil)
				So(result, ShouldNotBeNil)
				So(result.Aggregations, ShouldBeNil)
				So(len(result.HitSet.Hits), ShouldEqual, 0)
				So(hitsReceieved, ShouldEqual, testScrollManyHitsNum)
				So(result.HitSet.Total.Value, ShouldEqual, testScrollManyHitsNum)
			})
		})
	})
}

func TestElasticSearchClientReal(t *testing.T) {
	host := os.Getenv("FARMER_TEST_HOST")
	username := os.Getenv("FARMER_TEST_USERNAME")
	password := os.Getenv("FARMER_TEST_PASSWORD")
	scheme := os.Getenv("FARMER_TEST_SCHEME")
	portStr := os.Getenv("FARMER_TEST_PORT")
	index := os.Getenv("FARMER_TEST_INDEX")

	if host == "" || username == "" || password == "" || scheme == "" || portStr == "" || index == "" {
		SkipConvey("Skipping real elasticsearch client tests without FARMER_TEST_* env vars set", t, func() {})

		return
	}

	port, err := strconv.Atoi(portStr)
	if err != nil {
		SkipConvey("Skipping real elasticsearch client tests since FARMER_TEST_PORT was not a number", t, func() {})

		return
	}

	Convey("Given some config", t, func() {
		config := Config{
			Host:     host,
			Username: username,
			Password: password,
			Scheme:   scheme,
			Port:     port,
			Index:    index,
		}

		doClientTests(t, config, 403)
	})
}
