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

func TestElasticSearchClient(t *testing.T) {
	host := os.Getenv("FARMER_TEST_HOST")
	username := os.Getenv("FARMER_TEST_USERNAME")
	password := os.Getenv("FARMER_TEST_PASSWORD")
	scheme := os.Getenv("FARMER_TEST_SCHEME")
	portStr := os.Getenv("FARMER_TEST_PORT")
	index := os.Getenv("FARMER_TEST_INDEX")

	if host == "" || username == "" || password == "" || scheme == "" || portStr == "" || index == "" {
		SkipConvey("Skipping elasticsearch client test without FARMER_TEST_* env vars set", t, func() {})
		return
	}

	port, err := strconv.Atoi(portStr)
	if err != nil {
		SkipConvey("Skipping elasticsearch client test since FARMER_TEST_PORT was not a number", t, func() {})
		return
	}

	Convey("Given some config", t, func() {
		config := Config{
			Host:     host,
			Username: username,
			Password: password,
			Scheme:   scheme,
			Port:     port,
		}

		Convey("You can create an elasticsearch client", func() {
			client, err := NewClient(config)
			So(err, ShouldBeNil)
			So(client, ShouldNotBeNil)

			info, err := client.Info()
			So(err, ShouldBeNil)
			So(info.Version.Number, ShouldEqual, "7.17.6")

			Convey("And given an elasticsearch aggregation query json", func() {
				json := `{"aggs":{"stats":{"multi_terms":{"terms":[{"field":"ACCOUNTING_NAME"},{"field":"NUM_EXEC_PROCS"},{"field":"Job"}],"size":1000},"aggs":{"cpu_avail_sec":{"sum":{"field":"AVAIL_CPU_TIME_SEC"}},"cpu_wasted_sec":{"sum":{"field":"WASTED_CPU_SECONDS"}}}}},"size":0,"query":{"bool":{"filter":[{"match_phrase":{"META_CLUSTER_NAME":"farm"}},{"range":{"timestamp":{"lte":"2024-05-04T00:10:00Z","gte":"2024-05-04T00:00:00Z","format":"strict_date_optional_time"}}}]}}}`

				r := strings.NewReader(json)

				query, err := NewQuery(r)
				So(err, ShouldBeNil)

				Convey("You can do a search request", func() {
					result, err := client.Search(index, query)
					So(err, ShouldBeNil)
					So(result, ShouldNotBeNil)
					So(len(result.Aggregations.Stats.Buckets), ShouldEqual, 6)
					So(len(result.HitSet.Hits), ShouldEqual, 0)
					So(result.HitSet.Total.Value, ShouldEqual, 403)
				})
			})
		})
	})
}
