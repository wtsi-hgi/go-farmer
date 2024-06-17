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
	"encoding/json"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

const mockVersionJSON = `{"version":{"number":"7.17.6"}}`

type mockTransport struct{}

func (m mockTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	jsonStr := mockVersionJSON

	if req.Body != nil {
		query := &Query{}

		if err := json.NewDecoder(req.Body).Decode(query); err != nil {
			return nil, err
		}

		if query.Aggs != nil {
			jsonStr = `{
				"hits": {
					"total":{"value":2}
				},
				"aggregations": {
					"stats": {
						"buckets": [
							{"key_as_string": "a"},
							{"key_as_string": "b"},
							{"key_as_string": "c"}
						]
					}
				}
			}`
		} else {
			if query.Size == 0 {
				jsonStr = `{
					"hits": {
						"total":{"value":2},
						"hits": []
					},
					"aggregations": {}
				}`
			} else {
				if len(query.Source) == 0 {
					jsonStr = `{
						"hits": {
							"total":{"value":2},
							"hits": [
								{"_id": "1", "_source": { "ACCOUNTING_NAME": "a1", "USER_NAME": "u1", "QUEUE_NAME": "q1" } },
								{"_id": "2", "_source": { "ACCOUNTING_NAME": "a2", "USER_NAME": "u2", "QUEUE_NAME": "q2" } }
							]
						},
						"aggregations": {}
					}`
				} else {
					jsonStr = `{
						"hits": {
							"total":{"value":2},
							"hits": [
								{"_id": "1", "_source": { "USER_NAME": "u1", "QUEUE_NAME": "q1" } },
								{"_id": "2", "_source": { "USER_NAME": "u2", "QUEUE_NAME": "q2" } }
							]
						},
						"aggregations": {}
					}`
				}
			}
		}
	}

	return &http.Response{
		StatusCode: http.StatusOK,
		Body:       io.NopCloser(strings.NewReader(jsonStr)),
		Header:     http.Header{"X-Elastic-Product": []string{"Elasticsearch"}},
	}, nil
}

func TestElasticSearchClientMock(t *testing.T) {
	index := "mockindex"

	Convey("Given some config", t, func() {
		mockTrans := mockTransport{}

		config := Config{
			Host:      "mock",
			Username:  "mock",
			Password:  "mock",
			Scheme:    "http",
			Port:      1234,
			transport: mockTrans,
		}

		Convey("You can create an elasticsearch client", func() {
			client, err := NewClient(config)
			So(err, ShouldBeNil)
			So(client, ShouldNotBeNil)

			info, err := client.Info()
			So(err, ShouldBeNil)
			So(info.Version.Number, ShouldEqual, "7.17.6")

			expectedNumHits := 2

			Convey("And given an elasticsearch aggregation query json", func() {
				jsonStr := `{"aggs":{"stats":{"multi_terms":{"terms":[{"field":"ACCOUNTING_NAME"},{"field":"NUM_EXEC_PROCS"},{"field":"Job"}],"size":1000},"aggs":{"cpu_avail_sec":{"sum":{"field":"AVAIL_CPU_TIME_SEC"}},"cpu_wasted_sec":{"sum":{"field":"WASTED_CPU_SECONDS"}}}}},"size":0,"query":{"bool":{"filter":[{"match_phrase":{"META_CLUSTER_NAME":"farm"}},{"range":{"timestamp":{"lte":"2024-05-04T00:10:00Z","gte":"2024-05-04T00:00:00Z","format":"strict_date_optional_time"}}}]}}}`

				r := strings.NewReader(jsonStr)

				query, err := NewQuery(r)
				So(err, ShouldBeNil)

				Convey("You can do a search request", func() {
					result, err := client.Search(index, query)
					So(err, ShouldBeNil)
					So(result, ShouldNotBeNil)
					So(len(result.Aggregations.Stats.Buckets), ShouldEqual, 3)
					So(len(result.HitSet.Hits), ShouldEqual, 0)
					So(result.HitSet.Total.Value, ShouldEqual, expectedNumHits)
				})
			})

			Convey("And given an elasticsearch non-aggregation query json", func() {
				jsonStr := `{"query":{"bool":{"filter":[{"match_phrase":{"META_CLUSTER_NAME":"farm"}},{"range":{"timestamp":{"lte":"2024-05-04T00:10:00Z","gte":"2024-05-04T00:00:00Z","format":"strict_date_optional_time"}}}]}}}
`

				r := strings.NewReader(jsonStr)

				query, err := NewQuery(r)
				So(err, ShouldBeNil)

				Convey("You can do a search request", func() {
					result, err := client.Search(index, query)
					So(err, ShouldBeNil)
					So(result, ShouldNotBeNil)
					So(len(result.Aggregations.Stats.Buckets), ShouldEqual, 0)
					So(len(result.HitSet.Hits), ShouldEqual, 0)
					So(result.HitSet.Total.Value, ShouldEqual, expectedNumHits)
				})

				Convey("Search results change based on size and source", func() {
					query.Size = MaxSize
					result, err := client.Search(index, query)
					So(err, ShouldBeNil)
					So(result, ShouldNotBeNil)
					So(len(result.Aggregations.Stats.Buckets), ShouldEqual, 0)
					So(len(result.HitSet.Hits), ShouldEqual, expectedNumHits)
					So(result.HitSet.Total.Value, ShouldEqual, expectedNumHits)
					So(result.HitSet.Hits[0].Details.ACCOUNTING_NAME, ShouldEqual, "a1")
					So(result.HitSet.Hits[0].Details.USER_NAME, ShouldEqual, "u1")
					So(result.HitSet.Hits[0].Details.QUEUE_NAME, ShouldEqual, "q1")

					j, err := json.Marshal(result.HitSet.Hits[0].Details)
					So(err, ShouldBeNil)
					So(string(j), ShouldContainSubstring, "ACCOUNTING_NAME")
					So(string(j), ShouldContainSubstring, "USER_NAME")

					query.Source = []string{"USER_NAME", "QUEUE_NAME"}
					result, err = client.Search(index, query)
					So(err, ShouldBeNil)
					So(len(result.Aggregations.Stats.Buckets), ShouldEqual, 0)
					So(len(result.HitSet.Hits), ShouldEqual, expectedNumHits)
					So(result.HitSet.Hits[0].Details.ACCOUNTING_NAME, ShouldBeBlank)
					So(result.HitSet.Hits[0].Details.USER_NAME, ShouldNotBeBlank)
					So(result.HitSet.Hits[0].Details.QUEUE_NAME, ShouldNotBeBlank)

					j, err = json.Marshal(result.HitSet.Hits[0].Details)
					So(err, ShouldBeNil)
					So(string(j), ShouldNotContainSubstring, "ACCOUNTING_NAME")
					So(string(j), ShouldContainSubstring, "USER_NAME")
				})
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
		}

		Convey("You can create an elasticsearch client", func() {
			client, err := NewClient(config)
			So(err, ShouldBeNil)
			So(client, ShouldNotBeNil)

			info, err := client.Info()
			So(err, ShouldBeNil)
			So(info.Version.Number, ShouldEqual, "7.17.6")

			expectedNumHits := 403

			Convey("And given an elasticsearch aggregation query json", func() {
				jsonStr := `{"aggs":{"stats":{"multi_terms":{"terms":[{"field":"ACCOUNTING_NAME"},{"field":"NUM_EXEC_PROCS"},{"field":"Job"}],"size":1000},"aggs":{"cpu_avail_sec":{"sum":{"field":"AVAIL_CPU_TIME_SEC"}},"cpu_wasted_sec":{"sum":{"field":"WASTED_CPU_SECONDS"}}}}},"size":0,"query":{"bool":{"filter":[{"match_phrase":{"META_CLUSTER_NAME":"farm"}},{"range":{"timestamp":{"lte":"2024-05-04T00:10:00Z","gte":"2024-05-04T00:00:00Z","format":"strict_date_optional_time"}}}]}}}`

				r := strings.NewReader(jsonStr)

				query, err := NewQuery(r)
				So(err, ShouldBeNil)

				Convey("You can do a search request", func() {
					result, err := client.Search(index, query)
					So(err, ShouldBeNil)
					So(result, ShouldNotBeNil)
					So(len(result.Aggregations.Stats.Buckets), ShouldEqual, 6)
					So(len(result.HitSet.Hits), ShouldEqual, 0)
					So(result.HitSet.Total.Value, ShouldEqual, expectedNumHits)
				})
			})

			Convey("And given an elasticsearch non-aggregation query json", func() {
				jsonStr := `{"query":{"bool":{"filter":[{"match_phrase":{"META_CLUSTER_NAME":"farm"}},{"range":{"timestamp":{"lte":"2024-05-04T00:10:00Z","gte":"2024-05-04T00:00:00Z","format":"strict_date_optional_time"}}}]}}}
`

				r := strings.NewReader(jsonStr)

				query, err := NewQuery(r)
				So(err, ShouldBeNil)

				Convey("You can do a search request", func() {
					result, err := client.Search(index, query)
					So(err, ShouldBeNil)
					So(result, ShouldNotBeNil)
					So(len(result.Aggregations.Stats.Buckets), ShouldEqual, 0)
					So(len(result.HitSet.Hits), ShouldEqual, 0)
					So(result.HitSet.Total.Value, ShouldEqual, expectedNumHits)
				})

				Convey("Search results change based on size and source", func() {
					query.Size = MaxSize
					result, err := client.Search(index, query)
					So(err, ShouldBeNil)
					So(result, ShouldNotBeNil)
					So(len(result.Aggregations.Stats.Buckets), ShouldEqual, 0)
					So(len(result.HitSet.Hits), ShouldEqual, expectedNumHits)
					So(result.HitSet.Total.Value, ShouldEqual, expectedNumHits)
					So(result.HitSet.Hits[0].Details.ACCOUNTING_NAME, ShouldNotBeBlank)
					So(result.HitSet.Hits[0].Details.USER_NAME, ShouldNotBeBlank)
					So(result.HitSet.Hits[0].Details.QUEUE_NAME, ShouldNotBeBlank)

					j, err := json.Marshal(result.HitSet.Hits[0].Details)
					So(err, ShouldBeNil)
					So(string(j), ShouldContainSubstring, "ACCOUNTING_NAME")
					So(string(j), ShouldContainSubstring, "USER_NAME")

					query.Source = []string{"USER_NAME", "QUEUE_NAME"}
					result, err = client.Search(index, query)
					So(err, ShouldBeNil)
					So(len(result.Aggregations.Stats.Buckets), ShouldEqual, 0)
					So(len(result.HitSet.Hits), ShouldEqual, expectedNumHits)
					So(result.HitSet.Hits[0].Details.ACCOUNTING_NAME, ShouldBeBlank)
					So(result.HitSet.Hits[0].Details.USER_NAME, ShouldNotBeBlank)
					So(result.HitSet.Hits[0].Details.QUEUE_NAME, ShouldNotBeBlank)

					j, err = json.Marshal(result.HitSet.Hits[0].Details)
					So(err, ShouldBeNil)
					So(string(j), ShouldNotContainSubstring, "ACCOUNTING_NAME")
					So(string(j), ShouldContainSubstring, "USER_NAME")
				})
			})
		})
	})
}