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
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"strconv"
	"strings"
)

const (
	testAggQuery         = `{"aggs":{"stats":{"multi_terms":{"terms":[{"field":"ACCOUNTING_NAME"},{"field":"NUM_EXEC_PROCS"},{"field":"Job"}],"size":1000},"aggs":{"cpu_avail_sec":{"sum":{"field":"AVAIL_CPU_TIME_SEC"}},"cpu_wasted_sec":{"sum":{"field":"WASTED_CPU_SECONDS"}}}}},"size":0,"query":{"bool":{"filter":[{"match_phrase":{"META_CLUSTER_NAME":"farm"}},{"range":{"timestamp":{"lte":"2024-05-04T00:10:00Z","gte":"2024-05-04T00:00:00Z","format":"strict_date_optional_time"}}}]}}}` //nolint:lll
	testAggQueryResponse = `{
		"hits": {
			"total":{"value":2}
		},
		"aggregations": {
			"stats": {
				"buckets": [
					{"key_as_string": "a"},
					{"key_as_string": "b"},
					{"key_as_string": "c"},
					{"key_as_string": "d"},
					{"key_as_string": "e"},
					{"key_as_string": "f"}
				]
			}
		}
	}`

	testNonAggQuery                 = `{"query":{"bool":{"filter":[{"match_phrase":{"META_CLUSTER_NAME":"farm"}},{"range":{"timestamp":{"lte":"2024-05-04T00:10:00Z","gte":"2024-05-04T00:00:00Z","format":"strict_date_optional_time"}}}]}}}` //nolint:lll
	testNonAggQueryResponseZeroSize = `{
		"hits": {
			"total":{"value":2},
			"hits": []
		}
	}`
	testNonAggQueryResponseSize = `{
		"hits": {
			"total":{"value":2},
			"hits": [
				{"_id": "1", "_source": { "ACCOUNTING_NAME": "pathdev", "USER_NAME": "pathpipe", "QUEUE_NAME": "transfer" } },
                {"_id": "2", "_source": { "ACCOUNTING_NAME": "a2", "USER_NAME": "u2", "QUEUE_NAME": "q2" } }
			]
		}
	}`
	testNonAggQueryResponse0529 = `{"hits": {}}`
	testNonAggQueryResponse0530 = `{
		"hits": {
			"total":{"value":2},
			"hits": [
				{"_id": "1", "_source": {
					"BOM": "Human Genetics",
					"ACCOUNTING_NAME": "pathdev",
					"USER_NAME": "pathpipe",
					"QUEUE_NAME": "transfer",
					"timestamp": 1717027200 } }
			]
		}
	}`
	testNonAggQueryResponse0531 = `{
		"hits": {
			"total":{"value":2},
			"hits": [
				{"_id": "2", "_source": {
					"BOM": "Human Genetics",
					"ACCOUNTING_NAME": "a2",
					"USER_NAME": "u2",
					"QUEUE_NAME": "q2",
					"timestamp": 1717113600 } }
			]
		}
	}`
	testNonAggQueryResponseSizeSources = `{
		"hits": {
			"total":{"value":2},
			"hits": [
				{"_id": "1", "_source": { "USER_NAME": "pathpipe", "QUEUE_NAME": "transfer" } },
				{"_id": "2", "_source": { "USER_NAME": "u2", "QUEUE_NAME": "q2" } }
			]
		}
	}`

	testScollQueryManyHits = `{"size":10000,"query":{"bool":{"filter":[{"match_phrase":{"META_CLUSTER_NAME":"farm"}},{"range":{"timestamp":{"lte":"2024-05-04T00:00:00Z","gte":"2024-05-03T15:00:00Z","format":"strict_date_optional_time"}}}]}}}` //nolint:lll
	testScrollManyHitsNum  = 23581

	mockVersionJSON     = `{"version":{"number":"7.17.6"}}`
	testExpectedVersion = "7.17.6"
	mockPort            = 1234
)

var scrollHitsReturned = 0 //nolint:gochecknoglobals

type mockTransport struct{}

func (m mockTransport) RoundTrip(req *http.Request) (*http.Response, error) { //nolint:funlen,gocognit,gocyclo,cyclop
	jsonStr := mockVersionJSON

	if req.Body != nil { //nolint:nestif
		scrollParam := req.URL.Query().Get("scroll")
		scrollRequest := filepath.Base(req.URL.Path) == "scroll"
		query := &Query{}

		if err := json.NewDecoder(req.Body).Decode(query); err != nil {
			return nil, err
		}

		if scrollRequest { //nolint:gocritic
			jsonStr = m.scrollHits(req.Method == http.MethodPost)
		} else if query.Aggs != nil {
			jsonStr = testAggQueryResponse
		} else {
			if query.Size == 0 && scrollParam == "" {
				jsonStr = testNonAggQueryResponseZeroSize
			} else {
				if len(query.Source) == 0 {
					gte := ""

					if query.Query != nil && len(query.Query.Bool.Filter) > 1 {
						frange, found := query.Query.Bool.Filter[1]["range"]
						if found {
							timeStampFilter, ok := frange["timestamp"].(map[string]interface{})
							if ok {
								gteStr, ok := timeStampFilter["gte"].(string)
								if ok {
									gte = gteStr
								}
							}
						}
					}

					if gte == "2024-05-03T15:00:00Z" {
						jsonStr = m.scrollHits(req.Method == http.MethodPost)
					} else if gte == "2024-05-29T00:00:00Z" {
						jsonStr = testNonAggQueryResponse0529
					} else if gte == "2024-05-30T00:00:00Z" {
						jsonStr = testNonAggQueryResponse0530
					} else if gte == "2024-05-31T00:00:00Z" {
						jsonStr = testNonAggQueryResponse0531
					} else {
						jsonStr = testNonAggQueryResponseSize
					}
				} else {
					jsonStr = testNonAggQueryResponseSizeSources
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

func (m mockTransport) scrollHits(wasPost bool) string {
	if !wasPost {
		return `{
			"hits": {}
		}`
	}

	hitsToReturn := testScrollManyHitsNum - scrollHitsReturned
	if hitsToReturn > MaxSize {
		hitsToReturn = MaxSize
		scrollHitsReturned += hitsToReturn
	} else {
		scrollHitsReturned = 0
	}

	return `{
		"hits": {
			"total":{"value":` + strconv.Itoa(testScrollManyHitsNum) + `},
			"hits": [` +
		strings.Repeat(`{"_id": "id", "_source": { "USER_NAME": "u" } },`, hitsToReturn-1) +
		`
				{"_id": "id", "_source": { "USER_NAME": "u" } }
			]
		}
	}`
}

// Mock helps you test elasticsearch queries without a real elasticsearch
// server.
type Mock struct {
	client *Client
	index  string
}

// NewMock returns a Mock that you can get a mock Client and queries from.
func NewMock(index string) *Mock {
	config := Config{
		Host:      "mock",
		Username:  "mock",
		Password:  "mock",
		Scheme:    "http",
		Port:      mockPort,
		Index:     index,
		transport: mockTransport{},
	}

	client, _ := NewClient(config) //nolint:errcheck

	return &Mock{client: client, index: url.QueryEscape(index)}
}

// Client returns a Client that talks to a mock Elastic Search server, useful
// for testing elastic search interactions with our queries.
func (m *Mock) Client() *Client {
	return m.client
}

// AggQuery returns a http.Request that is requesting an aggregation search.
func (m *Mock) AggQuery() *http.Request {
	req := httptest.NewRequest(http.MethodPost, "/"+m.index+"/"+SearchPage, strings.NewReader(testAggQuery))

	return req
}

// ScrollQuery returns a http.Request that is requesting a scroll search. Any
// args are appended to the search url. Also returns the number of matching
// documents (which will match the number of hits you get back if your args
// include the scroll parameter).
func (m *Mock) ScrollQuery(args string) (*http.Request, int) {
	req := httptest.NewRequest(http.MethodPost, "/"+m.index+"/"+SearchPage+args, strings.NewReader(testScollQueryManyHits))

	return req, testScrollManyHitsNum
}
