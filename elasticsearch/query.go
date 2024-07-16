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
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/dgryski/go-farm"
)

const (
	ErrNoTimestampRange = "no timestamp range found"
	MaxSize             = 10000
	SearchPage          = "_search"
)

// Query describes the search query you wish to run against Elastic Search.
type Query struct {
	Size           int          `json:"size"`
	Aggs           *Aggs        `json:"aggs,omitempty"`
	Query          *QueryFilter `json:"query,omitempty"`
	Sort           []string     `json:"sort,omitempty"`
	Source         []string     `json:"_source,omitempty"`
	ScrollParamSet bool         `json:"_scroll,omitempty"`
}

// DesiredFields returns a map with keys corresponding to Source values, and
// values of true.
func (q *Query) DesiredFields() map[string]bool {
	df := make(map[string]bool, len(q.Source))

	for _, field := range q.Source {
		df[field] = true
	}

	return df
}

// Aggs is used to specify an aggregation query.
type Aggs struct {
	Stats interface{} `json:"stats"`
}

type AggsStats struct {
	MultiTerms *MultiTerms          `json:"multi_terms,omitempty"`
	Terms      *Field               `json:"terms,omitempty"`
	Aggs       map[string]AggsField `json:"aggs,omitempty"`
}

type MultiTerms struct {
	Terms []Field `json:"terms"`
	Size  int     `json:"size"`
}

type Field struct {
	Field string `json:"field"`
	Size  int    `json:"size,omitempty"`
}

type AggsField struct {
	Sum            *Field          `json:"sum,omitempty"`
	ScriptedMetric *ScriptedMetric `json:"scripted_metric,omitempty"`
}

type ScriptedMetric struct {
	InitScript    string      `json:"init_script"`
	MapScript     string      `json:"map_script"`
	CombineScript string      `json:"combine_script"`
	ReduceScript  string      `json:"reduce_script"`
	Params        interface{} `json:"params"`
}

// QueryFilter is used to filter the documents you're interested in.
type QueryFilter struct {
	Bool QFBool `json:"bool"`
}

type QFBool struct {
	Filter Filter `json:"filter"`
}

type MapStringStringOrMap map[string]interface{}

// GetMapString returns the string value of the given subKey of the map with the
// given key in this map. Returns blank string if the keys didn't exist, or the
// value wasn't a string.
func (m MapStringStringOrMap) GetMapString(key, subKey string) string {
	keyInterface, ok := m[key]
	if !ok {
		return ""
	}

	keyInterfaceMap, ok := keyInterface.(map[string]interface{})
	if ok { //nolint:nestif
		subKeyInterface, ok2 := keyInterfaceMap[subKey]
		if !ok2 {
			return ""
		}

		subKeyStr, ok2 := subKeyInterface.(string)
		if !ok2 {
			return ""
		}

		return subKeyStr
	}

	subKeyInterface, ok := keyInterface.(map[string]string)
	if !ok {
		return ""
	}

	return subKeyInterface[subKey]
}

type Filter []map[string]MapStringStringOrMap

// NewQuery looks at the given Request method, path, body and parameters to see
// if it's a search request, and converts it to a Query if so. The booleon will
// be false if not.
func NewQuery(req *http.Request) (*Query, bool) {
	if req.Method != http.MethodPost {
		return nil, false
	}

	path := filepath.Base(req.URL.Path)
	if path != SearchPage {
		return nil, false
	}

	if req.Body == nil {
		return nil, false
	}

	query, err := newQueryFromReader(req.Body)
	if err != nil {
		return nil, false
	}

	query.handleRequestParams((req.URL.Query()))

	return query, true
}

func newQueryFromReader(raw io.Reader) (*Query, error) {
	query := &Query{}
	err := json.NewDecoder(raw).Decode(query)

	return query, err
}

func (q *Query) handleRequestParams(parms url.Values) {
	sizeParam := parms.Get("size")
	if sizeParam != "" {
		size, err := strconv.Atoi(sizeParam)
		if err == nil {
			q.Size = size
		}
	}

	sourceParam := parms.Get("_source")
	if sourceParam != "" {
		q.Source = strings.Split(sourceParam, ",")
	}

	scrollParam := parms.Get("scroll")
	if scrollParam != "" {
		q.ScrollParamSet = true
	}
}

// IsScroll returns true if the http.Request this Query was made from had a
// scroll parameter.
func (q *Query) IsScroll() bool {
	return q.ScrollParamSet
}

// Key returns a string that is unique to this Query.
func (q *Query) Key() string {
	queryBytes, _ := json.Marshal(q) //nolint:errcheck,errchkjson
	l, h := farm.Hash128(queryBytes)

	return fmt.Sprintf("%016x%016x", l, h)
}

func (q *Query) asBody() (*bytes.Reader, error) {
	queryBytes, err := json.Marshal(q)
	if err != nil {
		return nil, err
	}

	return bytes.NewReader(queryBytes), nil
}

// DateRange looks at the query's range->timestamp and returns the lt, lte and
// gte values. Returns an error if none were found.
func (q *Query) DateRange() (lt, lte, gte time.Time, err error) { //nolint:gocognit,funlen,gocyclo
	for _, val := range q.Query.Bool.Filter {
		fRange, ok := val["range"]
		if !ok {
			continue
		}

		ltStr := fRange.GetMapString("timestamp", "lt")
		lteStr := fRange.GetMapString("timestamp", "lte")

		if ltStr == "" && lteStr == "" {
			continue
		}

		if ltStr != "" { //nolint:nestif
			lt, err = time.Parse(time.RFC3339, ltStr)
			if err != nil {
				return lt, lte, gte, err
			}
		} else {
			lte, err = time.Parse(time.RFC3339, lteStr)
			if err != nil {
				return lt, lte, gte, err
			}
		}

		gteStr := fRange.GetMapString("timestamp", "gte")
		if gteStr == "" {
			continue
		}

		gte, err = time.Parse(time.RFC3339, gteStr)
		if err != nil {
			return lt, lte, gte, err
		}

		return lt, lte, gte, err
	}

	return lt, lte, gte, Error{Msg: ErrNoTimestampRange}
}

// Filters returns the match_phrase and prefix key value pairs found in the
// query's filter. Returns an empty map if none found.
func (q *Query) Filters() map[string]string {
	filters := make(map[string]string)

	for _, val := range q.Query.Bool.Filter {
		addKeyValsToFilters(val, "match_phrase", filters)
		addKeyValsToFilters(val, "prefix", filters)
	}

	return filters
}

func addKeyValsToFilters(val map[string]MapStringStringOrMap, key string, filters map[string]string) {
	thisMap, ok := val[key]
	if !ok {
		return
	}

	for key, val := range thisMap {
		valStr, ok := val.(string)
		if !ok {
			continue
		}

		filters[key] = valStr
	}
}
