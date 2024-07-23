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
	"context"
	"encoding/json"
	"log/slog"
	"time"

	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/mailru/easyjson"
	es "github.com/wtsi-hgi/go-farmer/elasticsearch"
)

const (
	cacheKeyPrefixResults = "r."
	cacheKeyPrefixStrings = "s."
	hoursInDay            = 24
)

// Searcher types have a Search function for querying something like elastic
// search.
type Searcher interface {
	Search(query *es.Query) (*es.Result, error)
}

// Scroller types have a Scroll function for querying something like elastic
// search, automatically getting all hits in a single scroll call. They have a
// corresponding Done() function which takes the same Scroll query to release
// any resources associated with doing the Scroll. They also have a Usernames
// function that returns just the usernames from the hits.
type Scroller interface {
	Scroll(query *es.Query) (*es.Result, error)
	Done(query *es.Query) bool
	Usernames(query *es.Query) ([]string, error)
}

type querier func(query *es.Query) ([]byte, error)

// CachedQuerier is an LRU cache wrapper around a Searcher and a Scroller that
// stores and returns their Results as JSON.
type CachedQuerier struct {
	Searcher Searcher
	Scroller Scroller
	lru      *lru.Cache[string, []byte]
}

// New returns a CachedQuerier that takes a Searcher and a Scroller. It caches
// cacheSize Search() and Scroll() queries, evicting the least recently used
// query results once the cache is full. It stores and returns JSON encoding of
// the Results.
func New(searcher Searcher, scroller Scroller, cacheSize int) (*CachedQuerier, error) {
	l, err := lru.New[string, []byte](cacheSize)
	if err != nil {
		return nil, err
	}

	return &CachedQuerier{
		Searcher: searcher,
		Scroller: scroller,
		lru:      l,
	}, nil
}

// Search returns any cached data for the given query, otherwise returns the
// JSON result of calling our Searcher.Search().
func (c *CachedQuerier) Search(query *es.Query) ([]byte, error) {
	return c.wrapWithCache(cacheKeyPrefixResults, query, c.searchQuerier)
}

func (c *CachedQuerier) wrapWithCache(keyPrefix string, query *es.Query, querier querier) ([]byte, error) {
	cacheKey := keyPrefix + query.Key()

	jsonBytes, ok := c.lru.Get(cacheKey)
	if ok {
		return jsonBytes, nil
	}

	jsonBytes, err := querier(query)
	if err != nil {
		return nil, err
	}

	c.lru.Add(cacheKey, jsonBytes)

	return jsonBytes, nil
}

func (c *CachedQuerier) searchQuerier(query *es.Query) ([]byte, error) {
	t := time.Now()

	result, err := c.Searcher.Search(query)
	if err != nil {
		return nil, err
	}

	items := 0
	if result.Aggregations != nil {
		items = len(result.Aggregations.Stats.Buckets)
	}

	logQuery(t, items, query, "search")

	return resultToJSON(result, query)
}

func logQuery(start time.Time, items int, query *es.Query, kind string) {
	if !slog.Default().Enabled(context.Background(), slog.LevelDebug) {
		return
	}

	attrs := []slog.Attr{
		slog.Any("took", time.Since(start)),
		slog.Int("items", items),
	}

	daysAttr, err := queryToDaysAttr(query)
	if err == nil {
		attrs = append(attrs, daysAttr)
	}

	attrs = append(attrs, queryToFilterAttrs(query)...)

	slog.LogAttrs(context.Background(), slog.LevelDebug, kind+" query", attrs...)
}

func queryToDaysAttr(query *es.Query) (slog.Attr, error) {
	lt, lte, gte, err := query.DateRange()
	if err != nil {
		return slog.Attr{}, err
	}

	end := lt
	if lt.IsZero() {
		end = lte
	}

	return slog.Int("days", int(end.Sub(gte).Hours()/hoursInDay)), nil
}

func queryToFilterAttrs(query *es.Query) []slog.Attr {
	var attrs []slog.Attr //nolint:prealloc

	filters := query.Filters()
	for _, filter := range []string{"BOM", "ACCOUNTING_NAME", "USER_NAME"} {
		val, set := filters[filter]
		if set {
			attrs = append(attrs, slog.String(filter, val))
			delete(filters, filter)
		}
	}

	delete(filters, "META_CLUSTER_NAME")

	for k, v := range filters {
		attrs = append(attrs, slog.String(k, v))
	}

	return attrs
}

func resultToJSON(result *es.Result, query *es.Query) ([]byte, error) {
	t := time.Now()
	jsonBytes, err := result.MarshalFields(query.DesiredFields())
	if err != nil {
		return nil, err
	}

	slog.Debug("json.Marshal of Result", "took", time.Since(t))

	return jsonBytes, err
}

// Scroll returns any cached data for the given query, otherwise returns the
// JSON result of calling our Scroller.Scroll().
func (c *CachedQuerier) Scroll(query *es.Query) ([]byte, error) {
	return c.wrapWithCache(cacheKeyPrefixResults, query, c.scrollQuerier)
}

func (c *CachedQuerier) scrollQuerier(query *es.Query) ([]byte, error) {
	t := time.Now()

	result, err := c.Scroller.Scroll(query)
	if err != nil {
		return nil, err
	}

	logQuery(t, len(result.HitSet.Hits), query, "scroll")

	return resultToJSON(result, query)
}

// Done calls our Scroller.Done().
func (c *CachedQuerier) Done(query *es.Query) bool {
	return c.Scroller.Done(query)
}

// Usernames returns any cached slice for the given query, otherwise returns
// the slice from calling our Scroller.Usernames().
func (c *CachedQuerier) Usernames(query *es.Query) ([]byte, error) {
	return c.wrapWithCache(cacheKeyPrefixStrings, query, c.usernameQuerier)
}

func (c *CachedQuerier) usernameQuerier(query *es.Query) ([]byte, error) {
	t := time.Now()

	usernames, err := c.Scroller.Usernames(query)
	if err != nil {
		return nil, err
	}

	logQuery(t, len(usernames), query, "usernames")

	return stringsToJSON(usernames)
}

func stringsToJSON(strs []string) ([]byte, error) {
	t := time.Now()
	jsonBytes, err := json.Marshal(strs)
	if err != nil {
		return nil, err
	}

	slog.Debug("json.Marshal of usernames", "took", time.Since(t))

	return jsonBytes, err
}

// Decode takes the output of CachedQuerier.Search() or Scroll() and turns it
// back in to a Result.
func Decode(data []byte) (*es.Result, error) {
	t := time.Now()
	result := &es.Result{}

	err := easyjson.Unmarshal(data, result)
	if err != nil {
		return nil, err
	}

	slog.Debug("json.Unmarshal", "took", time.Since(t))

	return result, err
}
