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
	lru "github.com/hashicorp/golang-lru/v2"
	es "github.com/wtsi-hgi/go-farmer/elasticsearch"
)

// Searcher types have a Search function for querying something like elastic
// search.
type Searcher interface {
	Search(index string, query *es.Query) (*es.Result, error)
}

// Scroller types have a Scroll function for querying something like elastic
// search, automatically getting all hits in a single scroll call.
type Scroller interface {
	Scroll(index string, query *es.Query) (*es.Result, error)
}

type querier func(index string, query *es.Query) (*es.Result, error)

// CachedQuerier is an LRU cache wrapper around a Searcher and a Scroller.
type CachedQuerier struct {
	Searcher Searcher
	Scroller Scroller
	lru      *lru.Cache[string, *es.Result]
}

// New returns a CachedQuerier that both takes and is itself a Searcher and a
// Scroller. It caches cacheSize Search() and Scroll() queries, evicting the
// least recently used query results once the cache is full.
func New(searcher Searcher, scroller Scroller, cacheSize int) (*CachedQuerier, error) {
	l, err := lru.New[string, *es.Result](cacheSize)
	if err != nil {
		return nil, err
	}

	return &CachedQuerier{
		Searcher: searcher,
		Scroller: scroller,
		lru:      l,
	}, nil
}

// Search returns any cached result for the given query, otherwise returns the
// result of calling our Searcher.Search().
func (c *CachedQuerier) Search(index string, query *es.Query) (*es.Result, error) {
	return c.wrapWithCache(c.Searcher.Search, index, query)
}

func (c *CachedQuerier) wrapWithCache(querier querier, index string, query *es.Query) (*es.Result, error) {
	cacheKey := query.Key()

	result, ok := c.lru.Get(cacheKey)
	if ok {
		return result, nil
	}

	result, err := querier(index, query)
	if err != nil {
		return nil, err
	}

	c.lru.Add(cacheKey, result)

	return result, nil
}

// Scroll returns any cached result for the given query, otherwise returns the
// result of calling our Scroller.Scroll().
func (c *CachedQuerier) Scroll(index string, query *es.Query) (*es.Result, error) {
	return c.wrapWithCache(c.Scroller.Scroll, index, query)
}
