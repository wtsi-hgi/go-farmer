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
	"bytes"
	"encoding/json"
	"io"

	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/klauspost/compress/gzip"
	es "github.com/wtsi-hgi/go-farmer/elasticsearch"
)

// Searcher types have a Search function for querying something like elastic
// search.
type Searcher interface {
	Search(query *es.Query) (*es.Result, error)
}

// Scroller types have a Scroll function for querying something like elastic
// search, automatically getting all hits in a single scroll call.
type Scroller interface {
	Scroll(query *es.Query) (*es.Result, error)
}

type querier func(query *es.Query) (*es.Result, error)

// CachedQuerier is an LRU cache wrapper around a Searcher and a Scroller that
// stores and returns their Results as compressed JSON.
type CachedQuerier struct {
	Searcher Searcher
	Scroller Scroller
	lru      *lru.Cache[string, []byte]
}

// New returns a CachedQuerier that takes a Searcher and a Scroller. It caches
// cacheSize Search() and Scroll() queries, evicting the least recently used
// query results once the cache is full. It stores and returns compressed JSON
// of the Results.
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
// compressed JSON result of calling our Searcher.Search().
func (c *CachedQuerier) Search(query *es.Query) ([]byte, error) {
	return c.wrapWithCache(c.Searcher.Search, query)
}

func (c *CachedQuerier) wrapWithCache(querier querier, query *es.Query) ([]byte, error) {
	cacheKey := query.Key()

	compressed, ok := c.lru.Get(cacheKey)
	if ok {
		return compressed, nil
	}

	result, err := querier(query)
	if err != nil {
		return nil, err
	}

	compressed, err = compress(result)
	if err != nil {
		return nil, err
	}

	c.lru.Add(cacheKey, compressed)

	return compressed, nil
}

func compress(result *es.Result) ([]byte, error) {
	j, err := json.Marshal(result)
	if err != nil {
		return nil, err
	}

	buf := bytes.Buffer{}
	gz := gzip.NewWriter(&buf)

	_, err = gz.Write(j)

	gz.Close()

	return buf.Bytes(), err
}

// Scroll returns any cached data for the given query, otherwise returns the
// compressed JSON result of calling our Scroller.Scroll().
func (c *CachedQuerier) Scroll(query *es.Query) ([]byte, error) {
	return c.wrapWithCache(c.Scroller.Scroll, query)
}

// Decompress takes the output of CachedQuerier.Search() or Scroll() and turns
// it back in to a Result.
func Decompress(data []byte) (*es.Result, error) {
	gr, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, err
	}

	data, err = io.ReadAll(gr)
	if err != nil {
		return nil, err
	}

	gr.Close()

	result := &es.Result{}
	err = json.Unmarshal(data, result)

	return result, err
}
