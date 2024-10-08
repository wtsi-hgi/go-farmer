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
	"net/http"
	"time"

	es "github.com/elastic/go-elasticsearch/v7"
)

const scrollTime = 1 * time.Minute

// Config allows you to specify your Elastic Search server details. Currently
// only basic auth is supported, for an internal network server with "public"
// access.
type Config struct {
	Host      string
	Username  string
	Password  string
	Scheme    string
	Port      int
	Index     string
	transport http.RoundTripper
}

// Client is used to interact with an Elastic Search server.
type Client struct {
	index  string
	client *es.Client
	Error  error
}

// NewClient returns a Client that can talk to the configured Elastic Search
// server and will use the configured index for queries.
func NewClient(config Config) (*Client, error) {
	cfg := es.Config{
		Addresses: []string{
			fmt.Sprintf("%s://%s:%d", config.Scheme, config.Host, config.Port),
		},
		Username:  config.Username,
		Password:  config.Password,
		Transport: config.transport,
	}

	client, err := es.NewClient(cfg)

	return &Client{client: client, index: config.Index}, err
}

// ElasticInfo is the type returned by an Info() request. It just tells you the
// version number of the server.
type ElasticInfo struct {
	Version struct {
		Number string
	}
}

// Info tells you the version number info of the server.
func (c *Client) Info() (*ElasticInfo, error) {
	resp, err := c.client.Info()
	if err != nil {
		return nil, err
	}

	info := &ElasticInfo{}

	err = json.NewDecoder(resp.Body).Decode(info)

	return info, err
}

// Search uses our index and the given query to get back your desired search
// results. If there are more than 10,000 hits, you won't get them (use Scroll
// instead).
func (c *Client) Search(query *Query) (*Result, error) {
	qbody, err := query.asBody()
	if err != nil {
		return nil, err
	}

	resp, err := c.client.Search(
		c.client.Search.WithIndex(c.index),
		c.client.Search.WithBody(qbody),
	)
	if err != nil {
		return nil, err
	}

	result, _, err := parseResultResponse(resp, nil)

	return result, err
}

// Scroll uses our index and the given query to get back your desired search
// results. It auto-scrolls and returns all your hits via the given callback,
// and everything else in the returned Result.
func (c *Client) Scroll(query *Query, cb HitsCallBack) (*Result, error) {
	qbody, err := query.asBody()
	if err != nil {
		return nil, err
	}

	resp, err := c.client.Search(
		c.client.Search.WithIndex(c.index),
		c.client.Search.WithBody(qbody),
		c.client.Search.WithSize(MaxSize),
		c.client.Search.WithScroll(scrollTime),
	)
	if err != nil {
		return nil, err
	}

	result, n, err := parseResultResponse(resp, cb)
	if err != nil {
		return nil, err
	}

	defer c.scrollCleanup(result)

	err = c.scrollUntilAllHitsReceived(result, n, cb)

	return result, err
}

func (c *Client) scrollCleanup(result *Result) {
	scrollIDBody, err := scrollIDBody(result.ScrollID)
	if err != nil {
		c.Error = err

		return
	}

	_, err = c.client.ClearScroll(c.client.ClearScroll.WithBody(scrollIDBody))
	if err != nil {
		c.Error = err
	}
}

func scrollIDBody(scrollID string) (*bytes.Buffer, error) {
	scrollBytes, err := json.Marshal(&map[string]string{"scroll_id": scrollID})
	if err != nil {
		return nil, err
	}

	return bytes.NewBuffer(scrollBytes), nil
}

func (c *Client) scrollUntilAllHitsReceived(result *Result, previousNumHits int, cb HitsCallBack) error {
	total := result.HitSet.Total.Value
	if total <= MaxSize {
		return nil
	}

	for keepScrolling := true; keepScrolling; keepScrolling = previousNumHits < total {
		n, err := c.scroll(result, cb)
		if err != nil {
			return err
		}

		if n == 0 {
			break
		}

		previousNumHits += n
	}

	return nil
}

func (c *Client) scroll(result *Result, cb HitsCallBack) (int, error) {
	scrollIDBody, err := scrollIDBody(result.ScrollID)
	if err != nil {
		return 0, err
	}

	resp, err := c.client.Scroll(
		c.client.Scroll.WithBody(scrollIDBody),
		c.client.Scroll.WithScroll(scrollTime),
	)
	if err != nil {
		return 0, err
	}

	scrollResult, n, err := parseResultResponse(resp, cb)
	if err != nil {
		return 0, err
	}

	result.HitSet.Hits = append(result.HitSet.Hits, scrollResult.HitSet.Hits...)
	result.ScrollID = scrollResult.ScrollID

	return n, nil
}
