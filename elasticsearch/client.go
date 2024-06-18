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

type Config struct {
	Host      string
	Username  string
	Password  string
	Scheme    string
	Port      int
	transport http.RoundTripper
}

type Client struct {
	client *es.Client
	Error  error
}

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

	return &Client{client: client}, err
}

type ElasticInfo struct {
	Version struct {
		Number string
	}
}

func (c *Client) Info() (*ElasticInfo, error) {
	resp, err := c.client.Info()
	if err != nil {
		return nil, err
	}

	info := &ElasticInfo{}

	err = json.NewDecoder(resp.Body).Decode(info)

	return info, err
}

func (c *Client) Search(index string, query *Query) (*Result, error) {
	qbody, err := query.AsBody()
	if err != nil {
		return nil, err
	}

	resp, err := c.client.Search(
		c.client.Search.WithIndex(index),
		c.client.Search.WithBody(qbody),
	)
	if err != nil {
		return nil, err
	}

	return parseResultResponse(resp)
}

func (c *Client) Scroll(index string, query *Query) (*Result, error) {
	qbody, err := query.AsBody()
	if err != nil {
		return nil, err
	}

	resp, err := c.client.Search(
		c.client.Search.WithIndex(index),
		c.client.Search.WithBody(qbody),
		c.client.Search.WithSize(MaxSize),
		c.client.Search.WithScroll(scrollTime),
	)
	if err != nil {
		return nil, err
	}

	result, err := parseResultResponse(resp)
	if err != nil {
		return nil, err
	}

	defer c.scrollCleanup(result)

	err = c.scrollUntilAllHitsReceived(result)

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

func (c *Client) scrollUntilAllHitsReceived(result *Result) error {
	total := result.HitSet.Total.Value
	if total <= MaxSize {
		return nil
	}

	for keepScrolling := true; keepScrolling; keepScrolling = len(result.HitSet.Hits) < total {
		err := c.scroll(result)
		if err != nil {
			return err
		}
	}

	return nil
}

func (c *Client) scroll(result *Result) error {
	scrollIDBody, err := scrollIDBody(result.ScrollID)
	if err != nil {
		return err
	}

	resp, err := c.client.Scroll(
		c.client.Scroll.WithBody(scrollIDBody),
		c.client.Scroll.WithScroll(scrollTime),
	)
	if err != nil {
		return err
	}

	scrollResult, err := parseResultResponse(resp)
	if err != nil {
		return err
	}

	result.HitSet.Hits = append(result.HitSet.Hits, scrollResult.HitSet.Hits...)
	result.ScrollID = scrollResult.ScrollID

	return nil
}
