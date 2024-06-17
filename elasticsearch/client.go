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
	"fmt"
	"net/http"

	es "github.com/elastic/go-elasticsearch/v7"
)

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

	// bodyBytes, _ := io.ReadAll(resp.Body)
	// fmt.Println(string(bodyBytes))

	info := &ElasticInfo{}

	if err := json.NewDecoder(resp.Body).Decode(info); err != nil {
		return nil, err
	}

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
