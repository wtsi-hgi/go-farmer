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

package cmd

import (
	"net"
	"net/url"
	"os"
	"strconv"

	"github.com/wtsi-hgi/go-farmer/db"
	es "github.com/wtsi-hgi/go-farmer/elasticsearch"
	"gopkg.in/yaml.v3"
)

const (
	defaultCacheEntries = 128
)

type YAMLConfig struct {
	Elastic struct {
		Host     string
		Username string
		Password string
		Scheme   string
		Port     int
		Index    string
	}
	Farmer struct {
		Host         string
		Port         int
		DatabaseDir  string `yaml:"database_dir"`
		FileSize     int    `yaml:"file_size"`
		BufferSize   int    `yaml:"buffer_size"`
		CacheEntries int    `yaml:"cache_entries"`
		PoolSize     int    `yaml:"pool_size"`
	}
}

func ParseConfig() *YAMLConfig {
	if configPath == "" {
		die("you must supply a config file with -c")
	}

	data, err := os.ReadFile(configPath)
	if err != nil {
		die("missing config file: %s", err)
	}

	c := &YAMLConfig{}

	err = yaml.Unmarshal(data, &c)
	if err != nil {
		die("invalid config file: %s", err)
	}

	return c
}

func (c *YAMLConfig) ToESConfig() es.Config {
	return es.Config{
		Host:     c.Elastic.Host,
		Port:     c.Elastic.Port,
		Scheme:   c.Elastic.Scheme,
		Username: c.Elastic.Username,
		Password: c.Elastic.Password,
		Index:    c.Elastic.Index,
	}
}

func (c *YAMLConfig) ToDBConfig() db.Config {
	return db.Config{
		Directory:  c.Farmer.DatabaseDir,
		FileSize:   c.Farmer.FileSize,
		BufferSize: c.Farmer.BufferSize,
		PoolSize:   c.Farmer.PoolSize,
	}
}

func (c *YAMLConfig) CacheEntries() int {
	if c.Farmer.CacheEntries > 0 {
		return c.Farmer.CacheEntries
	}

	return defaultCacheEntries
}

func (c *YAMLConfig) ElasticURL() *url.URL {
	return &url.URL{
		Host:   net.JoinHostPort(c.Elastic.Host, strconv.Itoa(c.Elastic.Port)),
		Scheme: c.Elastic.Scheme,
	}
}

func (c *YAMLConfig) FarmerHostPort() string {
	return net.JoinHostPort(c.Farmer.Host, strconv.Itoa(c.Farmer.Port))
}
