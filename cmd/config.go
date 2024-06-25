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
	"os"

	es "github.com/wtsi-hgi/go-farmer/elasticsearch"
	"gopkg.in/yaml.v3"
)

type YAMLConfig struct {
	Elastic struct {
		Host     string
		Username string
		Password string
		Scheme   string
		Port     int
	}
	Farmer struct {
		Host         string
		Scheme       string
		Port         string
		Index        string
		DatabaseDir  string `yaml:"database_dir"`
		FileSize     int    `yaml:"file_size"`
		BufferSize   int    `yaml:"buffer_size"`
		CacheEntries int    `yaml:"cache_entries"`
	}
}

func ParseConfig(path string) *YAMLConfig {
	data, err := os.ReadFile(path)
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
	}
}
