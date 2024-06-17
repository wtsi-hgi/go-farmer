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
	"io"
)

const MaxSize = 10000

type Query struct {
	Size   int          `json:"size"`
	Aggs   *Aggs        `json:"aggs,omitempty"`
	Query  *QueryFilter `json:"query,omitempty"`
	Sort   []string     `json:"sort,omitempty"`
	Source []string     `json:"_source,omitempty"`
}

type Aggs struct {
	Stats AggsStats `json:"stats"`
}

type AggsStats struct {
	MultiTerms MultiTerms           `json:"multi_terms"`
	Aggs       map[string]AggsField `json:"aggs"`
}

type MultiTerms struct {
	Terms []Field `json:"terms"`
	Size  int     `json:"size"`
}

type Field struct {
	Field string `json:"field"`
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

type QueryFilter struct {
	Bool QFBool `json:"bool"`
}

type QFBool struct {
	Filter Filter `json:"filter"`
}

type Filter []map[string]map[string]interface{}

func NewQuery(raw io.Reader) (*Query, error) {
	query := &Query{}
	err := json.NewDecoder(raw).Decode(query)

	return query, err
}

func (q *Query) AsBody() (*bytes.Reader, error) {
	queryBytes, err := json.Marshal(q)
	if err != nil {
		return nil, err
	}

	return bytes.NewReader(queryBytes), nil
}
