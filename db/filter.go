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

package db

import (
	"bytes"
	"strings"
	"time"

	es "github.com/wtsi-hgi/go-farmer/elasticsearch"
)

const ErrNoBOM = "query does not specify a BOM"

type flatFilter struct {
	BOM             string
	LT              time.Time
	LTE             time.Time
	GTE             time.Time
	LTKey           []byte
	LTEKey          []byte
	GTEKey          []byte
	accountingName  string
	userName        string
	checkAccounting bool
	checkUser       bool
	checkGPU        bool
	checkLTE        bool
}

func newFlatFilter(query *es.Query) (*flatFilter, error) {
	lt, lte, gte, err := query.DateRange()
	if err != nil {
		return nil, err
	}

	filter := &flatFilter{
		LT:       lt,
		LTE:      lte,
		GTE:      gte,
		checkLTE: !lte.IsZero(),
	}

	filter.LTKey, filter.LTEKey, filter.GTEKey = i64tob(lt.Unix()), i64tob(lte.Unix()), i64tob(gte.Unix())
	filter.BOM, filter.accountingName, filter.userName, filter.checkGPU = queryToFilters(query)

	if filter.BOM == "" {
		return nil, Error{Msg: ErrNoBOM}
	}

	filter.checkAccounting = len(filter.accountingName) > 0
	filter.checkUser = len(filter.userName) > 0

	return filter, nil
}

func (f *flatFilter) beyondLastDate(current time.Time) bool {
	if f.checkLTE {
		return current.After(f.LTE)
	}

	return current.Equal(f.LT) || current.After(f.LT)
}

func queryToFilters(query *es.Query) (bom, accountingName, userName string, checkGPU bool) {
	filters := query.Filters()

	bom = filters["BOM"]
	accountingName = filters["ACCOUNTING_NAME"]
	userName = filters["USER_NAME"]

	qname, ok := filters["QUEUE_NAME"]
	if ok && strings.HasPrefix(qname, gpuPrefix) {
		checkGPU = true
	}

	return bom, accountingName, userName, checkGPU
}

type passChecker struct {
	filter  *flatFilter
	passing bool
}

// PassChecker returns a new passChecker that can be used in a goroutine to see
// if values all pass the filter.
func (f *flatFilter) PassChecker() *passChecker {
	return &passChecker{filter: f, passing: true}
}

// Fail will cause Passes() to return false.
func (p *passChecker) Fail() {
	p.passing = false
}

// LT sees if the given timestamp is less than (or less than or equal to,
// depending on what was set in the filter) the filter's LT/LTE value. This
// should be the first method you use in a loop as it overrides Passes() return
// value.
func (p *passChecker) LT(timestamp []byte) {
	if p.filter.checkLTE {
		p.passing = bytes.Compare(timestamp, p.filter.LTEKey) <= 0
	} else {
		p.passing = bytes.Compare(timestamp, p.filter.LTKey) < 0
	}
}

// GTE sees if the given timestamp is greater than or equal to the filter's GTE
// value. Does nothing if we're already not passing.
func (p *passChecker) GTE(timestamp []byte) {
	if !p.passing {
		return
	}

	p.passing = bytes.Compare(timestamp, p.filter.GTEKey) >= 0
}

// GPU sees if the given value matches the inGPUQueue value. Does nothing if
// we're already not passing, or the filter doesn't check for GPU queue
// membership.
func (p *passChecker) GPU(val byte) {
	if !p.passing || !p.filter.checkGPU {
		return
	}

	p.passing = val == inGPUQueue
}

// Passes returns true if Fail() hasn't been called and none of the filter check
// methods failed since the last Reset().
func (p *passChecker) Passes() bool {
	return p.passing
}
