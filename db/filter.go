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
	LTE             time.Time
	GTE             time.Time
	LTEKey          []byte
	GTEKey          []byte
	accountingName  []byte
	userName        []byte
	checkAccounting bool
	checkUser       bool
	checkGPU        bool
}

func newFlatFilter(query *es.Query) (*flatFilter, error) {
	lte, gte, err := query.DateRange()
	if err != nil {
		return nil, err
	}

	filter := &flatFilter{
		LTE: lte,
		GTE: gte,
	}

	filter.LTEKey, filter.GTEKey = i64tob(lte.Unix()), i64tob(gte.Unix())
	filter.BOM, filter.accountingName, filter.userName, filter.checkGPU = queryToFilters(query)

	if filter.BOM == "" {
		return nil, Error{Msg: ErrNoBOM}
	}

	filter.checkAccounting = len(filter.accountingName) > 0
	filter.checkUser = len(filter.userName) > 0

	return filter, nil
}

func queryToFilters(query *es.Query) (bom string, accountingName, userName []byte, checkGPU bool) {
	filters := query.Filters()

	bom = filters["BOM"]

	aname, ok := filters["ACCOUNTING_NAME"]
	if ok {
		if b, err := fixedWidthField(aname, accountingNameWidth); err == nil {
			accountingName = b
		}
	}

	uname, ok := filters["USER_NAME"]
	if ok {
		if b, err := fixedWidthField(uname, userNameWidth); err == nil {
			userName = b
		}
	}

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

func (f *flatFilter) PassChecker() *passChecker {
	return &passChecker{filter: f, passing: true}
}

func (p *passChecker) Fail() {
	p.passing = false
}

func (p *passChecker) AccountingName(val []byte) {
	if !p.passing || !p.filter.checkAccounting {
		return
	}

	p.passing = bytes.Equal(val, p.filter.accountingName)
}

func (p *passChecker) UserName(val []byte) {
	if !p.passing || !p.filter.checkUser {
		return
	}

	p.passing = bytes.Equal(val, p.filter.userName)
}

func (p *passChecker) GPU(val byte) {
	if !p.passing || !p.filter.checkGPU {
		return
	}

	p.passing = val == inGPUQueue
}

func (p *passChecker) Passes() bool {
	return p.passing
}
