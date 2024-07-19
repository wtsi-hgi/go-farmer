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
	"fmt"
	"strings"
	"time"

	es "github.com/wtsi-hgi/go-farmer/elasticsearch"
)

const ErrNoBOM = "query does not specify a BOM"

type sqlFilter struct {
	LT              int64
	LTE             int64
	GTE             int64
	bom             string
	accountingName  string
	userName        string
	checkLTE        bool
	checkAccounting bool
	checkUser       bool
	checkGPU        bool
	fields          []string
}

func newSQLFilter(query *es.Query) (*sqlFilter, error) { //nolint:funlen
	lt, lte, gte, err := query.DateRange()
	if err != nil {
		return nil, err
	}

	filters := query.Filters()

	filter := &sqlFilter{
		LT:             lt.Unix(),
		LTE:            lte.Unix(),
		GTE:            gte.Unix(),
		checkLTE:       !lte.IsZero(),
		bom:            filters["BOM"],
		accountingName: filters["ACCOUNTING_NAME"],
		userName:       filters["USER_NAME"],
		fields:         query.Source,
	}

	qname, ok := filters["QUEUE_NAME"]
	if ok && strings.HasPrefix(qname, gpuPrefix) {
		filter.checkGPU = true
	}

	if filter.bom == "" {
		return nil, Error{Msg: ErrNoBOM}
	}

	filter.checkAccounting = len(filter.accountingName) > 0
	filter.checkUser = len(filter.userName) > 0

	return filter, nil
}

func (f *sqlFilter) beyondLastDate(current time.Time) bool {
	cu := current.Unix()

	if f.checkLTE {
		return cu > f.LTE
	}

	return cu == f.LT || cu > f.LT
}

func (f *sqlFilter) toSelect() string { //nolint:funlen
	fields := "*"
	if len(f.fields) > 0 {
		fields = "_id, " + strings.Join(f.fields, ", ")
	}

	var clauses []string

	lComp := "<"
	lVal := f.LT

	if f.checkLTE {
		lComp = "<="
		lVal = f.LTE
	}

	clauses = append(clauses, fmt.Sprintf("(timestamp %s %d)", lComp, lVal))
	clauses = append(clauses, fmt.Sprintf("(timestamp >= %d)", f.GTE))

	if f.checkAccounting {
		clauses = append(clauses, fmt.Sprintf("(ACCOUNTING_NAME = '%s')", f.accountingName))
	}

	if f.checkUser {
		clauses = append(clauses, fmt.Sprintf("(USER_NAME = '%s')", f.userName))
	}

	if f.checkGPU {
		clauses = append(clauses, "(IsGPU = 1)")
	}

	return fmt.Sprintf("SELECT %s FROM Hits WHERE %s", fields, strings.Join(clauses, " AND "))
}
