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
	"context"
	"log/slog"
	"os"
	"time"

	es "github.com/wtsi-hgi/go-farmer/elasticsearch"
	"golang.org/x/sync/errgroup"
)

const (
	ErrAlreadyExists = "database directory already exists"

	maxSimultaneousBackfills = 16
)

// Scroller types have a Scroll function for querying something like elastic
// search, automatically getting all hits in a single scroll call.
type Scroller interface {
	Scroll(query *es.Query) (*es.Result, error)
}

func Backfill(client Scroller, config Config, from time.Time, period time.Duration) (err error) {
	if _, err = os.Stat(config.Directory); err == nil {
		return Error{Msg: ErrAlreadyExists}
	}

	ldb, errn := New(config)
	if errn != nil {
		err = errn

		return err
	}

	defer func() {
		errc := ldb.Close()
		if err == nil {
			err = errc
		}
	}()

	return backfillByDay(client, ldb, from, period)
}

func backfillByDay(client Scroller, ldb *DB, from time.Time, period time.Duration) error {
	gte, lt := timeRange(from, period)

	g, _ := errgroup.WithContext(context.Background())
	g.SetLimit(maxSimultaneousBackfills)

	for !gte.After(lt) {
		from := gte

		g.Go(func() error {
			return queryElasticAndStoreLocally(client, ldb, from, oneDay)
		})

		gte = gte.Add(oneDay)
	}

	return g.Wait()
}

func queryElasticAndStoreLocally(client Scroller, ldb *DB, from time.Time, period time.Duration) error {
	gte, lt := timeRange(from, period)

	query := rangeQuery(gte, lt)

	t := time.Now()

	result, err := client.Scroll(query)
	if err != nil {
		return err
	}

	slog.Info("search successful", "took", time.Since(t), "gte", timestamp(gte), "lte", timestamp(lt))
	t = time.Now()

	err = ldb.Store(result)
	if err != nil {
		return err
	}

	slog.Info("store successful", "took", time.Since(t))

	return nil
}

func timeRange(from time.Time, period time.Duration) (time.Time, time.Time) {
	y, m, d := from.Date()
	end := time.Date(y, m, d, 0, 0, 0, 0, time.UTC)
	start := end.Add(-period)

	if period > oneDay {
		y, m, d := start.Date()
		start = time.Date(y, m, d, 0, 0, 0, 0, time.UTC)
	}

	return start, end
}

func rangeQuery(from time.Time, to time.Time) *es.Query {
	return &es.Query{
		Size: es.MaxSize,
		Sort: []string{"timestamp", "_doc"},
		Query: &es.QueryFilter{Bool: es.QFBool{Filter: es.Filter{
			{"match_phrase": map[string]interface{}{"META_CLUSTER_NAME": "farm"}},
			{"range": map[string]interface{}{
				"timestamp": map[string]string{
					"lt":     timestamp(to),
					"gte":    timestamp(from),
					"format": "strict_date_optional_time",
				},
			}},
		}}},
	}
}

func timestamp(t time.Time) string {
	return t.Format(time.RFC3339)
}
