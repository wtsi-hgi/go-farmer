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
	"path/filepath"
	"time"

	es "github.com/wtsi-hgi/go-farmer/elasticsearch"
	"golang.org/x/sync/errgroup"
)

const (
	ErrAlreadyExists = "database directory already exists"

	maxSimultaneousBackfills = 16
	successBasename          = ".backfill_successful"
)

// Scroller types have a Scroll function for querying something like elastic
// search, automatically getting all hits in a single scroll call.
type Scroller interface {
	Scroll(query *es.Query, cb es.HitsCallBack) (*es.Result, error)
}

// Backfill uses the given client to request all hits from the end of the day
// prior to the given from time to the start of the day period time before then.
//
// If the configured database directory already has any results for a particular
// day, that day will be skipped.
func Backfill(client Scroller, config Config, from time.Time, period time.Duration) (err error) {
	ldb := newDBStruct(config, true)

	return backfillByDay(client, ldb, from, period)
}

func backfillByDay(client Scroller, ldb *DB, from time.Time, period time.Duration) error {
	gte, lt := timeRange(from, period)
	g, _ := errgroup.WithContext(context.Background())
	g.SetLimit(maxSimultaneousBackfills)

	for !gte.After(lt) {
		from, lt := timeRange(gte, oneDay)
		gte = gte.Add(oneDay)

		successPath, err := checkIfNeeded(ldb, from)
		if err != nil {
			return err
		}

		if successPath == "" {
			continue
		}

		g.Go(func() error {
			return queryElasticAndStoreLocally(client, ldb, from, lt, successPath)
		})
	}

	return g.Wait()
}

func queryElasticAndStoreLocally(client Scroller, ldb *DB, gte, lt time.Time, successPath string) error {
	query := rangeQuery(gte, lt)
	t := time.Now()
	hitCh := make(chan *es.Hit)
	errCh := make(chan error)
	cb := func(hit *es.Hit) {
		hitCh <- hit
	}

	go func() {
		_, err := client.Scroll(query, cb)
		close(hitCh)
		errCh <- err
	}()

	err := ldb.Store(hitCh)
	if err != nil {
		return err
	}

	err = <-errCh
	if err != nil {
		return err
	}

	slog.Info("search&store successful", "took", time.Since(t), "gte", timestamp(gte), "lte", timestamp(lt))

	return recordSuccess(successPath)
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

// checkIfNeeded returns the path of the success file you should create after
// successfully storing the data for this day, if this day hasn't already been
// done. So blank means skip.
func checkIfNeeded(ldb *DB, day time.Time) (string, error) {
	dir := ldb.dateFolder(day)
	successPath := filepath.Join(dir, successBasename)

	_, err := os.Stat(successPath)
	if err == nil {
		slog.Info("skip completed day", "gte", timestamp(day))

		return "", nil
	}

	var returnErr error

	_, err = os.Stat(dir)
	if err == nil {
		returnErr = os.RemoveAll(dir)
	}

	return successPath, returnErr
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

// recordSuccess creates an empty sential file so that we know we stored a whole
// day's hits. In case there were no hits for that day, we first make the
// directory (otherwise DB.Store() would have made it).
func recordSuccess(path string) error {
	err := os.MkdirAll(filepath.Dir(path), dbDirPerms)
	if err != nil {
		return err
	}

	f, err := os.Create(path)
	if err != nil {
		return err
	}

	return f.Close()
}
