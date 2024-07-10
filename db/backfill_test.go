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
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
	es "github.com/wtsi-hgi/go-farmer/elasticsearch"
)

func TestBackfill(t *testing.T) {
	from := time.Date(2024, 06, 1, 0, 30, 0, 0, time.UTC)

	Convey("You can convert durations to timestamp ranges", t, func() {
		gte, lte := timeRange(from, 15*time.Minute)
		So(timestamp(gte), ShouldEqual, "2024-05-31T23:45:00Z")
		So(timestamp(lte), ShouldEqual, "2024-06-01T00:00:00Z")

		gte, lte = timeRange(from, 2*time.Hour)
		So(timestamp(gte), ShouldEqual, "2024-05-31T22:00:00Z")
		So(timestamp(lte), ShouldEqual, "2024-06-01T00:00:00Z")

		gte, lte = timeRange(from, 3*oneDay)
		So(timestamp(gte), ShouldEqual, "2024-05-29T00:00:00Z")
		So(timestamp(lte), ShouldEqual, "2024-06-01T00:00:00Z")

		gte, lte = timeRange(from, 730*time.Hour)
		So(timestamp(gte), ShouldEqual, "2024-05-01T00:00:00Z")
		So(timestamp(lte), ShouldEqual, "2024-06-01T00:00:00Z")
	})

	period := (2 * 24) * time.Hour

	Convey("You can't Backfill() if the directory already exists", t, func() {
		dir := t.TempDir()
		mock := es.NewMock("some-indexes-*")
		config := Config{Directory: dir}

		err := Backfill(mock.Client(), config, from, period)
		So(err, ShouldNotBeNil)
	})

	host := os.Getenv("FARMER_TEST_HOST")
	username := os.Getenv("FARMER_TEST_USERNAME")
	password := os.Getenv("FARMER_TEST_PASSWORD")
	scheme := os.Getenv("FARMER_TEST_SCHEME")
	portStr := os.Getenv("FARMER_TEST_PORT")
	index := os.Getenv("FARMER_TEST_INDEX")

	if host == "" || username == "" || password == "" || scheme == "" || portStr == "" || index == "" {
		SkipConvey("Skipping real elasticsearch tests without FARMER_TEST_* env vars set", t, func() {})

		return
	}

	port, err := strconv.Atoi(portStr)
	if err != nil {
		SkipConvey("Skipping real elasticsearch tests since FARMER_TEST_PORT was not a number", t, func() {})

		return
	}

	Convey("Given a real elasticsearch client, db config and period, you can Backfill()", t, func() {
		dir := t.TempDir()
		config := Config{Directory: filepath.Join(dir, "db")}
		client, err := es.NewClient(es.Config{
			Host:     host,
			Username: username,
			Password: password,
			Scheme:   scheme,
			Port:     port,
			Index:    index,
		})
		So(err, ShouldBeNil)

		backfillTest(client, config, from, period)
	})
}

func backfillTest(client Scroller, config Config, from time.Time, period time.Duration) {
	slog.SetLogLoggerLevel(slog.LevelWarn)

	err := Backfill(client, config, from, period)
	So(err, ShouldBeNil)

	db, err := New(config)
	So(err, ShouldBeNil)

	total := 0

	var first, last time.Time

	for _, bom := range []string{"Human Genetics", "Genomic Surveillance Unit",
		"Tree of Life", "Cellular Genetics", "CASM", "Infection Genomics",
		"Management Operations", "Open Targets", "Scientific Operations"} {
		query := rangeQuery(timeRange(from, period))
		query.Query.Bool.Filter = append(query.Query.Bool.Filter,
			map[string]es.MapStringStringOrMap{"match_phrase": map[string]interface{}{"BOM": bom}})

		result, errs := db.Scroll(query)
		So(errs, ShouldBeNil)
		So(result.HitSet.Total.Value, ShouldBeGreaterThan, 0)

		updateFirstLastHitTimestamp(result, &first, &last)

		total += result.HitSet.Total.Value
	}

	So(total, ShouldEqual, 262830)

	expectedLast, err := time.Parse(time.RFC3339, "2024-05-31T23:59:59Z")
	So(err, ShouldBeNil)

	expectedFirst, err := time.Parse(time.RFC3339, "2024-05-30T00:00:00Z")
	So(err, ShouldBeNil)

	So(last.Truncate(1*time.Second).UTC(), ShouldEqual, expectedLast)
	So(first.Truncate(1*time.Second).UTC(), ShouldEqual, expectedFirst)

	query := &es.Query{
		Size: es.MaxSize,
		Sort: []string{"timestamp", "_doc"},
		Query: &es.QueryFilter{Bool: es.QFBool{Filter: es.Filter{
			{"match_phrase": map[string]interface{}{"META_CLUSTER_NAME": "farm"}},
			{"match_phrase": map[string]interface{}{"BOM": "Human Genetics"}},
			{"range": map[string]interface{}{
				"timestamp": map[string]string{
					"lte":    "2024-06-01T00:30:00Z",
					"gte":    "2024-06-01T00:00:00Z",
					"format": "strict_date_optional_time",
				},
			}},
		}}},
	}

	result, err := db.Scroll(query)
	So(err, ShouldBeNil)
	So(result.HitSet.Total.Value, ShouldEqual, 0)
}

func updateFirstLastHitTimestamp(result *es.Result, first, last *time.Time) {
	for _, hit := range result.HitSet.Hits {
		t := time.Unix(hit.Details.Timestamp, 0)

		if first.IsZero() || t.Before(*first) {
			*first = t
		}

		if last.IsZero() || t.After(*last) {
			*last = t
		}
	}
}
