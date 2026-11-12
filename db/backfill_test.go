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
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
	es "github.com/wtsi-hgi/go-farmer/elasticsearch"
)

const (
	goodBOM    = "Human Genetics"
	partialBOM = "Partial BOM"
)

// storeRejectingScroller is a Scroller whose Scroll sends hitsBeforeBad valid
// hits for the queried day spread over two BOMs, then a hit with a user name
// too long for DB.Store to accept, followed by hitsAfterBad valid hits. Its wg
// lets tests wait for every Scroll call to return.
type storeRejectingScroller struct {
	hitsBeforeBad int
	hitsAfterBad  int
	wg            sync.WaitGroup
}

func (s *storeRejectingScroller) Scroll(query *es.Query, cb es.HitsCallBack) (*es.Result, error) {
	s.wg.Add(1)
	defer s.wg.Done()

	day, err := time.Parse(time.RFC3339, query.Query.Bool.Filter[1]["range"].GetMapString("timestamp", "gte"))
	if err != nil {
		return nil, err
	}

	ts := day.Add(12 * time.Hour).Unix()
	hit := func(id, bom, user string) *es.Hit {
		return &es.Hit{ID: id, Details: &es.Details{Timestamp: ts, BOM: bom, UserName: user}}
	}

	for i := range s.hitsBeforeBad {
		cb(hit("before"+strconv.Itoa(i), "BOM "+strconv.Itoa(i%2), "user"))
	}

	cb(hit("bad", "Fake BOM", strings.Repeat("u", userNameWidth+1)))

	for i := range s.hitsAfterBad {
		cb(hit("after"+strconv.Itoa(i), "Fake BOM", "user"))
	}

	return &es.Result{}, nil
}

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

	Convey("Given a problematic mock elasticsearch client, you can Backfill() with an error", t, func() {
		slog.SetLogLoggerLevel(slog.LevelError)
		recordRetrySleeps()

		dir := t.TempDir()
		mock := es.NewMock("long_group")
		config := Config{Directory: filepath.Join(dir, "notexist")}

		errCh := make(chan error)

		go func() {
			errCh <- Backfill(mock, config, from, period)
		}()

		var err error

		select {
		case err = <-errCh:
			So(err, ShouldNotBeNil)
		case <-time.After(5 * time.Second):
			So(err, ShouldNotBeNil)
		}

		So(err.Error(), ShouldStartWith, "day 2024-05-31T00:00:00Z..2024-06-01T00:00:00Z: store locally: "+ErrFieldTooLong)

		var dbErr Error

		So(errors.As(err, &dbErr), ShouldBeTrue)
		So(dbErr.Msg, ShouldEqual, ErrFieldTooLong)
	})

	Convey("Given a scroller whose hits Store rejects part way through, Backfill() errors "+
		"without leaving Scroll blocked or files open", t, func() {
		slog.SetLogLoggerLevel(slog.LevelError)
		recordRetrySleeps()

		scroller := &storeRejectingScroller{hitsBeforeBad: 4, hitsAfterBad: 10}
		config := Config{Directory: t.TempDir()}
		fdsBefore := openFDCount()

		backfillErrCh := make(chan error, 1)

		go func() {
			backfillErrCh <- Backfill(scroller, config, from, oneDay)
		}()

		backfillOutcome := "Backfill() still blocked after 5s"

		select {
		case err := <-backfillErrCh:
			backfillOutcome = fmt.Sprint(err)
		case <-time.After(5 * time.Second):
		}

		storeErr := ": store locally: " + ErrFieldTooLong + ": '" +
			strings.Repeat("u", userNameWidth+1) + "' is > " + strconv.Itoa(userNameWidth) + " characters"
		So(backfillOutcome, ShouldEqual, "day 2024-05-30T00:00:00Z..2024-05-31T00:00:00Z"+storeErr+
			"\nday 2024-05-31T00:00:00Z..2024-06-01T00:00:00Z"+storeErr)

		scrollsDone := make(chan struct{})

		go func() {
			scroller.wg.Wait()
			close(scrollsDone)
		}()

		outcome := "Scroll still blocked after 5s"

		select {
		case <-scrollsDone:
			outcome = "Scroll returned"
		case <-time.After(5 * time.Second):
		}

		So(outcome, ShouldEqual, "Scroll returned")
		So(openFDCount(), ShouldEqual, fdsBefore)
		So(goroutineStackContaining("queryElasticAndStoreLocally", 2*time.Second), ShouldBeEmpty)
	})

	Convey("Given a scroller that fails, Backfill()'s error names the day and stage and wraps the cause", t, func() {
		slog.SetLogLoggerLevel(slog.LevelError)
		recordRetrySleeps()

		scrollErr := fmt.Errorf("scroll page 2: read response body: %w", io.ErrUnexpectedEOF)
		config := Config{Directory: t.TempDir()}

		err := Backfill(failingScroller{failGTE: "2024-05-31T00:00:00Z", err: scrollErr}, config, from, oneDay)
		So(err, ShouldWrap, io.ErrUnexpectedEOF)
		So(err.Error(), ShouldEqual, "day 2024-05-31T00:00:00Z..2024-06-01T00:00:00Z: gave up after 12 attempts: "+
			"query elasticsearch: scroll page 2: read response body: unexpected EOF")
	})

	Convey("Given a mock elasticsearch client, db config and period, you can Backfill()", t, func() {
		slog.SetLogLoggerLevel(slog.LevelWarn)

		dir := t.TempDir()
		mock := es.NewMock("some-indexes-*")
		config := Config{Directory: dir}

		err := Backfill(mock, config, from, period)
		So(err, ShouldBeNil)

		bom := "Human Genetics"
		localPath30 := filepath.Join(dir, "2024", "05", "30", bom, "0.index")
		localPath31 := filepath.Join(dir, "2024", "05", "31", bom, "0.index")

		infoOrig30, err := os.Stat(localPath30)
		So(err, ShouldBeNil)

		_, err = os.Stat(localPath31)
		So(err, ShouldBeNil)

		query := rangeQuery(timeRange(from, period))
		query.Query.Bool.Filter = append(query.Query.Bool.Filter,
			map[string]es.MapStringStringOrMap{"match_phrase": map[string]interface{}{"BOM": bom}})

		db, err := New(config, true)
		So(err, ShouldBeNil)

		result, errs := db.Scroll(query)
		So(errs, ShouldBeNil)
		So(result.HitSet.Total.Value, ShouldEqual, 2)

		Convey("Repeating Backfill() only stores missing days", func() {
			err = os.RemoveAll(filepath.Dir(filepath.Dir(localPath31)))
			So(err, ShouldBeNil)

			err = Backfill(mock, config, from, period)
			So(err, ShouldBeNil)

			infoRepeat, err := os.Stat(localPath30)
			So(err, ShouldBeNil)
			So(infoRepeat.ModTime(), ShouldEqual, infoOrig30.ModTime())

			_, err = os.Stat(localPath31)
			So(err, ShouldBeNil)
		})

		Convey("Repeating Backfill() handles potentially corrupt prior days by starting them from scratch", func() {
			err = os.Remove(filepath.Join(filepath.Dir(filepath.Dir(localPath31)), successBasename))
			So(err, ShouldBeNil)

			extraPath := filepath.Join(filepath.Dir(localPath31), "1")
			f, err := os.Create(extraPath)
			So(err, ShouldBeNil)

			err = f.Close()
			So(err, ShouldBeNil)

			past := time.Now().Add(-time.Hour)
			err = os.Chtimes(localPath31, past, past)
			So(err, ShouldBeNil)

			err = Backfill(mock, config, from, period)
			So(err, ShouldBeNil)

			infoRepeat, err := os.Stat(localPath31)
			So(err, ShouldBeNil)
			So(infoRepeat.ModTime(), ShouldHappenAfter, past)

			_, err = os.Stat(extraPath)
			So(err, ShouldNotBeNil)
		})
	})

	doSlow := os.Getenv("GOFARMER_SLOWTESTS")
	if doSlow != "1" {
		SkipConvey("Skipping real elasticsearch tests without GOFARMER_SLOWTESTS=1", t, func() {})

		return
	}

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

		realBackfillTest(client, config, from, period)
	})
}

func recordRetrySleeps() *sleepRecorder {
	r := &sleepRecorder{}
	orig := retrySleep

	Reset(func() { retrySleep = orig })

	retrySleep = func(d time.Duration) {
		r.mu.Lock()
		defer r.mu.Unlock()

		r.delays = append(r.delays, d)
	}

	return r
}

// openFDCount returns the number of file descriptors this process has open.
func openFDCount() int {
	entries, err := os.ReadDir("/proc/self/fd")
	So(err, ShouldBeNil)

	return len(entries)
}

// goroutineStackContaining polls for up to timeout until no goroutine's stack
// mentions name, returning the matching stack if one still does.
func goroutineStackContaining(name string, timeout time.Duration) string {
	buf := make([]byte, 1<<20)
	deadline := time.Now().Add(timeout)

	for {
		stacks := strings.Split(string(buf[:runtime.Stack(buf, true)]), "\n\n")

		i := slices.IndexFunc(stacks, func(stack string) bool {
			return strings.Contains(stack, name)
		})
		if i == -1 {
			return ""
		}

		if time.Now().After(deadline) {
			return stacks[i]
		}

		time.Sleep(10 * time.Millisecond)
	}
}

func realBackfillTest(client Scroller, config Config, from time.Time, period time.Duration) {
	var b strings.Builder

	logger := slog.New(slog.NewTextHandler(&b, nil))
	slog.SetDefault(logger)

	err := Backfill(client, config, from, period)
	So(err, ShouldBeNil)

	logged := b.String()

	expectedFirstLog := "gte=2024-05-30T00:00:00Z lte=2024-05-31T00:00:00Z"
	expectedSecondLog := "gte=2024-05-31T00:00:00Z lte=2024-06-01T00:00:00Z"
	expectedThirdLog := "gte=2024-05-29T00:00:00Z lte=2024-05-30T00:00:00Z"

	count := strings.Count(logged, expectedFirstLog)
	So(count, ShouldEqual, 1)

	count = strings.Count(logged, expectedSecondLog)
	So(count, ShouldEqual, 1)

	count = strings.Count(logged, expectedThirdLog)
	So(count, ShouldEqual, 1)

	indexFirst := strings.Index(logged, expectedFirstLog)
	indexSecond := strings.Index(logged, expectedSecondLog)
	indexThird := strings.Index(logged, expectedThirdLog)

	// these log tests assume the query times for the 3 different days remains
	// similar relative to each other; this might not be true and the tests
	// might break...
	So(indexFirst, ShouldBeLessThan, indexSecond)
	So(indexSecond, ShouldBeLessThan, indexThird)

	db, err := New(config, true)
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

	b.Reset()

	err = Backfill(client, config, from, period)
	So(err, ShouldBeNil)

	logged = b.String()
	So(logged, ShouldNotContainSubstring, "successful")

	count = strings.Count(logged, "skip completed day")
	So(count, ShouldEqual, 3)
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

// dayHitIDs returns the sorted IDs of the hits a new DB finds for bom between
// gte and lt.
func dayHitIDs(config Config, bom, gte, lt string) []string {
	ldb, err := New(config, true)
	So(err, ShouldBeNil)

	query := rangeQuery(mustParseTime(gte), mustParseTime(lt))
	query.Query.Bool.Filter = append(query.Query.Bool.Filter,
		map[string]es.MapStringStringOrMap{"match_phrase": map[string]interface{}{"BOM": bom}})

	result, err := ldb.Scroll(query)
	So(err, ShouldBeNil)

	ids := make([]string, 0, len(result.HitSet.Hits))
	for _, hit := range result.HitSet.Hits {
		ids = append(ids, hit.ID)
	}

	slices.Sort(ids)

	return ids
}

func mustParseTime(s string) time.Time {
	t, err := time.Parse(time.RFC3339, s)
	So(err, ShouldBeNil)

	return t
}

// failingScroller is a Scroller whose Scroll sends no hits, and returns err
// for the day starting at failGTE but succeeds for other days.
type failingScroller struct {
	failGTE string
	err     error
}

func (f failingScroller) Scroll(query *es.Query, _ es.HitsCallBack) (*es.Result, error) {
	if query.Query.Bool.Filter[1]["range"].GetMapString("timestamp", "gte") == f.failGTE {
		return nil, f.err
	}

	return &es.Result{}, nil
}

// flakyScroller is a Scroller that, for each day whose gte is a key of
// failures, sends one hit for partialBOM and then fails that day's first n attempts
// (all of them when n is negative), before sending goodHits hits. For each day
// whose gte is in storeRejects, it instead sends one hit for partialBOM and
// then a hit with a user name too long for DB.Store to accept. It counts
// attempts per day.
type flakyScroller struct {
	failures     map[string]int
	storeRejects []string
	goodHits     int

	mu       sync.Mutex
	attempts map[string]int
}

func (f *flakyScroller) Scroll(query *es.Query, cb es.HitsCallBack) (*es.Result, error) {
	gte := query.Query.Bool.Filter[1]["range"].GetMapString("timestamp", "gte")

	f.mu.Lock()
	if f.attempts == nil {
		f.attempts = make(map[string]int)
	}

	f.attempts[gte]++
	attempt := f.attempts[gte]
	f.mu.Unlock()

	day, err := time.Parse(time.RFC3339, gte)
	if err != nil {
		return nil, err
	}

	ts := day.Add(12 * time.Hour).Unix()
	hit := func(id, bom string) *es.Hit {
		return &es.Hit{ID: id, Details: &es.Details{ID: id, Timestamp: ts, BOM: bom, UserName: "user"}}
	}

	if slices.Contains(f.storeRejects, gte) {
		cb(hit("partial-"+strconv.Itoa(attempt), partialBOM))

		bad := hit("bad-"+strconv.Itoa(attempt), goodBOM)
		bad.Details.UserName = strings.Repeat("u", userNameWidth+1)
		cb(bad)

		return &es.Result{}, nil
	}

	if n, ok := f.failures[gte]; ok && (n < 0 || attempt <= n) {
		cb(hit("partial-"+strconv.Itoa(attempt), partialBOM))

		return nil, fmt.Errorf("scroll page 1: read response body: %w", io.ErrUnexpectedEOF)
	}

	for i := range f.goodHits {
		cb(hit("good-"+strconv.Itoa(i), goodBOM))
	}

	return &es.Result{}, nil
}

func (f *flakyScroller) attemptsFor(gte string) int {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.attempts[gte]
}

func TestBackfillRetries(t *testing.T) {
	from := time.Date(2024, 6, 1, 0, 30, 0, 0, time.UTC)
	day30, day31 := "2024-05-30T00:00:00Z", "2024-05-31T00:00:00Z"
	day29, dayEnd := "2024-05-29T00:00:00Z", "2024-06-01T00:00:00Z"
	goodIDs := []string{"good-0", "good-1", "good-2"}

	Convey("Given a scroller that fails a day's first attempts, Backfill() retries that day "+
		"with a doubling delay and stores only the successful attempt's hits", t, func() {
		sleeps := recordRetrySleeps()
		logs := captureLogs()
		scroller := &flakyScroller{failures: map[string]int{day31: 3}, goodHits: 3}
		config := Config{Directory: t.TempDir()}

		err := Backfill(scroller, config, from, oneDay)
		So(err, ShouldBeNil)

		So(scroller.attemptsFor(day31), ShouldEqual, 4)
		So(scroller.attemptsFor(day30), ShouldEqual, 1)
		So(sleeps.recorded(), ShouldResemble, []time.Duration{time.Minute, 2 * time.Minute, 4 * time.Minute})

		So(dayHitIDs(config, goodBOM, day31, dayEnd), ShouldResemble, goodIDs)
		So(dayHitIDs(config, partialBOM, day31, dayEnd), ShouldBeEmpty)
		So(dayHitIDs(config, goodBOM, day30, day31), ShouldResemble, goodIDs)

		logged := logs.String()
		So(strings.Count(logged, "level=WARN"), ShouldEqual, 3)

		for attempt, delay := range []string{"1m0s", "2m0s", "4m0s"} {
			So(logged, ShouldContainSubstring, "gte="+day31+" lt="+dayEnd+" attempt="+strconv.Itoa(attempt+1)+
				` err="query elasticsearch: scroll page 1: read response body: unexpected EOF" retry_in=`+delay)
		}
	})

	Convey("Given a scroller that always fails one day, Backfill() gives up on that day after 12 attempts "+
		"but completes the other day", t, func() {
		sleeps := recordRetrySleeps()
		logs := captureLogs()
		scroller := &flakyScroller{failures: map[string]int{day31: -1}, goodHits: 3}
		config := Config{Directory: t.TempDir()}

		err := Backfill(scroller, config, from, oneDay)
		So(err, ShouldWrap, io.ErrUnexpectedEOF)
		So(err.Error(), ShouldEqual, "day "+day31+".."+dayEnd+": gave up after 12 attempts: "+
			"query elasticsearch: scroll page 1: read response body: unexpected EOF")

		So(scroller.attemptsFor(day31), ShouldEqual, 12)
		So(sleeps.recorded(), ShouldResemble, []time.Duration{
			time.Minute, 2 * time.Minute, 4 * time.Minute, 8 * time.Minute, 16 * time.Minute, 32 * time.Minute,
			time.Hour, time.Hour, time.Hour, time.Hour, time.Hour,
		})
		So(strings.Count(logs.String(), "level=WARN"), ShouldEqual, 11)

		_, err = os.Stat(filepath.Join(config.Directory, "2024", "05", "31"))
		So(err, ShouldWrap, os.ErrNotExist)
		So(dayHitIDs(config, partialBOM, day31, dayEnd), ShouldBeEmpty)

		_, err = os.Stat(filepath.Join(config.Directory, "2024", "05", "30", successBasename))
		So(err, ShouldBeNil)
		So(dayHitIDs(config, goodBOM, day30, day31), ShouldResemble, goodIDs)
	})

	Convey("Given a scroller whose hits Store rejects for one day, Backfill() fails that day on its "+
		"first attempt without retrying or keeping partial data, but completes the other day", t, func() {
		sleeps := recordRetrySleeps()
		logs := captureLogs()
		scroller := &flakyScroller{storeRejects: []string{day31}, goodHits: 3}
		config := Config{Directory: t.TempDir()}

		err := Backfill(scroller, config, from, oneDay)
		So(err, ShouldNotBeNil)
		So(err.Error(), ShouldEqual, "day "+day31+".."+dayEnd+": store locally: "+ErrFieldTooLong+": '"+
			strings.Repeat("u", userNameWidth+1)+"' is > "+strconv.Itoa(userNameWidth)+" characters")

		var dbErr Error

		So(errors.As(err, &dbErr), ShouldBeTrue)
		So(dbErr.Msg, ShouldEqual, ErrFieldTooLong)

		So(scroller.attemptsFor(day31), ShouldEqual, 1)
		So(sleeps.recorded(), ShouldBeEmpty)
		So(logs.String(), ShouldNotContainSubstring, "level=WARN")

		_, err = os.Stat(filepath.Join(config.Directory, "2024", "05", "31"))
		So(err, ShouldWrap, os.ErrNotExist)

		_, err = os.Stat(filepath.Join(config.Directory, "2024", "05", "30", successBasename))
		So(err, ShouldBeNil)
		So(dayHitIDs(config, goodBOM, day30, day31), ShouldResemble, goodIDs)
	})

	Convey("Given a scroller that always fails two days, Backfill() reports both days "+
		"and completes the third", t, func() {
		recordRetrySleeps()
		captureLogs()

		scroller := &flakyScroller{failures: map[string]int{day29: -1, day31: -1}, goodHits: 3}
		config := Config{Directory: t.TempDir()}

		err := Backfill(scroller, config, from, 2*oneDay)
		So(err, ShouldWrap, io.ErrUnexpectedEOF)

		cause := ": gave up after 12 attempts: query elasticsearch: scroll page 1: read response body: unexpected EOF"
		So(err.Error(), ShouldEqual, "day "+day29+".."+day30+cause+"\nday "+day31+".."+dayEnd+cause)

		So(scroller.attemptsFor(day29), ShouldEqual, 12)
		So(scroller.attemptsFor(day31), ShouldEqual, 12)
		So(dayHitIDs(config, goodBOM, day30, day31), ShouldResemble, goodIDs)
	})

	if os.Geteuid() == 0 {
		SkipConvey("Skipping unremovable stale day test when running as root", t, func() {})

		return
	}

	Convey("Given a stale incomplete day that can't be removed, Backfill() reports that day "+
		"without querying it, but completes the other day", t, func() {
		recordRetrySleeps()

		dayJune1, dayJune2 := dayEnd, "2024-06-02T00:00:00Z"
		scroller := &flakyScroller{goodHits: 3}
		config := Config{Directory: t.TempDir()}
		monthDir := filepath.Join(config.Directory, "2024", "05")

		err := os.MkdirAll(filepath.Join(monthDir, "31"), dbDirPerms)
		So(err, ShouldBeNil)

		Reset(func() { os.Chmod(monthDir, dbDirPerms) }) //nolint:errcheck

		err = os.Chmod(monthDir, 0o555)
		So(err, ShouldBeNil)

		err = Backfill(scroller, config, mustParseTime(dayJune2).Add(30*time.Minute), oneDay)
		So(err, ShouldWrap, os.ErrPermission)
		So(err.Error(), ShouldStartWith, "day "+day31+".."+dayJune1+": ")

		So(scroller.attemptsFor(day31), ShouldEqual, 0)
		So(scroller.attemptsFor(dayJune1), ShouldEqual, 1)
		So(dayHitIDs(config, goodBOM, dayJune1, dayJune2), ShouldResemble, goodIDs)
	})
}

// captureLogs sends the default slog logger's output to the returned builder
// for the current Convey.
func captureLogs() *strings.Builder {
	var b strings.Builder

	orig := slog.Default()

	Reset(func() { slog.SetDefault(orig) })

	slog.SetDefault(slog.New(slog.NewTextHandler(&lockedWriter{w: &b}, nil)))

	return &b
}

// sleepRecorder replaces the backfill retry sleep for the current Convey,
// recording each requested delay instead of waiting.
type sleepRecorder struct {
	mu     sync.Mutex
	delays []time.Duration
}

func (r *sleepRecorder) recorded() []time.Duration {
	r.mu.Lock()
	defer r.mu.Unlock()

	return slices.Clone(r.delays)
}

type lockedWriter struct {
	mu sync.Mutex
	w  io.Writer
}

func (l *lockedWriter) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	return l.w.Write(p)
}
