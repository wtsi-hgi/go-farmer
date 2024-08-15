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
	"encoding/binary"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	es "github.com/wtsi-hgi/go-farmer/elasticsearch"
	"golang.org/x/sync/errgroup"
)

const (
	ErrFieldTooLong = "field value exceeds expected width"

	dbDirPerms = 0770

	timeStampWidth         = 8
	bomWidth               = 34
	accountingNameWidth    = 24
	userNameWidth          = 13
	gpuPrefix              = "gpu"
	notInGPUQueue          = byte(1)
	inGPUQueue             = byte(2)
	lengthEncodeWidth      = 4
	defaultFileSize        = 32 * 1024 * 1024
	defaultBufferSize      = 4 * 1024 * 1024
	defaultUpdateFrequency = 1 * time.Hour

	oneDay = 24 * time.Hour

	dateFormat      = "2006/01/02"
	pretendScrollID = "farmer_scroll_id"
)

// Error is an error type that has a Msg with one of our const Err* messages.
type Error struct {
	Msg   string
	cause string
}

// Error returns a string representation of the error.
func (e Error) Error() string {
	if e.cause != "" {
		return fmt.Sprintf("%s: %s", e.Msg, e.cause)
	}

	return e.Msg
}

// Config us used to configure a DB. At least Directory must be specified, which
// is the directory path you want to store the local database files, or where
// they are already stored.
type Config struct {
	Directory  string
	FileSize   int // FileSize defaults to 32MB
	BufferSize int // BufferSize defaults to 4MB
	// PoolSize defaults to zero, but the higher you make this, the better
	// first-time large queries will be. Set it to the expected max number of
	// hits your queries will return.
	PoolSize        int
	UpdateFrequency time.Duration // UpdateFrequency defaults to 1hr
}

// FileSizeOrDefault returns our FileSize value, unless that is 0, in which
// case it returns a sensible default value (32MB).
func (c Config) FileSizeOrDefault() int {
	if c.FileSize == 0 {
		return defaultFileSize
	}

	return c.FileSize
}

// BufferSizeOrDefault returns our BufferSize value, unless that is 0, in which
// case it returns a sensible default value (4MB).
func (c Config) BufferSizeOrDefault() int {
	if c.BufferSize == 0 {
		return defaultBufferSize
	}

	return c.BufferSize
}

// UpdateFrequencyOrDefault returns our UpdateFrequency value, unless that is 0,
// in which case it returns a sensible default value (1 hour).
func (c Config) UpdateFrequencyOrDefault() time.Duration {
	if c.UpdateFrequency == 0 {
		return defaultUpdateFrequency
	}

	return c.UpdateFrequency
}

// DB represents a local database that uses a number of flat files to store
// elasticsearch hit details and return them quickly.
type DB struct {
	dir                  string
	fileSize             int
	bufferSize           int
	bufPool              *bufPool
	updateFrequency      time.Duration
	checkBackfillSuccess bool
	latestDate           time.Time
	stopMonitoring       chan bool

	muDateBOMDirs sync.RWMutex
	dateBOMDirs   map[string][]*flatIndex
}

// New returns a DB that will create or use the database files in the configured
// Directory. Files created will be split if they get over the configured
// FileSize in bytes (default 32MB). Files will be read and written using a
// configured BufferSize buffer in bytes (default 4MB).
//
// For the purpose of a continuously running server that uses the returned DB,
// and for when you're separately Backfill()ing the database directory every
// day, the configured UpdateFrequency is used to update the DB's knowledge of
// available local database files, so that queries will make use of any added
// data over time. This defaults to checking for new files ever hour.
//
// If you're using Backfill, then provide a true bool to only load successful
// whole day database files.
func New(config Config, checkBackfillSuccess bool) (*DB, error) {
	db := newDBStruct(config, checkBackfillSuccess)

	_, err := os.Stat(config.Directory)
	if err == nil {
		err = db.loadAllFlatIndexes(db.dir)
		if err == nil {
			db.monitorFlatIndexes()
			db.bufPool.Warmup(config.PoolSize)
		}
	} else {
		err = os.MkdirAll(config.Directory, dbDirPerms)
	}

	return db, err
}

func newDBStruct(config Config, checkBackfillSuccess bool) *DB {
	return &DB{
		dir:                  config.Directory,
		fileSize:             config.FileSizeOrDefault(),
		bufferSize:           config.BufferSizeOrDefault(),
		bufPool:              newBufPool(),
		updateFrequency:      config.UpdateFrequencyOrDefault(),
		checkBackfillSuccess: checkBackfillSuccess,
		dateBOMDirs:          make(map[string][]*flatIndex),
	}
}

func (d *DB) loadAllFlatIndexes(dir string) error {
	eg := errgroup.Group{}

	err := filepath.WalkDir(dir, func(path string, de fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		if !de.Type().IsRegular() || !strings.HasSuffix(de.Name(), indexKind) {
			return nil
		}

		d.loadFlatIndexIfOK(path, &eg)

		return nil
	})

	errg := eg.Wait()

	if err == nil {
		err = errg
	}

	return err
}

func (d *DB) loadFlatIndexIfOK(path string, eg *errgroup.Group) {
	subDir := filepath.Dir(path)

	if d.checkBackfillSuccess {
		if _, err := os.Stat(filepath.Join(filepath.Dir(subDir), successBasename)); err != nil {
			return
		}
	}

	eg.Go(func() error {
		return d.loadFlatIndexAndUpdateLatestDate(path, subDir)
	})
}

func (d *DB) loadFlatIndexAndUpdateLatestDate(path, subDir string) error {
	fi, err := newFlatIndex(path, d.bufferSize)
	if err != nil {
		return err
	}

	d.muDateBOMDirs.Lock()
	defer d.muDateBOMDirs.Unlock()

	d.dateBOMDirs[subDir] = append(d.dateBOMDirs[subDir], fi)

	return d.updateLatestDate(filepath.Dir(subDir))
}

func (d *DB) updateLatestDate(dateDir string) error {
	dateStr, err := filepath.Rel(d.dir, dateDir)
	if err != nil {
		return err
	}

	date, err := time.Parse(dateFormat, dateStr)
	if err != nil {
		return err
	}

	if date.After(d.latestDate) {
		d.latestDate = date
	}

	return nil
}

func (d *DB) monitorFlatIndexes() {
	ticker := time.NewTicker(d.updateFrequency)
	d.stopMonitoring = make(chan bool)

	go func() {
		for {
			select {
			case <-ticker.C:
				d.loadLatestFlatIndexes()
			case <-d.stopMonitoring:
				ticker.Stop()

				return
			}
		}
	}()
}

func (d *DB) loadLatestFlatIndexes() {
	currentDay := d.latestDate.Add(oneDay)
	maxDay := time.Now()

	for {
		dateFolder := d.dateFolder(currentDay)

		_, err := os.Stat(dateFolder)
		if err == nil {
			err = d.loadAllFlatIndexes(dateFolder)
			if err != nil {
				slog.Error("loadAllFlatIndexes failed", "err", err)
			}
		}

		currentDay = currentDay.Add(oneDay)

		if currentDay.After(maxDay) {
			break
		}
	}
}

// Store stores the Details in the Hits of the given Result in flat database
// files in our directory, that can later be retrieved via Scroll().
//
// NB: What you Store() with this DB will not be available to Scroll(). You will
// make a New() one to Scroll() the stored hits.
//
// NB: You can only call Store() concurrently if the result supplied to each
// invocation is for a query of unique days.
func (d *DB) Store(result *es.Result) error {
	prevDay := ""

	flatDBs := make(map[string]*flatDB)

	for _, hit := range result.HitSet.Hits {
		group, user, isGPU, encodedDetails, err := d.getFixedWidthFields(hit)
		if err != nil {
			return err
		}

		day := timestampToDay(hit.Details.Timestamp)
		if day != prevDay && prevDay != "" {
			err = closeFlatDBs(flatDBs)
			if err != nil {
				return err
			}
		}

		dayBom := filepath.Join(day, hit.Details.BOM)

		fdb, err := d.getOrCreateFlatDB(flatDBs, dayBom)
		if err != nil {
			return err
		}

		if err = fdb.Store(
			[][]byte{
				i64tob(hit.Details.Timestamp),
				group,
				user,
				{isGPU},
			},
			encodedDetails,
		); err != nil {
			return err
		}

		prevDay = day
	}

	return closeFlatDBs(flatDBs)
}

func (d *DB) getFixedWidthFields(hit es.Hit) ([]byte, []byte, byte, []byte, error) {
	group, err := fixedWidthField(hit.Details.AccountingName, accountingNameWidth)
	if err != nil {
		return nil, nil, 0, nil, err
	}

	user, err := fixedWidthField(hit.Details.UserName, userNameWidth)
	if err != nil {
		return nil, nil, 0, nil, err
	}

	isGPU := notInGPUQueue
	if strings.HasPrefix(hit.Details.QueueName, gpuPrefix) {
		isGPU = inGPUQueue
	}

	hit.Details.ID = hit.ID

	encodedDetails, err := hit.Details.Serialize() //nolint:misspell
	if err != nil {
		return nil, nil, 0, nil, err
	}

	return group, user, isGPU, encodedDetails, nil
}

func fixedWidthField(str string, width int) ([]byte, error) {
	padding := width - len(str)
	if padding < 0 {
		return nil, Error{Msg: ErrFieldTooLong, cause: fmt.Sprintf("'%s' is > %d characters", str, width)}
	}

	return []byte(str + strings.Repeat(" ", padding)), nil
}

func timestampToDay(timeStamp int64) string {
	return time.Unix(timeStamp, 0).UTC().Format(dateFormat)
}

func closeFlatDBs(flatDBs map[string]*flatDB) error {
	for key, fdb := range flatDBs {
		if err := fdb.Close(); err != nil {
			return err
		}

		delete(flatDBs, key)
	}

	return nil
}

func (d *DB) getOrCreateFlatDB(flatDBs map[string]*flatDB, dayBom string) (*flatDB, error) {
	var err error

	fdb, ok := flatDBs[dayBom]
	if !ok {
		fdb, err = newFlatDB(filepath.Join(d.dir, dayBom), d.fileSize, d.bufferSize)
		if err != nil {
			return nil, err
		}

		flatDBs[dayBom] = fdb
	}

	return fdb, nil
}

// i64tob returns an 8-byte big endian representation of v. The result is a
// sortable byte representation of something like a unix time stamp in seconds.
func i64tob(v int64) []byte {
	b := make([]byte, timeStampWidth)
	binary.BigEndian.PutUint64(b, uint64(v))

	return b
}

type localDataEntry struct {
	fi    *flatIndex
	entry *flatIndexEntry
	start int
}

// Scroll returns all the hits that pass certain match and prefix filter terms
// in the given query, in the query's timestamp date range (which must be
// expressed with specific lte and gte RFC3339 values).
//
// To avoid memory allocations and increase performance, the returned Result
// Details are unsafely backed by a pool of byte slices. It is only safe to
// release these to the pool once you are done with the Result. To avoid a
// memory leak, you must signify when you are done by calling
// Done(result.PoolKey).
func (d *DB) Scroll(query *es.Query) (*es.Result, error) {
	filter, err := newFlatFilter(query)
	if err != nil {
		return nil, err
	}

	var (
		mu      sync.Mutex
		numHits int
		lenHits int
	)

	allLDEs := make(map[string][]localDataEntry)

	d.operateOnRequestedDays(filter, func(fi *flatIndex) {
		entries := fi.IndexSearch(filter)
		if len(entries) == 0 {
			return
		}

		ldes := make([]localDataEntry, len(entries))

		mu.Lock()
		defer mu.Unlock()

		for i, entry := range entries {
			ldes[i] = localDataEntry{
				fi:    fi,
				entry: entry,
				start: lenHits,
			}
			lenHits += entry.length
		}

		numHits += len(entries)

		allLDEs[fi.dataPath] = append(allLDEs[fi.dataPath], ldes...)
	})

	hits := make([]es.Hit, numHits)
	result := &es.Result{
		ScrollID: pretendScrollID,
		HitSet: &es.HitSet{
			Total: es.HitSetTotal{Value: numHits},
			Hits:  hits,
		},
	}

	if numHits == 0 {
		return result, nil
	}

	buf, poolKey := d.bufPool.Get(lenHits)
	result.PoolKey = poolKey
	hitI := 0
	eg := errgroup.Group{}

	for _, ldes := range allLDEs {
		startingHitIndex := hitI
		theseLDEs := ldes

		eg.Go(func() error {
			return d.getIndexEntriesHits(buf, theseLDEs, filter.desiredFields, hits, startingHitIndex)
		})

		hitI += len(ldes)
	}

	err = eg.Wait()

	return result, err
}

func (d *DB) getIndexEntriesHits(buf []byte, ldes []localDataEntry, fields es.Fields,
	hits []es.Hit, hitIndex int) error {
	for _, lde := range ldes {
		data := buf[lde.start : lde.start+lde.entry.length]

		err := lde.fi.getDataEntry(data, lde.entry)
		if err != nil {
			return err
		}

		details, err := es.DeserializeDetails(data, fields)
		if err != nil {
			return err
		}

		hits[hitIndex] = es.Hit{
			ID:      details.ID,
			Details: details,
		}

		hitIndex++
	}

	ldes[0].fi.close()

	return nil
}

func (d *DB) operateOnRequestedDays(filter *flatFilter, cb func(*flatIndex)) {
	currentDay := filter.GTE

	var wg sync.WaitGroup

	for {
		d.muDateBOMDirs.RLock()
		indexes := d.dateBOMDirs[filepath.Join(d.dateFolder(currentDay), filter.BOM)]
		d.muDateBOMDirs.RUnlock()

		wg.Add(len(indexes))

		for _, index := range indexes {
			go func(dbIndex *flatIndex) {
				defer wg.Done()

				cb(dbIndex)
			}(index)
		}

		currentDay = currentDay.Add(oneDay)

		if filter.beyondLastDate(currentDay) {
			break
		}
	}

	wg.Wait()
}

func (d *DB) dateFolder(day time.Time) string {
	return fmt.Sprintf("%s/%s", d.dir, day.UTC().Format(dateFormat))
}

// Done must be called when you have finished using the Result.PoolKey returned
// by Scroll(). It releases byte slices back to a pool so you don't run out of
// memory. Returns true if there were slices associated with the given PoolKey,
// false if it did nothing because there were not.
func (d *DB) Done(poolKey int) bool {
	return d.bufPool.Done(poolKey)
}

// Usernames is like Scroll(), but picks out and returns only the unique
// usernames from amongst the Hits.
func (d *DB) Usernames(query *es.Query) ([]string, error) {
	filter, err := newFlatFilter(query)
	if err != nil {
		return nil, err
	}

	var mu sync.Mutex

	usernamesMap := make(map[string]bool)

	d.operateOnRequestedDays(filter, func(fi *flatIndex) {
		theseUsernames := fi.Usernames(filter)

		mu.Lock()
		defer mu.Unlock()

		for name := range theseUsernames {
			usernamesMap[strings.TrimSpace(name)] = true
		}
	})

	usernames := make([]string, 0, len(usernamesMap))
	for username := range usernamesMap {
		usernames = append(usernames, username)
	}

	return usernames, nil
}

// Close stops any ongoing monitoring cleanly.
func (d *DB) Close() error {
	if d.stopMonitoring != nil {
		d.stopMonitoring <- true
	}

	return nil
}
