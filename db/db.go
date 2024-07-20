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
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/deneonet/benc"
	es "github.com/wtsi-hgi/go-farmer/elasticsearch"
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
	Directory       string
	FileSize        int           // FileSize defaults to 32MB
	BufferSize      int           // BufferSize defaults to 4MB
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
	dir             string
	fileSize        int
	bufferSize      int
	bufPool         *benc.BufPool
	dbs             map[string]*flatDB
	dateBOMDirs     map[string]string
	updateFrequency time.Duration
	latestDate      time.Time
	stopMonitoring  chan bool
	mu              sync.RWMutex
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
func New(config Config) (*DB, error) {
	db := &DB{
		dir:             config.Directory,
		fileSize:        config.FileSizeOrDefault(),
		bufferSize:      config.BufferSizeOrDefault(),
		bufPool:         benc.NewBufPool(benc.WithBufferSize(es.MaxEncodedDetailsLength)),
		dbs:             make(map[string]*flatDB),
		dateBOMDirs:     make(map[string]string),
		updateFrequency: config.UpdateFrequencyOrDefault(),
	}

	_, err := os.Stat(config.Directory)
	if err == nil {
		err = db.findAllFlatFiles(db.dir)
		if err == nil {
			db.monitorFlatFiles()
		}
	} else {
		err = os.MkdirAll(config.Directory, dbDirPerms)
	}

	return db, err
}

func (d *DB) findAllFlatFiles(dir string) error {
	return filepath.WalkDir(dir, func(path string, de fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		if !de.Type().IsRegular() || de.Name() != sqlfileBasename {
			return nil
		}

		subDir := filepath.Dir(path)

		d.mu.Lock()
		defer d.mu.Unlock()

		d.dateBOMDirs[subDir] = path

		return d.updateLatestDate(filepath.Dir(subDir))
	})
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

func (d *DB) monitorFlatFiles() {
	ticker := time.NewTicker(d.updateFrequency)
	d.stopMonitoring = make(chan bool)

	go func() {
		for {
			select {
			case <-ticker.C:
				d.findLatestFlatFiles()
			case <-d.stopMonitoring:
				ticker.Stop()

				return
			}
		}
	}()
}

func (d *DB) findLatestFlatFiles() {
	currentDay := d.latestDate.Add(oneDay)
	maxDay := time.Now()

	for {
		dateFolder := d.dateFolder(currentDay)

		_, err := os.Stat(dateFolder)
		if err == nil {
			err = d.findAllFlatFiles(dateFolder)
			if err != nil {
				slog.Error("findLatestFlatFiles failed", "err", err)
			}
		}

		currentDay = currentDay.Add(oneDay)

		if currentDay.After(maxDay) {
			break
		}
	}
}

// Store stores the Details in the Hits of the given Result in flat database
// files in our directory, that can later be retrieved via Scroll(). Call
// Close() after using this for the last time.
//
// NB: What you Store() with this DB will not be available to Scroll(). You will
// need to Close() this DB and make a New() one to Scroll() the stored hits.
func (d *DB) Store(result *es.Result) error {
	for _, hit := range result.HitSet.Hits {
		fdb, err := d.createOrGetFlatDB(hit.Details.Timestamp, hit.Details.BOM)
		if err != nil {
			return err
		}

		if err = fdb.Store(hit); err != nil {
			return err
		}
	}

	return nil
}

func (d *DB) createOrGetFlatDB(timeStamp int64, bom string) (*flatDB, error) {
	key := fmt.Sprintf("%s/%s", time.Unix(timeStamp, 0).UTC().Format(dateFormat), bom)

	d.mu.Lock()
	defer d.mu.Unlock()

	fdb, ok := d.dbs[key]
	if !ok {
		var err error

		fdb, err = newFlatDB(filepath.Join(d.dir, key))
		if err != nil {
			return nil, err
		}

		d.dbs[key] = fdb
	}

	return fdb, nil
}

// Scroll returns all the hits that pass certain match and prefix filter terms
// in the given query, in the query's timestamp date range (which must be
// expressed with specific lte and gte RFC3339 values).
func (d *DB) Scroll(query *es.Query) (*es.Result, error) {
	filter, err := newSQLFilter(query)
	if err != nil {
		return nil, err
	}

	result := es.NewResult()

	var wg sync.WaitGroup

	d.scrollRequestedDays(&wg, filter, result)

	wg.Wait()

	errors := result.Errors()
	if len(errors) > 0 {
		return nil, errors[0]
	}

	result.ScrollID = pretendScrollID

	return result, nil
}

func (d *DB) scrollRequestedDays(wg *sync.WaitGroup, filter *sqlFilter, result *es.Result) {
	currentDay := time.Unix(filter.GTE, 0)

	for {
		d.mu.RLock()
		path, found := d.dateBOMDirs[filepath.Join(d.dateFolder(currentDay), filter.bom)]
		d.mu.RUnlock()

		if !found {
			break
		}

		d.scrollFlatFilesAndHandleErrors(wg, path, filter, result)

		currentDay = currentDay.Add(oneDay)

		if filter.beyondLastDate(currentDay) {
			break
		}
	}
}

func (d *DB) dateFolder(day time.Time) string {
	return fmt.Sprintf("%s/%s", d.dir, day.UTC().Format(dateFormat))
}

func (d *DB) scrollFlatFilesAndHandleErrors(wg *sync.WaitGroup, path string,
	filter *sqlFilter, result *es.Result) {
	wg.Add(1)

	go func() {
		defer wg.Done()

		if err := scrollFlatFile(path, filter, result); err != nil {
			result.AddError(err)
		}
	}()
}

// Usernames is like Scroll(), but picks out and returns only the unique
// usernames from amongst the Hits.
func (d *DB) Usernames(query *es.Query) ([]string, error) {
	r, err := d.Scroll(query)
	if err != nil {
		return nil, err
	}

	usernamesMap := make(map[string]bool)

	for _, hit := range r.HitSet.Hits {
		usernamesMap[hit.Details.UserName] = true
	}

	usernames := make([]string, 0, len(usernamesMap))
	for username := range usernamesMap {
		usernames = append(usernames, username)
	}

	return usernames, nil
}

// Close closes any open filehandles. You must call this after your last use of
// Store(), or your database files will be corrupt.
func (d *DB) Close() error {
	for _, fdb := range d.dbs {
		if err := fdb.Close(); err != nil && !errors.Is(err, os.ErrClosed) {
			return err
		}
	}

	if d.stopMonitoring != nil {
		d.stopMonitoring <- true
	}

	return nil
}
