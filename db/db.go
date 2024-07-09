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
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/deneonet/benc/bpre"
	es "github.com/wtsi-hgi/go-farmer/elasticsearch"
)

const (
	ErrFieldTooLong = "field value exceeds expected width"

	dbDirPerms = 0770

	timeStampWidth      = 8
	bomWidth            = 34
	accountingNameWidth = 24
	userNameWidth       = 12
	gpuPrefix           = "gpu"
	notInGPUQueue       = byte(1)
	inGPUQueue          = byte(2)
	lengthEncodeWidth   = 4
	detailsBufferLength = 16 * 1024
	defaultFileSize     = 32 * 1024 * 1024
	defaultBufferSize   = 4 * 1024 * 1024

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

type Config struct {
	Directory  string
	FileSize   int
	BufferSize int
}

// DB represents a local database that uses a number of flat files to store
// elasticsearch hit details and return them quickly.
type DB struct {
	dir         string
	fileSize    int
	bufferSize  int
	dbs         map[string]*flatDB
	dateBOMDirs map[string][]string
}

// New returns a DB that will create or use the database files in the configured
// Directory. Files created will be split if they get over the configured
// FileSize in bytes (default 32MB). Files will be read and written using a
// BufferSize buffer in bytes (default 4MB).
func New(config Config) (*DB, error) {
	dateBOMDirs := make(map[string][]string)

	_, err := os.Stat(config.Directory)
	if err == nil {
		err = findFlatFiles(config.Directory, dateBOMDirs)
	} else {
		err = os.MkdirAll(config.Directory, dbDirPerms)
	}

	if err != nil {
		return nil, err
	}

	fileSize := config.FileSize
	if fileSize == 0 {
		fileSize = defaultFileSize
	}

	bufferSize := config.BufferSize
	if bufferSize == 0 {
		bufferSize = defaultBufferSize
	}

	return &DB{
		dir:         config.Directory,
		fileSize:    fileSize,
		bufferSize:  bufferSize,
		dbs:         make(map[string]*flatDB),
		dateBOMDirs: dateBOMDirs,
	}, nil
}

func findFlatFiles(dir string, dateBOMDirs map[string][]string) error {
	return filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		if !d.Type().IsRegular() {
			return nil
		}

		subDir := filepath.Dir(path)

		dateBOMDirs[subDir] = append(dateBOMDirs[subDir], path)

		return nil
	})
}

// Store stores the Details in the Hits of the given Result in flat database
// files in our directory, that can later be retrieved via Scroll(). Call
// Close() after using this for the last time.
//
// NB: What you Store() with this DB will not be available to Scroll(). You will
// need to Close() this DB and make a New() one to Scroll() the stored hits.
func (d *DB) Store(result *es.Result) error {
	bpre.Marshal(detailsBufferLength)

	for _, hit := range result.HitSet.Hits {
		group, user, isGPU, encodedDetails, err := getFixedWidthFields(hit)
		if err != nil {
			return err
		}

		fdb, err := d.createOrGetFlatDB(hit.Details.Timestamp, hit.Details.BOM)
		if err != nil {
			return err
		}

		if err = fdb.Store(
			i64tob(hit.Details.Timestamp),
			group,
			user,
			[]byte{isGPU},
			i32tob(int32(len(encodedDetails))),
			encodedDetails,
		); err != nil {
			return err
		}
	}

	return nil
}

func getFixedWidthFields(hit es.Hit) ([]byte, []byte, byte, []byte, error) {
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

func (d *DB) createOrGetFlatDB(timeStamp int64, bom string) (*flatDB, error) {
	key := fmt.Sprintf("%s/%s", time.Unix(timeStamp, 0).UTC().Format(dateFormat), bom)

	fdb, ok := d.dbs[key]
	if !ok {
		var err error

		fdb, err = newFlatDB(filepath.Join(d.dir, key), d.fileSize, d.bufferSize)
		if err != nil {
			return nil, err
		}

		d.dbs[key] = fdb
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

// i32tob is like i64tob, but for int32s.
func i32tob(v int32) []byte {
	b := make([]byte, lengthEncodeWidth)
	binary.BigEndian.PutUint32(b, uint32(v))

	return b
}

// Scroll returns all the hits that pass certain match and prefix filter terms
// in the given query, in the query's timestamp date range (which must be
// expressed with specific lte and gte RFC3339 values).
func (d *DB) Scroll(query *es.Query) (*es.Result, error) {
	filter, err := newFlatFilter(query)
	if err != nil {
		return nil, err
	}

	result := es.NewResult()

	var wg sync.WaitGroup

	d.scrollRequestedDays(&wg, filter, result, query.Source)

	wg.Wait()

	errors := result.Errors()
	if len(errors) > 0 {
		return nil, errors[0]
	}

	result.ScrollID = pretendScrollID

	return result, nil
}

func (d *DB) scrollRequestedDays(wg *sync.WaitGroup, filter *flatFilter, result *es.Result, fields []string) {
	currentDay := filter.GTE

	for {
		fileKey := currentDay.UTC().Format(dateFormat)
		paths := d.dateBOMDirs[fmt.Sprintf("%s/%s/%s", d.dir, fileKey, filter.BOM)]

		d.scrollFlatFilesAndHandleErrors(wg, paths, filter, result, fields)

		currentDay = currentDay.Add(oneDay)

		if filter.beyondLastDate(currentDay) {
			break
		}
	}
}

func (d *DB) scrollFlatFilesAndHandleErrors(wg *sync.WaitGroup, paths []string,
	filter *flatFilter, result *es.Result, fields []string) {
	wg.Add(len(paths))

	for _, path := range paths {
		go func(dbFilePath string) {
			defer wg.Done()

			if err := scrollFlatFile(dbFilePath, filter, result, fields, d.bufferSize); err != nil {
				result.AddError(err)
			}
		}(path)
	}
}

// Close closes any open filehandles. You must call this after your last use of
// Store(), or your database files will be corrupt.
func (d *DB) Close() error {
	for _, fdb := range d.dbs {
		if err := fdb.Close(); err != nil && !errors.Is(err, os.ErrClosed) {
			return err
		}
	}

	return nil
}
