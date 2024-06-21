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
	"os"
	"path/filepath"
	"strings"
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
	notInGPUQueue       = byte(1)
	inGPUQueue          = byte(2)
	lengthEncodeWidth   = 4
	detailsBufferLength = 16 * 1024

	dateFormat = "2006/01/02"
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

// DB represents a local database that uses a number of flat files to store
// elasticsearch hit details and return them quickly.
type DB struct {
	dir        string
	fileSize   int
	bufferSize int
	dbs        map[string]*flatDB
}

// New returns a DB that will create or use the database files in the given
// directory. Files created will be split if they get over the given fileSize in
// bytes. Files will be read and written using a bufferSize buffer in bytes.
func New(dir string, fileSize, bufferSize int) (*DB, error) {
	err := os.MkdirAll(dir, dbDirPerms)
	if err != nil {
		return nil, err
	}

	return &DB{
		dir:        dir,
		fileSize:   fileSize,
		bufferSize: bufferSize,
		dbs:        make(map[string]*flatDB),
	}, nil
}

// Store stores the Details in the Hits of the given Result in flat database
// files in our directory, that can later be retrieved via Scroll(). Call
// Close() after using this for the last time.
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
	if strings.HasPrefix(hit.Details.QueueName, "gpu") {
		isGPU = inGPUQueue
	}

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
