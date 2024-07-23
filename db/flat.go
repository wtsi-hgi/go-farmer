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
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
)

const (
	indexKind           = "index"
	dataKind            = "data"
	entriesKeySeparator = "."
)

type flatDB struct {
	dir             string
	desiredFileSize int
	bufferSize      int

	indexF *os.File
	indexW *bufio.Writer

	dataF         *os.File
	dataW         *bufio.Writer
	dataPos       int
	dataFileIndex int
}

func newFlatDB(dir string, fileSize, bufferSize int) (*flatDB, error) {
	f := &flatDB{
		dir:             dir,
		desiredFileSize: fileSize,
		bufferSize:      bufferSize,
	}

	err := f.createFilesAndWriters()

	return f, err
}

func (f *flatDB) createFilesAndWriters() error {
	var err error

	f.indexF, f.indexW, err = f.createFileAndWriter(indexKind)
	if err != nil {
		return err
	}

	f.dataF, f.dataW, err = f.createFileAndWriter(dataKind)

	return err
}

func (f *flatDB) createFileAndWriter(kind string) (*os.File, *bufio.Writer, error) {
	err := os.MkdirAll(f.dir, dbDirPerms)
	if err != nil {
		return nil, nil, err
	}

	fh, err := os.Create(fmt.Sprintf("%s/%d.%s", f.dir, f.dataFileIndex, kind))
	if err != nil {
		return nil, nil, err
	}

	w := bufio.NewWriterSize(fh, f.bufferSize)

	return fh, w, nil
}

func (f *flatDB) Store(indexFields [][]byte, data []byte) error {
	n, err := f.dataW.Write(data)
	if err != nil {
		return err
	}

	dataIndex := i32tob(int32(f.dataPos))
	dataLen := i32tob(int32(len(data)))
	indexFields = append(indexFields, dataIndex, dataLen)

	for _, field := range indexFields {
		_, err := f.indexW.Write(field)

		if err != nil {
			return err
		}
	}

	f.dataPos += n
	if f.dataPos > f.desiredFileSize {
		if err := f.switchToNewFiles(); err != nil {
			return err
		}
	}

	return nil
}

// i32tob is like i64tob, but for int32s.
func i32tob(v int32) []byte {
	b := make([]byte, lengthEncodeWidth)
	binary.BigEndian.PutUint32(b, uint32(v))

	return b
}

func (f *flatDB) switchToNewFiles() error {
	err := f.Close()
	if err != nil {
		return err
	}

	f.dataFileIndex++

	err = f.createFilesAndWriters()
	if err != nil {
		return err
	}

	f.dataPos = 0

	return nil
}

func (f *flatDB) Close() error {
	f.indexW.Flush()
	f.dataW.Flush()

	err := f.indexF.Close()
	if err != nil {
		return err
	}

	return f.dataF.Close()
}

type flatIndexEntry struct {
	timeStamp []byte
	gpu       byte
	userName  string
	index     int64
	length    int
}

// Passes first bool will be false if LT doesn't pass. The second bool will be
// true if everything passed the passChecker.
func (e *flatIndexEntry) Passes(check *passChecker) (bool, bool) {
	check.LT(e.timeStamp)

	if !check.Passes() {
		return false, false
	}

	check.GTE(e.timeStamp)
	check.GPU(e.gpu)

	return true, check.Passes()
}

type flatIndex struct {
	bomEntries       []*flatIndexEntry
	groupEntries     map[string][]*flatIndexEntry
	userEntries      map[string][]*flatIndexEntry
	groupUserEntries map[string][]*flatIndexEntry

	dataPath string
	fh       *os.File
}

func newFlatIndex(path string, fileBufferSize int) (*flatIndex, error) { //nolint:funlen,gocognit,gocyclo
	f, erro := os.Open(path)
	if erro != nil {
		return nil, erro
	}

	br := bufio.NewReaderSize(f, fileBufferSize)

	fi := &flatIndex{
		dataPath:         strings.TrimSuffix(path, indexKind) + dataKind,
		groupEntries:     make(map[string][]*flatIndexEntry),
		userEntries:      make(map[string][]*flatIndexEntry),
		groupUserEntries: make(map[string][]*flatIndexEntry),
	}

	for {
		entry := &flatIndexEntry{}

		tsBuf := make([]byte, timeStampWidth)
		_, err := io.ReadFull(br, tsBuf)
		if err != nil {
			if !errors.Is(err, io.EOF) {
				return nil, err
			}

			break
		}

		entry.timeStamp = tsBuf

		accBuf := make([]byte, accountingNameWidth)
		if _, err = io.ReadFull(br, accBuf); err != nil {
			return nil, err
		}

		userBuf := make([]byte, userNameWidth)
		if _, err = io.ReadFull(br, userBuf); err != nil {
			return nil, err
		}

		gpuByte, err := br.ReadByte()
		if err != nil {
			return nil, err
		}

		entry.gpu = gpuByte

		numBuf := make([]byte, lengthEncodeWidth)
		if _, err = io.ReadFull(br, numBuf); err != nil {
			return nil, err
		}

		entry.index = int64(btoi(numBuf))

		lenBuf := make([]byte, lengthEncodeWidth)
		if _, err = io.ReadFull(br, lenBuf); err != nil {
			return nil, err
		}

		entry.length = btoi(lenBuf)

		group := strings.TrimSpace(string(accBuf))
		user := strings.TrimSpace(string(userBuf))
		entry.userName = user

		fi.bomEntries = append(fi.bomEntries, entry)
		fi.groupEntries[group] = append(fi.groupEntries[group], entry)
		fi.userEntries[user] = append(fi.userEntries[user], entry)

		groupUserKey := group + entriesKeySeparator + user
		fi.groupUserEntries[groupUserKey] = append(fi.groupUserEntries[groupUserKey], entry)
	}

	errc := f.Close()

	return fi, errc
}

func btoi(b []byte) int {
	return int(binary.BigEndian.Uint32(b[0:4]))
}

func (f *flatIndex) IndexSearch(filter *flatFilter) []*flatIndexEntry {
	entries := f.getEntries(filter)
	check := filter.PassChecker()

	passEntries := make([]*flatIndexEntry, 0, len(entries))

	for _, entry := range entries {
		continueOK, passes := entry.Passes(check)
		if !continueOK {
			break
		}

		if !passes {
			continue
		}

		passEntries = append(passEntries, entry)
	}

	return passEntries
}

func (f *flatIndex) getEntries(filter *flatFilter) []*flatIndexEntry {
	entries := f.bomEntries

	if filter.checkUser {
		if filter.checkAccounting {
			entries = f.groupUserEntries[filter.accountingName+entriesKeySeparator+filter.userName]
		} else {
			entries = f.userEntries[filter.userName]
		}
	} else if filter.checkAccounting {
		entries = f.groupEntries[filter.accountingName]
	}

	return entries
}

func (f *flatIndex) getDataEntry(buf []byte, entry *flatIndexEntry) error {
	err := f.openDataFile()
	if err != nil {
		return err
	}

	n, err := f.fh.ReadAt(buf, entry.index)
	if err != nil && n == entry.length {
		err = nil
	}

	return err
}

func (f *flatIndex) openDataFile() error {
	if f.fh != nil {
		return nil
	}

	fh, err := os.Open(f.dataPath)
	if err != nil {
		return err
	}

	f.fh = fh

	return nil
}

func (f *flatIndex) close() {
	if f.fh != nil {
		return
	}

	f.fh.Close()
	f.fh = nil
}

func (f *flatIndex) Usernames(filter *flatFilter) map[string]bool {
	entries := f.getEntries(filter)
	check := filter.PassChecker()

	usernames := make(map[string]bool)

	for _, entry := range entries {
		continueOK, passes := entry.Passes(check)
		if !continueOK {
			break
		}

		if !passes {
			continue
		}

		usernames[entry.userName] = true
	}

	return usernames
}
