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
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"

	es "github.com/wtsi-hgi/go-farmer/elasticsearch"
)

type flatDB struct {
	path       string
	fileSize   int
	bufferSize int

	f     *os.File
	w     *bufio.Writer
	n     int
	index int
}

func newFlatDB(path string, fileSize, bufferSize int) (*flatDB, error) {
	f, w, err := createFileAndWriter(path, 0, bufferSize)
	if err != nil {
		return nil, err
	}

	return &flatDB{
		path:       path,
		fileSize:   fileSize,
		bufferSize: bufferSize,

		f: f,
		w: w,
	}, nil
}

func createFileAndWriter(path string, index, bufferSize int) (*os.File, *bufio.Writer, error) {
	err := os.MkdirAll(path, dbDirPerms)
	if err != nil {
		return nil, nil, err
	}

	f, err := os.Create(fmt.Sprintf("%s/%d", path, index))
	if err != nil {
		return nil, nil, err
	}

	w := bufio.NewWriterSize(f, bufferSize)

	return f, w, nil
}

func (f *flatDB) Store(fields ...[]byte) error {
	total := 0

	for _, field := range fields {
		n, err := f.w.Write(field)
		total += n

		if err != nil {
			return err
		}
	}

	f.n += total
	if f.n > f.fileSize {
		if err := f.switchToNewFile(); err != nil {
			return err
		}
	}

	return nil
}

func (f *flatDB) switchToNewFile() error {
	err := f.Close()
	if err != nil {
		return err
	}

	f.index++

	file, w, err := createFileAndWriter(f.path, f.index, f.bufferSize)
	if err != nil {
		return err
	}

	f.f = file
	f.w = w
	f.n = 0

	return nil
}

func (f *flatDB) Close() error {
	f.w.Flush()

	return f.f.Close()
}

func scrollFlatFile(dbFilePath string, filter *flatFilter, result *es.Result, fileBufferSize int) error { //nolint:funlen,gocognit,gocyclo,cyclop,lll
	f, err := os.Open(dbFilePath)
	if err != nil {
		return err
	}

	tsBuf := make([]byte, timeStampWidth)
	accBuf := make([]byte, accountingNameWidth)
	userBuf := make([]byte, userNameWidth)
	lenBuf := make([]byte, lengthEncodeWidth)
	detailsBuf := make([]byte, detailsBufferLength)
	br := bufio.NewReaderSize(f, fileBufferSize)

	for {
		_, err = io.ReadFull(br, tsBuf)
		if err != nil {
			if !errors.Is(err, io.EOF) {
				return err
			}

			break
		}

		if bytes.Compare(tsBuf, filter.LTEKey) > 0 {
			break
		}

		check := filter.PassChecker()

		if bytes.Compare(tsBuf, filter.GTEKey) < 0 {
			check.Fail()
		}

		_, err = io.ReadFull(br, accBuf)
		if err != nil {
			return err
		}

		check.AccountingName(accBuf)

		_, err = io.ReadFull(br, userBuf)
		if err != nil {
			return err
		}

		check.UserName(userBuf)

		gpuByte, err := br.ReadByte()
		if err != nil {
			return err
		}

		check.GPU(gpuByte)

		_, err = io.ReadFull(br, lenBuf)
		if err != nil {
			return err
		}

		detailsLength := btoi(lenBuf)

		buf := detailsBuf[0:detailsLength]

		_, err = io.ReadFull(br, buf)
		if err != nil {
			return err
		}

		if check.Passes() {
			d, err := es.DeserializeDetails(buf)
			if err != nil {
				return err
			}

			result.AddHitDetails(d)
		}
	}

	return f.Close()
}

func btoi(b []byte) int {
	return int(binary.BigEndian.Uint32(b[0:4]))
}
