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
	"os"
	"path/filepath"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
	es "github.com/wtsi-hgi/go-farmer/elasticsearch"
)

const (
	fileSize   = 512 * 1024
	bufferSize = 256 * 1024
)

func TestDB(t *testing.T) {
	Convey("Given a directory, you can make a new database", t, func() {
		tmpDir := t.TempDir()
		dbDir := filepath.Join(tmpDir, "db")

		db, err := New(dbDir, fileSize, bufferSize)
		So(err, ShouldBeNil)
		So(db, ShouldNotBeNil)

		defer func() {
			err = db.Close()
			So(err, ShouldBeNil)
		}()

		_, err = os.Stat(dbDir)
		So(err, ShouldBeNil)

		Convey("Which lets you store elasticsearch results", func() {
			gteStr := "2024-02-04T00:00:00Z"
			lteStr := "2024-02-06T00:00:00Z"

			gte, err := time.Parse(time.RFC3339, gteStr)
			So(err, ShouldBeNil)

			lte, err := time.Parse(time.RFC3339, lteStr)
			So(err, ShouldBeNil)

			result := makeResult(gte, lte)
			expectedNumHits := 172800
			So(result.HitSet.Total.Value, ShouldEqual, expectedNumHits)

			err = db.Store(result)
			So(err, ShouldBeNil)

			err = db.Close()
			So(err, ShouldBeNil)

			entries, err := os.ReadDir(dbDir)
			So(err, ShouldBeNil)
			So(len(entries), ShouldEqual, 1)
			So(entries[0].IsDir(), ShouldBeTrue)
			So(entries[0].Name(), ShouldEqual, "2024")

			dir := filepath.Join(dbDir, "2024")
			entries, err = os.ReadDir(dir)
			So(err, ShouldBeNil)
			So(len(entries), ShouldEqual, 1)
			So(entries[0].IsDir(), ShouldBeTrue)
			So(entries[0].Name(), ShouldEqual, "02")

			dir = filepath.Join(dir, "02")
			entries, err = os.ReadDir(dir)
			So(err, ShouldBeNil)
			So(len(entries), ShouldEqual, 2)
			So(entries[0].IsDir(), ShouldBeTrue)
			So(entries[0].Name(), ShouldEqual, "04")
			So(entries[1].IsDir(), ShouldBeTrue)
			So(entries[1].Name(), ShouldEqual, "05")

			dir = filepath.Join(dir, "04")
			entries, err = os.ReadDir(dir)
			So(err, ShouldBeNil)
			So(len(entries), ShouldEqual, 2)
			So(entries[0].IsDir(), ShouldBeTrue)
			So(entries[0].Name(), ShouldEqual, "bomA")
			So(entries[1].IsDir(), ShouldBeTrue)
			So(entries[1].Name(), ShouldEqual, "bomB")

			dir = filepath.Join(dir, "bomA")
			entries, err = os.ReadDir(dir)
			So(err, ShouldBeNil)
			So(len(entries), ShouldEqual, 14)
			So(entries[0].Type().IsRegular(), ShouldBeTrue)
			So(entries[0].Name(), ShouldEqual, "0")
			So(entries[13].Type().IsRegular(), ShouldBeTrue)
			So(entries[13].Name(), ShouldEqual, "9")
			So(entries[5].Type().IsRegular(), ShouldBeTrue)
			So(entries[5].Name(), ShouldEqual, "13")

			filePath := filepath.Join(dir, "0")
			b, err := os.ReadFile(filePath)
			So(err, ShouldBeNil)

			So(b[0:timeStampWidth], ShouldResemble, []byte{0, 0, 0, 0, 101, 190, 211, 129})

			stamp := timeStampBytesToFormatString(b[0:timeStampWidth])
			So(stamp, ShouldEqual, "2024-02-04T00:00:01Z")

			nextFieldStart := timeStampWidth
			So(string(b[nextFieldStart:nextFieldStart+accountingNameWidth]), ShouldEqual, "groupA                  ")

			nextFieldStart += accountingNameWidth
			So(string(b[nextFieldStart:nextFieldStart+userNameWidth]), ShouldEqual, "userA       ")

			nextFieldStart += userNameWidth
			So(b[nextFieldStart:nextFieldStart+1], ShouldEqual, []byte{notInGPUQueue})

			nextFieldStart++
			detailsLen := int(binary.BigEndian.Uint32(b[nextFieldStart : nextFieldStart+lengthEncodeWidth]))
			expectedDetailsLen := 117
			So(detailsLen, ShouldEqual, expectedDetailsLen)

			nextFieldStart += lengthEncodeWidth
			detailsBytes := b[nextFieldStart : nextFieldStart+detailsLen]
			details, err := es.DeserializeDetails(detailsBytes)
			So(err, ShouldBeNil)

			timeStamp, err := time.Parse(time.RFC3339, "2024-02-04T00:00:01Z")
			So(err, ShouldBeNil)

			So(details, ShouldResemble, result.HitSet.Hits[1].Details)
			So(details.Timestamp, ShouldEqual, timeStamp.Unix())

			nextFieldStart += detailsLen
			stamp = timeStampBytesToFormatString(b[nextFieldStart : nextFieldStart+timeStampWidth])
			So(stamp, ShouldEqual, "2024-02-04T00:00:03Z")

			dir = filepath.Join(dbDir, "2024", "02", "05", "bomA")
			entries, err = os.ReadDir(dir)
			So(err, ShouldBeNil)
			So(len(entries), ShouldEqual, 14)

			filePath = filepath.Join(dir, "13")
			b, err = os.ReadFile(filePath)
			So(err, ShouldBeNil)

			nextFieldStart = len(b) - expectedDetailsLen
			detailsBytes = b[nextFieldStart:]
			details, err = es.DeserializeDetails(detailsBytes)
			So(err, ShouldBeNil)
			So(details, ShouldResemble, result.HitSet.Hits[expectedNumHits-1].Details)
		})
	})
}

func makeResult(gte, lte time.Time) *es.Result {
	result := &es.Result{
		HitSet: &es.HitSet{},
	}

	hits := 0
	timeStamp := gte

	for timeStamp.Before(lte) {
		bom := "bomA"
		aName := "groupA"
		uName := "userA"
		qName := "normal"
		jName := "jobA"

		if hits%2 == 0 {
			bom = "bomB"
		}

		if hits%5 == 0 {
			aName = "groupB"
		}

		if hits%9 == 0 {
			uName = "userB"
		}

		if hits%16 == 0 {
			qName = "gpu-normal"
		}

		if hits%19 == 0 {
			jName = ""
		}

		result.HitSet.AddHit("", &es.Details{
			Timestamp:         timeStamp.Unix(),
			BOM:               bom,
			AccountingName:    aName,
			UserName:          uName,
			QueueName:         qName,
			Command:           "cmd",
			JobName:           jName,
			Job:               "job",
			MemRequestedMB:    1,
			MemRequestedMBSec: 2,
			NumExecProcs:      3,
			PendingTimeSec:    4,
			RunTimeSec:        5,
			WastedCPUSeconds:  6.1,
			WastedMBSeconds:   7.1,
		})

		hits++
		timeStamp = timeStamp.Add(1 * time.Second)
	}

	result.HitSet.Total = es.HitSetTotal{
		Value: hits,
	}

	return result
}

func timeStampBytesToFormatString(b []byte) string {
	return time.Unix(int64(binary.BigEndian.Uint64(b)), 0).UTC().Format(time.RFC3339)
}
