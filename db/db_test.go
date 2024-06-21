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
	"sort"
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

			So(len(b), ShouldBeGreaterThanOrEqualTo, fileSize)
			So(len(b), ShouldBeLessThan, fileSize*2)

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

			Convey("Which you can then retrieve via Scroll()", func() {
				query := &es.Query{
					Query: &es.QueryFilter{Bool: es.QFBool{Filter: es.Filter{
						{"match_phrase": map[string]interface{}{"META_CLUSTER_NAME": "farm"}},
						{"range": map[string]interface{}{
							"timestamp": map[string]string{
								"lte":    lteStr,
								"gte":    gteStr,
								"format": "strict_date_optional_time",
							},
						}},
					}}},
				}

				_, err = db.Scroll(query)
				So(err, ShouldNotBeNil)

				bomMatch := map[string]es.MapStringStringOrMap{"match_phrase": map[string]interface{}{"BOM": "bomA"}}
				query.Query.Bool.Filter = append(query.Query.Bool.Filter, bomMatch)
				retrieved, err := db.Scroll(query)
				So(err, ShouldBeNil)
				So(retrieved.HitSet.Hits, ShouldBeNil)

				db, err = New(dbDir, fileSize, bufferSize)
				So(err, ShouldBeNil)

				retrieved, err = db.Scroll(query)
				So(err, ShouldBeNil)
				So(retrieved.HitSet, ShouldNotBeNil)

				expectedBomHits := expectedNumHits / 2
				So(len(retrieved.HitSet.Hits), ShouldEqual, expectedBomHits)

				firstHitIndex := -1
				lastHitIndex := -1

				for i, retrievedHit := range retrieved.HitSet.Hits {
					switch retrievedHit.Details.Timestamp {
					case result.HitSet.Hits[1].Details.Timestamp:
						firstHitIndex = i
					case result.HitSet.Hits[expectedNumHits-1].Details.Timestamp:
						lastHitIndex = i
					}
				}

				So(firstHitIndex, ShouldNotEqual, -1)
				So(retrieved.HitSet.Hits[firstHitIndex].Details, ShouldResemble, result.HitSet.Hits[1].Details)
				So(lastHitIndex, ShouldNotEqual, -1)
				So(retrieved.HitSet.Hits[lastHitIndex].Details, ShouldResemble, result.HitSet.Hits[expectedNumHits-1].Details)

				aMatch := map[string]es.MapStringStringOrMap{"match_phrase": map[string]interface{}{"ACCOUNTING_NAME": "groupA"}}
				query.Query.Bool.Filter = append(query.Query.Bool.Filter, aMatch)
				retrieved, err = db.Scroll(query)
				So(err, ShouldBeNil)
				So(retrieved.HitSet, ShouldNotBeNil)
				So(len(retrieved.HitSet.Hits), ShouldEqual, 69120)

				uMatch := map[string]es.MapStringStringOrMap{"match_phrase": map[string]interface{}{"USER_NAME": "userA"}}
				query.Query.Bool.Filter = append(query.Query.Bool.Filter, uMatch)
				retrieved, err = db.Scroll(query)
				So(err, ShouldBeNil)
				So(retrieved.HitSet, ShouldNotBeNil)
				So(len(retrieved.HitSet.Hits), ShouldEqual, 61440)

				qMatch := map[string]es.MapStringStringOrMap{"match_phrase": map[string]interface{}{"QUEUE_NAME": "gpu-any"}}
				query.Query.Bool.Filter = append(query.Query.Bool.Filter, qMatch)
				retrieved, err = db.Scroll(query)
				So(err, ShouldBeNil)
				So(retrieved.HitSet, ShouldNotBeNil)
				So(len(retrieved.HitSet.Hits), ShouldEqual, 8777)

				query = &es.Query{
					Query: &es.QueryFilter{Bool: es.QFBool{Filter: es.Filter{
						{"match_phrase": map[string]interface{}{"META_CLUSTER_NAME": "farm"}},
						bomMatch,
						{"range": map[string]interface{}{
							"timestamp": map[string]string{
								"lte":    "2024-02-05T00:00:04Z",
								"gte":    "2024-02-05T00:00:00Z",
								"format": "strict_date_optional_time",
							},
						}},
					}}},
				}

				retrieved, err = db.Scroll(query)
				So(err, ShouldBeNil)
				So(retrieved.HitSet, ShouldNotBeNil)
				So(len(retrieved.HitSet.Hits), ShouldEqual, 2)
				sort.Slice(retrieved.HitSet.Hits, func(i, j int) bool {
					return retrieved.HitSet.Hits[i].Details.Timestamp < retrieved.HitSet.Hits[j].Details.Timestamp
				})

				stamp = time.Unix(retrieved.HitSet.Hits[0].Details.Timestamp, 0).UTC().Format(time.RFC3339)
				So(stamp, ShouldEqual, "2024-02-05T00:00:01Z")

				stamp = time.Unix(retrieved.HitSet.Hits[1].Details.Timestamp, 0).UTC().Format(time.RFC3339)
				So(stamp, ShouldEqual, "2024-02-05T00:00:03Z")
			})
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

		if hits%7 == 0 {
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
