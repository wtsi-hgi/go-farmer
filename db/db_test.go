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
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
	es "github.com/wtsi-hgi/go-farmer/elasticsearch"
)

const (
	fileSize        = 512 * 1024
	bufferSize      = 256 * 1024
	updateFrequency = 50 * time.Millisecond
)

func TestDB(t *testing.T) {
	Convey("Given a directory, you can make a new database", t, func() {
		tmpDir := t.TempDir()
		dbDir := filepath.Join(tmpDir, "db")
		config := Config{
			Directory:  dbDir,
			FileSize:   fileSize,
			BufferSize: bufferSize,
		}

		db, err := New(config)
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

			bomA := "bomA"
			So(entries[0].Name(), ShouldEqual, bomA)
			So(entries[1].IsDir(), ShouldBeTrue)
			So(entries[1].Name(), ShouldEqual, "bomB")

			dir = filepath.Join(dir, bomA)
			entries, err = os.ReadDir(dir)
			So(err, ShouldBeNil)
			So(len(entries), ShouldEqual, 1)
			So(entries[0].Type().IsRegular(), ShouldBeTrue)
			So(entries[0].Name(), ShouldEqual, sqlfileBasename)

			dir = filepath.Join(dbDir, "2024", "02", "05", bomA)
			entries, err = os.ReadDir(dir)
			So(err, ShouldBeNil)
			So(len(entries), ShouldEqual, 1)

			Convey("Which you can then retrieve via Scroll() and Usernames()", func() {
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

				bomMatch := map[string]es.MapStringStringOrMap{"match_phrase": map[string]interface{}{"BOM": bomA}}
				query.Query.Bool.Filter = append(query.Query.Bool.Filter, bomMatch)
				retrieved, err := db.Scroll(query)
				So(err, ShouldBeNil)
				So(retrieved.HitSet.Hits, ShouldBeNil)

				db, err = New(config)
				So(err, ShouldBeNil)

				retrieved, err = db.Scroll(query)
				So(err, ShouldBeNil)
				So(retrieved.HitSet, ShouldNotBeNil)
				So(retrieved.ScrollID, ShouldEqual, pretendScrollID)

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

				usernames, err := db.Usernames(query)
				So(err, ShouldBeNil)

				sort.Strings(usernames)
				So(usernames, ShouldResemble, []string{"userA", "userB"})

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
					Source: []string{"USER_NAME", "timestamp"},
				}

				retrieved, err = db.Scroll(query)
				So(err, ShouldBeNil)
				So(retrieved.HitSet, ShouldNotBeNil)
				So(len(retrieved.HitSet.Hits), ShouldEqual, 2)
				sort.Slice(retrieved.HitSet.Hits, func(i, j int) bool {
					return retrieved.HitSet.Hits[i].Details.Timestamp < retrieved.HitSet.Hits[j].Details.Timestamp
				})

				stamp := time.Unix(retrieved.HitSet.Hits[0].Details.Timestamp, 0).UTC().Format(time.RFC3339)
				So(stamp, ShouldEqual, "2024-02-05T00:00:01Z")

				// stamp = time.Unix(retrieved.HitSet.Hits[1].Details.Timestamp, 0).UTC().Format(time.RFC3339)

				So(retrieved.HitSet.Hits[0].Details.UserName, ShouldNotBeBlank)
				So(retrieved.HitSet.Hits[0].Details.AccountingName, ShouldBeBlank)

				usernames, err = db.Usernames(query)
				So(err, ShouldBeNil)
				So(usernames, ShouldResemble, []string{"userA"})
			})

			Convey("A DB's knowledge of available flat files updates over time", func() {
				config.UpdateFrequency = updateFrequency
				db, err = New(config)
				So(err, ShouldBeNil)

				db.mu.RLock()
				So(len(db.dateBOMDirs), ShouldEqual, 4)
				db.mu.RUnlock()

				febDir := filepath.Join(dbDir, "2024", "02")
				olderFile := filepath.Join(febDir, "03", bomA, sqlfileBasename)
				newerFile := filepath.Join(febDir, "08", bomA, sqlfileBasename)
				today := time.Now().Format(dateFormat)
				newestFile := filepath.Join(dbDir, today, bomA, sqlfileBasename)

				err = makeFiles(olderFile, newerFile, newestFile)
				So(err, ShouldBeNil)

				<-time.After(config.UpdateFrequencyOrDefault() * 2)
				db.mu.RLock()
				defer db.mu.RUnlock()

				So(len(db.dateBOMDirs), ShouldEqual, 6)

				_, ok := db.dateBOMDirs[filepath.Dir(olderFile)]
				So(ok, ShouldBeFalse)

				_, ok = db.dateBOMDirs[filepath.Dir(newerFile)]
				So(ok, ShouldBeTrue)

				_, ok = db.dateBOMDirs[filepath.Dir(newestFile)]
				So(ok, ShouldBeTrue)
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
			Command:           "cmd' DELETE * FROM * Where * == *",
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

func makeFiles(paths ...string) error {
	for _, path := range paths {
		err := os.MkdirAll(filepath.Dir(path), dbDirPerms)
		if err != nil {
			return err
		}

		f, err := os.Create(path)
		if err != nil {
			return err
		}

		err = f.Close()
		if err != nil {
			return err
		}
	}

	return nil
}
