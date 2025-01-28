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
	"sync"
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

		db, err := New(config, false)
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

			hitCh := make(chan *es.Hit)
			errCh := make(chan error)

			go func() {
				errs := db.Store(hitCh)
				errCh <- errs
			}()

			for _, hit := range result.HitSet.Hits {
				hitCh <- &hit
			}

			close(hitCh)

			err = <-errCh
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
			So(len(entries), ShouldEqual, 3)
			So(entries[0].IsDir(), ShouldBeTrue)

			bomA := "bomA"
			So(entries[0].Name(), ShouldEqual, bomA)
			So(entries[1].IsDir(), ShouldBeTrue)
			So(entries[1].Name(), ShouldEqual, "bomB")
			So(entries[2].IsDir(), ShouldBeTrue)
			So(entries[2].Name(), ShouldEqual, "bomC-IDS")

			dir = filepath.Join(dir, bomA)
			entries, err = os.ReadDir(dir)
			So(err, ShouldBeNil)
			So(len(entries), ShouldEqual, 24)
			So(entries[0].Type().IsRegular(), ShouldBeTrue)
			So(entries[0].Name(), ShouldEqual, "0.data")
			So(entries[1].Type().IsRegular(), ShouldBeTrue)
			So(entries[1].Name(), ShouldEqual, "0.index")
			So(entries[23].Type().IsRegular(), ShouldBeTrue)
			So(entries[23].Name(), ShouldEqual, "9.index")
			So(entries[7].Type().IsRegular(), ShouldBeTrue)
			So(entries[7].Name(), ShouldEqual, "11.index")

			indexFilePath := filepath.Join(dir, "0.index")
			bIndex, err := os.ReadFile(indexFilePath)
			So(err, ShouldBeNil)

			dataFilePath := filepath.Join(dir, "0.data")
			bData, err := os.ReadFile(dataFilePath)
			So(err, ShouldBeNil)

			So(len(bData), ShouldBeGreaterThanOrEqualTo, fileSize)
			So(len(bData), ShouldBeLessThan, fileSize*2)

			So(bIndex[0:timeStampWidth], ShouldResemble, []byte{0, 0, 0, 0, 101, 190, 211, 129})

			stamp := timeStampBytesToFormatString(bIndex[0:timeStampWidth])
			So(stamp, ShouldEqual, "2024-02-04T00:00:01Z")

			nextFieldStart := timeStampWidth
			So(string(bIndex[nextFieldStart:nextFieldStart+accountingNameWidth]), ShouldEqual, "groupA                  ")

			nextFieldStart += accountingNameWidth
			So(string(bIndex[nextFieldStart:nextFieldStart+userNameWidth]), ShouldEqual, "userA          ")

			nextFieldStart += userNameWidth
			So(bIndex[nextFieldStart:nextFieldStart+1], ShouldEqual, []byte{notInGPUQueue})

			nextFieldStart++
			dataPos := int(binary.BigEndian.Uint32(bIndex[nextFieldStart : nextFieldStart+lengthEncodeWidth]))
			expectedDataPos := 0
			So(dataPos, ShouldEqual, expectedDataPos)

			nextFieldStart += lengthEncodeWidth
			detailsLen := int(binary.BigEndian.Uint32(bIndex[nextFieldStart : nextFieldStart+lengthEncodeWidth]))
			expectedDetailsLen := 143
			So(detailsLen, ShouldEqual, expectedDetailsLen)

			detailsBytes := bData[dataPos:detailsLen]
			details, err := es.DeserializeDetails(detailsBytes, 0)
			So(err, ShouldBeNil)

			timeStamp, err := time.Parse(time.RFC3339, "2024-02-04T00:00:01Z")
			So(err, ShouldBeNil)

			So(details, ShouldResemble, result.HitSet.Hits[1].Details)
			So(details.Timestamp, ShouldEqual, timeStamp.Unix())
			So(details.ID, ShouldEqual, result.HitSet.Hits[1].ID)

			nextFieldStart += lengthEncodeWidth
			stamp = timeStampBytesToFormatString(bIndex[nextFieldStart : nextFieldStart+timeStampWidth])
			So(stamp, ShouldEqual, "2024-02-04T00:00:03Z")

			dir = filepath.Join(dbDir, "2024", "02", "05", bomA)
			entries, err = os.ReadDir(dir)
			So(err, ShouldBeNil)
			So(len(entries), ShouldEqual, 24)

			indexFilePath = filepath.Join(dir, "11.index")
			bIndex, err = os.ReadFile(indexFilePath)
			So(err, ShouldBeNil)

			dataFilePath = filepath.Join(dir, "11.data")
			bData, err = os.ReadFile(dataFilePath)
			So(err, ShouldBeNil)

			nextFieldStart = len(bIndex) - (2 * lengthEncodeWidth)
			dataPos = int(binary.BigEndian.Uint32(bIndex[nextFieldStart : nextFieldStart+lengthEncodeWidth]))
			So(dataPos, ShouldBeGreaterThan, 0)

			nextFieldStart += lengthEncodeWidth
			detailsLen = int(binary.BigEndian.Uint32(bIndex[nextFieldStart : nextFieldStart+lengthEncodeWidth]))
			So(detailsLen, ShouldEqual, expectedDetailsLen)

			detailsBytes = bData[dataPos:]
			So(len(detailsBytes), ShouldEqual, detailsLen)
			details, err = es.DeserializeDetails(detailsBytes, 0)
			So(err, ShouldBeNil)
			So(details, ShouldResemble, result.HitSet.Hits[expectedNumHits-1].Details)

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

				Convey("if you specify a BoM", func() {
					dbStore := db
					db, err = New(config, false)
					So(err, ShouldBeNil)

					_, err = db.Scroll(query)
					So(err, ShouldNotBeNil)

					released := db.Done(0)
					So(released, ShouldBeFalse)

					bomMatch := map[string]es.MapStringStringOrMap{"match_phrase": map[string]interface{}{"BOM": bomA}}
					query.Query.Bool.Filter = append(query.Query.Bool.Filter, bomMatch)

					Convey("unless you use the same db instance that did the store", func() {
						retrieved, errs := dbStore.Scroll(query)
						So(errs, ShouldBeNil)
						So(len(retrieved.HitSet.Hits), ShouldEqual, 0)
					})

					retrieved, errs := db.Scroll(query)
					So(errs, ShouldBeNil)
					So(retrieved.HitSet, ShouldNotBeNil)
					So(retrieved.ScrollID, ShouldEqual, pretendScrollID)

					expectedBomHits := expectedNumHits/2 - 1
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

					released = db.Done(retrieved.PoolKey)
					So(released, ShouldBeTrue)

					usernames, erru := db.Usernames(query)
					So(erru, ShouldBeNil)

					sort.Strings(usernames)
					So(usernames, ShouldResemble, []string{"userA", "userB", "userNameLongest"})

					Convey("you can filter on things not in the index", func() {
						jMatch := map[string]es.MapStringStringOrMap{"prefix": map[string]interface{}{"JOB_NAME": "nf"}}
						query.Query.Bool.Filter = append(query.Query.Bool.Filter, jMatch)
						retrieved, err = db.Scroll(query)
						So(err, ShouldBeNil)
						So(retrieved.HitSet, ShouldNotBeNil)
						So(len(retrieved.HitSet.Hits), ShouldEqual, 4114)
					})

					Convey("you can filter on things in the index", func() {
						aMatch := map[string]es.MapStringStringOrMap{"match_phrase": map[string]interface{}{"ACCOUNTING_NAME": "groupA"}}
						query.Query.Bool.Filter = append(query.Query.Bool.Filter, aMatch)
						retrieved, err = db.Scroll(query)
						So(err, ShouldBeNil)
						So(retrieved.HitSet, ShouldNotBeNil)
						So(len(retrieved.HitSet.Hits), ShouldEqual, 69119)

						uMatch := map[string]es.MapStringStringOrMap{"match_phrase": map[string]interface{}{"USER_NAME": "userA"}}
						query.Query.Bool.Filter = append(query.Query.Bool.Filter, uMatch)
						retrieved, err = db.Scroll(query)
						So(err, ShouldBeNil)
						So(retrieved.HitSet, ShouldNotBeNil)
						So(len(retrieved.HitSet.Hits), ShouldEqual, 61439)

						qMatch := map[string]es.MapStringStringOrMap{"match_phrase": map[string]interface{}{"QUEUE_NAME": "gpu-any"}}
						query.Query.Bool.Filter = append(query.Query.Bool.Filter, qMatch)
						retrieved, err = db.Scroll(query)
						So(err, ShouldBeNil)
						So(retrieved.HitSet, ShouldNotBeNil)
						So(len(retrieved.HitSet.Hits), ShouldEqual, 8776)

						released = db.Done(retrieved.PoolKey)
						So(released, ShouldBeTrue)

						query = &es.Query{
							Query: &es.QueryFilter{Bool: es.QFBool{Filter: es.Filter{
								{"match_phrase": map[string]interface{}{"META_CLUSTER_NAME": "farm"}},
								bomMatch,
								uMatch,
								{"range": map[string]interface{}{
									"timestamp": map[string]string{
										"lte":    lteStr,
										"gte":    gteStr,
										"format": "strict_date_optional_time",
									},
								}},
							}}},
						}

						retrieved, err = db.Scroll(query)
						So(err, ShouldBeNil)
						So(retrieved.HitSet, ShouldNotBeNil)
						So(len(retrieved.HitSet.Hits), ShouldEqual, 76798)

						released = db.Done(retrieved.PoolKey)
						So(released, ShouldBeTrue)

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

						stamp = time.Unix(retrieved.HitSet.Hits[0].Details.Timestamp, 0).UTC().Format(time.RFC3339)
						So(stamp, ShouldEqual, "2024-02-05T00:00:01Z")

						stamp = time.Unix(retrieved.HitSet.Hits[1].Details.Timestamp, 0).UTC().Format(time.RFC3339)

						So(retrieved.HitSet.Hits[0].Details.UserName, ShouldNotBeBlank)
						So(retrieved.HitSet.Hits[0].Details.AccountingName, ShouldBeBlank)

						released = db.Done(retrieved.PoolKey)
						So(released, ShouldBeTrue)

						usernames, err = db.Usernames(query)
						So(err, ShouldBeNil)
						So(usernames, ShouldResemble, []string{"userA"})

						Convey("Which works concurrently", func() {
							numRoutines := 100
							rCh := make(chan *es.Result)
							eCh := make(chan error)

							var wg sync.WaitGroup

							wg.Add(numRoutines)

							for range numRoutines {
								go func() {
									defer wg.Done()

									r, e := db.Scroll(query)
									rCh <- r
									eCh <- e

									db.Done(r.PoolKey)
								}()
							}

							errors := 0

							go func() {
								for e := range eCh {
									if e != nil {
										errors++
									}
								}
							}()

							oks := 0
							okDoneCh := make(chan bool)

							go func() {
								for r := range rCh {
									if r != nil && r.HitSet.Total.Value == 2 {
										oks++
									}
								}

								close(okDoneCh)
							}()

							wg.Wait()
							close(eCh)
							close(rCh)
							<-okDoneCh

							So(errors, ShouldEqual, 0)
							So(oks, ShouldEqual, numRoutines)
						})
					})
				})

				Convey("if you specify a BoM with special characters in it", func() {
					db, err = New(config, false)
					So(err, ShouldBeNil)

					bomMatch := map[string]es.MapStringStringOrMap{"match_phrase": map[string]interface{}{"BOM": "bomC–IDS"}}
					query.Query.Bool.Filter = append(query.Query.Bool.Filter, bomMatch)

					retrieved, errs := db.Scroll(query)
					So(errs, ShouldBeNil)
					So(retrieved.HitSet, ShouldNotBeNil)
					So(retrieved.ScrollID, ShouldEqual, pretendScrollID)
					So(len(retrieved.HitSet.Hits), ShouldEqual, 1)
					So(retrieved.HitSet.Hits[0].Details.BOM, ShouldEqual, "bomC–IDS")
				})

				Convey("if you specify a username that is 15 characters long", func() {
					db, err = New(config, false)
					So(err, ShouldBeNil)

					longName := "userNameLongest"

					userMatch := []map[string]es.MapStringStringOrMap{
						{"match_phrase": map[string]interface{}{"BOM": bomA}},
						{"match_phrase": map[string]interface{}{"USER_NAME": longName}},
					}
					query.Query.Bool.Filter = append(query.Query.Bool.Filter, userMatch...)

					retrieved, errs := db.Scroll(query)
					So(errs, ShouldBeNil)
					So(retrieved.HitSet, ShouldNotBeNil)
					So(retrieved.ScrollID, ShouldEqual, pretendScrollID)
					So(len(retrieved.HitSet.Hits), ShouldEqual, 1)
					So(retrieved.HitSet.Hits[0].Details.UserName, ShouldEqual, longName)
				})
			})

			Convey("A DB's knowledge of available flat files updates over time", func() {
				config.UpdateFrequency = updateFrequency
				db, err = New(config, false)
				So(err, ShouldBeNil)

				db.muDateBOMDirs.RLock()
				So(len(db.dateBOMDirs), ShouldEqual, 5)
				db.muDateBOMDirs.RUnlock()

				febDir := filepath.Join(dbDir, "2024", "02")
				olderFile := filepath.Join(febDir, "03", bomA, "0.index")
				newerFile := filepath.Join(febDir, "08", bomA, "0.index")
				today := time.Now().Format(dateFormat)
				newestFile := filepath.Join(dbDir, today, bomA, "0.index")

				err = makeFiles(olderFile, newerFile, newestFile)
				So(err, ShouldBeNil)

				<-time.After(config.UpdateFrequencyOrDefault() * 2)
				db.muDateBOMDirs.RLock()
				defer db.muDateBOMDirs.RUnlock()

				So(len(db.dateBOMDirs), ShouldEqual, 7)

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

		if hits%21 == 0 {
			jName = "nf-foo"
		}

		if hits == 5 {
			uName = "userNameLongest"
		}

		if hits == 7 {
			bom = "bomC–IDS"
		}

		hit := es.Hit{
			Details: &es.Details{
				Timestamp:           timeStamp.Unix(),
				BOM:                 bom,
				AccountingName:      aName,
				UserName:            uName,
				QueueName:           qName,
				Command:             "cmd",
				JobName:             jName,
				Job:                 "job",
				MemRequestedMB:      1,
				MemRequestedMBSec:   2,
				NumExecProcs:        3,
				PendingTimeSec:      4,
				RunTimeSec:          5,
				WastedCPUSeconds:    6.1,
				WastedMBSeconds:     7.1,
				RawWastedCPUSeconds: 8.1,
				RawWastedMBSeconds:  9.1,
			},
		}

		result.HitSet.Hits = append(result.HitSet.Hits, hit)
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
