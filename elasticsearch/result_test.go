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

package elasticsearch

import (
	"strconv"
	"sync"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestDetails(t *testing.T) {
	Convey("You can serialize and deserialze Details ", t, func() { //nolint:misspell
		details := &Details{
			AccountingName:    "aname",
			AvailCPUTimeSec:   0,
			BOM:               "bname",
			Command:           "cmd",
			JobName:           "jname",
			Job:               "job",
			MemRequestedMB:    1,
			MemRequestedMBSec: 2,
			NumExecProcs:      3,
			PendingTimeSec:    4,
			QueueName:         "qname",
			RunTimeSec:        5,
			Timestamp:         6,
			UserName:          "uname",
			WastedCPUSeconds:  7.1,
			WastedMBSeconds:   7.2,
		}

		detailBytes, err := details.Serialize() //nolint:misspell
		So(err, ShouldBeNil)
		So(len(detailBytes), ShouldEqual, 119)

		recovered, err := DeserializeDetails(detailBytes, []string{})
		So(err, ShouldBeNil)
		So(recovered, ShouldResemble, details)

		expectedID := "id"
		details = &Details{
			ID:                expectedID,
			AccountingName:    "aname",
			AvailCPUTimeSec:   0,
			BOM:               "bname",
			Command:           "cmd",
			JobName:           "",
			Job:               "job",
			MemRequestedMB:    1,
			MemRequestedMBSec: 2,
			NumExecProcs:      3,
			PendingTimeSec:    4,
			QueueName:         "qname",
			RunTimeSec:        5,
			Timestamp:         6,
			UserName:          "uname",
			WastedCPUSeconds:  7.1,
			WastedMBSeconds:   7.2,
		}

		detailBytes, err = details.Serialize() //nolint:misspell
		So(err, ShouldBeNil)
		So(len(detailBytes), ShouldEqual, 116)

		recovered, err = DeserializeDetails(detailBytes, []string{})
		So(err, ShouldBeNil)
		So(recovered, ShouldResemble, details)

		recovered, err = DeserializeDetails(detailBytes, []string{"ACCOUNTING_NAME"})
		So(err, ShouldBeNil)
		So(recovered, ShouldResemble, &Details{ID: expectedID, AccountingName: "aname"})

		recovered, err = DeserializeDetails(detailBytes, []string{"AVAIL_CPU_TIME_SEC"})
		So(err, ShouldBeNil)
		So(recovered, ShouldResemble, &Details{ID: expectedID, AvailCPUTimeSec: 0})

		recovered, err = DeserializeDetails(detailBytes, []string{"BOM"})
		So(err, ShouldBeNil)
		So(recovered, ShouldResemble, &Details{ID: expectedID, BOM: "bname"})

		recovered, err = DeserializeDetails(detailBytes, []string{"Command"})
		So(err, ShouldBeNil)
		So(recovered, ShouldResemble, &Details{ID: expectedID, Command: "cmd"})

		recovered, err = DeserializeDetails(detailBytes, []string{"JOB_NAME"})
		So(err, ShouldBeNil)
		So(recovered, ShouldResemble, &Details{ID: expectedID, JobName: ""})

		recovered, err = DeserializeDetails(detailBytes, []string{"Job"})
		So(err, ShouldBeNil)
		So(recovered, ShouldResemble, &Details{ID: expectedID, Job: "job"})

		recovered, err = DeserializeDetails(detailBytes, []string{"MEM_REQUESTED_MB"})
		So(err, ShouldBeNil)
		So(recovered, ShouldResemble, &Details{ID: expectedID, MemRequestedMB: 1})

		recovered, err = DeserializeDetails(detailBytes, []string{"MEM_REQUESTED_MB_SEC"})
		So(err, ShouldBeNil)
		So(recovered, ShouldResemble, &Details{ID: expectedID, MemRequestedMBSec: 2})

		recovered, err = DeserializeDetails(detailBytes, []string{"NUM_EXEC_PROCS"})
		So(err, ShouldBeNil)
		So(recovered, ShouldResemble, &Details{ID: expectedID, NumExecProcs: 3})

		recovered, err = DeserializeDetails(detailBytes, []string{"PENDING_TIME_SEC"})
		So(err, ShouldBeNil)
		So(recovered, ShouldResemble, &Details{ID: expectedID, PendingTimeSec: 4})

		recovered, err = DeserializeDetails(detailBytes, []string{"QUEUE_NAME"})
		So(err, ShouldBeNil)
		So(recovered, ShouldResemble, &Details{ID: expectedID, QueueName: "qname"})

		recovered, err = DeserializeDetails(detailBytes, []string{"RUN_TIME_SEC"})
		So(err, ShouldBeNil)
		So(recovered, ShouldResemble, &Details{ID: expectedID, RunTimeSec: 5})

		recovered, err = DeserializeDetails(detailBytes, []string{"timestamp"})
		So(err, ShouldBeNil)
		So(recovered, ShouldResemble, &Details{ID: expectedID, Timestamp: 6})

		recovered, err = DeserializeDetails(detailBytes, []string{"USER_NAME"})
		So(err, ShouldBeNil)
		So(recovered, ShouldResemble, &Details{ID: expectedID, UserName: "uname"})

		recovered, err = DeserializeDetails(detailBytes, []string{"WASTED_CPU_SECONDS"})
		So(err, ShouldBeNil)
		So(recovered, ShouldResemble, &Details{ID: expectedID, WastedCPUSeconds: 7.1})

		recovered, err = DeserializeDetails(detailBytes, []string{"WASTED_MB_SECONDS"})
		So(err, ShouldBeNil)
		So(recovered, ShouldResemble, &Details{ID: expectedID, WastedMBSeconds: 7.2})

		recovered, err = DeserializeDetails(detailBytes, []string{"WASTED_MB_SECONDS", "BOM"})
		So(err, ShouldBeNil)
		So(recovered, ShouldResemble, &Details{ID: expectedID, BOM: "bname", WastedMBSeconds: 7.2})
	})
}

func TestHitSet(t *testing.T) {
	Convey("You can add Hits to a HitSet in parallel", t, func() {
		hitSet := &HitSet{}

		numHits := 100

		runOpInParallel(func(id string) {
			hitSet.AddHit(id, &Details{Command: "cmd." + id})
		}, numHits)

		So(len(hitSet.Hits), ShouldEqual, numHits)
		So(hitSet.Total.Value, ShouldEqual, numHits)
	})

	Convey("You can add Hits to a Result in parallel", t, func() {
		result := NewResult()

		numHits := 100

		runOpInParallel(func(id string) {
			result.AddHitDetails(&Details{Command: "cmd." + id})
		}, numHits)

		So(len(result.HitSet.Hits), ShouldEqual, numHits)
		So(result.HitSet.Total.Value, ShouldEqual, numHits)
	})
}

func runOpInParallel(op func(string), count int) {
	var wg sync.WaitGroup //nolint:varnamelen

	wg.Add(count)

	for i := range count {
		id := strconv.Itoa(i)

		go func(id string) {
			defer wg.Done()

			op(id)
		}(id)
	}

	wg.Wait()
}

func TestResultErrors(t *testing.T) {
	Convey("You can add errors to a Result in parallel", t, func() {
		result := NewResult()

		numErrors := 100

		runOpInParallel(func(id string) {
			result.AddError(Error{Msg: id})
		}, numErrors)

		So(len(result.Errors()), ShouldEqual, numErrors)
	})
}
