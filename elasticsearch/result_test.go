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
		So(len(detailBytes), ShouldEqual, 117)

		recovered, err := DeserializeDetails(detailBytes)
		So(err, ShouldBeNil)
		So(recovered, ShouldResemble, details)

		details = &Details{
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
		So(len(detailBytes), ShouldEqual, 112)

		recovered, err = DeserializeDetails(detailBytes)
		So(err, ShouldBeNil)
		So(recovered, ShouldResemble, details)
	})
}

func TestHitSet(t *testing.T) {
	Convey("You can add Hits to a HitSet in parallel", t, func() {
		hitSet := &HitSet{}

		var wg sync.WaitGroup //nolint:varnamelen

		numHits := 100

		wg.Add(numHits)

		for i := range numHits {
			id := strconv.Itoa(i)

			go func(id string) {
				defer wg.Done()
				hitSet.AddHit(id, &Details{Command: "cmd." + id})
			}(id)
		}

		wg.Wait()

		So(len(hitSet.Hits), ShouldEqual, numHits)
	})
}
