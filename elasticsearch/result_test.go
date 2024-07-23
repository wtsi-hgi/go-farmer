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
	"strings"
	"testing"

	"github.com/deneonet/benc"
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

		bufPool := benc.NewBufPool(benc.WithBufferSize(MaxEncodedDetailsLength))

		detailBytes, err := details.Serialize(bufPool) //nolint:misspell
		So(err, ShouldBeNil)
		So(len(detailBytes), ShouldEqual, 127)

		recovered, err := DeserializeDetails(detailBytes, 0)
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

		detailBytes, err = details.Serialize(bufPool) //nolint:misspell
		So(err, ShouldBeNil)
		So(len(detailBytes), ShouldEqual, 124)

		recovered, err = DeserializeDetails(detailBytes, 0)
		So(err, ShouldBeNil)
		So(recovered, ShouldResemble, details)

		recovered, err = DeserializeDetails(detailBytes, FieldAccountingName)
		So(err, ShouldBeNil)
		So(recovered, ShouldResemble, &Details{ID: expectedID, AccountingName: "aname"})

		recovered, err = DeserializeDetails(detailBytes, FieldAvailCPUTimeSec)
		So(err, ShouldBeNil)
		So(recovered, ShouldResemble, &Details{ID: expectedID, AvailCPUTimeSec: 0})

		recovered, err = DeserializeDetails(detailBytes, FieldBOM)
		So(err, ShouldBeNil)
		So(recovered, ShouldResemble, &Details{ID: expectedID, BOM: "bname"})

		recovered, err = DeserializeDetails(detailBytes, FieldCommand)
		So(err, ShouldBeNil)
		So(recovered, ShouldResemble, &Details{ID: expectedID, Command: "cmd"})

		recovered, err = DeserializeDetails(detailBytes, FieldJobName)
		So(err, ShouldBeNil)
		So(recovered, ShouldResemble, &Details{ID: expectedID, JobName: ""})

		recovered, err = DeserializeDetails(detailBytes, FieldJob)
		So(err, ShouldBeNil)
		So(recovered, ShouldResemble, &Details{ID: expectedID, Job: "job"})

		recovered, err = DeserializeDetails(detailBytes, FieldMemRequestedMB)
		So(err, ShouldBeNil)
		So(recovered, ShouldResemble, &Details{ID: expectedID, MemRequestedMB: 1})

		recovered, err = DeserializeDetails(detailBytes, FieldMemRequestedMBSec)
		So(err, ShouldBeNil)
		So(recovered, ShouldResemble, &Details{ID: expectedID, MemRequestedMBSec: 2})

		recovered, err = DeserializeDetails(detailBytes, FieldNumExecProcs)
		So(err, ShouldBeNil)
		So(recovered, ShouldResemble, &Details{ID: expectedID, NumExecProcs: 3})

		recovered, err = DeserializeDetails(detailBytes, FieldPendingTimeSec)
		So(err, ShouldBeNil)
		So(recovered, ShouldResemble, &Details{ID: expectedID, PendingTimeSec: 4})

		recovered, err = DeserializeDetails(detailBytes, FieldQueueName)
		So(err, ShouldBeNil)
		So(recovered, ShouldResemble, &Details{ID: expectedID, QueueName: "qname"})

		recovered, err = DeserializeDetails(detailBytes, FieldRunTimeSec)
		So(err, ShouldBeNil)
		So(recovered, ShouldResemble, &Details{ID: expectedID, RunTimeSec: 5})

		recovered, err = DeserializeDetails(detailBytes, FieldTimestamp)
		So(err, ShouldBeNil)
		So(recovered, ShouldResemble, &Details{ID: expectedID, Timestamp: 6})

		recovered, err = DeserializeDetails(detailBytes, FieldUserName)
		So(err, ShouldBeNil)
		So(recovered, ShouldResemble, &Details{ID: expectedID, UserName: "uname"})

		recovered, err = DeserializeDetails(detailBytes, FieldWastedCPUSeconds)
		So(err, ShouldBeNil)
		So(recovered, ShouldResemble, &Details{ID: expectedID, WastedCPUSeconds: 7.1})

		recovered, err = DeserializeDetails(detailBytes, FieldWastedMBSeconds)
		So(err, ShouldBeNil)
		So(recovered, ShouldResemble, &Details{ID: expectedID, WastedMBSeconds: 7.2})

		recovered, err = DeserializeDetails(detailBytes, FieldWastedMBSeconds|FieldBOM)
		So(err, ShouldBeNil)
		So(recovered, ShouldResemble, &Details{ID: expectedID, BOM: "bname", WastedMBSeconds: 7.2})

		details = &Details{
			ID:                strings.Repeat("a", 26),
			AccountingName:    strings.Repeat("a", 24),
			AvailCPUTimeSec:   0,
			BOM:               strings.Repeat("b", 34),
			Command:           strings.Repeat("cmd:", 1000),
			JobName:           strings.Repeat("jname-", 2000),
			Job:               strings.Repeat("job;", 3000),
			MemRequestedMB:    1,
			MemRequestedMBSec: 2,
			NumExecProcs:      3,
			PendingTimeSec:    4,
			QueueName:         strings.Repeat("q", 22),
			RunTimeSec:        5,
			Timestamp:         6,
			UserName:          strings.Repeat("u", 12),
			WastedCPUSeconds:  7.1,
			WastedMBSeconds:   7.2,
		}

		detailBytes, err = details.Serialize(bufPool) //nolint:misspell
		So(err, ShouldBeNil)
		So(len(detailBytes), ShouldEqual, 7714)

		recovered, err = DeserializeDetails(detailBytes, 0)
		So(err, ShouldBeNil)
		So(recovered.ID, ShouldEqual, details.ID)
		So(recovered.AccountingName, ShouldEqual, details.AccountingName)
		So(recovered.BOM, ShouldEqual, details.BOM)
		So(len(recovered.Command), ShouldEqual, maxFieldLength)
		So(recovered.Command, ShouldStartWith, "cmd:cmd")
		So(recovered.Command, ShouldEndWith, "cmd:cmd:")
		So(recovered.Command, ShouldContainSubstring, truncationIndicator)
		So(len(recovered.JobName), ShouldEqual, maxFieldLength)
		So(recovered.JobName, ShouldStartWith, "jname-jname-")
		So(recovered.JobName, ShouldEndWith, "jname-jname-")
		So(recovered.JobName, ShouldContainSubstring, truncationIndicator)
		So(len(recovered.Job), ShouldEqual, maxFieldLength)
		So(recovered.Job, ShouldStartWith, "job;job;")
		So(recovered.Job, ShouldEndWith, "job;job;")
		So(recovered.Job, ShouldContainSubstring, truncationIndicator)
		So(recovered.QueueName, ShouldEqual, details.QueueName)
		So(recovered.UserName, ShouldEqual, details.UserName)
	})
}
