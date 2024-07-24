/*******************************************************************************
 * Copyright (c) 2024 Genome Research Ltd.
 *
 * Author: Sendu Bala <sb10@sanger.ac.uk>
 * Author: Iaroslav Popov <ip13@sanger.ac.uk>
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
	"fmt"
	"log/slog"

	"github.com/deneonet/benc"
	"github.com/deneonet/benc/bstd"
	"github.com/elastic/go-elasticsearch/v7/esapi"
	easyjson "github.com/mailru/easyjson"
)

const (
	maxFieldLength      = 2500
	truncationIndicator = " [....] "
	headTailLen         = (maxFieldLength / 2) - (len(truncationIndicator) / 2) //nolint:mnd

	MaxEncodedDetailsLength = 16 * 1024
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

const ErrFailedQuery = "elasticsearch query failed"

// Result holds the results of a search query.
type Result struct {
	ScrollID     string        `json:"_scroll_id,omitempty"`
	Took         int           `json:"took"`
	TimedOut     bool          `json:"timed_out"`
	HitSet       *HitSet       `json:"hits"`
	Aggregations *Aggregations `json:"aggregations,omitempty"`
	PoolKey      int           `json:"-"`
}

// NewResult returns a Result with an empty HitSet in it, suitable for adding
// hits and errors to.
func NewResult() *Result {
	return &Result{
		HitSet: &HitSet{},
	}
}

// HitSet is the container of all Hits, plus a Total.Value which may tell you
// the total number of matching documents.
type HitSet struct {
	Total HitSetTotal `json:"total"`
	Hits  []Hit       `json:"hits"`
}

type HitSetTotal struct {
	Value int `json:"value"`
}

type Hit struct {
	ID      string   `json:"_id,omitempty"`
	Details *Details `json:"_source"`
}

// Details holds the document information of a Hit.
type Details struct {
	ID                string  `json:"_id"`
	AccountingName    string  `json:"ACCOUNTING_NAME"`
	AvailCPUTimeSec   int64   `json:"AVAIL_CPU_TIME_SEC"`
	BOM               string  `json:"BOM"`
	Command           string  `json:"Command"`
	JobName           string  `json:"JOB_NAME"`
	Job               string  `json:"Job"`
	MemRequestedMB    int64   `json:"MEM_REQUESTED_MB"`
	MemRequestedMBSec int64   `json:"MEM_REQUESTED_MB_SEC"`
	NumExecProcs      int64   `json:"NUM_EXEC_PROCS"`
	PendingTimeSec    int64   `json:"PENDING_TIME_SEC"`
	QueueName         string  `json:"QUEUE_NAME"`
	RunTimeSec        int64   `json:"RUN_TIME_SEC"`
	Timestamp         int64   `json:"timestamp"`
	UserName          string  `json:"USER_NAME"`
	WastedCPUSeconds  float64 `json:"WASTED_CPU_SECONDS"`
	WastedMBSeconds   float64 `json:"WASTED_MB_SECONDS"`
	// AVG_MEM_EFFICIENCY_PERCENT     float64
	// AVRG_MEM_USAGE_MB              float64
	// AVRG_MEM_USAGE_MB_SEC_COOKED   float64
	// AVRG_MEM_USAGE_MB_SEC_RAW      float64
	// CLUSTER_NAME                   string
	// COOKED_CPU_TIME_SEC            float64
	// END_TIME                       int
	// EXEC_HOSTNAME                  []string
	// Exit_Info                      int
	// Exitreason                     string
	// JOB_ID          int
	// JOB_ARRAY_INDEX int
	// JOB_EXIT_STATUS                int
	// Job_Efficiency_Percent         float64
	// Job_Efficiency_Raw_Percent     float64
	// MAX_MEM_EFFICIENCY_PERCENT     float64
	// MAX_MEM_USAGE_MB               float64
	// MAX_MEM_USAGE_MB_SEC_COOKED    float64
	// MAX_MEM_USAGE_MB_SEC_RAW       float64
	// NumberOfHosts                  int
	// NumberOfUniqueHosts            int
	// PROJECT_NAME                   string
	// RAW_AVG_MEM_EFFICIENCY_PERCENT float64
	// RAW_CPU_TIME_SEC               float64
	// RAW_MAX_MEM_EFFICIENCY_PERCENT float64
	// RAW_WASTED_CPU_SECONDS         float64
	// RAW_WASTED_MB_SECONDS          float64
	// SUBMIT_TIME  int
}

// Serialize converts a Details to a byte slice representation suitable for
// storing on disk.
func (d *Details) Serialize() ([]byte, error) { //nolint:funlen,misspell
	d.headTailStrings()

	var (
		size int
		err  error
	)

	addSize(&size, &err, func() (int, error) { return bstd.SizeString(d.ID) })
	addSize(&size, &err, func() (int, error) { return bstd.SizeString(d.AccountingName) })
	addSize(&size, &err, func() (int, error) { return bstd.SizeInt64(), nil })
	addSize(&size, &err, func() (int, error) { return bstd.SizeString(d.BOM) })
	addSize(&size, &err, func() (int, error) { return bstd.SizeString(d.Command) })
	addSize(&size, &err, func() (int, error) { return bstd.SizeString(d.JobName) })
	addSize(&size, &err, func() (int, error) { return bstd.SizeString(d.Job) })
	addSize(&size, &err, func() (int, error) { return bstd.SizeInt64(), nil })
	addSize(&size, &err, func() (int, error) { return bstd.SizeInt64(), nil })
	addSize(&size, &err, func() (int, error) { return bstd.SizeInt64(), nil })
	addSize(&size, &err, func() (int, error) { return bstd.SizeInt64(), nil })
	addSize(&size, &err, func() (int, error) { return bstd.SizeString(d.QueueName) })
	addSize(&size, &err, func() (int, error) { return bstd.SizeInt64(), nil })
	addSize(&size, &err, func() (int, error) { return bstd.SizeInt64(), nil })
	addSize(&size, &err, func() (int, error) { return bstd.SizeString(d.UserName) })
	addSize(&size, &err, func() (int, error) { return bstd.SizeFloat64(), nil })
	addSize(&size, &err, func() (int, error) { return bstd.SizeFloat64(), nil })

	if err != nil {
		return nil, err
	}

	return d.marshal(size)
}

func addSize(size *int, err *error, fn func() (int, error)) {
	thisSize, thisErr := fn()
	if thisErr != nil {
		*err = thisErr
	}

	*size += thisSize
}

func (d *Details) marshal(size int) ([]byte, error) { //nolint:funlen,gocyclo
	n, encoded := benc.Marshal(size)

	n, err := bstd.MarshalString(n, encoded, d.ID)
	if err != nil {
		return nil, err
	}

	n, err = bstd.MarshalString(n, encoded, d.AccountingName)
	if err != nil {
		return nil, err
	}

	n = bstd.MarshalInt64(n, encoded, d.AvailCPUTimeSec)

	n, err = bstd.MarshalString(n, encoded, d.BOM)
	if err != nil {
		return nil, err
	}

	n, err = bstd.MarshalString(n, encoded, d.Command)
	if err != nil {
		return nil, err
	}

	n, err = bstd.MarshalString(n, encoded, d.JobName)
	if err != nil {
		return nil, err
	}

	n, err = bstd.MarshalString(n, encoded, d.Job)
	if err != nil {
		return nil, err
	}

	n = bstd.MarshalInt64(n, encoded, d.MemRequestedMB)
	n = bstd.MarshalInt64(n, encoded, d.MemRequestedMBSec)
	n = bstd.MarshalInt64(n, encoded, d.NumExecProcs)
	n = bstd.MarshalInt64(n, encoded, d.PendingTimeSec)

	n, err = bstd.MarshalString(n, encoded, d.QueueName)
	if err != nil {
		return nil, err
	}

	n = bstd.MarshalInt64(n, encoded, d.RunTimeSec)
	n = bstd.MarshalInt64(n, encoded, d.Timestamp)

	n, err = bstd.MarshalString(n, encoded, d.UserName)
	if err != nil {
		return nil, err
	}

	n = bstd.MarshalFloat64(n, encoded, d.WastedCPUSeconds)
	n = bstd.MarshalFloat64(n, encoded, d.WastedMBSeconds)

	err = benc.VerifyMarshal(n, encoded)

	return encoded, err
}

// headTailStrings reduces the length of our string values to a maximum length
// by keeping only the start and end of them if too long. This means we can know
// the max length of a Details and have an appropriate sized buffer and no
// problems deserializing.
func (d *Details) headTailStrings() {
	if len(d.Command) > maxFieldLength {
		d.Command = headTailString(d.Command)
	}

	if len(d.JobName) > maxFieldLength {
		d.JobName = headTailString(d.JobName)
	}

	if len(d.Job) > maxFieldLength {
		d.Job = headTailString(d.Job)
	}
}

func headTailString(s string) string {
	return s[0:headTailLen] + truncationIndicator + s[len(s)-headTailLen:]
}

// DeserializeDetails takes the output of Details.Serialize and converts it
// back in to a Details. Provide a non-zero Fields (from Query.DesiredFields())
// to skip the unmarshalling of undesired fields, for a speed boost.
func DeserializeDetails(encoded []byte, desired Fields) (*Details, error) { //nolint:funlen,gocognit,gocyclo,cyclop
	details := &Details{}

	var (
		n   int //nolint:varnamelen
		err error
	)

	n, details.ID, err = bstd.UnmarshalUnsafeString(0, encoded)
	if err != nil {
		return nil, err
	}

	if WantsField(desired, FieldAccountingName) {
		n, details.AccountingName, err = bstd.UnmarshalUnsafeString(n, encoded)
	} else {
		n, err = bstd.SkipString(n, encoded)
	}

	if err != nil {
		return nil, err
	}

	if WantsField(desired, FieldAvailCPUTimeSec) {
		n, details.AvailCPUTimeSec, err = bstd.UnmarshalInt64(n, encoded)
	} else {
		n, err = bstd.SkipInt64(n, encoded)
	}

	if err != nil {
		return nil, err
	}

	if WantsField(desired, FieldBOM) {
		n, details.BOM, err = bstd.UnmarshalUnsafeString(n, encoded)
	} else {
		n, err = bstd.SkipString(n, encoded)
	}

	if err != nil {
		return nil, err
	}

	if WantsField(desired, FieldCommand) {
		n, details.Command, err = bstd.UnmarshalUnsafeString(n, encoded)
	} else {
		n, err = bstd.SkipString(n, encoded)
	}

	if err != nil {
		return nil, err
	}

	if WantsField(desired, FieldJobName) {
		n, details.JobName, err = bstd.UnmarshalUnsafeString(n, encoded)
	} else {
		n, err = bstd.SkipString(n, encoded)
	}

	if err != nil {
		return nil, err
	}

	if WantsField(desired, FieldJob) {
		n, details.Job, err = bstd.UnmarshalUnsafeString(n, encoded)
	} else {
		n, err = bstd.SkipString(n, encoded)
	}

	if err != nil {
		return nil, err
	}

	if WantsField(desired, FieldMemRequestedMB) {
		n, details.MemRequestedMB, err = bstd.UnmarshalInt64(n, encoded)
	} else {
		n, err = bstd.SkipInt64(n, encoded)
	}

	if err != nil {
		return nil, err
	}

	if WantsField(desired, FieldMemRequestedMBSec) {
		n, details.MemRequestedMBSec, err = bstd.UnmarshalInt64(n, encoded)
	} else {
		n, err = bstd.SkipInt64(n, encoded)
	}

	if err != nil {
		return nil, err
	}

	if WantsField(desired, FieldNumExecProcs) {
		n, details.NumExecProcs, err = bstd.UnmarshalInt64(n, encoded)
	} else {
		n, err = bstd.SkipInt64(n, encoded)
	}

	if err != nil {
		return nil, err
	}

	if WantsField(desired, FieldPendingTimeSec) {
		n, details.PendingTimeSec, err = bstd.UnmarshalInt64(n, encoded)
	} else {
		n, err = bstd.SkipInt64(n, encoded)
	}

	if err != nil {
		return nil, err
	}

	if WantsField(desired, FieldQueueName) {
		n, details.QueueName, err = bstd.UnmarshalUnsafeString(n, encoded)
	} else {
		n, err = bstd.SkipString(n, encoded)
	}

	if err != nil {
		return nil, err
	}

	if WantsField(desired, FieldRunTimeSec) {
		n, details.RunTimeSec, err = bstd.UnmarshalInt64(n, encoded)
	} else {
		n, err = bstd.SkipInt64(n, encoded)
	}

	if err != nil {
		return nil, err
	}

	if WantsField(desired, FieldTimestamp) {
		n, details.Timestamp, err = bstd.UnmarshalInt64(n, encoded)
	} else {
		n, err = bstd.SkipInt64(n, encoded)
	}

	if err != nil {
		return nil, err
	}

	if WantsField(desired, FieldUserName) {
		n, details.UserName, err = bstd.UnmarshalUnsafeString(n, encoded)
	} else {
		n, err = bstd.SkipString(n, encoded)
	}

	if err != nil {
		return nil, err
	}

	if WantsField(desired, FieldWastedCPUSeconds) {
		n, details.WastedCPUSeconds, err = bstd.UnmarshalFloat64(n, encoded)
	} else {
		n, err = bstd.SkipFloat64(n, encoded)
	}

	if err != nil {
		return nil, err
	}

	if WantsField(desired, FieldWastedMBSeconds) {
		n, details.WastedMBSeconds, err = bstd.UnmarshalFloat64(n, encoded)
	} else {
		n, err = bstd.SkipFloat64(n, encoded)
	}

	if err != nil {
		return nil, err
	}

	err = benc.VerifyUnmarshal(n, encoded)
	if err != nil {
		slog.Error("unmarhsal failed", "err", err,
			"attempt", details, "cmd_length", len(details.Command),
			"jobname_length", len(details.JobName), "job_length", len(details.Job),
			"encoded_length", len(encoded), "n", n)
	}

	return details, err
}

type Aggregations struct {
	Stats *Buckets `json:"stats,omitempty"`
}

type Buckets struct {
	Buckets []interface{} `json:"buckets,omitempty"`
}

func parseResultResponse(resp *esapi.Response) (*Result, error) {
	if resp.IsError() {
		return nil, Error{Msg: ErrFailedQuery, cause: resp.String()}
	}

	defer resp.Body.Close()

	var result Result

	if err := easyjson.UnmarshalFromReader(resp.Body, &result); err != nil {
		return nil, err
	}

	return &result, nil
}
