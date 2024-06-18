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
	"encoding/json"
	"fmt"

	"github.com/elastic/go-elasticsearch/v7/esapi"
)

type Error struct {
	Msg   string
	cause string
}

func (e Error) Error() string {
	if e.cause != "" {
		return fmt.Sprintf("%s: %s", e.Msg, e.cause)
	}

	return e.Msg
}

const ErrFailedQuery = "elasticsearch query failed"

type Result struct {
	ScrollID     string `json:"_scroll_id"`
	Took         int
	TimedOut     bool    `json:"timed_out"`
	HitSet       *HitSet `json:"hits"`
	Aggregations Aggregations
}

type HitSet struct {
	Total struct {
		Value int
	}
	Hits []Hit
}

type Hit struct {
	ID      string   `json:"_id"`
	Details *Details `json:"_source"`
}

type Details struct {
	AccountingName  string `json:"ACCOUNTING_NAME,omitempty"`
	AvailCPUTimeSec int    `json:"AVAIL_CPU_TIME_SEC,omitempty"`
	// AVG_MEM_EFFICIENCY_PERCENT     float64
	// AVRG_MEM_USAGE_MB              float64
	// AVRG_MEM_USAGE_MB_SEC_COOKED   float64
	// AVRG_MEM_USAGE_MB_SEC_RAW      float64
	BOM string `json:",omitempty"`
	// CLUSTER_NAME                   string
	// COOKED_CPU_TIME_SEC            float64
	Command string `json:",omitempty"`
	// END_TIME                       int
	// EXEC_HOSTNAME                  []string
	// Exit_Info                      int
	// Exitreason                     string
	// JOB_ID          int
	// JOB_ARRAY_INDEX int
	// JOB_EXIT_STATUS                int
	JobName string `json:"JOB_NAME,omitempty"`
	Job     string `json:",omitempty"`
	// Job_Efficiency_Percent         float64
	// Job_Efficiency_Raw_Percent     float64
	// MAX_MEM_EFFICIENCY_PERCENT     float64
	// MAX_MEM_USAGE_MB               float64
	// MAX_MEM_USAGE_MB_SEC_COOKED    float64
	// MAX_MEM_USAGE_MB_SEC_RAW       float64
	MemRequestedMB    int `json:"MEM_REQUESTED_MB,omitempty"`
	MemRequestedMBSec int `json:"MEM_REQUESTED_MB_SEC,omitempty"`
	NumExecProcs      int `json:"NUM_EXEC_PROCS,omitempty"`
	// NumberOfHosts                  int
	// NumberOfUniqueHosts            int
	PendingTimeSec int `json:"PENDING_TIME_SEC,omitempty"`
	// PROJECT_NAME                   string
	QueueName string `json:"QUEUE_NAME,omitempty"`
	// RAW_AVG_MEM_EFFICIENCY_PERCENT float64
	// RAW_CPU_TIME_SEC               float64
	// RAW_MAX_MEM_EFFICIENCY_PERCENT float64
	// RAW_WASTED_CPU_SECONDS         float64
	// RAW_WASTED_MB_SECONDS          float64
	RunTimeSec int `json:"RUN_TIME_SEC,omitempty"`
	// SUBMIT_TIME  int
	Timestamp        int64   `json:"timestamp,omitempty"`
	UserName         string  `json:"USER_NAME,omitempty"`
	WastedCPUSeconds float64 `json:"WASTED_CPU_SECONDS,omitempty"`
	WastedMBSeconds  float64 `json:"WASTED_MB_SECONDS,omitempty"`
}

type Aggregations struct {
	Stats struct {
		Buckets []struct {
			Key          string      `json:"key_as_string"`
			CPUAvailSec  BucketValue `json:"cpu_avail_sec"`
			MemAvailSec  BucketValue `json:"mem_avail_mb_sec"`
			CPUWastedSec BucketValue `json:"cpu_wasted_sec"`
			MemWastedSec BucketValue `json:"mem_wasted_mb_sec"`
			WastedCost   BucketValue `json:"wasted_cost"`
		}
	}
}

type BucketValue struct {
	Value float64
}

func parseResultResponse(resp *esapi.Response) (*Result, error) {
	if resp.IsError() {
		return nil, Error{Msg: ErrFailedQuery, cause: resp.String()}
	}

	defer resp.Body.Close()

	var result Result

	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}

	return &result, nil
}
