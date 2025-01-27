// From code generated by easyjson for marshaling/unmarshaling using:
// go get github.com/mailru/easyjson && go install github.com/mailru/easyjson/...@latest
// easyjson -all elasticsearch/result.go
// then edited to make non-pointers pointers, and customised to handle dynamic
// desired fields.

package elasticsearch

import (
	"encoding/json"

	easyjson "github.com/mailru/easyjson"
	jwriter "github.com/mailru/easyjson/jwriter"
)

// MarshalFields converts to JSON, but the JSON will only include the given
// fields of the hit details, even if they're zero value. If the desired map is
// empty, all fields are included.
func (v *Result) MarshalFields(desired Fields) ([]byte, error) {
	w := jwriter.Writer{}
	marshalFieldsResult(&w, v, desired)
	return w.Buffer.BuildBytes(), w.Error
}

func marshalFieldsResult(out *jwriter.Writer, in *Result, desired Fields) {
	out.RawByte('{')
	first := true
	_ = first
	if in.ScrollID != "" {
		const prefix string = ",\"_scroll_id\":"
		first = false
		out.RawString(prefix[1:])
		out.String(string(in.ScrollID))
	}
	{
		const prefix string = ",\"took\":"
		if first {
			first = false
			out.RawString(prefix[1:])
		} else {
			out.RawString(prefix)
		}
		out.Int(int(in.Took))
	}
	{
		const prefix string = ",\"timed_out\":"
		out.RawString(prefix)
		out.Bool(bool(in.TimedOut))
	}
	{
		const prefix string = ",\"hits\":"
		out.RawString(prefix)
		if in.HitSet == nil {
			out.RawString("null")
		} else {
			(*in.HitSet).MarshalFields(out, desired)
		}
	}
	if in.Aggregations != nil {
		const prefix string = ",\"aggregations\":"
		out.RawString(prefix)
		(*in.Aggregations).MarshalEasyJSON(out)
	}
	out.RawByte('}')
}

// MarshalFields converts to JSON, but the JSON will only include the given
// fields of the hit details, even if they're zero value. If the desired map is
// empty, all fields are included.
func (v *HitSet) MarshalFields(w *jwriter.Writer, desired Fields) {
	w.RawByte('{')
	first := true
	_ = first
	{
		const prefix string = ",\"total\":"
		w.RawString(prefix[1:])
		(v.Total).MarshalEasyJSON(w)
	}
	{
		const prefix string = ",\"hits\":"
		w.RawString(prefix)
		if v.Hits == nil && (w.Flags&jwriter.NilSliceAsEmpty) == 0 {
			w.RawString("null")
		} else {
			w.RawByte('[')
			for v2, v3 := range v.Hits {
				if v2 > 0 {
					w.RawByte(',')
				}
				(v3).MarshalEasyJSON(w, desired)
			}
			w.RawByte(']')
		}
	}
	w.RawByte('}')
}

// MarshalEasyJSON supports easyjson.Marshaler interface
func (v HitSetTotal) MarshalEasyJSON(w *jwriter.Writer) {
	w.RawByte('{')
	first := true
	_ = first
	{
		const prefix string = ",\"value\":"
		w.RawString(prefix[1:])
		w.Int(int(v.Value))
	}
	w.RawByte('}')
}

// MarshalEasyJSON supports easyjson.Marshaler interface
func (v Hit) MarshalEasyJSON(w *jwriter.Writer, desired Fields) {
	w.RawByte('{')
	first := true
	_ = first
	if v.ID != "" {
		const prefix string = ",\"_id\":"
		first = false
		w.RawString(prefix[1:])
		w.String(string(v.ID))
	}
	{
		const prefix string = ",\"_source\":"
		if first {
			first = false
			w.RawString(prefix[1:])
		} else {
			w.RawString(prefix)
		}
		if v.Details == nil {
			w.RawString("null")
		} else {
			(*v.Details).MarshalEasyJSON(w, desired)
		}
	}
	w.RawByte('}')
}

// MarshalEasyJSON supports easyjson.Marshaler interface
func (v Details) MarshalEasyJSON(w *jwriter.Writer, desired Fields) {
	w.RawByte('{')
	first := true
	_ = first

	if WantsField(desired, FieldAccountingName) {
		const prefix string = ",\"ACCOUNTING_NAME\":"
		if first {
			first = false
			w.RawString(prefix[1:])
		} else {
			w.RawString(prefix)
		}
		w.String(string(v.AccountingName))
	}

	if WantsField(desired, FieldAvailCPUTimeSec) {
		const prefix string = ",\"AVAIL_CPU_TIME_SEC\":"
		if first {
			first = false
			w.RawString(prefix[1:])
		} else {
			w.RawString(prefix)
		}
		w.Int64(int64(v.AvailCPUTimeSec))
	}

	if WantsField(desired, FieldBOM) {
		const prefix string = ",\"BOM\":"
		if first {
			first = false
			w.RawString(prefix[1:])
		} else {
			w.RawString(prefix)
		}
		w.String(string(v.BOM))
	}

	if WantsField(desired, FieldCommand) {
		const prefix string = ",\"Command\":"
		if first {
			first = false
			w.RawString(prefix[1:])
		} else {
			w.RawString(prefix)
		}
		w.String(string(v.Command))
	}

	if WantsField(desired, FieldJobName) {
		const prefix string = ",\"JOB_NAME\":"
		if first {
			first = false
			w.RawString(prefix[1:])
		} else {
			w.RawString(prefix)
		}
		w.String(string(v.JobName))
	}

	if WantsField(desired, FieldJob) {
		const prefix string = ",\"Job\":"
		if first {
			first = false
			w.RawString(prefix[1:])
		} else {
			w.RawString(prefix)
		}
		w.String(string(v.Job))
	}

	if WantsField(desired, FieldMemRequestedMB) {
		const prefix string = ",\"MEM_REQUESTED_MB\":"
		if first {
			first = false
			w.RawString(prefix[1:])
		} else {
			w.RawString(prefix)
		}
		w.Int64(int64(v.MemRequestedMB))
	}

	if WantsField(desired, FieldMemRequestedMBSec) {
		const prefix string = ",\"MEM_REQUESTED_MB_SEC\":"
		if first {
			first = false
			w.RawString(prefix[1:])
		} else {
			w.RawString(prefix)
		}
		w.Int64(int64(v.MemRequestedMBSec))
	}

	if WantsField(desired, FieldNumExecProcs) {
		const prefix string = ",\"NUM_EXEC_PROCS\":"
		if first {
			first = false
			w.RawString(prefix[1:])
		} else {
			w.RawString(prefix)
		}
		w.Int64(int64(v.NumExecProcs))
	}

	if WantsField(desired, FieldPendingTimeSec) {
		const prefix string = ",\"PENDING_TIME_SEC\":"
		if first {
			first = false
			w.RawString(prefix[1:])
		} else {
			w.RawString(prefix)
		}
		w.Int64(int64(v.PendingTimeSec))
	}

	if WantsField(desired, FieldQueueName) {
		const prefix string = ",\"QUEUE_NAME\":"
		if first {
			first = false
			w.RawString(prefix[1:])
		} else {
			w.RawString(prefix)
		}
		w.String(string(v.QueueName))
	}

	if WantsField(desired, FieldRunTimeSec) {
		const prefix string = ",\"RUN_TIME_SEC\":"
		if first {
			first = false
			w.RawString(prefix[1:])
		} else {
			w.RawString(prefix)
		}
		w.Int64(int64(v.RunTimeSec))
	}

	if WantsField(desired, FieldTimestamp) {
		const prefix string = ",\"timestamp\":"
		if first {
			first = false
			w.RawString(prefix[1:])
		} else {
			w.RawString(prefix)
		}
		w.Int64(int64(v.Timestamp))
	}

	if WantsField(desired, FieldUserName) {
		const prefix string = ",\"USER_NAME\":"
		if first {
			first = false
			w.RawString(prefix[1:])
		} else {
			w.RawString(prefix)
		}
		w.String(string(v.UserName))
	}

	if WantsField(desired, FieldWastedCPUSeconds) {
		const prefix string = ",\"WASTED_CPU_SECONDS\":"
		if first {
			first = false
			w.RawString(prefix[1:])
		} else {
			w.RawString(prefix)
		}
		w.Float64(float64(v.WastedCPUSeconds))
	}

	if WantsField(desired, FieldWastedMBSeconds) {
		const prefix string = ",\"WASTED_MB_SECONDS\":"
		if first {
			first = false
			w.RawString(prefix[1:])
		} else {
			w.RawString(prefix)
		}
		w.Float64(float64(v.WastedMBSeconds))
	}

	if WantsField(desired, FieldRawWastedCPUSeconds) {
		const prefix string = ",\"RAW_WASTED_CPU_SECONDS\":"
		if first {
			first = false
			w.RawString(prefix[1:])
		} else {
			w.RawString(prefix)
		}
		w.Float64(float64(v.RawWastedCPUSeconds))
	}

	if WantsField(desired, FieldRawWastedMBSeconds) {
		const prefix string = ",\"RAW_WASTED_MB_SECONDS\":"
		if first {
			first = false
			w.RawString(prefix[1:])
		} else {
			w.RawString(prefix)
		}
		w.Float64(float64(v.RawWastedMBSeconds))
	}

	w.RawByte('}')
}

// MarshalEasyJSON supports easyjson.Marshaler interface
func (v Aggregations) MarshalEasyJSON(w *jwriter.Writer) {
	w.RawByte('{')
	first := true
	_ = first
	if v.Stats != nil {
		const prefix string = ",\"stats\":"
		first = false
		w.RawString(prefix[1:])
		(*v.Stats).MarshalEasyJSON(w)
	}
	w.RawByte('}')
}

// MarshalEasyJSON supports easyjson.Marshaler interface
func (v Buckets) MarshalEasyJSON(w *jwriter.Writer) {
	w.RawByte('{')
	first := true
	_ = first
	if len(v.Buckets) != 0 {
		const prefix string = ",\"buckets\":"
		first = false
		w.RawString(prefix[1:])
		{
			w.RawByte('[')
			for v5, v6 := range v.Buckets {
				if v5 > 0 {
					w.RawByte(',')
				}
				if m, ok := v6.(easyjson.Marshaler); ok {
					m.MarshalEasyJSON(w)
				} else if m, ok := v6.(json.Marshaler); ok {
					w.Raw(m.MarshalJSON())
				} else {
					w.Raw(json.Marshal(v6))
				}
			}
			w.RawByte(']')
		}
	}
	w.RawByte('}')
}
