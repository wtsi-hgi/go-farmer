package main

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"

	bstd "github.com/deneonet/benc"
	"github.com/deneonet/benc/bpre"
	"github.com/deneonet/benc/bunsafe"
	"github.com/dgryski/go-farm"
	es "github.com/elastic/go-elasticsearch/v7"
	"github.com/elastic/go-elasticsearch/v7/esapi"
	lru "github.com/hashicorp/golang-lru/v2"
	"gopkg.in/yaml.v3"
)

const (
	index             = "user-data-ssg-isg-lsf-analytics-*"
	maxSize           = 10000
	scrollTime        = 1 * time.Minute
	cacheSize         = 128
	bucketName        = "hits"
	dateFormat        = "2006-01-02"
	dbDirPerms        = 0770
	fileBufferSize    = 4 * 1024 * 1024
	desiredFlatDBSize = 32 * 1024 * 1024

	timeStampLength        = 8
	bomMaxWidth            = 34
	accountingNameMaxWidth = 24
	userNameMaxWidth       = 12
	notInGPUQueue          = byte(1)
	inGPUQueue             = byte(2)
	lengthEncodeLength     = 4
	detailsBufferLength    = 16 * 1024

	testPeriod = 0 // 1 is 10 mins, 2 is 3 days, otherwise over a month
	debug      = false
)

type Query struct {
	Size  int          `json:"size"`
	Aggs  *Aggs        `json:"aggs,omitempty"`
	Query *QueryFilter `json:"query,omitempty"`
	Sort  []string     `json:"sort,omitempty"`
}

type Aggs struct {
	Stats AggsStats `json:"stats"`
}

type AggsStats struct {
	MultiTerms MultiTerms           `json:"multi_terms"`
	Aggs       map[string]AggsField `json:"aggs"`
}

type MultiTerms struct {
	Terms []Field `json:"terms"`
	Size  int     `json:"size"`
}

type Field struct {
	Field string `json:"field"`
}

type AggsField struct {
	Sum            *Field          `json:"sum,omitempty"`
	ScriptedMetric *ScriptedMetric `json:"scripted_metric,omitempty"`
}

type ScriptedMetric struct {
	InitScript    string      `json:"init_script"`
	MapScript     string      `json:"map_script"`
	CombineScript string      `json:"combine_script"`
	ReduceScript  string      `json:"reduce_script"`
	Params        interface{} `json:"params"`
}

type QueryFilter struct {
	Bool QFBool `json:"bool"`
}

type QFBool struct {
	Filter Filter `json:"filter"`
}

type Filter []map[string]map[string]interface{}

func (q *Query) AsBody() (*bytes.Reader, error) {
	queryBytes, err := q.toJSON()
	if err != nil {
		return nil, err
	}

	// fmt.Printf("query: %s\n", string(queryBytes))

	return bytes.NewReader(queryBytes), nil
}

func (q *Query) toJSON() ([]byte, error) {
	return json.Marshal(q)
}

func (q *Query) CacheKey() (string, error) {
	queryBytes, err := q.toJSON()
	if err != nil {
		return "", err
	}

	l, h := farm.Hash128(queryBytes)

	return fmt.Sprintf("%016x%016x", l, h), nil
}

type Config struct {
	Elastic struct {
		Host     string
		Username string
		Password string
		Scheme   string
		Port     int
	}
}

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
	mu   sync.Mutex
}

func (h *HitSet) AddHit(id string, details *Details) {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.Hits = append(h.Hits, Hit{
		ID:      id,
		Details: details,
	})
}

type Hit struct {
	ID      string   `json:"_id"`
	Details *Details `json:"_source"`
}

type Details struct {
	ACCOUNTING_NAME    string
	AVAIL_CPU_TIME_SEC int
	// AVG_MEM_EFFICIENCY_PERCENT     float64
	// AVRG_MEM_USAGE_MB              float64
	// AVRG_MEM_USAGE_MB_SEC_COOKED   float64
	// AVRG_MEM_USAGE_MB_SEC_RAW      float64
	BOM string
	// CLUSTER_NAME                   string
	// COOKED_CPU_TIME_SEC            float64
	Command string
	// END_TIME                       int
	// EXEC_HOSTNAME                  []string
	// Exit_Info                      int
	// Exitreason                     string
	// JOB_ID          int
	// JOB_ARRAY_INDEX int
	// JOB_EXIT_STATUS                int
	JOB_NAME string
	Job      string
	// Job_Efficiency_Percent         float64
	// Job_Efficiency_Raw_Percent     float64
	// MAX_MEM_EFFICIENCY_PERCENT     float64
	// MAX_MEM_USAGE_MB               float64
	// MAX_MEM_USAGE_MB_SEC_COOKED    float64
	// MAX_MEM_USAGE_MB_SEC_RAW       float64
	MEM_REQUESTED_MB     int
	MEM_REQUESTED_MB_SEC int
	NUM_EXEC_PROCS       int
	// NumberOfHosts                  int
	// NumberOfUniqueHosts            int
	PENDING_TIME_SEC int
	// PROJECT_NAME                   string
	QUEUE_NAME string
	// RAW_AVG_MEM_EFFICIENCY_PERCENT float64
	// RAW_CPU_TIME_SEC               float64
	// RAW_MAX_MEM_EFFICIENCY_PERCENT float64
	// RAW_WASTED_CPU_SECONDS         float64
	// RAW_WASTED_MB_SECONDS          float64
	RUN_TIME_SEC int
	// SUBMIT_TIME  int
	Timestamp          int64 `json:"timestamp"`
	USER_NAME          string
	WASTED_CPU_SECONDS float64
	WASTED_MB_SECONDS  float64
}

func (d *Details) serialize() ([]byte, error) {
	n := bstd.SizeString(d.ACCOUNTING_NAME)
	n += bstd.SizeInt()
	n += bstd.SizeString(d.BOM)
	n += bstd.SizeString(d.Command)
	n += bstd.SizeString(d.JOB_NAME)
	n += bstd.SizeString(d.Job)
	n += bstd.SizeInt()
	n += bstd.SizeInt()
	n += bstd.SizeInt()
	n += bstd.SizeInt()
	n += bstd.SizeString(d.QUEUE_NAME)
	n += bstd.SizeInt()
	n += bstd.SizeInt64()
	n += bstd.SizeString(d.USER_NAME)
	n += bstd.SizeFloat64()
	n += bstd.SizeFloat64()

	bpre.Reset()

	n, encoded := bstd.Marshal(n)
	n = bstd.MarshalString(n, encoded, d.ACCOUNTING_NAME)
	n = bstd.MarshalInt(n, encoded, d.AVAIL_CPU_TIME_SEC)
	n = bstd.MarshalString(n, encoded, d.BOM)
	n = bstd.MarshalString(n, encoded, d.Command)
	n = bstd.MarshalString(n, encoded, d.JOB_NAME)
	n = bstd.MarshalString(n, encoded, d.Job)
	n = bstd.MarshalInt(n, encoded, d.MEM_REQUESTED_MB)
	n = bstd.MarshalInt(n, encoded, d.MEM_REQUESTED_MB_SEC)
	n = bstd.MarshalInt(n, encoded, d.NUM_EXEC_PROCS)
	n = bstd.MarshalInt(n, encoded, d.PENDING_TIME_SEC)
	n = bstd.MarshalString(n, encoded, d.QUEUE_NAME)
	n = bstd.MarshalInt(n, encoded, d.RUN_TIME_SEC)
	n = bstd.MarshalInt64(n, encoded, d.Timestamp)
	n = bstd.MarshalString(n, encoded, d.USER_NAME)
	n = bstd.MarshalFloat64(n, encoded, d.WASTED_CPU_SECONDS)
	n = bstd.MarshalFloat64(n, encoded, d.WASTED_MB_SECONDS)

	err := bstd.VerifyMarshal(n, encoded)

	return encoded, err
}

func (d *Details) deserialize(buf []byte) error {
	var (
		n   int
		err error
	)

	n, d.ACCOUNTING_NAME, err = bunsafe.UnmarshalString(0, buf)
	if err != nil {
		return err
	}

	n, d.AVAIL_CPU_TIME_SEC, err = bstd.UnmarshalInt(n, buf)
	if err != nil {
		return err
	}

	n, d.BOM, err = bunsafe.UnmarshalString(n, buf)
	if err != nil {
		return err
	}

	n, d.Command, err = bunsafe.UnmarshalString(n, buf)
	if err != nil {
		return err
	}

	n, d.JOB_NAME, err = bstd.UnmarshalString(n, buf)
	if err != nil {
		return err
	}

	n, d.Job, err = bunsafe.UnmarshalString(n, buf)
	if err != nil {
		return err
	}

	n, d.MEM_REQUESTED_MB, err = bstd.UnmarshalInt(n, buf)
	if err != nil {
		return err
	}

	n, d.MEM_REQUESTED_MB_SEC, err = bstd.UnmarshalInt(n, buf)
	if err != nil {
		return err
	}

	n, d.NUM_EXEC_PROCS, err = bstd.UnmarshalInt(n, buf)
	if err != nil {
		return err
	}

	n, d.PENDING_TIME_SEC, err = bstd.UnmarshalInt(n, buf)
	if err != nil {
		return err
	}

	n, d.QUEUE_NAME, err = bunsafe.UnmarshalString(n, buf)
	if err != nil {
		return err
	}

	n, d.RUN_TIME_SEC, err = bstd.UnmarshalInt(n, buf)
	if err != nil {
		return err
	}

	n, d.Timestamp, err = bstd.UnmarshalInt64(n, buf)
	if err != nil {
		return err
	}

	n, d.USER_NAME, err = bunsafe.UnmarshalString(n, buf)
	if err != nil {
		return err
	}

	n, d.WASTED_CPU_SECONDS, err = bstd.UnmarshalFloat64(n, buf)
	if err != nil {
		return err
	}

	n, d.WASTED_MB_SECONDS, err = bstd.UnmarshalFloat64(n, buf)
	if err != nil {
		return err
	}

	return bstd.VerifyUnmarshal(n, buf)
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

func main() {
	data, err := os.ReadFile(os.Args[1])
	if err != nil {
		log.Fatalf("error: %v", err)
	}

	c := Config{}

	err = yaml.Unmarshal([]byte(data), &c)
	if err != nil {
		log.Fatalf("error: %v", err)
	}

	cfg := es.Config{
		Addresses: []string{
			fmt.Sprintf("%s://%s:%d", c.Elastic.Scheme, c.Elastic.Host, c.Elastic.Port),
		},
		Username: c.Elastic.Username,
		Password: c.Elastic.Password,
	}

	client, err := es.NewClient(cfg)
	if err != nil {
		log.Fatalf("%s\n", err)
	}

	l, err := lru.New[string, *Result](cacheSize)
	if err != nil {
		log.Fatalf("%s\n", err)
	}

	dbPath := os.Args[2]

	dbExisted := false

	if _, err = os.Stat(dbPath); err == nil {
		dbExisted = true
	}

	if !dbExisted {
		err = initDB(dbPath, client)
		if err != nil {
			log.Fatalf("%s\n", err)
		}

		return
	}

	// t := time.Now()

	// bomQuery := &Query{
	// 	Aggs: &Aggs{
	// 		Stats: AggsStats{
	// 			MultiTerms: MultiTerms{
	// 				Terms: []Field{
	// 					{Field: "ACCOUNTING_NAME"},
	// 					{Field: "NUM_EXEC_PROCS"},
	// 					{Field: "Job"},
	// 				},
	// 				Size: 1000,
	// 			},
	// 			Aggs: map[string]AggsField{
	// 				"cpu_avail_sec": {
	// 					Sum: &Field{Field: "AVAIL_CPU_TIME_SEC"},
	// 				},
	// 				"cpu_wasted_sec": {
	// 					Sum: &Field{Field: "WASTED_CPU_SECONDS"},
	// 				},
	// 				"mem_avail_mb_sec": {
	// 					Sum: &Field{Field: "MEM_REQUESTED_MB_SEC"},
	// 				},
	// 				"mem_wasted_mb_sec": {
	// 					Sum: &Field{Field: "WASTED_MB_SECONDS"},
	// 				},
	// 				"wasted_cost": {
	// 					ScriptedMetric: &ScriptedMetric{
	// 						InitScript:    "state.costs = []",
	// 						MapScript:     "double cpu_cost = doc.WASTED_CPU_SECONDS.value * params.cpu_second; double mem_cost = doc.WASTED_MB_SECONDS.value * params.mb_second; state.costs.add(Math.max(cpu_cost, mem_cost))",
	// 						CombineScript: "double total = 0; for (t in state.costs) { total += t } return total",
	// 						ReduceScript:  "double total = 0; for (a in states) { total += a } return total",
	// 						Params: map[string]float64{
	// 							"cpu_second": 7.0556e-07,
	// 							"mb_second":  5.8865e-11,
	// 						},
	// 					},
	// 				},
	// 			},
	// 		},
	// 	},
	// 	Query: &QueryFilter{
	// 		Bool: QFBool{
	// 			Filter: Filter{
	// 				{"match_phrase": map[string]interface{}{"META_CLUSTER_NAME": "farm"}},
	// 				{"range": map[string]interface{}{
	// 					"timestamp": map[string]string{
	// 						"lte":    "2024-06-04T00:00:00Z",
	// 						"gte":    "2024-05-04T00:00:00Z",
	// 						"format": "strict_date_optional_time",
	// 					},
	// 				}},
	// 				{"match_phrase": map[string]interface{}{"BOM": "Human Genetics"}},
	// 			},
	// 		},
	// 	},
	// }

	// result, err := Search(l, client, index, bomQuery)
	// if err != nil {
	// 	log.Fatalf("Error searching: %s", err)
	// }

	// if len(result.HitSet.Hits) > 0 {
	// 	fmt.Printf("first hit: %+v\n", result.HitSet.Hits[0])
	// }

	// if len(result.Aggregations.Stats.Buckets) > 0 {
	// 	fmt.Printf("first agg: %+v\n", result.Aggregations.Stats.Buckets[0])
	// }
	// fmt.Printf("took: %s\n\n", time.Since(t))

	lte := "2024-06-04T00:00:00Z"
	gte := "2024-05-04T00:00:00Z"
	if testPeriod == 1 {
		lte = "2024-06-09T23:55:00Z"
		gte = "2024-06-09T23:50:00Z"
	} else if testPeriod == 2 {
		gte = "2024-06-03T00:00:00Z"
	}

	filter := Filter{
		{"match_phrase": map[string]interface{}{"META_CLUSTER_NAME": "farm"}},
		{"range": map[string]interface{}{
			"timestamp": map[string]string{
				"lte":    lte,
				"gte":    gte,
				"format": "strict_date_optional_time",
			},
		}},
		{"match_phrase": map[string]interface{}{"BOM": "Human Genetics"}},
		{"match_phrase": map[string]interface{}{"ACCOUNTING_NAME": "hgi"}},
	}

	t := time.Now()
	result, err := Scroll(l, dbPath, client, index, filter)
	if err != nil {
		log.Fatalf("Error searching: %s", err)
	}

	if result == nil {
		return
	}

	if len(result.HitSet.Hits) > 0 {
		fmt.Printf("num hits: %+v\n", len(result.HitSet.Hits))
		fmt.Printf("first hit: %+v\n", result.HitSet.Hits[0].Details)
	}

	if len(result.Aggregations.Stats.Buckets) > 0 {
		fmt.Printf("first agg: %+v\n", result.Aggregations.Stats.Buckets[0])
	}
	fmt.Printf("took: %s\n\n", time.Since(t))

	// t = time.Now()
	// result, err = Search(l, client, index, bomQuery)
	// if err != nil {
	// 	log.Fatalf("Error searching: %s", err)
	// }

	// if len(result.HitSet.Hits) > 0 {
	// 	fmt.Printf("first hit: %+v\n", result.HitSet.Hits[0])
	// }

	// if len(result.Aggregations.Stats.Buckets) > 0 {
	// 	fmt.Printf("first agg: %+v\n", result.Aggregations.Stats.Buckets[0])
	// }
	// fmt.Printf("took: %s\n\n", time.Since(t))

	// t = time.Now()
	// result, err = Scroll(l, client, index, filter)
	// if err != nil {
	// 	log.Fatalf("Error searching: %s", err)
	// }

	// if result == nil {
	// 	return
	// }

	// if len(result.HitSet.Hits) > 0 {
	// 	fmt.Printf("num hits: %+v\n", len(result.HitSet.Hits))
	// 	fmt.Printf("first hit: %+v\n", result.HitSet.Hits[0])
	// }

	// if len(result.Aggregations.Stats.Buckets) > 0 {
	// 	fmt.Printf("first agg: %+v\n", result.Aggregations.Stats.Buckets[0])
	// }
	// fmt.Printf("took: %s\n\n", time.Since(t))
}

func initDB(dbPath string, client *es.Client) error {
	lte := "2024-06-10T00:00:00Z"
	gte := "2024-05-01T00:00:00Z"
	if testPeriod == 1 {
		gte = "2024-06-09T23:50:00Z"
	} else if testPeriod == 2 {
		lte = "2024-06-04T00:00:00Z"
		gte = "2024-06-02T00:00:00Z"
	}

	filter := Filter{
		{"match_phrase": map[string]interface{}{"META_CLUSTER_NAME": "farm"}},
		{"range": map[string]interface{}{
			"timestamp": map[string]string{
				"lte":    lte,
				"gte":    gte,
				"format": "strict_date_optional_time",
			},
		}},
	}

	query := &Query{
		Size:  maxSize,
		Sort:  []string{"timestamp", "_doc"},
		Query: &QueryFilter{Bool: QFBool{Filter: filter}},
	}

	t := time.Now()

	result, err := searchWithScroll(client, query)
	if err != nil {
		return err
	}

	fmt.Printf("\nsearch took: %s\n", time.Since(t))
	t = time.Now()

	err = storeInLocalDB(dbPath, result)
	if err != nil {
		return err
	}

	fmt.Printf("store took: %s\n", time.Since(t))

	return nil
}

type FlatDB struct {
	path  string
	f     *os.File
	w     *bufio.Writer
	n     int64
	index int
}

func NewFlatDB(path string) (*FlatDB, error) {
	f, w, err := createFileAndWriter(path, 0)
	if err != nil {
		return nil, err
	}

	return &FlatDB{
		path: path,
		f:    f,
		w:    w,
	}, nil
}

func createFileAndWriter(path string, index int) (*os.File, *bufio.Writer, error) {
	f, err := os.Create(fmt.Sprintf("%s.%d", path, index))
	if err != nil {
		return nil, nil, err
	}

	w := bufio.NewWriterSize(f, fileBufferSize)

	return f, w, nil
}

func (f *FlatDB) Store(fields ...[]byte) error {
	total := 0

	for _, field := range fields {
		n, err := f.w.Write(field)
		total += n

		if err != nil {
			return err
		}
	}

	f.n += int64(total)
	if f.n > desiredFlatDBSize {
		err := f.Close()
		if err != nil {
			return err
		}

		f.index++

		file, w, err := createFileAndWriter(f.path, f.index)
		if err != nil {
			return err
		}

		f.f = file
		f.w = w
		f.n = 0
	}

	return nil
}

func (f *FlatDB) Close() error {
	f.w.Flush()

	return f.f.Close()
}

type LocalDB struct {
	dir string
	dbs map[string]*FlatDB
}

func NewLocalDB(dir string) (*LocalDB, error) {
	err := os.MkdirAll(dir, dbDirPerms)
	if err != nil {
		return nil, err
	}

	return &LocalDB{
		dir: dir,
		dbs: make(map[string]*FlatDB),
	}, nil
}

func (l *LocalDB) GetFlatDB(timestamp int64, bom string) (*FlatDB, error) {
	key := fmt.Sprintf("%s.%s", time.Unix(timestamp, 0).UTC().Format(dateFormat), bom)

	fdb, ok := l.dbs[key]
	if !ok {
		var err error

		fdb, err = NewFlatDB(filepath.Join(l.dir, key))
		if err != nil {
			return nil, err
		}

		l.dbs[key] = fdb
	}

	return fdb, nil
}

func (l *LocalDB) Close() error {
	for _, fdb := range l.dbs {
		if err := fdb.Close(); err != nil {
			return err
		}
	}

	return nil
}

func storeInLocalDB(dbPath string, result *Result) (err error) {
	fmt.Printf("storing %d hits\n", len(result.HitSet.Hits))

	ldb, errl := NewLocalDB(dbPath)
	if errl != nil {
		return errl
	}

	defer func() {
		errc := ldb.Close()
		if err == nil {
			err = errc
		}
	}()

	// var buf bytes.Buffer

	// buf := make([]byte, detailsBufferLength)
	bpre.Marshal(detailsBufferLength)

	for _, hit := range result.HitSet.Hits {
		group, errf := fixedWidthGroup(hit.Details.ACCOUNTING_NAME)
		if errf != nil {
			err = errf
			return
		}

		user, errf := fixedWidthUser(hit.Details.USER_NAME)
		if errf != nil {
			err = errf
			return
		}

		isGPU := notInGPUQueue
		if strings.HasPrefix(hit.Details.QUEUE_NAME, "gpu") {
			isGPU = inGPUQueue
		}

		fdb, errl := ldb.GetFlatDB(hit.Details.Timestamp, hit.Details.BOM)
		if errl != nil {
			err = errf
			return
		}

		encoded, errs := hit.Details.serialize()
		if errs != nil {
			err = errs
			return
		}

		errs = fdb.Store(
			i64tob(hit.Details.Timestamp),
			group,
			user,
			[]byte{isGPU},
			i32tob(int32(len(encoded))),
			encoded,
		)
		if errs != nil {
			err = errs
			return
		}
	}

	return
}

func fixedWidthGroup(group string) ([]byte, error) {
	return fixedWidthString(group, accountingNameMaxWidth, "ACCOUNTING_NAME")
}

func fixedWidthString(str string, max int, kind string) ([]byte, error) {
	padding := max - len(str)
	if padding < 0 {
		return nil, errors.New(kind + " too long")
	}

	return []byte(str + strings.Repeat(" ", padding)), nil
}

func fixedWidthUser(user string) ([]byte, error) {
	return fixedWidthString(user, userNameMaxWidth, "USER_NAME")
}

// i64tob returns an 8-byte big endian representation of v. The result is a
// sortable byte representation of something like a unix time stamp in seconds.
func i64tob(v int64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(v))
	return b
}

func i32tob(v int32) []byte {
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, uint32(v))
	return b
}

func btoi(b []byte) int {
	return int(binary.BigEndian.Uint32(b[0:4]))
}

func Search(l *lru.Cache[string, *Result], client *es.Client, index string, query *Query) (*Result, error) {
	cacheKey, err := query.CacheKey()
	if err != nil {
		return nil, err
	}

	result, ok := l.Get(cacheKey)
	if ok {
		return result, nil
	}

	qbody, err := query.AsBody()
	if err != nil {
		return nil, err
	}

	resp, err := client.Search(
		client.Search.WithIndex(index),
		client.Search.WithBody(qbody),
	)
	if err != nil {
		return nil, err
	}

	result, err = parseResponse(resp)
	if err != nil {
		return nil, err
	}

	l.Add(cacheKey, result)

	return result, nil
}

func parseResponse(resp *esapi.Response) (*Result, error) {
	if resp.IsError() {
		return nil, fmt.Errorf("elasticsearch query failed: %s", resp)
	}

	defer resp.Body.Close()

	// bodyBytes, _ := io.ReadAll(resp.Body)
	// fmt.Println(string(bodyBytes))
	// os.Exit(0)

	var result Result

	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}

	// fmt.Printf("total: %d; hits: %d; aggs: %d\n",
	// 	result.HitSet.Total.Value, len(result.HitSet.Hits), len(result.Aggregations.Stats.Buckets))
	fmt.Printf(".")

	return &result, nil
}

func Scroll(l *lru.Cache[string, *Result], dbPath string, client *es.Client, index string, filter Filter) (*Result, error) {
	query := &Query{
		Size:  maxSize,
		Sort:  []string{"_doc"},
		Query: &QueryFilter{Bool: QFBool{Filter: filter}},
	}

	cacheKey, err := query.CacheKey()
	if err != nil {
		return nil, err
	}

	result, ok := l.Get(cacheKey)
	if ok {
		return result, nil
	}

	result, err = searchLocal(dbPath, query)
	if err != nil {
		return nil, err
	}

	l.Add(cacheKey, result)

	return result, nil
}

type LocalFilter struct {
	BOM             string
	LTE             time.Time
	GTE             time.Time
	LTEKey          []byte
	GTEKey          []byte
	accountingName  []byte
	userName        []byte
	checkAccounting bool
	checkUser       bool
	checkGPU        bool
}

func NewLocalFilter(query *Query) (*LocalFilter, error) {
	lte, gte, err := parseRange(query.Query.Bool.Filter)
	if err != nil {
		return nil, err
	}

	bom, err := queryToBomStr(query)
	if err != nil {
		return nil, err
	}

	filter := &LocalFilter{
		BOM: bom,
		LTE: lte,
		GTE: gte,
	}

	filter.LTEKey, filter.GTEKey = i64tob(lte.Unix()), i64tob(gte.Unix())

	// lteStamp := time.Unix(btoi64(filter.LTEKey), 0).UTC().Format(time.RFC3339)

	filter.accountingName, filter.userName, filter.checkGPU = queryToFilters(query)
	filter.checkAccounting = len(filter.accountingName) > 0
	filter.checkUser = len(filter.userName) > 0

	return filter, nil
}

func (f *LocalFilter) PassesAccountingName(val []byte) bool {
	if !f.checkAccounting {
		return true
	}

	return bytes.Equal(val, f.accountingName)
}

func (f *LocalFilter) PassesUserName(val []byte) bool {
	if !f.checkUser {
		return true
	}

	return bytes.Equal(val, f.userName)
}

func (f *LocalFilter) PassesGPUCheck(val byte) bool {
	if !f.checkGPU {
		return true
	}

	return val == inGPUQueue
}

func searchLocal(dbPath string, query *Query) (*Result, error) {
	entries, err := os.ReadDir(dbPath)
	if err != nil {
		return nil, err
	}

	basenames := make(map[string][]string)

	for _, entry := range entries {
		ext := filepath.Ext(entry.Name())
		if ext == "" {
			continue
		}

		name := strings.TrimSuffix(entry.Name(), ext)

		basenames[name] = append(basenames[name], filepath.Join(dbPath, entry.Name()))
	}

	filter, err := NewLocalFilter(query)
	if err != nil {
		return nil, err
	}

	errCh := make(chan error)
	result := &Result{
		HitSet: &HitSet{},
	}

	errsDoneCh := make(chan struct{})
	go func() {
		for err := range errCh {
			fmt.Printf("error: %s\n", err)
		}

		close(errsDoneCh)
	}()

	var wg sync.WaitGroup

	currentDay := filter.GTE

	for {
		fileKey := currentDay.UTC().Format(dateFormat)
		paths := basenames[fmt.Sprintf("%s.%s", fileKey, filter.BOM)]

		for _, path := range paths {
			wg.Add(1)
			go func(dbFilePath string) {
				defer wg.Done()

				searchLocalFile(dbFilePath, filter, result.HitSet, errCh)
			}(path)
		}

		currentDay = currentDay.Add(24 * time.Hour)
		if currentDay.After(filter.LTE) {
			break
		}
	}

	wg.Wait()
	close(errCh)
	<-errsDoneCh

	return result, nil
}

func searchLocalFile(dbFilePath string, filter *LocalFilter, hitset *HitSet, errCh chan error) {
	f, err := os.Open(dbFilePath)
	if err != nil {
		errCh <- err

		return
	}

	tsBuf := make([]byte, timeStampLength)
	accBuf := make([]byte, accountingNameMaxWidth)
	userBuf := make([]byte, userNameMaxWidth)
	lenBuf := make([]byte, lengthEncodeLength)
	detailsBuf := make([]byte, detailsBufferLength)

	br := bufio.NewReaderSize(f, fileBufferSize)

	t := time.Now()
	total := 0
	numGoroutines := runtime.NumGoroutine()

	for {
		total++

		_, err = io.ReadFull(br, tsBuf)
		if err != nil {
			if !errors.Is(err, io.EOF) {
				errCh <- err

				return
			}

			break
		}

		if bytes.Compare(tsBuf, filter.LTEKey) > 0 {
			break
		}

		var passesFilter bool

		if bytes.Compare(tsBuf, filter.GTEKey) >= 0 {
			passesFilter = true
		}

		_, err = io.ReadFull(br, accBuf)
		if err != nil {
			errCh <- err

			return
		}

		if passesFilter && !filter.PassesAccountingName(accBuf) {
			passesFilter = false
		}

		_, err = io.ReadFull(br, userBuf)
		if err != nil {
			errCh <- err

			return
		}

		if passesFilter && !filter.PassesUserName(userBuf) {
			passesFilter = false
		}

		gpuByte, err := br.ReadByte()
		if err != nil {
			errCh <- err

			return
		}

		if passesFilter && !filter.PassesGPUCheck(gpuByte) {
			passesFilter = false
		}

		_, err = io.ReadFull(br, lenBuf)
		if err != nil {
			errCh <- err

			return
		}

		detailsLength := btoi(lenBuf)

		buf := detailsBuf[0:detailsLength]

		_, err = io.ReadFull(br, buf)
		if err != nil {
			errCh <- err

			return
		}

		if passesFilter {
			d := &Details{}

			err = d.deserialize(buf)
			if err != nil {
				errCh <- err

				return
			}

			hitset.AddHit("", d)
		}
	}

	if debug {
		fmt.Printf("%s took %s for %d hits, starting with %d goroutines\n", dbFilePath, time.Since(t), total, numGoroutines)
	}

	f.Close()
}

func parseRange(filter Filter) (lte time.Time, gte time.Time, err error) {
	for _, val := range filter {
		fRange, ok := val["range"]
		if !ok {
			continue
		}

		timestampInterface, ok := fRange["timestamp"]
		if !ok {
			break
		}

		timestamp, ok := timestampInterface.(map[string]string)
		if !ok {
			break
		}

		lte, err = time.Parse(time.RFC3339, timestamp["lte"])
		if err != nil {
			return
		}

		gte, err = time.Parse(time.RFC3339, timestamp["gte"])
		if err != nil {
			return
		}

		return
	}

	err = errors.New("no timestamp range found")

	return
}

func queryToBomStr(query *Query) (string, error) {
	for _, val := range query.Query.Bool.Filter {
		mp, ok := val["match_phrase"]
		if !ok {
			continue
		}

		bomStr := stringFromFilterValue(mp, "BOM")
		if bomStr == "" {
			continue
		}

		return bomStr, nil
	}

	return "", errors.New("BOM not specified")
}

func stringFromFilterValue(fv map[string]interface{}, key string) string {
	keyInterface, ok := fv[key]
	if !ok {
		return ""
	}

	keyString, ok := keyInterface.(string)
	if !ok {
		return ""
	}

	return keyString
}

func queryToFilters(query *Query) (accountingName, userName []byte, checkGPU bool) {
	for _, val := range query.Query.Bool.Filter {
		mp, ok := val["match_phrase"]
		if !ok {
			continue
		}

		thisStr := stringFromFilterValue(mp, "ACCOUNTING_NAME")
		if thisStr != "" {
			if b, err := fixedWidthGroup(thisStr); err == nil {
				accountingName = b
			}
		}

		thisStr = stringFromFilterValue(mp, "USER_NAME")
		if thisStr != "" {
			if b, err := fixedWidthUser(thisStr); err == nil {
				userName = b
			}
		}
	}

	for _, val := range query.Query.Bool.Filter {
		p, ok := val["prefix"]
		if !ok {
			continue
		}

		thisStr := stringFromFilterValue(p, "QUEUE_NAME")
		if thisStr == "gpu" {
			checkGPU = true

			break
		}
	}

	return
}

func searchWithScroll(client *es.Client, query *Query) (*Result, error) {
	qbody, err := query.AsBody()
	if err != nil {
		return nil, err
	}

	resp, err := client.Search(
		client.Search.WithIndex(index),
		client.Search.WithBody(qbody),
		client.Search.WithSize(maxSize),
		client.Search.WithScroll(scrollTime),
	)
	if err != nil {
		return nil, err
	}

	result, err := parseResponse(resp)
	if err != nil {
		return nil, err
	}

	defer func() {
		scrollIDBody, err := scrollIDBody(result.ScrollID)
		if err != nil {
			log.Fatalf("scrollIDBody failed: %s\n", err)
			return
		}

		_, err = client.ClearScroll(client.ClearScroll.WithBody(scrollIDBody))
		if err != nil {
			log.Fatalf("clearscroll failed: %s\n", err)
		}
	}()

	total := result.HitSet.Total.Value
	if total <= maxSize {
		return result, nil
	}

	for keepScrolling := true; keepScrolling; keepScrolling = len(result.HitSet.Hits) < total {
		err = scroll(client, result)
		if err != nil {
			return nil, err
		}
	}

	return result, nil
}

func scrollIDBody(scrollID string) (*bytes.Buffer, error) {
	scrollBytes, err := json.Marshal(&map[string]string{"scroll_id": scrollID})
	if err != nil {
		return nil, err
	}

	return bytes.NewBuffer(scrollBytes), nil
}

func scroll(client *es.Client, result *Result) error {
	scrollIDBody, err := scrollIDBody(result.ScrollID)
	if err != nil {
		return err
	}

	resp, err := client.Scroll(
		client.Scroll.WithBody(scrollIDBody),
		client.Scroll.WithScroll(scrollTime),
	)
	if err != nil {
		return err
	}

	scrollResult, err := parseResponse(resp)
	if err != nil {
		return err
	}

	result.HitSet.Hits = append(result.HitSet.Hits, scrollResult.HitSet.Hits...)
	result.ScrollID = scrollResult.ScrollID

	return nil
}
