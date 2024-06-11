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
	"strings"
	"syscall"
	"time"

	"github.com/dgryski/go-farm"
	es "github.com/elastic/go-elasticsearch/v7"
	"github.com/elastic/go-elasticsearch/v7/esapi"
	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/ugorji/go/codec"
	bolt "go.etcd.io/bbolt"
	"gopkg.in/yaml.v3"

	"github.com/dsnet/compress/bzip2"
)

const (
	index          = "user-data-ssg-isg-lsf-analytics-*"
	maxSize        = 10000
	scrollTime     = 1 * time.Minute
	cacheSize      = 128
	bucketName     = "hits"
	dateFormat     = "2006-01-02"
	dbDirPerms     = 0770
	flatDBReadSize = 4687 // max length of a record on a month's worth of sample data

	timeStampLength        = 8
	endOfTimeStampIndex    = timeStampLength - 1
	bomMaxWidth            = 34
	startOfBomIndex        = timeStampLength
	endOfBomIndex          = startOfBomIndex + bomMaxWidth
	accountingNameMaxWidth = 24
	startOfAccountingIndex = endOfBomIndex
	endOfAccountingIndex   = startOfAccountingIndex + accountingNameMaxWidth
	userNameMaxWidth       = 12
	startOfUserIndex       = endOfAccountingIndex
	endOfUserIndex         = startOfUserIndex + userNameMaxWidth
	notInGPUQueue          = byte(1)
	inGPUQueue             = byte(2)
	isGPUIndex             = endOfUserIndex
	lengthEncodeLength     = 4

	testMode       = false
	useMapPopulate = false
	loadFromBolt   = false
	boltPath       = "/lustre/scratch123/hgi/mdt2/teams/hgi/ip13/farmers/bolt.db.keys"
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
	TimedOut     bool   `json:"timed_out"`
	HitSet       HitSet `json:"hits"`
	Aggregations Aggregations
}

type HitSet struct {
	Total struct {
		Value int
	}
	Hits []Hit
}

type Hit struct {
	ID      string  `json:"_id"`
	Details Details `json:"_source"`
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
	if testMode {
		lte = "2024-06-09T23:55:00Z"
		gte = "2024-06-09T23:50:00Z"
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
		// fmt.Printf("first hit: %+v\n", result.HitSet.Hits[0])
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

func openDB(path string) (*bolt.DB, error) {
	opts := &bolt.Options{
		PreLoadFreelist: true,
		FreelistType:    bolt.FreelistMapType,
	}

	if useMapPopulate {
		opts.MmapFlags = syscall.MAP_POPULATE
	}

	db, err := bolt.Open(path, 0600, opts)
	if err != nil {
		return nil, err
	}

	err = db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(bucketName))

		return err
	})
	if err != nil {
		return nil, err
	}

	return db, nil
}

func initDB(dbPath string, client *es.Client) error {
	lte := "2024-06-10T00:00:00Z"
	gte := "2024-05-01T00:00:00Z"
	if testMode {
		gte = "2024-06-09T23:50:00Z"
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

	var result *Result

	var err error

	if loadFromBolt {
		bdb, erro := openDB(boltPath)
		if erro != nil {
			return erro
		}

		defer bdb.Close()

		result, err = searchBolt(bdb, query)
	} else {
		result, err = searchWithScroll(client, query)
	}

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

type LocalDB struct {
	dir string
	dbs map[string]*flatDB
}

type flatDB struct {
	f *os.File
	w io.WriteCloser
}

func NewLocalDB(dir string) (*LocalDB, error) {
	err := os.MkdirAll(dir, dbDirPerms)
	if err != nil {
		return nil, err
	}

	return &LocalDB{
		dir: dir,
		dbs: make(map[string]*flatDB),
	}, nil
}

func (l *LocalDB) getFlatDB(timestamp int64, bom string) (*flatDB, error) {
	key := fmt.Sprintf("%s.%s", time.Unix(timestamp, 0).UTC().Format(dateFormat), bom)

	fdb, ok := l.dbs[key]
	if !ok {
		f, err := os.Create(filepath.Join(l.dir, key))
		if err != nil {
			return nil, err
		}

		w, _ := bzip2.NewWriter(f, nil)

		fdb = &flatDB{f, w}
		l.dbs[key] = fdb
	}

	return fdb, nil
}

func (l *LocalDB) Close() error {
	for _, fdb := range l.dbs {
		if err := fdb.w.Close(); err != nil {
			return err
		}

		if err := fdb.f.Close(); err != nil {
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

	var buf bytes.Buffer

	ch := new(codec.BincHandle)

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

		fdb, errl := ldb.getFlatDB(hit.Details.Timestamp, hit.Details.BOM)
		if errl != nil {
			err = errf
			return
		}

		fdb.w.Write(i64tob(hit.Details.Timestamp))
		fdb.w.Write(group)
		fdb.w.Write(user)
		fdb.w.Write([]byte{isGPU})

		buf.Reset()
		enc := codec.NewEncoder(&buf, ch)
		enc.MustEncode(hit.Details)
		encoded := buf.Bytes()

		fdb.w.Write(i32tob(int32(len(encoded))))
		fdb.w.Write(encoded)
	}

	return
}

func fixedWidthBOM(bom string) ([]byte, error) {
	return fixedWidthString(bom, bomMaxWidth, "BOM")
}

func fixedWidthString(str string, max int, kind string) ([]byte, error) {
	padding := max - len(str)
	if padding < 0 {
		return nil, errors.New(kind + " too long")
	}

	return []byte(str + strings.Repeat(" ", padding)), nil
}

func fixedWidthGroup(group string) ([]byte, error) {
	return fixedWidthString(group, accountingNameMaxWidth, "ACCOUNTING_NAME")
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

// btoi64 converts an 8-byte slice into an int64.
func btoi64(b []byte) int64 {
	return int64(binary.BigEndian.Uint64(b[0:8]))
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

func searchLocal(dbPath string, query *Query) (*Result, error) {
	// ldb, err := NewLocalDB(dbPath)
	// if err != nil {
	// 	return nil, err
	// }

	lte, gte, err := parseRange(query.Query.Bool.Filter)
	if err != nil {
		return nil, err
	}

	bom, err := queryToBomStr(query)
	if err != nil {
		return nil, err
	}

	lteKey, gteKey := i64tob(lte.Unix()), i64tob(gte.Unix())

	accountingName, userName, checkGPU := queryToFilters(query)

	ch := new(codec.BincHandle)

	result := &Result{}

	currentDay := gte

	tsBuf := make([]byte, timeStampLength)
	accBuf := make([]byte, accountingNameMaxWidth)
	userBuf := make([]byte, userNameMaxWidth)
	lenBuf := make([]byte, lengthEncodeLength)

	keyLen := timeStampLength + accountingNameMaxWidth + userNameMaxWidth + lengthEncodeLength
	longestDetails := 0

	for {
		fileKey := currentDay.UTC().Format(dateFormat)
		dbFilePath := filepath.Join(dbPath, fmt.Sprintf("%s.%s", fileKey, bom))
		fmt.Printf("%s\n", dbFilePath)

		f, err := os.Open(dbFilePath)
		if err != nil {
			return nil, err
		}

		r, _ := bzip2.NewReader(f, nil)
		br := bufio.NewReaderSize(r, flatDBReadSize)

		for {
			_, err = io.ReadFull(br, tsBuf)
			if err != nil {
				if !errors.Is(err, io.EOF) {
					fmt.Printf("a\n")
					return nil, err
				}

				break
			}

			if bytes.Compare(tsBuf, lteKey) > 0 {
				break
			}

			var passesFilter bool

			if bytes.Compare(tsBuf, gteKey) >= 0 {
				passesFilter = true
			}

			_, err = io.ReadFull(br, accBuf)
			if err != nil {
				fmt.Printf("b\n")
				return nil, err
			}

			if passesFilter && len(accountingName) > 0 && !bytes.Equal(accBuf, accountingName) {
				passesFilter = false
			}

			_, err = io.ReadFull(br, userBuf)
			if err != nil {
				fmt.Printf("c\n")
				return nil, err
			}

			if passesFilter && len(userName) > 0 && !bytes.Equal(userBuf, userName) {
				passesFilter = false
			}

			gpuByte, err := br.ReadByte()
			if err != nil {
				fmt.Printf("d\n")
				return nil, err
			}

			if passesFilter && checkGPU && gpuByte != inGPUQueue {
				passesFilter = false
			}

			_, err = io.ReadFull(br, lenBuf)
			if err != nil {
				fmt.Printf("e\n")
				return nil, err
			}

			detailsLength := btoi(lenBuf)

			if detailsLength > longestDetails {
				longestDetails = detailsLength
			}

			buf := make([]byte, detailsLength)

			_, err = io.ReadFull(br, buf)
			if err != nil {
				fmt.Printf("f %d\n", detailsLength)
				return nil, err
			}

			if passesFilter {
				dec := codec.NewDecoderBytes(buf, ch)

				var details *Details

				dec.MustDecode(&details)

				result.HitSet.Hits = append(result.HitSet.Hits, Hit{Details: *details})
				result.HitSet.Total.Value++
			}
		}

		r.Close()
		f.Close()

		currentDay = currentDay.Add(24 * time.Hour)
		if currentDay.After(lte) {
			break
		}
	}

	fmt.Printf("buffer should be %d long\n", keyLen+longestDetails)

	return result, nil
}

func searchBolt(db *bolt.DB, query *Query) (*Result, error) {
	lte, gte, err := queryToBoltPrefixRange(query)
	if err != nil {
		return nil, err
	}

	// lteStamp := time.Unix(btoi64(lte), 0).UTC().Format(time.RFC3339)

	bom, _ := queryToBom(query)

	accountingName, userName, checkGPU := queryToFilters(query)

	ch := new(codec.BincHandle)

	result := &Result{}

	err = db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucketName))

		i := 0

		b.ForEach(func(k, v []byte) error {
			dec := codec.NewDecoderBytes(v, ch)

			var details *Details

			dec.MustDecode(&details)

			result.HitSet.Hits = append(result.HitSet.Hits, Hit{Details: *details})
			result.HitSet.Total.Value++

			i++
			if i%10000 == 0 {
				fmt.Printf(".")
			}

			return nil
		})

		return nil

		c := tx.Bucket([]byte(bucketName)).Cursor()

		for k, v := c.Seek(gte); k != nil && bytes.Compare(k[0:endOfTimeStampIndex], lte) <= 0; k, v = c.Next() {
			if len(bom) > 0 && !bytes.Equal(k[startOfBomIndex:endOfBomIndex], bom) {
				continue
			}

			if len(accountingName) > 0 &&
				!bytes.Equal(k[startOfAccountingIndex:endOfAccountingIndex], accountingName) {
				continue
			}

			if len(userName) > 0 &&
				!bytes.Equal(k[startOfUserIndex:endOfUserIndex], userName) {
				continue
			}

			if checkGPU && k[isGPUIndex] != inGPUQueue {
				continue
			}

			dec := codec.NewDecoderBytes(v, ch)

			var details *Details

			dec.MustDecode(&details)

			result.HitSet.Hits = append(result.HitSet.Hits, Hit{Details: *details})
			result.HitSet.Total.Value++
		}

		return nil
	})

	return result, err
}

// queryToBoltPrefixRange extracts the timestamps from the query and converts
// them in to byte slices that would match what we stored in our bolt database.
func queryToBoltPrefixRange(query *Query) ([]byte, []byte, error) {
	lte, gte, err := parseRange(query.Query.Bool.Filter)
	if err != nil {
		return nil, nil, err
	}

	return i64tob(lte.Unix()), i64tob(gte.Unix()), nil
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

func queryToBom(query *Query) ([]byte, error) {
	for _, val := range query.Query.Bool.Filter {
		mp, ok := val["match_phrase"]
		if !ok {
			continue
		}

		bomStr := stringFromFilterValue(mp, "BOM")
		if bomStr == "" {
			continue
		}

		return fixedWidthBOM(bomStr)
	}

	return nil, errors.New("BOM not specified")
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
