package main

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/dgryski/go-farm"
	es "github.com/elastic/go-elasticsearch/v7"
	"github.com/elastic/go-elasticsearch/v7/esapi"
	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/ugorji/go/codec"
	bolt "go.etcd.io/bbolt"
	"gopkg.in/yaml.v3"
)

const (
	index      = "user-data-ssg-isg-lsf-analytics-*"
	maxSize    = 10000
	scrollTime = 1 * time.Minute
	cacheSize  = 128
	dbBasename = ".farmer.boltdb"
	bucketName = "hits"
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
	// ACCOUNTING_NAME                string
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

	db, err := openDB()
	if err != nil {
		log.Fatalf("%s\n", err)
	}

	defer db.Close()

	err = initDB(db, client)
	if err != nil {
		log.Fatalf("%s\n", err)
	}

	ch := new(codec.BincHandle)

	db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucketName))

		b.ForEach(func(k, v []byte) error {
			dec := codec.NewDecoderBytes(v, ch)

			var details *Details

			dec.MustDecode(&details)

			timestamp := time.Unix(btoi64(k[0:7]), 0).UTC().Format(time.RFC3339)

			bom := string(k[8:])

			fmt.Printf("%v.%s: %+v\n", timestamp, bom, details)
			return nil
		})

		return nil
	})

	return

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

	filter := Filter{
		{"match_phrase": map[string]interface{}{"META_CLUSTER_NAME": "farm"}},
		{"range": map[string]interface{}{
			"timestamp": map[string]string{
				"lte":    "2024-06-04T00:00:00Z",
				"gte":    "2024-05-04T00:00:00Z",
				"format": "strict_date_optional_time",
			},
		}},
		{"match_phrase": map[string]interface{}{"BOM": "Human Genetics"}},
		{"match_phrase": map[string]interface{}{"ACCOUNTING_NAME": "hgi"}},
	}

	t := time.Now()
	result, err := Scroll(l, db, client, index, filter)
	if err != nil {
		log.Fatalf("Error searching: %s", err)
	}

	if result == nil {
		return
	}

	if len(result.HitSet.Hits) > 0 {
		fmt.Printf("num hits: %+v\n", len(result.HitSet.Hits))
		fmt.Printf("first hit: %+v\n", result.HitSet.Hits[0])

		timestamp := result.HitSet.Hits[0].Details.Timestamp

		ut := time.Unix(timestamp, 0).UTC()

		fmt.Printf("%d; %s\n", timestamp, ut)

		var lte, gte time.Time

		for _, val := range filter {
			fRange, ok := val["range"]
			if ok {
				timestampInter, ok := fRange["timestamp"]
				if ok {
					timestamp, ok := timestampInter.(map[string]string)
					if ok {
						lte, err = time.Parse(time.RFC3339, timestamp["lte"])
						if err != nil {
							log.Fatalf("time parse error: %s", err)
							return
						}

						gte, err = time.Parse(time.RFC3339, timestamp["gte"])
						if err != nil {
							log.Fatalf("time parse error: %s", err)
							return
						}
					}
				}
			}
		}

		fmt.Printf("%v (%s) (%d)\n", i64tob(lte.Unix()), lte, lte.Unix())
		fmt.Printf("%v (%s)\n", i64tob(timestamp), ut)
		fmt.Printf("%v (%s)\n", i64tob(gte.Unix()), gte)
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

func openDB() (*bolt.DB, error) {
	db, err := bolt.Open(dbBasename, 0600, nil)
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

func initDB(db *bolt.DB, client *es.Client) error {
	filter := Filter{
		{"match_phrase": map[string]interface{}{"META_CLUSTER_NAME": "farm"}},
		{"range": map[string]interface{}{
			"timestamp": map[string]string{
				"lte": "2024-06-10T00:00:00Z",
				// "gte":    "2024-05-01T00:00:00Z",
				"gte":    "2024-06-09T23:50:00Z",
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

	err = storeInLocalDB(db, result)
	if err != nil {
		return err
	}

	fmt.Printf("store took: %s\n", time.Since(t))

	return nil
}

func storeInLocalDB(db *bolt.DB, result *Result) error {
	ch := new(codec.BincHandle)

	return db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucketName))
		if b == nil {
			return errors.New("bucket does not exist")
		}

		// boms := make(map[string]bool)

		fmt.Printf("storing %d hits\n", len(result.HitSet.Hits))

		for _, hit := range result.HitSet.Hits {
			// boms[hit.Details.BOM] = true

			key := i64tob(hit.Details.Timestamp)
			key = append(key, []byte(hit.Details.BOM)...)

			var encoded []byte
			enc := codec.NewEncoderBytes(&encoded, ch)
			enc.MustEncode(hit.Details)

			err := b.Put(key, encoded)
			if err != nil {
				return err
			}
		}

		// longest := 0
		// for bom := range boms {
		// 	fmt.Printf("%s\n", bom)

		// 	if len(bom) > longest {
		// 		longest = len(bom)
		// 	}
		// }
		// fmt.Printf("longest: %d\n", longest) 21? From an hour window

		return nil
	})
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

func Scroll(l *lru.Cache[string, *Result], db *bolt.DB, client *es.Client, index string, filter Filter) (*Result, error) {
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

	result, err = searchWithScroll(client, query)
	if err != nil {
		return nil, err
	}

	l.Add(cacheKey, result)

	return result, nil
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
