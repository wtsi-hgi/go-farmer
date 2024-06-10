package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/dgryski/go-farm"
	es "github.com/elastic/go-elasticsearch/v7"
	"github.com/elastic/go-elasticsearch/v7/esapi"
	lru "github.com/hashicorp/golang-lru/v2"
	"gopkg.in/yaml.v3"
)

const (
	index        = "user-data-ssg-isg-lsf-analytics-*"
	maxSize      = 10000
	maxSlices    = 2
	scrollTime   = 1 * time.Minute
	cacheSize    = 128
	keepAlive    = "1m"
	pitSortOrder = "_shard_doc"
)

type Query struct {
	Size           int          `json:"size"`
	PIT            *PIT         `json:"pit,omitempty"`
	Aggs           *Aggs        `json:"aggs,omitempty"`
	Query          *QueryFilter `json:"query,omitempty"`
	Sort           string       `json:"sort,omitempty"`
	SearchAfter    []int        `json:"search_after,omitempty"`
	TrackTotalHits bool         `json:"track_total_hits,omitempty"`
}

type QuerySlice struct {
	Field string `json:"field,omitempty"`
	ID    int    `json:"id"`
	Max   int    `json:"max"`
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

type PIT struct {
	ID        string `json:"id"`
	KeepAlive string `json:"keep_alive,omitempty"`
}

type Result struct {
	PitID        string `json:"pit_id"`
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
	Sort    []int   `json:"sort"`
}

type Details struct {
	// ACCOUNTING_NAME                string
	AVAIL_CPU_TIME_SEC int
	// AVG_MEM_EFFICIENCY_PERCENT     float64
	// AVRG_MEM_USAGE_MB              float64
	// AVRG_MEM_USAGE_MB_SEC_COOKED   float64
	// AVRG_MEM_USAGE_MB_SEC_RAW      float64
	// BOM                            string
	// CLUSTER_NAME                   string
	// COOKED_CPU_TIME_SEC            float64
	Command string
	// END_TIME                       int
	// EXEC_HOSTNAME                  []string
	// Exit_Info                      int
	// Exitreason                     string
	// JOB_ID                         int
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
	// timestamp                      int
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

	t := time.Now()

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

	t = time.Now()
	result, err := SearchAfter(l, client, index, filter)
	if err != nil {
		log.Fatalf("Error searching: %s", err)
	}

	if result == nil {
		return
	}

	if len(result.HitSet.Hits) > 0 {
		fmt.Printf("num hits: %+v\n", len(result.HitSet.Hits))
		fmt.Printf("first hit: %+v\n", result.HitSet.Hits[0])
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
	// result, err = SearchAfter(l, client, index, filter)
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
		return nil, fmt.Errorf("search failed: %s, %+v", resp.Status(), resp)
	}

	defer resp.Body.Close()

	// jsonHits, _ := json.Marshal(er.HitSet.Hits)
	// jsonHits, _ := json.Marshal(er.Aggregations.Stats.Buckets)
	// bodyBytes, _ := io.ReadAll(resp.Body)
	// jsonHits, _ := json.Marshal(string(bodyBytes))
	// fmt.Println(string(jsonHits))
	// bodyBytes, _ := io.ReadAll(resp.Body)
	// fmt.Println(string(bodyBytes))

	var result Result

	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}

	fmt.Printf("total: %d; hits: %d; aggs: %d\n",
		result.HitSet.Total.Value, len(result.HitSet.Hits), len(result.Aggregations.Stats.Buckets))

	return &result, nil
}

func SearchAfter(l *lru.Cache[string, *Result], client *es.Client, index string, filter Filter) (*Result, error) {
	query := &Query{Query: &QueryFilter{Bool: QFBool{Filter: filter}}}

	cacheKey, err := query.CacheKey()
	if err != nil {
		return nil, err
	}

	result, ok := l.Get(cacheKey)
	if ok {
		return result, nil
	}

	pit, err := getPointInTime(client)
	if err != nil {
		return nil, err
	}

	query = &Query{
		Size:           maxSize,
		Sort:           pitSortOrder,
		PIT:            pit,
		Query:          query.Query,
		TrackTotalHits: false,
	}

	result, err = searchAfter(client, query)
	if err != nil {
		return nil, err
	}

	l.Add(cacheKey, result)

	return result, nil
}

func getPointInTime(client *es.Client) (*PIT, error) {
	resp, err := client.OpenPointInTime([]string{index}, keepAlive)
	if err != nil {
		return nil, err
	}

	if resp.IsError() {
		return nil, fmt.Errorf("pit request failed: %s", resp.Status())
	}

	defer resp.Body.Close()

	var pit PIT

	if err := json.NewDecoder(resp.Body).Decode(&pit); err != nil {
		return nil, err
	}

	pit.KeepAlive = keepAlive

	return &pit, nil
}

func searchAfter(client *es.Client, query *Query) (*Result, error) {
	qbody, err := query.AsBody()
	if err != nil {
		return nil, err
	}

	resp, err := client.Search(
		client.Search.WithBody(qbody),
		client.Search.WithSize(maxSize),
	)
	if err != nil {
		return nil, err
	}

	result, err := parseResponse(resp)
	if err != nil {
		return nil, err
	}

	if len(result.HitSet.Hits) < maxSize {
		return result, nil
	}

	keepPaging := true

	for keepPaging {
		keepPaging, err = page(client, query, result)
		if err != nil {
			return nil, err
		}
	}

	return result, nil
}

func page(client *es.Client, query *Query, result *Result) (bool, error) {
	query.PIT.ID = result.PitID
	query.SearchAfter = result.HitSet.Hits[len(result.HitSet.Hits)-1].Sort

	qbody, err := query.AsBody()
	if err != nil {
		return false, err
	}

	resp, err := client.Search(
		client.Search.WithBody(qbody),
		client.Search.WithSize(maxSize),
	)
	if err != nil {
		return false, err
	}

	pageResult, err := parseResponse(resp)
	if err != nil {
		return false, err
	}

	result.PitID = pageResult.PitID
	result.HitSet.Hits = append(result.HitSet.Hits, pageResult.HitSet.Hits...)

	return len(pageResult.HitSet.Hits) == maxSize, nil
}
