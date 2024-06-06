package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	es "github.com/elastic/go-elasticsearch/v7"
	"github.com/elastic/go-elasticsearch/v7/esapi"
	"gopkg.in/yaml.v3"
)

const index = "user-data-ssg-isg-lsf-analytics-*"
const MaxSize = 10000
const scrollTime = 1 * time.Minute

const bomQuery = `{"aggs":{"stats":{"multi_terms":{"terms":[{"field":"ACCOUNTING_NAME"},{"field":"NUM_EXEC_PROCS"},{"field":"Job"}],"size":1000},"aggs":{"cpu_avail_sec":{"sum":{"field":"AVAIL_CPU_TIME_SEC"}},"cpu_wasted_sec":{"sum":{"field":"WASTED_CPU_SECONDS"}},"mem_avail_mb_sec":{"sum":{"field":"MEM_REQUESTED_MB_SEC"}},"mem_wasted_mb_sec":{"sum":{"field":"WASTED_MB_SECONDS"}},"wasted_cost":{"scripted_metric":{"init_script":"state.costs = []","map_script":"double cpu_cost = doc.WASTED_CPU_SECONDS.value * params.cpu_second; double mem_cost = doc.WASTED_MB_SECONDS.value * params.mb_second; state.costs.add(Math.max(cpu_cost, mem_cost))","combine_script":"double total = 0; for (t in state.costs) { total += t } return total","reduce_script":"double total = 0; for (a in states) { total += a } return total","params":{"cpu_second":7.0556e-07,"mb_second":5.8865e-11}}}}}},"size":0,"query":{"bool":{"filter":[{"match_phrase":{"META_CLUSTER_NAME":"farm"}},{"range":{"timestamp":{"lte":"2024-06-04T00:00:00Z","gte":"2024-05-04T00:00:00Z","format":"strict_date_optional_time"}}},{"match_phrase":{"BOM":"Human Genetics"}}]}}}`
const teamsQuery = `{"size":10000,"sort":["_doc"],"query":{"bool":{"filter":[{"match_phrase":{"META_CLUSTER_NAME":"farm"}},{"range":{"timestamp":{"lte":"2024-06-04T00:00:00Z","gte":"2024-05-04T00:00:00Z","format":"strict_date_optional_time"}}},{"match_phrase":{"BOM":"Human Genetics"}},{"match_phrase":{"ACCOUNTING_NAME":"hgi"}}]}}}`

// const teamsQuerySlice1 = `{"slice":{"id":0,"max":2},"size":10000,"query":{"bool":{"filter":[{"match_phrase":{"META_CLUSTER_NAME":"farm"}},{"range":{"timestamp":{"lte":"2024-06-04T00:00:00Z","gte":"2024-05-04T00:00:00Z","format":"strict_date_optional_time"}}},{"match_phrase":{"BOM":"Human Genetics"}},{"match_phrase":{"ACCOUNTING_NAME":"hgi"}}]}}}`
// const teamsQuerySlice2 = `{"slice":{"id":1,"max":2},"size":10000,"query":{"bool":{"filter":[{"match_phrase":{"META_CLUSTER_NAME":"farm"}},{"range":{"timestamp":{"lte":"2024-06-04T00:00:00Z","gte":"2024-05-04T00:00:00Z","format":"strict_date_optional_time"}}},{"match_phrase":{"BOM":"Human Genetics"}},{"match_phrase":{"ACCOUNTING_NAME":"hgi"}}]}}}`

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
	// BOM                            string
	// CLUSTER_NAME                   string
	// COOKED_CPU_TIME_SEC            float64
	// Command                        string
	// END_TIME                       int
	// EXEC_HOSTNAME                  []string
	// Exit_Info                      int
	// Exitreason                     string
	// JOB_EXIT_STATUS                int
	Job string
	// Job_Efficiency_Percent         float64
	// Job_Efficiency_Raw_Percent     float64
	// MAX_MEM_EFFICIENCY_PERCENT     float64
	// MAX_MEM_USAGE_MB               float64
	// MAX_MEM_USAGE_MB_SEC_COOKED    float64
	// MAX_MEM_USAGE_MB_SEC_RAW       float64
	MEM_REQUESTED_MB     int
	MEM_REQUESTED_MB_SEC int
	// NUM_EXEC_PROCS                 int
	// NumberOfHosts                  int
	// NumberOfUniqueHosts            int
	// PENDING_TIME_SEC               int
	// PROJECT_NAME                   string
	// QUEUE_NAME                     string
	// RAW_AVG_MEM_EFFICIENCY_PERCENT float64
	// RAW_CPU_TIME_SEC               float64
	// RAW_MAX_MEM_EFFICIENCY_PERCENT float64
	// RAW_WASTED_CPU_SECONDS         float64
	// RAW_WASTED_MB_SECONDS          float64
	// RUN_TIME_SEC                   int
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

	t := time.Now()
	result, err := Search(client, index, bomQuery)
	if err != nil {
		log.Fatalf("Error searching: %s", err)
	}

	if len(result.HitSet.Hits) > 0 {
		fmt.Printf("first hit: %+v\n", result.HitSet.Hits[0])
	}

	if len(result.Aggregations.Stats.Buckets) > 0 {
		fmt.Printf("first agg: %+v\n", result.Aggregations.Stats.Buckets[0])
	}
	fmt.Printf("took: %s\n\n", time.Since(t))

	t = time.Now()
	result, err = Scroll(client, index, teamsQuery)
	if err != nil {
		log.Fatalf("Error searching: %s", err)
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
	// result, err = Scroll(client, index, teamsQuerySlice1)
	// if err != nil {
	// 	log.Fatalf("Error searching: %s", err)
	// }

	// if len(result.HitSet.Hits) > 0 {
	// 	fmt.Printf("num hits: %+v\n", len(result.HitSet.Hits))
	// 	fmt.Printf("first hit: %+v\n", result.HitSet.Hits[0])
	// }

	// if len(result.Aggregations.Stats.Buckets) > 0 {
	// 	fmt.Printf("first agg: %+v\n", result.Aggregations.Stats.Buckets[0])
	// }
	// fmt.Printf("took: %s\n\n", time.Since(t))

	// t = time.Now()
	// result, err = Scroll(client, index, teamsQuerySlice2)
	// if err != nil {
	// 	log.Fatalf("Error searching: %s", err)
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

func Search(client *es.Client, index string, query string) (*Result, error) {
	resp, err := client.Search(
		client.Search.WithIndex(index),
		client.Search.WithBody(strings.NewReader(query)),
	)
	if err != nil {
		return nil, err
	}

	return parseResponse(resp)
}

func parseResponse(resp *esapi.Response) (*Result, error) {
	if resp.IsError() {
		return nil, fmt.Errorf("search failed: %s", resp.Status())
	}

	defer resp.Body.Close()

	// jsonHits, _ := json.Marshal(er.HitSet.Hits)
	// jsonHits, _ := json.Marshal(er.Aggregations.Stats.Buckets)
	// bodyBytes, _ := io.ReadAll(resp.Body)
	// jsonHits, _ := json.Marshal(string(bodyBytes))
	// fmt.Println(string(jsonHits))

	var result Result

	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}

	fmt.Printf("total: %d; hits: %d; aggs: %d\n", result.HitSet.Total.Value, len(result.HitSet.Hits), len(result.Aggregations.Stats.Buckets))

	return &result, nil
}

func Scroll(client *es.Client, index string, query string) (*Result, error) {
	resp, err := client.Search(
		client.Search.WithIndex(index),
		client.Search.WithBody(strings.NewReader(query)),
		client.Search.WithSize(MaxSize),
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
	if total <= MaxSize {
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
