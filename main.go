package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"

	es "github.com/elastic/go-elasticsearch/v7"
	"gopkg.in/yaml.v3"
)

const index = "user-data-ssg-isg-lsf-analytics-*"

type Config struct {
	Elastic struct {
		Host     string
		Username string
		Password string
		Scheme   string
		Port     int
	}
}

type ElasticResponse struct {
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

	// query := `{ "query": { "match_all": {} } }`
	// query := `{"size":10000,"query":{"bool":{"filter":[{"match_phrase":{"META_CLUSTER_NAME":"farm"}},{"range":{"timestamp":{"lte":"2024-06-04T00:00:00Z","gte":"2024-05-04T00:00:00Z","format":"strict_date_optional_time"}}},{"match_phrase":{"BOM":"Human Genetics"}},{"match_phrase":{"ACCOUNTING_NAME":"hgi"}}]}}}`
	query := `{"aggs":{"stats":{"multi_terms":{"terms":[{"field":"ACCOUNTING_NAME"},{"field":"NUM_EXEC_PROCS"},{"field":"Job"}],"size":1000},"aggs":{"cpu_avail_sec":{"sum":{"field":"AVAIL_CPU_TIME_SEC"}},"cpu_wasted_sec":{"sum":{"field":"WASTED_CPU_SECONDS"}},"mem_avail_mb_sec":{"sum":{"field":"MEM_REQUESTED_MB_SEC"}},"mem_wasted_mb_sec":{"sum":{"field":"WASTED_MB_SECONDS"}},"wasted_cost":{"scripted_metric":{"init_script":"state.costs = []","map_script":"double cpu_cost = doc.WASTED_CPU_SECONDS.value * params.cpu_second; double mem_cost = doc.WASTED_MB_SECONDS.value * params.mb_second; state.costs.add(Math.max(cpu_cost, mem_cost))","combine_script":"double total = 0; for (t in state.costs) { total += t } return total","reduce_script":"double total = 0; for (a in states) { total += a } return total","params":{"cpu_second":7.0556e-07,"mb_second":5.8865e-11}}}}}},"size":0,"query":{"bool":{"filter":[{"match_phrase":{"META_CLUSTER_NAME":"farm"}},{"range":{"timestamp":{"lte":"2024-06-04T00:00:00Z","gte":"2024-05-04T00:00:00Z","format":"strict_date_optional_time"}}},{"match_phrase":{"BOM":"Human Genetics"}}]}}}`

	resp, err := client.Search(
		client.Search.WithIndex(index),
		client.Search.WithBody(strings.NewReader(query)),
	)
	if err != nil {
		log.Fatalf("Error doing search: %s", err)
	}

	if resp.IsError() {
		log.Fatalf("Error doing search: %s", resp.Status())
	}

	er := &ElasticResponse{}

	err = json.NewDecoder(resp.Body).Decode(&er)
	if err != nil {
		log.Fatalf("Error decoding search result: %s", err)
	}

	// fmt.Printf("took: %d\n", er.Took)
	fmt.Printf("hits: %d; hitset total: %d; aggs total: %d\n", len(er.HitSet.Hits), er.HitSet.Total.Value, len(er.Aggregations.Stats.Buckets))

	// jsonHits, _ := json.Marshal(er.HitSet.Hits)
	// jsonHits, _ := json.Marshal(er.Aggregations.Stats.Buckets)

	// bodyBytes, _ := io.ReadAll(resp.Body)
	// jsonHits, _ := json.Marshal(string(bodyBytes))

	// fmt.Println(string(jsonHits))
}
