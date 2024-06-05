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
	Took     int
	TimedOut bool   `json:"timed_out"`
	HitSet   HitSet `json:"hits"`
}

type HitSet struct {
	Hits  []Hit
	Total struct {
		Value int
	}
}

type Hit struct {
	ID      string      `json:"_id"`
	Details interface{} `json:"_source"`
}

type Details struct {
	ACCOUNTING_NAME                string
	AVAIL_CPU_TIME_SEC             int
	AVG_MEM_EFFICIENCY_PERCENT     float64
	AVRG_MEM_USAGE_MB              float64
	AVRG_MEM_USAGE_MB_SEC_COOKED   float64
	AVRG_MEM_USAGE_MB_SEC_RAW      float64
	BOM                            string
	CLUSTER_NAME                   string
	COOKED_CPU_TIME_SEC            float64
	Command                        string
	END_TIME                       int
	EXEC_HOSTNAME                  []string
	Exit_Info                      int
	Exitreason                     string
	JOB_EXIT_STATUS                int
	Job                            string
	Job_Efficiency_Percent         float64
	Job_Efficiency_Raw_Percent     float64
	MAX_MEM_EFFICIENCY_PERCENT     float64
	MAX_MEM_USAGE_MB               float64
	MAX_MEM_USAGE_MB_SEC_COOKED    float64
	MAX_MEM_USAGE_MB_SEC_RAW       float64
	MEM_REQUESTED_MB               int
	MEM_REQUESTED_MB_SEC           int
	NUM_EXEC_PROCS                 int
	NumberOfHosts                  int
	NumberOfUniqueHosts            int
	PENDING_TIME_SEC               int
	PROJECT_NAME                   string
	QUEUE_NAME                     string
	RAW_AVG_MEM_EFFICIENCY_PERCENT float64
	RAW_CPU_TIME_SEC               float64
	RAW_MAX_MEM_EFFICIENCY_PERCENT float64
	RAW_WASTED_CPU_SECONDS         float64
	RAW_WASTED_MB_SECONDS          float64
	RUN_TIME_SEC                   int
	USER_NAME                      string
	WASTED_CPU_SECONDS             float64
	WASTED_MB_SECONDS              float64
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
	query := `{"query":{"bool":{"filter":[{"match_phrase":{"META_CLUSTER_NAME":"farm"}},{"range":{"timestamp":{"lte":"2024-06-04T00:00:00Z","gte":"2024-05-04T00:00:00Z","format":"strict_date_optional_time"}}},{"match_phrase":{"BOM":"Human Genetics"}},{"match_phrase":{"ACCOUNTING_NAME":"hgi"}}]}}}`

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
	jsonHits, _ := json.Marshal(er.HitSet.Hits)
	fmt.Println(string(jsonHits))
}
