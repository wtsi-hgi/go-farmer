package main

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/wtsi-hgi/go-farmer/cache"
	"github.com/wtsi-hgi/go-farmer/db"
	es "github.com/wtsi-hgi/go-farmer/elasticsearch"
	"gopkg.in/yaml.v3"
)

const (
	index             = "user-data-ssg-isg-lsf-analytics-*"
	maxSize           = 10000
	cacheSize         = 128
	bucketName        = "hits"
	dateFormat        = "2006/01/02"
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

	testPeriod = 1 // 1 is 10 mins, 2 is 3 days, otherwise over a month
	debug      = false
)

type YAMLConfig struct {
	Elastic struct {
		Host     string
		Username string
		Password string
		Scheme   string
		Port     int
	}
}

func main() {
	data, err := os.ReadFile(os.Args[1])
	if err != nil {
		log.Fatalf("error: %v", err)
	}

	c := YAMLConfig{}

	err = yaml.Unmarshal([]byte(data), &c)
	if err != nil {
		log.Fatalf("error: %v", err)
	}

	cfg := es.Config{
		Host:     c.Elastic.Host,
		Port:     c.Elastic.Port,
		Scheme:   c.Elastic.Scheme,
		Username: c.Elastic.Username,
		Password: c.Elastic.Password,
	}

	client, err := es.NewClient(cfg, index)
	if err != nil {
		log.Fatalf("%s\n", err)
	}

	cq, err := cache.New(client, client, cacheSize)
	if err != nil {
		log.Fatalf("%s\n", err)
	}

	dbDir := os.Args[2]

	dbExisted := false

	if _, err = os.Stat(dbDir); err == nil {
		dbExisted = true
	}

	ldb, err := db.New(dbDir, desiredFlatDBSize, fileBufferSize)
	if err != nil {
		log.Fatalf("%s\n", err)
	}

	defer func() {
		err = ldb.Close()
		if err != nil {
			log.Fatalf("%s\n", err)
		}
	}()

	if !dbExisted {
		err = initDB(dbDir, client, ldb)
		if err != nil {
			log.Fatalf("%s\n", err)
		}

		return
	}

	cq.Scroller = ldb

	t := time.Now()

	bomQuery := &es.Query{
		Aggs: &es.Aggs{
			Stats: es.AggsStats{
				MultiTerms: es.MultiTerms{
					Terms: []es.Field{
						{Field: "ACCOUNTING_NAME"},
						{Field: "NUM_EXEC_PROCS"},
						{Field: "Job"},
					},
					Size: 1000,
				},
				Aggs: map[string]es.AggsField{
					"cpu_avail_sec": {
						Sum: &es.Field{Field: "AVAIL_CPU_TIME_SEC"},
					},
					"cpu_wasted_sec": {
						Sum: &es.Field{Field: "WASTED_CPU_SECONDS"},
					},
					"mem_avail_mb_sec": {
						Sum: &es.Field{Field: "MEM_REQUESTED_MB_SEC"},
					},
					"mem_wasted_mb_sec": {
						Sum: &es.Field{Field: "WASTED_MB_SECONDS"},
					},
					"wasted_cost": {
						ScriptedMetric: &es.ScriptedMetric{
							InitScript:    "state.costs = []",
							MapScript:     "double cpu_cost = doc.WASTED_CPU_SECONDS.value * params.cpu_second; double mem_cost = doc.WASTED_MB_SECONDS.value * params.mb_second; state.costs.add(Math.max(cpu_cost, mem_cost))",
							CombineScript: "double total = 0; for (t in state.costs) { total += t } return total",
							ReduceScript:  "double total = 0; for (a in states) { total += a } return total",
							Params: map[string]float64{
								"cpu_second": 7.0556e-07,
								"mb_second":  5.8865e-11,
							},
						},
					},
				},
			},
		},
		Query: &es.QueryFilter{
			Bool: es.QFBool{
				Filter: es.Filter{
					{"match_phrase": map[string]interface{}{"META_CLUSTER_NAME": "farm"}},
					{"range": map[string]interface{}{
						"timestamp": map[string]string{
							"lte":    "2024-06-04T00:00:00Z",
							"gte":    "2024-05-04T00:00:00Z",
							"format": "strict_date_optional_time",
						},
					}},
					{"match_phrase": map[string]interface{}{"BOM": "Human Genetics"}},
				},
			},
		},
	}

	result, err := cq.Search(bomQuery)
	if err != nil {
		log.Fatalf("Error searching: %s", err)
	}

	if len(result.HitSet.Hits) > 0 {
		fmt.Printf("num hits: %+v\n", len(result.HitSet.Hits))
		// fmt.Printf("first hit: %+v\n", result.HitSet.Hits[0].Details)
	}

	if len(result.Aggregations.Stats.Buckets) > 0 {
		fmt.Printf("num aggs: %+v\n", len(result.Aggregations.Stats.Buckets))
		// fmt.Printf("first agg: %+v\n", result.Aggregations.Stats.Buckets[0])
	}
	fmt.Printf("took: %s\n\n", time.Since(t))

	lte := "2024-06-04T00:00:00Z"
	gte := "2024-05-04T00:00:00Z"
	if testPeriod == 1 {
		lte = "2024-06-09T23:55:00Z"
		gte = "2024-06-09T23:50:00Z"
	} else if testPeriod == 2 {
		gte = "2024-06-03T00:00:00Z"
	}

	teamQuery := &es.Query{
		Size: maxSize,
		Sort: []string{"_doc"},
		Query: &es.QueryFilter{Bool: es.QFBool{Filter: es.Filter{
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
		}}},
	}

	t = time.Now()
	result, err = cq.Scroll(teamQuery)
	if err != nil {
		log.Fatalf("Error searching: %s", err)
	}

	if result == nil {
		return
	}

	if len(result.HitSet.Hits) > 0 {
		fmt.Printf("num hits: %+v\n", len(result.HitSet.Hits))
		// fmt.Printf("first hit: %+v\n", result.HitSet.Hits[0].Details)
	}

	if len(result.Aggregations.Stats.Buckets) > 0 {
		fmt.Printf("num aggs: %+v\n", len(result.Aggregations.Stats.Buckets))
		// fmt.Printf("first agg: %+v\n", result.Aggregations.Stats.Buckets[0])
	}
	fmt.Printf("took: %s\n\n", time.Since(t))

	t = time.Now()
	result, err = cq.Search(bomQuery)
	if err != nil {
		log.Fatalf("Error searching: %s", err)
	}

	if len(result.HitSet.Hits) > 0 {
		fmt.Printf("num hits: %+v\n", len(result.HitSet.Hits))
		// fmt.Printf("first hit: %+v\n", result.HitSet.Hits[0].Details)
	}

	if len(result.Aggregations.Stats.Buckets) > 0 {
		fmt.Printf("num aggs: %+v\n", len(result.Aggregations.Stats.Buckets))
		// fmt.Printf("first agg: %+v\n", result.Aggregations.Stats.Buckets[0])
	}
	fmt.Printf("took: %s\n\n", time.Since(t))

	t = time.Now()
	result, err = cq.Scroll(teamQuery)
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
		fmt.Printf("num aggs: %+v\n", len(result.Aggregations.Stats.Buckets))
		// fmt.Printf("first agg: %+v\n", result.Aggregations.Stats.Buckets[0])
	}
	fmt.Printf("took: %s\n\n", time.Since(t))
}

func initDB(dbPath string, client *es.Client, db *db.DB) error {
	lte := "2024-06-10T00:00:00Z"
	gte := "2024-05-01T00:00:00Z"
	if testPeriod == 1 {
		gte = "2024-06-09T23:50:00Z"
	} else if testPeriod == 2 {
		lte = "2024-06-04T00:00:00Z"
		gte = "2024-06-02T00:00:00Z"
	}

	filter := es.Filter{
		{"match_phrase": map[string]interface{}{"META_CLUSTER_NAME": "farm"}},
		{"range": map[string]interface{}{
			"timestamp": map[string]string{
				"lte":    lte,
				"gte":    gte,
				"format": "strict_date_optional_time",
			},
		}},
	}

	query := &es.Query{
		Size:  maxSize,
		Sort:  []string{"timestamp", "_doc"},
		Query: &es.QueryFilter{Bool: es.QFBool{Filter: filter}},
	}

	t := time.Now()

	result, err := client.Scroll(query)
	if err != nil {
		return err
	}

	fmt.Printf("\nsearch took: %s\n", time.Since(t))
	t = time.Now()

	err = db.Store(result)
	if err != nil {
		return err
	}

	fmt.Printf("store took: %s\n", time.Since(t))

	return nil
}
