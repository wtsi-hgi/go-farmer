/*******************************************************************************
 * Copyright (c) 2024 Genome Research Ltd.
 *
 * Author: Sendu Bala <sb10@sanger.ac.uk>
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

package cmd

import (
	"os"
	"time"

	"github.com/spf13/cobra"
	"github.com/wtsi-hgi/go-farmer/cache"
	"github.com/wtsi-hgi/go-farmer/db"
	es "github.com/wtsi-hgi/go-farmer/elasticsearch"
)

var demoPeriod string
var demoDebug bool

var demoCmd = &cobra.Command{
	Use:   "demo",
	Short: "demo for testing only",
	Long: `demo for testing only.

Supply a -c config.yml (see root command help for details).

If the configured database_dir doesn't exist, a test query will be run to get
hits from elasticsearch and store them in the local database.

If it exists, test queries will run to show performance of using the local
database and in-memory caching.

Optionally supply -p month to query 1 month of data, or -p days for 3 days.
Default is -p mins for 10 minutes of data.
`,
	Run: func(_ *cobra.Command, _ []string) {
		config := ParseConfig()

		period := 0
		switch demoPeriod {
		case "mins":
			period = 1
		case "days":
			period = 2
		case "month":
			period = 3
		default:
			die("invalid period supplied")
		}

		demo(config, period, demoDebug)
	},
}

func init() {
	RootCmd.AddCommand(demoCmd)

	// flags specific to this sub-command
	demoCmd.Flags().StringVarP(&demoPeriod, "period", "p", "mins",
		"period of time to pull results for; mins for 10mins, days for 3 days, month for a month")
	demoCmd.Flags().BoolVarP(&demoDebug, "debug", "d", false,
		"output additional debug info")
}

func demo(config *YAMLConfig, period int, debug bool) {
	client, err := es.NewClient(config.ToESConfig(), config.Farmer.Index)
	if err != nil {
		die("failed to create real elasticsearch client: %s", err)
	}

	cq, err := cache.New(client, client, config.CacheEntries())
	if err != nil {
		die("failed to create an LRU cache: %s", err)
	}

	dbDir := config.Farmer.DatabaseDir

	dbExisted := false

	if _, err = os.Stat(dbDir); err == nil {
		dbExisted = true
	}

	ldb, err := db.New(dbDir, config.FileSize(), config.BufferSize())
	if err != nil {
		die("failed to open local database: %s", err)
	}

	defer func() {
		err = ldb.Close()
		if err != nil {
			die("failed to close local database: %s", err)
		}
	}()

	if !dbExisted {
		err = initDB(cq, ldb, period)
		if err != nil {
			die("failed to create local database: %s", err)
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
							MapScript:     "double cpu_cost = doc.WASTED_CPU_SECONDS.value * params.cpu_second; double mem_cost = doc.WASTED_MB_SECONDS.value * params.mb_second; state.costs.add(Math.max(cpu_cost, mem_cost))", //nolint:lll
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
		die("error searching: %s", err)
	}

	if len(result.HitSet.Hits) > 0 {
		cliPrint("num hits: %+v\n", len(result.HitSet.Hits))
	}

	if len(result.Aggregations.Stats.Buckets) > 0 {
		cliPrint("num aggs: %+v\n", len(result.Aggregations.Stats.Buckets))
	}

	cliPrint("took: %s\n\n", time.Since(t))

	lte := "2024-06-04T00:00:00Z"
	gte := "2024-05-04T00:00:00Z"

	if period == 1 {
		lte = "2024-06-09T23:55:00Z"
		gte = "2024-06-09T23:50:00Z"
	} else if period == 2 {
		gte = "2024-06-03T00:00:00Z"
	}

	teamQuery := &es.Query{
		Size: es.MaxSize,
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
		die("error searching: %s", err)
	}

	if result == nil {
		return
	}

	if len(result.HitSet.Hits) > 0 {
		cliPrint("num hits: %+v\n", len(result.HitSet.Hits))
	}

	if len(result.Aggregations.Stats.Buckets) > 0 {
		cliPrint("num aggs: %+v\n", len(result.Aggregations.Stats.Buckets))
	}

	cliPrint("took: %s\n\n", time.Since(t))

	t = time.Now()

	result, err = cq.Search(bomQuery)
	if err != nil {
		die("error searching: %s", err)
	}

	if len(result.HitSet.Hits) > 0 {
		cliPrint("num hits: %+v\n", len(result.HitSet.Hits))
	}

	if len(result.Aggregations.Stats.Buckets) > 0 {
		cliPrint("num aggs: %+v\n", len(result.Aggregations.Stats.Buckets))
	}

	cliPrint("took: %s\n\n", time.Since(t))

	t = time.Now()

	result, err = cq.Scroll(teamQuery)
	if err != nil {
		die("error searching: %s", err)
	}

	if result == nil {
		return
	}

	if len(result.HitSet.Hits) > 0 {
		cliPrint("num hits: %+v\n", len(result.HitSet.Hits))

		if debug {
			cliPrint("first hit: %+v\n", result.HitSet.Hits[0].Details)
		}
	}

	if len(result.Aggregations.Stats.Buckets) > 0 {
		cliPrint("num aggs: %+v\n", len(result.Aggregations.Stats.Buckets))
	}

	cliPrint("took: %s\n\n", time.Since(t))
}

func initDB(cq *cache.CachedQuerier, db *db.DB, period int) error {
	lte := "2024-06-10T00:00:00Z"
	gte := "2024-05-01T00:00:00Z"

	if period == 1 {
		gte = "2024-06-09T23:50:00Z"
	} else if period == 2 {
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
		Size:  es.MaxSize,
		Sort:  []string{"timestamp", "_doc"},
		Query: &es.QueryFilter{Bool: es.QFBool{Filter: filter}},
	}

	t := time.Now()

	result, err := cq.Scroll(query)
	if err != nil {
		return err
	}

	cliPrint("search took: %s\n", time.Since(t))
	t = time.Now()

	err = db.Store(result)
	if err != nil {
		return err
	}

	cliPrint("store took: %s\n", time.Since(t))

	return nil
}
