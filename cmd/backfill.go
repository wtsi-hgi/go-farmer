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
	"fmt"
	"os"
	"regexp"
	"strconv"
	"time"

	"github.com/spf13/cobra"
	"github.com/wtsi-hgi/go-farmer/db"
	es "github.com/wtsi-hgi/go-farmer/elasticsearch"
)

const (
	hoursInDay   = 24
	hoursInWeek  = hoursInDay * 7
	hoursInMonth = 730
)

var backfillPeriod string

var backfillCmd = &cobra.Command{
	Use:   "backfill",
	Short: "backfill local database",
	Long: `backfill local database.

Supply a -c config.yml (see root command help for details), and a --period to
backfill.

If the configured database_dir already exists, this will fail.

Otherwise, the directory will be created and a query run to get all hits from
the real configured elastic search from the previous midnight to the given
period long ago. The hits will then be stored in database files in the dir.
`,
	Run: func(_ *cobra.Command, _ []string) {
		config := ParseConfig()
		period := parsePeriod(backfillPeriod)
		backfill(config, period)
	},
}

func init() {
	RootCmd.AddCommand(backfillCmd)

	// flags specific to this sub-command
	backfillCmd.Flags().StringVarP(&backfillPeriod, "period", "p", "2m",
		"period of time to pull hits for, eg. 1h for 1 hour, 2d for 2 day, 3w for 3 weeks or 4m for 4 months")
}

func parsePeriod(periodStr string) time.Duration {
	durationRegex := regexp.MustCompile("[0-9]+[hdwm]")

	periodStr = durationRegex.ReplaceAllStringFunc(periodStr, func(d string) string {
		num, err := strconv.ParseInt(d[:len(d)-1], 10, 64)
		if err != nil {
			return d
		}

		switch d[len(d)-1] {
		case 'd':
			num *= hoursInDay
		case 'w':
			num *= hoursInWeek
		case 'm':
			num *= hoursInMonth
		}

		return strconv.FormatInt(num, 10) + "h"
	})

	d, err := time.ParseDuration(periodStr)
	if err != nil {
		die("invalid period: %s", err)
	}

	return d
}

func backfill(config *YAMLConfig, period time.Duration) {
	client, err := es.NewClient(config.ToESConfig())
	if err != nil {
		die("failed to create real elasticsearch client: %s", err)
	}

	err = realbackfill(client, config.ToDBConfig(), period)
	if err != nil {
		die("backfill failed: %s", err)
	}
}

func realbackfill(client *es.Client, config db.Config, period time.Duration) (err error) {
	gte, lte := timestampRange(period)

	if _, err = os.Stat(config.Directory); err == nil {
		err = fmt.Errorf("database directory [%s] already exists", config.Directory)

		return err
	}

	ldb, errn := db.New(config)
	if errn != nil {
		err = errn

		return err
	}

	defer func() {
		errc := ldb.Close()
		if err == nil {
			err = errc
		}
	}()

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

	result, err := client.Scroll(query)
	if err != nil {
		return err
	}

	cliPrint("search took: %s\n", time.Since(t))
	t = time.Now()

	err = ldb.Store(result)
	if err != nil {
		return err
	}

	cliPrint("store took: %s\n", time.Since(t))

	return err
}

func timestampRange(period time.Duration) (string, string) {
	y, m, d := time.Now().Date()
	end := time.Date(y, m, d, 0, 0, 0, 0, time.UTC)
	start := end.Add(-period)

	if period > hoursInDay*time.Hour {
		y, m, d := start.Date()
		start = time.Date(y, m, d, 0, 0, 0, 0, time.UTC)
	}

	return start.Format(time.RFC3339), end.Format(time.RFC3339)
}
