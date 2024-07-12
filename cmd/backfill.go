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
	hoursInYear  = 8760
)

var backfillPeriod string

var backfillCmd = &cobra.Command{
	Use:   "backfill",
	Short: "backfill local database",
	Long: `backfill local database.

Supply a -c config.yml (see root command help for details), and a --period to
backfill.

The configured directory will be created or updated by running a query to get
all hits from the real configured elastic search from the previous midnight to
the given period long ago. The hits will then be stored in database files in the
dir. Days already present in the database directory will be skipped, so it is
safe to eg. run:

farmer backfill -p 2d

every day, to cover yourself if it failed on one day for example, or you forgot
to run it.
`,
	Run: func(_ *cobra.Command, _ []string) {
		config := ParseConfig()
		period := parsePeriod(backfillPeriod)

		client, err := es.NewClient(config.ToESConfig())
		if err != nil {
			die("failed to create real elasticsearch client: %s", err)
		}

		err = db.Backfill(client, config.ToDBConfig(), time.Now(), period)
		if err != nil {
			die("backfill failed: %s", err)
		}
	},
}

func init() {
	RootCmd.AddCommand(backfillCmd)

	// flags specific to this sub-command
	backfillCmd.Flags().StringVarP(&backfillPeriod, "period", "p", "2m",
		"period of time to pull hits for, eg. 1h for 1 hour, 2d for 2 day, 3w for 3 weeks, 4m for 4 months and 5y for 5 years") //nolint:lll
}

func parsePeriod(periodStr string) time.Duration {
	durationRegex := regexp.MustCompile("[0-9]+[hdwmy]")

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
		case 'y':
			num *= hoursInYear
		}

		return strconv.FormatInt(num, 10) + "h"
	})

	d, err := time.ParseDuration(periodStr)
	if err != nil {
		die("invalid period: %s", err)
	}

	return d
}
