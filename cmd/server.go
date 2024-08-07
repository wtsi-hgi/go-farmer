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
	"log/slog"
	"os"
	"runtime"
	"runtime/pprof"
	"time"

	"github.com/spf13/cobra"
	"github.com/wtsi-hgi/go-farmer/cache"
	"github.com/wtsi-hgi/go-farmer/db"
	es "github.com/wtsi-hgi/go-farmer/elasticsearch"
	"github.com/wtsi-hgi/go-farmer/server"
	"gopkg.in/tylerb/graceful.v1"
)

const gracefulTimeout = 10 * time.Second

var (
	serverDebug bool
	serverPprof string
)

var serverCmd = &cobra.Command{
	Use:   "server",
	Short: "server",
	Long: `server.
 
Supply a -c config.yml (see root command help for details).
 
This will start a server listening on the configured farmer host:port, that you
should configure the R farmers report to look at.

This command will block forever in the foreground; you can background it with
ctrl-z; bg. Or better yet, use the daemonize program to daemonize this. To stop
the server gracefully, just send it a kill signal (ctrl-c).

Aggregation query results will come from an in-memory cached version of what the
configured real elastic server returns.

Scroll search query results will come from an in-memory cached version of what
the configured local database returns. That local database will check every hour
for any new files added by you running the backfill command.

All other requests will be served by the real elastic server, with this server
acting as a transparent proxy. (Except for /_search/scroll queries, which return
a fixed fake answer since we handle scrolls during search.)
`,
	Run: func(_ *cobra.Command, _ []string) {
		if serverDebug {
			slog.SetLogLoggerLevel(slog.LevelDebug)
		}

		config := ParseConfig()

		client, err := es.NewClient(config.ToESConfig())
		if err != nil {
			die("failed to create real elasticsearch client: %s", err)
		}

		info("loading local database indexes")
		t := time.Now()

		ldb, err := db.New(config.ToDBConfig(), true)
		if err != nil {
			die("failed to open local database: %s", err)
		}

		info("load took %s, server now ready", time.Since(t))

		defer func() {
			err = ldb.Close()
			if err != nil {
				die("failed to close local database: %s", err)
			}
		}()

		cq, err := cache.New(client, ldb, config.CacheEntries())
		if err != nil {
			die("failed to create an LRU cache: %s", err)
		}

		server := server.New(cq, config.Elastic.Index, config.ElasticURL())

		if serverPprof != "" {
			fCPU, err := os.Create(serverPprof + ".cpu")
			if err != nil {
				die("failed to create pprof output file: %s", err)
			}

			fMem, err := os.Create(serverPprof + ".mem")
			if err != nil {
				die("failed to create pprof output file: %s", err)
			}

			defer fCPU.Close()
			defer fMem.Close()

			runtime.GC()
			pprof.StartCPUProfile(fCPU) //nolint:errcheck

			defer func() {
				pprof.StopCPUProfile()
				runtime.GC()
				pprof.WriteHeapProfile(fMem) //nolint:errcheck
			}()
		}

		graceful.Run(config.FarmerHostPort(), gracefulTimeout, server)
	},
}

func init() {
	RootCmd.AddCommand(serverCmd)

	serverCmd.Flags().BoolVarP(&serverDebug, "debug", "d", false,
		"output additional debug info")
	serverCmd.Flags().StringVarP(&serverPprof, "pprof", "p", "",
		"output profiling data to files with the given prefix path")
}
