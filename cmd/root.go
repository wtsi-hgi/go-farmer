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

// package cmd is the cobra file that enables subcommands and handles
// command-line args.

package cmd

import (
	"fmt"
	"os"

	"github.com/inconshreveable/log15"
	"github.com/spf13/cobra"
)

// appLogger is used for logging events in our commands.
var appLogger = log15.New()

// global options.
var configPath string

// RootCmd represents the base command when called without any subcommands.
var RootCmd = &cobra.Command{
	Use:   "farmer",
	Short: "farmer enables faster elasticsearch queries for farmer reports",
	Long: `farmer enables faster elasticsearch queries for farmer reports.

Initially, you'll grab all past hits from real elasticsearch and store them in
a local database for fast queries later:

farmer backfill -c config.yml

Then you'll set up a cronjob to update the local database with the last day's
hits:

farmer update -c config.yml

Finally, you'll start a server that pretends to be like elasticsearch, and
configure the farmers report R to query this server instead of the real one:

farmer serve -c config.yml


All sub-commands take a config.yml file, which should be in this format:

elastic:
  host: "elasticsearch.domain.com"
  username: "public"
  password: "public"
  scheme: "http"
  port: 19200
farmer:
  host: "localhost"
  port: 19201
  index: "elasticsearchindex-*"
  database_dir: "/path/to/local/database_dir"
  file_size: 33554432
  buffer_size: 4194304
  cache_entries: 128

Where file and buffer size are in bytes. file_size determines the desired size
of local database files within database_dir, and buffer_size is the write and
read buffer size when creating/parsing those files. The default values for these
are given in the example above (32MB and 4MB respectively).

cache_entries is the number of query results that will be stored in an in-memory
LRU cache. Defaults to 128.

index will be the index supplied to the real elasticsearch when doing search and
scroll queries.
`,
}

// Execute adds all child commands to the root command and sets flags
// appropriately. This is called by main.main(). It only needs to happen once to
// the rootCmd.
func Execute() {
	if err := RootCmd.Execute(); err != nil {
		die(err.Error())
	}
}

func init() {
	// set up logging to stderr
	appLogger.SetHandler(log15.LvlFilterHandler(log15.LvlInfo, log15.StderrHandler))

	// global flags
	RootCmd.PersistentFlags().StringVarP(&configPath, "config", "c", "config.yml", "config file")
}

// cliPrint outputs the message to STDOUT.
func cliPrint(msg string, a ...interface{}) {
	fmt.Fprintf(os.Stdout, msg, a...)
}

// die is a convenience to log a message at the Error level and exit non zero.
func die(msg string, a ...interface{}) {
	appLogger.Error(fmt.Sprintf(msg, a...))
	os.Exit(1)
}
