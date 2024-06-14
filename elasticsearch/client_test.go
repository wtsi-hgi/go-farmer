/*******************************************************************************
 * Copyright (c) 2024 Genome Research Ltd.
 *
 * Author: Sendu Bala <sb10@sanger.ac.uk>
 * Author: Iaroslav Popov <ip13@sanger.ac.uk>
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

package elasticsearch

import (
	"os"
	"strconv"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

const modePermUser = 0770

func TestElasticSearchClient(t *testing.T) {
	host := os.Getenv("FARMER_TEST_HOST")
	username := os.Getenv("FARMER_TEST_USERNAME")
	password := os.Getenv("FARMER_TEST_PASSWORD")
	scheme := os.Getenv("FARMER_TEST_SCHEME")
	portStr := os.Getenv("FARMER_TEST_PORT")

	if host == "" || username == "" || password == "" || scheme == "" || portStr == "" {
		SkipConvey("Skipping elasticsearch client test without FARMER_TEST_* env vars set", t, func() {})
		return
	}

	port, err := strconv.Atoi(portStr)
	if err != nil {
		SkipConvey("Skipping elasticsearch client test since FARMER_TEST_PORT was not a number", t, func() {})
		return
	}

	Convey("Given some config", t, func() {
		config := Config{
			Host:     host,
			Username: username,
			Password: password,
			Scheme:   scheme,
			Port:     port,
		}

		Convey("You can create an elasticsearch client", func() {
			client, err := NewClient(config)
			So(err, ShouldBeNil)
			So(client, ShouldNotBeNil)

			info, err := client.Info()
			So(err, ShouldBeNil)
			So(info.Version.Number, ShouldEqual, "7.17.6")
		})
	})
}
