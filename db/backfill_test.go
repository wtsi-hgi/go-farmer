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

package db

import (
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
)

func TestBackfill(t *testing.T) {
	Convey("You can convert durations to timestamp ranges", t, func() {
		from := time.Date(2024, 06, 1, 0, 30, 0, 0, time.UTC)

		gte, lte := timestampRange(from, 15*time.Minute)
		So(gte, ShouldEqual, "2024-05-31T23:45:00Z")
		So(lte, ShouldEqual, "2024-06-01T00:00:00Z")

		gte, lte = timestampRange(from, 2*time.Hour)
		So(gte, ShouldEqual, "2024-05-31T22:00:00Z")
		So(lte, ShouldEqual, "2024-06-01T00:00:00Z")

		gte, lte = timestampRange(from, 3*oneDay)
		So(gte, ShouldEqual, "2024-05-29T00:00:00Z")
		So(lte, ShouldEqual, "2024-06-01T00:00:00Z")

		gte, lte = timestampRange(from, 730*time.Hour)
		So(gte, ShouldEqual, "2024-05-01T00:00:00Z")
		So(lte, ShouldEqual, "2024-06-01T00:00:00Z")
	})

	Convey("Given a directory and duration, you can Backfill()", t, func() {

	})
}
