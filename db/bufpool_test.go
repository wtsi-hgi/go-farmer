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

	. "github.com/smartystreets/goconvey/convey"
)

func TestBufPool(t *testing.T) {
	Convey("You can create new buffers in an empty pool", t, func() {
		pool := newBufPool()

		b := pool.get(10, "10")
		So(len(b), ShouldEqual, 10)
		So(len(pool.entries), ShouldEqual, 1)

		b = pool.get(20, "20")
		So(len(b), ShouldEqual, 20)
		So(len(pool.entries), ShouldEqual, 2)

		b = pool.get(15, "15")
		So(len(b), ShouldEqual, 15)
		So(len(pool.entries), ShouldEqual, 3)

		b = pool.get(10, "10")
		So(b, ShouldBeNil)
		So(len(pool.entries), ShouldEqual, 3)

		Convey("You can reuse done() buffers", func() {
			ok := pool.done("notexist")
			So(ok, ShouldBeFalse)

			ok = pool.done("20")
			So(ok, ShouldBeTrue)

			ok = pool.done("15")
			So(ok, ShouldBeTrue)

			b = pool.get(14, "14")
			So(len(b), ShouldEqual, 15)
			So(len(pool.entries), ShouldEqual, 3)

			ok = pool.done("10")
			So(ok, ShouldBeTrue)

			b = pool.get(10, "10")
			So(len(b), ShouldEqual, 10)
			So(len(pool.entries), ShouldEqual, 3)
		})
	})
}
