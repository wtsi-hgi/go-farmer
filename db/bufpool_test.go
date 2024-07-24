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
	es "github.com/wtsi-hgi/go-farmer/elasticsearch"
)

func TestBufPool(t *testing.T) {
	Convey("You can create new buffers in an empty pool", t, func() {
		pool := newBufPool()

		b, key10 := pool.Get(10)
		So(len(b), ShouldEqual, 10)
		So(key10, ShouldNotEqual, 0)
		So(len(pool.entries), ShouldEqual, 1)

		b, key20 := pool.Get(20)
		So(len(b), ShouldEqual, 20)
		So(len(pool.entries), ShouldEqual, 2)

		b, key15 := pool.Get(15)
		So(len(b), ShouldEqual, 15)
		So(len(pool.entries), ShouldEqual, 3)

		b, key10b := pool.Get(10)
		So(len(b), ShouldEqual, 10)
		So(key10b, ShouldNotEqual, key10)
		So(len(pool.entries), ShouldEqual, 4)

		Convey("You can reuse done() buffers", func() {
			ok := pool.Done(-1)
			So(ok, ShouldBeFalse)

			ok = pool.Done(key20)
			So(ok, ShouldBeTrue)

			ok = pool.Done(key15)
			So(ok, ShouldBeTrue)

			b, _ = pool.Get(14)
			So(len(b), ShouldEqual, 15)
			So(len(pool.entries), ShouldEqual, 4)

			ok = pool.Done(key10)
			So(ok, ShouldBeTrue)

			b, _ = pool.Get(10)
			So(len(b), ShouldEqual, 10)
			So(len(pool.entries), ShouldEqual, 4)

			b, key20b := pool.Get(20)
			So(len(b), ShouldEqual, 20)
			So(len(pool.entries), ShouldEqual, 4)

			ok = pool.Done(key20b)
			So(ok, ShouldBeTrue)

			b, _ = pool.Get(20)
			So(len(b), ShouldEqual, 20)
			So(len(pool.entries), ShouldEqual, 4)
		})
	})

	Convey("You can Warmup a pool", t, func() {
		pool := newBufPool()
		So(len(pool.entries), ShouldEqual, 0)

		pool.Warmup(10)
		So(len(pool.entries), ShouldEqual, 6)
		So(pool.entries[0].len, ShouldEqual, es.MaxEncodedDetailsLength*2)
		So(pool.entries[1].len, ShouldEqual, es.MaxEncodedDetailsLength*3)
		So(pool.entries[2].len, ShouldEqual, es.MaxEncodedDetailsLength*4)
		So(pool.entries[3].len, ShouldEqual, es.MaxEncodedDetailsLength*6)
		So(pool.entries[4].len, ShouldEqual, es.MaxEncodedDetailsLength*8)
		So(pool.entries[5].len, ShouldEqual, es.MaxEncodedDetailsLength*10)

		for _, entry := range pool.entries {
			So(entry.inUse, ShouldBeFalse)
		}
	})
}
