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
	"cmp"
	"slices"
	"strconv"
	"sync"

	es "github.com/wtsi-hgi/go-farmer/elasticsearch"
)

const (
	bufPoolWarmupMuliplier = 0.8
)

type poolEntry struct {
	buf     *[]byte
	len     int
	inUseBy string
}

type bufPool struct {
	mu      sync.Mutex
	entries []*poolEntry
}

func newBufPool() *bufPool {
	return &bufPool{}
}

func (b *bufPool) warmup(numHits int) {
	if numHits <= 0 {
		return
	}

	lengthNeeded := numHits * es.MaxEncodedDetailsLength

	for lengthNeeded > es.MaxEncodedDetailsLength {
		key := strconv.Itoa(lengthNeeded)

		b.get(lengthNeeded, key)
		defer b.done(key)

		hits := lengthNeeded / es.MaxEncodedDetailsLength
		lengthNeeded = int(float64(hits)*bufPoolWarmupMuliplier) * es.MaxEncodedDetailsLength
	}
}

func (b *bufPool) get(lengthNeeded int, key string) []byte {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.keyInUse(key) {
		return nil
	}

	var assignedBuf []byte

	for _, pe := range b.entries {
		if pe.inUseBy != "" || pe.len < lengthNeeded {
			continue
		}

		pe.inUseBy = key
		assignedBuf = *pe.buf

		break
	}

	if assignedBuf == nil {
		buf := make([]byte, lengthNeeded)
		assignedBuf = buf

		b.entries = append(b.entries, &poolEntry{
			buf:     &buf,
			len:     lengthNeeded,
			inUseBy: key,
		})

		slices.SortFunc[[]*poolEntry, *poolEntry](b.entries, func(a, b *poolEntry) int {
			return cmp.Compare(a.len, b.len)
		})
	}

	return assignedBuf
}

func (b *bufPool) keyInUse(key string) bool {
	for _, pe := range b.entries {
		if pe.inUseBy == key {
			return true
		}
	}

	return false
}

func (b *bufPool) done(key string) bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	for _, pe := range b.entries {
		if pe.inUseBy == key {
			pe.inUseBy = ""

			return true
		}
	}

	return false
}
