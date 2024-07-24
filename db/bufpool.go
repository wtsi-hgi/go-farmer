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
	"sort"
	"strconv"
	"sync"

	es "github.com/wtsi-hgi/go-farmer/elasticsearch"
)

const (
	bufPoolWarmupMuliplier = 0.8
)

type poolEntry struct {
	buf   *[]byte
	len   int
	index int
	inUse bool
}

// bufPool holds a permanent pool of buffers of mixed size and is able to return
// an existing unused one that is closest in size to a desired buffer size.
type bufPool struct {
	mu         sync.Mutex
	entries    []*poolEntry
	keyToIndex map[string]int
}

func newBufPool() *bufPool {
	return &bufPool{
		keyToIndex: map[string]int{},
	}
}

// Warmup populates the pool with buffers large enough to handle the given
// number of hits, plus a sequence of smaller ones all the way down to one big
// enough for 2 hits.
func (b *bufPool) Warmup(numHits int) {
	if numHits <= 0 {
		return
	}

	lengthNeeded := numHits * es.MaxEncodedDetailsLength

	for lengthNeeded > es.MaxEncodedDetailsLength {
		key := strconv.Itoa(lengthNeeded)

		b.Get(lengthNeeded, key)
		defer b.Done(key)

		hits := lengthNeeded / es.MaxEncodedDetailsLength
		lengthNeeded = int(float64(hits)*bufPoolWarmupMuliplier) * es.MaxEncodedDetailsLength
	}
}

// Get returns the smallest buffer not in use from the pool that is at least
// lengthNeeded long. If none exist, one of the given size is created in the
// pool.
//
// The buf is associated with the given key, which you must then pass to Done()
// when you have finished all reading and writing from the buf, to release it
// back to the pool.
func (b *bufPool) Get(lengthNeeded int, key string) []byte {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, exists := b.keyToIndex[key]; exists {
		return nil
	}

	assignedBuf := b.getExisting(lengthNeeded, key)

	if assignedBuf == nil {
		assignedBuf = b.makeNewBuf(lengthNeeded, key)
	}

	return assignedBuf
}

func (b *bufPool) getExisting(lengthNeeded int, key string) []byte {
	var assignedBuf []byte

	for _, pe := range b.entries {
		if pe.inUse || pe.len < lengthNeeded {
			continue
		}

		pe.inUse = true
		b.keyToIndex[key] = pe.index
		assignedBuf = *pe.buf

		break
	}

	return assignedBuf
}

func (b *bufPool) makeNewBuf(lengthNeeded int, key string) []byte {
	buf := make([]byte, lengthNeeded)

	b.insertSorted(&poolEntry{
		buf:   &buf,
		len:   lengthNeeded,
		inUse: true,
	}, key)

	return buf
}

func (b *bufPool) insertSorted(pe *poolEntry, key string) {
	i := sort.Search(len(b.entries), func(i int) bool { return b.entries[i].len > pe.len })
	b.entries = append(b.entries, nil)
	copy(b.entries[i+1:], b.entries[i:])
	b.entries[i] = pe
	pe.index = i

	for j := i + 1; j < len(b.entries); j++ {
		b.entries[j].index++
	}

	for key, index := range b.keyToIndex {
		if index >= i {
			b.keyToIndex[key]++
		}
	}

	b.keyToIndex[key] = i
}

// Done releases the buffer you previously got from Get() with the same key.
// Returns true if the key was known about and the buffer was released.
func (b *bufPool) Done(key string) bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	index, found := b.keyToIndex[key]
	if !found {
		return false
	}

	b.entries[index].inUse = false
	delete(b.keyToIndex, key)

	return true
}
