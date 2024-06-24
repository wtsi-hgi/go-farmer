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

package server

import (
	"encoding/json"
	"log/slog"
	"net/http"

	es "github.com/wtsi-hgi/go-farmer/elasticsearch"
)

const root = "/"

// SearchScroller types have Search and Scroll functions for querying something
// like elastic search. The Scroll will automatically get all hits in a single
// scroll call.
type SearchScroller interface {
	Search(query *es.Query) (*es.Result, error)
	Scroll(query *es.Query) (*es.Result, error)
}

// Server is a http.Handler that pretends to be like an elastic search server,
// but only handles what is required for the farmer's report.
type Server struct {
	mux http.Handler
	sc  SearchScroller
}

// New returns a Server, which is an http.Handler.
//
// It takes SearchScroller, such as a CachedQuerier, which will be used to get
// the results of requested searches.
//
// To start a webserver, do something like:
//
//	s := New()
//	http.ListenAndServe(80, s)
func New(sc SearchScroller) *Server {
	mux := http.NewServeMux()
	s := &Server{
		mux: mux,
		sc:  sc,
	}

	mux.HandleFunc(root+es.SearchPage, s.search)
	mux.HandleFunc(root, s.nonSearch)

	return s
}

// ServeHTTP handles search requests using real elasticsearch or our local
// database for scroll searches. Everything else just returns OK.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mux.ServeHTTP(w, r)
}

func (s *Server) nonSearch(w http.ResponseWriter, _ *http.Request) {
	sendMessageToClient(w, "non-search request ignored")
}

func sendMessageToClient(w http.ResponseWriter, msg string) {
	if _, err := w.Write([]byte(msg)); err != nil {
		slog.Error("write to client failed", "err", err)
	}
}

func (s *Server) search(w http.ResponseWriter, r *http.Request) {
	query, ok := es.NewQuery(r)
	if !ok {
		w.WriteHeader(http.StatusBadRequest)

		return
	}

	result, ok := s.handleQuery(w, query)
	if !ok {
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	if err := json.NewEncoder(w).Encode(result); err != nil {
		slog.Error("failed to encode result as JSON", "err", err)
	}
}

func (s *Server) handleQuery(w http.ResponseWriter, query *es.Query) (*es.Result, bool) {
	var (
		result *es.Result
		err    error
	)

	if query.IsScroll() {
		result, err = s.sc.Scroll(query)
	} else {
		result, err = s.sc.Search(query)
	}

	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		sendMessageToClient(w, err.Error())

		return nil, false
	}

	return result, true
}
