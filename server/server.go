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
	"log/slog"
	"net/http"
	"net/http/httputil"
	"net/url"

	es "github.com/wtsi-hgi/go-farmer/elasticsearch"
)

const (
	slash                = "/"
	scrollPage           = "scroll"
	getUsernamesEndpoint = "get_usernames"
)

// SearchScroller types have Search and Scroll functions for querying something
// like elastic search. The Scroll will automatically get all hits in a single
// scroll call. They return compressed JSON of the results.
type SearchScroller interface {
	Search(query *es.Query) ([]byte, error)
	Scroll(query *es.Query) ([]byte, error)
	Usernames(query *es.Query) ([]byte, error)
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
// the results of requested searches. Search requests are those sent to
// "/index/_search".
//
// It takes proxyTarget, which should be the URL of the real elasticsearch
// server, for which we will become a transparent proxy for all non-search
// requests. (Except for /_search/scroll requests, which are handled by
// returning some fixed results since we don't do real scolls.)
//
// To start a webserver, do something like:
//
//	s := New(sc, "index", &url.URL{Host: "domain:port", Scheme: "http"})
//	http.ListenAndServe(80, s)
func New(sc SearchScroller, index string, proxyTarget *url.URL) *Server {
	proxy := httputil.NewSingleHostReverseProxy(proxyTarget)

	mux := http.NewServeMux()
	s := &Server{
		mux: mux,
		sc:  sc,
	}

	mux.HandleFunc(slash+url.QueryEscape(index)+slash+es.SearchPage, s.search)
	mux.HandleFunc(slash+es.SearchPage+slash+scrollPage, s.fakeScroll)
	mux.HandleFunc(slash+getUsernamesEndpoint, s.usernames)
	mux.Handle(slash, proxy)

	return s
}

// ServeHTTP handles search requests using our SearchScroller. Everything else
// just returns OK.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mux.ServeHTTP(w, r)
}

func sendMessageToClient(w http.ResponseWriter, msg string) {
	if _, err := w.Write([]byte(msg)); err != nil {
		slog.Error("write to client failed", "err", err)
	}
}

// search handles /index/_search requests which are for aggregation queries, and
// also for ?scroll searches which we will auto-scroll without the use of the
// /_search/scroll endpoint.
func (s *Server) search(w http.ResponseWriter, r *http.Request) {
	query, ok := es.NewQuery(r)
	if !ok {
		w.WriteHeader(http.StatusBadRequest)

		return
	}

	jsonResult, ok := s.handleQuery(w, query)
	if !ok {
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	_, err := w.Write(jsonResult)
	if err != nil {
		slog.Error("write to client failed", "err", err)
	}
}

func (s *Server) handleQuery(w http.ResponseWriter, query *es.Query) ([]byte, bool) {
	var (
		jsonResult []byte
		err        error
	)

	if query.IsScroll() {
		jsonResult, err = s.sc.Scroll(query)
	} else {
		jsonResult, err = s.sc.Search(query)
	}

	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		sendMessageToClient(w, err.Error())

		return nil, false
	}

	return jsonResult, true
}

// fakeScroll handles unneeded requests to the /_search/scroll endpoint.
func (s *Server) fakeScroll(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	msg := ""

	if r.Method == http.MethodPost {
		msg = `{"_scroll_id":"farmer_scroll_id"}`
	} else if r.Method == http.MethodDelete {
		msg = `{"succeeded":true,"num_freed":0}`
	}

	sendMessageToClient(w, msg)
}

// usernames handles /get_usernames requests which are treated like scroll
// search requests, but we only return an array of unique usernames found in the
// result.
func (s *Server) usernames(w http.ResponseWriter, r *http.Request) {
	r.URL.Path = es.SearchPage

	query, ok := es.NewQuery(r)
	if !ok {
		w.WriteHeader(http.StatusBadRequest)

		return
	}

	jsonStrs, err := s.sc.Usernames(query)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		sendMessageToClient(w, err.Error())

		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	_, err = w.Write(jsonStrs)
	if err != nil {
		slog.Error("write to client failed", "err", err)
	}
}
