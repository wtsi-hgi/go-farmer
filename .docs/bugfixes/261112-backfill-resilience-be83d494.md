# Backfill resilience fixes

- Branch: `backfill-retry-f16a65a2`
- Worktree: `../go-farmer-backfill-retry`
- Base: `origin/develop` @ `5908506396002f1c1fafc3237b09c1b4ed9cb190`
- Queue owner: this branch / this checklist (deliver workflow)

## Delivery queue

| Branch | PR | Target | Deps | Head SHA | Status | Waits on | Next action |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `backfill-retry-f16a65a2` | - | `develop` | none | `5908506` | implementing | - | fix gate blockers, then items 2-3 |

## Items

- [x] If `ldb.Store` returns early with an error in `db/backfill.go`, the
  scroll goroutine blocks forever sending on `hitCh`.
  - Red: `CGO_ENABLED=1 go test -tags netgo --count 1 ./db -v -run TestBackfill`
    (exit 1) on unfixed code:
    `Expected: "Scroll returned"  Actual: "Scroll still blocked after 5s"`
  - Fixed in `db/backfill.go`, `db/db.go`, `db/backfill_test.go`: the hit
    callback selects on a `storeFailed` channel closed when Store fails, and
    the caller then drains `errCh` so the Scroll goroutine always exits;
    `Store` closes open flatDBs on error. Test asserts the error, that Scroll
    returns, no fd growth, and no leftover goroutine. Review PASS (cycle 3),
    all four fix components killed by mutants.
- [x] Backfill errors lack context: cron reported only
  `backfill failed: unexpected EOF`, with no day or stage (initial search,
  scroll page, body read) identified.
  - Red: `CGO_ENABLED=1 go test -tags netgo --count 1 ./db ./elasticsearch -run
    'TestBackfill|TestScrollErrorContext'` (exit 1), e.g. `Expected "scroll page 1
    after 1 of 23581 hits: read response body: unexpected EOF" Actual "unexpected EOF"`.
  - Fixed in `elasticsearch/result.go`, `elasticsearch/client.go`,
    `db/backfill.go` (+ tests `elasticsearch/client_errors_test.go`,
    `db/backfill_test.go`): `%w` wraps add `day <gte>..<lt>: {query
    elasticsearch|store locally}: {initial search|scroll page N after X of Y
    hits}: {read response body|decode response}:`. Review PASS, 16/17
    mutants killed (survivor: decode `%w`->`%v`, outside requirements).
- [ ] Backfill does not retry a failed day. Retry each failed day with
  backoff that quickly grows to one hour, to ride out long Elasticsearch
  indexing or availability problems.
  - Design (user direction 2026-11-12): backoff 1m doubling, capped at 1h
    (1, 2, 4, 8, 16, 32, 60, 60... minutes). Limit by retry count, not by
    clock time: max 11 retries (12 attempts), so a 04:00 cron run that keeps
    failing gives up at about 10:30, before 11:00.

## Gate blockers found during item 1 (pre-existing, fixed on this branch)

- [x] `elasticsearch/client_test.go:66` hard-codes ES version 7.17.6; the live
  test server reports 8.19.14, so `make test` fails when `FARMER_TEST_*` is set.
  - Red: `CGO_ENABLED=0 go test -tags netgo --count 1 ./elasticsearch` (exit 1):
    `Line 66: Expected: "7.17.6" Actual: "8.19.14"`.
  - Fixed in `elasticsearch/client_test.go`: `doClientTests` takes a version
    check; the mock asserts the exact mock version, the real server asserts a
    dotted version number. Review PASS; `make test` exit 0.
- [x] `.golangci.yml` is v1 format but the installed golangci-lint is v2.14.0,
  so `make lint` fails with "unsupported version of the configuration".
  - Red: `make lint` (exit 2): `can't load config: unsupported version of the
    configuration: ""`.
  - Fixed in `.golangci.yml` (migrated to v2, dropped settings restored or
    mapped: deadline->timeout, gomnd->mnd, tenv->usetesting; check-exported,
    govet.shadow key and exportloopref removed as v2 equivalents/defaults)
    and the Makefile install comment. `make lint` 0 issues. Review PASS.
- [x] `make race` with slow tests exceeds `go test`'s default 10m timeout
  (real-ES `TestBackfill` ~630s under `-race`); the target sets no `-timeout`.
  - Red: `make -n race` showed no `-timeout`, so go test's 10m default applied.
  - Fixed in `Makefile`: `testslow` and `race` pass `-timeout 30m`. Review PASS.
- [x] Flaky: db `TestBackfill` "Repeating Backfill() handles potentially
  corrupt prior days by starting them from scratch" fails its
  `infoRepeat.ModTime() ShouldHappenAfter infoOrig31.ModTime()` check when the
  two mtimes are equal (5/50 runs on clean HEAD `5908506`).
  - Red: `CGO_ENABLED=1 go test -tags netgo --count 50 ./db -run TestBackfill`
    (exit 1) at `cc2f840`: 15/50 failures at line 231, `Expected '...19.486538678'
    to happen after '...19.486538678' (it happened '0s' before)!`
  - Fixed in `db/backfill_test.go`: backdate the incomplete day's `0.index`
    by an hour with `os.Chtimes` before the repeat, then assert the new mtime
    is after that. 0/50 failures, `-race` 20/20; both `checkIfNeeded` mutants
    still killed. Review PASS.

## Found during item 2

- [x] `elasticsearch/result.go` `parseResultResponse` returns on an error
  status before `defer resp.Body.Close()`, so the body (and connection) leaks
  on every failed request; retries (item 3) would repeat the leak.
  - Not a leak as described: go-elasticsearch v7.17.10 `Response.String()`
    closes the body. Real leaks found instead in `elasticsearch/client.go`:
    `Info()` and `scrollCleanup` (ClearScroll response) never closed bodies.
  - Red: `CGO_ENABLED=1 go test -tags netgo --count 1 ./elasticsearch -v -run
    TestResponseBodiesAreClosed` (exit 1): 3 cases `Expected: 0 Actual: 1`
    unclosed bodies.
  - Fixed in `elasticsearch/client.go` (+ `client_body_close_test.go`,
    `client_errors_test.go` helper extraction). Review PASS (cycle 2), 4/4
    mutants killed incl. nil-resp guard in `scrollCleanup`.
- [ ] A multi-day backfill where several days fail reports only the first
  failure (errgroup), so the cron email hides other failed days. Address with
  item 3 by reporting every failed day.

## Known limitation (not fixed here)

- Errors from the clear-scroll call in `scrollCleanup` are stored in
  `Client.Error` and never read by backfill; a failed clear only leaves the
  scroll to expire after `scrollTime`.

- `Scroller` cannot be cancelled: after a Store failure the ES scroll keeps
  fetching pages (hits discarded) until exhausted. Fixing needs a context on
  `Scroller`/`es.Client.Scroll`. Only affects local Store failures, which are
  rare; ES failures already end the scroll.
