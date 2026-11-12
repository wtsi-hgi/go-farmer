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
- [ ] Backfill errors lack context: cron reported only
  `backfill failed: unexpected EOF`, with no day or stage (initial search,
  scroll page, body read) identified.
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
- [ ] `make race` with slow tests exceeds `go test`'s default 10m timeout
  (real-ES `TestBackfill` ~630s under `-race`); the target sets no `-timeout`.
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

## Known limitation (not fixed here)

- `Scroller` cannot be cancelled: after a Store failure the ES scroll keeps
  fetching pages (hits discarded) until exhausted. Fixing needs a context on
  `Scroller`/`es.Client.Scroll`. Only affects local Store failures, which are
  rare; ES failures already end the scroll.
