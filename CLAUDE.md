# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## This is a fork

Gearlay's fork of `josdejong/workerpool` (remote `upstream`, published under the original name). `README.md` and `HISTORY.md` are upstream's and do **not** document anything the fork added, so treat them as reference for the original API only.

> **Name the repo on every `gh` command that opens something.** In a fork, `gh` resolves the default repo to the upstream **parent**, so a bare `gh pr create` opens a pull request against the public `josdejong/workerpool` and publishes this fork's whole diff there. Always:
>
> ```bash
> gh pr create --repo Gearlay/workerpool --base master --head <branch>
> ```
>
> then check that the URL it prints is a `Gearlay/workerpool` one. This has already happened once, and it cannot be undone. A fork shares one object store with its parent and GitHub pins the commits at `refs/pull/<n>/head`, so the diff stays readable at an upstream URL after the PR is closed, after its body is redacted, after a force push, and after the branch is deleted. Only GitHub Support can remove a pull request record. Treat opening a PR here as publishing to the internet.

The fork follows upstream by merging upstream release tags into `master` (last: `v10.0.3`). `git diff v10.0.3 -- src test` shows exactly what the fork adds. Keep the `upstream` remote's push URL disabled.

MaestroBot installs the fork as a git dependency pinned to a commit, and npm never builds a git dependency here, so everything `package.json` points at must be committed: `src/` is loaded directly, and the generated `types/` is committed on purpose (upstream ignores it). After changing any JSDoc in `src/`, run `npm run build:types` and commit `types/`.

Fork-only additions:

| Option / API | Effect |
|-|-|
| `concurrency` | Tasks a single worker may process at once (`busy()` threshold, default 1) |
| `roundrobin` | Pick workers round-robin instead of first-available |
| `affinity` (an `exec` option) | Pin a task to `workers[affinity % workers.length]`, trumps roundrobin |
| `maxExec` | Retire a worker after N executions |
| `markNotReadyAfterExec` | Worker becomes not-ready after each exec and must call `workerReady()` again |
| `readyTimeoutDuration`, `initReadyTimeoutDuration` | Terminate a worker that fails to signal ready (init one covers first startup) |
| `gradualScaling` | Minimum ms between new worker creations, avoids spawn storms |
| `pool.wstats()` | Timing plus event-loop-utilization stats across live workers, avg/min/max/total for both the interval since the last call (`avgUtil`) and each worker's whole life (`avgLifetimeUtil`), backed by `WorkerHandler.utilization()` |
| `stats().availableWorkers`, `getNumberAvailableWorkers()` | Count from `available()` rather than `busy()` |
| `peek()` on the built-in queues | Lets `_next()` read the next task's `affinity` before choosing a worker; a custom `queueStrategy` without `peek` gets no affinity |
| Worker replacement | Every worker that exits, whether it crashed, was retired or was terminated with its pool, is removed and replaced up to `minWorkers` at once. Intended: a pool with `minWorkers` never stays below it, even after `terminate()` |

## Commands

```bash
npm test                          # rollup build, mocha, then the type tests
npx mocha test/Pool.test.js       # single file, skips the build (.mocharc.js sets a 10s timeout)
npx mocha test -g "supports auto" # single test by name
npm run build                     # rollup bundles into dist/ plus tsc into types/
npm run build:types               # types/ only
npm run test:types                # tsc against test/types
```

Install with `npm ci --ignore-scripts`. Tests require the real thing: they fork processes and worker threads, so failures are often timing- or platform-related rather than logic bugs. `WorkerHandler.test.js` also shells out via `find-process` to assert orphaned processes are gone.

## Running the tests

The full suite completes: **185 passing, 2 pending** on node 26. `.mocharc.js` sets `exit: true`, since pools with `minWorkers` bring their workers back after `terminate()` and those keep the process alive. Two child-process tests are flaky upstream too (`should trigger event stdout in abort handler`, `should trigger event in abort handler`); pristine `v10.0.3` failed them in 2 of 3 runs. An `ERR_IPC_CHANNEL_CLOSED` printed after the summary is also upstream's: a child process from the last `WorkerHandler` test outlives the run.

The fork's own behavior is covered under `describe('fork scheduling options')` and `describe('wstats')` in `test/Pool.test.js`.

## Architecture

Four layers, each with one job:

- `src/index.js` is the public facade (`pool`, `worker`, `workerEmit`, `workerReady`, `Promise`, `Transfer`, `platform`, `isMainThread`, `cpus`). It lazily requires `Pool` and `worker` so importing in a worker context does not pull in the pool.
- `src/Pool.js` owns the task queue (`taskQueue`, FIFO by default, see `queues.js`) and scheduling. `exec()` pushes a task, `_next()` pops one when `_getWorker()` returns a worker, and completion re-enters `_next()`.
- `src/WorkerHandler.js` owns a single worker: transport setup, the request/response bookkeeping (`processing` keyed by id, `tracking` for tasks being cleaned up after a timeout or cancel), readiness, timing stats, and termination.
- `src/worker.js` runs *inside* the worker. It registers methods, dispatches incoming RPC, runs abort listeners on cleanup, and sends results back.

### Transports

`setupWorker()` resolves `workerType` (`auto` | `web` | `thread` | `process`) and then shims whatever it gets so it looks like a `child_process`: everything gets `send`, `on`, and `kill`. Code above `setupWorker` never branches on transport again. `auto` prefers `worker_threads` on node and falls back to child processes. `forkOpts`, `workerThreadOpts` and `workerOpts` are checked against allow lists in `validateOptions.js` right before a worker is created; an unknown key throws.

### Wire protocol

Plain JSON-RPC-ish messages over `send`/`on('message')`:

- request: `{ id, method, params }`
- response: `{ id, result, error }` (errors serialized by `convertError`, revived by `objectToError`)
- intermediate event: `{ id, isEvent: true, payload }`, delivered to the task's `options.on` callback
- the bare string `"ready"` signals worker readiness
- the bare string `__workerpool-terminate__` asks the worker to exit
- `{ id, method: '__workerpool-cleanup__' }` asks the worker to run its abort listeners for a timed-out or cancelled task

Those constants are declared separately in both `worker.js` and `WorkerHandler.js`. Change one, change both.

`convertError` passes a thrown string through, uses `toJSON` when the error has one, and otherwise copies the error's own non-function properties, so structured cloning keeps bigints and cycles. Upstream uses a JSON round trip, which throws on both; the fork keeps the copy because a throw there leaves the task without a response.

### Readiness

`worker.ready` gates sending. Requests that arrive early sit in `requestQueue` and flush on `"ready"`, before `onWorkerReady()` (which is `Pool._next()`) runs. A scripted worker signals readiness as a `"ready"` message. A script-less worker is ready at once, and its `onWorkerReady()` is deferred with `setTimeout(..., 0)`: called synchronously it ran before the pool had listed the handler, so `_getWorker()` kept creating workers until the stack gave out. The init ready timeout is armed only for a worker that is not ready yet. With `markNotReadyAfterExec`, the worker script is responsible for calling `workerpool.workerReady()` after each task, and `readyTimeoutDuration` is the safety net that kills a worker which never does.

### Worker selection

`Pool._getWorker(affinity)` in priority order: affinity, then roundrobin, then the first worker whose `available()` is true. Affinity and roundrobin pick a worker even when it is busy or not ready yet. After that it creates a new worker if under `maxWorkers` (rate-limited by `gradualScaling`), and uses it only if nothing was picked. `available()` is the fuller check (`ready`, not terminating, under `maxExec`, not `busy()`); `busy()` compares in-flight tasks against `concurrency` and also covers upstream's cleanup window (`cleaning`, `tracking`).

### Exits

`onWorkerExit` runs once per handler: on every `error` or `exit` event, including an exit the handler asked for, and when `maxExec` is reached. The pool then removes the worker and tops up to `minWorkers`. That replacement is the fork's design, so terminating a pool with `minWorkers` leaves fresh workers behind; do not "fix" it.

Under upstream's graceful shutdown the replacement hook calls `terminate()` on a worker that is already terminating. `terminate()` therefore chains a new callback onto a pending one instead of overwriting it (overwriting left `pool.terminate()` unresolved forever), and `cleanup()` clears the handler once it has run. The pool reports each worker to `onTerminateWorker` once (`_notifyWorkerTerminated`), though both the shutdown and the replacement see it.

## Conventions

The custom promise in `src/Promise.js` (exported as `{ Promise }`) is what the public API returns, not a native Promise. It carries `.pending`, `.cancel()`, `.timeout(delay)`, `.always()`, `.finally()`, and `Promise.CancellationError` / `Promise.TimeoutError`. It resolves synchronously. Two behaviors depend on it:

- `Pool.exec` replaces `promise.timeout` so a queued task's timer starts when the task actually starts, not when it was queued.
- A cancelled or timed-out task sends `__workerpool-cleanup__`; unless the worker's abort listeners settle it within `workerTerminateTimeout`, the worker is force-terminated, which rejects every other task on that worker too.

Upstream code is ES5 by design (`var`, prototype methods, `var me = this`). Match the surrounding file.

## Known rough edges

- `Pool._createWorkerHandler` does `console.info` on every worker creation, and `onError` does `console.warn` on every worker exit, including clean shutdowns. Consumers see this output.
- `_next()` dispatches one task per call. With `concurrency` above 1, a backlog that built up while no worker had room drains one task per completion or new `exec`, not all at once.
- `worker.js` tracks one `currentRequestId`, so with `concurrency` above 1 a `workerEmit` event goes to whichever task started last on that worker.
