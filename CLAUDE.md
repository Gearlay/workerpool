# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## This is a fork

Gearlay's fork of `josdejong/workerpool` (remote `upstream`, published under the original name). `README.md` and `HISTORY.md` are upstream's and do **not** document anything the fork added, so treat them as reference for the original API only.

Fork-only additions, all in `Pool.js` / `WorkerHandler.js`:

| Option / API | Effect |
|-|-|
| `concurrency` | Tasks a single worker may process at once (`busy()` threshold, default 1) |
| `roundrobin` | Pick workers round-robin instead of first-available |
| `affinity` (an `exec` option) | Pin a task to `workers[affinity % workers.length]`, trumps roundrobin |
| `maxExec` | Retire a worker after N executions |
| `markNotReadyAfterExec` | Worker becomes not-ready after each exec and must call `workerReady()` again |
| `readyTimeoutDuration`, `initReadyTimeoutDuration` | Terminate a worker that fails to signal ready (init one covers first startup) |
| `gradualScaling` | Minimum ms between new worker creations, avoids spawn storms |
| `pool.wstats()` | Timing plus event-loop-utilization stats across workers, avg/min/max/total for both the interval since the last call (`avgUtil`) and each worker's whole life (`avgLifetimeUtil`), backed by `WorkerHandler.utilization()` |
| `stats().availableWorkers` | Count from `available()` rather than `busy()` |

## Commands

```bash
npm test                                   # build, then mocha test --timeout 2000
npx mocha test/Pool.test.js --timeout 2000 # single file, skips the build
npx mocha test --timeout 2000 -g "supports auto" # single test by name
npm run build                              # gulp: clean, bundle worker, bundle+minify library
npm run watch                              # rebuild on src changes
npm run coverage                           # istanbul, report at ./coverage/lcov-report/index.html
```

Tests require the real thing: they fork processes and worker threads, so failures are often timing- or platform-related rather than logic bugs. `WorkerHandler.test.js` also shells out via `find-process` to assert orphaned processes are gone.

`npm test` runs the build first, and the build rewrites the committed file `src/generated/embeddedWorker.js`. Expect that file to show up dirty after any test run; only commit it when the worker source actually changed. `dist/` is gitignored.

## Running the tests, for real

**The suite cannot complete right now.** A pool built without a script recurses on its first `exec` and spawns workers until the process dies, so a full run drowns in `Creating new worker for script null` and exits 133 (4496 worker creations against a healthy 265). Most tests build script-less pools, so this takes almost everything with it. The mechanism is under Readiness below; it arrived in commit 64b6020 and is known, with a fix pending.

To verify your own work in the meantime, neutralize it in a throwaway copy rather than in the repo. Deferring that one callback in `src/WorkerHandler.js` is enough:

```bash
git archive HEAD | tar -x -C /tmp/base && ln -s "$PWD/node_modules" /tmp/base/node_modules
# in /tmp/base/src/WorkerHandler.js, make the script-less branch defer:
#   setTimeout(this.onWorkerReady, 0);   instead of   this.onWorkerReady();
(cd /tmp/base && npx mocha test --timeout 2000 --exit)
```

A neutralized run completes and is the real baseline: **100 passing, 2 pending, 6 failing** at HEAD. Do the same to a copy carrying your change and diff the failing test names; that is the only way to tell your regression from the scenery.

**Mocha never exits on its own.** Each `WorkerHandler` starts a stats-reset `setInterval` that is never cleared, so the process idles for up to five minutes after the last test. Pass `--exit`.

**Those 6 failures predate any local change.** Four are process-worker tests timing out at 2000ms (`supports process`, `supports forkOpts parameter to pass options to fork`, `supports worker creation hook to pass dynamic options to fork (for example)`, `should wait until subprocesses have ended`), all of them hitting the termination hang listed under rough edges. `should return statistics` compares `stats()` by `deepStrictEqual` against an expected object written before the fork added `availableWorkers`. `should increase maxWorkers to match minWorkers` expects `maxWorkers` to stay at the `minWorkers` of 16, but the pool raises it to cpus-1, so the test only passes on a machine with 17 or fewer cores.

The CI matrix lists node 10.x through 16.x while `src/` needs 14+. Locally, node 20 and 25 behave the same, so use whichever is on PATH.

## Architecture

Four layers, each with one job:

- `src/index.js` is the public facade (`pool`, `worker`, `workerEmit`, `workerReady`, `Promise`, `platform`, `isMainThread`, `cpus`). It lazily requires `Pool` and `worker` so importing in a worker context does not pull in the pool.
- `src/Pool.js` owns the task queue and scheduling. `exec()` pushes a task, `_next()` pops one when `_getWorker()` returns a worker, and completion re-enters `_next()`.
- `src/WorkerHandler.js` owns a single worker: transport setup, the request/response bookkeeping (`processing` keyed by id), readiness, timing stats, and termination.
- `src/worker.js` runs *inside* the worker. It registers methods, dispatches incoming RPC, and sends results back.

### Transports

`setupWorker()` resolves `workerType` (`auto` | `web` | `thread` | `process`) and then shims whatever it gets so it looks like a `child_process`: everything gets `send`, `on`, and `kill`. Code above `setupWorker` never branches on transport again. `auto` prefers `worker_threads` on node and falls back to child processes.

Default worker script when none is passed: `__dirname/worker.js` on node (so `src/worker.js` in dev, `dist/worker.js` when bundled), and in the browser a Blob built from `src/generated/embeddedWorker.js`.

### Wire protocol

Plain JSON-RPC-ish messages over `send`/`on('message')`:

- request: `{ id, method, params }`
- response: `{ id, result, error }` (errors serialized by `convertError`, revived by `objectToError`)
- intermediate event: `{ id, isEvent: true, payload }`, delivered to the task's `options.on` callback
- the bare string `"ready"` signals worker readiness
- the bare string `__workerpool-terminate__` asks the worker to exit

That terminate constant is declared separately in both `worker.js` and `WorkerHandler.js`. Change one, change both.

### Readiness

`worker.ready` gates sending. Requests that arrive early sit in `requestQueue` and flush on `"ready"`. A scripted worker signals readiness asynchronously, as a `"ready"` message. The script-less case does not: `WorkerHandler`'s constructor sets `worker.ready` and calls `onWorkerReady()` on the spot, which is `Pool._next()`. That runs before the handler has been pushed into `pool.workers`, so `_getWorker()` re-runs against an unchanged `workers.length`, decides it is still short of `maxWorkers`, and creates another worker, one real process or thread per stack frame, until the stack gives out. Deferring that call (`setTimeout(this.onWorkerReady, 0)`) fixes it and makes both paths notify asynchronously. Until that lands, assume any script-less pool plus an `exec` is a spawn bomb. A pool created without a script marks its worker ready immediately, because the built-in worker never calls `worker.add()`. With `markNotReadyAfterExec`, the worker script is responsible for calling `workerpool.workerReady()` after each task, and `readyTimeoutDuration` is the safety net that kills a worker which never does.

### Worker selection

`Pool._getWorker(affinity)` in priority order: affinity, then roundrobin, then the first worker whose `available()` is true. After that it may spawn a new worker if under `maxWorkers` (rate-limited by `gradualScaling`). `available()` is the fuller check (`ready`, not terminating, under `maxExec`, not `busy()`); `busy()` alone only compares in-flight tasks against `concurrency`.

## Conventions

The custom promise in `src/Promise.js` is what the public API returns, not a native Promise. It carries `.pending`, `.cancel()`, `.timeout(delay)`, `.always()`, and `Promise.CancellationError` / `Promise.TimeoutError`. It resolves synchronously, and its `_then` only flattens a returned value that has **both** `then` and `catch`, so a bare thenable resolves as a plain value instead of being awaited. Two behaviors depend on it:

- `Pool.exec` replaces `promise.timeout` so a queued task's timer starts when the task actually starts, not when it was queued.
- A cancelled or timed-out task force-terminates its worker (`WorkerHandler.exec`'s catch block). Nothing else recovers a worker mid-task.

Node built-ins in `src/` go through `requireFoolWebpack` (an `eval`'d `require`), otherwise webpack tries to bundle `os` / `child_process` / `worker_threads` into the browser build. `src/worker.js` inlines its own copy of that eval because it is a separate webpack entry point.

Upstream code is ES5 by design (`var`, prototype methods, `var me = this`). Match the surrounding file. Note that fork additions already use arrow functions, `const`/`let`, and optional chaining, so `src/` needs node 14+ despite the CI matrix still listing 10.x and 12.x.

## Known rough edges

- `pool.exec(fn, params, options)` throws the options away. The function branch recurses as `this.exec("run", [String(method), params])` with no third argument, so `affinity` and the `on` event callback only reach the pool when you name a method. To offload a function and keep the options, call the worker's built-in directly: `pool.exec('run', [String(fn), args], options)`.
- `terminate(force, timeout)` ignores the timeout. `terminateAndNotify` assigns the number over the promise's `timeout` method (`resolver.promise.timeout = timeout`) instead of calling it, so no timer is ever armed and a stuck termination waits forever.
- `WorkerHandler` starts a stats-reset `setInterval` that is never cleared and not unref'd, so each handler keeps a timer alive for the process lifetime, and its comment says "every hour" while the code uses 5 minutes.
- `Pool._createWorkerHandler` does `console.info` on every worker creation, and `onError` does `console.warn` on every worker exit. Consumers see this output.
- The build's `bundle-worker` task rewrites a committed generated file, so build output and source control are coupled. See the Commands note above.
