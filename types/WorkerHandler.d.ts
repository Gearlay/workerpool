export = WorkerHandler;
/**
 * A WorkerHandler controls a single worker. This worker can be a child process
 * on node.js or a WebWorker in a browser environment.
 * @param {String} [script] If no script is provided, a default worker with a
 *                          function run will be created.
 * @param {import('./types.js').WorkerPoolOptions} [_options] See docs
 * @constructor
 */
declare function WorkerHandler(script?: string, _options?: import("./types.js").WorkerPoolOptions): void;
declare class WorkerHandler {
    /**
     * A WorkerHandler controls a single worker. This worker can be a child process
     * on node.js or a WebWorker in a browser environment.
     * @param {String} [script] If no script is provided, a default worker with a
     *                          function run will be created.
     * @param {import('./types.js').WorkerPoolOptions} [_options] See docs
     * @constructor
     */
    constructor(script?: string, _options?: import("./types.js").WorkerPoolOptions);
    script: any;
    worker: any;
    debugPort: any;
    forkOpts: any;
    forkArgs: string[] | undefined;
    workerOpts: WorkerOptions | undefined;
    workerThreadOpts: any;
    workerTerminateTimeout: number | undefined;
    concurrency: number;
    maxExec: number;
    markNotReadyAfterExec: boolean;
    readyTimeoutDuration: number | undefined;
    initReadyTimeoutDuration: number | undefined;
    readyTimeoutTimer: number | null;
    requestCount: number;
    responseCount: number;
    totalTime: number;
    minTime: number;
    maxTime: number;
    lastTime: number;
    lastEnded: number;
    lastElu: any;
    onWorkerReady: any;
    onWorkerExit: () => void;
    statsWindowTimer: number;
    requestQueue: any[];
    processing: any;
    tracking: any;
    terminating: boolean;
    terminated: boolean;
    cleaning: boolean;
    terminationHandler: Function | null;
    lastId: number;
    /**
     * Get a list with methods available on the worker.
     * @return {Promise.<String[], Error>} methods
     */
    methods(): Promise<string[], Error>;
    /**
     * Execute a method with given parameters on the worker
     * @param {String} method
     * @param {Array} [params]
     * @param {{resolve: Function, reject: Function}} [resolver]
     * @param {import('./types.js').ExecOptions}  [options]
     * @return {Promise.<*, Error>} result
     */
    exec(method: string, params?: any[], resolver?: {
        resolve: Function;
        reject: Function;
    }, options?: import("./types.js").ExecOptions): Promise<any, Error>;
    /**
     * Test whether the worker is at its `concurrency` limit or cleaning up before termination.
     * @return {boolean} Returns true if the worker is busy
     */
    busy(): boolean;
    /**
     * Test whether the worker can take a task right now.
     * @return {boolean} Returns true if the worker is available
     */
    available(): boolean;
    /**
     * Event loop utilization over the worker's life and since the previous call; null outside worker_threads.
     * @return {{lifetime: number, window: number} | null} utilization
     */
    utilization(): {
        lifetime: number;
        window: number;
    } | null;
    /**
     * Terminate the worker unless it reports ready within `duration` milliseconds; a falsy duration only clears the timer.
     * @param {number} [duration]
     */
    setReadyTimeout(duration?: number): void;
    clearReadyTimeout(): void;
    /**
     * Terminate the worker.
     * @param {boolean} [force=false]   If false (default), the worker is terminated
     *                                  after finishing all tasks currently in
     *                                  progress. If true, the worker will be
     *                                  terminated immediately.
     * @param {function} [callback=null] If provided, will be called when process terminates.
     */
    terminate(force?: boolean, callback?: Function): void;
    /**
     * Terminate the worker, returning a Promise that resolves when the termination has been done.
     * @param {boolean} [force=false]   If false (default), the worker is terminated
     *                                  after finishing all tasks currently in
     *                                  progress. If true, the worker will be
     *                                  terminated immediately.
     * @param {number} [timeout]        If provided and non-zero, worker termination promise will be rejected
     *                                  after timeout if worker process has not been terminated.
     * @return {Promise.<WorkerHandler, Error>}
     */
    terminateAndNotify(force?: boolean, timeout?: number): Promise<WorkerHandler, Error>;
}
declare namespace WorkerHandler {
    export { tryRequireWorkerThreads as _tryRequireWorkerThreads, setupProcessWorker as _setupProcessWorker, setupBrowserWorker as _setupBrowserWorker, setupWorkerThreadWorker as _setupWorkerThreadWorker, ensureWorkerThreads, TerminateError };
}
import { Promise } from "./Promise";
declare function tryRequireWorkerThreads(): any;
declare function setupProcessWorker(script: any, options: any, child_process: any): any;
declare function setupBrowserWorker(script: any, workerOpts: any, Worker: any): any;
declare function setupWorkerThreadWorker(script: any, WorkerThreads: any, options: any): any;
declare function ensureWorkerThreads(): any;
declare class TerminateError extends Error {
    /**
     * Create a timeout error
     * @param {String} [message]
     * @param {Error=} [cause]
     */
    constructor(message?: string, cause?: Error | undefined);
    cause: Error | undefined;
}
