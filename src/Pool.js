var { Promise } = require("./Promise");
var WorkerHandler = require("./WorkerHandler");
var environment = require("./environment");
var { FIFOQueue, LIFOQueue } = require("./queues");
var DebugPortAllocator = require("./debug-port-allocator");
var DEBUG_PORT_ALLOCATOR = new DebugPortAllocator();

/**
 * A pool to manage workers, which can be created using the function workerpool.pool.
 *
 * @param {String} [script]   Optional worker script
 * @param {import('./types.js').WorkerPoolOptions} [options]  See docs
 * @constructor
 */
function Pool(script, options) {
  if (typeof script === "string") {
    /** @readonly */
    this.script = script || null;
  } else {
    this.script = null;
    options = script;
  }

  /** @private */
  this.workers = []; // queue with all workers

  /** @private */
  this.taskQueue = this._createQueue(
    (options && options.queueStrategy) || "fifo"
  ); // queue with tasks awaiting execution

  options = options || {};

  /** @readonly */
  this.forkArgs = Object.freeze(options.forkArgs || []);
  /** @readonly */
  this.forkOpts = Object.freeze(options.forkOpts || {});
  /** @readonly */
  this.workerOpts = Object.freeze(options.workerOpts || {});
  /** @readonly */
  this.workerThreadOpts = Object.freeze(options.workerThreadOpts || {});
  /** @private */
  this.debugPortStart = options.debugPortStart || 43210;
  /** @readonly @deprecated */
  this.nodeWorker = options.nodeWorker;
  /** @readonly
   * @type {'auto' | 'web' | 'process' | 'thread'}
   */
  this.workerType = options.workerType || options.nodeWorker || "auto";
  /** @readonly */
  this.maxQueueSize = options.maxQueueSize || Infinity;
  /** @readonly */
  this.workerTerminateTimeout = options.workerTerminateTimeout || 1000;

  /** @readonly */
  this.onCreateWorker = options.onCreateWorker || (() => null);
  /** @readonly */
  this.onTerminateWorker = options.onTerminateWorker || (() => null);

  /** @readonly */
  this.emitStdStreams = options.emitStdStreams || false;

  /** @readonly */
  this.roundrobin = options.roundrobin || false;
  /** @readonly */
  this.concurrency = options.concurrency;
  /** @readonly */
  this.gradualScaling = options.gradualScaling || 0;
  /** @private */
  this.canCreateWorker = true;
  /** @readonly */
  this.maxExec = options.maxExec || 0;
  /** @readonly */
  this.markNotReadyAfterExec = options.markNotReadyAfterExec || false;
  /** @readonly */
  this.readyTimeoutDuration = options.readyTimeoutDuration || 0;
  /** @readonly */
  this.initReadyTimeoutDuration =
    options.initReadyTimeoutDuration || this.readyTimeoutDuration;
  /** @private */
  this.lastChosen = -1;

  // configuration
  if (options && "maxWorkers" in options) {
    validateMaxWorkers(options.maxWorkers);
    /** @readonly */
    this.maxWorkers = options.maxWorkers;
  } else {
    this.maxWorkers = Math.max((environment.cpus || 4) - 1, 1);
  }

  if (options && "minWorkers" in options) {
    if (options.minWorkers === "max") {
      /** @readonly */
      this.minWorkers = this.maxWorkers;
    } else {
      validateMinWorkers(options.minWorkers);
      this.minWorkers = options.minWorkers;
      this.maxWorkers = Math.max(this.minWorkers, this.maxWorkers); // in case minWorkers is higher than maxWorkers
    }
    this._ensureMinWorkers();
  }

  /** @private */
  this._boundNext = this._next.bind(this);

  if (this.workerType === "thread") {
    WorkerHandler.ensureWorkerThreads();
  }
}

/**
 * Execute a function on a worker.
 *
 * Example usage:
 *
 *   var pool = new Pool()
 *
 *   // call a function available on the worker
 *   pool.exec('fibonacci', [6])
 *
 *   // offload a function
 *   function add(a, b) {
 *     return a + b
 *   };
 *   pool.exec(add, [2, 4])
 *       .then(function (result) {
 *         console.log(result); // outputs 6
 *       })
 *       .catch(function(error) {
 *         console.log(error);
 *       });
 * @template { (...args: any[]) => any } T
 * @param {String | T} method  Function name or function.
 *                                    If `method` is a string, the corresponding
 *                                    method on the worker will be executed
 *                                    If `method` is a Function, the function
 *                                    will be stringified and executed via the
 *                                    workers built-in function `run(fn, args)`.
 * @param {Parameters<T> | null} [params]  Function arguments applied when calling the function
 * @param {import('./types.js').ExecOptions} [options]  Options
 * @return {Promise<ReturnType<T>>}
 */
Pool.prototype.exec = function (method, params, options) {
  // validate type of arguments
  if (params && !Array.isArray(params)) {
    throw new TypeError('Array expected as argument "params"');
  }

  if (typeof method === "string") {
    var resolver = Promise.defer();

    if (this.taskQueue.size() >= this.maxQueueSize) {
      throw new Error("Max queue size of " + this.maxQueueSize + " reached");
    }

    // add a new task to the queue
    var task = {
      method: method,
      params: params,
      resolver: resolver,
      timeout: null,
      options: options,
    };
    this.taskQueue.push(task);

    // replace the timeout method of the Promise with our own,
    // which starts the timer as soon as the task is actually started
    // TODO: how can i find if the task is still in the queue?
    var originalTimeout = resolver.promise.timeout;
    var taskQueue = this.taskQueue;
    resolver.promise.timeout = function timeout(delay) {
      if (taskQueue.contains(task)) {
        // task is still queued -> start the timer later on
        task.timeout = delay;
        return resolver.promise;
      } else {
        // task is already being executed -> start timer immediately
        return originalTimeout.call(resolver.promise, delay);
      }
    };

    // trigger task execution
    this._next();

    return resolver.promise;
  } else if (typeof method === "function") {
    // send stringified function and function arguments to worker
    return this.exec("run", [String(method), params], options);
  } else {
    throw new TypeError('Function or string expected as argument "method"');
  }
};

/**
 * Create a proxy for current worker. Returns an object containing all
 * methods available on the worker. All methods return promises resolving the methods result.
 * @template { { [k: string]: (...args: any[]) => any } } T
 * @return {Promise<import('./types.js').Proxy<T>, Error>} Returns a promise which resolves with a proxy object
 */
Pool.prototype.proxy = function () {
  if (arguments.length > 0) {
    throw new Error("No arguments expected");
  }

  var pool = this;
  return this.exec("methods").then(function (methods) {
    var proxy = {};

    methods.forEach(function (method) {
      proxy[method] = function () {
        return pool.exec(method, Array.prototype.slice.call(arguments));
      };
    });

    return proxy;
  });
};

/**
 * Creates new array with the results of calling a provided callback function
 * on every element in this array.
 * @param {Array} array
 * @param {function} callback  Function taking two arguments:
 *                             `callback(currentValue, index)`
 * @return {Promise.<Array>} Returns a promise which resolves  with an Array
 *                           containing the results of the callback function
 *                           executed for each of the array elements.
 */
/* TODO: implement map
Pool.prototype.map = function (array, callback) {
};
*/

/**
 * Grab the first task from the queue, find a free worker, and assign the
 * worker to the task.
 * @private
 */
Pool.prototype._next = function () {
  if (this.taskQueue.size() > 0) {
    // there are tasks in the queue

    // find an available worker
    var next = this.taskQueue.peek ? this.taskQueue.peek() : undefined;
    var worker = this._getWorker(next && next.options && next.options.affinity);
    if (worker) {
      // get the first task from the queue
      var me = this;
      var task = this.taskQueue.pop();

      // check if the task is still pending (and not cancelled -> promise rejected)
      if (task.resolver.promise.pending) {
        // send the request to the worker
        var promise = worker
          .exec(task.method, task.params, task.resolver, task.options)
          .then(me._boundNext)
          .catch(function () {
            // if the worker crashed and terminated, remove it from the pool
            if (worker.terminated) {
              return me._removeWorker(worker);
            }
          })
          .then(function () {
            me._next(); // trigger next task in the queue
          });

        // start queued timer now
        if (typeof task.timeout === "number") {
          promise.timeout(task.timeout);
        }
      } else {
        // The task taken was already complete (either rejected or resolved), so just trigger next task in the queue
        me._next();
      }
    }
  }
};

/**
 * Pick the affinity slot, else the next worker in turn, else the first available one; below `maxWorkers` also add a worker.
 * @param {number} [affinity]
 * @return {WorkerHandler | undefined} worker
 * @private
 */
Pool.prototype._getWorker = function (affinity) {
  var workers = this.workers;
  var chosenWorker;

  // affinity trumps roundrobin, and neither checks whether the worker is busy
  if (affinity != null) {
    chosenWorker = workers[affinity % workers.length];
  }

  if (!chosenWorker && this.roundrobin && workers.length > 0) {
    this.lastChosen = (this.lastChosen + 1) % workers.length;
    chosenWorker = workers[this.lastChosen];
  }

  if (!chosenWorker) {
    for (var i = 0; i < workers.length; i++) {
      if (workers[i].available()) {
        chosenWorker = workers[i];
        break;
      }
    }
  }

  if (workers.length < this.maxWorkers && this.canCreateWorker) {
    if (this.gradualScaling) {
      this.canCreateWorker = false;
      setTimeout(() => (this.canCreateWorker = true), this.gradualScaling);
    }
    var worker = this._createWorkerHandler();
    workers.push(worker);
    chosenWorker = chosenWorker || worker;
  }

  return chosenWorker;
};

/**
 * Timing and event loop utilization stats over the live workers; each call starts a new utilization window.
 * @return {{totalTime: number, minTime: number, lastTime: number, maxTime: number, requestCount: number, workerCount: number, workerMax: number, workersReady: number, totalUtil: number, avgUtil: number, minUtil: number, maxUtil: number, totalLifetimeUtil: number, avgLifetimeUtil: number, minLifetimeUtil: number, maxLifetimeUtil: number}} Returns an object with statistics
 */
Pool.prototype.wstats = function () {
  var workers = this.workers;
  const statObj = {
    totalTime: 0,
    minTime: Infinity,
    lastTime: 0,
    maxTime: 0,
    requestCount: 0,
    totalUtil: 0,
    avgUtil: 0,
    minUtil: Infinity,
    maxUtil: 0,
    totalLifetimeUtil: 0,
    avgLifetimeUtil: 0,
    minLifetimeUtil: Infinity,
    maxLifetimeUtil: 0,
    workerCount: this.workers.length,
    workerMax: this.maxWorkers,
    workersReady: 0,
  };

  var utilWorkers = 0;
  var lastEnded = 0;

  for (var i = 0; i < workers.length; i++) {
    const worker = workers[i];
    if (!worker.worker || worker.terminated || worker.cleaning) {
      continue;
    }
    worker.worker.ready && statObj.workersReady++;
    statObj.requestCount += worker.requestCount;
    statObj.totalTime += worker.totalTime;
    statObj.minTime = Math.min(statObj.minTime, worker.minTime);
    statObj.maxTime = Math.max(statObj.maxTime, worker.maxTime);

    // the pool's most recent task, not the largest of the per-worker last durations
    if (worker.lastEnded > lastEnded) {
      lastEnded = worker.lastEnded;
      statObj.lastTime = worker.lastTime;
    }

    const utilization = worker.utilization();
    if (utilization !== null) {
      utilWorkers++;

      statObj.totalUtil += utilization.window;
      statObj.minUtil = Math.min(statObj.minUtil, utilization.window);
      statObj.maxUtil = Math.max(statObj.maxUtil, utilization.window);

      statObj.totalLifetimeUtil += utilization.lifetime;
      statObj.minLifetimeUtil = Math.min(statObj.minLifetimeUtil, utilization.lifetime);
      statObj.maxLifetimeUtil = Math.max(statObj.maxLifetimeUtil, utilization.lifetime);
    }
  }

  if (statObj.minTime === Infinity) {
    statObj.minTime = 0;
  }

  if (statObj.minUtil === Infinity) {
    statObj.minUtil = 0;
    statObj.minLifetimeUtil = 0;
  }

  statObj.avgUtil = utilWorkers > 0 ? statObj.totalUtil / utilWorkers : 0;
  statObj.avgLifetimeUtil = utilWorkers > 0 ? statObj.totalLifetimeUtil / utilWorkers : 0;
  return statObj;
};

/**
 * Retrieve the number of workers that can take a task right now.
 * @return {number}
 */
Pool.prototype.getNumberAvailableWorkers = function () {
  return this.workers.filter(function (worker) {
    return worker.available();
  }).length;
};

/**
 * Remove a worker from the pool.
 * Attempts to terminate worker if not already terminated, and ensures the minimum
 * pool size is met.
 * @param {WorkerHandler} worker
 * @return {Promise<WorkerHandler>}
 * @private
 */
Pool.prototype._removeWorker = function (worker) {
  var me = this;

  DEBUG_PORT_ALLOCATOR.releasePort(worker.debugPort);
  // _removeWorker will call this, but we need it to be removed synchronously
  this._removeWorkerFromList(worker);
  // If minWorkers set, spin up new workers to replace the crashed ones
  this._ensureMinWorkers();
  // terminate the worker (if not already terminated)
  return new Promise(function (resolve, reject) {
    worker.terminate(false, function (err) {
      me.onTerminateWorker({
        forkArgs: worker.forkArgs,
        forkOpts: worker.forkOpts,
        workerThreadOpts: worker.workerThreadOpts,
        script: worker.script,
      });
      if (err) {
        reject(err);
      } else {
        resolve(worker);
      }
    });
  });
};

/**
 * Remove a worker from the pool list.
 * @param {WorkerHandler} worker
 * @private
 */
Pool.prototype._removeWorkerFromList = function (worker) {
  // remove from the list with workers
  var index = this.workers.indexOf(worker);
  if (index !== -1) {
    this.workers.splice(index, 1);
  }
};

/**
 * Close all active workers. Tasks currently being executed will be finished first.
 * @param {boolean} [force=false]   If false (default), the workers are terminated
 *                                  after finishing all tasks currently in
 *                                  progress. If true, the workers will be
 *                                  terminated immediately.
 * @param {number} [timeout]        If provided and non-zero, worker termination promise will be rejected
 *                                  after timeout if worker process has not been terminated.
 * @return {Promise.<void, Error>}
 */
Pool.prototype.terminate = function (force, timeout) {
  var me = this;

  // cancel any pending tasks
  var taskQueue = this.taskQueue;

  while (taskQueue.size() > 0) {
    var task = taskQueue.pop();
    if (task) {
      task.resolver.reject(new Error("Pool terminated"));
    } else {
      break;
    }
  }

  taskQueue.clear();

  var f = function (worker) {
    DEBUG_PORT_ALLOCATOR.releasePort(worker.debugPort);
    this._removeWorkerFromList(worker);
  };
  var removeWorker = f.bind(this);

  var promises = [];
  var workers = this.workers.slice();
  workers.forEach(function (worker) {
    var termPromise = worker
      .terminateAndNotify(force, timeout)
      .then(removeWorker)
      .always(function () {
        me.onTerminateWorker({
          forkArgs: worker.forkArgs,
          forkOpts: worker.forkOpts,
          workerThreadOpts: worker.workerThreadOpts,
          script: worker.script,
        });
      });
    promises.push(termPromise);
  });
  return Promise.all(promises);
};

/**
 * Retrieve statistics on tasks and workers.
 * @return {{totalWorkers: number, busyWorkers: number, availableWorkers: number, idleWorkers: number, pendingTasks: number, activeTasks: number}} Returns an object with statistics
 */
Pool.prototype.stats = function () {
  var totalWorkers = this.workers.length;
  var busyWorkers = this.workers.filter(function (worker) {
    return worker.busy();
  }).length;

  return {
    totalWorkers: totalWorkers,
    busyWorkers: busyWorkers,
    availableWorkers: this.getNumberAvailableWorkers(),
    idleWorkers: totalWorkers - busyWorkers,

    pendingTasks: this.taskQueue.size(),
    activeTasks: busyWorkers,
  };
};

/**
 * Ensures that a minimum of minWorkers is up and running
 * @private
 */
Pool.prototype._ensureMinWorkers = function () {
  if (this.minWorkers) {
    for (var i = this.workers.length; i < this.minWorkers; i++) {
      this.workers.push(this._createWorkerHandler());
    }
  }
};

/**
 * Helper function to create a new WorkerHandler and pass all options.
 * @return {WorkerHandler}
 * @private
 */
Pool.prototype._createWorkerHandler = function () {
  const overriddenParams =
    this.onCreateWorker({
      forkArgs: this.forkArgs,
      forkOpts: this.forkOpts,
      workerOpts: this.workerOpts,
      workerThreadOpts: this.workerThreadOpts,
      script: this.script,
    }) || {};

  console.info("Creating new worker for script", this.script);
  const worker = new WorkerHandler(overriddenParams.script || this.script, {
    forkArgs: overriddenParams.forkArgs || this.forkArgs,
    forkOpts: overriddenParams.forkOpts || this.forkOpts,
    workerOpts: overriddenParams.workerOpts || this.workerOpts,
    workerThreadOpts:
      overriddenParams.workerThreadOpts || this.workerThreadOpts,
    debugPort: DEBUG_PORT_ALLOCATOR.nextAvailableStartingAt(
      this.debugPortStart
    ),
    workerType: this.workerType,
    workerTerminateTimeout: this.workerTerminateTimeout,
    emitStdStreams: this.emitStdStreams,
    concurrency: this.concurrency,
    maxExec: overriddenParams.maxExec || this.maxExec,
    markNotReadyAfterExec:
      overriddenParams.markNotReadyAfterExec == null
        ? this.markNotReadyAfterExec
        : overriddenParams.markNotReadyAfterExec,
    readyTimeoutDuration:
      overriddenParams.readyTimeoutDuration || this.readyTimeoutDuration,
    initReadyTimeoutDuration:
      overriddenParams.initReadyTimeoutDuration || this.initReadyTimeoutDuration,
    onWorkerExit: () => {
      this._removeWorker(worker);
    },
    onWorkerReady: () => {
      this._next();
    },
  });

  return worker;
};

/**
 * Create queue instance based on strategy
 * @param {'fifo' | 'lifo' | import('./types').TaskQueue} strategy
 * @returns {import('./types').TaskQueue} Queue instance
 * @private
 */
Pool.prototype._createQueue = function (strategy) {
  if (typeof strategy === "string") {
    switch (strategy) {
      case "fifo":
        return new FIFOQueue();
      case "lifo":
        return new LIFOQueue();
      default:
        throw new Error("Unknown queue strategy: " + strategy);
    }
  }

  if (!strategy) {
    throw new Error("Queue strategy cannot be null or undefined");
  }

  // validate if custom queue implements required methods
  var requiredMethods = ["push", "pop", "size", "contains", "clear"];

  for (var i = 0; i < requiredMethods.length; i++) {
    var method = requiredMethods[i];
    if (typeof strategy[method] !== "function") {
      throw new Error("Queue strategy must implement method: " + method);
    }
  }

  return strategy;
};
/**
 * Ensure that the maxWorkers option is an integer >= 1
 * @param {*} maxWorkers
 * @returns {boolean} returns true maxWorkers has a valid value
 */
function validateMaxWorkers(maxWorkers) {
  if (!isNumber(maxWorkers) || !isInteger(maxWorkers) || maxWorkers < 1) {
    throw new TypeError("Option maxWorkers must be an integer number >= 1");
  }
}

/**
 * Ensure that the minWorkers option is an integer >= 0
 * @param {*} minWorkers
 * @returns {boolean} returns true when minWorkers has a valid value
 */
function validateMinWorkers(minWorkers) {
  if (!isNumber(minWorkers) || !isInteger(minWorkers) || minWorkers < 0) {
    throw new TypeError("Option minWorkers must be an integer number >= 0");
  }
}

/**
 * Test whether a variable is a number
 * @param {*} value
 * @returns {boolean} returns true when value is a number
 */
function isNumber(value) {
  return typeof value === "number";
}

/**
 * Test whether a number is an integer
 * @param {number} value
 * @returns {boolean} Returns true if value is an integer
 */
function isInteger(value) {
  return Math.round(value) == value;
}

module.exports = Pool;
