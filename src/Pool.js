var Promise = require("./Promise");
var WorkerHandler = require("./WorkerHandler");
var environment = require("./environment");
var DebugPortAllocator = require("./debug-port-allocator");
var DEBUG_PORT_ALLOCATOR = new DebugPortAllocator();

/**
 * @typedef {"auto" | "thread" | "thread" | "web"} WorkerType
 * @typedef {Object} WorkerPoolOptions
 * @property {string[]} [forkArgs]
 * @property {import('child_process').ForkOptions} [forkOpts]
 * @property {import('worker_threads').WorkerOptions} [workerThreadOpts]
 * @property {number} [debugPortStart]
 * @property {WorkerType} [nodeWorker] alias to workerType
 * @property {boolean} [roundrobin]
 * @property {WorkerType} [workerType]
 * @property {number} [maxQueueSize]
 * @property {number} [concurrency]
 * @property {number} [gradualScaling]
 * @property {number} [maxExec] the number of distinct executions allowed
 * @property {Function} [onCreateWorker]
 * @property {Function} [onTerminateWorker]
 * @property {number} [maxWorkers]
 * @property {number | "max"} [minWorkers]
 * @property {boolean} [markNotReadyAfterExec] Default false. If true will mark the worker as not ready after an execution finishes. It then expects the worker to signal ready afterwards
 * @property {number} [readyTimeoutDuration] if not set or set to 0 will not have a ready timeout
 * @property {number} [initReadyTimeoutDuration] defaults to `readyTimeoutDuration`
/**
 * A pool to manage workers
 * @param {String} [script]   Optional worker script
 * @param {WorkerPoolOptions} [options]  See docs
 * @constructor
 */
function Pool(script, options) {
  if (typeof script === "string") {
    this.script = script || null;
  } else {
    this.script = null;
    options = script;
  }

  this.workers = []; // queue with all workers
  this.tasks = []; // queue with tasks awaiting execution

  options = options || {};

  this.forkArgs = Object.freeze(options.forkArgs || []);
  this.forkOpts = Object.freeze(options.forkOpts || {});
  this.workerThreadOpts = Object.freeze(options.workerThreadOpts || {});
  this.debugPortStart = options.debugPortStart || 43210;
  this.nodeWorker = options.nodeWorker;
  this.roundrobin = options.roundrobin;
  this.workerType = options.workerType || options.nodeWorker || "auto";
  this.maxQueueSize = options.maxQueueSize || Infinity;
  this.concurrency = options.concurrency;
  this.gradualScaling = options.gradualScaling || 0;
  this.canCreateWorker = true;
  this.maxExec = options.maxExec || 0;
  this.markNotReadyAfterExec = options.markNotReadyAfterExec || false;
  this.readyTimeoutDuration = options.readyTimeoutDuration || 0;
  this.initReadyTimeoutDuration =
    options.initReadyTimeoutDuration || this.readyTimeoutDuration;

  this.onCreateWorker = options.onCreateWorker || (() => null);
  this.onTerminateWorker = options.onTerminateWorker || (() => null);

  // configuration
  if (options && "maxWorkers" in options) {
    validateMaxWorkers(options.maxWorkers);
    this.maxWorkers = options.maxWorkers;
  } else {
    this.maxWorkers = Math.max((environment.cpus || 4) - 1, 1);
  }

  if (options && "minWorkers" in options) {
    if (options.minWorkers === "max") {
      this.minWorkers = this.maxWorkers;
    } else {
      validateMinWorkers(options.minWorkers);
      this.minWorkers = options.minWorkers;
      this.maxWorkers = Math.max(this.minWorkers, this.maxWorkers); // in case minWorkers is higher than maxWorkers
    }
    this._ensureMinWorkers();
  }

  this._boundNext = this._next.bind(this);

  if (this.workerType === "thread") {
    WorkerHandler.ensureWorkerThreads();
  }

  this.lastChosen = -1;
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
 *
 * @param {String | Function} method  Function name or function.
 *                                    If `method` is a string, the corresponding
 *                                    method on the worker will be executed
 *                                    If `method` is a Function, the function
 *                                    will be stringified and executed via the
 *                                    workers built-in function `run(fn, args)`.
 * @param {Array} [params]  Function arguments applied when calling the function
 * @param {ExecOptions} [options]  Options object
 * @return {Promise.<*, Error>} result
 */
Pool.prototype.exec = function (method, params, options) {
  // validate type of arguments
  if (params && !Array.isArray(params)) {
    throw new TypeError('Array expected as argument "params"');
  }

  if (typeof method === "string") {
    var resolver = Promise.defer();

    if (this.tasks.length >= this.maxQueueSize) {
      throw new Error("Max queue size of " + this.maxQueueSize + " reached");
    }

    // add a new task to the queue
    var tasks = this.tasks;
    var task = {
      method: method,
      params: params,
      resolver: resolver,
      timeout: null,
      options: options,
    };
    tasks.push(task);

    // replace the timeout method of the Promise with our own,
    // which starts the timer as soon as the task is actually started
    var originalTimeout = resolver.promise.timeout;
    resolver.promise.timeout = function timeout(delay) {
      if (tasks.indexOf(task) !== -1) {
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
    return this.exec("run", [String(method), params]);
  } else {
    throw new TypeError('Function or string expected as argument "method"');
  }
};

/**
 * Create a proxy for current worker. Returns an object containing all
 * methods available on the worker. The methods always return a promise.
 *
 * @return {Promise.<Object, Error>} proxy
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
 * @protected
 */
Pool.prototype._next = function () {
  if (this.tasks.length > 0) {
    // there are tasks in the queue

    // find an available worker
    const affinity = this.tasks[0].options?.affinity;
    var worker = this._getWorker(affinity);
    if (worker) {
      // get the first task from the queue
      var me = this;
      var task = this.tasks.shift();

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
 * Get an available worker. If no worker is available and the maximum number
 * of workers isn't yet reached, a new worker will be created and returned.
 * If no worker is available and the maximum number of workers is reached,
 * null will be returned.
 *
 * @return {WorkerHandler | null} worker
 * @private
 */
Pool.prototype._getWorker = function (affinity) {
  var workers = this.workers;
  let chosenWorker;

  // Affinity trumps roundrobin
  if (affinity != null) {
    chosenWorker = workers[affinity % workers.length];
  }

  if (!chosenWorker && this.roundrobin && workers.length > 0) {
    chosenWorker =
      workers[(this.lastChosen = ++this.lastChosen % workers.length)];
  }

  if (!chosenWorker) {
    for (var i = 0; i < workers.length; i++) {
      var worker = workers[i];
      if (worker.available()) {
        chosenWorker = worker;
        break;
      }
    }
  }

  if (workers.length < this.maxWorkers) {
    // create a new worker
    if (this.gradualScaling === 0) {
      worker = this._createWorkerHandler();
      workers.push(worker);
      chosenWorker = chosenWorker || worker;
    } else if (this.canCreateWorker) {
      this.canCreateWorker = false;
      setTimeout(() => (this.canCreateWorker = true), this.gradualScaling);
      worker = this._createWorkerHandler();
      workers.push(worker);
      chosenWorker = chosenWorker || worker;
    }
  }
  return chosenWorker;
};

/**
 * Get an available worker. If no worker is available and the maximum number
 * of workers isn't yet reached, a new worker will be created and returned.
 * If no worker is available and the maximum number of workers is reached,
 * null will be returned.
 *
 * @return {WorkerHandler | null} worker
 * @private
 */
Pool.prototype.wstats = function () {
  var workers = this.workers;
  const statObj = {
    totalTime: 0,
    minTime: 0,
    lastTime: 0,
    maxTime: 0,
    requestCount: 0,
    totalUtil: 0,
    workerCount: this.workers.length,
    workerMax: this.maxWorkers,
    workersReady: 0,
  };

  for (var i = 0; i < workers.length; i++) {
    const worker = workers[i];
    if (!worker.worker) {
      continue;
    }
    worker.worker.ready && statObj.workersReady++;
    statObj.requestCount += worker.requestCount;
    statObj.totalTime += worker.totalTime;
    if (statObj.minTime > worker.minTime) {
      statObj.minTime = worker.minTime;
    }

    if (statObj.maxTime < worker.maxTime) {
      statObj.maxTime = worker.maxTime;
    }

    if (statObj.lastTime < worker.lastTime) {
      statObj.lastTime = worker.lastTime;
    }
    statObj.totalUtil += worker.worker.performance
      ? worker.worker.performance.eventLoopUtilization().utilization
      : 0;
  }

  statObj.avgUtil = statObj.totalUtil / workers.length;
  return statObj;
};

/**
 * Remove a worker from the pool.
 * Attempts to terminate worker if not already terminated, and ensures the minimum
 * pool size is met.
 * @param {WorkerHandler} worker
 * @return {Promise<WorkerHandler>}
 * @protected
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
 * @protected
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
  this.tasks.forEach(function (task) {
    task.resolver.reject(new Error("Pool terminated"));
  });
  this.tasks.length = 0;

  var f = function (worker) {
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
          script: worker.script,
        });
      });
    promises.push(termPromise);
  });
  return Promise.all(promises);
};

/**
 * Retrieve the number of available workers.
 * @return {number}
 */
Pool.prototype.getNumberAvailableWorkers = function () {
  return this.workers.filter(function (worker) {
    return worker.available();
  }).length;
};

/**
 * Retrieve statistics on tasks and workers.
 * @return {{totalWorkers: number, busyWorkers: number, idleWorkers: number, pendingTasks: number, activeTasks: number}} Returns an object with statistics
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

    pendingTasks: this.tasks.length,
    activeTasks: busyWorkers,
  };
};

/**
 * Ensures that a minimum of minWorkers is up and running
 * @protected
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
  const overridenParams =
    this.onCreateWorker({
      forkArgs: this.forkArgs,
      forkOpts: this.forkOpts,
      workerThreadOpts: this.workerThreadOpts,
      script: this.script,
    }) || {};

  console.info("Creating new worker for script", this.script);
  const worker = new WorkerHandler(overridenParams.script || this.script, {
    forkArgs: overridenParams.forkArgs || this.forkArgs,
    forkOpts: overridenParams.forkOpts || this.forkOpts,
    workerThreadOpts: overridenParams.workerThreadOpts || this.workerThreadOpts,
    debugPort: DEBUG_PORT_ALLOCATOR.nextAvailableStartingAt(
      this.debugPortStart
    ),
    workerType: this.workerType,
    concurrency: this.concurrency,
    maxExec: overridenParams.maxExec || this.maxExec,
    markNotReadyAfterExec:
      overridenParams.markNotReadyAfterExec == null
        ? this.markNotReadyAfterExec
        : overridenParams.markNotReadyAfterExec,
    readyTimeoutDuration:
      overridenParams.readyTimeoutDuration || this.readyTimeoutDuration,
    initReadyTimeoutDuration:
      overridenParams.initReadyTimeoutDuration || this.initReadyTimeoutDuration,
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
