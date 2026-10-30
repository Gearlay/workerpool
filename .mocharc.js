'use strict';

module.exports = {
  timeout: 10000,
  'forbid-only': Boolean(process.env.CI),
  // a pool with minWorkers brings its workers back after terminate(), and they keep the process alive
  exit: true
};
