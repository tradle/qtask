var Q = require('q');
var mkdirp = require('mkdirp');
var levelup = require('level');
var levelQuery = require('level-queryengine');
var jsonQueryEngine = require('jsonquery-engine');
var typeforce = require('typeforce');
var extend = require('extend');
var promisifyDB = require('./lib/promisify-level');
var path = require('path');
var EventEmitter = require('events').EventEmitter;
var inherits = require('util').inherits;
var wait = require('./lib/wait');
var pick = require('object.pick');
var ERR_PROPS = ['message', 'arguments', 'type', 'name', 'code'];
// var TASK_STATES = ['pending', 'running', 'success', 'fail', 'struckout'];
var STRIKES = 3;

// TODO: make these an option to Queue constructor

function Queue(options) {
  var self = this;

  typeforce({
    path: 'String',
    run: 'Function',
    throttle: 'Number'
  }, options);

  options.strikes = options.strikes || STRIKES;

  EventEmitter.call(this);

  var dir = mkdirp.sync(path.dirname(options.path));

  this._run = options.run;
  this._throttle = options.throttle;
  this._strikes = options.strikes || STRIKES;
  this._blockOnFail = options.blockOnFail;

  this._q = [];
  this._db = levelQuery(levelup(options.path, { valueEncoding: 'json' }));
  this._db.query.use(jsonQueryEngine());
  this._db.ensureIndex('status');
  this._db.ensureIndex('failCount');
  this._deleting = {};
  promisifyDB(this._db);

  this._setupCount();
  this._load();
}

inherits(Queue, EventEmitter);

Queue.prototype._setupCount = function() {
  var self = this;

  if ('count' in this) return;

  var count = -1;

  this._count = function() {
    return this._db.get('COUNT')
      .catch(function(err) {
        if (err.name === 'NotFoundError') {
          count = 0;
          return self._db.put('COUNT', 0);
        }
        else throw err;
      });
  }

  this._inc = function() {
    return this.ready
      .then(function() {
        return self._db.put('COUNT', count + 1);
      })
      .then(function() {
        count++;
        self.emit('count', count);
        return count;
      })
  }

  Object.defineProperty(this, 'count', {
    get: function() {
      if (self.ready.inspect().state !== 'fulfilled') throw new Error('Not ready yet!')

      return count;
    }.bind(this)
  });

  Object.defineProperty(this, 'length', {
    get: function() {
      return self._q.length
    }
  });
}

Queue.prototype._load = function() {
  var self = this;

  if (this.ready) return this.ready;

  var load = Q.all([
      this.pending(),
      this.failed(this._strikes)
    ])
    .spread(function(pending, failed) {
      self._q = pending.concat(failed).sort(function(a, b) {
        return a.id - b.id;
      })
    });

  return this.ready = Q.all([
      this._count(),
      load
    ])
    .then(function() {
      self.emit('ready');
    });
}

Queue.prototype.failed = function(maxTimes) {
  if (arguments.length === 0)
    return this._db.readStreamPromise({ status: 'fail' });
  else
    return this._db.readStreamPromise({ failCount: { $lt: maxTimes } });
}

Queue.prototype.pending = function() {
  return this._db.readStreamPromise({ status: 'pending' });
}

Queue.prototype.running = function() {
  return this._db.readStreamPromise({ status: 'running' });
}

Queue.prototype.succeeded = function() {
  return this._db.readStreamPromise({ status: 'success' });
}

Queue.prototype.getById = function(id) {
  return this._db.get(id);
}

Queue.prototype.delete = function(id) {
  var self = this;

  if (this._deleting[id]) return this._deleting[id];

  if (this.isProcessing(id)) {
    return Q.reject(new Error('Can\'t delete task while it\'s being processed'));
  }

  return this._deleting[id] = this._db.del(id)
    .then(function() {
      remove(self._q, id);
      self.emit('deleted', id);
    })
    .finally(function() {
      delete self._deleting[id];
      if (self._blocked && self._blocked.id === id) delete self._blocked;

      self._processQueue();
    });
}

/**
 * skip struck-out task that's blocking the queue
 */
Queue.prototype.skip = function(id) {
  var self = this;

  if (this.isProcessing(id)) throw new Error('Can\'t skip task while it\'s being processed');

  var idx = find(this._q, id);
  if (idx === -1) return Q.reject(new Error('task not found'));

  var task = extend(true, {}, this._q[idx]); // defensive copy
  task.status = 'skipped';
  return this._db.put(id, task)
    .then(function() {
      delete self._blocked;
      remove(self._q, id);
      self._processQueue();
    });
}

Queue.prototype.isProcessing = function(id) {
  return this._processing && this._processing.id === id;
}

Queue.prototype.push = function(data) {
  var self = this;

  var task = {
    status: 'pending',
    input: data,
    value: null, // fulfillment value
    errors: [],
    failCount: 0
  };

  return this._inc()
    .then(function() {
      task.id = self.count - 1;
      return self._db.put(task.id, task)
    })
    .then(function() {
      self._q.push(task);
      self.emit('taskpushed', task);
      self._processQueue();
      return task.id;
    });
}

Queue.prototype._processQueue = function() {
  var self = this;

  if (this._processing || this._blocked || !this._q.length) return;

  var orig = this._q[0];
  this._processOne(orig)
    .then(function(task) {
      if (task.status === 'success') {
        self._q.shift();
      }
      else if (task.status === 'struckout') {
        if (self._blockOnFail) {
          self._blocked = task;
        }
        else {
          self._q.shift();
        }
      }

      if (find(self._q, task.id) !== -1) extend(true, orig, task);

      // if a task failed but didn't strike out,
      // it remains as the next task to be processed

      return wait(self._throttle);
    })
    .then(function() {
      self._processQueue();
    })
    .done();
}

Queue.prototype._processOne = function(task) {
  var self = this;

  task = this._processing = extend(true, {}, task);
  task.status = 'running';
  this.emit('taskstart', task);
  return this._db.put(task.id, task)
    .then(function() {
      // run
      return self._run(task);
    })
    .then(function(result) {
      // on success
      task.status = 'success';
      task.data = result;
    })
    .catch(function(err) {
      // on fail
      task.failCount++;
      task.status = task.failCount >= self._strikes ? 'struckout' : 'fail';
      task.errors.push(pick(err, ERR_PROPS));
    })
    .then(function() {
      // whatever happened, save
      return self._db.put(task.id, task);
    })
    .then(function() {
      self._processing = undefined;

      if (task.status === 'success') {
        self.emit('status:success', task);
      }
      else {
        var lastErr = task.errors[task.errors.length - 1];
        self.emit('status:fail', task, lastErr);
        if (task.status === 'struckout') {
          self.emit('status:struckout', task);
        }
      }

      return task;
    })
}

Queue.prototype.destroy = function() {
  this._destroyed = true;
  return Q.ninvoke(this._db, 'close');
}

function find(q, id) {
  var idx;
  q.some(function(task, i) {
    if (task.id === id) {
      // let's hope Array.prototype.some doesn't use a java iterator
      idx = i
      return true;
    }
  });

  return idx;
}

function remove(q, id) {
  var idx = find(q, id);
  if (idx !== -1) q.splice(idx, 1);
}

module.exports = Queue;
