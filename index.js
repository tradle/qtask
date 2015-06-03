var Q = require('q');
var mkdirp = require('mkdirp');
var levelup = require('levelup');
var levelQuery = require('level-queryengine');
var jsonQueryEngine = require('jsonquery-engine');
var typeforce = require('typeforce');
var extend = require('extend');
var promisifyDB = require('q-level');
var path = require('path');
var EventEmitter = require('events').EventEmitter;
var inherits = require('util').inherits;
var wait = require('./lib/wait');
var pick = require('object.pick');
var ERR_PROPS = ['message', 'arguments', 'type', 'name', 'code'];
var indices = ['status', 'failCount'];
// var TASK_STATES = ['pending', 'running', 'success', 'fail', 'struckout'];

// TODO: make these an option to Queue constructor

function Queue(options) {
  var self = this;

  typeforce({
    path: 'String',
    process: 'Function',
    throttle: 'Number',
    leveldown: 'Function'
  }, options);

  EventEmitter.call(this);

  this._options = extend(true, {}, options);
  this._process = options.process;
  this._throttle = options.throttle;
  this._strikes = ('strikes' in options) ? options.strikes : false;
  this._blockOnFail = options.blockOnFail;

  this._q = [];
  this._db = levelQuery(levelup(options.path, {
    db: options.leveldown,
    valueEncoding: 'json'
  }));

  this._db.query.use(jsonQueryEngine());

  indices.forEach(function(i) {
    self._db.ensureIndex(i);
  });

  this._deleting = {};
  promisifyDB(this._db);
  promisifyDB(this._db, 'query', { type: 'readable' })

  this._setupCount();
  this._load()
    .then(function() {
      if (options.autostart) self.start();
    });
}

inherits(Queue, EventEmitter);

Queue.prototype._setupCount = function() {
  var self = this;

  if ('count' in this) return;

  var count = 0;

  this._count = function() {
    return this._db.get('COUNT')
      .catch(function(err) {
        if (err.name === 'NotFoundError') {
          return self._db.put('COUNT', count);
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

  var query = {
    status: { $in: ['running', 'fail', 'pending'] }
  }

  var results = [];
  var load = this._db.query(query)
    .progress(function(data) {
      results.push(data);
    })
    .then(function() {
      results.forEach(function(r) {
        if (r.status === 'running') r.status = 'pending';
      })

      self._q = results.sort(function(a, b) {
        return a.id - b.id;
      })
    });

  return this.ready = Q.all([
      Q.nfcall(mkdirp, path.dirname(this._options.path)),
      this._count(),
      load
    ])
    .then(function() {
      self.emit('ready');
    });
}

/**
 * @param {Number} [lessThan] - failed less than this many times
 */
Queue.prototype.failed = function(lessThan) {
  if (arguments.length === 0)
    return this.query({ status: 'fail' });
  else
    return this.query({ $and: [{ status: 'fail'}, { failCount: { $lt: lessThan } }] });
}

Queue.prototype.query = function(query) {
  // only return tasks
  var orig = query || {};
  query = { $and: [{ $not: { id: undefined } }, orig] };

  return this._db.readStreamPromise(query);
}

Queue.prototype.pending = function() {
  return this.query({ status: 'pending' });
}

Queue.prototype.running = function() {
  return this.query({ status: 'running' });
}

Queue.prototype.succeeded = function() {
  return this.query({ status: 'success' });
}

Queue.prototype.getById = function(id) {
  return this._db.get(id);
}

Queue.prototype.delete = function(id) {
  var self = this;

  if (this._deleting[id]) return this._deleting[id];

  var rejection = this._check('delete', id);
  if (rejection) return rejection;

  return this._deleting[id] = this._db.del(id)
    .then(function() {
      remove(self._q, id);
      self.emit('deleted', id);
    })
    .finally(function() {
      delete self._deleting[id];
      if (self._blocked && self._blocked.id === id) delete self._blocked;

      self.start();
    });
}

/**
 * skip struck-out task that's blocking the queue
 */
Queue.prototype.skip = function(id) {
  var self = this;
  var rejection = this._check('skip', id);
  if (rejection) return rejection;

  var idx = find(this._q, id);
  var task = extend(true, {}, this._q[idx]); // defensive copy
  task.status = 'skipped';
  return this._db.put(id, task)
    .then(function() {
      delete self._blocked;
      remove(self._q, id);
      self.start();
    });
}

Queue.prototype._check = function(action, id) {
  if (this.isProcessing(id)) return Q.reject(new Error('Can\'t ' + action + ' task while it\'s being processed'));

  if (find(this._q, id) === -1) return Q.reject(new Error('task not found'));
}

/**
 * @param {Number} id
 * @param {Object} data - new input data for task with id [id]
 */
Queue.prototype.update = function(id, data) {
  var self = this;
  var rejection = this._check('update', id);
  if (rejection) return rejection;

  var idx = find(this._q, id);
  var task = extend(true, {}, this._q[idx]); // defensive copy
  task.input = data;
  return this._db.put(id, task)
    .then(function() {
      self.start();
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
      self.start();
      return task.id;
    });
}

Queue.prototype.start = function() {
  return this.ready.then(this._start.bind(this));
}

Queue.prototype._start = function() {
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
      self.start();
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
      task.input = normalize(task.input)
      return self._process(task.input);
    })
    .then(function(result) {
      // on success
      task.status = 'success';
      task.result = result;
    })
    .catch(function(err) {
      // on fail
      task.failCount++;
      if (self._strikes !== false && task.failCount >= self._strikes)
        task.status = 'struckout';
      else
        task.status = 'fail';

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
  return this._db.close();
}

function find(q, id) {
  var idx = -1;
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

function normalize(input) {
  for (var p in input) {
    var val = input[p]
    if (val) {
      if (val.type === 'Buffer'
        && Array.isArray(val.data)
        && Object.keys(val).length === 2) {
        input[p] = new Buffer(val.data)
      }
      else if (typeof val === 'object') {
        normalize(val)
      }
    }
  }

  return input
}

module.exports = Queue;
