var Q = require('q');
var mkdirp = require('mkdirp');
var levelup = require('level');
var levelQuery = require('level-queryengine');
var jsonQueryEngine = require('jsonquery-engine');
var typeforce = require('typeforce');
var promisifyDB = require('./lib/promisify-level');
var path = require('path');
var EventEmitter = require('events').EventEmitter;
var inherits = require('util').inherits;
var wait = require('./lib/wait');
var pick = require('object.pick');
var ERR_PROPS = ['message', 'arguments', 'type', 'name', 'code'];
// var TASK_STATES = ['pending', 'running', 'success', 'fail'];
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

  if (self._processing && self._processing.id === id) {
    return Q.reject(new Error('Can\'t delete task while it\'s being processed'));
  }

  return this._deleting[id] = this._db.del(id)
    .then(function() {
      self._q.some(function(task, idx) {
        if (task.id === id) {
          // let's hope Array.prototype.some doesn't use a java iterator
          self._q.splice(idx, 1);
          return true;
        }
      });

      self.emit('deleted', id);
    })
    .finally(function() {
      delete self._deleting[id];
      self._processQueue();
    });
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
      self.emit('taskpushed', task.id);
      self._processQueue();
      return task.id;
    });
}

Queue.prototype._processQueue = function() {
  var self = this;
  if (this._processing || !this._q.length) return;

  this._processing = this._q[0];
  this._processOne(this._processing)
    .then(function(task) {
      if (task.status === 'success') self._q.shift();
      if (task.status === 'fail' && task.failCount >= self._strikes) self._q.shift();

      self._processing = undefined;

      return wait(self._throttle);
    })
    .then(function() {
      self._processQueue();
    })
    .done();
}

Queue.prototype._processOne = function(task) {
  var self = this;

  task.status = 'running';
  this.emit('taskstart', task.id);
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
      task.status = task.failCount >= self._strikes ? 'fail' : 'pending';
      task.errors.push(pick(err, ERR_PROPS));
    })
    .then(function() {
      // whatever happened, save
      return self._db.put(task.id, task);
    })
    .then(function() {
      var event = 'taskstatus:' + task.status;
      switch (task.status) {
      case 'success':
        self.emit(event, task.id);
        break;
      case 'pending':
      case 'fail':
        var lastErr = task.errors[task.errors.length - 1];
        self.emit(event, task.id, lastErr);
        if (task.status === 'fail') self.emit('strikeout', task.id, task);

        break;
      }


      return task;
    })
}

Queue.prototype.destroy = function() {
  this._destroyed = true;
  return Q.ninvoke(this._db, 'close');
}

module.exports = Queue;
