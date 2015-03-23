var Q = require('q');
var rimraf = require('rimraf');
var test = require('tape');
var Queue = require('../');
var wait = require('../lib/wait');
var config = {
  throttle: 100,
  blockOnFail: true,
  strikes: 3,
  run: postpone,
  path: './db/txrs.db'
};

function run(task) {
  switch (task.input.actions.shift()) {
    case 'success':
      break;
    case 'fail':
      throw new Error(task.input.data);
    // case 'pending':
    //   console.log('postponing');
    //   return postpone(task);
  }
}

function postpone(task) {
  return wait(task.input.timeout || 0)
    .then(run.bind(null, task));
}

function succeedOn(num) {
  var actions = [];
  for (var i = 0; i < num - 1; i++) {
    actions.push('fail');
  }

  actions.push('success');
  return actions;
}

function failRepeatedly(numTimes) {
  var actions = [];
  for (var i = 0; i < numTimes; i++) {
    actions.push('fail');
  }

  return actions;
}

test('success', function(t) {
  rimraf.sync(config.path);
  var q = new Queue(config);
  var reqs = [];
  var num = 4;

  reqs.push({
    actions: failRepeatedly(config.strikes),
    data: {
      index: 3,
      oi: 'vey'
    }
  });

  for (var i = 0; i < num - 1; i++) {
    reqs.push({
      // timeout: num - i, // timeout first one the most, see if they finish in order
      actions: succeedOn(config.strikes),
      data: {
        index: i,
        oi: 'vey'
      },
      public: true
    });
  }

  reqs.forEach(function(d) {
    return q.push(d).done();
  });

  var finished = 0;
  var started = 0;
  var pushed = 0;

  q.on('taskpushed', function(id) {
    // console.log('Pushed: ' + id);
    t.equal(id, pushed++);
  });

  q.on('taskstart', function(id) {
    // console.log('Running: ' + id);
    t.equal(id, started);
  });

  q.on('taskstatus:fail', function(id) {
    // console.log('Fail: ' + id);
    t.equal(id, 0);
    started++; // next one can start now
    finish();
  });

  q.on('taskstatus:success', function(id) {
    // console.log('Success: ' + id);
    started++; // next one can start now
    t.equal(id, finished);
    finish();
  });

  function finish() {
    if (++finished === num) {
      q.destroy().done(t.end);
    }
  }
})
