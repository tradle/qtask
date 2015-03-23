var Q = require('q');
var rimraf = require('rimraf');
var test = require('tape');
var path = require('path');
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

function succeedAfter(num) {
  var actions = [];
  for (var i = 0; i < num; i++) {
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

test('order is preserved', function(t) {
  rimraf.sync(config.path);
  var q = new Queue(config);
  var reqs = [];
  var num = 4;

  reqs.push({
    actions: failRepeatedly(config.strikes),
    data: {
      oi: 'vey'
    }
  });

  for (var i = 1; i < num; i++) {
    reqs.push({
      // timeout: num - i, // timeout first one the most, see if they finish in order
      actions: succeedAfter(config.strikes - 1), // succeed just before striking out
      data: {
        oi: 'vey' + i
      },
      public: true
    });
  }

  reqs.forEach(function(d) {
    return q.push(d).done();
  });

  var finished = 0;
  // var failed = 0;
  var started = 0;
  var pushed = 0;

  q.on('taskpushed', function(task) {
    // console.log('Pushed: ' + task.id);
    t.equal(task.id, pushed++);
  });

  q.on('taskstart', function(task) {
    // console.log('Running: ' + task.id);
    t.equal(task.id, started);
  });

  // q.on('status:fail', function(task) {
  //   console.log('Fail: ' + task.id);
  // });

  q.on('status:struckout', function(task) {
    // console.log('Struck out: ' + task.id);
    t.equal(task.id, 0);
    if (config.blockOnFail) {
      q.skip(task.id)
      // .then(function() {
      //   console.log('Skipped: ' + task.id);
      // });
    }

    finish();
  });

  q.on('status:success', function(task) {
    // console.log('Success: ' + task.id);
    t.equal(task.id, finished);
    finish();
  });

  function finish() {
    started++; // next one can start now
    if (++finished === num) {
      q.destroy().done(t.end);
    }
  }
})

test('cleanup', function(t) {
  rimraf(path.dirname(config.path), t.end);
});
