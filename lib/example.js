
var conf = {};
var Bitjoe = require('bitjoe-js');
var joe = new Bitjoe(conf);
var Queue = require('./');
var wait = require('./wait');
var q = new Queue({
  run: function(task) {
    return joe.create()
      .data(task.data)
      .recipients(task.recipients || [])
      .cleartext(!!(task.public || task.cleartext))
      .setPublic(!!task.public)
      .execute();
  },
  path: './db/txrs.db'
})

var reqs = [];

for (var i = 0; i < 3; i++) {
  reqs.push({
    data: {
      index: i,
      oi: 'vey'
    },
    public: true
  });
}

var promises = reqs.map(function(d) {
  return q.push(d);
});

Q.all(promises).then(check);

function check(data) {
  Q.all(data.map(function(d) {
      return q.getById(d.id);
    })
    .then(function(results) {
      var status = {};
      results.forEach(function(task) {
        if (task.status in status) status[task.status]++;
        else status[task.status] = 0;
      });

      console.log('Status: ' + status);
      if (status.pending) setTimeout(check.bind(null, data), 10000);
    }));
}
