
var Q = require('q');

function readStreamPromise(db, query, filter) {
  var results = [];
  var defer = Q.defer();

  db.query(query)
    .on('data', function(data) {
      if (!filter || filter(data.value, data.key)) results.push(data.value);
    })
    .on('error', function(err) {
      defer.reject(err);
    })
    .on('close', function() {
      if (defer.promise.inspect().state === 'pending') defer.resolve(results);
    })

  return defer.promise;
}

module.exports = {
  readStreamPromise: readStreamPromise
}
