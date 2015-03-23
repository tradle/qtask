
var Q = require('q');
var manifest = require('level-manifest');
var dbUtils = require('./dbutils');

module.exports = function promisify(db) {
  var man = manifest(db);
  var methods = man.methods;
  // methods.query = man.query || { type: 'async' };

  for (var methodName in methods) {
    if (methods[methodName].type === 'async') {
      db[methodName] = Q.nbind(db[methodName], db);
    }
  }

  db.readStreamPromise = dbUtils.readStreamPromise.bind(dbUtils, db);

  if (db.sublevels) {
    for(var name in db.sublevels) {
      promisify(db.sublevels[name]);
    }
  }
}
