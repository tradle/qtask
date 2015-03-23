var Q = require('q');

function wait(millis) {
  return Q.Promise(function(resolve) {
    setTimeout(resolve, millis);
  });
}

module.exports = wait;
