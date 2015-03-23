# Promise-based persistent task queue

Work in progress

## Usage

```js
var Queue = require('qtask');
var q = new Queue({
  throttle: 100,
  blockOnFail: true, // prevent next task from running if previous task struck out
  strikes: 3, // defaults to 3
  run: function(data) {
    // return Q.Promise or a value
    return Q.Promise(function(resolve, reject) {
      request(data.url, function(err, resp, body) {
        if (err) return reject(err);
        else return resolve(resp);
      })
    });
  },
  path: './path/to/queue.db'
});

q.push({
  url: 'http://tradle.io'
});

q.push({
  url: 'http://urbien.com'
});

q.push({
  url: 'this might not be a valid url'
});
```

## Events

### 'status:success'

Task succeeded

### 'status:fail'

Task failed

### 'status:struckout'

Task struck out

