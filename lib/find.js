
function find(arr, filter) {
  var idx;
  arr.some(arr, function(item, i) {
    if (filter(item)) {
      idx = i;
      return true;
    }
  });

  return idx;
}

module.exports = find;
