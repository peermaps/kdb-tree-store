var kdbtree = require('../')
var test = require('tape')
var fdstore = require('fd-chunk-store')
var tmpdir = require('os').tmpdir()

var path = require('path')
var file = path.join(tmpdir, 'kdb-tree-' + Math.random())

test('query', function (t) {
  t.plan(3)
  var kdb = kdbtree({
    types: [ 'float32', 'float32', 'float32' ],
    size: 4096,
    store: fdstore(4096, file),
    root: 0
  })
  kdb.insert([1,2,3,9999], function (err) {
    t.ifError(err)
    kdb.query([1,2,3], function (err, pts) {
      t.ifError(err)
      t.deepEqual(pts, [[1,2,3,9999]])
    })
  })
})
