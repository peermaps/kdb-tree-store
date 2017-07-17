var kdbtree = require('../')
var test = require('tape')
var fdstore = require('fd-chunk-store')
var memstore = require('memory-chunk-store')
var tmpdir = require('os').tmpdir()

var path = require('path')
var file = path.join(tmpdir, 'kdb-tree-' + Math.random())

test('one point', function (t) {
  t.plan(3)
  var kdb = kdbtree({
    types: [ 'float32', 'float32', 'float32', 'uint32' ],
    size: 4096,
    store: fdstore(4096, file)
  })
  kdb.insert([1,2,3], 9999, function (err) {
    t.ifError(err)
    kdb.query([1,2,3], function (err, pts) {
      t.ifError(err)
      t.deepEqual(pts, [ { point: [1,2,3], value: 9999 } ])
    })
  })
})

test('one point: memory-chunk-store backend', function (t) {
  t.plan(3)
  var kdb = kdbtree({
    types: [ 'float32', 'float32', 'float32', 'uint32' ],
    size: 4096,
    store: memstore(4096)
  })
  kdb.insert([1,2,3], 9999, function (err) {
    t.ifError(err)
    kdb.query([1,2,3], function (err, pts) {
      t.ifError(err)
      t.deepEqual(pts, [ { point: [1,2,3], value: 9999 } ])
    })
  })
})
