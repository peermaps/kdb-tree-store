var kdbtree = require('../')
var test = require('tape')
var fdstore = require('fd-chunk-store')
var tmpdir = require('os').tmpdir()
var almostEqual = require('almost-equal')
var FLT = almostEqual.FLT_EPSILON

var path = require('path')
var file = path.join(tmpdir, 'kdb-tree-' + Math.random())

test('remove', function (t) {
  t.plan(7)
  var kdb = kdbtree({
    types: [ 'float32', 'float32', 'float32', 'uint32' ],
    size: 4096,
    store: fdstore(4096, file)
  })
  kdb.insert([ 1, 2, 3 ], 333, function (err) {
    t.ifError(err)
    kdb.insert([ -1, 0, -2 ], 444, function (err) {
      t.ifError(err)
      kdb.query([[-5, 5],[-5,5],[-5,5]], function (err, pts) {
        t.ifError(err)
        t.deepEqual(pts, [
          { point: [ 1, 2, 3 ], value: 333 },
          { point: [ -1, 0, -2 ], value: 444 }
        ])
        remove()
      })
    })
  })
  function remove () {
    kdb.remove([ 1, 2, 3 ], function (err) {
      t.ifError(err)
      kdb.query([[-5, 5],[-5,5],[-5,5]], function (err, pts) {
        t.ifError(err)
        t.deepEqual(pts, [
          { point: [ -1, 0, -2 ], value: 444 }
        ])
      })
    })
  }
})
