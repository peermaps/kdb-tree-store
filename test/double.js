var kdbtree = require('../')
var test = require('tape')
var fdstore = require('fd-chunk-store')
var tmpdir = require('os').tmpdir()

var path = require('path')
var file = path.join(tmpdir, 'kdb-tree-' + Math.random())

test('double point', function (t) {
  t.plan(4)
  var kdb = kdbtree({
    types: [ 'float32', 'float32', 'float32', 'uint32' ],
    size: 4096,
    store: fdstore(4096, file)
  })
  kdb.insert([1,2,3], 444, function (err) {
    t.ifError(err)
    kdb.insert([1,2,3], 555, function (err) {
      t.ifError(err)
      kdb.query([1,2,3], function (err, pts) {
        t.ifError(err)
        t.deepEqual(pts, [
          { point: [1,2,3], value: 444 },
          { point: [1,2,3], value: 555 }
        ])
      })
    })
  })
})
