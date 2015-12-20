var kdbtree = require('../')
var test = require('tape')
var fdstore = require('fd-chunk-store')
var tmpdir = require('os').tmpdir()

var path = require('path')
var file = path.join(tmpdir, 'kdb-tree-' + Math.random())

test('remove by index', function (t) {
  t.plan(9)
  var kdb = kdbtree({
    types: [ 'float32', 'float32', 'float32', 'uint32' ],
    size: 4096,
    store: fdstore(4096, file)
  })
  var points = [
    { point: [1,2,3], value: 444 },
    { point: [4,5,6], value: 333 },
    { point: [1,2,3], value: 555 },
    { point: [1,2,3], value: 222 }
  ]
  var pending = points.length
  points.forEach(function (p) {
    kdb.insert(p.point, p.value, function (err) {
      t.ifError(err)
      if (--pending === 0) check()
    })
  })
  function check () {
    kdb.query([[-9,9],[-9,9],[-9,9]], { index: true }, function (err, pts) {
      t.ifError(err)
      t.deepEqual(fields(['point','value'], pts), [
        { point: [1,2,3], value: 444 },
        { point: [4,5,6], value: 333 },
        { point: [1,2,3], value: 555 },
        { point: [1,2,3], value: 222 }
      ])
      remove(pts[2].index)
    })
  }
  function remove (ix) {
    kdb.remove({ index: ix }, function (err) {
      t.ifError(err)
      kdb.query([[-9,9],[-9,9],[-9,9]], function (err, pts) {
        t.ifError(err)
        t.deepEqual(fields(['point','value'], pts), [
          { point: [1,2,3], value: 444 },
          { point: [4,5,6], value: 333 },
          { point: [1,2,3], value: 222 }
        ])
      })
    })
  }
})

function fields (keys, pts) {
  return pts.map(function (pt) {
    var row = {}
    keys.forEach(function (key) { row[key] = pt[key] })
    return row
  })
}
