var kdbtree = require('../')
var test = require('tape')
var fdstore = require('fd-chunk-store')
var tmpdir = require('os').tmpdir()

var path = require('path')
var file = path.join(tmpdir, 'kdb-tree-' + Math.random())

test('insert/remove update', function (t) {
  t.plan(9)
  var kdb = kdbtree({
    types: [ 'float32', 'float32', 'float32', 'uint32' ],
    size: 4096,
    store: fdstore(4096, file)
  })
  var points = [
    { point: [1,2,3], value: 444 },
    { point: [4,5,6], value: 333 },
    { point: [1,2,3], value: 555 }
  ]
  var pending = points.length
  points.forEach(function (p) {
    kdb.insert(p.point, p.value, function (err) {
      t.ifError(err)
      if (--pending === 0) check1()
    })
  })
  function check1 () {
    kdb.query([[-9,9],[-9,9],[-9,9]], function (err, pts) {
      t.ifError(err)
      t.deepEqual(pts, [
        { point: [1,2,3], value: 444 },
        { point: [4,5,6], value: 333 },
        { point: [1,2,3], value: 555 }
      ])
      remove()
    })
  }
  function remove () {
    kdb.remove([1,2,3], function (err) {
      t.ifError(err)
      kdb.insert([1,2,3], 999, function (err) {
        t.ifError(err)
        check2()
      })
    })
  }
  function check2 () {
    kdb.query([[-9,9],[-9,9],[-9,9]], function (err, pts) {
      t.ifError(err)
      t.deepEqual(pts, [
        { point: [4,5,6], value: 333 },
        { point: [1,2,3], value: 999 }
      ])
    })
  }
})
