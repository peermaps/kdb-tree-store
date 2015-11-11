var kdbtree = require('../')
var test = require('tape')
var fdstore = require('fd-chunk-store')
var tmpdir = require('os').tmpdir()
var almostEqual = require('almost-equal')
var FLT = almostEqual.FLT_EPSILON

var path = require('path')
var file = path.join(tmpdir, 'kdb-tree-' + Math.random())

test('points', function (t) {
  var n = 2
  t.plan(n*(2+4+2) + 2)
  var kdb = kdbtree({
    types: [ 'float32', 'float32', 'float32' ],
    size: 4096,
    store: fdstore(4096, file),
    root: 0
  })
  var data = []
  var pending = n
  for (var i = 0; i < n; i++) {
    var x = Math.random() * 200 - 100
    var y = Math.random() * 200 - 100
    var z = Math.random() * 200 - 100
    var loc = Math.floor(Math.random() * 1000)
    data.push([x,y,z,loc])
    kdb.insert([x,y,z,loc], function (err) {
      t.ifError(err)
      kdb.query([x,y,z], function (err, pts) {
        t.ifError(err)
        approx(t, pts, [[x,y,z,loc]])
        if (--pending === 0) check()
      })
    })
  }
  function check () {
    kdb.query([[15,50],[-60,10],[50,100]], function (err, pts) {
      t.ifError(err)
      t.deepEqual(pts, data.filter(function (pt) {
        return pt[0] >= 15 && pt[0] <= 50
          && pt[1] >= -60 && pt[1] <= 10
          && pt[2] >= 50 && pt[2] <= 100
      }))
    })
  }
})

function approx (t, a, b) {
  t.equal(a.length, b.length, 'approx length')
  for (var i = 0; i < a.length; i++) {
    t.equal(a[i].length, b[i].length, 'approx length [' + i + ']')
    for (var j = 0; j < a[0].length; j++) {
      t.ok(almostEqual(a[i][j], b[i][j], FLT, FLT), 'approx equal')
    }
  }
}
