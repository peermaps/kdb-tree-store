var kdbtree = require('../')
var test = require('tape')
var fdstore = require('fd-chunk-store')
var tmpdir = require('os').tmpdir()
var almostEqual = require('almost-equal')
var FLT = almostEqual.FLT_EPSILON

var path = require('path')
var file = path.join(tmpdir, 'kdb-tree-' + Math.random())

test('overflow', function (t) {
  var n = 200
  t.plan(n*(2+4+2) + 2)
  var kdb = kdbtree({
    types: [ 'float32', 'float32', 'float32' ],
    size: 256,
    store: fdstore(256, file),
    root: 0
  })
  var data = []
  var pending = n
  for (var i = 0; i < n; i++) (function () {
    var x = Math.random() * 200 - 100
    var y = Math.random() * 200 - 100
    var z = Math.random() * 200 - 100
    var loc = Math.floor(Math.random() * 1000)
    data.push([x,y,z,loc])
    kdb.insert([x,y,z,loc], function (err) {
      t.ifError(err)
      kdb.query([x,y,z], function (err, pts) {
        t.ifError(err)
        t.equal(pts.length, 1)
        approx(t, pts[0], [x,y,z,loc])
        t.equal(pts[0][3], loc)
        if (--pending === 0) check()
      })
    })
  })()

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
  for (var i = 0; i < a.length; i++) {
    t.ok(almostEqual(a[i], b[i], FLT, FLT),
      'approx equal: ' + a[i] + ' ~ ' + b[i])
  }
}
