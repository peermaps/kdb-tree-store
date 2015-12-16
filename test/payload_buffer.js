var kdbtree = require('../')
var test = require('tape')
var fdstore = require('fd-chunk-store')
var tmpdir = require('os').tmpdir()
var almostEqual = require('almost-equal')
var FLT = almostEqual.FLT_EPSILON
var randomBytes = require('crypto').randomBytes

var path = require('path')
var file = path.join(tmpdir, 'kdb-tree-' + Math.random())

test('buffer payload', function (t) {
  var n = 20
  var kdb = kdbtree({
    types: [ 'float32', 'float32', 'float32', 'buffer[64]' ],
    size: 4096,
    store: fdstore(4096, file)
  })
  var data = []
  var pending = n
  for (var i = 0; i < n; i++) (function (i) {
    var x = Math.random() * 200 - 100
    var y = Math.random() * 200 - 100
    var z = Math.random() * 200 - 100
    var value = randomBytes(64)
    data.push({ point: [x,y,z], value: value })
    kdb.insert([x,y,z], value, function (err) {
      t.ifError(err, 'insert ifError')
      kdb.query([x,y,z], function (err, pts) {
        t.ifError(err, 'query ifError')
        t.equal(pts.length, 1, 'single query result for single point')
        if (pts[0]) {
          approx(t, pts[0].point, [x,y,z])
          t.deepEqual(pts[0].value, value, 'point value')
        } else t.fail('no point')
        if (--pending === 0) check()
      })
    })
  })(i)

  function check () {
    kdb.query([[15,50],[-60,10],[50,100]], function (err, pts) {
      t.ifError(err)
      var expected = data.filter(function (p) {
        var pt = p.point
        return pt[0] >= 15 && pt[0] <= 50
          && pt[1] >= -60 && pt[1] <= 10
          && pt[2] >= 50 && pt[2] <= 100
      })
      for (var i = 0; i < Math.max(pts.length, expected.length); i++) {
        approx(t, pts[i], expected[i])
      }
      t.end()
    })
  }
})

function approx (t, a, b) {
  for (var i = 0; i < a.length; i++) {
    t.ok(almostEqual(a[i], b[i], FLT, FLT),
      'approx equal: ' + a[i] + ' ~ ' + b[i])
  }
}
