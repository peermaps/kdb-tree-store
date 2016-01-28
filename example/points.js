var kdbtree = require('../')
var fdstore = require('fd-chunk-store')
var tmpdir = require('os').tmpdir()

var path = require('path')
var file = path.join(tmpdir, 'kdb-tree-' + Math.random())

var n = 5000
var kdb = kdbtree({
  types: [ 'float32', 'float32', 'float32', 'uint32' ],
  store: fdstore(1024, file)
})
var pending = n
for (var i = 0; i < n; i++) (function () {
  var x = Math.random() * 200 - 100
  var y = Math.random() * 200 - 100
  var z = Math.random() * 200 - 100
  var loc = Math.floor(Math.random() * 1000)
  kdb.insert([x,y,z], loc, function (err) {
    if (--pending === 0) check()
  })
})()

function check () {
  kdb.query([[-100,0],[0,5],[-50,-40]], function (err, pts) {
    console.log(pts)
  })
}
