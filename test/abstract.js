var test = require('tape')
var suite = require('abstract-point-store/test')
var kdbtree = require('..')
var fdstore = require('fd-chunk-store')
var tmpdir = require('os').tmpdir()
var path = require('path')

suite(test, kdbtree, makeStore)

function makeStore() {
  var file = path.join(tmpdir, 'kdb-tree-' + Math.random())
  return fdstore(1024, file)
}
