var Readable = require('readable-stream').Readable
var collect = require('./lib/collect.js')
var dist = require('euclidean-distance')
var inherits = require('inherits')
var EventEmitter = require('events').EventEmitter

var almostEqual = require('almost-equal')
var FLT = almostEqual.FLT_EPSILON

var REGION = 0, POINT = 1

module.exports = KDB
inherits(KDB, EventEmitter)

function KDB (opts) {
  if (!(this instanceof KDB)) return new KDB(opts)
  EventEmitter.call(this)
  if (!opts.store) throw new Error('opts.store required')
  if (opts.root === undefined) throw new Error('opts.root required')
  if (!opts.types) throw new Error('opts.types required')
  if (!opts.size) throw new Error('opts.size required')
  this.store = opts.store
  this.root = opts.root
  this.types = opts.types
  this.size = opts.size
  this.free = opts.free || 0
  this._insertQueue = []
  this._pending = 0
}

KDB.prototype.query = function (rquery, cb) {
  var self = this
  var q = normq(rquery)
  var pages = [ [ self.root, 0 ] ]

  var results = new Readable({ objectMode: true })
  results._read = read
  if (cb) collect(results, cb)
  return results

  function read () {
    if (pages.length === 0) return results.push(null)
    var page = pages.shift()
    self.store.get(page[0], function (err, buf) {
      if (err) return results.emit('error', err)
      if (buf.length === 0) return results.push(null)

      if (buf[0] === REGION) {
        self._parseRegion(buf, pages, page[1])
        read()
      } else if (buf[0] === POINT) {
        var pts = self._parsePoint(buf, page[1], q)
        if (pts.length === 0) read()
        for (var i = 0; i < pts.length; i++) results.push(pts[i])
      }
    })
  }
}

KDB.prototype._parseRegion = function (buf, pages, depth) {
  var self = this
  var nregions = buf.readUInt16BE(1)
  var len = self.types.length
  var q = query[depth % len]
  var offset = 3

  for (var i = 0; i < nregions; i++) {
    var match = false
    for (var j = 0; j < len; j++) {
      var tt = self.types[j % len]
      var d = j % len === depth % len
      if (d && tt === 'float32') {
        var min = buf.readFloatBE(offset)
        var max = buf.readFloatBE(offset + 4)
        if (ltef32(q[0], min) && gte(q[1], max)) match = true
      }
      if (tt === 'float32') {
        offset += 4 + 4
      } else throw new Error('unsupported type: ' + tt)
    }
    if (match) {
      var page = buf.readUInt32BE(offset)
      pages.push([ page, depth+1 ])
    }
    offset += 4
  }
}

KDB.prototype._parsePoint = function (buf, depth, query) {
  var self = this
  var npoints = buf.readUInt16BE(1)
  var len = self.types.length
  var offset = 3
  var results = []
  for (var i = 0; i < npoints; i++) {
    var pt = []
    var m = true
    for (var j = 0; j < len; j++) {
      var t = self.types[j % len]
      if (t === 'float32') {
        var p = buf.readFloatBE(offset)
        offset += 4
      } else throw new Error('unsupported type: ' + t)
      if (!m) continue
      var qj = query.query[j]
      if (ltf32(qj[0], p) || gtf32(qj[1], p)) m = false
      pt.push(p)
    }
    if (m && query.filter(pt)) {
      pt.push(buf.readUInt32BE(offset))
      results.push(pt)
    }
    offset += 4
  }
  return results
}

function normq (q) {
  var query = [], filter = ftrue
  if (Array.isArray(q)) {
    for (var i = 0; i < q.length; i++) {
      if (Array.isArray(q[i])) {
        query.push(q[i])
      } else {
        query.push([ q[i], q[i] ]) // min, max
      }
    }
  } else if (q && q.center && q.radius) {
    var center = q.center, radius = q.radius
    for (var i = 0; i < self.types.length; i++) {
      query.push([ center[i] - radius, center[i] + radius ]) // min, max
    }
    filter = function (pt) { return dist(pt, center) <= radius }
  } else throw new Error('malformed query')
  return { query: query, filter: filter }
  function ftrue () { return true }
}

KDB.prototype.insert = function (pt, cb) {
  var self = this
  if (self._pending++ === 0) {
    self._insert(pt, oninsert(cb))
  } else {
    self._insertQueue.push([pt,cb])
  }
  function done () {
    if (self._insertQueue.length === 0) return
    var q = self._insertQueue.shift()
    self._insert(q[0], oninsert(q[1]))
  }
  function oninsert (f) {
    return function (err) {
      f(err)
      self._pending--
      done()
    }
  }
}

KDB.prototype._insert = function (pt, cb) {
  var self = this
  if (!cb) cb = noop
  var query = []
  for (var i = 0; i < pt.length - 1; i++) query.push(pt[i], pt[i])
  var pages = [ [ self.root, 0 ] ]
  var region = null, regionPage = null

  ;(function read () {
    if (pages.length === 0) return results.push(null)
    var page = pages.shift()
    self.store.get(page[0], function (err, buf) {
      if (err) return cb(err)
      if (buf.length === 0 && self.root === page[0]) {
        var pbuf = self._createPointPage()
        self._addPoint(pbuf, pt)
        return self.store.put(self.root, pbuf, cb)
      } else if (buf.length === 0) {
        return cb(new Error('empty page: ' + page[0]))
      }

      if (buf[0] === REGION) {
        self._parseRegion(buf, pages, page[1])
        region = buf
        regionPage = page[0]
        read()
      } else if (buf[0] === POINT) {
        if (self._addPoint(buf, pt)) {
          return self.store.put(page[0], buf, cb)
        }
        var sp = self._splitPointPage(buf, page[1])
        throw new Error('update region...')
        /*
        if (self._addRegion(region, sp.left, sp.right)) {
          self.store.put(self._available(), 
            sp.left
          self.store.put(regionPage, region, done)
        } else { // region overflow
          // split region, re-order
          throw new Error('handle region overflow')
        }
        */
      }
    })
  })()
}

KDB.prototype._available = function () {
  var i = self.free++
  self.emit('free', i)
}

KDB.prototype._addRegion = function (buf, left, right) {
  throw new Error('add region...')
  return true
}

KDB.prototype._splitPointPage = function (buf, depth) {
  var self = this
  var len = self.types.length
  var npoints = buf.readUInt16BE(1)
  var d = depth % len
  var points = [], coords = []
  for (var i = 0; i < npoints; i++) {
    var pt = []
    for (var j = 0; j < len; j++) {
      var t = self.types[j % len]
      if (t === 'float32') {
        var p = buf.readFloatBE(offset)
        offset += 4
      } else throw new Error('unsupported type: ' + t)
      pt.push(p)
    }
    points.push(pt)
    coords.push(pt[d])
  }
  var pivot = median(coords)
  var left = [], right = []
  for (var i = 0; i < npoints; i++) {
    if (points[i][d] < pivot) left.push(points[i])
    else right.push(points[i])
  }
  return { left: left, right: right }
}

KDB.prototype._createPointPage = function () {
  var self = this
  var buf = new Buffer(self.size)
  buf[0] = POINT
  buf.writeUInt16BE(0, 1)
  return buf
}

KDB.prototype._addPoint = function (buf, pt) {
  var self = this
  var npoints = buf.readUInt16BE(1)
  var len = self.types.length
  var offset = 3

  for (var i = 0; i < npoints; i++) {
    for (var j = 0; j < len; j++) {
      var t = self.types[j]
      if (t === 'float32') {
        offset += 4
      } else throw new Error('unknown type: ' + t)
    }
  }
  for (var j = 0; j < pt.length - 1; j++) {
    var t = self.types[j]
    if (t === 'float32') {
      if (offset > buf.length) return false // overflow
      buf.writeFloatBE(pt[j], offset)
      offset += 4
    } else throw new Error('unknown type: ' + t)
  }
  if (offset > buf.length) return false // overflow
  buf.writeUInt32BE(pt[j], offset)
  buf.writeUInt16BE(1, npoints+1)
  return true // no overflow
}

function noop () {}
function ltf32 (a, b) { return a < b && !almostEqual(a, b, FLT, FLT) }
function ltef32 (a, b) { return a < b || almostEqual(a, b, FLT, FLT) }
function gtf32 (a, b) { return a > b && !almostEqual(a, b, FLT, FLT) }
function gtef32 (a, b) { return a > b || almostEqual(a, b, FLT, FLT) }
