var Readable = require('readable-stream').Readable
var collect = require('./lib/collect.js')
var dist = require('euclidean-distance')
var inherits = require('inherits')
var EventEmitter = require('events').EventEmitter
var median = require('median')
var once = require('once')

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

  this._ptsize = 4
  this._rsize = 4
  this._maxext = []
  for (var i = 0; i < this.types.length; i++) {
    var t = this.types[i]
    if (t === 'float32') {
      this._ptsize += 4
      this._rsize += 8
      this._maxext.push([-Infinity, Infinity])
    } else throw new Error('unhandled type: ' + t)
  }
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
        self._parseRegion(buf, q.query, pages, page[1])
        read()
      } else if (buf[0] === POINT) {
        var pts = self._parsePoints(buf, page[1], q)
        if (pts.length === 0) read()
        for (var i = 0; i < pts.length; i++) results.push(pts[i])
      }
    })
  }
}

KDB.prototype._parseRegion = function (buf, query, pages, depth) {
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
        if (contains(q[0], q[1], min, max)) match = true
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

KDB.prototype._parsePoints = function (buf, depth, query) {
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
      if (ltf32(p, qj[0]) || gtf32(p, qj[1])) m = false
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

KDB.prototype._median = function (buf, d) {
  var self = this
  var npoints = buf.readUInt16BE(1)
  var len = self.types.length
  var offset = 3
  var coords = []
  if (buf[0] !== REGION) throw new Error('non-region median')

  for (var i = 0; i < npoints; i++) {
    for (var j = 0; j < len; j++) {
      var t = self.types[j % len]
      if (t === 'float32') {
        if (j === d) {
          var min = buf.readFloatBE(offset)
          var max = buf.readFloatBE(offset+4)
          if (min > -Infinity && max < Infinity) coords.push((min+max)/2)
        }
        offset += 8
      } else throw new Error('unsupported type: ' + t)
    }
    offset += 4
  }
  return coords.length ? median(coords) : 0
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
  cb = once(cb || noop)
  var query = []
  for (var i = 0; i < pt.length - 1; i++) query.push([pt[i], pt[i]])
  var pages = [ [ self.root, 0 ] ]
  var len = self.types.length
  var regions = []
  var pending = 0

  ;(function read () {
    if (pages.length === 0) return cb(null)
    var page = pages.shift()
    self.store.get(page[0], function (err, buf) {
      if (err) return cb(err)
      if (buf.length === 0 && self.root === page[0]) {
        var rbuf = self._createRegionPage()
        var pbuf = self._createPointPage()
        self._available()
        var npt = self._available()
        self._addRegions(rbuf, [[npt,self._maxext]])
        self._addPoints(pbuf, [pt])
        var done = ndone(2, cb)
        self.store.put(self.root, rbuf, done)
        return self.store.put(npt, pbuf, done)
      } else if (buf.length === 0) {
        return cb(new Error('empty page: ' + page[0]))
      }
      if (buf[0] === REGION) {
        self._parseRegion(buf, query, pages, page[1])
        regions.push([buf,page[0]])
        read()
      } else if (buf[0] === POINT) {
        handlePoint(buf, page)
      } else cb(new Error('unrecognized page type: ' + buf[0]))
    })
  })()

  function handlePoint (buf, page) {
    if (self._addPoints(buf, [pt])) {
      return self.store.put(page[0], buf, cb) // no overflow
    }
    if (!regions.length) return cb(new Error('expected parent region page'))
    var lr = regions[regions.length-1]
    var sp = self._splitPointPage(buf, page[1])
    var d = (page[1]-1+len) % len

    if (pt[d] < sp.pivot) sp.left.push(pt)
    else sp.right.push(pt)

    var lbuf = self._createPointPage()
    var rbuf = self._createPointPage()
    self._addPoints(lbuf, sp.left)
    self._addPoints(rbuf, sp.right)

    var left = page[0], right = self._available()
    var r = self._removeRegion(lr[0], page[0])

    var exleft = [], exright = []
    for (var i = 0; i < len; i++) {
      if (i === d) {
        exleft.push([r[i][0], sp.pivot])
        exright.push([sp.pivot, r[i][1]])
      } else {
        exleft.push(r[i])
        exright.push(r[i])
      }
    }

    if (self._addRegions(lr[0], [[left,exleft], [right,exright]])) {
      var done = ndone(3, cb)
      self.store.put(lr[1], lr[0], done)
      self.store.put(left, lbuf, done)
      self.store.put(right, rbuf, done)
    } else {
      var dim = (page[0]+1) % len
      self._split(regions, dim, cb)
    }
  }
}

KDB.prototype._available = function () {
  var i = this.free++
  this.emit('free', i)
  return i
}

KDB.prototype._split = function (regions, axis, cb) {
  var self = this
  var len = self.types.length
  var pivot = self._median(regions[regions.length-1][0], axis)

  ;(function split (regions, n, cb) {
    self.store.get(n, function (err, buf) {
      if (err) return cb(err)
      if (buf[0] === POINT) {
        var spp = self._splitPointPage(buf, axis, pivot)
        var lbuf = self._createPointPage()
        var rbuf = self._createPointPage()
        var ln = n, rn = self._available()
        self._addPoints(lbuf, spp.left)
        self._addPoints(rbuf, spp.right)
        var done = ndone(2, function (err) {
          if (err) return cb(err)
          cb(null,
            spp.left.length ? [ln,extents(spp.left)] : null,
            spp.right.length ? [rn,extents(spp.right)] : null
          )
        })
        self.store.put(ln, lbuf, done)
        self.store.put(rn, rbuf, done)
        return
      }
console.log('pivot=', pivot)
      var sp = self._splitRegionPage(buf, axis, pivot)
      console.log(sp)
      var lbuf = self._createRegionPage()
      var rbuf = self._createRegionPage()

      if (sp.center.length === 0) {
        throw new Error('handle zero center case')
      }
      var done = ndone(sp.center.length, function (err) {
        console.log('done with recursive split!')
        if (!self._addRegions(lbuf, sp.left)) {
          throw new Error('left overflow!')
        }
        if (!self._addRegions(rbuf, sp.right)) {
          throw new Error('right overflow!')
        }
        var ln = n, rn = self._available()
        var rs = [[ln,extents(sp.left)],[rn,extents(sp.right)]]
        var r = regions[regions.length-1]
        if (self._addRegions(r[0], rs)) {
          return self.store.put(r[0], r[1], cb)
        } else {
          console.log(JSON.stringify(rs))
          throw new Error('recurse!')
        }
      })
      var nregions = regions.concat([[n,buf]])
      sp.center.forEach(function (c) {
        split(nregions, c[0], function (err, lrs, rrs) {
          if (err) return done(err)
          if (lrs) sp.left.push(lrs)
          if (rrs) sp.right.push(rrs)
          done()
        })
      })
    })
  })(regions, regions.length-1, cb)
}

KDB.prototype._addRegions = function (buf, regions) {
  var self = this
  var nregions = buf.readUInt16BE(1)
  var offset = 3 + nregions * self._rsize
  if (offset + self._rsize * regions.length > buf.length) {
    return false // overflow
  }
  for (var i = 0; i < regions.length; i++) {
    var r = regions[i], ex = r[1]
    for (var j = 0; j < ex.length; j++) {
      var t = self.types[j]
      if (t === 'float32') {
        buf.writeFloatBE(ex[j][0], offset)
        buf.writeFloatBE(ex[j][1], offset+4)
        offset += 8
      } else throw new Error('unhandled type: ' + t)
    }
    buf.writeUInt32BE(r[0], offset)
    offset += 4
  }
  buf.writeUInt16BE(nregions + regions.length, 1)
  return true
}

KDB.prototype._removeRegion = function (buf, loc) {
  var self = this
  var len = self.types.length
  var nregions = buf.readUInt16BE(1)
  var offset = 3
  for (var i = 0; i < nregions; i++) {
    if (buf.readUInt32BE(offset + self._rsize - 4) === loc) {
      var r = []
      for (var j = 0; j < len; j++) {
        var t = self.types[j]
        if (t === 'float32') {
          var min = buf.readFloatBE(offset)
          var max = buf.readFloatBE(offset+4)
          r.push([min, max])
          offset += 8
        } else throw new Error('unhandled type: ' + t)
      }
      offset += 4
      buf.slice(offset).copy(buf, offset - self._rsize)
      buf.writeUInt16BE(nregions-1, 1)
      return r
    }
    offset += self._rsize
  }
}

KDB.prototype._splitPointPage = function (buf, depth, pivot) {
  var self = this
  var len = self.types.length
  var offset = 3
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
    pt.push(buf.readUInt32BE(offset))
    offset += 4
    points.push(pt)
    coords.push(pt[d])
  }
  if (pivot === undefined) pivot = median(coords)

  var left = [], right = []
  for (var i = 0; i < npoints; i++) {
    if (points[i][d] < pivot) left.push(points[i])
    else right.push(points[i])
  }
  return { left: left, right: right, pivot: pivot }
}

KDB.prototype._splitRegionPage = function (buf, axis, pivot) {
  var self = this
  var len = self.types.length
  var offset = 3
  var nregions = buf.readUInt16BE(1)

  var left = [], right = [], center = []
  for (var i = 0; i < nregions; i++) {
    var region = []
    for (var j = 0; j < len; j++) {
      var t = self.types[j]
      if (t === 'float32') {
        var min = buf.readFloatBE(offset)
        var max = buf.readFloatBE(offset+4)
        region.push([min,max])
        offset += 8
      } else throw new Error('unsupported type: ' + t)
    }
    var r = [ buf.readUInt32BE(offset), region ]
    offset += 4
    if (ltf32(region[axis][0], pivot) && ltf32(region[axis][1], pivot)) {
      left.push(r)
    } else if (gtf32(region[axis][0], pivot) && gtf32(region[axis][1], pivot)) {
      right.push(r)
    } else {
      center.push(r)
    }
  }
  return { left: left, right: right, center: center }
}

KDB.prototype._createPointPage = function () {
  var self = this
  var buf = new Buffer(self.size)
  buf[0] = POINT
  buf.writeUInt16BE(0, 1)
  return buf
}

KDB.prototype._createRegionPage = function (left, right) {
  var self = this
  var buf = new Buffer(self.size)
  buf[0] = REGION
  buf.writeUInt32BE(0, 1)
  return buf
}

KDB.prototype._addPoints = function (buf, pts) {
  var self = this
  var npoints = buf.readUInt16BE(1)
  var len = self.types.length
  var offset = 3 + npoints * self._ptsize
  if (offset + pts.length * self._ptsize > buf.length) return false // overflow

  for (var i = 0; i < pts.length; i++) {
    var pt = pts[i]
    for (var j = 0; j < pt.length - 1; j++) {
      var t = self.types[j]
      if (t === 'float32') {
        buf.writeFloatBE(pt[j], offset)
        offset += 4
      } else throw new Error('unknown type: ' + t)
    }
    buf.writeUInt32BE(pt[j], offset)
    offset += 4
  }
  buf.writeUInt16BE(npoints + pts.length, 1)
  return true // no overflow
}

function noop () {}
function ltf32 (a, b) { return a < b && !almostEqual(a, b, FLT, FLT) }
function ltef32 (a, b) { return a <= b || almostEqual(a, b, FLT, FLT) }
function gtf32 (a, b) { return a > b && !almostEqual(a, b, FLT, FLT) }
function gtef32 (a, b) { return a >= b || almostEqual(a, b, FLT, FLT) }

function extents (pts) {
  var bbox = []
  var len = pts[0].length - 1
  for (var i = 0; i < len; i++) {
    bbox[i] = [ pts[0][i], pts[0][i] ]
  }
  for (var i = 1; i < pts.length; i++) {
    for (var j = 0; j < len; j++) {
      if (pts[i][j] < bbox[j][0]) bbox[j][0] = pts[i][j]
      if (pts[i][j] > bbox[j][1]) bbox[j][1] = pts[i][j]
    }
  }
  return bbox
}

function contains (qmin, qmax, min, max) {
  return (gtef32(qmin, min) && ltef32(qmin, max))
    || (gtef32(qmax, min) && ltef32(qmax, max))
    || (ltf32(qmin, min) && gtf32(qmax, max))
}

function ndone (n, cb) {
  cb = once(cb || noop)
  return function (err) {
    if (err) cb(err)
    else if (--n === 0) cb(null)
  }
}
