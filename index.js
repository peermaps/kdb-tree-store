var median = require('median')
var once = require('once')
var Readable = require('readable-stream').Readable
var xtend = require('xtend')
var inherits = require('inherits')
var EventEmitter = require('events').EventEmitter

var REGION = 0, POINTS = 1
var builtinTypes = require('./types.js')

module.exports = KDB
inherits(KDB, EventEmitter)

function KDB (opts) {
  var self = this
  if (!(this instanceof KDB)) return new KDB(opts)
  EventEmitter.call(this)
  this.store = opts.store
  this.size = opts.size
  this._available = opts.available || 0
  this.types = opts.types.map(function (t) {
    var bt = builtinTypes(t)
    if (bt) return bt
    if (typeof t === 'string') throw new Error('unrecognized type: ' + t)
    return t
  })
  this.dim = this.types.length - 1
  this._insertQueue = []
  this._pending = 0

  this._rsize = 4
  for (var i = 0; i < this.dim; i++) this._rsize += this.types[i].size * 2
  this._psize = this.types[this.dim].size
  for (var i = 0; i < this.dim; i++) this._psize += this.types[i].size
}

KDB.prototype.query = function (q, opts, cb) {
  var self = this
  if (typeof opts === 'function') {
    cb = opts
    opts = {}
  }
  if (!opts) opts = {}
  if (!cb) cb = noop
  if (!opts.each) cb = once(cb)
  if (!Array.isArray(q[0])) q = q.map(function (x) { return [x,x] })

  var pending = 1
  var results = opts.each ? null : []
  get(0, 0)

  function get (n, depth) {
    self._get(n, function f (err, node) {
      if (err) return cb(err)
      if (!node) node = { type: REGION, regions: [] }
      if (node.type === REGION) {
        for (var i = 0; i < node.regions.length; i++) {
          var r = node.regions[i]
          if (self._overlappingRange(q, r.range)) {
            pending++
            get(r.node, depth + 1)
          }
        }
      } else if (node.type === POINTS) {
        for (var i = 0; i < node.points.length; i++) {
          var p = node.points[i]
          if (self._overlappingPoint(q, p.point)) {
            if (opts.depth) p.depth = depth
            if (results) results.push(p)
            else cb(null, p)
          }
        }
      }
      if (--pending === 0) cb(null, results)
    })
  }
}

KDB.prototype.queryStream = function (q, opts) {
  var self = this
  if (!opts) opts = {}
  var stream = new Readable({ objectMode: true })
  stream._read = function () {}
  self.query(q, xtend(opts, { each: true }), function (err, p) {
    if (err) stream.emit('error', err)
    else if (p) stream.push(p)
    else stream.push(null)
  })
  return stream
}

KDB.prototype._get = function (n, cb) {
  var self = this
  self.store.get(n, function (err, buf) {
    if (err) return cb(err)
    if (buf.length === 0) return cb(null, undefined)
    var node = { type: buf[0], n: n }
    if (node.type === REGION) {
      node.regions = []
      var nregions = buf.readUInt16BE(1)
      var offset = 3
      for (var i = 0; i < nregions; i++) {
        var range = []
        for (var j = 0; j < self.dim; j++) {
          var t = self.types[j]
          var min = t.read(buf, offset)
          offset += t.size
          var max = t.read(buf, offset)
          offset += t.size
          range.push([ min, max ])
        }
        node.regions.push({
          range: range,
          node: buf.readUInt32BE(offset)
        })
        offset += 4
      }
      cb(null, node)
    } else if (node.type === POINTS) {
      node.points = []
      var npoints = buf.readUInt16BE(1)
      var offset = 3
      for (var i = 0; i < npoints; i++) {
        var pt = []
        for (var j = 0; j < self.dim; j++) {
          var t = self.types[j]
          var coord = t.read(buf, offset)
          offset += t.size
          pt.push(coord)
        }
        var t = self.types[j]
        node.points.push({
          point: pt,
          value: t.read(buf, offset)
        })
        offset += t.size
      }
      cb(null, node)
    } else cb(new Error('unknown type: ' + node.type))
  })
}

KDB.prototype._put = function (n, node, cb) {
  var self = this
  var buf = new Buffer(self.size)
  buf.writeUInt8(node.type, 0)
  if (node.type === REGION) {
    var len = node.regions.length
    buf.writeUInt16BE(len, 1)
    var offset = 3
    for (var i = 0; i < len; i++) {
      for (var j = 0; j < self.dim; j++) {
        var t = self.types[j]
        t.write(buf, node.regions[i].range[j][0], offset)
        offset += t.size
        t.write(buf, node.regions[i].range[j][1], offset)
        offset += t.size
      }
      var rn = node.regions[i].node
      buf.writeUInt32BE(typeof rn === 'number' ? rn : rn.n, offset)
      offset += 4
    }
  } else if (node.type === POINTS) {
    var len = node.points.length
    buf.writeUInt16BE(len, 1)
    var offset = 3
    for (var i = 0; i < len; i++) {
      for (var j = 0; j < self.dim; j++) {
        var t = self.types[j]
        t.write(buf, node.points[i].point[j], offset)
        offset += t.size
      }
      var t = self.types[j]
      t.write(buf, node.points[i].value, offset)
      offset += t.size
    }
  } else cb(new Error('unknown type: ' + node.type))
  self.store.put(n, buf, cb)
}

KDB.prototype.insert = function (pt, value, cb) {
  var self = this
  if (self._pending++ === 0) {
    self._insert(pt, value, oninsert(cb))
  } else {
    self._insertQueue.push([pt,value,cb])
  }
  function done () {
    if (self._insertQueue.length === 0) return
    var q = self._insertQueue.shift()
    self._insert(q[0], q[1], oninsert(q[2]))
  }
  function oninsert (f) {
    return function (err) {
      f(err)
      self._pending--
      done()
    }
  }
}

KDB.prototype._insert = function (pt, value, cb) {
  var self = this
  cb = once(cb || noop)
  var q = [], rec = { point: pt, value: value }
  for (var i = 0; i < pt.length; i++) q.push([pt[i],pt[i]])

  self._get(0, function f (err, node) {
    if (err) cb(err)
    else if (!node) {
      node = {
        type: REGION,
        regions: [ { range: [], node: 1 } ]
      }
      for (var i = 0; i < self.dim; i++) {
        var t = self.types[i]
        node.regions[0].range.push([t.min,t.max])
      }
      var pts = { type: POINTS, points: [] }
      var pending = 2
      self._put(self._alloc(), node, function (err) {
        if (--pending === 0) f(err, node)
      })
      self._put(self._alloc(), pts, function (err) {
        if (--pending === 0) f(err, node)
      })
    } else insert(node, 0)
  })

  function insert (node, depth) {
    if (node.type === REGION) {
      for (var i = 0; i < node.regions.length; i++) {
        var r = node.regions[i]
        if (self._overlappingRange(q, r.range)) {
          if (typeof r.node === 'number') {
            self._get(r.node, function (err, rnode) {
              rnode.parent = { node: node, index: i }
              insert(rnode, depth+1)
            })
          } else {
            r.node.parent = { node: node, index: i }
            insert(r.node, depth+1)
          }
          return
        }
      }
      cb(new Error('INVALID STATE'))
    } else if (node.type === POINTS) {
      if (3 + (node.points.length + 1) * self._psize < self.size) {
        node.points.push({ point: pt, value: value })
        return self._put(node.n, node, cb)
      }

      var coords = []
      var axis = (depth + 1) % pt.length
      for (var i = 0; i < node.points.length; i++) {
        coords.push(node.points[i].point[axis])
      }
      var pivot = median(coords)
      if (!node.parent) return cb(new Error('unexpectedly at the root node'))

      if (self._willOverflow(node.parent.node, 1)) {
        ;(function loop (p) {
          if (!self._willOverflow(p.node, 1)) {
            return insert(p.node, depth+1)
          }
          self._splitRegionNode(p, pivot, axis, function (err, right) {
            if (err) return cb(err)
            if (p.node.n === 0 || self._willOverflow(p.node, 1)) {
              p.range = self._regionRange(p.node.regions)
              var root = {
                type: REGION,
                regions: [ p, right ]
              }
              var pending = 2
              var n = p.node.n
              p.node.n = self._alloc()
              p.node.parent = root
              right.node.parent = root
              self._put(p.node.n, p.node, done)
              self._put(n, root, done)
              function done (err) {
                if (err) cb(err)
                else if (--pending === 0) insert(root, 0)
              }
            } else {
              p.node.regions.push(right)
              self._put(p.node.n, p.node, function (err) {
                if (err) cb(err)
                else loop(p.node.parent)
              })
            }
          })
        })(node.parent)
      } else {
        self._splitPointNode(node, pivot, axis, function (err, right) {
          if (err) return cb(err)
          var pnode = node.parent.node
          var pix = node.parent.index
          var lrange = clone(pnode.regions[pix].range)
          var rrange = clone(pnode.regions[pix].range)
          lrange[axis][1] = pivot
          rrange[axis][0] = pivot
          var lregion = { range: lrange, node: node.n }
          var rregion = { range: rrange, node: right.n }
          pnode.regions[pix] = lregion
          pnode.regions.push(rregion)
          self._put(pnode.n, pnode, function (err) {
            if (err) cb(err)
            else insert(pnode, depth+1)
          })
        })
      }
    }
  }
}

KDB.prototype._splitPointNode = function (node, pivot, axis, cb) {
  var self = this
  var right = { type: POINTS, points: [] }
  for (var i = 0; i < node.points.length; i++) {
    var p = node.points[i]
    if (p.point[axis] >= pivot) {
      right.points.push(p)
      node.points.splice(i, 1)
      i--
    }
  }
  right.n = self._alloc()
  var pending = 2
  self._put(right.n, right, onput)
  self._put(node.n, node, onput)
  function onput (err) {
    if (err) cb(err)
    else if (--pending === 0) cb(null, right)
  }
}

KDB.prototype._splitRegionNode = function (node, pivot, axis, cb) {
  var self = this
  var rrange = self._regionRange(node.node.regions)
  rrange[axis][0] = pivot

  var right = {
    range: rrange,
    node: {
      type: REGION,
      regions: []
    }
  }
  var left = node

  ;(function loop (i) {
    if (i >= node.node.regions.length) return done()

    var r = node.node.regions[i]
    if (r.range[axis][1] <= pivot) {
      // already in the right place
      loop(i+1)
    } else if (r.range[axis][0] >= pivot) {
      right.node.regions.push(r)
      left.node.regions.splice(i, 1)
      loop(i)
    } else {
      var rright = {
        range: clone(r.range)
      }
      rright.range[axis][0] = pivot
      right.node.regions.push(rright)

      var rleft = r
      rleft.range[axis][1] = pivot
      self._get(r.node, function (err, rnode) {
        if (err) return cb(err)
        if (rnode.type === POINTS) {
          self._splitPointNode(rnode, pivot, axis, function (err, rn) {
            if (err) return cb(err)
            rright.node = rn
            loop(i+1)
          })
        } else if (rnode.type === REGION) {
          r.node = rnode
          self._splitRegionNode(r, pivot, axis, function (err, spr) {
            if (err) return cb(err)
            rright.node = { type: REGION, regions: [ spr ] }
            rright.node.n = self._alloc()
            var pending = 2
            self._put(rright.node.n, rright.node, done)
            self._put(rnode.n, rnode, done)
            function done (err) {
              if (err) cb(err)
              else if (--pending === 0) loop(i+1)
            }
          })
        } else return cb(new Error('unknown type: ' + rnode.type))
      })
    }
  })(0)

  function done () {
    right.node.n = self._alloc()
    self._put(right.node.n, right.node, function (err) {
      if (err) cb(err)
      else cb(null, right)
    })
  }
}

KDB.prototype._overlappingPoint = function (a, p) {
  for (var i = 0; i < a.length; i++) {
    var cmp = this.types[i].cmp
    if (!overlappingmm(cmp, a[i][0], a[i][1], p[i], p[i])) return false
  }
  return true
}

function overlappingmm (cmp, amin, amax, bmin, bmax) {
  return (cmp.gte(amin, bmin) && cmp.lte(amin, bmax))
    || (cmp.gte(amax, bmin) && cmp.lte(amax <= bmax))
    || (cmp.lt(amin, bmin) && cmp.gt(amax, bmax))
}

KDB.prototype._overlappingRange = function (a, b) {
  for (var i = 0; i < a.length; i++) {
    var cmp = this.types[i].cmp
    if (!overlapping(cmp, a[i], b[i])) return false
  }
  return true
}

function overlapping (cmp, a, b) {
  return (cmp.gte(a[0], b[0]) && cmp.lte(a[0], b[1]))
    || (cmp.gte(a[1], b[0]) && cmp.lte(a[1], b[1]))
    || (cmp.lt(a[0], b[0]) && cmp.gt(a[1], b[1]))
}

function clone (xs) {
  return xs.map(function (x) { return x.slice() })
}

KDB.prototype._regionRange = function (regions) {
  var self = this
  var range = []
  for (var j = 0; j < self.dim; j++) {
    var t = self.types[j]
    var r0 = regions.length === 0
      ? [t.min,t.max]
      : regions[0].range[j]
    range[j] = [r0[0],r0[1]]
    for (var i = 1; i < regions.length; i++) {
      var r = regions[i].range
      if (r[j][0] < range[j][0]) range[j][0] = r[j][0]
      if (r[j][1] > range[j][1]) range[j][1] = r[j][1]
    }
  }
  return range
}

KDB.prototype._willOverflow = function (node, spots) {
  return 3 + (node.regions.length + spots) * this._rsize > this.size
}

KDB.prototype._alloc = function () {
  var n = this._available++
  this.emit('available', this._available)
  return n
}

function noop () {}
