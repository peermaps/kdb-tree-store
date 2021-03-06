var median = require('median')
var once = require('once')
var Readable = require('readable-stream').Readable
var xtend = require('xtend')
var inherits = require('inherits')
var EventEmitter = require('events').EventEmitter
var overlappingBox = require('bounding-box-overlap-test')
var builtinTypes = require('comparable-storable-types')

var REGION = 0, POINTS = 1

module.exports = KDB
inherits(KDB, EventEmitter)

function KDB (opts) {
  var self = this
  this.cache = []
  if (!(this instanceof KDB)) return new KDB(opts)
  EventEmitter.call(this)
  this.store = opts.store
  this.size = opts.size || this.store.chunkLength
  this._available = opts.available || 0
  this.types = opts.types.map(function (t) {
    var bt = builtinTypes(t)
    if (bt) return bt
    if (typeof t === 'string') throw new Error('unrecognized type: ' + t)
    return t
  })
  this.dim = this.types.length - 1
  this._queue = []
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
          if (r.node === node.n) {
            return cb(new Error('corrupt data: region cycle detected'))
          }
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
            if (opts.index) p.index = [n,i]
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
  if (self.cache[n]) return process.nextTick(cb, null, self.cache[n])
  self.store.get(n, function (err, buf) {
    if (err && err.notFound) return cb(null, undefined)
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

KDB.prototype._put = function (n, node, cb, skipCache) {
  var self = this
  var buf = new Buffer(self.size)
  buf.writeUInt8(node.type, 0)
  node.n = n
  if (node.type === REGION) {
    if (!skipCache) self.cache[n] = node
    else self.cache[n] = null
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
    self.cache[n] = node
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

KDB.prototype.remove = queue(function (q, opts, cb) {
  var self = this
  if (typeof opts === 'function') {
    cb = opts
    opts = {}
  }
  if (!opts) opts = {}
  if (!Array.isArray(q) && q && typeof q === 'object') {
    opts = q
    q = []
    for (var i = 0; i < self.dim; i++) {
      var t = self.types[i]
      q.push([t.min,t.max])
    }
  }
  if (!Array.isArray(q[0])) q = q.map(function (x) { return [x,x] })
  cb = once(cb || noop)
  var removed = opts.each ? null : []

  if (opts.index) {
    return self._get(opts.index[0], function (err, node) {
      if (err) return cb(err)
      if (!node) return cb(null, removed)
      if (node.type !== POINTS) return cb(new Error('not a point node'))
      var p = node.points[opts.index[1]]
      if (removed) removed.push(p)
      else cb(null, p)
      node.points.splice(opts.index[1], 1)
      self._put(opts.index[0], node, function (err) {
        if (err) cb(err)
        else cb(null, removed)
      })
    })
  }
  var pending = 0
  var filter = opts.filter
  if (!filter && opts.value) {
    var eq = opts.value ? self.types[self.types.length-1].cmp.eq : null
    filter = function (p) { return eq(p.value, opts.value) }
  }
  if (!filter) filter = function () { return true }
  get(0, 0)

  function get (n, depth) {
    pending++
    self._get(n, function f (err, node) {
      if (err) return cb(err)
      if (!node) {}
      else if (node.type === REGION) {
        for (var i = 0; i < node.regions.length; i++) {
          var r = node.regions[i]
          if (r.node === n) {
            return cb(new Error('corrupt data: region cycle detected'))
          }
          if (self._overlappingRange(q, r.range)) {
            get(r.node, depth + 1)
          }
        }
      } else if (node.type === POINTS) {
        var rm = 0
        for (var i = 0; i < node.points.length; i++) {
          var p = node.points[i]
          if (self._overlappingPoint(q, p.point) && filter(p)) {
            rm++
            node.points.splice(i, 1)
            i--
            if (removed) removed.push(p)
            else cb(null, p)
          }
        }
        if (rm > 0) {
          return self._put(n, node, function (err) {
            if (err) cb(err)
            else if (--pending === 0) cb(null, removed)
          })
        }
      }
      if (--pending === 0) cb(null, removed)
    })
  }
})

function queue (fn) {
  return function () {
    var self = this
    var called = false
    var args = addcb([].slice.call(arguments), oninsert)
    if (self._pending++ === 0) {
      fn.apply(self, args)
    } else {
      self._queue.push([fn,args])
    }
    function done () {
      if (self._queue.length === 0) return
      var q = self._queue.shift()
      q[0].apply(self, q[1])
    }
    function oninsert (f) {
      return function () {
        if (f) f.apply(this, arguments)
        if (called) return
        called = true
        self._pending--
        done()
      }
    }
  }
}

function addcb (args, f) {
  for (var i = 0; i < args.length; i++) {
    if (typeof args[i] === 'function') {
      args[i] = f(args[i])
      return args
    }
  }
  args.push(f())
  return args
}

KDB.prototype.insert = queue(function (pt, value, cb) {
  var self = this
  cb = once(cb || noop)
  var q = [], rec = { point: pt, value: value }
  for (var i = 0; i < pt.length; i++) q.push([pt[i],pt[i]])

  self._get(0, function f (err, node) {
    if (err) cb(err)
    else if (!node) {
      node = {
        type: REGION,
        regions: [ { range: [], node: 1 } ],
        n: 0
      }
      for (var i = 0; i < self.dim; i++) {
        var t = self.types[i]
        node.regions[0].range.push([t.min,t.max])
      }
      var pts = { type: POINTS, points: [], node: 1 }
      var pending = 2
      self._put(self._alloc(), node, function (err) {
        if (--pending === 0) f(err, node)
      })
      self._put(self._alloc(), pts, function (err) {
        if (--pending === 0) f(err, node)
      })
    } else _insert(node.n, 0)
  })

  var parents = []

  function _insert (nodeIdx, depth) {
    self._get(nodeIdx, function (err, node) {
      if (err) return cb(err)
      if (node.type === REGION) {
        for (var i = 0; i < node.regions.length; i++) {
          var r = node.regions[i]
          if (r.node === node.n) {
            return cb(new Error('corrupt data: region cycle detected'))
          }
          if (self._overlappingRange(q, r.range)) {
            if (typeof r.node !== 'number') return cb(new Error('region.node should be a number'))
            parents[r.node] = { node: nodeIdx, index: i }
            return _insert(r.node, depth+1)
          }
        }
        cb(new Error('INVALID STATE'))
      } else if (node.type === POINTS) {
        if (!self._willPointsOverflow(node, 1)) {
          node.points.push({ point: pt, value: value })
          return self._put(node.n, node, cb)
        }

        // determine the axis and pivot point for the region split on this point node's parent
        var coords = []
        var axis = (depth + 1) % pt.length
        for (var i = 0; i < node.points.length; i++) {
          coords.push(node.points[i].point[axis])
        }
        var pivot = median(coords)
        if (!parents[node.n]) return cb(new Error('unexpectedly at the root node'))

        var parentNodeIdx = parents[node.n].node
        self._get(parents[node.n].node, function (err, parentNode) {
          if (self._willRegionOverflow(parentNode, 1)) {
            ;(function loop (regionEntry) {
              self._get(regionEntry.node, function (err, node) {
                if (err) return cb(err)
                if (!self._willRegionOverflow(node, 1)) {
                  return _insert(regionEntry.node, depth+1)
                }
                self._splitRegionNode(node, pivot, axis, function (err, rightRegion, leftNode) {
                  if (err) return cb(err)
                  if (regionEntry.node === 0 || self._willRegionOverflow(leftNode, 1)) {
                    regionEntry.range = self._regionRange(leftNode.regions)
                    var root = {
                      type: REGION,
                      regions: [ regionEntry, rightRegion ]
                    }
                    var pending = 2
                    var n = regionEntry.node
                    regionEntry.node = self._alloc()
                    parents[regionEntry.node] = { node: n, index: 0 }
                    parents[rightRegion.node] = { node: n, index: 1 }
                    self._put(regionEntry.node, leftNode, done)
                    self._put(n, root, done)
                    function done (err) {
                      if (err) cb(err)
                      else if (--pending === 0) _insert(n, 0)
                    }
                  } else {
                    leftNode.regions.push(rightRegion)
                    self._put(regionEntry.node, leftNode, function (err) {
                      if (err) cb(err)
                      else loop(parents[regionEntry.node])
                    })
                  }
                })
              })
            })(parents[node.n])
          } else {
            self._splitPointNode(node.n, pivot, axis, function (err, left, right) {
              if (err) return cb(err)
              var pnodeidx = parents[left.n].node
              var pix = parents[left.n].index
              self._get(pnodeidx, function (err, pnode) {
                var lrange = clone(pnode.regions[pix].range)
                var rrange = clone(pnode.regions[pix].range)
                lrange[axis][1] = pivot
                rrange[axis][0] = pivot
                var lregion = { range: lrange, node: left.n }
                var rregion = { range: rrange, node: right.n }
                pnode.regions[pix] = lregion
                pnode.regions.push(rregion)
                self._put(pnode.n, pnode, function (err) {
                  if (err) cb(err)
                  else _insert(pnode.n, depth+1)
                })
              })
            })
          }
        })
      }
    })
  }
})

KDB.prototype._splitPointNode = function (nodeIdx, pivot, axis, cb) {
  var self = this
  var right = { type: POINTS, points: [] }
  self._get(nodeIdx, function (err, node) {
    if (err) return cb(err)

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
      else if (--pending === 0) cb(null, node, right)
    }
  })
}

KDB.prototype._splitRegionNode = function (node, pivot, axis, cb) {
  var self = this

  var rightNode = {
    type: REGION,
    regions: []
  }

  var rrange = self._regionRange(node.regions)
  rrange[axis][0] = pivot
  var rightRegion = {
    range: rrange,
    node: undefined
  }

  ;(function loop (i) {
    if (i >= node.regions.length) return done()

    var r = node.regions[i]
    if (r.range[axis][1] <= pivot) {
      // already in the right place
      loop(i+1)
    } else if (r.range[axis][0] >= pivot) {
      rightNode.regions.push(r)
      node.regions.splice(i, 1)
      loop(i)
    } else {
      var rright = {
        range: clone(r.range)
      }
      rright.range[axis][0] = pivot
      rightNode.regions.push(rright)

      r.range[axis][1] = pivot
      self._get(r.node, function (err, rnode) {
        if (err) return cb(err)
        if (rnode.type === POINTS) {
          self._splitPointNode(rnode.n, pivot, axis, function (err, ln, rn) {
            if (err) return cb(err)
            rright.node = rn
            loop(i+1)
          })
        } else if (rnode.type === REGION) {
          self._splitRegionNode(rnode, pivot, axis, function (err, spr, leftNode) {
            if (err) return cb(err)
            rright.node = { type: REGION, regions: [ spr ] }
            rright.node.n = self._alloc()
            var pending = 2
            self._put(rright.node.n, rright.node, done)
            self._put(leftNode.n, leftNode, done)
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
    rightNode.n = self._alloc()
    rightRegion.node = rightNode.n
    self._put(rightNode.n, rightNode, function (err) {
      if (err) cb(err)
      else cb(null, rightRegion, node)
    }, true)
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
  return overlappingBox(a, b)
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

KDB.prototype._willRegionOverflow = function (node, spots) {
  return 3 + (node.regions.length + spots) * this._rsize > this.size
}

KDB.prototype._willPointsOverflow = function (node, spots) {
  return 3 + (node.points.length + spots) * this._psize > this.size
}

KDB.prototype._alloc = function () {
  var n = this._available++
  this.emit('available', this._available)
  return n
}

function noop () {}
