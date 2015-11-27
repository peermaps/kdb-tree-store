var median = require('median')
var once = require('once')
var almostEqual = require('almost-equal')
var FLT = almostEqual.FLT_EPSILON

var REGION = 0, POINTS = 1

module.exports = KDB

function KDB (opts) {
  var self = this
  if (!(this instanceof KDB)) return new KDB(opts)
  this.a = opts.a || 4 // points
  this.b = opts.b || 3 // regions
  this.store = opts.store
  this.size = opts.size
  this._available = 0
  this.types = opts.types.map(function (t) {
    if (t === 'float32') {
      return {
        read: function (buf, offset) {
          return {
            value: buf.readFloatBE(offset),
            size: 4
          }
        },
        write: function (buf, value, offset) {
          buf.writeFloatBE(value, offset)
          return 4
        },
        cmp: {
          lt: function (a, b) { return a < b && !almostEqual(a, b, FLT, FLT) },
          lte: function (a, b) { return a <= b || almostEqual(a, b, FLT, FLT) },
          gt: function (a, b) { return a > b && !almostEqual(a, b, FLT, FLT) },
          gte: function (a, b) { return a >= b || almostEqual(a, b, FLT, FLT) }
        }
      }
    } return t
  })
  this.dim = this.types.length
  this._insertQueue = []
  this._pending = 0
}

KDB.prototype.query = function (q, cb) {
  var self = this
  cb = once(cb || noop)
  if (!Array.isArray(q[0])) q = q.map(function (x) { return [x,x] })

  var pending = 1
  var results = []
  self._get(0, function f (err, node) {
    if (err) return cb(err)
    if (!node) node = { type: REGION, regions: [] }
    if (node.type === REGION) {
      for (var i = 0; i < node.regions.length; i++) {
        var r = node.regions[i]
        if (self._overlappingRange(q, r.range)) {
          pending++
          self._get(r.node, f)
        }
      }
    } else if (node.type === POINTS) {
      for (var i = 0; i < node.points.length; i++) {
        var p = node.points[i]
        if (self._overlappingPoint(q, p.point)) results.push(p)
      }
    }
    if (--pending === 0) cb(null, results)
  })
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
          var min = self.types[j].read(buf, offset)
          offset += min.size
          var max = self.types[j].read(buf, offset)
          offset += max.size
          range.push([ min.value, max.value ])
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
          var coord = self.types[j].read(buf, offset)
          offset += coord.size
          pt.push(coord.value)
        }
        node.points.push({
          point: pt,
          value: buf.readUInt32BE(offset)
        })
        offset += 4
      }
      cb(null, node)
    } else throw new Error('unknown type: ' + node.type)
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
        offset += self.types[j].write(buf, node.regions[i].range[j][0], offset)
        offset += self.types[j].write(buf, node.regions[i].range[j][1], offset)
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
        offset += self.types[j].write(buf, node.points[i].point[j], offset)
      }
      buf.writeUInt32BE(node.points[i].value, offset)
      offset += 4
    }
  } else throw new Error('unknown type: ' + node.type)
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
        node.regions[0].range.push([-Infinity,Infinity])
      }
      var pts = { type: POINTS, points: [] }
      var pending = 2
      self._put(self._available++, node, function (err) {
        if (--pending === 0) f(err, node)
      })
      self._put(self._available++, pts, function (err) {
        if (--pending === 0) f(err, node)
      })
    } else insert(node, 0)
  })

  function insert (node, depth) {
    if (node.type === REGION) {
      for (var i = 0; i < node.regions.length; i++) {
        var r = node.regions[i]
        if (self._overlappingRange(q, r.range)) {
          return self._get(r.node, function (err, rnode) {
            rnode.parent = { node: node, index: i }
            insert(rnode, depth+1)
          })
        }
      }
      throw new Error('INVALID STATE')
    } else if (node.type === POINTS) {
      if (node.points.length < self.a) {
        node.points.push({ point: pt, value: value })
        return self._put(node.n, node, cb)
      }
      var coords = []
      var axis = (depth + 1) % pt.length
      for (var i = 0; i < node.points.length; i++) {
        coords.push(node.points[i].point[axis])
      }
      var pivot = median(coords)
      if (!node.parent) {
        throw new Error('at the root!')
      } else if (node.parent.node.regions.length >= self.b - 1) {
        ;(function loop (p) {
          if (p.node.regions.length < self.b - 1) {
            return insert(p.node, depth+1)
          }
          self._splitRegionNode(p, pivot, axis, function (err, right) {
            if (err) return cb(err)
            if (p.node.n === 0) {
              p.range = regionRange(self.dim, p.node.regions)
              var root = {
                type: REGION,
                regions: [ p, right ]
              }
              var pending = 2
              var n = p.node.n
              p.node.n = self._available++
              self._put(p.node.n, p.node, done)
              self._put(n, root, done)
              function done (err) {
                if (err) cb(err)
                else if (--pending === 0) insert(root, 0)
              }
            } else {
              p.node.regions.push(right)
              self._put(p.n, p.node, function (err) {
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
  right.n = self._available++
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
  var rrange = regionRange(self.dim, node.node.regions)
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
          self._splitPointNode(rnode, pivot, axis, function (err, rrnode) {
            if (err) return cb(err)
            rright.node = rrnode
            loop(i+1)
          })
        } else if (rnode.type === REGION) {
throw new Error('recursive split region!')
          rright.node = {
            type: REGION,
            regions: [ splitRegionNode(r, pivot, axis) ]
          }
          loop(i+1)
        } else return cb(new Error('unknown type: ' + rnode.type))
      })
    }
  })(0)

  function done () {
    var pending = 2
    right.node.n = self._available++
    self._put(right.node.n, right.node, f)
    self._put(left.node.n, left.node, f)
    function f (err) {
      if (err) cb(err)
      else if (--pending === 0) cb(null, right)
    }
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

function regionRange (dim, regions) {
  var range = []
  for (var j = 0; j < dim; j++) {
    var r0 = regions.length === 0
      ? [-Infinity,Infinity]
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

function noop () {}
