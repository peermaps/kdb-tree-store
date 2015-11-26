var median = require('median')
var once = require('once')
var REGION = 0, POINTS = 1

module.exports = KDB

function KDB (opts) {
  var self = this
  if (!(this instanceof KDB)) return new KDB(opts)
  this.a = opts.a || 4 // points
  this.b = opts.b || 3 // regions
  this.store = opts.store
  this.size = opts.size
  this.types = opts.types.map(function (t) {
    if (t === 'float32') {
      return function (buf, offset) {
        return {
          value: buf.readFloat32(offset),
          size: 4
        }
      }
    } return t
  })
  this.dim = this.types.length
}

KDB.prototype.query = function (q, cb) {
  var self = this
  cb = once(cb || noop)
  if (!Array.isArray(q[0])) q = q.map(function (x) { return [x,x] })

  var pending = 1
  var results = []
  self._get(0, function f (err, node) {
    if (err) return cb(err)
    if (node.type === REGION) {
      var pending = node.regions.length
      for (var i = 0; i < node.regions.length; i++) {
        var r = node.regions[i]
        if (overlappingRange(q, r.range)) {
          pending++
          self._get(r.node, f)
        }
      }
    } else if (node.type === POINTS) {
      for (var i = 0; i < node.points.length; i++) {
        var p = node.points[i]
        if (overlappingPoint(q, p.point)) results.push(p)
      }
    }
    if (--pending === 0) cb(null, results)
  })
}

KDB.prototype._get = function (n, cb) {
  var self = this
  self.store.get(n, function (err, buf) {
    if (err) return cb(err)
    var node = { type: buf[0] }
    if (node.type === REGION) {
      node.regions = []
      var nregions = buf.readUInt16BE(1)
      var offset = 3
      for (var i = 0; i < nregions; i++) {
        var range = []
        for (var j = 0; j < self.dim; j++) {
          var min = self.types[j](buf, offset)
          offset += min.size
          var max = self.types[j](buf, offset)
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
          var coord = self.types[j](buf, offset)
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
    }
  })
}

KDB.prototype.insert = function (pt, value) {
  var self = this
  var q = [], rec = { point: pt, value: value }
  for (var i = 0; i < pt.length; i++) q.push([pt[i],pt[i]])
  return insert(this.root, 0)

  function insert (node, depth) {
    if (node.type === REGION) {
      for (var i = 0; i < node.regions.length; i++) {
        var r = node.regions[i]
        if (overlappingRange(q, r.range)) {
          r.node.parent = { node: node, index: i }
          return insert(r.node, 0)
        }
      }
      throw new Error('INVALID STATE')
    } else if (node.type === POINTS) {
      if (node.points.length < self.a) {
        node.points.push({ point: pt, value: value })
        return
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
        var p = node.parent
        while (p.node.regions.length >= self.b - 1) {
          var right = splitRegionNode(p, pivot, axis)
          if (p.node === self.root) {
            p.range = regionRange(self.dim, p.node.regions)
            self.root = {
              type: REGION,
              regions: [ p, right ]
            }
            return insert(self.root, 0)
          } else {
            p.node.regions.push(right)
            p = p.node.parent
          }
        }
        insert(p.node, depth+1)
      } else {
        var right = splitPointNode(node, pivot, axis)
        var pnode = node.parent.node
        var pix = node.parent.index
        var lrange = clone(pnode.regions[pix].range)
        var rrange = clone(pnode.regions[pix].range)
        lrange[axis][1] = pivot
        rrange[axis][0] = pivot
        var lregion = { range: lrange, node: node }
        var rregion = { range: rrange, node: right }
        pnode.regions[pix] = lregion
        pnode.regions.push(rregion)
        insert(pnode, depth+1)
      }
    }
  }

  function splitPointNode (node, pivot, axis) {
    var right = { type: POINTS, points: [] }
    for (var i = 0; i < node.points.length; i++) {
      var p = node.points[i]
      if (p.point[axis] >= pivot) {
        right.points.push(p)
        node.points.splice(i, 1)
        i--
      }
    }
    return right
  }
  function splitRegionNode (node, pivot, axis) {
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

    for (var i = 0; i < node.node.regions.length; i++) {
      var r = node.node.regions[i]
      if (r.range[axis][1] <= pivot) {
        // already in the right place
      } else if (r.range[axis][0] >= pivot) {
        right.node.regions.push(r)
        left.node.regions.splice(i, 1)
        i--
      } else {
        var rright = {
          range: clone(r.range)
        }
        rright.range[axis][0] = pivot
        right.node.regions.push(rright)

        var rleft = r
        rleft.range[axis][1] = pivot

        if (r.node.type === POINTS) {
          rright.node = splitPointNode(r.node, pivot, axis)
        } else if (r.node.type === REGION) {
          rright.node = {
            type: REGION,
            regions: [ splitRegionNode(r, pivot, axis) ]
          }
        } else throw new Error('unknown type: ' + r.node.type)
      }
    }
    return right
  }
}

function overlappingPoint (a, p) {
  for (var i = 0; i < a.length; i++) {
    if (!overlappingmm(a[i][0], a[i][1], p[i], p[i])) return false
  }
  return true
}

function overlappingmm (amin, amax, bmin, bmax) {
  return (amin >= bmin && amin <= bmax)
    || (amax >= bmin && amax <= bmax)
    || (amin < bmin && amax > bmax)
}

function overlappingRange (a, b) {
  for (var i = 0; i < a.length; i++) {
    if (!overlapping(a[i], b[i])) return false
  }
  return true
}

function overlapping (a, b) {
  return (a[0] >= b[0] && a[0] <= b[1])
    || (a[1] >= b[0] && a[1] <= b[1])
    || (a[0] < b[0] && a[1] > b[1])
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
