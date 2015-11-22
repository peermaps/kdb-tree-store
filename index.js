var median = require('median')
var REGION = 0, POINTS = 1

module.exports = KDB

function KDB (opts) {
  if (!(this instanceof KDB)) return new KDB(opts)
  this.a  = 4 // points
  this.b  = 3 // regions
  this.dim = opts.dim
  this.root = {
    type: REGION,
    regions: [
      {
        range: [],
        node: {
          type: POINTS,
          points: []
        }
      }
    ]
  }
  for (var i = 0; i < this.dim; i++) {
    this.root.regions[0].range.push([-Infinity,Infinity])
  }
}

KDB.prototype.query = function (q) {
  if (!Array.isArray(q[0])) q = q.map(function (x) { return [x,x] })
  return (function query (node) {
    var results = []
    if (node.type === REGION) {
      for (var i = 0; i < node.regions.length; i++) {
        var r = node.regions[i]
        if (overlappingRange(q, r.range)) {
          results.push.apply(results, query(r.node))
        }
      }
    } else if (node.type === POINTS) {
      for (var i = 0; i < node.points.length; i++) {
        var p = node.points[i]
        if (overlappingPoint(q, p.point)) results.push(p)
      }
    }
    return results
  })(this.root)
}

KDB.prototype.insert = function (pt, value) {
  var self = this
  var q = [], rec = { point: pt, value: value }
  for (var i = 0; i < pt.length; i++) q.push([pt[i],pt[i]])
  return insert(this.root, [])

  function insert (node, parents) {
    if (node.type === REGION) {
      for (var i = 0; i < node.regions.length; i++) {
        var r = node.regions[i]
        if (overlappingRange(q, r.range)) {
          var nparents = [{ node: node, index: i }].concat(parents)
          return insert(r.node, nparents)
        }
      }
      throw new Error('INVALID STATE')
    } else if (node.type === POINTS) {
      if (node.points.length < self.a) {
        node.points.push({ point: pt, value: value })
        return
      }
      var coords = []
      var axis = (parents.length + 1) % pt.length
      for (var i = 0; i < node.points.length; i++) {
        coords.push(node.points[i].point[axis])
      }
      var pivot = median(coords)
      if (parents[0].node.regions.length === self.b) {
        for (var i = 0; i < parents.length
        && parents[i].node.regions.length === self.b; i++);
        i -= 1
        var right = splitRegionNode(parents[i], pivot, axis)
        parents[i].node.regions.push(right)
        insert(parents[i].node, parents.slice(i+1))
      } else {
        var right = splitPointNode(node, pivot, axis)
        var pnode = parents[0].node
        var pindex = parents[0].index
        var lrange = clone(pnode.regions[pindex].range)
        var rrange = clone(pnode.regions[pindex].range)
        lrange[axis][1] = pivot
        rrange[axis][0] = pivot
        var lregion = { range: lrange, node: node }
        var rregion = { range: rrange, node: right }
        parents[0].node.regions[pindex] = lregion
        parents[0].node.regions.push(rregion)
        insert(parents[0].node, parents.slice(1))
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
