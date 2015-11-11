var Readable = require('readable-stream').Readable
var dist = require('euclidean-distance')
var REGION = 0, POINT = 1

module.exports = KDB

function KDB (opts) {
  if (!(this instanceof KDB)) return new KDB(opts)
  if (!opts.store) throw new Error('opts.store required')
  if (!opts.root) throw new Error('opts.root required')
  if (!opts.types) throw new Error('opts.types required')
  if (!opts.size) throw new Error('opts.size required')
  this.store = opts.store
  this.root = opts.root
  this.types = opts.types
  this.size = opts.size
}

KDB.prototype.query = function (rquery) {
  var self = this
  var q = normq(rquery)
  var pages = [ [ self.root, 0 ] ]

  var results = new Readable
  results._read = read
  return results

  function read () {
    if (pages.length === 0) return results.push(null)
    var page = pages.shift()
    self.store.get(page[0], function (err, buf) {
      if (err) return results.emit('error', err)
      if (buf.length === 0) return results.push(null)

      if (buf[0] === REGION) {
        parseRegion(buf, page[1])
        read()
      } else if (buf[0] === POINT) {
        if (!parsePoint(buf, page[1])) return read()
      }
    })
  }

  function parseRegion (buf, depth) {
    var nregions = buf.readUInt16BE(1)
    var len = self.types.length
    var t = self.types[depth % len]
    var q = query[depth % len]
    for (var i = 0; i < nregions; i++) {
      if (t === 'float32') {
        var min = buf.readFloat32BE(3 + i * 12)
        var max = buf.readFloat32BE(3 + i * 12 + 4)
        var page = buf.readUInt32BE(3 + i * 12 + 8)
        if (q[0] >= min && q[1] <= max) {
          pages.push([ page, depth+1 ])
        }
      } else throw new Error('unsupported type: ' + t)
    }
  }
 
  function parsePoint (buf, depth) {
    var npoints = buf.readUInt16BE(1)
    var len = self.types.length
    var offset = 3
    var matched = 0
    for (var i = 0; i < npoints; i++) {
      var pt = []
      var m = true
      for (var j = 0; j < len; j++) {
        var t = self.types[j % len]
        if (t === 'float32') {
          var p = buf.readFloat32BE(offset)
          offset += 4
        } else throw new Error('unsupported type: ' + t)
        if (!m) continue
        var qj = query[j]
        if (qj[0] < p || qj[1] > p) m = false
        pt.push(p)
      }
      if (m && q.filter(pt)) {
        matched ++
        pt.push(buf.readUInt32BE(offset))
        result.push(pt)
      }
      offset += 4
    }
    return matched
  }
}

function normq (q) {
  var query = [], filter = ftrue
  if (Array.isArray(q)) {
    for (var i = 0; i < q.length; i++) {
      if (Array.isArray(q[i])) {
        query.push(q[i])
      }
      else {
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
