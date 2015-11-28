var almostEqual = require('almost-equal')
var FLT = almostEqual.FLT_EPSILON
var DBL = almostEqual.DBL_EPSILON

module.exports = function (t) {
  if (/^(f|f32|float32)$/.test(t)) {
    return {
      read: function (buf, offset) {
        return buf.readFloatBE(offset)
      },
      write: function (buf, value, offset) {
        return buf.writeFloatBE(value, offset)
      },
      size: 4,
      cmp: {
        lt: function (a, b) { return a < b && !almostEqual(a, b, FLT, FLT) },
        lte: function (a, b) { return a <= b || almostEqual(a, b, FLT, FLT) },
        gt: function (a, b) { return a > b && !almostEqual(a, b, FLT, FLT) },
        gte: function (a, b) { return a >= b || almostEqual(a, b, FLT, FLT) }
      }
    }
  } else if (/^(d|f64|double|float64)$/.test(t)) {
    return {
      read: function (buf, offset) {
        return buf.readDoubleBE(offset)
      },
      write: function (buf, value, offset) {
        return buf.writeDoubleBE(value, offset)
      },
      size: 8,
      cmp: {
        lt: function (a, b) { return a < b && !almostEqual(a, b, DBL, DBL) },
        lte: function (a, b) { return a <= b || almostEqual(a, b, DBL, DBL) },
        gt: function (a, b) { return a > b && !almostEqual(a, b, DBL, DBL) },
        gte: function (a, b) { return a >= b || almostEqual(a, b, DBL, DBL) }
      }
    }
  } else if (/^u(|i|int)?8$/.test(t)) {
    return {
      read: function (buf, offset) {
        return buf.readUInt8BE(offset)
      },
      write: function (buf, value, offset) {
        return buf.writeUInt8BE(value, offset)
      },
      size: 1,
      cmp: icmp
    }
  } else if (/^u(|i|int)?16$/.test(t)) {
    return {
      read: function (buf, offset) {
        return buf.readUInt16BE(offset)
      },
      write: function (buf, value, offset) {
        return buf.writeUInt16BE(value, offset)
      },
      size: 2,
      cmp: icmp
    }
  } else if (/^u(|i|int)?32$/.test(t)) {
    return {
      read: function (buf, offset) {
        return buf.readUInt32BE(offset)
      },
      write: function (buf, value, offset) {
        return buf.writeUInt32BE(value, offset)
      },
      size: 4,
      cmp: icmp
    }
  } else if (/^s?(|i|int)?8$/.test(t)) {
    return {
      read: function (buf, offset) {
        return buf.readInt8BE(offset)
      },
      write: function (buf, value, offset) {
        return buf.writeInt8BE(value, offset)
      },
      size: 1,
      cmp: icmp
    }
  } else if (/^s?(|i|int)?16$/.test(t)) {
    return {
      read: function (buf, offset) {
        return buf.readInt16BE(offset)
      },
      write: function (buf, value, offset) {
        return buf.writeInt16BE(value, offset)
      },
      size: 2,
      cmp: icmp
    }
  } else if (/^s?(|i|int)?32/.test(t)) {
    return {
      read: function (buf, offset) {
        return buf.readInt32BE(offset)
      },
      write: function (buf, value, offset) {
        return buf.writeInt32BE(value, offset)
      },
      size: 4,
      cmp: icmp
    }
  }
}

var icmp = { lt: lt, lte: lte, gt: gt, gte: gte }
function lt (a, b) { return a < b }
function lte (a, b) { return a <= b }
function gt (a, b) { return a > b }
function gte (a, b) { return a >= b }
