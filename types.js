var almostEqual = require('almost-equal')
var FLT = almostEqual.FLT_EPSILON
var DBL = almostEqual.DBL_EPSILON

module.exports = function (t) {
  if (/^(f|f32|float32|float)$/.test(t)) {
    return {
      read: function (buf, offset) {
        return buf.readFloatBE(offset)
      },
      write: function (buf, value, offset) {
        return buf.writeFloatBE(value, offset)
      },
      size: 4,
      min: -Infinity,
      max: Infinity,
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
      min: -Infinity,
      max: Infinity,
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
        return buf.readUInt8(offset)
      },
      write: function (buf, value, offset) {
        return buf.writeUInt8(value, offset)
      },
      size: 1,
      min: 0,
      max: 255,
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
      min: 0,
      max: 65535,
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
      min: 0,
      max: 4294967295,
      cmp: icmp
    }
  } else if (/^s?(|i|int)?8$/.test(t)) {
    return {
      read: function (buf, offset) {
        return buf.readInt8(offset)
      },
      write: function (buf, value, offset) {
        return buf.writeInt8(value, offset)
      },
      size: 1,
      min: -128,
      max: 127,
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
      min: -32768,
      max: 32767,
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
      min: -2147483648,
      max: 2147483647,
      cmp: icmp
    }
  }
}

var icmp = { lt: lt, lte: lte, gt: gt, gte: gte }
function lt (a, b) { return a < b }
function lte (a, b) { return a <= b }
function gt (a, b) { return a > b }
function gte (a, b) { return a >= b }
