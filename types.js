var almostEqual = require('almost-equal')
var FLT = almostEqual.FLT_EPSILON
var DBL = almostEqual.DBL_EPSILON

module.exports = function (t) {
  if (t === 'float32' || t === 'f32' || t === 'f') {
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
  } else if (t === 'float64' || t === 'double' || t === 'f64' || t === 'd') {
    return {
      read: function (buf, offset) {
        return buf.readFloatBE(offset)
      },
      write: function (buf, value, offset) {
        return buf.writeFloatBE(value, offset)
      },
      size: 8,
      cmp: {
        lt: function (a, b) { return a < b && !almostEqual(a, b, DBL, DBL) },
        lte: function (a, b) { return a <= b || almostEqual(a, b, DBL, DBL) },
        gt: function (a, b) { return a > b && !almostEqual(a, b, DBL, DBL) },
        gte: function (a, b) { return a >= b || almostEqual(a, b, DBL, DBL) }
      }
    }
  }
}
