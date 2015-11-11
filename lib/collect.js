var once = require('once')

module.exports = function (stream, cb) {
  cb = once(cb)
  var rows = []
  stream.once('end', function () { cb(null, rows) })
  stream.once('error', cb)
  read()
  stream.on('readable', read)

  function read () {
    var row = null
    while (row = stream.read()) rows.push(row)
  }
}
