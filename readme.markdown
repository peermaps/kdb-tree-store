# kdb-tree-store

k-dimensional B tree backed to a chunk store

This code is based on the [original kdb tree paper](http://www.ccs.neu.edu/home/zhoupf/teaching/csu430/paper/kd-b-tree.pdf)
and the algorithm description in "Data Structures and Algorithms in C++, 4th
edition".

For an in-memory version of this algorithm, look at the
[kdb-tree](https://npmjs.com/package/kdb-tree) package.

# example

``` js
var kdbtree = require('kdb-tree-store')
var fdstore = require('fd-chunk-store')
var tmpdir = require('os').tmpdir()

var path = require('path')
var file = path.join(tmpdir, 'kdb-tree-' + Math.random())

var n = 5000
var kdb = kdbtree({
  types: [ 'float32', 'float32', 'float32' ],
  size: 1024,
  store: fdstore(1024, file)
})
var pending = n
for (var i = 0; i < n; i++) (function () {
  var x = Math.random() * 200 - 100
  var y = Math.random() * 200 - 100
  var z = Math.random() * 200 - 100
  var loc = Math.floor(Math.random() * 1000)
  kdb.insert([x,y,z], loc, function (err) {
    if (--pending === 0) check()
  })
})()

function check () {
  kdb.query([[-100,0],[0,5],[-50,-40]], function (err, pts) {
    console.log(pts)
  })
}
```

# api

``` js
var kdbtree = require('kdb-tree-store')
```

## var kdb = kdbtree(opts)

Create a new kdb tree instance `kdb` given `opts`:

* `opts.types` - array of data types for each dimension
* `opts.store` - [chunk store](https://npmjs.com/package/abstract-chunk-store) instance
* `opts.size` - size of the chunks in the chunk store

## kdb.query(q, opts={}, cb)

Query for results with `q`, an array of `[min,max]` arrays for each dimension.
The results are given as an array of points in `cb(err, results)`. Each element
in `results` has a `point` and `value` property.

Add depth information to each point in the tree by setting `opts.depth = true`.

## var stream = kdb.queryStream(q, opts={})

Return a readable `stream` of query results from the query `q`.

## kdb.insert(pt, value, cb)

Insert `value` (a 32-bit integer) at a point `pt`.

### data types

These data types are provided under string aliases:

* `float` (`float32`)
* `double` (`float64`)
* `uint8`
* `uint16`
* `uint32`
* `int8`
* `int16`
* `int32`

Otherwise, a data type must be an object with these properties:

* `t.read(buf, offset)`
* `t.write(buf, value, offset)`
* `t.size` (in bytes)
* `t.min`
* `t.max`
* `t.cmp.lt(a, b)`
* `t.cmp.lte(a, b)`
* `t.cmp.gt(a, b)`
* `t.cmp.gte(a, b)`

# balancing

The kdb tree paper describes the resulting tree as balanced, but this module
does not yet generate very balanaced trees in practice. Some help on this part
would be great!

The splitting plane is not yet chosen very well, looking only at the median of
the presently overfull point page along the depth modulo dimension axis.

Here is a histogram of depths (right column) for 15000 points under the
current implementation:

```
$ node example/depth.js 15000 | uniq -c
   2876 2
   2487 4
   2825 5
    274 6
   1204 7
   1990 8
   1223 9
   1092 10
    338 11
    242 13
    124 14
    208 15
    117 17
```

# install

```
npm install kdb-tree-store
```

# license

BSD
