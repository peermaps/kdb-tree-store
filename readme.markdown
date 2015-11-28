# kdb-tree-store

k-dimensional B tree backed to a chunk store

This code is based on the [original kdb tree paper](http://www.ccs.neu.edu/home/zhoupf/teaching/csu430/paper/kd-b-tree.pdf)
and the algorithm described in "Data Structures and Algorithms in C++, 4th
edition".

# example

``` js
```

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

# license

BSD
