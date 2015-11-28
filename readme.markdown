# kdb-tree

This code is based on the [original kdb tree paper](http://www.ccs.neu.edu/home/zhoupf/teaching/csu430/paper/kd-b-tree.pdf)
and the algorithm described in "Data Structures and Algorithms in C++, 4th
edition".

# balancing

The kdb tree paper describes the resulting tree as balanced, but this module
does not yet generate very balanaced trees in practice.

The splitting plane is not yet chosen very well, looking only at the median of
the presently overfull point page along the depth modulo dimension axis.

