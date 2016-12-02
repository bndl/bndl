Running a cluster
=================


Workers
typically 1 per core
fully interconnected
Driver
your application
to put them to work


$ bndl-compute-workers &
[1] 17780

$ bndl-compute-workers 2 &
[1] 17805

$ bndl-compute-shell --seeds localhost
......................................
In [1]: ctx.worker_count
Out[1]: 6





me @ node1 $ bndl-compute-workers -- --seeds node1:5000 node2:5000
me @ node2 $ bndl-compute-workers -- --seeds node1:5000 node2:5000
me @ node3 $ bndl-compute-workers -- --seeds node1:5000 node2:5000
me @ node4 $ bndl-compute-workers -- --seeds node1:5000 node2:5000
me @ node5 $ bndl-compute-workers -- --seeds node1:5000 node2:5000

me @ node6 $ cat my_script.py
from bndl.compute.run import ctx
print(ctx.range(10000).mean())

me @ node6 $ export BNDL_CONF="bndl.net.seeds=node1:5000,node2:5000"
me @ node6 $ python my_script.py
4999.5



