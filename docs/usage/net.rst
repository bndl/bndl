Networking
==========

Form clusters through seed nodes and gossip
Build with asyncio, so fairly scalable
#connections = cores * (cores-1) / 2
for 100 cores: 4950 connections
each core has 99 peers

Serialization through marshall / pickle / cloudpickle
fairly optimistic: check if marshalling is viable, if not pickle, try cloudpickle on failure
marshall for basic bndl.net protocol
pickle for
