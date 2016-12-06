Networking
==========

BNDL nodes form fully interconnected clusters through seed nodes and gossip. They communicate over
TCP. See :doc:`./compute/getting_started` and :doc:`./configuration` on how to configure seed nodes
(and listen addresses)

The networking part of the stack is built with ``asyncio`` so it consumes one thread to do the
network IO. Serialization in the network protocol is based on ``marshal.dumps``, if the data to be
sent can be marshalled, or ``pickle.dumps`` if not. If even pickle fails using a cythonized version
of ``cloudpickle`` is attempted. The protocol itself is framed with a length field and includes one
flag for whether the marshal was used or not (pickle being the only other option).
