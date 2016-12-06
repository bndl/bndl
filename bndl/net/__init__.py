from bndl.net.connection import getlocalhostname
from bndl.util.conf import CSV


listen_addresses = CSV(['tcp://%s:5000' % getlocalhostname()],
                       desc='The addresses for the local BNDL node to listen on.')
seeds = CSV(desc='The seed addresses for BNDL nodes to form a cluster through gossip.')
