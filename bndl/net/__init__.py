from bndl.net.connection import getlocalhostname
from bndl.util.conf import CSV


listen_addresses = CSV(['tcp://%s:5000' % getlocalhostname()])
seeds = CSV()
