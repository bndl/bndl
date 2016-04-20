from cassandra.policies import DCAwareRoundRobinPolicy, HostDistance


class LocalNodeFirstPolicy(DCAwareRoundRobinPolicy):
    def __init__(self, local_hosts):
        super().__init__()
        self._local_hosts = local_hosts

    def distance(self, host):
        if host.address in self._local_hosts:
            return HostDistance.LOCAL
        else:
            return HostDistance.REMOTE
