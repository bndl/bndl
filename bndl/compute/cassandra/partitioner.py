import copy
from operator import itemgetter

from bndl.util.collection import sortgroupby
from bndl.compute.cassandra import conf


T_COUNT = 2 ** 64
T_MIN = -(2 ** 63)
T_MAX = (2 ** 63) - 1


def get_token_ranges(ring):
    ring = [t.value for t in ring]
    return list(zip([T_MIN] + ring, ring + [T_MAX]))


def repartition(partitions, min_pcount):
    while len(partitions) < min_pcount:
        repartitioned = []
        for replicas, token_ranges in partitions:
            if len(token_ranges) == 1:
                repartitioned.append((replicas, token_ranges))
                continue
            mid = len(token_ranges) // 2
            repartitioned.append((replicas, token_ranges[:mid]))
            repartitioned.append((replicas, token_ranges[mid:]))
        if len(repartitioned) == len(partitions):
            break
        partitions = repartitioned
    return partitions


def partition_ranges(session, keyspace, table=None, size_estimates=None, min_pcount=None,
                     part_size_keys=conf.DEFAULTS[conf.PART_SIZE_KEYS],
                     part_size_mb=conf.DEFAULTS[conf.PART_SIZE_MB]):
    # estimate size of table
    size_estimate = size_estimates or estimate_size(session, keyspace, table)

    # get raw ranges from token ring
    token_map = session.cluster.metadata.token_map
    raw_ranges = get_token_ranges(token_map.ring)

    # group by replica
    by_replicas = sortgroupby(
        (
            (set(replica.address for replica in token_map.get_replicas(keyspace, token_map.token_class(start))),
             start, end)
            for start, end in raw_ranges
        ), itemgetter(0)
    )

    # divide the token ranges in partitions
    # joining ranges for the same replica set
    # but limited in size (in bytes and Cassandra partition keys)
    partitions = []
    current_ranges = []
    current_ranges_size_mb = 0
    current_ranges_size_keys = 0

    for replicas, ranges in by_replicas:
        ranges = list(ranges)
        for _, start, end in ranges:
            length = end - start
            size_b = length * size_estimate.token_size_b
            size_pk = length * size_estimate.token_size_pk

            current_ranges_size_mb += (size_b / 1024. / 1024.)
            current_ranges_size_keys += size_pk

            if current_ranges_size_mb > part_size_mb or current_ranges_size_keys > part_size_keys:
                # possibly a single token range exceeds our limits
                # TODO split that range into chunks within our limits
                if current_ranges:
                    partitions.append((replicas, current_ranges))
                current_ranges = []
                current_ranges_size_mb = 0
                current_ranges_size_keys = 0

            current_ranges.append((start, end))

        if current_ranges:
            partitions.append((replicas, current_ranges))
            current_ranges = []
            current_ranges_size_mb = 0
            current_ranges_size_keys = 0

    if min_pcount:
        return repartition(partitions, min_pcount)
    else:
        return partitions



class SizeEstimate(object):
    def __init__(self, size, partitions, fraction):
        if fraction:
            self.table_size_b = int(size / fraction)
            self.table_size_pk = int(partitions / fraction)
            self.token_size_b = float(self.table_size_b) / T_COUNT
            self.token_size_pk = float(self.table_size_pk) / T_COUNT
        else:
            self.table_size_b = 0
            self.table_size_pk = 0
            self.token_size_b = 0
            self.token_size_pk = 0

    def __add__(self, other):
        est = copy.copy(self)
        est += other
        return est

    def __iadd__(self, other):
        self.table_size_b += other.table_size_b
        self.table_size_pk += other.table_size_pk
        self.token_size_pk = (self.token_size_pk + other.token_size_pk) / 2
        self.token_size_b = (self.token_size_b + other.token_size_b) / 2
        return self

    def __repr__(self):
        return '<SizeEstimate: size=%s, partitions=%s, partitions / token=%s, tokensize=%s>' % (
            self.table_size_b / 1024. / 1024.,
            self.table_size_pk,
            self.token_size_pk,
            self.token_size_b
        )


def estimate_size(session, keyspace, table):
    size_estimate_query = '''
        select range_start, range_end, partitions_count, mean_partition_size
        from system.size_estimates
        where keyspace_name = %s and table_name = %s
    '''
    size_estimates = list(session.execute(size_estimate_query, (keyspace, table)))

    size_b = 0
    size_pk = 0
    tokens = 0

    if len(size_estimates) == 1:
        range_estimate = size_estimates[0]
        size_pk = range_estimate.partitions_count
        size_b = range_estimate.mean_partition_size * range_estimate.partitions_count
        tokens = T_COUNT
    else:
        for range_estimate in size_estimates:
            start, end = int(range_estimate.range_start), int(range_estimate.range_end)
            # don't bother unwrapping the token range crossing 0
            if start > end:
                continue
            # count partitions, bytes and size of the range
            size_pk += range_estimate.partitions_count
            size_b += range_estimate.mean_partition_size * range_estimate.partitions_count
            tokens += int(range_estimate.range_end) - int(range_estimate.range_start)

    fraction = tokens / T_COUNT

    return SizeEstimate(size_b, size_pk, fraction)
