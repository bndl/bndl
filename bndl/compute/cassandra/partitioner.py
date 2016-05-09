from operator import itemgetter

from bndl.util.collection import sortgroupby
import copy


T_COUNT = 2 ** 64
T_MIN = -(2 ** 63)
T_MAX = (2 ** 63) - 1



MAX_PARTITIONS_SIZE_PK = 100 * 1000
MAX_PARTITIONS_SIZE_MB = 100
MAX_PARTITIONS_SIZE_B = MAX_PARTITIONS_SIZE_MB * 1024 * 1024



def get_token_ranges(ring):
    ring = [t.value for t in ring]
    return list(zip([T_MIN] + ring, ring + [T_MAX]))


def partition_ranges(session, keyspace, table=None, size_estimates=None, min_pcount=None):
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
    # joining ranges for the same replicaset
    # but limited in size (in bytes and cassandra partition keys)
    partitions = []
    current_ranges = []
    current_ranges_size_b = 0
    current_ranges_size_pk = 0

    for replicas, ranges in by_replicas:
        ranges = list(ranges)
        for _, start, end in ranges:
            length = end - start
            size_b = length * size_estimate.token_size_b
            size_pk = length * size_estimate.token_size_pk

            current_ranges_size_b += size_b
            current_ranges_size_pk += size_pk

            if current_ranges_size_b > MAX_PARTITIONS_SIZE_B or current_ranges_size_pk > MAX_PARTITIONS_SIZE_PK:
                # possibly a single token range exceeds our limits
                # TODO split that range into chunks within our limits
                if current_ranges:
                    partitions.append((replicas, current_ranges))
                current_ranges = []
                current_ranges_size_b = 0
                current_ranges_size_pk = 0

            current_ranges.append((start, end))

        if current_ranges:
            partitions.append((replicas, current_ranges))
            if not current_ranges:
                print('end')
            current_ranges = []
            current_ranges_size_b = 0
            current_ranges_size_pk = 0



    if min_pcount:
        while len(partitions) < min_pcount:
            repartitioned = []
            for replicas, token_ranges in partitions:
                if len(token_ranges) == 1:
                    continue
                mid = len(token_ranges) // 2
                repartitioned.append((replicas, token_ranges[:mid]))
                repartitioned.append((replicas, token_ranges[mid:]))
            if len(repartitioned) == len(partitions):
                break
            partitions = repartitioned

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
    ranges = list(
        session.execute('''
            select range_start, range_end, partitions_count, mean_partition_size
            from system.size_estimates
            where keyspace_name = %s and table_name = %s
        ''',
        (keyspace, table)
    ))

    size_b = 0
    size_pk = 0
    tokens = 0

    if len(ranges) == 1:
        r = ranges[0]
        size_pk = r.partitions_count
        size_b = r.mean_partition_size * r.partitions_count
        tokens = T_COUNT
    else:
        for r in ranges:
            start, end = int(r.range_start), int(r.range_end)
            # don't bother unwrapping the token range crossing 0
            if start > end:
                continue
            # count partitions, bytes and size of the range
            size_pk += r.partitions_count
            size_b += r.mean_partition_size * r.partitions_count
            tokens += int(r.range_end) - int(r.range_start)

    fraction = tokens / T_COUNT

    return SizeEstimate(size_b, size_pk, fraction)
