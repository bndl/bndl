from bndl.util.cython import try_pyximport_install; try_pyximport_install()

import logging

from bndl.compute.cassandra.session import cassandra_session
from bndl.compute.dataset.zip import ZippedDataset, ZippedPartition
from bndl.util.funcs import identity
from bndl.compute.cassandra.partitioner import estimate_size, SizeEstimate
from bndl.compute.cassandra import partitioner


logger = logging.getLogger(__name__)


class CassandraCoScanDataset(ZippedDataset):
    def __init__(self, *scans, keys=None, dset_id=None):
        for scan in scans[1:]:
            assert scan.contact_points == scans[0].contact_points, "only scan in parallel within the same cluster"
            assert scan.keyspace == scans[0].keyspace, "only scan in parallel within the same keyspace"
        assert len(set(scan.table for scan in scans)) == len(scans), "don't scan the same table twice"

        super().__init__(*scans, comb=self.combiner, dset_id=dset_id)
        self.contact_points = scans[0].contact_points
        self.keyspace = scans[0].keyspace

        # TODO check format (dicts, tuples, namedtuples, etc.)
        # TODO adapt keyfuncs to below

        self.keys = keys

        with cassandra_session(self.ctx, contact_points=self.contact_points) as session:
            ks_meta = session.cluster.metadata.keyspaces[self.keyspace]
            tbl_metas = [ks_meta.tables[scan.table] for scan in scans]

            if not keys:
                primary_key_length = len(tbl_metas[0].primary_key)
                for tbl_meta in tbl_metas[1:]:
                    assert len(tbl_meta.primary_key) == primary_key_length, \
                        "can't co-scan without keys with varying primary key length"

                def keyfunc(row):
                    return tuple(row[i] for i in range(primary_key_length))
                self.keyfuncs = [keyfunc] * len(scans)

                def grouptransform(group):
                    return group[0] if group else None
                self.grouptransforms = [grouptransform] * len(scans)

            else:
                assert len(keys) == len(scans), \
                    "provide a key for each table scanned or none at all"

                self.keyfuncs = []
                self.grouptransforms = []
                for key, scan, tbl_meta in zip(keys, scans, tbl_metas):
                    if isinstance(key, str):
                        key = (key,)
                    keylen = len(key)

                    assert len(tbl_meta.partition_key) <= keylen, \
                        "can't co-scan over a table keyed by part of the partition key"
                    assert tuple(key) == tuple(c.name for c in tbl_meta.primary_key)[:keylen], \
                        "the key columns must be the first part (or all) of the primary key"
                    assert scan._select is None or tuple(key) == tuple(scan._select)[:keylen], \
                        "select all columns or the primary key columns in the order as they " \
                        "are defined in the CQL schema"

                    self.keyfuncs.append(lambda row, keylen=keylen: row[:keylen])
                    if keylen == len(tbl_meta.primary_key):
                        self.grouptransforms.append(lambda group: group[0] if group else None)
                    else:
                        self.grouptransforms.append(identity)



    def coscan(self, other, keys=None):
        from bndl.compute.cassandra.dataset import CassandraScanDataset
        assert isinstance(other, CassandraScanDataset)
        return CassandraCoScanDataset(*self.src + (other,), keys=keys)


    def combiner(self, *partitions):
        merged = {}

        for cidx, child in enumerate(partitions):
            key = self.keyfuncs[cidx]
            for row in child:
                k = key(row)
                batch = merged.get(k)
                if not batch:
                    merged[k] = batch = [[] for _ in partitions]
                batch[cidx].append(row)

        for key, groups in merged.items():
            for idx, (group, transform) in enumerate(zip(groups, self.grouptransforms)):
                groups[idx] = transform(group)
            yield key, groups


    def parts(self):
        from bndl.compute.cassandra.dataset import CassandraScanPartition

        with cassandra_session(self.ctx, contact_points=self.contact_points) as session:
            size_estimates = sum((estimate_size(session, self.keyspace, src.table) for src in self.src),
                                 SizeEstimate(0, 0, 0))

            partitions = partitioner.partition_ranges(session, self.keyspace, size_estimates=size_estimates,
                                                      min_pcount=self.ctx.default_pcount)

            return [
                CassandraParallelScanDataset(self, idx, [CassandraScanPartition(scan, idx, replicas, token_ranges)
                                                         for scan in self.src])
                for idx, (replicas, token_ranges) in enumerate(partitions)
            ]



class CassandraParallelScanDataset(ZippedPartition):
    def preferred_workers(self, workers):
        if self.cached_on:
            return super().preferred_workers(workers)

        return self.children[0].preferred_workers(workers)
