from bndl.util.cython import try_pyximport_install; try_pyximport_install()

import logging

from bndl.compute.cassandra import partitioner, conf
from bndl.compute.cassandra.partitioner import estimate_size, SizeEstimate
from bndl.compute.cassandra.session import cassandra_session
from bndl.compute.dataset.base import Dataset, Partition
from bndl.util.funcs import identity


logger = logging.getLogger(__name__)


class CassandraCoScanDataset(Dataset):
    def __init__(self, *scans, keys=None, dset_id=None):
        assert len(scans) > 1
        super().__init__(scans[0].ctx, src=scans, dset_id=dset_id)

        for scan in scans[1:]:
            assert scan.contact_points == scans[0].contact_points, "only scan in parallel within the same cluster"
            assert scan.keyspace == scans[0].keyspace, "only scan in parallel within the same keyspace"
        assert len(set(scan.table for scan in scans)) == len(scans), "don't scan the same table twice"

        self.contact_points = scans[0].contact_points
        self.keyspace = scans[0].keyspace

        self.srcparts = [src.parts() for src in scans]
        self.pcount = len(self.srcparts[0])

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


    def parts(self):
        from bndl.compute.cassandra.dataset import CassandraScanPartition

        with cassandra_session(self.ctx, contact_points=self.contact_points) as session:
            size_estimates = sum((estimate_size(session, self.keyspace, src.table) for src in self.src),
                                 SizeEstimate(0, 0, 0))

            partitions = partitioner.partition_ranges(session, self.keyspace, size_estimates=size_estimates,
                                                      min_pcount=self.ctx.default_pcount,
                                                      part_size_keys=self.ctx.conf.get_int(conf.PART_SIZE_KEYS, defaults=conf.DEFAULTS),
                                                      part_size_mb=self.ctx.conf.get_int(conf.PART_SIZE_MB, defaults=conf.DEFAULTS))

            return [
                CassandraCoScanPartition(self, idx, [CassandraScanPartition(scan, idx, replicas, token_ranges)
                                                         for scan in self.src])
                for idx, (replicas, token_ranges) in enumerate(partitions)
            ]



class CassandraCoScanPartition(Partition):
    def __init__(self, dset, idx, scans):
        super().__init__(dset, idx)
        self.scans = scans

    def _preferred_workers(self, workers):
        return self.scans[0].preferred_workers(workers)

    def _materialize(self, ctx):
        keyfuncs = self.dset.keyfuncs
        grouptransforms = self.dset.grouptransforms

        subscans = [scan.materialize(ctx) for scan in self.scans]
        merged = {}

        for cidx, scan in enumerate(subscans):
            key = keyfuncs[cidx]
            for row in scan:
                k = key(row)
                batch = merged.get(k)
                if not batch:
                    merged[k] = batch = [[] for _ in subscans]
                batch[cidx].append(row)

        for key, groups in merged.items():
            for idx, (group, transform) in enumerate(zip(groups, grouptransforms)):
                groups[idx] = transform(group)
            yield key, groups
