from bndl.util.cython import try_pyximport_install ; try_pyximport_install()

import logging

from bndl.compute.cassandra.session import cassandra_session
from bndl.util.funcs import identity
from bndl.compute.dataset.zip import ZippedDataset


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
                for m in tbl_metas[1:]:
                    assert len(m.primary_key) == primary_key_length, "can't co-scan without key over tables with varying primary key column count"
                self.keyfuncs = [lambda row: tuple(row[i] for i in range(primary_key_length))] * len(scans)
                self.grouptransforms = [lambda group: group[0] if group else None] * len(scans)
            else:
                assert len(keys) == len(scans), "provide a key for each table scanned or none at all"
                self.keyfuncs = []
                self.grouptransforms = []
                for key, scan, tbl_meta in zip(keys, scans, tbl_metas):
                    if isinstance(key, str):
                        key = (key,)

                    keylen = len(key)
                    assert len(tbl_meta.partition_key) <= keylen, "can't co-scan over a table keyed by part of the partition key"
                    assert tuple(key) == tuple(c.name for c in tbl_meta.primary_key)[:keylen], "the key columns must be the first part (or all) of the primary key"
                    assert scan._select == None or tuple(key) == tuple(scan._select)[:keylen], "select all columns or the primary key columns in the order as they are defined in the CQL schema"
                    self.keyfuncs.append(lambda row: row[:keylen])
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
