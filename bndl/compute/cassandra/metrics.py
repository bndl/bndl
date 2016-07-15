import copy
import math

from bndl.compute.cassandra.session import cassandra_session


def get_cassandra_metrics(worker):
    try:
        pools = getattr(cassandra_session, 'pools')
    except AttributeError:
        return {}

    metrics_per_pool = {}
    for pool in pools.values():
        cluster = pool.cluster
        metrics = cluster.metrics
        metrics_per_pool[cluster.metadata.cluster_name] = {
            'request_timer': copy.deepcopy(metrics.request_timer),
            'connection_errors': int(metrics.connection_errors),
            'write_timeouts': int(metrics.write_timeouts),
            'read_timeouts': int(metrics.read_timeouts),
            'unavailables': int(metrics.unavailables),
            'other_errors': int(metrics.other_errors),
            'retries': int(metrics.retries),
            'ignores': int(metrics.ignores),

            'known_hosts': metrics.known_hosts(),
            'connected_to': metrics.connected_to(),
            'open_connections': metrics.open_connections(),
        }

    return metrics_per_pool


def combine_metrics(metrics1, metrics2):
    metrics1['connection_errors'] += metrics2['connection_errors']
    metrics1['write_timeouts'] += metrics2['write_timeouts']
    metrics1['read_timeouts'] += metrics2['read_timeouts']
    metrics1['unavailables'] += metrics2['unavailables']
    metrics1['other_errors'] += metrics2['other_errors']
    metrics1['retries'] += metrics2['retries']
    metrics1['ignores'] += metrics2['ignores']
    metrics1['errors'] = (metrics1['connection_errors'] +
                          metrics1['write_timeouts'] +
                          metrics1['read_timeouts'] +
                          metrics1['unavailables'] +
                          metrics1['other_errors'] +
                          metrics1['retries'] +
                          metrics1['ignores'])

    metrics1['known_hosts'] += metrics2['known_hosts']
    metrics1['connected_to'] += metrics2['connected_to']
    metrics1['open_connections'] += metrics2['open_connections']
    # summing them doesn't make that much sense, nor does taking the max
    metrics1['connected_to'] /= 2
    metrics1['known_hosts'] /= 2
    metrics1['open_connections'] /= 2

    request_timer1 = metrics1['request_timer']
    request_timer2 = metrics2['request_timer']

    # take the weighted mean
    if request_timer1['count'] + request_timer2['count']:
        request_timer1['mean'] = (request_timer1['mean'] * request_timer1['count'] +
                                  request_timer2['mean'] * request_timer2['count']) / \
                                  (request_timer1['count'] + request_timer2['count'])
        # work back to variances, add them and take the square root to get back to the combined stdev
        # based on http://mathbench.umd.edu/modules/statistical-tests_t-tests/page05.htm
        request_timer1['stddev'] = math.sqrt(request_timer1['stddev'] ** 2 / request_timer1['count'] +
                                             request_timer2['stddev'] ** 2 / request_timer2['count'])
    # take min of min and max of max
    request_timer1['min'] = min(request_timer1['min'], request_timer2['min'])
    request_timer1['max'] = max(request_timer1['max'], request_timer2['max'])
    request_timer1['count'] = request_timer1['count'] + request_timer2['count']

    # use max for percentiles ... better than nothing
    request_timer1['75percentile'] = max(request_timer1['75percentile'], request_timer2['75percentile'])
    request_timer1['95percentile'] = max(request_timer1['95percentile'], request_timer2['95percentile'])
    request_timer1['98percentile'] = max(request_timer1['98percentile'], request_timer2['98percentile'])
    request_timer1['99percentile'] = max(request_timer1['99percentile'], request_timer2['99percentile'])
    request_timer1['999percentile'] = max(request_timer1['999percentile'], request_timer2['999percentile'])

    # using max for median is probably even more shaky than for the 50+ percentiles
    if 'median' in request_timer1:
        del request_timer1['median']


def metrics_by_cluster(metrics):
    by_cluster = {}

    for metrics in metrics:
        for cluster, metrics in metrics.items():
            if not cluster in by_cluster:
                by_cluster[cluster] = metrics
            else:
                combine_metrics(by_cluster[cluster], metrics)

    return by_cluster

