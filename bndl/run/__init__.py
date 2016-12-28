from bndl.util.conf import Bool, Config


numactl = Bool(default=True, desc='Pin processe (workers) to specific NUMA zones with numactl.')
pincore = Bool(default=False, desc='Pin processes (workers) to specific cores with taskset.')
jemalloc = Bool(default=True, desc='Use jemalloc if available.')
