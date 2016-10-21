import psutil


process = psutil.Process()
process_memory_percent = process.memory_percent

cpu_count = psutil.cpu_count
cpu_percent = psutil.cpu_percent
cpu_stats = psutil.cpu_stats
cpu_times = psutil.cpu_times
cpu_times_percent = psutil.cpu_times_percent
disk_io_counters = psutil.disk_io_counters
disk_partitions = psutil.disk_partitions
disk_usage = psutil.disk_usage
net_connections = psutil.net_connections
net_if_addrs = psutil.net_if_addrs
net_if_stats = psutil.net_if_stats
pid_exists = psutil.pid_exists
pids = psutil.pids
process_iter = psutil.process_iter
swap_memory = psutil.swap_memory
test = psutil.test
users = psutil.users
virtual_memory = psutil.virtual_memory
wait_procs = psutil.wait_procs


if tuple(map(int, psutil.__version__.split('.'))) < (4, 4):
    import psutil._psutil_linux as cext
    from psutil._pslinux import svmem, get_procfs_path, open_binary
    from psutil._common import usage_percent
    import warnings

    def virtual_memory():
        total, free, buffers, shared, _, _, unit_multiplier = cext.linux_sysinfo()
        total *= unit_multiplier
        free *= unit_multiplier
        buffers *= unit_multiplier
        # Note: this (on my Ubuntu 14.04, kernel 3.13 at least) may be 0.
        # If so, it will be determined from /proc/meminfo.
        shared *= unit_multiplier or None
        if shared == 0:
            shared = None

        cached = active = inactive = avail = None
        with open_binary('%s/meminfo' % get_procfs_path()) as f:
            for line in f:
                if cached is None and line.startswith(b"Cached:"):
                    cached = int(line.split()[1]) * 1024
                elif active is None and line.startswith(b"Active:"):
                    active = int(line.split()[1]) * 1024
                elif inactive is None and line.startswith(b"Inactive:"):
                    inactive = int(line.split()[1]) * 1024
                elif inactive is None and line.startswith(b"MemAvailable:"):
                    avail = int(line.split()[1]) * 1024
                # From "man free":
                # The shared memory column represents either the MemShared
                # value (2.4 kernels) or the Shmem value (2.6+ kernels) taken
                # from the /proc/meminfo file. The value is zero if none of
                # the entries is exported by the kernel.
                elif shared is None and \
                        line.startswith(b"MemShared:") or \
                        line.startswith(b"Shmem:"):
                    shared = int(line.split()[1]) * 1024

        missing = []
        if cached is None:
            missing.append('cached')
            cached = 0
        if active is None:
            missing.append('active')
            active = 0
        if inactive is None:
            missing.append('inactive')
            inactive = 0
        if shared is None:
            missing.append('shared')
            shared = 0
        if missing:
            msg = "%s memory stats couldn't be determined and %s set to 0" % (
                ", ".join(missing),
                "was" if len(missing) == 1 else "were")
            warnings.warn(msg, RuntimeWarning)

        # Gracefully degrade to an approximation if MemAvailable not in /proc/meminfo
        # (this is the case on < 3.14 kernels)
        if avail is None:
            avail = free + buffers + cached
        # Note: this value matches "free", but not all the time, see:
        # https://github.com/giampaolo/psutil/issues/685#issuecomment-202914057
        used = total - free
        # Note: this value matches "htop" perfectly.
        percent = usage_percent((total - avail), total, _round=1)
        return svmem(total, avail, percent, used, free,
                     active, inactive, buffers, cached, shared)
