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
