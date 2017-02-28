# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from psutil import *

process = Process()
process_memory_percent = process.memory_percent
process_memory_info = process.memory_info


def process_memory_percent_nonshared():
    mem_info = process.memory_info()
    return (mem_info.rss - mem_info.shared) / virtual_memory().total * 100

# cpu_count = psutil.cpu_count
# cpu_percent = psutil.cpu_percent
# cpu_stats = psutil.cpu_stats
# cpu_times = psutil.cpu_times
# cpu_times_percent = psutil.cpu_times_percent
# disk_io_counters = psutil.disk_io_counters
# disk_partitions = psutil.disk_partitions
# disk_usage = psutil.disk_usage
# net_connections = psutil.net_connections
# net_if_addrs = psutil.net_if_addrs
# net_if_stats = psutil.net_if_stats
# pid_exists = psutil.pid_exists
# pids = psutil.pids
# process_iter = psutil.process_iter
# swap_memory = psutil.swap_memory
# test = psutil.test
# users = psutil.users
# virtual_memory = psutil.virtual_memory
# wait_procs = psutil.wait_procs
