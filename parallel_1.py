import pprocess
import os
import time

from logic import LogMapper, LogCounter, count_names
from utils import seek_open, file_offsets, parse_argv, Stats

def parse_chunk(file_name, start, end):
    start_time = time.clock()
    
    f = seek_open(file_name, start, end)
    
    mapper = LogMapper()
    for line in f:
        mapper.parse_line(line)
    
    return (mapper, os.getpid(), time.clock() - start_time)

def parse_file(file_name, cores, jobs_per_core, stats):
    file_size = os.path.getsize(file_name)
    chunks = int(cores * jobs_per_core)
    stats.initial_report(file_name, file_size, chunks, cores)
    
    queue = pprocess.Queue(limit=cores)
    parse_chunk_async = queue.manage(pprocess.MakeParallel(parse_chunk))
    
    for (start, end) in file_offsets(file_size, chunks):
        parse_chunk_async(file_name, start, end)
    
    total = dict( (count_name, LogCounter(count_name)) for count_name in count_names )
    
    stats.waiting()
    for (mapper, job_pid, job_time) in queue:
        stats.received_job_result()
        start_reduce_time = time.time()
        
        for (count_name, counter) in mapper.get_counters().iteritems():
            total[count_name].add_counter(counter)
        
        stats.job_report(job_pid, job_time, time.time() - start_reduce_time)
        stats.waiting()
    
    for name in count_names:
        print total[name].report()

if __name__ == '__main__':
    args = parse_argv()
    
    stats = Stats(args.log_file, log_each_job=args.log_each_job)
    parse_file(args.filename, args.cores, args.jobs_per_core, stats)
    
    stats.report_master_stats()
