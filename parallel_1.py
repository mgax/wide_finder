import pprocess
import os
import time

from logic import LogCounter
from utils import seek_open, file_offsets, parse_argv, Stats

def parse_chunk(file_name, start, end):
    start_time = time.clock()
    
    f = seek_open(file_name, start, end)
    
    counter = LogCounter()
    for line in f:
        counter.parse_line(line)
    
    return (counter, os.getpid(), time.clock() - start_time)

def parse_file(file_name, cores, jobs_per_core, stats):
    file_size = os.path.getsize(file_name)
    chunks = int(cores * jobs_per_core)
    stats.initial_report(file_name, file_size, chunks, cores)
    
    queue = pprocess.Queue(limit=cores)
    parse_chunk_async = queue.manage(pprocess.MakeParallel(parse_chunk))
    
    for (start, end) in file_offsets(file_size, chunks):
        parse_chunk_async(file_name, start, end)
    
    total = LogCounter()
    
    stats.waiting()
    for (counter, job_pid, job_time) in queue:
        stats.received_job_result()
        stats.job_report(job_pid, job_time)
        
        if counter:
            total.add_counter(counter)
        
        stats.waiting()
    
    return total

if __name__ == '__main__':
    args = parse_argv()
    
    stats = Stats(args.log_file, log_each_job=args.log_each_job)
    counter = parse_file(args.filename, args.cores, args.jobs_per_core, stats)
    
    stats.begin_report()
    if not args.quiet:
        counter.report()
    
    stats.done_report()
    stats.report_master_stats()
