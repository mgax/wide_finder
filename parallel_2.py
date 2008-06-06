import pprocess
import os
import time
import pickle
import tempfile
import shutil

from logic import LogCounter
from utils import seek_open, file_offsets, parse_argv, Stats

def parse_chunk(file_name, temp_dir, start, end):
    start_time = time.time()
    
    f = seek_open(file_name, start, end)
    
    counter = LogCounter()
    for line in f:
        counter.parse_line(line)
    
    out_file_name = "%s/%d" % (temp_dir, start)
    pickle.dump(counter, open(out_file_name, 'wb'))
    return (out_file_name, os.getpid(), time.time() - start_time)

def parse_file(file_name, cores, jobs_per_core, stats):
    file_size = os.path.getsize(file_name)
    chunks = int(cores * jobs_per_core)
    stats.initial_report(file_name, file_size, chunks, cores)
    
    queue = pprocess.Queue(limit=cores)
    parse_chunk_async = queue.manage(pprocess.MakeParallel(parse_chunk))
    
    temp_dir = tempfile.mkdtemp('.wf2')
    try:
        for (start, end) in file_offsets(file_size, chunks):
            parse_chunk_async(file_name, temp_dir, start, end)
        
        total = LogCounter()
        
        stats.waiting()
        for (file_name, job_pid, job_time) in queue:
            stats.received_job_result()
            start_reduce_time = time.time()
            
            if file_name:
                total.add_counter(pickle.load(open(file_name, 'rb')))
                os.remove(file_name)
            
            stats.job_report(job_pid, job_time, time.time() - start_reduce_time)
            stats.waiting()
    
    finally:
        shutil.rmtree(temp_dir)
    
    return total

if __name__ == '__main__':
    args = parse_argv()
    
    stats = Stats(args.log_file, log_each_job=args.log_each_job)
    counter = parse_file(args.filename, args.cores, args.jobs_per_core, stats)
    
    stats.begin_report()
    counter.report(args.quiet)
    stats.done_report()
    
    stats.report_master_stats()
