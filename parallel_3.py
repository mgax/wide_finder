import pprocess
import os
import time
import pickle
import tempfile
import shutil

from logic import LogMapper, LogCounter, count_names
from utils import seek_open, file_offsets, parse_argv, pickle_to_file, Stats

def parse_chunk(file_name, temp_dir, start, end, job_id):
    start_time = time.time()
    
    f = seek_open(file_name, start, end)
    
    mapper = LogMapper()
    for line in f:
        mapper.parse_line(line)
    
    file_dict = {}
    for (count_name, counter) in mapper.get_counters().iteritems():
        pickle.dump(counter, open("%s/map_%d_%d" % (temp_dir, job_id, count_names.index(count_name)), 'wb'))
    
    return job_id

def map_count(channel, count_name, temp_dir):
    total = LogCounter(count_name)
    
    count = 0
    job_id = channel.receive()
    while job_id != None:
        temp_file_name = "%s/map_%d_%d" % (temp_dir, job_id, count_names.index(count_name))
        total.add_counter(pickle.load(open(temp_file_name, 'rb')))
        count += 1
        os.remove(temp_file_name)
        job_id = channel.receive()
    
    channel.send(pickle_to_file(total.report(), "%s/out_%d" % (temp_dir, count_names.index(count_name))))

def parse_file(file_name, cores, jobs_per_core, stats):
    file_size = os.path.getsize(file_name)
    chunks = int(cores * jobs_per_core)
    
    queue = pprocess.Queue(limit=cores)
    parse_chunk_async = queue.manage(pprocess.MakeParallel(parse_chunk))
    
    temp_dir = tempfile.mkdtemp('.wf2')
    
    mappers = {}
    for count_name in count_names:
        mappers[count_name] = pprocess.start(map_count, count_name, temp_dir)
    
    c = 0
    for (start, end) in file_offsets(file_size, chunks):
        c += 1
        parse_chunk_async(file_name, temp_dir, start, end, c)
    
    stats._output('all map jobs queued')
    for job_id in queue:
        start_reduce_time = time.time()
        
        stats._output('map job finished: pid=%d' % job_id)
        for c in range(len(count_names)):
            mappers[count_names[c]].send(job_id)
    
    stats._output('all map jobs finished')
    for (mapper, count_name) in ((mappers[count_name], count_name) for count_name in count_names):
        mapper.send(None)
        print pickle.load(open(mapper.receive(), 'rb'))
        stats._output('reduce job finished: name=%s' % count_name)
    
    shutil.rmtree(temp_dir)

if __name__ == '__main__':
    args = parse_argv()
    
    stats = Stats(args.log_file, log_each_job=args.log_each_job)
    counters = parse_file(args.filename, args.cores, args.jobs_per_core, stats)
