from asyncgen import async, generator_map, generator_splitter, async_log
import os
import time

from logic import LogMapper, LogCounter, count_names
from utils import seek_open, file_offsets, parse_argv, pickle_to_file, Stats

@async('file_fragments', tempfile_output=True)
def parse_chunks(file_fragments):
    for file_name, start, end in file_fragments:
        mapper = LogMapper()
        for line in seek_open(file_name, start, end):
            mapper.parse_line(line)
        
        yield mapper.get_counters()

@async('chunks', tempfile_output=True, buffer=1)
def reduce_log_mapper(chunks):
    yield reduce(lambda a, b: a.add_counter(b), chunks).report()

def parse_file(file_name, cores, jobs_per_core):
    def fragments():
        for start, end in file_offsets(os.path.getsize(file_name), int(cores * jobs_per_core)):
            yield file_name, start, end
    
    chunk_parsers = parse_chunks(file_fragments=fragments())
    chunk_parsers.workers = int(cores)
    chunk_parsers.buffer_size = int(cores * 1.5)
    
    temp_data = generator_splitter(chunk_parsers, count_names)
    
    counters = dict( (name, reduce_log_mapper(chunks=temp_data[name])) for name in count_names )
    
    for name in count_names:
        print counters[name].next()

if __name__ == '__main__':
    args = parse_argv()
    
    async_log.enable(to_stderr=True)
    
    counters = parse_file(args.filename, args.cores, args.jobs_per_core)
