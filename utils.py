from optparse import OptionParser

def seek_open(file_name, start=0, end=-1):
    f = open(file_name, 'rb')
    
    length = start
    
    if start > 0:
        f.seek(start)
        length += len(f.next())
    
    for line in f:
        yield line
        
        length += len(line)
        if end > -1 and length > end:
            return

def file_offsets(file_size, chunks):
    offsets = list([int(file_size/chunks * n) for n in range(chunks)])
    offsets.append(file_size)
    return map(lambda s, e: (s, e), offsets[:-1], offsets[1:])

def report_job_time(start, end, time):
    return "%d -> %d: %.3f" % (start, end, time)

def parse_argv():
    parser = OptionParser()
    parser.add_option("-q", "--quiet", dest="quiet", action="store_true", default=False)
    (options, args) = parser.parse_args()
    
    return {
        'filename': args[0],
        'output': not options.quiet,
    }
