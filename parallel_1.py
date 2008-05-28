from logic import LogCounter
from smart_read import seek_open

def read_log_file(file_name, start, end):
    f = seek_open(file_name, start, end)
    if start > 0:
        f.next() # skip the first line, it's being read with the previous block
    
    counter = LogCounter()
    for line in f:
        counter.parse_line(line)
    
    return counter

def file_offsets(file_size):
    cuts = range(0, file_size, file_size/10)
    cuts[10] = file_size
    return map(lambda s, e: (s, e), cuts[:-1], cuts[1:])

def main():
    import sys
    import os
    
    file_name = sys.argv[1]
    
    total = LogCounter()
    for (start, end) in file_offsets(os.path.getsize(file_name)):
        partial = read_log_file(file_name, start, end)
        total.add_counter(partial)
    
    total.report()

if __name__ == '__main__':
    # import cProfile
    # import pstats
    # cProfile.Profile.bias = 1.1190049099519999e-05
    # cProfile.run('main()', 'profile.data')
    # pstats.Stats('profile.data').sort_stats('cumulative').print_stats()
    main()
