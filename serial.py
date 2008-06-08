from logic import LogMapper, count_names
from utils import parse_argv

def parse_file(data_file):
    mapper = LogMapper()
    for line in open(data_file):
        mapper.parse_line(line)
    return mapper.get_counters()

if __name__ == '__main__':
    args = parse_argv()
    
    counters = parse_file(args.filename)
    
    if not args.quiet:
        for name in count_names:
            print counters[name].report()
