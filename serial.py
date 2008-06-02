from logic import LogCounter
from utils import parse_argv

def parse_file(data_file):
    counter = LogCounter()
    for line in open(data_file):
        counter.parse_line(line)
    return counter

if __name__ == '__main__':
    args = parse_argv()
    
    counter = parse_file(args.filename)
    
    if not args.quiet:
        counter.report()
