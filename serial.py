import sys
from optparse import OptionParser

from logic import LogCounter

def parse_file(data_file):
    counter = LogCounter()
    for line in open(data_file):
        counter.parse_line(line)
    return counter

if __name__ == '__main__':
    parser = OptionParser()
    parser.add_option("-q", "--quiet", dest="quiet", action="store_true", default=False)
    (options, args) = parser.parse_args()
    
    counter = parse_file(args[0])
    
    if not options.quiet:
        counter.report()
