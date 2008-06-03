import re

_line_matcher = re.compile(r'^' + r'([^\s]+)\s+' * 11)
_blog_matcher = re.compile(r'^/ongoing/When/\d\d\dx/\d\d\d\d/\d\d/\d\d/[^ .]+$')

def _add(the_dict, key, value=1):
    if key in the_dict:
        the_dict[key] += value
    else:
        the_dict[key] = value

def _report(quiet, label, the_dict, shrink=False):
    if not quiet:
        print "Top %s:" % label
    
    if shrink:
        format = ' %9.1fM: %s'
    else:
        format = ' %10d: %s'
    
    top = [ (value, key) for key, value in the_dict.iteritems() ]
    top.sort()
    top.reverse()
    
    for (value, key) in top[:10]:
        if shrink:
            try:
                value /= float(2 ** 20)
            except:
                if not quiet:
                    print value
                    print type(value)
                raise
        if not quiet:
            print format % (value, key)
    
    if not quiet:
        print

class LogCounter:
    def __init__(self):
        self.stats_hits = {}
        self.stats_bytes = {}
        self.stats_404s = {}
        self.stats_clients = {}
        self.stats_referrals = {}
    
    def parse_line(self, line):
        (client, method, url, status, bytes, referral) = _line_matcher.match(line).group(1, 6, 7, 9, 10, 11)
        
        if method != '"GET':
            return
        
        if status == '404':
            _add(self.stats_404s, url)
        else:
            if status not in ('200', '304'):
                return
            
            if status == '200':
                _add(self.stats_bytes, url, int(bytes))
            
            if _blog_matcher.match(url):
                _add(self.stats_hits, url)
                _add(self.stats_clients, client)
                if not (referral == '"-"' or referral.startswith('"http://www.tbray.org/ongoing/')):
                    _add(self.stats_referrals, referral[1:-1])
    
    def add_counter(self, other):
        for (s, o) in map(lambda p, f: (p, f), self._get_counts_as_tuple(), other._get_counts_as_tuple()):
            for key, value in o.iteritems():
                _add(s, key, value)
    
    def _get_counts_as_tuple(self):
        return (self.stats_hits, self.stats_bytes, self.stats_404s, self.stats_clients, self.stats_referrals)
    
    def report(self, quiet=False):
        _report(quiet, 'URIs by hit', self.stats_hits)
        _report(quiet, 'URIs by bytes', self.stats_bytes, True)
        _report(quiet, '404s', self.stats_404s)
        _report(quiet, 'client addresses', self.stats_clients)
        _report(quiet, 'referrers', self.stats_referrals)
