import re

_blog_matcher = re.compile(r'^/ongoing/When/\d\d\dx/\d\d\d\d/\d\d/\d\d/[^ .]+$')

class LogCounter:
    def __init__(self, name):
        self.name = name
        self.data = {}
    
    def add(self, key, value=1):
        if key in self.data:
            self.data[key] += value
        else:
            self.data[key] = value
    
    def _top_10(self):
        top = []
        bar = 0
        for (key, value) in self.data.iteritems():
            if value > bar:
                top.append((value, key))
                if len(top) > 10:
                    top.sort(reverse=True)
                    top.pop()
                    bar = top[-1][0]
        return top
    
    def report(self):
        out = "Top %s:\n" % self.name
        
        if self.name == 'URIs by bytes':
            format = ' %9.1fM: %s'
        else:
            format = ' %10d: %s'
        
        for (value, key) in self._top_10():
            if self.name == 'URIs by bytes':
                value /= float(2 ** 20)
            out += format % (value, key) + "\n"
        
        return out
    
    def add_counter(self, other):
        for key, value in other.data.iteritems():
            self.add(key, value)

count_names = ['URIs by hit', 'URIs by bytes', '404s', 'client addresses', 'referrers']

class LogMapper:
    def __init__(self):
        self.hits = LogCounter(count_names[0])
        self.bytes = LogCounter(count_names[1])
        self.n404s = LogCounter(count_names[2])
        self.clients = LogCounter(count_names[3])
        self.referrals = LogCounter(count_names[4])
    
    def parse_line(self, line):
        ls = line.split(' ')
        (client, method, url, status, bytes, referral) = (ls[0], ls[5], ls[6], ls[8], ls[9], ls[10])
        
        if method != '"GET':
            return
        
        if status == '404':
            self.n404s.add(url)
        else:
            if status not in ('200', '304'):
                return
            
            if status == '200':
                try:
                    self.bytes.add(url, int(bytes))
                except ValueError:
                    pass
            
            if url.startswith('/ongoing/When/') and _blog_matcher.match(url):
                self.hits.add(url)
                self.clients.add(client)
                if not (referral == '"-"' or referral.startswith('"http://www.tbray.org/ongoing/')):
                    self.referrals.add(referral[1:-1])
    
    def get_counters(self):
        return dict((count.name, count) for count in (self.hits, self.bytes, self.n404s, self.clients, self.referrals))
