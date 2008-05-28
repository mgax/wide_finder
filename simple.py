import re

def count(the_dict, key, value=1):
    if key in the_dict:
        the_dict[key] += value
    else:
        the_dict[key] = value

def report(label, the_dict, shrink=False):
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
                print value
                print type(value)
                raise
        print format % (value, key)
    
    print

def parse_log_file(file):
    stats_hits = {}
    stats_bytes = {}
    stats_404s = {}
    stats_clients = {}
    stats_referrals = {}
    
    line_matcher = re.compile(r'^' + r'([^\s]+)\s+' * 11)
    blog_matcher = re.compile(r'^/ongoing/When/\d\d\dx/\d\d\d\d/\d\d/\d\d/[^ .]+$')
    
    for line in file:
        (client, method, url, status, bytes, referral) = line_matcher.match(line).group(1, 6, 7, 9, 10, 11)
        
        if method != '"GET':
            continue
        
        if status == '404':
            count(stats_404s, url)
        else:
            if status not in ('200', '304'):
                continue
            
            if status == '200':
                count(stats_bytes, url, int(bytes))
            
            if blog_matcher.match(url):
                count(stats_hits, url)
                count(stats_clients, client)
                if not (referral == '"-"' or referral.startswith('"http://www.tbray.org/ongoing/')):
                    count(stats_referrals, referral[1:-1])
    
    report('URIs by hit', stats_hits)
    report('URIs by bytes', stats_bytes, True)
    report('404s', stats_404s)
    report('client addresses', stats_clients)
    report('referrers', stats_referrals)

if __name__ == '__main__':
    import sys
    parse_log_file(open(sys.argv[1], 'r'))
