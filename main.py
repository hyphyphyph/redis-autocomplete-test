import argparse
import json
import redis


parser = argparse.ArgumentParser(description='Process some integers.')
parser.add_argument('search_term', type=str, action='store')
parser.add_argument('--index', dest='index', action='store_const', const=True)
parser.add_argument('--iter', dest='iter', type=int, action='store', default=1)

args = parser.parse_args()

r = redis.StrictRedis('localhost', port=6379, db=0)

if args.index:
    f = open('meta.txt', 'r')
    content = json.loads(f.read())
    f.close()

    for site in content:
        complete = u''
        for letter in site[0:-1]:
            complete += letter
            r.zadd('autocomplete', 0, complete)
        r.zadd('autocomplete', 0, u'%s*' % site)

        site = site.strip('*')
        if (site.split('.')[0] == 'google'):
            if (site.split('.')[-1] == 'com'):
                rank = 0
            else:
                rank = 2
            r.zadd('site_rank', rank, u'%s*' % site)
        elif (site.split('.')[-1] == 'com'):
            r.zadd('site_rank', 1, u'%s*' % site)
        else:
            r.zadd('site_rank', 2, u'%s*' % site)

search_term = args.search_term
for x in xrange(args.iter):
    rank = r.zrank('autocomplete', search_term) or 0
    results = r.zrange('autocomplete', rank, rank + 100)
    sites = [r_ for r_ in results if r_.endswith('*')]

    final_sites = []
    for site in sites:
        rank = r.zrank('site_rank', site) or 0
        map(final_sites.append, r.zrange('site_rank', rank, rank + 10, withscores=True))

    final_sites = [fs for fs in final_sites if fs[0].startswith(search_term)]
    final_sites = [fs[0].strip('*') for fs in sorted(final_sites, key=lambda site: site[1])[0:10]]
    print final_sites
