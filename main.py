import argparse
import json
import redis
import time


parser = argparse.ArgumentParser(description='Process some integers.')
parser.add_argument('search_term', type=str, action='store')
parser.add_argument('--index', dest='index', action='store_const', const=True)
parser.add_argument('--iter', dest='iter', type=int, action='store', default=1)

parser.add_argument('--x', dest='x', type=int, action='store', default=100)
parser.add_argument('--y', dest='y', type=int, action='store', default=5)

parser.add_argument('--pipeline', dest='pipeline', action='store_const', const=False)


args = parser.parse_args()

r = redis.StrictRedis('localhost', port=6379, db=0)

if args.index:
    total = 0
    f = open('meta.txt', 'r')
    content = json.loads(f.read())
    f.close()

    for site in content:
        complete = u''
        for letter in site[0:-1]:
            complete += letter
            if complete.startswith('g'):
                total += 1
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
    print total

search_term = args.search_term

rank = r.zrank('autocomplete', search_term) or 0
results = r.zrange('autocomplete', rank, rank + args.x)
sites = [r_ for r_ in results if r_.endswith('*')]
final_sites = []

rng = range(args.iter)
start_time = time.time()
for x in rng:
    if args.pipeline:
        p = r.pipeline()
        for site in sites:
            p.zrank('site_rank', site)
        ranks = p.execute()
        for rank in ranks:
            p.zrange('site_rank', rank, rank + 10, withscores=True)
        res = p.execute()
        for s in res:
            map(final_sites.append, s)
    else:
        for site in sites:
            rank = r.zrank('site_rank', site)
            map(final_sites.append, r.zrange('site_rank', rank, rank + args.y, withscores=True))

    final_sites = [fs for fs in final_sites if fs[0].startswith(search_term)]
    final_sites = [fs[0].strip('*') for fs in sorted(final_sites, key=lambda site: site[1])[0:10]]

print final_sites
print 'Took %s milliseconds.' % ((time.time() - start_time) * 1000 / args.iter)
