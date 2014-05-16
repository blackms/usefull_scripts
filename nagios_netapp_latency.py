#!/usr/bin/env python

import os
import sys
import datetime
import argparse

limit = 40
warn = 25
samples = 5
DEBUG = 0
retval = 0
retmsg = ''

parser = argparse.ArgumentParser()
parser.add_argument('-i', '--ip', help='Specify file ip.', action='store', dest='ip', required=True)
p = parser.parse_args()
perffile = '/tmp/.latency_%s.stats' % p.ip

def retrieve_stats(filers):
    dictObj = {}
    for filer in filers:
        read_latency_cmd = 'rsh {0} stats show system:system:sys_read_latency'.format(filer)
        write_latency_cmd = 'rsh {0} stats show system:system:sys_write_latency'.format(filer)
        try:
          read_latency_return_f = os.popen(read_latency_cmd)
          read_latency_return= read_latency_return_f.read().strip()
          read_latency_return_f.close()

          write_latency_return_f = os.popen(write_latency_cmd)
          write_latency_return = write_latency_return_f.read().strip()
          write_latency_return_f.close()

          read_latency = read_latency_return.split(':')[3].split('.')[0].replace("ms", "")
          write_latency = write_latency_return.split(':')[3].split('.')[0].replace("ms", "")
        except IndexError:
            read_latency = '10000'
            write_latency = '10000'
        dictObj[filer] = {}
        dictObj[filer]['read'] = read_latency
        dictObj[filer]['write'] = write_latency
    return dictObj


def read_file():
    perfobj = {}
    fbuff = open(perffile, 'r').read().strip()
    for line in fbuff.split('\n'):
        filer, read, write = line.split(',')[0], line.split(',')[1], line.split(',')[2]
        if not perfobj.has_key(filer):
            perfobj[filer] = []
        if len(perfobj[filer]) < samples:
            perfobj[filer].append({'read': read, 'write': write})
    return perfobj


def write_file(perflist):
    write_buffer = ''
    for filer in perflist.keys():
        for sample in perflist[filer]:
            line = '{0},{1},{2}\n'.format(filer, sample['read'], sample['write'])
            write_buffer += line
    with open(perffile, 'w') as fh:
        fh.write(write_buffer)

if __name__ == '__main__':
    perflist = {}
    not_empty = False
    if os.path.isfile(perffile) and os.path.getsize(perffile) > 0:
        perflist = read_file()
        not_empty = True
    # Pop first elemt of each sample list per filer in case of we already have 5 samples.
    if not_empty:
        for k in perflist.keys():
            filer = k
            if len(perflist[filer]) == samples:
                del perflist[filer][0]
    # In order to avoid to rewrite the function
    # retrieve_stats i will pass the ip as list 
    # with only one element.
    obj = retrieve_stats([p.ip])
    # Add new sample in position last.
    for filer in obj.keys():
        if not perflist.has_key(filer): perflist[filer] = []
        perflist[filer].append({'read': obj[filer]['read'], 'write': obj[filer]['write']})
    # Writing samples to file.
    write_file(perflist)
    if DEBUG == 1:
        print perflist
    # Check if each sample is higher than limit.
    for filer in sorted(perflist.keys()):
        read, write = [], []
        if DEBUG == 1:
            print 'Processing: %s' % filer
        for sample in perflist[filer]:
            read.append(int(sample['read'].replace('ms', '')))
            write.append(int(sample['write'].replace('ms', '')))
        hi_read = [x for x in read if x >= limit]
        hi_write = [x for x in write if x >= limit]
        if DEBUG == 1:
            print 'Read: %s' % hi_read
            print len(hi_read)
            print 'Write: %s' % hi_write
            print len(hi_write)
        avg_read = int(sum([int(x['read'].replace('ms', '')) for x in perflist[filer]]) / samples)
        avg_write = int(sum([int(x['write'].replace('ms', '')) for x in perflist[filer]]) / samples)
        if avg_read >= warn and avg_read < limit or avg_write >= warn and avt_write < limit:
            retval = 1
            state = 'WARNING - '
        elif avg_read >= limit or avg_write >= limit:
            retval = 2
            state = 'CRITICAL - '
        else:
            retval = 0
            state = 'OK - '
    if retval is 0:
        print('%swrite avg: %d, read avg: %d |read=%dms;%d;%d;0;100 write=%ds;%d;%d;0;100' % (state, avg_write, avg_read, avg_read, warn, limit, avg_write, warn, limit))
        sys.exit(retval)
    else:
        print('CRITICAL - write avg: %d, read avg: %d |read=%dms;%d;%d;0;100 write=%ds;%d;%d;0;100' % (avg_write, avg_read, avg_read, warn, limit, avg_write, warn, limit))
        sys.exit(retval)
