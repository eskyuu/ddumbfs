#!/bin/env python
# accounting.py

import sys, os, time, stat
import shutil

def count_line(filename):
    count=0
    for line in open(filename):
        line=line.lstrip()
        if line.startswith('//'):
            continue
        count+=1
    return count

try:
    distdir=sys.argv[1]
except IndexError:
    distdir=''
    
if not distdir:
    print >>sys.stderr, 'first argument must be ${distdir} and cannot be empty'
    sys.exit(1)
    
try:
    os.chdir(distdir)
except OSError:
    print >>sys.stderr, 'cannot change dir to %s' % (distdir, )
    sys.exit(1)

total_c=0
    
for root, dirs, fls in os.walk('.'):
    for filename in fls:
        if filename.endswith('.c'):
            pathname=os.path.join(root, filename)
            total_c+=count_line(pathname)

print 'C Line: %d' % (total_c)            
