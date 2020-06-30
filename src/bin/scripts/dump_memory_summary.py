#!/usr/bin/env python
#Copyright (c) 2020 Huawei Technologies Co.,Ltd.
#
#openGauss is licensed under Mulan PSL v2.
#You can use this software according to the terms and conditions of the Mulan PSL v2.
#You may obtain a copy of Mulan PSL v2 at:
#
#          http://license.coscl.org.cn/MulanPSL2
#
# THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
# EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
# MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
# See the Mulan PSL v2 for more details.
#-------------------------------------------------------------------------
#
# dump_memory_summary.py
#    script to dump memory summary 
# 
# IDENTIFICATION
#    src/bin/scripts/dump_memory_summary.py
#
#-------------------------------------------------------------------------

import sys

usage  = "usage:\n"
usage += "python dump_memory_summary.py [--count] abc.log or\n"
usage += "cat *.log | python dump_memory_summary.py [--count]"
sort_by_count = False
if len(sys.argv) == 1:
  fd = sys.stdin
elif len(sys.argv) == 2:
  if sys.argv[1] == "--count":
    fd = sys.stdin
    sort_by_count = True
  else:
    fd = open(sys.argv[1])
elif len(sys.argv) == 3:
  if sys.argv[1] == "--count":
    sort_by_count = True
    fd = open(sys.argv[2])
  else:
    print usage
    exit(1)
else:
  print usage
  exit(1)


summary = {}
for line in fd:
  pos, size, req_size = line.split(',')
  id = pos
  if id not in summary.keys():
    summary[id] = [0, 0, 0]
  summary[id][0] += int(size)
  summary[id][1] += int(req_size)
  summary[id][2] += 1

# [pos, [size, req_size, count]]
size_sort = summary.items()
if sort_by_count == True:
  size_sort.sort(cmp=lambda x,y: cmp(x[1][2], y[1][2]))
else:
  size_sort.sort(cmp=lambda x,y: cmp(x[1][0], y[1][0]))
size_sort.reverse()

print "POSITION                      SIZE           REQUESTED_SIZE    COUNT"
for pos, ss in size_sort:
  print "%-30s%-15lu%-18lu%-15d" % (pos, ss[0], ss[1], ss[2])
