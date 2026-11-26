#! /usr/bin/env python
# Copyright (c) 2026 Huawei Technologies Co., Ltd.
# openGauss-server is licensed under Mulan PSL v2.
# You can use this software according to the terms and conditions of the Mulan PSL v2.
# You may obtain a copy of Mulan PSL v2 at:
#          http://license.coscl.org.cn/MulanPSL2
# THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
# EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
# MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
# See the Mulan PSL v2 for more details.


import sys, string, locale
locale.setlocale(locale.LC_ALL, "")

if len(sys.argv) <> 2:
   sys.stderr.write("Usage: sort.py filename\n")
   sys.exit(1)

infile = open(sys.argv[1], 'r')
list = infile.readlines()
infile.close()

for i in range(0, len(list)):
   list[i] = list[i][:-1] # chop!

list.sort(locale.strcoll)
print string.join(list, '\n')
