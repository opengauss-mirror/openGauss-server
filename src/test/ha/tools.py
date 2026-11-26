#!/usr/bin/python
# Copyright (c) 2026 Huawei Technologies Co., Ltd.
# openGauss-server is licensed under Mulan PSL v2.
# You can use this software according to the terms and conditions of the Mulan PSL v2.
# You may obtain a copy of Mulan PSL v2 at:
#          http://license.coscl.org.cn/MulanPSL2
# THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
# EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
# MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
# See the Mulan PSL v2 for more details.

import sys

args = sys.argv[1:]
tablename = args[0]
delimiter = args[1]
#print(delimiter)

insert = "insert into %s values" % (tablename)

for line in sys.stdin:
  line  = line.strip()
  line  = line.replace("\t", "|")
  words = line.split(delimiter)
  values = ""
  for w in words:
    w = w.replace("\"", "\'");
    values += '%s,' % (w)
  values = values[:-1]
  values = insert + "(" + values + ");"
  print(values)
