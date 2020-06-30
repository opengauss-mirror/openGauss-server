#!/usr/bin/python
import sys

args = sys.argv[1:]
tablename = args[0]
delimiter = args[1]
#print delimiter

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
  print values
