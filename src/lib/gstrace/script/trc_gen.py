#!/usr/bin/env python
# coding=utf-8
#
# Copyright (c) 2020 Huawei Technologies Co.,Ltd.
# 
# openGauss is licensed under Mulan PSL v2.
# You can use this software according to the terms and conditions of the Mulan PSL v2.
# You may obtain a copy of Mulan PSL v2 at:
# 
#          http://license.coscl.org.cn/MulanPSL2
# 
# THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
# EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
# MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
# See the Mulan PSL v2 for more details.
# ---------------------------------------------------------------------------------------
# 
#  trc_gen.py
#        Generate gstrace header files according to gstrace config files.
# 
# IDENTIFICATION
#        src/lib/gstrace/script/trc_gen.py
# 
# ---------------------------------------------------------------------------------------


import os
import re
import argparse
import hashlib
import sys

struct_def = """struct comp_func {
   uint32_t comp;
   uint32_t func_num;
   const char **func_names;
};

struct comp_name {
    uint32_t comp;
    const char *compName;
};
"""

sha256sum = ''

def parse_line(line):
    pattern = '<(.*)>\s*(.*)'
    rematch = re.match(pattern, line)
    return (rematch.group(1), rematch.group(2)) if rematch else None

class FuncArray:
  def __init__(self, comp, list_funcs):
    self.name = 'GS_TRC_{0}_FUNC_NAMES'.format(comp.upper())
    self.list_funcs = list_funcs
  
  def __str__(self):
    res = 'static const char* %s[] =\n{\n    ""' % (self.name)
    if len(self.list_funcs) > 0:
      res = '%s,\n' % (res)
    else:
      res = '%s\n' % (res)
    for idx, func in enumerate(self.list_funcs):
      res = '%s    "%s"' % (res, func)
      if idx != len(self.list_funcs) - 1:
        res = '%s,\n' % (res)
      else:
        res = '%s\n' % (res)
    res = '%s};' % (res)
    return res
    
class CompArray:
  def __init__(self, comps):
    self.comps = comps
    
  def __str__(self):
    res = 'static struct comp_func GS_TRC_FUNC_NAMES_BY_COMP[] =\n{\n    {0, 0, NULL}'
    if len(self.comps) > 0:
      res = '%s,\n' % (res)
    else:
      res = '%s\n' % (res)
      
    for idx, comp in enumerate(self.comps):
      res = '%s    {COMP_%s, %s, GS_TRC_%s_FUNC_NAMES}' % (res, comp["comp"].upper(), comp["num"]+1, comp["comp"].upper())
      if idx != len(self.comps) - 1:
        res = '%s,\n' % (res)
      else:
        res = '%s\n' % (res)
    res = '%s};' % (res)
    return res
    
class CompNameArray:
  def __init__(self, comps):
    self.comps = comps
    
  def __str__(self):
    res = 'static struct comp_name GS_TRC_COMP_NAMES_BY_COMP[] =\n{\n    {0, NULL}'
    if len(self.comps) > 0:
      res = '%s,\n' % (res)
    else:
      res = '%s\n' % (res)
      
    for idx, comp in enumerate(self.comps):
      res = '%s    {COMP_%s, "%s"}' % (res, comp.upper(), comp)
      if idx != len(self.comps) - 1:
        res = '%s,\n' % (res)
      else:
        res = '%s\n' % (res)
    res = '%s};' % (res)
    return res

def parse_file(path):
  parsed_lines = []
  with open(path, 'r') as file:
    # can be redirected from a file, i.e. sql.in
    for line in file.readlines():
        stripped = line.strip()
        if not stripped:
            break
        parsed = parse_line(stripped)
        if parsed:
            parsed_lines.append(parsed)
  return parsed_lines

def calculate_sha(path):
    content = ''
    with open(path, 'rb') as file:
        content = file.read()
    return hashlib.sha256(content).hexdigest()

class FileSystemVisitor:
  def __init__(self):
    self.result = []
  
  def visit(self, path):
    if os.path.isdir(path):
      self.visit_dir(path)
    elif path.endswith('.in'):
      self.visit_input(path)
      
  def visit_dir(self, path):
    for file in os.listdir(path):
      file_path = os.path.join(path, file)
      self.visit(file_path)
    
  def visit_input(self, path):
    pass
    
class CompFileVisitor(FileSystemVisitor):
  def visit_input(self, path):
    parsed = parse_file(path)
    if len(parsed) == 0:
      return
    if parsed[0][0] != 'comp':
      return
    else:
      self.result = [comp[1].lower() for comp in parsed]

class ShaVisitor(FileSystemVisitor):
  def visit_input(self, path):
    self.result += calculate_sha(path)
      
class FuncFileVisitor(FileSystemVisitor):
  def __init__(self):
    self.result = {}
    
  def visit_input(self, path):
    parsed = parse_file(path)
    if len(parsed) == 0:
      return
    if parsed[0][0] != 'func':
      return
    else:
      comp_name = os.path.basename(path).split('.')[0]
      self.result[comp_name.lower()] = [comp[1] for comp in parsed]

def GenFuncTrcId(comp, listfuncs, outdir):
    res = '// This is a generated file\n\n'
    # include guard
    res = '%s#ifndef _%s_GSTRACE_H\n#define _%s_GSTRACE_H\n\n' % (res, comp.upper(), comp.upper())
    res = '%s#include "comps.h"\n#include "gstrace_infra.h"\n\n' % (res)
    cnt = 1
    for idx, func in enumerate(listfuncs):
        res = '%s#define GS_TRC_ID_%s GS_TRC_ID_PACK(COMP_%s, %s)\n' % (res, func, comp.upper(), cnt)
        cnt += 1
    # include guard
    res = '%s\n#endif' % (res)

    if args.debug:
        print(res)

    # append to the file
    output = '{0}/{1}_gstrace.h'.format(outdir, comp)
    with open(output, 'w') as file:
        file.write(res)

def GenCompId(comps, outdir):
    res = '// This is a generated file\n\n'
    # include guard
    res = '%s#ifndef _COMPS_H\n#define _COMPS_H\n\n' % (res)
    cnt = 1
    for comp in comps:
        res = '%s#define COMP_%s %s\n' % (res, comp.upper(), cnt)
        cnt += 1
    res = '%s#define COMP_MAX %s\n' % (res, cnt)
    # include guard
    res = '%s\n#endif' % (res)

    # append to the file
    output = '{0}/comps.h'.format(outdir)
    with open(output, 'w') as file:
        file.write(res)

def GenFuncCompName(map_comp, comp_with_funcnum, comps, outdir):
    filename = 'funcs.comps'

    res = '// This is a generated file\n\n'
    res = '%s#include "comps.h"\n\n' % (res)
    res = '%s#ifndef _FUNCSCOMPS_H\n#define _FUNCSCOMPS_H\n\n' % (res)
    res = '%s#define HASH_TRACE_CONFIG "%s"\n\n' % (res, str(sha256sum))
    res = '%s%s\n\n' % (res, struct_def)

    for k,v in map_comp.items():
        res = '%s%s\n\n' % (res, str(FuncArray(k, v)))

    res = '%s%s\n\n' % (res, str(CompArray(comp_with_funcnum)))
    res = '%s%s\n\n' % (res, str(CompNameArray(comps)))
    res = '%s#endif' % (res)

    if args.debug:
       print(res)

    # output
    output = '{0}/{1}.h'.format(outdir, filename)
    with open(output, 'w') as file:
        file.write(res)

if __name__ == '__main__':
    # argparser
    parser = argparse.ArgumentParser()
    parser.add_argument("-s", "--source", help="source directory")
    parser.add_argument("-o", "--output", help="output directory")
    parser.add_argument("-d", "--debug", action="store_true")
    args = parser.parse_args()

    if args.source is None or args.output is None:
        print('Error: -s -o required.')
        exit(1)

    visitor = FuncFileVisitor()
    visitor.visit(args.source)
    funcs = visitor.result

    comp_visitor = CompFileVisitor()
    comp_visitor.visit(args.source)
    comps = comp_visitor.result

    sha_visitor = ShaVisitor()
    sha_visitor.visit(args.source)
    str_result = ",".join(sha_visitor.result)
    sha256sum = hashlib.sha256(str_result.encode("utf-8")).hexdigest()

    map_comp = {}
    for comp in comps:
        map_comp[comp] = []

    for k,v in funcs.items():
        map_comp[k] = v

    comp_with_funcnum = []  
    for comp in comps:
        comp_with_funcnum.append({
              "comp": comp,
              "num": len(map_comp[comp])
        })

    print('Generate trace header begin.')
    # generate component Ids
    GenCompId(comps, args.output)

    # generate functions' trace id
    for k,v in map_comp.items():
        GenFuncTrcId(k, v, args.output)

    # generate functions and componentes' name for gstrace tool
    GenFuncCompName(map_comp, comp_with_funcnum, comps, args.output)

    print('Generate trace header done.')
