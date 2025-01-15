## 什么是gs_filedump

gs_filedump是一个用于格式化openGauss堆、映射文件、控制文件的工具。它将文件内容以文本形式输出。

版权所有 （c） 2002-2010 Red Hat， Inc.

版权所有 （c） 2011-2023，PostgreSQL 全球开发组

该程序是免费软件;您可以重新分发它或修改它根据 GNU 通用公共许可证的条款发布 自由软件基金会;许可证的版本 2，或 （由您选择）任何更高版本。

原文作者：帕特里克·麦克唐纳 patrickm@redhat.com

## 概述

gs_filedump是一个用于格式化openGauss堆、映射文件、控制文件的实用程序，转换为人类可读的形式。您可以通过多种方式格式化/转储文件。

文件类型（堆/索引）通常可以自动确定 按文件中块的内容。但是，要格式化 pg_control文件中，您必须使用 -c 选项。

默认情况下，使用 块 0 并显示块相对地址。这些默认值可以通过运行时选项进行修改。

有些选项可能看起来很奇怪，但它们的存在是有原因的。例如，区块大小。它在那里是因为如果区块 0 的标头被损坏，你需要一种方法来强制块大小。

## 编译/安装

编译gs_filedump，你需要正确配置openGauss原代码树或开发包（包含头文件）的相应openGauss主要版本。

gs_filedump源码位置位于openGauss源码树中的contrib/gs_filedump/目录下，该工具默认不包含于openGauss安装包中，如需此工具需自行编译。

```bash
make
make install
```

## 如何使用gs_filedump

```bash
gs_filedump -h

Version 6.0.0 (for openGauss 6.x.x ...)
Copyright (c) 2002-2010 Red Hat, Inc.
Copyright (c) 2011-2023, PostgreSQL Global Development Group

Usage: gs_filedump [-abcdfhikuxy] [-r relfilenode] [-T reltoastrelid] [-R startblock [endblock]] [-D attrlist] [-S blocksize] [-s segsize] [-n segnumber] file

Display formatted contents of a PostgreSQL heap/index/control file
Defaults are: relative addressing, range of the entire file, block
               size as listed on block 0 in the file

The following options are valid for heap and index files:
  -a  Display absolute addresses when formatting (Block header
      information is always block relative)
  -b  Display binary block images within a range (Option will turn
      off all formatting options)
  -d  Display formatted block content dump (Option will turn off
      all other formatting options)
  -D  Decode tuples using given comma separated list of types
      Supported types:
        bigint bigserial bool char charN date float float4 float8 int
        json macaddr name numeric oid real serial smallint smallserial text
        time timestamp timestamptz timetz uuid varchar varcharN xid xml
      ~ ignores all attributes left in a tuple
  -f  Display formatted block content dump along with interpretation
  -h  Display this information
  -i  Display interpreted item details
  -k  Verify block checksums
  -o  Do not dump old values.
  -R  Display specific block ranges within the file (Blocks are
      indexed from 0)
        [startblock]: block to start at
        [endblock]: block to end at
      A startblock without an endblock will format the single block
  -s  Force segment size to [segsize]
  -u  Decode block which storage type is ustore
  -t  Dump TOAST files
  -v  Ouput additional information about TOAST relations
  -n  Force segment number to [segnumber]
  -S  Force block size to [blocksize]
  -x  Force interpreted formatting of block items as index items
  -y  Force interpreted formatting of block items as heap items

The following options are valid for segment storage table:
      When specifying a segmented storage table, the file path must be specified as '{filedir}/1'
  -r  Specify the relfilenode [relfilenode] of the table 
  -T  Specify the relfilenode [reltoastrelid] of the pg_toast of the table
      Parameter '-t' will not support
The following options are valid for control files:
  -c  Interpret the file listed as a control file
  -f  Display formatted content dump along with interpretation
  -S  Force block size to [blocksize]
Additional functions:
  -m  Interpret file as pg_filenode.map file and print contents (all
      other options will be ignored)

Report bugs to <pgsql-bugs@postgresql.org>
```

更多使用说明请参考文件[使用案例](./README.help.md)