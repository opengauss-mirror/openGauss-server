#
# Copyright (c) 2020 Huawei Technologies Co.,Ltd.
#
# openGauss is licensed under Mulan PSL v2.
# You can use this software according to the terms and conditions of the Mulan PSL v2.
# You may obtain a copy of Mulan PSL v2 at:
#
#         http://license.coscl.org.cn/MulanPSL2
#
# THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
# EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
# MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
# See the Mulan PSL v2 for more details.
#
# -------------------------------------------------------------------------
#
# IDENTIFICATION
#    src/gausskernel/grpc/Makefile
#
# -------------------------------------------------------------------------

subdir = src/gausskernel/cbb/grpc
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global

override CPPFLAGS := -std=c++11 $(CPPFLAGS)

ifneq "$(MAKECMDGOALS)" "clean"
  ifneq "$(MAKECMDGOALS)" "distclean"
    ifneq "$(shell which g++ |grep hutaf_llt |wc -l)" "1"
      -include $(DEPEND)
    endif
  endif
endif
OBJS= remote_read_client.o

include $(top_srcdir)/src/gausskernel/common.mk

#remote_read.pb.cc:remote_read.proto
#	protoc --cpp_out=. $<
#remote_read.grpc.pb.cc:remote_read.proto
#	protoc --grpc_out=. --plugin=protoc-gen-grpc=`which grpc_cpp_plugin` $<
