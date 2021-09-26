#
# Copyright (c) 2021 Huawei Technologies Co.,Ltd.
#
# openGauss is licensed under Mulan PSL v2.
# You can use this software according to the terms and conditions of the Mulan PSL v2.
# You may obtain a copy of Mulan PSL v2 at:
#
# 			http://license.coscl.org.cn/MulanPSL2
#
# THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
# EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
# MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
# See the Mulan PSL v2 for more details.
# -------------------------------------------------------------------------
#
# Makefile for client_logic_data_fetcher
#
# IDENTIFICATION
# 	  src\common\interfaces\libpq\client_logic_data_fetcher\Makefile
#
# -------------------------------------------------------------------------
#
subdir = src/common/interfaces/libpq/client_logic_data_fetcher
top_builddir = ../../../../../

# shared library parameters
NAME= client_logic_data_fetcher

# We can't use Makefile variables here because the MSVC build system scrapes
# OBJS from this file.
OBJS=data_fetcher_manager.o lib_pq_cursor.o data_fetcher.o jni_conn_cursor.o
include $(top_builddir)/src/Makefile.global
include $(top_builddir)/src/Makefile.shlib

override CPPFLAGS := -DFRONTEND -DFRONTEND_PARSER -DPGXC -I$(srcdir) $(CPPFLAGS) -I$(top_builddir)/src/ -I$(top_builddir)/src/include -I$(top_builddir)/src/common/interfaces/libpq 
override CPPFLAGS += -I$(top_builddir)/src/common/interfaces/libpq/client_logic_common -I$(top_builddir)/src/include/libpq/ -I$(JAVA_HOME)/include -I$(JAVA_HOME)/include/linux
override CPPFLAGS := $(filter-out -fPIE, $(CPPFLAGS)) -fPIC -fstack-protector-all -DHAVE_CE
override CFLAGS := $(filter-out -fPIE, $(CFLAGS)) -fPIC -fstack-protector-all


all: all-static-lib

installcheck:
	$(MAKE) -C test $@

clean distclean: clean-lib
	rm -f *.o

maintainer-clean: distclean maintainer-clean-lib
