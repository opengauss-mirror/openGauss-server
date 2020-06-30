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
# Makefile
#        Makefile for foreign-Data Wrapper for LOG file
# 
# IDENTIFICATION
#        contrib/log_fdw/Makefile
# 
# ---------------------------------------------------------------------------------------
MODULE_big = log_fdw
OBJS = log_fdw.o prflog_dump.o

EXTENSION = log_fdw
DATA = log_fdw--1.0.sql log_fdw.control

REGRESS = log_fdw


EXTRA_CLEAN = sql/log_fdw.sql expected/log_fdw.out

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/log_fdw
top_builddir = ../..
include $(top_builddir)/src/Makefile.global

include $(top_srcdir)/src/gausskernel/common.mk
override CXXFLAGS += -D_GSLOG_ITERATOR_MODE_ -I$(top_builddir)/src/bin/gs_log/ -I$(top_builddir)/src/common/timezone/
install:install-data

.PHONY: install-data
install-data: installdirs
	$(INSTALL_DATA) $(addprefix $(srcdir)/, $(DATA)) '$(DESTDIR)$(datadir)/extension/'

installdirs:
	$(MKDIR_P) '$(DESTDIR)$(datadir)/extension'

uninstall:uninstall-data

.PHONY: uninstall-data
uninstall-data:
	rm -f $(addprefix '$(DESTDIR)$(datadir)/extension'/, $(notdir $(DATA)))

endif

