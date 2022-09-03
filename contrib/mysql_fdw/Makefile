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
#     Makefile for the mysql_fdw
#
# IDENTIFICATION
#        contrib/mysql_fdw/Makefile
#
# ---------------------------------------------------------------------------------------

all:mysql_fdw_target
install:install-data

top_builddir ?= ../../
include $(top_builddir)/src/Makefile.global
CODE_DIR=mysql_fdw

.PHONY: mysql_fdw_target
mysql_fdw_target: prep_checked
	@make -C $(MYFDW_HOME) TOP_DIR=$(abs_top_srcdir)

prep_checked:
	@test -d $(MYFDW_HOME) || ( echo "ERROR: You need fetch and copy mysql_fdw from 'third_party' repo to 'third_party_binarylibs' repo and keep directory strucutre unchanged" && exit 1 )
	@test -d $(MYFDW_HOME) && date > prep_checked
.PHONY: install-data
install-data: mysql_fdw_target
	@make -C $(MYFDW_HOME) TOP_DIR=$(abs_top_srcdir) install
uninstall distclean clean:
	@rm -rf $(MYFDW_HOME)/*.o
	@rm -rf $(MYFDW_HOME)/*.so
	@rm -f prep_checked

MYSQL_FDW_RELEVANT_SOURCES = connection.c deparse.c mysql_fdw.c mysql_query.c option.c
