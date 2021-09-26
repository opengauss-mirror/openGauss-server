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
#     Makefile for the oracle_fdw
#
# IDENTIFICATION
#        contrib/oracle_fdw/Makefile
#
# ---------------------------------------------------------------------------------------

all:oracle_fdw_target
install:install-data

top_builddir ?= ../../
include $(top_builddir)/src/Makefile.global
CODE_DIR=oracle_fdw

.PHONY: oracle_fdw_target
oracle_fdw_target: prep_checked
	@make -C $(ORCFDW_HOME) NO_PGXS=1 TOP_DIR=$(abs_top_srcdir)

prep_checked:
	@test -d $(ORCFDW_HOME) || ( echo "ERROR: You need copy oracle_fdw from 'third_party' repo to 'third_party_binarylibs' repo and keep directory strucutre unchanged" && exit 1 )
	@test -d $(ORCFDW_HOME) && date > prep_checked

.PHONY: install-data
install-data: oracle_fdw_target
	@make -C $(ORCFDW_HOME) NO_PGXS=1 TOP_DIR=$(abs_top_srcdir) install

uninstall distclean clean:
	@rm -rf $(ORCFDW_HOME)/*.o
	@rm -rf $(ORCFDW_HOME)/*.so
	@rm -f prep_checked
