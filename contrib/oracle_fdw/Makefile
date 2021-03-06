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
ORACLE_FDW_DIR=output
ORACLE_FDW_PACKAGE=oracle_fdw-ORACLE_FDW_2_2_0
ORACLE_FDW_UNZIPPED_PACKAGE=oracle_fdw-ORACLE_FDW_2_2_0
ORACLE_FDW_PATCH=opengauss_oracle_fdw-2.2.0_patch
ORACLE_FDW_MEGRED_SOURCES_DIR=$(ORACLE_FDW_DIR)/code

.PHONY: oracle_fdw_target
oracle_fdw_target: prep_checked
	@$(call create_oracle_fdw_sources)
	@make -C $(ORACLE_FDW_MEGRED_SOURCES_DIR)/$(ORACLE_FDW_UNZIPPED_PACKAGE) NO_PGXS=1

prep_checked:
	@test -f $(ORCFDW_HOME)/$(ORACLE_FDW_PACKAGE).tar.gz || ( echo "ERROR: You need copy oracle_fdw from 'third_party' repo to 'third_party_binarylibs' repo and keep directory strucutre unchanged" && exit 1 )
	@test -f $(ORCFDW_HOME)/$(ORACLE_FDW_PACKAGE).tar.gz && date > prep_checked

.PHONY: install-data
install-data: oracle_fdw_target
	@make -C $(ORACLE_FDW_MEGRED_SOURCES_DIR)/$(ORACLE_FDW_UNZIPPED_PACKAGE) NO_PGXS=1 install

uninstall distclean clean:
	@rm -rf $(ORACLE_FDW_DIR)

define create_oracle_fdw_sources
	rm -rf $(ORACLE_FDW_DIR);\
	mkdir $(ORACLE_FDW_DIR);\
	cp -r $(ORCFDW_HOME)/. $(ORACLE_FDW_DIR);\
	mkdir $(ORACLE_FDW_MEGRED_SOURCES_DIR); \
	tar -xzvf $(ORACLE_FDW_DIR)/$(ORACLE_FDW_PACKAGE).tar.gz -C $(ORACLE_FDW_MEGRED_SOURCES_DIR) &> /dev/null; \
	patch -p0 -d $(ORACLE_FDW_MEGRED_SOURCES_DIR)/$(ORACLE_FDW_UNZIPPED_PACKAGE) < $(ORACLE_FDW_DIR)/$(ORACLE_FDW_PATCH).patch &> /dev/null;
endef
