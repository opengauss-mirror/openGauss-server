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
MYSQL_FDW_DIR=$(top_builddir)/third_party/dependency/mysql_fdw
MYSQL_FDW_PACKAGE=mysql_fdw-REL-2_5_3
MYSQL_FDW_PATCH=huawei_mysql_fdw-2.5.3_patch
MYSQL_FDW_MEGRED_SOURCES_DIR=$(MYSQL_FDW_DIR)/code

.PHONY: mysql_fdw_target
mysql_fdw_target:
	@$(call create_mysql_fdw_sources)
	@make -C $(MYSQL_FDW_MEGRED_SOURCES_DIR)/$(MYSQL_FDW_PACKAGE)

.PHONY: install-data
install-data: mysql_fdw_target
	@make -C $(MYSQL_FDW_MEGRED_SOURCES_DIR)/$(MYSQL_FDW_PACKAGE) install

uninstall distclean clean:
	@rm -rf $(MYSQL_FDW_MEGRED_SOURCES_DIR)

MYSQL_FDW_RELEVANT_SOURCES = connection.c deparse.c mysql_fdw.c mysql_query.c option.c

define create_mysql_fdw_sources
	rm -rf $(MYSQL_FDW_MEGRED_SOURCES_DIR); \
	mkdir $(MYSQL_FDW_MEGRED_SOURCES_DIR); \
	tar xfzv $(MYSQL_FDW_DIR)/$(MYSQL_FDW_PACKAGE).tar.gz -C $(MYSQL_FDW_MEGRED_SOURCES_DIR) &> /dev/null; \
	rename ".c" ".cpp" $(MYSQL_FDW_MEGRED_SOURCES_DIR)/$(MYSQL_FDW_PACKAGE)/*.c; \
	patch -d $(MYSQL_FDW_MEGRED_SOURCES_DIR)/$(MYSQL_FDW_PACKAGE) < $(MYSQL_FDW_DIR)/$(MYSQL_FDW_PATCH).patch &> /dev/null;
endef
