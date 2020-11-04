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
#     Makefile for the postgis
#
# IDENTIFICATION
#        contrib/postgis/Makefile
#
# ---------------------------------------------------------------------------------------

install:install-data

top_builddir = ../../
POSTGIS_DIR=$(top_builddir)/third_party/dependency/postgis
#source code
POSTGIS_PACKAGE=postgis-xc

.PHONY: install-data
install-data:
        @$(call install_postgis)

uninstall distclean clean:
        @rm -rf $(POSTGIS_DIR)/$(POSTGIS_PACKAGE)

define install_postgis
        cd $(POSTGIS_DIR); \
        sh install_postgis.sh;  \
        cd - ;
endef

