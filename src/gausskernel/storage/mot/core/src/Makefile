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
#     Makefile for the MOT storage engine core components
#
# IDENTIFICATION
#        src/gausskernel/storage/mot/core/src/Makefile
#
# ---------------------------------------------------------------------------------------

all: mot_target


subdir=src/gausskernel/storage/mot/core/src
top_builddir ?= ../../../../../../
MASSTREE_DIR=$(top_builddir)/third_party/dependency/masstree
MASSTREE_PACKAGE=masstree-beta-0.9.0
MOT_MASSTREE_PATCH=huawei_masstree-beta-0.9.0_patch
MASSTREE_MEGRED_SOURCES_DIR=$(MASSTREE_DIR)/code


mot_target:
	@$(call create_masstree_sources)
	@make -f Makefile.local

clean:
	@rm -rf $(MASSTREE_MEGRED_SOURCES_DIR)
	@make -f Makefile.local clean

MASSTREE_RELEVANT_SOURCES = \
		btree_leaflink.hh \
		circular_int.hh \
		compiler.cc \
		compiler.hh \
		hashcode.hh \
		kpermuter.hh \
		ksearch.hh   \
		masstree_get.hh \
		masstree.hh \
		masstree_insert.hh \
		masstree_key.hh \
		masstree_remove.hh \
		masstree_scan.hh \
		masstree_split.hh \
		masstree_struct.hh \
		masstree_tcursor.hh \
		memdebug.cc \
		memdebug.hh \
		mtcounters.hh \
		kvthread.cc \
		kvthread.hh \
		nodeversion.hh \
		small_vector.hh \
		straccum.cc \
		straccum.hh \
		str.hh \
		stringbag.hh \
		string_base.hh \
		string.cc \
		string.hh \
		string_slice.hh \
		timestamp.hh

define create_masstree_sources
   rm -rf $(MASSTREE_DIR)/tmp; \
   rm -rf $(MASSTREE_MEGRED_SOURCES_DIR); \
   mkdir $(MASSTREE_DIR)/tmp; \
   mkdir $(MASSTREE_MEGRED_SOURCES_DIR); \
   tar xfzv $(MASSTREE_DIR)/$(MASSTREE_PACKAGE).tar.gz -C $(MASSTREE_DIR)/tmp &> /dev/null; \
   for src_file in $(MASSTREE_RELEVANT_SOURCES); \
   do \
	cp $(MASSTREE_DIR)/tmp/$(MASSTREE_PACKAGE)/$$src_file $(MASSTREE_MEGRED_SOURCES_DIR); \
   done
   rename ".cc" ".cpp" $(MASSTREE_MEGRED_SOURCES_DIR)/*.cc; \
   rm -rf $(MASSTREE_DIR)/tmp; \
   patch -d $(MASSTREE_MEGRED_SOURCES_DIR) < $(MASSTREE_DIR)/$(MOT_MASSTREE_PATCH).patch &> /dev/null;
endef

