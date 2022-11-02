#
#Copyright (c) 2020 Huawei Technologies Co.,Ltd.
#
#openGauss is licensed under Mulan PSL v2.
#You can use this software according to the terms and conditions of the Mulan PSL v2.
#You may obtain a copy of Mulan PSL v2 at:
#
#          http://license.coscl.org.cn/MulanPSL2
#
# THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
# EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
# MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
# See the Mulan PSL v2 for more details.
#-------------------------------------------------------------------------
#
# Makefile--
#    Makefile for codegen
#
# IDENTIFICATION
#    src/gausskernel/runtime/codegen/Makefile
#
#-------------------------------------------------------------------------

subdir = src/gausskernel/runtime/codegen
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global

override CPPFLAGS:=-std=c++14 $(CPPFLAGS)
ifneq "$(MAKECMDGOALS)" "clean"
  ifneq "$(MAKECMDGOALS)" "distclean"
    ifneq "$(shell which g++ |grep hutaf_llt |wc -l)" "1"
      -include $(DEPEND)
    endif
  endif
endif
OBJS = gscodegen.o

# append include directory about zlib1.2.8
override CPPFLAGS += -I$(LIBLLVM_INCLUDE_PATH) -D_DEBUG -D_GNU_SOURCE -D__STDC_CONSTANT_MACROS -D__STDC_FORMAT_MACROS -D__STDC_LIMIT_MACROS -O2 -fomit-frame-pointer -fvisibility-inlines-hidden -fexceptions -fno-rtti  -L$(LIBLLVM_LIB_PATH) -lz -pthread -D_REENTRANT -lncurses -lrt -ldl -lm $(LLVM_LIBS)
override mywarningflags:=-Wall -Werror -Wendif-labels -Wformat-security -Wmissing-format-attribute -Wpointer-arith -Wno-write-strings -Wno-attributes -Wno-unused-but-set-variable
override CPPFLAGS:=$(filter-out $(mywarningflags),$(CPPFLAGS))


SUBDIRS = executor vecexecutor codegenutil

.PHONY: install-data
install-data:
ifeq "$(host_cpu)" "aarch64"
	$(INSTALL_DATA) $(srcdir)/llvmir/GaussDB_expr_aarch64.ir '$(DESTDIR)$(llvmirdir)/GaussDB_expr.ir'
else
	$(INSTALL_DATA) $(srcdir)/llvmir/GaussDB_expr.ir '$(DESTDIR)$(llvmirdir)/GaussDB_expr.ir'
endif

.PHONY: uninstall-data
uninstall-data:
	rm -f $(addprefix '$(DESTDIR)$(llvmirdir)'/, GaussDB_expr.ir)

include $(top_srcdir)/src/gausskernel/common.mk

###########################################
# Set the warning option of compiler
# ###########################################
override CXXFLAGS:=$(filter-out $(mywarningflags),$(CXXFLAGS))
override CXXFLAGS+=$(mywarningflags)

