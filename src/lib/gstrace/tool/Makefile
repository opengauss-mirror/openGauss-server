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
#        MPPDB gstrace tool Makefile
# 
# IDENTIFICATION
#        src/lib/gstrace/tool/Makefile
# 
# ---------------------------------------------------------------------------------------

top_builddir=../../../..
include $(top_builddir)/src/Makefile.global
subdir=src/gstrace/tool/
loginfo="hello test"

$(info, $(LIBS))
ifneq ($(PORTNAME), win32)
    override CFLAGS += $(PTHREAD_CFLAGS) -L$(LIBOPENSSL_LIB_PATH) -lssl -lcrypto -L$(LIBCGROUP_LIB_PATH) -lcgroup
    ifeq ($(enable_lite_mode), no)
        override CFLAGS += -lgssapi_krb5_gauss -lkrb5_gauss -lk5crypto_gauss -lcom_err_gauss -lkrb5support_gauss
    endif
    override CPPFLAGS := -I$(LIBETCDAPI_INCLUDE_PATH) -I$(LIBCGROUP_INCLUDE_PATH) $(CPPFLAGS)
endif

##############################################################################
# memory checking component
###############################################################################
ifeq ($(enable_memory_check), yes)
    LDFLAGS += -L$(MEMCHECK_LIB_PATH)
  else
    ifeq ($(enable_thread_check), yes)
      LDFLAGS += -L$(MEMCHECK_LIB_PATH)
    endif
endif

ifeq ($(enable_memory_check), yes)
  LIBS += -l$(MEMCHECK_LIB_NAME_ASAN)
  LDFLAGS += -fsanitize=address -fsanitize=leak -fno-omit-frame-pointer
  CXXFLAGS += -fsanitize=address -fsanitize=leak -fno-omit-frame-pointer
else
  ifeq ($(enable_thread_check), yes)
    LIBS += -l$(MEMCHECK_LIB_NAME_TSAN)
    LDFLAGS +=  -fsanitize=thread -fno-omit-frame-pointer
    CXXFLAGS += -fsanitize=thread -fno-omit-frame-pointer
  endif
endif

include $(top_srcdir)/src/gausskernel/common.mk

ifneq "$(MAKECMDGOALS)" "clean"
  ifneq "$(MAKECMDGOALS)" "distclean"
    ifneq "$(shell which g++ |grep hutaf_llt |wc -l)" "1"
      -include $(DEPEND)
    endif
  endif
endif

override CXXFLAGS += -fPIE
LDFLAGS += -pie

OBJS = gstrace_tool.o $(top_builddir)/src/lib/gstrace/common/libgstrace.a

all: gstrace

gstrace: $(OBJS)
	$(CC) $(CFLAGS) $(LDFLAGS) $(call expand_subsys,$^) $(LIBS) -Wl,-z,relro,-z,now -DENABLE_GSTRACE -o $@

install: all installdirs
	$(INSTALL_PROGRAM) gstrace$(X) '$(DESTDIR)$(bindir)/gstrace$(X)'

installdirs:
	$(MKDIR_P) '$(DESTDIR)$(bindir)'

uninstall:
	rm -f '$(DESTDIR)$(bindir)/gstrace$(X)'

clean distclean maintainer-clean:
	rm -f gstrace$(X) $(OBJS) *.depend

$(top_builddir)/src/lib/gstrace/common/libgstrace.a:
	$(MAKE) -C $(top_builddir)/src/lib/gstrace/common/

