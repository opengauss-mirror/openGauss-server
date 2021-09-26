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
#        Makefile for gs_log
# 
# IDENTIFICATION
#        src/bin/gs_log/Makefile
# 
# ---------------------------------------------------------------------------------------

PGFILEDESC = "gs_log - Gauss tools for logs, including profile logs"

subdir = src/bin/gs_log
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

override CPPFLAGS := -I$(top_builddir)/src/common/timezone -I$(top_builddir)/src/include/  -fPIE $(CPPFLAGS)

# use system timezone data?
ifneq (,$(with_system_tzdata))
override CPPFLAGS += '-DSYSTEMTZDIR="$(with_system_tzdata)"'
endif

# for clean && distclean
ifneq "$(MAKECMDGOALS)" "clean"
  ifneq "$(MAKECMDGOALS)" "distclean"
    ifneq "$(shell which g++ |grep hutaf_llt |wc -l)" "1"
      -include $(DEPEND)
    endif
  endif
endif

OBJS = gs_log_dump.o gs_log.o localtime.o strftime.o findtimezone.o $(top_builddir)/src/lib/elog/elog.a

include $(top_builddir)/src/gausskernel/common.mk

##############################################################################
# memory checking component
###############################################################################
ifeq ($(enable_memory_check), yes)
  LDFLAGS += -L$(MEMCHECK_LIB_PATH) -fsanitize=address -fsanitize=leak -fno-omit-frame-pointer -l$(MEMCHECK_LIB_NAME_ASAN) -ldl
  CXXFLAGS += -fsanitize=address -fsanitize=leak -fno-omit-frame-pointer
else
  ifeq ($(enable_thread_check), yes)
    LDFLAGS += -L$(MEMCHECK_LIB_PATH) -fsanitize=thread -fno-omit-frame-pointer -l$(MEMCHECK_LIB_NAME_TSAN) -ldl
    CXXFLAGS += -fsanitize=thread -fno-omit-frame-pointer
  endif
endif
LDFLAGS += -pie
# Likewise, pull in localtime.cpp from src/timezones
localtime.cpp: % : $(top_builddir)/src/common/timezone/%
	rm -f $@ && $(LN_S) $< .

strftime.cpp: % : $(top_builddir)/src/common/timezone/%
	rm -f $@ && $(LN_S) $< .

findtimezone.cpp: % : $(top_builddir)/src/bin/initdb/%
	rm -f $@ && $(LN_S) $< .

$(top_builddir)/src/lib/elog/elog.a:
	$(MAKE) -C $(top_builddir)/src/lib/elog elog.a

all:gs_log
gs_log: $(OBJS) | submake-libpgport
	$(CC) $(CXXFLAGS) $(OBJS) $(LDFLAGS) $(LDFLAGS_EX) $(LIBS) -o $@$(X)

clean distclean maintainer-clean:
	rm -f localtime.*  strftime.* findtimezone.* 
	rm -rf *.so *.a *.o 
	rm -rf *.depend
	rm -rf gs_log

install: all installdirs
	$(INSTALL_PROGRAM) gs_log$(X) '$(DESTDIR)$(bindir)/gs_log$(X)'

utgslog: $(OBJS) | submake-libpq submake-libpgport
	$(CC) -fPIC -shared $(CXXFLAGS) $(OBJS)  $(LDFLAGS) $(LDFLAGS_EX) -o libutgslog.so
	mv libutgslog.so $(top_builddir)/../distribute/test/ut/lib
