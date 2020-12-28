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
# 
# 
# IDENTIFICATION
#        src/bin/license_checker/Makefile
# 
# ---------------------------------------------------------------------------------------
top_builddir=../../..
include $(top_builddir)/src/Makefile.global
subdir=src/bin/license_checker

LIBS := $(filter-out -lpgport -lz -ledit -lssl -lcrypto, $(LIBS))

ifneq "$(MAKECMDGOALS)" "clean"
  ifneq "$(MAKECMDGOALS)" "distclean"
    ifneq "$(shell which g++ |grep hutaf_llt |wc -l)" "1"
      -include $(DEPEND)
    endif
  endif
endif
OBJS = main.o callback.o 

all:license_checker

license_checker: $(OBJS) 
	$(CC) $(CPPFLAGS) $^ $(LDFLAGS) $(LDFLAGS_EX) $(LIBS) -o $@$(X) -llm

install: all installdirs
	$(INSTALL_PROGRAM) license_checker$(X) '$(DESTDIR)$(bindir)/license_checker$(X)'
	$(INSTALL_PROGRAM) license.dat '$(DESTDIR)$(bindir)/license.dat'

installdirs:
	$(MKDIR_P) '$(DESTDIR)$(bindir)'

uninstall:
	rm -f '$(DESTDIR)$(bindir)/license_checker$(X)'

clean distclean maintainer-clean:
	rm -f license_checker$(X) $(OBJS) *.depend

