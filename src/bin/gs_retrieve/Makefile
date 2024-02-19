#
# Copyright (c) 2021 Huawei Technologies Co.,Ltd.
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
# IDENTIFICATION
#        src/bin/gs_retrieve/Makefile
# 
# ---------------------------------------------------------------------------------------

subdir = src/bin/gs_retrieve
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

override CPPFLAGS := -I$(libpq_srcdir) $(CPPFLAGS) -DHAVE_LIBZ -DFRONTEND -I${top_builddir}/src/include
ifeq ($(enable_lite_mode), no)
    LIBS += -lgssapi_krb5_gauss -lgssrpc_gauss -lkrb5_gauss -lkrb5support_gauss -lk5crypto_gauss -lcom_err_gauss
endif
ifneq "$(MAKECMDGOALS)" "clean"
  ifneq "$(MAKECMDGOALS)" "distclean"
    ifneq "$(shell which g++ |grep hutaf_llt |wc -l)" "1"
      -include $(DEPEND)
    endif
  endif
endif
OBJS = gs_retrieve.o xlogreader.o xlogreader_common.o $(top_builddir)/src/lib/elog/elog.a \
			$(top_builddir)/src/lib/pgcommon/libpgcommon.a

OBJS += $(top_builddir)/src/gausskernel/storage/dss/dss_adaptor.o

all: gs_retrieve

gs_retrieve: $(OBJS) | submake-libpq submake-libpgport
	$(CC) -fPIC $(CXXFLAGS) $(OBJS) $(LIBS) $(libpq_pgport) $(LDFLAGS) $(LDFLAGS_EX) -o $@$(X)

xlogreader.cpp: % : $(top_srcdir)/src/gausskernel/storage/access/transam/%
	rm -f $@ && $(LN_S) $< .
xlogreader_common.cpp: % : $(top_srcdir)/src/gausskernel/storage/access/redo/%
	rm -f $@ && $(LN_S) $< .

install: all installdirs
	$(INSTALL_PROGRAM) gs_retrieve$(X) '$(DESTDIR)$(bindir)/gs_retrieve$(X)'

installdirs:
	$(MKDIR_P) '$(DESTDIR)$(bindir)'

uninstall:
	rm -f '$(DESTDIR)$(bindir)/gs_retrieve$(X)'

clean distclean maintainer-clean:
	rm -f gs_retrieve$(X) $(OBJS) *.depend

$(top_builddir)/src/lib/elog/elog.a:
	$(MAKE) -C $(top_builddir)/src/lib/elog elog.a
$(top_builddir)/src/lib/pgcommon/libpgcommon.a:
	$(MAKE) -C $(top_builddir)/src/lib/pgcommon libpgcommon.a
