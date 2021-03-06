#-------------------------------------------------------------------------
#
# Copyright (c) 2020 Huawei Technologies Co.,Ltd.
#
# src/bin/gs_tool/Makefile
#
#-------------------------------------------------------------------------

subdir = src/bin/gs_ktool
top_builddir = ../../..

include $(top_builddir)/src/Makefile.global

override CPPFLAGS +=  -I. -I$(top_builddir)/src/include/gs_ktool -I$(LIBKMC_INCLUDE_PATH)
override LDFLAGS +=  -L. -L$(LIBKMC_LIB_PATH)
override LDLIBS += -lsecurec -lssl -lcrypto -lkmc -ldl -lrt -Wl,-z,relro,-z,now -ftrapv -fstack-protector-strong

override CPPFLAGS := $(filter-out -fPIE, $(CPPFLAGS)) -fPIC -shared

OBJS = kt_common.o kt_kmc_callback.o kt_log_manage.o kt_key_manage.o kt_interface.o  kt_main.o 

all: gs_ktool libgs_ktool.so

gs_ktool: $(OBJS)
	$(CC) -fPIE -pie $(CPPLAGS) $^ $(LDFLAGS) $(LDLIBS) -o $@
  
libgs_ktool.so: $(OBJS)
	$(CC) -fPIC -shared $(CPPFLAGS) $^ $(LDFLAGS) $(LDLIBS) -o $@

install: all installdirs
	$(INSTALL_PROGRAM) gs_ktool$(X) '$(DESTDIR)$(bindir)/gs_ktool$(X)'
	$(INSTALL_DATA) gs_ktool_conf.ini$(X) '$(DESTDIR)$(bindir)/../etc/gs_ktool_file/gs_ktool_conf.ini$(X)'
	$(INSTALL_DATA) libgs_ktool.so$(X) '$(DESTDIR)$(bindir)/../lib/libgs_ktool.so$(X)'
	$(INSTALL_DATA) $(LIBKMC_LIB_PATH)/libkmc.so$(X) '$(DESTDIR)$(bindir)/../lib/libkmc.so$(X)'
	rm -f ./gs_ktool

installdirs:
	$(MKDIR_P) '$(DESTDIR)$(bindir)'

clean distclean maintainer-clean:
	rm -f ./*.o ./gs_ktool ./*.so

