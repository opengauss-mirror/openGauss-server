#-------------------------------------------------------------------------
#
# Copyright (c) 2020 Huawei Technologies Co.,Ltd.
#
# src/common/interfaces/libpq/client_logic_hooks/cmk_entity_manager_hooks/
#
#-------------------------------------------------------------------------

subdir = src/common/interfaces/libpq/client_logic_hooks/cmk_entity_manager_hooks/
top_builddir = ../../../../../../

include $(top_builddir)/src/Makefile.global

override CPPFLAGS +=  -I. -I$(top_builddir)/src/include -I../encryption_hooks
override LDFLAGS +=  -L. -L$(LIBOPENSSL_LIB_PATH)
override LDLIBS += -l$(SECURE_C_CHECK) -lssl -lcrypto
CMKEM_OBJS = common.o cmkem_comm_algorithm.o reg_hook_frame.o

override CPPFLAGS += -ftrapv -fstack-protector-strong
override LDFLAGS +=  -ldl -lrt -Wl,-z,relro,-z,now

# add new cmk managerment tool/components/services here 						   #
# if you need to copy .so/.a to install lib dir, please use '$(INSTALL_DATA) ...'  #

# (1) gs_ktool
override CPPFLAGS += -I$(top_builddir)/src/include/gs_ktool -I$(LIBKMC_INCLUDE_PATH)
override LDFLAGS += -L$(LIBKMC_LIB_PATH)
override LDLIBS += -lkmc -lgs_ktool
CMKEM_OBJS += register_gs_ktool.o

# (2) localkms
override CPPFLAGS += -I$(CJSON_INCLUDE_PATH) -I$(LIBCURL_INCLUDE_PATH)
override LDFLAGS += -L$(CJSON_LIB_PATH) -L$(LIBCURL_LIB_PATH)
override LDLIBS += -lcjson -lcurl
CMKEM_OBJS += register_local_kms.o

# (3) huawei_kms
override CPPFLAGS += 
override LDFLAGS +=
override LDLIBS += 
CMKEM_OBJS += register_huawei_kms.o

# (4) remain to add new tool/components/services
# ...

# (x) grammar_check 
CMKEM_OBJS += register_gram_check.o

### end ###


CMKEM_OBJS += reg_cmkem_manager_main.o
override CPPFLAGS := $(filter-out -fPIE, $(CPPFLAGS)) -fPIC -shared

BIN_OBJS = $(CMKEM_OBJS) test.o

all: libcmk_entity_manager_hooks.so

libcmk_entity_manager_hooks.so: $(CMKEM_OBJS)
	echo $(CMKEM_OBJS)
	$(CC) -fPIC -shared $(CPPFLAGS) $^ $(LDFLAGS) $(LDLIBS) -o $@

install: cmktest installdirs

cmktest: $(BIN_OBJS)
	echo $(LDLIBS)
	echo $(BIN_OBJS)
	#gcc $(BIN_OBJS) $(CPPFLAGS) $(LDFLAGS) -L. -lcmk_entity_manager_hooks -lgs_ktool  -lcjson -lcurl -o $@
	$(CC) -fPIE -pie $(CPPLAGS) $^ $(LDFLAGS) $(LDLIBS) -lcmk_entity_manager_hooks -o $@

installdirs: cmktest
	$(INSTALL_DATA) libgs_ktool.so$(X) '$(DESTDIR)$(bindir)/../lib/libgs_ktool.so$(X)'
	$(INSTALL_DATA) libcmk_entity_manager_hooks.so$(X) '$(DESTDIR)$(bindir)/../lib/libcmk_entity_manager_hooks.so$(X)'
	$(INSTALL_PROGRAM) cmktest$(X) '$(DESTDIR)$(bindir)/cmktest$(X)'

clean distclean maintainer-clean:
	rm -f ./*.o libcmk_entity_manager_hooks.so cmktest core.*
