#-------------------------------------------------------------------------
#
# Copyright (c) 2020 Huawei Technologies Co.,Ltd.
#
# src/common/interfaces/libpq/client_logic_hooks/cmk_entity_manager_hooks/convert_jsonfile/Makefile
#
#-------------------------------------------------------------------------

subdir = src/common/interfaces/libpq/client_logic_hooks/cmk_entity_manager_hooks/convert_jsonfile
top_builddir = ../../../../../../../

include $(top_builddir)/src/Makefile.global

override CPPFLAGS +=  -I.. -I. -I$(CJSON_INCLUDE_PATH) -I$(LIBCURL_INCLUDE_PATH)
override CPPFLAGS +=  -g
override LDFLAGS +=  -L. -L$(CJSON_LIB_PATH) -L$(LIBCURL_LIB_PATH)
override LDLIBS += -lcurl -lcjson -lsecurec

override CPPFLAGS := $(filter-out -fPIE, $(CPPFLAGS)) -fPIC -shared

OBJS = ../common.o convjson_common.o convjson.o

all: convjson

convjson: $(OBJS)
	cp $(CJSON_LIB_PATH)/* ./
	gcc -fPIE -pie $(CPPLAGS) $^ $(LDFLAGS) $(LDLIBS) -o $@

clean distclean maintainer-clean:
	rm -f ./*.o ./convjson libcjson* kms_httpmsg_temp.ini
