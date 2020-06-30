
# Makefile for the tde module
#
# src/gausskernel/security/keymanagement/Makefile
#

top_builddir=../../../..
include $(top_builddir)/src/Makefile.global
subdir=src/gausskernel/security/keymanagement

override CPPFLAGS += -I$(LIBOPENSSL_INCLUDE_PATH) -I$(LIBCURL_INCLUDE_PATH)

ifneq "$(MAKECMDGOALS)" "clean"
  ifneq "$(MAKECMDGOALS)" "distclean"
    ifneq "$(shell which g++ |grep hutaf_llt |wc -l)" "1"
      -include $(DEPEND)
    endif
  endif
endif
OBJS = FIKMSmessage.o  HttpRestfulClient.o  KeyManager.o  KeyRecord.o TDEKeysFile.o TDEKeysRecords.pb.o

include $(top_srcdir)/src/gausskernel/common.mk

# generate TDEKeysRecords protoc-c code.
 
# TDEKeysRecords.pb.cc: TDEKeysRecords.proto
#       protoc --cpp_out=. TDEKeysRecords.proto
