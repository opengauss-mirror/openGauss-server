# ---------------------------------------------------------------------------------------
# 
# Makefile 1
#        Makefile for jdbc client logic jni so
# 
# IDENTIFICATION
#        src/common/interfaces/libpq/jdbc/client_logic_jni/Makefile
# 
# ---------------------------------------------------------------------------------------

subdir = src/common/interfaces/libpq/jdbc/client_logic_jni/
top_builddir = ../../../../../../
include $(top_builddir)/src/Makefile.global
NAME= libgauss_cl_jni

INCLUDES = -I$(top_builddir)/src/include -I"$(JAVA_HOME)/include" -I"$(JAVA_HOME)/include/linux" -I$(top_builddir)/src/include/libpq -I../../ -I$(LIBOPENSSL_INCLUDE_PATH) -I$(KERBEROS_INCLUDE_PATH)
SHLIB_LINK :=  -L"../../" -lpq_ce -lrt -lz -L$(CJSON_LIB_PATH) -L$(LIBCURL_LIB_PATH) -lcjson -lcurl
ifneq ($(enable_multiple_nodes)_$(enable_privategauss), no_no)
  ifneq ($(enable_lite_mode), yes)
    SHLIB_LINK += -L$(top_builddir)/../distribute/bin/gs_ktool/ -lgs_ktool -l$(SECURE_C_CHECK) -L$(LIBKMC_LIB_PATH) -lkmc
  endif
endif
override CPPFLAGS := ${INCLUDES} -I$(CJSON_INCLUDE_PATH) -I$(LIBCURL_INCLUDE_PATH) -DHAVE_CE 

CPP_SRCS = \
client_logic_jni.cpp \
driver_error.cpp \
jni_string_convertor.cpp \
jni_util.cpp \
jni_logger.cpp \
org_postgresql_jdbc_ClientLogicImpl.cpp\
org_opengauss_jdbc_ClientLogicImpl.cpp

OBJS = $(CPP_SRCS:.cpp=.o)

all: libgauss_cl_jni.so

# Makefile.shlib has declare target: libgauss_cl_jni.so
libgauss_cl_jni.so: $(OBJS)

install: all installdirs
	$(INSTALL_DATA) libgauss_cl_jni.so$(X) '$(DESTDIR)$(bindir)/../lib/libgauss_cl_jni.so$(X)'

installdirs:
	$(MKDIR_P) '$(DESTDIR)$(bindir)'

include $(top_srcdir)/src/Makefile.shlib

clean distclean: clean-lib
	rm -f $(OBJS)
