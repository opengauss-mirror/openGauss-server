# contrib/common_cipher/Makefile

SRCS		= common_err.cpp common_utils.cpp common_algo.cpp common_internal_interfaces.cpp common_cipher.cpp

MODULE_big	= common_cipher
OBJS		= $(SRCS:.cpp=.o)

subdir = contrib/common_cipher
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk

override CPPFLAGS := -fPIC -shared 
override CFLAGS :=  -fPIC -shared
