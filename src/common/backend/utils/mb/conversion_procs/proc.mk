SRCS		+= $(NAME).cpp
OBJS		+= $(NAME).o

rpath =

all: all-shared-lib

include $(top_srcdir)/src/Makefile.shlib

ifeq ($(enable_debug), no)
    override CXXFLAGS := $(filter-out -fstack-protector, $(CFLAGS)) -fstack-protector-all -Wl,-z,relro,-z,now
else
    override CXXFLAGS := $(filter-out -fstack-protector, $(CFLAGS)) -fstack-protector-all -Wl,-z,relro,-z,now -fPIC
endif

install: all installdirs install-lib

installdirs: installdirs-lib

uninstall: uninstall-lib

clean distclean maintainer-clean: clean-lib
	rm -f $(OBJS)
