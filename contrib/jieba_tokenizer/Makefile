# contrib/jieba_tokenizer/Makefile

MODULE_big = jiebatokenizer
OBJS = tokenizer.o
NAME = $(MODULE_big)
top_builddir = ../..
subdir = contrib/jieba_tokenizer

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/src/gausskernel/common.mk
override CXXFLAGS += -I$(top_builddir)/contrib/jieba_tokenizer/ -I$(top_builddir)/contrib/jieba_tokenizer/include -I$(top_builddir)/contrib/jieba_tokenizer/deps
endif

DICT = jieba_dict

install:install-data
.PHONY: install-data
install-data:
	cp -r $(addprefix $(srcdir)/, $(DICT)) '$(DESTDIR)$(libdir)/'

uninstall:uninstall-data
.PHONY: uninstall-data
uninstall-data:
	rm -r $(addprefix '$(DESTDIR)$(libdir)/', $(DICT))

clean:

