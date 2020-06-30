#-------------------------------------------------------------------------
#
# Makefile for common/backend/tsearch
#
# Copyright (c) 2006-2012, PostgreSQL Global Development Group
#
# src/common/backend/tsearch/Makefile
#
#-------------------------------------------------------------------------
subdir = src/common/backend/tsearch
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global

DICTDIR=tsearch_data

# List of dictionaries files
DICTFILES=synonym_sample.syn thesaurus_sample.ths \
	hunspell_sample.affix \
	ispell_sample.affix ispell_sample.dict \
	hunspell_sample_long.affix hunspell_sample_long.dict \
	hunspell_sample_num.affix hunspell_sample_num.dict

# Local paths to dictionaries files
DICTFILES_PATH=$(addprefix dicts/,$(DICTFILES))

ifneq "$(MAKECMDGOALS)" "clean"
  ifneq "$(MAKECMDGOALS)" "distclean"
    ifneq "$(shell which g++ |grep hutaf_llt |wc -l)" "1"
      -include $(DEPEND)
    endif
  endif
endif
OBJS = ts_locale.o ts_parse.o wparser.o wparser_def.o dict.o \
	dict_simple.o dict_synonym.o dict_thesaurus.o \
	dict_ispell.o regis.o spell.o ts_zh_ngram.o \
	to_tsany.o ts_selfuncs.o ts_typanalyze.o ts_utils.o ts_zh_zhparser.o ts_zh_pound.o

include $(top_srcdir)/src/gausskernel/common.mk

.PHONY: install-data
install-data: $(DICTFILES_PATH) installdirs
	$(INSTALL_DATA) $(addprefix $(srcdir)/,$(DICTFILES_PATH)) '$(DESTDIR)$(datadir)/$(DICTDIR)/'

installdirs:
	$(MKDIR_P) '$(DESTDIR)$(datadir)' '$(DESTDIR)$(datadir)/$(DICTDIR)'

.PHONY: uninstall-data
uninstall-data:
	rm -rf $(addprefix '$(DESTDIR)$(datadir)/$(DICTDIR)/',$(DICTFILES))
