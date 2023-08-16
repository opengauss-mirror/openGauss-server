subdir = src/gausskernel/storage/replication
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global

override CPPFLAGS := -I$(srcdir) $(CPPFLAGS) -I$(libpq_srcdir)

FLEXFLAGS = -CF -b -p -i

ifneq "$(MAKECMDGOALS)" "clean"
  ifneq "$(MAKECMDGOALS)" "distclean"
    ifneq "$(shell which g++ |grep hutaf_llt |wc -l)" "1"
      -include $(DEPEND)
    endif
  endif
endif
OBJS = walsender.o datasender.o walreceiverfuncs.o walreceiver.o walrcvwriter.o subscription_walreceiver.o\
	datareceiver.o datarcvwriter.o basebackup.o libpqwalreceiver.o archive_walreceiver.o repl_gram.o\
	syncrep.o dataqueue.o bcm.o datasyncrep.o catchup.o slot.o slotfuncs.o shared_storage_walreceiver.o\
	syncrep_gram.o heartbeat.o rto_statistic.o libpqsw.o ss_cluster_replication.o
SUBDIRS = logical heartbeat dcf

include $(top_srcdir)/src/gausskernel/common.mk

# repl_scanner is compiled as part of repl_gram
repl_gram.o: repl_scanner.inc

# See notes in src/common/parser/Makefile about the following two rules
repl_gram.hpp: repl_gram.y repl_gram.cpp
ifdef BISON
	$(BISON) -d $(BISONFLAGS) -o repl_gram.cpp $<
else
	@$(missing) bison $< $@
endif

repl_gram.cpp: repl_gram.y
ifdef BISON
	$(BISON) -d $(BISONFLAGS) -o $@ $<
else
	@$(missing) bison $< $@
endif

repl_scanner.inc: repl_scanner.l
ifdef FLEX
	$(FLEX) $(FLEXFLAGS) -o'$@' $<
	sed -i "s/YY_NULL/YY_ZERO/g" repl_scanner.inc
	@rm -f lex.backup
else
	@$(missing) flex $< $@
	sed -i 's/YY_NULL/YY_ZERO/g' repl_scanner.inc	
endif

# repl_gram.cpp and repl_scanner.inc are in the distribution tarball, so
# they are not cleaned here.

# scan is compiled as part of gram
syncrep_gram.o: syncrep_scanner.inc

# Latest flex causes warnings in this file.
ifeq ($(GCC),yes)
syncrep_gram.o: CXXFLAGS += -Wno-error
endif

syncrep_gram.hpp: syncrep_gram.y syncrep_gram.cpp
ifdef BISON
	$(BISON) -d $(BISONFLAGS) -o syncrep_gram.cpp $<
else
	@$(missing) bison $< $@
endif

syncrep_gram.cpp: syncrep_gram.y
ifdef BISON
	$(BISON) -d $(BISONFLAGS) -o $@ $<
else
	@$(missing) bison $< $@
endif

syncrep_scanner.inc: syncrep_scanner.l
ifdef FLEX
	$(FLEX) $(FLEXFLAGS) -o'$@' $<
	@if [ `wc -l <lex.backup` -eq 1 ]; then rm -f lex.backup; else echo "Warning: scanner may require backup, see lex.backup."; rm -f lex.backup; fi
	sed -i 's/YY_NULL/YY_ZERO/g' syncrep_scanner.inc
else
	@$(missing) flex $< $@
	sed -i 's/YY_NULL/YY_ZERO/g' syncrep_scanner.inc
endif
