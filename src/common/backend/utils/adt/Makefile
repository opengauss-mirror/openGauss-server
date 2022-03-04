#
# Makefile for utils/adt
#
# src/common/backend/utils/adt/Makefile
#

subdir = src/common/backend/utils/adt
top_builddir = ../../../../..
include $(top_builddir)/src/Makefile.global

# seems to be required for some date/time stuff 1999/07/22 bjm
ifeq "$(findstring alpha,$(host_cpu))" "alpha"
ifeq "$(GCC)" "yes"
override CFLAGS+= -mieee
endif
endif

ifneq "$(MAKECMDGOALS)" "clean"
  ifneq "$(MAKECMDGOALS)" "distclean"
    ifneq "$(shell which g++ |grep hutaf_llt |wc -l)" "1"
      -include $(DEPEND)
    endif
  endif
endif
OBJS = acl.o arrayfuncs.o array_selfuncs.o array_typanalyze.o \
	array_userfuncs.o arrayutils.o bool.o \
	cash.o char.o date.o datetime.o datum.o domains.o \
	enum.o float.o format_type.o \
	geo_ops.o geo_selfuncs.o hotkey.o int.o int8.o int16.o \
	json.o jsonb.o jsonb_gin.o jsonb_op.o jsonb_util.o jsonfuncs.o like.o lockfuncs.o \
	misc.o nabstime.o name.o numeric.o numutils.o \
	oid.o a_compat.o orderedsetaggs.o pseudotypes.o rangetypes.o rangetypes_gist.o \
	rowtypes.o regexp.o regproc.o ruleutils.o selfuncs.o \
	tid.o timestamp.o varbit.o varchar.o varlena.o version.o xid.o \
	network.o mac.o inet_cidr_ntop.o inet_net_pton.o \
	ri_triggers.o pg_lzcompress.o pg_lsn.o pg_locale.o formatting.o \
	ascii.o quote.o pgxlogstatfuncs.o pgundostatfuncs.o pgstatfuncs.o encode.o dbsize.o genfile.o trigfuncs.o \
	tsginidx.o tsgistidx.o tsquery.o tsquery_cleanup.o tsquery_gist.o \
	tsquery_op.o tsquery_rewrite.o tsquery_util.o tsrank.o \
	tsvector.o tsvector_op.o tsvector_parser.o \
	txid.o uuid.o windowfuncs.o xml.o extended_statistics.o clientlogic_bytea.o clientlogicsettings.o \
	median_aggs.o expr_distinct.o nlssort.o memory_func.o first_last_agg.o

like.o: like.cpp like_match.cpp

include $(top_srcdir)/src/gausskernel/common.mk
