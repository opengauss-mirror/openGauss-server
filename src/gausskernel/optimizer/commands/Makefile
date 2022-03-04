#---------------------------------------------------------------------------------------
#     
# IDENTIFICATION
#        src/gausskernel/optimizer/commands/Makefile
#
# ---------------------------------------------------------------------------------------

subdir = src/gausskernel/optimizer/commands
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global

ifneq "$(MAKECMDGOALS)" "clean"
  ifneq "$(MAKECMDGOALS)" "distclean"
    ifneq "$(shell which g++ |grep hutaf_llt |wc -l)" "1"
      -include $(DEPEND)
    endif
  endif
endif

SUBDIRS     = sequence

OBJS = aggregatecmds.o alter.o analyze.o async.o cluster.o comment.o  \
	collationcmds.o constraint.o conversioncmds.o copy.o createas.o \
	dbcommands.o define.o discard.o dropcmds.o explain.o extension.o \
	foreigncmds.o functioncmds.o \
	indexcmds.o lockcmds.o matview.o operatorcmds.o opclasscmds.o \
	portalcmds.o prepare.o proclang.o packagecmds.o publicationcmds.o\
	schemacmds.o seclabel.o sec_rls_cmds.o subscriptioncmds.o tablecmds.o tablespace.o trigger.o \
	tsearchcmds.o typecmds.o user.o vacuum.o vacuumlazy.o \
	variable.o verifyrepair.o verify.o view.o gds_stream.o formatter.o datasourcecmds.o \
	directory.o auto_explain.o shutdown.o

ifeq ($(enable_lite_mode), no)
OBJS += obs_stream.o
endif

include $(top_srcdir)/src/gausskernel/common.mk
