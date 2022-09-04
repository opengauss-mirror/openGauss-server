#---------------------------------------------------------------------------------------
#
# IDENTIFICATION
#        src/gausskernel/dbmind/db4ai/executor/xgboost
#
# ---------------------------------------------------------------------------------------

subdir = src/gausskernel/dbmind/db4ai/executor/algorithms
top_builddir = ../../../../../..

include $(top_builddir)/src/Makefile.global

override CPPFLAGS += -I$(XGBOOST_LIB_PATH)/../include

ifneq "$(MAKECMDGOALS)" "clean"
  ifneq "$(MAKECMDGOALS)" "distclean"
    ifneq "$(shell which g++ |grep hutaf_llt |wc -l)" "1"
      -include $(DEPEND)
    endif
  endif
endif

SUBDIRS = bayes
OBJS = kmeans.o linregr.o logregr.o pca.o svm.o xgboost.o multiclass.o

include $(top_srcdir)/src/gausskernel/common.mk
