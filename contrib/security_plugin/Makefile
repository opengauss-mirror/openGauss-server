# contrib/security_plugin/Makefile

SRCS = gs_policy_plugin.cpp gs_policy_labels.cpp gs_policy_object_types.cpp privileges_audit.cpp access_audit.cpp \
gs_policy_logical_tree.cpp gs_policy_filter.cpp gs_audit_policy.cpp masking.cpp gs_mask_policy.cpp

# MODULES = security_plugin
MODULE_big = security_plugin
OBJS        = $(SRCS:.cpp=.o)

EXTENSION = security_plugin
DATA = security_plugin--1.0.sql

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/security_plugin
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
override CPPFLAGS := $(filter-out -fPIE, $(CPPFLAGS)) -fPIC
override CFLAGS := $(filter-out -fPIE, $(CFLAGS)) -fPIC
endif

gs_policy_plugin.o: gs_policy_plugin.cpp gs_policy_plugin.h
gs_policy_object_types.o: gs_policy_object_types.cpp gs_policy_object_types.h
gs_policy_labels.o: gs_policy_labels.cpp gs_policy_labels.h
privileges_audit.o: privileges_audit.cpp privileges_audit.h
access_audit.o: access_audit.cpp access_audit.h
gs_policy_logical_tree.o: gs_policy_logical_tree.cpp gs_policy_logical_tree.h
gs_policy_filter.o: gs_policy_filter.cpp gs_policy_filter.h
gs_audit_policy.o: gs_audit_policy.cpp gs_audit_policy.h
masking.o: masking.cpp masking.h
gs_mask_policy.o: gs_mask_policy.cpp gs_mask_policy.h
