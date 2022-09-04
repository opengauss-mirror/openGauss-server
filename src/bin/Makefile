#-------------------------------------------------------------------------
#
# Makefile for src/bin (client programs)
#
# Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
# Portions Copyright (c) 1994, Regents of the University of California
#
# src/bin/Makefile
#
#-------------------------------------------------------------------------

subdir = src/bin
top_builddir = ../..
include $(top_builddir)/src/Makefile.global

ifeq ($(enable_multiple_nodes), yes)
SUBDIRS = \
	initdb \
	pg_ctl \
	pg_rewind \
	pg_dump \
	psql \
	scripts \
	pg_config \
	pg_controldata \
	pg_resetxlog \
	gs_guc \
	gs_plan_simulator \
	pgxc_clean \
	gs_log \
	gs_cgroup \
	gsqlerr \
	pg_upgrade \
	pg_basebackup \
	pg_probackup

ifeq ($(PORTNAME), win32)
SUBDIRS += pgevent
else
ALWAYS_SUBDIRS += pgevent
endif
else
SUBDIRS = \
	initdb \
	pg_ctl \
	pg_rewind \
	pg_dump \
	psql \
	gs_loader \
	pg_config \
	pg_controldata \
	pg_resetxlog \
	gs_guc \
	gs_cgroup \
	gsqlerr \
	pg_basebackup \
	pg_probackup \
	gs_plan_simulator

ifeq ($(enable_ut), yes)
	SUBDIRS += gs_persist
else
ifeq ($(enable_multiple_nodes)_$(enable_privategauss), no_yes)
	SUBDIRS += gs_persist
endif
endif
endif


$(recurse)
$(recurse_always)
