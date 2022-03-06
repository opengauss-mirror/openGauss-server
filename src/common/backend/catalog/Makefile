#-------------------------------------------------------------------------
#
# Makefile for common/backend/catalog
#
# src/common/backend/catalog/Makefile
#
#-------------------------------------------------------------------------

subdir = src/common/backend/catalog
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global

ifneq "$(MAKECMDGOALS)" "clean"
  ifneq "$(MAKECMDGOALS)" "distclean"
     ifneq "$(shell which g++ |grep hutaf_llt |wc -l)" "1"
        -include $(DEPEND)
     endif
  endif
endif
OBJS = catalog.o dependency.o heap.o index.o indexing.o namespace.o aclchk.o \
       objectaddress.o pg_aggregate.o pg_collation.o pg_constraint.o pg_conversion.o \
       pg_depend.o pg_enum.o pg_inherits.o pg_largeobject.o pg_namespace.o pg_object.o\
       pg_operator.o gs_package.o pg_proc.o pg_range.o pg_db_role_setting.o pg_shdepend.o pg_synonym.o\
       pg_type.o pgxc_class.o storage.o storage_gtt.o toasting.o pg_job.o pg_partition.o\
       pg_hashbucket.o cstore_ctlg.o dfsstore_ctlg.o pg_builtin_proc.o streaming_stream.o\
       gs_matview.o pgxc_slice.o pg_job_proc.o gs_job_argument.o gs_job_attribute.o pg_uid.o gs_global_config.o\
       gs_db_privilege.o pg_publication.o pg_subscription.o

BKIFILES = postgres.bki postgres.description postgres.shdescription

include $(top_srcdir)/src/gausskernel/common.mk

all: $(BKIFILES) schemapg.h

# Note: there are some undocumented dependencies on the ordering in which
# the catalog header files are assembled into postgres.bki.  In particular,
# indexing.h had better be last, and toasting.h just before it.

POSTGRES_BKI_SRCS = $(addprefix $(top_srcdir)/src/include/catalog/,\
	gs_package.h pg_proc.h pg_type.h pg_attribute.h pg_class.h pg_partition.h\
	pg_attrdef.h pg_constraint.h pg_inherits.h pg_index.h pg_operator.data \
	pg_opfamily.h pg_opclass.h pg_am.h pg_amop.data pg_amproc.h \
	pg_language.h pg_largeobject_metadata.h pg_largeobject.h pg_aggregate.h \
	pg_statistic.h pg_rewrite.h pg_trigger.h pg_description.h \
	pg_cast.h pg_enum.h pg_namespace.h pg_conversion.h pg_depend.h \
	pg_database.h pg_db_role_setting.h pg_tablespace.h pg_pltemplate.h \
	pg_authid.h pg_auth_members.h pg_shdepend.h pg_shdescription.h \
	pg_ts_config.h pg_ts_config_map.h pg_ts_dict.h \
	pg_ts_parser.h pg_ts_template.h pg_auth_history.h pg_user_status.h pg_extension.h \
	pg_obsscaninfo.h pg_foreign_data_wrapper.h pg_foreign_server.h pg_user_mapping.h \
	pgxc_class.h pgxc_node.h pgxc_group.h pg_resource_pool.h pg_workload_group.h \
	pg_app_workloadgroup_mapping.h pg_foreign_table.h pg_rlspolicy.h \
	pg_default_acl.h pg_seclabel.h pg_shseclabel.h pg_collation.h pg_range.h gs_policy_label.h \
	gs_auditing_policy.h gs_auditing_policy_acc.h gs_auditing_policy_priv.h gs_auditing_policy_filter.h \
	gs_masking_policy.h gs_masking_policy_actions.h gs_masking_policy_filters.h \
	gs_encrypted_columns.h gs_column_keys.h gs_column_keys_args.h \
	gs_client_global_keys.h gs_client_global_keys_args.h gs_encrypted_proc.h\
	pg_job.h gs_asp.h pg_job_proc.h pg_extension_data_source.h pg_statistic_ext.h pg_object.h  pg_synonym.h \
	toasting.h indexing.h gs_obsscaninfo.h pg_directory.h pg_hashbucket.h gs_global_chain.h gs_global_config.h\
	pg_streaming_stream.h pg_streaming_cont_query.h pg_streaming_reaper_status.h gs_matview.h\
	gs_matview_dependency.h pgxc_slice.h gs_opt_model.h gs_model.h\
	pg_recyclebin.h pg_snapshot.h gs_job_argument.h gs_job_attribute.h pg_uid.h gs_db_privilege.h\
	pg_replication_origin.h pg_publication.h pg_publication_rel.h pg_subscription.h\
    )

# location of Catalog.pm
catalogdir = $(top_srcdir)/src/common/backend/catalog

# locations of headers that genbki.pl needs to read
pg_includes = -I$(top_srcdir)/src/include/catalog -I$(top_builddir)/src/include/catalog

# see explanation in ../parser/Makefile
postgres.description: postgres.bki ;

postgres.shdescription: postgres.bki ;

schemapg.h: postgres.bki ;

# Technically, this should depend on Makefile.global, but then
# postgres.bki would need to be rebuilt after every configure run,
# even in distribution tarballs.  So this is cheating a bit, but it
# will achieve the goal of updating the version number when it
# changes.
postgres.bki: genbki.pl Catalog.pm $(POSTGRES_BKI_SRCS) $(top_srcdir)/configure
	$(PERL) -I $(catalogdir) $< $(pg_includes) --set-version=$(MAJORVERSION) $(POSTGRES_BKI_SRCS)

.PHONY: install-data
ifeq ($(enable_multiple_nodes), yes)
install-data: $(BKIFILES) installdirs
	$(INSTALL_DATA) $(call vpathsearch,postgres.bki) '$(DESTDIR)$(datadir)/postgres.bki'
	$(INSTALL_DATA) $(call vpathsearch,postgres.description) '$(DESTDIR)$(datadir)/postgres.description'
	$(INSTALL_DATA) $(call vpathsearch,postgres.shdescription) '$(DESTDIR)$(datadir)/postgres.shdescription'
	$(INSTALL_DATA) $(top_builddir)/../distribute/kernel/catalog/system_views.sql '$(DESTDIR)$(datadir)/system_views.sql'
	$(INSTALL_DATA) $(top_builddir)/../distribute/kernel/catalog/performance_views.sql '$(DESTDIR)$(datadir)/performance_views.sql'
	$(INSTALL_DATA) $(srcdir)/information_schema.sql '$(DESTDIR)$(datadir)/information_schema.sql'
	$(INSTALL_DATA) $(srcdir)/sql_features.txt '$(DESTDIR)$(datadir)/sql_features.txt'
	$(INSTALL_DATA) $(srcdir)/pg_cast_oid.txt '$(DESTDIR)$(datadir)/pg_cast_oid.txt'
else
ifeq ($(enable_privategauss), yes)

install-data: $(BKIFILES) installdirs
	$(INSTALL_DATA) $(call vpathsearch,postgres.bki) '$(DESTDIR)$(datadir)/postgres.bki'
	$(INSTALL_DATA) $(call vpathsearch,postgres.description) '$(DESTDIR)$(datadir)/postgres.description'
	$(INSTALL_DATA) $(call vpathsearch,postgres.shdescription) '$(DESTDIR)$(datadir)/postgres.shdescription'
	$(INSTALL_DATA) $(srcdir)/../../../../../privategauss/kernel/catalog/private_system_views.sql '$(DESTDIR)$(datadir)/private_system_views.sql'	
	$(INSTALL_DATA) $(srcdir)/system_views.sql '$(DESTDIR)$(datadir)/system_views.sql'
	$(INSTALL_DATA) $(srcdir)/performance_views.sql '$(DESTDIR)$(datadir)/performance_views.sql'
	$(INSTALL_DATA) $(srcdir)/information_schema.sql '$(DESTDIR)$(datadir)/information_schema.sql'
	$(INSTALL_DATA) $(srcdir)/sql_features.txt '$(DESTDIR)$(datadir)/sql_features.txt'
	$(INSTALL_DATA) $(srcdir)/pg_cast_oid.txt '$(DESTDIR)$(datadir)/pg_cast_oid.txt'
	$(MKDIR_P) '$(DESTDIR)$(datadir)/db4ai/snapshots'
	$(INSTALL_DATA) $(srcdir)/../../../gausskernel/dbmind/db4ai/snapshots/* '$(DESTDIR)$(datadir)/db4ai/snapshots'
else
install-data: $(BKIFILES) installdirs
	$(INSTALL_DATA) $(call vpathsearch,postgres.bki) '$(DESTDIR)$(datadir)/postgres.bki'
	$(INSTALL_DATA) $(call vpathsearch,postgres.description) '$(DESTDIR)$(datadir)/postgres.description'
	$(INSTALL_DATA) $(call vpathsearch,postgres.shdescription) '$(DESTDIR)$(datadir)/postgres.shdescription'
	$(INSTALL_DATA) $(srcdir)/system_views.sql '$(DESTDIR)$(datadir)/system_views.sql'
	$(INSTALL_DATA) $(srcdir)/performance_views.sql '$(DESTDIR)$(datadir)/performance_views.sql'
	$(INSTALL_DATA) $(srcdir)/information_schema.sql '$(DESTDIR)$(datadir)/information_schema.sql'
	$(INSTALL_DATA) $(srcdir)/sql_features.txt '$(DESTDIR)$(datadir)/sql_features.txt'
	$(INSTALL_DATA) $(srcdir)/pg_cast_oid.txt '$(DESTDIR)$(datadir)/pg_cast_oid.txt'
	$(MKDIR_P) '$(DESTDIR)$(datadir)/db4ai/snapshots'
	$(INSTALL_DATA) $(srcdir)/../../../gausskernel/dbmind/db4ai/snapshots/* '$(DESTDIR)$(datadir)/db4ai/snapshots'
endif
endif

installdirs:
	$(MKDIR_P) '$(DESTDIR)$(datadir)'

.PHONY: uninstall-data
uninstall-data:
	rm -f $(addprefix '$(DESTDIR)$(datadir)'/, $(BKIFILES) system_views.sql performance_views.sql private_system_views.sql information_schema.sql sql_features.txt)

# postgres.bki, postgres.description, postgres.shdescription, and schemapg.h
# are in the distribution tarball, so they are not cleaned here.
clean:

maintainer-clean: clean
	rm -f $(BKIFILES)
