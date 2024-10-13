# contrib/Makefile

subdir = contrib
top_builddir = ..
include $(top_builddir)/src/Makefile.global

SUBDIRS = \
		adminpack	\
		auth_delay	\
		auto_explain	\
		btree_gin	\
		btree_gist	\
		chkpass		\
		citext		\
		cube		\
		dblink		\
		dict_int	\
		dict_xsyn	\
		dummy_seclabel	\
		earthdistance	\
		file_fdw	\
		fuzzystrmatch	\
		hstore		\
		log_fdw		\
		intagg		\
		intarray	\
		isn		\
		lo		\
		ltree		\
		oid2name	\
		pagehack	\
		pageinspect	\
		passwordcheck	\
		pg_archivecleanup \
		pg_buffercache	\
		pg_freespacemap \
		pg_standby	\
		pg_stat_statements \
		pg_test_fsync	\
		pg_test_timing	\
		pg_trgm		\
		pg_upgrade_support \
		pg_xlogdump	\
		pgbench		\
		pgcrypto	\
		pgrowlocks	\
		pgstattuple	\
		seg		\
		spi		\
		tablefunc	\
		tcn		\
		test_decoding   \
		mppdb_decoding  \
		test_parser	\
		tsearch2	\
		unaccent	\
		vacuumlo	\
		security_plugin	\
		ndpplugin       \
		gms_utility		\
		gms_profiler    \
		gms_inaddr  \
		gms_output      \
		gms_stats       \
		gms_lob         \
		gms_sql         \
		gms_i18n      \
		gms_debug     \
		gms_raw       \
		gms_match

ifeq ($(with_openssl),yes)
SUBDIRS += sslinfo
else
ALWAYS_SUBDIRS += sslinfo
endif

ifeq ($(with_libxml),yes)
SUBDIRS += xml2
SUBDIRS += gms_xmlgen
else
ALWAYS_SUBDIRS += xml2
ALWAYS_SUBDIRS += gms_xmlgen
endif

ifeq ($(with_selinux),yes)
SUBDIRS += sepgsql
else
ALWAYS_SUBDIRS += sepgsql
endif

$(recurse)
$(recurse_always)
