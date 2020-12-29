#-------------------------------------------------------------------------
#
# Makefile for src/include
#
# 'make install' installs whole contents of src/include.
#
# src/include/Makefile
#
#-------------------------------------------------------------------------

subdir = src/include
top_builddir = ../..
include $(top_builddir)/src/Makefile.global


all: pg_config.h pg_config_os.h


# Subdirectories containing headers for server-side dev
SUBDIRS = bootstrap catalog commands client_logic datatype executor foreign gs_policy iprange lib libpq mb \
	nodes optimizer parser pgxc postmaster regex replication rewrite  \
	tcop snowball snowball/libstemmer tsearch tsearch/dicts utils\
	port port/win32 port/win32_msvc port/win32_msvc/sys \
	port/win32/arpa port/win32/netinet port/win32/sys \
	portability \
	gtm \
	storage vecexecutor access streaming access/obs \
	hotpatch \
	gstrace \
	knl knl/knl_guc threadpool workload
ifneq ($(enable_multiple_nodes), yes)
    SUBDIRS += storage/lock storage/buf storage/item storage/cstore storage/mot fmgr
endif

# Install all headers
install: all installdirs
# These headers are needed by the public headers of the interfaces.
	$(INSTALL_DATA) $(srcdir)/postgres_ext.h   '$(DESTDIR)$(includedir)'
	$(INSTALL_DATA) $(srcdir)/libpq/libpq-fs.h '$(DESTDIR)$(includedir)/libpq'
	$(INSTALL_DATA) pg_config.h    '$(DESTDIR)$(includedir)'
	$(INSTALL_DATA) pg_config_os.h '$(DESTDIR)$(includedir)'
	$(INSTALL_DATA) $(srcdir)/pg_config_manual.h '$(DESTDIR)$(includedir)'
# These headers are needed by libpq.
	$(INSTALL_DATA) gs_thread.h '$(DESTDIR)$(includedir)'
	$(INSTALL_DATA) gs_threadlocal.h '$(DESTDIR)$(includedir)'
# These headers are needed by the not-so-public headers of the interfaces.
	$(INSTALL_DATA) $(srcdir)/c.h            '$(DESTDIR)$(includedir_internal)'
	$(INSTALL_DATA) $(srcdir)/port.h         '$(DESTDIR)$(includedir_internal)'
	$(INSTALL_DATA) $(srcdir)/postgres_fe.h  '$(DESTDIR)$(includedir_internal)'
	$(INSTALL_DATA) $(srcdir)/libpq/pqcomm.h '$(DESTDIR)$(includedir_internal)/libpq'
# These headers are needed for server-side development
	$(INSTALL_DATA) pg_config.h    '$(DESTDIR)$(includedir_server)'
	$(INSTALL_DATA) pg_config_os.h '$(DESTDIR)$(includedir_server)'
	$(INSTALL_DATA) utils/errcodes.h '$(DESTDIR)$(includedir_server)/utils'
	$(INSTALL_DATA) utils/fmgroids.h '$(DESTDIR)$(includedir_server)/utils'
# These headers are needed by madlib
	$(INSTALL_DATA) $(with_3rd)/$(BINARYPATH)/cjson/comm/include/cjson/cJSON.h '$(DESTDIR)$(includedir_server)'/cjson/cJSON.h
	$(INSTALL_DATA) $(with_3rd)/$(BINARYPATH)/libobs/comm/include/eSDKOBS.h '$(DESTDIR)$(includedir_server)'/access/obs/eSDKOBS.h
	$(INSTALL_DATA) $(srcdir)/access/hash.inl '$(DESTDIR)$(includedir_server)'/access/hash.inl
# These headers are needed by fdw
	$(INSTALL_DATA) gssignal/gs_signal.h '$(DESTDIR)$(includedir_server)/gssignal/gs_signal.h'
	$(INSTALL_DATA) libcomm/libcomm.h '$(DESTDIR)$(includedir_server)/libcomm/libcomm.h'
	$(INSTALL_DATA) cm/etcdapi.h '$(DESTDIR)$(includedir_server)/cm/etcdapi.h'
	$(INSTALL_DATA) access/parallel_recovery/redo_item.h '$(DESTDIR)$(includedir_server)/access/parallel_recovery/redo_item.h'
	$(INSTALL_DATA) access/parallel_recovery/posix_semaphore.h '$(DESTDIR)$(includedir_server)/access/parallel_recovery/posix_semaphore.h'
	$(INSTALL_DATA) access/dfs/dfs_am.h '$(DESTDIR)$(includedir_server)/access/dfs/dfs_am.h'
	$(INSTALL_DATA) alarm/alarm.h '$(DESTDIR)$(includedir_server)/alarm/alarm.h'
	$(INSTALL_DATA) bulkload/dist_fdw.h '$(DESTDIR)$(includedir_server)/bulkload/dist_fdw.h'
	$(INSTALL_DATA) bulkload/importerror.h '$(DESTDIR)$(includedir_server)/bulkload/importerror.h'
	$(INSTALL_DATA) bulkload/utils.h '$(DESTDIR)$(includedir_server)/bulkload/utils.h'
	$(INSTALL_DATA) ssl/gs_openssl_client.h '$(DESTDIR)$(includedir_server)/ssl/gs_openssl_client.h'
	$(INSTALL_DATA) instruments/list.h '$(DESTDIR)$(includedir_server)/instruments/list.h'
	$(INSTALL_DATA) instruments/instr_event.h '$(DESTDIR)$(includedir_server)/instruments/instr_event.h'
	$(INSTALL_DATA) storage/dfs/dfs_connector.h '$(DESTDIR)$(includedir_server)/storage/dfs/dfs_connector.h' 
	$(INSTALL_DATA) storage/buf/block.h '$(DESTDIR)$(includedir_server)/storage/buf/block.h'
	$(INSTALL_DATA) storage/buf/buf.h '$(DESTDIR)$(includedir_server)/storage/buf/buf.h'
	$(INSTALL_DATA) storage/buf/buf_internals.h '$(DESTDIR)$(includedir_server)/storage/buf/buf_internals.h'
	$(INSTALL_DATA) storage/buf/buffile.h '$(DESTDIR)$(includedir_server)/storage/buf/buffile.h'
	$(INSTALL_DATA) storage/buf/bufmgr.h '$(DESTDIR)$(includedir_server)/storage/buf/bufmgr.h'
	$(INSTALL_DATA) storage/buf/bufpage.h '$(DESTDIR)$(includedir_server)/storage/buf/bufpage.h'
	$(INSTALL_DATA) storage/buf/buf_internals.h '$(DESTDIR)$(includedir_server)/storage/buf/buf_internals.h'
	$(INSTALL_DATA) storage/cstore/cstore_compress.h '$(DESTDIR)$(includedir_server)/storage/cstore/cstore_compress.h'
	$(INSTALL_DATA) storage/cstore/cstore_mem_alloc.h '$(DESTDIR)$(includedir_server)/storage/cstore/cstore_mem_alloc.h'
	$(INSTALL_DATA) storage/cstore/cstorealloc.h '$(DESTDIR)$(includedir_server)/storage/cstore/cstorealloc.h'
	$(INSTALL_DATA) storage/item/item.h '$(DESTDIR)$(includedir_server)/storage/item/item.h'
	$(INSTALL_DATA) storage/item/itemid.h '$(DESTDIR)$(includedir_server)/storage/item/itemid.h'
	$(INSTALL_DATA) storage/item/itemptr.h '$(DESTDIR)$(includedir_server)/storage/item/itemptr.h'
	$(INSTALL_DATA) storage/lock/lock.h '$(DESTDIR)$(includedir_server)/storage/lock/lock.h'
	$(INSTALL_DATA) storage/lock/lwlock.h '$(DESTDIR)$(includedir_server)/storage/lock/lwlock.h'
	$(INSTALL_DATA) storage/lock/lwlock_be.h '$(DESTDIR)$(includedir_server)/storage/lock/lwlock_be.h'
	$(INSTALL_DATA) storage/lock/s_lock.h '$(DESTDIR)$(includedir_server)/storage/lock/s_lock.h'
	$(INSTALL_DATA) storage/lock/pg_sema.h '$(DESTDIR)$(includedir_server)/storage/lock/pg_sema.h'
	$(INSTALL_DATA) distributelayer/streamConsumer.h '$(DESTDIR)$(includedir_server)/distributelayer/streamConsumer.h'
	$(INSTALL_DATA) distributelayer/streamCore.h '$(DESTDIR)$(includedir_server)/distributelayer/streamCore.h'
	$(INSTALL_DATA) distributelayer/streamProducer.h '$(DESTDIR)$(includedir_server)/distributelayer/streamProducer.h'
	$(INSTALL_DATA) distributelayer/streamTransportCore.h '$(DESTDIR)$(includedir_server)/distributelayer/streamTransportCore.h'
	$(INSTALL_DATA) fmgr/fmgr_comp.h '$(DESTDIR)$(includedir_server)/fmgr/fmgr_comp.h'
	$(INSTALL_DATA) fmgr/fmgr_core.h '$(DESTDIR)$(includedir_server)/fmgr/fmgr_core.h'
# We don't use INSTALL_DATA for performance reasons --- there are a lot of files
	cp $(srcdir)/*.h '$(DESTDIR)$(includedir_server)'/ || exit; \
	chmod $(INSTALL_DATA_MODE) '$(DESTDIR)$(includedir_server)'/*.h  || exit; \
	cp $(srcdir)/vecexecutor/*.inl '$(DESTDIR)$(includedir_server)'/ || exit; \
	chmod $(INSTALL_DATA_MODE) '$(DESTDIR)$(includedir_server)'/*.inl  || exit; \
	for dir in $(SUBDIRS); do \
	  cp $(srcdir)/$$dir/*.h '$(DESTDIR)$(includedir_server)'/$$dir/ || exit; \
	  chmod $(INSTALL_DATA_MODE) '$(DESTDIR)$(includedir_server)'/$$dir/*.h  || exit; \
	done

ifeq ($(vpath_build),yes)
	for file in dynloader.h catalog/schemapg.h parser/gram.h utils/probes.h; do \
	  cp $$file '$(DESTDIR)$(includedir_server)'/$$file || exit; \
	  chmod $(INSTALL_DATA_MODE) '$(DESTDIR)$(includedir_server)'/$$file || exit; \
	done
endif

installdirs:
	$(MKDIR_P) '$(DESTDIR)$(includedir)/libpq' '$(DESTDIR)$(includedir_internal)/libpq'
	$(MKDIR_P) $(addprefix '$(DESTDIR)$(includedir_server)'/, $(SUBDIRS))


uninstall:
	rm -f $(addprefix '$(DESTDIR)$(includedir)'/, pg_config.h pg_config_os.h pg_config_manual.h postgres_ext.h libpq/libpq-fs.h)
	rm -f $(addprefix '$(DESTDIR)$(includedir_internal)'/, c.h port.h postgres_fe.h libpq/pqcomm.h)
# heuristic...
	rm -rf $(addprefix '$(DESTDIR)$(includedir_server)'/, $(SUBDIRS) *.h)


clean:
	rm -f utils/fmgroids.h utils/errcodes.h parser/gram.hpp utils/probes.h catalog/schemapg.h replication/syncrep_gram.hpp replication/repl_gram.hpp

distclean maintainer-clean: clean
	rm -f pg_config.h dynloader.h pg_config_os.h stamp-h

maintainer-check:
	cd catalog && ./duplicate_oids
