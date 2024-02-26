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
SUBDIRS = bootstrap catalog commands client_logic datatype executor foreign gs_policy gs_ledger iprange lib libpq mb \
	nodes optimizer parser pgxc postmaster regex replication rewrite  \
	tcop snowball snowball/libstemmer tsearch tsearch/dicts utils\
	port port/win32 port/win32_msvc port/win32_msvc/sys \
	port/win32/arpa port/win32/netinet port/win32/sys \
	portability \
	gtm tde_key_management \
	storage vecexecutor access streaming access/obs \
	hotpatch \
	gstrace \
	knl knl/knl_guc threadpool workload lite keymgr
	
ifneq ($(enable_multiple_nodes), yes)
    SUBDIRS += storage/smgr storage/lock storage/buf storage/item storage/cstore storage/mot fmgr
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
	$(MKDIR_P) '$(DESTDIR)$(includedir_server)'/cjson/
	$(INSTALL_DATA) $(CJSON_INCLUDE_PATH)/cjson/cJSON.h '$(DESTDIR)$(includedir_server)'/cjson/cJSON.h
ifeq ($(enable_lite_mode), no)
	$(INSTALL_DATA) $(with_3rd)/$(BINARYPATH)/libobs/comm/include/eSDKOBS.h '$(DESTDIR)$(includedir_server)'/access/obs/eSDKOBS.h
endif
	$(INSTALL_DATA) $(srcdir)/access/hash.inl '$(DESTDIR)$(includedir_server)'/access/hash.inl
# These headers are needed by fdw
	$(INSTALL_DATA) gssignal/gs_signal.h '$(DESTDIR)$(includedir_server)/gssignal/gs_signal.h'
	$(INSTALL_DATA) libcomm/libcomm.h '$(DESTDIR)$(includedir_server)/libcomm/libcomm.h'
	$(INSTALL_DATA) cm/etcdapi.h '$(DESTDIR)$(includedir_server)/cm/etcdapi.h'
	$(INSTALL_DATA) access/parallel_recovery/redo_item.h '$(DESTDIR)$(includedir_server)/access/parallel_recovery/redo_item.h'
	$(INSTALL_DATA) access/parallel_recovery/posix_semaphore.h '$(DESTDIR)$(includedir_server)/access/parallel_recovery/posix_semaphore.h'
	$(INSTALL_DATA) alarm/alarm.h '$(DESTDIR)$(includedir_server)/alarm/alarm.h'
	$(INSTALL_DATA) bulkload/dist_fdw.h '$(DESTDIR)$(includedir_server)/bulkload/dist_fdw.h'
	$(INSTALL_DATA) bulkload/importerror.h '$(DESTDIR)$(includedir_server)/bulkload/importerror.h'
	$(INSTALL_DATA) bulkload/utils.h '$(DESTDIR)$(includedir_server)/bulkload/utils.h'
	$(INSTALL_DATA) ssl/gs_openssl_client.h '$(DESTDIR)$(includedir_server)/ssl/gs_openssl_client.h'
	$(INSTALL_DATA) instruments/list.h '$(DESTDIR)$(includedir_server)/instruments/list.h'
	$(INSTALL_DATA) instruments/instr_event.h '$(DESTDIR)$(includedir_server)/instruments/instr_event.h'
	$(INSTALL_DATA) storage/smgr/fd.h '$(DESTDIR)$(includedir_server)/storage/smgr/fd.h'
	$(INSTALL_DATA) storage/smgr/knl_usync.h '$(DESTDIR)$(includedir_server)/storage/smgr/knl_usync.h'
	$(INSTALL_DATA) storage/smgr/relfilenode.h '$(DESTDIR)$(includedir_server)/storage/smgr/relfilenode.h'
	$(INSTALL_DATA) storage/smgr/segment_internal.h '$(DESTDIR)$(includedir_server)/storage/smgr/segment_internal.h'
	$(INSTALL_DATA) storage/smgr/segment.h '$(DESTDIR)$(includedir_server)/storage/smgr/segment.h'
	$(INSTALL_DATA) storage/smgr/smgr.h '$(DESTDIR)$(includedir_server)/storage/smgr/smgr.h'
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
	$(INSTALL_DATA) lite/memory_lite.h '$(DESTDIR)$(includedir_server)/lite/memory_lite.h'
# These headers are needed by postgis
	$(INSTALL_DATA) access/ustore/undo/knl_uundotype.h '$(DESTDIR)$(includedir_server)/access/ustore/undo/knl_uundotype.h'
	$(INSTALL_DATA) access/ustore/undo/knl_uundoapi.h '$(DESTDIR)$(includedir_server)/access/ustore/undo/knl_uundoapi.h'
	$(INSTALL_DATA) access/ustore/knl_uheap.h '$(DESTDIR)$(includedir_server)/access/ustore/knl_uheap.h'
	$(INSTALL_DATA) access/ustore/knl_utuple.h '$(DESTDIR)$(includedir_server)/access/ustore/knl_utuple.h'
	$(INSTALL_DATA) access/ustore/knl_utype.h '$(DESTDIR)$(includedir_server)/access/ustore/knl_utype.h'
	$(INSTALL_DATA) access/ustore/knl_upage.h '$(DESTDIR)$(includedir_server)/access/ustore/knl_upage.h'
	$(INSTALL_DATA) access/ustore/knl_uredo.h '$(DESTDIR)$(includedir_server)/access/ustore/knl_uredo.h'
	$(INSTALL_DATA) access/ustore/knl_uundovec.h '$(DESTDIR)$(includedir_server)/access/ustore/knl_uundovec.h'
	$(INSTALL_DATA) access/ustore/knl_uundorecord.h '$(DESTDIR)$(includedir_server)/access/ustore/knl_uundorecord.h'
	$(INSTALL_DATA) access/ustore/knl_uverify.h '$(DESTDIR)$(includedir_server)/access/ustore/knl_uverify.h'
	$(INSTALL_DATA) access/ustore/undo/knl_uundoxlog.h '$(DESTDIR)$(includedir_server)/access/ustore/undo/knl_uundoxlog.h'
	$(INSTALL_DATA) access/ustore/undo/knl_uundotxn.h '$(DESTDIR)$(includedir_server)/access/ustore/undo/knl_uundotxn.h'
	$(INSTALL_DATA) access/ustore/undo/knl_uundozone.h '$(DESTDIR)$(includedir_server)/access/ustore/undo/knl_uundozone.h'
	$(INSTALL_DATA) access/ustore/undo/knl_uundospace.h '$(DESTDIR)$(includedir_server)/access/ustore/undo/knl_uundospace.h'
	$(INSTALL_DATA) communication/commproxy_basic.h '$(DESTDIR)$(includedir_server)/communication/commproxy_basic.h'
	$(INSTALL_DATA) access/parallel_recovery/page_redo.h '$(DESTDIR)$(includedir_server)/access/parallel_recovery/page_redo.h'
	$(INSTALL_DATA) access/parallel_recovery/spsc_blocking_queue.h '$(DESTDIR)$(includedir_server)/access/parallel_recovery/spsc_blocking_queue.h'
	$(INSTALL_DATA) executor/exec/execdesc.h '$(DESTDIR)$(includedir_server)/executor/exec/execdesc.h'
	$(INSTALL_DATA) db4ai/matrix.h '$(DESTDIR)$(includedir_server)/db4ai/matrix.h'
	$(INSTALL_DATA) db4ai/scores.h '$(DESTDIR)$(includedir_server)/db4ai/scores.h'
	$(INSTALL_DATA) db4ai/db4ai.h '$(DESTDIR)$(includedir_server)/db4ai/db4ai.h'
	$(INSTALL_DATA) instruments/gs_stack.h '$(DESTDIR)$(includedir_server)/instruments/gs_stack.h'
	$(INSTALL_DATA) instruments/instr_mfchain.h '$(DESTDIR)$(includedir_server)/instruments/instr_mfchain.h'
	$(INSTALL_DATA) access/multixact.h '$(DESTDIR)$(includedir_server)/access/multixact.h'
	$(INSTALL_DATA) access/xlogutils.h '$(DESTDIR)$(includedir_server)/access/xlogutils.h'
	$(INSTALL_DATA) executor/functions.h '$(DESTDIR)$(includedir_server)/executor/functions.h'
	$(INSTALL_DATA) storage/lock/waitpolicy.h '$(DESTDIR)$(includedir_server)/storage/lock/waitpolicy.h'
	$(INSTALL_DATA) storage/page_compression.h '$(DESTDIR)$(includedir_server)/storage/page_compression.h'
	$(INSTALL_DATA) storage/smgr/knl_usync.h '$(DESTDIR)$(includedir_server)/storage/smgr/knl_usync.h'
	$(INSTALL_DATA) storage/smgr/fd.h '$(DESTDIR)$(includedir_server)/storage/smgr/fd.h'
	$(INSTALL_DATA) storage/smgr/smgr.h '$(DESTDIR)$(includedir_server)/storage/smgr/smgr.h'
	$(INSTALL_DATA) storage/sharedfileset.h '$(DESTDIR)$(includedir_server)/storage/sharedfileset.h'
	$(INSTALL_DATA) utils/knl_globalsysdbcache.h '$(DESTDIR)$(includedir_server)/utils/knl_globalsysdbcache.h'
	$(INSTALL_DATA) utils/knl_globaldbstatmanager.h '$(DESTDIR)$(includedir_server)/utils/knl_globaldbstatmanager.h'
	$(INSTALL_DATA) utils/knl_globalsyscache_common.h '$(DESTDIR)$(includedir_server)/utils/knl_globalsyscache_common.h'
	$(INSTALL_DATA) utils/knl_globalbucketlist.h '$(DESTDIR)$(includedir_server)/utils/knl_globalbucketlist.h'
	$(INSTALL_DATA) utils/knl_globalsystabcache.h '$(DESTDIR)$(includedir_server)/utils/knl_globalsystabcache.h'
	$(INSTALL_DATA) utils/knl_globalsystupcache.h '$(DESTDIR)$(includedir_server)/utils/knl_globalsystupcache.h'
	$(INSTALL_DATA) utils/knl_globaltabdefcache.h '$(DESTDIR)$(includedir_server)/utils/knl_globaltabdefcache.h'
	$(INSTALL_DATA) utils/knl_globalbasedefcache.h '$(DESTDIR)$(includedir_server)/utils/knl_globalbasedefcache.h'
	$(INSTALL_DATA) utils/knl_globalpartdefcache.h '$(DESTDIR)$(includedir_server)/utils/knl_globalpartdefcache.h'
	$(INSTALL_DATA) utils/knl_globalrelmapcache.h '$(DESTDIR)$(includedir_server)/utils/knl_globalrelmapcache.h'
	$(INSTALL_DATA) utils/knl_localsysdbcache.h '$(DESTDIR)$(includedir_server)/utils/knl_localsysdbcache.h'
	$(INSTALL_DATA) utils/knl_localsystabcache.h '$(DESTDIR)$(includedir_server)/utils/knl_localsystabcache.h'
	$(INSTALL_DATA) utils/knl_localsystupcache.h '$(DESTDIR)$(includedir_server)/utils/knl_localsystupcache.h'
	$(INSTALL_DATA) utils/knl_localsyscache_common.h '$(DESTDIR)$(includedir_server)/utils/knl_localsyscache_common.h'
	$(INSTALL_DATA) utils/knl_localtabdefcache.h '$(DESTDIR)$(includedir_server)/utils/knl_localtabdefcache.h'
	$(INSTALL_DATA) utils/knl_localbasedefcache.h '$(DESTDIR)$(includedir_server)/utils/knl_localbasedefcache.h'
	$(INSTALL_DATA) utils/knl_localbucketlist.h '$(DESTDIR)$(includedir_server)/utils/knl_localbucketlist.h'
	$(INSTALL_DATA) utils/knl_localpartdefcache.h '$(DESTDIR)$(includedir_server)/utils/knl_localpartdefcache.h'
	$(INSTALL_DATA) utils/pl_global_package_runtime_cache.h '$(DESTDIR)$(includedir_server)/utils/pl_global_package_runtime_cache.h'
	$(INSTALL_DATA) catalog/pg_tablespace.h '$(DESTDIR)$(includedir_server)/catalog/pg_tablespace.h'
	$(INSTALL_DATA) catalog/pg_subscription.h '$(DESTDIR)$(includedir_server)/catalog/pg_subscription.h'
	$(INSTALL_DATA) catalog/pg_replication_origin.h '$(DESTDIR)$(includedir_server)/catalog/pg_replication_origin.h'
	$(INSTALL_DATA) postmaster/barrier_creator.h '$(DESTDIR)$(includedir_server)/postmaster/barrier_creator.h'
	$(INSTALL_DATA) postmaster/pagerepair.h '$(DESTDIR)$(includedir_server)/postmaster/pagerepair.h'
	$(INSTALL_DATA) replication/libpqwalreceiver.h '$(DESTDIR)$(includedir_server)/replication/libpqwalreceiver.h'
	$(INSTALL_DATA) replication/worker_internal.h '$(DESTDIR)$(includedir_server)/replication/worker_internal.h'
	$(INSTALL_DATA) replication/origin.h '$(DESTDIR)$(includedir_server)/replication/origin.h'
	$(INSTALL_DATA) parser/scanner.h '$(DESTDIR)$(includedir_server)/parser/scanner.h'
	$(INSTALL_DATA) parser/keywords.h '$(DESTDIR)$(includedir_server)/parser/keywords.h'
	$(INSTALL_DATA) ddes/dms/dms_api.h '$(DESTDIR)$(includedir_server)/ddes/dms/dms_api.h'
# These headers are needed by AGE(A Graph Extension)
	$(INSTALL_DATA) db4ai/predict_by.h '$(DESTDIR)$(includedir_server)/db4ai/predict_by.h'
	$(INSTALL_DATA) db4ai/model_warehouse.h '$(DESTDIR)$(includedir_server)/db4ai/model_warehouse.h'
	$(INSTALL_DATA) executor/node/nodeCtescan.h '$(DESTDIR)$(includedir_server)/executor/node/nodeCtescan.h'
	$(INSTALL_DATA) executor/exec/execStream.h '$(DESTDIR)$(includedir_server)/executor/exec/execStream.h'
	$(INSTALL_DATA) instruments/unique_sql_basic.h '$(DESTDIR)$(includedir_server)/instruments/unique_sql_basic.h'
	$(INSTALL_DATA) storage/file/fio_device_com.h '$(DESTDIR)$(includedir_server)/storage/file/fio_device_com.h'
	$(INSTALL_DATA) storage/gs_uwal/uwal.h '$(DESTDIR)$(includedir_server)/storage/gs_uwal/uwal.h'
	$(INSTALL_DATA) ddes/dms/ss_aio.h '$(DESTDIR)$(includedir_server)/ddes/dms/ss_aio.h'
	$(INSTALL_DATA) ddes/dms/ss_dms_recovery.h '$(DESTDIR)$(includedir_server)/ddes/dms/ss_dms_recovery.h'
	$(INSTALL_DATA) ddes/dms/ss_common_attr.h '$(DESTDIR)$(includedir_server)/ddes/dms/ss_common_attr.h'
	$(INSTALL_DATA) ddes/dms/ss_dms_callback.h '$(DESTDIR)$(includedir_server)/ddes/dms/ss_dms_callback.h'
	$(INSTALL_DATA) ddes/dms/ss_init.h '$(DESTDIR)$(includedir_server)/ddes/dms/ss_init.h'
	$(INSTALL_DATA) ddes/dms/ss_xmin.h '$(DESTDIR)$(includedir_server)/ddes/dms/ss_xmin.h'
	$(INSTALL_DATA) ddes/dms/ss_txnstatus.h '$(DESTDIR)$(includedir_server)/ddes/dms/ss_txnstatus.h'
	$(INSTALL_DATA) storage/dss/dss_api_def.h '$(DESTDIR)$(includedir_server)/storage/dss/dss_api_def.h'
	$(INSTALL_DATA) executor/node/nodeExtensible.h '$(DESTDIR)$(includedir_server)/executor/node/nodeExtensible.h'
	$(INSTALL_DATA) executor/node/nodeAgg.h '$(DESTDIR)$(includedir_server)/executor/node/nodeAgg.h'
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
