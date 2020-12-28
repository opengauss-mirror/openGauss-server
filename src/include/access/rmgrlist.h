/* ---------------------------------------------------------------------------------------
 * 
 * rmgrlist.h
 *
 *
 * The resource manager list is kept in its own source file for possible
 * use by automatic tools.  The exact representation of a rmgr is determined
 * by the PG_RMGR macro, which is not defined in this file; it can be
 * defined by the caller for special purposes.
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * 
 * IDENTIFICATION
 *        src/include/access/rmgrlist.h
 *
 * ---------------------------------------------------------------------------------------
 */

/* there is deliberately not an #ifndef RMGRLIST_H here
 * The header file needs to be repeated in the rmgrdesc.cpp package for array initialization.
 * An #ifndef RMGRLIST_H cannot be added.
 */

/*
 * List of resource manager entries.  Note that order of entries defines the
 * numerical values of each rmgr's ID, which is stored in WAL records.  New
 * entries should be added at the end, to avoid changing IDs of existing
 * entries.
 *
 * Changes to this list possibly need an XLOG_PAGE_MAGIC bump.
 */

/* symbol name, textual name, redo, desc, identify, startup, cleanup */
PG_RMGR(RM_XLOG_ID, "XLOG", xlog_redo, xlog_desc, NULL, NULL, NULL)
PG_RMGR(RM_XACT_ID, "Transaction", xact_redo, xact_desc, NULL, NULL, NULL)
PG_RMGR(RM_SMGR_ID, "Storage", smgr_redo, smgr_desc, NULL, NULL, NULL)
PG_RMGR(RM_CLOG_ID, "CLOG", clog_redo, clog_desc, NULL, NULL, NULL)
PG_RMGR(RM_DBASE_ID, "Database", dbase_redo, dbase_desc, NULL, NULL, NULL)
PG_RMGR(RM_TBLSPC_ID, "Tablespace", tblspc_redo, tblspc_desc, NULL, NULL, NULL)
PG_RMGR(RM_MULTIXACT_ID, "MultiXact", multixact_redo, multixact_desc, NULL, NULL, NULL)
PG_RMGR(RM_RELMAP_ID, "RelMap", relmap_redo, relmap_desc, NULL, NULL, NULL)
#ifndef ENABLE_MULTIPLE_NODES
PG_RMGR(RM_STANDBY_ID, "Standby", standby_redo, standby_desc, StandbyXlogStartup, StandbyXlogCleanup, StandbySafeRestartpoint)
#else
PG_RMGR(RM_STANDBY_ID, "Standby", standby_redo, standby_desc, NULL, NULL, NULL)
#endif
PG_RMGR(RM_HEAP2_ID, "Heap2", heap2_redo, heap2_desc, NULL, NULL, NULL)
PG_RMGR(RM_HEAP_ID, "Heap", heap_redo, heap_desc, NULL, NULL, NULL)
PG_RMGR(RM_BTREE_ID, "Btree", btree_redo, btree_desc, btree_xlog_startup, btree_xlog_cleanup, btree_safe_restartpoint)
PG_RMGR(RM_HASH_ID, "Hash", hash_redo, hash_desc, NULL, NULL, NULL)
PG_RMGR(RM_GIN_ID, "Gin", gin_redo, gin_desc, gin_xlog_startup, gin_xlog_cleanup, NULL)
PG_RMGR(RM_GIST_ID, "Gist", gist_redo, gist_desc, gist_xlog_startup, gist_xlog_cleanup, NULL)
PG_RMGR(RM_SEQ_ID, "Sequence", seq_redo, seq_desc, NULL, NULL, NULL)
PG_RMGR(RM_SPGIST_ID, "SPGist", spg_redo, spg_desc, spg_xlog_startup, spg_xlog_cleanup, NULL)
PG_RMGR(RM_SLOT_ID, "Slot", slot_redo, slot_desc, NULL, NULL, NULL)
PG_RMGR(RM_HEAP3_ID, "Heap3", heap3_redo, heap3_desc, NULL, NULL, NULL)
PG_RMGR(RM_BARRIER_ID, "Barrier", barrier_redo, barrier_desc, NULL, NULL, NULL)
#ifdef ENABLE_MOT
PG_RMGR(RM_MOT_ID, "MOT", MOTRedo, MOTDesc, NULL, NULL, NULL)
#endif
