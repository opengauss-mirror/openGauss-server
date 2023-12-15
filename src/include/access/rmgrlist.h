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

/* symbol name, textual name, redo, desc, identify, startup, cleanup, undo, undo_desc info_type_name */
PG_RMGR(RM_XLOG_ID, "XLOG", xlog_redo, xlog_desc, NULL, NULL, NULL, NULL, NULL, xlog_type_name)
PG_RMGR(RM_XACT_ID, "Transaction", xact_redo, xact_desc, NULL, NULL, NULL, NULL, NULL, xact_type_name)
PG_RMGR(RM_SMGR_ID, "Storage", smgr_redo, smgr_desc, NULL, NULL, NULL, NULL, NULL, smgr_type_name)
PG_RMGR(RM_CLOG_ID, "CLOG", clog_redo, clog_desc, NULL, NULL, NULL, NULL, NULL, clog_type_name)
PG_RMGR(RM_DBASE_ID, "Database", dbase_redo, dbase_desc, NULL, NULL, NULL, NULL, NULL, dbase_type_name)
PG_RMGR(RM_TBLSPC_ID, "Tablespace", tblspc_redo, tblspc_desc, NULL, NULL, NULL, NULL, NULL, tblspc_type_name)
PG_RMGR(RM_MULTIXACT_ID, "MultiXact", multixact_redo, multixact_desc, NULL, NULL, NULL, NULL, NULL, multixact_type_name)
PG_RMGR(RM_RELMAP_ID, "RelMap", relmap_redo, relmap_desc, NULL, NULL, NULL, NULL, NULL, relmap_type_name)

PG_RMGR(RM_STANDBY_ID, "Standby", standby_redo, standby_desc, StandbyXlogStartup, StandbyXlogCleanup, \
    NULL, NULL, NULL, standby_type_name)

PG_RMGR(RM_HEAP2_ID, "Heap2", heap2_redo, heap2_desc, NULL, NULL, NULL, NULL, NULL, heap2_type_name)
PG_RMGR(RM_HEAP_ID, "Heap", heap_redo, heap_desc, NULL, NULL, NULL, NULL, NULL, heap_type_name)
PG_RMGR(RM_BTREE_ID, "Btree", btree_redo, btree_desc, btree_xlog_startup, btree_xlog_cleanup, btree_safe_restartpoint,
    NULL, NULL, btree_type_name)
PG_RMGR(RM_HASH_ID, "Hash", hash_redo, hash_desc, NULL, NULL, NULL, NULL, NULL, hash_type_name)
PG_RMGR(RM_GIN_ID, "Gin", gin_redo, gin_desc, gin_xlog_startup, gin_xlog_cleanup, NULL, NULL, NULL, gin_type_name)
PG_RMGR(RM_GIST_ID, "Gist", gist_redo, gist_desc, gist_xlog_startup, gist_xlog_cleanup, NULL, NULL, NULL, \
    gist_type_name)
PG_RMGR(RM_SEQ_ID, "Sequence", seq_redo, seq_desc, NULL, NULL, NULL, NULL, NULL, seq_type_name)
PG_RMGR(RM_SPGIST_ID, "SPGist", spg_redo, spg_desc, spg_xlog_startup, spg_xlog_cleanup, NULL, NULL, NULL, \
    spg_type_name)
PG_RMGR(RM_SLOT_ID, "Slot", slot_redo, slot_desc, NULL, NULL, NULL, NULL, NULL, slot_type_name)
PG_RMGR(RM_HEAP3_ID, "Heap3", heap3_redo, heap3_desc, NULL, NULL, NULL, NULL, NULL, heap3_type_name)
PG_RMGR(RM_BARRIER_ID, "Barrier", barrier_redo, barrier_desc, NULL, NULL, NULL, NULL, NULL, barrier_type_name)

#ifdef ENABLE_MOT
PG_RMGR(RM_MOT_ID, "MOT", MOTRedo, MOTDesc, NULL, NULL, NULL, NULL, NULL, MOT_type_name)
#endif

PG_RMGR(RM_UHEAP_ID, "UHeap", UHeapRedo, UHeapDesc, NULL, NULL, NULL, UHeapUndoActions, NULL, uheap_type_name)
PG_RMGR(RM_UHEAP2_ID, "UHeap2", UHeap2Redo, UHeap2Desc, NULL, NULL, NULL, NULL, NULL, uheap2_type_name)
PG_RMGR(RM_UNDOLOG_ID, "UndoLog", undo::UndoXlogRedo, undo::UndoXlogDesc, NULL, NULL, NULL, NULL, NULL, \
    undo::undo_xlog_type_name)
PG_RMGR(RM_UHEAPUNDO_ID, "UHeapUndo", UHeapUndoRedo, UHeapUndoDesc, NULL, NULL, NULL, NULL, NULL, uheap_undo_type_name)
PG_RMGR(RM_UNDOACTION_ID, "UndoAction", undo::UndoXlogRollbackFinishRedo, undo::UndoXlogRollbackFinishDesc, NULL, NULL,
    NULL, NULL, NULL, undo::undo_xlog_roll_back_finish_type_name)
PG_RMGR(RM_UBTREE_ID, "UBtree", UBTreeRedo, UBTreeDesc, UBTreeXlogStartup, UBTreeXlogCleanup, UBTreeSafeRestartPoint,
NULL, NULL, ubtree_type_name)
PG_RMGR(RM_UBTREE2_ID, "UBtree2", UBTree2Redo, UBTree2Desc, NULL, NULL, NULL, NULL, NULL, ubtree2_type_name)
PG_RMGR(RM_SEGPAGE_ID, "SegpageStorage", segpage_smgr_redo, segpage_smgr_desc, NULL, NULL, NULL, NULL, NULL, \
    segpage_smgr_type_name)
PG_RMGR(RM_REPLORIGIN_ID, "ReplicationOrigin", replorigin_redo, replorigin_desc, NULL, NULL, NULL, NULL, NULL, \
    replorigin_type_name)
PG_RMGR(RM_COMPRESSION_REL_ID, "CompressionRelation", CfsShrinkRedo, CfsShrinkDesc, NULL, NULL, NULL, NULL, NULL, \
    CfsShrinkTypeName)
PG_RMGR(RM_LOGICALDDLMSG_ID, "LogicalDDLMessage", logicalddlmsg_redo, logicalddlmsg_desc, NULL, NULL, NULL, NULL, NULL, \
    logicalddlmsg_type_name)
