/* ----------
 *	pgstat.h
 *
 *	Definitions for the PostgreSQL statistics collector daemon.
 *
 *	Copyright (c) 2001-2012, PostgreSQL Global Development Group
 *
 *	src/include/pgstat.h
 * ----------
 */
#ifndef PGSTAT_H
#define PGSTAT_H

#include "datatype/timestamp.h"
#include "fmgr.h"
#include "gtm/gtm_c.h"
#include "libpq/pqcomm.h"
#include "mb/pg_wchar.h"
#include "portability/instr_time.h"
#include "storage/barrier.h"
#include "utils/hsearch.h"
#include "utils/relcache.h"
#include "utils/partcache.h"
#include "utils/memutils.h"
#include "pgtime.h"
#include "pgxc/execRemote.h"
#include "storage/lock/lwlock.h"
#include "storage/buf/block.h"
#include "storage/smgr/relfilenode.h"
#include "workload/workload.h"
#include "access/multi_redo_settings.h"
#include "instruments/instr_event.h"
#include "instruments/unique_sql_basic.h"
#include "knl/knl_instance.h"
#include "og_record_time.h"


/* Values for track_functions GUC variable --- order is significant! */
typedef enum TrackFunctionsLevel { TRACK_FUNC_OFF, TRACK_FUNC_PL, TRACK_FUNC_ALL } TrackFunctionsLevel;

/* ----------
 * Paths for the statistics files (relative to installation's $PGDATA).
 * ----------
 */
#define PGSTAT_STAT_PERMANENT_FILENAME "global/pgstat.stat"
#define PGSTAT_STAT_PERMANENT_TMPFILE "global/pgstat.tmp"

/* ----------
 * The types of backend -> collector messages
 * ----------
 */
typedef enum StatMsgType {
    PGSTAT_MTYPE_DUMMY,
    PGSTAT_MTYPE_INQUIRY,
    PGSTAT_MTYPE_TABSTAT,
    PGSTAT_MTYPE_TABPURGE,
    PGSTAT_MTYPE_DROPDB,
    PGSTAT_MTYPE_RESETCOUNTER,
    PGSTAT_MTYPE_RESETSHAREDCOUNTER,
    PGSTAT_MTYPE_RESETSINGLECOUNTER,
    PGSTAT_MTYPE_AUTOVAC_START,
    PGSTAT_MTYPE_VACUUM,
    PGSTAT_MTYPE_TRUNCATE,
    PGSTAT_MTYPE_ANALYZE,
    PGSTAT_MTYPE_BGWRITER,
    PGSTAT_MTYPE_FUNCSTAT,
    PGSTAT_MTYPE_FUNCPURGE,
    PGSTAT_MTYPE_RECOVERYCONFLICT,
    PGSTAT_MTYPE_TEMPFILE,
    PGSTAT_MTYPE_DEADLOCK,
    PGSTAT_MTYPE_AUTOVAC_STAT,
    PGSTAT_MTYPE_FILE,
    PGSTAT_MTYPE_DATA_CHANGED,
    PGSTAT_MTYPE_MEMRESERVED,
    PGSTAT_MTYPE_BADBLOCK,
    PGSTAT_MTYPE_COLLECTWAITINFO,
    PGSTAT_MTYPE_RESPONSETIME,
    PGSTAT_MTYPE_PROCESSPERCENTILE,
    PGSTAT_MTYPE_CLEANUPHOTKEYS,
    PGSTAT_MTYPE_PRUNESTAT
} StatMsgType;

/* ----------
 * The data type used for counters.
 * ----------
 */
typedef int64 PgStat_Counter;

/* ----------
 * PgStat_TableCounts			The actual per-table counts kept by a backend
 *
 * This struct should contain only actual event counters, because we memcmp
 * it against zeroes to detect whether there are any counts to transmit.
 * It is a component of PgStat_TableStatus (within-backend state) and
 * PgStat_TableEntry (the transmitted message format).
 *
 * Note: for a table, tuples_returned is the number of tuples successfully
 * fetched by heap_getnext, while tuples_fetched is the number of tuples
 * successfully fetched by heap_fetch under the control of bitmap indexscans.
 * For an index, tuples_returned is the number of index entries returned by
 * the index AM, while tuples_fetched is the number of tuples successfully
 * fetched by heap_fetch under the control of simple indexscans for this index.
 *
 * tuples_inserted/updated/deleted/hot_updated count attempted actions,
 * regardless of whether the transaction committed.  delta_live_tuples,
 * delta_dead_tuples, and changed_tuples are set depending on commit or abort.
 * Note that delta_live_tuples and delta_dead_tuples can be negative!
 * ----------
 */
typedef struct PgStat_TableCounts {
    PgStat_Counter t_numscans;

    PgStat_Counter t_tuples_returned;
    PgStat_Counter t_tuples_fetched;

    PgStat_Counter t_tuples_inserted;
    PgStat_Counter t_tuples_updated;
    PgStat_Counter t_tuples_deleted;
    PgStat_Counter t_tuples_inplace_updated;
    PgStat_Counter t_tuples_hot_updated;
    bool t_truncated;

    PgStat_Counter t_tuples_inserted_post_truncate;
    PgStat_Counter t_tuples_updated_post_truncate;
    PgStat_Counter t_tuples_deleted_post_truncate;
    PgStat_Counter t_tuples_inplace_updated_post_truncate;

    PgStat_Counter t_delta_live_tuples;
    PgStat_Counter t_delta_dead_tuples;
    PgStat_Counter t_changed_tuples;

    PgStat_Counter t_blocks_fetched;
    PgStat_Counter t_blocks_hit;

    PgStat_Counter t_cu_mem_hit;
    PgStat_Counter t_cu_hdd_sync;
    PgStat_Counter t_cu_hdd_asyn;
} PgStat_TableCounts;

#ifdef DEBUG_UHEAP

typedef uint64 UHeapStat_Counter;

#define MAX_TYPE_GET_TRANSSLOT_FROM            8
#define MAX_TYPE_XID_IN_PROGRESS               7
#define MAX_TYPE_SINGLE_LOCKER_STATUS          4

struct IUD_Count
{
    unsigned int ins;
    unsigned int del;
    unsigned int upd;
};

struct IUD_FreeSpace
{
    size_t ins;
    size_t del;
    size_t upd;
};

typedef struct UHeapStat_Collect
{
    UHeapStat_Counter prune_page[7]; // 0 is success, 1 is not enough space, 2 is update is in progress

    /*
     * Where to get the transaction slot
     * 0 - got a slot reserved by current transaction
     * 3 - got a free slot after invalidation
     * 4 - got a free slot after freezing
     * 7 - cannot get a free slot
     */
    UHeapStat_Counter get_transslot_from[MAX_TYPE_GET_TRANSSLOT_FROM];

    UHeapStat_Counter update[2]; /* 0 inplace 1 non-inplace */

    UHeapStat_Counter dml;
    UHeapStat_Counter retry;
    UHeapStat_Counter retry_time;
    UHeapStat_Counter retry_max;
    UHeapStat_Counter retry_time_max;
    /*
     * Reasons why non-inplace update happened instead of inplace update:
     *
     * 0 - Index Updated
     * 1 - Toast
     * 2 - Page Prune Not Successful
     * 3 - nblocks <= NUM_BLOCKS_FOR_NON_INPLACE_UPDATES
     * 4 - Slot reused
     */
    UHeapStat_Counter noninplace_update_cause[5];

    /* Reasons why a transaction slot is marked frozen
     * 0: transaction slot is already frozen - debugging purposes only
     * 1: xid in slot is older than any xid in undo
     * 2: tuple xid is marked as committed from hint bit
     * 3: tuple slot is invalid but xid in the slot is visible to the snapshot
     * 4: slot is invalid, trans info is grabbed from undo but xid is less than any xid in undo
     * 5: Current xid is the tuple single locker
    */
    UHeapStat_Counter visibility_check_with_xid[6];

    UHeapStat_Counter tuple_visits;
    UHeapStat_Counter tuple_old_version_visits;

    UHeapStat_Counter undo_groups_allocate;
    UHeapStat_Counter undo_slots_allocate;
    UHeapStat_Counter undo_groups_release;
    UHeapStat_Counter undo_slots_recycle;
    UHeapStat_Counter undo_space_recycle;
    UHeapStat_Counter undo_space_unrecycle;
    uint64 oldest_xid_having_undo_delay;

    UHeapStat_Counter undo_page_visited_sum_len;

    UHeapStat_Counter undo_record_prepare_nzero_count;
    UHeapStat_Counter undo_record_prepare_rzero_count;

    UHeapStat_Counter undo_chain_visited_count;
    UHeapStat_Counter undo_chain_visited_sum_len;
    UHeapStat_Counter undo_chain_visited_max_len;
    UHeapStat_Counter undo_chain_visited_min_len;
    UHeapStat_Counter undo_chain_visited_miss_count;

    UHeapStat_Counter undo_discard_lock_hold_cnt;
    UHeapStat_Counter undo_discard_lock_hold_time_sum;
    UHeapStat_Counter undo_discard_lock_hold_time_max;
    UHeapStat_Counter undo_discard_lock_hold_time_min;

    UHeapStat_Counter undo_discard_lock_wait_cnt;
    UHeapStat_Counter undo_discard_lock_wait_time_sum;
    UHeapStat_Counter undo_discard_lock_wait_time_max;
    UHeapStat_Counter undo_discard_lock_wait_time_min;

    UHeapStat_Counter undo_space_lock_hold_cnt;
    UHeapStat_Counter undo_space_lock_hold_time_sum;
    UHeapStat_Counter undo_space_lock_hold_time_max;
    UHeapStat_Counter undo_space_lock_hold_time_min;

    UHeapStat_Counter undo_space_lock_wait_cnt;
    UHeapStat_Counter undo_space_lock_wait_time_sum;
    UHeapStat_Counter undo_space_lock_wait_time_max;
    UHeapStat_Counter undo_space_lock_wait_time_min;
    struct IUD_Count  op_count_suc;
    struct IUD_Count  op_count_tot;
    struct IUD_FreeSpace op_space_tot;
} UHeapStat_Collect;

#define UHeapStat_local t_thrd.uheap_stats_cxt.uheap_stat_local
#define UHeapStat_shared t_thrd.uheap_stats_cxt.uheap_stat_shared

extern void UHeapSchemeInit(void);
extern Size UHeapShmemSize(void);
extern void UHeapStatInit(UHeapStat_Collect *UHeapStat);
extern void AtEOXact_UHeapStats();

/* Type of INPLACE XLOG OPERATION */

/* Type of INDEX XLOG OPERATION */


/* Type of HEAP XLOG OPERATION */

/* Types of UPDATE */
#define INPLACE_UPDATE 0
#define NON_INPLACE_UPDATE 1

/* Types of GetTransactionSlotInfo */


/* Types of visibility_checks_with_xid */
#define VISIBILITY_CHECK_SUCCESS_FROZEN_SLOT      0
#define VISIBILITY_CHECK_SUCCESS_OLDEST_XID       1
#define VISIBILITY_CHECK_SUCCESS_WITH_XID_STATUS  2
#define VISIBILITY_CHECK_SUCCESS_INVALID_SLOT     3
#define VISIBILITY_CHECK_SUCCESS_UNDO             4
#define VISIBILITY_CHECK_SUCCESS_SINGLE_LOCKER    5

/* Types of visibility_checks */

/* IndeOnlyNext Tuple Visibility. */

/* Types of prune_page */
#define PRUNE_PAGE_SUCCESS            0
#define PRUNE_PAGE_NO_SPACE           1
#define PRUNE_PAGE_UPDATE_IN_PROGRESS 2
#define PRUNE_PAGE_IN_RECOVERY        3
#define PRUNE_PAGE_INVALID            4
#define PRUNE_PAGE_XID_FILTER         5
#define PRUNE_PAGE_FILLFACTOR         6

/* Places to get transslot */
#define TRANSSLOT_RESERVED_BY_CURRENT_XID		0
#define TRANSSLOT_FREE							1
#define TRANSSLOT_ON_TPD_BY_CURRENT_XID			2
#define TRANSSLOT_FREE_AFTER_INVALIDATION		3
#define TRANSSLOT_FREE_AFTER_FREEZING			4
#define TRANSSLOT_RESERVE_FREE_SLOTT_FROM_TPD	5
#define TRANSSLOT_ALLOCATE_TPD_AND_RESERVE		6
#define TRANSSLOT_CANNOT_GET					7

/* Noninplace update reasons */
#define INDEX_UPDATED				0
#define TOAST						1
#define PAGE_PRUNE_FAILED	2
#define nblocks_LESS_THAN_NBLOCKS	3
#define SLOT_REUSED					4

#define UHEAPSTAT_COUNT_DML() \
                UHeapStat_local->dml += 1
#define UHEAPSTAT_COUNT_RETRY(time, retry) \
                UHeapStat_local->retry += retry;              \
                UHeapStat_local->retry_time += time;         \
                UHeapStat_local->retry_max = Max(UHeapStat_local->retry_max, retry);          \
                UHeapStat_local->retry_time_max = Max(UHeapStat_local->retry_time_max, time)

#define UHEAPSTAT_COUNT_OP_PRUNEPAGE(optype, counts) \
        UHeapStat_local->op_count_tot.optype += counts;
    
#define UHEAPSTAT_COUNT_OP_PRUNEPAGE_SUC(optype, counts) \
        UHeapStat_local->op_count_suc.optype += counts;
    
#define UHEAPSTAT_COUNT_OP_PRUNEPAGE_SPC(optype, counts) \
        UHeapStat_local->op_space_tot.optype += counts;

#define UHEAPSTAT_COUNT_PRUNEPAGE(type) \
    UHeapStat_local->prune_page[type] += 1
#define UHEAPSTAT_COUNT_GET_TRANSSLOT_FROM(type) \
    UHeapStat_local->get_transslot_from[type] += 1
#define UHEAPSTAT_COUNT_UPDATE(type) \
    UHeapStat_local->update[type] += 1
#define UHEAPSTAT_COUNT_NONINPLACE_UPDATE_CAUSE(type) \
    UHeapStat_local->noninplace_update_cause[type] += 1
#define UHEAPSTAT_COUNT_VISIBILITY_CHECK_WITH_XID(result) \
    UHeapStat_local->visibility_check_with_xid[result] += 1

#define UHEAPSTAT_COUNT_TUPLE_VISITS() \
    UHeapStat_local->tuple_visits += 1
#define UHEAPSTAT_COUNT_TUPLE_OLD_VERSION_VISITS() \
    UHeapStat_local->tuple_old_version_visits += 1

#define UHEAPSTAT_COUNT_UNDO_GROUPS_ALLOCATE() \
    UHeapStat_shared->undo_groups_allocate += 1
#define UHEAPSTAT_COUNT_UNDO_GROUPS_RELEASE() \
    UHeapStat_shared->undo_groups_release += 1
#define UHEAPSTAT_COUNT_UNDO_SLOTS_ALLOCATE() \
    UHeapStat_shared->undo_slots_allocate += 1
#define UHEAPSTAT_COUNT_UNDO_SLOTS_RECYCLE() \
    UHeapStat_shared->undo_slots_recycle += 1
#define UHEAPSTAT_COUNT_UNDO_SPACE_RECYCLE() \
    UHeapStat_shared->undo_space_recycle += 1
#define UHEAPSTAT_COUNT_UNDO_SPACE_UNRECYCLE() \
    UHeapStat_shared->undo_space_unrecycle += 1
#define UHEAPSTAT_COUNT_XID_DELAY(len) \
    UHeapStat_shared->oldest_xid_having_undo_delay = len

#define UHEAPSTAT_COUNT_UNDO_PAGE_VISITS() \
    UHeapStat_local->undo_page_visited_sum_len += 1

#define UHEAPSTAT_COUNT_UNDO_RECORD_PREPARE_NZERO() \
    UHeapStat_local->undo_record_prepare_nzero_count += 1
#define UHEAPSTAT_COUNT_UNDO_RECORD_PREPARE_RZERO() \
    UHeapStat_local->undo_record_prepare_rzero_count += 1

#define UHEAPSTAT_COUNT_UNDO_CHAIN_VISTIED(len)                 \
    UHeapStat_local->undo_chain_visited_count++;                \
    UHeapStat_local->undo_chain_visited_sum_len += len;         \
    if (len > UHeapStat_local->undo_chain_visited_max_len) {    \
        UHeapStat_local->undo_chain_visited_max_len = len;      \
    }                                                           \
    if (len < UHeapStat_local->undo_chain_visited_min_len) {    \
        UHeapStat_local->undo_chain_visited_min_len = len;      \
    }
#define UHEAPSTAT_COUNT_UNDO_CHAIN_VISITED_MISS() \
    UHeapStat_local->undo_chain_visited_miss_count += 1

#define UHEAPSTAT_UPDATE_UNDO_DISCARD_LOCK_WAIT_TIME(time) \
    UHeapStat_local->undo_discard_lock_wait_cnt += 1; \
    UHeapStat_local->undo_discard_lock_wait_time_sum += time; \
    UHeapStat_local->undo_discard_lock_wait_time_max = Max(UHeapStat_local->undo_discard_lock_wait_time_max, time); \
    UHeapStat_local->undo_discard_lock_wait_time_min = Min(UHeapStat_local->undo_discard_lock_wait_time_min, time)
#define UHEAPSTAT_UPDATE_UNDO_DISCARD_LOCK_HOLD_TIME(time) \
    UHeapStat_local->undo_discard_lock_hold_cnt += 1; \
    UHeapStat_local->undo_discard_lock_hold_time_sum += time; \
    UHeapStat_local->undo_discard_lock_hold_time_max = Max(UHeapStat_local->undo_discard_lock_hold_time_max, time); \
    UHeapStat_local->undo_discard_lock_hold_time_min = Min(UHeapStat_local->undo_discard_lock_hold_time_min, time)
#define UHEAPSTAT_UPDATE_UNDO_SPACE_LOCK_WAIT_TIME(time) \
    UHeapStat_local->undo_space_lock_wait_cnt += 1; \
    UHeapStat_local->undo_space_lock_wait_time_sum += time; \
    UHeapStat_local->undo_space_lock_wait_time_max = Max(UHeapStat_local->undo_space_lock_wait_time_max, time); \
    UHeapStat_local->undo_space_lock_wait_time_min = Min(UHeapStat_local->undo_space_lock_wait_time_min, time)
#define UHEAPSTAT_UPDATE_UNDO_SPACE_LOCK_HOLD_TIME(time) \
    UHeapStat_local->undo_space_lock_hold_cnt += 1; \
    UHeapStat_local->undo_space_lock_hold_time_sum += time; \
    UHeapStat_local->undo_space_lock_hold_time_max = Max(UHeapStat_local->undo_space_lock_hold_time_max, time); \
    UHeapStat_local->undo_space_lock_hold_time_min = Min(UHeapStat_local->undo_space_lock_hold_time_min, time)
#endif // DEBUG_UHEAP

/* Possible targets for resetting cluster-wide shared values */
typedef enum PgStat_Shared_Reset_Target { RESET_BGWRITER } PgStat_Shared_Reset_Target;

/* Possible object types for resetting single counters */
typedef enum PgStat_Single_Reset_Type { RESET_TABLE, RESET_FUNCTION } PgStat_Single_Reset_Type;

/* ------------------------------------------------------------
 * Structures kept in backend local memory while accumulating counts
 * ------------------------------------------------------------
 */

/* ----------
 * PgStat_TableStatus			Per-table status within a backend
 *
 * Many of the event counters are nontransactional, ie, we count events
 * in committed and aborted transactions alike.  For these, we just count
 * directly in the PgStat_TableStatus.	However, delta_live_tuples,
 * delta_dead_tuples, and changed_tuples must be derived from event counts
 * with awareness of whether the transaction or subtransaction committed or
 * aborted.  Hence, we also keep a stack of per-(sub)transaction status
 * records for every table modified in the current transaction.  At commit
 * or abort, we propagate tuples_inserted/updated/deleted up to the
 * parent subtransaction level, or out to the parent PgStat_TableStatus,
 * as appropriate.
 * ----------
 */
typedef struct PgStat_TableStatus {
    Oid t_id;      /* table's OID */
    bool t_shared; /* is it a shared catalog? */
    /*
     * if t_id is a parition oid , then t_statFlag is the corresponding partitioned table oid;
     * fi t_id is a non-parition oid, then t_statFlag is InvlaidOId
     */
    uint32 t_statFlag;
    struct PgStat_TableXactStatus* trans; /* lowest subxact's counts */
    PgStat_TableCounts t_counts;          /* event counts to be sent */

    /*
     * The global statistics has been checked.
     * We should do it once per transaction for perf reasons.
     * Nice to have: might need to add an expiry for long running transactions.
     */
    bool t_globalStatChecked;

    /* snapshot deadtuples % */
    float4 t_freeRatio;
    float4 t_pruneSuccessRatio;

    /*
     * Pointer to the starting_blocks array and the current index
     * in the array we're currently traversing.
     */
    pg_atomic_uint32 *startBlockArray;
    uint32 startBlockIndex;
} PgStat_TableStatus;

/* ----------
 * PgStat_TableXactStatus		Per-table, per-subtransaction status
 * ----------
 */
typedef struct PgStat_TableXactStatus {
    PgStat_Counter tuples_inserted; /* tuples inserted in (sub)xact */
    PgStat_Counter tuples_updated;  /* tuples updated in (sub)xact */
    PgStat_Counter tuples_deleted;  /* tuples deleted in (sub)xact */
    PgStat_Counter tuples_inplace_updated;
    bool truncated;                 /* relation truncated in this (sub)xact */
    PgStat_Counter inserted_pre_trunc;  /* tuples inserted prior to truncate */
    PgStat_Counter updated_pre_trunc;   /* tuples updated prior to truncate */
    PgStat_Counter deleted_pre_trunc;   /* tuples deleted prior to truncate */
    PgStat_Counter inplace_updated_pre_trunc;
    /* The following members with _accum suffix will not be erased by truncate */
    PgStat_Counter tuples_inserted_accum;
    PgStat_Counter tuples_updated_accum;
    PgStat_Counter tuples_deleted_accum;
    PgStat_Counter tuples_inplace_updated_accum;
    int nest_level;                 /* subtransaction nest level */
    /* links to other structs for same relation: */
    struct PgStat_TableXactStatus* upper; /* next higher subxact if any */
    PgStat_TableStatus* parent;           /* per-table status */
    /* structs of same subxact level are linked here: */
    struct PgStat_TableXactStatus* next; /* next of same subxact */
} PgStat_TableXactStatus;

/* ------------------------------------------------------------
 * Message formats follow
 * ------------------------------------------------------------
 */

/* ----------
 * PgStat_MsgHdr				The common message header
 * ----------
 */
typedef struct PgStat_MsgHdr {
    StatMsgType m_type;
    int m_size;
} PgStat_MsgHdr;

/* ----------
 * Space available in a message.  This will keep the UDP packets below 1K,
 * which should fit unfragmented into the MTU of the lo interface on most
 * platforms. Does anybody care for platforms where it doesn't?
 * ----------
 */
#define PGSTAT_MSG_PAYLOAD (1000 - sizeof(PgStat_MsgHdr))

/* ----------
 * PgStat_MsgDummy				A dummy message, ignored by the collector
 * ----------
 */
typedef struct PgStat_MsgDummy {
    PgStat_MsgHdr m_hdr;
} PgStat_MsgDummy;

/* ----------
 * PgStat_MsgInquiry			Sent by a backend to ask the collector
 *								to write the stats file.
 * ----------
 */

typedef struct PgStat_MsgInquiry {
    PgStat_MsgHdr m_hdr;
    TimestampTz inquiry_time; /* minimum acceptable file timestamp */
} PgStat_MsgInquiry;

/* ----------
 * PgStat_TableEntry			Per-table info in a MsgTabstat
 * ----------
 */
typedef struct PgStat_TableEntry {
    Oid t_id;
    uint32 t_statFlag;
    PgStat_TableCounts t_counts;
} PgStat_TableEntry;

/* ----------
 * PgStat_MsgTabstat			Sent by the backend to report table
 *								and buffer access statistics.
 * ----------
 */
#define PGSTAT_NUM_TABENTRIES ((PGSTAT_MSG_PAYLOAD - sizeof(Oid) - 3 * sizeof(int)) / sizeof(PgStat_TableEntry))

typedef struct PgStat_MsgTabstat {
    PgStat_MsgHdr m_hdr;
    Oid m_databaseid;
    int m_nentries;
    int m_xact_commit;
    int m_xact_rollback;
    PgStat_Counter m_block_read_time; /* times in microseconds */
    PgStat_Counter m_block_write_time;
    PgStat_TableEntry m_entry[PGSTAT_NUM_TABENTRIES];
} PgStat_MsgTabstat;

/* ----------
 * PgStat_MsgTabpurge			Sent by the backend to tell the collector
 *								about dead tables.
 * ----------
 */
typedef struct PgStat_MsgTabEntry {
    Oid m_tableid;
    uint32 m_statFlag;
} PgStat_MsgTabEntry;
#define PGSTAT_NUM_TABPURGE ((PGSTAT_MSG_PAYLOAD - sizeof(Oid) - sizeof(int)) / sizeof(PgStat_MsgTabEntry))

typedef struct PgStat_MsgTabpurge {
    PgStat_MsgHdr m_hdr;
    Oid m_databaseid;
    int m_nentries;
    PgStat_MsgTabEntry m_entry[PGSTAT_NUM_TABPURGE];
} PgStat_MsgTabpurge;

/* ----------
 * PgStat_MsgDropdb				Sent by the backend to tell the collector
 *								about a dropped database
 * ----------
 */
typedef struct PgStat_MsgDropdb {
    PgStat_MsgHdr m_hdr;
    Oid m_databaseid;
} PgStat_MsgDropdb;

/* ----------
 * PgStat_MsgResetcounter		Sent by the backend to tell the collector
 *								to reset counters
 * ----------
 */
typedef struct PgStat_MsgResetcounter {
    PgStat_MsgHdr m_hdr;
    Oid m_databaseid;
} PgStat_MsgResetcounter;

typedef struct PgStat_MsgCleanupHotkeys {
    PgStat_MsgHdr m_hdr;
    Oid m_databaseOid;
    Oid m_tableOid;
} PgStat_MsgCleanupHotkeys;

/* ----------
 * PgStat_MsgResetsharedcounter Sent by the backend to tell the collector
 *								to reset a shared counter
 * ----------
 */
typedef struct PgStat_MsgResetsharedcounter {
    PgStat_MsgHdr m_hdr;
    PgStat_Shared_Reset_Target m_resettarget;
} PgStat_MsgResetsharedcounter;

/* ----------
 * PgStat_MsgResetsinglecounter Sent by the backend to tell the collector
 *								to reset a single counter
 * ----------
 */
typedef struct PgStat_MsgResetsinglecounter {
    PgStat_MsgHdr m_hdr;
    Oid m_databaseid;
    PgStat_Single_Reset_Type m_resettype;
    Oid m_objectid;
    Oid p_objectid; /* parentted objectid if m_objectid is a partition objectid */
} PgStat_MsgResetsinglecounter;

/* ----------
 * PgStat_MsgAutovacStart		Sent by the autovacuum daemon to signal
 *								that a database is going to be processed
 * ----------
 */
typedef struct PgStat_MsgAutovacStart {
    PgStat_MsgHdr m_hdr;
    Oid m_databaseid;
    TimestampTz m_start_time;
} PgStat_MsgAutovacStart;

/* ----------
 * PgStat_MsgVacuum				Sent by the backend or autovacuum daemon
 *								after VACUUM
 * ----------
 */
typedef struct PgStat_MsgVacuum {
    PgStat_MsgHdr m_hdr;
    Oid m_databaseid;
    Oid m_tableoid;
    uint32 m_statFlag;
    bool m_autovacuum;
    TimestampTz m_vacuumtime;
    PgStat_Counter m_tuples;
} PgStat_MsgVacuum;

typedef struct PgStat_MsgPrune {
    PgStat_MsgHdr m_hdr;
    Oid m_databaseid;
    Oid m_tableoid;
    uint32 m_statFlag;
    PgStat_Counter m_scanned_blocks;
    PgStat_Counter m_pruned_blocks;
} PgStat_MsgPrune;

/* ----------
 * PgStat_MsgIUD                             Sent by the insert/delete/update
 * ----------
 */
typedef struct PgStat_MsgDataChanged {
    PgStat_MsgHdr m_hdr;
    Oid m_databaseid;
    Oid m_tableoid;
    uint32 m_statFlag;
    TimestampTz m_changed_time;
} PgStat_MsgDataChanged;

/* ----------
 * PgStat_MsgAvStat	Sent autovac stat to collector
 *				Reset by
 * ----------
 */
#define AV_TIMEOUT (1 << 0) /* autovac have been canceled due to timeout */
#define AV_ANALYZE (1 << 1) /* is doing auto-analyze */
#define AV_VACUUM (1 << 2)  /* is doing auto-vacuum */

typedef struct PgStat_MsgAutovacStat {
    PgStat_MsgHdr m_hdr;
    Oid m_databaseid;
    Oid m_tableoid;
    uint32 m_statFlag;
    int64 m_autovacStat;
} PgStat_MsgAutovacStat;

/* ----------
 * PgStat_MsgTruncate				Sent by the truncate
 * ----------
 */

typedef struct PgStat_MsgTruncate {
    PgStat_MsgHdr m_hdr;
    Oid m_databaseid;
    Oid m_tableoid;
    /*
     * if m_tableoid is partition oid, then m_statFlag is the corresponding
     * partitioned table oid, else it is InvalidOId
     */
    uint32 m_statFlag;
} PgStat_MsgTruncate;

/* ----------
 * PgStat_MsgAnalyze			Sent by the backend or autovacuum daemon
 *								after ANALYZE
 * ----------
 */
typedef struct PgStat_MsgAnalyze {
    PgStat_MsgHdr m_hdr;
    Oid m_databaseid;
    Oid m_tableoid;
    uint32 m_statFlag;
    bool m_autovacuum;
    TimestampTz m_analyzetime;
    PgStat_Counter m_live_tuples;
    PgStat_Counter m_dead_tuples;
} PgStat_MsgAnalyze;

/* ----------
 * PgStat_MsgBgWriter			Sent by the bgwriter to update statistics.
 * ----------
 */
typedef struct PgStat_MsgBgWriter {
    PgStat_MsgHdr m_hdr;

    PgStat_Counter m_timed_checkpoints;
    PgStat_Counter m_requested_checkpoints;
    PgStat_Counter m_buf_written_checkpoints;
    PgStat_Counter m_buf_written_clean;
    PgStat_Counter m_maxwritten_clean;
    PgStat_Counter m_buf_written_backend;
    PgStat_Counter m_buf_fsync_backend;
    PgStat_Counter m_buf_alloc;
    PgStat_Counter m_checkpoint_write_time; /* times in milliseconds */
    PgStat_Counter m_checkpoint_sync_time;
} PgStat_MsgBgWriter;

/* ----------
 * PgStat_MsgRecoveryConflict	Sent by the backend upon recovery conflict
 * ----------
 */
typedef struct PgStat_MsgRecoveryConflict {
    PgStat_MsgHdr m_hdr;

    Oid m_databaseid;
    int m_reason;
} PgStat_MsgRecoveryConflict;

/* ----------
 * PgStat_MsgTempFile	Sent by the backend upon creating a temp file
 * ----------
 */
typedef struct PgStat_MsgTempFile {
    PgStat_MsgHdr m_hdr;

    Oid m_databaseid;
    size_t m_filesize;
} PgStat_MsgTempFile;

/* ----------
 * PgStat_MsgMemReserved     Sent by the backend upon succeeding in reserving memory
 * ----------
 */
typedef struct PgStat_MsgMemReserved {
    PgStat_MsgHdr m_hdr;
    Oid m_databaseid;
    int64 m_memMbytes;
    int m_reserve_or_release;
} PgStat_MsgMemReserved;

/* ----------
 * PgStat_FunctionCounts	The actual per-function counts kept by a backend
 *
 * This struct should contain only actual event counters, because we memcmp
 * it against zeroes to detect whether there are any counts to transmit.
 *
 * Note that the time counters are in instr_time format here.  We convert to
 * microseconds in PgStat_Counter format when transmitting to the collector.
 * ----------
 */
typedef struct PgStat_FunctionCounts {
    PgStat_Counter f_numcalls;
    instr_time f_total_time;
    instr_time f_self_time;
} PgStat_FunctionCounts;

/* ----------
 * PgStat_BackendFunctionEntry	Entry in backend's per-function hash table
 * ----------
 */
typedef struct PgStat_BackendFunctionEntry {
    Oid f_id;
    PgStat_FunctionCounts f_counts;
} PgStat_BackendFunctionEntry;

/* ----------
 * PgStat_FunctionEntry			Per-function info in a MsgFuncstat
 * ----------
 */
typedef struct PgStat_FunctionEntry {
    Oid f_id;
    PgStat_Counter f_numcalls;
    PgStat_Counter f_total_time; /* times in microseconds */
    PgStat_Counter f_self_time;
} PgStat_FunctionEntry;

/* ----------
 * PgStat_MsgFuncstat			Sent by the backend to report function
 *								usage statistics.
 * ----------
 */
#define PGSTAT_NUM_FUNCENTRIES ((PGSTAT_MSG_PAYLOAD - sizeof(Oid) - sizeof(int)) / sizeof(PgStat_FunctionEntry))

typedef struct PgStat_MsgFuncstat {
    PgStat_MsgHdr m_hdr;
    Oid m_databaseid;
    int m_nentries;
    PgStat_FunctionEntry m_entry[PGSTAT_NUM_FUNCENTRIES];
} PgStat_MsgFuncstat;

/* ----------
 * PgStat_MsgFuncpurge			Sent by the backend to tell the collector
 *								about dead functions.
 * ----------
 */
#define PGSTAT_NUM_FUNCPURGE ((PGSTAT_MSG_PAYLOAD - sizeof(Oid) - sizeof(int)) / sizeof(Oid))

typedef struct PgStat_MsgFuncpurge {
    PgStat_MsgHdr m_hdr;
    Oid m_databaseid;
    int m_nentries;
    Oid m_functionid[PGSTAT_NUM_FUNCPURGE];
} PgStat_MsgFuncpurge;

/* ----------
 * PgStat_MsgDeadlock			Sent by the backend to tell the collector
 *								about a deadlock that occurred.
 * ----------
 */
typedef struct PgStat_MsgDeadlock {
    PgStat_MsgHdr m_hdr;
    Oid m_databaseid;
} PgStat_MsgDeadlock;

/* ----------
 * PgStat_MsgFile	Sent by the backend upon reading/writing a data file
 * ----------
 */
typedef struct PgStat_MsgFile {
    PgStat_MsgHdr m_hdr;

    Oid dbid;
    Oid spcid;
    Oid fn;
    char rw;
    PgStat_Counter cnt;
    PgStat_Counter blks;
    PgStat_Counter tim;
    PgStat_Counter lsttim;
    PgStat_Counter mintim;
    PgStat_Counter maxtim;
} PgStat_MsgFile;

/*
 * PgStat_MsgBadBlock Sent by the backend where read bad page / cu
 */

typedef struct BadBlockHashKey {
    RelFileNode relfilenode;
    ForkNumber forknum;
} BadBlockHashKey;

typedef struct BadBlockHashEnt {
    BadBlockHashKey key;
    int error_count;
    TimestampTz first_time;
    TimestampTz last_time;
} BadBlockHashEnt;

#define PGSTAT_NUM_BADBLOCK_ENTRIES ((PGSTAT_MSG_PAYLOAD - sizeof(int)) / sizeof(BadBlockHashEnt))

typedef struct PgStat_MsgBadBlock {
    PgStat_MsgHdr m_hdr;
    int m_nentries;
    BadBlockHashEnt m_entry[PGSTAT_NUM_BADBLOCK_ENTRIES];
} PgStat_MsgBadBlock;

#ifndef ENABLE_LITE_MODE
const int MAX_SQL_RT_INFO_COUNT = 100000;
#else
const int MAX_SQL_RT_INFO_COUNT = 10;
#endif

typedef struct SqlRTInfo {
    uint64 UniqueSQLId;
    int64 start_time;
    int64 rt;
} SqlRTInfo;

typedef struct SqlRTInfoArray {
    volatile int32 sqlRTIndex;
    bool isFull;
    SqlRTInfo sqlRT[MAX_SQL_RT_INFO_COUNT];
} SqlRTInfoArray;

typedef struct PgStat_SqlRT {
    PgStat_MsgHdr m_hdr;
    SqlRTInfo sqlRT;
} PgStat_SqlRT;
const int MAX_SQL_RT_INFO_COUNT_REMOTE = MaxAllocSize / sizeof(SqlRTInfo);

typedef struct PgStat_PrsPtl {
    PgStat_MsgHdr m_hdr;
    int64 now;
} PgStat_PrsPtl;

/* ----------
 * PgStat_Msg					Union over all possible messages.
 * ----------
 */
typedef union PgStat_Msg {
    PgStat_MsgHdr msg_hdr;
    PgStat_MsgDummy msg_dummy;
    PgStat_MsgInquiry msg_inquiry;
    PgStat_MsgTabstat msg_tabstat;
    PgStat_MsgTabpurge msg_tabpurge;
    PgStat_MsgDropdb msg_dropdb;
    PgStat_MsgResetcounter msg_resetcounter;
    PgStat_MsgResetsharedcounter msg_resetsharedcounter;
    PgStat_MsgResetsinglecounter msg_resetsinglecounter;
    PgStat_MsgAutovacStart msg_autovacuum;
    PgStat_MsgVacuum msg_vacuum;
    PgStat_MsgTruncate msg_truncate;
    PgStat_MsgAnalyze msg_analyze;
    PgStat_MsgBgWriter msg_bgwriter;
    PgStat_MsgFuncstat msg_funcstat;
    PgStat_MsgFuncpurge msg_funcpurge;
    PgStat_MsgRecoveryConflict msg_recoveryconflict;
    PgStat_MsgDeadlock msg_deadlock;
    PgStat_MsgFile msg_file;
    PgStat_SqlRT msg_sqlrt;
    PgStat_PrsPtl msg_prosqlrt;
} PgStat_Msg;

/* ------------------------------------------------------------
 * Statistic collector data structures follow
 *
 * PGSTAT_FILE_FORMAT_ID should be changed whenever any of these
 * data structures change.
 * ------------------------------------------------------------
 */

#define PGSTAT_FILE_FORMAT_ID 0x01A5BC9B

/* ----------
 * PgStat_StatDBEntry			The collector's data per database
 * ----------
 */
typedef struct PgStat_StatDBEntry {
    Oid databaseid;
    PgStat_Counter n_xact_commit;
    PgStat_Counter n_xact_rollback;
    PgStat_Counter n_blocks_fetched;
    PgStat_Counter n_blocks_hit;

    PgStat_Counter n_cu_mem_hit;
    PgStat_Counter n_cu_hdd_sync;
    PgStat_Counter n_cu_hdd_asyn;

    PgStat_Counter n_tuples_returned;
    PgStat_Counter n_tuples_fetched;
    PgStat_Counter n_tuples_inserted;
    PgStat_Counter n_tuples_updated;
    PgStat_Counter n_tuples_deleted;
    TimestampTz last_autovac_time;
    PgStat_Counter n_conflict_tablespace;
    PgStat_Counter n_conflict_lock;
    PgStat_Counter n_conflict_snapshot;
    PgStat_Counter n_conflict_bufferpin;
    PgStat_Counter n_conflict_startup_deadlock;
    PgStat_Counter n_temp_files;
    PgStat_Counter n_temp_bytes;
    PgStat_Counter n_deadlocks;
    PgStat_Counter n_block_read_time; /* times in microseconds */
    PgStat_Counter n_block_write_time;
    PgStat_Counter n_mem_mbytes_reserved;

    TimestampTz stat_reset_timestamp;

    /*
     * tables and functions must be last in the struct, because we don't write
     * the pointers out to the stats file.
     */
    HTAB* tables;
    HTAB* functions;
} PgStat_StatDBEntry;

#define START_BLOCK_ARRAY_SIZE 20

typedef struct PgStat_StartBlockTableKey {
    Oid dbid;
    Oid relid;
    Oid parentid;
} PgStat_StartBlockTableKey;

typedef struct PgStat_StartBlockTableEntry {
    PgStat_StartBlockTableKey tabkey;
    pg_atomic_uint32 starting_blocks[START_BLOCK_ARRAY_SIZE];
} PgStat_StartBlockTableEntry;

typedef struct UHeapPruneStat {
    /* in-memory representation of db and table statistics. Updated by GlobalStatsTracker thread */
    HTAB *global_stats_map;

    /* No backend can read the hash when 1 */
    pg_atomic_uint64 quiesce;

    /* Current number of backends reading the hash */
    pg_atomic_uint64 readers;

    /* assigns a block number for a backend to prune */
    HTAB *blocks_map;

    /* Memory context where the HTABs live */
    MemoryContext global_stats_cxt;
} UHeapPruneStat;

typedef enum PgStat_StatTabType {
    STATFLG_RELATION = 0,  /* stat info for relation */
    STATFLG_PARTITION = 1, /* stat info for partition */
} PgStat_StatTabType;

/*
 * stat table key
 * if tableid is a relation oid, statFlag is invalidoid
 * if tableid is a partition oid, statFlag is the corresponding
 * partitioned table's oid
 */
typedef struct PgStat_StatTabKey {
    Oid tableid;
    uint32 statFlag;
} PgStat_StatTabKey;

/* ----------
 * PgStat_StatTabEntry			The collector's data per table (or index)
 * ----------
 */
#define CONTINUED_TIMEOUT_BITMAP 0X00000000000000FF
#define MAX_CONTINUED_TIMEOUT_COUNT CONTINUED_TIMEOUT_BITMAP
#define CONTINUED_TIMEOUT_COUNT(status) (0X00000000000000FF & (status))
#define reset_continued_timeout(status)                      \
    do {                                                     \
        (status) = ((~CONTINUED_TIMEOUT_BITMAP) & (status)); \
    } while (0);

#define increase_continued_timeout(status)                                 \
    do {                                                                   \
        if (CONTINUED_TIMEOUT_COUNT(status) < MAX_CONTINUED_TIMEOUT_COUNT) \
            (status)++;                                                    \
    } while (0);

#define TOTAL_TIMEOUT_BITMAP 0X0000000000FFFF00
#define MAX_TOTAL_TIMEOUT_COUNT TOTAL_TIMEOUT_BITMAP >> 16
#define TOTAL_TIMEOUT_COUNT(status) (((status)&TOTAL_TIMEOUT_BITMAP) >> 16)
#define increase_toatl_timeout(status)                             \
    do {                                                           \
        if (TOTAL_TIMEOUT_COUNT(status) < MAX_TOTAL_TIMEOUT_COUNT) \
            status = status + 0X0000000000000100;                  \
    } while (0);

typedef struct PgStat_StatTabEntry {
    PgStat_StatTabKey tablekey;

    PgStat_Counter numscans;

    PgStat_Counter tuples_returned;
    PgStat_Counter tuples_fetched;

    PgStat_Counter tuples_inserted;
    PgStat_Counter tuples_updated;
    PgStat_Counter tuples_deleted;
    PgStat_Counter tuples_inplace_updated;
    PgStat_Counter tuples_hot_updated;

    PgStat_Counter n_live_tuples;
    PgStat_Counter n_dead_tuples;
    PgStat_Counter changes_since_analyze;

    PgStat_Counter blocks_fetched;
    PgStat_Counter blocks_hit;

    PgStat_Counter cu_mem_hit;
    PgStat_Counter cu_hdd_sync;
    PgStat_Counter cu_hdd_asyn;

    PgStat_Counter success_prune_cnt;
    PgStat_Counter total_prune_cnt;

    TimestampTz vacuum_timestamp; /* user initiated vacuum */
    PgStat_Counter vacuum_count;
    TimestampTz autovac_vacuum_timestamp; /* autovacuum initiated */
    PgStat_Counter autovac_vacuum_count;
    TimestampTz analyze_timestamp; /* user initiated */
    PgStat_Counter analyze_count;
    TimestampTz autovac_analyze_timestamp; /* autovacuum initiated */
    PgStat_Counter autovac_analyze_count;
    TimestampTz data_changed_timestamp; /* start to insert/delete/upate */
    uint64 autovac_status;
} PgStat_StatTabEntry;

/* ----------
 * PgStat_StatFuncEntry			The collector's data per function
 * ----------
 */
typedef struct PgStat_StatFuncEntry {
    Oid functionid;

    PgStat_Counter f_numcalls;

    PgStat_Counter f_total_time; /* times in microseconds */
    PgStat_Counter f_self_time;
} PgStat_StatFuncEntry;

/*
 * Global statistics kept in the stats collector
 */
typedef struct PgStat_GlobalStats {
    TimestampTz stats_timestamp; /* time of stats file update */
    PgStat_Counter timed_checkpoints;
    PgStat_Counter requested_checkpoints;
    PgStat_Counter checkpoint_write_time; /* times in milliseconds */
    PgStat_Counter checkpoint_sync_time;
    PgStat_Counter buf_written_checkpoints;
    PgStat_Counter buf_written_clean;
    PgStat_Counter maxwritten_clean;
    PgStat_Counter buf_written_backend;
    PgStat_Counter buf_fsync_backend;
    PgStat_Counter buf_alloc;
    TimestampTz stat_reset_timestamp;
} PgStat_GlobalStats;

/* ----------
 * Backend states
 * ----------
 */
typedef enum BackendState {
    STATE_UNDEFINED,
    STATE_IDLE,
    STATE_RUNNING,
    STATE_IDLEINTRANSACTION,
    STATE_FASTPATH,
    STATE_IDLEINTRANSACTION_ABORTED,
    STATE_DISABLED,
    STATE_RETRYING,
    STATE_COUPLED,
    STATE_DECOUPLED,
} BackendState;

/* ----------
 * Backend waiting states
 * NOTE: if you add a WaitState enum value, remember to add it's description in WaitStateDesc.
 * ----------
 */
typedef enum WaitState {
    STATE_WAIT_UNDEFINED = 0,
    STATE_WAIT_LWLOCK,
    STATE_WAIT_LOCK,
    STATE_WAIT_IO,
    STATE_WAIT_COMM,
    STATE_WAIT_POOLER_GETCONN,
    STATE_WAIT_POOLER_ABORTCONN,
    STATE_WAIT_POOLER_CLEANCONN,
    STATE_POOLER_CREATE_CONN,
    STATE_POOLER_WAIT_GETCONN,
    STATE_POOLER_WAIT_SETCMD,
    STATE_POOLER_WAIT_RESETCMD,
    STATE_POOLER_WAIT_CANCEL,
    STATE_POOLER_WAIT_STOP,
    STATE_WAIT_NODE,
    STATE_WAIT_XACTSYNC,
    STATE_WAIT_WALSYNC,
    STATE_WAIT_DATASYNC,
    STATE_WAIT_DATASYNC_QUEUE,
    STATE_WAIT_FLUSH_DATA,
    STATE_WAIT_RESERVE_TD,
    STATE_WAIT_TD_ROLLBACK,
    STATE_WAIT_AVAILABLE_TD,
    STATE_WAIT_TRANSACTION_ROLLBACK,
    STATE_PRUNE_TABLE,
    STATE_PRUNE_INDEX,
    STATE_STREAM_WAIT_CONNECT_NODES,
    STATE_STREAM_WAIT_PRODUCER_READY,
    STATE_STREAM_WAIT_THREAD_SYNC_QUIT,
    STATE_STREAM_WAIT_NODEGROUP_DESTROY,
    STATE_WAIT_ACTIVE_STATEMENT,
    STATE_WAIT_MEMORY,
    STATE_EXEC_SORT,
    STATE_EXEC_SORT_FETCH_TUPLE,
    STATE_EXEC_SORT_WRITE_FILE,
    STATE_EXEC_MATERIAL,
    STATE_EXEC_MATERIAL_WRITE_FILE,
    STATE_EXEC_HASHJOIN_BUILD_HASH,
    STATE_EXEC_HASHJOIN_WRITE_FILE,
    STATE_EXEC_HASHAGG_BUILD_HASH,
    STATE_EXEC_HASHAGG_WRITE_FILE,
    STATE_EXEC_HASHSETOP_BUILD_HASH,
    STATE_EXEC_HASHSETOP_WRITE_FILE,
    STATE_EXEC_NESTLOOP,
    STATE_CREATE_INDEX,
    STATE_ANALYZE,
    STATE_VACUUM,
    STATE_VACUUM_FULL,
    STATE_GTM_CONNECT,
    STATE_GTM_RESET_XMIN,
    STATE_GTM_GET_XMIN,
    STATE_GTM_GET_GXID,
    STATE_GTM_GET_CSN,
    STATE_GTM_GET_SNAPSHOT,
    STATE_GTM_BEGIN_TRANS,
    STATE_GTM_COMMIT_TRANS,
    STATE_GTM_ROLLBACK_TRANS,
    STATE_GTM_START_PREPARE_TRANS,
    STATE_GTM_PREPARE_TRANS,
    STATE_GTM_OPEN_SEQUENCE,
    STATE_GTM_CLOSE_SEQUENCE,
    STATE_GTM_CREATE_SEQUENCE,
    STATE_GTM_ALTER_SEQUENCE,
    STATE_GTM_SEQUNCE_GET_NEXT_VAL,
    STATE_GTM_SEQUENCE_SET_VAL,
    STATE_GTM_DROP_SEQUENCE,
    STATE_GTM_RENAME_SEQUENCE,
    STATE_GTM_SET_DISASTER_CLUSTER,
    STATE_GTM_GET_DISASTER_CLUSTER,
    STATE_GTM_DEL_DISASTER_CLUSTER,
    STATE_WAIT_SYNC_CONSUMER_NEXT_STEP,
    STATE_WAIT_SYNC_PRODUCER_NEXT_STEP,
    STATE_GTM_SET_CONSISTENCY_POINT,
    STATE_WAIT_SYNC_BGWORKERS,
    STATE_STANDBY_READ_RECOVERY_CONFLICT,
    STATE_STANDBY_GET_SNAPSHOT,
    STATE_WAIT_NUM  // MUST be last, DO NOT use this value.
} WaitState;

/* ----------
 * Backend phase for "wait node" status
 * NOTE: if you add a WaitStatePhase enum value, remember to add it's description in WaitStatePhaseDesc.
 * ----------
 */
typedef enum WaitStatePhase {
    PHASE_NONE = 0,
    PHASE_BEGIN,
    PHASE_COMMIT,
    PHASE_ROLLBACK,
    PHASE_WAIT_QUOTA,
    PHASE_AUTOVACUUM
} WaitStatePhase;

/* ----------
 * Wait Event Classes
 * ----------
 */
#define WAIT_EVENT_END 0x00000000U
#define PG_WAIT_LWLOCK 0x01000000U
#define PG_WAIT_LOCK 0x03000000U
#define PG_WAIT_IO 0x0A000000U
#define PG_WAIT_SQL 0x0B000000U
#define PG_WAIT_STATE 0x0C000000U
#define PG_WAIT_DMS 0x0D000000U

/* ----------
 * Wait Events - IO
 *
 * Use this category when a process is waiting for a IO.
 * ----------
 */
typedef enum WaitEventIO {
    WAIT_EVENT_BUFFILE_READ = PG_WAIT_IO,
    WAIT_EVENT_BUFFILE_WRITE,
    WAIT_EVENT_BUF_HASH_SEARCH,
    WAIT_EVENT_BUF_STRATEGY_GET,
    WAIT_EVENT_CONTROL_FILE_READ,
    WAIT_EVENT_CONTROL_FILE_SYNC,
    WAIT_EVENT_CONTROL_FILE_SYNC_UPDATE,
    WAIT_EVENT_CONTROL_FILE_WRITE,
    WAIT_EVENT_CONTROL_FILE_WRITE_UPDATE,
    WAIT_EVENT_COPY_FILE_READ,
    WAIT_EVENT_COPY_FILE_WRITE,
    WAIT_EVENT_DATA_FILE_EXTEND,
    WAIT_EVENT_DATA_FILE_IMMEDIATE_SYNC,
    WAIT_EVENT_DATA_FILE_PREFETCH,
    WAIT_EVENT_DATA_FILE_READ,
    WAIT_EVENT_DATA_FILE_SYNC,
    WAIT_EVENT_DATA_FILE_TRUNCATE,
    WAIT_EVENT_DATA_FILE_WRITE,
    WAIT_EVENT_LOCK_FILE_ADDTODATADIR_READ,
    WAIT_EVENT_LOCK_FILE_ADDTODATADIR_SYNC,
    WAIT_EVENT_LOCK_FILE_ADDTODATADIR_WRITE,
    WAIT_EVENT_LOCK_FILE_CREATE_READ,
    WAIT_EVENT_LOCK_FILE_CREATE_SYNC,
    WAIT_EVENT_LOCK_FILE_CREATE_WRITE,
    WAIT_EVENT_RELATION_MAP_READ,
    WAIT_EVENT_RELATION_MAP_SYNC,
    WAIT_EVENT_RELATION_MAP_WRITE,
    WAIT_EVENT_REPLICATION_SLOT_READ,
    WAIT_EVENT_REPLICATION_SLOT_RESTORE_SYNC,
    WAIT_EVENT_REPLICATION_SLOT_SYNC,
    WAIT_EVENT_REPLICATION_SLOT_WRITE,
    WAIT_EVENT_SLRU_FLUSH_SYNC,
    WAIT_EVENT_SLRU_READ,
    WAIT_EVENT_SLRU_SYNC,
    WAIT_EVENT_SLRU_WRITE,
    WAIT_EVENT_TWOPHASE_FILE_READ,
    WAIT_EVENT_TWOPHASE_FILE_SYNC,
    WAIT_EVENT_TWOPHASE_FILE_WRITE,
    WAIT_EVENT_UNDO_FILE_EXTEND,
    WAIT_EVENT_UNDO_FILE_PREFETCH,
    WAIT_EVENT_UNDO_FILE_READ,
    WAIT_EVENT_UNDO_FILE_WRITE,
    WAIT_EVENT_UNDO_FILE_SYNC,
    WAIT_EVENT_UNDO_FILE_UNLINK,
    WAIT_EVENT_UNDO_META_SYNC,
    WAIT_EVENT_WAL_BOOTSTRAP_SYNC,
    WAIT_EVENT_WAL_BOOTSTRAP_WRITE,
    WAIT_EVENT_WAL_COPY_READ,
    WAIT_EVENT_WAL_COPY_SYNC,
    WAIT_EVENT_WAL_COPY_WRITE,
    WAIT_EVENT_WAL_INIT_SYNC,
    WAIT_EVENT_WAL_INIT_WRITE,
    WAIT_EVENT_WAL_READ,
    WAIT_EVENT_WAL_SYNC_METHOD_ASSIGN,
    WAIT_EVENT_WAL_WRITE,
    WAIT_EVENT_WAL_BUFFER_ACCESS,
    WAIT_EVENT_WAL_BUFFER_FULL,
    WAIT_EVENT_DW_READ,
    WAIT_EVENT_DW_WRITE,
    WAIT_EVENT_DW_SINGLE_POS,
    WAIT_EVENT_DW_SINGLE_WRITE,
    WAIT_EVENT_PREDO_PROCESS_PENDING,
    WAIT_EVENT_PREDO_APPLY,
    WAIT_EVENT_DISABLE_CONNECT_FILE_READ,
    WAIT_EVENT_DISABLE_CONNECT_FILE_SYNC,
    WAIT_EVENT_DISABLE_CONNECT_FILE_WRITE,
    WAIT_EVENT_MPFL_INIT,
    WAIT_EVENT_MPFL_READ,
    WAIT_EVENT_MPFL_WRITE,
    WAIT_EVENT_OBS_LIST,
    WAIT_EVENT_OBS_READ,
    WAIT_EVENT_OBS_WRITE,
    WAIT_EVENT_LOGCTRL_SLEEP,
    WAIT_EVENT_COMPRESS_ADDRESS_FILE_FLUSH,
    WAIT_EVENT_COMPRESS_ADDRESS_FILE_SYNC,
    WAIT_EVENT_LOGICAL_SYNC_DATA,
    WAIT_EVENT_LOGICAL_SYNC_STATE_CHANGE,
    WAIT_EVENT_REPLICATION_ORIGIN_DROP,
    WAIT_EVENT_REPLICATION_SLOT_DROP,
    IO_EVENT_NUM = WAIT_EVENT_LOGCTRL_SLEEP - WAIT_EVENT_BUFFILE_READ + 1  // MUST be last, DO NOT use this value.
} WaitEventIO;

typedef enum WaitEventDMS {
    WAIT_EVENT_IDLE_WAIT = PG_WAIT_DMS,

    WAIT_EVENT_GC_BUFFER_BUSY,
    WAIT_EVENT_DCS_REQ_MASTER4PAGE_1WAY,
    WAIT_EVENT_DCS_REQ_MASTER4PAGE_2WAY,
    WAIT_EVENT_DCS_REQ_MASTER4PAGE_3WAY,
    WAIT_EVENT_DCS_REQ_MASTER4PAGE_TRY,
    WAIT_EVENT_DCS_REQ_OWNER4PAGE,
    WAIT_EVENT_DCS_CLAIM_OWNER,
    WAIT_EVENT_DCS_RELEASE_OWNER,
    WAIT_EVENT_DCS_INVLDT_SHARE_COPY_REQ,
    WAIT_EVENT_DCS_INVLDT_SHARE_COPY_PROCESS,
    WAIT_EVENT_DCS_TRANSFER_PAGE_LATCH,
    WAIT_EVENT_DCS_TRANSFER_PAGE_READONLY2X,
    WAIT_EVENT_DCS_TRANSFER_PAGE_FLUSHLOG,
    WAIT_EVENT_DCS_TRANSFER_PAGE,
    WAIT_EVENT_PCR_REQ_BTREE_PAGE,
    WAIT_EVENT_PCR_REQ_HEAP_PAGE,
    WAIT_EVENT_PCR_REQ_MASTER,
    WAIT_EVENT_PCR_REQ_OWNER,
    WAIT_EVENT_PCR_CHECK_CURR_VISIBLE,
    WAIT_EVENT_TXN_REQ_INFO,
    WAIT_EVENT_TXN_REQ_SNAPSHOT,
    WAIT_EVENT_DLS_REQ_LOCK,
    WAIT_EVENT_DLS_REQ_TABLE,
    WAIT_EVENT_DLS_REQ_PART_X,
    WAIT_EVENT_DLS_REQ_PART_S,
    WAIT_EVENT_DLS_WAIT_TXN,
    WAIT_EVENT_DEAD_LOCK_TXN,
    WAIT_EVENT_DEAD_LOCK_TABLE,
    WAIT_EVENT_DEAD_LOCK_ITL,
    WAIT_EVENT_BROADCAST_BTREE_SPLIT,
    WAIT_EVENT_BROADCAST_ROOT_PAGE,
    WAIT_EVENT_QUERY_OWNER_ID,
    WAIT_EVENT_LATCH_X,
    WAIT_EVENT_LATCH_S,
    WAIT_EVENT_LATCH_X_REMOTE,
    WAIT_EVENT_LATCH_S_REMOTE,
    WAIT_EVENT_ONDEMAND_REDO,
    WAIT_EVENT_PAGE_STATUS_INFO,
    WAIT_EVENT_OPENGAUSS_SEND_XMIN,
    WAIT_EVENT_DCS_REQ_CREATE_XA_RES,
    WAIT_EVENT_DCS_REQ_DELETE_XA_RES,
    WAIT_EVENT_DCS_REQ_XA_OWNER_ID,
    WAIT_EVENT_DCS_REQ_XA_IN_USE,
    WAIT_EVENT_DCS_REQ_END_XA,
    DMS_EVENT_NUM = WAIT_EVENT_DCS_REQ_END_XA - WAIT_EVENT_IDLE_WAIT + 1  // MUST be last, DO NOT use this value.
} WaitEventDMS;

/* ----------
 * Wait Events - SQL
 *
 * Using this to indicate the type of  SQL DML event.
 * ----------
 */
typedef enum WaitEventSQL {
    WAIT_EVENT_SQL_SELECT = PG_WAIT_SQL,
    WAIT_EVENT_SQL_UPDATE,
    WAIT_EVENT_SQL_INSERT,
    WAIT_EVENT_SQL_DELETE,
    WAIT_EVENT_SQL_MERGEINTO,
    WAIT_EVENT_SQL_DDL,
    WAIT_EVENT_SQL_DML,
    WAIT_EVENT_SQL_DCL,
    WAIT_EVENT_SQL_TCL
} WaitEventSQL;

/* ----------
 * WAIT_COUNT_ARRAY_SIZE      Size of the array used for user`s sql count
 * ----------
 */
#define WAIT_COUNT_ARRAY_SIZE 128

typedef struct {
    uint64 total_time; /* total time for sql */
    uint64 min_time;   /* min time for sql */
    uint64 max_time;   /* max time for sql */
} ElapseTime;
/* ----------
 * PgStat_WaitCount		      The sql count result used for QPS
 * ----------
 */
typedef struct PgStat_WaitCount {
    uint64 wc_sql_select;
    uint64 wc_sql_update;
    uint64 wc_sql_insert;
    uint64 wc_sql_delete;
    uint64 wc_sql_mergeinto;
    uint64 wc_sql_ddl;
    uint64 wc_sql_dml;
    uint64 wc_sql_dcl;
    uint64 wc_sql_tcl;
    ElapseTime insertElapse;
    ElapseTime updateElapse;
    ElapseTime selectElapse;
    ElapseTime deleteElapse;
} PgStat_WaitCount;

/* ----------
 * PgStat_WaitCountStatus		The sql count result for per user
 * ----------
 */
typedef struct PgStat_WaitCountStatus {
    PgStat_WaitCount wc_cnt;
    uint32 userid;
} PgStat_WaitCountStatus;

/* ----------
 * PgStat_WaitCountStatusCell    The data cell of WaitCountStatusList
 * ----------
 */
typedef struct PgStat_WaitCountStatusCell {
    PgStat_WaitCountStatus WaitCountArray[WAIT_COUNT_ARRAY_SIZE];
} PgStat_WaitCountStatusCell;

/* ----------
 * WaitCountHashValue                  The value when find WaitCountHashTbl
 * ----------
 */
typedef struct WaitCountHashValue {
    Oid userid;
    int idx;
} WaitCountHashValue;

/* ----------
 * The data type used for performance monitor.
 * ----------
 */
typedef enum WorkloadManagerIOState {
    IOSTATE_NONE = 0,
    IOSTATE_READ,
    IOSTATE_WRITE,
    IOSTATE_VACUUM
} WorkloadManagerIOState;

typedef enum WorkloadManagerStmtTag { STMTTAG_NONE = 0, STMTTAG_READ, STMTTAG_WRITE } WorkloadManagerStmtTag;

/* ----------
 * Workload manager states
 * ----------
 */
typedef enum WorkloadManagerEnqueueState {
    STATE_NO_ENQUEUE,
    STATE_MEMORY,
    STATE_ACTIVE_STATEMENTS,
} WorkloadManagerEnqueueState;

typedef struct RemoteInfo {
    char remote_name[NAMEDATALEN];
    char remote_ip[MAX_IP_STR_LEN];
    char remote_port[MAX_PORT_LEN];
    int socket;
    int logic_id;
} RemoteInfo;

/* ----------
 * Shared-memory data structures
 * ----------
 */

/* Reserve 2 additional 3rd plugin lwlocks.*/
#define LWLOCK_EVENT_NUM (LWTRANCHE_NATIVE_TRANCHE_NUM + 2)
typedef struct WaitStatisticsInfo {
    int64 max_duration;
    int64 min_duration;
    int64 total_duration;
    int64 avg_duration;
    uint64 counter;
    uint64 failed_counter;
    TimestampTz last_updated;
} WaitStatisticsInfo;

typedef struct WaitStatusInfo {
    int64 start_time;  // current wait starttime
    WaitStatisticsInfo statistics_info[STATE_WAIT_NUM + 1];
} WaitStatusInfo;

typedef struct WaitEventInfo {
    int64 start_time;  // current wait starttime
    int64 duration;    // current wait duration
    WaitStatisticsInfo io_info[IO_EVENT_NUM];
    WaitStatisticsInfo dms_info[DMS_EVENT_NUM];
    WaitStatisticsInfo lock_info[LOCK_EVENT_NUM];
    WaitStatisticsInfo lwlock_info[LWLOCK_EVENT_NUM];
} WaitEventInfo;

typedef struct WaitInfo {
    WaitEventInfo event_info;
    WaitStatusInfo status_info;
} WaitInfo;

/* ----------
 * PgBackendStatus
 *
 * Each live backend maintains a PgBackendStatus struct in shared memory
 * showing its current activity.  (The structs are allocated according to
 * BackendId, but that is not critical.)  Note that the collector process
 * has no involvement in, or even access to, these structs.
 * ----------
 */
typedef struct PgBackendStatus {
    /*
     * To avoid locking overhead, we use the following protocol: a backend
     * increments st_changecount before modifying its entry, and again after
     * finishing a modification.  A would-be reader should note the value of
     * st_changecount, copy the entry into private memory, then check
     * st_changecount again.  If the value hasn't changed, and if it's even,
     * the copy is valid; otherwise start over.  This makes updates cheap
     * while reads are potentially expensive, but that's the tradeoff we want.
     *
     * The above protocol needs the memory barriers to ensure that
     * the apparent order of execution is as it desires. Otherwise,
     * for example, the CPU might rearrange the code so that st_changecount
     * is incremented twice before the modification on a machine with
     * weak memory ordering. This surprising result can lead to bugs.
     */
    int st_changecount;

    /* The entry is valid iff st_procpid > 0, unused if st_procpid == 0 */
    ThreadId st_procpid;
    /* The entry is valid when one session is coupled to a thread pool worker */
    uint64 st_sessionid;
    GlobalSessionId globalSessionId;

    /* Times when current backend, transaction, and activity started */
    TimestampTz st_proc_start_timestamp;
    TimestampTz st_xact_start_timestamp;
    TimestampTz st_activity_start_timestamp;
    TimestampTz st_state_start_timestamp;

    /* Database OID, owning user's OID, connection client address */
    Oid st_databaseid;
    Oid st_userid;
    SockAddr st_clientaddr;
    char* st_clienthostname; /* MUST be null-terminated */

    void* st_connect_info; /* pool connection state */

    /* current state */
    BackendState st_state;

    /* application name; MUST be null-terminated */
    char* st_appname;

    /* connection info;  */
    char* st_conninfo;

    /* current command string; MUST be null-terminated */
    char* st_activity;

    /* which resource waiting on? */
    WorkloadManagerEnqueueState st_waiting_on_resource;

    /* workload info */
    TimestampTz st_block_start_time;    /* block start time */
    TimestampTz st_elapsed_start_time;  /* elapsed start time */
    WLMStatistics st_backstat;          /* workload backend state */
    void* st_debug_info;                /* workload debug info */
    char* st_cgname;                    /* workload cgroup name */
    WorkloadManagerIOState st_io_state; /* 0: none 1: read: 2: write */
    WorkloadManagerStmtTag st_stmttag;  /* 0: none 1: read: 2: write */

    uint64 st_queryid;                  /* debug query id of current query */
    UniqueSQLKey st_unique_sql_key;     /* get unique sql key */
    pid_t st_tid;                       /* thread ID */
    uint64 st_parent_sessionid;         /* parent session ID, equals parent pid under non thread pool mode */
    int st_thread_level;                /* thread level, mark with plan node id of Stream node */
    uint32 st_smpid;                    /* smp worker id, used for parallel execution */
    WaitState st_waitstatus;            /* backend waiting states */
    int st_waitnode_count;              /* count of waiting nodes */
    int st_nodeid;                      /* maybe for nodeoid/nodeidx */
    int st_plannodeid;                  /* indentify which consumer is receiving data for SCTP */
    int st_numnodes;                    /* nodes number when reporting waitstatus in case it changed */
    uint32 st_waitevent;                /* backend's wait event */
    int st_stmtmem;                     /* statment mem for query */
    uint64 st_xid;                      /* for transaction id, fit for 64-bit */
    WaitStatePhase st_waitstatus_phase; /* detailed phase for wait status, now only for 'wait node' status */
    char* st_relname;                   /* relation name, for analyze, vacuum, .etc.*/
    Oid st_libpq_wait_nodeid;           /* for libpq, point to libpq_wait_node*/
    int st_libpq_wait_nodecount;        /* for libpq, point to libpq_wait_nodecount*/
    uint32 st_tempid;                   /* tempid for temp table */
    uint32 st_timelineid;               /* timeline id for temp table */
    int4 st_jobid;                      /* job work id */

    /* Latest connected GTM host index and time line */
    GtmHostIndex st_gtmhost;
    GTM_Timeline st_gtmtimeline;
    slock_t use_mutex; /* protect above variables */

    /* lwlock deadlock check */
    /* +1 before waiting; +1 after holding */
    int lw_count;
    /* lwlock object now requiring */
    LWLock* lw_want_lock;

    /* all lwlocks held by this thread */
    int* lw_held_num;                      /* point to num_held_lwlocks */
    void* lw_held_locks;                   /* point to held_lwlocks[] */
    volatile bool st_lw_access_flag;       /* valid flag */
    volatile bool st_lw_is_cleanning_flag; /* is cleanning lw ptr */

    RemoteInfo remote_info;
    WaitInfo waitInfo;
    LOCALLOCKTAG locallocktag; /* locked object */
    /* The entry is valid if st_block_sessionid > 0, unused if st_block_sessionid == 0 */

    volatile uint64 st_block_sessionid; /* block session */
    syscalllock statement_cxt_lock;     /* mutex for statement context(between session and statement flush thread) */
    void* statement_cxt;                /* statement context of full sql */
    knl_u_trace_context trace_cxt;      /* request trace id */

    HTAB* my_prepared_queries;
    pthread_mutex_t* my_pstmt_htbl_lock;
} PgBackendStatus;

typedef struct PgBackendStatusNode {
    PgBackendStatus* data;
    NameData database_name;
    PgBackendStatusNode* next;
} PgBackendStatusNode;

typedef struct ThreadWaitStatusInfo {
    ParallelFunctionState* state;
    TupleTableSlot* slot;
} ThreadWaitStatusInfo;

typedef struct CommInfoParallel {
    ParallelFunctionState* state;
    TupleTableSlot* slot;
} CommInfoParallel;

extern CommInfoParallel* getGlobalCommStatus(TupleDesc tuple_desc, const char* queryString);
extern ThreadWaitStatusInfo* getGlobalThreadWaitStatus(TupleDesc tuple_desc);

extern PgBackendStatus* PgBackendStatusArray;

/*
 * Macros to load and store st_changecount with the memory barriers.
 *
 * pgstat_increment_changecount_before() and
 * pgstat_increment_changecount_after() need to be called before and after
 * PgBackendStatus entries are modified, respectively. This makes sure that
 * st_changecount is incremented around the modification.
 *
 * Also pgstat_save_changecount_before() and pgstat_save_changecount_after()
 * need to be called before and after PgBackendStatus entries are copied into
 * private memory, respectively.
 */
#define pgstat_increment_changecount_before(beentry) \
    do {                                             \
        beentry->st_changecount++;                   \
        pg_write_barrier();                          \
    } while (0)

#define pgstat_increment_changecount_after(beentry) \
    do {                                            \
        pg_write_barrier();                         \
        beentry->st_changecount++;                  \
        Assert((beentry->st_changecount & 1) == 0); \
    } while (0)

#define pgstat_save_changecount_before(beentry, save_changecount) \
    do {                                                          \
        save_changecount = beentry->st_changecount;               \
        pg_read_barrier();                                        \
    } while (0)

#define pgstat_save_changecount_after(beentry, save_changecount) \
    do {                                                         \
        pg_read_barrier();                                       \
        save_changecount = beentry->st_changecount;              \
    } while (0)

extern char* getThreadWaitStatusDesc(PgBackendStatus* beentry);
extern const char* pgstat_get_waitstatusdesc(uint32 wait_event_info);
extern const char* pgstat_get_waitstatusname(uint32 wait_event_info);
extern const char* PgstatGetWaitstatephasename(uint32 waitPhaseInfo);

/*
 * Working state needed to accumulate per-function-call timing statistics.
 */
typedef struct PgStat_FunctionCallUsage {
    /* Link to function's hashtable entry (must still be there at exit!) */
    /* NULL means we are not tracking the current function call */
    PgStat_FunctionCounts* fs;
    /* Total time previously charged to function, as of function start */
    instr_time save_f_total_time;
    /* Backend-wide total time as of function start */
    instr_time save_total;
    /* system clock as of function start */
    instr_time f_start;
} PgStat_FunctionCallUsage;

extern THR_LOCAL volatile Oid* libpq_wait_nodeid;
extern THR_LOCAL volatile int* libpq_wait_nodecount;

/* ----------
 * Functions called from postmaster
 * ----------
 */
extern Size BackendStatusShmemSize(void);
extern void CreateSharedBackendStatus(void);

extern void pgstat_init(void);
extern ThreadId pgstat_start(void);
extern void pgstat_reset_all(void);
extern void allow_immediate_pgstat_restart(void);
extern void PgstatCollectorMain();

/* ----------
 * Functions called from backends
 * ----------
 */
const int NUM_PERCENTILE = 2;
extern void pgstat_ping(void);
extern void UpdateWaitStatusStat(volatile WaitInfo* InstrWaitInfo, uint32 waitstatus,
    int64 duration, TimestampTz current_time);
extern void UpdateWaitEventStat(WaitInfo* InstrWaitInfo, uint32 wait_event_info,
    int64 duration, TimestampTz current_time);
extern void UpdateWaitEventFaildStat(volatile WaitInfo* InstrWaitInfo, uint32 wait_event_info);
extern void CollectWaitInfo(WaitInfo* gsInstrWaitInfo, WaitStatusInfo status_info, WaitEventInfo event_info);
extern void InstrWaitEventInitLastUpdated(PgBackendStatus* current_entry, TimestampTz current_time);
extern void pgstat_report_stat(bool force);
extern void pgstat_vacuum_stat(void);
extern void pgstat_drop_database(Oid databaseid);

extern void pgstat_clear_snapshot(void);
extern void pgstat_reset_counters(void);
extern void pgstat_reset_shared_counters(const char*);
extern void pgstat_reset_single_counter(Oid p_objoid, Oid objectid, PgStat_Single_Reset_Type type);

extern void pgstat_report_autovac(Oid dboid);
extern void pgstat_report_autovac_timeout(Oid tableoid, uint32 statFlag, bool shared);
extern void pgstat_report_vacuum(Oid tableoid, uint32 statFlag, bool shared, PgStat_Counter tuples);
extern void pgstat_report_truncate(Oid tableoid, uint32 statFlag, bool shared);
extern void pgstat_report_data_changed(Oid tableoid, uint32 statFlag, bool shared);
void PgstatReportPrunestat(Oid tableoid, uint32 statFlag,
                            bool shared, PgStat_Counter scanned,
                            PgStat_Counter pruned);
extern void pgstat_report_sql_rt(uint64 UniqueSQLId, int64 start_time, int64 rt);
extern void pgstat_report_analyze(Relation rel, PgStat_Counter livetuples, PgStat_Counter deadtuples);

extern void pgstat_report_recovery_conflict(int reason);
extern void pgstat_report_deadlock(void);
const char* remote_conn_type_string(int remote_conn_type);
extern void pgstat_initialize(void);
extern void pgstat_bestart(void);
extern void pgstat_initialize_session(void);
extern void pgstat_deinitialize_session(void);
extern void pgstat_couple_decouple_session(bool is_couple);
extern void pgstat_beshutdown_session(int ctrl_index);

extern const char* pgstat_get_wait_io(WaitEventIO w);
extern const char* pgstat_get_wait_dms(WaitEventDMS w);
extern void pgstat_report_activity(BackendState state, const char* cmd_str);
extern void pgstat_report_tempfile(size_t filesize);
extern void pgstat_report_memReserved(int4 memReserved, int reserve_or_release);
extern void pgstat_report_statement_wlm_status();
extern void pgstat_refresh_statement_wlm_time(volatile PgBackendStatus* beentry);
extern void pgstat_report_wait_count(uint32 wait_event_info);
extern void pgstat_report_appname(const char* appname);
extern void pgstat_report_conninfo(const char* conninfo);
extern void pgstat_report_xact_timestamp(TimestampTz tstamp);
extern void pgstat_report_waiting_on_resource(WorkloadManagerEnqueueState waiting);
extern void pgstat_report_queryid(uint64 queryid);
extern void pgstat_report_unique_sql_id(bool resetUniqueSql);
extern void pgstat_report_global_session_id(GlobalSessionId globalSessionId);
extern void pgstat_report_jobid(uint64 jobid);
extern void pgstat_report_parent_sessionid(uint64 sessionid, uint32 level = 0);
extern void pgstat_report_bgworker_parent_sessionid(uint64 sessionid);
extern void pgstat_report_smpid(uint32 smpid);

extern void pgstat_report_blocksid(void* waitLockThrd, uint64 blockSessionId);

extern void pgstat_report_trace_id(knl_u_trace_context *trace_cxt, bool is_report_trace_id = false);
extern bool pgstat_get_waitlock(uint32 wait_event_info);
extern const char* pgstat_get_wait_event(uint32 wait_event_info);
extern const char* pgstat_get_backend_current_activity(ThreadId pid, bool checkUser);
extern const char* pgstat_get_crashed_backend_activity(ThreadId pid, char* buffer, int buflen);

extern PgStat_TableStatus* find_tabstat_entry(Oid rel_id, uint32 statFlag);
extern PgStat_BackendFunctionEntry* find_funcstat_entry(Oid func_id);
extern void pgstat_initstats(Relation rel);

extern void pgstat_report_connected_gtm_host(GtmHostIndex gtm_host);
extern void pgstat_report_connected_gtm_timeline(GTM_Timeline gtm_timeline);
extern void pgstat_cancel_invalid_gtm_conn(void);
extern void pgstat_reply_percentile_record_count();
extern void pgstat_reply_percentile_record();
extern int pgstat_fetch_sql_rt_info_counter();
extern void pgstat_fetch_sql_rt_info_internal(SqlRTInfo* sqlrt);
extern void processCalculatePercentile(void);
void pgstat_update_responstime_singlenode(uint64 UniqueSQLId, int64 start_time, int64 rt);
void pgstate_update_percentile_responsetime(void);

#define IS_PGSTATE_TRACK_UNDEFINE \
    (!u_sess->attr.attr_common.pgstat_track_activities || !t_thrd.shemem_ptr_cxt.MyBEEntry)

/*
 * Simple way, only updates wait status and return the last wait status
 * Note. when isOnlyFetch is flaged true, only fetch last waitstatus.
 */
static inline WaitState pgstat_report_waitstatus(WaitState waitstatus, bool isOnlyFetch = false)
{
    volatile PgBackendStatus* beentry = t_thrd.shemem_ptr_cxt.MyBEEntry;
    WaitState oldwaitstatus;

    if (IS_PGSTATE_TRACK_UNDEFINE)
        return STATE_WAIT_UNDEFINED;

    WaitState oldStatus = beentry->st_waitstatus;

    if (isOnlyFetch)
        return oldStatus;

    pgstat_increment_changecount_before(beentry);
    /*
     * Since this is a single-byte field in a struct that only this process
     * may modify, there seems no need to bother with the st_changecount
     * protocol.  The update must appear atomic in any case.
     */
    oldwaitstatus = beentry->st_waitstatus;
    beentry->st_waitstatus = waitstatus;
    if (t_thrd.role == THREADPOOL_WORKER) {
        t_thrd.threadpool_cxt.worker->m_waitState = waitstatus;
    }

    /* If it switches into STATE_POOLER_CREATE_CONN, point to global thread local parameters. */
    if (STATE_POOLER_CREATE_CONN == waitstatus) {
        libpq_wait_nodeid = &(beentry->st_libpq_wait_nodeid);
        libpq_wait_nodecount = &(beentry->st_libpq_wait_nodecount);
    }

    /* If it is restored to STATE_WAIT_UNDEFINED, restore the related parameters. */
    if (STATE_WAIT_UNDEFINED == waitstatus) {
        beentry->st_xid = 0;
        beentry->st_nodeid = -1;
        beentry->st_waitnode_count = 0;
        beentry->st_plannodeid = -1;
        beentry->st_numnodes = -1;
        beentry->st_relname[0] = '\0';
        beentry->st_relname[NAMEDATALEN * 2 - 1] = '\0';
        beentry->st_libpq_wait_nodecount = 0;
        beentry->st_libpq_wait_nodeid = InvalidOid;
    }

    if (u_sess->attr.attr_common.enable_instr_track_wait && (int)waitstatus != (int)STATE_WAIT_UNDEFINED) {
        beentry->waitInfo.status_info.start_time = GetCurrentTimestamp();
    } else if (u_sess->attr.attr_common.enable_instr_track_wait &&
               (uint32)oldwaitstatus != (uint32)STATE_WAIT_UNDEFINED && waitstatus == STATE_WAIT_UNDEFINED) {
        TimestampTz current_time =  GetCurrentTimestamp();
        int64 duration = current_time - beentry->waitInfo.status_info.start_time;
        UpdateWaitStatusStat(&beentry->waitInfo, (uint32)oldwaitstatus, duration, current_time);
        beentry->waitInfo.status_info.start_time = 0;
    }

    pgstat_increment_changecount_after(beentry);

    return oldStatus;
}

/*
 * For 64-bit xid, report waitstatus and xid, then return the last wait status.
 * Note. when isOnlyFetch is flaged true, only fetch last waitstatus.
 */
static inline WaitState pgstat_report_waitstatus_xid(WaitState waitstatus, uint64 xid, bool isOnlyFetch = false)
{
    volatile PgBackendStatus* beentry = t_thrd.shemem_ptr_cxt.MyBEEntry;

    if (IS_PGSTATE_TRACK_UNDEFINE)
        return STATE_WAIT_UNDEFINED;
    WaitState oldStatus = beentry->st_waitstatus;

    if (isOnlyFetch)
        return oldStatus;

    if (u_sess->attr.attr_common.enable_instr_track_wait && (int)waitstatus != (int)STATE_WAIT_UNDEFINED)
        beentry->waitInfo.status_info.start_time = GetCurrentTimestamp();

    /*
     * Since this is a single-byte field in a struct that only this process
     * may modify, there seems no need to bother with the st_changecount
     * protocol.  The update must appear atomic in any case.
     */
    beentry->st_waitstatus = waitstatus;
    beentry->st_xid = xid;

    return oldStatus;
}

/*
 * For status related to relation, eg.vacuum, analyze, etc. report waitstatus and relname.
 * Then, return the last wait status.
 * Note. when isOnlyFetch is flaged true, only fetch last waitstatus.
 */
static inline WaitState pgstat_report_waitstatus_relname(WaitState waitstatus, char* relname, bool isOnlyFetch = false)
{
    volatile PgBackendStatus* beentry = t_thrd.shemem_ptr_cxt.MyBEEntry;
    int len = 0;

    if (IS_PGSTATE_TRACK_UNDEFINE)
        return STATE_WAIT_UNDEFINED;
    WaitState oldStatus = beentry->st_waitstatus;

    if (isOnlyFetch)
        return oldStatus;

    /* This should be unnecessary if GUC did its job, but be safe */
    if (relname != NULL) {
        len = pg_mbcliplen(relname, strlen(relname), NAMEDATALEN * 2 - 1);
    }

    if (u_sess->attr.attr_common.enable_instr_track_wait && (int)waitstatus != (int)STATE_WAIT_UNDEFINED)
        beentry->waitInfo.status_info.start_time = GetCurrentTimestamp();

    /*
     * Since this is a single-byte field in a struct that only this process
     * may modify, there seems no need to bother with the st_changecount
     * protocol.  The update must appear atomic in any case.
     */
    beentry->st_waitstatus = waitstatus;
    if (relname != NULL) {
        errno_t rc = memcpy_s((char*)beentry->st_relname, NAMEDATALEN * 2, relname, len);
        securec_check(rc, "\0", "\0");

        pfree(relname);
        relname = NULL;
    }
    beentry->st_relname[len] = '\0';

    return oldStatus;
}

/*
 * For wait status with wait node info, update node info and return last wait status.
 * Note. when isOnlyFetch is flaged true, only fetch last waitstatus.
 */
static inline WaitState pgstat_report_waitstatus_comm(WaitState waitstatus, int nodeId = -1, int waitnode_count = -1,
    int plannodeid = -1, int numnodes = -1, bool isOnlyFetch = false)
{
    volatile PgBackendStatus* beentry = t_thrd.shemem_ptr_cxt.MyBEEntry;

    if (IS_PGSTATE_TRACK_UNDEFINE)
        return STATE_WAIT_UNDEFINED;
    WaitState oldStatus = beentry->st_waitstatus;

    if (isOnlyFetch)
        return oldStatus;

    if (u_sess->attr.attr_common.enable_instr_track_wait && (int)waitstatus != (int)STATE_WAIT_UNDEFINED)
        beentry->waitInfo.status_info.start_time = GetCurrentTimestamp();

    /*
     * Since this is a single-byte field in a struct that only this process
     * may modify, there seems no need to bother with the st_changecount
     * protocol.  The update must appear atomic in any case.
     */
    beentry->st_waitstatus = waitstatus;
    beentry->st_nodeid = nodeId;
    beentry->st_waitnode_count = waitnode_count;
    beentry->st_plannodeid = plannodeid;
    beentry->st_numnodes = numnodes;

    return oldStatus;
}

/*
 * For wait status which needs to focus its phase, update phase info and return the last wait phase.
 * Note. when isOnlyFetch is flaged true, only fetch last phase.
 */
static inline WaitStatePhase pgstat_report_waitstatus_phase(WaitStatePhase waitstatus_phase, bool isOnlyFetch = false)
{
    volatile PgBackendStatus* beentry = t_thrd.shemem_ptr_cxt.MyBEEntry;

    if (IS_PGSTATE_TRACK_UNDEFINE)
        return PHASE_NONE;

    WaitStatePhase oldPhase = beentry->st_waitstatus_phase;

    if (isOnlyFetch)
        return oldPhase;

    /*
     * Since this is a single-byte field in a struct that only this process
     * may modify, there seems no need to bother with the st_changecount
     * protocol.  The update must appear atomic in any case.
     */
    beentry->st_waitstatus_phase = waitstatus_phase;
    return oldPhase;
}

static inline void pgstat_report_wait_lock_failed(uint32 wait_event_info)
{
    volatile PgBackendStatus* beentry = t_thrd.shemem_ptr_cxt.MyBEEntry;
    if (!u_sess->attr.attr_common.pgstat_track_activities || !u_sess->attr.attr_common.enable_instr_track_wait ||
        !beentry)
        return;
    pgstat_increment_changecount_before(beentry);
    uint32 old_wait_event_info = beentry->st_waitevent;
    UpdateWaitEventFaildStat(&beentry->waitInfo, old_wait_event_info);
    pgstat_increment_changecount_after(beentry);
}

/* ----------
 * pgstat_report_waitevent() -
 *
 *	Called from places where server process needs to wait.  This is called
 *	to report wait event information.  The wait information is stored
 *	as 4-bytes where first byte represents the wait event class (type of
 *	wait, for different types of wait, refer WaitClass) and the next
 *	3-bytes represent the actual wait event.  Currently 2-bytes are used
 *	for wait event which is sufficient for current usage, 1-byte is
 *	reserved for future usage.
 *
 * ----------
 */
static inline void pgstat_report_waitevent(uint32 wait_event_info)
{
    PgBackendStatus* beentry = t_thrd.shemem_ptr_cxt.MyBEEntry;

    if (IS_PGSTATE_TRACK_UNDEFINE)
        return;

    pgstat_increment_changecount_before(beentry);
    /*
     * Since this is a four-byte field which is always read and written as
     * four-bytes, updates are atomic.
     */
    uint32 old_wait_event_info = beentry->st_waitevent;
    beentry->st_waitevent = wait_event_info;

    if (u_sess->attr.attr_common.enable_instr_track_wait && wait_event_info != WAIT_EVENT_END) {
        beentry->waitInfo.event_info.start_time = GetCurrentTimestamp();
    } else if (u_sess->attr.attr_common.enable_instr_track_wait && old_wait_event_info != WAIT_EVENT_END &&
               wait_event_info == WAIT_EVENT_END) {
        TimestampTz current_time = GetCurrentTimestamp();
        int64 duration = current_time - beentry->waitInfo.event_info.start_time;
        UpdateWaitEventStat(&beentry->waitInfo, old_wait_event_info, duration, current_time);
        beentry->waitInfo.event_info.start_time = 0;
        beentry->waitInfo.event_info.duration = duration;
    }

    pgstat_increment_changecount_after(beentry);
}

static inline void pgstat_report_waitevent_count(uint32 wait_event_info)
{
    PgBackendStatus* beentry = t_thrd.shemem_ptr_cxt.MyBEEntry;

    if (IS_PGSTATE_TRACK_UNDEFINE)
        return;

    pgstat_increment_changecount_before(beentry);
    /*
     * Since this is a four-byte field which is always read and written as
     * four-bytes, updates are atomic.
     */
    if (u_sess->attr.attr_common.enable_instr_track_wait && wait_event_info != WAIT_EVENT_END) {
        beentry->st_waitevent = WAIT_EVENT_END;
        TimestampTz current_time = GetCurrentTimestamp();
        UpdateWaitEventStat(&beentry->waitInfo, wait_event_info, 0, current_time);
    }

    pgstat_increment_changecount_after(beentry);
}

static inline void pgstat_reset_waitStatePhase(WaitState waitstatus, WaitStatePhase waitstatus_phase)
{
    volatile PgBackendStatus* beentry = t_thrd.shemem_ptr_cxt.MyBEEntry;

    if (IS_PGSTATE_TRACK_UNDEFINE)
        return;

    beentry->st_waitstatus = waitstatus;
    beentry->st_waitstatus_phase = waitstatus_phase;

    beentry->st_xid = 0;
    beentry->st_nodeid = -1;
    beentry->st_waitnode_count = 0;
    beentry->st_plannodeid = -1;
    beentry->st_numnodes = -1;
    beentry->st_relname[0] = '\0';
    beentry->st_relname[NAMEDATALEN * 2 - 1] = '\0';
    beentry->st_libpq_wait_nodeid = InvalidOid;
    beentry->st_libpq_wait_nodecount = 0;
}

/* nontransactional event counts are simple enough to inline */
#define pgstat_count_heap_scan(rel)                    \
    do {                                               \
        if ((rel)->pgstat_info != NULL)                \
            (rel)->pgstat_info->t_counts.t_numscans++; \
        pgstatCountHeapScan4SessionLevel();            \
    } while (0)
#define pgstat_count_heap_getnext(rel)                        \
    do {                                                      \
        if ((rel)->pgstat_info != NULL)                       \
            (rel)->pgstat_info->t_counts.t_tuples_returned++; \
    } while (0)
#define pgstat_count_heap_fetch(rel)                         \
    do {                                                     \
        if ((rel)->pgstat_info != NULL)                      \
            (rel)->pgstat_info->t_counts.t_tuples_fetched++; \
    } while (0)
#define pgstat_count_index_scan(rel)                   \
    do {                                               \
        if ((rel)->pgstat_info != NULL)                \
            (rel)->pgstat_info->t_counts.t_numscans++; \
        pgstatCountIndexScan4SessionLevel();           \
    } while (0)
#define pgstat_count_index_tuples(rel, n)                          \
    do {                                                           \
        if ((rel)->pgstat_info != NULL)                            \
            (rel)->pgstat_info->t_counts.t_tuples_returned += (n); \
    } while (0)
#define pgstat_count_buffer_read(rel)                        \
    do {                                                     \
        if ((rel)->pgstat_info != NULL)                      \
            (rel)->pgstat_info->t_counts.t_blocks_fetched++; \
    } while (0)
#define pgstat_count_buffer_hit(rel)                     \
    do {                                                 \
        if ((rel)->pgstat_info != NULL)                  \
            (rel)->pgstat_info->t_counts.t_blocks_hit++; \
    } while (0)
#define pgstat_count_buffer_read_time(n) (u_sess->stat_cxt.pgStatBlockReadTime += (n))
#define pgstat_count_buffer_write_time(n) (u_sess->stat_cxt.pgStatBlockWriteTime += (n))

#define pgstat_count_cu_mem_hit(rel)                     \
    do {                                                 \
        if ((rel)->pgstat_info != NULL)                  \
            (rel)->pgstat_info->t_counts.t_cu_mem_hit++; \
    } while (0)
#define pgstat_count_cu_hdd_sync(rel)                     \
    do {                                                  \
        if ((rel)->pgstat_info != NULL)                   \
            (rel)->pgstat_info->t_counts.t_cu_hdd_sync++; \
    } while (0)
#define pgstat_count_cu_hdd_asyn(rel, n)                       \
    do {                                                       \
        if ((rel)->pgstat_info != NULL)                        \
            (rel)->pgstat_info->t_counts.t_cu_hdd_asyn += (n); \
    } while (0)

extern void pgstat_count_heap_insert(Relation rel, int n);
extern void pgstat_count_heap_update(Relation rel, bool hot);
extern void pgstat_count_heap_delete(Relation rel);
extern void PgstatCountHeapUpdateInplace(Relation rel);
extern void pgstat_count_truncate(Relation rel);
extern void PgstatCountHeapUpdateInplace(Relation rel);
extern void pgstat_update_heap_dead_tuples(Relation rel, int delta);

extern void pgstat_count_cu_update(Relation rel, int n);
extern void pgstat_count_cu_delete(Relation rel, int n);

#define pgstat_count_cu_insert(rel, n)    \
    do {                                  \
        pgstat_count_heap_insert(rel, n); \
    } while (0)

#define pgstat_count_dfs_insert(rel, n) \
    do {                                \
        pgstat_count_cu_insert(rel, n); \
    } while (0)
#define pgstat_count_dfs_update(rel, n) \
    do {                                \
        pgstat_count_cu_update(rel, n); \
    } while (0)
#define pgstat_count_dfs_delete(rel, n) \
    do {                                \
        pgstat_count_cu_delete(rel, n); \
    } while (0)

extern void pgstat_init_function_usage(FunctionCallInfoData* fcinfo, PgStat_FunctionCallUsage* fcu);
extern void pgstat_end_function_usage(PgStat_FunctionCallUsage* fcu, bool finalize);

extern void AtEOXact_PgStat(bool isCommit);
extern void AtEOSubXact_PgStat(bool isCommit, int nestDepth);

extern void AtPrepare_PgStat(void);
extern void PostPrepare_PgStat(void);

extern void pgstat_twophase_postcommit(TransactionId xid, uint16 info, void* recdata, uint32 len);
extern void pgstat_twophase_postabort(TransactionId xid, uint16 info, void* recdata, uint32 len);

extern void pgstat_send_bgwriter(void);

/* ----------
 * Support functions for the SQL-callable functions to
 * generate the pgstat* views.
 * ----------
 */
extern PgStat_StatDBEntry* pgstat_fetch_stat_dbentry(Oid dbid);
extern PgStat_StatTabEntry* pgstat_fetch_stat_tabentry(PgStat_StatTabKey* tabkey);
extern PgStat_StatFuncEntry* pgstat_fetch_stat_funcentry(Oid funcid);
extern PgStat_GlobalStats* pgstat_fetch_global(void);
extern PgStat_WaitCountStatus* pgstat_fetch_waitcount(void);

extern void pgstat_initstats_partition(Partition part);

typedef enum StatisticsLevel {
    STAT_LEVEL_OFF = 0,
    STAT_LEVEL_BASIC,
    STAT_LEVEL_TYPICAL,
    STAT_LEVEL_ALL
} StatisticsLevel;

/*  Values for stat_view --- order is significant! */
typedef enum STAT_VIEW {
    PV_CONFIG_PARAMETER = 0,
    PV_ACTIVITY,
    PV_LOCK,
    PV_SESSION_STAT,
    PV_DB_STAT,
    PV_INSTANCE_STAT,
    PV_STAT_NAME,
    PV_OS_RUN_INFO,
    PV_BASIC_LEVEL, /*Above are at basic level,defaut on*/

    PV_LIGHTWEIGHT_LOCK,
    PV_WAIT_TYPE,
    PV_SESSION_WAIT,
    PV_SESSION_WAIT_SUMMARY,
    PV_DB_WAIT_SUMMARY,
    PV_INSTANCE_WAIT_SUMMARY,
    PV_SESSION_MEMORY_INFO,
    PV_SESSION_TIME,
    PV_DB_TIME,
    PV_INSTANCE_TIME,
    PV_REDO_STAT,
    PV_TYPICAL_LEVEL, /*Above are at typical level,defaut off*/

    PV_STATEMENT,
    PV_FILE_STAT,
    PV_IOSTAT_NETWORK,
    PV_SHARE_MEMORY_INFO,
    PV_PLAN,
    PV_ALL_LEVEL /*Above are at all level,defaut off*/
} STAT_VIEW;

// static StatisticsLevel viewLevel[]={
//										STAT_LEVEL_BASIC,
//										STAT_LEVEL_BASIC,
//										STAT_LEVEL_BASIC,
//										STAT_LEVEL_BASIC,
//										STAT_LEVEL_BASIC,
//										STAT_LEVEL_BASIC,
//										STAT_LEVEL_BASIC,
//										STAT_LEVEL_BASIC,
//									      STAT_LEVEL_BASIC, /*Above are at basic level,defaut on*/
//
//										STAT_LEVEL_TYPICAL,
//										STAT_LEVEL_TYPICAL,
//										STAT_LEVEL_TYPICAL,
//										STAT_LEVEL_TYPICAL,
//										STAT_LEVEL_TYPICAL,
//										STAT_LEVEL_TYPICAL,
//										STAT_LEVEL_TYPICAL,
//										STAT_LEVEL_TYPICAL,
//										STAT_LEVEL_TYPICAL,
//										STAT_LEVEL_TYPICAL,
//										STAT_LEVEL_TYPICAL,
//										STAT_LEVEL_TYPICAL,/*Above are at typical level,defaut off*/
//
//										STAT_LEVEL_ALL,
//										STAT_LEVEL_ALL,
//										STAT_LEVEL_ALL,
//										STAT_LEVEL_ALL,
//										STAT_LEVEL_ALL,
//										STAT_LEVEL_ALL
//									};

typedef enum OSRunInfoTypes {
    /*cpu numbers*/
    NUM_CPUS = 0,
    NUM_CPU_CORES,
    NUM_CPU_SOCKETS,

    /*cpu times*/
    IDLE_TIME,
    BUSY_TIME,
    USER_TIME,
    SYS_TIME,
    IOWAIT_TIME,
    NICE_TIME,

    /*avg cpu times*/
    AVG_IDLE_TIME,
    AVG_BUSY_TIME,
    AVG_USER_TIME,
    AVG_SYS_TIME,
    AVG_IOWAIT_TIME,
    AVG_NICE_TIME,

    /*virtual memory page in/out data*/
    VM_PAGE_IN_BYTES,
    VM_PAGE_OUT_BYTES,

    /*os run load*/
    RUNLOAD,

    /*physical memory size*/
    PHYSICAL_MEMORY_BYTES,

    TOTAL_OS_RUN_INFO_TYPES
} OSRunInfoTypes;

/*
 *this is used to represent the numbers of cpu time we should read from file.BUSY_TIME will be
 *calculate by USER_TIME plus SYS_TIME,so it wouldn't be counted.
 */
#define NumOfCpuTimeReads (AVG_IDLE_TIME - IDLE_TIME - 1)

/*the type we restore our collected data. It is a union of all the possible data types of the os run info*/
typedef union NumericValue {
    uint64 int64Value;  /*cpu times,vm pgin/pgout size,total memory etc.*/
    float8 float8Value; /*load*/
    uint32 int32Value;  /*cpu numbers*/
} NumericValue;

/*
 *description of the os run info fields. For a particluar field, all the members except got
 *are fixed.
 */
typedef struct OSRunInfoDesc {
    /*hook to convert our data to Datum type, it decides by the data type the field*/
    Datum (*getDatum)(NumericValue data);

    char* name;      /*field name*/
    bool cumulative; /*represent whether the field is cumulative*/

    /*
     *it represent whether we successfully get data of this field. Because some fields may be subject to the
     *os platform on which the database is running, or not available in some exception cases. I don't think
     *it's a big deal, we just show the infomation we can get.
     */
    bool got;
    char* comments; /*field comments*/
} OSRunInfoDesc;

extern const OSRunInfoDesc osStatDescArrayOrg[TOTAL_OS_RUN_INFO_TYPES];

extern int64 getCpuTime(void);
extern int64 JiffiesToSec(uint64);
extern void getCpuNums(void);
extern void getCpuTimes(void);
extern void getVmStat(void);
extern void getTotalMem(void);
extern void getOSRunLoad(void);

extern Datum Int64GetNumberDatum(NumericValue value);
extern Datum Float8GetNumberDatum(NumericValue value);
extern Datum Int32GetNumberDatum(NumericValue value);

static inline ssize_t gs_getline(char** lineptr, size_t* n, FILE* stream)
{
    *lineptr = (char*)palloc0(4096);
    *n = 4096;
    return getline(lineptr, n, stream);
}

#define SESSION_ID_LEN 32
extern void getSessionID(char* sessid, pg_time_t startTime, ThreadId Threadid);
extern void getThrdID(char* thrdid, pg_time_t startTime, ThreadId Threadid);

#define NUM_MOT_SESSION_MEMORY_DETAIL_ELEM 4
#define NUM_MOT_JIT_DETAIL_ELEM 11
#define NUM_MOT_JIT_PROFILE_ELEM 12

typedef struct MotSessionMemoryDetail {
    ThreadId threadid;
    pg_time_t threadStartTime;
    int64 totalSize;
    int64 freeSize;
    int64 usedSize;
} MotSessionMemoryDetail;

typedef struct MotSessionMemoryDetailPad {
    uint32 nelements;
    MotSessionMemoryDetail* sessionMemoryDetail;
} MotSessionMemoryDetailPad;

typedef struct MotMemoryDetail {
    int64 numaNode;
    int64 reservedMemory;
    int64 usedMemory;
} MotMemoryDetail;

typedef struct MotMemoryDetailPad {
    uint32 nelements;
    MotMemoryDetail* memoryDetail;
} MotMemoryDetailPad;

typedef struct MotJitDetail {
    Oid procOid;
    char* query;
    char* nameSpace;
    char* jittableStatus;
    char* validStatus;
    TimestampTz lastUpdatedTimestamp;
    char* planType;
    int64 codegenTime;
    int64 verifyTime;
    int64 finalizeTime;
    int64 compileTime;
} MotJitDetail;

typedef struct MotJitProfile {
    Oid procOid;
    int32 id;
    int32 parentId;
    char* query;
    char* nameSpace;
    float4 weight;
    int64 totalTime;
    int64 selfTime;
    int64 childGrossTime;
    int64 childNetTime;
    int64 defVarsTime;
    int64 initVarsTime;
} MotJitProfile;

extern MotSessionMemoryDetail* GetMotSessionMemoryDetail(uint32* num);
extern MotMemoryDetail* GetMotMemoryDetail(uint32* num, bool isGlobal);
extern MotJitDetail* GetMotJitDetail(uint32* num);
extern MotJitProfile* GetMotJitProfile(uint32* num);

#ifdef MEMORY_CONTEXT_CHECKING
typedef enum { STANDARD_DUMP, SHARED_DUMP } DUMP_TYPE;

extern void DumpMemoryContext(DUMP_TYPE type);
#endif

extern void getThreadMemoryDetail(Tuplestorestate* tupStore, TupleDesc tupDesc, uint32* procIdx);
extern void getSharedMemoryDetail(Tuplestorestate* tupStore, TupleDesc tupDesc);

typedef struct SessionTimeEntry {
    /*
     *protect the rest part of the entry.
     */
    uint32 changeCount;

    bool isActive;

    uint64 sessionid;
    pg_time_t myStartTime;

    int64 array[TOTAL_TIME_INFO_TYPES];
} SessionTimeEntry;

/*
 *this macro is used to read a entry from global array to a local buffer. we use changeCount to
 *ensure data consistency.
 */
#define READ_AN_ENTRY(dest, src, changeCount, type)                           \
    do {                                                                      \
        for (;;) {                                                            \
            uint32 saveChangeCount = changeCount;                             \
            errno_t rc = 0;                                                   \
            rc = memcpy_s(dest, sizeof(type), src, sizeof(type));             \
            securec_check(rc, "\0", "\0");                                    \
            if ((saveChangeCount & 1) == 0 && saveChangeCount == changeCount) \
                break;                                                        \
            CHECK_FOR_INTERRUPTS();                                           \
        }                                                                     \
    } while (0)

#define SessionTimeArraySize (BackendStatusArray_size)

#define PGSTAT_INIT_TIME_RECORD() OgRecordOperator og_record_opt(false);

#define PGSTAT_START_TIME_RECORD()                    \
    do {                                              \
        if (t_thrd.shemem_ptr_cxt.mySessionTimeEntry) \
            og_record_opt.enter();                    \
    } while (0)

#define PGSTAT_END_TIME_RECORD(stage)                                                        \
    do {                                                                                     \
        if (t_thrd.shemem_ptr_cxt.mySessionTimeEntry)                                        \
            og_record_opt.exit(stage);                                                       \
    } while (0)

#define PGSTAT_START_PLSQL_TIME_RECORD()                                                    \
    do {                                                                                    \
        if (u_sess->stat_cxt.isTopLevelPlSql && t_thrd.shemem_ptr_cxt.mySessionTimeEntry) { \
            u_sess->stat_cxt.isTopLevelPlSql = false;                                       \
            needRecord = true;                                                              \
            og_record_opt.enter();                    \
        }                                                                                   \
    } while (0)

#define PGSTAT_END_PLSQL_TIME_RECORD(stage)                                                  \
    do {                                                                                     \
        if (needRecord == true && t_thrd.shemem_ptr_cxt.mySessionTimeEntry) {                \
            u_sess->stat_cxt.isTopLevelPlSql = true;                                         \
            og_record_opt.exit(stage);                    \
        }                                                                                    \
    } while (0)

extern Size sessionTimeShmemSize(void);
extern void sessionTimeShmemInit(void);

extern void timeInfoRecordStart(void);
extern void timeInfoRecordEnd(void);

extern void getSessionTimeStatus(Tuplestorestate *tupStore, TupleDesc tupDesc,
    void (*insert)(Tuplestorestate *tupStore, TupleDesc tupDesc, const SessionTimeEntry *entry));

extern SessionTimeEntry* getInstanceTimeStatus();

typedef struct PgStat_RedoEntry {
    PgStat_Counter writes;
    PgStat_Counter writeBlks;
    PgStat_Counter writeTime;
    PgStat_Counter avgIOTime;
    PgStat_Counter lstIOTime;
    PgStat_Counter minIOTime;
    PgStat_Counter maxIOTime;
} PgStat_RedoEntry;

extern PgStat_RedoEntry redoStatistics;
// extern LWLock* redoStatLock;

extern void reportRedoWrite(PgStat_Counter blks, PgStat_Counter tim);

typedef struct PgStat_FileEntry {
    int changeCount;
    TimestampTz time;

    Oid dbid;
    Oid spcid;
    Oid fn;

    PgStat_Counter reads;
    PgStat_Counter writes;
    PgStat_Counter readBlks;
    PgStat_Counter readTime;
    PgStat_Counter writeBlks;
    PgStat_Counter writeTime;
    PgStat_Counter avgIOTime;
    PgStat_Counter lstIOTime;
    PgStat_Counter minIOTime;
    PgStat_Counter maxIOTime;
} PgStat_FileEntry;

#define NUM_FILES 2000
#define STAT_MSG_BATCH 100  // reduce message frequence by count 100 times.

extern PgStat_FileEntry pgStatFileArray[NUM_FILES];
extern uint32 fileStatCount;
extern void reportFileStat(PgStat_MsgFile* msg);

typedef enum SessionStatisticType {
    N_COMMIT_SESSION_LEVEL = 0,
    N_ROLLBACK_SESSION_LEVEL,
    N_SQL_SESSION_LEVEL,

    N_TABLE_SCAN_SESSION_LEVEL,

    N_BLOCKS_FETCHED_SESSION_LEVEL,
    N_PHYSICAL_READ_OPERATION_SESSION_LEVEL, /*it is equal to N_BLOCKS_FETCHED_SESSION_LEVEL now*/
    N_SHARED_BLOCKS_DIRTIED_SESSION_LEVEL,
    N_LOCAL_BLOCKS_DIRTIED_SESSION_LEVEL,
    N_SHARED_BLOCKS_READ_SESSION_LEVEL,
    N_LOCAL_BLOCKS_READ_SESSION_LEVEL,
    T_BLOCKS_READ_TIME_SESSION_LEVEL,
    T_BLOCKS_WRITE_TIME_SESSION_LEVEL,

    N_SORT_IN_MEMORY_SESSION_LEVEL,
    N_SORT_IN_DISK_SESSION_LEVEL,

    N_CU_MEM_HIT,
    N_CU_HDD_SYNC_READ,
    N_CU_HDD_ASYN_READ,

    N_TOTAL_SESSION_STATISTICS_TYPES
} SessionStatisticType;

typedef struct SessionLevelStatistic {
    pg_time_t sessionStartTime;
    uint64 sessionid;
    bool isValid;

    PgStat_Counter array[N_TOTAL_SESSION_STATISTICS_TYPES];
} SessionLevelStatistic;

#define SessionStatArraySize (BackendStatusArray_size)

#define pgstatCountTransactionCommit4SessionLevel(isCommit)                                  \
    do {                                                                                     \
        if (NULL != t_thrd.shemem_ptr_cxt.mySessionStatEntry) {                              \
            if (isCommit) {                                                                  \
                t_thrd.shemem_ptr_cxt.mySessionStatEntry->array[N_COMMIT_SESSION_LEVEL]++;   \
            } else {                                                                         \
                t_thrd.shemem_ptr_cxt.mySessionStatEntry->array[N_ROLLBACK_SESSION_LEVEL]++; \
            }                                                                                \
        }                                                                                    \
    } while (0)

#define pgstatCountSQL4SessionLevel()                                               \
    do {                                                                            \
        if (NULL != t_thrd.shemem_ptr_cxt.mySessionStatEntry)                       \
            t_thrd.shemem_ptr_cxt.mySessionStatEntry->array[N_SQL_SESSION_LEVEL]++; \
    } while (0)

#define pgstatCountIndexScan4SessionLevel()                                                \
    do {                                                                                   \
        if (NULL != t_thrd.shemem_ptr_cxt.mySessionStatEntry)                              \
            t_thrd.shemem_ptr_cxt.mySessionStatEntry->array[N_TABLE_SCAN_SESSION_LEVEL]++; \
    } while (0)

#define pgstatCountHeapScan4SessionLevel()                                                 \
    do {                                                                                   \
        if (NULL != t_thrd.shemem_ptr_cxt.mySessionStatEntry)                              \
            t_thrd.shemem_ptr_cxt.mySessionStatEntry->array[N_TABLE_SCAN_SESSION_LEVEL]++; \
    } while (0)

#define pgstatCountBlocksFetched4SessionLevel()                                                         \
    do {                                                                                                \
        if (NULL != t_thrd.shemem_ptr_cxt.mySessionStatEntry) {                                         \
            t_thrd.shemem_ptr_cxt.mySessionStatEntry->array[N_BLOCKS_FETCHED_SESSION_LEVEL]++;          \
            t_thrd.shemem_ptr_cxt.mySessionStatEntry->array[N_PHYSICAL_READ_OPERATION_SESSION_LEVEL]++; \
        }                                                                                               \
    } while (0)

#define pgstatCountSharedBlocksDirtied4SessionLevel()                                                 \
    do {                                                                                              \
        if (NULL != t_thrd.shemem_ptr_cxt.mySessionStatEntry)                                         \
            t_thrd.shemem_ptr_cxt.mySessionStatEntry->array[N_SHARED_BLOCKS_DIRTIED_SESSION_LEVEL]++; \
    } while (0)

#define pgstatCountLocalBlocksDirtied4SessionLevel()                                                 \
    do {                                                                                             \
        if (NULL != t_thrd.shemem_ptr_cxt.mySessionStatEntry)                                        \
            t_thrd.shemem_ptr_cxt.mySessionStatEntry->array[N_LOCAL_BLOCKS_DIRTIED_SESSION_LEVEL]++; \
    } while (0)

#define pgstatCountSharedBlocksRead4SessionLevel()                                                 \
    do {                                                                                           \
        if (NULL != t_thrd.shemem_ptr_cxt.mySessionStatEntry)                                      \
            t_thrd.shemem_ptr_cxt.mySessionStatEntry->array[N_SHARED_BLOCKS_READ_SESSION_LEVEL]++; \
    } while (0)

#define pgstatCountLocalBlocksRead4SessionLevel()                                                 \
    do {                                                                                          \
        if (NULL != t_thrd.shemem_ptr_cxt.mySessionStatEntry)                                     \
            t_thrd.shemem_ptr_cxt.mySessionStatEntry->array[N_LOCAL_BLOCKS_READ_SESSION_LEVEL]++; \
    } while (0)

#define pgstatCountBlocksReadTime4SessionLevel(value)                                                   \
    do {                                                                                                \
        if (NULL != t_thrd.shemem_ptr_cxt.mySessionStatEntry)                                           \
            t_thrd.shemem_ptr_cxt.mySessionStatEntry->array[T_BLOCKS_READ_TIME_SESSION_LEVEL] += value; \
    } while (0)

#define pgstatCountBlocksWriteTime4SessionLevel(value)                                                   \
    do {                                                                                                 \
        if (NULL != t_thrd.shemem_ptr_cxt.mySessionStatEntry)                                            \
            t_thrd.shemem_ptr_cxt.mySessionStatEntry->array[T_BLOCKS_WRITE_TIME_SESSION_LEVEL] += value; \
    } while (0)

#define pgstatCountSort4SessionLevel(isSortInMemory)                                               \
    do {                                                                                           \
        if (NULL != t_thrd.shemem_ptr_cxt.mySessionStatEntry) {                                    \
            if (isSortInMemory) {                                                                  \
                t_thrd.shemem_ptr_cxt.mySessionStatEntry->array[N_SORT_IN_MEMORY_SESSION_LEVEL]++; \
            } else {                                                                               \
                t_thrd.shemem_ptr_cxt.mySessionStatEntry->array[N_SORT_IN_DISK_SESSION_LEVEL]++;   \
            }                                                                                      \
        }                                                                                          \
    } while (0)

#define pgstatCountCUMemHit4SessionLevel()                                   \
    do {                                                                     \
        if (NULL != t_thrd.shemem_ptr_cxt.mySessionStatEntry)                \
            t_thrd.shemem_ptr_cxt.mySessionStatEntry->array[N_CU_MEM_HIT]++; \
    } while (0)

#define pgstatCountCUHDDSyncRead4SessionLevel()                                    \
    do {                                                                           \
        if (NULL != t_thrd.shemem_ptr_cxt.mySessionStatEntry)                      \
            t_thrd.shemem_ptr_cxt.mySessionStatEntry->array[N_CU_HDD_SYNC_READ]++; \
    } while (0)

#define pgstatCountCUHDDAsynRead4SessionLevel(value)                                        \
    do {                                                                                    \
        if (NULL != t_thrd.shemem_ptr_cxt.mySessionStatEntry)                               \
            t_thrd.shemem_ptr_cxt.mySessionStatEntry->array[N_CU_HDD_ASYN_READ] += (value); \
    } while (0)

extern void DumpLWLockInfoToServerLog(void);
extern void getSessionStatistics(Tuplestorestate* tupStore, TupleDesc tupDesc,
    void (* insert)(Tuplestorestate* tupStore, TupleDesc tupDesc, const SessionLevelStatistic* entry));
extern Size sessionStatShmemSize(void);
extern void sessionStatShmemInit(void);

#define NUM_BUFFERCACHE_PAGES_ELEM 12

#define CONNECTIONINFO_LEN 8192 /* Maximum length of GUC parameter connection_info */

/*
 * Record structure holding the to-be-exposed cache data.
 */
typedef struct {
    uint32 bufferid;
    Oid relfilenode;
    int4 bucketnode;
    uint32 storage_type;
    Oid reltablespace;
    Oid reldatabase;
    ForkNumber forknum;
    BlockNumber blocknum;
    bool isvalid;
    bool isdirty;
    uint16 usagecount;
    uint32 	pinning_backends;
} BufferCachePagesRec;

typedef struct {
    int bufferid;
    uint8 is_remote_dirty;
    uint8 lock_mode;
    uint8 is_edp;
    uint8 force_request;
    uint8 need_flush;
    int buf_id;
    uint32 state;
    uint32 pblk_relno;
    uint32 pblk_blkno;
    uint64 pblk_lsn;
    uint8 seg_fileno;
    uint32 seg_blockno;
} SSBufferCtrlRec;

/*
 * Function context for data persisting over repeated calls.
 */
typedef struct {
    TupleDesc tupdesc;
    BufferCachePagesRec* record;
} BufferCachePagesContext;

typedef struct {
    TupleDesc tupdesc;
    SSBufferCtrlRec* record;
} SSBufferCtrlContext;

/* Function context for table distribution over repeated calls. */
typedef struct TableDistributionInfo {
    ParallelFunctionState* state;
    TupleTableSlot* slot;
} TableDistributionInfo;

typedef struct SessionLevelMemory {
    pg_time_t threadStartTime; /* thread start time */
    uint64 sessionid;          /* session id */
    bool isValid;              /* is valid  */
    bool iscomplex;            /* is complex query  */

    int initMemInChunks;  /* initialize memory */
    int queryMemInChunks; /* query used memory */
    int peakChunksQuery;  /* peak memory */

    int spillCount;          /* dn spill count */
    int64 spillSize;         /* dn spill size */
    int64 broadcastSize;     /* broadcast size */
    int64 estimate_time;     /* estimate total time */
    int estimate_memory;     /* estimate total memory, unit is MB */
    uint32 warning;          /* warning info */
    char* query_plan_issue;  /* query plan warning info */
    char* query_plan;        /* query plan */
    TimestampTz dnStartTime; /* start time on dn */
    TimestampTz dnEndTime;   /* end time on dn */
    uint64 plan_size;
} SessionLevelMemory;

extern void getSessionMemory(Tuplestorestate* tupStore, TupleDesc tupDesc,
    void (* insert)(Tuplestorestate* tupStore, TupleDesc tupDesc, const SessionLevelMemory* entry));
extern Size sessionMemoryShmemSize(void);
extern void sessionMemoryShmemInit(void);

extern int pgstat_get_current_active_numbackends(void);
extern PgBackendStatus* pgstat_get_backend_single_entry(ThreadId tid);
extern void pgstat_increase_session_spill();
extern void pgstat_increase_session_spill_size(int64 size);
extern void pgstat_add_warning_early_spill();
extern void pgstat_add_warning_spill_on_memory_spread();
extern void pgstat_add_warning_hash_conflict();
extern void pgstat_set_io_state(WorkloadManagerIOState iostate);
extern void pgstat_set_stmt_tag(WorkloadManagerStmtTag stmttag);
extern ThreadId* pgstat_get_user_io_entry(Oid userid, int* num);
extern ThreadId* pgstat_get_stmttag_write_entry(int* num);
extern PgBackendStatusNode* pgstat_get_backend_status_by_appname(const char* appName, int* num);
extern List* pgstat_get_user_backend_entry(Oid userid);
extern void pgstat_reset_current_status(void);
extern WaitInfo* read_current_instr_wait_info(void);
extern TableDistributionInfo* getTableDataDistribution(
    TupleDesc tuple_desc, char* schema_name = NULL, char* table_name = NULL);
extern TableDistributionInfo* getTableStat(
    TupleDesc tuple_desc, int dirty_pecent, int n_tuples, char* schema_name = NULL);
extern TableDistributionInfo* get_remote_stat_pagewriter(TupleDesc tuple_desc);
extern TableDistributionInfo* get_remote_stat_ckpt(TupleDesc tuple_desc);
extern TableDistributionInfo* get_remote_stat_bgwriter(TupleDesc tuple_desc);
extern TableDistributionInfo* get_remote_stat_candidate(TupleDesc tuple_desc);
extern TableDistributionInfo* get_remote_single_flush_dw_stat(TupleDesc tuple_desc);
extern TableDistributionInfo* get_remote_stat_double_write(TupleDesc tuple_desc);
extern TableDistributionInfo* get_remote_stat_redo(TupleDesc tuple_desc);
extern TableDistributionInfo* get_rto_stat(TupleDesc tuple_desc);
extern TableDistributionInfo* get_recovery_stat(TupleDesc tuple_desc);
extern TableDistributionInfo* streaming_hadr_get_recovery_stat(TupleDesc tuple_desc);
extern TableDistributionInfo* get_remote_node_xid_csn(TupleDesc tuple_desc);
extern TableDistributionInfo* get_remote_index_status(TupleDesc tuple_desc, const char *schname, const char *idxname);

#define SessionMemoryArraySize (BackendStatusArray_size)

/* Code Area for LWLock deadlock monitor */

#define CHANGECOUNT_IS_EVEN(_x) (((_x)&1) == 0)

typedef void (*FuncType)(Tuplestorestate *tupStore, TupleDesc tupDesc, const PgBackendStatus *beentry);

typedef struct {
    ThreadId thread_id;
    uint64 st_sessionid;
} lock_entry_id;

typedef struct {
    /* thread id for backend */
    lock_entry_id entry_id;
    /* light weight change count */
    int lw_count;
} lwm_light_detect;

typedef struct {
    lock_entry_id holder_tid;
    LWLockMode lock_sx;
} holding_lockmode;

typedef struct {
    lock_entry_id be_tid;         /* thread id */
    int be_idx;                   /* backend position */
    LWLockAddr want_lwlock;       /* lock to acquire */
    int lwlocks_num;              /* number of locks held */
    lwlock_id_mode* held_lwlocks; /* held lwlocks */
} lwm_lwlocks;

typedef struct FileIOStat {
    unsigned int changeCount;
    PgStat_Counter reads;     /* read count of file */
    PgStat_Counter writes;    /* write count of file */
    PgStat_Counter readBlks;  /* num of read blocks */
    PgStat_Counter writeBlks; /* num of write blocks */
} FileIOStat;

extern lwm_light_detect* pgstat_read_light_detect(void);
extern lwm_lwlocks* pgstat_read_diagnosis_data(
    lwm_light_detect* light_det, const int* candidates_idx, int num_candidates);
extern TimestampTz pgstat_read_xact_start_tm(int be_index);

extern THR_LOCAL HTAB* analyzeCheckHash;
extern void pgstat_read_analyzed();
typedef struct PgStat_AnaCheckEntry {
    Oid tableid;
    bool is_analyzed;
} PgStat_AnaCheckEntry;

extern HTAB* global_bad_block_stat;
extern void initLocalBadBlockStat();
extern void addBadBlockStat(const RelFileNode* relfilenode, ForkNumber forknum);
extern void resetBadBlockStat();

extern bool CalcSQLRowStatCounter(
    PgStat_TableCounts* last_total_counter, PgStat_TableCounts* current_sql_table_counter);
extern void GetCurrentTotalTableCounter(PgStat_TableCounts* total_table_counter);

typedef struct XLogStatCollect {
    double entryScanTime;
    double IOTime;
    double memsetTime;
    double entryUpdateTime;
    uint64 writeBytes;
    uint64 scanEntryCount;
    uint64 writeSomethingCount;
    uint64 flushWaitCount;
    double xlogFlushWaitTime;
    uint32 walAuxWakeNum;
    XLogRecPtr writeRqstPtr;
    XLogRecPtr minCopiedPtr;
    double IONotificationTime;
    double sendBufferTime;
    double memsetNotificationTime;
    uint32 remoteFlushWaitCount;
} XLogStatCollect;

extern THR_LOCAL XLogStatCollect *g_xlog_stat_shared;

extern void XLogStatShmemInit(void);
extern Size XLogStatShmemSize(void);
extern bool CheckUserExist(Oid userId, bool removeCount);

extern void FreeBackendStatusNodeMemory(PgBackendStatusNode* node);
extern PgBackendStatusNode* gs_stat_read_current_status(uint32* maxCalls);
extern uint32 gs_stat_read_current_status(Tuplestorestate *tupStore, TupleDesc tupDesc, FuncType insert,
                                          bool hasTID = false, ThreadId threadId = 0);
extern void pgstat_setup_memcxt(void);
extern void pgstat_clean_memcxt(void);
extern PgBackendStatus* gs_stat_fetch_stat_beentry(int32 beid);
extern void pgstat_send(void* msg, int len);
extern char* GetGlobalSessionStr(GlobalSessionId globalSessionId);
void ResetMemory(void* dest, size_t size);


typedef struct PgStat_NgMemSize {
    int* ngmemsize;
    char** ngname;
    uint32 cnti;
    uint32 cntj;
    uint32 allcnt;
} PgStat_NgMemSize;

#define END_NET_SEND_INFO(str_len)                        \
    do {                                                  \
        if (t_thrd.shemem_ptr_cxt.mySessionTimeEntry)  {  \
            og_record_opt.exit(NET_SEND_TIMES, str_len);  \
        } \
    } while (0)
#define END_NET_SEND_INFO_DUPLICATE(str_len)                                      \
                do {                                                              \
                    if (t_thrd.shemem_ptr_cxt.mySessionTimeEntry)  {              \
                        og_record_opt.report_duplicate(NET_SEND_TIMES, str_len);  \
                    } \
                } while (0)


#define END_NET_STREAM_SEND_INFO(str_len)                                         \
    do {                                                                          \
        if (t_thrd.shemem_ptr_cxt.mySessionTimeEntry)  {                          \
            og_record_opt.exit(NET_STREAM_SEND_TIMES, str_len);                   \
        }             \
    } while (0)

#define END_NET_RECV_INFO(str_len)                                                \
    do {                                                                          \
        if (t_thrd.shemem_ptr_cxt.mySessionTimeEntry)  {                          \
            og_record_opt.exit(NET_RECV_TIMES, str_len);                          \
        }       \
    } while (0)

#define END_NET_STREAM_RECV_INFO(str_len)                                         \
    do {                                                                          \
        if (t_thrd.shemem_ptr_cxt.mySessionTimeEntry)  {                          \
            og_record_opt.exit(NET_STREAM_RECV_TIMES, str_len);                   \
        } \
    } while(0)

bool GetTableGstats(Oid dbid, Oid relid, Oid parentid, PgStat_StatTabEntry *tabentry);
extern void GlobalStatsTrackerMain();
extern void GlobalStatsTrackerInit();
extern bool IsGlobalStatsTrackerProcess();
PgStat_StartBlockTableEntry* StartBlockHashTableLookup(PgStat_StartBlockTableKey *tabkey);
PgStat_StartBlockTableEntry* StartBlockHashTableAdd(PgStat_StartBlockTableKey *tabkey);
PgStat_StartBlockTableEntry* GetStartBlockHashEntry(PgStat_StartBlockTableKey *tabkey);

typedef struct IoWaitStatGlobalInfo {
    char device[MAX_DEVICE_DIR]; /* device name */

    /* iostat */
    double rs;      /* r/s */
    double ws;      /* w/s */
    double util;    /* %util */
    double w_ratio; /* write proportion */

    /* for io scheduler */
    int total_tbl_util;
    int tick_count;

    /* for io wait list */
    int io_wait_list_len;
} IoWaitStatGlobalInfo;

void pgstat_release_session_memory_entry();
extern void gs_stat_free_stat_node(PgBackendStatusNode* node);
extern void gs_stat_free_stat_beentry(PgBackendStatus* beentry);

#define MAX_PATH 256

typedef struct BadBlockKey {
    RelFileNode relfilenode;
    ForkNumber forknum;
    uint32 blocknum;
} BadBlockKey;

typedef struct BadBlockEntry {
    BadBlockKey key;
    char path[MAX_PATH];
    TimestampTz check_time;
    TimestampTz repair_time;
    XLogPhyBlock pblk;
} BadBlockEntry;

#endif /* PGSTAT_H */

