/* -------------------------------------------------------------------------
 *
 * pgstatfuncs.c
 *   Functions for accessing the statistics collector data
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *    src/backend/utils/adt/pgundostatfuncs.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"
#include <time.h>

#include "access/transam.h"
#include "access/tableam.h"
#include "access/ustore/undo/knl_uundoapi.h"
#include "access/ustore/undo/knl_uundotxn.h"
#include "access/ustore/undo/knl_uundozone.h"
#include "access/ubtree.h"
#include "access/redo_statistic.h"
#include "access/xlog.h"
#include "connector.h"
#include "commands/user.h"
#include "commands/vacuum.h"
#include "funcapi.h"
#include "gaussdb_version.h"
#include "libpq/ip.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/globalplancache.h"
#include "utils/inet.h"
#include "utils/timestamp.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/memprot.h"
#include "utils/typcache.h"
#include "utils/syscache.h"
#include "pgxc/pgxc.h"
#include "pgxc/nodemgr.h"
#include "storage/lock/lwlock.h"
#include "postgres.h"
#include "knl/knl_variable.h"
#include "storage/smgr/segment.h"
#include "storage/proc.h"
#include "storage/procarray.h"
#include "storage/buf/buf_internals.h"
#include "workload/cpwlm.h"
#include "workload/workload.h"
#include "pgxc/pgxcnode.h"
#include "access/hash.h"
#include "libcomm/libcomm.h"
#include "pgxc/poolmgr.h"
#include "pgxc/execRemote.h"
#include "utils/elog.h"
#include "utils/memtrace.h"
#include "commands/user.h"
#include "instruments/gs_stat.h"
#include "instruments/list.h"
#include "replication/rto_statistic.h"
#include "storage/lock/lock.h"

const int STAT_USTORE_BUFF_SIZE = 10240;
const int STAT_UNDO_COLS = 10;
const int STAT_UNDO_BUFFER_SIZE = 500;
const uint TOP_USED_ZONE_NUM = 3;
const float FORCE_RECYCLE_PERCENT = 0.8;
const int UNDO_SLOT_FILE_MAXSIZE = 1024 * 32;
const int MBYTES_TO_KBYTES = 1024;
const int UNDO_TOPUSED = 0;
const int UNDO_SECONDUSED = 1;
const int UNDO_THIRDUSED = 2;
const int PG_STAT_USP_PERSIST_META_COLS = 9;
const int STAT_UNDO_LOG_SIZE = 17;
const int PG_STAT_UBTREE_IDX_VERFIY_COLS = 4;
const int PG_STAT_UBTREE_RECYCLE_QUEUE_COLS = 6;
const int PG_STAT_TRANSLOT_META_COLS = 7;
const int COMMITED_STATUS = 0;
const int INPROCESS_STATUS = 1;
const int ABORTING_STATUS = 2;
const int ABORTED_STATUS = 3;

const int TYPE_UNDO_ZONE = 0;
const int TYPE_GROUP = 1;
const int TYPE_UNDO_SPACE = 2;
const int TYPE_SLOT_SPACE = 3;

/* ustore stat */
extern Datum gs_stat_ustore(PG_FUNCTION_ARGS);
extern void CheckUser(const char *fName);
extern char *ParsePage(char *path, int64 blocknum, char *relation_type, bool read_memory, bool dumpUndo = false);

typedef struct UndoHeader {
    UndoRecordHeader whdr_;
    UndoRecordBlock wblk_;
    UndoRecordTransaction wtxn_;
    UndoRecordPayload wpay_;
    UndoRecordOldTd wtd_;
    UndoRecordPartition wpart_;
    UndoRecordTablespace wtspc_;
    StringInfoData rawdata_;
} UndoHeader;

typedef struct UHeapDiskTupleDataHeader {
    ShortTransactionId xid;
    uint16 td_id : 8, reserved : 8; /* Locker as well as the last updater, 8 bits each */
    uint16 flag;                          /* Flag for tuple attributes */
    uint16 flag2;                         /* Number of attributes for now(11 bits) */
    uint8 t_hoff;                         /*  header incl. bitmap, padding */
} UHeapDiskTupleDataHeader;

void Checkfd(int fd)
{
    if (fd < 0) {
        ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION), errmsg("could not open file \%s", UNDO_META_FILE)));
        return;
    }
}

Datum gs_stat_ustore(PG_FUNCTION_ARGS)
{
    char result[STAT_USTORE_BUFF_SIZE] = {0};
#ifdef DEBUG_UHEAP
    errno_t ret;
    LWLockAcquire(UHeapStatLock, LW_SHARED);

    ret = snprintf_s(result, sizeof(result), sizeof(result) - 1, "Prune Page    \n");
    securec_check_ss(ret, "\0", "\0");
    ret = snprintf_s(result + strlen(result), sizeof(result) - strlen(result), sizeof(result) - strlen(result) -1,
        "\tPrune Page (SUCCESS) = %lu    \n",
        UHeapStat_shared->prune_page[PRUNE_PAGE_SUCCESS]);
    securec_check_ss(ret, "\0", "\0");
    ret = snprintf_s(result + strlen(result), sizeof(result) - strlen(result), sizeof(result) - strlen(result) -1,
        "\tPrune Page (NO_SPACE) = %lu    \n",
        UHeapStat_shared->prune_page[PRUNE_PAGE_NO_SPACE]);
    securec_check_ss(ret, "\0", "\0");
    ret = snprintf_s(result + strlen(result), sizeof(result) - strlen(result), sizeof(result) - strlen(result) -1,
        "\tPrune Page (UPDATE_IN_PROGRESS)= %lu    \n=========================\n\n",
        UHeapStat_shared->prune_page[PRUNE_PAGE_UPDATE_IN_PROGRESS]);
    securec_check_ss(ret, "\0", "\0");
    ret = snprintf_s(result + strlen(result), sizeof(result) - strlen(result), sizeof(result) - strlen(result) -1,
        "\tPrune Page (PRUNE_PAGE_IN_RECOVERY)= %lu    \n=========================\n\n",
        UHeapStat_shared->prune_page[PRUNE_PAGE_IN_RECOVERY]);
    securec_check_ss(ret, "\0", "\0");
    ret = snprintf_s(result + strlen(result), sizeof(result) - strlen(result), sizeof(result) - strlen(result) -1,
        "\tPrune Page (PRUNE_PAGE_INVALID)= %lu    \n=========================\n\n",
        UHeapStat_shared->prune_page[PRUNE_PAGE_INVALID]);
    securec_check_ss(ret, "\0", "\0");
    ret = snprintf_s(result + strlen(result), sizeof(result) - strlen(result), sizeof(result) - strlen(result) -1,
        "\tPrune Page (PRUNE_PAGE_XID_FILTER)= %lu    \n=========================\n\n",
        UHeapStat_shared->prune_page[PRUNE_PAGE_XID_FILTER]);
    securec_check_ss(ret, "\0", "\0");
    ret = snprintf_s(result + strlen(result), sizeof(result) - strlen(result), sizeof(result) - strlen(result) -1,
        "\tPrune Page (PRUNE_PAGE_FILLFACTOR)= %lu    \n=========================\n\n",
        UHeapStat_shared->prune_page[PRUNE_PAGE_FILLFACTOR]);
    securec_check_ss(ret, "\0", "\0");

    ret = snprintf_s(result + strlen(result), sizeof(result) - strlen(result), sizeof(result) - strlen(result) -1,
        "Prune Page OPs profile    \n");
    securec_check_ss(ret, "\0", "\0");
    ret = snprintf_s(result + strlen(result), sizeof(result) - strlen(result), sizeof(result) - strlen(result) -1,
        "\t Prune Page SUC: %u %u %u    \n",
        UHeapStat_shared->op_count_suc.ins, UHeapStat_shared->op_count_suc.del, UHeapStat_shared->op_count_suc.upd);
    securec_check_ss(ret, "\0", "\0");

    ret = snprintf_s(result + strlen(result), sizeof(result) - strlen(result), sizeof(result) - strlen(result) -1,
        "\t Prune Page TOT: %u %u %u    \n",
        UHeapStat_shared->op_count_tot.ins, UHeapStat_shared->op_count_tot.del, UHeapStat_shared->op_count_tot.upd);
    securec_check_ss(ret, "\0", "\0");

    ret = snprintf_s(result + strlen(result), sizeof(result) - strlen(result), sizeof(result) - strlen(result) -1,
        "Prune Page OPs freespace profile    \n");
    securec_check_ss(ret, "\0", "\0");
    ret = snprintf_s(result + strlen(result), sizeof(result) - strlen(result), sizeof(result) - strlen(result) -1,
        "\t Prune Page FreeSpace TOT: %u %u %u    \n", UHeapStat_shared->op_space_tot.ins,
        UHeapStat_shared->op_space_tot.del, UHeapStat_shared->op_space_tot.upd);
    securec_check_ss(ret, "\0", "\0");

    ret = snprintf_s(result + strlen(result), sizeof(result) - strlen(result), sizeof(result) - strlen(result) -1,
        "PageReserveTransactionSlot (where to get transaction slot)    \n");
    securec_check_ss(ret, "\0", "\0");
    ret = snprintf_s(result + strlen(result), sizeof(result) - strlen(result), sizeof(result) - strlen(result) -1,
        "\tSlot has been reserved by current xid: %lu    \n",
        UHeapStat_shared->get_transslot_from[TRANSSLOT_RESERVED_BY_CURRENT_XID]);
    securec_check_ss(ret, "\0", "\0");
    ret = snprintf_s(result + strlen(result), sizeof(result) - strlen(result), sizeof(result) - strlen(result) -1,
        "\tGot free slot after invalidating slots: %lu    \n",
        UHeapStat_shared->get_transslot_from[TRANSSLOT_FREE_AFTER_INVALIDATION]);
    securec_check_ss(ret, "\0", "\0");
    ret = snprintf_s(result + strlen(result), sizeof(result) - strlen(result), sizeof(result) - strlen(result) -1,
        "\tGot free slot after freezing slots: %lu    \n",
        UHeapStat_shared->get_transslot_from[TRANSSLOT_FREE_AFTER_FREEZING]);
    securec_check_ss(ret, "\0", "\0");
    ret = snprintf_s(result + strlen(result), sizeof(result) - strlen(result), sizeof(result) - strlen(result) -1,
        "\tCannot get a free transaction slot: %lu    \n=========================\n\n",
        UHeapStat_shared->get_transslot_from[TRANSSLOT_CANNOT_GET]);
    securec_check_ss(ret, "\0", "\0");

    ret = snprintf_s(result + strlen(result), sizeof(result) - strlen(result), sizeof(result) - strlen(result) -1,
        "Inplace Update Stats    \n");
    securec_check_ss(ret, "\0", "\0");
    ret = snprintf_s(result + strlen(result), sizeof(result) - strlen(result), sizeof(result) - strlen(result) -1,
        "\tINPLACE UPDATE: %lu    \n",
        UHeapStat_shared->update[INPLACE_UPDATE]);
    securec_check_ss(ret, "\0", "\0");
    ret = snprintf_s(result + strlen(result), sizeof(result) - strlen(result), sizeof(result) - strlen(result) -1,
        "\tNON INPLACE UPDATE: %lu    \n",
        UHeapStat_shared->update[NON_INPLACE_UPDATE]);
    securec_check_ss(ret, "\0", "\0");
    ret = snprintf_s(result + strlen(result),  sizeof(result) - strlen(result), sizeof(result) - strlen(result) -1,
        "\tTotal: %lu    \n=========================\n\n",
        UHeapStat_shared->update[NON_INPLACE_UPDATE] + UHeapStat_shared->update[INPLACE_UPDATE]);
    securec_check_ss(ret, "\0", "\0");

    ret = snprintf_s(result + strlen(result), sizeof(result) - strlen(result), sizeof(result) - strlen(result) -1,
        "Non Inplace Update Reasons    \n");
    securec_check_ss(ret, "\0", "\0");
    ret = snprintf_s(result + strlen(result), sizeof(result) - strlen(result), sizeof(result) - strlen(result) -1,
        "\tIndex Updated: %lu    \n",
        UHeapStat_shared->noninplace_update_cause[INDEX_UPDATED]);
    securec_check_ss(ret, "\0", "\0");
    ret = snprintf_s(result + strlen(result), sizeof(result) - strlen(result), sizeof(result) - strlen(result) -1,
        "\tToast: %lu    \n",
        UHeapStat_shared->noninplace_update_cause[TOAST]);
    securec_check_ss(ret, "\0", "\0");
    ret = snprintf_s(result + strlen(result), sizeof(result) - strlen(result), sizeof(result) - strlen(result) -1,
        "\tPrune Page Failed: %lu    \n",
        UHeapStat_shared->noninplace_update_cause[PAGE_PRUNE_FAILED]);
    securec_check_ss(ret, "\0", "\0");
    ret = snprintf_s(result + strlen(result), sizeof(result) - strlen(result), sizeof(result) - strlen(result) -1,
        "\tSlot reused: %lu    \n",
        UHeapStat_shared->noninplace_update_cause[SLOT_REUSED]);
    securec_check_ss(ret, "\0", "\0");
    ret = snprintf_s(result + strlen(result), sizeof(result) - strlen(result), sizeof(result) - strlen(result) -1,
        "\tnblocks < NBLOCKS: %lu    \n=========================\n\n",
        UHeapStat_shared->noninplace_update_cause[nblocks_LESS_THAN_NBLOCKS]);
    securec_check_ss(ret, "\0", "\0");

    ret = snprintf_s(result + strlen(result), sizeof(result) - strlen(result), sizeof(result) - strlen(result) -1,
        "Slot status in UHeapTupleFetch   \n");
    securec_check_ss(ret, "\0", "\0");
    ret = snprintf_s(result + strlen(result), sizeof(result) - strlen(result), sizeof(result) - strlen(result) -1,
        "\tFROZEN_SLOT = %lu    \n",
        UHeapStat_shared->visibility_check_with_xid[VISIBILITY_CHECK_SUCCESS_FROZEN_SLOT]);
    securec_check_ss(ret, "\0", "\0");
    ret = snprintf_s(result + strlen(result), sizeof(result) - strlen(result), sizeof(result) - strlen(result) -1,
        "\tXID < Oldest XID in UNDO = %lu    \n",
        UHeapStat_shared->visibility_check_with_xid[VISIBILITY_CHECK_SUCCESS_OLDEST_XID]);
    securec_check_ss(ret, "\0", "\0");
    ret = snprintf_s(result + strlen(result), sizeof(result) - strlen(result), sizeof(result) - strlen(result) -1,
        "\tSlot is invalid and Xid is Visible in Snapshot = %lu    \n",
        UHeapStat_shared->visibility_check_with_xid[VISIBILITY_CHECK_SUCCESS_INVALID_SLOT]);
    securec_check_ss(ret, "\0", "\0");
    ret = snprintf_s(result + strlen(result), sizeof(result) - strlen(result), sizeof(result) - strlen(result) -1,
        "\tFetch Trans Info From UNDO  = %lu\n=========================\n\n",
        UHeapStat_shared->visibility_check_with_xid[VISIBILITY_CHECK_SUCCESS_UNDO]);
    securec_check_ss(ret, "\0", "\0");

    ret = snprintf_s(result + strlen(result), sizeof(result) - strlen(result), sizeof(result) - strlen(result) -1,
        "Undo Chain Traversal Stat    \n");
    securec_check_ss(ret, "\0", "\0");

    double tuple_old_version_visit_rate = 0.0;
    if (UHeapStat_shared->tuple_visits > 0) {
        tuple_old_version_visit_rate =
            1.0 * UHeapStat_shared->tuple_old_version_visits / UHeapStat_shared->tuple_visits;
    }
    ret = snprintf_s(result + strlen(result),  sizeof(result) - strlen(result), sizeof(result) - strlen(result) -1,
        "Tuple visits: %lu\tOld version visits: %lu\tOld version visit rate: %.6f    \n",
        UHeapStat_shared->tuple_visits, UHeapStat_shared->tuple_old_version_visits, tuple_old_version_visit_rate);
    securec_check_ss(ret, "\0", "\0");
    ret = snprintf_s(result + strlen(result), sizeof(result) - strlen(result), sizeof(result) - strlen(result) -1,
        "Undo Chain Traversal Length    \n");
    securec_check_ss(ret, "\0", "\0");

    double chain_visited_avg_len = 0.0;
    if (UHeapStat_shared->undo_chain_visited_count > 0) {
        chain_visited_avg_len =
            UHeapStat_shared->undo_chain_visited_sum_len * 1.0 / UHeapStat_shared->undo_chain_visited_count;
    }
    ret = snprintf_s(result + strlen(result), sizeof(result) - strlen(result), sizeof(result) - strlen(result) -1,
        "\t# Of undo_chain_visited_sum_len = %ld | visited_count = %ld | miss_count = %ld | visited_avg_len = %lf | "
        "visited_max_len = %ld | visited_min_len = %ld \n",
        UHeapStat_shared->undo_chain_visited_sum_len, UHeapStat_shared->undo_chain_visited_count,
        UHeapStat_shared->undo_chain_visited_miss_count, chain_visited_avg_len,
        UHeapStat_shared->undo_chain_visited_max_len, UHeapStat_shared->undo_chain_visited_min_len);
    securec_check_ss(ret, "\0", "\0");

    double page_visited_avg_len = 0.0;
    if (UHeapStat_shared->undo_chain_visited_count > 0) {
        page_visited_avg_len =
            UHeapStat_shared->undo_page_visited_sum_len * 1.0 / UHeapStat_shared->undo_chain_visited_count;
    }
    ret = snprintf_s(result + strlen(result), sizeof(result) - strlen(result), sizeof(result) - strlen(result) -1,
        "\t# Of undo_page_visited_sum_len = %ld | visited_count = %ld | page_visited_avg_len = %lf \n",
        UHeapStat_shared->undo_page_visited_sum_len, UHeapStat_shared->undo_chain_visited_count, page_visited_avg_len);
    securec_check_ss(ret, "\0", "\0");

    ret = snprintf_s(result + strlen(result), sizeof(result) - strlen(result), sizeof(result) - strlen(result) -1,
        "prepare undo record rzero count %lu nzero_count %lu \n", UHeapStat_shared->undo_record_prepare_rzero_count,
        UHeapStat_shared->undo_record_prepare_nzero_count);
    securec_check_ss(ret, "\0", "\0");

    ret = snprintf_s(result + strlen(result), sizeof(result) - strlen(result), sizeof(result) - strlen(result) -1,
        "groups allocated %lu released %lu \n",
        UHeapStat_shared->undo_groups_allocate, UHeapStat_shared->undo_groups_release);
    securec_check_ss(ret, "\0", "\0");
    ret = snprintf_s(result + strlen(result), sizeof(result) - strlen(result), sizeof(result) - strlen(result) -1,
        "slots allocated %lu released %lu \n",
        UHeapStat_shared->undo_slots_allocate, UHeapStat_shared->undo_slots_recycle);
    securec_check_ss(ret, "\0", "\0");
    ret = snprintf_s(result + strlen(result), sizeof(result) - strlen(result), sizeof(result) - strlen(result) -1,
        "space recycle %lu unrecycle %lu \n",
        UHeapStat_shared->undo_space_recycle, UHeapStat_shared->undo_space_unrecycle);
    securec_check_ss(ret, "\0", "\0");
    ret = snprintf_s(result + strlen(result), sizeof(result) - strlen(result), sizeof(result) - strlen(result) -1,
        "oldest xid delay %lu \n",
        UHeapStat_shared->oldest_xid_having_undo_delay);
    securec_check_ss(ret, "\0", "\0");

    ret = snprintf_s(result + strlen(result), sizeof(result) - strlen(result), sizeof(result) - strlen(result) -1,
        "Undo lock information:    \n");
    securec_check_ss(ret, "\0", "\0");
    ret = snprintf_s(result + strlen(result), sizeof(result) - strlen(result), sizeof(result) - strlen(result) -1,
        "\tDiscard lock hold time(us): total %lu\tmin %lu\tmax %lu\tcnt %lu \tavg %.6f    \n",
        UHeapStat_shared->undo_discard_lock_hold_time_sum, UHeapStat_shared->undo_discard_lock_hold_time_min,
        UHeapStat_shared->undo_discard_lock_hold_time_max, UHeapStat_shared->undo_discard_lock_hold_cnt,
        1.0 * UHeapStat_shared->undo_discard_lock_hold_time_sum / Max(1, UHeapStat_shared->undo_discard_lock_hold_cnt));
    securec_check_ss(ret, "\0", "\0");
    ret = snprintf_s(result + strlen(result), sizeof(result) - strlen(result), sizeof(result) - strlen(result) -1,
        "\tDiscard lock wait time(us): total %lu\tmin %lu\tmax %lu\tcnt %lu\tavg %.6f    \n",
        UHeapStat_shared->undo_discard_lock_wait_time_sum, UHeapStat_shared->undo_discard_lock_wait_time_min,
        UHeapStat_shared->undo_discard_lock_wait_time_max, UHeapStat_shared->undo_discard_lock_wait_cnt,
        1.0 * UHeapStat_shared->undo_discard_lock_wait_time_sum / Max(1, UHeapStat_shared->undo_discard_lock_wait_cnt));
    securec_check_ss(ret, "\0", "\0");
    ret = snprintf_s(result + strlen(result), sizeof(result) - strlen(result), sizeof(result) - strlen(result) -1,
        "\tSpace lock hold time(us): total %lu\tmin %lu\tmax %lu\tcnt %lu\tavg %.6f    \n",
        UHeapStat_shared->undo_space_lock_hold_time_sum, UHeapStat_shared->undo_space_lock_hold_time_min,
        UHeapStat_shared->undo_space_lock_hold_time_max, UHeapStat_shared->undo_space_lock_hold_cnt,
        1.0 * UHeapStat_shared->undo_space_lock_hold_time_sum / Max(1, UHeapStat_shared->undo_space_lock_hold_cnt));
    securec_check_ss(ret, "\0", "\0");
    ret = snprintf_s(result + strlen(result), sizeof(result) - strlen(result), sizeof(result) - strlen(result) -1,
        "\tSpace lock wait time(us): total %lu\tmin %lu\tmax %lu\tcnt %lu\tavg %.6f    \n",
        UHeapStat_shared->undo_space_lock_wait_time_sum, UHeapStat_shared->undo_space_lock_wait_time_min,
        UHeapStat_shared->undo_space_lock_wait_time_max, UHeapStat_shared->undo_space_lock_wait_cnt,
        1.0 * UHeapStat_shared->undo_space_lock_wait_time_sum / Max(1, UHeapStat_shared->undo_space_lock_wait_cnt));
    securec_check_ss(ret, "\0", "\0");
    ret = snprintf_s(result + strlen(result), sizeof(result) - strlen(result), sizeof(result) - strlen(result) -1,
        "INSERT: %lu\tRetry: %lu\tRetry Time: %lu\tRetry Max: %lu\tRetry Time MAX: %lu \n", UHeapStat_shared->dml,
        UHeapStat_shared->retry, UHeapStat_shared->retry_time, UHeapStat_shared->retry_max,
        UHeapStat_shared->retry_time_max);
    securec_check_ss(ret, "\0", "\0");

    result[strlen(result)] = '\0';

    LWLockRelease(UHeapStatLock);

#endif
    PG_RETURN_TEXT_P(cstring_to_text(result));
}

using namespace undo;

bool ReadUndoBytes(char *destptr, int destlen, char **readeptr, char *endptr, int *myBytesRead, int *alreadyRead)
{
    if (*myBytesRead >= destlen) {
        *myBytesRead -= destlen;
        return true;
    }
    int remaining = destlen - *myBytesRead;
    int maxReadOnCurrPage = endptr - *readeptr;
    int canRead = Min(remaining, maxReadOnCurrPage);
    if (canRead == 0) {
        return false;
    }
    errno_t rc = memcpy_s(destptr + *myBytesRead, remaining, *readeptr, canRead);
    securec_check(rc, "\0", "\0");
    *readeptr += canRead;
    *alreadyRead += canRead;
    *myBytesRead = 0;
    return (canRead == remaining);
}
bool ReadUndoRecord(UndoHeader *urec, char *buffer, int startingByte, int *alreadyRead)
{
    char *readptr = buffer + startingByte;
    char *endptr = buffer + BLCKSZ;
    int myBytesRead = *alreadyRead;
    if (!ReadUndoBytes((char *)&(urec->whdr_), SIZE_OF_UNDO_RECORD_HEADER, &readptr, endptr, &myBytesRead,
        alreadyRead)) {
        return false;
    }
    if (!ReadUndoBytes((char *)&(urec->wblk_), SIZE_OF_UNDO_RECORD_BLOCK, &readptr, endptr, &myBytesRead,
        alreadyRead)) {
        return false;
    }
    if ((urec->whdr_.uinfo & UNDO_UREC_INFO_TRANSAC) != 0) {
        if (!ReadUndoBytes((char *)&urec->wtxn_, SIZE_OF_UNDO_RECORD_TRANSACTION, &readptr, endptr, &myBytesRead,
            alreadyRead)) {
            return false;
        }
    }
    if ((urec->whdr_.uinfo & UNDO_UREC_INFO_OLDTD) != 0) {
        if (!ReadUndoBytes((char *)&urec->wtd_, SIZE_OF_UNDO_RECORD_OLDTD, &readptr, endptr, &myBytesRead,
            alreadyRead)) {
            return false;
        }
    }
    if ((urec->whdr_.uinfo & UNDO_UREC_INFO_HAS_PARTOID) != 0) {
        if (!ReadUndoBytes((char *)&urec->wpart_, SIZE_OF_UNDO_RECORD_PARTITION, &readptr, endptr, &myBytesRead,
            alreadyRead)) {
            return false;
        }
    }
    if ((urec->whdr_.uinfo & UNDO_UREC_INFO_HAS_TABLESPACEOID) != 0) {
        if (!ReadUndoBytes((char *)&urec->wtspc_, SIZE_OF_UNDO_RECORD_TABLESPACE, &readptr, endptr, &myBytesRead,
            alreadyRead)) {
            return false;
        }
    }
    if ((urec->whdr_.uinfo & UNDO_UREC_INFO_PAYLOAD) != 0) {
        if (!ReadUndoBytes((char *)&urec->wpay_, SIZE_OF_UNDO_RECORD_PAYLOAD, &readptr, endptr, &myBytesRead,
            alreadyRead)) {
            return false;
        }
        urec->rawdata_.len = urec->wpay_.payloadlen;
        if (urec->rawdata_.len > 0) {
            if (urec->rawdata_.data == NULL) {
                urec->rawdata_.data = (char *)malloc(urec->rawdata_.len);
                if (NULL == urec->rawdata_.data) {
                    elog(ERROR, "out of memory");
                    return false;
                }
            }
            if (!ReadUndoBytes((char *)urec->rawdata_.data, urec->rawdata_.len,
                &readptr, endptr, &myBytesRead, alreadyRead)) {
                return false;
            }
        }
    }
    return true;
}
static int OpenUndoBlock(int zoneId, BlockNumber blockno)
{
    char fileName[100] = {0};
    errno_t rc = EOK;
    int segno = blockno / UNDOSEG_SIZE;
    rc = snprintf_s(fileName, sizeof(fileName), sizeof(fileName), "undo/permanent/%05X.%07zX", zoneId, segno);
    securec_check_ss(rc, "\0", "\0");
    int fd = open(fileName, O_RDONLY | PG_BINARY, S_IRUSR | S_IWUSR);
    Checkfd(fd);

    return fd;
}
static bool ParseUndoRecord(UndoRecPtr urp, Tuplestorestate *tupstore, TupleDesc tupDesc)
{
    char buffer[BLCKSZ] = {'\0'};
    BlockNumber blockno = UNDO_PTR_GET_BLOCK_NUM(urp);
    int zoneId = UNDO_PTR_GET_ZONE_ID(urp);
    int startingByte = ((urp) & ((UINT64CONST(1) << 44) - 1)) % BLCKSZ;
    int fd = -1;
    int alreadyRead = 0;
    off_t seekpos;
    errno_t rc = EOK;
    uint32 ret = 0;
    UndoHeader *urec = (UndoHeader *)malloc(sizeof(UndoHeader));
    UndoRecPtr blkprev = INVALID_UNDO_REC_PTR;
    rc = memset_s(urec, sizeof(UndoHeader), (0), sizeof(UndoHeader));
    securec_check(rc, "\0", "\0");
    do {
        fd = OpenUndoBlock(zoneId, blockno);
        if (fd < 0) {
            free(urec);
            return false;
        }
        seekpos = (off_t)BLCKSZ * (blockno % ((BlockNumber)UNDOSEG_SIZE));
        lseek(fd, seekpos, SEEK_SET);
        rc = memset_s(buffer, BLCKSZ, 0, BLCKSZ);
        securec_check(rc, "\0", "\0");
        ret = read(fd, (char *)buffer, BLCKSZ);
        if (ret != BLCKSZ) {
            close(fd);
            free(urec);
            fprintf(stderr, "Read undo meta page failed, expect size(8192), real size(%u).\n", ret);
            return false;
        }
        if (ReadUndoRecord(urec, buffer, startingByte, &alreadyRead)) {
            break;
        }
        startingByte = UNDO_LOG_BLOCK_HEADER_SIZE;
        blockno++;
    } while (true);
    blkprev = urec->wblk_.blkprev;
    char textBuffer[STAT_UNDO_LOG_SIZE] = {'\0'};
    bool nulls[21] = {false};
    Datum values[21];

    rc = memset_s(textBuffer, STAT_UNDO_LOG_SIZE, 0, STAT_UNDO_LOG_SIZE);
    securec_check(rc, "\0", "\0");
    values[ARR_0] = ObjectIdGetDatum(urp);
    values[ARR_1] = ObjectIdGetDatum(urec->whdr_.xid);

    rc = snprintf_s(textBuffer, sizeof(textBuffer), sizeof(textBuffer) - 1, UNDO_REC_PTR_FORMAT, urec->whdr_.cid);
    securec_check_ss(rc, "\0", "\0");
    values[ARR_2] = CStringGetTextDatum(textBuffer);

    rc = snprintf_s(textBuffer, sizeof(textBuffer), sizeof(textBuffer) - 1, UNDO_REC_PTR_FORMAT, urec->whdr_.reloid);
    securec_check_ss(rc, "\0", "\0");
    values[ARR_3] = CStringGetTextDatum(textBuffer);

    rc = snprintf_s(textBuffer, sizeof(textBuffer), sizeof(textBuffer) - 1, UNDO_REC_PTR_FORMAT,
        urec->whdr_.relfilenode);
    securec_check_ss(rc, "\0", "\0");
    values[ARR_4] = CStringGetTextDatum(textBuffer);

    rc = snprintf_s(textBuffer, sizeof(textBuffer), sizeof(textBuffer) - 1, UNDO_REC_PTR_FORMAT, urec->whdr_.uinfo);
    securec_check_ss(rc, "\0", "\0");
    values[ARR_5] = CStringGetTextDatum(textBuffer);

    rc = snprintf_s(textBuffer, sizeof(textBuffer), sizeof(textBuffer) - 1, UNDO_REC_PTR_FORMAT, urec->wblk_.blkprev);
    securec_check_ss(rc, "\0", "\0");
    values[ARR_6] = CStringGetTextDatum(textBuffer);

    rc = snprintf_s(textBuffer, sizeof(textBuffer), sizeof(textBuffer) - 1, UNDO_REC_PTR_FORMAT, urec->wblk_.blkno);
    securec_check_ss(rc, "\0", "\0");
    values[ARR_7] = CStringGetTextDatum(textBuffer);

    rc = snprintf_s(textBuffer, sizeof(textBuffer), sizeof(textBuffer) - 1, UNDO_REC_PTR_FORMAT, urec->wblk_.offset);
    securec_check_ss(rc, "\0", "\0");
    values[ARR_8] = CStringGetTextDatum(textBuffer);

    rc = snprintf_s(textBuffer, sizeof(textBuffer), sizeof(textBuffer) - 1, UNDO_REC_PTR_FORMAT, urec->wtxn_.prevurp);
    securec_check_ss(rc, "\0", "\0");
    values[ARR_9] = CStringGetTextDatum(textBuffer);

    rc =
        snprintf_s(textBuffer, sizeof(textBuffer), sizeof(textBuffer) - 1,
        UNDO_REC_PTR_FORMAT, urec->wpay_.payloadlen);
    securec_check_ss(rc, "\0", "\0");
    values[ARR_10] = CStringGetTextDatum(textBuffer);

    rc = snprintf_s(textBuffer, sizeof(textBuffer), sizeof(textBuffer) - 1,
        UNDO_REC_PTR_FORMAT, urec->wtd_.oldxactid);
    securec_check_ss(rc, "\0", "\0");
    values[ARR_11] = CStringGetTextDatum(textBuffer);

    rc = snprintf_s(textBuffer, sizeof(textBuffer), sizeof(textBuffer) - 1,
        UNDO_REC_PTR_FORMAT, urec->wpart_.partitionoid);
    securec_check_ss(rc, "\0", "\0");
    values[ARR_12] = CStringGetTextDatum(textBuffer);

    rc = snprintf_s(textBuffer, sizeof(textBuffer), sizeof(textBuffer) - 1,
        UNDO_REC_PTR_FORMAT, urec->wtspc_.tablespace);
    securec_check_ss(rc, "\0", "\0");
    values[ARR_13] = CStringGetTextDatum(textBuffer);

    rc = snprintf_s(textBuffer, sizeof(textBuffer), sizeof(textBuffer) - 1, UNDO_REC_PTR_FORMAT, alreadyRead);
    securec_check_ss(rc, "\0", "\0");
    values[ARR_14] = CStringGetTextDatum(textBuffer);

    char prevLen[2];
    UndoRecordSize byteToRead = sizeof(UndoRecordSize);
    char *readptr = buffer + startingByte - byteToRead;
    for (auto i = 0; i < byteToRead; i++) {
        prevLen[i] = *readptr;
        readptr++;
    }
    UndoRecordSize prevRecLen = *(UndoRecordSize *)(prevLen);
    rc = snprintf_s(textBuffer, sizeof(textBuffer), sizeof(textBuffer) - 1, UNDO_REC_PTR_UFORMAT, prevRecLen);
    securec_check_ss(rc, "\0", "\0");
    values[ARR_15] = CStringGetTextDatum(textBuffer);

    rc = snprintf_s(textBuffer, sizeof(textBuffer), sizeof(textBuffer) - 1, UNDO_REC_PTR_DFORMAT, -1);
    securec_check_ss(rc, "\0", "\0");
    values[ARR_16] = CStringGetTextDatum(textBuffer);

    rc = snprintf_s(textBuffer, sizeof(textBuffer), sizeof(textBuffer) - 1, UNDO_REC_PTR_DFORMAT, -1);
    securec_check_ss(rc, "\0", "\0");
    values[ARR_17] = CStringGetTextDatum(textBuffer);

    rc = snprintf_s(textBuffer, sizeof(textBuffer), sizeof(textBuffer) - 1, UNDO_REC_PTR_DFORMAT, -1);
    securec_check_ss(rc, "\0", "\0");
    values[ARR_18] = CStringGetTextDatum(textBuffer);

    rc = snprintf_s(textBuffer, sizeof(textBuffer), sizeof(textBuffer) - 1, UNDO_REC_PTR_DFORMAT, -1);
    securec_check_ss(rc, "\0", "\0");
    values[ARR_19] = CStringGetTextDatum(textBuffer);

    rc = snprintf_s(textBuffer, sizeof(textBuffer), sizeof(textBuffer) - 1, UNDO_REC_PTR_DFORMAT, -1);
    securec_check_ss(rc, "\0", "\0");
    values[ARR_20] = CStringGetTextDatum(textBuffer);

    if (urec->whdr_.utype != UNDO_INSERT && urec->whdr_.utype != UNDO_MULTI_INSERT &&
        urec->rawdata_.len > 0 && urec->rawdata_.data != NULL) {
        UHeapDiskTupleDataHeader diskTuple;
        if (urec->whdr_.utype == UNDO_INPLACE_UPDATE) {
            Assert(urec->rawdata_.len >= (int)SizeOfUHeapDiskTupleData);
            errno_t rc = memcpy_s((char *)&diskTuple + OffsetTdId, SizeOfUHeapDiskTupleHeaderExceptXid,
                urec->rawdata_.data + sizeof(uint8), SizeOfUHeapDiskTupleHeaderExceptXid);
            securec_check(rc, "", "");
            diskTuple.xid = (ShortTransactionId)InvalidTransactionId;
        } else {
            Assert(urec->rawdata_.len >= (int)SizeOfUHeapDiskTupleHeaderExceptXid);
            errno_t rc = memcpy_s(((char *)&diskTuple + OffsetTdId), SizeOfUHeapDiskTupleHeaderExceptXid,
                urec->rawdata_.data, SizeOfUHeapDiskTupleHeaderExceptXid);
            securec_check(rc, "", "");
            diskTuple.xid = (ShortTransactionId)InvalidTransactionId;
        }
        rc = snprintf_s(textBuffer, sizeof(textBuffer), sizeof(textBuffer) - 1,
            UNDO_REC_PTR_UFORMAT, diskTuple.td_id);
        securec_check_ss(rc, "\0", "\0");
        values[ARR_16] = CStringGetTextDatum(textBuffer);

        rc = snprintf_s(textBuffer, sizeof(textBuffer), sizeof(textBuffer) - 1,
            UNDO_REC_PTR_UFORMAT, diskTuple.reserved);
        securec_check_ss(rc, "\0", "\0");
        values[ARR_17] = CStringGetTextDatum(textBuffer);

        rc = snprintf_s(textBuffer, sizeof(textBuffer), sizeof(textBuffer) - 1,
            UNDO_REC_PTR_UFORMAT, diskTuple.flag);
        securec_check_ss(rc, "\0", "\0");
        values[ARR_18] = CStringGetTextDatum(textBuffer);

        rc = snprintf_s(textBuffer, sizeof(textBuffer), sizeof(textBuffer) - 1,
            UNDO_REC_PTR_UFORMAT, diskTuple.flag2);
        securec_check_ss(rc, "\0", "\0");
        values[ARR_19] = CStringGetTextDatum(textBuffer);

        rc = snprintf_s(textBuffer, sizeof(textBuffer), sizeof(textBuffer) - 1,
            UNDO_REC_PTR_UFORMAT, diskTuple.t_hoff);
        securec_check_ss(rc, "\0", "\0");
        values[ARR_20] = CStringGetTextDatum(textBuffer);
    }

    tuplestore_putvalues(tupstore, tupDesc, values, nulls);
    free(urec);
    close(fd);
    if (blkprev != INVALID_UNDO_REC_PTR) {
        ParseUndoRecord(blkprev, tupstore, tupDesc);
    }
    return true;
}

void moveMaxUsedSpaceItem (uint startIdx, uint32 *maxUsedSpace,
    uint32 *maxUsedSpaceZoneId, const uint size)
{
    for (uint usedSpaceIdx = (size - 1); usedSpaceIdx > startIdx; usedSpaceIdx--) {
        maxUsedSpace[usedSpaceIdx] = maxUsedSpace[usedSpaceIdx-1];
        maxUsedSpaceZoneId[usedSpaceIdx] = maxUsedSpaceZoneId[usedSpaceIdx-1];
    }
}

void UpdateMaxUsedSapceStat(uint32 usedSpace, uint32 idx, uint32 *maxUsedSpace,
    uint32 *maxUsedSpaceZoneId, const uint size)
{
    for (uint usedSpaceIdx = 0; usedSpaceIdx < size; usedSpaceIdx++) {
        if (usedSpace > maxUsedSpace[usedSpaceIdx]) {
            if (maxUsedSpaceZoneId[usedSpaceIdx] != idx) {
                moveMaxUsedSpaceItem(usedSpaceIdx, maxUsedSpace, maxUsedSpaceZoneId, size);
            }
            maxUsedSpace[usedSpaceIdx] = usedSpace;
            maxUsedSpaceZoneId[usedSpaceIdx] = idx;
            break;
        }
    }
}
Datum gs_stat_undo(PG_FUNCTION_ARGS)
{
#ifdef ENABLE_MULTIPLE_NODES
    ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("unsupported view in multiple nodes mode.")));
    PG_RETURN_VOID();
#else
    uint32 undoTotalSize = g_instance.undo_cxt.undoTotalSize;
    uint32 limitSize = (uint32)(u_sess->attr.attr_storage.undo_space_limit_size * FORCE_RECYCLE_PERCENT);
    uint32 createdUndoFiles = 0;
    uint32 discardedUndoFiles = 0;
    uint32 zoneUsedCount = 0;
    uint32 maxUsedSpace[TOP_USED_ZONE_NUM] = {0};
    uint32 maxUsedSpaceZoneId[TOP_USED_ZONE_NUM] = {0};

    int rc = 0;
    char textBuffer[STAT_UNDO_BUFFER_SIZE] = {'\0'};
    ReturnSetInfo *rsinfo = (ReturnSetInfo *)fcinfo->resultinfo;
    TupleDesc tupDesc;
    Tuplestorestate *tupstore = NULL;
    MemoryContext per_query_ctx;
    MemoryContext oldcontext;
    if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo)) {
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("set-valued function called in context that cannot accept a set")));
        PG_RETURN_VOID();
    }
    if (!(rsinfo->allowedModes & SFRM_Materialize)) {
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("materialize mode required, but it is not allowed in this context")));
        PG_RETURN_VOID();
    }
    if (get_call_result_type(fcinfo, NULL, &tupDesc) != TYPEFUNC_COMPOSITE) {
        elog(ERROR, "return type must be a row type");
        PG_RETURN_VOID();
    }
    per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
    oldcontext = MemoryContextSwitchTo(per_query_ctx);
    tupstore = tuplestore_begin_heap(true, false, u_sess->attr.attr_memory.work_mem);
    rsinfo->returnMode = SFRM_Materialize;
    rsinfo->setResult = tupstore;
    rsinfo->setDesc = tupDesc;
    MemoryContextSwitchTo(oldcontext);
    for (uint64 idx = 0; idx <= UNDO_ZONE_COUNT - 1; idx++) {
        if (g_instance.undo_cxt.uZones == NULL) {
            break;
        }
        UndoZone *uzone = (undo::UndoZone *)g_instance.undo_cxt.uZones[idx];
        if (uzone == NULL) {
            continue;
        }
        uint32 usedSpace = uzone->UndoSize() * (BLCKSZ / MBYTES_TO_KBYTES) / MBYTES_TO_KBYTES;
        if (usedSpace != 0) {
            UpdateMaxUsedSapceStat(usedSpace, idx, maxUsedSpace, maxUsedSpaceZoneId, TOP_USED_ZONE_NUM);
        }
        zoneUsedCount += 1;
        UndoSpace *undoSpace = uzone->GetUndoSpace();
        UndoSpace *slotSpace = uzone->GetSlotSpace();
        createdUndoFiles += (undoSpace->Tail() / UNDO_FILE_MAXSIZE + slotSpace->Tail() / UNDO_SLOT_FILE_MAXSIZE);
        discardedUndoFiles += (undoSpace->Head() / UNDO_FILE_MAXSIZE + slotSpace->Head() / UNDO_SLOT_FILE_MAXSIZE);
    }
    bool nulls[STAT_UNDO_COLS] = {false};
    Datum values[STAT_UNDO_COLS];
    rc = memset_s(textBuffer, STAT_UNDO_BUFFER_SIZE, 0, STAT_UNDO_BUFFER_SIZE);
    securec_check(rc, "\0", "\0");

    values[ARR_0] = UInt32GetDatum((uint32)zoneUsedCount);
    rc = snprintf_s(textBuffer, sizeof(textBuffer), sizeof(textBuffer) - 1, "%u : %u, %u : %u, %u : %u",
        maxUsedSpaceZoneId[UNDO_TOPUSED], maxUsedSpace[UNDO_TOPUSED], maxUsedSpaceZoneId[UNDO_SECONDUSED],
        maxUsedSpace[UNDO_SECONDUSED], maxUsedSpaceZoneId[UNDO_THIRDUSED], maxUsedSpace[UNDO_THIRDUSED]);
    securec_check_ss(rc, "\0", "\0");
    values[ARR_1] = CStringGetTextDatum(textBuffer);
    values[ARR_2] = UInt32GetDatum((uint32)(undoTotalSize * (BLCKSZ / MBYTES_TO_KBYTES) / MBYTES_TO_KBYTES));
    values[ARR_3] = UInt32GetDatum((uint32)(limitSize * (BLCKSZ / MBYTES_TO_KBYTES) / MBYTES_TO_KBYTES));
    values[ARR_4] = UInt64GetDatum(pg_atomic_read_u64(&g_instance.undo_cxt.globalRecycleXid));
    values[ARR_5] = UInt64GetDatum((uint64)GetGlobalOldestXmin());
    values[ARR_6] = Int64GetDatum((int64)g_instance.undo_cxt.undoChainTotalSize);
    values[ARR_7] = Int64GetDatum((int64)g_instance.undo_cxt.maxChainSize);
    values[ARR_8] = UInt32GetDatum((uint32)createdUndoFiles);
    values[ARR_9] = UInt32GetDatum((uint32)discardedUndoFiles);

    tuplestore_putvalues(tupstore, tupDesc, values, nulls);
    tuplestore_donestoring(tupstore);
    PG_RETURN_VOID();
#endif
}

#ifndef ENABLE_MULTIPLE_NODES
static uint64 UndoSize(UndoSpaceType type)
{
    uint64 used = 0;
    for (auto idx = 0; idx < UNDO_ZONE_COUNT; idx++) {
        undo::UndoZone *uzone = (undo::UndoZone *)g_instance.undo_cxt.uZones[idx];
        if (uzone == NULL) {
            continue;
        }
        if (type == UNDO_LOG_SPACE) {
            used += uzone->UndoSize();
        } else {
            used += uzone->SlotSize();
        }
    }
    return used;
}

static void PutTranslotInfoToTuple(int zoneId, uint32 offset, TransactionSlot *slot, Tuplestorestate *tupstore,
    TupleDesc tupDesc)
{
    if (slot->XactId() != InvalidTransactionId || slot->StartUndoPtr() != INVALID_UNDO_REC_PTR) {
        char textBuffer[STAT_UNDO_LOG_SIZE] = {'\0'};
        bool nulls[PG_STAT_TRANSLOT_META_COLS] = {false};
        Datum values[PG_STAT_TRANSLOT_META_COLS];
        int rc = 0;

        rc = memset_s(textBuffer, STAT_UNDO_LOG_SIZE, 0, STAT_UNDO_LOG_SIZE);
        securec_check(rc, "\0", "\0");
        values[ARR_0] = ObjectIdGetDatum((Oid)zoneId);
        rc = snprintf_s(textBuffer, sizeof(textBuffer), sizeof(textBuffer) - 1, "%016lu", (uint64)slot->XactId());
        securec_check_ss(rc, "\0", "\0");
        values[ARR_1] = CStringGetTextDatum(textBuffer);
        rc = snprintf_s(textBuffer, sizeof(textBuffer), sizeof(textBuffer) - 1, UNDO_REC_PTR_FORMAT,
            UNDO_PTR_GET_OFFSET(slot->StartUndoPtr()));
        securec_check_ss(rc, "\0", "\0");
        values[ARR_2] = CStringGetTextDatum(textBuffer);
        rc = snprintf_s(textBuffer, sizeof(textBuffer), sizeof(textBuffer) - 1, UNDO_REC_PTR_FORMAT,
            UNDO_PTR_GET_OFFSET(slot->EndUndoPtr()));
        securec_check_ss(rc, "\0", "\0");
        values[ARR_3] = CStringGetTextDatum(textBuffer);
        rc = snprintf_s(textBuffer, sizeof(textBuffer), sizeof(textBuffer) - 1, UNDO_REC_PTR_FORMAT, offset);
        securec_check_ss(rc, "\0", "\0");
        values[ARR_4] = CStringGetTextDatum(textBuffer);
        if (TransactionIdDidCommit((uint64)slot->XactId())) {
            values[ARR_5] = COMMITED_STATUS;
        } else if (TransactionIdIsInProgress((uint64)slot->XactId())) {
            values[ARR_5] = INPROCESS_STATUS;
        } else if (slot->NeedRollback()) {
            values[ARR_5] = ABORTING_STATUS;
        } else {
            values[ARR_5] = ABORTED_STATUS;
        }
        tuplestore_putvalues(tupstore, tupDesc, values, nulls);
    }
}

static void GetTranslotFromOneSegFile(int fd, int zoneId, Tuplestorestate *tupstore,
    TupleDesc tupDesc, TransactionId xid, MiniSlot *returnSlot)
{
    TransactionSlot *slot = NULL;
    errno_t rc = EOK;
    off_t seekpos;
    uint32 ret = 0;
    char buffer[BLCKSZ] = {'\0'};

    for (uint32 loop = 0; loop < UNDO_META_SEG_SIZE; loop++) {
        seekpos = (off_t)BLCKSZ * loop;
        lseek(fd, seekpos, SEEK_SET);
        rc = memset_s(buffer, BLCKSZ, 0, BLCKSZ);
        securec_check(rc, "\0", "\0");

        ret = read(fd, (char *)buffer, BLCKSZ);
        if (ret != BLCKSZ) {
            close(fd);
            ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION), errmsg("can't read a block")));
        }

        for (uint32 offset = UNDO_LOG_BLOCK_HEADER_SIZE; offset < BLCKSZ - MAXALIGN(sizeof(TransactionSlot));
            offset += MAXALIGN(sizeof(TransactionSlot))) {
            slot = (TransactionSlot *)(buffer + offset);
            if (TransactionIdIsValid(xid) && slot->XactId() < xid) {
                continue;
            } else if (TransactionIdIsValid(xid) && (uint64)slot->XactId() > xid) {
                return;
            } else if (TransactionIdIsValid(xid) && (uint64)slot->XactId() == xid) {
                if (returnSlot != NULL) {
                    returnSlot->endUndoPtr = slot->EndUndoPtr();
                    returnSlot->startUndoPtr = slot->StartUndoPtr();
                    returnSlot->xactId = slot->XactId();
                    returnSlot->dbId = slot->DbId();
                    return;
                }
                PutTranslotInfoToTuple(zoneId, offset, slot, tupstore, tupDesc);
                return;
            }
            PutTranslotInfoToTuple(zoneId, offset, slot, tupstore, tupDesc);
        }
    }
}

static void GetTranslotFromSegFiles(int zoneId, int segnobegin, int segnoend, Tuplestorestate *tupstore,
    TupleDesc tupDesc, TransactionId xid, MiniSlot *returnSlot)
{
    for (int segcurrent = segnobegin; segcurrent <= segnoend; segcurrent++) {
        errno_t rc = EOK;
        char fileName[100] = {0};
        rc = snprintf_s(fileName, sizeof(fileName), sizeof(fileName) - 1, "undo/permanent/%05X.meta.%07zX", zoneId,
            segcurrent);
        securec_check_ss(rc, "\0", "\0");
        int fd = open(fileName, O_RDONLY | PG_BINARY, S_IRUSR | S_IWUSR);
        Checkfd(fd);
        GetTranslotFromOneSegFile(fd, zoneId, tupstore, tupDesc, xid, returnSlot);
        close(fd);
    }
}

static void ReadTranslotFromDisk(int startIdx, int endIdx, Tuplestorestate *tupstore,
    TupleDesc tupDesc, TransactionId xid, MiniSlot *slot)
{
    int fd = BasicOpenFile(UNDO_META_FILE, O_RDWR | PG_BINARY, S_IRUSR | S_IWUSR);
    for (auto idx = startIdx; idx <= endIdx; idx++) {
        uint32 undoSpaceBegin = 0;
        uint32 undoZoneMetaPageCnt = 0;
        uint32 undoSpaceMetaPageCnt = 0;
        UNDOZONE_META_PAGE_COUNT(PERSIST_ZONE_COUNT, UNDOZONE_COUNT_PER_PAGE, undoZoneMetaPageCnt);
        UNDOZONE_META_PAGE_COUNT(PERSIST_ZONE_COUNT, UNDOSPACE_COUNT_PER_PAGE, undoSpaceMetaPageCnt);
        undoSpaceBegin = (undoZoneMetaPageCnt + undoSpaceMetaPageCnt) * UNDO_META_PAGE_SIZE;
        uint32 readPos = 0;
        UndoSpaceMetaInfo undoSpaceMeta;

        readPos = undoSpaceBegin + (idx / UNDOSPACE_COUNT_PER_PAGE) * UNDO_META_PAGE_SIZE +
            (idx % UNDOSPACE_COUNT_PER_PAGE) * sizeof(UndoSpaceMetaInfo);
        lseek(fd, readPos, SEEK_SET);
        int ret = read(fd, &undoSpaceMeta, sizeof(UndoSpaceMetaInfo));
        if (ret != sizeof(UndoSpaceMetaInfo)) {
            ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION),
                errmsg("Read undo meta file fail, expect size(%lu), real size(%u)", sizeof(UndoSpaceMetaInfo), ret)));
            break;
        }

        int segnobegin = undoSpaceMeta.head / UNDO_SLOT_FILE_MAXSIZE;
        int segnoend = undoSpaceMeta.tail / UNDO_SLOT_FILE_MAXSIZE - 1;

        GetTranslotFromSegFiles(idx, segnobegin, segnoend, tupstore, tupDesc, xid, slot);
    }
    close(fd);
}

static void ReadTranslotFromMemory(int startIdx, int endIdx,
    Tuplestorestate *tupstore, TupleDesc tupDesc, TransactionId xid)
{
    char textBuffer[STAT_UNDO_LOG_SIZE] = {'\0'};
    bool getSlotFlag = false;
    for (auto idx = startIdx; idx <= endIdx; idx++) {
        UndoZone *uzone = (UndoZone *)g_instance.undo_cxt.uZones[idx];
        if (uzone == NULL) {
            continue;
        }
        for (UndoSlotPtr slotPtr = uzone->GetRecycleTSlotPtr(); slotPtr < uzone->GetAllocateTSlotPtr();
            slotPtr = GetNextSlotPtr(slotPtr)) {
            /* Query translot meta info from shared memory. */
            UndoSlotBuffer buf;
            buf.PrepareTransactionSlot(slotPtr);
            TransactionSlot *slot = NULL;
            slot = buf.FetchTransactionSlot(slotPtr);
            bool nulls[PG_STAT_TRANSLOT_META_COLS] = {false};
            Datum values[PG_STAT_TRANSLOT_META_COLS];
            errno_t rc;
            if (TransactionIdIsValid(xid) && slot->XactId() < xid) {
                buf.Release();
                uzone->ReleaseSlotBuffer();
                continue;
            } else if (TransactionIdIsValid(xid) && (uint64)slot->XactId() > xid) {
                buf.Release();
                uzone->ReleaseSlotBuffer();
                return;
            } else if (TransactionIdIsValid(xid) && (uint64)slot->XactId() == xid) {
                getSlotFlag = true;
            }

            rc = memset_s(textBuffer, STAT_UNDO_LOG_SIZE, 0, STAT_UNDO_LOG_SIZE);
            securec_check(rc, "\0", "\0");
            values[ARR_0] = ObjectIdGetDatum((Oid)idx);
            rc = snprintf_s(textBuffer, sizeof(textBuffer), sizeof(textBuffer) - 1, "%016lu", (uint64)slot->XactId());
            securec_check_ss(rc, "\0", "\0");
            values[ARR_1] = CStringGetTextDatum(textBuffer);
            rc = snprintf_s(textBuffer, sizeof(textBuffer), sizeof(textBuffer) - 1, UNDO_REC_PTR_FORMAT,
                UNDO_PTR_GET_OFFSET(slot->StartUndoPtr()));
            securec_check_ss(rc, "\0", "\0");
            values[ARR_2] = CStringGetTextDatum(textBuffer);
            rc = snprintf_s(textBuffer, sizeof(textBuffer), sizeof(textBuffer) - 1, UNDO_REC_PTR_FORMAT,
                UNDO_PTR_GET_OFFSET(slot->EndUndoPtr()));
            securec_check_ss(rc, "\0", "\0");
            values[ARR_3] = CStringGetTextDatum(textBuffer);
            rc = snprintf_s(textBuffer, sizeof(textBuffer), sizeof(textBuffer) - 1, UNDO_REC_PTR_FORMAT,
                UNDO_PTR_GET_OFFSET(slotPtr));
            securec_check_ss(rc, "\0", "\0");
            values[ARR_4] = CStringGetTextDatum(textBuffer);
            if (TransactionIdDidCommit((uint64)slot->XactId())) {
                values[ARR_5] = COMMITED_STATUS;
            } else if (TransactionIdIsInProgress((uint64)slot->XactId())) {
                values[ARR_5] = INPROCESS_STATUS;
            } else if (slot->NeedRollback()) {
                values[ARR_5] = ABORTING_STATUS;
            } else {
                values[ARR_5] = ABORTED_STATUS;
            }
            tuplestore_putvalues(tupstore, tupDesc, values, nulls);
            buf.Release();
            uzone->ReleaseSlotBuffer();
            if (getSlotFlag) {
                return;
            }
        }
    }

    tuplestore_donestoring(tupstore);
}

static void ReadUndoZoneMetaFromShared(int id, TupleDesc *tupleDesc, Tuplestorestate *tupstore)
{
    Assert(tupleDesc != NULL);
    Assert(tupstore != NULL);

    uint32 startIdx = 0;
    uint32 endIdx = 0;
    uint64 used = 0;
    char textBuffer[STAT_UNDO_LOG_SIZE] = {'\0'};
    if (id == INVALID_ZONE_ID) {
        used = UndoSize(UNDO_LOG_SPACE) + UndoSize(UNDO_SLOT_SPACE);
        endIdx = UNDO_ZONE_COUNT - 1;
    } else {
        used = UndoSize(UNDO_LOG_SPACE);
        endIdx = id;
        startIdx = id;
    }

    for (auto idx = startIdx; idx <= endIdx; idx++) {
        Datum values[PG_STAT_USP_PERSIST_META_COLS];
        bool nulls[PG_STAT_USP_PERSIST_META_COLS] = {false};
        UndoZone *uzone = (undo::UndoZone *)g_instance.undo_cxt.uZones[idx];
        if (uzone == NULL) {
            continue;
        }
        values[ARR_0] = ObjectIdGetDatum((Oid)uzone->GetZoneId());
        values[ARR_1] = ObjectIdGetDatum((Oid)uzone->GetPersitentLevel());
        errno_t rc = snprintf_s(textBuffer, sizeof(textBuffer), sizeof(textBuffer) - 1, UNDO_REC_PTR_FORMAT,
            UNDO_PTR_GET_OFFSET(uzone->GetInsertURecPtr()));
        securec_check_ss(rc, "\0", "\0");
        values[ARR_2] = CStringGetTextDatum(textBuffer);
        rc = snprintf_s(textBuffer, sizeof(textBuffer), sizeof(textBuffer) - 1, UNDO_REC_PTR_FORMAT,
            UNDO_PTR_GET_OFFSET(uzone->GetDiscardURecPtr()));
        securec_check_ss(rc, "\0", "\0");
        values[ARR_3] = CStringGetTextDatum(textBuffer);
        rc = snprintf_s(textBuffer, sizeof(textBuffer), sizeof(textBuffer) - 1, UNDO_REC_PTR_FORMAT,
            UNDO_PTR_GET_OFFSET(uzone->GetForceDiscardURecPtr()));
        securec_check_ss(rc, "\0", "\0");
        values[ARR_4] = CStringGetTextDatum(textBuffer);
        rc = snprintf_s(textBuffer, sizeof(textBuffer), sizeof(textBuffer) - 1, UNDO_REC_PTR_FORMAT, used);
        securec_check_ss(rc, "\0", "\0");
        values[ARR_5] = CStringGetTextDatum(textBuffer);
        rc = snprintf_s(textBuffer, sizeof(textBuffer), sizeof(textBuffer) - 1, UNDO_REC_PTR_FORMAT, uzone->GetLSN());
        securec_check_ss(rc, "\0", "\0");
        values[ARR_6] = CStringGetTextDatum(textBuffer);
        tuplestore_putvalues(tupstore, *tupleDesc, values, nulls);
    }

    tuplestore_donestoring(tupstore);
}

void Checkid(const int id, uint32 *startIdx, uint32 *endIdx)
{
    if (id == INVALID_ZONE_ID) {
        *endIdx = UNDO_ZONE_COUNT - 1;
    } else {
        *startIdx = id;
        *endIdx = id;
    }
}

void GetZoneMetaValues(Datum *values, char *textBuffer, UndoZoneMetaInfo undoZoneMeta, uint32 idx, errno_t *rc)
{
    *rc = snprintf_s(textBuffer, STAT_UNDO_LOG_SIZE, STAT_UNDO_LOG_SIZE - 1, UNDO_REC_PTR_FORMAT,
        UNDO_PTR_GET_OFFSET(undoZoneMeta.insertURecPtr));
    securec_check_ss(*rc, "\0", "\0");
    values[ARR_2] = CStringGetTextDatum(textBuffer);
    *rc = snprintf_s(textBuffer, STAT_UNDO_LOG_SIZE, STAT_UNDO_LOG_SIZE - 1, UNDO_REC_PTR_FORMAT,
        UNDO_PTR_GET_OFFSET(undoZoneMeta.discardURecPtr));
    securec_check_ss(*rc, "\0", "\0");
    values[ARR_3] = CStringGetTextDatum(textBuffer);
    *rc = snprintf_s(textBuffer, STAT_UNDO_LOG_SIZE, STAT_UNDO_LOG_SIZE - 1, UNDO_REC_PTR_FORMAT,
        UNDO_PTR_GET_OFFSET(undoZoneMeta.forceDiscardURecPtr));
    securec_check_ss(*rc, "\0", "\0");
    values[ARR_4] = CStringGetTextDatum(textBuffer);
    *rc = snprintf_s(textBuffer, STAT_UNDO_LOG_SIZE, STAT_UNDO_LOG_SIZE - 1, UNDO_REC_PTR_FORMAT, 0);
    securec_check_ss(*rc, "\0", "\0");
    values[ARR_5] = CStringGetTextDatum(textBuffer);
    *rc = snprintf_s(textBuffer, STAT_UNDO_LOG_SIZE, STAT_UNDO_LOG_SIZE - 1, UNDO_REC_PTR_FORMAT, undoZoneMeta.lsn);
    securec_check_ss(*rc, "\0", "\0");
    values[ARR_6] = CStringGetTextDatum(textBuffer);
    values[ARR_7] = ObjectIdGetDatum((Oid)0); // unused
}

static void ReadTransSlotMetaFromShared(int id, TupleDesc *tupleDesc, Tuplestorestate *tupstore)
{
    Assert(tupleDesc != NULL);
    Assert(tupstore != NULL);

    uint32 startIdx = 0;
    uint32 endIdx = 0;
    char textBuffer[STAT_UNDO_LOG_SIZE] = {'\0'};

    if (id == INVALID_ZONE_ID) {
        endIdx = UNDO_ZONE_COUNT - 1;
    } else {
        startIdx = id;
        endIdx = id;
    }

    for (auto idx = startIdx; idx <= endIdx; idx++) {
        bool nulls[PG_STAT_USP_PERSIST_META_COLS] = {false};
        Datum values[PG_STAT_USP_PERSIST_META_COLS];
        UndoZone *uzone = (undo::UndoZone *)g_instance.undo_cxt.uZones[idx];
        if (uzone == NULL) {
            continue;
        }
        values[ARR_0] = ObjectIdGetDatum((Oid)idx);
        values[ARR_1] = ObjectIdGetDatum((Oid)0); // unused
        errno_t rc = snprintf_s(textBuffer, sizeof(textBuffer), sizeof(textBuffer) - 1, UNDO_REC_PTR_FORMAT,
            UNDO_PTR_GET_OFFSET(uzone->GetAllocateTSlotPtr()));
        securec_check_ss(rc, "\0", "\0");
        values[ARR_2] = CStringGetTextDatum(textBuffer);
        rc = snprintf_s(textBuffer, sizeof(textBuffer), sizeof(textBuffer) - 1, UNDO_REC_PTR_FORMAT,
            UNDO_PTR_GET_OFFSET(uzone->GetRecycleTSlotPtr()));
        securec_check_ss(rc, "\0", "\0");
        values[ARR_3] = CStringGetTextDatum(textBuffer);
        rc = snprintf_s(textBuffer, sizeof(textBuffer), sizeof(textBuffer) - 1, "%016lu",
            uzone->GetRecycleXid());
        securec_check_ss(rc, "\0", "\0");
        values[ARR_4] = CStringGetTextDatum(textBuffer);
        rc = snprintf_s(textBuffer, sizeof(textBuffer), sizeof(textBuffer) - 1, "%016lu",
            pg_atomic_read_u64(&g_instance.undo_cxt.globalRecycleXid));
        securec_check_ss(rc, "\0", "\0");
        values[ARR_5] = CStringGetTextDatum(textBuffer);
        rc = snprintf_s(textBuffer, sizeof(textBuffer), sizeof(textBuffer) - 1, "%016lu",
            pg_atomic_read_u64(&g_instance.undo_cxt.globalFrozenXid));
        securec_check_ss(rc, "\0", "\0");
        values[ARR_6] = CStringGetTextDatum(textBuffer);
        values[ARR_7] = ObjectIdGetDatum((Oid)0); // unused
        tuplestore_putvalues(tupstore, *tupleDesc, values, nulls);
    }

    tuplestore_donestoring(tupstore);
}

void GetTransMetaValues(Datum *values, char *textBuffer, UndoZoneMetaInfo undoZoneMeta, uint32 loop, errno_t *rc)
{
    TransactionId recycleXmin;
    GetOldestXminForUndo(&recycleXmin);
    *rc = snprintf_s(textBuffer, STAT_UNDO_LOG_SIZE, STAT_UNDO_LOG_SIZE - 1, UNDO_REC_PTR_FORMAT,
        UNDO_PTR_GET_OFFSET(undoZoneMeta.allocateTSlotPtr));
    securec_check_ss(*rc, "\0", "\0");
    values[ARR_2] = CStringGetTextDatum(textBuffer);
    *rc = snprintf_s(textBuffer, STAT_UNDO_LOG_SIZE, STAT_UNDO_LOG_SIZE - 1, UNDO_REC_PTR_FORMAT,
        UNDO_PTR_GET_OFFSET(undoZoneMeta.recycleTSlotPtr));
    securec_check_ss(*rc, "\0", "\0");
    values[ARR_3] = CStringGetTextDatum(textBuffer);
    *rc = snprintf_s(textBuffer, STAT_UNDO_LOG_SIZE, STAT_UNDO_LOG_SIZE - 1, "%016lu", undoZoneMeta.recycleXid);
    securec_check_ss(*rc, "\0", "\0");
    values[ARR_4] = CStringGetTextDatum(textBuffer);
    *rc = snprintf_s(textBuffer, STAT_UNDO_LOG_SIZE, STAT_UNDO_LOG_SIZE - 1, "%016lu", recycleXmin);
    securec_check_ss(*rc, "\0", "\0");
    values[ARR_5] = CStringGetTextDatum(textBuffer);
    *rc = snprintf_s(textBuffer, STAT_UNDO_LOG_SIZE, STAT_UNDO_LOG_SIZE - 1, "%016lu",
        pg_atomic_read_u64(&g_instance.undo_cxt.globalRecycleXid));
    securec_check_ss(*rc, "\0", "\0");
    values[ARR_6] = CStringGetTextDatum(textBuffer);
    values[ARR_7] = ObjectIdGetDatum((Oid)0); // unused
}

static void ReadUndoMetaFromDisk(int id, TupleDesc *tupleDesc, Tuplestorestate *tupstore, const int type)
{
    Assert(tupleDesc != NULL);
    Assert(tupstore != NULL);
    int ret = 0;
    uint32 startIdx = 0;
    uint32 endIdx = 0;
    char textBuffer[STAT_UNDO_LOG_SIZE] = {'\0'};
    int fd = BasicOpenFile(UNDO_META_FILE, O_RDWR | PG_BINARY, S_IRUSR | S_IWUSR);

    Checkfd(fd);
    Checkid(id, &startIdx, &endIdx);
    for (auto idx = startIdx; idx <= endIdx; idx++) {
        bool nulls[PG_STAT_USP_PERSIST_META_COLS] = {false};
        Datum values[PG_STAT_USP_PERSIST_META_COLS];
        uint32 readPos = 0;
        UndoZoneMetaInfo undoZoneMeta;
        errno_t rc;

        if (idx < PERSIST_ZONE_COUNT) {
            readPos = (idx / UNDOZONE_COUNT_PER_PAGE) * UNDO_META_PAGE_SIZE +
                (idx % UNDOZONE_COUNT_PER_PAGE) * sizeof(UndoZoneMetaInfo);
            lseek(fd, readPos, SEEK_SET);
            ret = read(fd, &undoZoneMeta, sizeof(UndoZoneMetaInfo));
            if (ret != sizeof(UndoZoneMetaInfo)) {
                close(fd);
                ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION), errmsg(
                    "Read undo meta file fail, expect size(%lu), real size(%u)", sizeof(UndoZoneMetaInfo), ret)));
                break;
            }
        } else {
            rc = memset_s(&undoZoneMeta, sizeof(UndoZoneMetaInfo), 0, sizeof(UndoZoneMetaInfo));
            securec_check(rc, "\0", "\0");
        }
        DECLARE_NODE_COUNT();
        GET_UPERSISTENCE_BY_ZONEID((int)idx, nodeCount);
        values[ARR_0] = ObjectIdGetDatum((Oid)idx);
        values[ARR_1] = ObjectIdGetDatum((Oid)upersistence);
        if (type == TYPE_UNDO_ZONE) {
            GetZoneMetaValues(values, textBuffer, undoZoneMeta, idx, &rc);
        } else {
            GetTransMetaValues(values, textBuffer, undoZoneMeta, idx, &rc);
        }
        tuplestore_putvalues(tupstore, *tupleDesc, values, nulls);
    }

    tuplestore_donestoring(tupstore);
    close(fd);
}

static uint64 UndoSpaceSize(UndoSpaceType type)
{
    uint64 used = 0;
    for (auto idx = 0; idx < UNDO_ZONE_COUNT; idx++) {
        UndoSpace *usp;
        if (g_instance.undo_cxt.uZones[idx] == NULL) {
            continue;
        }
        if (type == UNDO_LOG_SPACE) {
            usp = ((UndoZone *)g_instance.undo_cxt.uZones[idx])->GetUndoSpace();
        } else {
            usp = ((UndoZone *)g_instance.undo_cxt.uZones[idx])->GetSlotSpace();
        }
        used += (uint64)usp->Used();
    }
    return used;
}

static void ReadUndoSpaceFromShared(int id, TupleDesc *tupleDesc, Tuplestorestate *tupstore, UndoSpaceType type)
{
    Assert(tupleDesc != NULL);
    Assert(tupstore != NULL);

    uint32 startIdx = 0;
    uint32 endIdx = 0;
    char textBuffer[STAT_UNDO_LOG_SIZE] = {'\0'};
    uint64 used = UndoSpaceSize(type);

    if (id == INVALID_ZONE_ID) {
        used = UndoSpaceSize(UNDO_LOG_SPACE) + UndoSpaceSize(UNDO_SLOT_SPACE);
        endIdx = UNDO_ZONE_COUNT - 1;
    } else {
        used = UndoSpaceSize(type);
        startIdx = id;
        endIdx = id;
    }

    for (auto idx = startIdx; idx <= endIdx; idx++) {
        bool nulls[PG_STAT_USP_PERSIST_META_COLS] = {false};
        Datum values[PG_STAT_USP_PERSIST_META_COLS];
        UndoSpace *usp;
        if (g_instance.undo_cxt.uZones[idx] == NULL) {
            continue;
        }
        if (type == UNDO_LOG_SPACE) {
            usp = ((UndoZone *)g_instance.undo_cxt.uZones[idx])->GetUndoSpace();
        } else {
            usp = ((UndoZone *)g_instance.undo_cxt.uZones[idx])->GetSlotSpace();
        }
        values[ARR_0] = ObjectIdGetDatum((Oid)idx);
        values[ARR_1] = ObjectIdGetDatum((Oid)0); // unused
        errno_t rc = snprintf_s(textBuffer, sizeof(textBuffer), sizeof(textBuffer) - 1, UNDO_REC_PTR_FORMAT,
            UNDO_PTR_GET_OFFSET(usp->Tail()));
        securec_check_ss(rc, "\0", "\0");
        values[ARR_2] = CStringGetTextDatum(textBuffer);
        rc = snprintf_s(textBuffer, sizeof(textBuffer), sizeof(textBuffer) - 1, UNDO_REC_PTR_FORMAT,
            UNDO_PTR_GET_OFFSET(usp->Head()));
        securec_check_ss(rc, "\0", "\0");
        values[ARR_3] = CStringGetTextDatum(textBuffer);
        rc = snprintf_s(textBuffer, sizeof(textBuffer), sizeof(textBuffer) - 1, UNDO_REC_PTR_FORMAT, used);
        securec_check_ss(rc, "\0", "\0");
        values[ARR_4] = CStringGetTextDatum(textBuffer);
        rc = snprintf_s(textBuffer, sizeof(textBuffer), sizeof(textBuffer) - 1, UNDO_REC_PTR_FORMAT, usp->Used());
        securec_check_ss(rc, "\0", "\0");
        values[ARR_5] = CStringGetTextDatum(textBuffer);
        rc = snprintf_s(textBuffer, sizeof(textBuffer), sizeof(textBuffer) - 1, UNDO_REC_PTR_FORMAT, usp->LSN());
        securec_check_ss(rc, "\0", "\0");
        values[ARR_6] = CStringGetTextDatum(textBuffer);
        values[ARR_7] = ObjectIdGetDatum((Oid)0); // unused
        tuplestore_putvalues(tupstore, *tupleDesc, values, nulls);
    }

    tuplestore_donestoring(tupstore);
}

void GetUndoSpaceValues(Datum *values, char *textBuffer, UndoSpaceMetaInfo undoSpaceMeta, uint32 loop, errno_t *rc)
{
    *rc = snprintf_s(textBuffer, STAT_UNDO_LOG_SIZE, STAT_UNDO_LOG_SIZE - 1, UNDO_REC_PTR_FORMAT,
        UNDO_PTR_GET_OFFSET(undoSpaceMeta.tail));
    securec_check_ss(*rc, "\0", "\0");
    values[ARR_2] = CStringGetTextDatum(textBuffer);
    *rc = snprintf_s(textBuffer, STAT_UNDO_LOG_SIZE, STAT_UNDO_LOG_SIZE - 1, UNDO_REC_PTR_FORMAT,
        UNDO_PTR_GET_OFFSET(undoSpaceMeta.head));
    securec_check_ss(*rc, "\0", "\0");
    values[ARR_3] = CStringGetTextDatum(textBuffer);
    *rc = snprintf_s(textBuffer, STAT_UNDO_LOG_SIZE, STAT_UNDO_LOG_SIZE - 1, UNDO_REC_PTR_FORMAT, (uint64)0xFFFF);
    securec_check_ss(*rc, "\0", "\0");
    values[ARR_4] = CStringGetTextDatum(textBuffer);
    *rc = snprintf_s(textBuffer, STAT_UNDO_LOG_SIZE, STAT_UNDO_LOG_SIZE - 1, UNDO_REC_PTR_FORMAT,
        (undoSpaceMeta.tail - undoSpaceMeta.head) / BLCKSZ);
    securec_check_ss(*rc, "\0", "\0");
    values[ARR_5] = CStringGetTextDatum(textBuffer);
    *rc = snprintf_s(textBuffer, STAT_UNDO_LOG_SIZE, STAT_UNDO_LOG_SIZE - 1, UNDO_REC_PTR_FORMAT, undoSpaceMeta.lsn);
    securec_check_ss(*rc, "\0", "\0");
    values[ARR_6] = CStringGetTextDatum(textBuffer);
    values[ARR_7] = ObjectIdGetDatum((Oid)0); // unused
}

static void ReadUndoSpaceFromDisk(int id, TupleDesc *tupleDesc, Tuplestorestate *tupstore, const int type)
{
    Assert(tupleDesc != NULL);
    Assert(tupstore != NULL);
    int ret = 0;
    uint32 startIdx = 0;
    uint32 endIdx = 0;
    uint32 undoSpaceBegin = 0;
    uint32 undoZoneMetaPageCnt = 0;
    uint32 undoSpaceMetaPageCnt = 0;
    char textBuffer[STAT_UNDO_LOG_SIZE] = {'\0'};
    int fd = BasicOpenFile(UNDO_META_FILE, O_RDWR | PG_BINARY, S_IRUSR | S_IWUSR);

    Checkfd(fd);
    Checkid(id, &startIdx, &endIdx);

    /* Seek start position for writing transactionGroup meta. */
    UNDOZONE_META_PAGE_COUNT(PERSIST_ZONE_COUNT, UNDOZONE_COUNT_PER_PAGE, undoZoneMetaPageCnt);
    UNDOZONE_META_PAGE_COUNT(PERSIST_ZONE_COUNT, UNDOSPACE_COUNT_PER_PAGE, undoSpaceMetaPageCnt);
    if (type == UNDO_LOG_SPACE) {
        undoSpaceBegin = undoZoneMetaPageCnt * UNDO_META_PAGE_SIZE;
    } else if (type == UNDO_SLOT_SPACE) {
        undoSpaceBegin = (undoZoneMetaPageCnt + undoSpaceMetaPageCnt) * UNDO_META_PAGE_SIZE;
    }

    for (auto idx = startIdx; idx <= endIdx; idx++) {
        bool nulls[PG_STAT_USP_PERSIST_META_COLS] = {false};
        Datum values[PG_STAT_USP_PERSIST_META_COLS];
        uint32 readPos = 0;
        UndoSpaceMetaInfo undoSpaceMeta;
        errno_t rc;
        if (idx < PERSIST_ZONE_COUNT) {
            readPos = undoSpaceBegin + (idx / UNDOSPACE_COUNT_PER_PAGE) * UNDO_META_PAGE_SIZE +
                (idx % UNDOSPACE_COUNT_PER_PAGE) * sizeof(UndoSpaceMetaInfo);
            lseek(fd, readPos, SEEK_SET);
            ret = read(fd, &undoSpaceMeta, sizeof(UndoSpaceMetaInfo));
            if (ret != sizeof(UndoSpaceMetaInfo)) {
                close(fd);
                ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION), errmsg(
                    "Read undo meta file fail, expect size(%lu), real size(%u)", sizeof(UndoSpaceMetaInfo), ret)));
                break;
            }
        } else {
            rc = memset_s(&undoSpaceMeta, sizeof(UndoSpaceMetaInfo), 0, sizeof(UndoSpaceMetaInfo));
            securec_check(rc, "\0", "\0");
        }
        DECLARE_NODE_COUNT();
        GET_UPERSISTENCE_BY_ZONEID((int)idx, nodeCount);
        values[ARR_0] = ObjectIdGetDatum((Oid)idx);
        values[ARR_1] = ObjectIdGetDatum((Oid)upersistence);
        GetUndoSpaceValues(values, textBuffer, undoSpaceMeta, idx, &rc);
        tuplestore_putvalues(tupstore, *tupleDesc, values, nulls);
    }

    tuplestore_donestoring(tupstore);
    close(fd);
}

bool Checkrsinfo(const ReturnSetInfo *rsinfo)
{
    if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo)) {
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("set-valued function called in context that cannot accept a set")));
        return true;
    }
    if (!(rsinfo->allowedModes & SFRM_Materialize)) {
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("materialize mode required, but it is not allowed in this context")));
        return true;
    }
    return false;
}

bool CheckUndoParameter(int zone_id)
{
    if (zone_id < -1 || zone_id >= UNDO_ZONE_COUNT) {
        elog(ERROR, "Invalid input param");
        return false;
    }
    if (g_instance.undo_cxt.uZones == NULL) {
        elog(ERROR, "Haven't used Ustore");
        return false;
    }
    return true;
}

static void FillUndoMetaZoneInfo(UndoZone* uzone, Datum *values, TupleDesc *tupleDesc, Tuplestorestate *tupstore)
{
    char textBuffer[STAT_UNDO_LOG_SIZE] = {'\0'};

    values[ARR_0] = ObjectIdGetDatum((Oid)uzone->GetZoneId());
    values[ARR_1] = ObjectIdGetDatum((Oid)uzone->GetPersitentLevel());
    
    errno_t rc = snprintf_s(textBuffer, sizeof(textBuffer), sizeof(textBuffer) - 1, UNDO_REC_PTR_UFORMAT,
        UNDO_PTR_GET_OFFSET(uzone->GetInsertURecPtr()));
    securec_check_ss(rc, "\0", "\0");
    values[ARR_2] = CStringGetTextDatum(textBuffer);
    
    rc = snprintf_s(textBuffer, sizeof(textBuffer), sizeof(textBuffer) - 1, UNDO_REC_PTR_UFORMAT,
        UNDO_PTR_GET_OFFSET(uzone->GetDiscardURecPtr()));
    securec_check_ss(rc, "\0", "\0");
    values[ARR_3] = CStringGetTextDatum(textBuffer);
    
    rc = snprintf_s(textBuffer, sizeof(textBuffer), sizeof(textBuffer) - 1, UNDO_REC_PTR_UFORMAT,
        UNDO_PTR_GET_OFFSET(uzone->GetForceDiscardURecPtr()));
    securec_check_ss(rc, "\0", "\0");
    values[ARR_4] = CStringGetTextDatum(textBuffer);
    
    rc = snprintf_s(textBuffer, sizeof(textBuffer), sizeof(textBuffer) - 1, UNDO_REC_PTR_UFORMAT, uzone->GetLSN());
    securec_check_ss(rc, "\0", "\0");
    values[ARR_5] = CStringGetTextDatum(textBuffer);
}

static void FillUndoMetaZoneInfoFromDisk(UndoZoneMetaInfo* undoZoneMeta, Datum *values,
    TupleDesc *tupleDesc, Tuplestorestate *tupstore)
{
    char textBuffer[STAT_UNDO_LOG_SIZE] = {'\0'};

    errno_t rc = snprintf_s(textBuffer, sizeof(textBuffer), sizeof(textBuffer) - 1, UNDO_REC_PTR_UFORMAT,
        UNDO_PTR_GET_OFFSET(undoZoneMeta->insertURecPtr));
    securec_check_ss(rc, "\0", "\0");
    values[ARR_2] = CStringGetTextDatum(textBuffer);

    rc = snprintf_s(textBuffer, sizeof(textBuffer), sizeof(textBuffer) - 1, UNDO_REC_PTR_UFORMAT,
        UNDO_PTR_GET_OFFSET(undoZoneMeta->discardURecPtr));
    securec_check_ss(rc, "\0", "\0");
    values[ARR_3] = CStringGetTextDatum(textBuffer);

    rc = snprintf_s(textBuffer, sizeof(textBuffer), sizeof(textBuffer) - 1, UNDO_REC_PTR_UFORMAT,
        UNDO_PTR_GET_OFFSET(undoZoneMeta->forceDiscardURecPtr));
    securec_check_ss(rc, "\0", "\0");
    values[ARR_4] = CStringGetTextDatum(textBuffer);

    rc = snprintf_s(textBuffer, sizeof(textBuffer), sizeof(textBuffer) - 1, UNDO_REC_PTR_UFORMAT, undoZoneMeta->lsn);
    securec_check_ss(rc, "\0", "\0");
    values[ARR_5] = CStringGetTextDatum(textBuffer);
}

static void FillUndoMetaSpacesInfo(UndoZone* uzone, Datum *values, TupleDesc *tupleDesc, Tuplestorestate *tupstore)
{
    char textBuffer[STAT_UNDO_LOG_SIZE] = {'\0'};
    UndoSpace *undoRecordSpace = uzone->GetUndoSpace();
    UndoSpace *undoSlotSpace = uzone->GetSlotSpace();
    values[ARR_0] = ObjectIdGetDatum((Oid)uzone->GetZoneId());

    errno_t rc = snprintf_s(textBuffer, sizeof(textBuffer), sizeof(textBuffer) - 1, UNDO_REC_PTR_FORMAT,
        UNDO_PTR_GET_OFFSET(undoRecordSpace->Tail()));
    securec_check_ss(rc, "\0", "\0");
    values[ARR_1] = CStringGetTextDatum(textBuffer);
    
    rc = snprintf_s(textBuffer, sizeof(textBuffer), sizeof(textBuffer) - 1, UNDO_REC_PTR_FORMAT,
        UNDO_PTR_GET_OFFSET(undoRecordSpace->Head()));
    securec_check_ss(rc, "\0", "\0");
    values[ARR_2] = CStringGetTextDatum(textBuffer);

    rc = snprintf_s(textBuffer, sizeof(textBuffer), sizeof(textBuffer) - 1,
        UNDO_REC_PTR_UFORMAT, undoRecordSpace->LSN());
    securec_check_ss(rc, "\0", "\0");
    values[ARR_3] = CStringGetTextDatum(textBuffer);
    
    rc = snprintf_s(textBuffer, sizeof(textBuffer), sizeof(textBuffer) - 1, UNDO_REC_PTR_FORMAT,
        UNDO_PTR_GET_OFFSET(undoSlotSpace->Tail()));
    securec_check_ss(rc, "\0", "\0");
    values[ARR_4] = CStringGetTextDatum(textBuffer);
    
    rc = snprintf_s(textBuffer, sizeof(textBuffer), sizeof(textBuffer) - 1, UNDO_REC_PTR_FORMAT,
        UNDO_PTR_GET_OFFSET(undoSlotSpace->Head()));
    securec_check_ss(rc, "\0", "\0");
    values[ARR_5] = CStringGetTextDatum(textBuffer);

    rc = snprintf_s(textBuffer, sizeof(textBuffer), sizeof(textBuffer) - 1, UNDO_REC_PTR_UFORMAT, undoSlotSpace->LSN());
    securec_check_ss(rc, "\0", "\0");
    values[ARR_6] = CStringGetTextDatum(textBuffer);
}

static void FillUndoMetaSpacesInfoFromDisk(UndoSpaceMetaInfo* undoRecordSpaceMeta, UndoSpaceMetaInfo* undoSlotSpaceMeta,
    Datum *values, TupleDesc *tupleDesc, Tuplestorestate *tupstore)
{
    char textBuffer[STAT_UNDO_LOG_SIZE] = {'\0'};

    errno_t rc = snprintf_s(textBuffer, sizeof(textBuffer), sizeof(textBuffer) - 1, UNDO_REC_PTR_FORMAT,
        UNDO_PTR_GET_OFFSET(undoRecordSpaceMeta->tail));
    securec_check_ss(rc, "\0", "\0");
    values[ARR_1] = CStringGetTextDatum(textBuffer);
    
    rc = snprintf_s(textBuffer, sizeof(textBuffer), sizeof(textBuffer) - 1, UNDO_REC_PTR_FORMAT,
        UNDO_PTR_GET_OFFSET(undoRecordSpaceMeta->head));
    securec_check_ss(rc, "\0", "\0");
    values[ARR_2] = CStringGetTextDatum(textBuffer);

    rc = snprintf_s(textBuffer, sizeof(textBuffer), sizeof(textBuffer) - 1,
        UNDO_REC_PTR_UFORMAT, undoRecordSpaceMeta->lsn);
    securec_check_ss(rc, "\0", "\0");
    values[ARR_3] = CStringGetTextDatum(textBuffer);
    
    rc = snprintf_s(textBuffer, sizeof(textBuffer), sizeof(textBuffer) - 1, UNDO_REC_PTR_FORMAT,
        UNDO_PTR_GET_OFFSET(undoSlotSpaceMeta->tail));
    securec_check_ss(rc, "\0", "\0");
    values[ARR_4] = CStringGetTextDatum(textBuffer);
    
    rc = snprintf_s(textBuffer, sizeof(textBuffer), sizeof(textBuffer) - 1, UNDO_REC_PTR_FORMAT,
        UNDO_PTR_GET_OFFSET(undoSlotSpaceMeta->head));
    securec_check_ss(rc, "\0", "\0");
    values[ARR_5] = CStringGetTextDatum(textBuffer);

    rc = snprintf_s(textBuffer, sizeof(textBuffer), sizeof(textBuffer) - 1,
        UNDO_REC_PTR_UFORMAT, undoSlotSpaceMeta->lsn);
    securec_check_ss(rc, "\0", "\0");
    values[ARR_6] = CStringGetTextDatum(textBuffer);
}

static void FillUndoMetaSlotsInfoFromDisk(UndoZoneMetaInfo* undoZoneMeta, Datum *values,
    TupleDesc *tupleDesc, Tuplestorestate *tupstore)
{
    char textBuffer[STAT_UNDO_LOG_SIZE] = {'\0'};

    errno_t rc = snprintf_s(textBuffer, sizeof(textBuffer), sizeof(textBuffer) - 1, UNDO_REC_PTR_UFORMAT,
        UNDO_PTR_GET_OFFSET(undoZoneMeta->allocateTSlotPtr));
    securec_check_ss(rc, "\0", "\0");
    values[ARR_1] = CStringGetTextDatum(textBuffer);
    
    rc = snprintf_s(textBuffer, sizeof(textBuffer), sizeof(textBuffer) - 1, UNDO_REC_PTR_UFORMAT,
        UNDO_PTR_GET_OFFSET(undoZoneMeta->recycleTSlotPtr));
    securec_check_ss(rc, "\0", "\0");
    values[ARR_2] = CStringGetTextDatum(textBuffer);
    
    rc = snprintf_s(textBuffer, sizeof(textBuffer), sizeof(textBuffer) - 1, UNDO_REC_PTR_UFORMAT,
        0);
    securec_check_ss(rc, "\0", "\0");
    values[ARR_3] = CStringGetTextDatum(textBuffer);
    
    rc = snprintf_s(textBuffer, sizeof(textBuffer), sizeof(textBuffer) - 1, UNDO_REC_PTR_UFORMAT,
        pg_atomic_read_u64(&g_instance.undo_cxt.globalFrozenXid));
    securec_check_ss(rc, "\0", "\0");
    values[ARR_4] = CStringGetTextDatum(textBuffer);

    rc = snprintf_s(textBuffer, sizeof(textBuffer), sizeof(textBuffer) - 1, UNDO_REC_PTR_UFORMAT,
        undoZoneMeta->recycleXid);
    securec_check_ss(rc, "\0", "\0");
    values[ARR_5] = CStringGetTextDatum(textBuffer);
    
    rc = snprintf_s(textBuffer, sizeof(textBuffer), sizeof(textBuffer) - 1, UNDO_REC_PTR_UFORMAT,
        pg_atomic_read_u64(&g_instance.undo_cxt.globalRecycleXid));
    securec_check_ss(rc, "\0", "\0");
    values[ARR_6] = CStringGetTextDatum(textBuffer);
}

static void FillUndoMetaSlotsInfo(UndoZone* uzone, Datum *values, TupleDesc *tupleDesc, Tuplestorestate *tupstore)
{
    char textBuffer[STAT_UNDO_LOG_SIZE] = {'\0'};
    values[ARR_0] = ObjectIdGetDatum((Oid)uzone->GetZoneId());

    errno_t rc = snprintf_s(textBuffer, sizeof(textBuffer), sizeof(textBuffer) - 1, UNDO_REC_PTR_UFORMAT,
        UNDO_PTR_GET_OFFSET(uzone->GetAllocateTSlotPtr()));
    securec_check_ss(rc, "\0", "\0");
    values[ARR_1] = CStringGetTextDatum(textBuffer);
    
    rc = snprintf_s(textBuffer, sizeof(textBuffer), sizeof(textBuffer) - 1, UNDO_REC_PTR_UFORMAT,
        UNDO_PTR_GET_OFFSET(uzone->GetRecycleTSlotPtr()));
    securec_check_ss(rc, "\0", "\0");
    values[ARR_2] = CStringGetTextDatum(textBuffer);
    
    rc = snprintf_s(textBuffer, sizeof(textBuffer), sizeof(textBuffer) - 1, UNDO_REC_PTR_UFORMAT,
        uzone->GetFrozenXid());
    securec_check_ss(rc, "\0", "\0");
    values[ARR_3] = CStringGetTextDatum(textBuffer);
    
    rc = snprintf_s(textBuffer, sizeof(textBuffer), sizeof(textBuffer) - 1, UNDO_REC_PTR_UFORMAT,
        pg_atomic_read_u64(&g_instance.undo_cxt.globalFrozenXid));
    securec_check_ss(rc, "\0", "\0");
    values[ARR_4] = CStringGetTextDatum(textBuffer);

    rc = snprintf_s(textBuffer, sizeof(textBuffer), sizeof(textBuffer) - 1, UNDO_REC_PTR_UFORMAT,
        uzone->GetRecycleXid());
    securec_check_ss(rc, "\0", "\0");
    values[ARR_5] = CStringGetTextDatum(textBuffer);
    
    rc = snprintf_s(textBuffer, sizeof(textBuffer), sizeof(textBuffer) - 1, UNDO_REC_PTR_UFORMAT,
        pg_atomic_read_u64(&g_instance.undo_cxt.globalRecycleXid));
    securec_check_ss(rc, "\0", "\0");
    values[ARR_6] = CStringGetTextDatum(textBuffer);
}

static void ReadUndoMetaInfoFromFile(int zone_id, TupleDesc *tupleDesc,
    Tuplestorestate *tupstore, UndoMetaInfoType type)
{
    int ret = 0;
    uint32 startIdx = 0;
    uint32 endIdx = 0;
    int fd = BasicOpenFile(UNDO_META_FILE, O_RDWR | PG_BINARY, S_IRUSR | S_IWUSR);

    Checkfd(fd);
    Checkid(zone_id, &startIdx, &endIdx);
    for (auto idx = startIdx; idx <= endIdx; idx++) {
        bool nulls[PG_STAT_USP_PERSIST_META_COLS] = {false};
        Datum values[PG_STAT_USP_PERSIST_META_COLS];
        uint32 readPos = 0;
        uint32 undoRecordSpaceReadBegin = 0;
        uint32 undoSlotSpaceReadBegin = 0;
        uint32 undoZoneMetaPageCnt = 0;
        uint32 undoSpaceMetaPageCnt = 0;
        UndoZoneMetaInfo undoZoneMeta;
        UndoSpaceMetaInfo undoRecordSpaceMeta;
        UndoSpaceMetaInfo undoSlotSpaceMeta;

        /* Seek start position for writing transactionGroup meta. */
        UNDOZONE_META_PAGE_COUNT(PERSIST_ZONE_COUNT, UNDOZONE_COUNT_PER_PAGE, undoZoneMetaPageCnt);
        UNDOZONE_META_PAGE_COUNT(PERSIST_ZONE_COUNT, UNDOSPACE_COUNT_PER_PAGE, undoSpaceMetaPageCnt);
        if (type == UNDO_META_SPACES) {
            undoRecordSpaceReadBegin = undoZoneMetaPageCnt * UNDO_META_PAGE_SIZE;
            undoSlotSpaceReadBegin = (undoZoneMetaPageCnt + undoSpaceMetaPageCnt) * UNDO_META_PAGE_SIZE;
        }

        if (idx < PERSIST_ZONE_COUNT) {
            if (type == UNDO_META_ZONE || type == UNDO_META_SLOTS) {
                readPos = (idx / UNDOZONE_COUNT_PER_PAGE) * UNDO_META_PAGE_SIZE +
                    (idx % UNDOZONE_COUNT_PER_PAGE) * sizeof(UndoZoneMetaInfo);
                lseek(fd, readPos, SEEK_SET);
                ret = read(fd, &undoZoneMeta, sizeof(UndoZoneMetaInfo));
                if (ret != sizeof(UndoZoneMetaInfo)) {
                    close(fd);
                    ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION), errmsg(
                        "Read undo meta file fail, expect size(%lu), real size(%u)", sizeof(UndoZoneMetaInfo), ret)));
                    break;
                }
                DECLARE_NODE_COUNT();
                GET_UPERSISTENCE_BY_ZONEID((int)idx, nodeCount);
                values[ARR_0] = ObjectIdGetDatum((Oid)idx);
                if (type == UNDO_META_ZONE) {
                    values[ARR_1] = ObjectIdGetDatum((Oid)upersistence);
                    FillUndoMetaZoneInfoFromDisk(&undoZoneMeta, values, tupleDesc, tupstore);
                } else {
                    FillUndoMetaSlotsInfoFromDisk(&undoZoneMeta, values, tupleDesc, tupstore);
                }
            }
            if (type == UNDO_META_SPACES) {
                readPos = undoRecordSpaceReadBegin + (idx / UNDOSPACE_COUNT_PER_PAGE) * UNDO_META_PAGE_SIZE +
                    (idx % UNDOSPACE_COUNT_PER_PAGE) * sizeof(UndoSpaceMetaInfo);
                lseek(fd, readPos, SEEK_SET);
                ret = read(fd, &undoRecordSpaceMeta, sizeof(UndoSpaceMetaInfo));
                if (ret != sizeof(UndoSpaceMetaInfo)) {
                    close(fd);
                    ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION), errmsg(
                        "Read undo meta file fail, expect size(%lu), real size(%u)", sizeof(UndoSpaceMetaInfo), ret)));
                    break;
                }
                readPos = undoSlotSpaceReadBegin + (idx / UNDOSPACE_COUNT_PER_PAGE) * UNDO_META_PAGE_SIZE +
                    (idx % UNDOSPACE_COUNT_PER_PAGE) * sizeof(UndoSpaceMetaInfo);
                lseek(fd, readPos, SEEK_SET);
                ret = read(fd, &undoSlotSpaceMeta, sizeof(UndoSpaceMetaInfo));
                if (ret != sizeof(UndoSpaceMetaInfo)) {
                    close(fd);
                    ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION), errmsg(
                        "Read undo meta file fail, expect size(%lu), real size(%u)", sizeof(UndoSpaceMetaInfo), ret)));
                    break;
                }
                values[ARR_0] = ObjectIdGetDatum((Oid)idx);
                FillUndoMetaSpacesInfoFromDisk(&undoRecordSpaceMeta, &undoSlotSpaceMeta, values, tupleDesc, tupstore);
            }
        } else {
            close(fd);
            return;
        }
        tuplestore_putvalues(tupstore, *tupleDesc, values, nulls);
    }

    tuplestore_donestoring(tupstore);
    close(fd);
}

static void ReadUndoMetaInfoFromMemory(int zone_id, TupleDesc *tupleDesc,
    Tuplestorestate *tupstore, UndoMetaInfoType type)
{
    uint32 startIdx = 0;
    uint32 endIdx = 0;
    if (zone_id == ALL_ZONES) {
        endIdx = UNDO_ZONE_COUNT - 1;
    } else {
        endIdx = zone_id;
        startIdx = zone_id;
    }

    for (auto idx = startIdx; idx <= endIdx; idx++) {
        Datum values[PG_STAT_USP_PERSIST_META_COLS];
        bool nulls[PG_STAT_USP_PERSIST_META_COLS] = {false};
        UndoZone *uzone = (undo::UndoZone *)g_instance.undo_cxt.uZones[idx];
        if (uzone == NULL) {
            continue;
        }
        switch (type) {
            case UNDO_META_ZONE:
                FillUndoMetaZoneInfo(uzone, values, tupleDesc, tupstore);
                break;
            case UNDO_META_SPACES:
                FillUndoMetaSpacesInfo(uzone, values, tupleDesc, tupstore);
                break;
            case UNDO_META_SLOTS:
                FillUndoMetaSlotsInfo(uzone, values, tupleDesc, tupstore);
                break;
            default:
                elog(ERROR, "Invalid input param");
                break;
        }
        tuplestore_putvalues(tupstore, *tupleDesc, values, nulls);
    }
    tuplestore_donestoring(tupstore);
}
#endif

Datum gs_undo_meta_dump_zone(PG_FUNCTION_ARGS)
{
#ifdef ENABLE_MULTIPLE_NODES
    ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("unsupported view in multiple nodes mode.")));
    PG_RETURN_VOID();
#else
    int zone_id = PG_GETARG_INT32(0);
    bool read_memory = PG_GETARG_INT32(1);

    ReturnSetInfo *rsinfo = (ReturnSetInfo *)fcinfo->resultinfo;
    TupleDesc tupDesc;
    Tuplestorestate *tupstore = NULL;
    MemoryContext per_query_ctx;
    MemoryContext oldcontext;

    if (!CheckUndoParameter(zone_id) || Checkrsinfo(rsinfo)) {
        PG_RETURN_VOID();
    }

    if (get_call_result_type(fcinfo, NULL, &tupDesc) != TYPEFUNC_COMPOSITE) {
        elog(ERROR, "return type must be a row type");
        PG_RETURN_VOID();
    }

    per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
    oldcontext = MemoryContextSwitchTo(per_query_ctx);
    tupstore = tuplestore_begin_heap(true, false, u_sess->attr.attr_memory.work_mem);
    rsinfo->returnMode = SFRM_Materialize;
    rsinfo->setResult = tupstore;
    rsinfo->setDesc = tupDesc;
    MemoryContextSwitchTo(oldcontext);

    if (read_memory) {
        ReadUndoMetaInfoFromMemory(zone_id, &tupDesc, tupstore, UNDO_META_ZONE);
    } else {
        ReadUndoMetaInfoFromFile(zone_id, &tupDesc, tupstore, UNDO_META_ZONE);
    }
    PG_RETURN_VOID();
#endif
}

Datum gs_undo_meta_dump_spaces(PG_FUNCTION_ARGS)
{
#ifdef ENABLE_MULTIPLE_NODES
    ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("unsupported view in multiple nodes mode.")));
    PG_RETURN_VOID();
#else
    int zone_id = PG_GETARG_INT32(0);
    bool read_memory = PG_GETARG_INT32(1);

    ReturnSetInfo *rsinfo = (ReturnSetInfo *)fcinfo->resultinfo;
    TupleDesc tupDesc;
    Tuplestorestate *tupstore = NULL;
    MemoryContext per_query_ctx;
    MemoryContext oldcontext;

    if (g_instance.undo_cxt.uZones == NULL) {
        elog(ERROR, "Haven't used Ustore");
    }

    if (zone_id < -1 || zone_id >= UNDO_ZONE_COUNT) {
        elog(ERROR, "Invalid input param");
    }

    if (Checkrsinfo(rsinfo)) {
        PG_RETURN_VOID();
    }

    if (get_call_result_type(fcinfo, NULL, &tupDesc) != TYPEFUNC_COMPOSITE) {
        elog(ERROR, "return type must be a row type");
        PG_RETURN_VOID();
    }

    per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
    oldcontext = MemoryContextSwitchTo(per_query_ctx);
    tupstore = tuplestore_begin_heap(true, false, u_sess->attr.attr_memory.work_mem);
    rsinfo->returnMode = SFRM_Materialize;
    rsinfo->setResult = tupstore;
    rsinfo->setDesc = tupDesc;
    MemoryContextSwitchTo(oldcontext);
    
    if (read_memory) {
        ReadUndoMetaInfoFromMemory(zone_id, &tupDesc, tupstore, UNDO_META_SPACES);
    } else {
        ReadUndoMetaInfoFromFile(zone_id, &tupDesc, tupstore, UNDO_META_SPACES);
    }

    PG_RETURN_VOID();
#endif
}

Datum gs_undo_meta_dump_slot(PG_FUNCTION_ARGS)
{
#ifdef ENABLE_MULTIPLE_NODES
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("unsupported view in multiple nodes mode.")));
        PG_RETURN_VOID();
#else
        int zone_id = PG_GETARG_INT32(0);
        bool read_memory = PG_GETARG_INT32(1);
    
        ReturnSetInfo *rsinfo = (ReturnSetInfo *)fcinfo->resultinfo;
        TupleDesc tupDesc;
        Tuplestorestate *tupstore = NULL;
        MemoryContext per_query_ctx;
        MemoryContext oldcontext;

        if (!CheckUndoParameter(zone_id) || Checkrsinfo(rsinfo)) {
            PG_RETURN_VOID();
        }

        if (get_call_result_type(fcinfo, NULL, &tupDesc) != TYPEFUNC_COMPOSITE) {
            elog(ERROR, "return type must be a row type");
            PG_RETURN_VOID();
        }

        per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
        oldcontext = MemoryContextSwitchTo(per_query_ctx);
        tupstore = tuplestore_begin_heap(true, false, u_sess->attr.attr_memory.work_mem);
        rsinfo->returnMode = SFRM_Materialize;
        rsinfo->setResult = tupstore;
        rsinfo->setDesc = tupDesc;
        MemoryContextSwitchTo(oldcontext);

        if (read_memory) {
            ReadUndoMetaInfoFromMemory(zone_id, &tupDesc, tupstore, UNDO_META_SLOTS);
        } else {
            ReadUndoMetaInfoFromFile(zone_id, &tupDesc, tupstore, UNDO_META_SLOTS);
        }

        PG_RETURN_VOID();
#endif
}

Datum gs_undo_translot_dump_slot(PG_FUNCTION_ARGS)
{
#ifdef ENABLE_MULTIPLE_NODES
    ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("unsupported view in multiple nodes mode.")));
    PG_RETURN_VOID();
#else
    int zone_id = PG_GETARG_INT32(0);
    bool read_memory = PG_GETARG_INT32(1);
    ReturnSetInfo *rsinfo = (ReturnSetInfo *)fcinfo->resultinfo;
    TupleDesc tupDesc;
    Tuplestorestate *tupstore = NULL;
    MemoryContext per_query_ctx;
    MemoryContext oldcontext;

    if (!CheckUndoParameter(zone_id) || Checkrsinfo(rsinfo)) {
        PG_RETURN_VOID();
    }
    
    if (get_call_result_type(fcinfo, NULL, &tupDesc) != TYPEFUNC_COMPOSITE) {
        elog(ERROR, "return type must be a row type");
        PG_RETURN_VOID();
    }

    per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
    oldcontext = MemoryContextSwitchTo(per_query_ctx);
    tupstore = tuplestore_begin_heap(true, false, u_sess->attr.attr_memory.work_mem);
    rsinfo->returnMode = SFRM_Materialize;
    rsinfo->setResult = tupstore;
    rsinfo->setDesc = tupDesc;
    MemoryContextSwitchTo(oldcontext);

    uint32 startIdx = 0;
    uint32 endIdx = 0;

    if (zone_id == INVALID_ZONE_ID) {
        endIdx = PERSIST_ZONE_COUNT - 1;
    } else {
        startIdx = zone_id;
        endIdx = zone_id;
    }

    if (read_memory) {
        ReadTranslotFromMemory(startIdx, endIdx, tupstore, tupDesc, InvalidTransactionId);
    } else {
        ReadTranslotFromDisk(startIdx, endIdx, tupstore, tupDesc, InvalidTransactionId, NULL);
    }

    PG_RETURN_VOID();
#endif
}

Datum gs_undo_translot_dump_xid(PG_FUNCTION_ARGS)
{
#ifdef ENABLE_MULTIPLE_NODES
    ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("unsupported view in multiple nodes mode.")));
    PG_RETURN_VOID();
#else

    TransactionId xid = (TransactionId)PG_GETARG_TRANSACTIONID(0);
    bool read_memory = PG_GETARG_INT32(1);
    
    if (!TransactionIdIsValid(xid)) {
        elog(ERROR, "xid is invalid");
        PG_RETURN_VOID();
    }
    ReturnSetInfo *rsinfo = (ReturnSetInfo *)fcinfo->resultinfo;
    TupleDesc tupDesc;
    Tuplestorestate *tupstore = NULL;
    MemoryContext per_query_ctx;
    MemoryContext oldcontext;

    if (g_instance.undo_cxt.uZones == NULL) {
        elog(ERROR, "Haven't used Ustore");
    }

    if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo)) {
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("set-valued function called in context that cannot accept a set")));
        PG_RETURN_VOID();
    }
    if (!(rsinfo->allowedModes & SFRM_Materialize)) {
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("materialize mode required, but it is not allowed in this context")));
        PG_RETURN_VOID();
    }
    if (get_call_result_type(fcinfo, NULL, &tupDesc) != TYPEFUNC_COMPOSITE) {
        elog(ERROR, "return type must be a row type");
        PG_RETURN_VOID();
    }

    per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
    oldcontext = MemoryContextSwitchTo(per_query_ctx);
    tupstore = tuplestore_begin_heap(true, false, u_sess->attr.attr_memory.work_mem);
    rsinfo->returnMode = SFRM_Materialize;
    rsinfo->setResult = tupstore;
    rsinfo->setDesc = tupDesc;
    MemoryContextSwitchTo(oldcontext);

    uint32 startIdx = 0;
    uint32 endIdx = PERSIST_ZONE_COUNT - 1;

    if (read_memory) {
        ReadTranslotFromMemory(startIdx, endIdx, tupstore, tupDesc, xid);
    } else {
        ReadTranslotFromDisk(startIdx, endIdx, tupstore, tupDesc, xid, NULL);
    }

    PG_RETURN_VOID();
#endif
}

Datum gs_undo_dump_record(PG_FUNCTION_ARGS)
{
#ifdef ENABLE_MULTIPLE_NODES
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("unsupported view in multiple nodes mode.")));
        PG_RETURN_VOID();
#else
        UndoRecPtr undoptr = DatumGetUInt64(PG_GETARG_DATUM(0));
        ReturnSetInfo *rsinfo = (ReturnSetInfo *)fcinfo->resultinfo;
        TupleDesc tupDesc;
        Tuplestorestate *tupstore = NULL;
        MemoryContext per_query_ctx;
        MemoryContext oldcontext;
    
        if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo)) {
            ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("set-valued function called in context that cannot accept a set")));
            PG_RETURN_VOID();
        }
        if (!(rsinfo->allowedModes & SFRM_Materialize)) {
            ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("materialize mode required, but it is not allowed in this context")));
            PG_RETURN_VOID();
        }
        if (get_call_result_type(fcinfo, NULL, &tupDesc) != TYPEFUNC_COMPOSITE) {
            elog(ERROR, "return type must be a row type");
            PG_RETURN_VOID();
        }
    
        per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
        oldcontext = MemoryContextSwitchTo(per_query_ctx);
        tupstore = tuplestore_begin_heap(true, false, u_sess->attr.attr_memory.work_mem);
        rsinfo->returnMode = SFRM_Materialize;
        rsinfo->setResult = tupstore;
        rsinfo->setDesc = tupDesc;
        MemoryContextSwitchTo(oldcontext);
    
        ParseUndoRecord(undoptr, tupstore, tupDesc);
        tuplestore_donestoring(tupstore);
    
        PG_RETURN_VOID();
#endif
}

Datum gs_undo_dump_xid(PG_FUNCTION_ARGS)
{
#ifdef ENABLE_MULTIPLE_NODES
    ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("unsupported view in multiple nodes mode.")));
    PG_RETURN_VOID();
#else
    TransactionId xid = (TransactionId)PG_GETARG_TRANSACTIONID(0);
    if (!TransactionIdIsValid(xid)) {
        elog(ERROR, "xid is invalid");
        PG_RETURN_VOID();
    }

    ReturnSetInfo *rsinfo = (ReturnSetInfo *)fcinfo->resultinfo;
    TupleDesc tupDesc;
    Tuplestorestate *tupstore = NULL;
    MemoryContext per_query_ctx;
    MemoryContext oldcontext;

    if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo)) {
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("set-valued function called in context that cannot accept a set")));
        PG_RETURN_VOID();
    }
    if (!(rsinfo->allowedModes & SFRM_Materialize)) {
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("materialize mode required, but it is not allowed in this context")));
        PG_RETURN_VOID();
    }
    if (get_call_result_type(fcinfo, NULL, &tupDesc) != TYPEFUNC_COMPOSITE) {
        elog(ERROR, "return type must be a row type");
        PG_RETURN_VOID();
    }

    per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
    oldcontext = MemoryContextSwitchTo(per_query_ctx);
    tupstore = tuplestore_begin_heap(true, false, u_sess->attr.attr_memory.work_mem);
    rsinfo->returnMode = SFRM_Materialize;
    rsinfo->setResult = tupstore;
    rsinfo->setDesc = tupDesc;
    MemoryContextSwitchTo(oldcontext);

    uint32 startIdx = 0;
    uint32 endIdx = PERSIST_ZONE_COUNT - 1;
    MiniSlot miniSlot;
    miniSlot.dbId = 0;
    miniSlot.endUndoPtr = INVALID_UNDO_REC_PTR;
    miniSlot.startUndoPtr = INVALID_UNDO_REC_PTR;
    miniSlot.xactId = InvalidTransactionId;

    ReadTranslotFromDisk(startIdx, endIdx, tupstore, tupDesc, xid, &miniSlot);
    if (!TransactionIdIsValid(miniSlot.xactId) ||
        miniSlot.endUndoPtr == INVALID_UNDO_REC_PTR) {
        PG_RETURN_VOID();
    }
    UndoRecPtr undoptr = GetPrevUrp(miniSlot.endUndoPtr);
    ParseUndoRecord(undoptr, tupstore, tupDesc);
    tuplestore_donestoring(tupstore);

    PG_RETURN_VOID();
#endif
}

Datum gs_undo_meta(PG_FUNCTION_ARGS)
{
#ifdef ENABLE_MULTIPLE_NODES
    ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("unsupported view in multiple nodes mode.")));
    PG_RETURN_VOID();
#else
    int type = PG_GETARG_INT32(0);         // Indicate meta data type(0:undozone, 1:group, 2:undoSpace, 3:slotSpace)
    int id = PG_GETARG_INT32(1);           // zoneId (-1 represents all)
    int metaLocation = PG_GETARG_INT32(2); // meta location (0:shared , 1:disk)
    ReturnSetInfo *rsinfo = (ReturnSetInfo *)fcinfo->resultinfo;
    TupleDesc tupDesc;
    Tuplestorestate *tupstore = NULL;
    MemoryContext per_query_ctx;
    MemoryContext oldcontext;

    if (g_instance.undo_cxt.uZones == NULL) {
        elog(ERROR, "Haven't used Ustore");
    }

    if (id < -1 || id >= UNDO_ZONE_COUNT || (metaLocation != 0 && metaLocation != 1)) {
        elog(ERROR, "Invalid input param");
    }

    if (Checkrsinfo(rsinfo)) {
        PG_RETURN_VOID();
    }

    if (get_call_result_type(fcinfo, NULL, &tupDesc) != TYPEFUNC_COMPOSITE) {
        elog(ERROR, "return type must be a row type");
        PG_RETURN_VOID();
    }

    per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
    oldcontext = MemoryContextSwitchTo(per_query_ctx);
    tupstore = tuplestore_begin_heap(true, false, u_sess->attr.attr_memory.work_mem);
    rsinfo->returnMode = SFRM_Materialize;
    rsinfo->setResult = tupstore;
    rsinfo->setDesc = tupDesc;
    MemoryContextSwitchTo(oldcontext);

    switch (type) {
        /* Undospace meta info. */
        case TYPE_UNDO_ZONE:
            if (metaLocation == 0) {
                ReadUndoZoneMetaFromShared(id, &tupDesc, tupstore);
            } else {
                ReadUndoMetaFromDisk(id, &tupDesc, tupstore, TYPE_UNDO_ZONE);
            }
            break;
        /* TransactionGroup meta info. */
        case TYPE_GROUP:
            if (metaLocation == 0) {
                ReadTransSlotMetaFromShared(id, &tupDesc, tupstore);
            } else {
                ReadUndoMetaFromDisk(id, &tupDesc, tupstore, TYPE_GROUP);
            }
            break;
        case TYPE_UNDO_SPACE:
            if (metaLocation == 0) {
                ReadUndoSpaceFromShared(id, &tupDesc, tupstore, UNDO_LOG_SPACE);
            } else {
                ReadUndoSpaceFromDisk(id, &tupDesc, tupstore, UNDO_LOG_SPACE);
            }
            break;
        case TYPE_SLOT_SPACE:
            if (metaLocation == 0) {
                ReadUndoSpaceFromShared(id, &tupDesc, tupstore, UNDO_SLOT_SPACE);
            } else {
                ReadUndoSpaceFromDisk(id, &tupDesc, tupstore, UNDO_SLOT_SPACE);
            }
            break;
        default:
            elog(ERROR, "Invalid input param");
            break;
    }

    PG_RETURN_VOID();
#endif
}

Datum gs_undo_record(PG_FUNCTION_ARGS)
{
#ifdef ENABLE_MULTIPLE_NODES
    ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("unsupported view in multiple nodes mode.")));
    PG_RETURN_VOID();
#else
    UndoRecPtr undoptr = DatumGetUInt64(PG_GETARG_DATUM(0));
    ReturnSetInfo *rsinfo = (ReturnSetInfo *)fcinfo->resultinfo;
    TupleDesc tupDesc;
    Tuplestorestate *tupstore = NULL;
    MemoryContext per_query_ctx;
    MemoryContext oldcontext;

    if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo)) {
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("set-valued function called in context that cannot accept a set")));
        PG_RETURN_VOID();
    }
    if (!(rsinfo->allowedModes & SFRM_Materialize)) {
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("materialize mode required, but it is not allowed in this context")));
        PG_RETURN_VOID();
    }
    if (get_call_result_type(fcinfo, NULL, &tupDesc) != TYPEFUNC_COMPOSITE) {
        elog(ERROR, "return type must be a row type");
        PG_RETURN_VOID();
    }

    per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
    oldcontext = MemoryContextSwitchTo(per_query_ctx);
    tupstore = tuplestore_begin_heap(true, false, u_sess->attr.attr_memory.work_mem);
    rsinfo->returnMode = SFRM_Materialize;
    rsinfo->setResult = tupstore;
    rsinfo->setDesc = tupDesc;
    MemoryContextSwitchTo(oldcontext);

    ParseUndoRecord(undoptr, tupstore, tupDesc);
    tuplestore_donestoring(tupstore);

    PG_RETURN_VOID();
#endif
}

Datum gs_undo_translot(PG_FUNCTION_ARGS)
{
#ifdef ENABLE_MULTIPLE_NODES
    ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("unsupported view in multiple nodes mode.")));
    PG_RETURN_VOID();
#else
    int type = PG_GETARG_INT32(0);     // Indicates query meta from share memory or persistent file
    int32 zoneId = PG_GETARG_INT32(1); // zone id
    ReturnSetInfo *rsinfo = (ReturnSetInfo *)fcinfo->resultinfo;
    TupleDesc tupDesc;
    Tuplestorestate *tupstore = NULL;
    MemoryContext per_query_ctx;
    MemoryContext oldcontext;

    if (g_instance.undo_cxt.uZones == NULL) {
        elog(ERROR, "Haven't used Ustore");
    }

    if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo)) {
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("set-valued function called in context that cannot accept a set")));
        PG_RETURN_VOID();
    }
    if (!(rsinfo->allowedModes & SFRM_Materialize)) {
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("materialize mode required, but it is not allowed in this context")));
        PG_RETURN_VOID();
    }
    if (get_call_result_type(fcinfo, NULL, &tupDesc) != TYPEFUNC_COMPOSITE) {
        elog(ERROR, "return type must be a row type");
        PG_RETURN_VOID();
    }

    per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
    oldcontext = MemoryContextSwitchTo(per_query_ctx);
    tupstore = tuplestore_begin_heap(true, false, u_sess->attr.attr_memory.work_mem);
    rsinfo->returnMode = SFRM_Materialize;
    rsinfo->setResult = tupstore;
    rsinfo->setDesc = tupDesc;
    MemoryContextSwitchTo(oldcontext);

    if (type == 0 || type == 1) {
        if (zoneId < -1 || zoneId >= UNDO_ZONE_COUNT) {
            ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("Zone id is invalid %d", zoneId)));
            PG_RETURN_VOID();
        }
    } else {
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("Invalid input param")));
            PG_RETURN_VOID();
    }

    uint32 startIdx = 0;
    uint32 endIdx = 0;

    if (zoneId == INVALID_ZONE_ID) {
        endIdx = PERSIST_ZONE_COUNT - 1;
    } else {
        startIdx = zoneId;
        endIdx = zoneId;
    }

    if (type == 1) {
        ReadTranslotFromDisk(startIdx, endIdx, tupstore, tupDesc, InvalidTransactionId, NULL);
    } else {
        ReadTranslotFromMemory(startIdx, endIdx, tupstore, tupDesc, InvalidTransactionId);
    }

    PG_RETURN_VOID();
#endif
}

Datum gs_index_verify(PG_FUNCTION_ARGS)
{
#ifdef ENABLE_MULTIPLE_NODES
    ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("unsupported view in multiple nodes mode.")));
    PG_RETURN_VOID();
#else
    Oid relationOid = PG_GETARG_OID(0);
    uint32 blkno = (uint32)PG_GETARG_OID(1);
    ReturnSetInfo *rsinfo = (ReturnSetInfo *)fcinfo->resultinfo;
    TupleDesc tupDesc;
    Tuplestorestate *tupstore = NULL;
    MemoryContext per_query_ctx;
    MemoryContext oldcontext;

    if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo)) {
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("set-valued function called in context that cannot accept a set")));
        PG_RETURN_VOID();
    }
    if (!(rsinfo->allowedModes & SFRM_Materialize)) {
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("materialize mode required, but it is not allowed in this context")));
        PG_RETURN_VOID();
    }
    if (get_call_result_type(fcinfo, NULL, &tupDesc) != TYPEFUNC_COMPOSITE) {
        elog(ERROR, "return type must be a row type");
        PG_RETURN_VOID();
    }

    per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
    oldcontext = MemoryContextSwitchTo(per_query_ctx);
    tupstore = tuplestore_begin_heap(true, false, u_sess->attr.attr_memory.work_mem);
    rsinfo->returnMode = SFRM_Materialize;
    rsinfo->setResult = tupstore;
    rsinfo->setDesc = tupDesc;
    MemoryContextSwitchTo(oldcontext);

    if (relationOid == InvalidOid) {
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("relation oid is invalid %d", relationOid)));
        PG_RETURN_VOID();
    }

    Relation relation = relation_open(relationOid, AccessShareLock);
    Assert(relation->rd_isvalid);
    if (!RelationIsUstoreIndex(relation)) {
        relation_close(relation, AccessShareLock);
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmodule(MOD_USTORE),
                errmsg("Relaiton corresponding to oid(%u) is not ubtree index.", relationOid),
                errdetail("N/A"),
                errcause("feature not supported"),
                erraction("check defination of this rel")));
        PG_RETURN_VOID();
    }

    if (blkno == InvalidBlockNumber) {
        relation_close(relation, AccessShareLock);
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmodule(MOD_USTORE),
                errmsg("Block number(%u) is invalid.", blkno),
                errdetail("N/A"),
                errcause("Invalid block number."),
                erraction("Check the blkno parameter.")));
        PG_RETURN_VOID();
    } else if (blkno == 0) {
        // Verfiy whole index tree
        UBTreeVerifyIndex(relation, &tupDesc, tupstore, UBTREE_VERIFY_OUTPUT_PARAM_CNT);
    } else {
        // Verify single index page
        uint32 verifyRes;
        Page page = NULL;
        bool nulls[UBTREE_VERIFY_OUTPUT_PARAM_CNT] = {false};
        Datum values[UBTREE_VERIFY_OUTPUT_PARAM_CNT];
        BTScanInsert cmpKeys = UBTreeMakeScanKey(relation, NULL);
        Buffer buf = _bt_getbuf(relation, blkno, BT_READ);
        if (BufferIsInvalid(buf)) {
            relation_close(relation, AccessShareLock);
            ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("Block number %u is invalid", blkno)));
            PG_RETURN_VOID();
        }

        page = (Page)BufferGetPage(buf);
        verifyRes = UBTreeVerifyOnePage(relation, page, cmpKeys, NULL);
        _bt_relbuf(relation, buf);
        pfree(cmpKeys);
        values[ARR_0] = CStringGetTextDatum(UBTGetVerifiedPageTypeStr((uint32)VERIFY_MAIN_PAGE));
        values[ARR_1] = ObjectIdGetDatum((Oid)blkno);
        values[ARR_2] = CStringGetTextDatum(UBTGetVerifiedResultStr(verifyRes));
        tuplestore_putvalues(tupstore, tupDesc, values, nulls);
    }

    tuplestore_donestoring(tupstore);
    relation_close(relation, AccessShareLock);
    PG_RETURN_VOID();
#endif
}

Datum gs_index_recycle_queue(PG_FUNCTION_ARGS)
{
#ifdef ENABLE_MULTIPLE_NODES
    ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("unsupported view in multiple nodes mode.")));
    PG_RETURN_VOID();
#else
    Oid relationOid = PG_GETARG_OID(0);
    uint32 type = (uint32)PG_GETARG_OID(1);
    uint32 blkno = (uint32)PG_GETARG_OID(2);
    ReturnSetInfo *rsinfo = (ReturnSetInfo *)fcinfo->resultinfo;
    TupleDesc tupDesc;
    Tuplestorestate *tupstore = NULL;
    MemoryContext per_query_ctx;
    MemoryContext oldcontext;

    if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo)) {
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("set-valued function called in context that cannot accept a set")));
        PG_RETURN_VOID();
    }
    if (!(rsinfo->allowedModes & SFRM_Materialize)) {
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("materialize mode required, but it is not allowed in this context")));
        PG_RETURN_VOID();
    }
    if (get_call_result_type(fcinfo, NULL, &tupDesc) != TYPEFUNC_COMPOSITE) {
        elog(ERROR, "return type must be a row type");
        PG_RETURN_VOID();
    }

    per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
    oldcontext = MemoryContextSwitchTo(per_query_ctx);
    tupstore = tuplestore_begin_heap(true, false, u_sess->attr.attr_memory.work_mem);
    rsinfo->returnMode = SFRM_Materialize;
    rsinfo->setResult = tupstore;
    rsinfo->setDesc = tupDesc;
    MemoryContextSwitchTo(oldcontext);

    if (relationOid == InvalidOid) {
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("relation oid is invalid %u", relationOid)));
        PG_RETURN_VOID();
    }
    if (type > RECYCLE_NONE_FORK) {
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("Invalid parse type %u", type)));
        PG_RETURN_VOID();
    }

    Relation relation = relation_open(relationOid, AccessShareLock);
    Assert(relation->rd_isvalid);
    if (!RelationIsUstoreIndex(relation)) {
        relation_close(relation, AccessShareLock);
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmodule(MOD_USTORE),
                errmsg("Relaiton correspondisng to oid(%u) is not ubtree index.", relationOid),
                errdetail("N/A"),
                errcause("feature not supported"),
                erraction("check defination of this rel")));
        PG_RETURN_VOID();
    }

    if (type == RECYCLE_NONE_FORK) {
        /*
         * Check blkno first.Blkno 0 is the meta page of ubtree index, no need dump.
         * And blkno -1 is invalid block number.
         */
        if (blkno == 0 || blkno == InvalidBlockNumber) {
            relation_close(relation, AccessShareLock);
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmodule(MOD_USTORE),
                    errmsg("Block number(%u) is invalid.", blkno),
                    errdetail("N/A"),
                    errcause("Invalid block number."),
                    erraction("Check the blkno parameter.")));
            PG_RETURN_VOID();
        }
        Buffer buf = _bt_getbuf(relation, blkno, BT_READ);
        if (BufferIsInvalid(buf)) {
            relation_close(relation, AccessShareLock);
            ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("Block number %u is invalid", blkno)));
            PG_RETURN_VOID();
        }
        (void) UBTreeRecycleQueuePageDump(relation, buf, true, &tupDesc, tupstore, UBTREE_RECYCLE_OUTPUT_PARAM_CNT);
        _bt_relbuf(relation, buf);
    } else {
        UBTreeDumpRecycleQueueFork(relation, (UBTRecycleForkNumber)type, &tupDesc, tupstore,
            UBTREE_RECYCLE_OUTPUT_PARAM_CNT);
    }

    relation_close(relation, AccessShareLock);
    // call verify interface in index module
    tuplestore_donestoring(tupstore);
    PG_RETURN_VOID();
#endif
}

Datum gs_undo_dump_parsepage_mv(PG_FUNCTION_ARGS)
{
#ifdef ENABLE_MULTIPLE_NODES
    ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("unsupported view in multiple nodes mode.")));
    PG_RETURN_VOID();
#else
    if (ENABLE_DSS) {
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("unsupported view when enable dss.")));
        PG_RETURN_VOID();
    }

    /* check user's right */
    const char fName[MAXFNAMELEN] = "gs_undo_dump_parsepage_mv";
    CheckUser(fName);

    /* read in parameters */
    char *path = text_to_cstring(PG_GETARG_TEXT_P(0));
    int64 blockno = PG_GETARG_INT64(1);
    char *relationType = text_to_cstring(PG_GETARG_TEXT_P(2));
    bool readMem = PG_GETARG_BOOL(3);
    /* Current only support data page of usotre. */
    if (path == NULL || relationType == NULL || strcmp(relationType, "uheap") != 0 ||
        readMem) {
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmodule(MOD_USTORE),
                errmsg("relpath, reltype or rmem parameter is incorrect,"
                "only data pages and undo data in the ustore table can be parsed from disks."),
                errdetail("N/A"),
                errcause("Invalid parametes."),
                erraction("Check the input parameters.")));
        PG_RETURN_VOID();
    }

    /*
     * In order to avoid querying the shared buffer and applying LW locks, blocking the business.
     * When parsing all pages, only the data on the disk is parsed.
     */
    if (blockno == -1) {
        readMem = false;
    }

    char *outputFilename = ParsePage(path, blockno, relationType, readMem, true);
    PG_RETURN_TEXT_P(cstring_to_text(outputFilename));
#endif
}

