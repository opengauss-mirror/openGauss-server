/*
 * Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 *          http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 *
 * ---------------------------------------------------------------------------------------
 *
 * gs_stat.cpp
 *
 * IDENTIFICATION
 *        /Code/src/gausskernel/cbb/instruments/slow_query/gs_stat.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include "postgres.h"
#include "pgstat.h"
#include "catalog/pg_database.h"
#include "utils/timestamp.h"
#include "utils/snapmgr.h"
#include "access/tableam.h"
#include "access/heapam.h"

void copy_beentry(PgBackendStatus* localentry, PgBackendStatus* beentry)
{
    char* localappname = NULL;
    char* localactivity = NULL;
    errno_t rc;

    rc = memcpy_s(localentry, sizeof(PgBackendStatus), (void*)beentry, sizeof(PgBackendStatus));
    securec_check(rc, "", "");
    /*
     * strcpy is safe even if the string is modified concurrently,
     * because there's always a \0 at the end of the buffer.
     */
    localappname = (char*)palloc(NAMEDATALEN);
    rc = strcpy_s(localappname, NAMEDATALEN, (char*)beentry->st_appname);
    securec_check(rc, "", "");
    localentry->st_appname = localappname;
    localactivity = (char*)palloc((Size)g_instance.attr.attr_common.pgstat_track_activity_query_size);
    rc = strcpy_s(localactivity,
        (size_t)g_instance.attr.attr_common.pgstat_track_activity_query_size,
        (char*)beentry->st_activity);
    securec_check(rc, "", "");
    localentry->st_activity = localactivity;
}

inline static bool check_beentry_not_timeout(int timeout_threshold, TimestampTz current_time, PgBackendStatus* beentry)
{
    return (beentry->st_state != STATE_RUNNING || beentry->st_activity_start_timestamp == 0 ||
            !TimestampDifferenceExceeds(
                beentry->st_activity_start_timestamp, current_time, timeout_threshold * MSECS_PER_SEC));
}

PgBackendStatus* gsstat_check_beentry_timeout(int timeout_threshold, PgBackendStatus* beentry)
{
    PgBackendStatus* localentry = NULL;
    TimestampTz current_time = GetCurrentTimestamp();

    /*
     * Follow the protocol of retrying if st_changecount changes while we
     * copy the entry, or if it's odd.  (The check for odd is needed to
     * cover the case where we are able to completely copy the entry while
     * the source backend is between increment steps.)	We use a volatile
     * pointer here to ensure the compiler doesn't try to get cute.
     */
    do {
        int before_changecount;
        int after_changecount;

        /* reset localentry */
        if (localentry != NULL) {
            pfree(localentry->st_appname);
            pfree(localentry->st_activity);
            pfree(localentry);
            localentry = NULL;
        }

        pgstat_save_changecount_before(beentry, before_changecount);

        if (beentry->st_procpid == 0 && beentry->st_sessionid == 0) {
            return localentry;
        }

        /* Pass that state is not running and execute not timeout */
        if (check_beentry_not_timeout(timeout_threshold, current_time, beentry)) {
            return localentry;
        }

        localentry = (PgBackendStatus*)palloc(sizeof(PgBackendStatus));
        copy_beentry(localentry, beentry);

        pgstat_save_changecount_after(beentry, after_changecount);
        if (before_changecount == after_changecount && ((uint)before_changecount & 1) == 0) {
            return localentry;
        }

        /* Make sure we can break out of loop if stuck... */
        CHECK_FOR_INTERRUPTS();
    } while (true);

    return localentry;
}

void gs_stat_get_database_name(PgBackendStatusNode* list)
{
    PgBackendStatusNode* node = NULL;
    PgBackendStatus* beentry = NULL;
    Relation rel = NULL;
    HeapTuple tup = NULL;
    TableScanDesc scan = NULL;
    errno_t rc;

    rel = heap_open(DatabaseRelationId, AccessShareLock);
    scan = tableam_scan_begin(rel, SnapshotNow, 0, NULL);

    while (HeapTupleIsValid(tup = (HeapTuple) tableam_scan_getnexttuple(scan, ForwardScanDirection))) {
        if (HeapTupleIsValid(tup)) {
            Form_pg_database database = (Form_pg_database)GETSTRUCT(tup);
            node = list;
            while (node) {
                beentry = node->data;
                if (beentry->st_databaseid == HeapTupleGetOid(tup)) {
                    rc = strncpy_s(
                        NameStr(node->database_name), NAMEDATALEN, NameStr(database->datname), NAMEDATALEN - 1);
                    securec_check(rc, "\0", "\0");
                }
                node = node->next;
            }
        }
    }

    tableam_scan_end(scan);
    heap_close(rel, AccessShareLock);
}

void gs_stat_get_timeout_beentry(int timeout_threshold, Tuplestorestate* tupStore, TupleDesc tupDesc, FuncType insert)
{
    PgBackendStatus* beentry = t_thrd.shemem_ptr_cxt.BackendStatusArray + BackendStatusArray_size - 1;
    PgBackendStatus* localentry = NULL;
    /*
     * We go through BackendStatusArray from back to front, because
     * in thread pool mode, both an active session and its thread pool worker
     * will have the same procpid in their entry and in most cases what we want
     * is session's entry, which will be returned first in such searching direction.
     */
    for (int i = 1; i <= BackendStatusArray_size; i++) {
        localentry = gsstat_check_beentry_timeout(timeout_threshold, beentry);

        /* Only valid entries get included into the local array */
        if (localentry != NULL &&
            (localentry->st_procpid > 0 || localentry->st_sessionid > 0)) {
            insert(tupStore, tupDesc, localentry);
        }
        beentry--;
        if (localentry != NULL) {
            pfree(localentry->st_appname);
            pfree(localentry->st_activity);
            pfree(localentry);
        }
    }
}

/*
 * Free PgBackendStatusNode and its underlying palloc-ed data structures, include st_appname, st_clienthostname
 * st_conninfo and st_activity. We just have palloced these four variables in function 'gs_stat_encap_status_info'.
 */
void FreeBackendStatusNodeMemory(PgBackendStatusNode* node)
{
    while (node != NULL) {
        if (node->data != NULL) {
            pfree_ext(node->data->st_appname);
            pfree_ext(node->data->st_clienthostname);
            pfree_ext(node->data->st_conninfo);
            pfree_ext(node->data->st_activity);
        }
        pfree_ext(node->data);
        PgBackendStatusNode* tempNode = node;
        node = node->next;
        pfree_ext(tempNode);
    }
}

bool gs_stat_encap_status_info(PgBackendStatus* localentry, PgBackendStatus* beentry)
{
#define NVL(a,b) (((a) == NULL)?(b):(a))
    char* appnameStr = NVL(localentry->st_appname, (char*)palloc(NAMEDATALEN));
    char* clienthostnameStr = NVL(localentry->st_clienthostname, (char*)palloc(NAMEDATALEN));
    char* conninfoStr = NVL(localentry->st_conninfo, (char*)palloc(CONNECTIONINFO_LEN));
    char* activityStr = NVL(localentry->st_activity, (char*)palloc((Size)(g_instance.attr.attr_common.pgstat_track_activity_query_size)));
    errno_t rc = EOK;

    for (;;) {
        int before_changecount, after_changecount;

        pgstat_save_changecount_before(beentry, before_changecount);
        localentry->st_procpid = beentry->st_procpid;
        localentry->st_sessionid = beentry->st_sessionid;
        if (localentry->st_procpid > 0 || localentry->st_sessionid > 0) {
            rc = memcpy_s(localentry, sizeof(PgBackendStatus), (char*)beentry, sizeof(PgBackendStatus));
            securec_check(rc, "", "");
            /*
             * strcpy is safe even if the string is modified concurrently,
             * because there's always a \0 at the end of the buffer.
             */
            (void)memset_s(appnameStr, NAMEDATALEN, 0, NAMEDATALEN);
            rc = strcpy_s(appnameStr, NAMEDATALEN, (char*)beentry->st_appname);
            securec_check(rc, "", "");
            (void)memset_s(clienthostnameStr, NAMEDATALEN, 0, NAMEDATALEN);
            rc = strcpy_s(clienthostnameStr, NAMEDATALEN, (char*)beentry->st_clienthostname);
            securec_check(rc, "", "");
            (void)memset_s(conninfoStr, CONNECTIONINFO_LEN, 0, CONNECTIONINFO_LEN);
            rc = strcpy_s(conninfoStr, CONNECTIONINFO_LEN, (char*)beentry->st_conninfo);
            securec_check(rc, "", "");
            (void)memset_s(activityStr,
                (size_t)(g_instance.attr.attr_common.pgstat_track_activity_query_size),
                0,
                (size_t)(g_instance.attr.attr_common.pgstat_track_activity_query_size));
            rc = strcpy_s(activityStr,
                (size_t)(g_instance.attr.attr_common.pgstat_track_activity_query_size),
                (char*)beentry->st_activity);
            securec_check(rc, "", "");
            localentry->st_block_sessionid = beentry->st_block_sessionid;
            localentry->globalSessionId = beentry->globalSessionId;
            localentry->st_unique_sql_key = beentry->st_unique_sql_key;
        }

        pgstat_save_changecount_after(beentry, after_changecount);
        if (before_changecount == after_changecount && ((uint)before_changecount & 1) == 0) {
            break;
        }
        /* Make sure we can break out of loop if stuck... */
        CHECK_FOR_INTERRUPTS();
    }

    localentry->st_appname = appnameStr;
    localentry->st_clienthostname = clienthostnameStr;
    localentry->st_conninfo = conninfoStr;
    localentry->st_activity = activityStr;

    if (localentry->st_procpid > 0 || localentry->st_sessionid > 0) {
        return true;
    } else {
        return false;
    }
}

/* ----------
 * gs_stat_read_current_status() -
 *
 *	Copy the current contents of the PgBackendStatus array to local memory,
 *	if not already done in this transaction.
 * ----------
 */
PgBackendStatusNode* gs_stat_read_current_status(uint32* maxCalls)
{
    PgBackendStatus* beentry = NULL;
    PgBackendStatusNode* localtable = NULL;
    MemoryContext memcontext, oldContext;
    PgBackendStatus* localentry = NULL;

    pgstat_setup_memcxt();
    memcontext = (u_sess->stat_cxt.pgStatRunningInCollector) ? u_sess->stat_cxt.pgStatCollectThdStatusContext
                                                             : u_sess->stat_cxt.pgStatLocalContext;
    oldContext = MemoryContextSwitchTo(memcontext);

    beentry = t_thrd.shemem_ptr_cxt.BackendStatusArray + BackendStatusArray_size - 1;
    localtable = (PgBackendStatusNode*)palloc(sizeof(PgBackendStatusNode));
    localtable->next = NULL;
    PgBackendStatusNode* tmpLocaltable = localtable;

    if (maxCalls != NULL) {
        (*maxCalls) = 0;
    }

    /*
     * We go through BackendStatusArray from back to front, because
     * in thread pool mode, both an active session and its thread pool worker
     * will have the same procpid in their entry and in most cases what we want
     * is session's entry, which will be returned first in such searching direction.
     */
    for (int i = 1; i <= BackendStatusArray_size; i++) {
        /*
         * Follow the protocol of retrying if st_changecount changes while we
         * copy the entry, or if it's odd.  (The check for odd is needed to
         * cover the case where we are able to completely copy the entry while
         * the source backend is between increment steps.)	We use a volatile
         * pointer here to ensure the compiler doesn't try to get cute.
         */
        if (localentry == NULL) {
            localentry = (PgBackendStatus*)palloc0(sizeof(PgBackendStatus));
        }
        if (gs_stat_encap_status_info(localentry, beentry)) {
            PgBackendStatusNode* entry_node = (PgBackendStatusNode*)palloc(sizeof(PgBackendStatusNode));
            entry_node->data = localentry;
            tmpLocaltable->next = entry_node;
            tmpLocaltable = tmpLocaltable->next;
            tmpLocaltable->next = NULL;
            if (maxCalls != NULL) {
                (*maxCalls)++;
            }
            localentry = NULL;
        }
        beentry--;
    }
    if (localentry != NULL) {
        pfree_ext(localentry->st_appname);
        pfree_ext(localentry->st_clienthostname);
        pfree_ext(localentry->st_conninfo);
        pfree_ext(localentry->st_activity);
        pfree_ext(localentry);
        localentry = NULL;
    }
    (void)MemoryContextSwitchTo(oldContext);
    return localtable->next;
}

// Using TupleStore to optimize the process
uint32 gs_stat_read_current_status(Tuplestorestate *tupStore, TupleDesc tupDesc, FuncType insert, bool hasTID,
                                   ThreadId threadId)
{
    PgBackendStatus *beentry = t_thrd.shemem_ptr_cxt.BackendStatusArray + BackendStatusArray_size - 1;

    PgBackendStatus *localentry = (PgBackendStatus *) palloc0(sizeof(PgBackendStatus));

    uint32 maxCalls = 0;
    /*
     * We go through BackendStatusArray from back to front, because
     * in thread pool mode, both an active session and its thread pool worker
     * will have the same procpid in their entry and in most cases what we want
     * is session's entry, which will be returned first in such searching direction.
     */
    for (int i = 1; i <= BackendStatusArray_size; i++) {
        /*
         * Follow the protocol of retrying if st_changecount changes while we
         * copy the entry, or if it's odd.  (The check for odd is needed to
         * cover the case where we are able to completely copy the entry while
         * the source backend is between increment steps.)	We use a volatile
         * pointer here to ensure the compiler doesn't try to get cute.
         */
        if (gs_stat_encap_status_info(localentry, beentry)) {
            bool flag = false;
            if (!hasTID || localentry->st_procpid == threadId) {
                if (insert != NULL) {
                    insert(tupStore, tupDesc, localentry);
                }
                maxCalls++;
                flag = true;
            }
            // Find only items with the same thread ID.
            if (hasTID && flag) {
                break;
            }
        }
        beentry--;
    }

    pfree_ext(localentry->st_appname);
    pfree_ext(localentry->st_clienthostname);
    pfree_ext(localentry->st_conninfo);
    pfree_ext(localentry->st_activity);
    pfree_ext(localentry);
    return maxCalls;
}

PgBackendStatus* gs_stat_fetch_stat_beentry(int32 beid)
{
    PgBackendStatus* beentry = NULL;
    int index = 1;

    if (t_thrd.shemem_ptr_cxt.BackendStatusArray == NULL) {
        if (NULL != PgBackendStatusArray)
            t_thrd.shemem_ptr_cxt.BackendStatusArray = PgBackendStatusArray;
        else
            return NULL;
    }

    PgBackendStatusNode* node = gs_stat_read_current_status(NULL);
    while (node != NULL) {
        beentry = node->data;
        if (beentry != NULL) {
            if (index == beid) {
                return beentry;
            }
        }
        index++;
        node = node->next;
    }

    return NULL;
}

void gs_stat_free_stat_node(PgBackendStatusNode* node)
{
    PgBackendStatusNode* tmpNode = node;
    PgBackendStatusNode* freeNode = NULL;
    PgBackendStatus* beentry = NULL;
 
    while (tmpNode != NULL) {
        beentry = tmpNode->data;
        gs_stat_free_stat_beentry(beentry);
        freeNode = tmpNode;
        tmpNode = tmpNode->next;
        pfree_ext(freeNode);
    }
}

void gs_stat_free_stat_beentry(PgBackendStatus* beentry)
{
    if (beentry == NULL) {
        return;
    }
    pfree_ext(beentry->st_appname);
    pfree_ext(beentry->st_clienthostname);
    pfree_ext(beentry->st_conninfo);
    pfree_ext(beentry->st_activity);
    pfree_ext(beentry);
}