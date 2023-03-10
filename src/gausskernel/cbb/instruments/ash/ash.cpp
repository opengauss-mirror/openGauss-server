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
 * -------------------------------------------------------------------------
 *
 * ash.cpp
 *   functions for active session profile
 *
 * IDENTIFICATION
 *    src/gausskernel/cbb/instruments/ash/ash.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "pgxc/pgxc.h"
#include "pgstat.h"
#include "pgxc/poolutils.h"
#include "executor/spi.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "utils/memutils.h"
#include "storage/proc.h"
#include "storage/latch.h"
#include "storage/ipc.h"
#include "workload/workload.h"
#include "catalog/pg_database.h"
#include "gssignal/gs_signal.h"
#include "utils/guc.h"
#include "utils/ps_status.h"
#include "utils/elog.h"
#include "utils/memprot.h"
#include "utils/builtins.h"
#include "tcop/dest.h"
#include "tcop/tcopprot.h"
#include "gs_thread.h"
#include "access/heapam.h"
#include "utils/rel.h"
#include "utils/postinit.h"
#include "workload/gscgroup.h"
#include "instruments/generate_report.h"
#include "libpq/pqsignal.h"
#include "pgxc/groupmgr.h"
#include "instruments/ash.h"
#include "funcapi.h"
#include "libpq/ip.h"
#include "instruments/instr_unique_sql.h"
#include "cjson/cJSON.h"
#include "catalog/gs_asp.h"
#include "catalog/indexing.h"
#include "storage/lock/lock.h"
#include "utils/snapmgr.h"
#include "access/tableam.h"
#include "utils/fmgroids.h"

#define NUM_UNIQUE_SQL_PARTITIONS 64
#define UINT32_ACCESS_ONCE(var) ((uint32)(*((volatile uint32*)&(var))))
#define UNIQUE_SQL_MAX_LEN (g_instance.attr.attr_common.pgstat_track_activity_query_size + 1)
#define ATTR_NUM 30
/* unique SQL max hash table size */
const int UNIQUE_SQL_MAX_HASH_SIZE = 1000;
extern Datum hash_uint32(uint32 k);

typedef struct {
    UniqueSQLKey key; /* CN oid + user oid + unique sql id */

    /* alloc extra UNIQUE_SQL_MAX_LEN space to store unique sql string */
    char* unique_sql; /* unique sql text */
} ASHUniqueSQL;

namespace Asp {
    void SubAspWorker();
}
using namespace Asp;
const int PGSTAT_RESTART_INTERVAL = 60;

static void instr_asp_exit(SIGNAL_ARGS)
{
    t_thrd.ash_cxt.need_exit = true;
    die(postgres_signal_arg);
}
/* SIGHUP handler for collector process */
static void asp_sighup_handler(SIGNAL_ARGS)
{
    t_thrd.ash_cxt.got_SIGHUP = true;
}
void JobAspIAm(void)
{
    t_thrd.role = ASH_WORKER;
}

bool IsJobAspProcess(void)
{
    return t_thrd.role == ASH_WORKER;
}
static void ASPSleep(int32 sleepSec)
{
    for (int32 i = 0; i < sleepSec; i++) {
        if (t_thrd.ash_cxt.need_exit) {
            break;
        }
        pg_usleep(USECS_PER_SEC);
    }
}
/* user unique sql function ? */
static uint32 AspUniqueSQLHashCode(const void* key, Size size)
{
    const UniqueSQLKey* k = (const UniqueSQLKey*)key;

    return hash_uint32((uint32)k->cn_id) ^ hash_uint32((uint32)k->user_id) ^ hash_uint32((uint32)k->unique_sql_id);
}

static int AspUniqueSQLMatch(const void* key1, const void* key2, Size keysize)
{
    const UniqueSQLKey* k1 = (const UniqueSQLKey*)key1;
    const UniqueSQLKey* k2 = (const UniqueSQLKey*)key2;

    if (k1 != NULL && k2 != NULL && k1->user_id == k2->user_id && k1->unique_sql_id == k2->unique_sql_id) {
        return 0;
    } else {
        return 1;
    }
}

static LWLock* LockAspUniqueSQLHashPartition(uint32 hashCode, LWLockMode lockMode)
{
    LWLock* partitionLock = GetMainLWLockByIndex(FirstASPMappingLock + (hashCode % NUM_UNIQUE_SQL_PARTITIONS));
    LWLockAcquire(partitionLock, lockMode);

    return partitionLock;
}

static void UnlockAspUniqueSQLHashPartition(uint32 hashCode)
{
    LWLock* partitionLock = GetMainLWLockByIndex(FirstASPMappingLock + (hashCode % NUM_UNIQUE_SQL_PARTITIONS));
    LWLockRelease(partitionLock);
}

static inline int uint64_to_str(char* str, int strLen, uint64 val)
{
    Assert(strLen >= MAX_LEN_CHAR_TO_BIGINT_BUF);
    char stack[MAX_LEN_CHAR_TO_BIGINT_BUF] = {0};
    int idx = 0;
    int i = 0;
    Assert(str != NULL);
    Assert(val >= 0);
    while (val > 0) {
        stack[idx] = '0' + (val % 10);
        val /= 10;
        idx++;
    }
    while (idx > 0) {
        str[i] = stack[idx - 1];
        i++;
        idx--;
    }
    return i;
}
static void CleanupAspUniqueSqlHash()
{
    UniqueSQL* entry = NULL;
    HASH_SEQ_STATUS hash_seq;
    int i;
    for (i = 0; i < NUM_UNIQUE_SQL_PARTITIONS; i++) {
        LWLockAcquire(GetMainLWLockByIndex(FirstASPMappingLock + i), LW_EXCLUSIVE);
    }

    hash_seq_init(&hash_seq, g_instance.stat_cxt.ASHUniqueSQLHashtbl);
    while ((entry = (UniqueSQL*)hash_seq_search(&hash_seq)) != NULL) {
        hash_search(g_instance.stat_cxt.ASHUniqueSQLHashtbl, &entry->key, HASH_REMOVE, NULL);
    }
    for (i = 0; i < NUM_UNIQUE_SQL_PARTITIONS; i++) {
        LWLockRelease(GetMainLWLockByIndex(FirstASPMappingLock + i));
    }
}
void InitAsp()
{
    /* init memory context */
    g_instance.stat_cxt.AshContext = AllocSetContextCreate(g_instance.instance_context,
        "AshContext",
        ALLOCSET_DEFAULT_MINSIZE,
        ALLOCSET_DEFAULT_INITSIZE,
        ALLOCSET_DEFAULT_MAXSIZE,
        SHARED_CONTEXT);

    /* Apply for memory to save the history information of wait events */
    g_instance.stat_cxt.active_sess_hist_arrary = 
        (ActiveSessHistArrary *)MemoryContextAllocZero(g_instance.stat_cxt.AshContext, sizeof(ActiveSessHistArrary));
    g_instance.stat_cxt.active_sess_hist_arrary->curr_index = 0;
    g_instance.stat_cxt.active_sess_hist_arrary->max_size = g_instance.attr.attr_common.asp_sample_num;

    g_instance.stat_cxt.active_sess_hist_arrary->active_sess_hist_info =
        (SessionHistEntry *)MemoryContextAllocZero(g_instance.stat_cxt.AshContext,
        g_instance.stat_cxt.active_sess_hist_arrary->max_size * sizeof(SessionHistEntry));
    char* ash_appname = (char*)MemoryContextAlloc(g_instance.stat_cxt.AshContext,
        NAMEDATALEN * g_instance.stat_cxt.active_sess_hist_arrary->max_size);
    char* ash_clienthostname = (char*)MemoryContextAlloc(g_instance.stat_cxt.AshContext,
        NAMEDATALEN * g_instance.stat_cxt.active_sess_hist_arrary->max_size);
    char* ashRelname = (char*)MemoryContextAlloc(g_instance.stat_cxt.AshContext,
        2 * NAMEDATALEN * g_instance.stat_cxt.active_sess_hist_arrary->max_size);
    for (uint32 i = 0; i < g_instance.stat_cxt.active_sess_hist_arrary->max_size; i++) {
        (g_instance.stat_cxt.active_sess_hist_arrary->active_sess_hist_info + i)->st_appname =
        (ash_appname + i * NAMEDATALEN);
        (g_instance.stat_cxt.active_sess_hist_arrary->active_sess_hist_info + i)->clienthostname =
        (ash_clienthostname + i * NAMEDATALEN);
        (g_instance.stat_cxt.active_sess_hist_arrary->active_sess_hist_info + i)->relname =
        (ashRelname + i * 2 * NAMEDATALEN);
    }

    /* init hash table for active session history */
    HASHCTL ctl;
    errno_t rc;
    rc = memset_s(&ctl, sizeof(ctl), 0, sizeof(ctl));
    securec_check_c(rc, "\0", "\0");
    ctl.hcxt = g_instance.stat_cxt.AshContext;
    ctl.keysize = sizeof(UniqueSQLKey);

    /* alloc extra space for normalized query(only CN stores sql string) */
    if (need_normalize_unique_string()) {
        ctl.entrysize = sizeof(ASHUniqueSQL) + UNIQUE_SQL_MAX_LEN;
    } else {
        ctl.entrysize = sizeof(ASHUniqueSQL);
    }
    ctl.hash = AspUniqueSQLHashCode;
    ctl.match = AspUniqueSQLMatch;
    ctl.num_partitions = NUM_UNIQUE_SQL_PARTITIONS;
    g_instance.stat_cxt.ASHUniqueSQLHashtbl = hash_create("ASP unique sql hash table",
        UNIQUE_SQL_MAX_HASH_SIZE,
        &ctl,
        HASH_ELEM | HASH_SHRCTX | HASH_FUNCTION | HASH_COMPARE | HASH_PARTITION | HASH_NOEXCEPT);
}

static int64 GetTableRetentionTime()
{
    return (int64)GetCurrentTimestamp() - u_sess->attr.attr_common.asp_retention_days * USECS_PER_DAY;
}
static void CleanAspTable()
{
    ScanKeyData key;
    SysScanDesc indesc = NULL;
    HeapTuple tup = NULL;
    PG_TRY();
    {
        StartTransactionCommand();
        PushActiveSnapshot(GetTransactionSnapshot());
        Relation rel = heap_open(GsAspRelationId, RowExclusiveLock);
        int64 minTime = GetTableRetentionTime();
        ScanKeyInit(&key, Anum_gs_asp_sample_time, BTLessEqualStrategyNumber,
            F_TIMESTAMP_LE, TimestampGetDatum(minTime));
        indesc = systable_beginscan(rel, GsAspSampleIdTimedexId, true, SnapshotSelf, 1, &key);
        while (HeapTupleIsValid(tup = systable_getnext(indesc))) {
            simple_heap_delete(rel, &tup->t_self);
        }
        systable_endscan(indesc);
        heap_close(rel, RowExclusiveLock);
        PopActiveSnapshot();
        CommitTransactionCommand();
    }
    PG_CATCH();
    {
        ereport(WARNING, (errcode(ERRCODE_WARNING), errmsg("clean gs_asp table  failed")));
        PopActiveSnapshot();
        AbortCurrentTransaction();
        PG_RE_THROW();
    }
    PG_END_TRY();
}
static void FormatClientInfo(cJSON * root, SessionHistEntry *beentry)
{
    /* A zeroed client addr means we don't know */
    SockAddr zero_clientaddr;
    errno_t rc = memset_s(&zero_clientaddr, sizeof(zero_clientaddr), 0, sizeof(zero_clientaddr));
    securec_check(rc, "\0", "\0");
    if (memcmp(&(beentry->clientaddr), &zero_clientaddr, sizeof(zero_clientaddr)) == 0) {
        cJSON_AddItemToObject(root, "client_addr", cJSON_CreateNull());
        cJSON_AddItemToObject(root, "client_hostname", cJSON_CreateNull());
        cJSON_AddItemToObject(root, "client_port", cJSON_CreateNull());
    } else {
        if (beentry->clientaddr.addr.ss_family == AF_INET
#ifdef HAVE_IPV6
            || beentry->clientaddr.addr.ss_family == AF_INET6
#endif
        ) {
            char remote_host[NI_MAXHOST];
            char remote_port[NI_MAXSERV];
            int ret;
            remote_host[0] = '\0';
            remote_port[0] = '\0';
            ret = pg_getnameinfo_all(&beentry->clientaddr.addr,
                beentry->clientaddr.salen,
                remote_host,
                sizeof(remote_host),
                remote_port,
                sizeof(remote_port),
                NI_NUMERICHOST | NI_NUMERICSERV);
            if (ret == 0) {
                clean_ipv6_addr(beentry->clientaddr.addr.ss_family, remote_host);
                char* inet = Datum_to_string(
                    DirectFunctionCall1(inet_in, CStringGetDatum(remote_host)), INETOID, false);
                cJSON_AddItemToObject(root, "client_addr", cJSON_CreateString(inet));
                if (beentry->clienthostname && beentry->clienthostname[0])
                    cJSON_AddItemToObject(root, "client_hostname",
                        cJSON_CreateString(beentry->clienthostname));
                else
                    cJSON_AddItemToObject(root, "client_hostname", cJSON_CreateNull());
                cJSON_AddItemToObject(root, "client_port", cJSON_CreateNumber(atoi(remote_port)));
            } else {
                cJSON_AddItemToObject(root, "client_addr", cJSON_CreateNull());
                cJSON_AddItemToObject(root, "client_hostname", cJSON_CreateNull());
                cJSON_AddItemToObject(root, "client_port", cJSON_CreateNull());
            }
        } else if (beentry->clientaddr.addr.ss_family == AF_UNIX) {
            /*
            * Unix sockets always reports NULL for host and -1 for
            * port, so it's possible to tell the difference to
            * connections we have no permissions to view, or with
            * errors.
            */
            cJSON_AddItemToObject(root, "client_addr", cJSON_CreateNull());
            cJSON_AddItemToObject(root, "client_hostname", cJSON_CreateNull());
            cJSON_AddItemToObject(root, "client_port", cJSON_CreateNumber(-1));
        } else {
            /* Unknown address type, should never happen */
            cJSON_AddItemToObject(root, "client_hostname", cJSON_CreateNull());
            cJSON_AddItemToObject(root, "client_hostname", cJSON_CreateNull());
            cJSON_AddItemToObject(root, "client_port", cJSON_CreateNull());
        }
    }
}

static void FormatSQLInfo(cJSON * root, SessionHistEntry *beentry)
{
    char query_id[MAX_LEN_CHAR_TO_BIGINT_BUF] = {0};
    char unique_query_id[MAX_LEN_CHAR_TO_BIGINT_BUF] = {0};
    int len = uint64_to_str(query_id, MAX_LEN_CHAR_TO_BIGINT_BUF, beentry->queryid);
    cJSON_AddItemToObject(root, "query_id", cJSON_CreateString(query_id));
    if (beentry->unique_sql_key.unique_sql_id != 0) {
        len = uint64_to_str(unique_query_id, MAX_LEN_CHAR_TO_BIGINT_BUF, beentry->unique_sql_key.unique_sql_id);
        cJSON_AddItemToObject(root, "unique_query_id", cJSON_CreateString(unique_query_id));
        cJSON_AddItemToObject(root, "user_id", cJSON_CreateNumber(beentry->unique_sql_key.user_id));
        cJSON_AddItemToObject(root, "cn_id", cJSON_CreateNumber(beentry->unique_sql_key.cn_id));
    } else {
        cJSON_AddItemToObject(root, "unique_query_id", cJSON_CreateNull());
        cJSON_AddItemToObject(root, "user_id", cJSON_CreateNull());
        cJSON_AddItemToObject(root, "cn_id", cJSON_CreateNull());
    }
    if (need_normalize_unique_string() && beentry->unique_sql_key.unique_sql_id != 0) {
        /* add lock for hash table */
        uint32 hashCode = AspUniqueSQLHashCode(&beentry->unique_sql_key, sizeof(beentry->unique_sql_key));
        LockAspUniqueSQLHashPartition(hashCode, LW_SHARED);
        ASHUniqueSQL *entry = (ASHUniqueSQL*)hash_search(g_instance.stat_cxt.ASHUniqueSQLHashtbl,
            &beentry->unique_sql_key, HASH_FIND, NULL);
        if (entry == NULL || entry->unique_sql == NULL)
            cJSON_AddItemToObject(root, "unique_query", cJSON_CreateNull());
        else
            cJSON_AddItemToObject(root, "unique_query", cJSON_CreateString(entry->unique_sql));
        UnlockAspUniqueSQLHashPartition(hashCode);
    } else {
        cJSON_AddItemToObject(root, "unique_query", cJSON_CreateNull());
    }
}

static const char* SwitchStatusDesc(BackendState state)
{
    switch (state) {
        case STATE_IDLE:
            return "idle";
        case STATE_RUNNING:
            return "active";
        case STATE_IDLEINTRANSACTION:
            return "idle in transaction";
        case STATE_FASTPATH:
            return "fastpath function call";
        case STATE_IDLEINTRANSACTION_ABORTED:
            return "idle in transaction (aborted)";
        case STATE_DISABLED:
            return "disabled";
        case STATE_RETRYING:
            return "retrying";
        case STATE_COUPLED:
            return "coupled to session";
        case STATE_DECOUPLED:
            return "decoupled from session";
        case STATE_UNDEFINED:
            return "state_undefined";
        default:
            return "undefined";
    }
}

static void FormatBasicInfo(cJSON * root, SessionHistEntry *beentry)
{
    char sampleid[MAX_LEN_CHAR_TO_BIGINT_BUF] = {0};
    char thread_id[MAX_LEN_CHAR_TO_BIGINT_BUF] = {0};
    char sessionid[MAX_LEN_CHAR_TO_BIGINT_BUF] = {0};
    int len = uint64_to_str(sampleid, MAX_LEN_CHAR_TO_BIGINT_BUF, beentry->sample_id);
    len = uint64_to_str(thread_id, MAX_LEN_CHAR_TO_BIGINT_BUF, beentry->procpid);
    len = uint64_to_str(sessionid, MAX_LEN_CHAR_TO_BIGINT_BUF, beentry->session_id);

    cJSON_AddItemToObject(root, "sampleid", cJSON_CreateString(sampleid));
    char* timestamp = Datum_to_string(TimestampGetDatum(beentry->sample_time), TIMESTAMPTZOID, false);
    cJSON_AddItemToObject(root, "sample_time", cJSON_CreateString(timestamp));
    cJSON_AddItemToObject(root, "need_flush_sample", cJSON_CreateBool(beentry->need_flush_sample));
    cJSON_AddItemToObject(root, "databaseid", cJSON_CreateNumber(beentry->databaseid));
    cJSON_AddItemToObject(root, "thread_id", cJSON_CreateString(thread_id));
    cJSON_AddItemToObject(root, "sessionid", cJSON_CreateString(sessionid));
    char* gId = GetGlobalSessionStr(beentry->globalSessionId);
    cJSON_AddItemToObject(root, "global_sessionid", cJSON_CreateString(gId));
    pfree(gId);
    pfree(timestamp);
    timestamp = Datum_to_string(TimestampGetDatum(beentry->start_time), TIMESTAMPTZOID, false);
    cJSON_AddItemToObject(root, "start_time", cJSON_CreateString(timestamp));
    pfree(timestamp);
    if (beentry->xact_start_time != 0) {
        timestamp = Datum_to_string(TimestampGetDatum(beentry->xact_start_time), TIMESTAMPTZOID, false);
        cJSON_AddItemToObject(root, "xact_start_time", cJSON_CreateString(timestamp));
        pfree(timestamp);
    } else {
        cJSON_AddItemToObject(root, "xact_start_time", cJSON_CreateNull());
    }
    if (beentry->query_start_time != 0) {
        timestamp = Datum_to_string(TimestampGetDatum(beentry->query_start_time), TIMESTAMPTZOID, false);
        cJSON_AddItemToObject(root, "query_start_time", cJSON_CreateString(timestamp));
        pfree(timestamp);
    } else {
        cJSON_AddItemToObject(root, "query_start_time", cJSON_CreateNull());
    }
    cJSON_AddItemToObject(root, "state", cJSON_CreateString(SwitchStatusDesc(beentry->state)));
}

static void GetWaitstatusXid(const SessionHistEntry *beentry, StringInfo waitStatus)
{    
    if (beentry->waitstatus == STATE_WAIT_XACTSYNC) {
        appendStringInfo(waitStatus, "%s: %lu",
            pgstat_get_waitstatusname(beentry->waitstatus),
            beentry->xid);
    } else {
        appendStringInfo(waitStatus, "%s", pgstat_get_waitstatusname(beentry->waitstatus));
    }
}

static void GetWaitstatusRelnamePhase(const SessionHistEntry *beentry, StringInfo waitStatus)
{
    if (beentry->waitstatus_phase != PHASE_NONE) {
        appendStringInfo(waitStatus, "%s: %s, %s",
            pgstat_get_waitstatusname(beentry->waitstatus),
            beentry->relname,
            PgstatGetWaitstatephasename(beentry->waitstatus_phase));
    } else {
        appendStringInfo(waitStatus, "%s: %s",
            pgstat_get_waitstatusname(beentry->waitstatus),
            beentry->relname);
    }
}

static void GetWaitstatusLibpqwaitnodeCount(const SessionHistEntry *beentry, StringInfo waitStatus)
{

    if (IS_PGXC_COORDINATOR) {
        NameData nodename = {{0}};
        appendStringInfo(waitStatus, "%s: %s, total %d",
            pgstat_get_waitstatusname(beentry->waitstatus),
            get_pgxc_nodename(beentry->libpq_wait_nodeid, &nodename),
            beentry->libpq_wait_nodecount);
    } else if (IS_PGXC_DATANODE) {
        if (global_node_definition != NULL && global_node_definition->nodesDefinition != NULL &&
            global_node_definition->num_nodes == beentry->numnodes) {
            AutoMutexLock copyLock(&nodeDefCopyLock);
            NodeDefinition* nodeDef = global_node_definition->nodesDefinition;
            copyLock.lock();
            appendStringInfo(waitStatus, "%s: %s, total %d",
                             pgstat_get_waitstatusname(beentry->waitstatus),
                             nodeDef[beentry->libpq_wait_nodeid].nodename.data,
                             beentry->libpq_wait_nodecount);
            copyLock.unLock();
        } else {
            appendStringInfo(waitStatus, "%s", pgstat_get_waitstatusname(beentry->waitstatus));
        }
    }
}

static void GetWaitstatusNodeCountPhase(const SessionHistEntry *beentry, StringInfo waitStatus)
{
    NameData nodename = {{0}};

    if (beentry->waitnode_count > 0) {
        if (beentry->waitstatus_phase != PHASE_NONE) {
            appendStringInfo(waitStatus, "%s: %s, total %d, %s",
                             pgstat_get_waitstatusname(beentry->waitstatus),
                             get_pgxc_nodename((Oid)beentry->nodeid, &nodename), beentry->waitnode_count, 
                             PgstatGetWaitstatephasename(beentry->waitstatus_phase));
        } else {
            appendStringInfo(waitStatus, "%s: %s, total %d",
                             pgstat_get_waitstatusname(beentry->waitstatus),
                             get_pgxc_nodename((Oid)beentry->nodeid, &nodename), beentry->waitnode_count);
        }
    } else {
        if (beentry->waitstatus_phase !=  PHASE_NONE) {
            appendStringInfo(waitStatus, "%s: %s, %s", pgstat_get_waitstatusname(beentry->waitstatus),
                             get_pgxc_nodename((Oid)beentry->nodeid, &nodename),
                             PgstatGetWaitstatephasename(beentry->waitstatus_phase));
        } else {
            appendStringInfo(waitStatus, "%s: %s", pgstat_get_waitstatusname(beentry->waitstatus),
                             get_pgxc_nodename((Oid)beentry->nodeid, &nodename));
        }
    }
}

static void GetWaitstatusNodePlannodeCountPhase(const SessionHistEntry *beentry, StringInfo waitStatus)
{
    NodeDefinition* nodeDef = global_node_definition->nodesDefinition;

    if (beentry->waitstatus_phase != PHASE_NONE) {
        if (beentry->plannodeid != -1) {
            appendStringInfo(waitStatus, "%s: %s(%d), total %d, %s",
                             pgstat_get_waitstatusname(beentry->waitstatus),
                             nodeDef[beentry->nodeid].nodename.data, beentry->plannodeid, beentry->waitnode_count,
                             PgstatGetWaitstatephasename(beentry->waitstatus_phase));
        } else {
            appendStringInfo(waitStatus, "%s: %s, total %d, %s",
                             pgstat_get_waitstatusname(beentry->waitstatus),
                             nodeDef[beentry->nodeid].nodename.data, beentry->waitnode_count,
                             PgstatGetWaitstatephasename(beentry->waitstatus_phase));
        }
    } else {
        if (beentry->plannodeid != -1) {
            appendStringInfo(waitStatus, "%s: %s(%d), total %d",
                             pgstat_get_waitstatusname(beentry->waitstatus),
                             nodeDef[beentry->nodeid].nodename.data, beentry->plannodeid,
                             beentry->waitnode_count);
        } else {
            appendStringInfo(waitStatus, "%s: %s, total %d",
                             pgstat_get_waitstatusname(beentry->waitstatus),
                             nodeDef[beentry->nodeid].nodename.data, beentry->waitnode_count);
        }
    } 
}

static void GetWaitstatusNodePlannodePhase(const SessionHistEntry *beentry, StringInfo waitStatus)
{
    NodeDefinition* nodeDef = global_node_definition->nodesDefinition;
    
    if (beentry->waitstatus_phase != PHASE_NONE) {
        if (beentry->plannodeid != -1) {
            appendStringInfo(waitStatus, "%s: %s(%d), %s",
                             pgstat_get_waitstatusname(beentry->waitstatus),
                             nodeDef[beentry->nodeid].nodename.data, beentry->plannodeid,
                             PgstatGetWaitstatephasename(beentry->waitstatus_phase));
        } else {
            appendStringInfo(waitStatus, "%s: %s, %s",
                             pgstat_get_waitstatusname(beentry->waitstatus),
                             nodeDef[beentry->nodeid].nodename.data,
                             PgstatGetWaitstatephasename(beentry->waitstatus_phase));
        }
    } else {
        if (beentry->plannodeid != -1) {
            appendStringInfo(waitStatus, "%s: %s(%d)",
                             pgstat_get_waitstatusname(beentry->waitstatus),
                             nodeDef[beentry->nodeid].nodename.data, beentry->plannodeid);
        } else {
            appendStringInfo(waitStatus, "%s: %s", pgstat_get_waitstatusname(beentry->waitstatus),
                             nodeDef[beentry->nodeid].nodename.data);
        }
    }

}

static void GetWaitstatusNodeCountPhasePlannode(const SessionHistEntry *beentry, StringInfo waitStatus)
{
    if (IS_PGXC_COORDINATOR && (Oid)beentry->nodeid != InvalidOid) {
        GetWaitstatusNodeCountPhase(beentry, waitStatus);
    } else if (IS_PGXC_DATANODE) {
        if (global_node_definition != NULL && global_node_definition->nodesDefinition &&
            global_node_definition->num_nodes == beentry->numnodes) {
            AutoMutexLock copyLock(&nodeDefCopyLock);
            copyLock.lock();
            if (beentry->waitnode_count > 0) {
                GetWaitstatusNodePlannodeCountPhase(beentry, waitStatus);
            } else {            
                GetWaitstatusNodePlannodePhase(beentry, waitStatus);
            }
            copyLock.unLock();
        } else {
            appendStringInfo(waitStatus, "%s", pgstat_get_waitstatusname(beentry->waitstatus));
        }
    }
}

static char* GetWaitStatusInfo(const SessionHistEntry *beentry)
{
    StringInfoData waitStatus;
    initStringInfo(&waitStatus);

    if (beentry->xid != 0) {
        GetWaitstatusXid(beentry, &waitStatus);
    } else if (beentry->relname && beentry->relname[0] != '\0' &&
               (beentry->waitstatus == STATE_VACUUM || beentry->waitstatus == STATE_ANALYZE ||
               beentry->waitstatus == STATE_VACUUM_FULL)) {
        GetWaitstatusRelnamePhase(beentry, &waitStatus);
    } else if (beentry->libpq_wait_nodeid != InvalidOid && beentry->libpq_wait_nodecount != 0) {
        GetWaitstatusLibpqwaitnodeCount(beentry, &waitStatus);
    } else if (beentry->nodeid != -1) {
        GetWaitstatusNodeCountPhasePlannode(beentry, &waitStatus);
    } else {
        appendStringInfo(&waitStatus, "%s", pgstat_get_waitstatusname(beentry->waitstatus));
    }

    return waitStatus.data;
}

static void FormatBlockInfo(cJSON *root, const SessionHistEntry *beentry)
{
    if (beentry->locallocktag.lock.locktag_lockmethodid == 0) {
        (void)cJSON_AddItemToObject(root, "locktag", cJSON_CreateNull());
        (void)cJSON_AddItemToObject(root, "lockmode", cJSON_CreateNull());
        (void)cJSON_AddItemToObject(root, "block_sessionid", cJSON_CreateNull());
        return;
    }

    if ((beentry->waitevent & 0xFF000000) == PG_WAIT_LOCK) {
        char* blocklocktag = LocktagToString(beentry->locallocktag.lock);
        const char* lock_mode = (char*)GetLockmodeName(beentry->locallocktag.lock.locktag_lockmethodid,
                                                       beentry->locallocktag.mode);
        (void)cJSON_AddItemToObject(root, "locktag", cJSON_CreateString(blocklocktag));
        (void)cJSON_AddItemToObject(root, "lockmode", cJSON_CreateString(lock_mode));
        (void)cJSON_AddItemToObject(root, "block_sessionid", cJSON_CreateNumber(beentry->st_block_sessionid));
        pfree_ext(blocklocktag);
    } else {
        (void)cJSON_AddItemToObject(root, "locktag", cJSON_CreateNull());
        (void)cJSON_AddItemToObject(root, "lockmode", cJSON_CreateNull());
        (void)cJSON_AddItemToObject(root, "block_sessionid", cJSON_CreateNull());
    }
}

static void FormatWaitEventInfo(cJSON * root, SessionHistEntry *beentry)
{
    /* waitEvent info */
    const char* wait_event = NULL;

    if (beentry->waitevent != 0) {
        uint32 raw_wait_event = UINT32_ACCESS_ONCE(beentry->waitevent);
        wait_event = pgstat_get_wait_event(raw_wait_event);
        (void)cJSON_AddItemToObject(root, "event", cJSON_CreateString(wait_event));
        (void)cJSON_AddItemToObject(root, "waitstatus", cJSON_CreateString(pgstat_get_waitstatusdesc(raw_wait_event)));
    } else {
        wait_event = pgstat_get_waitstatusname(beentry->waitstatus);
        (void)cJSON_AddItemToObject(root, "event", cJSON_CreateString(wait_event));
        char* waitStatus = GetWaitStatusInfo(beentry);
        (void)cJSON_AddItemToObject(root, "waitstatus", cJSON_CreateString(waitStatus));
        pfree_ext(waitStatus);
    }

    cJSON_AddItemToObject(root, "lwtid", cJSON_CreateNumber(beentry->tid));

    if (0 != beentry->psessionid) {
        char psessionid[MAX_LEN_CHAR_TO_BIGINT_BUF] = {0};
        uint64_to_str(psessionid, MAX_LEN_CHAR_TO_BIGINT_BUF, beentry->psessionid);
        cJSON_AddItemToObject(root, "psessionid", cJSON_CreateString(psessionid));
    } else {
        cJSON_AddItemToObject(root, "psessionid", cJSON_CreateNull());
    }

    cJSON_AddItemToObject(root, "tlevel", cJSON_CreateNumber(beentry->thread_level));
    cJSON_AddItemToObject(root, "smpid", cJSON_CreateNumber(beentry->smpid));
    cJSON_AddItemToObject(root, "userid", cJSON_CreateNumber(beentry->userid));

    if (beentry->st_appname)
        cJSON_AddItemToObject(root, "application_name", cJSON_CreateString(beentry->st_appname));
    else
        cJSON_AddItemToObject(root, "application_name", cJSON_CreateNull());
}
/*
 * format active session profile to json
 */
static void FormatActiveSessInfoToJson(SessionHistEntry *beentry)
{
    cJSON * root =  cJSON_CreateObject();
    char *cjson_str = NULL;
    FormatBasicInfo(root, beentry);
    FormatWaitEventInfo(root, beentry);
    FormatBlockInfo(root, beentry);
    FormatClientInfo(root, beentry);
    FormatSQLInfo(root, beentry);
    cjson_str = cJSON_PrintUnformatted(root);
    ereport(LOG, (errcode(ERRCODE_ACTIVE_SESSION_PROFILE),
        errmsg("%s", cjson_str), errhidestmt(true), errhideprefix(true)));
    pfree_ext(cjson_str);
    cJSON_Delete(root);
}

static void GetClientTuple(Datum* values, bool* nulls, SessionHistEntry *beentry)
{
    SockAddr zero_clientaddr;
    /* A zeroed client addr means we don't know */
    errno_t rc = memset_s(&zero_clientaddr, sizeof(zero_clientaddr), 0, sizeof(zero_clientaddr));
    securec_check(rc, "\0", "\0");
    if (memcmp(&(beentry->clientaddr), &zero_clientaddr, sizeof(zero_clientaddr)) == 0) {
        nulls[Anum_gs_asp_client_addr - 1] = true;
        nulls[Anum_gs_asp_client_hostname - 1] = true;
        nulls[Anum_gs_asp_client_port - 1] = true;
    } else {
        if (beentry->clientaddr.addr.ss_family == AF_INET
#ifdef HAVE_IPV6
            || beentry->clientaddr.addr.ss_family == AF_INET6
#endif
            ) {
            char remote_host[NI_MAXHOST];
            char remote_port[NI_MAXSERV];
            int ret;
            remote_host[0] = '\0';
            remote_port[0] = '\0';
            ret = pg_getnameinfo_all(&beentry->clientaddr.addr,
                beentry->clientaddr.salen, remote_host, sizeof(remote_host),
                remote_port, sizeof(remote_port), NI_NUMERICHOST | NI_NUMERICSERV);
            if (ret == 0) {
                clean_ipv6_addr(beentry->clientaddr.addr.ss_family, remote_host);
                values[Anum_gs_asp_client_addr - 1] =
                    DirectFunctionCall1(inet_in, CStringGetDatum(remote_host));
                if (beentry->clienthostname && beentry->clienthostname[0])
                    values[Anum_gs_asp_client_hostname - 1] = CStringGetTextDatum(beentry->clienthostname);
                else
                    nulls[Anum_gs_asp_client_hostname - 1] = true;
                values[Anum_gs_asp_client_port - 1] = Int32GetDatum(atoi(remote_port));
            } else {
                nulls[Anum_gs_asp_client_addr - 1] = true;
                nulls[Anum_gs_asp_client_hostname - 1] = true;
                nulls[Anum_gs_asp_client_port - 1] = true;
            }
        } else if (beentry->clientaddr.addr.ss_family == AF_UNIX) {
           /*
            * Unix sockets always reports NULL for host and -1 for
            * port, so it's possible to tell the difference to
            * connections we have no permissions to view, or with
            * errors.
            */
            nulls[Anum_gs_asp_client_addr - 1] = true;
            nulls[Anum_gs_asp_client_hostname - 1] = true;
            values[Anum_gs_asp_client_port - 1] = DatumGetInt32(-1);
        } else {
            /* Unknown address type, should never happen */
            nulls[Anum_gs_asp_client_addr - 1] = true;
            nulls[Anum_gs_asp_client_hostname - 1] = true;
            nulls[Anum_gs_asp_client_port - 1] = true;
        }
    }

}

static void GetQueryTuple(Datum* values, bool* nulls, SessionHistEntry *beentry)
{
    values[Anum_gs_asp_query_id - 1] = Int64GetDatum(beentry->queryid);
    if (beentry->unique_sql_key.unique_sql_id != 0) {
        values[Anum_gs_asp_unique_query_id - 1] = Int64GetDatum(beentry->unique_sql_key.unique_sql_id);
        values[Anum_gs_asp_user_id - 1] = ObjectIdGetDatum(beentry->unique_sql_key.user_id);
        values[Anum_gs_asp_cn_id - 1] = UInt32GetDatum(beentry->unique_sql_key.cn_id);
    } else {
        nulls[Anum_gs_asp_unique_query_id - 1] = true;
        nulls[Anum_gs_asp_user_id - 1] = true;
        nulls[Anum_gs_asp_cn_id - 1] = true;
    }
    if (need_normalize_unique_string() && beentry->unique_sql_key.unique_sql_id != 0) {
        uint32 hashCode = AspUniqueSQLHashCode(&beentry->unique_sql_key, sizeof(beentry->unique_sql_key));
        LockAspUniqueSQLHashPartition(hashCode, LW_SHARED);
        /* add lock for hash table */
        ASHUniqueSQL *entry = (ASHUniqueSQL*)hash_search(g_instance.stat_cxt.ASHUniqueSQLHashtbl,
            &beentry->unique_sql_key, HASH_FIND, NULL);
        if (entry == NULL || entry->unique_sql == NULL)
            nulls[Anum_gs_asp_unique_query - 1] = true;
        else
            values[Anum_gs_asp_unique_query - 1] = CStringGetTextDatum(entry->unique_sql);
        UnlockAspUniqueSQLHashPartition(hashCode);
    } else {
        nulls[Anum_gs_asp_unique_query - 1] = true;
    }
}

static void GetBlockInfo(Datum* values, bool* nulls, const SessionHistEntry *beentry)
{
    struct LOCKTAG locktag = beentry->locallocktag.lock;

    if (beentry->locallocktag.lock.locktag_lockmethodid == 0) {
        nulls[Anum_gs_asp_locktag - 1] = true;
        nulls[Anum_gs_asp_lockmode - 1] = true;
        nulls[Anum_gs_asp_block_sessionid - 1] = true;
        return;
    }

    if ((beentry->waitevent & 0xFF000000) == PG_WAIT_LOCK) {
        char* blocklocktag = LocktagToString(locktag);
        values[Anum_gs_asp_locktag - 1] = CStringGetTextDatum(blocklocktag);
        values[Anum_gs_asp_lockmode - 1] = CStringGetTextDatum(
        (GetLockmodeName(beentry->locallocktag.lock.locktag_lockmethodid,
                         beentry->locallocktag.mode)));
        values[Anum_gs_asp_block_sessionid - 1] = Int64GetDatum(beentry->st_block_sessionid);
        pfree_ext(blocklocktag);
    } else {
        nulls[Anum_gs_asp_locktag - 1] = true;
        nulls[Anum_gs_asp_lockmode - 1] = true;
        nulls[Anum_gs_asp_block_sessionid - 1] = true;
    }
}

static void SwitchStatus(BackendState state, Datum* values, bool* nulls)
{
    if (state == STATE_UNDEFINED) {
        nulls[Anum_gs_asp_state - 1] = true;
        return;
    }
    values[Anum_gs_asp_state - 1] = CStringGetTextDatum(SwitchStatusDesc(state));
}

static void GetTuple(Datum* values, int val_len, bool* nulls, int null_len, SessionHistEntry *beentry)
{
    Assert(val_len == Natts_gs_asp && null_len == Natts_gs_asp);
    values[Anum_gs_asp_sample_id - 1] = Int64GetDatum(beentry->sample_id);
    values[Anum_gs_asp_sample_time - 1] = TimestampTzGetDatum(beentry->sample_time);
    values[Anum_gs_asp_need_flush_sample - 1] = BoolGetDatum(beentry->need_flush_sample);
    values[Anum_gs_asp_databaseid - 1] = ObjectIdGetDatum(beentry->databaseid);
    values[Anum_gs_asp_tid - 1] = Int64GetDatum(beentry->procpid);
    values[Anum_gs_asp_sessionid - 1] = Int64GetDatum(beentry->session_id);
    values[Anum_gs_asp_start_time - 1] = TimestampTzGetDatum(beentry->start_time);

    if (beentry->xact_start_time != 0) {
        values[Anum_gs_asp_xact_start_time - 1] = TimestampTzGetDatum(beentry->xact_start_time);
    } else {
        nulls[Anum_gs_asp_xact_start_time - 1] = true;
    }

    if (beentry->query_start_time != 0) {
        values[Anum_gs_asp_query_start_time - 1] = TimestampTzGetDatum(beentry->query_start_time);
    } else {
        nulls[Anum_gs_asp_query_start_time - 1] = true;
    }
   
    /* waitEvent info */
    if (beentry->waitevent != 0) {
        uint32 raw_wait_event;
        /* read only once be atomic */
        raw_wait_event = UINT32_ACCESS_ONCE(beentry->waitevent);
        values[Anum_gs_asp_event - 1] = CStringGetTextDatum(pgstat_get_wait_event(raw_wait_event));
        values[Anum_gs_asp_wait_status - 1] = CStringGetTextDatum(pgstat_get_waitstatusdesc(raw_wait_event));
    } else {
        values[Anum_gs_asp_event - 1] = CStringGetTextDatum(pgstat_get_waitstatusname(beentry->waitstatus));
        char* waitStatus = GetWaitStatusInfo(beentry);
        values[Anum_gs_asp_wait_status - 1] = CStringGetTextDatum(waitStatus);
        pfree_ext(waitStatus);
    }

    values[Anum_gs_asp_lwtid - 1] = Int32GetDatum(beentry->tid);
    if (0 != beentry->psessionid)
        values[Anum_gs_asp_psessionid - 1] = Int64GetDatum(beentry->psessionid);
    else
        nulls[Anum_gs_asp_psessionid - 1] = true;
    values[Anum_gs_asp_tlevel - 1] = Int32GetDatum(beentry->thread_level);
    values[Anum_gs_asp_smpid - 1] = UInt32GetDatum(beentry->smpid);

    values[Anum_gs_asp_useid - 1] = ObjectIdGetDatum(beentry->userid);
    if (beentry->st_appname)
        values[Anum_gs_asp_application_name - 1] = CStringGetTextDatum(beentry->st_appname);
    else
        nulls[Anum_gs_asp_application_name - 1] = true;

    GetBlockInfo(values, nulls, beentry);
    GetClientTuple(values, nulls, beentry);
    GetQueryTuple(values, nulls, beentry);
    char* gId = GetGlobalSessionStr(beentry->globalSessionId);
    values[Anum_gs_asp_global_sessionid - 1] = CStringGetTextDatum(gId);
    SwitchStatus(beentry->state, values, nulls);

    pfree(gId);
}

static void WriteToSysTable(SessionHistEntry *beentry)
{
    Relation rel = heap_open(GsAspRelationId, RowExclusiveLock);

    /* Insert a job to pg_asp.*/
    Datum values[Natts_gs_asp];
    bool nulls[Natts_gs_asp];
    errno_t rc = memset_s(values, sizeof(values), 0, sizeof(values));
    securec_check(rc, "\0", "\0");
    rc = memset_s(nulls, sizeof(nulls), false, sizeof(nulls));
    securec_check_c(rc, "\0", "\0");

    /* get active session profile tuple */
    GetTuple(values, Natts_gs_asp, nulls, Natts_gs_asp, beentry);
    HeapTuple tuple = heap_form_tuple(RelationGetDescr(rel), values, nulls);
    simple_heap_insert(rel, tuple);
    CatalogUpdateIndexes(rel, tuple);
    heap_freetuple_ext(tuple);
    heap_close(rel, RowExclusiveLock);
}

/*
 * Convert the active session data in buff to json format and write it to a file
 * By default, the data in the buff is written to the file at 10: 1
 */
static void WriteActiveSessInfo()
{
    PreventCommandIfReadOnly("ASP flushing");
    ActiveSessHistArrary *active_sess_hist_arrary = g_instance.stat_cxt.active_sess_hist_arrary;
    /* if the size of table is bigger, then delete older data in table */
    CleanAspTable();
    PG_TRY();
    {
        StartTransactionCommand();
        PushActiveSnapshot(GetTransactionSnapshot());
        for (uint32 i = 0; i < active_sess_hist_arrary->max_size; i++) {
            SessionHistEntry *ash_arrary_slot = active_sess_hist_arrary->active_sess_hist_info + i;
            if (ash_arrary_slot->need_flush_sample) {
                if (strcmp(u_sess->attr.attr_common.asp_flush_mode, "table") == 0 ||
                    strcmp(u_sess->attr.attr_common.asp_flush_mode, "all") == 0) {
                    WriteToSysTable(ash_arrary_slot);
                } 
                if (strcmp(u_sess->attr.attr_common.asp_flush_mode, "file") == 0 ||
                    strcmp(u_sess->attr.attr_common.asp_flush_mode, "all") == 0) {
                    FormatActiveSessInfoToJson(ash_arrary_slot);
                }
            }
        }
        PopActiveSnapshot();
        CommitTransactionCommand();
    }
    PG_CATCH();
    {
        ereport(WARNING, (errcode(ERRCODE_WARNING), errmsg("flush pg_asp table into disk failed")));
        PopActiveSnapshot();
        AbortCurrentTransaction();
        PG_RE_THROW();
    }
    PG_END_TRY();
    CleanupAspUniqueSqlHash();
}

/*
 * Save the unique query into hash table
 * all the unique query come from UniqueSQLHashtbl
 * UniqueSQLHashtbl is reset, sample may can not find unique query from UniqueSQLHashtbl
 */
static void UpdataAspUniqueSQL(UniqueSQLKey key)
{
    ASHUniqueSQL* ash_entry = NULL;
    bool found = false;

    uint32 hashCode = AspUniqueSQLHashCode(&key, sizeof(key));
    LockAspUniqueSQLHashPartition(hashCode, LW_EXCLUSIVE);
    /* step 1.  find ASHUniqueSQLHashtbl has the unique query */
    ash_entry = (ASHUniqueSQL*)hash_search(g_instance.stat_cxt.ASHUniqueSQLHashtbl, &key, HASH_ENTER, &found);
    /* out of memory */
    if (ash_entry == NULL) {
        UnlockAspUniqueSQLHashPartition(hashCode);
        return;
    }
    if (!found) {
        /* step 2. find the unique query from UniqueSQLHashtbl, then insert into ASHUniqueSQLHashtbl */
        ash_entry->unique_sql = (char*)(ash_entry + 1);
        FindUniqueSQL(key, ash_entry->unique_sql);
    }
    UnlockAspUniqueSQLHashPartition(hashCode);
}
/* 
 * copy_active_sess_entry - copy active session history from BackendStatusArray
 * @ return the stat of entry of BackendStatusArray
 */
static void CopyActiveSessInfo(uint64 sample_id, bool need_flush_sample,
    PgBackendStatus* beentry, SessionHistEntry *ash_arrary_slot)
{
    /* read session stat from BackendStatusArray */
    ash_arrary_slot->sample_id = sample_id;
    ash_arrary_slot->need_flush_sample = need_flush_sample;
    ash_arrary_slot->sample_time = GetCurrentTimestamp();

    ash_arrary_slot->session_id = beentry->st_sessionid; 
    ash_arrary_slot->start_time = beentry->st_proc_start_timestamp;
    ash_arrary_slot->psessionid = beentry->st_parent_sessionid;
    ash_arrary_slot->is_flushed_sample = false;
    ash_arrary_slot->databaseid = beentry->st_databaseid;
    ash_arrary_slot->userid = beentry->st_userid;
    ash_arrary_slot->waitevent = beentry->st_waitevent;
    ash_arrary_slot->waitstatus = beentry->st_waitstatus;
    ash_arrary_slot->clientaddr = beentry->st_clientaddr;
    ash_arrary_slot->queryid = beentry->st_queryid;
    ash_arrary_slot->unique_sql_key = beentry->st_unique_sql_key;
    ash_arrary_slot->thread_level = beentry->st_thread_level;
    ash_arrary_slot->smpid = beentry->st_smpid;
    ash_arrary_slot->tid = beentry->st_tid;
    ash_arrary_slot->procpid = beentry->st_procpid;
    ash_arrary_slot->tid = beentry->st_tid;
    ash_arrary_slot->procpid = beentry->st_procpid;
    ash_arrary_slot->xid = beentry->st_xid;
    ash_arrary_slot->waitnode_count = beentry->st_waitnode_count;
    ash_arrary_slot->nodeid = beentry->st_nodeid;
    ash_arrary_slot->plannodeid = beentry->st_plannodeid;
    ash_arrary_slot->libpq_wait_nodeid = beentry->st_libpq_wait_nodeid;
    ash_arrary_slot->libpq_wait_nodecount = beentry->st_libpq_wait_nodecount;
    ash_arrary_slot->waitstatus_phase = beentry->st_waitstatus_phase;
    ash_arrary_slot->numnodes = beentry->st_numnodes;
    ash_arrary_slot->locallocktag = beentry->locallocktag;
    ash_arrary_slot->st_block_sessionid = beentry->st_block_sessionid;
    ash_arrary_slot->globalSessionId = beentry->globalSessionId;
    ash_arrary_slot->xact_start_time = beentry->st_xact_start_timestamp;
    ash_arrary_slot->query_start_time = beentry->st_activity_start_timestamp;
    ash_arrary_slot->state = beentry->st_state;
}
static bool IsValidEntry(PgBackendStatus* entry)
{
    bool state = false;
    if (entry->st_procpid > 0 || entry->st_sessionid > 0) {
        switch (entry->st_state) {
            case STATE_RUNNING:
            case STATE_IDLEINTRANSACTION:
            case STATE_FASTPATH:
            case STATE_IDLEINTRANSACTION_ABORTED:
            case STATE_DISABLED:
            case STATE_RETRYING:
                state = true;
                break;
            case STATE_UNDEFINED:
            case STATE_IDLE:
            case STATE_COUPLED:
            case STATE_DECOUPLED:
                state = false;
                break;
            default:
                ereport(WARNING, (errmsg("Invalid session state:%d", entry->st_state)));
                break;
        }
    } else {
        return false;
    }
    return state;
}

static void CollectActiveSessionStatus(uint64 sample_id, bool need_flush_sample)
{
    PgBackendStatus* beentry = NULL;
    errno_t rc = EOK;
    beentry = t_thrd.shemem_ptr_cxt.BackendStatusArray + BackendStatusArray_size - 1;
    ActiveSessHistArrary *active_sess_hist_arrary = g_instance.stat_cxt.active_sess_hist_arrary;

    /* Insert data into rolling buff from the last position */
    SessionHistEntry *ash_arrary_slot =
        active_sess_hist_arrary->active_sess_hist_info +  active_sess_hist_arrary->curr_index;
    PgBackendStatus* medium_beentry = (PgBackendStatus*)palloc(sizeof(PgBackendStatus));
    for (int i = 1; i <= BackendStatusArray_size; i++) {
        ash_arrary_slot->changCount++;
        for (;;) {
            /*
             * Follow the protocol of retrying if st_changecount changes while we
             * copy the entry, or if it's odd.  (The check for odd is needed to
             * cover the case where we are able to completely copy the entry while
             * the source backend is between increment steps.)  We use a volatile
             * pointer here to ensure the compiler doesn't try to get cute.
            */
            int save_changecount = beentry->st_changecount;

            (void)memset_s(ash_arrary_slot->st_appname, NAMEDATALEN, 0, NAMEDATALEN);
            rc = strcpy_s(ash_arrary_slot->st_appname, NAMEDATALEN, (char*)beentry->st_appname);
            securec_check(rc, "", "");
            (void)memset_s(ash_arrary_slot->clienthostname, NAMEDATALEN, 0, NAMEDATALEN);
            rc = strcpy_s(ash_arrary_slot->clienthostname, NAMEDATALEN, (char*)beentry->st_clienthostname);
            securec_check(rc, "", "");
            (void)memset_s(ash_arrary_slot->relname, NAMEDATALEN, 0, NAMEDATALEN);
            rc = strcpy_s(ash_arrary_slot->relname, 2 * NAMEDATALEN, (char*)beentry->st_relname);
            securec_check(rc, "", "");
            (void)memset_s(medium_beentry, sizeof(PgBackendStatus), 0, sizeof(PgBackendStatus));
            rc = memcpy_s(medium_beentry, sizeof(PgBackendStatus), beentry, sizeof(PgBackendStatus));
            securec_check_c(rc, "\0", "\0");
            medium_beentry->st_block_sessionid = beentry->st_block_sessionid;

            if (save_changecount == beentry->st_changecount && ((unsigned int)save_changecount & 1) == 0)
                break;
            /* Make sure we can break out of loop if stuck... */
            CHECK_FOR_INTERRUPTS();
        }

        ash_arrary_slot->changCount++;
        /* 
         * the active session history info will be copy to active_sess_hist_arrary
         * At the same time, the unique query will be recorded in the hash table
        */
        if (IsValidEntry(medium_beentry)) {
            CopyActiveSessInfo(sample_id, need_flush_sample, medium_beentry, ash_arrary_slot);
            if (need_normalize_unique_string() && ash_arrary_slot->unique_sql_key.unique_sql_id != 0) {
                UpdataAspUniqueSQL(ash_arrary_slot->unique_sql_key);
            }
            /* the slot of arrary will ++, the active session history info be copy to arrary */
            pg_atomic_add_fetch_u32(&active_sess_hist_arrary->curr_index, 1);
            ash_arrary_slot++;
            /* 
             * Before sample data, determine whether the buff is full
             * if the rolling buff is full, Write data in buff to disk 
             */
            if (active_sess_hist_arrary->curr_index >= active_sess_hist_arrary->max_size) {
                active_sess_hist_arrary->curr_index = 0;
                ash_arrary_slot = active_sess_hist_arrary->active_sess_hist_info;
                (void)WriteActiveSessInfo();
            }
        }
        beentry--;
    }
    pfree_ext(medium_beentry);
}
static void ProcessSignal(void)
{
    /*
     * Ignore all signals usually bound to some action in the postmaster,
     * except SIGHUP, SIGTERM and SIGQUIT.
     */
    (void)gspqsignal(SIGHUP, asp_sighup_handler);
    (void)gspqsignal(SIGURG, print_stack);
    (void)gspqsignal(SIGINT, SIG_IGN);
    (void)gspqsignal(SIGTERM, instr_asp_exit); /* cancel current query and exit */
    (void)gspqsignal(SIGQUIT, quickdie);
    (void)gspqsignal(SIGUSR1, procsignal_sigusr1_handler);
    (void)gspqsignal(SIGALRM, SIG_IGN);
    (void)gspqsignal(SIGPIPE, SIG_IGN);
    (void)gspqsignal(SIGUSR2, SIG_IGN);
    (void)gspqsignal(SIGCHLD, SIG_DFL);
    (void)gspqsignal(SIGTTIN, SIG_DFL);
    (void)gspqsignal(SIGTTOU, SIG_DFL);
    (void)gspqsignal(SIGCONT, SIG_DFL);
    (void)gspqsignal(SIGWINCH, SIG_DFL);

    gs_signal_setmask(&t_thrd.libpq_cxt.UnBlockSig, NULL);
    (void)gs_signal_unblock_sigusr2();
    if (u_sess->proc_cxt.MyProcPort->remote_host)
        pfree(u_sess->proc_cxt.MyProcPort->remote_host);
    u_sess->proc_cxt.MyProcPort->remote_host = pstrdup("localhost");

    t_thrd.wlm_cxt.thread_node_group = &g_instance.wlm_cxt->MyDefaultNodeGroup;  // initialize the default value
    t_thrd.wlm_cxt.thread_climgr = &t_thrd.wlm_cxt.thread_node_group->climgr;
    t_thrd.wlm_cxt.thread_srvmgr = &t_thrd.wlm_cxt.thread_node_group->srvmgr;
}

static void SetThrdCxt(void)
{
    t_thrd.mem_cxt.msg_mem_cxt = AllocSetContextCreate(TopMemoryContext,
        "MessageContext",
        ALLOCSET_DEFAULT_MINSIZE,
        ALLOCSET_DEFAULT_INITSIZE,
        ALLOCSET_DEFAULT_MAXSIZE);
    /*Create the memory context we will use in the main loop.*/
    t_thrd.mem_cxt.mask_password_mem_cxt = AllocSetContextCreate(TopMemoryContext,
        "MaskPasswordCtx",
        ALLOCSET_DEFAULT_MINSIZE,
        ALLOCSET_DEFAULT_INITSIZE,
        ALLOCSET_DEFAULT_MAXSIZE);
    /*
     * Create a resource owner to keep track of our resources (currently only
     * buffer pins).
     */
    t_thrd.utils_cxt.CurrentResourceOwner = ResourceOwnerCreate(NULL, "Asp",
        THREAD_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_DFX));
}

static void ReloadInfo()
{
    /* Reload configuration if we got SIGHUP from the postmaster.*/
    if (t_thrd.ash_cxt.got_SIGHUP) {
        t_thrd.ash_cxt.got_SIGHUP = false;
        ProcessConfigFile(PGC_SIGHUP);
    }
    if (IsGotPoolReload()) {
        processPoolerReload();
        ResetGotPoolReload(false);
    }
}

NON_EXEC_STATIC void ActiveSessionCollectMain()
{
    char username[NAMEDATALEN] = {'\0'};

    /* we are a postmaster subprocess now */
    IsUnderPostmaster = true;
    JobAspIAm();
    /* reset MyProcPid */
    t_thrd.proc_cxt.MyProcPid = gs_thread_self();
    /* record Start Time for logging */
    t_thrd.proc_cxt.MyStartTime = time(NULL);

    t_thrd.proc_cxt.MyProgName = "ASP";
    u_sess->attr.attr_common.application_name = pstrdup("ASP");
    SetProcessingMode(InitProcessing);
    ProcessSignal();
    /* Early initialization */
    BaseInit();
#ifndef EXEC_BACKEND
    InitProcess();
#endif
    t_thrd.proc_cxt.PostInit->SetDatabaseAndUser((char*)pstrdup(DEFAULT_DATABASE), InvalidOid, username);
    t_thrd.proc_cxt.PostInit->InitAspWorker();
    SetProcessingMode(NormalProcessing);
    on_shmem_exit(PGXCNodeCleanAndRelease, 0);
    /* Identify myself via ps */
    init_ps_display("ASP process", "", "", "");
    SetThrdCxt();
    u_sess->proc_cxt.MyProcPort->SessionStartTime = GetCurrentTimestamp();
    Reset_Pseudo_CurrentUserId();
    /*initialize current pool handles, it's also only once*/
    exec_init_poolhandles();
    pgstat_bestart();
    pgstat_report_appname("Asp");
    ereport(LOG, ( errmsg("ASP thread start")));
    pgstat_report_activity(STATE_IDLE, NULL);
    if (g_instance.attr.attr_storage.dms_attr.enable_dms && !SS_OFFICIAL_PRIMARY) {
        u_sess->attr.attr_common.enable_asp = false;
    }
    SubAspWorker();
}

void Asp::SubAspWorker()
{
    bool need_flush_sample = false;
    uint64 sample_id = 0;
    while (!t_thrd.ash_cxt.need_exit && ENABLE_ASP) {
        ReloadInfo();
        pgstat_report_activity(STATE_RUNNING, NULL);
        PG_TRY();
        {
            /* default flush to disk every 10 samples */
            if ((sample_id % u_sess->attr.attr_common.asp_flush_rate) == 0)
                need_flush_sample = true;
            else
                need_flush_sample = false;
            CollectActiveSessionStatus(sample_id, need_flush_sample);
            if (OidIsValid(u_sess->proc_cxt.MyDatabaseId))
                pgstat_report_stat(true);
            ASPSleep(u_sess->attr.attr_common.asp_sample_interval);
            sample_id++;
        }
        PG_CATCH();
        {
            EmitErrorReport();
            FlushErrorState();
            ereport(WARNING, (errcode(ERRCODE_WARNING), errmsg("ASP failed")));
            ASPSleep(SECS_PER_MINUTE);
        }
        PG_END_TRY();
    }
    gs_thread_exit(0);
}

static void InitTupleAttr(FuncCallContext** funcctx)
{
    MemoryContext oldcontext;
    TupleDesc tupdesc = NULL;
    int i = 0;
    oldcontext = MemoryContextSwitchTo((*funcctx)->multi_call_memory_ctx);
    tupdesc = CreateTemplateTupleDesc(ATTR_NUM, false);
    TupleDescInitEntry(tupdesc, (AttrNumber)++i, "sampleid", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)++i, "sample_time", TIMESTAMPTZOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)++i, "need_flush_sample", BOOLOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)++i, "databaseid", OIDOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)++i, "thread_id", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)++i, "sessionid", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)++i, "start_time", TIMESTAMPTZOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)++i, "cur_event", TEXTOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)++i, "lwtid", INT4OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)++i, "psessionid", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)++i, "tlevel", INT4OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)++i, "smpid", INT4OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)++i, "userid", OIDOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)++i, "application_name", TEXTOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)++i, "client_addr", INETOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)++i, "client_hostname", TEXTOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)++i, "client_port", INT4OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)++i, "query_id", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)++i, "unique_query_id", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)++i, "user_id", OIDOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)++i, "cn_id", INT4OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)++i, "unique_query", TEXTOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)++i, "locktag", TEXTOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)++i, "lockmode", TEXTOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)++i, "block_sessionid", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)++i, "wait_status", TEXTOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)++i, "global_sessionid", TEXTOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)++i, "xact_start_time", TIMESTAMPTZOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)++i, "query_start_time", TIMESTAMPTZOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)++i, "state", TEXTOID, -1, 0);
    Assert(i == ATTR_NUM);
    (*funcctx)->tuple_desc = BlessTupleDesc(tupdesc);
    (*funcctx)->user_fctx = palloc0(sizeof(int));
    (*funcctx)->max_calls = g_instance.stat_cxt.active_sess_hist_arrary->curr_index;
    MemoryContextSwitchTo(oldcontext);
}

Datum get_local_active_session(PG_FUNCTION_ARGS)
{
    FuncCallContext* funcctx = NULL;
    if (SRF_IS_FIRSTCALL()) {
        funcctx = SRF_FIRSTCALL_INIT();
        InitTupleAttr(&funcctx);
        if (!u_sess->attr.attr_common.enable_asp) {
            ereport(WARNING, (errcode(ERRCODE_WARNING), (errmsg("GUC parameter 'enable_asp' is off"))));
            SRF_RETURN_DONE(funcctx);
        }
    }
    /* stuff done on every call of the function */
    funcctx = SRF_PERCALL_SETUP();

    if (funcctx->call_cntr < funcctx->max_calls) {
        /* for each row */
        Datum values[ATTR_NUM];
        bool nulls[ATTR_NUM] = {false};
        HeapTuple tuple = NULL;
        SessionHistEntry *beentry = NULL;
        errno_t rc = memset_s(values, sizeof(values), 0, sizeof(values));
        securec_check(rc, "\0", "\0");
        rc = memset_s(nulls, sizeof(nulls), 0, sizeof(nulls));
        securec_check(rc, "\0", "\0");
        beentry = g_instance.stat_cxt.active_sess_hist_arrary->active_sess_hist_info + funcctx->call_cntr;
        for (;;) {
            uint64 save_changecount = beentry->changCount;
            /* Values only available to same user or superuser */
            if (superuser() || isMonitoradmin(GetUserId()) || beentry->userid == GetUserId()) {
                GetTuple(values, Natts_gs_asp, nulls, Natts_gs_asp, beentry);
            } else {
                /* No permissions to view data about this session */
                for (uint32 i = 0; i < ATTR_NUM; i++) {
                    nulls[i] = true;
                }
            }
            if (save_changecount == beentry->changCount && ((unsigned int)save_changecount & 1) == 0)
                break;
        }
        tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);

        SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tuple));
    } else {
        /* nothing left */
        SRF_RETURN_DONE(funcctx);
    }
}
