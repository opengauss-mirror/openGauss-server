/* -------------------------------------------------------------------------
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2011, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *   statctl.cpp
 *   functions for statistics control
 *
 * IDENTIFICATION
 *    src/gausskernel/cbb/workload/statctl.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "catalog/pg_authid.h"
#include "commands/user.h"
#include "executor/instrument.h"
#include "gssignal/gs_signal.h"
#include "libpq/ip.h"
#include "libpq/libpq-fe.h"
#include "libpq/pqformat.h"
#include "libpq/pqsignal.h"
#include "miscadmin.h"
#include "pgxc/pgxc.h"
#include "pgxc/poolmgr.h"
#include "postmaster/syslogger.h"
#include "storage/ipc.h"
#include "storage/lmgr.h"
#include "utils/atomic.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/memprot.h"
#include "utils/lsyscache.h"
#include "utils/postinit.h"
#include "utils/ps_status.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"
#include "access/heapam.h"
#include "datatype/timestamp.h"
#include "catalog/pg_database.h"
#include "utils/errcodes.h"
#include "commands/tablespace.h"
#include "optimizer/planner.h"

#include <mntent.h>
#include "workload/commgr.h"
#include "workload/workload.h"
#include "instruments/list.h"
#include "pgxc/groupmgr.h"
#include "instruments/instr_unique_sql.h"
#include "instruments/instr_slow_query.h"

#ifdef PGXC
#include "pgxc/poolutils.h"
#endif
#define SPACE_MIN_PERCENT 85           /* used for adjustment */
#define SPACE_ERROR_PERCENT 10         /* the least deviation for adjustment */
#define MAX_MEMORY_THREHOLD (1 * 1024) /* 1GB */
#define WLM_RECORD_SQL_LEN 100
#define WLM_CLEAN_SQL_LEN 160

#define POOL_BUFF_MEMORY 32 /* KB */

#define STRING_NULL "null"

/* get max and min data in the session info, min value maybe 0, so set -1 as init value */
#define GetMaxAndMinSessionInfo(indinator, item, data)                                        \
    do {                                                                                      \
        if ((indinator)->item.max_value <= (data)) {                                          \
            (indinator)->item.max_value = (data);                                             \
        }                                                                                     \
        if ((indinator)->item.min_value == -1) {                                              \
            (indinator)->item.min_value = (data);                                             \
        } else if ((indinator)->item.min_value >= (data)) {                                   \
            (indinator)->item.min_value = (data);                                             \
        }                                                                                     \
        (indinator)->item.total_value += (data);                                              \
        (indinator)->item.avg_value = (indinator)->item.total_value / (indinator)->nodeCount; \
        (indinator)->item.skew_percent = WLMGetSkewPercent(&(indinator)->item);               \
    } while (0)

#define GetMaxAndMinUserInfo(indinator, item, data)         \
    do {                                                    \
        if ((indinator)->item.max_value <= (data)) {        \
            (indinator)->item.max_value = (data);           \
        }                                                   \
        if ((indinator)->item.min_value == -1) {            \
            (indinator)->item.min_value = (data);           \
        } else if ((indinator)->item.min_value >= (data)) { \
            (indinator)->item.min_value = (data);           \
        }                                                   \
    } while (0)

#define session_info_collect_timer 180 /* seconds */

#define G_WLM_IOSTAT_HASH(bucket) \
    g_instance.wlm_cxt->stat_manager.iostat_info_hashtbl[bucket].hashTable

extern uint64 pg_relation_perm_table_size(Relation rel);
extern uint64 pg_relation_table_size(Relation rel);

extern int64 MetaCacheGetCurrentUsedSize();

extern bool is_searchserver_api_load();
extern void* get_searchlet_resource_info(int* used_mem, int* peak_mem);
extern void ExplainOneQueryForStatistics(QueryDesc* queryDesc);

int WLMGetQueryMemDN(const QueryDesc* desc, bool isQueryDesc, bool max = true);
extern void WLMRemoteNodeExecuteCmd(const char* sql, bool is_coord_only_query = true);
extern double WLMmonitor_get_average_value(uint64 value1, uint64 value2, uint64 itv, uint32 unit);

void updateIOFlowData(const WLMUserCollInfoDetail* data, UserData* userdata);
void updateIOFlowDataOnDN(UserData* userdata);
void updateIOFlowData4GroupUserOnCN(UserData* userdata);

/*
 * function name: IsQidInvalid
 * description  : qid is invalid, while qid pointer is null or queryId is 0
 *                or procId is not in 1~1024 or stamp is 0
 * arguments    :
 *                _in_ qid: wlm qid
 * return value : true: invalid
 *                false: valid
 */
bool IsQidInvalid(const Qid* qid)
{
#ifdef ENABLE_MULTIPLE_NODES
    return (qid == NULL || qid->queryId <= 0 || qid->procId > KBYTES || qid->procId <= 0 || qid->stamp < 0);
#else
    return (qid == NULL || qid->queryId <= 0 || qid->stamp < 0);
#endif
}

/*
 * function name: ReserveQid
 * description  : generate a qid for a statement,
 *                procId is coordinator id,
 *                queryId is thread pid of the statement.
 * arguments    :
 *                _out_ qid: qid pointer to reserve
 * return value : qid pointer
 */
Qid* ReserveQid(Qid* qid)
{
    qid->procId = u_sess->pgxc_cxt.PGXCNodeId;
    qid->queryId = (IS_THREAD_POOL_WORKER ? u_sess->session_id : t_thrd.proc_cxt.MyProcPid);

    return qid;
}

/*
 * function name: ReleaseQid
 * description  : release a qid for a statement,
 *                reset procId and queryId.
 * arguments    :
 *                _in_ qid: qid pointer to release
 * return value : void
 */
void ReleaseQid(Qid* qid)
{
    qid->procId = 0;
    qid->queryId = 0;
    qid->stamp = 0;
}

/*
 * function name: IsQidEqual
 * description  : check if a qid equal to another.
 * arguments    : first qid and second qid
 *                _in_ qid1: first qid
 *                _in_ qid2: secord qid
 * return value : true: equal
 *                false: not equal
 */
bool IsQidEqual(const Qid* qid1, const Qid* qid2)
{
    return (qid1->procId == qid2->procId && qid1->queryId == qid2->queryId && qid1->stamp == qid2->stamp);
}

/*
 * function name: GetHashCode
 * description  : generate a hash code by qid
 * arguments    :
 *                _in_ qid: qid pointer to get hash code
 * return value : unsigned int32 hash code.
 */
uint32 GetHashCode(const Qid* qid)
{
    Oid newValue = qid->queryId + qid->stamp;
    return oid_hash(&newValue, sizeof(Oid));
}

/*
 * function name: GetIoStatBucket
 * description  : generate a bucket id by qid
 * arguments    :
 *                _in_ qid: qid reference to get hash code
 * return value : unsigned int32 hash code
 */
uint32 GetIoStatBucket(const Qid* qid)
{
    return GetHashCode(qid) % NUM_IO_STAT_PARTITIONS;
}

/*
 * function name: GetIoStatLockId
 * description  : generate a lock id by bucket id
 * arguments    :
 *                _in_ bucketId: bucket id to get lock id
 * return value : int lock id
 */
int GetIoStatLockId(const uint32 bucketId)
{
    return g_instance.wlm_cxt->stat_manager.iostat_info_hashtbl[bucketId].lockId;
}

/*
 * function name: InitQnodeInfo
 * description  : init qnode info
 * arguments    :
 *                _in_ pCollectInfo: collect info pointer
 * return value : void
 */
void InitQnodeInfo(WLMCollectInfo* pCollectInfo, WLMQNodeInfo* qnode)
{
    /* init qnode info */
    qnode->condition = (pthread_cond_t)PTHREAD_COND_INITIALIZER;
    qnode->removed = false;
    qnode->privilege = false;
    qnode->lcnode = NULL;
    qnode->userid = GetUserId(); /* set user id */
    qnode->priority =
        gscgroup_get_percent(t_thrd.wlm_cxt.thread_node_group, pCollectInfo->cginfo.cgroup); /* set priority */
    qnode->rp = NULL;
    qnode->sessid = (IS_THREAD_POOL_WORKER) ? u_sess->session_id :
        t_thrd.proc_cxt.MyProcPid; /* set current thread id */
}

/*
 * function name: InitDNodeInfo
 * description  : initialize dnode collect info with qid.
 * arguments    :
 *                _in_ pDNodeInfo: data node collect info to initialize
 *                _in_ qid: qid pointer to use
 * return value : void
 */
void InitDNodeInfo(WLMDNodeInfo* pDNodeInfo, const Qid* qid)
{
    int rc;

    rc = memset_s(pDNodeInfo, sizeof(WLMDNodeInfo), 0, sizeof(WLMDNodeInfo));
    securec_check(rc, "\0", "\0");

    pDNodeInfo->qid = *qid;

    /* set control group in data node info */
    rc = snprintf_s(pDNodeInfo->cgroup, NAMEDATALEN, NAMEDATALEN - 1, "%s", u_sess->wlm_cxt->control_group);
    securec_check_ss(rc, "\0", "\0");

    /* set node group in data node info */
    rc = snprintf_s(pDNodeInfo->nodegroup, NAMEDATALEN, NAMEDATALEN - 1, "%s", u_sess->wlm_cxt->wlm_params.ngroup);
    securec_check_ss(rc, "\0", "\0");
}

/*
 * function name: InitCollectInfo
 * description  : init cn collect info
 * arguments    :
 *                _in_ pCollectInfo: collect info pointer to initialize
 * return value : void
 */
void InitCollectInfo(WLMCollectInfo* pCollectInfo)
{
    int rc;

    /* reset collect info */
    pfree_ext(pCollectInfo->sdetail.statement);
    rc = memset_s(pCollectInfo, sizeof(WLMCollectInfo), 0, sizeof(WLMCollectInfo));
    securec_check(rc, "\0", "\0");

    /*
     * If control group is empty, we will get the control group
     * from the resource pool with the user id, but if we cannot
     * find the resource pool or cgroup in the resource pool is
     * invalid, we will use default group.
     */
    if (*u_sess->wlm_cxt->control_group &&
        (!u_sess->wlm_cxt->wlm_params.rpdata.cgchange || u_sess->wlm_cxt->cgroup_state == CG_USERSET)) {
        rc = snprintf_s(pCollectInfo->cginfo.cgroup,
            sizeof(pCollectInfo->cginfo.cgroup),
            sizeof(u_sess->wlm_cxt->control_group) - 1,
            "%s",
            u_sess->wlm_cxt->control_group);
    } else {
        rc = snprintf_s(pCollectInfo->cginfo.cgroup,
            sizeof(pCollectInfo->cginfo.cgroup),
            sizeof(pCollectInfo->cginfo.cgroup) - 1,
            "%s",
            u_sess->wlm_cxt->wlm_params.cgroup);
    }
    securec_check_ss(rc, "\0", "\0");

    /* initialize queue node info */
    InitQnodeInfo(pCollectInfo, &t_thrd.wlm_cxt.qnode);
}

/*
 * function name: ResetAllStatInfo
 * description  : reset all statistics info
 * arguments    : void
 * return value : void
 */
void ResetAllStatInfo(void)
{
    int rc;
    WLMGeneralParam* g_wlm_params = &u_sess->wlm_cxt->wlm_params;

    /* reset qid */
    ReleaseQid(&g_wlm_params->qid);

    /* reset flags */
    g_wlm_params->cpuctrl = 0;
    g_wlm_params->memtrack = 0;
    g_wlm_params->infoinit = 0;
    g_wlm_params->iotrack = 0;
    g_wlm_params->iocontrol = 0;
    g_wlm_params->iocount = 0;

    InitCollectInfo(t_thrd.wlm_cxt.collect_info);

    rc = memset_s(t_thrd.wlm_cxt.except_ctl, sizeof(ExceptionManager), 0, sizeof(ExceptionManager));
    securec_check(rc, "\0", "\0");

    /* set statistics control debug info */
    u_sess->wlm_cxt->wlm_debug_info.colinfo = t_thrd.wlm_cxt.collect_info;
    u_sess->wlm_cxt->wlm_debug_info.statctl = t_thrd.wlm_cxt.except_ctl;
    u_sess->wlm_cxt->wlm_debug_info.wparams = &u_sess->wlm_cxt->wlm_params;
}

/*
 * function name: WLMAlloc0NoExcept4Hash
 * description  : alloc memory without exception
 * arguments    : _in_ size: memory size
 * return value : alloc pointer
 */
void* WLMAlloc0NoExcept4Hash(Size size)
{
    USE_MEMORY_CONTEXT(hash_get_current_dynacxt());

    return palloc0_noexcept(size);
}

/*
 * function name: WLMGetSkewPercent
 * description  : Get skew percent for given data of WLMDataIndicator
 * arguments    : _in_ indicator: data of WLMDataIndicator
 * return value : skew percent
 */
template <typename DataType>
int WLMGetSkewPercent(WLMDataIndicator<DataType>* indicator)
{
    Assert(indicator != NULL);

    if (indicator->max_value == 0) {
        return 0;
    }

    return (indicator->max_value - indicator->avg_value) * FULL_PERCENT / indicator->max_value;
}

/*
 * function name: WLMInitMinValue4SessionInfo
 * description  : min value maybe 0, so set -1 as init value
 * arguments    : gendata: structure pointer that data will be set
 * return value : void
 */
void WLMInitMinValue4SessionInfo(WLMGeneralData* gendata)
{
    gendata->peakMemoryData.min_value = -1;
    gendata->usedMemoryData.min_value = -1;
    gendata->spillData.min_value = -1;
    gendata->broadcastData.min_value = -1;
    gendata->currIopsData.min_value = -1;
    gendata->peakIopsData.min_value = -1;
    gendata->dnTimeData.min_value = -1;
    gendata->cpuData.min_value = -1;
    gendata->WLMCPUTopDnInfo = (WLMTopDnList*)palloc0(1 * sizeof(WLMTopDnList));
    gendata->WLMCPUTopDnInfo->topDn.data = -1;
    gendata->WLMCPUTopDnInfo->nextTopDn = NULL;
    gendata->WLMMEMTopDnInfo = (WLMTopDnList*)palloc0(1 * sizeof(WLMTopDnList));
    gendata->WLMMEMTopDnInfo->topDn.data = -1;
    gendata->WLMMEMTopDnInfo->nextTopDn = NULL;
}

/*
 * function name: WLMCheckMinValue4SessionInfo
 * description  : check min value, if min value is -1, set it as 0
 * arguments    : gendata: structure pointer that data will be set
 * return value : void
 */
void WLMCheckMinValue4SessionInfo(WLMGeneralData* gendata)
{
    if (gendata->peakMemoryData.min_value == -1) {
        gendata->peakMemoryData.min_value = 0;
    }
    if (gendata->usedMemoryData.min_value == -1) {
        gendata->usedMemoryData.min_value = 0;
    }
    if (gendata->spillData.min_value == -1) {
        gendata->spillData.min_value = 0;
    }
    if (gendata->broadcastData.min_value == -1) {
        gendata->broadcastData.min_value = 0;
    }
    if (gendata->currIopsData.min_value == -1) {
        gendata->currIopsData.min_value = 0;
    }
    if (gendata->peakIopsData.min_value == -1) {
        gendata->peakIopsData.min_value = 0;
    }
    if (gendata->dnTimeData.min_value == -1) {
        gendata->dnTimeData.min_value = 0;
    }
    if (gendata->cpuData.min_value == -1) {
        gendata->cpuData.min_value = 0;
    }
}

void setTopNWLMSessionInfo(
    WLMGeneralData* gendata, int64 MemtotalValues, int64 CputotalValues, const char* nodeName, int TopN)
{
    WLMTopDnList* MemElem = (WLMTopDnList*)palloc0(1 * sizeof(WLMTopDnList));
    MemElem->topDn.data = MemtotalValues;
    int rc = strncpy_s(MemElem->topDn.nodeName, sizeof(MemElem->topDn.nodeName), nodeName, strlen(nodeName));
    securec_check(rc, "\0", "\0");
    WLMTopDnList* CpuElem = (WLMTopDnList*)palloc0(1 * sizeof(WLMTopDnList));
    CpuElem->topDn.data = CputotalValues;
    rc = strncpy_s(CpuElem->topDn.nodeName, sizeof(CpuElem->topDn.nodeName), nodeName, strlen(nodeName));
    securec_check(rc, "\0", "\0");
    InsertElemSortTopN(&(gendata->WLMCPUTopDnInfo), &CpuElem, TopN);
    InsertElemSortTopN(&(gendata->WLMMEMTopDnInfo), &MemElem, TopN);
}

/*
 * function name: WLMParseFunc4SessionInfo
 * description  : parse msg to suminfo -- call-back function
 * arguments    : msg: messages fetched from DN
 *              : suminfo: structure that data will be stored in
 *              : size: size of structure of suminfo
 * return value : void
 */
void WLMParseFunc4SessionInfo(StringInfo msg, void* suminfo, int size)
{
    const int TOP5 = 5;
    WLMGeneralData* gendata = (WLMGeneralData*)suminfo;

    WLMCollectInfoDetail detail;

    errno_t errval = memset_s(&detail, sizeof(detail), 0, sizeof(detail));
    securec_check_errval(errval, , LOG);

    detail.usedMemory = pq_getmsgint(msg, 4);
    detail.peakMemory = pq_getmsgint(msg, 4);
    detail.spillCount = pq_getmsgint(msg, 4);
    detail.space = (uint64)pq_getmsgint64(msg);
    detail.dnTime = pq_getmsgint64(msg);
    detail.spillSize = pq_getmsgint64(msg);
    detail.broadcastSize = pq_getmsgint64(msg);
    detail.cpuTime = pq_getmsgint64(msg);
    detail.warning = pq_getmsgint(msg, 4);
    int peak_iops = pq_getmsgint(msg, 4);
    int curr_iops = pq_getmsgint(msg, 4);

    errval = strncpy_s(detail.nodeName, sizeof(detail.nodeName), pq_getmsgstring(msg), sizeof(detail.nodeName) - 1);
    securec_check_errval(errval, , LOG);

    if (t_thrd.proc->workingVersionNum >= SLOW_QUERY_VERSION && gendata->tag == WLM_COLLECT_SESSINFO) {
        /* slow query info */
        /* tuple info & cache IO */
        gendata->slowQueryInfo.current_table_counter->t_tuples_returned += pq_getmsgint64(msg);
        gendata->slowQueryInfo.current_table_counter->t_tuples_fetched += pq_getmsgint64(msg);
        gendata->slowQueryInfo.current_table_counter->t_tuples_inserted += pq_getmsgint64(msg);
        gendata->slowQueryInfo.current_table_counter->t_tuples_updated += pq_getmsgint64(msg);
        gendata->slowQueryInfo.current_table_counter->t_tuples_deleted += pq_getmsgint64(msg);
        gendata->slowQueryInfo.current_table_counter->t_blocks_fetched += pq_getmsgint64(msg);
        gendata->slowQueryInfo.current_table_counter->t_blocks_hit += pq_getmsgint64(msg);

        /* time Info */
        for (uint32 idx = 0; idx < TOTAL_TIME_INFO_TYPES; idx++) {
            gendata->slowQueryInfo.localTimeInfoArray[idx] += pq_getmsgint64(msg);
        }

        /* plan info */
        int64 plan_size = pq_getmsgint64(msg);
        const char* plan = pq_getmsgstring(msg);
        char* query_plan = (char*) palloc0(plan_size);
        if (strstr(plan, "NoPlan") == NULL) {
            errval = memcpy_s(query_plan, plan_size, plan, plan_size);
            securec_check_errval(errval, , LOG);
            gendata->query_plan = lappend(gendata->query_plan, query_plan);
        }
    }

    pq_getmsgend(msg);

    gendata->nodeCount++;
    gendata->warning |= detail.warning;
    if (detail.spillSize / MBYTES >= WARNING_SPILL_SIZE) {
        gendata->warning |= (1 << WLM_WARN_SPILL_FILE_LARGE);
    }

    detail.spillSize = (detail.spillSize + MBYTES - 1) / MBYTES;         /* From byte to MB , rounded up */
    detail.broadcastSize = (detail.broadcastSize + MBYTES - 1) / MBYTES; /* From byte to MB , rounded up */
    detail.cpuTime = detail.cpuTime / USECS_PER_MSEC;

    /* Get max and min values for genenal info. */
    GetMaxAndMinSessionInfo(gendata, peakMemoryData, detail.peakMemory);
    GetMaxAndMinSessionInfo(gendata, usedMemoryData, detail.usedMemory);
    GetMaxAndMinSessionInfo(gendata, spillData, detail.spillSize);
    GetMaxAndMinSessionInfo(gendata, broadcastData, detail.broadcastSize);
    GetMaxAndMinSessionInfo(gendata, cpuData, detail.cpuTime);
    GetMaxAndMinSessionInfo(gendata, dnTimeData, detail.dnTime);
    GetMaxAndMinSessionInfo(gendata, peakIopsData, peak_iops);
    GetMaxAndMinSessionInfo(gendata, currIopsData, curr_iops);
    setTopNWLMSessionInfo(gendata, detail.usedMemory, detail.cpuTime, detail.nodeName, TOP5);
    if (detail.spillCount > 0 || detail.spillSize > 0) {
        gendata->spillNodeCount++;
    }

    /* sum the info */
    gendata->totalSpace += detail.space;
}

/*
 * function name: WLMParseFunc4SessionInfo
 * description  : parse msg to suminfo -- call-back function
 * arguments    : msg: messages fetched from DN
 *              : suminfo: structure that data will be stored in
 *              : size: size of structure of suminfo
 * return value : void
 */
void WLMParseFunc4CollectInfo(StringInfo msg, void* suminfo, int size)
{
    WLMCollectInfoDetail* info = (WLMCollectInfoDetail*)suminfo;

    WLMCollectInfoDetail detail;

    errno_t errval = memset_s(&detail, sizeof(detail), 0, sizeof(detail));
    securec_check_errval(errval, , LOG);

    /* fetch IO utilization and cpu utilization */
    int io_util = pq_getmsgint(msg, 4);
    int cpu_util = pq_getmsgint(msg, 4);
    detail.cpuCnt = pq_getmsgint(msg, 4);

    pq_getmsgend(msg);

    GetMaxAndMinCollectInfo(info, IOUtil, io_util);
    GetMaxAndMinCollectInfo(info, CpuUtil, cpu_util);

    info->cpuCnt += detail.cpuCnt;

    info->nodeCount++;
}

/*
 * @Description   : parse io info
 * @IN            : msg:  message fetched from DNs
 * @OUT           : info: the parsed values will be stored here
 * @IN            : size: sizeof (info)
 * @RETURN        : void
 */
void WLMParseIOInfo(StringInfo msg, void* info, int size)
{
    WLMIoGeninfo* detail = (WLMIoGeninfo*)info;

    int curr_iops = pq_getmsgint(msg, 4);
    int peak_iops = pq_getmsgint(msg, 4);

    pq_getmsgend(msg);

    detail->nodeCount++;

    ereport(DEBUG1, (errmsg("get iops %d|%d", curr_iops, peak_iops)));

    GetMaxAndMinSessionInfo(detail, currIopsData, curr_iops);
    GetMaxAndMinSessionInfo(detail, peakIopsData, peak_iops);
}

/*
 * function name: WLMParseFunc4UserInfo
 * description  : parse msg to suminfo -- call-back function
 * arguments    : msg: messages fetched from DN
 *              : suminfo: structure that data will be stored in
 *              : size: size of structure of suminfo
 * return value : void
 */
void WLMParseFunc4UserInfo(StringInfo msg, void* suminfo, int size)
{
    WLMUserCollInfoDetail* info = (WLMUserCollInfoDetail*)suminfo;

    char nodeName[NAMEDATALEN];

    /* fetch IO utilization and cpu utilization */
    int useMemory = pq_getmsgint(msg, 4);
    int cpuUsedCnt = pq_getmsgint(msg, 4);
    int cpuTotalCnt = pq_getmsgint(msg, 4);
    int peak_iops = pq_getmsgint(msg, 4);
    int curr_iops = pq_getmsgint(msg, 4);
    uint64 readBytes = pq_getmsgint64(msg);
    uint64 writeBytes = pq_getmsgint64(msg);
    uint64 readCounts = pq_getmsgint64(msg);
    uint64 writeCounts = pq_getmsgint64(msg);
    uint64 totalSpace = pq_getmsgint64(msg);
    uint64 tmpSpace = pq_getmsgint64(msg);
    uint64 spillSpace = pq_getmsgint64(msg);

    errno_t errval = strncpy_s(nodeName, sizeof(nodeName), pq_getmsgstring(msg), sizeof(nodeName) - 1);
    securec_check_errval(errval, , LOG);

    pq_getmsgend(msg);

    GetMaxAndMinUserInfo(info, usedMemoryData, useMemory);
    GetMaxAndMinUserInfo(info, currIopsData, curr_iops);
    GetMaxAndMinUserInfo(info, peakIopsData, peak_iops);

    /* IO flow data */
    info->readBytes += readBytes;
    info->writeBytes += writeBytes;
    info->readCounts += readCounts;
    info->writeCounts += writeCounts;

    info->cpuUsedCnt += cpuUsedCnt;
    info->cpuTotalCnt += cpuTotalCnt;

    info->totalSpace += totalSpace;
    info->tmpSpace += tmpSpace;
    info->spillSpace += spillSpace;

    if (cpuTotalCnt) {
        info->nodeCount++;
    }
}

/*
 * function name: WLMStrategyFuncIgnoreResult
 * description  : execute a command on each data node and ignore any result.
 * arguments    :
 *                __inout state: parallel function state pointer
 * return value : void
 */
void WLMStrategyFuncIgnoreResult(ParallelFunctionState* state)
{
    state->result = 0;
    return;
}

/*
 * function name: WLMFillGeneralInfoWithIndicator
 * description  : Fill data type of WLMGeneralInfo with type of WLMGeneralData.
 * arguments    :
 *                __inout info: data type of WLMGeneralInfo
 *                _in_ gendata: data type of WLMGeneralData
 * return value : void
 */
void WLMFillGeneralInfoWithIndicator(WLMGeneralInfo* info, const WLMGeneralData* gendata)
{
    Assert(info != NULL);
    Assert(gendata != NULL);

    errno_t errval = memset_s(info, sizeof(*info), 0, sizeof(*info));
    securec_check_errval(errval, , LOG);

    info->maxPeakChunksQuery = gendata->peakMemoryData.max_value;
    info->minPeakChunksQuery = gendata->peakMemoryData.min_value;

    info->maxCpuTime = gendata->cpuData.max_value;
    info->minCpuTime = gendata->cpuData.min_value;
    info->totalCpuTime = gendata->cpuData.total_value;

    info->maxUsedMemory = gendata->usedMemoryData.max_value;

    info->spillCount = gendata->spillData.total_value;
    info->spillNodeCount = gendata->spillNodeCount;

    info->spillSize = gendata->spillData.min_value;

    info->nodeCount = gendata->nodeCount;

    info->totalSpace = gendata->totalSpace;
}

/*
 * function name: WLMGetSessionLevelInfoInternalByNG
 * description  : get session level info internal by nodegroup
 * arguments    :
 *                _in_ pgxc_handles: pgxc node handles
 *                _in_ group_name: node group name
 *                _in_ keystr: key string
 *                _in_ gendata: gendata
 *                __out: int
 * return value : int
 */
int WLMGetSessionLevelInfoInternalByNG(const char* group_name, const char* keystr, WLMGeneralData* gendata)
{
    Assert(IS_PGXC_COORDINATOR);

    int ret = WLMRemoteInfoSenderByNG(group_name, keystr, WLM_COLLECT_ANY);

    if (ret != 0) {
        return ret;
    }

    errno_t errval = memset_s(gendata, sizeof(*gendata), 0, sizeof(*gendata));
    securec_check_errval(errval, , LOG);
    WLMInitMinValue4SessionInfo(gendata);

    gendata->tag = WLM_COLLECT_ANY;
    WLMRemoteInfoReceiverByNG(group_name, gendata, sizeof(*gendata), WLMParseFunc4SessionInfo);
    WLMCheckMinValue4SessionInfo(gendata);

    return 0;
}

/*
 * function name: WLMGetSessionLevelInfoInternal
 * description  : get session level info internal
 * arguments    :
 *                _in_ qid: qid info
 *                __out: general info
 * return value : info string
 */
int WLMGetSessionLevelInfoInternal(PGXCNodeAllHandles* pgxc_handles, const char* keystr, WLMGeneralData* gendata)
{
    Assert(IS_PGXC_COORDINATOR);

    int ret = WLMRemoteInfoSender(pgxc_handles, keystr, WLM_COLLECT_ANY);

    if (ret != 0) {
        return ret;
    }

    errno_t errval = memset_s(gendata, sizeof(*gendata), 0, sizeof(*gendata));
    securec_check_errval(errval, , LOG);
    WLMInitMinValue4SessionInfo(gendata);

    WLMRemoteInfoReceiver(pgxc_handles, gendata, sizeof(*gendata), WLMParseFunc4SessionInfo);
    WLMCheckMinValue4SessionInfo(gendata);

    return 0;
}

/*
 * function name: WLMDoSleepAndRetry
 * description  : report retry counts and sleep. if retry more than three times, report error.
 * return value : void
 */
void WLMDoSleepAndRetry(int* retryCount)
{
    (*retryCount)++;
    ereport(WARNING, (errmsg("send failed, retry: %d", *retryCount)));
    pg_usleep(3 * USECS_PER_SEC);

    if (*retryCount >= 3) {
        ereport(ERROR,
            (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE), errmsg("Remote Sender: Failed to send command to datanode")));
    }
}

/*
 * function name: WLMFetchCollectInfoFromDN
 * description  : CN fetch cpu time info from datanode.
 * arguments    :
 *                __inout pDNodeInfo: data node collect info
 * return value : void
 */
void WLMFetchCollectInfoFromDN(WLMNodeUpdateInfo* nodeinfo, int num)
{
    int retry_count = 0;

retry:
    WLMGeneralInfo geninfo;

    for (int i = 0; i < num; ++i) {
        WLMNodeUpdateInfo* info = &nodeinfo[i];

        char keystr[NAMEDATALEN] = {0};

        errno_t ssval = snprintf_s(keystr,
            sizeof(keystr),
            sizeof(keystr) - 1,
            "%u,%lu,%ld,%d",
            info->qid.procId,
            info->qid.queryId,
            info->qid.stamp,
            info->geninfo.action);
        securec_check_ssval(ssval, , LOG);

        WLMGeneralData gendata;

        int ret = WLMGetSessionLevelInfoInternalByNG(info->nodegroup, keystr, &gendata);

        if (ret != 0) {
            WLMDoSleepAndRetry(&retry_count);
            goto retry;
        }

        WLMFillGeneralInfoWithIndicator(&geninfo, &gendata);
        DeleteList(&(gendata.WLMCPUTopDnInfo));
        DeleteList(&(gendata.WLMMEMTopDnInfo));
        /* need control cpu */
        if (info->cpuctrl) {
            /* We have to save the current scan count to avoid being overwritten. */
            int scanCount = info->geninfo.scanCount;
            WLMActionTag action = info->geninfo.action;

            errno_t errval = memcpy_s(&info->geninfo, sizeof(WLMGeneralInfo), &geninfo, sizeof(WLMGeneralInfo));
            securec_check_errval(errval, , LOG);

            /* get new scan count */
            gs_lock_test_and_set(&info->geninfo.scanCount, scanCount + 1);
            info->geninfo.action = action;

            /* reset the flag to do penalty exception */
            if (info->geninfo.action == WLM_ACTION_ADJUST) {
                info->geninfo.action = WLM_ACTION_REMAIN;
            }

            ereport(DEBUG4,
                (errmsg("fetch collect info from dn. tid: %lu, "
                        "data0 is %ld",
                    info->qid.queryId,
                    info->geninfo.totalCpuTime)));
        }

        CHECK_FOR_INTERRUPTS();
    }
}

/*
 * function name: WLMGetQidByBEEntry
 * description  : get qid for the query with thread id.
 * arguments    :
 *                _in_ beentry: backend status entry
 *                _out_ qid: wlm qid
 * return value : qid pointer
 */
Qid* WLMGetQidByBEEntry(PgBackendStatus* beentry, Qid* qid)
{
    qid->procId = u_sess->pgxc_cxt.PGXCNodeId;
    qid->queryId = beentry->st_sessionid;
    qid->stamp = beentry->st_block_start_time;

    return qid;
}

/*
 * function name: WLMUpdateCgroup
 * description  : update cgroup in the hash table and
 *                the control group of the thread.
 * arguments    :
 *                _in_ ng: node group information
 *                _in_ beentry: backend status entry
 *                _in_ cgroup: control group
 * return value : qid pointer
 */
void WLMUpdateCgroup(WLMNodeGroupInfo* ng, PgBackendStatus* beentry, const char* cgroup)
{
    Qid qid;
    errno_t rc;

    WLMStatistics* backstat = (WLMStatistics*)&beentry->st_backstat;

    /* We must make sure the status of the job is pending or running */
    if (strcmp(backstat->status, "pending") != 0 && strcmp(backstat->status, "running") != 0) {
        return;
    }

    int i = 0;

    for (i = 0; i < SIG_RECVDATA_COUNT; ++i) {
        WLMSigReceiveData* data = g_instance.wlm_cxt->stat_manager.recvdata + i;

        if (gs_compare_and_swap_32(&data->used, 0, 1)) {
            data->tid = beentry->st_procpid;
            data->priority = gscgroup_get_percent(ng, cgroup);

            errno_t errval = strncpy_s(data->cgroup, sizeof(data->cgroup), cgroup, sizeof(data->cgroup) - 1);
            securec_check_errval(errval, , LOG);

            gs_atomic_add_32((int*)&g_instance.wlm_cxt->stat_manager.sendsig, 1);

            (void)LWLockAcquire(ProcArrayLock, LW_SHARED);
            int ret = SendProcSignal(data->tid, PROCSIG_UPDATE_WORKLOAD_DATA, InvalidBackendId);
            LWLockRelease(ProcArrayLock);

            if (ret) {
                ereport(ERROR,
                    (errcode(ERRCODE_INVALID_OPERATION),
                        errmsg("Fail to send signal to backend(tid:%lu).", (unsigned long)data->tid)));
            }

            break;
        }
    }

    if (i >= SIG_RECVDATA_COUNT) {
        ereport(LOG, (errmsg("receive data slot is full, update cgroup message failed")));
    }

    if (IsQidInvalid(WLMGetQidByBEEntry(beentry, &qid))) {
        return;
    }

    uint32 hashCode = WLMHashCode(&qid, sizeof(Qid));

    (void)LockSessRealTHashPartition(hashCode, LW_EXCLUSIVE);

    /*
     * We have to reset the adjust flag to avoid doing
     * penalty exception on data nodes. If the penalty
     * exception unfortunately happened, you need switch
     * new control group again.
     */
    WLMDNodeInfo* info =
        (WLMDNodeInfo*)hash_search(g_instance.wlm_cxt->stat_manager.collect_info_hashtbl, &qid, HASH_FIND, NULL);

    /* check node info tag */
    if (info != NULL && info->tag == WLM_ACTION_REMAIN) {
        /* Ignore changing cgroup this time and use new cgroup. */
        info->geninfo.action = WLM_ACTION_REMAIN;

        rc = sprintf_s(info->cgroup, sizeof(info->cgroup), "%s", cgroup);
        securec_check_ss(rc, "\0", "\0");
    }

    UnLockSessRealTHashPartition(hashCode);
}

/*
 * function name: WLMAjustCGroupByCNPid
 * description  : update control group by query thread id on coordinator
 * arguments    :
 *                _in_ ng: the node group information
 *                _in_ sess_id: session id to adjust
 *                _in_ cgroup: the new group name
 * return value : true: adjust success
 *                false: adjust failed
 */
bool WLMAjustCGroupByCNSessid(WLMNodeGroupInfo* ng, uint64 sess_id, const char* cgroup)
{
    /* We must check the group name firstly, and meanwhile create group key name. */
    if (gscgroup_check_group_name(ng, cgroup) != 0) {
        return false;
    }

    if (!g_instance.wlm_cxt->gscgroup_init_done) {
        return false;
    }

    errno_t rc;

    /*
     * We must make sure the group is in the group hash
     * table. If not, insert it into the hash table.
     */
    gscgroup_update_hashtbl(ng, u_sess->wlm_cxt->group_keyname);

    if (IS_PGXC_COORDINATOR) {
        char query[KBYTES];
        PgBackendStatus* beentry = NULL;
        PgBackendStatusNode* node = gs_stat_read_current_status(NULL);
        while (node != NULL) {
            PgBackendStatus* tmpBeentry = node->data;
            if ((tmpBeentry != NULL) && (tmpBeentry->st_sessionid == sess_id)) {
                beentry = tmpBeentry;
                break;
            }
            node = node->next;
        }

        if (beentry == NULL || beentry->st_cgname == NULL) {
            return false;
        }

        WLMStatistics* backstat = (WLMStatistics*)&beentry->st_backstat;

        /* Current control group is not changed, nothing to do on the coordinator. */
        if (strcmp(backstat->cgroup, u_sess->wlm_cxt->group_keyname) != 0) {
            if (g_instance.wlm_cxt->dynamic_workload_inited) {
                dywlm_client_move_node_to_list(ng, sess_id, u_sess->wlm_cxt->group_keyname);
            /*
             * We have to check and move the queue node to new node list
             * while it's pending in the global waiting list.
             */
            } else if (strcmp(backstat->enqueue, "Global") == 0) {
                WLMMoveNodeToList(ng, sess_id, u_sess->wlm_cxt->group_keyname);
            }

            pid_t tid = beentry->st_tid;

            int is_once = 1;

            /* attach new cgroup failed, return switching failed */
            if (gscgroup_attach_task_batch(ng, u_sess->wlm_cxt->group_keyname, tid, &is_once) == -1) {
                return false;
            }

            /*
             * Update cgroup in the collect info hash table.
             * And we must make sure the sesson's control group
             * has been changed.
             */
            WLMUpdateCgroup(ng, beentry, u_sess->wlm_cxt->group_keyname);
        }

        /* Only the job on running need switch cgroup on data nodes. */
        if (strcmp(backstat->status, "running") == 0) {
            rc = snprintf_s(query,
                sizeof(query),
                sizeof(query) - 1,
                "select pg_catalog.gs_wlm_switch_cgroup(%lu, \'%s\');",
                (ThreadId)beentry->st_queryid,
                cgroup);
            securec_check_ss(rc, "\0", "\0");

            /* attach control group for each thread for the session on each data node */
            (void)FetchStatistics4WLM(query, NULL, 0, WLMStrategyFuncIgnoreResult);
        }
        
    }

    if (IS_PGXC_DATANODE) {
        int i;
        uint32 num = 0;
        int j = 0;
        int is_changed = 1;

        /* No threads to update, nothing to do. */
        PgBackendStatusNode* node = gs_stat_read_current_status(&num);
        if (num == 0) {
            return true;
        }

        pid_t* threads = (pid_t*)palloc0(num * sizeof(pid_t));

        /* Get all threads with the coordinator process id */
        while (node != NULL) {
            PgBackendStatus* beentry = node->data;
            if (beentry != NULL && (ThreadId)beentry->st_queryid == sess_id) {
                threads[j++] = beentry->st_tid;
            }
            node = node->next;
        }

        /* Attach these threads batch with the new group. */
        for (i = 0; i < j; ++i) {
            gscgroup_attach_task_batch(ng, u_sess->wlm_cxt->group_keyname, threads[i], &is_changed);
        }

        pfree(threads);
    }

    return true;
}

/*
 * function name: WLMAdjustCGroup4EachThreadOnDN
 * description  : switch cgroup for each thread on data node.
 * arguments    :
 *                _in_ info: data node collect info pointer
 * return value : void
 */
void WLMAdjustCGroup4EachThreadOnDN(WLMDNodeInfo* info)
{
    Assert(info != NULL);

    if (g_instance.attr.attr_common.enable_thread_pool) {
        ereport(LOG, (errmsg("Cannot adjust control group in thread pool mode.")));
        return;
    }

    /* Get the node group info from hash table */
    WLMNodeGroupInfo* ng = WLMGetNodeGroupFromHTAB((char*)info->nodegroup);

    if (ng == NULL) {
        ereport(LOG, (errmsg("Failed to get node group with %s in htab.", info->nodegroup)));
        return;
    }

    /* Check the thread id whether is valid, and get the next group. */
    if (info->tid <= 0 || gscgroup_get_next_group(ng, info->cgroup) <= 0) {
        return;
    }

    /*
     * If we can get next timeshare group, we will use it
     * to attach every threads of the query on data nodes.
     */
    (void)WLMAjustCGroupByCNSessid(ng, info->qid.queryId, info->cgroup);

    ereport(LOG,
        (errmsg("adjust cgroup for each threads on dn, "
                "new cgroup: \"%s\".",
            info->cgroup)));
}

/*
 * function name: WLMCNUpdateDNodeInfoState
 * description  : update dnode info in hash table on coordinator.
 *                We set threadCount to check the info whether is in use.
 *                1: in use, 0: not in use.
 *                This is only set on coordinator.
 * arguments    :
 *                _in_ used: the state of the data node info in the hash table
 * return value : void
 */
void WLMCNUpdateDNodeInfoState(void* info, WLMActionTag action)
{
    if (info == NULL) {
        return;
    }

    int* tag = (int*)&(((WLMDNodeInfo*)info)->tag);

    /*
     * We set used flag to check the info whether is in use.
     * 1: in use, 0: not in use. This is only set on coordinator.
     */
    gs_lock_test_and_set(tag, action);
}

/*
 * function name: WLMCNUpdateIoInfoState
 * description  : update dnode IO info in hash table on coordinator.
 *                This is only set on coordinator.
 * arguments    :
 *                _in_ used: the state of the data node info in the hash table
 * return value : void
 */
void WLMCNUpdateIoInfoState(void* info, WLMActionTag action)
{
    if (info == NULL) {
        return;
    }

    int* tag = (int*)&(((WLMDNodeIOInfo*)info)->tag);

    /*
     * We set used flag to check the info whether is in use.
     * 1: in use, 0: not in use. This is only set on coordinator.
     */
    gs_lock_test_and_set(tag, action);
}

/*
 * function name: WLMCleanUpNodeInternal
 * description  : update cpu time detail in the hash table on datanodes.
 *                WLM worker thread will do this one by one
 *                in the collect info list.
 * arguments    :
 *                _in_ qid: query qid
 * return value : void
 */
void WLMCleanUpNodeInternal(const Qid* qid)
{
    if (IsQidInvalid(qid)) {
        return;
    }

    uint32 hashCode = WLMHashCode(qid, sizeof(Qid));
    (void)LockSessRealTHashPartition(hashCode, LW_EXCLUSIVE);

    WLMDNodeInfo* info =
        (WLMDNodeInfo*)hash_search(g_instance.wlm_cxt->stat_manager.collect_info_hashtbl, qid, HASH_FIND, NULL);
    if (info != NULL) {
        /*
         * We will check the thread count, if all threads has finished,
         * we will release the thread list and remove it from the hash table.
         */
        (void)pg_atomic_fetch_sub_u32((volatile uint32*)&info->threadCount, 1);

        /* No threads already, remove the info from the hash table. */
        if (info->threadCount <= 0) {
            hash_search(g_instance.wlm_cxt->stat_manager.collect_info_hashtbl, qid, HASH_REMOVE, NULL);
        }
    }

    UnLockSessRealTHashPartition(hashCode);
}

/*
 * function name: WLMCleanUpNode
 * description  : update cpu time detail in the hash table on datanodes.
 *                WLM worker thread will do this one by one
 *                in the collect info list.
 * arguments    :
 *                _in_ pDetail: data node collect detail info
 * return value : void
 */
void WLMCleanUpNode(WLMDNRealTimeInfoDetail* pDetail)
{
    /*
     * It works on the backend thread on data node,
     * if the thread need exit, we must stop and return.
     */
    if (g_instance.wlm_cxt->stat_manager.stop) {
        return;
    }

    WLMCleanUpNodeInternal(&pDetail->qid);
}

/*
 * function name: GetSkewRatioOfDN
 * description  : calculate skew ratio of dn,
 *                the skew ratio is
 *                (max - avg) / max * 100%
 * arguments    :
 *                _in_ pGenInfo: collect general info
 * return value : cpu skew percent
 */
int GetSkewRatioOfDN(const WLMGeneralInfo* pGenInfo)
{
    if (pGenInfo->maxCpuTime == 0 || pGenInfo->nodeCount == 0) {
        return 0;
    }

    int64 avgCpuTime = pGenInfo->totalCpuTime / pGenInfo->nodeCount;

    return (pGenInfo->maxCpuTime - avgCpuTime) * 100 / pGenInfo->maxCpuTime;
}

/*
 * function name: GetActionName
 * description  : get action name with tag
 * arguments    :
 *                _in_ tag: action tag
 * return value : action name
 */
char* GetActionName(WLMActionTag tag)
{
    switch (tag) {
        case WLM_ACTION_ABORT:
            return "abort";
        case WLM_ACTION_ADJUST:
            return "adjust";
        case WLM_ACTION_FINISHED:
            return "finish";
        default:
            break;
    }

    /* action name is unknown */
    return "unknown";
}

/*
 * function name: GetStatusName
 * description  : get status name with tag
 * arguments    :
 *                _in_ tag: status tag
 * return value : status name
 */
char* GetStatusName(WLMStatusTag tag)
{
    switch (tag) {
        case WLM_STATUS_RESERVE:
            return "active";
        case WLM_STATUS_PENDING:
            return "pending";
        case WLM_STATUS_RUNNING:
            return "running";
        case WLM_STATUS_FINISHED:
            return "finished";
        case WLM_STATUS_ABORT:
            return "aborted";
        default:
            break;
    }

    /* status name is unknown */
    return "unknown";
}

char* GetPendingEnqueueState()
{
    if (g_instance.wlm_cxt->dynamic_workload_inited) {
        if (t_thrd.wlm_cxt.parctl_state.preglobal_waiting) {
            return "Global";
        } else if (IsTransactionBlock()) {
            return "Transaction";
        } else if (t_thrd.wlm_cxt.parctl_state.subquery) {
            return "StoredProc";
        } else if (t_thrd.wlm_cxt.parctl_state.central_waiting) {
            return "CentralQueue";
        } else if (t_thrd.wlm_cxt.parctl_state.respool_waiting) {
            return "Respool";
        }
    } else {
        if (t_thrd.wlm_cxt.parctl_state.global_waiting) {
            return "Global";
        } else if (IsTransactionBlock()) {
            return "Transaction";
        } else if (t_thrd.wlm_cxt.parctl_state.subquery) {
            return "StoredProc";
        } else if (t_thrd.wlm_cxt.parctl_state.respool_waiting) {
            return "Respool";
        }
    }

    return "None";
}

/*
 * function name: GetEnqueueState
 * description  : get enqueue state
 * arguments    :
 *                _in_ pCollectInfo: collect info
 * return value : enqueue state:
 *                "respool": waiting in the resource pool
 *                "global": waiting in the global list
 */
char* GetEnqueueState(const WLMCollectInfo* pCollectInfo)
{
    /* Current status must be "pending". */
    if (pCollectInfo->status == WLM_STATUS_PENDING) {
        return GetPendingEnqueueState();
    }

    /*
     * Set enqueue state to "Transaction" if it's in a
     * transaction block and the role is not super user.
     */
    if (pCollectInfo->status == WLM_STATUS_RUNNING && u_sess->wlm_cxt->forced_running) {
        return "Forced None";
    }
    if (IsTransactionBlock() && !u_sess->wlm_cxt->wlm_params.rpdata.superuser) {
        return "Transaction";
    }
    if (t_thrd.wlm_cxt.parctl_state.subquery && !u_sess->wlm_cxt->wlm_params.rpdata.superuser) {
        return "StoredProc";
    }

    /* when statement finish and out of respool, we
     * should mark it not out of global reserve
     */
    if (pCollectInfo->status == WLM_STATUS_FINISHED &&
        (t_thrd.wlm_cxt.parctl_state.reserve == 1 ||
        t_thrd.wlm_cxt.parctl_state.global_reserve == 1)) {
        return "Respool";
    }

    return "None";
}

/*
 * function name: GetQueryType
 * description  : get query type
 * arguments    :
 *                _in_ pCollectInfo: collect info
 * return value : query type
 */
char* GetPendingQueryType()
{
    if (t_thrd.wlm_cxt.qnode.rp) { /* waiting in the respool */
        if (t_thrd.wlm_cxt.parctl_state.simple) {
            return "Simple";
        } else {
            return "Complicated";
        }
    } else if (t_thrd.wlm_cxt.qnode.lcnode) { /* waiting in the global queue */
        if (t_thrd.wlm_cxt.parctl_state.special) { /* check it whether is special */
            return "Internal";
        } else {
            return "Ordinary";
        }
    } else if (t_thrd.wlm_cxt.parctl_state.global_reserve) {
        return "Ordinary";
    } else if (g_instance.wlm_cxt->dynamic_workload_inited) {
        if (t_thrd.wlm_cxt.parctl_state.simple) {
            return "Ordinary";
        } else {
            return "Complicated";
        }
    } else {
        return "Internal";
    }

}

char* GetOtherQueryType(const WLMCollectInfo* pCollectInfo)
{
    if (t_thrd.wlm_cxt.parctl_state.special) {
        return "Internal";
    } else if (t_thrd.wlm_cxt.parctl_state.simple ||
               (!IsQueuedSubquery() && !g_instance.wlm_cxt->dynamic_workload_inited)) {
        return "Simple";
    } else if (pCollectInfo->execStartTime == 0 &&
               t_thrd.wlm_cxt.qnode.rp ==
                 NULL) { /* not in running or resource pool list, just like cancel a pending query */
        return "Ordinary";
    } else {
        return "Complicated";
    }
}


char* GetQueryType(const WLMCollectInfo* pCollectInfo)
{
    if (pCollectInfo->status == WLM_STATUS_PENDING) {
        return GetPendingQueryType();
    } else {
        return GetOtherQueryType(pCollectInfo);
    }
}

/*
 * function name: GetQualificationTime
 * description  : get qualification time
 * arguments    :
 *                _in_ pCollectInfo: collect info
 * return value : qualification time
 */
int GetQualificationTime(const WLMCollectInfo* pCollectInfo)
{
    switch (pCollectInfo->action) {
        case WLM_ACTION_ABORT:
            /*
             * the action is abort, we will get the qualification
             * time from abort threshold, if it's not set, will get
             * the qualification time from penalty threshold.
             */
            if (t_thrd.wlm_cxt.except_ctl->except[EXCEPT_ABORT].qualitime > 0) {
                return (int)t_thrd.wlm_cxt.except_ctl->except[EXCEPT_ABORT].qualitime;
            } else {
                return (int)t_thrd.wlm_cxt.except_ctl->except[EXCEPT_PENALTY].qualitime;
            }
        case WLM_ACTION_ADJUST:
            return (int)t_thrd.wlm_cxt.except_ctl->except[EXCEPT_PENALTY].qualitime;
        case WLM_ACTION_FINISHED:
            return (int)t_thrd.wlm_cxt.except_ctl->except[EXCEPT_PENALTY].qualitime;
        default:
            break;
    }

    return 0;
}

/*
 * function name: WLMInsertCollectInfoIntoView
 * description  : save collect info.
 * arguments    :
 *                _in_ pCollectInfo: collect info
 * return value : void
 */
void WLMInsertCollectInfoIntoView(const WLMCollectInfo* pCollectInfo)
{
    WLMContextLock stat_lock(&g_instance.wlm_cxt->stat_manager.statistics_mutex);

    stat_lock.Lock();

    /* get current position to write */
    WLMStatistics* statistics = g_instance.wlm_cxt->stat_manager.statistics + g_instance.wlm_cxt->stat_manager.index;
    int rc;

    /* write the statistics with collect info, cpu time unit is changed to second */
    statistics->blocktime =
        WLMGetTimestampDuration(pCollectInfo->blockStartTime, pCollectInfo->blockEndTime) / MSECS_PER_SEC;
    statistics->elapsedtime =
        WLMGetTimestampDuration(pCollectInfo->execStartTime, pCollectInfo->execEndTime) / MSECS_PER_SEC;
    statistics->maxcputime = pCollectInfo->sdetail.geninfo.maxCpuTime / MSECS_PER_SEC;
    statistics->totalcputime = pCollectInfo->sdetail.geninfo.totalCpuTime / MSECS_PER_SEC;
    statistics->qualitime = GetQualificationTime(pCollectInfo);
    statistics->skewpercent = GetSkewRatioOfDN(&pCollectInfo->sdetail.geninfo);
    statistics->status = GetStatusName(pCollectInfo->status);
    statistics->action = GetActionName(pCollectInfo->action);

    rc = strncpy_s(
        statistics->stmt, sizeof(statistics->stmt), pCollectInfo->sdetail.statement, sizeof(statistics->stmt) - 1);
    securec_check_c(rc, "\0", "\0");

    rc = strncpy_s(
        statistics->cgroup, sizeof(statistics->cgroup), pCollectInfo->cginfo.cgroup, sizeof(statistics->cgroup) - 1);
    securec_check_c(rc, "\0", "\0");

    g_instance.wlm_cxt->stat_manager.index++;

    /*
     * If we don't have enough space to write next
     * record, reset next position to write is 0.
     */
    if (g_instance.wlm_cxt->stat_manager.index >= g_instance.wlm_cxt->stat_manager.max_statistics_num) {
        g_instance.wlm_cxt->stat_manager.index = 0;

        /* record starts to overwrite */
        if (g_instance.wlm_cxt->stat_manager.overwrite == 0) {
            g_instance.wlm_cxt->stat_manager.overwrite = 1;
        }
    }

    stat_lock.UnLock();
}

/*
 * function name: WLMProcessStatInfoVisualization
 * description  : append collect info to the view.
 * arguments    :
 *                _in_ pCollectInfo: collect info
 * return value : void
 */
void WLMProcessStatInfoVisualization(const WLMCollectInfo* pCollectInfo)
{
    if (pCollectInfo->attribute != WLM_ATTR_INDB) {
        return;
    }

    /* the query is valid, we will insert it into the statistics array */
    if (StringIsValid(pCollectInfo->sdetail.statement)) {
        WLMInsertCollectInfoIntoView(pCollectInfo);
    }

    /* report current backend status */
    pgstat_report_statement_wlm_status();
}

void WLMReleaseStmtDetailItem(WLMStmtDetail* detail)
{
    Assert(detail != NULL);

    if (detail->query_plan != NULL) {
        pfree_ext(detail->query_plan);
    }

    if (detail->query_plan_issue != NULL) {
        pfree_ext(detail->query_plan_issue);
    }

    if (detail->query_band != NULL) {
        pfree_ext(detail->query_band);
    }

    if (detail->msg != NULL) {
        pfree_ext(detail->msg);
    }

    if (detail->statement != NULL) {
        pfree_ext(detail->statement);
    }

    if (detail->clienthostname != NULL) {
        pfree_ext(detail->clienthostname);
    }

    if (detail->schname != NULL) {
        pfree_ext(detail->schname);
    }

    if (detail->username != NULL) {
        pfree_ext(detail->username);
    }

    if (detail->clientaddr != NULL) {
        pfree_ext(detail->clientaddr);
    }

    pfree_ext(detail->slowQueryInfo.current_table_counter);
    pfree_ext(detail->slowQueryInfo.localTimeInfoArray);
}

/*
 * function name: WLMRemoveExpiredRecords
 * description  : remove the expired record
 * arguments    : void
 * return value : void
 */
void WLMRemoveExpiredRecords(void)
{
    WLMStmtDetail* detail = NULL;
    HASH_SEQ_STATUS hash_seq;

    int count = 0;
    int i = 0;
    int j = 0;

    for (j = 0; j < NUM_SESSION_HISTORY_PARTITIONS; j++) {
		LWLockAcquire(GetMainLWLockByIndex(FirstSessionHistLock + j), LW_SHARED);
    }

    if ((count = hash_get_num_entries(g_instance.wlm_cxt->stat_manager.session_info_hashtbl)) == 0) {
        for (j = NUM_SESSION_HISTORY_PARTITIONS; --j >= 0;) {
            LWLockRelease(GetMainLWLockByIndex(FirstSessionHistLock + j));
        }
        return;
    }

    Qid* qids = (Qid*)palloc0_noexcept(count * sizeof(Qid));

    if (qids == NULL) {
        for (j = NUM_SESSION_HISTORY_PARTITIONS; --j >= 0;) {
            LWLockRelease(GetMainLWLockByIndex(FirstSessionHistLock + j));
        }

        ereport(LOG, (errmsg("alloc memory to remove expired records failed")));
        return;
    }

    hash_seq_init(&hash_seq, g_instance.wlm_cxt->stat_manager.session_info_hashtbl);

    while ((detail = (WLMStmtDetail*)hash_seq_search(&hash_seq)) != NULL) {
        /* check the record whether is invalid or expired */
        if (!detail->valid || (detail->exptime <= GetCurrentTimestamp() &&
            (detail->status == WLM_STATUS_FINISHED || detail->status == WLM_STATUS_ABORT))) {
            qids[i++] = detail->qid;
        }
    }

    for (j = NUM_SESSION_HISTORY_PARTITIONS; --j >= 0;) {
        LWLockRelease(GetMainLWLockByIndex(FirstSessionHistLock + j));
    }

    count = i;
    uint32 hashCode = 0;

    for (i = 0; i < count; ++i) {
        hashCode = WLMHashCode(qids + i, sizeof(Qid));
        LockSessHistHashPartition(hashCode, LW_EXCLUSIVE);
        
        detail = (WLMStmtDetail*)hash_search(
            g_instance.wlm_cxt->stat_manager.session_info_hashtbl, qids + i, HASH_FIND, NULL);

        if (IS_PGXC_COORDINATOR && detail != NULL) {
            if (detail != NULL) {
                WLMReleaseStmtDetailItem(detail);
            }
        }

        if (IS_PGXC_DATANODE && detail != NULL) {
            pfree_ext(detail->slowQueryInfo.current_table_counter);
            pfree_ext(detail->slowQueryInfo.localTimeInfoArray);
            pfree_ext(detail->query_plan);
        }
        hash_search(g_instance.wlm_cxt->stat_manager.session_info_hashtbl, qids + i, HASH_REMOVE, NULL);

        UnLockSessHistHashPartition(hashCode);
    }

    pfree(qids);
}

/*
 * @Description: update collect info on coordinator
 * @IN void
 * @Return: void
 * @See also:
 */
void WLMUpdateCollectInfo(void)
{
    HASH_SEQ_STATUS* hash_seq = &g_instance.wlm_cxt->stat_manager.collect_info_seq;

    int num = 0;
    int idx = 0;

    int j = 0;

    for (j = 0; j < NUM_SESSION_REALTIME_PARTITIONS; j++) {
        LWLockAcquire(GetMainLWLockByIndex(FirstSessionRealTLock + j), LW_EXCLUSIVE);
    }

    /* no record to update */
    if ((num = hash_get_num_entries(g_instance.wlm_cxt->stat_manager.collect_info_hashtbl)) <= 0) {
        for (j = NUM_SESSION_REALTIME_PARTITIONS; --j >= 0;) {
            LWLockRelease(GetMainLWLockByIndex(FirstSessionRealTLock + j));
        }
        return;
    }

    WLMDNodeInfo* info = NULL;

    /* count the collect info in the hash table */
    WLMNodeUpdateInfo* infoptrs = (WLMNodeUpdateInfo*)palloc0_noexcept(num * sizeof(WLMNodeUpdateInfo));

    if (infoptrs == NULL) {
        ereport(LOG, (errmsg("alloc memory failed for collecting datanode info.")));

        for (j = NUM_SESSION_REALTIME_PARTITIONS; --j >= 0;) {
            LWLockRelease(GetMainLWLockByIndex(FirstSessionRealTLock + j));
        }

        return;
    }

    hash_seq_init(hash_seq, g_instance.wlm_cxt->stat_manager.collect_info_hashtbl);

    /* get all info to update with data node info */
    while ((info = (WLMDNodeInfo*)hash_seq_search(hash_seq)) != NULL) {
        /*
         * If the record in the hash table is valid,
         * we will send a request to every datanodes
         * to get its cpu time, and update the record
         * in the hash table on coordinator.
         */
        if (info->tag == WLM_ACTION_REMAIN && info->cpuctrl > 0) {
            if (idx >= num) {
                if (hash_get_seq_num() > 0) {
                    hash_seq_term(hash_seq);
                }
                break;
            }
            infoptrs[idx].cpuctrl = info->cpuctrl;
            errno_t errval = strncpy_s(infoptrs[idx].nodegroup, NAMEDATALEN, info->nodegroup, NAMEDATALEN - 1);
            securec_check_errval(errval, , LOG);

            errval = memcpy_s(&infoptrs[idx].qid, sizeof(Qid), &info->qid, sizeof(Qid));
            securec_check_errval(errval, , LOG);

            errval = memcpy_s(&infoptrs[idx].geninfo, sizeof(WLMGeneralInfo), &info->geninfo, sizeof(WLMGeneralInfo));
            securec_check_errval(errval, , LOG);
            idx++;
        }
    }

    num = idx;

    for (j = NUM_SESSION_REALTIME_PARTITIONS; --j >= 0;) {
        LWLockRelease(GetMainLWLockByIndex(FirstSessionRealTLock + j));
    }
#ifdef ENABLE_MULTIPLE_NODES
    /* fetch collect info from data nodes */
    WLMFetchCollectInfoFromDN(infoptrs, num);

    for (idx = 0; idx < num; ++idx) {
        uint32 hashCode = WLMHashCode(&infoptrs[idx].qid, sizeof(Qid));
        (void)LockSessRealTHashPartition(hashCode, LW_EXCLUSIVE);
        info = (WLMDNodeInfo*)hash_search(
            g_instance.wlm_cxt->stat_manager.collect_info_hashtbl, &infoptrs[idx].qid, HASH_FIND, NULL);
        if (info != NULL) {
            errno_t errval =
                memcpy_s(&info->geninfo, sizeof(WLMGeneralInfo), &infoptrs[idx].geninfo, sizeof(WLMGeneralInfo));
            securec_check_errval(errval, , LOG);
        }

        UnLockSessRealTHashPartition(hashCode);
    }
#endif
    CHECK_FOR_INTERRUPTS();

#ifndef ENABLE_UT
    pg_usleep(USECS_PER_SEC * 2);
#endif

    pfree(infoptrs);
}

/*
 * @Description: check whether we need adjust user space
 * @IN userdata: user data in htab
 * @Return: true: need adjust false: need not adjust
 * @See also:
 */
bool WLMUserNeedAdjustSpace(UserData* userdata)
{
    if (IS_PGXC_COORDINATOR) {
        /* max waiting count to do next adjust */
        const unsigned char max_wait_count = 60;  // default 10 min

        /* user has no space limit, nothing to do. */
        if (userdata->spacelimit == 0) {
            return false;
        }

        int64 min_space_size = (userdata->spacelimit * SPACE_MIN_PERCENT / FULL_PERCENT);

        /* user's space has been 80% of the space limit, we may adjust the space */
        if (userdata->adjust == 0 && (userdata->totalspace >= min_space_size)) {
            userdata->adjust++;
            return true;
        /* In the latest 10 min, it doesn't need to adjust */
        } else if (userdata->adjust >= max_wait_count && userdata->totalspace < min_space_size) {
            userdata->adjust = 0;  // need to do the further adjustment
        } else if (userdata->adjust > 0) {
            userdata->adjust++;
        }
    }

    if (IS_PGXC_DATANODE) {
        /* user has adjust space on datanode, we need not do it again. */
        if (userdata->adjust) {
            return false;
        }
        /* user's space has been 50% of the space limit, we may adjust the space */
        if (userdata->totalspace >= (userdata->spacelimit / 2)) {
            userdata->adjust++;
            return true;
        }
    }

    return false;
}

/*
 * @Description: re-adjust user total space
 * @IN userdata: user data in htab
 * @IN isForce: force to adjust user space
 * @Return: void
 * @See also:
 */
void WLMReAdjustUserSpace(UserData* userdata, bool isForce)
{
    UpdateUsedSpace(userdata->userid, userdata->totalspace, userdata->tmpSpace);
    userdata->spaceUpdate = false;
}

/*
 * @Description: re-adjust all user total space
 * @IN uid: user id
 * @Return: void
 * @See also:
 */
void WLMReadjustAllUserSpace(Oid uid)
{
    char username[NAMEDATALEN] = {0};

    if (!OidIsValid(uid)) {
        if (!superuser()) {
            ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                errmsg("Only system admin user can use this function")));
        }

        if (!IS_PGXC_COORDINATOR) {
            WLMReadjustUserSpaceThroughAllDbs(ALL_USER);
        } else {
            const char* query = "select pg_catalog.gs_wlm_readjust_user_space(0);";
            /* verify all user space on each data node */
            (void)FetchStatistics4WLM(query, NULL, 0, WLMStrategyFuncIgnoreResult);
        }
    } else {
        if (uid == BOOTSTRAP_SUPERUSERID) {
            ereport(NOTICE, (errmsg("super user need not verify space")));
            return;
        }

        if (!superuser() && uid != GetUserId()) {
            ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                errmsg("Normal user could not readjust other user space")));
        }

        if (GetRoleName(uid, username, NAMEDATALEN) != NULL) {
            /* readjust user space */
            WLMReadjustUserSpaceThroughAllDbs(username);
        }
    }
}

void WLMReadjustUserSpaceByQuery(const char* username, List* database_name_list)
{
#define MAXCONNINFO 1024
#define SQLMAXLEN 1024
    PGconn* pgConn = NULL;
    PGresult* res = NULL;

    ListCell* cell = NULL;
    bool isFirstDb = true;
    foreach(cell, database_name_list) {
        char conninfo[MAXCONNINFO];
        errno_t rc = snprintf_s(conninfo,
            sizeof(conninfo),
            sizeof(conninfo) - 1,
            "dbname=%s port=%d application_name='statctl'",
            lfirst(cell),
            g_instance.attr.attr_network.PostPortNumber);
        securec_check_ss(rc, "\0", "\0");

        char query[SQLMAXLEN];
        rc = snprintf_s(query,
            sizeof(query),
            sizeof(query) - 1,
            "select pg_catalog.gs_wlm_readjust_user_space_with_reset_flag(\'%s\', %s);",
            username,
            isFirstDb ? "true" : "false");
        securec_check_ss(rc, "\0", "\0");
        isFirstDb = false;

        pgConn = PQconnectdb(conninfo);
        if (PQstatus(pgConn) != CONNECTION_OK) {
            ereport(WARNING,
                (errcode(ERRCODE_CONNECTION_TIMED_OUT),
                errmsg("could not connect to the primary server: %s", PQerrorMessage(pgConn))));
            PQfinish(pgConn);
            continue;
        }

        res = PQexec(pgConn, query);
        if (PQresultStatus(res) != PGRES_TUPLES_OK) {
            ereport(WARNING,
                (errcode(ERRCODE_INVALID_STATUS),
                errmsg("could not execute query : %s", PQerrorMessage(pgConn))));
        }

        /* clear resource and close connection */
        PQclear(res);
        PQfinish(pgConn);
    }


}

void GetUserSpaceThroughAllDbs(const char* username)
{
    TableScanDesc scan;
    Datum datum;
    HeapTuple tup = NULL;
    Relation pg_database_rel = NULL;
    List* database_name_list = NULL;
    char* database_name = NULL;
    bool isNull = false;

    /* get all db names through oid */
    pg_database_rel = heap_open(DatabaseRelationId, AccessShareLock);

    if (!pg_database_rel) {
        ereport(ERROR, (errcode(ERRCODE_SYSTEM_ERROR), errmsg("cannot open database")));
    }

    /* scan db names */
    scan = heap_beginscan(pg_database_rel, SnapshotNow, 0, NULL);
    while ((tup = heap_getnext(scan, ForwardScanDirection)) != NULL) {
        datum = heap_getattr(tup, Anum_pg_database_datname, RelationGetDescr(pg_database_rel), &isNull);
        if (isNull) {
            ereport(WARNING,
                (errcode(ERRCODE_SYSTEM_ERROR), errmsg("can not get tuple for database: %d", pg_database_rel->rd_id)));
            g_instance.comm_cxt.cal_all_space_info_in_progress = false;
            continue;
        }

        /* get database name */
        database_name = (char*)pstrdup((const char*)DatumGetCString(datum));
        if (strcmp(database_name, "template0") == 0 || strcmp(database_name, "template1") == 0) {
            continue;
        }

        database_name_list = lappend(database_name_list, database_name);
    }
    heap_endscan(scan);
    heap_close(pg_database_rel, AccessShareLock);

    WLMReadjustUserSpaceByQuery(username, database_name_list);
    list_free_deep(database_name_list);
}

void WLMUpdateUserSpaceInfo(Oid uid, const char* username)
{
    HASH_SEQ_STATUS hash_seq;
    UserData* userdata = NULL;

    USE_AUTO_LWLOCK(WorkloadUserInfoLock, LW_SHARED);
    if (strcmp(username, ALL_USER) == 0) {  // updata all user used space
        hash_seq_init(&hash_seq, g_instance.wlm_cxt->stat_manager.user_info_hashtbl);
        while ((userdata = (UserData*)hash_seq_search(&hash_seq)) != NULL) {
            if (userdata->userid == BOOTSTRAP_SUPERUSERID) {
                continue;
            }
            userdata->totalspace = userdata->reAdjustPermSpace;
            userdata->tmpSpace = userdata->reAdjustTmpSpace;
            userdata->spaceUpdate = true;
        }
        g_instance.comm_cxt.cal_all_space_info_in_progress = false;
    } else {
        /* search the user with the user id */
        userdata = GetUserDataFromHTab(uid, true);
        if (userdata != NULL) {
            userdata->totalspace = userdata->reAdjustPermSpace;
            userdata->tmpSpace = userdata->reAdjustTmpSpace;
            userdata->spaceUpdate = true;
        }
    }
}

/*
 * @Description: re-adjust all user total space through all dbs
 * @IN username: user name
 * @Return: void
 * @See also:
 */
void WLMReadjustUserSpaceThroughAllDbs(const char* username)
{
#define SQLMAXLEN 1024
    /* get oid through username */
    Oid uid = InvalidOid;
    bool allUser = (strcmp(username, ALL_USER) == 0);

    if (!allUser) {
        uid = GetRoleOid(username);
        if (!superuser() && uid != GetUserId()) {
            ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                errmsg("Normal user could not readjust other user space")));
        }
    } else if (!superuser()) {
        ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
            errmsg("Only system admin user can use this function")));
    }

    if (OidIsValid(uid) || allUser) {
        /* super user */
        if (uid == BOOTSTRAP_SUPERUSERID) {
            ereport(NOTICE, (errmsg("super user need not verify space")));
            return;
        }
        
        if (!IS_PGXC_COORDINATOR) {
            if (allUser) {
                if (g_instance.comm_cxt.cal_all_space_info_in_progress) {
                    ereport(
                        LOG, (errmsg("re-calculate all user space info is in progress in another thread...return...")));
                    return;
                } else {
                    g_instance.comm_cxt.cal_all_space_info_in_progress = true;
                }
            }
            GetUserSpaceThroughAllDbs(username);
            WLMUpdateUserSpaceInfo(uid, username);
        } else {
            errno_t rc;
            char query[SQLMAXLEN];

            rc = snprintf_s(query,
                sizeof(query),
                sizeof(query) - 1,
                "select pg_catalog.gs_wlm_readjust_user_space_through_username(\'%s\');",
                username);
            securec_check_ss(rc, "\0", "\0");

            /* verify all user space on each data node */
            (void)FetchStatistics4WLM(query, NULL, 0, WLMStrategyFuncIgnoreResult);
        }
    } else {
        ereport(NOTICE, (errmsg("invalid uid")));
        return;
    }
    return;
}

static bool allocAllUserSpaceInfo(List*** rels, UserData*** userDataInfos, int64** size, int64** tmpSize, int userNum)
{
    if (userNum <= 0) {
        return false;
    }
    *rels = (List**)palloc0(sizeof(List*) * userNum);
    *userDataInfos = (UserData**)palloc0(sizeof(UserData*) * userNum);
    *size = (int64*)palloc0(sizeof(int64) * userNum);
    *tmpSize = (int64*)palloc0(sizeof(int64) * userNum);
    if (*rels == NULL || *userDataInfos == NULL || *size == NULL || *tmpSize == 0) {
        if (*rels != NULL) {
            pfree(*rels);
            *rels = NULL;
        }
        if (*userDataInfos != NULL) {
            pfree(*userDataInfos);
            *userDataInfos = NULL;
        }
        if (*size != NULL) {
            pfree(*size);
            *size = NULL;
        }
        if (*tmpSize != NULL) {
            pfree(*tmpSize);
            *tmpSize = NULL;
        }
        return false;
    }
    return true;
}

static void freeAllUserSpaceInfo(List*** rels, UserData*** userDataInfos, int64** size, int64** tmpSize)
{
    if (*rels != NULL) {
        pfree(*rels);
        *rels = NULL;
    }
    if (*userDataInfos != NULL) {
        pfree(*userDataInfos);
        *userDataInfos = NULL;
    }
    if (*size != NULL) {
        pfree(*size);
        *size = NULL;
    }
    if (*tmpSize != NULL) {
        pfree(*tmpSize);
        *tmpSize = NULL;
    }
    return;
}

void WLMReAdjustAllUserSpaceInternal(bool isReset)
{
    int userNum = 0;
    List** rels = NULL;
    UserData** userDataInfos = NULL;
    int64* size = NULL;
    int64* tmpSize = NULL;

    int index = 0;
    ListCell* cell = NULL;
    UserData* userdata = NULL;
    Relation rel;
    HeapTuple tup;
    HASH_SEQ_STATUS hash_seq;

    USE_AUTO_LWLOCK(WorkloadUserInfoLock, LW_SHARED);
    userNum = hash_get_num_entries(g_instance.wlm_cxt->stat_manager.user_info_hashtbl);
    if (!allocAllUserSpaceInfo(&rels, &userDataInfos, &size, &tmpSize, userNum)) {
        // print warning info
        RELEASE_AUTO_LWLOCK();
        return;
    }
    hash_seq_init(&hash_seq, g_instance.wlm_cxt->stat_manager.user_info_hashtbl);

    while ((userdata = (UserData*)hash_seq_search(&hash_seq)) != NULL) {
        if (userdata->userid == BOOTSTRAP_SUPERUSERID) {
            continue;
        }
        userDataInfos[index] = userdata;
        ++index;
    }
    RELEASE_AUTO_LWLOCK();

    rel = heap_open(RelationRelationId, AccessShareLock);

    SysScanDesc scan = systable_beginscan(rel, InvalidOid, false, NULL, 0, NULL);

    /* get all user in system table */
    while (HeapTupleIsValid((tup = systable_getnext(scan)))) {
        Form_pg_class reldata = (Form_pg_class)GETSTRUCT(tup);
        int pos = 0;

        /* get all table oid for the all user */
        if (reldata->relowner == BOOTSTRAP_SUPERUSERID) {
            continue;
        }
        if (IsCStoreNamespace(reldata->relnamespace) || IsToastNamespace(reldata->relnamespace)) {
            continue;
        }
        for (pos = 0; pos < index; pos++) {
            if (userDataInfos[pos]->userid == reldata->relowner) {
                rels[pos] = lappend_oid(rels[pos], HeapTupleGetOid(tup));
                break;
            }
        }
    }

    systable_endscan(scan);
    heap_close(rel, AccessShareLock);

    for (index = 0; index < userNum; ++index) {
        foreach (cell, rels[index]) {
            Oid reloid = lfirst_oid(cell);

            /* Get the lock first */
            LockRelationOid(reloid, AccessShareLock);

            /*
             * Now that we have the lock, probe to see if the relation really exists
             * or not.
             */
            if (!SearchSysCacheExists1(RELOID, ObjectIdGetDatum(reloid))) {
                /* Release useless lock */
                UnlockRelationOid(reloid, AccessShareLock);
                continue;
            }

            /* Should be safe to do a relcache load */
            rel = RelationIdGetRelation(reloid);

            if (!RelationIsValid(rel)) {
                /* Release useless lock */
                UnlockRelationOid(reloid, AccessShareLock);
                continue;
            }

            /* compute total space size */
            if (RelationUsesSpaceType(rel->rd_rel->relpersistence) == SP_TEMP) {
                tmpSize[index] += (uint64)pg_relation_table_size(rel);
            } else {
                size[index] += (uint64)pg_relation_table_size(rel);
            }

            relation_close(rel, AccessShareLock);
        }

        if (userDataInfos[index] != NULL) {
            if (isReset) {
                (void)gs_lock_test_and_set_64((int64*)&userDataInfos[index]->reAdjustPermSpace, size[index]);
                (void)gs_lock_test_and_set_64((int64*)&userDataInfos[index]->reAdjustTmpSpace, tmpSize[index]);
            } else {
                (void)gs_lock_test_and_set_64((int64*)&userDataInfos[index]->reAdjustPermSpace,
                    userDataInfos[index]->reAdjustPermSpace + size[index]);
                (void)gs_lock_test_and_set_64((int64*)&userDataInfos[index]->reAdjustTmpSpace,
                    userDataInfos[index]->reAdjustTmpSpace + tmpSize[index]);
            }
            userDataInfos[index]->spaceUpdate = true;
        }

        list_free(rels[index]);
    }
    freeAllUserSpaceInfo(&rels, &userDataInfos, &size, &tmpSize);
    return;
}

/*
 * @Description: adjust user total space
 * @IN userdata: user data in htab
 * @Return: void
 * @See also:
 */
void WLMReAdjustUserSpaceWithResetFlagInternal(UserData* userdata, bool isReset)
{
    List* rels = NULL;
    ListCell* cell = NULL;
    Relation rel;
    HeapTuple tup;
    int64 size = 0;
    int64 tmpSize = 0;

    rel = heap_open(RelationRelationId, AccessShareLock);

    SysScanDesc scan = systable_beginscan(rel, InvalidOid, false, NULL, 0, NULL);

    /* get all user in system table */
    while (HeapTupleIsValid((tup = systable_getnext(scan)))) {
        Form_pg_class reldata = (Form_pg_class)GETSTRUCT(tup);

        /* get all table oid for the user */
        if (reldata->relowner == userdata->userid) {
            if (!IsCStoreNamespace(reldata->relnamespace) && !IsToastNamespace(reldata->relnamespace)) {
                rels = lappend_oid(rels, HeapTupleGetOid(tup));
            }
            continue;
        }
    }

    systable_endscan(scan);
    heap_close(rel, AccessShareLock);

    foreach (cell, rels) {
        Oid reloid = lfirst_oid(cell);

        /* Get the lock first */
        LockRelationOid(reloid, AccessShareLock);

        /*
         * Now that we have the lock, probe to see if the relation really exists
         * or not.
         */
        if (!SearchSysCacheExists1(RELOID, ObjectIdGetDatum(reloid))) {
            /* Release useless lock */
            UnlockRelationOid(reloid, AccessShareLock);
            continue;
        }

        /* Should be safe to do a relcache load */
        rel = RelationIdGetRelation(reloid);

        if (!RelationIsValid(rel)) {
            /* Release useless lock */
            UnlockRelationOid(reloid, AccessShareLock);
            continue;
        }

        /* compute total space size */
        if (RelationUsesSpaceType(rel->rd_rel->relpersistence) == SP_TEMP) {
            tmpSize += (uint64)pg_relation_table_size(rel);
        } else {
            size += (uint64)pg_relation_table_size(rel);
        }

        relation_close(rel, AccessShareLock);
    }

    if (isReset) {
        (void)gs_lock_test_and_set_64((int64*)&userdata->reAdjustPermSpace, size);
        (void)gs_lock_test_and_set_64((int64*)&userdata->reAdjustTmpSpace, tmpSize);
    } else {
        (void)gs_lock_test_and_set_64((int64*)&userdata->reAdjustPermSpace, userdata->reAdjustPermSpace + size);
        (void)gs_lock_test_and_set_64((int64*)&userdata->reAdjustTmpSpace, userdata->reAdjustTmpSpace + tmpSize);
    }
    userdata->spaceUpdate = true;
    list_free(rels);

    return;
}

/*
 * @Description: re-adjust user total space with reset flag
 * @IN userdata: user data in htab
 * @IN isReset: reset user space to 0
 * @Return: void
 * @See also:
 */
void WLMReAdjustUserSpaceWithResetFlag(UserData* userdata, bool isReset)
{
    WLMReAdjustUserSpaceWithResetFlagInternal(userdata, isReset);

    /* set config file info */
    if (userdata->infoptr) {
        (void)gs_lock_test_and_set_64((int64*)&userdata->infoptr->space, userdata->totalspace);
    }
}

/*
 * @Description: re-adjust user total space by username & resetFlag,
 *      if resetFlag is true, set used space to 0,
 *      if resetFlag is false, calculate sum of used space for all databases.
 * @IN username: user name
 * @Return: void
 * @See also:
 */
void WLMReadjustUserSpaceByNameWithResetFlag(const char* username, bool resetFlag)
{
    /* get oid through username */
    Oid uid = InvalidOid;
    if (strcmp(username, ALL_USER) != 0) {
        uid = GetRoleOid(username);
        if (!superuser() && uid != GetUserId()) {
            ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                errmsg("Normal user could not readjust other user space")));
        }
    } else if (!superuser()) {
        ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
            errmsg("Only system admin user can use this function")));
    }

    HASH_SEQ_STATUS hash_seq;
    UserData* userdata = NULL;

    if (OidIsValid(uid)) {
        /* super user */
        if (uid == BOOTSTRAP_SUPERUSERID) {
            ereport(NOTICE, (errmsg("super user need not verify space")));
            return;
        }

        if (!IS_PGXC_COORDINATOR) {
            USE_AUTO_LWLOCK(WorkloadUserInfoLock, LW_SHARED);
            hash_seq_init(&hash_seq, g_instance.wlm_cxt->stat_manager.user_info_hashtbl);

            while ((userdata = (UserData*)hash_seq_search(&hash_seq)) != NULL) {
                if (userdata->userid == uid) {
                    /* readjust user space */
                    WLMReAdjustUserSpaceWithResetFlag(userdata, resetFlag);
                    hash_seq_term(&hash_seq);
                    break;
                }
            }
            RELEASE_AUTO_LWLOCK();
        }
    } else if (strcmp(username, ALL_USER) == 0) {
        if (!IS_PGXC_COORDINATOR) {
            WLMReAdjustAllUserSpaceInternal(resetFlag);
        }
    } else {
        ereport(NOTICE, (errmsg("invalid uid")));
        return;
    }
}

/*
 * @Description: check user space to report error
 * @IN userdata: user data in htab
 * @Return: void
 * @See also:
 */
void WLMUserSpaceCheckAndHandle(UserData* userdata)
{
    if ((userdata->spacelimit > 0 && userdata->totalspace >= userdata->spacelimit) ||
        (userdata->parent && userdata->parent->spacelimit > 0 &&
            userdata->parent->totalspace >= userdata->parent->spacelimit)) {
        int num = 0;

        /* get user thread */
        ThreadId* threads = pgstat_get_user_io_entry(userdata->userid, &num);

        if (threads == NULL) {
            return;
        }

        (void)LWLockAcquire(ProcArrayLock, LW_SHARED);

        /* send signal to finish the thread */
        for (int i = 0; i < num; ++i) {
            (void)SendProcSignal(threads[i], PROCSIG_SPACE_LIMIT, InvalidBackendId);
            ereport(LOG, (errmsg("cancel thread %ld for space limit exceed.", (long)threads[i])));
        }

        LWLockRelease(ProcArrayLock);

        pfree(threads);
    }
}

/*
 * @Description: update user data in htab
 * @IN uid: user oid
 * @IN space: space size to update
 * @Return: void
 * @See also:
 */
void WLMUpdateUserDataInternal(Oid uid, WLMUserCollInfoDetail* data)
{
    Assert(data != NULL);

    USE_AUTO_LWLOCK(WorkloadUserInfoLock, LW_SHARED);
    /* search the user with the user id */
    UserData* userdata = GetUserDataFromHTab(uid, true);
    RELEASE_AUTO_LWLOCK();

    if (userdata != NULL) {
        userdata->memsize = data->usedMemoryData.max_value;

        userdata->usedCpuCnt = data->nodeCount ? (data->cpuUsedCnt / data->nodeCount) : 0;
        userdata->totalCpuCnt = data->nodeCount ? (data->cpuTotalCnt / data->nodeCount) : 0;

        if (u_sess->attr.attr_resource.enable_resource_track) {
            userdata->ioinfo.peakIopsData = data->peakIopsData;
            userdata->ioinfo.currIopsData = data->currIopsData;
        }

        /* update user io flow */
        updateIOFlowData(data, userdata);

        /* update child user total space */
        userdata->totalspace = data->totalSpace;
        userdata->tmpSpace = data->tmpSpace;
        userdata->globalSpillSpace = data->spillSpace + userdata->spillSpace;

        WLMReAdjustUserSpace(userdata);
    }
}

/* IO flow data */
void updateIOFlowData(const WLMUserCollInfoDetail* data, UserData* userdata)
{
    TimestampTz lastTime = userdata->ioinfo.timestampTz;
    TimestampTz now = GetCurrentTimestamp();
    long secs;
    int usecs;
    int sleeptime = 1;
    TimestampDifference(lastTime, now, &secs, &usecs);
    sleeptime = secs * 1000 + usecs / 1000 + 1;

    if (userdata->childlist == NULL) {
        userdata->ioinfo.timestampTz = now;

        /* keep last value */
        userdata->ioinfo.read_bytes[0] = userdata->ioinfo.read_bytes[1];
        userdata->ioinfo.write_bytes[0] = userdata->ioinfo.write_bytes[1];
        userdata->ioinfo.read_counts[0] = userdata->ioinfo.read_counts[1];
        userdata->ioinfo.write_counts[0] = userdata->ioinfo.write_counts[1];

        /* get recent value */
        userdata->ioinfo.read_bytes[1] = data->readBytes;
        userdata->ioinfo.write_bytes[1] = data->writeBytes;
        userdata->ioinfo.read_counts[1] = data->readCounts;
        userdata->ioinfo.write_counts[1] = data->writeCounts;

        /* calculate IO flow speed */
        if (data->readBytes > userdata->ioinfo.read_bytes[0]) {
            userdata->ioinfo.read_speed =
                data->nodeCount ? ((data->readBytes - userdata->ioinfo.read_bytes[0]) / data->nodeCount / sleeptime)
                                : 0;
        } else {
            userdata->ioinfo.read_speed = 0;
        }
        if (data->writeBytes > userdata->ioinfo.write_bytes[0]) {
            userdata->ioinfo.write_speed =
                data->nodeCount ? ((data->writeBytes - userdata->ioinfo.write_bytes[0]) / data->nodeCount / sleeptime)
                                : 0;
        } else {
            userdata->ioinfo.write_speed = 0;
        }
    } else {
        /* keep for group user */
        userdata->ioinfo.group_value[0] = data->readBytes;
        userdata->ioinfo.group_value[1] = data->writeBytes;
        userdata->ioinfo.group_value[2] = data->readCounts;
        userdata->ioinfo.group_value[3] = data->writeCounts;
        userdata->ioinfo.nodeCount = data->nodeCount;
    }

    /* reset io for super user */
    if (userdata->userid == BOOTSTRAP_SUPERUSERID) {
        /* reset io */
        (void)gs_lock_test_and_set_64((int64*)&userdata->ioinfo.read_bytes[0], 0);
        (void)gs_lock_test_and_set_64((int64*)&userdata->ioinfo.read_bytes[1], 0);
        (void)gs_lock_test_and_set_64((int64*)&userdata->ioinfo.write_bytes[0], 0);
        (void)gs_lock_test_and_set_64((int64*)&userdata->ioinfo.write_bytes[1], 0);
        (void)gs_lock_test_and_set_64((int64*)&userdata->ioinfo.read_counts[0], 0);
        (void)gs_lock_test_and_set_64((int64*)&userdata->ioinfo.read_counts[1], 0);
        (void)gs_lock_test_and_set_64((int64*)&userdata->ioinfo.write_counts[0], 0);
        (void)gs_lock_test_and_set_64((int64*)&userdata->ioinfo.write_counts[1], 0);
        (void)gs_lock_test_and_set_64((int64*)&userdata->ioinfo.read_speed, 0);
        (void)gs_lock_test_and_set_64((int64*)&userdata->ioinfo.write_speed, 0);
    }
}

void WLMUpdateGroupUserSpace(UserData* userdata)
{
    if (userdata != NULL && list_length(userdata->childlist) > 0) {
        int64 parent_space = 0;
        int64 parentTmpSpace = 0;
        int64 parentSpillSpace = 0;
        ListCell* cell = NULL;

        /* set parent total space */
        foreach (cell, userdata->childlist) {
            UserData* childdata = (UserData*)lfirst(cell);
            parent_space += childdata->totalspace;
            parentTmpSpace += childdata->tmpSpace;
            parentSpillSpace += childdata->globalSpillSpace;
        }

        userdata->totalspace += parent_space;
        userdata->tmpSpace += parentTmpSpace;
        userdata->globalSpillSpace += parentSpillSpace;
        WLMReAdjustUserSpace(userdata);
    }
}

/*
 * function name: WLMUpdatePartialSessionIops
 * description  : update user iops or query iops for partial hash table
 * arguments    :
 *                _in_ bucketId: which bucket in partial IO hash table
 * return value : void
 */
static void WLMUpdatePartialSessionIops(uint32 bucketId)
{
    int j = 0;

    HASH_SEQ_STATUS* hash_seq = &g_instance.wlm_cxt->stat_manager.iostat_info_seq;

    int lockId = GetIoStatLockId(bucketId);
    (void)LWLockAcquire(GetMainLWLockByIndex(lockId), LW_SHARED);
    long idx = hash_get_num_entries(G_WLM_IOSTAT_HASH(bucketId));
    ereport(DEBUG3,
        (errmsg("WLMProcessThreadMain: WLMUpdateSessionIops: "
                "length of g_statManager.iostat_info_hashtbl in bucket %u: %ld", bucketId, idx)));

    /* get current user info count, we will do nothing if it's 0 */
    if (idx <= 0) {
        LWLockRelease(GetMainLWLockByIndex(lockId));
        return;
    }

    WLMDNodeIOInfo** sessionarray = (WLMDNodeIOInfo**)palloc0_noexcept(idx * sizeof(WLMDNodeIOInfo*));
    WLMDNodeIOInfo* DNodeIoInfo = NULL;

    if (sessionarray == NULL) {
        ereport(LOG, (errmsg("Alloc memory to update user IO info failed, ignore this time.")));
        LWLockRelease(GetMainLWLockByIndex(lockId));
        return;
    }
    hash_seq_init(hash_seq, G_WLM_IOSTAT_HASH(bucketId));

    /* Get all user data from the register info hash table. */
    while ((DNodeIoInfo = (WLMDNodeIOInfo*)hash_seq_search(hash_seq)) != NULL) {
        if (DNodeIoInfo->tag == WLM_ACTION_REMAIN) {
            sessionarray[j++] = DNodeIoInfo;
        }
    }

    LWLockRelease(GetMainLWLockByIndex(lockId));

    int retry_count = 0;

retry:
    for (int i = 0; i < j; ++i) {
        Qid qid = sessionarray[i]->qid;

        char keystr[NAMEDATALEN];

        errno_t ssval =
            snprintf_s(keystr, sizeof(keystr), sizeof(keystr) - 1, "%u,%lu,%ld", qid.procId, qid.queryId, qid.stamp);
        securec_check_ssval(ssval, , LOG);

        int ret = WLMRemoteInfoSenderByNG(sessionarray[i]->nodegroup, keystr, WLM_COLLECT_IO_RUNTIME);

        if (ret != 0) {
            WLMDoSleepAndRetry(&retry_count);
            goto retry;
        }

        WLMIoGeninfo ioinfo;

        errno_t errval = memset_s(&ioinfo, sizeof(ioinfo), 0, sizeof(ioinfo));
        securec_check_errval(errval, , LOG);

        WLMRemoteInfoReceiverByNG(sessionarray[i]->nodegroup, &ioinfo, sizeof(ioinfo), WLMParseIOInfo);

        (void)LWLockAcquire(GetMainLWLockByIndex(lockId), LW_EXCLUSIVE);
        if (sessionarray[i]->tag == WLM_ACTION_REMAIN) {
            sessionarray[i]->io_geninfo = ioinfo;
        }
        LWLockRelease(GetMainLWLockByIndex(lockId));
        ereport(DEBUG2, (errmodule(MOD_WLM), errmsg("[IOSTAT] SETIFNO queryId: %lu, maxpeak_iops: %d, curr_iops: %d",
            qid.queryId, ioinfo.peakIopsData.max_value, ioinfo.currIopsData.max_value)));
        CHECK_FOR_INTERRUPTS();
    }
    pfree_ext(sessionarray);
}
/*
 * @Description: update user iops or query iops
 * @IN :
 * @See also:
 */
void WLMUpdateSessionIops(void)
{
    if (g_instance.wlm_cxt->stat_manager.iostat_info_hashtbl != NULL) {
        for (uint32 bucketId = 0; bucketId < NUM_IO_STAT_PARTITIONS; ++bucketId) {
            WLMUpdatePartialSessionIops(bucketId);
        }
    }
}

void WLMUpdateSingleNodeUserInfo(UserData* userData)
{
    int usedCpuCnt = 0;
    int totalCpuCnt = 0;
    WLMUserCollInfoDetail info = {0};
    int curr_iops,peak_iops;
    int memsize = 0;

    if (userData == NULL) {
        ereport(LOG, (errmsg("userData is NULL.")));
        return;
    }
    memsize = WLMGetUserMemory(userData);
    if (userData->childlist) {
        foreach_cell(cell, userData->childlist) {
            UserData* childdata = (UserData*)lfirst(cell);
            if (childdata != NULL) {
                memsize += WLMGetUserMemory(childdata);
            }
        }
    }
    curr_iops = userData->ioinfo.curr_iops;
    peak_iops = userData->ioinfo.peak_iops;
    if (userData->respool != NULL) {
        WLMNodeGroupInfo* ng = WLMMustGetNodeGroupFromHTAB(userData->respool->ngroup);
        gscgroup_entry_t* cg_entry = NULL;
        if ((cg_entry = gscgroup_lookup_hashtbl(ng, userData->respool->cgroup)) != NULL) {
            usedCpuCnt = cg_entry->usedCpuCount;
            totalCpuCnt = cg_entry->cpuCount;
        }
    }

    GetMaxAndMinUserInfo(&info, usedMemoryData, memsize);
    GetMaxAndMinUserInfo(&info, currIopsData, curr_iops);
    GetMaxAndMinUserInfo(&info, peakIopsData, peak_iops);
    userData->memsize = info.usedMemoryData.max_value;
    userData->usedCpuCnt = usedCpuCnt;
    userData->totalCpuCnt = totalCpuCnt;
    userData->ioinfo.peakIopsData = info.peakIopsData;
    userData->ioinfo.currIopsData = info.currIopsData;

    info.readBytes = userData->ioinfo.read_bytes[1];
    info.writeBytes = userData->ioinfo.write_bytes[1];
    info.readCounts = userData->ioinfo.read_counts[1];
    info.writeCounts = userData->ioinfo.write_counts[1];

    updateIOFlowData(&info, userData);
}

/*
 * @Description: update user info
 * @IN void
 * @Return: void
 * @See also:
 */
void WLMUpdateUserInfo(void)
{
    int num = 0;
    int idx = 0;

    HASH_SEQ_STATUS hash_seq;

    UserData* userdata = NULL;

    USE_AUTO_LWLOCK(WorkloadUserInfoLock, LW_SHARED);

    /* no user data in htab, nothing to do */
    if ((num = hash_get_num_entries(g_instance.wlm_cxt->stat_manager.user_info_hashtbl)) <= 0) {
        return;
    }

    Oid* uids = (Oid*)palloc0_noexcept(num * sizeof(Oid));

    if (uids == NULL) {
        ereport(LOG, (errmsg("Alloc memory to update user info failed, ignore this time.")));
        return;
    }

    char** ngnames = (char**)palloc0_noexcept(num * sizeof(char*));

    if (ngnames == NULL) {
        ereport(LOG, (errmsg("Alloc memory to update ngname info failed, ignore this time.")));
        return;
    }

    hash_seq_init(&hash_seq, g_instance.wlm_cxt->stat_manager.user_info_hashtbl);

    /* we get all user oid from the htab */
    while ((userdata = (UserData*)hash_seq_search(&hash_seq)) != NULL) {
        if (userdata->userid == BOOTSTRAP_SUPERUSERID || userdata->is_dirty || userdata->respool == NULL) {
            continue;
        }

        uids[idx] = userdata->userid;
        ngnames[idx] = pstrdup(userdata->respool->ngroup);
        idx++;
    }

    RELEASE_AUTO_LWLOCK();

    num = idx;

    if (IS_PGXC_COORDINATOR) {
        WLMUserCollInfoDetail gendata;

        int retry_count = 0;

    retry:
        for (idx = 0; idx < num; ++idx) {
            Oid uid = uids[idx];

            char name[NAMEDATALEN];
            char keystr[NAMEDATALEN + 2] = {0};

            /* we will get user name, but maybe the user has been removed */
            if (GetRoleName(uid, name, sizeof(name)) == NULL) {
                continue;
            }

            errno_t ssval = snprintf_s(keystr, sizeof(keystr), sizeof(keystr) - 1, "M|%s", name);
            securec_check_ssval(ssval, , LOG);

            int ret = WLMRemoteInfoSenderByNG(ngnames[idx], keystr, WLM_COLLECT_USERINFO);

            if (ret != 0) {
                WLMDoSleepAndRetry(&retry_count);
                goto retry;
            }

            errno_t errval = memset_s(&gendata, sizeof(gendata), 0, sizeof(gendata));
            securec_check_errval(errval, , LOG);
            gendata.usedMemoryData.min_value = -1;
            gendata.currIopsData.min_value = -1;
            gendata.peakIopsData.min_value = -1;

            /* fetch space statistics from each data node. */
            WLMRemoteInfoReceiverByNG(ngnames[idx], &gendata, sizeof(gendata), WLMParseFunc4UserInfo);

            if (gendata.usedMemoryData.min_value == -1) {
                gendata.usedMemoryData.min_value = 0;
            }
            if (gendata.currIopsData.min_value == -1) {
                gendata.currIopsData.min_value = 0;
            }
            if (gendata.peakIopsData.min_value == -1) {
                gendata.peakIopsData.min_value = 0;
            }

            /* update the user data with total space from the datanodes */
            WLMUpdateUserDataInternal(uid, &gendata);

            CHECK_FOR_INTERRUPTS();
        }

        for (idx = 0; idx < num; ++idx) {
            Oid uid = uids[idx];
            char keystr[NAMEDATALEN + 64] = {0};
            char name[NAMEDATALEN];
            /* we will get user name, but maybe the user has been removed */
            if (GetRoleName(uid, name, sizeof(name)) == NULL) {
                continue;
            }

            UserData* userdata =
                (UserData*)hash_search(g_instance.wlm_cxt->stat_manager.user_info_hashtbl, &uid, HASH_FIND, NULL);
            if (userdata == NULL) {
                continue;
            }

            // if user is a group user, updata its space info with all child user's space info
            WLMUpdateGroupUserSpace(userdata);

            errno_t ssval = snprintf_s(keystr,
                sizeof(keystr),
                sizeof(keystr) - 1,
                "P|%lld %lld %lld %s",
                userdata->totalspace,
                userdata->tmpSpace,
                userdata->globalSpillSpace,
                name);
            securec_check_ssval(ssval, , LOG);

            int ret = WLMRemoteInfoSenderByNG(ngnames[idx], keystr, WLM_COLLECT_USERINFO);
            if (ret != 0) {
                WLMDoSleepAndRetry(&retry_count);
                goto retry;
            }

            CHECK_FOR_INTERRUPTS();
        }
    }

    if (IS_PGXC_DATANODE) {
        for (idx = 0; idx < num; ++idx) {
            Oid uid = uids[idx];

            USE_AUTO_LWLOCK(WorkloadUserInfoLock, LW_SHARED);

            /* search the user with the user id */
            userdata = GetUserDataFromHTab(uid, true);

            RELEASE_AUTO_LWLOCK();
#ifndef ENABLE_MULTIPLE_NODES
            WLMUpdateSingleNodeUserInfo(userdata);
#endif
            if (userdata != NULL && userdata->spaceUpdate) {
                WLMReAdjustUserSpace(userdata);
            }

            CHECK_FOR_INTERRUPTS();
        }
    }

    pfree_ext(uids);
    pfree_ext(ngnames);
}

/*
 * function name: WLMCleanUpCollectInfo
 * description  : clean up all invalid collect info in the hash .
 * arguments    : void
 * return value : void
 */
void WLMCleanUpCollectInfo(void)
{
    HASH_SEQ_STATUS* hash_seq = &g_instance.wlm_cxt->stat_manager.collect_info_seq;
    WLMDNodeInfo* info = NULL;
    long num = hash_get_num_entries(g_instance.wlm_cxt->stat_manager.collect_info_hashtbl);

    /* nothing to clean up */
    if (num <= 0) {
        return;
    }

    ereport(DEBUG1,
        (errmsg("clean up all invalid collect info, count/max: %ld/%d",
            num,
            g_instance.wlm_cxt->stat_manager.max_collectinfo_num)));

    hash_seq_init(hash_seq, g_instance.wlm_cxt->stat_manager.collect_info_hashtbl);

    int j = 0;

    for (j = 0; j < NUM_SESSION_REALTIME_PARTITIONS; j++) {
        LWLockAcquire(GetMainLWLockByIndex(FirstSessionRealTLock + j), LW_EXCLUSIVE);
    }

    while ((info = (WLMDNodeInfo*)hash_seq_search(hash_seq)) != NULL) {
        /*
         * If the record in the hash table is valid,
         * we will send a request to every datanodes
         * to get its cpu time, and update the record
         * in the hash table on coordinator.
         */
        if (info->tag == WLM_ACTION_REMOVED) {
            if (info->qband) {
                pfree_ext(info->qband);
            }

            pfree_ext(info->statement);

            /*
             * If the record in the hash table is
             * valid no longer, it will be removed.
             */
            hash_search(g_instance.wlm_cxt->stat_manager.collect_info_hashtbl, &info->qid, HASH_REMOVE, NULL);
        }

        CHECK_FOR_INTERRUPTS();
    }

    for (j = NUM_SESSION_REALTIME_PARTITIONS; --j >= 0;) {
        LWLockRelease(GetMainLWLockByIndex(FirstSessionRealTLock + j));
    }
}

/*
 * function name: WLMCleanUpIoInfo
 * description  : clean up all invalid io info in the hash .
 * arguments    : void
 * return value : void
 */
void WLMCleanUpIoInfo(void)
{
    HASH_SEQ_STATUS hash_seq;

    WLMDNodeIOInfo* info = NULL;
    long num;

    for (uint32 bucketId = 0; bucketId < NUM_IO_STAT_PARTITIONS; ++bucketId) {
        int lockId = GetIoStatLockId(bucketId);
        (void)LWLockAcquire(GetMainLWLockByIndex(lockId), LW_EXCLUSIVE);
        num = hash_get_num_entries(G_WLM_IOSTAT_HASH(bucketId));
        ereport(DEBUG1, (errmsg("clean up all invalid io info, count/max in bucket %u: %ld/%d",
            bucketId, num, g_instance.wlm_cxt->stat_manager.max_iostatinfo_num)));

        if (num <= 0) {
            /* nothing to clean up */
            LWLockRelease(GetMainLWLockByIndex(lockId));
            continue;
        }
        hash_seq_init(&hash_seq, G_WLM_IOSTAT_HASH(bucketId));
        while ((info = (WLMDNodeIOInfo*)hash_seq_search(&hash_seq)) != NULL) {
            /*
             * If the record in the hash table is valid,
             * we will send a request to every datanodes
             * to get its cpu time, and update the record
             * in the hash table on coordinator.
             */
            if (info->tag == WLM_ACTION_REMOVED) {
                /*
                 * If the record in the hash table is
                 * valid no longer, it will be removed.
                 */
                hash_search(G_WLM_IOSTAT_HASH(bucketId), &info->qid, HASH_REMOVE, NULL);
            }
        }
        LWLockRelease(GetMainLWLockByIndex(lockId));
    }
}

/*
 * @Description: get the mounted device name by reading the file "/proc/mounts"
 * @IN:         datadir: data directory.
 * @OUT:        devicename: device name like /dev/sda2
 * @Return:     void
 * @See also:
 */
void WLMDNGetDiskName(const char* datadir, char* devicename, int length)
{
    unsigned int buf_len = 0;
    unsigned int len_count = 0;
    FILE* mtfp = NULL;
    errno_t rc;
    struct mntent* ent = NULL;
    char mntent_buffer[4 * FILENAME_MAX];
    struct mntent temp_ent;

    mtfp = fopen(FILE_MOUNTS, "r");

    if (NULL == mtfp) {
        ereport(LOG, (errmsg("cannot open file %s.\n", FILE_MOUNTS)));
        return;
    }

    while ((ent = getmntent_r(mtfp, &temp_ent, mntent_buffer, sizeof(mntent_buffer))) != NULL) {
        buf_len = (unsigned int)strlen(ent->mnt_dir);
        /*
         * get the file system with type of ext* or xfs.
         * find the best fit for the data directory
         *
         * buf_len > len_count: avoid two mount points both matches, keep the longer one
         */
        if (strncmp(ent->mnt_dir, datadir, buf_len) == 0 && strlen(datadir) >= buf_len &&
            (strncmp(ent->mnt_type, "ext", 3) == 0 || strcmp(ent->mnt_type, "xfs") == 0) && len_count < buf_len) {
            if (strncmp("/dev/sd", ent->mnt_fsname, 7) == 0 || strncmp("/dev/hd", ent->mnt_fsname, 7) == 0 ||
                strncmp("/dev/xvd", ent->mnt_fsname, 8) == 0 || strncmp("/dev/xcd", ent->mnt_fsname, 8) == 0 ||
                strncmp("/dev/hvc", ent->mnt_fsname, 8) == 0 || strncmp("/dev/vcs", ent->mnt_fsname, 8) == 0) {
                len_count = buf_len;

                rc = strncpy_s(devicename, length, ent->mnt_fsname + 5, strlen(ent->mnt_fsname + 5));
                securec_check(rc, "\0", "\0");
            } else if (strncmp("/dev/mapper", ent->mnt_fsname, 11) == 0) { /* dev mapper is symbolic link */
                len_count = buf_len;

                char* retVal = NULL;
                char real_path[PATH_MAX] = {0};

                /* start to read symbolic link */
                retVal = realpath(ent->mnt_fsname, real_path);
                if (retVal != NULL && '\0' != real_path[0]) {
                    if (strncmp("/dev/", real_path, 5) == 0) {
                        rc = strncpy_s(devicename, length, real_path + 5, strlen(real_path + 5));
                        securec_check(rc, "\0", "\0");
                    }
                }
            }
        }
    }

    ereport(DEBUG1, (errmsg("device name is %s", devicename)));

    fclose(mtfp);
}

void read_uptime(unsigned long long* uptime)
{
#define UPTIME "/proc/uptime"
    FILE* fp = NULL;
    char line[128];
    unsigned long up_sec, up_cent;

    if ((fp = fopen(UPTIME, "r")) == NULL) {
        ereport(LOG, (errmsg("cannot open file: %s \n", UPTIME)));
        return;
    }

    if (fgets(line, sizeof(line), fp) == NULL) {
        ereport(LOG, (errmsg("cannot read file: %s \n", UPTIME)));
        fclose(fp);
        return;
    }

    if (sscanf_s(line, "%lu.%lu", &up_sec, &up_cent) == 2) {
        *uptime = (unsigned long long)up_sec * 100 + (unsigned long long)up_cent;
    }
    fclose(fp);
}

/**
 * @description: parse /proc/diskstats file and fill in DiskIOStats struct
 * @param device
 */
void read_diskstat(struct DiskIOStats* device)
{
#define DISKSTATS "/proc/diskstats"
    int i;
    FILE* fp;
    char line[256], dev_name[MAX_DEVICE_LEN];
    unsigned int discard_ticks, total_ticks, queue_ticks, write_ticks;
    unsigned long read_io, read_merges, read_ticks, write_io;
    unsigned long write_merges, read_sectors, write_sectors;
    unsigned long dc_ios, dc_merges, dc_sec, dc_ticks;
    unsigned int major, minor;

    if ((fp = fopen(DISKSTATS, "r")) == NULL) {
        return;
    }

    while (fgets(line, sizeof(line), fp) != NULL) {
        /* major minor name rio rmerge rsect ruse wio wmerge wsect wuse running use aveq dcio dcmerge dcsect dcuse */
        i = sscanf_s(line,
            "%u %u %s %lu %lu %lu %lu %lu %lu %lu %u %u %u %u %lu %lu %lu %lu",
            &major,
            &minor,
            dev_name,
            MAX_DEVICE_LEN,
            &read_io,
            &read_merges,
            &read_sectors,
            &read_ticks,
            &write_io,
            &write_merges,
            &write_sectors,
            &write_ticks,
            &discard_ticks,
            &total_ticks,
            &queue_ticks,
            &dc_ios,
            &dc_merges,
            &dc_sec,
            &dc_ticks);

        unsigned int buf_len = (unsigned int)strlen(g_instance.wlm_cxt->instance_manager.diskname);

        if (strncmp(g_instance.wlm_cxt->instance_manager.diskname, dev_name, buf_len) != 0) {
            continue;
        }

        if (i >= 14) {
            device->read_io = read_io;
            device->read_merges = read_merges;
            device->read_sectors = read_sectors;
            device->read_ticks = (unsigned int)read_ticks;
            device->write_io = write_io;
            device->write_merges = write_merges;
            device->write_sectors = write_sectors;
            device->write_ticks = write_ticks;
            device->discard_ticks = discard_ticks;
            device->total_ticks = total_ticks;
            device->queue_ticks = queue_ticks;

            device->total_io = device->read_io + device->write_io;
        }
    }
    fclose(fp);
}

/*
 * Compute time interval.
 * IN:
 *  @prev	Previous uptime value (in jiffies).
 *  @curr	Current uptime value (in jiffies).
 * RETURNS:
 *  Interval of time in jiffies.
 */
unsigned long long get_interval(unsigned long long prev, unsigned long long curr)
{
    unsigned long long interval = curr - prev;

    return (interval == 0) ? 1 : interval;
}

/*
 * function name: compute_disk_stats
 * description  : compute device metric .
 * arguments    : device1 is prev stat data, device2 is curr stat data,
 *                  itv is interval, metric is calculate result
 * return value : void
 */
void compute_disk_stats(
    struct DiskIOStats* device1, struct DiskIOStats* device2, unsigned long long itv, struct DiskIOStatsMetric* metric)
{
/* With S_VALUE macro, the interval of time (@p) is given in 1/100th of a second */
#define S_VALUE(m, n, p) (((double)((m) - (n))) / (p)*100)
    if (itv == 0) {
        itv = 1;
    }
    metric->util = S_VALUE(device2->total_ticks, device1->total_ticks, itv) / 10.0;
    metric->await =
        (device2->total_io - device1->total_io)
            ? ((device2->read_ticks - device1->read_ticks) + (device2->write_ticks - device1->write_ticks)) /
                  ((double)(device2->total_io - device1->total_io))
            : 0.0;

    /* sesctors, one sector is 512Bytes, so need to divide by 2 */
    metric->rsectors = S_VALUE(device2->read_sectors, device1->read_sectors, itv);
    metric->wsectors = S_VALUE(device2->write_sectors, device1->write_sectors, itv);
}

/*
 * function name: WLMInitDiskname
 * description  : get DN/CN disk name .
 * return value : void
 */
void WLMInitDiskname(void)
{
    /* need to initialize the diskname first */
    if (strlen(g_instance.wlm_cxt->instance_manager.diskname) == 0) {
        /* get device name of the mount directory */
        WLMDNGetDiskName(t_thrd.proc_cxt.DataDir, g_instance.wlm_cxt->instance_manager.diskname, MAX_DEVICE_LEN);
    }
}

/*
 * function name: WLMCollectDiskIOStat
 * description  : collect disk IO info .
 * arguments    : instanceInfo
 * return value : void
 */
void WLMCollectDiskIOStat(WLMInstanceInfo* instanceInfo)
{
    WLMInitDiskname();

    struct DiskIOStatsMetric metric;
    unsigned long long itv;

    errno_t rc = EOK;
    rc = memset_s(&metric, sizeof(struct DiskIOStatsMetric), 0, sizeof(struct DiskIOStatsMetric));
    securec_check(rc, "\0", "\0");

    /* clear current device object to fill in statistics data */
    rc = memset_s(
        &g_instance.wlm_cxt->instance_manager.device[1], sizeof(struct DiskIOStats), 0, sizeof(struct DiskIOStats));
    securec_check(rc, "\0", "\0");

    /* read uptime */
    read_uptime(&g_instance.wlm_cxt->instance_manager.uptime[1]);

    /* read disk stat */
    read_diskstat(&g_instance.wlm_cxt->instance_manager.device[1]);

    /* calculate time interval */
    itv = get_interval(g_instance.wlm_cxt->instance_manager.uptime[0], g_instance.wlm_cxt->instance_manager.uptime[1]);

    /* compute disk stat */
    compute_disk_stats(
        &g_instance.wlm_cxt->instance_manager.device[0], &g_instance.wlm_cxt->instance_manager.device[1], itv, &metric);

    instanceInfo->io_await = metric.await;
    instanceInfo->io_util = metric.util;
    /* one sector is 512Bytes, so need to divide by 2 */
    instanceInfo->disk_read = metric.rsectors / 2;
    instanceInfo->disk_write = metric.wsectors / 2;

    /* store current value */
    g_instance.wlm_cxt->instance_manager.uptime[0] = g_instance.wlm_cxt->instance_manager.uptime[1];
    g_instance.wlm_cxt->instance_manager.device[0] = g_instance.wlm_cxt->instance_manager.device[1];
}

/*
 * function name: WLMCollectProcessIOStat
 * description  : collect process IO info .
 * arguments    : instanceInfo
 * return value : void
 */
void WLMCollectProcessIOStat(WLMInstanceInfo* instanceInfo)
{
#define PROCESS_IO "/proc/self/io"
    long secs;
    int usecs;
    int sleeptime = 1;

    TimestampDifference(g_instance.wlm_cxt->instance_manager.last_timestamp,
        g_instance.wlm_cxt->instance_manager.recent_timestamp,
        &secs,
        &usecs);
    sleeptime = (int)(secs * 1000 + usecs / 1000 + 1);

    if (sleeptime == 0) {
        return;
    }

    FILE* fp;
    if ((fp = fopen(PROCESS_IO, "r")) == NULL) {
        return;
    }

    char line[256];
    int i;
    const char *readstr = "read_bytes:";
    const char *writestr = "write_bytes:";
    Size readlen = strlen(readstr);
    Size writelen = strlen(writestr);

    while (fgets(line, sizeof(line), fp) != NULL) {
        if (strncmp(line, readstr, readlen) == 0) {
            i = sscanf_s(line + readlen + 1, "%lu", &g_instance.wlm_cxt->instance_manager.process_read_bytes[1]);
            if (i < 1) {
                ereport(WARNING, (errmsg("cannot get process_read_bytes from /proc/self/io \n")));
            }
        } else if (strncmp(line, writestr, writelen) == 0) {
            i = sscanf_s(line + writelen + 1, "%lu", &g_instance.wlm_cxt->instance_manager.process_write_bytes[1]);
            if (i < 1) {
                ereport(WARNING, (errmsg("cannot get process_write_bytes from /proc/self/io \n")));
            }
        }
    }
    fclose(fp);

    uint64 read_speed = (g_instance.wlm_cxt->instance_manager.process_read_bytes[1] -
                            g_instance.wlm_cxt->instance_manager.process_read_bytes[0]) /
                        sleeptime * 1000 / 1024;
    uint64 write_speed = (g_instance.wlm_cxt->instance_manager.process_write_bytes[1] -
                             g_instance.wlm_cxt->instance_manager.process_write_bytes[0]) /
                         sleeptime * 1000 / 1024;

    /* store current value */
    g_instance.wlm_cxt->instance_manager.process_read_bytes[0] =
        g_instance.wlm_cxt->instance_manager.process_read_bytes[1];
    g_instance.wlm_cxt->instance_manager.process_write_bytes[0] =
        g_instance.wlm_cxt->instance_manager.process_write_bytes[1];

    instanceInfo->process_read_speed = read_speed;
    instanceInfo->process_write_speed = write_speed;
}

/*
 * function name: WLMCollectInstanceLogicalIOStat
 * description  : collect logical IO info .
 * arguments    : void
 * return value : void
 */
void WLMCollectInstanceLogicalIOStat(WLMInstanceInfo* instanceInfo)
{
    long secs;
    int usecs;
    int sleeptime = 1;

    TimestampDifference(g_instance.wlm_cxt->instance_manager.last_timestamp,
        g_instance.wlm_cxt->instance_manager.recent_timestamp,
        &secs,
        &usecs);
    sleeptime = secs * 1000 + usecs / 1000 + 1;

    if (sleeptime == 0) {
        return;
    }

    uint64 read_speed = (g_instance.wlm_cxt->instance_manager.logical_read_bytes[1] -
                            g_instance.wlm_cxt->instance_manager.logical_read_bytes[0]) /
                        sleeptime * 1000 / 1024;
    uint64 write_speed = (g_instance.wlm_cxt->instance_manager.logical_write_bytes[1] -
                             g_instance.wlm_cxt->instance_manager.logical_write_bytes[0]) /
                         sleeptime * 1000 / 1024;

    uint64 read_counts =
        g_instance.wlm_cxt->instance_manager.read_counts[1] - g_instance.wlm_cxt->instance_manager.read_counts[0];
    uint64 write_counts =
        g_instance.wlm_cxt->instance_manager.write_counts[1] - g_instance.wlm_cxt->instance_manager.write_counts[0];

    /* store current logical read/write bytes */
    g_instance.wlm_cxt->instance_manager.logical_read_bytes[0] =
        g_instance.wlm_cxt->instance_manager.logical_read_bytes[1];
    g_instance.wlm_cxt->instance_manager.logical_write_bytes[0] =
        g_instance.wlm_cxt->instance_manager.logical_write_bytes[1];

    /* store current logical read/write operation count */
    g_instance.wlm_cxt->instance_manager.read_counts[0] = g_instance.wlm_cxt->instance_manager.read_counts[1];
    g_instance.wlm_cxt->instance_manager.write_counts[0] = g_instance.wlm_cxt->instance_manager.write_counts[1];

    /* assign read/write bytes value */
    instanceInfo->logical_read_speed = read_speed;
    instanceInfo->logical_write_speed = write_speed;

    /* assign read/write operation value */
    instanceInfo->read_counts = read_counts;
    instanceInfo->write_counts = write_counts;
}
/*
 * function name: WLMCollectInstanceMemStat
 * description  : get the remaining memory available to the process
 * arguments    : void
 * return value : void
 */
void WLMCollectInstanceMemStat(WLMInstanceInfo* instanceInfo)
{
    const char* statmPath = "/proc/self/statm";
    FILE* statmPathfp = NULL;
    unsigned long totalVm = 0;
    unsigned long res = 0;
    unsigned long shared = 0;
    unsigned long text = 0;
    unsigned long lib, data, dt;
    uint32 totalProcessMemory = 0;
    int pageSize = 0;

    statmPathfp = fopen(statmPath, "r");
    pageSize = getpagesize();  // gets the size(byte) for a page
    if (pageSize <= 0) {
        ereport(WARNING,
            (errcode(ERRCODE_WARNING),
                (errmsg("error for call 'getpagesize()', the values for instance_used_memory and instance_free_memory "
                        "are error!"))));
        pageSize = 1;
    }
    if (statmPathfp == NULL) {
        ereport(LOG, (errmsg("cannot open file: %s \n", statmPath)));
        return;
    }
    if (fscanf_s(statmPathfp, "%lu %lu %lu %lu %lu %lu %lu\n", &totalVm, &res, &shared, &text, &lib, &data, &dt) == 7) {
        totalVm = totalVm * pageSize / (1024 * 1024);  // page translated to MB
        res = res * pageSize / (1024 * 1024);          // page translated to MB
        shared = shared * pageSize / (1024 * 1024);    // page translated to MB
        text = text * pageSize / (1024 * 1024);        // page translated to MB
    }
    fclose(statmPathfp);

    instanceInfo->used_mem = res;
    totalProcessMemory = (uint32)((unsigned int)g_instance.attr.attr_memory.max_process_memory >> BITS_IN_KB);

    /* make sure res not greater than total memory */
    if (res >= totalProcessMemory) {
        instanceInfo->free_mem = 0;
    } else {
        instanceInfo->free_mem = (int)(totalProcessMemory - res);
    }
}
/*
 * function name: get_location
 * description  : get the location of utime
 * arguments    : void
 * return value : void
 */
char* GetLocation(char* currentLocation, int number)
{
    char* newLocation = currentLocation;
    int length = strlen(currentLocation);
    int spaceNum = 0;
    int i = 0;
    if (number <= 1) {
        return newLocation;
    }
    for (i = 0; i < length; i++) {
        if (*newLocation == ' ') {
            spaceNum++;
            if (spaceNum == (number - 1)) {
                newLocation++;
                break;
            }
        }
        newLocation++;
    }
    return newLocation;
}

/*
 * function name: WLMCollectInstanceCpuStat
 * description  : collect cpu info .
 * arguments    : void
 * return value : void
 */
void WLMCollectInstanceCpuStat(WLMInstanceInfo* instanceInfo)
{
    errno_t rc;
    FILE *cpuFp = NULL;
    FILE *cpuProceeFp = NULL;
    char firstLine[1024];
    uint64 cpuUser = 0;
    uint64 cpuNice = 0;
    uint64 cpuSys = 0;
    uint64 cpuIdle = 0;
    uint64 cpuIowait = 0;
    uint64 cpuHardirq = 0;
    uint64 cpuSoftirq = 0;
    uint64 cpuSteal = 0;
    uint64 cpuGuest = 0;
    uint64 cpuGuestNice = 0;
    uint64 utimeCurrent = 0;
    uint64 stimeCurrent = 0;
    uint64 itv = 0;
    char* ctimeLocation = NULL;
    const char* statPath = "/proc/self/stat";

    if ((cpuFp = fopen(FILE_CPUSTAT, "r")) == NULL) {
        ereport(LOG, (errmsg("cannot open file: %s \n", FILE_CPUSTAT)));
        return;
    }
    if (fgets(firstLine, sizeof(firstLine), cpuFp) != NULL) {
        rc = sscanf_s(firstLine + 5,
            "%lu %lu %lu %lu %lu %lu %lu %lu %lu %lu",
            &cpuUser,
            &cpuNice,
            &cpuSys,
            &cpuIdle,
            &cpuIowait,
            &cpuHardirq,
            &cpuSoftirq,
            &cpuSteal,
            &cpuGuest,
            &cpuGuestNice);
        /* There are some hosts that we can not get ten CPU parameters, such as kernel 2.6. */
        if (rc < 9) {
            ereport(WARNING, (errmsg("cannot get whole cpu information, we only get %d.\n", rc)));
        }

        g_instance.wlm_cxt->instance_manager.totalCPUTime[1] =
            cpuUser + cpuNice + cpuSys + cpuIdle + cpuIowait + cpuHardirq + cpuSteal + cpuSoftirq;
    }
    fclose(cpuFp);

    rc = memset_s(firstLine, sizeof(firstLine) / sizeof(char), 0, sizeof(firstLine) / sizeof(char));
    securec_check(rc, "\0", "\0");

    if ((cpuProceeFp = fopen(statPath, "r")) == NULL) {
        ereport(LOG, (errmsg("cannot open file: %s \n", statPath)));
        return;
    }

    if (fgets(firstLine, sizeof(firstLine), cpuProceeFp) != NULL) {
        ctimeLocation = GetLocation(firstLine, 14);
        rc = sscanf_s(ctimeLocation, "%lu %lu", &utimeCurrent, &stimeCurrent);
        if (rc < 2) {
            ereport(WARNING, (errmsg("cannot get user cpu time and system cpu time informations\n")));
        }
        g_instance.wlm_cxt->instance_manager.userCPUTime[1] = utimeCurrent;
        g_instance.wlm_cxt->instance_manager.sysCPUTime[1] = stimeCurrent;
    }
    fclose(cpuProceeFp);

    itv = g_instance.wlm_cxt->instance_manager.totalCPUTime[1] - g_instance.wlm_cxt->instance_manager.totalCPUTime[0];

    if (itv == 0) {
        itv = 1;
    }

    instanceInfo->usr_cpu = WLMmonitor_get_average_value(g_instance.wlm_cxt->instance_manager.userCPUTime[0],
        g_instance.wlm_cxt->instance_manager.userCPUTime[1],
        itv,
        FULL_PERCENT);
    instanceInfo->sys_cpu = WLMmonitor_get_average_value(g_instance.wlm_cxt->instance_manager.sysCPUTime[0],
        g_instance.wlm_cxt->instance_manager.sysCPUTime[1],
        itv,
        FULL_PERCENT);

    /* store current CPU time value */
    g_instance.wlm_cxt->instance_manager.totalCPUTime[0] = g_instance.wlm_cxt->instance_manager.totalCPUTime[1];
    g_instance.wlm_cxt->instance_manager.userCPUTime[0] = g_instance.wlm_cxt->instance_manager.userCPUTime[1];
    g_instance.wlm_cxt->instance_manager.sysCPUTime[0] = g_instance.wlm_cxt->instance_manager.sysCPUTime[1];
}
/*
 * function name: WLMCollectInstanceStat
 * description  : collect instance statistic info into hash .
 * arguments    : void
 * return value : void
 */
void WLMCollectInstanceStat(void)
{
    bool hasFound = false;

    /* hash is full, skip */
    if (hash_get_num_entries(g_instance.wlm_cxt->instance_manager.instance_info_hashtbl) >=
        g_instance.wlm_cxt->instance_manager.max_stat_num) {
        ereport(DEBUG1,
            (errmsg("Too many instance statistics in the memory, max_stat_num=%d.",
                g_instance.wlm_cxt->instance_manager.max_stat_num)));
        return;
    }

    uint32 hashCode = WLMHashCode(&g_instance.wlm_cxt->instance_manager.recent_timestamp, sizeof(int));

    LockInstanceRealTHashPartition(hashCode, LW_EXCLUSIVE);

    WLMInstanceInfo* instanceInfo =
        (WLMInstanceInfo*)hash_search(g_instance.wlm_cxt->instance_manager.instance_info_hashtbl,
            &g_instance.wlm_cxt->instance_manager.recent_timestamp,
            HASH_ENTER_NULL,
            &hasFound);

    if (instanceInfo != NULL) {
        instanceInfo->timestamp = g_instance.wlm_cxt->instance_manager.recent_timestamp;

        /* cpu */
        WLMCollectInstanceCpuStat(instanceInfo);

        /* memory */
        WLMCollectInstanceMemStat(instanceInfo);

        /* disk io */
        WLMCollectDiskIOStat(instanceInfo);

        /* process io */
        WLMCollectProcessIOStat(instanceInfo);

        /* logical io */
        WLMCollectInstanceLogicalIOStat(instanceInfo);
    }

    UnLockInstanceRealTHashPartition(hashCode);
}

/*
 * function name: WLMPersistentInstanceStat
 * description  : persistent instance statistic info in hash .
 * arguments    : void
 * return value : void
 */
void WLMPersistentInstanceStat(void)
{
#ifdef ENABLE_MULTIPLE_NODES
    if (!(IS_PGXC_COORDINATOR && PgxcIsCentralCoordinator(g_instance.attr.attr_common.PGXCNodeName))) {
        return;
    }

    char sql[WLM_RECORD_SQL_LEN] = {0};

    errno_t rc = EOK;
    rc = sprintf_s(sql, WLM_RECORD_SQL_LEN, "select create_wlm_instance_statistics_info();");
    securec_check_ss(rc, "\0", "\0");

    /*
     * We will start a connection to all nodes to
     * persistent instance info.
     */
    WLMRemoteNodeExecuteCmd(sql, false);
#endif
}

/*
 * function name: WLMCleanupHistoryInstanceStat
 * description  : cleanup history instance statistic info in table.
 * arguments    : void
 * return value : void
 */
void WLMCleanupHistoryInstanceStat(void)
{
#ifdef ENABLE_MULTIPLE_NODES
    if (!(IS_PGXC_COORDINATOR && PgxcIsCentralCoordinator(g_instance.attr.attr_common.PGXCNodeName))) {
        return;
    }

    if (u_sess->attr.attr_resource.instance_metric_retention_time == 0) {
        return;
    }

    time_t expired_time;
    struct tm* expired_tm = NULL;
    char expired_time_str[30] = {0};
    time_t time_now;

    /* get expired time */
    time_now = time(NULL);
    if (time_now > u_sess->attr.attr_resource.instance_metric_retention_time * SECS_PER_DAY) {
        expired_time = time_now - u_sess->attr.attr_resource.instance_metric_retention_time * SECS_PER_DAY;
        expired_tm = localtime(&expired_time);
    } else {
        ereport(LOG,
            (errmsg("The current system time minus the aging time is negative,current system time is %s\n",
                ctime(&time_now))));
        return;
    }
    if (NULL != expired_tm) {
        (void)strftime(expired_time_str, 30, "%Y-%m-%d_%H%M%S", expired_tm);

        char sql[WLM_RECORD_SQL_LEN] = {0};

        errno_t rc = EOK;
        rc = sprintf_s(
            sql, WLM_RECORD_SQL_LEN, "delete from gs_wlm_instance_history where timestamp < '%s';", expired_time_str);
        securec_check_ss(rc, "\0", "\0");

        /*
         * We will start a connection to all nodes to
         * cleanup history instance info.
         */
        WLMRemoteNodeExecuteCmd(sql, false);

    } else {
        ereport(LOG, (errmsg("failed to expire instance record. \n")));
    }
#endif
}

/*
 * function name: WLMPersistentUserResourceInfo
 * description  : persistent user resource info in hash .
 * arguments    : void
 * return value : void
 * */
void WLMPersistentUserResourceInfo(void)
{
#ifdef ENABLE_MULTIPLE_NODES
    if (!(IS_PGXC_COORDINATOR && PgxcIsCentralCoordinator(g_instance.attr.attr_common.PGXCNodeName))) {
        return;
    }

    char sql[WLM_RECORD_SQL_LEN] = {0};

    errno_t rc = EOK;
    rc = sprintf_s(sql, WLM_RECORD_SQL_LEN, "select gs_wlm_persistent_user_resource_info();");
    securec_check_ss(rc, "\0", "\0");

    USE_AUTO_LWLOCK(WorkloadUserInfoLock, LW_SHARED);

    /* get current session info count, we will do nothing if it's 0 */
    if (g_instance.wlm_cxt->stat_manager.user_info_hashtbl == NULL ||
        (hash_get_num_entries(g_instance.wlm_cxt->stat_manager.user_info_hashtbl)) == 0) {
        return;
    }

    RELEASE_AUTO_LWLOCK();

    WLMRemoteNodeExecuteCmd(sql, true);
#endif
}

/**
 * function name: WLMCleanupHistoryUserResourceInfo
 * description  : cleanup user resource info in table.
 * arguments    : void
 * return value : void
 * */
void WLMCleanupHistoryUserResourceInfo(void)
{
#ifdef ENABLE_MULTIPLE_NODES
    if (!(IS_PGXC_COORDINATOR && PgxcIsCentralCoordinator(g_instance.attr.attr_common.PGXCNodeName))) {
        return;
    }

    if (u_sess->attr.attr_resource.user_metric_retention_time == 0) {
        return;
    }

    time_t expired_time;
    struct tm* expired_tm = NULL;
    char expired_time_str[30] = {0};
    time_t time_now;

    /* get expired time */
    time_now = time(NULL);
    if (time_now > u_sess->attr.attr_resource.user_metric_retention_time * SECS_PER_DAY) {
        expired_time = time_now - u_sess->attr.attr_resource.user_metric_retention_time * SECS_PER_DAY;
        expired_tm = localtime(&expired_time);
    } else {
        ereport(LOG,
            (errmsg("The current system time minus the aging time is negative,current system time is %s\n",
                ctime(&time_now))));
        return;
    }

    if (NULL != expired_tm) {
        (void)strftime(expired_time_str, 30, "%Y-%m-%d_%H%M%S", expired_tm);

        char sql[WLM_RECORD_SQL_LEN] = {0};

        errno_t rc = EOK;
        rc = sprintf_s(sql,
            WLM_RECORD_SQL_LEN,
            "delete from gs_wlm_user_resource_history where timestamp < '%s';",
            expired_time_str);
        securec_check_ss(rc, "\0", "\0");

        WLMRemoteNodeExecuteCmd(sql, true);
    } else {
        ereport(LOG, (errmsg("failed to expire user resource record. \n")));
    }
#endif
}

void WLMInitPostgres()
{
    /*
     * We will initialize the connection to the datanodes,
     * and make the multinode executor ready. We do it only
     * once at the first time to get cpu time.
     */
    MemoryContext oldContext = MemoryContextSwitchTo(THREAD_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_CBB));

    char* username = NULL;

    if (strcmp(t_thrd.proc_cxt.MyProgName, "WLMmonitor")) {
        username = g_instance.wlm_cxt->stat_manager.mon_user;
    } else {
        username = g_instance.wlm_cxt->stat_manager.user;
    }

    t_thrd.proc_cxt.PostInit->SetDatabaseAndUser(g_instance.wlm_cxt->stat_manager.database, InvalidOid, username);
    t_thrd.proc_cxt.PostInit->InitWLM();

    (void)MemoryContextSwitchTo(oldContext);
}

/*
 * @Description: init transcation on workload backend thread
 * @OUT backinit: transaction init
 * @Return: void
 * @See also:
 */
void WLMInitTransaction(bool* backinit)
{
    if (*backinit || (IS_PGXC_DATANODE && g_instance.wlm_cxt->stat_manager.infoinit &&
                         !g_instance.attr.attr_resource.enable_perm_space))
        return;	
    /*
     * Reset the transaction state, it's always TRANS_INPROGRESS
     * in this thread until the thread is finished.
     */
    StartTransactionCommand();

    t_thrd.wlm_cxt.wlm_xact_start = true;

    /* We init multinode executor to send query only once. */
    *backinit = true;

    pgstat_report_statement_wlm_status();

    pgstat_report_activity(STATE_RUNNING, t_thrd.wlm_cxt.collect_info->sdetail.statement);
}

/*
 * @Description: scan all data node info in the hash table
 *               to collect cpu info on data node, fetch
 *               these info to coordinator.
 * @Return: void
 * @See also:
 */
void WLMCollectInfoScanner(void)
{
    CHECK_FOR_INTERRUPTS();

    if (IS_PGXC_COORDINATOR || IS_SINGLE_NODE) {
        /* No query need handle cpu time exception, we will do nothing. */
        if (hash_get_num_entries(g_instance.wlm_cxt->stat_manager.collect_info_hashtbl) > 0) {
            /* clean up all collect info first */
            WLMCleanUpCollectInfo();

            /* update collect info with data node info */
            WLMUpdateCollectInfo();

#ifndef ENABLE_UT
            pg_usleep(USECS_PER_SEC);
#endif
        }

        /* update user data in htab for perm space and IO info */
        WLMUpdateUserInfo();
    }

    if (!(IS_PGXC_COORDINATOR || IS_SINGLE_NODE)) {
        /* update user data in htab */
        WLMUpdateUserInfo();

        /* wait for the next time */
        pg_usleep(USECS_PER_SEC);
    }

    /* remove expired records in the session table on data node */
    WLMRemoveExpiredRecords();

    /* build user info and resource pool hash table if does not exist */
    if (!g_instance.wlm_cxt->stat_manager.infoinit) {
        if (!BuildUserRPHash()) {
            ereport(LOG, (errmsg("build user data failed")));
        } else {
            WLMSetBuildHashStat(1);
            ereport(LOG, (errmsg("build user data finished")));
        }
    }
}

/*
 * @Description: check whether IO is under control or not
 *               check whether io_priority and iops_limits of resource pool are not 0
 *               check whether GUC variables of io_priority and io_limits are not 0
 * @Return: void
 * @See also:
 */
bool WLMCheckIOIsUnderControl(void)
{
    // GUC variables
    if (u_sess->wlm_cxt->wlm_params.iops_limits || u_sess->wlm_cxt->wlm_params.io_priority) {
        return true;
    }

    USE_AUTO_LWLOCK(WorkloadUserInfoLock, LW_SHARED);

    UserData* userdata = (UserData*)GetUserDataFromHTab(GetUserId(), true);

    // resource pool variables
    if (userdata != NULL && userdata->respool != NULL &&
        (userdata->respool->iops_limits || userdata->respool->io_priority)) {
        return true;
    }

    return false;
}

/*
 * function name: WLMCpuCheckPoint
 * description  : get cpu time, and append the info to
 *                the collect info list.
 * arguments    : void
 * return value : void
 */
void WLMCpuCheckPoint(void)
{
    /* Prevent interrupts while computing cpu time */
    HOLD_INTERRUPTS();

    if (u_sess->wlm_cxt->wlm_params.ptr) {
        WLMDNodeInfo* info = (WLMDNodeInfo*)u_sess->wlm_cxt->wlm_params.ptr;

        int64 newCpuTime = getCpuTime();

        /* Compute the delta cpu time */
        int64 deltaTime = newCpuTime - t_thrd.wlm_cxt.dn_cpu_detail->cpuStartTime;

        /* save last cpu time as new start time */
        t_thrd.wlm_cxt.dn_cpu_detail->cpuStartTime = newCpuTime;

        /* increase total cpu time at query level */
        gs_atomic_add_64(&info->geninfo.totalCpuTime, deltaTime);
    }

    RESUME_INTERRUPTS();
}

/*
 * function name: WLMCheckPoint
 * description  : workload check point
 * arguments    :
 *                _in_ status: current query status
 * return value : void
 */
void WLMCheckPoint(WLMStatusTag status)
{
    /* status is invalid, nothing to do */
    if (status != WLM_STATUS_RESERVE && status != WLM_STATUS_RUNNING && status != WLM_STATUS_RELEASE) {
        return;
    }

    /* Get cpu time */
    WLMCpuCheckPoint();
}

/*
 * function name: WLMReleaseNodeFromHash
 * description  : append the info to the collect
 *                info list to release the node in hash.
 * arguments    : void
 * return value : void
 */
void WLMReleaseNodeFromHash(void)
{
    USE_MEMORY_CONTEXT(g_instance.wlm_cxt->workload_manager_mcxt);

    WLMCleanUpNodeInternal(&u_sess->wlm_cxt->wlm_params.qid);
}

/*
 * function name: WLMReleaseNodeFromHash
 * description  : release IO info node in hash.
 * arguments    : void
 * return value : void
 */
void WLMReleaseIoInfoFromHash(void)
{
    WLMGeneralParam* g_wlm_params = &u_sess->wlm_cxt->wlm_params;
    if (!u_sess->wlm_cxt->wlm_params.iotrack && !u_sess->wlm_cxt->wlm_params.iocontrol) {
        return;
    }

    uint32 bucketId = GetIoStatBucket(&g_wlm_params->qid);
    int lockId = GetIoStatLockId(bucketId);
    (void)LWLockAcquire(GetMainLWLockByIndex(lockId), LW_EXCLUSIVE);

    WLMDNodeIOInfo* ioinfo = (WLMDNodeIOInfo*)hash_search(
        G_WLM_IOSTAT_HASH(bucketId), &g_wlm_params->qid, HASH_FIND, NULL);
    if (ioinfo != NULL) {
        /*
         * We will check the thread count, if all threads has finished,
         * we will release the thread list and remove it from the hash table.
         */
        (void)pg_atomic_fetch_sub_u32((volatile uint32*)&ioinfo->threadCount, 1);

        /* No threads already, remove the info from the hash table. */
        if (ioinfo->threadCount <= 0) {
            hash_search(G_WLM_IOSTAT_HASH(bucketId), &g_wlm_params->qid, HASH_REMOVE, NULL);
        }
    }
    LWLockRelease(GetMainLWLockByIndex(lockId));
    g_wlm_params->iotrack = 0;
    g_wlm_params->iocontrol = 0;
    g_wlm_params->iocount = 0;
}

/*
 * function name: WLMSetTimer
 * description  : set timer
 * arguments    : start time and stop time
 *                _in_ start: start time
 *                _in_ stop : stop time
 * return value : true: set success
 *                false: set failed
 */
bool WLMSetTimer(TimestampTz start, TimestampTz stop)
{
    long secs;
    int usecs;
    struct itimerval timeval;

    TimestampDifference(start, stop, &secs, &usecs);

    /* If start time is equal to stop time, we set only 1 microsecond */
    if (secs == 0 && usecs == 0) {
        usecs = 1;
    }

    int ss_rc = memset_s(&timeval, sizeof(timeval), 0, sizeof(struct itimerval));
    securec_check(ss_rc, "\0", "\0");
    timeval.it_value.tv_sec = secs;
    timeval.it_value.tv_usec = usecs;

    if (gs_signal_settimer(&timeval)) {
        return false;
    }

    /*
     * We have set the timer, timer is active now,
     * and it's finish time stop time.
     */
    t_thrd.wlm_cxt.wlmalarm_timeout_active = true;
    t_thrd.wlm_cxt.wlmalarm_fin_time = stop;

    return true;
}

/*
 * function name: WLMGetIntervalTime
 * description  : get interval time
 * arguments    :
 *                 _in_ except: except data
 *                 _in_ kind: except kind
 * return value : interval time
 */
int WLMGetIntervalTime(const except_data_t* except, int kind)
{
    unsigned int interval_time = u_sess->attr.attr_resource.cpu_collect_timer;

    /*
     * If the threshold "allcputime" is set, we will set
     * the interval time is cpu collect timer, if not
     * we will use the qualification time.
     */
    if (except[kind].allcputime > 0 || except[kind].spoolsize > 0) {
        return interval_time;
    }

    if (except[kind].skewpercent > 0 && except[kind].qualitime > 0) {
        return rtl::max(interval_time, except[kind].qualitime);
    }

    return 0;
}

/*
 * function name: WLMIsExceptValid
 * description  : check if exception threshold is valid
 * arguments    :
 *                _in_ grp: cgroup info
 * return value : true: valid
 *                false: invalid
 */
bool WLMIsExceptValid(const gscgroup_grp_t* grp)
{
    if (grp == NULL) {
        return false;
    }

    const except_data_t* except = grp->except;

    /* check each threshold of the exception data */
    for (int i = 0; i < EXCEPT_ALL_KINDS; ++i) {
        if (except[i].blocktime > 0 || except[i].elapsedtime > 0 || except[i].allcputime > 0 ||
            except[i].spoolsize > 0 || except[i].broadcastsize > 0 ||
            (except[i].skewpercent > 0 && except[i].qualitime > 0)) {
            return true;
        }
    }

    return false;
}

/*
 * function name: WLMIsOutOfThreshold
 * description  : check if collect info is out of threshold
 * arguments    :
 *                _in_ pCollectInfo: collect info
 *                _in_ except: exception data
 * return value : true: yes
 *                false: no
 */
WLMExceptTag WLMIsOutOfThreshold(const WLMCollectInfo* pCollectInfo, const except_data_t* except)
{
    /* check all cpu time */
    if (except->allcputime > 0 &&
        pCollectInfo->sdetail.geninfo.totalCpuTime >= (int64)except->allcputime * MSECS_PER_SEC) {
        return WLM_EXCEPT_CPU;  // cpu exception
    }

    /* check cpu skew percent. */
    if (t_thrd.wlm_cxt.except_ctl->qualiEndTime > 0 && except->skewpercent > 0 &&
        pCollectInfo->execEndTime >= t_thrd.wlm_cxt.except_ctl->qualiEndTime) {
        return (GetSkewRatioOfDN(&pCollectInfo->sdetail.geninfo) >= (int)except->skewpercent) ? WLM_EXCEPT_CPU
                                                                                            : WLM_EXCEPT_NONE;
    }

    if (except->spoolsize > 0 && pCollectInfo->sdetail.geninfo.spillSize >= except->spoolsize) {
        return WLM_EXCEPT_SPOOL;
    }

    return WLM_EXCEPT_NONE;
}

/*
 * function name: WLMGetThresholdAction
 * description  : get threshold and the action with the exception conf
 * arguments    :
 *                _in_ pCollectInfo: collect info
 * return value : void
 */
void WLMGetThresholdAction(const WLMCollectInfo* pCollectInfo)
{
    ExceptionManager* g_exceptctl = t_thrd.wlm_cxt.except_ctl;

    t_thrd.wlm_cxt.except_ctl->max_interval_time = 0;
    g_exceptctl->max_adjust_time = 0;

    if (pCollectInfo->attribute == WLM_ATTR_INVALID) {
        return;
    }

    if (pCollectInfo->status == WLM_STATUS_PENDING) {
        g_exceptctl->max_waiting_time = 0;

        /* Get max waiting time while query is waiting in the queue. */
        if (g_exceptctl->except[EXCEPT_ABORT].blocktime > 0) {
            g_exceptctl->max_waiting_time = (int)g_exceptctl->except[EXCEPT_ABORT].blocktime;
        }
    } else if (pCollectInfo->status == WLM_STATUS_RUNNING) {
        g_exceptctl->max_running_time = 0;

        /* Get max running time while query is running. */
        if (g_exceptctl->except[EXCEPT_ABORT].elapsedtime > 0) {
            g_exceptctl->max_running_time = (int)g_exceptctl->except[EXCEPT_ABORT].elapsedtime;
        }

        int abort_interval_time = WLMGetIntervalTime(g_exceptctl->except, EXCEPT_ABORT);
        int penalty_interval_time = WLMGetIntervalTime(g_exceptctl->except, EXCEPT_PENALTY);

        /*
         * Get max interval time to handle penalty exception
         * while query is running. We will get the minimum
         * interval time between abort threshold and penalty
         * threshold.
         */
        if (abort_interval_time > 0) {
            if (penalty_interval_time > 0 && penalty_interval_time < abort_interval_time) {
                g_exceptctl->max_interval_time = penalty_interval_time;
            } else {
                g_exceptctl->max_interval_time = abort_interval_time;
            }
        } else {
            g_exceptctl->max_interval_time = penalty_interval_time;
        }
    }
}

/*
 * function name: WLMInsertCollectInfoIntoHashTable
 * description  : register collect info the hash table.
 * arguments    : void
 * return value : true: insert wlm collect info into hash table, false: not insert
 */
bool WLMInsertCollectInfoIntoHashTable(void)
{
    bool hasFound = false;
    WLMGeneralParam* g_wlm_params = &u_sess->wlm_cxt->wlm_params;

    /*
     * If the query do not use group exception handler
     * and resource track, we will not register info.
     */
    if (t_thrd.wlm_cxt.collect_info->attribute == WLM_ATTR_INVALID && g_wlm_params->memtrack == 0) {
        return false;
    }

    /* too many collect info now, ignore this time. */
    if (hash_get_num_entries(g_instance.wlm_cxt->stat_manager.collect_info_hashtbl) >
        g_instance.wlm_cxt->stat_manager.max_collectinfo_num) {
        t_thrd.wlm_cxt.collect_info->attribute = WLM_ATTR_INVALID;
        g_wlm_params->memtrack = 0;
        ereport(DEBUG1,
            (errmsg("Too many collect info in the memory, max_collectinfo_num=%d.",
                g_instance.wlm_cxt->stat_manager.max_collectinfo_num)));
        return false;
    }
    if (t_thrd.wlm_cxt.parctl_state.subquery) {
        return false;
    }

    uint32 hashCode = WLMHashCode(&g_wlm_params->qid, sizeof(Qid));

    (void)LockSessRealTHashPartition(hashCode, LW_EXCLUSIVE);

    WLMDNodeInfo* pDNodeInfo = (WLMDNodeInfo*)hash_search(
        g_instance.wlm_cxt->stat_manager.collect_info_hashtbl, &g_wlm_params->qid, HASH_ENTER_NULL, &hasFound);

    if (pDNodeInfo != NULL) {
        t_thrd.wlm_cxt.has_cursor_record = false;
        InitDNodeInfo(pDNodeInfo, &g_wlm_params->qid);

        /* data node info is in use */
        WLMCNUpdateDNodeInfoState(pDNodeInfo, WLM_ACTION_REMAIN);

        pDNodeInfo->tid = t_thrd.proc_cxt.MyProcPid;
        pDNodeInfo->respool = GetRespoolFromHTab(g_wlm_params->rpdata.rpoid, true);
        pDNodeInfo->mementry = t_thrd.shemem_ptr_cxt.mySessionMemoryEntry; /* get session memory entry */
        pDNodeInfo->block_time = WLMGetTimestampDuration(
            t_thrd.wlm_cxt.collect_info->blockStartTime, t_thrd.wlm_cxt.collect_info->blockEndTime);
        pDNodeInfo->start_time = t_thrd.wlm_cxt.collect_info->execStartTime;
        pDNodeInfo->userid = GetUserId();

        /* get schema name */
        if (u_sess->attr.attr_common.namespace_current_schema) {
            errno_t errval = strncpy_s(pDNodeInfo->schname,
                sizeof(pDNodeInfo->schname),
                u_sess->attr.attr_common.namespace_current_schema,
                sizeof(pDNodeInfo->schname) - 1);
            securec_check_errval(errval, , LOG);
        }

        USE_MEMORY_CONTEXT(g_instance.wlm_cxt->query_resource_track_mcxt);
        if (u_sess->attr.attr_resource.query_band) {
            pDNodeInfo->qband = pstrdup(u_sess->attr.attr_resource.query_band);
        }

        pDNodeInfo->statement = pstrdup(t_thrd.wlm_cxt.collect_info->sdetail.statement);

        /* cpu threshold is valid, set cpu control flag */
        if (t_thrd.wlm_cxt.except_ctl->max_interval_time > 0 || g_wlm_params->memtrack) {
            pDNodeInfo->cpuctrl = 1;
            g_wlm_params->cpuctrl = 1;
        }

        if (g_wlm_params->memtrack) {
            pDNodeInfo->restrack = 1;
        }
        WLMCNUpdateDNodeInfoState(g_wlm_params->ptr, WLM_ACTION_REMOVED);
        g_wlm_params->ptr = pDNodeInfo;

        ereport(DEBUG1, (errmsg("insert wlm collect info into hash table, tid: %lu.", g_wlm_params->qid.queryId)));

        UnLockSessRealTHashPartition(hashCode);
        return true;
    }

    UnLockSessRealTHashPartition(hashCode);
    return false;
}

/*
 * function name: WLMInsertIoInfoIntoHashTable
 * description  : register IO info the hash table.
 * arguments    : void
 * return value : void
 */
void WLMInsertIoInfoIntoHashTable(void)
{
    bool hasFound = false;
    errno_t rc;
    WLMGeneralParam* g_wlm_params = &u_sess->wlm_cxt->wlm_params;

    if (g_wlm_params->iotrack == 0) {
        return;
    }

    uint32 bucketId = GetIoStatBucket(&g_wlm_params->qid);
    int lockId = GetIoStatLockId(bucketId);
    (void)LWLockAcquire(GetMainLWLockByIndex(lockId), LW_EXCLUSIVE);
    /* too many collect info now, ignore this time. */
    if (hash_get_num_entries(G_WLM_IOSTAT_HASH(bucketId)) > g_instance.wlm_cxt->stat_manager.max_iostatinfo_num) {
        g_wlm_params->iotrack = 0;
        LWLockRelease(GetMainLWLockByIndex(lockId));
        return;
    }

    WLMDNodeIOInfo* pDNodeIoInfo = (WLMDNodeIOInfo*)hash_search(
        G_WLM_IOSTAT_HASH(bucketId), &g_wlm_params->qid, HASH_ENTER_NULL, &hasFound);

    if (pDNodeIoInfo != NULL) {
        rc = memset_s(pDNodeIoInfo, sizeof(WLMDNodeIOInfo), 0, sizeof(WLMDNodeIOInfo));
        securec_check(rc, "\0", "\0");

        pDNodeIoInfo->qid = g_wlm_params->qid;

        /* data node info is in use */
        WLMCNUpdateIoInfoState(pDNodeIoInfo, WLM_ACTION_REMAIN);

        pDNodeIoInfo->tid = t_thrd.proc_cxt.MyProcPid;
        pDNodeIoInfo->iops_limits = g_wlm_params->iops_limits;
        pDNodeIoInfo->io_priority = g_wlm_params->io_priority;

        /* set node group in data node IO info */
        rc = snprintf_s(pDNodeIoInfo->nodegroup, NAMEDATALEN, NAMEDATALEN - 1, "%s", g_wlm_params->ngroup);
        securec_check_ss(rc, "\0", "\0");

        g_wlm_params->ioptr = pDNodeIoInfo;

        ereport(DEBUG1, (errmsg("insert wlm io collect info into hash table, tid: %lu.", g_wlm_params->qid.queryId)));
    }
    LWLockRelease(GetMainLWLockByIndex(lockId));
}

/*
 * function name: WLMRecheckExcept
 * description  : reset exception threshold
 * arguments    :
 *                _in_ pCollectInfo: collect info
 * return value : void
 */
void WLMRecheckExcept(const WLMCollectInfo* pCollectInfo)
{
    ExceptionManager* g_exceptctl = t_thrd.wlm_cxt.except_ctl;

    g_exceptctl->blockEndTime = 0;
    g_exceptctl->execEndTime = 0;

    if (!WLMIsExceptValid(pCollectInfo->cginfo.grpinfo)) {
        return;
    }

    int rc = memcpy_s(g_exceptctl->except,
        sizeof(g_exceptctl->except),
        pCollectInfo->cginfo.grpinfo->except,
        sizeof(pCollectInfo->cginfo.grpinfo->except));
    securec_check(rc, "\0", "\0");

    /* get new threshold and action */
    WLMGetThresholdAction(pCollectInfo);

    /* We must check the threshold, and get the new interval time. */
    if (g_exceptctl->max_running_time > 0 || u_sess->attr.attr_common.StatementTimeout > 0) {
        g_exceptctl->execEndTime = 0;

        if (u_sess->attr.attr_common.StatementTimeout > 0) {
            g_exceptctl->execEndTime = GetStatementFinTime();
        }

        /* Get the minimum execution time */
        if (g_exceptctl->max_running_time > 0) {
            TimestampTz tmpEndTime =
                TimestampTzPlusMilliseconds(pCollectInfo->execStartTime, g_exceptctl->max_running_time * MSECS_PER_SEC);

            if (g_exceptctl->execEndTime <= 0 || g_exceptctl->execEndTime > tmpEndTime) {
                g_exceptctl->execEndTime = tmpEndTime;
            }
        }
    }
}

/*
 * function name: WLMCNProcessActionRemain
 * description  : process the remain action, if the threshold is
 *                still valid, it will reset the timer.
 * arguments    :
 *                _in_ pCollectInfo: collect info
 * return value : 0: Normal but not set timer
 *                1: Normal but set timer
 *                -1: abnormal and not set timer
 */
int WLMCNProcessActionRemain(const WLMCollectInfo* pCollectInfo)
{
    int ret = 0;

    /* If status is running and cgroup is still valid, we will reset timer. */
    if (pCollectInfo->status == WLM_STATUS_RUNNING && !pCollectInfo->cginfo.invalid) {
        TimestampTz now = GetCurrentTimestamp();
        ExceptionManager* g_exceptctl = t_thrd.wlm_cxt.except_ctl;

        if (g_exceptctl->max_interval_time > 0) {
            g_exceptctl->intvalEndTime =
                TimestampTzPlusMilliseconds(now, g_exceptctl->max_interval_time * MSECS_PER_SEC);
        }

        /* Compute a new interval end time. */
        if (g_exceptctl->intvalEndTime <= 0 ||
            (g_exceptctl->execEndTime > 0 && g_exceptctl->intvalEndTime > g_exceptctl->execEndTime)) {
            g_exceptctl->intvalEndTime = g_exceptctl->execEndTime;
        }

        /* The new interval end time is valid, it's OK to set a new timer. */
        if (g_exceptctl->intvalEndTime > 0) {
            (void)WLMSetTimer(now, g_exceptctl->intvalEndTime);
            ret = 1;
        }
    }

    return ret;
}

/*
 * function name: WLMCNProcessActionAdjust
 * description  : process the adjust action, if the threshold is
 *                still valid, it will reset the timer.
 * arguments    :
 *                _in_ pCollectInfo: collect info
 * return value : 0: Normal but not set timer
 *                1: Normal but set timer
 *                -1: abnormal and not set timer
 */
int WLMCNProcessActionAdjust(WLMCollectInfo* pCollectInfo)
{
    int ret = 0;

    /*
     * To avoid blocking the query execution, we update
     * a flag of the record in the hash table, while
     * backend collecor thread fetching cpu time, it will
     * switch cgroup for each thread on data nodes of the query.
     * If the cpu threshold is not valid, we only switch the
     * query thread on the coordinator.
     */
    if (pCollectInfo->status == WLM_STATUS_RUNNING) {
        if (g_instance.attr.attr_common.enable_thread_pool) {
            ereport(LOG, (errmsg("Cannot adjust control group in thread pool mode.")));
            return ret;
        }

        TimestampTz now = 0;

        if (StringIsValid(pCollectInfo->cginfo.cgroup)) {
            WLMDNodeInfo* pDNodeInfo = NULL;

            gscgroup_attach_task(t_thrd.wlm_cxt.thread_node_group, pCollectInfo->cginfo.cgroup);

            /* reset the priority */
            t_thrd.wlm_cxt.qnode.priority =
                gscgroup_get_percent(t_thrd.wlm_cxt.thread_node_group, pCollectInfo->cginfo.cgroup);

            uint32 hashCode = WLMHashCode(&u_sess->wlm_cxt->wlm_params.qid, sizeof(Qid));

            (void)LockSessRealTHashPartition(hashCode, LW_EXCLUSIVE);

            pDNodeInfo = (WLMDNodeInfo*)hash_search(g_instance.wlm_cxt->stat_manager.collect_info_hashtbl,
                &u_sess->wlm_cxt->wlm_params.qid,
                HASH_FIND,
                NULL);

            /* we use this flag to check whether to adjust cgroup. */
            if (pDNodeInfo != NULL && pDNodeInfo->geninfo.action == WLM_ACTION_REMAIN) {
                pDNodeInfo->geninfo.action = WLM_ACTION_ADJUST;
                errno_t rc =
                    sprintf_s(pDNodeInfo->cgroup, sizeof(pDNodeInfo->cgroup), "%s", pCollectInfo->cginfo.cgroup);
                securec_check_ss(rc, "\0", "\0");
            }

            UnLockSessRealTHashPartition(hashCode);

            /*
             * Thread has been attached to new cgroup, we must
             * revert the control group while query finished.
             */
            u_sess->wlm_cxt->cgroup_state = CG_USERSET;

            ereport(LOG, (errmsg("switch to attach new cgroup: \"%s\" group", pCollectInfo->cginfo.cgroup)));

            WLMProcessStatInfoVisualization(pCollectInfo);
        }

        now = GetCurrentTimestamp();
        ExceptionManager* g_exceptctl = t_thrd.wlm_cxt.except_ctl;

        if (g_exceptctl->max_interval_time > 0) {
            g_exceptctl->intvalEndTime =
                TimestampTzPlusMilliseconds(now, g_exceptctl->max_interval_time * MSECS_PER_SEC);
        }

        /* Compute a new interval end time. */
        if (g_exceptctl->intvalEndTime <= 0 ||
            (g_exceptctl->execEndTime > 0 && g_exceptctl->intvalEndTime > g_exceptctl->execEndTime)) {
            g_exceptctl->intvalEndTime = g_exceptctl->execEndTime;
        }

        /* The new interval end time is valid, it's OK to set a new timer. */
        if (g_exceptctl->intvalEndTime > 0) {
            (void)WLMSetTimer(now, g_exceptctl->intvalEndTime);
            ret = 1;
        }
    }

    return ret;
}

char* WLMGetExceptWarningMsg(WLMExceptTag tag)
{
    USE_MEMORY_CONTEXT(g_instance.wlm_cxt->query_resource_track_mcxt);

    switch (tag) {
        case WLM_EXCEPT_CPU:
            return pstrdup("query cpu time on datanodes limit exceeds");
        case WLM_EXCEPT_SPOOL:
            return pstrdup("query on datanodes spill size exceeds");
        case WLM_EXCEPT_RUN_TIMEOUT:
            return pstrdup("statement running timeout");
        case WLM_EXCEPT_QUEUE_TIMEOUT:
            return pstrdup("wlm active statements enqueue timeout");
        default:
            break;
    }

    return NULL;
}

/*
 * function name: WLMCNCentralProcessor
 * description  : cn process collect info
 * arguments    :
 *                _in_ pCollectInfo: collect info
 * return value : 0: Normal but not set timer
 *                1: Normal but set timer
 *                -1: abnormal and not set timer
 */
int WLMCNStatInfoProcessor(WLMCollectInfo* pCollectInfo)
{
    int ret = 0;
    WLMGeneralParam* g_wlm_params = &u_sess->wlm_cxt->wlm_params;

    switch (pCollectInfo->action) {
        case WLM_ACTION_REMAIN:
            ret = WLMCNProcessActionRemain(pCollectInfo);
            break;
        case WLM_ACTION_ADJUST:
            ret = WLMCNProcessActionAdjust(pCollectInfo);
            break;
        case WLM_ACTION_ABORT:
            /* it muse be running */
            if (pCollectInfo->status == WLM_STATUS_RUNNING) {
                /*
                 * If it's out of the threshold of the
                 * exception data, throw a warning messge.
                 */
                if (t_thrd.wlm_cxt.except_ctl->max_running_time > 0 || pCollectInfo->trait) {
                    ereport(WARNING, (errmsg("%s", pCollectInfo->sdetail.msg)));
                }

                /* send signal to abort the session */
                u_sess->wlm_cxt->cancel_from_wlm = true;
                (void)gs_signal_send(t_thrd.proc_cxt.MyProcPid, SIGINT);
            }
            break;
        case WLM_ACTION_FINISHED:

            if ((g_wlm_params->cpuctrl || g_wlm_params->memtrack) && t_thrd.wlm_cxt.parctl_state.subquery == 0) {
                /*
                 * if we have registered info in the hash table,
                 * reset the using flag and backend thread will remove it.
                 */
                WLMCNUpdateDNodeInfoState(g_wlm_params->ptr, WLM_ACTION_REMOVED);

                g_wlm_params->ptr = NULL;
                g_wlm_params->cpuctrl = 0;
            }

            if (g_wlm_params->iotrack) {
                WLMCNUpdateIoInfoState(g_wlm_params->ioptr, WLM_ACTION_REMOVED);
                g_wlm_params->ioptr = NULL;
            }

            /* reset the qid */
            ReleaseQid(&g_wlm_params->qid);

            g_wlm_params->ptr = NULL;

            /* insert the record into the view */
            WLMProcessStatInfoVisualization(pCollectInfo);
            break;
        default:
            break;
    }

    return ret;
}

/*
 * function name: WLMCNCentralAnalyzer
 * description  : cn analyze collect info,
 *                get the action to do.
 * arguments    :
 *                _in_ pCollectInfo: collect info
 * return value : 0: Normal but not set timer
 *                1: Normal but set timer
 *                -1: abnormal and not set timer
 */
int WLMCNStatInfoAnalyzer(WLMCollectInfo* pCollectInfo)
{
    if (pCollectInfo->status == WLM_STATUS_RUNNING) {
        pCollectInfo->action = WLM_ACTION_REMAIN;
        pCollectInfo->attribute = WLM_ATTR_NORMAL;
        pCollectInfo->trait = 0;

        /* check the threshold */
        if (t_thrd.wlm_cxt.except_ctl->execEndTime > 0 &&
            pCollectInfo->execEndTime >= t_thrd.wlm_cxt.except_ctl->execEndTime) {
            pCollectInfo->action = WLM_ACTION_ABORT;
            pCollectInfo->attribute = WLM_ATTR_INDB;

            /* The error message is to print if cgroup exception is triggered. */
            pCollectInfo->sdetail.msg = WLMGetExceptWarningMsg(WLM_EXCEPT_RUN_TIMEOUT);
        } else if (pCollectInfo->sdetail.geninfo.nodeCount > 0) { /* Have we got new cpu info? */
            WLMExceptTag etag = WLM_EXCEPT_NONE;
            /*
             * If the cpu time of the record in the hash
             * table is new, we will check the cpu exception.
             * First is abort, and then penalty.
             */
            if ((etag = WLMIsOutOfThreshold(pCollectInfo, t_thrd.wlm_cxt.except_ctl->except + EXCEPT_ABORT)) !=
                WLM_EXCEPT_NONE) {
                pCollectInfo->action = WLM_ACTION_ABORT;
                pCollectInfo->attribute = WLM_ATTR_INDB;
                pCollectInfo->trait = 1; /* trait for cpu except */

                /* The error message to print. */
                pCollectInfo->sdetail.msg = WLMGetExceptWarningMsg(etag);
            } else if ((etag = WLMIsOutOfThreshold(pCollectInfo, t_thrd.wlm_cxt.except_ctl->except + EXCEPT_PENALTY)) !=
                       WLM_EXCEPT_NONE) {
                if (StringIsValid(pCollectInfo->cginfo.cgroup)) {
                    pCollectInfo->action = WLM_ACTION_ADJUST;
                    pCollectInfo->attribute = WLM_ATTR_INDB;
                }
            }
        }

        /*
         * If it will adjust cgroup, we must check next cgroup
         * whether is valid. If the next cgroup is not valid,
         * we will set a flag and check this threshold no longer.
         */
        if (pCollectInfo->action == WLM_ACTION_ADJUST) {
            int ret = -1;

            if ((ret = gscgroup_get_next_group(t_thrd.wlm_cxt.thread_node_group, pCollectInfo->cginfo.cgroup)) <= 0) {
                pCollectInfo->action = WLM_ACTION_REMAIN;
                pCollectInfo->attribute = WLM_ATTR_NORMAL;

                /* Check current cgroup whether is valid no longer. */
                if (!pCollectInfo->cginfo.invalid) {
                    ereport(LOG,
                        (errmsg("Cgroup: \"%s\" can not get lower, "
                                "we will check this exception no longer.",
                            pCollectInfo->cginfo.cgroup)));

                    pCollectInfo->cginfo.invalid = true;
                }
            }
        }
    } else if (pCollectInfo->status == WLM_STATUS_FINISHED) {
        pCollectInfo->action = WLM_ACTION_FINISHED;

        if (t_thrd.wlm_cxt.except_ctl->max_interval_time > 0) {
            pCollectInfo->attribute = WLM_ATTR_INDB;
        }
    }

    return WLMCNStatInfoProcessor(pCollectInfo);
}

/*
 * function name: WLMCNCentralCollector
 * description  : cn collect dn info
 * arguments    :
 *                _in_ pCollectInfo: collect info
 * return value : 0: Normal but not set timer
 *                1: Normal but set timer
 *                -1: abnormal and not set timer
 */
int WLMCNStatInfoCollector(WLMCollectInfo* pCollectInfo)
{
    if (pCollectInfo->status == WLM_STATUS_RUNNING) {
        pCollectInfo->sdetail.geninfo.nodeCount = 0;

        uint32 hashCode = WLMHashCode(&u_sess->wlm_cxt->wlm_params.qid, sizeof(Qid));

        (void)LockSessRealTHashPartition(hashCode, LW_SHARED);

        WLMDNodeInfo* pDNodeInfo = (WLMDNodeInfo*)hash_search(
            g_instance.wlm_cxt->stat_manager.collect_info_hashtbl, &u_sess->wlm_cxt->wlm_params.qid, HASH_FIND, NULL);

        if (pDNodeInfo != NULL) {
            /*
             * If the current control group has changed, we must
             * update the collect info and get new exception data.
             */
            if (strcmp(pCollectInfo->cginfo.cgroup, pDNodeInfo->cgroup) != 0) {
                errno_t rc = sprintf_s(
                    pCollectInfo->cginfo.cgroup, sizeof(pCollectInfo->cginfo.cgroup), "%s", pDNodeInfo->cgroup);
                securec_check_ss(rc, "\0", "\0");

                pCollectInfo->cginfo.grpinfo =
                    gscgroup_get_grpconf(t_thrd.wlm_cxt.thread_node_group, pCollectInfo->cginfo.cgroup);
                t_thrd.wlm_cxt.qnode.priority =
                    gscgroup_get_percent(t_thrd.wlm_cxt.thread_node_group, pCollectInfo->cginfo.cgroup);
            }

            /* We have to reset exception threshold if the threshold has been changed. */
            WLMRecheckExcept(pCollectInfo);

            /*
             * we check scan count of data node info in the hash table,
             * avoid getting the old info.
             */
            if (t_thrd.wlm_cxt.except_ctl->max_interval_time > 0 && pDNodeInfo->cpuctrl > 0 &&
                pDNodeInfo->geninfo.scanCount > pCollectInfo->sdetail.geninfo.scanCount) {
                pCollectInfo->sdetail.geninfo.maxCpuTime = pDNodeInfo->geninfo.maxCpuTime;
                pCollectInfo->sdetail.geninfo.minCpuTime = pDNodeInfo->geninfo.minCpuTime;
                pCollectInfo->sdetail.geninfo.totalCpuTime = pDNodeInfo->geninfo.totalCpuTime;

                pCollectInfo->sdetail.geninfo.scanCount = pDNodeInfo->geninfo.scanCount;
                pCollectInfo->sdetail.geninfo.nodeCount = pDNodeInfo->geninfo.nodeCount;

                pCollectInfo->sdetail.geninfo.spillSize = pDNodeInfo->geninfo.spillSize;

                pgstat_report_statement_wlm_status();
            }
        }

        UnLockSessRealTHashPartition(hashCode);
    }

    return WLMCNStatInfoAnalyzer(pCollectInfo);
}

/*
 * function name: WLMSetSessionInfo
 * description  : insert the session info into the hash table
 *                for the finished query.
 * arguments    : void
 * return value : void
 */
void WLMSetSessionInfo(void)
{
    bool hasFound = false;
    errno_t rc;
    WLMGeneralParam* g_wlm_params = &u_sess->wlm_cxt->wlm_params;

    if (IsQidInvalid(&g_wlm_params->qid)) {
        return;
    }

    if (t_thrd.wlm_cxt.parctl_state.subquery) {
        return;
    }
    if ((IS_PGXC_COORDINATOR && !IsConnFromCoord()) || IS_SINGLE_NODE) {
        uint32 hashCode = WLMHashCode(&g_wlm_params->qid, sizeof(Qid));

        (void)LockSessRealTHashPartition(hashCode, LW_SHARED);
        LockSessHistHashPartition(hashCode, LW_EXCLUSIVE);
        int64 cnDuration =
            WLMGetTimestampDuration(t_thrd.wlm_cxt.collect_info->blockStartTime, GetCurrentTimestamp()) / MSECS_PER_SEC;
        // if execute time is smaller than resource_track_duration,not record session info to session_history table
        if (cnDuration < u_sess->attr.attr_resource.resource_track_duration) {
            hash_search(g_instance.wlm_cxt->stat_manager.session_info_hashtbl, &g_wlm_params->qid, HASH_REMOVE, NULL);
            UnLockSessHistHashPartition(hashCode);
            UnLockSessRealTHashPartition(hashCode);
            return;
        }
        /* Out of the memory we set, new record will not be written. */
        if (hash_get_num_entries(g_instance.wlm_cxt->stat_manager.session_info_hashtbl) >
            g_instance.wlm_cxt->stat_manager.max_detail_num) {
            UnLockSessHistHashPartition(hashCode);
            UnLockSessRealTHashPartition(hashCode);
            ereport(DEBUG1,
                (errmsg("Too many session info in the memory,max num is %d, wait for dumping into the table.",
                    g_instance.wlm_cxt->stat_manager.max_detail_num)));
            return;
        }
        WLMDNodeInfo* pRealDetail = (WLMDNodeInfo*)hash_search(
            g_instance.wlm_cxt->stat_manager.collect_info_hashtbl, &u_sess->wlm_cxt->wlm_params.qid, HASH_FIND, NULL);
        if (pRealDetail == NULL) {
            UnLockSessHistHashPartition(hashCode);
            UnLockSessRealTHashPartition(hashCode);
            return;
        }
        WLMStmtDetail* pDetail = (WLMStmtDetail*)hash_search(
            g_instance.wlm_cxt->stat_manager.session_info_hashtbl, &g_wlm_params->qid, HASH_ENTER_NULL, &hasFound);
        /* If we can find it in the hash table, we will do nothing. */
        if (pDetail == NULL || hasFound) {
            UnLockSessHistHashPartition(hashCode);
            UnLockSessRealTHashPartition(hashCode);
            return;
        }

        rc = memset_s(pDetail, sizeof(WLMStmtDetail), 0, sizeof(WLMStmtDetail));
        securec_check(rc, "\0", "\0");

        USE_MEMORY_CONTEXT(g_instance.wlm_cxt->query_resource_track_mcxt);

        /* Get session info from real session info. */
        pDetail->start_time = pRealDetail->start_time;
        pDetail->block_time = pRealDetail->block_time;
        pDetail->statement = FindCurrentUniqueSQL();

        /* Get session info from current thread. */
        pDetail->qid = g_wlm_params->qid;
        pDetail->databaseid = u_sess->proc_cxt.MyDatabaseId;
        pDetail->userid = GetUserId();
        if (u_sess->debug_query_id == 0)
            pDetail->debug_query_id = u_sess->slow_query_cxt.slow_query.debug_query_sql_id;
        else
            pDetail->debug_query_id = u_sess->debug_query_id;
        pDetail->fintime = GetCurrentTimestamp();
        pDetail->status = t_thrd.wlm_cxt.collect_info->status;
        /*
         * We have to save the max time to keep the record in the memory,
         * while the record expired, we will remove it from the hash table.
         */
        pDetail->exptime =
            TimestampTzPlusMilliseconds(GetCurrentTimestamp(), session_info_collect_timer * MSECS_PER_SEC);
        pDetail->estimate_time = t_thrd.shemem_ptr_cxt.mySessionMemoryEntry->estimate_time;
        pDetail->estimate_memory = t_thrd.shemem_ptr_cxt.mySessionMemoryEntry->estimate_memory;
        pDetail->query_plan_issue = pstrdup(t_thrd.shemem_ptr_cxt.mySessionMemoryEntry->query_plan_issue);
        pDetail->query_plan = pstrdup(t_thrd.shemem_ptr_cxt.mySessionMemoryEntry->query_plan);
        pDetail->plan_size = t_thrd.shemem_ptr_cxt.mySessionMemoryEntry->plan_size;

        pDetail->query_band = NULL;
        pDetail->msg = pstrdup(t_thrd.wlm_cxt.collect_info->sdetail.msg);

        pDetail->clienthostname = NULL;
        pDetail->schname = NULL;
        pDetail->username = NULL;
        /* only recored session info is slow query or when the duration is greater or equal to
         * u_sess->attr.attr_resource.resource_track_duration */
        int64 duration = (pDetail->fintime - pDetail->start_time) / USECS_PER_SEC;
        pDetail->valid = true;
        if (duration < u_sess->attr.attr_resource.resource_track_duration)
            pDetail->valid = false;

        pfree_ext(t_thrd.wlm_cxt.collect_info->sdetail.msg);
        pfree_ext(t_thrd.shemem_ptr_cxt.mySessionMemoryEntry->query_plan_issue);
        pfree_ext(t_thrd.shemem_ptr_cxt.mySessionMemoryEntry->query_plan);
        t_thrd.shemem_ptr_cxt.mySessionMemoryEntry->plan_size = 0;
        t_thrd.shemem_ptr_cxt.mySessionMemoryEntry->iscomplex = 1;

        if (u_sess->attr.attr_resource.query_band != NULL) {
            pDetail->query_band = pstrdup(u_sess->attr.attr_resource.query_band);
        }

        if (t_thrd.shemem_ptr_cxt.MyBEEntry->st_clienthostname) {
            pDetail->clienthostname = pstrdup(t_thrd.shemem_ptr_cxt.MyBEEntry->st_clienthostname);
        }

        if (u_sess->attr.attr_common.namespace_current_schema) {
            pDetail->schname = pstrdup(u_sess->attr.attr_common.namespace_current_schema);
        }

        if (u_sess->attr.attr_common.application_name) {
            errno_t errval = strncpy_s(pDetail->appname, sizeof(pDetail->appname),
                u_sess->attr.attr_common.application_name, sizeof(pDetail->appname) - 1);
            securec_check_errval(errval, , LOG);
        }
        pDetail->clientaddr = DUP_POINTER(&t_thrd.shemem_ptr_cxt.MyBEEntry->st_clientaddr);

        /* get resource pool */
        errno_t errval = strncpy_s(
            pDetail->respool, sizeof(pDetail->respool), g_wlm_params->rpdata.rpname, sizeof(pDetail->respool) - 1);
        securec_check_errval(errval, , LOG);

        /* get cgroup name */
        rc = snprintf_s(pDetail->cgroup, sizeof(pDetail->cgroup), sizeof(pDetail->cgroup) - 1,
            "%s", t_thrd.wlm_cxt.collect_info->cginfo.cgroup);
        securec_check_ss(rc, "\0", "\0");

        /* get node group name */
        errval = strncpy_s(
            pDetail->nodegroup, sizeof(pDetail->nodegroup), g_wlm_params->ngroup, sizeof(pDetail->nodegroup) - 1);
        securec_check_errval(errval, , LOG);
        WLMSetSessionSlowInfo(pDetail);

        WLMCNUpdateDNodeInfoState(pRealDetail, WLM_ACTION_REMOVED);

        UnLockSessHistHashPartition(hashCode);
        UnLockSessRealTHashPartition(hashCode);
    }

    if (IS_PGXC_DATANODE) {
        /* stream thread need not save session info */
        if (StreamThreadAmI()) {
            return;
        }

        uint32 hashCode = WLMHashCode(&g_wlm_params->qid, sizeof(Qid));

        LockSessHistHashPartition(hashCode, LW_EXCLUSIVE);
        int64 dnDuration = WLMGetTimestampDuration(t_thrd.shemem_ptr_cxt.mySessionMemoryEntry->dnStartTime,
            ((t_thrd.shemem_ptr_cxt.mySessionMemoryEntry->dnEndTime > 0) ?
             t_thrd.shemem_ptr_cxt.mySessionMemoryEntry->dnEndTime : GetCurrentTimestamp())) / MSECS_PER_SEC;
        if (t_thrd.wlm_cxt.dn_cpu_detail->status == WLM_STATUS_FINISHED &&
            dnDuration < u_sess->attr.attr_resource.resource_track_duration) {
            hash_search(g_instance.wlm_cxt->stat_manager.session_info_hashtbl, &g_wlm_params->qid, HASH_REMOVE, NULL);
            UnLockSessHistHashPartition(hashCode);
            return;
        }

        /* Out of the memory we set, new record will not be written. */
        if (hash_get_num_entries(g_instance.wlm_cxt->stat_manager.session_info_hashtbl) >
            g_instance.wlm_cxt->stat_manager.max_detail_num) {
            UnLockSessHistHashPartition(hashCode);
            ereport(DEBUG1,
                (errmsg("Too many session info in the memory,max num is %d, wait for dumping into the table.",
                    g_instance.wlm_cxt->stat_manager.max_detail_num)));
            return;
        }

        WLMStmtDetail* pDetail = (WLMStmtDetail*)hash_search(
            g_instance.wlm_cxt->stat_manager.session_info_hashtbl, &g_wlm_params->qid, HASH_ENTER_NULL, &hasFound);

        if (pDetail == NULL) {
            UnLockSessHistHashPartition(hashCode);
            return;
        }
#ifdef ENABLE_MULTIPLE_NODES
        rc = memset_s(pDetail, sizeof(WLMStmtDetail), 0, sizeof(WLMStmtDetail));
        securec_check(rc, "\0", "\0");
#endif
        pDetail->qid = g_wlm_params->qid;
        pDetail->valid = true;
        pDetail->threadid = t_thrd.proc_cxt.MyProcPid;

        /*
         * We have to save the max time to keep the record in the memory,
         * while the record expired, we will remove it from the hash table.
         */
        pDetail->exptime =
            TimestampTzPlusMilliseconds(GetCurrentTimestamp(), session_info_collect_timer * 2 * MSECS_PER_SEC);

        /* get the peak memory, spill count and total cpu time */
        pDetail->estimate_memory = t_thrd.shemem_ptr_cxt.mySessionMemoryEntry->estimate_memory;
        pDetail->geninfo.maxPeakChunksQuery = t_thrd.shemem_ptr_cxt.mySessionMemoryEntry->peakChunksQuery
                                              << (chunkSizeInBits - BITS_IN_MB);
        pDetail->geninfo.spillCount = t_thrd.shemem_ptr_cxt.mySessionMemoryEntry->spillCount;
        pDetail->geninfo.spillSize = t_thrd.shemem_ptr_cxt.mySessionMemoryEntry->spillSize;
        pDetail->geninfo.broadcastSize = t_thrd.shemem_ptr_cxt.mySessionMemoryEntry->broadcastSize;
        TimestampTz endTime = (t_thrd.shemem_ptr_cxt.mySessionMemoryEntry->dnEndTime > 0)
                                  ? t_thrd.shemem_ptr_cxt.mySessionMemoryEntry->dnEndTime
                                  : GetCurrentTimestamp();
        pDetail->geninfo.dnTime =
            WLMGetTimestampDuration(t_thrd.shemem_ptr_cxt.mySessionMemoryEntry->dnStartTime, endTime);
        pDetail->warning = t_thrd.shemem_ptr_cxt.mySessionMemoryEntry->warning;
        pDetail->geninfo.totalCpuTime = 0;
        pDetail->status = t_thrd.wlm_cxt.dn_cpu_detail->status;
        MemoryContext oldContext = MemoryContextSwitchTo(g_instance.wlm_cxt->query_resource_track_mcxt);
        pDetail->query_plan = pstrdup(t_thrd.shemem_ptr_cxt.mySessionMemoryEntry->query_plan);
        MemoryContextSwitchTo(oldContext);
        pDetail->plan_size = t_thrd.shemem_ptr_cxt.mySessionMemoryEntry->plan_size;
        pfree_ext(t_thrd.shemem_ptr_cxt.mySessionMemoryEntry->query_plan);
        t_thrd.shemem_ptr_cxt.mySessionMemoryEntry->plan_size = 0;

        /* update the mySessionMemoryEntry for memory manager display */
        t_thrd.shemem_ptr_cxt.mySessionMemoryEntry->iscomplex = 1;

        /* get resource pool */
        errno_t errval = strncpy_s(
            pDetail->respool, sizeof(pDetail->respool), g_wlm_params->rpdata.rpname, sizeof(pDetail->respool) - 1);
        securec_check_errval(errval, , LOG);
        pDetail->rpoid = g_wlm_params->rpdata.rpoid;

        if (g_wlm_params->ioptr != NULL) {
            WLMIoGeninfo ioinfo = ((WLMDNodeIOInfo*)g_wlm_params->ioptr)->io_geninfo;
            errval = memcpy_s(&pDetail->ioinfo, sizeof(ioinfo), &ioinfo, sizeof(ioinfo));
            securec_check_errval(errval, , LOG);
        }

        if (g_wlm_params->ptr != NULL) {
            pDetail->geninfo.totalCpuTime = ((WLMDNodeInfo*)g_wlm_params->ptr)->geninfo.totalCpuTime;
        }
        if (t_thrd.wlm_cxt.dn_cpu_detail->status == WLM_STATUS_FINISHED) {
            WLMSetSessionSlowInfo(pDetail);
        }

        UnLockSessHistHashPartition(hashCode);
    }
}

void WLMFillGeneralDataFromStmtInfo(WLMGeneralData* generalData, WLMStmtDetail* stmtInfo)
{
    int64 spillSize, broadcastSize, cpuTime;
    const int TOP5 = 5;

    WLMInitMinValue4SessionInfo(generalData);
    generalData->nodeCount = 1;
    generalData->warning |= stmtInfo->warning;
    spillSize = stmtInfo->geninfo.spillSize;
    if (spillSize / MBYTES >= WARNING_SPILL_SIZE) {
        generalData->warning |= (1 << WLM_WARN_SPILL_FILE_LARGE);
    }
    spillSize = (stmtInfo->geninfo.spillSize + MBYTES - 1) / MBYTES;
    broadcastSize = (stmtInfo->geninfo.broadcastSize + MBYTES - 1) / MBYTES;
    cpuTime = stmtInfo->geninfo.totalCpuTime / USECS_PER_MSEC;
    GetMaxAndMinSessionInfo(generalData, peakMemoryData, stmtInfo->geninfo.maxPeakChunksQuery);
    GetMaxAndMinSessionInfo(generalData, usedMemoryData, 0);
    GetMaxAndMinSessionInfo(generalData, spillData, stmtInfo->geninfo.spillSize);
    GetMaxAndMinSessionInfo(generalData, broadcastData, stmtInfo->geninfo.broadcastSize);
    GetMaxAndMinSessionInfo(generalData, cpuData, cpuTime);
    GetMaxAndMinSessionInfo(generalData, dnTimeData, stmtInfo->geninfo.dnTime);
    GetMaxAndMinSessionInfo(generalData, peakIopsData, stmtInfo->ioinfo.peak_iops);
    GetMaxAndMinSessionInfo(generalData, currIopsData, stmtInfo->ioinfo.curr_iops);
    setTopNWLMSessionInfo(generalData, 0, cpuTime, g_instance.attr.attr_common.PGXCNodeName, TOP5);
    if (stmtInfo->geninfo.spillCount > 0 || stmtInfo->geninfo.spillSize > 0) {
        generalData->spillNodeCount++;
    }
}

/*
 * function name: WLMGetSessionInfo
 * description  : get the session info from the hash table for the
 *                finished query. and then remove the record from
 *                the hash table.
 * arguments    :
 *                _in_ qid: qid info
                  _in_ removed: remove flag
                  _out_ num: record count
 * return value : session info array
 */
void* WLMGetSessionInfo(const Qid* qid, int removed, int* num)
{
    errno_t rc;

    /* check workload manager is valid */
    if (!ENABLE_WORKLOAD_CONTROL) {
        ereport(WARNING, (errmsg("workload manager is not valid.")));
        return NULL;
    }

    if (!u_sess->attr.attr_resource.enable_resource_track) {
        ereport(WARNING, (errmsg("enable_resource_track is not valid.")));
        return NULL;
    }

    if (IS_PGXC_COORDINATOR || IS_SINGLE_NODE) {
        int j;

        for (j = 0; j < NUM_SESSION_HISTORY_PARTITIONS; j++) {
            LWLockAcquire(GetMainLWLockByIndex(FirstSessionHistLock + j), LW_EXCLUSIVE);
        }

        /* no records, nothing to do. */
        if ((*num = hash_get_num_entries(g_instance.wlm_cxt->stat_manager.session_info_hashtbl)) <= 0) {
            for (j = NUM_SESSION_HISTORY_PARTITIONS; --j >= 0;) {
                LWLockRelease(GetMainLWLockByIndex(FirstSessionHistLock + j));
            }
            return NULL;
        }

        int i = 0;

        WLMStmtDetail* pDetail = NULL;
        WLMSessionStatistics* detail = NULL;
        Size totalSize = mul_size(sizeof(WLMSessionStatistics), (Size)(*num));
        WLMSessionStatistics* pDetails = (WLMSessionStatistics*)palloc0(totalSize);

        bool is_super_user = superuser() || isMonitoradmin(GetUserId());
        Oid cur_user_id = GetUserId();

        HASH_SEQ_STATUS hash_seq;
        hash_seq_init(&hash_seq, g_instance.wlm_cxt->stat_manager.session_info_hashtbl);

        /* Fetch all session info from the hash table. */
        while ((pDetail = (WLMStmtDetail*)hash_seq_search(&hash_seq)) != NULL) {
            if (pDetail->valid && (is_super_user || pDetail->userid == cur_user_id)) {
                StringInfoData plan_string;
                detail = pDetails + i;

                rc = memset_s(&detail->gendata, sizeof(detail->gendata), 0, sizeof(detail->gendata));
                securec_check_errval(rc, , LOG);
                WLMInitMinValue4SessionInfo(&detail->gendata);

                detail->qid = pDetail->qid;
                detail->databaseid = pDetail->databaseid;
                detail->userid = pDetail->userid;
                detail->debug_query_id = pDetail->debug_query_id;
                detail->start_time = pDetail->start_time;
                detail->fintime = pDetail->fintime;
                detail->status = pDetail->status;
                detail->block_time = pDetail->block_time;

                if (pDetail->query_plan == NULL) {
                    initStringInfo(&plan_string);
                    appendStringInfo(&plan_string, "Coordinator Name: %s \nNoPlan\n\n\n",
                        g_instance.attr.attr_common.PGXCNodeName);
                    plan_string.data[plan_string.len -1] = '\0';
                    detail->query_plan = pstrdup(plan_string.data);
                    pfree_ext(plan_string.data);
                } else {
                    detail->query_plan = pstrdup(pDetail->query_plan);
                }

                if (t_thrd.proc->workingVersionNum >= SLOW_QUERY_VERSION) {
                    detail->gendata.slowQueryInfo.current_table_counter =
                        (PgStat_TableCounts*) palloc0(sizeof(PgStat_TableCounts));
                    detail->gendata.slowQueryInfo.current_table_counter->t_tuples_returned =
                        pDetail->slowQueryInfo.current_table_counter->t_tuples_returned;
                    detail->gendata.slowQueryInfo.current_table_counter->t_tuples_fetched =
                        pDetail->slowQueryInfo.current_table_counter->t_tuples_fetched;
                    detail->gendata.slowQueryInfo.current_table_counter->t_tuples_inserted =
                        pDetail->slowQueryInfo.current_table_counter->t_tuples_inserted;
                    detail->gendata.slowQueryInfo.current_table_counter->t_tuples_updated =
                        pDetail->slowQueryInfo.current_table_counter->t_tuples_updated;
                    detail->gendata.slowQueryInfo.current_table_counter->t_tuples_deleted =
                        pDetail->slowQueryInfo.current_table_counter->t_tuples_deleted;
                    detail->gendata.slowQueryInfo.current_table_counter->t_blocks_fetched =
                        pDetail->slowQueryInfo.current_table_counter->t_blocks_fetched;
                    detail->gendata.slowQueryInfo.current_table_counter->t_blocks_hit =
                        pDetail->slowQueryInfo.current_table_counter->t_blocks_hit;

                    detail->gendata.slowQueryInfo.localTimeInfoArray =
                        (int64*) palloc0(sizeof(int64) * TOTAL_TIME_INFO_TYPES);
                    for (uint32 idx = 0; idx < TOTAL_TIME_INFO_TYPES; idx++) {
                        detail->gendata.slowQueryInfo.localTimeInfoArray[idx] =
                            pDetail->slowQueryInfo.localTimeInfoArray[idx];
                    }
                    detail->gendata.query_plan =
                        lappend(detail->gendata.query_plan, pstrdup(detail->query_plan));
                }
                detail->n_returned_rows = pDetail->slowQueryInfo.n_returned_rows;
                detail->query_plan_issue =
                    (pDetail->query_plan_issue == NULL) ? (char*)"" : pstrdup(pDetail->query_plan_issue);
                detail->query_band = (pDetail->query_band == NULL) ? (char*)"" : pstrdup(pDetail->query_band);
                detail->err_msg = (pDetail->msg == NULL) ? (char*)"" : pstrdup(pDetail->msg);
                detail->statement = (pDetail->statement == NULL) ? (char*)"" : pstrdup(pDetail->statement);
                detail->clienthostname =
                    (pDetail->clienthostname == NULL) ? (char*)"" : pstrdup(pDetail->clienthostname);
                detail->schname = (pDetail->schname == NULL) ? (char*)"" : pstrdup(pDetail->schname);
                detail->username = (pDetail->username == NULL) ? (char*)"" : pstrdup(pDetail->username);
                detail->appname = pstrdup(pDetail->appname);
                detail->clientaddr = NULL;
                detail->estimate_time = pDetail->estimate_time;
                detail->estimate_memory = pDetail->estimate_memory;
                /* not include block time */
                detail->duration = (detail->fintime - detail->start_time) / USECS_PER_MSEC;
                detail->remove = false;

                rc = snprintf_s(
                    detail->respool, sizeof(detail->respool), sizeof(detail->respool) - 1, "%s", pDetail->respool);
                securec_check_ss(rc, "\0", "\0");

                rc = snprintf_s(
                    detail->cgroup, sizeof(detail->cgroup), sizeof(detail->cgroup) - 1, "%s", pDetail->cgroup);
                securec_check_ss(rc, "\0", "\0");

                rc = snprintf_s(detail->nodegroup,
                    sizeof(detail->nodegroup),
                    sizeof(detail->nodegroup) - 1,
                    "%s",
                    pDetail->nodegroup);
                securec_check_ss(rc, "\0", "\0");

                if (pDetail->clientaddr != NULL) {
                    detail->clientaddr = palloc0(sizeof(SockAddr));
                    errno_t errval =
                        memcpy_s(detail->clientaddr, sizeof(SockAddr), pDetail->clientaddr, sizeof(SockAddr));
                    securec_check_errval(errval, , LOG);
                }

                if (u_sess->attr.attr_resource.enable_resource_record) {
                    if (removed > 0)
                        detail->remove = true;
                } else {
                    if (pDetail->exptime <= GetCurrentTimestamp())
                        detail->remove = true;
                }
#ifndef ENABLE_MULTIPLE_NODES
                WLMFillGeneralDataFromStmtInfo(&detail->gendata, pDetail);
#endif
                /* remove it from the hash table. */
                if (detail->remove) {
                    WLMReleaseStmtDetailItem(pDetail);

                    (void)hash_search(
                        g_instance.wlm_cxt->stat_manager.session_info_hashtbl, &pDetail->qid, HASH_REMOVE, NULL);
                }

                ++i;
            }
        }

        for (j = NUM_SESSION_HISTORY_PARTITIONS; --j >= 0;) {
            LWLockRelease(GetMainLWLockByIndex(FirstSessionHistLock + j));
        }

        *num = i;
#ifdef ENABLE_MULTIPLE_NODES
        char keystr[NAMEDATALEN];
        PGXCNodeAllHandles* pgxc_handles = NULL;

        pgxc_handles = WLMRemoteInfoCollectorStart();

        if (pgxc_handles == NULL) {
            pfree(pDetails);
            *num = 0;
            ereport(LOG, (errmsg("remote collector failed, reason: connect error.")));
            return NULL;
        }

        for (i = 0; i < *num; ++i) {
            detail = pDetails + i;
            errno_t ssval = snprintf_s(keystr,
                sizeof(keystr),
                sizeof(keystr) - 1,
                "%u,%lu,%ld,%d",
                detail->qid.procId,
                detail->qid.queryId,
                detail->qid.stamp,
                detail->remove);
            securec_check_ssval(ssval, , LOG);

            int ret = WLMRemoteInfoSender(pgxc_handles, keystr, WLM_COLLECT_SESSINFO);

            if (ret != 0) {
                release_pgxc_handles(pgxc_handles);
                pgxc_handles = NULL;

                ereport(ERROR,
                    (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
                    errmsg("Remote Sender: Failed to send command to datanode")));

            }

            /* Fetch session statistics from each datanode */
            detail->gendata.tag = WLM_COLLECT_SESSINFO;
            WLMRemoteInfoReceiver(pgxc_handles, &detail->gendata, sizeof(detail->gendata), WLMParseFunc4SessionInfo);
            WLMCheckMinValue4SessionInfo(&detail->gendata);
        }

        WLMRemoteInfoCollectorFinish(pgxc_handles);
#endif
        return pDetails;
    }

    return NULL;
}

char* PlanListToString(const List* query_plan)
{
    StringInfoData string;
    ListCell* l = NULL;
    initStringInfo(&string);

    foreach (l, query_plan) {
        char* plan = (char*)lfirst(l);
        appendStringInfoString(&string, plan);
    }
    return string.data;
}


/*
 * function name: WLMGetInstanceInfo
 * description  : get the instance statistics info from the hash table,
 *                and then remove the record from the hash table.
 * arguments    :
                  _out_ num: record count
 * return value : instance info array
 */
void* WLMGetInstanceInfo(int* num, bool isCleanup)
{
    /* check workload manager is valid */
    if (!u_sess->attr.attr_resource.use_workload_manager) {
        ereport(WARNING, (errmsg("workload manager is not valid.")));
        return NULL;
    }

    int j;

    for (j = 0; j < NUM_INSTANCE_REALTIME_PARTITIONS; j++) {
        LWLockAcquire(GetMainLWLockByIndex(FirstInstanceRealTLock + j), LW_EXCLUSIVE);
    }

    if ((*num = hash_get_num_entries(g_instance.wlm_cxt->instance_manager.instance_info_hashtbl)) == 0) {
        for (j = NUM_INSTANCE_REALTIME_PARTITIONS; --j >= 0;) {
            LWLockRelease(GetMainLWLockByIndex(FirstInstanceRealTLock + j));
        }
        ereport(DEBUG2, (errmodule(MOD_WLM), errmsg("the number of instance info is %d.", *num)));
        return NULL;
    }
    ereport(DEBUG2, (errmodule(MOD_WLM), errmsg("the number of instance info is %d.", *num)));

    int i = 0;

    WLMInstanceInfo* pDetail = NULL;
    WLMInstanceInfo* detail = NULL;
    WLMInstanceInfo* pDetails = (WLMInstanceInfo*)palloc0(*num * sizeof(WLMInstanceInfo));

    HASH_SEQ_STATUS hash_seq;
    hash_seq_init(&hash_seq, g_instance.wlm_cxt->instance_manager.instance_info_hashtbl);

    /* Fetch all session info from the hash table. */
    while ((pDetail = (WLMInstanceInfo*)hash_seq_search(&hash_seq)) != NULL) {
        detail = pDetails + i;

        detail->timestamp = pDetail->timestamp;
        detail->usr_cpu = pDetail->usr_cpu;
        detail->sys_cpu = pDetail->sys_cpu;
        detail->free_mem = pDetail->free_mem;
        detail->used_mem = pDetail->used_mem;
        detail->io_await = pDetail->io_await;
        detail->io_util = pDetail->io_util;
        detail->disk_read = pDetail->disk_read;
        detail->disk_write = pDetail->disk_write;
        detail->process_read_speed = pDetail->process_read_speed;
        detail->process_write_speed = pDetail->process_write_speed;
        detail->logical_read_speed = pDetail->logical_read_speed;
        detail->logical_write_speed = pDetail->logical_write_speed;
        detail->read_counts = pDetail->read_counts;
        detail->write_counts = pDetail->write_counts;

        if (isCleanup) {
            /* remove it from the hash table. */
            hash_search(
                g_instance.wlm_cxt->instance_manager.instance_info_hashtbl, &pDetail->timestamp, HASH_REMOVE, NULL);
            ereport(DEBUG2, (errmodule(MOD_WLM),
                errmsg("remove instance info(%lu) from hashtable.", pDetail->timestamp)));
        }

        ++i;

        /* break loop, because pDetails only contains num size of instance */
        if (i >= *num) {
            break;
        }
    }

    for (j = NUM_INSTANCE_REALTIME_PARTITIONS; --j >= 0;) {
        LWLockRelease(GetMainLWLockByIndex(FirstInstanceRealTLock + j));
    }

    if (hash_get_seq_num() > 0) {
        hash_seq_term(&hash_seq);
    }

    *num = i;
    ereport(DEBUG2, (errmodule(MOD_WLM), errmsg("the real-number of instance info is %d.", *num)));

    return pDetails;
}

/*
 * @Description: check user perm space
 * @IN void
 * @Return: void
 * @See also:
 */
void WLMCheckPermSpace(void)
{
    if (u_sess->wlm_cxt->wlm_params.iostate != IOSTATE_WRITE) {
        return;
    }

    USE_AUTO_LWLOCK(WorkloadUserInfoLock, LW_SHARED);

    UserData* userdata = GetUserDataFromHTab(GetUserId(), true);

    if (userdata == NULL) {
        return;
    }

    if ((userdata->spacelimit > 0 && userdata->totalspace >= userdata->spacelimit) ||
        (userdata->parent && userdata->parent->spacelimit > 0 &&
            userdata->parent->totalspace >= userdata->parent->spacelimit)) {
        RELEASE_AUTO_LWLOCK();
        ereport(ERROR, (errcode(ERRCODE_QUERY_CANCELED), errmsg("user space is out of space limit")));
    }
}

/*
 * @Description: set running start time
 * @IN void
 * @Return: void
 * @See also:
 */
void WLMSetExecutorStartTime(void)
{
    t_thrd.wlm_cxt.collect_info->blockEndTime = GetCurrentTimestamp();
    t_thrd.wlm_cxt.collect_info->execStartTime = GetCurrentTimestamp();
}

/*
 * @Description: get duration
 * @IN start: start time
 * @IN end: end time
 * @Return: duration (microsecords)
 * @See also:
 */
int64 WLMGetTimestampDuration(TimestampTz start, TimestampTz end)
{
    if (start <= 0) {
        return 0;
    }

    int64 result = end - start;

    if (result > 0) {
#ifdef HAVE_INT64_TIMESTAMP
        result /= USECS_PER_MSEC;
#else
        result *= MSECS_PER_SEC;
#endif
    } else {
        result = 0;
    }

    return result;
}


bool WLMSetCNCollectInfoStatusPending(WLMGeneralParam* g_wlm_params)
{
    t_thrd.wlm_cxt.collect_info->blockStartTime = GetCurrentTimestamp();
    
    /* reserve new qid */
    ReserveQid(&g_wlm_params->qid);

    g_wlm_params->qid.stamp = t_thrd.wlm_cxt.collect_info->blockStartTime;

    pgstat_report_statement_wlm_status();

    if (t_thrd.wlm_cxt.collect_info->attribute == WLM_ATTR_INVALID) {
        return false;
    }

    /* Get current threshold and action. */
    WLMGetThresholdAction(t_thrd.wlm_cxt.collect_info);

    /*
     * If we have set the waiting time that statement is waiting
     * in the queue, we will set timer to finish it or switch
     * to new priority list. Before set timer, we must check statement
     * finish time, if max waiting time is out of the finish time,
     * the timer will be set from block start time to statement finish time.
     */
    if (t_thrd.wlm_cxt.parctl_state.enqueue && t_thrd.wlm_cxt.except_ctl->max_waiting_time > 0) {
        const TimestampTz statement_fin_time = GetStatementFinTime();

        /* get current block end time during this time */
        if (t_thrd.wlm_cxt.except_ctl->max_waiting_time > 0) {
            t_thrd.wlm_cxt.except_ctl->blockEndTime =
                TimestampTzPlusMilliseconds(t_thrd.wlm_cxt.collect_info->blockStartTime,
                    t_thrd.wlm_cxt.except_ctl->max_waiting_time * MSECS_PER_SEC);
        }

        if (u_sess->attr.attr_common.StatementTimeout > 0 &&
            t_thrd.wlm_cxt.except_ctl->blockEndTime > statement_fin_time) {
            t_thrd.wlm_cxt.except_ctl->blockEndTime = statement_fin_time;
        }

        (void)WLMSetTimer(t_thrd.wlm_cxt.collect_info->blockStartTime, t_thrd.wlm_cxt.except_ctl->blockEndTime);
    }

    return true;
}

bool WLMSetCNCollectInfoStatusRunning(WLMGeneralParam* g_wlm_params)
{
    errno_t rc;

    dywlm_client_set_respool_memory(g_wlm_params->memsize, WLM_STATUS_RESERVE);

    WLMSetExecutorStartTime();

    /* reset datanode collect info */
    if (g_wlm_params->ptr != NULL) {
        WLMCNUpdateDNodeInfoState(g_wlm_params->ptr, WLM_ACTION_REMOVED);
        g_wlm_params->ptr = NULL;
    }

    if (g_wlm_params->ioptr != NULL) {
        WLMCNUpdateIoInfoState(g_wlm_params->ioptr, WLM_ACTION_REMOVED);
        g_wlm_params->ioptr = NULL;
    }

    /* memory track is on and query is need track resource, we will track the session info */
    if ((u_sess->attr.attr_resource.enable_resource_track && u_sess->exec_cxt.need_track_resource &&
            u_sess->attr.attr_resource.resource_track_level != RESOURCE_TRACK_NONE) ||
        !t_thrd.wlm_cxt.parctl_state.simple) {
        g_wlm_params->memtrack = 1;
    } else {
        g_wlm_params->memtrack = 0;

        /* When mySessionMemoryEntry->query_plan is not NULL,
           should not free g_collectInfo.sdetail.statement. */
        if (t_thrd.shemem_ptr_cxt.mySessionMemoryEntry != NULL &&
            t_thrd.shemem_ptr_cxt.mySessionMemoryEntry->query_plan == NULL &&
            t_thrd.wlm_cxt.collect_info->sdetail.statement) {
            pfree_ext(t_thrd.wlm_cxt.collect_info->sdetail.statement);
        }
    }

    // set IO control info on coordinator
    g_wlm_params->iops_limits = u_sess->attr.attr_resource.iops_limits ? u_sess->attr.attr_resource.iops_limits
                                                                       : (g_wlm_params->rpdata.iops_limits);
    g_wlm_params->io_priority = u_sess->attr.attr_resource.io_priority ? u_sess->attr.attr_resource.io_priority
                                                                       : (g_wlm_params->rpdata.io_priority);

    /*
     * IO is tracked on DN for complicated query or load job,
     * when enable_resource_track, iops_limits or io_priority is set by user.
     * IO collect info for complicated queries is gathered on CN
     * every 10 seconds only if enable_resource_track is open.
     */
    if (t_thrd.wlm_cxt.parctl_state.iocomplex == 1 || g_wlm_params->iostate != IOSTATE_NONE) {
        if (u_sess->attr.attr_resource.enable_resource_track) {
            g_wlm_params->iotrack = 1;
        }

        if (WLMCheckIOIsUnderControl()) {
            g_wlm_params->iocontrol = 1;
        }
    }

    /* Maybe the query have no control group, we have to set a default group. */
    if (*u_sess->wlm_cxt->control_group == '\0') {
        rc = snprintf_s(t_thrd.wlm_cxt.collect_info->cginfo.cgroup,
            sizeof(t_thrd.wlm_cxt.collect_info->cginfo.cgroup),
            sizeof(t_thrd.wlm_cxt.collect_info->cginfo.cgroup) - 1,
            "%s:%s",
            GSCGROUP_DEFAULT_CLASS,
            GSCGROUP_MEDIUM_TIMESHARE);
    } else {
        /*
         * control group maybe has changed in pending mode, so we
         * must reset the control group in the collect info here.
         */
        rc = snprintf_s(t_thrd.wlm_cxt.collect_info->cginfo.cgroup,
            sizeof(t_thrd.wlm_cxt.collect_info->cginfo.cgroup),
            sizeof(t_thrd.wlm_cxt.collect_info->cginfo.cgroup) - 1,
            "%s",
            u_sess->wlm_cxt->control_group);
    }
    securec_check_ss(rc, "\0", "\0");

    t_thrd.wlm_cxt.qnode.priority =
        gscgroup_get_percent(t_thrd.wlm_cxt.thread_node_group, u_sess->wlm_cxt->control_group);

    pgstat_report_statement_wlm_status();

    /* qid is invalid, attribute also is invalid */
    if (IsQidInvalid(&g_wlm_params->qid)) {
        t_thrd.wlm_cxt.collect_info->attribute = WLM_ATTR_INVALID;

        if (t_thrd.shemem_ptr_cxt.mySessionMemoryEntry != NULL &&
            t_thrd.shemem_ptr_cxt.mySessionMemoryEntry->query_plan != NULL) {
            pfree_ext(t_thrd.shemem_ptr_cxt.mySessionMemoryEntry->query_plan);
            t_thrd.shemem_ptr_cxt.mySessionMemoryEntry->plan_size = 0;
        }

        if (t_thrd.shemem_ptr_cxt.mySessionMemoryEntry != NULL &&
            t_thrd.shemem_ptr_cxt.mySessionMemoryEntry->query_plan_issue != NULL) {
            pfree_ext(t_thrd.shemem_ptr_cxt.mySessionMemoryEntry->query_plan_issue);
        }

        if (t_thrd.wlm_cxt.collect_info->sdetail.statement != NULL) {
            pfree_ext(t_thrd.wlm_cxt.collect_info->sdetail.statement);
        }

        return false;
    }

    /*
     * if we do not set any exception info in cgroup or the statement
     * is a simple statement, the attribute of collect info will be
     * set invalid, that is we need not handle any exception.
     */
    if (t_thrd.wlm_cxt.parctl_state.simple) {
        t_thrd.wlm_cxt.collect_info->attribute = WLM_ATTR_INVALID;
    }

    WLMGetThresholdAction(t_thrd.wlm_cxt.collect_info);

    /* register collect info */
    if ((t_thrd.wlm_cxt.parctl_state.subquery == 0) && !WLMInsertCollectInfoIntoHashTable()) {
        if (t_thrd.shemem_ptr_cxt.mySessionMemoryEntry != NULL &&
            t_thrd.shemem_ptr_cxt.mySessionMemoryEntry->query_plan != NULL) {
            pfree_ext(t_thrd.shemem_ptr_cxt.mySessionMemoryEntry->query_plan);
            t_thrd.shemem_ptr_cxt.mySessionMemoryEntry->plan_size = 0;
        }

        if (t_thrd.shemem_ptr_cxt.mySessionMemoryEntry != NULL &&
            t_thrd.shemem_ptr_cxt.mySessionMemoryEntry->query_plan_issue != NULL) {
            pfree_ext(t_thrd.shemem_ptr_cxt.mySessionMemoryEntry->query_plan_issue);
        }

        if (t_thrd.wlm_cxt.collect_info->sdetail.statement != NULL) {
            pfree_ext(t_thrd.wlm_cxt.collect_info->sdetail.statement);
        }
    }

    /*
     * insert io into hash table on CN,
     *  only when enable_resource_track is true.
     */
    WLMInsertIoInfoIntoHashTable();

    if (t_thrd.wlm_cxt.collect_info->attribute == WLM_ATTR_INVALID) {
        return false;
    }

    const TimestampTz statement_fin_time = GetStatementFinTime();
    TimestampTz intervalEndTime = 0;

    /* cpu threshold is valid */
    if (t_thrd.wlm_cxt.except_ctl->max_interval_time > 0) {
        intervalEndTime = TimestampTzPlusMilliseconds(t_thrd.wlm_cxt.collect_info->execStartTime,
            t_thrd.wlm_cxt.except_ctl->max_interval_time * MSECS_PER_SEC);

        t_thrd.wlm_cxt.except_ctl->qualiEndTime = intervalEndTime;
    }

    if (u_sess->attr.attr_common.StatementTimeout > 0) {
        t_thrd.wlm_cxt.except_ctl->execEndTime = statement_fin_time;
    }

    if (t_thrd.wlm_cxt.except_ctl->max_running_time > 0) {
        TimestampTz tmpEndTime = TimestampTzPlusMilliseconds(t_thrd.wlm_cxt.collect_info->execStartTime,
            t_thrd.wlm_cxt.except_ctl->max_running_time * MSECS_PER_SEC);

        if (t_thrd.wlm_cxt.except_ctl->execEndTime <= 0 ||
            t_thrd.wlm_cxt.except_ctl->execEndTime > tmpEndTime) {
            t_thrd.wlm_cxt.except_ctl->execEndTime = tmpEndTime;
        }
    }

    if (intervalEndTime <= 0 || (t_thrd.wlm_cxt.except_ctl->execEndTime > 0 &&
                                    intervalEndTime > t_thrd.wlm_cxt.except_ctl->execEndTime)) {
        intervalEndTime = t_thrd.wlm_cxt.except_ctl->execEndTime;
    }

    t_thrd.wlm_cxt.except_ctl->intvalEndTime = intervalEndTime;

    /* interval end time is valid, we will set a timer */
    if (t_thrd.wlm_cxt.except_ctl->intvalEndTime > 0) {
        (void)WLMSetTimer(t_thrd.wlm_cxt.collect_info->execStartTime,
                          t_thrd.wlm_cxt.except_ctl->intvalEndTime);
    }

    return true;
}

bool WLMSetCNCollectInfoStatusFinish(WLMGeneralParam* g_wlm_params)
{
    t_thrd.wlm_cxt.collect_info->execEndTime = GetCurrentTimestamp();
    
    pgstat_report_statement_wlm_status();

    dywlm_client_set_respool_memory(-g_wlm_params->memsize, WLM_STATUS_RELEASE);

    if (g_wlm_params->iotrack) {
        WLMCNUpdateIoInfoState(g_wlm_params->ioptr, WLM_ACTION_REMOVED);
    }

    u_sess->wlm_cxt->is_active_statements_reset = true;

    /* No exception handler and memory track, nothing to do. */
    if (t_thrd.wlm_cxt.collect_info->attribute == WLM_ATTR_INVALID && g_wlm_params->memtrack == 0 &&
        g_wlm_params->iotrack == 0) {
        return false;
    }

    if (g_wlm_params->memtrack) {
        WLMSetSessionInfo();
    }

    /* It's to clean collect info and insert the info into the view. */
    (void)WLMCNStatInfoCollector(t_thrd.wlm_cxt.collect_info);

    return true;
}

/*
 * function name: WLMSetCNCollectInfoStatus
 * description  : set cn collect info status
 * arguments    :
 *                _in_ status: query status to set
 * return value : void
 */
void WLMSetCNCollectInfoStatus(WLMStatusTag status)
{
    WLMGeneralParam* g_wlm_params = &u_sess->wlm_cxt->wlm_params;

    t_thrd.wlm_cxt.collect_info->status = status;

    switch (status) {
        case WLM_STATUS_PENDING:
            if (!WLMSetCNCollectInfoStatusPending(g_wlm_params)) {
                return;
            }
            break;
        case WLM_STATUS_RUNNING: {
            if (!WLMSetCNCollectInfoStatusRunning(g_wlm_params)) {
                return;
            }
            break;
        }
        case WLM_STATUS_FINISHED:
            if (!WLMSetCNCollectInfoStatusFinish(g_wlm_params)) {
                return;
            }
            break;
        case WLM_STATUS_ABORT:
        default:
            break;
    }
}

/*
 * function name: WLMSetDNodeInfoStatus
 * description  : set dn collect info status
 * arguments    : _in_ qid: qid
 *                _in_ status: query status to set
 * return value : void
 */
void WLMSetDNodeInfoStatus(const Qid* qid, WLMStatusTag status)
{
    errno_t rc;

    t_thrd.wlm_cxt.dn_cpu_detail->status = status;

    switch (status) {
        case WLM_STATUS_PENDING:
            rc = memset_s(
                t_thrd.wlm_cxt.dn_cpu_detail, sizeof(WLMDNRealTimeInfoDetail), 0, sizeof(WLMDNRealTimeInfoDetail));
            securec_check(rc, "\0", "\0");

            t_thrd.wlm_cxt.dn_cpu_detail->status = status;
            t_thrd.wlm_cxt.dn_cpu_detail->cpuStartTime = getCpuTime(); /* get the cpu start time */
            t_thrd.wlm_cxt.dn_cpu_detail->tid = THREADID;

            t_thrd.wlm_cxt.except_ctl->intvalEndTime = 0;

            /* set cpu time */
            WLMCheckPoint(WLM_STATUS_RESERVE);

            break;
        case WLM_STATUS_RUNNING: {
            /* set cpu time */
            WLMCheckPoint(WLM_STATUS_RUNNING);

            /* resource track is valid, we must save the session info */
            if (u_sess->wlm_cxt->wlm_params.memtrack) {
                WLMSetSessionInfo();
            }

            break;
        }
        case WLM_STATUS_FINISHED:
        case WLM_STATUS_ABORT:
            /* we will cancel the timer in stream thread while it is finished. */
            if (StreamThreadAmI()) {
                disable_sig_alarm(false);
            }

            /* check last cpu time. */
            WLMCheckPoint(WLM_STATUS_RELEASE);

            if (!StreamThreadAmI()) {
                t_thrd.shemem_ptr_cxt.mySessionMemoryEntry->dnEndTime = GetCurrentTimestamp();
            }

            /* resource track is valid, we must save the session info */
            if (u_sess->wlm_cxt->wlm_params.memtrack) {
                WLMSetSessionInfo();
            }

            /* The query is finished, we will remove the node from hash. */
            WLMReleaseNodeFromHash();
            WLMReleaseIoInfoFromHash();

            t_thrd.wlm_cxt.except_ctl->intvalEndTime = 0;

            ReleaseQid(&u_sess->wlm_cxt->wlm_params.qid);
            break;
        default:
            break;
    }
}

/*
 * function name: WLMCleanIOHashTable
 * description  : clean up IO hash table --- extern function for clean up IO info for vacuum lazy
 * arguments    : void
 * return value : void
 */
void WLMCleanIOHashTable()
{
    WLMGeneralParam* g_wlm_params = &u_sess->wlm_cxt->wlm_params;

    if (g_wlm_params->iotrack == 0 && g_wlm_params->iocontrol == 0) {
        return;
    }

    if (IS_PGXC_COORDINATOR) {
        if (g_wlm_params->ioptr) {
            WLMCNUpdateIoInfoState(g_wlm_params->ioptr, WLM_ACTION_REMOVED);
        }

        g_wlm_params->iotrack = 0;
    }
    if (IS_PGXC_DATANODE) {
        if (g_wlm_params->iotrack || g_wlm_params->iocontrol) {
            WLMReleaseIoInfoFromHash();
        }
    }
}

/*
 * @Description: search user info in the user configure file
 * @IN uid: user oid
 * @Return: user info in the config file
 * @See also:
 */
WLMUserInfo* WLMSearchUserInfo(Oid uid)
{
    /* make sure configure file is ok and uid is valid */
    if (!OidIsValid(uid) || g_instance.wlm_cxt->stat_manager.userinfo[0] == NULL) {
        return NULL;
    }

    for (int i = 0; i < GSUSER_ALLNUM; ++i) {
        if (g_instance.wlm_cxt->stat_manager.userinfo[i]->used == 0 ||
            !OidIsValid(g_instance.wlm_cxt->stat_manager.userinfo[i]->userid)) {
            continue;
        }

        /* match user id */
        if (g_instance.wlm_cxt->stat_manager.userinfo[i]->userid == uid) {
            return g_instance.wlm_cxt->stat_manager.userinfo[i];
        }
    }

    return NULL;
}

/*
 * @Description: get user info in the user configure file
 * @IN uid: user oid
 * @OUT found: user is found or not
 * @Return: user info in the config file
 * @See also:
 */
WLMUserInfo* WLMGetUserInfoFromConfig(Oid uid, bool* found)
{
    if (found != NULL) {
        *found = false;
    }

    Assert(uid != InvalidOid);

    if (!ENABLE_WORKLOAD_CONTROL || g_instance.wlm_cxt->stat_manager.userinfo == NULL) {
        return NULL;
    }

    /* search info with user id first */
    WLMUserInfo* info = WLMSearchUserInfo(uid);

    /* OK, we find it and return it to use */
    if (info != NULL) {
        if (found != NULL) {
            *found = true;
        }

        return info;
    }

    /* if info does not exist, we must create a new one */
    for (int i = 0; i < GSUSER_ALLNUM; ++i) {
        if (g_instance.wlm_cxt->stat_manager.userinfo[i] == NULL) {
            continue;
        }
        if (gs_compare_and_swap_32(&g_instance.wlm_cxt->stat_manager.userinfo[i]->used, 0, 1)) {
            return g_instance.wlm_cxt->stat_manager.userinfo[i];
        }
    }

    return NULL;
}

/*
 * function name: WLMCreateDNodeInfoOnDN
 * description  : create dnode info on dn
 * arguments    :
 *                _in_ qd: query description info
 * return value : void
 */
void WLMCreateDNodeInfoOnDN(const QueryDesc* qd)
{
    bool hasFound = false;
    Oid uid = GetUserId();
    WLMDNodeInfo* pDNodeInfo = NULL;
    int rc;
    WLMGeneralParam* g_wlm_params = &u_sess->wlm_cxt->wlm_params;

    USE_MEMORY_CONTEXT(g_instance.wlm_cxt->workload_manager_mcxt);

    if (!StreamThreadAmI() && g_instance.wlm_cxt->gscgroup_init_done > 0) {
        rc = snprintf_s(g_wlm_params->cgroup, NAMEDATALEN, NAMEDATALEN - 1, "%s", u_sess->wlm_cxt->control_group);
        securec_check_ss(rc, "\0", "\0");
    }

    g_wlm_params->userdata = NULL;

    /* this is used for IO statstics */
    if (g_wlm_params->complicate && StreamThreadAmI() && !IsQidInvalid(&g_wlm_params->qid)) {
        USE_AUTO_LWLOCK(WorkloadUserInfoLock, LW_SHARED);

        /* retrieve the user information based on ownerid from hash table */
        UserData* userdata = GetUserDataFromHTab(uid, true);
        g_wlm_params->userdata = userdata;
        g_wlm_params->complicate_stream = 1;
    }

    /* stream thread does not compute */
    if (StreamThreadAmI() || IsQidInvalid(&g_wlm_params->qid)) {
        g_wlm_params->complicate = 0;
    } else if (g_wlm_params->complicate
               && u_sess->debug_query_id && uid != BOOTSTRAP_SUPERUSERID &&
               t_thrd.shemem_ptr_cxt.mySessionMemoryEntry) {
        if (NULL == u_sess->wlm_cxt->local_foreign_respool) {
            USE_AUTO_LWLOCK(WorkloadUserInfoLock, LW_SHARED);

            /* retrieve the user information based on ownerid from hash table */
            UserData* userdata = GetUserDataFromHTab(uid, true);

            if (userdata != NULL) {
                USE_CONTEXT_LOCK(&userdata->mutex);

                PG_TRY();
                {
                    userdata->entry_list = lappend(userdata->entry_list, t_thrd.shemem_ptr_cxt.mySessionMemoryEntry);
                }
                PG_CATCH();
                {
                    RELEASE_CONTEXT_LOCK();

                    PG_RE_THROW();
                }
                PG_END_TRY();
            }

            g_wlm_params->userdata = userdata;
        } else {
            USE_CONTEXT_LOCK(&u_sess->wlm_cxt->local_foreign_respool->mutex);

            PG_TRY();
            {
                u_sess->wlm_cxt->local_foreign_respool->entry_list = lappend(
                    u_sess->wlm_cxt->local_foreign_respool->entry_list, t_thrd.shemem_ptr_cxt.mySessionMemoryEntry);
            }
            PG_CATCH();
            {
                RELEASE_CONTEXT_LOCK();

                PG_RE_THROW();
            }
            PG_END_TRY();
        }
    }

    if (g_wlm_params->complicate) {
        gs_atomic_add_32(&g_instance.wlm_cxt->stat_manager.comp_count, 1);
    }

    if (IsQidInvalid(&g_wlm_params->qid) || (g_wlm_params->cpuctrl == 0 && g_wlm_params->memtrack == 0)) {
        return;
    }

    /* We can only increase thread count while in the stream thread */
    if (StreamThreadAmI() && g_wlm_params->ptr) {
        WLMDNodeInfo* info = (WLMDNodeInfo*)g_wlm_params->ptr;

        /* count the thread of the query */
        (void)pg_atomic_fetch_add_u32((volatile uint32*)&info->threadCount, 1);

        return;
    }

    uint32 hashCode = WLMHashCode(&g_wlm_params->qid, sizeof(Qid));

    (void)LockSessRealTHashPartition(hashCode, LW_EXCLUSIVE);

    pDNodeInfo = (WLMDNodeInfo*)hash_search(
        g_instance.wlm_cxt->stat_manager.collect_info_hashtbl, &g_wlm_params->qid, HASH_ENTER_NULL, &hasFound);

    if (pDNodeInfo != NULL) {
        if (hasFound == false) {
            InitDNodeInfo(pDNodeInfo, &g_wlm_params->qid);

            if (qd != NULL && qd->plannedstmt != NULL && u_sess->wlm_cxt->wlm_num_streams <= 0) {
                u_sess->wlm_cxt->wlm_num_streams = qd->plannedstmt->num_streams;
            }

            /* Set thread count of the query on datanodes. */
            pDNodeInfo->threadCount = 1;
            pDNodeInfo->mementry = t_thrd.shemem_ptr_cxt.mySessionMemoryEntry; /* get session memory entry */
            pDNodeInfo->tid = (ThreadId)u_sess->debug_query_id;

            pDNodeInfo->userdata = GetUserDataFromHTab(uid, true);
            /* Set exception data of broadcast size on datanodes. */
            gscgroup_grp_t* grpinfo = gscgroup_get_grpconf(t_thrd.wlm_cxt.thread_node_group, pDNodeInfo->cgroup);
            if (WLMIsExceptValid(grpinfo)) {
                pDNodeInfo->geninfo.broadcastThreshold = grpinfo->except[EXCEPT_ABORT].broadcastsize * MBYTES;
            }

            g_wlm_params->ptr = pDNodeInfo;
        } else {
            (void)pg_atomic_fetch_add_u32((volatile uint32*)&pDNodeInfo->threadCount, 1);
        }

        UnLockSessRealTHashPartition(hashCode);
    } else {
        UnLockSessRealTHashPartition(hashCode);

        ereport(LOG, (errmsg("WLM Error: Cannot alloc memory, out of memory!")));
        return;
    }
}

/*
 * function name: WLMCreateIOInfoOnDN
 * description  : create IO info on dn
 * arguments    :
 * return value : void
 */
void WLMCreateIOInfoOnDN()
{
    bool hasFound = false;

    WLMDNodeIOInfo* pDNodeIoInfo = NULL;

    int rc;

    WLMGeneralParam* g_wlm_params = &u_sess->wlm_cxt->wlm_params;

    USE_MEMORY_CONTEXT(g_instance.wlm_cxt->workload_manager_mcxt);

    // only complicated query is considered in IO control
    if (IsQidInvalid(&g_wlm_params->qid) || (g_wlm_params->iocontrol == 0 && g_wlm_params->iotrack == 0)) {
        return;
    }

    /* We can only increase thread count while in the stream thread */
    if (StreamThreadAmI() && g_wlm_params->ioptr) {
        WLMDNodeIOInfo* info = (WLMDNodeIOInfo*)g_wlm_params->ioptr;

        /* count the thread of the query */
        (void)pg_atomic_fetch_add_u32((volatile uint32*)&info->threadCount, 1);

        return;
    }

    uint32 bucketId = GetIoStatBucket(&g_wlm_params->qid);
    int lockId = GetIoStatLockId(bucketId);
    (void)LWLockAcquire(GetMainLWLockByIndex(lockId), LW_EXCLUSIVE);
    WLMAutoLWLock user_lock(WorkloadUserInfoLock, LW_SHARED);

    pDNodeIoInfo = (WLMDNodeIOInfo*)hash_search(
        G_WLM_IOSTAT_HASH(bucketId), &g_wlm_params->qid, HASH_ENTER_NULL, &hasFound);

    if (pDNodeIoInfo != NULL) {
        if (!hasFound) {
            rc = memset_s(pDNodeIoInfo, sizeof(WLMDNodeIOInfo), 0, sizeof(WLMDNodeIOInfo));
            securec_check(rc, "\0", "\0");

            pDNodeIoInfo->qid = g_wlm_params->qid;
            pDNodeIoInfo->tid = (ThreadId)u_sess->debug_query_id;

            /* Set io parameters of the query on datanodes. */
            pDNodeIoInfo->io_priority = g_wlm_params->io_priority;
            pDNodeIoInfo->iops_limits = g_wlm_params->iops_limits;
            pDNodeIoInfo->io_state = g_wlm_params->iostate;
            pDNodeIoInfo->threadCount = 1;

            /* set node group in data node IO info */
            rc = snprintf_s(pDNodeIoInfo->nodegroup, NAMEDATALEN, NAMEDATALEN - 1, "%s", g_wlm_params->ngroup);
            securec_check_ss(rc, "\0", "\0");

            errno_t errval = memset_s(pDNodeIoInfo->io_geninfo.hist_iops_tbl,
                sizeof(pDNodeIoInfo->io_geninfo.hist_iops_tbl),
                0, sizeof(pDNodeIoInfo->io_geninfo.hist_iops_tbl));
            securec_check_errval(errval, , LOG);

            user_lock.AutoLWLockAcquire();

            pDNodeIoInfo->userptr = (UserData*)GetUserDataFromHTab(GetUserId(), true);

            user_lock.AutoLWLockRelease();

            g_wlm_params->ioptr = pDNodeIoInfo;

            ereport(DEBUG1, (errmsg("insert one element into IO hash table !")));
        } else {
            (void)pg_atomic_fetch_add_u32((volatile uint32*)&pDNodeIoInfo->threadCount, 1);
        }

        LWLockRelease(GetMainLWLockByIndex(lockId));
    } else {
        ereport(LOG, (errmsg("WLM Error: Cannot alloc memory, out of memory!")));

        LWLockRelease(GetMainLWLockByIndex(lockId));

        return;
    }
}

/*
 * function name: WLMResetStatInfo4Exception
 * description  : reset stat info for exception
 * arguments    : void
 * return value : void
 */
void WLMResetStatInfo4Exception(void)
{
    WLMGeneralParam* g_wlm_params = &u_sess->wlm_cxt->wlm_params;

    if (IS_PGXC_COORDINATOR && !IsConnFromCoord()) {
        if (t_thrd.wlm_cxt.collect_info->execStartTime) {
            t_thrd.wlm_cxt.collect_info->execEndTime = GetCurrentTimestamp();
        } else {
            t_thrd.wlm_cxt.collect_info->blockEndTime = GetCurrentTimestamp();
        }

        t_thrd.wlm_cxt.collect_info->status = WLM_STATUS_ABORT; /* set before call WLMSetSessionInfo() */
        t_thrd.wlm_cxt.collect_info->action = WLM_ACTION_ABORT;

        if (t_thrd.wlm_cxt.collect_info->attribute != WLM_ATTR_INVALID || g_wlm_params->memtrack) {
            /* resource track is valid, we must save the session info */
            if (g_wlm_params->memtrack) {
                WLMSetSessionInfo();
            }

            WLMCNUpdateDNodeInfoState(g_wlm_params->ptr, WLM_ACTION_REMOVED);

            /* release the qid */
            ReleaseQid(&g_wlm_params->qid);

            g_wlm_params->ptr = NULL;
        }

        if (g_wlm_params->rp) {
            dywlm_client_set_respool_memory(-g_wlm_params->memsize, WLM_STATUS_RELEASE);
        }

        if (!g_wlm_params->memtrack && t_thrd.wlm_cxt.collect_info->sdetail.msg) {
            pfree_ext(t_thrd.wlm_cxt.collect_info->sdetail.msg);
        }

        if (g_wlm_params->ioptr) {
            WLMCNUpdateIoInfoState(g_wlm_params->ioptr, WLM_ACTION_REMOVED);
            g_wlm_params->ioptr = NULL;
        }

        t_thrd.wlm_cxt.except_ctl->intvalEndTime = 0;

        WLMProcessStatInfoVisualization(t_thrd.wlm_cxt.collect_info);

        pgstat_report_statement_wlm_status();
    }

    if (IS_PGXC_DATANODE) {
        /* qid is invalid? */
        if (IsQidInvalid(&g_wlm_params->qid)) {
            return;
        }

        /* we must set a new status to avoid processing timer */
        t_thrd.wlm_cxt.dn_cpu_detail->status = WLM_STATUS_ABORT;

        if (g_wlm_params->memtrack || g_wlm_params->cpuctrl) {
            WLMCheckPoint(WLM_STATUS_RELEASE);

            /* If memory track enable, we must save the session info. */
            if (g_wlm_params->memtrack) {
                WLMSetSessionInfo();
            }

            /* release the node in the hash */
            WLMReleaseNodeFromHash();
        }

        if (g_wlm_params->iotrack || g_wlm_params->iocontrol) {
            WLMReleaseIoInfoFromHash();
        }

        /* release the qid */
        ReleaseQid(&g_wlm_params->qid);
    }
}

/*
 * function name: WLMHashMatch
 * description  : check if key is match
 * arguments    :
 *                _in_ key1: first key
 *                _in_ key2: second key
 *                _in_ keysize: size of the key
 * return value : 0: match
 *                1: not match
 */
int WLMHashMatch(const void* key1, const void* key2, Size keysize)
{
    return IsQidEqual((const Qid*)key1, (const Qid*)key2) ? 0 : 1;
}

/*
 * function name: GetHashCode
 * description  : generate a hash code by a key
 * arguments    :
 *                _in_ key: key info
 *                _in_ keysize: size of the key
 * return value : unsigned int32 hash code.
 */
uint32 WLMHashCode(const void* key, Size keysize)
{
    return GetHashCode((const Qid*)key);
}

/*
 * function name: WLMSetCollectInfoStatusFinish
 * description  : set collect info to finished on cn or dn
 * arguments    : void
 * return value : void
 */
void WLMSetCollectInfoStatusFinish()
{
    if (IS_PGXC_COORDINATOR || IS_SINGLE_NODE) {
        volatile PgBackendStatus* beentry = t_thrd.shemem_ptr_cxt.MyBEEntry;
        beentry->st_backstat.enqueue = "None";
    }
}

/*
 * function name: WLMSetCollectInfoStatus
 * description  : set collect info on cn or dn
 * arguments    :
 *                _in_ status: query status to set
 * return value : void
 */
void WLMSetCollectInfoStatus(WLMStatusTag status)
{
    if (IS_PGXC_COORDINATOR || (IS_SINGLE_NODE && !StreamThreadAmI())) {
        WLMSetCNCollectInfoStatus(status);
    }

    if (IS_PGXC_DATANODE) {
        WLMGeneralParam* g_wlm_params = &u_sess->wlm_cxt->wlm_params;

        /*
         * The qid is invalid and cpu control is not set
         * and resource track is off, it's nothing to do.
         */
        if (IsQidInvalid(&g_wlm_params->qid) || (g_wlm_params->cpuctrl == 0 && g_wlm_params->memtrack == 0 &&
                                                    g_wlm_params->iotrack == 0 && g_wlm_params->iocontrol == 0)) {
            return;
        }

        WLMSetDNodeInfoStatus(&g_wlm_params->qid, status);
    }
}

/*
 * @Description: initialize user info configure file
 * @IN void
 * @Return void
 * @See also:
 */
void WLMInitializeConfigUserInfo(void)
{
    /* initialize all user info */
    if (!g_instance.wlm_cxt->stat_manager.userinfo) {
        g_instance.wlm_cxt->stat_manager.userinfo = (WLMUserInfo**)palloc0(sizeof(WLMUserInfo*) * GSUSER_ALLNUM);
    }

    /* parse user info configure file */
    if (ParseUserInfoConfigFile() == -1) {
        ereport(WARNING, (errmsg("failed to parse user configure file!")));
        return;
    }
}

/*
 * @Description: get cpu data indicator
 * @IN beentry: backend entry
 * @OUT indicator: cpu data indicator
 * @Return void
 * @See also:
 */
void WLMGetCPUDataIndicator(PgBackendStatus* beentry, WLMDataIndicator<int64>* indicator)
{
    Assert(indicator != NULL);

    Qid qid = {0, 0, 0};

    if (!ENABLE_WORKLOAD_CONTROL || IsQidInvalid(WLMGetQidByBEEntry(beentry, &qid))) {
        return;
    }

    uint32 hashCode = WLMHashCode(&qid, sizeof(Qid));

    (void)LockSessRealTHashPartition(hashCode, LW_SHARED);

    WLMDNodeInfo* info =
        (WLMDNodeInfo*)hash_search(g_instance.wlm_cxt->stat_manager.collect_info_hashtbl, &qid, HASH_FIND, NULL);

    if (info != NULL) {
        indicator->max_value = info->geninfo.maxCpuTime;
        indicator->min_value = info->geninfo.minCpuTime;
        indicator->total_value = info->geninfo.totalCpuTime;
        indicator->skew_percent = GetSkewRatioOfDN(&info->geninfo);
    }

    UnLockSessRealTHashPartition(hashCode);

    return;
}

/*
 * function name: WLMInitializeStatInfo
 * description  : initialize statistics info
 * arguments    :
 *                _in_ hash_ctl: HASHCTRL to creat hash table
 * return value : void
 */
static void WLMIoStatInit(HASHCTL& hash_ctl)
{
    IOHashCtrl* ioctrl = (IOHashCtrl*)MemoryContextAllocZero(
        g_instance.wlm_cxt->workload_manager_mcxt, sizeof(IOHashCtrl) * NUM_IO_STAT_PARTITIONS);
    int flags = HASH_ELEM | HASH_SHRCTX | HASH_FUNCTION | HASH_COMPARE | HASH_ALLOC | HASH_DEALLOC;
    for (int i = 0; i < NUM_IO_STAT_PARTITIONS; ++i) {
        ioctrl[i].hashTable = hash_create("wlm iostat info hash table", WORKLOAD_STAT_HASH_SIZE, &hash_ctl, flags);
        ioctrl[i].lockId = FirstIOStatLock + i;
    }
    g_instance.wlm_cxt->stat_manager.iostat_info_hashtbl = ioctrl;
}

/*
 * function name: WLMInitializeStatInfo
 * description  : initialize statistics info
 * arguments    : void
 * return value : void
 */
void WLMInitializeStatInfo(void)
{
    HASHCTL hash_ctl;
    errno_t rc;

    /* memory size we will use */
    const int size = MBYTES;

    rc = memset_s(&hash_ctl, sizeof(hash_ctl), 0, sizeof(hash_ctl));
    securec_check(rc, "\0", "\0");

    hash_ctl.keysize = sizeof(Qid);
    hash_ctl.hcxt = g_instance.wlm_cxt->workload_manager_mcxt; /* use workload manager memory context */
    hash_ctl.entrysize = sizeof(WLMDNodeInfo);
    hash_ctl.hash = WLMHashCode;
    hash_ctl.match = WLMHashMatch;
    hash_ctl.alloc = WLMAlloc0NoExcept4Hash;
    hash_ctl.dealloc = pfree;
    hash_ctl.num_partitions = NUM_SESSION_REALTIME_PARTITIONS;

    g_instance.wlm_cxt->stat_manager.collect_info_hashtbl = hash_create("wlm collector hash table",
        WORKLOAD_STAT_HASH_SIZE,
        &hash_ctl,
        HASH_ELEM | HASH_SHRCTX | HASH_FUNCTION | HASH_COMPARE | HASH_ALLOC | HASH_DEALLOC | HASH_PARTITION);

    rc = memset_s(&hash_ctl, sizeof(hash_ctl), 0, sizeof(hash_ctl));
    securec_check(rc, "\0", "\0");

    hash_ctl.keysize = sizeof(Qid);
    hash_ctl.hcxt = g_instance.wlm_cxt->workload_manager_mcxt;
    hash_ctl.entrysize = sizeof(WLMStmtDetail);
    hash_ctl.hash = WLMHashCode;
    hash_ctl.match = WLMHashMatch;
    hash_ctl.alloc = WLMAlloc0NoExcept4Hash;
    hash_ctl.dealloc = pfree;
    hash_ctl.num_partitions = NUM_SESSION_HISTORY_PARTITIONS;

    g_instance.wlm_cxt->stat_manager.session_info_hashtbl = hash_create("wlm session info hash table",
        WORKLOAD_STAT_HASH_SIZE,
        &hash_ctl,
        HASH_ELEM | HASH_SHRCTX | HASH_FUNCTION | HASH_COMPARE | HASH_ALLOC | HASH_DEALLOC | HASH_PARTITION);

    rc = memset_s(&hash_ctl, sizeof(hash_ctl), 0, sizeof(hash_ctl));
    securec_check(rc, "\0", "\0");

    hash_ctl.keysize = sizeof(Qid);
    hash_ctl.hcxt = g_instance.wlm_cxt->workload_manager_mcxt;
    hash_ctl.entrysize = sizeof(WLMDNodeIOInfo);
    hash_ctl.hash = WLMHashCode;
    hash_ctl.match = WLMHashMatch;
    hash_ctl.alloc = WLMAlloc0NoExcept4Hash;
    hash_ctl.dealloc = pfree;

    /* creat hash table for wlm iostat info */
    WLMIoStatInit(hash_ctl);

    rc = memset_s(&hash_ctl, sizeof(hash_ctl), 0, sizeof(hash_ctl));
    securec_check_errval(rc, , LOG);

    hash_ctl.keysize = NAMEDATALEN;
    hash_ctl.hcxt = g_instance.wlm_cxt->workload_manager_mcxt; /* use workload manager memory context */
    hash_ctl.entrysize = sizeof(WLMNodeGroupInfo);
    hash_ctl.hash = string_hash;
    hash_ctl.alloc = WLMAlloc0NoExcept4Hash;
    hash_ctl.dealloc = pfree;

    /* creat hash table for node group */
    g_instance.wlm_cxt->stat_manager.node_group_hashtbl = hash_create("node group hash table",
        WORKLOAD_STAT_HASH_SIZE,
        &hash_ctl,
        HASH_ELEM | HASH_SHRCTX | HASH_FUNCTION | HASH_ALLOC | HASH_DEALLOC);

    if (IS_PGXC_COORDINATOR) {
        g_instance.wlm_cxt->stat_manager.max_collectinfo_num =
            u_sess->attr.attr_resource.session_statistics_memory * KBYTES / sizeof(WLMDNodeInfo);
    }

    if (IS_PGXC_DATANODE) { /* We only use 1 MB memory, we must compute the count of the collect info we can save. */
        g_instance.wlm_cxt->stat_manager.max_collectinfo_num =
            u_sess->attr.attr_resource.session_statistics_memory * KBYTES / sizeof(WLMDNRealTimeInfoDetail);
    }

    g_instance.wlm_cxt->stat_manager.max_iostatinfo_num =
        u_sess->attr.attr_resource.session_statistics_memory * KBYTES / sizeof(WLMDNodeInfo);

    (void)pthread_mutex_init(&g_instance.wlm_cxt->stat_manager.statistics_mutex, NULL);

    g_instance.wlm_cxt->stat_manager.sendsig = 0;
    /* initialize user info config file */
    WLMInitializeConfigUserInfo();

    /* If memory of the view doesn't exist, create it, it's max size is 1 MB. */
    if (IS_PGXC_COORDINATOR || IS_SINGLE_NODE) {
        g_instance.wlm_cxt->stat_manager.max_statistics_num = size / sizeof(WLMStatistics);
        g_instance.wlm_cxt->stat_manager.statistics = (WLMStatistics*)palloc0(size);
        g_instance.wlm_cxt->stat_manager.index = 0;
    }

    /* The memory to save the session info can not be out of 10MB. */
    g_instance.wlm_cxt->stat_manager.max_detail_num =
        u_sess->attr.attr_resource.session_history_memory * KBYTES / sizeof(WLMStmtDetail);

    USE_MEMORY_CONTEXT(g_instance.wlm_cxt->workload_manager_mcxt);

    g_instance.wlm_cxt->stat_manager.recvdata =
        (WLMSigReceiveData*)palloc0(SIG_RECVDATA_COUNT * sizeof(WLMSigReceiveData));

    /* config g_instance.wlm_cxt->instance_manager */
    g_instance.wlm_cxt->instance_manager.max_stat_num = 64 * KBYTES / sizeof(WLMInstanceInfo);
    g_instance.wlm_cxt->instance_manager.collect_interval = 10;     /* 10s */
    g_instance.wlm_cxt->instance_manager.persistence_interval = 60; /* 60s */
    g_instance.wlm_cxt->instance_manager.cleanup_interval = 300;    /* 300s */

    rc = memset_s(
        &g_instance.wlm_cxt->instance_manager.device[0], sizeof(struct DiskIOStats), 0, sizeof(struct DiskIOStats));
    securec_check(rc, "\0", "\0");
    rc = memset_s(
        &g_instance.wlm_cxt->instance_manager.device[1], sizeof(struct DiskIOStats), 0, sizeof(struct DiskIOStats));
    securec_check(rc, "\0", "\0");

    g_instance.wlm_cxt->instance_manager.instance_info_hashtbl = hash_create("instance statistics hash table",
        WORKLOAD_STAT_HASH_SIZE,
        &hash_ctl,
        HASH_ELEM | HASH_SHRCTX | HASH_FUNCTION | HASH_ALLOC | HASH_DEALLOC);
    /* instancename */
    rc = strncpy_s(g_instance.wlm_cxt->instance_manager.instancename,
        NAMEDATALEN,
        g_instance.attr.attr_common.PGXCNodeName,
        NAMEDATALEN - 1);
    securec_check(rc, "\0", "\0");

    rc = strncpy_s(g_instance.wlm_cxt->instance_manager.diskname, MAX_DEVICE_LEN, "", MAX_DEVICE_LEN - 1);
    securec_check(rc, "\0", "\0");
}

/*
 * function name: WLMSetStatInfo
 * description  : set statistics info
 * arguments    :
 *                _in_ sqltext: sql string
 * return value : void
 */
void WLMSetStatInfo(const char* sqltext)
{
    /* reset all statistics info */
    ResetAllStatInfo();

    t_thrd.wlm_cxt.collect_info->blockStartTime = GetCurrentTimestamp();
    t_thrd.wlm_cxt.collect_info->status = WLM_STATUS_RESERVE;
    t_thrd.wlm_cxt.collect_info->cginfo.grpinfo =
        gscgroup_get_grpconf(t_thrd.wlm_cxt.thread_node_group, t_thrd.wlm_cxt.collect_info->cginfo.cgroup);

    if (sqltext != NULL) {
        USE_MEMORY_CONTEXT(t_thrd.wlm_cxt.query_resource_track_mcxt);

        if (strlen(sqltext) < 8 * KBYTES) {
            t_thrd.wlm_cxt.collect_info->sdetail.statement = pstrdup(sqltext);
        } else {
            t_thrd.wlm_cxt.collect_info->sdetail.statement = (char*)palloc0(8 * KBYTES);
            errno_t rc = strncpy_s(t_thrd.wlm_cxt.collect_info->sdetail.statement, 8 * KBYTES, sqltext, 8 * KBYTES - 1);
            securec_check_c(rc, "\0", "\0");
        }
    }

    /*
     * The exception threshold is not valid, we will not handle
     * any exception, but if the resource track is on, we still
     * track the resource in using.
     */
    if (!WLMIsExceptValid(t_thrd.wlm_cxt.collect_info->cginfo.grpinfo)) {
        t_thrd.wlm_cxt.collect_info->attribute = WLM_ATTR_INVALID;
    } else {
        errno_t rc = memcpy_s(t_thrd.wlm_cxt.except_ctl->except,
            sizeof(t_thrd.wlm_cxt.except_ctl->except),
            t_thrd.wlm_cxt.collect_info->cginfo.grpinfo->except,
            sizeof(t_thrd.wlm_cxt.collect_info->cginfo.grpinfo->except));
        securec_check(rc, "\0", "\0");
    }

    /* Start to handle resource track, set the status to "pending" firstly. */
    WLMSetCollectInfoStatus(WLM_STATUS_PENDING);
}

/*
 * function name: WLMGetStatistics
 * description  : Called by pgstat_report_statement_wlm_status.
 *                Attempt to get query runtime statistics, include
 *                block start time, execution start time and current
 *                control group.
 * arguments    :
 *                _out_ blockStartTime: block start time
 *                _out_ execStartTime: execution start time
 *                _out_ pStat: statistics to show
 * return value : void
 */
void WLMGetStatistics(TimestampTz* blockStartTime, TimestampTz* execStartTime, WLMStatistics* pStat)
{
    TimestampTz blockEndTime;
    TimestampTz execEndTime;

    int rc = memset_s(pStat, sizeof(WLMStatistics), 0, sizeof(WLMStatistics));
    securec_check(rc, "\0", "\0");

    /* we get the block end time or execution end time according to the query status */
    if (t_thrd.wlm_cxt.collect_info->status == WLM_STATUS_PENDING) {
        blockEndTime = GetCurrentTimestamp();
    } else if (t_thrd.wlm_cxt.collect_info->blockStartTime > 0) {
        blockEndTime = t_thrd.wlm_cxt.collect_info->blockEndTime;
    } else {
        blockEndTime = 0;
    }

    if (t_thrd.wlm_cxt.collect_info->status == WLM_STATUS_RUNNING) {
        execEndTime = GetCurrentTimestamp();
    } else if (t_thrd.wlm_cxt.collect_info->execStartTime > 0) {
        execEndTime = t_thrd.wlm_cxt.collect_info->execEndTime;
    } else {
        execEndTime = 0;
    }

    /* get the block start time and execution start time */
    *blockStartTime = t_thrd.wlm_cxt.collect_info->blockStartTime;
    *execStartTime = t_thrd.wlm_cxt.collect_info->execStartTime;

    /* If the status before "pending" no block time and elapsed time need to set */
    if (t_thrd.wlm_cxt.collect_info->status == WLM_STATUS_RESERVE) {
        pStat->blocktime = 0;
        pStat->elapsedtime = 0;
    } else {
        /* compute the block time and elapsed time, covert the unit to second */
        pStat->blocktime =
            WLMGetTimestampDuration(t_thrd.wlm_cxt.collect_info->blockStartTime, blockEndTime) / MSECS_PER_SEC;
        pStat->elapsedtime =
            WLMGetTimestampDuration(t_thrd.wlm_cxt.collect_info->execStartTime, execEndTime) / MSECS_PER_SEC;
    }

    /* set statistics info */
    pStat->priority = t_thrd.wlm_cxt.qnode.priority;

    pStat->status = GetStatusName(t_thrd.wlm_cxt.collect_info->status);
    pStat->enqueue = GetEnqueueState(t_thrd.wlm_cxt.collect_info);
    pStat->qtype = GetQueryType(t_thrd.wlm_cxt.collect_info);

    WLMGeneralParam* g_wlm_params = &u_sess->wlm_cxt->wlm_params;

    pStat->stmt_mem = (g_wlm_params->memsize == 0 ||
                          ((strcmp(pStat->status, "running") == 0) && (strcmp(pStat->qtype, "Simple") == 0) &&
                              (g_wlm_params->memsize >= MEM_THRESHOLD)))
                          ? 1
                          : g_wlm_params->memsize;  // 1MB if statement_mem is 0
    pStat->act_pts = ((strcmp(pStat->status, "running") == 0) && (strcmp(pStat->qtype, "Simple") == 0) &&
                         (g_wlm_params->memsize >= MEM_THRESHOLD))
                         ? 1
                         : g_wlm_params->rpdata.act_pts;
    pStat->dop_value = g_wlm_params->dopvalue;
    pStat->is_planA = g_wlm_params->use_planA;

    rc = snprintf_s(pStat->groupname, NAMEDATALEN, NAMEDATALEN - 1, "%s", g_wlm_params->ngroup);
    securec_check_ss(rc, "\0", "\0");

    rc = snprintf_s(pStat->srespool, NAMEDATALEN, NAMEDATALEN - 1, "%s", g_wlm_params->rpdata.rpname);
    securec_check_ss(rc, "\0", "\0");

    /* If the control group is not initialized, we will set a invalid group name */
    if (g_instance.wlm_cxt->gscgroup_init_done == 0) {
        rc = snprintf_s(pStat->cgroup, NAMEDATALEN, NAMEDATALEN - 1, "%s", GSCGROUP_INVALID_GROUP);
        securec_check_ss(rc, "\0", "\0");

        return;
    }

    /*
     * We must get the current control group name, if the query
     * is finished, it's OK to use the group of its collect info,
     * but if it's still in running, we have to check whether it
     * has switch to the "TopWD" group, if it happened, we must
     * use it as the current group name.
     */
    if (t_thrd.wlm_cxt.collect_info->status == WLM_STATUS_FINISHED) {
        rc = snprintf_s(pStat->cgroup, NAMEDATALEN, NAMEDATALEN - 1, "%s", t_thrd.wlm_cxt.collect_info->cginfo.cgroup);
        securec_check_ss(rc, "\0", "\0");
    } else if (t_thrd.wlm_cxt.collect_info->attribute != WLM_ATTR_INVALID) {
        GetCurrentCgroup(pStat->cgroup, t_thrd.wlm_cxt.collect_info->cginfo.cgroup, sizeof(pStat->cgroup));
    } else if (pStat->cgroup[0] == '\0') {
        rc = snprintf_s(pStat->cgroup, NAMEDATALEN, NAMEDATALEN - 1, "%s", u_sess->wlm_cxt->control_group);
        securec_check_ss(rc, "\0", "\0");
    }
}

/*
 * function name: WLMGetStatistics
 * description  : a reload version of WLMGetStatistics.
 *                Called by pg_stat_get_wlm_statistics.
 *                Attempt to get the statistics, which is the record
 *                we handled every exception, include finish status and action.
 * arguments    :
 *                _out_ num: the count of the records
 * return value : statistics array
 */
WLMStatistics* WLMGetStatistics(int* num)
{
    WLMStatistics* pStat = NULL;
    WLMContextLock stat_lock(&g_instance.wlm_cxt->stat_manager.statistics_mutex);
    int rc;

    stat_lock.Lock();

    /*
     * We will copy every valid record to a new memory, so that we can
     * read them with no lock. But before that, We have to check if the
     * statistics has been overwritten, if not, we need not change
     * order, otherwise we must change order, make the records
     * one by one from the earliest to the latest.
     */
    if (g_instance.wlm_cxt->stat_manager.overwrite > 0) {
        int len = g_instance.wlm_cxt->stat_manager.max_statistics_num - g_instance.wlm_cxt->stat_manager.index;

        pStat = (WLMStatistics*)palloc0(g_instance.wlm_cxt->stat_manager.max_statistics_num * sizeof(WLMStatistics));

        /* change order */
        rc = memcpy_s(pStat,
            len * sizeof(WLMStatistics),
            g_instance.wlm_cxt->stat_manager.statistics + g_instance.wlm_cxt->stat_manager.index,
            len * sizeof(WLMStatistics));
        securec_check_errval(rc, stat_lock.UnLock(), ERROR);

        /* if the position is first element, no need to change order */
        if (g_instance.wlm_cxt->stat_manager.index > 0) {
            rc = memcpy_s(pStat + len,
                g_instance.wlm_cxt->stat_manager.index * sizeof(WLMStatistics),
                g_instance.wlm_cxt->stat_manager.statistics,
                g_instance.wlm_cxt->stat_manager.index * sizeof(WLMStatistics));
            securec_check_errval(rc, stat_lock.UnLock(), ERROR);
        }

        *num = g_instance.wlm_cxt->stat_manager.max_statistics_num;
    } else {
        /* no records */
        if (g_instance.wlm_cxt->stat_manager.index <= 0) {
            *num = 0;
            return NULL;
        }

        pStat = (WLMStatistics*)palloc0(g_instance.wlm_cxt->stat_manager.index * sizeof(WLMStatistics));

        /* no overwritten, it's OK to copy from position 0 to the current index */
        rc = memcpy_s(pStat,
            g_instance.wlm_cxt->stat_manager.index * sizeof(WLMStatistics),
            g_instance.wlm_cxt->stat_manager.statistics,
            g_instance.wlm_cxt->stat_manager.index * sizeof(WLMStatistics));
        securec_check_errval(rc, stat_lock.UnLock(), ERROR);

        *num = g_instance.wlm_cxt->stat_manager.index;
    }

    stat_lock.UnLock();

    return pStat;
}

/*
 * function name: WLMGetSpillInfo
 * description  : get spill info
 * arguments    :
 *                _out_ spillInfo: spill info string to get
 *                _in_ size: string size
 *                _in_ geninfo: general info
 *                OUT: spillInfo
 * return value : spill info string
 */
char* WLMGetSpillInfo(char* spillInfo, int size, int spillNodeCount)
{
    errno_t rc = memset_s(spillInfo, size, 0, size);
    securec_check(rc, "\0", "\0");

    if (spillNodeCount >= u_sess->pgxc_cxt.NumDataNodes) {
        /* all data nodes have spilled */
        rc = sprintf_s(spillInfo, size, "%s", "All");
    } else if (spillNodeCount > 0) {
        /* get the num of the node which has spilled */
        rc = sprintf_s(spillInfo, size, "%d : %d", spillNodeCount, u_sess->pgxc_cxt.NumDataNodes);
    } else {
        /* no spilled on each data nodes */
        rc = sprintf_s(spillInfo, size, "%s", "None");
    }
    securec_check_ss(rc, "\0", "\0");

    return spillInfo;
}

/*
 * function name: WLMGetWarnInfo
 * description  : get warn info
 * arguments    :
 *                _out_ warnInfo: warn info string to get
 *                _in_ size: string size
 *                _in_ warn_bit: warning bit
 * return value : warning info string
 */
void WLMGetWarnInfo(char* warnInfo, int size, unsigned int warn_bit, int spill_size, int broadcast_size,
    WLMSessionStatistics* session_stat)
{
    errno_t rc = memset_s(warnInfo, size, 0, size);
    securec_check(rc, "\0", "\0");

    int current_size = 0;

    if (warn_bit & (1 << WLM_WARN_SPILL)) {
        rc = sprintf_s(warnInfo + current_size, size - current_size, "Spill file\n");
        securec_check_ss(rc, "\0", "\0");
        current_size += rc;
    }

    if (warn_bit & (1 << WLM_WARN_SPILL_FILE_LARGE)) {
        if (spill_size > WARNING_SPILL_SIZE) {
            rc = sprintf_s(warnInfo + current_size,
                size - current_size,
                "The max spill size is %dMB and exceeds the alarm size %dMB\n",
                spill_size,
                WARNING_SPILL_SIZE);
            securec_check_ss(rc, "\0", "\0");
            current_size += rc;
        } else {
            rc = sprintf_s(warnInfo + current_size,
                size - current_size,
                "The max spill size exceeds the alarm size %dMB\n",
                WARNING_SPILL_SIZE);
            securec_check_ss(rc, "\0", "\0");
            current_size += rc;
        }
    }

    if (warn_bit & (1 << WLM_WARN_BROADCAST_LARGE)) {
        if (broadcast_size > WARNING_BROADCAST_SIZE) {
            rc = sprintf_s(warnInfo + current_size,
                size - current_size,
                "The max broadcast size is %dMB and exceeds the alarm size %dMB\n",
                broadcast_size,
                WARNING_BROADCAST_SIZE);
            securec_check_ss(rc, "\0", "\0");
            current_size += rc;
        } else {
            rc = sprintf_s(warnInfo + current_size,
                size - current_size,
                "The max broadcast size exceeds the alarm size %dMB\n",
                WARNING_BROADCAST_SIZE);
            securec_check_ss(rc, "\0", "\0");
            current_size += rc;
        }
    }

    if (warn_bit & (1 << WLM_WARN_EARLY_SPILL)) {
        rc = sprintf_s(warnInfo + current_size, size - current_size, "Early spill\n");
        securec_check_ss(rc, "\0", "\0");
        current_size += rc;
    }

    if (warn_bit & (1 << WLM_WARN_SPILL_TIMES_LARGE)) {
        rc = sprintf_s(warnInfo + current_size, size - current_size, "Spill times is greater than 3\n");
        securec_check_ss(rc, "\0", "\0");
        current_size += rc;
    }

    if (warn_bit & (1 << WLM_WARN_SPILL_ON_MEMORY_SPREAD)) {
        rc = sprintf_s(warnInfo + current_size, size - current_size, "Spill on memory adaptive\n");
        securec_check_ss(rc, "\0", "\0");
        current_size += rc;
    }

    if (warn_bit & (1 << WLM_WARN_HASH_CONFLICT)) {
        rc = sprintf_s(warnInfo + current_size, size - current_size, "Hash table conflict\n");
        securec_check_ss(rc, "\0", "\0");
        current_size += rc;
    }

    if (session_stat != NULL && session_stat->query_plan_issue != NULL) {
        rc = sprintf_s(warnInfo + current_size, size - current_size, "%s", session_stat->query_plan_issue);
        securec_check_ss(rc, "\0", "\0");
        current_size += rc;
    }

    if (current_size > 0) {
        warnInfo[current_size - 1] = '\0';
    }
}

/*
 * function name: WLMTopSQLReady
 * description  : ready for TopSQL query view
 * arguments    :
 *                _in_ queryDesc
 * return value : void
 */
void WLMTopSQLReady(QueryDesc* queryDesc)
{
    if (queryDesc == NULL) {
        return;
    }

    if (t_thrd.wlm_cxt.parctl_state.subquery) {
        return;
    }

    if (((IS_PGXC_COORDINATOR && !IsConnFromCoord()) || IS_SINGLE_NODE) && u_sess->attr.attr_resource.enable_resource_track) {
        if (queryDesc->sourceText) {
            USE_MEMORY_CONTEXT(t_thrd.wlm_cxt.query_resource_track_mcxt);

            if (t_thrd.wlm_cxt.collect_info->sdetail.statement != NULL) {
                pfree_ext(t_thrd.wlm_cxt.collect_info->sdetail.statement);
            }
            if (strlen(queryDesc->sourceText) < 8 * KBYTES) {
                t_thrd.wlm_cxt.collect_info->sdetail.statement = pstrdup(queryDesc->sourceText);
            } else {
                t_thrd.wlm_cxt.collect_info->sdetail.statement = (char*)palloc0(8 * KBYTES);
                errno_t rc = strncpy_s(
                    t_thrd.wlm_cxt.collect_info->sdetail.statement, 8 * KBYTES, queryDesc->sourceText, 8 * KBYTES - 1);
                securec_check_c(rc, "\0", "\0");
            }
        }
        /* Check if need track resource */
        u_sess->exec_cxt.need_track_resource = WLMNeedTrackResource(queryDesc);
    }
}

/*
 * function name: WLMInitQueryPlan
 * description  : init query plan for wlm query view
 * arguments    :
 *                _in_ queryDesc
 * return value : void
 */
void WLMInitQueryPlan(QueryDesc* queryDesc, bool isQueryDesc)
{
    /* If queryDesc is NULL, just return and
       this function will be called again after CreateQueryDesc() like in ProcessQuery or ExplainOnePlan etc. */
    if (queryDesc == NULL) {
        return;
    }
#ifdef ENABLE_MULTIPLE_NODES
    if (IS_PGXC_COORDINATOR && !IsConnFromCoord() &&
#else
    if (!StreamThreadAmI() &&
#endif
        u_sess->attr.attr_resource.enable_resource_track &&
        u_sess->exec_cxt.need_track_resource && t_thrd.shemem_ptr_cxt.mySessionMemoryEntry != NULL &&
        t_thrd.shemem_ptr_cxt.mySessionMemoryEntry->query_plan == NULL && isQueryDesc) {

        MemoryContext TmpQueryContext = AllocSetContextCreate(CurrentMemoryContext,
            "TmpQueryContext",
            ALLOCSET_DEFAULT_MINSIZE,
            ALLOCSET_DEFAULT_INITSIZE,
            ALLOCSET_DEFAULT_MAXSIZE);

        MemoryContext oldcontext = MemoryContextSwitchTo(TmpQueryContext);

        /* Analyze plan-not-shipping, statistic not collect issues */
        if (u_sess->attr.attr_resource.resource_track_level > RESOURCE_TRACK_NONE) {
            List* issueResults = PlanAnalyzerQuery((QueryDesc*)queryDesc);

            if (issueResults != NIL) {
                RecordQueryPlanIssues(issueResults);
            }
        }

        ExplainOneQueryForStatistics(queryDesc); /* execute after ExecutorStart() */
        (void)MemoryContextSwitchTo(oldcontext);
        MemoryContextDelete(TmpQueryContext);
        TmpQueryContext = NULL;
    }

    if (queryDesc != NULL && t_thrd.shemem_ptr_cxt.mySessionMemoryEntry != NULL) {
        double total_cost = WLMGetTotalCost(queryDesc, isQueryDesc);
        const int cost_rate = 200;
        const int kb_per_mb = 1024;
        t_thrd.shemem_ptr_cxt.mySessionMemoryEntry->estimate_time =
            (int64)((total_cost / cost_rate < INT64_MAX) ? (total_cost / cost_rate) : INT64_MAX);
        if (u_sess->wlm_cxt->local_foreign_respool == NULL) {
            t_thrd.shemem_ptr_cxt.mySessionMemoryEntry->estimate_memory =
                WLMGetQueryMem(queryDesc, isQueryDesc) / kb_per_mb;
        } else {
            t_thrd.shemem_ptr_cxt.mySessionMemoryEntry->estimate_memory =
                WLMGetQueryMemDN(queryDesc, isQueryDesc) / 1024;
        }
    }
}

void WLMFillGeneralDataSingleNode(WLMGeneralData* gendata, WLMDNodeInfo* nodeInfo)
{
    int queryMemInChunks, peakChunksQuery;
    const int TOP5 = 5;
    int64 dnTime;
    int64 totalCpuTime;
    int peak_iops = 0;
    int curr_iops = 0;
    SessionLevelMemory* sessionMemory = (SessionLevelMemory*)nodeInfo->mementry;

    if (!IsQidInvalid(&nodeInfo->qid)) {
        uint32 bucketId = GetIoStatBucket(&nodeInfo->qid);
        int lockId = GetIoStatLockId(bucketId);
        (void)LWLockAcquire(GetMainLWLockByIndex(lockId), LW_SHARED);
    
        /* get history peak read/write Bps/iops and current total bytes/times of read/write from hash table */
        WLMDNodeIOInfo* ioinfo = (WLMDNodeIOInfo*)hash_search(
            G_WLM_IOSTAT_HASH(bucketId), &nodeInfo->qid, HASH_FIND, NULL);
    
        if (ioinfo != NULL) {
            peak_iops = ioinfo->io_geninfo.peak_iops;
            curr_iops = ioinfo->io_geninfo.curr_iops;
        }
        LWLockRelease(GetMainLWLockByIndex(lockId));
    }
    
    WLMInitMinValue4SessionInfo(gendata);
    gendata->spillNodeCount = 1;
    gendata->nodeCount = 1;
    queryMemInChunks =sessionMemory->queryMemInChunks << (chunkSizeInBits - BITS_IN_MB);
    peakChunksQuery = sessionMemory->peakChunksQuery << (chunkSizeInBits - BITS_IN_MB);
    totalCpuTime = nodeInfo->geninfo.totalCpuTime;
    dnTime = (sessionMemory->dnEndTime > 0) ? sessionMemory->dnEndTime : GetCurrentTimestamp();
    dnTime = WLMGetTimestampDuration(sessionMemory->dnStartTime, dnTime);

    GetMaxAndMinSessionInfo(gendata, peakMemoryData, peakChunksQuery);
    GetMaxAndMinSessionInfo(gendata, usedMemoryData, queryMemInChunks);
    GetMaxAndMinSessionInfo(gendata, spillData, sessionMemory->spillSize);
    GetMaxAndMinSessionInfo(gendata, broadcastData, (int)sessionMemory->broadcastSize);
    GetMaxAndMinSessionInfo(gendata, cpuData, totalCpuTime);
    GetMaxAndMinSessionInfo(gendata, dnTimeData, dnTime);
    GetMaxAndMinSessionInfo(gendata, peakIopsData, peak_iops);
    GetMaxAndMinSessionInfo(gendata, currIopsData, curr_iops);
    setTopNWLMSessionInfo(gendata, queryMemInChunks, totalCpuTime, g_instance.attr.attr_common.PGXCNodeName, TOP5);
}

static bool WLMCheckSessionStatisticsAllowed()
{
    if (! (IS_PGXC_COORDINATOR || IS_SINGLE_NODE)) {
        ereport(WARNING, (errmsg("This view is not allowed on datanode.")));
        return false;
    }

    /* check workload manager is valid */
    if (!ENABLE_WORKLOAD_CONTROL) {
        ereport(WARNING, (errmsg("workload manager is not valid.")));
        return false;
    }

    /* disable resource track function, nothing to fetch */
    if (!u_sess->attr.attr_resource.enable_resource_track) {
        ereport(WARNING, (errmsg("enable_resource_track is not valid.")));
        return false;
    }

    return true;
}


/*
 * function name: WLMGetSessionStatistics
 * description  : get session statistics
 * arguments    :
 *                _out_ num: the count of the records
 * return value : session statistics array
 */
void* WLMGetSessionStatistics(int* num)
{
    int i = 0;

    WLMDNodeInfo* pDNodeInfo = NULL;

    HASH_SEQ_STATUS hash_seq;

    if (!WLMCheckSessionStatisticsAllowed()) {
        return NULL;
    }

    int j;

    for (j = 0; j < NUM_SESSION_REALTIME_PARTITIONS; j++) {
        LWLockAcquire(GetMainLWLockByIndex(FirstSessionRealTLock + j), LW_SHARED);
    }

    /* get current session info count, we will do nothing if it's 0 */
    if ((*num = hash_get_num_entries(g_instance.wlm_cxt->stat_manager.collect_info_hashtbl)) == 0) {
        for (j = NUM_SESSION_REALTIME_PARTITIONS; --j >= 0;) {
            LWLockRelease(GetMainLWLockByIndex(FirstSessionRealTLock + j));
        }
        return NULL;
    }

    WLMSessionStatistics* stat_element = NULL;
    WLMSessionStatistics* stat_array = (WLMSessionStatistics*)palloc0(*num * sizeof(WLMSessionStatistics));

    hash_seq_init(&hash_seq, g_instance.wlm_cxt->stat_manager.collect_info_hashtbl);

    bool is_super_user = superuser();
    Oid cur_user_id = GetUserId();

    /* Get all real time session statistics from the register info hash table. */
    while ((pDNodeInfo = (WLMDNodeInfo*)hash_seq_search(&hash_seq)) != NULL) {
        if (pDNodeInfo->tag == WLM_ACTION_REMAIN && (is_super_user || pDNodeInfo->userid == cur_user_id) &&
            pDNodeInfo->restrack == 1) {
            stat_element = stat_array + i;

            stat_element->qid = pDNodeInfo->qid;
            stat_element->tid = pDNodeInfo->tid;

            SessionLevelMemory* entry = (SessionLevelMemory*)pDNodeInfo->mementry;

            /* Mask password in query string */
            char* queryMaskedPassWd = NULL;
            if (pDNodeInfo->statement != NULL) {
                queryMaskedPassWd = maskPassword(pDNodeInfo->statement);
            }
            if (queryMaskedPassWd == NULL) {
                queryMaskedPassWd = pDNodeInfo->statement == NULL ? (char*)"" : pDNodeInfo->statement;
            }
            stat_element->statement = queryMaskedPassWd;
            stat_element->query_plan = (entry->query_plan == NULL) ? (char*)"NoPlan" : pstrdup(entry->query_plan);
            stat_element->query_plan_issue =
                (entry->query_plan_issue == NULL) ? (char*)"" : pstrdup(entry->query_plan_issue);
            stat_element->query_band = pstrdup(pDNodeInfo->qband);
            stat_element->block_time = pDNodeInfo->block_time;
            stat_element->start_time = pDNodeInfo->start_time;
            stat_element->schname = pstrdup(pDNodeInfo->schname);
            stat_element->estimate_time = entry->estimate_time;
            stat_element->estimate_memory = entry->estimate_memory;
            stat_element->duration =
                (GetCurrentTimestamp() - pDNodeInfo->start_time) / USECS_PER_MSEC; /* not include block time */

            errno_t errval = strncpy_s(stat_element->cgroup,
                sizeof(stat_element->cgroup),
                pDNodeInfo->cgroup,
                sizeof(stat_element->cgroup) - 1);
            securec_check_errval(errval, , LOG);
            errval = strncpy_s(stat_element->nodegroup,
                sizeof(stat_element->nodegroup),
                pDNodeInfo->nodegroup,
                sizeof(stat_element->nodegroup) - 1);
            securec_check_errval(errval, , LOG);
#ifndef ENABLE_MULTIPLE_NODES
            WLMFillGeneralDataSingleNode(&(stat_element->gendata), pDNodeInfo);
#endif
            ++i;
        }
    }

    for (j = NUM_SESSION_REALTIME_PARTITIONS; --j >= 0;) {
        LWLockRelease(GetMainLWLockByIndex(FirstSessionRealTLock + j));
    }

    *num = i;
#ifdef ENABLE_MULTIPLE_NODES

    char keystr[NAMEDATALEN] = {0};
    int retry_count = 0;

retry:
    for (i = 0; i < *num; ++i) {
        stat_element = stat_array + i;

        errno_t ssval = snprintf_s(keystr,
            sizeof(keystr),
            sizeof(keystr) - 1,
            "%u,%lu,%ld,%d",
            stat_element->qid.procId,
            stat_element->qid.queryId,
            stat_element->qid.stamp,
            WLM_ACTION_REMAIN);
        securec_check_ssval(ssval, , LOG);

        /* Get real time info from each data nodes */
        int ret = WLMGetSessionLevelInfoInternalByNG(stat_element->nodegroup, keystr, &stat_element->gendata);
        if (ret != 0) {
            WLMDoSleepAndRetry(&retry_count);
            goto retry;
        }

        /* get spill info string */
        WLMGetSpillInfo(stat_element->spillInfo, sizeof(stat_element->spillInfo), stat_element->gendata.spillNodeCount);
    }
#endif
    return stat_array;
}

/*
 * function name: WLMParseThreadInfo
 * description  : parse messages to suminfo
 * arguments    : msg: messages fetched from DN
                : suminfo: the messages parsed will be stored into suminfo
                : size:  size of structure suminfo
 * return value : void
 */
void WLMParseThreadInfo(StringInfo msg, void* suminfo, int size)
{
    WLMCollectInfoDetail* details = (WLMCollectInfoDetail*)suminfo;
    WLMCollectInfoDetail* detail = details + details->nodeCount;
    details->nodeCount++;

    detail->usedMemory = (int)pq_getmsgint(msg, 4);
    detail->peakMemory = (int)pq_getmsgint(msg, 4);
    detail->spillCount = (int)pq_getmsgint(msg, 4);
    detail->space = pq_getmsgint64(msg);
    detail->dnTime = pq_getmsgint64(msg);
    detail->spillSize = pq_getmsgint64(msg);
    detail->broadcastSize = pq_getmsgint64(msg);
    detail->cpuTime = pq_getmsgint64(msg);
    detail->warning = (int)pq_getmsgint(msg, 4);
    detail->peak_iops = (int)pq_getmsgint(msg, 4);
    detail->curr_iops = (int)pq_getmsgint(msg, 4);

    errno_t errval =
        strncpy_s(detail->nodeName, sizeof(detail->nodeName), pq_getmsgstring(msg), sizeof(detail->nodeName) - 1);
    securec_check_errval(errval, , LOG);
}

/*
 * @Description: get IO statistics
 * @OUT        : num: number of elements in iostat_info_hashtbl
 * @Return:      io statistics info
 * @See also:
 */
WLMIoStatisticsList* WLMGetIOStatisticsGeneral()
{
    const int maxHashCount = 1000;
    /* check workload manager is valid */
    if (!ENABLE_WORKLOAD_CONTROL) {
        ereport(WARNING, (errmsg("workload manager is not valid.")));
        return NULL;
    }

    /* disable resource track function, nothing to fetch */
    if (!u_sess->attr.attr_resource.enable_resource_track) {
        return NULL;
    }

    WLMIoStatisticsList results;
    results.next = NULL;
    HASH_SEQ_STATUS hash_seq;
    WLMDNodeIOInfo* pDNodeIoInfo = NULL;
    for (uint32 bucketId = 0; bucketId < NUM_IO_STAT_PARTITIONS; ++bucketId) {
        int lockId = GetIoStatLockId(bucketId);
        (void)LWLockAcquire(GetMainLWLockByIndex(lockId), LW_SHARED);
        long count = hash_get_num_entries(G_WLM_IOSTAT_HASH(bucketId));
        if (count <= 0) {
            (void)LWLockRelease(GetMainLWLockByIndex(lockId));
            continue;
        }
        if (count >= maxHashCount) {
             ereport(LOG, (errcode(ERRCODE_LOG),
                           errmsg("[WLM IOStatistics]hash count:%ld, bucket id :%u", count, bucketId)));
        }
        hash_seq_init(&hash_seq, G_WLM_IOSTAT_HASH(bucketId));
        /* Get all real time session statistics from the register info hash table. */
        while ((pDNodeIoInfo = (WLMDNodeIOInfo*)hash_seq_search(&hash_seq)) != NULL) {
            if (pDNodeIoInfo->tag == WLM_ACTION_REMAIN) {
                WLMIoStatisticsList* stat_node = (WLMIoStatisticsList*)palloc0(sizeof(WLMIoStatisticsList));
                WLMIoStatisticsGenenral* stat_element =
                    (WLMIoStatisticsGenenral*)palloc0(sizeof(WLMIoStatisticsGenenral));

                stat_element->tid = pDNodeIoInfo->tid;
                stat_element->maxcurr_iops = pDNodeIoInfo->io_geninfo.currIopsData.max_value;
                stat_element->mincurr_iops = pDNodeIoInfo->io_geninfo.currIopsData.min_value;
                stat_element->maxpeak_iops = pDNodeIoInfo->io_geninfo.peakIopsData.max_value;
                stat_element->minpeak_iops = pDNodeIoInfo->io_geninfo.peakIopsData.min_value;

                stat_element->iops_limits = pDNodeIoInfo->iops_limits;
                stat_element->io_priority = pDNodeIoInfo->io_priority;
                stat_element->curr_iops_limit = pDNodeIoInfo->io_geninfo.curr_iops_limit;

                stat_node->node = stat_element;
                stat_node->next = results.next;
                results.next = stat_node;
            }
        }
        LWLockRelease(GetMainLWLockByIndex(lockId));
    }

    return results.next;
}

/*
 * function name: WLMGetProgPath
 * description  : get the data path to write session info.
 * arguments    :
 *                _in_ argv0: binary name
 * return value : -1: failed to get program path
 *                0:  get program path success
 */
int WLMGetProgPath(const char* argv0)
{
    char gaussdb_bin_path[STAT_PATH_LEN] = {0};
    int rc;
    char real_exec_path[PATH_MAX + 1] = {'\0'};

    char* exec_path = gs_getenv_r("GAUSSHOME");
    if (exec_path == NULL) {
        /* find the binary path with binary name */
        if (find_my_exec(argv0, gaussdb_bin_path) < 0) {
            ereport(LOG,
                (errmsg("%s: could not locate "
                        "my own executable path",
                    argv0)));
            return -1;
        }

        exec_path = gaussdb_bin_path;
        get_parent_directory(exec_path);
    }
    Assert(exec_path != NULL);

    if (realpath(exec_path, real_exec_path) == NULL) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("Incorrect backend environment variable $GAUSSHOME"),
                errdetail("Please refer to the backend instance log for the detail")));
    }
    if (backend_env_valid(real_exec_path, "GAUSSHOME") == false) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            errmsg("Incorrect backend environment variable $GAUSSHOME"),
            errdetail("Please refer to the backend instance log for the detail")));
    }
    check_backend_env(real_exec_path);
    rc = snprintf_s(g_instance.wlm_cxt->stat_manager.execdir,
        sizeof(g_instance.wlm_cxt->stat_manager.execdir),
        sizeof(g_instance.wlm_cxt->stat_manager.execdir) - 1,
        "%s",
        real_exec_path);
    securec_check_ss(rc, "\0", "\0");

    /* If it's absolute path, it will use it as log directory */
    if (is_absolute_path(u_sess->attr.attr_common.Log_directory)) {
        rc = snprintf_s(g_instance.wlm_cxt->stat_manager.datadir,
            sizeof(g_instance.wlm_cxt->stat_manager.datadir),
            sizeof(g_instance.wlm_cxt->stat_manager.datadir) - 1,
            "%s",
            u_sess->attr.attr_common.Log_directory);
    } else {
        rc = snprintf_s(g_instance.wlm_cxt->stat_manager.datadir,
            sizeof(g_instance.wlm_cxt->stat_manager.datadir),
            sizeof(g_instance.wlm_cxt->stat_manager.datadir) - 1,
            "%s/pg_log",
            g_instance.attr.attr_common.data_directory);
    }

    securec_check_ss(rc, "\0", "\0");

    return 0;
}

/*
 * function name: WLMStartToGetStatistics
 * description  : start to dump the statistics from the database
 * arguments    : void
 * return value : void
 */
void WLMStartToGetStatistics(void)
{
    char sql[WLM_RECORD_SQL_LEN] = {0};

    int query_statistics_num = hash_get_num_entries(g_instance.wlm_cxt->stat_manager.session_info_hashtbl);
    int operator_statistics_num = hash_get_num_entries(g_operator_table.collected_info_hashtbl);

    /* no records, nothing to do. */
    if (!u_sess->attr.attr_resource.enable_resource_track ||
        (query_statistics_num <= 0 && operator_statistics_num <= 0)) {
        return;
    }

    errno_t rc = EOK;
    int current_size = 0;
    if (query_statistics_num) {
        rc = sprintf_s(sql,
            WLM_RECORD_SQL_LEN,
            "select pg_catalog.create_wlm_session_info(%d);",
            u_sess->attr.attr_resource.enable_resource_record ? 1 : 0);
        securec_check_ss(rc, "\0", "\0");
        current_size += rc;
    }
    if (operator_statistics_num) {
        rc = sprintf_s(sql + current_size,
            WLM_RECORD_SQL_LEN - current_size,
            "select pg_catalog.create_wlm_operator_info(%d);",
            u_sess->attr.attr_resource.enable_resource_record ? 1 : 0);
        securec_check_ss(rc, "\0", "\0");
    }

    /*
     * We will start a connection to cn self to
     * get session info of the finished query. The
     * script will move the info from the memory to
     * a user table.
     */
    WLMRemoteNodeExecuteSql(sql);
}

/*
+ * function name: WLMCleanTopSqlInfo
+ * description  : delete records from info
+ * arguments    : void
+ * return value : void
+ */
void WLMCleanTopSqlInfo(void)
{
    if (!u_sess->attr.attr_resource.enable_resource_track || !u_sess->attr.attr_resource.enable_resource_record ||
        (u_sess->attr.attr_resource.topsql_retention_time == 0)) {
        return;
    }

    errno_t rc = EOK;
    char cleanSessionSql[WLM_CLEAN_SQL_LEN] = {0};
    char cleanOperatorSql[WLM_CLEAN_SQL_LEN] = {0};
    time_t expiredTime;
    struct tm expiredTm;
    char expiredTimeStr[30] = {0};
    time_t timeNow;

    timeNow = time(NULL);
    if (timeNow > u_sess->attr.attr_resource.topsql_retention_time * SECS_PER_DAY) {
        expiredTime = timeNow - u_sess->attr.attr_resource.topsql_retention_time * SECS_PER_DAY;
        if (localtime_r((const time_t*)&expiredTime, &expiredTm) == NULL) {
            ereport(LOG, (errmsg("failed to expire info record.\n")));
            return;
        }
    } else {
        ereport(LOG,
            (errmsg("The current system time minus the aging time is negative,current system time is %ld\n", timeNow)));
        return;
    }
    (void)strftime(expiredTimeStr, 30, "%Y-%m-%d_%H%M%S", &expiredTm);
    rc = sprintf_s(cleanOperatorSql,
        WLM_CLEAN_SQL_LEN,
        "delete from gs_wlm_operator_info where queryid in (select distinct queryid from gs_wlm_operator_info where "
        "start_time < '%s');",
        expiredTimeStr);
    securec_check_ss(rc, "\0", "\0");
    WLMRemoteNodeExecuteSql(cleanOperatorSql);
    rc = sprintf_s(cleanSessionSql,
        WLM_CLEAN_SQL_LEN,
        "delete from gs_wlm_session_query_info_all where finish_time < '%s';",
        expiredTimeStr);
    securec_check_ss(rc, "\0", "\0");
    WLMRemoteNodeExecuteSql(cleanSessionSql);
}

/*
 * function name: WLMHandleAlarm
 * description  : handle sig alarm the wlm worker thread
 * arguments    : SIGNAL NO
 * return value : void
 */
void WLMHandleAlarm(SIGNAL_ARGS)
{
    t_thrd.wlm_cxt.wlmalarm_dump_active = true;

    TimestampTz now = GetCurrentTimestamp();
    TimestampTz end = TimestampTzPlusMilliseconds(now, session_info_collect_timer * MSECS_PER_SEC);

    /* reset the timer for the next time */
    (void)WLMSetTimer(now, end);
}

/*
 * function name: WLMSigHupHandler
 * description  : handle SIGHUP signal
 * arguments    : SIGNAL NO
 * return value : void
 */
void WLMSigHupHandler(SIGNAL_ARGS)
{
    t_thrd.wlm_cxt.wlm_got_sighup = true;
}

/*
 * function name: WLMProcessWorkloadManager
 * description  : process workload manager
 * arguments    : void
 * return value : 1: success and set timer
 *                0: success and not set timer
 *                -1: failed and not set timer
 */
int WLMProcessWorkloadManager(void)
{
    int ret = 0;

    if (IS_PGXC_COORDINATOR || IS_SINGLE_NODE) {
        /* If the query is not in running, noting to handle */
        if (t_thrd.wlm_cxt.collect_info->attribute == WLM_ATTR_INVALID ||
            t_thrd.wlm_cxt.collect_info->status != WLM_STATUS_RUNNING) {
            return ret;
        }

        pgstat_report_statement_wlm_status();

        t_thrd.wlm_cxt.collect_info->execEndTime = GetCurrentTimestamp();

        /* start to handle exception data */
        ret = WLMCNStatInfoCollector(t_thrd.wlm_cxt.collect_info);
    }

    if (IS_PGXC_DATANODE) {
        if (IsQidInvalid(&u_sess->wlm_cxt->wlm_params.qid) ||
            (u_sess->wlm_cxt->wlm_params.cpuctrl == 0 && u_sess->wlm_cxt->wlm_params.memtrack == 0)) {
            return ret;
        }

        /* While it's in running, we will compute the total cpu time and set timer */
        if (t_thrd.wlm_cxt.dn_cpu_detail->status == WLM_STATUS_RUNNING) {
            TimestampTz now;

            WLMCheckPoint(WLM_STATUS_RUNNING);

            now = GetCurrentTimestamp();
            t_thrd.wlm_cxt.except_ctl->intvalEndTime =
                TimestampTzPlusMilliseconds(now, u_sess->attr.attr_resource.cpu_collect_timer * MSECS_PER_SEC);
            (void)WLMSetTimer(now, t_thrd.wlm_cxt.except_ctl->intvalEndTime);
            return 1;
        }
    }

    return ret;
}

/*
 * function name: WLMUpdateCgroupCPUInfo
 * description  : update cgroup cpu uasage info
 * arguments    : void
 * return value : void
 */
void WLMUpdateCgroupCPUInfo(void)
{
    if (g_instance.wlm_cxt->gscgroup_init_done <= 0) {
        return;
    }

    gscgroup_update_hashtbl_cpuinfo(&g_instance.wlm_cxt->MyDefaultNodeGroup);

    WLMNodeGroupInfo* ng = NULL;

    USE_AUTO_LWLOCK(WorkloadNodeGroupLock, LW_SHARED);

    HASH_SEQ_STATUS hash_seq;
    hash_seq_init(&hash_seq, g_instance.wlm_cxt->stat_manager.node_group_hashtbl);

    while ((ng = (WLMNodeGroupInfo*)hash_seq_search(&hash_seq)) != NULL) {
        /* skip unused node group and elastic group */
        if (!ng->used || NULL == ng->cgroups_htab || pg_strcasecmp(VNG_OPTION_ELASTIC_GROUP, ng->group_name) == 0) {
            continue;
        }

        /* update cpu info in the group hash table */
        gscgroup_update_hashtbl_cpuinfo(ng);
    }
}

/*
 * function name: WLMUpdateMemoryInfo
 * description  : update memory uasage info
 * arguments    : void
 * return value : void
 */
bool WLMUpdateMemoryInfo(bool need_adjust)
{
#define MEMPROT_ADJUST_SIZE (4 * 1024)  // 4GB

    if (!t_thrd.utils_cxt.gs_mp_inited) {
        return false;
    }

    unsigned long total_vm = 0;
    unsigned long res = 0;
    unsigned long shared = 0;
    unsigned long text = 0;
    unsigned long lib = 0;
    unsigned long data = 0;
    unsigned long dt = 0;
    uint32 cu_size = (uint32)((uint64)(CUCache->GetCurrentMemSize() + MetaCacheGetCurrentUsedSize()) >> BITS_IN_MB);
    int gpu_used = 0;

#ifdef ENABLE_MULTIPLE_NODES
    int gpu_peak = 0;
    /* Get the GPU info */
    if (is_searchserver_api_load()) {
        void* mem_info = get_searchlet_resource_info(&gpu_used, &gpu_peak);
        if (mem_info != NULL) {
            pfree(mem_info);
        }
    }
#endif

    const char* statm_path = "/proc/self/statm";
    FILE* f = fopen(statm_path, "r");
    int pageSize = getpagesize();  // get the size(bytes) for a page
    if (pageSize <= 0) {
        ereport(WARNING,
            (errcode(ERRCODE_WARNING),
                errmsg("error for call 'getpagesize()', the values for "
                       "process_used_memory and other_used_memory are error!")));
    }

    if (f != NULL) {
        if (7 == fscanf_s(f, "%lu %lu %lu %lu %lu %lu %lu\n", &total_vm, &res, &shared, &text, &lib, &data, &dt)) {
            /* page translated to MB */
            total_vm = BYTES_TO_MB((unsigned long)(total_vm * pageSize));
            res = BYTES_TO_MB((unsigned long)(res * pageSize));
            shared = BYTES_TO_MB((unsigned long)(shared * pageSize));
            text = BYTES_TO_MB((unsigned long)(text * pageSize));
        }
        fclose(f);
    }

    if (res == 0) {
        return false;
    }

    int mctx_real_used =
        (unsigned long)(res - shared - text - cu_size - gpu_used + MEMPROT_INIT_SIZE) >> (chunkSizeInBits - BITS_IN_MB);

    int adjust_count = MEMPROT_ADJUST_SIZE >> (chunkSizeInBits - BITS_IN_MB);

    /* if current used value is larger than recorded value, reset it */
    if ((mctx_real_used + adjust_count) < processMemInChunks) {
        if (need_adjust) {
            adjust_count /= 2;  // just adjust 2GB

            (void)pg_atomic_sub_fetch_u32((volatile uint32*)&dynmicTrackedMemChunks, adjust_count);
            (void)pg_atomic_sub_fetch_u32((volatile uint32*)&processMemInChunks, adjust_count);
            ereport(LOG,
                (errmsg("Reset memory counting for real used memory is %d MB "
                        "and counting used memory is %d MB, adjust memory is %d MB.",
                    mctx_real_used,
                    processMemInChunks,
                    adjust_count)));

            return false;
        }
        return true;
    }

    return false;
}

/*
 * function name: WLMWorkerInitialize
 * description  : Initialize wlm worker thread
 * arguments    :
 *                __inout port: port info
 * return value : void
 */
void WLMWorkerInitialize(struct Port* port)
{
    char remote_host[NI_MAXHOST];
    char remote_port[NI_MAXSERV];
    char remote_ps_data[NI_MAXHOST];

    int rc;

    /* Save port etc. for ps status */
    u_sess->proc_cxt.MyProcPort = port;

    /* This flag will remain set until InitPostgres finishes authentication */
    u_sess->ClientAuthInProgress = true; /* limit visibility of log messages */

    /* save process start time */
    port->SessionStartTime = GetCurrentTimestamp();
    t_thrd.proc_cxt.MyStartTime = timestamptz_to_time_t(port->SessionStartTime);

    /* set these to empty in case they are needed before we set them up */
    port->remote_host = "";
    port->remote_port = "";

    /*
     * Initialize libpq and enable reporting of ereport errors to the client.
     * Must do this now because authentication uses libpq to send messages.
     */
    t_thrd.postgres_cxt.whereToSendOutput = DestRemote; /* now safe to ereport to client */

    /*
     * Get the remote host name and port for logging and status display.
     */
    remote_host[0] = '\0';
    remote_port[0] = '\0';

    if (pg_getnameinfo_all(&port->raddr.addr,
            port->raddr.salen,
            remote_host,
            sizeof(remote_host),
            remote_port,
            sizeof(remote_port),
            (u_sess->attr.attr_common.log_hostname ? 0 : NI_NUMERICHOST) | NI_NUMERICSERV) != 0) {
        (void)pg_getnameinfo_all(&port->raddr.addr,
            port->raddr.salen,
            remote_host,
            sizeof(remote_host),
            remote_port,
            sizeof(remote_port),
            NI_NUMERICHOST | NI_NUMERICSERV);
    }

    if (remote_port[0] == '\0') {
        rc = snprintf_s(remote_ps_data, sizeof(remote_ps_data), sizeof(remote_ps_data) - 1, "%s", remote_host);
        securec_check_ss(rc, "\0", "\0");
    } else {
        rc = snprintf_s(
            remote_ps_data, sizeof(remote_ps_data), sizeof(remote_ps_data) - 1, "%s(%s)", remote_host, remote_port);
        securec_check_ss(rc, "\0", "\0");
    }

    port->remote_host = MemoryContextStrdup(SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_CBB), remote_host);
    port->remote_port = MemoryContextStrdup(SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_CBB), remote_port);
}

/*
 * @Description: set user info init
 * @IN status: info status
 * @Return: void
 * @See also:
 */
void WLMSetBuildHashStat(int status)
{
    gs_lock_test_and_set((int32*)&g_instance.wlm_cxt->stat_manager.infoinit, status);
}

/*
 * @Description: check whether query is finished
 * @IN void
 * @Return: true - yes
 *          false - no
 * @See also:
 */
bool WLMIsQueryFinished(void)
{
    if (!t_thrd.wlm_cxt.parctl_state.enqueue || IsQidInvalid(&u_sess->wlm_cxt->wlm_params.qid)) {
        return true;
    }

    if (t_thrd.wlm_cxt.collect_info->status == WLM_STATUS_PENDING && t_thrd.wlm_cxt.parctl_state.central_waiting) {
        return false;
    }

    return true;
}

/*
 * @Description: check whether dump data
 * @IN sql: sql text
 * @Return: true - yes
 *          false - no
 * @See also:
 */
bool WLMIsDumpActive(const char* sql)
{
    if (!u_sess->attr.attr_resource.enable_resource_track) {
        return false;
    }

    char prefix[] = "select create_wlm_session_info";

    if (strncmp(sql, prefix, sizeof(prefix) - 1) == 0) {
        return true;
    }

    return false;
}

/*
 * @Description: check received data for signal
 * @IN void
 * @Return: void
 * @See also:
 */
void WLMCheckSigRecvData(void)
{
    if (!g_instance.wlm_cxt->stat_manager.sendsig) {
        return;
    }

    for (int i = 0; i < SIG_RECVDATA_COUNT; ++i) {
        WLMSigReceiveData* data = g_instance.wlm_cxt->stat_manager.recvdata + i;

        if (data->used == 0) {
            continue;
        }

        if (data->tid == t_thrd.proc_cxt.MyProcPid) {
            if (t_thrd.shemem_ptr_cxt.MyBEEntry) {
                errno_t errval = strncpy_s(t_thrd.shemem_ptr_cxt.MyBEEntry->st_backstat.cgroup,
                    sizeof(t_thrd.shemem_ptr_cxt.MyBEEntry->st_backstat.cgroup),
                    data->cgroup,
                    sizeof(t_thrd.shemem_ptr_cxt.MyBEEntry->st_backstat.cgroup) - 1);
                securec_check_errval(errval, , LOG);
                errval = strncpy_s(u_sess->wlm_cxt->control_group,
                    sizeof(u_sess->wlm_cxt->control_group),
                    data->cgroup,
                    sizeof(u_sess->wlm_cxt->control_group) - 1);
                securec_check_errval(errval, , LOG);

                t_thrd.shemem_ptr_cxt.MyBEEntry->st_backstat.priority = data->priority;
            }

            (void)gs_atomic_add_32((int*)&g_instance.wlm_cxt->stat_manager.sendsig, -1);

            gs_lock_test_and_set(&data->used, 0);
        }
    }
}

/*
 * function name: WLMProcessThreadMain
 * description  : backend thread
 * arguments    : void
 * return value : success or not
 */
int WLMProcessThreadMain(void)
{
    sigjmp_buf local_sigjmp_buf;
    int rc;

    bool backinit = false;
    t_thrd.wlm_cxt.wlm_xact_start = false;

    /* reset t_thrd.proc_cxt.MyProcPid */
    t_thrd.proc_cxt.MyProcPid = gs_thread_self();

    /* record Start Time for logging */
    t_thrd.proc_cxt.MyStartTime = time(NULL);

    t_thrd.proc_cxt.MyProgName = "WLMCollectWorker";

    if (u_sess->proc_cxt.MyProcPort->remote_host) {
        pfree(u_sess->proc_cxt.MyProcPort->remote_host);
    }

    u_sess->proc_cxt.MyProcPort->remote_host = pstrdup("localhost");

    /* Identify myself via ps */
    init_ps_display("wlm collect worker process", "", "", "");

    SetProcessingMode(InitProcessing);

    t_thrd.bootstrap_cxt.MyAuxProcType = WLMWorkerProcess;

    g_instance.wlm_cxt->stat_manager.stop = 0;
    g_instance.wlm_cxt->stat_manager.thread_id = t_thrd.proc_cxt.MyProcPid;

    rc = memset_s(
        &u_sess->wlm_cxt->wlm_params, sizeof(u_sess->wlm_cxt->wlm_params), 0, sizeof(u_sess->wlm_cxt->wlm_params));
    securec_check(rc, "\0", "\0");

    ReserveQid(&u_sess->wlm_cxt->wlm_params.qid);

    u_sess->wlm_cxt->wlm_params.qid.procId = 0;

    rc = snprintf_s(t_thrd.wlm_cxt.collect_info->cginfo.cgroup,
        sizeof(t_thrd.wlm_cxt.collect_info->cginfo.cgroup),
        sizeof(t_thrd.wlm_cxt.collect_info->cginfo.cgroup) - 1,
        "%s",
        GSCGROUP_DEFAULT_BACKEND);
    securec_check_ss(rc, "\0", "\0");

    t_thrd.wlm_cxt.collect_info->sdetail.statement = "WLM fetch collect info from data nodes";

    u_sess->attr.attr_common.application_name = pstrdup("workload");

    t_thrd.wlm_cxt.parctl_state.special = 1;

    if (IS_PGXC_COORDINATOR && IsPostmasterEnvironment) {
        /*
         * If we exit, first try and clean connections and send to
         * pooler thread does NOT exist any more, PoolerLock of LWlock is used instead.
         *
         * PoolManagerDisconnect() which is called by PGXCNodeCleanAndRelease()
         * is the last call to pooler in the postgres thread, and PoolerLock is
         * used in PoolManagerDisconnect(), but it is called after ProcKill()
         * when postgres thread exits.
         * ProcKill() releases any of its held LW locks. So Assert(!(proc == NULL ...))
         * will fail in LWLockAcquire() which is called by PoolManagerDisconnect().
         *
         * All exit functions in "on_shmem_exit_list" will be called before those functions
         * in "on_proc_exit_list", so move PGXCNodeCleanAndRelease() to "on_shmem_exit_list"
         * and registers it after ProcKill(), and PGXCNodeCleanAndRelease() will
         * be called before ProcKill().
         */
        on_shmem_exit(PGXCNodeCleanAndRelease, 0);
    }

    /*
     * Set up signal handlers.  We operate on databases much like a regular
     * backend, so we use the same signal handling.  See equivalent code in
     * tcop/postgres.c.
     *
     * Currently, we don't pay attention to postgresql.conf changes that
     * happen during a single daemon iteration, so we can ignore SIGHUP.
     *
     * SIGINT is used to signal canceling; SIGTERM
     * means abort and exit cleanly, and SIGQUIT means abandon ship.
     */
    (void)gspqsignal(SIGINT, SIG_IGN);
    (void)gspqsignal(SIGTERM, die);
    (void)gspqsignal(SIGQUIT, quickdie);
    (void)gspqsignal(SIGALRM, WLMHandleAlarm);

    (void)gspqsignal(SIGPIPE, SIG_IGN);
    (void)gspqsignal(SIGUSR1, procsignal_sigusr1_handler);
    (void)gspqsignal(SIGUSR2, SIG_IGN);
    (void)gspqsignal(SIGFPE, FloatExceptionHandler);
    (void)gspqsignal(SIGCHLD, SIG_DFL);
    (void)gspqsignal(SIGHUP, WLMSigHupHandler);
    (void)gs_signal_unblock_sigusr2();

    if (IsUnderPostmaster) {
        /* We allow SIGQUIT (quickdie) at all times */
        (void)sigdelset(&t_thrd.libpq_cxt.BlockSig, SIGQUIT);
    }

    /* Early initialization */
    BaseInit();

    gs_signal_setmask(&t_thrd.libpq_cxt.UnBlockSig, NULL);

    WLMInitPostgres();
    SetProcessingMode(NormalProcessing);

    int curTryCounter;
    int *oldTryCounter = NULL;
    if (sigsetjmp(local_sigjmp_buf, 1) != 0) {
        gstrace_tryblock_exit(true, oldTryCounter);

        /* Since not using PG_TRY, we must reset error stack by hand */
        t_thrd.log_cxt.error_context_stack = NULL;

        t_thrd.log_cxt.call_stack = NULL;

        /* Prevents interrupts while cleaning up */
        HOLD_INTERRUPTS();

        /*
         * Forget any pending QueryCancel request, since we're returning to
         * the idle loop anyway, and cancel the statement timer if running.
         */
        t_thrd.int_cxt.QueryCancelPending = false;
        disable_sig_alarm(true);
        t_thrd.int_cxt.QueryCancelPending = false; /* again in case timeout occurred */

        if ((IS_PGXC_COORDINATOR || IS_SINGLE_NODE) && hash_get_seq_num() > 0) {
            release_all_seq_scan();
        }

        /* Report the error to the client and/or server log */
        EmitErrorReport();

        /*
         * Abort the current transaction in order to recover.
         */
        if (t_thrd.wlm_cxt.wlm_xact_start) {
            AbortCurrentTransaction();
            t_thrd.wlm_cxt.wlm_xact_start = false;
        }
        /* release resource held by lsc */
        AtEOXact_SysDBCache(false);

        /*
         *   Notice: at the most time it isn't necessary to call because
         *   all the LWLocks are released in AbortCurrentTransaction().
         *   but in some rare exception not in one transaction (for
         *   example the following InitMultinodeExecutor() calling )
         *   maybe hold LWLocks unused.
         */
        LWLockReleaseAll();

        /*
         * Now return to normal top-level context and clear ErrorContext for
         * next time.
         */
        (void)MemoryContextSwitchTo(THREAD_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_DEFAULT));
        FlushErrorState();

        /* Now we can allow interrupts again */
        RESUME_INTERRUPTS();

        return 0;
    }
    oldTryCounter = gstrace_tryblock_entry(&curTryCounter);

    t_thrd.log_cxt.PG_exception_stack = &local_sigjmp_buf;

    /*
     * Only coordinator can dump the session info, if the
     * enable_resource_track is off, we need not do this.
     */
    if ((IS_PGXC_COORDINATOR || IS_SINGLE_NODE) && WLMGetProgPath("gaussdb") == 0) {
        TimestampTz now = GetCurrentTimestamp();
        TimestampTz end = TimestampTzPlusMilliseconds(now, session_info_collect_timer * MSECS_PER_SEC);

        (void)WLMSetTimer(now, end);
    }

    /* init transaction to execute query */
    WLMInitTransaction(&backinit);

    ereport(LOG, (errmsg("process wlm thread starting up.")));

    /* build user info and resource pool hash table if does not exist */
    if (!g_instance.wlm_cxt->stat_manager.infoinit) {
        if (!BuildUserRPHash()) {
            ereport(LOG, (errmsg("build user data failed")));
        } else {
            WLMSetBuildHashStat(1);
            ereport(LOG, (errmsg("build user data finished")));
        }

        /* get the node group name which the local dn belongs to */
        if (IS_PGXC_DATANODE) {
            char* local_nodegroup = PgxcGroupGetFirstLogicCluster();

            if (local_nodegroup != NULL && *local_nodegroup) {
                rc = snprintf_s(
                    g_instance.wlm_cxt->local_dn_ngname, NAMEDATALEN, NAMEDATALEN - 1, "%s", local_nodegroup);
                securec_check_ss(rc, "\0", "\0");

                pfree(local_nodegroup);
            }

            if (*g_instance.wlm_cxt->local_dn_ngname) {
                g_instance.wlm_cxt->local_dn_nodegroup =
                    WLMMustGetNodeGroupFromHTAB(g_instance.wlm_cxt->local_dn_ngname);

                ereport(LOG,
                    (errmsg("During initialization, the local node group is '%s'.",
                        *g_instance.wlm_cxt->local_dn_ngname ? g_instance.wlm_cxt->local_dn_ngname : "NULL")));
            }

            if (g_instance.wlm_cxt->local_dn_nodegroup && g_instance.wlm_cxt->local_dn_nodegroup->foreignrp &&
                g_instance.wlm_cxt->local_dn_nodegroup->foreignrp->parentoid != InvalidOid) {
                Oid parentoid = g_instance.wlm_cxt->local_dn_nodegroup->foreignrp->parentoid;

                /* find the parent resource pool  */
                ResourcePool* parentrp =
                    (ResourcePool*)hash_search(g_instance.wlm_cxt->resource_pool_hashtbl, &parentoid, HASH_FIND, NULL);
                if (parentrp == NULL) {
                    ereport(LOG,
                        (errmsg("failed to find the parent resource pool "
                                "when initializing  the foreign respool hash table")));
                } else {
                    g_instance.wlm_cxt->local_dn_nodegroup->foreignrp->parentrp = parentrp;
                    parentrp->foreignrp = g_instance.wlm_cxt->local_dn_nodegroup->foreignrp;

                    g_instance.wlm_cxt->local_dn_nodegroup->foreignrp->actpct =
                        g_instance.wlm_cxt->local_dn_nodegroup->foreignrp->mempct * parentrp->mempct / 100;

                    ereport(LOG,
                        (errmsg("The memory percent of foreign resource pool is %d.",
                            g_instance.wlm_cxt->local_dn_nodegroup->foreignrp->actpct)));
                }
            }
        }
    }

    if (IS_PGXC_COORDINATOR) {
        int count = 0;

        while (count < 10) {
            ++count;

            if (g_instance.wlm_cxt->stat_manager.stop) {
                return 0;
            }

            pg_usleep(USECS_PER_SEC);
        }

        /* initialize current pool handles, it's also only once */
        exec_init_poolhandles();

        /*
         * If the PGXC_NODE system table is not prepared, the number of CN / DN
         * can not be obtained, if we can not get the number of DN or CN, that
         * will make the collection module can not complete the task, so the
         * thread need restart
         */
        if (u_sess->pgxc_cxt.NumDataNodes == 0 || u_sess->pgxc_cxt.NumCoords == 0) {
            ereport(ERROR,
                (errcode(ERRCODE_INVALID_TRANSACTION_INITIATION),
                    errmsg("init transaction error, data nodes or coordinators num init failed")));
        }

        /* estimate the memory of one query has ; at least it has 3MB */
        g_instance.wlm_cxt->parctl_process_memory =
            PARCTL_MEMORY_UNIT +
            (int)((u_sess->pgxc_cxt.NumDataNodes + u_sess->pgxc_cxt.NumCoords) * POOL_BUFF_MEMORY / 1024);

        if (g_instance.wlm_cxt->dynamic_workload_inited) {
            USE_CONTEXT_LOCK(&g_instance.wlm_cxt->MyDefaultNodeGroup.climgr.statement_list_mutex);

            g_instance.wlm_cxt->MyDefaultNodeGroup.climgr.max_support_statements =
                t_thrd.utils_cxt.gs_mp_inited ? (MAX_PARCTL_MEMORY * PARCTL_ACTIVE_PERCENT) /
                                                    (FULL_PERCENT * g_instance.wlm_cxt->parctl_process_memory)
                                              : 0;

            g_instance.wlm_cxt->MyDefaultNodeGroup.climgr.max_statements =
                t_thrd.utils_cxt.gs_mp_inited
                    ? (MAX_PARCTL_MEMORY * DYWLM_HIGH_QUOTA / FULL_PERCENT -
                          g_instance.wlm_cxt->MyDefaultNodeGroup.climgr.max_active_statements *
                              g_instance.wlm_cxt->parctl_process_memory) /
                          PARCTL_MEMORY_UNIT
                    : 0;
        } else {
            USE_CONTEXT_LOCK(&g_instance.wlm_cxt->MyDefaultNodeGroup.parctl.statements_list_mutex);

            g_instance.wlm_cxt->MyDefaultNodeGroup.parctl.max_support_statements =
                t_thrd.utils_cxt.gs_mp_inited ? (MAX_PARCTL_MEMORY * PARCTL_ACTIVE_PERCENT) /
                                                    (FULL_PERCENT * g_instance.wlm_cxt->parctl_process_memory)
                                              : 0;

            g_instance.wlm_cxt->MyDefaultNodeGroup.parctl.max_statements =
                t_thrd.utils_cxt.gs_mp_inited
                    ? (MAX_PARCTL_MEMORY * DYWLM_HIGH_QUOTA / FULL_PERCENT -
                          g_instance.wlm_cxt->MyDefaultNodeGroup.parctl.max_active_statements *
                              g_instance.wlm_cxt->parctl_process_memory) /
                          PARCTL_MEMORY_UNIT
                    : 0;
        }
    }

    TimestampTz last_persistent_time = GetCurrentTimestamp();
    TimestampTz last_cleanup_time = GetCurrentTimestamp();
    TimestampTz topsql_clean_last_time = GetCurrentTimestamp();

    /*
     * workload manager is off, we need not use this
     * thread after finishing build hash table */
    if (!ENABLE_WORKLOAD_CONTROL) {
        g_instance.wlm_cxt->stat_manager.stop = 1;
    }

    const int SEVEN_DAYS = 7 * 24 * 60 * 60 * 1000;
    TimestampTz get_thread_status_last_time = GetCurrentTimestamp();
#ifdef ENABLE_MULTIPLE_NODES
    const int ONE_HOURS = 60 * 60;
    TimestampTz cgroupCheckLast = GetCurrentTimestamp();
#endif
    while (g_instance.wlm_cxt->stat_manager.stop == 0) {
        if (!t_thrd.wlm_cxt.wlm_xact_start) {
            StartTransactionCommand();
            t_thrd.wlm_cxt.wlm_xact_start = true;
        }

        if (t_thrd.wlm_cxt.wlm_got_sighup) {
            t_thrd.wlm_cxt.wlm_got_sighup = false;
            ProcessConfigFile(PGC_SIGHUP);
        }

        if (IsGotPoolReload()) {
            processPoolerReload();
            ResetGotPoolReload(false);
        }

        /* timer is triggerred, start to get session info from the database. */
        if (t_thrd.wlm_cxt.wlmalarm_dump_active) {
            WLMStartToGetStatistics();
            t_thrd.wlm_cxt.wlmalarm_dump_active = false;
        }

#ifdef ENABLE_MULTIPLE_NODES
        /* if cgroup not init, retry init it */
        if (!g_instance.wlm_cxt->gscgroup_config_parsed) {
            TimestampTz cgroupCheckNow = GetCurrentTimestamp();
            if (cgroupCheckNow > cgroupCheckLast + ONE_HOURS * USECS_PER_SEC) {
                gscgroup_init();
                cgroupCheckLast = cgroupCheckNow;
            }
        }
#endif

        /* Fetch collect info from each data nodes. */
        WLMCollectInfoScanner();

        if (IS_PGXC_COORDINATOR || IS_SINGLE_NODE) {
            if (u_sess->attr.attr_resource.use_workload_manager &&
                u_sess->attr.attr_resource.enable_user_metric_persistent) {
                TimestampTz now = GetCurrentTimestamp();
                /* interval is 30s, set to 28s, in case mis-fire */
                if (now > last_persistent_time + 28 * USECS_PER_SEC) {
                    /* persistent user resource info */
                    WLMPersistentUserResourceInfo();
                    last_persistent_time = now;
                }
                /* interval is 300s, set to 298, in case mis-fire */
                if (now > last_cleanup_time + 298 * USECS_PER_SEC) {
                    /* cleanup history user resource info */
                    WLMCleanupHistoryUserResourceInfo();
                    last_cleanup_time = now;
                }
            }

            TimestampTz now = GetCurrentTimestamp();
            /* interval is 600s, set to 598, in case mis-fire */
            if (now > topsql_clean_last_time + 598 * USECS_PER_SEC) {
                WLMCleanTopSqlInfo();
                topsql_clean_last_time = now;
            }

            /* Check if the memory is out of control */
            if (backinit && (getSessionMemoryUsageMB() > MAX_MEMORY_THREHOLD)) {
                ereport(ERROR,
                    (errcode(ERRCODE_INVALID_TRANSACTION_INITIATION),
                        errmsg("The memory usage of %s is out of control. it will exit!", t_thrd.proc_cxt.MyProgName)));
            }

            /* check database mode. */
            if (u_sess->attr.attr_common.upgrade_mode != 1) {
                WLMDefaultXactReadOnlyCheckAndHandle();
            }

            pg_usleep(MAX_SLEEP_TIME * USECS_PER_SEC);
        }

        TimestampTz get_thread_status_current_time = GetCurrentTimestamp();
        if (IS_PGXC_DATANODE) {
            if (g_instance.comm_cxt.force_cal_space_info ||
                (TimestampDifferenceExceeds(get_thread_status_last_time, get_thread_status_current_time, SEVEN_DAYS) &&
                    g_instance.attr.attr_resource.enable_perm_space)) {
                get_thread_status_last_time = get_thread_status_current_time;
                g_instance.comm_cxt.force_cal_space_info = false;
                WLMReadjustUserSpaceThroughAllDbs(ALL_USER);
            }
        }

        if (t_thrd.wlm_cxt.wlm_xact_start) {
            CommitTransactionCommand();
            t_thrd.wlm_cxt.wlm_xact_start = false;
        }
    }

    /* If transaction has started, we must commit it here. */
    if (t_thrd.wlm_cxt.wlm_xact_start) {
        CommitTransactionCommand();
        t_thrd.wlm_cxt.wlm_xact_start = false;
    }

    backinit = false;

    ereport(DEBUG1, (errmsg("exit wlm worker thread!")));

    return 0;
}

/*
 * function name: WLMProcessThreadShutDown
 * description  : Called by postmaster at shutting down.
 * arguments    : void
 * return value : void
 */
void WLMProcessThreadShutDown(void)
{
    g_instance.wlm_cxt->stat_manager.stop = 1;
    ereport(LOG, (errmsg("wlm collect worker process shutting down.")));
}

/*
 * @Description: check htab whether has built
 * @IN void
 * @Return: true: has built
 * @Retrun: false: not built
 * @See also:
 */
bool WLMIsInfoInit(void)
{
    return g_instance.wlm_cxt->stat_manager.infoinit > 0;
}

/*
 * function name: WLMGetWorkloadStruct
 * description  : Called by pgstat to get workload structure info of the current cn
 * arguments    : strinfo: string buffer
 * return value : void
 */
void WLMGetWorkloadStruct(StringInfo strinfo)
{
    if (!IS_PGXC_COORDINATOR) {
        return;
    }

    if (g_instance.wlm_cxt->dynamic_workload_inited) { /* dynamic workload manager */
        /* display the info of global parallel control */
        dywlm_client_display_climgr_info(strinfo);

        /* display the info of server if there are running or waiting queries */
        dywlm_server_display_srvmgr_info(strinfo);

        /* display the info of resource pool */
        dywlm_server_display_respool_info(strinfo);
    } else { /* static workload manager */
        ;
    }
}

/*
 * function name: WLMGetDebugInfo
 * description  : Called by pgstat to get workload debug info.
 * arguments    : strinfo: string buffer
 *                debug_info: debug_info in pgstat
 * return value : void
 */
void WLMGetDebugInfo(StringInfo strinfo, WLMDebugInfo* debug_info)
{
    WLMDNodeInfo info;

    errno_t rc = memset_s(&info, sizeof(WLMDNodeInfo), 0, sizeof(WLMDNodeInfo));
    securec_check(rc, "\0", "\0");

    /* we must have wlm params to make debug info */
    if (debug_info->wparams == NULL) {
        return;
    }

    if (g_instance.wlm_cxt->dynamic_workload_inited) {
        /* print client dynamic workload state debug info */
        appendStringInfo(strinfo,
            _("Client DynaWLM: active_statements[%d], central_waiting_count[%d], max_info_count[%d], "
              "max_active_statements[%d], freesize[%d], max_statements[%d]\n"),
            debug_info->climgr->active_statements,
            debug_info->climgr->central_waiting_count,
            debug_info->climgr->max_info_count,
            debug_info->climgr->max_active_statements,
            debug_info->climgr->freesize,
            debug_info->climgr->max_statements);

        appendStringInfo(strinfo,
            _("Client DynaWLM: recover[%s], register count[%ld], waiting list count[%d], used_size[%d], "
              "max_support_statements[%d], current_support_statements[%d]\n"),
            debug_info->climgr->recover ? "YES" : "NO",
            hash_get_num_entries(debug_info->climgr->dynamic_info_hashtbl),
            list_length(debug_info->climgr->statements_waiting_list),
            debug_info->climgr->usedsize,
            debug_info->climgr->max_support_statements,
            debug_info->climgr->current_support_statements);

        /* print server dynamic workload state debug info */
        appendStringInfo(strinfo,
            _("Server DynaWLM: totalsize[%d], freesize[%d], freesize_inc[%d], freesize_update[%d], "
              "freesize_limit[%d]\n"),
            debug_info->srvmgr->totalsize,
            debug_info->srvmgr->freesize,
            debug_info->srvmgr->freesize_inc,
            debug_info->srvmgr->freesize_update,
            debug_info->srvmgr->freesize_limit);

        appendStringInfo(strinfo,
            _("Server DynaWLM: rp_memsize[%d], active_count[%d], statement_quota[%d], recover[%s]\n"),
            debug_info->srvmgr->rp_memsize,
            debug_info->srvmgr->active_count,
            debug_info->srvmgr->statement_quota,
            debug_info->srvmgr->recover ? "YES" : "NO");

        appendStringInfo(strinfo,
            _("Server DynaWLM: global waiting list count[%d], global info hashtbl count[%ld]\n"),
            list_length(debug_info->srvmgr->global_waiting_list),
            hash_get_num_entries(debug_info->srvmgr->global_info_hashtbl));
    } else {
        /* print parctl manager debug info */
        appendStringInfo(strinfo,
            _("ParctlManager: max_active_statements[%d], statements_waiting_count[%d], "
              "statements_runtime_count[%d], active_statement[%d]\n"),
            debug_info->parctl->max_active_statements,
            debug_info->parctl->statements_waiting_count,
            debug_info->parctl->statements_runtime_count,
            debug_info->active_statement);

        /* print parctl manager debug info */
        appendStringInfo(strinfo,
            _("ParctlManager: max_statements[%d], max_support_statements[%d], current_support_statements[%d], "
              "respool_waiting_count[%d]\n"),
            debug_info->parctl->max_statements,
            debug_info->parctl->max_support_statements,
            debug_info->parctl->current_support_statements,
            debug_info->parctl->respool_waiting_count);
    }

    /* use the node info if the node info exists */
    if (!IsQidInvalid(&debug_info->wparams->qid) && debug_info->wparams->ptr) {
        rc = memcpy_s(&info, sizeof(WLMDNodeInfo), debug_info->wparams->ptr, sizeof(WLMDNodeInfo));
        securec_check(rc, "\0", "\0");
    }

    /* print general params debug info */
    appendStringInfo(strinfo,
        _("GeneralParams: qid[%u,%lu,%ld], cgroup[%s], cpuctrl[%d], memtrack[%d], memsize[%d]\n"),
        debug_info->wparams->qid.procId,
        debug_info->wparams->qid.queryId,
        debug_info->wparams->qid.stamp,
        debug_info->wparams->cgroup,
        debug_info->wparams->cpuctrl,
        debug_info->wparams->memtrack,
        debug_info->wparams->memsize);

    /* print general resource pool data debug info */
    appendStringInfo(strinfo,
        _("GeneralParams RPDATA: rpoid[%u], max_pts[%d], mem_size[%d], act_pts[%d], max_dop[%d], cgchange[%s]\n"),
        debug_info->wparams->rpdata.rpoid,
        debug_info->wparams->rpdata.max_pts,
        debug_info->wparams->rpdata.mem_size,
        debug_info->wparams->rpdata.act_pts,
        debug_info->wparams->rpdata.max_dop,
        debug_info->wparams->rpdata.cgchange ? "Yes" : "No");

    /* print statctl manager debug info */
    appendStringInfo(strinfo,
        _("ExceptionManager: max_waiting_time[%d], max_running_time[%d], max_adjust_time[%d], max_interval_time[%d]\n"),
        debug_info->statctl->max_waiting_time,
        debug_info->statctl->max_running_time,
        debug_info->statctl->max_adjust_time,
        debug_info->statctl->max_interval_time);

    /* print collect info on coordinator debug info */
    appendStringInfo(strinfo,
        _("CollectInfo: scan_count[%d], cgroup[%s], attribute[%s], max_mem[%ldKB], avail_mem[%ldKB]\n"),
        debug_info->colinfo->sdetail.geninfo.scanCount,
        debug_info->colinfo->cginfo.cgroup,
        (debug_info->colinfo->attribute == WLM_ATTR_INVALID) ? "Invalid" : "Valid",
        debug_info->colinfo->max_mem,
        debug_info->colinfo->avail_mem);

    /* print general info debug info */
    appendStringInfo(strinfo,
        _("GeneralInfo: totalCpuTime[%ld], maxCpuTime[%ld], minCpuTime[%ld]\n"),
        info.geninfo.totalCpuTime,
        info.geninfo.maxCpuTime,
        info.geninfo.minCpuTime);

    /* print general info debug info */
    appendStringInfo(strinfo,
        _("GeneralInfo: scanCount[%d], skewPct[%d], used[%d]\n"),
        info.geninfo.scanCount,
        GetSkewRatioOfDN(&info.geninfo),
        info.tag);

    /* print parctl state debug info */
    appendStringInfo(strinfo,
        _("ParctlState: global_reserve[%d], rp_reserve[%d], release[%d], rp_release[%d], except[%d], special[%d], "
          "transact[%d]\n"),
        debug_info->pstate->global_reserve,
        debug_info->pstate->rp_reserve,
        debug_info->pstate->release,
        debug_info->pstate->rp_release,
        debug_info->pstate->except,
        debug_info->pstate->special,
        debug_info->pstate->transact);

    /* print parctl state debug info */
    appendStringInfo(strinfo,
        _("ParctlState: simple[%d], enqueue[%d], reserve[%d], transact_begin[%d], in "
          "trasaction[%s], errjmp[%d]\n"),
        debug_info->pstate->simple,
        debug_info->pstate->enqueue,
        debug_info->pstate->reserve,
        debug_info->pstate->transact_begin,
        (debug_info->reserved_in_transaction && *(debug_info->reserved_in_transaction)) ? "Yes" : "No",
        debug_info->pstate->errjmp);

    /* print parctl state debug info */
    appendStringInfo(strinfo,
        _("ParctlState: waiting in global queue[%d], waiting in respool[%d], "
          "waiting in simple global queue[%d], waiting in central queue[%d], error jump flag[%d]\n"),
        debug_info->pstate->global_waiting,
        debug_info->pstate->respool_waiting,
        debug_info->pstate->preglobal_waiting,
        debug_info->pstate->central_waiting,
        debug_info->pstate->errjmp);

    appendStringInfo(strinfo,
        _("CPU INFO: max cpu util[%d], min cpu count[%d], active_counts[%d]\n"),
        dywlm_get_cpu_util(),
        dywlm_get_cpu_count(),
        dywlm_get_active_statement_count());

    if (*debug_info->wparams->ngroup && strcmp(debug_info->wparams->ngroup, DEFAULT_NODE_GROUP)) {
        WLMNodeGroupInfo* ng = WLMGetNodeGroupFromHTAB(debug_info->wparams->ngroup);
        if (ng != NULL && ng->foreignrp) {
            appendStringInfo(strinfo,
                _("Foreign Resource Pool INFO: memsiz[%d], memused[%d], mempct[%d]"),
                ng->foreignrp->memsize,
                ng->foreignrp->memused,
                ng->foreignrp->actpct);
        }
    }
}

/*
 * function name: WLMGetTotalCost
 * description  : get plan total cost from desc
 * arguments    : desc: query/utility desc info
 *                isQueryDesc: is query or utility desc info
 * return value : plan total cost
 */
double WLMGetTotalCost(const QueryDesc* desc, bool isQueryDesc)
{
    if (isQueryDesc != false) {
        if (desc != NULL && desc->plannedstmt && desc->plannedstmt->planTree) {
            return desc->plannedstmt->planTree->total_cost;
        }
    } else if (desc != NULL) {
        const UtilityDesc* result = (UtilityDesc*)desc;
        return result->cost;
    }

    return 0;
}

/*
 * function name: WLMGetQueryMem
 * description  : get plan mem from desc
 * arguments    : desc: query/utility desc info
 *                isQueryDesc: is query or utility desc info
 *                max: return query max mem or min mem
 * return value : plan max/min mem
 */
int WLMGetQueryMem(const QueryDesc* desc, bool isQueryDesc, bool max)
{
    if (isQueryDesc != false) {
        if (desc != NULL && desc->plannedstmt != NULL) {
            return max ? desc->plannedstmt->query_mem[0] : desc->plannedstmt->query_mem[1];
        }
    } else if (desc != NULL) {
        const UtilityDesc* result = (const UtilityDesc*)desc;
        return max ? result->query_mem[0] : result->query_mem[1];
    }

    return 0;
}

/*
 * function name: WLMGetInComputePool
 * description  : get in compute poll info from desc
 * arguments    : desc: query/utility desc info
 *                isQueryDesc: is query or utility desc info
 * return value : whether in compute pool
 */
bool WLMGetInComputePool(const QueryDesc* desc, bool isQueryDesc)
{
    if (isQueryDesc != false && desc != NULL && desc->plannedstmt != NULL) {
        return desc->plannedstmt->in_compute_pool;
    }

    return false;
}

/*
 * function name: WLMGetQueryMemDN
 * description  : get plan mem from desc for this DN
 * arguments    : desc: query/utility desc info
 *                isQueryDesc: is query or utility desc info
 *                max: return query max mem or min mem
 * return value : plan max mem
 */
int WLMGetQueryMemDN(const QueryDesc* desc, bool isQueryDesc, bool max)
{
    if (isQueryDesc != false) {
        if (desc != NULL && desc->plannedstmt != NULL) {
            /* not logic cluster case */
            if (desc->plannedstmt->ng_queryMem == NULL) {
                return max ? desc->plannedstmt->query_mem[0] : desc->plannedstmt->query_mem[1];
            }

            for (int i = 0; i < desc->plannedstmt->ng_num; i++) {
                if (0 == strcasecmp(desc->plannedstmt->ng_queryMem[i].nodegroup,
                                    u_sess->wlm_cxt->local_foreign_respool->ngroup)) {
                    return max ? desc->plannedstmt->ng_queryMem[i].query_mem[0]
                               : desc->plannedstmt->ng_queryMem[i].query_mem[1];
                }
            }

            /* not found */
            ereport(DEBUG1, (errmsg("WLMGetQueryMemDN: not found query_mem for DN")));
            return max ? desc->plannedstmt->query_mem[0] : desc->plannedstmt->query_mem[1];
        }
    } else if (desc != NULL) {
        const UtilityDesc* result = (const UtilityDesc*)desc;
        return max ? result->query_mem[0] : result->query_mem[1];
    }

    return 0;
}

/*
 * function name: WLMGetMinMem
 * description  : get min mem from desc
 * arguments    : desc: query/utility desc info
 *                isQueryDesc: is query or utility desc info
 *                max: return query max mem or min mem
 * return value : min mem
 */
int WLMGetMinMem(const QueryDesc* desc, bool isQueryDesc, bool max)
{
    if (isQueryDesc != false) {
        if (desc != NULL && desc->plannedstmt != NULL) {
            int query_mem = max ? desc->plannedstmt->query_mem[0] : desc->plannedstmt->query_mem[1];

            return Min(query_mem, desc->plannedstmt->assigned_query_mem[1]);
        }
    } else if (desc != NULL) {
        const UtilityDesc* result = (const UtilityDesc*)desc;
        int query_mem = max ? result->query_mem[0] : result->query_mem[1];
        bool use_tenant = false;

        return Min(query_mem, (int)dywlm_client_get_max_memory(&use_tenant));
    }

    return 0;
}

/*
 * function name: WLMGetAvailbaleMemory
 * description  : get available memory from specified node group
 * arguments    : ngname: the specified node group
 * return value : memory size in MB
 */
int WLMGetAvailbaleMemory(const char* ngname)
{
    if (!StringIsValid(ngname)) {
        return 0;
    }

    int minsize = 0;
    Oid userid = GetCurrentUserId();

    /* get the node group information of current userid */
    WLMNodeGroupInfo* ng = WLMGetNodeGroupByUserId(userid);
    if (NULL == ng) {
        ereport(LOG, (errmsg("cannot get logic cluster by user %u.", userid)));
        return 0;
    }

    /* the same node group */
    if (0 == strcmp(ng->group_name, ngname)) {
        if (ng->foreignrp != NULL) {
            minsize = (ng->total_memory - ng->foreignrp->memsize) * u_sess->attr.attr_resource.dynamic_memory_quota /
                          FULL_PERCENT -
                      ng->used_memory;
        } else {
            minsize =
                ng->total_memory * u_sess->attr.attr_resource.dynamic_memory_quota / FULL_PERCENT - ng->used_memory;
        }
    } else { /* other node group */
        minsize = ng->total_memory * OTHER_USED_PERCENT / FULL_PERCENT - ng->used_memory;
        if (minsize > 0 && ng->foreignrp != NULL) {
            int others = 0;

            /* check the estimated size and used size */
            if (ng->foreignrp->memused > ng->foreignrp->estmsize) {
                others = ng->foreignrp->memsize * u_sess->attr.attr_resource.dynamic_memory_quota / FULL_PERCENT -
                         ng->foreignrp->memused;
            } else {
                others = ng->foreignrp->memsize * u_sess->attr.attr_resource.dynamic_memory_quota / FULL_PERCENT -
                         ng->foreignrp->estmsize;
            }

            minsize = Min(minsize, others);
        }
    }

    return Max(minsize, 0);
}

/*
 * WLMChoosePlanA
 *	check if planA can be used, according to current resource usage info.
 *
 * Parameters:
 *	@in stmt: original plan A
 *
 * Returns: true, use plan A, else use plan B
 */
bool WLMChoosePlanA(PlannedStmt* stmt)
{
    WLMNodeGroupInfo* wlmnginfo = NULL;
    int minsize = 0;
    Oid my_group_oid = get_pgxc_logic_groupoid(GetCurrentUserId());
    int maxused = 0;
    bool use_planA = true;

    Assert(OidIsValid(my_group_oid));
    Assert(stmt->ng_num > 0);

    for (int i = 0; i < stmt->ng_num; i++) {
        wlmnginfo = WLMGetNodeGroupFromHTAB(stmt->ng_queryMem[i].nodegroup);
        Assert(wlmnginfo);
        maxused = Max(wlmnginfo->used_memory, wlmnginfo->estimate_memory);

        /* the logic cluster is already busy, so we use plan B */
        if ((wlmnginfo->total_memory * OTHER_USED_PERCENT / FULL_PERCENT) < maxused) {
            use_planA = false;
            ereport(DEBUG2,
                (errmsg(
                    "The used memory on node group '%s' is beyond 60%% total memory", stmt->ng_queryMem[i].nodegroup)));
            break;
        }

        /* not defined foreign pool, or my nodegroup */
        if (wlmnginfo->foreignrp == NULL || my_group_oid == stmt->ng_queryMem[i].ng_oid) {
            minsize =
                Max(SIMPLE_THRESHOLD, (wlmnginfo->total_memory * OTHER_USED_PERCENT / FULL_PERCENT - maxused) * 1024);
        } else {
            if (wlmnginfo->foreignrp->memused > wlmnginfo->foreignrp->estmsize) {
                minsize = Max(SIMPLE_THRESHOLD,
                    (wlmnginfo->foreignrp->memsize * u_sess->attr.attr_resource.dynamic_memory_quota / FULL_PERCENT -
                        wlmnginfo->foreignrp->memused) *
                        1024);
            } else {
                minsize = Max(SIMPLE_THRESHOLD,
                    (wlmnginfo->foreignrp->memsize * u_sess->attr.attr_resource.dynamic_memory_quota / FULL_PERCENT -
                        wlmnginfo->foreignrp->estmsize) *
                        1024);
            }
        }

        /* the query mem cannot be satisfied, so we use plan B */
        if ((stmt->ng_queryMem[i].query_mem[1] > 0 && stmt->ng_queryMem[i].query_mem[1] > minsize) ||
            (stmt->ng_queryMem[i].query_mem[1] == 0 && stmt->ng_queryMem[i].query_mem[0] > minsize)) {
            use_planA = false;
            ereport(DEBUG2,
                (errmsg("Need reset plan for query: nodegroup %s, query_mem[0] %d, query_mem[1] %d, minsize %d",
                    stmt->ng_queryMem[i].nodegroup,
                    stmt->ng_queryMem[i].query_mem[0],
                    stmt->ng_queryMem[i].query_mem[1],
                    minsize)));
            break;
        }
    }

    return use_planA;
}

/*
 * @Description: check DefaultXactReadOnly to report error
 * @IN void
 * @Return: void
 * @See also:
 */
void WLMDefaultXactReadOnlyCheckAndHandle(void)
{
    /* just need cancel current running stmt when set DefaultXactReadOnly=true */
    if (u_sess->attr.attr_storage.DefaultXactReadOnly && !t_thrd.xact_cxt.CancelStmtForReadOnly) {
        t_thrd.xact_cxt.CancelStmtForReadOnly = true;

        int num = 0;

        /* get running thread */
        ThreadId* threads = pgstat_get_stmttag_write_entry(&num);

        if (threads == NULL) {
            return;
        }

        (void)LWLockAcquire(ProcArrayLock, LW_SHARED);

        /* send signal to finish the thread */
        for (int i = 0; i < num; ++i) {
            (void)SendProcSignal(threads[i], PROCSIG_DEFAULTXACT_READONLY, InvalidBackendId);
            ereport(LOG, (errmsg("cancel thread %ld due to insufficient disk space.", (long)threads[i])));
        }

        LWLockRelease(ProcArrayLock);

        pfree(threads);
    } else if (t_thrd.xact_cxt.CancelStmtForReadOnly && !u_sess->attr.attr_storage.DefaultXactReadOnly) {
        t_thrd.xact_cxt.CancelStmtForReadOnly = false;
    }
}
