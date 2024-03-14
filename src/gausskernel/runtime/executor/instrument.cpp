/* -------------------------------------------------------------------------
 *
 * instrument.cpp
 *	 functions for instrumentation of plan execution
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Copyright (c) 2001-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 2021, openGauss Contributors
 *
 * IDENTIFICATION
 *	  src/gausskernel/runtime/executor/instrument.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "executor/instrument.h"
#ifdef PGXC
#include "catalog/pgxc_node.h"
#include "pgxc/pgxc.h"
#include "pgxc/execRemote.h"
#endif
#include "connector.h"
#include "executor/executor.h"
#include "miscadmin.h"
#include "nodes/memnodes.h"
#include "utils/memtrack.h"
#include <linux/perf_event.h>
#include <sys/ioctl.h>
#include <asm/unistd.h>
#include "postmaster/fork_process.h"
#include "postmaster/postmaster.h"
#include "storage/buf/bufmgr.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/pmsignal.h"
#include "storage/proc.h"
#include "storage/procsignal.h"
#include "utils/ps_status.h"
#include "gssignal/gs_signal.h"
#include "libpq/ip.h"
#include "libpq/pqformat.h"
#include "libpq/pqsignal.h"
#include "access/xact.h"
#include "distributelayer/streamMain.h"
#include "commands/dbcommands.h"
#include "utils/builtins.h"
#include "utils/snapmgr.h"
#include "utils/memprot.h"
#include "workload/commgr.h"
#include "workload/workload.h"
#include "optimizer/streamplan.h"
#include "parser/parsetree.h"
#include "utils/lsyscache.h"
#include "executor/exec/execdesc.h"
#include "libpq/pqformat.h"

extern const char* GetStreamType(Stream* node);
extern void insert_obsscaninfo(
    uint64 queryid, const char* rel_name, int64 file_count, double scan_data_size, double total_time, int format);

static void BufferUsageAccumDiff(BufferUsage* dst, const BufferUsage* add, const BufferUsage* sub);
static void CPUUsageGetCurrent(CPUUsage* cur);
static void CPUUsageAccumDiff(CPUUsage* dst, const CPUUsage* add, const CPUUsage* sub);

OperatorProfileTable g_operator_table;

#define OPERATOR_INFO_COLLECT_TIMER 180 /* seconds */

/* get max and min data in the session info */
#define GetMaxAndMinExplainSessionInfo(info, elem, data) \
    do {                                                 \
        if ((data) > (info)->max_##elem)                 \
            (info)->max_##elem = data;                   \
        if ((info)->min_##elem == -1)                    \
            (info)->min_##elem = data;                   \
        else if ((data) < (info)->min_##elem)            \
            (info)->min_##elem = data;                   \
    } while (0)

#ifndef WIN32
static inline uint64 rdtsc(void)
{
#ifdef __aarch64__
    uint64 cval = 0;
    asm volatile("isb; mrs %0, cntvct_el0" : "=r"(cval) : : "memory");

    return cval;
#elif defined(__GNUC__) && (defined(__x86_64__) || defined(__i386__))
    uint32 hi = 0;
    uint32 lo = 0;
    asm volatile("rdtsc" : "=a"(lo), "=d"(hi));

    return ((uint64)lo) | (((uint64)hi) << 32);
#else
    return clock();
#endif
}
#else

#include "intrin.h"

static inline uint64 rdtsc(void)
{
    return (uint64)__rdtsc();
}
#endif

sortMessage sortmessage[] = {
    {HEAPSORT, "top-N heapsort"},
    {QUICKSORT, "quicksort"},
    {EXTERNALSORT, "external sort"},
    {EXTERNALMERGE, "external merge"},
    {STILLINPROGRESS, "still in progress"},
};

TrackDesc trackdesc[] = {
    {RECV_PLAN, "begin query", false, TRACK_TIMESTAMP},

    {START_EXEC, "execution start", false, TRACK_TIMESTAMP},

    {FINISH_EXEC, "execution finish", false, TRACK_TIMESTAMP},

    {REPORT_FINISH, "report finish", false, TRACK_TIMESTAMP},

    {PASS_WLM, "pass workload manager", false, TRACK_TIMESTAMP},

    {STREAMNET_INIT, "Datanode build connection", false, TRACK_TIME},

    {LOAD_CU_DESC, "load CU description", true, TRACK_TIME | TRACK_COUNT},

    {MIN_MAX_CHECK, "min/max check", true, TRACK_TIME | TRACK_COUNT},

    {FILL_BATCH, "fill vector batch", true, TRACK_TIME | TRACK_COUNT},

    {GET_CU_DATA, "get CU data", true, TRACK_TIME | TRACK_COUNT},

    {UNCOMPRESS_CU, "uncompress CU data", true, TRACK_TIME | TRACK_COUNT},

    {CSTORE_PROJECT, "apply projection and filter", true, TRACK_TIME | TRACK_COUNT},

    {LLVM_COMPILE_TIME, "LLVM Compilation", true, TRACK_TIME | TRACK_COUNT},

    {CNNET_INIT, "coordinator get datanode connection", true, TRACK_TIME | TRACK_COUNT},

    {CN_CHECK_AND_UPDATE_NODEDEF, "Coordinator check and update node definition", true, TRACK_TIME | TRACK_COUNT}, 

    {CN_SERIALIZE_PLAN, "Coordinator serialize plan", true, TRACK_TIME | TRACK_COUNT},

    {CN_SEND_QUERYID_WITH_SYNC, "Coordinator send query id with sync", true, TRACK_TIME | TRACK_COUNT},

    {CN_SEND_BEGIN_COMMAND, "Coordinator send begin command", true, TRACK_TIME | TRACK_COUNT},

    {CN_START_COMMAND, "Coordinator start transaction and send query", true, TRACK_TIME | TRACK_COUNT},

    {DN_STREAM_THREAD_INIT, "Datanode start up stream thread", true, TRACK_TIME | TRACK_COUNT},

    {FILL_LATER_BATCH, "fill later vector batch", true, TRACK_TIME | TRACK_COUNT},

    {GET_CU_DATA_LATER_READ, "get cu data for later read", true, TRACK_TIME | TRACK_COUNT},

    {GET_CU_SIZE, "get cu size", true, TRACK_VALUE | TRACK_VALUE_COUNT},

    {GET_CU_DATA_FROM_CACHE, "get cu data from cache", true, TRACK_TIME | TRACK_COUNT},

    {FILL_VECTOR_BATCH_BY_TID, "fill vector batch by TID", true, TRACK_TIME | TRACK_COUNT},

    {SCAN_BY_TID, "scan by TID", true, TRACK_TIME | TRACK_COUNT},

    {PREFETCH_CU_LIST, "prefetch CU list", true, TRACK_TIME | TRACK_COUNT},

    {GET_COMPUTE_DATANODE_CONNECTION, "get compute datanode connection", true, TRACK_TIME | TRACK_COUNT},

    {GET_COMPUTE_POOL_CONNECTION, "get compute pool connection", true, TRACK_TIME | TRACK_COUNT},

    {GET_PG_LOG_TUPLES, "get pg_log tuples", true, TRACK_TIME | TRACK_COUNT},

    {GET_GS_PROFILE_TUPLES, "get gs_profile tuples", true, TRACK_TIME | TRACK_COUNT},

    {TSSTORE_PROJECT, "timeseries projection and filter", true, TRACK_TIME | TRACK_COUNT},

    {TSSTORE_SEARCH, "search timeseries table", true, TRACK_TIME | TRACK_COUNT},

    {TSSTORE_SEARCH_TAGID, "search tag table get tag id", true, TRACK_TIME | TRACK_COUNT},
};

/*
 * @Description: stream consumer poll time start
 * @in instr: current instrument in question
 * @return: void
 */
void NetWorkTimePollStart(Instrumentation* instr)
{
    if (unlikely(instr != NULL))
        INSTR_TIME_SET_CURRENT(instr->network_perfdata.start_poll_time);
}

/*
 * @Description: stream consumer deserialize time start
 * @in instr: current instrument in question
 * @return: void
 */
FORCE_INLINE
void NetWorkTimeDeserializeStart(Instrumentation* instr)
{
    if (unlikely(instr != NULL))
        INSTR_TIME_SET_CURRENT(instr->network_perfdata.start_deserialize_time);
}

/*
 * @Description: local stream consumer copy time start
 * @in instr: current instrument in question
 * @return: void
 */
FORCE_INLINE
void NetWorkTimeCopyStart(Instrumentation* instr)
{
    if (unlikely(instr != NULL))
        INSTR_TIME_SET_CURRENT(instr->network_perfdata.start_copy_time);
}

/*
 * @Description: stream consumer poll time collect
 * @in instr: current instrument in question
 * @return: void
 */
FORCE_INLINE
void NetWorkTimePollEnd(Instrumentation* instr)
{
    if (unlikely(instr != NULL)) {
        instr_time end_time;
        INSTR_TIME_SET_CURRENT(end_time);
        INSTR_TIME_ACCUM_DIFF(
            instr->network_perfdata.network_poll_time, end_time, instr->network_perfdata.start_poll_time);
        INSTR_TIME_SET_ZERO(instr->network_perfdata.start_poll_time);
    }
}

/*
 * @Description: stream consumer deserialize time collect
 * @in instr: current instrument in question
 * @return: void
 */
FORCE_INLINE
void NetWorkTimeDeserializeEnd(Instrumentation* instr)
{
    if (unlikely(instr != NULL)) {
        instr_time end_time;
        INSTR_TIME_SET_CURRENT(end_time);
        INSTR_TIME_ACCUM_DIFF(
            instr->network_perfdata.network_deserialize_time, end_time, instr->network_perfdata.start_deserialize_time);
        INSTR_TIME_SET_ZERO(instr->network_perfdata.start_deserialize_time);
    }
}

/*
 * @Description: stream consumer deserialize time collect
 * @in instr: current instrument in question
 * @return: void
 */
FORCE_INLINE
void NetWorkTimeCopyEnd(Instrumentation* instr)
{
    if (unlikely(instr != NULL)) {
        instr_time end_time;
        INSTR_TIME_SET_CURRENT(end_time);
        INSTR_TIME_ACCUM_DIFF(
            instr->network_perfdata.network_copy_time, end_time, instr->network_perfdata.start_copy_time);
        INSTR_TIME_SET_ZERO(instr->network_perfdata.start_copy_time);
    }
}

/*
 * @Description: stream send time start time
 * @in instr: current instrument in question
 * @return: void
 */
FORCE_INLINE
void StreamTimeSendStart(Instrumentation* instr)
{
    if (unlikely(instr != NULL))
        INSTR_TIME_SET_CURRENT(instr->stream_senddata.start_send_time);
}

/*
 * @Description: stream producer wait quota start time
 * @in instr: current instrument in question
 * @return: void
 */
FORCE_INLINE
void StreamTimeWaitQuotaStart(Instrumentation* instr)
{
    if (unlikely(instr != NULL))
        INSTR_TIME_SET_CURRENT(instr->stream_senddata.start_wait_quota_time);
}

/*
 * @Description: stream OS send time start time
 * @in instr: current instrument in question
 * @return: void
 */
FORCE_INLINE
void StreamTimeOSSendStart(Instrumentation* instr)
{
    if (unlikely(instr != NULL))
        INSTR_TIME_SET_CURRENT(instr->stream_senddata.start_OS_send_time);
}

/*
 * @Description: stream producer serialization start time
 * @in instr: current instrument in question
 * @return: void
 */
FORCE_INLINE
void StreamTimeSerilizeStart(Instrumentation* instr)
{
    if (unlikely(instr != NULL))
        INSTR_TIME_SET_CURRENT(instr->stream_senddata.start_serialize_time);
}

/*
 * @Description: local stream producer data copy start time
 * @in instr: current instrument in question
 * @return: void
 */
FORCE_INLINE
void StreamTimeCopyStart(Instrumentation* instr)
{
    if (unlikely(instr != NULL))
        INSTR_TIME_SET_CURRENT(instr->stream_senddata.start_copy_time);
}

/*
 * @Description: stream send time collect time
 * @in instr: current instrument in question
 * @return: void
 */
FORCE_INLINE
void StreamTimeSendEnd(Instrumentation* instr)
{
    if (unlikely(instr != NULL)) {
        instr_time end_time;
        INSTR_TIME_SET_CURRENT(end_time);
        instr->stream_senddata.loops = true;
        INSTR_TIME_ACCUM_DIFF(instr->stream_senddata.stream_send_time, end_time, instr->stream_senddata.start_send_time);
        INSTR_TIME_SET_ZERO(instr->stream_senddata.start_send_time);
    }
}

/*
 * @Description: stream producer serialization time collect time
 * @in instr: current instrument in question
 * @return: void
 */
FORCE_INLINE
void StreamTimeSerilizeEnd(Instrumentation* instr)
{
    if (unlikely(instr != NULL)) {
        instr_time end_time;
        INSTR_TIME_SET_CURRENT(end_time);
        instr->stream_senddata.loops = true;
        INSTR_TIME_ACCUM_DIFF(
            instr->stream_senddata.stream_serialize_time, end_time, instr->stream_senddata.start_serialize_time);
        INSTR_TIME_SET_ZERO(instr->stream_senddata.start_serialize_time);
    }
}

/*
 * @Description: local stream producer data copy time collect time
 * @in instr: current instrument in question
 * @return: void
 */
FORCE_INLINE
void StreamTimeCopyEnd(Instrumentation* instr)
{
    if (unlikely(instr != NULL)) {
        instr_time end_time;
        INSTR_TIME_SET_CURRENT(end_time);
        instr->stream_senddata.loops = true;
        INSTR_TIME_ACCUM_DIFF(instr->stream_senddata.stream_copy_time, end_time, instr->stream_senddata.start_copy_time);
        INSTR_TIME_SET_ZERO(instr->stream_senddata.start_copy_time);
    }
}

/*
 * @Description: stream producer wait quota time collect time
 * @in instr: current instrument in question
 * @return: void
 */
FORCE_INLINE
void StreamTimeWaitQuotaEnd(Instrumentation* instr)
{
    if (unlikely(instr != NULL)) {
        instr_time end_time;
        INSTR_TIME_SET_CURRENT(end_time);
        instr->stream_senddata.loops = true;
        INSTR_TIME_ACCUM_DIFF(
            instr->stream_senddata.stream_wait_quota_time, end_time, instr->stream_senddata.start_wait_quota_time);
        INSTR_TIME_SET_ZERO(instr->stream_senddata.start_wait_quota_time);
    }
}

/*
 * @Description: stream OS send time collect time
 * @in instr: current instrument in question
 * @return: void
 */
FORCE_INLINE
void StreamTimeOSSendEnd(Instrumentation* instr)
{
    if (unlikely(instr != NULL)) {
        instr_time end_time;
        INSTR_TIME_SET_CURRENT(end_time);
        instr->stream_senddata.loops = true;
        INSTR_TIME_ACCUM_DIFF(
            instr->stream_senddata.stream_OS_send_time, end_time, instr->stream_senddata.start_OS_send_time);
        INSTR_TIME_SET_ZERO(instr->stream_senddata.start_OS_send_time);
    }
}

/*
 * @Description: set t_thrd.pgxc_cxt.GlobalNetInstr to NULL
 * @in : void
 * @return: void
 */
FORCE_INLINE
void SetInstrNull()
{
    t_thrd.pgxc_cxt.GlobalNetInstr = NULL;
    u_sess->instr_cxt.global_instr = NULL;
    u_sess->instr_cxt.thread_instr = NULL;
}

void CalculateContextSize(MemoryContext ctx, int64* memory_size)
{
#ifndef ENABLE_MEMORY_CHECK
    AllocSetContext* aset = (AllocSetContext*)ctx;
#else
    AsanSetContext* aset = (AsanSetContext*)ctx;
#endif
    MemoryContext child;

    if (ctx == NULL)
        return;

    /* to return the accurate value when memory tracking is enable */
    if (u_sess->attr.attr_memory.memory_tracking_mode > MEMORY_TRACKING_PEAKMEMORY && aset->track)
        *memory_size = aset->track->allBytesPeak;
    else {
        /* calculate MemoryContext Stats */
        *memory_size += (aset->totalSpace - aset->freeSpace);

        /* recursive MemoryContext's child */
        for (child = ctx->firstchild; child != NULL; child = child->nextchild) {
            CalculateContextSize(child, memory_size);
        }
    }
}

/* Allocate new instrumentation structure(s) */
Instrumentation* InstrAlloc(int n, int instrument_options)
{
    Instrumentation* instr = NULL;

    /* initialize all fields to zeroes, then modify as needed */
    instr = (Instrumentation*)palloc0(n * sizeof(Instrumentation));
    if (instrument_options & (INSTRUMENT_BUFFERS | INSTRUMENT_TIMER)) {
        bool need_buffers = (instrument_options & INSTRUMENT_BUFFERS) != 0;
        bool need_timer = (instrument_options & INSTRUMENT_TIMER) != 0;
#ifndef ENABLE_MULTIPLE_NODES
        bool need_cpu = (instrument_options & INSTRUMENT_CPUS) != 0;
#endif
        int i;

        for (i = 0; i < n; i++) {
            instr[i].need_bufusage = need_buffers;
            instr[i].need_timer = need_timer;
#ifndef ENABLE_MULTIPLE_NODES
            instr[i].need_cpu = need_cpu;
#endif
        }
    }

    return instr;
}

/* Entry to a plan node */
void InstrStartNode(Instrumentation* instr)
{

#if !defined(ENABLE_MULTIPLE_NODES) && !defined(USE_SPQ)
    if (!u_sess->attr.attr_common.enable_seqscan_fusion && !instr->first_time) {
#else
    if ((t_thrd.spq_ctx.spq_role == ROLE_UTILITY && !u_sess->attr.attr_common.enable_seqscan_fusion && !instr->first_time)
        || (t_thrd.spq_ctx.spq_role != ROLE_UTILITY && !instr->first_time)) {
#endif
        instr->enter_time = GetCurrentTimestamp();
        instr->first_time = true;
    }

#if !defined(ENABLE_MULTIPLE_NODES) && !defined(USE_SPQ)
    if (!u_sess->attr.attr_common.enable_seqscan_fusion && !instr->first_time) {
#else
    if ((t_thrd.spq_ctx.spq_role == ROLE_UTILITY && !u_sess->attr.attr_common.enable_seqscan_fusion && !instr->first_time)
        || (t_thrd.spq_ctx.spq_role != ROLE_UTILITY && !instr->first_time)) {
#endif
        CPUUsageGetCurrent(&instr->cpuusage_start);
    }

    if (instr->need_timer) {
        if (INSTR_TIME_IS_ZERO(instr->starttime)) {
            INSTR_TIME_SET_CURRENT(instr->starttime);
        } else {
            elog(DEBUG2, "InstrStartNode called twice in a row");
        }
    }

    /* save buffer usage totals at node entry, if needed */
    if (instr->need_bufusage)
        instr->bufusage_start = *u_sess->instr_cxt.pg_buffer_usage;
}

/*
 * @Description: add memory context to the control list hold by a instrument
 * @in instr: current instrument in question
 * @in context: memory context to be added
 * @return: void
 */
void AddControlMemoryContext(Instrumentation* instr, MemoryContext context)
{
    /* add u_sess->instr_cxt.global_instr == NULL condition to avoid core when using gsql to datanode directly */
    if (instr == NULL || u_sess->instr_cxt.global_instr == NULL)
        return;

    MemoryContext old_context = NULL;
    if (IS_SPQ_COORDINATOR || IS_PGXC_COORDINATOR) {
        Assert(u_sess->instr_cxt.global_instr->getInstrDataContext() != NULL);
        old_context = MemoryContextSwitchTo(u_sess->instr_cxt.global_instr->getInstrDataContext());
    } else {
        MemoryContext original_stream_runtime_context = MemoryContextOriginal((char*)u_sess->instr_cxt.global_instr);
        Assert(original_stream_runtime_context != NULL);
        old_context = MemoryContextSwitchTo(original_stream_runtime_context);
    }

    List* control_list = instr->memoryinfo.controlContextList;
    control_list = lappend(control_list, context);
    instr->memoryinfo.controlContextList = control_list;
    MemoryContextSwitchTo(old_context);
}

/* Exit from a plan node */
void InstrStopNode(Instrumentation* instr, double n_tuples, bool containMemory)
{
    instr_time end_time;
    CPUUsage cpu_usage = {0};

    /* count the returned tuples */
    instr->ntuples += n_tuples;

    /* let's update the time only if the timer was requested */
    if (instr->need_timer) {
        if (INSTR_TIME_IS_ZERO(instr->starttime)) {
            elog(DEBUG2, "InstrStopNode called without start");
            return;
        }

        INSTR_TIME_SET_CURRENT(end_time);
        INSTR_TIME_ACCUM_DIFF(instr->counter, end_time, instr->starttime);

        INSTR_TIME_SET_ZERO(instr->starttime);
    }

#if !defined(ENABLE_MULTIPLE_NODES) && !defined(USE_SPQ)
    if (!u_sess->attr.attr_common.enable_seqscan_fusion)
#else
    if ((t_thrd.spq_ctx.spq_role == ROLE_UTILITY && !u_sess->attr.attr_common.enable_seqscan_fusion)
        || t_thrd.spq_ctx.spq_role != ROLE_UTILITY)
#endif
        CPUUsageGetCurrent(&cpu_usage);  

    /* Add delta of buffer usage since entry to node's totals */
    if (instr->need_bufusage) {
        BufferUsageAccumDiff(&instr->bufusage, u_sess->instr_cxt.pg_buffer_usage, &instr->bufusage_start);
    }

#if !defined(ENABLE_MULTIPLE_NODES) && !defined(USE_SPQ)
    if (!u_sess->attr.attr_common.enable_seqscan_fusion)
#else
    if ((t_thrd.spq_ctx.spq_role == ROLE_UTILITY && !u_sess->attr.attr_common.enable_seqscan_fusion)
        || t_thrd.spq_ctx.spq_role != ROLE_UTILITY)
#endif
        CPUUsageAccumDiff(&instr->cpuusage, &cpu_usage, &instr->cpuusage_start);

    /* Is this the first tuple of this cycle? */
    if (!instr->running) {
        instr->running = true;
        instr->firsttuple = INSTR_TIME_GET_DOUBLE(instr->counter);
    }

#if !defined(ENABLE_MULTIPLE_NODES) && !defined(USE_SPQ)
    if (!u_sess->attr.attr_common.enable_seqscan_fusion && containMemory) {
#else
    if ((t_thrd.spq_ctx.spq_role == ROLE_UTILITY && !u_sess->attr.attr_common.enable_seqscan_fusion && containMemory)
        || (t_thrd.spq_ctx.spq_role != ROLE_UTILITY && containMemory)) {
#endif
        int64 memory_size = 0;
        int64 control_memory_size = 0;
        /* calculate the memory context size of this Node */
        CalculateContextSize(instr->memoryinfo.nodeContext, &memory_size);
        if (instr->memoryinfo.peakOpMemory < memory_size)
            instr->memoryinfo.peakOpMemory = memory_size;

        List* control_list = instr->memoryinfo.controlContextList;
        ListCell* context_cell = NULL;

        /* calculate all control memory */
        foreach (context_cell, control_list) {
            MemoryContext context = (MemoryContext)lfirst(context_cell);
            CalculateContextSize(context, &control_memory_size);
        }

        if (instr->memoryinfo.peakControlMemory < control_memory_size)
            instr->memoryinfo.peakControlMemory = control_memory_size;
    }
}

/* Finish a run cycle for a plan node */
void InstrEndLoop(Instrumentation* instr)
{
    double total_time;

    /* Skip if nothing has happened, or already shut down */
    if (!instr->running)
        return;

    if (!INSTR_TIME_IS_ZERO(instr->starttime)) {
        elog(DEBUG2, "InstrEndLoop called on running node");
    }
    /* Accumulate per-cycle statistics into totals */
    total_time = INSTR_TIME_GET_DOUBLE(instr->counter);

    instr->startup += instr->firsttuple;
    instr->total += total_time;
    instr->ntuples += instr->tuplecount;
    instr->nloops += 1;

    instr->network_perfdata.total_poll_time += INSTR_TIME_GET_MILLISEC(instr->network_perfdata.network_poll_time);
    instr->network_perfdata.total_deserialize_time +=
        INSTR_TIME_GET_MILLISEC(instr->network_perfdata.network_deserialize_time);
    instr->network_perfdata.total_copy_time += INSTR_TIME_GET_MILLISEC(instr->network_perfdata.network_copy_time);

    instr->stream_senddata.total_send_time += INSTR_TIME_GET_MILLISEC(instr->stream_senddata.stream_send_time);
    instr->stream_senddata.total_wait_quota_time +=
        INSTR_TIME_GET_MILLISEC(instr->stream_senddata.stream_wait_quota_time);
    instr->stream_senddata.total_os_send_time += INSTR_TIME_GET_MILLISEC(instr->stream_senddata.stream_OS_send_time);
    instr->stream_senddata.total_serialize_time +=
        INSTR_TIME_GET_MILLISEC(instr->stream_senddata.stream_serialize_time);
    instr->stream_senddata.total_copy_time += INSTR_TIME_GET_MILLISEC(instr->stream_senddata.stream_copy_time);

    /* Reset for next cycle (if any) */
    instr->running = false;
    INSTR_TIME_SET_ZERO(instr->starttime);
    INSTR_TIME_SET_ZERO(instr->counter);

    INSTR_TIME_SET_ZERO(instr->network_perfdata.start_poll_time);
    INSTR_TIME_SET_ZERO(instr->network_perfdata.start_deserialize_time);
    INSTR_TIME_SET_ZERO(instr->network_perfdata.start_copy_time);
    INSTR_TIME_SET_ZERO(instr->network_perfdata.network_poll_time);
    INSTR_TIME_SET_ZERO(instr->network_perfdata.network_deserialize_time);
    INSTR_TIME_SET_ZERO(instr->network_perfdata.network_copy_time);

    INSTR_TIME_SET_ZERO(instr->stream_senddata.start_send_time);
    INSTR_TIME_SET_ZERO(instr->stream_senddata.start_OS_send_time);
    INSTR_TIME_SET_ZERO(instr->stream_senddata.start_wait_quota_time);
    INSTR_TIME_SET_ZERO(instr->stream_senddata.start_serialize_time);
    INSTR_TIME_SET_ZERO(instr->stream_senddata.start_copy_time);
    INSTR_TIME_SET_ZERO(instr->stream_senddata.stream_send_time);
    INSTR_TIME_SET_ZERO(instr->stream_senddata.stream_OS_send_time);
    INSTR_TIME_SET_ZERO(instr->stream_senddata.stream_wait_quota_time);
    INSTR_TIME_SET_ZERO(instr->stream_senddata.stream_serialize_time);
    INSTR_TIME_SET_ZERO(instr->stream_senddata.stream_copy_time);

    instr->firsttuple = 0;
    instr->tuplecount = 0;
}

void InstrStartStream(StreamTime* instr)
{
    if (instr->need_timer) {
        if (INSTR_TIME_IS_ZERO(instr->starttime)) {
            INSTR_TIME_SET_CURRENT(instr->starttime);
        } else {
            elog(DEBUG2, "InstrStartNode called twice in a row");
        }
    }
}

void InstrStopStream(StreamTime* instr, double n_tuples)
{
    instr_time end_time;

    /* count the returned tuples */
    instr->tuplecount += n_tuples;

    /* let's update the time only if the timer was requested */
    if (instr->need_timer) {
        if (INSTR_TIME_IS_ZERO(instr->starttime)) {
            elog(DEBUG2, "InstrStopNode called without start");
            return;
        }

        INSTR_TIME_SET_CURRENT(end_time);
        INSTR_TIME_ACCUM_DIFF(instr->counter, end_time, instr->starttime);

        INSTR_TIME_SET_ZERO(instr->starttime);
    }

    /* Is this the first tuple of this cycle? */
    if (!instr->running) {
        instr->running = true;
        instr->firsttuple = INSTR_TIME_GET_DOUBLE(instr->counter);
    }
}

/* Finish a run cycle for a plan node */
void StreamEndLoop(StreamTime* instr)
{
    double total_time;

    /* Skip if nothing has happened, or already shut down */
    if (!instr->running)
        return;

    if (!INSTR_TIME_IS_ZERO(instr->starttime)) {
        elog(DEBUG2, "InstrEndLoop called on running node");
    }

    /* Accumulate per-cycle statistics into totals */
    total_time = INSTR_TIME_GET_DOUBLE(instr->counter);

    instr->startup += instr->firsttuple;
    instr->total += total_time;
    instr->ntuples += instr->tuplecount;
    instr->nloops += 1;

    /* Reset for next cycle (if any) */
    instr->running = false;
    INSTR_TIME_SET_ZERO(instr->starttime);
    INSTR_TIME_SET_ZERO(instr->counter);
    instr->firsttuple = 0;
    instr->tuplecount = 0;
}

/* 
 * BufferUsageAccumDiff
 * calculate every element of dst like: dst += add - sub 
 */
static void BufferUsageAccumDiff(BufferUsage* dst, const BufferUsage* add, const BufferUsage* sub)
{
    dst->shared_blks_hit += add->shared_blks_hit - sub->shared_blks_hit;
    dst->shared_blks_read += add->shared_blks_read - sub->shared_blks_read;
    dst->shared_blks_dirtied += add->shared_blks_dirtied - sub->shared_blks_dirtied;
    dst->shared_blks_written += add->shared_blks_written - sub->shared_blks_written;
    dst->local_blks_hit += add->local_blks_hit - sub->local_blks_hit;
    dst->local_blks_read += add->local_blks_read - sub->local_blks_read;
    dst->local_blks_dirtied += add->local_blks_dirtied - sub->local_blks_dirtied;
    dst->local_blks_written += add->local_blks_written - sub->local_blks_written;
    dst->temp_blks_read += add->temp_blks_read - sub->temp_blks_read;
    dst->temp_blks_written += add->temp_blks_written - sub->temp_blks_written;
    INSTR_TIME_ACCUM_DIFF(dst->blk_read_time, add->blk_read_time, sub->blk_read_time);
    INSTR_TIME_ACCUM_DIFF(dst->blk_write_time, add->blk_write_time, sub->blk_write_time);
}

/*
 * @Description: ThreadInstrumentation Constructor
 * 				 m_instrArrayMap make the position of plannode in current stream.
 *				 m_instrArray store the instrument data of all plannode in current stream.
 * 				 you can find the instrument by search m_instrArray[m_instrArrayMap[plannodeid-1]].
 * @in queryId: debug queryid
 * @in segmentid:Top plannode_id in current stream
 * @in nodesNumInThread: how many plan nodes in current stream
 * @in nodesNumInSql: how many plannodes in all
 * @return: None
 */
ThreadInstrumentation::ThreadInstrumentation(uint64 query_id, int segment_id, int nodes_num_in_thread, int nodes_num_in_sql)
    : m_queryId(query_id),
      m_segmentId(segment_id),
      m_instrArrayLen(nodes_num_in_thread),
      m_instrArrayMapLen(nodes_num_in_sql),
      m_instrArrayAllocIndx(0)
{
    m_queryString = NULL;
    m_generalTrackNum = 0;
    m_nodeTrackNum = 0;
    m_instrArrayMap = (int*)palloc0(sizeof(int) * m_instrArrayMapLen);

    for (int i = 0; i < m_instrArrayMapLen; i++)
        m_instrArrayMap[i] = -1;

    for (uint i = 0; i < lengthof(trackdesc); i++) {
        if (trackdesc[i].nodeBind == false)
            m_generalTrackNum++;
        else
            m_nodeTrackNum++;
    }

    m_instrArray = (NodeInstr*)palloc0(sizeof(NodeInstr) * m_instrArrayLen);

    /* node initialize */
    for (int i = 0; i < m_instrArrayLen; i++) {
        m_instrArray[i].instr.isExecute = false;
        m_instrArray[i].instr.isValid = false;
    }

    /* generalTrack initialize */
    m_generalTrackArray = (Track*)palloc0(m_generalTrackNum * sizeof(Track));
    for (int i = 0; i < m_generalTrackNum; i++) {
        m_generalTrackArray[i].track_time.need_timer = false;
        m_generalTrackArray[i].active = false;
    }

    /* nodeTrack initialize if DN is larger enough, the nodetrack will take up a lot space */
    for (int i = 0; i < m_instrArrayLen; i++) {
        m_instrArray[i].tracks = (Track*)palloc0(m_nodeTrackNum * sizeof(Track));
        for (int j = 0; j < m_nodeTrackNum; j++) {
            m_instrArray[i].tracks[j].track_time.need_timer = false;
            m_instrArray[i].tracks[j].active = false;
        }
    }
}

/*
 * threadinstrumentation is not pfree
 * because the space is universally released by globalstreaminstrumentaion
 */
ThreadInstrumentation::~ThreadInstrumentation()
{
    if (m_instrArrayMap) {
        pfree_ext(m_instrArrayMap);
        m_instrArrayMap = NULL;
    }

    if (m_instrArray) {
        pfree_ext(m_instrArray);
        m_instrArray = NULL;
    }

    if (m_generalTrackArray) {
        pfree_ext(m_generalTrackArray);
        m_generalTrackArray = NULL;
    }

    m_queryString = NULL;
}

/*
 * @Description: ThreadInstrumentation return slot for every plan_node_id
 * 				 only for plannode above GATHER in CN
 *				 we obtain plannode info to prepare for writing csv.
 * @in planNodeId: plan_node_id
 * @in parentId:parent node id
 * @in plan: plan
 * @in estate: Estate
 * @return: Instrumentatin
 */
Instrumentation* ThreadInstrumentation::allocInstrSlot(int plan_node_id, int parent_id, Plan* plan, struct EState* estate)
{
    char* pname = NULL;
    char* relname = NULL;
    RemoteQuery* rq = NULL;
    errno_t rc = 0;
    MemoryContext tmp_context;
    int i = 0;
    NodeInstr* node_instr = NULL;

    /*
     * if allocInstrSlot exec on CN or on compute pool, switch context to m_instrDataContext
     * else switch context to streamRuntimeContext
     */
    if ((IS_SPQ_COORDINATOR || IS_PGXC_COORDINATOR) &&
	u_sess->instr_cxt.global_instr)
        tmp_context = u_sess->instr_cxt.global_instr->getInstrDataContext();
    else
        tmp_context = MemoryContextOriginal((char*)u_sess->instr_cxt.global_instr);
    AutoContextSwitch streamCxtGuard(tmp_context);

    /* find if planNodeId has exists */
    for (i = 0; i < m_instrArrayAllocIndx; i++) {
        node_instr = &m_instrArray[i];
        if (node_instr->planNodeId == plan_node_id)
            break;
    }

    /* if not exists,let m_instrArrayAllocIndx++ */
    if (m_instrArrayAllocIndx == i) {
        node_instr = &m_instrArray[m_instrArrayAllocIndx];
        m_instrArrayMap[plan_node_id - 1] = m_instrArrayAllocIndx;
        m_instrArrayAllocIndx++;
        node_instr->planNodeId = plan_node_id;
        node_instr->planParentId = parent_id;
    }

    Assert(plan_node_id > 0);
    Assert(plan_node_id <= m_instrArrayMapLen);
    Assert(m_instrArrayAllocIndx <= m_instrArrayLen);

    /* ready for thread write file */
    int plan_type = 0;
    switch (nodeTag(plan)) {
        case T_BaseResult:
            pname = "Result";
            plan_type = UTILITY_OP;
            break;
        case T_ProjectSet: {
            pname = "ProjectSet";
            plan_type = IO_OP;
            break;
        }
        case T_VecResult:
            pname = "Vector Result";
            plan_type = UTILITY_OP;
            break;
        case T_ModifyTable:
            plan_type = IO_OP;
            switch (((ModifyTable*)plan)->operation) {
                case CMD_INSERT:
                    pname = "Insert";
                    break;
                case CMD_UPDATE:
                    pname = "Update";
                    break;
                case CMD_DELETE:
                    pname = "Delete";
                    break;
                default:
                    pname = "?\?\?";
                    break;
            }
            break;
        case T_Append:
            plan_type = UTILITY_OP;
            pname = "Append";
            break;
        case T_MergeAppend:
            pname = "Merge Append";
            plan_type = UTILITY_OP;
            break;
        case T_RecursiveUnion:
            pname = "Recursive Union";
            plan_type = UTILITY_OP;
            break;
        case T_StartWithOp:
            pname = "StartWithOp";
            plan_type = UTILITY_OP;
            break;
        case T_BitmapAnd:
            pname = "BitmapAnd";
            plan_type = UTILITY_OP;
            break;
        case T_BitmapOr:
            pname = "BitmapOr";
            plan_type = UTILITY_OP;
            break;
        case T_NestLoop:
            pname = "Nested Loop";
            plan_type = JOIN_OP;
            break;
        case T_VecNestLoop:
            pname = "Vector Nest Loop";
            plan_type = JOIN_OP;
            break;
        case T_MergeJoin:
            pname = "Merge Join"; /* "Join" gets added by jointype switch */
            plan_type = JOIN_OP;
            break;
        case T_HashJoin:
            pname = "Hash Join"; /* "Join" gets added by jointype switch */
            plan_type = HASHJOIN_OP;
            break;
        case T_VecHashJoin:
            plan_type = HASHJOIN_OP;
            switch (((Join*)plan)->jointype) {
                case JOIN_INNER:
                    pname = "Vector Hash Join Inner";
                    break;
                case JOIN_LEFT:
                    pname = "Vector Hash Join Left";
                    break;
                case JOIN_FULL:
                    pname = "Vector Hash Join Full";
                    break;
                case JOIN_RIGHT:
                    pname = "Vector Hash Join Right";
                    break;
                case JOIN_SEMI:
                    pname = "Vector Hash Join Semi";
                    break;
                case JOIN_ANTI:
                    pname = "Vector Hash Join Anti";
                    break;
                case JOIN_LEFT_ANTI_FULL:
                    pname = "Vector Hash Join Left Anti Full";
                    break;
                case JOIN_RIGHT_ANTI_FULL:
                    pname = "Vector Hash Join Right Anti Full";
                    break;
                default:
                    pname = "Vector Hash Join ?\?\?";
                    break;
            }
            break;
#ifdef USE_SPQ
        case T_SpqSeqScan:
            if (!((Scan*)plan)->tablesample) {
                if (((Scan*)plan)->isPartTbl) {
                    pname = "Partitioned Seq Scan";
                } else {
                    pname = "Spq Seq Scan";
                }
            } else {
                if (((Scan*)plan)->isPartTbl) {
                    pname = "Partitioned Sample Scan";
                } else {
                    pname = "Spq Sample Scan";
                }
            }
            plan_type = IO_OP;
            break;
        case T_AssertOp:
            pname = "Assert";
            plan_type = UTILITY_OP;
            break;
        case T_ShareInputScan:
            pname = "ShareInputScan";
            plan_type = UTILITY_OP;
            break;
        case T_Sequence:
            pname = "Sequence";
            plan_type = UTILITY_OP;
            break;
        case T_SpqIndexScan:
            if (((IndexScan*)plan)->scan.isPartTbl)
                pname = "Partitioned Index Scan";
            else
                pname = "Spq Index Scan";
            plan_type = IO_OP;
            break;
        case T_SpqIndexOnlyScan:
            if (((IndexOnlyScan*)plan)->scan.isPartTbl)
                pname = "Partitioned Index Only Scan";
            else
                pname = "Spq Index Only Scan";
            plan_type = IO_OP;
            break;
        case T_SpqBitmapHeapScan:
            if (((Scan*)plan)->isPartTbl)
                pname = "Partitioned Bitmap Heap Scan";
            else
                pname = "Spq Bitmap Heap Scan";
            plan_type = IO_OP;
            break;
#endif
        case T_SeqScan:
            if (!((Scan*)plan)->tablesample) {
                if (((Scan*)plan)->isPartTbl) {
                    pname = "Partitioned Seq Scan";
                } else {
                    pname = "Seq Scan";
                }
            } else {
                if (((Scan*)plan)->isPartTbl) {
                    pname = "Partitioned Sample Scan";
                } else {
                    pname = "Sample Scan";
                }
            }

            plan_type = IO_OP;
            break;
        case T_CStoreScan:
            if (!((Scan*)plan)->tablesample) {
                if (((Scan*)plan)->isPartTbl) {
                    pname = "Partitioned CStore Scan";
                } else {
                    pname = "CStore Scan";
                }
            } else {
                if (((Scan*)plan)->isPartTbl) {
                    pname = "Partitioned VecSample Scan";
                } else {
                    pname = "VecSample Scan";
                }
            }
            plan_type = IO_OP;
            break;
#ifdef ENABLE_MULTIPLE_NODES
        case T_TsStoreScan:
            if (!((Scan*)plan)->tablesample) {
                pname = "Partitioned TsStore Scan";
            } else {
                pname = "Partitioned TsSore Sample Scan";
            }
            plan_type = IO_OP;
            break;
#endif   /* ENABLE_MULTIPLE_NODES */
        case T_IndexScan:
            if (((IndexScan*)plan)->scan.isPartTbl)
                pname = "Partitioned Index Scan";
            else
                pname = "Index Scan";
            plan_type = IO_OP;
            break;
        case T_IndexOnlyScan:
            if (((IndexOnlyScan*)plan)->scan.isPartTbl)
                pname = "Partitioned Index Only Scan";
            else
                pname = "Index Only Scan";
            plan_type = IO_OP;
            break;
        case T_BitmapIndexScan:
            if (((BitmapIndexScan*)plan)->scan.isPartTbl)
                pname = "Partitioned Bitmap Index Scan";
            else
                pname = "Bitmap Index Scan";
            plan_type = IO_OP;
            break;
        case T_BitmapHeapScan:
            if (((Scan*)plan)->isPartTbl)
                pname = "Partitioned Bitmap Heap Scan";
            else
                pname = "Bitmap Heap Scan";
            plan_type = IO_OP;
            break;
        case T_CStoreIndexScan:
            if (((CStoreIndexScan*)plan)->scan.isPartTbl) {
                if (((CStoreIndexScan*)plan)->indexonly)
                    pname = "Partitioned CStore Index Only Scan";
                else
                    pname = "Partitioned CStore Index Scan";
            } else {
                if (((CStoreIndexScan*)plan)->indexonly)
                    pname = "CStore Index Only Scan";
                else
                    pname = "CStore Index Scan";
            }
            plan_type = IO_OP;
            break;
        case T_CStoreIndexCtidScan:
            if (((CStoreIndexCtidScan*)plan)->scan.isPartTbl)
                pname = "Partitioned CStore Index Ctid Scan";
            else
                pname = "CStore Index Ctid Scan";
            plan_type = IO_OP;
            break;
        case T_CStoreIndexHeapScan:
            if (((CStoreIndexHeapScan*)plan)->scan.isPartTbl)
                pname = "Partitioned CStore Index Heap Scan";
            else
                pname = "CStore Index Heap Scan";
            plan_type = IO_OP;
            break;
        case T_CStoreIndexAnd:
            pname = "CStore Index And";
            plan_type = IO_OP;
            break;
        case T_CStoreIndexOr:
            pname = "CStore Index Or";
            plan_type = IO_OP;
            break;
        case T_TidScan:
            if (((Scan*)plan)->isPartTbl)
                pname = "Partitioned Tid Scan";
            else
                pname = "Tid Scan";
            plan_type = IO_OP;
            break;
        case T_SubqueryScan:
            pname = "Subquery Scan";
            plan_type = UTILITY_OP;
            break;
        case T_VecSubqueryScan:
            pname = "Vector Subquery Scan";
            plan_type = UTILITY_OP;
            break;
        case T_FunctionScan:
            pname = "Function Scan";
            plan_type = UTILITY_OP;
            break;
        case T_ValuesScan:
            pname = "Values Scan";
            plan_type = UTILITY_OP;
            break;
        case T_CteScan:
            pname = "CTE Scan";
            plan_type = UTILITY_OP;
            break;
        case T_WorkTableScan:
            pname = "WorkTable Scan";
            plan_type = UTILITY_OP;
            break;
        case T_RemoteQuery:
            rq = (RemoteQuery*)plan;
            if (rq->position == PLAN_ROUTER)
                pname = "Streaming (type: PLAN ROUTER)";
            else if (rq->position == SCAN_GATHER)
                pname = "Streaming (type: SCAN GATHER)";
            else {
                if (rq->is_simple)
                    pname = "Streaming (type: GATHER)";
                else
                    pname = "Data Node Scan";
            }
            plan_type = NET_OP;
            break;
        case T_VecRemoteQuery:
            rq = (RemoteQuery*)plan;
            Assert(rq->is_simple);
            if (rq->position == PLAN_ROUTER)
                pname = "Vector Streaming (type: PLAN ROUTER)";
            else if (rq->position == SCAN_GATHER)
                pname = "Vector Streaming (type: SCAN GATHER)";
            else
                pname = "Vector Streaming (type: GATHER)";
            plan_type = NET_OP;
            break;
        case T_Stream:
        case T_VecStream:
            pname = (char*)GetStreamType((Stream*)plan);
            plan_type = NET_OP;
            break;

        case T_ForeignScan:
            if (((Scan*)plan)->isPartTbl) {
                /* @hdfs
                 * Add hdfs partitioned foreign scan plan explanation.
                 */
                pname = "Partitioned Foreign Scan";
            } else
                pname = "Foreign Scan";
            plan_type = IO_OP;
            break;
        case T_VecForeignScan:
            if (((Scan*)plan)->isPartTbl) {
                /* @hdfs
                 * Add hdfs partitioned foreign scan plan explanation.
                 */
                pname = "Partitioned Vector Foreign Scan";
            } else
                pname = "Vector Foreign Scan";
            plan_type = IO_OP;
            break;
        case T_ExtensiblePlan:
            pname = "Extensible Plan";
            plan_type = UTILITY_OP;
            break;
        case T_Material:
            pname = "Materialize";
            plan_type = IO_OP;
            break;
        case T_VecMaterial:
            pname = "Vector Materialize";
            plan_type = IO_OP;
            break;
        case T_Sort:
            pname = "Sort";
            plan_type = SORT_OP;
            break;
        case T_VecSort:
            pname = "Vector Sort";
            plan_type = SORT_OP;
            break;
        case T_Group:
            pname = "Group";
            plan_type = UTILITY_OP;
            break;
        case T_VecGroup:
            pname = "Vector Group";
            plan_type = UTILITY_OP;
            break;
        case T_Agg:
            plan_type = UTILITY_OP;
            switch (((Agg*)plan)->aggstrategy) {
                case AGG_PLAIN:
                    pname = "Plain Aggregate";
                    break;
                case AGG_SORTED:
                    pname = "Sort Aggregate";
                    break;
                case AGG_HASHED:
                    pname = "Hash Aggregate";
                    plan_type = HASHAGG_OP;
                    break;
                default:
                    pname = "Aggregate ?\?\?";
                    break;
            }
            break;
        case T_VecAgg:
            plan_type = UTILITY_OP;
            switch (((Agg*)plan)->aggstrategy) {
                case AGG_PLAIN:
                    pname = "Vector Aggregate";
                    break;
                case AGG_HASHED:
                    pname = "Vector Hash Aggregate";
                    plan_type = HASHAGG_OP;
                    break;
                case AGG_SORTED:
                    pname = "Vector Sort Aggregate";
                    break;
                default:
                    pname = "Vector Aggregate ?\?\?";
                    break;
            }
            break;
        case T_WindowAgg:
            pname = "WindowAgg";
            plan_type = UTILITY_OP;
            break;
        case T_VecWindowAgg:
            pname = "Vector WindowAgg";
            plan_type = UTILITY_OP;
            break;
        case T_Unique:
            pname = "Unique";
            plan_type = UTILITY_OP;
            break;
        case T_VecUnique:
            pname = "Vector Unique";
            plan_type = UTILITY_OP;
            break;
        case T_SetOp:
            plan_type = UTILITY_OP;
            switch (((SetOp*)plan)->strategy) {
                case SETOP_SORTED:
                    pname = "Sort SetOp";
                    break;
                case SETOP_HASHED:
                    pname = "Hash SetOp";
                    plan_type = HASHAGG_OP;
                    break;
                default:
                    pname = "SetOp ?\?\?";
                    break;
            }
            break;
        case T_VecSetOp:
            plan_type = UTILITY_OP;
            switch (((VecSetOp*)plan)->strategy) {
                case SETOP_SORTED:
                    pname = "Vector Sort SetOp";
                    break;
                case SETOP_HASHED:
                    pname = "Vector Hash SetOp";
                    plan_type = HASHAGG_OP;
                    break;
                default:
                    pname = "Vector SetOp ?\?\?";
                    break;
            }
            break;
        case T_LockRows:
            pname = "LockRows";
            plan_type = UTILITY_OP;
            break;
        case T_Limit:
            pname = "Limit";
            plan_type = UTILITY_OP;
            break;
        case T_Hash:
            pname = "Hash";
            plan_type = HASHJOIN_OP;
            break;
        case T_PartIterator:
            pname = "Partition Iterator";
            plan_type = UTILITY_OP;
            break;
        case T_VecPartIterator:
            pname = "Vector Partition Iterator";
            plan_type = UTILITY_OP;
            break;
        case T_VecToRow:
            pname = "Row Adapter";
            plan_type = UTILITY_OP;
            break;
        case T_RowToVec:
            pname = "Vector Adapter";
            plan_type = UTILITY_OP;
            break;
        case T_VecAppend:
            pname = "Vector Append";
            plan_type = UTILITY_OP;
            break;
        case T_VecModifyTable:
            plan_type = IO_OP;
            switch (((ModifyTable*)plan)->operation) {
                case CMD_INSERT:
                    pname = "Vector Insert";
                    break;
                case CMD_UPDATE:
                    pname = "Vector Update";
                    break;
                case CMD_DELETE:
                    pname = "Vector Delete";
                    break;
                default:
                    pname = "?\?\?";
                    break;
            }
            break;
        case T_VecLimit:
            pname = "Vector Limit";
            plan_type = UTILITY_OP;
            break;
        case T_VecMergeJoin:
            pname = "Vector Merge Join";
            plan_type = UTILITY_OP;
            break;
        default:
            pname = "Unknown Operator";
            Assert(false);
            break;
    }

    /* Append rel name to the scan node */
    switch (nodeTag(plan)) {
        case T_SeqScan:
        case T_CStoreScan:
#ifdef ENABLE_MULTIPLE_NODES
        case T_TsStoreScan:
#endif   /* ENABLE_MULTIPLE_NODES */
        case T_IndexScan:
        case T_IndexOnlyScan:
        case T_BitmapHeapScan:
        case T_CStoreIndexScan:
        case T_CStoreIndexCtidScan:
        case T_CStoreIndexHeapScan:
        case T_TidScan:
        case T_ForeignScan:
        case T_VecForeignScan: {
            Index rti = ((Scan*)plan)->scanrelid;
            RangeTblEntry* rte = rt_fetch(rti, estate->es_range_table);
            /* Assert it's on a real relation */
            Assert(rte->rtekind == RTE_RELATION);
            relname = get_rel_name(rte->relid);
            break;
        }
        default:
            break;
    }
    if (relname != NULL) {
        int name_len = strlen(pname) + strlen(relname) + 5;
        node_instr->name = (char*)palloc0(name_len);
        rc = snprintf_s(node_instr->name, name_len, name_len - 1, "%s on %s", pname, relname);
        securec_check_ss(rc, "\0", "\0");
    } else {
        node_instr->name = (char*)palloc0(strlen(pname) + 1);
        rc = snprintf_s(node_instr->name, strlen(pname) + 1, strlen(pname), "%s", pname);
        securec_check_ss(rc, "\0", "\0");
    }

    if (estate->es_instrument & (INSTRUMENT_BUFFERS | INSTRUMENT_TIMER)) {
        bool need_buffers = (estate->es_instrument & INSTRUMENT_BUFFERS) != 0;
        bool need_timer = (estate->es_instrument & INSTRUMENT_TIMER) != 0;

        node_instr->instr.instruPlanData.need_timer = need_timer;
        node_instr->instr.instruPlanData.need_bufusage = need_buffers;
    }

    node_instr->instr.isValid = true;
    node_instr->planType = plan_type;

    if (plan_type == HASHAGG_OP)
        node_instr->planTypeStrIdx = 0;
    else if (plan_type == HASHJOIN_OP)
        node_instr->planTypeStrIdx = 1;
    else if (plan_type == SORT_OP)
        node_instr->planTypeStrIdx = 2;
    else if (plan_type == JOIN_OP)
        node_instr->planTypeStrIdx = 3;
    else if (plan_type == NET_OP)
        node_instr->planTypeStrIdx = 4;
    else if (plan_type == IO_OP)
        node_instr->planTypeStrIdx = 5;
    else if (plan_type == UTILITY_OP)
        node_instr->planTypeStrIdx = 6;

    return &node_instr->instr.instruPlanData;
}

void ThreadInstrumentation::startTrack(int plan_node_id, ThreadTrack instr_idx)
{
    Track* track = NULL;
    bool has_perf = CPUMon::m_has_perf;
    int m_option = u_sess->instr_cxt.global_instr->get_option();

    if (plan_node_id == -1) {
        /* generaltrack if plannodeid = -1 */
        Assert((int)instr_idx < m_generalTrackNum);
        track = &m_generalTrackArray[(int)instr_idx];
    } else if (m_instrArrayMap[plan_node_id - 1] == -1) {
        /* avoid for active sql */
        track = &m_instrArray[0].tracks[(int)instr_idx - m_generalTrackNum];
    } else {
        Assert(m_instrArrayMap[plan_node_id - 1] >= 0);
        track = &m_instrArray[m_instrArrayMap[plan_node_id - 1]].tracks[(int)instr_idx - m_generalTrackNum];
    }

    track->node_id = plan_node_id;
    track->active = true;
    track->registerTrackId = (int)instr_idx;

    track->track_time.need_timer = ((m_option & INSTRUMENT_TIMER) != 0);

    InstrStartStream(&track->track_time);

    if (has_perf) {
        CPUMon::ResetCounters(track->accumCounters.tempCounters);
        CPUMon::ReadCounters(track->accumCounters.tempCounters);
    }
}

void ThreadInstrumentation::endTrack(int plan_node_id, ThreadTrack instr_idx)
{
    Track* track = NULL;
    bool has_perf = CPUMon::m_has_perf;

    if (plan_node_id == -1) {
        /* generaltrack if plannodeid = -1 */
        Assert((int)instr_idx < m_generalTrackNum);
        track = &m_generalTrackArray[(int)instr_idx];
    } else if (m_instrArrayMap[plan_node_id - 1] == -1) {
        /* avoid for active sql */
        track = &m_instrArray[0].tracks[(int)instr_idx - m_generalTrackNum];
    } else {
        Assert(m_instrArrayMap[plan_node_id - 1] >= 0);
        track = &m_instrArray[m_instrArrayMap[plan_node_id - 1]].tracks[(int)instr_idx - m_generalTrackNum];
    }

    InstrStopStream(&track->track_time, 1.0);
    if (has_perf)
        CPUMon::AccumCounters(track->accumCounters.tempCounters, track->accumCounters.accumCounters);
}

void ThreadInstrumentation::setQueryString(char* qstr)
{
    m_queryString = qstr;
}

/*
 * @Description: StreamInstrumentation Constructor
 * @in size: DN node number
 * @in num_streams:all streams number
 * @in query_dop: query_dop
 * @in plan_size: plan node number
 * @in start_node_id: start node id in result plan
 * @in option: instrument option
 * @in trackoption: track option
 * @return: None
 */
StreamInstrumentation::StreamInstrumentation(int size, int num_streams, int gather_count, int query_dop, int plan_size,
    int start_node_id, int option, bool trackoption)
    : m_nodes_num(size),
      m_num_streams(num_streams),
      m_gather_count(gather_count),
      m_query_dop(query_dop),
      m_plannodes_num(plan_size),
      m_start_node_id(start_node_id),
      m_option(option),
      m_trackoption(trackoption)
{
    m_query_id = u_sess->debug_query_id;
    MemoryContext oldcontext = NULL;
    if (IS_SPQ_COORDINATOR || IS_PGXC_COORDINATOR){
        m_instrDataContext = AllocSetContextCreate(CurrentMemoryContext,
            "InstrDataContext",
            ALLOCSET_DEFAULT_MINSIZE,
            ALLOCSET_DEFAULT_INITSIZE,
            ALLOCSET_DEFAULT_MAXSIZE);
        oldcontext = MemoryContextSwitchTo(m_instrDataContext);
    } else {
        m_instrDataContext = NULL;
    }

     if (IS_SPQ_COORDINATOR || IS_PGXC_COORDINATOR){
        /* adopt for a lot gather operator */
        m_threadInstrArrayLen = m_nodes_num * ((m_num_streams + m_gather_count) * m_query_dop) + 1;

        /* including thr top consumer list and cn */
        m_streamInfo = (ThreadInstrInfo*)palloc0(sizeof(ThreadInstrInfo) * (m_num_streams + m_gather_count + 1));
    } else {
#if defined(ENABLE_MULTIPLE_NODES) || defined(USE_SPQ)
         if (t_thrd.spq_ctx.spq_role != ROLE_UTILITY) {
             /*
              * in DN, m_gather_count is 1 in general(gather operator)
              * in compute pool, m_gather_count = 3 actually.
              */
             m_threadInstrArrayLen = (m_num_streams + m_gather_count) * m_query_dop;
             /* including the top consumer list. */
             m_streamInfo = (ThreadInstrInfo*)palloc0(sizeof(ThreadInstrInfo) * (m_num_streams + m_gather_count));
         } else {
             m_threadInstrArrayLen = (m_num_streams + m_gather_count + 1) * m_query_dop;
             m_streamInfo = (ThreadInstrInfo*)palloc0(sizeof(ThreadInstrInfo) * (m_num_streams + m_gather_count + 1));
         }
#else
        /* single node need a lot gather operator */
        m_threadInstrArrayLen = (m_num_streams + m_gather_count + 1) * m_query_dop;
        m_streamInfo = (ThreadInstrInfo*)palloc0(sizeof(ThreadInstrInfo) * (m_num_streams + m_gather_count + 1));
#endif

    }

    /* streamInfo INIT */
    m_streamInfoIdx = 0;
    m_streamInfo[0].segmentId = m_start_node_id;
    m_streamInfo[0].offset = 0;
    m_streamInfo[0].numOfNodes = 0;

    m_node_dops = (int*)palloc0(sizeof(int) * (m_plannodes_num - (m_start_node_id - 1)));

    /* planId offset in  threadinstrArray */
    m_planIdOffsetArray = (int*)palloc0(sizeof(int) * m_plannodes_num);

    /* alloc Threadinstrumentation pointer space */
    m_threadInstrArray = (ThreadInstrumentation**)palloc0(sizeof(ThreadInstrumentation*) * m_threadInstrArrayLen);

    for (int i = 0; i < m_threadInstrArrayLen; i++) {
        m_threadInstrArray[i] = NULL;
    }

    if (IS_SPQ_COORDINATOR || IS_PGXC_COORDINATOR) {
        MemoryContextSwitchTo(oldcontext);
    }

    if (u_sess->instr_cxt.perf_monitor_enable) /* Don't use perf util you set has_use_perf = true */
        CPUMon::Initialize(CMON_GENERAL);
}

StreamInstrumentation::~StreamInstrumentation()
{
    if (CPUMon::m_has_perf)
        CPUMon::Shutdown();

    if (IS_SPQ_COORDINATOR || IS_PGXC_COORDINATOR) {
        Assert(m_instrDataContext != NULL);

        if (m_instrDataContext) {
            MemoryContextDelete(m_instrDataContext);
            m_instrDataContext = NULL;
        }
        m_planIdOffsetArray = NULL;
        m_streamInfo = NULL;
        m_threadInstrArray = NULL;
        return;
    }

    /* clean up in DN */
    for (int i = 0; i < m_threadInstrArrayLen; i++) {
        if (m_threadInstrArray[i]) {
            pfree_ext(m_threadInstrArray[i]);
            m_threadInstrArray[i] = NULL;
        }
    }
    pfree_ext(m_threadInstrArray);

    if (m_planIdOffsetArray) {
        pfree_ext(m_planIdOffsetArray);
        m_planIdOffsetArray = NULL;
    }

    if (m_streamInfo) {
        pfree_ext(m_streamInfo);
        m_streamInfo = NULL;
    }

    if (m_node_dops) {
        pfree_ext(m_node_dops);
        m_node_dops = NULL;
    }
}

/* init streaminstrumentation on DN */
StreamInstrumentation* StreamInstrumentation::InitOnDn(void* desc, int dop)
{
    QueryDesc* query_desc = (QueryDesc*)desc;

    StreamInstrumentation* instr = New(CurrentMemoryContext) StreamInstrumentation(1,
        query_desc->plannedstmt->num_streams,
        query_desc->plannedstmt->gather_count,
        query_desc->plannedstmt->query_dop,
        query_desc->plannedstmt->num_plannodes,
        query_desc->plannedstmt->planTree->plan_node_id,
        query_desc->instrument_options,
        true);

    instr->getStreamInfo(query_desc->plannedstmt->planTree, query_desc->plannedstmt, dop, &instr->m_streamInfo[0], 0);

    /* allocate threadinstrumentation in DN */
    instr->allocateAllThreadInstrOnDN(query_desc->plannedstmt->in_compute_pool);

    return instr;
}

/* init streaminstrumentation on Compute pool */
StreamInstrumentation* StreamInstrumentation::InitOnCP(void* desc, int dop)
{
    QueryDesc* query_desc = (QueryDesc*)desc;

    StreamInstrumentation* instr = New(CurrentMemoryContext) StreamInstrumentation(u_sess->pgxc_cxt.NumDataNodes,
        query_desc->plannedstmt->num_streams,
        query_desc->plannedstmt->gather_count,
        query_desc->plannedstmt->query_dop,
        query_desc->plannedstmt->num_plannodes,
        query_desc->plannedstmt->planTree->plan_node_id,
        query_desc->instrument_options,
        true);

    MemoryContext old_context = MemoryContextSwitchTo(instr->getInstrDataContext());

    instr->getStreamInfo(query_desc->plannedstmt->planTree, query_desc->plannedstmt, dop, &instr->m_streamInfo[0], 0);

    /* allocate threadinstrumentation in compute pool */
    instr->allocateAllThreadInstrOnCP(dop);

    MemoryContextSwitchTo(old_context);

    return instr;
}

/* init streaminstrumentation on CN */
StreamInstrumentation* StreamInstrumentation::InitOnCn(void* desc, int dop)
{
    QueryDesc* query_desc = (QueryDesc*)desc;

    StreamInstrumentation* instr = New(CurrentMemoryContext) StreamInstrumentation(query_desc->plannedstmt->num_nodes,
        query_desc->plannedstmt->num_streams,
        query_desc->plannedstmt->gather_count,
        query_desc->plannedstmt->query_dop,
        query_desc->plannedstmt->num_plannodes,
        1,
        query_desc->instrument_options,
        true);

    MemoryContext old_context = MemoryContextSwitchTo(instr->getInstrDataContext());

    instr->getStreamInfo(query_desc->plannedstmt->planTree, query_desc->plannedstmt, dop, &instr->m_streamInfo[0], 0);

    /* allocate threadinstrumentation in CN */
    instr->allocateAllThreadInstrOnCN();

    MemoryContextSwitchTo(old_context);

    return instr;
}

/*
 * @Description: traverse the plan tree to get information include m_planIdOffsetArray,m_node_dops and m_streamInfo,
 *				  m_streamInfo is differentiate by thread when the node is stream and remote query, m_streaminfoidx ++,
 *				  m_streamInfo record the nodenums, segmentid, offset of thread,
 *               m_planIdOffsetArray record absolute offset of plannodeid in all thread.
 * @param[IN] result_plan:  top node of a plan tree or sub plan tree
 * @param[IN] planned_stmt:  holds the "one time" information needed by the executor
 * @param[IN] dop:  dop of current query which comes from CN
 * @param[IN] info: ThreadInstrInfo include numofNodes segmentId offset
 *			 when plannode is remotequery and stream,streamInfoIdx++
 * @param[IN] offset: nodeid offset in m_planIdArray
 * @return: void
 */
void StreamInstrumentation::getStreamInfo(
    Plan* result_plan, PlannedStmt* planned_stmt, int dop, ThreadInstrInfo* info, int offset)
{
    int node_id = result_plan->plan_node_id;
    int query_dop = result_plan->parallel_enabled ? dop : 1;

    /* record node dop in every plannodeid */
    m_node_dops[node_id - m_start_node_id] = query_dop;

    m_planIdOffsetArray[node_id - 1] = offset;

    info->numOfNodes++;

    switch (nodeTag(result_plan)) {
        case T_MergeAppend: {
            MergeAppend* ma = (MergeAppend*)result_plan;
            ListCell* lc = NULL;
            foreach (lc, ma->mergeplans) {
                Plan* plan = (Plan*)lfirst(lc);
                getStreamInfo(plan, planned_stmt, dop, info, offset);
            }
        } break;
        case T_Append:
        case T_VecAppend: {
            Append* append = (Append*)result_plan;
            ListCell* lc = NULL;
            foreach (lc, append->appendplans) {
                Plan* plan = (Plan*)lfirst(lc);
                getStreamInfo(plan, planned_stmt, dop, info, offset);
            }
        } break;
        case T_ModifyTable:
        case T_VecModifyTable: {
            ModifyTable* mt = (ModifyTable*)result_plan;
            ListCell* lc = NULL;
            foreach (lc, mt->plans) {
                Plan* plan = (Plan*)lfirst(lc);
                getStreamInfo(plan, planned_stmt, dop, info, offset);
            }
        } break;
        case T_SubqueryScan:
        case T_VecSubqueryScan: {
            SubqueryScan* ss = (SubqueryScan*)result_plan;
            if (ss->subplan)
                getStreamInfo(ss->subplan, planned_stmt, dop, info, offset);
        } break;
        case T_BitmapAnd:
        case T_CStoreIndexAnd: {
            BitmapAnd* ba = (BitmapAnd*)result_plan;
            ListCell* lc = NULL;
            foreach (lc, ba->bitmapplans) {
                Plan* plan = (Plan*)lfirst(lc);
                getStreamInfo(plan, planned_stmt, dop, info, offset);
            }
        } break;
        case T_BitmapOr:
        case T_CStoreIndexOr: {
            BitmapOr* bo = (BitmapOr*)result_plan;
            ListCell* lc = NULL;
            foreach (lc, bo->bitmapplans) {
                Plan* plan = (Plan*)lfirst(lc);
                getStreamInfo(plan, planned_stmt, dop, info, offset);
            }
        } break;
        case T_Stream:
        case T_VecStream: {
            /* record the left tree plan id. */
            m_streamInfoIdx++;
            m_streamInfo[m_streamInfoIdx].segmentId = result_plan->lefttree->plan_node_id;
            m_streamInfo[m_streamInfoIdx].offset = m_streamInfoIdx;
            m_streamInfo[m_streamInfoIdx].numOfNodes = 0;

            Assert(m_streamInfoIdx <= m_num_streams + m_gather_count);
            if (m_streamInfoIdx > m_num_streams + m_gather_count)
                ereport(ERROR,
                    (errcode(ERRCODE_UNEXPECTED_NODE_STATE),
                        errmsg("streaminfo space is not enough because STREAM NUMBER : %d + GATHER NUMBER : %d < "
                               "streamInfoIdx : %d",
                            m_num_streams,
                            m_gather_count,
                            m_streamInfoIdx)));
            getStreamInfo(result_plan->lefttree,
                planned_stmt,
                dop,
                &m_streamInfo[m_streamInfoIdx],
                m_streamInfo[m_streamInfoIdx].offset);
        } break;
        /* stream Gather */
        case T_RemoteQuery:
        case T_VecRemoteQuery: {
            /* record the left tree plan id. */
            m_streamInfoIdx++;
            m_streamInfo[m_streamInfoIdx].segmentId = result_plan->lefttree->plan_node_id;
            m_streamInfo[m_streamInfoIdx].offset = m_streamInfoIdx;
            m_streamInfo[m_streamInfoIdx].numOfNodes = 0;

            Assert(m_streamInfoIdx <= m_num_streams + m_gather_count);
            if (m_streamInfoIdx > m_num_streams + m_gather_count)
                ereport(ERROR,
                    (errcode(ERRCODE_UNEXPECTED_NODE_STATE),
                        errmsg("streaminfo space is not enough because STREAM NUMBER : %d + GATHER NUMBER : %d < "
                               "streamInfoIdx : %d",
                            m_num_streams,
                            m_gather_count,
                            m_streamInfoIdx)));
            getStreamInfo(result_plan->lefttree,
                planned_stmt,
                dop,
                &m_streamInfo[m_streamInfoIdx],
                m_streamInfo[m_streamInfoIdx].offset);
        } break;
        case T_ExtensiblePlan: {
            ExtensiblePlan* eplan = (ExtensiblePlan*)result_plan;
            ListCell* lc = NULL;
            foreach (lc, eplan->extensible_plans) {
                Plan* plan = (Plan*)lfirst(lc);
                getStreamInfo(plan, planned_stmt, dop, info, offset);
            }
        } break;
#ifdef USE_SPQ
        case T_Sequence: {
            Sequence* sequence = (Sequence*)result_plan;
            ListCell* lc = NULL;
            foreach (lc, sequence->subplans) {
                Plan* plan = (Plan*)lfirst(lc);
                getStreamInfo(plan, planned_stmt, dop, info, offset);
            }
            break;
        }
#endif /* USE_SPQ */

        default:
            if (result_plan->lefttree)
                getStreamInfo(result_plan->lefttree, planned_stmt, dop, info, offset);
            if (result_plan->righttree)
                getStreamInfo(result_plan->righttree, planned_stmt, dop, info, offset);
            break;
    }

    if (planned_stmt && planned_stmt->subplans) {
        ListCell* lst = NULL;
        foreach (lst, planned_stmt->subplans) {
            Plan* sub_plan = (Plan*)lfirst(lst);
            if (NULL == sub_plan)
                continue;

            /* subplan' plannode id should add to its parentnodeid. */
            if (result_plan->plan_node_id == sub_plan->parent_node_id) {
#ifdef ENABLE_MULTIPLE_NODES
                /* CN, allocate memory for all plan nodes. */
                if (IS_PGXC_COORDINATOR)
                    getStreamInfo(sub_plan, planned_stmt, dop, info, offset);
                /* DN, allocate memory just for plan nodes executed on DN. */
                if (IS_PGXC_DATANODE &&
                    (sub_plan->exec_type == EXEC_ON_DATANODES || sub_plan->exec_type == EXEC_ON_ALL_NODES)) {
                    getStreamInfo(sub_plan, planned_stmt, dop, info, offset);
                }
#else
                /* single node, allocate memory for all plan nodes */
                getStreamInfo(sub_plan, planned_stmt, dop, info, offset);
#endif
            }
        }
    }
}

/* connect ThreadInstrumentation and globalstreamInstrumentation->m_threadInstrArray */
ThreadInstrumentation* StreamInstrumentation::allocThreadInstrumentation(int segment_id)
{
    ThreadInstrInfo* info = NULL;
    int stream_info_idx = m_streamInfoIdx + 1;
    for (int i = 0; i < stream_info_idx; i++) {
        if (m_streamInfo[i].segmentId == segment_id) {
            info = &m_streamInfo[i];
            break;
        }
    }

    Assert(info != NULL);
    Assert((uint32)u_sess->stream_cxt.smp_id < (uint32)m_query_dop);
    if ((uint32)u_sess->stream_cxt.smp_id >= (uint32)m_query_dop || (uint32)u_sess->stream_cxt.smp_id < 0) {
        ereport(ERROR,
            (errcode(ERRCODE_DOP_VALUE_OUT_OF_RANGE),
                errmsg("query dop is out of range: %u [0-%d]", u_sess->stream_cxt.smp_id, m_query_dop - 1)));
    }

    return m_threadInstrArray[info->offset * m_query_dop + u_sess->stream_cxt.smp_id];
}

/* alloc threadInstrumentation in DN in need */
void StreamInstrumentation::allocateAllThreadInstrOnDN(bool in_compute_pool)
{
    int stream_info_idx = m_streamInfoIdx + 1;
    if (in_compute_pool) {
        /* in compute pool, dop always is 1 */
        for (int i = 0; i < stream_info_idx; i++) {
            m_threadInstrArray[i] = New(CurrentMemoryContext) ThreadInstrumentation(
                m_query_id, m_streamInfo[i].segmentId, m_streamInfo[i].numOfNodes, m_plannodes_num);
        }
    } else {
        for (int i = 0; i < stream_info_idx; i++) {
            int node_dop = m_node_dops[m_streamInfo[i].segmentId - m_start_node_id];
            Assert(node_dop <= m_query_dop && node_dop > 0);
            for (int j = 0; j < node_dop; j++) {
                m_threadInstrArray[i * m_query_dop + j] = New(CurrentMemoryContext) ThreadInstrumentation(
                    m_query_id, m_streamInfo[i].segmentId, m_streamInfo[i].numOfNodes, m_plannodes_num);
            }
        }
    }
}

/* alloc threadInstrumentation in Compute pool in need */
void StreamInstrumentation::allocateAllThreadInstrOnCP(int dop)
{
    int i, j;
    Assert(dop == 1);
    int dn_num_streams = DN_NUM_STREAMS_IN_CN(m_num_streams, m_gather_count, dop);
    /* threadinstrumentation on compute pool CN */
    m_threadInstrArray[0] = New(CurrentMemoryContext)
        ThreadInstrumentation(m_query_id, m_streamInfo[0].segmentId, m_streamInfo[0].numOfNodes, m_plannodes_num);
    /* threadinstrumentation on all compute pool DN */
    for (i = 0; i < u_sess->pgxc_cxt.NumDataNodes; i++) {
        for (j = 0; j < m_streamInfoIdx; j++) {
            m_threadInstrArray[1 + i * dn_num_streams + j] = New(CurrentMemoryContext) ThreadInstrumentation(
                m_query_id, m_streamInfo[j + 1].segmentId, m_streamInfo[j + 1].numOfNodes, m_plannodes_num);
        }
    }
}

/* alloc threadInstrumentation in CN in need */
void StreamInstrumentation::allocateAllThreadInstrOnCN()
{
    /* threadinstrumentation on CN */
    m_threadInstrArray[0] = New(CurrentMemoryContext)
        ThreadInstrumentation(m_query_id, m_streamInfo[0].segmentId, m_streamInfo[0].numOfNodes, m_plannodes_num);

    /* threadinstrumentation on DN is allocated in deserialize function in need. */
}

/* send instrumentation */
void StreamInstrumentation::serializeSend()
{
    StringInfoData buf;

    if (m_query_dop == 0) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("m_query_dop can not be zero")));
    }

    for (int i = 0; i < m_threadInstrArrayLen; i++) {
        ThreadInstrumentation* thread_instr = m_threadInstrArray[i];
        if (thread_instr != NULL) {
            int array_len = thread_instr->m_instrArrayLen;
            int* m_instr_array_map = thread_instr->m_instrArrayMap;
            int m_instr_array_map_len = thread_instr->m_instrArrayMapLen;
            Assert(m_query_dop != 0);
            int smp_id = i % m_query_dop;
            Assert(m_instr_array_map_len == m_plannodes_num);

            for (int j = 0; j < array_len; j++) {
                InstrStreamPlanData* instr = &thread_instr->m_instrArray[j].instr;
                int node_id = thread_instr->m_instrArray[j].planNodeId;
                Assert(instr != NULL);

                if (instr->isValid == true) {
                    pq_beginmessage(&buf, 'U');
                    pq_sendint64(&buf, u_sess->debug_query_id);
                    pq_sendint32(&buf, node_id);
                    pq_sendint32(&buf, smp_id);
                    pq_sendint32(&buf, m_instr_array_map_len);
                    for (int k = 0; k < m_instr_array_map_len; k++) {
                        pq_sendint32(&buf, m_instr_array_map[k]);
                    }
                    pq_sendbytes(&buf, (char*)instr, sizeof(InstrStreamPlanData));
                    pq_endmessage(&buf);
                }
            }
        }
    }
}

// deserialize the data, and put in the specific slot. -1 means on DWS CN, else on DWS DN
void StreamInstrumentation::deserialize(int idx, char* msg, size_t len, bool operator_statitics, int cur_smp_id)
{
    errno_t rc = EOK;
    uint64 query_id = 0;
    int node_id = 0;
    int smp_id = 0;
    int m_instr_array_map_len = 0;
    int* m_instr_array_map = NULL;
    MemoryContext tmp_context;

    rc = memcpy_s(&query_id, sizeof(uint64), msg, sizeof(uint64));
    securec_check(rc, "\0", "\0");
    query_id = ntohl64(query_id);
    msg += 8;

#ifndef USE_SPQ
    if (!operator_statitics) {
        Assert(query_id == (uint64)u_sess->debug_query_id);
        if (query_id != (uint64)u_sess->debug_query_id) {
            ereport(ERROR,
                (errcode(ERRCODE_STREAM_CONNECTION_RESET_BY_PEER),
                    errmsg("Expecting messages of query: %lu, but received messages of query: %lu",
                        u_sess->debug_query_id,
                        query_id)));
        }
    }
#endif

    rc = memcpy_s(&node_id, sizeof(int), msg, sizeof(int));
    securec_check(rc, "\0", "\0");
    node_id = ntohl(node_id);
    msg += 4;

    rc = memcpy_s(&smp_id, sizeof(int), msg, sizeof(int));
    securec_check(rc, "\0", "\0");
    smp_id = ntohl(smp_id);
    msg += 4;

    if (cur_smp_id != -1)
        smp_id = cur_smp_id;

    rc = memcpy_s(&m_instr_array_map_len, sizeof(int), msg, sizeof(int));
    securec_check(rc, "\0", "\0");
    m_instr_array_map_len = ntohl(m_instr_array_map_len);
    msg += 4;

    /*
     * offset means plan node id offset in all streams,
     * dn_num_streams means all streams in one DN,
     * (idx, offset, smpid) can return only slot in streaminstrumentation of CN.
     * m_query_dop = 1 in compute pool.
     */
    Assert(node_id > 0);
    int offset = m_planIdOffsetArray[node_id - 1] == 0 ? -1 : (m_planIdOffsetArray[node_id - 1] - 1) * m_query_dop;
    int streaminfoidx = m_planIdOffsetArray[node_id - 1];
    int dn_num_streams = DN_NUM_STREAMS_IN_CN(m_num_streams, m_gather_count, m_query_dop);
    int slot = 1 + idx * dn_num_streams + offset + smp_id;
    /* adopt for compute pool */
    if (!IS_SPQ_COORDINATOR && IS_PGXC_DATANODE) {
        int plan_id_offset =
            m_planIdOffsetArray[node_id - 1] == 0 ? -1 : (m_planIdOffsetArray[node_id - 1] * m_query_dop);
        slot = plan_id_offset + smp_id;
    }

    /* allocate threadinstrumentation in CN if receive from DN. */
    if (m_threadInstrArray[slot] == NULL) {
	    if (!IS_SPQ_EXECUTOR || IS_PGXC_DATANODE)
            tmp_context = MemoryContextOriginal((char*)u_sess->instr_cxt.global_instr);
        else
            tmp_context = u_sess->instr_cxt.global_instr->getInstrDataContext();
        AutoContextSwitch csv(tmp_context);
        m_threadInstrArray[slot] = New(tmp_context) ThreadInstrumentation(
            query_id, m_streamInfo[streaminfoidx].segmentId, m_streamInfo[streaminfoidx].numOfNodes, m_plannodes_num);
    }

    ThreadInstrumentation* thread_instr = m_threadInstrArray[slot];
    m_instr_array_map = thread_instr->m_instrArrayMap;

    for (int i = 0; i < m_instr_array_map_len; i++) {
        rc = memcpy_s(&m_instr_array_map[i], sizeof(int), msg, sizeof(int));
        securec_check(rc, "\0", "\0");
        m_instr_array_map[i] = ntohl(m_instr_array_map[i]);
        msg += 4;
    }

    InstrStreamPlanData* instr = &thread_instr->m_instrArray[m_instr_array_map[node_id - 1]].instr;

    Assert(m_instr_array_map[node_id - 1] >= 0);
    thread_instr->m_instrArray[m_instr_array_map[node_id - 1]].planNodeId = node_id;
    rc = memcpy_s(instr, sizeof(InstrStreamPlanData), msg, sizeof(InstrStreamPlanData));

    securec_check(rc, "\0", "\0");
    msg += sizeof(InstrStreamPlanData);

    if (m_threadInstrArray[1 + idx * dn_num_streams]) {
        m_threadInstrArray[1 + idx * dn_num_streams]->m_instrArray[0].instr.isExecute = true;
    }
}

/*
 * @Description: serialize the track data to remote one stream by one stream
 *	only send track when track->active== true
 * @return: void
 */
void StreamInstrumentation::serializeSendTrack()
{
    StringInfoData buf;

    for (int i = 0; i < m_threadInstrArrayLen; i++) {
        ThreadInstrumentation* thread_instr = m_threadInstrArray[i];

        if (thread_instr != NULL && thread_instr->m_instrArrayLen > 0) {
            int array_len = thread_instr->m_instrArrayLen;
            int segment_id = thread_instr->m_segmentId;

            pq_beginmessage(&buf, 'V');
            pq_sendint32(&buf, i);
            pq_sendint32(&buf, array_len);
            pq_sendint32(&buf, segment_id);

            Assert(array_len > 0);

            /* send generaltrack */
            Track* m_general_track_array = thread_instr->m_generalTrackArray;
            int m_general_track_len = thread_instr->m_generalTrackNum;
            int general_track_count = 0;

            Assert(m_general_track_len > 0);
            for (int j = 0; j < m_general_track_len; j++) {
                Track* track = &m_general_track_array[j];
                if (track->active == true) {
                    general_track_count++;
                }
            }
            pq_sendint(&buf, general_track_count, 4);

            if (general_track_count > 0) {
                for (int j = 0; j < m_general_track_len; j++) {
                    Track* track = &m_general_track_array[j];
                    if (track->active == true) {
                        pq_sendint32(&buf, j);
                        pq_sendbytes(&buf, (char*)track, sizeof(Track));
                    }
                }
            }

            /* send nodetrack */
            for (int j = 0; j < array_len; j++) {
                Track* track_array = thread_instr->m_instrArray[j].tracks;
                int track_num = thread_instr->get_tracknum();

                pq_sendint32(&buf, j);

                Assert(track_num > 0);
                /* how many tracknum where track->active is true */
                int track_count = 0;
                for (int k = 0; k < track_num; k++) {
                    Track* track = &track_array[k];
                    if (track->active) {
                        track_count++;
                    }
                }

                pq_sendint32(&buf, track_count);
                if (track_count > 0) {
                    for (int k = 0; k < track_num; k++) {
                        Track* track = &track_array[k];
                        if (track->active) {
                            pq_sendint32(&buf, k);
                            pq_sendbytes(&buf, (char*)track, sizeof(Track));
                        }
                    }
                }
            }

            pq_endmessage(&buf);
        }
    }
}

/*
 * @Description: deserialize the track data in a thread
 *
 * @param[IN] idx:  node index of datanode
 * @param[IN] msg:  pointer to incoming message
 * @param[IN] len:  length of msg
 * @return: void
 */
void StreamInstrumentation::deserializeTrack(int idx, char* msg, size_t len)
{
    errno_t rc = EOK;
    int track_idx;
    int array_len;
    int track_count = 0;
    int general_track_count = 0;
    int thread_idx;
    int array_idx;
    int segment_id = 0;
    MemoryContext tmp_context;

    rc = memcpy_s(&thread_idx, sizeof(int), msg, sizeof(int));
    securec_check(rc, "\0", "\0");
    thread_idx = ntohl(thread_idx);
    msg += 4;

    rc = memcpy_s(&array_len, sizeof(int), msg, sizeof(int));
    securec_check(rc, "\0", "\0");
    array_len = ntohl(array_len);
    msg += 4;

    rc = memcpy_s(&segment_id, sizeof(int), msg, sizeof(int));
    securec_check(rc, "\0", "\0");
    segment_id = ntohl(segment_id);
    msg += 4;

    /*
     * allocate threadinstrumentation in CN if receive from DN.
     * offset means plan node id offset in all streams,
     * dn_num_streams means all streams in one DN,
     * (idx, offset, smpid) can return only slot in streaminstrumentation of CN.
     * m_query_dop = 1 in compute pool.
     */
    int stream_info_idx = m_planIdOffsetArray[segment_id - 1];
    Assert(m_query_dop != 0);
    int smp_id = thread_idx % m_query_dop;
    int offset = m_planIdOffsetArray[segment_id - 1] == 0 ? -1 : (m_planIdOffsetArray[segment_id - 1] - 1) * m_query_dop;
    int dn_num_streams = DN_NUM_STREAMS_IN_CN(m_num_streams, m_gather_count, m_query_dop);
    int slot = 1 + idx * dn_num_streams + offset + smp_id;
    /* adopt for compute pool */
    if (!IS_SPQ_COORDINATOR && IS_PGXC_DATANODE) {
        offset = m_planIdOffsetArray[segment_id - 1] == 0 ? -1 : (m_planIdOffsetArray[segment_id - 1] * m_query_dop);
        slot = offset;
    }

    if (slot < 0 || slot >= m_threadInstrArrayLen) {
        ereport(ERROR, (errmsg("slot is out of range")));
    }

    if (m_threadInstrArray[slot] == NULL) {
        if (!IS_SPQ_COORDINATOR && IS_PGXC_DATANODE)
            tmp_context = MemoryContextOriginal((char*)u_sess->instr_cxt.global_instr);
        else
            tmp_context = u_sess->instr_cxt.global_instr->getInstrDataContext();
        AutoContextSwitch csv(tmp_context);
        m_threadInstrArray[slot] = New(tmp_context) ThreadInstrumentation(u_sess->debug_query_id,
            m_streamInfo[stream_info_idx].segmentId,
            m_streamInfo[stream_info_idx].numOfNodes,
            m_plannodes_num);
    }
    ThreadInstrumentation* thread_instr = m_threadInstrArray[slot];

    /* deserialize generaltrack */
    Track* m_general_track_array = thread_instr->m_generalTrackArray;

    rc = memcpy_s(&general_track_count, sizeof(int), msg, sizeof(int));
    securec_check(rc, "\0", "\0");
    general_track_count = ntohl(general_track_count);
    msg += 4;

    Assert(general_track_count >= 0);
    if (general_track_count > 0) {
        for (int generaltrackidx = 0; generaltrackidx < general_track_count; generaltrackidx++) {
            rc = memcpy_s(&track_idx, sizeof(int), msg, sizeof(int));
            securec_check(rc, "\0", "\0");
            track_idx = ntohl(track_idx);
            msg += 4;

            rc = memcpy_s(&m_general_track_array[track_idx], sizeof(Track), msg, sizeof(Track));
            securec_check(rc, "\0", "\0");
            msg += sizeof(Track);
        }
    }

    /* deserialize nodetrack */
    Assert(array_len >= 0);
    for (int array_len_idx = 0; array_len_idx < array_len; array_len_idx++) {
        rc = memcpy_s(&array_idx, sizeof(int), msg, sizeof(int));
        securec_check(rc, "\0", "\0");
        array_idx = ntohl(array_idx);
        msg += 4;

        Track* track_array = thread_instr->m_instrArray[array_idx].tracks;

        /* deserialize trackcount */
        rc = memcpy_s(&track_count, sizeof(int), msg, sizeof(int));
        securec_check(rc, "\0", "\0");
        track_count = ntohl(track_count);
        msg += 4;

        if (track_count > 0) {
            for (int track_count_idx = 0; track_count_idx < track_count; track_count_idx++) {
                rc = memcpy_s(&track_idx, sizeof(int), msg, sizeof(int));
                securec_check(rc, "\0", "\0");
                track_idx = ntohl(track_idx);
                msg += 4;

                rc = memcpy_s(&track_array[track_idx], sizeof(Track), msg, sizeof(Track));
                securec_check(rc, "\0", "\0");
                msg += sizeof(Track);
            }
        }
    }
}

/*
 * @Description: return a slot in specific idx, plannodeid and smpid=0
 *
 * @param[IN] idx:  node index of datanode
 * @param[IN] planNodeId:  plan node id of operator
 * @param[IN] smpId:  smpid of parallel thread
 * @return: the specific element by incoming parameters
 */
Instrumentation* StreamInstrumentation::getInstrSlot(int idx, int plan_node_id)
{
    Instrumentation* instr = NULL;

    ThreadInstrumentation* thread_instr =
        getThreadInstrumentation(idx, plan_node_id, 0);

    /* if threadinstr is not execute ,return NULL */
    if (thread_instr == NULL)
        return NULL;
    int* m_instr_array_map = thread_instr->m_instrArrayMap;
    /* m_instrArrayMap[planNodeId - 1] = -1 means plannode is initialized but not execute */
    if (m_instr_array_map[plan_node_id - 1] == -1)
        return NULL;
    instr = &thread_instr->m_instrArray[m_instr_array_map[plan_node_id - 1]].instr.instruPlanData;
    Assert(instr != NULL);

    return instr;
}

/*
 * @Description: return a slot in specific idx, plannodeid and smpid
 *
 * @param[IN] idx:  node index of datanode
 * @param[IN] planNodeId:  plan node id of operator
 * @param[IN] smpId:  smpid of parallel thread
 * @return: the specific element by incoming parameters
 */
Instrumentation* StreamInstrumentation::getInstrSlot(int idx, int plan_node_id, int smp_id)
{
    Instrumentation* instr = NULL;

    ThreadInstrumentation* thread_instr =
        getThreadInstrumentation(idx, plan_node_id, smp_id);

    /* if threadinstr is not execute ,return NULL */
    if (thread_instr == NULL)
        return NULL;
    int* m_instr_array_map = thread_instr->m_instrArrayMap;
    /* m_instrArrayMap[planNodeId - 1] = -1 means plannode is initialized but not execute */
    if (m_instr_array_map[plan_node_id - 1] == -1)
        return NULL;
    instr = &thread_instr->m_instrArray[m_instr_array_map[plan_node_id - 1]].instr.instruPlanData;
    Assert(instr != NULL);

    return instr;
}

/* traverse thr track is track->active == true */
bool StreamInstrumentation::isTrack()
{
    for (int i = 0; i < m_threadInstrArrayLen; i++) {
        ThreadInstrumentation* thread_instr = m_threadInstrArray[i];
        if (thread_instr != NULL) {
            /* watch generaltrack */
            int general_track_num = thread_instr->getgeneraltracknum();
            Track* m_general_track_array = thread_instr->m_generalTrackArray;

            for (int k = 0; k < general_track_num; k++) {
                if (m_general_track_array[k].active == true)
                    return true;
            }

            /* watch nodetrack */
            for (int j = 0; j < thread_instr->m_instrArrayLen; j++) {
                NodeInstr* instr_array = &thread_instr->m_instrArray[j];
                int track_num = thread_instr->get_tracknum();

                for (int k = 0; k < track_num; k++) {
                    Track* track = &instr_array->tracks[k];
                    if (track->active == true)
                        return true;
                }
            }
        }
    }
    return false;
}

void StreamInstrumentation::TrackStartTime(int plan_node_id, int track_id)
{
    if (u_sess->instr_cxt.thread_instr && u_sess->instr_cxt.global_instr != NULL) {
        u_sess->instr_cxt.thread_instr->startTrack(plan_node_id, (ThreadTrack)track_id);
    }
}

void StreamInstrumentation::TrackEndTime(int plan_node_id, int track_id)
{
    if (u_sess->instr_cxt.thread_instr && u_sess->instr_cxt.global_instr != NULL) {
        u_sess->instr_cxt.thread_instr->endTrack(plan_node_id, (ThreadTrack)track_id);
    }
}

/* run in CN only m_planIdOffsetArray[planNodeId-1] > 0 , planNodeId RUN IN DN */
bool StreamInstrumentation::isFromDataNode(int plan_node_id)
{
#if defined(ENABLE_MULTIPLE_NODES) || defined(USE_SPQ)
    if (t_thrd.spq_ctx.spq_role != ROLE_UTILITY) {
        if (u_sess->instr_cxt.global_instr && m_planIdOffsetArray[plan_node_id - 1] > 0) {
            int num_streams = u_sess->instr_cxt.global_instr->getInstruThreadNum();
            int query_dop = u_sess->instr_cxt.global_instr->get_query_dop();
            int dn_num_threads = DN_NUM_STREAMS_IN_CN(num_streams, m_gather_count, query_dop);
            int offset = (m_planIdOffsetArray[plan_node_id - 1] - 1) * query_dop;

            for (int i = 0; i < u_sess->instr_cxt.global_instr->getInstruNodeNum(); i++) {
                /* avoid for activesql */
                ThreadInstrumentation* thread_instr = m_threadInstrArray[1 + i * dn_num_threads + offset];
                if (thread_instr != NULL && thread_instr->m_instrArrayMap[plan_node_id - 1] != -1)
                    return true;
            }
            return false;
        }
        return false;
    } else {
        return true;
    }
#else
    return true;
#endif
}

/* set NetWork in DN */
void StreamInstrumentation::SetNetWork(int plan_node_id, int64 buf_len)
{
    if (!IS_SPQ_EXECUTOR || IS_PGXC_DATANODE){
        ThreadInstrumentation* thread_instr =
            m_threadInstrArray[m_planIdOffsetArray[plan_node_id - 1] * m_query_dop + u_sess->stream_cxt.smp_id];

        int* m_instr_array_map = thread_instr->m_instrArrayMap;

        Instrumentation* instr = &thread_instr->m_instrArray[m_instr_array_map[plan_node_id - 1]].instr.instruPlanData;
        instr->network_perfdata.network_size += buf_len;
    }
}

/*
 * aggregate in CN of compute pool
 * aggregate function collect all instrument of DN in compute pool
 * the cn of compute pool send data to DN in original cluster
 */
void StreamInstrumentation::aggregate(int plannode_num)
{
    Assert(m_query_dop == 1);
    int dn_num_streams = DN_NUM_STREAMS_IN_CN(m_num_streams, m_gather_count, m_query_dop);

    for (int node_id = m_start_node_id + 1; node_id <= m_start_node_id + plannode_num; node_id++) {
        int dn_idx = -1;
        /* find the offset which exec on dn in compute pool */
        int offset = (m_planIdOffsetArray[node_id - 1] - 1) * m_query_dop;
        /* find last valid threadinstrumentation idx*/
        for (int dn = 0; dn < m_nodes_num; dn++) {
            const int thread_idx = 1 + dn * dn_num_streams + offset;
            ThreadInstrumentation* thread_instr = m_threadInstrArray[thread_idx];
            int* m_instr_array_map = thread_instr->m_instrArrayMap;
            /* find the last valid dn_idx */
            if (m_instr_array_map[m_start_node_id] != -1)
                dn_idx = dn;
        }

        /* if no valid threadinstrumentation, continue */
        if (dn_idx == -1)
            continue;

        /* the last valid m_threadInstrArray in dn_idx */
        ThreadInstrumentation* thread_rs = m_threadInstrArray[1 + dn_idx * dn_num_streams + offset];

        int* array_map = thread_rs->m_instrArrayMap;
        Assert(array_map[node_id - 1] != -1);
        InstrStreamPlanData* rs_data = &(thread_rs->m_instrArray[array_map[node_id - 1]].instr);
        Instrumentation* rs = &(rs_data->instruPlanData);
        /* before aggregate, let rsdata->isValid is true */
        rs_data->isValid = true;

        for (int dn = 0; dn < dn_idx; dn++) {
            ThreadInstrumentation* thread_instr = m_threadInstrArray[1 + dn * dn_num_streams + offset];
            int* instr_array_map = thread_instr->m_instrArrayMap;
            
            if (instr_array_map[node_id - 1] == -1)
                continue;
            InstrStreamPlanData* instr_data = &(thread_instr->m_instrArray[instr_array_map[node_id - 1]].instr);
            Instrumentation* instr = &(instr_data->instruPlanData);
            /* after aggregate, let instrdata->isValid is false */
            instr_data->isValid = false;

            rs->need_timer = (rs->need_timer || instr->need_timer) ? true : false;
            rs->need_bufusage = (rs->need_bufusage || instr->need_bufusage) ? true : false;
            rs->needRCInfo = (rs->needRCInfo || instr->needRCInfo) ? true : false;

            rs->running = (rs->running || instr->running) ? true : false;
            if (INSTR_TIME_IS_BIGGER(rs->starttime, instr->starttime))
                rs->starttime = instr->starttime;
            if (INSTR_TIME_IS_BIGGER(instr->counter, rs->counter))
                rs->counter = instr->counter;
            if (instr->firsttuple < rs->firsttuple)
                rs->firsttuple = instr->firsttuple;
            rs->tuplecount += instr->tuplecount;

            if (instr->startup > rs->startup)
                rs->startup = instr->startup;
            if (instr->total > rs->total)
                rs->total = instr->total;
            rs->ntuples += instr->ntuples;
            rs->nloops += instr->nloops;
            rs->nfiltered1 += instr->nfiltered1;
            rs->nfiltered2 += instr->nfiltered2;

            rs->dynamicPrunFiles += instr->dynamicPrunFiles;
            rs->staticPruneFiles += instr->staticPruneFiles;
            rs->bloomFilterRows += instr->bloomFilterRows;
            rs->bloomFilterBlocks += instr->bloomFilterBlocks;
            rs->minmaxFilterRows += instr->minmaxFilterRows;

            if (instr->init_time < rs->init_time)
                rs->init_time = instr->init_time;
            if (instr->end_time > rs->end_time)
                rs->end_time = instr->end_time;

            rs->localBlock += instr->localBlock;
            rs->remoteBlock += instr->remoteBlock;

            rs->nnCalls += instr->nnCalls;
            rs->dnCalls += instr->dnCalls;

            rs->minmaxCheckFiles += instr->minmaxCheckFiles;
            rs->minmaxFilterFiles += instr->minmaxFilterFiles;
            rs->minmaxCheckStripe += instr->minmaxCheckStripe;
            rs->minmaxFilterStripe += instr->minmaxFilterStripe;
            rs->minmaxCheckStride += instr->minmaxCheckStride;
            rs->minmaxFilterStride += instr->minmaxFilterStride;
            rs->orcDataCacheBlockCount += instr->orcDataCacheBlockCount;
            rs->orcDataCacheBlockSize += instr->orcDataCacheBlockSize;
            rs->orcDataLoadBlockCount += instr->orcDataLoadBlockCount;
            rs->orcDataLoadBlockSize += instr->orcDataLoadBlockSize;
            rs->orcMetaCacheBlockCount += instr->orcMetaCacheBlockCount;
            rs->orcMetaCacheBlockSize += instr->orcMetaCacheBlockSize;
            rs->orcMetaLoadBlockCount += instr->orcMetaLoadBlockCount;
            rs->orcMetaLoadBlockSize += instr->orcMetaLoadBlockSize;

            rs->isLlvmOpt = (rs->isLlvmOpt || instr->isLlvmOpt) ? true : false;
        }
    }
}

/*
 * @Description:  set stream is send
 * @in planNodeId -plan node  id
 * @bool send - is send
 * @return - void
 */
void StreamInstrumentation::SetStreamSend(int plan_node_id, bool send)
{
    if (!IS_SPQ_COORDINATOR && IS_PGXC_DATANODE) {
        ThreadInstrumentation* thread_instr =
            m_threadInstrArray[m_planIdOffsetArray[plan_node_id - 1] * m_query_dop + u_sess->stream_cxt.smp_id];

        int* m_instr_array_map = thread_instr->m_instrArrayMap;

        InstrStreamPlanData* instr = &thread_instr->m_instrArray[m_instr_array_map[plan_node_id - 1]].instr;

        instr->isSend = send;
    }
}

/*
 * @Description:  get stream is send
 * @in idx - datanode index
 * @in plan_id -plan node  id
 * @in smp_id - current smp index
 * @return - is data send CN
 */
bool StreamInstrumentation::IsSend(int idx, int planId, int smpId)
{
    ThreadInstrumentation* thread_instr = getThreadInstrumentation(idx, planId, smpId);
    if (thread_instr == NULL) {
        return false;
    }

    int* m_instr_array_map = thread_instr->m_instrArrayMap;

    /* m_instr_array_map[planId - 1] = -1 means plannode is initialized but not execute */
    if (m_instr_array_map[planId - 1] == -1) {
        return false;
    }

    InstrStreamPlanData* instr = &thread_instr->m_instrArray[m_instr_array_map[planId - 1]].instr;

    return instr->isSend;
}

/* set peakNodeMemory in DN */
void StreamInstrumentation::SetPeakNodeMemory(int plan_node_id, int64 memory_size)
{
    ThreadInstrumentation* thread_instr = m_threadInstrArray[m_planIdOffsetArray[plan_node_id - 1] * m_query_dop];

    int* m_instr_array_map = thread_instr->m_instrArrayMap;

    Instrumentation* instr = &thread_instr->m_instrArray[m_instr_array_map[plan_node_id - 1]].instr.instruPlanData;

    instr->memoryinfo.peakNodeMemory = memory_size;
}
// Pre-defined CPU monitor groups
//
static const CPUMonGroup CpuMonGroups[] = {
    {CMON_GENERAL, 3, {PERF_COUNT_HW_CPU_CYCLES, PERF_COUNT_HW_INSTRUCTIONS, PERF_COUNT_HW_CACHE_MISSES}}};

// Static initialization of CPUMon members
//

THR_LOCAL int CPUMon::m_fd[] = {-1, -1, -1};
THR_LOCAL int CPUMon::m_cCounters = 3;
THR_LOCAL bool CPUMon::m_has_perf = false;
THR_LOCAL bool CPUMon::m_has_initialize = false;

// CPUMon::Initialize
//      Intialize CPU Monitoring with given groupID and start monitoring
//
void CPUMon::Initialize(_in_ CPUMonGroupID id)
{
    struct perf_event_attr hw_event;
    const pid_t pid = 0;  // current thread
    const int cpu = -1;
    const int group_fd = -1;
    const unsigned long flags = 0;
    int ret;
    int c_counters;
    int rc = 0;

    if (CPUMon::m_has_initialize == true)
        return;

    DBG_ASSERT(id == CpuMonGroups[id].m_id);
    c_counters = CpuMonGroups[id].m_cCounters;

    // Create a set of counters
    //
    rc = memset_s(&hw_event, sizeof(hw_event), 0, sizeof(hw_event));
    securec_check(rc, "\0", "\0");
    hw_event.type = PERF_TYPE_HARDWARE;
    hw_event.size = sizeof(struct perf_event_attr);
    hw_event.disabled = 1;
    hw_event.exclude_kernel = 1;
    hw_event.exclude_hv = 1;
    for (int i = 0; i < c_counters; i++) {
        hw_event.config = CpuMonGroups[id].m_counters[i];
        ret = syscall(__NR_perf_event_open, &hw_event, pid, cpu, group_fd, flags);

        if (ret < 0) {
            ereport(LOG, (errmsg("failed to set up perf events %ld: %m", (int64)hw_event.config)));
            CPUMon::m_has_perf = false;
            return;
        }

        m_fd[i] = ret;
    }

    // Reset and start up counters
    //
    for (int i = 0; i < c_counters; i++) {
        ret = ioctl(m_fd[i], PERF_EVENT_IOC_RESET, 0);
        if (ret != 0) {
            ereport(LOG, (errmsg("failed to reset perf events: %m")));
        }

        ret = ioctl(m_fd[i], PERF_EVENT_IOC_ENABLE, 0);
        if (ret != 0) {
            ereport(LOG, (errmsg("failed to start up perf events: %m")));
        }
    }

    m_cCounters = c_counters;
    CPUMon::m_has_initialize = true;
    CPUMon::m_has_perf = true;
    return;
}

// CPUMon::Shutdown
//      Stop all related to the monitoring
//
void CPUMon::Shutdown()
{
    for (int i = 0; i < m_cCounters; i++) {
        if (m_fd[i] != -1) {
            int ret = ioctl(m_fd[i], PERF_EVENT_IOC_DISABLE, 0);
            if (ret != 0) {
                ereport(LOG, (errmsg("failed to stop perf events")));
            }

            close(m_fd[i]);
            m_fd[i] = -1;
        }
    }

    CPUMon::m_has_initialize = false;
}

// CPUMon::ReadCounter
//      Read a specific counter
//
int64 CPUMon::ReadCounter(_in_ int i)
{
    int64 value;
    int ret;
    validCounter(i);
    ret = read(m_fd[i], &value, sizeof(value));
    if (ret != sizeof(value)) {
        ereport(ERROR, (errcode(ERRCODE_S_R_E_READING_SQL_DATA_NOT_PERMITTED), errmsg("cannot read results")));
    }

    return value;
}

// CPUMon::ResetCounter
//      Reset all counters
//
int CPUMon::ResetCounters(_out_ int64* p_counters)
{
    for (int i = 0; i < m_cCounters; i++) {
        validCounter(i);
        p_counters[i] = 0LL;
    }

    return m_cCounters;
}

// CPUMon::ReadCounter
//      Read all counters enabled
//
int CPUMon::ReadCounters(_out_ int64* p_counters)
{
    int64 value;
    int ret;
    for (int i = 0; i < m_cCounters; i++) {
        validCounter(i);
        ret = read(m_fd[i], &value, sizeof(value));
        if (unlikely(ret != sizeof(value))) {
            ereport(ERROR, (errcode(ERRCODE_S_R_E_READING_SQL_DATA_NOT_PERMITTED), errmsg("cannot read results")));
        }

        p_counters[i] = value;
    }

    return m_cCounters;
}

int CPUMon::RestartCounters(_out_ int64* p_counters, _out_ int64* p_accum_counters)
{
    ReadCounters(p_counters);
    ResetCounters(p_accum_counters);
    return m_cCounters;
}

// CPUMon::AccumCounters
//      Read all counters enabled and accumulate them
//
int CPUMon::AccumCounters(__inout int64* p_counters, __inout int64* p_accum_counters)
{
    int64 ovalue, value;
    int ret;
    for (int i = 0; i < m_cCounters; i++) {
        validCounter(i);
        ret = read(m_fd[i], &value, sizeof(value));
        if (unlikely(ret != sizeof(value))) {
            ereport(ERROR, (errcode(ERRCODE_S_R_E_READING_SQL_DATA_NOT_PERMITTED), errmsg("cannot read results")));
        }

        ovalue = p_counters[i];
        p_accum_counters[i] += value > ovalue ? value - ovalue : 0;
        p_counters[i] = value;
    }

    return m_cCounters;
}

bool CPUMon::get_perf()
{
    return m_has_perf;
}

static void CPUUsageGetCurrent(CPUUsage* cur)
{
    cur->m_cycles = rdtsc();
}

static void CPUUsageAccumDiff(CPUUsage* dst, const CPUUsage* add, const CPUUsage* sub)
{
    dst->m_cycles += add->m_cycles - sub->m_cycles;
}

OBSInstrumentation::OBSInstrumentation()
    : m_p_globalOBSInstrument_valid(&u_sess->instr_cxt.OBS_instr_valid), m_rels(NIL), m_ctx(CurrentMemoryContext)

{
    ereport(DEBUG5, (errmodule(MOD_ACCELERATE), errmsg("in %s", __FUNCTION__)));
    u_sess->instr_cxt.OBS_instr_valid = true;
}

OBSInstrumentation::~OBSInstrumentation()
{
    ereport(DEBUG5, (errmodule(MOD_ACCELERATE), errmsg("in %s", __FUNCTION__)));

    LWLockAcquire(OBSRuntimeLock, LW_EXCLUSIVE);

    list_free_ext(m_rels);

    m_rels = NIL;

    u_sess->instr_cxt.OBS_instr_valid = false;

    LWLockRelease(OBSRuntimeLock);
}

void OBSInstrumentation::serializeSend()
{
    ereport(DEBUG5, (errmodule(MOD_ACCELERATE), errmsg("in %s", __FUNCTION__)));

    if ((!IS_SPQ_COORDINATOR && IS_PGXC_DATANODE) &&
        !StreamTopConsumerAmI() && u_sess->instr_cxt.p_OBS_instr_valid == NULL)
        return;

    StringInfoData buf;
    ListCell* lc = NULL;

    LWLockAcquire(OBSRuntimeLock, LW_EXCLUSIVE);

    if ((!IS_SPQ_COORDINATOR && IS_PGXC_DATANODE) &&
        !StreamTopConsumerAmI() && *u_sess->instr_cxt.p_OBS_instr_valid == false) {
        ereport(DEBUG1,
            (errmodule(MOD_ACCELERATE), errmsg("u_sess->instr_cxt.obs_instr is deleted in top consumer thread.")));
        LWLockRelease(OBSRuntimeLock);
        return;
    }

    foreach (lc, m_rels) {
        OBSRuntimeInfo* info = (OBSRuntimeInfo*)lfirst(lc);

        pq_beginmessage(&buf, 'u');
        pq_sendbytes(&buf, info->relname.data, NAMEDATALEN);
        pq_sendint(&buf, info->file_scanned, 4);
        pq_sendbytes(&buf, (char*)&info->data_size, sizeof(int64));
        pq_sendbytes(&buf, (char*)&info->actual_time, sizeof(double));
        pq_sendint(&buf, info->format, 4);
        pq_endmessage(&buf);
    }

    LWLockRelease(OBSRuntimeLock);
}

void OBSInstrumentation::deserialize(char* msg, size_t len)
{
    ereport(DEBUG5, (errmodule(MOD_ACCELERATE), errmsg("in %s", __FUNCTION__)));

    if ((!IS_SPQ_COORDINATOR && IS_PGXC_DATANODE) &&
        !StreamTopConsumerAmI() && u_sess->instr_cxt.p_OBS_instr_valid == NULL)
        return;

    errno_t rc = EOK;
    bool found = false;

    ListCell* lc = NULL;
    OBSRuntimeInfo* info = NULL;

    LWLockAcquire(OBSRuntimeLock, LW_EXCLUSIVE);

    if ((!IS_SPQ_COORDINATOR && IS_PGXC_DATANODE) &&
        !StreamTopConsumerAmI() && *u_sess->instr_cxt.p_OBS_instr_valid == false) {
        ereport(DEBUG1,
            (errmodule(MOD_ACCELERATE), errmsg("u_sess->instr_cxt.obs_instr is deleted in top consumer thread.")));
        LWLockRelease(OBSRuntimeLock);
        return;
    }

    foreach (lc, m_rels) {
        info = (OBSRuntimeInfo*)lfirst(lc);
        if (!pg_strcasecmp(msg, info->relname.data)) {
            found = true;
            break;
        }
    }

    if (!found) {
        AutoContextSwitch memGuard(m_ctx);

        info = (OBSRuntimeInfo*)palloc0(sizeof(OBSRuntimeInfo));
        m_rels = lappend(m_rels, info);

        rc = memcpy_s(info->relname.data, NAMEDATALEN, msg, NAMEDATALEN);
        securec_check(rc, "\0", "\0");
    }

    msg += NAMEDATALEN;

    int32 file_scanned;
    rc = memcpy_s(&file_scanned, sizeof(int32), msg, sizeof(int32));
    securec_check(rc, "\0", "\0");
    file_scanned = ntohl(file_scanned);
    msg += 4;

    int64 data_size;
    rc = memcpy_s(&data_size, sizeof(int64), msg, sizeof(int64));
    securec_check(rc, "\0", "\0");
    msg += 8;

    double actual_time;
    rc = memcpy_s(&actual_time, sizeof(double), msg, sizeof(double));
    securec_check(rc, "\0", "\0");
    msg += sizeof(double);

    int32 format;
    rc = memcpy_s(&format, sizeof(int32), msg, sizeof(int32));
    securec_check(rc, "\0", "\0");
    format = ntohl(format);
    msg += 4;

    info->file_scanned += file_scanned;
    info->data_size += data_size;
    if (actual_time > info->actual_time)
        info->actual_time = actual_time;
    info->format = format;

    LWLockRelease(OBSRuntimeLock);
}

void OBSInstrumentation::save(const char* relname, int file_scanned, int64 data_size, double actual_time, int32 format)
{
    ereport(DEBUG5, (errmodule(MOD_ACCELERATE), errmsg("in %s", __FUNCTION__)));

    if ((!IS_SPQ_COORDINATOR && IS_PGXC_DATANODE) &&
        !StreamTopConsumerAmI() && u_sess->instr_cxt.p_OBS_instr_valid == NULL)
        return;

    errno_t rc = EOK;
    bool found = false;

    ListCell* lc = NULL;
    OBSRuntimeInfo* info = NULL;

    LWLockAcquire(OBSRuntimeLock, LW_EXCLUSIVE);

    if ((!IS_SPQ_COORDINATOR && IS_PGXC_DATANODE) &&
        !StreamTopConsumerAmI() && *u_sess->instr_cxt.p_OBS_instr_valid == false) {
        ereport(DEBUG1,
            (errmodule(MOD_ACCELERATE), errmsg("u_sess->instr_cxt.obs_instr is deleted in top consumer thread.")));
        LWLockRelease(OBSRuntimeLock);
        return;
    }

    foreach (lc, m_rels) {
        info = (OBSRuntimeInfo*)lfirst(lc);
        if (!pg_strcasecmp(relname, info->relname.data)) {
            found = true;
            break;
        }
    }

    if (!found) {
        AutoContextSwitch memGuard(m_ctx);

        info = (OBSRuntimeInfo*)palloc0(sizeof(OBSRuntimeInfo));
        m_rels = lappend(m_rels, info);

        rc = memcpy_s(info->relname.data, NAMEDATALEN, relname, NAMEDATALEN);
        securec_check(rc, "\0", "\0");
    }

    info->file_scanned = file_scanned;
    info->data_size = data_size;
    info->actual_time = actual_time;
    info->format = format;

    LWLockRelease(OBSRuntimeLock);
}

void OBSInstrumentation::insertData(uint64 queryid)
{
    ereport(DEBUG5, (errmodule(MOD_ACCELERATE), errmsg("in %s", __FUNCTION__)));

    ListCell* lc = NULL;
    OBSRuntimeInfo* info = NULL;

#ifndef USE_SPQ
    Assert(IS_PGXC_COORDINATOR);
#endif

    Assert(!StreamTopConsumerAmI());

    LWLockAcquire(OBSRuntimeLock, LW_EXCLUSIVE);

    foreach (lc, m_rels) {
        info = (OBSRuntimeInfo*)lfirst(lc);

        insert_obsscaninfo(
            queryid, info->relname.data, info->file_scanned, (double)info->data_size, info->actual_time, info->format);
    }

    list_free_ext(m_rels);

    m_rels = NIL;

    LWLockRelease(OBSRuntimeLock);
}

void deleteGlobalOBSInstrumentation()
{
    if (!StreamTopConsumerAmI())
        return;

    if (!u_sess->instr_cxt.obs_instr)
        return;

    ereport(DEBUG5, (errmodule(MOD_ACCELERATE), errmsg("in %s", __FUNCTION__)));

    OBSInstrumentation* obs_info = u_sess->instr_cxt.obs_instr;

    u_sess->instr_cxt.obs_instr = NULL;

    delete obs_info;
}

/*
 * @Description: hash value function
 * @in key1 -hash key
 * @int keysize - value size
 * @return - hash value
 */
uint32 GetHashPlanCode(const void* key1, Size key_size)
{
    Qpid* qid = (Qpid*)key1;
    bool is_positive = ((int64)qid->queryId) >= 0;
    uint32 lo_half = (uint32)qid->queryId;
    uint32 hi_half = (uint32)(qid->queryId >> 32);

    /* This idea is borrowed from hashint8. please refer to hashint8 for detail. */
    lo_half ^= is_positive ? hi_half : ~hi_half;
    uint32 newValue = (qid->procId + lo_half + qid->plannodeid);
    return oid_hash(&newValue, sizeof(Oid));
}

/*
 * @Description: hash match function
 * @in key1 -hash key1
 * @in key2 -hash key2
 * @int keysize - match size
 * @return - match or not
 */
int ExplainHashMatch(const void* key1, const void* key2, Size key_size)
{
    Qpid* v1 = (Qpid*)key1;
    Qpid* v2 = (Qpid*)key2;

    if (v1->plannodeid == v2->plannodeid && v1->queryId == v2->queryId && v1->procId == v2->procId)
        return 0;
    else
        return 1;
}

/*
 * @Description: initialize hashtable for operator statistics information
 * @return - void
 */
void InitOperStatProfile(void)
{
    MemoryContext old_context;
    errno_t rc;
    HASHCTL hash_ctl;

    old_context = MemoryContextSwitchTo(g_instance.wlm_cxt->oper_resource_track_mcxt);

    rc = memset_s(&g_operator_table, sizeof(OperatorProfileTable), 0, sizeof(OperatorProfileTable));
    securec_check(rc, "\0", "\0");

    rc = memset_s(&hash_ctl, sizeof(hash_ctl), 0, sizeof(hash_ctl));
    securec_check(rc, "\0", "\0");
    hash_ctl.keysize = sizeof(Qpid);
    hash_ctl.hcxt = g_instance.wlm_cxt->oper_resource_track_mcxt; /* use operator resource track memory context */
    hash_ctl.entrysize = sizeof(ExplainDNodeInfo);
    hash_ctl.hash = GetHashPlanCode;
    hash_ctl.match = ExplainHashMatch;
    hash_ctl.num_partitions = NUM_OPERATOR_REALTIME_PARTITIONS;

    g_operator_table.explain_info_hashtbl = hash_create("operator running hash table",
        512,
        &hash_ctl,
        HASH_ELEM | HASH_SHRCTX | HASH_FUNCTION | HASH_COMPARE | HASH_PARTITION);

    HASHCTL hash_ctl1;
    rc = memset_s(&hash_ctl1, sizeof(hash_ctl1), 0, sizeof(hash_ctl1));
    securec_check(rc, "\0", "\0");
    hash_ctl1.keysize = sizeof(Qpid);
    hash_ctl1.hcxt = g_instance.wlm_cxt->oper_resource_track_mcxt; /* use operator resource track memory context */
    hash_ctl1.entrysize = sizeof(ExplainDNodeInfo);
    hash_ctl1.hash = GetHashPlanCode;
    hash_ctl1.match = ExplainHashMatch;
    hash_ctl1.num_partitions = NUM_OPERATOR_HISTORY_PARTITIONS;

    g_operator_table.collected_info_hashtbl = hash_create("operator collector hash table",
        512,
        &hash_ctl1,
        HASH_ELEM | HASH_SHRCTX | HASH_FUNCTION | HASH_COMPARE | HASH_PARTITION);

    MemoryContextSwitchTo(old_context);
}

/*
 * @Description: qid is valid
 * @return - bool
 */
bool IsQpidInvalid(const Qpid* qid)
{
    if (qid == NULL || qid->queryId <= 0 || qid->plannodeid < 0) {
        return true;
    }

    if (qid->procId <= 0 && !IS_SINGLE_NODE) {
        return true;
    }

    return false;
}

/*
 * @Description: insert  into the qid to hashtable for both CN adn DN
 * @in qid -current plan node id, query id and proc id
 * @in instr -instrumentation for current plan node id
 * @in on_dn - current operator execute on datanode or not
 * @in plan_name - plan node name
 * @in dop - query dop
 * @in estimated_rows - estimated rows
 * @return - void
 */
void ExplainCreateDNodeInfoOnDN(
    Qpid* qid, Instrumentation* instr, bool on_dn, const char* plan_name, int dop, int64 estimated_rows)
{
    bool has_found = false;
    ExplainDNodeInfo* p_dnode_info = NULL;
    errno_t rc = EOK;

    if (IsQpidInvalid(qid))
        return;

    uint32 hash_code = GetHashPlanCode(qid, sizeof(Qpid));
    LockOperRealTHashPartition(hash_code, LW_EXCLUSIVE);

    p_dnode_info = (ExplainDNodeInfo*)hash_search(g_operator_table.explain_info_hashtbl, qid, HASH_ENTER, &has_found);

    if (p_dnode_info != NULL) {
        if (likely(!has_found)) {
            rc = memset_s(p_dnode_info, sizeof(ExplainDNodeInfo), 0, sizeof(ExplainDNodeInfo));
            securec_check(rc, "\0", "\0");
            p_dnode_info->qid = *qid;
            p_dnode_info->explain_entry = instr; /* get session memory entry */
            p_dnode_info->execute_on_datanode = on_dn;
            p_dnode_info->userid = GetUserId();

            if (IS_SPQ_COORDINATOR || IS_PGXC_COORDINATOR ||
                IS_SINGLE_NODE) {
                MemoryContext old_context;
                old_context = MemoryContextSwitchTo(g_instance.wlm_cxt->oper_resource_track_mcxt);
                int plan_len = strlen(plan_name) + 1;
                p_dnode_info->plan_name = (char*)palloc0(sizeof(char) * plan_len);
                rc = memcpy_s(p_dnode_info->plan_name, plan_len, plan_name, plan_len);
                securec_check(rc, "\0", "\0");
                MemoryContextSwitchTo(old_context);
                p_dnode_info->query_dop = dop;
                p_dnode_info->estimated_rows = estimated_rows;
            }
        } else {
            if (IS_SPQ_COORDINATOR || IS_PGXC_COORDINATOR ||
                IS_SINGLE_NODE) {
                ereport(LOG,
                    (errmsg("Realtime Trace Error: The new information has the same hash key as the existed record in "
                            "the hash table, which is not expected.")));
            }
        }
    } else {
        ereport(LOG, (errmsg("Realtime Trace Error: Cannot alloc memory, out of memory!")));
    }
    UnLockOperRealTHashPartition(hash_code);
}

FORCE_INLINE
int32 convert_to_mb(int64 memory_size)
{
    return ((memory_size + 1023) / 1024 + 1023) / 1024;
}

int64 e_rows_convert_to_int64(double plan_rows)
{
    const int64 max_estimated_rows = INT64_MAX;
    return plan_rows >= (double)max_estimated_rows ? max_estimated_rows : (int64)plan_rows;
}

static int get_warning_info(int64 spill_size, const char* plan_node_name)
{
    int warning = 0;
    if (spill_size == 0)
        return 0;

    if (strcmp(plan_node_name, "Stream") == 0 || strcmp(plan_node_name, "VecStream") == 0) {
        if (spill_size / (1 << 20) >= WARNING_BROADCAST_SIZE) {
            return (1 << WLM_WARN_BROADCAST_LARGE);
        }
    } else {
        if (convert_to_mb(spill_size) > 0) {
            warning = (uint32)warning | (1 << WLM_WARN_SPILL);
        }

        if (convert_to_mb(spill_size) >= WARNING_SPILL_SIZE) {
            warning = (uint32)warning | (1 << WLM_WARN_SPILL_FILE_LARGE);
        }
    }

    return warning;
}

/*
 * @Description: get CN value from hashtable
 * @in stat_element: pointer to store value.
 * @in str: instrumentation info.
 * @return - void
 */
void setCnGeneralInfo(ExplainGeneralInfo* stat_element, OperatorInfo* opstr)
{
    stat_element->tuple_processed = opstr->total_tuples;
    stat_element->start_time = opstr->enter_time;
    stat_element->startup_time = opstr->startup_time;
    stat_element->duration_time = opstr->execute_time;
    stat_element->max_cpu_time = stat_element->min_cpu_time = stat_element->total_cpu_time = 0;
    stat_element->max_peak_memory = stat_element->min_peak_memory = stat_element->avg_peak_memory =
        convert_to_mb(opstr->peak_memory);
    stat_element->max_spill_size = stat_element->min_spill_size = stat_element->avg_spill_size =
        convert_to_mb(opstr->spill_size);
    stat_element->memory_skewed = 0;
    stat_element->cpu_skew = 0;
    stat_element->i_o_skew = 0;
    stat_element->warn_prof_info = get_warning_info(opstr->spill_size, stat_element->plan_node_name);

    stat_element->status = opstr->status;
}

static inline void CpyCStringField(char** dest, const char* src)
{
    if (dest == NULL)
        ereport(ERROR, (errmodule(MOD_OPT_AI),  errcode(ERRCODE_UNEXPECTED_NULL_VALUE),
                        errmsg("Unexpected NULL value.")));
    if (src != NULL) {
        *dest = pstrdup(src);
    } else {
        *dest = (char*) palloc0(1);
    }
}

static void PredSetOptMem(OperatorInfo *operatorMemory, OperatorPlanInfo* opt_plan_info)
{
    if (opt_plan_info != NULL) {
        CpyCStringField(&operatorMemory->planinfo.operation, opt_plan_info->operation);
        CpyCStringField(&operatorMemory->planinfo.orientation, opt_plan_info->orientation);
        CpyCStringField(&operatorMemory->planinfo.strategy, opt_plan_info->strategy);
        CpyCStringField(&operatorMemory->planinfo.options, opt_plan_info->options);
        CpyCStringField(&operatorMemory->planinfo.condition, opt_plan_info->condition);
        CpyCStringField(&operatorMemory->planinfo.projection, opt_plan_info->projection);

        operatorMemory->planinfo.parent_node_id = opt_plan_info->parent_node_id;
        operatorMemory->planinfo.left_child_id = opt_plan_info->left_child_id;
        operatorMemory->planinfo.right_child_id = opt_plan_info->right_child_id;
    } else {
        operatorMemory->planinfo.operation= NULL;
        operatorMemory->planinfo.orientation = NULL;
        operatorMemory->planinfo.strategy = NULL;
        operatorMemory->planinfo.options = NULL;
        operatorMemory->planinfo.condition = NULL;
        operatorMemory->planinfo.projection = NULL;
    }
}

void setOperatorInfo(OperatorInfo* operatorMemory, Instrumentation* InstrMemory, OperatorPlanInfo* opt_plan_info)
{
    static const int s_to_ms = 1000;
    operatorMemory->total_tuples = (int64)(InstrMemory->ntuples + InstrMemory->tuplecount);
    operatorMemory->peak_memory = InstrMemory->memoryinfo.peakOpMemory;
    operatorMemory->spill_size = InstrMemory->sorthashinfo.spill_size;
    operatorMemory->enter_time = InstrMemory->enter_time;
    operatorMemory->startup_time = (int64)(InstrMemory->startup * s_to_ms);
    operatorMemory->execute_time =
        (int64)(InstrMemory->total * s_to_ms + INSTR_TIME_GET_DOUBLE(InstrMemory->counter) * s_to_ms);
    operatorMemory->status = InstrMemory->status;

    MemoryContext oldcontext;
    oldcontext = MemoryContextSwitchTo(g_instance.wlm_cxt->oper_resource_track_mcxt);
    operatorMemory->datname = get_database_name(u_sess->proc_cxt.MyDatabaseId);
    PredSetOptMem(operatorMemory, opt_plan_info);
    MemoryContextSwitchTo(oldcontext);

    operatorMemory->warning = InstrMemory->warning;
    if (InstrMemory->sysBusy) {
        operatorMemory->warning |= (1 << WLM_WARN_EARLY_SPILL);
    }

    if (InstrMemory->spreadNum > 0 && InstrMemory->sorthashinfo.spill_size > 0) {
        operatorMemory->warning |= (1 << WLM_WARN_SPILL_ON_MEMORY_SPREAD);
    }

    operatorMemory->ec_operator = InstrMemory->ec_operator;
    if (operatorMemory->ec_operator == IS_EC_OPERATOR) {
        operatorMemory->ec_status = InstrMemory->ec_status;
        operatorMemory->ec_libodbc_type = InstrMemory->ec_libodbc_type;
        operatorMemory->ec_fetch_count = InstrMemory->ec_fetch_count;

        oldcontext = MemoryContextSwitchTo(g_instance.wlm_cxt->oper_resource_track_mcxt);

        errno_t rc = EOK;
        if (InstrMemory->ec_execute_datanode) {
            int len = strlen(InstrMemory->ec_execute_datanode) + 1;
            operatorMemory->ec_execute_datanode = (char*)palloc(len);
            rc = memcpy_s(operatorMemory->ec_execute_datanode, len, InstrMemory->ec_execute_datanode, len);
            securec_check(rc, "\0", "\0");
        } else {
            operatorMemory->ec_execute_datanode = (char*)palloc0(1);
        }

        if (InstrMemory->ec_dsn) {
            int len = strlen(InstrMemory->ec_dsn) + 1;
            operatorMemory->ec_dsn = (char*)palloc(len);
            rc = memcpy_s(operatorMemory->ec_dsn, len, InstrMemory->ec_dsn, len);
            securec_check(rc, "\0", "\0");
        } else {
            operatorMemory->ec_dsn = (char*)palloc0(1);
        }

        if (InstrMemory->ec_username) {
            int len = strlen(InstrMemory->ec_username) + 1;
            operatorMemory->ec_username = (char*)palloc(len);
            rc = memcpy_s(operatorMemory->ec_username, len, InstrMemory->ec_username, len);
            securec_check(rc, "\0", "\0");
        } else {
            operatorMemory->ec_username = (char*)palloc0(1);
        }

        if (InstrMemory->ec_query) {
            int len = strlen(InstrMemory->ec_query) + 1;
            operatorMemory->ec_query = (char*)palloc(len);
            rc = memcpy_s(operatorMemory->ec_query, len, InstrMemory->ec_query, len);
            securec_check(rc, "\0", "\0");
        } else {
            operatorMemory->ec_query = (char*)palloc0(1);
        }

        MemoryContextSwitchTo(oldcontext);
    }
}

void fillCommonOperatorInfo(size_info* info, OperatorInfo* operator_info)
{
    TimestampTz start_time;

    start_time = operator_info->enter_time;
    if (info->start_time == 0 || (start_time != 0 && start_time < info->start_time))
        info->start_time = start_time;
    if (info->startup_time == -1 || (info->startup_time != -1 && operator_info->startup_time < info->startup_time))
        info->startup_time = operator_info->startup_time;
    if (operator_info->execute_time > info->duration_time)
        info->duration_time = operator_info->execute_time;
    info->warn_prof_info |= operator_info->warning;
    info->warn_prof_info |= (uint)get_warning_info(operator_info->spill_size, info->plan_node_name);
    info->status &= operator_info->status;

    info->total_tuple += operator_info->total_tuples;
    info->total_memory += operator_info->peak_memory;
    info->total_spill_size += operator_info->spill_size;
    info->total_cpu_time += operator_info->execute_time;
    GetMaxAndMinExplainSessionInfo(info, cpu_time, operator_info->execute_time);
    GetMaxAndMinExplainSessionInfo(info, peak_memory, operator_info->peak_memory);
    GetMaxAndMinExplainSessionInfo(info, spill_size, operator_info->spill_size);
}

void fillCommmonOperatorInfoInGeneralInfo(ExplainGeneralInfo* info, size_info* temp_info)
{
    double avg_cpu_time = 0;
    int64 avg_memory = 0;
    int64 avg_spill_size = 0;

    info->total_cpu_time = temp_info->total_cpu_time;
    info->tuple_processed = temp_info->total_tuple;
    info->startup_time = temp_info->startup_time;
    info->duration_time = temp_info->duration_time;
    info->start_time = temp_info->start_time;
    info->warn_prof_info = temp_info->warn_prof_info;
    info->status = temp_info->status;

    if (temp_info->max_cpu_time && temp_info->dn_count > 0) {
        avg_cpu_time = (double)(info->total_cpu_time / temp_info->dn_count);
        info->cpu_skew = (int)((temp_info->max_cpu_time - avg_cpu_time) * 100 / temp_info->max_cpu_time);
    }
    if (temp_info->max_peak_memory && temp_info->dn_count > 0) {
        avg_memory = temp_info->total_memory / temp_info->dn_count;
        info->memory_skewed = (temp_info->max_peak_memory - avg_memory) * 100 / temp_info->max_peak_memory;
    }
    if (temp_info->max_spill_size > 0 && temp_info->dn_count > 0) {
        avg_spill_size = temp_info->total_spill_size / temp_info->dn_count;
        info->i_o_skew = (temp_info->max_spill_size - avg_spill_size) * 100 / temp_info->max_spill_size;
    }

    info->max_peak_memory = convert_to_mb(temp_info->max_peak_memory);
    info->min_peak_memory = convert_to_mb(temp_info->min_peak_memory);
    info->avg_peak_memory = convert_to_mb(avg_memory);
    info->max_spill_size = convert_to_mb(temp_info->max_spill_size);
    info->min_spill_size = convert_to_mb(temp_info->min_spill_size);
    info->avg_spill_size = convert_to_mb(avg_spill_size);
    info->max_cpu_time = temp_info->max_cpu_time;
    info->min_cpu_time = temp_info->min_cpu_time;

    if (strcmp(info->plan_node_name, "Stream") == 0 || strcmp(info->plan_node_name, "VecStream") == 0) {
        info->avg_spill_size = 0;
        info->max_spill_size = 0;
        info->min_spill_size = 0;
    }

    if (info->min_cpu_time == -1)
        info->min_cpu_time = 0;
    if (info->min_peak_memory == -1)
        info->min_peak_memory = 0;
    if (info->min_spill_size == -1)
        info->min_spill_size = 0;

}

void fillSingleNodeGeneralInfo(ExplainGeneralInfo* session_info, OperatorInfo* operator_info)
{
    size_info info = {0};

    info.plan_node_name = session_info->plan_node_name;
    fillCommonOperatorInfo(&info, operator_info);
    fillCommmonOperatorInfoInGeneralInfo(session_info, &info);
}

/*
 * function name: OperatorStrategyFunc4SessionInfo
 * description  : parse msg to suminfo -- call-back function
 * arguments    : msg: messages fetched from DN
 *              : suminfo: structure that data will be stored in
 *              : size: size of structure of suminfo
 * return value : void
 */
void OperatorStrategyFunc4SessionInfo(StringInfo msg, void* sum_info, int size)
{
    size_info* info = (size_info*)sum_info;
    OperatorInfo detail;
    info->has_data = true;

    errno_t rc = memset_s(&detail, sizeof(detail), 0, sizeof(detail));
    securec_check_errval(rc, , LOG);

    detail.total_tuples = pq_getmsgint64(msg);
    detail.peak_memory = pq_getmsgint64(msg);
    detail.spill_size = pq_getmsgint64(msg);
    detail.enter_time = pq_getmsgint64(msg);
    detail.startup_time = pq_getmsgint64(msg);
    detail.execute_time = pq_getmsgint64(msg);
    detail.status = pq_getmsgint(msg, 4);
    detail.warning = pq_getmsgint(msg, 4);
    detail.ec_operator = pq_getmsgint(msg, 4);
    if (detail.ec_operator == IS_EC_OPERATOR) {
        detail.ec_status = pq_getmsgint(msg, 4);
        detail.ec_execute_datanode = (char*)(pq_getmsgstring(msg));
        detail.ec_dsn = (char*)(pq_getmsgstring(msg));
        detail.ec_username = (char*)(pq_getmsgstring(msg));
        detail.ec_query = (char*)(pq_getmsgstring(msg));
        detail.ec_libodbc_type = pq_getmsgint(msg, 4);
        detail.ec_fetch_count = pq_getmsgint64(msg);
    }
    pq_getmsgend(msg);
    fillCommonOperatorInfo(info, &detail);
    info->ec_operator = detail.ec_operator;
    if (info->ec_operator == IS_EC_OPERATOR) {
        info->ec_status = detail.ec_status;
        if (detail.ec_execute_datanode) {
            int len = strlen(detail.ec_execute_datanode) + 1;
            info->ec_execute_datanode = (char*)palloc(len);
            rc = memcpy_s(info->ec_execute_datanode, len, detail.ec_execute_datanode, len);
            securec_check(rc, "\0", "\0");
        } else {
            info->ec_execute_datanode = (char*)palloc0(1);
        }

        if (detail.ec_dsn) {
            int len = strlen(detail.ec_dsn) + 1;
            info->ec_dsn = (char*)palloc(len);
            rc = memcpy_s(info->ec_dsn, len, detail.ec_dsn, len);
            securec_check(rc, "\0", "\0");
        } else {
            info->ec_dsn = (char*)palloc0(1);
        }

        if (detail.ec_username) {
            int len = strlen(detail.ec_username) + 1;
            info->ec_username = (char*)palloc(len);
            rc = memcpy_s(info->ec_username, len, detail.ec_username, len);
            securec_check(rc, "\0", "\0");
        } else {
            info->ec_username = (char*)palloc0(1);
        }

        if (detail.ec_query) {
            int len = strlen(detail.ec_query) + 1;
            info->ec_query = (char*)palloc(len);
            rc = memcpy_s(info->ec_query, len, detail.ec_query, len);
            securec_check(rc, "\0", "\0");
        } else {
            info->ec_query = (char*)palloc0(1);
        }

        info->ec_libodbc_type = detail.ec_libodbc_type;
        info->ec_fetch_count = detail.ec_fetch_count;
    }

    info->dn_count++;
}

/*
 * @Description: send instrumentation info to CN
 * @in sessionMemory: instrumentation info
 * @return - void
 */
void sendExplainInfo(OperatorInfo* session_memory)
{
    StringInfoData ret_buf;
    initStringInfo(&ret_buf);

    pq_beginmessage(&ret_buf, 'u');
    pq_sendint64(&ret_buf, session_memory->total_tuples);
    pq_sendint64(&ret_buf, session_memory->peak_memory);
    pq_sendint64(&ret_buf, session_memory->spill_size);
    pq_sendint64(&ret_buf, session_memory->enter_time);
    pq_sendint64(&ret_buf, session_memory->startup_time);
    pq_sendint64(&ret_buf, session_memory->execute_time);
    pq_sendint32(&ret_buf, session_memory->status);
    pq_sendint32(&ret_buf, session_memory->warning);

    /* send ec info */
    pq_sendint32(&ret_buf, session_memory->ec_operator);
    if (session_memory->ec_operator == IS_EC_OPERATOR) {
        pq_sendint32(&ret_buf, session_memory->ec_status);
        pq_sendstring(&ret_buf, (const char*)session_memory->ec_execute_datanode);
        pq_sendstring(&ret_buf, (const char*)session_memory->ec_dsn);
        pq_sendstring(&ret_buf, (const char*)session_memory->ec_username);
        pq_sendstring(&ret_buf, (const char*)session_memory->ec_query);
        pq_sendint32(&ret_buf, session_memory->ec_libodbc_type);
        pq_sendint64(&ret_buf, session_memory->ec_fetch_count);
    }

    pq_endmessage(&ret_buf);

    return;
}

/*
 * @Description : Get lock according to the hashcode for realtime hash table
 * @in hashCode : hash key value
 * @in lockMode :  lockMode value
 * @return lock if successfully.
 */
LWLock* LockOperRealTHashPartition(uint32 hash_code, LWLockMode lock_mode)
{
    LWLock* partion_lock = GetMainLWLockByIndex(FirstOperatorRealTLock + (hash_code % NUM_OPERATOR_REALTIME_PARTITIONS));

    /*
     * When we abort during acquire lock during collect info from datanodes or update realtime hash
     * table, we should check if current lockid is held by ourself or not, or there will be dead lock
     * because ExplainCreateDNodeInfoOnDN or WLMReplyCollectInfo need it.
     */
    if (LWLockHeldByMe(partion_lock)) {
        HOLD_INTERRUPTS();
        LWLockRelease(partion_lock);
    }

    LWLockAcquire(partion_lock, lock_mode);
    return partion_lock;
}

/*
 * @Description : Get lock according to the hashcode for history hash table
 * @in hashCode : hash key value
 * @in lockMode :  lockMode value
 * @return lock if successfully.
 */
LWLock* LockOperHistHashPartition(uint32 hash_code, LWLockMode lock_mode)
{
    LWLock* partion_lock = GetMainLWLockByIndex(FirstOperatorHistLock + (hash_code % NUM_OPERATOR_HISTORY_PARTITIONS));

    /*
     * When we abort during acquire lock during collect info from datanodes or update history hash
     * table, we should check if current lockid is held by ourself or not, or there will be dead lock
     * because ExplainSetSessionInfo or WLMReplyCollectInfo need it.
     */
    if (LWLockHeldByMe(partion_lock)) {
        HOLD_INTERRUPTS();
        LWLockRelease(partion_lock);
    }

    LWLockAcquire(partion_lock, lock_mode);
    return partion_lock;
}

/*
 * @Description : Release lock according to the hashcode for realtime operator hash table
 * @in hashCode : hash key value
 * @return : void
 */
void UnLockOperRealTHashPartition(uint32 hash_code)
{
    LWLock* partion_lock = GetMainLWLockByIndex(FirstOperatorRealTLock + (hash_code % NUM_OPERATOR_REALTIME_PARTITIONS));
    LWLockRelease(partion_lock);
}

/*
 * @Description : Release lock according to the hashcode for history operator hash table
 * @in hashCode : hash key value
 * @return : void
 */
void UnLockOperHistHashPartition(uint32 hash_code)
{
    LWLock* partion_lock = GetMainLWLockByIndex(FirstOperatorHistLock + (hash_code % NUM_OPERATOR_HISTORY_PARTITIONS));
    LWLockRelease(partion_lock);
}

/*
 * @Description: send instrumentation info to CN
 * @in sessionMemory: instrumentation info
 * @return - void
 */
void initGenralInfo(ExplainGeneralInfo* info)
{
    info->total_cpu_time = 0;
    info->min_cpu_time = 0;
    info->avg_peak_memory = 0;
    info->max_peak_memory = 0;
    info->min_peak_memory = 0;
    info->avg_spill_size = 0;
    info->min_spill_size = 0;
    info->max_spill_size = 0;

    info->warn_prof_info = 0;
    info->status = 0;
    info->tuple_processed = 0;
    info->start_time = 0;
    info->startup_time = 0;
    info->duration_time = 0;
    info->i_o_skew = 0;
    info->memory_skewed = 0;
    info->cpu_skew = 0;
}

void getFinalInfo(ExplainGeneralInfo* info, size_info temp_info)
{
    fillCommmonOperatorInfoInGeneralInfo(info, &temp_info);

    if (!temp_info.ec_execute_datanode) {
        info->ec_operator = NOT_EC_OPERATOR;
    } else {
        info->ec_operator = IS_EC_OPERATOR;
        info->ec_status = temp_info.ec_status;
        errno_t rc = EOK;
        if (temp_info.ec_execute_datanode) {
            int len = strlen(temp_info.ec_execute_datanode) + 1;
            info->ec_execute_datanode = (char*)palloc(len);
            rc = memcpy_s(info->ec_execute_datanode, len, temp_info.ec_execute_datanode, len);
            securec_check(rc, "\0", "\0");
        } else {
            info->ec_execute_datanode = (char*)palloc0(1);
        }

        if (temp_info.ec_dsn) {
            int len = strlen(temp_info.ec_dsn) + 1;
            info->ec_dsn = (char*)palloc(len);
            rc = memcpy_s(info->ec_dsn, len, temp_info.ec_dsn, len);
            securec_check(rc, "\0", "\0");
        } else {
            info->ec_dsn = (char*)palloc0(1);
        }

        if (temp_info.ec_username) {
            int len = strlen(temp_info.ec_username) + 1;
            info->ec_username = (char*)palloc(len);
            rc = memcpy_s(info->ec_username, len, temp_info.ec_username, len);
            securec_check(rc, "\0", "\0");
        } else {
            info->ec_username = (char*)palloc0(1);
        }

        if (temp_info.ec_query) {
            int len = strlen(temp_info.ec_query) + 1;
            info->ec_query = (char*)palloc(len);
            rc = memcpy_s(info->ec_query, len, temp_info.ec_query, len);
            securec_check(rc, "\0", "\0");
        } else {
            info->ec_query = (char*)palloc0(1);
        }

        info->ec_libodbc_type = temp_info.ec_libodbc_type;
        info->ec_fetch_count = temp_info.ec_fetch_count;
    }
}

/*
 * @Description: get operator info from session hashtable
 * @in num: the number of hash entry
 * @return - void
 */
void* ExplainGetSessionStatistics(int* num)
{
    /* check workload manager is valid */
    if (!u_sess->attr.attr_resource.use_workload_manager) {
        ereport(WARNING, (errmsg("workload manager is not valid.")));
        return NULL;
    }
    if (!u_sess->attr.attr_resource.enable_resource_track) {
        ereport(WARNING, (errmsg("enable_resource_track is not valid.")));
        return NULL;
    }

    if (!((IS_SPQ_COORDINATOR || IS_PGXC_COORDINATOR) ||
        IS_SINGLE_NODE)) {
        ereport(WARNING, (errmsg("This view is not allowed on datanode.")));
        return NULL;
    }

    int i = 0;
    int j = 0;
    int rc = EOK;

    ExplainDNodeInfo* p_dnode_info = NULL;
    HASH_SEQ_STATUS hash_seq;

    for (j = 0; j < NUM_OPERATOR_REALTIME_PARTITIONS; j++)
        LWLockAcquire(GetMainLWLockByIndex(FirstOperatorRealTLock + j), LW_SHARED);

    /* get current session info count, we will do nothing if it's 0 */
    if ((*num = hash_get_num_entries(g_operator_table.explain_info_hashtbl)) == 0) {
        for (j = NUM_OPERATOR_REALTIME_PARTITIONS; --j >= 0;)
            LWLockRelease(GetMainLWLockByIndex(FirstOperatorRealTLock + j));
        return NULL;
    }

    ExplainGeneralInfo* stat_element = NULL;
    Size array_size = mul_size(sizeof(ExplainGeneralInfo), (Size)(*num));
    ExplainGeneralInfo* stat_array = (ExplainGeneralInfo*)palloc0(array_size);

    hash_seq_init(&hash_seq, g_operator_table.explain_info_hashtbl);

    bool is_super_user = superuser();
    Oid current_user_id = GetUserId();

    /* Get all real time session statistics from the register info hash table. */
    while ((p_dnode_info = (ExplainDNodeInfo*)hash_seq_search(&hash_seq)) != NULL) {
        if (is_super_user || current_user_id == p_dnode_info->userid) {
            Instrumentation* str = (Instrumentation*)p_dnode_info->explain_entry;
            stat_element = stat_array + i;

            stat_element->plan_node_id = p_dnode_info->qid.plannodeid;
            stat_element->tid = p_dnode_info->qid.procId;
            stat_element->query_id = p_dnode_info->qid.queryId;
            stat_element->execute_on_datanode = p_dnode_info->execute_on_datanode;
            stat_element->estimate_rows = p_dnode_info->estimated_rows;
            stat_element->query_dop = p_dnode_info->query_dop;

            if (p_dnode_info->plan_name) {
                int len = strlen(p_dnode_info->plan_name) + 1;
                stat_element->plan_node_name = (char*)palloc0(len);
                rc = memcpy_s(stat_element->plan_node_name, len, p_dnode_info->plan_name, len);
                securec_check(rc, "\0", "\0");
            } else {
                stat_element->plan_node_name = (char*)palloc0(1);
            }

            if (stat_element->execute_on_datanode == false) {
                OperatorInfo operator_memory;
                setOperatorInfo(&operator_memory, str);
                setCnGeneralInfo(stat_element, &operator_memory);
            }

            ++i;
        }
    }

    for (j = NUM_OPERATOR_REALTIME_PARTITIONS; --j >= 0;)
        LWLockRelease(GetMainLWLockByIndex(FirstOperatorRealTLock + j));

    *num = i;
#if defined(ENABLE_MULTIPLE_NODES) || defined(USE_SPQ)
    if (IS_SPQ_RUNNING) {
        char keystr[NAMEDATALEN] = {0};
        int retry_count = 0;
        PGXCNodeAllHandles* pgxc_handles = NULL;

        retry:
        pgxc_handles = WLMRemoteInfoCollectorStart();

        if (pgxc_handles == NULL) {
            pfree_ext(stat_array);
            *num = 0;
            ereport(LOG, (errmsg("remote collector failed, reason: connect error.")));
            return NULL;
        }

        for (i = 0; i < *num; ++i) {
            stat_element = stat_array + i;

            /* Get real time info from each data nodes */
            if (stat_element->execute_on_datanode) {
                rc = snprintf_s(keystr,
                                NAMEDATALEN,
                                NAMEDATALEN - 1,
                                "%lu,%lu,%d",
                                stat_element->tid,
                                stat_element->query_id,
                                stat_element->plan_node_id);
                securec_check_ss(rc, "\0", "\0");

                int ret = WLMRemoteInfoSender(pgxc_handles, keystr, WLM_COLLECT_OPERATOR_RUNTIME);
                if (ret != 0) {
                    ++retry_count;
                    release_pgxc_handles(pgxc_handles);
                    ereport(WARNING, (errmsg("send failed, retry_count: %d", retry_count)));
                    pg_usleep(3 * USECS_PER_SEC);

                    if (retry_count >= 3)
                        ereport(ERROR,
                                (errcode(ERRCODE_CONNECTION_FAILURE),
                                    errmsg("Remote Sender: Failed to send command to datanode")));

                    goto retry;
                }

                initGenralInfo(stat_element);
                size_info temp_info;
                rc = memset_s(&temp_info, sizeof(size_info), 0, sizeof(size_info));
                securec_check(rc, "\0", "\0");
                temp_info.plan_node_name = stat_element->plan_node_name;
                temp_info.min_cpu_time = -1;
                temp_info.min_peak_memory = -1;
                temp_info.min_spill_size = -1;
                temp_info.dn_count = 0;
                temp_info.startup_time = -1;

                /* Fetch session statistics from each datanode */
                WLMRemoteInfoReceiver(pgxc_handles, &temp_info, sizeof(size_info), OperatorStrategyFunc4SessionInfo);
                if (temp_info.has_data)
                    getFinalInfo(stat_element, temp_info);
                else
                    stat_element->status = true;
            }
        }

        WLMRemoteInfoCollectorFinish(pgxc_handles);
    }
#endif
    return stat_array;
}

/*
 * @Description: insert  into the qid to hashtable for both CN adn DN
 * @in plan_node_id -current plan node id
 * @in instr -instrumentation for current plan node id
 * @in on_datanode - current operator execute on datanode or not
 * @in plan_name - plan node name
 * @in dop - query dop
 * @in estimated_rows - estimated rows
 * @return - void
 */
void ExplainSetSessionInfo(int plan_node_id, Instrumentation* instr, bool on_datanode, const char* plan_name, int dop,
    int64 estimated_rows, TimestampTz current_time, OperatorPlanInfo* opt_plan_info)
{
    bool has_found = false;
    Qpid qid;
    int rc = 0;

    qid.procId = u_sess->instr_cxt.gs_query_id->procId;
    qid.queryId = u_sess->instr_cxt.gs_query_id->queryId;
    qid.plannodeid = plan_node_id;

    if (IsQpidInvalid(&qid))
        return;

    InstrEndLoop(instr);

    uint32 hash_code = GetHashPlanCode(&qid, sizeof(Qpid));

    if (((IS_SPQ_COORDINATOR || IS_PGXC_COORDINATOR) &&
    !IsConnFromCoord()) || IS_SINGLE_NODE) {
        LockOperHistHashPartition(hash_code, LW_EXCLUSIVE);

        ExplainDNodeInfo* p_detail =
            (ExplainDNodeInfo*)hash_search(g_operator_table.collected_info_hashtbl, &qid, HASH_FIND, &has_found);
        /* If we can not find it in the hash table, we will do nothing. */
        if (p_detail == NULL) {
            UnLockOperHistHashPartition(hash_code);
            return;
        }

        rc = memset_s(p_detail, sizeof(ExplainDNodeInfo), 0, sizeof(ExplainDNodeInfo));
        securec_check(rc, "\0", "\0");

        /* Get session info from current thread. */
        p_detail->qid = qid;
        p_detail->userid = GetUserId();
        p_detail->execute_on_datanode = on_datanode;

        MemoryContext old_context;
        old_context = MemoryContextSwitchTo(g_instance.wlm_cxt->oper_resource_track_mcxt);
        int plan_len = strlen(plan_name) + 1;
        p_detail->plan_name = (char*)palloc0(sizeof(char) * plan_len);
        rc = memcpy_s(p_detail->plan_name, plan_len, plan_name, plan_len);
        securec_check(rc, "\0", "\0");
        MemoryContextSwitchTo(old_context);

        p_detail->query_dop = dop;
        p_detail->estimated_rows = estimated_rows;
        p_detail->exptime = TimestampTzPlusMilliseconds(current_time, OPERATOR_INFO_COLLECT_TIMER * MSECS_PER_SEC);

        setOperatorInfo(&p_detail->geninfo, instr, opt_plan_info);
        if (plan_node_id == 1 &&
            (p_detail->geninfo.execute_time / 1000) < u_sess->attr.attr_resource.resource_track_duration) {
            u_sess->instr_cxt.can_record_to_table = false;
        }
        p_detail->can_record_to_table = u_sess->instr_cxt.can_record_to_table;
        p_detail->status = Operator_Normal;
        UnLockOperHistHashPartition(hash_code);
#if !defined(ENABLE_MULTIPLE_NODES) && !defined(USE_SPQ)
            return;
#else
            if (t_thrd.spq_ctx.spq_role == ROLE_UTILITY) {
                return;
            }
#endif
    }

    if (IS_SPQ_COORDINATOR || IS_PGXC_DATANODE) {
        LockOperHistHashPartition(hash_code, LW_EXCLUSIVE);
        ExplainDNodeInfo* p_detail =
            (ExplainDNodeInfo*)hash_search(g_operator_table.collected_info_hashtbl, &qid, HASH_ENTER, &has_found);

        if (p_detail == NULL || has_found) {
            UnLockOperHistHashPartition(hash_code);
            return;
        }

        rc = memset_s(p_detail, sizeof(ExplainDNodeInfo), 0, sizeof(ExplainDNodeInfo));
        securec_check(rc, "\0", "\0");

        p_detail->qid = qid;
        p_detail->execute_on_datanode = true;

        setOperatorInfo(&p_detail->geninfo, instr, opt_plan_info);

        UnLockOperHistHashPartition(hash_code);
    }
}

static inline void PredSetStatElemt(ExplainGeneralInfo* stat_element, ExplainDNodeInfo* pDetail)
{
    CpyCStringField(&stat_element->operation, pDetail->geninfo.planinfo.operation);
    CpyCStringField(&stat_element->orientation, pDetail->geninfo.planinfo.orientation);
    CpyCStringField(&stat_element->strategy, pDetail->geninfo.planinfo.strategy);
    CpyCStringField(&stat_element->options, pDetail->geninfo.planinfo.options);
    CpyCStringField(&stat_element->condition, pDetail->geninfo.planinfo.condition);
    CpyCStringField(&stat_element->projection, pDetail->geninfo.planinfo.projection);
    CpyCStringField(&stat_element->datname, pDetail->geninfo.datname);
    stat_element->parent_node_id = pDetail->geninfo.planinfo.parent_node_id;
    stat_element->left_child_id = pDetail->geninfo.planinfo.left_child_id;
    stat_element->right_child_id = pDetail->geninfo.planinfo.right_child_id;
}

/*
 * @Description: get operator info from session hashtable
 * @in qid: plan node id, query id and proc id
 * @in removed: remove hash entry from hashtable.
 * @in num: the number of hash entry
 * @return - void
 */
void* ExplainGetSessionInfo(const Qpid* qid, int removed, int* num)
{
    int j;
    /* check workload manager is valid */
    if (!u_sess->attr.attr_resource.use_workload_manager) {
        ereport(WARNING, (errmsg("workload manager is not valid.")));
        return NULL;
    }
    if (!u_sess->attr.attr_resource.enable_resource_track) {
        ereport(WARNING, (errmsg("enable_resource_track is not valid.")));
        return NULL;
    }

    if (IS_SPQ_COORDINATOR || IS_PGXC_COORDINATOR || IS_SINGLE_NODE) {
        TimestampTz current_time = GetCurrentTimestamp();

        for (j = 0; j < NUM_OPERATOR_HISTORY_PARTITIONS; j++)
            LWLockAcquire(GetMainLWLockByIndex(FirstOperatorHistLock + j), LW_EXCLUSIVE);

        /* no records, nothing to do. */
        if ((*num = hash_get_num_entries(g_operator_table.collected_info_hashtbl)) <= 0) {
            for (j = NUM_OPERATOR_HISTORY_PARTITIONS; --j >= 0;)
                LWLockRelease(GetMainLWLockByIndex(FirstOperatorHistLock + j));
            return NULL;
        }

        errno_t rc = EOK;
        ExplainDNodeInfo* p_detail = NULL;
        ExplainGeneralInfo* stat_element = NULL;
        Size array_size = mul_size(sizeof(ExplainGeneralInfo), (Size)(*num));
        ExplainGeneralInfo* stat_array = (ExplainGeneralInfo*)palloc0(array_size);

        HASH_SEQ_STATUS hash_seq;
        hash_seq_init(&hash_seq, g_operator_table.collected_info_hashtbl);

        bool is_super_user = superuser();
        Oid current_user_id = GetUserId();

        int record_pos = 0;
        int un_record_pos = *num - 1;

        /* Fetch all session info from the hash table. */
        while ((p_detail = (ExplainDNodeInfo*)hash_seq_search(&hash_seq)) != NULL) {
            if (is_super_user || current_user_id == p_detail->userid) {
                if (p_detail->status == Operator_Pending) {
                    continue;
                }

                if (p_detail->status == Operator_Invalid || !p_detail->can_record_to_table) {
                    stat_element = stat_array + un_record_pos;
                    un_record_pos--;
                } else {
                    stat_element = stat_array + record_pos;
                    record_pos++;
                }

                stat_element->plan_node_id = p_detail->qid.plannodeid;
                stat_element->tid = p_detail->qid.procId;
                stat_element->query_id = p_detail->qid.queryId;
                stat_element->execute_on_datanode = p_detail->execute_on_datanode;
                stat_element->estimate_rows = p_detail->estimated_rows;
                stat_element->query_dop = p_detail->query_dop;
                stat_element->remove = false;

                if (p_detail->plan_name) {
                    int len = strlen(p_detail->plan_name) + 1;
                    stat_element->plan_node_name = (char*)palloc(len);
                    rc = memcpy_s(stat_element->plan_node_name, len, p_detail->plan_name, len);
                    securec_check(rc, "\0", "\0");
                } else {
                    stat_element->plan_node_name = (char*)palloc0(1);
                }

                PredSetStatElemt(stat_element, p_detail);

                if ((p_detail->execute_on_datanode == false) || IS_SINGLE_NODE) {
                    setCnGeneralInfo(stat_element, &p_detail->geninfo);
                }

                if (IS_SINGLE_NODE) {
                    fillSingleNodeGeneralInfo(stat_element, &p_detail->geninfo);
                }

                if (u_sess->attr.attr_resource.enable_resource_record) {
                    if (removed > 0)
                        stat_element->remove = true;
                } else {
                    if (p_detail->exptime <= current_time)
                        stat_element->remove = true;
                }

                if (p_detail->status == Operator_Invalid) {
                    stat_element->remove = true;
                    stat_element->execute_on_datanode = true;
                }

                /* remove it from the hash table. */
                if (stat_element->remove == true) {
                    if (p_detail->plan_name)
                        pfree_ext(p_detail->plan_name);
                    hash_search(g_operator_table.collected_info_hashtbl, &p_detail->qid, HASH_REMOVE, NULL);
                }
            }
        }

        for (j = NUM_OPERATOR_HISTORY_PARTITIONS; --j >= 0;)
            LWLockRelease(GetMainLWLockByIndex(FirstOperatorHistLock + j));
#if defined(ENABLE_MULTIPLE_NODES) || defined(USE_SPQ)
        if (t_thrd.spq_ctx.spq_role != ROLE_UTILITY) {
            int retry_count = 0;
            int i;
            PGXCNodeAllHandles* pgxc_handles = NULL;
            char keystr[NAMEDATALEN];

            retry:
            pgxc_handles = WLMRemoteInfoCollectorStart();

            if (pgxc_handles == NULL) {
                pfree_ext(stat_array);
                *num = 0;
                return NULL;
            }

            for (i = 0; i < *num; ++i) {
                if (i >= record_pos && i <= un_record_pos) {
                    continue;
                }

                stat_element = stat_array + i;

                if (stat_element->execute_on_datanode) {
                    rc = snprintf_s(keystr,
                                    NAMEDATALEN,
                                    NAMEDATALEN - 1,
                                    "%lu,%lu,%d,%d",
                                    stat_element->tid,
                                    stat_element->query_id,
                                    stat_element->plan_node_id,
                                    stat_element->remove);
                    securec_check_ss(rc, "\0", "\0");

                    int ret = WLMRemoteInfoSender(pgxc_handles, keystr, WLM_COLLECT_OPERATOR_SESSION);

                    if (ret != 0) {
                        ++retry_count;
                        release_pgxc_handles(pgxc_handles);
                        ereport(WARNING, (errmsg("send failed, retry_count: %d", retry_count)));

                        pg_usleep(3 * USECS_PER_SEC);

                        if (retry_count >= 3)
                            ereport(ERROR,
                                    (errcode(ERRCODE_CONNECTION_FAILURE),
                                        errmsg("Remote Sender: Failed to send command to datanode")));
                        goto retry;
                    }

                    initGenralInfo(stat_element);
                    size_info temp_info;
                    rc = memset_s(&temp_info, sizeof(size_info), 0, sizeof(size_info));
                    securec_check(rc, "\0", "\0");
                    temp_info.plan_node_name = stat_element->plan_node_name;
                    temp_info.min_cpu_time = -1;
                    temp_info.min_peak_memory = -1;
                    temp_info.min_spill_size = -1;
                    temp_info.dn_count = 0;
                    temp_info.startup_time = -1;

                    /* Fetch session statistics from each datanode */
                    WLMRemoteInfoReceiver(pgxc_handles, &temp_info, sizeof(size_info), OperatorStrategyFunc4SessionInfo);

                    if (temp_info.has_data)
                        getFinalInfo(stat_element, temp_info);
                }
            }

            WLMRemoteInfoCollectorFinish(pgxc_handles);
        }
#endif
        *num = record_pos;
        return stat_array;
    }

    return NULL;
}

/*
 * @Description: removed hash entry from hashtable
 * @return - void
 */
void releaseExplainTable()
{
    if (!u_sess->attr.attr_resource.use_workload_manager ||
        u_sess->attr.attr_resource.resource_track_level != RESOURCE_TRACK_OPERATOR)
        return;

    int plan_number = u_sess->instr_cxt.operator_plan_number;
    Qpid qid;
    bool found = false;

    qid.procId = u_sess->instr_cxt.gs_query_id->procId;
    qid.queryId = u_sess->instr_cxt.gs_query_id->queryId;

    if (qid.queryId <= 0 || qid.procId <= 0) {
        return;
    }

    for (int i = 0; i <= plan_number; i++) {
        qid.plannodeid = i;
        uint32 hash_code = GetHashPlanCode(&qid, sizeof(Qpid));
        LockOperHistHashPartition(hash_code, LW_EXCLUSIVE);

        ExplainDNodeInfo* p_dnode_info =
            (ExplainDNodeInfo*)hash_search(g_operator_table.collected_info_hashtbl, &qid, HASH_FIND, &found);

        if (found && p_dnode_info != NULL) {
            p_dnode_info->status = Operator_Invalid;
        }

        if (!IS_SPQ_COORDINATOR && IS_PGXC_DATANODE) {
            hash_search(g_operator_table.collected_info_hashtbl, &qid, HASH_REMOVE, NULL);
        }

        UnLockOperHistHashPartition(hash_code);
    }

    for (int i = 0; i <= plan_number; i++) {
        qid.plannodeid = i;
        uint32 hash_code = GetHashPlanCode(&qid, sizeof(Qpid));
        LockOperRealTHashPartition(hash_code, LW_EXCLUSIVE);

        ExplainDNodeInfo* p_dnode_info =
            (ExplainDNodeInfo*)hash_search(g_operator_table.explain_info_hashtbl, &qid, HASH_FIND, &found);
        if (found && (IS_SPQ_COORDINATOR || IS_PGXC_COORDINATOR || IS_SINGLE_NODE) &&
            p_dnode_info != NULL && p_dnode_info->plan_name != NULL) {
                pfree_ext(p_dnode_info->plan_name);
        }

        hash_search(g_operator_table.explain_info_hashtbl, &qid, HASH_REMOVE, NULL);
        UnLockOperRealTHashPartition(hash_code);
    }
}

void removeExplainInfo(int plan_node_id)
{
    if (!u_sess->attr.attr_resource.use_workload_manager ||
        u_sess->attr.attr_resource.resource_track_level != RESOURCE_TRACK_OPERATOR)
        return;

    Qpid qid;
    bool found = false;

    qid.procId = u_sess->instr_cxt.gs_query_id->procId;
    qid.queryId = u_sess->instr_cxt.gs_query_id->queryId;
    qid.plannodeid = plan_node_id;

    if (IsQpidInvalid(&qid))
        return;

    uint32 hash_code = GetHashPlanCode(&qid, sizeof(Qpid));
    LockOperRealTHashPartition(hash_code, LW_EXCLUSIVE);

    ExplainDNodeInfo* p_dnode_info =
        (ExplainDNodeInfo*)hash_search(g_operator_table.explain_info_hashtbl, &qid, HASH_FIND, &found);
    if (found && (IS_SPQ_COORDINATOR || IS_PGXC_COORDINATOR || IS_SINGLE_NODE) &&
        p_dnode_info != NULL && p_dnode_info->plan_name != NULL) {
        pfree_ext(p_dnode_info->plan_name);
    }

    hash_search(g_operator_table.explain_info_hashtbl, &qid, HASH_REMOVE, NULL);
    UnLockOperRealTHashPartition(hash_code);
}

/*
 * @Description: release EC memory in OperatorInfo
 * @return - void
 */
void releaseOperatorInfoEC(OperatorInfo* operator_memory)
{
    if (operator_memory->ec_operator == IS_EC_OPERATOR) {
        operator_memory->ec_operator = NOT_EC_OPERATOR;

        operator_memory->ec_fetch_count = 0;

        operator_memory->ec_status = EC_STATUS_INIT;

        operator_memory->ec_libodbc_type = 0;

        if (operator_memory->ec_execute_datanode) {
            pfree_ext(operator_memory->ec_execute_datanode);
        }
        if (operator_memory->ec_dsn) {
            pfree_ext(operator_memory->ec_dsn);
        }
        if (operator_memory->ec_username) {
            pfree_ext(operator_memory->ec_username);
        }
        if (operator_memory->ec_query) {
            pfree_ext(operator_memory->ec_query);
        }
    }
}
