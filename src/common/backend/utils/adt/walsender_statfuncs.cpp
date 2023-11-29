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
 * walsender_statfuncs.cpp
 *    stream main Interface.
 *
 * IDENTIFICATION
 *    src/common/backend/utils/adt/walsender_statfuncs.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "fmgr/fmgr_comp.h"
#include "funcapi.h"
#include "knl/knl_variable.h"
#include "postgres.h"
#include "replication/walsender_private.h"
#include "securec.h"
#include "storage/spin.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "utils/statfuncs.h"
#include "utils/timestamp.h"
#include "catalog/pg_type.h"

#define WAL_SENDER_STATS_COLS 9

typedef struct SenderStatUserContext {
    int curIdx;
    bool isEnableStat;
    int maxSize;
    int usedCount;
    TimestampTz curTime;
    TimestampTz lastResetTime;
    WalSenderStat* outputStats;
} SenderStatUserContext;

/**
 * gs_stat_walsender
 *     obtain the running status of WalSender.
 * input args:
 *     operation: int, -1: disable, 0: reset, 1: enable, 2: get
 * output columns:
 *     is_enable_stat
 *     channel
 *     cur_time
 *     send_times
 *     first_send_time
 *     last_send_time
 *     last_reset_time
 *     avg_send_interval
 *     since_last_send_interval
 */
Datum gs_stat_walsender(PG_FUNCTION_ARGS)
{
    const int operation = PG_GETARG_INT32(0);
    FuncCallContext* funcContext = nullptr;
    SenderStatUserContext* userContext = nullptr;
    errno_t rc = 0;

    if (SRF_IS_FIRSTCALL()) {
        funcContext = SRF_FIRSTCALL_INIT();
        const int maxSize = g_instance.attr.attr_storage.max_wal_senders;
        MemoryContext oldContext = MemoryContextSwitchTo(funcContext->multi_call_memory_ctx);
        userContext = (SenderStatUserContext*)palloc(sizeof(SenderStatUserContext));
        userContext->outputStats = (WalSenderStat*)palloc(sizeof(WalSenderStat) * maxSize);

        volatile WalSenderStats* senderStats = g_instance.wal_cxt.walSenderStats;
        SpinLockAcquire(&senderStats->mutex);
        TimestampTz curTime = GetCurrentTimestamp();

        switch (operation) {
            case STAT_OPER_GET:
                break;
            case STAT_OPER_ENABLE:
                senderStats->isEnableStat = true;
                break;
            case STAT_OPER_DISABLE:
                senderStats->isEnableStat = false;
                break;
            case STAT_OPER_RESET:
                senderStats->lastResetTime = curTime;
                if (senderStats->stats == nullptr) {
                    break;
                }
                for (int i = 0; i < maxSize; ++i) {
                    WalSenderStat* stat = senderStats->stats[i];
                    if (stat) {
                        stat->sendTimes = 0;
                        stat->firstSendTime = 0;
                        stat->lastSendTime = 0;
                    }
                }
                break;
            default:
                SpinLockRelease(&senderStats->mutex);
                ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                    errmsg("illegal value \"%d\" for parameter \"operation\".", operation),
                    errhint("-1: disable, 0: reset, 1: enable, 2: get")));
                SRF_RETURN_DONE(funcContext);
        }

        userContext->isEnableStat = senderStats->isEnableStat;
        userContext->lastResetTime = senderStats->lastResetTime;
        userContext->curTime = curTime;
        userContext->curIdx = 0;
        userContext->usedCount = 0;
        userContext->maxSize = maxSize;

        if (senderStats->stats) {
            for (int i = 0; i < maxSize; ++i) {
                WalSenderStat* walStat = senderStats->stats[i];
                if (walStat == nullptr || walStat->walsnd == nullptr) {
                    continue;
                }
                
                WalSenderStat& outStats = userContext->outputStats[userContext->usedCount];
                userContext->usedCount++;
                outStats.sendTimes = walStat->sendTimes;
                outStats.firstSendTime = walStat->firstSendTime;
                outStats.lastSendTime = walStat->lastSendTime;
                outStats.walsnd = walStat->walsnd;
            }
        }
        SpinLockRelease(&senderStats->mutex);

        TupleDesc tupDesc = CreateTemplateTupleDesc(WAL_SENDER_STATS_COLS, false);
        int attno = 0;
        TupleDescInitEntry(tupDesc, (AttrNumber) ++attno, "is_enable_stat", BOOLOID, -1, 0);
        TupleDescInitEntry(tupDesc, (AttrNumber) ++attno, "channel", TEXTOID, -1, 0);
        TupleDescInitEntry(tupDesc, (AttrNumber) ++attno, "cur_time", TIMESTAMPTZOID, -1, 0);
        TupleDescInitEntry(tupDesc, (AttrNumber) ++attno, "send_times", UINT8OID, -1, 0);
        TupleDescInitEntry(tupDesc, (AttrNumber) ++attno, "first_send_time", TIMESTAMPTZOID, -1, 0);
        TupleDescInitEntry(tupDesc, (AttrNumber) ++attno, "last_send_time", TIMESTAMPTZOID, -1, 0);
        TupleDescInitEntry(tupDesc, (AttrNumber) ++attno, "last_reset_time", TIMESTAMPTZOID, -1, 0);
        TupleDescInitEntry(tupDesc, (AttrNumber) ++attno, "avg_send_interval", UINT8OID, -1, 0);
        TupleDescInitEntry(tupDesc, (AttrNumber) ++attno, "since_last_send_interval", UINT8OID, -1, 0);

        Assert(WAL_SENDER_STATS_COLS == attno);

        funcContext->tuple_desc = BlessTupleDesc(tupDesc);
        funcContext->user_fctx = (void*)userContext;

        MemoryContextSwitchTo(oldContext);
    }

    funcContext = SRF_PERCALL_SETUP();
    userContext = (SenderStatUserContext*)funcContext->user_fctx;

    while (userContext->curIdx < userContext->usedCount) {
        WalSenderStat& outStat = userContext->outputStats[userContext->curIdx++];
        int localport = 0;
        int remoteport = 0;
        char localip[IP_LEN] = {0};
        char remoteip[IP_LEN] = {0};
        char location[MAXFNAMELEN * 3] = {0};
        volatile WalSnd* walsnd = outStat.walsnd;
        if (walsnd == nullptr) {
            continue;
        }

        SpinLockAcquire(&walsnd->mutex);
        if (walsnd->pid == 0) {
            SpinLockRelease(&walsnd->mutex);
            continue;
        }
        localport = walsnd->wal_sender_channel.localport;
        remoteport = walsnd->wal_sender_channel.remoteport;
        rc = strncpy_s(localip, IP_LEN, (char*)walsnd->wal_sender_channel.localhost, IP_LEN - 1);
        securec_check(rc, "\0", "\0");
        rc = strncpy_s(remoteip, IP_LEN, (char*)walsnd->wal_sender_channel.remotehost, IP_LEN - 1);
        securec_check(rc, "\0", "\0");
        SpinLockRelease(&walsnd->mutex);

        localip[IP_LEN - 1] = '\0';
        remoteip[IP_LEN - 1] = '\0';

        bool isnulls[WAL_SENDER_STATS_COLS] = {false};
        Datum values[WAL_SENDER_STATS_COLS];
        int colIdx = 0;

        /* is_enable_stat */
        values[colIdx++] = BoolGetDatum(userContext->isEnableStat);

        /* channel */
        if (strlen(localip) == 0 || strlen(remoteip) == 0 || localport == 0 || remoteport == 0) {
            location[0] = '\0';
        } else {
            int ret = snprintf_s(location, sizeof(location), sizeof(location) - 1, "%s:%d-->%s:%d",
                            localip, localport, remoteip, remoteport);
            securec_check_ss(ret, "\0", "\0");
        }
        values[colIdx++] = CStringGetTextDatum(location);

        /* cur_time */
        values[colIdx++] = TimestampTzGetDatum(userContext->curTime);
        /* send_times */
        values[colIdx++] = UInt64GetDatum(outStat.sendTimes);
        /* first_send_time */
        isnulls[colIdx] = (outStat.firstSendTime == 0);
        values[colIdx++] = TimestampTzGetDatum(outStat.firstSendTime);
        /* last_send_time */
        isnulls[colIdx] = (outStat.lastSendTime == 0);
        values[colIdx++] = TimestampTzGetDatum(outStat.lastSendTime);
        /* last_reset_time */
        isnulls[colIdx] = (userContext->lastResetTime == 0);
        values[colIdx++] = TimestampTzGetDatum(userContext->lastResetTime);
       
        /* avg_send_interval */
        int64 avgInterval;
        if (outStat.sendTimes > 1) {
            avgInterval = (outStat.lastSendTime - outStat.firstSendTime) / (outStat.sendTimes - 1);
        } else {
            avgInterval = 0;
            isnulls[colIdx] = true;
        }
        values[colIdx++] = UInt64GetDatum(avgInterval);

        /* since_last_send_interval */
        int64 lastInterval;
        if (outStat.lastSendTime > 0) {
            lastInterval = userContext->curTime - outStat.lastSendTime;
        } else {
            lastInterval = 0;
            isnulls[colIdx] = true;
        }
        values[colIdx++] = UInt64GetDatum(lastInterval);

        Assert(WAL_SENDER_STATS_COLS == colIdx);

        HeapTuple tuple = heap_form_tuple(funcContext->tuple_desc, values, isnulls);
        Datum result = HeapTupleGetDatum(tuple);
        SRF_RETURN_NEXT(funcContext, result);
    }

    SRF_RETURN_DONE(funcContext);
}
