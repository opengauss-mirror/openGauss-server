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
 * instr_event.cpp
 *   functions for wait status event
 *
 * IDENTIFICATION
 *    src/gausskernel/cbb/instruments/event/instr_event.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"
#include "c.h"
#include "instruments/instr_event.h"
#include "utils/builtins.h"
#include "utils/syscache.h"
#include "pgstat.h"
#include "commands/async.h"
#include "funcapi.h"
#include "storage/lmgr.h"
#include "workload/statctl.h"
#include "instruments/instr_statement.h"
#include "ddes/dms/ss_dms.h"
#include "instruments/instr_waitevent.h"

const int MASK_CLASS_ID = 0xFF000000;
const int MASK_EVENT_ID = 0x00FFFFFF;

static bool gs_compare_and_swap_64(volatile int64* dest, int64 oldval, int64 newval)
{
    if (oldval == newval) {
        return true;
    }

    return __sync_bool_compare_and_swap(dest, oldval, newval);
}

/*
 * updateMaxValueForAtomicType - using atomic type to store max value,
 * we need update the max value by using atomic method
 */
static void updateMaxValueForAtomicType(int64 new_val, volatile int64* max)
{
    int64 prev;
    do {
        prev = *max;
    } while (prev < new_val && !gs_compare_and_swap_64(max, prev, new_val));
}

/*
 * updateMinValueForAtomicType - update ming value for atomic type
 */
static void updateMinValueForAtomicType(int64 new_val, volatile int64* mix)
{
    int64 prev;
    do {
        prev = *mix;
    } while ((prev == 0 || prev > new_val) && !gs_compare_and_swap_64(mix, prev, new_val));
}

static inline void UpdateMinValue(int64 newVal, int64* min)
{
    int64 prev = *min;

    *min = ((prev == 0) || (newVal < prev)) ? newVal : prev;
}

static inline void UpdateMaxValue(int64 newVal, int64* max)
{
    int64 prev = *max;

    *max = (newVal > prev) ? newVal : prev;
}

static uint32 get_event_id(uint32 wait_event_info)
{
    uint32 classId = wait_event_info & MASK_CLASS_ID;
    uint32 eventId = wait_event_info & MASK_EVENT_ID; /* For LWLock, use trancheId directly. */

    switch (classId) {
        case PG_WAIT_LWLOCK: {
            if (eventId >= LWLOCK_EVENT_NUM) {
                ereport(LOG, (errmsg("lwlock trancheId %u", wait_event_info)));
                eventId = UINT32_MAX;
            }
            break;
        }
        case PG_WAIT_LOCK:
            if (eventId >= LOCK_EVENT_NUM) {
                ereport(LOG, (errmsg("lockid %u", wait_event_info)));
                eventId = UINT32_MAX;
            }
            break;
        case PG_WAIT_IO:
            eventId = (WaitEventIO)wait_event_info - (WaitEventIO)WAIT_EVENT_BUFFILE_READ;
            if (eventId >= IO_EVENT_NUM) {
                ereport(LOG, (errmsg("io eventId %u", wait_event_info)));
                eventId = UINT32_MAX;
            }
            break;
        case PG_WAIT_DMS:
            eventId = (WaitEventDMS)wait_event_info - (WaitEventDMS)WAIT_EVENT_IDLE_WAIT;
            if (eventId >= DMS_EVENT_NUM) {
                ereport(LOG, (errmsg("dms eventId %u", wait_event_info)));
                eventId = UINT32_MAX;
            }
            break;
        default:
            eventId = UINT32_MAX;
            break;
    }
    return eventId;
}

/* using for DBE_PERF.wait_events */
static void update_max_last_updated(volatile WaitStatisticsInfo* event, TimestampTz last_updated)
{
    if (event != NULL && last_updated > event->last_updated) {
        event->last_updated = last_updated;
    }
}

/* using for DBE_PERF.wait_events */
static void updateWaitStatusInfo(WaitInfo* gsInstrWaitInfo, WaitStatusInfo status_info)
{
    for (int i = 0; i < STATE_WAIT_NUM; i++) {
        update_max_last_updated(&gsInstrWaitInfo->status_info.statistics_info[i],
            status_info.statistics_info[i].last_updated);
        if (status_info.statistics_info[i].counter == 0) {
            continue;
        }
        updateMinValueForAtomicType(status_info.statistics_info[i].min_duration,
            &(gsInstrWaitInfo->status_info.statistics_info[i].min_duration));
        updateMaxValueForAtomicType(status_info.statistics_info[i].max_duration,
            &(gsInstrWaitInfo->status_info.statistics_info[i].max_duration));
        gsInstrWaitInfo->status_info.statistics_info[i].counter += status_info.statistics_info[i].counter;
        gsInstrWaitInfo->status_info.statistics_info[i].total_duration += status_info.statistics_info[i].total_duration;
        gsInstrWaitInfo->status_info.statistics_info[i].avg_duration =
            gsInstrWaitInfo->status_info.statistics_info[i].total_duration /
            gsInstrWaitInfo->status_info.statistics_info[i].counter;
    }
}

/* init all events's last updated time for all backend entries */
void InstrWaitEventInitLastUpdated(PgBackendStatus* current_entry, TimestampTz current_time)
{
    if (current_entry == NULL) {
        return;
    }
    int i = 0;

    /* io event */
    for (i = 0; i < IO_EVENT_NUM; i++) {
        current_entry->waitInfo.event_info.io_info[i].last_updated = current_time;
    }

    /* dms event */
    for (i = 0; i < DMS_EVENT_NUM; i++) {
        current_entry->waitInfo.event_info.dms_info[i].last_updated = current_time;
    }

    /* lock info */
    for (i = 0; i < LOCK_EVENT_NUM; i++) {
        current_entry->waitInfo.event_info.lock_info[i].last_updated = current_time;
    }

    /* lwlock info */
    for (i = 0; i < LWLOCK_EVENT_NUM; i++) {
        current_entry->waitInfo.event_info.lwlock_info[i].last_updated = current_time;
    }

    /* status */
    for (i = 0; i < STATE_WAIT_NUM + 1; i++) {
        current_entry->waitInfo.status_info.statistics_info[i].last_updated = current_time;
    }
}

/* update last updated time of wait event  */
static void instr_wait_event_report_last_updated(volatile WaitStatisticsInfo* event)
{
    event->last_updated = GetCurrentTimestamp();
}

void UpdateWaitStatusStat(volatile WaitInfo* InstrWaitInfo, uint32 waitstatus, int64 duration, TimestampTz current_time)
{
    /* Because the time precision is microseconds,
     * all actions less than microseconds are recorded as 0.
     * When the duration is 0, we set the duration to 1
     */
    duration = (duration == 0) ? 1 : duration;
    instr_stmt_set_wait_events_bitmap(PG_WAIT_STATE, waitstatus);
    updateMinValueForAtomicType(duration,
        &(InstrWaitInfo->status_info.statistics_info[waitstatus].min_duration));
    InstrWaitInfo->status_info.statistics_info[waitstatus].counter++;
    InstrWaitInfo->status_info.statistics_info[waitstatus].total_duration += duration;
    updateMaxValueForAtomicType(duration, &(InstrWaitInfo->status_info.statistics_info[waitstatus].max_duration));
    InstrWaitInfo->status_info.statistics_info[waitstatus].last_updated = current_time;
}

void UpdateWaitEventStat(WaitInfo* instrWaitInfo, uint32 wait_event_info, int64 duration, TimestampTz currentTime)
{
    uint32 classId = wait_event_info & MASK_CLASS_ID;
    uint32 eventId = get_event_id(wait_event_info);
    if (eventId == UINT32_MAX) {
        return;
    }
    /* Because the time precision is microseconds,
     * all actions less than microseconds are recorded as 0.
     * When the duration is 0, we set the duration to 1
     */
    duration = (duration == 0) ? 1 : duration;
    instr_stmt_set_wait_events_bitmap(classId, eventId);
    switch (classId) {
        case PG_WAIT_LWLOCK:
            UpdateMinValue(duration,
                &(instrWaitInfo->event_info.lwlock_info[eventId].min_duration));
            instrWaitInfo->event_info.lwlock_info[eventId].counter++;
            instrWaitInfo->event_info.lwlock_info[eventId].total_duration += duration;
            UpdateMaxValue(duration, &(instrWaitInfo->event_info.lwlock_info[eventId].max_duration));
            instrWaitInfo->event_info.lwlock_info[eventId].last_updated = currentTime;
            break;
        case PG_WAIT_LOCK:
            UpdateMinValue(duration,
                &(instrWaitInfo->event_info.lock_info[eventId].min_duration));
            instrWaitInfo->event_info.lock_info[eventId].counter++;
            instrWaitInfo->event_info.lock_info[eventId].total_duration += duration;
            UpdateMaxValue(duration, &(instrWaitInfo->event_info.lock_info[eventId].max_duration));
            instrWaitInfo->event_info.lock_info[eventId].last_updated = currentTime;
            break;
        case PG_WAIT_IO:
            if (wait_event_info != WAIT_EVENT_WAL_BUFFER_ACCESS) {
                UpdateMinValue(duration,
                    &(instrWaitInfo->event_info.io_info[eventId].min_duration));
                instrWaitInfo->event_info.io_info[eventId].counter++;
                instrWaitInfo->event_info.io_info[eventId].total_duration += duration;
                UpdateMaxValue(duration, &(instrWaitInfo->event_info.io_info[eventId].max_duration));
                instrWaitInfo->event_info.io_info[eventId].last_updated = currentTime;
            } else {
                instrWaitInfo->event_info.io_info[eventId].counter++;
            }
            break;
        case PG_WAIT_DMS:
            UpdateMinValue(duration,
                &(instrWaitInfo->event_info.dms_info[eventId].min_duration));
            instrWaitInfo->event_info.dms_info[eventId].counter++;
            instrWaitInfo->event_info.dms_info[eventId].total_duration += duration;
            UpdateMaxValue(duration, &(instrWaitInfo->event_info.dms_info[eventId].max_duration));
            instrWaitInfo->event_info.dms_info[eventId].last_updated = currentTime;
            break;
        default:
            break;
    }
}

void UpdateWaitEventFaildStat(volatile WaitInfo* InstrWaitInfo, uint32 wait_event_info)
{
    uint32 classId = wait_event_info & MASK_CLASS_ID;
    uint32 eventId = get_event_id(wait_event_info);
    if (eventId == UINT32_MAX) {
        return;
    }

    switch (classId) {
        case PG_WAIT_LWLOCK:
            InstrWaitInfo->event_info.lwlock_info[eventId].failed_counter++;
            instr_wait_event_report_last_updated(&InstrWaitInfo->event_info.lwlock_info[eventId]);
            break;
        case PG_WAIT_LOCK:
            InstrWaitInfo->event_info.lock_info[eventId].failed_counter++;
            instr_wait_event_report_last_updated(&InstrWaitInfo->event_info.lock_info[eventId]);
            break;
        default:
            break;
    }
}

/* using in DBE_PERF.wait_events */
void CollectWaitInfo(WaitInfo* gsInstrWaitInfo, WaitStatusInfo status_info, WaitEventInfo event_info)
{
    /* update status wait info */
    updateWaitStatusInfo(gsInstrWaitInfo, status_info);

    /* update Io Event wait info */
    for (int i = 0; i < IO_EVENT_NUM; i++) {
        WaitStatisticsInfo *io_info = &gsInstrWaitInfo->event_info.io_info[i];

        update_max_last_updated(io_info, event_info.io_info[i].last_updated);
        if (event_info.io_info[i].counter != 0) {
            updateMinValueForAtomicType(event_info.io_info[i].min_duration,
                &io_info->min_duration);
            updateMaxValueForAtomicType(event_info.io_info[i].max_duration, &io_info->max_duration);
            io_info->counter += event_info.io_info[i].counter;
            io_info->total_duration += event_info.io_info[i].total_duration;
            io_info->avg_duration = io_info->total_duration / io_info->counter;
        }
    }

    /* update Dms Event wait info */
    for (int i = 0; i < DMS_EVENT_NUM; i++) {
        WaitStatisticsInfo *dms_info = &gsInstrWaitInfo->event_info.dms_info[i];

        update_max_last_updated(dms_info, event_info.dms_info[i].last_updated);
        if (event_info.dms_info[i].counter != 0) {
            updateMinValueForAtomicType(event_info.dms_info[i].min_duration,
                &dms_info->min_duration);
            updateMaxValueForAtomicType(event_info.dms_info[i].max_duration, &dms_info->max_duration);
            dms_info->counter += event_info.dms_info[i].counter;
            dms_info->total_duration += event_info.dms_info[i].total_duration;
            dms_info->avg_duration = dms_info->total_duration / dms_info->counter;
        }
    }

    /* update Lock Event wait info */
    for (int i = 0; i < LOCK_EVENT_NUM; i++) {
        WaitStatisticsInfo *lock_info = &gsInstrWaitInfo->event_info.lock_info[i];

        update_max_last_updated(lock_info, event_info.lock_info[i].last_updated);
        if (event_info.lock_info[i].counter != 0) {
            updateMinValueForAtomicType(event_info.lock_info[i].min_duration,
                &lock_info->min_duration);
            updateMaxValueForAtomicType(event_info.lock_info[i].max_duration, &lock_info->max_duration);
            lock_info->counter += event_info.lock_info[i].counter;
            lock_info->failed_counter += event_info.lock_info[i].failed_counter;
            lock_info->total_duration += event_info.lock_info[i].total_duration;
            lock_info->avg_duration = lock_info->total_duration / lock_info->counter;
        }
    }

    /* update LWLock Event wait info */
    for (int i = 0; i < LWLOCK_EVENT_NUM; i++) {
        WaitStatisticsInfo *lwlock_info = &gsInstrWaitInfo->event_info.lwlock_info[i];

        update_max_last_updated(lwlock_info, event_info.lwlock_info[i].last_updated);
        if (event_info.lwlock_info[i].counter != 0) {
            updateMinValueForAtomicType(event_info.lwlock_info[i].min_duration,
                &lwlock_info->min_duration);
            updateMaxValueForAtomicType(event_info.lwlock_info[i].max_duration, &lwlock_info->max_duration);
            lwlock_info->counter += event_info.lwlock_info[i].counter;
            lwlock_info->failed_counter += event_info.lwlock_info[i].failed_counter;
            lwlock_info->total_duration += event_info.lwlock_info[i].total_duration;
            lwlock_info->avg_duration = lwlock_info->total_duration / lwlock_info->counter;
        }
    }
}

static void create_tuple_entry(TupleDesc tupdesc)
{
    int i = 0;
    TupleDescInitEntry(tupdesc, (AttrNumber)++i, "nodename", TEXTOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)++i, "type", TEXTOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)++i, "event", TEXTOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)++i, "wait", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)++i, "failed_wait", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)++i, "total_wait_time", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)++i, "avg_wait_time", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)++i, "max_wait_time", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)++i, "min_wait_time", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)++i, "last_updated", TIMESTAMPTZOID, -1, 0);
}

static void set_status_tuple_value(WaitInfo* gsInstrWaitInfo, Datum* values, int i, uint32 eventId)
{
    values[++i] = CStringGetTextDatum("STATUS");
    values[++i] = CStringGetTextDatum(pgstat_get_waitstatusname(eventId));
    values[++i] = Int64GetDatum(gsInstrWaitInfo->status_info.statistics_info[eventId].counter);
    values[++i] = Int64GetDatum(gsInstrWaitInfo->status_info.statistics_info[eventId].failed_counter);
    values[++i] = Int64GetDatum(gsInstrWaitInfo->status_info.statistics_info[eventId].total_duration);
    values[++i] = Int64GetDatum(gsInstrWaitInfo->status_info.statistics_info[eventId].avg_duration);
    values[++i] = Int64GetDatum(gsInstrWaitInfo->status_info.statistics_info[eventId].max_duration);
    values[++i] = Int64GetDatum(gsInstrWaitInfo->status_info.statistics_info[eventId].min_duration);
    values[++i] = TimestampTzGetDatum(gsInstrWaitInfo->status_info.statistics_info[eventId].last_updated);
}

static void set_io_event_tuple_value(WaitInfo* gsInstrWaitInfo, Datum* values, int i, uint32 eventId)
{
    values[++i] = CStringGetTextDatum("IO_EVENT");
    values[++i] = CStringGetTextDatum(pgstat_get_wait_io(WaitEventIO(eventId + PG_WAIT_IO)));
    values[++i] = Int64GetDatum(gsInstrWaitInfo->event_info.io_info[eventId].counter);
    values[++i] = Int64GetDatum(gsInstrWaitInfo->event_info.io_info[eventId].failed_counter);
    values[++i] = Int64GetDatum(gsInstrWaitInfo->event_info.io_info[eventId].total_duration);
    values[++i] = Int64GetDatum(gsInstrWaitInfo->event_info.io_info[eventId].avg_duration);
    values[++i] = Int64GetDatum(gsInstrWaitInfo->event_info.io_info[eventId].max_duration);
    values[++i] = Int64GetDatum(gsInstrWaitInfo->event_info.io_info[eventId].min_duration);
    values[++i] = TimestampTzGetDatum(gsInstrWaitInfo->event_info.io_info[eventId].last_updated);
}

static bool set_dms_event_tuple_value(WaitInfo* gsInstrWaitInfo, Datum* values, int i, uint32 eventId)
{
    if (!ENABLE_DMS) {
        return false;
    }
    values[++i] = CStringGetTextDatum("DMS_EVENT");
    values[++i] = CStringGetTextDatum(pgstat_get_wait_dms(WaitEventDMS(eventId + PG_WAIT_DMS)));

    if (!g_instance.dms_cxt.dmsInited) {
        ereport(WARNING, (errmsg("[SS] dms not init!")));
        return false;
    }
    unsigned long long cnt = 0;
    unsigned long long time = 0;
    dms_get_event(dms_wait_event_t(eventId), &cnt, &time);

    values[++i] = Int64GetDatum(cnt);
    values[++i] = Int64GetDatum(INT64_MIN);
    values[++i] = Int64GetDatum(time);
    values[++i] = Int64GetDatum(cnt == 0 ? 0 : time / cnt);
    values[++i] = Int64GetDatum(INT64_MIN);
    values[++i] = Int64GetDatum(INT64_MIN);
    values[++i] = TimestampTzGetDatum(gsInstrWaitInfo->event_info.dms_info[eventId].last_updated);
    return true;
}

static bool set_dms_cmd_tuple_value(WaitInfo *gsInstrWaitInfo, Datum *values, int i, uint32 eventId)
{
    if (!ENABLE_DMS) {
        return false;
    }
    values[++i] = CStringGetTextDatum("DMS_CMD");
    if (!g_instance.dms_cxt.dmsInited) {
        ereport(WARNING, (errmsg("[SS] dms not init!")));
        return false;
    }
    wait_cmd_stat_result_t cmd_stat_result;
    dms_get_cmd_stat(eventId, &cmd_stat_result);
    if (!cmd_stat_result.is_valid) {
        return false;
    }
    values[++i] = CStringGetTextDatum(cmd_stat_result.name);
    values[++i] = Int64GetDatum(cmd_stat_result.wait_count);
    values[++i] = Int64GetDatum(INT64_MIN);
    values[++i] = Int64GetDatum(cmd_stat_result.wait_time);
    values[++i] =
        Int64GetDatum(cmd_stat_result.wait_count == 0 ? 0 : cmd_stat_result.wait_time / cmd_stat_result.wait_count);
    values[++i] = Int64GetDatum(INT64_MIN);
    values[++i] = Int64GetDatum(INT64_MIN);
    values[++i] = TimestampTzGetDatum(gsInstrWaitInfo->event_info.dms_info[eventId].last_updated);
    return true;
}

static void set_lock_event_tuple_value(WaitInfo* gsInstrWaitInfo, Datum* values, int i, uint32 eventId)
{
    values[++i] = CStringGetTextDatum("LOCK_EVENT");
    values[++i] = CStringGetTextDatum(GetLockNameFromTagType(eventId));
    values[++i] = Int64GetDatum(gsInstrWaitInfo->event_info.lock_info[eventId].counter);
    values[++i] = Int64GetDatum(gsInstrWaitInfo->event_info.lock_info[eventId].failed_counter);
    values[++i] = Int64GetDatum(gsInstrWaitInfo->event_info.lock_info[eventId].total_duration);
    values[++i] = Int64GetDatum(gsInstrWaitInfo->event_info.lock_info[eventId].avg_duration);
    values[++i] = Int64GetDatum(gsInstrWaitInfo->event_info.lock_info[eventId].max_duration);
    values[++i] = Int64GetDatum(gsInstrWaitInfo->event_info.lock_info[eventId].min_duration);
    values[++i] = TimestampTzGetDatum(gsInstrWaitInfo->event_info.lock_info[eventId].last_updated);
}

static void set_lwlock_event_tuple_value(WaitInfo* gsInstrWaitInfo, Datum* values, int i, uint32 eventId, bool* nulls)
{
    values[++i] = CStringGetTextDatum("LWLOCK_EVENT");
    if (eventId < LWLOCK_EVENT_NUM) {
        values[++i] = CStringGetTextDatum(GetLWLockIdentifier(PG_WAIT_LWLOCK, eventId));
    } else {
        values[++i] = CStringGetTextDatum("unknown_lwlock_event");
        return;
    }

    values[++i] = Int64GetDatum(gsInstrWaitInfo->event_info.lwlock_info[eventId].counter);
    values[++i] = Int64GetDatum(gsInstrWaitInfo->event_info.lwlock_info[eventId].failed_counter);
    values[++i] = Int64GetDatum(gsInstrWaitInfo->event_info.lwlock_info[eventId].total_duration);
    values[++i] = Int64GetDatum(gsInstrWaitInfo->event_info.lwlock_info[eventId].avg_duration);
    values[++i] = Int64GetDatum(gsInstrWaitInfo->event_info.lwlock_info[eventId].max_duration);
    values[++i] = Int64GetDatum(gsInstrWaitInfo->event_info.lwlock_info[eventId].min_duration);
    values[++i] = TimestampTzGetDatum(gsInstrWaitInfo->event_info.lwlock_info[eventId].last_updated);
}

static bool set_tuple_value(
    WaitInfo* gsInstrWaitInfo, Datum* values, bool* nulls, int i, uint32 eventId, uint32 call_cn)
{
    values[++i] = CStringGetTextDatum(g_instance.attr.attr_common.PGXCNodeName);
    if (call_cn < STATE_WAIT_NUM) {
        eventId = call_cn;
        set_status_tuple_value(gsInstrWaitInfo, values, i, eventId);
    } else if (call_cn < IO_EVENT_NUM + STATE_WAIT_NUM) {
        eventId = call_cn - STATE_WAIT_NUM;
        set_io_event_tuple_value(gsInstrWaitInfo, values, i, eventId);
    } else if (call_cn < LOCK_EVENT_NUM + IO_EVENT_NUM + STATE_WAIT_NUM) {
        eventId = call_cn - STATE_WAIT_NUM - IO_EVENT_NUM;
        set_lock_event_tuple_value(gsInstrWaitInfo, values, i, eventId);
    } else if (call_cn < LOCK_EVENT_NUM + IO_EVENT_NUM + STATE_WAIT_NUM + LWLOCK_EVENT_NUM) {
        eventId = call_cn - LOCK_EVENT_NUM - IO_EVENT_NUM - STATE_WAIT_NUM;
        set_lwlock_event_tuple_value(gsInstrWaitInfo, values, i, eventId, nulls);
    } else if (call_cn < LOCK_EVENT_NUM + IO_EVENT_NUM + STATE_WAIT_NUM + LWLOCK_EVENT_NUM + DMS_EVENT_NUM) {
        eventId = call_cn - LOCK_EVENT_NUM - IO_EVENT_NUM - STATE_WAIT_NUM - LWLOCK_EVENT_NUM;
        return set_dms_event_tuple_value(gsInstrWaitInfo, values, i, eventId);
    } else {
        eventId = call_cn - LOCK_EVENT_NUM - IO_EVENT_NUM - STATE_WAIT_NUM - LWLOCK_EVENT_NUM - DMS_EVENT_NUM;
        return set_dms_cmd_tuple_value(gsInstrWaitInfo, values, i, eventId);
    }
    return true;
}

Datum get_instr_wait_event(PG_FUNCTION_ARGS)
{
    const int INSTR_WAITEVENT_ATTRUM = 10;
    FuncCallContext* funcctx = NULL;

    if (SRF_IS_FIRSTCALL()) {
        MemoryContext oldcontext = NULL;
        TupleDesc tupdesc = NULL;

        funcctx = SRF_FIRSTCALL_INIT();

        oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

        tupdesc = CreateTemplateTupleDesc(INSTR_WAITEVENT_ATTRUM, false);

        create_tuple_entry(tupdesc);

        funcctx->tuple_desc = BlessTupleDesc(tupdesc);

        if (!u_sess->attr.attr_common.enable_instr_track_wait) {
            ereport(WARNING, (errcode(ERRCODE_WARNING), (errmsg("GUC parameter 'enable_instr_track_wait' is off"))));
            MemoryContextSwitchTo(oldcontext);
            SRF_RETURN_DONE(funcctx);
        }

        funcctx->user_fctx = read_current_instr_wait_info();

        MemoryContextSwitchTo(oldcontext);

        if (funcctx->user_fctx == NULL)
            SRF_RETURN_DONE(funcctx);
    }

    funcctx = SRF_PERCALL_SETUP();
    if (funcctx->user_fctx != NULL) {
        Datum values[INSTR_WAITEVENT_ATTRUM];
        bool nulls[INSTR_WAITEVENT_ATTRUM] = {false};
        HeapTuple tuple = NULL;
        Datum result;
        int i = -1;
        uint32 eventId = 0;
        WaitInfo* gsInstrWaitInfo = (WaitInfo*)funcctx->user_fctx;
        errno_t rc = memset_s(values, sizeof(values), 0, sizeof(values));
        securec_check(rc, "\0", "\0");

        rc = memset_s(nulls, sizeof(nulls), 0, sizeof(nulls));
        securec_check(rc, "\0", "\0");

        if (set_tuple_value(gsInstrWaitInfo, values, nulls, i, eventId, funcctx->call_cntr)) {
            tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
            result = HeapTupleGetDatum(tuple);
            SRF_RETURN_NEXT(funcctx, result);
        }
    }

    pfree_ext(funcctx->user_fctx);
    funcctx->user_fctx = NULL;

    SRF_RETURN_DONE(funcctx);
}
