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
 * ---------------------------------------------------------------------------------------
 * 
 * snapshot.h
 *        definitions for WDR snapshot.
 *
 *
 *   By default, the snapshot thread collects database statistics
 *   from the views in dbe_perf schema in one hour,
 *   and insert these data into the corresponding tables in the snapshot schema.
 *   The WDR report generates a performance diagnosis report
 *   based on the data collected by the snapshot thread.
 * 
 * IDENTIFICATION
 *        src/include/instruments/snapshot.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef INSTR_SNAPSHOT_H
#define INSTR_SNAPSHOT_H
#include "gs_thread.h"

extern ThreadId snapshot_start(void);
extern void SnapshotMain();
extern Datum create_wdr_snapshot(PG_FUNCTION_ARGS);

extern void JobSnapshotIAm(void);
extern bool IsJobSnapshotProcess(void);
extern THR_LOCAL bool am_job_snapshot;
extern THR_LOCAL bool operate_snapshot_tables;

extern Datum GetDatumValue(const char* query, uint32 row, uint32 col, bool* isnull = NULL);

extern void instrSnapshotClean(void);

extern void instrSnapshotCancel(void);

#endif
