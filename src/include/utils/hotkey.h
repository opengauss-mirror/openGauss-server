/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
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
 * hotkey.h
 *
 *
 *
 * IDENTIFICATION
 *        src/include/utils/hotkey.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef SRC_INCLUDE_UTILS_HOTKEYS_H
#define SRC_INCLUDE_UTILS_HOTKEYS_H

#include "postgres.h"
#include "pgstat.h"
#include "pgxc/locator.h"
#include "nodes/nodes.h"
#include "nodes/pg_list.h"
#include "lib/lrucache.h"

const int LRU_QUEUE_LENGTH = 16;
const int HOTKEYS_QUEUE_LENGTH = 500;
const int FIFO_QUEUE_LENGTH = 32;

/* basic hotkey info to be present */
typedef struct HotkeyValue {
    char* databaseName;
    char* schemaName;
    char* relName;
    List* constValues; /* List of const nodes */
    Size totalLength;  /* Total length of values */
} HotkeyValue;

/* missing hotkey info for DFX */
typedef struct HotkeyMissInfo {
    char* databaseName;
    char* schemaName;
    char* relName;
    char* missInfo;
} HotkeyMissInfo;

/* Hotkey candidates collected from plan */
typedef struct HotkeyCandidate {
    Oid relid;
    uint32 hashValue;
    HotkeyValue* hotkeyValue;
} HotkeyCandidate;

/* HotkeyInfo structure */
typedef struct HotkeyInfo {
    uint64 count;     /* counter of key */
    Oid relationOid;  /* OID of relation the queries depend on */
    Oid databaseOid;  /* database oid */
    uint32 hashValue; /* hash value of key */
    HotkeyValue* hotkeyValue;
} HotkeyInfo;

/* Hotkey statistics info present to caller */
typedef struct HotkeyGeneralInfo {
    char* database;
    char* schema;
    char* table;
    char* value;
    uint32 hashValue;
    uint64 count;
} HotkeyGeneralInfo;

extern void CleanHotkeyCandidates(bool deep);
extern bool CmdtypeSupportsHotkey(CmdType commandType);
extern bool ExecOnSingleNode(List* nodeList);
extern void InsertKeysToLRU(HotkeyInfo** temp, int length);
extern void ReportHotkeyCandidate(RelationLocInfo* rel_loc_info, RelationAccessType accessType, List* idx_dist_by_col,
    const bool* nulls, Oid* attr, Datum* values, uint32 hashValue);
extern void SendHotkeyToPgstat();
extern Datum gs_stat_get_hotkeys_info(PG_FUNCTION_ARGS);
extern Datum gs_stat_clean_hotkeys(PG_FUNCTION_ARGS);
extern Datum global_stat_get_hotkeys_info(PG_FUNCTION_ARGS);
extern Datum global_stat_clean_hotkeys(PG_FUNCTION_ARGS);
extern void pgstat_recv_cleanup_hotkeys(PgStat_MsgCleanupHotkeys* msg, int len);
extern void pgstat_send_cleanup_hotkeys(Oid db_oid, Oid t_oid);
extern void PgstatUpdateHotkeys();
extern bool IsEqual(CmpFuncType fp, void* t1, void* t2);
extern bool IsHotkeyEqual(void* t1, void* t2);
extern void CleanHotkeyInfo(HotkeyInfo* keyInfo);
extern HotkeyInfo* MakeHotkeyInfo(const Oid relationOid, uint32 hashvalue, HotkeyValue* hotkeyValue, MemoryContext cxt);
extern List* CopyHotKeys(List* srcHotKeys);
extern void CheckHotkeys(RemoteQueryState* planstate, ExecNodes* exec_nodes, RemoteQueryExecType exec_type);

#endif /* SRC_INCLUDE_UTILS_HOTKEYS_H */