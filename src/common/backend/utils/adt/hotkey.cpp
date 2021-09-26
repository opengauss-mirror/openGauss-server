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
 * hotkey.cpp
 *      Functions that collect or clean hotkey.
 *
 *
 * IDENTIFICATION
 *      src/common/backend/utils/adt/hotkey.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "postgres.h"
#include "funcapi.h"
#include "pgstat.h"
#include "access/transam.h"
#include "access/tableam.h"
#include "access/tuptoaster.h"
#include "commands/dbcommands.h"
#include "commands/vacuum.h"
#include "knl/knl_session.h"
#include "lib/lrucache.h"
#include "nodes/makefuncs.h"
#include "nodes/nodes.h"
#include "nodes/pg_list.h"
#include "pgxc/locator.h"
#include "utils/builtins.h"
#include "utils/cash.h"
#include "utils/guc.h"
#include "utils/hotkey.h"
#include "utils/int8.h"
#include "utils/lsyscache.h"

/* We only collect keys than smaller than 1MB. */
const Size MAX_KEY_VALUE_LENGTH = 1048576;
const char* HOTKEY_VALUE_TOO_LONG = "hotkey value too long";

typedef struct HotkeyInfoParallel {
    ParallelFunctionState* state;
    TupleTableSlot* slot;
} HotkeyInfoParallel;
/* fetch hotkey statistics info from remote CN */
static HotkeyInfoParallel* GetGlobalHotkeysInfo(TupleDesc tupleDesc, const char* queryString);
static bool HotKeyValueIsEqual(HotkeyValue* hotkeyValue1, HotkeyValue* hotkeyValue2);
/* copy key from lockfreee queue to a temp list */
static void CopyLockFreeQueue();
static void* GetHotkeysInfo(uint32* num);
static bool GetTypByVal(Oid type);
static int GetTypLen(Oid type);
static char* GetDatabaseName();
static char* GetSchemaName(Oid relid);
static char* GetRelName(Oid relid);
static Size GetDatumLength(Oid type, Datum datum);
static HotkeyValue* CopyHotkeyValue(HotkeyValue* srcHotkeyValue);
/* collect valid keys with lockfree circular queue */
static void AddHotkeysToCollectList(Oid relid, uint32 hashValue, HotkeyValue* hotkeyValue);
char* ConstructHotkeyValue(HotkeyValue* hotkeyValue);

static Const* MakeConstFromDatum(Datum datum, Oid typeOid, bool isNull, int typLen)
{
    Datum copyValue;
    bool typByVal = GetTypByVal(typeOid);
    if (isNull) {
        copyValue = PointerGetDatum(NULL);
    } else {
        copyValue = datumCopy(datum, typByVal, GetTypLen(typeOid));
    }
    return makeConst(typeOid, -1, InvalidOid, typLen, copyValue, isNull, typByVal);
}

static HotkeyValue* MakeHotkeyValue(Oid relid, List* idx_dist_by_col, const bool* nulls, Oid* attr, Datum* values)
{
    ListCell* cell = NULL;
    int colNum = 0;
    Size valueLength = 0;
    HotkeyValue* hotkeyValue = (HotkeyValue*)palloc0(sizeof(HotkeyValue));
    hotkeyValue->databaseName = GetDatabaseName();
    hotkeyValue->schemaName = GetSchemaName(relid);
    hotkeyValue->relName = GetRelName(relid);

    foreach (cell, idx_dist_by_col) {
        colNum = lfirst_int(cell);
        valueLength = nulls[colNum] ? 0 : GetDatumLength(attr[colNum], values[colNum]);
        hotkeyValue->totalLength += valueLength;
        if (hotkeyValue->totalLength > MAX_KEY_VALUE_LENGTH)
            break;

        /* Copy value */
        Const* constValue = MakeConstFromDatum(values[colNum], attr[colNum], nulls[colNum], GetTypLen(attr[colNum]));
        hotkeyValue->constValues = lappend(hotkeyValue->constValues, constValue);
    }
    return hotkeyValue;
}

static void CleanHotkeyValue(HotkeyValue* hotkeyValue)
{
    ListCell* lcValue;
    foreach (lcValue, hotkeyValue->constValues) {
        Const* cnst = (Const*)lfirst(lcValue);
        if (!cnst->constisnull && !cnst->constbyval) {
            pfree(DatumGetPointer(cnst->constvalue));
        }
        pfree_ext(cnst);
    }
    list_free(hotkeyValue->constValues);
    pfree_ext(hotkeyValue->databaseName);
    pfree_ext(hotkeyValue->schemaName);
    pfree_ext(hotkeyValue->relName);
    pfree_ext(hotkeyValue);
}

static HotkeyCandidate* MakeHotkeyCandidate(Oid relid, uint32 hashValue)
{
    HotkeyCandidate* hotkeyCandidate = (HotkeyCandidate*)palloc0(sizeof(HotkeyCandidate));
    hotkeyCandidate->relid = relid;
    hotkeyCandidate->hashValue = hashValue;
    hotkeyCandidate->hotkeyValue = NULL;
    return hotkeyCandidate;
}

static void CleanHotkeyCandidate(HotkeyCandidate* hotkeyCandidate, bool deep)
{
    if (hotkeyCandidate == NULL)
        return;

    if (hotkeyCandidate->hotkeyValue != NULL && deep) {
        CleanHotkeyValue(hotkeyCandidate->hotkeyValue);
    }

    pfree_ext(hotkeyCandidate);
}

void CleanHotkeyInfo(HotkeyInfo* keyInfo)
{
    if (keyInfo == NULL) {
        return;
    }
    if (keyInfo->hotkeyValue != NULL) {
        CleanHotkeyValue(keyInfo->hotkeyValue);
    }

    pfree_ext(keyInfo);
}

bool CmdtypeSupportsHotkey(CmdType commandType)
{
    return (commandType == CMD_SELECT || commandType == CMD_UPDATE);
}

void CleanHotkeyCandidates(bool deep)
{
    if (u_sess->stat_cxt.hotkeyCandidates != NIL) {
        ListCell* lc = NULL;
        foreach (lc, u_sess->stat_cxt.hotkeyCandidates) {
            CleanHotkeyCandidate((HotkeyCandidate*)lfirst(lc), deep);
        }
        list_free(u_sess->stat_cxt.hotkeyCandidates);
        u_sess->stat_cxt.hotkeyCandidates = NIL;
    }
}

static void AddHotkeysToCollectList(Oid relid, uint32 hashValue, HotkeyValue* hotkeyValue)
{
    CircularQueue* collectList = g_instance.stat_cxt.hotkeysCollectList;

    /* update the lock-free circular queue */
    if (collectList->IsFull()) {
        return;
    }
    HotkeyInfo* newKey = MakeHotkeyInfo(relid, hashValue, hotkeyValue, collectList->GetContext());
    if (!collectList->LockFreeEnQueue(newKey)) {
        CleanHotkeyInfo(newKey);
    }
    return;
}

static void DebugPrintHotkeyValue(StringInfo str, HotkeyValue* hotkeyValue)
{
    appendStringInfo(str, "\tDatabaseName:%s,\n", hotkeyValue->databaseName);
    appendStringInfo(str, "\tSchemaName:%s,\n", hotkeyValue->schemaName);
    appendStringInfo(str, "\tTableName:%s,\n", hotkeyValue->relName);
    appendStringInfo(str, "\tdetail:[\n");
    ListCell* lc;
    foreach (lc, hotkeyValue->constValues) {
        Const* cnst = (Const*)lfirst(lc);
        if (cnst->constisnull) {
            appendStringInfo(str, "\t\ttypeid:null,\n");
        } else {
            /* For security reason, we do not print value. */
            appendStringInfo(str, "\t\ttypeid:%u,\n", cnst->consttype);
        }
    }
    appendStringInfo(str, "\t]\n");
    appendStringInfo(str, "\tTotal length:%lu\n", hotkeyValue->totalLength);
}

void DebugPrintHotkeyInfo(HotkeyInfo* hotkeyInfo)
{
    if (log_min_messages > DEBUG2 || !module_logging_is_on(MOD_HOTKEY))
        return;

    HotkeyValue* hotkeyValue = hotkeyInfo->hotkeyValue;
    StringInfoData str;
    initStringInfo(&str);
    appendStringInfo(&str, "Hotkey record collected\n");
    appendStringInfo(&str, "{\n\trelid:%u,\n", hotkeyInfo->relationOid);
    appendStringInfo(&str, "\thashvalue:%u,\n", hotkeyInfo->hashValue);
    DebugPrintHotkeyValue(&str, hotkeyValue);
    appendStringInfo(&str, "}\n");

    ereport(LOG, (errmodule(MOD_HOTKEY), errmsg("%s", str.data)));
    pfree_ext(str.data);
}

void DebugPrintHotkey(HotkeyCandidate* hotkeyCandidate)
{
    if (log_min_messages > DEBUG2 || !module_logging_is_on(MOD_HOTKEY))
        return;

    HotkeyValue* hotkeyValue = hotkeyCandidate->hotkeyValue;
    StringInfoData str;
    initStringInfo(&str);
    appendStringInfo(&str, "Hotkey record collected\n");
    appendStringInfo(&str, "{\n\trelid:%u,\n", hotkeyCandidate->relid);
    appendStringInfo(&str, "\thashvalue:%u,\n", hotkeyCandidate->hashValue);
    DebugPrintHotkeyValue(&str, hotkeyValue);
    appendStringInfo(&str, "}\n");

    ereport(LOG, (errmodule(MOD_HOTKEY), errmsg("%s", str.data)));
    pfree_ext(str.data);
}

bool ExecOnSingleNode(List* nodeList)
{
    if (nodeList == NULL)
        return false;
    return (nodeList->length == 1);
}

bool IsEqual(CmpFuncType fp, void* t1, void* t2)
{
    return fp(t1, t2);
}

bool IsHotkeyEqual(void* t1, void* t2)
{
    if (t1 == NULL || t2 == NULL) {
        return false;
    }
    HotkeyInfo* key1 = (HotkeyInfo*)t1;
    HotkeyInfo* key2 = (HotkeyInfo*)t2;
    return (key1->hashValue == key2->hashValue) && (key1->relationOid == key2->relationOid) &&
           (key1->databaseOid == key2->databaseOid) && HotKeyValueIsEqual(key1->hotkeyValue, key2->hotkeyValue);
}

bool HotKeyValueIsEqual(HotkeyValue* hotkeyValue1, HotkeyValue* hotkeyValue2)
{
    if (list_length(hotkeyValue1->constValues) != list_length(hotkeyValue2->constValues))
        return false;

    bool isEqual = true;
    ListCell* lcValue1;
    ListCell* lcValue2 = list_head(hotkeyValue2->constValues);
    foreach (lcValue1, hotkeyValue1->constValues) {
        Const* cnst1 = (Const*)lfirst(lcValue1);
        Const* cnst2 = (Const*)lfirst(lcValue2);
        Oid typid = cnst1->consttype;
        Datum value1 = cnst1->constvalue;
        Datum value2 = cnst2->constvalue;
        if (cnst1->constisnull && cnst2->constisnull) {
            isEqual = (cnst1->consttype == cnst2->consttype);
        } else if (cnst1->constisnull || cnst2->constisnull) {
            /* Only one side is null, values can't be equal. */
            isEqual = false;
        } else {
            isEqual = datumIsEqual(value1, value2, GetTypByVal(typid), GetTypLen(typid));
        }

        if (!isEqual)
            return false;
        lcValue2 = lcValue2->next;
    }
    return isEqual;
}

void InsertKeysToLRU(HotkeyInfo** temp, int length)
{
    for (int i = 0; i < length; i++) {
        HotkeyInfo* keyInfo = temp[i];
        if (keyInfo == NULL) {
            continue;
        }
        /* judge if lru contains this hot key at first, if yes, update lfu, otherwise put it into fifo */
        if (g_instance.stat_cxt.lru->Contain(keyInfo, IsHotkeyEqual)) {
            continue;
        }
        /* judge whether this key appears more than 2 times in the fifo queue, if yes, it's a hot key */
        ListCell* cell;
        bool isHotkey = false;

        foreach (cell, g_instance.stat_cxt.fifo) {
            HotkeyInfo* t = (HotkeyInfo*)lfirst(cell);
            if (IsEqual(IsHotkeyEqual, keyInfo, t)) {
                isHotkey = true;
                break;
            }
        }

        if (isHotkey) {
            /* put it into lru list */
            g_instance.stat_cxt.lru->Put(keyInfo);
        } else {
            if (list_length(g_instance.stat_cxt.fifo) == FIFO_QUEUE_LENGTH) {
                HotkeyInfo* deleteKey = (HotkeyInfo*)lfirst(list_nth_cell(g_instance.stat_cxt.fifo, 0));
                CleanHotkeyInfo(deleteKey);
                g_instance.stat_cxt.fifo = list_delete_first(g_instance.stat_cxt.fifo);
            }
            MemoryContext old = MemoryContextSwitchTo(g_instance.stat_cxt.hotkeysCxt);
            g_instance.stat_cxt.fifo = lappend(g_instance.stat_cxt.fifo, keyInfo);
            (void)MemoryContextSwitchTo(old);
        }
    }
    return;
}

static char* GetDatabaseName()
{
    if (u_sess->proc_cxt.MyProcPort->database_name != NULL) {
        return pstrdup(u_sess->proc_cxt.MyProcPort->database_name);
    } else {
        Assert(u_sess->proc_cxt.MyProcPort->database_name != NULL);
        char* dbName = get_database_name(u_sess->proc_cxt.MyDatabaseId);
        if (dbName == NULL) {
            return pstrdup("unknown");
        } else {
            return dbName;
        }
    }
}

static char* GetSchemaName(Oid relid)
{
    char* namespaceName = get_namespace_name(get_rel_namespace(relid));
    if (namespaceName != NULL) {
        return namespaceName;
    } else {
        return pstrdup("unknown");
    }
}

static char* GetRelName(Oid relid)
{
    char* relName = get_rel_name(relid);
    if (relName != NULL) {
        return relName;
    } else {
        return pstrdup("unknown");
    }
}

static HotkeyValue* CopyHotkeyValue(HotkeyValue* srcHotkeyValue)
{
    HotkeyValue* dstHotkeyValue = (HotkeyValue*)palloc0(sizeof(HotkeyValue));
    dstHotkeyValue->databaseName = pstrdup(srcHotkeyValue->databaseName);
    dstHotkeyValue->schemaName = pstrdup(srcHotkeyValue->schemaName);
    dstHotkeyValue->relName = pstrdup(srcHotkeyValue->relName);
    dstHotkeyValue->totalLength = srcHotkeyValue->totalLength;

    ListCell* cell;
    foreach (cell, srcHotkeyValue->constValues) {
        Const* cnst = (Const*)copyObject((Const*)lfirst(cell));
        dstHotkeyValue->constValues = lappend(dstHotkeyValue->constValues, cnst);
    }
    return dstHotkeyValue;
}

HotkeyInfo* MakeHotkeyInfo(const Oid relationOid, uint32 hashValue, HotkeyValue* hotkeyValue, MemoryContext cxt)
{
    MemoryContext old = MemoryContextSwitchTo(cxt);
    HotkeyInfo* newKey = (HotkeyInfo*)palloc0(sizeof(HotkeyInfo));
    newKey->count = 1;
    newKey->relationOid = relationOid;
    newKey->hashValue = hashValue;
    newKey->databaseOid = u_sess->proc_cxt.MyDatabaseId;
    newKey->hotkeyValue = CopyHotkeyValue(hotkeyValue);

    (void)MemoryContextSwitchTo(old);
    return newKey;
}

static HotkeyInfo* CopyHotkeyInfo(HotkeyInfo* srcHotkey)
{
    HotkeyInfo* dstHotkey = (HotkeyInfo*)palloc0(sizeof(HotkeyInfo));
    dstHotkey->hotkeyValue = CopyHotkeyValue(srcHotkey->hotkeyValue);
    dstHotkey->hashValue = srcHotkey->hashValue;
    dstHotkey->databaseOid = srcHotkey->databaseOid;
    dstHotkey->relationOid = srcHotkey->relationOid;
    return dstHotkey;
}

List* CopyHotKeys(List* srcHotKeys)
{
    List* dstHotkeys = NIL;
    ListCell* lc;
    foreach (lc, srcHotKeys) {
        HotkeyInfo* srcHotkey = (HotkeyInfo*)lfirst(lc);
        HotkeyInfo* dstHotkey = CopyHotkeyInfo(srcHotkey);
        dstHotkeys = lappend(dstHotkeys, dstHotkey);
    }
    return dstHotkeys;
}

HotkeyMissInfo* MakeHotkeyMissInfo(HotkeyValue* hotkeyValue, MemoryContext cxt, const char* missInfo)
{
    MemoryContext old = MemoryContextSwitchTo(cxt);
    HotkeyMissInfo* hotkeyMissInfo = (HotkeyMissInfo*)palloc0(sizeof(HotkeyMissInfo));
    hotkeyMissInfo->missInfo = pstrdup(missInfo);
    hotkeyMissInfo->databaseName = pstrdup(hotkeyValue->databaseName);
    hotkeyMissInfo->schemaName = pstrdup(hotkeyValue->schemaName);
    hotkeyMissInfo->relName = pstrdup(hotkeyValue->relName);
    (void)MemoryContextSwitchTo(old);
    return hotkeyMissInfo;
}

#define CASE_VARLENA    \
    case INT2VECTOROID: \
    case CLOBOID:       \
    case NVARCHAR2OID:  \
    case VARCHAROID:    \
    case TEXTOID:       \
    case OIDVECTOROID:  \
    case BPCHAROID:     \
    case RAWOID:        \
    case BYTEAOID:      \
    case NUMERICOID

static Size GetDatumLength(Oid type, Datum datum)
{
    int typLen = GetTypLen(type);
    if (typLen > 0)
        return (Size)typLen;

    /* only varlen will return -1 */
    switch (type) {
        /* varlena */
        CASE_VARLENA:
            return (Size)VARSIZE_ANY_EXHDR(datum);

        case BYTEAWITHOUTORDERWITHEQUALCOLOID:
        case BYTEAWITHOUTORDERCOLOID:
        default:
            /* do not collect hot key when meet encrypted type or other type. */
            return MAX_KEY_VALUE_LENGTH + 1;
    }
    return 0;
}

static bool GetTypByVal(Oid type)
{
    switch (type) {
        /* fixed data length */
        case INT8OID:
            return FLOAT8PASSBYVAL;
        case INT1OID:
        case INT2OID:
        case OIDOID:
        case INT4OID:
        case BOOLOID:
        case CHAROID:
            return true;
        case NAMEOID:
            return false;
        case FLOAT4OID:
            return FLOAT4PASSBYVAL;
        case FLOAT8OID:
            return FLOAT8PASSBYVAL;
        case ABSTIMEOID:
        case RELTIMEOID:
        case DATEOID:
            return true;
        case CASHOID:
        case TIMEOID:
        case TIMESTAMPOID:
        case TIMESTAMPTZOID:
        case SMALLDATETIMEOID:
            return FLOAT8PASSBYVAL;
        case UUIDOID:
        case INTERVALOID:
        case TIMETZOID:
            return false;

        /* varlena */
        CASE_VARLENA:
        case BYTEAWITHOUTORDERWITHEQUALCOLOID:
        case BYTEAWITHOUTORDERCOLOID:
            return false;

        default:
            ereport(
                ERROR, (errcode(ERRCODE_WRONG_OBJECT_TYPE), errmsg("Unsupported datatype for hotkey collction.\n")));
    }
    return false;
}

static int GetTypLen(Oid type)
{
    const int oneByte = 1;
    const int twoByte = 1;
    const int fourBytes = 4;
    const int eightBytes = 8;
    const int sixteenBytes = 16;
    switch (type) {
        /* fixed data length */
        case INT1OID:
            return oneByte;
        case INT2OID:
            return twoByte;
        case OIDOID:
            return fourBytes;
        case BOOLOID:
            return oneByte;
        case CHAROID:
            return oneByte;
        case NAMEOID:
            return NAMEDATALEN;
        case FLOAT4OID:
            return fourBytes;
        case FLOAT8OID:
            return eightBytes;
        case INT4OID:
        case ABSTIMEOID:
        case RELTIMEOID:
        case DATEOID:
            return fourBytes;
        case INT8OID:
        case CASHOID:
        case TIMEOID:
        case TIMESTAMPOID:
        case TIMESTAMPTZOID:
        case SMALLDATETIMEOID:
            return eightBytes;
        case UUIDOID:
        case INTERVALOID:
            return sixteenBytes;
        case TIMETZOID:
            return fourBytes + eightBytes;

        /* varlena */
        CASE_VARLENA:
        case BYTEAWITHOUTORDERWITHEQUALCOLOID:
        case BYTEAWITHOUTORDERCOLOID:
        default:
            return -1;
    }
    return 0;
}

static void RecordAbandonHotkey(HotkeyCandidate* hotkeyCandidate, const char* missInfo)
{
    ereport(DEBUG2, (errmodule(MOD_HOTKEY), errmsg("Hot key abandoned because %s", missInfo)));
    DebugPrintHotkey(hotkeyCandidate);
    return;
}

static inline bool IsLocatorDistributed(char locatorType)
{
    return (locatorType == LOCATOR_TYPE_HASH || IsLocatorDistributedBySlice(locatorType));
}

void ReportHotkeyCandidate(RelationLocInfo* rel_loc_info, RelationAccessType accessType, List* idx_dist_by_col,
    const bool* nulls, Oid* attr, Datum* values, uint32 hashValue)
{
    if (!u_sess->attr.attr_resource.enable_hotkeys_collection)
        return;

    if (g_instance.role != VCOORDINATOR || u_sess->attr.attr_sql.under_explain) {
        /* no need to collect hotkey if is explaining query or on datanode */
        return;
    }

    if (rel_loc_info->relid < FirstNormalObjectId || !IsLocatorDistributed(rel_loc_info->locatorType)) {
        /* Do not collect on system table and only support hash/list/range distributed table. */
        return;
    }

    if (accessType == RELATION_ACCESS_INSERT) {
        /* Only support select or update. */
        return;
    }

    if (idx_dist_by_col != NULL) {
        MemoryContext oldcontext = MemoryContextSwitchTo(u_sess->stat_cxt.hotkeySessContext);
        HotkeyCandidate* hotkeyCandidate = MakeHotkeyCandidate(rel_loc_info->relid, hashValue);
        hotkeyCandidate->hotkeyValue = MakeHotkeyValue(rel_loc_info->relid, idx_dist_by_col, nulls, attr, values);

        if (hotkeyCandidate->hotkeyValue->totalLength > MAX_KEY_VALUE_LENGTH) {
            RecordAbandonHotkey(hotkeyCandidate, HOTKEY_VALUE_TOO_LONG);
            CleanHotkeyCandidate(hotkeyCandidate, true);
        } else {
            u_sess->stat_cxt.hotkeyCandidates = lappend(u_sess->stat_cxt.hotkeyCandidates, hotkeyCandidate);
        }
        (void)MemoryContextSwitchTo(oldcontext);
    }
}

void SendHotkeyToPgstat()
{
    if (u_sess->stat_cxt.hotkeyCandidates != NULL) {
        ListCell* lc = NULL;
        foreach (lc, u_sess->stat_cxt.hotkeyCandidates) {
            HotkeyCandidate* hotkeyCandidate = (HotkeyCandidate*)lfirst(lc);
            DebugPrintHotkey(hotkeyCandidate);
            AddHotkeysToCollectList(hotkeyCandidate->relid, hotkeyCandidate->hashValue, hotkeyCandidate->hotkeyValue);
        }
        CleanHotkeyCandidates(true);
    }
}

const int HOTKEYS_INFO_ATTRNUM = 6;
static void ConstructHotkeyInfo(
    HotkeyGeneralInfo* stat, Datum values[HOTKEYS_INFO_ATTRNUM], bool nulls[HOTKEYS_INFO_ATTRNUM])
{
    int i = -1;
    if (stat->database != NULL) {
        values[++i] = CStringGetTextDatum(stat->database);
    } else {
        nulls[++i] = true;
    }
    if (stat->schema != NULL) {
        values[++i] = CStringGetTextDatum(stat->schema);
    } else {
        nulls[++i] = true;
    }
    if (stat->table != NULL) {
        values[++i] = CStringGetTextDatum(stat->table);
    } else {
        nulls[++i] = true;
    }
    if (stat->value != NULL) {
        values[++i] = CStringGetTextDatum(stat->value);
    } else {
        nulls[++i] = true;
    }
    values[++i] = Int64GetDatum(stat->hashValue);
    values[++i] = Int64GetDatum(stat->count);
}

static char* ConstGetCstring(Const* cnst)
{
    Datum d = cnst->constvalue;
    switch (cnst->consttype) {
        /* fixed data length */
        case INT8OID:
            return DatumGetCString(DirectFunctionCall1(int8out, d));
        case INT1OID:
            return DatumGetCString(DirectFunctionCall1(int1out, d));
        case INT2OID:
            return DatumGetCString(DirectFunctionCall1(int2out, d));
        case OIDOID:
            return DatumGetCString(DirectFunctionCall1(oidout, d));
        case INT4OID:
            return DatumGetCString(DirectFunctionCall1(int4out, d));
        case BOOLOID:
            return DatumGetCString(DirectFunctionCall1(boolout, d));
        case CHAROID:
            return DatumGetCString(DirectFunctionCall1(charout, d));
        case NAMEOID:
            return DatumGetCString(DirectFunctionCall1(nameout, d));
        case FLOAT4OID:
            return DatumGetCString(DirectFunctionCall1(float4out, d));
        case FLOAT8OID:
            return DatumGetCString(DirectFunctionCall1(float8out, d));
        case ABSTIMEOID:
            return DatumGetCString(DirectFunctionCall1(abstimeout, d));
        case RELTIMEOID:
            return DatumGetCString(DirectFunctionCall1(reltimeout, d));
        case DATEOID:
            return DatumGetCString(DirectFunctionCall1(date_out, d));
        case CASHOID:
            return DatumGetCString(DirectFunctionCall1(cash_out, d));
        case TIMEOID:
            return DatumGetCString(DirectFunctionCall1(time_out, d));
        case TIMESTAMPOID:
            return DatumGetCString(DirectFunctionCall1(timestamp_out, d));
        case TIMESTAMPTZOID:
            return DatumGetCString(DirectFunctionCall1(timestamptz_out, d));
        case SMALLDATETIMEOID:
            return DatumGetCString(DirectFunctionCall1(smalldatetime_out, d));
        case UUIDOID:
            return DatumGetCString(DirectFunctionCall1(uuid_out, d));
        case INTERVALOID:
            return DatumGetCString(DirectFunctionCall1(interval_out, d));
        case TIMETZOID:
            return DatumGetCString(DirectFunctionCall1(timetz_out, d));
        case INT2VECTOROID:
            return DatumGetCString(DirectFunctionCall1(int2vectorout, d));
        case CLOBOID:
            return DatumGetCString(DirectFunctionCall1(textout, d));
        case NVARCHAR2OID:
            return DatumGetCString(DirectFunctionCall1(nvarchar2out, d));
        case VARCHAROID:
            return DatumGetCString(DirectFunctionCall1(varcharout, d));
        case TEXTOID:
            return DatumGetCString(DirectFunctionCall1(textout, d));
        case OIDVECTOROID:
            return DatumGetCString(DirectFunctionCall1(oidvectorout, d));
        case BPCHAROID:
            return DatumGetCString(DirectFunctionCall1(bpcharout, d));
        case RAWOID:
            return DatumGetCString(DirectFunctionCall1(rawout, d));
        case BYTEAOID:
            return DatumGetCString(DirectFunctionCall1(byteaout, d));
        case NUMERICOID:
            return DatumGetCString(DirectFunctionCall1(numeric_out, d));

        default:
            ereport(ERROR,
                (errcode(ERRCODE_WRONG_OBJECT_TYPE), errmsg("Unhandled datatype for modulo or hash distribution\n")));
    }
    return pstrdup("UNKNOWN");
}

char* ConstructHotkeyValue(HotkeyValue* hotkeyValue)
{
    StringInfoData str;
    initStringInfo(&str);
    List* values = hotkeyValue->constValues;
    ListCell* lc;
    bool first = true;
    appendStringInfo(&str, "{");
    foreach (lc, values) {
        Const* cnst = (Const*)lfirst(lc);
        char* cnstText;
        if (cnst->constisnull) {
            cnstText = pstrdup("null");
        } else {
            cnstText = ConstGetCstring(cnst);
        }

        if (!first) {
            appendStringInfo(&str, ",");
        }
        appendStringInfo(&str, "%s", cnstText);
        pfree_ext(cnstText);
        first = false;
    }
    appendStringInfo(&str, "}");
    return str.data;
}

/*
 * @Description: get hot-keys statistics information from current coordinator
 * @return - Datum
 */
Datum gs_stat_get_hotkeys_info(PG_FUNCTION_ARGS)
{
    FuncCallContext* funcctx = NULL;
    int i = 0;

    if (SRF_IS_FIRSTCALL()) {
        MemoryContext oldcontext;
        TupleDesc tupdesc;
        i = 0;

        funcctx = SRF_FIRSTCALL_INIT();

        oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

        tupdesc = CreateTemplateTupleDesc(HOTKEYS_INFO_ATTRNUM, false);

        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "database", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "schema", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "table", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "value", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "hash_value", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "count", INT8OID, -1, 0);

        funcctx->tuple_desc = BlessTupleDesc(tupdesc);

        funcctx->user_fctx = GetHotkeysInfo(&funcctx->max_calls);

        MemoryContextSwitchTo(oldcontext);

        if (funcctx->user_fctx == NULL) {
            SRF_RETURN_DONE(funcctx);
        }
    }

    funcctx = SRF_PERCALL_SETUP();
    if (funcctx->call_cntr < funcctx->max_calls) {
        Datum values[HOTKEYS_INFO_ATTRNUM];
        bool nulls[HOTKEYS_INFO_ATTRNUM] = {false};
        HeapTuple tuple = NULL;
        Datum result;

        errno_t rc = 0;
        rc = memset_s(values, sizeof(values), 0, sizeof(values));
        securec_check(rc, "\0", "\0");
        rc = memset_s(nulls, sizeof(nulls), 0, sizeof(nulls));
        securec_check(rc, "\0", "\0");

        HotkeyGeneralInfo* stat = (HotkeyGeneralInfo*)funcctx->user_fctx + funcctx->call_cntr;
        if (stat == NULL) {
            for (int i = 0; (unsigned int)(i) < sizeof(nulls) / sizeof(nulls[0]); i++) {
                nulls[i] = true;
            }
        } else {
            ConstructHotkeyInfo(stat, values, nulls);
        }

        tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
        result = HeapTupleGetDatum(tuple);
        SRF_RETURN_NEXT(funcctx, result);
    } else {
        SRF_RETURN_DONE(funcctx);
    }
}

const char* SELECT_HOTKEY_QUERY = "select * from gs_stat_get_hotkeys_info()";
/*
 * @Description: get hot-keys statistics information from all coordinators
 * @return - Datum
 */
Datum global_stat_get_hotkeys_info(PG_FUNCTION_ARGS)
{
    FuncCallContext* funcctx = NULL;
    Datum values[HOTKEYS_INFO_ATTRNUM];
    bool nulls[HOTKEYS_INFO_ATTRNUM];
    HeapTuple tuple;
    int i = 0;

    if (SRF_IS_FIRSTCALL()) {
        MemoryContext old;
        TupleDesc tupdesc;

        funcctx = SRF_FIRSTCALL_INIT();

        old = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

        tupdesc = CreateTemplateTupleDesc(HOTKEYS_INFO_ATTRNUM, false);

        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "database", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "schema", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "table", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "value", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "hash_value", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "count", INT8OID, -1, 0);

        funcctx->tuple_desc = BlessTupleDesc(tupdesc);
        /* fetch local tuple at first */
        funcctx->user_fctx = GetHotkeysInfo(&funcctx->max_calls);
        MemoryContextSwitchTo(old);

        if (funcctx->user_fctx == NULL) {
            MemoryContext old = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);
            funcctx->user_fctx = GetGlobalHotkeysInfo(funcctx->tuple_desc, SELECT_HOTKEY_QUERY);
            (void)MemoryContextSwitchTo(old);

            if (funcctx->user_fctx == NULL) {
                SRF_RETURN_DONE(funcctx);
            }
        }
    }

    /* stuff done on every call of the function */
    funcctx = SRF_PERCALL_SETUP();
    /* step1. fetch info from local */
    if (funcctx->call_cntr < funcctx->max_calls) {
        errno_t rc = 0;
        rc = memset_s(values, sizeof(values), 0, sizeof(values));
        securec_check(rc, "\0", "\0");
        rc = memset_s(nulls, sizeof(nulls), 0, sizeof(nulls));
        securec_check(rc, "\0", "\0");

        HotkeyGeneralInfo* stat = (HotkeyGeneralInfo*)funcctx->user_fctx + funcctx->call_cntr;

        if (stat == NULL) {
            for (int i = 0; (unsigned int)(i) < sizeof(nulls) / sizeof(nulls[0]); i++) {
                nulls[i] = true;
            }
        } else {
            ConstructHotkeyInfo(stat, values, nulls);
        }

        /* after finishing fetching from local, try to fetch from remote */
        if (funcctx->call_cntr == funcctx->max_calls - 1) {
            MemoryContext old = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);
            funcctx->user_fctx = GetGlobalHotkeysInfo(funcctx->tuple_desc, SELECT_HOTKEY_QUERY);
            (void)MemoryContextSwitchTo(old);

            if (funcctx->user_fctx == NULL) {
                SRF_RETURN_DONE(funcctx);
            }
        }

        tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
        SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tuple));
    } else if (funcctx->user_fctx != NULL) {
        /* step2. fetch from remote */
        Tuplestorestate* tupleStore = ((HotkeyInfoParallel*)funcctx->user_fctx)->state->tupstore;
        TupleTableSlot* slot = ((HotkeyInfoParallel*)funcctx->user_fctx)->slot;
        if (!tuplestore_gettupleslot(tupleStore, true, false, slot)) {
            FreeParallelFunctionState(((HotkeyInfoParallel*)funcctx->user_fctx)->state);
            ExecDropSingleTupleTableSlot(slot);
            pfree_ext(funcctx->user_fctx);
            SRF_RETURN_DONE(funcctx);
        }

        for (int i = 0; i < HOTKEYS_INFO_ATTRNUM; i++) {
            values[i] = tableam_tslot_getattr(slot, i + 1, &nulls[i]);
        }

        tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
        ExecClearTuple(slot);
        SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tuple));
    }
    SRF_RETURN_DONE(funcctx);
}

/*
 * @Description: parallel get hotkeys info in entire cluster througn ExecRemoteFunctionInParallel in
 * RemoteFunctionResultHandler.
 * @return: one tuple slot of result set.
 */
static HotkeyInfoParallel* GetGlobalHotkeysInfo(TupleDesc tupleDesc, const char* queryString)
{
    StringInfoData buf;
    HotkeyInfoParallel* streamInfo = NULL;
    ExecNodes* exec_nodes = NULL;
    exec_nodes = (ExecNodes*)makeNode(ExecNodes);
    exec_nodes->accesstype = RELATION_ACCESS_READ;
    exec_nodes->baselocatortype = LOCATOR_TYPE_HASH;
    exec_nodes->nodeList = GetAllCoordNodes();
    exec_nodes->primarynodelist = NIL;
    exec_nodes->en_expr = NULL;

    initStringInfo(&buf);
    streamInfo = (HotkeyInfoParallel*)palloc0(sizeof(HotkeyInfoParallel));
    appendStringInfoString(&buf, queryString);
    streamInfo->state = RemoteFunctionResultHandler(buf.data, exec_nodes, NULL, true, EXEC_ON_COORDS, true);
    streamInfo->slot = MakeSingleTupleTableSlot(tupleDesc);
    pfree_ext(buf.data);
    return streamInfo;
}

void PgstatUpdateHotkeys()
{
    if (g_instance.role != VCOORDINATOR || !u_sess->attr.attr_resource.enable_hotkeys_collection) {
        return;
    }

    DEBUG_MOD_START_TIMER(MOD_HOTKEY);

    /* copy keys from lockfree queue to a temp queue */
    CopyLockFreeQueue();

    DEBUG_MOD_STOP_TIMER(MOD_HOTKEY, "PgstatUpdateHotkeys");

    return;
}

void CopyLockFreeQueue()
{
    if (g_instance.stat_cxt.hotkeysCollectList == NULL) {
        ereport(ERROR, (errmsg("collection list has not been initialized!")));
    }

    const uint32 start = g_instance.stat_cxt.hotkeysCollectList->GetStart();
    const uint32 end = g_instance.stat_cxt.hotkeysCollectList->GetEnd();
    if (start == end) {
        return;
    }
    CircularQueue* list = g_instance.stat_cxt.hotkeysCollectList;
    uint32 length = list->GetLength(start, end);
    /* initialize temp queue for copying hotkeys */
    HotkeyInfo* temp[HOTKEYS_QUEUE_LENGTH];
    Assert(length < HOTKEYS_QUEUE_LENGTH);
    uint32 i = 0;
    for (i = 0; i < length; i++) {
        temp[i] = (HotkeyInfo*)list->GetElem(i);
        if (temp[i] == NULL) {
            break;
        }
    }
    length = i;
    /* set new head */
    g_instance.stat_cxt.hotkeysCollectList->SetStart(start + i);

    /* insert keys into lru list */
    InsertKeysToLRU(temp, length);
    return;
}

void pgstat_send_cleanup_hotkeys(Oid dbOid, Oid tOid)
{
    PgStat_MsgCleanupHotkeys msg;

    if (g_instance.stat_cxt.pgStatSock == PGINVALID_SOCKET)
        return;

    /* pgstat_setheader */
    msg.m_hdr.m_type = PGSTAT_MTYPE_CLEANUPHOTKEYS;
    msg.m_databaseOid = dbOid;
    msg.m_tableOid = tOid;
    pgstat_send(&msg, sizeof(msg));
}

void pgstat_recv_cleanup_hotkeys(PgStat_MsgCleanupHotkeys* msg, int len)
{
    Oid dbOid = msg->m_databaseOid;
    Oid tOid = msg->m_tableOid;

    if (dbOid == InvalidOid && tOid == InvalidOid) {
        // delete all statistics info in current coordinator
        g_instance.stat_cxt.lru->Clean();
        return;
    } else if (tOid == InvalidOid) {
        g_instance.stat_cxt.lru->DeleteHotkeysInDB(dbOid);
        return;
    } else {
        g_instance.stat_cxt.lru->DeleteHotkeysInTAB(tOid);
        return;
    }
}

static HotkeyGeneralInfo* FetchHotkeysFromLRU(LRUCache* lru, uint32* num)
{
    (void)pthread_rwlock_rdlock(lru->GetLock());

    if (lru->GetLength() == 0) {
        *num = 0;
        (void)pthread_rwlock_unlock(lru->GetLock());
        return NULL;
    }
    RefNode* arr[16];

    DllNode* ptr = lru->GetFirstElem();
    RefNode* temp = NULL;
    int i = 0;
    while (ptr != NULL) {
        // traverse the whole list because the node maybe just added to the tail
        if (ptr->node == NULL || ptr->node->key == NULL) {
            ptr = ptr->next;
            continue;
        }
        uint32 expected = 0;
        while (!pg_atomic_compare_exchange_u32(&ptr->state, &expected, 1)) {
            expected = 0;
        }
        temp = ptr->node;
        pg_atomic_add_fetch_u64(&temp->refcount, 1);
        arr[i] = temp;
        ((HotkeyInfo*)arr[i]->key)->count = ptr->value;
        expected = 1;
        pg_atomic_compare_exchange_u32(&(ptr->state), &expected, 0);
        ptr = ptr->next;
        i++;
    }
    *num = i;
    (void)pthread_rwlock_unlock(lru->GetLock());
    if (i == 0) {
        return NULL;
    }
    HotkeyGeneralInfo* result = (HotkeyGeneralInfo*)palloc0(sizeof(HotkeyGeneralInfo) * (*num));
    for (uint32 i = 0; i < *num; i++) {
        if (arr[i]->key != NULL) {
            result[i].hashValue = ((HotkeyInfo*)arr[i]->key)->hashValue;
            result[i].count = ((HotkeyInfo*)arr[i]->key)->count;
            HotkeyValue* hotkeyValue = ((HotkeyInfo*)arr[i]->key)->hotkeyValue;
            if (hotkeyValue != NULL) {
                result[i].database = pstrdup(hotkeyValue->databaseName);
                result[i].schema = pstrdup(hotkeyValue->schemaName);
                result[i].table = pstrdup(hotkeyValue->relName);
                result[i].value = ConstructHotkeyValue(hotkeyValue);
            }
        }
        if (pg_atomic_sub_fetch_u64(&(arr[i]->refcount), 1) == 0) {
            arr[i]->Destroy();
            arr[i] = NULL;
        }
    }
    return result;
}

static void* GetHotkeysInfo(uint32* num)
{
    if (g_instance.role != VCOORDINATOR) {
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                (errmsg("It is not supported to obtain hotkeys statistics info from DN."))));
    }

    if (!u_sess->attr.attr_resource.enable_hotkeys_collection) {
        ereport(NOTICE, (errmsg("GUC parameter 'enable_hotkeys_collection' is off")));
        return NULL;
    }

    if (!superuser() && !isMonitoradmin(GetUserId())) {
        ereport(ERROR,
            (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), (errmsg("only system/monitor admin can obtain hotkeys info"))));
    }

    HotkeyGeneralInfo* lruStatInfo = FetchHotkeysFromLRU(g_instance.stat_cxt.lru, num);

    return lruStatInfo;
}

/*
 * @Description: cleanup hot-keys statistics info on cuurent coordinator
 * @return - void
 */
Datum gs_stat_clean_hotkeys(PG_FUNCTION_ARGS)
{
    if (g_instance.role != VCOORDINATOR) {
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                (errmsg("It is not supported to clean hotkeys statistics info on DN."))));
        PG_RETURN_BOOL(false);
    }

    if (!superuser() && !isMonitoradmin(GetUserId())) {
        ereport(ERROR,
            (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), (errmsg("only system/monitor admin can clean hotkeys info"))));
    }

    pgstat_send_cleanup_hotkeys(InvalidOid, InvalidOid);

    PG_RETURN_BOOL(true);
}

/*
 * @Description: cleanup hot-keys statistics info on all coordinators
 * @return - void
 */
Datum global_stat_clean_hotkeys(PG_FUNCTION_ARGS)
{
    if (g_instance.role != VCOORDINATOR) {
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                (errmsg("It is not supported to clean hotkeys statistics info on DN."))));
        PG_RETURN_BOOL(false);
    }
    
    if (!superuser() && !isMonitoradmin(GetUserId())) {
        ereport(ERROR,
            (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), (errmsg("only system/monitor admin can clean hotkeys info"))));
    }
    /* clean local hotkey info */
    pgstat_send_cleanup_hotkeys(InvalidOid, InvalidOid);

    /* clean remote hotkey info */
    const char* query = "select * from gs_stat_clean_hotkeys()";

    StringInfoData buf;
    ParallelFunctionState* state = NULL;

    initStringInfo(&buf);
    appendStringInfoString(&buf, query);

    state = RemoteFunctionResultHandler(buf.data, NULL, NULL, true, EXEC_ON_COORDS, true);
    FreeParallelFunctionState(state);
    pfree_ext(buf.data);

    PG_RETURN_BOOL(true);
}

void CheckHotkeys(RemoteQueryState* planstate, ExecNodes* exec_nodes, RemoteQueryExecType exec_type)
{
    if (!u_sess->attr.attr_resource.enable_hotkeys_collection)
        return;

    if (g_instance.role != VCOORDINATOR || u_sess->attr.attr_sql.under_explain) {
        /* no need to collect hotkey if is explaining query or on datanode */
        return;
    }

    if (exec_type != EXEC_ON_DATANODES) {
        /* Do not collect on system table and only support hash distributed table. */
        return;
    }

    if (list_length(exec_nodes->nodeList) != 1 || exec_nodes->hotkeys == NULL)
        return;

    ListCell* lc = NULL;
    foreach (lc, exec_nodes->hotkeys) {
        HotkeyInfo* hotkeyInfo = (HotkeyInfo*)lfirst(lc);
        DebugPrintHotkeyInfo(hotkeyInfo);
        AddHotkeysToCollectList(hotkeyInfo->relationOid, hotkeyInfo->hashValue, hotkeyInfo->hotkeyValue);
    }
}
