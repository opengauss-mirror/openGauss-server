/*
 * Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 * http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 * -------------------------------------------------------------------------
 *
 * encoding.cpp
 *
 * IDENTIFICATION
 * src/gausskernel/optimizer/util/learn/encoding.cpp
 *
 * DESCRIPTION
 * Functions to encode extracted information of plan operations, to be called for
 * both training data collection and model prediction after extraction.
 *
 * -------------------------------------------------------------------------
 */

#include "access/hash.h"
#include "optimizer/clauses.h"
#include "optimizer/encoding.h"
#include "optimizer/learn.h"
#include "optimizer/streamplan.h"
#include "utils/snapmgr.h"

static inline void AppendStringInfoWithSpace(StringInfo buf, const char* fmt, ...);
static inline bool IsScan(Plan* plan);
static inline bool IsJoin(Plan* plan);
static void GetOPTEncoding(StringInfo des, char* optname, char* orientation, char* strategy, char* options, int dop,
    char* conditions, char* projections);
static void GetPlanOptProjection(PlanState* planstate, StringInfo projection, int maxlen, List* rtable);
static void GetPlanOptConditionFromQual(
    List* qual, PlanState* planstate, StringInfo condition, int maxlen, List* rtable);
static void GetPlanOptCondition(PlanState* planstate, StringInfo condition, int maxlen, List* rtable);

typedef struct {
    NodeTag nodeTag;
    char* name;
    char* strategy;
} OperationInfo;

#ifdef USE_SPQ
const unsigned int G_MAX_OPERATION_NUMBER = 69;
#else
const unsigned int G_MAX_OPERATION_NUMBER = 65;
#endif

const OperationInfo G_OPERATION_INFO_TABLE[G_MAX_OPERATION_NUMBER] = {
    {T_BaseResult,          TEXT_OPTNAME_RESULT,            ""},
    {T_VecResult,           TEXT_OPTNAME_RESULT,            ""},
    {T_ModifyTable,         TEXT_OPTNAME_MODIFY_TABLE,      ""},
    {T_VecModifyTable,      TEXT_OPTNAME_MODIFY_TABLE,      ""},
    {T_Append,              TEXT_OPTNAME_APPEND,            TEXT_STRATEGY_APPEND_PLAIN},
    {T_VecAppend,           TEXT_OPTNAME_APPEND,            TEXT_STRATEGY_APPEND_PLAIN},
    {T_MergeAppend,         TEXT_OPTNAME_APPEND,            TEXT_STRATEGY_APPEND_MERGE},
    {T_RecursiveUnion,      TEXT_OPTNAME_RECURSIVE_UNION,   ""},
    {T_BitmapAnd,           TEXT_OPTNAME_BITMAP,            TEXT_STRATEGY_BITMAP_AND},
    {T_CStoreIndexAnd,      TEXT_OPTNAME_BITMAP,            TEXT_STRATEGY_BITMAP_AND},
    {T_BitmapOr,            TEXT_OPTNAME_BITMAP,            TEXT_STRATEGY_BITMAP_OR},
    {T_CStoreIndexOr,       TEXT_OPTNAME_BITMAP,            TEXT_STRATEGY_BITMAP_OR},
    {T_NestLoop,            TEXT_OPTNAME_JOIN,              TEXT_STRATEGY_JOIN_NESTED_LOOP},
    {T_VecNestLoop,         TEXT_OPTNAME_JOIN,              TEXT_STRATEGY_JOIN_NESTED_LOOP},
    {T_MergeJoin,           TEXT_OPTNAME_JOIN,              TEXT_STRATEGY_JOIN_MERGE},
    {T_VecMergeJoin,        TEXT_OPTNAME_JOIN,              TEXT_STRATEGY_JOIN_MERGE},
    {T_HashJoin,            TEXT_OPTNAME_JOIN,              TEXT_STRATEGY_JOIN_HASH},
    {T_VecHashJoin,         TEXT_OPTNAME_JOIN,              TEXT_STRATEGY_JOIN_HASH},
    {T_CStoreScan,          TEXT_OPTNAME_SCAN,              TEXT_STRATEGY_SCAN_SEQ},
    {T_SeqScan,             TEXT_OPTNAME_SCAN,              TEXT_STRATEGY_SCAN_SEQ},
#ifdef USE_SPQ
    {T_SpqSeqScan,          TEXT_OPTNAME_SCAN,              TEXT_STRATEGY_SCAN_SEQ},
    {T_SpqIndexScan,        TEXT_OPTNAME_SCAN,              TEXT_STRATEGY_SCAN_INDEX},
    {T_SpqIndexOnlyScan,    TEXT_OPTNAME_SCAN,              TEXT_STRATEGY_SCAN_INDEX_ONLY},
    {T_SpqBitmapHeapScan,   TEXT_OPTNAME_SCAN,              TEXT_STRATEGY_SCAN_BITMAP_HEAP},
#endif
    {T_IndexScan,           TEXT_OPTNAME_SCAN,              TEXT_STRATEGY_SCAN_INDEX},
    {T_CStoreIndexScan,     TEXT_OPTNAME_SCAN,              TEXT_STRATEGY_SCAN_INDEX},
    {T_IndexOnlyScan,       TEXT_OPTNAME_SCAN,              TEXT_STRATEGY_SCAN_INDEX_ONLY},
    {T_BitmapIndexScan,     TEXT_OPTNAME_SCAN,              TEXT_STRATEGY_SCAN_BITMAP_INDEX},
    {T_CStoreIndexHeapScan, TEXT_OPTNAME_SCAN,              TEXT_STRATEGY_SCAN_BITMAP_HEAP},
    {T_BitmapHeapScan,      TEXT_OPTNAME_SCAN,              TEXT_STRATEGY_SCAN_BITMAP_HEAP},
    {T_TidScan,             TEXT_OPTNAME_SCAN,              TEXT_STRATEGY_SCAN_TID},
    {T_CStoreIndexCtidScan, TEXT_OPTNAME_SCAN,              TEXT_STRATEGY_SCAN_TID},
    {T_SubqueryScan,        TEXT_OPTNAME_SCAN,              TEXT_STRATEGY_SCAN_SUBQUERY},
    {T_VecSubqueryScan,     TEXT_OPTNAME_SCAN,              TEXT_STRATEGY_SCAN_SUBQUERY},
    {T_FunctionScan,        TEXT_OPTNAME_SCAN,              TEXT_STRATEGY_SCAN_FUNCTION},
    {T_ValuesScan,          TEXT_OPTNAME_SCAN,              TEXT_STRATEGY_SCAN_VALUES},
    {T_CteScan,             TEXT_OPTNAME_SCAN,              TEXT_STRATEGY_SCAN_CTE},
    {T_WorkTableScan,       TEXT_OPTNAME_SCAN,              TEXT_STRATEGY_SCAN_WORK_TABLE},
    {T_VecRemoteQuery,      TEXT_OPTNAME_STREAM,            ""},
    {T_RemoteQuery,         TEXT_OPTNAME_STREAM,            ""},
    {T_Stream,              TEXT_OPTNAME_STREAM,            ""},
    {T_VecStream,           TEXT_OPTNAME_STREAM,            ""},
    {T_ForeignScan,         TEXT_OPTNAME_SCAN,              TEXT_STRATEGY_SCAN_FOREIGN},
    {T_VecForeignScan,      TEXT_OPTNAME_SCAN,              TEXT_STRATEGY_SCAN_FOREIGN},
    {T_ExtensiblePlan,      TEXT_OPTNAME_EXTENSIBLE,        ""},
    {T_Material,            TEXT_OPTNAME_MATERIALIZE,       ""},
    {T_VecMaterial,         TEXT_OPTNAME_MATERIALIZE,       ""},
    {T_Sort,                TEXT_OPTNAME_SORT,              ""},
    {T_VecSort,             TEXT_OPTNAME_SORT,              ""},
    {T_Group,               TEXT_OPTNAME_GROUP,             ""},
    {T_VecGroup,            TEXT_OPTNAME_GROUP,             ""},
    {T_Agg,                 TEXT_OPTNAME_AGG,               ""},
    {T_VecAgg,              TEXT_OPTNAME_AGG,               ""},
    {T_WindowAgg,           TEXT_OPTNAME_AGG,               TEXT_STRATEGY_AGG_HASHED},
    {T_VecWindowAgg,        TEXT_OPTNAME_AGG,               TEXT_STRATEGY_AGG_HASHED},
    {T_Unique,              TEXT_OPTNAME_UNIQUE,            TEXT_OPTNAME_UNIQUE},
    {T_VecUnique,           TEXT_OPTNAME_UNIQUE,            TEXT_OPTNAME_UNIQUE},
    {T_SetOp,               TEXT_OPTNAME_SET_OP,            ""},
    {T_VecSetOp,            TEXT_OPTNAME_SET_OP,            ""},
    {T_LockRows,            TEXT_OPTNAME_LOCKROWS,          ""},
    {T_Limit,               TEXT_OPTNAME_LIMIT,             ""},
    {T_VecLimit,            TEXT_OPTNAME_LIMIT,             ""},
    {T_Hash,                TEXT_OPTNAME_HASH,              ""},
    {T_PartIterator,        TEXT_OPTNAME_PART_ITER,         ""},
    {T_VecPartIterator,     TEXT_OPTNAME_PART_ITER,         ""},
    {T_VecToRow,            TEXT_OPTNAME_ADAPTOR,           ""},
    {T_RowToVec,            TEXT_OPTNAME_ADAPTOR,           ""}
};

Datum encode_plan_node(PG_FUNCTION_ARGS)
{
#ifdef ENABLE_MULTIPLE_NODES
    ereport(ERROR, (errmodule(MOD_OPT_AI), errmsg("This function is not available in multipule nodes mode")));
#endif
    int argIdx = -1;
    char* optname = NULL;
    char* orientation = NULL;
    char* strategy = NULL;
    char* options = NULL;
    char* quals = NULL;
    char* projection = NULL;

    if (PG_ARGISNULL(++argIdx))
        ereport(ERROR, (errmodule(MOD_OPT_AI), errmsg("Optname should not be NULL.")));
    else
        optname = (char*)(text_to_cstring(PG_GETARG_TEXT_P(argIdx)));
    
    if (PG_ARGISNULL(++argIdx))
        ereport(ERROR, (errmodule(MOD_OPT_AI), errmsg("Orientation should not be NULL.")));
    else
        orientation = (char*)(text_to_cstring(PG_GETARG_TEXT_P(argIdx)));

    if (PG_ARGISNULL(++argIdx))
        strategy = pstrdup(" ");
    else
        strategy = (char*)(text_to_cstring(PG_GETARG_TEXT_P(argIdx)));

    if (PG_ARGISNULL(++argIdx))
        options = pstrdup(" ");
    else
        options = (char*)(text_to_cstring(PG_GETARG_TEXT_P(argIdx)));

    int dop = (int)(PG_GETARG_INT8(++argIdx));

    if (PG_ARGISNULL(++argIdx))
        quals = pstrdup(" ");
    else
        quals = (char*)(text_to_cstring(PG_GETARG_TEXT_P(argIdx)));

    if (PG_ARGISNULL(++argIdx))
        projection = pstrdup(" ");
    else
        projection = (char*)(text_to_cstring(PG_GETARG_TEXT_P(argIdx)));

    StringInfo code = makeStringInfo();
    GetOPTEncoding(code, optname, orientation, strategy, options, dop, quals, projection);

    pfree(optname);
    pfree(orientation);
    pfree(strategy);
    pfree(options);
    pfree(quals);
    pfree(projection);

    text* res = cstring_to_text(code->data);
    pfree(code->data);
    pfree(code);
    PG_RETURN_TEXT_P(res);
}

/* *
 * @Description: Save data from pgxc_wlm_plan_encoding_table to a tmp file for training
 * @in file name
 * @out data written in file
 */
void SaveDataToFile(const char* filename)
{
    Oid encodingTable = RelnameGetRelid("gs_wlm_plan_encoding_table");
    Relation relation = heap_open(encodingTable, AccessShareLock);
    SysScanDesc scan = systable_beginscan(relation, InvalidOid, false, NULL, 0, NULL);
    HeapTuple tuple;
    TreeEncPtr enc;
    FILE* fpout = NULL;
    errno_t ret = EOK;
    size_t rc;
    char* str = NULL;
    char buf[MAX_LEN_ROW];  // single row data

    /*
     * Open the file to write out the encodings and related info
     */
    if (unlink(filename) < 0) {
        ereport(DEBUG2,
            (errmodule(MOD_OPT_AI), errcode_for_file_access(), errmsg("could not unlink file \"%s\": %m", filename)));
    } else {
        ereport(DEBUG2, (errmodule(MOD_OPT_AI), errmsg("Unlinked file: \"%s\"", filename)));
    }
    fpout = AllocateFile(filename, PG_BINARY_A);
    if (fpout == NULL) {
        ereport(ERROR,
            (errmodule(MOD_OPT_AI),
                errcode_for_file_access(),
                errmsg("could not open temporary statistics file \"%s\": %m", filename)));
    }
    while (HeapTupleIsValid(tuple = systable_getnext(scan))) {
        ret = memset_s(buf, sizeof(buf), 0, sizeof(buf));
        securec_check(ret, "\0", "\0");
        enc = (TreeEncPtr)GETSTRUCT(tuple);
        str = (char*)(text_to_cstring(enc->encode));
        ret = sprintf_s(buf,
            sizeof(buf),
            "%ld, %d, %d, %s, %ld, %ld, %ld, %d\n",
            enc->query_id,
            enc->plan_node_id,
            enc->parent_node_id,
            str,
            enc->startup_time,
            enc->total_time,
            enc->rows,
            enc->peak_memory);
        securec_check_ss(ret, "\0", "\0");
        rc = fwrite(buf, strlen(buf), 1, fpout);
        if (rc != 1)
            ereport(ERROR,
                (errmodule(MOD_OPT_AI),
                    errcode_for_file_access(),
                    errmsg("could not open temporary statistics file \"%s\": %m", filename)));
        pfree(str);
    }
    (void)FreeFile(fpout);
    systable_endscan(scan);
    heap_close(relation, AccessShareLock);
}

/*
 * initialize text arrays which determines the encoding
 * for new items please append to the rear of the list.
 */
const OptText OPT_TEXT_ARR[LEN_ENCODE_OPTNAME] = {
    {TEXT_OPTNAME_ADAPTOR, LEN_ENCODE_STRATEGY_ADAPTOR, {}, LEN_ENCODE_OPTION_ADAPTOR, {}},
    {TEXT_OPTNAME_AGG,
        LEN_ENCODE_STRATEGY_AGG,
        {TEXT_STRATEGY_AGG_PLAIN, TEXT_STRATEGY_AGG_SORTED, TEXT_STRATEGY_AGG_HASHED, TEXT_STRATEGY_AGG_WINDOW},
        LEN_ENCODE_OPTION_AGG,
        {}},
    {TEXT_OPTNAME_APPEND,
        LEN_ENCODE_STRATEGY_APPEND,
        {TEXT_STRATEGY_APPEND_PLAIN, TEXT_STRATEGY_APPEND_MERGE},
        LEN_ENCODE_OPTION_APPEND,
        {}},
    {TEXT_OPTNAME_BITMAP,
        LEN_ENCODE_STRATEGY_BITMAP,
        {TEXT_STRATEGY_BITMAP_AND, TEXT_STRATEGY_BITMAP_OR},
        LEN_ENCODE_OPTION_BITMAP,
        {}},
    {TEXT_OPTNAME_EXTENSIBLE, LEN_ENCODE_STRATEGY_EXTENSIBLE, {}, LEN_ENCODE_OPTION_EXTENSIBLE, {}},
    {TEXT_OPTNAME_GROUP, LEN_ENCODE_STRATEGY_GROUP, {}, LEN_ENCODE_OPTION_GROUP, {}},
    {TEXT_OPTNAME_HASH, LEN_ENCODE_STRATEGY_HASH, {}, LEN_ENCODE_OPTION_HASH, {}},
    {TEXT_OPTNAME_JOIN,
        LEN_ENCODE_STRATEGY_JOIN,
        {TEXT_STRATEGY_JOIN_NESTED_LOOP, TEXT_STRATEGY_JOIN_MERGE, TEXT_STRATEGY_JOIN_HASH},
        LEN_ENCODE_OPTION_JOIN,
        {TEXT_OPTION_JOIN_INNER,
            TEXT_OPTION_JOIN_LEFT,
            TEXT_OPTION_JOIN_FULL,
            TEXT_OPTION_JOIN_RIGHT,
            TEXT_OPTION_JOIN_SEMI,
            TEXT_OPTION_JOIN_ANTI,
            TEXT_OPTION_JOIN_UNIQUE}},
    {TEXT_OPTNAME_LIMIT, LEN_ENCODE_STRATEGY_LIMIT, {}, LEN_ENCODE_OPTION_LIMIT, {}},
    {TEXT_OPTNAME_LOCKROWS, LEN_ENCODE_STRATEGY_LOCKROWS, {}, LEN_ENCODE_OPTION_LOCKROWS, {}},
    {TEXT_OPTNAME_MATERIALIZE, LEN_ENCODE_STRATEGY_MATERIALIZE, {}, LEN_ENCODE_OPTION_MATERIALIZE, {}},
    {TEXT_OPTNAME_MODIFY_TABLE,
        LEN_ENCODE_STRATEGY_MODIFY_TABLE,
        {TEXT_STRATEGY_MODIFY_TABLE_INSERT,
            TEXT_STRATEGY_MODIFY_TABLE_UPDATE,
            TEXT_STRATEGY_MODIFY_TABLE_DELETE,
            TEXT_STRATEGY_MODIFY_TABLE_MERGE},
        LEN_ENCODE_OPTION_MODIFY_TABLE,
        {}},
    {TEXT_OPTNAME_PART_ITER, LEN_ENCODE_STRATEGY_PART_ITER, {}, LEN_ENCODE_OPTION_PART_ITER, {}},
    {TEXT_OPTNAME_RECURSIVE_UNION, LEN_ENCODE_STRATEGY_RECURSIVE_UNION, {}, LEN_ENCODE_OPTION_RECURSIVE_UNION, {}},
    {TEXT_OPTNAME_RESULT, LEN_ENCODE_STRATEGY_RESULT, {}, LEN_ENCODE_OPTION_RESULT, {}},
    {TEXT_OPTNAME_SCAN,
        LEN_ENCODE_STRATEGY_SCAN,
        {TEXT_STRATEGY_SCAN_SEQ,
            TEXT_STRATEGY_SCAN_INDEX,
            TEXT_STRATEGY_SCAN_INDEX_ONLY,
            TEXT_STRATEGY_SCAN_BITMAP_INDEX,
            TEXT_STRATEGY_SCAN_BITMAP_HEAP,
            TEXT_STRATEGY_SCAN_TID,
            TEXT_STRATEGY_SCAN_SUBQUERY,
            TEXT_STRATEGY_SCAN_FOREIGN,
            TEXT_STRATEGY_SCAN_DATA_NODE,
            TEXT_STRATEGY_SCAN_FUNCTION,
            TEXT_STRATEGY_SCAN_VALUES,
            TEXT_STRATEGY_SCAN_CTE,
            TEXT_STRATEGY_SCAN_WORK_TABLE},
        LEN_ENCODE_OPTION_SCAN,
        {TEXT_OPTION_SCAN_PARTITIONED, TEXT_OPTION_SCAN_DFS, TEXT_OPTION_SCAN_SAMPLE}},
    {TEXT_OPTNAME_SET_OP,
        LEN_ENCODE_STRATEGY_SET_OP,
        {TEXT_STRATEGY_SET_OP_SORTED, TEXT_STRATEGY_SET_OP_HASHED},
        LEN_ENCODE_OPTION_SET_OP,
        {}},
    {TEXT_OPTNAME_SORT, LEN_ENCODE_STRATEGY_SORT, {}, LEN_ENCODE_OPTION_SORT, {}},
    {TEXT_OPTNAME_STREAM,
        LEN_ENCODE_STRATEGY_STREAM,
        {TEXT_STRATEGY_STREAM_BROADCAST,
            TEXT_STRATEGY_STREAM_REDISTRIBUTE,
            TEXT_STRATEGY_STREAM_GATHER,
            TEXT_STRATEGY_STREAM_ROUND_ROBIN,
            TEXT_STRATEGY_STREAM_SCAN_GATHER,
            TEXT_STRATEGY_STREAM_PLAN_ROUTER,
            TEXT_STRATEGY_STREAM_HYBRID},
        LEN_ENCODE_OPTION_STREAM,
        {TEXT_OPTION_STREAM_LOCAL, TEXT_OPTION_STREAM_SPLIT}},
    {TEXT_OPTNAME_UNIQUE, LEN_ENCODE_STRATEGY_UNIQUE, {}, LEN_ENCODE_OPTION_UNIQUE, {}}
};

static void EncodeToBitamp(const char* strategy, const char* options, const char* optname, int* bitmapOptname,
    int* bitmapStrategy, int* bitmapOption)
{
    int i, j;
    errno_t ret = EOK;
    ret = memset_s(bitmapOptname, LEN_ENCODE_OPTNAME * sizeof(int), 0, LEN_ENCODE_OPTNAME * sizeof(int));
    securec_check(ret, "\0", "\0");

    ret = memset_s(bitmapStrategy, LEN_ENCODE_STRATEGY * sizeof(int), 0, LEN_ENCODE_STRATEGY * sizeof(int));
    securec_check(ret, "\0", "\0");

    ret = memset_s(bitmapOption, LEN_ENCODE_OPTION * sizeof(int), 0, LEN_ENCODE_OPTION * sizeof(int));
    securec_check(ret, "\0", "\0");

    /* encode the given text */
    for (i = 0; i < LEN_ENCODE_OPTNAME; i++) {
        if (strcmp(optname, OPT_TEXT_ARR[i].optname) == 0) {
            bitmapOptname[i] = 1;
            for (j = 0; j < OPT_TEXT_ARR[i].len_strategy; j++) {
                if (strcmp(strategy, OPT_TEXT_ARR[i].strategy[j]) == 0) {
                    bitmapStrategy[j] = 1;
                    break;
                }
            }
            for (j = 0; j < OPT_TEXT_ARR[i].len_option; j++) {
                if (strcmp(options, OPT_TEXT_ARR[i].options[j]) == 0)
                    bitmapOption[j] = 1;
            }
        }
    }
}

static void AppendEncodedInfo(StringInfo des, int dop, const int* bitmapOrientation, const int* bitmapOptname,
    const int* bitmapStrategy, const int* bitmapOption, const int* bitmapCondition, const int* bitmapProjection)
{
    int i;
    float dopInverse = 1.0 / ((float)dop);
    appendStringInfo(des, "%.3f ", dopInverse);

    /* append orientation encoding */
    for (i = 0; i < LEN_ENCODE_ORIENTATION; i++) {
        appendStringInfoChar(des, '0' + bitmapOrientation[i]);
        appendStringInfoChar(des, ' ');
    }

    /* append optname encoding */
    for (i = 0; i < LEN_ENCODE_OPTNAME; i++) {
        appendStringInfoChar(des, '0' + bitmapOptname[i]);
        appendStringInfoChar(des, ' ');
    }

    /* append strategy encoding */
    for (i = 0; i < LEN_ENCODE_STRATEGY; i++) {
        appendStringInfoChar(des, '0' + bitmapStrategy[i]);
        appendStringInfoChar(des, ' ');
    }

    /* append options encoding */
    for (i = 0; i < LEN_ENCODE_OPTION; i++) {
        appendStringInfoChar(des, '0' + bitmapOption[i]);
        appendStringInfoChar(des, ' ');
    }

    /* append conditions encoding */
    for (i = 0; i < LEN_ENCODE_CONDITION; i++) {
        appendStringInfoChar(des, '0' + bitmapCondition[i]);
        appendStringInfoChar(des, ' ');
    }

    /* append projections encoding */
    for (i = 0; i < LEN_ENCODE_PROJECTION; i++) {
        appendStringInfoChar(des, '0' + bitmapProjection[i]);
        appendStringInfoChar(des, ' ');
    }
}

/* *
 * @Description: Internel worker to encode features into encoded numbers between 0 and 1
 * e.g. for a hash anti join operator it may have the following attributes
 * | dop | orientation | operation | strategy | options  |   condition                      | projection
 * ----+-----+-------------+-----------+----------+----------+----------------------------------+----------------------
 * val | 2   | Col         | Join      | Hash     | Anti     | public.part.key = public.sup.key | public.partsupp.brand
 * ----+-----+-------------+-----------+----------+----------+----------------------------------+----------------------
 * code| 0.5 | 01          | 0..010..0 | 0010..0  | 00010..0 | 010010000111                     | 101010001100
 * this function essentially gives a static mapping from val to code.
 * For now we hash the conditions and projections. This could be improved by one-hot encoding of schema information.
 * @in optname, orientation, strategy, ioptions, dop, conditions
 * @out result will be stored in projection
 */
static void GetOPTEncoding(StringInfo des, char* optname, char* orientation, char* strategy, char* options, int dop,
    char* conditions, char* projections)
{
    AssertEreport(optname != NULL && orientation != NULL && strategy != NULL && options != NULL,
        MOD_OPT_AI,
        "NULL Datum ptr when encoding.");

    errno_t ret = EOK;
    int i;

    /* initialize bitmaps for encoding */
    int bitmapOptname[LEN_ENCODE_OPTNAME];

    int bitmapOrientation[LEN_ENCODE_ORIENTATION];
    ret = memset_s(bitmapOrientation, LEN_ENCODE_ORIENTATION * sizeof(int), 0, LEN_ENCODE_ORIENTATION * sizeof(int));
    securec_check(ret, "\0", "\0");

    int bitmapStrategy[LEN_ENCODE_STRATEGY];

    int bitmapOption[LEN_ENCODE_OPTION];

    int bitmapCondition[LEN_ENCODE_PROJECTION];
    ret = memset_s(bitmapCondition, LEN_ENCODE_CONDITION * sizeof(int), 0, LEN_ENCODE_CONDITION * sizeof(int));
    securec_check(ret, "\0", "\0");

    int bitmapProjection[LEN_ENCODE_CONDITION];
    ret = memset_s(bitmapProjection, LEN_ENCODE_PROJECTION * sizeof(int), 0, LEN_ENCODE_PROJECTION * sizeof(int));
    securec_check(ret, "\0", "\0");

    if (strcmp(orientation, TEXT_ORIENTATION_ROW) == 0) {
        bitmapOrientation[0] = 1;
    } else if (strcmp(orientation, TEXT_ORIENTATION_COL) == 0) {
        bitmapOrientation[1] = 1;
    }

    /* *
     * for now we hash the conditions and projections to 8-bit binary encoding
     */
    uint32 hashcodeCondition = DatumGetUInt32(hash_any((const unsigned char*)conditions, strlen(conditions)));
    uint32 hashcodeProjection = DatumGetUInt32(hash_any((const unsigned char*)projections, strlen(projections)));
    static const int binaryBase = 2;
    for (i = 0; i < LEN_ENCODE_PROJECTION; i++) {
        bitmapCondition[i] = hashcodeCondition % binaryBase;
        hashcodeCondition = hashcodeCondition >> 1;
        bitmapProjection[i] = hashcodeProjection % binaryBase;
        hashcodeProjection = hashcodeProjection >> 1;
    }

    /* encode the given text */
    EncodeToBitamp(strategy, options, optname, bitmapOptname, bitmapStrategy, bitmapOption);

    /* append encoded info */
    AppendEncodedInfo(
        des, dop, bitmapOrientation, bitmapOptname, bitmapStrategy, bitmapOption, bitmapCondition, bitmapProjection);

    ereport(DEBUG1,
        (errmodule(MOD_OPT_AI),
            errmsg("Plan Node Info:\n\t%s|%s|%s|%s|%d|%s|%s\n\t%s",
                optname,
                orientation,
                strategy,
                options,
                dop,
                conditions,
                projections,
                des->data)));
}

static inline void AppendStringInfoWithSpace(StringInfo buf, const char* fmt, ...)
{
    appendStringInfo(buf, "%s", fmt);
    appendStringInfoChar(buf, ' ');
}

static inline bool IsScan(Plan* plan)
{
    return IsA(plan, SubqueryScan) || IsA(plan, Scan) || IsA(plan, SeqScan) || IsA(plan, IndexScan) ||
           IsA(plan, IndexOnlyScan) || IsA(plan, BitmapIndexScan) || IsA(plan, VecSubqueryScan) ||
           IsA(plan, BitmapHeapScan) || IsA(plan, TidScan) || IsA(plan, CStoreScan) || IsA(plan, VecForeignScan) ||
           IsA(plan, CStoreIndexScan) || IsA(plan, CStoreIndexCtidScan) || IsA(plan, CStoreIndexHeapScan) ||
           IsA(plan, FunctionScan) || IsA(plan, ValuesScan) || IsA(plan, CteScan) ||
           IsA(plan, WorkTableScan) || IsA(plan, ForeignScan) || IsA(plan, VecScan) ||
           IsA(plan, VecIndexScan) || IsA(plan, VecIndexOnlyScan) || IsA(plan, VecBitmapIndexScan) ||
           IsA(plan, VecBitmapHeapScan);
}

static inline bool IsJoin(Plan* plan)
{
    return IsA(plan, VecNestLoop) || IsA(plan, VecMergeJoin) || IsA(plan, VecHashJoin) || IsA(plan, NestLoop) ||
           IsA(plan, MergeJoin) || IsA(plan, HashJoin) || IsA(plan, Join);
}

static char* GetOperationName(Plan* plan)
{
    char* operationName = NULL;
    bool found = false;
    NodeTag nodeTag = nodeTag(plan);
    for (unsigned int i = 0; i < G_MAX_OPERATION_NUMBER; i++) {
        if (nodeTag == G_OPERATION_INFO_TABLE[i].nodeTag) {
            operationName = pstrdup(G_OPERATION_INFO_TABLE[i].name);
            found = true;
            break;
        }
    }

    if (!found) {
        ereport(DEBUG1, (errmodule(MOD_OPT_AI), errmsg("nodeTag %d not matched in PHGetPlanNodeText()\n", nodeTag)));
        operationName = pstrdup(TEXT_UNKNOWN);
    }

    if (nodeTag == T_VecRemoteQuery || nodeTag == T_RemoteQuery) {
        pfree(operationName);
        RemoteQuery* rq = (RemoteQuery*)plan;
        if (rq->position == PLAN_ROUTER || rq->position == SCAN_GATHER || rq->is_simple) {
            operationName = pstrdup(TEXT_OPTNAME_STREAM);
        } else {
            operationName = pstrdup(TEXT_OPTNAME_SCAN);
        }
    }

    return operationName;
}

static void GetModifyTableStrategy(Plan* plan, char** strategy)
{
    NodeTag nodeTag = nodeTag(plan);
    if (nodeTag == T_ModifyTable || nodeTag == T_VecModifyTable) {
        pfree(*strategy);
        switch (((ModifyTable*)plan)->operation) {
            case CMD_INSERT:
                *strategy = pstrdup(TEXT_STRATEGY_MODIFY_TABLE_INSERT);
                break;
            case CMD_UPDATE:
                *strategy = pstrdup(TEXT_STRATEGY_MODIFY_TABLE_UPDATE);
                break;
            case CMD_DELETE:
                *strategy = pstrdup(TEXT_STRATEGY_MODIFY_TABLE_DELETE);
                break;
            case CMD_MERGE:
                *strategy = pstrdup(TEXT_STRATEGY_MODIFY_TABLE_MERGE);
                break;
            default:
                *strategy = pstrdup(TEXT_UNKNOWN);
                break;
        }
    }
}

static void GetRemoteQueryStrategy(Plan* plan, char** strategy)
{
    NodeTag nodeTag = nodeTag(plan);
    if (nodeTag == T_VecRemoteQuery || nodeTag == T_RemoteQuery) {
        pfree(*strategy);
        RemoteQuery* rq = (RemoteQuery*)plan;
        if (rq->position == PLAN_ROUTER) {
            *strategy = pstrdup(TEXT_STRATEGY_STREAM_PLAN_ROUTER);
        } else if (rq->position == SCAN_GATHER) {
            *strategy = pstrdup(TEXT_STRATEGY_STREAM_SCAN_GATHER);
        } else {
            if (rq->is_simple) {
                *strategy = pstrdup(TEXT_STRATEGY_STREAM_GATHER);
            } else {
                *strategy = pstrdup(TEXT_STRATEGY_SCAN_DATA_NODE);
            }
        }
    }
}

static void GetAggStrategy(Plan* plan, char** strategy)
{
    NodeTag nodeTag = nodeTag(plan);
    if (nodeTag == T_Agg || nodeTag == T_VecAgg) {
        pfree(*strategy);
        switch (((Agg*)plan)->aggstrategy) {
            case AGG_PLAIN:
                *strategy = pstrdup(TEXT_STRATEGY_AGG_PLAIN);
                break;
            case AGG_SORTED:
                *strategy = pstrdup(TEXT_STRATEGY_AGG_SORTED);
                break;
            case AGG_HASHED:
                *strategy = pstrdup(TEXT_STRATEGY_AGG_HASHED);
                break;
            default:
                *strategy = pstrdup(TEXT_UNKNOWN);
                break;
        }
    }
}

static void GetSetOpStrategy(Plan* plan, char** strategy)
{
    NodeTag nodeTag = nodeTag(plan);
    if (nodeTag == T_SetOp || nodeTag == T_VecSetOp) {
        pfree(*strategy);
        switch (((SetOp*)plan)->strategy) {
            case SETOP_SORTED:
                *strategy = pstrdup(TEXT_STRATEGY_SET_OP_SORTED);
                break;
            case SETOP_HASHED:
                *strategy = pstrdup(TEXT_STRATEGY_SET_OP_HASHED);
                break;
            default:
                *strategy = pstrdup(TEXT_UNKNOWN);
                break;
        }
    }
}

static void GetStreamStrategy(Plan* plan, char** strategy)
{
    NodeTag nodeTag = nodeTag(plan);
    if (nodeTag == T_Stream || nodeTag == T_VecStream) {
        pfree(*strategy);
        Stream* streamNode = (Stream*)plan;
        switch (streamNode->type) {
            case STREAM_BROADCAST:
                *strategy = pstrdup(TEXT_STRATEGY_STREAM_BROADCAST);
                break;
            case STREAM_REDISTRIBUTE:
                switch (streamNode->smpDesc.distriType) {
                    case LOCAL_BROADCAST:
                        *strategy = pstrdup(TEXT_STRATEGY_STREAM_BROADCAST);
                        break;
                    case LOCAL_ROUNDROBIN:
                        *strategy = pstrdup(TEXT_STRATEGY_STREAM_HYBRID);
                        break;
                    case REMOTE_SPLIT_DISTRIBUTE:
                    case LOCAL_DISTRIBUTE:
                    default:
                        *strategy = pstrdup(TEXT_STRATEGY_STREAM_REDISTRIBUTE);
                        break;
                }
                break;
            case STREAM_HYBRID:
                *strategy = pstrdup(TEXT_STRATEGY_STREAM_HYBRID);
                break;
            default:
                *strategy = pstrdup(TEXT_UNKNOWN);
                break;
        }
    }
}

static char* GetStrategy(Plan* plan)
{
    NodeTag nodeTag = nodeTag(plan);
    char* strategy = NULL;
    bool found = false;
    for (unsigned int i = 0; i < G_MAX_OPERATION_NUMBER; i++) {
        if (nodeTag == G_OPERATION_INFO_TABLE[i].nodeTag) {
            strategy = pstrdup(G_OPERATION_INFO_TABLE[i].strategy);
            found = true;
            break;
        }
    }

    if (!found) {
        ereport(DEBUG1, (errmodule(MOD_OPT_AI), errmsg("nodeTag %d not matched in PHGetPlanNodeText()\n", nodeTag)));
        strategy = pstrdup(TEXT_UNKNOWN);
    }

    GetModifyTableStrategy(plan, &strategy);

    GetRemoteQueryStrategy(plan, &strategy);

    GetAggStrategy(plan, &strategy);

    GetSetOpStrategy(plan, &strategy);

    GetStreamStrategy(plan, &strategy);

    return strategy;
}

static void GetStreamOption(Plan* plan, StringInfo option)
{
    NodeTag nodeTag = nodeTag(plan);
    if (nodeTag == T_Stream || nodeTag == T_VecStream) {
        Stream* streamNode = (Stream*)plan;
        switch (streamNode->type) {
            case STREAM_BROADCAST:
                if (REMOTE_SPLIT_BROADCAST == streamNode->smpDesc.distriType) {
                    AppendStringInfoWithSpace(option, TEXT_OPTION_STREAM_SPLIT);
                }
                break;
            case STREAM_REDISTRIBUTE:
                switch (streamNode->smpDesc.distriType) {
                    case LOCAL_DISTRIBUTE:
                    case LOCAL_BROADCAST:
                    case LOCAL_ROUNDROBIN:
                        AppendStringInfoWithSpace(option, TEXT_OPTION_STREAM_LOCAL);
                        break;
                    case REMOTE_SPLIT_DISTRIBUTE:
                        AppendStringInfoWithSpace(option, TEXT_OPTION_STREAM_SPLIT);
                        break;
                    default:
                        break;
                }
                break;
            case STREAM_HYBRID:
            default:
                break;
        }
    }
}

static void GetJoinOption(Plan* plan, StringInfo option)
{
    switch (((Join*)plan)->jointype) {
        case JOIN_INNER:
            AppendStringInfoWithSpace(option, TEXT_OPTION_JOIN_INNER);
            break;
        case JOIN_LEFT:
            AppendStringInfoWithSpace(option, TEXT_OPTION_JOIN_LEFT);
            break;
        case JOIN_FULL:
            AppendStringInfoWithSpace(option, TEXT_OPTION_JOIN_FULL);
            break;
        case JOIN_RIGHT:
            AppendStringInfoWithSpace(option, TEXT_OPTION_JOIN_RIGHT);
            break;
        case JOIN_SEMI:
            AppendStringInfoWithSpace(option, TEXT_OPTION_JOIN_SEMI);
            break;
        case JOIN_ANTI:
            AppendStringInfoWithSpace(option, TEXT_OPTION_JOIN_ANTI);
            break;
        case JOIN_RIGHT_SEMI:
            AppendStringInfoWithSpace(option, TEXT_OPTION_JOIN_RIGHT);
            AppendStringInfoWithSpace(option, TEXT_OPTION_JOIN_SEMI);
            break;
        case JOIN_RIGHT_ANTI:
            AppendStringInfoWithSpace(option, TEXT_OPTION_JOIN_RIGHT);
            AppendStringInfoWithSpace(option, TEXT_OPTION_JOIN_ANTI);
            break;
        case JOIN_UNIQUE_OUTER:
        case JOIN_UNIQUE_INNER:
            AppendStringInfoWithSpace(option, TEXT_OPTION_JOIN_UNIQUE);
            break;
        case JOIN_LEFT_ANTI_FULL:
            AppendStringInfoWithSpace(option, TEXT_OPTION_JOIN_LEFT);
            AppendStringInfoWithSpace(option, TEXT_OPTION_JOIN_ANTI);
            AppendStringInfoWithSpace(option, TEXT_OPTION_JOIN_FULL);
            break;
        case JOIN_RIGHT_ANTI_FULL:
            AppendStringInfoWithSpace(option, TEXT_OPTION_JOIN_RIGHT);
            AppendStringInfoWithSpace(option, TEXT_OPTION_JOIN_ANTI);
            AppendStringInfoWithSpace(option, TEXT_OPTION_JOIN_FULL);
            break;
        default:
            AppendStringInfoWithSpace(option, TEXT_UNKNOWN);
            break;
    }
}

/* *
 * @Description: This piece of code aims to give standard text description for different node type for encoding
 * convenience
 */
void PHGetPlanNodeText(Plan* plan, char** pname, bool* isRow, char** strategy, char** optionData)
{
    NodeTag nodeTag = nodeTag(plan);
    *isRow = (nodeTag >= T_VecPlan) ? false : true;
    *strategy = "";
    StringInfo option = makeStringInfo();

    *pname = GetOperationName(plan);

    *strategy = GetStrategy(plan);

    if (nodeTag == T_Stream || nodeTag == T_VecStream) {
        GetStreamOption(plan, option);
    } else if (IsJoin(plan)) {
        GetJoinOption(plan, option);
    } else if (IsScan(plan)) {
        if (((Scan*)plan)->tablesample) {
            AppendStringInfoWithSpace(option, TEXT_OPTION_SCAN_SAMPLE);
        }

        if (((Scan*)plan)->isPartTbl) {
            AppendStringInfoWithSpace(option, TEXT_OPTION_SCAN_PARTITIONED);
        }
    }

    if (optionData != NULL) {
        *optionData = pstrdup(option->data);
    }

    pfree(option->data);
    pfree(option);
}

/* *
 * @Description: Internel worker to extract projection info
 * @in planstate and rtable to extract information. maxlen is the maximum length that we want in projectioninfo
 * @out result will be stored in projection
 */
static void GetPlanOptProjection(PlanState* planstate, StringInfo projection, int maxlen, List* rtable)
{
    List* result = NIL;
    ListCell* lc = NULL;
    Plan* plan = planstate->plan;
    List* context = NIL;
    bool first = true;
    if (plan->targetlist == NIL) {
        return;
    }

    /* Foreign Scan not supported */
    if (IsA(plan, ForeignScan) || IsA(plan, VecForeignScan)) {
        return;
    }

    /* Set up deparsing context */
    context = deparse_context_for_planstate((Node*)planstate, NIL, rtable);

    /* Deparse each result column (we now include resjunk ones) */
    foreach (lc, plan->targetlist) {
        TargetEntry* tle = (TargetEntry*)lfirst(lc);
        result = lappend(result, deparse_expression((Node*)tle->expr, context, true, false, true));
    }

    /* Append to projection info to be returned */
    if (result == NIL) {
        return;
    }

    foreach (lc, result) {
        if (projection->len > maxlen) {
            break;
        }
        if (!first) {
            appendStringInfoString(projection, ", ");
        }
        appendStringInfoString(projection, (const char*)lfirst(lc));
        first = false;
    }
    list_free_deep(result);
}

/* *
 * @Description: Internel worker to extract quals info
 * @in qual, planstate and rtable to extract information. maxlen is the maximum length that we want in qual_info
 * @out result will be stored in condition
 */
static void GetPlanOptConditionFromQual(
    List* qual, PlanState* planstate, StringInfo condition, int maxlen, List* rtable)
{
    /* add splitter if multiple types of quals coexist */
    if (condition->len > 0) {
        appendStringInfoString(condition, " && ");
    }

    /* no need to work empty quals */
    if (qual == NIL) {
        return;
    }

    Node* node = NULL;
    List* context = NIL;
    char* exprstr = NULL;

    /* convert AND list to explicit AND */
    node = (Node*)make_ands_explicit(qual);

    /* Set up deparsing context */
    context = deparse_context_for_planstate((Node*)planstate, NIL, rtable);

    /* Deparse the expression */
    exprstr = deparse_expression(node, context, true, false, true);
    if (strlen(exprstr) + condition->len > (uint)maxlen) {
        return;
    }
    appendStringInfoString(condition, exprstr);
}

static void GetSpecialPlanOptCondition(PlanState* planstate, StringInfo condition, int maxlen, List* rtable)
{
    Plan* plan = planstate->plan;
    /* process special cases such as IndexScan which has indexquals */
    switch (nodeTag(plan)) {
        case T_IndexScan:
            GetPlanOptConditionFromQual(((IndexScan*)plan)->indexqualorig, planstate, condition, maxlen, rtable);
            break;
        case T_IndexOnlyScan:
            GetPlanOptConditionFromQual(((IndexOnlyScan*)plan)->indexqual, planstate, condition, maxlen, rtable);
            break;
        case T_BitmapIndexScan:
            GetPlanOptConditionFromQual(((BitmapIndexScan*)plan)->indexqualorig, planstate, condition, maxlen, rtable);
            break;
        case T_CStoreIndexCtidScan:
            GetPlanOptConditionFromQual(
                ((CStoreIndexCtidScan*)plan)->indexqualorig, planstate, condition, maxlen, rtable);
            break;
        case T_CStoreIndexScan:
            GetPlanOptConditionFromQual(((CStoreIndexScan*)plan)->indexqualorig, planstate, condition, maxlen, rtable);
            break;
        case T_BitmapHeapScan:
        case T_CStoreIndexHeapScan:
            GetPlanOptConditionFromQual(((BitmapHeapScan*)plan)->bitmapqualorig, planstate, condition, maxlen, rtable);
            break;
        case T_VecNestLoop:
        case T_NestLoop:
            GetPlanOptConditionFromQual(((NestLoop*)plan)->join.joinqual, planstate, condition, maxlen, rtable);
            break;
        case T_VecMergeJoin:
        case T_MergeJoin:
            GetPlanOptConditionFromQual(((MergeJoin*)plan)->mergeclauses, planstate, condition, maxlen, rtable);
            break;
        case T_HashJoin:
        case T_VecHashJoin:
            GetPlanOptConditionFromQual(((HashJoin*)plan)->hashclauses, planstate, condition, maxlen, rtable);
            GetPlanOptConditionFromQual(((HashJoin*)plan)->join.joinqual, planstate, condition, maxlen, rtable);
            break;
        case T_BaseResult:
        case T_VecResult:
            GetPlanOptConditionFromQual(
                (List*)((BaseResult*)plan)->resconstantqual, planstate, condition, maxlen, rtable);
            break;
        default:
            break;
    }
}
/* *
 * @Description: Internel worker to call GetPlanOptConditionFromQual for different nodes
 * @in qual, planstate and rtable to extract information. maxlen is the maximum length that we want in qual_info
 * @out result will be stored in condition
 */
static void GetPlanOptCondition(PlanState* planstate, StringInfo condition, int maxlen, List* rtable)
{
    Plan* plan = planstate->plan;
    if (plan == NULL) {
        return;
    }

    GetSpecialPlanOptCondition(planstate, condition, maxlen, rtable);

    /* process more general cases that their quals are in plan->qual */
    switch (nodeTag(plan)) {
        case T_SeqScan:
        case T_CStoreScan:
        case T_ValuesScan:
        case T_CteScan:
        case T_WorkTableScan:
        case T_SubqueryScan:
        case T_VecSubqueryScan:
        case T_FunctionScan:
        case T_TidScan:
        case T_Group:
        case T_VecGroup:
        case T_VecAgg:
        case T_Agg:
        case T_IndexScan:
        case T_IndexOnlyScan:
        case T_CStoreIndexScan:
        case T_BitmapHeapScan:
        case T_CStoreIndexHeapScan:
        case T_VecNestLoop:
        case T_NestLoop:
        case T_VecMergeJoin:
        case T_MergeJoin:
        case T_HashJoin:
        case T_VecHashJoin:
        case T_BaseResult:
        case T_VecResult:
            GetPlanOptConditionFromQual(plan->qual, planstate, condition, maxlen, rtable);
            break;
        default:
            break;
    }
}

static inline void ExactPlanOptStmt(PlanState* resultPlan, PlannedStmt* pstmt, OperatorPlanInfo* optPlanInfo)
{
    /* extract projection */
    StringInfo projection = makeStringInfo();
    appendStringInfoChar(projection, ' ');
    int pjMaxLen = 3999;
    GetPlanOptProjection(resultPlan, projection, pjMaxLen, pstmt->rtable);

    /* extract conditions */
    StringInfo condition = makeStringInfo();
    appendStringInfoChar(condition, ' ');
    int cdMaxLen = 3999;
    GetPlanOptCondition(resultPlan, condition, cdMaxLen, pstmt->rtable);

    optPlanInfo->condition = condition->data;
    optPlanInfo->projection = projection->data;
}

/*
 * main entrance to extract Plan for each operator after the entire plan is finished.
 */
OperatorPlanInfo* ExtractOperatorPlanInfo(PlanState* resultPlan, PlannedStmt* pstmt)
{
    OperatorPlanInfo* optPlanInfo = (OperatorPlanInfo*)palloc0(sizeof(OperatorPlanInfo));

    /* extract operation name and related stuff */
    bool isRow = false;
    char* operation = NULL;
    char* orientation = NULL;
    char* strategy = NULL;
    char* options = NULL;
    Plan* node = resultPlan->plan;
    PHGetPlanNodeText(node, &operation, &isRow, &strategy, &options);
    if (isRow == false) {
        orientation = pstrdup("COL");
    } else {
        orientation = pstrdup("ROW");
    }

    ExactPlanOptStmt(resultPlan, pstmt, optPlanInfo);

    /* extract child id (0 if not exist) */
    int leftChildId = 0;
    if (node->lefttree != NULL) {
        leftChildId = node->lefttree->plan_node_id;
    }
    int rightChildId = 0;
    if (node->righttree != NULL) {
        rightChildId = node->righttree->plan_node_id;
    }
    int parentNodeId = 0;
    parentNodeId = node->parent_node_id;
    if (operation == NULL) {
        operation = pstrdup("???");
    }
    if (strategy == NULL) {
        strategy = pstrdup("???");
    }
    if (options == NULL) {
        options = pstrdup("???");
    }
    optPlanInfo->operation = operation;
    optPlanInfo->orientation = orientation;
    optPlanInfo->strategy = strategy;
    optPlanInfo->options = options;
    optPlanInfo->parent_node_id = parentNodeId;
    optPlanInfo->left_child_id = leftChildId;
    optPlanInfo->right_child_id = rightChildId;
    return optPlanInfo;
}
