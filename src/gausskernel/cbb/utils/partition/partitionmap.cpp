/*
 * Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 2021, openGauss Contributors
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
 * File Name	: partitionmap.cpp
 * Target		: data partition
 * Brief		:
 * Description	:
 * History	:
 *
 * IDENTIFICATION
 *	  src/gausskernel/cbb/utils/partition/partitionmap.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/heapam.h"
#include "access/tableam.h"
#include "catalog/pg_partition.h"
#include "catalog/pg_partition_fn.h"
#include "catalog/pg_type.h"
#include "datatype/timestamp.h"
#include "executor/executor.h"
#include "nodes/value.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "parser/parse_node.h"
#include "parser/parse_coerce.h"
#include "parser/parse_expr.h"
#include "utils/catcache.h"
#include "utils/syscache.h"
#include "utils/array.h"
#include "utils/numeric.h"
#include "utils/numeric_gs.h"
#include "utils/datetime.h"
#include "utils/int8.h"
#include "utils/lsyscache.h"
#include "utils/partcache.h"
#include "utils/partitionmap.h"
#include "utils/partitionmap_gs.h"
#include "utils/partitionkey.h"
#include "utils/date.h"
#include "utils/resowner.h"
#include "utils/rel.h"
#include "utils/rel_gs.h"
#include "fmgr.h"
#include "utils/memutils.h"
#include "utils/datum.h"
#include "utils/knl_relcache.h"

#define SAMESIGN(a, b) (((a) < 0) == ((b) < 0))
#define overFlowCheck(arg)                                                                \
    do {                                                                                  \
        if ((arg) > (MAX_PARTITION_NUM - 1)) {                                            \
            ereport(ERROR,                                                                \
                (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),                             \
                    errmsg("inserted partition key does not map to any table partition"), \
                    errdetail("the sequnece number is too large for range table")));      \
        }                                                                                 \
    } while (0)

#define SN(max, tpoint, scale, dist, mode, isclose, missIsOk, seqnum) \
    do {                                                              \
        (dist) = (max) - (tpoint);                                    \
        Assert(0 <= (dist) && 0 < (scale));                           \
        (mode) = (dist) % (scale);                                    \
        *(seqnum) = (dist) / (scale);                                 \
        if (!(mode) && !(isclose)) {                                  \
            *(seqnum) = *(seqnum) - 1;                                  \
        }                                                             \
        if (!(missIsOk)) {                                            \
            overFlowCheck(*(seqnum));                                 \
        }                                                             \
    } while (0)

#define Int2VectorSize(n) (offsetof(int2vector, values) + (n) * sizeof(int2))

#define constIsMaxValue(value) ((value)->ismaxvalue)

#define int_cmp_partition(arg1, arg2, compare) \
    do {                                       \
        if ((arg1) < (arg2)) {                 \
            (compare) = -1;                    \
        } else if ((arg1) > (arg2)) {          \
            (compare) = 1;                     \
        } else {                               \
            (compare) = 0;                     \
        }                                      \
    } while (0)

/*
 * 1. NAN = NAN
 * 2. NAN > non-NAN
 * 3. non-NAN < NAN
 * 4. non-NAN cmp non-NAN
 */
#define float_cmp_partition(arg1, arg2, compare) \
    do {                                         \
        if (isnan(arg1)) {                       \
            if (isnan(arg2)) {                   \
                (compare) = 0;                   \
            } else {                             \
                (compare) = 1;                   \
            }                                    \
        } else if (isnan(arg2)) {                \
            (compare) = -1;                      \
        } else {                                 \
            if ((arg1) > (arg2)) {               \
                (compare) = 1;                   \
            } else if ((arg1) < (arg2)) {        \
                (compare) = -1;                  \
            } else {                             \
                (compare) = 0;                   \
            }                                    \
        }                                        \
    } while (0)

#define numerics_cmp_partition(arg1, arg2, compare) \
    do {                                            \
        (compare) = cmp_numerics((arg1), (arg2));   \
    } while (0)

#define bpchar_cmp_partition(arg1, arg2, collid, compare)                                   \
    do {                                                                                    \
        int len1 = bcTruelen(arg1);                                                         \
        int len2 = bcTruelen(arg2);                                                         \
        (compare) = varstr_cmp(VARDATA_ANY(arg1), len1, VARDATA_ANY(arg2), len2, (collid)); \
    } while (0)

#define text_cmp_partition(arg1, arg2, collid, compare)         \
    do {                                                        \
        char* a1p = NULL;                                       \
        char* a2p = NULL;                                       \
        int len1;                                               \
        int len2;                                               \
        a1p = VARDATA_ANY(arg1);                                \
        a2p = VARDATA_ANY(arg2);                                \
        len1 = VARSIZE_ANY_EXHDR(arg1);                         \
        len2 = VARSIZE_ANY_EXHDR(arg2);                         \
        (compare) = varstr_cmp(a1p, len1, a2p, len2, (collid)); \
    } while (0)

#define date_cmp_partition(arg1, arg2, compare) \
    do {                                        \
        if ((arg1) < (arg2)) {                  \
            (compare) = -1;                     \
        } else if ((arg1) > (arg2)) {           \
            (compare) = 1;                      \
        } else {                                \
            (compare) = 0;                      \
        }                                       \
    } while (0)

#define timestamp_cmp_partition(arg1, arg2, compare)        \
    do {                                                    \
        (compare) = timestamp_cmp_internal((arg1), (arg2)); \
    } while (0)

#define name_cmp_partition(arg1, arg2, compare)                               \
    do {                                                                      \
        Assert(PointerIsValid(arg1));                                         \
        Assert(PointerIsValid(arg2));                                         \
        (compare) = strncmp(NameStr(*(arg1)), NameStr(*(arg2)), NAMEDATALEN); \
    } while (0)

#define interval_cmp_partition(arg1, arg2, compare)        \
    do {                                                   \
        Assert(PointerIsValid(arg1));                      \
        Assert(PointerIsValid(arg1));                      \
        (compare) = interval_cmp_internal((arg1), (arg2)); \
    } while (0)

#define constCompare_baseType(value1, value2, compare)                                                                 \
    do {                                                                                                               \
        switch ((value1)->consttype) {                                                                                 \
            case INT2OID:                                                                                              \
                Assert((value2)->consttype == INT2OID);                                                                \
                int_cmp_partition(                                                                                     \
                    DatumGetInt16((value1)->constvalue), DatumGetInt16((value2)->constvalue), (compare));              \
                break;                                                                                                 \
            case INT4OID:                                                                                              \
                Assert((value2)->consttype == INT4OID);                                                                \
                int_cmp_partition(                                                                                     \
                    DatumGetInt32((value1)->constvalue), DatumGetInt32((value2)->constvalue), (compare));              \
                break;                                                                                                 \
            case INT8OID:                                                                                              \
                Assert((value2)->consttype == INT8OID);                                                                \
                int_cmp_partition(                                                                                     \
                    DatumGetInt64((value1)->constvalue), DatumGetInt64((value2)->constvalue), (compare));              \
                break;                                                                                                 \
            case FLOAT4OID:                                                                                            \
                Assert((value2)->consttype == FLOAT4OID);                                                              \
                float_cmp_partition(                                                                                   \
                    DatumGetFloat4((value1)->constvalue), DatumGetFloat4((value2)->constvalue), (compare));            \
                break;                                                                                                 \
            case FLOAT8OID:                                                                                            \
                Assert((value2)->consttype == FLOAT8OID);                                                              \
                float_cmp_partition(                                                                                   \
                    DatumGetFloat8((value1)->constvalue), DatumGetFloat8((value2)->constvalue), (compare));            \
                break;                                                                                                 \
            case NUMERICOID:                                                                                           \
                Assert((value2)->consttype == NUMERICOID);                                                             \
                numerics_cmp_partition(                                                                                \
                    DatumGetNumeric((value1)->constvalue), DatumGetNumeric((value2)->constvalue), (compare));          \
                break;                                                                                                 \
            case BPCHAROID:                                                                                            \
                Assert((value2)->consttype == BPCHAROID);                                                              \
                Assert((value1)->constcollid == (value2)->constcollid);                                                \
                bpchar_cmp_partition(DatumGetBpCharP((value1)->constvalue),                                            \
                    DatumGetBpCharP((value2)->constvalue),                                                             \
                    (value1)->constcollid,                                                                             \
                    (compare));                                                                                        \
                break;                                                                                                 \
            case VARCHAROID:                                                                                           \
                Assert((value2)->consttype == VARCHAROID);                                                             \
                Assert((value1)->constcollid == (value2)->constcollid);                                                \
                text_cmp_partition(DatumGetTextP((value1)->constvalue),                                                \
                    DatumGetTextP((value2)->constvalue),                                                               \
                    (value1)->constcollid,                                                                             \
                    (compare));                                                                                        \
                break;                                                                                                 \
            case TEXTOID:                                                                                              \
                Assert((value2)->consttype == TEXTOID);                                                                \
                Assert((value1)->constcollid == (value2)->constcollid);                                                \
                text_cmp_partition(DatumGetTextP((value1)->constvalue),                                                \
                    DatumGetTextP((value2)->constvalue),                                                               \
                    (value1)->constcollid,                                                                             \
                    (compare));                                                                                        \
                break;                                                                                                 \
            case DATEOID:                                                                                              \
                Assert((value2)->consttype == DATEOID);                                                                \
                date_cmp_partition(                                                                                    \
                    DatumGetDateADT((value1)->constvalue), DatumGetDateADT((value2)->constvalue), (compare));          \
                break;                                                                                                 \
            case TIMESTAMPOID:                                                                                         \
                Assert((value2)->consttype == TIMESTAMPOID);                                                           \
                timestamp_cmp_partition(                                                                               \
                    DatumGetTimestamp((value1)->constvalue), DatumGetTimestamp((value2)->constvalue), (compare));      \
                break;                                                                                                 \
            case TIMESTAMPTZOID:                                                                                       \
                Assert((value2)->consttype == TIMESTAMPTZOID);                                                         \
                timestamp_cmp_partition(                                                                               \
                    DatumGetTimestampTz((value1)->constvalue), DatumGetTimestampTz((value2)->constvalue), (compare));  \
                break;                                                                                                 \
            case NAMEOID:                                                                                              \
                Assert((value2)->consttype == NAMEOID);                                                                \
                name_cmp_partition(DatumGetName((value1)->constvalue), DatumGetName((value2)->constvalue), (compare)); \
                break;                                                                                                 \
            case INTERVALOID:                                                                                          \
                Assert((value2)->consttype == INTERVALOID);                                                            \
                interval_cmp_partition(                                                                                \
                    DatumGetIntervalP((value1)->constvalue), DatumGetIntervalP((value2)->constvalue), (compare));      \
                break;                                                                                                 \
            default:                                                                                                   \
                (compare) = constCompare_constType((value1), (value2));                                                \
                break;                                                                                                 \
        }                                                                                                              \
    } while (0)
/*
 * @Description: partition value routing
 * @Param[IN] compare: returned value
 * @Param[IN] value1: left value to compare
 * @Param[OUT] value2: right value to compare
 * @See also:
 */
void constCompare(Const* value1, Const* value2, int& compare)
{
    if (t_thrd.utils_cxt.gValueCompareContext == NULL) {
        /*
         * In a query each tuple be updated delete or update, constcompare is called,
         * the memory allocated in function is not freed until query end, we allocate a
         * temp momeryContext to free the memory allocated in constcompare fucntion to
         * avoid take up too many memory in a query.
         * create this memory context under TOP memory context.
         */
        t_thrd.utils_cxt.gValueCompareContext = AllocSetContextCreate(t_thrd.top_mem_cxt,
            "CONST_COMPARE_CONTEXT",
            ALLOCSET_SMALL_MINSIZE,
            ALLOCSET_SMALL_INITSIZE,
            ALLOCSET_SMALL_MAXSIZE);
        t_thrd.utils_cxt.ContextUsedCount = 1;
    } else {
        t_thrd.utils_cxt.ContextUsedCount++;
    }

    MemoryContext oldContext = MemoryContextSwitchTo(t_thrd.utils_cxt.gValueCompareContext);

    PG_TRY();
    {
        if (value1->consttype == value2->consttype) {
            constCompare_baseType(value1, value2, compare);
        } else {
            compare = constCompare_constType(value1, value2);
        }
    }
    PG_CATCH();
    {
        t_thrd.utils_cxt.ContextUsedCount--;
        /* switch to previous memory context */
        (void)MemoryContextSwitchTo(oldContext);

        if (t_thrd.utils_cxt.ContextUsedCount == 0) {
            MemoryContextReset(t_thrd.utils_cxt.gValueCompareContext);
        }

        PG_RE_THROW();
    }
    PG_END_TRY();

    /* switch to previous memory context */
    (void)MemoryContextSwitchTo(oldContext);

    /* reset this memory context after each partition routing */
    if (t_thrd.utils_cxt.ContextUsedCount > 0) {
        t_thrd.utils_cxt.ContextUsedCount--;
    }

    if (t_thrd.utils_cxt.ContextUsedCount == 0) {
        MemoryContextReset(t_thrd.utils_cxt.gValueCompareContext);
    }
}

#define BuildRangeElement(range, type, typelen, relid, attrno, tuple, desc, isInter) \
    do {                                                                             \
        Assert(PointerIsValid(range));                                               \
        Assert(PointerIsValid(type) && PointerIsValid(attrno));                      \
        Assert(PointerIsValid(tuple) && PointerIsValid(desc));                       \
        Assert((attrno)->dim1 <= RANGE_PARTKEYMAXNUM);                               \
        Assert((attrno)->dim1 == (typelen));                                         \
        unserializePartitionStringAttribute((range)->boundary,                       \
            RANGE_PARTKEYMAXNUM,                                                     \
            (type),                                                                  \
            (typelen),                                                               \
            (relid),                                                                 \
            (attrno),                                                                \
            (tuple),                                                                 \
            Anum_pg_partition_boundaries,                                            \
            (desc));                                                                 \
        (range)->partitionOid = HeapTupleGetOid(tuple);                              \
        (range)->len = (typelen);                                                    \
        (range)->isInterval = (isInter);                                             \
    } while (0)

#define buildListElement(range, type, typelen, relid, attrno, tuple, desc)  \
    do {                                                                    \
        Assert(PointerIsValid(range));                                      \
        Assert(PointerIsValid(type) && PointerIsValid(attrno));             \
        Assert(PointerIsValid(tuple) && PointerIsValid(desc));              \
        Assert((attrno)->dim1 == (typelen));                                \
        unserializeListPartitionAttribute(&((range)->len),                  \
            &((range)->boundary),                                           \
            (type),                                                         \
            (typelen),                                                      \
            (relid),                                                        \
            (attrno),                                                       \
            (tuple),                                                        \
            Anum_pg_partition_boundaries,                                   \
            (desc));                                                        \
        (range)->partitionOid = HeapTupleGetOid(tuple);                     \
    } while (0)

#define buildHashElement(range, type, typelen, relid, attrno, tuple, desc)  \
    do {                                                                    \
        Assert(PointerIsValid(range));                                      \
        Assert(PointerIsValid(type) && PointerIsValid(attrno));             \
        Assert(PointerIsValid(tuple) && PointerIsValid(desc));              \
        Assert((attrno)->dim1 == (typelen));                                \
        unserializeHashPartitionAttribute((range)->boundary,              \
            RANGE_PARTKEYMAXNUM,                                            \
            (relid),                                                        \
            (attrno),                                                       \
            (tuple),                                                        \
            Anum_pg_partition_boundaries,                                   \
            (desc));                                                        \
        (range)->partitionOid = HeapTupleGetOid(tuple);                     \
    } while (0)

static void RebuildListPartitionMap(ListPartitionMap* oldMap, ListPartitionMap* newMap);
static void RebuildHashPartitionMap(HashPartitionMap* oldMap, HashPartitionMap* newMap);
/* these routines are partition map related */
static void buildRangePartitionMap(Relation relation, Form_pg_partition partitioned_form, HeapTuple partitioned_tuple,
    Relation pg_partition, const List* partition_list);
static void BuildHashPartitionMap(Relation relation, Form_pg_partition partitioned_form, HeapTuple partitioned_tuple,
    Relation pg_partition, List* partition_list);
static void BuildListPartitionMap(Relation relation, Form_pg_partition partitioned_form, HeapTuple partitioned_tuple,
    Relation pg_partition, List* partition_list);
ValuePartitionMap* buildValuePartitionMap(Relation relation, Relation pg_partition, HeapTuple partitioned_tuple);

/*
 * @@GaussDB@@
 * Brief		:
 * Description	: read boundaries, transitpoint or interval attribute value
 *                      from pg_partition for partition and transform it to maxvalue array
 * Notes		:
 */
void unserializePartitionStringAttribute(Const** outMaxValue, int outMaxValueLen, Oid* partKeyDataType,
    int partKeyDataTypeLen, Oid relid, int2vector* partKeyAttrNo, HeapTuple partition_tuple, int att_num,
    TupleDesc pg_partition_tupledsc)
{
    Datum attribute_raw_value;
    bool isNull = true;
    List* boundary = NULL;
    ListCell* cell = NULL;
    int counter = 0;

    Assert(partKeyDataTypeLen == partKeyAttrNo->dim1);

    attribute_raw_value = tableam_tops_tuple_getattr(partition_tuple, (uint32)att_num, pg_partition_tupledsc, &isNull);

    if (isNull) {
        ereport(ERROR,
            (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                errmsg("null maxvalue for tuple %u", HeapTupleGetOid(partition_tuple))));
    }

    /* unstransform string items to Value list */
    boundary = untransformPartitionBoundary(attribute_raw_value);

    Assert(boundary->length == partKeyAttrNo->dim1);
    Assert(boundary->length <= RANGE_PARTKEYMAXNUM);

    /* Now, for each max value item, call it's typin function, save it in datum */
    counter = 0;

    foreach (cell, boundary) {
        int16 typlen = 0;
        bool typbyval = false;
        char typalign;
        char typdelim;
        Oid typioparam = InvalidOid;
        Oid func = InvalidOid;
        Oid typid = InvalidOid;
        Oid typelem = InvalidOid;
        Oid typcollation = InvalidOid;
        int32 typmod = -1;
        Datum value;
        Value* max_value = NULL;

        max_value = (Value*)lfirst(cell);

        /* get the oid/mod/collation/ of column i */
        get_atttypetypmodcoll(relid, partKeyAttrNo->values[counter], &typid, &typmod, &typcollation);

        /* deal with null */
        if (!PointerIsValid(max_value->val.str)) {
            outMaxValue[counter++] = makeMaxConst(typid, typmod, typcollation);
            continue;
        }

        /*
         * for interval column in pg_partition, typmod above is not correct
         * have to get typmod from 'intervalvalue(typmod)'
         */
        if (INTERVALOID == partKeyDataType[counter]) {
            typmod = -1;
        }

        /* get the typein function's oid of current type */
        get_type_io_data(
            partKeyDataType[counter], IOFunc_input, &typlen, &typbyval, &typalign, &typdelim, &typioparam, &func);
        typelem = get_element_type(partKeyDataType[counter]);

        /* now call the typein function with collation,string, element_type, typemod
         * as it's parameters.
         */
        value = OidFunctionCall3Coll(
            func, typcollation, CStringGetDatum(max_value->val.str), ObjectIdGetDatum(typelem), Int32GetDatum(typmod));
        /* save the output values */
        outMaxValue[counter++] = makeConst(typid, typmod, typcollation, typlen, value, false, typbyval);
    }
    list_free_ext(boundary);
}

void unserializeListPartitionAttribute(int *len, Const*** listValues, Oid* partKeyDataType,
    int partKeyDataTypeLen, Oid relid, int2vector* partKeyAttrNo, HeapTuple partition_tuple, int att_num,
    TupleDesc pg_partition_tupledsc)
{
    Datum attribute_raw_value;
    bool isNull = true;
    List* boundary = NULL;
    ListCell* cell = NULL;
    int counter = 0;

    Assert(partKeyDataTypeLen == partKeyAttrNo->dim1);

    attribute_raw_value = heap_getattr(partition_tuple, (uint32)att_num, pg_partition_tupledsc, &isNull);

    if (isNull) {
        ereport(ERROR,
            (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                errmsg("null maxvalue for tuple %u", HeapTupleGetOid(partition_tuple))));
    }

    /* unstransform string items to Value list */
    boundary = untransformPartitionBoundary(attribute_raw_value);
    *len = list_length(boundary);
   
    *listValues = (Const**)palloc0(sizeof(Const*) * (*len));
    Const** values = *listValues;
     /* Now, for each max value item, call it's typin function, save it in datum */
    counter = 0;
    
    foreach (cell, boundary) {
        int16 typlen = 0;
        bool typbyval = false;
        char typalign;
        char typdelim;
        Oid typioparam = InvalidOid;
        Oid func = InvalidOid;
        Oid typid = InvalidOid;
        Oid typelem = InvalidOid;
        Oid typcollation = InvalidOid;
        int32 typmod = -1;
        Datum value;
        Value* list_value = NULL;
        int key_count = 0;

        list_value = (Value*)lfirst(cell);

        /* get the oid/mod/collation/ of column i */
        get_atttypetypmodcoll(relid, partKeyAttrNo->values[key_count], &typid, &typmod, &typcollation);

        /* deal with null */
        if (!PointerIsValid(list_value->val.str)) {
            values[counter++] = makeMaxConst(typid, typmod, typcollation);
            continue;
        }

        /*
         * for interval column in pg_partition, typmod above is not correct
         * have to get typmod from 'intervalvalue(typmod)'
         */
        if (INTERVALOID == partKeyDataType[key_count]) {
            typmod = -1;
        }

        /* get the typein function's oid of current type */
        get_type_io_data(
            partKeyDataType[key_count], IOFunc_input, &typlen, &typbyval, &typalign, &typdelim, &typioparam, &func);
        typelem = get_element_type(partKeyDataType[key_count]);

        /* now call the typein function with collation,string, element_type, typemod
         * as it's parameters.
         */
        value = OidFunctionCall3Coll(
            func, typcollation, CStringGetDatum(list_value->val.str), ObjectIdGetDatum(typelem), Int32GetDatum(typmod));
        /* save the output values */
        values[counter++] = makeConst(typid, typmod, typcollation, typlen, value, false, typbyval);
    }
    list_free_ext(boundary);
}

void unserializeHashPartitionAttribute(Const** hashBuckets, int outMaxValueLen, 
    Oid relid, int2vector* partKeyAttrNo, HeapTuple partition_tuple, int att_num,
    TupleDesc pg_partition_tupledsc)
{
    Datum attribute_raw_value;
    bool isNull = true;
    List* boundary = NULL;
    ListCell* cell = NULL;
    int counter = 0;

    /* this function is used  to retrive maxvalue list in these 3 fields */
    Assert(att_num == Anum_pg_partition_boundaries || att_num == Anum_pg_partition_transit ||
           att_num == Anum_pg_partition_interval);

    attribute_raw_value = heap_getattr(partition_tuple, (uint32)att_num, pg_partition_tupledsc, &isNull);

    if (isNull) {
        ereport(ERROR,
            (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                errmsg("null maxvalue for tuple %u", HeapTupleGetOid(partition_tuple))));
    }

    /* unstransform string items to Value list */
    boundary = untransformPartitionBoundary(attribute_raw_value);

    Assert(boundary->length == partKeyAttrNo->dim1);

    /* Now, for each max value item, call it's typin function, save it in datum */
    counter = 0;

    Oid hashValueTypeId = INT4OID;

    foreach (cell, boundary) {
        int16 typlen = 0;
        bool typbyval = false;
        char typalign;
        char typdelim;
        Oid typioparam = InvalidOid;
        Oid func = InvalidOid;
        Oid typid = InvalidOid;
        Oid typelem = InvalidOid;
        Oid typcollation = InvalidOid;
        int32 typmod = -1;
        Datum value;
        Value* hash_value = NULL;

        hash_value = (Value*)lfirst(cell);

        /* get the oid/mod/collation/ of column i */
        get_atttypetypmodcoll(relid, partKeyAttrNo->values[counter], &typid, &typmod, &typcollation);


        /* get the typein function's oid of current type */
        get_type_io_data(
            hashValueTypeId, IOFunc_input, &typlen, &typbyval, &typalign, &typdelim, &typioparam, &func);
        typelem = get_element_type(hashValueTypeId);

        value = OidFunctionCall3Coll(
            func, typcollation, CStringGetDatum(hash_value->val.str), ObjectIdGetDatum(typelem), Int32GetDatum(typmod));
        /* save the output values */
        hashBuckets[counter++] = makeConst(hashValueTypeId, -1, InvalidOid, sizeof(int32), value, false, true);
    }
    list_free_ext(boundary);
}

/*
 * getPartitionKeyAttrNo(), get out typid and attribute number of partition key
 * parameters: [OUT] outOid and return value
 *
 * @@GaussDB@@
 * Brief		:
 * Description	: get attribute number and data type oid of partition key from
 *                      partitioned-table tuple.
 * Notes		:
 * Parameters  :  [out]typeOids: data type array of partition key
 *                       [in] pg_partitioin_tuple: partitioned-table tuple in pg_partition
 *			    [in] pg_partition_tupledsc: TupleDesc of pg_partition
 *                       [in] base_table_tupledsc:  TupleDesc of partitioned-table
 *			    return : attribute number array of partition key
 */
int2vector* getPartitionKeyAttrNo(
    Oid** typeOids, HeapTuple pg_partition_tuple, TupleDesc pg_partition_tupledsc, TupleDesc base_table_tupledsc)
{
    Datum partkey_raw;
    ArrayType* partkey_columns = NULL;
    bool isNull = false;
    int16* attnums = NULL;
    int n_key_column, i, j;
    int2vector* partkey = NULL;
    Oid* oidArr = NULL;
    Form_pg_attribute* rel_attrs = base_table_tupledsc->attrs;

    Assert(PointerIsValid(typeOids));

    /* Get the raw data which contain patition key's columns */
    partkey_raw = tableam_tops_tuple_getattr(pg_partition_tuple, Anum_pg_partition_partkey, pg_partition_tupledsc, &isNull);

    /* if the raw value of partition key is null, then report error */
    if (isNull) {
        ereport(ERROR,
            (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                errmsg("null maxvalue for tuple %u", HeapTupleGetOid(pg_partition_tuple))));
    }

    /*  convert Datum to ArrayType */
    partkey_columns = DatumGetArrayTypeP(partkey_raw);

    /* Get number of partition key columns from int2verctor */
    n_key_column = ARR_DIMS(partkey_columns)[0];

    /* CHECK: the ArrayType of partition key is valid */
    if (ARR_NDIM(partkey_columns) != 1 || n_key_column < 0 || ARR_HASNULL(partkey_columns) ||
        ARR_ELEMTYPE(partkey_columns) != INT2OID) {
        ereport(ERROR,
            (errcode(ERRCODE_ARRAY_ELEMENT_ERROR),
                errmsg("partition key column's number is not a 1-D smallint array")));
    }

    /* Get int2 array of partition key column numbers */
    attnums = (int16*)ARR_DATA_PTR(partkey_columns);

    Assert(n_key_column <= RANGE_PARTKEYMAXNUM);

    /* Initialize int2verctor structure for attribute number array of partition key */
    partkey = buildint2vector(NULL, n_key_column);
    oidArr = (Oid*)palloc0(sizeof(Oid) * n_key_column);

    /* specify value to int2verctor and build type oid array */
    for (i = 0; i < n_key_column; i++) {
        int16 attnum = attnums[i];
        partkey->values[i] = attnum;
        for (j = 0; j < base_table_tupledsc->natts; j++) {
            if (attnum == rel_attrs[j]->attnum) {
                oidArr[i] = rel_attrs[j]->atttypid;
                break;
            }
        }
    }
    *typeOids = oidArr;
    return partkey;
}

char GetSubPartitionStrategy(List* partition_list, Form_pg_partition partitioned_form, bool isSubPartition)
{
    char partstrategy;
    if (isSubPartition) {
        HeapTuple subPartitionTuple = (HeapTuple)list_nth(partition_list, 0);
        Form_pg_partition subPartitionForm = (Form_pg_partition)GETSTRUCT(subPartitionTuple);
        partstrategy = subPartitionForm->partstrategy;
    } else {
        partstrategy = partitioned_form->partstrategy;
    }
    return partstrategy;
}

/*
 * @@GaussDB@@
 * Brief		:
 * Description	: When load RelationData to relcache, intalize PartitionMap which
 *			: stores partition boundary.
 * Notes		: We must note than the data of the catalog may change between
 *			: two access. So it is reasonable if
 *			: partitioned_form->intervalnum + partitioned_form->rangenum !=  partition_list->length
 */
void RelationInitPartitionMap(Relation relation, bool isSubPartition)
{
    List* partition_list = NIL;
    Relation pg_partition = NULL;
    HeapTuple partitioned_tuple = NULL;
    Form_pg_partition partitioned_form = NULL;
    MemoryContext old_context;
    MemoryContext tmp_context;

    Assert(PointerIsValid(relation));

    /*
     * Unsupport to bulid partitionmap for non-partitioned table.
     * Never happen, just to be self-contained.
     */
    if (RelationIsNonpartitioned(relation)) {
        ereport(ERROR,
            (errcode(ERRCODE_PARTITION_ERROR),
                errmsg("Fail to build partitionmap for realtion\"%s\".", RelationGetRelationName(relation)),
                errdetail("Relation is a non-partitioned table.")));
    }

    /* create a tmp memorycontext */
    tmp_context = AllocSetContextCreate(t_thrd.top_mem_cxt,
        "InitPartitionMapTmpMemoryContext",
        ALLOCSET_SMALL_MINSIZE,
        ALLOCSET_SMALL_INITSIZE,
        ALLOCSET_SMALL_MAXSIZE);

    /*
     * switch memeorycontext to relcache's memeorycontext
     */
    old_context = MemoryContextSwitchTo(tmp_context);

    pg_partition = relation_open(PartitionRelationId, AccessShareLock);
    if (isSubPartition) {
        partitioned_tuple = SearchSysCache1(PARTRELID, ObjectIdGetDatum(relation->rd_id));
    } else {
        partitioned_tuple = searchPgPartitionByParentIdCopy(PART_OBJ_TYPE_PARTED_TABLE, relation->rd_id);
    }
    
    if (!HeapTupleIsValid(partitioned_tuple)) {
        if (RecoveryInProgress()) {
            ereport(ERROR,
                (errcode(ERRCODE_RUN_TRANSACTION_DURING_RECOVERY),
                    errmsg("Can not run transaction to remote nodes during recovery.")));
        }
        Assert(0);
        ereport(ERROR,
            (errcode(ERRCODE_UNDEFINED_OBJECT),
                errmsg("could not find tuple with partition OID %u.", relation->rd_id)));
    }
    partitioned_form = (Form_pg_partition)GETSTRUCT(partitioned_tuple);
    /*
     * For value based partition-table, we only have to retrieve partkeys
     */
    if (partitioned_form->partstrategy == PART_STRATEGY_VALUE) {
        /* create ValuePartitionMap */
        (void)MemoryContextSwitchTo(LocalMyDBCacheMemCxt());

        relation->partMap = (PartitionMap*)buildValuePartitionMap(relation, pg_partition, partitioned_tuple);

        heap_freetuple_ext(partitioned_tuple);
        freePartList(partition_list);

        /* close pg_partition */
        relation_close(pg_partition, AccessShareLock);

        (void)MemoryContextSwitchTo(old_context);
        MemoryContextDelete(tmp_context);

        return;
    }

    /*
     * Fail to get relation tuple for the partitioned table
     * Never happen, just to be self-contained
     */
    if (!PointerIsValid(partitioned_tuple)) {
        relation_close(pg_partition, AccessShareLock);
        (void)MemoryContextSwitchTo(old_context);
        MemoryContextDelete(tmp_context);
        ereport(ERROR,
            (errcode(ERRCODE_PARTITION_ERROR),
                errmsg("Fail to build partitionmap for partitioned table \"%s\"", RelationGetRelationName(relation)),
                errdetail("Could not find the partitioned table")));
    }

    /* read out patition tuples from pg_partition */
    if (isSubPartition) {
        partition_list = searchPgPartitionByParentId(PART_OBJ_TYPE_TABLE_SUB_PARTITION, relation->rd_id);
    } else {
        partition_list = searchPgPartitionByParentId(PART_OBJ_TYPE_TABLE_PARTITION, relation->rd_id);
    }

    /*
     * Fail to get relation tuple for the partitioned table
     * Never happen, just to be self-contained
     */
    if (!PointerIsValid(partition_list)) {
        if (isSubPartition)
            ReleaseSysCache(partitioned_tuple);
        else
            heap_freetuple_ext(partitioned_tuple);
        relation_close(pg_partition, AccessShareLock);

        (void)MemoryContextSwitchTo(old_context);
        MemoryContextDelete(tmp_context);

        ereport(ERROR,
            (errcode(ERRCODE_PARTITION_ERROR),
                errmsg("Fail to build partitionmap for partitioned table \"%s\".", RelationGetRelationName(relation)),
                errdetail("Could not find partition for the partitioned table.")));
    }

    char partstrategy = GetSubPartitionStrategy(partition_list, partitioned_form, isSubPartition);

    switch (partstrategy) {
        case PART_STRATEGY_RANGE:
        case PART_STRATEGY_INTERVAL:
            buildRangePartitionMap(relation, partitioned_form, partitioned_tuple, pg_partition, partition_list);
            break;
        case PART_STRATEGY_LIST:
            BuildListPartitionMap(relation, partitioned_form, partitioned_tuple, pg_partition, partition_list);
            break;
        case PART_STRATEGY_HASH:
            BuildHashPartitionMap(relation, partitioned_form, partitioned_tuple, pg_partition, partition_list);
            break;
        default:
            (void)MemoryContextSwitchTo(old_context);
            MemoryContextDelete(tmp_context);
            ereport(ERROR,
                (errcode(ERRCODE_PARTITION_ERROR),
                    errmsg("Fail to build partitionmap for partitioned table \"%u\".", partitioned_form->parentid),
                    errdetail(
                        "Incorrect partition strategy \"%c\" for partitioned table.", partitioned_form->partstrategy)));
            break;
    }

    relation->partMap->refcount = 0;
    relation->partMap->isDirty = false;

    /* release the partition_list and partitioined_table_tuple */
    if (isSubPartition)
        ReleaseSysCache(partitioned_tuple);
    else
        heap_freetuple_ext(partitioned_tuple);
    freePartList(partition_list);

    /* close pg_partition */
    relation_close(pg_partition, AccessShareLock);

    (void)MemoryContextSwitchTo(old_context);
    MemoryContextDelete(tmp_context);
}

/*
 * Brief: build PartitionMap data structure for value partition table
 * Input:
 *   @relation: Relation descriptor for value partitioned table
 *   @pg_relation: Relation descriptor for pg_partition
 *   @partitioned_tuple: the primary tuple of pg_partition that describe
 *                       how this relation is (value) partitioned
 * Return Value : ValuePartitoinMap pointer for given relation.
 * Notes        : None.
 */
ValuePartitionMap* buildValuePartitionMap(Relation relation, Relation pg_partition, HeapTuple partitioned_tuple)
{
    Oid* partitionKeyDataType = NULL;
    int2vector* partitionKey = NULL;
    ValuePartitionMap* vpm = NULL;

    /* create ValuePartitionMap */
    vpm = (ValuePartitionMap*)palloc0(sizeof(ValuePartitionMap));
    vpm->type.type = PART_TYPE_VALUE;
    vpm->relid = RelationGetRelid(relation);

    /* get attno. which is a member of partitionkey */
    partitionKey = getPartitionKeyAttrNo(
        &(partitionKeyDataType), partitioned_tuple, RelationGetDescr(pg_partition), RelationGetDescr(relation));

    /* build partList */
    for (int i = 0; i < partitionKey->dim1; i++) {
        vpm->partList = lappend_int(vpm->partList, partitionKey->values[i]);
    }

    return vpm;
}

void RebuildPartitonMap(PartitionMap* oldMap, PartitionMap* newMap)
{
    if (oldMap->type != newMap->type) {
        ereport(ERROR,
            (errcode(ERRCODE_PARTITION_ERROR),
                errmsg("rebuild partition map ERROR"),
                errdetail("NEW partitioned table MUST have same partition strategy as OLD partitioned table")));
    }

    // when the map is referenced, don't rebuild the partitionmap
    if (oldMap->refcount == 0) {
        if (PartitionMapIsList(oldMap)) {
            RebuildListPartitionMap((ListPartitionMap*)oldMap, (ListPartitionMap*)newMap);
        } else if (PartitionMapIsHash(oldMap)) {
            RebuildHashPartitionMap((HashPartitionMap*)oldMap, (HashPartitionMap*)newMap);
        } else {
            RebuildRangePartitionMap((RangePartitionMap*)oldMap, (RangePartitionMap*)newMap);
        }
    } else {
        oldMap->isDirty = true;
        SetRelCacheNeedEOXActWork(true);
        elog(LOG, "map refcount is not zero when RebuildPartitonMap ");
    }
}

#define PARTITIONMAP_SWAPFIELD(fieldType, fieldName) \
    do {                                             \
        fieldType _temp = oldMap->fieldName;         \
        oldMap->fieldName = newMap->fieldName;       \
        newMap->fieldName = _temp;                   \
    } while (0);

void RebuildRangePartitionMap(RangePartitionMap* oldMap, RangePartitionMap* newMap)
{
    RangePartitionMap tempMap;
    errno_t rc = 0;

    rc = memcpy_s(&tempMap, sizeof(RangePartitionMap), newMap, sizeof(RangePartitionMap));
    securec_check(rc, "\0", "\0");
    rc = memcpy_s(newMap, sizeof(RangePartitionMap), oldMap, sizeof(RangePartitionMap));
    securec_check(rc, "\0", "\0");
    rc = memcpy_s(oldMap, sizeof(RangePartitionMap), &tempMap, sizeof(RangePartitionMap));
    securec_check(rc, "\0", "\0");

    PARTITIONMAP_SWAPFIELD(int2vector*, partitionKey);
    PARTITIONMAP_SWAPFIELD(Oid*, partitionKeyDataType);
}

static void RebuildHashPartitionMap(HashPartitionMap* oldMap, HashPartitionMap* newMap)
{
    HashPartitionMap tempMap;
    errno_t rc = 0;

    rc = memcpy_s(&tempMap, sizeof(HashPartitionMap), newMap, sizeof(HashPartitionMap));
    securec_check(rc, "\0", "\0");
    rc = memcpy_s(newMap, sizeof(HashPartitionMap), oldMap, sizeof(HashPartitionMap));
    securec_check(rc, "\0", "\0");
    rc = memcpy_s(oldMap, sizeof(HashPartitionMap), &tempMap, sizeof(HashPartitionMap));
    securec_check(rc, "\0", "\0");

    PARTITIONMAP_SWAPFIELD(int2vector*, partitionKey);
    PARTITIONMAP_SWAPFIELD(Oid*, partitionKeyDataType);
}

static void RebuildListPartitionMap(ListPartitionMap* oldMap, ListPartitionMap* newMap)
{
    ListPartitionMap tempMap;
    errno_t rc = 0;

    rc = memcpy_s(&tempMap, sizeof(ListPartitionMap), newMap, sizeof(ListPartitionMap));
    securec_check(rc, "\0", "\0");
    rc = memcpy_s(newMap, sizeof(ListPartitionMap), oldMap, sizeof(ListPartitionMap));
    securec_check(rc, "\0", "\0");
    rc = memcpy_s(oldMap, sizeof(ListPartitionMap), &tempMap, sizeof(ListPartitionMap));
    securec_check(rc, "\0", "\0");

    PARTITIONMAP_SWAPFIELD(int2vector*, partitionKey);
    PARTITIONMAP_SWAPFIELD(Oid*, partitionKeyDataType);
}

ListPartElement* CopyListElements(ListPartElement* src, int elementNum)
{
    int i = 0;
    int j = 0;
    Size sizeRet = sizeof(ListPartElement) * elementNum;
    ListPartElement* ret = NULL;
    errno_t rc = 0;

    ret = (ListPartElement*)palloc0(sizeRet);
    rc = memcpy_s(ret, sizeRet, src, sizeRet);
    securec_check(rc, "\0", "\0");

    for (i = 0; i < elementNum; i++) {
        ret[i].boundary = (Const**)palloc0(sizeof(Const*)*src[i].len); 
        for (j = 0; j < src[i].len; j++) {
            ret[i].boundary[j] = (Const*)copyObject(src[i].boundary[j]);
        }
    }

    return ret;
}

void DestroyListElements(ListPartElement* src, int elementNum)
{
    int i = 0;
    int j = 0;
    Const* value = NULL;
    ListPartElement* part = NULL;

    for (i = 0; i < elementNum; i++) {
        part = &(src[i]);
        for (j = 0; j < part->len; j++) {
            value = part->boundary[j];
            if (PointerIsValid(value)) {
                if (!value->constbyval && !value->constisnull &&
                    PointerIsValid(DatumGetPointer(value->constvalue))) {
                    pfree(DatumGetPointer(value->constvalue));
                }

                pfree_ext(value);
                value = NULL;
            }
        }
        pfree_ext(part->boundary);
    }
    pfree_ext(src);
}

/*
 * @@GaussDB@@
 * Target		: data partition
 * Brief		:
 * Description	        :
 * Notes		:
 */
void partitionMapDestroyRangeArray(RangeElement* rangeArray, int arrLen)
{
    int i, j;
    RangeElement* range = NULL;
    Const* maxConst = NULL;

    if (rangeArray == NULL || arrLen < 1) {
        return;
    }

    /* before free range array, free max array in each rangeElement */
    for (i = 0; i < arrLen; i++) {
        range = &(rangeArray[i]);
        for (j = 0; j < range->len; j++) {
            maxConst = range->boundary[j];
            if (PointerIsValid(maxConst)) {
                if (!maxConst->constbyval && !maxConst->constisnull &&
                    PointerIsValid(DatumGetPointer(maxConst->constvalue))) {
                    pfree(DatumGetPointer(maxConst->constvalue));
                }

                pfree_ext(maxConst);
                maxConst = NULL;
            }
        }
    }

    /* free range array */
    pfree_ext(rangeArray);
}

void PartitionMapDestroyHashArray(HashPartElement* hashArray, int arrLen)
{
    int i;
    HashPartElement* hashValues = NULL;
    Const* value = NULL;

    if (hashArray == NULL || arrLen < 1) {
        return;
    }

    /* before free hash array, free max array in each hashElement */
    for (i = 0; i < arrLen; i++) {
        hashValues = &(hashArray[i]);

        value = hashValues->boundary[0];
        if (PointerIsValid(value)) {
            if (!value->constbyval && !value->constisnull &&
                PointerIsValid(DatumGetPointer(value->constvalue))) {
                pfree(DatumGetPointer(value->constvalue));
            }

            pfree_ext(value);
            value = NULL;
        } 
    }
    pfree_ext(hashArray);
}

void RelationDestroyPartitionMap(PartitionMap* partMap)
{
    /* already a non-partitioned relation, just return */
    if (!partMap)
        return;

    /* partitioned relation, destroy the partition map */
    if (partMap->type == PART_TYPE_RANGE || partMap->type == PART_TYPE_INTERVAL) {
        RangePartitionMap* range_map = ((RangePartitionMap*)(partMap));

        /* first free partKeyNum/partitionKeyDataType/ranges in the range map */
        if (range_map->partitionKey) {
            pfree_ext(range_map->partitionKey);
        }
        if (range_map->partitionKeyDataType) {
            pfree_ext(range_map->partitionKeyDataType);
        }
        if (range_map->intervalValue) {
            pfree_ext(range_map->intervalValue);
        }
        if (range_map->intervalTablespace) {
            pfree_ext(range_map->intervalTablespace);
        }
        if (range_map->rangeElements) {
            partitionMapDestroyRangeArray(range_map->rangeElements, range_map->rangeElementsNum);
        }
    }  else if (partMap->type == PART_TYPE_LIST) {
        ListPartitionMap* list_map = (ListPartitionMap*)(partMap);
        if (list_map->partitionKey) {
            pfree_ext(list_map->partitionKey);
            list_map->partitionKey = NULL;
        }
        if (list_map->partitionKeyDataType) {
            pfree_ext(list_map->partitionKeyDataType);
            list_map->partitionKeyDataType = NULL;
        }
        if (list_map->listElements) {
            DestroyListElements(list_map->listElements, list_map->listElementsNum);
            list_map->listElements = NULL;
        }
    } else if (partMap->type == PART_TYPE_HASH) {
        HashPartitionMap* hash_map = (HashPartitionMap*)(partMap);
        if (hash_map->partitionKey) {
            pfree_ext(hash_map->partitionKey);
            hash_map->partitionKey = NULL;
        }
        if (hash_map->partitionKeyDataType) {
            pfree_ext(hash_map->partitionKeyDataType);
            hash_map->partitionKeyDataType = NULL;
        }
        if (hash_map->hashElements) {
            PartitionMapDestroyHashArray(hash_map->hashElements, hash_map->hashElementsNum);
            hash_map->hashElements = NULL;
        }
    }
    pfree_ext(partMap);
    return;
}

HashPartElement* CopyHashElements(HashPartElement* src, int elementNum, int partkeyNum)
{
    int i = 0;
    int j = 0;
    Size sizeRet = sizeof(HashPartElement) * elementNum;
    HashPartElement* ret = NULL;
    errno_t rc = 0;

    ret = (HashPartElement*)palloc0(sizeRet);
    rc = memcpy_s(ret, sizeRet, src, sizeRet);
    securec_check(rc, "\0", "\0");

    for (i = 0; i < elementNum; i++) {
        for (j = 0; j < partkeyNum; j++) {
            ret[i].boundary[j] = (Const*)copyObject(src[i].boundary[j]);
        }
    }

    return ret;
}


/*
 * copy the rangeElement
 */
RangeElement* copyRangeElements(RangeElement* src, int elementNum, int partkeyNum)
{
    int i = 0;
    int j = 0;
    Size size_ret = sizeof(RangeElement) * elementNum;
    RangeElement* ret = NULL;
    errno_t rc = 0;

    ret = (RangeElement*)palloc0(size_ret);
    rc = memcpy_s(ret, size_ret, src, size_ret);
    securec_check(rc, "\0", "\0");

    for (i = 0; i < elementNum; i++) {
        for (j = 0; j < partkeyNum; j++) {
            ret[i].boundary[j] = (Const*)copyObject(src[i].boundary[j]);
        }
    }

    return ret;
}

RangeElement* CopyRangeElementsWithoutBoundary(const RangeElement* src, int elementNum)
{
    Size size_ret = sizeof(RangeElement) * elementNum;
    RangeElement* ret = (RangeElement*)palloc0(size_ret);
    errno_t rc = memcpy_s(ret, size_ret, src, size_ret);
    securec_check(rc, "\0", "\0");
    return ret;
}

char* ReadIntervalStr(HeapTuple tuple, TupleDesc tupleDesc)
{
    bool isNull = true;
    Oid elemType;
    int16 elemLen;
    bool elemByval = false;
    char elemAlign;
    int numElems;
    Datum* elemValues = NULL;
    bool* elemNulls = NULL;
    Datum attrRawValue = heap_getattr(tuple, (uint32)Anum_pg_partition_interval, tupleDesc, &isNull);
    ArrayType* array = DatumGetArrayTypeP(attrRawValue);

    elemType = ARR_ELEMTYPE(array);
    Assert(elemType == TEXTOID);
    get_typlenbyvalalign(elemType, &elemLen, &elemByval, &elemAlign);
    deconstruct_array(array, elemType, elemLen, elemByval, elemAlign, &elemValues, &elemNulls, &numElems);
    Assert(numElems == 1);
    Assert(!elemNulls[0]);
    char* intervalStr = text_to_cstring(DatumGetTextP(*elemValues));
    pfree(elemValues);
    pfree(elemNulls);
    return intervalStr;
}

static Interval* ReadInterval(HeapTuple tuple, TupleDesc tupleDesc)
{
    int32 typmod = -1;
    char* intervalStr = ReadIntervalStr(tuple, tupleDesc);
    Interval* res = char_to_interval(intervalStr, typmod);
    pfree(intervalStr);
    return res;
}

oidvector* ReadIntervalTablespace(HeapTuple tuple, TupleDesc tupleDesc)
{
    Datum tablespaceRaw;
    ArrayType* tablespaceArray = NULL;
    bool isNull = false;
    Oid* values = NULL;
    int arraySize;

    /* Get the raw data which contain interval tablespace's columns */
    tablespaceRaw = heap_getattr(tuple, Anum_pg_partition_intablespace, tupleDesc, &isNull);

    if (isNull) {
        return NULL;
    }

    /*  convert Datum to ArrayType */
    tablespaceArray = DatumGetArrayTypeP(tablespaceRaw);
    arraySize = ARR_DIMS(tablespaceArray)[0];

    /* CHECK: the ArrayType of interval tablespace is valid */
    if (ARR_NDIM(tablespaceArray) != 1 || arraySize <= 0 || ARR_HASNULL(tablespaceArray) ||
        ARR_ELEMTYPE(tablespaceArray) != OIDOID) {
        ereport(ERROR,
            (errcode(ERRCODE_ARRAY_ELEMENT_ERROR), errmsg("interval tablespace column's number is not a oid array")));
    }

    values = (Oid*)ARR_DATA_PTR(tablespaceArray);
    return buildoidvector(values, arraySize);
}

Oid GetRootPartitionOid(Relation relation)
{
    Oid relid = RelationGetRelid(relation);
    if (relid != relation->parentId && OidIsValid(relation->parentId)) {
        relid = relation->parentId;
    }
    return relid;
}

static void BuildListPartitionMap(Relation relation, Form_pg_partition partitioned_form, HeapTuple partitioned_tuple,
    Relation pg_partition, List* partition_list)
{
    int list_itr = 0;
    ListPartitionMap* list_map = NULL;
    ListPartElement* list_eles = NULL;
    Form_pg_partition partition_form = NULL;
    HeapTuple partition_tuple = NULL;
    ListCell* tuple_cell = NULL;

    int2vector* partitionKey = NULL;
    MemoryContext old_context = NULL;
    Oid* partitionKeyDataType = NULL;
    errno_t rc = 0;

    /* build ListPartitionMap */
    list_map = (ListPartitionMap*)palloc0(sizeof(ListPartitionMap));
    list_map->type.type = PART_TYPE_LIST;
    list_map->relid = RelationGetRelid(relation);
    list_map->listElementsNum = partition_list->length;

    /* get attribute NO. which is a member of partitionkey */
    partitionKey = getPartitionKeyAttrNo(
        &(partitionKeyDataType), partitioned_tuple, RelationGetDescr(pg_partition), RelationGetDescr(relation));
    /* copy the partitionKey */
    old_context = MemoryContextSwitchTo(LocalMyDBCacheMemCxt());

    list_map->partitionKey = (int2vector*)palloc(Int2VectorSize(partitionKey->dim1));
    rc = memcpy_s(
        list_map->partitionKey, Int2VectorSize(partitionKey->dim1), partitionKey, Int2VectorSize(partitionKey->dim1));
    securec_check(rc, "\0", "\0");
    list_map->partitionKeyDataType = (Oid*)palloc(sizeof(Oid) * partitionKey->dim1);
    rc = memcpy_s(list_map->partitionKeyDataType,
        sizeof(Oid) * partitionKey->dim1,
        partitionKeyDataType,
        sizeof(Oid) * partitionKey->dim1);
    securec_check(rc, "\0", "\0");
    (void)MemoryContextSwitchTo(old_context);

    /* allocate range element array */
    list_eles = (ListPartElement*)palloc0(sizeof(ListPartElement) * (list_map->listElementsNum));

    /* we will use reloid to get the column information from pg_attribute.
     * Only rootPartitionOid is in pg_attribute, so we can only use it.
     */
    Oid rootPartitionOid = GetRootPartitionOid(relation);

    /* iterate partition tuples, build RangeElement for per partition tuple */
    list_itr = 0;
    foreach (tuple_cell, partition_list) {
        partition_tuple = (HeapTuple)lfirst(tuple_cell);
        partition_form = (Form_pg_partition)GETSTRUCT(partition_tuple);

        if (PART_STRATEGY_LIST != partition_form->partstrategy) {
            pfree_ext(list_eles);
            pfree_ext(list_map);

            ereport(ERROR,
                (errcode(ERRCODE_PARTITION_ERROR),
                    errmsg("Fail to build partitionmap for partitioned table \"%u\"", partition_form->parentid),
                    errdetail("Incorrect partition strategy for partition %u", HeapTupleGetOid(partition_tuple))));
        }

        buildListElement(&(list_eles[list_itr]),
            list_map->partitionKeyDataType,
            list_map->partitionKey->dim1,
            rootPartitionOid,
            list_map->partitionKey,
            partition_tuple,
            RelationGetDescr(pg_partition));

        list_itr++;
    }

    /* list element array back in RangePartitionMap */
    old_context = MemoryContextSwitchTo(LocalMyDBCacheMemCxt());

    list_map->listElements = CopyListElements(list_eles, list_map->listElementsNum);
    relation->partMap = (PartitionMap*)palloc(sizeof(ListPartitionMap));
    rc = memcpy_s(relation->partMap, sizeof(ListPartitionMap), list_map, sizeof(ListPartitionMap));
    securec_check(rc, "\0", "\0");

    (void)MemoryContextSwitchTo(old_context);
    DestroyListElements(list_eles, list_map->listElementsNum);
}

bool CheckHashPartitionMap(HashPartElement* hash_eles, int len)
{
    int actualPartitionNum = 0;
    for (int i = 0; i < len; i++) {
        if (DatumGetInt32(hash_eles[i].boundary[0]->constvalue) > actualPartitionNum) {
            actualPartitionNum = DatumGetInt32(hash_eles[i].boundary[0]->constvalue);
        }
    }
    /* During DDL, like exchange partition, a temporary partition is added. 
     * In this case, we do not check because the actual number of partitions may be larger than the original. 
     */
    if (actualPartitionNum != len - 1) {
        return true;
    }
    // Constvalue must be in reverse order due to design issues.
    for (int i = 0; i < len; i++) {
        if (DatumGetInt32(hash_eles[len - 1 - i].boundary[0]->constvalue) != i) {
            return false;
        }
    }
    return true;
}

static void BuildHashPartitionMap(Relation relation, Form_pg_partition partitioned_form, HeapTuple partitioned_tuple,
    Relation pg_partition, List* partition_list)
{
    int hash_itr = 0;
    HashPartitionMap* hash_map = NULL;
    HashPartElement* hash_eles = NULL;
    Form_pg_partition partition_form = NULL;
    HeapTuple partition_tuple = NULL;
    ListCell* tuple_cell = NULL;

    int2vector* partitionKey = NULL;
    MemoryContext old_context = NULL;
    Oid* partitionKeyDataType = NULL;
    errno_t rc = 0;

    /* build RangePartitionMap */
    hash_map = (HashPartitionMap*)palloc0(sizeof(HashPartitionMap));
    hash_map->type.type = PART_TYPE_HASH;
    hash_map->relid = RelationGetRelid(relation);
    hash_map->hashElementsNum = partition_list->length;

    /* get attribute NO. which is a member of partitionkey */
    partitionKey = getPartitionKeyAttrNo(
        &(partitionKeyDataType), partitioned_tuple, RelationGetDescr(pg_partition), RelationGetDescr(relation));
    /* copy the partitionKey */
    old_context = MemoryContextSwitchTo(LocalMyDBCacheMemCxt());

    hash_map->partitionKey = (int2vector*)palloc(Int2VectorSize(partitionKey->dim1));
    rc = memcpy_s(
        hash_map->partitionKey, Int2VectorSize(partitionKey->dim1), partitionKey, Int2VectorSize(partitionKey->dim1));
    securec_check(rc, "\0", "\0");
    hash_map->partitionKeyDataType = (Oid*)palloc(sizeof(Oid) * partitionKey->dim1);
    rc = memcpy_s(hash_map->partitionKeyDataType,
        sizeof(Oid) * partitionKey->dim1,
        partitionKeyDataType,
        sizeof(Oid) * partitionKey->dim1);
    securec_check(rc, "\0", "\0");
    (void)MemoryContextSwitchTo(old_context);

    /* allocate hash element array */
    hash_eles = (HashPartElement*)palloc0(sizeof(HashPartElement) * (hash_map->hashElementsNum));

    /* we will use reloid to get the column information from pg_attribute.
     * Only rootPartitionOid is in pg_attribute, so we can only use it.
     */
    Oid rootPartitionOid = GetRootPartitionOid(relation);

    /* iterate partition tuples, build RangeElement for per partition tuple */
    hash_itr = 0;
    foreach (tuple_cell, partition_list) {
        partition_tuple = (HeapTuple)lfirst(tuple_cell);
        partition_form = (Form_pg_partition)GETSTRUCT(partition_tuple);

        if (PART_STRATEGY_HASH != partition_form->partstrategy) {
            pfree_ext(hash_eles);
            pfree_ext(hash_map);

            ereport(ERROR,
                (errcode(ERRCODE_PARTITION_ERROR),
                    errmsg("Fail to build partitionmap for partitioned table \"%u\"", partition_form->parentid),
                    errdetail("Incorrect partition strategy for partition %u", HeapTupleGetOid(partition_tuple))));
        }

        buildHashElement(&(hash_eles[hash_itr]),
            hash_map->partitionKeyDataType,
            hash_map->partitionKey->dim1,
            rootPartitionOid,
            hash_map->partitionKey,
            partition_tuple,
            RelationGetDescr(pg_partition));

        hash_itr++;
    }

    Assert(partitionKey->dim1 == 1);
    qsort(hash_eles, hash_map->hashElementsNum, sizeof(HashPartElement), HashElementCmp);
    Assert(CheckHashPartitionMap(hash_eles, hash_map->hashElementsNum));

    /* hash element array back in RangePartitionMap */
    old_context = MemoryContextSwitchTo(LocalMyDBCacheMemCxt());
    hash_map->hashElements = CopyHashElements(hash_eles, hash_map->hashElementsNum, partitionKey->dim1);
    relation->partMap = (PartitionMap*)palloc(sizeof(HashPartitionMap));
    rc = memcpy_s(relation->partMap, sizeof(HashPartitionMap), hash_map, sizeof(HashPartitionMap));
    securec_check(rc, "\0", "\0");

    (void)MemoryContextSwitchTo(old_context);
    PartitionMapDestroyHashArray(hash_eles, hash_map->hashElementsNum);
}

/*
 * @@GaussDB@@
 * Target		: data partition
 * Brief		: build RangePartitionMap for partitioned table
 * Description	:
 * Notes		:
 */
static void buildRangePartitionMap(Relation relation, Form_pg_partition partitioned_form, HeapTuple partitioned_tuple,
    Relation pg_partition, const List* partition_list)
{
    int range_itr = 0;
    RangePartitionMap* range_map = NULL;
    RangeElement* range_eles = NULL;
    Form_pg_partition partition_form = NULL;
    HeapTuple partition_tuple = NULL;
    ListCell* tuple_cell = NULL;

    int2vector* partitionKey = NULL;
    MemoryContext old_context = NULL;
    Oid* partitionKeyDataType = NULL;
    errno_t rc = 0;

    /* build RangePartitionMap */
    range_map = (RangePartitionMap*)palloc0(sizeof(RangePartitionMap));
    range_map->type.type = PART_TYPE_RANGE;
    range_map->relid = RelationGetRelid(relation);
    range_map->rangeElementsNum = partition_list->length;

    /* get attribute NO. which is a member of partitionkey */
    partitionKey = getPartitionKeyAttrNo(
        &(partitionKeyDataType), partitioned_tuple, RelationGetDescr(pg_partition), RelationGetDescr(relation));
    /* copy the partitionKey */
    old_context = MemoryContextSwitchTo(LocalMyDBCacheMemCxt());

    range_map->partitionKey = (int2vector*)palloc(Int2VectorSize(partitionKey->dim1));
    rc = memcpy_s(
        range_map->partitionKey, Int2VectorSize(partitionKey->dim1), partitionKey, Int2VectorSize(partitionKey->dim1));
    securec_check(rc, "\0", "\0");
    range_map->partitionKeyDataType = (Oid*)palloc(sizeof(Oid) * partitionKey->dim1);
    rc = memcpy_s(range_map->partitionKeyDataType,
        sizeof(Oid) * partitionKey->dim1,
        partitionKeyDataType,
        sizeof(Oid) * partitionKey->dim1);
    securec_check(rc, "\0", "\0");

    if (partitioned_form->partstrategy == PART_STRATEGY_INTERVAL) {
        range_map->type.type = PART_TYPE_INTERVAL;
        /* the interval partition only supports one partition key */
        Assert(partitionKey->dim1 == 1);
        range_map->intervalValue = ReadInterval(partitioned_tuple, RelationGetDescr(pg_partition));
        range_map->intervalTablespace = ReadIntervalTablespace(partitioned_tuple, RelationGetDescr(pg_partition));
    }
    (void)MemoryContextSwitchTo(old_context);

    /* allocate range element array */
    range_eles = (RangeElement*)palloc0(sizeof(RangeElement) * (range_map->rangeElementsNum));

    /* we will use reloid to get the column information from pg_attribute.
     * Only rootPartitionOid is in pg_attribute, so we can only use it.
     */
    Oid rootPartitionOid = GetRootPartitionOid(relation);

    /* iterate partition tuples, build RangeElement for per partition tuple */
    range_itr = 0;
    foreach (tuple_cell, partition_list) {
        partition_tuple = (HeapTuple)lfirst(tuple_cell);
        partition_form = (Form_pg_partition)GETSTRUCT(partition_tuple);

        if (partition_form->partstrategy != PART_STRATEGY_RANGE &&
            partition_form->partstrategy != PART_STRATEGY_INTERVAL) {
            pfree_ext(range_eles);
            pfree_ext(range_map);

            ereport(ERROR,
                (errcode(ERRCODE_PARTITION_ERROR),
                    errmsg("Fail to build partitionmap for partitioned table \"%u\"", partition_form->parentid),
                    errdetail("Incorrect partition strategy for partition %u", HeapTupleGetOid(partition_tuple))));
        }

        BuildRangeElement(&(range_eles[range_itr]),
            range_map->partitionKeyDataType,
            range_map->partitionKey->dim1,
            rootPartitionOid,
            range_map->partitionKey,
            partition_tuple,
            RelationGetDescr(pg_partition),
            partition_form->partstrategy == PART_STRATEGY_INTERVAL);
        range_itr++;
    }

    qsort(range_eles, range_map->rangeElementsNum, sizeof(RangeElement), rangeElementCmp);

    /* range element array back in RangePartitionMap */
    old_context = MemoryContextSwitchTo(LocalMyDBCacheMemCxt());

    range_map->rangeElements = copyRangeElements(range_eles, range_map->rangeElementsNum, partitionKey->dim1);
    relation->partMap = (PartitionMap*)palloc(sizeof(RangePartitionMap));
    rc = memcpy_s(relation->partMap, sizeof(RangePartitionMap), range_map, sizeof(RangePartitionMap));
    securec_check(rc, "\0", "\0");

    (void)MemoryContextSwitchTo(old_context);
    partitionMapDestroyRangeArray(range_eles, range_map->rangeElementsNum);
}

/*
 * @@GaussDB@@
 * Brief		:
 * Description	: transform Datum data type to Const
 * Notes		:
 */
Const* transformDatum2Const(TupleDesc tupledesc, int16 attnum, Datum datumValue, bool isnull, Const* cnst)
{

    Oid typid = InvalidOid;
    Oid collid = InvalidOid;
    int32 attindex = -1;
    int32 typmod = -1;
    int16 typlen = 0;
    bool typbyval = false;
    Form_pg_attribute att = NULL;

    Assert(PointerIsValid(tupledesc));
    Assert(attnum <= tupledesc->natts);
    Assert(attnum >= 1);
    attindex = attnum - 1;
    att = tupledesc->attrs[attindex];

    typid = att->atttypid;
    typmod = att->atttypmod;
    collid = att->attcollation;
    typlen = att->attlen;
    typbyval = att->attbyval;

    cnst->xpr.type = T_Const;
    cnst->consttype = typid;
    cnst->consttypmod = typmod;
    cnst->constcollid = collid;
    cnst->constlen = typlen;
    cnst->constvalue = isnull ? 0 : datumValue;
    cnst->constisnull = isnull;
    cnst->constbyval = typbyval;
    cnst->location = -1; /* "unknown" */
    cnst->ismaxvalue = false;
    return cnst;
}

/* 1. increace the ref count of partition map, to protect from rebuilding of partition map
 * 2. copy out the boundary of src partition, forming a new boundary list.
 * 3. decreace the ref count of partition map.
 */
List* getRangePartitionBoundaryList(Relation rel, int sequence)
{
    List* result = NIL;
    RangePartitionMap* partMap = (RangePartitionMap*)rel->partMap;

    if (!RELATION_IS_PARTITIONED(rel))
        return NIL;

    incre_partmap_refcount(rel->partMap);
    if (sequence >= 0 && sequence < partMap->rangeElementsNum) {
        int i = 0;
        int partKeyNum = partMap->partitionKey->dim1;
        Const** srcBound = partMap->rangeElements[sequence].boundary;

        for (i = 0; i < partKeyNum; i++) {
            result = lappend(result, (Const*)copyObject(srcBound[i]));
        }
    } else {
        decre_partmap_refcount(rel->partMap);
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("invalid partition sequence: %d of relation \"%s\", check whether the table name and partition "
                       "name are correct.",
                    sequence,
                    RelationGetRelationName(rel))));
    }
    decre_partmap_refcount(rel->partMap);
    return result;
}

List* getListPartitionBoundaryList(Relation rel, int sequence)
{
    List* result = NIL;
    ListPartitionMap* partMap = (ListPartitionMap*)rel->partMap;

    if (!RELATION_IS_PARTITIONED(rel))
        return NIL;

    incre_partmap_refcount(rel->partMap);
    if (sequence >= 0 && sequence < partMap->listElementsNum) {
        int i = 0;
        Const** srcBound = partMap->listElements[sequence].boundary;
        int len = partMap->listElements[sequence].len;

        for (i = 0; i < len; i++) {
            result = lappend(result, (Const*)copyObject(srcBound[i]));
        }
    } else {
        decre_partmap_refcount(rel->partMap);
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("invalid partition sequence: %d of relation \"%s\", check whether the table name and partition "
                       "name are correct.",
                    sequence,
                    RelationGetRelationName(rel))));
    }
    decre_partmap_refcount(rel->partMap);
    return result;
}

List* getHashPartitionBoundaryList(Relation rel, int sequence)
{
    List* result = NIL;
    HashPartitionMap* partMap = (HashPartitionMap*)rel->partMap;

    if (!RELATION_IS_PARTITIONED(rel))
        return NIL;

    incre_partmap_refcount(rel->partMap);
    if (sequence >= 0 && sequence < partMap->hashElementsNum) {
        int i = 0;
        int partKeyNum = partMap->partitionKey->dim1;
        Const** srcBound = partMap->hashElements[sequence].boundary;

        for (i = 0; i < partKeyNum; i++) {
            result = lappend(result, (Const*)copyObject(srcBound[i]));
        }
    } else {
        decre_partmap_refcount(rel->partMap);
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("invalid partition sequence: %d of relation \"%s\", check whether the table name and partition "
                       "name are correct.",
                    sequence,
                    RelationGetRelationName(rel))));
    }
    decre_partmap_refcount(rel->partMap);
    return result;
}

/*
 * @@GaussDB@@
 * Brief		:
 * Description	: routing partition key value list to targeting partition
 * Notes		:
 */
Oid partitionKeyValueListGetPartitionOid(Relation rel, List* partKeyValueList, bool topClosed)
{
    ListCell* cell = NULL;
    int len = 0;

    if (list_length(partKeyValueList) > PARTKEY_VALUE_MAXNUM) {
        ereport(ERROR, (errcode(ERRCODE_CONFIGURATION_LIMIT_EXCEEDED),
            (errmsg("too many partition keys, allowed is %d", PARTKEY_VALUE_MAXNUM), errdetail("N/A"),
            errcause("too many partition keys for this syntax."), erraction("Please check the syntax is Ok"))));
    }

    foreach (cell, partKeyValueList) {
        t_thrd.utils_cxt.valueItemArr[len++] = (Const*)lfirst(cell);
    }
    partitionRoutingForValue(rel, t_thrd.utils_cxt.valueItemArr, len, topClosed, false, &(*t_thrd.utils_cxt.partId));

    return (*t_thrd.utils_cxt.partId).partitionId;
}

/*
 * @@GaussDB@@
 * Brief		:
 * Description	:give the tuple, return oid of the corresponding partition
 * Notes		:
 */
Oid getRangePartitionOid(PartitionMap *partitionmap, Const** partKeyValue, int32* partSeq, bool topClosed)
{
    RangeElement* rangeElementIterator = NULL;
    RangePartitionMap* rangePartMap = NULL;
    Oid result = InvalidOid;
    int keyNums;
    int hit = -1;
    int min_part_id = 0;
    int max_part_id = 0;
    int local_part_id = 0;
    int compare = 0;
    Const** boundary = NULL;

    Assert(PointerIsValid(partitionmap));
    Assert(PointerIsValid(partKeyValue));

    incre_partmap_refcount(partitionmap);
    rangePartMap = (RangePartitionMap*)(partitionmap);

    keyNums = rangePartMap->partitionKey->dim1;
    max_part_id = rangePartMap->rangeElementsNum - 1;
    boundary = rangePartMap->rangeElements[max_part_id].boundary;
    partitonKeyCompareForRouting(partKeyValue, boundary, (uint32)keyNums, compare);

    if (compare == 0) {
        if (topClosed) {
            hit = -1;
        } else {
            hit = max_part_id;
        }
    } else if (compare > 0) {
        hit = -1;
    } else {
        /* semi-seek */
        while (max_part_id > min_part_id) {
            local_part_id = (uint32)(max_part_id + min_part_id) >> 1;
            rangeElementIterator = &(rangePartMap->rangeElements[local_part_id]);

            boundary = rangeElementIterator->boundary;
            partitonKeyCompareForRouting(partKeyValue, boundary, (uint32)keyNums, compare);

            if (compare == 0) {
                hit = local_part_id;

                if (topClosed) {
                    hit += 1;
                }
                break;
            } else if (compare > 0) {
                min_part_id = local_part_id + 1;
            } else {
                max_part_id = local_part_id;
            }
        }
        /* get it */
        if (max_part_id == min_part_id) {
            hit = max_part_id;
        }
    }

    if (PointerIsValid(partSeq)) {
        *partSeq = hit;
    }

    if (hit >= 0) {
        result = rangePartMap->rangeElements[hit].partitionOid;
    }

    decre_partmap_refcount(partitionmap);
    return result;
}

Oid getListPartitionOid(PartitionMap* partMap, Const** partKeyValue, int32* partSeq, bool topClosed)
{
    ListPartitionMap* listPartMap = NULL;
    Oid result = InvalidOid;
    int keyNums = 0;
    int hit = -1;
    int compare = 0;
    Const** boundary = NULL;
    Oid defaultPartitionOid = InvalidOid;
    bool existDefaultPartition = false;
    int defaultPartitionHit = -1;

    Assert(PointerIsValid(partMap));
    Assert(PointerIsValid(partKeyValue));

    incre_partmap_refcount(partMap);
    listPartMap = (ListPartitionMap*)(partMap);
    keyNums = listPartMap->partitionKey->dim1;
    
    int i = 0;
    while (i < listPartMap->listElementsNum && hit < 0) {
        boundary = listPartMap->listElements[i].boundary;
        int list_len = listPartMap->listElements[i].len;
        if (list_len == 1 && ((Const*)boundary[0])->ismaxvalue) {
            defaultPartitionOid = listPartMap->listElements[i].partitionOid;
            existDefaultPartition = true;
            defaultPartitionHit = i;
        }
        int j = 0;
        while (j < list_len) {
            partitonKeyCompareForRouting(partKeyValue, boundary + j, (uint32)keyNums, compare);
            if (compare == 0) {
                hit = i;
                break;
            }
            j++;
        }
        i++;
    }

    if (PointerIsValid(partSeq)) {
        *partSeq = hit;
    }

    if (hit >= 0) {
        result = listPartMap->listElements[hit].partitionOid;
    } else if (existDefaultPartition) {
        result = defaultPartitionOid;
        *partSeq = defaultPartitionHit;
    }

    decre_partmap_refcount(partMap);
    return result;
}

Oid getHashPartitionOid(PartitionMap* partMap, Const** partKeyValue, int32* partSeq, bool topClosed)
{
    HashPartitionMap* hashPartMap = NULL;
    Oid result = InvalidOid;
    int keyNums = 0;
    int hit = -1;

    Assert(PointerIsValid(partMap));
    Assert(PointerIsValid(partKeyValue));

    incre_partmap_refcount(partMap);
    hashPartMap = (HashPartitionMap*)(partMap);

    keyNums = hashPartMap->partitionKey->dim1;
    
    int i = 0;
    uint32 hash_value = 0;
    while (i < keyNums) {
        if (partKeyValue[i]->constisnull) {
            if (PointerIsValid(partSeq)) {
                *partSeq = hit;
            }
            decre_partmap_refcount(partMap);
            return result;
        }
        hash_value = hashValueCombination(hash_value, partKeyValue[i]->consttype, partKeyValue[i]->constvalue, false,
                                          LOCATOR_TYPE_HASH);
        i++;
    }

    hit = hash_value % (uint32)(hashPartMap->hashElementsNum);

    if (PointerIsValid(partSeq)) {
        *partSeq = hit;
    }

    if (hit >= 0) {
        result = hashPartMap->hashElements[hit].partitionOid;
    }

    decre_partmap_refcount(partMap);
    return result;
}

Oid GetPartitionOidByParam(Relation relation, Param *paramArg, ParamExternData *prm)
{
    int16 typLen;
    bool typByVal = false;
    Assert(paramArg != NULL);
    Assert(prm->ptype == paramArg->paramtype);
    get_typlenbyval(paramArg->paramtype, &typLen, &typByVal);
    Const *value = makeConst(paramArg->paramtype, paramArg->paramtypmod, paramArg->paramcollid,
                             (int)typLen, prm->value, prm->isnull, typByVal);

    return getRangePartitionOid(relation->partMap, &value, NULL, true);
}

static Const* CalcLowBoundary(const Const* upBoundary, Interval* intervalValue)
{
    Assert(upBoundary->consttype == TIMESTAMPOID || upBoundary->consttype == TIMESTAMPTZOID ||
           upBoundary->consttype == DATEOID);
    Datum lowValue;
    if (upBoundary->consttype == DATEOID) {
        Timestamp lowTs = timestamp_mi_interval(date2timestamp(DatumGetDateADT(upBoundary->constvalue)), intervalValue);
        lowValue = timestamp2date(lowTs);

    } else {
        Timestamp lowTs = timestamp_mi_interval(DatumGetTimestamp(upBoundary->constvalue), intervalValue);
        lowValue = TimestampGetDatum(lowTs);
    }

    return makeConst(upBoundary->consttype,
        upBoundary->consttypmod,
        upBoundary->constcollid,
        upBoundary->constlen,
        lowValue,
        upBoundary->constisnull,
        upBoundary->constbyval);
}

void getFakeReationForPartitionOid(HTAB **fakeRels, MemoryContext cxt, Relation rel, Oid partOid,
                                   Relation *fakeRelation, Partition *partition, LOCKMODE lmode)
{
    PartRelIdCacheKey _key = {partOid, -1};
    Relation partParentRel = rel;
    if (PointerIsValid(*partition)) {
        return;
    }
    if (RelationIsNonpartitioned(partParentRel)) {
        *fakeRelation = NULL;
        *partition = NULL;
        return;
    }
    if (PointerIsValid(*fakeRels)) {
        FakeRelationIdCacheLookup((*fakeRels), _key, *fakeRelation, *partition);
        if (!RelationIsValid(*fakeRelation)) {
            *partition = partitionOpen(partParentRel, partOid, lmode);
            *fakeRelation = partitionGetRelation(partParentRel, *partition);
            FakeRelationCacheInsert((*fakeRels), (*fakeRelation), (*partition), -1);
        }
    } else {
        HASHCTL ctl;
        errno_t errorno = EOK;
        errorno = memset_s(&ctl, sizeof(ctl), 0, sizeof(ctl));
        securec_check_c(errorno, "\0", "\0");
        ctl.keysize = sizeof(PartRelIdCacheKey);
        ctl.entrysize = sizeof(PartRelIdCacheEnt);
        ctl.hash = tag_hash;
        ctl.hcxt = cxt;
        *fakeRels = hash_create("fakeRelationCache by OID", FAKERELATIONCACHESIZE, &ctl,
                                HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT);
        *partition = partitionOpen(partParentRel, partOid, lmode);
        *fakeRelation = partitionGetRelation(partParentRel, *partition);
        FakeRelationCacheInsert((*fakeRels), (*fakeRelation), (*partition), -1);
    }
}

int ValueCmpLowBoudary(Const** partKeyValue, const RangeElement* partition, Interval* intervalValue)
{
    Assert(partition->isInterval);
    Assert(partition->len == 1);
    int compare = 0;
    Const* lowBoundary = CalcLowBoundary(partition->boundary[0], intervalValue);
    partitonKeyCompareForRouting(partKeyValue, &lowBoundary, (uint32)(partition->len), compare);
    pfree(lowBoundary);
    return compare;
}

/* the low boundary is close */
bool ValueSatisfyLowBoudary(Const** partKeyValue, RangeElement* partition, Interval* intervalValue, bool topClosed)
{
    int compare = ValueCmpLowBoudary(partKeyValue, partition, intervalValue);
    if (compare > 0 || (compare == 0 && topClosed)) {
        return true;
    }

    return false;
}

/*
 * @@GaussDB@@
 * Target		: data partition
 * Brief		:
 * Description	:
 * Notes		:
 */
int getNumberOfRangePartitions(Relation rel)
{
    int ret;

    if (!RELATION_IS_PARTITIONED(rel)) {
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("CAN NOT get number of partition against NON-PARTITIONED relation")));
    }

    if (rel->partMap->type == PART_TYPE_RANGE || rel->partMap->type == PART_TYPE_INTERVAL) {
        RangePartitionMap* rangeMap = NULL;
        rangeMap = (RangePartitionMap*)(rel->partMap);

        ret = rangeMap->rangeElementsNum;
    } else { /* type equals to "PART_TYPE_INTERVAL" */
        ret = 0;
    }
    return ret;
}

int getNumberOfListPartitions(Relation rel)
{
    int ret;

    if (!RELATION_IS_PARTITIONED(rel)) {
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("CAN NOT get number of partition against NON-PARTITIONED relation")));
    }

    if (rel->partMap->type == PART_TYPE_LIST) {
        ListPartitionMap* listMap = NULL;
        listMap = (ListPartitionMap*)(rel->partMap);

        ret = listMap->listElementsNum;
    } else { /* type equals to "PART_TYPE_INTERVAL" */
        ret = 0;
    }
    return ret;
}

int getNumberOfHashPartitions(Relation rel)
{
    int ret;

    if (!RELATION_IS_PARTITIONED(rel)) {
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("CAN NOT get number of partition against NON-PARTITIONED relation")));
    }

    if (rel->partMap->type == PART_TYPE_HASH) {
        HashPartitionMap* hashMap = NULL;
        hashMap = (HashPartitionMap*)(rel->partMap);

        ret = hashMap->hashElementsNum;
    } else { /* type equals to "PART_TYPE_INTERVAL" */
        ret = 0;
    }
    return ret;
}

/*
 * @@GaussDB@@
 * Target		: data partition
 * Brief		:
 * Description	:
 * Notes		:
 */
int getNumberOfIntervalPartitions(Relation rel)
{
    return 0;
}

int getNumberOfPartitions(Relation rel)
{
    int ranges = 0;

    if (!RELATION_IS_PARTITIONED(rel)) {
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("CAN NOT get number of partition against NON-PARTITIONED relation")));
    }

    if (rel->partMap->type == PART_TYPE_LIST) {
        ranges = getNumberOfListPartitions(rel);
    } else if (rel->partMap->type == PART_TYPE_HASH) {
        ranges = getNumberOfHashPartitions(rel);
    } else {
        ranges = getNumberOfRangePartitions(rel);
    }

    return ranges;
}

Oid partIDGetPartOid(Relation relation, PartitionIdentifier* partID)
{
    Oid partid = InvalidOid;

    if (!PointerIsValid(relation) || !PointerIsValid(partID)) {
        ereport(
            ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("partIDGetPartOid(), invalid input parameters")));
    }

    if (!relation->partMap || partID->partSeq < 0) {
        return InvalidOid;
    }

    if (relation->partMap->type == PART_TYPE_RANGE || relation->partMap->type == PART_TYPE_INTERVAL) {

        RangePartitionMap* rang_map = NULL;
        rang_map = (RangePartitionMap*)(relation->partMap);

        if (partID->partSeq <= rang_map->rangeElementsNum) {
            partid = (rang_map->rangeElements + partID->partSeq)->partitionOid;
        } else {
            partid = InvalidOid;
            ereport(ERROR,
                (errcode(ERRCODE_INTERVAL_FIELD_OVERFLOW),
                    errmsg("fail to get partition oid, because range partition index is overflow.")));
        }
    } else if (relation->partMap->type == PART_TYPE_LIST) {
        Assert(partID->partArea == PART_AREA_LIST);
        ListPartitionMap* rang_map = NULL;

        rang_map = (ListPartitionMap*)(relation->partMap);

        if (partID->partSeq <= rang_map->listElementsNum) {
            partid = (rang_map->listElements + partID->partSeq)->partitionOid;
        } else {
            partid = InvalidOid;
            ereport(ERROR,
                (errcode(ERRCODE_INTERVAL_FIELD_OVERFLOW),
                    errmsg("fail to get partition oid, because range partition index is overflow.")));
        }
    }  else if (relation->partMap->type == PART_TYPE_HASH) {
        Assert(partID->partArea == PART_AREA_HASH);
        HashPartitionMap* rang_map = NULL;

        rang_map = (HashPartitionMap*)(relation->partMap);

        if (partID->partSeq <= rang_map->hashElementsNum) {
            partid = (rang_map->hashElements + partID->partSeq)->partitionOid;
        } else {
            partid = InvalidOid;
            ereport(ERROR,
                (errcode(ERRCODE_INTERVAL_FIELD_OVERFLOW),
                    errmsg("fail to get partition oid, because range partition index is overflow.")));
        }
    } else {
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("unsupported partition strategy")));
    }

    return partid;
}

/*
 * @@GaussDB@@
 * Target		: data partition
 * Brief		:
 * Description	:
 * Notes		:
 */
PartitionIdentifier* partOidGetPartID(Relation rel, Oid partOid)
{
    PartitionIdentifier* result = NULL;

    /* never happen */
    if (!PointerIsValid(rel) || !OidIsValid(partOid)) {
        ereport(ERROR, (errcode(ERRCODE_FETCH_DATA_FAILED), errmsg("fail to get partition strategy")));
    }

    result = (PartitionIdentifier*)palloc0(sizeof(PartitionIdentifier));
    if (rel->partMap->type == PART_TYPE_RANGE || rel->partMap->type == PART_TYPE_INTERVAL) {
        int i;
        RangePartitionMap* rangeMap = (RangePartitionMap*)rel->partMap;

        for (i = 0; i < rangeMap->rangeElementsNum; i++) {
            if (partOid == rangeMap->rangeElements[i].partitionOid) {
                if (rangeMap->rangeElements[i].isInterval) {
                    result->partArea = PART_AREA_INTERVAL;
                } else {
                    result->partArea = PART_AREA_RANGE;
                }
                result->partSeq = i;
                result->fileExist = true;
                result->partitionId = partOid;
                break;
            }
        }
    } else if (PART_TYPE_LIST == rel->partMap->type) {
        int i;
        ListPartitionMap* rangeMap = (ListPartitionMap*)rel->partMap;

        for (i = 0; i < rangeMap->listElementsNum; i++) {
            if (partOid == rangeMap->listElements[i].partitionOid) {
                result->partArea = PART_AREA_LIST;
                result->partSeq = i;
                result->fileExist = true;
                result->partitionId = partOid;
                break;
            }
        }
    } else if (PART_TYPE_HASH == rel->partMap->type) {
        int i;
        HashPartitionMap* rangeMap = (HashPartitionMap*)rel->partMap;

        for (i = 0; i < rangeMap->hashElementsNum; i++) {
            if (partOid == rangeMap->hashElements[i].partitionOid) {
                result->partArea = PART_AREA_HASH;
                result->partSeq = i;
                result->fileExist = true;
                result->partitionId = partOid;
                break;
            }
        }
    } else {
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("unsupported partition strategy")));
    }

    return result;
}

// comment: the function result value of the first partition seq is 1
// a wrapper for partOidGetPartID
int partOidGetPartSequence(Relation rel, Oid partOid)
{
    int resultPartSequence = 0;
    PartitionIdentifier* resultPartID = NULL;

    if (RelationIsNonpartitioned(rel))
        return -1;

    resultPartID = partOidGetPartID(rel, partOid);
    if (resultPartID == NULL) {
        resultPartSequence = -1;
    } else if (false == resultPartID->fileExist || PART_AREA_NONE == resultPartID->partArea) {
        resultPartSequence = -1;
    } else {
        resultPartSequence = resultPartID->partSeq + 1;
    }

    pfree_ext(resultPartID);
    return resultPartSequence;
}

/*
 * @Description: compare two const,datatype of const must be one of datatype partition key supported,
 * 	and the datatype is bpchar varchar or text,the collation id of two consts must be same.
 * @param
 * @param
 * @return 0: value1==value2   1:value1>vlaue2    -1:value1<value2
 */

int constCompare_constType(Const* value1, Const* value2)
{
    EState* estate = NULL;
    ExprContext* econtext = NULL;
    ExprState* exprstate = NULL;
    bool result = false;
    int ret = 0;
    Expr* eqExpr = NULL;
    Expr* gtExpr = NULL;
    bool isNull = false;
    ParseState* pstate = NULL;

    pstate = make_parsestate(NULL);

    eqExpr = (Expr*)makeSimpleA_Expr(AEXPR_OP, "=", (Node*)value1, (Node*)value2, -1);

    eqExpr = (Expr*)transformExpr(pstate, (Node*)eqExpr);

    gtExpr = (Expr*)makeSimpleA_Expr(AEXPR_OP, ">", (Node*)value1, (Node*)value2, -1);

    gtExpr = (Expr*)transformExpr(pstate, (Node*)gtExpr);
    ((OpExpr*)gtExpr)->inputcollid = value1->constcollid;

    estate = CreateExecutorState();
    econtext = GetPerTupleExprContext(estate);

    exprstate = ExecPrepareExpr(eqExpr, estate);
    if (!PointerIsValid(exprstate)) {
        ereport(ERROR,
            (errcode(ERRCODE_E_R_I_E_INVALID_SQLSTATE_RETURNED),
                errmsg("failed when making EQUAL expression state for constCompare")));
    }

    result = DatumGetBool(ExecEvalExpr(exprstate, econtext, &isNull, NULL));

    FreeExecutorState(estate);

    if (result) {
        ret = 0;
    } else {
        estate = CreateExecutorState();
        econtext = GetPerTupleExprContext(estate);
        exprstate = ExecPrepareExpr(gtExpr, estate);

        if (!PointerIsValid(exprstate)) {
            ereport(ERROR,
                (errcode(ERRCODE_E_R_I_E_INVALID_SQLSTATE_RETURNED),
                    errmsg("failed when making GREATE-THAN expression state for constCompare")));
        }

        result = DatumGetBool(ExecEvalExpr(exprstate, econtext, &isNull, NULL));

        if (result) {
            ret = 1;
        } else {
            ret = -1;
        }

        FreeExecutorState(estate);
    }

    pfree_ext(pstate);

    return ret;
}

int rangeElementCmp(const void* a, const void* b)
{
    const RangeElement* rea = (const RangeElement*)a;
    const RangeElement* reb = (const RangeElement*)b;

    Assert(rea->len == reb->len);
    Assert(rea->len <= RANGE_PARTKEYMAXNUM);

    return partitonKeyCompare((Const**)rea->boundary, (Const**)reb->boundary, rea->len);
}

int HashElementCmp(const void* a, const void* b)
{
    const HashPartElement* rea = (const HashPartElement*)a;
    const HashPartElement* reb = (const HashPartElement*)b;

    int32 constvalue1 = DatumGetInt32((Const*)rea->boundary[0]->constvalue);
    int32 constvalue2 = DatumGetInt32((Const*)reb->boundary[0]->constvalue);
    if (constvalue1 < constvalue2) {
        return 1;
    } else if (constvalue1 > constvalue2) {
        return -1;
    } else {
        return 0;
    }
}

/*
 * @@GaussDB@@
 * Brief		:
 * Description	: return the partition number of a partitioned table ,include range partitions and
 * 				  interval partitions
 *
 * Notes		: caller should keep a suitable lock on the parittioned table
 */
int getPartitionNumber(PartitionMap* map)
{
    int result = -1;

    if (map->type == PART_TYPE_RANGE || map->type == PART_TYPE_INTERVAL) {
        result = ((RangePartitionMap*)map)->rangeElementsNum;
    } else if (map->type == PART_TYPE_LIST) { 
        result = ((ListPartitionMap*)map)->listElementsNum;
    } else if (map->type == PART_TYPE_HASH) { 
        result = ((HashPartitionMap*)map)->hashElementsNum;
    } else {
        ereport(ERROR, (errcode(ERRCODE_INVALID_OBJECT_DEFINITION), errmsg("unsupported partitioned strategy")));
    }

    return result;
}

int GetSubPartitionNumber(Relation rel)
{
    PartitionMap* map = rel->partMap;
    int result = getPartitionNumber(map);
    Oid partOid = InvalidOid;
    int subPartNum = 0;
    for (int conuter = 0; conuter < result; ++conuter) {
        if (map->type == PART_TYPE_LIST) {
            partOid = ((ListPartitionMap *)map)->listElements[conuter].partitionOid;
        } else if (map->type == PART_TYPE_HASH) {
            partOid = ((HashPartitionMap *)map)->hashElements[conuter].partitionOid;
        } else {
            partOid = ((RangePartitionMap *)map)->rangeElements[conuter].partitionOid;
        }
        Partition part = partitionOpen(rel, partOid, AccessShareLock);
        Relation partRel = partitionGetRelation(rel, part);
        subPartNum += getPartitionNumber(partRel->partMap);
        releaseDummyRelation(&partRel);
        partitionClose(rel, part, AccessShareLock);
    }

    return subPartNum;
}

// check the partition has toast
bool partitionHasToast(Oid partOid)
{
    HeapTuple tuple = NULL;
    Form_pg_partition partForm = NULL;
    bool result = false;
    tuple = SearchSysCache1(PARTRELID, ObjectIdGetDatum(partOid));
    if (!HeapTupleIsValid(tuple)) {
        Assert(0);
        ereport(ERROR,
            (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for table partition %u", partOid)));
    }
    Assert(HeapTupleGetOid(tuple) == partOid);

    partForm = (Form_pg_partition)GETSTRUCT(tuple);

    if (OidIsValid(partForm->reltoastrelid)) {
        result = true;
    }

    ReleaseSysCache(tuple);

    return result;
}

void incre_partmap_refcount(PartitionMap* map)
{
    ResourceOwnerEnlargePartitionMapRefs(t_thrd.utils_cxt.CurrentResourceOwner);
    map->refcount += 1;
    if (!IsBootstrapProcessingMode())
        ResourceOwnerRememberPartitionMapRef(t_thrd.utils_cxt.CurrentResourceOwner, map);
}

void decre_partmap_refcount(PartitionMap* map)
{
    Assert(map->refcount > 0);
    map->refcount -= 1;
    if (!IsBootstrapProcessingMode())
        ResourceOwnerForgetPartitionMapRef(t_thrd.utils_cxt.CurrentResourceOwner, map);
}

/*
 * Get the oid of the partition which is a interval partition and next to the droped range partition which is
 * specificed by partOid. If the droped partition is a interval partition, the next partition no need to
 * be changed to range partition, return InvalidOid. If the next partition is a range partition, nothing need
 * to do, return InvalidOid.
 */
Oid GetNeedDegradToRangePartOid(Relation rel, Oid partOid)
{
    /* never happen */
    if (!PointerIsValid(rel) || !OidIsValid(partOid)) {
        ereport(ERROR,
            (errcode(ERRCODE_FETCH_DATA_FAILED), errmsg("invalid partitioned table relaiton or partition table oid")));
    }
    Assert(rel->partMap->type == PART_TYPE_RANGE || rel->partMap->type == PART_TYPE_INTERVAL);

    /* In normal range partitioned tabel, there has no interval ranges. */
    if (rel->partMap->type == PART_TYPE_RANGE) {
        return InvalidOid;
    }

    RangePartitionMap* rangeMap = (RangePartitionMap*)rel->partMap;
    for (int i = 0; i < rangeMap->rangeElementsNum; i++) {
        if (rangeMap->rangeElements[i].partitionOid == partOid) {
            /*
             * 1. the droped range is interval range
             * 2. there is no more ranges
             * 3. the next partition is a range partition
             */
            if (rangeMap->rangeElements[i].isInterval || (i == rangeMap->rangeElementsNum - 1) ||
                !rangeMap->rangeElements[i + 1].isInterval) {
                return InvalidOid;
            }

            return rangeMap->rangeElements[i + 1].partitionOid;
        }
    }
    /* It must never happened. */
    ereport(ERROR, (errcode(ERRCODE_CASE_NOT_FOUND), errmsg("Not find the target partiton %u", partOid)));
    return InvalidOid;
}
