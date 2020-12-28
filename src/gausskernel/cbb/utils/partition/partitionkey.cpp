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
 * File Name	: partitionkey.cpp
 * Target		: data partition
 * Brief		:
 * Description	:
 * History	:
 * 
 * IDENTIFICATION
 *	  src/gausskernel/cbb/utils/partition/partitionkey.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "catalog/pg_type.h"
#include "nodes/parsenodes.h"
#include "nodes/value.h"
#include "utils/array.h"
#include "utils/lsyscache.h"
#include "utils/builtins.h"
#include "utils/partitionkey.h"

/*
 * @@GaussDB@@
 * Brief
 * Description	: 	transform the list of maxvalue
 *				  	(partition's boundary, Const node form) into TEXT array
 * input		: 	List of Const
 * return value	: 	TEXT array
 * Note			:
 * Review       : 	xuzhongqing 67238
 */
#define constIsMaxValue(value) ((value)->ismaxvalue)

Datum transformPartitionBoundary(List* bondary, const bool* isTimestamptz)
{
    Datum result;
    ArrayBuildState* astate = NULL;
    ListCell* cell = NULL;
    int16 typlen = 0;
    bool typbyval = false;
    char typalign;
    char typdelim;
    Oid typioparam = InvalidOid;
    Oid outfunc = InvalidOid;

    int partKeyIdx = 0;

    /* no change if empty list */
    if (bondary == NIL) {
        return (Datum)0;
    }

    /* We build new array using accumArrayResult */
    astate = NULL;

    /*
     * If CREATE/SET, add new options to array; if RESET, just check that the
     * user didn't say RESET (option=val).  (Must do this because the grammar
     * doesn't enforce it.)
     */
    foreach (cell, bondary) {
        Node* partKeyFld = (Node*)lfirst(cell);
        Const* maxValueItem = NULL;
        text* t = NULL;
        char* maxValue = NULL;
        Size len = 0;
        errno_t rc = 0;
        Datum datumValue = (Datum)0;

        Assert(nodeTag(partKeyFld) == T_Const);
        maxValueItem = (Const*)partKeyFld;

        if (!constIsMaxValue(maxValueItem)) {
            /* get outfunc for consttype, excute the corresponding typeout function
             * transform Const->constvalue into string format.
             */
            get_type_io_data(maxValueItem->consttype,
                IOFunc_output,
                &typlen,
                &typbyval,
                &typalign,
                &typdelim,
                &typioparam,
                &outfunc);
            maxValue =
                DatumGetCString(OidFunctionCall1Coll(outfunc, maxValueItem->constcollid, maxValueItem->constvalue));

            if (isTimestamptz[partKeyIdx]) {
                int tmp = u_sess->time_cxt.DateStyle;
                u_sess->time_cxt.DateStyle = USE_ISO_DATES;
                datumValue = DirectFunctionCall3(
                    timestamptz_in, CStringGetDatum(maxValue), ObjectIdGetDatum(InvalidOid), Int32GetDatum(-1));

                maxValue = (char*)DirectFunctionCall1(timestamptz_out, datumValue);
                u_sess->time_cxt.DateStyle = tmp;
            }

            /* turn typeout function's output into string */
            len = VARHDRSZ + strlen(maxValue);

            t = (text*)palloc(len + 1);
            SET_VARSIZE(t, len);

            rc = snprintf_s(VARDATA(t), (len + 1 - VARHDRSZ), strlen(maxValue), "%s", maxValue);
            securec_check_ss(rc, "\0", "\0");

            astate = accumArrayResult(astate, PointerGetDatum(t), false, TEXTOID, CurrentMemoryContext);
        } else {
            maxValue = NULL;
            astate = accumArrayResult(astate, (Datum)NULL, true, TEXTOID, CurrentMemoryContext);
        }

        partKeyIdx++;
    }

    result = makeArrayResult(astate, CurrentMemoryContext);
    return result;
}

Datum transformListBoundary(List* bondary, const bool* isTimestamptz)
{
    Datum result;
    ArrayBuildState* astate = NULL;
    ListCell* cell = NULL;
    int16 typlen = 0;
    bool typbyval = false;
    char typalign;
    char typdelim;
    Oid typioparam = InvalidOid;
    Oid outfunc = InvalidOid;

    int partKeyIdx = 0;

    /* no change if empty list */
    if (bondary == NIL) {
        return (Datum)0;
    }

    /* We build new array using accumArrayResult */
    astate = NULL;

    /*
 *      * If CREATE/SET, add new options to array; if RESET, just check that the
 *           * user didn't say RESET (option=val).  (Must do this because the grammar
 *                * doesn't enforce it.)
 *                     */
    foreach (cell, bondary) {
        Node* partKeyFld = (Node*)lfirst(cell);
        Const* maxValueItem = NULL;
        text* t = NULL;
        char* maxValue = NULL;
        Size len = 0;
        errno_t rc = 0;
        Datum datumValue = (Datum)0;

        Assert(nodeTag(partKeyFld) == T_Const);
        maxValueItem = (Const*)partKeyFld;

        if (!constIsMaxValue(maxValueItem)) {
            /* get outfunc for consttype, excute the corresponding typeout function
 *              * transform Const->constvalue into string format.
 *                           */
            get_type_io_data(maxValueItem->consttype,
                IOFunc_output,
                &typlen,
                &typbyval,
                &typalign,
                &typdelim,
                &typioparam,
                &outfunc);
            maxValue =
                DatumGetCString(OidFunctionCall1Coll(outfunc, maxValueItem->constcollid, maxValueItem->constvalue));

            if (isTimestamptz[partKeyIdx]) {
                int tmp = u_sess->time_cxt.DateStyle;
                u_sess->time_cxt.DateStyle = USE_ISO_DATES;
                datumValue = DirectFunctionCall3(
                    timestamptz_in, CStringGetDatum(maxValue), ObjectIdGetDatum(InvalidOid), Int32GetDatum(-1));

                maxValue = (char*)DirectFunctionCall1(timestamptz_out, datumValue);
                u_sess->time_cxt.DateStyle = tmp;
            }

            /* turn typeout function's output into string */
            len = VARHDRSZ + strlen(maxValue);

            t = (text*)palloc(len + 1);
            SET_VARSIZE(t, len);

            rc = snprintf_s(VARDATA(t), (len + 1 - VARHDRSZ), strlen(maxValue), "%s", maxValue);
            securec_check_ss(rc, "\0", "\0");

            astate = accumArrayResult(astate, PointerGetDatum(t), false, TEXTOID, CurrentMemoryContext);
        } else {
            maxValue = NULL;
            astate = accumArrayResult(astate, (Datum)NULL, true, TEXTOID, CurrentMemoryContext);
        }
    }

    result = makeArrayResult(astate, CurrentMemoryContext);
    return result;
}

/*
 * @@GaussDB@@
 * Brief
 * Description	: transform TEXT array into a list of Value
 * input		: TEXT Array
 * return value	: List of Value
 * Note			:
 */
List* untransformPartitionBoundary(Datum options)
{
    List* result = NIL;
    ArrayType* array = NULL;
    Datum* optiondatums = NULL;
    int noptions;
    int i;
    bool* isnull = NULL;

    /* Nothing to do if no options */
    if (!PointerIsValid(DatumGetPointer(options))) {
        return result;
    }

    array = DatumGetArrayTypeP(options);

    Assert(ARR_ELEMTYPE(array) == TEXTOID);

    deconstruct_array(array, TEXTOID, -1, false, 'i', &optiondatums, &isnull, &noptions);

    for (i = 0; i < noptions; i++) {
        char* s = NULL;

        if (!isnull[i]) {
            s = TextDatumGetCString(optiondatums[i]);
        } else {
            s = NULL;
        }
        result = lappend(result, makeString(s));
    }

    pfree_ext(isnull);

    return result;
}
