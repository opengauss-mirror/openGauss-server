
/*
 * Copyright (c) 2022 Huawei Technologies Co.,Ltd.
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
 * subtype.cpp
 *	  I/O functions for subtypes
 *
 *  SUBTYPE is only suport in PLpgSQL, The syntax for defining a subtype is
 *  { precision [, scale ] } [ NOT NULL ]
 *  The output functions for a subtype are just the same one provided by its
 *  underlying base type. The input functions, however, must be prepare to
 *  apply any constaints defined by the type. So, we create special input
 *  functions that invoke the base type's input funtions and the check the
 *  constraints.
 *
 * IDENTIFICATION
 *	  src/backend/utils/adt/subtype.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "commands/typecmds.h"
#include "executor/executor.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"

/*
 * struct to cache state across ultiple calls
 */
typedef struct SubtypeIOData {
    Oid domain_type;
    /* needed to call basetype's input function */
    Oid typiofunc;
    Oid typioparam;
    int32 typtypmod;
    FmgrInfo proc;
    bool isNotNull;
    /* memory context this cache is in */
    MemoryContext mcxt;
} SubtypeIOData;

static void subtype_state_setup(SubtypeIOData* myExtra, Oid subType, bool binary, MemoryContext mcxt);
static void subtype_check_input(int32 value, bool isnull, SubtypeIOData* myExtra);

Datum subtype_in(PG_FUNCTION_ARGS)
{
    char* string = NULL;
    Oid subType;
    SubtypeIOData* myExtra = NULL;
    Datum value;
    int32 typmod;
    if (PG_ARGISNULL(0)) {
        string = NULL;
    } else {
        string = PG_GETARG_CSTRING(0);
    }
    if (PG_ARGISNULL(1)) {
        PG_RETURN_NULL();
    }
    subType = PG_GETARG_OID(1);
    typmod = PG_GETARG_INT32(2);
    
    /* Try to get cache , if it fails, cache the required information */
    myExtra = (SubtypeIOData*)fcinfo->flinfo->fn_extra;
    if (myExtra == NULL) {
        myExtra = (SubtypeIOData*)MemoryContextAlloc(fcinfo->flinfo->fn_mcxt, sizeof(SubtypeIOData));
        subtype_state_setup((myExtra), subType, false, fcinfo->flinfo->fn_mcxt);
        fcinfo->flinfo->fn_extra = (void*)myExtra;
    }
    Assert(myExtra != NULL);

    /* Invoke the base type's typinput procedure to convert the data */
    typmod = (typmod != -1) ? typmod : myExtra->typtypmod;
    value = InputFunctionCall(&myExtra->proc, string, myExtra->typioparam, typmod);

    /* Do the necessary checks */
    subtype_check_input(Int32GetDatum(value), (string == NULL), myExtra);
    
    if (string == NULL) {
        PG_RETURN_NULL();
    } else {
        PG_RETURN_DATUM(value);
    }
}

/*
 * subtype_recv     - binary input routine for any subtype
 */
Datum subtype_recv(PG_FUNCTION_ARGS)
{
    StringInfo buf;
    Oid subType;
    SubtypeIOData* myExtra;
    Datum value;
    int32 typmod;

    if (PG_ARGISNULL(0)) {
        buf = NULL;
    } else {
        buf =(StringInfo)PG_GETARG_POINTER(0);
    }
    if (PG_ARGISNULL(1)) {
        PG_RETURN_NULL();
    }
    subType = PG_GETARG_OID(1);
    /*
     * Use basetype's typmod in pg_type or this
     * This typmod is use to support bounded precision ranges
     */
    typmod = PG_GETARG_INT32(2);

    /* Try to get the cache, if it fails, cache the required information */
    myExtra = (SubtypeIOData*)fcinfo->flinfo->fn_extra;
    if (myExtra == NULL) {
        myExtra = (SubtypeIOData*)MemoryContextAlloc(fcinfo->flinfo->fn_mcxt, sizeof(SubtypeIOData));
        subtype_state_setup(myExtra, subType, false, fcinfo->flinfo->fn_mcxt);
        fcinfo->flinfo->fn_extra = (void*)myExtra;
    }
    Assert(myExtra != NULL);

    /* Invoke the base type's typinput procedure to convert the data. */
    typmod = (typmod != -1) ? typmod : myExtra->typtypmod;
    value = ReceiveFunctionCall(&myExtra->proc, buf, myExtra->typioparam, typmod);

    /* Do the necessary checks */
    subtype_check_input(Int32GetDatum(value), (buf == NULL), myExtra);

    if (buf == NULL) {
        PG_RETURN_NULL();
    } else {
        PG_RETURN_DATUM(value);
    }
}

/*
 * subtype_state_setup - initialize the cache for a new subtype
 */
static void subtype_state_setup(SubtypeIOData* myExtra, Oid subType, bool binary, MemoryContext mcxt)
{
    Oid baseType;

    /* Mark cache invalied */
    myExtra->domain_type = InvalidOid;

    /* Find out the base type */
    myExtra->typtypmod = -1;
    baseType = getBaseTypeAndTypmod(subType, &myExtra->typtypmod);
    if (baseType == subType) {
        ereport(
            ERROR,
            (errcode(ERRCODE_DATATYPE_MISMATCH),
              errmsg("type %s is not a subtype", format_type_be(subType))
            ));
    }

    /* Look up underlying I/O function */
    if (binary) {
        getTypeBinaryInputInfo(baseType, &myExtra->typiofunc, &myExtra->typioparam);
    } else {
        getTypeInputInfo(baseType, &myExtra->typiofunc, &myExtra->typioparam);
    }

    fmgr_info_cxt(myExtra->typiofunc, &myExtra->proc, mcxt);

    /* NOT NULL constraints are store in the pg_type */
    HeapTuple tup = SearchSysCache1(TYPEOID, ObjectIdGetDatum(subType));
    Form_pg_type typTup = (Form_pg_type)GETSTRUCT(tup);
    myExtra->isNotNull = typTup->typnotnull;
    ReleaseSysCache(tup);

    myExtra->mcxt = mcxt;

    /* Mark cache valid */
    myExtra->domain_type = subType;
}

/*
 * subtype_check_input - check constraints
 */
static void subtype_check_input(int32 value, bool isnull, SubtypeIOData* myExtra)
{
    if (isnull) {
        if (myExtra->isNotNull) {
            ereport(ERROR, (errcode(ERRCODE_NOT_NULL_VIOLATION),
                errmsg("value of subtype %s can not be null", format_type_be(myExtra->domain_type))));
        }
        return;
    }
}
