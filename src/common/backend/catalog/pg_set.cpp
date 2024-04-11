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
 * pg_set.cpp
 *	  routines to support manipulation of the pg_set relation
 *
 *
 *
 * IDENTIFICATION
 *	  src/common/backend/catalog/pg_set.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"
#include "access/genam.h"
#include "access/heapam.h"
#include "catalog/catalog.h"
#include "catalog/indexing.h"
#include "catalog/pg_set.h"
#include "catalog/pg_type.h"
#include "miscadmin.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/syscache.h"
#include "utils/snapmgr.h"
#include "catalog/gs_collation.h"

static void checkSetLableValue(char *label)
{
    /* character ',' is used as delimeter */
    if (strchr(label, ',')) {
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_NAME),
            errmsg("Illegal set '%s' found during parsing", label),
            errdetail("Set value must not contain ','")));
    }

    /* character length can not over 255 */
    text *text_label = cstring_to_text(label);
    if (text_length(PointerGetDatum(text_label)) > SETNAMELEN) {
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_NAME),
            errmsg("Too long set value for column"),
            errdetail("Set value must contain 0 to %d characters.", SETNAMELEN)));
    }
}

/*
 * SetValuesCreate
 *		Create an entry in pg_set for each of the supplied set values.
 *
 * vals is a list of Value strings.
 */
void SetValuesCreate(Oid setTypeOid, List* vals, Oid collation)
{
    Relation pg_set = NULL;
    text *setlabel = NULL;
    Oid* oids = NULL;
    int1 elemno;
    int num_elems;
    Datum values[Natts_pg_set];
    bool nulls[Natts_pg_set];
    ListCell* lc = NULL;
    HeapTuple tup = NULL;

    num_elems = list_length(vals);
    if (num_elems > SETLABELNUM) {
        /* empty set has checked before */
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                errmsg("Too many strings for SET column"),
                errdetail("SET must contain 1 to %d strings.", SETLABELNUM)));
    }
    /*
     * We do not bother to check the list of values for duplicates --- if you
     * have any, you'll get a less-than-friendly unique-index violation. It is
     * probably not worth trying harder.
     */

    check_duplicate_value_by_collation(vals, collation, TYPTYPE_SET);

    pg_set = heap_open(SetRelationId, RowExclusiveLock);

    /*
     * Allocate OIDs for the set's members.
     *
     * While this method does not absolutely guarantee that we generate no
     * duplicate OIDs (since we haven't entered each oid into the table before
     * allocating the next), trouble could only occur if the OID counter wraps
     * all the way around before we finish. Which seems unlikely.
     */
    oids = (Oid*)palloc(num_elems * sizeof(Oid));

    for (elemno = 0; elemno < num_elems; elemno++) {
        /*
         * We assign even-numbered OIDs to all the new set labels.  This
         * tells the comparison functions the OIDs are in the correct sort
         * order and can be compared directly.
         */
        Oid new_oid;

        do {
            new_oid = GetNewOid(pg_set);
        } while (new_oid & 1);
        oids[elemno] = new_oid;
    }

    /* sort them, just in case OID counter wrapped from high to low */
    qsort(oids, num_elems, sizeof(Oid), oid_cmp);

    /* and make the entries */
    for (int i = 0; i < Natts_pg_set; i++) {
        nulls[i] = false;
    }

    elemno = 0;
    foreach (lc, vals) {
        char* lab = strVal(lfirst(lc));
        /*
         * labels are stored in a name field, for easier syscache lookup, so
         * check the length to make sure it's within range.
         */
        checkSetLableValue(lab);
        values[Anum_pg_set_settypid - 1] = ObjectIdGetDatum(setTypeOid);
        values[Anum_pg_set_setnum - 1] = Int8GetDatum(num_elems);
        values[Anum_pg_set_setsortorder - 1] = Int8GetDatum(elemno);
        setlabel = cstring_to_text(lab);
        /* trim the right space for set label */
        values[Anum_pg_set_setlabel - 1] = DirectFunctionCall1(rtrim1, PointerGetDatum(setlabel));

        tup = heap_form_tuple(RelationGetDescr(pg_set), values, nulls);
        HeapTupleSetOid(tup, oids[elemno]);

        (void)simple_heap_insert(pg_set, tup);
        CatalogUpdateIndexes(pg_set, tup);
        heap_freetuple_ext(tup);
        pfree_ext(setlabel);
        elemno++;
    }

    /* clean up */
    pfree_ext(oids);
    heap_close(pg_set, RowExclusiveLock);
}

/*
 * SetValuesDelete
 *		Remove all the pg_set entries for the specified set type.
 */
void SetValuesDelete(Oid setTypeOid)
{
    Relation pg_set = NULL;
    ScanKeyData key[1];
    SysScanDesc scan = NULL;
    HeapTuple tup = NULL;

    pg_set = heap_open(SetRelationId, RowExclusiveLock);

    ScanKeyInit(&key[0], Anum_pg_set_settypid, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(setTypeOid));

    scan = systable_beginscan(pg_set, SetTypIdLabelIndexId, true, NULL, 1, key);

    while (HeapTupleIsValid(tup = systable_getnext(scan))) {
        simple_heap_delete(pg_set, &tup->t_self);
    }

    systable_endscan(scan);

    heap_close(pg_set, RowExclusiveLock);
}

/* GetSetDefineStr
 * output the 'SET(a,b,c,d)' type text by set type oid
 */
Datum GetSetDefineStr(Oid settypid)
{
    bool needdelimt = false;
    Relation pg_set = NULL;
    Relation set_idx = NULL;
    ScanKeyData key;
    SysScanDesc scan = NULL;
    HeapTuple tup = NULL;
    bool isnull = true;
    char* label = NULL;
    text* setlabel = NULL;
    text* result = NULL;

    StringInfoData buf;
    initStringInfo(&buf);
    appendStringInfoString(&buf, "SET(");

    pg_set = heap_open(SetRelationId, AccessShareLock);
    set_idx = index_open(SetTypIdOrderIndexId, AccessShareLock);

    ScanKeyInit(&key, Anum_pg_set_settypid, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(settypid));
    scan = systable_beginscan_ordered(pg_set, set_idx, GetTransactionSnapshot(), 1, &key);

    while (HeapTupleIsValid(tup = systable_getnext_ordered(scan, ForwardScanDirection))) {
        setlabel = (text*)heap_getattr(tup, Anum_pg_set_setlabel, pg_set->rd_att, &isnull);
        label = text_to_cstring(setlabel);
        
        if (needdelimt) {
            appendStringInfoString(&buf, ", ");
        } else {
            needdelimt = true;
        }

        appendStringInfoString(&buf, "'");
        appendStringInfoString(&buf, label);
        appendStringInfoString(&buf, "'");
    }

    appendStringInfoString(&buf, ")");

    systable_endscan_ordered(scan);
    index_close(set_idx, AccessShareLock);
    heap_close(pg_set, AccessShareLock);

    result = cstring_to_text_with_len(buf.data, buf.len);
    FreeStringInfo(&buf);

    PG_RETURN_TEXT_P(result);
}
