/* -------------------------------------------------------------------------
 *
 * tupdesc.cpp
 *	  openGauss tuple descriptor support code
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2021, openGauss Contributors
 *
 *
 * IDENTIFICATION
 *	  src/gausskernel/storage/access/common/tupdesc.cpp
 *
 * NOTES
 *	  some of the executor utility code such as "ExecTypeFromTL" should be
 *	  moved here.
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/transam.h"
#include "catalog/dependency.h"
#include "catalog/pg_type.h"
#include "miscadmin.h"
#include "parser/parse_type.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/rel.h"
#include "utils/rel_gs.h"
#include "utils/resowner.h"
#include "utils/syscache.h"
#include "pgxc/pgxc.h"
#include "utils/lsyscache.h"

/*
 * CreateTemplateTupleDesc
 *		This function allocates an empty tuple descriptor structure.
 *
 * Tuple type ID information is initially set for an anonymous record type;
 * caller can overwrite this if needed.
 */
TupleDesc CreateTemplateTupleDesc(int natts, bool hasoid, TableAmType tam)
{
    TupleDesc desc;
    char *stg = NULL;
    int attroffset;

    /*
     * sanity checks
     */
    AssertArg(natts >= 0);

    /*
     * Allocate enough memory for the tuple descriptor, including the
     * attribute rows, and set up the attribute row pointers.
     *
     * Note: we assume that sizeof(struct tupleDesc) is a multiple of the
     * struct pointer alignment requirement, and hence we don't need to insert
     * alignment padding between the struct and the array of attribute row
     * pointers.
     *
     * Note: Only the fixed part of pg_attribute rows is included in tuple
     * descriptors, so we only need ATTRIBUTE_FIXED_PART_SIZE space per attr.
     * That might need alignment padding, however.
     */
    attroffset = sizeof(struct tupleDesc) + natts * sizeof(Form_pg_attribute);
    attroffset = MAXALIGN((uint32)attroffset);
    stg = (char *)palloc0(attroffset + natts * MAXALIGN(ATTRIBUTE_FIXED_PART_SIZE));
    desc = (TupleDesc)stg;

    if (natts > 0) {
        Form_pg_attribute *attrs = NULL;
        int i;

        attrs = (Form_pg_attribute *)(stg + sizeof(struct tupleDesc));
        desc->attrs = attrs;
        stg += attroffset;
        for (i = 0; i < natts; i++) {
            attrs[i] = (Form_pg_attribute)stg;
            stg += MAXALIGN(ATTRIBUTE_FIXED_PART_SIZE);
        }
    } else {
        desc->attrs = NULL;
    }

    /*
     * Initialize other fields of the tupdesc.
     */
    desc->natts = natts;
    desc->constr = NULL;
    desc->tdtypeid = RECORDOID;
    desc->tdtypmod = -1;
    desc->tdhasoid = hasoid;
    desc->tdrefcount = -1;    /* assume not reference-counted */
    desc->initdefvals = NULL; /* initialize the attrinitdefvals */
    desc->tdhasuids = false;
    desc->tdisredistable = false;
    desc->tdTableAmType = tam;

    return desc;
}

/*
 * CreateTupleDesc
 *		This function allocates a new TupleDesc pointing to a given
 *		Form_pg_attribute array.
 *
 * Note: if the TupleDesc is ever freed, the Form_pg_attribute array
 * will not be freed thereby.
 *
 * Tuple type ID information is initially set for an anonymous record type;
 * caller can overwrite this if needed.
 */
TupleDesc CreateTupleDesc(int natts, bool hasoid, Form_pg_attribute* attrs, TableAmType tam)
{
    TupleDesc desc;

    /*
     * sanity checks
     */
    AssertArg(natts >= 0);

    desc = (TupleDesc)palloc(sizeof(struct tupleDesc));
    desc->attrs = attrs;
    desc->natts = natts;
    desc->constr = NULL;
    desc->tdtypeid = RECORDOID;
    desc->tdtypmod = -1;
    desc->tdhasoid = hasoid;
    desc->tdrefcount = -1;    /* assume not reference-counted */
    desc->initdefvals = NULL; /* initialize the attrinitdefvals */
    desc->tdisredistable = false;
    desc->tdhasuids = false;
    desc->tdTableAmType = tam;

    return desc;
}

/*
 * tupInitDefValCopy
 *		This function creates a new TupInitDefVal by copying from an existing
 *		TupInitDefVal.
 */
TupInitDefVal *tupInitDefValCopy(TupInitDefVal *pInitDefVal, int nAttr)
{
    TupInitDefVal *dvals = (TupInitDefVal *)palloc(nAttr * sizeof(TupInitDefVal));
    for (int i = 0; i < nAttr; ++i) {
        dvals[i].isNull = pInitDefVal[i].isNull;
        dvals[i].dataLen = pInitDefVal[i].dataLen;
        dvals[i].datum = NULL;

        if (!dvals[i].isNull) {
            char *data = (char *)palloc(dvals[i].dataLen);
            MemCpy(data, pInitDefVal[i].datum, dvals[i].dataLen);
            dvals[i].datum = (Datum *)data;
        }
    }
    return dvals;
}

/*
 * CreateTupleDescCopy
 *		This function creates a new TupleDesc by copying from an existing
 *		TupleDesc.
 *
 * !!! Constraints and defaults are not copied !!!
 */
TupleDesc CreateTupleDescCopy(TupleDesc tupdesc)
{
    TupleDesc desc;
    int i;
    errno_t rc = EOK;

    desc = CreateTemplateTupleDesc(tupdesc->natts, tupdesc->tdhasoid, tupdesc->tdTableAmType);

    for (i = 0; i < desc->natts; i++) {
        rc = memcpy_s(desc->attrs[i], ATTRIBUTE_FIXED_PART_SIZE, tupdesc->attrs[i], ATTRIBUTE_FIXED_PART_SIZE);
        securec_check(rc, "\0", "\0");
        desc->attrs[i]->attnotnull = false;
        desc->attrs[i]->atthasdef = false;
    }

    desc->tdtypeid = tupdesc->tdtypeid;
    desc->tdtypmod = tupdesc->tdtypmod;
    desc->tdisredistable = tupdesc->tdisredistable;
    desc->tdhasuids = tupdesc->tdhasuids;

    /* copy the attinitdefval */
    if (tupdesc->initdefvals) {
        desc->initdefvals = tupInitDefValCopy(tupdesc->initdefvals, tupdesc->natts);
    }

    return desc;
}

/*
 * TupleConstrCopy
 *    This function creates a new TupleConstr by copying from an existing TupleConstr.
 */
TupleConstr *TupleConstrCopy(const TupleDesc tupdesc)
{
    TupleConstr *constr = tupdesc->constr;
    TupleConstr *cpy = (TupleConstr *)palloc0(sizeof(TupleConstr));

    cpy->has_not_null = constr->has_not_null;
    cpy->has_generated_stored = constr->has_generated_stored;

    errno_t rc = EOK;
    if ((cpy->num_defval = constr->num_defval) > 0) {
        cpy->defval = (AttrDefault *)palloc(cpy->num_defval * sizeof(AttrDefault));
        rc = memcpy_s(cpy->defval, cpy->num_defval * sizeof(AttrDefault), constr->defval,
            cpy->num_defval * sizeof(AttrDefault));
        securec_check(rc, "\0", "\0");
        for (int i = cpy->num_defval - 1; i >= 0; i--) {
            if (constr->defval[i].adbin) {
                cpy->defval[i].adbin = pstrdup(constr->defval[i].adbin);
            }
        }
        uint32 genColsLen = (uint32)tupdesc->natts * sizeof(char);
        cpy->generatedCols = (char *)palloc(genColsLen);
        rc = memcpy_s(cpy->generatedCols, genColsLen, constr->generatedCols, genColsLen);
        securec_check(rc, "\0", "\0");
    }

    if ((cpy->num_check = constr->num_check) > 0) {
        cpy->check = (ConstrCheck *)palloc(cpy->num_check * sizeof(ConstrCheck));
        rc = memcpy_s(cpy->check, cpy->num_check * sizeof(ConstrCheck), constr->check,
            cpy->num_check * sizeof(ConstrCheck));
        securec_check(rc, "\0", "\0");
        for (int i = cpy->num_check - 1; i >= 0; i--) {
            if (constr->check[i].ccname) {
                cpy->check[i].ccname = pstrdup(constr->check[i].ccname);
            }
            if (constr->check[i].ccbin) {
                cpy->check[i].ccbin = pstrdup(constr->check[i].ccbin);
            }
            cpy->check[i].ccvalid = constr->check[i].ccvalid;
            cpy->check[i].ccnoinherit = constr->check[i].ccnoinherit;
        }
    }
    return cpy;
}

/*
 * CreateTupleDescCopyConstr
 *		This function creates a new TupleDesc by copying from an existing
 *		TupleDesc (including its constraints and defaults).
 */
TupleDesc CreateTupleDescCopyConstr(TupleDesc tupdesc)
{
    errno_t rc = EOK;
    TupleDesc desc = CreateTemplateTupleDesc(tupdesc->natts, tupdesc->tdhasoid, tupdesc->tdTableAmType);

    for (int i = 0; i < desc->natts; i++) {
        rc = memcpy_s(desc->attrs[i], ATTRIBUTE_FIXED_PART_SIZE, tupdesc->attrs[i], ATTRIBUTE_FIXED_PART_SIZE);
        securec_check(rc, "\0", "\0");
    }

    if (tupdesc->constr != NULL) {
        desc->constr = TupleConstrCopy(tupdesc);
    }

    /* copy the attinitdefval */
    if (tupdesc->initdefvals) {
        desc->initdefvals = tupInitDefValCopy(tupdesc->initdefvals, tupdesc->natts);
    }

    desc->tdtypeid = tupdesc->tdtypeid;
    desc->tdtypmod = tupdesc->tdtypmod;
    desc->tdisredistable = tupdesc->tdisredistable;

    return desc;
}

char GetGeneratedCol(TupleDesc tupdesc, int atti)
{
    if (!tupdesc->constr)
        return '\0';

    if (tupdesc->constr->num_defval == 0)
        return '\0';

    return tupdesc->constr->generatedCols[atti];
}

/*
 * Free a TupleDesc including all substructure
 */
void FreeTupleDesc(TupleDesc tupdesc)
{
    int i;

    /*
     * Possibly this should assert tdrefcount == 0, to disallow explicit
     * freeing of un-refcounted tupdescs?
     */
    Assert(tupdesc->tdrefcount <= 0);

    if (tupdesc->constr) {
        if (tupdesc->constr->num_defval > 0) {
            AttrDefault *attrdef = tupdesc->constr->defval;

            for (i = tupdesc->constr->num_defval - 1; i >= 0; i--) {
                if (attrdef[i].adbin)
                    pfree(attrdef[i].adbin);
            }
            pfree(attrdef);
            pfree(tupdesc->constr->generatedCols);
        }
        if (tupdesc->constr->num_check > 0) {
            ConstrCheck *check = tupdesc->constr->check;

            for (i = tupdesc->constr->num_check - 1; i >= 0; i--) {
                if (check[i].ccname)
                    pfree(check[i].ccname);
                if (check[i].ccbin)
                    pfree(check[i].ccbin);
            }
            pfree(check);
        }
        pfree_ext(tupdesc->constr->clusterKeys);
        pfree(tupdesc->constr);
        tupdesc->constr = NULL;
    }

    /* free the attinitdefval */
    if (tupdesc->initdefvals) {
        for (i = 0; i < tupdesc->natts; ++i) {
            if (tupdesc->initdefvals[i].datum != NULL)
                pfree_ext(tupdesc->initdefvals[i].datum);
        }
        pfree_ext(tupdesc->initdefvals);
    }

    pfree(tupdesc);
}

/*
 * Increment the reference count of a tupdesc, and log the reference in
 * CurrentResourceOwner.
 *
 * Do not apply this to tupdescs that are not being refcounted.  (Use the
 * macro PinTupleDesc for tupdescs of uncertain status.)
 */
void IncrTupleDescRefCount(TupleDesc tupdesc)
{
    Assert(tupdesc->tdrefcount >= 0);

    ResourceOwnerEnlargeTupleDescs(t_thrd.utils_cxt.CurrentResourceOwner);
    tupdesc->tdrefcount++;
    ResourceOwnerRememberTupleDesc(t_thrd.utils_cxt.CurrentResourceOwner, tupdesc);
}

/*
 * Decrement the reference count of a tupdesc, remove the corresponding
 * reference from CurrentResourceOwner, and free the tupdesc if no more
 * references remain.
 *
 * Do not apply this to tupdescs that are not being refcounted.  (Use the
 * macro ReleaseTupleDesc for tupdescs of uncertain status.)
 */
void DecrTupleDescRefCount(TupleDesc tupdesc)
{
    Assert(tupdesc->tdrefcount > 0);

    ResourceOwnerForgetTupleDesc(t_thrd.utils_cxt.CurrentResourceOwner, tupdesc);
    if (--tupdesc->tdrefcount == 0)
        FreeTupleDesc(tupdesc);
}

/* compare the attinitdefval */
static bool compareInitdefvals(TupleDesc tupdesc1, TupleDesc tupdesc2)
{
    int i;
    Assert(tupdesc1->natts == tupdesc2->natts);

    if (tupdesc1->initdefvals == NULL && tupdesc2->initdefvals == NULL) {
        return true;
    }

    if (tupdesc1->initdefvals != NULL && tupdesc2->initdefvals != NULL) {
        for (i = 0; i < tupdesc1->natts; ++i) {
            if (tupdesc1->initdefvals[i].isNull != tupdesc2->initdefvals[i].isNull) {
                return false;
            }

            if (tupdesc1->initdefvals[i].dataLen != tupdesc2->initdefvals[i].dataLen) {
                return false;
            }

            if (tupdesc1->initdefvals[i].isNull) {
                continue;
            }

            Assert(tupdesc1->initdefvals[i].dataLen > 0);

            if (memcmp(tupdesc1->initdefvals[i].datum, tupdesc2->initdefvals[i].datum,
                       tupdesc1->initdefvals[i].dataLen) != 0) {
                return false;
            }
        }
        return true;
    } else {
        TupInitDefVal *vals = tupdesc1->initdefvals == NULL ? tupdesc2->initdefvals : tupdesc1->initdefvals;

        for (i = 0; i < tupdesc1->natts; i++) {
            if (!vals[i].isNull) {
                return false;
            }
        }
        return true;
    }
}

/*
 * @Description: check whether one relation has Partial Cluster Key.
 * @IN constr: all tuple constraints info about this relation.
 * @Return: true if PCK exists; otherwise, return false.
 * @See also:
 */
bool tupledesc_have_pck(TupleConstr *constr)
{
    return (constr && constr->clusterKeyNum > 0);
}

/*
 * @Description: compare and check whether two PCKs are eqaul to.
 * @Param[IN] tupdesc1: PCK constraint1
 * @Param[IN] tupdesc2: PCK constraint2
 * @Return: true if the two PCK info are equal to; otherwise false.
 * @See also:
 */
static bool comparePartialClusterKeys(TupleConstr *const constr1, TupleConstr *const constr2)
{
    int n = constr1->clusterKeyNum;
    if (n != (int)constr2->clusterKeyNum) {
        return false;
    }

    AttrNumber *pck1 = constr1->clusterKeys;
    AttrNumber *pck2 = constr2->clusterKeys;
    for (int i = 0; i < n; ++i) {
        /* two PCKs are equal to only if pck1[i] == pck[i],
         * which means that they have the same attributes set
         * and the same order.
         */
        if (*pck1 != *pck2) {
            return false;
        } else {
            /* compare next attribute no */
            ++pck1;
            ++pck2;
        }
    }
    return true;
}

/*
 * Compare two TupleDesc structures for logical equality
 *
 * Note: we deliberately do not check the attrelid and tdtypmod fields.
 * This allows typcache.c to use this routine to see if a cached record type
 * matches a requested type, and is harmless for relcache.c's uses.
 * We don't compare tdrefcount, either.
 */
bool equalTupleDescs(TupleDesc tupdesc1, TupleDesc tupdesc2)
{
    int i, j, n;

    if (tupdesc1->natts != tupdesc2->natts) {
        return false;
    }
    if (tupdesc1->tdtypeid != tupdesc2->tdtypeid) {
        return false;
    }
    if (tupdesc1->tdhasoid != tupdesc2->tdhasoid) {
        return false;
    }

    if (tupdesc1->tdisredistable != tupdesc2->tdisredistable) {
        return false;
    }

    if (tupdesc1->tdTableAmType != tupdesc2->tdTableAmType) {
        return false;
    }

    for (i = 0; i < tupdesc1->natts; i++) {
        Form_pg_attribute attr1 = tupdesc1->attrs[i];
        Form_pg_attribute attr2 = tupdesc2->attrs[i];

        /*
         * We do not need to check every single field here: we can disregard
         * attrelid and attnum (which were used to place the row in the attrs
         * array in the first place).  It might look like we could dispense
         * with checking attlen/attbyval/attalign, since these are derived
         * from atttypid; but in the case of dropped columns we must check
         * them (since atttypid will be zero for all dropped columns) and in
         * general it seems safer to check them always.
         *
         * attcacheoff must NOT be checked since it's possibly not set in both
         * copies.
         */
        if (strcmp(NameStr(attr1->attname), NameStr(attr2->attname)) != 0) {
            return false;
        }
        const bool cl_skip = IsClientLogicType(attr1->atttypid) && (Oid)attr1->atttypmod == attr2->atttypid;
        if (attr1->atttypid != attr2->atttypid && !cl_skip) {
            return false;
        }
        if (attr1->attstattarget != attr2->attstattarget) {
            return false;
        }
        if (attr1->attlen != attr2->attlen) {
            return false;
        }
        if (attr1->attndims != attr2->attndims) {
            return false;
        }
        if (attr1->atttypmod != attr2->atttypmod && !cl_skip) {
            return false;
        }
        if (attr1->attbyval != attr2->attbyval) {
            return false;
        }
        if (attr1->attstorage != attr2->attstorage && !cl_skip) {
            return false;
        }
        if (attr1->attkvtype != attr2->attkvtype) {
            return false;
        }
        if (attr1->attcmprmode != attr2->attcmprmode) {
            return false;
        }
        if (attr1->attalign != attr2->attalign) {
            return false;
        }
        if (attr1->attnotnull != attr2->attnotnull) {
            return false;
        }
        if (attr1->atthasdef != attr2->atthasdef) {
            return false;
        }
        if (attr1->attisdropped != attr2->attisdropped) {
            return false;
        }
        if (attr1->attislocal != attr2->attislocal) {
            return false;
        }
        if (attr1->attinhcount != attr2->attinhcount) {
            return false;
        }
        if (attr1->attcollation != attr2->attcollation && !cl_skip) {
            return false;
        }
        /* attacl, attoptions and attfdwoptions are not even present... */
    }

    if (tupdesc1->constr != NULL) {
        TupleConstr *constr1 = tupdesc1->constr;
        TupleConstr *constr2 = tupdesc2->constr;

        if (constr2 == NULL) {
            return false;
        }

        /* check whether has_not_null is equal to. */
        if (constr1->has_not_null != constr2->has_not_null) {
            return false;
        }

        if (constr1->has_generated_stored != constr2->has_generated_stored) {
            return false;
        }

        /* check whether default values are eqaul to. */
        n = constr1->num_defval;
        if (n != (int)constr2->num_defval) {
            return false;
        }
        for (i = 0; i < n; i++) {
            AttrDefault *defval1 = constr1->defval + i;
            AttrDefault *defval2 = constr2->defval;

            /*
             * We can't assume that the items are always read from the system
             * catalogs in the same order; so use the adnum field to identify
             * the matching item to compare.
             */
            for (j = 0; j < n; defval2++, j++) {
                if (defval1->adnum == defval2->adnum) {
                    break;
                }
            }
            if (j >= n) {
                return false;
            }
            if (strcmp(defval1->adbin, defval2->adbin) != 0) {
                return false;
            }
            if (defval1->generatedCol != defval2->generatedCol) {
                return false;
            }
        }

        /* check whether check constraints are equal to. */
        n = constr1->num_check;
        if (n != (int)constr2->num_check) {
            return false;
        }
        for (i = 0; i < n; i++) {
            ConstrCheck *check1 = constr1->check + i;
            ConstrCheck *check2 = constr2->check;

            /*
             * Similarly, don't assume that the checks are always read in the
             * same order; match them up by name and contents. (The name
             * *should* be unique, but...)
             */
            for (j = 0; j < n; check2++, j++) {
                if (strcmp(check1->ccname, check2->ccname) == 0 && strcmp(check1->ccbin, check2->ccbin) == 0 &&
                    check1->ccvalid == check2->ccvalid && check1->ccnoinherit == check2->ccnoinherit) {
                    break;
                }
            }
            if (j >= n) {
                return false;
            }
        }

        /* check whether PCK info is equal to */
        if (!comparePartialClusterKeys(constr1, constr2)) {
            return false;
        }
    } else if (tupdesc2->constr != NULL) {
        return false;
    }

    /* compare the attinitdefval */
    return compareInitdefvals(tupdesc1, tupdesc2);
}

static bool ComparePgAttribute(Form_pg_attribute attr1, Form_pg_attribute attr2)
{
    /*
     * We do not need to check every single field here: we can disregard
     * attrelid and attnum (which were used to place the row in the attrs
     * array in the first place).  It might look like we could dispense
     * with checking attlen/attbyval/attalign, since these are derived
     * from atttypid; but in the case of dropped columns we must check
     * them (since atttypid will be zero for all dropped columns) and in
     * general it seems safer to check them always.
     *
     * attcacheoff must NOT be checked since it's possibly not set in both
     * copies.
     */
    if (strcmp(NameStr(attr1->attname), NameStr(attr2->attname)) != 0)
        return false;
    if (attr1->atttypid != attr2->atttypid)
        return false;
    if (attr1->attstattarget != attr2->attstattarget)
        return false;
    if (attr1->attlen != attr2->attlen)
        return false;
    if (attr1->attndims != attr2->attndims)
        return false;
    if (attr1->atttypmod != attr2->atttypmod)
        return false;
    if (attr1->attbyval != attr2->attbyval)
        return false;
    if (attr1->attstorage != attr2->attstorage)
        return false;
    if (attr1->attkvtype != attr2->attkvtype)
        return false;
    if (attr1->attcmprmode != attr2->attcmprmode)
        return false;
    if (attr1->attalign != attr2->attalign)
        return false;
    if (attr1->attnotnull != attr2->attnotnull)
        return false;
    if (attr1->atthasdef != attr2->atthasdef)
        return false;
    
    if (attr1->attisdropped != attr2->attisdropped)
        return false;
    if (attr1->attislocal != attr2->attislocal)
        return false;
    if (attr1->attinhcount != attr2->attinhcount)
        return false;
    if (attr1->attcollation != attr2->attcollation)
        return false;
    /* attacl, attoptions and attfdwoptions are not even present... */

    return true;
}

/*
 * Compare the delta TupleDesc structures with column store main TupleDesc
 *
 * Note: we don't compare tdtypeid, constraints and pck.
 */
bool equalDeltaTupleDescs(TupleDesc mainTupdesc, TupleDesc deltaTupdesc)
{
    int i;

    if (mainTupdesc->natts != deltaTupdesc->natts)
        return false;

    for (i = 0; i < mainTupdesc->natts; i++) {
        Form_pg_attribute attr1 = mainTupdesc->attrs[i];
        Form_pg_attribute attr2 = deltaTupdesc->attrs[i];

        if (GetGeneratedCol(mainTupdesc, i) != GetGeneratedCol(deltaTupdesc, i)) {
            return false;
        }

        if (!ComparePgAttribute(attr1, attr2)) {
            return false;
        }
    }

    /* compare the attinitdefval */
    return compareInitdefvals(mainTupdesc, deltaTupdesc);
}

/*
 * TupleDescInitEntry
 *		This function initializes a single attribute structure in
 *		a previously allocated tuple descriptor.
 *
 * If attributeName is NULL, the attname field is set to an empty string
 * (this is for cases where we don't know or need a name for the field).
 * Also, some callers use this function to change the datatype-related fields
 * in an existing tupdesc; they pass attributeName = NameStr(att->attname)
 * to indicate that the attname field shouldn't be modified.
 *
 * Note that attcollation is set to the default for the specified datatype.
 * If a nondefault collation is needed, insert it afterwards using
 * TupleDescInitEntryCollation.
 */
void TupleDescInitEntry(TupleDesc desc, AttrNumber attributeNumber, const char *attributeName, Oid oidtypeid,
                        int32 typmod, int attdim)
{
    HeapTuple tuple;
    Form_pg_type typeForm;
    Form_pg_attribute att;

    /*
     * sanity checks
     */
    AssertArg(PointerIsValid(desc));
    AssertArg(attributeNumber >= 1);
    AssertArg(attributeNumber <= desc->natts);

    /*
     * initialize the attribute fields
     */
    att = desc->attrs[attributeNumber - 1];

    att->attrelid = 0; /* dummy value */

    /*
     * Note: attributeName can be NULL, because the planner doesn't always
     * fill in valid resname values in targetlists, particularly for resjunk
     * attributes. Also, do nothing if caller wants to re-use the old attname.
     */
    if (attributeName == NULL) {
        errno_t rc = memset_s(NameStr(att->attname), NAMEDATALEN, 0, NAMEDATALEN);
        securec_check(rc, "\0", "\0");
    } else if (attributeName != NameStr(att->attname)) {
        namestrcpy(&(att->attname), attributeName);
    }

    att->attstattarget = -1;
    att->attcacheoff = -1;
    att->atttypmod = typmod;

    att->attnum = attributeNumber;
    att->attndims = attdim;

    att->attnotnull = false;
    att->atthasdef = false;
    att->attisdropped = false;
    att->attislocal = true;
    att->attinhcount = 0;
    att->attkvtype = ATT_KV_UNDEFINED;     /* not specified */
    att->attcmprmode = ATT_CMPR_UNDEFINED; /* not specified */
    /* attacl, attoptions and attfdwoptions are not present in tupledescs */
    tuple = SearchSysCache1(TYPEOID, ObjectIdGetDatum(oidtypeid));
    if (!HeapTupleIsValid(tuple))
        ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for type %u", oidtypeid)));
    typeForm = (Form_pg_type)GETSTRUCT(tuple);

    att->atttypid = oidtypeid;
    att->attlen = typeForm->typlen;
    att->attbyval = typeForm->typbyval;
    att->attalign = typeForm->typalign;
    att->attstorage = typeForm->typstorage;
    att->attcollation = typeForm->typcollation;

    ReleaseSysCache(tuple);
}

/*
 * TupleDescInitEntryCollation
 *
 * Assign a nondefault collation to a previously initialized tuple descriptor
 * entry.
 */
void TupleDescInitEntryCollation(TupleDesc desc, AttrNumber attributeNumber, Oid collationid)
{
    /*
     * sanity checks
     */
    AssertArg(PointerIsValid(desc));
    AssertArg(attributeNumber >= 1);
    AssertArg(attributeNumber <= desc->natts);

    desc->attrs[attributeNumber - 1]->attcollation = collationid;
}

/*
 * VerifyAttrCompressMode
 * verify the specified compress mode for one attribute
 */
void VerifyAttrCompressMode(int8 mode, int attlen, const char *attname)
{
    char *errinfo = NULL;
    switch (mode) {
        case ATT_CMPR_DELTA: {
            if (attlen > 0 && attlen <= 8)
                return;

            errinfo = "DELTA";
            break;
        }

        case ATT_CMPR_PREFIX: {
            if (-1 == attlen || -2 == attlen)
                return;

            errinfo = "PREFIX";
            break;
        }

        case ATT_CMPR_NUMSTR: {
            if (-1 == attlen || -2 == attlen)
                return;

            errinfo = "NUMSTR";
            break;
        }

        default:
            return;
    }

    ereport(ERROR, (errcode(ERRCODE_INVALID_TABLE_DEFINITION),
                    errmsg("column \"%s\" cannot be applied %s compress mode", attname, errinfo)));
}

void copyDroppedAttribute(Form_pg_attribute target, Form_pg_attribute source)
{
    char *attributeName = NameStr(source->attname);

    target->attrelid = 0;
    namestrcpy(&(target->attname), attributeName);
    target->atttypid = source->atttypid;
    target->attstattarget = source->attstattarget;
    target->attlen = source->attlen;
    target->attnum = source->attnum;
    target->attndims = source->attndims;
    target->attcacheoff = source->attcacheoff;
    target->atttypmod = source->atttypmod;
    target->attbyval = source->attbyval;
    target->attstorage = source->attstorage;
    target->attalign = source->attalign;
    target->attnotnull = source->attnotnull;
    target->atthasdef = source->atthasdef;
    target->attisdropped = source->attisdropped;
    target->attislocal = source->attislocal;
    target->attkvtype = source->attkvtype;
    target->attcmprmode = source->attcmprmode;
    target->attinhcount = source->attinhcount;
    target->attcollation = source->attcollation;
}

int UpgradeAdaptAttr(Oid atttypid, ColumnDef *entry)
{
    int attdim;
    if (u_sess->attr.attr_common.upgrade_mode != 0 &&
        OidIsValid(u_sess->upg_cxt.Inplace_upgrade_next_heap_pg_class_oid) &&
        u_sess->upg_cxt.Inplace_upgrade_next_heap_pg_class_oid < FirstBootstrapObjectId) {
        char typcategory;
        bool type_tmp = false;
        get_type_category_preferred(atttypid, &typcategory, &type_tmp);
        attdim = (typcategory == 'A') ? 1 : 0;
    } else {
        attdim = list_length(entry->typname->arrayBounds);
    }
    return attdim;
}

static void BlockRowCompressRelOption(const char *tableFormat, const ColumnDef *entry)
{
    if (pg_strcasecmp(ORIENTATION_ROW, tableFormat) == 0 &&  ATT_CMPR_NOCOMPRESS < entry->cmprs_mode &&
        entry->cmprs_mode <= ATT_CMPR_NUMSTR) {
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_TABLE_DEFINITION),
             errmsg("row-oriented table does not support compression")));
    }
}

static void BlockColumnRelOption(const char *tableFormat, const Oid atttypid, const int32 atttypmod)
{
    if (((pg_strcasecmp(ORIENTATION_COLUMN, tableFormat)) == 0 ||
        (pg_strcasecmp(ORIENTATION_TIMESERIES, tableFormat)) == 0) &&
        !IsTypeSupportedByCStore(atttypid)) {
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("type \"%s\" is not supported in column store", format_type_with_typemod(atttypid, atttypmod))));
    }
}

static void BlockORCRelOption(const char *tableFormat, const Oid atttypid, const int32 atttypmod)
{
    if ((pg_strcasecmp(ORIENTATION_ORC, tableFormat) == 0) && !IsTypeSupportedByORCRelation(atttypid)) {
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("type \"%s\" is not supported in DFS ORC format column store",
                               format_type_with_typemod(atttypid, atttypmod))));
    }
}

/*
 * BuildDescForRelation
 *
 * Given a relation schema (list of ColumnDef nodes), build a TupleDesc.
 *
 * Note: the default assumption is no OIDs; caller may modify the returned
 * TupleDesc if it wants OIDs.	Also, tdtypeid will need to be filled in
 * later on.
 */
TupleDesc BuildDescForRelation(List *schema, Node *orientedFrom, char relkind)
{
    int natts;
    AttrNumber attnum = 0;
    ListCell *l = NULL;
    TupleDesc desc;
    bool has_not_null = false;
    char *attname = NULL;
    Oid atttypid;
    int32 atttypmod;
    Oid attcollation;
    int attdim;
    const char *tableFormat = "";

    if (orientedFrom != NULL) {
        tableFormat = strVal(orientedFrom);
    }

    /*
     * allocate a new tuple descriptor
     */
    natts = list_length(schema);
    desc = CreateTemplateTupleDesc(natts, false, TAM_HEAP);

    foreach (l, schema) {
        ColumnDef *entry = (ColumnDef *)lfirst(l);
        AclResult aclresult;

        /*
         * for each entry in the list, get the name and type information from
         * the list and have TupleDescInitEntry fill in the attribute
         * information we need.
         */
        attnum++;

        if (u_sess->attr.attr_sql.enable_cluster_resize && entry->dropped_attr != NULL) {
            copyDroppedAttribute(desc->attrs[attnum - 1], entry->dropped_attr);
            continue;
        }
        attname = entry->colname;

        typenameTypeIdAndMod(NULL, entry->typname, &atttypid, &atttypmod);
#ifndef ENABLE_MULTIPLE_NODES
    /* don't allow package or procedure type as column type */
    if (u_sess->plsql_cxt.curr_compile_context == NULL && IsPackageDependType(atttypid, InvalidOid)) {
        ereport(ERROR,
            (errcode(ERRCODE_DATATYPE_MISMATCH),
                errmodule(MOD_PLSQL),
                errmsg("type \"%s\" is not supported as column type", TypeNameToString(entry->typname)),
                errdetail("\"%s\" is a package or procedure type", TypeNameToString(entry->typname)),
                errcause("feature not supported"),
                erraction("check type name")));
    }
#endif
        if (relkind == RELKIND_COMPOSITE_TYPE && IS_PGXC_COORDINATOR) {
            HeapTuple typTupe;
            Form_pg_type typForm;

            typTupe = SearchSysCache1(TYPEOID, ObjectIdGetDatum(atttypid));
            if (!HeapTupleIsValid(typTupe)) {
                ereport(ERROR,
                        (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("cache lookup failed for type  %u", atttypid)));
            }
            typForm = (Form_pg_type)GETSTRUCT(typTupe);
            if (typForm->typrelid >= FirstNormalObjectId && typForm->typtype == TYPTYPE_COMPOSITE) {
                HeapTuple relTupe = SearchSysCache1(RELOID, ObjectIdGetDatum(typForm->typrelid));
                Form_pg_class relForm;
                if (HeapTupleIsValid(relTupe)) {
                    relForm = (Form_pg_class)GETSTRUCT(relTupe);
                    bool flag = relForm->relkind == RELKIND_VIEW || relForm->relkind == RELKIND_CONTQUERY;
                    if (flag) {
                        ereport(ERROR,
                                (errcode(ERRCODE_UNDEFINED_OBJECT),
                                 errmsg("the attribute column of user defined CompositeType does not support view type "
                                        "\"%s\" ",
                                        TypeNameToString(entry->typname))));
                    }
                }
                ReleaseSysCache(relTupe);
            }
            ReleaseSysCache(typTupe);
        }

        aclresult = pg_type_aclcheck(atttypid, GetUserId(), ACL_USAGE);
        if (aclresult != ACLCHECK_OK)
            aclcheck_error_type(aclresult, atttypid);

        attcollation = GetColumnDefCollation(NULL, entry, atttypid);
        attdim = UpgradeAdaptAttr(atttypid, entry);

        BlockColumnRelOption(tableFormat, atttypid, atttypmod);
        BlockORCRelOption(tableFormat, atttypid, atttypmod);

        if (entry->typname->setof)
            ereport(ERROR, (errcode(ERRCODE_INVALID_TABLE_DEFINITION),
                            errmsg("column \"%s\" cannot be declared SETOF", attname)));

        TupleDescInitEntry(desc, attnum, attname, atttypid, atttypmod, attdim);

        /* Override TupleDescInitEntry's settings as requested */
        TupleDescInitEntryCollation(desc, attnum, attcollation);
        if (entry->storage)
            desc->attrs[attnum - 1]->attstorage = entry->storage;

        /* Fill in additional stuff not handled by TupleDescInitEntry */
        desc->attrs[attnum - 1]->attnotnull = entry->is_not_null;
        /*
         * PG source code: has_not_null |= entry->is_not_null;
         */
        has_not_null = has_not_null || entry->is_not_null;
        desc->attrs[attnum - 1]->attislocal = entry->is_local;
        desc->attrs[attnum - 1]->attinhcount = entry->inhcount;

        Form_pg_attribute thisatt = desc->attrs[attnum - 1];
        thisatt->attkvtype = entry->kvtype;
        VerifyAttrCompressMode(entry->cmprs_mode, thisatt->attlen, attname);
        thisatt->attcmprmode = entry->cmprs_mode;

        BlockRowCompressRelOption(tableFormat, entry);
    }

    if (has_not_null) {
        TupleConstr *constr = (TupleConstr *)palloc0(sizeof(TupleConstr));

        constr->has_not_null = true;
        constr->defval = NULL;
        constr->num_defval = 0;
        constr->check = NULL;
        constr->num_check = 0;
        constr->generatedCols = NULL;
        desc->constr = constr;
    } else {
        desc->constr = NULL;
    }

    return desc;
}

/*
 * BuildDescFromLists
 *
 * Build a TupleDesc given lists of column names (as String nodes),
 * column type OIDs, typmods, and collation OIDs.
 *
 * No constraints are generated.
 *
 * This is essentially a cut-down version of BuildDescForRelation for use
 * with functions returning RECORD.
 */
TupleDesc BuildDescFromLists(List *names, List *types, List *typmods, List *collations)
{
    int natts;
    AttrNumber attnum;
    ListCell *l1 = NULL;
    ListCell *l2 = NULL;
    ListCell *l3 = NULL;
    ListCell *l4 = NULL;
    TupleDesc desc;

    natts = list_length(names);
    Assert(natts == list_length(types));
    Assert(natts == list_length(typmods));
    Assert(natts == list_length(collations));

    /*
     * allocate a new tuple descriptor
     */
    desc = CreateTemplateTupleDesc(natts, false, TAM_HEAP);

    attnum = 0;

    l2 = list_head(types);
    l3 = list_head(typmods);
    l4 = list_head(collations);
    foreach (l1, names) {
        char *attname = strVal(lfirst(l1));
        Oid atttypid;
        int32 atttypmod;
        Oid attcollation;

        atttypid = lfirst_oid(l2);
        l2 = lnext(l2);
        atttypmod = lfirst_int(l3);
        l3 = lnext(l3);
        attcollation = lfirst_oid(l4);
        l4 = lnext(l4);

        attnum++;

        TupleDescInitEntry(desc, attnum, attname, atttypid, atttypmod, 0);
        TupleDescInitEntryCollation(desc, attnum, attcollation);
    }

    return desc;
}
