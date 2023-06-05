/* -------------------------------------------------------------------------
 *
 * inv_api.cpp
 *	  routines for manipulating inversion fs large objects. This file
 *	  contains the user-level large object application interface routines.
 *
 *
 * Note: we access pg_largeobject.data using its C struct declaration.
 * This is safe because it immediately follows pageno which is an int4 field,
 * and therefore the data field will always be 4-byte aligned, even if it
 * is in the short 1-byte-header format.  We have to detoast it since it's
 * quite likely to be in compressed or short format.  We also need to check
 * for NULLs, since initdb will mark loid and pageno but not data as NOT NULL.
 *
 * Note: many of these routines leak memory in CurrentMemoryContext, as indeed
 * does most of the backend code.  We expect that CurrentMemoryContext will
 * be a short-lived context.  Data that must persist across function calls
 * is kept either in u_sess->cache_mem_cxt (the Relation structs) or in the
 * memory context given to inv_open (for LargeObjectDesc structs).
 *
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/gausskernel/storage/large_object/inv_api.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/tableam.h"
#include "access/genam.h"
#include "access/heapam.h"
#include "access/sysattr.h"
#include "access/tuptoaster.h"
#include "access/xact.h"
#include "catalog/dependency.h"
#include "catalog/indexing.h"
#include "catalog/objectaccess.h"
#include "catalog/pg_largeobject.h"
#include "catalog/pg_largeobject_metadata.h"
#include "libpq/libpq-fs.h"
#include "miscadmin.h"
#include "storage/large_object.h"
#include "utils/fmgroids.h"
#include "utils/rel.h"
#include "utils/rel_gs.h"
#include "utils/snapmgr.h"
#include "access/heapam.h"

/*
 * Open pg_largeobject and its index, if not already done in current xact
 */
static void open_lo_relation(void)
{
    ResourceOwner currentOwner;

    if (t_thrd.storage_cxt.lo_heap_r && t_thrd.storage_cxt.lo_index_r)
        return; /* already open in current xact */

    /* Arrange for the top xact to own these relation references */
    currentOwner = t_thrd.utils_cxt.CurrentResourceOwner;
    PG_TRY();
    {
        t_thrd.utils_cxt.CurrentResourceOwner = t_thrd.utils_cxt.TopTransactionResourceOwner;

        /* Use RowExclusiveLock since we might either read or write */
        if (t_thrd.storage_cxt.lo_heap_r == NULL)
            t_thrd.storage_cxt.lo_heap_r = heap_open(LargeObjectRelationId, RowExclusiveLock);
        if (t_thrd.storage_cxt.lo_index_r == NULL)
            t_thrd.storage_cxt.lo_index_r = index_open(LargeObjectLOidPNIndexId, RowExclusiveLock);
    }
    PG_CATCH();
    {
        /* Ensure CurrentResourceOwner is restored on error */
        t_thrd.utils_cxt.CurrentResourceOwner = currentOwner;
        PG_RE_THROW();
    }
    PG_END_TRY();
    t_thrd.utils_cxt.CurrentResourceOwner = currentOwner;
}

/*
 * Clean up at main transaction end
 */
void close_lo_relation(bool isCommit)
{
    if (t_thrd.storage_cxt.lo_heap_r || t_thrd.storage_cxt.lo_index_r) {
        /*
         * Only bother to close if committing; else abort cleanup will handle
         * it
         */
        if (isCommit) {
            ResourceOwner currentOwner;

            currentOwner = t_thrd.utils_cxt.CurrentResourceOwner;
            PG_TRY();
            {
                t_thrd.utils_cxt.CurrentResourceOwner = t_thrd.utils_cxt.TopTransactionResourceOwner;

                if (t_thrd.storage_cxt.lo_index_r)
                    index_close(t_thrd.storage_cxt.lo_index_r, NoLock);
                if (t_thrd.storage_cxt.lo_heap_r)
                    heap_close(t_thrd.storage_cxt.lo_heap_r, NoLock);
            }
            PG_CATCH();
            {
                /* Ensure CurrentResourceOwner is restored on error */
                t_thrd.utils_cxt.CurrentResourceOwner = currentOwner;
                PG_RE_THROW();
            }
            PG_END_TRY();
            t_thrd.utils_cxt.CurrentResourceOwner = currentOwner;
        }
        t_thrd.storage_cxt.lo_heap_r = NULL;
        t_thrd.storage_cxt.lo_index_r = NULL;
    }
}

/*
 * Same as pg_largeobject.c's LargeObjectExists(), except snapshot to
 * read with can be specified.
 */
static bool myLargeObjectExists(Oid loid, Snapshot snapshot)
{
    Relation pg_lo_meta;
    ScanKeyData skey[1];
    SysScanDesc sd;
    HeapTuple tuple;
    bool retval = false;

    ScanKeyInit(&skey[0], ObjectIdAttributeNumber, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(loid));

    pg_lo_meta = heap_open(LargeObjectMetadataRelationId, AccessShareLock);

    sd = systable_beginscan(pg_lo_meta, LargeObjectMetadataOidIndexId, true, snapshot, 1, skey);

    tuple = systable_getnext(sd);
    if (HeapTupleIsValid(tuple))
        retval = true;

    systable_endscan(sd);

    heap_close(pg_lo_meta, AccessShareLock);

    return retval;
}

/*
 * Extract data field from a pg_largeobject tuple, detoasting if needed
 * and verifying that the length is sane.  Returns data pointer (a bytea *),
 * data length, and an indication of whether to pfree the data pointer.
 */
static void getdatafield(Form_pg_largeobject tuple, bytea** pdatafield, int* plen, bool* pfreeit)
{
    bytea* datafield = NULL;
    int len;
    bool freeit = false;

    datafield = &(tuple->data); /* see note at top of file */
    if (VARATT_IS_EXTENDED(datafield)) {
        datafield = (bytea*)heap_tuple_untoast_attr((struct varlena*)datafield);
        freeit = true;
    }
    len = VARSIZE(datafield) - VARHDRSZ;
    if (len < 0 || len > LOBLKSIZE)
        ereport(ERROR,
                (errcode(ERRCODE_DATA_CORRUPTED),
                 errmsg("pg_largeobject entry for OID %u, page %d has invalid data field size %d",
                        tuple->loid,
                        tuple->pageno,
                        len)));
    *pdatafield = datafield;
    *plen = len;
    *pfreeit = freeit;
}

/*
 *	inv_create -- create a new large object
 *
 *	Arguments:
 *	  lobjId - OID to use for new large object, or InvalidOid to pick one
 *
 *	Returns:
 *	  OID of new object
 *
 * If lobjId is not InvalidOid, then an error occurs if the OID is already
 * in use.
 */
Oid inv_create(Oid lobjId)
{
    Oid lobjId_new;

    /*
     * Create a new largeobject with empty data pages
     */
    lobjId_new = LargeObjectCreate(lobjId);

    /*
     * dependency on the owner of largeobject
     *
     * The reason why we use LargeObjectRelationId instead of
     * LargeObjectMetadataRelationId here is to provide backward compatibility
     * to the applications which utilize a knowledge about internal layout of
     * system catalogs. OID of pg_largeobject_metadata and loid of
     * pg_largeobject are same value, so there are no actual differences here.
     */
    recordDependencyOnOwner(LargeObjectRelationId, lobjId_new, GetUserId());

    /* Post creation hook for new large object */
    InvokeObjectAccessHook(OAT_POST_CREATE, LargeObjectRelationId, lobjId_new, 0, NULL);

    /*
     * Advance command counter to make new tuple visible to later operations.
     */
    CommandCounterIncrement();

    return lobjId_new;
}

/*
 *	inv_open -- access an existing large object.
 *
 *		Returns:
 *		  Large object descriptor, appropriately filled in.  The descriptor
 *		  and subsidiary data are allocated in the specified memory context,
 *		  which must be suitably long-lived for the caller's purposes.
 */
LargeObjectDesc* inv_open(Oid lobjId, int flags, MemoryContext mcxt)
{
    LargeObjectDesc* retval = NULL;

    retval = (LargeObjectDesc*)MemoryContextAlloc(mcxt, sizeof(LargeObjectDesc));

    retval->id = lobjId;
    retval->subid = GetCurrentSubTransactionId();
    retval->offset = 0;

    if (flags & INV_WRITE) {
        retval->snapshot = SnapshotNow;
        retval->flags = IFS_WRLOCK | IFS_RDLOCK;
    } else if (flags & INV_READ) {
        /*
         * We must register the snapshot in TopTransaction's resowner, because
         * it must stay alive until the LO is closed rather than until the
         * current portal shuts down.
         */
        retval->snapshot = RegisterSnapshotOnOwner(GetActiveSnapshot(), t_thrd.utils_cxt.TopTransactionResourceOwner);
        retval->flags = IFS_RDLOCK;
    } else
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("invalid flags: %d", flags)));

    /* Can't use LargeObjectExists here because it always uses SnapshotNow */
    if (!myLargeObjectExists(lobjId, retval->snapshot))
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("large object %u does not exist", lobjId)));

    return retval;
}

/*
 * Closes a large object descriptor previously made by inv_open(), and
 * releases the long-term memory used by it.
 */
void inv_close(LargeObjectDesc* obj_desc)
{
    Assert(PointerIsValid(obj_desc));

    if (obj_desc->snapshot != SnapshotNow)
        UnregisterSnapshotFromOwner(obj_desc->snapshot, t_thrd.utils_cxt.TopTransactionResourceOwner);

    pfree(obj_desc);
}

/*
 * Destroys an existing large object (not to be confused with a descriptor!)
 *
 * returns -1 if failed
 */
int inv_drop(Oid lobjId)
{
    ObjectAddress object;

    /*
     * Delete any comments and dependencies on the large object
     */
    object.classId = LargeObjectRelationId;
    object.objectId = lobjId;
    object.objectSubId = 0;
    performDeletion(&object, DROP_CASCADE, 0);

    /*
     * Advance command counter so that tuple removal will be seen by later
     * large-object operations in this transaction.
     */
    CommandCounterIncrement();

    return 1;
}

/*
 * Determine size of a large object
 *
 * NOTE: LOs can contain gaps, just like Unix files.  We actually return
 * the offset of the last byte + 1.
 */
static uint32 inv_getsize(LargeObjectDesc* obj_desc)
{
    uint32 lastbyte = 0;
    ScanKeyData skey[1];
    SysScanDesc sd;
    HeapTuple tuple;

    Assert(PointerIsValid(obj_desc));

    open_lo_relation();

    ScanKeyInit(&skey[0], Anum_pg_largeobject_loid, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(obj_desc->id));

    sd = systable_beginscan_ordered(
             t_thrd.storage_cxt.lo_heap_r, t_thrd.storage_cxt.lo_index_r, obj_desc->snapshot, 1, skey);

    /*
     * Because the pg_largeobject index is on both loid and pageno, but we
     * constrain only loid, a backwards scan should visit all pages of the
     * large object in reverse pageno order.  So, it's sufficient to examine
     * the first valid tuple (== last valid page).
     */
    tuple = systable_getnext_ordered(sd, BackwardScanDirection);
    if (HeapTupleIsValid(tuple)) {
        Form_pg_largeobject data;
        bytea* datafield = NULL;
        int len;
        bool pfreeit = false;

        if (HeapTupleHasNulls(tuple)) /* paranoia */
            ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("null field found in pg_largeobject")));
        data = (Form_pg_largeobject)GETSTRUCT(tuple);
        getdatafield(data, &datafield, &len, &pfreeit);
        lastbyte = data->pageno * LOBLKSIZE + len;
        if (pfreeit)
            pfree(datafield);
    }

    systable_endscan_ordered(sd);

    return lastbyte;
}

int inv_seek(LargeObjectDesc* obj_desc, int offset, int whence)
{
    Assert(PointerIsValid(obj_desc));

    switch (whence) {
        case SEEK_SET:
            if (offset < 0)
                ereport(ERROR, (errcode_for_file_access(), errmsg("invalid seek offset: %d", offset)));
            obj_desc->offset = offset;
            break;
        case SEEK_CUR:
            if (offset < 0 && obj_desc->offset < ((uint32)(-offset)))
                ereport(ERROR, (errcode_for_file_access(), errmsg("invalid seek offset: %d", offset)));
            obj_desc->offset += offset;
            break;
        case SEEK_END: {
            uint32 size = inv_getsize(obj_desc);
            if (offset < 0 && size < ((uint32)(-offset)))
                ereport(ERROR, (errcode_for_file_access(), errmsg("invalid seek offset: %d", offset)));
            obj_desc->offset = size + offset;
        } break;
        default:
            ereport(ERROR, (errcode_for_file_access(), errmsg("invalid whence: %d", whence)));
    }
    return obj_desc->offset;
}

int inv_tell(LargeObjectDesc* obj_desc)
{
    Assert(PointerIsValid(obj_desc));

    return obj_desc->offset;
}

int inv_read(LargeObjectDesc* obj_desc, char* buf, int nbytes)
{
    Assert(PointerIsValid(obj_desc));
    int nread = 0;
    int n;
    int off;
    int len;
    int32 pageno = (int32)(obj_desc->offset / LOBLKSIZE);
    uint32 pageoff;
    ScanKeyData skey[2];
    SysScanDesc sd;
    HeapTuple tuple;
    errno_t rc = EOK;

    Assert(buf != NULL);

    if (nbytes <= 0) {
        return 0;
    }

    open_lo_relation();

    ScanKeyInit(&skey[0], Anum_pg_largeobject_loid, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(obj_desc->id));

    ScanKeyInit(&skey[1], Anum_pg_largeobject_pageno, BTGreaterEqualStrategyNumber, F_INT4GE, Int32GetDatum(pageno));

    sd = systable_beginscan_ordered(
             t_thrd.storage_cxt.lo_heap_r, t_thrd.storage_cxt.lo_index_r, obj_desc->snapshot, 2, skey);

    while ((tuple = systable_getnext_ordered(sd, ForwardScanDirection)) != NULL) {
        Form_pg_largeobject data;
        bytea* datafield = NULL;
        bool pfreeit = false;

        if (HeapTupleHasNulls(tuple)) /* paranoia */
            ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("null field found in pg_largeobject")));
        data = (Form_pg_largeobject)GETSTRUCT(tuple);

        /*
         * We expect the indexscan will deliver pages in order.  However,
         * there may be missing pages if the LO contains unwritten "holes". We
         * want missing sections to read out as zeroes.
         */
        pageoff = ((uint32)data->pageno) * LOBLKSIZE;
        if (pageoff > obj_desc->offset) {
            n = pageoff - obj_desc->offset;
            n = (n <= (nbytes - nread)) ? n : (nbytes - nread);
            rc = memset_s(buf + nread, n, '\0', n);
            securec_check(rc, "", "");
            nread += n;
            obj_desc->offset += n;
        }

        if (nread < nbytes) {
            off = (int)(obj_desc->offset - pageoff);
            if (off < 0 || off >= LOBLKSIZE) {
                ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT),
                                errmsg("invalid offset num:%d", off)));
            }

            getdatafield(data, &datafield, &len, &pfreeit);
            if (len > off) {
                n = len - off;
                n = (n <= (nbytes - nread)) ? n : (nbytes - nread);
                rc = memcpy_s(buf + nread, n, VARDATA(datafield) + off, n);
                securec_check(rc, "", "");
                nread += n;
                obj_desc->offset += n;
            }
            if (pfreeit)
                pfree(datafield);
        }

        if (nread >= nbytes)
            break;
    }

    systable_endscan_ordered(sd);

    return nread;
}

void check_obj_desc(const LargeObjectDesc* obj_desc)
{
    /* enforce writability because snapshot is probably wrong otherwise */
    if ((obj_desc->flags & IFS_WRLOCK) == 0)
        ereport(ERROR,
                (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                 errmsg("large object %u was not opened for writing", obj_desc->id)));

    /* check existence of the target largeobject */
    if (!LargeObjectExists(obj_desc->id))
        ereport(
            ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("large object %u was already dropped", obj_desc->id)));
}

int inv_write(LargeObjectDesc* obj_desc, const char* buf, int nbytes)
{
    if (nbytes <= 0) {
        return 0;
    }
    int nwritten = 0;
    int n;
    int off;
    int len;
    int32 pageno;
    ScanKeyData skey[2];
    SysScanDesc sd;
    HeapTuple oldtuple = NULL;
    Form_pg_largeobject olddata = NULL;
    bool neednextpage = true;
    bytea* datafield = NULL;
    bool pfreeit = false;
    union {
        bytea hdr;
        char data[LOBLKSIZE + sizeof(bytea) + sizeof(int32)]; /* make struct big enough */
        int32 align_it;       /* ensure struct is aligned well enough */
    } workbuf;
    char* workb = VARDATA(&workbuf.hdr);
    HeapTuple newtup;
    Datum values[Natts_pg_largeobject];
    bool nulls[Natts_pg_largeobject];
    bool replace[Natts_pg_largeobject];
    CatalogIndexState indstate;
    errno_t rc;

    if (unlikely(!PointerIsValid(obj_desc))) {
        return 0;
    }
    pageno = (int32)(obj_desc->offset / LOBLKSIZE);

    rc = memset_s(&workbuf, sizeof(workbuf), 0, sizeof(workbuf));
    securec_check(rc, "\0", "\0");
    Assert(buf != NULL);

    check_obj_desc(obj_desc);
    open_lo_relation();

    indstate = CatalogOpenIndexes(t_thrd.storage_cxt.lo_heap_r);

    ScanKeyInit(&skey[0], Anum_pg_largeobject_loid, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(obj_desc->id));

    ScanKeyInit(&skey[1], Anum_pg_largeobject_pageno, BTGreaterEqualStrategyNumber, F_INT4GE, Int32GetDatum(pageno));

    sd = systable_beginscan_ordered(
             t_thrd.storage_cxt.lo_heap_r, t_thrd.storage_cxt.lo_index_r, obj_desc->snapshot, 2, skey);

    while (nwritten < nbytes) {
        /*
         * If possible, get next pre-existing page of the LO.  We expect the
         * indexscan will deliver these in order --- but there may be holes.
         */
        if (neednextpage) {
            if ((oldtuple = systable_getnext_ordered(sd, ForwardScanDirection)) != NULL) {
                if (HeapTupleHasNulls(oldtuple)) /* paranoia */
                    ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("null field found in pg_largeobject")));
                olddata = (Form_pg_largeobject)GETSTRUCT(oldtuple);
                Assert(olddata->pageno >= pageno);
            }
            neednextpage = false;
        }

        /*
         * If we have a pre-existing page, see if it is the page we want to
         * write, or a later one.
         */
        if (olddata != NULL && olddata->pageno == pageno) {
            /*
             * Update an existing page with fresh data.
             *
             * First, load old data into workbuf
             */
            getdatafield(olddata, &datafield, &len, &pfreeit);
            rc = memcpy_s(workb, len, VARDATA(datafield), len);
            securec_check(rc, "", "");
            if (pfreeit)
                pfree(datafield);

            /*
             * Fill any hole
             */
            off = (int)(obj_desc->offset % LOBLKSIZE);
            if (off > len) {
                rc = memset_s(workb + len, off - len, '\0', off - len);
                securec_check(rc, "", "");
            }

            /*
             * Insert appropriate portion of new data
             */
            n = LOBLKSIZE - off;
            n = (n <= (nbytes - nwritten)) ? n : (nbytes - nwritten);
            rc = memcpy_s(workb + off, n, buf + nwritten, n);
            securec_check(rc, "", "");
            nwritten += n;
            obj_desc->offset += n;
            off += n;
            /* compute valid length of new page */
            len = (len >= off) ? len : off;
            SET_VARSIZE(&workbuf.hdr, len + VARHDRSZ);

            /*
             * Form and insert updated tuple
             */
            rc = memset_s(values, sizeof(values), 0, sizeof(values));
            securec_check(rc, "", "");
            rc = memset_s(nulls, sizeof(nulls), false, sizeof(nulls));
            securec_check(rc, "", "");
            rc = memset_s(replace, sizeof(replace), false, sizeof(replace));
            securec_check(rc, "", "");
            values[Anum_pg_largeobject_data - 1] = PointerGetDatum(&workbuf);
            replace[Anum_pg_largeobject_data - 1] = true;
            newtup =
                heap_modify_tuple(oldtuple, RelationGetDescr(t_thrd.storage_cxt.lo_heap_r), values, nulls, replace);
            simple_heap_update(t_thrd.storage_cxt.lo_heap_r, &newtup->t_self, newtup);
            CatalogIndexInsert(indstate, newtup);
            heap_freetuple(newtup);

            /*
             * We're done with this old page.
             */
            oldtuple = NULL;
            olddata = NULL;
            neednextpage = true;
        } else {
            /*
             * Write a brand new page.
             *
             * First, fill any hole
             */
            off = (int)(obj_desc->offset % LOBLKSIZE);
            if (off > 0) {
                rc = memset_s(workb, off, '\0', off);
                securec_check(rc, "", "");
            }

            /*
             * Insert appropriate portion of new data
             */
            n = LOBLKSIZE - off;
            n = (n <= (nbytes - nwritten)) ? n : (nbytes - nwritten);
            rc = memcpy_s(workb + off, n, buf + nwritten, n);
            securec_check(rc, "", "");
            nwritten += n;
            obj_desc->offset += n;
            /* compute valid length of new page */
            len = off + n;
            SET_VARSIZE(&workbuf.hdr, len + VARHDRSZ);

            /*
             * Form and insert updated tuple
             */
            rc = memset_s(values, sizeof(values), 0, sizeof(values));
            securec_check(rc, "", "");
            rc = memset_s(nulls, sizeof(nulls), false, sizeof(nulls));
            securec_check(rc, "", "");
            values[Anum_pg_largeobject_loid - 1] = ObjectIdGetDatum(obj_desc->id);
            values[Anum_pg_largeobject_pageno - 1] = Int32GetDatum(pageno);
            values[Anum_pg_largeobject_data - 1] = PointerGetDatum(&workbuf);
            newtup = heap_form_tuple(t_thrd.storage_cxt.lo_heap_r->rd_att, values, nulls);
            (void)simple_heap_insert(t_thrd.storage_cxt.lo_heap_r, newtup);
            CatalogIndexInsert(indstate, newtup);
            heap_freetuple(newtup);
        }
        pageno++;
    }

    systable_endscan_ordered(sd);

    CatalogCloseIndexes(indstate);

    /*
     * Advance command counter so that my tuple updates will be seen by later
     * large-object operations in this transaction.
     */
    CommandCounterIncrement();

    return nwritten;
}

void inv_truncate(LargeObjectDesc* obj_desc, int len)
{
    int32 pageno = (int32)(len / LOBLKSIZE);
    int off;
    ScanKeyData skey[2];
    SysScanDesc sd;
    HeapTuple oldtuple;
    Form_pg_largeobject olddata;
    union {
        bytea hdr;
        char data[LOBLKSIZE + sizeof(bytea) + sizeof(int32)]; /* make struct big enough */
        int32 align_it;       /* ensure struct is aligned well enough */
    } workbuf;
    char* workb = VARDATA(&workbuf.hdr);
    HeapTuple newtup;
    Datum values[Natts_pg_largeobject] = {0, 0, 0};
    bool nulls[Natts_pg_largeobject]  = {false, false, false};
    bool replace[Natts_pg_largeobject] = {false, false, false};

    CatalogIndexState indstate;
    errno_t rc;
    rc = memset_s(&workbuf, sizeof(workbuf), 0, sizeof(workbuf));
    securec_check(rc, "\0", "\0");
    Assert(PointerIsValid(obj_desc));

    check_obj_desc(obj_desc);
    open_lo_relation();

    indstate = CatalogOpenIndexes(t_thrd.storage_cxt.lo_heap_r);

    /*
     * Set up to find all pages with desired loid and pageno >= target
     */
    ScanKeyInit(&skey[0], Anum_pg_largeobject_loid, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(obj_desc->id));

    ScanKeyInit(&skey[1], Anum_pg_largeobject_pageno, BTGreaterEqualStrategyNumber, F_INT4GE, Int32GetDatum(pageno));

    sd = systable_beginscan_ordered(
             t_thrd.storage_cxt.lo_heap_r, t_thrd.storage_cxt.lo_index_r, obj_desc->snapshot, 2, skey);

    /*
     * If possible, get the page the truncation point is in. The truncation
     * point may be beyond the end of the LO or in a hole.
     */
    olddata = NULL;
    if ((oldtuple = systable_getnext_ordered(sd, ForwardScanDirection)) != NULL) {
        if (HeapTupleHasNulls(oldtuple)) /* paranoia */
            ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("null field found in pg_largeobject")));
        olddata = (Form_pg_largeobject)GETSTRUCT(oldtuple);
        Assert(olddata->pageno >= pageno);
    }

    /*
     * If we found the page of the truncation point we need to truncate the
     * data in it.	Otherwise if we're in a hole, we need to create a page to
     * mark the end of data.
     */
    if (olddata != NULL && olddata->pageno == pageno) {
        /* First, load old data into workbuf */
        bytea* datafield = NULL;
        bool pfreeit = false;
        int pagelen;

        getdatafield(olddata, &datafield, &pagelen, &pfreeit);
        rc = memcpy_s(workb, pagelen, VARDATA(datafield), pagelen);
        securec_check(rc, "", "");
        if (pfreeit)
            pfree(datafield);

        /*
         * Fill any hole
         */
        off = len % LOBLKSIZE;
        if (off > pagelen) {
            rc = memset_s(workb + pagelen, off - pagelen, '\0', off - pagelen);
            securec_check(rc, "", "");
        }

        /* compute length of new page */
        SET_VARSIZE(&workbuf.hdr, off + VARHDRSZ);

        /*
         * Form and insert updated tuple
         */
        values[Anum_pg_largeobject_data - 1] = PointerGetDatum(&workbuf);
        replace[Anum_pg_largeobject_data - 1] = true;
        newtup = heap_modify_tuple(oldtuple, RelationGetDescr(t_thrd.storage_cxt.lo_heap_r), values, nulls, replace);
        simple_heap_update(t_thrd.storage_cxt.lo_heap_r, &newtup->t_self, newtup);
        CatalogIndexInsert(indstate, newtup);
        heap_freetuple(newtup);
    } else {
        /*
         * If the first page we found was after the truncation point, we're in
         * a hole that we'll fill, but we need to delete the later page
         * because the loop below won't visit it again.
         */
        if (olddata != NULL) {
            Assert(olddata->pageno > pageno);
            simple_heap_delete(t_thrd.storage_cxt.lo_heap_r, &oldtuple->t_self);
        }

        /*
         * Write a brand new page.
         *
         * Fill the hole up to the truncation point
         */
        off = len % LOBLKSIZE;
        if (off > 0) {
            rc = memset_s(workb, off, '\0', off);
            securec_check(rc, "", "");
        }

        /* compute length of new page */
        SET_VARSIZE(&workbuf.hdr, off + VARHDRSZ);

        /*
         * Form and insert new tuple
         */
        values[Anum_pg_largeobject_loid - 1] = ObjectIdGetDatum(obj_desc->id);
        values[Anum_pg_largeobject_pageno - 1] = Int32GetDatum(pageno);
        values[Anum_pg_largeobject_data - 1] = PointerGetDatum(&workbuf);
        newtup = heap_form_tuple(t_thrd.storage_cxt.lo_heap_r->rd_att, values, nulls);
        (void)simple_heap_insert(t_thrd.storage_cxt.lo_heap_r, newtup);
        CatalogIndexInsert(indstate, newtup);
        heap_freetuple(newtup);
    }

    /*
     * Delete any pages after the truncation point.  If the initial search
     * didn't find a page, then of course there's nothing more to do.
     */
    if (olddata != NULL) {
        while ((oldtuple = systable_getnext_ordered(sd, ForwardScanDirection)) != NULL) {
            simple_heap_delete(t_thrd.storage_cxt.lo_heap_r, &oldtuple->t_self);
        }
    }

    systable_endscan_ordered(sd);

    CatalogCloseIndexes(indstate);

    /*
     * Advance command counter so that tuple updates will be seen by later
     * large-object operations in this transaction.
     */
    CommandCounterIncrement();
}
