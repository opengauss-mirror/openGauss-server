/* -------------------------------------------------------------------------
 *
 * proto.c
 * 		logical replication protocol functions
 *
 * Copyright (c) 2015, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 * 		src/backend/replication/logical/proto.c
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/sysattr.h"
#include "access/tableam.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_type.h"
#include "libpq/pqformat.h"
#include "replication/logicalproto.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"

/*
 * Protocol message flags.
 */
static const int LOGICALREP_IS_REPLICA_IDENTITY = 1;

static void logicalrep_write_attrs(StringInfo out, Relation rel);
static void logicalrep_write_tuple(StringInfo out, Relation rel, HeapTuple tuple, bool binary);

static void logicalrep_read_attrs(StringInfo in, LogicalRepRelation *rel);
static void logicalrep_read_tuple(StringInfo in, LogicalRepTupleData *tuple);

static void logicalrep_write_namespace(StringInfo out, Oid nspid);
static const char *logicalrep_read_namespace(StringInfo in);

/*
 * Write BEGIN to the output stream.
 */
void logicalrep_write_begin(StringInfo out, ReorderBufferTXN *txn)
{
    pq_sendbyte(out, 'B'); /* BEGIN */

    /* fixed fields */
    pq_sendint64(out, txn->final_lsn);
    pq_sendint64(out, txn->commit_time);
    pq_sendint64(out, txn->xid);
}

/*
 * Read transaction BEGIN from the stream.
 */
void logicalrep_read_begin(StringInfo in, LogicalRepBeginData *begin_data)
{
    /* read fields */
    begin_data->final_lsn = pq_getmsgint64(in);
    if (begin_data->final_lsn == InvalidXLogRecPtr)
        elog(ERROR, "final_lsn not set in begin message");
    begin_data->committime = pq_getmsgint64(in);
    begin_data->xid = pq_getmsgint64(in);
}


/*
 * Write COMMIT to the output stream.
 */
void logicalrep_write_commit(StringInfo out, ReorderBufferTXN *txn, XLogRecPtr commit_lsn)
{
    uint8 flags = 0;

    pq_sendbyte(out, 'C'); /* sending COMMIT */

    /* send the flags field (unused for now) */
    pq_sendbyte(out, flags);

    /* send fields */
    pq_sendint64(out, commit_lsn);
    pq_sendint64(out, txn->end_lsn);
    pq_sendint64(out, txn->commit_time);
}

/*
 * Read transaction COMMIT from the stream.
 */
void logicalrep_read_commit(StringInfo in, LogicalRepCommitData *commit_data)
{
    /* read flags (unused for now) */
    uint8 flags = pq_getmsgbyte(in);
    if (flags != 0) {
        elog(ERROR, "unknown flags %u in commit message", flags);
    }

    /* read fields */
    commit_data->commit_lsn = pq_getmsgint64(in);
    commit_data->end_lsn = pq_getmsgint64(in);
    commit_data->committime = pq_getmsgint64(in);
}

/*
 * Write ORIGIN to the output stream.
 */
void logicalrep_write_origin(StringInfo out, const char *origin, XLogRecPtr origin_lsn)
{
    pq_sendbyte(out, 'O'); /* ORIGIN */

    /* fixed fields */
    pq_sendint64(out, origin_lsn);

    /* origin string */
    pq_sendstring(out, origin);
}

/*
 * Write INSERT to the output stream.
 */
void logicalrep_write_insert(StringInfo out, Relation rel, HeapTuple newtuple, bool binary)
{
    pq_sendbyte(out, 'I'); /* action INSERT */

    /* use Oid as relation identifier */
    pq_sendint32(out, RelationGetRelid(rel));

    pq_sendbyte(out, 'N'); /* new tuple follows */
    logicalrep_write_tuple(out, rel, newtuple, binary);
}

/*
 * Read INSERT from stream.
 *
 * Fills the new tuple.
 */
LogicalRepRelId logicalrep_read_insert(StringInfo in, LogicalRepTupleData *newtup)
{
    char action;
    LogicalRepRelId relid;

    /* read the relation id */
    relid = pq_getmsgint(in, sizeof(LogicalRepRelId));

    action = pq_getmsgbyte(in);
    if (action != 'N')
        elog(ERROR, "expected new tuple but got %d", action);

    logicalrep_read_tuple(in, newtup);

    return relid;
}

/*
 * Write UPDATE to the output stream.
 */
void logicalrep_write_update(StringInfo out, Relation rel, HeapTuple oldtuple, HeapTuple newtuple, bool binary)
{
    pq_sendbyte(out, 'U'); /* action UPDATE */

    /* use Oid as relation identifier */
    pq_sendint32(out, RelationGetRelid(rel));

    if (oldtuple != NULL) {
        char relreplident = RelationGetRelReplident(rel);
        Assert(relreplident == REPLICA_IDENTITY_DEFAULT || relreplident == REPLICA_IDENTITY_FULL ||
            relreplident == REPLICA_IDENTITY_INDEX);
        if (relreplident == REPLICA_IDENTITY_FULL)
            pq_sendbyte(out, 'O'); /* old tuple follows */
        else
            pq_sendbyte(out, 'K'); /* old key follows */
        logicalrep_write_tuple(out, rel, oldtuple, binary);
    }

    pq_sendbyte(out, 'N'); /* new tuple follows */
    logicalrep_write_tuple(out, rel, newtuple, binary);
}

/*
 * Read UPDATE from stream.
 */
LogicalRepRelId logicalrep_read_update(StringInfo in, bool *has_oldtuple, LogicalRepTupleData *oldtup,
    LogicalRepTupleData *newtup)
{
    char action;
    LogicalRepRelId relid;

    /* read the relation id */
    relid = pq_getmsgint(in, sizeof(LogicalRepRelId));
    /* read and verify action */
    action = pq_getmsgbyte(in);
    if (action != 'K' && action != 'O' && action != 'N') {
        elog(ERROR, "expected action 'N', 'O' or 'K', got %c", action);
    }

    /* check for old tuple */
    if (action == 'K' || action == 'O') {
        logicalrep_read_tuple(in, oldtup);
        *has_oldtuple = true;

        action = pq_getmsgbyte(in);
    } else {
        *has_oldtuple = false;
    }

    /* check for new  tuple */
    if (action != 'N') {
        elog(ERROR, "expected action 'N', got %c", action);
    }

    logicalrep_read_tuple(in, newtup);

    return relid;
}

/*
 * Write DELETE to the output stream.
 */
void logicalrep_write_delete(StringInfo out, Relation rel, HeapTuple oldtuple, bool binary)
{
    char relreplident = RelationGetRelReplident(rel);
    Assert(relreplident == REPLICA_IDENTITY_DEFAULT ||
        relreplident == REPLICA_IDENTITY_FULL || relreplident == REPLICA_IDENTITY_INDEX);

    pq_sendbyte(out, 'D'); /* action DELETE */

    /* use Oid as relation identifier */
    pq_sendint32(out, RelationGetRelid(rel));

    if (relreplident == REPLICA_IDENTITY_FULL)
        pq_sendbyte(out, 'O'); /* old tuple follows */
    else
        pq_sendbyte(out, 'K'); /* old key follows */

    logicalrep_write_tuple(out, rel, oldtuple, binary);
}

/*
 * Read DELETE from stream.
 *
 * Fills the old tuple.
 */
LogicalRepRelId logicalrep_read_delete(StringInfo in, LogicalRepTupleData *oldtup)
{
    char action;
    LogicalRepRelId relid;

    /* read the relation id */
    relid = pq_getmsgint(in, sizeof(LogicalRepRelId));

    /* read and verify action */
    action = pq_getmsgbyte(in);
    if (action != 'K' && action != 'O')
        elog(ERROR, "expected action 'O' or 'K', got %c", action);

    logicalrep_read_tuple(in, oldtup);

    return relid;
}

/*
 * Write relation description to the output stream.
 */
void logicalrep_write_rel(StringInfo out, Relation rel)
{
    char *relname;

    pq_sendbyte(out, 'R'); /* sending RELATION */

    /* use Oid as relation identifier */
    pq_sendint32(out, RelationGetRelid(rel));

    /* send qualified relation name */
    logicalrep_write_namespace(out, RelationGetNamespace(rel));
    relname = RelationGetRelationName(rel);
    pq_sendstring(out, relname);

    /* send replica identity */
    pq_sendbyte(out, RelationGetRelReplident(rel));

    /* send the attribute info */
    logicalrep_write_attrs(out, rel);
}

/*
 * Read the relation info from stream and return as LogicalRepRelation.
 */
LogicalRepRelation *logicalrep_read_rel(StringInfo in)
{
    LogicalRepRelation *rel = (LogicalRepRelation *)palloc(sizeof(LogicalRepRelation));

    rel->remoteid = pq_getmsgint(in, sizeof(LogicalRepRelId));

    /* Read relation name from stream */
    rel->nspname = pstrdup(logicalrep_read_namespace(in));
    rel->relname = pstrdup(pq_getmsgstring(in));

    /* Read the replica identity. */
    rel->replident = pq_getmsgbyte(in);

    /* Get attribute description */
    logicalrep_read_attrs(in, rel);

    return rel;
}

/*
 * Write type info to the output stream.
 *
 * This function will always write base type info.
 */
void logicalrep_write_typ(StringInfo out, Oid typoid)
{
    Oid basetypoid = getBaseType(typoid);
    HeapTuple tup;
    Form_pg_type typtup;

    pq_sendbyte(out, 'Y'); /* sending TYPE */

    tup = SearchSysCache1(TYPEOID, ObjectIdGetDatum(basetypoid));
    if (!HeapTupleIsValid(tup))
        elog(ERROR, "cache lookup failed for type %u", basetypoid);
    typtup = (Form_pg_type)GETSTRUCT(tup);

    /* use Oid as relation identifier */
    pq_sendint32(out, typoid);

    /* send qualified type name */
    logicalrep_write_namespace(out, typtup->typnamespace);
    pq_sendstring(out, NameStr(typtup->typname));

    ReleaseSysCache(tup);
}

/*
 * Read type info from the output stream.
 */
void logicalrep_read_typ(StringInfo in, LogicalRepTyp *ltyp)
{
    ltyp->remoteid = pq_getmsgint(in, sizeof(LogicalRepRelId));

    /* Read type name from stream */
    ltyp->nspname = pstrdup(logicalrep_read_namespace(in));
    ltyp->typname = pstrdup(pq_getmsgstring(in));
}

/*
 * Write a tuple to the outputstream, in the most efficient format possible.
 */
static void logicalrep_write_tuple(StringInfo out, Relation rel, HeapTuple tuple, bool binary)
{
    TupleDesc desc;
    Datum values[MaxTupleAttributeNumber];
    bool isnull[MaxTupleAttributeNumber];
    int i;
    uint16 nliveatts = 0;

    desc = RelationGetDescr(rel);

    for (i = 0; i < desc->natts; i++) {
        if (desc->attrs[i]->attisdropped || GetGeneratedCol(desc, i))
            continue;
        nliveatts++;
    }
    pq_sendint16(out, nliveatts);

    /* try to allocate enough memory from the get-go */
    enlargeStringInfo(out, tuple->t_len + nliveatts * (1 + sizeof(int)));

    tableam_tops_deform_tuple(tuple, desc, values, isnull);

    /* Write the values */
    for (i = 0; i < desc->natts; i++) {
        HeapTuple typtup;
        Form_pg_type typclass;
        Form_pg_attribute att = desc->attrs[i];

        /* skip dropped columns */
        if (att->attisdropped || GetGeneratedCol(desc, i)) {
            continue;
        }

        if (isnull[i]) {
            pq_sendbyte(out, LOGICALREP_COLUMN_NULL); /* null column */
            continue;
        }

        typtup = SearchSysCache1(TYPEOID, ObjectIdGetDatum(att->atttypid));
        if (!HeapTupleIsValid(typtup))
            elog(ERROR, "cache lookup failed for type %u", att->atttypid);
        typclass = (Form_pg_type)GETSTRUCT(typtup);

        /*
        * Send in binary if requested and type has suitable send function.
        */
        if (binary && OidIsValid(typclass->typsend)) {
            bytea* outputbytes = NULL;
            pq_sendbyte(out, LOGICALREP_COLUMN_BINARY);
            if (!typclass->typbyval && typclass->typlen == -1) {
                /* definitely detoasted Datum */
                Datum val = PointerGetDatum(PG_DETOAST_DATUM(values[i]));
                outputbytes = OidSendFunctionCall(typclass->typsend, val);
            } else {
                outputbytes = OidSendFunctionCall(typclass->typsend, values[i]);
            }
            int len = VARSIZE(outputbytes) - VARHDRSZ;
            pq_sendint(out, len, 4);    /* length */
            pq_sendbytes(out, VARDATA(outputbytes), len);    /* data */
            if (outputbytes != NULL) {
                pfree(outputbytes);
            }
        } else {
            char* outputstr = NULL;
            pq_sendbyte(out, LOGICALREP_COLUMN_TEXT);
            if (!typclass->typbyval && typclass->typlen == -1) {
                /* definitely detoasted Datum */
                Datum val = PointerGetDatum(PG_DETOAST_DATUM(values[i]));
                outputstr = OidOutputFunctionCall(typclass->typoutput, val);
            } else {
                outputstr = OidOutputFunctionCall(typclass->typoutput, values[i]);
            }
            pq_sendcountedtext(out, outputstr, strlen(outputstr), false);
            if (outputstr != NULL) {
                pfree(outputstr);
            }
        }
        ReleaseSysCache(typtup);
    }
}

/*
 * Read tuple in logical replication format from stream.
 */
static void logicalrep_read_tuple(StringInfo in, LogicalRepTupleData *tuple)
{
    /* Get number of attributes. */
    uint16 natts = pq_getmsgint(in, sizeof(uint16));

    /* Allocate space for per-column values; zero out unused StringInfoDatas */
    tuple->colvalues = (StringInfoData *) palloc0(natts * sizeof(StringInfoData));
    tuple->colstatus = (char *) palloc(natts * sizeof(char));
    tuple->ncols = natts;

    /* Read the data */
    for (uint16 i = 0; i < natts; i++) {
        char kind = pq_getmsgbyte(in);
        tuple->colstatus[i] = kind;
        uint32 len;
        StringInfo value = &tuple->colvalues[i];

        switch (kind) {
            case LOGICALREP_COLUMN_NULL: /* null */
                /* nothing more to do */
                break;
            case LOGICALREP_COLUMN_TEXT:
                len = pq_getmsgint(in, sizeof(uint32));    /* read length */

                /* and data */
                value->data = (char *) palloc((len + 1) * sizeof(char));
                pq_copymsgbytes(in, value->data, len);
                value->data[len] = '\0';
                /* make StringInfo fully valid */
                value->len = len;
                value->cursor = 0;
                value->maxlen = len;
                break;
            case LOGICALREP_COLUMN_BINARY:
                len = pq_getmsgint(in, sizeof(uint32));    /* read length */

                /* and data */
                value->data = (char *)palloc0((len + 1) * sizeof(char));
                pq_copymsgbytes(in, value->data, len);
                /* make StringInfo fully valid */
                value->len = len;
                value->cursor = 0;
                value->maxlen = len;
                break;
            default:
                elog(ERROR, "unrecognized data representation type '%c'", kind);
                break;
        }
    }
}

/*
 * Write relation attributes metadata to the stream.
 */
static void logicalrep_write_attrs(StringInfo out, Relation rel)
{
    TupleDesc desc;
    int i;
    uint16 nliveatts = 0;
    Bitmapset *idattrs = NULL;
    bool replidentfull;

    desc = RelationGetDescr(rel);

    /* send number of live attributes */
    for (i = 0; i < desc->natts; i++) {
        if (desc->attrs[i]->attisdropped || GetGeneratedCol(desc, i))
            continue;
        nliveatts++;
    }
    pq_sendint16(out, nliveatts);

    /* fetch bitmap of REPLICATION IDENTITY attributes */
    replidentfull = (RelationGetRelReplident(rel) == REPLICA_IDENTITY_FULL);
    if (!replidentfull)
        idattrs = RelationGetIndexAttrBitmap(rel, INDEX_ATTR_BITMAP_IDENTITY_KEY);

    /* send the attributes */
    for (i = 0; i < desc->natts; i++) {
        Form_pg_attribute att = desc->attrs[i];
        uint8 flags = 0;

        if (att->attisdropped || GetGeneratedCol(desc, i))
            continue;

        /* REPLICA IDENTITY FULL means all columns are sent as part of key. */
        if (replidentfull || bms_is_member(att->attnum - FirstLowInvalidHeapAttributeNumber, idattrs))
            flags |= LOGICALREP_IS_REPLICA_IDENTITY;

        pq_sendbyte(out, flags);

        /* attribute name */
        pq_sendstring(out, NameStr(att->attname));

        /* attribute type id */
        pq_sendint32(out, (int)att->atttypid);

        /* attribute mode */
        pq_sendint32(out, att->atttypmod);
    }

    bms_free(idattrs);
}

/*
 * Read relation attribute metadata from the stream.
 */
static void logicalrep_read_attrs(StringInfo in, LogicalRepRelation *rel)
{
    uint16 i;
    uint16 natts;
    char **attnames;
    Oid *atttyps;
    Bitmapset *attkeys = NULL;

    natts = pq_getmsgint(in, sizeof(uint16));
    attnames = (char **)palloc(natts * sizeof(char *));
    atttyps = (Oid *)palloc(natts * sizeof(Oid));

    /* read the attributes */
    for (i = 0; i < natts; i++) {
        uint8 flags;

        /* Check for replica identity column */
        flags = pq_getmsgbyte(in);
        if (flags & LOGICALREP_IS_REPLICA_IDENTITY)
            attkeys = bms_add_member(attkeys, i);

        /* attribute name */
        attnames[i] = pstrdup(pq_getmsgstring(in));

        /* attribute type id */
        atttyps[i] = (Oid)pq_getmsgint(in, sizeof(Oid));

        /* we ignore attribute mode for now */
        (void)pq_getmsgint(in, sizeof(int));
    }

    rel->attnames = attnames;
    rel->atttyps = atttyps;
    rel->attkeys = attkeys;
    rel->natts = natts;
}

/*
 * Write the namespace name or empty string for pg_catalog (to save space).
 */
static void logicalrep_write_namespace(StringInfo out, Oid nspid)
{
    if (nspid == PG_CATALOG_NAMESPACE)
        pq_sendbyte(out, '\0');
    else {
        char *nspname = get_namespace_name(nspid);

        if (nspname == NULL)
            elog(ERROR, "cache lookup failed for namespace %u", nspid);

        pq_sendstring(out, nspname);
    }
}

/*
 * Read the namespace name while treating empty string as pg_catalog.
 */
static const char *logicalrep_read_namespace(StringInfo in)
{
    const char *nspname = pq_getmsgstring(in);

    if (nspname[0] == '\0') {
        nspname = "pg_catalog";
    }

    return nspname;
}

/*
 * Write conninfo to the output stream.
 */
void logicalrep_write_conninfo(StringInfo out, char* conninfo)
{
    pq_sendbyte(out, 'S'); /* action */

    pq_writestring(out, conninfo); /* conninfo follows */
}

/*
 * Read conninfo from stream.
 */
void logicalrep_read_conninfo(StringInfo in, char** conninfo)
{
    const char* conninfoTemp = pq_getmsgstring(in);
    size_t conninfoLen = strlen(conninfoTemp) + 1;
    *conninfo = (char*)palloc(conninfoLen);
    int rc = strcpy_s(*conninfo, conninfoLen, conninfoTemp);
    securec_check(rc, "", "");
}
