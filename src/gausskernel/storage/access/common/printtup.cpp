/* -------------------------------------------------------------------------
 *
 * printtup.cpp
 *	  Routines to print out tuples to the destination (both frontend
 *	  clients and standalone backends are supported here).
 *
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2021, openGauss Contributors
 *
 * IDENTIFICATION
 *	  src/gausskernel/storage/access/common/printtup.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/printtup.h"
#include "access/transam.h"
#include "access/tableam.h"
#include "libpq/libpq.h"
#include "libpq/pqformat.h"
#include "tcop/pquery.h"
#include "utils/lsyscache.h"
#include "mb/pg_wchar.h"
#include "miscadmin.h"
#ifdef PGXC
#include "pgxc/pgxc.h"
#include "fmgr.h"
#include "utils/builtins.h"
#endif
#include "distributelayer/streamProducer.h"
#include "executor/exec/execStream.h"
#include "access/heapam.h"

static void printtup_startup(DestReceiver *self, int operation, TupleDesc typeinfo);
static void printtup_20(TupleTableSlot *slot, DestReceiver *self);
static void printtup_internal_20(TupleTableSlot *slot, DestReceiver *self);
static void printtup_shutdown(DestReceiver *self);
static void printtup_destroy(DestReceiver *self);

static void SendRowDescriptionCols_2(StringInfo buf, TupleDesc typeinfo, List *targetlist, int16 *formats);
static void SendRowDescriptionCols_3(StringInfo buf, TupleDesc typeinfo, List *targetlist, int16 *formats);
static void writeString(StringInfo buf, const char *name, bool isWrite);
#ifndef ENABLE_MULTIPLE_NODES
static bool checkNeedUpperAndToUpper(char *dest, const char *source);
#endif

/* for stream send function */
static void printBroadCastTuple(TupleTableSlot *tuple, DestReceiver *self);
static void printLocalBroadCastTuple(TupleTableSlot *tuple, DestReceiver *self);
static void printRedistributeTuple(TupleTableSlot *tuple, DestReceiver *self);
static void printLocalRedistributeTuple(TupleTableSlot *tuple, DestReceiver *self);
static void printLocalRoundRobinTuple(TupleTableSlot *tuple, DestReceiver *self);
static void printHybridTuple(TupleTableSlot *tuple, DestReceiver *self);
static void printStreamShutdown(DestReceiver *self);
static void printStreamStartup(DestReceiver *self, int operation, TupleDesc typeinfo);
static void printBroadCastBatchCompress(VectorBatch *batch, DestReceiver *self);
static void printLocalBroadCastBatch(VectorBatch *batch, DestReceiver *self);
static void printRedistributeBatch(VectorBatch *batch, DestReceiver *self);
static void printLocalRedistributeBatch(VectorBatch *batch, DestReceiver *self);
static void printLocalRoundRobinBatch(VectorBatch *batch, DestReceiver *self);
static void printHybridBatch(VectorBatch *batch, DestReceiver *self);
static void finalizeLocalStream(DestReceiver *self);

inline void AddCheckInfo(StringInfo buf);
/* ----------------------------------------------------------------
 *		printtup / debugtup support
 * ----------------------------------------------------------------
 */
/* ----------------
 *		Private state for a printtup destination object
 *
 * NOTE: finfo is the lookup info for either typoutput or typsend, whichever
 * we are using for this column.
 * ----------------
 */
/*
 * @Description: Create destReciever for stream comm.
 *
 * @param[IN] dest: stream type.
 * @return DestReceiver
 */
DestReceiver *createStreamDestReceiver(CommandDest dest)
{
    streamReceiver *self = (streamReceiver *)palloc0(sizeof(streamReceiver));

    /* Assign data send function based on the stream type. */
    switch (dest) {
        case DestTupleBroadCast:
            self->pub.receiveSlot = printBroadCastTuple;
            break;

        case DestTupleLocalBroadCast:
            self->pub.receiveSlot = printLocalBroadCastTuple;
            break;

        case DestTupleRedistribute:
            self->pub.receiveSlot = printRedistributeTuple;
            break;

        case DestTupleLocalRedistribute:
            self->pub.receiveSlot = printLocalRedistributeTuple;
            break;

        case DestTupleLocalRoundRobin:
            self->pub.receiveSlot = printLocalRoundRobinTuple;
            break;

        case DestTupleHybrid:
            self->pub.receiveSlot = printHybridTuple;
            break;

        case DestBatchBroadCast:
            self->pub.sendBatch = printBroadCastBatchCompress;
            break;

        case DestBatchLocalBroadCast:
            self->pub.sendBatch = printLocalBroadCastBatch;
            break;

        case DestBatchRedistribute:
            self->pub.sendBatch = printRedistributeBatch;
            break;

        case DestBatchLocalRedistribute:
            self->pub.sendBatch = printLocalRedistributeBatch;
            break;

        case DestBatchLocalRoundRobin:
            self->pub.sendBatch = printLocalRoundRobinBatch;
            break;

        case DestBatchHybrid:
            self->pub.sendBatch = printHybridBatch;
            break;

        default:
            Assert(false);
            break;
    }

    self->pub.rStartup = printStreamStartup;
    self->pub.rShutdown = printStreamShutdown;
    self->pub.rDestroy = printtup_destroy;
    self->pub.finalizeLocalStream = NULL;
    self->pub.mydest = dest;
    self->pub.tmpContext = NULL;

    return (DestReceiver *)self;
}

/*
 * @Description: Flush data in the buffer
 *
 * @param[IN] dest: dest receiver.
 * @return void
 */
static void printStreamShutdown(DestReceiver *self)
{
    streamReceiver *rec = (streamReceiver *)self;
    rec->arg->flushStream();
}

/*
 * @Description: Send a tuple by broadcast
 *
 * @param[IN] tuple: tuple to send.
 * @param[IN] dest: dest receiver.
 * @return void
 */
static void printBroadCastTuple(TupleTableSlot *tuple, DestReceiver *self)
{
    streamReceiver *rec = (streamReceiver *)self;
    rec->arg->broadCastStream(tuple, self);
}

/*
 * @Description: Send a tuple by local broadcast
 *
 * @param[IN] tuple: tuple to send.
 * @param[IN] dest: dest receiver.
 * @return void
 */
static void printLocalBroadCastTuple(TupleTableSlot *tuple, DestReceiver *self)
{
    streamReceiver *rec = (streamReceiver *)self;
    rec->arg->localBroadCastStream(tuple);
}

/*
 * @Description: Send a tuple by redistribute
 *
 * @param[IN] tuple: tuple to send.
 * @param[IN] dest: dest receiver.
 * @return void
 */
static void printRedistributeTuple(TupleTableSlot *tuple, DestReceiver *self)
{
    streamReceiver *rec = (streamReceiver *)self;
    rec->arg->redistributeStream(tuple, self);
}

/*
 * @Description: Send a tuple by local redistribute
 *
 * @param[IN] tuple: tuple to send.
 * @param[IN] dest: dest receiver.
 * @return void
 */
static void printLocalRedistributeTuple(TupleTableSlot *tuple, DestReceiver *self)
{
    streamReceiver *rec = (streamReceiver *)self;
    rec->arg->localRedistributeStream(tuple);
}

/*
 * @Description: Send a tuple by local roundrobin
 *
 * @param[IN] tuple: tuple to send.
 * @param[IN] dest: dest receiver.
 * @return void
 */
static void printLocalRoundRobinTuple(TupleTableSlot *tuple, DestReceiver *self)
{
    streamReceiver *rec = (streamReceiver *)self;
    rec->arg->localRoundRobinStream(tuple);
}

/*
 * @Description: Send a tuple in hybrid ways, some data with special values
 *               shoule be sent in special way.
 *
 * @param[IN] tuple: tuple to send.
 * @param[IN] dest: dest receiver.
 * @return void
 */
static void printHybridTuple(TupleTableSlot *tuple, DestReceiver *self)
{
    streamReceiver *rec = (streamReceiver *)self;
    rec->arg->hybridStream(tuple, self);
}

/*
 * @Description: Send a batch by local broadcast
 *
 * @param[IN] batch: batch to send.
 * @param[IN] dest: dest receiver.
 * @return void
 */
static void printLocalBroadCastBatch(VectorBatch *batch, DestReceiver *self)
{
    streamReceiver *rec = (streamReceiver *)self;
    rec->arg->localBroadCastStream(batch);
}

/*
 * @Description: Send a batch by broadcast with compressed
 *
 * @param[IN] batch: batch to send.
 * @param[IN] dest: dest receiver.
 * @return void
 */
static void printBroadCastBatchCompress(VectorBatch *batch, DestReceiver *self)
{
    streamReceiver *rec = (streamReceiver *)self;
    rec->arg->broadCastStreamCompress(batch);
}

/*
 * @Description: Send a batch by redistribute
 *
 * @param[IN] batch: batch to send.
 * @param[IN] dest: dest receiver.
 * @return void
 */
static void printRedistributeBatch(VectorBatch *batch, DestReceiver *self)
{
    streamReceiver *rec = (streamReceiver *)self;
    rec->arg->redistributeStream(batch);
}

/*
 * @Description: Send a batch by local redistribute
 *
 * @param[IN] batch: batch to send.
 * @param[IN] dest: dest receiver.
 * @return void
 */
static void printLocalRedistributeBatch(VectorBatch *batch, DestReceiver *self)
{
    streamReceiver *rec = (streamReceiver *)self;
    rec->arg->localRedistributeStream(batch);
}

/*
 * @Description: Send a batch by local roundrobin
 *
 * @param[IN] batch: batch to send.
 * @param[IN] dest: dest receiver.
 * @return void
 */
static void printLocalRoundRobinBatch(VectorBatch *batch, DestReceiver *self)
{
    streamReceiver *rec = (streamReceiver *)self;
    rec->arg->localRoundRobinStream(batch);
}

/*
 * @Description: Send a batch in hybrid ways, some data with special values
 *               shoule be sent in special way.
 *
 * @param[IN] batch: batch to send.
 * @param[IN] dest: dest receiver.
 * @return void
 */
static void printHybridBatch(VectorBatch *batch, DestReceiver *self)
{
    streamReceiver *rec = (streamReceiver *)self;
    rec->arg->hybridStream(batch, self);
}

/*
 * @Description: Send a final signal to consumer for local stream
 *
 * @param[IN] dest: dest receiver.
 * @return void
 */
static void finalizeLocalStream(DestReceiver *self)
{
    streamReceiver *rec = (streamReceiver *)self;
    rec->arg->finalizeLocalStream();
}

static void printStreamStartup(DestReceiver *self, int operation, TupleDesc typeinfo)
{
    int i, res;
    int ndirection;

    streamReceiver *streamRec = (streamReceiver *)self;
    StreamProducer *arg = streamRec->arg;
    Portal portal = streamRec->portal;

    /* create buffer to be used for all messages */
    initStringInfo(&streamRec->buf);

    ndirection = arg->getConnNum();
    StreamTransport **transport = arg->getTransport();
    /* Prepare a DataBatch message */
    for (i = 0; i < ndirection; i++) {
        if (arg->netSwitchDest(i)) {
            /*
             * If we are supposed to emit row descriptions, then send the tuple
             * descriptor of the tuples.
             */
            if (streamRec->sendDescrip)
                SendRowDescriptionMessage(&streamRec->buf, typeinfo, FetchPortalTargetList(portal), portal->formats);
            res = pq_flush();
            if (res == EOF) {
                transport[i]->release();
            }
            arg->netStatusSave(i);
        }
    }
}

void SetStreamReceiverParams(DestReceiver *self, StreamProducer *arg, Portal portal)
{
    streamReceiver *myState = (streamReceiver *)self;

    Assert(myState->pub.mydest >= DestTupleBroadCast);

    myState->arg = arg;
    myState->portal = portal;
    myState->attrinfo = NULL;
    myState->nattrs = 0;
    myState->myinfo = NULL;
    myState->sendDescrip = false;

    if (STREAM_IS_LOCAL_NODE(arg->getParallelDesc().distriType))
        myState->pub.finalizeLocalStream = finalizeLocalStream;
}

/* ----------------
 *		Initialize: create a DestReceiver for printtup
 * ----------------
 */
DestReceiver *printtup_create_DR(CommandDest dest)
{
    DR_printtup *self = (DR_printtup *)palloc0(sizeof(DR_printtup));

    if (StreamTopConsumerAmI() == true)
        self->pub.receiveSlot = printtupStream;
    else
        self->pub.receiveSlot = printtup; /* might get changed later */

    self->pub.sendBatch = printBatch;
    self->pub.rStartup = printtup_startup;
    self->pub.rShutdown = printtup_shutdown;
    self->pub.rDestroy = printtup_destroy;
    self->pub.finalizeLocalStream = NULL;
    self->pub.mydest = dest;
    self->pub.tmpContext = NULL;

    /*
     * Send T message automatically if DestRemote, but not if
     * DestRemoteExecute
     */
    self->sendDescrip = (dest == DestRemote);

    self->attrinfo = NULL;
    self->nattrs = 0;
    self->myinfo = NULL;
    self->formats = NULL;

    return (DestReceiver *)self;
}

/*
 * Set parameters for a DestRemote (or DestRemoteExecute) receiver
 */
void SetRemoteDestReceiverParams(DestReceiver *self, Portal portal)
{
    DR_printtup *myState = (DR_printtup *)self;

    Assert(myState->pub.mydest == DestRemote || myState->pub.mydest == DestRemoteExecute);

    myState->portal = portal;

    if (PG_PROTOCOL_MAJOR(FrontendProtocol) < 3) {
        /*
         * In protocol 2.0 the Bind message does not exist, so there is no way
         * for the columns to have different print formats; it's sufficient to
         * look at the first one.
         */
        if (portal->formats && portal->formats[0] != 0)
            myState->pub.receiveSlot = printtup_internal_20;
        else
            myState->pub.receiveSlot = printtup_20;
    }
}

static void printtup_startup(DestReceiver *self, int operation, TupleDesc typeinfo)
{
    DR_printtup *myState = (DR_printtup *)self;
    Portal portal = myState->portal;

    /* create buffer to be used for all messages */
    initStringInfo(&myState->buf);

    if (PG_PROTOCOL_MAJOR(FrontendProtocol) < 3) {
        /*
         * Send portal name to frontend (obsolete cruft, gone in proto 3.0)
         *
         * If portal name not specified, use "blank" portal.
         */
        const char *portalName = portal->name;

        if (portalName == NULL || portalName[0] == '\0')
            portalName = "blank";

        pq_puttextmessage('P', portalName);
    }

    /*
     * If we are supposed to emit row descriptions, then send the tuple
     * descriptor of the tuples.
     */
    if (myState->sendDescrip)
        SendRowDescriptionMessage(&myState->buf, typeinfo, FetchPortalTargetList(portal), portal->formats);

    /* ----------------
     * We could set up the derived attr info at this time, but we postpone it
     * until the first call of printtup, for 2 reasons:
     * 1. We don't waste time (compared to the old way) if there are no
     *	  tuples at all to output.
     * 2. Checking in printtup allows us to handle the case that the tuples
     *	  change type midway through (although this probably can't happen in
     *	  the current executor).
     * ----------------
     */
}

/*
 * SendRowDescriptionMessage --- send a RowDescription message to the frontend
 *
 * Notes: the TupleDesc has typically been manufactured by ExecTypeFromTL()
 * or some similar function; it does not contain a full set of fields.
 * The targetlist will be NIL when executing a utility function that does
 * not have a plan.  If the targetlist isn't NIL then it is a Query node's
 * targetlist; it is up to us to ignore resjunk columns in it.	The formats[]
 * array pointer might be NULL (if we are doing Describe on a prepared stmt);
 * send zeroes for the format codes in that case.
 */
void SendRowDescriptionMessage(StringInfo buf, TupleDesc typeinfo, List *targetlist, int16 *formats)
{
    int natts = typeinfo->natts;
    int proto = PG_PROTOCOL_MAJOR(FrontendProtocol);

    /* tuple descriptor message type */
    pq_beginmessage_reuse(buf, 'T');
    /* # of attrs in tuples */
    pq_sendint16(buf, natts);

    if (proto >= 3)
        SendRowDescriptionCols_3(buf, typeinfo, targetlist, formats);
    else
        SendRowDescriptionCols_2(buf, typeinfo, targetlist, formats);

    pq_endmessage_reuse(buf);
}

/*
 * Send description for each column when using v3+ protocol
 */
static void SendRowDescriptionCols_3(StringInfo buf, TupleDesc typeinfo, List *targetlist, int16 *formats)
{
    Form_pg_attribute *attrs = typeinfo->attrs;
    int natts = typeinfo->natts;
    int i;
    ListCell *tlist_item = list_head(targetlist);
    int typenameLen = 0;

    /*
     * Preallocate memory for the entire message to be sent. That allows to
     * use the significantly faster inline pqformat.h functions and to avoid
     * reallocations.
     *
     * Have to overestimate the size of the column-names, to account for
     * character set overhead.
     */
    if (IsConnFromCoord())
        typenameLen = (2 * NAMEDATALEN + 1) * MAX_CONVERSION_GROWTH;

    enlargeStringInfo(buf, (NAMEDATALEN * MAX_CONVERSION_GROWTH /* attname */
                            + sizeof(Oid)                       /* resorigtbl */
                            + sizeof(AttrNumber)                /* resorigcol */
                            + sizeof(Oid)                       /* atttypid */
                            + sizeof(int16)                     /* attlen */
                            + sizeof(int32)                     /* attypmod */
                            + typenameLen                       /* typename */
                            + sizeof(int16)                     /* format */
                            ) * natts);

    for (i = 0; i < natts; ++i) {
        Oid atttypid = attrs[i]->atttypid;
        int32 atttypmod = attrs[i]->atttypmod;
        if (IsClientLogicType(atttypid) && atttypmod == -1) {
            elog(DEBUG1, "client logic without original type is sent to client");
        }

        writeString(buf, NameStr(attrs[i]->attname), true);

#ifdef PGXC
        /*
         * for analyze global stats, because DN will send sample rows to CN,
         * if we encounter droped columns, we should send it to CN. but atttypid of dropped column
         * is invalid in pg_attribute, it will generate error, so we should do special process for the reason.
         */
        if (IsConnFromCoord() && attrs[i]->attisdropped)
            atttypid = UNKNOWNOID;
#endif

        /* column ID info appears in protocol 3.0 and up */
        /* Do we have a non-resjunk tlist item? */
        while (tlist_item &&
#ifdef STREAMPLAN
               StreamTopConsumerAmI() == false && StreamThreadAmI() == false &&
#endif
               ((TargetEntry *)lfirst(tlist_item))->resjunk)
            tlist_item = lnext(tlist_item);
        if (tlist_item != NULL) {
            TargetEntry *tle = (TargetEntry *)lfirst(tlist_item);

            pq_writeint32(buf, tle->resorigtbl);
            pq_writeint16(buf, tle->resorigcol);
            tlist_item = lnext(tlist_item);
        } else {
            /* No info available, so send zeroes */
            pq_writeint32(buf, 0);
            pq_writeint16(buf, 0);
        }

        /* If column is a domain, send the base type and typmod instead */
        atttypid = getBaseTypeAndTypmod(atttypid, &atttypmod);
        pq_writeint32(buf, atttypid);
        pq_writeint16(buf, attrs[i]->attlen);
        /* typmod appears in protocol 2.0 and up */
        pq_writeint32(buf, atttypmod);

        /*
         * Send the type name from a openGauss backend node.
         * This preserves from OID inconsistencies as architecture is shared nothing.
         */
        /* Description: unified cn/dn cn/client  tupledesc data format under normal type. */
        if (IsConnFromCoord() && atttypid >= FirstBootstrapObjectId) {
            char *typenameVar = "";
            typenameVar = get_typename_with_namespace(atttypid);
            pq_writestring(buf, typenameVar);
        }

        /* format info appears in protocol 3.0 and up */
        if (formats != NULL)
            pq_writeint16(buf, formats[i]);
        else
            pq_writeint16(buf, 0);
    }
}

/*
 * Send description for each column when using v2 protocol
 */
static void SendRowDescriptionCols_2(StringInfo buf, TupleDesc typeinfo, List *targetlist, int16 *formats)
{
    Form_pg_attribute *attrs = typeinfo->attrs;
    int natts = typeinfo->natts;
    int i;

    for (i = 0; i < natts; ++i) {
        Oid atttypid = attrs[i]->atttypid;
        int32 atttypmod = attrs[i]->atttypmod;

        writeString(buf, NameStr(attrs[i]->attname), false);

#ifdef PGXC
        /*
         * for analyze global stats, because DN will send sample rows to CN,
         * if we encounter droped columns, we should send it to CN. but atttypid of dropped column
         * is invalid in pg_attribute, it will generate error, so we should do special process for the reason.
         */
        if (IsConnFromCoord() && attrs[i]->attisdropped)
            atttypid = UNKNOWNOID;
#endif

        /* If column is a domain, send the base type and typmod instead */
        atttypid = getBaseTypeAndTypmod(atttypid, &atttypmod);
        pq_sendint32(buf, atttypid);
        pq_sendint16(buf, attrs[i]->attlen);
        /* typmod appears in protocol 2.0 and up */
        pq_sendint32(buf, atttypmod);

        /*
         * Send the type name from a openGauss backend node.
         * This preserves from OID inconsistencies as architecture is shared nothing.
         */
        /* Description: unified cn/dn cn/client  tupledesc data format under normal type. */
        if (IsConnFromCoord() && atttypid >= FirstBootstrapObjectId) {
            char *typenameVar = "";
            typenameVar = get_typename_with_namespace(atttypid);
            pq_sendstring(buf, typenameVar);
        }
    }
}

/*
 * Using pq_writestring in SendRowDescriptionCols_3 and pq_sendstring in SendRowDescriptionCols_2.
 */
static void writeString(StringInfo buf, const char *name, bool isWrite)
{
    char *res = (char *)name;

#ifndef ENABLE_MULTIPLE_NODES
    /*
     * Uppercasing attribute name only works in ORA compatibility mode and centralized environment.
     * If the letters is all lowercase, return the result after converting to uppercase.
     */
    char objectNameUppercase[NAMEDATALEN] = {'\0'};
    if (u_sess->attr.attr_sql.sql_compatibility == A_FORMAT && u_sess->attr.attr_sql.uppercase_attribute_name &&
        checkNeedUpperAndToUpper(objectNameUppercase, name)) {
        res = objectNameUppercase;
    }
#endif

    if (likely(isWrite)) {
        pq_writestring(buf, res);
    } else {
        pq_sendstring(buf, res);
    }
}

#ifndef ENABLE_MULTIPLE_NODES
/*
 * Check whether the letters is all lowercase. If yes, then needUpper is true.
 * Use dest to save the result after converting to uppercase.
 */
static bool checkNeedUpperAndToUpper(char *dest, const char *source)
{
    size_t i = 0;
    bool needUpper = true;
    while (*source != '\0') {
        int mblen = pg_mblen(source);
        /*
         * If mblen == 1, then need to further determine whether this single-byte character is an uppercase letter.
         * Otherwise, copy directly from source to dest.
         */
        if (mblen == 1) {
            /* this single-byte character is an uppercase letter, do not need upper. */
            if (unlikely(isupper(*source))) {
                needUpper = false;
                break;
            }
            dest[i++] = toupper(*source++);
        } else {
            for (int j = 0; j < mblen; j++) {
                dest[i++] = *source++;
            }
        }
    }
    dest[i] = '\0';
    return needUpper;
}
#endif

/*
 * Get the lookup info that printtup() needs
 */
static void printtup_prepare_info(DR_printtup *myState, TupleDesc typeinfo, int numAttrs)
{
    int16 *formats = myState->portal != NULL ? myState->portal->formats : myState->formats;
    int i;

    /* get rid of any old data */
    if (myState->myinfo != NULL) {
        pfree(myState->myinfo);
    }
    myState->myinfo = NULL;

    myState->attrinfo = typeinfo;
    myState->nattrs = numAttrs;
    if (numAttrs <= 0) {
        return;
    }


    if (myState->portal != NULL && myState->portal->tupDesc != NULL) {
#ifdef USE_ASSERT_CHECKING
        Assert(numAttrs <= myState->portal->tupDesc->natts);
#else
        if (numAttrs > myState->portal->tupDesc->natts) {
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                    errmsg("num attrs from DN is %d, mismatch num attrs %d in portal", numAttrs, myState->portal->tupDesc->natts)));
        }
#endif
    }

    myState->myinfo = (PrinttupAttrInfo *)palloc0(numAttrs * sizeof(PrinttupAttrInfo));

    for (i = 0; i < numAttrs; i++) {
        PrinttupAttrInfo *thisState = myState->myinfo + i;
        int16 format = (formats ? formats[i] : 0);

        /*
         * for analyze global stats, because DN will send sample rows to CN,
         * if we encounter droped columns, we should send it to CN. but atttypid of dropped column
         * is invalid in pg_attribute, it will generate error, so we should do special process for the reason.
         */
        if (typeinfo->attrs[i]->attisdropped) {
            typeinfo->attrs[i]->atttypid = UNKNOWNOID;
        }

        thisState->format = format;
        if (format == 0) {
            getTypeOutputInfo(typeinfo->attrs[i]->atttypid, &thisState->typoutput, &thisState->typisvarlena);
            fmgr_info(thisState->typoutput, &thisState->finfo);
        } else if (format == 1) {
            getTypeBinaryOutputInfo(typeinfo->attrs[i]->atttypid, &thisState->typsend, &thisState->typisvarlena);
            fmgr_info(thisState->typsend, &thisState->finfo);
        } else {
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("unsupported format code: %d", format)));
        }
    }
}

/*
 * Get the lookup info that printtup() needs
 * this function mainly to tackle junk field
 */
static void printtup_prepare_info_for_stream(DR_printtup *myState, TupleDesc typeinfo, int numAttrs)
{
    int i;

    /* get rid of any old data */
    if (myState->myinfo != NULL) {
        pfree(myState->myinfo);
    }
    myState->myinfo = NULL;

    myState->attrinfo = typeinfo;
    myState->nattrs = numAttrs;
    if (numAttrs <= 0) {
        return;
    }

    myState->myinfo = (PrinttupAttrInfo *)palloc0(numAttrs * sizeof(PrinttupAttrInfo));

    /* let's say for stream thread only support format = 0; */
    for (i = 0; i < numAttrs; i++) {
        PrinttupAttrInfo *thisState = myState->myinfo + i;
        thisState->format = 0;
        getTypeOutputInfo(typeinfo->attrs[i]->atttypid, &thisState->typoutput, &thisState->typisvarlena);
        fmgr_info(thisState->typoutput, &thisState->finfo);
    }
}

inline MemoryContext changeToTmpContext(DestReceiver *self)
{
    MemoryContext old_context = CurrentMemoryContext;
    if (self->tmpContext != NULL) {
        old_context = MemoryContextSwitchTo(self->tmpContext);
    }
    return old_context;
}

void assembleStreamMessage(TupleTableSlot *slot, DestReceiver *self, StringInfo buf)
{
    TupleDesc typeinfo = slot->tts_tupleDescriptor;
    DR_printtup *myState = (DR_printtup *)self;
    int natts = typeinfo->natts;
    int i;

    StreamTimeSerilizeStart(t_thrd.pgxc_cxt.GlobalNetInstr);
    if (slot->tts_dataRow) {
        Assert(buf->len == 0);

        /*
         * Prepare a DataRow message
         */
        buf->cursor = 'D';
        pq_sendbytes(buf, slot->tts_dataRow, slot->tts_dataLen);
    } else {
        /* Set or update my derived attribute info, if needed */
        if (myState->attrinfo != typeinfo || myState->nattrs != natts)
            printtup_prepare_info_for_stream(myState, typeinfo, natts);

        /* Make sure the tuple is fully deconstructed */
        tableam_tslot_getallattrs(slot);

        MemoryContext old_context = changeToTmpContext(self);

        /* Prepare a DataRow message */
        Assert(buf->len == 0);
        buf->cursor = 'D';
        pq_sendint16(buf, natts);

        /*
         * send the attributes of this tuple
         */
        for (i = 0; i < natts; ++i) {
            PrinttupAttrInfo *thisState = myState->myinfo + i;
            Datum origattr = slot->tts_values[i];
            Datum attr = static_cast<uintptr_t>(0);

            if (slot->tts_isnull[i]) {
                pq_sendint32(buf, (uint32)-1);
                continue;
            }

            /*
             * If we have a toasted datum, forcibly detoast it here to avoid
             * memory leakage inside the type's output routine.
             */
            if (thisState->typisvarlena)
                attr = PointerGetDatum(PG_DETOAST_DATUM(origattr));
            else
                attr = origattr;

            if (thisState->format == 0) {
                /* Text output */
                char *outputstr = NULL;

                outputstr = OutputFunctionCall(&thisState->finfo, attr);
                pq_sendcountedtext(buf, outputstr, strlen(outputstr), false);
                pfree(outputstr);
            } else {
                /* Binary output */
                bytea *outputbytes = NULL;

                outputbytes = SendFunctionCall(&thisState->finfo, attr);
                pq_sendint32(buf, VARSIZE(outputbytes) - VARHDRSZ);
                pq_sendbytes(buf, VARDATA(outputbytes), VARSIZE(outputbytes) - VARHDRSZ);
                pfree(outputbytes);
            }

            /* Clean up detoasted copy, if any */
            if (DatumGetPointer(attr) != DatumGetPointer(origattr))
                pfree(DatumGetPointer(attr));
        }

        (void)MemoryContextSwitchTo(old_context);
    }

    StreamTimeSerilizeEnd(t_thrd.pgxc_cxt.GlobalNetInstr);
}

/* ----------------
 *		printtup for stream--- print a stream tuple in protocol 3.0
 * ----------------
 */
void printtupStream(TupleTableSlot *slot, DestReceiver *self)
{
    TupleDesc typeinfo = slot->tts_tupleDescriptor;
    DR_printtup *myState = (DR_printtup *)self;
    StringInfo buf = &myState->buf;
    int natts = typeinfo->natts;
    int i;

    StreamTimeSerilizeStart(t_thrd.pgxc_cxt.GlobalNetInstr);

#ifdef PGXC
    /*
     * If we are having DataRow-based tuple we do not have to encode attribute
     * values, just send over the DataRow message as we received it from the
     * Datanode
     */
    if (slot->tts_dataRow) {
        pq_beginmessage_reuse(buf, 'D');
        appendBinaryStringInfo(buf, slot->tts_dataRow, slot->tts_dataLen);
        AddCheckInfo(buf);
        pq_endmessage_reuse(buf);
        StreamTimeSerilizeEnd(t_thrd.pgxc_cxt.GlobalNetInstr);
        return;
    }
#endif
    /* Set or update my derived attribute info, if needed */
    if (myState->attrinfo != typeinfo || myState->nattrs != natts)
        printtup_prepare_info_for_stream(myState, typeinfo, natts);

    /* Make sure the tuple is fully deconstructed */
    tableam_tslot_getallattrs(slot);

    MemoryContext old_context = changeToTmpContext(self);
    /*
     * Prepare a DataRow message
     */
    pq_beginmessage_reuse(buf, 'D');

    pq_sendint16(buf, natts);

    /*
     * send the attributes of this tuple
     */
    for (i = 0; i < natts; ++i) {
        PrinttupAttrInfo *thisState = myState->myinfo + i;
        Datum origattr = slot->tts_values[i];
        Datum attr = static_cast<uintptr_t>(0);

        if (slot->tts_isnull[i]) {
            pq_sendint32(buf, (uint32)-1);
            continue;
        }

        /*
         * If we have a toasted datum, forcibly detoast it here to avoid
         * memory leakage inside the type's output routine.
         */
        if (thisState->typisvarlena)
            attr = PointerGetDatum(PG_DETOAST_DATUM(origattr));
        else
            attr = origattr;

        if (thisState->format == 0) {
            /* Text output */
            char *outputstr = NULL;

            outputstr = OutputFunctionCall(&thisState->finfo, attr);
            pq_sendcountedtext(buf, outputstr, strlen(outputstr), false);
            pfree(outputstr);
        } else {
            /* Binary output */
            bytea *outputbytes = NULL;

            outputbytes = SendFunctionCall(&thisState->finfo, attr);
            pq_sendint32(buf, VARSIZE(outputbytes) - VARHDRSZ);
            pq_sendbytes(buf, VARDATA(outputbytes), VARSIZE(outputbytes) - VARHDRSZ);
            pfree(outputbytes);
        }

        /* Clean up detoasted copy, if any */
        if (DatumGetPointer(attr) != DatumGetPointer(origattr))
            pfree(DatumGetPointer(attr));
    }

    (void)MemoryContextSwitchTo(old_context);
    StreamTimeSerilizeEnd(t_thrd.pgxc_cxt.GlobalNetInstr);

    AddCheckInfo(buf);
    pq_endmessage_reuse(buf);
}

/* ----------------
 *		printtup --- print a tuple in protocol 3.0
 * ----------------
 */
void printBatch(VectorBatch *batch, DestReceiver *self)
{
    DR_printtup *myState = (DR_printtup *)self;
    StringInfo buf = &myState->buf;
    pq_beginmessage_reuse(buf, 'B');
    batch->SerializeWithLZ4Compress(buf);
    AddCheckInfo(buf);
    pq_endmessage_reuse(buf);
}

/* ----------------
 *		printtup --- print a tuple in protocol 3.0
 * ----------------
 */
void printtup(TupleTableSlot *slot, DestReceiver *self)
{
    TupleDesc typeinfo = slot->tts_tupleDescriptor;
    DR_printtup *myState = (DR_printtup *)self;
    StringInfo buf = &myState->buf;
    int natts = typeinfo->natts;
    int i;
    bool binary = false;
    /* just as we define in backend/commands/analyze.cpp */
#define WIDTH_THRESHOLD 1024

    StreamTimeSerilizeStart(t_thrd.pgxc_cxt.GlobalNetInstr);
    /* Set or update my derived attribute info, if needed */
    if (myState->attrinfo != typeinfo || myState->nattrs != natts)
        printtup_prepare_info(myState, typeinfo, natts);

#ifdef PGXC

    /*
     * The datanodes would have sent all attributes in TEXT form. But
     * if the client has asked for any attribute to be sent in a binary format,
     * then we must decode the datarow and send every attribute in the format
     * that the client has asked for. Otherwise its ok to just forward the
     * datarow as it is
     */
    for (i = 0; i < natts; ++i) {
        PrinttupAttrInfo *thisState = myState->myinfo + i;
        if (thisState->format != 0)
            binary = true;
    }

    /*
     * If we are having DataRow-based tuple we do not have to encode attribute
     * values, just send over the DataRow message as we received it from the
     * Datanode
     */
    if (slot->tts_dataRow != NULL && (pg_get_client_encoding() == GetDatabaseEncoding()) && !binary) {
        pq_beginmessage_reuse(buf, 'D');
        appendBinaryStringInfo(buf, slot->tts_dataRow, slot->tts_dataLen);
        AddCheckInfo(buf);
        pq_endmessage_reuse(buf);
        StreamTimeSerilizeEnd(t_thrd.pgxc_cxt.GlobalNetInstr);
        return;
    }
#endif

    /* Make sure the tuple is fully deconstructed */
    tableam_tslot_getallattrs(slot);

    MemoryContext old_context = changeToTmpContext(self);
    /*
     * Prepare a DataRow message
     */
    pq_beginmessage_reuse(buf, 'D');

    pq_sendint16(buf, natts);

    /*
     * send the attributes of this tuple
     */
    for (i = 0; i < natts; ++i) {
        PrinttupAttrInfo *thisState = myState->myinfo + i;
        Datum origattr = slot->tts_values[i];
        Datum attr = static_cast<uintptr_t>(0);

        /*
         * skip null value attribute,
         * we need to skip the droped columns for analyze global stats.
         */
        if (slot->tts_isnull[i] || typeinfo->attrs[i]->attisdropped) {
            pq_sendint32(buf, (uint32)-1);
            continue;
        }

        if (typeinfo->attrs[i]->atttypid == ANYARRAYOID && slot->tts_dataRow != NULL) {
            /*
             * For ANYARRAY type, the not null DataRow-based tuple indicates the value in
             * origattr had been converted to CSTRING type previously by using anyarray_out.
             * just send over the DataRow message as we received it.
             */
            pq_sendcountedtext(buf, (char *)origattr, strlen((char *)origattr), false);
        } else {
            /*
             * If we have a toasted datum, forcibly detoast it here to avoid
             * memory leakage inside the type's output routine.
             */
            if (thisState->typisvarlena)
                attr = PointerGetDatum(PG_DETOAST_DATUM(origattr));
            else
                attr = origattr;

            if (thisState->format == 0) {
                /* Text output */
                char *outputstr = NULL;

                outputstr = OutputFunctionCall(&thisState->finfo, attr);
                if (thisState->typisvarlena && self->forAnalyzeSampleTuple &&
                    (typeinfo->attrs[i]->atttypid == BYTEAOID || typeinfo->attrs[i]->atttypid == CHAROID ||
                     typeinfo->attrs[i]->atttypid == TEXTOID || typeinfo->attrs[i]->atttypid == BLOBOID ||
                     typeinfo->attrs[i]->atttypid == CLOBOID || typeinfo->attrs[i]->atttypid == RAWOID ||
                     typeinfo->attrs[i]->atttypid == BPCHAROID || typeinfo->attrs[i]->atttypid == VARCHAROID ||
                     typeinfo->attrs[i]->atttypid == NVARCHAR2OID) &&
                    strlen(outputstr) > WIDTH_THRESHOLD * 2) {
                    /*
                     * in compute_scalar_stats, we just skip detoast value if value size is
                     * bigger than WIDTH_THRESHOLD to avoid consuming too much memory
                     * during analysis, so we just send as WIDTH_THRESHOLD + 4 to cn so that
                     * it can use as little memory as we can to satisfy the threshold
                     */
                    const int length = WIDTH_THRESHOLD + 4;
                    text *txt = NULL;
                    Datum str;
                    text *result = NULL;

                    txt = cstring_to_text(outputstr);
                    pfree(outputstr);

                    str = DirectFunctionCall3(substrb_with_lenth, PointerGetDatum(txt), Int32GetDatum(0),
                                              Int32GetDatum(length));
                    result = DatumGetTextP(str);
                    if (result != txt)
                        pfree(txt);

                    outputstr = TextDatumGetCString(str);
                    pfree(result);
                }
                pq_sendcountedtext(buf, outputstr, strlen(outputstr), false);
                pfree(outputstr);
            } else {
                /* Binary output */
                bytea *outputbytes = NULL;

                outputbytes = SendFunctionCall(&thisState->finfo, attr);
                pq_sendint32(buf, VARSIZE(outputbytes) - VARHDRSZ);
                pq_sendbytes(buf, VARDATA(outputbytes), VARSIZE(outputbytes) - VARHDRSZ);
                pfree(outputbytes);
            }

            /* Clean up detoasted copy, if any */
            if (DatumGetPointer(attr) != DatumGetPointer(origattr))
                pfree(DatumGetPointer(attr));
        }
    }

    (void)MemoryContextSwitchTo(old_context);
    StreamTimeSerilizeEnd(t_thrd.pgxc_cxt.GlobalNetInstr);

    AddCheckInfo(buf);
    pq_endmessage_reuse(buf);
}

/* ----------------
 *		printtup_20 --- print a tuple in protocol 2.0
 * ----------------
 */
static void printtup_20(TupleTableSlot *slot, DestReceiver *self)
{
    TupleDesc typeinfo = slot->tts_tupleDescriptor;
    DR_printtup *myState = (DR_printtup *)self;
    StringInfo buf = &myState->buf;
    int natts = typeinfo->natts;
    int i, j;
    uint k;

    /* Set or update my derived attribute info, if needed */
    if (myState->attrinfo != typeinfo || myState->nattrs != natts)
        printtup_prepare_info(myState, typeinfo, natts);

    /* Make sure the tuple is fully deconstructed */
    tableam_tslot_getallattrs(slot);

    MemoryContext old_context = changeToTmpContext(self);

    /*
     * tell the frontend to expect new tuple data (in ASCII style)
     */
    pq_beginmessage_reuse(buf, 'D');

    /*
     * send a bitmap of which attributes are not null
     */
    j = 0;
    k = 1U << 7;
    for (i = 0; i < natts; ++i) {
        if (!slot->tts_isnull[i])
            j |= k; /* set bit if not null */
        k >>= 1;
        if (k == 0) { /* end of byte? */
            pq_sendint8(buf, j);
            j = 0;
            k = 1U << 7;
        }
    }
    if (k != (1U << 7)) /* flush last partial byte */
        pq_sendint8(buf, j);

    /*
     * send the attributes of this tuple
     */
    for (i = 0; i < natts; ++i) {
        PrinttupAttrInfo *thisState = myState->myinfo + i;
        Datum origattr = slot->tts_values[i];
        Datum attr = static_cast<uintptr_t>(0);
        char *outputstr = NULL;

        if (slot->tts_isnull[i])
            continue;

        Assert(thisState->format == 0);

        /*
         * If we have a toasted datum, forcibly detoast it here to avoid
         * memory leakage inside the type's output routine.
         */
        if (thisState->typisvarlena)
            attr = PointerGetDatum(PG_DETOAST_DATUM(origattr));
        else
            attr = origattr;

        outputstr = OutputFunctionCall(&thisState->finfo, attr);
        pq_sendcountedtext(buf, outputstr, strlen(outputstr), true);
        pfree(outputstr);

        /* Clean up detoasted copy, if any */
        if (DatumGetPointer(attr) != DatumGetPointer(origattr))
            pfree(DatumGetPointer(attr));
    }

    (void)MemoryContextSwitchTo(old_context);
    pq_endmessage_reuse(buf);
}

/* ----------------
 *		printtup_shutdown
 * ----------------
 */
static void printtup_shutdown(DestReceiver *self)
{
    DR_printtup *myState = (DR_printtup *)self;

    if (myState->myinfo != NULL)
        pfree(myState->myinfo);
    myState->myinfo = NULL;

    myState->attrinfo = NULL;
}

/* ----------------
 *		printtup_destroy
 * ----------------
 */
static void printtup_destroy(DestReceiver *self)
{
    pfree(self);
}

/* ----------------
 *		printatt
 * ----------------
 */
static void printatt(unsigned attributeId, Form_pg_attribute attributeP, const char *value)
{
    printf("\t%2u: %s%s%s%s\t(typeid = %u, len = %d, typmod = %d, byval = %c)\n", attributeId,
           NameStr(attributeP->attname), value != NULL ? " = \"" : "", value != NULL ? value : "",
           value != NULL ? "\"" : "", (unsigned int)(attributeP->atttypid), attributeP->attlen, attributeP->atttypmod,
           attributeP->attbyval ? 't' : 'f');
}

/* ----------------
 *		debugStartup - prepare to print tuples for an interactive backend
 * ----------------
 */
void debugStartup(DestReceiver *self, int operation, TupleDesc typeinfo)
{
    int natts = typeinfo->natts;
    Form_pg_attribute *attinfo = typeinfo->attrs;
    int i;

    /*
     * show the return type of the tuples
     */
    for (i = 0; i < natts; ++i)
        printatt((unsigned)i + 1, attinfo[i], NULL);
    printf("\t----\n");
}

/* ----------------
 *		debugtup - print one tuple for an interactive backend
 * ----------------
 */
void debugtup(TupleTableSlot *slot, DestReceiver *self)
{
    TupleDesc typeinfo = slot->tts_tupleDescriptor;
    int natts = typeinfo->natts;
    int i;
    Datum origattr, attr;
    char *value = NULL;
    bool isnull = false;
    Oid typoutput;
    bool typisvarlena = false;

    for (i = 0; i < natts; ++i) {
        origattr = tableam_tslot_getattr(slot, i + 1, &isnull);
        if (isnull) {
            continue;
        }
        getTypeOutputInfo(typeinfo->attrs[i]->atttypid, &typoutput, &typisvarlena);

        /*
         * If we have a toasted datum, forcibly detoast it here to avoid
         * memory leakage inside the type's output routine.
         */
        if (typisvarlena) {
            attr = PointerGetDatum(PG_DETOAST_DATUM(origattr));
        } else {
            attr = origattr;
        }

        value = OidOutputFunctionCall(typoutput, attr);

        printatt((unsigned)i + 1, typeinfo->attrs[i], value);

        pfree(value);

        /* Clean up detoasted copy, if any */
        if (DatumGetPointer(attr) != DatumGetPointer(origattr)) {
            pfree(DatumGetPointer(attr));
        }
    }
    printf("\t----\n");
}

/* ----------------
 *		printtup_internal_20 --- print a binary tuple in protocol 2.0
 *
 * We use a different message type, i.e. 'B' instead of 'D' to
 * indicate a tuple in internal (binary) form.
 *
 * This is largely same as printtup_20, except we use binary formatting.
 * ----------------
 */
static void printtup_internal_20(TupleTableSlot *slot, DestReceiver *self)
{
    TupleDesc typeinfo = slot->tts_tupleDescriptor;
    DR_printtup *myState = (DR_printtup *)self;
    StringInfo buf = &myState->buf;
    int natts = typeinfo->natts;
    int i, j;
    uint k;

    /* Set or update my derived attribute info, if needed */
    if (myState->attrinfo != typeinfo || myState->nattrs != natts)
        printtup_prepare_info(myState, typeinfo, natts);

    /* Make sure the tuple is fully deconstructed */
    tableam_tslot_getallattrs(slot);

    /*
     * tell the frontend to expect new tuple data (in binary style)
     */
    pq_beginmessage_reuse(buf, 'B');

    /*
     * send a bitmap of which attributes are not null
     */
    j = 0;
    k = 1U << 7;
    for (i = 0; i < natts; ++i) {
        if (!slot->tts_isnull[i])
            j |= k; /* set bit if not null */
        k >>= 1;
        if (k == 0) { /* end of byte? */
            pq_sendint8(buf, j);
            j = 0;
            k = 1U << 7;
        }
    }
    if (k != (1U << 7)) /* flush last partial byte */
        pq_sendint8(buf, j);

    /*
     * send the attributes of this tuple
     */
    for (i = 0; i < natts; ++i) {
        PrinttupAttrInfo *thisState = myState->myinfo + i;
        Datum origattr = slot->tts_values[i];
        Datum attr = static_cast<uintptr_t>(0);
        bytea *outputbytes = NULL;

        if (slot->tts_isnull[i])
            continue;

        Assert(thisState->format == 1);

        /*
         * If we have a toasted datum, forcibly detoast it here to avoid
         * memory leakage inside the type's output routine.
         */
        if (thisState->typisvarlena)
            attr = PointerGetDatum(PG_DETOAST_DATUM(origattr));
        else
            attr = origattr;

        outputbytes = SendFunctionCall(&thisState->finfo, attr);
        /* We assume the result will not have been toasted */
        pq_sendint32(buf, VARSIZE(outputbytes) - VARHDRSZ);
        pq_sendbytes(buf, VARDATA(outputbytes), VARSIZE(outputbytes) - VARHDRSZ);
        pfree(outputbytes);

        /* Clean up detoasted copy, if any */
        if (DatumGetPointer(attr) != DatumGetPointer(origattr))
            pfree(DatumGetPointer(attr));
    }

    pq_endmessage_reuse(buf);
}

/*
 * @Description:
 *    Assemble stream batch message based on choosed compress method.
 *
 * @param[IN] batch: batch to be send through stream.
 * @param[OUT] buf: store the message of batch.
 * @return void
 *
 */
void assembleStreamBatchMessage(BatchCompressType ctype, VectorBatch *batch, StringInfo buf)
{
    buf->cursor = 'B';
    switch (ctype) {
        case BCT_NOCOMP:
            batch->SerializeWithoutCompress(buf);
            break;
        case BCT_LZ4:
            batch->SerializeWithLZ4Compress(buf);
            break;
        default:
            ereport(ERROR, (errcode(ERRCODE_DATATYPE_MISMATCH), errmsg("unrecognized batch compress type")));
    }
}

inline void AddCheckInfo(StringInfo buf)
{
    StringInfoData buf_check;
    bool is_check_added = false;

    /* add check info  for datanode and coordinator */
    if (IsConnFromCoord()) {
#ifdef USE_ASSERT_CHECKING
        initStringInfo(&buf_check);
        AddCheckMessage(&buf_check, buf, false);
        is_check_added = true;
#else
        if (anls_opt_is_on(ANLS_STREAM_DATA_CHECK)) {
            initStringInfo(&buf_check);
            AddCheckMessage(&buf_check, buf, false);
            is_check_added = true;
        }
#endif

        if (unlikely(is_check_added)) {
            pfree(buf->data);
            buf->len = buf_check.len;
            buf->maxlen = buf_check.maxlen;
            buf->data = buf_check.data;
        }
    }
}
