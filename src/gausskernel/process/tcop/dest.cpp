/* -------------------------------------------------------------------------
 *
 * dest.cpp
 *    support for communication destinations
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *    src/gausskernel/process/tcop/dest.cpp
 *
 * -------------------------------------------------------------------------
 */
/*
 *	 INTERFACE ROUTINES
 *		BeginCommand - initialize the destination at start of command
 *		CreateDestReceiver - create tuple receiver object for destination
 *		EndCommand - clean up the destination at end of command
 *		NullCommand - tell dest that an empty query string was recognized
 *		ReadyForQuery - tell dest that we are ready for a new query
 *		ReadyForQuery_noblock - Same as above but non-blocking
 *
 *	 NOTES
 *		These routines do the appropriate work before and after
 *		tuples are returned by a query to keep the backend and the
 *		"destination" portals synchronized.
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/printtup.h"
#include "access/xact.h"
#include "commands/copy.h"
#include "commands/createas.h"
#include "db4ai/create_model.h"
#include "commands/matview.h"
#include "executor/functions.h"
#include "executor/spi.h"
#include "executor/tstoreReceiver.h"
#include "libpq/libpq.h"
#include "libpq/pqformat.h"
#include "utils/portal.h"

#include "tcop/stmt_retry.h"

typedef DestReceiver* (*ProcDestReciverHook)(CommandDest dest);

/* ----------------
 *		dummy DestReceiver functions
 * ----------------
 */
static void donothingReceive(TupleTableSlot* slot, DestReceiver* self)
{}

static void donothingStartup(DestReceiver* self, int operation, TupleDesc typeinfo)
{}

static void donothingCleanup(DestReceiver* self)
{
    /* this is used for both shutdown and destroy methods */
}

/* ----------------
 *		static DestReceiver structs for dest types needing no local state
 * ----------------
 */
static DestReceiver donothingDR = {donothingReceive, donothingStartup, donothingCleanup, donothingCleanup, DestNone};

static DestReceiver debugtupDR = {debugtup, debugStartup, donothingCleanup, donothingCleanup, DestDebug};

void InitSpiPrinttupDR(DestReceiver* dr)
{
    dr->receiveSlot = spi_printtup;
    dr->rStartup = spi_dest_startup;
    dr->rShutdown = donothingCleanup;
    dr->rDestroy = donothingCleanup;
    dr->mydest = DestSPI;
}
/* Globally available receiver for DestNone */
DestReceiver* None_Receiver = &donothingDR;

/* ----------------
 *		BeginCommand - initialize the destination at start of command
 * ----------------
 */
void BeginCommand(const char* commandTag, CommandDest dest)
{
    /* Nothing to do at present */
}

/*
 * donothingDR is global variables, if function pointer in it is changed,
 * the other sessions will be affected.
 * merge_one_relation() needs to change the function pointer in donothingDR
 * to receive the result tuple from exec_query_for_merge(CTE, ...,"deltamerge");
 * So, it's better to return a copy of donothingDR.
 */
DestReceiver* CreateReceiverForMerge(CommandDest dest)
{
    if (DestNone == dest) {
        DestReceiver* rcv = NULL;
        rcv = (DestReceiver*)palloc0(sizeof(DestReceiver));
        *rcv = donothingDR;
        return rcv;
    }

    return CreateDestReceiver(dest);
}

/* ----------------
 *		CreateDestReceiver - return appropriate receiver function set for dest
 * ----------------
 */
DestReceiver* CreateDestReceiver(CommandDest dest)
{
    switch (dest) {
        case DestRemote:
        case DestRemoteExecute:
            return u_sess->proc_cxt.MyProcPort->protocol_config->fn_printtup_create_DR(dest);

        case DestNone:
            return &donothingDR;

        case DestDebug:
            return &debugtupDR;

        case DestSPI:
            u_sess->utils_cxt.spi_printtupDR->mydest = DestSPI;
            return u_sess->utils_cxt.spi_printtupDR;

        case DestSPITupleAnalyze:
            return createAnalyzeSPIDestReceiver(dest);

        case DestTuplestore:
            return CreateTuplestoreDestReceiver();

        case DestIntoRel:
            return CreateIntoRelDestReceiver(NULL);

        case DestCopyOut:
            return CreateCopyDestReceiver();

        case DestSQLFunction:
            return CreateSQLFunctionDestReceiver();

        case DestTransientRel:
            return CreateTransientRelDestReceiver(InvalidOid);

        case DestTupleBroadCast:
        case DestTupleLocalBroadCast:
        case DestTupleRedistribute:
        case DestTupleLocalRedistribute:
        case DestTupleLocalRoundRobin:
        case DestTupleHybrid:
#ifdef USE_SPQ
        case DestTupleRoundRobin:
        case DestBatchRoundRobin:
        case DestTupleDML:
#endif
        case DestBatchBroadCast:
        case DestBatchLocalBroadCast:
        case DestBatchRedistribute:
        case DestBatchLocalRedistribute:
        case DestBatchLocalRoundRobin:
        case DestBatchHybrid:
            return createStreamDestReceiver(dest);

        case DestTrainModel:
            return CreateTrainModelDestReceiver();

        case DestSqlProcSPI:
            return ((ProcDestReciverHook)u_sess->hook_cxt.pluginProcDestReciverHook)(dest);
        default:
            break;
    }

    /* should never get here */
    return &donothingDR;
}

/* ----------------
 *		EndCommand - clean up the destination at end of command
 * ----------------
 */
void EndCommand(const char* commandTag, CommandDest dest)
{
    switch (dest) {
        case DestRemote:
        case DestRemoteExecute:
        case DestTupleBroadCast:
        case DestTupleRedistribute:
        case DestBatchBroadCast:
        case DestBatchLocalBroadCast:
        case DestBatchRedistribute: {
            Port *MyPort = u_sess->proc_cxt.MyProcPort; 
            if (MyPort && MyPort->protocol_config && MyPort->protocol_config->fn_end_command) {
                MyPort->protocol_config->fn_end_command(commandTag);
            } else {
                /*
                * We assume the commandTag is plain ASCII and therefore requires
                * no encoding conversion.
                */
                pq_putmessage('C', commandTag, strlen(commandTag) + 1);
            }
            break;
        }
        case DestNone:
        case DestDebug:
        case DestSPI:
        case DestSPITupleAnalyze:
        case DestTuplestore:
        case DestIntoRel:
        case DestCopyOut:
        case DestSQLFunction:
        case DestTransientRel:
        case DestTrainModel:
        default:
            break;
    }
}

/* ----------------
 *		EndCommand_noblock, Same as EndCommand but non-blocking
 * ----------------
 */
void EndCommand_noblock(const char* commandTag, CommandDest dest)
{
    switch (dest) {
        case DestRemote:
        case DestRemoteExecute:

            /*
             * We assume the commandTag is plain ASCII and therefore requires
             * no encoding conversion.
             */
            (void)pq_putmessage_noblock('C', commandTag, strlen(commandTag) + 1);
            break;

        case DestNone:
        case DestDebug:
        case DestSPI:
        case DestSPITupleAnalyze:
        case DestTuplestore:
        case DestIntoRel:
        case DestCopyOut:
        case DestSQLFunction:
        case DestTrainModel:
        default:
            break;
    }
}

/* ----------------
 *		NullCommand - tell dest that an empty query string was recognized
 *
 *		In FE/BE protocol version 1.0, this hack is necessary to support
 *		libpq's crufty way of determining whether a multiple-command
 *		query string is done.  In protocol 2.0 it's probably not really
 *		necessary to distinguish empty queries anymore, but we still do it
 *		for backwards compatibility with 1.0.  In protocol 3.0 it has some
 *		use again, since it ensures that there will be a recognizable end
 *		to the response to an Execute message.
 * ----------------
 */
void NullCommand(CommandDest dest)
{
    switch (dest) {
        case DestRemote:
        case DestRemoteExecute:
        case DestTupleBroadCast:
        case DestTupleRedistribute:
        case DestBatchBroadCast:
        case DestBatchLocalBroadCast:
        case DestBatchRedistribute:

            /*
             * tell the fe that we saw an empty query string.  In protocols
             * before 3.0 this has a useless empty-string message body.
             */
            if (PG_PROTOCOL_MAJOR(FrontendProtocol) >= 3)
                pq_putemptymessage('I');
            else
                pq_putmessage('I', "", 1);
            break;

        case DestNone:
        case DestDebug:
        case DestSPI:
        case DestSPITupleAnalyze:
        case DestTuplestore:
        case DestIntoRel:
        case DestCopyOut:
        case DestSQLFunction:
        case DestTransientRel:
        case DestTrainModel:
        default:
            break;
    }
}

/* ----------------
 *		ReadyForQuery - tell dest that we are ready for a new query
 *
 *		The ReadyForQuery message is sent in protocol versions 2.0 and up
 *		so that the FE can tell when we are done processing a query string.
 *		In versions 3.0 and up, it also carries a transaction state indicator.
 *
 *		Note that by flushing the stdio buffer here, we can avoid doing it
 *		most other places and thus reduce the number of separate packets sent.
 * ----------------
 */
void ReadyForQuery(CommandDest dest)
{
    switch (dest) {
        case DestRemote:
        case DestRemoteExecute:
        case DestTupleBroadCast:
        case DestTupleRedistribute:
        case DestBatchBroadCast:
        case DestBatchLocalBroadCast:
        case DestBatchRedistribute:
            if (PG_PROTOCOL_MAJOR(FrontendProtocol) >= 3) {
                StringInfoData buf;

                pq_beginmessage(&buf, 'Z');
                if (u_sess->attr.attr_common.enable_full_encryption) {
                    unsigned short s = TransactionBlockStatusCode();
                    s = s << 8; 
                    s |= ce_cache_refresh_type;
                    pq_sendint16(&buf, s);
                } else {
                    pq_sendbyte(&buf, TransactionBlockStatusCode());
                }
                pq_endmessage(&buf);
            } else if (PG_PROTOCOL_MAJOR(FrontendProtocol) >= 2)
                pq_putemptymessage('Z');

            /* Flush output at end of cycle in any case. */
            pq_flush();

            break;

        case DestNone:
        case DestDebug:
        case DestSPI:
        case DestSPITupleAnalyze:
        case DestTuplestore:
        case DestIntoRel:
        case DestCopyOut:
        case DestSQLFunction:
        case DestTransientRel:
        case DestTrainModel:
        default:
            break;
    }
}

/* ----------------
 *		ReadyForQuery_noblock, Same as ReadyForQuery but non-block
 * ----------------
 */
void ReadyForQuery_noblock(CommandDest dest, int timeout)
{
    switch (dest) {
        case DestRemote:
        case DestRemoteExecute:
            if (PG_PROTOCOL_MAJOR(FrontendProtocol) >= 3) {
                StringInfoData buf;

                pq_beginmessage(&buf, 'Z');
                pq_sendbyte(&buf, TransactionBlockStatusCode());
                pq_endmessage_noblock(&buf);
            } else if (PG_PROTOCOL_MAJOR(FrontendProtocol) >= 2)
                pq_putemptymessage_noblock('Z');
            /* Flush output at end of cycle in any case. */
            pq_flush_timedwait(timeout);
            break;

        case DestNone:
        case DestDebug:
        case DestSPI:
        case DestSPITupleAnalyze:
        case DestTuplestore:
        case DestIntoRel:
        case DestCopyOut:
        case DestSQLFunction:
        case DestTrainModel:
        default:
            break;
    }
}
