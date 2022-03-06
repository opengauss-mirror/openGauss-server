/* -------------------------------------------------------------------------
 *
 * pgxcnode.c
 *
 *    Functions for the Coordinator communicating with the PGXC nodes:
 *    Datanodes and Coordinators
 *
 *
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 * IDENTIFICATION
 *    $$
 *
 *
 * -------------------------------------------------------------------------
 */

#include "postgres.h"
#include "knl/knl_variable.h"
#include <sys/time.h>
#include <sys/ioctl.h>
#include "access/gtm.h"
#include "access/transam.h"
#include "access/xact.h"
#include "catalog/pg_type.h"
#include "commands/prepare.h"
#include "gtm/gtm_c.h"
#include "nodes/nodes.h"
#include "pgxc/pgxcnode.h"
#include "pgxc/execRemote.h"
#include "catalog/namespace.h"
#include "catalog/pg_namespace.h"
#include "catalog/pgxc_node.h"
#include "catalog/pg_collation.h"
#include "pgxc/locator.h"
#include "pgxc/nodemgr.h"
#include "pgxc/pgxc.h"
#include "pgxc/poolmgr.h"
#include "tcop/dest.h"
#include "utils/builtins.h"
#include "utils/distribute_test.h"
#include "utils/elog.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/fmgroids.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"
#include "utils/lsyscache.h"
#include "utils/formatting.h"
#include "workload/cpwlm.h"
#include "libpq/libpq-fe.h"
#include "libpq/libpq-int.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "postmaster/postmaster.h"
#include "postmaster/twophasecleaner.h"
#include "libcomm/libcomm.h"
#include "storage/procarray.h"
#include "libpq/pqformat.h"
#include "instruments/instr_unique_sql.h"
#include "utils/elog.h"

#ifdef HAVE_POLL_H
#include <poll.h>
#endif
#ifdef HAVE_SYS_POLL_H
#include <sys/poll.h>
#endif

#ifdef ENABLE_UT
#define static
#endif

#define CMD_ID_MSG_LEN 8
#define PGXC_CANCEL_DELAY 15

#define ERROR_OCCURED       true
#define NO_ERROR_OCCURED    false

#pragma  GCC diagnostic ignored  "-Wunused-function"

extern GlobalNodeDefinition *global_node_definition;

extern bool PQsendQueryStart(PGconn *conn);
extern bool IsAutoVacuumWorkerProcess();
extern char *format_type_with_nspname(Oid type_oid);

static void pgxc_node_all_free(void);
static void LibcommFinish(NODE_CONNECTION *conn);
static int LibcommWaitpoll(NODE_CONNECTION *conn);
PGresult *LibcommGetResult(NODE_CONNECTION *conn);

static int  get_int(PGXCNodeHandle* conn, size_t len, int *out);
static int  get_char(PGXCNodeHandle* conn, char *out);
extern PoolHandle * GetPoolAgent();
extern void destroy_slots(List *slots);

extern unsigned long LIBCOMM_BUFFER_SIZE;

#define CONCAT(a, b) (#a " " #b)
#define CONNECTION_ERROR_NORMALIZE(level, str, conn) \
do\
{\
    NodeDefinition* result = PgxcNodeGetDefinition((conn)->nodeoid);\
    if(result != NULL)  \
    {\
        ereport((level), \
                (errcode(ERRCODE_CONNECTION_FAILURE), \
                 errmsg(CONCAT(str, (Local:%s Remote: %s:%d Oid:%u)), g_instance.attr.attr_common.PGXCNodeName, \
                        result->nodename.data, result->nodeport,conn->nodeoid))); \
    }\
    else  if ((conn)->remoteNodeName != NULL) \
    {\
        ereport((level), \
                (errcode(ERRCODE_CONNECTION_FAILURE), \
                 errmsg(CONCAT(str, (Local:%s Remote: %s)), g_instance.attr.attr_common.PGXCNodeName, \
                        conn->remoteNodeName))); \
    }\
}\
while(0);\

/*
 * @Description: Valid inBuffer and outBuffer of PGXCNodeHandle struct
 *
 * @param[IN] pgxc_handle:  connection handle with one datanode
 * @return: void
 */
static void
valid_pgxc_handle(PGXCNodeHandle *pgxc_handle)
{
    if (NULL == pgxc_handle->outBuffer || NULL == pgxc_handle->inBuffer)
        ereport(ERROR,
                (errcode(ERRCODE_OUT_OF_MEMORY),
                 errmsg("invalid input/output buffer in node handle")));
}

/*
 * Initialize PGXCNodeHandle struct
 */
void
init_pgxc_handle(PGXCNodeHandle *pgxc_handle)
{
#ifndef ENABLE_MULTIPLE_NODES
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return;
#else

    /*
     * Socket descriptor is small non-negative integer,
     * Indicate the handle is not initialized yet
     */
    pgxc_handle->sock = NO_SOCKET;

    /* Initialise buffers */
    pgxc_handle->error = NULL;
    pgxc_handle->outSize = 16 * 1024;
    pgxc_handle->outBuffer = (char *) palloc(pgxc_handle->outSize);
    pgxc_handle->inSize = 16 * 1024;
    pgxc_handle->inBuffer = (char *) palloc(pgxc_handle->inSize);
    pgxc_handle->combiner = NULL;
    pgxc_handle->inStart = 0;
    pgxc_handle->inEnd = 0;
    pgxc_handle->inCursor = 0;
    pgxc_handle->outEnd = 0;

    if (pgxc_handle->outBuffer == NULL || pgxc_handle->inBuffer == NULL)
    {
        ereport(ERROR,
                (errcode(ERRCODE_OUT_OF_MEMORY),
                 errmsg("out of memory")));
    }

#endif
}

char *
getNodenameByIndex(int index)
{
	Assert(false);
	DISTRIBUTED_FEATURE_NOT_SUPPORTED();
	return NULL;
}

/*
 * Allocate and initialize memory to store Datanode and Coordinator handles.
 */
void
InitMultinodeExecutor(bool is_force)
{
#ifndef ENABLE_MULTIPLE_NODES
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return;
#else

    int             count;
	Oid				*coOids = NULL;
	Oid				*dnOids = NULL;

	/* Free all the existing information first */
	if (is_force)
		pgxc_node_all_free();

	/* This function could get called multiple times because of sigjmp */
	if (dn_handles != NULL &&
		co_handles != NULL)
		return;

	/* Update node table in the shared memory */
	PgxcNodeListAndCount();

	/* Get classified list of node Oids */
	PgxcNodeGetOids(&coOids, &dnOids, &NumCoords, &NumDataNodes, true);

	/* Do proper initialization of handles */
	if (NumDataNodes > 0)
		dn_handles = (PGXCNodeHandle *)
			palloc(NumDataNodes * sizeof(PGXCNodeHandle));
	if (NumCoords > 0)
		co_handles = (PGXCNodeHandle *)
			palloc(NumCoords * sizeof(PGXCNodeHandle));

	if ((!dn_handles && NumDataNodes > 0) ||
		(!co_handles && NumCoords > 0))
		ereport(ERROR,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("out of memory for node handles")));

	/* Initialize new empty slots */
	for (count = 0; count < NumDataNodes; count++)
	{
		init_pgxc_handle(&dn_handles[count]);
		dn_handles[count].nodeoid = dnOids[count];
	}
	for (count = 0; count < NumCoords; count++)
	{
		init_pgxc_handle(&co_handles[count]);
		co_handles[count].nodeoid = coOids[count];
	}

	datanode_count = 0;
	coord_count = 0;
	PGXCNodeId = 0;

	/* Finally determine which is the node-self */
	for (count = 0; count < NumCoords; count++)
	{
		if (pg_strcasecmp(PGXCNodeName,
				   get_pgxc_nodename(co_handles[count].nodeoid)) == 0)
			PGXCNodeId = count + 1;
	}

	/* No node-self */
	if (PGXCNodeId == 0)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_EXCEPTION),
				 errmsg("Coordinator cannot identify itself")));

#endif
}


/*
 * Builds up a connection string
 */
char *
PGXCNodeConnStr(const char *host, int port, const char *dbname,
    const char *user, const char *pgoptions, const char *remote_type, int proto_type, const char *remote_nodename)
{
	char *out = NULL;
	char connstr[512];
	int  num;
	char *appName = NULL;

	/*
	 * under thread pool mode, we use ha port in the following scenarios:
	 * 1. for inner maintenance tools
	 * 2. for connections to cn ha port from init user
	 */
	if (g_instance.attr.attr_common.enable_thread_pool &&
		(u_sess->proc_cxt.IsInnerMaintenanceTools || IsHAPort(u_sess->proc_cxt.MyProcPort))) {
		port++;
	}

	/*
	 * Build up connection string
	 * remote type can be Coordinator, Datanode or application.
         * In order to identify WDRXdb work on the remote node,
         * we need to set "application_name = WDRXdb".
	 */
	if (pgoptions != NULL && strstr(pgoptions, "application_name=WDRXdb") != NULL) {
		appName = "WDRXdb";
	} else {
		appName = g_instance.attr.attr_common.PGXCNodeName;
	}
        num = snprintf_s(connstr, sizeof(connstr), sizeof(connstr)-1,
            "host=%s port=%d dbname=%s user=%s application_name=%s connect_timeout=%d rw_timeout=%d "
            "options='-c remotetype=%s %s' prototype=%d "
            "keepalives_idle=%d keepalives_interval=%d keepalives_count=%d",
            host, port, dbname, user, appName, u_sess->attr.attr_network.PoolerConnectTimeout,
            u_sess->attr.attr_network.PoolerTimeout, remote_type, pgoptions, proto_type,
            u_sess->attr.attr_common.tcp_keepalives_idle, u_sess->attr.attr_common.tcp_keepalives_interval,
            u_sess->attr.attr_common.tcp_keepalives_count);
	/* Check for overflow */
	if (num != -1)
	{
		/* Output result */
		out = (char *) palloc(num + 1);
		errno_t ss_rc = strncpy_s(out, num+1, connstr, num);
		securec_check(ss_rc, "\0", "\0");
		out[num] = '\0';
		return out;
	}

	/* return NULL if we have problem */
	return NULL;
}


/*
 * Connect to a Datanode using a connection string
 */
NODE_CONNECTION *
PGXCNodeConnect(const char *connstr)
{
	PGconn	   *conn = NULL;

	/* Delegate call to the pglib */
	conn = PQconnectdb(connstr);
	return (NODE_CONNECTION *) conn;
}


/*
 * Close specified connection
 */
void
PGXCNodeClose(NODE_CONNECTION *conn)
{
	if (conn == NULL)
		return;

	if(conn->is_logic_conn)
		LibcommFinish(conn);
	else
		PQfinish((PGconn *) conn);
}

/*
 * LibcommPacketSend() -- convenience routine to send a message to server.
 *
 * pack_type: the single-byte message type code.  (Pass zero for startup
 * packets, which have no message type code.)
 *
 * buf, buf_len: contents of message.  The given length includes only what
 * is in buf; the message type and message length fields are added here.
 *
 * RETURNS: STATUS_ERROR if the write fails, STATUS_OK otherwise.
 * SIDE_EFFECTS: may block.
 *
 * Note: all messages sent with this routine have a length word, whether
 * it's protocol 2.0 or 3.0.
 */
int
LibcommPacketSend(NODE_CONNECTION *conn, char pack_type,
		const void *buf, size_t buf_len)
{
	/* Start the message. */
	if (pqPutMsgStart(pack_type, true, conn))
		return STATUS_ERROR;

	/* Send the message body. */
	if (pqPutnchar((const char *)buf, buf_len, conn))
		return STATUS_ERROR;

	/* Finish the message. */
	if (LibcommPutMsgEnd(conn))
		return STATUS_ERROR;

	/* Flush to ensure backend gets it. */
	if (LibcommFlush(conn))
		return STATUS_ERROR;

	return STATUS_OK;
}

/*
 * LibcommCloseConn: send "close connection" message
 * to remote pg thread via Libcomm interface.
 */
void
LibcommCloseConn(NODE_CONNECTION *conn)
{
	/* close logic connnection smoothly when the status is ok*/
	if (conn->status == CONNECTION_OK)
	{
		/*
		 * Try to send "close connection" message to backend. Ignore any
		 * error.
		 */
		(void)pqPutMsgStart('X', false, conn);
		(void)LibcommPutMsgEnd(conn);
		(void)LibcommFlush(conn);
	}
	/* close gs socket directly in case of DN's openGauss thread leaking */
	else
		gs_close_gsocket(&conn->libcomm_addrinfo.gs_sock);
}

bool
LibcommStopQuery(NODE_CONNECTION * conn)
{
	return gs_stop_query(&conn->libcomm_addrinfo.gs_sock, htonl(conn->be_pid));
}

/*
 * LibcommCancelOrStop: request query cancel or stop, build a new logic conn with backend
 * and send  request query cancel or stop by startup packet, then wait backend handle this message
 *
 * Returns TRUE if able to send the cancel or stop request and backend has handled this message, 
 * FALSE if not.
 *
 * On failure, an error message is stored in *errbuf, which must be of size
 * errbufsize (recommended size is 256 bytes).	*errbuf is not changed on
 * success return.
 */
bool
LibcommCancelOrStop(NODE_CONNECTION * conn, char * errbuf,int errbufsize,int timeout, uint32 request_code)
{

	if (conn->be_pid == 0 || (conn->be_key==0 && request_code == CANCEL_REQUEST_CODE))
	{
		securec_check(strncpy_s(errbuf,errbufsize,"LibcommCancelOrStop() -- no cancel object supplied", errbufsize-1),"\0", "\0");
		return FALSE;
	}

	int			save_errno = SOCK_ERRNO;
	char		sebuf[256];
	int			maxlen;
	char*		crp = NULL;
	int			pkg_size;

	struct
	{
		uint32		packetlen;
		CancelRequestPacket cp;
    } cancel_crp = {0};

    struct
    {
        uint32      packetlen;
        StopRequestPacket cp;
    } stop_crp = {0};

	/*We need to open a temporary connection to the postmaster.*/

	/* STEP1: build sctpaddr for gs_connect */
	libcommaddrinfo tmp_sctp_addr = {0};
	int rc = strncpy_s(tmp_sctp_addr.nodename, NAMEDATALEN, conn->libcomm_addrinfo.nodename, strlen(conn->libcomm_addrinfo.nodename));
	securec_check(rc, "\0", "\0");
	tmp_sctp_addr.host = conn->pghost;
	tmp_sctp_addr.ctrl_port = conn->libcomm_addrinfo.ctrl_port;
	tmp_sctp_addr.listen_port = conn->libcomm_addrinfo.listen_port;
	libcommaddrinfo *libcomm_addr = &tmp_sctp_addr;

	/* STEP2: make temp logic conn by gs_connect */
	int error = gs_connect(&libcomm_addr, 1, timeout);
	if(libcomm_addr->gs_sock.type == GSOCK_INVALID)
	{
		securec_check(strncpy_s(errbuf,errbufsize, "LibcommCancelOrStop() -- gs_connect() failed: ", errbufsize-1),"\0", "\0");
		goto cancel_errReturn;
	}

	/* STEP3: build cancel request packet */
	if(request_code == CANCEL_REQUEST_CODE)
	{
		pkg_size = sizeof(cancel_crp);
		cancel_crp.cp.cancelRequestCode = (MsgType) htonl(CANCEL_REQUEST_CODE);
		cancel_crp.cp.backendPID = htonl(conn->be_pid);
		cancel_crp.cp.cancelAuthCode = htonl(conn->be_key);
		cancel_crp.packetlen = htonl((uint32) pkg_size);
		crp = (char*)&cancel_crp;
	}
	else
	{
		pkg_size = sizeof(stop_crp);
		stop_crp.cp.stopRequestCode = (MsgType) htonl(STOP_REQUEST_CODE);
		stop_crp.cp.backendPID = htonl(conn->be_pid);
        stop_crp.cp.query_id_first = htonl((uint32)(u_sess->debug_query_id >> 32));
        stop_crp.cp.query_id_end = htonl((uint32)(u_sess->debug_query_id & 0xFFFFFFFF));
		stop_crp.packetlen = htonl((uint32) pkg_size);
		crp = (char*)&stop_crp;
	}

	/* STEP4: send cancel request packet */
	error = gs_send(&(tmp_sctp_addr.gs_sock), crp, pkg_size, timeout, TRUE);
	if(error <= 0)
	{
		/* 
		 * remote thread exit after receive cancel request is expected.
		 * so ECOMMTCPREMOETECLOSE in here is expected.
		 */
		if(errno == ECOMMTCPREMOETECLOSE)
			return TRUE;
		securec_check(strncpy_s(errbuf,errbufsize,"LibcommCancelOrStop() -- gs_send() failed.", errbufsize-1),"\0", "\0");
		goto cancel_errReturn;
	}

	/* STEP5: wait gs_sock closed by remote. */
	/* Note: once DN has received cancel request and handle it.
	 * DN will exit thread and close this temp logic conn.
	 * thus gs_wait_poll < 0 is expected.
	 */
	while(1)
	{
		int producer;
		error = gs_wait_poll(&(tmp_sctp_addr.gs_sock), 1, &producer, timeout, true);

		if(errno == ECOMMTCPEPOLLTIMEOUT)
		{
			/* connection timeout here */
			securec_check(strncpy_s(errbuf,errbufsize, "LibcommCancelOrStop() -- gs_wait_poll() timeout, failed: ", errbufsize-1),"\0", "\0");
			break;
		}
		else if(error < 0)
			return TRUE;
	}

cancel_errReturn:

	/*
	 * Make sure we don't overflow the error buffer. Leave space for the \n at
	 * the end, and for the terminating zero.
	 */
	maxlen = errbufsize - strlen(errbuf) - 2;
	if (maxlen >= 0)
	{
		rc = strcat_s(errbuf, maxlen, SOCK_STRERROR(SOCK_ERRNO, sebuf, sizeof(sebuf)));
		securec_check(rc, "\0", "\0");
		rc = strcat_s(errbuf, errbufsize, "\n");
		securec_check(rc, "\0", "\0");
	}

	gs_close_gsocket(&(tmp_sctp_addr.gs_sock));

	SOCK_ERRNO_SET(save_errno);
	return FALSE;
}

/*
 * LibcommFinish: properly close a connection to the backend.
 * replace PQfinish
 * LibcommCloseConn: only send  "close connection" message.
 * closePGconn: clear PGconn Struct
 * freePGconn frees the PGconn data structure
 * so it shouldn't be re-used after this.
 */
static void
LibcommFinish(NODE_CONNECTION *conn)
{
	if (conn != NULL)
	{
		LibcommCloseConn(conn);
		/*
		 * Note: closePGconn will send "close connection" message
		 * if the connection is physical via libpq interface.
		 * for logic conn, we have done this step in LibcommCloseConn.
		 */
		closePGconn(conn);
		freePGconn(conn);
	}
}

/*
 * LibcommSendSome: send data waiting in the output buffer.
 * replace pqSendSome
 * the different with pqSendSome:
 * we do not need to read some data in case channel is full,
 * cause we use diff mailbox for send and recv.
 *
 * len is how much to try to send (typically equal to outCount, but may
 * be less).
 *
 * Return 0 on success, -1 on failure and 1 when not all data could be sent
 * because the socket would block and the connection is non-blocking.
 *
 */
int
LibcommSendSome(NODE_CONNECTION *conn, int len)
{
	char	   *ptr = conn->outBuffer;
	int			remaining = conn->outCount;
	int			result = 0;
	int 		sent = 0;

	if (conn->libcomm_addrinfo.gs_sock.type == GSOCK_INVALID)
	{
		printfPQExpBuffer(&conn->errorMessage,
						  libpq_gettext("connection not open\n"));
		/* Discard queued data; no chance it'll ever be sent */
		conn->outCount = 0;
		goto err_handle;
	}

	/* while there's still data to send */
	while (len > 0)
	{
		/* gs_send will wait quota if consumer is busy.*/
		sent = gs_send(&(conn->libcomm_addrinfo.gs_sock), ptr, len, -1, TRUE);
		if(sent < 0)
		{
			goto err_handle;
		}
		else
		{
			ptr += sent;
			len -= sent;
			remaining -= sent;
		}

		/*
		 * if wait quota timeout or some msg has been sent, return 1 for non block conn.
		 * for block mode, keep send the remaining.
		 */
		if((conn)->nonblocking)
		{
			result = 1;
			break;
		}
	}
	/* shift the remaining contents of the buffer if we have sent some bytes*/
	if (remaining > 0 && sent != 0)
	{
		int rc = memmove_s(conn->outBuffer, remaining, ptr, remaining);	
		securec_check(rc,"\0","\0");
	}
	conn->outCount = remaining;

	return result;

err_handle:
	return -1;
}

/*
 * LibcommPutMsgEnd: finish constructing a message and possibly send it
 * replace pqPutMsgEnd
 * totally some logic with pqPutMsgEnd.
 *
 * Returns 0 on success, EOF on error
 *
 * We don't actually send anything here unless we've accumulated at least
 * 8K worth of data (the typical size of a pipe buffer on Unix systems).
 * This avoids sending small partial packets.  The caller must use pqFlush
 * when it's important to flush all the data out to the server.
 */
int
LibcommPutMsgEnd(NODE_CONNECTION *conn)
{
	if (conn->Pfdebug != NULL)
		fprintf(conn->Pfdebug, "To backend > Msg complete, length %d\n",
				conn->outMsgEnd - conn->outCount);

	/* Fill in length word if needed */
	if (conn->outMsgStart >= 0)
	{
		uint32 msgLen = conn->outMsgEnd - conn->outMsgStart;

		msgLen = htonl(msgLen);
		errno_t rc = memcpy_s(conn->outBuffer + conn->outMsgStart, conn->outBufSize - conn->outMsgStart, &msgLen, 4);
		securec_check(rc, "", "");
	}

	/* Make message eligible to send */
	conn->outCount = conn->outMsgEnd;

	if ((unsigned long)conn->outCount >= LIBCOMM_BUFFER_SIZE)
	{
		int toSend = conn->outCount - (conn->outCount % LIBCOMM_BUFFER_SIZE);

		/* call LibcommSendSome instead of pqSendSome*/
		if (LibcommSendSome(conn, toSend) < 0)
			return EOF;
		/* in nonblock mode, don't complain if unable to send it all */
	}

	return 0;
}

/*
 * LibcommFlush: send any data waiting in the output buffer
 * replace pqFlush
 * totally some logic with pqFlush.
 *
 * Return 0 on success, -1 on failure and 1 when not all data could be sent
 * because the socket would block and the connection is non-blocking.
 *
 */
int
LibcommFlush(NODE_CONNECTION *conn)
{
	if (conn->outCount > 0)
		return LibcommSendSome(conn, conn->outCount);

	return 0;
}

/*
 * LibcommHandleSendFailure: try to clean up after failure to send command.
 * replace pqHandleSendFailure
 * totally some logic with pqHandleSendFailure.
 *
 * Primarily, what we want to accomplish here is to process an async
 * NOTICE message that the backend might have sent just before it died.
 *
 * NOTE: this routine should only be called in PGASYNC_IDLE state.
 */
void LibcommHandleSendFailure(NODE_CONNECTION *conn)
{
	/*
	 * Accept any available input data, ignoring errors.
	 */
	while (LibcommReadData(conn) > 0) {};
	/* loop until no more data readable */

	/*
	 * Parse any available input messages.	Since we are in PGASYNC_IDLE
	 * state, only NOTICE and NOTIFY messages will be eaten.
	 */
	parseInput(conn);
}

/*
 * LibcommSendQueryCommon
 * replace PQsendQuery without status change, conn->asyncStatus is set outter
 * totally some logic with PQsendQuery.
 *
 *	 Submit a query, but don't wait for it to finish
 *
 * Returns: 1 if successfully submitted
 *			0 if error (conn->errorMessage is set)
 */
int LibcommSendQueryCommon(NODE_CONNECTION *conn, const char *query, char pack_type)
{
	if (!PQsendQueryStart(conn)) {
		return 0;
	}

	if (query == NULL) {
		printfPQExpBuffer(&conn->errorMessage,
						libpq_gettext("command string is a null pointer\n"));
		return 0;
	}

	/* construct the outgoing Query message */
	if (pqPutMsgStart(pack_type, false, conn) < 0 ||
		pqPuts(query, conn) < 0 ||
		LibcommPutMsgEnd(conn) < 0) {
		LibcommHandleSendFailure(conn);
		return 0;
	}

	/* remember we are using simple query protocol */
	conn->queryclass = PGQUERY_SIMPLE;

	/* and remember the query text too, if possible */
	/* if insufficient memory, last_query just winds up NULL */
	if (conn->last_query != NULL) {
		free(conn->last_query);
	}
	conn->last_query = strdup(query);

	/*
	 * Give the data a push.  In nonblock mode, don't complain if we're unable
	 * to send it all; LibcommGetResult()/PQgetResult() will do any additional flushing needed.
	 */
	if (LibcommFlush(conn) < 0) {
		LibcommHandleSendFailure(conn);
		return 0;
	}

	return 1;
}

/*
 * LibcommSendQuery
 * replace PQsendQuery
 * totally some logic with PQsendQuery.
 *
 *	 Submit a query, but don't wait for it to finish
 *
 * Returns: 1 if successfully submitted
 *			0 if error (conn->errorMessage is set)
 */
int
LibcommSendQuery(NODE_CONNECTION *conn, const char *query)
{
	int ret;
	conn->asyncStatus = PGASYNC_IDLE;
	ret = LibcommSendQueryCommon(conn, query, 'Q');
	if (ret < 1) {
		return ret;
	}
	/* OK, it's launched! */
	conn->asyncStatus = PGASYNC_BUSY;
	return 1;
}

/*
 * read_data_prepare_buffer, calc buffer valid area and set cursor
 * start, end, cursor are poniter, their changes need to take effect in conn
 * buffer is address, we can use it's value direct
 */
void read_data_prepare_buffer(size_t *start, size_t *end, size_t *cursor, char *buffer)
{
	/* Left-justify any data in the buffer to make room */
	if (*start < *end) {
		if (*start > 0)	{
			errno_t ss_rc  = memmove_s(buffer, *end - *start, buffer + *start,
			                           *end - *start);
			securec_check(ss_rc, "\0", "\0");
			*end -= *start;
			*cursor -= *start;
			*start = 0;
		}
	} else {
		/* buffer is logically empty, reset it */
		*start = 0;
		*cursor = 0;
		*end = 0;
	}

	return;
}

/*
 * LibcommReadData: read more data, if any is available
 * replace pqReadData
 * Note in pqReadData, it try to pqsecure_read for serval times
 * as some kernels will only give us back 1 packet per recv() call
 * even if we asked for more and there is more available.
 * For logic conn it is redundant, cause gs_recv will copy all packets to output buf.
 *
 * Possible return values:
 *	 1: successfully loaded at least one more byte
 *	 0: no data is presently available, but no error detected
 *	-1: error detected (including EOF = connection closure);
 *		conn->errorMessage set
 * NOTE: callers must not assume that pointers or indexes into conn->inBuffer
 * remain valid across this call!
 */
int
LibcommReadData(NODE_CONNECTION *conn)
{
	int			nread;

	/* Left-justify any data in the buffer to make room */
	if (conn->inStart < conn->inEnd)
	{
		if (conn->inStart > 0)
		{
			errno_t ss_rc  = memmove_s(conn->inBuffer, conn->inEnd - conn->inStart, conn->inBuffer + conn->inStart,
					conn->inEnd - conn->inStart);
			securec_check(ss_rc, "\0", "\0");
			conn->inEnd -= conn->inStart;
			conn->inCursor -= conn->inStart;
			conn->inStart = 0;
		}
	}
	else
	{
		/* buffer is logically empty, reset it */
		conn->inStart = conn->inCursor = conn->inEnd = 0;
	}

	/*
	 * If the buffer is fairly full, enlarge it. We need to be able to enlarge
	 * the buffer in case a single message exceeds the initial buffer size. We
	 * enlarge before filling the buffer entirely so as to avoid asking the
	 * kernel for a partial packet. The magic constant here should be large
	 * enough for a TCP packet or Unix pipe bufferload.  8K is the usual pipe
	 * buffer size, so...
	 */
	if ((unsigned long)(conn->inBufSize - conn->inEnd) < LIBCOMM_BUFFER_SIZE)
	{
		if (pqCheckInBufferSpace(conn->inEnd + (size_t) LIBCOMM_BUFFER_SIZE, conn))
		{
			/*
			 * We don't insist that the enlarge worked, but we need some room
			 */
			if (conn->inBufSize - conn->inEnd < 100)
				return -1;		/* errorMessage already set */
		}
	}
	char *ptr = conn->inBuffer + conn->inEnd;
	int len = conn->inBufSize - conn->inEnd;

	/*
	 *gs_recv will copy all packets to output buf
	 *so only recv one time.
	 */
	nread = gs_recv(&(conn->libcomm_addrinfo.gs_sock), ptr, len);
	if (nread <= 0)
	{
		/* gs_recv return -1 with errno ECOMMTCPNODATA if data is not arrived */
		if(errno == ECOMMTCPNODATA)
		{
			return 0;
		}
		else
		{
			goto definitelyFailed;
		}
	}
	else
	{
		conn->inEnd += nread;
		return 1;
	}
	
	/*
	 * error handle is the same with pqReadData, just close logic conn
	 */
definitelyFailed:
	conn->status = CONNECTION_BAD;		/* No more connection to backend */
	gs_close_gsocket(&(conn->libcomm_addrinfo.gs_sock));
	conn->sock = -1;

	return -1;
}

/*
 * LibcommSendQueryPoolerStatelessReuse use only in pooler stateless reuse mode.
 *	 Submit a query, but don't wait for it to finish
 * replace PQsendQueryPoolerStatelessReuse
 * totally some logic with PQsendQueryPoolerStatelessReuse.
 * Returns: 1 if successfully submitted
 *			0 if error (conn->errorMessage is set)
 */
int
LibcommSendQueryPoolerStatelessReuse(NODE_CONNECTION *conn, const char *query)
{
	int ret = LibcommSendQueryCommon(conn, query, 'O');
	if (ret < 1) {
		return ret;
	}

	/* OK, it's launched! */
	conn->asyncStatus = PGASYNC_BUSY;
	return 1;
}

/*
 * LibcommGetResult
 *	  Get the next PGresult produced by a query.  Returns NULL if no
 *	  query work remains or an error has occurred (e.g. out of
 *	  memory).
 * replace PQgetResult
 */

PGresult *
LibcommGetResult(NODE_CONNECTION *conn)
{
	PGresult   *res = NULL;
	if (conn == NULL)
		return NULL;
	/* Parse any available data, if our state permits. */
	parseInput(conn);

	/* If not ready to return something, block until we are. */
	while (conn->asyncStatus == PGASYNC_BUSY)
	{
		int			flushResult;
		/*
		 * If data remains unsent, send it.  Else we might be waiting for the
		 * result of a command the backend hasn't even got yet. 
		 * Note LibcommFlush will wait for quota if consumer is busy.
		 * so while 1 is fine.
		 */
		while (1)
		{
			if((flushResult = LibcommFlush(conn)) <= 0)
				break;
		}

		/* Wait for some more data, and load it. same logic with  PQgetResult*/
		if (flushResult || LibcommWaitpoll(conn) || LibcommReadData(conn) < 0)
		{
			/*
			 * conn->errorMessage has been set by pqWait or pqReadData. We
			 * want to append it to any already-received error message.
			 */
			conn->status = CONNECTION_BAD;
			pqSaveErrorResult(conn);
			conn->asyncStatus = PGASYNC_IDLE;
			return pqPrepareAsyncResult(conn);
		}

		/* Parse it. */
		parseInput(conn);
	}

	/* Return the appropriate thing. */
	switch (conn->asyncStatus)
	{
		case PGASYNC_IDLE:
			res = NULL;			/* query is complete */
			break;
		case PGASYNC_READY:
			res = pqPrepareAsyncResult(conn);
			/* Set the state back to BUSY, allowing parsing to proceed. */
			conn->asyncStatus = PGASYNC_BUSY;
			break;
		case PGASYNC_COPY_IN:
			res = getCopyResult(conn, PGRES_COPY_IN);
			break;
		case PGASYNC_COPY_OUT:
			res = getCopyResult(conn, PGRES_COPY_OUT);
			break;
		case PGASYNC_COPY_BOTH:
			res = getCopyResult(conn, PGRES_COPY_BOTH);
			break;
		default:
			printfPQExpBuffer(&conn->errorMessage,
							  libpq_gettext("unexpected asyncStatus: %d\n"),
							  (int) conn->asyncStatus);
			res = PQmakeEmptyPGresult(conn, PGRES_FATAL_ERROR);
			break;
	}

	if (res != NULL)
	{
		int			i;

		for (i = 0; i < res->nEvents; i++)
		{
			PGEventResultCreate evt;

			evt.conn = conn;
			evt.result = res;
			if (!res->events[i].proc(PGEVT_RESULTCREATE, &evt,
									 res->events[i].passThrough))
			{
				printfPQExpBuffer(&conn->errorMessage,
								  libpq_gettext("PGEventProc \"%s\" failed during PGEVT_RESULTCREATE event\n"),
								  res->events[i].name);
				pqSetResultError(res, conn->errorMessage.data);
				res->resultStatus = PGRES_FATAL_ERROR;
				break;
			}
			res->events[i].resultInitialized = TRUE;
		}
	}
	return res;
}

/*
  * LibcommWaitpoll: wait for data arriving.
  * replace pqWaitTimed for read.
  * return 0 if data is arrived and -1 when error happened.
 */
static int
LibcommWaitpoll(NODE_CONNECTION *conn)
{
	int producer;
	int time_out = -1;
	if (conn->rw_timeout != NULL)
	{
		int timeout = atoi(conn->rw_timeout);

		if (timeout > 0)
		{
			time_out = timeout;
		}
	}

	retry:
	NetWorkTimePollStart(t_thrd.pgxc_cxt.GlobalNetInstr);
	int ret = gs_wait_poll(&(conn->libcomm_addrinfo.gs_sock), 1, &producer, time_out, false);
	NetWorkTimePollEnd(t_thrd.pgxc_cxt.GlobalNetInstr);
	/* no data but wake up, check interrupts then retry */
	if (ret == 0)
		goto retry;
	if (ret > 0)
		return 0;
	else
		return -1;
}

/*
 * PGXCNodeSetQueryEreport: if get result fail, we report the error and clear result, or continue.
 * replace pqWaitTimed for read.
 * return 0 if no error happened, -1 when failed.
 */
int PGXCNodeSetQueryEreport(NODE_CONNECTION *conn, PGresult *result)
{
	Assert(result != NULL);

	if (!u_sess->attr.attr_network.comm_stat_mode && (result->resultStatus == PGRES_FATAL_ERROR ||
		result->resultStatus == PGRES_BAD_RESPONSE)) {
		ereport(WARNING,
		        (errcode(ERRCODE_CONNECTION_EXCEPTION),
		         errmsg("Get result failed when send set query error: %s.", PQerrorMessage(conn))));
		PQclear(result);
		return -1;
	}
	if (result != NULL) {
		PQclear(result);
		result = NULL;
	}

	return 0;
}

int
PGXCNodeSetQueryGetResult(NODE_CONNECTION *conn)
{
	PGresult	*result = NULL;
	int ret = 0;
	/* 
	* Consume results from SET commands.
	* If the connection was lost, stop getting result and break the loop. 
	* Use comm_stat_mode just for 2pc checking, we ingore
	* any failed on DN when we send 'execute direct on'
	* query to DN.
	* We set comm_stat_mode = on when we do 2pc check.
	* Normally, comm_stat_mode should be false.
	*/
	if(conn->is_logic_conn)
	{
		while ((result = LibcommGetResult((NODE_CONNECTION *) conn)) != NULL && conn->status != CONNECTION_BAD)
		{
			ret = PGXCNodeSetQueryEreport(conn, result);
			if (ret < 0) {
				return ret;
			}
		}
	}
	else
	{
		while ((result = PQgetResult((PGconn *) conn)) != NULL && PQsocket(conn) != -1)
		{
			ret = PGXCNodeSetQueryEreport(conn, result);
			if (ret < 0) {
				return ret;
			}
		}
	}
	if(result != NULL)
		PQclear(result);

	if ((PQsocket(conn) == -1 && !conn->is_logic_conn) || PQstatus(conn) == CONNECTION_BAD)
	{
		ereport(WARNING,
			(errcode(ERRCODE_CONNECTION_EXCEPTION),
				errmsg("Connection error: %s.", PQerrorMessage(conn))));
		return -1;
	}

	return 0;

}


/*
 * Send SET query to given connection.
 * Query is sent asynchronously and results are consumed
 */
int
PGXCNodeSendSetQuery(NODE_CONNECTION *conn, const char *sql_command)
{
	if(conn->is_logic_conn)
	{
		if (!LibcommSendQuery((NODE_CONNECTION *) conn, sql_command))
		{
			ereport(LOG, (errmsg("Send query failed %s", PQerrorMessage(conn))));
			return -1;
		}
	}
	else
	{
		if (!PQsendQuery((PGconn *) conn, sql_command))
		{
			ereport(LOG, (errmsg("Send query failed %s", PQerrorMessage(conn))));
			return -1;
		}
	}

	return PGXCNodeSetQueryGetResult(conn);
}

/*
 * set nodepool size if Send SET query fail.
 * cn and dn all do this
 */
void PGXCNodeSendSetFail(List* invalid_slots, PoolAgent *agent, PGXCNodePoolSlot **connection, Oid* conn_oid)
{
	PGXCNodePool  *nodePool = NULL;
	bool found = false;

	LWLockAcquire(PoolerLock, LW_SHARED);

	invalid_slots = lappend(invalid_slots, *connection);
	*connection = NULL;

	nodePool = (PGXCNodePool *) hash_search(agent->pool->nodePools,
	    conn_oid, HASH_FIND, &found);
	if (!found) {
		LWLockRelease(PoolerLock);
		return;
	}

	pthread_mutex_lock(&nodePool->lock);
	if (nodePool->valid && nodePool->size > 0) {
			nodePool->size--;
	}

	pthread_mutex_unlock(&nodePool->lock);
	LWLockRelease(PoolerLock);
	return;
}

/*
 * Send SET query to all connections of agent.
 * Query is sent asynchronously and results are consumed
 */
int
PGXCNodeSendSetParallel(PoolAgent *agent, const char *set_command)
{
	NODE_CONNECTION 	*conn = NULL;
	List* invalid_slots = NIL;
	int		err_count = 0;
	int i;
	int ret = 0;

	WaitState oldStatus = pgstat_report_waitstatus(STATE_WAIT_UNDEFINED, true);

	for (i = 0; i < agent->num_coord_connections; i++)
	{
		if (agent->coord_connections[i] == NULL)
			continue;

		pgstat_report_waitstatus_comm(STATE_POOLER_WAIT_SETCMD, agent->coord_conn_oids[i]);

		conn = agent->coord_connections[i]->conn;
		
		if (conn->is_logic_conn)
			ret = LibcommSendQuery(conn, set_command);
		else
			ret = PQsendQuery(conn, set_command);

		if (ret == 0)
		{
			ereport(LOG, (errmsg("Set command(%s) to node %u failed %s.", 
				set_command,
				agent->coord_conn_oids[i],
				PQerrorMessage(conn))));

			PGXCNodeSendSetFail(invalid_slots, agent, &agent->coord_connections[i], &(agent->coord_conn_oids[i]));

			++err_count;
		}
	}


	for (i = 0; i < agent->num_dn_connections; i++)
	{
		if (agent->dn_connections[i] == NULL)
			continue;

		pgstat_report_waitstatus_comm(STATE_POOLER_WAIT_SETCMD, agent->dn_conn_oids[i]);

		conn = agent->dn_connections[i]->conn;
		
		if (conn->is_logic_conn)
			ret = LibcommSendQuery(conn, set_command);
		else
			ret = PQsendQuery(conn, set_command);

		if (ret == 0)
		{
			ereport(LOG, (errmsg("Set command(%s) to node %u failed %s.", 
				set_command,
				agent->dn_conn_oids[i],
				PQerrorMessage(conn))));

			PGXCNodeSendSetFail(invalid_slots, agent, &agent->dn_connections[i], &(agent->dn_conn_oids[i]));

			++err_count;
		}
	}

	for (i = 0; i < agent->num_coord_connections; i++)
	{
		if (agent->coord_connections[i] == NULL)
			continue;

		pgstat_report_waitstatus_comm(STATE_POOLER_WAIT_SETCMD, agent->coord_conn_oids[i]);

		conn = agent->coord_connections[i]->conn;

		if (PGXCNodeSetQueryGetResult(conn) == -1)
		{
			ereport(LOG, (errmsg("Wait result for set commond(%s) from node %u failed %s.", 
				set_command,
				agent->coord_conn_oids[i],
				PQerrorMessage(conn))));

			PGXCNodeSendSetFail(invalid_slots, agent, &agent->coord_connections[i], &(agent->coord_conn_oids[i]));

			++err_count;
		}
	}

	for (i = 0; i < agent->num_dn_connections; i++)
	{
		if (agent->dn_connections[i] == NULL)
			continue;

		pgstat_report_waitstatus_comm(STATE_POOLER_WAIT_SETCMD, agent->dn_conn_oids[i]);

		conn = agent->dn_connections[i]->conn;

		if (PGXCNodeSetQueryGetResult(conn) == -1)
		{
			ereport(LOG, (errmsg("Wait result for set commond(%s) from node %u failed %s.", 
				set_command,
				agent->dn_conn_oids[i],
				PQerrorMessage(conn))));

			PGXCNodeSendSetFail(invalid_slots, agent, &agent->dn_connections[i], &(agent->dn_conn_oids[i]));

			++err_count;
		}
	}

    (void)pgstat_report_waitstatus(oldStatus);
	destroy_slots(invalid_slots);

	return err_count;
}


/*
 * Send SET query to given connection in pooler stateless reuse mode.
 * Query is sent asynchronously and results are consumed
 */
int
PGXCNodeSendSetQueryPoolerStatelessReuse(NODE_CONNECTION *conn, const char *sql_command)
{
	if (conn->is_logic_conn)
	{
		if (!LibcommSendQueryPoolerStatelessReuse((NODE_CONNECTION *) conn, sql_command))
		return -1;
	}
	else
	{
		if (!PQsendQueryPoolerStatelessReuse((PGconn *) conn, sql_command))
			return -1;
	}

	return 0;
}


/*
 * Checks if connection active
 */
int
PGXCNodeConnected(NODE_CONNECTION *conn)
{
	/* Delegate call to the pglib */
	PGconn	   *pgconn = (PGconn *) conn;
	/*
	 * Simple check, want to do more comprehencive -
	 * check if it is ready for guery
	 */
	return (pgconn != NULL) && PQstatus(pgconn) == CONNECTION_OK;
}

/* Close the socket handle (this process' copy) and free occupied memory
 *
 * Note that we do not free the handle and its members. This will be
 * taken care of when the transaction ends, when u_sess->top_transaction_mem_cxt
 * is destroyed in xact.c.
 */
void pgxc_node_free(PGXCNodeHandle *handle)
{
#ifndef ENABLE_MULTIPLE_NODES
	Assert(false);
	DISTRIBUTED_FEATURE_NOT_SUPPORTED();
	return;
#else

	close(handle->sock);
	handle->sock = NO_SOCKET;

#endif
}

/*
 * Free all the node handles cached
 */
static void
pgxc_node_all_free(void)
{
    int i, j;

    for (i = 0; i < 2; i++) {
        int num_nodes;
        PGXCNodeHandle *array_handles = NULL;

        switch (i) {
            case 0:
                num_nodes = u_sess->pgxc_cxt.NumCoords;
                array_handles = u_sess->pgxc_cxt.co_handles;
                break;
            case 1:
                num_nodes = u_sess->pgxc_cxt.NumDataNodes;
                array_handles = u_sess->pgxc_cxt.dn_handles;
                break;
            default:
                Assert(0);
        }

        // In some abnormal cases, handles may not be initialized, for example,
        // CN is in restore mode and a new node is created. We should check NULL for
        // such cases.
        if (array_handles != NULL) {
            for (j = 0; j < num_nodes; j++) {
                PGXCNodeHandle *handle = &array_handles[j];
                pgxc_node_free(handle);
                pfree_ext(handle->inBuffer);
                pfree_ext(handle->outBuffer);
                pfree_ext(handle->error);
            }
            FREE_POINTER(array_handles);
        }
    }

    u_sess->pgxc_cxt.co_handles = NULL;
    u_sess->pgxc_cxt.NumCoords = 0;
    u_sess->pgxc_cxt.dn_handles = NULL;
    u_sess->pgxc_cxt.NumDataNodes = 0;
    u_sess->pgxc_cxt.NumTotalDataNodes = 0;
    u_sess->pgxc_cxt.NumStandbyDataNodes = 0;
    u_sess->pgxc_cxt.primary_data_node = InvalidOid;
    u_sess->pgxc_cxt.num_preferred_data_nodes = 0;
}

/*
 * Create and initialise internal structure to communicate to
 * Datanode via supplied socket descriptor.
 * Structure stores state info and I/O buffers
 */
void
pgxc_node_init(PGXCNodeHandle *handle, int sock)
{
#ifndef ENABLE_MULTIPLE_NODES
	Assert(false);
	DISTRIBUTED_FEATURE_NOT_SUPPORTED();
	return;
#else

	handle->sock = sock;
	handle->transaction_status = 'I';
	handle->state = DN_CONNECTION_STATE_IDLE;
	handle->combiner = NULL;
#ifdef DN_CONNECTION_DEBUG
	handle->have_row_desc = false;
#endif
	handle->error = NULL;
	handle->outEnd = 0;
	handle->inStart = 0;
	handle->inEnd = 0;
	handle->inCursor = 0;

#endif
}

/*
 * Is there any data enqueued in the TCP input buffer waiting
 * to be read sent by the PGXC node connection
 */

int
pgxc_node_is_data_enqueued(PGXCNodeHandle *conn)
{
	int ret;
	int enqueued;

	if (conn->sock < 0)
		return 0;
	ret = ioctl(conn->sock, FIONREAD, &enqueued);
	if (ret != 0)
		return 0;

	return enqueued;
}

int
light_node_read_data(PGXCNodeHandle *conn)
{
	const	int	someread = 0;
	int			nread;
	int			save_errno = 0;

	/* Left-justify any data in the buffer to make room */
	read_data_prepare_buffer(&conn->inStart, &conn->inEnd, &conn->inCursor, conn->inBuffer);

	if (conn->inSize - conn->inEnd < 8192)
	{
		ensure_in_buffer_capacity(8192, conn);
	}

retry:
	PGSTAT_INIT_TIME_RECORD();
	PGSTAT_START_TIME_RECORD();
	nread = recv(conn->sock, conn->inBuffer + conn->inEnd,
				 conn->inSize - conn->inEnd, 0);
	END_NET_RECV_INFO(nread);

	if (nread < 0)
	{
		save_errno = errno;

		if (save_errno == EINTR)
			goto retry;
		/* Some systems return EAGAIN/EWOULDBLOCK for no data */
#ifdef EAGAIN
		if (save_errno == EAGAIN)
			return someread;
#endif
#if defined(EWOULDBLOCK) && (!defined(EAGAIN) || (EWOULDBLOCK != EAGAIN))
		if (save_errno == EWOULDBLOCK)
			return someread;
#endif
		/* We might get ECONNRESET here if using TCP and backend died */
#ifdef ECONNRESET
		if (save_errno == ECONNRESET)
		{
			/*
			 * OK, we are getting a zero read even though select() says ready. This
			 * means the connection has been closed.  Cope.
			 */

			add_error_message(conn, "%s",
							"Datanode closed the connection unexpectedly\n"
				"\tThis probably means the Datanode terminated abnormally\n"
							"\tbefore or while processing the request.\n");
			conn->state = DN_CONNECTION_STATE_ERROR_FATAL;	/* No more connection to
														* backend */

			setSocketError(NULL, conn->remoteNodeName);
			return -1;
		}
#endif

		add_error_message(conn, "%s", "could not receive data from server");
		setSocketError(NULL, conn->remoteNodeName);
		return -1;

	}

	if (nread > 0)
	{
		conn->inEnd += nread;
		return 1;
	}

	if (nread == 0)
	{
		setSocketError("Remote close socket unexpectedly", conn->remoteNodeName);
		return EOF;
	}

	return EOF;
}

/*
 * Read up incoming messages from the PGXC node connection
 */
int
pgxc_node_read_data(PGXCNodeHandle *conn, bool close_if_error, bool StreamConnection)
{
	int			someread = 0;
	int			nread;

	if (conn->sock < 0)
	{
		if (close_if_error)
			add_error_message(conn, "%s", "bad socket");
		setSocketError("bad socket", conn->remoteNodeName);
		return EOF;
	}

	/* Left-justify any data in the buffer to make room */
	read_data_prepare_buffer(&conn->inStart, &conn->inEnd, &conn->inCursor, conn->inBuffer);

	/*
	 * If the buffer is fairly full, enlarge it. We need to be able to enlarge
	 * the buffer in case a single message exceeds the initial buffer size. We
	 * enlarge before filling the buffer entirely so as to avoid asking the
	 * kernel for a partial packet. The magic constant here should be large
	 * enough for a TCP packet or Unix pipe bufferload.  8K is the usual pipe
	 * buffer size, so...
	 */
	if (conn->inSize - conn->inEnd < LIBCOMM_BUFFER_SIZE)
	{
		ensure_in_buffer_capacity(LIBCOMM_BUFFER_SIZE, conn);
	}

retry:
#ifdef USE_SSL
	if (conn->pg_conn != NULL && conn->pg_conn->ssl != NULL)
	{
		nread = pgfdw_pqsecure_read(conn->pg_conn, conn->inBuffer + conn->inEnd, conn->inSize - conn->inEnd);
	}
	else
#endif
	{
		PGSTAT_INIT_TIME_RECORD();
		PGSTAT_START_TIME_RECORD();
		nread = recv(conn->sock, conn->inBuffer + conn->inEnd,
				 conn->inSize - conn->inEnd, 0);
		END_NET_RECV_INFO(nread);
	}

	if (nread < 0)
	{
		if (close_if_error)
			elog(DEBUG1, "dnrd errno = %d", errno);
		if (errno == EINTR)
			goto retry;
		/* Some systems return EAGAIN/EWOULDBLOCK for no data */
#ifdef EAGAIN
		if (errno == EAGAIN)
			return someread;
#endif
#if defined(EWOULDBLOCK) && (!defined(EAGAIN) || (EWOULDBLOCK != EAGAIN))
		if (errno == EWOULDBLOCK)
			return someread;
#endif
		/* We might get ECONNRESET here if using TCP and backend died */
#ifdef ECONNRESET
		if (errno == ECONNRESET)
		{
			/*
			 * OK, we are getting a zero read even though select() says ready. This
			 * means the connection has been closed.  Cope.
			 */
			if (close_if_error)
			{
				add_error_message(conn, "%s",
								"Datanode closed the connection unexpectedly\n"
					"\tThis probably means the Datanode terminated abnormally\n"
								"\tbefore or while processing the request.\n");
				conn->state = DN_CONNECTION_STATE_ERROR_FATAL;	/* No more connection to
															* backend */

				//Stream Connection, socket close by consumer object.
				if(StreamConnection == false)
					closesocket(conn->sock);

				conn->sock = NO_SOCKET;
			}
			setSocketError(NULL, conn->remoteNodeName);
			return -1;
		}
#endif
		if (close_if_error)
			add_error_message(conn, "%s", "could not receive data from server");
		setSocketError(NULL, conn->remoteNodeName);
		return -1;

	}

	if (nread > 0)
	{
		conn->inEnd += nread;
		/*
		 * Hack to deal with the fact that some kernels will only give us back
		 * 1 packet per recv() call, even if we asked for more and there is
		 * more available.	If it looks like we are reading a long message,
		 * loop back to recv() again immediately, until we run out of data or
		 * buffer space.  Without this, the block-and-restart behavior of
		 * libpq's higher levels leads to O(N^2) performance on long messages.
		 *
		 * Since we left-justified the data above, conn->inEnd gives the
		 * amount of data already read in the current message.	We consider
		 * the message "long" once we have acquired 32k ...
		 */

		//for stream connection, we can not stick to one connection, that will lead to hang.
		//for ssl connection, we can not stick to one connection, that will lead to hang.
		if (conn->pg_conn == NULL && StreamConnection == false && conn->inEnd > 32768 &&
			(conn->inSize - conn->inEnd) >= LIBCOMM_BUFFER_SIZE)
		{
			someread = 1;
			goto retry;
		}

		return 1;
	}

	if (nread == 0)
	{
		if (close_if_error)
			elog(DEBUG1, "nread returned 0");

		setSocketError("Remote close socket unexpectedly", conn->remoteNodeName);
		return EOF;
	}

	if (someread)
		return 1;				/* got a zero read after successful tries */

	return 0;
}

/*
 * Read up incoming messages from the PGXC node connection
 * Stream commucation.
 */
int
pgxc_node_read_data_from_logic_conn(PGXCNodeHandle *conn, bool close_if_error)
{
	int	nread;

	/* Left-justify any data in the buffer to make room */
	read_data_prepare_buffer(&conn->inStart, &conn->inEnd, &conn->inCursor, conn->inBuffer);

	/*
	 * If the buffer is fairly full, enlarge it. We need to be able to enlarge
	 * the buffer in case a single message exceeds the initial buffer size. We
	 * enlarge before filling the buffer entirely so as to avoid asking the
	 * kernel for a partial packet. The magic constant here should be large
	 * enough for a TCP packet or Unix pipe bufferload.  8K is the usual pipe
	 * buffer size, so...
	 */
	if (conn->inSize - conn->inEnd < LIBCOMM_BUFFER_SIZE)
	{
		ensure_in_buffer_capacity(LIBCOMM_BUFFER_SIZE, conn);
	}

	nread = gs_recv(&conn->gsock, conn->inBuffer + conn->inEnd,
				 conn->inSize - conn->inEnd);

	if (nread < 0)
	{
		if(errno == ECOMMTCPNODATA)
		{
			return 0;
		}
		if (close_if_error)
		{
			elog(DEBUG1, "dnrd errno = %d", errno);
			add_error_message(conn, "%s", "could not receive data from server");
		}
		return -1;

	}
	
	if (nread == 0)
	{
		if (close_if_error)
			elog(DEBUG1, "nread returned 0");
		return EOF;
	}
	
	conn->inEnd += nread;
	return 1;
	
}

/* receive only one connection, for light proxy */
bool
light_node_receive(PGXCNodeHandle* handle)
{
  	int			res_select;
	struct 		pollfd  ufd[1];

	ufd[0].fd = handle->sock;
	ufd[0].events = POLLIN | POLLPRI |POLLRDHUP | POLLERR | POLLHUP;

	/* Allow cancel/die interrupts to be processed while waiting */
	t_thrd.int_cxt.ImmediateInterruptOK = true;
	//Check for interrupts here to handle cancel request when it keeps polling
	CHECK_FOR_INTERRUPTS();
	const	int time_out = -1;
	WaitStatePhase oldPhase = pgstat_report_waitstatus_phase(PHASE_NONE, true);
	WaitState oldStatus = pgstat_report_waitstatus_comm(STATE_WAIT_NODE, handle->nodeoid, 1);

retry:

	res_select = poll(ufd, 1, time_out);

	t_thrd.int_cxt.ImmediateInterruptOK = false;
	if (res_select < 0)
	{
		/* error - retry if EINTR or EAGAIN */
		if (errno == EINTR || errno == EAGAIN)
			goto retry;

		if (errno == EBADF)
		{
			elog(WARNING, "select() bad file descriptor set");
		}
		elog(WARNING, "select() error: %d", errno);
		pgstat_reset_waitStatePhase(oldStatus, oldPhase);

		if (errno)
			return ERROR_OCCURED;
		return NO_ERROR_OCCURED;
	}

	if (res_select == 0)
	{
		/* Handle timeout */
		elog(WARNING, "timeout while waiting for response, error %d", errno);
		//do not return, as we specifiy timeout  -1
	}

	if (ufd[0].revents & (POLLIN|POLLERR|POLLHUP))
	{
		ufd[0].revents = 0;
		int	read_status = light_node_read_data(handle);
		pgstat_reset_waitStatePhase(oldStatus, oldPhase);
		if (read_status == EOF || read_status < 0)
		{
			/* Should we read from the other connections before returning? */
			elog(WARNING, "receive unexpected EOF on datanode connection.");
			return ERROR_OCCURED;
		}
		else
			return NO_ERROR_OCCURED;
	}
	pgstat_reset_waitStatePhase(oldStatus, oldPhase);
	return NO_ERROR_OCCURED;
}


/* receive only one connection from logic connection, for light proxy */
bool
light_node_receive_from_logic_conn(PGXCNodeHandle* handle)
{
	Assert(false);
	DISTRIBUTED_FEATURE_NOT_SUPPORTED();
	return false;
}

/*
 * palloc array list for t_thrd.pgxc_cxt.pgxc_net_ctl which is used to receive msgs
 * from backend by function pgxc_node_recveive
 */
void pgxc_palloc_net_ctl(int conn_num)
{
	if (conn_num <= t_thrd.pgxc_cxt.pgxc_net_ctl->conn_num)
		return;
	
	/* Free all the existing information first when realloc*/
	if (t_thrd.pgxc_cxt.pgxc_net_ctl->conn_num > 0)
	{
		if(t_thrd.pgxc_cxt.pgxc_net_ctl->poll2conn != NULL)
			pfree_ext(t_thrd.pgxc_cxt.pgxc_net_ctl->poll2conn);
		if(t_thrd.pgxc_cxt.pgxc_net_ctl->datamarks != NULL)
			pfree_ext(t_thrd.pgxc_cxt.pgxc_net_ctl->datamarks);
		if(t_thrd.pgxc_cxt.pgxc_net_ctl->gs_sock != NULL)
			pfree_ext(t_thrd.pgxc_cxt.pgxc_net_ctl->gs_sock);
		if(t_thrd.pgxc_cxt.pgxc_net_ctl->ufds != NULL)
			pfree_ext(t_thrd.pgxc_cxt.pgxc_net_ctl->ufds);
	}
	t_thrd.pgxc_cxt.pgxc_net_ctl->conn_num = 0;

	MemoryContext oldcontext = MemoryContextSwitchTo(THREAD_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_COMMUNICATION));
	
	t_thrd.pgxc_cxt.pgxc_net_ctl->poll2conn = (int *)palloc((conn_num)  * sizeof(int));
	t_thrd.pgxc_cxt.pgxc_net_ctl->datamarks = (int *)palloc((conn_num) * sizeof(int));
	t_thrd.pgxc_cxt.pgxc_net_ctl->gs_sock = (gsocket *)palloc((conn_num)  * sizeof(gsocket));
	t_thrd.pgxc_cxt.pgxc_net_ctl->ufds = (pollfd*)palloc((conn_num)	* sizeof(struct pollfd));
	
	MemoryContextSwitchTo(oldcontext);

	t_thrd.pgxc_cxt.pgxc_net_ctl->conn_num = conn_num;
}

/*
 * Wait while at least one of specified connections has data available and read
 * the data into the buffer
 */
bool
pgxc_node_receive(const int conn_count,
							PGXCNodeHandle **connections,
							struct timeval *timeout)
{
#ifndef ENABLE_MULTIPLE_NODES
	Assert(false);
	DISTRIBUTED_FEATURE_NOT_SUPPORTED();
	return false;
#else

#define ERROR_OCCURED		true
#define NO_ERROR_OCCURED	false
	int			i,
				res_select,
				nfds = 0;
	fd_set			readfds;
	bool			is_msg_buffered;

	FD_ZERO(&readfds);

	is_msg_buffered = false;
	for (i = 0; i < conn_count; i++)
	{
		/* If connection has a buffered message */
		if (HAS_MESSAGE_BUFFERED(connections[i]))
		{
			is_msg_buffered = true;
			break;
		}
	}

	for (i = 0; i < conn_count; i++)
	{
		/* If connection finished sending do not wait input from it */
		if (connections[i]->state == DN_CONNECTION_STATE_IDLE || HAS_MESSAGE_BUFFERED(connections[i]))
			continue;

		/* prepare select params */
		if (connections[i]->sock > 0)
		{
			FD_SET(connections[i]->sock, &readfds);
			nfds = connections[i]->sock;
		}
		else
		{
			/* flag as bad, it will be removed from the list */
			connections[i]->state = DN_CONNECTION_STATE_ERROR_FATAL;
		}
	}

	/*
	 * Return if we do not have connections to receive input
	 */
	if (nfds == 0)
	{
		if (is_msg_buffered)
			return NO_ERROR_OCCURED;
		return ERROR_OCCURED;
	}

retry:
	res_select = select(nfds + 1, &readfds, NULL, NULL, timeout);
	if (res_select < 0)
	{
		/* error - retry if EINTR or EAGAIN */
		if (errno == EINTR || errno == EAGAIN)
			goto retry;

		if (errno == EBADF)
		{
			elog(WARNING, "select() bad file descriptor set");
		}
		elog(WARNING, "select() error: %d", errno);
		if (errno)
			return ERROR_OCCURED;
		return NO_ERROR_OCCURED;
	}

	if (res_select == 0)
	{
		/* Handle timeout */
		elog(WARNING, "timeout while waiting for response");
		return ERROR_OCCURED;
	}

	/* read data */
	for (i = 0; i < conn_count; i++)
	{
		PGXCNodeHandle *conn = connections[i];

		if (FD_ISSET(conn->sock, &readfds))
		{
			int	read_status = pgxc_node_read_data(conn, true);

			if (read_status == EOF || read_status < 0)
			{
				/* Can not read - no more actions, just discard connection */
				conn->state = DN_CONNECTION_STATE_ERROR_FATAL;
				add_error_message(conn, "%s", "unexpected EOF on datanode connection");
				elog(WARNING, "unexpected EOF on datanode connection");
				/* Should we read from the other connections before returning? */
				return ERROR_OCCURED;
			}
		}
	}
	return NO_ERROR_OCCURED;

#endif
}

/*
 * @Description: For TCP mode. Wait while at least one of specified connections
 *               has data available and read the data into the buffer
 *
 * @param[IN] conn_count:  number of connections
 * @param[IN] connections:  all connection handle with Datanode
 * @param[IN] StreamNetCtl:  stream net controller for node receiving
 * @return: bool, true if receive data successfully
 */
bool
datanode_receive_from_physic_conn(const int conn_count,
							PGXCNodeHandle **connections,
							struct timeval *timeout)
{
	int			i,
				res_select,
				nfds = 0,
				target = 0;
	bool		is_msg_buffered = false;

	struct pollfd* ufds = t_thrd.pgxc_cxt.pgxc_net_ctl->ufds;
	int* poll2conn = t_thrd.pgxc_cxt.pgxc_net_ctl->poll2conn;

	for (i = 0; i < conn_count; i++)
	{
		/* If connection has a buffered message */
		if (HAS_MESSAGE_BUFFERED(connections[i]))
		{
			is_msg_buffered = true;
			break;
		}
	}

	for (i = 0; i < conn_count; i++)
	{
		/* If connection finished sending do not wait input from it */
		if (connections[i]->state == DN_CONNECTION_STATE_IDLE || HAS_MESSAGE_BUFFERED(connections[i]))
			continue;

		/* prepare select params */
		// our system will close STDIN,STDOUT,STDERR
		// reference the logic SysLoggerMain close the standard in/out/error
		if (connections[i]->sock >= 0)
		{
			target = i;
			ufds[nfds].fd = connections[i]->sock;
			ufds[nfds].events = POLLIN | POLLPRI |POLLRDHUP | POLLERR | POLLHUP ;
			poll2conn[nfds] = i;
			++nfds;
		}
		else
		{
			//inconsistent connection status will lead to hang, so we should just return error.
			/* flag as bad, it will be removed from the list */
			connections[i]->state = DN_CONNECTION_STATE_ERROR_FATAL;
			setSocketError("Invalid socket", connections[i]->remoteNodeName);
			elog(WARNING, "pgxc_node_receive set DN_CONNECTION_STATE_ERROR_FATAL for node %u", connections[i]->nodeoid);

			return ERROR_OCCURED;
		}
	}

	/*
	 * Return if we do not have connections to receive input
	 */
	if (nfds == 0)
	{
		if (is_msg_buffered)
			return NO_ERROR_OCCURED;
		return ERROR_OCCURED;
	}

	/* use nodeoid in CN, we can get nodename by get_pgxc_nodename directly */
	WaitStatePhase oldPhase = pgstat_report_waitstatus_phase(PHASE_NONE, true);
	WaitState oldStatus = pgstat_report_waitstatus_comm(STATE_WAIT_NODE, connections[target]->nodeoid, nfds);

retry:
	/* Allow cancel/die interrupts to be processed while waiting */

	t_thrd.int_cxt.ImmediateInterruptOK = true;
	//Check for interrupts here to handle cancel request when it keeps polling
	//
	CHECK_FOR_INTERRUPTS();
	int time_out = -1;
	if(timeout != NULL)
	{
		time_out = timeout->tv_sec *1000;
	}

	NetWorkTimePollStart(t_thrd.pgxc_cxt.GlobalNetInstr);

	res_select = poll(ufds, nfds, time_out);

    NetWorkTimePollEnd(t_thrd.pgxc_cxt.GlobalNetInstr);

    t_thrd.int_cxt.ImmediateInterruptOK = false;
	if (res_select < 0)
	{
		/* error - retry if EINTR or EAGAIN */
		if (errno == EINTR || errno == EAGAIN)
			goto retry;

		if (errno == EBADF)
		{
			elog(WARNING, "select() bad file descriptor set");
		}
		elog(WARNING, "select() error: %d", errno);

		pgstat_reset_waitStatePhase(oldStatus, oldPhase);

		if (errno)
			return ERROR_OCCURED;
		return NO_ERROR_OCCURED;
	}

	if (res_select == 0)
	{
		/* Handle timeout */
		elog(WARNING, "timeout while waiting for response");
		pgstat_reset_waitStatePhase(oldStatus, oldPhase);
		return ERROR_OCCURED;
	}

	/* read data */
	for (i = 0; i < nfds; i++)
	{
		if (ufds[i].revents & (POLLIN|POLLERR|POLLHUP))
		{
			PGXCNodeHandle *conn = connections[poll2conn[i]];
			ufds[i].revents = 0;
			
			int	read_status = pgxc_node_read_data(conn, true, false);

			if (read_status == EOF || read_status < 0)
			{
				/* Can not read - no more actions, just discard connection */
				conn->state = DN_CONNECTION_STATE_ERROR_FATAL;
				add_error_message(conn, "%s", "unexpected EOF on datanode connection");
				CONNECTION_ERROR_NORMALIZE(WARNING, receive unexpected "EOF", conn);
				/* Should we read from the other connections before returning? */
				pgstat_reset_waitStatePhase(oldStatus, oldPhase);
				return ERROR_OCCURED;
			}
		}
	}

	pgstat_reset_waitStatePhase(oldStatus, oldPhase);
	return NO_ERROR_OCCURED;
}


/*
 * @Description: For logic connection mode. Wait while at least one of specified connections
 *               has data available and read the data into the buffer
 *
 * @param[IN] conn_count:  number of connections
 * @param[IN] connections:  all connection handle with Datanode
 * @param[IN] StreamNetCtl:  stream net controller for node receiving
 * @return: bool, true if receive data successfully
 */
bool
datanode_receive_from_logic_conn(const int conn_count,
											PGXCNodeHandle** connections,
											StreamNetCtl* ctl,
											int timeout)
{
	Assert(false);
	DISTRIBUTED_FEATURE_NOT_SUPPORTED();
	return false;
}

/*
 * Get one character from the connection buffer and advance cursor
 */
static int
get_char(PGXCNodeHandle * conn, char *out)
{
	if (conn->inCursor < conn->inEnd)
	{
		*out = conn->inBuffer[conn->inCursor++];
		return 0;
	}
	return EOF;
}

/*
 * Read an integer from the connection buffer and advance cursor
 */
static int
get_int(PGXCNodeHandle *conn, size_t len, int *out)
{
	unsigned short tmp2;
	unsigned int tmp4;
	errno_t	ss_rc = 0;

	if (conn->inCursor + len > conn->inEnd)
		return EOF;

	switch (len)
	{
		case 2:
			ss_rc = memcpy_s(&tmp2, sizeof(unsigned short), conn->inBuffer + conn->inCursor, 2);
			securec_check(ss_rc,"\0","\0");
			conn->inCursor += 2;
			*out = (int) ntohs(tmp2);
			break;
		case 4:
			ss_rc = memcpy_s(&tmp4, sizeof(unsigned int), conn->inBuffer + conn->inCursor, 4);
			securec_check(ss_rc,"\0","\0");
			conn->inCursor += 4;
			*out = (int) ntohl(tmp4);
			break;
		default:
			add_error_message(conn, "%s", "not supported int size");
			return EOF;
	}

	return 0;
}

/*
 * get_message
 * If connection has enough data read entire message from the connection buffer
 * and returns message type. Message data and data length are returned as
 * var parameters.
 * If buffer does not have enough data leaves cursor unchanged, changes
 * connection status to DN_CONNECTION_STATE_QUERY indicating it needs to
 * receive more and returns \0
 * conn - connection to read from
 * len - returned length of the data where msg is pointing to
 * msg - returns pointer to memory in the incoming buffer. The buffer probably
 * will be overwritten upon next receive, so if caller wants to refer it later
 * it should make a copy.
 */
char get_message(PGXCNodeHandle *conn, int *len, char **msg)
{
#ifndef ENABLE_MULTIPLE_NODES
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return '\0';
#else

    char msgtype;

    if (get_char(conn, &msgtype) || get_int(conn, 4, len))
    {
        /* Successful get_char would move cursor, restore position */
        conn->inCursor = conn->inStart;
        return '\0';
    }

    *len -= 4;
    if (unlikely(*len < 0)) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("message len is too short")));
    }

    if (unlikely(conn->inCursor > (INT_MAX - *len))) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("conn cursor overflow")));
    }
    if ((((size_t)*len) > MaxAllocSize)) {
		add_error_message(conn, "%s", "illegal message size");
		return '\0';
	}

    if (conn->inCursor + *len > conn->inEnd)
    {
        /*
         * Not enough data in the buffer, we should read more.
         * Reading function will discard already consumed data in the buffer
         * till conn->inBegin. Then we want the message that is partly in the
         * buffer now has been read completely, to avoid extra read/handle
         * cycles. The space needed is 1 byte for message type, 4 bytes for
         * message length and message itself which size is currently in *len.
         * The buffer may already be large enough, in this case the function
         * ensure_in_buffer_capacity() will immediately return
         */
        ensure_in_buffer_capacity(5 + (size_t) *len, conn);
        conn->inCursor = conn->inStart;
        return '\0';
    }

    *msg = conn->inBuffer + conn->inCursor;
    conn->inCursor += *len;
    conn->inStart = conn->inCursor;
    return msgtype;

#endif
}

/*
 * Destory all Datanode and Coordinator connections,release occupied memory
 * The connection will not back to pool
 */
void
destroy_handles(void)
{
	ereport(DEBUG5, (errcode(ERRCODE_SUCCESSFUL_COMPLETION),
				errmsg("destroy_handles()")));

	int			i;

	PoolHandle * pool_agent = GetPoolAgent();
	if (pool_agent == NULL || (pool_agent->temp_namespace != NULL && u_sess->pgxc_cxt.datanode_count == 0 && u_sess->pgxc_cxt.coord_count == 0))
	{
		/*
		 * Because happand ERROR in create connections,
		 * maybe some connections is built in agent but not send params,
		 * must send params to all connections next time.
		 */
        u_sess->pgxc_cxt.PoolerResendParams = true;
        return;
	}

	/* Mark all datanode statements as deactive before destory connection */
	DeActiveAllDataNodeStatements();

	/* Free Datanodes handles */
	for (i = 0; i < u_sess->pgxc_cxt.NumDataNodes; i++)
	{
		PGXCNodeHandle *handle = &u_sess->pgxc_cxt.dn_handles[i];

		handle->state = DN_CONNECTION_STATE_IDLE;

		pgxc_node_free(handle);
		pgxc_node_init(handle, NO_SOCKET);
	}

	/* Collect Coordinator handles */
	for (i = 0; i < u_sess->pgxc_cxt.NumCoords; i++)
	{
		PGXCNodeHandle *handle = &u_sess->pgxc_cxt.co_handles[i];

		handle->state = DN_CONNECTION_STATE_IDLE;
		pgxc_node_free(handle);
		pgxc_node_init(handle, NO_SOCKET);
	}

	/* And finally release all the connections on pooler */
	PoolManagerReleaseConnections(NULL, 0, true);

	u_sess->pgxc_cxt.datanode_count = 0;
	u_sess->pgxc_cxt.coord_count = 0;
}

/*
 * Release all pgxc handles
 * release occupied memory
 */
void
release_pgxc_handles(PGXCNodeAllHandles *pgxc_handles)
{
	Assert(false);
	DISTRIBUTED_FEATURE_NOT_SUPPORTED();
	return;
}

/*
 * Release all Datanode and Coordinator connections
 * back to pool and release occupied memory
 */
void
release_handles(void)
{
	ereport(DEBUG5, (errcode(ERRCODE_SUCCESSFUL_COMPLETION),
				errmsg("release_handles")));

	int			i;
	bool		has_error = false;

	/* If PoolAgent is NULL, return is OK */
	if (GetPoolAgent() == NULL)
		return;

	/* Do not release connections if we have prepared statements on nodes */
	if (HaveActiveDatanodeStatements())
		return;

	// Each byte identify a handle status and send the status string 
	// back to the pool agent when handle state abnormal.
	char *status_array = (char *)palloc( sizeof(char) * (u_sess->pgxc_cxt.NumDataNodes + u_sess->pgxc_cxt.NumCoords + 1));

	securec_check(memset_s(status_array, (u_sess->pgxc_cxt.NumDataNodes + u_sess->pgxc_cxt.NumCoords),
					CONN_STATE_NORMAL, (u_sess->pgxc_cxt.NumDataNodes + u_sess->pgxc_cxt.NumCoords)), "\0", "\0");

	/* Free Datanodes handles */
	for (i = 0; i < u_sess->pgxc_cxt.NumDataNodes; i++)
	{
		PGXCNodeHandle *handle = &u_sess->pgxc_cxt.dn_handles[i];

		if (IS_VALID_CONNECTION(handle))
		{
			if (handle->state != DN_CONNECTION_STATE_IDLE){
				status_array[i]= CONN_STATE_ERROR;
				has_error = true;
				elog(DEBUG1, "Connection to Datanode %u has unexpected state %d and will be dropped",
					 handle->nodeoid, handle->state);
			}
			pgxc_node_free(handle);
		}
	}

	/* Collect Coordinator handles */
	for (i = 0; i < u_sess->pgxc_cxt.NumCoords; i++)
	{
		PGXCNodeHandle *handle = &u_sess->pgxc_cxt.co_handles[i];

		if (handle->sock != NO_SOCKET)
		{
			if (handle->state != DN_CONNECTION_STATE_IDLE){
				status_array[u_sess->pgxc_cxt.NumDataNodes + i]  = CONN_STATE_ERROR;
				has_error = true;
				elog(DEBUG1, "Connection to Coordinator %u has unexpected state %d and will be dropped",
					 handle->nodeoid, handle->state);
			}
			pgxc_node_free(handle);
		}
	}

	status_array[u_sess->pgxc_cxt.NumDataNodes + u_sess->pgxc_cxt.NumCoords] = '\0';

	/* And finally release all the connections on pooler */
	PoolManagerReleaseConnections(status_array, (u_sess->pgxc_cxt.NumDataNodes + u_sess->pgxc_cxt.NumCoords), has_error);

	pfree(status_array);
	status_array = NULL;

	u_sess->pgxc_cxt.datanode_count = 0;
	u_sess->pgxc_cxt.coord_count = 0;
}

//stop a query due to early stop, mainly for cursor.
void
stop_query(void)
{
    ereport(DEBUG3, (errmsg("[%s()]: shouldn't run here", __FUNCTION__)));
    return;
}

// Reset handle's outBuffer.
// the default buffer size is 16K. see also init_pgxc_handle().
//
// now it's called during ROLLBACK. because this buffer maybe holds too many memory (for
// example failed when sending ~1G data), so that ROLLBACK cannot require extra
// space to append abort message and send to other nodes if this buffer is not freed.
//
// the other thing is, we must use the outBuffer's existing memory-context but not the current
// memory-context which will be destroyed and deleted. so repalloc() is called.
//
// Make sure this transaction will be rolled back successfully.
//
// Warning: all the messages in this buffer current will be cleaned up and skipped.
// 
void
ResetHandleOutBuffer(PGXCNodeHandle *handle)
{
	if (handle != NULL && handle->outBuffer && (handle->outSize > (16 * 1024)))
	{
		/* 
		 * We need allocate outBuffer under t_thrd.top_mem_cxt as it is accessed
		 * in function exec_init_poolhandles().
		 */
		MemoryContext old_context = MemoryContextSwitchTo(MemoryContextOriginal((char*)handle->outBuffer));
		char * temBuffer = handle->outBuffer;
		handle->outBuffer = (char *) palloc(16 * 1024);
		handle->outSize = 16 * 1024;
		handle->outEnd = 0;
		(void)MemoryContextSwitchTo(old_context);

		/* free the buffer only when we successfully palloc a new one. */
		pfree(temBuffer);
		temBuffer = NULL;
	}
}

void
cancel_query_without_read(void)
{
	int			i;
	int 		dn_cancel[u_sess->pgxc_cxt.NumDataNodes];
	int			co_cancel[u_sess->pgxc_cxt.NumCoords];
	int			dn_count = 0;
	int			co_count = 0;

	if (u_sess->pgxc_cxt.datanode_count == 0 && u_sess->pgxc_cxt.coord_count == 0)
		return;

	/* Collect Datanodes handles */
	for (i = 0; i < u_sess->pgxc_cxt.NumDataNodes; i++)
	{
		PGXCNodeHandle *handle = &u_sess->pgxc_cxt.dn_handles[i];

		ResetHandleOutBuffer(handle);

		if (IS_VALID_CONNECTION(handle))
		{
			dn_cancel[dn_count++] = PGXCNodeGetNodeId(handle->nodeoid,
													PGXC_NODE_DATANODE);
		}
	}

	/* Collect Coordinator handles */
	for (i = 0; i < u_sess->pgxc_cxt.NumCoords; i++)
	{
		PGXCNodeHandle *handle = &u_sess->pgxc_cxt.co_handles[i];

		ResetHandleOutBuffer(handle);

		if (handle->sock != NO_SOCKET)
		{
			co_cancel[co_count++] = PGXCNodeGetNodeId(handle->nodeoid,
													PGXC_NODE_COORDINATOR);
		}
	}

	/* Failed to send cancel to some data nodes */
	if (PoolManagerCancelQuery(dn_count, dn_cancel, co_count, co_cancel))
		destroy_handles();
}


/*
 * cancel a running query due to error while processing rows
 */
void
cancel_query(void)
{
	int			i;
	int			dn_cancel[u_sess->pgxc_cxt.NumDataNodes];
	int			co_cancel[u_sess->pgxc_cxt.NumCoords];
	int			dn_count = 0;
	int			co_count = 0;
	int			res = 0;
	bool	flushOK = true;

	if (u_sess->pgxc_cxt.datanode_count == 0 && u_sess->pgxc_cxt.coord_count == 0)
		return;

	/* Collect Datanodes handles */
	for (i = 0; i < u_sess->pgxc_cxt.NumDataNodes; i++)
	{
		PGXCNodeHandle *handle = &u_sess->pgxc_cxt.dn_handles[i];

		if (IS_VALID_CONNECTION(handle) && (handle->state != DN_CONNECTION_STATE_IDLE))
		{
			dn_cancel[dn_count++] = PGXCNodeGetNodeId(handle->nodeoid,
													  PGXC_NODE_DATANODE);
		}
	}

	/* Collect Coordinator handles */
	for (i = 0; i < u_sess->pgxc_cxt.NumCoords; i++)
	{
		PGXCNodeHandle *handle = &u_sess->pgxc_cxt.co_handles[i];

		if ((handle->sock != NO_SOCKET) && (handle->state != DN_CONNECTION_STATE_IDLE))
		{
			co_cancel[co_count++] = PGXCNodeGetNodeId(handle->nodeoid,
													PGXC_NODE_COORDINATOR);
		}
	}

	res = PoolManagerCancelQuery(dn_count, dn_cancel, co_count, co_cancel);

#ifdef ENABLE_DISTRIBUTE_TEST
	/*
	 * Read responses from the nodes to whom we sent the cancel command. This
	 * ensures that there are no pending messages left on the connection
	 */
	if (TEST_STUB(CN_CANCEL_SUBQUERY_FLUSH_FAILED, twophase_default_error_emit) ||
		TEST_STUB(CN_COMMIT_BEFORE_GTM_FAILED_AND_CANCEL_FLUSH_FAILED, twophase_default_error_emit))
	{
		ereport(g_instance.distribute_test_param_instance->elevel, 
		    (errmsg("SUBXACT_TEST  %s: CN flush read failed during cancel subquery.", 
		        g_instance.attr.attr_common.PGXCNodeName)));
		flushOK = false;
	}

#endif
	if (flushOK)
	{
		for (i = 0; i < u_sess->pgxc_cxt.NumDataNodes; i++)
		{
			PGXCNodeHandle *handle = &u_sess->pgxc_cxt.dn_handles[i];

			if (IS_VALID_CONNECTION(handle) && (handle->state != DN_CONNECTION_STATE_IDLE))
			{
				if (!pgxc_node_flush_read(handle))
				{
					flushOK = false;
					break;
				}

				handle->state = DN_CONNECTION_STATE_IDLE;
				handle->outEnd = 0;
			}
		}
	}

	if (flushOK)
	{
		for (i = 0; i < u_sess->pgxc_cxt.NumCoords; i++)
		{
			PGXCNodeHandle *handle = &u_sess->pgxc_cxt.co_handles[i];

			if (handle->sock != NO_SOCKET && handle->state != DN_CONNECTION_STATE_IDLE)
			{
				if (!pgxc_node_flush_read(handle))
				{
					flushOK = false;
					break;
				}

				handle->state = DN_CONNECTION_STATE_IDLE;
				handle->outEnd = 0;
			}
		}
	}

	/* Failed to send cancel to some nodes or failed to flush some nodes */
	if (res || !flushOK)
	{
		destroy_handles();
		t_thrd.xact_cxt.handlesDestroyedInCancelQuery = true;
	}

	/*
	 * Hack to wait a moment to cancel requests are processed in other nodes.
	 * If we send a new query to nodes before cancel requests get to be
	 * processed, the query will get unanticipated failure.
	 * As we have no way to know when to the request processed, we can't not
	 * wait an experimental duration (10ms).
	 */
#if PGXC_CANCEL_DELAY > 0
	pg_usleep(PGXC_CANCEL_DELAY * 1000);
#endif
}

/*
 * This method won't return until all network buffers are empty
 * To ensure all data in all network buffers is read and wasted
 */
void
clear_all_data(void)
{
	int			i;

	if (u_sess->pgxc_cxt.datanode_count == 0 && u_sess->pgxc_cxt.coord_count == 0)
		return;

	/* Collect Datanodes handles */
	for (i = 0; i < u_sess->pgxc_cxt.NumDataNodes; i++)
	{
		PGXCNodeHandle *handle = &u_sess->pgxc_cxt.dn_handles[i];

		if (IS_VALID_CONNECTION(handle) && handle->state != DN_CONNECTION_STATE_IDLE)
		{
			pgxc_node_flush_read(handle);
			handle->state = DN_CONNECTION_STATE_IDLE;
		}
		/* Clear any previous error messages */
		pfree_ext(handle->error);
	}

	/* Collect Coordinator handles */
	for (i = 0; i < u_sess->pgxc_cxt.NumCoords; i++)
	{
		PGXCNodeHandle *handle = &u_sess->pgxc_cxt.co_handles[i];

		if (handle->sock != NO_SOCKET && handle->state != DN_CONNECTION_STATE_IDLE)
		{
			pgxc_node_flush_read(handle);
			handle->state = DN_CONNECTION_STATE_IDLE;
		}
		/* Clear any previous error messages */
		pfree_ext(handle->error);
	}
}

/*
 * Ensure specified amount of data can fit to the incoming buffer and
 * increase it if necessary
 */
void
ensure_in_buffer_capacity(size_t bytes_needed, PGXCNodeHandle *handle)
{
	enlargeBufferSize (bytes_needed, 
			handle->inEnd, &handle->inSize, &handle->inBuffer);
}

/*
 * Ensure specified amount of data can fit to the outgoing buffer and
 * increase it if necessary
 */
void
ensure_out_buffer_capacity(size_t bytes_needed, PGXCNodeHandle *handle)
{
#ifndef ENABLE_MULTIPLE_NODES
	Assert(false);
	DISTRIBUTED_FEATURE_NOT_SUPPORTED();
	return;
#else

	int			newsize = handle->outSize;
	char	   *newbuf = NULL;

	if (bytes_needed <= (size_t) newsize)
		return 0;

	do
	{
		newsize *= 2;
	} while (newsize > 0 && bytes_needed > (size_t) newsize);

	if (newsize > 0 && bytes_needed <= (size_t) newsize)
	{
		newbuf = repalloc(handle->outBuffer, newsize);
		if (newbuf)
		{
			/* repalloc succeeded */
			handle->outBuffer = newbuf;
			handle->outSize = newsize;
			return 0;
		}
	}

	newsize = handle->outSize;
	do
	{
		newsize += 8192;
	} while (newsize > 0 && bytes_needed > (size_t) newsize);

	if (newsize > 0 && bytes_needed <= (size_t) newsize)
	{
		newbuf = repalloc(handle->outBuffer, newsize);
		if (newbuf)
		{
			/* repalloc succeeded */
			handle->outBuffer = newbuf;
			handle->outSize = newsize;
			return 0;
		}
	}

	return EOF;

#endif
}

/*
 * Send specified amount of data from the outgoing buffer over the connection
 */
int
send_some(PGXCNodeHandle *handle, int len)
{
	char	   *ptr = handle->outBuffer;
	bool 		SaveImmediateInterruptOK = t_thrd.int_cxt.ImmediateInterruptOK;

	LIBCOMM_DEBUG_LOG("send_some to node:%s[nid:%u,sid:%u] with msg:%c, len:%d.",
		handle->remoteNodeName,
		handle->gsock.idx,
		handle->gsock.sid,
		ptr[0],
		len);

	/* while there's still data to send */
	while (len > 0)
	{
		int			sent;

		/*
		 * After primary gtm is down, CN will cancel the process connected to the down gtm, 
		 * it will make send_some to process interrupt immediately, and at 
		 * abort transaction, it will cancel query without read. When dn is
		 * receiving data(pgxc_node_receive or read command), it will 
		 * receive SIGINT to be interrupted.
		 * 
		 * Sending is ok but processing interrupt before return in gs_send,  
		 * may cause handle buffer's data to be resent and get unexpected mistakes.
		 * Now, always process interrupt immediately, instead of processing interrupt in gs_send.
		 */

		t_thrd.int_cxt.ImmediateInterruptOK = true;
		CHECK_FOR_INTERRUPTS();

		/* send msg through libcomm interface if connection is logic */
		if(handle->is_logic_conn)
		{
#ifdef USE_SSL
			if (handle->pg_conn != NULL && handle->pg_conn->ssl != NULL)
			{
				sent = pgfdw_pqsecure_write(handle->pg_conn, ptr, Min(len, (int)LIBCOMM_BUFFER_SIZE));
			}
			else
#endif
			{
				sent = gs_send(&(handle->gsock), ptr, Min(len, (int)LIBCOMM_BUFFER_SIZE), -1, TRUE);
			}
		}
		else
		{
#ifndef WIN32
#ifdef USE_SSL
			if (handle->pg_conn != NULL && handle->pg_conn->ssl != NULL) {
				sent = pgfdw_pqsecure_write(handle->pg_conn, ptr, len);
			} else
#endif
			{
				PGSTAT_INIT_TIME_RECORD();
				PGSTAT_START_TIME_RECORD();
				sent = send(handle->sock, ptr, len, 0);
				END_NET_SEND_INFO(sent);
			}
#endif
		}

		t_thrd.int_cxt.ImmediateInterruptOK = SaveImmediateInterruptOK;

		if (sent < 0)
		{
			/*
			 * Anything except EAGAIN/EWOULDBLOCK/EINTR is trouble. If it's
			 * EPIPE or ECONNRESET, assume we've lost the backend connection
			 * permanently.
			 */
			switch (errno)
			{
#ifdef EAGAIN
				case EAGAIN:
					continue;
#endif
#if defined(EWOULDBLOCK) && (!defined(EAGAIN) || (EWOULDBLOCK != EAGAIN))
				case EWOULDBLOCK:
					continue;
#endif
				case EINTR:
					continue;

				case EPIPE:
#ifdef ECONNRESET
				case ECONNRESET:
#endif
					handle->state = DN_CONNECTION_STATE_ERROR_FATAL;
					add_error_message(handle, "%s", "could not send data to server");

					/*
					 * We used to close the socket here, but that's a bad idea
					 * since there might be unread data waiting (typically, a
					 * NOTICE message from the backend telling us it's
					 * committing hara-kiri...).  Leave the socket open until
					 * pqReadData finds no more data can be read.  But abandon
					 * attempt to send data.
					 */
					handle->outEnd = 0;
					return -1;

				default:
					handle->state = DN_CONNECTION_STATE_ERROR_FATAL;
					add_error_message(handle, "%s", "could not send data to server");
					/* We don't assume it's a fatal error... */
					handle->outEnd = 0;
					return -1;
			}
		}
		else
		{
			ptr += sent;
			len -= sent;
			handle->outEnd -= sent;
		}

	}

	handle->outEnd = 0;
	return 0;
}

int find_name_by_params(Oid *param_types, char	**paramTypes, short num_params)
{
	int paramTypeLen = 0;
	int cnt_params;
	Oid typeoid;

	for (cnt_params = 0; cnt_params < num_params; cnt_params++) {
		/* Parameters with no types are simply ignored */
		if (OidIsValid(param_types[cnt_params])) {
			typeoid = param_types[cnt_params];
		} else {
			typeoid = INT4OID;
		}

		paramTypes[cnt_params] = format_type_with_nspname(typeoid);
		paramTypeLen += strlen(paramTypes[cnt_params]) + 1;
	}

	return paramTypeLen;
}

void add_params_name_to_send(PGXCNodeHandle *handle, char	**paramTypes, short num_params)
{
	int	cnt_params;
	errno_t	ss_rc = 0;
	/*
	 * instead of parameter ids we should send parameter names (qualified by
	 * schema name if required). The OIDs of types can be different on
	 * Datanodes.
	 */
	for (cnt_params = 0; cnt_params < num_params; cnt_params++) {
		ss_rc = memcpy_s(handle->outBuffer + handle->outEnd, handle->outSize - handle->outEnd, paramTypes[cnt_params],
					strlen(paramTypes[cnt_params]) + 1);
		securec_check(ss_rc,"\0","\0");
		handle->outEnd += strlen(paramTypes[cnt_params]) + 1;
		pfree_ext(paramTypes[cnt_params]);
	}

	return;
}

/*
 * Send PARSE message with specified statement down to the Datanode
 */
int
pgxc_node_send_parse(PGXCNodeHandle * handle, const char* statement,
						const char *query, short num_params, Oid *param_types)
{
#ifndef ENABLE_MULTIPLE_NODES
	Assert(false);
	DISTRIBUTED_FEATURE_NOT_SUPPORTED();
	return 0;
#else

	/* statement name size (allow NULL) */
	int			stmtLen = statement ? (strlen(statement) + 1) : 1;
	/* size of query string */
	int			strLen = strlen(query) + 1;
	char 		**paramTypes = (char **)palloc(sizeof(char *) * num_params);
	/* total size of parameter type names */
	int 		paramTypeLen;
	/* message length */
	int			msgLen;
	int			cnt_params;

#define old_outEnd handle->outEnd

    if(ENABLE_CN_GPC) {
        GPC_LOG("send parse", 0, statement);
    }

	/* if there are parameters, param_types should exist */
	Assert(num_params <= 0 || param_types);
	/* 2 bytes for number of parameters, preceding the type names */
	paramTypeLen = 2;
	/* find names of the types of parameters */
	for (cnt_params = 0; cnt_params < num_params; cnt_params++)
	{
		Oid typeoid;

		/* Parameters with no types are simply ignored */
		if (OidIsValid(param_types[cnt_params]))
			typeoid = param_types[cnt_params];
		else
			typeoid = INT4OID;

		paramTypes[cnt_params] = format_type_be(typeoid);
		paramTypeLen += strlen(paramTypes[cnt_params]) + 1;
	}

	/* size + stmtLen + strlen + paramTypeLen */
	msgLen = 4 + stmtLen + strLen + paramTypeLen;

	/* msgType + msgLen */
	if (ensure_out_buffer_capacity(handle->outEnd + 1 + msgLen, handle) != 0)
	{
		add_error_message(handle, "%s", "out of memory");
		return EOF;
	}

	handle->outBuffer[handle->outEnd++] = 'P';
	/* size */
	msgLen = htonl(msgLen);
    errno_t rc;
    rc = memcpy_s(handle->outBuffer + handle->outEnd, 4, &msgLen, 4);
    securec_check(rc, "\0", "\0");
	handle->outEnd += 4;
	/* statement name */
	if (statement)
	{
        rc = memcpy_s(handle->outBuffer + handle->outEnd, stmtLen, statement, stmtLen);
        securec_check(rc, "\0", "\0");
		handle->outEnd += stmtLen;
	}
	else
		handle->outBuffer[handle->outEnd++] = '\0';
	/* query */
    rc = memcpy_s(handle->outBuffer + handle->outEnd, strLen, query, strLen);
    securec_check(rc, "\0", "\0");
	handle->outEnd += strLen;
	/* parameter types */
	Assert(sizeof(num_params) == 2);
	*((short *)(handle->outBuffer + handle->outEnd)) = htons(num_params);
	handle->outEnd += sizeof(num_params);
	/*
	 * instead of parameter ids we should send parameter names (qualified by
	 * schema name if required). The OIDs of types can be different on
	 * Datanodes.
	 */
	for (cnt_params = 0; cnt_params < num_params; cnt_params++)
	{
        int len = strlen(paramTypes[cnt_params]) + 1;
        rc = memcpy_s(handle->outBuffer + handle->outEnd, len, paramTypes[cnt_params], len);
        securec_check(rc, "\0", "\0");
		handle->outEnd += strlen(paramTypes[cnt_params]) + 1;
		pfree(paramTypes[cnt_params]);
	}
	pfree(paramTypes);
	Assert(old_outEnd + ntohl(msgLen) + 1 == handle->outEnd);

 	return 0;

#endif
}

/*
 * Send BIND message down to the Datanode
 */
int
pgxc_node_send_bind(PGXCNodeHandle * handle, const char *portal,
					const char *statement, int paramlen, const char *params)
{
	int			pnameLen;
	int			stmtLen;
	int 		paramCodeLen;
	int 		paramValueLen;
	int 		paramOutLen;
	int			msgLen;
	errno_t	ss_rc = 0;

	/* Invalid connection state, return error */
	if (handle->state != DN_CONNECTION_STATE_IDLE)
		return EOF;

	/* portal name size (allow NULL) */
	pnameLen = portal ? (strlen(portal) + 1) : 1;
	/* statement name size (allow NULL) */
	stmtLen = statement ? (strlen(statement) + 1) : 1;
	/* size of parameter codes array (always empty for now) */
	paramCodeLen = 2;
	/* size of parameter values array, 2 if no params */
	paramValueLen = paramlen ? paramlen : 2;
	/* size of output parameter codes array (always empty for now) */
	paramOutLen = 2;
	/* size + pnameLen + stmtLen + parameters */
	msgLen = 4 + pnameLen + stmtLen + paramCodeLen + paramValueLen + paramOutLen;

	/* msgType + msgLen */
	ensure_out_buffer_capacity(1 + msgLen, handle);
	Assert(handle->outBuffer != NULL);
	handle->outBuffer[handle->outEnd++] = 'B';
	/* size */
	msgLen = htonl(msgLen);
	ss_rc = memcpy_s(handle->outBuffer + handle->outEnd, handle->outSize - handle->outEnd, &msgLen, 4);
	securec_check(ss_rc,"\0","\0");
	handle->outEnd += 4;
	/* portal name */
	if (portal != NULL)
	{
		ss_rc = memcpy_s(handle->outBuffer + handle->outEnd, handle->outSize - handle->outEnd, portal, pnameLen);
		securec_check(ss_rc,"\0","\0");
		handle->outEnd += pnameLen;
	}
	else
		handle->outBuffer[handle->outEnd++] = '\0';
	/* statement name */
	if (statement != NULL)
	{
		ss_rc = memcpy_s(handle->outBuffer + handle->outEnd, handle->outSize - handle->outEnd, statement, stmtLen);
		securec_check(ss_rc,"\0","\0");
		handle->outEnd += stmtLen;
	}
	else
		handle->outBuffer[handle->outEnd++] = '\0';
	/* parameter codes (none) */
	handle->outBuffer[handle->outEnd++] = 0;
	handle->outBuffer[handle->outEnd++] = 0;
	/* parameter values */
	if (paramlen != 0)
	{
		ss_rc = memcpy_s(handle->outBuffer + handle->outEnd, handle->outSize - handle->outEnd, params, paramlen);
		securec_check(ss_rc,"\0","\0");
		handle->outEnd += paramlen;
	}
	else
	{
		handle->outBuffer[handle->outEnd++] = 0;
		handle->outBuffer[handle->outEnd++] = 0;
	}
	/* output parameter codes (none) */
	handle->outBuffer[handle->outEnd++] = 0;
	handle->outBuffer[handle->outEnd++] = 0;

 	return 0;
}


/*
 * Send DESCRIBE message (portal or statement) down to the Datanode
 */
int
pgxc_node_send_describe(PGXCNodeHandle * handle, bool is_statement,
						const char *name)
{
	int			nameLen;
	int			msgLen;
	errno_t		ss_rc = 0;

	/* Invalid connection state, return error */
    if (handle->state != DN_CONNECTION_STATE_IDLE) {
        return EOF;
    }

	/* statement or portal name size (allow NULL) */
	nameLen = name ? (strlen(name) + 1) : 1;

	/* size + statement/portal + name */
	msgLen = 4 + 1 + nameLen;

	/* msgType + msgLen */
	ensure_out_buffer_capacity(1 + msgLen, handle);
	Assert(handle->outBuffer != NULL);
	handle->outBuffer[handle->outEnd++] = 'D';
	/* size */
	msgLen = htonl(msgLen);
	ss_rc = memcpy_s(handle->outBuffer + handle->outEnd, handle->outSize - handle->outEnd, &msgLen, 4);
	securec_check(ss_rc,"\0","\0");
	handle->outEnd += 4;
	/* statement/portal flag */
	handle->outBuffer[handle->outEnd++] = is_statement ? 'S' : 'P';
	/* object name */
    if (name != NULL) {
        ss_rc = memcpy_s(handle->outBuffer + handle->outEnd, handle->outSize - handle->outEnd, name, nameLen);
        securec_check(ss_rc,"\0","\0");
        handle->outEnd += nameLen;
    } else {
        handle->outBuffer[handle->outEnd++] = '\0';
    }

 	return 0;
}


/*
 * Send CLOSE message (portal or statement) down to the Datanode
 */
int
pgxc_node_send_close(PGXCNodeHandle * handle, bool is_statement,
					 const char *name)
{
	/* statement or portal name size (allow NULL) */
	int			nameLen = name ? (strlen(name) + 1) : 1;

	/* size + statement/portal + name */
	int			msgLen = 4 + 1 + nameLen;
	errno_t		ss_rc = 0;

	/* msgType + msgLen */
	ensure_out_buffer_capacity(1 + msgLen, handle);
	Assert(handle->outBuffer != NULL);
	handle->outBuffer[handle->outEnd++] = 'C';
	/* size */
	msgLen = htonl(msgLen);
	ss_rc = memcpy_s(handle->outBuffer + handle->outEnd, handle->outSize - handle->outEnd, &msgLen, 4);
	securec_check(ss_rc,"\0","\0");
	handle->outEnd += 4;
	/* statement/portal flag */
	handle->outBuffer[handle->outEnd++] = is_statement ? 'S' : 'P';
	/* object name */
	if (name != NULL)
	{
		ss_rc = memcpy_s(handle->outBuffer + handle->outEnd, handle->outSize - handle->outEnd, name, nameLen);
		securec_check(ss_rc,"\0","\0");
		handle->outEnd += nameLen;
	}
	else
		handle->outBuffer[handle->outEnd++] = '\0';

	handle->state = DN_CONNECTION_STATE_QUERY;

 	return 0;
}

/*
 * Send EXECUTE message down to the Datanode
 */
int
pgxc_node_send_execute(PGXCNodeHandle * handle, const char *portal, int fetch)
{
#ifndef ENABLE_MULTIPLE_NODES
	Assert(false);
	DISTRIBUTED_FEATURE_NOT_SUPPORTED();
	return 0;
#else

	/* portal name size (allow NULL) */
	int			pnameLen = portal ? (strlen(portal) + 1) : 1;

	/* size + pnameLen + fetchLen */
	int			msgLen = 4 + pnameLen + 4;

	/* msgType + msgLen */
	if (ensure_out_buffer_capacity(handle->outEnd + 1 + msgLen, handle) != 0)
	{
		add_error_message(handle, "%s", "out of memory");
		return EOF;
	}

	handle->outBuffer[handle->outEnd++] = 'E';
	/* size */
	msgLen = htonl(msgLen);
    errno_t rc;
    rc = memcpy_s(handle->outBuffer + handle->outEnd, 4, &msgLen, 4);
    securec_check(rc, "\0", "\0");
	handle->outEnd += 4;
	/* portal name */
	if (portal)
	{
        rc = memcpy_s(handle->outBuffer + handle->outEnd, pnameLen, portal, pnameLen);
        securec_check(rc, "\0", "\0");
		handle->outEnd += pnameLen;
	}
	else
		handle->outBuffer[handle->outEnd++] = '\0';

	/* fetch */
	fetch = htonl(fetch);
    rc = memcpy_s(handle->outBuffer + handle->outEnd, 4, &fetch, 4);
    securec_check(rc, "\0", "\0");
	handle->outEnd += 4;

	handle->state = DN_CONNECTION_STATE_QUERY;

	return 0;

#endif
}

/*
 * Send schema name down to the PGXC node
 */
int
pgxc_node_send_pushschema(PGXCNodeHandle *handle, bool *isPush, bool *isCreateSchemaPush)
{
	*isPush = false;
	bool inProcedure = false;
    *isCreateSchemaPush = false;

	/* Invalid connection state, return error */
	if (handle->state != DN_CONNECTION_STATE_IDLE)
		return EOF;

	if (u_sess->catalog_cxt.overrideStack)
	{
		inProcedure = ((OverrideStackEntry *) linitial(u_sess->catalog_cxt.overrideStack))->inProcedure;
	}

	if (IS_PGXC_COORDINATOR && inProcedure && u_sess->catalog_cxt.overrideStack)
	{
		int strLenPush = 0;
		int msgLenPush = 0;
		errno_t		ss_rc = 0;
		HeapTuple tuple;
		Form_pg_namespace nsptup;
		Oid namespaceOid = ((OverrideStackEntry *)linitial(u_sess->catalog_cxt.overrideStack))->creationNamespace;
		tuple = SearchSysCache1(NAMESPACEOID, ObjectIdGetDatum(namespaceOid));
		if (!HeapTupleIsValid(tuple))
            ereport(ERROR,
				(errmsg("cache lookup failed for namespace %u", namespaceOid)));

		nsptup = (Form_pg_namespace) GETSTRUCT(tuple);
		strLenPush += strlen(NameStr(nsptup->nspname)) + 1;
		/* size + push/pop + schema name */
		msgLenPush = 4 + 1 + strLenPush;

		ensure_out_buffer_capacity(1 + msgLenPush, handle);
		Assert(handle->outBuffer != NULL);
		*isPush = true;

		handle->outBuffer[handle->outEnd++] = 'I';
		msgLenPush = htonl(msgLenPush);
		ss_rc = memcpy_s(handle->outBuffer + handle->outEnd, handle->outSize - handle->outEnd, &msgLenPush, 4);
		securec_check(ss_rc,"\0","\0");
		handle->outEnd += 4;

		handle->outBuffer[handle->outEnd++] = 'P'; // Push

		ss_rc = memcpy_s(handle->outBuffer + handle->outEnd, handle->outSize - handle->outEnd, NameStr(nsptup->nspname), strLenPush);
		securec_check(ss_rc,"\0","\0");
		handle->outEnd += strLenPush;
		ereport(DEBUG2, (errmodule(MOD_SCHEMA), errmsg("pgxc_node_send_pushschema:%s", NameStr(nsptup->nspname))));
		ReleaseSysCache(tuple);
	}

	return 0;
}

/*
 * Pop schema name from PGXC node
 */
int
pgxc_node_send_popschema(PGXCNodeHandle *handle, bool isPush)
{
	int strLenPop = 1;
	/* msgType + push/pop + msgLen */
	int msgLenPop = 4 + 1 + strLenPop;
	errno_t		ss_rc = 0;
	ensure_out_buffer_capacity(1 + msgLenPop, handle);
	Assert(handle->outBuffer != NULL);

	handle->outBuffer[handle->outEnd++] = 'I';
	msgLenPop = htonl(msgLenPop);
	ss_rc = memcpy_s(handle->outBuffer + handle->outEnd, handle->outSize - handle->outEnd, &msgLenPop, 4);
	securec_check(ss_rc,"\0","\0");
	handle->outEnd += 4;

	handle->outBuffer[handle->outEnd++] = isPush ? 'p' : 'F'; // pop
	ss_rc = memcpy_s(handle->outBuffer + handle->outEnd, handle->outSize - handle->outEnd, "", strLenPop);
	securec_check(ss_rc,"\0","\0");
	handle->outEnd += strLenPop;

	return 0;
}

/*
 * Send SYNC message down to the Datanode
 */
int
pgxc_node_send_sync(PGXCNodeHandle * handle)
{
#ifndef ENABLE_MULTIPLE_NODES
	Assert(false);
	DISTRIBUTED_FEATURE_NOT_SUPPORTED();
	return 0;
#else
	/* size */
	int			msgLen = 4;

	/* msgType + msgLen */
	if (ensure_out_buffer_capacity(handle->outEnd + 1 + msgLen, handle) != 0)
	{
		add_error_message(handle, "%s", "out of memory");
		return EOF;
	}

	handle->outBuffer[handle->outEnd++] = 'S';
	/* size */
	msgLen = htonl(msgLen);
    errno_t rc;
    rc = memcpy_s(handle->outBuffer + handle->outEnd, 4, &msgLen, 4);
    securec_check(rc, "\0", "\0");
	handle->outEnd += 4;

	return pgxc_node_flush(handle);
#endif
}


/*
 * Send the GXID down to the Datanode
 */
int
pgxc_node_send_query_extended(PGXCNodeHandle *handle, const char *query,
							  const char *statement, const char *portal,
							  int num_params, Oid *param_types,
							  int paramlen, const char *params,
							  bool send_describe, int fetch_size)
{
	/* NULL query indicates already prepared statement */
	if (query != NULL)
		if (pgxc_node_send_parse(handle, statement, query, num_params, param_types))
			return EOF;
	if (pgxc_node_send_bind(handle, portal, statement, paramlen, params))
		return EOF;
	if (send_describe)
		if (pgxc_node_send_describe(handle, false, portal))
			return EOF;
	if (fetch_size >= 0)
		if (pgxc_node_send_execute(handle, portal, fetch_size))
			return EOF;
	if (pgxc_node_send_sync(handle))
		return EOF;

	return 0;
}

/*
 * Support prepare/execute with plan shipping, send plan and params to remote node.
 */
int
pgxc_node_send_plan_with_params(PGXCNodeHandle *handle, const char *query,
							  short num_params, Oid *param_types,
							  int paramlen, const char *params, int fetch_size)
{
	/* size of query string */
	int			strLen;
	char	  **paramTypes = (char **)palloc(sizeof(char *) * num_params);
	/* total size of parameter type names */
	int 		paramTypeLen;
	/* message length */
	int			msgLen;
	int 		paramCodeLen;
	int 		paramValueLen;
	int 		paramOutLen;
	char	   *tmpQuery = (char*)query;
	int nodeId = 1;
	errno_t	ss_rc = 0;

	/* Invalid connection state, return error */
	if (handle->state != DN_CONNECTION_STATE_IDLE)
		return EOF;

	/* if there are parameters, param_types should exist */
	Assert(num_params <= 0 || param_types);

	/* start from 0 */
	if (IS_PGXC_COORDINATOR)
		nodeId = PGXCNodeGetNodeId(handle->nodeoid, PGXC_NODE_DATANODE);
	else
		/* DWS DN sends plan to CN of the compute pool if run here. */
		nodeId = u_sess->pgxc_cxt.PGXCNodeId;

	strLen = strlen(query) + 1;

	/* 2 bytes for number of parameters, preceding the type names */
	paramTypeLen = 2;

	/* find names of the types of parameters */
	paramTypeLen += find_name_by_params(param_types, paramTypes, num_params);

	/* size of parameter codes array (always empty for now) */
	paramCodeLen = 2;

	/* size of parameter values array, 2 if no params */
	paramValueLen = paramlen ? paramlen : 2;

	/* size of output parameter codes array (always empty for now) */
	paramOutLen = 2;

	/* size + nodeid + stmtLen + strlen + paramTypeLen + parameters + fetchlen + cursorOption*/
	msgLen = 4 + 4 + strLen + paramTypeLen + paramCodeLen + paramValueLen + paramOutLen + 4;

	/* msgType + msgLen */
	ensure_out_buffer_capacity(1 + msgLen, handle);
	if(('Z' == *tmpQuery))
	{
		tmpQuery++;
		strLen--;
		msgLen--;
	}

	/* 'Y' message means execute plan with params */
	Assert(handle->outBuffer != NULL);
	handle->outBuffer[handle->outEnd++] = 'Y';

	/* size */
	msgLen = htonl(msgLen);
	ss_rc = memcpy_s(handle->outBuffer + handle->outEnd, handle->outSize - handle->outEnd, &msgLen, 4);
	securec_check(ss_rc,"\0","\0");
	handle->outEnd += 4;

	/* the remote node id to send */
	nodeId = htonl(nodeId);
	ss_rc = memcpy_s(handle->outBuffer + handle->outEnd, handle->outSize - handle->outEnd, &nodeId, 4);
	securec_check(ss_rc, "\0", "\0");
	handle->outEnd += 4;

	/* query */
	ss_rc = memcpy_s(handle->outBuffer + handle->outEnd, handle->outSize - handle->outEnd, tmpQuery, strLen);
	securec_check(ss_rc,"\0","\0");
	handle->outEnd += strLen;
	
	/* parameter num */
	*((short *)(handle->outBuffer + handle->outEnd)) = htons(num_params);
	handle->outEnd += sizeof(num_params);

	/*
	 * instead of parameter ids we should send parameter names (qualified by
	 * schema name if required). The OIDs of types can be different on
	 * Datanodes.
	 */
	add_params_name_to_send(handle, paramTypes, num_params);
	pfree_ext(paramTypes);

	/* parameter codes (none) */
	handle->outBuffer[handle->outEnd++] = 0;
	handle->outBuffer[handle->outEnd++] = 0;

	/* parameter values */
	if (paramlen)
	{
		ss_rc = memcpy_s(handle->outBuffer + handle->outEnd, handle->outSize - handle->outEnd, params, paramlen);
		securec_check(ss_rc,"\0","\0");
		handle->outEnd += paramlen;
	}
	else
	{
		handle->outBuffer[handle->outEnd++] = 0;
		handle->outBuffer[handle->outEnd++] = 0;
	}

	/* output parameter codes (none) */
	handle->outBuffer[handle->outEnd++] = 0;
	handle->outBuffer[handle->outEnd++] = 0;

	/* fetch */
	fetch_size = htonl(fetch_size);
	ss_rc = memcpy_s(handle->outBuffer + handle->outEnd, handle->outSize - handle->outEnd, &fetch_size, 4);
	securec_check(ss_rc,"\0","\0");
	handle->outEnd += 4;

	handle->state = DN_CONNECTION_STATE_QUERY;

	NameData nodename = {{0}};
	if (IS_PGXC_COORDINATOR)
		elog(DEBUG3, "DTM: Sending Query with params:%s to--> %s \n",
			query, get_pgxc_nodename(handle->nodeoid, &nodename));

	return pgxc_node_flush(handle);
}

/*
 * This method won't return until connection buffer is empty or error occurs
 * To ensure all data are on the wire before waiting for response
 */
int
pgxc_node_flush(PGXCNodeHandle *handle)
{
#ifndef ENABLE_MULTIPLE_NODES
	Assert(false);
	DISTRIBUTED_FEATURE_NOT_SUPPORTED();
	return 0;
#else
	while (handle->outEnd)
	{
		if (send_some(handle, handle->outEnd) < 0)
		{
			add_error_message(handle, "%s", "failed to send data to datanode");
			return EOF;
		}
	}
	return 0;
#endif
}

/*
 * This method won't return until network buffer is empty or error occurs
 * To ensure all data in network buffers is read and wasted.
 * or if no data avaliable util PoolcancelTimeout, it will then return false
 */
bool
pgxc_node_flush_read(PGXCNodeHandle *handle)
{
	bool	is_ready = false;
	int	read_result;

	if (handle == NULL)
		return true;

	int64 waitingTime = (int64) u_sess->attr.attr_network.PoolerCancelTimeout * 1000; //ms
	size_t inEndtmp = handle->inEnd;
	while(true)
	{
		if(handle->is_logic_conn)
			read_result = pgxc_node_read_data_from_logic_conn(handle, false);
		else
			read_result = pgxc_node_read_data(handle, false);

		if (read_result < 0)
			break;

		is_ready = is_data_node_ready(handle);
		if (is_ready == true)
			break;

		if (u_sess->attr.attr_network.PoolerCancelTimeout && (inEndtmp == handle->inEnd || handle->inEnd == 0))
		{
			if (waitingTime <= 0)
			{
				ereport(WARNING,
					(errmsg("Timeout happened when read and waste the handle %s data stream after %ds.", handle->remoteNodeName, u_sess->attr.attr_network.PoolerCancelTimeout)));

				return false;
			}

			pg_usleep(100000); //100ms
			waitingTime = waitingTime - 100;
			inEndtmp = handle->inEnd;
		}
	}
	return true;
}

/*
 * Send the queryId(for debug) down to the PGXC node
 */
int
pgxc_node_send_queryid(PGXCNodeHandle *handle, uint64 queryid)
{
	Assert(false);
	DISTRIBUTED_FEATURE_NOT_SUPPORTED();
	return 0;
}

/*
 * Send unique sql id to PGXC node
 *
 * CAUTION: becase same user has diff user oids on CN/DN,
 * so also send unique sql id + user id to DN node.
 */
int
pgxc_node_send_unique_sql_id(PGXCNodeHandle *handle)
{
	Assert(false);
	DISTRIBUTED_FEATURE_NOT_SUPPORTED();
	return 0;
}

/*
 * Send cp config down to DWS DN
 */
int
pgxc_node_send_userpl(PGXCNodeHandle * handle, int64 userpl)
{
	uint32	n32;
	int			msgLen;
	errno_t		ss_rc = 0;

	/* Invalid connection state, return error */
	if (handle->state != DN_CONNECTION_STATE_IDLE)
		return EOF;

	/* size + strlen */
	msgLen = 4 + 1 + 8;

	/* msgType + msgLen */
	ensure_out_buffer_capacity(1 + msgLen, handle);
	Assert(handle->outBuffer != NULL);

	handle->outBuffer[handle->outEnd++] = 'A';
	
	msgLen = htonl(msgLen);
	ss_rc = memcpy_s(handle->outBuffer + handle->outEnd, handle->outSize - handle->outEnd, &msgLen, 4);
	securec_check(ss_rc,"\0","\0");
	handle->outEnd += 4;

	/* assemble sub command in 'A' */
	handle->outBuffer[handle->outEnd++] = 'P';

	n32 = (uint32) ((uint64)userpl >> 32);
	n32 = htonl(n32);
	ss_rc = memcpy_s(handle->outBuffer + handle->outEnd, handle->outSize - handle->outEnd, &n32, 4);
	securec_check(ss_rc,"\0","\0");
	handle->outEnd += 4;

	/* Now the low order half */
	n32 = (uint32) userpl;
	n32 = htonl(n32);
	ss_rc = memcpy_s(handle->outBuffer + handle->outEnd, handle->outSize - handle->outEnd, &n32, 4);
	securec_check(ss_rc,"\0","\0");
	handle->outEnd += 4;

	return 0;
}

int
pgxc_node_send_threadid(PGXCNodeHandle *handle, uint32 threadid)
{
	int			msglen = 9;
	errno_t		ss_rc = 0;
	bool		track_resource = u_sess->exec_cxt.need_track_resource;
	int         procId  = 0;

	/* Invalid connection state, return error */
	if (handle->state != DN_CONNECTION_STATE_IDLE)
		return EOF;

	/* msgType + msgLen */
	ensure_out_buffer_capacity(1 + msglen, handle);
	Assert(handle->outBuffer != NULL);
	handle->outBuffer[handle->outEnd++] = 'e';
	msglen = htonl(msglen);
	ss_rc = memcpy_s(handle->outBuffer + handle->outEnd, handle->outSize - handle->outEnd, &msglen, 4);
	securec_check(ss_rc,"\0","\0");
	handle->outEnd += 4;
	procId = htonl(threadid);
	ss_rc = memcpy_s(handle->outBuffer + handle->outEnd, handle->outSize - handle->outEnd, &procId, sizeof(uint32));
	securec_check(ss_rc,"\0","\0");
	handle->outEnd += sizeof(uint32);

	ss_rc = memcpy_s(handle->outBuffer + handle->outEnd, handle->outSize - handle->outEnd, &track_resource, 1);
	securec_check(ss_rc,"\0","\0");
	handle->outEnd += sizeof(bool);	
	return 0;
}

/*
 * send gtm mode when begin transaction.
 */
void pgxc_node_send_gtm_mode(PGXCNodeHandle *handle)
{
	int			msgLen;
	errno_t 	ss_rc = 0;

	/* meslen + gtm_mode */
	msgLen = sizeof(int) + sizeof(bool);

	/* msgType + msgLen */
	ensure_out_buffer_capacity(1 + msgLen, handle);
	Assert(handle->outBuffer != NULL);
	handle->outBuffer[handle->outEnd++] = 'j';
	msgLen = htonl(msgLen);
	ss_rc = memcpy_s(handle->outBuffer + handle->outEnd, handle->outSize - handle->outEnd, &msgLen, sizeof(int));
	securec_check(ss_rc, "\0", "\0");
	handle->outEnd += sizeof(int);
	ss_rc = memcpy_s(handle->outBuffer + handle->outEnd, handle->outSize - handle->outEnd,
					&g_instance.attr.attr_storage.enable_gtm_free, sizeof(bool));
	securec_check(ss_rc,"\0","\0");
	handle->outEnd += sizeof(bool);

	return;
}

/*
 * Send specified statement down to the PGXC node
 */
int
pgxc_node_send_query(PGXCNodeHandle * handle, const char *query, bool isPush, bool isCreateSchemaPush,
    bool trigger_ship, bool check_gtm_mode, const char* compressedPlan, int cLen)
{
#ifndef ENABLE_MULTIPLE_NODES
	Assert(false);
	DISTRIBUTED_FEATURE_NOT_SUPPORTED();
	return 0;
#else
	int			strLen;
	int			msgLen;

	/* Invalid connection state, return error */
	if (handle->state != DN_CONNECTION_STATE_IDLE)
		return EOF;

	strLen = strlen(query) + 1;
	/* size + strlen */
	msgLen = 4 + strLen;

	/* msgType + msgLen */
	if (ensure_out_buffer_capacity(handle->outEnd + 1 + msgLen, handle) != 0)
	{
		add_error_message(handle, "%s", "out of memory");
		return EOF;
	}

	handle->outBuffer[handle->outEnd++] = 'Q';
	msgLen = htonl(msgLen);
    errno_t rc = 0;
    rc = memcpy_s(handle->outBuffer + handle->outEnd, 4, &msgLen, 4);
    securec_check(rc, "\0", "\0");
    handle->outEnd += 4;
    rc = memcpy_s(handle->outBuffer + handle->outEnd, strLen, query, strLen);
    securec_check(rc, "\0", "\0");
	handle->outEnd += strLen;

	handle->state = DN_CONNECTION_STATE_QUERY;

 	return pgxc_node_flush(handle);
#endif
}


/*
 * Send the GXID down to the PGXC node
 */
int
pgxc_node_send_gxid(PGXCNodeHandle *handle, GlobalTransactionId gxid, bool isforcheck)
{

	int			msglen = 4 + sizeof(TransactionId) + sizeof(bool);

	/* Invalid connection state, return error */
	if (handle->state != DN_CONNECTION_STATE_IDLE)
		return EOF;

	errno_t ss_rc = 0;
	/* msgType + msgLen */
	ensure_out_buffer_capacity(1 + msglen, handle);
	Assert(handle->outBuffer != NULL);
	handle->outBuffer[handle->outEnd++] = 'g';
	msglen = htonl(msglen);
	ss_rc = memcpy_s(handle->outBuffer + handle->outEnd, handle->outSize - handle->outEnd, &msglen, 4);
	securec_check(ss_rc,"\0","\0");
	handle->outEnd += 4;
	ss_rc = memcpy_s(handle->outBuffer + handle->outEnd, handle->outSize - handle->outEnd, &gxid, sizeof(TransactionId));
	securec_check(ss_rc,"\0","\0");
	handle->outEnd += sizeof(TransactionId);

	/* add the tag for identifying special xid for check */
	ss_rc = memcpy_s(handle->outBuffer + handle->outEnd, handle->outSize - handle->outEnd, &isforcheck, sizeof(bool));
	securec_check(ss_rc,"\0","\0");
	handle->outEnd += sizeof(bool);

	return 0;
}

/*
 * Send the commit csn down to the PGXC node
 */
int
pgxc_node_send_commit_csn(PGXCNodeHandle * handle, uint64 commit_csn)
{
    int			msglen = 4 + sizeof(uint64); // message length and csn length

	/* Invalid connection state, return error */
	if (handle->state != DN_CONNECTION_STATE_IDLE)
		return EOF;

	errno_t ss_rc = 0;
	/* msgType + msgLen */
	ensure_out_buffer_capacity(1 + msglen, handle);
	Assert(handle->outBuffer != NULL);
	handle->outBuffer[handle->outEnd++] = 'N';
	msglen = htonl(msglen);
	ss_rc = memcpy_s(handle->outBuffer + handle->outEnd, handle->outSize - handle->outEnd, &msglen, 4);
	securec_check(ss_rc,"\0","\0");
	handle->outEnd += 4;
	ss_rc = memcpy_s(handle->outBuffer + handle->outEnd, handle->outSize - handle->outEnd, &commit_csn, sizeof(uint64));
	securec_check(ss_rc,"\0","\0");
	handle->outEnd += sizeof(uint64);

	return 0;
}

/*
 * Send the commit csn down to the PGXC node
 */
int
pgxc_node_notify_commit(PGXCNodeHandle * handle)
{
	int			msglen = 4 + sizeof(uint64); // message length and csn length

	/* Invalid connection state, return error */
	if (handle->state != DN_CONNECTION_STATE_IDLE)
		return EOF;

	errno_t ss_rc = 0;
	/* msgType + msgLen */
	ensure_out_buffer_capacity(1 + msglen, handle);
	Assert(handle->outBuffer != NULL);
	handle->outBuffer[handle->outEnd++] = 'n';
	/* size */
	msglen = htonl(msglen);
	ss_rc = memcpy_s(handle->outBuffer + handle->outEnd, handle->outSize - handle->outEnd, &msglen, 4);
	securec_check(ss_rc,"\0","\0");
	handle->outEnd += 4;
	CommitSeqNo currentNextCommitSeqNo = pg_atomic_read_u64(&t_thrd.xact_cxt.ShmemVariableCache->nextCommitSeqNo);
	ss_rc = memcpy_s(handle->outBuffer + handle->outEnd, handle->outSize - handle->outEnd,
					 &currentNextCommitSeqNo, sizeof(uint64));
	securec_check(ss_rc,"\0","\0");
	handle->outEnd += sizeof(uint64);

	/* Wait for response */
	handle->state = DN_CONNECTION_STATE_QUERY;
	return pgxc_node_flush(handle);
}

/*
 * Send the Command ID down to the PGXC node
 */
int
pgxc_node_send_cmd_id(PGXCNodeHandle *handle, CommandId cid)
{
#ifndef ENABLE_MULTIPLE_NODES
	Assert(false);
	DISTRIBUTED_FEATURE_NOT_SUPPORTED();
	return 0;
#else
	int			msglen = CMD_ID_MSG_LEN;
	int			i32;

	/* No need to send command ID if its sending flag is not enabled */
	if (!IsSendCommandId())
		return 0;

	/* Invalid connection state, return error */
	if (handle->state != DN_CONNECTION_STATE_IDLE)
		return EOF;

	/* msgType + msgLen */
	if (ensure_out_buffer_capacity(handle->outEnd + 1 + msglen, handle) != 0)
	{
		add_error_message(handle, "%s", "out of memory");
		return EOF;
	}

	handle->outBuffer[handle->outEnd++] = 'M';
	msglen = htonl(msglen);
	errno_t ss_rc = memcpy_s(handle->outBuffer + handle->outEnd, handle->outSize - handle->outEnd, &msglen, 4);
	securec_check(ss_rc, "\0", "\0");
	handle->outEnd += 4;
	i32 = htonl(cid);
	ss_rc = memcpy_s(handle->outBuffer + handle->outEnd, handle->outSize - handle->outEnd, &i32, 4);
	securec_check(ss_rc, "\0", "\0");
	handle->outEnd += 4;

	return 0;
#endif
}

/*
 * Send the WLM Control Group down to the PGXC node
 */
int
pgxc_node_send_wlm_cgroup(PGXCNodeHandle *handle)
{
	Assert(false);
	DISTRIBUTED_FEATURE_NOT_SUPPORTED();
	return 0;

}

/* CN sends request to DN for collecting job info*/
int
pgxc_node_dywlm_send_params_for_jobs(PGXCNodeHandle *handle, int tag, const char *keystr)
{
	Assert(false);
	DISTRIBUTED_FEATURE_NOT_SUPPORTED();
	return 0;
}

int
pgxc_node_pgstat_send(PGXCNodeHandle *handle, char tag)
{
	Assert(false);
	DISTRIBUTED_FEATURE_NOT_SUPPORTED();
	return 0;
}

int
pgxc_node_dywlm_send_params(PGXCNodeHandle *handle, const void *info)
{
	Assert(false);
	DISTRIBUTED_FEATURE_NOT_SUPPORTED();
	return 0;
}

/*
 * Send the WLM info to the PGXC node
 */
int
pgxc_node_dywlm_send_record(PGXCNodeHandle *handle, int tag, const char *keystr)
{
	Assert(false);
	DISTRIBUTED_FEATURE_NOT_SUPPORTED();
	return 0;
}

/*
 * Send the WLM info to the PGXC node
 */
int
pgxc_node_send_pgfdw(PGXCNodeHandle *handle, int tag, const char *keystr, int len)
{
	Assert(keystr != NULL);

	int         strLen;
	int         msgLen;
	int         flag;

	/* Invalid connection state, return error */
	if (handle->state != DN_CONNECTION_STATE_IDLE)
		return EOF;

	/* msgType + msgLen */
	strLen = len;

	/* size + strlen */
	msgLen = 4 + strLen + 4;

	errno_t ss_rc = 0;
	/* msgType + msgLen */
	ensure_out_buffer_capacity(1 + msgLen, handle);
	Assert(handle->outBuffer != NULL);
	handle->outBuffer[handle->outEnd++] = 'L';

	msgLen = htonl(msgLen);
	ss_rc = memcpy_s(handle->outBuffer + handle->outEnd, handle->outSize - handle->outEnd, &msgLen, 4);
	securec_check(ss_rc,"\0","\0");
	handle->outEnd += 4;

	flag = htonl(tag);
	ss_rc = memcpy_s(handle->outBuffer + handle->outEnd, handle->outSize - handle->outEnd, &flag, 4);
	securec_check(ss_rc,"\0","\0");
	handle->outEnd += 4;

	ss_rc = memcpy_s(handle->outBuffer + handle->outEnd, handle->outSize - handle->outEnd, keystr, strLen);
	securec_check(ss_rc,"\0","\0");
	handle->outEnd += strLen;

	handle->state = DN_CONNECTION_STATE_QUERY;

	return pgxc_node_flush(handle);
}

/*
 * Send the snapshot down to the PGXC node
 */
int
pgxc_node_send_snapshot(PGXCNodeHandle *handle, Snapshot snapshot, int max_push_sqls)
{
#ifndef ENABLE_MULTIPLE_NODES
	Assert(false);
	DISTRIBUTED_FEATURE_NOT_SUPPORTED();
	return 0;
#else
	int			msglen;
	int			nval;
	int			i;

	/* Invalid connection state, return error */
	if (handle->state != DN_CONNECTION_STATE_IDLE)
		return EOF;

	/* calculate message length */
	msglen = 20;
	if (snapshot->xcnt > 0)
		msglen += snapshot->xcnt * 4;

	/* msgType + msgLen */
	if (ensure_out_buffer_capacity(handle->outEnd + 1 + msglen, handle) != 0)
	{
		add_error_message(handle, "%s", "out of memory");
		return EOF;
	}

	handle->outBuffer[handle->outEnd++] = 's';
	msglen = htonl(msglen);
    errno_t rc;
    rc = memcpy_s(handle->outBuffer + handle->outEnd, 4, &msglen, 4);
    securec_check(rc, "\0", "\0");
    handle->outEnd += 4;

    nval = htonl(snapshot->xmin);
    rc = memcpy_s(handle->outBuffer + handle->outEnd, 4, &nval, 4);
    securec_check(rc, "\0", "\0");
    handle->outEnd += 4;

    nval = htonl(snapshot->xmax);
    rc = memcpy_s(handle->outBuffer + handle->outEnd, 4, &nval, 4);
    securec_check(rc, "\0", "\0");
    handle->outEnd += 4;

    nval = htonl(RecentGlobalXmin);
    rc = memcpy_s(handle->outBuffer + handle->outEnd, 4, &nval, 4);
    securec_check(rc, "\0", "\0");
    handle->outEnd += 4;

    nval = htonl(snapshot->xcnt);
    rc = memcpy_s(handle->outBuffer + handle->outEnd, 4, &nval, 4);
    securec_check(rc, "\0", "\0");
    handle->outEnd += 4;

	for (i = 0; i < snapshot->xcnt; i++)
	{
		nval = htonl(snapshot->xip[i]);
        rc = memcpy_s(handle->outBuffer + handle->outEnd, 4, &nval, 4);
        securec_check(rc, "\0", "\0");
		handle->outEnd += 4;
	}

	return 0;
#endif
}

/*
 * pgxc_node_send_timestamp
 *
 * Send the timestamp down to the PGXC node, The start time of the
 * statement must be delivered before the gtm time.
 *
 * gtmstart_timestamp: gtm timestamp
 * stmtsys_timestamp:  stmt sys timestamp
 */
int
pgxc_node_send_timestamp(PGXCNodeHandle *handle, 
									TimestampTz gtmstart_timestamp,
									TimestampTz stmtsys_timestamp)
{
#ifndef ENABLE_MULTIPLE_NODES
	Assert(false);
	DISTRIBUTED_FEATURE_NOT_SUPPORTED();
	return 0;
#else

	int		msglen = 12; /* 4 bytes for msglen and 8 bytes for timestamp (int64) */
	uint32	n32;
	int64	i = (int64) timestamp;

	/* Invalid connection state, return error */
	if (handle->state != DN_CONNECTION_STATE_IDLE)
		return EOF;

	/* msgType + msgLen */
	if (ensure_out_buffer_capacity(handle->outEnd + 1 + msglen, handle) != 0)
	{
		add_error_message(handle, "%s", "out of memory");
		return EOF;
	}
	handle->outBuffer[handle->outEnd++] = 't';
	msglen = htonl(msglen);
    errno_t rc;
    rc = memcpy_s(handle->outBuffer + handle->outEnd, 4, &msglen, 4);
    securec_check(rc, "\0", "\0");
	handle->outEnd += 4;

	/* High order half first */
#ifdef INT64_IS_BUSTED
	/* don't try a right shift of 32 on a 32-bit word */
	n32 = (i < 0) ? -1 : 0;
#else
	n32 = (uint32) (i >> 32);
#endif
    n32 = htonl(n32);
    rc = memcpy_s(handle->outBuffer + handle->outEnd, 4, &n32, 4);
    securec_check(rc, "\0", "\0");
    handle->outEnd += 4;

    /* Now the low order half */
    n32 = (uint32) i;
    n32 = htonl(n32);
    rc = memcpy_s(handle->outBuffer + handle->outEnd, 4, &n32, 4);
    securec_check(rc, "\0", "\0");
	handle->outEnd += 4;

	return 0;

#endif
}

/*
 * Add another message to the list of errors to be returned back to the client
 * at the convenient time
 */
void add_error_message(PGXCNodeHandle *handle, const char *message, ...)
{
    MemoryContext old;
    const int MSG_LEN = 1024;
    char msgBuf[MSG_LEN];
    va_list args;

    /* Format the message */
    va_start(args, message);
    int rc = vsnprintf_truncated_s(msgBuf, sizeof(msgBuf), message, args);
    securec_check_ss(rc,"\0","\0");
    va_end(args);
    
    msgBuf[sizeof(msgBuf) - 1] = '\0'; /* make real sure it's terminated */    
    elog(DEBUG5, "Connection error %s: %s", handle->remoteNodeName, msgBuf);
    handle->transaction_status = 'E';
    /*
     * hanle->error release depend on memory context destruct
     * if we reset memcxt, memory is free, but hanle->error is not set NULL immediately
     * in some abnormal scene, it will cause heap-use-after-free
     * so we mount error to topcontext, free and null at the same time
     */
    if (handle->error == NULL && SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_COMMUNICATION) != NULL) {
        old = MemoryContextSwitchTo(SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_COMMUNICATION));
        handle->error = pstrdup(message);
        MemoryContextSwitchTo(old);
    }
}

/*
 * for specified list return array of PGXCNodeHandles
 * acquire from pool if needed.
 * the lenth of returned array is the same as of nodelist
 * For Datanodes, Special case is empty or NIL nodeList, in this case return all the nodes.
 * The returned list should be pfree'd when no longer needed.
 * For Coordinator, do not get a connection if Coordinator list is NIL,
 * Coordinator fds is returned only if transaction uses a DDL
 */
PGXCNodeAllHandles *
get_handles(List *datanodelist, List *coordlist, bool is_coord_only_query, List *dummynodelist)
{
#ifndef ENABLE_MULTIPLE_NODES
	Assert(false);
	DISTRIBUTED_FEATURE_NOT_SUPPORTED();
	return NULL;
#else

	PGXCNodeAllHandles	*result = NULL;
	ListCell        *node_list_item = NULL;
	List			*dn_allocate = NIL;
	List			*co_allocate = NIL;
	PGXCNodeHandle		*node_handle = NULL;

	/* index of the result array */
	int			i = 0;

	result = (PGXCNodeAllHandles *) palloc(sizeof(PGXCNodeAllHandles));
	if (!result)
	{
		ereport(ERROR,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("out of memory")));
	}

	result->primary_handle = NULL;
	result->datanode_handles = NULL;
	result->coord_handles = NULL;
	result->co_conn_count = list_length(coordlist);
	result->dn_conn_count = list_length(datanodelist);

	/*
	 * Get Handles for Datanodes
	 * If node list is empty execute request on current nodes.
	 * It is also possible that the query has to be launched only on Coordinators.
	 */
	if (!is_coord_only_query)
	{
		if (list_length(datanodelist) == 0)
		{
			/*
			 * We do not have to zero the array - on success all items will be set
			 * to correct pointers, on error the array will be freed
			 */
			result->datanode_handles = (PGXCNodeHandle **)
									   palloc(NumDataNodes * sizeof(PGXCNodeHandle *));
			if (!result->datanode_handles)
			{
				ereport(ERROR,
						(errcode(ERRCODE_OUT_OF_MEMORY),
						 errmsg("out of memory")));
			}

			for (i = 0; i < NumDataNodes; i++)
			{
				node_handle = &dn_handles[i];
				result->datanode_handles[i] = node_handle;
				if (node_handle->sock == NO_SOCKET)
					dn_allocate = lappend_int(dn_allocate, i);
			}
		}
		else
		{
			/*
			 * We do not have to zero the array - on success all items will be set
			 * to correct pointers, on error the array will be freed
			 */

			result->datanode_handles = (PGXCNodeHandle **)
				palloc(list_length(datanodelist) * sizeof(PGXCNodeHandle *));
			if (!result->datanode_handles)
			{
				ereport(ERROR,
						(errcode(ERRCODE_OUT_OF_MEMORY),
						 errmsg("out of memory")));
			}

			i = 0;
			foreach(node_list_item, datanodelist)
			{
				int	node = lfirst_int(node_list_item);

				if (node < 0 || node >= NumDataNodes)
				{
					ereport(ERROR,
							(errcode(ERRCODE_OUT_OF_MEMORY),
							errmsg("Invalid Datanode number")));
				}

				node_handle = &dn_handles[node];
				result->datanode_handles[i++] = node_handle;
				if (node_handle->sock == NO_SOCKET)
					dn_allocate = lappend_int(dn_allocate, node);
			}
		}
	}

	/*
	 * Get Handles for Coordinators
	 * If node list is empty execute request on current nodes
	 * There are transactions where the Coordinator list is NULL Ex:COPY
	 */

	if (coordlist)
	{
		if (list_length(coordlist) == 0)
		{
			/*
			 * We do not have to zero the array - on success all items will be set
			 * to correct pointers, on error the array will be freed
			 */
			result->coord_handles = (PGXCNodeHandle **)palloc(NumCoords * sizeof(PGXCNodeHandle *));
			if (!result->coord_handles)
			{
				ereport(ERROR,
						(errcode(ERRCODE_OUT_OF_MEMORY),
						 errmsg("out of memory")));
			}

			for (i = 0; i < NumCoords; i++)
			{
				node_handle = &co_handles[i];
				result->coord_handles[i] = node_handle;
				if (node_handle->sock == NO_SOCKET)
					co_allocate = lappend_int(co_allocate, i);
			}
		}
		else
		{
			/*
			 * We do not have to zero the array - on success all items will be set
			 * to correct pointers, on error the array will be freed
			 */
			result->coord_handles = (PGXCNodeHandle **)
									palloc(list_length(coordlist) * sizeof(PGXCNodeHandle *));
			if (!result->coord_handles)
			{
				ereport(ERROR,
						(errcode(ERRCODE_OUT_OF_MEMORY),
						 errmsg("out of memory")));
			}

			i = 0;
			/* Some transactions do not need Coordinators, ex: COPY */
			foreach(node_list_item, coordlist)
			{
				int			node = lfirst_int(node_list_item);

				if (node < 0 || node >= NumCoords)
				{
					ereport(ERROR,
							(errcode(ERRCODE_OUT_OF_MEMORY),
							errmsg("Invalid coordinator number")));
				}

				node_handle = &co_handles[node];

				result->coord_handles[i++] = node_handle;
				if (node_handle->sock == NO_SOCKET)
					co_allocate = lappend_int(co_allocate, node);
			}
		}
	}

	/*
	 * Pooler can get activated even if list of Coordinator or Datanode is NULL
	 * If both lists are NIL, we don't need to call Pooler.
	 */
	if (dn_allocate || co_allocate)
	{
		int	j = 0;
		int	*fds = PoolManagerGetConnections(dn_allocate, co_allocate);

		if (!fds)
		{
			if (coordlist)
				if (result->coord_handles)
					pfree(result->coord_handles);
			if (datanodelist)
				if (result->datanode_handles)
					pfree(result->datanode_handles);

			pfree(result);
			if (dn_allocate)
				list_free(dn_allocate);
			if (co_allocate)
				list_free(co_allocate);
			ereport(ERROR,
					(errcode(ERRCODE_INSUFFICIENT_RESOURCES),
					 errmsg("Failed to get pooled connections")));
		}
		/* Initialisation for Datanodes */
		if (dn_allocate)
		{
			foreach(node_list_item, dn_allocate)
			{
				int			node = lfirst_int(node_list_item);
				int			fdsock = fds[j++];

				if (node < 0 || node >= NumDataNodes)
				{
					ereport(ERROR,
							(errcode(ERRCODE_OUT_OF_MEMORY),
							errmsg("Invalid Datanode number")));
				}

				node_handle = &dn_handles[node];
				pgxc_node_init(node_handle, fdsock);
				dn_handles[node] = *node_handle;
				datanode_count++;
			}
		}
		/* Initialisation for Coordinators */
		if (co_allocate)
		{
			foreach(node_list_item, co_allocate)
			{
				int			node = lfirst_int(node_list_item);
				int			fdsock = fds[j++];

				if (node < 0 || node >= NumCoords)
				{
					ereport(ERROR,
							(errcode(ERRCODE_OUT_OF_MEMORY),
							errmsg("Invalid coordinator number")));
				}

				node_handle = &co_handles[node];
				pgxc_node_init(node_handle, fdsock);
				co_handles[node] = *node_handle;
				coord_count++;
			}
		}

		pfree(fds);

		if (co_allocate)
			list_free(co_allocate);
		if (dn_allocate)
			list_free(dn_allocate);
	}

	return result;

#endif
}


/* Free PGXCNodeAllHandles structure */
void
pfree_pgxc_all_handles(PGXCNodeAllHandles *pgxc_handles)
{
	int i;
	if (pgxc_handles == NULL)
		return;

	/*primary_handle refer datanode_handles[0], don't free it */
	pgxc_handles->primary_handle = NULL;

	if (pgxc_handles->datanode_handles != NULL && pgxc_handles->dn_conn_count != 0)
	{
		for (i = 0;i < pgxc_handles->dn_conn_count;i++)
		{
			if(pgxc_handles->datanode_handles[i]->state != DN_CONNECTION_STATE_ERROR_FATAL)
				pgxc_handles->datanode_handles[i]->state = DN_CONNECTION_STATE_IDLE;
		}
 		pfree(pgxc_handles->datanode_handles);
 		pgxc_handles->datanode_handles = NULL;
	}

	if (pgxc_handles->coord_handles != NULL && pgxc_handles->co_conn_count != 0)
	{
		for (i = 0;i < pgxc_handles->co_conn_count;i++)
		{
			if(pgxc_handles->coord_handles[i]->state != DN_CONNECTION_STATE_ERROR_FATAL)
				pgxc_handles->coord_handles[i]->state = DN_CONNECTION_STATE_IDLE;
		}
 		pfree(pgxc_handles->coord_handles);
 		pgxc_handles->coord_handles = NULL;
	}

	pfree(pgxc_handles);
	pgxc_handles = NULL;
}

/*
 * PGXCNode_getNodeId
 *		Look at the data cached for handles and return node position
 */
 //麻烦
int
PGXCNodeGetNodeId(Oid nodeoid, char node_type)
{
	PGXCNodeHandle *handles = NULL;
	int				num_nodes, i;
	int				res = -1;

	/*
	 * In case of restomore mode, the u_sess->pgxc_cxt.co_handles/u_sess->pgxc_cxt.dn_handles is not initialized yet,
	 * so return 0 as first NodeId.
	 */
	if (isRestoreMode)
	{
		return 0;
	}

	/*
	 * if a table distribute by replication,
	 * when all primary's nodeis_preferred is false in pgxc_node,nodeoid maybe is invalid
	 */
	if (IS_DN_MULTI_STANDYS_MODE() && !OidIsValid(nodeoid))
		return res;

	switch (node_type)
	{
		case PGXC_NODE_COORDINATOR:
			num_nodes = u_sess->pgxc_cxt.NumCoords;
			handles = u_sess->pgxc_cxt.co_handles;
			break;
		case PGXC_NODE_DATANODE:
		case PGXC_NODE_DATANODE_STANDBY:
			/* add replication type check */
			if(node_type == PGXC_NODE_DATANODE_STANDBY && !IS_DN_MULTI_STANDYS_MODE())
			{
				ereport(PANIC, (errmsg("replication type is invalid in PGXCNodeGetNodeId (nodeoid = %u, "
				        "node_type = %c",nodeoid, node_type)));
			}
            
			num_nodes = u_sess->pgxc_cxt.NumDataNodes;
			handles = u_sess->pgxc_cxt.dn_handles;
			break;
		default:
			/* Should not happen */
			Assert(0);
			return res;
	}

	/* Look into the handles and return correct position in array */
	for (i = 0; i < num_nodes; i++)
	{
		if (IS_DN_MULTI_STANDYS_MODE() && node_type == PGXC_NODE_DATANODE)
		{
			if (PgxcNodeCheckDnMatric(handles[i].nodeoid, nodeoid))
			{
				res = i;
				break;
			}
		}
		else
		{
			if (handles[i].nodeoid == nodeoid)
			{
				res = i;
				break;
			}
		}
	}

	if (IS_DN_MULTI_STANDYS_MODE() &&
		res < 0 &&
			u_sess->attr.attr_common.upgrade_mode == 0)
	{
		ereport(DEBUG5,	(errmsg("get idx from handles for (nodeoid = %u, "
					"node_type = %c real node type is %c",
					nodeoid, node_type, get_pgxc_nodetype(nodeoid))));
	}

	return res;
}

/*
 * PGXCNode_getNodeOid
 *		Look at the data cached for handles and return node Oid
 */
Oid
PGXCNodeGetNodeOid(int nodeid, char node_type)
{
#ifndef ENABLE_MULTIPLE_NODES
	Assert(false);
	DISTRIBUTED_FEATURE_NOT_SUPPORTED();
	return 0;
#else

	PGXCNodeHandle *handles = NULL;

	switch (node_type)
	{
		case PGXC_NODE_COORDINATOR:
			handles = co_handles;
			break;
		case PGXC_NODE_DATANODE:
			handles = dn_handles;
			break;
		default:
			/* Should not happen */
			Assert(0);
			return InvalidOid;
	}

	return handles[nodeid].nodeoid;

#endif
}

/*
 * pgxc_node_str
 *
 * get the name of the node
 */
Datum
pgxc_node_str(PG_FUNCTION_ARGS)
{
	if (!g_instance.attr.attr_common.PGXCNodeName || g_instance.attr.attr_common.PGXCNodeName[0] == '\0')
		PG_RETURN_NULL();

	PG_RETURN_DATUM(DirectFunctionCall1(namein, CStringGetDatum(g_instance.attr.attr_common.PGXCNodeName)));
}

/*
 * PGXCNodeGetNodeIdFromName
 *		Return node position in handles array
 */
 //ma fan
int
PGXCNodeGetNodeIdFromName(const char *node_name, char node_type)
{
	char *nm = NULL;
	Oid nodeoid;

	if (node_name == NULL)
		return -1;

	nm = str_tolower(node_name, strlen(node_name), DEFAULT_COLLATION_OID);

	nodeoid = get_pgxc_nodeoid(nm);
	pfree(nm);
	nm = NULL;
	if (!OidIsValid(nodeoid))
		return -1;

	return PGXCNodeGetNodeId(nodeoid, node_type);
}

/*
 * PGXCNodeGetNodeNameFromId
 * 		Return node name in handles array
 */
char *
PGXCNodeGetNodeNameFromId(int nodeid, char node_type)
{
	Assert(false);
	DISTRIBUTED_FEATURE_NOT_SUPPORTED();
	return NULL;
}

/*
 * @Description: connect the remote CN with conn_str.
 *
 * @param[IN] conn_str :  connection string for remote CN
 * @param[IN] handle   :  save connected socket
 * @param[IN] host     :  the address of remote CN
 * @param[IN] port     :  port of remote CN
 */
void
connect_server(const char *conn_str, PGXCNodeHandle **handle, const char *host, int port, const char *log_conn_str)
{
	PGconn *conn = PQconnectdb(conn_str);
	if (conn != NULL && CONNECTION_OK != conn->status)
	{
		elog(LOG, "Failed to connect to the compute pool. cause: %s, connstr: %s",
		    conn->errorMessage.data, log_conn_str);
		ereport(ERROR, (errcode(ERRCODE_CONNECTION_FAILURE),
		    errmsg("Failed to connect to the compute pool. See log file for more details.")));
	}

	/* release handle in release_conn_to_compute_pool(), out of current memory context */
	{
		AutoContextSwitch newContext(THREAD_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_COMMUNICATION));
		*handle = (PGXCNodeHandle*) palloc0(sizeof(PGXCNodeHandle));
	}

	/* initialize the connection information */
	init_pgxc_handle(*handle);
	
	(*handle)->sock = PQsocket(conn);

	/* cut host address to NAMEDATALEN */
	int hostlen = strlen(host);
	if (hostlen >= NAMEDATALEN)
	{
		char *tmp = (char*)host;
		tmp[NAMEDATALEN-1] = '\0';
	}

	errno_t rc = strcpy_s(NameStr((*handle)->connInfo.host), NAMEDATALEN, host);
	securec_check(rc, "\0", "\0");
	(*handle)->connInfo.port = port;

	/* save the conn to the comute pool */
	if (t_thrd.pgxc_cxt.compute_pool_conn)
		release_conn_to_compute_pool();

	t_thrd.pgxc_cxt.compute_pool_handle = *handle;
	t_thrd.pgxc_cxt.compute_pool_conn   = conn;
}

void
release_conn_to_compute_pool()
{
	if (NULL == t_thrd.pgxc_cxt.compute_pool_conn)
		return;

	PGXCNodeHandle *handle = t_thrd.pgxc_cxt.compute_pool_handle;
	PGconn *conn   = t_thrd.pgxc_cxt.compute_pool_conn;

	t_thrd.pgxc_cxt.compute_pool_handle = NULL;
	t_thrd.pgxc_cxt.compute_pool_conn   = NULL;

	pfree(handle);
	handle = NULL;

	Assert(conn);
	PQfinish(conn);
}

void
release_pgfdw_conn()
{
	ListCell *lc1 = NULL;
	ListCell *lc2 = NULL;

	forboth(lc1, u_sess->pgxc_cxt.connection_cache, lc2, u_sess->pgxc_cxt.connection_cache_handle)
	{
		PGconn *conn = (PGconn *)lfirst(lc1);
		PGXCNodeAllHandles *pgxc_handle = (PGXCNodeAllHandles *)lfirst(lc2);

		if (conn != NULL)
		{
			if (pgxc_handle != NULL)
			{
				pfree_ext(pgxc_handle->datanode_handles[0]);
				pfree_ext(pgxc_handle->datanode_handles);
				pfree_ext(pgxc_handle);
			}
		
			PQfinish(conn);
			conn = NULL;
		}
	}

	list_free_ext(u_sess->pgxc_cxt.connection_cache);
	list_free_ext(u_sess->pgxc_cxt.connection_cache_handle);
}

int
light_node_send_begin(PGXCNodeHandle * handle, bool check_gtm_mode)
{
	Assert(false);
	DISTRIBUTED_FEATURE_NOT_SUPPORTED();
	return 0;
}

void
pgxc_node_free_def(PoolConnDef *result)
{
	Assert(false);
	DISTRIBUTED_FEATURE_NOT_SUPPORTED();
	return;
}
