/* -------------------------------------------------------------------------
 *
 * pqcomm.c
 *	  Communication functions between the Frontend and the Backend
 *
 * These routines handle the low-level details of communication between
 * frontend and backend.  They just shove data across the communication
 * channel, and are ignorant of the semantics of the data --- or would be,
 * except for major brain damage in the design of the old COPY OUT protocol.
 * Unfortunately, COPY OUT was designed to commandeer the communication
 * channel (it just transfers data without wrapping it into messages).
 * No other messages can be sent while COPY OUT is in progress; and if the
 * copy is aborted by an ereport(ERROR), we need to close out the copy so that
 * the frontend gets back into sync.  Therefore, these routines have to be
 * aware of COPY OUT state.  (New COPY-OUT is message-based and does *not*
 * set the DoingCopyOut flag.)
 *
 * NOTE: generally, it's a bad idea to emit outgoing messages directly with
 * pq_putbytes(), especially if the message would require multiple calls
 * to send.  Instead, use the routines in pqformat.c to construct the message
 * in a buffer and then emit it in one call to pq_putmessage.  This ensures
 * that the channel will not be clogged by an incomplete message if execution
 * is aborted by ereport(ERROR) partway through the message.  The only
 * non-libpq code that should call pq_putbytes directly is old-style COPY OUT.
 *
 * At one time, libpq was shared between frontend and backend, but now
 * the backend's "backend/libpq" is quite separate from "interfaces/libpq".
 * All that remains is similarities of names to trap the unwary...
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *	src/common/backend/libpq/pqcomm.cpp
 *
 * -------------------------------------------------------------------------
 */

/* ------------------------
 * INTERFACE ROUTINES
 *
 * setup/teardown:
 *		StreamServerPort	- Open postmaster's server port
 *		StreamConnection	- Create new connection with client
 *		StreamClose			- Close a client/backend connection
 *		TouchSocketFile		- Protect socket file against /tmp cleaners
 *		pq_init			- initialize libpq at backend startup
 *		pq_comm_reset	- reset libpq during error recovery
 *		pq_close		- shutdown libpq at backend exit
 *
 * low-level I/O:
 *		pq_getbytes		- get a known number of bytes from connection
 *		pq_getstring	- get a null terminated string from connection
 *		pq_getmessage	- get a message with length word from connection
 *		pq_getbyte		- get next byte from connection
 *		pq_peekbyte		- peek at next byte from connection
 *		pq_putbytes		- send bytes to connection (not flushed until pq_flush)
 *		pq_flush		- flush pending output
 *		pq_flush_if_writable - flush pending output if writable without blocking
 *		pq_getbyte_if_available - get a byte if available without blocking
 *
 * message-level I/O (and old-style-COPY-OUT cruft):
 *		pq_putmessage	- send a normal message (suppressed in COPY OUT mode)
 *		pq_putmessage_noblock - buffer a normal message (suppressed in COPY OUT)
 *		pq_startcopyout - inform libpq that a COPY OUT transfer is beginning
 *		pq_endcopyout	- end a COPY OUT transfer
 *
 * ------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"
#include <fcntl.h>
#include <grp.h>
#include <sys/file.h>
#include <sys/time.h>
#ifdef HAVE_NETINET_TCP_H
#include <netinet/tcp.h>
#endif
#include <arpa/inet.h>
#ifdef HAVE_UTIME_H
#include <utime.h>
#endif
#ifdef WIN32_ONLY_COMPILER /* mstcpip.h is missing on mingw */
#include <mstcpip.h>
#endif
#ifdef HAVE_POLL_H
#include <poll.h>
#endif
#include "pgxc/pgxc.h"
#include "libpq/ip.h"
#include "libpq/libpq.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "storage/ipc.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "libpq/pqcomm.h"
#include "replication/replicainternal.h"
#include "utils/timestamp.h"
#include "postmaster/postmaster.h"
#include "libcomm/libcomm.h"
#include "libpq/pqformat.h"
#include "pgxc/nodemgr.h"
#include "storage/lz4_file.h"
#include "tcop/stmt_retry.h"
#include "communication/commproxy_interface.h"
#include "distributelayer/streamProducer.h"

#define MAXLISTEN 64
#define IP_LEN 64
#define CRC_HEADER 12  // uint32 sequence number + uint32 data length + uint32 crc checksum.

extern GlobalNodeDefinition* global_node_definition;

extern ProtocolExtensionConfig* ListenConfig[MAXLISTEN];
extern ProtocolExtensionConfig default_protocol_config;

/*
 * Buffers for low-level I/O.
 *
 * The receive buffer is fixed size. Send buffer is usually 8k, but can be
 * enlarged by pq_putmessage_noblock() if the message doesn't fit otherwise.
 */
#define PQ_BUFFER_SIZE 8192
#define PQ_SEND_BUFFER_SIZE PQ_BUFFER_SIZE

#ifdef USE_RETRY_STUB
#define PQ_RECV_BUFFER_SIZE 16
#else
#define PQ_RECV_BUFFER_SIZE PQ_BUFFER_SIZE
#endif

#define NAPTIME_PER_SEND_RETRY 100 /* max sleep between two send try (100ms) */
#define NAPTIME_PER_SEND 10        /* max sleep before sending next batch of data (10ms) */

void pq_close(int code, Datum arg);
int internal_putbytes(const char* s, size_t len);

/* Internal functions */
static int internal_flush(void);
static void pq_set_nonblocking(bool nonblocking);
static void pq_disk_generate_checking_header(
    const char* src_data, StringInfo dest_data, uint32 data_len, uint32 seq_num);
static size_t pq_disk_read_data_block(
    LZ4File* file_handle, char* src_data, char* dest_data, uint32 data_len, uint32 seq_num);

static int libnet_flush(void);

#ifdef HAVE_UNIX_SOCKETS
static int Lock_AF_UNIX(unsigned short portNumber, const char* unixSocketName, bool is_create_psql_sock);
static int Setup_AF_UNIX(bool is_create_psql_sock);
#endif /* HAVE_UNIX_SOCKETS */

extern bool FencedUDFMasterMode;

/* --------------------------------
 *		usages for temp file operations
 * --------------------------------
 */
typedef struct TempFileContextInfo {
    LZ4File* file_handle;
    TempFileState file_state;
    size_t file_size;
    uint32 seq_count;          /* count number of PqRecvBuffers */
    StringInfoData crc_buffer; /* store stringinfo with crc header */
} TempFileContextInfo;

void temp_file_context_init(knl_t_libpq_context* libpq_cxt)
{
    libpq_cxt->PqTempFileContextInfo = (TempFileContextInfo*)palloc0(sizeof(TempFileContextInfo));
}

/*
 * @Description: enable temp file for saving query result.
 */
void pq_disk_enable_temp_file(void)
{
    if ((u_sess->attr.attr_sql.max_cn_temp_file_size > 0) && IS_PGXC_COORDINATOR) {
        t_thrd.libpq_cxt.save_query_result_to_disk = true;
    }
}

/*
 * @Description: disable temp file to saving query result.
 */
void pq_disk_disable_temp_file(void)
{
    t_thrd.libpq_cxt.save_query_result_to_disk = false;
}

/*
 * @Description: check temp file enabled or not.
 * @return - true, tempfile is enabled. false, meaning temp file is disabled.
 */
bool pq_disk_is_temp_file_enabled(void)
{
    return t_thrd.libpq_cxt.save_query_result_to_disk;
}

/*
 * @Description: check temp file created or not.
 * @return - true, tempfile is created.
 */
bool pq_disk_is_temp_file_created(void)
{
    return t_thrd.libpq_cxt.PqTempFileContextInfo->file_handle != NULL;
}

/*
 * @Description: create temp file for saving query result. which will update PqTempFileContextInfo at the same time.
 */
static inline void pq_disk_create_tempfile(void)
{
    MemoryContext oldcontext;
    oldcontext = MemoryContextSwitchTo(SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_COMMUNICATION));

    t_thrd.libpq_cxt.PqTempFileContextInfo->file_handle = LZ4FileCreate(true);
    t_thrd.libpq_cxt.PqTempFileContextInfo->file_state = TEMPFILE_CREATED;
    t_thrd.libpq_cxt.PqTempFileContextInfo->file_size = 0;
    t_thrd.libpq_cxt.PqTempFileContextInfo->seq_count = 0;

    initStringInfo(&(t_thrd.libpq_cxt.PqTempFileContextInfo->crc_buffer));

    (void)MemoryContextSwitchTo(oldcontext);
}

/*
 * @Description: write query result to temp file instead of saving query result to PqSendBuffer.
 * @in - data, data pointer.
 * @in - size, size of data to be written.
 * @return - written size, EOF if error happens.
 */
static size_t pq_disk_write_tempfile(const void* data, size_t size)
{
    size_t nwritten = 0;

    if (t_thrd.libpq_cxt.PqTempFileContextInfo->crc_buffer.data != NULL) {
        resetStringInfo(&(t_thrd.libpq_cxt.PqTempFileContextInfo->crc_buffer));
    } else {
        ereport(LOG, (errmodule(MOD_CN_RETRY), errmsg("invalid crc buffer for temp file.")));
        return EOF;
    }

    MemoryContext oldcontext;
    oldcontext = MemoryContextSwitchTo(SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_COMMUNICATION));

    /* generate a crc header and append data after it */
    pq_disk_generate_checking_header((char*)data,
        &(t_thrd.libpq_cxt.PqTempFileContextInfo->crc_buffer),
        size,
        t_thrd.libpq_cxt.PqTempFileContextInfo->seq_count++);

    nwritten = LZ4FileWrite(t_thrd.libpq_cxt.PqTempFileContextInfo->file_handle,
        t_thrd.libpq_cxt.PqTempFileContextInfo->crc_buffer.data,
        size + CRC_HEADER);

    (void)MemoryContextSwitchTo(oldcontext);

    if (nwritten < CRC_HEADER) {
        return 0;
    }

    nwritten -= CRC_HEADER;  // don't calculate file size with crc header
    Assert(nwritten == size);

    t_thrd.libpq_cxt.PqTempFileContextInfo->file_state = TEMPFILE_FLUSHED;
    t_thrd.libpq_cxt.PqTempFileContextInfo->file_size += nwritten;

    /* be ware to write file first before flush file data to frontend */
    size_t file_size = t_thrd.libpq_cxt.PqTempFileContextInfo->file_size / (GUC_UNIT_KB);

    if (file_size > (size_t)u_sess->attr.attr_sql.max_cn_temp_file_size) {
        /*
         * if here, meaning temp file size has exceeded, in order to continue
         * query execution,
         * 1. we need to send current query result in temp file to
         *    the frontend.
         * 2. but current query can't be retried any more, since we can't
         *    protect total query result stay in temp file.
         * 3. close current file and create a new one
         */
        ereport(LOG,
            (errmodule(MOD_CN_RETRY),
                errmsg(" %s temp file exceeded, max temp file size : %d KB, current result size : %lu KB",
                    PRINT_PREFIX_TYPE_ALERT,
                    u_sess->attr.attr_sql.max_cn_temp_file_size,
                    file_size)));

        StmtRetrySetFileExceededFlag();
        pq_disk_send_to_frontend();

        pq_disk_create_tempfile();
    }

    return nwritten;
}
/*
 * @Description: extract send buffer data to temp file.
 */
void pq_disk_extract_sendbuffer(void)
{
    if (t_thrd.libpq_cxt.PqSendPointer) {
        (void)pq_disk_write_tempfile(
            t_thrd.libpq_cxt.PqSendBuffer + t_thrd.libpq_cxt.PqSendStart, t_thrd.libpq_cxt.PqSendPointer);
        t_thrd.libpq_cxt.PqSendPointer = 0;
    }
}

/*
 * @Description: reset PqTempFileContextInfo.
 */
void pq_disk_reset_tempfile_contextinfo(void)
{
    t_thrd.libpq_cxt.PqTempFileContextInfo->file_handle = NULL;
    t_thrd.libpq_cxt.PqTempFileContextInfo->file_state = TEMPFILE_DEFAULT;
    t_thrd.libpq_cxt.PqTempFileContextInfo->file_size = 0;
    t_thrd.libpq_cxt.PqTempFileContextInfo->seq_count = 0;

    if (t_thrd.libpq_cxt.PqTempFileContextInfo->crc_buffer.data != NULL) {
        pfree_ext(t_thrd.libpq_cxt.PqTempFileContextInfo->crc_buffer.data);
    }
    t_thrd.libpq_cxt.PqTempFileContextInfo->crc_buffer.len = 0;
    t_thrd.libpq_cxt.PqTempFileContextInfo->crc_buffer.maxlen = 0;
    t_thrd.libpq_cxt.PqTempFileContextInfo->crc_buffer.cursor = 0;
}

/*
 * @Description: check whether temp file is once flushed before.
 * @return - true, temp file had been flushed. false, temp file never flushed.
 */
bool pq_disk_is_flushed(void)
{
    return (t_thrd.libpq_cxt.PqTempFileContextInfo->file_state == TEMPFILE_FLUSHED);
}

/*
 * @Description: send query result data in tempfile to frontend.
 * @in use_flush_protection - if true use pq_flush to send data else use internal_flush
 * @return - 0 if OK, EOF if trouble
 */
int pq_disk_send_to_frontend(void)
{
    Assert(t_thrd.libpq_cxt.PqTempFileContextInfo->file_handle);

    int res = 0;

    ereport(DEBUG2,
        (errmodule(MOD_CN_RETRY),
            errmsg("data in tempfile \"%zu\". ", t_thrd.libpq_cxt.PqTempFileContextInfo->file_size)));

    LZ4FileRewind(t_thrd.libpq_cxt.PqTempFileContextInfo->file_handle);

    size_t total_read = 0;
    size_t read_size = 0;
    uint32 read_seq = 0;

    t_thrd.libpq_cxt.PqTempFileContextInfo->file_state = TEMPFILE_ON_SENDING;

    char* read_data = NULL;
    read_data = (char*)palloc0(t_thrd.libpq_cxt.PqSendBufferSize + CRC_HEADER);

    do {
        /* exclude a crc header and extract data after it */
        read_size = pq_disk_read_data_block(t_thrd.libpq_cxt.PqTempFileContextInfo->file_handle,
            read_data,
            t_thrd.libpq_cxt.PqSendBuffer,
            t_thrd.libpq_cxt.PqSendBufferSize,
            read_seq);
        if (read_size == 0) {
            break;
        }

        read_seq++;
        t_thrd.libpq_cxt.PqSendPointer = read_size;
        total_read += read_size;

        res = internal_flush();
        if (res == EOF) {
            ereport(LOG, (errmodule(MOD_CN_RETRY), errmsg("get EOF while flushing data.")));
            t_thrd.libpq_cxt.PqTempFileContextInfo->file_state = TEMPFILE_ERROR_SEND;
            break;
        }
    } while (read_size == (size_t)t_thrd.libpq_cxt.PqSendBufferSize);

    pfree_ext(read_data);

    if (read_seq != t_thrd.libpq_cxt.PqTempFileContextInfo->seq_count) {
        ereport(FATAL,
            (errmodule(MOD_CN_RETRY),
                errcode(ERRCODE_DATA_EXCEPTION),
                errmsg("Last read message sequence %u is not equal to the max written message sequence %u",
                    read_seq,
                    t_thrd.libpq_cxt.PqTempFileContextInfo->seq_count)));
    }

    t_thrd.libpq_cxt.PqTempFileContextInfo->file_size -= total_read;

    if (t_thrd.libpq_cxt.PqTempFileContextInfo->file_state != TEMPFILE_ERROR_SEND) {
        t_thrd.libpq_cxt.PqTempFileContextInfo->file_state = TEMPFILE_SENDED;
    }

    ereport(DEBUG2,
        (errmodule(MOD_CN_RETRY),
            errmsg("remaining data in tempfile \"%zu\", total read data \"%zu\" ",
                t_thrd.libpq_cxt.PqTempFileContextInfo->file_size,
                total_read)));

    /* since we can not reuse tempfile discard it */
    pq_disk_discard_temp_file();
    return res;
}

/*
 * @Description: discard temp file if created, which will close and delete temp file at same time.
 */
void pq_disk_discard_temp_file(void)
{
    LZ4File* file_handle = t_thrd.libpq_cxt.PqTempFileContextInfo->file_handle;
    pq_disk_reset_tempfile_contextinfo();

    if (file_handle != NULL) {
        LZ4FileClose(file_handle);
    }
}

/*
 * @Description: get temp file state.
 * @return - file state
 */
TempFileState pq_disk_file_state(void)
{
    return t_thrd.libpq_cxt.PqTempFileContextInfo->file_state;
}

/* --------------------------------
 *		pq_init - initialize libpq at backend startup
 * --------------------------------
 */
void pq_init(void)
{
    t_thrd.libpq_cxt.PqSendBufferSize = g_instance.attr.attr_network.cn_send_buffer_size * (GUC_UNIT_KB);
#ifdef USE_RETRY_STUB
    t_thrd.libpq_cxt.PqSendBufferSize = 64;
#endif
    t_thrd.libpq_cxt.PqSendBuffer = (char*)MemoryContextAlloc(
        THREAD_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_COMMUNICATION), t_thrd.libpq_cxt.PqSendBufferSize);

    t_thrd.libpq_cxt.PqRecvBuffer = (char*)MemoryContextAlloc(
        THREAD_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_COMMUNICATION), PQ_RECV_BUFFER_SIZE);
    t_thrd.libpq_cxt.PqRecvBufferSize = PQ_RECV_BUFFER_SIZE;
    t_thrd.libpq_cxt.PqSendPointer = t_thrd.libpq_cxt.PqSendStart = t_thrd.libpq_cxt.PqRecvPointer =
        t_thrd.libpq_cxt.PqRecvLength = 0;
    t_thrd.libpq_cxt.PqCommBusy = false;
    t_thrd.libpq_cxt.DoingCopyOut = false;

    pq_disk_reset_tempfile_contextinfo();

    on_proc_exit(pq_close, 0);

    /*
     * For light comm, initially, set the underlying socket in nonblocking mode
     * and use latches to implement blocking semantics if needed.
     * (e.g., WAL will assume that the standy node has exited, if no information is received)
     *
     * should always use COMMERROR on failure during communication
     */
#ifndef WIN32
    if (g_instance.attr.attr_common.light_comm == TRUE &&
        u_sess->proc_cxt.MyProcPort->sock != PGINVALID_SOCKET && !pg_set_noblock(u_sess->proc_cxt.MyProcPort->sock)){
        ereport(COMMERROR, (errmsg("could not set socket to nonblocking mode: %m")));
    }
#endif
}

/* --------------------------------
 *		pq_comm_reset - reset libpq during error recovery
 *
 * This is called from error recovery at the outer idle loop.  It's
 * just to get us out of trouble if we somehow manage to elog() from
 * inside a pqcomm.c routine (which ideally will never happen, but...)
 * --------------------------------
 */
void pq_comm_reset(void)
{
    /* Do not throw away pending data, but do reset the busy flag */
    t_thrd.libpq_cxt.PqCommBusy = false;
    /* We can abort any old-style COPY OUT, too */
    pq_endcopyout(true);
}

/* --------------------------------
 *		pq_close - shutdown libpq at backend exit
 *
 * Note: in a standalone backend u_sess->proc_cxt.MyProcPort will be null,
 * don't crash during exit...
 * --------------------------------
 */
void pq_close(int code, Datum arg)
{
    if (t_thrd.postmaster_cxt.KeepSocketOpenForStream) {
        return;
    }

    if (u_sess->proc_cxt.MyProcPort != NULL) {
        if (u_sess->proc_cxt.MyProcPort->gss != NULL) {
#if defined(ENABLE_GSS) || defined(ENABLE_SSPI)
#ifdef ENABLE_GSS
            OM_uint32 min_s;

            /* Shutdown GSSAPI layer */
            if (u_sess->proc_cxt.MyProcPort->gss->ctx != GSS_C_NO_CONTEXT) {
                gss_delete_sec_context(&min_s, &u_sess->proc_cxt.MyProcPort->gss->ctx, NULL);
            }

            if (u_sess->proc_cxt.MyProcPort->gss->cred != GSS_C_NO_CREDENTIAL) {
                gss_release_cred(&min_s, &u_sess->proc_cxt.MyProcPort->gss->cred);
            }

#endif /* ENABLE_GSS */
            /* GSS and SSPI share the port->gss struct */
            pfree_ext(u_sess->proc_cxt.MyProcPort->gss);
#endif /* ENABLE_GSS || ENABLE_SSPI */
        }

        /* Cleanly shut down SSL layer */
        secure_close(u_sess->proc_cxt.MyProcPort);

        /*
         * Formerly we did an explicit close() here, but it seems better to
         * leave the socket open until the process dies.  This allows clients
         * to perform a "synchronous close" if they care --- wait till the
         * transport layer reports connection closure, and you can be sure the
         * backend has exited.
         *
         * We do set sock to PGINVALID_SOCKET to prevent any further I/O,
         * though.
         */
        if (u_sess->proc_cxt.MyProcPort->is_logic_conn) {
            gs_close_gsocket(&(u_sess->proc_cxt.MyProcPort->gs_sock));
        } else {
            if (u_sess->proc_cxt.MyProcPort->sock != PGINVALID_SOCKET) {
                comm_closesocket(u_sess->proc_cxt.MyProcPort->sock);
            }
            u_sess->proc_cxt.MyProcPort->sock = PGINVALID_SOCKET;
        }
    }
}

/*
 * Streams -- wrapper around Unix socket system calls
 *
 * Stream functions are used for vanilla TCP connection protocol.
 */

/* StreamDoUnlink()
 * Shutdown routine for backend connection
 * If a Unix socket is used for communication, explicitly close it.
 */
#ifdef HAVE_UNIX_SOCKETS
static void StreamDoUnlink(int code, Datum arg)
{
    if ((int)arg == PSQL_LISTEN_SOCKET) {
        Assert(t_thrd.libpq_cxt.sock_path[0]);
        unlink(t_thrd.libpq_cxt.sock_path);
    } else if ((int)arg == HA_LISTEN_SOCKET) {
        Assert(t_thrd.libpq_cxt.ha_sock_path[0]);
        unlink(t_thrd.libpq_cxt.ha_sock_path);
    }
}
#endif /* HAVE_UNIX_SOCKETS */

/*
 * StreamServerPort -- open a "listening" port to accept connections.
 *
 * Successfully opened sockets are added to the ListenSocket[] array,
 * at the first position that isn't PGINVALID_SOCKET.
 *
 * RETURNS: STATUS_OK or STATUS_ERROR
 */
int StreamServerPort(int family, char* hostName, unsigned short portNumber, const char* unixSocketName,
    pgsocket ListenSocket[], int MaxListen, bool add_localaddr_flag,
    bool is_create_psql_sock, bool is_create_libcomm_sock, ListenChanelType listen_channel,
    ProtocolExtensionConfig* protocol_config) {
#define RETRY_SLEEP_TIME 1000000L
    pgsocket fd = PGINVALID_SOCKET;
    int err;
    int maxconn;
    int ret;
    char portNumberStr[32];
    const char* familyDesc = NULL;
    char familyDescBuf[64];
    char* service = NULL;
    struct addrinfo* addrs = NULL;
    struct addrinfo* addr = NULL;
    struct addrinfo hint;
    int listen_index = 0;
    int added = 0;
    const int tryBindNum = 3;
    int i = 0;
    errno_t rc = EOK;

#if !defined(WIN32) || defined(IPV6_V6ONLY)
    int one = 1;
#endif

    /* Initialize hint structure */
    rc = memset_s(&hint, sizeof(hint), 0, sizeof(hint));
    securec_check(rc, "\0", "\0");
    hint.ai_family = family;
    hint.ai_flags = AI_PASSIVE;
    hint.ai_socktype = SOCK_STREAM;

#ifdef HAVE_UNIX_SOCKETS
    if (family == AF_UNIX) {
        /* Lock_AF_UNIX will also fill in sock_path. */
        if (Lock_AF_UNIX(portNumber, unixSocketName, is_create_psql_sock) != STATUS_OK) {
            return STATUS_ERROR;
        }
        service = (is_create_psql_sock ? t_thrd.libpq_cxt.sock_path : t_thrd.libpq_cxt.ha_sock_path);
    } else
#endif /* HAVE_UNIX_SOCKETS */
    {
        rc = snprintf_s(portNumberStr, sizeof(portNumberStr), sizeof(portNumberStr) - 1, "%hu", portNumber);
        securec_check_ss(rc, "\0", "\0");
        service = portNumberStr;
    }

    ret = pg_getaddrinfo_all(hostName, service, &hint, &addrs);
    if (ret || addrs == NULL) {
        if (hostName != NULL) {
            ereport(LOG,
                (errmsg("could not translate host name \"%s\", service \"%s\" to address: %s",
                    hostName,
                    service,
                    gai_strerror(ret))));
        } else {
            ereport(LOG, (errmsg("could not translate service \"%s\" to address: %s", service, gai_strerror(ret))));
        }
        if (addrs != NULL) {
            pg_freeaddrinfo_all(hint.ai_family, addrs);
        }
        return STATUS_ERROR;
    }

    for (addr = addrs; addr; addr = addr->ai_next) {
        /* init value of fd */
        fd = PGINVALID_SOCKET;

        if (!IS_AF_UNIX(family) && IS_AF_UNIX(addr->ai_family)) {
            /*
             * Only set up a unix domain socket when they really asked for it.
             * The service/port is different in that case.
             */
            continue;
        }

        /* See if there is still room to add 1 more socket. */
        for (; listen_index < MaxListen; listen_index++) {
            if (ListenSocket[listen_index] == PGINVALID_SOCKET) {
                break;
            }
        }
        if (listen_index >= MaxListen) {
            ereport(LOG, (errmsg("could not bind to all requested addresses: MAXLISTEN (%d) exceeded", MaxListen)));
            break;
        }

        /* set up family name for possible error messages */
        switch (addr->ai_family) {
            case AF_INET:
                familyDesc = _("IPv4");
                break;
#ifdef HAVE_IPV6
            case AF_INET6:
                familyDesc = _("IPv6");
                break;
#endif
#ifdef HAVE_UNIX_SOCKETS
            case AF_UNIX:
                familyDesc = _("Unix");
                break;
#endif
            default:
                rc = snprintf_s(familyDescBuf,
                    sizeof(familyDescBuf),
                    sizeof(familyDescBuf) - 1,
                    _("unrecognized address family %d"),
                    addr->ai_family);
                securec_check_ss(rc, "\0", "\0");
                familyDesc = familyDescBuf;
                break;
        }

        /*
         * Config the socket() is in proxy mode
         *
         * Note: Onece fd is created from socket(), go raw TCP/IP or proxy-enabled path is
         *       determined, here we make the specification in StreamServerPort()
         */
        CommConfigSocketOption(CommConnSocketServer, hostName, portNumber);
        fd = comm_socket(addr->ai_family, SOCK_STREAM, 0);
        if (fd < 0) {
            ereport(LOG,
                (errcode_for_socket_access(),
                    /* translator: %s is IPv4, IPv6, or Unix */
                    errmsg("could not create %s socket: %m", familyDesc)));
            goto errhandle;
        }
        
        /*
         * save unix domain sock,
         * thus we will know when
         * rece flow ctrl thread
         * send the libcomm addr to server loop
         */
        if (is_create_libcomm_sock) {
            t_thrd.libpq_cxt.listen_fd_for_recv_flow_ctrl = fd;
        }

#ifdef F_SETFD
        if (comm_fcntl(fd, F_SETFD, FD_CLOEXEC) == -1) {
            ereport(LOG, (errcode_for_socket_access(), errmsg("comm_setsockopt(FD_CLOEXEC) failed: %m")));
            goto errhandle;
        }
#endif /* F_SETFD */

#ifndef WIN32

        /*
         * Without the SO_REUSEADDR flag, a new postmaster can't be started
         * right away after a stop or crash, giving "address already in use"
         * error on TCP ports.
         *
         * On win32, however, this behavior only happens if the
         * SO_EXLUSIVEADDRUSE is set. With SO_REUSEADDR, win32 allows multiple
         * servers to listen on the same address, resulting in unpredictable
         * behavior. With no flags at all, win32 behaves as Unix with
         * SO_REUSEADDR.
         */
        if (!IS_AF_UNIX(addr->ai_family) || IsCommProxyStartUp()) {
            if ((comm_setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, (char*)&one, sizeof(one))) == -1) {
                ereport(LOG, (errcode_for_socket_access(), errmsg("comm_setsockopt(SO_REUSEADDR) failed: %m")));
                goto errhandle;
            }
        }
#endif

#ifdef IPV6_V6ONLY
        if (addr->ai_family == AF_INET6) {
            if (comm_setsockopt(fd, IPPROTO_IPV6, IPV6_V6ONLY, (char*)&one, sizeof(one)) == -1) {
                ereport(LOG, (errcode_for_socket_access(), errmsg("comm_setsockopt(IPV6_V6ONLY) failed: %m")));

                goto errhandle;
            }
        }
#endif

        /*
         * Note: This might fail on some OS's, like Linux older than
         * 2.4.21-pre3, that don't have the IPV6_V6ONLY socket option, and map
         * ipv4 addresses to ipv6.	It will show ::ffff:ipv4 for all ipv4
         * connections.if the bind failded, we sleep for 100ms and try it again.
         * We will try at most 30 times, because of slow clean of OS.
         */
        for (i = 0; i != tryBindNum; ++i) {
            err = comm_bind(fd, addr->ai_addr, addr->ai_addrlen);
            if (err < 0) {
                /* need not retry when a addr is added before */
                if (added != 0) {
                    i = tryBindNum;
                    break;
                }
                ereport(LOG,
                    (errcode_for_socket_access(),
                        /* translator: %s is IPv4, IPv6, or Unix */
                        errmsg("could not bind %s socket at the %d time: %m", familyDesc, i),
                        (IS_AF_UNIX(addr->ai_family))
                            ? errhint("Is another postmaster already running on port %d?"
                                      " If not, remove socket file \"%s\" and retry.",
                                  (int)portNumber,
                                  service)
                            : errhint("Port %u is used, run 'netstat -anop|grep %u' or "
                                      "'lsof -i:%u'(need root) to see who is using this port.",
                                  portNumber,
                                  portNumber,
                                  portNumber)));
                pg_usleep(RETRY_SLEEP_TIME);
                continue;
            } else {
                break;
            }
        }
        if (i == tryBindNum) {
            goto errhandle;
        }

#ifdef HAVE_UNIX_SOCKETS
        if (addr->ai_family == AF_UNIX) {
            if (Setup_AF_UNIX(is_create_psql_sock) != STATUS_OK) {
                goto errhandle;
            }
        }
#endif

        /*
         * Select appropriate accept-queue length limit.  PG_SOMAXCONN is only
         * intended to provide a clamp on the request on platforms where an
         * overly large request provokes a kernel error (are there any?).
         */
        maxconn = g_instance.attr.attr_network.MaxConnections * 6;
        maxconn = Max(maxconn, PG_SOMINCONN);
        maxconn = Min(maxconn, PG_SOMAXCONN);

        err = comm_listen(fd, maxconn);
        if (err < 0) {
            ereport(LOG,
                (errcode_for_socket_access(),
                    /* translator: %s is IPv4, IPv6, or Unix */
                    errmsg("could not listen on %s socket[%s:%d]: %m", familyDesc, hostName, (int)portNumber)));
            goto errhandle;
        }
        ListenSocket[listen_index] = fd;
        ListenConfig[listen_index] = protocol_config;
        added++;
        if (add_localaddr_flag == true) {
            struct sockaddr* sinp = NULL;
            char* result = NULL;

            sinp = (struct sockaddr*)(addr->ai_addr);
            if (addr->ai_family == AF_INET6) {
                result = inet_net_ntop(AF_INET6,
                    &((struct sockaddr_in6*)sinp)->sin6_addr,
                    128,
                    t_thrd.postmaster_cxt.LocalAddrList[t_thrd.postmaster_cxt.LocalIpNum],
                    IP_LEN);
            } else if (addr->ai_family == AF_INET) {
                result = inet_net_ntop(AF_INET,
                    &((struct sockaddr_in*)sinp)->sin_addr,
                    32,
                    t_thrd.postmaster_cxt.LocalAddrList[t_thrd.postmaster_cxt.LocalIpNum],
                    IP_LEN);
            }
            if (result == NULL) {
                ereport(WARNING, (errmsg("inet_net_ntop failed, error: %d", EAFNOSUPPORT)));
            } else {
                ereport(DEBUG5, (errmodule(MOD_COMM_FRAMEWORK),
                    errmsg("[reload listen IP]set LocalIpNum[%d] %s",
                    t_thrd.postmaster_cxt.LocalIpNum,
                    t_thrd.postmaster_cxt.LocalAddrList[t_thrd.postmaster_cxt.LocalIpNum])));
                t_thrd.postmaster_cxt.LocalIpNum++;
            }
        }
        if (is_create_psql_sock) {
            g_instance.listen_cxt.listen_sock_type[listen_index] = PSQL_LISTEN_SOCKET;
        } else {
            g_instance.listen_cxt.listen_sock_type[listen_index] = HA_LISTEN_SOCKET;
        }

        /*
         * note:
         * NORMAL_LISTEN_CHANEL include : listen_address or libcomm_bind_addr.
         * REPL_LISTEN_CHANEL include : replication_info
         * EXT_LISTEN_CHANEL include : listen_address_ext
         */
        g_instance.listen_cxt.listen_chanel_type[listen_index] = listen_channel;

        /* for debug info */
        rc = strcpy_s(g_instance.listen_cxt.all_listen_addr_list[listen_index], IP_LEN,
            (hostName == NULL) ? ((addr->ai_family == AF_UNIX) ? "unix domain" : "*") : hostName);
        securec_check(rc, "", "");
        g_instance.listen_cxt.all_listen_port_list[listen_index] = portNumber;

        continue;

    errhandle:
        if (fd != PGINVALID_SOCKET) {
            comm_closesocket(fd);
        }
    }

    pg_freeaddrinfo_all(hint.ai_family, addrs);

    if (!added) {
        return STATUS_ERROR;
    }

    return STATUS_OK;
}

#ifdef HAVE_UNIX_SOCKETS

/*
 * Lock_AF_UNIX -- configure unix socket file path
 */
static int Lock_AF_UNIX(unsigned short portNumber, const char* unixSocketName, bool is_create_psql_sock)
{
    char* sock_path = NULL;

    if (is_create_psql_sock) {
        UNIXSOCK_PATH(t_thrd.libpq_cxt.sock_path, portNumber, unixSocketName);
        sock_path = t_thrd.libpq_cxt.sock_path;
    } else {
        UNIXSOCK_PATH(t_thrd.libpq_cxt.ha_sock_path, portNumber, unixSocketName);
        sock_path = t_thrd.libpq_cxt.ha_sock_path;
    }

    if (strlen(sock_path) >= UNIXSOCK_PATH_BUFLEN) {
        ereport(LOG,
            (errmsg("Unix-domain socket path \"%s\" is too long (maximum %d bytes)",
                sock_path,
                (int)(UNIXSOCK_PATH_BUFLEN - 1))));
        return STATUS_ERROR;
    }

    /*
     * Grab an interlock file associated with the socket file.
     *
     * Note: there are two reasons for using a socket lock file, rather than
     * trying to interlock directly on the socket itself.  First, it's a lot
     * more portable, and second, it lets us remove any pre-existing socket
     * file without race conditions.
     */
    CreateSocketLockFile(sock_path, true, is_create_psql_sock);

    /*
     * Once we have the interlock, we can safely delete any pre-existing
     * socket file to avoid failure at bind() time.
     */
    unlink(sock_path);

    return STATUS_OK;
}

/*
 * Setup_AF_UNIX -- configure unix socket permissions
 */
static int Setup_AF_UNIX(bool is_create_psql_sock)
{
    /* Arrange to unlink the socket file at exit */
    on_proc_exit(StreamDoUnlink, is_create_psql_sock ? (Datum)PSQL_LISTEN_SOCKET : (Datum)HA_LISTEN_SOCKET);

    const char* sock_path = (is_create_psql_sock ? t_thrd.libpq_cxt.sock_path : t_thrd.libpq_cxt.ha_sock_path);

    /*
     * Fix socket ownership/permission if requested.  Note we must do this
     * before we listen() to avoid a window where unwanted connections could
     * get accepted.
     */
    Assert(g_instance.attr.attr_network.Unix_socket_group);
    if (g_instance.attr.attr_network.Unix_socket_group[0] != '\0') {
#ifdef WIN32
        ereport(WARNING, (errmsg("configuration item unix_socket_group is not supported on this platform")));
#else
        char* endptr = NULL;
        unsigned long val;
        gid_t gid;

        val = strtoul(g_instance.attr.attr_network.Unix_socket_group, &endptr, 10);
        if (*endptr == '\0') { /* numeric group id */
            gid = val;
        } else { /* convert group name to id */
            // use the getgrnam_r to guarantee thread safe
            struct group grp;
            struct group* grpptr = &grp;
            struct group* tmpGrpPtr = NULL;
            char grpBuffer[200];
            int grpLineLen = sizeof(grpBuffer);
            int ret;

            if ((ret = getgrnam_r(
                     g_instance.attr.attr_network.Unix_socket_group, grpptr, grpBuffer, grpLineLen, &tmpGrpPtr)) != 0) {
                ereport(LOG, (errmsg("getgrnam_r() error, error num is %d", ret)));
                return STATUS_ERROR;
            }

            if (tmpGrpPtr == NULL) {
                ereport(LOG, (errmsg("group \"%s\" does not exist", g_instance.attr.attr_network.Unix_socket_group)));
                return STATUS_ERROR;
            }
            gid = grp.gr_gid;
        }
        if (chown(sock_path, -1, gid) == -1) {
            ereport(LOG, (errcode_for_file_access(), errmsg("could not set group of file \"%s\": %m", sock_path)));
            return STATUS_ERROR;
        }
#endif
    }

    if (chmod(sock_path, g_instance.attr.attr_network.Unix_socket_permissions) == -1) {
        ereport(LOG, (errcode_for_file_access(), errmsg("could not set permissions of file \"%s\": %m", sock_path)));
        return STATUS_ERROR;
    }
    return STATUS_OK;
}
#endif /* HAVE_UNIX_SOCKETS */

/*
 * StreamConnection -- create a new connection with client using
 *		server port.  Set port->sock to the FD of the new connection.
 *
 * ASSUME: that this doesn't need to be non-blocking because
 *		the Postmaster uses select() to tell when the server master
 *		socket is ready for accept().
 *
 * RETURNS: STATUS_OK or STATUS_ERROR
 */
int StreamConnection(pgsocket server_fd, Port* port)
{
    /* accept connection and fill in the client (remote) address */
    port->raddr.salen = sizeof(port->raddr.addr);

    /* CommProxy Support */
    if ((port->sock = comm_accept4(server_fd, (struct sockaddr*)&port->raddr.addr, &port->raddr.salen, SOCK_CLOEXEC)) < 0) {
        ereport(LOG, (errcode_for_socket_access(), errmsg("could not comm_accept() new connection: %m")));

        /*
         * If accept() fails then postmaster.c will still see the server
         * socket as read-ready, and will immediately try again.  To avoid
         * uselessly sucking lots of CPU, delay a bit before trying again.
         * (The most likely reason for failure is being out of kernel file
         * table slots; we can do little except hope some will get freed up.)
         */
        pg_usleep(100000L); /* wait 0.1 sec */
        return STATUS_ERROR;
    }

#ifdef SCO_ACCEPT_BUG

    /*
     * UnixWare 7+ and OpenServer 5.0.4 are known to have this bug, but it
     * shouldn't hurt to catch it for all versions of those platforms.
     */
    if (port->raddr.addr.ss_family == 0) {
        port->raddr.addr.ss_family = AF_UNIX;
    }
#endif

    /* fill in the server (local) address */
    port->laddr.salen = sizeof(port->laddr.addr);

    /* CommProxy Support */
    if (comm_getsockname(port->sock, (struct sockaddr*)&port->laddr.addr, &port->laddr.salen) < 0) {
        ereport(LOG, (errmsg("comm_getsockname() failed: %m")));
        return STATUS_ERROR;
    }

    /* select NODELAY and KEEPALIVE options if it's a TCP connection */
    if (!IS_AF_UNIX(port->laddr.addr.ss_family)) {
        int on;
        int opval = 0;
        on = 1;

#ifndef USE_LIBNET
        /* libnet not support SO_PROTOCOL in lwip */
        socklen_t oplen = sizeof(opval);
        /* CommProxy Support */
        if (comm_getsockopt(port->sock, SOL_SOCKET, SO_PROTOCOL, &opval, &oplen) < 0) {
            ereport(LOG, (errmsg("comm_getsockopt(SO_PROTOCOL) failed: %m")));
            return STATUS_ERROR;
        }
#endif

        /* CommProxy Support */
        if (comm_setsockopt(port->sock, IPPROTO_TCP, TCP_NODELAY, (char*)&on, sizeof(on)) < 0) {
            ereport(LOG, (errmsg("comm_setsockopt(TCP_NODELAY) failed: %m")));
            return STATUS_ERROR;
        }

        /* CommProxy Support */
        if (comm_setsockopt(port->sock, SOL_SOCKET, SO_KEEPALIVE, (char*)&on, sizeof(on)) < 0) {
            ereport(LOG, (errmsg("comm_setsockopt(SO_KEEPALIVE) failed: %m")));
            return STATUS_ERROR;
        }
#ifdef WIN32

        /*
         * This is a Win32 socket optimization.
         * The ideal buffer size is 32k for a Win32 socket at most efficiency.
         */
        on = PQ_SEND_BUFFER_SIZE * 4;

        /* CommProxy Support */
        if (comm_setsockopt(port->sock, SOL_SOCKET, SO_SNDBUF, (char*)&on, sizeof(on)) < 0) {
            ereport(LOG, (errmsg("comm_setsockopt(SO_SNDBUF) failed: %m")));
            return STATUS_ERROR;
        }
#endif

        /*
         * Also apply the current keepalive parameters.  If we fail to set a
         * parameter, don't error out, because these aren't universally
         * supported.  (Note: you might think we need to reset the GUC
         * variables to 0 in such a case, but it's not necessary because the
         * show hooks for these variables report the truth anyway.)
         */
        if (opval == IPPROTO_TCP) {
            (void)pq_setkeepalivesidle(u_sess->attr.attr_common.tcp_keepalives_idle, port);
            (void)pq_setkeepalivesinterval(u_sess->attr.attr_common.tcp_keepalives_interval, port);
            (void)pq_setkeepalivescount(u_sess->attr.attr_common.tcp_keepalives_count, port);
            (void)pq_settcpusertimeout(u_sess->attr.attr_common.tcp_user_timeout, port);
        }
    }

    return STATUS_OK;
}

/*
 * StreamClose -- close a client/backend connection
 *
 * NOTE: this is NOT used to terminate a session; it is just used to release
 * the file descriptor in a process that should no longer have the socket
 * open.  (For example, the postmaster calls this after passing ownership
 * of the connection to a child process.)  It is expected that someone else
 * still has the socket open.  So, we only want to close the descriptor,
 * we do NOT want to send anything to the far end.
 */
void StreamClose(pgsocket sock)
{
    /* CommProxy Support */
    comm_closesocket(sock);
}

/*
 * TouchSocketFileInternel & TouchSocketFile -- mark socket file as recently accessed
 *
 * This routine should be called every so often to ensure that the socket
 * file has a recent mod date (ordinary operations on sockets usually won't
 * change the mod date).  That saves it from being removed by
 * overenthusiastic /tmp-directory-cleaner daemons.  (Another reason we should
 * never have put the socket file in /tmp...)
 */
void TouchSocketFileInternel(const char* sock_path)
{
    /* Do nothing if we did not create a socket... */
    if (sock_path[0] != '\0') {
        /*
         * utime() is POSIX standard, utimes() is a common alternative. If we
         * have neither, there's no way to affect the mod or access time of
         * the socket :-(
         *
         * In either path, we ignore errors; there's no point in complaining.
         */
#ifdef HAVE_UTIME
        utime(sock_path, NULL);
#else /* !HAVE_UTIME */
#ifdef HAVE_UTIMES
        utimes(sock_path, NULL);
#endif /* HAVE_UTIMES */
#endif /* HAVE_UTIME */
    }
}

void TouchSocketFile(void)
{
    TouchSocketFileInternel(t_thrd.libpq_cxt.sock_path);
    TouchSocketFileInternel(t_thrd.libpq_cxt.ha_sock_path);
}

/* --------------------------------
 * Low-level I/O routines begin here.
 *
 * These routines communicate with a frontend client across a connection
 * already established by the preceding routines.
 * --------------------------------
 */

/* --------------------------------
 *			  pq_set_nonblocking - set socket blocking/non-blocking
 *
 * Sets the socket non-blocking if nonblocking is TRUE, or sets it
 * blocking otherwise.
 * --------------------------------
 */
void pq_set_nonblocking(bool nonblocking)
{
    if (g_instance.attr.attr_common.light_comm == FALSE) {
        if (u_sess->proc_cxt.MyProcPort->noblock == nonblocking) {
            return;
        }

#ifdef WIN32
        pgwin32_noblock = nonblocking ? 1 : 0;
#else

        /*
         * Use COMMERROR on failure, because ERROR would try to send the error to
         * the client, which might require changing the mode again, leading to
         * infinite recursion.
         */
        if (nonblocking) {
            if (!pg_set_noblock(u_sess->proc_cxt.MyProcPort->sock)) {
                ereport(COMMERROR, (errmsg("fd:[%d] could not set socket to non-blocking mode: %m", u_sess->proc_cxt.MyProcPort->sock)));
            }
        } else {
            if (!pg_set_block(u_sess->proc_cxt.MyProcPort->sock)) {
                ereport(COMMERROR, (errmsg("fd:[%d] could not set socket to blocking mode: %m", u_sess->proc_cxt.MyProcPort->sock)));
            }
        }
#endif
    }
    /*
     * For light_comm, we set socket to nonblocked mode initially (see pq_init()).
     * During the whole process of database communication, socket is not set to blocked mode any more.
     * Here, we just set the global variable to blocked if needed, without actually operating the socket,
     * For blocked semantics, use latch.
     */
    u_sess->proc_cxt.MyProcPort->noblock = nonblocking;
}

/* --------------------------------
 *		pq_recvbuf - load some bytes into the input buffer
 *
 *		returns 0 if OK, EOF if trouble
 * --------------------------------
 */
static int pq_recvbuf(void)
{
    if (t_thrd.libpq_cxt.PqRecvPointer > 0) {
        if (t_thrd.libpq_cxt.PqRecvLength > t_thrd.libpq_cxt.PqRecvPointer) {
            /* still some unread data, left-justify it in the buffer */
            errno_t rc = memmove_s(t_thrd.libpq_cxt.PqRecvBuffer,
                t_thrd.libpq_cxt.PqRecvLength - t_thrd.libpq_cxt.PqRecvPointer,
                t_thrd.libpq_cxt.PqRecvBuffer + t_thrd.libpq_cxt.PqRecvPointer,
                t_thrd.libpq_cxt.PqRecvLength - t_thrd.libpq_cxt.PqRecvPointer);
            securec_check(rc, "\0", "\0");
            t_thrd.libpq_cxt.PqRecvLength -= t_thrd.libpq_cxt.PqRecvPointer;
            t_thrd.libpq_cxt.PqRecvPointer = 0;
        } else {
            t_thrd.libpq_cxt.PqRecvLength = t_thrd.libpq_cxt.PqRecvPointer = 0;
        }
    }

    /* Ensure that we're in blocking mode */
    pq_set_nonblocking(false);

    /* Can fill buffer from PqRecvLength and upwards */
    for (;;) {
        int r;

        WaitState oldStatus = pgstat_report_waitstatus(STATE_WAIT_COMM);
        r = secure_read(u_sess->proc_cxt.MyProcPort,
            t_thrd.libpq_cxt.PqRecvBuffer + t_thrd.libpq_cxt.PqRecvLength,
            PQ_RECV_BUFFER_SIZE - t_thrd.libpq_cxt.PqRecvLength);
        (void)pgstat_report_waitstatus(oldStatus);

        if (r < 0) {
            if (errno == EINTR) {
                continue; /* Ok if interrupted */
            }

            /*
             * Careful: an ereport() that tries to write to the client would
             * cause recursion to here, leading to stack overflow and core
             * dump!  This message must go *only* to the postmaster log.
             */
            ereport(COMMERROR,
                (errcode_for_socket_access(), errmsg("could not receive data from client: %s", gs_comm_strerror())));
            return EOF;
        }
        if (r == 0) {
            /*
             * EOF detected.  We used to write a log message here, but it's
             * better to expect the ultimate caller to do that.
             */
            return EOF;
        }
        /* r contains number of bytes read, so just incr length */
        t_thrd.libpq_cxt.PqRecvLength += r;
        return 0;
    }
}

/* --------------------------------
 *		pq_getbyte	- get a single byte from connection, or return EOF
 * --------------------------------
 */
int pq_getbyte(void)
{
    while (t_thrd.libpq_cxt.PqRecvPointer >= t_thrd.libpq_cxt.PqRecvLength) {
        if (pq_recvbuf()) { /* If nothing in buffer, then recv some */
            return EOF;   /* Failed to recv data */
        }
    }
    return (unsigned char)t_thrd.libpq_cxt.PqRecvBuffer[t_thrd.libpq_cxt.PqRecvPointer++];
}

/* --------------------------------
 *		pq_peekbyte		- peek at next byte from connection
 *
 *	 Same as pq_getbyte() except we don't advance the pointer.
 * --------------------------------
 */
int pq_peekbyte(void)
{
    while (t_thrd.libpq_cxt.PqRecvPointer >= t_thrd.libpq_cxt.PqRecvLength) {
        if (pq_recvbuf()) {/* If nothing in buffer, then recv some */
            return EOF;   /* Failed to recv data */
        }
    }
    return (unsigned char)t_thrd.libpq_cxt.PqRecvBuffer[t_thrd.libpq_cxt.PqRecvPointer];
}

/* --------------------------------
 *		pq_getbyte_if_available - get a single byte from connection,
 *			if available
 *
 * The received byte is stored in *c. Returns 1 if a byte was read,
 * 0 if no data was available, or EOF if trouble.
 * --------------------------------
 */
int pq_getbyte_if_available(unsigned char* c)
{
    int r;

    if (t_thrd.libpq_cxt.PqRecvPointer < t_thrd.libpq_cxt.PqRecvLength) {
        *c = t_thrd.libpq_cxt.PqRecvBuffer[t_thrd.libpq_cxt.PqRecvPointer++];
        return 1;
    }

    /* Put the socket into non-blocking mode */
    pq_set_nonblocking(true);

    r = secure_read(u_sess->proc_cxt.MyProcPort, c, 1);
    if (r < 0) {
        /*
         * Ok if no data available without blocking or interrupted (though
         * EINTR really shouldn't happen with a non-blocking socket). Report
         * other errors.
         */
        if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR) {
            r = 0;
        } else {
            /*
             * Careful: an ereport() that tries to write to the client would
             * cause recursion to here, leading to stack overflow and core
             * dump!  This message must go *only* to the postmaster log.
             */
            ereport(COMMERROR,
                (errcode_for_socket_access(), errmsg("could not receive data from client: %s", gs_comm_strerror())));
            r = EOF;
        }
    } else if (r == 0) {
        /* EOF detected */
        r = EOF;
    }

    return r;
}

/* --------------------------------
 *		pq_getbytes		- get a known number of bytes from connection
 *
 *		returns 0 if OK, EOF if trouble
 * --------------------------------
 */
int pq_getbytes(char* s, size_t len)
{
    size_t amount;

    while (len > 0) {
        while (t_thrd.libpq_cxt.PqRecvPointer >= t_thrd.libpq_cxt.PqRecvLength) {
            if (pq_recvbuf()) { /* If nothing in buffer, then recv some */
                return EOF;   /* Failed to recv data */
            }
        }
        amount = t_thrd.libpq_cxt.PqRecvLength - t_thrd.libpq_cxt.PqRecvPointer;
        if (amount > len) {
            amount = len;
        }
        errno_t rc = memcpy_s(s, amount, t_thrd.libpq_cxt.PqRecvBuffer + t_thrd.libpq_cxt.PqRecvPointer, amount);
        securec_check(rc, "\0", "\0");

        t_thrd.libpq_cxt.PqRecvPointer += amount;
        s += amount;
        len -= amount;
    }

    return 0;
}

/* --------------------------------
 *		pq_discardbytes		- throw away a known number of bytes
 *
 *		same as pq_getbytes except we do not copy the data to anyplace.
 *		this is used for resynchronizing after read errors.
 *
 *		returns 0 if OK, EOF if trouble
 * --------------------------------
 */
int pq_discardbytes(size_t len)
{
    size_t amount;

    ereport(WARNING,
        (errcode(ERRCODE_PROTOCOL_VIOLATION),
            errmsg("incomplete message, client info [Remote IP: %s PORT: %s FD: %d BLOCK: %d], "
                "packet info [PqRecvLength: %d PqRecvPointer: %d expectedLen: %d].",
                u_sess->proc_cxt.MyProcPort->remote_host,
                (u_sess->proc_cxt.MyProcPort->remote_port != NULL &&
                 u_sess->proc_cxt.MyProcPort->remote_port[0] != '\0')
                ? u_sess->proc_cxt.MyProcPort->remote_port : "",
                u_sess->proc_cxt.MyProcPort->sock,
                u_sess->proc_cxt.MyProcPort->noblock,
                t_thrd.libpq_cxt.PqRecvLength,
                t_thrd.libpq_cxt.PqRecvPointer,
                (int)len)));
    PrintUnexpectedBufferContent(t_thrd.libpq_cxt.PqRecvBuffer, t_thrd.libpq_cxt.PqRecvLength);

    while (len > 0) {
        while (t_thrd.libpq_cxt.PqRecvPointer >= t_thrd.libpq_cxt.PqRecvLength) {
            if (pq_recvbuf()) { /* If nothing in buffer, then recv some */
                return EOF;   /* Failed to recv data */
            }
        }
        amount = t_thrd.libpq_cxt.PqRecvLength - t_thrd.libpq_cxt.PqRecvPointer;
        if (amount > len) {
            amount = len;
        }
        t_thrd.libpq_cxt.PqRecvPointer += amount;
        len -= amount;
    }
    return 0;
}

/* --------------------------------
 * pq_buffer_has_data - is any buffered data available to read?
 *
 * This will *not* attempt to read more data.
 * --------------------------------
 */
bool pq_buffer_has_data(void)
{
    return (t_thrd.libpq_cxt.PqRecvPointer < t_thrd.libpq_cxt.PqRecvLength);
}

/* --------------------------------
 *		pq_getstring	- get a null terminated string from connection
 *
 *		The return value is placed in an expansible StringInfo, which has
 *		already been initialized by the caller.
 *
 *		This is used only for dealing with old-protocol clients.  The idea
 *		is to produce a StringInfo that looks the same as we would get from
 *		pq_getmessage() with a newer client; we will then process it with
 *		pq_getmsgstring.  Therefore, no character set conversion is done here,
 *		even though this is presumably useful only for text.
 *
 *		returns 0 if OK, EOF if trouble
 * --------------------------------
 */
int pq_getstring(StringInfo s)
{
    int i;

    resetStringInfo(s);

    /* Read until we get the terminating '\0' */
    for (;;) {
        while (t_thrd.libpq_cxt.PqRecvPointer >= t_thrd.libpq_cxt.PqRecvLength) {
            if (pq_recvbuf()) {/* If nothing in buffer, then recv some */
                return EOF;   /* Failed to recv data */
            }
        }

        for (i = t_thrd.libpq_cxt.PqRecvPointer; i < t_thrd.libpq_cxt.PqRecvLength; i++) {
            if (t_thrd.libpq_cxt.PqRecvBuffer[i] == '\0') {
                /* include the '\0' in the copy */
                appendBinaryStringInfo(s,
                    t_thrd.libpq_cxt.PqRecvBuffer + t_thrd.libpq_cxt.PqRecvPointer,
                    i - t_thrd.libpq_cxt.PqRecvPointer + 1);
                t_thrd.libpq_cxt.PqRecvPointer = i + 1; /* advance past \0 */
                return 0;
            }
        }

        /* If we're here we haven't got the \0 in the buffer yet. */
        appendBinaryStringInfo(s,
            t_thrd.libpq_cxt.PqRecvBuffer + t_thrd.libpq_cxt.PqRecvPointer,
            t_thrd.libpq_cxt.PqRecvLength - t_thrd.libpq_cxt.PqRecvPointer);
        t_thrd.libpq_cxt.PqRecvPointer = t_thrd.libpq_cxt.PqRecvLength;
    }
}

/* --------------------------------
 *		pq_getmessage	- get a message with length word from connection
 *
 *		The return value is placed in an expansible StringInfo, which has
 *		already been initialized by the caller.
 *		Only the message body is placed in the StringInfo; the length word
 *		is removed.  Also, s->cursor is initialized to zero for convenience
 *		in scanning the message contents.
 *
 *		If maxlen is greater than zero, it is an upper limit on the length 
 *		of the message we are willing to accept.  We abort the connection 
 *		(by returning EOF) if client tries to send more than that.
 *
 *		returns 0 if OK, EOF if trouble
 * --------------------------------
 */
int pq_getmessage(StringInfo s, int maxlen)
{
    int32 len;

    resetStringInfo(s);

    /* Read message length word */
    if (pq_getbytes((char*)&len, 4) == EOF) {
        ereport(COMMERROR, (errcode(ERRCODE_PROTOCOL_VIOLATION), errmsg("unexpected EOF within message length word")));
        return EOF;
    }

    len = ntohl(len);

    if (len < 4 || (maxlen > 0 && len > maxlen)) {
        ereport(COMMERROR, (errcode(ERRCODE_PROTOCOL_VIOLATION), errmsg("invalid message length")));
        return EOF;
    }

    len -= 4; /* discount length itself */

    if (len > 0) {
        /*
         * Allocate space for message.	If we run out of room (ridiculously
         * large message), we will elog(ERROR), but we want to discard the
         * message body so as not to lose communication sync.
         */
        PG_TRY();
        {
            enlargeStringInfo(s, len);
        }
        PG_CATCH();
        {
            if (pq_discardbytes(len) == EOF) {
                ereport(COMMERROR, (errcode(ERRCODE_PROTOCOL_VIOLATION), errmsg("incomplete message from client")));
            }
            PG_RE_THROW();
        }
        PG_END_TRY();

        /* And grab the message */
        if (pq_getbytes(s->data, len) == EOF) {
            ereport(COMMERROR, (errcode(ERRCODE_PROTOCOL_VIOLATION), errmsg("incomplete message from client")));
            return EOF;
        }
        s->len = len;
        /* Place a trailing null per StringInfo convention */
        s->data[len] = '\0';
    }

    return 0;
}

/* --------------------------------
 *		pq_putbytes		- send bytes to connection (not flushed until pq_flush)
 *
 *		returns 0 if OK, EOF if trouble
 * --------------------------------
 */
int pq_putbytes(const char* s, size_t len)
{
    int res;

    /* Should only be called by old-style COPY OUT */
    Assert(t_thrd.libpq_cxt.DoingCopyOut);
    /* No-op if reentrant call */
    if (t_thrd.libpq_cxt.PqCommBusy) {
        return 0;
    }
    t_thrd.libpq_cxt.PqCommBusy = true;
    res = internal_putbytes(s, len);
    t_thrd.libpq_cxt.PqCommBusy = false;
    return res;
}

int internal_putbytes(const char* s, size_t len)
{
    size_t amount;

    while (len > 0) {
        /* If buffer is full, then flush it out */
        if (t_thrd.libpq_cxt.PqSendPointer >= t_thrd.libpq_cxt.PqSendBufferSize) {
            if (pq_disk_is_temp_file_enabled()) {
                /* create temp file to store the result, it is caller's responsibility
                 * to close the file done */
                if (!t_thrd.libpq_cxt.PqTempFileContextInfo->file_handle) {
                    pq_disk_create_tempfile();
                }
                if ((int)pq_disk_write_tempfile(
                        t_thrd.libpq_cxt.PqSendBuffer, ((size_t)t_thrd.libpq_cxt.PqSendPointer)) == EOF) {
                    return EOF;
                }
                t_thrd.libpq_cxt.PqSendPointer = 0;
            } else {
                StmtRetrySetFileExceededFlag(); /* once flush data to frontend, can not retry this query anymore */
                pq_set_nonblocking(false);
                if (internal_flush()) {
                    return EOF;
                }
            }
        }
        amount = t_thrd.libpq_cxt.PqSendBufferSize - t_thrd.libpq_cxt.PqSendPointer;
        if (amount > len) {
            amount = len;
        }
        errno_t rc = memcpy_s(t_thrd.libpq_cxt.PqSendBuffer + t_thrd.libpq_cxt.PqSendPointer, amount, s, amount);
        securec_check(rc, "\0", "\0");
        t_thrd.libpq_cxt.PqSendPointer += amount;
        s += amount;
        len -= amount;
    }

    return 0;
}

/* --------------------------------
 *		pq_flush		- flush pending output
 *
 *		returns 0 if OK, EOF if trouble
 * --------------------------------
 */
int pq_flush(void)
{
    int res = 0;

    /* No-op if reentrant call */
    if (t_thrd.libpq_cxt.PqCommBusy) {
        return res;
    }
    t_thrd.libpq_cxt.PqCommBusy = true;
    pq_set_nonblocking(false);

    if (t_thrd.libpq_cxt.save_query_result_to_disk &&
        (t_thrd.libpq_cxt.PqTempFileContextInfo->file_state == TEMPFILE_FLUSHED)) {
        if (!u_sess->wlm_cxt->spill_limit_error) {
            MemoryContext oldMemory;
            oldMemory = MemoryContextSwitchTo(SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_COMMUNICATION));

            /*
             * read query result from temp file, then flush to client.
             * extract remaining data in send buffer to disk,
             * in order to send total query result together through temp file
             */
            pq_disk_extract_sendbuffer();
            res = pq_disk_send_to_frontend();

            (void)MemoryContextSwitchTo(oldMemory);
        } else {
            pq_disk_discard_temp_file();
        }
    } else {
        res = internal_flush();
    }
    t_thrd.libpq_cxt.PqCommBusy = false;
    return res;
}

/* If query is been canceled, print information */
static void QueryCancelPrint()
{
    if (t_thrd.storage_cxt.cancel_from_timeout) {
        /* For STATEMENT、AUTOVACUUM、WLM、DEADLOCK timeout */
        ereport(LOG,
            (errcode(ERRCODE_QUERY_CANCELED),
                errmsg("canceling statement due to statement timeout"),
                ignore_interrupt(true)));
    } else {
        /* If node is coordinator, client is "user". And if node is datanode, client is "coordinator" */
        ereport(LOG,
            (errcode(ERRCODE_QUERY_CANCELED),
                errmsg("canceling statement due to %s request", IS_PGXC_DATANODE ? "coordinator" : "user"),
                ignore_interrupt(true)));
    }
    return;
}

/* If send message to client failed, set flag to true */
static void SetConnectionLostFlag()
{
    if ((StreamThreadAmI() == false) && (!t_thrd.proc_cxt.proc_exit_inprogress)) {
        /* For client connection, set ClientConnectionLost to true */
        t_thrd.int_cxt.ClientConnectionLost = 1;
        InterruptPending = 1;
    } else if (StreamThreadAmI()) {
        /* For stream connection, set StreamConnectionLost to true */
        t_thrd.int_cxt.StreamConnectionLost = 1;
    }
    return;
}

/* --------------------------------
 *		internal_flush - flush pending output
 *
 * Returns 0 if OK (meaning everything was sent, or operation would block
 * and the socket is in non-blocking mode), or EOF if trouble.
 * --------------------------------
 */
int internal_flush(void)
{
    if ((t_thrd.walsender_cxt.ep_fd != -1 && g_comm_proxy_config.s_send_xlog_mode == CommSendXlogWaitIn)) {
        return libnet_flush();
    }

    static THR_LOCAL int last_reported_send_errno = 0;

    errno_t ret;
    char* bufptr = t_thrd.libpq_cxt.PqSendBuffer + t_thrd.libpq_cxt.PqSendStart;
    char* bufend = t_thrd.libpq_cxt.PqSendBuffer + t_thrd.libpq_cxt.PqSendPointer;
    char connTimeInfoStr[INITIAL_EXPBUFFER_SIZE] = {'\0'};
    WaitState oldStatus = pgstat_report_waitstatus(STATE_WAIT_UNDEFINED, true);

    if (StreamThreadAmI() == false) {
        oldStatus = pgstat_report_waitstatus(STATE_WAIT_FLUSH_DATA);
    } else {
        /* Add node name to mark where to flush data for SCTP */
        oldStatus = pgstat_report_waitstatus_comm(STATE_WAIT_FLUSH_DATA,
            u_sess->proc_cxt.MyProcPort->libcomm_addrinfo->nodeIdx,
            -1,
            u_sess->stream_cxt.producer_obj->getParentPlanNodeId(),
            global_node_definition ? global_node_definition->num_nodes : -1);
    }

    /*
     * In session initialize process, client is connecting.
     * If reply messages failed, we record entire connecting time and print it in error message.
     */
    if (u_sess->clientConnTime_cxt.checkOnlyInConnProcess) {
        instr_time currentTime;
        INSTR_TIME_SET_CURRENT(u_sess->clientConnTime_cxt.connEndTime);
        currentTime = u_sess->clientConnTime_cxt.connEndTime;
        INSTR_TIME_SUBTRACT(currentTime, u_sess->clientConnTime_cxt.connStartTime);
        ret = sprintf_s(connTimeInfoStr, INITIAL_EXPBUFFER_SIZE,
            " Client connection consumes %lfms.", INSTR_TIME_GET_MILLISEC(currentTime));
        securec_check_ss(ret, "", "");
    }

    while (bufptr < bufend) {
        int r;

        r = secure_write(u_sess->proc_cxt.MyProcPort, bufptr, bufend - bufptr);
        if (unlikely(r == 0 && (StreamThreadAmI() == true || u_sess->proc_cxt.MyProcPort->is_logic_conn))) {
            /* Stop query when cancel happend */
            if (t_thrd.int_cxt.QueryCancelPending) {
                QueryCancelPrint();
                (void)pgstat_report_waitstatus(oldStatus);
                return EOF;
            } else {
                continue;
            }
        }

        if (unlikely(r <= 0)) {
            if (errno == EINTR) {
                continue; /* Ok if we were interrupted */
            }

            /*
             * Ok if no data writable without blocking, and the socket is in
             * non-blocking mode.
             */
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                (void)pgstat_report_waitstatus(oldStatus);
                return 0;
            }

            /*
             * Careful: an ereport() that tries to write to the client would
             * cause recursion to here, leading to stack overflow and core
             * dump!  This message must go *only* to the postmaster log.
             *
             * If a client disconnects while we're in the midst of output, we
             * might write quite a bit of data before we get to a safe query
             * abort point.  So, suppress duplicate log messages.
             */
            // if it is stream thread, suppress the error message.
            if (errno != last_reported_send_errno && StreamThreadAmI() == false) {
                last_reported_send_errno = errno;
                ereport(COMMERROR,
                    (errcode_for_socket_access(),
                        errmsg("could not send data to client [ Remote IP: %s PORT: %s FD: %d BLOCK: %d].%s Detail: %m",
                            u_sess->proc_cxt.MyProcPort->remote_host,
                            (u_sess->proc_cxt.MyProcPort->remote_port != NULL &&
                             u_sess->proc_cxt.MyProcPort->remote_port[0] != '\0')
                            ? u_sess->proc_cxt.MyProcPort->remote_port : "",
                            u_sess->proc_cxt.MyProcPort->sock,
                            u_sess->proc_cxt.MyProcPort->noblock,
                            connTimeInfoStr)));
            }

            /*
             * We drop the buffered data anyway so that processing can
             * continue, even though we'll probably quit soon. We also set a
             * flag that'll cause the next CHECK_FOR_INTERRUPTS to terminate
             * the connection.
             */
            t_thrd.libpq_cxt.PqSendStart = t_thrd.libpq_cxt.PqSendPointer = 0;
            SetConnectionLostFlag();
            (void)pgstat_report_waitstatus(oldStatus);
            return EOF;
        }

        last_reported_send_errno = 0; /* reset after any successful send */
        bufptr += r;
        t_thrd.libpq_cxt.PqSendStart += r;
    }

    t_thrd.libpq_cxt.PqSendStart = t_thrd.libpq_cxt.PqSendPointer = 0;
    (void)pgstat_report_waitstatus(oldStatus);
    return 0;
}

static int libnet_flush(void)
{
    char* bufptr = t_thrd.libpq_cxt.PqSendBuffer + t_thrd.libpq_cxt.PqSendStart;
    char* bufend = t_thrd.libpq_cxt.PqSendBuffer + t_thrd.libpq_cxt.PqSendPointer;
    WaitState oldStatus = pgstat_report_waitstatus(STATE_WAIT_UNDEFINED, true);

    int ep_fd = t_thrd.walsender_cxt.ep_fd;
    struct epoll_event events[512];
    int fd = u_sess->proc_cxt.MyProcPort->sock;
    int temp_len = 0;
    int r;
    do {
        int nevents = comm_epoll_wait(ep_fd, events, 512, 10000);
        for (int i = 0; i < nevents; ++i) {
            if (events[i].events & EPOLLOUT) {
                int event_fd = events[i].data.fd;
                if (event_fd == fd) {
                    r = comm_send(fd, bufptr, bufend - bufptr);
                    if (r > 0) {
                        bufptr += r;
                        t_thrd.libpq_cxt.PqSendStart += r;
                    } else if (temp_len < 0 && !IgnoreErrorNo(errno)) {
                        return EOF;
                    }
                } else {

                }
            }
        }
    } while (bufptr < bufend);

    t_thrd.libpq_cxt.PqSendStart = t_thrd.libpq_cxt.PqSendPointer = 0;
    (void)pgstat_report_waitstatus(oldStatus);
    return 0;
}


/* --------------------------------
 *		pq_flush_if_writable - flush pending output if writable without blocking
 *
 * Returns 0 if OK, or EOF if trouble.
 * --------------------------------
 */
int pq_flush_if_writable(void)
{
    int res;

    /* Quick exit if nothing to do */
    if (t_thrd.libpq_cxt.PqSendPointer == t_thrd.libpq_cxt.PqSendStart) {
        return 0;
    }

    /* No-op if reentrant call */
    if (t_thrd.libpq_cxt.PqCommBusy) {
        return 0;
    }

    /* Temporarily put the socket into non-blocking mode */
    pq_set_nonblocking(true);

    t_thrd.libpq_cxt.PqCommBusy = true;
    res = internal_flush();
    t_thrd.libpq_cxt.PqCommBusy = false;
    return res;
}

/* --------------------------------
 *		pq_flush_timedwait - Check if some data is pending to be flushed.
 *		If yes then call the existing non-block flush function to flush.
 *		If all datas are flushed (means PqSendStart is 0), then return
 *		Otherwise check even if at least few bytes of datas are flushed
 *		(by checking the before and after PqSendStart), if yes then
 *		update the last flush time otherwise check if any data was able
 *		to flush during maximum configured timeout.
 * --------------------------------
 */
void pq_flush_timedwait(int timeout)
{
    int sleeptime = 0;
    int send_start_before_flush = 0;
    TimestampTz start_time = 0;
    start_time = GetCurrentTimestamp();

    for (;;) {
        /* Check if still some data is pending to be sent */
        if (!pq_is_send_pending()) {
            break;
        }

        send_start_before_flush = t_thrd.libpq_cxt.PqSendStart;
        if (pq_flush_if_writable()) {
            ereport(COMMERROR, (errmsg("could not send data due to connection reset, terminating process")));
            proc_exit(0);
        }

        if (t_thrd.libpq_cxt.PqSendStart == 0) {
            /*
             * Means either nothing was flushed or
             * all datas are flushed. So loop back and see if
             * if any data to be send pending, if it zero because
             * everything was flushed then no data will be pending
             * to send otherwise it will be pending, so try to send
             * again. So here there is no need to reset the flush time
             */
            if (!pq_is_send_pending()) {
                break;
            }
        } else if (send_start_before_flush != t_thrd.libpq_cxt.PqSendStart) {
            /* Some more data have been flushed */
            sleeptime = 0;
            pg_usleep(NAPTIME_PER_SEND * 1000);
            continue;
        }

        if (timeout > 0 && sleeptime >= timeout) {
            long secs;
            int usecs;
            TimestampTz stop_time = GetCurrentTimestamp();

            TimestampDifference(start_time, stop_time, &secs, &usecs);
            sleeptime = secs * 1000 + usecs / 1000 + 1;

            /*
             * By checking the delayed time again, it ensures we won't delay
             * less than the specified time if pg_usleep is interrupted by other
             * signals such as SIGHUP.
             */
            if (stop_time < start_time || sleeptime >= timeout) {
                ereport(COMMERROR, (errmsg("could not send data during maximum timeout, terminating process")));
                proc_exit(0);
            }
        }

        pg_usleep(NAPTIME_PER_SEND_RETRY * 1000);
        sleeptime += NAPTIME_PER_SEND_RETRY;
    }
}

/* --------------------------------
 *		pq_is_send_pending	- is there any pending data in the output buffer?
 * --------------------------------
 */
bool pq_is_send_pending(void)
{
    return (t_thrd.libpq_cxt.PqSendStart < t_thrd.libpq_cxt.PqSendPointer);
}

/* --------------------------------
 * Message-level I/O routines begin here.
 *
 * These routines understand about the old-style COPY OUT protocol.
 * --------------------------------
 */

/* --------------------------------
 *		pq_putmessage	- send a normal message (suppressed in COPY OUT mode)
 *
 *		If msgtype is not '\0', it is a message type code to place before
 *		the message body.  If msgtype is '\0', then the message has no type
 *		code (this is only valid in pre-3.0 protocols).
 *
 *		len is the length of the message body data at *s.  In protocol 3.0
 *		and later, a message length word (equal to len+4 because it counts
 *		itself too) is inserted by this routine.
 *
 *		All normal messages are suppressed while old-style COPY OUT is in
 *		progress.  (In practice only a few notice messages might get emitted
 *		then; dropping them is annoying, but at least they will still appear
 *		in the postmaster log.)
 *
 *		We also suppress messages generated while pqcomm.c is busy.  This
 *		avoids any possibility of messages being inserted within other
 *		messages.  The only known trouble case arises if SIGQUIT occurs
 *		during a pqcomm.c routine --- quickdie() will try to send a warning
 *		message, and the most reasonable approach seems to be to drop it.
 *
 *		returns 0 if OK, EOF if trouble
 * --------------------------------
 */
int pq_putmessage(char msgtype, const char* s, size_t len)
{
    if (t_thrd.libpq_cxt.DoingCopyOut || t_thrd.libpq_cxt.PqCommBusy) {
        return 0;
    }
    t_thrd.libpq_cxt.PqCommBusy = true;
    if (msgtype) {
        if (internal_putbytes(&msgtype, 1)) {
            goto fail;
        }
    }
    if (PG_PROTOCOL_MAJOR(FrontendProtocol) >= 3) {
        uint32 n32;

        n32 = htonl((uint32)(len + 4));
        if (internal_putbytes((char*)&n32, 4)) {
            goto fail;
        }
    }
    if (internal_putbytes(s, len)) {
        goto fail;
    }
    t_thrd.libpq_cxt.PqCommBusy = false;
    return 0;

fail:
    t_thrd.libpq_cxt.PqCommBusy = false;
    return EOF;
}

/* --------------------------------
 *		pq_putmessage_noblock	- like pq_putmessage, but never blocks
 *
 *		If the output buffer is too small to hold the message, the buffer
 *		is enlarged.
 */
int pq_putmessage_noblock(char msgtype, const char* s, size_t len)
{
    int res;
    int required;
    const int datalen = 1 + 4 + len;
    /*
     * Ensure we have enough space in the output buffer for the message header
     * as well as the message itself.
     */
    Assert((unsigned int)t_thrd.libpq_cxt.PqSendPointer <= MaxBuildAllocSize);
    if (MaxBuildAllocSize - (unsigned int)t_thrd.libpq_cxt.PqSendPointer >= (unsigned int)datalen) {
        required = t_thrd.libpq_cxt.PqSendPointer + datalen;
        if (required > t_thrd.libpq_cxt.PqSendBufferSize) {
            t_thrd.libpq_cxt.PqSendBuffer = (char*)repalloc(t_thrd.libpq_cxt.PqSendBuffer, required);
            t_thrd.libpq_cxt.PqSendBufferSize = required;
        }
    }
    res = pq_putmessage(msgtype, s, len);
    return res;
}

/* --------------------------------
 *		pq_startcopyout - inform libpq that an old-style COPY OUT transfer
 *			is beginning
 * --------------------------------
 */
void pq_startcopyout(void)
{
    t_thrd.libpq_cxt.DoingCopyOut = true;
}

/* --------------------------------
 *		pq_endcopyout	- end an old-style COPY OUT transfer
 *
 *		If errorAbort is indicated, we are aborting a COPY OUT due to an error,
 *		and must send a terminator line.  Since a partial data line might have
 *		been emitted, send a couple of newlines first (the first one could
 *		get absorbed by a backslash...)  Note that old-style COPY OUT does
 *		not allow binary transfers, so a textual terminator is always correct.
 * --------------------------------
 */
void pq_endcopyout(bool errorAbort)
{
    if (!t_thrd.libpq_cxt.DoingCopyOut) {
        return;
    }
    if (errorAbort) {
        pq_putbytes("\n\n\\.\n", 5);
    }
    /* in non-error case, copy.c will have emitted the terminator line */
    t_thrd.libpq_cxt.DoingCopyOut = false;
}

/* --------------------------------
 *		pq_select	- Wait until we can read data, or timeout.
 *		Returns true if data has become available for reading, false if timed out
 *		or interrupted by signal.
 *		This is based on libpq_select of libpq_walreceiver.cpp.
 * --------------------------------
 */
bool pq_select(int timeout_ms)
{
    int ret;

    /* We use comm_poll(2) if available, otherwise select(2) */
    {
#ifdef HAVE_POLL
        struct pollfd input_fd;

        input_fd.fd = u_sess->proc_cxt.MyProcPort->sock;
        input_fd.events = POLLIN | POLLERR;
        input_fd.revents = 0;

        /* CommProxy Intefrace changes */
        ret = comm_poll(&input_fd, 1, timeout_ms);
#else  /* !HAVE_POLL */

        fd_set input_mask;
        struct timeval timeout;
        struct timeval* ptr_timeout = NULL;

        FD_ZERO(&input_mask);
        FD_SET(u_sess->proc_cxt.MyProcPort->sock, &input_mask);

        if (timeout_ms < 0) {
            ptr_timeout = NULL;
        } else {
            timeout.tv_sec = timeout_ms / 1000;
            timeout.tv_usec = (timeout_ms % 1000) * 1000;
            ptr_timeout = &timeout;
        }

        /* CommProxy Intefrace changes */
        ret = comm_select(u_sess->proc_cxt.MyProcPort->sock + 1, &input_mask, NULL, NULL, ptr_timeout);
#endif /* HAVE_POLL */
    }

    if (ret == 0 || (ret < 0 && errno == EINTR)) {
        return false;
    }
    if (ret < 0) {
        ereport(ERROR, (errcode_for_socket_access(), errmsg("comm_select() failed: %m")));
    }
    return true;
}

/*
 * Support for TCP Keepalive parameters
 */

/*
 * On Windows, we need to set both idle and interval at the same time.
 * We also cannot reset them to the default (setting to zero will
 * actually set them to zero, not default), therefor we fallback to
 * the out-of-the-box default instead.
 */
#if defined(WIN32) && defined(SIO_KEEPALIVE_VALS)
static int pq_setkeepaliveswin32(Port* port, int idle, int interval)
{
    struct tcp_keepalive ka;
    DWORD retsize;

    if (idle <= 0)
        idle = 2 * 60 * 60; /* default = 2 hours */
    if (interval <= 0)
        interval = 1; /* default = 1 second */

    ka.onoff = 1;
    ka.keepalivetime = idle * 1000;
    ka.keepaliveinterval = interval * 1000;

    if (WSAIoctl(port->sock, SIO_KEEPALIVE_VALS, (LPVOID)&ka, sizeof(ka), NULL, 0, &retsize, NULL, NULL) != 0) {
        ereport(LOG, (errmsg("WSAIoctl(SIO_KEEPALIVE_VALS) failed: %ui", WSAGetLastError())));
        return STATUS_ERROR;
    }
    if (port->keepalives_idle != idle)
        port->keepalives_idle = idle;
    if (port->keepalives_interval != interval)
        port->keepalives_interval = interval;
    return STATUS_OK;
}
#endif

int pq_getkeepalivesidle(Port* port)
{
#if defined(TCP_KEEPIDLE) || defined(TCP_KEEPALIVE) || defined(WIN32)
    if (port == NULL || IS_AF_UNIX(port->laddr.addr.ss_family)) {
        return 0;
    }

    if (port->keepalives_idle != 0) {
        return port->keepalives_idle;
    }

    if ((port->default_keepalives_idle == 0) && (port->sock != NO_SOCKET)) {
#ifndef WIN32
        ACCEPT_TYPE_ARG3 size = sizeof(port->default_keepalives_idle);

#ifdef TCP_KEEPIDLE
        /* CommProxy Intefrace changes */
        if (comm_getsockopt(port->sock, IPPROTO_TCP, TCP_KEEPIDLE, (char*)&port->default_keepalives_idle, &size) < 0) {
            ereport(LOG, (errmsg("comm_getsockopt(TCP_KEEPIDLE) failed: %m")));
            port->default_keepalives_idle = -1; /* don't know */
        }
#else
        /* CommProxy Intefrace changes */
        if (comm_getsockopt(port->sock, IPPROTO_TCP, TCP_KEEPALIVE, (char*)&port->default_keepalives_idle, &size) < 0) {
            ereport(LOG, (errmsg("comm_getsockopt(TCP_KEEPALIVE) failed: %m")));
            port->default_keepalives_idle = -1; /* don't know */
        }
#endif /* TCP_KEEPIDLE */
#else  /* WIN32 */
        /* We can't get the defaults on Windows, so return "don't know" */
        port->default_keepalives_idle = -1;
#endif /* WIN32 */
    }

    return port->default_keepalives_idle;
#else
    return 0;
#endif
}

int pq_setkeepalivesidle(int idle, Port* port)
{
    if (port == NULL || IS_AF_UNIX(port->laddr.addr.ss_family)) {
        return STATUS_OK;
    }

#if defined(TCP_KEEPIDLE) || defined(TCP_KEEPALIVE) || defined(SIO_KEEPALIVE_VALS)
    if (idle == port->keepalives_idle) {
        return STATUS_OK;
    }

#ifndef WIN32
    if (port->default_keepalives_idle <= 0) {
        if (pq_getkeepalivesidle(port) < 0) {
            if (idle == 0) {
                return STATUS_OK; /* default is set but unknown */
            } else {
                return STATUS_ERROR;
            }
        }
    }

    if (idle == 0) {
        idle = port->default_keepalives_idle;
    }

    if (port->sock != NO_SOCKET) {
#ifdef TCP_KEEPIDLE
        /* CommProxy Intefrace changes */
        if (comm_setsockopt(port->sock, IPPROTO_TCP, TCP_KEEPIDLE, (char*)&idle, sizeof(idle)) < 0) {
            ereport(LOG, (errmsg("comm_setsockopt(TCP_KEEPIDLE) failed: %m")));
            return STATUS_ERROR;
        }
#else
        /* CommProxy Intefrace changes */
        if (comm_setsockopt(port->sock, IPPROTO_TCP, TCP_KEEPALIVE, (char*)&idle, sizeof(idle)) < 0) {
            ereport(LOG, (errmsg("comm_setsockopt(TCP_KEEPALIVE) failed: %m")));
            return STATUS_ERROR;
        }
#endif
        port->keepalives_idle = idle;
    }

#else /* WIN32 */
    return pq_setkeepaliveswin32(port, idle, port->keepalives_interval);
#endif
#else /* TCP_KEEPIDLE || SIO_KEEPALIVE_VALS */
    if (idle != 0) {
        ereport(LOG, (errmsg("setting the keepalive idle time is not supported")));
        return STATUS_ERROR;
    }
#endif
    return STATUS_OK;
}

int pq_getkeepalivesinterval(Port* port)
{
#if defined(TCP_KEEPINTVL) || defined(SIO_KEEPALIVE_VALS)
    if (port == NULL || IS_AF_UNIX(port->laddr.addr.ss_family)) {
        return 0;
    }

    if (port->keepalives_interval != 0) {
        return port->keepalives_interval;
    }

    if ((port->default_keepalives_interval == 0) && (port->sock != NO_SOCKET)) {
#ifndef WIN32
        ACCEPT_TYPE_ARG3 size = sizeof(port->default_keepalives_interval);

        /* CommProxy Intefrace changes */
        if (comm_getsockopt(port->sock, IPPROTO_TCP, TCP_KEEPINTVL, (char*)&port->default_keepalives_interval, &size) < 0) {
            ereport(LOG, (errmsg("comm_getsockopt(TCP_KEEPINTVL) failed: %m")));
            port->default_keepalives_interval = -1; /* don't know */
        }
#else
        /* We can't get the defaults on Windows, so return "don't know" */
        port->default_keepalives_interval = -1;
#endif /* WIN32 */
    }

    return port->default_keepalives_interval;
#else
    return 0;
#endif
}

int pq_setkeepalivesinterval(int interval, Port* port)
{
    if (port == NULL || IS_AF_UNIX(port->laddr.addr.ss_family)) {
        return STATUS_OK;
    }

#if defined(TCP_KEEPINTVL) || defined(SIO_KEEPALIVE_VALS)
    if (interval == port->keepalives_interval) {
        return STATUS_OK;
    }

#ifndef WIN32
    if (port->default_keepalives_interval <= 0) {
        if (pq_getkeepalivesinterval(port) < 0) {
            if (interval == 0) {
                return STATUS_OK; /* default is set but unknown */
            } else {
                return STATUS_ERROR;
            }
        }
    }

    if (interval == 0) {
        interval = port->default_keepalives_interval;
    }

    if (port->sock != NO_SOCKET) {
        /* CommProxy Intefrace changes */
        if (comm_setsockopt(port->sock, IPPROTO_TCP, TCP_KEEPINTVL, (char*)&interval, sizeof(interval)) < 0) {
            ereport(LOG, (errmsg("comm_setsockopt(TCP_KEEPINTVL) failed: %m")));
            return STATUS_ERROR;
        }

        port->keepalives_interval = interval;
    }

#else /* WIN32 */
    return pq_setkeepaliveswin32(port, port->keepalives_idle, interval);
#endif
#else
    if (interval != 0) {
        ereport(LOG, (errmsg("comm_setsockopt(TCP_KEEPINTVL) not supported")));
        return STATUS_ERROR;
    }
#endif

    return STATUS_OK;
}

int pq_getkeepalivescount(Port* port)
{
#ifdef TCP_KEEPCNT
    if (port == NULL || IS_AF_UNIX(port->laddr.addr.ss_family)) {
        return 0;
    }

    if (port->keepalives_count != 0) {
        return port->keepalives_count;
    }

    if ((port->default_keepalives_count == 0) && (port->sock != NO_SOCKET)) {
        ACCEPT_TYPE_ARG3 size = sizeof(port->default_keepalives_count);

        /* CommProxy Intefrace changes */
        if (comm_getsockopt(port->sock, IPPROTO_TCP, TCP_KEEPCNT, (char*)&port->default_keepalives_count, &size) < 0) {
            ereport(LOG, (errmsg("comm_getsockopt(TCP_KEEPCNT) failed: %m")));
            port->default_keepalives_count = -1; /* don't know */
        }
    }

    return port->default_keepalives_count;
#else
    return 0;
#endif
}

int pq_setkeepalivescount(int count, Port* port)
{
    if (port == NULL || IS_AF_UNIX(port->laddr.addr.ss_family)) {
        return STATUS_OK;
    }

#ifdef TCP_KEEPCNT
    if (count == port->keepalives_count) {
        return STATUS_OK;
    }

    if (port->default_keepalives_count <= 0) {
        if (pq_getkeepalivescount(port) < 0) {
            if (count == 0) {
                return STATUS_OK; /* default is set but unknown */
            } else {
                return STATUS_ERROR;
            }
        }
    }

    if (count == 0) {
        count = port->default_keepalives_count;
    }

    if (port->sock != NO_SOCKET) {
        /* CommProxy Intefrace changes */
        if (setsockopt(port->sock, IPPROTO_TCP, TCP_KEEPCNT, (char*)&count, sizeof(count)) < 0) {
            ereport(LOG, (errmsg("comm_setsockopt(TCP_KEEPCNT) failed: %m")));
            return STATUS_ERROR;
        }

        port->keepalives_count = count;
    }

#else
    if (count != 0) {
        ereport(LOG, (errmsg("comm_setsockopt(TCP_KEEPCNT) not supported")));
        return STATUS_ERROR;
    }
#endif

    return STATUS_OK;
}

int pq_gettcpusertimeout(Port *port)
{
#ifdef TCP_USER_TIMEOUT
    if (port == NULL || IS_AF_UNIX(port->laddr.addr.ss_family)) {
        return 0;
    }

    if (port->tcp_user_timeout != 0) {
        return port->tcp_user_timeout;
    }

    if ((port->default_tcp_user_timeout == 0) && (port->sock != NO_SOCKET)) {
        ACCEPT_TYPE_ARG3 size = sizeof(port->default_tcp_user_timeout);

        if (comm_getsockopt(port->sock, IPPROTO_TCP, TCP_USER_TIMEOUT,
            (char *)&port->default_tcp_user_timeout, &size) < 0) {
            ereport(LOG, (errmsg("comm_getsockopt(TCP_USER_TIMEOUT) failed: %m")));
            port->default_tcp_user_timeout = -1;    /* don't know */
        }
    }

    return port->default_tcp_user_timeout;
#else
    return 0;
#endif
}

int pq_settcpusertimeout(int timeout, Port *port)
{
    if (port == NULL || IS_AF_UNIX(port->laddr.addr.ss_family)) {
        return STATUS_OK;
    }

#ifdef TCP_USER_TIMEOUT
    if (timeout == port->tcp_user_timeout) {
        return STATUS_OK;
    }

    if (port->default_tcp_user_timeout <= 0) {
        if (pq_gettcpusertimeout(port) < 0) {
            if (timeout == 0) {
                return STATUS_OK;    /* default is set but unknown */
            } else {
                return STATUS_ERROR;
            }
        }
    }

    if (timeout == 0) {
        timeout = port->default_tcp_user_timeout;
    }

    if (port->sock != NO_SOCKET) {
        if (comm_setsockopt(port->sock, IPPROTO_TCP, TCP_USER_TIMEOUT, (char *) &timeout, sizeof(timeout)) < 0) {
            ereport(LOG, (errmsg("comm_setsockopt(TCP_USER_TIMEOUT) %d failed: %m", port->sock)));
            return STATUS_ERROR;
        }

        port->tcp_user_timeout = timeout;
    }
#else
    if (timeout != 0) {
        ereport(LOG, (errmsg("comm_setsockopt(TCP_USER_TIMEOUT) not supported")));
        return STATUS_ERROR;
    }
#endif

    return STATUS_OK;
}

/*
 * @Description: reset send buffer cursors
 */
void pq_abandon_sendbuffer(void)
{
    t_thrd.libpq_cxt.PqSendPointer = 0;
    t_thrd.libpq_cxt.PqSendStart = 0;
}

/*
 * @Description: reset recv buffer cursors
 */
void pq_abandon_recvbuffer(void)
{
    t_thrd.libpq_cxt.PqRecvPointer = 0;
    t_thrd.libpq_cxt.PqRecvLength = 0;
}

/*
 * @Description: resize PqRecvBuffer
 */
void pq_resize_recvbuffer(int size)
{
#ifdef USE_RETRY_STUB
    elog(LOG,
        "%s %s  resize pqrecvbuffer from %d to %d",
        STUB_PRINT_PREFIX,
        STUB_PRINT_PREFIX_TYPE_S,
        t_thrd.libpq_cxt.PqRecvBufferSize,
        size);
#endif

    char* enlarged_buffer = (char*)MemoryContextAlloc(
        THREAD_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_COMMUNICATION), size);
    if (t_thrd.libpq_cxt.PqRecvBuffer != NULL) {
        /* since MemoryContextAlloc may fail, so alloc new memory first, then free old memory */
        pfree(t_thrd.libpq_cxt.PqRecvBuffer); 
        t_thrd.libpq_cxt.PqRecvBuffer = NULL;
    }
    t_thrd.libpq_cxt.PqRecvBuffer = enlarged_buffer;
    t_thrd.libpq_cxt.PqRecvBufferSize = size;
    t_thrd.libpq_cxt.PqRecvPointer = 0;
    t_thrd.libpq_cxt.PqRecvLength = 0;
}

/*
 * @Description: revert PqRecvBuffer to given data
 */
void pq_revert_recvbuffer(const char* data, int len)
{
    if (unlikely(data == NULL || len < 0 || t_thrd.libpq_cxt.PqRecvBufferSize < len)) {
        ereport(ERROR,
            (errmodule(MOD_CN_RETRY),
                errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("alert, failed in revert command buffer, invalid params data len %d pq buffer size %d",
                    len,
                    t_thrd.libpq_cxt.PqRecvBufferSize)));
    }

    errno_t rc = memcpy_s(t_thrd.libpq_cxt.PqRecvBuffer, t_thrd.libpq_cxt.PqRecvBufferSize, data, len);
    securec_check(rc, "", "");

    t_thrd.libpq_cxt.PqRecvPointer = 0;
    t_thrd.libpq_cxt.PqRecvLength = len;
}

/*
 * @Description: generate crc checking header
 * @in - src data and length and sequence number
 * @return - int seqnum + int datalength + pg_crc32 crc as char *.
 */
static void pq_disk_generate_checking_header(
    const char* src_data, StringInfo dest_data, uint32 data_len, uint32 seq_num)
{
    Assert(src_data != NULL);

    pq_sendint(dest_data, seq_num, 4);
    pq_sendint(dest_data, data_len, 4);

    /* Add CRC check. */
    pg_crc32 val_crc;
    INIT_CRC32(val_crc);

#ifdef USE_ASSERT_CHECKING
    COMP_CRC32(val_crc, src_data, data_len);
#endif

    FIN_CRC32(val_crc);
    pq_sendint(dest_data, val_crc, 4);
    appendBinaryStringInfo(dest_data, src_data, data_len);

    return;
}

/*
 * @Description: read data file and do crc checking
 * @in - src data and length
 * @return - pqSendBuf read size.
 */
static size_t pq_disk_read_data_block(
    LZ4File* file_handle, char* src_data, char* dest_data, uint32 data_len, uint32 seq_num)
{
    Assert(file_handle != NULL && src_data != NULL);

    errno_t rc = EOK;
    uint32 actual_crc_val;
    uint32 actual_seq_num = 0;
    uint32 actual_msg_len = 0;

    size_t read_len = LZ4FileRead(file_handle, src_data, data_len + CRC_HEADER);

    if (read_len < CRC_HEADER) {
        return 0;
    }

    read_len -= CRC_HEADER;

    rc = memcpy_s(&actual_seq_num, 4, src_data, 4);
    securec_check(rc, "\0", "\0");
    actual_seq_num = ntohl(actual_seq_num);
    src_data += 4;

    Assert(actual_seq_num == seq_num);

    if (actual_seq_num != seq_num) {
        src_data -= 4;
        pfree_ext(src_data);
        ereport(FATAL,
            (errmodule(MOD_CN_RETRY),
                errcode(ERRCODE_DATA_EXCEPTION),
                errmsg("expected message sequnce is %u, actual message sequence is %u", seq_num, actual_seq_num)));
    }

    rc = memcpy_s(&actual_msg_len, 4, src_data, 4);
    securec_check(rc, "\0", "\0");
    actual_msg_len = ntohl(actual_msg_len);
    src_data += 4;

    Assert(actual_msg_len == read_len);

    if (actual_msg_len != read_len) {
        src_data -= 8;
        pfree_ext(src_data);
        ereport(FATAL,
            (errmodule(MOD_CN_RETRY),
                errcode(ERRCODE_STRING_DATA_LENGTH_MISMATCH),
                errmsg("expected message length is %u, actual message length is %u", actual_msg_len, data_len)));
    }

    /* CRC check. */
    rc = memcpy_s(&actual_crc_val, 4, src_data, 4);
    securec_check(rc, "\0", "\0");
    actual_crc_val = ntohl(actual_crc_val);
    src_data += 4;

#ifdef USE_ASSERT_CHECKING
    pg_crc32 valcrc;
    INIT_CRC32(valcrc);
    COMP_CRC32(valcrc, src_data, actual_msg_len);
    FIN_CRC32(valcrc);

    if (!EQ_CRC32(valcrc, actual_crc_val)) {
        src_data -= 12;
        pfree_ext(src_data);
        ereport(FATAL,
            (errmodule(MOD_CN_RETRY),
                errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("expected crc is %u, actual crc is %u", actual_crc_val, valcrc)));
    }
#endif

    errno_t err_rc = memcpy_s(dest_data, read_len, src_data, read_len);
    securec_check(err_rc, "\0", "\0");
    src_data -= CRC_HEADER;

    return read_len;
}

/*
 * To facilitate fault locating, abnormal packets need to be recorded.
 * For security purposes, the maximum print length does not exceed the buffer length.
 */
void PrintUnexpectedBufferContent(const char *buffer, int len)
{
    if (buffer == NULL) {
        Assert(buffer);
        ereport(ERROR,
            (errmodule(MOD_COMM_PARAM),
                errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("PrintUnexpectedPqBufferContent: invalid params[buffer: null len: %d]", len)));
    }
    if (len <= 0) {
        return;
    }

    /* Line-by-line printing with a width limit of 1024 */
    const int nBytePrintLine = 1024;
    const int pqBufferSize = PQ_BUFFER_SIZE;
    const char defaultNullChar = ' ';
    char tmp[nBytePrintLine + 1] = {0};
    int i, idx = 0;
    len = (len > pqBufferSize) ? pqBufferSize : len;

    ereport(WARNING, (errmsg("--------buffer begin--------")));
    for (i = 0; i < len; i++) {
        idx = i % nBytePrintLine;

        if (buffer[i] != '\0') {
            tmp[idx] = buffer[i];
        } else {
            tmp[idx] = defaultNullChar;
        }
        if ((i + 1) % nBytePrintLine == 0) {
            ereport(WARNING, (errmsg("%s", tmp)));
        }
    }

    /* Output buffer with the remaining length. */
    if (len % nBytePrintLine != 0) {
        /* reset dirty data */
        if (idx <= nBytePrintLine) {
            errno_t rc = memset_s(tmp + idx + 1, nBytePrintLine - idx, 0, nBytePrintLine - idx);
            securec_check(rc, "\0", "\0");
        }
        ereport(WARNING, (errmsg("%s", tmp)));
    }
    ereport(WARNING, (errmsg("--------buffer end--------")));
    return;
}
