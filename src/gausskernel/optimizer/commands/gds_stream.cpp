/* -------------------------------------------------------------------------
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 *
 * gds_stream.cpp
 *
 * IDENTIFICATION
 *	  src/gausskernel/optimizer/commands/gds_stream.cpp
 *
 * -------------------------------------------------------------------------
 */
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/poll.h>

#include "commands/gds_stream.h"
#include "libpq/ip.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "storage/smgr/fd.h"
#include "miscadmin.h"
#include "port.h"
#include "libpq/pqformat.h"
#include "pgstat.h"


/* extend size for GDS stream input buffer */
#define GDS_STREAM_BUF_EXTEND_SIZE 8192

extern const char* GSFS_PREFIX;
extern const char* GSFSS_PREFIX;

void GDSUri::Trim(char* str)
{
    int len;
    char* end = NULL;
    char* cur = NULL;
    char* begin = str;
    int maxlen;

    if (!str)
        return;
    maxlen = strlen(str);

    while (begin != NULL && isspace((int)*begin))
        begin++;

    for (end = cur = begin; *cur != '\0'; cur++) {
        if (!isspace((int)*cur))
            end = cur;
    }

    len = end - begin + 1;
    errno_t rc;
    rc = memmove_s(str, maxlen, begin, len);
    securec_check(rc, "", "");
    str[len] = '\0';
}

void GDSUri::Parse(const char* uri)
{
    char* delimPos = NULL;
    char* ptr = NULL;
    errno_t rc = 0;

    if (!uri)
        return;

    m_uri = pstrdup(uri);
    GDSUri::Trim(m_uri);
    ptr = m_uri;

    /*
     * Get protocal
     * if no protocal found, we suppose that the url is a local path
     */
    delimPos = strstr(ptr, "://");
    if (delimPos != NULL) {
        m_protocol = pnstrdup(ptr, delimPos - ptr);
        ptr = delimPos + 3;
    } else {
        m_path = pstrdup(m_uri);
        return;
    }

    /* Get Path */
    delimPos = strstr(ptr, "/");
    if (delimPos != NULL) {
        m_path = pstrdup(delimPos);
        *delimPos = '\0';
    }

    /* Get port */
    delimPos = strstr(ptr, ":");
    if (delimPos != NULL) {
        char* port = delimPos + 1;
        m_port = atoi(port);
        *delimPos = '\0';
    }

    /* Get hostname */
    if (*ptr != '\0')
        m_host = pstrdup(ptr);

    rc = strncpy_s(m_uri, strlen(uri) + 1, uri, strlen(uri));
    securec_check(rc, "\0", "\0");
}

GDSStream::GDSStream() : m_uri(NULL), m_fd(-1), m_ssl_enable(false), m_ssl(NULL), m_inBuf(NULL), m_outBuf(NULL)
{
    /* at default connection without SSH */
    m_read = &GDSStream::InternalRead;
}

GDSStream::~GDSStream()
{
    Close();
}

/*
 * @Description: init SSL evn
 * @See also:
 */
void GDSStream::InitSSL(void)
{
    gs_openssl_cli_init_system();
    char ssl_dir[MAXPGPATH] = {0};
    /*
     * GDSStream will load the certificate files if using SSL,
     * including  root(cacert.pem) and client (client.crt) certificate
     * files. All the files are loacated in $GAUSSHOME/share/sslcert/gds/
     */
    char* homedir = gs_getenv_r("GAUSSHOME");
    if (NULL == homedir) {
        ereport(ERROR,
            (errmodule(MOD_SSL), errcode_for_file_access(), errmsg("env $GAUSSHOME not found, please set it first")));
    }
    char real_homedir[PATH_MAX + 1] = {'\0'};
    if (realpath(homedir, real_homedir) == NULL) {
        ereport(ERROR,
            (errmodule(MOD_EXECUTOR), errcode(ERRCODE_EXTERNAL_ROUTINE_INVOCATION_EXCEPTION),
            errmsg("Failed to obtain environment value $GAUSSHOME!"),
            errdetail("N/A"),
            errcause("Incorrect environment value."),
            erraction("Please refer to backend log for more details.")));
    }
    homedir = NULL;
    if (backend_env_valid(real_homedir, "GAUSSHOME") == false) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            errmsg("Incorrect backend environment variable $GAUSSHOME"),
            errdetail("Please refer to the backend instance log for the detail")));
    }
    /* Load the ROOT CA files */
    int nRet = snprintf_s(ssl_dir, MAXPGPATH, MAXPGPATH - 1, "%s/share/sslcert/gds/", real_homedir);
    securec_check_ss_c(nRet, "\0", "\0");

    if (m_ssl == NULL) {
        m_ssl = gs_openssl_cli_create();
    }

    gs_openssl_cli_setfiles(m_ssl, ssl_dir, "cacert.pem", "server.key", "server.crt");
    if (gs_openssl_cli_initialize_SSL(m_ssl, m_fd) < 0) {
        ereport(ERROR, (errmodule(MOD_SSL), errcode_for_file_access(), errmsg("failed to bind fd(%d) to ssl", m_fd)));
    }
}

void GDSStream::Initialize(const char* uri)
{
    m_inBuf = makeStringInfo();
    m_outBuf = makeStringInfo();
    m_uri = New(CurrentMemoryContext) GDSUri;

    m_uri->Parse(uri);
    VerifyAddr();
    m_fd = AllocateSocket(m_uri->m_host, m_uri->m_port);

    /* using gsfss protocal to identify the SSL certification and transfer */
    m_ssl_enable = false;
    if (strncmp(m_uri->m_protocol, GSFSS_PREFIX, 5) == 0) {
        InitSSL();
        m_ssl_enable = true;

        /* set read function for SSH connection */
        m_read = &GDSStream::InternalReadSSL;
    }

    if (!pg_set_noblock(m_fd)) {
        ereport(WARNING, (errmsg("could not set socket to non-blocking mode: %m")));
    }
}

void GDSStream::Close()
{
    if (m_fd > 0) {
        (void)FreeSocket(m_fd);
        m_fd = -1;
    }

    /* destroy ssl object */
    gs_openssl_cli_destroy(m_ssl);
    m_ssl = NULL;

    if (m_uri) {
        delete m_uri;
        m_uri = NULL;
    }

    if (m_inBuf) {
        pfree_ext(m_inBuf->data);
        pfree_ext(m_inBuf);
        m_inBuf = NULL;
    }

    if (m_outBuf) {
        pfree_ext(m_outBuf->data);
        pfree_ext(m_outBuf);
        m_outBuf = NULL;
    }
}

int GDSStream::Read()
{
    int retval;
#ifdef HAVE_POLL
    pollfd ufds[2];
    errno_t rc = memset_s(ufds, sizeof(ufds), 0, sizeof(ufds));
    securec_check(rc, "\0", "\0");
#else
    fd_set readfds;
    FD_ZERO(&readfds);
#endif

retry:
    /* Allow cancel/die interrupts to be processed while waiting */
    t_thrd.int_cxt.ImmediateInterruptOK = true;
    CHECK_FOR_INTERRUPTS();
#ifdef HAVE_POLL
    ufds[0].fd = m_fd;
    ufds[0].events = POLLIN | POLLPRI;
    retval = poll(ufds, 1, -1);
#else
    if ((m_fd + 1) > FD_SETSIZE) {
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("m_fd + 1 cannot be greater than FD_SETSIZE")));
    }
    FD_SET(m_fd, &readfds);
    retval = select(m_fd + 1, &readfds, NULL, NULL, -1);
#endif
    /* Disallow cancel/die interrupts while get out of input state */
    t_thrd.int_cxt.ImmediateInterruptOK = false;

    if (retval < 0) {
        /* error - retry if EINTR or EAGAIN */
        if (errno == EINTR || errno == EAGAIN)
            goto retry;
        ereport(ERROR,
            (errcode_for_socket_access(),
                errmsg("Unexpected EOF on GDS connection \"%s\": %m", m_uri->ToString()),
                errhint("The peer GDS may run in safety mode with SSL certification.")));
        return EOF;
    }

    /* make sure '{' matches '}', so source insight can correctly
     * parse and display all the other functions.
     */
#ifdef HAVE_POLL
    if ((uint32)ufds[0].revents & POLLIN) {
#else
    if (FD_ISSET(m_fd, &readfds)) {
#endif /* HAVE_POLL */
#ifdef HAVE_POLL
        ufds[0].revents = 0;
#endif /* HAVE_POLL */

        int read_status = (this->*m_read)();
        if (read_status == EOF || read_status < 0) {
            ereport(ERROR,
                (errcode_for_socket_access(),
                    errmsg("The peer GDS has performed an orderly shutdown on current connection."),
                    errhint("Please refer to GDS log for more details.")));
            return EOF;
        }
    }

    return 0;
}

int GDSStream::InternalRead()
{
    int nread = 0;

    PrepareReadBuf();

    if (m_fd < 0)
        ereport(ERROR, (errcode_for_socket_access(), errmsg("Bad socket.")));

retry:
    PGSTAT_INIT_TIME_RECORD();
    PGSTAT_START_TIME_RECORD();
    nread = recv(m_fd, m_inBuf->data + m_inBuf->len, m_inBuf->maxlen - m_inBuf->len, 0);
    END_NET_RECV_INFO(nread);
    if (nread < 0) {
        if (errno == EINTR)
            goto retry;
            /* Some systems return EAGAIN/EWOULDBLOCK for no data */
#ifdef EAGAIN
        if (errno == EAGAIN)
            return 0;
#endif
#if defined(EWOULDBLOCK) && (!defined(EAGAIN) || (EWOULDBLOCK != EAGAIN))
        if (errno == EWOULDBLOCK)
            return 0;
#endif
        ereport(ERROR,
            (errcode(ERRCODE_CONNECTION_RESET_BY_PEER),
                errmsg("Unexpected EOF on GDS connection \"%s\": %m", m_uri->ToString())));
        return -1;

    } else if (nread == 0)
        return EOF;

    m_inBuf->len += nread;
    Assert(m_inBuf->len <= m_inBuf->maxlen);
    return 1;
}

/*
 * @Description: read input data from OPENSSL buffer
 * @Return: 1, read data successfully;
 *          0, no data read, try it again;
 *         <0, exception found;
 * @See also:
 */
int GDSStream::InternalReadSSL(void)
{
    int nread = 0;
    int times = 0;
    int maxreq = 0;

    PrepareReadBuf();

    maxreq = m_inBuf->maxlen - m_inBuf->len;
    Assert(maxreq > 0);
    nread = gs_openssl_cli_read(m_ssl, m_inBuf->data + m_inBuf->len, maxreq);
    if (nread > 0) {
        ++times; /* read times from OPENSSL buffer */

        /* update buffer's used length */
        m_inBuf->len += nread;
        Assert(m_inBuf->len <= m_inBuf->maxlen);

        while (nread == maxreq) {
            /* expand reading buffer directly */
            enlargeStringInfo(m_inBuf, GDS_STREAM_BUF_EXTEND_SIZE);

            /* read OPENSSL buffer repeated, untils there is no data */
            maxreq = m_inBuf->maxlen - m_inBuf->len;
            Assert(maxreq > 0);
            nread = gs_openssl_cli_read(m_ssl, m_inBuf->data + m_inBuf->len, maxreq);
            if (nread > 0) {
                ++times; /* read times from OPENSSL buffer */

                /* update buffer's used length */
                m_inBuf->len += nread;
                Assert(m_inBuf->len <= m_inBuf->maxlen);
            }
        }
    }
    Assert(nread < maxreq);

    /* the first read fails, handle exception */
    if (0 == times) {
        if (nread == OPENSSL_CLI_EAGAIN) {
            return 0; /* retry to read again */
        }
        if (nread == 0) {
            ereport(LOG,
                (errmodule(MOD_SSL),
                    errcode_for_socket_access(),
                    errmsg(
                        "read no data at the first time of poll() from GDS connection \"%s\": %m", m_uri->ToString())));
            return EOF;
        }
        return nread;
    }

    Assert(times > 0);
    if (nread >= 0) {
        /* nread=0 means that we have read all the data in OPENSSL buffer */
        return 1;
    }
    if (nread == OPENSSL_CLI_EAGAIN) {
        ereport(LOG,
            (errmodule(MOD_SSL),
                errmsg("read no data in ssl buffer from GDS connection \"%s\": %m", m_uri->ToString())));
        return 1;
    }
    return nread;
}

/*
 * @Description: call this method before reading. it will check
 *   1. vacuum its reading buffer by moving data ahead
 *   2. extend input buffer if needed
 * @See also:
 */
void GDSStream::PrepareReadBuf(void)
{
    Assert(m_inBuf != NULL);

    if (m_inBuf->cursor < m_inBuf->len) {
        if (m_inBuf->cursor > 0) {
            errno_t rc = 0;
            /* vacuum its reading buffer */
            rc = memmove_s(
                m_inBuf->data, m_inBuf->maxlen, m_inBuf->data + m_inBuf->cursor, m_inBuf->len - m_inBuf->cursor);
            securec_check(rc, "", "");
            m_inBuf->len -= m_inBuf->cursor;
            m_inBuf->cursor = 0;
        }
    } else {
        /* reset reading buffer */
        m_inBuf->cursor = m_inBuf->len = 0;
    }

    /* expand input buffer for this reading if needed */
    if (m_inBuf->maxlen - m_inBuf->len < GDS_STREAM_BUF_EXTEND_SIZE) {
        enlargeStringInfo(m_inBuf, GDS_STREAM_BUF_EXTEND_SIZE);
    }
}

int GDSStream::Write(void* src, Size len)
{
    Assert(m_uri);

    char* bufptr = (char*)src;
    int ret = 0;

    if (!pg_set_block(m_fd)) {
        ereport(WARNING, (errmsg("could not set socket to blocking mode: %m")));
    }

    while ((Size)(bufptr - (char*)src) < len) {
        if (!m_ssl_enable) {
            t_thrd.int_cxt.ImmediateInterruptOK = true;
            ret = send(m_fd, bufptr, len - (bufptr - (char*)src), 0);
            t_thrd.int_cxt.ImmediateInterruptOK = false;
            if (ret < 0) {
                if (errno == EINTR)
                    continue;
                ereport(ERROR,
                    (errmodule(MOD_SSL),
                        errcode_for_socket_access(),
                        errmsg("Unexpected connection EOF from \"%s\":%m", m_uri->ToString())));
            }
        } else {
            ret = gs_openssl_cli_write(m_ssl, bufptr, len - (bufptr - (char*)src));
            if (ret < 0) {
                ereport(ERROR,
                    (errmodule(MOD_SSL),
                        errcode_for_socket_access(),
                        errmsg("exception from GDS \"%s\":%m", m_uri->ToString())));
            }
        }
        bufptr += ret;
    }

    if (!pg_set_noblock(m_fd)) {
        ereport(WARNING, (errmsg("could not set socket to non-blocking mode: %m")));
    }
    return len;
}

void GDSStream::Flush()
{
    if (m_outBuf->cursor < m_outBuf->len) {
        Write(m_outBuf->data + m_outBuf->cursor, m_outBuf->len - m_outBuf->cursor);
        resetStringInfo(m_outBuf);
    }
}

#define HAS_DATA_BUFFERED(buf)         \
    ((buf)->cursor + 4 < (buf)->len && \
       (size_t)((buf)->cursor + 4) + (size_t)ntohl(*(uint32*)((buf)->data + (buf)->cursor + 1)) < (size_t)(buf)->len)

int GDSStream::ReadMessage(StringInfoData& dst)
{
    int retval = 0;

    resetStringInfo(&dst);
    Assert(NULL != m_inBuf);
    while (!HAS_DATA_BUFFERED(m_inBuf) && (retval = this->Read()) != EOF)
        ;

    if (retval != EOF) {
        int length = ntohl(*(uint32*)(m_inBuf->data + m_inBuf->cursor + 1));

        if (length > m_inBuf->len - m_inBuf->cursor) {
            ereport(ERROR, (errmodule(MOD_GDS), errcode(ERRCODE_LOG),
                errmsg("Unexpected length of data coming supposedly from GDS. Could be an forged attack package.")));
        }
        if (length < 0 || (uint32)length > MaxAllocSize + 4) {
            ereport(ERROR,
                (errcode(ERRCODE_OPERATE_RESULT_NOT_EXPECTED),
                    errmsg("Message size exceeds the maximum allowed (%d)", (int)MaxAllocSize + 4)));
        }
        length += GDSCmdHeaderSize;
        appendBinaryStringInfo(&dst, m_inBuf->data + m_inBuf->cursor, length);
        m_inBuf->cursor += length;

    } else {
        ereport(ERROR, (errcode(ERRCODE_OPERATE_RESULT_NOT_EXPECTED), errmsg("Incomplete Message from GDS .")));
    }

    return dst.len;
}

void GDSStream::VerifyAddr()
{
    Assert(m_uri);
    if (!(m_uri->m_protocol != NULL &&
            (strncmp(m_uri->m_protocol, GSFS_PREFIX, 4) == 0 || strncmp(m_uri->m_protocol, GSFSS_PREFIX, 5) == 0) &&
            m_uri->m_host != NULL && m_uri->m_port >= 0)) {
        ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("invalid URI \"%s\"", m_uri->ToString())));
    }
}

static char* _WriteBegin(CmdBegin* cmd)
{
    WRITE_JSON_START(CmdBegin, cmd);
    WRITE_JSON_STRING(m_url);
    WRITE_JSON_UINT64(m_id);
    WRITE_JSON_INT(m_format);
    WRITE_JSON_INT(m_nodeType);
    WRITE_JSON_INT(m_escape);
    WRITE_JSON_STRING(m_eol);
    WRITE_JSON_INT(m_quote);
    WRITE_JSON_BOOL(m_header);
    WRITE_JSON_INT(m_nodeNum);
    WRITE_JSON_STRING(m_nodeName);
    WRITE_JSON_INT(m_fixSize);
    WRITE_JSON_STRING(m_prefix);
    WRITE_JSON_STRING(m_fileheader);
    WRITE_JSON_END();
}

void SerializeCmd(CmdBase* cmd, StringInfo buf)
{
    char cmdHeader[GDSCmdHeaderSize];
    uint32* lenPtr = (uint32*)&cmdHeader[1];
    char* body = NULL;
    uint32 len = 0;
    const int cmdtypeSize = 30;
    char cmdtype[cmdtypeSize];
    errno_t ret = 0;

    Assert(CurrentMemoryContext);
    cmdHeader[0] = cmd->m_type;

    switch (cmd->m_type) {
        case CMD_TYPE_BEGIN:
            body = _WriteBegin((CmdBegin*)cmd);
            if (body != NULL)
                len = strlen(body);
            else
                ereport(ERROR, (errcode(ERRCODE_OPERATE_RESULT_NOT_EXPECTED), errmsg("Invalid command to serialize.")));
            Assert(CurrentMemoryContext);
            ret = strcpy_s(cmdtype, cmdtypeSize, "CMD_TYPE_BEGIN_v1");
            securec_check(ret, "\0", "\0");
            break;
        case CMD_TYPE_REQ:
            len = 0;
            ret = strcpy_s(cmdtype, cmdtypeSize, "CMD_TYPE_REQ");
            securec_check(ret, "\0", "\0");
            break;
        case CMD_TYPE_END:
            len = 0;
            ret = strcpy_s(cmdtype, cmdtypeSize, "CMD_TYPE_END");
            securec_check(ret, "\0", "\0");
            break;
        case CMD_TYPE_REMOTELOG: {
            CmdRemoteLog* rdata = (CmdRemoteLog*)cmd;
            body = rdata->m_data;
            len = rdata->m_datasize;
            len += strlen(rdata->m_name);
            len += sizeof(rdata->m_datasize);
            *lenPtr = htonl(len);
            appendBinaryStringInfo(buf, cmdHeader, GDSCmdHeaderSize);
            pq_sendint32(buf, rdata->m_datasize);
            appendBinaryStringInfo(buf, rdata->m_name, strlen(rdata->m_name));
            appendBinaryStringInfo(buf, rdata->m_data, rdata->m_datasize);

            Assert(CurrentMemoryContext);
            ret = strcpy_s(cmdtype, cmdtypeSize, "CMD_TYPE_REMOTELOG");
            securec_check(ret, "\0", "\0");
        }
            return;
        default:
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("invalid GDS command: %d", cmd->m_type)));
    }

    Assert(CurrentMemoryContext);
    *lenPtr = htonl(len);
    appendBinaryStringInfo(buf, cmdHeader, GDSCmdHeaderSize);
    if (body != NULL)
        appendBinaryStringInfo(buf, body, len);

    Assert(CurrentMemoryContext);

    cmdtype[cmdtypeSize - 1] = '\0';
#ifdef ENABLE_MULTIPLE_NODES
    if (u_sess->attr.attr_storage.gds_debug_mod)
        ereport(LOG, (errmodule(MOD_GDS), (errmsg("Sending a %s to GDS", cmdtype))));
#endif
}

void PackData(StringInfo dst, StringInfo data)
{
    char cmdHeader[GDSCmdHeaderSize];
    uint32* lenPtr = (uint32*)&cmdHeader[1];

    cmdHeader[0] = CMD_TYPE_DATA;
    *lenPtr = htonl(data->len);
    appendBinaryStringInfo(dst, cmdHeader, GDSCmdHeaderSize);
    appendBinaryStringInfo(dst, data->data, data->len);
}

static int _ReadError(CmdError* cmd, const char* msg)
{
    READ_JSON_START(CmdError, cmd, msg);
    READ_JSON_INT(m_level);
    READ_JSON_STRING(m_detail);
    READ_JSON_END();
}

static int _ReadFileSwitch(CmdFileSwitch* cmd, const char* msg)
{
    READ_JSON_START(CmdFileSwitch, cmd, msg);
    READ_JSON_STRING(m_fileName);
    READ_JSON_END();
}

static int _ReadResponse(CmdResponse* cmd, const char* msg)
{
    READ_JSON_START(CmdResponse, cmd, msg);
    READ_JSON_INT(m_result);
    READ_JSON_STRING(m_reason);
    READ_JSON_END();
}

static int _ReadResult(CmdQueryResult* cmd, const char* msg)
{
    READ_JSON_START(CmdQueryResult, cmd, msg);
    READ_JSON_INT(m_result);
    READ_JSON_STRING(m_version_num);
    READ_JSON_END();
}

CmdBase* DeserializeCmd(StringInfo buf)
{
    /* 1 byte: message type */
    char type = *(buf->data + buf->cursor++);
    /* 4 bytes: message body length */
    uint32 length = *(uint32*)(buf->data + buf->cursor);
    CmdBase* cmd = NULL;
    char* strcmd = NULL; /* copy command string if needed */

    buf->cursor += sizeof(uint32);
    length = ntohl(length);

    if (length > (uint32)(buf->len - buf->cursor)) {
        ereport(ERROR, (errmodule(MOD_GDS), errcode(ERRCODE_LOG),
            errmsg("Unexpected length of data coming supposedly from GDS. Could be an forged attack package.")));
    }

    const int cmdtypeSize = 30;
    char cmdtype[cmdtypeSize];
    errno_t ret = 0;
    int errorCheck = 0;

    /* length bytes: message body data */
    switch (type) {
        case CMD_TYPE_ERROR:
            ret = strcpy_s(cmdtype, cmdtypeSize, "CMD_TYPE_ERROR");
            securec_check(ret, "\0", "\0");

            strcmd = pnstrdup(buf->data + buf->cursor, length);
            cmd = (CmdError*)palloc(sizeof(CmdError));
            errorCheck = _ReadError((CmdError*)cmd, strcmd);
            if (unlikely(errorCheck) != 0) {
                ereport(ERROR, (errmodule(MOD_GDS), errcode(ERRCODE_LOG),
                    errmsg("Failed to _ReadError in CMD_TYPE_ERROR, could be a forged package.")));
            }
            pfree_ext(strcmd);
            cmd->m_type = CMD_TYPE_ERROR;
            break;
        case CMD_TYPE_FILE_SWITCH:
            ret = strcpy_s(cmdtype, cmdtypeSize, "CMD_TYPE_FILE_SWITCH");
            securec_check(ret, "\0", "\0");

            strcmd = pnstrdup(buf->data + buf->cursor, length);
            cmd = (CmdFileSwitch*)palloc(sizeof(CmdFileSwitch));
            errorCheck = _ReadFileSwitch((CmdFileSwitch*)cmd, strcmd);
            if (unlikely(errorCheck) != 0) {
                ereport(ERROR, (errmodule(MOD_GDS), errcode(ERRCODE_LOG),
                    errmsg("Failed to _ReadFileSwitch in CMD_TYPE_FILE_SWITCH, could be a forged package.")));
            }
            pfree_ext(strcmd);
            cmd->m_type = CMD_TYPE_FILE_SWITCH;
            break;
        case CMD_TYPE_RESPONSE:
            ret = strcpy_s(cmdtype, cmdtypeSize, "CMD_TYPE_RESPONSE");
            securec_check(ret, "\0", "\0");

            strcmd = pnstrdup(buf->data + buf->cursor, length);
            cmd = (CmdResponse*)palloc(sizeof(CmdResponse));
            errorCheck = _ReadResponse((CmdResponse*)cmd, strcmd);
            if (unlikely(errorCheck) != 0) {
                ereport(ERROR, (errmodule(MOD_GDS), errcode(ERRCODE_LOG),
                    errmsg("Failed to _ReadResponse in CMD_TYPE_RESPONSE, could be a forged package.")));
            }
            pfree_ext(strcmd);
            cmd->m_type = CMD_TYPE_RESPONSE;
            break;
        case CMD_TYPE_END:
            ret = strcpy_s(cmdtype, cmdtypeSize, "CMD_TYPE_END");
            securec_check(ret, "\0", "\0");
            cmd = (CmdBase*)palloc(sizeof(CmdBase));
            cmd->m_type = CMD_TYPE_END;
            break;
        case CMD_TYPE_DATA:
        case CMD_TYPE_DATA_SEG:
            /*
             * this branch needn't call pnstrdup() to copy this message data,
             * because we don't care message body. we think the two message types
             * are the most, and it's worthy of this optimization.
             */
            ret = strcpy_s(cmdtype, cmdtypeSize, "CMD_TYPE_DATA");
            securec_check(ret, "\0", "\0");
            cmd = (CmdData*)palloc(sizeof(CmdData));
            ((CmdData*)cmd)->m_data = buf->data + buf->cursor;
            ((CmdData*)cmd)->m_len = length;
            cmd->m_type = type; /* CMD_TYPE_DATA or CMD_TYPE_DATA_SEG message type */
            break;
        case CMD_TYPE_QUERY_RESULT:
            ereport(ERROR,
                (errmodule(MOD_GDS),
                    errcode(ERRCODE_LOG),
                    errmsg("The corresponding GDS is of an older version. Please upgrade GDS to match the server "
                           "version.")));
            break;
        case CMD_TYPE_QUERY_RESULT_V1:
            ret = strcpy_s(cmdtype, cmdtypeSize, "CMD_TYPE_QUERY_RESULT");
            securec_check(ret, "\0", "\0");

            strcmd = pnstrdup(buf->data + buf->cursor, length);
            cmd = (CmdQueryResult*)palloc(sizeof(CmdQueryResult));
            errorCheck = _ReadResult((CmdQueryResult*)cmd, strcmd);
            if (unlikely(errorCheck) != 0) {
                ereport(ERROR, (errmodule(MOD_GDS), errcode(ERRCODE_LOG),
                    errmsg("Failed to _ReadResult in CMD_TYPE_QUERY_RESULT_V1, could be a forged package.")));
            }
            pfree_ext(strcmd);
            cmd->m_type = CMD_TYPE_QUERY_RESULT_V1;
            break;
        default: {
            ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                    errmsg("Failed to deserialize command, which type is %d", type)));
        } break;
    }

    buf->cursor += length;

    cmdtype[cmdtypeSize - 1] = '\0';
#ifdef ENABLE_MULTIPLE_NODES
    if (u_sess->attr.attr_storage.gds_debug_mod)
        ereport(LOG, (errmodule(MOD_GDS), (errmsg("Receiving a %s from GDS", cmdtype))));
#endif
    return cmd;
}
