/* -------------------------------------------------------------------------
 *
 * be-secure.cpp
 *    functions related to setting up a secure connection to the frontend.
 *    Secure connections are expected to provide confidentiality,
 *    message integrity and endpoint authentication.
 *
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *    src/common/backend/libpq/be-secure.cpp
 *
 *    Since the server static private key ($DataDir/server.key)
 *    will normally be stored unencrypted so that the database
 *    backend can restart automatically, it is important that
 *    we select an algorithm that continues to provide confidentiality
 *    even if the attacker has the server's private key.  Ephemeral
 *    DH (EDH) keys provide this, and in fact provide Perfect Forward
 *    Secrecy (PFS) except for situations where the session can
 *    be hijacked during a periodic handshake/renegotiation.
 *    Right now client certificates are always used (since the
 *    imposter will be unable to successfully complete renegotiation).
 *
 *    N.B., the static private key should still be protected to
 *    the largest extent possible, to minimize the risk of
 *    impersonations.
 *
 *    Another benefit of EDH is that it allows the backend and
 *    clients to use DSA keys.  DSA keys can only provide digital
 *    signatures, not encryption, and are often acceptable in
 *    jurisdictions where RSA keys are unacceptable.
 *
 *    The downside to EDH is that it makes it impossible to
 *    use ssldump(1) if there's a problem establishing an SSL
 *    session.  In this case you'll need to temporarily disable
 *    EDH by commenting out the callback.
 *
 *    ...
 *
 *    Because the risk of cryptanalysis increases as large
 *    amounts of data are sent with the same session key, the
 *    session keys are periodically renegotiated.
 *
 * -------------------------------------------------------------------------
 */

#include "postgres.h"
#include "knl/knl_variable.h"

#include <sys/stat.h>
#include <fcntl.h>
#ifdef HAVE_NETINET_TCP_H
#include <netinet/tcp.h>
#include <arpa/inet.h>
#endif

#ifdef USE_SSL

#include "openssl/err.h"
#include "openssl/ssl.h"
#include "openssl/rand.h"
#include "openssl/ossl_typ.h"
#include "openssl/sslerr.h"
#include "openssl/obj_mac.h"
#include "openssl/dh.h"
#include "openssl/bn.h"
#include "openssl/x509.h"
#include "openssl/x509_vfy.h"
#include "openssl/opensslconf.h"
#include "openssl/crypto.h"
#include "openssl/bio.h"
#endif /* USE_SSL */

#include "libpq/libpq.h"
#include "tcop/tcopprot.h"
#include "utils/memutils.h"
#include "libcomm/libcomm.h"
#include "miscadmin.h"
#include "cipher.h"
#include "pgstat.h"
#include "workload/workload.h"
#include "communication/commproxy_interface.h"

#ifdef USE_SSL
typedef enum DHKeyLength {
    DHKey768 = 1,
    DHKey1024,
    DHKey1536,
    DHKey2048,
    DHKey3072,
    DHKey4096,
    DHKey6144,
    DHKey8192
} DHKeyLength;

static void info_cb(const SSL* ssl, int type, int args);
static const char* SSLerrmessage(void);
static void set_user_config_ssl_ciphers(const char* sslciphers);
static void set_default_ssl_ciphers();
static void initialize_SSL(void);
static void secure_initialize(void);
static int open_server_SSL(Port*);
static void close_SSL(Port*);
static const char* SSLerrmessage(void);
static void init_server_ssl_passwd(SSL_CTX* pstContext);
static void check_permission_cipher_file(const char* parent_dir);
extern bool StreamThreadAmI();

static BIO_METHOD* my_BIO_s_socket(void);
static int SSL_set_fd_ex(Port* port, int fd);
static int my_sock_write(BIO* h, const char* buf, int size);
static int my_sock_read(BIO* h, char* buf, int size);
static char* ssl_cipher_list2string(const char* ciphers[], const int num);
static int SSL_CTX_set_cipher_list_ex(SSL_CTX* ctx, const char* ciphers[], const int num);
static DH* genDHKeyPair(DHKeyLength dhType);

extern THR_LOCAL unsigned char disable_pqlocking;

/* security ciphers suites in SSL connection */
static const char* ssl_ciphers_map[] = {
    TLS1_TXT_ECDHE_RSA_WITH_AES_128_GCM_SHA256,     /* TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256 */
    TLS1_TXT_ECDHE_RSA_WITH_AES_256_GCM_SHA384,     /* TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384 */
    TLS1_TXT_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,   /* TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256 */
    TLS1_TXT_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,   /* TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384 */
    /* The following are compatible with earlier versions of the client. */
    TLS1_TXT_DHE_RSA_WITH_AES_128_GCM_SHA256,       /* TLS_DHE_RSA_WITH_AES_128_GCM_SHA256, */
    TLS1_TXT_DHE_RSA_WITH_AES_256_GCM_SHA384,       /* TLS_DHE_RSA_WITH_AES_256_GCM_SHA384 */
    NULL};

#ifndef ENABLE_UT
static
#endif
bool g_server_crl_err = false;
#endif

const char* ssl_cipher_file = "server.key.cipher";
const char* ssl_rand_file = "server.key.rand";

/* ------------------------------------------------------------ */
/*                       Hardcoded values                       */
/* ------------------------------------------------------------ */
/*
 *  Hardcoded DH parameters, used in ephemeral DH keying.
 *  As discussed above, EDH protects the confidentiality of
 *  sessions even if the static private key is compromised,
 *  so we are *highly* motivated to ensure that we can use
 *  EDH even if the DBA... or an attacker... deletes the
 *  $DataDir/dh*.pem files.
 *
 *  We could refuse SSL connections unless a good DH parameter
 *  file exists, but some clients may quietly renegotiate an
 *  unsecured connection without fully informing the user.
 *  Very uncool.
 *
 *  Alternatively, the backend could attempt to load these files
 *  on startup if SSL is enabled - and refuse to start if any
 *  do not exist - but this would tend to piss off DBAs.
 *
 *  If you want to create your own hardcoded DH parameters
 *  for fun and profit, review "Assigned Number for SKIP
 *  Protocols" (http://www.skip-vpn.org/spec/numbers.html)
 *  for suggestions.
 */

/* ------------------------------------------------------------ */
/*           Procedures common to all secure sessions           */
/* ------------------------------------------------------------ */

/*
 *  Initialize global context
 */
static void secure_initialize(void)
{
#ifdef USE_SSL
    initialize_SSL();
#endif

    return ;
}

/*
 * Indicate if we have loaded the root CA store to verify certificates
 */
bool secure_loaded_verify_locations(void)
{
#ifdef USE_SSL
    return u_sess->libpq_cxt.ssl_loaded_verify_locations;
#endif

    return false;
}

/*
 *  Attempt to negotiate secure session.
 */
int secure_open_server(Port* port)
{
    int r = 0;

#ifdef USE_SSL
    secure_initialize();
    r = open_server_SSL(port);
#endif

    return r;
}

/*
 *  Close secure session.
 */
void secure_close(Port* port)
{
#ifdef USE_SSL
    /*
     * Free all resources about SSL, even though there is no SSL
     * connections. Since we always allocating resources at the
     * beginning of thread initialization.
     */
    close_SSL(port);
#endif
}

/*
 *  Read data from a secure connection.
 */
ssize_t secure_read(Port* port, void* ptr, size_t len)
{
    ssize_t n;

#ifdef USE_SSL
    if (port->ssl != NULL) {
        int err;

    rloop:
        errno = 0;
        ERR_clear_error();
        n = SSL_read(port->ssl, ptr, len);
        err = SSL_get_error(port->ssl, n);
        switch (err) {
            case SSL_ERROR_NONE:
                port->count += n;
                break;
            case SSL_ERROR_WANT_READ:
            case SSL_ERROR_WANT_WRITE:
                if (port->noblock) {
                    errno = EWOULDBLOCK;
                    n = -1;
                    break;
                }
#ifdef WIN32
                pgwin32_waitforsinglesocket(SSL_get_fd(port->ssl),
                    (err == SSL_ERROR_WANT_READ) ? (FD_READ | FD_CLOSE) : (FD_WRITE | FD_CLOSE),
                    INFINITE);
#endif
                goto rloop;
            case SSL_ERROR_SYSCALL:
                /* leave it to caller to ereport the value of errno */
                if (n != -1) {
                    errno = ECONNRESET;
                    n = -1;
                }
                break;
            case SSL_ERROR_SSL:
                ereport(COMMERROR,
                        (errcode(ERRCODE_PROTOCOL_VIOLATION),
                         errmsg("SSL error: %s, remote nodename %s", SSLerrmessage(), port->remote_hostname)));
                /* fall through */
            case SSL_ERROR_ZERO_RETURN:
                errno = ECONNRESET;
                n = -1;
                break;
            default:
                ereport(COMMERROR,
                        (errcode(ERRCODE_PROTOCOL_VIOLATION),
                         errmsg("unrecognized SSL error code: %d, remote nodename %s",
                                err, port->remote_hostname)));
                n = -1;
                break;
        }
    } else
#endif
    {
        if (port->is_logic_conn) {
            prepare_for_logic_conn_read();

            int producer;
        retry:
            NetWorkTimePollStart(t_thrd.pgxc_cxt.GlobalNetInstr);
            n = gs_wait_poll(&(port->gs_sock), 1, &producer, -1, false);
            NetWorkTimePollEnd(t_thrd.pgxc_cxt.GlobalNetInstr);
            /* no data but wake up, retry */
            if (n == 0) {
                logic_conn_read_check_ended();
                goto retry;
            }

            if (n > 0) {
                n = gs_recv(&(port->gs_sock), ptr, len);
                LIBCOMM_DEBUG_LOG("secure_read to node %s[nid:%d,sid:%d] with msg:%c, len:%d.",
                                  port->remote_hostname,
                                  port->gs_sock.idx,
                                  port->gs_sock.sid,
                                  ((char*)ptr)[0],
                                  (int)len);
            }
            logic_conn_read_check_ended();
        } else {
            prepare_for_client_read();
            PGSTAT_INIT_TIME_RECORD();
            PGSTAT_START_TIME_RECORD();

            /* CommProxy Interface Support */
            n = comm_recv(port->sock, ptr, len, 0);
            END_NET_RECV_INFO(n);
            client_read_ended();
        }
    }

    /* for log printing, dn receive message */
    IPC_PERFORMANCE_LOG_COLLECT(port->msgLog, ptr, n, port->remote_hostname, &port->gs_sock, SECURE_READ);
    return n;
}

/*
 *  Write data to a secure connection.
 */
ssize_t secure_write(Port* port, void* ptr, size_t len)
{
    ssize_t n;
    StreamTimeSendStart(t_thrd.pgxc_cxt.GlobalNetInstr);
#ifdef USE_SSL
    if (port->ssl != NULL) {
        int err;
    wloop:
        errno = 0;
        ERR_clear_error();
        n = SSL_write(port->ssl, ptr, len);

        err = SSL_get_error(port->ssl, n);
        switch (err) {
            case SSL_ERROR_NONE:
                port->count += n;
                break;
            case SSL_ERROR_WANT_READ:
            case SSL_ERROR_WANT_WRITE:
                if (port->noblock) {
                    errno = EWOULDBLOCK;
                    n = -1;
                    break;
                }
#ifdef WIN32
                pgwin32_waitforsinglesocket(SSL_get_fd(port->ssl),
                    (err == SSL_ERROR_WANT_READ) ? (FD_READ | FD_CLOSE) : (FD_WRITE | FD_CLOSE),
                    INFINITE);
#endif
                goto wloop;
            case SSL_ERROR_SYSCALL:
                /* leave it to caller to ereport the value of errno */
                if (n != -1) {
                    errno = ECONNRESET;
                    n = -1;
                }
                break;
            case SSL_ERROR_SSL:
                ereport(COMMERROR, (errcode(ERRCODE_PROTOCOL_VIOLATION), errmsg("SSL error: %s", SSLerrmessage())));
                /* fall through */
            case SSL_ERROR_ZERO_RETURN:
                errno = ECONNRESET;
                n = -1;
                break;
            default:
                ereport(
                    COMMERROR, (errcode(ERRCODE_PROTOCOL_VIOLATION), errmsg("unrecognized SSL error code: %d", err)));
                n = -1;
                break;
        }
    } else
#endif
        /*
         * for stream connection, when send msgs
         * to multiple connections(broadcasting),
         * gs_broadcast_send is called
         * in this function the remaining connections
         * will not be blocked when one connection is waitting quota.
         */
        if (StreamThreadAmI()) {
            if(port->libcomm_addrinfo->parallel_send_mode) {
                n = gs_broadcast_send(port->libcomm_addrinfo, (char*)ptr, len, -1);
                IPC_PERFORMANCE_LOG_COLLECT(port->msgLog, ptr, n, "all datanodes", NULL, SECURE_WRITE);
            } else {
                n = gs_send(&(port->libcomm_addrinfo->gs_sock), (char*)ptr, len, -1, TRUE);
                IPC_PERFORMANCE_LOG_COLLECT(port->msgLog, ptr, n, port->libcomm_addrinfo->nodename,
                    &(port->libcomm_addrinfo->gs_sock), SECURE_WRITE);
            }
    }
    /*
     * for logic connection, gs_send is called
     * as only one connection needed to send.
     */
    else if (port->is_logic_conn) {
        n = gs_send(&(port->gs_sock), (char*)ptr, len, -1, TRUE);
        LIBCOMM_DEBUG_LOG("secure_write to node[nid:%d,sid:%d] with msg:%c, len:%d.",
            port->gs_sock.idx,
            port->gs_sock.sid,
            ((char*)ptr)[0],
            (int)len);

        /* for log printing, send message */
        IPC_PERFORMANCE_LOG_COLLECT(port->msgLog, ptr, n, port->remote_hostname, &port->gs_sock, SECURE_WRITE);
    } else {
        PGSTAT_INIT_TIME_RECORD();
        PGSTAT_START_TIME_RECORD();

        /* CommProxy Interface Support */
        n = comm_send(port->sock, ptr, len, 0);
        PGSTAT_END_TIME_RECORD(NET_SEND_TIME);
        END_NET_SEND_INFO(n);
        
        /* for log printing, send message */
        IPC_PERFORMANCE_LOG_COLLECT(port->msgLog, ptr, n, port->remote_hostname, NULL, SECURE_WRITE);
    }

    StreamTimeSendEnd(t_thrd.pgxc_cxt.GlobalNetInstr);

    return n;
}

/* ------------------------------------------------------------ */
/*                        SSL specific code                     */
/* ------------------------------------------------------------ */
#ifdef USE_SSL

/*
 *  Certificate verification callback
 *
 *  This callback allows us to log intermediate problems during
 *  verification, but for now we'll see if the final error message
 *  contains enough information.
 *
 *  This callback also allows us to override the default acceptance
 *  criteria (e.g., accepting self-signed or expired certs), but
 *  for now we accept the default checks.
 */
int be_verify_cb(int ok, X509_STORE_CTX* ctx)
{
    if (ok) {
        return ok;
    }

    /* 
    * When the CRL is abnormal, it won't be used to check whether the certificate is revoked,
    * and the services shouldn't be affected due to the CRL exception.
    */
    const int crl_err_scenarios[] = {
        X509_V_ERR_CRL_HAS_EXPIRED,
        X509_V_ERR_UNABLE_TO_GET_CRL,
        X509_V_ERR_UNABLE_TO_DECRYPT_CRL_SIGNATURE,
        X509_V_ERR_CRL_SIGNATURE_FAILURE,
        X509_V_ERR_CRL_NOT_YET_VALID,
        X509_V_ERR_ERROR_IN_CRL_LAST_UPDATE_FIELD,
        X509_V_ERR_ERROR_IN_CRL_NEXT_UPDATE_FIELD,
        X509_V_ERR_UNABLE_TO_GET_CRL_ISSUER,
        X509_V_ERR_KEYUSAGE_NO_CRL_SIGN,
        X509_V_ERR_UNHANDLED_CRITICAL_CRL_EXTENSION,
        X509_V_ERR_DIFFERENT_CRL_SCOPE,
        X509_V_ERR_CRL_PATH_VALIDATION_ERROR
    };
    bool ignore_crl_err = false;

    int err_code = X509_STORE_CTX_get_error(ctx);
    const char *err_msg = X509_verify_cert_error_string(err_code);
    if (!g_server_crl_err) {
        for (size_t i = 0; i < sizeof(crl_err_scenarios) / sizeof(crl_err_scenarios[0]); i++) {
            if (err_code == crl_err_scenarios[i]) {
                ereport(LOG,
                    (errmsg("During SSL authentication, there are some errors in the CRL, so we just ignore the CRL. "
                        "{ssl err code: %d, ssl err message: %s}\n", err_code, err_msg)));
                
                g_server_crl_err = true;
                ignore_crl_err = true;
                break;
            }
        }
    } else {
        if (err_code == X509_V_ERR_CERT_REVOKED) {
            g_server_crl_err = false; /* reset */
            ignore_crl_err = true;
        }
    }

    if (ignore_crl_err) {
        X509_STORE_CTX_set_error(ctx, X509_V_OK);
        ok = 1;
    }

    return ok;
}

/*
 *  This callback is used to copy SSL information messages
 *  into the openGauss log.
 */
static void info_cb(const SSL* ssl, int type, int args)
{
    switch (type) {
        case SSL_CB_HANDSHAKE_START:
            ereport(DEBUG4, (errmsg_internal("SSL: handshake start")));
            break;
        case SSL_CB_HANDSHAKE_DONE:
            ereport(DEBUG4, (errmsg_internal("SSL: handshake done")));
            break;
        case SSL_CB_ACCEPT_LOOP:
            ereport(DEBUG4, (errmsg_internal("SSL: accept loop")));
            break;
        case SSL_CB_ACCEPT_EXIT:
            ereport(DEBUG4, (errmsg_internal("SSL: accept exit (%d)", args)));
            break;
        case SSL_CB_CONNECT_LOOP:
            ereport(DEBUG4, (errmsg_internal("SSL: connect loop")));
            break;
        case SSL_CB_CONNECT_EXIT:
            ereport(DEBUG4, (errmsg_internal("SSL: connect exit (%d)", args)));
            break;
        case SSL_CB_READ_ALERT:
            ereport(DEBUG4, (errmsg_internal("SSL: read alert (0x%04x)", args)));
            break;
        case SSL_CB_WRITE_ALERT:
            ereport(DEBUG4, (errmsg_internal("SSL: write alert (0x%04x)", args)));
            break;
        default:
            break;
    }
}

/*
 *  Close SSL connection.
 */
static void close_SSL(Port* port)
{
    if (port->ssl != NULL) {
        SSL_shutdown(port->ssl);
        SSL_free(port->ssl);
        port->ssl = NULL;
    }

    if (port->peer != NULL) {
        X509_free(port->peer);
        port->peer = NULL;
    }

    if (port->peer_cn != NULL) {
        pfree(port->peer_cn);
        port->peer_cn = NULL;
    }

    /* Free SSL context */
    if (u_sess->libpq_cxt.SSL_server_context != NULL) {
        SSL_CTX_free(u_sess->libpq_cxt.SSL_server_context);
        u_sess->libpq_cxt.SSL_server_context = NULL;
    }
}
/*
 * Brief        :set_default_ssl_ciphers,set default ssl ciphers
 * Description  : SEC.CNF.004
 */
static void set_default_ssl_ciphers()
{
    int default_ciphers_count = 0;

    for (int i = 0; ssl_ciphers_map[i] != NULL; i++) {
        default_ciphers_count++;
    }

    if (SSL_CTX_set_cipher_list_ex(u_sess->libpq_cxt.SSL_server_context, ssl_ciphers_map, default_ciphers_count) != 1) {
        ereport(FATAL, (errmsg("could not set the cipher list (no valid ciphers available)")));
    }
}
/*
 * Brief            : set_user_config_ssl_ciphers,set the specified ssl ciphers by user
 * Description  : SEC.CNF.004
 */
static void set_user_config_ssl_ciphers(const char* sslciphers)
{
    char* cipherStr = NULL;
    char* cipherStr_tmp = NULL;
    char* token = NULL;
    int counter = 1;
    char** ciphers_list = NULL;
    bool find_ciphers_in_list = false;
    int i = 0;
    char* ptok = NULL;
    if (sslciphers == NULL) {
        ereport(ERROR, (errmsg("sslciphers can not be null")));
    } else {
        cipherStr = (char*)strchr(sslciphers, ';'); /*if the sslciphers does not contain character ';',the count is 1*/
        while (cipherStr != NULL) {
            counter++;
            cipherStr++;
            if (*cipherStr == '\0') {
                break;
            }
            cipherStr = strchr(cipherStr, ';');
        }
        ciphers_list = (char**)palloc(counter * sizeof(char*));

        Assert(ciphers_list != NULL);
        if (ciphers_list == NULL) {
            ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("malloc failed")));
        }

        cipherStr_tmp = pstrdup(sslciphers);
        if (cipherStr_tmp == NULL) {
            if (ciphers_list != NULL)
                pfree(ciphers_list);
            ciphers_list = NULL;
            ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("malloc failed")));
        }
        token = strtok_r(cipherStr_tmp, ";", &ptok);
        while (token != NULL) {
            for (int j = 0; ssl_ciphers_map[j] != NULL; j++) {
                if (strlen(ssl_ciphers_map[j]) == strlen(token) &&
                    strncmp(ssl_ciphers_map[j], token, strlen(token)) == 0) {
                    ciphers_list[i] = (char*)ssl_ciphers_map[j];
                    find_ciphers_in_list = true;
                    break;
                }
            }
            if (!find_ciphers_in_list) {
                errno_t errorno = EOK;
                const int maxCipherStrLen = 64;
                char errormessage[maxCipherStrLen] = {0};
                errorno = strncpy_s(errormessage, sizeof(errormessage), token, sizeof(errormessage) - 1);
                securec_check(errorno, cipherStr_tmp, ciphers_list, "\0");
                errormessage[maxCipherStrLen - 1] = '\0';
                if (cipherStr_tmp != NULL) {
                    pfree(cipherStr_tmp);
                    cipherStr_tmp = NULL;
                }
                if (ciphers_list != NULL) {
                    pfree(ciphers_list);
                    ciphers_list = NULL;
                }
                ereport(ERROR, (errmsg("unrecognized ssl ciphers name: \"%s\"", errormessage)));
            }
            token = strtok_r(NULL, ";", &ptok);
            i++;
            find_ciphers_in_list = false;
        }
    }
    if (SSL_CTX_set_cipher_list_ex(u_sess->libpq_cxt.SSL_server_context, (const char**)ciphers_list, counter) != 1) {
        if (cipherStr_tmp != NULL) {
            pfree(cipherStr_tmp);
            cipherStr_tmp = NULL;
        }
        if (ciphers_list != NULL) {
            pfree(ciphers_list);
            ciphers_list = NULL;
        }
        ereport(FATAL, (errmsg("could not set the cipher list (no valid ciphers available)")));
    }
    if (cipherStr_tmp != NULL) {
        pfree(cipherStr_tmp);
        cipherStr_tmp = NULL;
    }
    if (ciphers_list != NULL) {
        pfree(ciphers_list);
        ciphers_list = NULL;
    }
}

/*
 * Brief            : static void initialize_SSL(void)
 * Description  : Initialize global SSL context.
 */
static void initialize_SSL(void)
{
    /* Already initialized SSL, return here */
    if (u_sess->libpq_cxt.ssl_initialized) {
        return;
    }

#define MAX_CERTIFICATE_DEPTH_SUPPORTED 20 /* The max certificate depth supported. */

    struct stat buf;
    STACK_OF(X509_NAME)* root_cert_list = NULL;
    errno_t errorno = EOK;

    if (!u_sess->libpq_cxt.SSL_server_context) {
        /* Sets the SSL library for thread safe running*/
        (void)OPENSSL_init_ssl(0, NULL);
        SSL_load_error_strings();

        u_sess->libpq_cxt.SSL_server_context = SSL_CTX_new(TLS_method());
        if (!u_sess->libpq_cxt.SSL_server_context) {
            ereport(FATAL, (errmsg("could not create SSL context : %s.)", SSLerrmessage())));
        }

        /*
         * Disable moving-write-buffer sanity check, because it
         * causes unnecessary failures in nonblocking send cases.
         */
        SSL_CTX_set_mode(u_sess->libpq_cxt.SSL_server_context, SSL_MODE_ACCEPT_MOVING_WRITE_BUFFER);

        /* set the default password for certificate/private key loading */
        init_server_ssl_passwd(u_sess->libpq_cxt.SSL_server_context);
        
        /* Load and verify server's certificate and private key*/
        if (SSL_CTX_use_certificate_chain_file(
                u_sess->libpq_cxt.SSL_server_context, g_instance.attr.attr_security.ssl_cert_file) != 1) {
            ereport(FATAL,
                (errcode(ERRCODE_CONFIG_FILE_ERROR),
                    errmsg("could not load server certificate file \"%s\": %s",
                        g_instance.attr.attr_security.ssl_cert_file,
                        SSLerrmessage())));
        }
        /* check certificate file permission */
#if !defined(WIN32) && !defined(__CYGWIN__)
        if (stat(g_instance.attr.attr_security.ssl_cert_file, &buf) == 0){
            if (!S_ISREG(buf.st_mode) || (buf.st_mode & (S_IRWXG | S_IRWXO)) || ((buf.st_mode & S_IRWXU) == S_IRWXU)) {
                ereport(FATAL,
                    (errcode(ERRCODE_CONFIG_FILE_ERROR),
                        errmsg("certificate file \"%s\" has group or world access",
                            g_instance.attr.attr_security.ssl_cert_file),
                        errdetail("Permissions should be u=rw (0600) or less.")));
            }
        }
#endif

        if (stat(g_instance.attr.attr_security.ssl_key_file, &buf) != 0) {
            ereport(FATAL,
                (errcode_for_file_access(),
                    errmsg(
                        "could not access private key file \"%s\": %m", g_instance.attr.attr_security.ssl_key_file)));
        }

        /*
         * Require no public access to key file.
         *
         * XXX temporarily suppress check when on Windows, because there may
         * not be proper support for Unix-y file permissions.  Need to think
         * of a reasonable check to apply on Windows.  (See also the data
         * directory permission check in postmaster.c)
         */
#if !defined(WIN32) && !defined(__CYGWIN__)
        if (!S_ISREG(buf.st_mode) || (buf.st_mode & (S_IRWXG | S_IRWXO)) || ((buf.st_mode & S_IRWXU) == S_IRWXU)) {
            ereport(FATAL,
                (errcode(ERRCODE_CONFIG_FILE_ERROR),
                    errmsg("private key file \"%s\" has group or world access",
                        g_instance.attr.attr_security.ssl_key_file),
                    errdetail("Permissions should be u=rw (0600) or less.")));
        }
#endif

        if (SSL_CTX_use_PrivateKey_file(u_sess->libpq_cxt.SSL_server_context,
                                         g_instance.attr.attr_security.ssl_key_file,
                                         SSL_FILETYPE_PEM) != 1) {
            ereport(FATAL,
                (errmsg("could not load private key file \"%s\": %s",
                    g_instance.attr.attr_security.ssl_key_file,
                    SSLerrmessage())));
        }

        if (SSL_CTX_check_private_key(u_sess->libpq_cxt.SSL_server_context) != 1) {
            ereport(FATAL,
                (errmsg("check of private key \"%s\"failed: %s",
                    g_instance.attr.attr_security.ssl_key_file,
                    SSLerrmessage())));
        }
    }
    /* check ca certificate file permission */
#if !defined(WIN32) && !defined(__CYGWIN__)
    if (stat(g_instance.attr.attr_security.ssl_ca_file, &buf) == 0){
        if (!S_ISREG(buf.st_mode) || (buf.st_mode & (S_IRWXG | S_IRWXO)) || ((buf.st_mode & S_IRWXU) == S_IRWXU)) {
            ereport(FATAL,
                (errcode(ERRCODE_CONFIG_FILE_ERROR),
                    errmsg("ca certificate file \"%s\" has group or world access",
                        g_instance.attr.attr_security.ssl_ca_file),
                    errdetail("Permissions should be u=rw (0600) or less.")));
        }
    }
#endif
    /* Check the signature algorithm.*/
    if (check_certificate_signature_algrithm(u_sess->libpq_cxt.SSL_server_context)) {
        ereport(WARNING, (errmsg("The server certificate contain a Low-intensity signature algorithm")));
    }
    /* Check the certificate expires time.*/
    long leftspan = check_certificate_time(u_sess->libpq_cxt.SSL_server_context, 
        u_sess->attr.attr_security.ssl_cert_notify_time);
    if (leftspan > 0) {
        int leftdays = (leftspan / 86400 > 0) ? (leftspan / 86400) : 1;
        if (leftdays > 1) {
            ereport(WARNING, (errmsg("The server certificate will expire in %d days", leftdays)));
        } else {
            ereport(WARNING, (errmsg("The server certificate will expire in %d day", leftdays)));
        }
    }

    /* set up ephemeral DH keys, and disallow SSL v2 while at it
     * free the dh directly safe as there is reference counts in DH
     */
    DH* dhkey = genDHKeyPair(DHKey3072);
    if (dhkey == NULL) {
        ereport(ERROR, (errmsg("DH: generating parameters (3072 bits) failed")));
    }
    SSL_CTX_set_tmp_dh(u_sess->libpq_cxt.SSL_server_context, dhkey);
    DH_free(dhkey);

    /* SSL2.0/SSL3.0/TLS1.0/TLS1.1 is forbidden here. */
    SSL_CTX_set_options(u_sess->libpq_cxt.SSL_server_context,
        SSL_OP_SINGLE_DH_USE | SSL_OP_NO_SSLv2 | SSL_OP_NO_SSLv3 | SSL_OP_NO_TLSv1 | SSL_OP_NO_TLSv1_1);

    /* set up the allowed cipher list */
    if (strcasecmp(g_instance.attr.attr_security.SSLCipherSuites, "ALL") == 0) {
        set_default_ssl_ciphers();
    } else {
        set_user_config_ssl_ciphers(g_instance.attr.attr_security.SSLCipherSuites);
    }

    /* Load CA store, so we can verify client certificates if needed.*/
    if (g_instance.attr.attr_security.ssl_ca_file[0]) {
        if (SSL_CTX_load_verify_locations(
                u_sess->libpq_cxt.SSL_server_context, g_instance.attr.attr_security.ssl_ca_file, NULL) != 1) {
            ereport(FATAL, (errmsg("could not load the ca certificate file")));
        }

        root_cert_list = SSL_load_client_CA_file(g_instance.attr.attr_security.ssl_ca_file);
        if (root_cert_list == NULL) {
            ereport(FATAL,
                (errmsg("could not load root certificate file \"%s\": %s",
                    g_instance.attr.attr_security.ssl_ca_file,
                    SSLerrmessage())));
        }
    }

    /* Load the Certificate Revocation List (CRL).*/
    if (g_instance.attr.attr_security.ssl_crl_file[0]) {
        X509_STORE* cvstore = SSL_CTX_get_cert_store(u_sess->libpq_cxt.SSL_server_context);
        if (cvstore != NULL) {
            /* Set the flags to check against the complete CRL chain */
            if (1 == X509_STORE_load_locations(cvstore, g_instance.attr.attr_security.ssl_crl_file, NULL)) {
                (void)X509_STORE_set_flags(cvstore, X509_V_FLAG_CRL_CHECK);
            } else {
                ereport(WARNING,
                    (errmsg("could not load SSL certificate revocation list file \"%s\": %s",
                        g_instance.attr.attr_security.ssl_crl_file,
                        SSLerrmessage())));
            }
        }
    }

    if (g_instance.attr.attr_security.ssl_ca_file[0]) {
        /*
         * Always ask for SSL client cert, but don't fail if it's not
         * presented.  We might fail such connections later, depending on
         * what we find in pg_hba.conf.
         */
        SSL_CTX_set_verify(u_sess->libpq_cxt.SSL_server_context, (SSL_VERIFY_PEER | SSL_VERIFY_CLIENT_ONCE),
            be_verify_cb);

        /* Increase the depth to support multi-level certificate. */
        SSL_CTX_set_verify_depth(u_sess->libpq_cxt.SSL_server_context, (MAX_CERTIFICATE_DEPTH_SUPPORTED - 2));

        /* Set flag to remember CA store is successfully loaded */
        u_sess->libpq_cxt.ssl_loaded_verify_locations = true;

        /*
         * send the list of root certs we trust to clients in
         * CertificateRequests.  This lets a client with a keystore select the
         * appropriate client certificate to send to us.
         */
        SSL_CTX_set_client_CA_list(u_sess->libpq_cxt.SSL_server_context, root_cert_list);
    }
    /*clear the sensitive info in server_key*/
    errorno = memset_s(u_sess->libpq_cxt.server_key, CIPHER_LEN + 1, 0, CIPHER_LEN + 1);
    securec_check(errorno, "\0", "\0");

    u_sess->libpq_cxt.ssl_initialized = true;
}

/*
 * Brief            : static int open_server_SSL(Port *port)
 * Description  : Attempt to negotiate SSL connection.
 */
static int open_server_SSL(Port* port)
{
    int r;
    int err;

    Assert(port->ssl == NULL);
    Assert(port->peer == NULL);

    g_server_crl_err = false;

    port->ssl = SSL_new(u_sess->libpq_cxt.SSL_server_context);
    if (port->ssl == NULL) {
        ereport(COMMERROR,
            (errcode(ERRCODE_PROTOCOL_VIOLATION), errmsg("could not initialize SSL connection: %s", SSLerrmessage())));
        close_SSL(port);
        return -1;
    }
    if (1 != SSL_set_fd_ex(port, (int)((intptr_t)(port->sock)))) {
        ereport(
            COMMERROR, (errcode(ERRCODE_PROTOCOL_VIOLATION), errmsg("could not set SSL socket: %s", SSLerrmessage())));
        close_SSL(port);
        return -1;
    }

aloop:
    ERR_clear_error();
    r = SSL_accept(port->ssl);
    if (r != 1) {
        err = SSL_get_error(port->ssl, r);
        switch (err) {
            case SSL_ERROR_WANT_READ:
            case SSL_ERROR_WANT_WRITE:
#ifdef WIN32
                pgwin32_waitforsinglesocket(SSL_get_fd(port->ssl),
                    (err == SSL_ERROR_WANT_READ) ? (FD_READ | FD_CLOSE | FD_ACCEPT) : (FD_WRITE | FD_CLOSE),
                    INFINITE);
#endif
                goto aloop;
            case SSL_ERROR_SYSCALL:
                if (r < 0)
                    ereport(COMMERROR, (errcode_for_socket_access(), errmsg("could not accept SSL connection: %m")));
                else
                    ereport(COMMERROR,
                        (errcode(ERRCODE_PROTOCOL_VIOLATION), errmsg("could not accept SSL connection: EOF detected")));
                break;
            case SSL_ERROR_SSL:
                ereport(COMMERROR,
                    (errcode(ERRCODE_PROTOCOL_VIOLATION),
                        errmsg("could not accept SSL connection: %s", SSLerrmessage())));
                break;
            case SSL_ERROR_ZERO_RETURN:
                ereport(COMMERROR,
                    (errcode(ERRCODE_PROTOCOL_VIOLATION), errmsg("could not accept SSL connection: EOF detected")));
                break;
            default:
                ereport(
                    COMMERROR, (errcode(ERRCODE_PROTOCOL_VIOLATION), errmsg("unrecognized SSL error code: %d", err)));
                break;
        }
        close_SSL(port);
        return -1;
    }

    port->count = 0;

    /* Get client certificate, if available. */
    port->peer = SSL_get_peer_certificate(port->ssl);

    /* and extract the Common Name from it. */
    port->peer_cn = NULL;
    if (port->peer != NULL) {
        int rt;
        int len;
        char* peer_cn = NULL;

        /* First find out the name's length and allocate a buffer for it. */
        len = X509_NAME_get_text_by_NID(X509_get_subject_name(port->peer), NID_commonName, NULL, 0);
        if (len != -1) {
            peer_cn = (char*)palloc(len + 1);

            rt = X509_NAME_get_text_by_NID(X509_get_subject_name(port->peer), NID_commonName, peer_cn, len + 1);
            if (rt != len) {
                /* shouldn't happen */
                pfree(peer_cn);
                close_SSL(port);
                return -1;
            }
            /*
             * Reject embedded NULLs in certificate common name to prevent
             * attacks like CVE-2009-4034.
             */
            if ((size_t)(unsigned)len != strlen(peer_cn)) {
                ereport(COMMERROR,
                    (errcode(ERRCODE_PROTOCOL_VIOLATION),
                        errmsg("SSL certificate's common name contains embedded null")));
                pfree(peer_cn);
                close_SSL(port);
                return -1;
            }
            port->peer_cn = peer_cn;
        }
    }
    ereport(DEBUG2, (errmsg("SSL connection from \"%s\"", port->peer_cn ? port->peer_cn : "(anonymous)")));

    /* set up debugging/info callback */
    if (port->peer != NULL)
        SSL_set_info_callback(port->ssl, info_cb);

    return 0;
}
/*
 * Obtain reason string for last SSL error
 *
 * Some caution is needed here since ERR_reason_error_string will
 * return NULL if it doesn't recognize the error code.  We don't
 * want to return NULL ever.
 */
static const char* SSLerrmessage(void)
{
    unsigned long errcode;
    const char* errreason = NULL;
    static THR_LOCAL char errbuf[32];

    errcode = ERR_get_error();
    if (errcode == 0)
        return _("no SSL error reported");
    errreason = ERR_reason_error_string(errcode);
    if (errreason != NULL)
        return errreason;
    int rcs = snprintf_s(errbuf, sizeof(errbuf), sizeof(errbuf) - 1, _("SSL error code %lu"), errcode);
    securec_check_ss(rcs, "\0", "\0");
    return errbuf;
}

/* set the default password for certificate/private key loading */
static void init_server_ssl_passwd(SSL_CTX* pstContext)
{
    char* parentdir = NULL;
    KeyMode keymode = SERVER_MODE;
    if (is_absolute_path(g_instance.attr.attr_security.ssl_key_file)) {
        parentdir = pstrdup(g_instance.attr.attr_security.ssl_key_file);
        get_parent_directory(parentdir);
        decode_cipher_files(keymode, NULL, parentdir, u_sess->libpq_cxt.server_key);
    } else {
        decode_cipher_files(keymode, NULL, t_thrd.proc_cxt.DataDir, u_sess->libpq_cxt.server_key);
        parentdir = pstrdup(t_thrd.proc_cxt.DataDir);
    }

    check_permission_cipher_file(parentdir);
    pfree_ext(parentdir);

    SSL_CTX_set_default_passwd_cb_userdata(pstContext, (char*)u_sess->libpq_cxt.server_key);
}

/* Check permissions of cipher file and rand file in server */
static void check_permission_cipher_file(const char* parent_dir)
{
    char cipher_file[MAXPGPATH] = {0};
    char rand_file[MAXPGPATH] = {0};
    struct stat cipherbuf;
    struct stat randbuf;
    int rcs = snprintf_s(cipher_file, MAXPGPATH, MAXPGPATH - 1, "%s/server%s", parent_dir, CIPHER_KEY_FILE);
    securec_check_ss(rcs, "\0", "\0");
    rcs = snprintf_s(rand_file, MAXPGPATH, MAXPGPATH - 1, "%s/server%s", parent_dir, RAN_KEY_FILE);
    securec_check_ss(rcs, "\0", "\0");
    if (lstat(cipher_file, &cipherbuf) != 0 || lstat(rand_file, &randbuf) != 0)
        return;
#if !defined(WIN32) && !defined(__CYGWIN__)
    if (!S_ISREG(cipherbuf.st_mode) || (cipherbuf.st_mode & (S_IRWXG | S_IRWXO)) || ((cipherbuf.st_mode & S_IRWXU) == S_IRWXU))
        ereport(FATAL,
            (errcode(ERRCODE_CONFIG_FILE_ERROR),
                errmsg("cipher file \"%s\" has group or world access", cipher_file),
                errdetail("Permissions should be u=rw (0600) or less.")));
    if (!S_ISREG(randbuf.st_mode) || (randbuf.st_mode & (S_IRWXG | S_IRWXO)) || ((randbuf.st_mode & S_IRWXU) == S_IRWXU))
        ereport(FATAL,
            (errcode(ERRCODE_CONFIG_FILE_ERROR),
                errmsg("rand file \"%s\" has group or world access", rand_file),
                errdetail("Permissions should be u=rw (0600) or less.")));

#endif
}

/*
 * Private substitute BIO: this does the sending and receiving using send() and
 * recv() instead. This is so that we can enable and disable interrupts
 * just while calling recv(). We cannot have interrupts occurring while
 * the bulk of openssl runs, because it uses malloc() and possibly other
 * non-reentrant libc facilities. We also need to call send() and recv()
 * directly so it gets passed through the socket/signals layer on Win32.
 *
 * They are closely modelled on the original socket implementations in OpenSSL.
 */
static BIO_METHOD* my_bio_methods = NULL;

ssize_t secure_raw_read(Port* port, void* ptr, size_t len)
{
    ssize_t n;

    /*
     * Try to read from the socket without blocking. If it succeeds we're
     * done, otherwise we'll wait for the socket using the latch mechanism.
     */
#ifdef WIN32
    pgwin32_noblock = true;
#endif
    PGSTAT_INIT_TIME_RECORD();
    PGSTAT_START_TIME_RECORD();

    /* CommProxy Interface Support */
    n = comm_recv(port->sock, ptr, len, 0);
    END_NET_RECV_INFO(n);
#ifdef WIN32
    pgwin32_noblock = false;
#endif

    return n;
}

ssize_t secure_raw_write(Port* port, const void* ptr, size_t len)
{
    ssize_t n;

#ifdef WIN32
    pgwin32_noblock = true;
#endif
    PGSTAT_INIT_TIME_RECORD();
    PGSTAT_START_TIME_RECORD();

    /* CommProxy Interface Support */
    n = comm_send(port->sock, ptr, len, 0);
    END_NET_SEND_INFO(n);
#ifdef WIN32
    pgwin32_noblock = false;
#endif

    return n;
}

static int my_sock_read(BIO* h, char* buf, int size)
{
    int res = 0;

    if (buf != NULL) {
        Port* myPort = (Port*)BIO_get_data(h);
        if (myPort == NULL) {
            return 0;
        }

        prepare_for_client_read();
        res = secure_raw_read(myPort, buf, size);
        client_read_ended();
        BIO_clear_retry_flags(h);
        if (res <= 0) {
            /* If we were interrupted, tell caller to retry */
            if (errno == EINTR || errno == EWOULDBLOCK || errno == EAGAIN) {
                BIO_set_retry_read(h);
            }
        }
    }

    return res;
}

static int my_sock_write(BIO* h, const char* buf, int size)
{
    int res = 0;
    Port* myPort = (Port*)BIO_get_data(h);
    if (myPort == NULL) {
        return 0;
    }

    res = secure_raw_write(myPort, (void*)buf, size);
    BIO_clear_retry_flags(h);
    if (res <= 0) {
        /* If we were interrupted, tell caller to retry */
        if (errno == EINTR || errno == EWOULDBLOCK || errno == EAGAIN) {
            BIO_set_retry_write(h);
        }
    }

    return res;
}

static BIO_METHOD* my_BIO_s_socket(void)
{
    if (my_bio_methods == NULL) {
        int my_bio_index;

        my_bio_index = BIO_get_new_index();
        if (my_bio_index == -1)
            return NULL;

        BIO_METHOD* biom = (BIO_METHOD*)BIO_s_socket();
        my_bio_methods = BIO_meth_new(my_bio_index, "socket");
        if (my_bio_methods == NULL) {
            return NULL;
        }

        if (!BIO_meth_set_write(my_bio_methods, my_sock_write) || !BIO_meth_set_read(my_bio_methods, my_sock_read) ||
            !BIO_meth_set_gets(my_bio_methods, BIO_meth_get_gets(biom)) ||
            !BIO_meth_set_puts(my_bio_methods, BIO_meth_get_puts(biom)) ||
            !BIO_meth_set_ctrl(my_bio_methods, BIO_meth_get_ctrl(biom)) ||
            !BIO_meth_set_create(my_bio_methods, BIO_meth_get_create(biom)) ||
            !BIO_meth_set_destroy(my_bio_methods, BIO_meth_get_destroy(biom)) ||
            !BIO_meth_set_callback_ctrl(my_bio_methods, BIO_meth_get_callback_ctrl(biom))) {
            BIO_meth_free(my_bio_methods);
            my_bio_methods = NULL;
            return NULL;
        }
    }
    return my_bio_methods;
}

/* This should exactly match openssl's SSL_set_fd except for using my BIO */
static int SSL_set_fd_ex(Port* port, int fd)
{
    BIO* bio = NULL;
    BIO_METHOD* bio_method = NULL;

    bio_method = my_BIO_s_socket();
    if (bio_method == NULL) {
        SSLerr(SSL_F_SSL_SET_FD, ERR_R_BUF_LIB);
        return 0;
    }

    bio = BIO_new(bio_method);
    if (bio == NULL) {
        SSLerr(SSL_F_SSL_SET_FD, ERR_R_BUF_LIB);
        return 0;
    }
    BIO_set_data(bio, port);

    BIO_set_fd(bio, fd, BIO_NOCLOSE);
    SSL_set_bio(port->ssl, bio, bio);
    return 1;
}

static char* ssl_cipher_list2string(const char* ciphers[], const int num)
{
    int i;
    int catlen = 0;
    char* cipher_buf = NULL;
    errno_t errorno = EOK;

    size_t CIPHER_BUF_SIZE = 0;
    for (i = 0; i < num; i++) {
        CIPHER_BUF_SIZE += (strlen(ciphers[i]) + 1);
    }

    cipher_buf = (char*)OPENSSL_zalloc(CIPHER_BUF_SIZE);
    if (cipher_buf == NULL) {
        return NULL;
    }

    for (i = 0; i < num; i++) {
        errorno = memcpy_s(cipher_buf + catlen, strlen(ciphers[i]), ciphers[i], strlen(ciphers[i]));
        securec_check(errorno, "\0", "\0");

        catlen += strlen(ciphers[i]);

        if (i < num - 1) {
            errorno = memcpy_s(cipher_buf + catlen, CIPHER_BUF_SIZE - catlen, ":", 1);
            securec_check(errorno, "\0", "\0");
            catlen += 1;
        }
    }

    cipher_buf[catlen] = 0;
    return cipher_buf;
}

/*
 * Brief         : static int SSL_CTX_set_cipher_list_ex(SSL_CTX *ctx, const char* ciphers[], const int num)
 * Description   : set ssl ciphers.
 */
static int SSL_CTX_set_cipher_list_ex(SSL_CTX* ctx, const char* ciphers[], const int num)
{
    int ret = 0;
    char* cipher_buf = NULL;

    if (ctx == NULL) {
        return 0;
    }

    cipher_buf = ssl_cipher_list2string(ciphers, num);
    if (cipher_buf == NULL) {
        return 0;
    }

    ret = SSL_CTX_set_cipher_list(ctx, cipher_buf);
    OPENSSL_free(cipher_buf);
    return ret;
}

/*
 * Brief            : DH* genDHKeyPair(DHKeyLength dhType)
 * Notes            : function to generate DH key pair
 */
DH* genDHKeyPair(DHKeyLength dhType)
{
    int ret = 0;
    DH* dh = NULL;
    BIGNUM* bn_prime = NULL;
    unsigned char GENERATOR_2[] = {DH_GENERATOR_2};
    BIGNUM* bn_genenrator_2 = BN_bin2bn(GENERATOR_2, sizeof(GENERATOR_2), NULL);
    if (bn_genenrator_2 == NULL) {
        return NULL;
    }

    switch (dhType) {
        case DHKey768:
            bn_prime = BN_get_rfc2409_prime_768(NULL);
            break;
        case DHKey1024:
            bn_prime = BN_get_rfc2409_prime_1024(NULL);
            break;
        case DHKey1536:
            bn_prime = BN_get_rfc3526_prime_1536(NULL);
            break;
        case DHKey2048:
            bn_prime = BN_get_rfc3526_prime_2048(NULL);
            break;
        case DHKey3072:
            bn_prime = BN_get_rfc3526_prime_3072(NULL);
            break;
        case DHKey4096:
            bn_prime = BN_get_rfc3526_prime_4096(NULL);
            break;
        case DHKey6144:
            bn_prime = BN_get_rfc3526_prime_6144(NULL);
            break;
        case DHKey8192:
            bn_prime = BN_get_rfc3526_prime_8192(NULL);
            break;
        default:
            break;
    }

    if (bn_prime == NULL) {
        BN_free(bn_genenrator_2);
        return NULL;
    }

    dh = DH_new();
    if (dh == NULL) {
        BN_free(bn_prime);
        BN_free(bn_genenrator_2);
        return NULL;
    }

    ret = DH_set0_pqg(dh, bn_prime, NULL, bn_genenrator_2);
    if (!ret) {
        BN_free(bn_prime);
        BN_free(bn_genenrator_2);
        DH_free(dh);
        return NULL;
    }

    ret = DH_generate_key(dh);
    if (!ret) {
        BN_free(bn_prime);
        BN_free(bn_genenrator_2);
        DH_free(dh);
        return NULL;
    }

    return dh;
}

#endif /* USE_SSL */
