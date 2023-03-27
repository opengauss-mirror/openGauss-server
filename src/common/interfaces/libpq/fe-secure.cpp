/* -------------------------------------------------------------------------
 *
 * fe-secure.cpp
 *    functions related to setting up a secure connection to the backend.
 *    Secure connections are expected to provide confidentiality,
 *    message integrity and endpoint authentication.
 *
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *    src/common/interfaces/libpq/fe-secure.cpp
 *
 * NOTES
 *
 *    We don't provide informational callbacks here (like
 *    info_cb() in be-secure.c), since there's no good mechanism to
 *    display such information to the user.
 *
 * -------------------------------------------------------------------------
 */

#include "postgres_fe.h"

#pragma GCC diagnostic ignored "-Wunused-function"
#include <signal.h>
#include <fcntl.h>
#include <ctype.h>

#include "libpq/libpq-fe.h"
#include "fe-auth.h"
#include "pqsignal.h"
#include "libpq/libpq-int.h"

#ifdef WIN32
#include "win32.h"
#else
#include <sys/socket.h>
#include <unistd.h>
#include <netdb.h>
#include <netinet/in.h>
#ifdef HAVE_NETINET_TCP_H
#include <netinet/tcp.h>
#endif
#include <arpa/inet.h>
#endif

#include <sys/stat.h>

#ifdef ENABLE_THREAD_SAFETY
#ifdef WIN32
#include "pthread-win32.h"
#else
#include <pthread.h>
#endif
#endif

#ifdef USE_SSL
#include "openssl/ssl.h"
#include "openssl/ossl_typ.h"
#include "openssl/x509.h"
#include "openssl/crypto.h"
#include "openssl/sslerr.h"
#include "openssl/err.h"
#include "utils/elog.h"

#ifndef WIN32
#define USER_CERT_FILE ".postgresql/postgresql.crt"
#define USER_KEY_FILE ".postgresql/postgresql.key"
#ifdef USE_TASSL
#define USER_ENC_CERT_FILE ".postgresql/postgresql_enc.crt"
#define USER_ENC_KEY_FILE ".postgresql/postgresql_enc.key"
#endif
#define ROOT_CERT_FILE ".postgresql/root.crt"
#define ROOT_CRL_FILE ".postgresql/root.crl"
#else
/* On Windows, the "home" directory is already PostgreSQL-specific */

#define USER_CERT_FILE "postgresql.crt"
#define USER_KEY_FILE "postgresql.key"
#ifdef USE_TASSL
#define USER_ENC_CERT_FILE "postgresql_enc.crt"
#define USER_ENC_KEY_FILE "postgresql_enc.key"
#endif
#define ROOT_CERT_FILE "root.crt"
#define ROOT_CRL_FILE "root.crl"
#endif

#include "cipher.h"

static bool verify_peer_name_matches_certificate(PGconn*);
static int init_ssl_system(PGconn* conn);
static void destroy_ssl_system(PGconn* conn);
static void destroySSL(PGconn* conn);
static void close_SSL(PGconn*);
#ifndef ENABLE_UT
static char* SSLerrmessage(void);
static int check_permission_cipher_file(const char* parent_dir, PGconn* conn, const char* username, bool enc);
static PostgresPollingStatusType open_client_SSL(PGconn*);
static int verify_cb(int ok, X509_STORE_CTX* ctx);
static int initialize_SSL(PGconn* conn);
static int init_client_ssl_passwd(SSL* pstContext, const char* path, const char* username, PGconn* conn, bool enc);
#else
char* SSLerrmessage(void);
int check_permission_cipher_file(const char* parent_dir, PGconn* conn, const char* username, bool enc);
PostgresPollingStatusType open_client_SSL(PGconn*);
int verify_cb(int ok, X509_STORE_CTX* ctx);
int initialize_SSL(PGconn* conn);
int init_client_ssl_passwd(SSL* pstContext, const char* path, const char* username, PGconn* conn, bool enc);
#endif

static void SSLerrfree(char* buf);

int ssl_security_DH_ECDH_cb(const SSL* s, const SSL_CTX* ctx, int op, int bits, int nid, void* other, void* ex);

static char* ssl_cipher_list2string(const char* ciphers[], const int num);
static int SSL_CTX_set_cipher_list_ex(SSL_CTX* ctx, const char* ciphers[], const int num);

static THR_LOCAL bool pq_init_ssl_lib = true;
static THR_LOCAL bool pq_init_crypto_lib = true;
static THR_LOCAL bool g_crl_invalid = false;
#ifdef ENABLE_UT
#define static
#endif

#ifndef ENABLE_UT
static bool set_client_ssl_ciphers(); /* set client security cipherslist*/
#else
bool set_client_ssl_ciphers(); /* set client security cipherslist*/
#endif  // ENABLE_UT

/*
 * SSL_context is currently shared between threads and therefore we need to be
 * careful to lock around any usage of it when providing thread safety.
 * ssl_config_mutex is the mutex that we use to protect it.
 */
static THR_LOCAL SSL_CTX* SSL_context = NULL;

THR_LOCAL unsigned char disable_pqlocking = 0;

#ifdef ENABLE_THREAD_SAFETY
static long ssl_open_connections = 0;

#ifndef WIN32
static pthread_mutex_t ssl_config_mutex = PTHREAD_MUTEX_INITIALIZER;
#else
static pthread_mutex_t ssl_config_mutex = NULL;
static long win32_ssl_create_mutex = 0;
#endif
#endif /* ENABLE_THREAD_SAFETY */

/* security ciphers suites in SSL connection */
static const char* ssl_ciphers_map[] = {
    TLS1_TXT_ECDHE_RSA_WITH_AES_128_GCM_SHA256,     /* TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256 */
    TLS1_TXT_ECDHE_RSA_WITH_AES_256_GCM_SHA384,     /* TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384 */
    TLS1_TXT_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,   /* TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256 */
    TLS1_TXT_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,   /* TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384 */
    /* The following are compatible with earlier versions of the server. */
    TLS1_TXT_DHE_RSA_WITH_AES_128_GCM_SHA256,       /* TLS_DHE_RSA_WITH_AES_128_GCM_SHA256, */
    TLS1_TXT_DHE_RSA_WITH_AES_256_GCM_SHA384,       /* TLS_DHE_RSA_WITH_AES_256_GCM_SHA384 */
#ifdef USE_TASSL
    /* GM */
    TLS1_TXT_ECDHE_WITH_SM4_SM3,                     /* TLS1_TXT_ECDHE_WITH_SM4_SM3 */
    TLS1_TXT_ECDHE_WITH_SM4_GCM_SM3,                 /* TLS1_TXT_ECDHE_WITH_SM4_GCM_SM3 */
    TLS1_TXT_ECC_WITH_SM4_SM3,                       /* TLS1_TXT_ECC_WITH_SM4_SM3 */
    TLS1_TXT_ECC_WITH_SM4_GCM_SM3,                   /* TLS1_TXT_ECC_WITH_SM4_GCM_SM3 */
#endif
    NULL};

#endif /* SSL */

/*
 * Macros to handle disabling and then restoring the state of SIGPIPE handling.
 * On Windows, these are all no-ops since there's no SIGPIPEs.
 */

#ifndef WIN32

#define SIGPIPE_MASKED(conn) ((conn)->sigpipe_so || (conn)->sigpipe_flag)

#ifdef ENABLE_THREAD_SAFETY

struct sigpipe_info {
    sigset_t oldsigmask;
    bool sigpipe_pending;
    bool got_epipe;
};

#define DECLARE_SIGPIPE_INFO(spinfo) struct sigpipe_info spinfo

#define DISABLE_SIGPIPE(conn, spinfo, failaction)                                      \
    do {                                                                               \
        (spinfo).got_epipe = false;                                                    \
        if (!SIGPIPE_MASKED(conn)) {                                                   \
            if (pq_block_sigpipe(&(spinfo).oldsigmask, &(spinfo).sigpipe_pending) < 0) \
                failaction;                                                            \
        }                                                                              \
    } while (0)

#define REMEMBER_EPIPE(spinfo, cond)   \
    do {                               \
        if (cond)                      \
            (spinfo).got_epipe = true; \
    } while (0)

#define RESTORE_SIGPIPE(conn, spinfo)                                                             \
    do {                                                                                          \
        if (!SIGPIPE_MASKED(conn))                                                                \
            pq_reset_sigpipe(&(spinfo).oldsigmask, (spinfo).sigpipe_pending, (spinfo).got_epipe); \
    } while (0)
#else /* !ENABLE_THREAD_SAFETY */

#define DECLARE_SIGPIPE_INFO(spinfo) pqsigfunc spinfo = NULL

#define DISABLE_SIGPIPE(conn, spinfo, failaction)  \
    do {                                           \
        if (!SIGPIPE_MASKED((conn))) {             \
            (spinfo) = pqsignal(SIGPIPE, SIG_IGN); \
        }                                          \
    } while (0)

#define REMEMBER_EPIPE(spinfo, cond)

#define RESTORE_SIGPIPE(conn, spinfo)    \
    do {                                 \
        if (!SIGPIPE_MASKED((conn))) {   \
            pqsignal(SIGPIPE, (spinfo)); \
        }                                \
    } while (0)
#endif /* ENABLE_THREAD_SAFETY */
#else  /* WIN32 */

#define DECLARE_SIGPIPE_INFO(spinfo)
#define DISABLE_SIGPIPE(conn, spinfo, failaction)
#define REMEMBER_EPIPE(spinfo, cond)
#define RESTORE_SIGPIPE(conn, spinfo)
#endif /* WIN32 */

/* ------------------------------------------------------------ */
/*           Procedures common to all secure sessions           */
/* ------------------------------------------------------------ */

/*
 *  Exported function to allow application to tell us it's already
 *  initialized OpenSSL.
 */
void PQinitSSL(int do_init)
{
    PQinitOpenSSL(do_init, do_init);
}

/*
 *  Exported function to allow application to tell us it's already
 *  initialized OpenSSL and/or libcrypto.
 */
void PQinitOpenSSL(int do_ssl, int do_crypto)
{
#ifdef USE_SSL
#ifdef ENABLE_THREAD_SAFETY

    /*
     * Disallow changing the flags while we have open connections, else we'd
     * get completely confused.
     */
    if (ssl_open_connections != 0)
        return;
#endif

    pq_init_ssl_lib = do_ssl;
    pq_init_crypto_lib = do_crypto;
#endif
}

/*
 *  Initialize global SSL context
 */
int pqsecure_initialize(PGconn* conn)
{
    int r = 0;

#ifdef USE_SSL
    r = init_ssl_system(conn);
#endif

    return r;
}

/*
 *  Destroy global context
 */
void pqsecure_destroy(PGconn* conn)
{
#ifdef USE_SSL
    destroySSL(conn);
#endif
}

/*
 *  Begin or continue negotiating a secure session.
 */
PostgresPollingStatusType pqsecure_open_client(PGconn* conn)
{
#ifdef USE_SSL
    disable_pqlocking = 0;

    /* First time through? */
    if (conn->ssl == NULL) {
#ifdef ENABLE_THREAD_SAFETY
        int rc = 0;
#endif

        /* We cannot use MSG_NOSIGNAL to block SIGPIPE when using SSL */
        conn->sigpipe_flag = false;

#ifdef ENABLE_THREAD_SAFETY
        if ((rc = pthread_mutex_lock(&ssl_config_mutex))) {
            printfPQExpBuffer(&conn->errorMessage, libpq_gettext("could not acquire mutex: %s\n"), strerror(rc));
            return (PostgresPollingStatusType)-1;
        }
#endif

        /* Create a connection-specific SSL object */
        if (((conn->ssl = SSL_new(SSL_context)) == NULL) || !SSL_set_app_data(conn->ssl, conn) ||
            !SSL_set_fd(conn->ssl, (int)((intptr_t)(conn->sock)))) {
            char* err = SSLerrmessage();

            printfPQExpBuffer(&conn->errorMessage,
                libpq_gettext("could not establish SSL connection, remote datanode %s: %s, err:%s\n"),
                conn->remote_nodename,
                err, strerror(errno));
            SSLerrfree(err);
#ifdef ENABLE_THREAD_SAFETY
            (void)pthread_mutex_unlock(&ssl_config_mutex);
#endif
            close_SSL(conn);
            return PGRES_POLLING_FAILED;
        }

#ifdef ENABLE_THREAD_SAFETY
        (void)pthread_mutex_unlock(&ssl_config_mutex);
#endif

        /*
         * Load client certificate, private key, and trusted CA certs.
         */
        if (initialize_SSL(conn) != 0) {
            /* initialize_SSL already put a message in conn->errorMessage */
            close_SSL(conn);
            return PGRES_POLLING_FAILED;
        }
    }

    /* Begin or continue the actual handshake */
    return open_client_SSL(conn);
#else
    /* shouldn't get here */
    return PGRES_POLLING_FAILED;
#endif
}
/*
 *  Close secure session.
 */
void pqsecure_close(PGconn* conn)
{
#ifdef USE_SSL
    if (conn->ssl != NULL)
        close_SSL(conn);
#endif
}

/*
 *  Read data from a secure connection.
 *
 * On failure, this function is responsible for putting a suitable message
 * into conn->errorMessage.  The caller must still inspect errno, but only
 * to determine whether to continue/retry after error.
 */
ssize_t pqsecure_read(PGconn* conn, void* ptr, size_t len)
{
    ssize_t n;
    int result_errno = 0;
    char sebuf[256];

#ifdef USE_SSL
    if (conn->ssl != NULL) {
        int err;

        DECLARE_SIGPIPE_INFO(spinfo);

        /* SSL_read can write to the socket, so we need to disable SIGPIPE */
        DISABLE_SIGPIPE(conn, spinfo, return -1);

    rloop:
        SOCK_ERRNO_SET(0);
        ERR_clear_error();
        n = SSL_read(conn->ssl, ptr, len);

        err = SSL_get_error(conn->ssl, n);
        switch (err) {
            case SSL_ERROR_NONE:
                if (n < 0) {
                    /* Not supposed to happen, so we don't translate the msg */
                    printfPQExpBuffer(&conn->errorMessage,
                        "SSL_read failed but did not provide error information, remote datanode %s, err: %s\n",
                        conn->remote_nodename, strerror(errno));
                    /* assume the connection is broken */
                    result_errno = ECONNRESET;
                }
                break;
            case SSL_ERROR_WANT_READ:
                n = 0;
                break;
            case SSL_ERROR_WANT_WRITE:

                /*
                 * Returning 0 here would cause caller to wait for read-ready,
                 * which is not correct since what SSL wants is wait for
                 * write-ready.  The former could get us stuck in an infinite
                 * wait, so don't risk it; busy-loop instead.
                 */
                goto rloop;
            case SSL_ERROR_SYSCALL:
                if (n < 0) {
                    result_errno = SOCK_ERRNO;
                    REMEMBER_EPIPE(spinfo, result_errno == EPIPE);
                    printfPQExpBuffer(&conn->errorMessage,
                        libpq_gettext("SSL SYSCALL error: %s, remote datanode %s, error: %s\n"),
                        SOCK_STRERROR(result_errno,
                        sebuf, sizeof(sebuf)), conn->remote_nodename, strerror(errno));
                } else {
                    printfPQExpBuffer(&conn->errorMessage,
                        libpq_gettext("SSL SYSCALL error: EOF detected, remote datanode %s, error: %s\n"),
                        conn->remote_nodename, strerror(errno));
                    /* assume the connection is broken */
                    result_errno = ECONNRESET;
                    n = -1;
                }
                break;
            case SSL_ERROR_SSL: {
                char* errm = SSLerrmessage();

                printfPQExpBuffer(&conn->errorMessage,
                    libpq_gettext("SSL error: %s, remote datanode %s, error: %s\n"),
                    errm, conn->remote_nodename, strerror(errno));
                SSLerrfree(errm);
                /* assume the connection is broken */
                result_errno = ECONNRESET;
                n = -1;
                break;
            }
            case SSL_ERROR_ZERO_RETURN:

                /*
                 * Per OpenSSL documentation, this error code is only returned
                 * for a clean connection closure, so we should not report it
                 * as a server crash.
                 */
                printfPQExpBuffer(&conn->errorMessage,
                    libpq_gettext("SSL connection has been closed unexpectedly, remote datanode %s, error: %s\n"),
                    conn->remote_nodename, strerror(errno));
                result_errno = ECONNRESET;
                n = -1;
                break;
            default:
                printfPQExpBuffer(&conn->errorMessage,
                    libpq_gettext("unrecognized SSL error code: %d, remote datanode %s, error: %s\n"),
                    err,
                    conn->remote_nodename, strerror(errno));
                /* assume the connection is broken */
                result_errno = ECONNRESET;
                n = -1;
                break;
        }

        RESTORE_SIGPIPE(conn, spinfo);
    } else
#endif /* USE_SSL */
    {
#ifdef WIN32
        n = recv(conn->sock, (char*)ptr, len, 0);
#else
        n = recv(conn->sock, ptr, len, 0);
#endif
        if (n < 0) {
            result_errno = SOCK_ERRNO;

            /* Set error message if appropriate */
            switch (result_errno) {
#ifdef EAGAIN
                case EAGAIN:
#endif
#if defined(EWOULDBLOCK) && (!defined(EAGAIN) || (EWOULDBLOCK != EAGAIN))
                case EWOULDBLOCK:
#endif
                case EINTR:
                    /* no error message, caller is expected to retry */
                    break;

#ifdef ECONNRESET
                case ECONNRESET:
                    printfPQExpBuffer(&conn->errorMessage,
                        libpq_gettext("could not receive data from server, error: %s, remote datanode: %s\n"),
                        SOCK_STRERROR(result_errno,
                        sebuf, sizeof(sebuf)), conn->remote_nodename);
                    break;
#endif

                default:
                    printfPQExpBuffer(&conn->errorMessage,
                        libpq_gettext("could not receive data from server, error: %s, remote datanode: %s\n"),
                        SOCK_STRERROR(result_errno, sebuf, sizeof(sebuf)), conn->remote_nodename);
                    break;
            }
        }
    }

    /* ensure we return the intended errno to caller */
    SOCK_ERRNO_SET(result_errno);

    return n;
}

/*
 *  Write data to a secure connection.
 *
 * On failure, this function is responsible for putting a suitable message
 * into conn->errorMessage.  The caller must still inspect errno, but only
 * to determine whether to continue/retry after error.
 */
ssize_t pqsecure_write(PGconn* conn, const void* ptr, size_t len)
{
    ssize_t n;
    int result_errno = 0;
    char sebuf[256];

    DECLARE_SIGPIPE_INFO(spinfo);

#ifdef USE_SSL
    if (conn->ssl != NULL) {
        int err;

        DISABLE_SIGPIPE(conn, spinfo, return -1);

        SOCK_ERRNO_SET(0);
        ERR_clear_error();
        n = SSL_write(conn->ssl, ptr, len);

        err = SSL_get_error(conn->ssl, n);
        switch (err) {
            case SSL_ERROR_NONE:
                if (n < 0) {
                    /* Not supposed to happen, so we don't translate the msg */
                    printfPQExpBuffer(&conn->errorMessage,
                        "SSL_write failed but did not provide error information, remote datanode %s, error: %s\n",
                        conn->remote_nodename, strerror(errno));
                    /* assume the connection is broken */
                    result_errno = ECONNRESET;
                }
                break;
            case SSL_ERROR_WANT_READ:

                /*
                 * Returning 0 here causes caller to wait for write-ready,
                 * which is not really the right thing, but it's the best we
                 * can do.
                 */
                n = 0;
                break;
            case SSL_ERROR_WANT_WRITE:
                n = 0;
                break;
            case SSL_ERROR_SYSCALL:
                if (n < 0) {
                    result_errno = SOCK_ERRNO;
                    REMEMBER_EPIPE(spinfo, result_errno == EPIPE);
                    printfPQExpBuffer(&conn->errorMessage,
                        libpq_gettext("SSL SYSCALL error: %s, remote datanode %s\n"),
                        SOCK_STRERROR(result_errno, sebuf, sizeof(sebuf)),
                        conn->remote_nodename);
                } else {
                    printfPQExpBuffer(&conn->errorMessage,
                        libpq_gettext("SSL SYSCALL error: EOF detected, remote datanode %s, error: %s\n"),
                        conn->remote_nodename, strerror(errno));
                    /* assume the connection is broken */
                    result_errno = ECONNRESET;
                    n = -1;
                }
                break;
            case SSL_ERROR_SSL: {
                char* errm = SSLerrmessage();

                printfPQExpBuffer(&conn->errorMessage,
                    libpq_gettext("SSL error: %s, remote datanode %s, error: %s\n"), errm,
                    conn->remote_nodename, strerror(errno));
                SSLerrfree(errm);
                /* assume the connection is broken */
                result_errno = ECONNRESET;
                n = -1;
                break;
            }
            case SSL_ERROR_ZERO_RETURN:

                /*
                 * Per OpenSSL documentation, this error code is only returned
                 * for a clean connection closure, so we should not report it
                 * as a server crash.
                 */
                printfPQExpBuffer(&conn->errorMessage,
                    libpq_gettext("SSL connection has been closed unexpectedly, remote datanode %s, error: %s\n"),
                    conn->remote_nodename, strerror(errno));
                result_errno = ECONNRESET;
                n = -1;
                break;
            default:
                printfPQExpBuffer(&conn->errorMessage,
                    libpq_gettext("unrecognized SSL error code: %d, remote datanode %s, error: %s\n"),
                    err, conn->remote_nodename, strerror(errno));
                /* assume the connection is broken */
                result_errno = ECONNRESET;
                n = -1;
                break;
        }
    } else
#endif /* USE_SSL */
    {
        unsigned int flags = 0;

#ifdef MSG_NOSIGNAL
        if (conn->sigpipe_flag)
            flags |= MSG_NOSIGNAL;

    retry_masked:
#endif /* MSG_NOSIGNAL */

        DISABLE_SIGPIPE(conn, spinfo, return -1);
#ifdef WIN32
        n = send(conn->sock, (const char*)ptr, len, flags);
#else
        n = send(conn->sock, ptr, len, flags);
#endif
        if (n < 0) {
            result_errno = SOCK_ERRNO;

            /*
             * If we see an EINVAL, it may be because MSG_NOSIGNAL isn't
             * available on this machine.  So, clear sigpipe_flag so we don't
             * try the flag again, and retry the send().
             */
#ifdef MSG_NOSIGNAL
            if (flags != 0 && result_errno == EINVAL) {
                conn->sigpipe_flag = false;
                flags = 0;
                goto retry_masked;
            }
#endif /* MSG_NOSIGNAL */

            /* Set error message if appropriate */
            switch (result_errno) {
#ifdef EAGAIN
                case EAGAIN:
#endif
#if defined(EWOULDBLOCK) && (!defined(EAGAIN) || (EWOULDBLOCK != EAGAIN))
                case EWOULDBLOCK:
#endif
                case EINTR:
                    /* no error message, caller is expected to retry */
                    break;

                case EPIPE:
                    /* Set flag for EPIPE */
                    REMEMBER_EPIPE(spinfo, true);
                    /* FALL THRU */

#ifdef ECONNRESET
                case ECONNRESET:
#endif
                    printfPQExpBuffer(&conn->errorMessage,
                        libpq_gettext("could not send data to server: %s\n"),
                        SOCK_STRERROR(result_errno, sebuf, sizeof(sebuf)));

                    break;

                default:
                    printfPQExpBuffer(&conn->errorMessage,
                        libpq_gettext("could not send data to server: %s\n"),
                        SOCK_STRERROR(result_errno, sebuf, sizeof(sebuf)));
                    break;
            }
        }
    }

    RESTORE_SIGPIPE(conn, spinfo);

    /* ensure we return the intended errno to caller */
    SOCK_ERRNO_SET(result_errno);

    return n;
}

/*
 *  Read data from a pg_fdw secure connection.
 */
ssize_t pgfdw_pqsecure_read(PGconn* conn, void* ptr, size_t len)
{
    ssize_t n = -1;
    int result_errno = 0;
    char sebuf[256];

#ifdef USE_SSL
    if (conn->ssl != NULL) {
        int err;

        DECLARE_SIGPIPE_INFO(spinfo);

        /* SSL_read can write to the socket, so we need to disable SIGPIPE */
        DISABLE_SIGPIPE(conn, spinfo, return -1);

    rloop:
        SOCK_ERRNO_SET(0);
        ERR_clear_error();
        n = SSL_read(conn->ssl, ptr, len);

        err = SSL_get_error(conn->ssl, n);
        switch (err) {
            case SSL_ERROR_NONE:
                if (n < 0) {
                    /* Not supposed to happen, so we don't translate the msg */
                    printfPQExpBuffer(&conn->errorMessage,
                        "SSL_read failed but did not provide error information, remote datanode %s, error: %s\n",
                        conn->remote_nodename, strerror(errno));
                    /* assume the connection is broken */
                    result_errno = ECONNRESET;
                }
                break;
            case SSL_ERROR_WANT_READ:
            case SSL_ERROR_WANT_WRITE:
                goto rloop;
            case SSL_ERROR_SYSCALL:
                if (n < 0) {
                    result_errno = SOCK_ERRNO;
                    REMEMBER_EPIPE(spinfo, result_errno == EPIPE);
                    printfPQExpBuffer(&conn->errorMessage,
                        libpq_gettext("SSL SYSCALL error: %s\n"),
                        SOCK_STRERROR(result_errno, sebuf, sizeof(sebuf)));
                } else {
                    printfPQExpBuffer(&conn->errorMessage,
                        libpq_gettext("SSL SYSCALL error: EOF detected, remote datanode %s, error: %s\n"),
                        conn->remote_nodename, strerror(errno));
                    /* assume the connection is broken */
                    result_errno = ECONNRESET;
                    n = -1;
                }
                break;
            case SSL_ERROR_SSL: {
                char* errm = SSLerrmessage();

                printfPQExpBuffer(&conn->errorMessage,
                    libpq_gettext("SSL error: %s, remote datanode %s, error: %s\n"),
                    errm, conn->remote_nodename, strerror(errno));
                SSLerrfree(errm);
                /* assume the connection is broken */
                result_errno = ECONNRESET;
                n = -1;
                break;
            }
            case SSL_ERROR_ZERO_RETURN:

                /*
                 * Per OpenSSL documentation, this error code is only returned
                 * for a clean connection closure, so we should not report it
                 * as a server crash.
                 */
                printfPQExpBuffer(&conn->errorMessage,
                    libpq_gettext("SSL connection has been closed unexpectedly, remote datanode %s, error:%s\n"),
                    conn->remote_nodename, strerror(errno));
                result_errno = ECONNRESET;
                n = -1;
                break;
            default:
                printfPQExpBuffer(&conn->errorMessage,
                    libpq_gettext("unrecognized SSL error code: %d, remote datanode %s, error:%s\n"),
                    err, conn->remote_nodename, strerror(errno));
                /* assume the connection is broken */
                result_errno = ECONNRESET;
                n = -1;
                break;
        }

        RESTORE_SIGPIPE(conn, spinfo);
    }
#endif /* USE_SSL */

    /* ensure we return the intended errno to caller */
    SOCK_ERRNO_SET(result_errno);

    return n;
}

/*
 *  Write data to a pg_fdw secure connection.
 */
ssize_t pgfdw_pqsecure_write(PGconn* conn, const void* ptr, size_t len)
{
    ssize_t n = -1;
    int result_errno = 0;
    char sebuf[256];

    DECLARE_SIGPIPE_INFO(spinfo);

#ifdef USE_SSL
    if (conn->ssl != NULL) {
        int err;

        DISABLE_SIGPIPE(conn, spinfo, return -1);

    wloop:
        SOCK_ERRNO_SET(0);
        ERR_clear_error();
        n = SSL_write(conn->ssl, ptr, len);

        err = SSL_get_error(conn->ssl, n);
        switch (err) {
            case SSL_ERROR_NONE:
                if (n < 0) {
                    /* Not supposed to happen, so we don't translate the msg */
                    printfPQExpBuffer(&conn->errorMessage,
                        "SSL_write failed but did not provide error information, remote datanode %s, error: %s\n",
                        conn->remote_nodename, strerror(errno));
                    /* assume the connection is broken */
                    result_errno = ECONNRESET;
                }
                break;
            case SSL_ERROR_WANT_READ:
            case SSL_ERROR_WANT_WRITE:
                goto wloop;
            case SSL_ERROR_SYSCALL:
                if (n < 0) {
                    result_errno = SOCK_ERRNO;
                    REMEMBER_EPIPE(spinfo, result_errno == EPIPE);
                    printfPQExpBuffer(&conn->errorMessage,
                        libpq_gettext("SSL SYSCALL error: %s\n"),
                        SOCK_STRERROR(result_errno, sebuf, sizeof(sebuf)));
                } else {
                    printfPQExpBuffer(&conn->errorMessage,
                        libpq_gettext("SSL SYSCALL error: EOF detected, remote datanode %s, error: %s\n"),
                        conn->remote_nodename, strerror(errno));
                    /* assume the connection is broken */
                    result_errno = ECONNRESET;
                    n = -1;
                }
                break;
            case SSL_ERROR_SSL: {
                char* errm = SSLerrmessage();

                printfPQExpBuffer(&conn->errorMessage, libpq_gettext("SSL error: %s\n"), errm);
                SSLerrfree(errm);
                /* assume the connection is broken */
                result_errno = ECONNRESET;
                n = -1;
                break;
            }
            case SSL_ERROR_ZERO_RETURN:

                /*
                 * Per OpenSSL documentation, this error code is only returned
                 * for a clean connection closure, so we should not report it
                 * as a server crash.
                 */
                printfPQExpBuffer(&conn->errorMessage,
                    libpq_gettext("SSL connection has been closed unexpectedly, remote datanode %s, error: %s\n"),
                    conn->remote_nodename, strerror(errno));
                result_errno = ECONNRESET;
                n = -1;
                break;
            default:
                printfPQExpBuffer(&conn->errorMessage, libpq_gettext("unrecognized SSL error code: %d\n"), err);
                /* assume the connection is broken */
                result_errno = ECONNRESET;
                n = -1;
                break;
        }
    }
#endif /* USE_SSL */

    RESTORE_SIGPIPE(conn, spinfo);

    /* ensure we return the intended errno to caller */
    SOCK_ERRNO_SET(result_errno);

    return n;
}

/* ------------------------------------------------------------ */
/*                        SSL specific code                     */
/* ------------------------------------------------------------ */
#ifdef USE_SSL

static bool is_crl_invalid(int errcode)
{
    const int err_scenarios[] = {
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

    for (size_t i = 0; i < sizeof(err_scenarios) / sizeof(err_scenarios[0]); i++) {
        if (errcode == err_scenarios[i]) {
            return true;
        }
    }

    return false;
}

/*
 *  Certificate verification callback
 *
 *  This callback allows us to log intermediate problems during
 *  verification, but there doesn't seem to be a clean way to get
 *  our PGconn * structure.  So we can't log anything!
 *
 *  This callback also allows us to override the default acceptance
 *  criteria (e.g., accepting self-signed or expired certs), but
 *  for now we accept the default checks.
 */
#ifndef ENABLE_UT
static
#endif
 int verify_cb(int ok, X509_STORE_CTX* ctx)
{
    if (ok) {
        return ok;
    }

    /* 
    * When the CRL is abnormal, it won't be used to check whether the certificate is revoked,
    * and the services shouldn't be affected due to the CRL exception.
    */
    bool ignore_crl_err = false;
    int err_code = X509_STORE_CTX_get_error(ctx);
    /*
     * while openssl check the cert with crl, the logic is:
     *      if crl is expired:
     *          verify_cb(X509_V_ERR_CRL_HAS_EXPIRED)
     *      if crl is invalid:
     *          verify_cb(other crl invalid err code)
     *      if cert in crl:
     *          verify_cb(X509_V_ERR_CERT_REVOKED)
     * so, the verify_cb() may be called more than once
     * for crl expired or invalid, we just ignore the error and return 1 (means succeed)
     */
    if (err_code == X509_V_ERR_CRL_HAS_EXPIRED) {
        ignore_crl_err = true;
        (void)printf("WARNING: the crl file '$PGSSLCRL' in client has expired, but we are still using it, "
            "please update it.\n");
    } else if (is_crl_invalid(err_code)) {
        g_crl_invalid = true;
        ignore_crl_err = true;
        (void)printf("WARNING: the crl file '$PGSSLCRL' in client is invalid, for toughness, we just ignore it.\n");
    } else if (err_code == X509_V_ERR_CERT_REVOKED) {
        if (g_crl_invalid) { /* only when crl is invalid, ignore the cert revoke error */
            ignore_crl_err = true;
        }
    } /* else: do not process other error */

    if (ignore_crl_err) {
        X509_STORE_CTX_set_error(ctx, X509_V_OK);
        ok = 1;
    }

    return ok;
}

/*
 * Check if a wildcard certificate matches the server hostname.
 *
 * The rule for this is:
 *  1. We only match the '*' character as wildcard
 *  2. We match only wildcards at the start of the string
 *  3. The '*' character does *not* match '.', meaning that we match only
 *     a single pathname component.
 *  4. We don't support more than one '*' in a single pattern.
 *
 * This is roughly in line with RFC2818, but contrary to what most browsers
 * appear to be implementing (point 3 being the difference)
 *
 * Matching is always case-insensitive, since DNS is case insensitive.
 */
#ifndef ENABLE_UT
static
#endif
int wildcard_certificate_match(const char* pattern, const char* string)
{
    int lenpat = strlen(pattern);
    int lenstr = strlen(string);

    /* If we don't start with a wildcard, it's not a match (rule 1 & 2) */
    if (lenpat < 3 || pattern[0] != '*' || pattern[1] != '.')
        return 0;

    if (lenpat > lenstr)
        /* If pattern is longer than the string, we can never match */
        return 0;

    if (pg_strcasecmp(pattern + 1, string + lenstr - lenpat + 1) != 0)

        /*
         * If string does not end in pattern (minus the wildcard), we don't
         * match
         */
        return 0;

    if (strchr(string, '.') < string + lenstr - lenpat)

        /*
         * If there is a dot left of where the pattern started to match, we
         * don't match (rule 3)
         */
        return 0;

    /* String ended with pattern, and didn't have a dot before, so we match */
    return 1;
}

/*
 * Brief            : static bool verify_peer_name_matches_certificate(PGconn *conn)
 * Description  : Verify that common name resolves to peer.
 * Notes            :  use internal ssl objects
 */
static bool verify_peer_name_matches_certificate(PGconn* conn)
{
    char* peer_cn = NULL;
    int r;
    int len;
    bool result = false;
    char* host = conn->connhost[conn->whichhost].host;

    /*
     * If told not to verify the peer name, don't do it. Return true
     * indicating that the verification was successful.
     */
    if (strcmp(conn->sslmode, "verify-full") != 0)
        return true;

    /* First find out the name's length and allocate a buffer for it. */
    len = X509_NAME_get_text_by_NID(X509_get_subject_name(conn->peer), NID_commonName, NULL, 0);
    if (len == -1) {
        printfPQExpBuffer(
            &conn->errorMessage, libpq_gettext("could not get server common name from server certificate\n"));
        return false;
    }

    peer_cn = (char*)malloc(len + 1);
    if (peer_cn == NULL) {
        printfPQExpBuffer(&conn->errorMessage, libpq_gettext("out of memory\n"));
        return false;
    }

    r = X509_NAME_get_text_by_NID(X509_get_subject_name(conn->peer), NID_commonName, peer_cn, len + 1);
    if (r != len) {
        printfPQExpBuffer(
            &conn->errorMessage, libpq_gettext("could not get server common name from server certificate\n"));
        libpq_free(peer_cn);
        return false;
    }

    peer_cn[len] = '\0';
    if ((size_t)len != strlen(peer_cn)) {
        printfPQExpBuffer(&conn->errorMessage,
            libpq_gettext("SSL certificate's common name contains embedded null, remote datanode %s, errno:%s\n"),
            conn->remote_nodename, strerror(errno));
        libpq_free(peer_cn);
        return false;
    }

    /*
     * We got the peer's common name. Now compare it against the originally
     * given hostname.
     */
    if (!((host != NULL) && host[0] != '\0')) {
        printfPQExpBuffer(
            &conn->errorMessage, libpq_gettext("host name must be specified for a verified SSL connection\n"));
        result = false;
    } else {
        if (pg_strcasecmp(peer_cn, host) == 0)
            /* Exact name match */
            result = true;
        else if (wildcard_certificate_match(peer_cn, host))
            /* Matched wildcard certificate */
            result = true;
        else {
            printfPQExpBuffer(&conn->errorMessage,
                libpq_gettext("server common name \"%s\" does not match host name \"%s\"\n"),
                peer_cn,
                host);
            result = false;
        }
    }
    libpq_free(peer_cn);
    return result;
}

#ifdef ENABLE_THREAD_SAFETY
/*
 *  Callback functions for OpenSSL internal locking
 */

static GS_UINT32 pq_threadidcallback(void)
{
    /*
     * This is not standards-compliant.  pthread_self() returns pthread_t, and
     * shouldn't be cast to unsigned long, but CRYPTO_set_id_callback requires
     * it, so we have to do it.
     */
    return (GS_UINT32)pthread_self();
}

#endif /* ENABLE_THREAD_SAFETY */

/*
 *  This function is needed because if the libpq library is unloaded
 *  from the application, the callback functions will no longer exist when
 *  libcrypto is used by other parts of the system.  For this reason,
 *  we unregister the callback functions when the last libpq
 *  connection is closed.  (The same would apply for OpenSSL callbacks
 *  if we had any.)
 *
 *  Callbacks are only set when we're compiled in threadsafe mode, so
 *  we only need to remove them in this case.
 */
static void destroy_ssl_system(PGconn* conn)
{
    bool is_gc_fdw_client = false;
    if ((conn->fbappname != NULL) && strcmp(conn->fbappname, "gc_fdw") == 0) {
        is_gc_fdw_client = true;
    }

#ifdef ENABLE_THREAD_SAFETY
    /* Mutex is created in initialize_ssl_system() */
    if (pthread_mutex_lock(&ssl_config_mutex))
        return;

    if (pq_init_crypto_lib && ssl_open_connections > 0)
        --ssl_open_connections;

    if (pq_init_crypto_lib && ssl_open_connections == 0 && !is_gc_fdw_client) {
        /* No connections left, unregister libcrypto callbacks */
        CRYPTO_set_id_callback(NULL);

        /*
         * We don't free the lock array or the SSL_context. If we get another connection in this
         * process, we will just re-use them with the existing mutexes.
         *
         * This means we leak a little memory on repeated load/unload of the
         * library.
         */
    }

    pthread_mutex_unlock(&ssl_config_mutex);
#endif
    /* Free SSL context */
    if (SSL_context != NULL) {
        SSL_CTX_free(SSL_context);
        SSL_context = NULL;
    }
}
/*
* Brief     : static int init_ssl_system(PGconn *conn)
* Description   : Initialize SSL system, in particular creating the SSL_context object
          that will be shared by all SSL-using connections in this process
*/
static int init_ssl_system(PGconn* conn)
{
    bool is_gc_fdw_client = false;
    if ((conn->fbappname != NULL) && strcmp(conn->fbappname, "gc_fdw") == 0) {
        is_gc_fdw_client = true;
    }

#ifdef ENABLE_THREAD_SAFETY
    int rc = 0;

#ifdef WIN32
    /* Also see similar code in fe-connect.c, default_threadlock() */
    if (ssl_config_mutex == NULL) {
        while (InterlockedExchange(&win32_ssl_create_mutex, 1) == 1)
            /* loop, another thread own the lock */;
        if (ssl_config_mutex == NULL) {
            if (pthread_mutex_init(&ssl_config_mutex, NULL))
                return -1;
        }
        InterlockedExchange(&win32_ssl_create_mutex, 0);
    }
#endif

    if ((rc = pthread_mutex_lock(&ssl_config_mutex))) {
        printfPQExpBuffer(&conn->errorMessage, libpq_gettext("could not acquire mutex: %s\n"), strerror(rc));
        return -1;
    }

    if (pq_init_crypto_lib) {
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-value"
        if (ssl_open_connections++ == 0 && !is_gc_fdw_client) {

            /* These are only required for threaded libcrypto applications */
            CRYPTO_THREADID_set_callback(pq_threadidcallback);
        }
#pragma GCC diagnostic pop
    }
#endif /* ENABLE_THREAD_SAFETY */

    if (SSL_context == NULL) {
        if (pq_init_ssl_lib) {
            if (OPENSSL_init_ssl(0, NULL) != 1) {
                char* err = SSLerrmessage();
                printfPQExpBuffer(&conn->errorMessage, libpq_gettext("Failed to initialize ssl library:%s\n"), err);
                SSLerrfree(err);
#ifdef ENABLE_THREAD_SAFETY
                pthread_mutex_unlock(&ssl_config_mutex);
#endif
                return -1;
            }
            SSL_load_error_strings();
        }
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
#ifdef USE_TASSL
        if (conn->ssltlcp) {
            SSL_context = SSL_CTX_new(CNTLS_client_method());
        } else {
            SSL_context = SSL_CTX_new(TLSv1_2_method());
        }   
#else
        SSL_context = SSL_CTX_new(TLSv1_2_method());
#endif 
        if (SSL_context == NULL) {
            char* err = SSLerrmessage();

            printfPQExpBuffer(&conn->errorMessage,
                libpq_gettext("could not create SSL context: %s, remote datanode %s, errno: %s\n"),
                err, conn->remote_nodename, strerror(errno));
            SSLerrfree(err);
#pragma GCC diagnostic pop
#ifdef ENABLE_THREAD_SAFETY
            pthread_mutex_unlock(&ssl_config_mutex);
#endif
            return -1;
        }

        /*
         * Disable  moving-write-buffer sanity check, because it
         * causes unnecessary failures in nonblocking send cases.
         */
        SSL_CTX_set_mode(SSL_context, SSL_MODE_ACCEPT_MOVING_WRITE_BUFFER);
    }

#ifdef ENABLE_THREAD_SAFETY
    pthread_mutex_unlock(&ssl_config_mutex);
#endif
    return 0;
}

/*
 * @Brief   : static bool set_client_ssl_ciphers()
 * @Description : set client security cipherslist.
 * @return  : return true if success to set SSL security cipherslist , otherwise return false.
 */
#ifndef ENABLE_UT
static
#endif  // ENABLE_UT
bool set_client_ssl_ciphers()
{
    int default_ciphers_count = 0;

    for (int i = 0; ssl_ciphers_map[i] != NULL; i++) {
        default_ciphers_count++;
    }

    /* Set up the client security cipher list. */
    if (SSL_CTX_set_cipher_list_ex(SSL_context, ssl_ciphers_map, default_ciphers_count) != 1) {
        return false;
    }
    return true;
}

/*
 * Brief        : static int initialize_SSL(PGconn *conn)
 * Description  : Initialize (potentially) per-connection SSL data, namely the client certificate, private key, and
 * trusted CA certs. Notes      : use internal ssl objects
 */
#ifdef ENABLE_UT
#ifdef stat
#undef stat
#endif  // stat
#define stat(path, buf) ((buf)->st_mode = (S_IRWXG | S_IRWXO), 0)
#else
static
#endif

/* Read the client certificate file */
int LoadSslCertFile(PGconn* conn, bool have_homedir, const PathData *homedir, bool *have_cert, bool enc)
{
    struct stat buf;
    char fnbuf[MAXPGPATH] = {0};
    char sebuf[256];
    errno_t rc = 0;
    int nRet = 0;
    char *cert;
    const char *certfile;
#ifdef USE_TASSL
    cert = enc ? conn->sslenccert : conn->sslcert;
    certfile = enc ? USER_ENC_CERT_FILE : USER_CERT_FILE;
#else
    cert = conn->sslcert;
    certfile = USER_CERT_FILE;
#endif

    if ((cert != NULL) && strlen(cert) > 0) {
        rc = strncpy_s(fnbuf, MAXPGPATH, cert, strlen(cert));
        securec_check_c(rc, "\0", "\0");
        fnbuf[MAXPGPATH - 1] = '\0';
    } else if (have_homedir) {
        nRet = snprintf_s(fnbuf, MAXPGPATH, MAXPGPATH - 1, "%s/%s", homedir->data, certfile);
        securec_check_ss_c(nRet, "\0", "\0");

    } else
        fnbuf[0] = '\0';
    if (fnbuf[0] == '\0') {
        /* no home directory, proceed without a client cert */
        *have_cert = false;
    } else if (stat(fnbuf, &buf) != 0) {
        /*
         * If file is not present, just go on without a client cert; server
         * might or might not accept the connection.  Any other error,
         * however, is grounds for complaint.
         */
        if (errno != ENOENT && errno != ENOTDIR) {
            printfPQExpBuffer(&conn->errorMessage,
                libpq_gettext("could not open certificate file \"%s\": %s\n"),
                fnbuf,
                pqStrerror(errno, sebuf, sizeof(sebuf)));
            return -1;
        }
        *have_cert = false;
    } else {
        /*
         * Cert file exists, so load it.  Since the ssl lib doesn't provide the
         * equivalent of "SSL_use_certificate_chain_file", we actually have to
         * load the file twice.  The first call loads any extra certs after
         * the first one into chain-cert storage associated with the
         * SSL_context.  The second call loads the first cert (only) into the
         * SSL object, where it will be correctly paired with the private key
         * we load below.  We do it this way so that each connection
         * understands which subject cert to present, in case different
         * sslcert settings are used for different connections in the same
         * process.
         *
         * NOTE: This function may also modify our SSL_context and therefore
         * we have to lock around this call and any places where we use the
         * SSL_context struct.
         */
#ifdef ENABLE_THREAD_SAFETY
        int rc = 0;
        if ((rc = pthread_mutex_lock(&ssl_config_mutex))) {
            printfPQExpBuffer(&conn->errorMessage, libpq_gettext("could not acquire mutex: %s\n"), strerror(rc));
            return -1;
        }
#endif
        /* set the default password for certificate/private key loading */
        if (init_client_ssl_passwd(conn->ssl, fnbuf, conn->pguser, conn, enc) != 0) {
#ifdef ENABLE_THREAD_SAFETY
            (void)pthread_mutex_unlock(&ssl_config_mutex);
#endif
            return -1;
        }
        /* check certificate file permission */
#ifndef WIN32
        if (!S_ISREG(buf.st_mode) || (buf.st_mode & (S_IRWXG | S_IRWXO)) || ((buf.st_mode & S_IRWXU) == S_IRWXU)) {
#ifdef ENABLE_THREAD_SAFETY
            (void)pthread_mutex_unlock(&ssl_config_mutex);
#endif
            printfPQExpBuffer(
                &conn->errorMessage, libpq_gettext("The file \"%s\" permission should be u=rw(600) or less.\n"), fnbuf);
            return -1;
        }
#endif
        if (SSL_CTX_use_certificate_chain_file(SSL_context, fnbuf) != 1) {
            char* err = SSLerrmessage();

            printfPQExpBuffer(
                &conn->errorMessage, libpq_gettext("could not read certificate file \"%s\": %s\n"), fnbuf, err);
            SSLerrfree(err);
#ifdef ENABLE_THREAD_SAFETY
            (void)pthread_mutex_unlock(&ssl_config_mutex);
#endif
            return -1;
        }
        if (SSL_use_certificate_file(conn->ssl, fnbuf, SSL_FILETYPE_PEM) != 1) {
            char* err = SSLerrmessage();

            printfPQExpBuffer(
                &conn->errorMessage, libpq_gettext("could not read certificate file \"%s\": %s\n"), fnbuf, err);
            SSLerrfree(err);
#ifdef ENABLE_THREAD_SAFETY
            (void)pthread_mutex_unlock(&ssl_config_mutex);
#endif
            return -1;
        }

        /* need to load the associated private key, too */
        *have_cert = true;
#ifdef ENABLE_THREAD_SAFETY
        (void)pthread_mutex_unlock(&ssl_config_mutex);
#endif
    }
    return 0;
}

int LoadSslKeyFile(PGconn* conn, bool have_homedir, const PathData *homedir, bool have_cert, bool enc)
{
    struct stat buf;
    char fnbuf[MAXPGPATH] = {0};
    errno_t rc = 0;
    int nRet = 0;
    /*
     * Read the SSL key. If a key is specified, treat it as an engine:key
     * combination if there is colon present - we don't support files with
     * colon in the name. The exception is if the second character is a colon,
     * in which case it can be a Windows filename with drive specification.
     */
    char *key;
    const char *keyfile;
#ifdef USE_TASSL
    key = enc ? conn->sslenckey : conn->sslkey;
    keyfile = enc ? USER_ENC_KEY_FILE : USER_KEY_FILE;
#else
    key = conn->sslkey;
    keyfile = USER_KEY_FILE;
#endif

    if (have_cert && key != NULL && strlen(key) > 0) {
        rc = strncpy_s(fnbuf, MAXPGPATH, key, strlen(key));
        securec_check_c(rc, "\0", "\0");
        fnbuf[MAXPGPATH - 1] = '\0';
    } else if (have_homedir) {
        /* No PGSSLKEY specified, load default file */
        nRet = snprintf_s(fnbuf, MAXPGPATH, MAXPGPATH - 1, "%s/%s", homedir->data, keyfile);
        securec_check_ss_c(nRet, "\0", "\0");
    } else
        fnbuf[0] = '\0';

    if (have_cert && fnbuf[0] != '\0') {
        /* read the client key from file */
        if (stat(fnbuf, &buf) != 0) {
            printfPQExpBuffer(
                &conn->errorMessage, libpq_gettext("certificate present, but not private key file \"%s\"\n"), fnbuf);
            return -1;
        }
        /* check key file permission */
#ifndef WIN32
        if (!S_ISREG(buf.st_mode) || (buf.st_mode & (S_IRWXG | S_IRWXO)) || ((buf.st_mode & S_IRWXU) == S_IRWXU)) {
            printfPQExpBuffer(
                &conn->errorMessage, libpq_gettext("The file \"%s\" permission should be u=rw(600) or less.\n"), fnbuf);
            return -1;
        }
#endif
        if (SSL_use_PrivateKey_file(conn->ssl, fnbuf, SSL_FILETYPE_PEM) != 1) {
            char* err = SSLerrmessage();

            printfPQExpBuffer(
                &conn->errorMessage, libpq_gettext("could not load private key file \"%s\": %s\n"), fnbuf, err);
            SSLerrfree(err);
            return -1;
        }
    }
        /* verify that the cert and key go together */
    if (have_cert && SSL_check_private_key(conn->ssl) != 1) {
        char* err = SSLerrmessage();

        printfPQExpBuffer(
            &conn->errorMessage, libpq_gettext("certificate does not match private key file \"%s\": %s\n"), fnbuf, err);
        SSLerrfree(err);
        return -1;
    }

    /* set up the allowed cipher list */
    if (!set_client_ssl_ciphers()) {
        char* err = SSLerrmessage();
        printfPQExpBuffer(&conn->errorMessage, libpq_gettext("SSL_ctxSetCipherList \"%s\": %s\n"), fnbuf, err);
        SSLerrfree(err);
        return -1;
    }
    return 0;
}


void LoadSslCrlFile(PGconn* conn, bool have_homedir, const PathData *homedir)
{
    struct stat buf;
    char fnbuf[MAXPGPATH] = {0};
    errno_t rc = 0;
    int nRet = 0;
    bool userSetSslCrl = false;
    X509_STORE* cvstore = SSL_CTX_get_cert_store(SSL_context);
    if (cvstore == NULL) {
        return;
    }

    if ((conn->sslcrl != NULL) && strlen(conn->sslcrl) > 0) {
        rc = strncpy_s(fnbuf, MAXPGPATH, conn->sslcrl, strlen(conn->sslcrl));
        securec_check_c(rc, "\0", "\0");
        fnbuf[MAXPGPATH - 1] = '\0';
        userSetSslCrl = true;
    } else if (have_homedir) {
        nRet = snprintf_s(fnbuf, MAXPGPATH, MAXPGPATH - 1, "%s/%s", homedir->data, ROOT_CRL_FILE);
        securec_check_ss_c(nRet, "\0", "\0");
    } else {
        fnbuf[0] = '\0';
    }

    if (fnbuf[0] == '\0') {
        return;
    }

    /* Set the flags to check against the complete CRL chain */
    if (stat(fnbuf, &buf) == 0 && X509_STORE_load_locations(cvstore, fnbuf, NULL) == 1) {
        (void)X509_STORE_set_flags(cvstore, X509_V_FLAG_CRL_CHECK);
    } else if (userSetSslCrl) {
        printfPQExpBuffer(&conn->errorMessage,
            libpq_gettext("could not load SSL certificate revocation list (file \"%s\")\n"), fnbuf);
        fprintf(stdout, "Warning: could not load SSL certificate revocation list (file \"%s\")\n", fnbuf);
    }
}

#define MAX_CERTIFICATE_DEPTH_SUPPORTED 20 /* The max certificate depth supported. */
int LoadRootCertFile(PGconn* conn, bool have_homedir, const PathData *homedir)
{
    struct stat buf;
    char fnbuf[MAXPGPATH] = {0};
    errno_t rc = 0;
    int nRet = 0;
    /*
     * If the root cert file exists, load it so we can perform certificate
     * verification. If sslmode is "verify-full" we will also do further
     * verification after the connection has been completed.
     */
    if ((conn->sslrootcert != NULL) && strlen(conn->sslrootcert) > 0) {
        rc = strncpy_s(fnbuf, MAXPGPATH, conn->sslrootcert, strlen(conn->sslrootcert));
        securec_check_c(rc, "\0", "\0");
        fnbuf[MAXPGPATH - 1] = '\0';
    } else if (have_homedir) {
        nRet = snprintf_s(fnbuf, MAXPGPATH, MAXPGPATH - 1, "%s/%s", homedir->data, ROOT_CERT_FILE);
        securec_check_ss_c(nRet, "\0", "\0");
    } else
        fnbuf[0] = '\0';
    if (fnbuf[0] != '\0' && stat(fnbuf, &buf) == 0) {
        if (SSL_CTX_load_verify_locations(SSL_context, fnbuf, NULL) != 1) {
            char* err = SSLerrmessage();

            printfPQExpBuffer(
                &conn->errorMessage, libpq_gettext("could not read root certificate file \"%s\": %s\n"), fnbuf, err);
            SSLerrfree(err);
            return -1;
        }
        /* check root cert file permission */ 
#ifndef WIN32
        if (!S_ISREG(buf.st_mode) || (buf.st_mode & (S_IRWXG | S_IRWXO)) || ((buf.st_mode & S_IRWXU) == S_IRWXU)) {
            printfPQExpBuffer(
                &conn->errorMessage, libpq_gettext("The ca file \"%s\" permission should be u=rw(600) or less.\n"), fnbuf);
            return -1;
        }
#endif
        LoadSslCrlFile(conn, have_homedir, homedir);
        /* Check the DH length to make sure it's at least 2048. */
        SSL_set_security_callback(conn->ssl, ssl_security_DH_ECDH_cb);

        /* set the SSL verify method */
        SSL_set_verify(conn->ssl, SSL_VERIFY_PEER | SSL_VERIFY_FAIL_IF_NO_PEER_CERT, verify_cb);

        /* set the SSL verify depth */
        SSL_set_verify_depth(conn->ssl, (MAX_CERTIFICATE_DEPTH_SUPPORTED - 2));
    } else {
        /*
         * stat() failed; assume root file doesn't exist.  If sslmode is
         * verify-ca or verify-full, this is an error.  Otherwise, continue
         * without performing any server cert verification.
         */
        if (conn->sslmode[0] == 'v') /* "verify-ca" or "verify-full" */
        {
            /*
             * The only way to reach here with an empty filename is if
             * pqGetHomeDirectory failed.  That's a sufficiently unusual case
             * that it seems worth having a specialized error message for it.
             */
            if (fnbuf[0] == '\0')
                printfPQExpBuffer(&conn->errorMessage,
                    libpq_gettext(
                        "could not get home directory to locate root certificate file\n"
                        "Either provide the file or change sslmode to disable server certificate verification.\n"));
            else
                printfPQExpBuffer(&conn->errorMessage,
                    libpq_gettext(
                        "root certificate file \"%s\" does not exist\n"
                        "Either provide the file or change sslmode to disable server certificate verification.\n"),
                    fnbuf);
            return -1;
        }
    }
    return 0;
}

int initialize_SSL(PGconn* conn)
{
    PathData homedir = {{0}};
    bool have_homedir = false;
    bool have_cert = false;
    errno_t rc = 0;
    int retval = 0;

    /*
     * We'll need the home directory if any of the relevant parameters are
     * defaulted.  If pqGetHomeDirectory fails, act as though none of the
     * files could be found.
     */
    if (!((conn->sslcert != NULL) && strlen(conn->sslcert) > 0) ||
        !((conn->sslkey != NULL) && strlen(conn->sslkey) > 0) ||
#ifdef USE_TASSL
        !((conn->sslenccert != NULL) && strlen(conn->sslenccert) > 0) ||
        !((conn->sslenckey != NULL) && strlen(conn->sslenckey) > 0) ||
#endif
        !((conn->sslrootcert != NULL) && strlen(conn->sslrootcert) > 0) ||
        !((conn->sslcrl != NULL) && strlen(conn->sslcrl) > 0))
        have_homedir = pqGetHomeDirectory(homedir.data, MAXPGPATH);
    else /* won't need it */
        have_homedir = false;
    
    retval = LoadSslCertFile(conn, have_homedir, &homedir, &have_cert, false);
    if (retval == -1) {
        return retval;
    }
    g_crl_invalid = false;
    retval = LoadSslKeyFile(conn, have_homedir, &homedir, have_cert, false);
    if (retval == -1) {
        return retval;
    }
#ifdef USE_TASSL
    if (conn->ssltlcp) {
        retval = LoadSslCertFile(conn, have_homedir, &homedir, &have_cert, true);
        if (retval == -1) {
            return retval;
        }
        g_crl_invalid = false;
        retval = LoadSslKeyFile(conn, have_homedir, &homedir, have_cert, true);
        if (retval == -1) {
            return retval;
        }
    }
#endif
    retval = LoadRootCertFile(conn, have_homedir, &homedir);
    if (retval == -1) {
        return retval;
    }
    
    /*
     * Check the signature algorithm.
     * NOTICE : Since the client errorMessage is only output in the error exit scene, the function fprintf is used here.
     * Use the parameter stdout to output an alarm to the log or screen.
     */
    if (check_certificate_signature_algrithm(SSL_context)) {
        fprintf(stdout, "Warning: The client certificate contain a Low-intensity signature algorithm.\n");
    }

    /* Check the certificate expires time, default alarm_days = 90d. */
    const int alarm_days = 90;
    long leftspan = check_certificate_time(SSL_context, alarm_days);
    if (leftspan > 0) {
        int leftdays = leftspan / 86400 > 0 ? leftspan / 86400 : 1;
        if (leftdays > 1) {
            fprintf(stdout, "Warning: The client certificate will expire in %d days.\n", leftdays);
        } else {
            fprintf(stdout, "Warning: The client certificate will expire in %d day.\n", leftdays);
        }
    }
    /*clear the sensitive info in server_key*/
    rc = memset_s(conn->cipher_passwd, CIPHER_LEN, 0, CIPHER_LEN);
    securec_check_c(rc, "\0", "\0");
    return 0;
}
#ifdef ENABLE_UT
#ifdef stat
#undef stat
#endif  // stat
#endif
static void destroySSL(PGconn* conn)
{
    destroy_ssl_system(conn);
}

/*
 *  Attempt to negotiate SSL connection.
 */
#ifndef ENABLE_UT
static
#endif
    PostgresPollingStatusType
    open_client_SSL(PGconn* conn)
{
    int r;

    ERR_clear_error();
    r = SSL_connect(conn->ssl);
    if (r <= 0) {
        int err = SSL_get_error(conn->ssl, r);

        switch (err) {
            case SSL_ERROR_WANT_READ:
                return PGRES_POLLING_READING;

            case SSL_ERROR_WANT_WRITE:
                return PGRES_POLLING_WRITING;

            case SSL_ERROR_SYSCALL: {
                char sebuf[256];

                if (r == -1)
                    printfPQExpBuffer(&conn->errorMessage,
                        libpq_gettext("SSL SYSCALL error: %s\n"),
                        SOCK_STRERROR(SOCK_ERRNO, sebuf, sizeof(sebuf)));
                else
                    printfPQExpBuffer(&conn->errorMessage, libpq_gettext("SSL SYSCALL error: EOF detected\n"));
                close_SSL(conn);
                return PGRES_POLLING_FAILED;
            }
            case SSL_ERROR_SSL: {
                char* err = SSLerrmessage();

                printfPQExpBuffer(&conn->errorMessage, libpq_gettext("SSL error: %s\n"), err);
                SSLerrfree(err);
                close_SSL(conn);
                return PGRES_POLLING_FAILED;
            }

            default:
                printfPQExpBuffer(&conn->errorMessage, libpq_gettext("unrecognized SSL error code: %d\n"), err);
                close_SSL(conn);
                return PGRES_POLLING_FAILED;
        }
    }

    /*
     * We already checked the server certificate in initialize_SSL() using
     * SSL_CTX_set_verify(), if root.crt exists.
     */

    /* get server certificate */
    conn->peer = SSL_get_peer_certificate(conn->ssl);
    if (conn->peer == NULL) {
        char* err = SSLerrmessage();

        printfPQExpBuffer(&conn->errorMessage, libpq_gettext("certificate could not be obtained: %s\n"), err);
        SSLerrfree(err);
        close_SSL(conn);
        return PGRES_POLLING_FAILED;
    }

    if (!verify_peer_name_matches_certificate(conn)) {
        close_SSL(conn);
        return PGRES_POLLING_FAILED;
    }

    /* SSL handshake is complete */
    return PGRES_POLLING_OK;
}

/*
 *  Close SSL connection.
 */
static void close_SSL(PGconn* conn)
{
    bool destroy_needed = false;

    if (conn->ssl != NULL) {
        DECLARE_SIGPIPE_INFO(spinfo);

        /*
         * We can't destroy everything SSL-related here due to the possible
         * later calls to OpenSSL routines which may need our thread
         * callbacks, so set a flag here and check at the end.
         */
        destroy_needed = true;

        DISABLE_SIGPIPE(conn, spinfo, (void)0);
        SSL_shutdown(conn->ssl);
        SSL_free(conn->ssl);
        conn->ssl = NULL;
        /* We have to assume we got EPIPE */
        REMEMBER_EPIPE(spinfo, true);
        RESTORE_SIGPIPE(conn, spinfo);
    }

    if (conn->peer != NULL) {
        X509_free(conn->peer);
        conn->peer = NULL;
    }

    /*
     * This will remove our SSL locking hooks, if this is the last SSL
     * connection, which means we must wait to call it until after all
     * SSL calls have been made, otherwise we can end up with a race
     * condition and possible deadlocks.
     *
     * See comments above destroy_ssl_system().
     */
    if (destroy_needed)
        pqsecure_destroy(conn);
}

/*
 * Obtain reason string for last SSL error
 *
 * Some caution is needed here since ERR_reason_error_string will
 * return NULL if it doesn't recognize the error code.  We don't
 * want to return NULL ever.
 */
static char ssl_nomem[] = "out of memory allocating error description";

#define SSL_ERR_LEN 128

#ifndef ENABLE_UT
static
#endif
    char*
    SSLerrmessage(void)
{
    unsigned long errcode;
    const char* errreason = NULL;
    char* errbuf = NULL;

    errbuf = (char*)malloc(SSL_ERR_LEN);
    if (errbuf == NULL)
        return ssl_nomem;
    errcode = ERR_get_error();
    if (errcode == 0) {
        check_sprintf_s(sprintf_s(errbuf, SSL_ERR_LEN, libpq_gettext("no SSL error reported")));
        return errbuf;
    }
    errreason = ERR_reason_error_string(errcode);
    if (errreason != NULL) {
        check_strncpy_s(strncpy_s(errbuf, SSL_ERR_LEN, errreason, strlen(errreason)));
        return errbuf;
    }
    check_sprintf_s(sprintf_s(errbuf, SSL_ERR_LEN, libpq_gettext("SSL error code %lu"), errcode));
    return errbuf;
}

static void SSLerrfree(char* buf)
{
    if (buf != ssl_nomem) {
        libpq_free(buf);
    }
}

/*
 *  Return pointer to OpenSSL object.
 */
void* PQgetssl(PGconn* conn)
{
    if (conn == NULL)
        return NULL;
    return conn->ssl;
}
#else  /* !USE_SSL */

void* PQgetssl(PGconn* conn)
{
    return NULL;
}
#endif /* USE_SSL */

#if defined(ENABLE_THREAD_SAFETY) && !defined(WIN32)

/*
 *  Block SIGPIPE for this thread.  This prevents send()/write() from exiting
 *  the application.
 */
int pq_block_sigpipe(sigset_t* osigset, bool* sigpipe_pending)
{
    sigset_t sigpipe_sigset;
    sigset_t sigset;

    sigemptyset(&sigpipe_sigset);
    sigaddset(&sigpipe_sigset, SIGPIPE);

    /* Block SIGPIPE and save previous mask for later reset */
    SOCK_ERRNO_SET(pthread_sigmask(SIG_BLOCK, &sigpipe_sigset, osigset));
    if (SOCK_ERRNO)
        return -1;

    /* We can have a pending SIGPIPE only if it was blocked before */
    if (sigismember(osigset, SIGPIPE)) {
        /* Is there a pending SIGPIPE? */
        if (sigpending(&sigset) != 0)
            return -1;

        if (sigismember(&sigset, SIGPIPE))
            *sigpipe_pending = true;
        else
            *sigpipe_pending = false;
    } else
        *sigpipe_pending = false;

    return 0;
}

/*
 *  Discard any pending SIGPIPE and reset the signal mask.
 *
 * Note: we are effectively assuming here that the C library doesn't queue
 * up multiple SIGPIPE events.  If it did, then we'd accidentally leave
 * ours in the queue when an event was already pending and we got another.
 * As long as it doesn't queue multiple events, we're OK because the caller
 * can't tell the difference.
 *
 * The caller should say got_epipe = FALSE if it is certain that it
 * didn't get an EPIPE error; in that case we'll skip the clear operation
 * and things are definitely OK, queuing or no.  If it got one or might have
 * gotten one, pass got_epipe = TRUE.
 *
 * We do not want this to change errno, since if it did that could lose
 * the error code from a preceding send().  We essentially assume that if
 * we were able to do pq_block_sigpipe(), this can't fail.
 */
void pq_reset_sigpipe(sigset_t* osigset, bool sigpipe_pending, bool got_epipe)
{
    int save_errno = SOCK_ERRNO;
    int signo;
    sigset_t sigset;

    /* Clear SIGPIPE only if none was pending */
    if (got_epipe && !sigpipe_pending) {
        if (sigpending(&sigset) == 0 && sigismember(&sigset, SIGPIPE)) {
            sigset_t sigpipe_sigset;

            sigemptyset(&sigpipe_sigset);
            sigaddset(&sigpipe_sigset, SIGPIPE);

            sigwait(&sigpipe_sigset, &signo);
        }
    }

    /* Restore saved block mask */
    pthread_sigmask(SIG_SETMASK, osigset, NULL);

    SOCK_ERRNO_SET(save_errno);
}

#endif /* ENABLE_THREAD_SAFETY && !WIN32 */

/* set the default password for certificate/private key loading */
#ifndef ENABLE_UT
static
#endif
    int
    init_client_ssl_passwd(SSL* pstContext, const char* path, const char* username, PGconn* conn, bool enc)
{
    char* CertFilesDir = NULL;
    char CertFilesPath[MAXPGPATH] = {0};
    char CipherFileName[MAXPGPATH] = {0};

    struct stat st;
    int retval = 0;
#ifdef USE_TASSL
    KeyMode mode = enc ? CLIENT_ENC_MODE : CLIENT_MODE;
#else
    KeyMode mode = CLIENT_MODE;
#endif
    int nRet = 0;

    char *uname = NULL;

    if (NULL == path || '\0' == path[0]) {
        printfPQExpBuffer(&conn->errorMessage, libpq_gettext("invalid cert file path\n"));
        return -1;
    }

    nRet = strncpy_s(CertFilesPath, MAXPGPATH, path, MAXPGPATH - 1);
    securec_check_ss_c(nRet, "\0", "\0");

    CertFilesDir = CertFilesPath;
    get_parent_directory(CertFilesDir);

    nRet = memset_s(conn->cipher_passwd, CIPHER_LEN + 1, 0, CIPHER_LEN + 1);
    securec_check_ss_c(nRet, "\0", "\0");
    
    /*check whether the cipher and rand files begins with username exist.
    if exist, decrypt it.
    if not,decrypt the default cipher and rand files begins with client%.
    Because,for every client user mayown certification and private key*/

    if(NULL != username) {
#ifdef USE_TASSL
        nRet = snprintf_s(CipherFileName, MAXPGPATH, MAXPGPATH - 1, "%s/%s%s%s", CertFilesDir, username, enc ? "_enc" : "", CIPHER_KEY_FILE);
#else
        nRet = snprintf_s(CipherFileName, MAXPGPATH, MAXPGPATH - 1, "%s/%s%s", CertFilesDir, username, CIPHER_KEY_FILE);
#endif
        securec_check_ss_c(nRet, "\0", "\0");

        if (lstat(CipherFileName, &st) < 0) {
            uname = NULL;
        }else {
            uname = (char*)username;
        }
    }

    retval = check_permission_cipher_file(CertFilesDir, conn, uname, enc);
    if (retval != 1) {
        return retval;
    }
        
    decode_cipher_files(mode, uname, CertFilesDir, conn->cipher_passwd);

    SSL_set_default_passwd_cb_userdata(pstContext, (char*)conn->cipher_passwd);
    return 0;
}

#ifdef ENABLE_UT
#ifdef lstat
#undef lstat
#endif
#define lstat(path, sb) 0
#ifdef S_ISREG
#undef S_ISREG
#endif
#define S_ISREG(x) 0
#else
static
#endif  // ENABLE_UT
/* Check permissions of cipher file and rand file in client */
int check_permission_cipher_file(const char* parent_dir, PGconn* conn, const char* username ,bool enc)
{
    char cipher_file[MAXPGPATH] = {0};
    char rand_file[MAXPGPATH] = {0};
    struct stat cipherbuf;
    struct stat randbuf;
    int nRet = 0;
#ifdef USE_TASSL
    if (NULL == username) {
        nRet = snprintf_s(cipher_file, MAXPGPATH, MAXPGPATH - 1, "%s/client%s%s", parent_dir, enc ? "_enc" : "", CIPHER_KEY_FILE);
        securec_check_ss_c(nRet, "\0", "\0");
        nRet = snprintf_s(rand_file, MAXPGPATH, MAXPGPATH - 1, "%s/client%s%s", parent_dir, enc ? "_enc" : "", RAN_KEY_FILE);
        securec_check_ss_c(nRet, "\0", "\0");
    } else {
        nRet = snprintf_s(cipher_file, MAXPGPATH, MAXPGPATH - 1, "%s/%s%s%s", parent_dir, username, enc ? "_enc" : "", CIPHER_KEY_FILE);
        securec_check_ss_c(nRet, "\0", "\0");
        nRet = snprintf_s(rand_file, MAXPGPATH, MAXPGPATH - 1, "%s/%s%s%s", parent_dir, username, enc ? "_enc" : "", RAN_KEY_FILE);
        securec_check_ss_c(nRet, "\0", "\0");
    }
#else
    if (NULL == username) {
        nRet = snprintf_s(cipher_file, MAXPGPATH, MAXPGPATH - 1, "%s/client%s", parent_dir, CIPHER_KEY_FILE);
        securec_check_ss_c(nRet, "\0", "\0");
        nRet = snprintf_s(rand_file, MAXPGPATH, MAXPGPATH - 1, "%s/client%s", parent_dir, RAN_KEY_FILE);
        securec_check_ss_c(nRet, "\0", "\0");
    } else {
        nRet = snprintf_s(cipher_file, MAXPGPATH, MAXPGPATH - 1, "%s/%s%s", parent_dir, username, CIPHER_KEY_FILE);
        securec_check_ss_c(nRet, "\0", "\0");
        nRet = snprintf_s(rand_file, MAXPGPATH, MAXPGPATH - 1, "%s/%s%s", parent_dir, username, RAN_KEY_FILE);
        securec_check_ss_c(nRet, "\0", "\0");
    }
#endif
    /*cipher file or rand file do not exist,skip check the permission.
    For key and certications without password,it is also ok*/
    if (lstat(cipher_file, &cipherbuf) != 0 || lstat(rand_file, &randbuf) != 0)
        return 0;

        /*cipher file and rand file exist,so check whether permissions meets the requirements */
#ifndef WIN32
    if (!S_ISREG(cipherbuf.st_mode) || (cipherbuf.st_mode & (S_IRWXG | S_IRWXO)) || ((cipherbuf.st_mode & S_IRWXU) == S_IRWXU)) {
        printfPQExpBuffer(
            &conn->errorMessage, libpq_gettext("The file \"%s\" permission should be u=rw(600) or less.\n"), cipher_file);
        return -1;
    }
    if (!S_ISREG(randbuf.st_mode) || (randbuf.st_mode & (S_IRWXG | S_IRWXO)) || ((randbuf.st_mode & S_IRWXU) == S_IRWXU)) {
        printfPQExpBuffer(
            &conn->errorMessage, libpq_gettext("The file \"%s\" permission should be u=rw(600) or less.\n"), rand_file);
        return -1;
    }
#endif
    /*files exist,and permission is ok!*/
    return 1;
}

int ssl_security_DH_ECDH_cb(const SSL* s, const SSL_CTX* ctx, int op, int bits, int nid, void* other, void* ex)
{
    switch (op) {
        case SSL_SECOP_TMP_DH:
            if ((bits < 112) || (bits > 128)) {
                return 0;
            }
        default:
            return 1;
    }
    return 1;
}

#ifdef USE_SSL

static char* ssl_cipher_list2string(const char* ciphers[], const int num)
{
    int i;
    int catlen = 0;
    char* cipher_buf = NULL;
    errno_t errorno = 0;

    size_t CIPHER_BUF_SIZE = 0;
    for (i = 0; i < num; i++) {
        CIPHER_BUF_SIZE += (strlen(ciphers[i]) + 2);
    }

    cipher_buf = (char*)OPENSSL_zalloc(CIPHER_BUF_SIZE);
    if (cipher_buf == NULL) {
        return NULL;
    }

    for (i = 0; i < num; i++) {
        errorno = strncpy_s(cipher_buf + catlen, strlen(ciphers[i]) + 1, ciphers[i], strlen(ciphers[i]));
        securec_check_c(errorno, "\0", "\0");

        catlen += strlen(ciphers[i]);

        if (i < num - 1) {
            errorno = strncpy_s(cipher_buf + catlen, 2, ":", 1);
            securec_check_c(errorno, "\0", "\0");

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
#endif
