/*
 * Copyright (c) 2021 Huawei Technologies Co.,Ltd.
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 *          http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 * -------------------------------------------------------------------------
 *
 * libcomm_server_ssl.cpp
 *
 * IDENTIFICATION
 *    src/gausskernel/cbb/communication/libcomm_utils/libcomm_server_ssl.cpp
 *
 * -------------------------------------------------------------------------
 */

#include <arpa/inet.h>
#include <ctype.h>
#include <errno.h>
#include <fcntl.h>
#include <libcgroup.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <netdb.h>
#include <net/if.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/wait.h>
#include <sys/param.h>
#include <sys/time.h>
#include <unistd.h>
#include <signal.h>

#include "../libcomm_core/mc_tcp.h"
#include "../libcomm_core/mc_poller.h"
#include "libcomm_thread.h"
#include "libcomm_lqueue.h"
#include "libcomm_queue.h"
#include "libcomm_lock_free_queue.h"
#include "distributelayer/streamCore.h"
#include "distributelayer/streamProducer.h"
#include "pgxc/poolmgr.h"
#include "libpq/auth.h"
#include "libpq/pqsignal.h"
#include "storage/ipc.h"
#include "utils/ps_status.h"
#include "utils/dynahash.h"

#include "vecexecutor/vectorbatch.h"
#include "vecexecutor/vecnodes.h"
#include "executor/exec/execStream.h"
#include "miscadmin.h"
#include "gssignal/gs_signal.h"
#include "pgxc/pgxc.h"
#include "libcomm/libcomm.h"
#include "../libcomm_common.h"

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

#define MAX_CERTIFICATE_DEPTH_SUPPORTED 20 /* The max certificate depth supported. */

/* ------------------------------------------------------------ */
/*                        SSL specific code                     */
/* ------------------------------------------------------------ */

typedef enum {
    DHKey768 = 1,
    DHKey1024,
    DHKey1536,
    DHKey2048,
    DHKey3072,
    DHKey4096,
    DHKey6144,
    DHKey8192
} COMM_SSL_DHKeyLength;

/* security ciphers suites in SSL connection */
const char* comm_ssl_ciphers_map[] = {
    TLS1_TXT_ECDHE_RSA_WITH_AES_128_GCM_SHA256,     /* TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256 */
    TLS1_TXT_ECDHE_RSA_WITH_AES_256_GCM_SHA384,     /* TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384 */
    TLS1_TXT_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,   /* TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256 */
    TLS1_TXT_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,   /* TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384 */
    NULL};

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
int comm_ssl_verify_cb(int code, X509_STORE_CTX* ctx)
{
    return code;
}

/*
 *  This callback is used to copy SSL information messages
 *  into the PostgreSQL log.
 */
static void comm_ssl_info_cb(const SSL* ssl, int type, int args)
{
    switch (type) {
        case SSL_CB_HANDSHAKE_START:
            LIBCOMM_ELOG(DEBUG4, "SSL: handshake start");
            break;
        case SSL_CB_HANDSHAKE_DONE:
            LIBCOMM_ELOG(DEBUG4, "SSL: handshake done");
            break;
        case SSL_CB_ACCEPT_LOOP:
            LIBCOMM_ELOG(DEBUG4, "SSL: accept loop");
            break;
        case SSL_CB_ACCEPT_EXIT:
            LIBCOMM_ELOG(DEBUG4, "SSL: accept exit (%d)", args);
            break;
        case SSL_CB_CONNECT_LOOP:
            LIBCOMM_ELOG(DEBUG4, "SSL: connect loop");
            break;
        case SSL_CB_CONNECT_EXIT:
            LIBCOMM_ELOG(DEBUG4, "SSL: connect exit (%d)", args);
            break;
        case SSL_CB_READ_ALERT:
            LIBCOMM_ELOG(DEBUG4, "SSL: read alert (0x%04x)", args);
            break;
        case SSL_CB_WRITE_ALERT:
            LIBCOMM_ELOG(DEBUG4, "SSL: write alert (0x%04x)", args);
            break;
        default:
            break;
    }
}

char* comm_ssl_get_cipher_string(const char* ciphers[], const int num)
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
 * Brief         : static int comm_ssl_set_cipher_list(SSL_CTX *ctx, const char* ciphers[], const int num)
 * Description   : set ssl ciphers.
 */
static int comm_ssl_set_cipher_list(SSL_CTX* ctx, const char* ciphers[], const int num)
{
    int ret = 0;
    int rc = 0;
    char* cipher_buf = NULL;

    if (ctx == NULL) {
        return 0;
    }

    cipher_buf = comm_ssl_get_cipher_string(ciphers, num);
    if (cipher_buf == NULL) {
        return 0;
    }

    ret = SSL_CTX_set_cipher_list(ctx, cipher_buf);
    rc = memset_s(cipher_buf, strlen(cipher_buf) + 1, 0, strlen(cipher_buf) + 1);
    securec_check(rc, "\0", "\0");
    OPENSSL_free(cipher_buf);
    return ret;
}

/*
 * Obtain reason string for last SSL error
 *
 * Some caution is needed here since ERR_reason_error_string will
 * return NULL if it doesn't recognize the error code.  We don't
 * want to return NULL ever.
 */
static const char* comm_ssl_get_errmsg(void)
{
    unsigned long errcode;
    const char* errreason = NULL;
    static char errbuf[32];

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

/*
 *  Close SSL connection.
 */
void comm_ssl_close(SSL_INFO* port)
{
    LIBCOMM_ELOG(LOG, "comm_ssl_close, sock is %d", port->sock);
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
}

ssize_t comm_ssl_read(SSL_INFO* port, void* ptr, size_t len)
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
    n = recv(port->sock, ptr, len, 0);
    END_NET_RECV_INFO(n);
#ifdef WIN32
    pgwin32_noblock = false;
#endif

    return n;
}

ssize_t comm_ssl_write(SSL_INFO* port, const void* ptr, size_t len)
{
    ssize_t n;

#ifdef WIN32
    pgwin32_noblock = true;
#endif
    PGSTAT_INIT_TIME_RECORD();
    PGSTAT_START_TIME_RECORD();
    n = send(port->sock, ptr, len, 0);
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
        SSL_INFO* myPort = (SSL_INFO*)BIO_get_data(h);
        if (myPort == NULL) {
            return 0;
        }

        prepare_for_client_read();
        res = comm_ssl_read(myPort, buf, size);
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
    SSL_INFO* myPort = (SSL_INFO*)BIO_get_data(h);
    if (myPort == NULL) {
        return 0;
    }

    res = comm_ssl_write(myPort, (void*)buf, size);
    BIO_clear_retry_flags(h);
    if (res <= 0) {
        /* If we were interrupted, tell caller to retry */
        if (errno == EINTR || errno == EWOULDBLOCK || errno == EAGAIN) {
            BIO_set_retry_write(h);
        }
    }

    return res;
}

static BIO_METHOD* comm_ssl_get_BIO_socket(void)
{
    static BIO_METHOD* my_bio_methods = NULL;
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
static int comm_ssl_set_fd(SSL_INFO* port, int fd)
{
    BIO* bio = NULL;
    BIO_METHOD* bio_method = NULL;

    bio_method = comm_ssl_get_BIO_socket();
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

/*
 * Brief        : int open_server_SSL(SSL_INFO *port)
 * Description  : Attempt to negotiate SSL connection.
 */
int comm_ssl_open_server(SSL_INFO* port, int fd)
{
    int r;
    int err;

    Assert(port->ssl == NULL);
    Assert(port->peer == NULL);

    if (port->ssl != NULL) {
        LIBCOMM_ELOG(WARNING, "comm_ssl_open_server port->ssl is not null(%p), fd is %d", port->ssl, fd);
        return -1;
    }

    if (port->peer != NULL) {
        LIBCOMM_ELOG(WARNING, "comm_ssl_open_server port->peer is not null(%p), fd is %d", port->peer, fd);
        return -1;
    }

    if (g_instance.attr.attr_network.SSL_server_context == NULL) {
        comm_initialize_SSL();
        if (g_instance.attr.attr_network.SSL_server_context != NULL) {
            LIBCOMM_ELOG(LOG, "comm_initialize_SSL in comm_ssl_open_server success");
        } else {
            LIBCOMM_ELOG(WARNING, "comm_initialize_SSL in comm_ssl_open_server failed");
            return -1;
        }
    }

    port->ssl = SSL_new(g_instance.attr.attr_network.SSL_server_context);
    if (port->ssl == NULL) {
        LIBCOMM_ELOG(WARNING, "SSL_new in comm_ssl_open_server failed");
        return -2;
    }
    LIBCOMM_ELOG(LOG, "SSL_new in comm_ssl_open_server success");
    
    int ret_bind = comm_ssl_set_fd(port, fd);
    if (1 != ret_bind) {
        LIBCOMM_ELOG(WARNING, "SSL_set_fd in comm_ssl_open_server failed");
        return -2;
    } else {
        LIBCOMM_ELOG(LOG, "SSL_set_fd in comm_ssl_open_server success");
    }
    SSL_set_accept_state(port->ssl);
aloop:
    ERR_clear_error();
    r = SSL_accept(port->ssl);
    if (r != 1) {
        err = SSL_get_error(port->ssl, r);
        switch (err) {
            case SSL_ERROR_WANT_READ: // 2
            case SSL_ERROR_WANT_WRITE: // 3
#ifdef WIN32
                pgwin32_waitforsinglesocket(SSL_get_fd(port->ssl),
                    (err == SSL_ERROR_WANT_READ) ? (FD_READ | FD_CLOSE | FD_ACCEPT) : (FD_WRITE | FD_CLOSE),
                    INFINITE);
#endif
                goto aloop;
            case SSL_ERROR_SYSCALL: {  // 5
                LIBCOMM_ELOG(LOG, "SSL_get_error in comm_ssl_open_server return SSL_ERROR_SYSCALL, %s %d",
                             comm_ssl_get_errmsg(), errno);
                if (r < 0) {
                    LIBCOMM_ELOG(LOG, "could not accept SSL connection");
                } else {
                    LIBCOMM_ELOG(LOG, "could not accept SSL connection: EOF detected");
                }
                break;
            }
            case SSL_ERROR_SSL: {  // 1
                LIBCOMM_ELOG(LOG, "SSL_get_error in comm_ssl_open_server return SSL_ERROR_SSL, %s",
                             comm_ssl_get_errmsg());
                break;
            }
            case SSL_ERROR_ZERO_RETURN: {  // 6
                LIBCOMM_ELOG(LOG, "SSL_get_error in comm_ssl_open_server return SSL_ERROR_ZERO_RETURN, %s",
                             comm_ssl_get_errmsg());
                break;
            }
            default: {
                LIBCOMM_ELOG(LOG, "SSL_get_error in comm_ssl_open_server return default, %s", comm_ssl_get_errmsg());
                break;
            }
        }
        return -2;
    }
    LIBCOMM_ELOG(LOG, "after SSL_accept with encryption");
    port->count = 0;

    /* Get client certificate, if available. */
    port->peer = SSL_get_peer_certificate(port->ssl);

    /* and extract the Common Name from it. */
    port->peer_cn = NULL;
    if (port->peer != NULL) {
        int rt;
        int len;
        char* peer_cn = NULL;
        LIBCOMM_ELOG(LOG, "SSL_get_peer_certificate in comm_ssl_open_server return peer not null");

        /* First find out the name's length and allocate a buffer for it. */
        len = X509_NAME_get_text_by_NID(X509_get_subject_name(port->peer), NID_commonName, NULL, 0);
        LIBCOMM_ELOG(LOG, "X509_NAME_get_text_by_NID in comm_ssl_open_server return %d", len);
        if (len != -1) {
            peer_cn = (char*)palloc(len + 1);

            rt = X509_NAME_get_text_by_NID(X509_get_subject_name(port->peer), NID_commonName, peer_cn, len + 1);
            if (rt != len) {
                /* shouldn't happen */
                pfree(peer_cn);
                return -2;
            }
            /*
             * Reject embedded NULLs in certificate common name to prevent
             * attacks like CVE-2009-4034.
             */
            if ((size_t)(unsigned)len != strlen(peer_cn)) {
                LIBCOMM_ELOG(WARNING, "SSL certificate's common name contains embedded null");
                pfree(peer_cn);
                return -2;
            }
            port->peer_cn = peer_cn;
        }
    }
    LIBCOMM_ELOG(DEBUG2, "SSL connection from \"%s\"", port->peer_cn ? port->peer_cn : "(anonymous)");
    LIBCOMM_ELOG(LOG, "comm_ssl_open_server SSL connection from \"%s\"",
                 (port->peer_cn ? port->peer_cn : "(anonymous)"));

    /* set up debugging/info callback */
    if (port->peer != NULL) {
        SSL_set_info_callback(port->ssl, comm_ssl_info_cb);
        LIBCOMM_ELOG(LOG, "SSL_set_info_callback in comm_ssl_open_server");
    }

    return 0;
}

/*
 * Brief            : DH* comm_ssl_genDHKeyPair(COMM_SSL_DHKeyLength dhType)
 * Notes            : function to generate DH key pair
 */
static DH* comm_ssl_genDHKeyPair(COMM_SSL_DHKeyLength dhType)
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

/*
 * Brief        : comm_ssl_set_default_ssl_ciphers,set default ssl ciphers
 * Description  : SEC.CNF.004
 */
static void comm_ssl_set_default_ssl_ciphers()
{
    int default_ciphers_count = 0;

    for (int i = 0; comm_ssl_ciphers_map[i] != NULL; i++) {
        default_ciphers_count++;
    }

    if (comm_ssl_set_cipher_list(g_instance.attr.attr_network.SSL_server_context,
                                   comm_ssl_ciphers_map, default_ciphers_count) != 1) {
        LIBCOMM_ELOG(WARNING, "could not set the cipher list (no valid ciphers available)");
        Assert(0);
        return;
    }
}

/*
 * Brief        : comm_ssl_set_user_config_ssl_ciphers,set the specified ssl ciphers by user
 * Description  : SEC.CNF.004
 */
static void comm_ssl_set_user_config_ssl_ciphers(const char* sslciphers)
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
        LIBCOMM_ELOG(WARNING, "ssl ciphers can not be null");
        Assert(0);
        return;
    } else {
        cipherStr = (char*)strchr(sslciphers, ';'); // if the sslciphers does not contain character ';', the count is 1
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
            LIBCOMM_ELOG(WARNING, "comm_ssl_set_user_config_ssl_ciphers malloc ciphers_list failed");
            Assert(0);
            return;
        }

        cipherStr_tmp = pstrdup(sslciphers);
        if (cipherStr_tmp == NULL) {
            if (ciphers_list != NULL)
                pfree(ciphers_list);
            ciphers_list = NULL;
            LIBCOMM_ELOG(WARNING, "comm_ssl_set_user_config_ssl_ciphers malloc cipherStr_tmp failed");
            Assert(0);
            return;
        }
        token = strtok_r(cipherStr_tmp, ";", &ptok);
        while (token != NULL) {
            for (int j = 0; comm_ssl_ciphers_map[j] != NULL; j++) {
                if (strlen(comm_ssl_ciphers_map[j]) == strlen(token) &&
                    strncmp(comm_ssl_ciphers_map[j], token, strlen(token)) == 0) {
                    ciphers_list[i] = (char*)comm_ssl_ciphers_map[j];
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
                LIBCOMM_ELOG(WARNING, "unrecognized ssl ciphers name: \"%s\"", errormessage);
                Assert(0);
                return;
            }
            token = strtok_r(NULL, ";", &ptok);
            i++;
            find_ciphers_in_list = false;
        }
    }
    if (comm_ssl_set_cipher_list(
        g_instance.attr.attr_network.SSL_server_context, (const char**)ciphers_list, counter) != 1) {
        if (cipherStr_tmp != NULL) {
            pfree(cipherStr_tmp);
            cipherStr_tmp = NULL;
        }
        if (ciphers_list != NULL) {
            pfree(ciphers_list);
            ciphers_list = NULL;
        }
        LIBCOMM_ELOG(WARNING, "could not set the cipher list (no valid ciphers available)");
        Assert(0);
        return;
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

/* Check permissions of cipher file and rand file in server */
static void comm_ssl_comm_check_cipher_file(const char* parent_dir)
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
    if (!S_ISREG(cipherbuf.st_mode) || 
        (cipherbuf.st_mode & (S_IRWXG | S_IRWXO)) || ((cipherbuf.st_mode & S_IRWXU) == S_IRWXU)) {
        LIBCOMM_ELOG(WARNING, "cipher file \"%s\" has group or world access. "
                            "Permissions should be u=rw (0600) or less.", cipher_file);
        Assert(0);
        return;
    }
    if (!S_ISREG(randbuf.st_mode) || 
        (randbuf.st_mode & (S_IRWXG | S_IRWXO)) || ((randbuf.st_mode & S_IRWXU) == S_IRWXU)) {
        LIBCOMM_ELOG(WARNING, "rand file \"%s\" has group or world access"
                            "Permissions should be u=rw (0600) or less.", rand_file);
        Assert(0);
        return;
    }

#endif
}

/* set the default password for certificate/private key loading */
static void comm_ssl_comm_init_server_ssl_pwd(SSL_CTX* sslContext)
{
    char* parentdir = NULL;
    KeyMode keymode = SERVER_MODE;
    if (g_instance.attr.attr_security.ssl_key_file == NULL) {
        LIBCOMM_ELOG(LOG, "In comm_ssl_comm_init_server_ssl_pwd, ssl_key_file is NULL");
        return;
    }
    if (is_absolute_path(g_instance.attr.attr_security.ssl_key_file)) {
        parentdir = pstrdup(g_instance.attr.attr_security.ssl_key_file);
        get_parent_directory(parentdir);
        decode_cipher_files(keymode, NULL, parentdir, g_instance.attr.attr_network.server_key);
    } else {
        decode_cipher_files(keymode, NULL, t_thrd.proc_cxt.DataDir, g_instance.attr.attr_network.server_key);
        parentdir = pstrdup(t_thrd.proc_cxt.DataDir);
    }

    comm_ssl_comm_check_cipher_file(parentdir);
    pfree_ext(parentdir);

    SSL_CTX_set_default_passwd_cb_userdata(sslContext, (char*)g_instance.attr.attr_network.server_key);
}

void comm_initialize_SSL()
{
    struct stat buf;
    STACK_OF(X509_NAME)* root_cert_list = NULL;
    errno_t errorno = EOK;

    /* Already initialized SSL, return here */
    if (g_instance.attr.attr_network.ssl_initialized) {
        LIBCOMM_ELOG(LOG, "In comm_initialize_SSL, ssl_initialized is true, return");
        return;
    } else {
        LIBCOMM_ELOG(LOG, "In comm_initialize_SSL, ssl_initialized is false");
    }

    if (!g_instance.attr.attr_network.SSL_server_context) {
        LIBCOMM_ELOG(LOG, "In comm_initialize_SSL, SSL_server_context is null");
        if (OPENSSL_init_ssl(0, NULL) != 1) {
            LIBCOMM_ELOG(LOG, "In comm_initialize_SSL, OPENSSL_init_ssl is failed");
            return;
        } else {
            LIBCOMM_ELOG(LOG, "In comm_initialize_SSL, OPENSSL_init_ssl success");
        }
        SSL_load_error_strings();

        g_instance.attr.attr_network.SSL_server_context = SSL_CTX_new(TLS_method());
        if (!g_instance.attr.attr_network.SSL_server_context) {
            LIBCOMM_ELOG(WARNING, "In comm_initialize_SSL, could not create SSL context");
            Assert(0);
            return;
        } else {
            LIBCOMM_ELOG(LOG, "In comm_initialize_SSL, create SSL context success");
        }
        if (g_instance.attr.attr_network.server_key == NULL) {
            g_instance.attr.attr_network.server_key = (GS_UCHAR*)palloc0((CIPHER_LEN + 1) * sizeof(GS_UCHAR));
        }
        if (g_instance.attr.attr_network.server_key == NULL) {
            LIBCOMM_ELOG(LOG, "In comm_initialize_SSL, palloc server_key failed");
        } else {
            LIBCOMM_ELOG(LOG, "In comm_initialize_SSL, palloc server_key success");
        }
        /*
         * Disable moving-write-buffer sanity check, because it
         * causes unnecessary failures in nonblocking send cases.
         */
        SSL_CTX_set_mode(g_instance.attr.attr_network.SSL_server_context, \
                         SSL_MODE_ACCEPT_MOVING_WRITE_BUFFER);
        LIBCOMM_ELOG(LOG, "In comm_initialize_SSL, SSL_CTX_set_mode");

        /* set the default password for certificate/private key loading */
        comm_ssl_comm_init_server_ssl_pwd(g_instance.attr.attr_network.SSL_server_context);
        LIBCOMM_ELOG(LOG, "In comm_initialize_SSL, comm_ssl_comm_init_server_ssl_pwd");
        char *buffer;
        if((buffer = getcwd(NULL,0)) == NULL){
            LIBCOMM_ELOG(WARNING, "In comm_initialize_SSL, getcwd error");
        } else{
            free(buffer);
        }

        /* Load and verify server's certificate and private key */
        if (SSL_CTX_use_certificate_chain_file(g_instance.attr.attr_network.SSL_server_context, \
                                               g_instance.attr.attr_security.ssl_cert_file) != 1) {
            LIBCOMM_ELOG(WARNING, "In comm_initialize_SSL, could not load server certificate file %s",
                g_instance.attr.attr_security.ssl_cert_file);
            Assert(0);
            return;
        }

        /* check certificate file permission */
#if !defined(WIN32) && !defined(__CYGWIN__)
        if (stat(g_instance.attr.attr_security.ssl_cert_file, &buf) == 0){
            if (!S_ISREG(buf.st_mode) || 
                (buf.st_mode & (S_IRWXG | S_IRWXO)) || ((buf.st_mode & S_IRWXU) == S_IRWXU)) {
                LIBCOMM_ELOG(WARNING, "certificate file \"%s\" has group or world access"
                            "Permissions should be u=rw (0600) or less.", g_instance.attr.attr_security.ssl_cert_file);
                Assert(0);
                return;
            }
        }
#endif

        if (stat(g_instance.attr.attr_security.ssl_key_file, &buf) != 0) {
            LIBCOMM_ELOG(WARNING, "In comm_initialize_SSL, could not access private key file %s",
                g_instance.attr.attr_security.ssl_key_file);
            Assert(0);
            return;
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
        if (!S_ISREG(buf.st_mode) || 
            (buf.st_mode & (S_IRWXG | S_IRWXO)) || ((buf.st_mode & S_IRWXU) == S_IRWXU)) {
            LIBCOMM_ELOG(WARNING,
                "private key file \"%s\" has group or world access Permissions should be u=rw (0600) or less.",
                g_instance.attr.attr_security.ssl_key_file);
        }
#endif

        if (SSL_CTX_use_PrivateKey_file(g_instance.attr.attr_network.SSL_server_context, \
                                        g_instance.attr.attr_security.ssl_key_file, \
                                        SSL_FILETYPE_PEM) != 1) {
            LIBCOMM_ELOG(WARNING, "In comm_initialize_SSL, could not load private key file \"%s\"",
                g_instance.attr.attr_security.ssl_key_file);
            Assert(0);
            return;
        }

        if (SSL_CTX_check_private_key(g_instance.attr.attr_network.SSL_server_context) != 1) {
            LIBCOMM_ELOG(LOG, "In comm_initialize_SSL, check of private key(%s) failed",
                g_instance.attr.attr_security.ssl_key_file);
        }
    }

    /* check ca certificate file permission */
#if !defined(WIN32) && !defined(__CYGWIN__)
    if (stat(g_instance.attr.attr_security.ssl_ca_file, &buf) == 0){
        if (!S_ISREG(buf.st_mode) || (buf.st_mode & (S_IRWXG | S_IRWXO)) || ((buf.st_mode & S_IRWXU) == S_IRWXU)) {
            LIBCOMM_ELOG(WARNING,
                "ca certificate file \"%s\" has group or world access Permissions should be u=rw (0600) or less.",
                g_instance.attr.attr_security.ssl_ca_file);
            Assert(0);
            return;
        }
    }
#endif

    /* Check the signature algorithm.*/
    if (check_certificate_signature_algrithm(g_instance.attr.attr_network.SSL_server_context)) {
        LIBCOMM_ELOG(LOG, "In initialize_SSL, The server certificate contain a Low-intensity signature algorithm");
    }

    /* Check the certificate expires time. */
    long leftspan = check_certificate_time(g_instance.attr.attr_network.SSL_server_context, 
        u_sess->attr.attr_security.ssl_cert_notify_time);
    if (leftspan > 0) {
        int leftdays = (leftspan / 86400 > 0) ? (leftspan / 86400) : 1;
        if (leftdays > 1) {
            LIBCOMM_ELOG(WARNING, "The server certificate will expire in %d days", leftdays);
        } else {
            LIBCOMM_ELOG(WARNING, "The server certificate will expire in %d day", leftdays);
        }
    }

    /* set up ephemeral DH keys, and disallow SSL v2 while at it
     * free the dh directly safe as there is reference counts in DH
     */
    DH* dhkey = comm_ssl_genDHKeyPair(DHKey3072);
    if (dhkey == NULL) {
        LIBCOMM_ELOG(WARNING, "DH: generating parameters (3072 bits) failed");
        Assert(0);
        return;
    }
    SSL_CTX_set_tmp_dh(g_instance.attr.attr_network.SSL_server_context, dhkey);
    DH_free(dhkey);

    /* SSL2.0/SSL3.0/TLS1.0/TLS1.1 is forbidden here. */
    SSL_CTX_set_options(g_instance.attr.attr_network.SSL_server_context,
        SSL_OP_SINGLE_DH_USE | SSL_OP_NO_SSLv2 | SSL_OP_NO_SSLv3 | SSL_OP_NO_TLSv1 | SSL_OP_NO_TLSv1_1);

    /* set up the allowed cipher list */
    if (strcasecmp(g_instance.attr.attr_security.SSLCipherSuites, "ALL") == 0) {
        comm_ssl_set_default_ssl_ciphers();
    } else {
        comm_ssl_set_user_config_ssl_ciphers(g_instance.attr.attr_security.SSLCipherSuites);
    }

    /* Load CA store, so we can verify client certificates if needed. */
    if (g_instance.attr.attr_security.ssl_ca_file[0]) {
        if (SSL_CTX_load_verify_locations(g_instance.attr.attr_network.SSL_server_context,
                                          g_instance.attr.attr_security.ssl_ca_file, NULL) != 1) {
            LIBCOMM_ELOG(WARNING, "could not load the ca certificate file");
        }

        root_cert_list = SSL_load_client_CA_file(g_instance.attr.attr_security.ssl_ca_file);
        if (root_cert_list == NULL) {
            LIBCOMM_ELOG(LOG, "In comm_initialize_SSL, could not load root certificate file %s",
                g_instance.attr.attr_security.ssl_ca_file);
        }
    }

    /* Load the Certificate Revocation List (CRL). */
    if (g_instance.attr.attr_security.ssl_crl_file[0]) {
        X509_STORE* cvstore = SSL_CTX_get_cert_store(g_instance.attr.attr_network.SSL_server_context);
        if (cvstore != NULL) {
            /* Set the flags to check against the complete CRL chain */
            if (1 == X509_STORE_load_locations(cvstore, g_instance.attr.attr_security.ssl_crl_file, NULL)) {
                (void)X509_STORE_set_flags(cvstore, X509_V_FLAG_CRL_CHECK);
            } else {
                LIBCOMM_ELOG(WARNING, "In comm_initialize_SSL, could not load SSL certificate revocation list file %s",
                    g_instance.attr.attr_security.ssl_crl_file);
            }
        }
    }

    if (g_instance.attr.attr_security.ssl_ca_file[0]) {
        /*
         * Always ask for SSL client cert, but don't fail if it's not
         * presented.  We might fail such connections later, depending on
         * what we find in pg_hba.conf.
         */
        SSL_CTX_set_verify(g_instance.attr.attr_network.SSL_server_context,
                           (SSL_VERIFY_PEER | SSL_VERIFY_CLIENT_ONCE),
                           comm_ssl_verify_cb);

        /* Increase the depth to support multi-level certificate. */
        SSL_CTX_set_verify_depth(g_instance.attr.attr_network.SSL_server_context,
                                 (MAX_CERTIFICATE_DEPTH_SUPPORTED - 2));

        /*
         * send the list of root certs we trust to clients in
         * CertificateRequests.  This lets a client with a keystore select the
         * appropriate client certificate to send to us.
         */
        SSL_CTX_set_client_CA_list(g_instance.attr.attr_network.SSL_server_context, root_cert_list);
    }

    /* clear the sensitive info in server_key */
    errorno = memset_s(g_instance.attr.attr_network.server_key, CIPHER_LEN + 1, 0, CIPHER_LEN + 1);
    securec_check(errorno, "\0", "\0");

    g_instance.attr.attr_network.ssl_initialized = true;
    LIBCOMM_ELOG(LOG, "In comm_initialize_SSL, set ssl_initialized true");
}

libcomm_sslinfo** comm_ssl_find_port(libcomm_sslinfo** head, int sock)
{
    Assert(head != NULL);

    libcomm_sslinfo** p;
    for (p = head; *p != NULL && (*p)->node.sock != sock; p = &(*p)->next) {
        // find node until sock is same
    }
    if (*p == NULL) {
        return p;
    }
    if ((*p)->node.sock == sock) {
        return p;
    }
    // should never go here
    LIBCOMM_ELOG(LOG, "In comm_ssl_find_port, find info of sock[%d] failed", sock);
    return head;
}

SSL* comm_ssl_find_ssl_by_fd(int sock)
{
    libcomm_sslinfo* port = g_instance.comm_cxt.libcomm_ctrl_port_list;
    while (port != NULL) {
        if (port->node.sock == sock) {
            return port->node.ssl;
        } else {
            port = port->next;
        }
    }
    port = g_instance.comm_cxt.libcomm_data_port_list;
    while (port != NULL) {
        if (port->node.sock == sock) {
            return port->node.ssl;
        } else {
            port = port->next;
        }
    }
    LIBCOMM_ELOG(LOG, "In comm_ssl_find_ssl_by_fd, find ssl of sock[%d] failed", sock);
    return NULL;
}

#endif