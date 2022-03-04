/*
 * Copyright (c) 2020 Huawei Technologies Co.,Ltd.
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
 * File Name	: gs_openssl_client.cpp
 * Brief		:
 * Description	: initialize the ssl system of client which is based on the OpenSSL library
 *
 * History	: 2017-6
 *
 * IDENTIFICATION
 *	  src/gausskernel/cbb/utils/ssl/gs_openssl_client.cpp
 * 
 * -------------------------------------------------------------------------
 */
#include "securec_check.h"
#include "gs_threadlocal.h"
#include "postgres.h"
#include "knl/knl_variable.h"
#include "ssl/gs_openssl_client.h"
#include "utils/be_module.h"
#include "utils/elog.h"
#include "utils/palloc.h"

#include "openssl/ssl.h"
#include "openssl/err.h"
#include "openssl/ossl_typ.h"
#include "openssl/x509.h"
#include "openssl/asn1.h"

#ifdef ENABLE_UT
#define static
#endif /* ENABLE_UT */

/* mutex for configuring OpenSSL evn */
#ifndef WIN32
static pthread_mutex_t openssl_conf_mtx = PTHREAD_MUTEX_INITIALIZER;
#else
static pthread_mutex_t openssl_conf_mtx = NULL;
#endif

#define OSSL_RANDOM_LEN 16
#define MIN_DHLENTH 112
#define MAX_DHLENTH 192

/* indicate whether VPP SSL evn is ready or not */
static bool openssl_system_inited = false;

static inline void ossl_clear_passwd_mem(gs_openssl_client* cli);

static inline void ossl_clear_passwd_mem(gs_openssl_client* cli);
static int opencli_dh_verify_cb(const SSL* s, const SSL_CTX* ctx, int op, int bits, int nid, void* other, void* ex);
static void ossl_init_ssl_context(gs_openssl_client* cli);
static int ossl_init_client_ssl_passwd(SSL_CTX* pstContext, const char* cert_file_path, GS_UCHAR* cipher_passwd);
static int ossl_load_ssl_files(gs_openssl_client* cli);
static int ossl_check_permission_of_cipher_file(const char* parent_dir);
static int ossl_new_ssl_and_bind_socket(gs_openssl_client* cli, int sock_id);
static int ossl_verify_method_name_matches_certificate(X509* peer);
static int ossl_open_connection(gs_openssl_client* cli);
static const char* ossl_error_message(void);
static char* ssl_cipher_list2string(const char* ciphers[], const int num);
static int SSL_CTX_set_cipher_list_ex(SSL_CTX* ctx, const char* ciphers[], const int num);

/* security ciphers suites in SSL connection */
static const char* ossl_cipher_list[] = {TLS1_TXT_ECDHE_RSA_WITH_AES_128_GCM_SHA256, NULL};

/* VPP SSL client configuration information */
struct gs_openssl_client {
    /* SSL context */
    SSL_CTX* m_ssl_context;
    /* SSL object */
    SSL* ssl;
    /* X509 cert */
    X509* peer;

    char* ssl_dir;       /* direcotry for holding the following files */
    char* sslclientkey;  /* client key filename */
    char* sslclientcert; /* client certificate filename */
    char* sslrootcert;   /* root certificate filename */

    /* plain password memory */
    GS_UCHAR plain_passwd[CIPHER_LEN + 1];
};

/*
 * @Description: set user defined files to gs_openssl_client
 * @Input: user defined value, ssl_dir, rootcert, client_key, clientcert
 * @Output: gs_openssl_client* cli, the client SSL information struct
 * @Return: NULL
 * @See also:
 */
void gs_openssl_cli_setfiles(
    gs_openssl_client* cli, const char* ssl_dir, const char* rootcert, const char* client_key, const char* clientcert)
{
    cli->ssl_dir = pstrdup(ssl_dir);
    cli->sslclientcert = pstrdup(clientcert);
    cli->sslclientkey = pstrdup(client_key);
    cli->sslrootcert = pstrdup(rootcert);
}

/*
 * @Description: make the member of gs_openssl_client be NULL
 * @Input: gs_openssl_client* cli, the client SSL information struct
 * @Return: NULL
 * @See also:
 */
gs_openssl_cli gs_openssl_cli_create(void)
{
    return (gs_openssl_cli)palloc0(sizeof(gs_openssl_client));
}

/*
 * @Description: clean the memory of gs_openssl_client
 * @Input: gs_openssl_client* cli, the client SSL information struct
 * @Return: NULL
 * @See also:
 */
void gs_openssl_cli_destroy(gs_openssl_client* cli)
{
    if (cli != NULL) {
        if (cli->ssl != NULL) {
            (void)SSL_free(cli->ssl);
            cli->ssl = NULL;
        }
        if (cli->m_ssl_context != NULL) {
            (void)SSL_CTX_free(cli->m_ssl_context);
            cli->m_ssl_context = NULL;
        }
        if (cli->ssl_dir != NULL) {
            pfree_ext(cli->ssl_dir);
        }
        if (cli->sslclientcert != NULL) {
            pfree_ext(cli->sslclientcert);
        }
        if (cli->sslclientkey != NULL) {
            pfree_ext(cli->sslclientkey);
        }
        if (cli->sslrootcert != NULL) {
            pfree_ext(cli->sslrootcert);
        }
        cli->peer = NULL;
    }
}

/*
 * @Description:  the main function of initialize the ssl system of client using OPENSSL.
 *		step1:prepare the ssl context.
 *      step2: load the certificate file and key file.
 *		step3: set the cipher list and verify mode.
 *      step4: bind SSL with socket
 *		step5: connect with server using SSL
 * @Input: gs_openssl_client* cli, the information struct of OPENSSL
 * @Input: int sock_id, the Socket ID number attached to SSL
 * @Return: success 0, failed  -1
 * @See also:
 */
int gs_openssl_cli_initialize_SSL(gs_openssl_client* cli, int sock_id)
{
    int ciphers_count = 0;

    ossl_init_ssl_context(cli);

    /* Important: make password buffer ending with '\0' */
    ossl_clear_passwd_mem(cli);

    /* set the default password for certificate/private key loading */
    if (ossl_init_client_ssl_passwd(cli->m_ssl_context, cli->ssl_dir, cli->plain_passwd) != 0) {
        SSL_CTX_free(cli->m_ssl_context);
        ereport(ERROR,
            (errmodule(MOD_SSL),
                errcode(ERRCODE_OPERATE_RESULT_NOT_EXPECTED),
                errmsg("Failed to set the default password for private key File: \"%s\"", cli->sslclientkey)));
        return -1;
    }

    (void)ossl_load_ssl_files(cli);

    /* Calculate the number of security ciphers suites */
    for (int i = 0; ossl_cipher_list[i] != NULL; i++) {
        ciphers_count++;
    }

    /* Set security ciphers suites in SSL connection for the client */
    if (SSL_CTX_set_cipher_list_ex(cli->m_ssl_context, ossl_cipher_list, ciphers_count) != 1) {
        SSL_CTX_free(cli->m_ssl_context);
        ereport(ERROR,
            (errmodule(MOD_SSL), errcode(ERRCODE_OPERATE_RESULT_NOT_EXPECTED), errmsg("%s", ossl_error_message())));
        return -1;
    }

    /* set the verificatrion mode and the callback function to
     * verify the  servers certificate for the ssl context
     */
    SSL_CTX_set_verify(cli->m_ssl_context, SSL_VERIFY_PEER, NULL);

    (void)ossl_new_ssl_and_bind_socket(cli, sock_id);

    (void)ossl_open_connection(cli);

    /* clear the sensitive info in password */
    ossl_clear_passwd_mem(cli);

    return 0;
}

/*
 * @Description: Report the error message of SSL
 * @Return: const char*, error message
 * @See also:
 */
static const char* ossl_error_message(void)
{
    GS_UINT32 errcode = ERR_get_error();
    if (errcode == 0) {
        return _("no SSL error reported");
    }

    const char* errreason = ERR_reason_error_string(errcode);
    if (errreason != NULL) {
        return errreason;
    }

/* error message length is 15, and errcode's type is int32,
 * so the total length is less than 32 bytes. it's safe here.
 */
#define ERR_BUF_MAXLEN 32
    static THR_LOCAL char errbuf[ERR_BUF_MAXLEN];
    int nRet;
    nRet = snprintf_s(errbuf, sizeof(errbuf), sizeof(errbuf) - 1, ("SSL error code %ul"), errcode);
    securec_check_ss_c(nRet, "\0", "\0");

    return errbuf;
}

/*
 * @Description: write at most len bytes to SSL connection from buf.
 *     caller must check the returned bytes be written. if returned
 *     bytes is less than len, please call this function again.
 * @IN buf: input buffer to write
 * @IN cli: gs_openssl_client object
 * @IN len: bytes number to write
 * @Return: written bytes number
 * @See also:
 */
int gs_openssl_cli_write(gs_openssl_client* cli, const char* buf, int len)
{
    int nwrite = 0;
    int sslerr = 0;
    int trytimes = 0;
    const int maxtimes = 10;

sslretry:

    errno = 0;
    ERR_clear_error();
    nwrite = SSL_write(cli->ssl, buf, len);

    sslerr = SSL_get_error(cli->ssl, nwrite);
    switch (sslerr) {
        case SSL_ERROR_NONE: {
            if (nwrite < 0) {
                ereport(WARNING,
                    (errmodule(MOD_SSL),
                        errmsg("write failed but not provide error info: write %d, errno %d", nwrite, errno)));
            }
            break;
        }
        case SSL_ERROR_WANT_READ:
        case SSL_ERROR_WANT_WRITE:
        case SSL_ERROR_ZERO_RETURN: {
            if (++trytimes > maxtimes) {
                ereport(ERROR,
                    (errmodule(MOD_SSL),
                        errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("too many writing times(>%d):%d, write %d, errno %d", maxtimes, sslerr, nwrite, errno)));
            } else {
                ereport(WARNING,
                    (errmodule(MOD_SSL), errmsg("try to write again:%d, write %d, errno %d", sslerr, nwrite, errno)));
            }
            nwrite = -1;
            goto sslretry;
        }
        case SSL_ERROR_SYSCALL:
        case SSL_ERROR_SSL: {
            ereport(ERROR,
                (errmodule(MOD_SSL),
                    errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                    errmsg("%d: write %d, errno %d, detail:%s", sslerr, nwrite, errno, ossl_error_message())));
            nwrite = -1;
            break;
        }
        default: {
            ereport(ERROR,
                (errmodule(MOD_SSL),
                    errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                    errmsg("unrecognized error:%d, write %d, errno %d", sslerr, nwrite, errno)));
            nwrite = -1;
            break;
        }
    }
    return nwrite;
}

/*
 * @Description: read at most maxlen bytes from SSL connection,
 *     and put data into obuf. caller must repeated call this
 *     function until there is no data in OPENSSL buffer and
 *     0 returned
 * @IN cli: gs_openssl_client object
 * @IN maxlen: max length of output buffer
 * @IN obuf: output buffer
 * @Return:
 * @See also:
 */
int gs_openssl_cli_read(gs_openssl_client* cli, char* obuf, int maxlen)
{
    int sslerr = 0;
    int nread = 0;

    Assert(cli && cli->ssl);
    int sockid = SSL_get_fd(cli->ssl);
    if (0 > sockid) {
        ereport(LOG, (errmodule(MOD_SSL), errmsg("bad socket when reading")));
        return OPENSSL_CLI_BAD_SOCKET;
    }

sslretry:

    errno = 0;
    ERR_clear_error();
    nread = SSL_read(cli->ssl, obuf, maxlen);

    sslerr = SSL_get_error(cli->ssl, nread);
    if (nread < 0) {
        if (errno == EINTR) {
            goto sslretry;
        }

        /* Some systems return EAGAIN/EWOULDBLOCK for no data */
#ifdef EAGAIN
        if (errno == EAGAIN) {
            return OPENSSL_CLI_EAGAIN;
        }
#endif

#if defined(EWOULDBLOCK) && (!defined(EAGAIN) || (EWOULDBLOCK != EAGAIN))
        if (errno == EWOULDBLOCK) {
            return OPENSSL_CLI_EAGAIN;
        }
#endif
        ereport(LOG, (errmodule(MOD_SSL), errmsg("exception status during read: sslerr %d, errno %d", sslerr, errno)));

        return OPENSSL_CLI_EXCEPTTION;
    }

    return nread;
}

/*
 * @Description: initialize OPENSSL system evn, including
 *    1. X509 checking
 *    2. CA key checking;
 *    3. load SSL library;
 *    4. load error string table;
 * @See also:
 */
void gs_openssl_cli_init_system(void)
{
    if (openssl_system_inited) {
        return;
    }

    (void)pthread_mutex_lock(&openssl_conf_mtx);
    if (!openssl_system_inited) {
        /* registers the available ciphers and digests */
        if (1 != OPENSSL_init_ssl(0, NULL)) {
            (void)pthread_mutex_unlock(&openssl_conf_mtx);

            ereport(ERROR,
                (errmodule(MOD_SSL),
                    errcode(ERRCODE_OPERATE_RESULT_NOT_EXPECTED),
                    errmsg("failed to initialize SSL library, detail:%s", ossl_error_message())));
        }

        /* registers the error strings required by SSL library */
        SSL_load_error_strings();

        /* openssl system has been inited ok */
        openssl_system_inited = true;
    }
    (void)pthread_mutex_unlock(&openssl_conf_mtx);
}

/*
 * @Description: prepare the ssl library in the beginning of initialization
 * @Input: gs_openssl_client* cli, the information struct of OPENSSL
 * @Return:
 * @See also:
 */
static void ossl_init_ssl_context(gs_openssl_client* cli)
{
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
    /* create SSL context using TLS v1.2 client method */
    cli->m_ssl_context = SSL_CTX_new(TLSv1_2_client_method());
#pragma GCC diagnostic pop

    if (cli->m_ssl_context == NULL) {
        ereport(ERROR,
            (errmodule(MOD_SSL),
                errcode(ERRCODE_OPERATE_RESULT_NOT_EXPECTED),
                errmsg("failed to create the SSL context, detail:%s", ossl_error_message())));
    }

    /*
     * Disable OPENSSL's moving-write-buffer sanity check, because it
     * causes unnecessary failures in nonblocking send cases.
     */
    (void)SSL_CTX_set_mode(cli->m_ssl_context, SSL_MODE_ACCEPT_MOVING_WRITE_BUFFER);
}

/*
 * @Description: set the default password for certificate/private key loading
 * @Input: SSL_CTX *pstContext, SSL context to set the password
 * @Input: char* cert_file_dir, the file path that contains certificate files
 * @Input: GS_UCHAR *cipher_passwd, decrypt password
 * @Return: success 0, failed -1
 * @See also:
 */
static int ossl_init_client_ssl_passwd(SSL_CTX* pstContext, const char* cert_file_dir, GS_UCHAR* cipher_passwd)
{
    int retval = 0;

    if (NULL == cert_file_dir || '\0' == cert_file_dir[0]) {
        SSL_CTX_free(pstContext);
        ereport(ERROR,
            (errmodule(MOD_SSL), errcode(ERRCODE_OPERATE_RESULT_NOT_EXPECTED), errmsg("invlid cert file directory")));
        return -1;
    }

    /*
     * Decrypt the default cipher and rand files begins with client
     */
    retval = ossl_check_permission_of_cipher_file(cert_file_dir);
    if (retval != 0) {
        return retval;
    }

    decode_cipher_files(GDS_MODE, NULL, cert_file_dir, cipher_passwd);
    SSL_CTX_set_default_passwd_cb_userdata(pstContext, (void*)cipher_passwd);

    return 0;
}

/*
 * @Description: load certificate files for SSL context of the client
 * @Input: gs_openssl_client* cli, the information struct of OPENSSL
 * @Return:success 0, failed -1
 * @See also:
 */
static int ossl_load_ssl_files(gs_openssl_client* cli)
{
    char fnbuf[MAXPGPATH] = {0};

    /* Load the ROOT CA files */
    int nRet = snprintf_s(fnbuf, MAXPGPATH, sizeof(fnbuf) - 1, "%s/%s", cli->ssl_dir, cli->sslrootcert);
    securec_check_ss_c(nRet, "\0", "\0");
    if (SSL_CTX_load_verify_locations(cli->m_ssl_context, fnbuf, NULL) != 1) {
        SSL_CTX_free(cli->m_ssl_context);
        ereport(ERROR,
            (errmodule(MOD_SSL),
                errcode(ERRCODE_OPERATE_RESULT_NOT_EXPECTED),
                errmsg("failed to load the root CA Certificate %s(%s)", cli->sslrootcert, fnbuf),
                errdetail("%s", ossl_error_message())));
        return -1;
    }

    /*
     * Load the client certificated file "client.crt" for the SSL context
     */
    nRet = snprintf_s(fnbuf, MAXPGPATH, sizeof(fnbuf) - 1, "%s/%s", cli->ssl_dir, cli->sslclientcert);
    securec_check_ss_c(nRet, "\0", "\0");
    if (SSL_CTX_use_certificate_chain_file(cli->m_ssl_context, fnbuf) != 1) {
        SSL_CTX_free(cli->m_ssl_context);
        ereport(ERROR,
            (errmodule(MOD_SSL),
                errcode(ERRCODE_OPERATE_RESULT_NOT_EXPECTED),
                errmsg("failed to certificate the %s(%s) file in SSL context", cli->sslclientcert, fnbuf)));
        return -1;
    }
    return 0;
}

/* Check permissions of cipher file and rand file in client */
static int ossl_check_permission_of_cipher_file(const char* parent_dir)
{
    char cipher_file[MAXPGPATH] = {0};
    char rand_file[MAXPGPATH] = {0};
    struct stat cipherbuf;
    struct stat randbuf;
    int nRet = 0;

    /* Here we ignore the specified user name of the client.crt file */
    nRet = snprintf_s(cipher_file, MAXPGPATH, MAXPGPATH - 1, "%s/server%s", parent_dir, CIPHER_KEY_FILE);
    securec_check_ss_c(nRet, "\0", "\0");
    nRet = snprintf_s(rand_file, MAXPGPATH, MAXPGPATH - 1, "%s/server%s", parent_dir, RAN_KEY_FILE);
    securec_check_ss_c(nRet, "\0", "\0");

    if (lstat(cipher_file, &cipherbuf) != 0) {
        ereport(ERROR,
            (errmodule(MOD_SSL),
                errcode_for_file_access(),
                errmsg("stat cipher file \"%s\" failed, detail: %s", cipher_file, gs_strerror(errno))));
        return -1;
    }
    if (lstat(rand_file, &randbuf) != 0) {
        ereport(ERROR,
            (errmodule(MOD_SSL),
                errcode_for_file_access(),
                errmsg("stat rand file \"%s\" failed, detail: %s", rand_file, gs_strerror(errno))));
        return -1;
    }

    /* cipher file and rand file exist,
     * so check whether permissions meets the requirements
     */
    if (!S_ISREG(cipherbuf.st_mode) || (cipherbuf.st_mode & (S_IRWXG | S_IRWXO))) {
        ereport(ERROR,
            (errmodule(MOD_SSL),
                errcode_for_file_access(),
                errmsg("cipher file \"%s\" has group or world access;"
                       "permissions should be u=rw (0600) or less with Error: %s",
                    cipher_file,
                    gs_strerror(errno))));
        return -1;
    }

    if (!S_ISREG(randbuf.st_mode) || (randbuf.st_mode & (S_IRWXG | S_IRWXO))) {
        ereport(ERROR,
            (errmodule(MOD_SSL),
                errcode_for_file_access(),
                errmsg("rand file \"%s\" has group or world access;"
                       "permissions should be u=rw (0600) or less with Error: %s",
                    rand_file,
                    gs_strerror(errno))));
        return -1;
    }

    /* files exist,and permission is ok! */
    return 0;
}

/*
 * @Description: bind the SSL with socket ID,  read the certificate files
 *		for SSL and check the verify method.
 * @Input: gs_openssl_client* cli, the information struct of OPENSSL
 * @Input: int sock_id, socket ID to be binded with.
 * @Return: success 0 failed -1
 * @See also:
 */
static int ossl_new_ssl_and_bind_socket(gs_openssl_client* cli, int sock_id)
{
#define MAX_CERTIFICATE_DEPTH_SUPPORTED 20 /* The max certificate depth supported. */

    /* create the SSL with the generated context */
    cli->ssl = SSL_new(cli->m_ssl_context);
    if (cli->ssl == NULL) {
        ereport(ERROR,
            (errmodule(MOD_SSL),
                errcode(ERRCODE_OPERATE_RESULT_NOT_EXPECTED),
                errmsg("Failed to create the SSL."),
                errdetail("%s", ossl_error_message())));
    }

    /* set the socked which is binded to SSL */
    if (SSL_set_fd(cli->ssl, sock_id) != 1) {
        SSL_free(cli->ssl);
        ereport(ERROR,
            (errmodule(MOD_SSL),
                errcode_for_socket_access(),
                errmsg("Failed to set the socket(%d) for SSL", sock_id),
                errdetail("%s", ossl_error_message())));
        return -1;
    }

    char fnbuf[MAXPGPATH] = {0};

    /* Read the certificate file for SSL */
    int nRet = snprintf_s(fnbuf, MAXPGPATH, sizeof(fnbuf) - 1, "%s/%s", cli->ssl_dir, cli->sslclientcert);
    securec_check_ss_c(nRet, "\0", "\0");
    if (SSL_use_certificate_file(cli->ssl, fnbuf, SSL_FILETYPE_PEM) != 1) {
        SSL_free(cli->ssl);
        ereport(ERROR,
            (errmodule(MOD_SSL),
                errcode_for_socket_access(),
                errmsg("Failed to certificate file \"%s\" in SSL object", fnbuf),
                errdetail("%s", ossl_error_message())));
        return -1;
    }

    /* Read the key files for SSL */
    nRet = snprintf_s(fnbuf, MAXPGPATH, sizeof(fnbuf) - 1, "%s/%s", cli->ssl_dir, cli->sslclientkey);
    securec_check_ss_c(nRet, "\0", "\0");
    if (SSL_use_PrivateKey_file(cli->ssl, fnbuf, SSL_FILETYPE_PEM) != 1) {
        SSL_free(cli->ssl);
        ereport(ERROR,
            (errmodule(MOD_SSL),
                errcode_for_socket_access(),
                errmsg("Failed to certificate file \"%s\" in SSL object", fnbuf),
                errdetail("%s", ossl_error_message())));
        return -1;
    }

    /* verify that the cert and key go together in SSL */
    if (SSL_check_private_key(cli->ssl) != 1) {
        SSL_free(cli->ssl);
        ereport(ERROR,
            (errmodule(MOD_SSL),
                errcode_for_socket_access(),
                errmsg("Failed to check the private key File."),
                errdetail("%s", ossl_error_message())));
        return -1;
    }

    /* set the DH callback function of SSL */
    SSL_set_security_callback(cli->ssl, opencli_dh_verify_cb);

    /* set the SSL verify method */
    SSL_set_verify(cli->ssl, SSL_VERIFY_PEER | SSL_VERIFY_FAIL_IF_NO_PEER_CERT, NULL);

    /* set the SSL verify depth */
    SSL_CTX_set_verify_depth(cli->m_ssl_context, MAX_CERTIFICATE_DEPTH_SUPPORTED - 2);

    return 0;
}

/*
 * @Description: verify the common name of the certificate
 * @Input: X509 *peer, extended certificate structure contains certificate
 * @Return: success 0, failed other value
 * @See also:
 */
static int ossl_verify_method_name_matches_certificate(X509* peer)
{
    int index;
    X509_NAME* pstName = NULL;
    X509_NAME_ENTRY* entry = NULL;
    ASN1_STRING* common_name_asn = NULL;
    char* common_name_str = NULL;

    pstName = X509_get_subject_name(peer);
    index = X509_NAME_get_index_by_NID(pstName, NID_commonName, 0);
    if (index < 0) {
        return -1;
    }

    entry = X509_NAME_get_entry(pstName, index);
    if (entry == NULL) {
        return -1;
    }

    common_name_asn = X509_NAME_ENTRY_get_data(entry);
    if (common_name_asn == NULL) {
        return -1;
    }

    common_name_str = (char*)ASN1_STRING_get0_data(common_name_asn);
    if (common_name_str == NULL) {
        return -1;
    }

    if ((size_t)ASN1_STRING_length(common_name_asn) != strlen(common_name_str)) {
        ereport(LOG, (errmodule(MOD_SSL), errmsg("certificate's common name contains embedded null")));
        return -1;
    }

    return 0;
}

/*
 * @Description: connect with server using SSL
 * @Input: gs_openssl_client* cli, the information struct of OPENSSL
 * @Return: success 0, failed other value
 * @See also:
 */
static int ossl_open_connection(gs_openssl_client* cli)
{
    /* open_client_SSL */
    ERR_clear_error();
    int r = SSL_connect(cli->ssl);
    if (r <= 0) {
        int err = SSL_get_error(cli->ssl, r);
        SSL_free(cli->ssl);
        cli->ssl = NULL;
        /*
         * We added the hint part because we don't have SSL compatibility yet on Windows GDS.
         * When we connect to an non-ssl GDS with gsfss, we fail through here. Therefore this hint
         * is meant to guild the user. Though not all SSL connection failures are result from the usage
         * of Windows GDS.
         * Moreover, the SSL handshake mech we currently have is really bad in many ways. It will need to
         * be further restructured and improved.
         */
        ereport(ERROR, (errmodule(MOD_SSL), errmsg("SSL connect failed, code %d", err)));
        return err;
    }

    ereport(DEBUG2,
        (errmodule(MOD_SSL),
            errcode(ERRCODE_OPERATE_RESULT_NOT_EXPECTED),
            errmsg("connection successful using cipher(%s)", SSL_get_cipher_name(cli->ssl))));

    cli->peer = SSL_get_peer_certificate(cli->ssl);
    if (NULL == cli->peer) {
        SSL_free(cli->ssl);
        cli->ssl = NULL;
        ereport(ERROR,
            (errmodule(MOD_SSL), errcode(ERRCODE_OPERATE_RESULT_NOT_EXPECTED), errmsg("%s", ossl_error_message())));
        return -1;
    }

    if (ossl_verify_method_name_matches_certificate(cli->peer) != 0) {
        SSL_free(cli->ssl);
        cli->ssl = NULL;
        ereport(ERROR,
            (errmodule(MOD_SSL), errcode(ERRCODE_OPERATE_RESULT_NOT_EXPECTED), errmsg("%s", ossl_error_message())));
        return -1;
    }
    return 0;
}

/*
 * @Description: clear the sensitive info in password
 * @Input: gs_openssl_client* cli, the information struct of OPENSSL
 * @Return: NULL
 * @See also:
 */
static inline void ossl_clear_passwd_mem(gs_openssl_client* cli)
{
    int rc = memset_s(cli->plain_passwd, CIPHER_LEN + 1, 0, CIPHER_LEN + 1);
    securec_check(rc, "\0", "\0");
}

/*
 * @Description:DH verification callback which verifies the DH paramters of
 *		Server DH Public key
 * @Return:success 1, failed 0
 * @See also:
 */
static int opencli_dh_verify_cb(const SSL* s, const SSL_CTX* ctx, int op, int bits, int nid, void* other, void* ex)
{
    if (op == SSL_SECOP_TMP_DH && ((bits < MIN_DHLENTH) || (bits > MAX_DHLENTH))) {
        return 0;
    }
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

    if (num == 0) {
        return 0;
    }

    if (ctx == NULL) {
        return 0;
    }
    cipher_buf = ssl_cipher_list2string(ciphers, num);
    if (cipher_buf == NULL) {
        return 0;
    }

    ret = SSL_CTX_set_cipher_list(ctx, cipher_buf);
    if (ret != 1) {
        return 0;
    }

    OPENSSL_free(cipher_buf);
    return ret;
}

#ifdef ENABLE_UT
#undef static
#endif /* ENABLE_UT */
