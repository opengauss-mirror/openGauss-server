#include "postgres.h"
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <poll.h>
#include "securec_check.h"
#include "libpq/libpq-int.h"
#include "libcomm/libcomm.h"
#include "libcomm_common.h"

#define DefaultSSLMode "prefer"
static int g_nodeCount = 0;
#ifdef USE_SSL
#include "openssl/ssl.h"
#include "openssl/ossl_typ.h"
#include "openssl/x509.h"
#include "openssl/crypto.h"
#include "openssl/sslerr.h"
#include "openssl/err.h"
#include "cipher.h"

/*
 * SSL_context is currently shared between threads and therefore we need to be
 * careful to lock around any usage of it when providing thread safety.
 * ssl_config_mutex is the mutex that we use to protect it.
 */
static SSL_CTX* g_libCommClientSSLContext = NULL;

/*
 * Obtain reason string for last SSL error
 *
 * Some caution is needed here since ERR_reason_error_string will
 * return NULL if it doesn't recognize the error code.  We don't
 * want to return NULL ever.
 */

void LibCommErrFree(void* buf) {
    if (NULL != buf) {
        pfree_ext(buf);
    }
}
#define SSL_ERR_LEN 128

/*
 * Macros to handle disabling and then restoring the state of SIGPIPE handling.
 * On Windows, these are all no-ops since there's no SIGPIPEs.
 */

#define MIN_DHLENTH 112
#define MAX_DHLENTH 192

/* security ciphers suites in SSL connection */
static const char* ssl_ciphers_map[] = {
    TLS1_TXT_ECDHE_RSA_WITH_AES_128_GCM_SHA256,     /* TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256 */
    TLS1_TXT_ECDHE_RSA_WITH_AES_256_GCM_SHA384,     /* TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384 */
    TLS1_TXT_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,   /* TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256 */
    TLS1_TXT_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,   /* TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384 */
    NULL};

static int LibCommClientCheckPermissionCipherFile(const char* parent_dir, LibCommConn * conn, const char* username);
static int LibCommClientSSLDHVerifyCb(const SSL* s, const SSL_CTX* ctx,
                                    int op, int bits, int nid, void* other, void* ex);
static int LibCommClient_wildcard_certificate_match(const char* pattern, const char* string);
static int LibCommClientSSLInit(LibCommConn * conn, bool isUnidirectional = true);
void LibCommClientSSLClose(LibCommConn *conn);

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
static int verify_cb(int ok, X509_STORE_CTX* ctx)
{
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
static int LibCommClient_wildcard_certificate_match(const char* pattern, const char* string)
{
    int lenpat = strlen(pattern);
    int lenstr = strlen(string);

    /* If we don't start with a wildcard, it's not a match (rule 1 & 2) */
    if (lenpat < 3 || pattern[0] != '*' || pattern[1] != '.') {
        return 0;
    }
    if (lenpat > lenstr) {
        /* If pattern is longer than the string, we can never match */
        return 0;
    }

    if (pg_strcasecmp(pattern + 1, string + lenstr - lenpat + 1) != 0) {

        /*
         * If string does not end in pattern (minus the wildcard), we don't
         * match
         */
        return 0;
    }

    if (strchr(string, '.') < string + lenstr - lenpat) {

        /*
         * If there is a dot left of where the pattern started to match, we
         * don't match (rule 3)
         */
        return 0;
    }
    /* String ended with pattern, and didn't have a dot before, so we match */
    return 1;
}
static int LibCommClientSSLDHVerifyCb(const SSL* s, const SSL_CTX* ctx,
                                    int op, int bits, int nid, void* other, void* ex)
{
    if ((op == SSL_SECOP_TMP_DH) && ((bits < MIN_DHLENTH) || (bits > MAX_DHLENTH))) {
        return 0;
    }
    return 1;
}

static char* ssl_cipher_list2string(const char* ciphers[], const int num) {
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
static int SSL_CTX_set_cipher_list_ex(SSL_CTX* ctx, const char* ciphers[], const int num) {
    int ret = 0;
    int rc = 0;
    char* cipher_buf = NULL;

    if (ctx == NULL) {
        return 0;
    }
    cipher_buf = ssl_cipher_list2string(ciphers, num);
    if (cipher_buf == NULL) {
        return 0;
    }
    ret = SSL_CTX_set_cipher_list(ctx, cipher_buf);
    rc = memset_s(cipher_buf, strlen(cipher_buf) + 1, 0, strlen(cipher_buf) + 1);
    securec_check(rc, "\0", "\0");
    OPENSSL_free(cipher_buf);
    return ret;
}
static bool set_client_ssl_ciphers(){
    int default_ciphers_count = 0;

    for (int i = 0; ssl_ciphers_map[i] != NULL; i++) {
        default_ciphers_count++;
    }

    /* Set up the client security cipher list. */
    if (SSL_CTX_set_cipher_list_ex(g_libCommClientSSLContext, ssl_ciphers_map, default_ciphers_count) != 1) {
        return false;
    }
    return true;
}
char* LibCommErrMessage(void) {
    unsigned long errcode;
    char *errreason = NULL;
    char *errBuf = NULL;

    errBuf = (char*)palloc0(SSL_ERR_LEN);

    errcode = ERR_get_error();
    if (errcode == 0) {
        check_sprintf_s(sprintf_s(errBuf, SSL_ERR_LEN, libpq_gettext("no SSL error reported")));
        return errBuf;
    }
    errreason = (char *)ERR_reason_error_string(errcode);
    if (errreason != NULL) {
        check_strncpy_s(strncpy_s(errBuf, SSL_ERR_LEN, errreason, strlen(errreason)));
        return errBuf;
    }
    check_sprintf_s(sprintf_s(errBuf, SSL_ERR_LEN, libpq_gettext("SSL error code %lu"), errcode));

    return errBuf;
}


int LibCommClientSSLPasswd(SSL* pstContext, const char * path, const char * userName, LibCommConn * conn) {
    char* CertFilesDir = NULL;
    char CertFilesPath[MAXPATH] = {0};
    char CipherFileName[MAXPATH] = {0};

    struct stat st;
    int retval = 0;
    KeyMode mode = SERVER_MODE;
    int nRet = 0;

    if (NULL == path || '\0' == path[0]) {
        LIBCOMM_ELOG(ERROR, "Error: invalid cert file path\n");
        return -1;
    }

    nRet = strncpy_s(CertFilesPath, MAXPATH, path, MAXPATH - 1);
    securec_check_ss_c(nRet, "\0", "\0");

    CertFilesDir = CertFilesPath;
    get_parent_directory(CertFilesDir);

    /*check whether the cipher and rand files begins with userName exist.
    if exist, decrypt it.
    if not,decrypt the default cipher and rand files begins with client%.
    Because,for every client user mayown certification and private key*/
    if (NULL == userName) {
        retval = LibCommClientCheckPermissionCipherFile(CertFilesDir, conn, NULL);
        if (retval != 1)
            return retval;
        decode_cipher_files(mode, NULL, CertFilesDir, conn->cipher_passwd);
    } else {
        nRet = snprintf_s(CipherFileName, MAXPATH, MAXPATH - 1, "%s/%s%s", CertFilesDir, userName, CIPHER_KEY_FILE);
        securec_check_ss_c(nRet, "\0", "\0");
        if (lstat(CipherFileName, &st) < 0) {
            retval = LibCommClientCheckPermissionCipherFile(CertFilesDir, conn, NULL);
            if (retval != 1)
                return retval;
            decode_cipher_files(mode, NULL, CertFilesDir, conn->cipher_passwd);
        } else {
            retval = LibCommClientCheckPermissionCipherFile(CertFilesDir, conn, userName);
            if (retval != 1)
                return retval;
            decode_cipher_files(mode, userName, CertFilesDir, conn->cipher_passwd);
        }
    }
    SSL_set_default_passwd_cb_userdata(pstContext, (char*)conn->cipher_passwd);
    return 0;
}

static 
int LibCommClientSSLLoadCertFile(LibCommConn * conn, bool have_homedir, const PathData *homeDir, bool *have_cert) {
    struct stat buf;
    char fnBuf[MAXPATH] = {0};
    char setBuf[MAXBUFSIZE] = {0};
    int nRet = 0;

    if ((conn->sslcert != NULL) && strlen(conn->sslcert) > 0) {
        nRet = snprintf_s(fnBuf, MAXPATH, MAXPATH - 1, "%s/%s", getcwd(NULL,0), conn->sslcert);
        securec_check_ss_c(nRet, "\0", "\0");
    } else if (have_homedir) {
        nRet = snprintf_s(fnBuf, MAXPATH, MAXPATH - 1, "%s/%s", homeDir->data, CLIENT_CERT_FILE);
        securec_check_ss_c(nRet, "\0", "\0");
    } else {
        fnBuf[0] = '\0';
    }

    if (fnBuf[0] == '\0') {
        /* no home directory, proceed without a client cert */
        *have_cert = false;
    } else if (stat(fnBuf, &buf) != 0) {
        /*
         * If file is not present, just go on without a client cert; server
         * might or might not accept the connection.  Any other error,
         * however, is grounds for complaint.
         */
        if (errno != ENOENT && errno != ENOTDIR) {
            LIBCOMM_ELOG(ERROR, "could not open certificate file \"%s\": %s\n",
                conn->sslcert, pqStrerror(errno, setBuf, sizeof(setBuf)));
            return -1;
        }
        *have_cert = false;
    } else {
        if (LibCommClientSSLPasswd(conn->ssl, fnBuf, NULL, conn) != 0) {
            LIBCOMM_ELOG(ERROR, "Error: LibCommClientSSLLoadCertFile LibCommClientSSLPasswd\n");
            return -1;
        }
        /* check certificate file permission */
        if (!S_ISREG(buf.st_mode) || (buf.st_mode & (S_IRWXG | S_IRWXO)) || ((buf.st_mode & S_IRWXU) == S_IRWXU)) {
            char *err = LibCommErrMessage();
            LIBCOMM_ELOG(ERROR, "Error: The file \"%s\" permission should be u=rw(600) or less. mode:%3x err:%s\n",
                conn->sslcert, buf.st_mode, err);
            LibCommErrFree(err);
            return -1;
        }
        if (SSL_CTX_use_certificate_chain_file(g_libCommClientSSLContext, fnBuf) !=  1) {
            char *err = LibCommErrMessage();
            LIBCOMM_ELOG(ERROR,
                "Error: LibCommClientSSLLoadCertFile SSL_CTX_use_certificate_chain_file.sslcert:%s err:%s\n",
                conn->sslcert, err);
            LibCommErrFree(err);
            return -1;
        }
        if (SSL_use_certificate_file(conn->ssl, fnBuf, SSL_FILETYPE_PEM) != 1) {
            char *err = LibCommErrMessage();
            LIBCOMM_ELOG(ERROR, "Error: LibCommClientSSLLoadCertFile SSL_use_certificate_file sslcert:%s err:%s.\n",
                conn->sslcert, err);
            LibCommErrFree(err);
            return -1;
        }
        *have_cert = true;
    }
    LIBCOMM_ELOG(LOG, "LibCommClientSSLLoadCertFile sslcrl %s", conn->sslcert);
    return 0;
}
static int LibCommClientSSLLoadKeyFile(LibCommConn* conn, bool have_homedir, const PathData *homedir, bool have_cert) {
    struct stat buf;
    char fnBuf[MAXPATH] = {0};
    int nRet = 0;

    /*
     * Read the SSL key. If a key is specified, treat it as an engine:key
     * combination if there is colon present - we don't support files with
     * colon in the name. The exception is if the second character is a colon,
     * in which case it can be a Windows filename with drive specification.
     */
    if (have_cert && (conn->sslkey != NULL) && strlen(conn->sslkey) > 0) {
        nRet = snprintf_s(fnBuf, MAXPATH, MAXPATH - 1, "%s/%s", getcwd(NULL,0), conn->sslkey);
        securec_check_ss_c(nRet, "\0", "\0");
    } else if (have_homedir) {
        /* No PGSSLKEY specified, load default file */
        nRet = snprintf_s(fnBuf, MAXPATH, MAXPATH - 1, "%s/%s", homedir->data, CLIENT_KEY_FILE);
        securec_check_ss_c(nRet, "\0", "\0");
    } else {
        fnBuf[0] = '\0';
    }

    if (have_cert && fnBuf[0] != '\0') {
        /* read the client key from file */
        if (stat(fnBuf, &buf) != 0) {
            LIBCOMM_ELOG(ERROR,"certificate present, but not private key file \"%s\"\n", conn->sslkey);
            return -1;
        }
#ifndef WIN32
        if (!S_ISREG(buf.st_mode) || (buf.st_mode & (S_IRWXG | S_IRWXO)) || ((buf.st_mode & S_IRWXU) == S_IRWXU)) {
            LIBCOMM_ELOG(ERROR, "The file \"%s\" permission should be u=rw(600) or lespermissions.\n", conn->sslkey);
            return -1;
        }
#endif
        /* check key file permission */
        nRet = SSL_use_PrivateKey_file(conn->ssl, fnBuf, SSL_FILETYPE_PEM);
        if (nRet != 1) {
            char* err = LibCommErrMessage();
            LIBCOMM_ELOG(ERROR, "could not load private key file \"%s\": %s\n", conn->sslkey, err);
            LibCommErrFree(err);
            return -1;
        }
    }
    /* verify that the cert and key go together */
    if (have_cert && SSL_check_private_key(conn->ssl) != 1) {
        char* err = LibCommErrMessage();
        LIBCOMM_ELOG(ERROR,"certificate does not match private key file \"%s\": %s\n", conn->sslkey, err);
        LibCommErrFree(err);
        return -1;
    }
    /* set up the allowed cipher list */
    if (!set_client_ssl_ciphers()) {
        char* err = LibCommErrMessage();
        LIBCOMM_ELOG(ERROR,"SSL_ctxSetCipherList \"%s\": %s\n", conn->sslkey, err);
        LibCommErrFree(err);
        return -1;
    }
    LIBCOMM_ELOG(LOG, "LibCommClientSSLLoadKeyFile sslkey %s", conn->sslkey);
    return 0;
}

static void LibCommClientSSLLoadCrlFile(LibCommConn* conn, bool have_homedir, const PathData *homedir)
{
    struct stat buf;
    char fnBuf[MAXPATH] = {0};
    int nRet = 0;
    bool userSetSslCrl = false;
    X509_STORE* cvstore = SSL_CTX_get_cert_store(g_libCommClientSSLContext);
    if (cvstore == NULL) {
        return;
    }

    if ((conn->sslcrl != NULL) && strlen(conn->sslcrl) > 0) {
        nRet = snprintf_s(fnBuf, MAXPATH, MAXPATH - 1, "%s/%s", getcwd(NULL,0), conn->sslcrl);
        securec_check_ss_c(nRet, "\0", "\0");
        userSetSslCrl = true;
    } else if (have_homedir) {
        nRet = snprintf_s(fnBuf, MAXPATH, MAXPATH - 1, "%s/%s", homedir->data, ROOT_CRL_FILE);
        securec_check_ss_c(nRet, "\0", "\0");
    } else {
        fnBuf[0] = '\0';
    }

    if (fnBuf[0] == '\0') {
        return;
    }
    /* Set the flags to check against the complete CRL chain */
    if (stat(fnBuf, &buf) == 0 && X509_STORE_load_locations(cvstore, fnBuf, NULL) == 1) {
        (void)X509_STORE_set_flags(cvstore, X509_V_FLAG_CRL_CHECK | X509_V_FLAG_CRL_CHECK_ALL);
    } else if (userSetSslCrl) {
        LIBCOMM_ELOG(WARNING, "could not load SSL certificate revocation list (file \"%s\")\n", fnBuf);
    }
}

#define MAX_CERTIFICATE_DEPTH_SUPPORTED 20 /* The max certificate depth supported. */
static int LibCommClientSSLLoadRootCertFile(LibCommConn* conn, bool have_homedir, const PathData *homedir)
{
    struct stat buf;
    char fnBuf[MAXPATH] = {0};
    int nRet = 0;
    /*
     * If the root cert file exists, load it so we can perform certificate
     * verification. If sslmode is "verify-full" we will also do further
     * verification after the connection has been completed.
     */
    if ((conn->sslrootcert != NULL) && strlen(conn->sslrootcert) > 0) {
        nRet = snprintf_s(fnBuf, MAXPATH, MAXPATH - 1, "%s/%s", getcwd(NULL,0), conn->sslrootcert);
        securec_check_ss_c(nRet, "\0", "\0");
    } else if (have_homedir) {
        nRet = snprintf_s(fnBuf, MAXPATH, MAXPATH - 1, "%s/%s", homedir->data, ROOT_CERT_FILE);
        securec_check_ss_c(nRet, "\0", "\0");
    } else {
        fnBuf[0] = '\0';
    }

    if (fnBuf[0] != '\0' && stat(fnBuf, &buf) == 0) {
        if (SSL_CTX_load_verify_locations(g_libCommClientSSLContext, fnBuf, NULL) != 1) {
            char* err = LibCommErrMessage();
            LIBCOMM_ELOG(ERROR, "could not read root certificate file \"%s\": %s\n", conn->sslrootcert, err);
            LibCommErrFree(err);
            return -1;
        }
        /* check root cert file permission */ 
#ifndef WIN32
        if (!S_ISREG(buf.st_mode) || (buf.st_mode & (S_IRWXG | S_IRWXO)) || ((buf.st_mode & S_IRWXU) == S_IRWXU)) {
            LIBCOMM_ELOG(ERROR, "The ca file \"%s\" permission should be u=rw(600) or less.\n", conn->sslrootcert);
            return -1;
        }
#endif
        LibCommClientSSLLoadCrlFile(conn, have_homedir, homedir);
        /* Check the DH length to make sure it's at least 2048. */
        SSL_set_security_callback(conn->ssl, LibCommClientSSLDHVerifyCb);

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
            if (fnBuf[0] == '\0') {
                LIBCOMM_ELOG(ERROR,
                    "could not get home directory to locate root certificate file\n"
                        "Either provide the file or change sslmode to disable server certificate verification.\n");
                        
            } else {
                LIBCOMM_ELOG(ERROR,
                    "root certificate file \"%s\" does not exist\n"
                        "Either provide the file or change sslmode to disable server certificate verification.\n",
                    conn->sslcrl);
            }
            return -1;
        }
    }
    LIBCOMM_ELOG(LOG, "LibCommClientSSLLoadRootCertFile end");
    return 0;
}
static int LibCommClientSSLSetFiles(LibCommConn* conn) {
    PathData homedir = {{0}};
    int retval = 0;
    bool have_homedir = false;
    bool have_cert = false;

    /*
     * We'll need the home directory if any of the relevant parameters are
     * defaulted.  If pqGetHomeDirectory fails, act as though none of the
     * files could be found.
     */
    if (!((conn->sslcert != NULL) && strlen(conn->sslcert) > 0) ||
        !((conn->sslkey != NULL) && strlen(conn->sslkey) > 0) ||
        !((conn->sslrootcert != NULL) && strlen(conn->sslrootcert) > 0) ||
        !((conn->sslcrl != NULL) && strlen(conn->sslcrl) > 0)) {
        have_homedir = pqGetHomeDirectory(homedir.data, MAXPATH);
    }

    retval = LibCommClientSSLLoadCertFile(conn, have_homedir, &homedir, &have_cert);
    if (retval == -1) {
        LIBCOMM_ELOG(ERROR, "Warning: The client load Cert files failed.\n");
        return retval;
    }
    retval = LibCommClientSSLLoadKeyFile(conn, have_homedir, &homedir, have_cert);
    if (retval == -1) {
        LIBCOMM_ELOG(ERROR, "Warning: The client load key files failed.\n");
        return retval;
    }
    retval = LibCommClientSSLLoadRootCertFile(conn, have_homedir, &homedir);
    if (retval == -1) {
        LIBCOMM_ELOG(ERROR, "Warning: The client load rootcert files failed.\n");
        return retval;
    }
    return 0;
}

static inline bool LibCommClientSSLCheck() {
    return check_certificate_signature_algrithm(g_libCommClientSSLContext);
}

static int LibCommClientSSLCreate() {
    if (g_libCommClientSSLContext != NULL) {
        LIBCOMM_ELOG(WARNING, "LibComm Client SSL has already Create!\n");
        return 0;
    }

    LIBCOMM_ELOG(LOG, "g_libCommClientSSLContext start");
    if (OPENSSL_init_ssl(0, NULL) != 1) {
        char *err = LibCommErrMessage();
        LIBCOMM_ELOG(ERROR,"Failed to initialize ssl library:%s", err);
        LibCommErrFree(err);
        return -1;
    }

    SSL_load_error_strings();
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
    g_libCommClientSSLContext = SSL_CTX_new(TLSv1_2_method());
    if (g_libCommClientSSLContext == NULL) {
        char *err = LibCommErrMessage();
        LIBCOMM_ELOG(ERROR,"could not create SSL context: %s, errno:%s", err, strerror(errno));
        LibCommErrFree(err);
#pragma GCC diagnostic pop
        return -1;
    }
    /*
     * Disable  moving-write-buffer sanity check, because it
     * causes unnecessary failures in nonblocking send cases.
     */
    SSL_CTX_set_mode(g_libCommClientSSLContext, SSL_MODE_ACCEPT_MOVING_WRITE_BUFFER);

    LIBCOMM_ELOG(LOG, "LibCommClientSSLCreate end!");
    return 0;
}

static int LibCommClientSSLInit(LibCommConn * conn, bool isUnidirectional) {
    errno_t rc = 0;
    if (!isUnidirectional) {
        int retval = LibCommClientSSLSetFiles(conn);
        if (-1 == retval) {
            LIBCOMM_ELOG(ERROR, "Warning: The client set ssl files failed.\n");
            return retval;
        }
    }

    /*
     * Check the signature algorithm.
     * NOTICE : Since the client errorMessage is only output in the error exit scene,
     *          the function fprintf is used here.
     * Use the parameter stdout to output an alarm to the log or screen.
     */

    if (LibCommClientSSLCheck()) {
        LIBCOMM_ELOG(LOG, "Warning: The client certificate contain a Low-intensity signature algorithm.\n");
    }

    /* Check the certificate expires time, default alarm_days = 90d. */
    const int alarm_days = 90;
    long leftspan = check_certificate_time(g_libCommClientSSLContext, alarm_days);
    if (leftspan > 0) {
        int leftdays = leftspan / 86400 > 0 ? leftspan / 86400 : 1;
        if (leftdays > 1) {
            LIBCOMM_ELOG(LOG, "Warning: The client certificate will expire in %d days.\n", leftdays);
        } else {
            LIBCOMM_ELOG(LOG, "Warning: The client certificate will expire in %d day.\n", leftdays);
        }
    }
    rc = memset_s(conn->cipher_passwd, CIPHER_LEN + 1, 0, CIPHER_LEN + 1);
    securec_check_c(rc, "\0", "\0");
    return 0;
}

static bool LibCommClientVerifyPeerNameMatchesCertificate(LibCommConn *conn)
{
    char* peer_cn = NULL;
    int r = 0;
    int len = 0;
    bool result = false;

    /*
     * If told not to verify the peer name, don't do it. Return true
     * indicating that the verification was successful.
     */
    if (strcmp(conn->sslmode, "verify-full") != 0)
        return true;

    /* First find out the name's length and allocate a buffer for it. */
    len = X509_NAME_get_text_by_NID(X509_get_subject_name(conn->peer), NID_commonName, NULL, 0);
    if (len == -1) {
        LIBCOMM_ELOG(ERROR, "could not get server common name from server certificate\n");
        return false;
    }

    peer_cn = (char*)palloc0(len + 1);

    r = X509_NAME_get_text_by_NID(X509_get_subject_name(conn->peer), NID_commonName, peer_cn, len + 1);
    if (r != len) {
        LIBCOMM_ELOG(ERROR, "could not get server common name from server certificate\n");
        pfree_ext(peer_cn);
        return false;
    }

    peer_cn[len] = '\0';
    if ((size_t)len != strlen(peer_cn)) {
        LIBCOMM_ELOG(ERROR, "SSL certificate's common name contains embedded null, remote datanode %s, errno:%s\n",
            conn->remote_nodename, strerror(errno));
        pfree_ext(peer_cn);
        return false;
    }

    /*
     * We got the peer's common name. Now compare it against the originally
     * given hostname.
     */
    if (!((conn->libcommhost != NULL) && conn->libcommhost[0] != '\0')) {
        LIBCOMM_ELOG(ERROR, "host name must be specified for a verified SSL connection\n");
        result = false;
    } else {
        if (pg_strcasecmp(peer_cn, conn->libcommhost) == 0)
            /* Exact name match */
            result = true;
        else if (LibCommClient_wildcard_certificate_match(peer_cn, conn->libcommhost))
            /* Matched wildcard certificate */
            result = true;
        else {
            LIBCOMM_ELOG(ERROR, "server common name \"%s\" does not match host name \"%s\"\n",
                peer_cn, conn->libcommhost);
            result = false;
        }
    }
    pfree_ext(peer_cn);
    return result;
}

LibcommPollingStatusType LibCommClientSSLOpen(LibCommConn *conn) {
    ERR_clear_error();
    int r = SSL_do_handshake(conn->ssl);
    if (r <= 0) {
        int err = SSL_get_error(conn->ssl, r);
        switch (err) {
            case SSL_ERROR_WANT_READ:
                return LIBCOMM_POLLING_READING;
            case SSL_ERROR_WANT_WRITE:
                return LIBCOMM_POLLING_WRITING;

            case SSL_ERROR_SYSCALL: {
                char sebuf[256];

                if (r == -1) {
                    LIBCOMM_ELOG(ERROR, "SSL SYSCALL error: %d %s %s\n",
                        errno, SOCK_STRERROR(SOCK_ERRNO, sebuf, sizeof(sebuf)), LibCommErrMessage());
                    if (errno == 0) {
                        return LIBCOMM_POLLING_SYSCALL;
                    }
                } else {
                    LIBCOMM_ELOG(ERROR, "SSL SYSCALL error: EOF detected\n");
                }
                return LIBCOMM_POLLING_SYSCALL;
            }
            case SSL_ERROR_SSL: {
                char* buf = LibCommErrMessage();
                LIBCOMM_ELOG(ERROR, "SSL error: %s errno: %d\n", buf, errno);
                LibCommErrFree(buf);
                LibCommClientSSLClose(conn);
                return LIBCOMM_POLLING_FAILED;
            }

            default:
                LIBCOMM_ELOG(ERROR, "unrecognized SSL error code: %d\n", err);
                LibCommClientSSLClose(conn);
                return LIBCOMM_POLLING_FAILED;
        }
    }
    /*
     * We already checked the server certificate in initialize_SSL() using
     * SSL_CTX_set_verify(), if root.crt exists.
     */

    /* get server certificate */
    conn->peer = SSL_get_peer_certificate(conn->ssl);
    if (conn->peer == NULL) {
        char *err = LibCommErrMessage();
        LIBCOMM_ELOG(ERROR, "certificate could not be obtained: %s\n", err);
        LibCommErrFree(err);
        LibCommClientSSLClose(conn);
        return LIBCOMM_POLLING_FAILED;
    }

    if (!LibCommClientVerifyPeerNameMatchesCertificate(conn)) {
        LIBCOMM_ELOG(ERROR, "LibCommClientVerifyPeerNameMatchesCertificate failed!\n");
        LibCommClientSSLClose(conn);
        return LIBCOMM_POLLING_FAILED;
    }
    /* SSL handshake is complate */
    return LIBCOMM_POLLING_OK;
}

/*
 *    Initialize global SSL context
 */
int LibCommClientSecureInit() {
    return LibCommClientSSLCreate();
}
static int pqSocketPoll(int sock, int forRead, int forWrite, time_t end_time)
{
    /* We use poll(2) if available, otherwise select(2) */
#ifdef HAVE_POLL
    struct pollfd input_fd;
    int timeout_ms;

    if (!forRead && !forWrite)
        return 0;

    input_fd.fd = sock;
    input_fd.events = POLLERR;
    input_fd.revents = 0;

    if (forRead)
        input_fd.events |= POLLIN;
    if (forWrite)
        input_fd.events |= POLLOUT;

    /* Compute appropriate timeout interval */
    if (end_time == ((time_t)-1))
        timeout_ms = -1;
    else {
        time_t now = time(NULL);

        if (end_time > now)
            timeout_ms = (end_time - now) * 1000;
        else
            timeout_ms = 0;
    }

    return poll(&input_fd, 1, timeout_ms);
#else  /* !HAVE_POLL */

    fd_set input_mask;
    fd_set output_mask;
    fd_set except_mask;
    struct timeval timeout;
    struct timeval* ptr_timeout = NULL;

    if (!forRead && !forWrite) {
        return 0;
    }

    FD_ZERO(&input_mask);
    FD_ZERO(&output_mask);
    FD_ZERO(&except_mask);
    if (forRead) {
        FD_SET(sock, &input_mask);
    }

    if (forWrite) {
        FD_SET(sock, &output_mask);
    }
    FD_SET(sock, &except_mask);

    /* Compute appropriate timeout interval */
    if (end_time == ((time_t)-1)) {
        ptr_timeout = NULL;
    } else {
        time_t now = time(NULL);

        if (end_time > now) {
            timeout.tv_sec = end_time - now;
        } else {
            timeout.tv_sec = 0;
        }
        timeout.tv_usec = 0;
        ptr_timeout = &timeout;
    }

    return select(sock + 1, &input_mask, &output_mask, &except_mask, ptr_timeout);
#endif /* HAVE_POLL */
}
int LibCommClientWaitTimed(int forRead, int forWrite, LibCommConn *conn, time_t finish_time) {
    int result = 0;

    if (conn == NULL) {
        return -1;
    }
    if (!forRead && !forWrite)
        return 0;

    if (conn->socket < 0) {
        LIBCOMM_ELOG(ERROR, "socket not open\n");
        return -1;
    }

    /* Check for SSL library buffering read bytes */
    if (forRead && (conn->ssl != NULL) && SSL_pending(conn->ssl) > 0 ) {
        /* short-circuit the select */
        return 1;
    }

    /* We will retry as long as we get EINTR */
    do {
        result = pqSocketPoll(conn->socket, forRead, forWrite, finish_time);
    } while (result < 0 && SOCK_ERRNO == EINTR);

    if (result < 0) {
        char sebuf[256];
        LIBCOMM_ELOG(ERROR, "select() failed: %s, remote datanode %s, errno: %s\n",
            SOCK_STRERROR(SOCK_ERRNO, sebuf, sizeof(sebuf)), conn->remote_nodename, strerror(errno));
        return EOF;
    }
    if (result == 0) {
        LIBCOMM_ELOG(ERROR, "wait %s timeout expired\n", conn->remote_nodename);
        return EOF;
    }

    return 0;
}
LibcommPollingStatusType LibCommClientSecureOpen(LibCommConn *conn, bool isUnidirectional) {
    LibcommPollingStatusType status = LIBCOMM_POLLING_OK;
    
    const int timeout = 2;
    if (conn->ssl == NULL) {
        /* We cannot use MSG_NOSIGNAL to block SIGPIPE when using SSL */
        conn->sigpipe_flag = false;

        /* Create a connection-specific SSL object */
        if ((conn->ssl = SSL_new(g_libCommClientSSLContext)) == NULL || (!SSL_set_app_data(conn->ssl, conn)) ||
            !SSL_set_fd(conn->ssl, (int)((intptr_t)(conn->socket)))) {
            char *err = LibCommErrMessage();
            LIBCOMM_ELOG(ERROR, "could not establish SSL connection, remote datanode %s: %s, err:%s\n", 
                conn->remote_nodename, err, strerror(errno));
            LibCommErrFree(err);
            LibCommClientSSLClose(conn);
            return LIBCOMM_POLLING_FAILED;
        }
        /*
         * Load client certificate, private key, and trusted CA certs.
         */
        if (LibCommClientSSLInit(conn, isUnidirectional) != 0) {
            LIBCOMM_ELOG(ERROR, "LibCommClientSSLInit failed!\n");
            LibCommClientSSLClose(conn);
            return LIBCOMM_POLLING_FAILED;
        }
        SSL_set_connect_state (conn->ssl);
loop:
        status = LibCommClientSSLOpen(conn);
        if (status == LIBCOMM_POLLING_OK) {
            LIBCOMM_ELOG(LOG, "LibCommClientSecureOpen SSL handshake done, ready to send startup packet");
        } else if (status == LIBCOMM_POLLING_FAILED) {
            LIBCOMM_ELOG(ERROR, "LibCommClientSecureOpen keep going\n");
            return LIBCOMM_POLLING_FAILED;
        } else if (status == LIBCOMM_POLLING_READING) {
            LIBCOMM_ELOG(LOG, "LibCommClientSecureOpen LIBCOMM_POLLING_READING");
            if (LibCommClientWaitTimed(1, 0, conn, time(NULL) + timeout)) {
                LIBCOMM_ELOG(WARNING, "LibCommClientWaitTimed 1 0 failed");
            }
            goto loop;
        } else if (status == LIBCOMM_POLLING_WRITING) {
            LIBCOMM_ELOG(LOG, "LibCommClientSecureOpen LIBCOMM_POLLING_WRITING");
            if (LibCommClientWaitTimed(0, 1, conn, time(NULL) + timeout)) {
                LIBCOMM_ELOG(WARNING, "LibCommClientWaitTimed 0 1 failed");
            }
            goto loop;
        } else if (status == LIBCOMM_POLLING_SYSCALL) {
            LIBCOMM_ELOG(WARNING, "LibCommClientSecureOpen LIBCOMM_POLLING_SYSCALL");
            return LIBCOMM_POLLING_SYSCALL;
        }
    }
    LIBCOMM_ELOG(LOG, "LibCommClientSecureOpen %d", status);
    return status;
}

SSL* LibCommClientGetSSLForSocket(int socket) {
    if (socket < 0) {
        return NULL;
    }
    int nodeNum = g_nodeCount;
    if (nodeNum <= 0) {
        return NULL;
    }

    for(int i = 0; i < nodeNum; i++) {
        if (g_instance.attr.attr_network.comm_ctrl_channel_conn[i]->ssl != NULL &&
            g_instance.attr.attr_network.comm_ctrl_channel_conn[i]->socket == socket) {
            return g_instance.attr.attr_network.comm_ctrl_channel_conn[i]->ssl;
        }

        if (g_instance.attr.attr_network.comm_data_channel_conn[i]->ssl != NULL &&
            g_instance.attr.attr_network.comm_data_channel_conn[i]->socket == socket) {
            return g_instance.attr.attr_network.comm_data_channel_conn[i]->ssl;
        }
    }

    return comm_ssl_find_ssl_by_fd(socket);
}

ssize_t LibCommClientSSLRead(SSL *ssl, void* ptr, size_t len) {
    ssize_t n = 0;
    int result_errno = 0;
    char sebuf[256];

    if (ssl != NULL) {
        int err;

    rloop:
        SOCK_ERRNO_SET(0);
        ERR_clear_error();
        n = SSL_read(ssl, ptr, len);

        err = SSL_get_error(ssl, n);
        switch (err) {
            case SSL_ERROR_NONE:
                if (n < 0) {
                    /* Not supposed to happen, so we don't translate the msg */
                    LIBCOMM_ELOG(ERROR, 
                        "SSL_read failed but did not provide error information, err: %s\n",
                        strerror(errno));
                    /* assume the connection is broken */
                    result_errno = ECONNRESET;
                }
                break;
            case SSL_ERROR_WANT_READ:
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
                    LIBCOMM_ELOG(ERROR, "SSL SYSCALL error: %s, error: %s\n",
                        SOCK_STRERROR(result_errno, sebuf, sizeof(sebuf)), strerror(errno));
                } else {
                    LIBCOMM_ELOG(ERROR, "SSL SYSCALL error: EOF detected, error: %s\n", strerror(errno));
                    /* assume the connection is broken */
                    result_errno = ECONNRESET;
                    n = -1;
                }
                break;
            case SSL_ERROR_SSL: {
                char* errm = LibCommErrMessage();
                LIBCOMM_ELOG(ERROR, "SSL error: %s, error: %s errno:%d\n", errm, strerror(errno), errno);
                LibCommErrFree(errm);
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
                LIBCOMM_ELOG(ERROR, "SSL connection has been closed unexpectedly, error: %s\n", strerror(errno));
                result_errno = ECONNRESET;
                n = -1;
                break;
            default:
                LIBCOMM_ELOG(ERROR, "unrecognized SSL error code: %d, error: %s\n", err, strerror(errno));
                /* assume the connection is broken */
                result_errno = ECONNRESET;
                n = -1;
                break;
        }
    }

    /* ensure we return the intended errno to caller */
    SOCK_ERRNO_SET(result_errno);
    return n;
}
ssize_t LibCommSSLRead(SSL *ssl, void* ptr, size_t len) {
    ssize_t n = 0;
    int result_errno = 0;
    char sebuf[256];

    if (ssl != NULL) {
        int err;

    rloop:
        SOCK_ERRNO_SET(0);
        ERR_clear_error();
        n = SSL_read(ssl, ptr, len);

        err = SSL_get_error(ssl, n);
        switch (err) {
            case SSL_ERROR_NONE:
                if (n < 0) {
                    /* Not supposed to happen, so we don't translate the msg */
                    LIBCOMM_ELOG(ERROR, 
                        "SSL_read failed but did not provide error information, err: %s\n",
                        strerror(errno));
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
                    LIBCOMM_ELOG(ERROR,
                        "SSL SYSCALL error: %s, error: %s\n",
                        SOCK_STRERROR(result_errno, sebuf, sizeof(sebuf)), strerror(errno));
                } else {
                    LIBCOMM_ELOG(ERROR, "SSL SYSCALL error: EOF detected, error: %s\n", strerror(errno));
                    /* assume the connection is broken */
                    result_errno = ECONNRESET;
                    n = -1;
                }
                break;
            case SSL_ERROR_SSL: {
                char* errm = LibCommErrMessage();
                LIBCOMM_ELOG(ERROR, "SSL error: %s, error: %s errno:%d\n", errm, strerror(errno), errno);
                LibCommErrFree(errm);
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
                LIBCOMM_ELOG(ERROR, "SSL connection has been closed unexpectedly, error: %s\n", strerror(errno));
                result_errno = ECONNRESET;
                n = -1;
                break;
            default:
                LIBCOMM_ELOG(ERROR, "unrecognized SSL error code: %d, error: %s\n", err, strerror(errno));
                /* assume the connection is broken */
                result_errno = ECONNRESET;
                n = -1;
                break;
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
ssize_t LibCommClientSSLWrite(SSL *ssl, const void* ptr, size_t len) {
    ssize_t n = 0;
    int result_errno = 0;
    char sebuf[256];

    if (ssl != NULL) {
        int err;

        SOCK_ERRNO_SET(0);
        ERR_clear_error();
        n = SSL_write(ssl, ptr, len);

        err = SSL_get_error(ssl, n);
        switch (err) {
            case SSL_ERROR_NONE:
                if (n < 0) {
                    /* Not supposed to happen, so we don't translate the msg */
                    LIBCOMM_ELOG(ERROR, 
                        "SSL_write failed but did not provide error information, error: %s\n", strerror(errno));
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
                    LIBCOMM_ELOG(ERROR, "SSL SYSCALL errno: %d error: %s\n",
                        errno, SOCK_STRERROR(result_errno, sebuf, sizeof(sebuf)));
                } else {
                    LIBCOMM_ELOG(ERROR, "SSL SYSCALL error: EOF detected, errno :%d error: %s\n",
                        errno, strerror(errno));
                    /* assume the connection is broken */
                    result_errno = ECONNRESET;
                    n = -1;
                }
                break;
            case SSL_ERROR_SSL: {
                char* errm = LibCommErrMessage();
                LIBCOMM_ELOG(ERROR, "SSL error: %s, error: %s\n", errm,strerror(errno));
                LibCommErrFree(errm);
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
                LIBCOMM_ELOG(ERROR, "SSL connection has been closed unexpectedly, error: %s\n", strerror(errno));
                result_errno = ECONNRESET;
                n = -1;
                break;
            default:
                LIBCOMM_ELOG(ERROR, "unrecognized SSL error code: %d, error: %s\n", err, strerror(errno));
                /* assume the connection is broken */
                result_errno = ECONNRESET;
                n = -1;
                break;
        }
    }
    /* ensure we return the intended errno to caller */
    SOCK_ERRNO_SET(result_errno);

    return n;
}
static void LibCommClientSSLDestory(LibCommConn *conn) {
    /* free ssl context */
    if (g_libCommClientSSLContext != NULL) {
        SSL_CTX_free(g_libCommClientSSLContext);
        g_libCommClientSSLContext = NULL;
    }
}

void LibCommClientSSLClose(LibCommConn *conn) {
    bool destroy_needed = false;
    if (conn->ssl != NULL) {
        /*
         * We can't destroy everything SSL-related here due to the possible
         * later calls to OpenSSL routines which may need our thread
         * callbacks, so set a flag here and check at the end.
         */
        destroy_needed = true;
        SSL_shutdown(conn->ssl);
        SSL_free(conn->ssl);
        conn->ssl = NULL;
    }
    if (conn->peer != NULL) {
        X509_free(conn->peer);
        conn->peer = NULL;
    }
    if (destroy_needed) {
        LibCommClientSSLDestory(conn);
        LIBCOMM_ELOG(LOG, "unrecognized SSL error code: %d, error:%s %m\n", errno, strerror(errno));
    }
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
#endif
int LibCommClientCheckPermissionCipherFile(const char* parent_dir, LibCommConn * conn, const char* username)
{
    char cipher_file[MAXPATH] = {0};
    char rand_file[MAXPATH] = {0};
    struct stat cipherbuf;
    struct stat randbuf;
    int nRet = 0;
    if (NULL == username) {
        nRet = snprintf_s(cipher_file, MAXPATH, MAXPATH - 1, "%s/server%s", parent_dir, CIPHER_KEY_FILE);
        securec_check_ss_c(nRet, "\0", "\0");
        nRet = snprintf_s(rand_file, MAXPATH, MAXPATH - 1, "%s/server%s", parent_dir, RAN_KEY_FILE);
        securec_check_ss_c(nRet, "\0", "\0");
    } else {
        nRet = snprintf_s(cipher_file, MAXPATH, MAXPATH - 1, "%s/%s%s", parent_dir, username, CIPHER_KEY_FILE);
        securec_check_ss_c(nRet, "\0", "\0");
        nRet = snprintf_s(rand_file, MAXPATH, MAXPATH - 1, "%s/%s%s", parent_dir, username, RAN_KEY_FILE);
        securec_check_ss_c(nRet, "\0", "\0");
    }
    /*cipher file or rand file do not exist,skip check the permission.
    For key and certications without password,it is also ok*/
    if (lstat(cipher_file, &cipherbuf) != 0 || lstat(rand_file, &randbuf) != 0)
        return 0;

        /*cipher file and rand file exist,so check whether permissions meets the requirements */
#ifndef WIN32
    if (!S_ISREG(cipherbuf.st_mode) ||
        (cipherbuf.st_mode & (S_IRWXG | S_IRWXO)) ||
        ((cipherbuf.st_mode & S_IRWXU) == S_IRWXU)) {
        LIBCOMM_ELOG(WARNING, "The file \"server%s\" permission should be u=rw(600) or less.\n", CIPHER_KEY_FILE);
        return -1;
    }
    if (!S_ISREG(randbuf.st_mode) ||
        (randbuf.st_mode & (S_IRWXG | S_IRWXO)) ||
        ((randbuf.st_mode & S_IRWXU) == S_IRWXU)) {
        LIBCOMM_ELOG(WARNING, "The file \"server%s\" permission should be u=rw(600) or less.\n", RAN_KEY_FILE);
        return -1;
    }
#endif
    /*files exist,and permission is ok!*/
    return 1;
}
#endif

static void LibcommClientSSLFillConn(LibCommConn * conn) {
    conn->sslcert = strdup(g_instance.attr.attr_security.ssl_cert_file);
    conn->sslkey = strdup(g_instance.attr.attr_security.ssl_key_file);
    conn->sslrootcert = strdup(g_instance.attr.attr_security.ssl_ca_file);
    conn->sslcrl = strdup(g_instance.attr.attr_security.ssl_crl_file);
    if (conn->sslmode != NULL) {
        if (strcmp(conn->sslmode, "disable") != 0 && strcmp(conn->sslmode, "allow") != 0 &&
            strcmp(conn->sslmode, "prefer") != 0 && strcmp(conn->sslmode, "require") != 0 &&
            strcmp(conn->sslmode, "verify-ca") != 0 && strcmp(conn->sslmode, "verify-full") != 0) {
            LIBCOMM_ELOG(ERROR,"invalid sslmode value: \"%s\"\n", conn->sslmode);
        }
    } else {
        conn->sslmode = strdup(DefaultSSLMode);
    }
}

/*
 * makeEmptyCommconn
 *	 - create a libCommconn data structure with (as yet) no interesting data
 */
LibCommConn* LibCommClientSSLMakeEmptyConn(void)
{
    LibCommConn* conn = NULL;

#ifdef WIN32

    /*
     * Make sure socket support is up and running.
     */
    WSADATA wsaData;

    if (WSAStartup(MAKEWORD(1, 1), &wsaData))
        return NULL;
    WSASetLastError(0);
#endif

    conn = (LibCommConn*)palloc(sizeof(LibCommConn));
    if (conn == NULL) {
#ifdef WIN32
        WSACleanup();
#endif
        return conn;
    }

    /* Zero all pointers and booleans */
    check_memset_s(memset_s(conn, sizeof(LibCommConn), 0, sizeof(LibCommConn)));

    /* install default notice hooks */

    conn->socket = -1;

    LibcommClientSSLFillConn(conn);
    return conn;
}

int LibCommInitChannelConn(int nodeNum) {
    if (g_instance.attr.attr_network.comm_ctrl_channel_conn != NULL &&
        g_instance.attr.attr_network.comm_data_channel_conn != NULL) {
        return 0;
    }
    g_nodeCount = nodeNum;
    if (g_instance.attr.attr_network.comm_ctrl_channel_conn == NULL) {
        g_instance.attr.attr_network.comm_ctrl_channel_conn = (LibCommConn**)palloc0(nodeNum * sizeof(LibCommConn*));
        for (int i = 0; i< nodeNum; i++) {
            g_instance.attr.attr_network.comm_ctrl_channel_conn[i] = LibCommClientSSLMakeEmptyConn();
            if (g_instance.attr.attr_network.comm_ctrl_channel_conn[i] == NULL) {
                return -1;
            }
        }
    }
    if (g_instance.attr.attr_network.comm_data_channel_conn == NULL) {
        g_instance.attr.attr_network.comm_data_channel_conn = (LibCommConn**)palloc0(nodeNum * sizeof(LibCommConn*));
        for (int i = 0; i< nodeNum; i++) {
            g_instance.attr.attr_network.comm_data_channel_conn[i] = LibCommClientSSLMakeEmptyConn();
            if (g_instance.attr.attr_network.comm_data_channel_conn[i] == NULL) {
                return -1;
            }
        }
    }

    return 0;
}
int LibCommClientReadBlock(LibCommConn *conn, void* data, int size, int flags)
{
#ifdef LIBCOMM_FAULT_INJECTION_ENABLE
    if (is_comm_fault_injection(LIBCOMM_FI_MC_TCP_READ_BLOCK_FAILED)) {
        LIBCOMM_ELOG(WARNING, "(mc tcp read block)\t[FAULT INJECTION]Failed to read block for %d.", fd);
        return -1;
    }
#endif
    uint64 time_enter, time_now;
    time_enter = mc_timers_ms();
    ssize_t nbytes = 0;
    struct timespec ts;
    ts.tv_sec = 0;
    ts.tv_nsec = 1000000;

    /* In our application, if we do not get an integrated message, we must continue receiving. */
    while (nbytes != size) {
        int rc = 0;
#ifdef USE_SSL
        if (conn->ssl != NULL) {
            rc = LibCommClientSSLRead(conn->ssl, (char*)data + nbytes, size - nbytes);
        } else {
            rc = recv(conn->socket, (char*)data + nbytes, size - nbytes, flags);
        }
#else
        rc = recv(conn->socket, (char*)data + nbytes, size - nbytes, flags);
#endif
        if (rc > 0) {
            if (((char*)data)[0] == '\0') {
                LIBCOMM_ELOG(ERROR, "(mc tcp read block)\tIllegal message from sock %d.", conn->socket);
                return -1;
            }

            nbytes = nbytes + rc;

        } else if (rc == 0) { //  Orderly shutdown by the other peer.
            nbytes = 0;
            break;
        } else if (rc < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR) {
                time_now = mc_timers_ms();
                // If can not receive data after 600 seconds later,
                // we think this connection has problem
                if (((time_now - time_enter) >
                        ((uint64)(unsigned)g_instance.comm_cxt.mctcp_cxt.mc_tcp_send_timeout * SEC_TO_MICRO_SEC)) &&
                    (time_now > time_enter)) {
                    errno = ECOMMTCPSENDTIMEOUT;
                    return -1;
                }
                (void)nanosleep(&ts, NULL);
                continue;
            } else {
                nbytes = -1;
                break;
            }
        }
    }
    /* Orderly shutdown by the other peer or Signalise peer failure. */
    if ((nbytes == 0) || (nbytes == -1 && (errno == ECONNRESET || errno == ECONNREFUSED || errno == ETIMEDOUT ||
                                              errno == EHOSTUNREACH))) {
        return -1;
    }

    return (size_t)nbytes;
}
int LibCommClientWriteBlock(LibCommConn *conn, void* data, int size)
{
#ifdef LIBCOMM_FAULT_INJECTION_ENABLE
    if (is_comm_fault_injection(LIBCOMM_FI_MC_TCP_WRITE_FAILED)) {
        LIBCOMM_ELOG(WARNING, "(mc tcp write)\t[FAULT INJECTION]Failed to write for %d.", fd);
        shutdown(fd, SHUT_RDWR);
        return -1;
    }
#endif
    ssize_t nbytes = 0;
    ssize_t nSend = 0;
    const int flags = 0;
    uint64 time_enter, time_now;
    time_enter = mc_timers_ms();

    //  Several errors are OK. When speculative write is being done we may not
    //  be able to write a single byte to the socket. Also, SIGSTOP issued
    //  by a debugging tool can result in EINTR error.
    //
    while (nSend != size) {
#ifdef USE_SSL
        if (conn->ssl != NULL) {
            nbytes = LibCommClientSSLWrite(conn->ssl, (void*)((char*)data + nSend), size - nSend);
        } else {
            nbytes = send(conn->socket, (const void*)((char*)data + nSend), size - nSend, flags);
        }
#else
        nbytes = send(conn->socket, (const void*)((char*)data + nSend), size - nSend, flags);
#endif

        if (nbytes <= 0) {
            if (nbytes == -1 && (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR || errno == ENOBUFS)) {
                time_now = mc_timers_ms();
                // If can not receive data after 600 seconds later,
                // we think this connection has problem
                if (((time_now - time_enter) >
                        ((uint64)(unsigned)g_instance.comm_cxt.mctcp_cxt.mc_tcp_send_timeout * SEC_TO_MICRO_SEC)) &&
                    (time_now > time_enter)) {
                    errno = ECOMMTCPSENDTIMEOUT;
                    return -1;
                }
                continue;
            }

            return -1;
        } else {
            nSend += nbytes;
        }
    }

    return (size_t)nSend;
}


int LibCommClientCheckSocket(LibCommConn * conn)
{
    char temp_buf[IOV_DATA_SIZE];
    bool is_sock_err = false;
    int error = -1;

    if (conn->socket < 0) {
        return -1;
    }

    while (false == is_sock_err) {
#ifdef USE_SSL
        if (g_instance.attr.attr_network.comm_enable_SSL) {
            if (conn->ssl != NULL) {
                error = LibCommSSLRead(conn->ssl, temp_buf, IOV_DATA_SIZE);
            } else {
                error = recv(conn->socket, temp_buf, IOV_DATA_SIZE, 0);
            }
        } else
#endif
        {
            error = recv(conn->socket, temp_buf, IOV_DATA_SIZE, 0);
        }

        if (error < 0) {
            // no data to recieve, and no socket error
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                break;
            }
            // need retry
            else if (errno == EINTR) {
                continue;
            }
            // other errno means really error
            else {
                is_sock_err = true;
                break;
            }
        }

        // remote has closed
        if (error == 0) {
            is_sock_err = true;
            break;
        }

        if (error > 0) {
            // something fault data
            continue;
        }
    }

    return 0;
}
