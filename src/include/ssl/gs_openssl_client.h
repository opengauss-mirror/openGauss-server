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
 * ---------------------------------------------------------------------------------------
 * 
 * gs_openssl_client.h
 *     initialize the ssl system of client which is based on the openssl library
 * 
 * IDENTIFICATION
 *        src/include/ssl/gs_openssl_client.h
 *
 * ---------------------------------------------------------------------------------------
 */


#ifndef GS_openssl_CLIENT
#define GS_openssl_CLIENT

#include "cipher.h"

#include "openssl/ossl_typ.h"
#include "openssl/err.h"
#include "openssl/err.h"
#include "openssl/ssl.h"
#include "openssl/bio.h"
#include "openssl/conf.h"
#include "openssl/crypto.h"
#include "openssl/evp.h"
#include "openssl/rand.h"
#include "openssl/conf.h"

#define OPENSSL_CLI_EXCEPTTION (-4)
#define OPENSSL_CLI_BAD_SOCKET (-3)
#define OPENSSL_CLI_EAGAIN (-2)

typedef struct gs_openssl_client* gs_openssl_cli;

extern void gs_openssl_cli_init_system(void);

/* create a gs_openssl_client object */
extern gs_openssl_cli gs_openssl_cli_create(void);
/* destroy a gs_openssl_client object */
extern void gs_openssl_cli_destroy(gs_openssl_cli cli);
/* set the certificate file names before loading them */
extern void gs_openssl_cli_setfiles(
    gs_openssl_cli cli, const char* ssl_dir, const char* rootcert, const char* client_key, const char* clientcert);

/* the main function of initialize the ssl system of client using openssl */
extern int gs_openssl_cli_initialize_SSL(gs_openssl_cli cli, int sock_id);

/* read && write interface  */
extern int gs_openssl_cli_read(gs_openssl_cli cli, char* obuf, int maxlen);
extern int gs_openssl_cli_write(gs_openssl_cli cli, const char* buf, int len);

#ifdef ENABLE_UT
extern int ossl_init_client_ssl_passwd(SSL_CTX* pstContext, const char* cert_file_dir, GS_UCHAR* cipher_passwd);
extern const char* ossl_error_message(void);
#endif /* ENABLE_UT */

#if OPENSSL_VERSION_NUMBER < 0x10100000L
static int OPENSSL_init_ssl(int unused1, const SSL_CTX* unused2)
{
    SSL_library_init();
    SSL_load_error_strings();
    OpenSSL_add_all_algorithms();
    return (ERR_peek_error() == 0) ? 1 : 0;
}

inline void* BIO_get_data(const BIO* bio)
{
    return bio->ptr;
}

inline void BIO_set_data(BIO* bio, void* data)
{
    bio->ptr = data;
}

inline void* OPENSSL_zalloc(size_t num)
{
    void* ptr = OPENSSL_malloc(num);
    if (ptr != NULL) {
        memset_s(ptr, num, 0, num);
    }
    return ptr;
}

inline void SSL_set_security_callback(
    SSL *ssl,
    int (*cb)(const SSL* s, const SSL_CTX* ctx, int op, int bits, int nid, void* other, void* ex))
{
    SSL_CTX_set_cert_verify_callback(SSL_get_SSL_CTX(ssl), (int (*)(X509_STORE_CTX*, void*))cb, NULL);
}

inline void SSL_set_default_passwd_cb_userdata(SSL *ssl, void *userdata)
{
    SSL_CTX_set_default_passwd_cb_userdata(SSL_get_SSL_CTX(ssl), userdata);
}

#define SSL_SECOP_TMP_DH 1

inline const EVP_CIPHER *EVP_sm4_ctr(void)
{
    return EVP_aes_128_ctr();
}

inline const unsigned char* ASN1_STRING_get0_data(const ASN1_STRING *x)
{
    return ASN1_STRING_data((ASN1_STRING *)x);
}

inline HMAC_CTX* HMAC_CTX_new()
{
    HMAC_CTX* ctx = (HMAC_CTX*)OPENSSL_malloc(sizeof(HMAC_CTX));
    if (ctx != NULL) {
        HMAC_CTX_init(ctx);
    }
    return ctx;
}
inline void HMAC_CTX_free(HMAC_CTX* ctx)
{
    if (ctx != NULL) {
        HMAC_CTX_cleanup(ctx);
        OPENSSL_free(ctx);
    }
}
inline EVP_MD_CTX* EVP_MD_CTX_new()
{
    return EVP_MD_CTX_create();
}
inline void EVP_MD_CTX_free(EVP_MD_CTX* ctx)
{
    EVP_MD_CTX_destroy(ctx);
}
inline const EVP_CIPHER* EVP_sm4_cbc()
{
    return EVP_aes_128_cbc();
}
inline int EVP_CIPHER_CTX_ctrl(EVP_CIPHER_CTX* ctx, int cmd, int p1, void* p2)
{
    return 1;
}

#ifndef EVP_CTRL_AEAD_GET_TAG
#define EVP_CTRL_AEAD_GET_TAG EVP_CTRL_GCM_GET_TAG
#endif
#ifndef EVP_CTRL_AEAD_SET_TAG
#define EVP_CTRL_AEAD_SET_TAG EVP_CTRL_GCM_SET_TAG
#endif
#define EVP_PKEY_SM2 0x00000013
#define NID_sm2 784

inline int EVP_PKEY_set_alias_type(EVP_PKEY *pkey, int type)
{
    if (type == EVP_PKEY_SM2) {
        EVP_PKEY_set_type(pkey, EVP_PKEY_EC);
    }
    return 1;
}

inline int DH_set0_pqg(DH *dh, BIGNUM *p, BIGNUM *q, BIGNUM *g)
{
    if (dh->p) BN_free(dh->p);
    dh->p = BN_dup(p);
    if (dh->g) BN_free(dh->g);
    dh->g = BN_dup(g);
    if (dh->q) BN_free(dh->q);
    dh->q = NULL;
    return 1;
}
#endif /* OPENSSL_VERSION_NUMBER < 0x10100000L */
#endif /* GS_openssl_CLIENT */
