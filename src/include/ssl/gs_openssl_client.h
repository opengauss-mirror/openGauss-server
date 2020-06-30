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

#endif /* GS_openssl_CLIENT */
