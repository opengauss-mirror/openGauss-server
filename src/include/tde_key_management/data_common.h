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
 *   data_common.h
 *   class for TDE data structure
 *
 * IDENTIFICATION
 *    src/include/tde_key_management/data_common.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef SEC_DATA_COMMON_H
#define SEC_DATA_COMMON_H

#include "postgres.h"
#include "utils/timestamp.h"
#include "storage/smgr/relfilenode.h"

/* TDE macros and struct */
#define DEK_CIPHER_LEN 320  /* The length of dek cipher is 312 for AES256, and for AES128 and SE4 is 280 */
#define DEK_CIPHER_AES256_LEN_PAGE 156 /* The length of dek cipher on page is 156 for AES256 */
#define DEK_CIPHER_AES128_LEN_PAGE 140 /* The length of dek cipher on page is 140 for AES128 and SE4 */
#define CMK_ID_LEN 40       /* cmk id string length is 36 from kms */
#define CMK_ID_LEN_PAGE 16
#define RANDOM_IV_LEN 16
#define GCM_TAG_LEN 16
#define RES_LEN 3

/* 
 * TdePageInfo is used to save the tde info on a page.
 * these arrays do not end with '\0', so cannot be directly processed as strings.
*/
typedef struct {
    uint8 dek_cipher[DEK_CIPHER_AES256_LEN_PAGE]; /* The maximum length is used to meet AES256 and AES128/SM4 */
    uint8 cmk_id[CMK_ID_LEN_PAGE];
    uint8 iv[RANDOM_IV_LEN];
    uint8 tag[GCM_TAG_LEN];
    uint8 algo;
    uint8 res[RES_LEN];
} TdePageInfo;

/* TdeInfo is used for encryption and decryption */
typedef struct {
    char dek_cipher[DEK_CIPHER_LEN];
    char cmk_id[CMK_ID_LEN];
    uint8 iv[RANDOM_IV_LEN];
    uint8 tag[GCM_TAG_LEN];
    uint8 algo;
    uint8 res[RES_LEN];
} TdeInfo;

typedef enum {
    IAM_TOKEN = 0,
    IAM_AGENCY_TOKEN,
    KMS_GEN_DEK,
    KMS_GET_DEK,
} ResetApiType;

typedef struct {
    char* user_name;
    char* password;
    char* domain_name;
    char* project_name;
} TokenInfo;

typedef struct {
    char* domain_name;
    char* agency_name;
    char* project_name;
} AgencyTokenInfo;

typedef struct {
    char* project_name;
    char* project_id;
} KmsInfo;

typedef struct AdvStrNode {
    char *str_val;
    struct AdvStrNode *next;
} AdvStrNode;

typedef struct AdvStrList {
    int node_cnt;
    AdvStrNode *first_node;
} AdvStrList;

typedef enum {
    IAM_AUTH_REQ = 0,
    IAM_AGENCY_TOKEN_REQ,
    TDE_GEN_DEK_REQ,
    TDE_DEC_DEK_REQ,
} KmsHttpMsgType;

typedef struct {
    const char *src_value;
    const char *dest_value;
} ReplaceJsonValue;

typedef struct {
    char* cipher;
    char* plain;
} DekInfo;

typedef struct {
    /* key_cipher is hash table key */
    char* key_cipher;
    /* dek_plaintext and timestamp are values */
    char* dek_plaintext;
    TimestampTz timestamp;
} TDECacheEntry;

/* TDE buffer cache entry key is RelFileNode  value is TdeInfo */
typedef struct {
    RelFileNode tde_node;
    TdeInfo *tde_info;
} TdeFileNodeEntry;

#define tde_list_free(ptr)        \
    do {                            \
        if ((ptr) != NULL) {        \
            free_advstr_list((ptr)); \
            (ptr) = NULL;           \
        }                           \
    } while (0)

AdvStrList *malloc_advstr_list(void);
size_t tde_list_len(AdvStrList *list);
void tde_append_node(AdvStrList *list, const char *str_val);
AdvStrList *tde_split_node(const char *str, char split_char);
void free_advstr_list(AdvStrList *list);
char *tde_get_val(AdvStrList *list, int list_pos);
void free_advstr_list_with_skip(AdvStrList *list, int list_pos);

class TDEData : public BaseObject {
public:
    TDEData();
    ~TDEData();

    char* cmk_id;
    char* dek_cipher;
    char* dek_plaintext;
};

#endif
