/*
 * Copyright (c) 2022 Huawei Technologies Co.,Ltd.
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
 * security_his_kms.cpp
 *
 * IDENTIFICATION
 *	  src/gausskernel/security/keymgr/src/his/security_his_kms.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "keymgr/his/security_his_kms.h"
#include <string.h>
#include <openssl/rand.h>
#include "keymgr/comm/security_http.h"
#include "keymgr/comm/security_utils.h"
#include "keymgr/comm/security_json.h"
#include "keymgr/comm/security_encode.h"
#include "keymgr/his/security_his_iam.h"

typedef struct {
    char *kmsurl;
    char *keyid;
} HisKmsKeyPath;

HisKmsMgr *his_kms_new(HisIamMgr *iam, KmErr *errbuf)
{
    HisKmsMgr *kms;

    if (iam == NULL) {
        return NULL;
    }

    kms = (HisKmsMgr *)km_alloc_zero(sizeof(HisKmsMgr));
    if (kms == NULL) {
        return NULL;
    }

    kms->err = errbuf;
    kms->iam = iam;
    kms->httpmgr = httpmgr_new(errbuf);
    if (kms->httpmgr == NULL) {
        km_free(kms);
        return NULL;
    }

    return kms;
}

void his_kms_free(HisKmsMgr *kms)
{
    if (kms == NULL) {
        return;
    }

    km_safe_free(kms->kmsurl);
    km_safe_free(kms->appid);
    km_safe_free(kms->domain);
    km_safe_free(kms->reqbody);

    httpmgr_free(kms->httpmgr);
    km_safe_free(kms->cacert);

    km_free(kms);
}

void his_kms_set_arg(HisKmsMgr *kms, const char *key, const char *value)
{
    if (key == NULL || value == NULL) {
        return;
    }

    if (strcasecmp(key, "hisappid") == 0) {
        km_safe_free(kms->appid);
        kms->appid = km_strdup(value);
    } else if (strcasecmp(key, "hisdomain") == 0) {
        km_safe_free(kms->domain);
        kms->domain = km_strdup(value);
    } else if (strcasecmp(key, "hiskmsurl") == 0) {
        km_safe_free(kms->kmsurl);
        kms->kmsurl = km_strdup(value);
    } else if (strcasecmp(key, "hiscacert") == 0) {
        km_safe_free(kms->cacert);
        kms->cacert = km_realpath(value, kms->err);
    } /* else just ignore unkown para */
}

/* the format of keypath is 'URL/ID', we need return 'KEYID' */
static HisKmsKeyPath his_kms_parser_key_path(HisKmsMgr *kms, const char *keypath)
{
#define HIS_KMS_URL_LEN 1024
    size_t split = 0;
    HisKmsKeyPath kpath = {0};
    size_t i;

    if (keypath == NULL || strlen(keypath) <= 1) {
        km_err_msg(kms->err, "his kms failed to get keypath filed.");
        return kpath;
    }

    if (strlen(keypath) > HIS_KMS_URL_LEN) {
        km_err_msg(kms->err, "failed to access his kms, the length of keypath exceeds 1024.");
        return kpath;
    }

    for (i = strlen(keypath) - 1; i > 0; i--) {
        if (keypath[i] == '/') {
            split = i;
            break;
        }
    }

    if (split == 0 || split == strlen(keypath) - 1) {
        km_err_msg(kms->err, "the format of keypath should be like 'KmsUrl/KeyId', but not '%s'.", keypath);
        return kpath;
    }

    kpath.keyid = km_strdup(keypath + split + 1);
    kpath.kmsurl = km_strndup(keypath, split);

    return kpath;
}

typedef enum {
    SEL_MK = 0,
    ENC_KEY,
    DEC_KEY
} UrlType;

static char *his_kms_get_url(HisKmsMgr *kms, const char *kmsurl, UrlType type)
{
    char *url;
    errno_t rc;

    if (kmsurl == NULL) {
        return NULL;
    }

    url = kms->url;
    switch (type) {
        case SEL_MK:
            rc = sprintf_s(url, KMS_URL_BUF_SZ, "%s/describe-key", kmsurl);
            break;
        case ENC_KEY:
            rc = sprintf_s(url, KMS_URL_BUF_SZ, "%s/encrypt-datakey", kmsurl);
            break;
        case DEC_KEY:
            rc = sprintf_s(url, KMS_URL_BUF_SZ, "%s/decrypt-datakey", kmsurl);
            break;
        default:
            break;
    }
    km_securec_check_ss(rc, "", "");
    return url;
}

static char *his_kms_mk_sel_reqbody(HisKmsMgr *kms, const char *keyid)
{
    const char *temp;
    
    temp = JS_TO_STR((
        {
            "data": {
                "attributes": {
                "key_id":"{key_id}"
                }
            }
        }
    ));

    ReplaceRule rules[] = {
        {"{key_id}", keyid}
    };

    km_safe_free(kms->reqbody);
    kms->reqbody = json_replace(temp, rules, ARR_LEN(rules));
    
    return kms->reqbody;
}

#define LEN_BUF_SZ 12
static char *his_kms_mk_enc_reqbody(HisKmsMgr *kms, const char *keyid, KmUnStr plain)
{
    const char *temp;
    KmStr hex;
    char hexlen[LEN_BUF_SZ];
    errno_t rc;

    temp = JS_TO_STR((
        {
            "data": {
                "attributes": {
                "key_id":"{key_id}",
                "plain_text": "{plain_encode}",
                "datakey_plain_length": "{plain_encode_len}"
                }
            }
        }
    ));

    hex = km_hex_encode(plain);
    if (hex.val == NULL) {
        km_err_msg(kms->err, "when access '%s', failed to encode cipher", kms->url);
        return NULL;
    }
    
    rc = sprintf_s(hexlen, LEN_BUF_SZ, "%lu", hex.len);
    km_securec_check_ss(rc, "", "");

    ReplaceRule rules[] = {
        {"{key_id}", keyid},
        {"{plain_encode}", hex.val},
        {"{plain_encode_len}", hexlen}
    };

    km_safe_free(kms->reqbody);
    kms->reqbody = json_replace(temp, rules, ARR_LEN(rules));
    km_safe_free(hex.val);
    
    return kms->reqbody;
}

static char *his_kms_mk_dec_reqbody(HisKmsMgr *kms, const char *keyid, KmUnStr cipher)
{
    const char *temp;
    char hexlen[LEN_BUF_SZ];
    errno_t rc;
    
    temp = JS_TO_STR((
        {
            "data": {
                "attributes": {
                "key_id":"{key_id}",
                "cipher_text": "{cipher_encode}",
                "datakey_plain_length": "{cipher_encode_len}"
                }
            }
        }
    ));

    KmStr hex = km_hex_encode(cipher);
    if (hex.val == NULL) {
        km_err_msg(kms->err, "when access '%s', failed to encode cipher", kms->url);
        return NULL;
    }

    rc = sprintf_s(hexlen, LEN_BUF_SZ, "%lu", hex.len);
    km_securec_check_ss(rc, "", "");

    ReplaceRule rules[] = {
        {"{key_id}", keyid},
        {"{cipher_encode}", hex.val},
        {"{cipher_encode_len}", hexlen},
    };

    km_safe_free(kms->reqbody);
    kms->reqbody = json_replace(temp, rules, ARR_LEN(rules));
    km_safe_free(hex.val);

    return kms->reqbody;
}

/*
 * do  - handle kms server error
 * out - return : 1 (no error), 0 (http 300+ error), -1 (token expired)
 */
static int his_kms_handle_err(HisKmsMgr *kms)
{
    HttpMgr *http;
    char *resbody;
    long state;
    char *kmserr;

    http = kms->httpmgr;

    state = httpmgr_get_status(http);
    /* if succeed, just return */
    if (httpmgr_get_status(http) < HTTP_MIN_UNEXCEPTED_CODE) {
        return 1;
    }

    resbody = httpmgr_get_res_body(http);
    kmserr = json_find(resbody, "code");
    if (kmserr != NULL && strcmp(kmserr, "KMS.3001") == 0) {
        km_safe_free(kmserr);
        return -1;
    }
    km_safe_free(kmserr);

    km_err_msg(kms->err, "failed to access '%s', http status: %ld, error message: %s", kms->url, state, resbody);
    return 0;
}

char *his_kms_mk_select(HisKmsMgr *kms, const char *keypath)
{
    HttpMgr *http;
    char *resbody;
    char *keystate;
    int ret;

    HisKmsKeyPath kpath = his_kms_parser_key_path(kms, keypath);
    if (km_err_catch(kms->err)) {
        return NULL;
    }

    http = kms->httpmgr;
#ifdef ENABLE_KM_DEBUG
    httpmgr_set_output(http, "./kms.out");
#endif
    httpmgr_set_req_line(http, his_kms_get_url(kms, kpath.kmsurl, SEL_MK), CURLOPT_HTTPPOST, kms->cacert);
    httpmgr_set_req_header(http, "Content-Type:application/json");
    httpmgr_set_req_header(http, his_iam_get_token(kms->iam));
    httpmgr_set_req_body(http, his_kms_mk_sel_reqbody(kms, kpath.keyid));
    char remain[3] = {0, 0, 1};
    httpmgr_set_response(http, remain, NULL);

    httpmgr_receive(http);
    km_safe_free(kpath.kmsurl);
    km_safe_free(kpath.keyid);
    /* handle client or net error */
    if (km_err_catch(kms->err)) {
        return NULL;
    }
    /* handle server error */
    ret = his_kms_handle_err(kms);
    if (ret == -1) {
        /*
         * -1 means: token expired. we nee refresh the token and try again.
         * we can make sure this function only be called once
         */
        his_iam_refresh_token(kms->iam);
        return his_kms_mk_select(kms, keypath);
    } else if (ret == 0) {
        return NULL;
    }

    resbody = httpmgr_get_res_body(http);
    keystate = json_find(resbody, "key_state");
    if (keystate == NULL) {
        km_err_msg(kms->err, "when access '%s', failed find 'key_state' from http response body", kms->url);
        return NULL;
    }

    return keystate;
}

KmUnStr his_kms_mk_encrypt(HisKmsMgr *kms, const char *keypath, KmUnStr plain)
{
    HttpMgr *http;
    char *resbody;
    KmStr hex = {0};
    KmUnStr cipher = {0};
    int ret;
    char *len;

    if (plain.val == NULL) {
        return cipher;
    }

    HisKmsKeyPath kpath = his_kms_parser_key_path(kms, keypath);
    if (km_err_catch(kms->err)) {
        return cipher;
    }

    http = kms->httpmgr;
#ifdef ENABLE_KM_DEBUG
    httpmgr_set_output(http, "./kms.out");
#endif
    httpmgr_set_req_line(http, his_kms_get_url(kms, kpath.kmsurl, ENC_KEY), CURLOPT_HTTPPOST, kms->cacert);
    httpmgr_set_req_header(http, "Content-Type:application/json");
    httpmgr_set_req_header(http, his_iam_get_token(kms->iam));
    httpmgr_set_req_body(http, his_kms_mk_enc_reqbody(kms, kpath.keyid, plain));

    char remain[3] = {0, 0, 1};
    httpmgr_set_response(http, remain, NULL);

    httpmgr_receive(http);
    km_safe_free(kpath.kmsurl);
    km_safe_free(kpath.keyid);
    /* check client error */
    if (km_err_catch(kms->err)) {
        return cipher;
    }

    /* check server error */
    ret = his_kms_handle_err(kms);
    if (ret == -1) {
        return his_kms_mk_encrypt(kms, keypath, plain);
    } else if (ret == 0) {
        return cipher;
    }

    resbody = httpmgr_get_res_body(http);

    hex.val = json_find(resbody, "cipher_text");
    if (hex.val == NULL) {
        km_err_msg(kms->err, "failed to find 'cipher_text' filed from http response body of '%s'.", kms->url);
        return cipher;
    }

    len = json_find(resbody, "datakey_length");
    if (len == NULL) {
        km_free(hex.val);
        km_err_msg(kms->err, "failed to find 'datakey_length' filed from http response body of '%s'.", kms->url);
        return cipher;
    }
    hex.len = atoi(len);
    cipher = km_hex_decode(hex);
    km_free(hex.val);
    km_free(len);
    if (cipher.val == NULL) {
        km_err_msg(kms->err, "failed to decode hex cipher in http response body of '%s'.", kms->url);
    }

    return cipher;
}

KmUnStr his_kms_mk_decrypt(HisKmsMgr *kms, const char *keypath, KmUnStr cipher)
{
    HttpMgr *http;
    char *resbody;
    KmStr hex;
    KmUnStr plain = {0};
    int ret;
    char *len;

    if (cipher.val == NULL) {
        return plain;
    }

    HisKmsKeyPath kpath = his_kms_parser_key_path(kms, keypath);
    if (km_err_catch(kms->err)) {
        return plain;
    }

    http = kms->httpmgr;
#ifdef ENABLE_KM_DEBUG
    httpmgr_set_output(http, "./kms.out");
#endif
    httpmgr_set_req_line(http, his_kms_get_url(kms, kpath.kmsurl, DEC_KEY), CURLOPT_HTTPPOST, kms->cacert);
    httpmgr_set_req_header(http, "Content-Type:application/json");
    httpmgr_set_req_header(http, his_iam_get_token(kms->iam));
    httpmgr_set_req_body(http, his_kms_mk_dec_reqbody(kms, kpath.keyid, cipher));

    char remain[3] = {0, 0, 1};
    httpmgr_set_response(http, remain, NULL);

    httpmgr_receive(http);
    km_safe_free(kpath.kmsurl);
    km_safe_free(kpath.keyid);
    /* check client error */
    if (km_err_catch(kms->err)) {
        return plain;
    }

    /* check server error */
    ret = his_kms_handle_err(kms);
    if (ret == -1) {
        return his_kms_mk_decrypt(kms, keypath, cipher);
    } else if (ret == 0) {
        return plain;
    }

    resbody = httpmgr_get_res_body(http);
    hex.val = json_find(resbody, "data_key");
    if (hex.val == NULL) {
        km_err_msg(kms->err, "failed to find 'data_key' filed from http resonpse of '%s'.", kms->url);
        return plain;
    }

    len = json_find(resbody, "datakey_length");
    if (len == NULL) {
        km_free(hex.val);
        km_err_msg(kms->err, "failed to find 'datakey_length' filed from http response body of '%s'.", kms->url);
        return plain;
    }
    hex.len = atoi(len);

    plain = km_hex_decode(hex);
    km_free(hex.val);
    km_free(len);
    if (plain.val == NULL) {
        km_err_msg(kms->err, "failed to decode hex plain in http response body of '%s'.", kms->url);
    }

    return plain;
}

KmUnStr his_kms_dk_create(HisKmsMgr *kms, const char *keypath, KmUnStr *cipher)
{
#define HIS_DK_DEFAULT_LEN 16

    KmUnStr plain = {0};
    KmUnStr _cipher = {0};

    if (cipher == NULL) {
        return plain;
    }

    plain.val = (unsigned char *)km_alloc(HIS_DK_DEFAULT_LEN);
    if (plain.val == NULL) {
        km_err_msg(kms->err, "failed to alloc memory.");
        return plain;
    }
    
    if (RAND_bytes(plain.val, HIS_DK_DEFAULT_LEN) != 1) {
        km_err_msg(kms->err, "his kms failed to create data key.");
    }
    plain.len = HIS_DK_DEFAULT_LEN;

    _cipher = his_kms_mk_encrypt(kms, keypath, plain);
    cipher->val = _cipher.val;
    cipher->len = _cipher.len;

    return plain;
}