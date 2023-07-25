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
 * security_hwc_kms.cpp
 *
 * IDENTIFICATION
 *	  src/gausskernel/security/keymgr/src/hwc/security_hwc_kms.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "keymgr/hwc/security_hwc_kms.h"
#include <string.h>
#include "keymgr/comm/security_http.h"
#include "keymgr/comm/security_utils.h"
#include "keymgr/comm/security_json.h"
#include "keymgr/comm/security_encode.h"
#include "keymgr/hwc/security_hwc_iam.h"

typedef struct {
    char *kmsurl;
    char *keyid;
} HwcKmsKeyPath;

HwcKmsMgr *hwc_kms_new(HwcIamMgr *iam, KmErr *errbuf)
{
    HwcKmsMgr *kms;

    if (iam == NULL) {
        return NULL;
    }

    kms = (HwcKmsMgr *)km_alloc_zero(sizeof(HwcKmsMgr));
    if (kms == NULL) {
        return NULL;
    }
    kms->err = errbuf;
    kms->aksk = false;
    kms->iam = iam;
    kms->httpmgr = httpmgr_new(errbuf);
    if (kms->httpmgr == NULL) {
        km_free(kms);
        return NULL;
    }

    return kms;
}

void hwc_kms_free(HwcKmsMgr *kms)
{
    if (kms == NULL) {
        return;
    }
    km_safe_free(kms->reqbody);

    httpmgr_free(kms->httpmgr);
    km_safe_free(kms->cacert);

    km_free(kms);
}

void hwc_kms_set_arg(HwcKmsMgr *kms, const char *key, const char *value)
{
#define HWC_PROJ_ID_MAX_LEN 96
    errno_t rc;

    if (key == NULL || value == NULL) {
        return;
    }

    if (strcasecmp(key, "kmscacert") == 0) {
        km_safe_free(kms->cacert); /* free the old one if need */
        kms->cacert = km_realpath(value, kms->err);
    } else if (strcasecmp(key, "kmsprojectid") == 0) {
        if (strlen(value) > HWC_PROJ_ID_MAX_LEN) {
            km_err_msg(kms->err, "the length of 'kmsProjectId' is too long, the maximum value is 96.");
            return;
        }
        rc = sprintf_s(kms->projid, HWC_PROJ_ID_HDR_SZ, "x-project-id: %s", value);
        km_securec_check_ss(rc, "", "");
    } else if (strcasecmp(key, "ak") == 0) {
        httpmgr_ak_set(kms->httpmgr, value);
        kms->aksk = true;
    } else if (strcasecmp(key, "sk") == 0) {
        httpmgr_sk_set(kms->httpmgr, value);
        kms->aksk = true;
    } /* ignore unknowned para */
}

/* the format of keypath is 'URL/ID', we need return 'KEYID' */
static HwcKmsKeyPath hwc_kms_parser_key_path(HwcKmsMgr *kms, const char *keypath)
{
#define HWC_KMS_URL_LEN 1024
    size_t split = 0;
    HwcKmsKeyPath kpath = {0};
    size_t i;

    if (keypath == NULL || strlen(keypath) <= 1) {
        km_err_msg(kms->err, "huawei kms failed to get keypath filed.");
        return kpath;
    }

    if (strlen(keypath) > HWC_KMS_URL_LEN) {
        km_err_msg(kms->err, "failed to access huawei kms, the length of keypath exceeds 1024.");
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
    DEC_KEY,
    CRT_KEY
} UrlType;

static char *hwc_kms_get_url(HwcKmsMgr *kms, const char *kmsurl, UrlType type)
{
    char *url;
    errno_t rc;

    if (kmsurl == NULL) {
        return NULL;
    }

    url = kms->url;
    switch (type) {
        case SEL_MK:
            rc = sprintf_s(url, HWC_KMS_URL_SZ, "%s/describe-key", kmsurl);
            break;
        case ENC_KEY:
            rc = sprintf_s(url, HWC_KMS_URL_SZ, "%s/encrypt-datakey", kmsurl);
            break;
        case DEC_KEY:
            rc = sprintf_s(url, HWC_KMS_URL_SZ, "%s/decrypt-datakey", kmsurl);
            break;
        case CRT_KEY:
            rc = sprintf_s(url, HWC_KMS_URL_SZ, "%s/create-datakey", kmsurl);
            break;
        default:
            break;
    }
    km_securec_check_ss(rc, "", "");

    if (kms->aksk && strlen(kms->projid) == 0) {
        km_err_msg(kms->err, "can't access '%s', please set arg 'kmsProjectId'.", url);
    }
    return url;
}

static char *hwc_kms_mk_sel_reqbody(HwcKmsMgr *kms, const char *keyid)
{
    const char *temp;
    
    temp = JS_TO_STR((
        {
            "key_id": "{key_id}"
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
static char *hwc_kms_mk_enc_reqbody(HwcKmsMgr *kms, const char *keyid, KmUnStr plain)
{
    const char *temp;
    KmStr sha;
    char shalen[LEN_BUF_SZ];
    errno_t rc;

    temp = JS_TO_STR((
        {
            "key_id": "{key_id}",
            "plain_text":"{plain_encode}",
            "datakey_plain_length": "{plain_encode_len}"
        }
    ));

    sha = km_sha_encode(plain);
    if (sha.val == NULL) {
        km_err_msg(kms->err, "when access '%s', failed to encode cipher", kms->url);
        return NULL;
    }
    
    rc = sprintf_s(shalen, LEN_BUF_SZ, "%lu", plain.len);
    km_securec_check_ss(rc, "", "");

    ReplaceRule rules[] = {
        {"{key_id}", keyid},
        {"{plain_encode}", sha.val},
        {"{plain_encode_len}", shalen}
    };

    km_safe_free(kms->reqbody);
    kms->reqbody = json_replace(temp, rules, ARR_LEN(rules));
    km_free(sha.val);
    
    return kms->reqbody;
}

static char *hwc_kms_mk_dec_reqbody(HwcKmsMgr *kms, const char *keyid, KmUnStr cipher)
{
#define PLAIN_LEN 92
    const char *temp;
    char plainlen[LEN_BUF_SZ];
    errno_t rc;

    temp = JS_TO_STR((
        {
            "key_id": "{key_id}",
            "cipher_text":"{cipher_encode}",
            "datakey_cipher_length": "{cipher_encode_len}"
        }
    ));

    KmStr sha = km_sha_encode(cipher);
    if (sha.val == NULL) {
        km_err_msg(kms->err, "when access '%s', failed to encode cipher", kms->url);
        return NULL;
    }

    if (cipher.len <= PLAIN_LEN) {
        km_err_msg(kms->err, "when access '%s', the length of cipher is too short: '%lu'.", kms->url, cipher.len);
        return NULL;
    }

    rc = sprintf_s(plainlen, LEN_BUF_SZ, "%lu", cipher.len - PLAIN_LEN);
    km_securec_check_ss(rc, "", "");

    ReplaceRule rules[] = {
        {"{key_id}", keyid},
        {"{cipher_encode}", sha.val},
        {"{cipher_encode_len}", plainlen},
    };

    km_safe_free(kms->reqbody);
    kms->reqbody = json_replace(temp, rules, ARR_LEN(rules));
    km_free(sha.val);

    return kms->reqbody;
}

static char *hwc_kms_dk_crt_reqbody(HwcKmsMgr *kms, const char *keyid)
{
    const char *temp;

    temp = JS_TO_STR((
        {
            "datakey_length": "128",
            "key_id": "{key_id}"
        }
    ));

    ReplaceRule rules[] = {
        {"{key_id}", keyid}
    };

    km_safe_free(kms->reqbody);
    kms->reqbody = json_replace(temp, rules, ARR_LEN(rules));
    return kms->reqbody;
}

/*
 * do  - handle kms server error
 * out - return : 1 (no error), 0 (http 300+ error), -1 (token expired)
 */
static int hwc_kms_handle_err(HwcKmsMgr *kms)
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
    kmserr = json_find(resbody, "error_code");
    if (kmserr != NULL && strcmp(kmserr, "KMS.0303") == 0) {
        km_safe_free(kmserr);
        return -1;
    }
    km_safe_free(kmserr);

    km_err_msg(kms->err, "failed to access '%s', http status: %ld, error message: %s", kms->url, state, resbody);
    return 0;
}

static char *hwc_kms_key_state(char *keystate)
{
    if (keystate == NULL) {
        return NULL;
    }

    /*
     * key state:
     *      1: to be active
     *      2: active
     *      3: disable
     *      4: to be deleted
     *      5: to be improved
     * do not use atoi() here, we get the 'keystate' from http message, the 'keystate' could be anything
     */
    if (strcmp(keystate, "1") == 0) {
        return km_strdup("to be active");
    } else if (strcmp(keystate, "2") == 0) {
        return km_strdup("active");
    } else if (strcmp(keystate, "3") == 0) {
        return km_strdup("disable");
    } else if (strcmp(keystate, "4") == 0) {
        return km_strdup("to be active");
    } else if (strcmp(keystate, "5") == 0) {
        return km_strdup("to be improved");
    } else {
        return km_strdup(keystate);
    }
}

char *hwc_kms_mk_select(HwcKmsMgr *kms, const char *keypath)
{
    HttpMgr *http;
    char *reqbody;
    char *resbody;
    char *keystate;
    char *state;
    int ret;

    HwcKmsKeyPath kpath = hwc_kms_parser_key_path(kms, keypath);
    if (km_err_catch(kms->err)) {
        return NULL;
    }

    http = kms->httpmgr;
#ifdef ENABLE_KM_DEBUG
    httpmgr_set_output(http, "./kms.out");
#endif
    httpmgr_set_req_line(http, hwc_kms_get_url(kms, kpath.kmsurl, SEL_MK), CURLOPT_HTTPPOST, kms->cacert);
    httpmgr_set_req_header(http, "Content-Type:application/json");
    kms->aksk ? httpmgr_set_req_header(http, kms->projid) : httpmgr_set_req_header(http, hwc_iam_get_token(kms->iam));
    reqbody = hwc_kms_mk_sel_reqbody(kms, kpath.keyid);
    httpmgr_set_req_header(http, httpmgr_get_req_body_len(http, reqbody));
    httpmgr_set_req_body(http, reqbody);
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
    ret = hwc_kms_handle_err(kms);
    if (ret == -1) {
        /*
         * -1 means: token expired. we nee refresh the token and try again.
         * we can make sure thwc function only be called once
         */
        hwc_iam_refresh_token(kms->iam);
        return hwc_kms_mk_select(kms, keypath);
    } else if (ret == 0) {
        return NULL;
    }

    resbody = httpmgr_get_res_body(http);
    keystate = json_find(resbody, "key_state");
    if (keystate == NULL) {
        km_err_msg(kms->err, "when access '%s', failed find 'key_state' from http response body", kms->url);
        return NULL;
    }

    state = hwc_kms_key_state(keystate);
    km_free(keystate);

    return state;
}

KmUnStr hwc_kms_mk_encrypt(HwcKmsMgr *kms, const char *keypath, KmUnStr plain)
{
    HttpMgr *http;
    char *reqbody;
    char *resbody;
    KmStr sha = {0};
    KmUnStr cipher = {0};
    int ret;

    if (plain.val == NULL) {
        return cipher;
    }

    HwcKmsKeyPath kpath = hwc_kms_parser_key_path(kms, keypath);
    if (km_err_catch(kms->err)) {
        return cipher;
    }

    http = kms->httpmgr;
#ifdef ENABLE_KM_DEBUG
    httpmgr_set_output(http, "./kms.out");
#endif
    httpmgr_set_req_line(http, hwc_kms_get_url(kms, kpath.kmsurl, ENC_KEY), CURLOPT_HTTPPOST, kms->cacert);
    reqbody = hwc_kms_mk_enc_reqbody(kms, kpath.keyid, plain);
    httpmgr_set_req_header(http, httpmgr_get_req_body_len(http, reqbody));
    httpmgr_set_req_header(http, "Content-Type:application/json");
    kms->aksk ? httpmgr_set_req_header(http, kms->projid) : httpmgr_set_req_header(http, hwc_iam_get_token(kms->iam));
    httpmgr_set_req_body(http, reqbody);

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
    ret = hwc_kms_handle_err(kms);
    if (ret == -1) {
        return hwc_kms_mk_encrypt(kms, keypath, plain);
    } else if (ret == 0) {
        return cipher;
    }

    resbody = httpmgr_get_res_body(http);
    sha.val = json_find(resbody, "cipher_text");
    if (sha.val == NULL) {
        km_err_msg(kms->err, "failed to find 'cipher_text' filed from http response body of '%s'.", kms->url);
        return cipher;
    }
    sha.len = strlen(sha.val);
    cipher = km_sha_decode(sha);
    km_free(sha.val);
    if (cipher.val == NULL) {
        km_err_msg(kms->err, "failed to decode hex cipher in http response body of '%s'.", kms->url);
    }

    return cipher;
}

KmUnStr hwc_kms_mk_decrypt(HwcKmsMgr *kms, const char *keypath, KmUnStr cipher)
{
    HttpMgr *http;
    char *reqbody;
    char *resbody;
    KmStr sha;
    KmUnStr plain = {0};
    int ret;

    if (cipher.val == NULL) {
        return plain;
    }

    HwcKmsKeyPath kpath = hwc_kms_parser_key_path(kms, keypath);
    if (km_err_catch(kms->err)) {
        return plain;
    }

    http = kms->httpmgr;
#ifdef ENABLE_KM_DEBUG
    httpmgr_set_output(http, "./kms.out");
#endif
    httpmgr_set_req_line(http, hwc_kms_get_url(kms, kpath.kmsurl, DEC_KEY), CURLOPT_HTTPPOST, kms->cacert);
    reqbody = hwc_kms_mk_dec_reqbody(kms, kpath.keyid, cipher);
    httpmgr_set_req_header(http, httpmgr_get_req_body_len(http, reqbody));
    httpmgr_set_req_header(http, "Content-Type:application/json");
    kms->aksk ? httpmgr_set_req_header(http, kms->projid) : httpmgr_set_req_header(http, hwc_iam_get_token(kms->iam));
    httpmgr_set_req_body(http, reqbody);

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
    ret = hwc_kms_handle_err(kms);
    if (ret == -1) {
        return hwc_kms_mk_decrypt(kms, keypath, cipher);
    } else if (ret == 0) {
        return plain;
    }

    resbody = httpmgr_get_res_body(http);
    sha.val = json_find(resbody, "data_key");
    if (sha.val == NULL) {
        km_err_msg(kms->err, "failed to find 'data_key' filed from http resonpse of '%s'.", kms->url);
        return plain;
    }
    sha.len = strlen(sha.val);

    plain = km_hex_decode(sha);
    km_free(sha.val);
    if (plain.val == NULL) {
        km_err_msg(kms->err, "failed to decode hex plain in http response body of '%s'.", kms->url);
    }

    return plain;
}

KmUnStr hwc_kms_dk_create(HwcKmsMgr *kms, const char *keypath, KmUnStr *cipher)
{
    HttpMgr *http;
    char *reqbody;
    char *resbody;
    KmUnStr plain = {0};
    char *hexplain;
    KmStr sha;
    KmUnStr _cipher;
    int ret;

    HwcKmsKeyPath kpath = hwc_kms_parser_key_path(kms, keypath);
    if (km_err_catch(kms->err)) {
        return plain;
    }

    http = kms->httpmgr;
#ifdef ENABLE_KM_DEBUG
    httpmgr_set_output(http, "./kms.out");
#endif
    httpmgr_set_req_line(http, hwc_kms_get_url(kms, kpath.kmsurl, CRT_KEY), CURLOPT_HTTPPOST, kms->cacert);
    reqbody = hwc_kms_dk_crt_reqbody(kms, kpath.keyid);
    httpmgr_set_req_header(http, httpmgr_get_req_body_len(http, reqbody));
    httpmgr_set_req_header(http, "Content-Type:application/json");
    if (!kms->aksk) {
        httpmgr_set_req_header(http, hwc_iam_get_token(kms->iam));
    }
    httpmgr_set_req_body(http, reqbody);

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
    ret = hwc_kms_handle_err(kms);
    if (ret == -1) {
        return hwc_kms_dk_create(kms, keypath, cipher);
    } else if (ret == 0) {
        return plain;
    }

    resbody = httpmgr_get_res_body(http);
    hexplain = json_find(resbody, "plain_text");
    if (hexplain == NULL) {
        km_err_msg(kms->err, "failed to find 'plain_text' filed from http response body of '%s'.", kms->url);
    }

    sha.val = json_find(resbody, "cipher_text");
    if (sha.val == NULL) {
        km_safe_free(hexplain);
        km_err_msg(kms->err, "failed to find 'cipher_text' filed from http response body of '%s'.", kms->url);
        return plain;
    }
    sha.len = strlen(sha.val);

    plain = km_hex_decode({hexplain, strlen(hexplain)});
    _cipher = km_sha_decode(sha);
    km_free(hexplain);
    km_free(sha.val);
    if (plain.val == NULL || _cipher.val == NULL) {
        km_safe_free(plain.val);
        km_safe_free(_cipher.val);
        km_err_msg(kms->err, "failed to decode hex data key plain and cipher in http response body of '%s'.", kms->url);
    }

    cipher->val = _cipher.val;
    cipher->len = _cipher.len;

    return plain;
}