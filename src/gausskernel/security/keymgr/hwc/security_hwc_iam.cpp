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
 * security_hwc_iam.cpp
 *
 * IDENTIFICATION
 *	  src/gausskernel/security/keymgr/src/hwc/security_hwc_iam.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "keymgr/hwc/security_hwc_iam.h"
#include <stdlib.h>
#include <string.h>
#include "keymgr/comm/security_http.h"
#include "keymgr/comm/security_utils.h"
#include "keymgr/comm/security_json.h"

HwcIamMgr *hwc_iam_new(KmErr *errbuf)
{
    HwcIamMgr *iam;

    iam = (HwcIamMgr *)km_alloc_zero(sizeof(HwcIamMgr));
    if (iam == NULL) {
        return NULL;
    }

    iam->err = errbuf;

    iam->httpmgr = httpmgr_new(errbuf);
    if (iam->httpmgr == NULL) {
        km_free(iam);
        return NULL;
    }

    return iam;
}

void hwc_iam_free(HwcIamMgr *iam)
{
    if (iam == NULL) {
        return;
    }
    
    km_safe_free(iam->username);
    km_safe_free(iam->passwd);
    km_safe_free(iam->domain);
    km_safe_free(iam->project);

    km_safe_free(iam->url);

    httpmgr_free(iam->httpmgr);
    km_safe_free(iam->cacert);

    km_safe_free(iam->reqbody);
    km_safe_free(iam->token);

    km_free(iam);
}

/*
 * set iam authentication info, and cache them.
 * when the token expired, we will use cached info to refresh token.
 */
void hwc_iam_set_arg(HwcIamMgr *iam, const char *key, const char *value)
{
    if (key == NULL || value == NULL) {
        return;
    }
    
    /*
     * if we have cached the http reqbody and token.
     * when we change any parameters, we need to clear the cache at the same time.
     */
    km_safe_free(iam->reqbody);
    km_safe_free(iam->token);

    if (strcasecmp(key, "iamuser") == 0) {
        km_safe_free(iam->username); /* free the old one if need */
        iam->username = km_strdup(value);
    } else if (strcasecmp(key, "iampassword") == 0) {
        km_safe_free(iam->passwd);
        iam->passwd = km_strdup(value);
    } else if (strcasecmp(key, "iamdomain") == 0) {
        km_safe_free(iam->domain);
        iam->domain = km_strdup(value);
    } else if (strcasecmp(key, "iamurl") == 0) {
        km_safe_free(iam->url);
        iam->url = km_strdup(value);
    } else if (strcasecmp(key, "kmsproject") == 0) {
        km_safe_free(iam->project);
        iam->project = km_strdup(value);
    } else if (strcasecmp(key, "iamcacert") == 0) {
        km_safe_free(iam->cacert);
        iam->cacert = km_realpath(value, iam->err);
    } /* ignore unknowned para */
}

static void hwc_iam_has_set_auth_info(HwcIamMgr *iam)
{
    if (iam->username == NULL) {
        km_err_msg(iam->err, "failed to access huawei cloud iam service, plase set parameter 'iamUser'");
        return;
    }
    if (iam->passwd == NULL) {
        km_err_msg(iam->err, "failed to access huawei cloud iam service, plase set parameter 'iamPassword'");
        return;
    }
    if (iam->domain == NULL) {
        km_err_msg(iam->err, "failed to access huawei cloud iam service, plase set parameter 'iamDomain'");
        return;
    }
    if (iam->project == NULL) {
        km_err_msg(iam->err, "failed to access huawei cloud iam service, plase set parameter 'kmsProject'");
        return;
    }
}

static char *hwc_iam_get_url(HwcIamMgr *iam)
{
#define HWC_IAM_URL_LEN 256
    if (iam->url == NULL) {
        km_err_msg(iam->err, "failed to access huawei cloud iam service, plase set parameter 'iamUrl'");
        return NULL;
    }

    if (strlen(iam->url) > HWC_IAM_URL_LEN) {
        km_err_msg(iam->err, "failed to access huawei cloud iam service, the length of 'iamUrl' exceeds 256");
        return NULL;
    }

    return iam->url;
}

static char *hwc_iam_get_req_body(HwcIamMgr *iam)
{
    hwc_iam_has_set_auth_info(iam);
    if (km_err_catch(iam->err)) {
        return NULL;
    }
    
    const char *reqtemp = JS_TO_STR((
        {
            "auth": {
                "identity": {
                    "methods": ["password"],
                    "password": {
                        "user": {
                            "name": "{username}",
                            "password": "{passwd}",
                            "domain": {
                                "name": "{domain}"
                            }
                        }
                    }
                },
                "scope": {
                    "project": {
                        "name": "{projname}"
                    }
                }
            }
        }
    ));

    ReplaceRule rules[] = {
        {"{username}", iam->username},
        {"{passwd}", iam->passwd},
        {"{projname}", iam->project},
        {"{domain}", iam->domain}
    };
    km_safe_free(iam->reqbody);
    iam->reqbody = json_replace(reqtemp, rules, ARR_LEN(rules));
    if (iam->reqbody == NULL) {
        km_err_msg(iam->err, "when create iam request body, failed to parse json template");
    }

    return iam->reqbody;
}

static char *hwc_iam_parse_token(HwcIamMgr *iam)
{
#define MAX_TOKEN_LEN 8192
    char *resbody;
    char *token;
    long state;
    errno_t rc;
    size_t tkhdrlen;

    resbody = httpmgr_get_res_body(iam->httpmgr);
    state = httpmgr_get_status(iam->httpmgr);
    if (state >= HTTP_MIN_UNEXCEPTED_CODE) {
        km_err_msg(iam->err, "failed to access '%s', http status: %ld, error message: %s", iam->url, state, resbody);
        return NULL;
    }

    token = httpmgr_get_res_header(iam->httpmgr);
    if (token == NULL) {
        km_err_msg(iam->err, "when access '%s', failed find 'access_token' from http response header", iam->url);
        return NULL;
    }

    /* we cache token until it gets expired */
    tkhdrlen = strlen("X-Auth-Token:%s ") + strlen(token) + 1;
    if (tkhdrlen > MAX_TOKEN_LEN) {
        km_err_msg(iam->err, "invalid token header len: %lu", tkhdrlen);
        return NULL;
    }

    iam->token = (char *)km_alloc(tkhdrlen);
    if (iam->token == NULL) {
        km_err_msg(iam->err, "malloc memory error");
        return NULL;
    }

    rc = sprintf_s(iam->token, tkhdrlen, "X-Auth-Token:%s ", token + strlen("X-Subject-Token: "));
    km_securec_check_ss(rc, "", "");

    return iam->token;
}

char *hwc_iam_get_token(HwcIamMgr *iam)
{
    HttpMgr *http;
    char *reqbody;

    /* if we have cache token, just return it. */
    if (iam->token != NULL) {
        return iam->token;
    }

    http = iam->httpmgr;
#ifdef ENABLE_KM_DEBUG
    httpmgr_set_output(http, "./iam.out");
#endif
    httpmgr_set_req_line(http, hwc_iam_get_url(iam), CURLOPT_HTTPPOST, iam->cacert);
    httpmgr_set_req_header(http, "Content-Type:application/json");
    reqbody = hwc_iam_get_req_body(iam);
    httpmgr_set_req_header(http, httpmgr_get_req_body_len(http, reqbody));
    httpmgr_set_req_body(http, reqbody);
    char remain[3] = {0, 1, 1};
    httpmgr_set_response(http, remain, "X-Subject-Token");

    httpmgr_receive(http);
    /* check client error here */
    if (km_err_catch(iam->err)) {
        return NULL;
    }

    /* check server error here, such as http 400, http 500 */
    return hwc_iam_parse_token(iam);
}

char *hwc_iam_refresh_token(HwcIamMgr *iam)
{
    km_safe_free(iam->token);

    return hwc_iam_get_token(iam);
}