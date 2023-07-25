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
 * security_his_iam.cpp
 *
 * IDENTIFICATION
 *	  src/gausskernel/security/keymgr/src/his/security_his_iam.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "keymgr/his/security_his_iam.h"

#include <stdlib.h>
#include <string.h>

#include "keymgr/comm/security_http.h"
#include "keymgr/comm/security_utils.h"
#include "keymgr/comm/security_json.h"

HisIamMgr *his_iam_new(KmErr *errbuf)
{
    HisIamMgr *iam;

    iam = (HisIamMgr *)km_alloc_zero(sizeof(HisIamMgr));
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

void his_iam_free(HisIamMgr *iam)
{
    errno_t rc;

    if (iam == NULL) {
        return;
    }

    km_safe_free(iam->account);
    if (iam->secret != NULL) {
        rc = memset_s(iam->secret, strlen(iam->secret), 0, strlen(iam->secret));
        km_securec_check(rc, "", "");
        km_free(iam->secret);
    }
    km_safe_free(iam->appid);
    km_safe_free(iam->enterprise);

    km_safe_free(iam->url);

    httpmgr_free(iam->httpmgr);
    km_safe_free(iam->cacert);

    km_safe_free(iam->reqbody);
    if (iam->token != NULL) {
        rc = memset_s(iam->token, strlen(iam->token), 0, strlen(iam->token));
        km_securec_check(rc, "", "");
        km_free(iam->token);
    }

    km_free(iam);
}

/*
 * set iam authentication info, and cache them.
 * when the token expired, we will use cached info to refresh token.
 */
void his_iam_set_arg(HisIamMgr *iam, const char *key, const char *value)
{
    errno_t rc = 0;

    if (key == NULL || value == NULL) {
        return;
    }

    /*
     * if we have cached the http reqbody and token.
     * when we change any parameters, we need to clear the cache at the same time.
     */
    km_safe_free(iam->reqbody);
    if (iam->token != NULL) {
        rc = memset_s(iam->token, strlen(iam->token), 0, strlen(iam->token));
        km_securec_check(rc, "", "");
        km_free(iam->token);
        iam->token = NULL;
    }

    if (strcasecmp(key, "hisaccount") == 0) {
        km_safe_free(iam->account); /* free the old one if need */
        iam->account = km_strdup(value);
    } else if (strcasecmp(key, "hissecret") == 0) {
        if (iam->secret != NULL) {
            rc = memset_s(iam->secret, strlen(iam->secret), 0, strlen(iam->secret));
            km_securec_check(rc, "", "");
            km_free(iam->secret);
        }
        iam->secret = km_strdup(value);
    } else if (strcasecmp(key, "hisappid") == 0) {
        km_safe_free(iam->appid);
        iam->appid = km_strdup(value);
    } else if (strcasecmp(key, "hisenterprise") == 0) {
        km_safe_free(iam->enterprise);
        iam->enterprise = km_strdup(value);
    } else if (strcasecmp(key, "hisiamurl") == 0) {
        km_safe_free(iam->url);
        iam->url = km_strdup(value);
    } /* ignore unknowned para */
}

static void his_iam_has_set_auth_info(HisIamMgr *iam)
{
    if (iam->account == NULL) {
        km_err_msg(iam->err, "failed to access his iam service, plase set parameter 'hisAccount'");
        return;
    }
    if (iam->secret == NULL) {
        km_err_msg(iam->err, "failed to access his iam service, plase set parameter 'hisSecret'");
        return;
    }
    if (iam->appid == NULL) {
        km_err_msg(iam->err, "failed to access his iam service, plase set parameter 'hisAppid'");
        return;
    }
    if (iam->enterprise == NULL) {
        km_err_msg(iam->err, "failed to access his iam service, plase set parameter 'hisEnterprise'");
        return;
    }
}

static char *his_iam_get_url(HisIamMgr *iam)
{
#define HIS_IAM_URL_LEN 256
    if (iam->url == NULL) {
        km_err_msg(iam->err, "failed to access his iam service, plase set parameter 'hisIamUrl'");
        return NULL;
    }

    if (strlen(iam->url) > HIS_IAM_URL_LEN) {
        km_err_msg(iam->err, "failed to access his iam service, the length of 'iamUrl' exceeds 256");
        return NULL;
    }

    return iam->url;
}

static char *his_iam_get_reqbody(HisIamMgr *iam)
{
    his_iam_has_set_auth_info(iam);
    if (km_err_catch(iam->err)) {
        return NULL;
    }

    const char *reqtemp = JS_TO_STR((
        {
            "data": {
                "type": "JWT-Token",
                "attributes": {
                    "method": "CREATE",
                    "account": "{account}",
                    "secret": "{secret}",
                    "project": "{appid}",
                    "enterprise": "{enterprise}"
                }
            }
        }
    ));

    ReplaceRule rules[] = {
        {"{account}", iam->account},
        {"{secret}", iam->secret},
        {"{appid}", iam->appid},
        {"{enterprise}", iam->enterprise}
    };
    km_safe_free(iam->reqbody);
    iam->reqbody = json_replace(reqtemp, rules, ARR_LEN(rules));
    if (iam->reqbody == NULL) {
        km_err_msg(iam->err, "when create iam request body, failed to parse json template");
    }

    return iam->reqbody;
}

static char *his_iam_parse_token(HisIamMgr *iam)
{
#define MAX_TOKEN_LEN 2048
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

    token = json_find(resbody, "access_token");
    if (token == NULL) {
        km_err_msg(iam->err, "when access '%s', failed find 'access_token' from http response body", iam->url);
        return NULL;
    }

    /* we cache token until it gets expired */
    tkhdrlen = strlen("Authorization: ") + strlen(token) + 1;
    if (tkhdrlen > MAX_TOKEN_LEN) {
        rc = memset_s(token, strlen(token), 0, strlen(token));
        km_securec_check(rc, "", "");
        km_free(token);

        km_err_msg(iam->err, "invalid token header len: %lu", tkhdrlen);
        return NULL;
    }

    iam->token = (char *)km_alloc(tkhdrlen);
    if (iam->token == NULL) {
        rc = memset_s(token, strlen(token), 0, strlen(token));
        km_securec_check(rc, "", "");
        km_free(token);

        km_err_msg(iam->err, "malloc memory error");
        return NULL;
    }

    rc = sprintf_s(iam->token, tkhdrlen, "Authorization: %s", token);
    km_securec_check_ss(rc, "", "");
    
    rc = memset_s(token, strlen(token), 0, strlen(token));
    km_securec_check(rc, "", "");
    km_free(token);

    return iam->token;
}

char *his_iam_get_token(HisIamMgr *iam)
{
    HttpMgr *http;

    /* if we have cache token, just return it. */
    if (iam->token != NULL) {
        return iam->token;
    }

    http = iam->httpmgr;
#ifdef ENABLE_KM_DEBUG
    httpmgr_set_output(http, "./iam.out");
#endif
    httpmgr_set_req_line(http, his_iam_get_url(iam), CURLOPT_HTTPPOST, iam->cacert);
    httpmgr_set_req_header(http, "Content-Type:application/json");
    httpmgr_set_req_body(http, his_iam_get_reqbody(iam));
    char remain[3] = {0, 0, 1};
    httpmgr_set_response(http, remain, NULL);

    httpmgr_receive(http);
    /* check client error here */
    if (km_err_catch(iam->err)) {
        return NULL;
    }

    /* check server error here, such as http 400, http 500 */
    return his_iam_parse_token(iam);
}

char *his_iam_refresh_token(HisIamMgr *iam)
{
    errno_t rc;

    if (iam->token != NULL) {
        rc = memset_s(iam->token, strlen(iam->token), 0, strlen(iam->token));
        km_securec_check(rc, "", "");
        km_free(iam->token);
        iam->token = NULL;
    }

    return his_iam_get_token(iam);
}