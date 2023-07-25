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
 * security_json.cpp
 *
 * IDENTIFICATION
 *	  src/gausskernel/security/keymgr/src/comm/security_json.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "keymgr/comm/security_json.h"
#include <string.h>
#include "keymgr/comm/security_error.h"
#include "keymgr/comm/security_utils.h"
#include "cjson/cJSON.h"

static cJSON *json_format(const char *str)
{
    if (str == NULL) {
        return NULL;
    }

    if (str[0] == '(') {
        /* -2 means: when process str format like: "(a string)",  remove parentheses at the head and tail */
        return cJSON_ParseWithLength(str + 1, strlen(str) - 2);
    }

    return cJSON_Parse(str);
}

static int cjson_replace(cJSON *cjson, ReplaceRule rules[], int rulecnt)
{
    char *newval = NULL;
    int ret;
    int i;

    if (cjson == NULL) {
        return 1;
    }

    if (cJSON_IsString(cjson)) {
        for (i = 0; i < rulecnt; i++) {
            if (strcmp(rules[i].src, cJSON_GetStringValue(cjson)) != 0) {
                continue;
            }

            newval = cJSON_SetValuestring(cjson, rules[i].dest);
            if (newval == NULL) {
                return 0;
            }
        }
    }

    ret = cjson_replace(cjson->next, rules, rulecnt);
    if (ret == 0) {
        return 0;
    }

    return cjson_replace(cjson->child, rules, rulecnt);
}

static char *cjson_find(cJSON *cjson, const char *key)
{
    char *result;
    
    if (cjson == NULL) {
        return NULL;
    }

    if (cjson->string != NULL) {
        if (strcmp(cjson->string, key) == 0) {
            return km_strdup(cJSON_GetStringValue(cjson));
        }
    }

    result = cjson_find(cjson->next, key);
    if (result != NULL) {
        return result;
    }

    return cjson_find(cjson->child, key);
}

char *json_replace(const char *json, ReplaceRule rules[], int rulecnt)
{
    cJSON *cjson;
    int ret;
    char *result = NULL;
    
    if (json == NULL) {
        return NULL;
    }

    cjson = json_format(json);
    if (cjson == NULL) {
        return NULL;
    }

    ret = cjson_replace(cjson, rules, rulecnt);
    if (ret == 0) {
        cJSON_Delete(cjson);
        return NULL;
    }
    result = cJSON_Print(cjson);
    cJSON_Delete(cjson);

    return result;
}

char *json_find(const char *json, const char *key)
{
    cJSON *cjson;
    char *result;

    if (json == NULL) {
        return NULL;
    }

    cjson = json_format(json);
    if (cjson == NULL) {
        return NULL;
    }

    result = cjson_find(cjson, key);
    cJSON_Delete(cjson);

    return result;
}