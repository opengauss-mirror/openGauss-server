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
 *
 * convjson.cpp
 *      we need to construct HTTP packages to connect to servers with restful style interfaces.
 *      however, it's hard to construct json strings directly in the code.
 *      so, we provide this bin tool to convert json file into code file: 'convjson'.
 * 
 * IDENTIFICATION
 *	  src/common/interfaces/libpq/client_logic_hooks/cmk_entity_manager_hooks/convert_jsonfile/convjson.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "convjson.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "securec.h"
#include "securec_check.h"
#include "cmkem_comm.h"
#include "convjson_common.h"

/*
 * rule (1) : str -> "str"
 * rule (2) : "str" -> "\"str\""
 * rele (3) : \n -> "\”\n\""
 *
 * e.g.
 * input:  
 *     line 1
 *     line 2
 *     "line3"
 * outpt :
 *    "line1"
 *    ""
 *    "line2"
 *    ""
 *    "\"line3\""
 */
CmkemStr *conv_str_to_code(const char *var, CmkemStr *var_val)
{
    errno_t rc = 0;
    
    /* 
     * input : var, var_val
     * output : "const char *var = \"var_val\";"
     * to make sure the buffer is enough, we consider the worsted situation, such as :
     * if var_val =  "\n\n\n", 
     * according to the rule(3), we should convert it to "\"\n\"\"\n\"\"\n\"" (= var_val * 3)
     */
    size_t code_len = strlen("const char *$ = \"") + strlen(var) + 3 * var_val->str_len + strlen("\";");
    CmkemStr *code = malloc_cmkem_str(code_len);
    if (code == NULL) {
        return NULL;
    }

    /* code = "const char *var = \"" */
    rc = sprintf_s(code->str_val, code_len, "const char *%s = \"", var);
    securec_check_ss_c(rc, "", "");
    code->str_len += strlen(var) + strlen("const char * = \"");

    /* code = "var = \"val_val" */
    for (size_t i = 0; i < var_val->str_len; i++) {
        char var_val_chr = var_val->str_val[i];
        if (var_val_chr == '\"') {
            push_char(code, '\\');
            push_char(code, var_val_chr);
        } else if (var_val_chr == '\n') {
            push_char(code, '\"');
            push_char(code, var_val_chr);
            push_char(code, '\"');
        } else {
            push_char(code, var_val_chr);
        }
    }

    /* code = "var = \"val_val\";" */
    push_char(code, '\"');
    push_char(code, ';');
    push_char(code, '\n');
    push_char(code, '\n');

    return code;
}

CmkemStr *conv_jsontree_to_advstr(cJSON *json_tree)
{
    char *json_str = NULL;
    CmkemStr *json_advstr = NULL;

    json_str = cJSON_Print(json_tree);
    if (json_str == NULL) {
        cmkem_errmsg("failed to parse json tree.");
        return NULL;
    }

    json_advstr = conv_str_to_cmkem_str(json_str);
    cJSON_free(json_str);
    if (json_advstr == NULL) {
        return NULL;
    }

    return json_advstr;
}

CmkemStr *conv_jsontree_to_code(cJSON *json_tree)
{
    char *json_str = NULL;
    CmkemStr *json_advstr = NULL;

    json_str = cJSON_Print(json_tree);
    if (json_str == NULL) {
        cmkem_errmsg("failed to parse json tree.");
        return NULL;
    }

    json_advstr = conv_str_to_cmkem_str(json_str);
    cJSON_free(json_str);
    if (json_advstr == NULL) {
        cmkem_errmsg("failed to malloc memory.");
        return NULL;
    }

    return conv_str_to_code(json_tree->valuestring, json_advstr);
}

cJSON *conv_jsonfile_to_cjson(const char *json_file)
{
    CmkemStr *json_file_content = NULL;

    json_file_content = read_file(json_file);
    if (json_file_content == NULL) {
        return NULL;
    }

    return cJSON_Parse(json_file_content->str_val);
}

CmkemErrCode conv_jsonfile_to_headfile_with_resolve(const char *json_file, const char *header_file,
    const char *child_json_list[])
{
    CmkemErrCode ret = CMKEM_SUCCEED;
    cJSON *father = NULL;
    cJSON *cur_child = NULL;
    CmkemStr *cur_kmsstr = NULL;
    CmkemStr *cur_code = NULL; /* cur_code = "const char *cur_var = \"cur_json\";" */

    ret = remove_file(header_file);
    if (ret != CMKEM_SUCCEED) {
        return ret;
    }

    father = conv_jsonfile_to_cjson(json_file);
    if (father == NULL) {
        return CMKEM_UNKNOWN_ERR;
    }

    ret = create_file(header_file);
    if (ret != CMKEM_SUCCEED) {
        return ret;
    }

    for (size_t i = 0; child_json_list[i] != NULL; i++) {
        cur_child = cJSON_GetObjectItem(father, child_json_list[i]);
        if (cur_child == NULL) {
            return CMKEM_FIND_CSJON_ERR;
        }

        cur_kmsstr = conv_jsontree_to_advstr(cur_child);

        cur_code = conv_str_to_code(child_json_list[i], cur_kmsstr);
        if (cur_code == NULL) {
            return CMKEM_MALLOC_MEM_ERR;
        }
        free_cmkem_str(cur_kmsstr);

        ret = write_content_to_tail(header_file, cur_code->str_val, cur_code->str_len);
        if (ret != CMKEM_SUCCEED) {
            return ret;
        }
        free_cmkem_str(cur_code);
    }

    return ret;
}

int main()
{
    CmkemErrCode ret = CMKEM_SUCCEED;

    system("rm -rf ./kms_httpmsg_temp.ini");
    
    const char *in_file = "./kms_httpmsg_temp.json";
    const char *out_file = "./kms_httpmsg_temp.ini";
    const char *temp_type_list[] = {
        "temp_iam_auth_req",
        "temp_kms_select_key_req",
        "temp_kms_select_key_res",
        "temp_kms_enc_key_req",
        "temp_kms_enc_key_res",
        "temp_kms_dec_key_req",
        "temp_kms_dec_key_res",
        "temp_iam_err_res",
        "temp_kms_err_res",
        NULL
    };

    ret = conv_jsonfile_to_headfile_with_resolve(in_file, out_file, temp_type_list);
    if (ret != CMKEM_SUCCEED) {
        print_cmkem_errmsg_buf();
    }

    return 0;
}
