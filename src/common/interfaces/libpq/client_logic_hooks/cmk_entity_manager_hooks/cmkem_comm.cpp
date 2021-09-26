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
 * cmkem_comm.cpp
 *      some common functions, include:
 *          1. error code and error process
 *          2. string process
 *          3. format and conversion
 * 
 * IDENTIFICATION
 *	  src/common/interfaces/libpq/client_logic_hooks/cmk_entity_manager_hooks/cmkem_comm.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "cmkem_comm.h"

#include <stdlib.h>
#include <string.h>
#include <limits.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <fcntl.h>
#include "securec.h"
#include "securec_check.h"

static char cmkem_errmsg_buf[MAX_CMKEM_ERRMSG_BUF_SIZE] = {0};

/* 
 * in some cases, where there is an error, we will report the error in detail.
 * if not, we will use simple and general error information according to the error code
 */
static CmkemErrMsg simple_errmsg_list[] = {
    {CMKEM_SUCCEED, "no error message, the result is 'SUCCEED'."},
    {CMKEM_UNKNOWN_ERR, "unknown error."},
    {CMKEM_CHECK_STORE_ERR, "failed to check the value of arg: 'KEY_STORE'."},
    {CMKEM_CHECK_ALGO_ERR, "failed to check the value of arg: 'ALGORITHM'."},
    {CMKEM_CHECK_CMK_ID_ERR, "failed to check the value of arg: 'KEY_PATH'."},
    {CMKEM_CHECK_IDENTITY_ERR, "faiel to check the usability of cmk entity."},
    {CMKEM_CHECK_INPUT_AUTH_ERR, "the authentication parameters are illegal."},
    {CMKEM_CHECK_PROJECT_NAME_ERR, "failed to check HuaWei KMS project name."},
    {CMKEM_GET_TOKEN_ERR, "failed to get token."},
    {CMKEM_WRITE_TO_BIO_ERR, "failed to wirite key to BIO buffer."},
    {CMKEM_READ_FROM_BIO_ERR, "failed to read key from BIO buffer."},
    {CMKEM_GET_RAND_NUM_ERR, "failed to generate a random num."},
    {CMKEM_DERIVED_KEY_ERR, "failed to derived key."},
    {CMKEM_GEN_RSA_KEY_ERR, "failed to create rsa key pair."},
    {CMKEM_GEN_SM2_KEY_ERR, "failed to create sm2 key pair."},
    {CMKEM_RSA_DECRYPT_ERR, "failed to decrypt cipher with RSA algorithm."},
    {CMKEM_RSA_ENCRYPT_ERR, "failed to encrypt plain with RSA algorithm."},
    {CMKEM_CHECK_HASH_ERR, "failed check the hash of data."},
    {CMKEM_EVP_ERR, "openssl evp error."},
    {CMKEM_SM2_ENC_ERR, "failed to encrypt plain with SM2 algorithm."},
    {CMKEM_SM2_DEC_ERR, "failed to decrypt cipher with SM2 algorithm."},
    {CMKEM_REG_CMK_MANAGER_ERR, "failed to register cmk entity manager."},
    {CMKEM_REGISTRE_FUNC_ERR, "failed to register hook function of cmk manager."},
    {CMKEM_CREATE_CMK_ERR, "failed to create client master key object."},
    {CMKEM_ENCRYPT_CEK_ERR, "failed to encrypt column encryption key plain with client master key."},
    {CMKEM_DECRYPT_CEK_ERR, "failed to decrypt column encryption key cipher with client master key."},
    {CMKEM_DROP_CMK_ERR, "failed to drop client master key object."},
    {CMKEM_GET_ENV_VAL_ERR, "failed to get environment value from system."},
    {CMKEM_CHECK_ENV_VAL_ERR, "the environment value is illegal."},
    {CMKEM_SET_ENV_VALUE_ERR, "failed to set global environment value."},
    {CMKEM_GS_KTOOL_ERR, "gs_ktool error."},
    {CMKEM_IAM_SERVER_ERR, "Huawei IAM server error."},
    {CMKEM_CHECK_CHACHE_ID_ERR, "the client cache id is invalid."},
    {CMKEM_CACHE_IS_EMPTY, "the specific cache block is empty."},
    {CMKEM_TOKEN_EXPIRED_ERR, "the token has expired."},
    {CMKEM_KMS_SERVER_ERR, "Huawei KMS server error."},
    {CMKEM_ENC_CMK_ERR, "failed to encrypt cmk entity plain."},
    {CMKEM_DEC_CMK_ERR, "failed to decrypt cmk entity cipher."},
    {CMKEM_CREATE_FILE_ERR, "failed to create file."},
    {CMKEM_FIND_FILE_ERR, "failed to find file."},
    {CMKEM_OPEN_FILE_ERR, "failed to open file."},
    {CMKEM_READ_FILE_ERR, "failed to read file."},
    {CMKEM_READ_FILE_STATUS_ERR, "failed to read file status."},
    {CMKEM_WRITE_FILE_ERR, "failed to write to file."},
    {CMKEM_REMOVE_FILE_ERR, "failed to remove file."},
    {CMKEM_MALLOC_MEM_ERR, "failed to malloc memory from heap."},
    {CMKEM_CHECK_BUF_LEN_ERR, "the buffer size is too short."},
    {CMKEM_CJSON_PARSE_ERR, "failed to parser string with 'CJSON'."},
    {CMKEM_FIND_CSJON_ERR, "failed to find node from cjson tree."},
    {CMKEM_SET_CJSON_VALUE_ERR, "failed to set the value of cjson tree node."},
    {CMKEM_CURL_INIT_ERR, "failed to init curl."},
    {CMKEM_CURL_ERR, "curl error."},
};

/* 
 * write error message to err_msg buffer, if need error message, can chose:
 *      1. cmkem_list_print error message.
 *      2. copy/dump error message to new err_msg buffer.
 */
void write_cmkem_errmsg(const char *errmsg)
{
    errno_t rc = 0;

    if (cmkem_errmsg_buf == NULL) {
        return;
    }
    
    rc = memset_s(cmkem_errmsg_buf, MAX_CMKEM_ERRMSG_BUF_SIZE, 0, sizeof(cmkem_errmsg_buf));
    securec_check_c(rc, "", "");

    rc = sprintf_s(cmkem_errmsg_buf, MAX_CMKEM_ERRMSG_BUF_SIZE, "%s", errmsg);
    securec_check_ss_c(rc, "", "");
}

void print_cmkem_errmsg_buf()
{
    if (cmkem_errmsg_buf == NULL) {
        printf("ERROR: error message buffer is 'NULL'.");
        return;
    }

    if (strlen(cmkem_errmsg_buf) > 0) {
        printf("ERROR: %s\n", cmkem_errmsg_buf);
    } else {
        printf("NOTICE: error message buffer is empty.\n");
    }
}

void dump_cmkem_errmsg_buf(char *dest_buf, size_t dest_buf_size)
{
    const char *empty_hint = "error message buffer is empty.";
    const char *buf_too_short_hint = "failed to dump error message, your buffer is too short.";
    errno_t rc = 0;

    if (cmkem_errmsg_buf == NULL) {
        return;
    }

    if (strlen(cmkem_errmsg_buf) == 0) {
        if (strlen(empty_hint) < dest_buf_size) {
            rc = strcpy_s(dest_buf, dest_buf_size, empty_hint);
            securec_check_c(rc, "", "");
        }
        return;
    }

    if (strlen(cmkem_errmsg_buf) > dest_buf_size) {
        if (strlen(buf_too_short_hint) < dest_buf_size) {
            rc = strcpy_s(dest_buf, dest_buf_size, buf_too_short_hint);
            securec_check_c(rc, "", "");
        }
        return;
    }

    rc = strcpy_s(dest_buf, dest_buf_size, cmkem_errmsg_buf);
    securec_check_c(rc, "", "");

    return;
}

const char *get_cmkem_errmsg(CmkemErrCode err_code)
{
    if (cmkem_errmsg_buf == NULL) {
        return NULL;
    }
    
    if (strlen(cmkem_errmsg_buf) == 0) {
        return simple_errmsg_list[err_code].err_msg;
    }
    
    return (const char *)cmkem_errmsg_buf;
}

void erase_data(void *data, size_t data_len)
{
    errno_t rc = 0;
    
    if (data == NULL) {
        return;
    }

    rc = memset_s(data, data_len, 0, data_len);
    securec_check_c(rc, "", "");
}

CmkemStr *malloc_cmkem_str(size_t str_buf_len)
{
    if (str_buf_len == 0) {
        return NULL;
    }
    
    CmkemStr *advstr = (CmkemStr *)malloc(sizeof(CmkemStr));
    if (advstr == NULL) {
        return NULL;
    }

    advstr->str_val = (char *)malloc(str_buf_len);
    if (advstr->str_val == NULL) {
        cmkem_free(advstr);
        return NULL;
    }

    advstr->str_len = 0;
    return advstr;
}

void free_cmkem_str(CmkemStr *advstr_ptr)
{
    if (advstr_ptr != NULL) {
        cmkem_free(advstr_ptr->str_val);
        cmkem_free(advstr_ptr);
    }

    advstr_ptr = NULL;
}

CmkemUStr *malloc_cmkem_ustr(size_t ust_buf_len)
{
    if (ust_buf_len == 0) {
        return NULL;
    }
    
    CmkemUStr *key_str = (CmkemUStr *)malloc(sizeof(CmkemUStr));
    if (key_str == NULL) {
        return NULL;
    }

    key_str->ustr_val = (unsigned char *)malloc(ust_buf_len);
    if (key_str->ustr_val == NULL) {
        cmkem_free(key_str);
        return NULL;
    }

    key_str->ustr_len = 0;
    return key_str;
}

void free_cmkem_ustr(CmkemUStr *cmkem_ustr)
{
    if (cmkem_ustr != NULL) {
        cmkem_free(cmkem_ustr->ustr_val);
        cmkem_free(cmkem_ustr);
    }

    cmkem_ustr = NULL;
}

void free_cmkem_ustr_with_erase(CmkemUStr *cmkem_ustr)
{
    if (cmkem_ustr != NULL) {
        erase_data(cmkem_ustr->ustr_val, cmkem_ustr->ustr_len);
        cmkem_free(cmkem_ustr->ustr_val);
        cmkem_free(cmkem_ustr);
    }

    cmkem_ustr = NULL;
}

CmkemStrList *malloc_cmkem_list(void)
{
    CmkemStrList *new_list = NULL;

    new_list = (CmkemStrList *)malloc(sizeof(CmkemStrList));
    if (new_list == NULL) {
        return NULL;
    }

    new_list->node_cnt = 0;
    new_list->first_node = NULL;
    return new_list;
}

void free_cmkem_list(CmkemStrList *list)
{
    CmkemStrNode *cur_node = NULL;
    CmkemStrNode *to_free = NULL;

    if (list == NULL) {
        return;
    }

    cur_node = list->first_node;
    while (cur_node != NULL) {
        to_free = cur_node;
        cur_node = cur_node->next;
        cmkem_free(to_free->str_val);
        cmkem_free(to_free);
    }

    cmkem_free(list);
}

void free_cmkem_list_with_skip(CmkemStrList *list, int list_pos)
{
    CmkemStrNode *cur_node = NULL;
    CmkemStrNode *to_free = NULL;

    if (list_pos == -1) {
        list_pos = list->node_cnt - 1;
    }

    cur_node = list->first_node;
    for (int i = 0; i < (int)cmkem_list_len(list); i++) {
        to_free = cur_node;
        cur_node = cur_node->next;

        if (i == list_pos) {
            cmkem_free(to_free);
        } else {
            cmkem_free(to_free->str_val);
            cmkem_free(to_free);
        }
    }

    cmkem_free(list);
}

size_t cmkem_list_len(CmkemStrList *list)
{
    return list->node_cnt;
}

void cmkem_list_size(CmkemStrList *list, size_t *list_size)
{
    for (size_t i = 0; i < cmkem_list_len(list); i++) {
        *list_size += strlen(cmkem_list_val(list, i));
    }
}

char *cmkem_list_val(CmkemStrList *list, int list_pos)
{
    CmkemStrNode *target_node = NULL;

    if (list_pos == -1) {
        list_pos = list->node_cnt - 1;
    }

    if (list_pos < 0 || list_pos >= list->node_cnt) {
        return NULL;
    }

    target_node = list->first_node;
    for (int i = 0; i < list_pos; i++) {
        target_node = target_node->next;
    }

    return target_node->str_val;
}

int cmkem_list_pos(CmkemStrList *list, const char *str_val)
{
    CmkemStrNode *target_node = NULL;

    if (list->node_cnt < 1) {
        return -1; /* -1 means : cannot find str_val in list */
    }

    target_node = list->first_node;
    for (int i = 0; i < list->node_cnt; i++) {
        if (strcmp(target_node->str_val, str_val) == 0) {
            return i;
        }
        target_node = target_node->next;
    }

    return -1;
}

void cmkem_list_print(CmkemStrList *list)
{
    CmkemStrNode *cur_node = NULL;

    printf("(%d)[", list->node_cnt);

    cur_node = list->first_node;
    for (int i = 0; i < list->node_cnt; i++) {
        if (i != list->node_cnt - 1) {
            printf("'%s', ", cur_node->str_val);
        } else {
            /* cmkem_list_print the last node */
            printf("'%s'", cur_node->str_val);
        }
        cur_node = cur_node->next;
    }

    printf("]\n");
}

/* 
 * malloc a new_node, then:
 *  the last_node -> next = new_node 
 */
void cmkem_list_append(CmkemStrList *list, const char *str_val)
{
    CmkemStrNode *last_node = NULL;
    CmkemStrNode *new_node = NULL;
    char *new_node_str_val = NULL;
    errno_t rc = 0;

    if (str_val == NULL) {
        return;
    }

    new_node_str_val = (char *)malloc(strlen(str_val) + 1);
    if (new_node_str_val == NULL) {
        return;
    }

    new_node = (CmkemStrNode *)malloc(sizeof(CmkemStrNode));
    if (new_node == NULL) {
        cmkem_free(new_node_str_val);
        return;
    }
    rc = strcpy_s(new_node_str_val, strlen(str_val) + 1, str_val);
    securec_check_c(rc, "", "");

    new_node->str_val = new_node_str_val;
    new_node->next = NULL;

    if (list->first_node == NULL) {
        list->first_node = new_node;
    } else {
        last_node = list->first_node;
        for (int i = 0; i < (list->node_cnt - 1); i++) {
            last_node = last_node->next;
        }
        last_node->next = new_node;
    }

    list->node_cnt++;
}

void cmkem_list_insert(CmkemStrList *list, const char *str_val, int list_pos)
{
    CmkemStrNode *new_node = NULL;
    CmkemStrNode *pre_new_node = NULL;
    char *new_node_str_val = NULL;
    errno_t rc = 0;

    if (list_pos < 0 || list_pos >= list->node_cnt) {
        return;
    }

    new_node_str_val = (char *)malloc(strlen(str_val) + 1);
    if (new_node_str_val == NULL) {
        return;
    }

    new_node = (CmkemStrNode *)malloc(sizeof(CmkemStrNode));
    if (new_node == NULL) {
        cmkem_free(new_node_str_val);
        return;
    }

    rc = strcpy_s(new_node_str_val, strlen(str_val) + 1, str_val);
    securec_check_c(rc, "", "");
    new_node->str_val = new_node_str_val;

    if (list_pos == 0) {
        new_node->next = list->first_node;
        list->first_node = new_node;
    } else {
        pre_new_node = list->first_node;
        for (int i = 0; i < (list_pos - 1); i++) {
            pre_new_node = pre_new_node->next;
        }
        new_node->next = pre_new_node->next;
        pre_new_node->next = new_node;
    }

    list->node_cnt++;
}

void cmkem_list_pop(CmkemStrList *list)
{
    CmkemStrNode *last_node = NULL;
    CmkemStrNode *pre_last_node = NULL;

    if (list->first_node == 0) {
        return;
    } else if (list->node_cnt == 1) {
        last_node = list->first_node;
        list->first_node = NULL;
    } else {
        pre_last_node = list->first_node;
        for (int i = 0; i < (list->node_cnt - 2); i++) {
            pre_last_node = pre_last_node->next;
        }
        last_node = pre_last_node->next;
        pre_last_node->next = NULL;
    }

    cmkem_free(last_node->str_val);
    cmkem_free(last_node);
    list->node_cnt--;
}

void cmkem_list_del(CmkemStrList *list, int list_pos)
{
    CmkemStrNode *target_node = NULL;
    CmkemStrNode *pre_target_node = NULL;

    if (list_pos == -1) {
        list_pos = list->node_cnt - 1;
    }

    if (list_pos == 0) {
        target_node = list->first_node;
        list->first_node = list->first_node->next;
    } else if (list_pos > 0 && list_pos < list->node_cnt) {
        pre_target_node = list->first_node;
        for (int i = 0; i < list_pos - 1; i++) {
            pre_target_node = pre_target_node->next;
        }
        target_node = pre_target_node->next;
        pre_target_node->next = target_node->next;
    } else {
        return;
    }

    cmkem_free(target_node->str_val);
    cmkem_free(target_node);
    list->node_cnt--;
}

void cmkem_list_cat(CmkemStrList *base_list, CmkemStrList *extra_list)
{
    CmkemStrNode *last_node = NULL;

    if (base_list->node_cnt == 0) {
        base_list->first_node = extra_list->first_node;
        base_list->node_cnt += extra_list->node_cnt;
        return;
    } else {
        last_node = base_list->first_node;
        for (int i = 0; i < (base_list->node_cnt - 1); i++) {
            last_node = last_node->next;
        }
        last_node->next = extra_list->first_node;
        base_list->node_cnt += extra_list->node_cnt;
    }
}

char *cmkem_list_merge(CmkemStrList* list)
{
    size_t all_strlen = 1; /* for EOF */
    char *merged_str = NULL;
    errno_t rc = 0;

    for (size_t i = 0; i < cmkem_list_len(list); i++) {
        all_strlen += strlen(cmkem_list_val(list, i));
    }

    merged_str = (char *)malloc(all_strlen);
    if (merged_str == NULL) {
        return NULL;
    }

    for (size_t i = 0; i < cmkem_list_len(list); i++) {
        if (i == 0) {
            rc = strcpy_s(merged_str, all_strlen, cmkem_list_val(list, i));
            securec_check_c(rc, "", "");
        } else {
            rc = strcat_s(merged_str, all_strlen, cmkem_list_val(list, i));
            securec_check_ss_c(rc, "", "");
        }
    }

    return merged_str;
}

CmkemStrList *cmkem_split(const char *str, char split_char)
{
    CmkemStrList *substr_list = NULL;
    size_t str_start = 0;
    errno_t rc = 0;

    substr_list = malloc_cmkem_list();
    if (substr_list == NULL) {
        return NULL;
    }

    for (size_t i = 0; i < strlen(str); i++) {
        if (str[i] == split_char || i == strlen(str) - 1) {
            if (i == strlen(str) - 1 && str[i] != split_char) {
                cmkem_list_append(substr_list, str + str_start);
            } else {
                char cur_substr[i - str_start + 1] = {0};
                rc = strncpy_s(cur_substr, i - str_start + 1, str + str_start, i - str_start);
                securec_check_c(rc, "", "");

                cur_substr[i - str_start] = '\0';
                str_start = i + 1;

                cmkem_list_append(substr_list, cur_substr);
            }
        }
    }

    if (cmkem_list_len(substr_list) == 0) {
        cmkem_list_free(substr_list);
        return NULL;
    }

    return substr_list;
}

bool is_str_empty(const char *str)
{
    return (str == NULL || strlen(str) == 0) ? true : false;
}

CmkemStr *conv_str_to_cmkem_str(const char *str)
{
    errno_t rc = 0;
    
    CmkemStr *advstr = malloc_cmkem_str(strlen(str) + 1);
    if (advstr == NULL) {
        return NULL;
    }

    rc = strcpy_s(advstr->str_val, strlen(str) + 1, str);
    securec_check_c(rc, "", "");
    advstr->str_len = strlen(str);

    return advstr;
}

CmkemUStr *conv_str_to_cmkem_ustr(const unsigned char *ustr, size_t ustr_len)
{   
    CmkemUStr *cmkem_ustr = malloc_cmkem_ustr(ustr_len + 1);
    if (cmkem_ustr == NULL) {
        return NULL;
    }

    for (size_t i = 0; i < ustr_len; i++) {
        cmkem_ustr->ustr_val[i] = ustr[i];
    }
    cmkem_ustr->ustr_val[ustr_len] = '\0';
    cmkem_ustr->ustr_len = ustr_len;

    return cmkem_ustr;
}

void push_char(CmkemStr *advstr_ptr, char chr)
{
    advstr_ptr->str_val[advstr_ptr->str_len] = chr;
    advstr_ptr->str_len += 1;
    advstr_ptr->str_val[advstr_ptr->str_len] = '\0';
}

void itoa(int num, char *str_buf, size_t buf_len)
{
    errno_t rc = 0;
    
    rc = sprintf_s(str_buf, buf_len, "%d", num);
    securec_check_ss_c(rc, "", "");
}

int hex_atoi(char hex)
{
    if (hex >= '0' && hex <= '9') {
        return hex - '0';
    } else if (hex >= 'a' && hex <= 'f') {
        return hex - 'a' + 0xa;
    } else if (hex >= 'A' && hex <= 'F') {
        return hex - 'A' + 0xa;
    }

    return 0;
}

CmkemStr *ustr_to_hex(CmkemUStr *ustr)
{
    const char *hex_tbl = "0123456789abcdef";
    size_t pos = 0;

    CmkemStr *hex = malloc_cmkem_str(ustr->ustr_len * 2 + 1); /* e.g ustr = "b",  hex = "62" + '\0' */
    if (hex == NULL) {
        return NULL;
    }
    
    for (; pos < ustr->ustr_len; pos++) {
        hex->str_val[2 * pos] = hex_tbl[(ustr->ustr_val[pos] >> 4) & 0xf];
        hex->str_val[2 * pos + 1] = hex_tbl[((ustr->ustr_val[pos] << 4) >> 4) & 0xf];
    }
    hex->str_val[2 * pos] = '\0';
    hex->str_len = 2 * pos;

    return hex;
}

CmkemUStr *hex_to_ustr(CmkemStr *hex)
{
    size_t pos = 0;
    unsigned int low = 0;
    unsigned int high = 0;

    CmkemUStr *ustr = malloc_cmkem_ustr((int) hex->str_len / 2 + 1); /* e.g hex = "62",  ustr = 'b' + '\0' */
    if (ustr == NULL) {
        return NULL;
    }

    for (; pos < hex->str_len; pos += 2) {
        high = (unsigned int)hex_atoi(hex->str_val[pos]);
        low = (unsigned int)hex_atoi(hex->str_val[pos + 1]);
        
        ustr->ustr_val[(int) pos / 2] = ((high << 4) & 0xff) ^ (low & 0xff);
    }
    ustr->ustr_val[(int) pos / 2] = '\0';
    ustr->ustr_len = (size_t) pos / 2;

    return ustr;
}
