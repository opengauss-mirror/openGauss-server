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
 * gs_bool.cpp
 *
 * IDENTIFICATION
 *	  src\common\interfaces\libpq\client_logic_fmt\gs_bool.cpp
 *
 * -------------------------------------------------------------------------
 */
 
#include "gs_bool.h"

bool scan_bool(const char *bool_val, bool *res, const char *err_msg);

bool parse_bool_with_len(const char *value, size_t len, bool *result);
unsigned char *bool_bin(const char *text, size_t *binary_size, const char *err_msg)
{
    unsigned char *binary = (unsigned char *)malloc(sizeof(bool));
    if (binary == NULL) {
        return NULL;
    }
    if (!scan_bool(text, reinterpret_cast<bool *>(binary), err_msg)) {
        free(binary);
        return NULL;
    }
    *binary_size = sizeof(bool);
    return binary;
}

char *bool_bout(const unsigned char *binary, size_t size, size_t *result_size)
{
    Assert(size == sizeof(bool));
    const size_t text_size = 2;
    char *text = (char *)malloc(text_size);
    if (text == NULL) {
        return NULL;
    }
    text[0] = binary[0] ? 't' : 'f';
    text[1] = '\0';
    *result_size = 1;
    return text;
}

/*
 * scan_bool - converts string to bool
 */
bool scan_bool(const char *in_str, bool *res, const char *err_msg)
{
    char *str = const_cast<char *>(in_str);
    while (isspace((unsigned char)*str)) {
        str++;
    }

    size_t len = strlen(str);
    while (len > 0 && isspace((unsigned char)str[len - 1])) {
        len--;
    }
    return parse_bool_with_len(str, len, res);
}

bool parse_bool_with_len(const char *value, size_t len, bool *result)
{
    bool tmp_res = false;
    bool parse_res = false;
    switch (tolower(*value)) {
        case 't':
            if (pg_strncasecmp(value, "true", len) == 0) {
                tmp_res = true;
                parse_res = true;
            }
            break;
        case 'f':
            if (pg_strncasecmp(value, "false", len) == 0) {
                tmp_res = false;
                parse_res = true;
            }
            break;
        case 'y':
            if (pg_strncasecmp(value, "yes", len) == 0) {
                tmp_res = true;
                parse_res = true;
            }
            break;
        case 'n':
            if (pg_strncasecmp(value, "no", len) == 0) {
                tmp_res = false;
                parse_res = true;
            }
            break;
        case 'o':
            /* 'o' is not unique enough */
            if (pg_strncasecmp(value, "on", (len > 2 ? len : 2)) == 0) {
                tmp_res = true;
                parse_res = true;
            } else if (pg_strncasecmp(value, "off", (len > 2 ? len : 2)) == 0) {
                tmp_res = false;
                parse_res = true;
            }
            break;
        case '1':
            if (len == 1) {
                tmp_res = true;
                parse_res = true;
            }
            break;
        case '0':
            if (len == 1) {
                tmp_res = false;
                parse_res = true;
            }
            break;
        default:
            break;
    }

    if (result != NULL) {
        *result = tmp_res;
    }
    return parse_res;
}
