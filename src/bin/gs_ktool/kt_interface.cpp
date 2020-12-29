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
 * kt_interface.cpp
 *      APIs for Encrypted Database.
 *
 * IDENTIFICATION
 *    src/bin/gs_ktool/kt_interface.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "kt_interface.h"
#include<stdio.h>
#include<stdlib.h>
#include "kt_common.h"
#include "kt_key_manage.h"
#include "kt_log_manage.h"

const int MAX_CMK_PLAIN_LEN = 112;

static bool init_kt_ksf();

static bool init_kt_ksf()
{
    const char required_conf[][MAX_CONF_KEY_LEN] = {"pri_ksf", "sec_ksf"};
    size_t required_conf_cnt = sizeof(required_conf) / sizeof(required_conf[0]);
    
    if (!reg_callback_funcs()) {
        if (g_is_print_err) {
            printf("ERROR(GS_KTOOL): failed to register callback functions.\n");
        }
        return false;
    }

    if (!initialize_logmodule()) {
        return false;
    }

    if (!load_key_manage_conf(required_conf, required_conf_cnt)) {
        return false;
    }

    if (!has_ksf_existed()) {
        insert_format_log(L_OPEN_FILE, KT_ERROR, 
            "please make sure 'gs_ktool' is installed and the key store file already exists");
        return false;
    }

    return init_or_create_ksf();
}

bool get_cmk_len(unsigned int cmk_id, unsigned int *cmk_len)
{
    KmcMkInfo cmk_info = { 0 };
    unsigned char cmk_plain[MAX_CMK_PLAIN_LEN] = { 0 };
    unsigned int cmk_plain_buf_len = MAX_CMK_PLAIN_LEN;
    bool ret = true;

    return_false_if(cmk_len == NULL);

    if (!init_kt_ksf()) {
        return false;
    }

    if (KmcGetMkDetail(DEFAULT_DOMAIN, cmk_id, &cmk_info, cmk_plain, &cmk_plain_buf_len) == WSEC_SUCCESS) {
        insert_format_log(L_SELECT_CMK_LEN, KT_SUCCEED, "cmk id: %d", cmk_id);
    } else {
        insert_format_log(L_SELECT_CMK_LEN, KT_ERROR, "cmk id: %d", cmk_id);
        ret = false;
    }
    WsecFinalizeEx();

    *cmk_len = cmk_plain_buf_len;
    return ret;
}

bool get_cmk_plain(unsigned int cmk_id, unsigned char *cmk_plain, unsigned int *cmk_len, bool is_report_err)
{
    KmcMkInfo cmk_info = { 0 };
    unsigned char tmp_cmk_plain[MAX_CMK_PLAIN_LEN] = {0};
    unsigned int tmp_plain_buf_len = MAX_CMK_PLAIN_LEN;
    bool ret = true;

    return_false_if(cmk_plain == NULL);

    g_is_print_err = is_report_err;

    if (!init_kt_ksf()) {
        return false;
    }

    if (KmcGetMkDetail(DEFAULT_DOMAIN, cmk_id, &cmk_info, tmp_cmk_plain, &tmp_plain_buf_len) == WSEC_SUCCESS) {
        for (unsigned int i = 0; i < tmp_plain_buf_len; i++) {
            cmk_plain[i] = tmp_cmk_plain[i];
        }
        *cmk_len = tmp_plain_buf_len;
        
        insert_format_log(L_SELECT_CMK_PLAIN, KT_SUCCEED, "cmk id: %d", cmk_id);
    } else {
        insert_format_log(L_SELECT_CMK_PLAIN, KT_ERROR, "cmk id: %d", cmk_id);
        ret = false;
    }
    WsecFinalizeEx();

    return ret;
}
