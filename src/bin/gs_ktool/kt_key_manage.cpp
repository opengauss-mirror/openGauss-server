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
 * kt_key_manage.cpp
 *    Foreword : before reading this part of code, you should be familiar with the principle for key classification; 
 *               This part is mainly used for encrypted database;
 *    Describe : in the scenario of three-level-key-schema, we use rk (root key) to encrypt cmk (client master key), 
 *               use cmk to encrypt cek (client encrypt key), use cek to encrypt data. We use KMC to manage cmk and rk;
 *    Function : cmk manage : 1) generate cmk; 2) delete cmk; 3) select cmk info; 4) export cmk; 5) import cmk;
 *               rk manage : 1) select rk info; 2) update rk;
 *               (this module calls KMC interfaces to realize the key management functions.)
 * 
 * IDENTIFICATION
 *    src/bin/gs_ktool/kt_key_manage.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "kt_key_manage.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "kt_log_manage.h"

static const char g_all_key_manage_conf[][MAX_CONF_KEY_LEN] = {"rk_validity", "cmk_validity", "cmk_length", "pri_ksf",
    "sec_ksf", "export_ksf"};

static bool set_global_key_conf(KeyManageConf key_conf, const char *conf_key, const char *conf_value);
static bool set_rk_validity();
static bool check_rk_validity(int *days);

bool reg_callback_funcs(void)
{
    return (RegFunCallback() == WSEC_SUCCESS);
}

bool load_key_manage_conf(const char required_conf[MAX_CONF_ITEM_QUANTITY][MAX_CONF_KEY_LEN],
    size_t required_conf_cnt)
{
    char key_list[MAX_CONF_ITEM_QUANTITY][MAX_CONF_KEY_LEN] = {0};
    char value_list[MAX_CONF_ITEM_QUANTITY][MAX_CONF_VALUE_LEN] = {0};
    size_t lost_conf_pos = 0;
    size_t conf_item_cnt = 0;
    bool ret = true;
    bool is_find_conf_item = false;

    if (!read_conf_segment(g_conf_file, "KeyManage", key_list, value_list, &conf_item_cnt)) {
        insert_format_log(L_READ_CONF, KT_ERROR, "cannot find configuration segment : %s", "KeyManage");
        return false;
    }

    if (!compare_list_element(required_conf, required_conf_cnt, key_list, conf_item_cnt, &lost_conf_pos)) {
        insert_format_log(L_READ_CONF, KT_ERROR, "lost configuration item : %s", required_conf[lost_conf_pos]);
        return false;
    }

    for (size_t i = 0; i < required_conf_cnt; i++) {
        for (size_t j = 0; j < sizeof(g_all_key_manage_conf) / sizeof(g_all_key_manage_conf[0]); j++) {
            if (strcmp(required_conf[i], g_all_key_manage_conf[j]) == 0) {
                is_find_conf_item = true;
                switch (j) {
                    case RK_VALIDITY:
                        if (!set_global_key_conf(RK_VALIDITY, key_list[j], value_list[j])) {
                            return false;
                        }
                        break;
                    case CMK_VALIDITY:
                        if (!set_global_key_conf(CMK_VALIDITY, key_list[j], value_list[j])) {
                            return false;
                        }
                        break;
                    case CMK_LENGTH:
                        if (!set_global_key_conf(CMK_LENGTH, key_list[j], value_list[j])) {
                            return false;
                        }
                        break;
                    case PRI_KSF:
                        ret = set_global_file(g_conf.pri_ksf, G_PRI_KSF, value_list[j]);
                        break;
                    case SEC_KSF:
                        ret = set_global_file(g_conf.sec_ksf, G_SEC_KSF, value_list[j]);
                        break;
                    case EXPORT_KSF:
                        ret = set_global_file(g_conf.export_ksf, G_EXPORT_KSF, value_list[j]);
                        break;
                    default:
                        break;
                }
            }

            if (!ret) {
                printf("%s %s\n", key_list[j], value_list[j]);
                insert_format_log(L_SET_CONF, KT_ERROR, "the value of '%s' is invalid", key_list[i]);
                return false;
            }
        }

        if (!is_find_conf_item) {
            insert_format_log(L_READ_CONF, KT_ERROR, "lost configuration item : %s", key_list[i]);
            return false;
        } else {
            is_find_conf_item = false;
        }
    }

    return true;
}

static bool set_global_key_conf(KeyManageConf key_conf, const char *conf_key, const char *conf_value)
{
    unsigned int input_num = 0;

    if (!atoi_strictly(conf_value, &input_num)) {
        insert_format_log(L_SET_CONF, KT_ERROR, "this configuration value '%s' is expected to be an positive integer",
            conf_value);
        return false;
    }

    switch (key_conf) {
        case RK_VALIDITY: 
            if (input_num < 1 || input_num > MAX_RK_VALIDITY) {
                insert_format_log(L_SET_CONF, KT_ERROR, "%s should be in range (0, %d]", conf_key, MAX_RK_VALIDITY);
                return false;
            }
            g_conf.rk_validity = input_num;
            break;
        case CMK_VALIDITY: {
            if (input_num < 1 || input_num > MAX_CMK_VALIDITY) {
                insert_format_log(L_SET_CONF, KT_ERROR, "%s should be in range (0, %d]", conf_key, MAX_CMK_VALIDITY);
                return false;
            }
            g_conf.cmk_validity = input_num;
            break;
        }
        case CMK_LENGTH:
            if (input_num < 1 || input_num > MAX_CMK_LEN) {
                insert_format_log(L_SET_CONF, KT_ERROR, "%s should be in range (0, %d]", conf_key, MAX_CMK_LEN);
                return false;
            }
            g_conf.cmk_length = input_num;
            break;
        default:
            break;
    }

    return true;
}

bool has_ksf_existed(void)
{
    FILE *pri_fp = NULL;
    FILE *sec_fp = NULL;

    pri_fp = fopen(g_conf.pri_ksf, "r");
    if (pri_fp == NULL) {
        return false;
    }

    sec_fp = fopen(g_conf.sec_ksf, "r");
    if (sec_fp == NULL) {
        fclose(pri_fp);
        return false;
    }

    fclose(pri_fp);
    fclose(sec_fp);
    return true;
}

bool init_or_create_ksf(void)
{
    KmcKsfName ksf_paths = { 0 };

    ksf_paths.keyStoreFile[0] = g_conf.pri_ksf;
    ksf_paths.keyStoreFile[1] = g_conf.sec_ksf;

    /*
     * in WsecInitializeEx():
     * if the ksf cannot be found, this func will create a new one
     * else if the ksf is found, this func only initialize it
     */
    return (WsecInitializeEx(KMC_ROLE_MASTER, &ksf_paths, WSEC_FALSE, NULL) == WSEC_SUCCESS);
}

static bool set_rk_validity()
{
    KmcCfgRootKey rk_conf = { 0 };
    rk_conf.validity = g_conf.rk_validity;
    rk_conf.rmkIter = DEFAULT_RK_ITER;

    return (KmcSetDefaultRootKeyCfg(&rk_conf) == WSEC_SUCCESS);
}

static bool check_rk_validity(int *days)
{
    KmcRkAttributes rk_attr = { 0 };
    Time today = { 0 };
    Time expired_day = { 0 };

    if (KmcGetRootKeyInfo(&rk_attr) != WSEC_SUCCESS) {
        insert_format_log(L_SELECT_RK, KT_ERROR, "");
        return false;
    }

    if (!get_sys_time(&today)) {
        insert_format_log(L_GET_TIME, KT_ERROR, "");
        return false;
    }

    trans_wsectime_to_time(rk_attr.rkExpiredTimeUtc, &expired_day);
    *days = calculate_interval(today, expired_day);

    return true;
}

bool init_keytool(void)
{
    int rk_expired_days = 0;
    const char required_conf[][MAX_CONF_KEY_LEN] = {"rk_validity", "cmk_validity", "cmk_length",
        "pri_ksf", "sec_ksf", "export_ksf"};
    size_t required_conf_cnt = sizeof(required_conf) / sizeof(required_conf[0]);

    if (!reg_callback_funcs()) {
        insert_format_log(L_REG_CALLBACK_FUNC, KT_ERROR, "");
        return false;
    }

    if (!load_key_manage_conf(required_conf, required_conf_cnt)) {
        return false;
    }

    if (!set_rk_validity()) {
        insert_format_log(L_SET_RK_VALIDITY, KT_ERROR, "");
        return false;
    }

    bool ret = has_ksf_existed();

    if (!init_or_create_ksf()) {
        insert_format_log(L_INIT_KMC, KT_ERROR, "");
        exit(1);
    }

    if (!ret) {
        printf("NOTICE: created new key store file : %s.\n", g_conf.pri_ksf);
        insert_format_log(L_CREATE_FILE, KT_NOTICE, "new key store file : %s", g_conf.pri_ksf);
    }

    if (!check_rk_validity(&rk_expired_days)) {
        return false;
    }

    if (rk_expired_days < 0) {
        printf("WARNING: your root key has expired \"%d\" days.\n", -rk_expired_days);
        insert_format_log(L_CHECK_RK_VALIDITY, KT_ALARM, "expired : %d", -rk_expired_days);

        printf("HINT: you can use following conmmand to cycle root key:");
        printf("        gs_ktool -R -u\n");
    }

    return true;
}

CmkInfo *init_cmk_info_list()
{
    CmkInfo *cmk_info_list = NULL;

    cmk_info_list = (CmkInfo *)malloc(sizeof(CmkInfo));
    if (cmk_info_list == NULL) {
        insert_format_log(L_MALLOC_MEMORY, KT_ERROR, "");
        exit(1);
    }

    cmk_info_list->cmk_id = INVALID_CMK_KD;
    cmk_info_list->next = NULL;

    return cmk_info_list;
}

bool add_cmk_info_node(CmkInfo *cmk_info_list, unsigned int cmk_id, unsigned int cmk_plain_len, 
    const Time cmk_create_time, const Time cmk_expired_time)
{
    bool is_list_empty = false;
    CmkInfo *last_node = NULL;
    CmkInfo *new_node = NULL;

    Time today = { 0 };
    bool is_expired = false;
    int days = 0;

    if (cmk_info_list->cmk_id == INVALID_CMK_KD) {
        is_list_empty = true;
        new_node = cmk_info_list;
    } else {
        new_node = (CmkInfo *)malloc(sizeof(CmkInfo));
        if (new_node == NULL) {
            insert_format_log(L_MALLOC_MEMORY, KT_ERROR, "");
            return false;
        }
    }

    /* check is cmk expired */
    if (!get_sys_time(&today)) {
        insert_format_log(L_GET_TIME, KT_ERROR, "");
        return false;
    }
    days = calculate_interval(today, cmk_expired_time);

    new_node->cmk_id = cmk_id;
    new_node->cmk_plain_len = cmk_plain_len;
    timecpy(&(new_node->create_time), cmk_create_time);
    timecpy(&(new_node->expired_time), cmk_expired_time);
    new_node->is_expired = is_expired;
    if (days <= 0) {
        new_node->is_expired = true;
        new_node->expired_days = -days;
    } else {
        new_node->is_expired = false;
        new_node->expired_days = 0;
    }
    new_node->next = NULL;

    if (!is_list_empty) {
        last_node = cmk_info_list;
        while (last_node->next != NULL) {
            last_node = last_node->next;
        }
        last_node->next = new_node;
    }

    return true;
}

void trav_cmk_info_list(CmkInfo *cmk_info_list)
{
    char table_line[15] = {0};
    CmkInfo *cur = cmk_info_list;
    unsigned int cmk_cnt = 0;
    char create_date_str[strlen("0000-00-00/")] = {0};
    char create_time_str[strlen("00:00:00/")] = {0};
    char expired_date_str[strlen("0000-00-00/")] = {0};

    if (cmk_info_list != NULL && cmk_info_list->cmk_id == INVALID_CMK_KD) {
        printf("no cmk found.\n");
        return;
    }

    make_table_line(table_line, 15);
    printf(" %10s | %10s | %15s | %15s | %15s\n", "cmk id", "cmk length", "create date UTC", "create time UTC",
        "expired date UTC");
    printf(" %10s-+-%10s-+-%15s-+-%15s-+-%15s\n", "----------", "----------", table_line, table_line, table_line);

    while (cur != NULL) {
        cmk_cnt++;
        date_ttos(cur->create_time, create_date_str, sizeof(create_date_str));
        time_ttos(cur->create_time, create_time_str, sizeof(create_time_str));
        date_ttos(cur->expired_time, expired_date_str, sizeof(expired_date_str));

        printf(" %10u | %10u | %15s | %15s | %15s", cur->cmk_id, cur->cmk_plain_len, create_date_str, create_time_str,
            expired_date_str);

        if (cur->is_expired) {
            printf(" (WARNING : EXPIRED %d days)\n", cur->expired_days);
        } else {
            printf("\n");
        }

        cur = cur->next;
    }

    printf("(%u Rows)\n", cmk_cnt);
}

void destroy_cmk_info_list(CmkInfo *cmk_info_list)
{
    CmkInfo *to_free = NULL;

    while (cmk_info_list != NULL) {
        to_free = cmk_info_list;
        cmk_info_list = cmk_info_list->next;
        free(to_free);
    }
}

void print_rk_info(const RkInfo *rk_info)
{
    char table_line[15] = {0};
    char create_date_str[strlen("0000-00-00/")] = {0};
    char create_time_str[strlen("00:00:00/")] = {0};
    char expired_date_str[strlen("0000-00-00/")] = {0};

    date_ttos(rk_info->create_time, create_date_str, sizeof(create_date_str));
    time_ttos(rk_info->create_time, create_time_str, sizeof(create_time_str));
    date_ttos(rk_info->expired_time, expired_date_str, sizeof(expired_date_str));

    make_table_line(table_line, 15);
    printf(" %15s | %15s | %15s\n", "create date UTC", "create time UTC", "expired date UTC");
    printf(" %15s-+-%15s-+-%15s\n",  table_line, table_line, table_line);
    printf(" %15s | %15s | %15s\n", create_date_str, create_time_str, expired_date_str);

    if (rk_info->is_expired) {
        printf("\nWARNNING: your root key has expired %d days.\n", rk_info->expired_days);
        printf("HINT: you can use following conmmand to cycle root key:");
        printf("        gs_ktool -R -u\n");
    }
}

bool generate_cmk(const bool has_set_len, unsigned int key_len, unsigned int *key_id)
{
    KmcCfgDomainInfo domain_info = { 0 };
    KmcCfgKeyType k_type = { 0 };
    unsigned int cmk_plain_len = 0;

    domain_info = { DEFAULT_DOMAIN, KMC_MK_GEN_BY_INNER, { 0 }, KMC_DOMAIN_TYPE_SHARE, 0 };
    errno_t rc = strcpy_s(domain_info.desc, sizeof(domain_info.desc), "CMK");
    securec_check_c(rc, "", "");

    if (has_set_len) {
        cmk_plain_len = key_len;
    } else {
        cmk_plain_len = g_conf.cmk_length;
    }

    k_type = { KMC_KEY_TYPE_ENCRPT_INTEGRITY, cmk_plain_len, g_conf.cmk_validity, 0 };

    return_false_if(KmcAddDomainEx(&domain_info) != WSEC_SUCCESS);
    return_false_if(KmcAddDomainKeyTypeEx(DEFAULT_DOMAIN, &k_type) != WSEC_SUCCESS);
    
    *key_id = KmcGetMkCount();
    if (*key_id >= MAX_CMK_QUANTITY) {
        insert_format_log(L_GEN_CMK, KT_ERROR, "the number of CMKs exceeds the upper limit : %d.", MAX_CMK_QUANTITY);
        return false;
    }
    
    return (KmcCreateMkEx(DEFAULT_DOMAIN, key_id) == WSEC_SUCCESS);
}

bool delete_cmk(const bool has_set_all, unsigned int cmk_id, unsigned int del_cmk_id_list[], unsigned int *del_cmk_cnt)
{
    KmcCfgDomainInfo domain_info = { 0 };
    KmcMkInfo kmc_cmk_info = { 0 };
    unsigned int all_cmk_cnt = 0;

    domain_info = { DEFAULT_DOMAIN, KMC_MK_GEN_BY_INNER, { 0 }, KMC_DOMAIN_TYPE_SHARE, 0 };
    if (KmcAddDomainEx(&domain_info) != WSEC_SUCCESS) {
        return false;
    }

    /* delete one cmk (user has specificed the cmk id) */
    if (!has_set_all) {
        return (KmcRmvMk(DEFAULT_DOMAIN, cmk_id) == WSEC_SUCCESS);
    } else { /* delete all cmk */
        all_cmk_cnt = KmcGetMkCount();
        for (unsigned int i = 0; i < all_cmk_cnt; i++) {
            if (KmcGetMk(1, &kmc_cmk_info) == WSEC_SUCCESS && kmc_cmk_info.domainId == DEFAULT_DOMAIN) {
                return_false_if(KmcRmvMk(DEFAULT_DOMAIN, kmc_cmk_info.keyId) != WSEC_SUCCESS);
                del_cmk_id_list[*del_cmk_cnt] = kmc_cmk_info.keyId;
                (*del_cmk_cnt)++;
            }
        }
        return true;
    }

    return false;
}

bool select_cmk_info(const bool has_set_all, unsigned int key_id, CmkInfo *cmk_info_list)
{
    KmcMkInfo kmc_cmk_info = { 0 };
    /* only used for call the func of selecting cmk plian len */
    unsigned char cmk_plain[MAX_CMK_LEN] = {0};
    unsigned int cmk_plain_len = 0;
    Time cmk_create_time = { 0 };
    Time cmk_expired_time = { 0 };
    errno_t rc = 0;

    return_false_if(cmk_info_list == NULL);

    /* select one cmk (user has specificed the cmk id) */
    if (!has_set_all) {
        for (int i = 0; i < KmcGetMkCount(); i++) {
            if (KmcGetMk(i, &kmc_cmk_info) == WSEC_SUCCESS && kmc_cmk_info.domainId == DEFAULT_DOMAIN) {
                if (kmc_cmk_info.keyId == key_id) {
                    cmk_plain_len = sizeof(cmk_plain);
                    return_false_if(KmcGetMkDetail(DEFAULT_DOMAIN, kmc_cmk_info.keyId, &kmc_cmk_info, cmk_plain,
                        &cmk_plain_len) != WSEC_SUCCESS);
                    rc = memset_s(cmk_plain, MAX_CMK_LEN, 0, sizeof(cmk_plain));
                    securec_check_c(rc, "", "");
                    trans_wsectime_to_time(kmc_cmk_info.mkCreateTimeUtc, &cmk_create_time);
                    trans_wsectime_to_time(kmc_cmk_info.mkExpiredTimeUtc, &cmk_expired_time);
                    add_cmk_info_node(cmk_info_list, kmc_cmk_info.keyId, cmk_plain_len, cmk_create_time,
                        cmk_expired_time);
                    return true;
                }
            }
        }
    } else { /* select all */
        for (int i = 0; i < KmcGetMkCount(); i++) {
            if (KmcGetMk(i, &kmc_cmk_info) == WSEC_SUCCESS && kmc_cmk_info.domainId == DEFAULT_DOMAIN) {
                cmk_plain_len = sizeof(cmk_plain);
                if (KmcGetMkDetail(DEFAULT_DOMAIN, kmc_cmk_info.keyId, &kmc_cmk_info, cmk_plain, &cmk_plain_len)
                    == WSEC_SUCCESS) {
                    rc = memset_s(cmk_plain, MAX_CMK_LEN, 0, sizeof(cmk_plain));
                    securec_check_c(rc, "", "");
                    trans_wsectime_to_time(kmc_cmk_info.mkCreateTimeUtc, &cmk_create_time);
                    trans_wsectime_to_time(kmc_cmk_info.mkExpiredTimeUtc, &cmk_expired_time);
                    add_cmk_info_node(cmk_info_list, kmc_cmk_info.keyId, cmk_plain_len, cmk_create_time,
                        cmk_expired_time);
                }
            }
        }
        return true;
    }

    return false;
}

bool import_cmks_from_ksf(const char *ksf, const unsigned char *ksf_passwd, unsigned int ksf_pass_len)
{
    return (KmcImportMkFileEx(ksf, ksf_passwd, ksf_pass_len) == WSEC_SUCCESS);
}

bool export_cmks_to_ksf(const bool has_set_export_ksf, const char *ksf, const unsigned char *ksf_passwd,
    unsigned int ksf_pass_len)
{
    return_false_if(ksf_passwd == NULL);

    if (has_set_export_ksf) {
        return (KmcExportMkFileEx(KMC_MKF_VER, ksf, ksf_passwd, ksf_pass_len, ITER_ROUND) == WSEC_SUCCESS);
    } else {
        return (KmcExportMkFileEx(KMC_MKF_VER, g_conf.export_ksf, ksf_passwd, ksf_pass_len, ITER_ROUND) == 
            WSEC_SUCCESS);
    }
}

bool select_rk_info(RkInfo *rk_info)
{
    KmcRkAttributes rk_attr = { 0 };
    Time today = { 0 };
    int days = 0;

    return_false_if(KmcGetRootKeyInfo(&rk_attr) != WSEC_SUCCESS);

    trans_wsectime_to_time(rk_attr.rkCreateTimeUtc, &rk_info->create_time);
    trans_wsectime_to_time(rk_attr.rkExpiredTimeUtc, &rk_info->expired_time);

    /* check if cmk expired */
    if (!get_sys_time(&today)) {
        insert_format_log(L_GET_TIME, KT_ERROR, "");
    }
    days = calculate_interval(today, rk_info->expired_time);
    if (days <= 0) {
        rk_info->is_expired = true;
        rk_info->expired_days = -days;
    } else {
        rk_info->is_expired = false;
        rk_info->expired_days = 0;
    }

    return true;
}

bool cycle_rk(void)
{
    return (KmcUpdateRootKey(NULL, 0) == WSEC_SUCCESS);
}