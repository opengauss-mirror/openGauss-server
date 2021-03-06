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

#ifndef GS_KT_KEY_MANAGE_H
#define GS_KT_KEY_MANAGE_H

#include "kt_common.h"

const int INVALID_CMK_KD = 0;
const unsigned int DEFAULT_RK_ITER = 10000;

/* key configure limited by both KMC and me  */
const int MAX_CMK_QUANTITY = 4095;
const int MAX_KSF_PASS_BUF_LEN = 256;
const int MIN_KSF_PASS_LEN = 8;
const int ITER_ROUND = 10000;

typedef struct RkInfo {
    Time create_time;
    Time expired_time;
    bool is_expired;
    unsigned int expired_days;
} RkInfo;

typedef struct CmkInfo {
    unsigned int cmk_id;
    unsigned int cmk_plain_len;
    Time create_time;
    Time expired_time;
    bool is_expired;
    unsigned int expired_days;
    struct CmkInfo *next;
} CmkInfo;

/* Description : before opetating cmk and rk, gs_ktool needs to be initialized first. */
extern bool reg_callback_funcs(void);
extern bool load_key_manage_conf(const char required_conf[MAX_CONF_ITEM_QUANTITY][MAX_CONF_KEY_LEN],
    size_t required_conf_cnt);
extern bool has_ksf_existed(void);
extern bool init_or_create_ksf(void);
extern bool init_keytool(void);

extern CmkInfo *init_cmk_info_list();
extern bool add_cmk_info_node(CmkInfo *cmk_info_list, unsigned int cmk_id, unsigned int cmk_plain_len, 
    const Time cmk_create_time, const Time cmk_expired_time);
extern void trav_cmk_info_list(CmkInfo *cmk_info_list);
extern void destroy_cmk_info_list(CmkInfo *cmk_info_list);
extern void print_rk_info(const RkInfo *rk_info);

/* functions to manage cmk : generate, delete, select, import, export. */
extern bool generate_cmk(const bool has_set_len, unsigned int key_len, unsigned int *key_id);
extern bool delete_cmk(const bool has_set_all, unsigned int cmk_id, unsigned int del_cmk_id_list[],
    unsigned int *del_cmk_cnt);
extern CmkInfo *select_cmk_info(const bool has_set_all, unsigned int key_id);
extern bool import_cmks_from_ksf(const char *ksf, const unsigned char *ksf_passwd, unsigned int ksf_pass_len);
extern bool export_cmks_to_ksf(const bool has_set_export_ksf, const char *ksf, const unsigned char *ksf_passwd,
    unsigned int ksf_pass_len);

/* functions to manage rk : select, cycle(update). */
extern bool select_rk_info(RkInfo *rk_info); /* we can use the same Struct to describ rk_info */
extern bool cycle_rk(void);

#endif
