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
 * reg_hook_frame.cpp
 *      we use 3 level key encryption scheme in Client Encrypt:
 *          1. use CEK (Column Encryption Key) to encrypt data
 *          2. use CMK (Client Master Key) to encrypt CEK
 *          3. use RK (Root Key) to encrypt CMK
 *      for CMK, we use CMKO and CMKE to describe it:
 *          1. CMKO: CMK object, create by {SQL: CREATE CLIENT MASTER KEY ...}
 *          2. CMKE: the actual CMK entity, managed by thierd party key management tool/component/service
 *      the CMKO only stores the method of reading and use CMKE.
 *      so, while creating CMKO, you need to use KEY_STORE and KEY_PATH to specify how to read and use CMKE.
 * 
 *      this part provides a flexible hook machanish to support users to register and manage their own CMKE by their
 *      key management tool/component/service.
 * 
 *      if you are familiar with Linux Netfilter or SELiunx, you will know the code logic of this part well soon.
 * 
 * IDENTIFICATION
 *	  src/common/interfaces/libpq/client_logic_hooks/cmk_entity_manager_hooks/reg_hook_frame.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "reg_hook_frame.h"

#include <stdlib.h>
#include "cmkem_comm.h"

static CmkEntityManager cmk_entity_manager_list[MAX_CMK_MANAGER_NUM] = {0};
size_t cmke_manager_cnt = 0;

CmkemErrCode reg_cmk_entity_manager(CmkEntityManager cmke_manager)
{
    if (cmke_manager_cnt >= MAX_CMK_MANAGER_NUM) {
        cmkem_errmsg("the number of cmk managers has reached the maximum: %d.", MAX_CMK_MANAGER_NUM);
        return CMKEM_REGISTRE_FUNC_ERR;
    }

    cmk_entity_manager_list[cmke_manager_cnt] = cmke_manager;
    cmke_manager_cnt++;
    return CMKEM_SUCCEED;
}

/* privided to client */
CmkemErrCode create_cmk_obj(CmkIdentity *cmk_identity)
{
    ProcessPolicy policy = POLICY_CONTINUE;

    if (cmke_manager_cnt == 0) {
        cmkem_errmsg("in hook point: CREATE_CMK_OBJ, no registered hook function found.");
        return CMKEM_REGISTRE_FUNC_ERR;
    }

    for (size_t i = 0; i < cmke_manager_cnt; i++) {
        if (cmk_entity_manager_list[i].crt_cmko_hookfunc == NULL) {
            continue;
        }

        policy = cmk_entity_manager_list[i].crt_cmko_hookfunc(cmk_identity);
        if (policy == POLICY_BREAK) {
            return CMKEM_SUCCEED;
        } else if (policy == POLICY_ERROR) {
            return CMKEM_CREATE_CMK_ERR;
        }
        /* else continue */
    }
    
    return CMKEM_SUCCEED;
}

/* privided to client */
CmkemErrCode encrypt_cek_plain(CmkemUStr *cek_plain, CmkIdentity *cmk_identity, CmkemUStr **cek_cipher)
{
    ProcessPolicy policy = POLICY_CONTINUE;

    if (cmke_manager_cnt == 0) {
        cmkem_errmsg("in hook point: ENCRYPT_CEK_ENTITY, no registered hook function found.");
        return CMKEM_REGISTRE_FUNC_ERR;
    }

    for (size_t i = 0; i < cmke_manager_cnt; i++) {
        if (cmk_entity_manager_list[i].enc_cek_cipher_hookfunc == NULL) {
            continue;
        }

        policy = cmk_entity_manager_list[i].enc_cek_cipher_hookfunc(cek_plain, cmk_identity, cek_cipher);
        if (policy == POLICY_BREAK) {
            return CMKEM_SUCCEED;
        } else if (policy == POLICY_ERROR) {
            return CMKEM_ENCRYPT_CEK_ERR;
        }
        /* else continue */
    }
    
    return CMKEM_SUCCEED;
}

/* privided to client */
CmkemErrCode decrypt_cek_cipher(CmkemUStr *cek_cipher, CmkIdentity *cmk_identity, CmkemUStr **cek_plain)
{
    ProcessPolicy policy = POLICY_CONTINUE;

    if (cmke_manager_cnt == 0) {
        cmkem_errmsg("in hook point: DECRYPT_CEK_ENTITY, no registered hook function found.");
        return CMKEM_REGISTRE_FUNC_ERR;
    }

    for (size_t i = 0; i < cmke_manager_cnt; i++) {
        if (cmk_entity_manager_list[i].dec_cek_plain_hookfunc == NULL) {
            continue;
        }

        policy = cmk_entity_manager_list[i].dec_cek_plain_hookfunc(cek_cipher, cmk_identity, cek_plain);
        if (policy == POLICY_BREAK) {
            return CMKEM_SUCCEED;
        } else if (policy == POLICY_ERROR) {
            return CMKEM_DECRYPT_CEK_ERR;
        }
        /* else continue */
    }
    
    return CMKEM_SUCCEED;
}

/* privided to client */
CmkemErrCode drop_cmk_obj(CmkIdentity *cmk_identity) 
{
    ProcessPolicy policy = POLICY_CONTINUE;

    if (cmke_manager_cnt == 0) {
        cmkem_errmsg("in hook point: DROP_CMK_OBJ, no registered hook function found.");
        return CMKEM_REGISTRE_FUNC_ERR;
    }

    for (size_t i = 0; i < cmke_manager_cnt; i++) {
        if (cmk_entity_manager_list[i].drop_cmko_hookfunc == NULL) {
            continue;
        }

        policy = cmk_entity_manager_list[i].drop_cmko_hookfunc(cmk_identity);
        if (policy == POLICY_BREAK) {
            return CMKEM_SUCCEED;
        } else if (policy == POLICY_ERROR) {
            return CMKEM_DROP_CMK_ERR;
        }
        /* else continue */
    }
    
    return CMKEM_SUCCEED;
}

CmkemErrCode post_create_cmk_obj(CmkIdentity *cmk_identity)
{
    ProcessPolicy policy = POLICY_CONTINUE;

    if (cmke_manager_cnt == 0) {
        cmkem_errmsg("in hook point: POST_CREATE_CMK_OBJ, no registered hook function found.");
        return CMKEM_REGISTRE_FUNC_ERR;
    }

    for (size_t i = 0; i < cmke_manager_cnt; i++) {
        if (cmk_entity_manager_list[i].post_crt_cmko_hookfunc == NULL) {
            continue;
        }

        policy = cmk_entity_manager_list[i].post_crt_cmko_hookfunc(cmk_identity);
        if (policy == POLICY_BREAK) {
            return CMKEM_SUCCEED;
        } else if (policy == POLICY_ERROR) {
            return CMKEM_CREATE_CMK_ERR;
        }
        /* else continue */
    }
    
    return CMKEM_SUCCEED;
}
