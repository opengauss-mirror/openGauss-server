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
 * register_gram_check.cpp
 *      we support users to register any number of key management tool/components/services, all of which
 *      have the right to process the CREATE CMKO statement.
 *      if a CREATE CMKO statement is not processed at all, we agree that it's a wrong and unexpected statement.
 *      so, we should put this grammar check at the end of hook function list, to make sure the unexpected SQL statement
 *      can be reported.
 * 
 * IDENTIFICATION
 *	  src/common/interfaces/libpq/client_logic_hooks/cmk_entity_manager_hooks/register_gram_check.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "register_gram_check.h"
#include "reg_hook_frame.h"

static ProcessPolicy create_cmk_obj_hookfunc(CmkIdentity *cmk_identity)
{
    if (cmk_identity->cmk_store == NULL) {
        cmkem_errmsg("failed to create client master key, failed to find arg: KEY_STORE.");
    } else {
        cmkem_errmsg("key store type '%s' is not supported.", cmk_identity->cmk_store);
    }
    
    return POLICY_ERROR;
}

static ProcessPolicy encrypt_cek_plain_hookfunc(CmkemUStr *cek_plain, CmkIdentity *cmk_identity, CmkemUStr **cek_cipher)
{
    cmkem_errmsg("failed to find client master key to encrypt column encryption key.");
    return POLICY_ERROR;
}

static ProcessPolicy decrypt_cek_cipher_hookfunc(CmkemUStr *cek_cipher, CmkIdentity *cmk_identity,
    CmkemUStr **cek_plain)
{ 
    cmkem_errmsg("failed to find client master key to decrypt column encryption key.");
    return POLICY_ERROR;
}

int reg_grammar_check_main() 
{
    CmkEntityManager gramm_check = {
        create_cmk_obj_hookfunc,
        encrypt_cek_plain_hookfunc,
        decrypt_cek_cipher_hookfunc,
        NULL, /* drop_cmk_obj_hook_func: no need */
        NULL, /* post_create_cmk_obj_hook_func: no need */
    };

    return (reg_cmk_entity_manager(gramm_check) == CMKEM_SUCCEED) ? 0 : -1;
}
