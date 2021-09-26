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
 * reg_hook_frame.h
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
 *	  src/common/interfaces/libpq/client_logic_hooks/cmk_entity_manager_hooks/reg_hook_frame.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef REG_HOOK_FRAME_H
#define REG_HOOK_FRAME_H

#include "cmkem_comm.h"

const int MAX_CMK_MANAGER_NUM = 20;
/*
 * if more than one key management tool/component/service is registered, all of them can handle the same SQL syntax
 * so, user need to coordinate how they handle it through the return value: ProcessPolicy
 */
typedef enum {
    POLICY_CONTINUE,
    POLICY_BREAK,
    POLICY_ERROR,
    PLLICY_PROCESSED,
} ProcessPolicy;

/* called while process {SQL: CREATE CLIENT MASTER KEY ...} */
/* called while process {SQL:CREATE COLUMN ENTRYPTION KEY ...} */
/* called while process {SQL: INSERT/UPDATE/SELECT encrypted_column ...} */
/* called while process {SQL: DROP CLIENT MASTER KEY ...} */
typedef ProcessPolicy (*CreateCmkObjectHookFunc)(CmkIdentity *cmk_identity);
typedef ProcessPolicy (*EncryptCekPlainHookFunc)(CmkemUStr *cek_plain, CmkIdentity *cmk_identity,
    CmkemUStr **cek_cipher);
typedef ProcessPolicy (*DecryptCekCipherHookFunc)(CmkemUStr *cek_cipher, CmkIdentity *cmk_identity,
    CmkemUStr **cek_plain);
typedef ProcessPolicy (*DropCmKObjectHookFunc)(CmkIdentity *cmk_identity);
typedef ProcessPolicy (*PostCreateCmkObjectHookFunc)(CmkIdentity *cmk_identity);

typedef struct CmkEntityManager {
    CreateCmkObjectHookFunc crt_cmko_hookfunc;
    EncryptCekPlainHookFunc enc_cek_cipher_hookfunc;
    DecryptCekCipherHookFunc dec_cek_plain_hookfunc;
    DropCmKObjectHookFunc drop_cmko_hookfunc;
    PostCreateCmkObjectHookFunc post_crt_cmko_hookfunc;
} CmkEntityManager;

/* privided to users for registering their own key management tool/component/service */
CmkemErrCode reg_cmk_entity_manager(CmkEntityManager cmke_manager);

/* privided to client for calling users callback function  */
CmkemErrCode create_cmk_obj(CmkIdentity *cmk_identity);
CmkemErrCode encrypt_cek_plain(CmkemUStr *cek_plain, CmkIdentity *cmk_identity, CmkemUStr **cek_cipher);
CmkemErrCode decrypt_cek_cipher(CmkemUStr *cek_cipher, CmkIdentity *cmk_identity, CmkemUStr **cek_plain);
CmkemErrCode drop_cmk_obj(CmkIdentity *cmk_identity);
CmkemErrCode post_create_cmk_obj(CmkIdentity *cmk_identity);

#endif /* REG_HOOK_FRAME_H */
