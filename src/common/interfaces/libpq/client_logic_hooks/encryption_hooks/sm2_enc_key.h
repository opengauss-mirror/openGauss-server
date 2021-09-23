/*
 * Copyright (c) 2021 Huawei Technologies Co.,Ltd.
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
 * sm2_enc_key.h
 *
 * IDENTIFICATION
 *	  src\common\interfaces\libpq\client_logic_hooks\encryption_hooks\sm2_enc_key.h
 *
 * -------------------------------------------------------------------------
 */
 
#ifndef SM2_ENC_KEY
#define SM2_ENC_KEY

#include "../cmk_entity_manager_hooks/cmkem_comm.h"

typedef struct Sm2KeyPair {
    CmkemUStr *pub_key;
    CmkemUStr *priv_key;
} Sm2KeyPair;

Sm2KeyPair* generate_encrypt_pair_key();
CmkemErrCode encrypt_with_sm2_pubkey(CmkemUStr *plain, CmkemUStr *pub_key, CmkemUStr **cipher);
CmkemErrCode decrypt_with_sm2_privkey(CmkemUStr *cipher, CmkemUStr *priv_key, CmkemUStr **plain);

#endif /* SM2_ENC_KEY */
