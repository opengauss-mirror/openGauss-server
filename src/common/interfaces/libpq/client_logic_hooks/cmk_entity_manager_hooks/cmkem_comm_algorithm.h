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
 * cmkem_comm_algorithm.h
 *      some general encryption and decryption function.
 *      you can use them to encrypt and decrypt your data, CEK entity and CMK entity.
 * 
 * IDENTIFICATION
 *	  src/common/interfaces/libpq/client_logic_hooks/cmk_entity_manager_hooks/cmkem_comm_algorithm.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef CMEKM_COMM_ALGORITHM
#define CMEKM_COMM_ALGORITHM

#include <openssl/rsa.h>
#include "cmkem_comm.h"

const int AES256_KEY_BUF_LEN = 257;
const int AES256_KEY_LEN = 256;
const int RSA2048_KEN_LEN = 2048;
const int RSA3072_KEN_LEN = 2048;
const int SHA256_HASH_LEN = 32;

const int MAX_ASYMM_KEY_BUF_LEN = 3072;

typedef enum {
    UNKNOWN_ALGO,
    AES_256_CBC,
    SM4,
    RSA_2048,
    RSA_3072,
    SM2,
} AlgoType;

typedef enum {
    PUBLIC_KEY,
    PRIVATE_KEY,
} AsymmetricKeyType;

extern size_t get_key_len_by_algo(AlgoType cmk_algo);
extern AlgoType get_algo_by_str(const char *algo_str);
extern CmkemErrCode get_hex_join_hash_from_ustr(CmkemUStr *ustr, CmkemStr **hex_join_hash);
extern CmkemErrCode get_ustr_from_hex_join_hash(CmkemStr *hex_join_hash, CmkemUStr **ustr);

extern CmkemErrCode encrypt_with_symm_algo(AlgoType algo, CmkemUStr *cek_plain, CmkemUStr *cmk_plain,
    CmkemUStr **cek_cipher);
extern CmkemErrCode decrypt_with_symm_algo(AlgoType algo, CmkemUStr *cek_cipher, CmkemUStr *cmk_plain,
    CmkemUStr **cek_plain);

extern CmkemErrCode conv_advustr_to_rsakey(CmkemUStr *key, AsymmetricKeyType rsa_key_type, RSA **rsa_key);
extern RSA *create_rsa_keypair(size_t rsa_key_len);
extern CmkemErrCode write_rsa_keypair_to_biobuf(RSA *rsa_key_pair, BIO **pub_key_biobuf, BIO **priv_key_biobuf);
extern CmkemErrCode read_rsa_key_from_biobuf(BIO *key_biobuf, CmkemUStr **key);

extern CmkemErrCode encrypt_with_rsa2048_pub_key(CmkemUStr *cek_plain, CmkemUStr *cmk_plain, CmkemUStr **cek_cipher);
extern CmkemErrCode decrypt_with_rsa2048_priv_key(CmkemUStr *cek_cipher, CmkemUStr *cmk_plain, CmkemUStr **cek_plain);

extern CmkemErrCode join_sha256(CmkemUStr *data, CmkemUStr **data_join_sha256);
extern CmkemErrCode strip_sha256(CmkemUStr *data_join_sha256, CmkemUStr **data);

#endif /* CMEKM_COMM_ALGORITHM */
