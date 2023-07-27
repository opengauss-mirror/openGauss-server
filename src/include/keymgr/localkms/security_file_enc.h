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
 * security_file_enc.h
 *
 * IDENTIFICATION
 *	  src/gausskernel/security/keymgr/include/localkms/security_file_enc.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef _KM_FILE_ENC_H_
#define _KM_FILE_ENC_H_

#include "keymgr/localkms/security_cmkem_comm_algorithm.h"

typedef enum {
    CREATE_KEY_FILE,
    READ_KEY_FILE
} CheckKeyFileType;

typedef enum {
    PUB_KEY_FILE,
    PRIV_KEY_FILE,
    PUB_KEY_ENC_IV_FILE,
    PRIV_KEY_ENC_IV_FILE,
} LocalkmsFileType;

CmkemErrCode set_global_env_value();
void get_file_path_from_cmk_id(const char *cmk_id_str, LocalkmsFileType file_type, char *file_path_buf, size_t buf_len);
CmkemErrCode check_file_exist(const char *real_file_path, CheckKeyFileType chk_type);
CmkemErrCode encrypt_and_write_key(const char *key_file_path, CmkemUStr *key_plain);
CmkemErrCode read_and_decrypt_cmk(const char *key_path, AsymmetricKeyType key_type, CmkemUStr **key_palin);

#endif /* __KM_FILE_ENC_H__ */
