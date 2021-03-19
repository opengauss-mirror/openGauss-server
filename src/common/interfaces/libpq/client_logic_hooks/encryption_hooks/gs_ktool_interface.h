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
 * gs_ktool_interface.h
 *
 *
 * IDENTIFICATION
 *	  src\common\interfaces\libpq\client_logic_hooks\encryption_hooks\gs_ktool_interface.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef KTOOL_KT_INTERFACES_H
#define KTOOL_KT_INTERFACES_H

#include "gs_ktool/kt_interface.h"
#include "client_logic/client_logic_enums.h"

#define DEFAULT_CMK_LEN 32
#define MAX_KEYPATH_LEN 256

extern bool kt_check_algorithm_type(CmkAlgorithm algo_type);
extern bool kt_atoi(const char *cmk_id_str, unsigned int *cmk_id);

extern bool create_cmk(unsigned int cmk_id);
extern bool read_cmk_plain(const unsigned int cmk_id, unsigned char *cmk_plain);

extern bool encrypt_cek_use_aes256(const unsigned char *cek_plain, size_t cek_plain_size, unsigned char *cmk_plain,
    unsigned char *cek_ciph, size_t &cek_ciph_size);
extern bool decrypt_cek_use_aes256(const unsigned char *cek_cipher, size_t cek_cipher_size, unsigned char *cmk_plain,
    unsigned char *cek_plain, size_t *cek_plain_len);

extern void free_cmk_plain_cache();
#endif