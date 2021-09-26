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
 * register_local_kms.h
 *      localkms is a lightweight key management component.
 *      different from other key management tools and services, localkms automatically generates CMKE when CREATE CMKO,
 *      it cannot manage key entities independently.
 *      localkms use openssl to generate cmk, then encrypt cmk plain and store cmk cipher in file.
 *      at the same time, we need to store/read the iv and salt that are used to derive a key to
 *      encrypt/decrypt cmk plain.
 * 
 * IDENTIFICATION
 *	  src/common/interfaces/libpq/client_logic_hooks/cmk_entity_manager_hooks/register_local_kms.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef REGISTER_LOCAL_KMS_H
#define REGISTER_LOCAL_KMS_H

extern int reg_cmke_manager_local_kms_main();

#endif /* REG_LOCALKMS_H */
