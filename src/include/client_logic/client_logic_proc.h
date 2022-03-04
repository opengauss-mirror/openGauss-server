/* -------------------------------------------------------------------------
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
 * client_logic_proc.h
 *
 * IDENTIFICATION
 *	  src\include\client_logic\client_logic_proc.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef CLIENT_LOGIC_PROC_H
#define CLIENT_LOGIC_PROC_H

#include "stdint.h"
#include "datatypes.h"
#include "postgres_ext.h"
#include "access/htup.h"

void add_rettype_orig(const Oid func_id, const Oid ret_type, const Oid res_type);
void add_allargtypes_orig(const Oid func_id, Datum* all_types_orig, Datum* all_types, const int tup_natts, const Oid relid = InvalidOid);
void record_proc_depend(const Oid func_id, const Oid gs_encrypted_proc_id);
void verify_rettype_for_out_param(const Oid func_id);
void delete_proc_client_info(HeapTuple);
void delete_proc_client_info(Oid func_id);

#endif
