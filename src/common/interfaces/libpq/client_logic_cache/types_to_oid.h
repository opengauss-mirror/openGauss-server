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
 * types_to_oid.h
 *
 * IDENTIFICATION
 *	  src\common\interfaces\libpq\client_logic_cache\types_to_oid.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef TYPES_TO_OID_H
#define TYPES_TO_OID_H

#include "client_logic_common/cstring_oid_map.h"

class TypesMap {
public:
    static CStringOidMap typesTextToOidMap;
};

#endif
