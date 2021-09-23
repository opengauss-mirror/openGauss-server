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
 * func_name_data.cpp
 *	
 * IDENTIFICATION
 *	  src\common\interfaces\libpq\client_logic_expressions\func_name_data.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "func_name_data.h"
#include <cstdio>
/*
 * get the column's full FQDN, as full as possible, depending on the amount of information in the ColumnRef
 * column FQDN: "database"."schema"."table"."column"
 */

func_name_data::func_name_data()
{
    m_catalogName.data[0] = '\0';
    m_schemaName.data[0] = '\0';
    m_functionName.data[0] = '\0';
}
