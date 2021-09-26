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
 * func_name_data.h
 *
 * IDENTIFICATION
 * src\common\interfaces\libpq\client_logic_expressions\func_name_data.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef FUNC_NAME_DATA_H
#define FUNC_NAME_DATA_H

#include "libpq-int.h"
class func_name_data {
public:
    func_name_data();
    ~func_name_data(){}
    NameData m_catalogName;
    NameData m_schemaName;
    NameData m_functionName;

private:
};
#endif