/*
 * Copyright (c) 2021 Huawei Technologies Co.,Ltd.
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 * http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 * -------------------------------------------------------------------------
 *
 * cursor_interface.h
 *
 * IDENTIFICATION
 * 	  src\common\interfaces\libpq\client_logic_data_fetcher\cursor_interface.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef DATAFETCHER_CURSOR_INTERFACE_IMPL_H_
#define DATAFETCHER_CURSOR_INTERFACE_IMPL_H_

/* ! \class CursorInterface
    \brief cursor / result set like interface to be used when fetching data
*/
class CursorInterface {
public:
    virtual bool load(const char *query) = 0;
    virtual bool next() = 0;
    virtual const char *operator[](int col) const = 0;
    virtual int get_column_index(const char *column_name) const = 0;
    virtual ~CursorInterface() {}
    virtual void clear_result() = 0;
};

#endif /* DB_CONN_INCLUDE_CURSOR_INTERFACE_IMPL_H_ */
