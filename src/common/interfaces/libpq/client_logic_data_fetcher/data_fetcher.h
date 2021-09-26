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
 * data_fetcher.h
 *
 * IDENTIFICATION
 * 	  src\common\interfaces\libpq\client_logic_data_fetcher\data_fetcher.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef DATA_FETCHER_H_
#define DATA_FETCHER_H_

#include "cursor_interface.h"
#include <cstddef>

/* ! \class DataFetcher
    \brief Wrap the cursor interface instance to make it pass by value and easier to use
*/
class DataFetcher {
public:
    explicit DataFetcher(CursorInterface *);
    virtual ~DataFetcher();
    bool next();
    const char *operator[](int col) const;
    int get_column_index(const char *column_name) const;
    bool load(const char *query);

private:
    CursorInterface *m_cursor = NULL;
};

#endif /* DATA_FETCHER_H_ */
