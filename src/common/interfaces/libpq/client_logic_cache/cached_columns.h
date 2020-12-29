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
 * cached_columns.h
 *
 * IDENTIFICATION
 *	  src\common\interfaces\libpq\client_logic_cache\cached_columns.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef CACHED_COLUMNS_H
#define CACHED_COLUMNS_H
#include <algorithm>
#include "icached_columns.h"

class CachedColumns : public ICachedColumns {
public:
    /* *
     * @param order are the columns pushed by their order in table
     * @param dummies is this container created for handling creating columns
     */
    explicit CachedColumns(bool order = false, bool dummies = false, bool is_cached_params = false);
    ~CachedColumns() override;
    void set_order(bool order) override;
    bool push(const ICachedColumn *cached_column) override;
    bool set(size_t index, const ICachedColumn *cached_column) override;
    bool is_empty() const override;
    bool is_to_process() const override;
    void append(const ICachedColumns *other_tmp) override;
    size_t size() const override;
    size_t not_null_size() const override;
    const ICachedColumn *at(size_t pos) const override;

    /* 
     * whether the list is ordered according to the original table order or is accorded 
     * according to the current DML query 
     */
    bool is_in_scheme_order() const override;
    void set_is_in_scheme_order(bool value) override;

    /* A number that divides into another without a remainder. */
    bool is_divisor(size_t value) const override;

private:
    bool m_is_in_scheme_order;
    bool m_is_to_process;
    const ICachedColumn **m_columns;
    size_t m_columns_size;
    bool m_dummies;                            /* is this container created for creating columns */
    bool m_is_cached_params;   /* if this container is being used in prepare stmt to hold params */
};

#endif
