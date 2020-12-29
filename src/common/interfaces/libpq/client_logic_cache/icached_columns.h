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
 * icached_columns.h
 *
 * IDENTIFICATION
 *	  src\common\interfaces\libpq\client_logic_cache\icached_columns.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef ICACHED_COLUMNS_H
#define ICACHED_COLUMNS_H

#include <algorithm>

class ICachedColumn;

class ICachedColumns {
public:
    virtual ~ICachedColumns() = 0;
    /*
     * iterate over columns
     */
    virtual size_t size() const = 0;
    virtual size_t not_null_size() const = 0;
    virtual const ICachedColumn *at(size_t pos) const = 0;
    virtual void set_order(bool order) = 0;

    /*
     * add columns
     */
    virtual bool push(const ICachedColumn *cached_column) = 0;
    virtual bool set(size_t index, const ICachedColumn *cached_column) = 0;
    virtual void append(const ICachedColumns *other_tmp) = 0;

    /* 
     * check if contains any columns
     */
    virtual bool is_empty() const = 0;
    virtual bool is_to_process() const = 0;
    /* 
     * whether the list is ordered according to the original table order or 
     * is accorded according to the current DML query
     */
    virtual bool is_in_scheme_order() const = 0;
    virtual void set_is_in_scheme_order(bool value) = 0;
    /*
     * A number that divides into another without a remainder.
     */
    virtual bool is_divisor(size_t value) const = 0;
};

inline ICachedColumns::~ICachedColumns() {};

#endif
