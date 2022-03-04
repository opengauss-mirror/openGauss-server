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
 * raw_values_list.h
 *
 * IDENTIFICATION
 *	  src\common\interfaces\libpq\client_logic_processor\raw_values_list.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef RAW_VALUES_LIST_H
#define RAW_VALUES_LIST_H

#include "c.h"

class RawValue;
class StatementData;

class RawValuesList {
public:
    RawValuesList(const bool should_free_values = true);
    ~RawValuesList();
    bool add(RawValue *raw_value);
    RawValue *at(size_t pos) const;
    size_t size() const;
    bool empty() const;
    bool resize(size_t size);
    bool set(size_t pos, RawValue *raw_value);
    bool erase(size_t pos, bool is_delete_object);
    void sort_by_location();
    void clear();
    void merge_from(const RawValuesList* other);
    RawValue **m_raw_values;
    bool gen_values_from_statement(const StatementData *statement_data);

private:
    void quicksort_by_location(int lo, int high);
    int partition_by_location(int lo, int high);
    void safe_delete(size_t pos);
    size_t m_raw_values_size;
     bool m_should_free_values;
};

#endif
