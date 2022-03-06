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
 * raw_values_list.cpp
 *
 * IDENTIFICATION
 *	  src\common\interfaces\libpq\client_logic_processor\raw_values_list.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "raw_values_list.h"
#include "raw_value.h"
#include "../client_logic_common/statement_data.h"
#include "libpq-int.h"

RawValuesList::RawValuesList(const bool should_free_values) : m_raw_values(NULL),
                                                              m_raw_values_size(0),
                                                              m_should_free_values(should_free_values) {
}

RawValuesList::~RawValuesList()
{
    clear();
}

void RawValuesList::clear()
{
    if (m_should_free_values) {
        for (size_t i = 0; i < m_raw_values_size; ++i) {
            safe_delete(i);
        }
    }

    if (m_raw_values != NULL) {
        free(m_raw_values);
        m_raw_values = NULL;
    }
    m_raw_values_size = 0;
}

bool RawValuesList::add(RawValue *raw_value)
{
    /* input of NULL is legal */
    m_raw_values = (RawValue **)libpq_realloc(m_raw_values, sizeof(*m_raw_values) * m_raw_values_size,
        sizeof(*m_raw_values) * (m_raw_values_size + 1));
    if (m_raw_values == NULL) {
        return false;
    }
    if (raw_value) {
        raw_value->inc_ref_count();
    }
    m_raw_values[m_raw_values_size] = raw_value;
    ++m_raw_values_size;
    return true;
}

RawValue *RawValuesList::at(size_t pos) const
{
    if (pos >= m_raw_values_size) {
        return NULL;
    }
    return m_raw_values[pos];
}

size_t RawValuesList::size() const
{
    return m_raw_values_size;
}

bool RawValuesList::empty() const
{
    return (m_raw_values_size == 0);
}

bool RawValuesList::resize(size_t size)
{
    if (size < m_raw_values_size) {
        return true;
    }
    m_raw_values = (RawValue **)libpq_realloc(m_raw_values, sizeof(*m_raw_values) * m_raw_values_size,
        sizeof(*m_raw_values) * (size));
    if (m_raw_values == NULL) {
        return false;
    }

    /* fill new elements with NULLs */
    errno_t rc = EOK;
    rc = memset_s(&m_raw_values[m_raw_values_size], sizeof(*m_raw_values) * (size - m_raw_values_size), 0,
        sizeof(*m_raw_values) * (size - m_raw_values_size));
    securec_check_c(rc, "\0", "\0");
    /* new size */
    m_raw_values_size = size;
    return true;
}

bool RawValuesList::set(size_t pos, RawValue *raw_value)
{
    if (pos >= m_raw_values_size) {
        return false;
    }
    if (m_raw_values[pos] != NULL) {
        return false;
    }
    if (raw_value) {
        raw_value->inc_ref_count();
    }
    m_raw_values[pos] = raw_value;
    return true;
}

void RawValuesList::safe_delete(size_t pos)
{
    if (m_raw_values[pos]) {
        m_raw_values[pos]->dec_ref_count();
        if (m_raw_values[pos]->safe_to_delete()) {
            delete m_raw_values[pos];
            m_raw_values[pos] = NULL;
        }
    }
}

bool RawValuesList::erase(size_t pos, bool is_delete)
{
    if (pos >= m_raw_values_size) {
        return false;
    }

    safe_delete(pos);
    if (!is_delete) {
        m_raw_values[pos] = NULL;
        return true;
    }

    std::swap(m_raw_values[pos], m_raw_values[m_raw_values_size - 1]);
    m_raw_values[m_raw_values_size - 1] = NULL;
    --m_raw_values_size;
    return true;
}

int RawValuesList::partition_by_location(int lo, int hi)
{
    RawValue *raw_value_pivot = m_raw_values[hi];

    /* this function should not be called where there are still NULL objects inside */
    if (raw_value_pivot == NULL) {
        Assert(false);
        return -1;
    }

    int i = (lo - 1);
    for (int j = lo; j <= hi; ++j) {
        RawValue *raw_value = m_raw_values[j];
        if (raw_value == NULL) {
            Assert(false);
            continue;
        }
        if (raw_value->m_location < raw_value_pivot->m_location) {
            ++i;
            std::swap(m_raw_values[i], m_raw_values[j]);
        }
    }
    std::swap(m_raw_values[i + 1], m_raw_values[hi]);
    return (i + 1);
}

void RawValuesList::quicksort_by_location(int lo, int hi)
{
    if (lo < hi) {
        int p = partition_by_location(lo, hi);
        quicksort_by_location(lo, p - 1);
        quicksort_by_location(p + 1, hi);
    }
}

void RawValuesList::sort_by_location()
{
    quicksort_by_location(0, (int)m_raw_values_size - 1);
}

void RawValuesList::merge_from(const RawValuesList *other)
{
    if (!other) {
        return;
    }
    for (size_t i = 0; i < other->size(); i++) {
        add(other->at(i));
    }
}

bool RawValuesList::gen_values_from_statement(const StatementData *statement_data)
{
    resize(statement_data->nParams);
    for (size_t param_num = 0; param_num < statement_data->nParams; ++param_num) {
        RawValue *raw_value = new (std::nothrow) RawValue(statement_data->conn);
        if (raw_value == NULL) {
            fprintf(stderr, "failed to new RawValue object\n");
            return false;
        }
        raw_value->m_is_param = true;
        raw_value->m_location = param_num; /* func : do not reset this variable. it's confusing. */
        if (statement_data->paramValues[param_num]) {
            raw_value->set_data((const unsigned char *)statement_data->paramValues[param_num],
                statement_data->paramLengths ? statement_data->paramLengths[param_num] :
                                               strlen(statement_data->paramValues[param_num]));
        }
        raw_value->m_data_value_format = statement_data->paramFormats ? statement_data->paramFormats[param_num] : 0;
        set(param_num, raw_value);
    }
    return true;
}
