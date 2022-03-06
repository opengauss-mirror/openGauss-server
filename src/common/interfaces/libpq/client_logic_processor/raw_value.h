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
 * raw_value.h
 *
 * IDENTIFICATION
 *	  src\common\interfaces\libpq\client_logic_processor\raw_value.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef RAW_VALUE_H
#define RAW_VALUE_H

#include <string.h>

typedef struct pg_conn PGconn;
typedef unsigned int Oid;

class ICachedColumn;
/*
 * this class is used to process data
 * and to keep track of the positions in the original query - to assist in the re-writing of the query.
 */
class RawValue {
public:
    explicit RawValue(PGconn *conn);
    ~RawValue();
    RawValue(const RawValue &) = delete;
    RawValue &operator = (const RawValue &) = delete;

    bool process(const ICachedColumn *cached_column, char *err_msg);
    void set_data(const unsigned char *data, size_t data_size);
    void set_data_value(const unsigned char *data_value, size_t size)
    {
        m_data_value = data_value;
        m_data_value_size = size;
    }
    bool original_value_emtpy() {
        return (!m_data_value || m_data_size == 0);
    }

    void inc_ref_count();
    void dec_ref_count();
    bool safe_to_delete() {
        return ref_count == 0;
    }
public:
    bool m_is_param;

    size_t m_location;
    size_t m_new_location;

    /* the original data, including quotes, and the original size in the query */
    const unsigned char *m_data;
    size_t m_data_size;

    /* the substring of the original data we want to process */
    const unsigned char *m_data_value;
    size_t m_data_value_size;
    int m_data_value_format;

    /* the processed data */
    unsigned char *m_processed_data;
    size_t m_processed_data_size;

    bool m_empty_repeat;

    /* the connection object */
    PGconn *m_conn;
private:
    void free_processed_data();
    int ref_count;
};
#endif /* RAW_VALUE_H */
