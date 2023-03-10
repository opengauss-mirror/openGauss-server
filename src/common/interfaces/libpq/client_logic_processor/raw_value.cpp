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
 * raw_value.cpp
 *
 * IDENTIFICATION
 *	  src\common\interfaces\libpq\client_logic_processor\raw_value.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "raw_value.h"
#include "postgres_fe.h"
#include "nodes/pg_list.h"
#include "parser/scanner.h"
#include "client_logic_hooks/hooks_manager.h"
#include "libpq-fe.h"
#include "libpq-int.h"
#include "client_logic_fmt/gs_fmt.h"
#include "client_logic_cache/icached_column.h"

unsigned char *PQescapeByteaCe(PGconn *conn, const unsigned char *from, size_t fromlen, size_t *tolen, bool addquote);

RawValue::RawValue(PGconn *conn)
    : m_is_param(false),
      m_location(0),
      m_new_location(0),
      m_data(nullptr),
      m_data_size(0),
      m_data_value(nullptr),
      m_data_value_size(0),
      m_data_value_format(0),
      m_processed_data(nullptr),
      m_processed_data_size(0),
      m_empty_repeat(false),
      m_conn(conn),
      ref_count(0)
{}

RawValue::~RawValue()
{
    free_processed_data();
}

void RawValue::free_processed_data()
{
    if (m_processed_data != NULL || m_processed_data_size != 0) {
        free(m_processed_data);
        m_processed_data = NULL;
        m_processed_data_size = 0;
    }
}

void RawValue::set_data(const unsigned char *data, size_t data_size)
{
    m_data = data;
    m_data_size = data_size;

    m_data_value = m_data;
    m_data_value_size = m_data_size;
}

bool RawValue::process(const ICachedColumn *cached_column, char *err_msg)
{
    free_processed_data();

    /* no need to process */
    if (!m_data_value || !m_data_size) {
        return true;
    }
    /* fromat different datatype to binary */
    unsigned char *binary(NULL);
    size_t binary_size(0);
    errno_t rcs = EOK;

    if (m_data_value_format == 0) {
        char *text_value(NULL);
        bool allocated = false;

        if (m_data_value[m_data_value_size] == 0) {
            text_value = (char *)m_data_value;
        } else {
            text_value = (char *)malloc((m_data_value_size + 1) * sizeof(char));
            if (text_value == NULL) {
                check_sprintf_s(sprintf_s(err_msg, MAX_ERRMSG_LENGTH, "failed to malloc text value"));
                return false;
            }
            text_value[0] = 0;
            check_strncat_s(
                strncat_s(text_value, m_data_value_size + 1, (const char *)m_data_value, m_data_value_size));
            allocated = true;
        }

        binary = Format::text_to_binary((const PGconn*)m_conn, text_value, cached_column->get_origdatatype_oid(),
            cached_column->get_origdatatype_mod(), &binary_size, err_msg);
        if (allocated) {
            free(text_value);
            text_value = NULL;
        }
        if (binary == NULL) {
            if (strlen(err_msg) == 0) {
                check_sprintf_s(sprintf_s(err_msg, MAX_ERRMSG_LENGTH, "failed to convert text to binary"));
            }
            return false;
        }
    } else {
        binary_size = m_data_size;
        binary = (unsigned char *)malloc(binary_size);
        if (binary == NULL)  {
            if (strlen(err_msg) == 0) {
                check_sprintf_s(sprintf_s(err_msg, MAX_ERRMSG_LENGTH, "failed to malloc memory binary"));
            }
            return false;
        }
        errno_t rcs = EOK;
        rcs = memcpy_s(binary, binary_size, m_data_value, m_data_size);
        securec_check_c(rcs, "", "");
        unsigned char *result = Format::verify_and_adjust_binary(binary, &binary_size,
            cached_column->get_origdatatype_oid(), cached_column->get_origdatatype_mod(), err_msg);
        if (!result) {
            if (strlen(err_msg) == 0) {
                check_sprintf_s(sprintf_s(err_msg, MAX_ERRMSG_LENGTH, "failed to convert text to binary"));
            }
            rcs = memset_s(binary, binary_size, 0, binary_size);
            securec_check_c(rcs, "", "");
            libpq_free(binary);
            return false;
        }
        binary = result;
    }

    m_processed_data = (unsigned char *)calloc(
        HooksManager::get_estimated_processed_data_size(cached_column->get_column_hook_executors(), binary_size) +
        sizeof(Oid),
        sizeof(unsigned char));
    RETURN_IF(m_processed_data == NULL, false);
    int processed_size = HooksManager::process_data(cached_column, cached_column->get_column_hook_executors(), binary,
        binary_size, m_processed_data);
    if (processed_size <= 0) {
        if (strlen(err_msg) == 0) {
            check_sprintf_s(sprintf_s(err_msg, MAX_ERRMSG_LENGTH, "failed to process data"));
        }
        rcs = memset_s(binary, binary_size, 0, binary_size);
        securec_check_c(rcs, "", "");
        libpq_free(binary);
        return false;
    }
    m_processed_data_size = processed_size + strlen("\\x") + strlen("\'\'");

    /* get rid of the evidence */
    rcs = memset_s(binary, binary_size, 0, binary_size);
    securec_check_c(rcs, "", "");
    libpq_free(binary);

    if (!m_is_param || m_data_value_format == 0) {
        /* replace processedData with its escaped version */
        size_t processed_data_size_tmp(0);
        const char *processed_data_tmp = (char *)PQescapeByteaCe(m_conn, (const unsigned char *)m_processed_data,
            m_processed_data_size, &processed_data_size_tmp, !m_is_param);
        free_processed_data();
        if (!processed_data_tmp) {
            if (strlen(err_msg) == 0) {
                check_sprintf_s(sprintf_s(err_msg, MAX_ERRMSG_LENGTH, "failed to escape data"));
            }
            return false;
        }

        m_processed_data = (unsigned char *)processed_data_tmp;
        m_processed_data_size = processed_data_size_tmp -
            1; /* the \0 is counted in the orignal PQescapeByteaCe function, so we need -1 */
    }

    return true;
}

void RawValue::inc_ref_count()
{
    Assert(ref_count >= 0);
    ref_count++;
}
void RawValue::dec_ref_count()
{
    Assert(ref_count > 0);
    ref_count--;
}
