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
 * record_processor.cpp
 *
 * IDENTIFICATION
 *	  src\common\interfaces\libpq\client_logic_processor\record_processor.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "record_processor.h"
#include "cl_state.h"
#include "libpq-int.h"
#include <cstring>
#include "values_processor.h"
#include "client_logic_cache/icached_rec.h"
#include "client_logic_cache/dataTypes.def"
#include "client_logic_cache/icached_column_manager.h"

bool RecordProcessor::DeProcessRecord(PGconn* conn, const char* processed_data, size_t processed_data_size,
    const int* original_typesid, const size_t original_typesid_size, int format,  unsigned char** plain_text,
    size_t& plain_text_size, bool* is_decrypted)
{
    DecryptDataRes dec_dat_res = DEC_DATA_ERR;
    if (!original_typesid) {
        fprintf(stderr, "failed to find original ids of record");
        return false;
    }
    const char* pdata = strstr(processed_data, "\\x");
    if (pdata == NULL) {
        return true;
    }
    size_t newsize = 0;
    char* result = strndup(processed_data, pdata - processed_data - strlen("\"\\"));
    char* new_res = NULL;
    if (result == NULL) {
        fprintf(stderr, "allocation failure when trying to deprocess record\n");
        return false;
    }
    const char* end = NULL;
    int idx = 0;
    bool first = true;
    while (pdata && (size_t) idx < original_typesid_size) {
        if (!first) {
            /* if this is not the first variable in the row, we need to add comma and space */
            size_t cur_size = strlen(result);
            /* 1: the count of char ',' between 2 results, such as 'a,b' */
            const int char_num = m_NULL_TERMINATION_SIZE;
            new_res = (char*)libpq_realloc(result, cur_size, cur_size + char_num + m_NULL_TERMINATION_SIZE);
            if (new_res == NULL) {
                libpq_free(result);
                fprintf(stderr, "allocation failure when trying to deprocess record\n");
                return false;
            }
            result = new_res;
            result[cur_size] = ',';
            result[cur_size + char_num] = '\0';
        }
        first = false;
        end = strstr(pdata, "\"");
        if (end == NULL) {
            libpq_free(result);
            return false;
        }
        unsigned char* plain = NULL;
        size_t plain_size = 0;
        int original_id = original_typesid[idx];
        ProcessStatus process_status = ONLY_VALUE;
        dec_dat_res = ValuesProcessor::deprocess_value(conn, (unsigned char*)pdata, end - pdata, original_id, format,
            &plain, plain_size, process_status);
        if (dec_dat_res == DEC_DATA_SUCCEED) {
            *is_decrypted = true;
            size_t oldsize = strlen(result);
            newsize = oldsize + plain_size + m_NULL_TERMINATION_SIZE;
            if (original_id == BYTEAOID) {
                newsize += m_BYTEA_PREFIX_SIZE;
            }
            new_res = (char*)libpq_realloc(result, oldsize + m_NULL_TERMINATION_SIZE, newsize);
            if (new_res == NULL) {
                libpq_free(result);
                libpq_free(plain);
                fprintf(stderr, "allocation failure when trying to deprocess record\n");
                return false;
            }
            result = new_res;
            if (original_id != BYTEAOID) {
                check_strncat_s(strncat_s(result, newsize, (char*)plain, plain_size));
            } else {
                size_t result_size = strlen(result) + m_NULL_TERMINATION_SIZE;
                result[result_size] = '"';
                result[result_size + m_NULL_TERMINATION_SIZE] = '\0';
                const char* bytea_begin = "\"\\";
                check_strncat_s(strncat_s(result, newsize, bytea_begin, m_BYTEA_PREFIX_SIZE));
                check_strncat_s(strncat_s(result, newsize, (char*)plain, plain_size));
                end--; /* for the quote will be copied */
            }
            libpq_free(plain);
            idx++; /* go forward only in case of success to avoid  standard bytea */
        } else if (dec_dat_res == CLIENT_CACHE_ERR) {
            conn->client_logic->isInvalidOperationOnColumn = true;
        } else {
            libpq_free(result);
            return false;
        }
        pdata = ((end != NULL) ? strstr(end, "\\x") : NULL);
    }
    if (end) {
        end++;
        size_t oldsize = strlen(result);
        newsize = oldsize + processed_data_size - (end - processed_data) + m_NULL_TERMINATION_SIZE;
        new_res = (char*)libpq_realloc(result, oldsize + 1, newsize);
        if (new_res == NULL) {
            libpq_free(result);
            fprintf(stderr, "allocation failure when trying to deprocess record\n");
            return false;
        }
        result = new_res;
        check_strncat_s(strncat_s(result, newsize, end, processed_data_size - (end - processed_data)));
        result[newsize - m_NULL_TERMINATION_SIZE] = '\0';
    }
    *plain_text = (unsigned char*)result;
    plain_text_size = newsize;
    return true;
}
