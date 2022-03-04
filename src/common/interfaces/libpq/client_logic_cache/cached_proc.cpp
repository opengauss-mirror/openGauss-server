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
 * cached_proc.cpp
 *
 * IDENTIFICATION
 *	  src\common\interfaces\libpq\client_logic_cache\cached_proc.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "cached_proc.h"
#include "libpq-fe.h"
#include "libpq-int.h"
#include "client_logic_common/client_logic_utils.h"

/*
 * @Description checks if param is used as output parameter
 * @Param param_mode
 * @Return true if param is output param, else false
 */
static const bool is_output_param(const char param_mode)
{
    return param_mode == FUNC_PARAM_OUT || param_mode == FUNC_PARAM_INOUT || param_mode == FUNC_PARAM_TABLE;
}

CachedProc::~CachedProc()
{
    if (m_proargnames) {
        for (size_t i = 0; i < m_nargnames; i++) {
            if (m_proargnames[i])
                libpq_free(m_proargnames[i]);
        }
    }
    libpq_free(m_proname);
    libpq_free(m_schema_name);
    libpq_free(m_dbname);
    libpq_free(m_proargnames);
    libpq_free(m_proargcachedcol);
    libpq_free(m_proargtypes);
    libpq_free(m_proallargtypes);
    libpq_free(m_proallargtypes_orig);
    libpq_free(m_proargmodes);
    if (m_original_ids) {
        libpq_free(m_original_ids);
        m_original_ids = NULL;
    }
}

void CachedProc::set_original_ids()
{
    if (m_original_ids == NULL) {
        m_original_ids = (int*)malloc(get_num_processed_args() * sizeof(int));
        if (m_original_ids == NULL) {
            printfPQExpBuffer(&m_conn->errorMessage, libpq_gettext("cannot allocate memory for m_original_ids\n"));
            return;
        }
        for (size_t i = 0; i < get_num_processed_args(); i++) {
            m_original_ids[i] = get_original_id(i);
        }
    }
}

const Oid CachedProc::get_original_id(const size_t idx) const
{
    if (idx >= m_nallargtypes || !m_proallargtypes) {
        return InvalidOid;
    }
    size_t index = 0;
    for (size_t i = 0; i < m_nallargtypes; i++) {
        /* Since this function is for use on response, for deprocessing the result set, input parms should be skipped */
        if (is_clientlogic_datatype(m_proallargtypes[i]) && is_output_param(m_proargmodes[i])) {
            if (index == idx) {
                return m_proallargtypes_orig[i];
            }
            index++;
        }
    }
    return InvalidOid;
}
