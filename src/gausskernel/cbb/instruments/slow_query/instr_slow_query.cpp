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
 * instr_slow_query.cpp
 *
 * IDENTIFICATION
 *    src/gausskernel/cbb/instruments/slow_query/instr_slow_query.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "postgres.h"
#include "utils/memutils.h"
#include "knl/knl_variable.h"
#include "utils/lsyscache.h"
#include "utils/hsearch.h"
#include "access/hash.h"
#include "access/xact.h"
#include "utils/memutils.h"
#include "miscadmin.h"
#include "pgxc/pgxc.h"
#include "funcapi.h"
#include "parser/analyze.h"
#include "commands/prepare.h"
#include "commands/dbcommands.h"
#include "libpq/pqformat.h"
#include "libpq/libpq.h"
#include "knl/knl_variable.h"
#include "instruments/unique_query.h"
#include "instruments/instr_slow_query.h"
#include "cjson/cJSON.h"
#include "workload/gscgroup.h"
#include "workload/statctl.h"

static inline int str_to_uint64(const char* str, uint64* val)
{
    char* end = NULL;
    uint64 uint64_value = 0;
    uint32 str_len = 0;

    Assert(str != NULL && val != NULL);
    str_len = strlen(str);
    if (str_len == 0) {
        return -1;
    }

    /* clear errno before convert */
    errno = 0;
#ifdef WIN32
    uint64_value = _strtoui64(str, &end, 10);
#else
    uint64_value = strtoul(str, &end, 10);
#endif
    if ((errno != 0) || (end != (str + str_len))) {
        return -1;
    }

    *val = uint64_value;
    return 0;
}

static inline int uint64_to_str(char* str, uint64 val)
{
    char stack[MAX_LEN_CHAR_TO_BIGINT_BUF] = {0};
    int idx = 0;
    int i = 0;
    Assert(str != NULL);
    Assert(val >= 0);

    while (val > 0) {
        stack[idx] = '0' + (val % 10);
        val /= 10;
        idx++;
    }

    while (idx > 0) {
        str[i] = stack[idx - 1];
        i++;
        idx--;
    }
    return i;
}

void WLMSetSessionSlowInfo(WLMStmtDetail* pDetail)
{
    MemoryContext oldContext;
    oldContext = MemoryContextSwitchTo(g_instance.wlm_cxt->workload_manager_mcxt);

    if (pDetail->slowQueryInfo.current_table_counter == NULL) {
        pDetail->slowQueryInfo.current_table_counter = (PgStat_TableCounts*)palloc0_noexcept(sizeof(PgStat_TableCounts));
    }

    if (pDetail->slowQueryInfo.current_table_counter == NULL) {
        ereport(LOG, (errmsg("Cannot allocate memory for current_table_counter, set session slow query info failed.")));
        MemoryContextSwitchTo(oldContext);
        return;
    }

    if (pDetail->slowQueryInfo.localTimeInfoArray == NULL) {
        pDetail->slowQueryInfo.localTimeInfoArray = (int64*)palloc0_noexcept(sizeof(int64) * TOTAL_TIME_INFO_TYPES);
    }

    if (pDetail->slowQueryInfo.localTimeInfoArray == NULL) {
        ereport(LOG, (errmsg("Cannot allocate memory or localTimeInfoArray, set session slow query info failed.")));
        MemoryContextSwitchTo(oldContext);
        return;
    }

    MemoryContextSwitchTo(oldContext);

    if (u_sess->slow_query_cxt.slow_query.current_table_counter != NULL) {
        int rc = memcpy_s(pDetail->slowQueryInfo.current_table_counter, sizeof(PgStat_TableCounts),
                          u_sess->slow_query_cxt.slow_query.current_table_counter, sizeof(PgStat_TableCounts));
        securec_check(rc,"\0","\0");
    }

    if (u_sess->slow_query_cxt.slow_query.localTimeInfoArray != NULL) {
        for (int idx = 0; idx < TOTAL_TIME_INFO_TYPES; idx++) {
            pDetail->slowQueryInfo.localTimeInfoArray[idx] += u_sess->slow_query_cxt.slow_query.localTimeInfoArray[idx];
        }
    }

    if (u_sess->slow_query_cxt.slow_query.n_returned_rows != 0)
        pDetail->slowQueryInfo.n_returned_rows = u_sess->slow_query_cxt.slow_query.n_returned_rows;
}

void ReportQueryStatus(void)
{
    WLMSetCollectInfoStatus(WLM_STATUS_FINISHED);
    u_sess->slow_query_cxt.slow_query.n_returned_rows = 0;
}

