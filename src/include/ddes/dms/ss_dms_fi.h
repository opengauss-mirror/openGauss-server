/*
 * Copyright (c) 2024 Huawei Technologies Co.,Ltd.
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
 * ---------------------------------------------------------------------------------------
 *
 * ss_dms_fi.h
 *        Defines the DMS fault injection function pointer.
 *
 * IDENTIFICATION
 *        src/include/ddes/dms/ss_dms_fi.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef __SS_DMS_FI_H__
#define __SS_DMS_FI_H__

#include "dms_api.h"
#include "ddes/dms/ddes_fault_injection_defs.h"
#include "ddes/dms/ss_dms.h"
#include "utils/elog.h"

#ifdef __cplusplus
extern "C" {
#endif

#define DB_FI_ENTRY_BEGIN      10000
#define DB_FI_ENTRY_COUNT      1024

typedef enum en_db_fi_point_name {
    // if CALL and TRIGGER both in kernel point range [10001, 10799]DDES_FI_ENTRY_END
    DB_FI_CHANGE_BUFFERTAG_BLOCKNUM = DB_FI_ENTRY_BEGIN + 1,
} db_fi_point_name;

// for alloc fi context with size return size
int ss_fi_get_context_size(void);
// set the fi context with the context, and init the context, the context alloced by DB
void ss_fi_set_and_init_context(void *context);

int ss_fi_set_entries(unsigned int type, unsigned int *entries, unsigned int count);
int ss_fi_get_entry_value(unsigned int type);
int ss_fi_set_entry_value(unsigned int type, unsigned int value);
int ss_fi_get_tls_trigger_custom(void);
void ss_fi_set_tls_trigger_custom(int val);
unsigned char ss_fi_entry_custom_valid(unsigned int point);

void ss_fi_change_buffertag_blocknum(const void *ddes_fi_entry, va_list args);

#ifdef USE_ASSERT_CHECKING
#define SS_FAULT_INJECTION_ACTION_TRIGGER_CUSTOM(point, action)                                    \
    do {                                                                                           \
        if (ss_fi_entry_custom_valid(point) && ss_fi_get_tls_trigger_custom() == TRUE) {           \
            ss_fi_set_tls_trigger_custom(FALSE);                                                   \
            ereport(DEBUG1, (errmsg("[KERNEL_FI] fi custom action happens at %s", __FUNCTION__))); \
            action;                                                                                \
        }                                                                                          \
    } while (0)

#define SS_FAULT_INJECTION_CALL(point, ...)                                                        \
    do {                                                                                           \
        if (g_ss_dms_func.inited) {                                                                \
            g_ss_dms_func.ddes_fi_call(point, ##__VA_ARGS__);                                      \
        }                                                                                          \
    } while (0)

#else
#define SS_FAULT_INJECTION_ACTION_TRIGGER_CUSTOM(point, action)
#define SS_FAULT_INJECTION_CALL(point, ...)
#endif

#ifdef __cplusplus
}
#endif

#endif /* __SS_DMS_FI_H__ */
