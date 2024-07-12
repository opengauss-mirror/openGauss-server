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
#include "ddes/dms/ss_dms.h"
#include "utils/elog.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef enum en_db_fi_point_name {
    // if CALL and TRIGGER both in kernel point range [10001, 10799]
    DB_FI_CHANGE_BUFFERTAG_BLOCKNUM = DB_FI_ENTRY_BEGIN + 1,
} db_fi_point_name;

int dms_fi_set_entries(unsigned int type, unsigned int *entries, unsigned int count);
int dms_fi_set_entry_value(unsigned int type, unsigned int value);
int dms_fi_get_tls_trigger_custom(void);
void dms_fi_set_tls_trigger_custom(int val);
unsigned char dms_fi_entry_custom_valid(unsigned int point);
void dms_fi_change_buffertag_blocknum(const dms_fi_entry *entry, va_list args);

#ifdef USE_ASSERT_CHECKING
#define FAULT_INJECTION_ACTION_TRIGGER_CUSTOM(point, action)                                       \
    do {                                                                                           \
        if (dms_fi_entry_custom_valid(point) && dms_fi_get_tls_trigger_custom() == TRUE) {         \
            dms_fi_set_tls_trigger_custom(FALSE);                                                  \
            ereport(DEBUG1, (errmsg("[KERNEL_FI] fi custom action happens at %s", __FUNCTION__))); \
            action;                                                                                \
        }                                                                                          \
    } while (0)

#define SS_FAULT_INJECTION_CALL(point, ...)                                                        \
    do {                                                                                           \
        if (g_ss_dms_func.inited) {                                                                \
            g_ss_dms_func.fault_injection_call(point, ##__VA_ARGS__);                              \
        }                                                                                          \
    } while (0)

#else
#define FAULT_INJECTION_ACTION_TRIGGER_CUSTOM(point, action)
#define SS_FAULT_INJECTION_CALL(point, ...)
#endif

#ifdef __cplusplus
}
#endif

#endif /* __SS_DMS_FI_H__ */
