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
 * ddes_fault_injection_def.h
 *
 *  Defines the DMS fault injection function
 *
 *
 * IDENTIFICATION
 *        src/gausskernel/ddes/adapter/ddes_fault_injection_def.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef __DDES_FAULT_INJECTION_DEFS_H__
#define __DDES_FAULT_INJECTION_DEFS_H__

#ifdef __cplusplus
extern "C" {
#endif

typedef enum e_ddes_fi_type {
    DDES_FI_TYPE_BEGIN = 0,
    DDES_FI_TYPE_PACKET_LOSS = DDES_FI_TYPE_BEGIN,
    DDES_FI_TYPE_NET_LATENCY,
    DDES_FI_TYPE_CPU_LATENCY,
    DDES_FI_TYPE_PROCESS_FAULT,
    DDES_FI_TYPE_CUSTOM_FAULT,
    DDES_FI_TYPE_END,
} ddes_fi_type_e;

#define DDES_FI_ENTRY_BEGIN 0
#define DDES_FI_ENTRY_COUNT 11024
#define DDES_FI_ENTRY_END (DDES_FI_ENTRY_BEGIN + DDES_FI_ENTRY_COUNT)
#define DDES_FI_ENTRY_COUNT_PER_TYPE 2000 // set count per time

// uplayer should NOT define entry id between [DDES_FI_ENTRY_RESERVE_BEGIN, DDES_FI_ENTRY_RESERVE_END],
// they are reserved for cbb functions
#define DDES_FI_ENTRY_RESERVE_BEGIN 2000
#define DDES_FI_ENTRY_RESERVE_COUNT 2000
#define DDES_FI_ENTRY_RESERVE_END (DDES_FI_ENTRY_RESERVE_BEGIN + DDES_FI_ENTRY_RESERVE_COUNT)

/*
* the follow defines SHOULD be the same as the defines in cbb/ddes_fi_ddes_fault_injection.h

// begin: SHOULD call these by caller to init fi context before using other interfaces
DDES_DECLARE int ddes_fi_get_context_size(void);
DDES_DECLARE void ddes_fi_set_and_init_context(void *context);
DDES_DECLARE void ddes_fi_set_context(void *context);
// end: SHOULD call these by caller to init fi context before using other interfaces

DDES_DECLARE int ddes_fi_set_entries(unsigned int type, unsigned int *entries, unsigned int count);
DDES_DECLARE int ddes_fi_parse_and_set_entry_list(unsigned int type, char *value);
DDES_DECLARE unsigned int ddes_fi_get_entry_value(unsigned int type);
DDES_DECLARE int ddes_fi_set_entry_value(unsigned int type, unsigned int value);
DDES_DECLARE int ddes_fi_get_tls_trigger(void);
DDES_DECLARE void ddes_fi_set_tls_trigger(int val);
DDES_DECLARE int ddes_fi_get_tls_trigger_custom(void);
DDES_DECLARE void ddes_fi_set_tls_trigger_custom(int val);
DDES_DECLARE void ddes_fi_call(unsigned int point, ...);
DDES_DECLARE void ddes_fi_call_ex(unsigned int point, va_list args);
DDES_DECLARE bool8 ddes_fi_entry_custom_valid(unsigned int point);
*/

#ifdef __cplusplus
}
#endif

#endif /* __DDES_FAULT_INJECTION_DEFS_H__ */