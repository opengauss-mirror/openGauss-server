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
 * ss_dms_fi.cpp
 *  Defines the DMS fault injection function
 *
 *
 * IDENTIFICATION
 *        src/gausskernel/ddes/adapter/ss_dms_fi.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include "ddes/dms/ss_dms_fi.h"
#include "storage/file/fio_device_com.h"

#ifdef USE_ASSERT_CHECKING
// for alloc fi context with size return size
int ss_fi_get_context_size(void)
{
    if (g_ss_dms_func.inited) {
        return g_ss_dms_func.ddes_fi_get_context_size();
    }
    return GS_ERROR;
}

// set the fi context with the context, and init the context, the context alloced by DB
void ss_fi_set_and_init_context(void *context)
{
    if (g_ss_dms_func.inited) {
        return g_ss_dms_func.ddes_fi_set_and_init_context(context);
    }
}

int ss_fi_set_entries(unsigned int type, unsigned int *entries, unsigned int count)
{
    if (g_ss_dms_func.inited) {
        return g_ss_dms_func.ddes_fi_set_entries(type, entries, count);
    }
    return GS_ERROR;
}

int ss_fi_get_entry_value(unsigned int type)
{
    if (g_ss_dms_func.inited) {
        return g_ss_dms_func.ddes_fi_get_entry_value(type);
    }
    return GS_ERROR;
}

int ss_fi_set_entry_value(unsigned int type, unsigned int value)
{
    if (g_ss_dms_func.inited) {
        return g_ss_dms_func.ddes_fi_set_entry_value(type, value);
    }
    return GS_ERROR;
}

int ss_fi_get_tls_trigger_custom(void)
{
    if (g_ss_dms_func.inited) {
        return g_ss_dms_func.ddes_fi_get_tls_trigger_custom();
    }
    return GS_ERROR;
}

void ss_fi_set_tls_trigger_custom(int val)
{
    if (g_ss_dms_func.inited) {
        return g_ss_dms_func.ddes_fi_set_tls_trigger_custom(val);
    }
}

unsigned char ss_fi_entry_custom_valid(unsigned int point)
{
    if (g_ss_dms_func.inited) {
        return g_ss_dms_func.ddes_fi_entry_custom_valid(point);
    }
    return GS_ERROR;
}

void ss_fi_call_ex(unsigned int point, ...)
{
    va_list args;
    va_start(args, point);

    if (g_ss_dms_func.inited) {
        return g_ss_dms_func.ddes_fi_call_ex(point, args);
    }
    va_end(args);
}
#endif

#ifdef USE_ASSERT_CHECKING
void ss_fi_change_buffertag_blocknum(const void *ddes_fi_entry, va_list args)
{
    ss_fi_set_tls_trigger_custom(TRUE);
}
#else
void ss_fi_change_buffertag_blocknum(const void *ddes_fi_entry, va_list args) {}
#endif