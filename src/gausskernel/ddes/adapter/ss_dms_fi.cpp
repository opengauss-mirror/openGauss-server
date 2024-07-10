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
int dms_fi_set_entries(unsigned int type, unsigned int *entries, unsigned int count)
{
    if (g_ss_dms_func.inited) {
        return g_ss_dms_func.dms_fi_set_entries(type, entries, count);
    }
    return GS_ERROR;
}

int dms_fi_set_entry_value(unsigned int type, unsigned int value)
{
    if (g_ss_dms_func.inited) {
        return g_ss_dms_func.dms_fi_set_entry_value(type, value);
    }
    return GS_ERROR;
}

int dms_fi_get_tls_trigger_custom()
{
    if (g_ss_dms_func.inited) {
        return g_ss_dms_func.dms_fi_get_tls_trigger_custom();
    }
    return GS_ERROR;
}

void dms_fi_set_tls_trigger_custom(int val)
{
    if (g_ss_dms_func.inited) {
        return g_ss_dms_func.dms_fi_set_tls_trigger_custom(val);
    }
}

unsigned char dms_fi_entry_custom_valid(unsigned int point)
{
    if (g_ss_dms_func.inited) {
        return g_ss_dms_func.dms_fi_entry_custom_valid(point);
    }
    return GS_ERROR;
}

void dms_fi_change_buffertag_blocknum(const dms_fi_entry *entry, va_list args)
{
    dms_fi_set_tls_trigger_custom(TRUE);
}
#else
int dms_fi_set_entries(unsigned int type, unsigned int *entries, unsigned int count) {return GS_ERROR;}
int dms_fi_set_entry_value(unsigned int type, unsigned int value) {return GS_ERROR;}
int dms_fi_get_tls_trigger_custom() {return GS_ERROR;}
void dms_fi_set_tls_trigger_custom(int val) {}
unsigned char dms_fi_entry_custom_valid(unsigned int point) {return GS_ERROR;}
void dms_fi_change_buffertag_blocknum(const dms_fi_entry *entry, va_list args) {};
#endif