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
 *-------------------------------------------------------------------------
 *
 * kt_interface.cpp
 *      APIs for Encrypted Database.
 *
 * IDENTIFICATION
 *    src/bin/gs_ktool/kt_interface.cpp
 *
 *-------------------------------------------------------------------------
 */

#ifndef KT_INTERFACE_H
#define KT_INTERFACE_H

extern bool get_cmk_len(unsigned int cmk_id, unsigned int *cmk_len);
extern bool get_cmk_plain(unsigned int cmk_id, unsigned char *cmk_plain_buffer, unsigned int *cmk_len);

#endif
