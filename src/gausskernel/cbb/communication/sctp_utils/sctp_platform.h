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
 * sctp_platform.h
 *
 * IDENTIFICATION
 *    src/gausskernel/cbb/communication/sctp_utils/sctp_platform.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef _UTILS_PLATFORM_H_
#define _UTILS_PLATFORM_H_

int cmp_with_kernel_release();

int mc_check_SLESSP2_version(void);

int mc_check_sctp_support(void);

#endif  // _UTILS_PLATFORM_H_
