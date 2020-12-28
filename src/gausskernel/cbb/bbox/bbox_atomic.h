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
 * bbox_atomic.h
 *
 * IDENTIFICATION
 *    src/gausskernel/cbb/bbox/bbox_atomic.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef DF_ATOMIC_H
#define DF_ATOMIC_H

#if defined(__i386__)
#include "bbox_atomic_i386.h"

#elif (defined(__x86_64__))
#include "bbox_atomic_X86_64.h"

#elif defined(__ARM_ARCH_5TE__)
#include "bbox_atomic_arm.h"

#elif defined(__ARM_ARCH_7A__)
#include "bbox_atomic_arm7.h"

#elif defined(__aarch64__)
#include "bbox_atomic_arm64.h"
#endif

#endif
