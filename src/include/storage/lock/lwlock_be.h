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
 * ---------------------------------------------------------------------------------------
 * 
 * lwlock_be.h
 *        bridge between lwlock.h and pgstat.h
 * 
 * 
 * IDENTIFICATION
 *        src/include/storage/lock/lwlock_be.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef SRC_INCLUDE_STORAGE_LWLOCK_BE_H
#define SRC_INCLUDE_STORAGE_LWLOCK_BE_H

#include "postgres.h"
#include "knl/knl_variable.h"
#include "storage/lock/lwlock.h"

extern void remember_lwlock_acquire(LWLock* lockid);
extern void forget_lwlock_acquire(void);

#endif  // SRC_INCLUDE_STORAGE_LWLOCK_BE_H