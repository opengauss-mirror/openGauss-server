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
 * posix_semaphore.h
 * 
 * 
 * 
 * IDENTIFICATION
 *        src/include/access/parallel_recovery/posix_semaphore.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef PARALLEL_RECOVERY_POSIX_SEMAPHORE_H
#define PARALLEL_RECOVERY_POSIX_SEMAPHORE_H

#include <semaphore.h>
namespace parallel_recovery {

typedef struct PosixSemaphore {
    sem_t semaphore;
    bool initialized;
} PosixSemaphore;

void PosixSemaphoreInit(PosixSemaphore* sem, unsigned int initValue);
void PosixSemaphoreDestroy(PosixSemaphore* sem);
void PosixSemaphoreWait(PosixSemaphore* sem);
void PosixSemaphorePost(PosixSemaphore* sem);
}
#endif
