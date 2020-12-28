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
 * gtm_semaphore.h
 *        Wrapper semaphore for the sysv sema. Currently the database only use the
 *        sysv sema. 
 * 
 * 
 * IDENTIFICATION
 *        src/include/gtm/gtm_semaphore.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef _GTM_SEMA_H
#define _GTM_SEMA_H

#include <sys/ipc.h>
#include <sys/sem.h>

typedef struct PGSemaphoreData {
    int semId;  /* semaphore set identifier */
    int semNum; /* semaphore number within set */
} PGSemaphoreData;

typedef PGSemaphoreData* PGSemaphore;

/* key_t is the int type */
typedef key_t IpcSemaphoreKey; /* semaphore key passed to semget */
typedef int IpcSemaphoreId;    /* semaphore ID returned by semget */

#define GTM_MAX_SEMASET (GTM_MAX_GLOBAL_TRANSACTIONS * 2)

/*
 * it is used to assign sema key. if the is_used[index] is true,
 * it means that the index key is used.
 */
typedef struct GlobalSemaKeySet {
    bool is_used[GTM_MAX_SEMASET];
    int current_key; /* current used sema key, default value is invalid. */
} GlobalSemaKeySet;

/*
 * the number of sema sets. when create one sema set,
 * must set the sema number. the valid sema number is SEMA_SET_NUM.
 * when we create the sema set, we assign the sema num is SEMA_SET_NUM + 1,
 * it stat from 0. the SEMA_SET_NUM sema is setted to PGSemaMagic, PGSemaMagic
 * represents the sema set is create by GTM Thread.
 */
#define SEMA_SET_NUM 1

/**
 * @Description: init glocal sema key. set current sema key is invalid.
 * and init the lock. Only the main thread can call this function.
 * destory the lock also is called by main thread only.
 * @return none.
 */
void init_global_sema_key();

/**
 * @Description: destroy sema_key lock
 * @return none.
 */
void destory_sema_key_lock();

/**
 * @Description: get the global seme key for creating the sema.
 * If could not get the sema key. report the error.
 * @return return the sema key.
 */
int get_next_sema_key();

/**
 * @Description: the thread will exit, we must reset the global sema in
 * order to be used by other new thread.
 * @in seme_key, the given key to be reset.
 * @return none.
 */
void reset_sema_key(int sema_key);

/**
 * @Description: create sema and init the sema by the given value.
 * we store the sema id in seme->semId. seme->semNum keeps the index of this
 * sema set array.
 * @in key, the given key for sema.
 * @in value, the given initial value.
 * @return none
 */
void create_sema(PGSemaphore sema, IpcSemaphoreKey& key, int value);

/**
 * @Description: Lock a semaphore (decrement count), blocking if
 * count would be < 0.
 * @return none.
 */
void sema_lock(PGSemaphore sema);

/**
 * @Description: Unlock a semaphore (increment count)
 * @return none.
 */
void sema_unlock(PGSemaphore sema);

/**
 * @Description: release the sema when current thread exit.
 * @in sema, it keeps the sema id info.
 * do the following things:
 * 1. release the sema from the system kenel.
 * 2. release the sema memory.
 * 3. set the is_used of GlobalSemaKeySet to false.
 * @return none
 */
void release_sema(PGSemaphore sema);

#endif
