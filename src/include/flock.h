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
 * flock.h
 * 
 * 
 * 
 * IDENTIFICATION
 *        src/include/flock.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef FLOCK_H
#define FLOCK_H

/*
 * flock ports
 */
#ifndef LOCK_EX

#undef LOCK_SH
#undef LOCK_EX
#undef LOCK_UN
#undef LOCK_NB

#define LOCK_SH 1 /* Shared lock.  */
#define LOCK_EX 2 /* Exclusive lock.  */
#define LOCK_UN 8 /* Unlock.  */
#define LOCK_NB 4 /* Don't block when locking.  */
#endif

#define START_LOCATION 1
#define CURRENT_LOCATION 2
#define END_LOCATION 4

extern int pgut_flock(int fd, int operation, int64 offset, int location, int64 len);
#define flock pgut_flock

#endif