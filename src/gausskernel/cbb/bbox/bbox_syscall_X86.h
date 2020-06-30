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
 * bbox_syscall_X86.h
 *
 * IDENTIFICATION
 *    src/gausskernel/cbb/bbox/bbox_syscall_X86.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef __BBOX_SYSCALL_X86_H_
#define __BBOX_SYSCALL_X86_H_

#include <sys/syscall.h>
#include <sys/errno.h>

#ifndef NT_PRXFPREG
#define NT_PRXFPREG 0x46e62b7f
#endif

extern long do_syscall(int number, ...);

#define SYS_NAME(name) sys_##name

#define ___syscall_return(type, __res)                         \
    do {                                                       \
        if ((unsigned long)(__res) >= (unsigned long)(-125)) { \
            errno = -(__res);                                  \
            __res = -1;                                        \
        }                                                      \
        return (type)(__res);                                  \
    } while (0)

/* define syscall that have no parameter. */
#define __syscall0(type, name)            \
    type SYS_NAME(name)(void)             \
    {                                     \
        long ___res;                      \
        ___res = do_syscall(__NR_##name); \
        ___syscall_return(type, ___res);  \
    }

/* define syscall that have 1 parameter. */
#define __syscall1(type, name, type1, arg1)               \
    type SYS_NAME(name)(type1 arg1)                       \
    {                                                     \
        long ___res;                                      \
        ___res = do_syscall(__NR_##name, ((long)(arg1))); \
        ___syscall_return(type, ___res);                  \
    }

/* define syscall that have 2 parameter. */
#define __syscall2(type, name, type1, arg1, type2, arg2)                  \
    type SYS_NAME(name)(type1 arg1, type2 arg2)                           \
    {                                                                     \
        long ___res;                                                      \
        ___res = do_syscall(__NR_##name, ((long)(arg1)), ((long)(arg2))); \
        ___syscall_return(type, ___res);                                  \
    }

/* define syscall that have 3 parameter. */
#define __syscall3(type, name, type1, arg1, type2, arg2, type3, arg3)                     \
    type SYS_NAME(name)(type1 arg1, type2 arg2, type3 arg3)                               \
    {                                                                                     \
        long ___res;                                                                      \
        ___res = do_syscall(__NR_##name, ((long)(arg1)), ((long)(arg2)), ((long)(arg3))); \
        ___syscall_return(type, ___res);                                                  \
    }

/* define syscall that have 4 parameter. */
#define __syscall4(type, name, type1, arg1, type2, arg2, type3, arg3, type4, arg4)                        \
    type SYS_NAME(name)(type1 arg1, type2 arg2, type3 arg3, type4 arg4)                                   \
    {                                                                                                     \
        long ___res;                                                                                      \
        ___res = do_syscall(__NR_##name, ((long)(arg1)), ((long)(arg2)), ((long)(arg3)), ((long)(arg4))); \
        ___syscall_return(type, ___res);                                                                  \
    }

/* define syscall that have 5 parameter. */
#define __syscall5(type, name, type1, arg1, type2, arg2, type3, arg3, type4, arg4, type5, arg5)                      \
    type SYS_NAME(name)(type1 arg1, type2 arg2, type3 arg3, type4 arg4, type5 arg5)                                  \
    {                                                                                                                \
        long ___res;                                                                                                 \
        ___res =                                                                                                     \
            do_syscall(__NR_##name, ((long)(arg1)), ((long)(arg2)), ((long)(arg3)), ((long)(arg4)), ((long)(arg5))); \
        ___syscall_return(type, ___res);                                                                             \
    }

/* define syscall that have 6 parameter. */
#define __syscall6(type, name, type1, arg1, type2, arg2, type3, arg3, type4, arg4, type5, arg5, type6, arg6) \
    type SYS_NAME(name)(type1 arg1, type2 arg2, type3 arg3, type4 arg4, type5 arg5, type6 arg6)              \
    {                                                                                                        \
        long ___res;                                                                                         \
        __asm__ volatile("push %%ebp ; movl %%eax,%%ebp ; movl %1,%%eax ; int $0x80 ; pop %%ebp"             \
                         : "=a"(___res)                                                                      \
                         : "i"(__NR_##name),                                                                 \
                         "b"((long)(arg1)),                                                                  \
                         "c"((long)(arg2)),                                                                  \
                         "d"((long)(arg3)),                                                                  \
                         "S"((long)(arg4)),                                                                  \
                         "D"((long)(arg5)),                                                                  \
                         "0"((long)(arg6)));                                                                 \
        ___syscall_return(type, ___res);                                                                     \
    }

#endif
