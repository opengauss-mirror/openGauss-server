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
 * bbox_syscall_ARM64.h
 *
 * IDENTIFICATION
 *    src/gausskernel/cbb/bbox/bbox_syscall_ARM64.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef __BBOX_SYSCALL_ARM64_H_
#define __BBOX_SYSCALL_ARM64_H_

#include <sys/syscall.h>
#include <sys/errno.h>

#define SYS_NAME(name) sys_##name

#define LOAD_ARGS_0() register long _x0 asm("x0");
#define LOAD_ARGS_1(x0)       \
    long _x0tmp = (long)(x0); \
    LOAD_ARGS_0()             \
    _x0 = _x0tmp;
#define LOAD_ARGS_2(x0, x1)   \
    long _x1tmp = (long)(x1); \
    LOAD_ARGS_1(x0)           \
    register long _x1 asm("x1") = _x1tmp;
#define LOAD_ARGS_3(x0, x1, x2) \
    long _x2tmp = (long)(x2);   \
    LOAD_ARGS_2(x0, x1)         \
    register long _x2 asm("x2") = _x2tmp;
#define LOAD_ARGS_4(x0, x1, x2, x3) \
    long _x3tmp = (long)(x3);       \
    LOAD_ARGS_3(x0, x1, x2)         \
    register long _x3 asm("x3") = _x3tmp;
#define LOAD_ARGS_5(x0, x1, x2, x3, x4) \
    long _x4tmp = (long)(x4);           \
    LOAD_ARGS_4(x0, x1, x2, x3)         \
    register long _x4 asm("x4") = _x4tmp;
#define LOAD_ARGS_6(x0, x1, x2, x3, x4, x5) \
    long _x5tmp = (long)(x5);               \
    LOAD_ARGS_5(x0, x1, x2, x3, x4)         \
    register long _x5 asm("x5") = _x5tmp;
#define LOAD_ARGS_7(x0, x1, x2, x3, x4, x5, x6) \
    long _x6tmp = (long)(x6);                   \
    LOAD_ARGS_6(x0, x1, x2, x3, x4, x5)         \
    register long _x6 asm("x6") = _x6tmp;

#define ASM_ARGS_0
#define ASM_ARGS_1 , "r"(_x0)
#define ASM_ARGS_2 ASM_ARGS_1, "r"(_x1)
#define ASM_ARGS_3 ASM_ARGS_2, "r"(_x2)
#define ASM_ARGS_4 ASM_ARGS_3, "r"(_x3)
#define ASM_ARGS_5 ASM_ARGS_4, "r"(_x4)
#define ASM_ARGS_6 ASM_ARGS_5, "r"(_x5)
#define ASM_ARGS_7 ASM_ARGS_6, "r"(_x6)

#define SYS_ify(syscall_name) (__NR_##syscall_name)

#define INTERNAL_SYSCALL_RAW(name, err, nr, args...)                                                    \
    ({                                                                                                  \
        long _sys_result;                                                                               \
        {                                                                                               \
            LOAD_ARGS_##nr(args) register long _x8 asm("x8") = (name);                                  \
            asm volatile("svc	0	// syscall " #name : "=r"(_x0) : "r"(_x8)ASM_ARGS_##nr : "memory"); \
            _sys_result = _x0;                                                                          \
        }                                                                                               \
        _sys_result;                                                                                    \
    })

#define INTERNAL_SYSCALL(name, err, nr, args...) INTERNAL_SYSCALL_RAW(SYS_ify(name), err, nr, args)

#define INLINE_SYSCALL(name, nr, args...) INTERNAL_SYSCALL(name, , nr, args)

#define ___syscall_return(type, ___res)                          \
    do {                                                         \
        if ((unsigned long)(___res) >= (unsigned long)(-4095)) { \
            errno = -(___res);                                   \
            ___res = -1;                                         \
        }                                                        \
        return (type)(___res);                                   \
    } while (0)

#define __syscall0(type, name)            \
    type SYS_NAME(name)()                 \
    {                                     \
        long ___res;                      \
        ___res = INLINE_SYSCALL(name, 0); \
        ___syscall_return(type, ___res);  \
    }

#define __syscall1(type, name, type1, arg1)     \
    type SYS_NAME(name)(type1 arg1)             \
    {                                           \
        long ___res;                            \
        ___res = INLINE_SYSCALL(name, 1, arg1); \
        ___syscall_return(type, ___res);        \
    }

#define __syscall2(type, name, type1, arg1, type2, arg2) \
    type SYS_NAME(name)(type1 arg1, type2 arg2)          \
    {                                                    \
        long ___res;                                     \
        ___res = INLINE_SYSCALL(name, 2, arg1, arg2);    \
        ___syscall_return(type, ___res);                 \
    }

#define __syscall3(type, name, type1, arg1, type2, arg2, type3, arg3) \
    type SYS_NAME(name)(type1 arg1, type2 arg2, type3 arg3)           \
    {                                                                 \
        long ___res;                                                  \
        ___res = INLINE_SYSCALL(name, 3, arg1, arg2, arg3);           \
        ___syscall_return(type, ___res);                              \
    }

#define __syscall4(type, name, type1, arg1, type2, arg2, type3, arg3, type4, arg4) \
    type SYS_NAME(name)(type1 arg1, type2 arg2, type3 arg3, type4 arg4)            \
    {                                                                              \
        long ___res;                                                               \
        ___res = INLINE_SYSCALL(name, 4, arg1, arg2, arg3, arg4);                  \
        ___syscall_return(type, ___res);                                           \
    }

#define __syscall5(type, name, type1, arg1, type2, arg2, type3, arg3, type4, arg4, type5, arg5) \
    type SYS_NAME(name)(type1 arg1, type2 arg2, type3 arg3, type4 arg4, type5 arg5)             \
    {                                                                                           \
        long ___res;                                                                            \
        ___res = INLINE_SYSCALL(name, 5, arg1, arg2, arg3, arg4, arg5);                         \
        ___syscall_return(type, ___res);                                                        \
    }

#define __syscall6(type, name, type1, arg1, type2, arg2, type3, arg3, type4, arg4, type5, arg5, type6, arg6) \
    type SYS_NAME(name)(type1 arg1, type2 arg2, type3 arg3, type4 arg4, type5 arg5, type6 arg6)              \
    {                                                                                                        \
        long ___res;                                                                                         \
        ___res = INLINE_SYSCALL(name, 6, arg1, arg2, arg3, arg4, arg5, arg6);                                \
        ___syscall_return(type, ___res);                                                                     \
    }

#endif
