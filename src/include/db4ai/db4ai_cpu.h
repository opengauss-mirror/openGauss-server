/**
Copyright (c) 2021 Huawei Technologies Co.,Ltd. 
openGauss is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:

  http://license.coscl.org.cn/MulanPSL2

THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details.
---------------------------------------------------------------------------------------

db4ai_cpu.h

IDENTIFICATION
    src/include/db4ai/db4ai_cpu.h

---------------------------------------------------------------------------------------
**/

#ifndef DB4AI_MRC_CPU_H
#define DB4AI_MRC_CPU_H

#if defined(__GNUC__)

#ifndef likely
#define likely(x)           __builtin_expect((x) != 0, 1)
#endif

#ifndef unlikely
#define unlikely(x)         __builtin_expect((x) != 0, 0)
#endif

#ifndef force_alignment
#define force_alignment(x)  __attribute__((aligned((x))))
#endif

#ifndef force_inline
#define force_inline        inline __attribute__((always_inline))
#endif

#ifndef prefetch
#define prefetch(address, rw, locality) (__builtin_prefetch(address, rw, locality))
#endif

#else

#define likely(x) (x)
#define unlikely(x) (x)
#define prefetch(address, rw, locality) ()
#define force_alignment(x) ()
#define force_inline ()
#endif

#endif //DB4AI_MRC_CPU_H
