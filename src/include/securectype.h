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
 * securectype.h
 *        define internal used macro and data type. The marco of SECUREC_ON_64BITS
 *        will be determined in this header file, which is a switch for part
 *        of code. Some macro are used to supress warning by MS compiler.
 * 
 * 
 * 
 * IDENTIFICATION
 *        src/include/securectype.h
 *
 * Note:
 *        user can change the value of SECUREC_STRING_MAX_LEN and SECUREC_MEM_MAX_LEN
 *        macro to meet their special need ,but The maximum value should not exceed 2G .
 *
 * ---------------------------------------------------------------------------------------
 */
/* [Standardize-exceptions]: Performance-sensitive
 * [reason]: Strict parameter verification has been done before use
 */

#ifndef __SECURECTYPE_H__A7BBB686_AADA_451B_B9F9_44DACDAE18A7
#define __SECURECTYPE_H__A7BBB686_AADA_451B_B9F9_44DACDAE18A7

#ifndef SECUREC_ONLY_DECLARE_MEMSET
/* Shielding VC symbol redefinition warning */
#if defined(_MSC_VER) && (_MSC_VER >= 1400)
#ifdef __STDC_WANT_SECURE_LIB__
#undef __STDC_WANT_SECURE_LIB__
#endif
#define __STDC_WANT_SECURE_LIB__ 0
#ifdef _CRTIMP_ALTERNATIVE
#undef _CRTIMP_ALTERNATIVE
#endif
#define _CRTIMP_ALTERNATIVE /* comment microsoft *_s function */
#endif
#endif

#if SECUREC_IN_KERNEL
#include <linux/kernel.h>
#include <linux/module.h>
#else
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#endif

/* if enable SECUREC_COMPATIBLE_WIN_FORMAT, the output format will be compatible to Windows. */
#if (defined(_WIN32) || defined(_WIN64) || defined(_MSC_VER))
#define SECUREC_COMPATIBLE_WIN_FORMAT
#endif

#if defined(SECUREC_COMPATIBLE_WIN_FORMAT)
/* in windows platform, can't use optimized function for there is no __builtin_constant_p like function */
/* If need optimized macro, can define this: define __builtin_constant_p(x) 0 */
#ifdef SECUREC_WITH_PERFORMANCE_ADDONS
#undef SECUREC_WITH_PERFORMANCE_ADDONS
#define SECUREC_WITH_PERFORMANCE_ADDONS 0
#endif
#endif

#if defined(__VXWORKS__) || defined(__vxworks) || defined(__VXWORKS) || defined(_VXWORKS_PLATFORM_) || \
    defined(SECUREC_VXWORKS_VERSION_5_4)
#if !defined(SECUREC_VXWORKS_PLATFORM)
#define SECUREC_VXWORKS_PLATFORM
#endif
#endif

/* if enable SECUREC_COMPATIBLE_LINUX_FORMAT, the output format will be compatible to Linux. */
#if !(defined(SECUREC_COMPATIBLE_WIN_FORMAT) || defined(SECUREC_VXWORKS_PLATFORM))
#define SECUREC_COMPATIBLE_LINUX_FORMAT
#endif
#ifdef SECUREC_COMPATIBLE_LINUX_FORMAT
#include <stddef.h>
#endif

/* add  the -DSECUREC_SUPPORT_FORMAT_WARNING  compiler option to supoort  -Wformat.
 * default does not check the format is that the same data type in the actual code
 * in the product is different in the original data type definition of VxWorks and Linux.
 */
#ifndef SECUREC_SUPPORT_FORMAT_WARNING
#define SECUREC_SUPPORT_FORMAT_WARNING 0
#endif

#if SECUREC_SUPPORT_FORMAT_WARNING && !defined(SECUREC_PCLINT)
#define SECUREC_ATTRIBUTE(x, y) __attribute__((format(printf, (x), (y))))
#else
#define SECUREC_ATTRIBUTE(x, y)
#endif

#if defined(__GNUC__) && ((__GNUC__ > 3 || (__GNUC__ == 3 && __GNUC_MINOR__ > 3))) && !defined(SECUREC_PCLINT)
/* This is a built-in function that can be used without a declaration, if you encounter an undeclared compilation alarm,
 * you can add -DSECUREC_NEED_BUILTIN_EXPECT_DECLARE to complier options
 */
#if defined(SECUREC_NEED_BUILTIN_EXPECT_DECLARE)
long __builtin_expect(long exp, long c);
#endif
#define SECUREC_LIKELY(x) __builtin_expect(!!(x), 1)
#define SECUREC_UNLIKELY(x) __builtin_expect(!!(x), 0)
#else
#define SECUREC_LIKELY(x) (x)
#define SECUREC_UNLIKELY(x) (x)
#endif

/* define the max length of the string */
#ifndef SECUREC_STRING_MAX_LEN
#define SECUREC_STRING_MAX_LEN (0x7fffffffUL)
#endif
#define SECUREC_WCHAR_STRING_MAX_LEN (SECUREC_STRING_MAX_LEN / sizeof(wchar_t))

/* add SECUREC_MEM_MAX_LEN for memcpy and memmove */
#ifndef SECUREC_MEM_MAX_LEN
#define SECUREC_MEM_MAX_LEN (0x7fffffffUL)
#endif
#define SECUREC_WCHAR_MEM_MAX_LEN (SECUREC_MEM_MAX_LEN / sizeof(wchar_t))

#if SECUREC_STRING_MAX_LEN > 0x7fffffff
#error "max string is 2G"
#endif

#if (defined(__GNUC__) && defined(__SIZEOF_POINTER__))
#if (__SIZEOF_POINTER__ != 4) && (__SIZEOF_POINTER__ != 8)
#error "unsupported system"
#endif
#endif

#if defined(_WIN64) || defined(WIN64) || defined(__LP64__) || defined(_LP64)
#define SECUREC_ON_64BITS
#endif

#if (!defined(SECUREC_ON_64BITS) && defined(__GNUC__) && defined(__SIZEOF_POINTER__))
#if __SIZEOF_POINTER__ == 8
#define SECUREC_ON_64BITS
#endif
#endif

#if defined(__SVR4) || defined(__svr4__)
#define SECUREC_ON_SOLARIS
#endif

#if (defined(__hpux) || defined(_AIX) || defined(SECUREC_ON_SOLARIS))
#define SECUREC_ON_UNIX
#endif

/* codes should run under the macro SECUREC_COMPATIBLE_LINUX_FORMAT in unknow system on default,
 * and strtold. The function
 * strtold is referenced first at ISO9899:1999(C99), and some old compilers can
 * not support these functions. Here provides a macro to open these functions:
 * SECUREC_SUPPORT_STRTOLD  -- if defined, strtold will   be used
 */
#ifndef SECUREC_SUPPORT_STRTOLD
#define SECUREC_SUPPORT_STRTOLD 0
#if (defined(SECUREC_COMPATIBLE_LINUX_FORMAT))
#if defined(__USE_ISOC99) || (defined(_AIX) && defined(_ISOC99_SOURCE)) || (defined(__hpux) && defined(__ia64)) ||  \
    (defined(SECUREC_ON_SOLARIS) && (!defined(_STRICT_STDC) && !defined(__XOPEN_OR_POSIX)) || defined(_STDC_C99) || \
        defined(__EXTENSIONS__))
#undef SECUREC_SUPPORT_STRTOLD
#define SECUREC_SUPPORT_STRTOLD 1
#endif
#endif
#if ((defined(SECUREC_WRLINUX_BELOW4) || defined(_WRLINUX_BELOW4_)))
#undef SECUREC_SUPPORT_STRTOLD
#define SECUREC_SUPPORT_STRTOLD 0
#endif
#endif

#if SECUREC_WITH_PERFORMANCE_ADDONS

#ifndef SECUREC_TWO_MIN
#define SECUREC_TWO_MIN(a, b) ((a) < (b) ? (a) : (b))
#endif

/* for strncpy_s performance optimization */
#define SECUREC_STRNCPY_SM(dest, destMax, src, count)                                                 \
    (((void*)dest != NULL && (void*)src != NULL && (size_t)destMax > 0 &&                             \
         (((unsigned long long)(destMax) & (unsigned long long)(-2)) < SECUREC_STRING_MAX_LEN) &&     \
         (SECUREC_TWO_MIN(count, strlen(src)) + 1) <= (size_t)destMax)                                \
            ? ((count < strlen(src)) ? (memcpy(dest, src, count), *((char*)dest + count) = '\0', EOK) \
                                     : (memcpy(dest, src, strlen(src) + 1), EOK))                     \
            : (strncpy_error(dest, destMax, src, count)))

#define SECUREC_STRCPY_SM(dest, destMax, src)                                                     \
    (((void*)dest != NULL && (void*)src != NULL && (size_t)destMax > 0 &&                         \
         (((unsigned long long)(destMax) & (unsigned long long)(-2)) < SECUREC_STRING_MAX_LEN) && \
         (strlen(src) + 1) <= (size_t)destMax)                                                    \
            ? (memcpy(dest, src, strlen(src) + 1), EOK)                                           \
            : (strcpy_error(dest, destMax, src)))

/* for strcat_s performance optimization */
#if defined(__GNUC__)
#define SECUREC_STRCAT_SM(dest, destMax, src)                                                        \
    ({                                                                                               \
        int catRet = EOK;                                                                            \
        if ((void*)dest != NULL && (void*)src != NULL && (size_t)(destMax) > 0 &&                    \
            (((unsigned long long)(destMax) & (unsigned long long)(-2)) < SECUREC_STRING_MAX_LEN)) { \
            char* catTmpDst = (dest);                                                                \
            size_t catRestSize = (destMax);                                                          \
            while (catRestSize > 0 && *catTmpDst) {                                                  \
                ++catTmpDst;                                                                         \
                --catRestSize;                                                                       \
            }                                                                                        \
            if (catRestSize == 0) {                                                                  \
                catRet = EINVAL;                                                                     \
            } else if ((strlen(src) + 1) <= catRestSize) {                                           \
                memcpy(catTmpDst, (src), strlen(src) + 1);                                           \
                catRet = EOK;                                                                        \
            } else {                                                                                 \
                catRet = ERANGE;                                                                     \
            }                                                                                        \
            if (catRet != EOK) {                                                                     \
                catRet = strcat_s((dest), (destMax), (src));                                         \
            }                                                                                        \
        } else {                                                                                     \
            catRet = strcat_s((dest), (destMax), (src));                                             \
        }                                                                                            \
        catRet;                                                                                      \
    })
#else
#define SECUREC_STRCAT_SM(dest, destMax, src) strcat_s(dest, destMax, src)
#endif

/* for strncat_s performance optimization */
#if defined(__GNUC__)
#define SECUREC_STRNCAT_SM(dest, destMax, src, count)                                                \
    ({                                                                                               \
        int ncatRet = EOK;                                                                           \
        if ((void*)dest != NULL && (void*)src != NULL && (size_t)destMax > 0 &&                      \
            (((unsigned long long)(destMax) & (unsigned long long)(-2)) < SECUREC_STRING_MAX_LEN) && \
            (((unsigned long long)(count) & (unsigned long long)(-2)) < SECUREC_STRING_MAX_LEN)) {   \
            char* ncatTmpDest = (dest);                                                              \
            size_t ncatRestSize = (destMax);                                                         \
            while (ncatRestSize > 0 && *ncatTmpDest) {                                               \
                ++ncatTmpDest;                                                                       \
                --ncatRestSize;                                                                      \
            }                                                                                        \
            if (ncatRestSize == 0) {                                                                 \
                ncatRet = EINVAL;                                                                    \
            } else if ((SECUREC_TWO_MIN((count), strlen(src)) + 1) <= ncatRestSize) {                \
                if ((count) < strlen(src)) {                                                         \
                    memcpy(ncatTmpDest, (src), (count));                                             \
                    *(ncatTmpDest + (count)) = '\0';                                                 \
                } else {                                                                             \
                    memcpy(ncatTmpDest, (src), strlen(src) + 1);                                     \
                }                                                                                    \
            } else {                                                                                 \
                ncatRet = ERANGE;                                                                    \
            }                                                                                        \
            if (ncatRet != EOK) {                                                                    \
                ncatRet = strncat_s((dest), (destMax), (src), (count));                              \
            }                                                                                        \
        } else {                                                                                     \
            ncatRet = strncat_s((dest), (destMax), (src), (count));                                  \
        }                                                                                            \
        ncatRet;                                                                                     \
    })
#else
#define SECUREC_STRNCAT_SM(dest, destMax, src, count) strncat_s(dest, destMax, src, count)
#endif

/* SECUREC_MEMCPY_SM do NOT check buffer overlap by default */
#define SECUREC_MEMCPY_SM(dest, destMax, src, count)                                                                   \
    (!(((size_t)destMax == 0) || (((unsigned long long)(destMax) & (unsigned long long)(-2)) > SECUREC_MEM_MAX_LEN) || \
         ((size_t)count > (size_t)destMax) || ((void*)dest) == NULL || ((void*)src == NULL))                           \
            ? (memcpy(dest, src, count), EOK)                                                                          \
            : (memcpy_s(dest, destMax, src, count)))

#define SECUREC_MEMSET_SM(dest, destMax, c, count)                                                                     \
    (!(((size_t)destMax == 0) || (((unsigned long long)(destMax) & (unsigned long long)(-2)) > SECUREC_MEM_MAX_LEN) || \
         ((void*)dest == NULL) || ((size_t)count > (size_t)destMax))                                                   \
            ? (memset(dest, c, count), EOK)                                                                            \
            : (memset_s(dest, destMax, c, count)))

#endif
#endif /* __SECURECTYPE_H__A7BBB686_AADA_451B_B9F9_44DACDAE18A7 */
