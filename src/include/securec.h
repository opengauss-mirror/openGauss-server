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
 * securec.h
 * 
 * 
 * 
 * IDENTIFICATION
 *        src/include/securec.h
 *
 * NOTE
 *        the user of this secure c library should include this header file
 *        in you source code. This header file declare all supported API
 *        prototype of the library, such as memcpy_s, strcpy_s, wcscpy_s,
 *        strcat_s, strncat_s, sprintf_s, scanf_s, and so on.
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef __SECUREC_H__5D13A042_DC3F_4ED9_A8D1_882811274C27
#define __SECUREC_H__5D13A042_DC3F_4ED9_A8D1_882811274C27

/* Compile in kernel under macro control */
#ifndef SECUREC_IN_KERNEL
#ifdef __KERNEL__
#define SECUREC_IN_KERNEL 1
#else
#define SECUREC_IN_KERNEL 0
#endif
#endif

/* If you need high performance, enable the SECUREC_WITH_PERFORMANCE_ADDONS macro, default is enable .
 * The macro is automatically closed on the windows platform in securectyp.h.
 */
#ifndef SECUREC_WITH_PERFORMANCE_ADDONS
#if SECUREC_IN_KERNEL
#define SECUREC_WITH_PERFORMANCE_ADDONS 0
#else
#define SECUREC_WITH_PERFORMANCE_ADDONS 1
#endif
#endif

#if SECUREC_IN_KERNEL
#ifndef SECUREC_ENABLE_SCANF_FILE
#define SECUREC_ENABLE_SCANF_FILE 0
#endif
#endif

#include "securectype.h"
#include <stdarg.h>

#ifndef SECUREC_HAVE_ERRNO_H
#if SECUREC_IN_KERNEL
#define SECUREC_HAVE_ERRNO_H 0
#else
#define SECUREC_HAVE_ERRNO_H 1
#endif
#endif

/* EINVAL ERANGE may defined in errno.h */
#if SECUREC_HAVE_ERRNO_H
#include <errno.h>
#endif

/* define error code */
#if !defined(__STDC_WANT_LIB_EXT1__) || (defined(__STDC_WANT_LIB_EXT1__) && (__STDC_WANT_LIB_EXT1__ == 0))
#ifndef SECUREC_DEFINED_ERRNO_TYPE
#define SECUREC_DEFINED_ERRNO_TYPE
/* just check whether macrodefinition exists. */
#ifndef errno_t
typedef int errno_t;
#endif
#endif
#endif

/* success */
#ifndef EOK
#define EOK (0)
#endif

#ifndef EINVAL
/* The src buffer is not correct and destination buffer cant not be reset */
#define EINVAL (22)
#endif

#ifndef EINVAL_AND_RESET
/* Once the error is detected, the dest buffer must be reseted! */
#define EINVAL_AND_RESET (22 | 128)
#endif

#ifndef ERANGE
/* The destination buffer is not long enough and destination buffer can not be reset */
#define ERANGE (34)
#endif

#ifndef ERANGE_AND_RESET
/* Once the error is detected, the dest buffer must be reseted! */
#define ERANGE_AND_RESET (34 | 128)
#endif

#ifndef EOVERLAP_AND_RESET
/* Once the buffer overlap is detected, the dest buffer must be reseted! */
#define EOVERLAP_AND_RESET (54 | 128)
#endif

/* if you need export the function of this library in Win32 dll, use __declspec(dllexport) */
#ifdef SECUREC_IS_DLL_LIBRARY
#ifdef SECUREC_DLL_EXPORT
#define SECUREC_API __declspec(dllexport)
#else
#define SECUREC_API __declspec(dllimport)
#endif
#else
/* Standardized function declaration . If a security function is declared in the your code,
 * it may cause a compilation alarm,Please delete the security function you declared
 */
#define SECUREC_API extern
#endif

#ifndef SECUREC_SNPRINTF_TRUNCATED
#define SECUREC_SNPRINTF_TRUNCATED 1
#endif

#ifdef __cplusplus
extern "C" {
#endif

/*
 * @Description:The GetHwSecureCVersion function get SecureC Version string and version number .
 * @param verStr -   address to store verison string
 * @param bufSize -The maximum length of dest
 * @param verNumber - to store version number
 * @return  no
 */
SECUREC_API void getHwSecureCVersion(char* verStr, int bufSize, unsigned short* verNumber);

/*
 * @Description:The memset_s function copies the value of c (converted to an unsigned char) into each of
 * the first count characters of the object pointed to by dest.
 * @param dest - destination  address
 * @param destMax -The maximum length of destination buffer
 * @param c - the value to be copied
 * @param count -copies fisrt count characters of  dest
 * @return  EOK if there was no runtime-constraint violation
 */
SECUREC_API errno_t memset_s(void* dest, size_t destMax, int c, size_t count);

/* The memset_s security function is not provided in Windows system,
 * but other security functions are provided. In this case, can only use the memset_s function
 */
#ifndef SECUREC_ONLY_DECLARE_MEMSET
/*
 * @Description:The memmove_s function copies n characters from the object pointed to by src
 * into the object pointed to by dest.
 * @param dest - destination  address
 * @param destMax -The maximum length of destination buffer
 * @param src -source  address
 * @param count -copies count wide characters from the  src
 * @return  EOK if there was no runtime-constraint violation
 */
SECUREC_API errno_t memmove_s(void* dest, size_t destMax, const void* src, size_t count);

/*
 * @Description:The memcpy_s function copies n characters from the object pointed to
 * by src into the object pointed to by dest.
 * @param dest - destination  address
 * @param destMax -The maximum length of destination buffer
 * @param src -source  address
 * @param count -copies count  characters from the  src
 * @return  EOK if there was no runtime-constraint violation
 */
SECUREC_API errno_t memcpy_s(void* dest, size_t destMax, const void* src, size_t count);

/*
 * @Description:The strcpy_s function copies the string pointed to by strSrc (including
 * the terminating null character) into the array pointed to by strDest
 * @param strDest - destination  address
 * @param destMax -The maximum length of destination buffer(including the terminating null character)
 * @param strSrc -source  address
 * @return  EOK if there was no runtime-constraint violation
 */
SECUREC_API errno_t strcpy_s(char* strDest, size_t destMax, const char* strSrc);

/*
 * @Description:The strncpy_s function copies not more than n successive characters (not including
 * the terminating null character)
 *                     from the array pointed to by strSrc to the array pointed to by strDest
 * @param strDest - destination  address
 * @param destMax -The maximum length of destination buffer(including the terminating null character)
 * @param strSrc -source  address
 * @param count -copies count  characters from the  src
 * @return  EOK if there was no runtime-constraint violation
 */
SECUREC_API errno_t strncpy_s(char* strDest, size_t destMax, const char* strSrc, size_t count);

/*
 * @Description:The strcat_s function appends a copy of the  string pointed to by strSrc (including
 * the terminating null  character)
 *                     to the end of the  string pointed to by strDest
 * @param strDest - destination  address
 * @param destMax -The maximum length of destination buffer(including the terminating null wide character)
 * @param strSrc -source  address
 * @return  EOK if there was no runtime-constraint violation
 */
SECUREC_API errno_t strcat_s(char* strDest, size_t destMax, const char* strSrc);

/*
 * @Description:The strncat_s function appends not more than n successive  characters (not including
 * the terminating null  character)
 *                       from the array pointed to by strSrc to the end of the  string pointed to by strDest.
 * @param strDest - destination  address
 * @param destMax -The maximum length of destination buffer(including the terminating null character)
 * @param strSrc -source  address
 * @param count -copies count  characters from the  src
 * @return  EOK if there was no runtime-constraint violation
 */
SECUREC_API errno_t strncat_s(char* strDest, size_t destMax, const char* strSrc, size_t count);

/*
 * @Description: The vsprintf_s function is equivalent to the vsprintf function except for the parameter destMax
 * and the explicit runtime-constraints violation
 * @param strDest -  produce output according to a format ,write to the character string strDest
 * @param destMax - The maximum length of destination buffer(including the terminating null wide characte)
 * @param format - fromat string
 * @param argList - instead of  a variable  number of arguments
 * @return:return the number of characters printed(not including the terminating null byte ('\0')),
 * If an error occurred return -1.
 */
SECUREC_API int vsprintf_s(char* strDest, size_t destMax, const char* format, va_list argList) SECUREC_ATTRIBUTE(3, 0);

/*
 * @Description: The sprintf_s function is equivalent to the sprintf function except for the parameter destMax
 * and the explicit runtime-constraints violation
 * @param strDest -  produce output according to a format ,write to the character string strDest
 * @param destMax - The maximum length of destination buffer(including the terminating null byte ('\0'))
 * @param format - fromat string
 * @return:success the number of characters printed(not including the terminating null byte ('\0')),
 * If an error occurred return -1.
 */
SECUREC_API int sprintf_s(char* strDest, size_t destMax, const char* format, ...) SECUREC_ATTRIBUTE(3, 4);

/*
 * @Description: The vsnprintf_s function is equivalent to the vsnprintf function except for the parameter
 * destMax/count and the explicit runtime-constraints violation
 * @param strDest -  produce output according to a format ,write to the character string strDest
 * @param destMax - The maximum length of destination buffer(including the terminating null  byte ('\0'))
 * @param count - do not write more than count bytes to strDest(not including the terminating null  byte ('\0'))
 * @param format - fromat string
 * @param argList - instead of  a variable  number of arguments
 * @return:return the number of characters printed(not including the terminating null byte ('\0')),
 * If an error occurred return -1.Pay special attention to returning -1 when truncation occurs
 */
SECUREC_API int vsnprintf_s(char* strDest, size_t destMax, size_t count, const char* format, va_list argList)
    SECUREC_ATTRIBUTE(4, 0);

/*
 * @Description: The snprintf_s function is equivalent to the snprintf function except for the parameter
 * destMax/count and the explicit runtime-constraints violation
 * @param strDest -  produce output according to a format ,write to the character string strDest
 * @param destMax - The maximum length of destination buffer(including the terminating null  byte ('\0'))
 * @param count - do not write more than count bytes to strDest(not including the terminating null  byte ('\0'))
 * @param format - fromat string
 * @return:return the number of characters printed(not including the terminating null byte ('\0')),
 * If an error occurred return -1.Pay special attention to returning -1 when truncation occurs
 */
SECUREC_API int snprintf_s(char* strDest, size_t destMax, size_t count, const char* format, ...)
    SECUREC_ATTRIBUTE(4, 5);

#if SECUREC_SNPRINTF_TRUNCATED
/*
 * @Description: The vsnprintf_truncated_s function is equivalent to the vsnprintf_s function except
 * no count parameter  and return value
 * @param strDest -  produce output according to a format ,write to the character string strDest
 * @param destMax - The maximum length of destination buffer(including the terminating null  byte ('\0'))
 * @param format - fromat string
 * @param argList - instead of  a variable  number of arguments
 * @return:return the number of characters printed(not including the terminating null byte ('\0')),
 * If an error occurred return -1.Pay special attention to returning destMax - 1 when truncation occurs
 */
SECUREC_API int vsnprintf_truncated_s(char* strDest, size_t destMax, const char* format, va_list argList)
    SECUREC_ATTRIBUTE(3, 0);

/*
 * @Description: The snprintf_truncated_s function is equivalent to the snprintf_2 function except
 * no count parameter  and return value
 * @param strDest -  produce output according to a format ,write to the character string strDest
 * @param destMax - The maximum length of destination buffer(including the terminating null  byte ('\0'))
 * @param format - fromat string
 * @return:return the number of characters printed(not including the terminating null byte ('\0')),
 * If an error occurred return -1.Pay special attention to returning destMax - 1 when truncation occurs
 */
SECUREC_API int snprintf_truncated_s(char* strDest, size_t destMax, const char* format, ...) SECUREC_ATTRIBUTE(3, 4);
#endif

/*
 * @Description: The scanf_s function is equivalent to fscanf_s with the argument stdin
 * interposed before the arguments to scanf_s
 * @param format - fromat string
 * @return:returns the number of input items assigned, If an error occurred return -1.
 */
SECUREC_API int scanf_s(const char* format, ...);

/*
 * @Description: The vscanf_s function is equivalent to scanf_s, with the variable argument list replaced by argList
 * @param format - fromat string
 * @param argList - instead of  a variable  number of arguments
 * @return:returns the number of input items assigned, If an error occurred return -1.
 */
SECUREC_API int vscanf_s(const char* format, va_list argList);

/*
 * @Description: The sscanf_s function is equivalent to fscanf_s, except that input is obtained from a
 * string (specified by the argument buffer) rather than from a stream
 * @param buffer -  read character from  buffer
 * @param format - fromat string
 * @return:returns the number of input items assigned, If an error occurred return -1.
 */
SECUREC_API int sscanf_s(const char* buffer, const char* format, ...);

/*
 * @Description: The vsscanf_s function is equivalent to sscanf_s, with the variable argument list
 * replaced by argList
 * @param buffer -  read character from  buffer
 * @param format - fromat string
 * @param argList - instead of  a variable  number of arguments
 * @return:returns the number of input items assigned, If an error occurred return -1.
 */
SECUREC_API int vsscanf_s(const char* buffer, const char* format, va_list argList);

#if (defined(SECUREC_ENABLE_SCANF_FILE) && SECUREC_ENABLE_SCANF_FILE == 1) || !(defined(SECUREC_ENABLE_SCANF_FILE))

/*
 * @Description: The fscanf_s function is equivalent to fscanf except that the c, s, and [ conversion specifiers
 * apply to a pair of arguments (unless assignment suppression is indicated by a*)
 * @param stream - stdio file stream
 * @param format - fromat string
 * @return:returns the number of input items assigned, If an error occurred return -1.
 */
SECUREC_API int fscanf_s(FILE* stream, const char* format, ...);

/*
 * @Description: The vfscanf_s function is equivalent to fscanf_s, with the variable argument list
 * replaced by argList
 * @param stream - stdio file stream
 * @param format - fromat string
 * @param argList - instead of  a variable  number of arguments
 * @return:returns the number of input items assigned, If an error occurred return -1.
 */
SECUREC_API int vfscanf_s(FILE* stream, const char* format, va_list argList);
#endif

/*
 * @Description: The  strtok_s  function parses a string into a sequence of tokens,On the first call to strtok_s
 * the string to be parsed should be specified in strToken. In each subsequent call that should parse the
 * same string, strToken should be NULL
 * @param strToken - the string to be delimited
 * @param strDelimit -specifies a set of characters that delimit the tokens in the parsed string
 * @param context -is a pointer to a char * variable that is used internally by strtok_s function
 * @return:returns a pointer to the first character of a token, or a null pointer if there is no token or there
 * is a runtime-constraint violation.
 */
SECUREC_API char* strtok_s(char* strToken, const char* strDelimit, char** context);
#if SECUREC_IN_KERNEL == 0
/*
 * @Description:The wmemcpy_s function copies n successive wide characters from the object pointed to
 * by src into the object pointed to by dest.
 * @param dest - destination  address
 * @param destMax -The maximum length of destination buffer
 * @param src -source  address
 * @param count -copies count wide characters from the  src
 * @return  EOK if there was no runtime-constraint violation
 */
SECUREC_API errno_t wmemcpy_s(wchar_t* dest, size_t destMax, const wchar_t* src, size_t count);

/*
 * @Description:The wmemmove_s function copies n successive wide characters from the object
 * pointed to by src into the object pointed to by dest.
 * @param dest - destination  address
 * @param destMax -The maximum length of destination buffer
 * @param src -source  address
 * @param count -copies count wide characters from the  src
 * @return  EOK if there was no runtime-constraint violation
 */
SECUREC_API errno_t wmemmove_s(wchar_t* dest, size_t destMax, const wchar_t* src, size_t count);

/*
 * @Description:The wcscpy_s function copies the wide string pointed to by strSrc (including theterminating
 * null wide character) into the array pointed to by strDest
 * @param strDest - destination  address
 * @param destMax -The maximum length of destination buffer
 * @param strSrc -source  address
 * @return  EOK if there was no runtime-constraint violation
 */
SECUREC_API errno_t wcscpy_s(wchar_t* strDest, size_t destMax, const wchar_t* strSrc);

/*
 * @Description:The wcsncpy_s function copies not more than n successive wide characters (not including the
 * terminating null wide character) from the array pointed to by strSrc to the array pointed to by strDest
 * @param strDest - destination  address
 * @param destMax -The maximum length of destination buffer(including the terminating wide character)
 * @param strSrc -source  address
 * @param count -copies count wide characters from the  src
 * @return  EOK if there was no runtime-constraint violation
 */
SECUREC_API errno_t wcsncpy_s(wchar_t* strDest, size_t destMax, const wchar_t* strSrc, size_t count);

/*
 * @Description:The wcscat_s function appends a copy of the wide string pointed to by strSrc (including the
 * terminating null wide character) to the end of the wide string pointed to by strDest
 * @param strDest - destination  address
 * @param destMax -The maximum length of destination buffer(including the terminating wide character)
 * @param strSrc -source  address
 * @return  EOK if there was no runtime-constraint violation
 */
SECUREC_API errno_t wcscat_s(wchar_t* strDest, size_t destMax, const wchar_t* strSrc);

/*
 * @Description:The wcsncat_s function appends not more than n successive wide characters (not including the
 * terminating null wide character) from the array pointed to by strSrc to the end of the wide string pointed to
 * by strDest.
 * @param strDest - destination  address
 * @param destMax -The maximum length of destination buffer(including the terminating wide character)
 * @param strSrc -source  address
 * @param count -copies count wide characters from the  src
 * @return  EOK if there was no runtime-constraint violation
 */
SECUREC_API errno_t wcsncat_s(wchar_t* strDest, size_t destMax, const wchar_t* strSrc, size_t count);

/*
 * @Description: The  wcstok_s  function  is  the  wide-character  equivalent  of the strtok_s function
 * @param strToken - the string to be delimited
 * @param strDelimit -specifies a set of characters that delimit the tokens in the parsed string
 * @param context -is a pointer to a char * variable that is used internally by strtok_s function
 * @return:returns a pointer to the first character of a token, or a null pointer if there is no token
 * or there is a runtime-constraint violation.
 */
SECUREC_API wchar_t* wcstok_s(wchar_t* strToken, const wchar_t* strDelimit, wchar_t** context);

/*
 * @Description: The  vswprintf_s  function  is  the  wide-character  equivalent  of the vsprintf_s function
 * @param strDest -  produce output according to a format ,write to the character string strDest
 * @param destMax - The maximum length of destination buffer(including the terminating null )
 * @param format - fromat string
 * @param argList - instead of  a variable  number of arguments
 * @return:return the number of characters printed(not including the terminating null wide characte),
 * If an error occurred return -1.
 */
SECUREC_API int vswprintf_s(wchar_t* strDest, size_t destMax, const wchar_t* format, va_list argList);

/*
 * @Description: The  swprintf_s  function  is  the  wide-character  equivalent  of the sprintf_s function
 * @param strDest -  produce output according to a format ,write to the character string strDest
 * @param destMax - The maximum length of destination buffer(including the terminating null )
 * @param format - fromat string
 * @return:success the number of characters printed(not including the terminating null wide characte),
 * If an error occurred return -1.
 */
SECUREC_API int swprintf_s(wchar_t* strDest, size_t destMax, const wchar_t* format, ...);

#if (defined(SECUREC_ENABLE_SCANF_FILE) && SECUREC_ENABLE_SCANF_FILE == 1) || !(defined(SECUREC_ENABLE_SCANF_FILE))
/*
 * @Description: The  fwscanf_s  function  is  the  wide-character  equivalent  of the fscanf_s function
 * @param stream - stdio file stream
 * @param format - fromat string
 * @return:returns the number of input items assigned, If an error occurred return -1.
 */
SECUREC_API int fwscanf_s(FILE* stream, const wchar_t* format, ...);

/*
 * @Description: The  vfwscanf_s  function  is  the  wide-character  equivalent  of the vfscanf_s function
 * @param stream - stdio file stream
 * @param format - fromat string
 * @param argList - instead of  a variable  number of arguments
 * @return:returns the number of input items assigned, If an error occurred return -1.
 */
SECUREC_API int vfwscanf_s(FILE* stream, const wchar_t* format, va_list argList);
#endif

/*
 * @Description: The  wscanf_s  function  is  the  wide-character  equivalent  of the scanf_s function
 * @param format - fromat string
 * @return:returns the number of input items assigned, If an error occurred return -1.
 */
SECUREC_API int wscanf_s(const wchar_t* format, ...);

/*
 * @Description: The  vwscanf_s  function  is  the  wide-character  equivalent  of the vscanf_s function
 * @param format - fromat string
 * @param argList - instead of  a variable  number of arguments
 * @return:returns the number of input items assigned, If an error occurred return -1.
 */
SECUREC_API int vwscanf_s(const wchar_t* format, va_list argList);

/*
 * @Description: The  swscanf_s  function  is  the  wide-character  equivalent  of the sscanf_s function
 * @param buffer -  read character from  buffer
 * @param format - fromat string
 * @return:returns the number of input items assigned, If an error occurred return -1.
 */
SECUREC_API int swscanf_s(const wchar_t* buffer, const wchar_t* format, ...);

/*
 * @Description: The  vswscanf_s  function  is  the  wide-character  equivalent  of the vsscanf_s function
 * @param buffer -  read character from  buffer
 * @param format - fromat string
 * @param argList - instead of  a variable  number of arguments
 * @return:returns the number of input items assigned, If an error occurred return -1.
 */
SECUREC_API int vswscanf_s(const wchar_t* buffer, const wchar_t* format, va_list argList);

/*
 * @Description:The gets_s function reads at most one less than the number of characters specified
 * by destMax from the stream pointed to by stdin, into the array pointed to by buffer
 * @param buffer - destination  address
 * @param destMax -The maximum length of destination buffer(including the terminating null character)
 * @return  buffer if there was no runtime-constraint violation,If an error occurred return NULL.
 */
SECUREC_API char* gets_s(char* buffer, size_t destMax);

/* those functions are used by macro ,must declare hare , also for  without function declaration warning */
extern errno_t strncpy_error(char* strDest, size_t destMax, const char* strSrc, size_t count);
extern errno_t strcpy_error(char* strDest, size_t destMax, const char* strSrc);
#endif
#endif

#if SECUREC_WITH_PERFORMANCE_ADDONS
/* those functions are used by macro */
extern errno_t memset_sOptAsm(void* dest, size_t destMax, int c, size_t count);
extern errno_t memset_sOptTc(void* dest, size_t destMax, int c, size_t count);
extern errno_t memcpy_sOptAsm(void* dest, size_t destMax, const void* src, size_t count);
extern errno_t memcpy_sOptTc(void* dest, size_t destMax, const void* src, size_t count);

/* strcpy_sp is a macro, NOT a function in performance optimization mode. */
#define strcpy_sp(dest, destMax, src)                                                                               \
    ((__builtin_constant_p((destMax)) && __builtin_constant_p((src))) ? SECUREC_STRCPY_SM((dest), (destMax), (src)) \
                                                                      : strcpy_s((dest), (destMax), (src)))

/* strncpy_sp is a macro, NOT a function in performance optimization mode. */
#define strncpy_sp(dest, destMax, src, count)                                                          \
    ((__builtin_constant_p((count)) && __builtin_constant_p((destMax)) && __builtin_constant_p((src))) \
            ? SECUREC_STRNCPY_SM((dest), (destMax), (src), (count))                                    \
            : strncpy_s((dest), (destMax), (src), (count)))

/* strcat_sp is a macro, NOT a function in performance optimization mode. */
#define strcat_sp(dest, destMax, src)                                                                               \
    ((__builtin_constant_p((destMax)) && __builtin_constant_p((src))) ? SECUREC_STRCAT_SM((dest), (destMax), (src)) \
                                                                      : strcat_s((dest), (destMax), (src)))

/* strncat_sp is a macro, NOT a function in performance optimization mode. */
#define strncat_sp(dest, destMax, src, count)                                                          \
    ((__builtin_constant_p((count)) && __builtin_constant_p((destMax)) && __builtin_constant_p((src))) \
            ? SECUREC_STRNCAT_SM((dest), (destMax), (src), (count))                                    \
            : strncat_s((dest), (destMax), (src), (count)))

/* memcpy_sp is a macro, NOT a function in performance optimization mode. */
#define memcpy_sp(dest, destMax, src, count)                                                                     \
    (__builtin_constant_p((count))                                                                               \
            ? (SECUREC_MEMCPY_SM((dest), (destMax), (src), (count)))                                             \
            : (__builtin_constant_p((destMax))                                                                   \
                      ? (((size_t)(destMax) > 0 &&                                                               \
                             (((unsigned long long)(destMax) & (unsigned long long)(-2)) < SECUREC_MEM_MAX_LEN)) \
                                ? memcpy_sOptTc((dest), (destMax), (src), (count))                               \
                                : ERANGE)                                                                        \
                      : memcpy_sOptAsm((dest), (destMax), (src), (count))))

/* memset_sp is a macro, NOT a function in performance optimization mode. */
#define memset_sp(dest, destMax, c, count)                                                                       \
    (__builtin_constant_p((count))                                                                               \
            ? (SECUREC_MEMSET_SM((dest), (destMax), (c), (count)))                                               \
            : (__builtin_constant_p((destMax))                                                                   \
                      ? (((size_t)(destMax) > 0 &&                                                               \
                             (((unsigned long long)(destMax) & (unsigned long long)(-2)) < SECUREC_MEM_MAX_LEN)) \
                                ? memset_sOptTc((dest), (destMax), (c), (count))                                 \
                                : ERANGE)                                                                        \
                      : memset_sOptAsm((dest), (destMax), (c), (count))))
#else
#define strcpy_sp strcpy_s
#define strncpy_sp strncpy_s
#define strcat_sp strcat_s
#define strncat_sp strncat_s
#define memcpy_sp memcpy_s
#define memset_sp memset_s
#endif

#ifdef __cplusplus
}
#endif /* __cplusplus */
#endif /* __SECUREC_H__5D13A042_DC3F_4ED9_A8D1_882811274C27 */
