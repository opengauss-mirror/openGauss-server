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
 * sctp_err.h
 *
 * IDENTIFICATION
 *    src/gausskernel/cbb/communication/sctp_utils/sctp_err.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef _UTILS_ERR_H_
#define _UTILS_ERR_H_

#include <assert.h>
#include <errno.h>
#include <stdio.h>
#include <string.h>
#include "libcomm_utils/libcomm_util.h"
#include "libcomm_common.h"

#if defined __GNUC__ || defined __llvm__
#define mc_fast(x) __builtin_expect((x), 1)
#define mc_slow(x) __builtin_expect((x), 0)
#else
#define mc_fast(x) (x)
#define mc_slow(x) (x)
#endif

#if defined __GNUC__
#define mc_likely(x) __builtin_expect((x), 1)
#define mc_unlikely(x) __builtin_expect((x), 0)
#else
#define mc_likely(x) (x)
#define mc_unlikely(x) (x)
#endif

/*  Same as system assert(). However, under Win32 assert has some deficiencies.
        Thus this macro. */
#define mc_assert(x)                                                                   \
    do {                                                                               \
        if (mc_slow(!(x))) {                                                           \
            COMM_DEBUG_LOG("sctp check failed: %s (%s:%d)\n", #x, __FILE__, __LINE__); \
            mc_err_abort();                                                            \
        }                                                                              \
    } while (0)

//  Provides convenient way to check for errors from getaddrinfo.
#define gai_assert(x)                                                    \
    do {                                                                 \
        if (mc_unlikely(x)) {                                            \
            const char* errstr = gai_strerror(x);                        \
            fprintf(stderr, "%s (%s:%d)\n", errstr, __FILE__, __LINE__); \
            mc_err_abort();                                              \
        }                                                                \
    } while (false)

//  Provides convenient way to check for POSIX errors.
#define posix_assert(x)                                                       \
    do {                                                                      \
        if (mc_unlikely(x)) {                                                 \
            fprintf(stderr, "%s (%s:%d)\n", strerror(x), __FILE__, __LINE__); \
            mc_err_abort();                                                   \
        }                                                                     \
    } while (false)

#define mc_assert_state(obj, state_name)                                                                            \
    do {                                                                                                            \
        if (mc_slow((obj)->state != (state_name))) {                                                                \
            fprintf(stderr, "Assertion failed: %d == %s (%s:%d)\n", (obj)->state, #state_name, __FILE__, __LINE__); \
            (void)fflush(stderr);                                                                                   \
            mc_err_abort();                                                                                         \
        }                                                                                                           \
    } while (0)

/*  Checks whether memory allocation was successful. */
#define alloc_assert(x)                                                     \
    do {                                                                    \
        if (mc_slow(!(x))) {                                                \
            fprintf(stderr, "Out of memory (%s:%d)\n", __FILE__, __LINE__); \
            (void)fflush(stderr);                                           \
            mc_err_abort();                                                 \
        }                                                                   \
    } while (0)

/*  Check the condition. If false prints out the errno. */
#define errno_assert(x)                                                                               \
    do {                                                                                              \
        if (mc_slow(!(x))) {                                                                          \
            fprintf(stderr, "%s [%d] (%s:%d)\n", mc_strerror(errno), (int)errno, __FILE__, __LINE__); \
            (void)fflush(stderr);                                                                     \
            mc_err_abort();                                                                           \
        }                                                                                             \
    } while (0)

/*  Checks whether supplied errno number is an error. */
#define errnum_assert(cond, err)                                                                    \
    do {                                                                                            \
        if (mc_slow(!(cond))) {                                                                     \
            fprintf(stderr, "%s [%d] (%s:%d)\n", mc_strerror(err), (int)(err), __FILE__, __LINE__); \
            (void)fflush(stderr);                                                                   \
            mc_err_abort();                                                                         \
        }                                                                                           \
    } while (0)

/* Checks the condition. If false prints out the GetLastError info. */
#define win_assert(x)                                                                              \
    do {                                                                                           \
        if (mc_slow(!(x))) {                                                                       \
            char errstr[256];                                                                      \
            mc_win_error((int)GetLastError(), errstr, 256);                                        \
            fprintf(stderr, "%s [%d] (%s:%d)\n", errstr, (int)GetLastError(), __FILE__, __LINE__); \
            (void)fflush(stderr);                                                                  \
            mc_err_abort();                                                                        \
        }                                                                                          \
    } while (0)

/* Checks the condition. If false prints out the WSAGetLastError info. */
#define wsa_assert(x)                                                                                 \
    do {                                                                                              \
        if (mc_slow(!(x))) {                                                                          \
            char errstr[256];                                                                         \
            mc_win_error(WSAGetLastError(), errstr, 256);                                             \
            fprintf(stderr, "%s [%d] (%s:%d)\n", errstr, (int)WSAGetLastError(), __FILE__, __LINE__); \
            (void)fflush(stderr);                                                                     \
            mc_err_abort();                                                                           \
        }                                                                                             \
    } while (0)

/*  Assertion-like macros for easier fsm debugging. */
#define mc_fsm_error(message, state, src, type)                                                                       \
    do {                                                                                                              \
        fprintf(stderr, "%s: state=%d source=%d action=%d (%s:%d)\n", message, state, src, type, __FILE__, __LINE__); \
        (void)fflush(stderr);                                                                                         \
        mc_err_abort();                                                                                               \
    } while (0)

#define mc_fsm_bad_action(state, src, type) mc_fsm_error("Unexpected action", state, src, type)
#define mc_fsm_bad_state(state, src, type) mc_fsm_error("Unexpected state", state, src, type)
#define mc_fsm_bad_source(state, src, type) mc_fsm_error("Unexpected source", state, src, type)

void mc_err_abort(void);
const char* mc_err_strerror(int errnum);

#endif  //_UTILS_ERR_H_
