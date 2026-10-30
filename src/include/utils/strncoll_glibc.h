/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
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
 * strncoll_glibc.h
 *   Supported glibc version range and private-field layout constants for
 *   the custom strncoll() implementation (strncoll.cpp / varlena.cpp).
 *
 *   The private __locale_data layout this code was written against is
 *   byte-identical from glibc 2.28 through 2.42.  glibc 2.36 shrank the
 *   __locale_data.private field from 16 to 8 bytes, which shifts the
 *   values[] array offset by 8 bytes; glibc_private_offset() compensates
 *   at runtime.  Keep all version thresholds here so they are defined once.
 */
#ifndef STRNCOLL_GLIBC_H
#define STRNCOLL_GLIBC_H

/* Supported glibc major/minor range for the private locale-data layout. */
#define STRNCOLL_GLIBC_MAJOR_MIN 2
#define STRNCOLL_GLIBC_MINOR_MIN 28
#define STRNCOLL_GLIBC_MINOR_MAX 42

/* glibc 2.36 shrank __locale_data.private from 16 to 8 bytes. */
#define STRNCOLL_GLIBC_PRIVATE_SHRINK_MINOR 36
#define STRNCOLL_GLIBC_PRIVATE_OFFSET_NEW 8

#endif /* STRNCOLL_GLIBC_H */
