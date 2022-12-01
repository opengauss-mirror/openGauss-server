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
 * mot_error_codes.h
 *    MOT error codes.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/system/mot_error_codes.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef MOT_ERROR_CODES_H
#define MOT_ERROR_CODES_H

// Severity Levels
/**
 * @define Normal severity level. Some operation failed, but nevertheless the status of MOT Engine
 * and the current session is still fine (e.g. transaction aborted due to conflict or unique
 * violation). No action is required by the user.
 */
#define MOT_SEVERITY_NORMAL 0

/**
 * @define Warning severity level. Some operation failed. The status of the MOT Engine is fine. The
 * current session can continue to operate, but severity is higher (e.g. configuration file is
 * missing, but the engine can operate with defaults). The user should note the MOT Engine condition
 * and might fix it.
 */
#define MOT_SEVERITY_WARN 1

/**
 * @define Error severity level. Indicates that an operation failed. This is a real error condition
 * that prevents form the current transaction to continue. The current session may continue if error
 * is acceptable and can be handled. Action is required by the envelope (usually report to the user
 * which operation failed and why).
 */
#define MOT_SEVERITY_ERROR 2

/** @define Fatal severity level. Indicates that the MOT Engine is inoperable. */
#define MOT_SEVERITY_FATAL 3

// Error Codes
/** @define Error code denoting success. */
#define MOT_NO_ERROR 0

/** @define Error code denoting out of memory. */
#define MOT_ERROR_OOM 1

/** @define Error code denoting invalid configuration settings. */
#define MOT_ERROR_INVALID_CFG 2

/** @define Error code denoting invalid argument passed to a function. */
#define MOT_ERROR_INVALID_ARG 3

/** @define Error code denoting operating system call failed. Log should contain additional information. */
#define MOT_ERROR_SYSTEM_FAILURE 4

/** @define Error code denoting resource limit has been reached. Log should contain additional information. */
#define MOT_ERROR_RESOURCE_LIMIT 5

/** @define Error code denoting internal engine logic error. */
#define MOT_ERROR_INTERNAL 6

/** @define Error code denoting some resource cannot be used at the moment. */
#define MOT_ERROR_RESOURCE_UNAVAILABLE 7

/** @define Error code denoting unique violation. */
#define MOT_ERROR_UNIQUE_VIOLATION 8

/**
 * @define Error code denoting that a memory allocation request was denied since the allocation
 * size was invalid (either too large or non-positive).
 */
#define MOT_ERROR_INVALID_MEMORY_SIZE 9

/** @define Error code denoting an attempt was made to access a resource with an index out of range. */
#define MOT_ERROR_INDEX_OUT_OF_RANGE 10

/** @define Error code denoting system is in invalid state. */
#define MOT_ERROR_INVALID_STATE 11

/** @define Error code denoting concurrent modification. */
#define MOT_ERROR_CONCURRENT_MODIFICATION 12

/** @define Error code denoting cancel request by user due to statement timeout. */
#define MOT_ERROR_STATEMENT_CANCELED 13

#endif /* MOT_ERROR_CODES_H */
