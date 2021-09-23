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
 * libpq-fe.h
 *	  This file contains definitions for structures and
 *	  externs for functions used by frontend openGauss applications.
 *
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 * 
 * 
 * IDENTIFICATION
 *        src/include/replication/heartbeat/libpq/libpq-fe.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef CM_LIBPQ_FE_H
#define CM_LIBPQ_FE_H

#include "replication/heartbeat/libpq/libpq-be.h"

namespace PureLibpq {

/* Synchronous (blocking) */
extern Port* PQconnect(const char* conninfo);

}  // namespace PureLibpq

#endif /* CM_LIBPQ_FE_H */
