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
 * percentile.h
 *     Definitions for the openGauss statistics collector daemon.
 *
 *	Copyright (c) 2001-2012, PostgreSQL Global Development Group
 *
 *
 * IDENTIFICATION
 *        src/include/instruments/percentile.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef PERCENTILE_H
#define PERCENTILE_H
#include "gs_thread.h"

extern void PercentileMain();
extern void JobPercentileIAm(void);
extern bool IsJobPercentileProcess(void);
extern void InitPercentile(void);

#endif