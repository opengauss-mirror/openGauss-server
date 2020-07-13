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
 * shutdown.h
 *        prototypes for shutdown.cpp
 *
 * IDENTIFICATION
 *        src/include/commands/shutdown.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef SHUTDOWN_H
#define SHUTDOWN_H

#include "nodes/parsenodes.h"

extern void DoShutdown(ShutdownStmt* stmt);

#endif /* SHUTDOWN_H */
