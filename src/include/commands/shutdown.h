/* -------------------------------------------------------------------------
 *
 * shutdown.h
 *        prototypes for shutdown.cpp
 *
 * Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 *
 * src/include/commands/shutdown.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef SHUTDOWN_H
#define SHUTDOWN_H

#include "nodes/parsenodes.h"

extern void DoShutdown(ShutdownStmt* stmt);

#endif /* SHUTDOWN_H */
