/*
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2011, PostgreSQL Global Development Group
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
 * fetchmot.h
 *    Interface for Fetching MOT checkpoint.
 *
 * IDENTIFICATION
 *    src/bin/pg_ctl/fetchmot.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef SRC_BIN_PG_CTL_FETCHMOT_H
#define SRC_BIN_PG_CTL_FETCHMOT_H

#include "libpq/libpq-fe.h"

/**
 * @brief Receives and writes the current MOT checkpoint.
 * @param basedir The directory in which to save the files in.
 * @param conn The connection to use in order to fetch.
 * @param progname The caller program name (for logging).
 * @param verbose Controls verbose output.
 * @return Boolean value denoting success or failure.
 */
extern void FetchMotCheckpoint(const char* basedir, PGconn* fetchConn, const char* progname, bool verbose,
                               const char format = 'p', const int compresslevel = 0);
/**
 * @brief Gets the checkpoint_dir config value from the mot.conf file.
 * @param dataDir pg_data directory.
 * @return checkpoint_dir config value as a malloc'd buffer or NULL if
 * it was not found. Caller needs to free this malloc'd buffer.
 */
char* GetMotCheckpointDir(const char* dataDir);

#endif /* SRC_BIN_PG_CTL_FETCHMOT_H */

