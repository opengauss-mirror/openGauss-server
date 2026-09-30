/*
 * Copyright (c) 2026 Huawei Technologies Co.,Ltd.
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
 * instr_database_sql.h
 *        Per-database SQL call / elapse aggregation for WDR
 *
 * IDENTIFICATION
 *        src/include/instruments/instr_database_sql.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef INSTR_DATABASE_SQL_H
#define INSTR_DATABASE_SQL_H

#include "postgres.h"

void InitDatabaseSQLStat(void);
void UpdateDatabaseSQLStat(int64 elapse_start);

#endif
