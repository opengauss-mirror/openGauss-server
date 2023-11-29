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
 * standby_read_delay_ddl.h
 *
 *
 *
 * IDENTIFICATION
 *        src/include/access/extreme_rto/standby_read/standby_read_delay_ddl.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef STANDBY_READ_DELAY_DDL_H
#define STANDBY_READ_DELAY_DDL_H

#include "gs_thread.h"
#include "postgres.h"
#include "access/xlogdefs.h"
#include "storage/smgr/relfilenode.h"

void do_all_old_delay_ddl();
void delete_by_lsn(XLogRecPtr lsn);
void init_delay_ddl_file();
void update_delay_ddl_db(Oid db_id, Oid tablespace_id, XLogRecPtr lsn);
void update_delay_ddl_files(ColFileNode* xnodes, int nrels, XLogRecPtr lsn);
void delete_by_table_space(Oid tablespace_id);
void update_delay_ddl_file_truncate_clog(XLogRecPtr lsn, int64 pageno);
#endif