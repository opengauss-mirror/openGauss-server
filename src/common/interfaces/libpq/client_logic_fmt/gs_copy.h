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
 * gs_copy.h
 *
 * IDENTIFICATION
 *	  src\common\interfaces\libpq\client_logic_fmt\gs_copy.h
 *
 * -------------------------------------------------------------------------
 */
 
#ifndef GS_COPY_H
#define GS_COPY_H

typedef struct pg_conn PGconn;
struct CopyStmt;

typedef struct CopyStateData CopyStateData;
CopyStateData *pre_copy(const CopyStmt * const stmt, const char *query_string);
void delete_copy_state(CopyStateData *cstate);
int deprocess_copy_line(PGconn *conn, const char *in_buffer, int msg_length, char **buffer);
int process_copy_chunk(PGconn *conn, const char *in_buffer, int msg_length, char **buffer);
#endif /* GS_COPY_H */
