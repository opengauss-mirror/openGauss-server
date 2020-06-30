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
 * auditfuncs.h
 *        record the aduit informations of the database operation
 * 
 * 
 * IDENTIFICATION
 *        src/include/auditfuncs.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef PGAUDIT_AGENT_H
#define PGAUDIT_AGENT_H
#include "pgaudit.h"
#include "postgres.h"
#include "knl/knl_variable.h"
#include "catalog/namespace.h"

#define PGAUDIT_MAXLENGTH 1024
#define NoShutdown 0
#define SmartShutdown 1
#define FastShutdown 2
#define ImmediateShutdown 3

char* pgaudit_get_relation_name(List* relation_name_list);
void pgaudit_dml_table(const char* objectname, const char* cmdtext);
void pgaudit_dml_table_select(const char* objectname, const char* cmdtext);

extern void pgaudit_agent_init(void);
extern void pgaudit_agent_fini(void);
extern void pgaudit_user_no_privileges(const char* object_name, const char* detailsinfo);
extern void pgaudit_system_start_ok(int port);
extern void pgaudit_system_switchover_ok(const char* detaisinfo);
extern void pgaudit_system_recovery_ok(void);
extern void pgaudit_system_stop_ok(int shutdown);
extern void pgaudit_user_login(bool login_ok, const char* object_name, const char* detaisinfo);
extern void pgaudit_user_logout(void);
extern void pgaudit_lock_or_unlock_user(bool islocked, const char* user_name);
#endif
