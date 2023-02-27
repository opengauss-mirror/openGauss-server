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
#define AUDIT_CLIENT_LEN 100
#define CRYPT_FUNC_ARG "(********)"

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
extern void pgaudit_ddl_sql_patch(const char* objectName, const char* cmdText);

extern bool audit_check_client_blacklist(char client_info[]);
extern bool audit_check_full_audit_user();
extern void audit_system_function(FunctionCallInfo fcinfo, const AuditResult result);
extern char* audit_get_value_bytype(FunctionCallInfo fcinfo, int n_arg);
 
const char* const g_audit_system_funcs[] = {
    "set_working_grand_version_num_manually",
    "set_config",
    "pg_cancel_backend",
    "pg_cancel_session",
    "pg_cancel_invalid_query",
    "pg_reload_conf",
    "pg_rotate_logfile",
    "pg_terminate_session",
    "pg_terminate_backend",
    "pg_create_restore_point",
    "pg_start_backup",
    "pg_stop_backup",
    "pg_switch_xlog",
    "pg_cbm_get_merged_file",
    "pg_cbm_recycle_file",
    "pg_enable_delay_ddl_recycle",
    "pg_disable_delay_ddl_recycle",
    "pg_cbm_rotate_file",
    "gs_roach_stop_backup",
    "gs_roach_enable_delay_ddl_recycle",
    "gs_roach_disable_delay_ddl_recycle",
    "gs_roach_switch_xlog",
    "pg_last_xlog_receive_location",
    "pg_xlog_replay_pause",
    "pg_xlog_replay_resume",
    "gs_pitr_clean_history_global_barriers",
    "gs_pitr_archive_slot_force_advance",
    "pg_create_physical_replication_slot_extern",
    "gs_set_obs_delete_location",
    "gs_hadr_do_switchover",
    "gs_set_obs_delete_location_with_slotname",
    "gs_streaming_dr_in_switchover",
    "gs_upload_obs_file",
    "gs_download_obs_file",
    "gs_set_obs_file_context",
    "gs_get_hadr_key_cn",
    "pg_advisory_lock",
    "pg_advisory_lock_shared",
    "pg_advisory_unlock",
    "pg_advisory_unlock_shared",
    "pg_advisory_unlock_all",
    "pg_advisory_xact_lock",
    "pg_advisory_xact_lock_shared",
    "pg_try_advisory_lock",
    "pg_try_advisory_lock_shared",
    "pg_try_advisory_xact_lock",
    "pg_try_advisory_xact_lock_shared",
    "pg_create_logical_replication_slot",
    "pg_drop_replication_slot",
    "pg_logical_slot_peek_changes",
    "pg_logical_slot_get_changes",
    "pg_logical_slot_get_binary_changes",
    "pg_replication_slot_advance",
    "pg_replication_origin_create",
    "pg_replication_origin_drop",
    "pg_replication_origin_session_setup",
    "pg_replication_origin_session_reset",
    "pg_replication_origin_session_progress",
    "pg_replication_origin_xact_setup",
    "pg_replication_origin_xact_reset",
    "pg_replication_origin_advance",
    "local_space_shrink",
    "gs_space_shrink",
    "global_space_shrink",
    "pg_free_remain_segment",
    "gs_fault_inject",
    "sqladvisor.init",
    "sqladvisor.set_weight_params",
    "sqladvisor.set_cost_params",
    "sqladvisor.assign_table_type",
    "gs_repair_file",
    "local_clear_bad_block_info",
    "gs_repair_page",
    NULL
    };

/* refer to funCrypt in elog.cpp */
const char* const g_audit_crypt_funcs[] = {"gs_encrypt_aes128", "gs_decrypt_aes128",
                                           "gs_encrypt", "gs_decrypt",
                                           "aes_encrypt", "aes_decrypt", "pg_create_physical_replication_slot_extern",
                                           "dblink_connect", NULL};
#endif
