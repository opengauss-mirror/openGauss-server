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
 * knl_session_attr_security.h
 *        Data struct to store all knl_session_attr_security GUC variables.
 *
 *   When anyone try to added variable in this file, which means add a guc
 *   variable, there are several rules needed to obey:
 *
 *   add variable to struct 'knl_@level@_attr_@group@'
 *
 *   @level@:
 *   1. instance: the level of guc variable is PGC_POSTMASTER.
 *   2. session: the other level of guc variable.
 *
 *   @group@: sql, storage, security, network, memory, resource, common
 *   select the group according to the type of guc variable.
 * 
 * IDENTIFICATION
 *        src/include/knl/knl_guc/knl_session_attr_security.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef SRC_INCLUDE_KNL_KNL_SESSION_ATTR_SECURITY_H_
#define SRC_INCLUDE_KNL_KNL_SESSION_ATTR_SECURITY_H_

#include "knl/knl_guc/knl_guc_common.h"

typedef struct knl_session_attr_security {
    bool RequireSSL;
    bool zero_damaged_pages;
    bool pg_krb_caseins_users;
    bool Audit_enabled;
    bool Audit_CleanupPolicy;
    bool Modify_initial_password;
    bool operation_mode;
    int PostAuthDelay;
    int AuthenticationTimeout;
    int PreAuthDelay;
    int ssl_renegotiation_limit;
    int ssl_cert_notify_time;
    int Password_policy;
    int Password_encryption_type;
    int Password_reuse_max;
    int Failed_login_attempts;
    int Password_min_upper;
    int Password_min_lower;
    int Password_min_digital;
    int Password_min_special;
    int Password_min_length;
    int Password_max_length;
    int Password_notify_time;
    int Audit_RotationAge;
    int Audit_RotationSize;
    int Audit_SpaceLimit;
    int Audit_RemainAge;
    int Audit_RemainThreshold;
    int Audit_Session;
    int Audit_ServerAction;
    int Audit_LockUser;
    int Audit_UserViolation;
    int Audit_PrivilegeAdmin;
    int Audit_DDL;
    int Audit_DML;
    int Audit_DML_SELECT;
    int Audit_Exec;
    int Audit_Copy;
    int Audit_Set;
    int auth_iteration_count;
    double Password_reuse_time;
    double Password_effect_time;
    double Password_lock_time;
    char* pg_krb_server_keyfile;
    char* pg_krb_srvnam;
    char* tde_cmk_id;    
    bool Enable_Security_Policy;
    int audit_xid_info;
    char* no_audit_client;
    char* full_audit_users;
    int audit_system_function_exec;
} knl_session_attr_security;

#endif /* SRC_INCLUDE_KNL_KNL_SESSION_ATTR_SECURITY_H_ */
