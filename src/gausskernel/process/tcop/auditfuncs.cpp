/*
 * Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 2021, openGauss Contributors
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
 * --------------------------------------------------------------------------
 * auditfuncs.cpp
 *    record the aduit informations of the database operation
 *
 * IDENTIFICATION
 *    src/gausskernel/process/auditfuncs.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "commands/dbcommands.h"
#include "knl/knl_variable.h"
#include "executor/executor.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "tcop/utility.h"
#include "pgaudit.h"
#include "libpq/libpq-be.h"
#include "catalog/namespace.h"
#include "auditfuncs.h"
#include "utils/elog.h"
#include "libpq/libpq-be.h"
#include "utils/builtins.h"
#include "miscadmin.h"
#include "utils/rangetypes.h"
#include "utils/inet.h"
#include "fmgr.h"
#include "utils/nabstime.h"
#include "access/tupmacs.h"
#include "utils/fmgrtab.h"
#include "lib/stringinfo.h"
#include "utils/cash.h"
#include "utils/lsyscache.h"
#include "catalog/namespace.h"
#include "utils/uuid.h"
#include "utils/builtins.h"
#include "utils/date.h"
#include "utils/numeric.h"
#include "utils/numeric_gs.h"
#include "workload/cpwlm.h"

#define AUDIT_BUFFERSIZ 512

typedef void (*AuditFunc)(const char* objectname, const char* cmdtext);
typedef struct AuditFuncMap {
    ObjectType objType;
    AuditFunc auditFunc;
}AuditFuncMap;

static THR_LOCAL ExecutorEnd_hook_type prev_ExecutorEnd = NULL;
static THR_LOCAL ProcessUtility_hook_type prev_ProcessUtility = NULL;

static char* pgaudit_get_function_name(List* funcnamelist);
static void pgaudit_ExecutorEnd(QueryDesc* queryDesc);
static void pgaudit_store_auditstat(
    AuditType audittype, AuditResult auditresult, const char* objectname, const char* detailsinfo);
static void pgaudit_ProcessUtility(processutility_context* processutility_cxt,
    DestReceiver* dest,
#ifdef PGXC
    bool sentToRemote,
#endif /* PGXC */
    char* completionTag,
    ProcessUtilityContext context,
    bool isCTAS);
static void pgaudit_ddl_database(const char* objectname, const char* cmdtext);
static void pgaudit_ddl_directory(const char* objectname, const char* cmdtext);
static void pgaudit_ddl_database_object(
    AuditType audittype, AuditResult auditresult, const char* objectname, const char* cmdtext);
static void pgaudit_ddl_index(const char* objectname, const char* cmdtext);
static void pgaudit_ddl_schema(const char* objectname, const char* cmdtext);
static void pgaudit_ddl_table(const char* objectname, const char* cmdtext);
static void pgaudit_ddl_tablespace(const char* objectname, const char* cmdtext);
static void pgaudit_ddl_trigger(const char* objectname, const char* cmdtext);
static void pgaudit_ddl_user(const char* objectname, const char* cmdtext);
static void pgaudit_ddl_view(const char* objectname, const char* cmdtext);
static void pgaudit_ddl_matview(const char* objectname, const char* cmdtext);
static void pgaudit_ddl_event(const char* objectname, const char* cmdtext);
static void pgaudit_ddl_function(const char* objectname, const char* cmdtext);
static void pgaudit_ddl_package(const char* objectname, const char* cmdtext);
static void pgaudit_ddl_resourcepool(const char* objectname, const char* cmdtext);
static void pgaudit_alter_globalconfig(const AlterGlobalConfigStmt* stmt, const char* cmdtext);
static void pgaudit_drop_globalconfig(const DropGlobalConfigStmt* stmt, const char* cmdtext);
static void pgaudit_ddl_workload(const char* objectname, const char* cmdtext);
static void pgaudit_ddl_serverforhardoop(const char* objectname, const char* cmdtext);
static void pgaudit_ddl_model(const char* objectname, const char* cmdtext);
static void pgaudit_process_alter_object(Node* node, const char* querystring);
static void pgaudit_process_alter_owner(Node* node, const char* querystring);
static void pgaudit_process_drop_objects(Node* node, const char* querystring);
static void pgaudit_process_reindex(Node* node, const char* querystring);
static void pgaudit_process_rename_object(Node* node, const char* querystring);
static void pgaudit_process_grant_or_revoke_roles(List* grantee_name_list, bool isgrant, const char* querystring);
static void pgaudit_delete_files(const char* objectname, const char* cmdtext);
static void pgaudit_ddl_weak_password(const char* cmdtext);
static void pgaudit_ddl_full_encryption_key(const char* cmdtext);
static void pgaudit_ddl_type(const char* objectname, const char* cmdtext);
static void pgaudit_ddl_datasource(const char* objectname, const char* cmdtext);
static void pgaudit_ddl_rowlevelsecurity(const char* objectname, const char* cmdtext);
static void pgaudit_ddl_synonym(const char* objectName, const char* cmdText);
static void pgaudit_ddl_textsearch(const char* objectname, const char* cmdtext);
static void pgaudit_ddl_publication_subscription(const char* objectname, const char* cmdtext);
static void pgaudit_ddl_fdw(const char* objectname, const char* cmdtext);
static char* audit_get_func_args(FunctionCallInfo fcinfo);
static char* audit_get_text_array_value(ArrayType* array, int* numitems);
 
#define BUF_LENGTH 64
#define PG_GETARG_COMMANDID(n) DatumGetCommandId(PG_GETARG_DATUM(n))
#define SYSTEM_FUNC_DEFAULT_VALUE "NULL"

static const AuditFuncMap g_auditFuncMap[] = {
    {OBJECT_SCHEMA, pgaudit_ddl_schema},
    {OBJECT_TABLE, pgaudit_ddl_table},
    {OBJECT_FOREIGN_TABLE, pgaudit_ddl_table},
    {OBJECT_STREAM, pgaudit_ddl_table},
    {OBJECT_INTERNAL, pgaudit_ddl_table},
    {OBJECT_TABLESPACE, pgaudit_ddl_tablespace},
    {OBJECT_ROLE, pgaudit_ddl_user},
    {OBJECT_USER, pgaudit_ddl_user},
    {OBJECT_TRIGGER, pgaudit_ddl_trigger},
    {OBJECT_CONTQUERY, pgaudit_ddl_view},
    {OBJECT_VIEW, pgaudit_ddl_view},
    {OBJECT_MATVIEW, pgaudit_ddl_matview},
    {OBJECT_INDEX, pgaudit_ddl_index},
    {OBJECT_TYPE, pgaudit_ddl_type},
    {OBJECT_DATABASE, pgaudit_ddl_database},
    {OBJECT_FUNCTION, pgaudit_ddl_function},
    {OBJECT_PACKAGE, pgaudit_ddl_package},
    {OBJECT_FOREIGN_SERVER, pgaudit_ddl_serverforhardoop},
    {OBJECT_DATA_SOURCE, pgaudit_ddl_datasource},
    {OBJECT_DIRECTORY, pgaudit_ddl_directory},
    {OBJECT_RLSPOLICY, pgaudit_ddl_rowlevelsecurity},
    {OBJECT_SYNONYM, pgaudit_ddl_synonym},
    {OBJECT_TSDICTIONARY, pgaudit_ddl_textsearch},
    {OBJECT_TSCONFIGURATION, pgaudit_ddl_textsearch},
    {OBJECT_PUBLICATION, pgaudit_ddl_publication_subscription},
    {OBJECT_SUBSCRIPTION, pgaudit_ddl_publication_subscription},
    {OBJECT_FDW, pgaudit_ddl_fdw},
    {OBJECT_EVENT, pgaudit_ddl_event}
};
static const int g_auditFuncMapNum = sizeof(g_auditFuncMap) / sizeof(AuditFuncMap);

/*
 * Brief		    : perfstat_agent_init()
 * Description	: Module load callback.
 * Notes		    : Called from postmaster.
 */
void pgaudit_agent_init(void)
{
    if (!IsPostmasterEnvironment || !u_sess->attr.attr_security.Audit_enabled ||
        u_sess->exec_cxt.g_pgaudit_agent_attached) {
        return;
    }
    prev_ExecutorEnd = ExecutorEnd_hook;
    ExecutorEnd_hook = pgaudit_ExecutorEnd;
    prev_ProcessUtility = ProcessUtility_hook;
    ProcessUtility_hook = (ProcessUtility_hook_type)pgaudit_ProcessUtility;
    u_sess->exec_cxt.g_pgaudit_agent_attached = true;
}

/*
 * Brief		    : perfstat_agent_fini()
 * Description	: Module unload callback.
 */
void pgaudit_agent_fini(void)
{
    if (!u_sess->exec_cxt.g_pgaudit_agent_attached) {
        return;
    }
    u_sess->exec_cxt.g_pgaudit_agent_attached = false;
    /* Uninstall hooks. */
    ExecutorEnd_hook = prev_ExecutorEnd;
    ProcessUtility_hook = prev_ProcessUtility;
}

/*
 * Brief		    : void pgaudit_system_recovery_ok()
 * Description	    : audit the system recovery ok
 */
void pgaudit_system_recovery_ok()
{
    AuditType audit_type;
    AuditResult audit_result;
    char details[PGAUDIT_MAXLENGTH] = {0};
    audit_type = AUDIT_SYSTEM_RECOVER;
    audit_result = AUDIT_OK;
    int rc = snprintf_s(details, sizeof(details), sizeof(details) - 1, "system recovery success");
    securec_check_ss(rc, "", "");
    audit_report(audit_type, audit_result, NULL, details);
}

/*
 * Brief		    : void pgaudit_system_start_ok(int port)
 * Description	: audit the system startup
 */
void pgaudit_system_start_ok(int port)
{
    AuditType audit_type;
    AuditResult audit_result;
    char details[PGAUDIT_MAXLENGTH] = {0};
    audit_type = AUDIT_SYSTEM_START;
    audit_result = AUDIT_OK;
    int rc = snprintf_s(details, sizeof(details), sizeof(details) - 1, "system startup success(port = %d)", port);
    securec_check_ss(rc, "", "");
    audit_report(audit_type, audit_result, NULL, details);
}

/*
 * Brief		    : void pgaudit_user_login(bool login_ok, char* object_name,const char* detaisinfo)
 * Description	: audit the user login
 */
void pgaudit_user_login(bool login_ok, const char* object_name, const char* detailinfo)
{
    AuditType audit_type;
    AuditResult audit_result;
    Assert(detailinfo);
    if (login_ok) {
        audit_type = AUDIT_LOGIN_SUCCESS;
        audit_result = AUDIT_OK;
    } else {
        audit_type = AUDIT_LOGIN_FAILED;
        audit_result = AUDIT_FAILED;
    }

    char new_login_info[PGAUDIT_MAXLENGTH] = {0};
    Port *port = u_sess->proc_cxt.MyProcPort;
    if (port != NULL) {
        int rc = snprintf_s(new_login_info, PGAUDIT_MAXLENGTH, PGAUDIT_MAXLENGTH - 1, "%s, SSL=%s", detailinfo,
            port->ssl != NULL ? "on" : "off");
        securec_check_ss(rc, "", "");
    }
    audit_report(audit_type, audit_result, object_name, port != NULL ? new_login_info : detailinfo);
}

/*
 * Brief		    : void pgaudit_user_logout()
 * Description	: audit the user logout
 */
void pgaudit_user_logout()
{
    if (!u_sess->misc_cxt.authentication_finished)
        return;
    AuditType audit_type;
    AuditResult audit_result;
    char details[PGAUDIT_MAXLENGTH] = {0};
    audit_type = AUDIT_USER_LOGOUT;
    audit_result = AUDIT_OK;
    TimestampTz now = GetCurrentTimestamp();
    int rc;
    /* check is logout or timeout */
    if (now >= u_sess->storage_cxt.session_fin_time && u_sess->attr.attr_common.SessionTimeout) {
        rc = snprintf_s(details,
            sizeof(details),
            sizeof(details) - 1,
            "session timeout, logout db(%s) success",
            u_sess->proc_cxt.MyProcPort->database_name);
    } else {
        rc = snprintf_s(details,
            sizeof(details),
            sizeof(details) - 1,
            "logout db(%s) success",
            u_sess->proc_cxt.MyProcPort->database_name);
    }
    securec_check_ss(rc, "", "");
    audit_report(audit_type, audit_result, u_sess->proc_cxt.MyProcPort->database_name, details);
}

/*
 * Brief		    : void pgaudit_system_stop_ok(int shutdown)
 * Description	: audit the system stop
 */
void pgaudit_system_stop_ok(int shutdown)
{

    AuditType audit_type;
    AuditResult audit_result;
    const char* shutdowmothods[] = {"smart", "fast", "immediate"};
    char details[PGAUDIT_MAXLENGTH] = {0};
    if (shutdown > ImmediateShutdown || shutdown < SmartShutdown)
        return;
    audit_type = AUDIT_SYSTEM_STOP;
    audit_result = AUDIT_OK;
    int rc = snprintf_s(
        details, sizeof(details), sizeof(details) - 1, "system stop %s success", shutdowmothods[shutdown - 1]);
    securec_check_ss(rc, "", "");
    audit_report(audit_type, audit_result, NULL, details);
}

/*
 * Brief		    : void pgaudit_system_switchover_ok(const char* detaisinfo)
 * Description	: audit the system switchover
 */
void pgaudit_system_switchover_ok(const char* detaisinfo)
{
    AuditType audit_type;
    AuditResult audit_result;
    char details[PGAUDIT_MAXLENGTH] = {0};
    Assert(detaisinfo);
    audit_type = AUDIT_SYSTEM_SWITCH;
    audit_result = AUDIT_OK;
    int rc = snprintf_s(details, sizeof(details), sizeof(details) - 1, "%s", detaisinfo);
    securec_check_ss(rc, "", "");
    audit_report(audit_type, audit_result, NULL, details);
}

/*
 * Brief		    : void pgaudit_user_no_privileges(const char* object_name,const char* detailsinfo)
 * Description	: audit the user have no privileges
 */
void pgaudit_user_no_privileges(const char* object_name, const char* detailsinfo)
{
    AuditType audit_type;
    AuditResult audit_result;
    audit_type = AUDIT_USER_VIOLATION;
    audit_result = AUDIT_FAILED;
    audit_report(audit_type, audit_result, object_name, detailsinfo);
}

/*
 * Brief		    : void pgaudit_lock_or_unlock_user(bool islocked,const char* user_name)
 * Description	: audit lock or unlock user
 */
void pgaudit_lock_or_unlock_user(bool islocked, const char* user_name)
{
    AuditType audit_type;
    AuditResult audit_result;
    char details[PGAUDIT_MAXLENGTH] = {0};
    int rc = 0;
    if (user_name == NULL) {
        user_name = "UNKOWN USER";
    }
    if (islocked) {
        audit_type = AUDIT_LOCK_USER;
        audit_result = AUDIT_OK;
        rc = snprintf_s(details, sizeof(details), sizeof(details) - 1, "the user(%s) has been locked", user_name);
    } else {
        audit_type = AUDIT_UNLOCK_USER;
        audit_result = AUDIT_OK;
        rc = snprintf_s(details, sizeof(details), sizeof(details) - 1, "the user(%s) has been unlocked", user_name);
    }
    securec_check_ss(rc, "", "");
    audit_report(audit_type, audit_result, user_name, details);
}

/*
 * Description: store the audit informations
 */
static void pgaudit_store_auditstat(
    AuditType audittype, AuditResult auditresult, const char* objectname, const char* detailsinfo)
{
    Assert(detailsinfo);
    audit_report(audittype, auditresult, objectname, detailsinfo);
}

/*
 * Description :Audit the ddl cmd
 */
static void pgaudit_ddl_database_object(
    AuditType audit_type, AuditResult audit_result, const char* objectname, const char* cmdtext)
{
    Assert(cmdtext != NULL);
    char* mask_string = maskPassword(cmdtext);
    if (mask_string == NULL) {
        if (u_sess->attr.attr_sql.dolphin) {
            mask_string = mask_error_password(cmdtext, strlen(cmdtext));
        } else {
            mask_string = (char*)cmdtext;
        }
    }

    switch (audit_type) {
        case AUDIT_DDL_DATABASE:
        case AUDIT_DDL_DIRECTORY:
        case AUDIT_DDL_INDEX:
        case AUDIT_DDL_SCHEMA:
        case AUDIT_DDL_FUNCTION:
        case AUDIT_DDL_PACKAGE:
        case AUDIT_DDL_TABLE:
        case AUDIT_DDL_TABLESPACE:
        case AUDIT_DDL_TRIGGER:
        case AUDIT_DDL_USER:
        case AUDIT_DDL_VIEW:
        case AUDIT_DDL_EVENT:
        case AUDIT_DDL_RESOURCEPOOL:
        case AUDIT_DDL_GLOBALCONFIG:
        case AUDIT_DDL_WORKLOAD:
        case AUDIT_DDL_SERVERFORHADOOP:
        case AUDIT_DDL_DATASOURCE:
        case AUDIT_DDL_NODEGROUP:
        case AUDIT_DDL_ROWLEVELSECURITY:
        case AUDIT_DDL_SYNONYM:
        case AUDIT_DDL_TYPE:
        case AUDIT_DDL_TEXTSEARCH:
        case AUDIT_DDL_SEQUENCE:
        case AUDIT_DDL_KEY:
        case AUDIT_DDL_MODEL:
        case AUDIT_DDL_PUBLICATION_SUBSCRIPTION:
        case AUDIT_DDL_FOREIGN_DATA_WRAPPER:
        case AUDIT_DDL_SQL_PATCH:
            pgaudit_store_auditstat(audit_type, audit_result, objectname, mask_string);
            break;
        default:
            ereport(LOG, (errmsg("UNKOWN AUDIT TYPE FOR DDL OPERATION.")));
            break;
    }

    if (mask_string != cmdtext) {
        selfpfree(mask_string);
    }

    return;
}

/*
 * Brief		    :void pgaudit_dml_table(char* objectname,const char* cmdtext)
 * Description	:Audit the DML cmd
 */
void pgaudit_dml_table(const char* objectname, const char* cmdtext)
{
    AuditType audit_type = AUDIT_DML_ACTION;
    AuditResult audit_result = AUDIT_OK;
    char* mask_string = NULL;
    Assert(cmdtext);
    bool is_full_audit_user = audit_check_full_audit_user();
    if (u_sess->attr.attr_security.Audit_DML == 0 && !is_full_audit_user) {
        return;
    }

    mask_string = maskPassword(cmdtext);
    if (mask_string == NULL)
        mask_string = (char*)cmdtext;
    pgaudit_store_auditstat(audit_type, audit_result, objectname, mask_string);
    if (mask_string != cmdtext)
        pfree(mask_string);
}

/*
 * Brief		    :void pgaudit_dml_select(char* objectname,const char* cmdtext)
 * Description	:Audit the DML select cmd
 */
void pgaudit_dml_table_select(const char* objectname, const char* cmdtext)
{
    AuditType audit_type = AUDIT_DML_ACTION_SELECT;
    AuditResult audit_result = AUDIT_OK;
    char* mask_string = NULL;
    bool is_full_audit_user = audit_check_full_audit_user();
    if (u_sess->attr.attr_security.Audit_DML_SELECT == 0 && !is_full_audit_user) {
        return;
    }

    mask_string = maskPassword(cmdtext);
    if (mask_string == NULL)
        mask_string = (char*)cmdtext;
    pgaudit_store_auditstat(audit_type, audit_result, objectname, mask_string);
    if (mask_string != cmdtext)
        pfree(mask_string);
}

/*
 * Brief		    : pgaudit_delete_files(char* objectname,const char* cmdtext)
 * Description	: audit delete DDL operations of the given table
 */
static void pgaudit_delete_files(const char* objectname, const char* cmdtext)
{
    AuditType audit_type;
    AuditResult audit_result;
    Assert(cmdtext);
    audit_type = AUDIT_INTERNAL_EVENT;
    audit_result = AUDIT_OK;
    pgaudit_store_auditstat(audit_type, audit_result, objectname, cmdtext);
}

/*
 * Brief		    : pgaudit_ddl_table(char* objectname, const char* cmdtext)
 * Description	: audit DDL operations of the given table
 */
static void pgaudit_ddl_table(const char* objectname, const char* cmdtext)
{
    AuditType audit_type = AUDIT_DDL_TABLE;
    AuditResult audit_result = AUDIT_OK;

    Assert(cmdtext != NULL);
    if (!CHECK_AUDIT_DDL(DDL_TABLE)) {
        return;
    }

    pgaudit_ddl_database_object(audit_type, audit_result, objectname, cmdtext);
    return;
}

/*
 * Brief		    : pgaudit_ddl_type(char* objectname, const char* cmdtext)
 * Description	: audit DDL operations of the given type
 */
static void pgaudit_ddl_type(const char* objectname, const char* cmdtext)
{
    AuditType audit_type = AUDIT_DDL_TYPE;
    AuditResult audit_result = AUDIT_OK;

    Assert(cmdtext != NULL);
    if (!CHECK_AUDIT_DDL(DDL_TYPE)) {
        return;
    }

    pgaudit_ddl_database_object(audit_type, audit_result, objectname, cmdtext);
    return;
}

/*
 * Brief		    : pgaudit_ddl_user(char* objectname, const char* cmdtext)
 * Description	: Audit the operations of user
 */
static void pgaudit_ddl_user(const char* objectname, const char* cmdtext)
{
    AuditType audit_type = AUDIT_DDL_USER;
    AuditResult audit_result = AUDIT_OK;

    Assert(cmdtext != NULL);
    if (!CHECK_AUDIT_DDL(DDL_USER)) {
        return;
    }

    pgaudit_ddl_database_object(audit_type, audit_result, objectname, cmdtext);
    return;
}

static void pgaudit_ddl_model(const char* objectname, const char* cmdtext)
{
    AuditType audit_type = AUDIT_DDL_MODEL;
    AuditResult audit_result = AUDIT_OK;

    Assert(cmdtext != NULL);
    if (!CHECK_AUDIT_DDL(DDL_MODEL)) {
        return;
    }

    pgaudit_ddl_database_object(audit_type, audit_result, objectname, cmdtext);
    return;
}

/*
 * Brief		    : pgaudit_ddl_view(char* objectname, const char* cmdtext)
 * Description	: Audit the operations of view
 */
static void pgaudit_ddl_view(const char* objectname, const char* cmdtext)
{
    AuditType audit_type = AUDIT_DDL_VIEW;
    AuditResult audit_result = AUDIT_OK;

    Assert(cmdtext != NULL);
    if (!CHECK_AUDIT_DDL(DDL_VIEW)) {
        return;
    }

    pgaudit_ddl_database_object(audit_type, audit_result, objectname, cmdtext);
    return;
}

static void pgaudit_ddl_event(const char* objectname, const char* cmdtext)
{
    AuditType audit_type = AUDIT_DDL_EVENT;
    AuditResult audit_result = AUDIT_OK;

    Assert(cmdtext != NULL);
    if (!CHECK_AUDIT_DDL(DDL_EVENT)) {
        return;
    }

    pgaudit_ddl_database_object(audit_type, audit_result, objectname, cmdtext);
    return;
}

/*
 * Brief		    : pgaudit_ddl_matview(char* objectname, const char* cmdtext)
 * Description	: Audit the operations of matview
 */
static void pgaudit_ddl_matview(const char* objectname, const char* cmdtext)
{
    AuditType audit_type = AUDIT_DDL_VIEW;
    AuditResult audit_result = AUDIT_OK;

    Assert(cmdtext != NULL);
    if (!CHECK_AUDIT_DDL(DDL_VIEW)) {
        return;
    }

    pgaudit_ddl_database_object(audit_type, audit_result, objectname, cmdtext);
    return;
}

/*
 * Brief		    : pgaudit_ddl_database(char* objectname, const char* cmdtext)
 * Description	: Audit the operations of database
 */
static void pgaudit_ddl_database(const char* objectname, const char* cmdtext)
{
    AuditType audit_type = AUDIT_DDL_DATABASE;
    AuditResult audit_result = AUDIT_OK;

    Assert(cmdtext != NULL);
    if (!CHECK_AUDIT_DDL(DDL_DATABASE)) {
        return;
    }

    pgaudit_ddl_database_object(audit_type, audit_result, objectname, cmdtext);
    return;
}

/*
 * Brief		    : pgaudit_ddl_directory(char* objectname, const char* cmdtext)
 * Description	: Audit the operations of directory
 */
static void pgaudit_ddl_directory(const char* objectname, const char* cmdtext)
{
    AuditType audit_type = AUDIT_DDL_DIRECTORY;
    AuditResult audit_result = AUDIT_OK;

    Assert(cmdtext != NULL);
    if (!CHECK_AUDIT_DDL(DDL_DIRECTORY)) {
        return;
    }

    pgaudit_ddl_database_object(audit_type, audit_result, objectname, cmdtext);
    return;
}

/*
 * Brief		    : pgaudit_ddl_schema(char* objectname, const char* cmdtext)
 * Description	: Audit the operations of schema
 */
static void pgaudit_ddl_schema(const char* objectname, const char* cmdtext)
{
    AuditType audit_type = AUDIT_DDL_SCHEMA;
    AuditResult audit_result = AUDIT_OK;

    Assert(cmdtext != NULL);
    if (!CHECK_AUDIT_DDL(DDL_SCHEMA)) {
        return;
    }

    pgaudit_ddl_database_object(audit_type, audit_result, objectname, cmdtext);
    return;
}

/*
 * Brief		    : pgaudit_ddl_tablespace(char* objectname, const char* cmdtext)
 * Description	: Audit the operations of tablespace
 */
static void pgaudit_ddl_tablespace(const char* objectname, const char* cmdtext)
{
    AuditType audit_type = AUDIT_DDL_TABLESPACE;
    AuditResult audit_result = AUDIT_OK;

    Assert(cmdtext != NULL);
    if (!CHECK_AUDIT_DDL(DDL_TABLESPACE)) {
        return;
    }

    pgaudit_ddl_database_object(audit_type, audit_result, objectname, cmdtext);
    return;
}

/*
 * Brief		    : pgaudit_grant_or_revoke_role(bool isgrant, char* objectname, const char* cmdtext)
 * Description	: Audit the operations of role,grant or revoke
 */
static void pgaudit_grant_or_revoke_role(bool isgrant, const char* objectname, const char* cmdtext)
{
    AuditType audit_type = isgrant ? AUDIT_GRANT_ROLE : AUDIT_REVOKE_ROLE;
    AuditResult audit_result = AUDIT_OK;
    char* mask_string = NULL;

    Assert(cmdtext != NULL);
    if (u_sess->attr.attr_security.Audit_PrivilegeAdmin == 0  && !audit_check_full_audit_user()) {
        return;
    }
    mask_string = maskPassword(cmdtext);
    if (mask_string == NULL) {
        mask_string = (char*)cmdtext;
    }

    pgaudit_store_auditstat(audit_type, audit_result, objectname, mask_string);
    if (mask_string != cmdtext) {
        selfpfree(mask_string);
    }
    return;
}

/*
 * Brief		    : pgaudit_ddl_index(const char* objectname, const char* cmdtext)
 * Description	: Audit the operations of index
 */
static void pgaudit_ddl_index(const char* objectname, const char* cmdtext)
{
    AuditType audit_type = AUDIT_DDL_INDEX;
    AuditResult audit_result = AUDIT_OK;

    Assert(cmdtext != NULL);
    if (!CHECK_AUDIT_DDL(DDL_INDEX)) {
        return;
    }

    pgaudit_ddl_database_object(audit_type, audit_result, objectname, cmdtext);
    return;
}

/*
 * Brief		    : pgaudit_ddl_trigger(const char* objectname, const char* cmdtext)
 * Description	: Audit the operations of trigger
 */
static void pgaudit_ddl_trigger(const char* objectname, const char* cmdtext)
{
    AuditType audit_type = AUDIT_DDL_TRIGGER;
    AuditResult audit_result = AUDIT_OK;

    Assert(cmdtext != NULL);
    if (!CHECK_AUDIT_DDL(DDL_TRIGGER)) {
        return;
    }

    pgaudit_ddl_database_object(audit_type, audit_result, objectname, cmdtext);
    return;
}

/*
 * Brief		    : pgaudit_ddl_sequence(const char* objectname, const char* cmdtext)
 * Description	: Audit the operations of sequence
 */
static void pgaudit_ddl_sequence(const char* objectname, const char* cmdtext)
{
    AuditType audit_type = AUDIT_DDL_SEQUENCE;
    AuditResult audit_result = AUDIT_OK;

    Assert(cmdtext != NULL);
    if (!CHECK_AUDIT_DDL(DDL_SEQUENCE)) {
        return;
    }

    pgaudit_ddl_database_object(audit_type, audit_result, objectname, cmdtext);
    return;
}

/*
 * Brief		    : pgaudit_ddl_function(const char* objectname, const char* cmdtext)
 * Description	: Audit the operations of function
 */
static void pgaudit_ddl_function(const char* objectname, const char* cmdtext)
{
    AuditType audit_type = AUDIT_DDL_FUNCTION;
    AuditResult audit_result = AUDIT_OK;

    Assert(cmdtext != NULL);
    if (!CHECK_AUDIT_DDL(DDL_FUNCTION)) {
        return;
    }

    pgaudit_ddl_database_object(audit_type, audit_result, objectname, cmdtext);
    return;
}

/*
 * Brief		    : pgaudit_ddl_package(const char* objectname, const char* cmdtext)
 * Description	: Audit the operations of package
 */
static void pgaudit_ddl_package(const char* objectname, const char* cmdtext)
{
    AuditType audit_type = AUDIT_DDL_PACKAGE;
    AuditResult audit_result = AUDIT_OK;

    Assert(cmdtext != NULL);
    if (!CHECK_AUDIT_DDL(DDL_PACKAGE)) {
        return;
    }
    pgaudit_ddl_database_object(audit_type, audit_result, objectname, cmdtext);
    return;
}

/*
 * Brief		    : pgaudit_ddl_resourcepool(const char* objectname, const char* cmdtext)
 * Description	: Audit the operations of resource pool
 */
static void pgaudit_ddl_resourcepool(const char* objectname, const char* cmdtext)
{
    AuditType audit_type = AUDIT_DDL_RESOURCEPOOL;
    AuditResult audit_result = AUDIT_OK;

    Assert(cmdtext != NULL);
    if (!CHECK_AUDIT_DDL(DDL_RESOURCEPOOL)) {
        return;
    }

    pgaudit_ddl_database_object(audit_type, audit_result, objectname, cmdtext);
    return;
}

/*
 * Brief		    : pgaudit_ddl_resourcepool(const char* objectname, const char* cmdtext)
 * Description	: Audit the operations of gs_global_config
 */
static void pgaudit_alter_globalconfig(const AlterGlobalConfigStmt* stmt, const char* cmdtext)
{
    Assert(cmdtext != NULL);
    if (!CHECK_AUDIT_DDL(DDL_GLOBALCONFIG)) {
        return;
    }
    ListCell* option = NULL;
    foreach (option, stmt->options) {
        DefElem *defel = (DefElem *)lfirst(option);
        pgaudit_ddl_database_object(AUDIT_DDL_GLOBALCONFIG, AUDIT_OK, defel->defname, cmdtext);
    }
    return;
}

/*
 * Brief		    : pgaudit_ddl_resourcepool(const char* objectname, const char* cmdtext)
 * Description	: Audit the operations of gs_global_config
 */
static void pgaudit_drop_globalconfig(const DropGlobalConfigStmt* stmt, const char* cmdtext)
{
    Assert(cmdtext != NULL);
    if (!CHECK_AUDIT_DDL(DDL_GLOBALCONFIG)) {
        return;
    }
    ListCell* option = NULL;
    foreach (option, stmt->options) {
        const char *global_name = strVal(lfirst(option));
        pgaudit_ddl_database_object(AUDIT_DDL_GLOBALCONFIG, AUDIT_OK, global_name, cmdtext);
    }
    return;
}

/*
 * Brief		    : pgaudit_ddl_workload(const char* objectname, const char* cmdtext)
 * Description	: Audit the operations of workload
 */
static void pgaudit_ddl_workload(const char* objectname, const char* cmdtext)
{
    AuditType audit_type = AUDIT_DDL_WORKLOAD;
    AuditResult audit_result = AUDIT_OK;

    Assert(cmdtext != NULL);
    if (!CHECK_AUDIT_DDL(DDL_WORKLOAD)) {
        return;
    }

    pgaudit_ddl_database_object(audit_type, audit_result, objectname, cmdtext);
    return;
}

/*
 * Brief		    : pgaudit_ddl_serverforhardoop(const char* objectname, const char* cmdtext)
 * Description	: Audit the operations of server for hardoop
 */
static void pgaudit_ddl_serverforhardoop(const char* objectname, const char* cmdtext)
{
    AuditType audit_type = AUDIT_DDL_SERVERFORHADOOP;
    AuditResult audit_result = AUDIT_OK;

    Assert(cmdtext != NULL);
    if (!CHECK_AUDIT_DDL(DDL_SERVERFORHADOOP)) {
        return;
    }

    pgaudit_ddl_database_object(audit_type, audit_result, objectname, cmdtext);
    return;
}

/*
 * pgaudit_ddl_weak_password:
 *             AUDIT FOR WEAK PASSWORD DICTIONARY
 * 	
 * @RETURN: void
 */
static void pgaudit_ddl_weak_password(const char* cmdtext)
{
    AuditType audit_type = AUDIT_DDL_TABLE;
    AuditResult audit_result = AUDIT_OK;

    Assert(cmdtext != NULL);
    if (!CHECK_AUDIT_DDL(DDL_TABLE)) {
        return;
    }

    pgaudit_ddl_database_object(audit_type, audit_result, "weak_password", cmdtext);
    return;
}
/*
 * pgaudit_ddl_full_encryption_key:
 *             AUDIT FOR security client key create
 * 	
 * @RETURN: void
 */
static void pgaudit_ddl_full_encryption_key(const char* cmdtext)
{
    AuditType audit_type = AUDIT_DDL_KEY;
    AuditResult audit_result = AUDIT_OK;

    Assert(cmdtext != NULL);
    if (!CHECK_AUDIT_DDL(DDL_KEY)) {
        return;
    }

    char *making_query = (char *)mask_encrypted_key(cmdtext, strlen(cmdtext));
    pgaudit_ddl_database_object(audit_type, audit_result, "full_encryption_key", making_query);
    if (making_query != cmdtext) {
        selfpfree(making_query);
    }
    return;
}

/*
 * pgaudit_ddl_datasource:
 * 	Audit the operations of data source
 *
 * @IN objectname:  data source name
 * @IN cmdtext:  cmd string
 * @RETURN: void
 */
static void pgaudit_ddl_datasource(const char* objectname, const char* cmdtext)
{
    AuditType audit_type = AUDIT_DDL_DATASOURCE;
    AuditResult audit_result = AUDIT_OK;

    Assert(cmdtext != NULL);
    if (!CHECK_AUDIT_DDL(DDL_DATASOURCE)) {
        return;
    }

    pgaudit_ddl_database_object(audit_type, audit_result, objectname, cmdtext);
    return;
}

/*
 * pgaudit_ddl_nodegroup:
 *	 Audit the operations of node group
 *
 * @IN objectname:	node group name
 * @IN cmdtext:  cmd string
 * @RETURN: void
 */
static void pgaudit_ddl_nodegroup(const char* objectname, const char* cmdtext)
{
    AuditType audit_type = AUDIT_DDL_NODEGROUP;
    AuditResult audit_result = AUDIT_OK;

    Assert(cmdtext != NULL);
    if (!CHECK_AUDIT_DDL(DDL_NODEGROUP)) {
        return;
    }

    pgaudit_ddl_database_object(audit_type, audit_result, objectname, cmdtext);
    return;
}

/*
 * pgaudit_ddl_rowlevelsecurity:
 *	 Audit the operations of node row level security
 *
 * @IN objectname:	node row level security policy name
 * @IN cmdtext:  cmd string
 * @RETURN: void
 */
static void pgaudit_ddl_rowlevelsecurity(const char* objectname, const char* cmdtext)
{
    AuditType audit_type = AUDIT_DDL_ROWLEVELSECURITY;
    AuditResult audit_result = AUDIT_OK;

    Assert(cmdtext != NULL);
    if (!CHECK_AUDIT_DDL(DDL_ROWLEVELSECURITY)) {
        return;
    }

    pgaudit_ddl_database_object(audit_type, audit_result, objectname, cmdtext);
    return;
}

/*
 * pgaudit_ddl_synonym:
 *	 Audit the operations of synonym
 *
 * @IN objectname:	synonym name
 * @IN cmdtext:  cmd string
 * @RETURN: void
 */
static void pgaudit_ddl_synonym(const char* objectName, const char* cmdText)
{
    AuditType auditType = AUDIT_DDL_SYNONYM;
    AuditResult auditResult = AUDIT_OK;

    Assert(cmdText);
    if (!CHECK_AUDIT_DDL(DDL_SYNONYM)) {
        return;
    }

    pgaudit_ddl_database_object(auditType, auditResult, objectName, cmdText);
    return;
}

/*
 * Brief		: pgaudit_ddl_textsearch(char* objectname, const char* cmdtext)
 * Description	: audit DDL operations of the given text search object
 */
static void pgaudit_ddl_textsearch(const char* objectname, const char* cmdtext)
{
    AuditType audit_type = AUDIT_DDL_TEXTSEARCH;
    AuditResult audit_result = AUDIT_OK;

    Assert(cmdtext != NULL);
    if (!CHECK_AUDIT_DDL(DDL_TEXTSEARCH)) {
        return;
    }

    pgaudit_ddl_database_object(audit_type, audit_result, objectname, cmdtext);
    return;
}

/*
 * pgaudit_ddl_publication_subscription:
 * 	Audit the operations of publication and subscription
 *
 * @IN objectname:  publication name or subscription name
 * @IN cmdtext:  cmd string
 * @RETURN: void
 */
static void pgaudit_ddl_publication_subscription(const char* objectname, const char* cmdtext)
{
    if (!CHECK_AUDIT_DDL(DDL_PUBLICATION_SUBSCRIPTION)) {
        return;
    }

    pgaudit_ddl_database_object(AUDIT_DDL_PUBLICATION_SUBSCRIPTION, AUDIT_OK, objectname, cmdtext);
    return;
}

/*
 * pgaudit_ddl_fdw:
 * 	Audit the operations of foreign data wrapper
 *
 * @IN objectname:  foreign data wrapper name
 * @IN cmdtext:  cmd string
 * @RETURN: void
 */
static void pgaudit_ddl_fdw(const char* objectname, const char* cmdtext)
{
    if (!CHECK_AUDIT_DDL(DDL_FOREIGN_DATA_WRAPPER)) {
        return;
    }

    pgaudit_ddl_database_object(AUDIT_DDL_FOREIGN_DATA_WRAPPER, AUDIT_OK, objectname, cmdtext);
    return;
}

void pgaudit_ddl_sql_patch(const char* objectname, const char* cmdtext)
{
    if (!CHECK_AUDIT_DDL(DDL_SQL_PATCH)) {
        return;
    }

    pgaudit_ddl_database_object(AUDIT_DDL_SQL_PATCH, AUDIT_OK, objectname, cmdtext);
}

/*
 * @Description: audit the operation of set parameter.
 * @in objectname : the object name need audited.
 * @in cmdtext : the command text need audited.
 * @return : non-return.
 */
static void pgaudit_process_set_parameter(const char* objectname, const char* cmdtext)
{
    AuditType audit_type = AUDIT_SET_PARAMETER;
    AuditResult audit_result = AUDIT_OK;
    char* mask_string = NULL;

    Assert(cmdtext != NULL);
    if (u_sess->attr.attr_security.Audit_Set == 0 && !audit_check_full_audit_user()) {
        return;
    }

    /* make the cmdtext which may contain senstive info like password. */
    mask_string = maskPassword(cmdtext);
    if (mask_string == NULL)
        mask_string = (char*)cmdtext;

    /* the real place where audit the info to the system auditor */
    audit_report(audit_type, audit_result, objectname, mask_string);

    if (mask_string != cmdtext)
        pfree(mask_string);
}

/*
 * Brief		    : pgaudit_process_drop_objects(Node* node,const char* querystring)
 * Description	: Audit the drop operation
 */
static void pgaudit_process_drop_objects(Node* node, const char* querystring)
{
    DropStmt* stmt = (DropStmt*)node;
    ListCell* arg = NULL;
    char* objectname = NULL;
    foreach (arg, stmt->objects) {
        List* names = NIL;
        RangeVar* rel = NULL;
        if (stmt->removeType == OBJECT_DOMAIN ||
             stmt->removeType == OBJECT_TYPE ) {
        List *objname = (List*)lfirst(arg);
        Node *ptype = (Node *) linitial(objname);
        TypeName* typname = NULL;
        if(ptype->type == T_String)
            typname = makeTypeNameFromNameList(list_make1(ptype));
        else if(ptype->type == T_TypeName)
            typname = (TypeName *) linitial(objname);
        else
            ereport(ERROR, (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE), errmsg("unkonw type: %d", (int)ptype->type)));
        names = typname->names;
        } else {
            names = (List*)lfirst(arg);
        }
        switch (stmt->removeType) {
            case OBJECT_TABLE:
            case OBJECT_STREAM:
            case OBJECT_FOREIGN_TABLE: {
                rel = makeRangeVarFromNameList(names);
                objectname = rel->relname;
                pgaudit_ddl_table(objectname, querystring);
            } break;
            case OBJECT_CONTQUERY:
            case OBJECT_VIEW: {
                rel = makeRangeVarFromNameList(names);
                objectname = rel->relname;
                pgaudit_ddl_view(objectname, querystring);
            } break;
            case OBJECT_EVENT: {
                rel = makeRangeVarFromNameList(names);
                objectname = rel->relname;
                pgaudit_ddl_view(objectname, querystring);
            } break;
            case OBJECT_MATVIEW: {
                rel = makeRangeVarFromNameList(names);
                objectname = rel->relname;
                pgaudit_ddl_matview(objectname, querystring);
            } break;
            case OBJECT_INDEX: {
                rel = makeRangeVarFromNameList(names);
                objectname = rel->relname;
                pgaudit_ddl_index(objectname, querystring);
            } break;
            case OBJECT_SCHEMA: {
                objectname = strVal(linitial(names));
                pgaudit_ddl_schema(objectname, querystring);
            } break;
            case OBJECT_TRIGGER: {
                objectname = strVal(lfirst(list_tail((List*)lfirst(list_tail(names)))));
                pgaudit_ddl_trigger(objectname, querystring);
            } break;
            case OBJECT_FUNCTION: {
                objectname = NameListToString(names);
                pgaudit_ddl_function(objectname, querystring);
            } break;
            case OBJECT_PACKAGE:
            case OBJECT_PACKAGE_BODY: {
                objectname = NameListToString(names);
                pgaudit_ddl_package(objectname, querystring);
            } break;
            case OBJECT_FOREIGN_SERVER: {
                objectname = NameListToString(names);
                pgaudit_ddl_serverforhardoop(objectname, querystring);
            } break;
            case OBJECT_DATA_SOURCE: {
                objectname = NameListToString(names);
                pgaudit_ddl_datasource(objectname, querystring);
            } break;
            case OBJECT_RLSPOLICY: {
                objectname = strVal(lfirst(list_tail(names)));
                pgaudit_ddl_rowlevelsecurity(objectname, querystring);
            } break;
            case OBJECT_TYPE: {
                objectname = NameListToString(names);
                pgaudit_ddl_type(objectname, querystring);
            } break;
            case OBJECT_TSDICTIONARY:
            case OBJECT_TSCONFIGURATION: {
                objectname = NameListToString(names);
                pgaudit_ddl_textsearch(objectname, querystring);
            } break;
            case OBJECT_SEQUENCE:
            case OBJECT_LARGE_SEQUENCE: {
                objectname = strVal(lfirst(list_tail(names)));
                pgaudit_ddl_sequence(objectname, querystring);
            } break;
            case OBJECT_GLOBAL_SETTING:
            case OBJECT_COLUMN_SETTING: {
                pgaudit_ddl_full_encryption_key(querystring);
            } break;
            case OBJECT_DB4AI_MODEL: {
                rel = makeRangeVarFromNameList(names);
                objectname = rel->relname;
                pgaudit_ddl_model(objectname, querystring);
            } break;
            case OBJECT_PUBLICATION:
                objectname = NameListToString(names);
                pgaudit_ddl_publication_subscription(objectname, querystring);
                break;
            case OBJECT_FDW:
                objectname = NameListToString(names);
                pgaudit_ddl_fdw(objectname, querystring);
                break;
            default:
                break;
        }
    }
}

/*
 * Brief		    : pgaudit_audit_object(const char* objname,int ObjectType,const char* cmdtext)
 * Description	: Audit the operations of database objects
 */
static void pgaudit_audit_object(const char* objname, int ObjectType, const char* cmdtext)
{
    Assert(cmdtext);
    for (int i = 0; i < g_auditFuncMapNum; i++) {
        if (g_auditFuncMap[i].objType == ObjectType) {
            g_auditFuncMap[i].auditFunc(objname, cmdtext);
            return;
        }
    }
}

/*
 * Brief		    : pgaudit_process_alter_owner(Node * node,const char* querystring)
 * Description	: process alter owner operation
 */
static void pgaudit_process_alter_owner(Node* node, const char* querystring)
{
    char* objectname = NULL;
    AlterOwnerStmt* alterownerstmt = (AlterOwnerStmt*)(node);

    switch (alterownerstmt->objectType) {
        case OBJECT_DATABASE:
        case OBJECT_DATA_SOURCE:
        case OBJECT_DIRECTORY:
        case OBJECT_FDW:
        case OBJECT_FOREIGN_SERVER:
        case OBJECT_LANGUAGE:
        case OBJECT_LARGEOBJECT:
        case OBJECT_SCHEMA:
        case OBJECT_TABLESPACE:
            objectname = strVal(linitial(alterownerstmt->object));
            break;
        case OBJECT_COLLATION:
        case OBJECT_CONVERSION:
        case OBJECT_DOMAIN:
        case OBJECT_FUNCTION:
        case OBJECT_OPCLASS:
        case OBJECT_OPERATOR:
        case OBJECT_OPFAMILY:
        case OBJECT_SYNONYM:
        case OBJECT_TSDICTIONARY:
        case OBJECT_TSCONFIGURATION:
        case OBJECT_TYPE:
        case OBJECT_PACKAGE:
        case OBJECT_PUBLICATION:
        case OBJECT_SUBSCRIPTION:
            objectname = NameListToString(alterownerstmt->object);
            break;
        default:
            break;
    }
    pgaudit_audit_object(objectname, alterownerstmt->objectType, querystring);
}

/*
 * Brief		    : pgaudit_process_reindex(Node* node,const char* querystring)
 * Description	: process reinex operation
 */
static void pgaudit_process_reindex(Node* node, const char* querystring)
{
    ReindexStmt* reindexstmt = (ReindexStmt*)(node);
    switch (reindexstmt->kind) {
        case OBJECT_INDEX:
        case OBJECT_TABLE:
        case OBJECT_INTERNAL:
            pgaudit_audit_object(reindexstmt->relation->relname, reindexstmt->kind, querystring);
            break;
        case OBJECT_DATABASE:
            pgaudit_audit_object(reindexstmt->name, reindexstmt->kind, querystring);
            break;
        default:
            break;
    }
}

/*
 * Brief		    : pgaudit_process_rename_object(Node* node,const char* querystring)
 * Description	: process the rename operation
 */
static void pgaudit_process_rename_object(Node* node, const char* querystring)
{
    RenameStmt* stmt = (RenameStmt*)(node);
    char* objectname = NULL;

    switch (stmt->renameType) {
        case OBJECT_DATABASE:
        case OBJECT_ROLE:
        case OBJECT_USER:
        case OBJECT_SCHEMA:
        case OBJECT_TABLESPACE:
        case OBJECT_TRIGGER:
        case OBJECT_RLSPOLICY:
        case OBJECT_DATA_SOURCE:
            objectname = stmt->subname;
            break;
        case OBJECT_EVENT_TRIGGER:
        case OBJECT_FDW:
        case OBJECT_FOREIGN_SERVER:
        case OBJECT_FUNCTION:
        case OBJECT_TYPE:
        case OBJECT_TSDICTIONARY:
        case OBJECT_TSCONFIGURATION:
        case OBJECT_PUBLICATION:
        case OBJECT_SUBSCRIPTION:
            objectname = NameListToString(stmt->object);
            break;
        case OBJECT_TABLE:
        case OBJECT_CONTQUERY:
        case OBJECT_VIEW:
        case OBJECT_MATVIEW:
        case OBJECT_INDEX:
        case OBJECT_FOREIGN_TABLE:
        case OBJECT_STREAM:
            if (stmt->renameTableflag && PointerIsValid(stmt->renameTargetList) &&
                    PointerIsValid(stmt->renameTargetList->head)) {
                objectname = ((RenameCell*)stmt->renameTargetList->head->data.ptr_value)->original_name->relname;
            } else {
                objectname = stmt->relation->relname;
            }
            break;
        default:
            break;
    }
    pgaudit_audit_object(objectname, stmt->renameType, querystring);
}

/*
 * Brief		    : pgaudit_process_alter_object(Node* node,const char* querystring)
 * Description	: process alter object operation
 */
static void pgaudit_process_alter_object(Node* node, const char* querystring)
{
    AlterObjectSchemaStmt* alterstmt = (AlterObjectSchemaStmt*)(node);
    char* objectname = NULL;

    switch (alterstmt->objectType) {
        case OBJECT_TABLE:
        case OBJECT_FOREIGN_TABLE:
        case OBJECT_STREAM:
        case OBJECT_CONTQUERY:
        case OBJECT_VIEW:
            objectname = alterstmt->relation->relname;
            break;
        case OBJECT_FUNCTION:
        case OBJECT_TYPE:
        case OBJECT_TSDICTIONARY:
        case OBJECT_TSCONFIGURATION:
            objectname = NameListToString(alterstmt->object);
            break;
        default:
            break;
    }
    pgaudit_audit_object(objectname, alterstmt->objectType, querystring);
}

static char* pgaudit_get_function_name(List* funcnamelist)
{
    char* objname = NULL;
    switch (list_length(funcnamelist)) {
        case 1:
            objname = strVal(linitial(funcnamelist));
            break;
        case 2:
            objname = strVal(lsecond(funcnamelist));
            break;
        case 3:
            objname = strVal(lthird(funcnamelist));
            break;
        default:
            break;
    }
    return objname;
}
/*
* Brief		    : pgaudit_ProcessUtility(Node *parsetree,const char *queryString, ParamListInfo params,
                                   bool isTopLevel,DestReceiver *dest, char *completionTag)
* Description	: ProcessUtility hook
*/
static void pgaudit_ProcessUtility(processutility_context* processutility_cxt,
    DestReceiver* dest,
#ifdef PGXC
    bool sentToRemote,
#endif /* PGXC */
    char* completionTag,
    ProcessUtilityContext context,
    bool isCTAS)
{
    char* object_name_pointer = NULL;

    if (prev_ProcessUtility)
        prev_ProcessUtility(processutility_cxt,
            dest,
#ifdef PGXC
            sentToRemote,
#endif /* PGXC */
            completionTag,
            context,
            isCTAS);
    else
        standard_ProcessUtility(processutility_cxt,
            dest,
#ifdef PGXC
            sentToRemote,
#endif /* PGXC */
            completionTag,
            context,
            isCTAS);

    Node* parsetree = processutility_cxt->parse_tree;
    const char* queryString = processutility_cxt->query_string;
    switch (nodeTag(parsetree)) {
        case T_CreateStmt: {
            CreateStmt* createtablestmt = (CreateStmt*)(parsetree); /* Audit create table */
            pgaudit_ddl_table(createtablestmt->relation->relname, queryString);
        } break;
        case T_LockStmt: {
            LockStmt* locktablestmt = (LockStmt*)(parsetree);
            ListCell* arg = NULL;
            foreach (arg, locktablestmt->relations) {
                RangeVar* rv = NULL;
                List* rv_lockmode = NULL;
                if (u_sess->attr.attr_sql.dolphin && locktablestmt->mode == NoLock){
                    rv_lockmode = (List*)lfirst(arg);
                    rv = (RangeVar*)linitial(rv_lockmode);
                } else {
                    rv = (RangeVar*)lfirst(arg);
                }
                pgaudit_ddl_table(rv->relname, queryString);
            }
        } break;
        case T_AlterTableStmt: {
            AlterTableStmt* altertablestmt = (AlterTableStmt*)(parsetree); /* Audit alter table */
            if (OBJECT_SEQUENCE == altertablestmt->relkind || OBJECT_LARGE_SEQUENCE == altertablestmt->relkind) {
                pgaudit_ddl_sequence(altertablestmt->relation->relname, queryString);
            } else {
                pgaudit_ddl_table(altertablestmt->relation->relname, queryString);
            }
        } break;
        case T_CreateTableAsStmt: {
            CreateTableAsStmt* createtablestmt = (CreateTableAsStmt*)(parsetree);
            IntoClause* intoclause = createtablestmt->into;
            pgaudit_ddl_table(intoclause->rel->relname, queryString);
        } break;
        case T_TruncateStmt: {
            TruncateStmt* truncatestmt = (TruncateStmt*)(parsetree);
            ListCell* arg = NULL;
            foreach (arg, truncatestmt->relations) {
                RangeVar* rv = (RangeVar*)lfirst(arg);
                pgaudit_ddl_table(rv->relname, queryString);
            }
        } break;
        case T_CreateForeignTableStmt: { /* Audit create foregin table */
            CreateStmt* createforeignstmt = (CreateStmt*)(parsetree);
            pgaudit_ddl_table(createforeignstmt->relation->relname, queryString);
        } break;
        case T_CreateUserMappingStmt: {
            CreateUserMappingStmt *createUserMappingStmt = (CreateUserMappingStmt*)parsetree;
            pgaudit_ddl_user(createUserMappingStmt->username, queryString);
        } break;
        case T_AlterUserMappingStmt: {
            AlterUserMappingStmt *alterUserMappingStmt = (AlterUserMappingStmt*)parsetree;
            pgaudit_ddl_user(alterUserMappingStmt->username, queryString);
        } break;
        case T_DropUserMappingStmt: {
            DropUserMappingStmt *dropUserMappingStmt = (DropUserMappingStmt*)parsetree;
            pgaudit_ddl_user(dropUserMappingStmt->username, queryString);
        } break;
        case T_CreateRoleStmt: { /* Audit create user */
            CreateRoleStmt* createrolestmt = (CreateRoleStmt*)(parsetree);
            pgaudit_ddl_user(createrolestmt->role, queryString);
        } break;
        case T_AlterRoleStmt: { /* Audit alter user */
            AlterRoleStmt* alterrolestmt = (AlterRoleStmt*)(parsetree);
            pgaudit_ddl_user(alterrolestmt->role, queryString);
        } break;
        case T_AlterRoleSetStmt: { /* Audit alter user */
            AlterRoleSetStmt* alterrolesetstmt = (AlterRoleSetStmt*)(parsetree);
            pgaudit_ddl_user(alterrolesetstmt->role, queryString);
        } break;
        case T_DropRoleStmt: { /* Audit delete user */
            DropRoleStmt* droprolestmt = (DropRoleStmt*)(parsetree);
            ListCell* arg = NULL;
            foreach (arg, droprolestmt->roles) {
                object_name_pointer = strVal(lfirst(arg));
                pgaudit_ddl_user(object_name_pointer, queryString);
            }
        } break;
        case T_DropOwnedStmt: {
            DropOwnedStmt* dropownedstmt = (DropOwnedStmt*)(parsetree);
            ListCell* arg = NULL;
            foreach (arg, dropownedstmt->roles) {
                object_name_pointer = strVal(lfirst(arg));
                pgaudit_ddl_user(object_name_pointer, queryString);
            }
        } break;
        case T_ReassignOwnedStmt: {
            ReassignOwnedStmt* reassignownedstmt = (ReassignOwnedStmt*)(parsetree);
            ListCell* arg = NULL;
            foreach (arg, reassignownedstmt->roles) {
                object_name_pointer = strVal(lfirst(arg));
                pgaudit_ddl_user(object_name_pointer, queryString);
            }
        } break;
        case T_IndexStmt: { /* Audit create index */
            IndexStmt* indexstmt = (IndexStmt*)parsetree;
            pgaudit_ddl_index(indexstmt->idxname, queryString);
        } break;
        case T_CreateSchemaStmt: { /* Audit create shema */
            CreateSchemaStmt* schemastmt = (CreateSchemaStmt*)(parsetree);
            pgaudit_ddl_schema(schemastmt->schemaname, queryString);
        } break;
        case T_CreateTableSpaceStmt: { /* Audit create tablespace */
            CreateTableSpaceStmt* createtabelspacestmt = (CreateTableSpaceStmt*)(parsetree); 
            pgaudit_ddl_tablespace(createtabelspacestmt->tablespacename, queryString);
        } break;
        case T_AlterTableSpaceOptionsStmt: { /* Audit alter tablespace */
            AlterTableSpaceOptionsStmt* altertabelspaceoptionsstmt =
                (AlterTableSpaceOptionsStmt*)(parsetree); 
            pgaudit_ddl_tablespace(altertabelspaceoptionsstmt->tablespacename, queryString);
        } break;
        case T_DropTableSpaceStmt: { /* Audit drop tablespace */
            DropTableSpaceStmt* droptabelspacestmt = (DropTableSpaceStmt*)(parsetree);
            pgaudit_ddl_tablespace(droptabelspacestmt->tablespacename, queryString);
        } break;
        case T_ViewStmt: { /* Audit create View */
            ViewStmt* viewstmt = (ViewStmt*)(parsetree);
            pgaudit_ddl_view(viewstmt->view->relname, queryString);
        } break;
        case T_CreateEventStmt: {
            CreateEventStmt* eventstmt = (CreateEventStmt*)(parsetree);
            pgaudit_ddl_event(eventstmt->event_name->relname, queryString);
        } break;
        case T_AlterEventStmt: {
            AlterEventStmt* eventstmt = (AlterEventStmt*)(parsetree);
            pgaudit_ddl_event(eventstmt->event_name->relname, queryString);
        } break;
        case T_DropEventStmt: {
            DropEventStmt* eventstmt = (DropEventStmt*)(parsetree);
            pgaudit_ddl_event(eventstmt->event_name->relname, queryString);
        } break;
        case T_CreateEnumStmt: {
            CreateEnumStmt* enumstmt = (CreateEnumStmt*)(parsetree);

            object_name_pointer = NameListToString(enumstmt->typname);
            pgaudit_ddl_type(object_name_pointer, queryString);
        } break;
        case T_AlterEnumStmt: {
            AlterEnumStmt* alterenumstmt = (AlterEnumStmt*)(parsetree);

            object_name_pointer = NameListToString(alterenumstmt->typname);
            pgaudit_ddl_type(object_name_pointer, queryString);
        } break;
        case T_CompositeTypeStmt: {
            CompositeTypeStmt* stmt = (CompositeTypeStmt*)(parsetree);

            pgaudit_ddl_type(stmt->typevar->relname, queryString);
        } break;
        case T_CreateSetStmt: {
            CreateSetStmt* setstmt = (CreateSetStmt*)parsetree;
            object_name_pointer = NameListToString(setstmt->typname->names);
            pgaudit_ddl_type(object_name_pointer, queryString);
        } break;
        case T_CreateFunctionStmt: { /* Audit  procedure */
            CreateFunctionStmt* createfunctionstmt = (CreateFunctionStmt*)(parsetree);

            object_name_pointer = pgaudit_get_function_name(createfunctionstmt->funcname);
            pgaudit_ddl_function(object_name_pointer, queryString);
        } break;
        case T_CreatePackageStmt:
        case T_CreatePackageBodyStmt: {
            pgaudit_ddl_package(object_name_pointer, queryString);
        } break;
        case T_AlterFunctionStmt: { /* Audit  procedure */
            AlterFunctionStmt* alterfunctionstmt = (AlterFunctionStmt*)(parsetree);

            object_name_pointer = NameListToString(alterfunctionstmt->func->funcname);
            pgaudit_ddl_function(object_name_pointer, queryString);
        } break;
        case T_CreatedbStmt: { /* Audit create database */
            CreatedbStmt* createdbstmt = (CreatedbStmt*)(parsetree); 
            pgaudit_ddl_database(createdbstmt->dbname, queryString);
        } break;
        case T_AlterDatabaseStmt: { /* Audit create database */
            AlterDatabaseStmt* alterdatabasestmt = (AlterDatabaseStmt*)(parsetree);
            pgaudit_ddl_database(alterdatabasestmt->dbname, queryString);
        } break;
        case T_AlterDatabaseSetStmt: { /* Audit create database */
            AlterDatabaseSetStmt* alterdatabasesetstmt = (AlterDatabaseSetStmt*)(parsetree);
            pgaudit_ddl_database(alterdatabasesetstmt->dbname, queryString);
        } break;
        case T_DropdbStmt: { /* Audit drop database */
            DropdbStmt* dropdbstmt = (DropdbStmt*)(parsetree);
            pgaudit_ddl_database(dropdbstmt->dbname, queryString);
        } break;
        case T_CreateTrigStmt: { /* Audti create trigger */
            CreateTrigStmt* createstmt = (CreateTrigStmt*)(parsetree);
            pgaudit_ddl_trigger(createstmt->trigname, queryString);
        } break;
        case T_AlterDefaultPrivilegesStmt: { /* ALTER DEFAULT PRIVILEGES statement */
            AlterDefaultPrivilegesStmt* alterprivilegesstmt = (AlterDefaultPrivilegesStmt*)(parsetree);
            pgaudit_grant_or_revoke_role(alterprivilegesstmt->action->is_grant, NULL, queryString);
        } break;
        case T_GrantStmt: { /* Grant or revoke a role */
            GrantStmt* grantstmt = (GrantStmt*)(parsetree);
            pgaudit_process_grant_or_revoke_roles(grantstmt->grantees, grantstmt->is_grant, queryString);
        } break;
        case T_GrantRoleStmt: { /* Audit grant or revoke role */
            GrantRoleStmt* grantrolestmt = (GrantRoleStmt*)(parsetree);
            pgaudit_process_grant_or_revoke_roles(grantrolestmt->grantee_roles, grantrolestmt->is_grant, queryString);
        } break;
        case T_GrantDbStmt: { /* Audit grant or revoke any privilege */
            GrantDbStmt* grantdbstmt = (GrantDbStmt*)(parsetree);
            pgaudit_process_grant_or_revoke_roles(grantdbstmt->grantees, grantdbstmt->is_grant, queryString);
        } break;
        case T_DropStmt: /* Audit drop objct */
            pgaudit_process_drop_objects(parsetree, queryString);
            break;
        case T_RenameStmt: /* Audit rename objct */
            pgaudit_process_rename_object(parsetree, queryString);
            break;
        case T_AlterObjectSchemaStmt: /* Audit alter objct */
            pgaudit_process_alter_object(parsetree, queryString);
            break;
        case T_AlterOwnerStmt: /* Audit alterowner objct */
            pgaudit_process_alter_owner(parsetree, queryString);
            break;
        case T_ReindexStmt:
            pgaudit_process_reindex(parsetree, queryString);
            break;
        case T_CreateResourcePoolStmt: {
            CreateResourcePoolStmt* createresourcepoolStmt = (CreateResourcePoolStmt*)(parsetree);
            pgaudit_ddl_resourcepool(createresourcepoolStmt->pool_name, queryString);
        } break;
        case T_AlterResourcePoolStmt: {
            AlterResourcePoolStmt* alterresourcepoolStmt = (AlterResourcePoolStmt*)(parsetree);
            pgaudit_ddl_resourcepool(alterresourcepoolStmt->pool_name, queryString);
        } break;
        case T_DropResourcePoolStmt: {
            DropResourcePoolStmt* dropresourcepoolStmt = (DropResourcePoolStmt*)(parsetree);
            pgaudit_ddl_resourcepool(dropresourcepoolStmt->pool_name, queryString);
        } break;
        case T_AlterGlobalConfigStmt: {
            AlterGlobalConfigStmt* alterglobalconfigStmt = (AlterGlobalConfigStmt*)(parsetree);
            pgaudit_alter_globalconfig(alterglobalconfigStmt, queryString);
        } break;
        case T_DropGlobalConfigStmt: {
            DropGlobalConfigStmt* dropglobalconfigStmt = (DropGlobalConfigStmt*)(parsetree);
            pgaudit_drop_globalconfig(dropglobalconfigStmt, queryString);
        } break;
        case T_CreateWorkloadGroupStmt: {
            CreateWorkloadGroupStmt* createworkloadgroupstmt = (CreateWorkloadGroupStmt*)(parsetree);
            pgaudit_ddl_workload(createworkloadgroupstmt->group_name, queryString);
        } break;
        case T_AlterWorkloadGroupStmt: {
            AlterWorkloadGroupStmt* alterworkloadgroupstmt = (AlterWorkloadGroupStmt*)(parsetree);
            pgaudit_ddl_workload(alterworkloadgroupstmt->group_name, queryString);
        } break;
        case T_DropWorkloadGroupStmt: {
            DropWorkloadGroupStmt* dropworkloadgroupstmt = (DropWorkloadGroupStmt*)(parsetree);
            pgaudit_ddl_workload(dropworkloadgroupstmt->group_name, queryString);
        } break;
        case T_CreateAppWorkloadGroupMappingStmt: {
            CreateAppWorkloadGroupMappingStmt* createappworkloadgroupmappingstmt =
                (CreateAppWorkloadGroupMappingStmt*)(parsetree);
            pgaudit_ddl_workload(createappworkloadgroupmappingstmt->app_name, queryString);
        } break;
        case T_AlterAppWorkloadGroupMappingStmt: {
            AlterAppWorkloadGroupMappingStmt* alterappworkloadgroupmappingstmt =
                (AlterAppWorkloadGroupMappingStmt*)(parsetree);
            pgaudit_ddl_workload(alterappworkloadgroupmappingstmt->app_name, queryString);
        } break;
        case T_DropAppWorkloadGroupMappingStmt: {
            DropAppWorkloadGroupMappingStmt* dropappworkloadgroupmappingstmt =
                (DropAppWorkloadGroupMappingStmt*)(parsetree);
            pgaudit_ddl_workload(dropappworkloadgroupmappingstmt->app_name, queryString);
        } break;
        case T_CreateForeignServerStmt: {
            CreateForeignServerStmt* createforeignserverstmt = (CreateForeignServerStmt*)(parsetree);
            pgaudit_ddl_serverforhardoop(createforeignserverstmt->servername, queryString);
        } break;
        case T_AlterForeignServerStmt: {
            AlterForeignServerStmt* alterforeignserverstmt = (AlterForeignServerStmt*)(parsetree);
            pgaudit_ddl_serverforhardoop(alterforeignserverstmt->servername, queryString);
        } break;
        case T_VariableSetStmt: {
            VariableSetStmt* variablesetstmt = (VariableSetStmt*)(parsetree);
            pgaudit_process_set_parameter(variablesetstmt->name, queryString);
        } break;
        case T_VariableMultiSetStmt: {
            pgaudit_process_set_parameter("set stmt", queryString);
        } break;
#ifndef ENABLE_MULTIPLE_NODES
        case T_AlterSystemStmt: {
            AlterSystemStmt* altersystemstmt = (AlterSystemStmt*)(parsetree);
            pgaudit_process_set_parameter(altersystemstmt->setstmt->name, queryString);
        } break;
#endif
        case T_CreateDataSourceStmt: {
            CreateDataSourceStmt* createdatasourcestmt = (CreateDataSourceStmt*)(parsetree);
            pgaudit_ddl_datasource(createdatasourcestmt->srcname, queryString);
        } break;
        case T_AlterDataSourceStmt: {
            AlterDataSourceStmt* alterdatasourcestmt = (AlterDataSourceStmt*)(parsetree);
            pgaudit_ddl_datasource(alterdatasourcestmt->srcname, queryString);
        } break;
        case T_CreateGroupStmt: {
            CreateGroupStmt* createnodegroupstmt = (CreateGroupStmt*)(parsetree);
            pgaudit_ddl_nodegroup(createnodegroupstmt->group_name, queryString);
        } break;
        case T_AlterGroupStmt: {
            AlterGroupStmt* alternodegroupstmt = (AlterGroupStmt*)(parsetree);
            pgaudit_ddl_nodegroup(alternodegroupstmt->group_name, queryString);
        } break;
        case T_DropGroupStmt: {
            DropGroupStmt* dropnodegroupstmt = (DropGroupStmt*)(parsetree);
            pgaudit_ddl_nodegroup(dropnodegroupstmt->group_name, queryString);
        } break;
        case T_CreateDirectoryStmt: {
            CreateDirectoryStmt* createdirectorystmt = (CreateDirectoryStmt*)(parsetree);
            pgaudit_ddl_directory(createdirectorystmt->directoryname, queryString);
            break;
        }
        case T_DropDirectoryStmt: {
            DropDirectoryStmt* dropdirectorystmt = (DropDirectoryStmt*)(parsetree);
            pgaudit_ddl_directory(dropdirectorystmt->directoryname, queryString);
            break;
        }
        case T_CreateRlsPolicyStmt: {
            CreateRlsPolicyStmt* createRlsPolicyStmt = (CreateRlsPolicyStmt*)(parsetree);
            pgaudit_ddl_rowlevelsecurity(createRlsPolicyStmt->policyName, queryString);
        } break;
        case T_AlterRlsPolicyStmt: {
            AlterRlsPolicyStmt* alterRlsPolicyStmt = (AlterRlsPolicyStmt*)(parsetree);
            pgaudit_ddl_rowlevelsecurity(alterRlsPolicyStmt->policyName, queryString);
        } break;
        case T_CreateSynonymStmt: {
            CreateSynonymStmt* createSynonymStmt = (CreateSynonymStmt*)(parsetree);
            char* synName = NameListToString(createSynonymStmt->synName);
            pgaudit_ddl_synonym(synName, queryString);
        } break;
        case T_DropSynonymStmt: {
            DropSynonymStmt* dropSynonymStmt = (DropSynonymStmt*)(parsetree);
            char* synName = NameListToString(dropSynonymStmt->synName);
            pgaudit_ddl_synonym(synName, queryString);
        } break;

        case T_DefineStmt: {
            DefineStmt* stmt = (DefineStmt*)parsetree;

            object_name_pointer = NameListToString(stmt->defnames);
            switch (stmt->kind) {
                case OBJECT_TSDICTIONARY:
                case OBJECT_TSCONFIGURATION:
                    pgaudit_ddl_textsearch(object_name_pointer, queryString);
                    break;
                case OBJECT_TYPE:
                    pgaudit_ddl_type(object_name_pointer, queryString);
                    break;
                default:
                    break;
            }
        } break;
        case T_AlterTSDictionaryStmt: {
            AlterTSDictionaryStmt* stmt = (AlterTSDictionaryStmt*)parsetree;

            object_name_pointer = NameListToString(stmt->dictname);
            pgaudit_ddl_textsearch(object_name_pointer, queryString);
        } break;
        case T_AlterTSConfigurationStmt: {
            AlterTSConfigurationStmt* stmt = (AlterTSConfigurationStmt*)parsetree;

            object_name_pointer = NameListToString(stmt->cfgname);
            pgaudit_ddl_textsearch(object_name_pointer, queryString);
        } break;
        case T_CreateSeqStmt: {
            CreateSeqStmt* stmt = (CreateSeqStmt*)parsetree;
            pgaudit_ddl_sequence(stmt->sequence->relname, queryString);
        } break;
        case T_AlterSeqStmt: {
            AlterSeqStmt* stmt = (AlterSeqStmt*)parsetree;
            pgaudit_ddl_sequence(stmt->sequence->relname, queryString);
        } break;
        case T_CreateWeakPasswordDictionaryStmt: {
            pgaudit_ddl_weak_password(queryString);
        } break;               
        case T_DropWeakPasswordDictionaryStmt: {
            pgaudit_ddl_weak_password(queryString);
        } break;       
        case T_CreateClientLogicGlobal:
        case T_CreateClientLogicColumn: {
            pgaudit_ddl_full_encryption_key(queryString);
        } break;
        case T_PurgeStmt: {
            PurgeStmt *stmt = (PurgeStmt *)parsetree;
            if (stmt->purtype == PURGE_TABLE) {
                pgaudit_ddl_table(stmt->purobj->relname, queryString);
            } else if (stmt->purtype == PURGE_INDEX) {
                pgaudit_ddl_index(stmt->purobj->relname, queryString);
            } else { /* PURGE RECYCLEBIN */
                char *dbname = get_database_name(u_sess->proc_cxt.MyDatabaseId);
                pgaudit_ddl_database(dbname, queryString);
            }
        } break;
        case T_TimeCapsuleStmt: {
            TimeCapsuleStmt *stmt = (TimeCapsuleStmt *)parsetree;
            pgaudit_ddl_table(stmt->relation->relname, queryString);
        } break;
        case T_CreateModelStmt: {
            CreateModelStmt* createModelStmt = (CreateModelStmt*)(parsetree);
            pgaudit_ddl_model(createModelStmt->model, queryString);
        } break;
        case T_CreatePublicationStmt: {
            CreatePublicationStmt *stmt = (CreatePublicationStmt*)parsetree;
            pgaudit_ddl_publication_subscription(stmt->pubname, queryString);
        } break;
        case T_AlterPublicationStmt: {
            AlterPublicationStmt *stmt = (AlterPublicationStmt*)parsetree;
            pgaudit_ddl_publication_subscription(stmt->pubname, queryString);
        } break;
        case T_CreateSubscriptionStmt: {
            CreateSubscriptionStmt *stmt = (CreateSubscriptionStmt*)parsetree;
            pgaudit_ddl_publication_subscription(stmt->subname, queryString);
        } break;
        case T_AlterSubscriptionStmt: {
            AlterSubscriptionStmt *stmt = (AlterSubscriptionStmt*)parsetree;
            pgaudit_ddl_publication_subscription(stmt->subname, queryString);
        } break;
        case T_DropSubscriptionStmt: {
            DropSubscriptionStmt *stmt = (DropSubscriptionStmt*)parsetree;
            pgaudit_ddl_publication_subscription(stmt->subname, queryString);
        } break;
        case T_CreateFdwStmt: {
            CreateFdwStmt *stmt = (CreateFdwStmt*)parsetree;
            pgaudit_ddl_fdw(stmt->fdwname, queryString);
        } break;
        case T_AlterFdwStmt: {
            AlterFdwStmt *stmt = (AlterFdwStmt*)parsetree;
            pgaudit_ddl_fdw(stmt->fdwname, queryString);
        } break;
        default:
            break;
    }
}

/*
 * Brief		    : pgaudit_process_grant_or_revoke_roles(List* grantee_name_list,bool isgrant,const char*
 * querystring) Description	: process grant or revoke roles operation
 */
static void pgaudit_process_grant_or_revoke_roles(List* grantee_name_list, bool isgrant, const char* querystring)
{
    ListCell* lc = NULL;
    char* object_name = NULL;

    foreach (lc, grantee_name_list) {
        PrivGrantee* rte = (PrivGrantee*)lfirst(lc);
        object_name = rte->rolname;
        pgaudit_grant_or_revoke_role(isgrant, object_name, querystring);
    }
}

char* pgaudit_get_relation_name(List* relation_name_list)
{
    ListCell* lc = NULL;
    char* object_name = NULL;

    foreach (lc, relation_name_list) {
        RangeTblEntry* rte = (RangeTblEntry*)lfirst(lc);
        if (rte->relname != NULL) {
            object_name = rte->relname;
            break;
        } else if (rte->eref != NULL && rte->eref->aliasname != NULL) {
            object_name = rte->eref->aliasname;
            break;
        }
    }
    return object_name;
}

/*
 * Brief		    : pgaudit_ExecutorEnd()
 * Description	: ExecutorEnd hook: store results if needed
 */
static void pgaudit_ExecutorEnd(QueryDesc* queryDesc)
{
    char* object_name = NULL;

    /* Add the pg_delete_audit operation to audit log */
    if (t_thrd.audit.Audit_delete) {
        pgaudit_delete_files(NULL, queryDesc->sourceText);
        t_thrd.audit.Audit_delete = false;
    }

    switch (queryDesc->operation) {
        case CMD_INSERT:
        case CMD_DELETE:
        case CMD_UPDATE:
        case CMD_MERGE:
            object_name = pgaudit_get_relation_name(queryDesc->estate->es_range_table);
            pgaudit_dml_table(object_name, queryDesc->sourceText);
            break;
        case CMD_SELECT:
            object_name = pgaudit_get_relation_name(queryDesc->estate->es_range_table);
            pgaudit_dml_table_select(object_name, queryDesc->sourceText);
            break;

        default:
            break;
    }

    if (prev_ExecutorEnd)
        (prev_ExecutorEnd)(queryDesc);
    else
        standard_ExecutorEnd(queryDesc);
}

void light_pgaudit_ExecutorEnd(Query* query)
{
    char* object_name = NULL;
    bool is_full_audit_user = audit_check_full_audit_user();

    switch (query->commandType) {
        case CMD_INSERT:
        case CMD_DELETE:
        case CMD_UPDATE:
            if (u_sess->attr.attr_security.Audit_DML != 0 || is_full_audit_user) {
                object_name = pgaudit_get_relation_name(query->rtable);
                pgaudit_dml_table(object_name, query->sql_statement);
            }
            break;
        case CMD_SELECT:
            if (u_sess->attr.attr_security.Audit_DML_SELECT != 0 || is_full_audit_user) {
                object_name = pgaudit_get_relation_name(query->rtable);
                pgaudit_dml_table_select(object_name, query->sql_statement);
            }
            break;
        /* Not support others */
        default:
            break;
    }
}

/*
 * Brief        : check if src_str includes target_str
 * Description  : split src_str by delimiter and search for target_str
 */
bool audit_search_str(char* src_str, char* target_str)
{
    bool is_found = false;
    char* ptok = NULL;
    char* sub_str = NULL;
    char* src_str_tmp = NULL;
    char* src_str_trim = NULL;
    src_str_tmp = pstrdup(src_str);
    if (src_str_tmp == NULL) {
        pfree(src_str_tmp);
        return is_found;
    }
    sub_str = (char*)strchr(src_str, ',');
    if (sub_str == NULL) {
        src_str_trim = trim(src_str_tmp);
        if (strcmp(target_str, src_str_trim) == 0) {
            pfree(src_str_tmp);
            return true;
        }
    } else {
        sub_str = strtok_s(src_str_tmp, ",", &ptok);
        while (sub_str != NULL) {
            src_str_trim = trim(sub_str);
            if (strcmp(target_str, src_str_trim) == 0) {
                is_found = true;
                break;
            }
            sub_str = strtok_s(NULL, ",", &ptok);
        }
    }
    pfree(src_str_tmp);
    return is_found;
}
 
 
/*
 * Brief        : check if the client_info is in the blackist, consists of clients
 *                whoes audit records should not be sent to audit pipe
 * Description  : search No_Audit_Client for current clientconn_info, if exists, skip audit_report
 */
bool audit_check_client_blacklist(char* client_info)
{
    bool is_blacklist = false;
    /* check if apply audit blacklist */
    if (u_sess->attr.attr_security.no_audit_client == NULL || strlen(u_sess->attr.attr_security.no_audit_client) == 0) {
        return is_blacklist;
    }
    is_blacklist = audit_search_str(u_sess->attr.attr_security.no_audit_client, client_info);
    return is_blacklist;
}
 
/*
 * Brief        : check if current user is under comprehensive audit
 * Description  : search Full_Audit_Users for current username, if exists, open all the audit options
 */
bool audit_check_full_audit_user()
{
    bool is_full_audit = false;
    char* username = NULL;
    if (u_sess->proc_cxt.MyProcPort == NULL) {
        return is_full_audit;
    }
    if (u_sess->misc_cxt.CurrentUserName != NULL) {
        username = (char*)u_sess->misc_cxt.CurrentUserName;
    } else {
        username = u_sess->proc_cxt.MyProcPort->user_name;
    }
    if (strlen(u_sess->attr.attr_security.full_audit_users) == 0 || username == NULL) {
        return is_full_audit;
    }
    is_full_audit = audit_search_str(u_sess->attr.attr_security.full_audit_users, username);
    return is_full_audit;
}
 
 
/*
 * Brief        : check if the system function should be audited
 * Description  : search list g_audit_system_funcs for func_name, if exists, do audit
 */
static bool audit_is_system_func(char *func_name)
{
    bool is_audit = false;
    int i;
    for (i = 0; g_audit_system_funcs[i] != NULL; i++) {
        if (strcmp((const char*)func_name, g_audit_system_funcs[i]) == 0) {
            is_audit = true;
        }
    }
    if (!is_audit) {
        List* search_path = fetch_search_path(false);
        char* nspname = NULL;
 
        if (search_path == NIL) {
            list_free_ext(search_path);
            return is_audit;
        }
        nspname = get_namespace_name(linitial_oid(search_path));
        func_name = quote_qualified_identifier(nspname, func_name);
        list_free_ext(search_path);
        for (i = 0; g_audit_system_funcs[i] != NULL; i++) {
            if (strcmp((const char*)func_name, g_audit_system_funcs[i]) == 0) {
                is_audit = true;
            }
        }
    }
    return is_audit;
}

/*
 * Brief        : check if the system function includes crypt information
 * Description  : search list g_audit_crypt_funcs for func_name
 */
static bool audit_is_crypt_func(char *func_name)
{
    bool is_crypt = false;
    for (int i = 0; g_audit_crypt_funcs[i] != NULL; i++) {
        if (strcmp((const char*)func_name, g_audit_crypt_funcs[i]) == 0) {
            is_crypt = true;
        }
    }
    return is_crypt;
}

/*
 * Brief        : generate audit record of system function in white list, and do audit_report
 * Description  : extract funtion name, oid and parameters of system function
 */
void audit_system_function(FunctionCallInfo fcinfo, const AuditResult result)
{
    Oid fn_oid = fcinfo->flinfo->fn_oid;
    char* fn_signature = format_procedure(fn_oid);
    /* extract function name */
    char* func_name = NULL;
    char* next_token = NULL;
    const char* token = "(";
    func_name = strtok_s((char*)format_procedure(fn_oid), token, &next_token);
    if (func_name == NULL) {
        return;
    }
    /* check if current function is system function and included the white list */
    if (fn_oid >= FirstBootstrapObjectId || !audit_is_system_func(func_name)) {
        return;
    }
    /* if function includes crypt infomation, skip extract arguments */
    bool is_crypt = audit_is_crypt_func(func_name);
    char details[PGAUDIT_MAXLENGTH];
    errno_t rcs = EOK;
    if (is_crypt) {
        rcs = snprintf_s(details, PGAUDIT_MAXLENGTH, PGAUDIT_MAXLENGTH - 1,
                         "Execute system function(oid = %u). args = %s",
                         fn_oid, CRYPT_FUNC_ARG);
    } else {
        char* fn_args = audit_get_func_args(fcinfo);
        rcs = snprintf_s(details, PGAUDIT_MAXLENGTH, PGAUDIT_MAXLENGTH - 1,
                         "Execute system function(oid = %u). args = %s",
                         fn_oid, fn_args);
        pfree(fn_args);
    }
    securec_check_ss(rcs, "\0", "\0");
    audit_report(AUDIT_SYSTEM_FUNCTION_EXEC, result, fn_signature, details);
}

/*
 * Brief        : generate the detail_info by extract arguments of system function
 * Description  : traverse fcinfo to get arguments value and form string
 */
static char* audit_get_func_args(FunctionCallInfo fcinfo)
{
    char* arg;
    StringInfoData buf;
    initStringInfo(&buf);
    appendStringInfo(&buf, "%s(", "");

    for (int i = 0; i < fcinfo->nargs; i++) {
        if (i > 0) {
            appendStringInfoString(&buf, ",");
        }
        arg = audit_get_value_bytype(fcinfo, i);
        appendStringInfo(&buf, "%s", arg);
    }
    appendStringInfoString(&buf, ")");
    char* result = buf.data;
    return result;
}
 
/*
 * Brief        : extract args from the fcinfo
 * Description  : get value from fcinfo by different arguments type and sequence number
 */
char* audit_get_value_bytype(FunctionCallInfo fcinfo, int n_arg)
{
    char* value = (char*)palloc(BUF_LENGTH);
    errno_t nRet = EOK;
    /* paramater can be NULL if it has default value */
    if (PG_ARGISNULL(n_arg)) {
        nRet = strncpy_s(value, BUF_LENGTH, SYSTEM_FUNC_DEFAULT_VALUE, BUF_LENGTH - 1);
        securec_check(nRet, "\0", "\0");
        return value;
    }
    Oid typeOid = fcinfo->argTypes[n_arg];
    switch (typeOid) {
        case TEXTARRAYOID: {
            int option_nitems;
            ArrayType* option_array = PG_GETARG_ARRAYTYPE_P(n_arg);
            pfree(value);
            value = audit_get_text_array_value(option_array, &option_nitems);
            break;
        }
        case BOOLOID: {
            bool arg = PG_GETARG_BOOL(n_arg);
            nRet = strncpy_s(value, BUF_LENGTH, (arg) ? "true" : "false", BUF_LENGTH - 1);
            securec_check(nRet, "\0", "\0");
            break;
        }
        case BYTEAOID: {
            bytea* v = PG_GETARG_BYTEA_P(n_arg);
            pfree(value);
            value = VARDATA_ANY(v);
            break;
        }
        case CHAROID: {
            char v = PG_GETARG_CHAR(n_arg);
            nRet = snprintf_s(value, BUF_LENGTH, BUF_LENGTH - 1, "%c", v);
            securec_check_ss(nRet, "\0", "\0");
            break;
        }
        case NAMEOID: {
            pfree(value);
            value = NameStr(*PG_GETARG_NAME(n_arg));
            break;
        }
        case INT8OID: {
            int64 v = PG_GETARG_INT64(n_arg);
            nRet = snprintf_s(value, BUF_LENGTH, BUF_LENGTH - 1, "%lld", v);
            securec_check_ss(nRet, "\0", "\0");
            break;
        }
        case INT2OID: {
            int2 v = PG_GETARG_INT16(n_arg);
            nRet = snprintf_s(value, BUF_LENGTH, BUF_LENGTH - 1, "%d", v);
            securec_check_ss(nRet, "\0", "\0");
            break;
        }
        case INT4OID: {
            int4 v = PG_GETARG_INT32(n_arg);
            nRet = snprintf_s(value, BUF_LENGTH, BUF_LENGTH - 1, "%ld", v);
            securec_check_ss(nRet, "\0", "\0");
            break;
        }
        case REGPROCOID: {
            RegProcedure v = PG_GETARG_OID(n_arg);
            nRet = snprintf_s(value, BUF_LENGTH, BUF_LENGTH - 1, "%u", v);
            securec_check_ss(nRet, "\0", "\0");
            break;
        }
        case TEXTOID: {
            pfree(value);
            value = text_to_cstring(PG_GETARG_TEXT_P(n_arg));
            break;
        }
        case OIDOID: {
            Oid v = PG_GETARG_OID(n_arg);
            if (v) {
                nRet = snprintf_s(value, BUF_LENGTH, BUF_LENGTH - 1, "%u", v);
                securec_check_ss(nRet, "\0", "\0");
            }
            break;
        }
        case TIDOID: {
            ThreadId v = (unsigned long)PG_GETARG_INT64(n_arg);
            nRet = snprintf_s(value, BUF_LENGTH, BUF_LENGTH - 1, "%lld", v);
            securec_check_ss(nRet, "\0", "\0");
            break;
        }
        case XIDOID: {
            TransactionId v = PG_GETARG_TRANSACTIONID(n_arg);
            nRet = snprintf_s(value, BUF_LENGTH, BUF_LENGTH - 1, "%llu", v);
            securec_check_ss(nRet, "\0", "\0");
            break;
        }
        case CIDOID: {
            CommandId v = PG_GETARG_COMMANDID(n_arg);
            nRet = snprintf_s(value, BUF_LENGTH, BUF_LENGTH - 1, "%lu", v);
            securec_check_ss(nRet, "\0", "\0");
            break;
        }
        case SHORTXIDOID: {
            ShortTransactionId v = PG_GETARG_SHORTTRANSACTIONID(n_arg);
            nRet = snprintf_s(value, BUF_LENGTH, BUF_LENGTH - 1, "%lu", v);
            securec_check_ss(nRet, "\0", "\0");
            break;
        }
        case FLOAT4OID: {
            float4 v = PG_GETARG_FLOAT4(n_arg);
            nRet = snprintf_s(value, BUF_LENGTH, BUF_LENGTH - 1, "%.4f", v);
            securec_check_ss(nRet, "\0", "\0");
            break;
        }
        case FLOAT8OID: {
            float8 v = PG_GETARG_FLOAT8(n_arg);
            nRet = snprintf_s(value, BUF_LENGTH, BUF_LENGTH - 1, "%.8f", v);
            securec_check_ss(nRet, "\0", "\0");
            break;
        }
        case ABSTIMEOID: {
            AbsoluteTime v = PG_GETARG_ABSOLUTETIME(n_arg);
            nRet = snprintf_s(value, BUF_LENGTH, BUF_LENGTH - 1, "%ld", v);
            securec_check_ss(nRet, "\0", "\0");
            break;
        }
        case RELTIMEOID: {
            RelativeTime v = PG_GETARG_RELATIVETIME(n_arg);
            nRet = snprintf_s(value, BUF_LENGTH, BUF_LENGTH - 1, "%ld", v);
            securec_check_ss(nRet, "\0", "\0");
            break;
        }
        case INTERVALOID: {
            Interval* v = PG_GETARG_INTERVAL_P(n_arg);
            TmToChar tmtc;
            struct pg_tm* tm = NULL;
            ZERO_tmtc(&tmtc);
            tm = tmtcTm(&tmtc);
            if (interval2tm(*v, tm, &tmtcFsec(&tmtc)) != 0) {
                nRet = strncpy_s(value, BUF_LENGTH, SYSTEM_FUNC_DEFAULT_VALUE, BUF_LENGTH - 1);
            } else {
                nRet = snprintf_s(value, BUF_LENGTH, BUF_LENGTH - 1,
                    "%d year %d mon %d day %02d:%02d:%02d", tm->tm_year,
                    tm->tm_mon, tm->tm_mday, tm->tm_hour, tm->tm_min, tm->tm_sec);
            }
            securec_check_ss(nRet, "\0", "\0");
            break;
        }
        case TINTERVALOID: {
            TimeInterval v = PG_GETARG_TIMEINTERVAL(n_arg);
            nRet = snprintf_s(value, BUF_LENGTH, BUF_LENGTH - 1, "%d", v->data);
            securec_check_ss(nRet, "\0", "\0");
            break;
        }
        case CASHOID: {
            Cash v = PG_GETARG_CASH(n_arg);
            nRet = snprintf_s(value, BUF_LENGTH, BUF_LENGTH - 1, "%lld", v);
            securec_check_ss(nRet, "\0", "\0");
            break;
        }
        case INETOID:case CIDROID: {
            inet* v = PG_GETARG_INET_PP(n_arg);
            pfree(value);
            value = (char*)VARDATA_ANY(v);
            break;
        }
        case BPCHAROID: {
            BpChar* v = PG_GETARG_BPCHAR_PP(n_arg);
            pfree(value);
            value = VARDATA_ANY(v);
            break;
        }
        case DATEOID: {
            DateADT v = PG_GETARG_DATEADT(n_arg);
            nRet = snprintf_s(value, BUF_LENGTH, BUF_LENGTH - 1, "%ld", v);
            securec_check_ss(nRet, "\0", "\0");
            break;
        }
        case TIMEOID: {
            TimeADT v = PG_GETARG_TIMEADT(n_arg);
#ifdef HAVE_INT64_TIMESTAMP
            nRet = snprintf_s(value, BUF_LENGTH, BUF_LENGTH - 1, "%lld", v);
#else
            nRet = snprintf_s(value, BUF_LENGTH, BUF_LENGTH - 1, "%lf", v);
#endif
            securec_check_ss(nRet, "\0", "\0");
            break;
        }
        case TIMESTAMPOID: {
            Timestamp v = PG_GETARG_TIMESTAMP(n_arg);
            nRet = snprintf_s(value, BUF_LENGTH, BUF_LENGTH - 1, "%lld", v);
            securec_check_ss(nRet, "\0", "\0");
            break;
        }
        case TIMESTAMPTZOID: {
            TimestampTz v = PG_GETARG_TIMESTAMPTZ(n_arg);
            nRet = snprintf_s(value, BUF_LENGTH, BUF_LENGTH - 1, "%lld", v);
            securec_check_ss(nRet, "\0", "\0");
            break;
        }
        case CSTRINGOID:
        case NVARCHAR2OID:
        case VARCHAROID:
        case RAWOID:
        case SMALLDATETIMEOID: {
            pfree(value);
            value = PG_GETARG_CSTRING(n_arg);
            break;
        }
        case TIMETZOID: {
            TimeTzADT* v = PG_GETARG_TIMETZADT_P(n_arg);
#ifdef HAVE_INT64_TIMESTAMP
            int64 time = v->time;
            nRet = snprintf_s(value, BUF_LENGTH, BUF_LENGTH - 1, "time %ld zone %lu", time, v->zone);
#else
            float8 time = v->time;
            nRet = snprintf_s(value, BUF_LENGTH, BUF_LENGTH - 1, "time %f zone %lu", time, v->zone);
#endif
            securec_check_ss(nRet, "\0", "\0");
            break;
        }
        case INT1OID: {
            int1 v = PG_GETARG_INT8(n_arg);
            nRet = snprintf_s(value, BUF_LENGTH, BUF_LENGTH - 1, "%s", v);
            securec_check_ss(nRet, "\0", "\0");
            break;
        }
        case UUIDOID: {
            pg_uuid_t* v_uuid = PG_GETARG_UUID_P(n_arg);
            char* v = (char*)v_uuid->data;
            nRet = snprintf_s(value, BUF_LENGTH, BUF_LENGTH - 1, "%s", v);
            securec_check_ss(nRet, "\0", "\0");
            break;
        }
        case ANYENUMOID: {
            uint32 v = (uint32)PG_GETARG_OID(n_arg);
            nRet = snprintf_s(value, BUF_LENGTH, BUF_LENGTH - 1, "%lu", v);
            securec_check_ss(nRet, "\0", "\0");
            break;
        }
        case INT2VECTOROID: {
            int2vector *key = (int2vector *)PG_GETARG_POINTER(n_arg);
            pfree(value);
            value = (char *)key->values;
            break;
        }
        case OIDVECTOROID: {
            oidvector *key = (oidvector *)PG_GETARG_POINTER(n_arg);
            pfree(value);
            value = (char *)key->values;
            break;
        }
        default: {
            ereport(LOG, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("UNKOWN PARAMETER TYPE FOR SYSTEM FUNCTION AUDIT.")));
            nRet = strncpy_s(value, BUF_LENGTH, SYSTEM_FUNC_DEFAULT_VALUE, BUF_LENGTH - 1);
            securec_check(nRet, "\0", "\0");
            break;
        }
    }
    return value;
}
 
/*
 * Brief        : get values of optional arguments in form of text array
 * Description  : deconstruct a text[] into C-strings
 */
static char* audit_get_text_array_value(ArrayType* array, int* numitems)
{
    int ndim = ARR_NDIM(array);
    int* dims = ARR_DIMS(array);
    int nitems;
    int16 typlen;
    bool typbyval = false;
    char typalign;
    char* ptr = NULL;
    bits8* bitmap = NULL;
    uint32 bitmask;
    int i;
    char* val = NULL;
 
    StringInfoData buf;
    initStringInfo(&buf);
    Assert(ARR_ELEMTYPE(array) == TEXTOID);
    *numitems = nitems = ArrayGetNItems(ndim, dims);
    get_typlenbyvalalign(ARR_ELEMTYPE(array), &typlen, &typbyval, &typalign);
 
    ptr = ARR_DATA_PTR(array);
    bitmap = ARR_NULLBITMAP(array);
    bitmask = 1;
    for (i = 0; i < nitems; i++) {
        if (i > 0)
            appendStringInfo(&buf, "%s", ",");
        if (bitmap && (*bitmap & bitmask) == 0) {
            val = NULL;
        } else {
            val = TextDatumGetCString(PointerGetDatum(ptr));
            ptr = att_addlength_pointer(ptr, typlen, ptr);
            ptr = (char*)att_align_nominal(ptr, typalign);
        }
        appendStringInfo(&buf, "%s", val);
        /* advance bitmap pointer if any */
        if (bitmap) {
            bitmask <<= 1;
            if (bitmask == 0x100) {
                bitmap++;
                bitmask = 1;
            }
        }
    }
    char* values = buf.data;
    return values;
}
