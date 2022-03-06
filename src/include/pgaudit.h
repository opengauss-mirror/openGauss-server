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
 * pgaudit.h
 * 
 * 
 * 
 * IDENTIFICATION
 *        src/include/pgaudit.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef _PGAUDIT_H
#define _PGAUDIT_H

#include "fmgr.h"

extern THR_LOCAL bool Audit_delete;

#define AUDIT_EXEC_ENABLED (u_sess->attr.attr_security.Audit_enabled && u_sess->attr.attr_security.Audit_Exec)
#define AUDIT_COPY_ENABLED (u_sess->attr.attr_security.Audit_enabled && u_sess->attr.attr_security.Audit_Copy)
#define CHECK_AUDIT_DDL(type) ((unsigned int)u_sess->attr.attr_security.Audit_DDL & (1 << (type)))
#define CHECK_AUDIT_LOGIN(type) (unsigned int)u_sess->attr.attr_security.Audit_Session & (1 << (type));
#define PG_QUERY_AUDIT_ARGS_MAX 3

extern THR_LOCAL bool am_sysauditor;

#ifndef WIN32
extern int sysauditPipe[2];
#else
extern HANDLE sysauditPipe[2];
#endif

extern ThreadId pgaudit_start(void);
extern void pgaudit_start_all(void);
extern void pgaudit_stop_all(void);
extern void allow_immediate_pgaudit_restart(void);

// multi-thread audit
extern void audit_process_cxt_init(void);
extern void audit_process_cxt_exit();
extern int audit_load_thread_index(void);

#ifdef EXEC_BACKEND
extern void PgAuditorMain();
#endif

/* ----------
 * Functions called from backends, the sequence is relevent to struct AuditTypeDescs which should be changed in the same time
 * ----------
 */

typedef enum {
    AUDIT_UNKNOWN_TYPE = 0,
    AUDIT_LOGIN_SUCCESS,
    AUDIT_LOGIN_FAILED,
    AUDIT_USER_LOGOUT,
    AUDIT_SYSTEM_START,
    AUDIT_SYSTEM_STOP,
    AUDIT_SYSTEM_RECOVER,
    AUDIT_SYSTEM_SWITCH,
    AUDIT_LOCK_USER,
    AUDIT_UNLOCK_USER,
    AUDIT_GRANT_ROLE,
    AUDIT_REVOKE_ROLE,
    AUDIT_USER_VIOLATION,
    AUDIT_DDL_DATABASE,
    AUDIT_DDL_DIRECTORY,
    AUDIT_DDL_TABLESPACE,
    AUDIT_DDL_SCHEMA,
    AUDIT_DDL_USER,
    AUDIT_DDL_TABLE,
    AUDIT_DDL_INDEX,
    AUDIT_DDL_VIEW,
    AUDIT_DDL_TRIGGER,
    AUDIT_DDL_FUNCTION,
    AUDIT_DDL_RESOURCEPOOL,
    AUDIT_DDL_WORKLOAD,
    AUDIT_DDL_SERVERFORHADOOP,
    AUDIT_DDL_DATASOURCE,
    AUDIT_DDL_NODEGROUP,
    AUDIT_DDL_ROWLEVELSECURITY,
    AUDIT_DDL_SYNONYM,
    AUDIT_DDL_TYPE,
    AUDIT_DDL_TEXTSEARCH,
    AUDIT_DML_ACTION,
    AUDIT_DML_ACTION_SELECT,
    AUDIT_INTERNAL_EVENT,
    AUDIT_FUNCTION_EXEC,
    AUDIT_COPY_TO,
    AUDIT_COPY_FROM,
    AUDIT_SET_PARAMETER,
    AUDIT_POLICY_EVENT,
    MASKING_POLICY_EVENT,
	SECURITY_EVENT,
	AUDIT_DDL_SEQUENCE,           // ddl_sequence in struct AuditTypeDescs   
    AUDIT_DDL_KEY,
    AUDIT_DDL_PACKAGE,
    AUDIT_DDL_MODEL,
    AUDIT_DDL_GLOBALCONFIG,
    AUDIT_DDL_PUBLICATION_SUBSCRIPTION,
    AUDIT_DDL_FOREIGN_DATA_WRAPPER
} AuditType;

/* keep the same sequence with parameter audit_system_object */
typedef enum {
    DDL_DATABASE = 0,
    DDL_SCHEMA,
    DDL_USER,
    DDL_TABLE,
    DDL_INDEX,
    DDL_VIEW,
    DDL_TRIGGER,
    DDL_FUNCTION,
    DDL_TABLESPACE,
    DDL_RESOURCEPOOL,
    DDL_WORKLOAD,
    DDL_SERVERFORHADOOP,
    DDL_DATASOURCE,
    DDL_NODEGROUP,
    DDL_ROWLEVELSECURITY,
    DDL_TYPE,
    DDL_TEXTSEARCH,
    DDL_DIRECTORY,
    DDL_SYNONYM,
    DDL_SEQUENCE,
    DDL_KEY,
    DDL_PACKAGE,
    DDL_MODEL,
    DDL_PUBLICATION_SUBSCRIPTION,
    DDL_GLOBALCONFIG,
    DDL_FOREIGN_DATA_WRAPPER
} DDLType;

/*
 * Brief        : the string field number in audit record
 * Description    :
 */
typedef enum {
    AUDIT_USER_ID = 0,
    AUDIT_USER_NAME,
    AUDIT_DATABASE_NAME,
    AUDIT_CLIENT_CONNINFO,
    AUDIT_OBJECT_NAME,
    AUDIT_DETAIL_INFO,
    AUDIT_NODENAME_INFO,
    AUDIT_THREADID_INFO,
    AUDIT_LOCALPORT_INFO,
    AUDIT_REMOTEPORT_INFO
} AuditStringFieldNum;

struct AuditElasticEvent {
    const char* aDataType;
    const char* aDataResult;
    const char* auditUserId;
    const char* auditUserName;
    const char* auditDatabaseName;
    const char* clientConnInfo;
    const char* objectName;
    const char* detailInfo;
    const char* nodeNameInfo;
    const char* threadIdInfo;
    const char* localPortInfo;
    const char* remotePortInfo;
    long long   eventTime;
};

typedef enum { AUDIT_UNKNOWN = 0, AUDIT_OK, AUDIT_FAILED } AuditResult;
typedef enum { AUDIT_FUNC_QUERY = 0, AUDIT_FUNC_DELETE } AuditFuncType;
typedef enum { STD_AUDIT_TYPE = 0, UNIFIED_AUDIT_TYPE } AuditClassType;

extern void audit_report(AuditType type, AuditResult result, const char* object_name, const char* detail_info, AuditClassType ctype = STD_AUDIT_TYPE);
extern Datum pg_query_audit(PG_FUNCTION_ARGS);
extern Datum pg_delete_audit(PG_FUNCTION_ARGS);
extern bool pg_auditor_thread(ThreadId pid);

/* define a macro about the return value of security function */
#define check_intval(errno, express, retval, file, line) \
    {                                                    \
        if (errno == -1) {                               \
            fprintf(stderr,                              \
                "%s:%d failed on calling "               \
                "security function.\n",                  \
                file,                                    \
                line);                                   \
            express;                                     \
            return retval;                               \
        }                                                \
    }

#define securec_check_intval(val, express, retval) check_intval(val, express, retval, __FILE__, __LINE__)

#endif /* _PGAUDIT_H */
