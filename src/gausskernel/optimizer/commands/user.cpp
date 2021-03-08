/* -------------------------------------------------------------------------
 *
 * user.cpp
 *	  Commands for manipulating roles (formerly called users).
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/gausskernel/optimizer/commands/user.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/genam.h"
#include "access/heapam.h"
#include "access/tableam.h"
#include "access/xact.h"
#include "catalog/catalog.h"
#include "commands/defrem.h"
#include "catalog/dependency.h"
#include "catalog/namespace.h"
#include "catalog/indexing.h"
#include "catalog/namespace.h"
#include "catalog/objectaccess.h"
#include "catalog/pg_auth_members.h"
#include "catalog/pg_authid.h"
#include "catalog/pg_database.h"
#include "catalog/pg_db_role_setting.h"
#include "catalog/pg_job.h"
#include "catalog/pg_namespace.h"
#include "catalog/gs_global_config.h"
#include "commands/comment.h"
#include "commands/dbcommands.h"
#include "commands/schemacmds.h"
#include "commands/seclabel.h"
#include "commands/user.h"
#include "gaussdb_version.h"
#include "libpq/auth.h"
#include "libpq/md5.h"
#include "libpq/crypt.h"
#include "miscadmin.h"
#include "nodes/pg_list.h"
#include "nodes/value.h"
#include "storage/lmgr.h"
#include "storage/predicate_internals.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/date.h"
#include "utils/fmgroids.h"
#include "utils/inval.h"
#include "utils/lsyscache.h"
#include "utils/memprot.h"
#include "utils/syscache.h"
#include "utils/timestamp.h"
#include "utils/snapmgr.h"
#include "catalog/pg_auth_history.h"
#include "catalog/pg_user_status.h"
#include "pgstat.h"
#include "libpq/sha2.h"
#include "storage/proc.h"
#include "storage/pmsignal.h"
#include "storage/procarray.h"
#include "auditfuncs.h"
#include "utils/inval.h"
#include "access/xact.h"
#include "pgxc/poolutils.h"
#include "tcop/utility.h"
#include "pgxc/pgxc.h"
#include "catalog/pgxc_group.h"
#include "openssl/rand.h"
#include "instruments/instr_workload.h"

#if defined(__LP64__) || defined(__64BIT__)
typedef unsigned int GS_UINT32;
#else
typedef unsigned long GS_UINT32;
#endif

MemoryContext WaitCountGlobalContext = NULL;

#define CREATE_PG_AUTH_ROLE 1
#define ALTER_PG_AUTH_ROLE 2
#define DEFAULT_PASSWORD_POLICY 1
#define PERSISTENCE_VERSION_NUM 92204

/* Hook to check passwords in CreateRole() and AlterRole() */
THR_LOCAL check_password_hook_type check_password_hook = NULL;

static List* roleNamesToIds(const List* memberNames);
static void AddRoleMems(
    const char* rolename, Oid roleid, const List* memberNames, List* memberIds, Oid grantorId, bool admin_opt);
static void DelRoleMems(const char* rolename, Oid roleid, const List* memberNames, List* memberIds, bool admin_opt);

/* to check whether the current schema belongs to one of the role in the llist */
static bool IsLockOnRelation(const LockInstanceData* instance);
static List* GetCancelQuery(const char* user_name);
static bool IsEligiblePid(Oid rel_oid, Oid nsp_oid, ThreadId pid, Oid db_oid, Form_pg_class form, List* query_list);
static bool IsDuplicatePid(const List* query_list, ThreadId pid);
static void CancelQuery(const char* user_name);
extern void cancel_backend(ThreadId pid);
static bool IsCurrentSchemaAttachRoles(List* roles);

/* Database Security: Support password complexity */
static bool IsSpecialCharacter(char ch);
static bool IsPasswdEqual(const char* roleID, char* passwd1, char* passwd2);
static void IsPasswdSatisfyPolicy(char* Password);
static bool CheckPasswordComplexity(
    const char* roleID, char* newPasswd, char* oldPasswd, bool isCreaterole);
static void AddAuthHistory(Oid roleID, const char* rolename, const char* passwd, int operatType, const char* salt);
static void DropAuthHistory(Oid roleID);

/* Check weak password */
static bool is_weak_password(const char* password);
static void check_weak_password(char *Password);

/* Database Security: Support lock/unlock account */
void TryLockAccount(Oid roleID, int extrafails, bool superlock);
bool TryUnlockAccount(Oid roleID, bool superunlock, bool isreset);
void TryUnlockAllAccounts(void);
USER_STATUS GetAccountLockedStatus(Oid roleID);
void SetAccountPasswordExpired(Oid roleID, bool expired);
void DropUserStatus(Oid roleID);
Oid GetRoleOid(const char* username);
static void UpdateUnlockAccountTuples(HeapTuple tuple, Relation rel, TupleDesc tupledesc);
/* Database Security: Support password complexity */
static char* reverse_string(const char* str);
/* Calculate the encrypt password. */
static Datum calculate_encrypted_password(
    bool is_encrypted, const char* password, const char* rolname, const char* salt_string);
bool IsRoleExist(const char* username);

/* show the expired password time from now. */
extern Datum gs_password_deadline(PG_FUNCTION_ARGS);
/* show the notice time user set. */
extern Datum gs_password_notifytime(PG_FUNCTION_ARGS);

extern uint64 parseTableSpaceMaxSize(char* maxSize, bool* unlimited, char** newMaxSize);
void encode_iteration(int auth_count, char* auth_iteration_string);
void initWaitCountHashTbl();
void initSqlCountUser();
void initWaitCountCell(
    Oid userid, PgStat_WaitCountStatusCell* WaitCountStatusCell, int dataid, int listNodeid, bool foundid);
void initWaitCount(Oid userid);
static inline void clean_role_password(const DefElem* dpassword);

/* Check if current user has createrole privileges */
static bool have_createrole_privilege(void)
{
    return has_createrole_privilege(GetUserId());
}

int WaitCountMatch(const void* key1, const void* key2, Size keysize)
{
    return *(Oid*)key1 == *(Oid*)key2 ? 0 : 1;
}

void* WaitCountAlloc(Size size)
{
    Assert(MemoryContextIsValid(WaitCountGlobalContext));
    return MemoryContextAlloc(WaitCountGlobalContext, size);
}

/*
 * @Description: create a shared memory context 'WaitCountGlobalContext' under g_instance.instance_context
 *			for g_instance.stat_cxt.WaitCountHashTbl and g_instance.stat_cxt.WaitCountStatusList. And create a share a
 * hashtable 'g_instance.stat_cxt.WaitCountHashTbl'.
 */
void initWaitCountHashTbl()
{
    HASHCTL ctl;
    MemoryContext oldContext;

    WaitCountGlobalContext = AllocSetContextCreate(g_instance.instance_context,
        "WaitCountGlobalContext",
        ALLOCSET_DEFAULT_MINSIZE,
        ALLOCSET_DEFAULT_INITSIZE,
        ALLOCSET_DEFAULT_MAXSIZE,
        SHARED_CONTEXT);

    errno_t rc = memset_s(&ctl, sizeof(ctl), 0, sizeof(ctl));
    securec_check(rc, "\0", "\0");

    oldContext = MemoryContextSwitchTo(WaitCountGlobalContext);

    ctl.keysize = sizeof(Oid);
    ctl.entrysize = sizeof(WaitCountHashValue);
    ctl.hash = oid_hash;
    ctl.hcxt = WaitCountGlobalContext;
    ctl.alloc = WaitCountAlloc;
    ctl.dealloc = pfree;
    ctl.match = WaitCountMatch;

    g_instance.stat_cxt.WaitCountHashTbl = hash_create("sql count lookup hash",
        256,
        &ctl,
        HASH_ELEM | HASH_FUNCTION | HASH_SHRCTX | HASH_ALLOC | HASH_DEALLOC | HASH_COMPARE);

    (void)MemoryContextSwitchTo(oldContext);
}

/*
 * @Description: init user`s sql count in WaitCountArray and insert idx into hashtable .
 * @in1 -userid : user`s id in system
 * @in2 -WaitCountStatusCell : one of the g_instance.stat_cxt.WaitCountStatusList cell
 * @in3 - dataid : array index
 * @in4 - listNodeid : g_instance.stat_cxt.WaitCountStatusList cell number
 * @in5 - foundid
 * @out - void
 */
void initWaitCountCell(
    Oid userid, PgStat_WaitCountStatusCell* WaitCountStatusCell, int dataid, int listNodeid, bool foundid)
{
    /* init user`s sql count in g_instance.stat_cxt.WaitCountStatusList */
    pg_atomic_init_u64(&WaitCountStatusCell->WaitCountArray[dataid].wc_cnt.wc_sql_select, 0);
    pg_atomic_init_u64(&WaitCountStatusCell->WaitCountArray[dataid].wc_cnt.wc_sql_update, 0);
    pg_atomic_init_u64(&WaitCountStatusCell->WaitCountArray[dataid].wc_cnt.wc_sql_insert, 0);
    pg_atomic_init_u64(&WaitCountStatusCell->WaitCountArray[dataid].wc_cnt.wc_sql_delete, 0);
    pg_atomic_init_u64(&WaitCountStatusCell->WaitCountArray[dataid].wc_cnt.wc_sql_mergeinto, 0);
    pg_atomic_init_u64(&WaitCountStatusCell->WaitCountArray[dataid].wc_cnt.wc_sql_ddl, 0);
    pg_atomic_init_u64(&WaitCountStatusCell->WaitCountArray[dataid].wc_cnt.wc_sql_dml, 0);
    pg_atomic_init_u64(&WaitCountStatusCell->WaitCountArray[dataid].wc_cnt.wc_sql_dcl, 0);
    pg_atomic_init_u64(&WaitCountStatusCell->WaitCountArray[dataid].wc_cnt.insertElapse.total_time, 0);
    pg_atomic_init_u64(&WaitCountStatusCell->WaitCountArray[dataid].wc_cnt.insertElapse.min_time, 0);
    pg_atomic_init_u64(&WaitCountStatusCell->WaitCountArray[dataid].wc_cnt.insertElapse.max_time, 0);
    pg_atomic_init_u64(&WaitCountStatusCell->WaitCountArray[dataid].wc_cnt.selectElapse.total_time, 0);
    pg_atomic_init_u64(&WaitCountStatusCell->WaitCountArray[dataid].wc_cnt.selectElapse.min_time, 0);
    pg_atomic_init_u64(&WaitCountStatusCell->WaitCountArray[dataid].wc_cnt.selectElapse.max_time, 0);
    pg_atomic_init_u64(&WaitCountStatusCell->WaitCountArray[dataid].wc_cnt.updateElapse.total_time, 0);
    pg_atomic_init_u64(&WaitCountStatusCell->WaitCountArray[dataid].wc_cnt.updateElapse.min_time, 0);
    pg_atomic_init_u64(&WaitCountStatusCell->WaitCountArray[dataid].wc_cnt.updateElapse.max_time, 0);
    pg_atomic_init_u64(&WaitCountStatusCell->WaitCountArray[dataid].wc_cnt.deleteElapse.total_time, 0);
    pg_atomic_init_u64(&WaitCountStatusCell->WaitCountArray[dataid].wc_cnt.deleteElapse.min_time, 0);
    pg_atomic_init_u64(&WaitCountStatusCell->WaitCountArray[dataid].wc_cnt.deleteElapse.max_time, 0);
    pg_atomic_init_u32(&WaitCountStatusCell->WaitCountArray[dataid].userid, userid);

    /* insert g_instance.stat_cxt.WaitCountStatusList idx into hashtable */
    WaitCountHashValue* WaitCountIdx =
        (WaitCountHashValue*)hash_search(g_instance.stat_cxt.WaitCountHashTbl, &userid, HASH_ENTER_NULL, &foundid);
    if (WaitCountIdx != NULL) {
        errno_t rc = memset_s(WaitCountIdx, sizeof(WaitCountHashValue), 0, sizeof(WaitCountHashValue));
        securec_check(rc, "\0", "\0");
        WaitCountIdx->userid = userid;
        WaitCountIdx->idx = listNodeid * WAIT_COUNT_ARRAY_SIZE + dataid;
    }
}

/*
 * @Description: if g_instance.stat_cxt.WaitCountStatusList is null, then new_list it.
 * g_instance.stat_cxt.WaitCountStatusList is full, then append a new cell to it. init user into
 * g_instance.stat_cxt.WaitCountStatusList and g_instance.stat_cxt.WaitCountHashTbl, and now hashtable is used for a
 * mapping table between userid and list index. convinient for insert and find user. when init action done , ready to
 * record sql count for user.
 * @in -userid : user`s id in system
 * @out - void
 */
void initWaitCount(Oid userid)
{
    int dataid;
    int listNodeid;
    bool foundid = FALSE;
    MemoryContext oldContext;

    oldContext = MemoryContextSwitchTo(WaitCountGlobalContext);

    /*
     * when init first user, g_instance.stat_cxt.WaitCountStatusList is NULL, new_list it and append first cell to it.
     * and init the user in WaitCountStatusCell, ready for sql count.
     */
    if (g_instance.stat_cxt.WaitCountStatusList == NULL) {
        PgStat_WaitCountStatusCell* WaitCountStatusCell =
            (PgStat_WaitCountStatusCell*)palloc0(sizeof(PgStat_WaitCountStatusCell));
        dataid = 0;
        listNodeid = 0;
        foundid = TRUE;
        initWaitCountCell(userid, WaitCountStatusCell, dataid, listNodeid, foundid);
        g_instance.stat_cxt.WaitCountStatusList =
            lappend(g_instance.stat_cxt.WaitCountStatusList, (void*)(WaitCountStatusCell));
    } else {
        ListCell* lc = NULL;
        listNodeid = 0;
        PgStat_WaitCountStatusCell* WaitCountStatusCell = NULL;
        foreach (lc, g_instance.stat_cxt.WaitCountStatusList) {
            WaitCountStatusCell = (PgStat_WaitCountStatusCell*)lfirst(lc);
            for (int i = 0; i < WAIT_COUNT_ARRAY_SIZE; i++) {
                if (WaitCountStatusCell->WaitCountArray[i].userid == 0) {
                    dataid = i;
                    foundid = TRUE;
                    initWaitCountCell(userid, WaitCountStatusCell, dataid, listNodeid, foundid);
                    break;
                }
            }
            listNodeid++;
            if (foundid)
                break;
        }
    }
    /*
     * when the first cell is full, can`t find the empty location to init user.
     * then, append the next cell to it.
     * and init the user in the new WaitCountStatusCell, ready for sql count.
     */
    if (!foundid) {
        PgStat_WaitCountStatusCell* WaitCountStatusCell =
            (PgStat_WaitCountStatusCell*)palloc0(sizeof(PgStat_WaitCountStatusCell));
        dataid = 0;
        listNodeid = g_instance.stat_cxt.WaitCountStatusList->length;
        foundid = TRUE;
        initWaitCountCell(userid, WaitCountStatusCell, dataid, listNodeid, foundid);
        g_instance.stat_cxt.WaitCountStatusList =
            lappend(g_instance.stat_cxt.WaitCountStatusList, (void*)(WaitCountStatusCell));
    }
    (void)MemoryContextSwitchTo(oldContext);
}

/*
 * @Description: use systable_beginscan to init all users for sql count;
 * @in -:
 * @out - void
 */
void initSqlCountUser()
{
    ResourceOwner currentOwner = t_thrd.utils_cxt.CurrentResourceOwner;
    ResourceOwner tmpOwner;
    t_thrd.utils_cxt.CurrentResourceOwner = ResourceOwnerCreate(NULL, "ForSqlCount", MEMORY_CONTEXT_OPTIMIZER);

    Relation relation = heap_open(AuthIdRelationId, AccessShareLock);
    SysScanDesc scan = systable_beginscan(relation, InvalidOid, false, SnapshotNow, 0, NULL);
    HeapTuple tup = NULL;

    while (HeapTupleIsValid((tup = systable_getnext(scan)))) {
        Oid roleid = HeapTupleGetOid(tup);
        initWaitCount(roleid);
    }

    systable_endscan(scan);
    heap_close(relation, AccessShareLock);

    ResourceOwnerRelease(t_thrd.utils_cxt.CurrentResourceOwner, RESOURCE_RELEASE_BEFORE_LOCKS, true, true);
    ResourceOwnerRelease(t_thrd.utils_cxt.CurrentResourceOwner, RESOURCE_RELEASE_LOCKS, true, true);
    ResourceOwnerRelease(t_thrd.utils_cxt.CurrentResourceOwner, RESOURCE_RELEASE_AFTER_LOCKS, true, true);
    tmpOwner = t_thrd.utils_cxt.CurrentResourceOwner;
    t_thrd.utils_cxt.CurrentResourceOwner = NULL;
    ResourceOwnerDelete(tmpOwner);
    t_thrd.utils_cxt.CurrentResourceOwner = currentOwner;
}

/*
 * @Description: init hashtable and users for sql count.
 * @in -:
 * @out - void
 */
void initSqlCount()
{
    (void)LWLockAcquire(WaitCountHashLock, LW_EXCLUSIVE);
    if (g_instance.stat_cxt.WaitCountHashTbl == NULL) {
        /* create hashtable at first */
        initWaitCountHashTbl();

        /* init all db users */
        initSqlCountUser();
    }
    LWLockRelease(WaitCountHashLock);
}

/*
 * Get the value of rolkind from the tuple
 */
static char get_rolkind(HeapTuple utup)
{
    bool isNull = true;
    Datum datum;

    datum = SysCacheGetAttr(AUTHOID, utup, Anum_pg_authid_rolkind, &isNull);

    return isNull ? ROLKIND_NORMAL : DatumGetChar(datum);
}

/*
 * Check all members of roleid whether all members are attached to group_oid.
 */
static void check_nodegroup_role_members(Oid group_oid, Oid roleid)
{
    TableScanDesc scan;
    Form_pg_authid auth;
    Oid member;

    Relation relation = heap_open(AuthIdRelationId, AccessShareLock);

    scan = tableam_scan_begin(relation, SnapshotNow, 0, NULL);
    HeapTuple rtup = (HeapTuple) tableam_scan_getnexttuple(scan, ForwardScanDirection);

    while (rtup) {
        auth = (Form_pg_authid)GETSTRUCT(rtup);

        /* ignore admin users. */
        if (auth->rolsuper || auth->rolsystemadmin || auth->rolcreaterole) {
            rtup = (HeapTuple) tableam_scan_getnexttuple(scan, ForwardScanDirection);
            continue;
        }

        member = HeapTupleGetOid(rtup);
        if (member == roleid) {
            rtup = (HeapTuple) tableam_scan_getnexttuple(scan, ForwardScanDirection);
            continue;
        }
        if (is_member_of_role_nosuper(member, roleid)) {
            if (get_pgxc_logic_groupoid(member) != group_oid) {
                tableam_scan_end(scan);
                heap_close(relation, AccessShareLock);
                ereport(ERROR,
                    (errcode(ERRCODE_INVALID_GRANT_OPERATION),
                        errmsg("Role \"%s\" is member of \"%s\", but "
                               "do not attach to node group \"%s\".",
                            NameStr(auth->rolname),
                            GetUserNameFromId(roleid),
                            get_pgxc_groupname(group_oid))));
            }
        }

        rtup = (HeapTuple) tableam_scan_getnexttuple(scan, ForwardScanDirection);
    }

    tableam_scan_end(scan);
    heap_close(relation, AccessShareLock);
}

/*
 * grant_nodegroup_to_role
 * grant all privilege of node group to role
 */
static void grant_nodegroup_to_role(Oid groupoid, Oid roleid, bool is_grant)
{
    char in_redis;
    grantNodeGroupToRole(groupoid, roleid, ACL_ALL_RIGHTS_NODEGROUP, is_grant);
    in_redis = get_pgxc_group_redistributionstatus(groupoid);
    if (in_redis == PGXC_REDISTRIBUTION_SRC_GROUP) {
        Oid dest_group = PgxcGroupGetRedistDestGroupOid();
        if (OidIsValid(dest_group)) {
            grantNodeGroupToRole(dest_group, roleid, ACL_ALL_RIGHTS_NODEGROUP, is_grant);
        }
    }
}

/*
 * switch_logic_cluster
 * Alter the node group of roieid to new_node_group
 */
static Oid switch_logic_cluster(Oid roleid, char* new_node_group, bool* is_installation)
{
    char group_kind;
    Oid current_group_id;
    Oid new_group_id;

    *is_installation = false;

    new_group_id = get_pgxc_groupoid(new_node_group);
    if (!OidIsValid(new_group_id)) {
        ereport(ERROR,
            (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("Node group \"%s\": node group not existed.", new_node_group)));
    }
    group_kind = get_pgxc_groupkind(new_group_id);
    if (group_kind != PGXC_GROUPKIND_LCGROUP && group_kind != PGXC_GROUPKIND_INSTALLATION) {
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("Node group \"%s\" must be virtual cluster or installation group.", new_node_group)));
    }

    Oid mem_nodegroup = get_nodegroup_privs_of(roleid);
    if (OidIsValid(mem_nodegroup) && new_group_id != mem_nodegroup) {
        char in_redis = get_pgxc_group_redistributionstatus(mem_nodegroup);
        char in_redis_new = get_pgxc_group_redistributionstatus(new_group_id);
        if (!(in_redis == PGXC_REDISTRIBUTION_SRC_GROUP || in_redis_new == PGXC_REDISTRIBUTION_DST_GROUP)) {
            /* Old group and new group are not redistribution group */
            ereport(ERROR,
                (errcode(ERRCODE_INVALID_GRANT_OPERATION),
                    (errmsg("Can not alter Role %u node group across logic clusters.", roleid))));
        }
    }

    check_nodegroup_role_members(new_group_id, roleid);

    current_group_id = get_pgxc_logic_groupoid(roleid);
    if (OidIsValid(current_group_id)) {
        /* revoke the privilege on old node group */
        grant_nodegroup_to_role(current_group_id, roleid, false);
    }

    if (group_kind == PGXC_GROUPKIND_INSTALLATION) {
        *is_installation = true;
    }

    return new_group_id;
}

static inline void clean_role_password(const DefElem* dpassword)
{
    ListCell* head = NULL;
    A_Const* pwdargs = NULL;
    char* password = NULL;
    char* replPasswd = NULL;

    if (dpassword != NULL && dpassword->arg != NULL) {
        head = list_head((List*)dpassword->arg);
        if (head != NULL) {
            /* reset password if any, usually used in create role and alter role password. */
            pwdargs = (A_Const*)linitial((List*)dpassword->arg);
            if (pwdargs != NULL) {
                password = strVal(&pwdargs->val);
                str_reset(password);
            }

            if (lnext(head)) {
                /* reset replace password if any, usually used in alter role password. */
                pwdargs = (A_Const*)lsecond((List*)dpassword->arg);
                if (pwdargs != NULL) {
                    replPasswd = strVal(&pwdargs->val);
                    str_reset(replPasswd);
                }
            }
        }
    }

    return;
}

/*
 * CREATE ROLE
 */
void CreateRole(CreateRoleStmt* stmt)
{
    Datum new_record[Natts_pg_authid];
    bool new_record_nulls[Natts_pg_authid] = {false};
    Oid roleid = InvalidOid;
    ListCell* item = NULL;
    ListCell* option = NULL;
    char* password = NULL; /* user password */
    char salt_bytes[SALT_LENGTH + 1] = {0};
    char salt_string[SALT_LENGTH * 2 + 1] = {0};
    bool encrypt_password = true;
    bool issuper = false;       /* Make the user a superuser? */
    bool inherit = true;        /* Auto inherit privileges? */
    bool createrole = false;    /* Can this user create roles? */
    bool createdb = false;      /* Can the user create databases? */
    bool useft = false;         /* Can the user use foreign table? */
    bool canlogin = false;      /* Can this user login? */
    bool isreplication = false; /* Is this a replication role? */
    /* Database Security:  Support separation of privilege.*/
    bool isauditadmin = false;  /* Is this a auditadmin role? */
    bool issystemadmin = false; /* Is this a systemadmin role? */
    bool ismonitoradmin = false;	/* Is this a monitoradmin role? */
    bool isoperatoradmin = false; /* Is this a operatoradmin role? */
    bool ispolicyadmin = false; /* Is this a security policyadmin role? */
    bool isvcadmin = false;     /* Is this a vcadmin role? */
    int connlimit = -1;         /* maximum connections allowed */
    List* addroleto = NIL;      /* roles to make this a member of */
    List* rolemembers = NIL;    /* roles to be members of this role */
    List* adminmembers = NIL;   /* roles to be admins of this role */
    char* validBegin = NULL;    /* time the login is valid begin */
    Datum validBegin_datum;     /* same, as timestamptz Datum */
    bool validBegin_null = false;
    char* validUntil = NULL; /* time the login is valid until */
    Datum validUntil_datum;  /* same, as timestamptz Datum */
    bool validUntil_null = false;
    char respool[NAMEDATALEN] = {0}; /* name of the resource pool */
    Datum respool_datum;             /* same, as resource pool Datum */
    bool respool_null = false;
    Oid parentid = InvalidOid; /* parent user id */
    Oid rpoid = InvalidOid;
    bool parentid_null = false;
    int64 spacelimit = 0;
    int64 tmpspacelimit = 0;
    int64 spillspacelimit = 0;
    Oid nodegroup_id = InvalidOid;
    bool isindependent = false;
    bool ispersistence = false;
    int isexpired = 0;
    DefElem* dpassword = NULL;
    DefElem* dinherit = NULL;
    DefElem* dcreaterole = NULL;
    DefElem* dcreatedb = NULL;
    DefElem* duseft = NULL;
    DefElem* dcanlogin = NULL;
    DefElem* disreplication = NULL;
    DefElem* dexpired = NULL;
    /* Database Security:  Support separation of privilege.*/
    DefElem* disauditadmin = NULL;
    DefElem* dissystemadmin = NULL;
    DefElem* dismonitoradmin = NULL;
    DefElem* disoperatoradmin = NULL;
    DefElem* dispolicyadmin = NULL;
    DefElem* disvcadmin = NULL;
    DefElem* dconnlimit = NULL;
    DefElem* daddroleto = NULL;
    DefElem* drolemembers = NULL;
    DefElem* dadminmembers = NULL;
    DefElem* dvalidBegin = NULL;
    DefElem* dvalidUntil = NULL;
    DefElem* drespool = NULL;
    DefElem* dtablespace = NULL;
    DefElem* dindependent = NULL;
    DefElem* dpersistence = NULL;
    DefElem* dparent = NULL;
    DefElem* dpguser = NULL;
    DefElem* dparent_default = NULL;
    DefElem* dspacelimit = NULL;
    DefElem* dtmpspacelimit = NULL;
    DefElem* dspillspacelimit = NULL;
    DefElem* dnode_group = NULL;
    bool is_default = false;
    GS_UINT32 retval = 0;

    /* Database Security: Support lock/unlock account */
    Relation pg_user_status_rel;
    A_Const* pwdargs = NULL;

    bool unLimited = false;
    bool tmpUnlimited = false;
    bool spillUnlimited = false;
    char* maxSizeStr = NULL;
    char* tmpMaxSizeStr = NULL;
    char* spillMaxSizeStr = NULL;

    /* The defaults can vary depending on the original statement type */
    switch (stmt->stmt_type) {
        case ROLESTMT_ROLE:
            break;
        case ROLESTMT_USER:
            canlogin = true;
            /* may eventually want inherit to default to false here */
            break;
        case ROLESTMT_GROUP:
            break;
        default:
            break;
    }

    /* Extract options from the statement node tree */
    foreach (option, stmt->options) {
        DefElem* defel = (DefElem*)lfirst(option);

        if (strcmp(defel->defname, "password") == 0 || strcmp(defel->defname, "encryptedPassword") == 0 ||
            strcmp(defel->defname, "unencryptedPassword") == 0 || strcmp(defel->defname, "expiredPassword") == 0) {
            if (dpassword != NULL) {
                clean_role_password(dpassword);
                ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("conflicting or redundant options")));
            }
            dpassword = defel;
            if (strcmp(defel->defname, "encryptedPassword") == 0)
                encrypt_password = true;
            else if (strcmp(defel->defname, "unencryptedPassword") == 0) {
                clean_role_password(dpassword);
                ereport(ERROR,
                    (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                        errmsg("Permission denied to create role with option UNENCRYPTED.")));
            } else if (strcmp(defel->defname, "expiredPassword") == 0) {
                isexpired = 1;
            }
        } else if (strcmp(defel->defname, "sysid") == 0) {
            ereport(NOTICE, (errmsg("SYSID can no longer be specified")));
        } else if (strcmp(defel->defname, "inherit") == 0) {
            if (dinherit != NULL) {
                clean_role_password(dpassword);
                ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("conflicting or redundant options")));
            }
            dinherit = defel;
        } else if (strcmp(defel->defname, "createrole") == 0) {
            if (dcreaterole != NULL) {
                clean_role_password(dpassword);
                ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("conflicting or redundant options")));
            }
            dcreaterole = defel;
        } else if (strcmp(defel->defname, "createdb") == 0) {
            if (dcreatedb != NULL) {
                clean_role_password(dpassword);
                ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("conflicting or redundant options")));
            }
            dcreatedb = defel;
        } else if (strcmp(defel->defname, "useft") == 0) {
            if (duseft != NULL) {
                clean_role_password(dpassword);
                ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("conflicting or redundant options")));
            }
            duseft = defel;
        } else if (strcmp(defel->defname, "canlogin") == 0) {
            if (dcanlogin != NULL) {
                clean_role_password(dpassword);
                ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("conflicting or redundant options")));
            }
            dcanlogin = defel;
        } else if (strcmp(defel->defname, "isreplication") == 0) {
            if (disreplication != NULL) {
                clean_role_password(dpassword);
                ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("conflicting or redundant options")));
            }
            disreplication = defel;
        } else if (strcmp(defel->defname, "isauditadmin") == 0) {
            /* add audit admin privilege */
            if (disauditadmin != NULL) {
                clean_role_password(dpassword);
                ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("conflicting or redundant options")));
            }
            disauditadmin = defel;
        } else if (strcmp(defel->defname, "issystemadmin") == 0) {
            /* Database Security:  Support separation of privilege. */
            if (dissystemadmin != NULL) {
                clean_role_password(dpassword);
                ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("conflicting or redundant options")));
            }
            dissystemadmin = defel;
        } else if (strcmp(defel->defname, "ismonitoradmin") == 0) {
            if (dismonitoradmin != NULL) {
                clean_role_password(dpassword);
                ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("conflicting or redundant options")));
            }
            dismonitoradmin = defel;
        } else if (strcmp(defel->defname, "isoperatoradmin") == 0) {
            if (disoperatoradmin != NULL) {
                clean_role_password(dpassword);
                ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("conflicting or redundant options")));
            }
            disoperatoradmin = defel;
        } else if (strcmp(defel->defname, "ispolicyadmin") == 0) {
            if (dispolicyadmin != NULL) {
                clean_role_password(dpassword);
                ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("conflicting or redundant options")));
            }
            dispolicyadmin = defel;
        } else if (strcmp(defel->defname, "isvcadmin") == 0) {
            if (disvcadmin != NULL) {
                clean_role_password(dpassword);
                ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("conflicting or redundant options")));
            }
            disvcadmin = defel;
        } else if (strcmp(defel->defname, "connectionlimit") == 0) {
            if (dconnlimit != NULL) {
                clean_role_password(dpassword);
                ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("conflicting or redundant options")));
            }
            dconnlimit = defel;
        } else if (strcmp(defel->defname, "addroleto") == 0) {
            if (daddroleto != NULL) {
                clean_role_password(dpassword);
                ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("conflicting or redundant options")));
            }
            daddroleto = defel;
        } else if (strcmp(defel->defname, "rolemembers") == 0) {
            if (drolemembers != NULL) {
                clean_role_password(dpassword);
                ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("conflicting or redundant options")));
            }
            drolemembers = defel;
        } else if (strcmp(defel->defname, "adminmembers") == 0) {
            if (dadminmembers != NULL) {
                clean_role_password(dpassword);
                ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("conflicting or redundant options")));
            }
            dadminmembers = defel;
        } else if (strcmp(defel->defname, "validBegin") == 0) {
            if (dvalidBegin != NULL) {
                clean_role_password(dpassword);
                ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("conflicting or redundant options")));
            }
            dvalidBegin = defel;
        } else if (strcmp(defel->defname, "validUntil") == 0) {
            if (dvalidUntil != NULL) {
                clean_role_password(dpassword);
                ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("conflicting or redundant options")));
            }
            dvalidUntil = defel;
        } else if (strcmp(defel->defname, "respool") == 0) {
            if (drespool != NULL) {
                clean_role_password(dpassword);
                ereport(ERROR,
                    (errcode(ERRCODE_SYNTAX_ERROR), errmsg("conflicting or redundant option: \"resource pool\"")));
            }
            drespool = defel;
        } else if (strcmp(defel->defname, "parent") == 0) {
            if (dparent != NULL || dparent_default != NULL) {
                clean_role_password(dpassword);
                ereport(
                    ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("conflicting or redundant option: \"user group\"")));
            }
            dparent = defel;
        } else if (strcmp(defel->defname, "parent_default") == 0) {
            if (dparent_default != NULL || dparent != NULL) {
                clean_role_password(dpassword);
                ereport(ERROR,
                    (errcode(ERRCODE_SYNTAX_ERROR), errmsg("conflicting or redundant option: \"user group default\"")));
            }
            dparent_default = defel;
        } else if (strcmp(defel->defname, "space_limit") == 0) {
            if (dspacelimit != NULL) {
                clean_role_password(dpassword);
                ereport(
                    ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("conflicting or redundant options: \"perm space\"")));
            }
            dspacelimit = defel;
        } else if (strcmp(defel->defname, "temp_space_limit") == 0) {
            if (dtmpspacelimit != NULL) {
                clean_role_password(dpassword);
                ereport(
                    ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("conflictiong or redundant option: \"temp space\"")));
            }
            dtmpspacelimit = defel;
        } else if (strcmp(defel->defname, "spill_space_limit") == 0) {
            if (dspillspacelimit != NULL) {
                clean_role_password(dpassword);
                ereport(ERROR,
                    (errcode(ERRCODE_SYNTAX_ERROR), errmsg("conflictiong or redundant option: \"spill space\"")));
            }
            dspillspacelimit = defel;
        } else if (strcmp(defel->defname, "tablespace") == 0) {
            if (dtablespace != NULL) {
                clean_role_password(dpassword);
                ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("conflicting or redundant options")));
            }
            dtablespace = defel;
        } else if (strcmp(defel->defname, "independent") == 0) {
            if (dindependent != NULL) {
                clean_role_password(dpassword);
                ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("conflicting or redundant options")));
            }
            dindependent = defel;
        } else if (strcmp(defel->defname, "persistence") == 0) {
            if (t_thrd.proc->workingVersionNum >= PERSISTENCE_VERSION_NUM) {
                if (dpersistence != NULL) {
                    clean_role_password(dpassword);
                    ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("conflicting or redundant options")));
                }
                dpersistence = defel;
            } else {
                clean_role_password(dpassword);
                ereport(ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("option \"persistence\" is not supported")));
            }
        } else if (strcmp(defel->defname, "expired") == 0) {
            if (dexpired != NULL) {
                clean_role_password(dpassword);
                ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("conflicting or redundant options")));
            }
            dexpired = defel;
        } else if (strcmp(defel->defname, "profile") == 0) {
            /* not used */
        } else if (strcmp(defel->defname, "pguser") == 0) {
            /*
             * Pguser means nothing now and just like normal user.
             * Keep the grammar here just for forward compatibility.
             */
            if (dpguser != NULL) {
                clean_role_password(dpassword);
                ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("conflicting or redundant options")));
            }
            dpguser = defel;
        } else if (strcmp(defel->defname, "node_group") == 0)
#ifdef ENABLE_MULTIPLE_NODES
        {
            if (dnode_group != NULL) {
                clean_role_password(dpassword);
                ereport(
                    ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("conflicting or redundant option: \"node group\"")));
            }
            dnode_group = defel;
        }
#else
        {
            DISTRIBUTED_FEATURE_NOT_SUPPORTED();
            dnode_group = NULL;
        }
#endif
        else {
            clean_role_password(dpassword);
            ereport(ERROR,
                (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE), errmsg("option \"%s\" not recognized", defel->defname)));
        }
    }

    if (dpassword != NULL && dpassword->arg != NULL) {
        /* Database Security: Support password complexity */
        if (list_head((List*)dpassword->arg)) {
            pwdargs = (A_Const*)linitial((List*)dpassword->arg);
            if (pwdargs != NULL) {
                password = strVal(&pwdargs->val);
            }
        }
    }

    if (dinherit != NULL)
        inherit = intVal(dinherit->arg) != 0;
    if (dcreaterole != NULL)
        createrole = intVal(dcreaterole->arg) != 0;
    if (dcreatedb != NULL)
        createdb = intVal(dcreatedb->arg) != 0;
    if (duseft != NULL)
        useft = intVal(duseft->arg) != 0;
    if (dcanlogin != NULL)
        canlogin = intVal(dcanlogin->arg) != 0;
    if (disreplication != NULL)
        isreplication = intVal(disreplication->arg) != 0;
    /* add audit admin privilege */
    /* Database Security:  Support separation of privilege.*/
    if (disauditadmin != NULL)
        isauditadmin = intVal(disauditadmin->arg) != 0;
    if (dissystemadmin != NULL)
        issystemadmin = intVal(dissystemadmin->arg) != 0;
    if (dismonitoradmin != NULL)
        ismonitoradmin = intVal(dismonitoradmin->arg) != 0;
    if (disoperatoradmin != NULL)
        isoperatoradmin = intVal(disoperatoradmin->arg) != 0;
    if (dispolicyadmin != NULL)
        ispolicyadmin = intVal(dispolicyadmin->arg) != 0;
    if (disvcadmin != NULL) {
        isvcadmin = intVal(disvcadmin->arg) != 0;
#ifdef ENABLE_MULTIPLE_NODES
        if (!isRestoreMode && isvcadmin && dnode_group == NULL) {
            str_reset(password);
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("Can not create vcadmin role without node group.")));
        }
#endif
    }
    if (dconnlimit != NULL) {
        connlimit = intVal(dconnlimit->arg);
        if (connlimit < -1) {
            str_reset(password);
            ereport(
                ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("invalid connection limit: %d", connlimit)));
        }
    }
    if (daddroleto != NULL)
        addroleto = (List*)daddroleto->arg;
    if (drolemembers != NULL)
        rolemembers = (List*)drolemembers->arg;
    if (dadminmembers != NULL)
        adminmembers = (List*)dadminmembers->arg;
    if (dvalidBegin != NULL)
        validBegin = strVal(dvalidBegin->arg);
    if (dvalidUntil != NULL)
        validUntil = strVal(dvalidUntil->arg);
    if (dindependent != NULL)
        isindependent = strVal(dindependent->arg);
    if (dpersistence != NULL)
        ispersistence = strVal(dpersistence->arg);
    if (dexpired != NULL)
        isexpired = intVal(dexpired->arg);

    if (drespool != NULL) {
        char* rp = strVal(drespool->arg); /* name of the resource pool */

        if (rp != NULL) {
            if (strlen(rp) >= NAMEDATALEN) {
                rp[NAMEDATALEN - 1] = '\0';
                ereport(NOTICE,
                    (errmsg("resource pool name is too long, "
                            "it will be trancated to \"%s\"",
                        rp)));
            }

            errno_t rc = strncpy_s(respool, sizeof(respool), rp, sizeof(respool) - 1);
            securec_check(rc, "\0", "\0");

            /* get resource pool oid */
            rpoid = get_resource_pool_oid(respool);

            if (!OidIsValid(rpoid)) {
                str_reset(password);
                ereport(ERROR,
                    (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("Resource Pool \"%s\": object not defined.", respool)));
            }

            if (in_logic_cluster()) {
                if (!isRestoreMode && dnode_group == NULL) {
                    str_reset(password);
                    ereport(ERROR,
                        (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                            errmsg("Can not create role with resource pool (%s) "
                                   "without node group in logic cluster.",
                                rp)));
                }
            }

            if (is_resource_pool_foreign(rpoid)) {
                str_reset(password);
                ereport(ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("Can not create role with resource pool (%s) "
                               "with foreign users option.",
                            rp)));
            }
        }
    }

    if (dparent != NULL) {
        char* parent = strVal(dparent->arg);

        if (parent != NULL) {
            if (strlen(parent) >= NAMEDATALEN) {
                parent[NAMEDATALEN - 1] = '\0';
                ereport(NOTICE,
                    (errmsg("parent user name is too long, "
                            "it will be trancated to \"%s\"",
                        parent)));
            }

            if (strcmp(parent, stmt->role) == 0) {
                str_reset(password);
                ereport(ERROR, (errcode(ERRCODE_INVALID_ROLE_SPECIFICATION), errmsg("parent cannot be itself.")));
            }
            /* get parent oid with parent name */
            parentid = get_role_oid(parent, false);

            if (!OidIsValid(parentid)) {
                str_reset(password);
                ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("Role \"%s\": object not defined.", parent)));
            }
        }
    }
    /* check and parse perm sapce */
    if (dspacelimit != NULL) {
        spacelimit = (int64)parseTableSpaceMaxSize(strVal(dspacelimit->arg), &unLimited, &maxSizeStr);
    }

    /* check and parse temp space */
    if (dtmpspacelimit != NULL) {
        tmpspacelimit = (int64)parseTableSpaceMaxSize(strVal(dtmpspacelimit->arg), &tmpUnlimited, &tmpMaxSizeStr);
    }

    /* check and parse temp space */
    if (dspillspacelimit != NULL) {
        spillspacelimit = (int64)parseTableSpaceMaxSize(strVal(dspillspacelimit->arg), &spillUnlimited, &spillMaxSizeStr);
    }

    if (dnode_group != NULL) {
        if (issystemadmin || ismonitoradmin || isoperatoradmin) {
            str_reset(password);
            ereport(ERROR,
                (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                    errmsg("Can not create logic cluster user with sysadmin, mondmin and opradmin.")));
        }

        nodegroup_id = get_pgxc_groupoid(strVal(dnode_group->arg));
        if (IS_PGXC_COORDINATOR && !OidIsValid(nodegroup_id)) {
            str_reset(password);
            ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_OBJECT),
                    errmsg("Node group \"%s\": node group not existed.", strVal(dnode_group->arg))));
        }
    }

    if (OidIsValid(nodegroup_id)) {
        char group_kind;
        group_kind = get_pgxc_groupkind(nodegroup_id);
        if (group_kind != 'v') {
            str_reset(password);
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("Node group \"%s\" must be logic cluster.", strVal(dnode_group->arg))));
        }
        /* check resource pool node groupid with user groupid */
        if (OidIsValid(rpoid) && rpoid != DEFAULT_POOL_OID) {
            char* result = get_resource_pool_ngname(rpoid);
            if (result && strcmp(strVal(dnode_group->arg), result) != 0) {
                str_reset(password);
                ereport(ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("The resource pool \"%s\" is not in logic cluster \"%s\".",
                            strVal(drespool->arg),
                            strVal(dnode_group->arg))));
            }
        }
    }

    if (IsUnderPostmaster) {
        if (dparent_default != NULL)
            is_default = true;

        CheckUserRelation(roleid, parentid, rpoid, is_default, issystemadmin);

        CheckUserSpaceLimit(
            InvalidOid, parentid, spacelimit, tmpspacelimit, spillspacelimit, is_default, false, false, false);
    }
    /* Check some permissions first */
    /* Only allow the initial user to create a persistence user */
    if (ispersistence && !initialuser()) {
        str_reset(password);
        ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), errmsg("Permission denied.")));
    }

    if (isoperatoradmin) {
        if (!initialuser()) {
            str_reset(password);
            ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), errmsg("Permission denied.")));
        }
    } else if (isreplication) {
        if (!isRelSuperuser()) {
            str_reset(password);
            ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), errmsg("Permission denied.")));
        }
    } else if (isauditadmin && g_instance.attr.attr_security.enablePrivilegesSeparate) {
        /* Forbid createrole holders to create auditadmin when PrivilegesSeparate enabled. */
        if (!isRelSuperuser()) {
            str_reset(password);
            ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), errmsg("Permission denied.")));
        }
    } else if (issystemadmin) {
        if (!isRelSuperuser()) {
            str_reset(password);
            ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), errmsg("Permission denied.")));
        }
    } else {
        if (!have_createrole_privilege()) {
            str_reset(password);
            ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), errmsg("Permission denied to create role.")));
        }
    }
    /* Database Security:  Support separation of privilege. */
    if (g_instance.attr.attr_security.enablePrivilegesSeparate && !issuper) {
        if (createrole && (createdb || isreplication || isauditadmin || issystemadmin)) {
            str_reset(password);
            ereport(ERROR,
                (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                    errmsg("Separation of privileges is used,user can't be created because of too many privileges.")));
        } else if (isauditadmin && (createdb || isreplication || issystemadmin || createrole)) {
            str_reset(password);
            ereport(ERROR,
                (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                    errmsg("Separation of privileges is used,user can't be created because of too many privileges.")));
        } else if (issystemadmin && (isauditadmin || createrole)) {
            str_reset(password);
            ereport(ERROR,
                (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                    errmsg("Separation of privileges is used,user can't be created because of too many privileges.")));
        }
    }

    if (strcmp(stmt->role, "public") == 0 || strcmp(stmt->role, "none") == 0) {
        str_reset(password);
        ereport(ERROR, (errcode(ERRCODE_RESERVED_NAME), errmsg("role name \"%s\" is reserved", stmt->role)));
    }

    /*
     * Check the pg_authid relation to be certain the role doesn't already
     * exist.
     */
    Relation pg_authid_rel = heap_open(AuthIdRelationId, RowExclusiveLock);
    TupleDesc pg_authid_dsc = RelationGetDescr(pg_authid_rel);

    if (OidIsValid(get_role_oid(stmt->role, true))) {
        str_reset(password);
        ereport(ERROR, (errcode(ERRCODE_DUPLICATE_OBJECT), errmsg("role \"%s\" already exists", stmt->role)));
    }

    /* Convert validBegin to internal form */
    if (validBegin != NULL) {
        validBegin_datum = DirectFunctionCall3(
            timestamptz_in, CStringGetDatum(validBegin), ObjectIdGetDatum(InvalidOid), Int32GetDatum(-1));
        validBegin_null = false;
    } else {
        validBegin_datum = (Datum)0;
        validBegin_null = true;
    }

    /* Convert validuntil to internal form */
    if (validUntil != NULL) {
        validUntil_datum = DirectFunctionCall3(
            timestamptz_in, CStringGetDatum(validUntil), ObjectIdGetDatum(InvalidOid), Int32GetDatum(-1));
        validUntil_null = false;
    } else {
        validUntil_datum = (Datum)0;
        validUntil_null = true;
    }

    /* The initiation time of password should less than the expiration time */
    if (!validBegin_null && !validUntil_null) {
        if (DatumGetTimestampTz(validBegin_datum) >= DatumGetTimestampTz(validUntil_datum)) {
            str_reset(password);
            ereport(ERROR,
                (errcode(ERRCODE_DATA_EXCEPTION),
                    errmsg("The expiration time could not be earlier than the starting time.")));
        }
    }

    if (*respool == 0) {
        errno_t rc = strncpy_s(respool, NAMEDATALEN, DEFAULT_POOL_NAME, NAMEDATALEN - 1);
        securec_check_ss(rc, "\0", "\0");
    }
    respool_datum = DirectFunctionCall1(namein, CStringGetDatum(respool));
    respool_null = false;

    /*
     * Call the password checking hook if there is one defined
     * Currently no hook and no use.
     */
    if (check_password_hook && password) {
        /* Database Security:  Support SHA256. */
        int pwd_type = PASSWORD_TYPE_PLAINTEXT;
        if (isMD5(password)) {
            pwd_type = PASSWORD_TYPE_MD5;
        } else if (!isSHA256(password)) {
            pwd_type = PASSWORD_TYPE_SHA256;
        }

        (*check_password_hook)(stmt->role, password, pwd_type, validUntil_datum, validUntil_null);
    }

    /*
     * Build a tuple to insert
     */
    errno_t errorno = memset_s(new_record, sizeof(new_record), 0, sizeof(new_record));
    securec_check(errorno, "\0", "\0");
    errorno = memset_s(new_record_nulls, sizeof(new_record_nulls), false, sizeof(new_record_nulls));
    securec_check(errorno, "\0", "\0");

    new_record[Anum_pg_authid_rolname - 1] = DirectFunctionCall1(namein, CStringGetDatum(stmt->role));

    new_record[Anum_pg_authid_rolsuper - 1] = BoolGetDatum(issuper);
    new_record[Anum_pg_authid_rolinherit - 1] = BoolGetDatum(inherit);
    new_record[Anum_pg_authid_rolcreaterole - 1] = BoolGetDatum(createrole);
    new_record[Anum_pg_authid_rolcreatedb - 1] = BoolGetDatum(createdb);
    /* superuser gets catupdate right by default */
    new_record[Anum_pg_authid_rolcatupdate - 1] = BoolGetDatum(issuper);
    new_record[Anum_pg_authid_rolcanlogin - 1] = BoolGetDatum(canlogin);
    new_record[Anum_pg_authid_rolreplication - 1] = BoolGetDatum(isreplication);
    new_record[Anum_pg_authid_rolauditadmin - 1] = BoolGetDatum(isauditadmin);
    new_record[Anum_pg_authid_rolsystemadmin - 1] = BoolGetDatum(issystemadmin);
    new_record[Anum_pg_authid_rolmonitoradmin - 1] = BoolGetDatum(ismonitoradmin);
    new_record[Anum_pg_authid_roloperatoradmin - 1] = BoolGetDatum(isoperatoradmin);
    new_record[Anum_pg_authid_rolpolicyadmin - 1] = BoolGetDatum(ispolicyadmin);
    new_record[Anum_pg_authid_rolconnlimit - 1] = Int32GetDatum(connlimit);

    /*
     * Create role with independent attribute or persistence attribute
     * Notice: Independent attribute and persistence attribute are designed for normal user,
     * it cannot have management attributes like systemadmin, auditadmin and createrole(securityadmin).
     */
    if (isindependent) {
        /* Check license support independent user or not */
        if (is_feature_disabled(PRIVATE_TABLE) == true) {
            str_reset(password);
            ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("Independent user is not supported.")));
        }

        if (issystemadmin || isauditadmin || createrole || ismonitoradmin || isoperatoradmin) {
            str_reset(password);
            ereport(ERROR,
                (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                    errmsg("Independent user cannot have sysadmin, auditadmin, vcadmin, createrole, "
                        "monadmin, opradmin and persistence attributes.")));
        } else if (ispersistence || isvcadmin) {
            str_reset(password);
            ereport(ERROR,
                (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                    errmsg("Users cannot have independent, vcadmin and persistence attributes at the same time.")));
        } else {
            new_record[Anum_pg_authid_rolkind - 1] = CharGetDatum(ROLKIND_INDEPENDENT);
        }
    } else if (ispersistence) {
        if (isvcadmin || isindependent) {
            str_reset(password);
            ereport(ERROR,
                (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                    errmsg("Users cannot have independent, vcadmin and persistence attributes at the same time.")));
        } else {
            new_record[Anum_pg_authid_rolkind - 1] = CharGetDatum(ROLKIND_PERSISTENCE);
        }
    } else {
        new_record[Anum_pg_authid_rolkind - 1] = CharGetDatum(isvcadmin ? ROLKIND_VCADMIN : ROLKIND_NORMAL);
    }

    if (password != NULL) {
        retval = RAND_priv_bytes((unsigned char*)salt_bytes, (GS_UINT32)SALT_LENGTH);
        if (retval != 1) {
            str_reset(password);
            ereport(ERROR,
                (errcode(ERRCODE_DATA_EXCEPTION), errmsg("Failed to Generate the random number, errcode:%u", retval)));
        }
        sha_bytes_to_hex64((uint8*)salt_bytes, salt_string);
        /* Database Security: Support password complexity */
        if (u_sess->attr.attr_security.Password_policy == DEFAULT_PASSWORD_POLICY &&
            !CheckPasswordComplexity(stmt->role, password, NULL, true)) {
            str_reset(password);
            ereport(ERROR,
                (errcode(ERRCODE_INVALID_PASSWORD),
                    errmsg("The password does not satisfy the complexity requirement")));
        }

        new_record[Anum_pg_authid_rolpassword - 1] =
            calculate_encrypted_password(encrypt_password, password, stmt->role, salt_string);
    } else {
        /* dpassword not null means it's DISABLE gram, allow set null password, otherwise NULL is not allowed. */
        if (dpassword != NULL)
            new_record_nulls[Anum_pg_authid_rolpassword - 1] = true;
        else {
            str_reset(password);
            ereport(ERROR, (errcode(ERRCODE_INVALID_PASSWORD), errmsg("The password could not be NULL.")));
        }
    }

    /* password initiation and expiration information */
    new_record[Anum_pg_authid_rolvalidbegin - 1] = validBegin_datum;
    new_record_nulls[Anum_pg_authid_rolvalidbegin - 1] = validBegin_null;
    new_record[Anum_pg_authid_rolvaliduntil - 1] = validUntil_datum;
    new_record_nulls[Anum_pg_authid_rolvaliduntil - 1] = validUntil_null;

    new_record[Anum_pg_authid_rolrespool - 1] = respool_datum;
    new_record_nulls[Anum_pg_authid_rolrespool - 1] = respool_null;
    new_record[Anum_pg_authid_roluseft - 1] = BoolGetDatum(useft);

    new_record[Anum_pg_authid_rolparentid - 1] = ObjectIdGetDatum(parentid);
    new_record_nulls[Anum_pg_authid_rolparentid - 1] = parentid_null;

    new_record[Anum_pg_authid_rolnodegroup - 1] = ObjectIdGetDatum(nodegroup_id);
    new_record_nulls[Anum_pg_authid_rolnodegroup - 1] = (nodegroup_id == InvalidOid);

    if (dspacelimit != NULL) {
        new_record_nulls[Anum_pg_authid_roltabspace - 1] = unLimited;

        if (!unLimited)
            new_record[Anum_pg_authid_roltabspace - 1] = DirectFunctionCall1(textin, CStringGetDatum(maxSizeStr));
    } else {
        new_record_nulls[Anum_pg_authid_roltabspace - 1] = true;
    }

    if (dtmpspacelimit != NULL) {
        new_record_nulls[Anum_pg_authid_roltempspace - 1] = tmpUnlimited;

        if (!tmpUnlimited)
            new_record[Anum_pg_authid_roltempspace - 1] = DirectFunctionCall1(textin, CStringGetDatum(tmpMaxSizeStr));
    } else {
        new_record_nulls[Anum_pg_authid_roltempspace - 1] = true;
    }

    if (dspillspacelimit != NULL) {
        new_record_nulls[Anum_pg_authid_rolspillspace - 1] = spillUnlimited;

        if (!spillUnlimited)
            new_record[Anum_pg_authid_rolspillspace - 1] =
                DirectFunctionCall1(textin, CStringGetDatum(spillMaxSizeStr));
    } else {
        new_record_nulls[Anum_pg_authid_rolspillspace - 1] = true;
    }

    new_record_nulls[Anum_pg_authid_rolexcpdata - 1] = true;

    HeapTuple tuple = heap_form_tuple(pg_authid_dsc, new_record, new_record_nulls);

    /*
     * pg_largeobject_metadata contains pg_authid.oid's, so we use the
     * binary-upgrade override, if specified.
     */
    if (u_sess->proc_cxt.IsBinaryUpgrade && OidIsValid(u_sess->upg_cxt.binary_upgrade_next_pg_authid_oid)) {
        HeapTupleSetOid(tuple, u_sess->upg_cxt.binary_upgrade_next_pg_authid_oid);
        u_sess->upg_cxt.binary_upgrade_next_pg_authid_oid = InvalidOid;
    }

    /*
     * Insert new record in the pg_authid table
     */
    roleid = simple_heap_insert(pg_authid_rel, tuple);

    /* add dependency of roleid on rpoid, no need add dependency on default_pool */
    if (IsUnderPostmaster) {
        if (OidIsValid(rpoid) && (rpoid != DEFAULT_POOL_OID))
            recordDependencyOnRespool(AuthIdRelationId, roleid, rpoid);

        u_sess->wlm_cxt->wlmcatalog_update_user = true;
    }

    /* Database Security: Support password complexity */
    /* whether the create role satisfied the reuse conditions */
    if (password != NULL) {
        AddAuthHistory(roleid, stmt->role, password, CREATE_PG_AUTH_ROLE, salt_string);
    }

    CatalogUpdateIndexes(pg_authid_rel, tuple);

    /* password is sensitive info, clean it when it's useless. */
    str_reset(password);

    /*
     * Advance command counter so we can see new record; else tests in
     * AddRoleMems may fail.
     */
    if (addroleto != NIL || adminmembers != NIL || rolemembers != NIL)
        CommandCounterIncrement();

    /*
     * Add the new role to the specified existing roles.
     */
    foreach (item, addroleto) {
        char* oldrolename = strVal(lfirst(item));
        Oid oldroleid = get_role_oid(oldrolename, false);

        AddRoleMems(
            oldrolename, oldroleid, list_make1(makeString(stmt->role)), list_make1_oid(roleid), GetUserId(), false);
    }

    /*
     * Add the specified members to this new role. adminmembers get the admin
     * option, rolemembers don't.
     */
    AddRoleMems(stmt->role, roleid, adminmembers, roleNamesToIds(adminmembers), GetUserId(), true);
    AddRoleMems(stmt->role, roleid, rolemembers, roleNamesToIds(rolemembers), GetUserId(), false);

    /* Post creation hook for new role */
    InvokeObjectAccessHook(OAT_POST_CREATE, AuthIdRelationId, roleid, 0, NULL);

    /*
     * Close pg_authid, but keep lock till commit.
     */
    heap_close(pg_authid_rel, NoLock);

    /* make sure later steps can see the role created here */
    CommandCounterIncrement();

    /*
     * simulate A db to create schema named by the user's name for the new user.
     * the role is the same as the user except that the role cannot login database,but
     * we only create the same name schema for user
     */
    if (stmt->stmt_type == ROLESTMT_USER) {
        const char* schema_name = stmt->role;
        Oid owner_uid = 0;
        int saved_secdefcxt = 0;
        Oid saved_uid = 0;

        /* get the current user ID and the SecurityRestrictionContext flags. */
        GetUserIdAndSecContext(&saved_uid, &saved_secdefcxt);

        /* get the schema owner's id */
        owner_uid = get_role_oid(stmt->role, false);

        /* Additional check to protect reserved schema names */
        if (!g_instance.attr.attr_common.allowSystemTableMods && !u_sess->attr.attr_common.IsInplaceUpgrade &&
            IsReservedName(schema_name))
            ereport(ERROR,
                (errcode(ERRCODE_RESERVED_NAME),
                    errmsg("unacceptable user name: fail to create same name schema for user \"%s\"", stmt->role),
                    errdetail("The prefix \"pg_\" is reserved for system schemas.")));

        /*
         * set the new role created as current user
         * so that the shema can be created with the correct ownership
         */
        if (saved_uid != owner_uid)
            SetUserIdAndSecContext(owner_uid, saved_secdefcxt | SECURITY_LOCAL_USERID_CHANGE);

        /* Create the schema's namespace  */
        (void)NamespaceCreate(schema_name, owner_uid, false);

        /* Advance cmd counter to make the namespace visible */
        CommandCounterIncrement();

        /* Reset current user */
        SetUserIdAndSecContext(saved_uid, saved_secdefcxt);
    }

    /* Insert new record in the pg_user_status table */
    pg_user_status_rel = heap_open(UserStatusRelationId, RowExclusiveLock);
    if (RelationIsValid(pg_user_status_rel)) {
        tuple = NULL;
        TupleDesc pg_user_status_dsc = NULL;
        Datum pg_user_status_record[Natts_pg_authid];
        bool pg_user_status_record_nulls[Natts_pg_authid] = {false};
        errorno = EOK;

        errorno = memset_s(pg_user_status_record, sizeof(pg_user_status_record), 0, sizeof(pg_user_status_record));
        securec_check(errorno, "\0", "\0");
        errorno = memset_s(pg_user_status_record_nulls,
            sizeof(pg_user_status_record_nulls),
            false,
            sizeof(pg_user_status_record_nulls));
        securec_check(errorno, "\0", "\0");

        tuple = SearchSysCache1(USERSTATUSROLEID, ObjectIdGetDatum(roleid));
        if (!HeapTupleIsValid(tuple)) {
            const char* currentTime = NULL;
            TimestampTz nowTime = GetCurrentTimestamp();
            HeapTuple new_tuple = NULL;
            pg_user_status_dsc = RelationGetDescr(pg_user_status_rel);

            currentTime = timestamptz_to_str(nowTime);
            pg_user_status_record[Anum_pg_user_status_locktime - 1] = DirectFunctionCall3(
                timestamptz_in, CStringGetDatum(currentTime), ObjectIdGetDatum(InvalidOid), Int32GetDatum(-1));

            pg_user_status_record[Anum_pg_user_status_roloid - 1] = ObjectIdGetDatum(roleid);
            pg_user_status_record[Anum_pg_user_status_failcount - 1] = Int32GetDatum(0);
            pg_user_status_record[Anum_pg_user_status_rolstatus - 1] = Int16GetDatum(UNLOCK_STATUS);
            pg_user_status_record[Anum_pg_user_status_passwordexpired - 1] =
                Int16GetDatum(isexpired ? EXPIRED_STATUS : UNEXPIRED_STATUS);
            new_tuple = heap_form_tuple(pg_user_status_dsc, pg_user_status_record, pg_user_status_record_nulls);
            (void)simple_heap_insert(pg_user_status_rel, new_tuple);
            CatalogUpdateIndexes(pg_user_status_rel, new_tuple);
            tableam_tops_free_tuple(new_tuple);
        } else {
            ReleaseSysCache(tuple);
        }
    } else {
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("the relation pg_user_status is invalid")));
    }

    /* Print prompts after all operations are normal. */
    if (isindependent)
        ereport(WARNING,
            (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                errmsg("Please carefully use independent user as it need more self-management."),
                errhint("Self-management include logical backup, password manage and so on.")));

    /* Close pg_user_status, but keep lock till commit.*/
    heap_close(pg_user_status_rel, NoLock);

    /* make sure later steps can see the role created here */
    CommandCounterIncrement();

    /* add the new user into sql count list */
    (void)LWLockAcquire(WaitCountHashLock, LW_EXCLUSIVE);
    initWaitCount(roleid);
    LWLockRelease(WaitCountHashLock);

    /* add the new user into workload transaction hashtbl */
    (void)LWLockAcquire(InstrWorkloadLock, LW_EXCLUSIVE);
    InitInstrOneUserTransaction(roleid);
    LWLockRelease(InstrWorkloadLock);

    if (OidIsValid(nodegroup_id)) {
        grant_nodegroup_to_role(nodegroup_id, roleid, true);
    }
}

/*
 * check if role is dba
 */
bool RoleIsDba(Oid rolOid)
{
    HeapTuple tup = NULL;
    Datum datum;
    bool isNull = false;
    bool result = false;

    Relation pg_database_rel = heap_open(DatabaseRelationId, AccessShareLock);

    TableScanDesc scan = tableam_scan_begin(pg_database_rel, SnapshotNow, 0, NULL);
    while ((tup = (HeapTuple) tableam_scan_getnexttuple(scan, ForwardScanDirection)) != NULL) {
        datum = heap_getattr(tup, Anum_pg_database_datdba, RelationGetDescr(pg_database_rel), &isNull);
        Assert(!isNull);

        if (DatumGetObjectId(datum) == rolOid) {
            result = true;
            break;
        }
    }
    tableam_scan_end(scan);
    heap_close(pg_database_rel, AccessShareLock);
    return result;
}

/*
 * ALTER ROLE
 *
 * Note: the rolemembers option accepted here is intended to support the
 * backwards-compatible ALTER GROUP syntax.  Although it will work to say
 * "ALTER ROLE role ROLE rolenames", we don't document it.
 */
void AlterRole(AlterRoleStmt* stmt)
{
    Datum new_record[Natts_pg_authid];
    bool new_record_nulls[Natts_pg_authid] = {false};
    bool new_record_repl[Natts_pg_authid] = {false};
    TupleDesc pg_authid_dsc = NULL;
    ListCell* option = NULL;
    char* password = NULL; /* user password */
    char salt_string[SALT_LENGTH * 2 + 1] = {0};
    char salt_bytes[SALT_LENGTH + 1] = {0};
    bool encrypt_password = true;
    int issuper = -1;       /* Make the user a superuser? */
    int inherit = -1;       /* Auto inherit privileges? */
    int createrole = -1;    /* Can this user create roles? */
    int createdb = -1;      /* Can the user create databases? */
    int useft = -1;         /* Can the user use foreign table? */
    int canlogin = -1;      /* Can this user login? */
    int isreplication = -1; /* Is this a replication role? */
    int isauditadmin = -1;  /* Is this a auditadmin role? */
    int issystemadmin = -1; /* Is this a systemadmin role? */
    int ismonitoradmin = -1; /* Is this a monitoradmin role? */
    int isoperatoradmin = -1; /* Is this a operatoradmin role? */
    int ispolicyadmin = -1; /* Is this a policyadmin role? */
    int isvcadmin = -1;
    int isindependent = -1;
    int ispersistence = -1;
    int isexpired = 0;
    int connlimit = -1;      /* maximum connections allowed */
    List* rolemembers = NIL; /* roles to be added/removed */
    char* validBegin = NULL; /* time the login is valid until */
    Datum validBegin_datum;  /* same, as timestamptz Datum */
    bool validBegin_null = false;
    char* validUntil = NULL; /* time the login is valid until */
    Datum validUntil_datum;  /* same, as timestamptz Datum */
    bool validUntil_null = false;
    char respool[NAMEDATALEN] = {0}; /* name of the resource pool */
    Datum respool_datum;             /* same, as resource pool Datum */
    Oid rpoid = InvalidOid;
    bool respool_null = false;
    Oid parentid = InvalidOid; /* parent user id */
    bool parentid_null = false;
    Oid nodegroup_id = InvalidOid;
    int64 spacelimit = 0;
    int64 tmpspacelimit = 0;
    int64 spillspacelimit = 0;
    DefElem* dpassword = NULL;
    DefElem* dinherit = NULL;
    DefElem* dcreaterole = NULL;
    DefElem* dcreatedb = NULL;
    DefElem* duseft = NULL;
    DefElem* dcanlogin = NULL;
    DefElem* disreplication = NULL;
    DefElem* disauditadmin = NULL;
    DefElem* dissystemadmin = NULL;
    DefElem* dismonitoradmin = NULL;
    DefElem* disoperatoradmin = NULL;
    DefElem* dispolicyadmin = NULL;
    DefElem* disvcadmin = NULL;
    DefElem* dconnlimit = NULL;
    DefElem* drolemembers = NULL;
    DefElem* dvalidBegin = NULL;
    DefElem* dvalidUntil = NULL;
    DefElem* drespool = NULL;
    DefElem* dparent = NULL;
    DefElem* dparent_default = NULL;
    DefElem* dspacelimit = NULL;
    DefElem* dtmpspacelimit = NULL;
    DefElem* dspillspacelimit = NULL;
    DefElem* dnode_group = NULL;
    DefElem* dindependent = NULL;
    DefElem* dpersistence = NULL;
    DefElem* dexpired = NULL;
    Oid roleid;
    bool isOnlyAlterPassword = false;
    bool is_default = false;
    bool isSuper = false;
    bool is_monadmin = false;
    bool is_opradmin = false;
    bool isNull = false;

    char* oldPasswd = NULL; /* get from pg_authid */
    Datum authidPasswdDatum;
    bool authidPasswdIsNull = false;
    char* replPasswd = NULL; /* get from args */

    bool unLimited = false;
    bool tmpUnLimited = false;
    bool spillUnLimited = false;
    char* maxSizeStr = NULL;
    char* tmpMaxSizeStr = NULL;
    char* spillMaxSizeStr = NULL;

    A_Const* pwdargs = NULL;
    ListCell* head = NULL;

    USER_STATUS rolestatus = UNLOCK_STATUS;

    /* Extract options from the statement node tree */
    foreach (option, stmt->options) {
        DefElem* defel = (DefElem*)lfirst(option);

        if (strcmp(defel->defname, "password") == 0 || strcmp(defel->defname, "encryptedPassword") == 0 ||
            strcmp(defel->defname, "unencryptedPassword") == 0 || strcmp(defel->defname, "expiredPassword") == 0) {
            if (dpassword != NULL) {
                clean_role_password(dpassword);
                ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("conflicting or redundant options")));
            }
            dpassword = defel;
            if (strcmp(defel->defname, "encryptedPassword") == 0)
                encrypt_password = true;
            else if (strcmp(defel->defname, "unencryptedPassword") == 0) {
                clean_role_password(dpassword);
                ereport(ERROR,
                    (errcode(ERRCODE_INVALID_ROLE_SPECIFICATION),
                        errmsg("Permission denied to create role with option UNENCRYPTED.")));
            } else if (strcmp(defel->defname, "expiredPassword") == 0) {
                isexpired = 1;
            }
        } else if (strcmp(defel->defname, "createrole") == 0) {
            if (dcreaterole != NULL) {
                clean_role_password(dpassword);
                ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("conflicting or redundant options")));
            }
            dcreaterole = defel;
        } else if (strcmp(defel->defname, "inherit") == 0) {
            if (dinherit != NULL) {
                clean_role_password(dpassword);
                ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("conflicting or redundant options")));
            }
            dinherit = defel;
        } else if (strcmp(defel->defname, "useft") == 0) {
            if (duseft != NULL) {
                clean_role_password(dpassword);
                ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("conflicting or redundant options")));
            }
            duseft = defel;
        } else if (strcmp(defel->defname, "createdb") == 0) {
            if (dcreatedb != NULL) {
                clean_role_password(dpassword);
                ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("conflicting or redundant options")));
            }
            dcreatedb = defel;
        } else if (strcmp(defel->defname, "isreplication") == 0) {
            if (disreplication != NULL) {
                clean_role_password(dpassword);
                ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("conflicting or redundant options")));
            }
            disreplication = defel;
        } else if (strcmp(defel->defname, "canlogin") == 0) {
            if (dcanlogin != NULL) {
                clean_role_password(dpassword);
                ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("conflicting or redundant options")));
            }
            dcanlogin = defel;
        } else if (strcmp(defel->defname, "isauditadmin") == 0) {
            /* Database Security:  Support separation of privilege. */
            if (disauditadmin != NULL) {
                clean_role_password(dpassword);
                ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("conflicting or redundant options")));
            }
            disauditadmin = defel;
        } else if (strcmp(defel->defname, "ismonitoradmin") == 0) {
            if (dismonitoradmin != NULL) {
                clean_role_password(dpassword);
                ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("conflicting or redundant options")));
            }
            dismonitoradmin = defel;
        } else if (strcmp(defel->defname, "isoperatoradmin") == 0) {
            if (disoperatoradmin != NULL) {
                clean_role_password(dpassword);
                ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("conflicting or redundant options")));
            }
            disoperatoradmin = defel;
        } else if (strcmp(defel->defname, "ispolicyadmin") == 0) {
            if (dispolicyadmin != NULL) {
                clean_role_password(dpassword);
                ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("conflicting or redundant options")));
            }
            dispolicyadmin = defel;
        } else if (strcmp(defel->defname, "connectionlimit") == 0) {
            if (dconnlimit != NULL) {
                clean_role_password(dpassword);
                ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("conflicting or redundant options")));
            }
            dconnlimit = defel;
        } else if (strcmp(defel->defname, "issystemadmin") == 0) {
            if (dissystemadmin != NULL) {
                clean_role_password(dpassword);
                ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("conflicting or redundant options")));
            }
            dissystemadmin = defel;
        } else if (strcmp(defel->defname, "rolemembers") == 0 && stmt->action != 0) {
            if (drolemembers != NULL) {
                clean_role_password(dpassword);
                ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("conflicting or redundant options")));
            }
            drolemembers = defel;
        } else if (strcmp(defel->defname, "validBegin") == 0) {
            if (dvalidBegin != NULL) {
                clean_role_password(dpassword);
                ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("conflicting or redundant options")));
            }
            dvalidBegin = defel;
        } else if (strcmp(defel->defname, "validUntil") == 0) {
            if (dvalidUntil != NULL) {
                clean_role_password(dpassword);
                ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("conflicting or redundant options")));
            }
            dvalidUntil = defel;
        } else if (strcmp(defel->defname, "respool") == 0) {
            if (drespool != NULL) {
                clean_role_password(dpassword);
                ereport(ERROR,
                    (errcode(ERRCODE_SYNTAX_ERROR), errmsg("conflicting or redundant option: \"resource pool\"")));
            }
            drespool = defel;
        } else if (strcmp(defel->defname, "parent") == 0) {
            if (dparent != NULL || dparent_default != NULL) {
                clean_role_password(dpassword);
                ereport(
                    ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("conflicting or redundant option: \"user group\"")));
            }
            dparent = defel;
        } else if (strcmp(defel->defname, "parent_default") == 0) {
            if (dparent_default != NULL || dparent != NULL) {
                clean_role_password(dpassword);
                ereport(ERROR,
                    (errcode(ERRCODE_SYNTAX_ERROR), errmsg("conflicting or redundant option: \"user group default\"")));
            }
            dparent_default = defel;
        } else if (strcmp(defel->defname, "space_limit") == 0) {
            if (dspacelimit != NULL) {
                clean_role_password(dpassword);
                ereport(
                    ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("conflicting or redundant option: \"perm space\"")));
            }
            dspacelimit = defel;
        } else if (strcmp(defel->defname, "temp_space_limit") == 0) {
            if (dtmpspacelimit != NULL) {
                clean_role_password(dpassword);
                ereport(
                    ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("conflicting or redundant option: \"temp space\"")));
            }
            dtmpspacelimit = defel;
        } else if (strcmp(defel->defname, "spill_space_limit") == 0) {
            if (dspillspacelimit != NULL) {
                clean_role_password(dpassword);
                ereport(
                    ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("conflicting or redundant option: \"spill space\"")));
            }
            dspillspacelimit = defel;
        } else if (strcmp(defel->defname, "independent") == 0) {
            if (dindependent != NULL) {
                clean_role_password(dpassword);
                ereport(
                    ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("conflicting or redundant option: \"independent\"")));
            }
            dindependent = defel;
        } else if (strcmp(defel->defname, "persistence") == 0) {
            if (t_thrd.proc->workingVersionNum >= PERSISTENCE_VERSION_NUM) {
                if (dpersistence != NULL) {
                    clean_role_password(dpassword);
                    ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("conflicting or redundant options")));
                }
                dpersistence = defel;
            } else {
                clean_role_password(dpassword);
                ereport(ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("option \"persistence\" is not supported")));
            }
        } else if (strcmp(defel->defname, "expired") == 0) {
            if (dexpired != NULL) {
                clean_role_password(dpassword);
                ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("conflicting or redundant options")));
            }
            dexpired = defel;
        } else if (strcmp(defel->defname, "isvcadmin") == 0) {
            if (disvcadmin != NULL) {
                clean_role_password(dpassword);
                ereport(
                    ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("conflicting or redundant option: \"isvcadmin\"")));
            }
            disvcadmin = defel;
        } else if (strcmp(defel->defname, "node_group") == 0)
#ifdef ENABLE_MULTIPLE_NODES
        {
            if (dnode_group != NULL) {
                clean_role_password(dpassword);
                ereport(
                    ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("conflicting or redundant option: \"node_group\"")));
            }
            dnode_group = defel;
        }
#else
        {
            DISTRIBUTED_FEATURE_NOT_SUPPORTED();
            dnode_group = NULL;
        }
#endif
        else {
            clean_role_password(dpassword);
            ereport(ERROR,
                (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE), errmsg("option \"%s\" not recognized", defel->defname)));
        }
    }

    if (dpassword != NULL && dpassword->arg != NULL) {
        /* Database Security: Support password complexity */
        head = list_head((List*)dpassword->arg);
        if (head != NULL) {
            /* get password if it is existent */
            pwdargs = (A_Const*)linitial((List*)dpassword->arg);
            if (pwdargs != NULL) {
                password = strVal(&pwdargs->val);
            }
            /* get replPasswd if it is existent */
            if (lnext(head)) {
                pwdargs = (A_Const*)lsecond((List*)dpassword->arg);
                if (pwdargs != NULL) {
                    replPasswd = strVal(&pwdargs->val);
                }
            }
        }
    }
    /* set the lock and unlock flag */
    if (dinherit != NULL)
        inherit = intVal(dinherit->arg);
    if (dcreaterole != NULL)
        createrole = intVal(dcreaterole->arg);
    if (dcreatedb != NULL)
        createdb = intVal(dcreatedb->arg);
    if (duseft != NULL)
        useft = intVal(duseft->arg);
    if (dcanlogin != NULL)
        canlogin = intVal(dcanlogin->arg);
    if (disreplication != NULL)
        isreplication = intVal(disreplication->arg);
    /* Database Security:  Support separation of privilege. */
    if (disauditadmin != NULL)
        isauditadmin = intVal(disauditadmin->arg);
    if (dismonitoradmin != NULL)
        ismonitoradmin = intVal(dismonitoradmin->arg);
    if (disoperatoradmin != NULL)
        isoperatoradmin = intVal(disoperatoradmin->arg);
    if (dispolicyadmin != NULL)
        ispolicyadmin = intVal(dispolicyadmin->arg);
    if (disvcadmin != NULL)
        isvcadmin = intVal(disvcadmin->arg);
    if (dissystemadmin != NULL)
        issystemadmin = intVal(dissystemadmin->arg);
    if (dconnlimit != NULL) {
        connlimit = intVal(dconnlimit->arg);
        if (connlimit < -1) {
            str_reset(password);
            str_reset(replPasswd);
            ereport(
                ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("invalid connection limit: %d", connlimit)));
        }
    }
    if (drolemembers != NULL)
        rolemembers = (List*)drolemembers->arg;

    if (dvalidBegin != NULL)
        validBegin = strVal(dvalidBegin->arg);
    if (dvalidUntil != NULL)
        validUntil = strVal(dvalidUntil->arg);
    if (dindependent != NULL)
        isindependent = intVal(dindependent->arg);
    if (dpersistence != NULL)
        ispersistence = intVal(dpersistence->arg);
    if (dexpired != NULL)
        isexpired = intVal(dexpired->arg);

    if (drespool != NULL) {
        char* rp = strVal(drespool->arg);

        if (rp != NULL) {
            if (strlen(rp) >= NAMEDATALEN) {
                rp[NAMEDATALEN - 1] = '\0';
                ereport(NOTICE,
                    (errmsg("resource pool name is too long, "
                            "it will be trancated to \"%s\"",
                        rp)));
            }

            errno_t rc = strncpy_s(respool, sizeof(respool), rp, sizeof(respool) - 1);
            securec_check(rc, "\0", "\0");

            rpoid = get_resource_pool_oid(respool);

            if (!OidIsValid(rpoid)) {
                str_reset(password);
                str_reset(replPasswd);
                ereport(ERROR,
                    (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("Resource Pool \"%s\": object not defined.", respool)));
            }

            if (is_resource_pool_foreign(rpoid)) {
                str_reset(password);
                str_reset(replPasswd);
                ereport(ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("Can not alter role with resource pool (%s) "
                               "with foreign users option.",
                            rp)));
            }
        }
    }

    if (dparent != NULL) {
        char* parent = strVal(dparent->arg);
        if (parent != NULL) {
            if (strlen(parent) >= NAMEDATALEN) {
                parent[NAMEDATALEN - 1] = '\0';
                ereport(NOTICE,
                    (errmsg("parent user name is too long, "
                            "it will be trancated to \"%s\"",
                        parent)));
            }

            if (strcmp(parent, stmt->role) == 0) {
                str_reset(password);
                str_reset(replPasswd);
                ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), errmsg("parent cannot be itself.")));
            }

            parentid = get_role_oid(parent, false);
        }
    }

    /* set perm space */
    if (dspacelimit != NULL) {
        spacelimit = (int64)parseTableSpaceMaxSize(strVal(dspacelimit->arg), &unLimited, &maxSizeStr);
    }

    /* check and parse temp space */
    if (dtmpspacelimit != NULL) {
        tmpspacelimit = (int64)parseTableSpaceMaxSize(strVal(dtmpspacelimit->arg), &tmpUnLimited, &tmpMaxSizeStr);
    }

    /* check and parse spill space */
    if (dspillspacelimit != NULL) {
        spillspacelimit = (int64)parseTableSpaceMaxSize(strVal(dspillspacelimit->arg), &spillUnLimited, &spillMaxSizeStr);
    }

    /*
     * Open pg_authid with RowExclusiveLock, do not release it until the end of the transaction.
     */
    Relation pg_authid_rel = heap_open(AuthIdRelationId, RowExclusiveLock);

    HeapTuple tuple = SearchSysCache1(AUTHNAME, PointerGetDatum(stmt->role));
    if (!HeapTupleIsValid(tuple)) {
        str_reset(password);
        str_reset(replPasswd);

        if (!have_createrole_privilege())
            ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), errmsg("Permission denied.")));
        else
            ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("role \"%s\" does not exist", stmt->role)));
    }
    roleid = HeapTupleGetOid(tuple);

    /* check before heap_open */
    if (IsUnderPostmaster) {
        if (dparent_default != NULL)
            is_default = true;

        CheckUserRelation(roleid, parentid, rpoid, is_default, issystemadmin);

        bool changed = (dspacelimit != NULL) ? true : false;
        bool tmpchanged = (dtmpspacelimit != NULL) ? true : false;
        bool spillchanged = (dspillspacelimit != NULL) ? true : false;

        CheckUserSpaceLimit(roleid,
            parentid,
            spacelimit,
            tmpspacelimit,
            spillspacelimit,
            is_default,
            changed,
            tmpchanged,
            spillchanged);
    }
    /*
     * Scan the pg_authid relation to be certain the user exists.
     */
    pg_authid_dsc = RelationGetDescr(pg_authid_rel);

    Datum authidrespoolDatum = heap_getattr(tuple, Anum_pg_authid_rolrespool, pg_authid_dsc, &isNull);

    Oid oldrpoid = get_resource_pool_oid(DatumGetPointer(authidrespoolDatum));

    if (!OidIsValid(oldrpoid)) {
        str_reset(password);
        str_reset(replPasswd);
        ereport(ERROR,
            (errcode(ERRCODE_RESOURCE_POOL_ERROR), errmsg("resource pool of role \"%s\" does not exist.", stmt->role)));
    }

    /*
     * Use heap_getattr to read the monadmin and opradmin attributes.
     * Due to the upgrade mechanism, the isNull maybe true.
    */
    Datum datum = heap_getattr(tuple, Anum_pg_authid_rolmonitoradmin, pg_authid_dsc, &isNull);
    if (!isNull) {
        is_monadmin = DatumGetBool(datum);
    } else if (roleid == BOOTSTRAP_SUPERUSERID) {
        is_monadmin = true;
    }
    datum = heap_getattr(tuple, Anum_pg_authid_roloperatoradmin, pg_authid_dsc, &isNull);
    if (!isNull) {
        is_opradmin = DatumGetBool(datum);
    } else if (roleid == BOOTSTRAP_SUPERUSERID) {
        is_opradmin = true;
    }

    /*
     *  For ALTER ROLE  ... NODE GROUP
     *  Check whether we can alter node group of user/role, also we will revoke the privilege of
     *  old node group and grant the privilege of new node group. only for logic cluster mode.
     */
    if (dnode_group != NULL) {
        bool is_installation = false;
        bool is_sysadmin = (((Form_pg_authid)GETSTRUCT(tuple))->rolsystemadmin || issystemadmin >= 0);

        if (is_sysadmin || (is_monadmin || ismonitoradmin >= 0) || (is_opradmin || isoperatoradmin >= 0)) {
            str_reset(password);
            str_reset(replPasswd);
            ereport(ERROR,
                (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                    errmsg("Can not alter sysadmin, monadmin and opradmin attach to logic cluster.")));
        }

        if (RoleIsDba(roleid)) {
            str_reset(password);
            str_reset(replPasswd);
            ereport(ERROR,
                (errcode(ERRCODE_DEPENDENT_OBJECTS_STILL_EXIST),
                    errmsg("role %s is database administrator,can not attach to logic cluster.", stmt->role)));
        }

        if (isRestoreMode || IS_PGXC_DATANODE) {
            nodegroup_id = get_pgxc_groupoid(strVal(dnode_group->arg));
        } else {
            nodegroup_id = switch_logic_cluster(roleid, strVal(dnode_group->arg), &is_installation);
            if (is_installation)
                nodegroup_id = InvalidOid;
        }
        /* check resource pool node groupid with user groupid */
        if (OidIsValid(rpoid) && rpoid != DEFAULT_POOL_OID) {
            char* result = get_resource_pool_ngname(rpoid);
            if (result && strcmp(strVal(dnode_group->arg), result) != 0) {
                str_reset(password);
                str_reset(replPasswd);
                ereport(ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("The resource pool \"%s\" is not in logic cluster \"%s\".",
                            strVal(drespool->arg),
                            strVal(dnode_group->arg))));
            }
        }
    }

    /*
     *  For ALTER ROLE  ... RESOURCE POOL
     *  Check the node group of resource pool, only for logic cluster mode.
     */
    if (in_logic_cluster() && dnode_group == NULL) {
        Oid grpid;
        grpid = get_pgxc_logic_groupoid(roleid);

        /* Don't allow grant sysadmin privilege to logic cluster user except cluster redistributing. */
        if (OidIsValid(grpid) && (issystemadmin > 0 || ismonitoradmin > 0 || isoperatoradmin > 0) && 
            PgxcGroupGetInRedistributionGroup() == NULL) {
            str_reset(password);
            str_reset(replPasswd);
            ereport(ERROR,
                (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                    errmsg("Can not grant sysadmin, monadmin and opradmin privilege to logic cluster user.")));
        }

        if (OidIsValid(rpoid) && rpoid != DEFAULT_POOL_OID) {
            char* gpname = NULL;
            char* tmpname = NULL;

            if (OidIsValid(grpid))
                gpname = get_pgxc_groupname(grpid, NULL);

            tmpname = get_resource_pool_ngname(rpoid);

            if (gpname == NULL || (tmpname && strcmp(gpname, tmpname) != 0)) {
                str_reset(password);
                str_reset(replPasswd);
                ereport(ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg(
                            "Can not assign resource pool \"%s\" to user \"%s\".", strVal(drespool->arg), stmt->role)));
            }

            if (gpname != NULL)
                pfree_ext(gpname);
            if (tmpname != NULL)
                pfree_ext(tmpname);
        }
    }

    /* Database Security: Support lock/unlock account */
    if (stmt->lockstatus != DO_NOTHING) {
        /*
         * Check if the current user has the privilege to lock/unlock
         * the user with the ROLEID 'roleid';
         */
        CheckLockPrivilege(roleid, tuple, is_opradmin);

        if (stmt->lockstatus == LOCK_ROLE) {
            if (t_thrd.postmaster_cxt.HaShmData->current_mode == STANDBY_MODE) {
                UpdateFailCountToHashTable(roleid, 0, true);
            } else {
                TryLockAccount(roleid, 0, true);
            }
        } else {
            if (t_thrd.postmaster_cxt.HaShmData->current_mode == STANDBY_MODE) {
                UnlockAccountToHashTable(roleid, true, false);
            } else {
                TryUnlockAccount(roleid, true, false);
            }
        }

        ReleaseSysCache(tuple);
        heap_close(pg_authid_rel, NoLock);
        str_reset(password);
        str_reset(replPasswd);
        return;
    }

    /* Database Security:  Support separation of privilege.*/
    if (roleid == BOOTSTRAP_SUPERUSERID) {
        if (!(issuper < 0 && inherit < 0 && createrole < 0 && createdb < 0 && canlogin < 0 && isreplication < 0 &&
                isauditadmin < 0 && issystemadmin < 0 && ismonitoradmin < 0 && isoperatoradmin < 0 &&
                ispolicyadmin < 0 && isvcadmin < 0 && useft < 0 && ispersistence < 0 && dconnlimit == NULL &&
                rolemembers == NULL && validBegin == NULL && validUntil == NULL && drespool == NULL &&
                dparent == NULL && dnode_group == NULL && dspacelimit == NULL && dtmpspacelimit == NULL &&
                dspillspacelimit == NULL)) {
            str_reset(password);
            str_reset(replPasswd);
            ereport(ERROR,
                (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                    errmsg("Permission denied to change privilege of the initial account.")));
        }
    }
    if (dpassword != NULL && roleid == BOOTSTRAP_SUPERUSERID && GetUserId() != BOOTSTRAP_SUPERUSERID) {
        str_reset(password);
        str_reset(replPasswd);
        ereport(ERROR,
            (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                errmsg("Permission denied to change password of the initial account.")));
    }

    /* Only alter password operator, but not alter lock/unlock and privileges operator. */
    isOnlyAlterPassword =
        dpassword &&
        (inherit < 0 && createrole < 0 && createdb < 0 && canlogin < 0 && isreplication < 0 && isauditadmin < 0 &&
            issystemadmin < 0 && ismonitoradmin < 0 && isoperatoradmin < 0 && ispolicyadmin < 0 &&
            isvcadmin < 0 && useft < 0 && ispersistence < 0 && dconnlimit == NULL && rolemembers == NULL &&
            validBegin == NULL && validUntil == NULL && !*respool && !OidIsValid(parentid) && !spacelimit &&
            !tmpspacelimit && !spillspacelimit && dnode_group == NULL);

    /*
     * To mess with a superuser you gotta be superuser; else you need
     * createrole, or just want to change your own password
     */
    if (get_rolkind(tuple) == ROLKIND_PERSISTENCE || ispersistence >= 0) {
        if (!(initialuser() || (isOnlyAlterPassword && roleid == GetUserId()))) {
            str_reset(password);
            str_reset(replPasswd);
            ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), errmsg("Permission denied.")));
        }
    } else if (is_opradmin || isoperatoradmin >= 0) {
        if (!(initialuser() || (isOnlyAlterPassword && roleid == GetUserId()))) {
            str_reset(password);
            str_reset(replPasswd);
            ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), errmsg("Permission denied.")));
        }
    }
    
    if (((Form_pg_authid)GETSTRUCT(tuple))->rolsuper || issuper >= 0) {
        if (!isRelSuperuser()) {
            str_reset(password);
            str_reset(replPasswd);
            ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), errmsg("Permission denied.")));
        }
    } else if (((Form_pg_authid)GETSTRUCT(tuple))->rolsystemadmin || issystemadmin >= 0) {
        /*
         * sysadmin user should not change the password of another sysadmin user without
         * offering the old password. This rule is also true even if the later sysadmin user
         * has other attributes.
         */
        if (!(isRelSuperuser() || (isOnlyAlterPassword && roleid == GetUserId())) ||
            (dpassword && ((GetUserId() != BOOTSTRAP_SUPERUSERID) && GetUserId() != roleid) &&
                ((Form_pg_authid)GETSTRUCT(tuple))->rolsystemadmin)) {
            str_reset(password);
            str_reset(replPasswd);
            ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), errmsg("Permission denied.")));
        }
    } else if (((Form_pg_authid)GETSTRUCT(tuple))->rolcreaterole || createrole >= 0) {
        if (!(isRelSuperuser() || (isOnlyAlterPassword && roleid == GetUserId()))) {
            str_reset(password);
            str_reset(replPasswd);
            ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), errmsg("Permission denied.")));
        }
    } else if (((Form_pg_authid)GETSTRUCT(tuple))->rolauditadmin || isauditadmin >= 0) {
        /* Database Security:  Support separation of privilege. */
        CheckAlterAuditadminPrivilege(roleid, isOnlyAlterPassword);
    } else if (((Form_pg_authid)GETSTRUCT(tuple))->rolreplication || isreplication >= 0) {
        if (!isRelSuperuser() && !have_createrole_privilege()) {
            str_reset(password);
            str_reset(replPasswd);
            ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), errmsg("Permission denied.")));
        }
    } else if (!have_createrole_privilege()) {
        if (!(inherit < 0 && createrole < 0 && createdb < 0 && canlogin < 0 && isreplication < 0 && isauditadmin < 0 &&
                issystemadmin < 0 && ismonitoradmin < 0 && isoperatoradmin < 0 && ispolicyadmin < 0 &&
                isvcadmin < 0 && useft < 0 && ispersistence < 0 && dconnlimit == NULL && rolemembers == NULL &&
                validBegin == NULL && validUntil == NULL && !*respool && !OidIsValid(parentid) && dnode_group == NULL &&
                !spacelimit && !tmpspacelimit && !spillspacelimit && !isexpired &&
                /* if not superuser or have createrole privilege, permission of lock and unlock is denied */
                stmt->lockstatus == DO_NOTHING &&
                /* if alter password, it will be handled below */
                roleid == GetUserId()) ||
            (roleid != GetUserId() && dpassword == NULL)) {
            str_reset(password);
            str_reset(replPasswd);
            ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), errmsg("Permission denied.")));
        }
    }

    if (IS_PGXC_COORDINATOR && isvcadmin > 0) {
        Oid group_oid = get_pgxc_logic_groupoid(roleid);
        if (!OidIsValid(group_oid) && !OidIsValid(nodegroup_id)) {
            str_reset(password);
            str_reset(replPasswd);
            ereport(ERROR,
                (errcode(ERRCODE_INVALID_GRANT_OPERATION),
                    (errmsg("Can not alter Role \"%s\" to vcadmin.", stmt->role))));
        }
    }

    /* alter dependency of roleid on resource pool */
    if (*respool) {
        if (strcmp(respool, DEFAULT_POOL_NAME) == 0 && (oldrpoid != DEFAULT_POOL_OID))
            deleteSharedDependencyRecordsFor(AuthIdRelationId, roleid, 0);
        else if (OidIsValid(rpoid) && (rpoid != oldrpoid)) {
            if (oldrpoid == DEFAULT_POOL_OID)
                recordDependencyOnRespool(AuthIdRelationId, roleid, rpoid);
            else
                changeDependencyOnRespool(AuthIdRelationId, roleid, rpoid);
        }
    }

    isSuper = (((Form_pg_authid)GETSTRUCT(tuple))->rolsystemadmin);

    /* Database Security:  Support separation of privilege. */
    if (g_instance.attr.attr_security.enablePrivilegesSeparate && !(((Form_pg_authid)GETSTRUCT(tuple))->rolsuper) &&
        issuper <= 0) {
        if ((createrole > 0 && (createdb > 0 || isreplication > 0 || isauditadmin > 0 || issystemadmin > 0)) ||
            (isauditadmin > 0 && (createdb > 0 || isreplication > 0 || issystemadmin > 0 || createrole > 0)) ||
            (issystemadmin > 0 && (isauditadmin > 0 || createrole > 0))) {
            str_reset(password);
            str_reset(replPasswd);
            ereport(ERROR,
                (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                    errmsg("Separation of privileges is used,user can't be altered because of too many privileges.")));
        } else if ((((Form_pg_authid)GETSTRUCT(tuple))->rolcreaterole) &&
                 (createdb > 0 || isreplication > 0 || isauditadmin > 0 || issystemadmin > 0) && createrole != 0) {
            str_reset(password);
            str_reset(replPasswd);
            ereport(ERROR,
                (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                    errmsg("Separation of privileges is used,user can't be altered because of too many privileges.")));
        } else if ((((Form_pg_authid)GETSTRUCT(tuple))->rolauditadmin) &&
                 (createdb > 0 || isreplication > 0 || createrole > 0 || issystemadmin > 0) && isauditadmin != 0) {
            str_reset(password);
            str_reset(replPasswd);
            ereport(ERROR,
                (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                    errmsg("Separation of privileges is used,user can't be altered because of too many privileges.")));
        } else if ((((Form_pg_authid)GETSTRUCT(tuple))->rolsystemadmin) && (createrole > 0 || isauditadmin > 0) &&
                 issystemadmin != 0) {
            str_reset(password);
            str_reset(replPasswd);
            ereport(ERROR,
                (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                    errmsg("Separation of privileges is used,user can't be altered because of too many privileges.")));
        } else if ((((Form_pg_authid)GETSTRUCT(tuple))->rolcreatedb) && (createrole > 0 || isauditadmin > 0) &&
                 createdb != 0) {
            str_reset(password);
            str_reset(replPasswd);
            ereport(ERROR,
                (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                    errmsg("Separation of privileges is used,user can't be altered because of too many privileges.")));
        } else if ((((Form_pg_authid)GETSTRUCT(tuple))->rolreplication) && (createrole > 0 || isauditadmin > 0) &&
                 isreplication != 0) {
            str_reset(password);
            str_reset(replPasswd);
            ereport(ERROR,
                (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                    errmsg("Separation of privileges is used,user can't be altered because of too many privileges.")));
        }
    }

    /* If locked, try unlock to see whether lock time is over. */
    if (t_thrd.postmaster_cxt.HaShmData->current_mode == STANDBY_MODE) {
        if (UNLOCK_STATUS != GetAccountLockedStatusFromHashTable(roleid)) {
            UnlockAccountToHashTable(roleid, false, false);
            rolestatus = GetAccountLockedStatusFromHashTable(roleid);
        }
    } else {
        if (UNLOCK_STATUS != GetAccountLockedStatus(roleid)) {
            (void)TryUnlockAccount(roleid, false, false);
            rolestatus = GetAccountLockedStatus(roleid);
        }
    }
    if (UNLOCK_STATUS != rolestatus) {
        str_reset(password);
        str_reset(replPasswd);
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_AUTHORIZATION_SPECIFICATION), (errmsg("The account has been locked."))));
    }

    /* Convert validbegin to internal form */
    if (validBegin != NULL) {
        validBegin_datum = DirectFunctionCall3(
            timestamptz_in, CStringGetDatum(validBegin), ObjectIdGetDatum(InvalidOid), Int32GetDatum(-1));
        validBegin_null = false;
    } else {
        /* fetch existing setting in case hook needs it */
        validBegin_datum = SysCacheGetAttr(AUTHNAME, tuple, Anum_pg_authid_rolvalidbegin, &validBegin_null);
    }

    /* Convert validuntil to internal form */
    if (validUntil != NULL) {
        validUntil_datum = DirectFunctionCall3(
            timestamptz_in, CStringGetDatum(validUntil), ObjectIdGetDatum(InvalidOid), Int32GetDatum(-1));
        validUntil_null = false;
    } else {
        /* fetch existing setting in case hook needs it */
        validUntil_datum = SysCacheGetAttr(AUTHNAME, tuple, Anum_pg_authid_rolvaliduntil, &validUntil_null);
    }

    /* The initiation time of password should less than the expiration time */
    if (!validBegin_null && !validUntil_null) {
        if (DatumGetTimestampTz(validBegin_datum) >= DatumGetTimestampTz(validUntil_datum)) {
            str_reset(password);
            str_reset(replPasswd);
            ereport(ERROR,
                (errcode(ERRCODE_DATA_EXCEPTION),
                    errmsg("The expiration time could not be earlier than the starting time.")));
        }
    }

    if (*respool) {
        respool_datum = DirectFunctionCall1(namein, CStringGetDatum(respool));
        respool_null = false;
    } else {
        respool_datum = SysCacheGetAttr(AUTHNAME, tuple, Anum_pg_authid_rolrespool, &respool_null);
    }

    /*
     * Call the password checking hook if there is one defined
     * Currently no hook and no use.
     */
    if (check_password_hook && password) {
        /* Database Security: Support password complexity */
        int pwd_type = PASSWORD_TYPE_PLAINTEXT;
        if (isMD5(password)) {
            pwd_type = PASSWORD_TYPE_MD5;
        } else if (!isSHA256(password)) {
            pwd_type = PASSWORD_TYPE_SHA256;
        }

        (*check_password_hook)(stmt->role, password, pwd_type, validUntil_datum, validUntil_null);
    }

    /*
     * Build an updated tuple, perusing the information just obtained
     */
    errno_t errorno = memset_s(new_record, sizeof(new_record), 0, sizeof(new_record));
    securec_check(errorno, "\0", "\0");
    errorno = memset_s(new_record_nulls, sizeof(new_record_nulls), false, sizeof(new_record_nulls));
    securec_check(errorno, "\0", "\0");
    errorno = memset_s(new_record_repl, sizeof(new_record_repl), false, sizeof(new_record_repl));
    securec_check(errorno, "\0", "\0");

    /*
     * issuper/createrole/catupdate/etc
     *
     * XXX It's rather unclear how to handle catupdate.  It's probably best to
     * keep it equal to the superuser status, otherwise you could end up with
     * a situation where no existing superuser can alter the catalogs,
     * including pg_authid!
     */
    if (issuper >= 0) {
        new_record[Anum_pg_authid_rolsuper - 1] = BoolGetDatum(issuper > 0);
        new_record_repl[Anum_pg_authid_rolsuper - 1] = true;

        new_record[Anum_pg_authid_rolcatupdate - 1] = BoolGetDatum(issuper > 0);
        new_record_repl[Anum_pg_authid_rolcatupdate - 1] = true;
    }

    if (inherit >= 0) {
        new_record[Anum_pg_authid_rolinherit - 1] = BoolGetDatum(inherit > 0);
        new_record_repl[Anum_pg_authid_rolinherit - 1] = true;
    }

    if (createrole >= 0) {
        /* Cannot add createrole(securityadmin) attribute to independent user. */
        if (createrole == 1 && is_role_independent(roleid)) {
            str_reset(password);
            str_reset(replPasswd);
            ereport(ERROR,
                (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                    errmsg("Independent user cannot have sysadmin, auditadmin, vcadmin, createrole, "
                        "monadmin, opradmin and persistence attributes.")));
        } else {
            new_record[Anum_pg_authid_rolcreaterole - 1] = BoolGetDatum(createrole > 0);
            new_record_repl[Anum_pg_authid_rolcreaterole - 1] = true;
        }
    }

    if (createdb >= 0) {
        new_record[Anum_pg_authid_rolcreatedb - 1] = BoolGetDatum(createdb > 0);
        new_record_repl[Anum_pg_authid_rolcreatedb - 1] = true;
    }

    if (canlogin >= 0) {
        new_record[Anum_pg_authid_rolcanlogin - 1] = BoolGetDatum(canlogin > 0);
        new_record_repl[Anum_pg_authid_rolcanlogin - 1] = true;
    }

    if (isreplication >= 0) {
        new_record[Anum_pg_authid_rolreplication - 1] = BoolGetDatum(isreplication > 0);
        new_record_repl[Anum_pg_authid_rolreplication - 1] = true;
    }
    /* Database Security:  Support separation of privilege. */
    if (isauditadmin >= 0) {
        /* Cannot add auditadmin attribute to independent user. */
        if (isauditadmin == 1 && is_role_independent(roleid)) {
            str_reset(password);
            str_reset(replPasswd);
            ereport(ERROR,
                (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                    errmsg("Independent user cannot have sysadmin, auditadmin, vcadmin, createrole, "
                        "monadmin, opradmin and persistence attributes.")));
        } else {
            new_record[Anum_pg_authid_rolauditadmin - 1] = BoolGetDatum(isauditadmin > 0);
            new_record_repl[Anum_pg_authid_rolauditadmin - 1] = true;
        }
    }

    if (issystemadmin >= 0) {
        /* Cannot add systemadmin attribute to independent user. */
        if (issystemadmin == 1 && is_role_independent(roleid)) {
            str_reset(password);
            str_reset(replPasswd);
            ereport(ERROR,
                (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                    errmsg("Independent user cannot have sysadmin, auditadmin, vcadmin, createrole, "
                        "monadmin, opradmin and persistence attributes.")));
        } else {
            new_record[Anum_pg_authid_rolsystemadmin - 1] = BoolGetDatum(issystemadmin > 0);
            new_record_repl[Anum_pg_authid_rolsystemadmin - 1] = true;
        }
    }

    if (ismonitoradmin >= 0) {
        /* Cannot add monitoradmin attribute to independent user. */
        if (ismonitoradmin == 1 && is_role_independent(roleid)) {
            str_reset(password);
            str_reset(replPasswd);
            ereport(ERROR,
                (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                    errmsg("Independent user cannot have sysadmin, auditadmin, vcadmin, createrole, "
                        "monadmin, opradmin and persistence attributes.")));
        } else {
            new_record[Anum_pg_authid_rolmonitoradmin - 1] = BoolGetDatum(ismonitoradmin > 0);
            new_record_repl[Anum_pg_authid_rolmonitoradmin - 1] = true;
        }
    }

    if (isoperatoradmin >= 0) {
        /* Cannot add operatoradmin attribute to independent user. */
        if (isoperatoradmin == 1 && is_role_independent(roleid)) {
            str_reset(password);
            str_reset(replPasswd);
            ereport(ERROR,
                (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                    errmsg("Independent user cannot have sysadmin, auditadmin, vcadmin, createrole, "
                        "monadmin, opradmin and persistence attributes.")));
        } else {
            new_record[Anum_pg_authid_roloperatoradmin - 1] = BoolGetDatum(isoperatoradmin > 0);
            new_record_repl[Anum_pg_authid_roloperatoradmin - 1] = true;
        }
    }

    if (ispolicyadmin >= 0) {
        new_record[Anum_pg_authid_rolpolicyadmin - 1] = BoolGetDatum(ispolicyadmin > 0);
        new_record_repl[Anum_pg_authid_rolpolicyadmin - 1] = true;
    }

    if (isvcadmin >= 0) {
        if (isvcadmin) {
            if (is_role_independent(roleid) || get_rolkind(tuple) == ROLKIND_PERSISTENCE) {
                str_reset(password);
                str_reset(replPasswd);
                ereport(ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("Can not alter independent user or persistence user to logic cluster admin user.")));
            } else {
                new_record[Anum_pg_authid_rolkind - 1] = CharGetDatum(ROLKIND_VCADMIN);
                new_record_repl[Anum_pg_authid_rolkind - 1] = true;
            }
        } else if (get_rolkind(tuple) == ROLKIND_VCADMIN) {
            new_record[Anum_pg_authid_rolkind - 1] = CharGetDatum(ROLKIND_NORMAL);
            new_record_repl[Anum_pg_authid_rolkind - 1] = true;
        }
    }

    if (dconnlimit != NULL) {
        new_record[Anum_pg_authid_rolconnlimit - 1] = Int32GetDatum(connlimit);
        new_record_repl[Anum_pg_authid_rolconnlimit - 1] = true;
    }

    /* Alter user independent and noindependent is strictly controlled. */
    if (isindependent >= 0) {
        /*
         * 1.Cannot add user independent attribute along with systemadmin, auditadmin and createrole(securityadmin).
         * 2.Cannot add user independent attribute to systemadmin, auditadmin and createrole(securityadmin).
         */
        if (isindependent) {
            if (issystemadmin == 1 || isauditadmin == 1 || createrole == 1 || isvcadmin == 1 ||
                ismonitoradmin == 1 || isoperatoradmin == 1 || ispersistence == 1 ||
                ((Form_pg_authid)GETSTRUCT(tuple))->rolsystemadmin ||
                ((Form_pg_authid)GETSTRUCT(tuple))->rolcreaterole ||
                ((Form_pg_authid) GETSTRUCT(tuple))->rolauditadmin ||
                is_monadmin || is_opradmin ||
                get_rolkind(tuple) == ROLKIND_VCADMIN || get_rolkind(tuple) == ROLKIND_PERSISTENCE) {
                str_reset(password);
                str_reset(replPasswd);
                ereport(ERROR,
                    (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                        errmsg("Independent user cannot have sysadmin, auditadmin, vcadmin, createrole, "
                        "monadmin, opradmin and persistence attributes.")));
            } else {
                /* Check license support independent user or not */
                if (is_feature_disabled(PRIVATE_TABLE) == true) {
                    str_reset(password);
                    str_reset(replPasswd);
                    ereport(
                        ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("Independent user is not supported.")));
                }
                new_record[Anum_pg_authid_rolkind - 1] = CharGetDatum(ROLKIND_INDEPENDENT);
                new_record_repl[Anum_pg_authid_rolkind - 1] = true;
            }
        } else {
            /* Only user himself can remove his own independent attribute. */
            if (GetUserId() != roleid) {
                str_reset(password);
                str_reset(replPasswd);
                ereport(ERROR,
                    (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                        errmsg("Only user himself can remove his own independent attribute.")));
            } else if (get_rolkind(tuple) == ROLKIND_INDEPENDENT) {
                new_record[Anum_pg_authid_rolkind - 1] = CharGetDatum(ROLKIND_NORMAL);
                new_record_repl[Anum_pg_authid_rolkind - 1] = true;
            }
        }
    }

    /* Alter user persistence and nopersistence is strictly controlled. */
    if (ispersistence >= 0) {
        if (ispersistence) {
            if (isvcadmin == 1 || isindependent == 1 ||
                get_rolkind(tuple) == ROLKIND_VCADMIN || get_rolkind(tuple) == ROLKIND_INDEPENDENT) {
                str_reset(password);
                str_reset(replPasswd);
                ereport(ERROR,
                    (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                        errmsg("Users cannot have independent, vcadmin and persistence attributes at the same time.")));
            } else {
                new_record[Anum_pg_authid_rolkind - 1] = CharGetDatum(ROLKIND_PERSISTENCE);
                new_record_repl[Anum_pg_authid_rolkind - 1] = true;
            }
        } else if (get_rolkind(tuple) == ROLKIND_PERSISTENCE) {
            new_record[Anum_pg_authid_rolkind - 1] = CharGetDatum(ROLKIND_NORMAL);
            new_record_repl[Anum_pg_authid_rolkind - 1] = true;
        }
    }

    /* Database Security: Support password complexity */
    authidPasswdDatum = heap_getattr(tuple, Anum_pg_authid_rolpassword, pg_authid_dsc, &authidPasswdIsNull);
    if (!(authidPasswdIsNull || (void*)authidPasswdDatum == NULL))
        oldPasswd = TextDatumGetCString(authidPasswdDatum);

    /* password */
    if (password != NULL) {
        GS_UINT32 retval = 0;
        if ((isRelSuperuser() || isSecurityadmin(GetUserId())) && GetUserId() != roleid &&
            is_role_independent(roleid)) {
            str_reset(password);
            str_reset(replPasswd);
            str_reset(oldPasswd);
            ereport(ERROR,
                (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                    errmsg("Only independent user himself can alter his own password.")));
        }

        /* if not superuser or have createrole privilege, we must check the oldPasswd and replPasswd */
        if (!(isRelSuperuser() || have_createrole_privilege()) || GetUserId() == roleid) {
            /* if rolepassword is seted in pg_authid, replPasswd must be checked. */
            if (oldPasswd != NULL) {
                if (replPasswd ==  NULL) {
                    str_reset(password);
                    str_reset(replPasswd);
                    str_reset(oldPasswd);
                    ereport(ERROR,
                        (errcode(ERRCODE_INVALID_PASSWORD),
                            errmsg("The old password can not be NULL, please input your old password with 'replace' "
                                   "grammar.")));
                } else {
                    /* Database Security: Support lock/unlock account */
                    if (!IsPasswdEqual(stmt->role, replPasswd, oldPasswd)) {
                        /* the password is not right, and try to lock the account */
                        if (u_sess->attr.attr_security.Password_lock_time > 0 &&
                            u_sess->attr.attr_security.Failed_login_attempts > 0) {
                            if (t_thrd.postmaster_cxt.HaShmData->current_mode == STANDBY_MODE) {
                                UpdateFailCountToHashTable(roleid, 1, false);
                            } else {
                                TryLockAccount(roleid, 1, false);
                            }
                        }
                        str_reset(password);
                        str_reset(replPasswd);
                        str_reset(oldPasswd);
                        ereport(ERROR, (errcode(ERRCODE_INVALID_PASSWORD), errmsg("The old password is invalid.")));
                    } else {
                        if (t_thrd.postmaster_cxt.HaShmData->current_mode == STANDBY_MODE) {
                            UnlockAccountToHashTable(roleid, false, true);
                        } else {
                            TryUnlockAccount(roleid, false, true);
                        }
                    }
                }
            } else {
                /* if rolepassword is not seted in pg_authid, replPasswd should not be specified */
                /*
                 * 1. only initial user and system admin can enable other user's password, but except independent
                 * user's.
                 * 2. independent's user can enable his own password.
                 */
                if (!isRelSuperuser()) {
                    if (!(GetUserId() == roleid && is_role_independent(roleid))) {
                        str_reset(password);
                        str_reset(replPasswd);
                        str_reset(oldPasswd);
                        ereport(ERROR,
                            (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                                errmsg("Only system admin can enable user's password.")));
                    }
                } else {
                    if (is_role_independent(roleid)) {
                        str_reset(password);
                        str_reset(replPasswd);
                        str_reset(oldPasswd);
                        ereport(ERROR,
                            (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                                errmsg("Only independent user himself can enable his own password.")));
                    } else if (!initialuser() && get_rolkind(tuple) == ROLKIND_PERSISTENCE) {
                        str_reset(password);
                        str_reset(replPasswd);
                        str_reset(oldPasswd);
                        ereport(ERROR,
                            (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                                errmsg("Only initial user can enable persistence user's password.")));
                    }
                }
            }
        }

        /* Check the complexity of the password */
        if (!(authidPasswdIsNull || NULL == (void*)authidPasswdDatum)) {
            oldPasswd = TextDatumGetCString(authidPasswdDatum);
        }
        if (DEFAULT_PASSWORD_POLICY == u_sess->attr.attr_security.Password_policy &&
            !CheckPasswordComplexity(stmt->role, password, oldPasswd, false)) {
            str_reset(password);
            str_reset(replPasswd);
            str_reset(oldPasswd);
            ereport(ERROR,
                (errcode(ERRCODE_INVALID_PASSWORD),
                    errmsg("The password does not satisfy the complexity requirement")));
        }
        retval = RAND_priv_bytes((unsigned char*)salt_bytes, (GS_UINT32)SALT_LENGTH);
        if (retval != 1) {
            str_reset(password);
            str_reset(replPasswd);
            str_reset(oldPasswd);
            ereport(ERROR,
                (errcode(ERRCODE_DATA_EXCEPTION), errmsg("Failed to Generate the random number, errcode:%u", retval)));
        }
        sha_bytes_to_hex64((uint8*)salt_bytes, salt_string);

        /* whether the alter role satisfied the reuse conditions */
        AddAuthHistory(roleid, stmt->role, password, ALTER_PG_AUTH_ROLE, salt_string);
        if (GetAccountPasswordExpired(roleid) == EXPIRED_STATUS) {
            SetAccountPasswordExpired(roleid, false);
        }

        new_record[Anum_pg_authid_rolpassword - 1] =
            calculate_encrypted_password(encrypt_password, password, stmt->role, salt_string);
        new_record_repl[Anum_pg_authid_rolpassword - 1] = true;
    }

    /* disable the password for only iam authenication. */
    if (dpassword != NULL && dpassword->arg == NULL) {
        if (GetRoleOid(stmt->role) == BOOTSTRAP_SUPERUSERID) {
            str_reset(password);
            str_reset(replPasswd);
            str_reset(oldPasswd);
            ereport(
                ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), errmsg("Can not disable initial user's password.")));
        }

        /*
         * 1. only initial user and system admin can disable other user's password, but except independent user's.
         * 2. independent's user can disable his own password.
         */
        if (!isRelSuperuser()) {
            if (!(GetUserId() == roleid && is_role_independent(roleid))) {
                str_reset(password);
                str_reset(replPasswd);
                str_reset(oldPasswd);
                ereport(ERROR,
                    (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                        errmsg("Only system admin can disable user's password.")));
            }
        } else {
            if (is_role_independent(roleid)) {
                str_reset(password);
                str_reset(replPasswd);
                str_reset(oldPasswd);
                ereport(ERROR,
                    (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                        errmsg("Only independent user himself can disable his own password.")));
            } else if (!initialuser() && get_rolkind(tuple) == ROLKIND_PERSISTENCE) {
                str_reset(password);
                str_reset(replPasswd);
                str_reset(oldPasswd);
                ereport(ERROR,
                    (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                        errmsg("Only initial user can disable persistence user's password.")));
            }
        }
        new_record_repl[Anum_pg_authid_rolpassword - 1] = true;
        new_record_nulls[Anum_pg_authid_rolpassword - 1] = true;
    }

    /* set perm space */
    if (dspacelimit != NULL) {
        new_record_repl[Anum_pg_authid_roltabspace - 1] = true;

        if (unLimited) {
            new_record_nulls[Anum_pg_authid_roltabspace - 1] = true;
        } else {
            new_record[Anum_pg_authid_roltabspace - 1] = DirectFunctionCall1(textin, CStringGetDatum(maxSizeStr));
        }
    } else {
        new_record_repl[Anum_pg_authid_roltabspace - 1] = false;
    }

    /* set temp space */
    if (dtmpspacelimit != NULL) {
        new_record_repl[Anum_pg_authid_roltempspace - 1] = true;

        if (tmpUnLimited) {
            new_record_nulls[Anum_pg_authid_roltempspace - 1] = true;
        } else {
            new_record[Anum_pg_authid_roltempspace - 1] = DirectFunctionCall1(textin, CStringGetDatum(tmpMaxSizeStr));
        }
    } else {
        new_record_repl[Anum_pg_authid_roltempspace - 1] = false;
    }

    /* set spill space */
    if (dspillspacelimit != NULL) {
        new_record_repl[Anum_pg_authid_rolspillspace - 1] = true;

        if (spillUnLimited) {
            new_record_nulls[Anum_pg_authid_rolspillspace - 1] = true;
        } else {
            new_record[Anum_pg_authid_rolspillspace - 1] =
                DirectFunctionCall1(textin, CStringGetDatum(spillMaxSizeStr));
        }
    } else {
        new_record_repl[Anum_pg_authid_rolspillspace - 1] = false;
    }

    new_record_repl[Anum_pg_authid_rolexcpdata - 1] = false;

    if (issystemadmin >= 0) {
        isSuper = issystemadmin > 0;
    }

    if (IsUnderPostmaster)
        u_sess->wlm_cxt->wlmcatalog_update_user = true;

    /* valid begin */
    new_record[Anum_pg_authid_rolvalidbegin - 1] = validBegin_datum;
    new_record_nulls[Anum_pg_authid_rolvalidbegin - 1] = validBegin_null;
    new_record_repl[Anum_pg_authid_rolvalidbegin - 1] = true;

    /* valid until */
    new_record[Anum_pg_authid_rolvaliduntil - 1] = validUntil_datum;
    new_record_nulls[Anum_pg_authid_rolvaliduntil - 1] = validUntil_null;
    new_record_repl[Anum_pg_authid_rolvaliduntil - 1] = true;

    /* resource pool */
    new_record[Anum_pg_authid_rolrespool - 1] = respool_datum;
    new_record_nulls[Anum_pg_authid_rolrespool - 1] = respool_null;
    if (*respool)
        new_record_repl[Anum_pg_authid_rolrespool - 1] = true;
    else
        new_record_repl[Anum_pg_authid_rolrespool - 1] = false;

    new_record[Anum_pg_authid_rolparentid - 1] = ObjectIdGetDatum(parentid);
    new_record_nulls[Anum_pg_authid_rolparentid - 1] = parentid_null;
    if (dparent_default != NULL || OidIsValid(parentid))
        new_record_repl[Anum_pg_authid_rolparentid - 1] = true;
    else
        new_record_repl[Anum_pg_authid_rolparentid - 1] = false;

    if (useft >= 0) {
        new_record[Anum_pg_authid_roluseft - 1] = BoolGetDatum(useft > 0);
        new_record_repl[Anum_pg_authid_roluseft - 1] = true;
    }

    if (dnode_group != NULL) {
        new_record[Anum_pg_authid_rolnodegroup - 1] = ObjectIdGetDatum(nodegroup_id);
        new_record_repl[Anum_pg_authid_rolnodegroup - 1] = true;
        new_record_nulls[Anum_pg_authid_rolnodegroup - 1] = !OidIsValid(nodegroup_id);
    }

    HeapTuple new_tuple = (HeapTuple) tableam_tops_modify_tuple(tuple, pg_authid_dsc, new_record, new_record_nulls, new_record_repl);
    simple_heap_update(pg_authid_rel, &tuple->t_self, new_tuple);

    /* Update indexes */
    CatalogUpdateIndexes(pg_authid_rel, new_tuple);

    ReleaseSysCache(tuple);
    tableam_tops_free_tuple(new_tuple);

    /* password is sensitive info, clean it when it's useless. */
    str_reset(password);
    str_reset(replPasswd);
    str_reset(oldPasswd);

    /*
     * Advance command counter so we can see new record; else tests in
     * AddRoleMems may fail.
     */
    if (rolemembers || OidIsValid(nodegroup_id))
        CommandCounterIncrement();

    if (OidIsValid(nodegroup_id)) {
        grant_nodegroup_to_role(nodegroup_id, roleid, true);
    }

    if (stmt->action == +1) /* add members to role */
        AddRoleMems(stmt->role, roleid, rolemembers, roleNamesToIds(rolemembers), GetUserId(), false);
    else if (stmt->action == -1) /* drop members from role */
        DelRoleMems(stmt->role, roleid, rolemembers, roleNamesToIds(rolemembers), false);

    /* Print prompts after all operations are normal. */
    if (isindependent == 1)
        ereport(WARNING,
            (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                errmsg("Please carefully use independent user as it need more self-management."),
                errhint("Self-management include logical backup, password manage and so on.")));

    /*
     * Close pg_authid, but keep lock till commit.
     */
    heap_close(pg_authid_rel, NoLock);

    if (isexpired && GetAccountPasswordExpired(roleid) != EXPIRED_STATUS) {
        SetAccountPasswordExpired(roleid, true);
    }
}

/*
 * ALTER ROLE ... SET
 */
void AlterRoleSet(AlterRoleSetStmt* stmt)
{
    Oid databaseid = InvalidOid;
    Oid roleid;
    Relation pg_authid_rel = NULL;
    TupleDesc pg_authid_dsc = NULL;
    bool is_opradmin = false;
    bool isNull = false;

    pg_authid_rel = heap_open(AuthIdRelationId, RowExclusiveLock);
    pg_authid_dsc = RelationGetDescr(pg_authid_rel);

    HeapTuple roletuple = SearchSysCache1(AUTHNAME, PointerGetDatum(stmt->role));

    if (!HeapTupleIsValid(roletuple))
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("role \"%s\" does not exist", stmt->role)));

    /*
     * Obtain a lock on the role and make sure it didn't go away in the
     * meantime.
     */
    shdepLockAndCheckObject(AuthIdRelationId, HeapTupleGetOid(roletuple));

    /*
     * To mess with a superuser you gotta be superuser; else you need
     * createrole, or just want to change your own settings
     */
    roleid = HeapTupleGetOid(roletuple);

    Datum datum = heap_getattr(roletuple, Anum_pg_authid_roloperatoradmin, pg_authid_dsc, &isNull);
    if (!isNull) {
        is_opradmin = DatumGetBool(datum);
    } else if (roleid == BOOTSTRAP_SUPERUSERID) {
        is_opradmin = true;
    }

    /* To mess with a persistence user you gotta be the initial user */
    if (((Form_pg_authid)GETSTRUCT(roletuple))->rolsuper) {
        if (BOOTSTRAP_SUPERUSERID != GetUserId())
            ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), errmsg("Permission denied.")));
    } else if (is_opradmin || get_rolkind(roletuple) == ROLKIND_PERSISTENCE) {
        if (!(GetUserId() == BOOTSTRAP_SUPERUSERID || roleid == GetUserId()))
            ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), errmsg("Permission denied.")));
    } else if (((Form_pg_authid)GETSTRUCT(roletuple))->rolsystemadmin ||
        ((Form_pg_authid)GETSTRUCT(roletuple))->rolcreaterole) {
        if (!(isRelSuperuser() || roleid == GetUserId()))
            ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), errmsg("Permission denied.")));
    } else {
        if (!have_createrole_privilege() && roleid != GetUserId())
            ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), errmsg("Permission denied.")));
    }

    /* look up and lock the database, if specified */
    if (stmt->database != NULL) {
        databaseid = get_database_oid(stmt->database, false);
        shdepLockAndCheckObject(DatabaseRelationId, databaseid);
    }

    AlterSetting(databaseid, HeapTupleGetOid(roletuple), stmt->setstmt);
#ifdef ENABLE_MULTIPLE_NODES
    printHintInfo(stmt->database, stmt->role);
#endif
    heap_close(pg_authid_rel, NoLock);
    ReleaseSysCache(roletuple);
}

/*
 * DROP ROLE
 */
void DropRole(DropRoleStmt* stmt)
{
    Relation pg_authid_rel = NULL;
    Relation pg_auth_members_rel = NULL;
    ListCell* item = NULL;
    DropStmt* n = makeNode(DropStmt);
    DropOwnedStmt drop_objectstmt;
    List* droplist = NULL;

    errno_t rc = memset_s(&drop_objectstmt, sizeof(DropOwnedStmt), 0, sizeof(DropOwnedStmt));
    securec_check(rc, "\0", "\0");
    if (!have_createrole_privilege())
        ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), errmsg("Permission denied to drop role.")));

    /*
     * deny to drop the role when one of the role in the list has the same name
     * as the current  schema
     */
    if ((DROP_CASCADE == stmt->behavior) && IsCurrentSchemaAttachRoles(stmt->roles))
        ereport(ERROR, (errcode(ERRCODE_OBJECT_IN_USE), errmsg("fail to drop the current schema")));

    /*
     * Scan the pg_authid relation to find the Oid of the role(s) to be
     * deleted.
     */
    if (stmt->inherit_from_parent)
        pg_authid_rel = heap_open(AuthIdRelationId, NoLock);
    else
        pg_authid_rel = heap_open(AuthIdRelationId, RowExclusiveLock);
    TupleDesc pg_authid_dsc = RelationGetDescr(pg_authid_rel);

    if (stmt->inherit_from_parent)
        pg_auth_members_rel = heap_open(AuthMemRelationId, NoLock);
    else
        pg_auth_members_rel = heap_open(AuthMemRelationId, RowExclusiveLock);

    foreach (item, stmt->roles) {
        const char* role = strVal(lfirst(item));
        ScanKeyData scankey;
        char* detail = NULL;
        char* detail_log = NULL;
        Oid roleid;
        bool isNull = false;
        bool is_opradmin = false;
        Oid nodegroup_id;

        HeapTuple tuple = SearchSysCache1(AUTHNAME, PointerGetDatum(role));
        if (!HeapTupleIsValid(tuple)) {
            if (!stmt->missing_ok) {
                ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("role \"%s\" does not exist", role)));
            } else {
                ereport(NOTICE, (errmsg("role \"%s\" does not exist, skipping", role)));
            }

            continue;
        }

        Datum authidrespoolDatum = heap_getattr(tuple, Anum_pg_authid_rolrespool, pg_authid_dsc, &isNull);

        Oid rpoid = get_resource_pool_oid(DatumGetPointer(authidrespoolDatum));

        if (!OidIsValid(rpoid))
            ereport(ERROR,
                (errcode(ERRCODE_RESOURCE_POOL_ERROR), errmsg("resource pool of role \"%s\" does not exsist.", role)));

        roleid = HeapTupleGetOid(tuple);

        if (roleid == GetUserId())
            ereport(ERROR, (errcode(ERRCODE_OBJECT_IN_USE), errmsg("current user cannot be dropped")));
        if (roleid == GetOuterUserId())
            ereport(ERROR, (errcode(ERRCODE_OBJECT_IN_USE), errmsg("current user cannot be dropped")));
        if (roleid == GetSessionUserId())
            ereport(ERROR, (errcode(ERRCODE_OBJECT_IN_USE), errmsg("session user cannot be dropped")));

        /* Only allow initial user to drop a persistence users */
        if (get_rolkind(tuple) == ROLKIND_PERSISTENCE && !initialuser()) {
            ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), errmsg("Permission denied.")));
        }

        /*
         * For safety's sake, we allow createrole holders to drop ordinary
         * roles but not superuser roles.  This is mainly to avoid the
         * scenario where you accidentally drop the last superuser.
         */
        /* Database Security:  Support separation of privilege. */
        if ((((Form_pg_authid)GETSTRUCT(tuple))->rolsuper || ((Form_pg_authid)GETSTRUCT(tuple))->rolsystemadmin) &&
            !isRelSuperuser())
            ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), errmsg("Permission denied.")));

        Datum datum = heap_getattr(tuple, Anum_pg_authid_roloperatoradmin, pg_authid_dsc, &isNull);
        if (!isNull) {
            is_opradmin = DatumGetBool(datum);
        } else if (roleid == BOOTSTRAP_SUPERUSERID) {
            is_opradmin = true;
        }
        if (is_opradmin && !initialuser()) {
            ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), errmsg("Permission denied.")));
        }

        /* Forbid createrole holders to drop auditadmin when PrivilegesSeparate enabled. */
        if ((((Form_pg_authid)GETSTRUCT(tuple))->rolauditadmin) &&
            g_instance.attr.attr_security.enablePrivilegesSeparate && !isRelSuperuser())
            ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), errmsg("Permission denied.")));

        /* DROP hook for the role being removed */
        if (object_access_hook) {
            ObjectAccessDrop drop_arg;

            rc = memset_s(&drop_arg, sizeof(ObjectAccessDrop), 0, sizeof(ObjectAccessDrop));
            securec_check(rc, "\0", "\0");
            InvokeObjectAccessHook(OAT_DROP, AuthIdRelationId, roleid, 0, &drop_arg);
        }

        /*
         * Lock the role, so nobody can add dependencies to her while we drop
         * her.  We keep the lock until the end of transaction.
         */
        LockSharedObject(AuthIdRelationId, roleid, 0, AccessExclusiveLock);

        /*
         * If the role is attached to logic cluster, the privilege should be revoked first.
         * Nodegroup_id is invalid for datanode, so grant_nodegroup_to_role is not
         * executed in datanode.
         */
        nodegroup_id = get_pgxc_logic_groupoid(roleid);
        if (stmt->behavior != DROP_CASCADE && OidIsValid(nodegroup_id)) {
            grant_nodegroup_to_role(nodegroup_id, roleid, false);
        }

        /*
         * Simulate A db to drop user user_name drop the schema that has the same name
         * as its owner which is going to be droped.
         *
         * NOTICE : When security admin create user/role, it may have't the privilege of the
         * role and schema created by itself, so we need check whether it's security admin here
         * to permit it's drop operator.
         */
        if (stmt->behavior != DROP_CASCADE && stmt->is_user &&
            GetUserIdFromNspId(get_namespace_oid(role, TRUE), isSecurityadmin(GetUserId()))) {
            n->removeType = OBJECT_SCHEMA;
            n->missing_ok = FALSE;
            n->objects = list_make1(list_make1(makeString((char*)role)));
            n->arguments = NIL;
            n->behavior = DROP_RESTRICT;

            RemoveObjects(n, true, isSecurityadmin(GetUserId()));
        }

        /*
         * Drop the objects owned by the role if its behavior mod is CASCADE
         */
        if (stmt->behavior == DROP_CASCADE) {
            char* user = NULL;

            CancelQuery(role);
            user = (char*)palloc(sizeof(char) * strlen(role) + 1);
            errno_t errorno = strncpy_s(user, strlen(role) + 1, role, strlen(role));
            securec_check(errorno, "\0", "\0");
            drop_objectstmt.behavior = stmt->behavior;
            drop_objectstmt.type = T_DropOwnedStmt;
            drop_objectstmt.roles = list_make1(makeString(user));

            DropOwnedObjects(&drop_objectstmt);
            list_free_deep(drop_objectstmt.roles);
        }

        /* Check for pg_shdepend entries depending on this role */
        if (checkSharedDependencies(AuthIdRelationId, roleid, &detail, &detail_log))
            ereport(ERROR,
                (errcode(ERRCODE_DEPENDENT_OBJECTS_STILL_EXIST),
                    errmsg("role \"%s\" cannot be dropped because some objects depend on it", role),
                    errdetail_internal("%s", detail),
                    errdetail_log("%s", detail_log)));

        /* Relate to remove all job belong the user. */
        remove_job_by_oid(NameStr(((Form_pg_authid)GETSTRUCT(tuple))->rolname), UserOid, true);

        if (IsUnderPostmaster) {
            DropRoleStmt stmt_temp;

            rc = memcpy_s(&stmt_temp, sizeof(DropRoleStmt), stmt, sizeof(DropRoleStmt));
            securec_check(rc, "\0", "\0");

            stmt_temp.roles = NULL;

            if (UserGetChildRoles(roleid, &stmt_temp) > 0)
                DropRole(&stmt_temp);
        }

        /*
         * Remove the role from the pg_authid table
         */
        simple_heap_delete(pg_authid_rel, &tuple->t_self);

        ReleaseSysCache(tuple);

        /*
         * Remove role from the pg_auth_members table.	We have to remove all
         * tuples that show it as either a role or a member.
         *
         * XXX what about grantor entries?	Maybe we should do one heap scan.
         */
        ScanKeyInit(&scankey, Anum_pg_auth_members_roleid, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(roleid));

        SysScanDesc sscan = systable_beginscan(pg_auth_members_rel, AuthMemRoleMemIndexId, true, SnapshotNow, 1, &scankey);
        HeapTuple tmp_tuple = NULL;
        while (HeapTupleIsValid(tmp_tuple = systable_getnext(sscan))) {
            simple_heap_delete(pg_auth_members_rel, &tmp_tuple->t_self);
        }

        systable_endscan(sscan);

        ScanKeyInit(&scankey, Anum_pg_auth_members_member, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(roleid));

        sscan = systable_beginscan(pg_auth_members_rel, AuthMemMemRoleIndexId, true, SnapshotNow, 1, &scankey);

        while (HeapTupleIsValid(tmp_tuple = systable_getnext(sscan))) {
            simple_heap_delete(pg_auth_members_rel, &tmp_tuple->t_self);
        }

        systable_endscan(sscan);
        /* Database Security: Support password complexity */
        /* delete the records of roleid in pg_auth_history */
        DropAuthHistory(roleid);
        /* Database Security: Support lock/unlock account */
        DropUserStatus(roleid);

        /*
         * Remove any comments or security labels on this role.
         */
        DeleteSharedComments(roleid, AuthIdRelationId);
        DeleteSharedSecurityLabel(roleid, AuthIdRelationId);

        /*
         * Remove settings for this role.
         */
        DropSetting(InvalidOid, roleid);

        /*
         * Advance command counter so that later iterations of this loop will
         * see the changes already made.  This is essential if, for example,
         * we are trying to drop both a role and one of its direct members ---
         * we'll get an error if we try to delete the linking pg_auth_members
         * tuple twice.  (We do not need a CCI between the two delete loops
         * above, because it's not allowed for a role to directly contain
         * itself.)
         */
        CommandCounterIncrement();
        droplist = lappend_oid(droplist, roleid);

        if (OidIsValid(rpoid) && (rpoid != DEFAULT_POOL_OID))
            deleteSharedDependencyRecordsFor(AuthIdRelationId, roleid, 0);
    }
    /*
     * drop user info in hash table when all the users are dropped in system tables.
     * if user group is specified, its child users will be dropped recursively,
     * hash table will not be updated until all the users are dropped.
     */
    if (IsUnderPostmaster)
        u_sess->wlm_cxt->wlmcatalog_update_user = true;

    /*
     * Now we can clean up; but keep locks until commit.
     */
    heap_close(pg_auth_members_rel, NoLock);
    heap_close(pg_authid_rel, NoLock);
}

/*
 * Rename role
 */
void RenameRole(const char* oldname, const char* newname)
{
    Datum datum;
    bool isnull = false;
    Datum repl_val[Natts_pg_authid];
    bool repl_null[Natts_pg_authid] = {false};
    bool repl_repl[Natts_pg_authid] = {false};
    int i;
    Oid roleid;
    bool is_opradmin = false;

    Relation rel = heap_open(AuthIdRelationId, RowExclusiveLock);
    TupleDesc dsc = RelationGetDescr(rel);

    HeapTuple oldtuple = SearchSysCache1(AUTHNAME, CStringGetDatum(oldname));
    if (!HeapTupleIsValid(oldtuple))
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("role \"%s\" does not exist", oldname)));

    /*
     * XXX Client applications probably store the session user somewhere, so
     * renaming it could cause confusion.  On the other hand, there may not be
     * an actual problem besides a little confusion, so think about this and
     * decide.	Same for SET ROLE ... we don't restrict renaming the current
     * effective userid, though.
     */
    roleid = HeapTupleGetOid(oldtuple);

    if (roleid == BOOTSTRAP_SUPERUSERID)
        ereport(ERROR,
            (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), errmsg("Permission denied to rename the initial account.")));

    if (roleid == GetSessionUserId())
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("session user cannot be renamed")));
    if (roleid == GetOuterUserId())
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("current user cannot be renamed")));

    /* make sure the new name doesn't exist */
    if (SearchSysCacheExists1(AUTHNAME, CStringGetDatum(newname)))
        ereport(ERROR, (errcode(ERRCODE_DUPLICATE_OBJECT), errmsg("role \"%s\" already exists", newname)));

    if (strcmp(newname, "public") == 0 || strcmp(newname, "none") == 0)
        ereport(ERROR, (errcode(ERRCODE_RESERVED_NAME), errmsg("role name \"%s\" is reserved", newname)));

    datum = heap_getattr(oldtuple, Anum_pg_authid_roloperatoradmin, dsc, &isnull);
    if (!isnull) {
        is_opradmin = DatumGetBool(datum);
    } else if (roleid == BOOTSTRAP_SUPERUSERID) {
        is_opradmin = true;
    }
    /*
     * createrole is enough privilege unless you want to mess with a superuser or operatoradmin or persistence user
     */
    if (is_opradmin || get_rolkind(oldtuple) == ROLKIND_PERSISTENCE) {
        if (!initialuser()) {
            ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), errmsg("Permission denied.")));
        }
    }
    if (((Form_pg_authid)GETSTRUCT(oldtuple))->rolsuper) {
        if (!isRelSuperuser())
            ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), errmsg("Permission denied.")));
    } else if (((Form_pg_authid)GETSTRUCT(oldtuple))->rolcreaterole) {
        if (!isRelSuperuser()) {
            ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), errmsg("Permission denied.")));
        }
    } else if (((Form_pg_authid)GETSTRUCT(oldtuple))->rolsystemadmin) {
        /* Database Security:  Support separation of privilege. */
        if (!isRelSuperuser())
            ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), errmsg("Permission denied.")));
    } else {
        if (!have_createrole_privilege())
            ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), errmsg("Permission denied to rename role.")));
    }

    /* OK, construct the modified tuple */
    for (i = 0; i < Natts_pg_authid; i++)
        repl_repl[i] = false;

    repl_repl[Anum_pg_authid_rolname - 1] = true;
    repl_val[Anum_pg_authid_rolname - 1] = DirectFunctionCall1(namein, CStringGetDatum(newname));
    repl_null[Anum_pg_authid_rolname - 1] = false;

    datum = heap_getattr(oldtuple, Anum_pg_authid_rolpassword, dsc, &isnull);

    if (!isnull && isMD5(TextDatumGetCString(datum))) {
        /* MD5 uses the username as salt, so just clear it on a rename */
        repl_repl[Anum_pg_authid_rolpassword - 1] = true;
        repl_null[Anum_pg_authid_rolpassword - 1] = true;

        ereport(NOTICE, (errmsg("MD5 password cleared because of role rename")));
    }

    /*
     * The md5 password contained by COMBINED password will be invalid after rename, because
     * md5 use the usename as the salt for encrypted. For compatible with PG client, whenever
     * renamed the role name should alter the role's password.
     */
    if (!isnull && isCOMBINED(TextDatumGetCString(datum)))
        ereport(WARNING,
            (errmsg("Please alter the role's password after rename role name for compatible with PG client.")));

    HeapTuple newtuple = (HeapTuple) tableam_tops_modify_tuple(oldtuple, dsc, repl_val, repl_null, repl_repl);
    simple_heap_update(rel, &oldtuple->t_self, newtuple);
    CatalogUpdateIndexes(rel, newtuple);
    ReleaseSysCache(oldtuple);

    /*
     * Close pg_authid, but keep lock till commit.
     */
    heap_close(rel, NoLock);
}

/*
 * GrantRoleStmt
 *
 * Grant/Revoke roles to/from roles
 */
void GrantRole(GrantRoleStmt* stmt)
{
    Relation pg_authid_rel;
    Oid grantor;
    List* grantee_ids = NIL;
    ListCell* item = NULL;

    if (stmt->grantor)
        grantor = get_role_oid(stmt->grantor, false);
    else
        grantor = GetUserId();

    grantee_ids = roleNamesToIds(stmt->grantee_roles);

    /* AccessShareLock is enough since we aren't modifying pg_authid */
    pg_authid_rel = heap_open(AuthIdRelationId, AccessShareLock);

    /*
     * Step through all of the granted roles and add/remove entries for the
     * grantees, or, if admin_opt is set, then just add/remove the admin
     * option.
     *
     * Note: Permissions checking is done by AddRoleMems/DelRoleMems
     */
    foreach (item, stmt->granted_roles) {
        AccessPriv* priv = (AccessPriv*)lfirst(item);
        char* rolename = priv->priv_name;
        Oid roleid;

        /* Must reject priv(columns) and ALL PRIVILEGES(columns) */
        if (rolename == NULL || priv->cols != NIL)
            ereport(ERROR,
                (errcode(ERRCODE_INVALID_GRANT_OPERATION),
                    errmsg("column names cannot be included in GRANT/REVOKE ROLE")));

        roleid = get_role_oid(rolename, false);
        if (stmt->is_grant)
            AddRoleMems(rolename, roleid, stmt->grantee_roles, grantee_ids, grantor, stmt->admin_opt);
        else
            DelRoleMems(rolename, roleid, stmt->grantee_roles, grantee_ids, stmt->admin_opt);
    }

    /*
     * Close pg_authid, but keep lock till commit.
     */
    heap_close(pg_authid_rel, NoLock);
}

/*
 * DropOwnedObjects
 *
 * Drop the objects owned by a given list of roles.
 */
void DropOwnedObjects(DropOwnedStmt* stmt)
{
    List* role_ids = roleNamesToIds(stmt->roles);
    ListCell* cell = NULL;

    /* Check privileges */
    foreach (cell, role_ids) {
        Oid roleid = lfirst_oid(cell);

        /*
         * Superuser can drop independent role's objects, but can not rely has_privs_of_role
         * to check its privilege.
         */
        if (!superuser_arg(GetUserId()) && !has_privs_of_role(GetUserId(), roleid) && !isSecurityadmin(GetUserId()))
            ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), errmsg("Permission denied to drop objects.")));
    }

    /* Ok, do it */
    shdepDropOwned(role_ids, stmt->behavior);
}

/*
 * ReassignOwnedObjects
 *
 * Give the objects owned by a given list of roles away to another user.
 */
void ReassignOwnedObjects(ReassignOwnedStmt* stmt)
{
    List* role_ids = roleNamesToIds(stmt->roles);
    ListCell* cell = NULL;
    Oid newrole;

    /* Check privileges */
    foreach (cell, role_ids) {
        Oid roleid = lfirst_oid(cell);

        if (!has_privs_of_role(GetUserId(), roleid))
            ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), errmsg("Permission denied to reassign objects.")));
    }

    /* Must have privileges on the receiving side too */
    newrole = get_role_oid(stmt->newrole, false);

    if (!has_privs_of_role(GetUserId(), newrole))
        ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), errmsg("Permission denied to reassign objects.")));

    /* Ok, do it */
    shdepReassignOwned(role_ids, newrole);
}

/*
 * roleNamesToIds
 *
 * Given a list of role names (as String nodes), generate a list of role OIDs
 * in the same order.
 */
static List* roleNamesToIds(const List* memberNames)
{
    List* result = NIL;
    ListCell* l = NULL;

    foreach (l, memberNames) {
        char* rolename = strVal(lfirst(l));
        Oid roleid = get_role_oid(rolename, false);

        result = lappend_oid(result, roleid);
    }
    return result;
}

/*
 * AddRoleMems -- Add given members to the specified role
 *
 * rolename: name of role to add to (used only for error messages)
 * roleid: OID of role to add to
 * memberNames: list of names of roles to add (used only for error messages)
 * memberIds: OIDs of roles to add
 * grantorId: who is granting the membership
 * admin_opt: granting admin option?
 *
 * Note: caller is responsible for calling auth_file_update_needed().
 */
static void AddRoleMems(
    const char* rolename, Oid roleid, const List* memberNames, List* memberIds, Oid grantorId, bool admin_opt)
{
    Relation pg_authmem_rel;
    TupleDesc pg_authmem_dsc;
    ListCell* nameitem = NULL;
    ListCell* iditem = NULL;

    Assert(list_length(memberNames) == list_length(memberIds));

    /* Skip permission check if nothing to do */
    if (memberIds == NULL)
        return;

    /* Only persistence user himself or initial user can add him to other members. */
    if (is_role_persistence(roleid)) {
        if(roleid != GetUserId() && !initialuser())
            ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), errmsg("Permission denied.")));
    }

    /*
     * Check permissions: must have createrole or admin option on the role to
     * be changed.	To mess with a superuser role, you gotta be superuser.
     */
    if (isOperatoradmin(roleid)) {
        if(roleid != GetUserId() && !initialuser())
            ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), errmsg("Permission denied.")));
    } else if (superuser_arg(roleid)) {
        if (!isRelSuperuser())
            ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), errmsg("Permission denied.")));
    } else if (systemDBA_arg(roleid)) {
        /* Database Security:  Support separation of privilege. */
        if (!is_admin_of_role(grantorId, roleid))
            ereport(ERROR,
                (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), errmsg("must have admin option on role \"%s\"", rolename)));
    } else if (roleid != GetUserId() && is_role_independent(roleid)) {
        /* Only independent user himself can add himself to other members. */
        ereport(ERROR,
            (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                errmsg("Only independent user himself can decide his own membership.")));
    } else {
        if (!have_createrole_privilege() && !is_admin_of_role(grantorId, roleid))
            ereport(ERROR,
                (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), errmsg("must have admin option on role \"%s\"", rolename)));
    }

    /*
     * The role membership grantor of record has little significance at
     * present.  Nonetheless, inasmuch as users might look to it for a crude
     * audit trail, let only superusers impute the grant to a third party.
     *
     * Before lifting this restriction, give the member == role case of
     * is_admin_of_role() a fresh look.  Ensure that the current role cannot
     * use an explicit grantor specification to take advantage of the session
     * user's self-admin right.
     */
    if (grantorId != GetUserId() && !superuser())
        ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), errmsg("must be system admin to set grantor")));

    pg_authmem_rel = heap_open(AuthMemRelationId, RowExclusiveLock);
    pg_authmem_dsc = RelationGetDescr(pg_authmem_rel);

    forboth(nameitem, memberNames, iditem, memberIds)
    {
        const char* membername = strVal(lfirst(nameitem));
        Oid memberid = lfirst_oid(iditem);
        HeapTuple tuple = NULL;
        Datum new_record[Natts_pg_auth_members];
        bool new_record_nulls[Natts_pg_auth_members] = {false};
        bool new_record_repl[Natts_pg_auth_members] = {false};

        /*
         * Refuse creation of membership loops, including the trivial case
         * where a role is made a member of itself.  We do this by checking to
         * see if the target role is already a member of the proposed member
         * role.  We have to ignore possible superuserness, however, else we
         * could never grant membership in a superuser-privileged role.
         */
        if (is_member_of_role_nosuper(roleid, memberid))
            ereport(ERROR,
                (errcode(ERRCODE_INVALID_GRANT_OPERATION),
                    (errmsg("role \"%s\" is a member of role \"%s\"", rolename, membername))));

        /*
         * Check if entry for this role/member already exists; if so, give
         * warning unless we are adding admin option.
         */
        HeapTuple authmem_tuple = SearchSysCache2(AUTHMEMROLEMEM, ObjectIdGetDatum(roleid), ObjectIdGetDatum(memberid));
        if (HeapTupleIsValid(authmem_tuple) &&
            (!admin_opt || ((Form_pg_auth_members)GETSTRUCT(authmem_tuple))->admin_option)) {
            ereport(NOTICE, (errmsg("role \"%s\" is already a member of role \"%s\"", membername, rolename)));
            ReleaseSysCache(authmem_tuple);
            continue;
        }

        /* Build a tuple to insert or update */
        errno_t errorno = memset_s(new_record, sizeof(new_record), 0, sizeof(new_record));
        securec_check(errorno, "\0", "\0");
        errorno = memset_s(new_record_nulls, sizeof(new_record_nulls), false, sizeof(new_record_nulls));
        securec_check(errorno, "\0", "\0");
        errorno = memset_s(new_record_repl, sizeof(new_record_repl), false, sizeof(new_record_repl));
        securec_check(errorno, "\0", "\0");

        new_record[Anum_pg_auth_members_roleid - 1] = ObjectIdGetDatum(roleid);
        new_record[Anum_pg_auth_members_member - 1] = ObjectIdGetDatum(memberid);
        new_record[Anum_pg_auth_members_grantor - 1] = ObjectIdGetDatum(grantorId);
        new_record[Anum_pg_auth_members_admin_option - 1] = BoolGetDatum(admin_opt);

        if (HeapTupleIsValid(authmem_tuple)) {
            new_record_repl[Anum_pg_auth_members_grantor - 1] = true;
            new_record_repl[Anum_pg_auth_members_admin_option - 1] = true;
            tuple = (HeapTuple) tableam_tops_modify_tuple(authmem_tuple, pg_authmem_dsc, new_record, new_record_nulls, new_record_repl);
            simple_heap_update(pg_authmem_rel, &tuple->t_self, tuple);
            CatalogUpdateIndexes(pg_authmem_rel, tuple);
            ReleaseSysCache(authmem_tuple);
        } else {
            tuple = heap_form_tuple(pg_authmem_dsc, new_record, new_record_nulls);
            (void)simple_heap_insert(pg_authmem_rel, tuple);
            CatalogUpdateIndexes(pg_authmem_rel, tuple);
        }

        /* CCI after each change, in case there are duplicates in list */
        CommandCounterIncrement();

        /* Check if grantee role have the same node group id with members. */
        if (in_logic_cluster()) {
            Oid role_nodegroup = get_pgxc_logic_groupoid(memberid);
            Oid member_nodegroup = get_nodegroup_member_of(memberid);
            if (role_nodegroup != member_nodegroup) {
                ereport(ERROR,
                    (errcode(ERRCODE_INVALID_GRANT_OPERATION),
                        (errmsg("Role \"%s\" can not be granted across virtual clusters.", membername))));
            }
        }
    }

    /*
     * Close pg_authmem, but keep lock till commit.
     */
    heap_close(pg_authmem_rel, NoLock);
}

/*
 * DelRoleMems -- Remove given members from the specified role
 *
 * rolename: name of role to del from (used only for error messages)
 * roleid: OID of role to del from
 * memberNames: list of names of roles to del (used only for error messages)
 * memberIds: OIDs of roles to del
 * admin_opt: remove admin option only?
 *
 * Note: caller is responsible for calling auth_file_update_needed().
 */
static void DelRoleMems(const char* rolename, Oid roleid, const List* memberNames, List* memberIds, bool admin_opt)
{
    Relation pg_authmem_rel;
    TupleDesc pg_authmem_dsc;
    ListCell* nameitem = NULL;
    ListCell* iditem = NULL;

    Assert(list_length(memberNames) == list_length(memberIds));

    /* Skip permission check if nothing to do */
    if (memberIds == NULL)
        return;

    /* Only persistence user himself or initial user can delete him from other members */
    if (is_role_persistence(roleid)) {
        if(roleid != GetUserId() && !initialuser())
            ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), errmsg("Permission denied.")));
    }

    /*
     * Check permissions: must have createrole or admin option on the role to
     * be changed.	To mess with a superuser role, you gotta be superuser.
     */
    if (isOperatoradmin(roleid)) {
        if(roleid != GetUserId() && !initialuser())
            ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), errmsg("Permission denied.")));
    } else if (superuser_arg(roleid)) {
        if (!isRelSuperuser())
            ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), errmsg("Permission denied.")));
    } else if (systemDBA_arg(roleid)) {
        /* Database Security:  Support separation of privilege. */
        if (!is_admin_of_role(GetUserId(), roleid))
            ereport(ERROR,
                (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), errmsg("must have admin option on role \"%s\"", rolename)));
    } else {
        if (!have_createrole_privilege() && !is_admin_of_role(GetUserId(), roleid))
            ereport(ERROR,
                (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), errmsg("must have admin option on role \"%s\"", rolename)));
    }

    pg_authmem_rel = heap_open(AuthMemRelationId, RowExclusiveLock);
    pg_authmem_dsc = RelationGetDescr(pg_authmem_rel);

    forboth(nameitem, memberNames, iditem, memberIds)
    {
        const char* membername = strVal(lfirst(nameitem));
        Oid memberid = lfirst_oid(iditem);

        /*
         * Find entry for this role/member
         */
        HeapTuple authmem_tuple = SearchSysCache2(AUTHMEMROLEMEM, ObjectIdGetDatum(roleid), ObjectIdGetDatum(memberid));
        if (!HeapTupleIsValid(authmem_tuple)) {
            ereport(WARNING, (errmsg("role \"%s\" is not a member of role \"%s\"", membername, rolename)));
            continue;
        }

        if (!admin_opt) {
            /* Remove the entry altogether */
            simple_heap_delete(pg_authmem_rel, &authmem_tuple->t_self);
        } else {
            /* Just turn off the admin option */
            Datum new_record[Natts_pg_auth_members];
            bool new_record_nulls[Natts_pg_auth_members] = {false};
            bool new_record_repl[Natts_pg_auth_members] = {false};

            /* Build a tuple to update with */
            errno_t errorno = memset_s(new_record, sizeof(new_record), 0, sizeof(new_record));
            securec_check(errorno, "\0", "\0");
            errorno = memset_s(new_record_nulls, sizeof(new_record_nulls), false, sizeof(new_record_nulls));
            securec_check(errorno, "\0", "\0");
            errorno = memset_s(new_record_repl, sizeof(new_record_repl), false, sizeof(new_record_repl));
            securec_check(errorno, "\0", "\0");

            new_record[Anum_pg_auth_members_admin_option - 1] = BoolGetDatum(false);
            new_record_repl[Anum_pg_auth_members_admin_option - 1] = true;

            HeapTuple tuple = (HeapTuple) tableam_tops_modify_tuple(authmem_tuple, pg_authmem_dsc, new_record, new_record_nulls, new_record_repl);
            simple_heap_update(pg_authmem_rel, &tuple->t_self, tuple);
            CatalogUpdateIndexes(pg_authmem_rel, tuple);
        }

        ReleaseSysCache(authmem_tuple);

        /* CCI after each change, in case there are duplicates in list */
        CommandCounterIncrement();
    }

    /*
     * Close pg_authmem, but keep lock till commit.
     */
    heap_close(pg_authmem_rel, NoLock);
}

/*
 * @@GaussDB@@
 * Brief		: check whether the current_schema's owner is one
 *			: role in the roles list
 * Description	:
 * Notes		:
 */
static bool IsCurrentSchemaAttachRoles(List* roles)
{
    List* search_path = NIL;
    ListCell* item = NULL;
    const char* nspowner = NULL;
    const char* nspname = NULL;
    Oid nspoid = InvalidOid;
    const char* rolname = NULL;

    /* get current_schema's oid */
    search_path = fetch_search_path(false);
    if (search_path == NIL)
        return false;

    nspoid = linitial_oid(search_path);

    /* get current_schema's name */
    nspname = get_namespace_name(nspoid);

    list_free_ext(search_path);

    /* return fasle if there is no schema corresponding to the oid */
    if (nspname == NULL)
        return false;

    /* get current_schema's owner */
    HeapTuple tuple = SearchSysCache1(NAMESPACEOID, ObjectIdGetDatum(nspoid));
    if (HeapTupleIsValid(tuple)) {
        Form_pg_namespace nsptup = (Form_pg_namespace)GETSTRUCT(tuple);
        nspowner = GetUserNameFromId(nsptup->nspowner);
        ReleaseSysCache(tuple);
    } else {
        /* should never happen */
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_SCHEMA), errmsg("schema \"%s\" doesnot exist", nspname)));
    }

    foreach (item, roles) {
        rolname = strVal(lfirst(item));

        if (rolname != NULL && nspowner != NULL && strncmp(rolname, nspowner, NAMEDATALEN) == 0)
            return true;

        rolname = NULL;
    }

    return false;
}

// check weather the lock is on a realtion
static bool IsLockOnRelation(const LockInstanceData* instance)
{
    bool on_relation = false;

    switch ((LockTagType)(instance->locktag.locktag_type)) {
        case LOCKTAG_RELATION:
            on_relation = true;
            break;
        case LOCKTAG_RELATION_EXTEND:
            on_relation = true;
            break;
        case LOCKTAG_PAGE:
            on_relation = true;
            break;
        case LOCKTAG_TUPLE:
            on_relation = true;
            break;
        case LOCKTAG_CSTORE_FREESPACE:
            on_relation = true;
            break;
        default:
            on_relation = false;
            break;
    }

    if (on_relation && (instance->holdMask || instance->waitLockMode)) {
        return true;
    }

    return false;
}

// get the list of  locked info with special condition
//
// the results is the same as sql statement "select distinct(pid) from
// pg_locks where relation in (select relfilenode from pg_namespace,
// pg_class where pg_namespace.oid=pg_class.relnamespace and
// (pg_class.relkind='r'::"char" or pg_class.relkind='S'::"char") and
// pg_namespace.nspname=user_name)"
static List* GetCancelQuery(const char* user_name)
{
    List* query_list = NIL;
    LockInfoBuck* lock_info = NULL;
    Oid rel_oid = InvalidOid;
    Oid db_oid = InvalidOid;
    HeapTuple tuple = NULL;
    LockData* lock_status_data = NULL;
    LockInstanceData* instance = NULL;
    int status_len = 0;
    PredicateLockData* pred_lock_data = NULL;
    PREDICATELOCKTARGETTAG* predTag = NULL;
    SERIALIZABLEXACT* xact = NULL;
    int pred_len = 0;
    int counter = 0;
    Form_pg_class form = NULL;

    /*
     * get regular locks infomation
     */
    lock_status_data = GetLockStatusData();
    if (lock_status_data != NULL)
        status_len = lock_status_data->nelements;

    /*
     * get SIREAD predicate locks infomation
     */
    pred_lock_data = GetPredicateLockStatusData();
    if (pred_lock_data != NULL)
        pred_len = pred_lock_data->nelements;

    Oid nsp_oid = get_namespace_oid(user_name, true);

    if (!OidIsValid(nsp_oid)) {
        return NIL;
    }

    /*
     * deal with regular locks
     */
    for (counter = 0; counter < status_len && lock_status_data; counter++) {
        instance = &(lock_status_data->locks[counter]);

        /*
         * only deal with the lock on relation
         */
        if (IsLockOnRelation(instance)) {
            rel_oid = (Oid)(instance->locktag.locktag_field2);
            db_oid = (Oid)(instance->locktag.locktag_field1);
            tuple = SearchSysCache1(RELOID, ObjectIdGetDatum(rel_oid));

            /*
             * ignore the relation if fail to find the relation in pg_class
             */
            if (!HeapTupleIsValid(tuple))
                continue;

            form = (Form_pg_class)GETSTRUCT(tuple);

            /*
             * Add a record when the following conditions are satisfed:
             * the namespace the relation locates is the current namespace
             * the database the relation located is the current database
             * the relation is an ordinary table or a sequence
             * the oid of the relation is equale to the field 'relfilenode' in pg_class
             * the pid locked the relation is unique for the existing records
             */
            if (IsEligiblePid(rel_oid, nsp_oid, instance->pid, db_oid, form, query_list)) {
                lock_info = (LockInfoBuck*)palloc(sizeof(LockInfoBuck));
                lock_info->pid = instance->pid;
                lock_info->database = db_oid;
                lock_info->relation = rel_oid;
                lock_info->nspoid = form->relnamespace;
                query_list = lappend(query_list, lock_info);
                lock_info = NULL;
            }

            ReleaseSysCache(tuple);
        }
    }

    /*
     * deal with SIREAD predicate locks
     */
    for (counter = 0; counter < pred_len && pred_lock_data; counter++) {
        predTag = &(pred_lock_data->locktags[counter]);
        xact = &(pred_lock_data->xacts[counter]);
        rel_oid = (Oid)(predTag->locktag_field2);
        db_oid = (Oid)(predTag->locktag_field1);
        tuple = SearchSysCache1(RELOID, ObjectIdGetDatum(rel_oid));

        /*
         * ignore the relation if fail to find the relation in pg_class
         */
        if (!HeapTupleIsValid(tuple))
            continue;

        form = (Form_pg_class)GETSTRUCT(tuple);

        /*
         * Add a record when the following conditions are satisfed:
         * the namespace the relation locates is the current namespace
         * the database the relation located is the current database
         * the relation is an ordinary table or a sequence
         * the oid of the relation is equale to the field 'relfilenode' in pg_class
         * the pid locked the relation is unique for the existing records
         */
        if (IsEligiblePid(rel_oid, nsp_oid, xact->pid, db_oid, form, query_list)) {
            lock_info = (LockInfoBuck*)palloc(sizeof(LockInfoBuck));
            lock_info->pid = xact->pid;
            lock_info->database = db_oid;
            lock_info->relation = rel_oid;
            lock_info->nspoid = form->relnamespace;
            query_list = lappend(query_list, lock_info);
            lock_info = NULL;
        }

        ReleaseSysCache(tuple);
    }

    return query_list;
}

// check weather the pid is recorded already
static bool IsDuplicatePid(const List* query_list, ThreadId pid)
{
    LockInfoBuck* lock_info = NULL;
    ListCell* curcell = NULL;

    if (query_list == NULL)
        return false;

    foreach (curcell, query_list) {
        lock_info = (LockInfoBuck*)(lfirst(curcell));

        if (lock_info->pid == pid)
            return true;
    }

    return false;
}

// the pid is effective when the following conditions are satisfed:
//		: the namespace the relation locates is the current namespace
//		: the database the relation located is the current database
//		: the relation is an ordinary table or is a sequence
//		: the oid of the relation is equale to the field 'relfilenode' in pg_class
//		: the pid locked the relation is unique for the existing records
static bool IsEligiblePid(Oid rel_oid, Oid nsp_oid, ThreadId pid, Oid db_oid, const Form_pg_class form, List* query_list)
{
    if (db_oid && pid && rel_oid && (nsp_oid == form->relnamespace) && ('r' == form->relkind || 'S' == form->relkind) &&
        (rel_oid == form->relfilenode) && (u_sess->proc_cxt.MyDatabaseId == db_oid) && !IsDuplicatePid(query_list, pid))
        return true;

    return false;
}

// kill the query that locks the relation we need to drop
static void CancelQuery(const char* user_name)
{
    List* query_list = NIL;
    ListCell* curcell = NULL;
    LockInfoBuck* lock_info = NULL;

    if (!u_sess->attr.attr_sql.enable_kill_query)
        return;

    query_list = GetCancelQuery(user_name);

    if (query_list == NULL)
        return;

    foreach (curcell, query_list) {
        lock_info = (LockInfoBuck*)(lfirst(curcell));

        if (lock_info->pid == t_thrd.proc_cxt.MyProcPid) {
            list_free_deep(query_list);
            ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("cannot cancel current session's query")));
        }

        /*
         * cancel a backend's current query
         */
        cancel_backend(lock_info->pid);
    }

    list_free_deep(query_list);
}

/* Database Security: Support password complexity */
/*
 * Brief			: whether the ch is one of the specifically letters
 * Description		:
 * Notes			:
 */
static bool IsSpecialCharacter(char ch)
{
    const char* spec_letters = "~!@#$%^&*()-_=+\\|[{}];:,<.>/?";
    char* ptr = (char*)spec_letters;
    while (*ptr != '\0') {
        if (*ptr == ch) {
            return true;
        }
        ptr++;
    }
    return false;
}

/*
 * Brief			: whether the passwd1 equal to the passwd2
 * Description		: the compare mainly contains encrypted password
 * Notes			:
 */
static bool IsPasswdEqual(const char* rolename, char* passwd1, char* passwd2)
{
    char encrypted_md5_password[MD5_PASSWD_LEN + 1] = {0};
    char encrypted_sha256_password[SHA256_PASSWD_LEN + 1] = {0};
    char encrypted_combined_password[MD5_PASSWD_LEN + SHA256_PASSWD_LEN + 1] = {0};
    char salt[SALT_LENGTH * 2 + 1] = {0};
    int iteration_count = 0;
    errno_t rc = EOK;

    if (passwd1 == NULL || passwd2 == NULL || strlen(passwd1) == 0 || strlen(passwd2) == 0) {
        return false;
    }

    if (isMD5(passwd2)) {
        if (!pg_md5_encrypt(passwd1, rolename, strlen(rolename), encrypted_md5_password)) {
            /* Var 'encrypted_md5_password' has not been assigned any value if the function return 'false' */
            str_reset(passwd1);
            str_reset(passwd2);
            ereport(ERROR, (errcode(ERRCODE_INVALID_PASSWORD), errmsg("md5-password encryption failed.")));
        }
        if (strncmp(passwd2, encrypted_md5_password, MD5_PASSWD_LEN + 1) == 0) {
            rc = memset_s(encrypted_md5_password, MD5_PASSWD_LEN + 1, 0, MD5_PASSWD_LEN + 1);
            securec_check(rc, "\0", "\0");
            return true;
        }
    } else if (isSHA256(passwd2)) {
        rc = strncpy_s(salt, sizeof(salt), &passwd2[SHA256_LENGTH], sizeof(salt) - 1);
        securec_check(rc, "\0", "\0");
        salt[sizeof(salt) - 1] = '\0';

        iteration_count = get_stored_iteration(rolename);
        if (!pg_sha256_encrypt(passwd1, salt, strlen(salt), encrypted_sha256_password, NULL, iteration_count)) {
            rc = memset_s(encrypted_sha256_password, SHA256_PASSWD_LEN + 1, 0, SHA256_PASSWD_LEN + 1);
            securec_check(rc, "\0", "\0");
            str_reset(passwd1);
            str_reset(passwd2);
            ereport(ERROR, (errcode(ERRCODE_INVALID_PASSWORD), errmsg("sha256-password encryption failed.")));
        }

        if (strncmp(passwd2, encrypted_sha256_password, SHA256_PASSWD_LEN) == 0) {
            rc = memset_s(encrypted_sha256_password, SHA256_PASSWD_LEN + 1, 0, SHA256_PASSWD_LEN + 1);
            securec_check(rc, "\0", "\0");
            return true;
        }
    } else if (isCOMBINED(passwd2)) {
        rc = strncpy_s(salt, sizeof(salt), &passwd2[SHA256_LENGTH], sizeof(salt) - 1);
        securec_check(rc, "\0", "\0");
        salt[sizeof(salt) - 1] = '\0';

        iteration_count = get_stored_iteration(rolename);
        if (!pg_sha256_encrypt(passwd1, salt, strlen(salt), encrypted_sha256_password, NULL, iteration_count)) {
            rc = memset_s(encrypted_sha256_password, SHA256_PASSWD_LEN + 1, 0, SHA256_PASSWD_LEN + 1);
            securec_check(rc, "\0", "\0");
            str_reset(passwd1);
            str_reset(passwd2);
            ereport(ERROR, (errcode(ERRCODE_INVALID_PASSWORD), errmsg("first stage encryption password failed")));
        }

        if (!pg_md5_encrypt(passwd1, rolename, strlen(rolename), encrypted_md5_password)) {
            /* Var 'encrypted_md5_password' has not been assigned any value if the function return 'false' */
            str_reset(passwd1);
            str_reset(passwd2);
            ereport(ERROR, (errcode(ERRCODE_INVALID_PASSWORD), errmsg("second stage encryption password failed")));
        }

        rc = snprintf_s(encrypted_combined_password,
            MD5_PASSWD_LEN + SHA256_PASSWD_LEN + 1,
            MD5_PASSWD_LEN + SHA256_PASSWD_LEN,
            "%s%s",
            encrypted_sha256_password,
            encrypted_md5_password);
        securec_check_ss(rc, "\0", "\0");

        /*
         * When alter user's password:
         * 1. No need to compare the new iteration and old iteration.
         * 2. No need to compare MD5 password, just compare SHA256 is enough.
         */
        if (strncmp(passwd2, encrypted_combined_password, SHA256_PASSWD_LEN) == 0) {
            /* clear the sensitive messages in the stack. */
            rc = memset_s(encrypted_md5_password, MD5_PASSWD_LEN + 1, 0, MD5_PASSWD_LEN + 1);
            securec_check(rc, "\0", "\0");
            rc = memset_s(encrypted_sha256_password, SHA256_PASSWD_LEN + 1, 0, SHA256_PASSWD_LEN + 1);
            securec_check(rc, "\0", "\0");
            rc = memset_s(encrypted_combined_password,
                MD5_PASSWD_LEN + SHA256_PASSWD_LEN + 1,
                0,
                MD5_PASSWD_LEN + SHA256_PASSWD_LEN + 1);
            securec_check(rc, "\0", "\0");
            return true;
        }
    } else {
        if (strcmp(passwd1, passwd2) == 0) {
            return true;
        }
    }

    /* clear sensitive messages in stack. */
    rc = memset_s(encrypted_md5_password, MD5_PASSWD_LEN + 1, 0, MD5_PASSWD_LEN + 1);
    securec_check(rc, "\0", "\0");
    rc = memset_s(encrypted_sha256_password, SHA256_PASSWD_LEN + 1, 0, SHA256_PASSWD_LEN + 1);
    securec_check(rc, "\0", "\0");
    rc = memset_s(
        encrypted_combined_password, MD5_PASSWD_LEN + SHA256_PASSWD_LEN + 1, 0, MD5_PASSWD_LEN + SHA256_PASSWD_LEN + 1);
    securec_check(rc, "\0", "\0");

    return false;
}

static void CalculateTheNumberOfAllTypesOfCharacters(const char* ptr, int *kinds, bool *include_unusual_character)
{
    while (*ptr != '\0') {
        if (*ptr >= 'A' && *ptr <= 'Z') {
            kinds[0]++;
        } else if (*ptr >= 'a' && *ptr <= 'z') {
            kinds[1]++;
        } else if (*ptr >= '0' && *ptr <= '9') {
            kinds[2]++;
        } else if (IsSpecialCharacter(*ptr)) {
            kinds[3]++;
        } else {
            *include_unusual_character = true;
        }

        ptr++;
    }
    
    return;
}

static void IsPasswdSatisfyPolicy(char* Password)
{

    int i = 0;
    const int max_kinds_size = 4;
    int kinds[max_kinds_size] = {0};
    int kinds_num = 0;
    const char* ptr = NULL;
    bool include_unusual_character = false;

    ptr = Password;

    /* Password must contain at least u_sess->attr.attr_security.Password_min_length characters */
    if ((int)strlen(Password) < u_sess->attr.attr_security.Password_min_length) {
        str_reset(Password);
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_PASSWORD),
                errmsg(
                    "Password must contain at least %d characters.", u_sess->attr.attr_security.Password_min_length)));
    }

    /* Password can't contain more than u_sess->attr.attr_security.Password_max_length characters */
    if ((int)strlen(Password) > u_sess->attr.attr_security.Password_max_length) {
        str_reset(Password);
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_PASSWORD),
                errmsg("Password can't contain more than %d characters.",
                    u_sess->attr.attr_security.Password_max_length)));
    }

    /* Calculate the number of all types of characters */
    CalculateTheNumberOfAllTypesOfCharacters(ptr, kinds, &include_unusual_character);

    /* If contain unusual character in password, show the warning. */
    if (include_unusual_character) {
        str_reset(Password);
        ereport(ERROR,
            (errmsg("Password cannot contain characters except numbers, alphabetic characters and "
                    "specified special characters.")));
    }

    /* Calculate the number of character types */
    for (i = 0; i != max_kinds_size; ++i) {
        if (kinds[i] > 0) {
            kinds_num++;
        }
    }

    /* Password must contain at least u_sess->attr.attr_security.Password_min_upper upper characters */
    if (kinds[0] < u_sess->attr.attr_security.Password_min_upper) {
        str_reset(Password);
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_PASSWORD),
                errmsg("Password must contain at least %d upper characters.",
                    u_sess->attr.attr_security.Password_min_upper)));
    }

    /* Password must contain at least u_sess->attr.attr_security.Password_min_lower lower characters */
    if (kinds[1] < u_sess->attr.attr_security.Password_min_lower) {
        str_reset(Password);
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_PASSWORD),
                errmsg("Password must contain at least %d lower characters.",
                    u_sess->attr.attr_security.Password_min_lower)));
    }

    /* Password must contain at least u_sess->attr.attr_security.Password_min_digital digital characters */
    if (kinds[2] < u_sess->attr.attr_security.Password_min_digital) {
        str_reset(Password);
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_PASSWORD),
                errmsg("Password must contain at least %d digital characters.",
                    u_sess->attr.attr_security.Password_min_digital)));
    }

    /* Password must contain at least u_sess->attr.attr_security.Password_min_special special characters */
    if (kinds[3] < u_sess->attr.attr_security.Password_min_special) {
        str_reset(Password);
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_PASSWORD),
                errmsg("Password must contain at least %d special characters.",
                    u_sess->attr.attr_security.Password_min_special)));
    }

    /* Password must contain at least three kinds of characters */
    if (kinds_num < 3) {
        str_reset(Password);
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_PASSWORD), errmsg("Password must contain at least three kinds of characters.")));
    }

    check_weak_password(Password);
}

/*
 * Brief			: Whether the password satisfy the complexity requirement.
 * Description		: The oldPasswd is NULL, means this is called by the CreateRole	operation.
 *				   Not NULL, means AlterRole operation;
 * Notes			:
 */
static bool CheckPasswordComplexity(const char* roleID, char* newPasswd, char* oldPasswd, bool isCreaterole)
{

    char* reverse_str = NULL;

    if (roleID ==  NULL) {
        str_reset(newPasswd);
        str_reset(oldPasswd);
        ereport(ERROR,
            (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("The parameter roleID of CheckPasswordComplexity is NULL")));
    }

    if (newPasswd == NULL) {
        str_reset(newPasswd);
        str_reset(oldPasswd);
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_PASSWORD), errmsg("The parameter newPasswd of CheckPasswordComplexity is NULL")));
    }

    /*
     * Don't check encrypted password complexity(for backup&recovery condition).
     * Just for createrole but not for alter, and alter password is not in dump content.
     */
    if (isCreaterole && isPWDENCRYPTED(newPasswd)) {
        ereport(NOTICE,
            (errcode(ERRCODE_INVALID_PASSWORD),
                errmsg("Using encrypted password directly now and it is not recommended."),
                errhint("The role can't be used if you don't know the original password before encrypted.")));
        return true;
    }

    /* Check whether password satisfy the Policy that user set */
    IsPasswdSatisfyPolicy(newPasswd);

    /* newPasswd should not equal to the rolname, include upper and lower */
    if (0 == pg_strcasecmp(newPasswd, roleID)) {
        str_reset(newPasswd);
        str_reset(oldPasswd);
        ereport(ERROR, (errcode(ERRCODE_INVALID_PASSWORD), errmsg("Password should not equal to the rolname.")));
    }

    /* newPasswd should not equal to the reverse of rolname, include upper and lower */
    reverse_str = reverse_string(roleID);
    if (reverse_str == NULL) {
        str_reset(newPasswd);
        str_reset(oldPasswd);
        ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("reverse_string failed, possibility out of memory")));
    }
    if (0 == pg_strcasecmp(newPasswd, reverse_str)) {
        free(reverse_str);
        str_reset(newPasswd);
        str_reset(oldPasswd);
        ereport(
            ERROR, (errcode(ERRCODE_INVALID_PASSWORD), errmsg("Password should not equal to the reverse of rolname.")));
    }
    free(reverse_str);
    reverse_str = NULL;

    /* If oldPasswd exist, newPasswd should not equal to the old ones */
    if (oldPasswd != NULL) {
        if (IsPasswdEqual(roleID, newPasswd, oldPasswd)) {
            str_reset(newPasswd);
            str_reset(oldPasswd);
            ereport(
                ERROR, (errcode(ERRCODE_INVALID_PASSWORD), errmsg("New password should not equal to the old ones.")));
        }
        /* newPasswd should not equal to the reverse of oldPasswd */
        reverse_str = reverse_string(newPasswd);
        if (reverse_str == NULL) {
            str_reset(newPasswd);
            str_reset(oldPasswd);
            ereport(
                ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("reverse_string failed, possibility out of memory")));
        }
        if (IsPasswdEqual(roleID, reverse_str, oldPasswd)) {
            free(reverse_str);
            str_reset(newPasswd);
            str_reset(oldPasswd);
            ereport(ERROR,
                (errcode(ERRCODE_INVALID_PASSWORD),
                    errmsg("New password should not equal to the reverse of old ones.")));
        }
        free(reverse_str);
        reverse_str = NULL;
    }

    return true;
}

/*
 * Brief			: when rolpassword is changed, add the change record in pg_auth_history
 * Description		: roleID - Oid of role
 *				  passwd - input password
 *				  operatType - whether the operation is create role or alter role
 * Notes			:
 */
static void AddAuthHistory(Oid roleID, const char* rolename, const char* passwd, int operatType, const char* salt)
{
    Relation pg_auth_history_rel;

    if (passwd == NULL) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PASSWORD), errmsg("the parameter passwd of AddAuthHistory is null")));
    }

    pg_auth_history_rel = RelationIdGetRelation(AuthHistoryRelationId);
    if (!RelationIsValid(pg_auth_history_rel)) {
        ereport(WARNING, (errmsg("the relation pg_auth_history is invalid")));
        return;
    }
    
    TupleDesc pg_auth_history_dsc = NULL;
    HeapTuple password_tuple = NULL;
    ScanKeyData key[1];
    SysScanDesc scan = NULL;
    TimestampTz fromTime; /* the time date back Password_reuse_time from nowTime */
    TimestampTz nowTime;  /* now time */
    Datum passwordtimeDatum;
    Datum rolpasswordDatum;
    char* rolpassword = NULL; /* get rolpassword from pg_auth_history */
    TimestampTz passwordTime; /* get passwordtime from pg_auth_history */
    Interval tspan;           /* interval between fromTime and nowTime */
    Datum fromTimeDatum;
    bool isTimeSafe = false;        /* whether the passwordTime in pg_auth_history is earlier than fromTime */
    bool isMaxSafe = false;         /* whether the password change times is larger than Password_reuse_max */
    int changeTimes = 0;            /* password change times */
    bool hasSame = false;           /* whether the password is same */
    const char* currentTime = NULL; /* strings of nowTime */
    bool passwordtimeIsNull = false;
    bool rolpasswordIsNull = false;
    Datum password_new_record[Natts_pg_auth_history];
    char password_new_record_nulls[Natts_pg_auth_history];
    char salt_stored[SALT_LENGTH * 2 + 1] = {0};
    char encrypted_sha256_password[SHA256_LENGTH + ENCRYPTED_STRING_LENGTH + 1] = {0};

    LockRelationOid(AuthHistoryRelationId, RowExclusiveLock);
    pgstat_initstats(pg_auth_history_rel);
    pg_auth_history_dsc = RelationGetDescr(pg_auth_history_rel);

    /* get current time */
    nowTime = GetCurrentTimestamp();

    /* if the operation is alter role, we will check the reuse conditons */
    if (operatType == ALTER_PG_AUTH_ROLE) {
        /* we transform the u_sess->attr.attr_security.Password_reuse_time to days and seconds */
        tspan.month = 0;
        tspan.day = (int)floor(u_sess->attr.attr_security.Password_reuse_time);
#ifdef HAVE_INT64_TIMESTAMP
        tspan.time = (u_sess->attr.attr_security.Password_reuse_time - tspan.day) * HOURS_PER_DAY * SECS_PER_HOUR *
                     USECS_PER_SEC;
#else
        tspan.time = (u_sess->attr.attr_security.Password_reuse_time - tspan.day) * HOURS_PER_DAY * SECS_PER_HOUR;
#endif

        /* get the fromTime */
        fromTimeDatum =
            DirectFunctionCall2(timestamptz_mi_interval, TimestampGetDatum(nowTime), PointerGetDatum(&tspan));
        fromTime = TimestampGetDatum(fromTimeDatum);
        /* get all the records from pg_auth_history of the roleID */
        ScanKeyInit(&key[0], Anum_pg_auth_history_roloid, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(roleID));
        scan = systable_beginscan(pg_auth_history_rel, AuthHistoryIndexId, true, SnapshotNow, 1, key);

        /* get the tuple according to reverse order of the index */
        while (HeapTupleIsValid(password_tuple = systable_getnext_back(scan))) {
            passwordtimeDatum = heap_getattr(
                password_tuple, Anum_pg_auth_history_passwordtime, pg_auth_history_dsc, &passwordtimeIsNull);
            rolpasswordDatum = heap_getattr(
                password_tuple, Anum_pg_auth_history_rolpassword, pg_auth_history_dsc, &rolpasswordIsNull);
            /* get the passwordtime of tuple */
            passwordTime = (passwordtimeIsNull || (void*)passwordtimeDatum == NULL ) ?
                           0 : DatumGetTimestampTz(passwordtimeDatum);

            /* get the password of the tuple */
            rolpassword = TextDatumGetCString(rolpasswordDatum);
            /* here only use sha256 to encrypt the password. */
            errno_t rc = strncpy_s(salt_stored, sizeof(salt_stored), &rolpassword[SHA256_LENGTH], sizeof(salt_stored) - 1);
            securec_check(rc, "\0", "\0");
            salt_stored[sizeof(salt_stored) - 1] = '\0';

            /* iteration can't be changed for check reuse. */
            if (!pg_sha256_encrypt(passwd, salt_stored, strlen(salt_stored), encrypted_sha256_password, NULL)) {
                str_reset(encrypted_sha256_password);
                ereport(ERROR, (errcode(ERRCODE_INVALID_PASSWORD), errmsg("sha256-password encryption failed")));
            }

            /* whether the tuple's rolepassword is same to passwd */
            if (0 == strncmp(encrypted_sha256_password, rolpassword, SHA256_PASSWD_LEN + 1)) {
                hasSame = true;
            }
            /* whether the passwordTime of the tuple is latter than fromTime */
            if (u_sess->attr.attr_security.Password_reuse_time > 0 && passwordTime < fromTime) {
                isTimeSafe = true;
            }
            changeTimes++;
            /* whether the changeTimes is larger than u_sess->attr.attr_security.Password_reuse_max */
            if (u_sess->attr.attr_security.Password_reuse_max > 0 &&
                changeTimes > u_sess->attr.attr_security.Password_reuse_max) {
                isMaxSafe = true;
            }
            
            if (u_sess->attr.attr_security.Password_reuse_time > 0 ||
                u_sess->attr.attr_security.Password_reuse_max > 0) {
                /* rolepassword is same and all the reuse conditions are not satisfied */
                if (hasSame && !isTimeSafe && !isMaxSafe) {
                    str_reset(encrypted_sha256_password);
                    ereport(ERROR, (errcode(ERRCODE_INVALID_PASSWORD), errmsg("The password cannot be reused.")));
                }
            }
        }

        systable_endscan(scan);
    }

    /* insert the current operation record into the table */
    errno_t rc = memset_s(password_new_record, sizeof(password_new_record), 0, sizeof(password_new_record));
    securec_check(rc, "\0", "\0");
    rc = memset_s(
        password_new_record_nulls, sizeof(password_new_record_nulls), ' ', sizeof(password_new_record_nulls));
    securec_check(rc, "\0", "\0");

    password_new_record[Anum_pg_auth_history_roloid - 1] = ObjectIdGetDatum(roleID);
    currentTime = timestamptz_to_str(nowTime);
    password_new_record[Anum_pg_auth_history_passwordtime - 1] = DirectFunctionCall3(
        timestamptz_in, CStringGetDatum(currentTime), ObjectIdGetDatum(InvalidOid), Int32GetDatum(-1));
    /* iteration can't be changed for check reuse. */
    if (!pg_sha256_encrypt(passwd, salt, strlen(salt), encrypted_sha256_password, NULL)) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PASSWORD), errmsg("sha256-password encryption failed")));
    }

    password_new_record[Anum_pg_auth_history_rolpassword - 1] = CStringGetTextDatum(encrypted_sha256_password);

    password_tuple = heap_formtuple(pg_auth_history_dsc, password_new_record, password_new_record_nulls);

    (void)simple_heap_insert(pg_auth_history_rel, password_tuple);

    CatalogUpdateIndexes(pg_auth_history_rel, password_tuple);

    heap_close(pg_auth_history_rel, NoLock);
}

/*
 * Brief			: delete all the records of roleID in pg_auth_history
 * Description		:
 * Notes			:
 */
static void DropAuthHistory(Oid roleID)
{
    Relation pg_auth_history_rel = NULL;
    SysScanDesc sscan = NULL;
    HeapTuple tmp_tuple = NULL;
    ScanKeyData scankey;

    pg_auth_history_rel = RelationIdGetRelation(AuthHistoryRelationId);

    /* if the relation is valid, then delete the records of the role */
    if (RelationIsValid(pg_auth_history_rel)) {
        LockRelationOid(AuthHistoryRelationId, RowExclusiveLock);
        pgstat_initstats(pg_auth_history_rel);

        ScanKeyInit(&scankey, Anum_pg_auth_history_roloid, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(roleID));

        sscan = systable_beginscan(pg_auth_history_rel, AuthHistoryIndexId, true, SnapshotNow, 1, &scankey);

        while (HeapTupleIsValid(tmp_tuple = systable_getnext(sscan))) {
            simple_heap_delete(pg_auth_history_rel, &tmp_tuple->t_self);
        }
        systable_endscan(sscan);
        heap_close(pg_auth_history_rel, NoLock);
    } else {
        ereport(WARNING, (errmsg("the relation pg_auth_history is invalid")));
        return;
    }
}

/*
 * Brief			: Check if the current user ID has the privilege to alter user
 *					: with auditadmin.
 * Description		: For auditadmin user, when enablePrivilegesSeparate is turned
 *					: off, users with createrole privilege could change the attribute,
 *					: when enablePrivilegesSeparate is turned on, only superusers
 *					: could.
 * @in roleid		: the user's roleid
 * @in isOnlyAlterPassword	: mark just change the passwd of this user when true.
 * Notes			: NA
 */
void CheckAlterAuditadminPrivilege(Oid roleid, bool isOnlyAlterPassword)
{
    if (!g_instance.attr.attr_security.enablePrivilegesSeparate) {
        /* separation of privilege is turned off */
        if (!(have_createrole_privilege() || (isOnlyAlterPassword && roleid == GetUserId()))) {
            ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), errmsg("Permission denied.")));
        }
    } else {
        /* separation of privilege is turned on */
        if (!(isRelSuperuser() || (isOnlyAlterPassword && roleid == GetUserId()))) {
            ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), errmsg("Permission denied.")));
        }
    }
}

/*
 * @Description: check whether role is a persistence role.
 * @roleid : the role need to be check.
 * @return : true for persistence and false for nopersistence.
 */
bool is_role_persistence(Oid roleid)
{
    bool flag = false;

    Relation relation = heap_open(AuthIdRelationId, AccessShareLock);
    TupleDesc pg_authid_dsc = RelationGetDescr(relation);

    /* Look up the information in pg_authid. */
    HeapTuple rtup = SearchSysCache1(AUTHOID, ObjectIdGetDatum(roleid));
    if (HeapTupleIsValid(rtup)) {
        bool is_null = false;
        Datum authid_rolkind_datum = heap_getattr(rtup, Anum_pg_authid_rolkind, pg_authid_dsc, &is_null);

        if (!is_null) {
            flag = DatumGetChar(authid_rolkind_datum) == ROLKIND_PERSISTENCE ? true : false;
        } else {
            flag = false;
        }

        ReleaseSysCache(rtup);
    }
    heap_close(relation, AccessShareLock);
    return flag;
}

/* Database Security: Support lock/unlock account */
/*
 * Brief			: Check if the current user ID has the privilege to lock/unlock
 *					: the user with ROLEID roleid
 * Description		: When enablePrivilegesSeparate is turned off, users with different
 *					: attribute may have different behavior. When enablePrivilegesSeparate
 *					: is turned on, users with sysadmin, auditadmin and createrole
 *					: have the same behavior.
 * @in roleid		: the user's roleid
 * @in tuple		: The user's tuple
 * @in is_opradmin   : is this user a operatoradmin
 * Notes			: NA
 */
void CheckLockPrivilege(Oid roleID, HeapTuple tuple, bool is_opradmin)
{
    if (!isRelSuperuser() && !have_createrole_privilege()) {
        ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), errmsg("Permission denied.")));
    }

    /* Only initialuser can lock/unlock persistence and operatoradmin users. */
    if (get_rolkind(tuple) == ROLKIND_PERSISTENCE || is_opradmin) {
        if(!initialuser())
            ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), errmsg("Permission denied.")));
    }

    /* make a difference for lock/unlock between g_instance.attr.attr_security.enablePrivilegesSeparate is on and off */
    if (!g_instance.attr.attr_security.enablePrivilegesSeparate) {
        /* No need to consider auditadmin user */
        if (((Form_pg_authid)GETSTRUCT(tuple))->rolsystemadmin && roleID != BOOTSTRAP_SUPERUSERID) {
            if (!isRelSuperuser()) {
                ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), errmsg("Permission denied.")));
            }
        }

        if (((Form_pg_authid)GETSTRUCT(tuple))->rolcreaterole) {
            if (!isRelSuperuser()) {
                ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), errmsg("Permission denied.")));
            }
        }
    } else {
        /* Only superuser can lock/unlock special users. */
        if (((Form_pg_authid)GETSTRUCT(tuple))->rolcreaterole || ((Form_pg_authid)GETSTRUCT(tuple))->rolauditadmin ||
            ((Form_pg_authid)GETSTRUCT(tuple))->rolsystemadmin) {
            if (!isRelSuperuser())
                ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), errmsg("Permission denied.")));
        }
    }

    return;
}

int64 SearchAllAccounts()
{
    Relation pg_user_status_rel = NULL;
    TupleDesc pg_user_status_dsc = NULL;
    HeapScanDesc scan = NULL;
    HeapTuple tuple = NULL;
    Datum roleid_datum;
    bool is_roleid_null = false;
    int64 num = 0;

    /* get the tuple of pg_user_status */
    pg_user_status_rel = RelationIdGetRelation(UserStatusRelationId);

    /* if the relation is valid, get the tuple of roleID*/
    if (RelationIsValid(pg_user_status_rel)) {
        LockRelationOid(UserStatusRelationId, RowExclusiveLock);
        pgstat_initstats(pg_user_status_rel);
        pg_user_status_dsc = RelationGetDescr(pg_user_status_rel);
        scan = (HeapScanDesc)heap_beginscan(pg_user_status_rel, SnapshotNow, 0, NULL);

        while ((tuple = heap_getnext((TableScanDesc)scan, ForwardScanDirection)) != NULL) {
            /* Database Security: Support database audit */
            roleid_datum = heap_getattr(tuple, Anum_pg_user_status_roloid, pg_user_status_dsc, &is_roleid_null);
            if (!(is_roleid_null || (void*)roleid_datum == NULL)) {
                HeapTuple tupleForSeachCache = NULL;
                tupleForSeachCache = SearchSysCache1(AUTHOID, roleid_datum);
                /* if user was not found in AUTHOID,we just do nothing.Because*/
                if (!HeapTupleIsValid(tupleForSeachCache)) {
                    continue;
                }
                num++;
                ReleaseSysCache(tupleForSeachCache);
            }
        }

        heap_endscan((TableScanDesc)scan);
        AcceptInvalidationMessages();
        (void)GetCurrentCommandId(true);
        CommandCounterIncrement();
        heap_close(pg_user_status_rel, NoLock);
    } else {
        ereport(WARNING, (errmsg("the relation pg_user_status is invalid")));
    }
    return num;
}

void InitAccountLockHashTable()
{
    HASHCTL hctl;
    errno_t rc = 0;
    int64 account_num = 0;
#define INIT_ACCOUNT_NUM 10

    LWLockAcquire(g_instance.policy_cxt.account_table_lock, LW_EXCLUSIVE);

    if (g_instance.policy_cxt.account_table != NULL) {
        LWLockRelease(g_instance.policy_cxt.account_table_lock);
        return;
    }

    account_num = SearchAllAccounts();
    if (account_num < INIT_ACCOUNT_NUM) {
        account_num = INIT_ACCOUNT_NUM;
    }

    rc = memset_s(&hctl, sizeof(HASHCTL), 0, sizeof(HASHCTL));
    securec_check(rc, "", "");
    hctl.keysize = sizeof(Oid);
    hctl.entrysize = sizeof(AccountLockHashEntry);
    hctl.hash = oid_hash;
    hctl.hcxt = g_instance.account_context;
    g_instance.policy_cxt.account_table = hash_create("User login info",
        account_num,
        &hctl,
        HASH_ELEM | HASH_FUNCTION | HASH_SHRCTX);

    LWLockRelease(g_instance.policy_cxt.account_table_lock);
}

void UpdateFailCountToHashTable(Oid roleid, int4 extrafails, bool superlock)
{
    AccountLockHashEntry *account_entry = NULL;
    bool found = false;
    /* Audit user locked or unlocked */
    bool lockflag = 0;
    char* rolename = NULL;

    if (!LockAccountParaValid(roleid, extrafails, superlock)) {
        return;
    }
    rolename = GetUserNameFromId(roleid);

    if (g_instance.policy_cxt.account_table == NULL) {
        InitAccountLockHashTable();
    }

    account_entry = (AccountLockHashEntry *)hash_search(g_instance.policy_cxt.account_table, &roleid, HASH_ENTER, &found);
    if (found == false) {
        SpinLockInit(&account_entry->mutex);
    }

    SpinLockAcquire(&account_entry->mutex);
    if (found == false) {
        account_entry->failcount = extrafails;
        account_entry->rolstatus = UNLOCK_STATUS;
    } else {
        account_entry->failcount += extrafails;
        if (u_sess->attr.attr_security.Failed_login_attempts > 0 && 
            account_entry->failcount >= u_sess->attr.attr_security.Failed_login_attempts) {
            lockflag = true;
        }
    }

    /* super lock account or exceed failed limit */
    if (extrafails == 0 || lockflag == true) {
        account_entry->rolstatus = superlock ? SUPERLOCK_STATUS : LOCK_STATUS;
        account_entry->locktime = GetCurrentTimestamp();
        lockflag = true;
        ereport(DEBUG2, (errmsg("%s locktime %s", rolename, timestamptz_to_str(account_entry->locktime))));
    }
    ereport(DEBUG2, (errmsg("%s failcount %d, rolstatus %d", rolename, account_entry->failcount, account_entry->rolstatus)));
    SpinLockRelease(&account_entry->mutex);

    ReportLockAccountMessage(lockflag, rolename);
}

void FillAccountRecord(AccountLockHashEntry *account_entry, TupleDesc pg_user_status_dsc, HeapTuple tuple, 
                    Datum *user_status_record, bool *user_status_record_repl) {
    Datum userStatusDatum;
    bool userStatusIsNull = false;
    int32 failcount_in_catalog = account_entry->failcount;
    const char* locktime_in_catalog = NULL;
    bool catalog_superlock = false;
    bool catalog_lock = false;
    
    userStatusDatum = heap_getattr(tuple, Anum_pg_user_status_failcount, pg_user_status_dsc, &userStatusIsNull);
    if (!(userStatusIsNull || (void*)userStatusDatum == NULL)) {
        failcount_in_catalog += DatumGetInt32(userStatusDatum);
    }

    userStatusDatum = heap_getattr(tuple, Anum_pg_user_status_rolstatus, pg_user_status_dsc, &userStatusIsNull);
    if (!(userStatusIsNull || (void*)userStatusDatum == NULL)) {
        if (DatumGetInt16(userStatusDatum) == SUPERLOCK_STATUS) {
            catalog_superlock = true;
        } else if (DatumGetInt16(userStatusDatum) == LOCK_STATUS) {
            catalog_lock = true;
        }
    }

    if (catalog_superlock == false) {
        if (account_entry->rolstatus == SUPERLOCK_STATUS) {
            locktime_in_catalog = timestamptz_to_str(account_entry->locktime);
            user_status_record[Anum_pg_user_status_locktime - 1] = DirectFunctionCall3(
                timestamptz_in, CStringGetDatum(locktime_in_catalog), ObjectIdGetDatum(InvalidOid), Int32GetDatum(-1));
            user_status_record_repl[Anum_pg_user_status_locktime - 1] = true;
            user_status_record[Anum_pg_user_status_rolstatus - 1] = Int16GetDatum(SUPERLOCK_STATUS);
            user_status_record_repl[Anum_pg_user_status_rolstatus - 1] = true;
        } else if (catalog_lock == false) {
            if (account_entry->rolstatus == LOCK_STATUS) {
                locktime_in_catalog = timestamptz_to_str(account_entry->locktime);
                user_status_record[Anum_pg_user_status_locktime - 1] = DirectFunctionCall3(
                    timestamptz_in, CStringGetDatum(locktime_in_catalog), ObjectIdGetDatum(InvalidOid), Int32GetDatum(-1));
                user_status_record_repl[Anum_pg_user_status_locktime - 1] = true;
                user_status_record[Anum_pg_user_status_rolstatus - 1] = Int16GetDatum(LOCK_STATUS);
                user_status_record_repl[Anum_pg_user_status_rolstatus - 1] = true;
            } else if (u_sess->attr.attr_security.Failed_login_attempts > 0 && 
                failcount_in_catalog >= u_sess->attr.attr_security.Failed_login_attempts) {
                /* The sum of failcount in hash table and pg_user_status > Failed_login_attempts, update rolestatus*/
                user_status_record[Anum_pg_user_status_rolstatus - 1] = Int16GetDatum(LOCK_STATUS);
                user_status_record_repl[Anum_pg_user_status_rolstatus - 1] = true;
                TimestampTz nowTime = GetCurrentTimestamp();
                locktime_in_catalog = timestamptz_to_str(nowTime);
                user_status_record[Anum_pg_user_status_locktime - 1] = DirectFunctionCall3(
                    timestamptz_in, CStringGetDatum(locktime_in_catalog), ObjectIdGetDatum(InvalidOid), Int32GetDatum(-1));
                user_status_record_repl[Anum_pg_user_status_locktime - 1] = true;
            }
        }
    }

    user_status_record[Anum_pg_user_status_failcount - 1] = Int32GetDatum(failcount_in_catalog);
    user_status_record_repl[Anum_pg_user_status_failcount - 1] = true;
}

void UpdateAccountInfoFromHashTable()
{
    AccountLockHashEntry *account_entry = NULL;
    HASH_SEQ_STATUS hseq_status;

    Relation pg_user_status_rel = NULL;
    TupleDesc pg_user_status_dsc = NULL;
    HeapTuple tuple = NULL;
    HeapTuple new_tuple = NULL;

    /* get tuple of pg_user_status*/
    pg_user_status_rel = RelationIdGetRelation(UserStatusRelationId);
    /* if the relation is valid, get the tuple of roleID*/
    if (RelationIsValid(pg_user_status_rel)) {
        LockRelationOid(UserStatusRelationId, RowExclusiveLock);
        pgstat_initstats(pg_user_status_rel);
        pg_user_status_dsc = RelationGetDescr(pg_user_status_rel);

        hash_seq_init(&hseq_status, g_instance.policy_cxt.account_table);
        while ((account_entry = (AccountLockHashEntry*)hash_seq_search(&hseq_status)) != NULL) {
            tuple = SearchSysCache1(USERSTATUSROLEID, PointerGetDatum(account_entry->roleoid));
            if (HeapTupleIsValid(tuple)) {
                Datum user_status_record[Natts_pg_user_status] = {0};
                bool user_status_record_nulls[Natts_pg_user_status] = {false};
                bool user_status_record_repl[Natts_pg_user_status] = {false};
                FillAccountRecord(account_entry, pg_user_status_dsc, tuple, user_status_record, user_status_record_repl);
                new_tuple = (HeapTuple) tableam_tops_modify_tuple(
                    tuple, pg_user_status_dsc, user_status_record, user_status_record_nulls, user_status_record_repl);
                heap_inplace_update(pg_user_status_rel, new_tuple);
                CacheInvalidateHeapTupleInplace(pg_user_status_rel, new_tuple);
                tableam_tops_free_tuple(new_tuple);
                ReleaseSysCache(tuple);
            }
            hash_search(g_instance.policy_cxt.account_table, &(account_entry->roleoid), HASH_REMOVE, NULL);
        }
        AcceptInvalidationMessages();
        heap_close(pg_user_status_rel, NoLock);
    }
}

bool CanUnlockAccount(TimestampTz locktime)
{
    TimestampTz now_time;
    TimestampTz from_time;
    Datum from_time_datum;
    Interval tspan;

    /* we transform the u_sess->attr.attr_security.Password_lock_time to days and seconds */
    tspan.month = 0;
    tspan.day = (int)floor(u_sess->attr.attr_security.Password_lock_time);
#ifdef HAVE_INT64_TIMESTAMP
    tspan.time =
        (u_sess->attr.attr_security.Password_lock_time - tspan.day) * HOURS_PER_DAY * SECS_PER_HOUR * USECS_PER_SEC;
#else
    tspan.time = (u_sess->attr.attr_security.Password_lock_time - tspan.day) * HOURS_PER_DAY * SECS_PER_HOUR;
#endif

    now_time = GetCurrentTimestamp();
    from_time_datum = DirectFunctionCall2(timestamptz_mi_interval, TimestampGetDatum(now_time), PointerGetDatum(&tspan));
    from_time = DatumGetTimestampTz(from_time_datum);
    if (locktime < from_time) {
        return true;
    } else {
        return false;
    }
}

bool UnlockAccountToHashTable(Oid roleid, bool superlock, bool isreset)
{
    bool found = false;
    AccountLockHashEntry *account_entry = NULL;
    int2 status;

    /* user account has not been locked if account_table is null */
    if (g_instance.policy_cxt.account_table == NULL) {
        char* relName = NULL;
        relName = get_rel_name(roleid);
        if (relName != NULL) {
            ereport(NOTICE, (errmsg("user account %s has not been locked", relName)));
        }
        return true;
    }

    account_entry = (AccountLockHashEntry *)hash_search(g_instance.policy_cxt.account_table, &roleid, HASH_FIND, &found);
    if (found) {
        SpinLockAcquire(&account_entry->mutex);
        status = account_entry->rolstatus;
        if (superlock) {
            account_entry->rolstatus = UNLOCK_STATUS;
            account_entry->failcount = 0;
            SpinLockRelease(&account_entry->mutex);
            ereport(DEBUG2, (errmsg("super unlock account %u", roleid)));
            return true;
        } else {
            if (status == SUPERLOCK_STATUS) {
                SpinLockRelease(&account_entry->mutex);
                return false;
            }
            if (status == UNLOCK_STATUS) {
                if (isreset) {
                    account_entry->failcount = 0;
                }
                SpinLockRelease(&account_entry->mutex);
                return true;
            }
            if (CanUnlockAccount(account_entry->locktime)) {
                account_entry->failcount = 0;
                account_entry->rolstatus = UNLOCK_STATUS;
                SpinLockRelease(&account_entry->mutex);
                return true;
            }
            SpinLockRelease(&account_entry->mutex);
            return false;
        }
    }
    return true;
}

bool LockAccountParaValid(Oid roleID, int extrafails, bool superlock)
{
#define INITUSER_OID 10

    if (!OidIsValid(roleID)) {
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("TryLockAccount(): roleid is not valid.")));
        return false;
    }

    if (INITUSER_OID == roleID) {
        if (superlock)
            ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), errmsg("Permission denied.")));
        return false;
    }

    if (extrafails < 0) {
        ereport(ERROR,
            (errcode(ERRCODE_DATA_EXCEPTION), errmsg("TryLockAccount(): parameter extrafails is less than zero.")));
        return false;
    }
    return true;
}

void ReportLockAccountMessage(bool locked, const char *rolename)
{
    if (locked) {
        pgaudit_lock_or_unlock_user(true, rolename);
    }

    AcceptInvalidationMessages();
    (void)GetCurrentCommandId(true);
    CommandCounterIncrement();
}

/* Database Security: Support lock/unlock account */
/*
 * Brief			: try to lock the account, just update the pg_user_status
 * Description		: if the roleID is not exist in pg_user_status, then add the record
 *				: if the roleID is exist in pg_user_status, then update the record
 * Notes			:
 */
void TryLockAccount(Oid roleID, int extrafails, bool superlock)
{
    Relation pg_user_status_rel = NULL;
    TupleDesc pg_user_status_dsc = NULL;
    HeapTuple tuple = NULL;
    HeapTuple new_tuple = NULL;
    const char* currentTime = NULL;
    TimestampTz nowTime;
    int32 failedcount = 0;
    int16 status = 0;
    Datum userStatusDatum;
    bool userStatusIsNull = false;
    Datum user_status_record[Natts_pg_user_status];
    bool user_status_record_nulls[Natts_pg_user_status] = {false};
    bool user_status_record_repl[Natts_pg_user_status] = {false};

    /* Audit user locked or unlocked */
    bool lockflag = 0;
    char* rolename = NULL;

    /* We could not insert new xlog if recovery in process */
    if (RecoveryInProgress()) {
        return;
    }

    if (!LockAccountParaValid(roleID, extrafails, superlock)) {
        return;
    }

    rolename = GetUserNameFromId(roleID);

    /* get tuple of pg_user_status*/
    pg_user_status_rel = RelationIdGetRelation(UserStatusRelationId);

    /* if the relation is valid, get the tuple of roleID*/
    if (RelationIsValid(pg_user_status_rel)) {
        LockRelationOid(UserStatusRelationId, RowExclusiveLock);
        pgstat_initstats(pg_user_status_rel);
        pg_user_status_dsc = RelationGetDescr(pg_user_status_rel);
        tuple = SearchSysCache1(USERSTATUSROLEID, PointerGetDatum(roleID));

        /* insert/update a new login failed record into the pg_user_status */
        errno_t errorno = memset_s(user_status_record, sizeof(user_status_record), 0, sizeof(user_status_record));
        securec_check(errorno, "\0", "\0");
        errorno = memset_s(
            user_status_record_nulls, sizeof(user_status_record_nulls), false, sizeof(user_status_record_nulls));
        securec_check(errorno, "\0", "\0");
        errorno =
            memset_s(user_status_record_repl, sizeof(user_status_record_repl), false, sizeof(user_status_record_repl));
        securec_check(errorno, "\0", "\0");

        /* if there is no record of the role, then add one record in the pg_user_status */
        if (HeapTupleIsValid(tuple)) {
            userStatusDatum = heap_getattr(tuple, Anum_pg_user_status_failcount, pg_user_status_dsc, &userStatusIsNull);
            if (!(userStatusIsNull || (void*)userStatusDatum == NULL)) {
                failedcount = DatumGetInt32(userStatusDatum);
            } else {
                failedcount = 0;
            }
            failedcount += extrafails;
            userStatusDatum = heap_getattr(tuple, Anum_pg_user_status_rolstatus, pg_user_status_dsc, &userStatusIsNull);
            if (!(userStatusIsNull || (void*)userStatusDatum == NULL)) {
                status = DatumGetInt16(userStatusDatum);
            } else {
                status = UNLOCK_STATUS;
            }

            /* if superuser try lock, just update the status */
            if (superlock && status != SUPERLOCK_STATUS) {
                nowTime = GetCurrentTimestamp();
                currentTime = timestamptz_to_str(nowTime);
                user_status_record[Anum_pg_user_status_rolstatus - 1] = Int16GetDatum(SUPERLOCK_STATUS);
                user_status_record_repl[Anum_pg_user_status_rolstatus - 1] = true;
                user_status_record[Anum_pg_user_status_locktime - 1] = DirectFunctionCall3(
                    timestamptz_in, CStringGetDatum(currentTime), ObjectIdGetDatum(InvalidOid), Int32GetDatum(-1));
                user_status_record_repl[Anum_pg_user_status_locktime - 1] = true;
                user_status_record[Anum_pg_user_status_failcount - 1] = Int32GetDatum(failedcount);
                user_status_record_repl[Anum_pg_user_status_failcount - 1] = true;
                lockflag = 1;
            } else {
                /* Update the failedcount, only when the account is not locked */
                if (status == UNLOCK_STATUS) {
                    user_status_record[Anum_pg_user_status_failcount - 1] = Int32GetDatum(failedcount);
                    user_status_record_repl[Anum_pg_user_status_failcount - 1] = true;
                }
                /* if account is not locked and the failedcount is larger than
                   u_sess->attr.attr_security.Failed_login_attempts, update the lock time and status */
                if (u_sess->attr.attr_security.Failed_login_attempts > 0 &&
                    failedcount >= u_sess->attr.attr_security.Failed_login_attempts && status == UNLOCK_STATUS) {
                    nowTime = GetCurrentTimestamp();
                    currentTime = timestamptz_to_str(nowTime);
                    user_status_record[Anum_pg_user_status_locktime - 1] = DirectFunctionCall3(
                        timestamptz_in, CStringGetDatum(currentTime), ObjectIdGetDatum(InvalidOid), Int32GetDatum(-1));
                    user_status_record[Anum_pg_user_status_rolstatus - 1] = Int16GetDatum(LOCK_STATUS);
                    user_status_record_repl[Anum_pg_user_status_locktime - 1] = true;
                    user_status_record_repl[Anum_pg_user_status_rolstatus - 1] = true;
                    lockflag = 1;
                }
            }
            new_tuple = (HeapTuple) tableam_tops_modify_tuple(
                tuple, pg_user_status_dsc, user_status_record, user_status_record_nulls, user_status_record_repl);
            heap_inplace_update(pg_user_status_rel, new_tuple);
            CacheInvalidateHeapTupleInplace(pg_user_status_rel, new_tuple);
            tableam_tops_free_tuple(new_tuple);
            ReleaseSysCache(tuple);
        } else {
            /* if the record is already exist, update the record */
            ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("The tuple of pg_user_status not found")));
        }

        heap_close(pg_user_status_rel, RowExclusiveLock);

    } else {
        ereport(WARNING, (errmsg("the relation pg_user_status is invalid")));
        return;
    }

    ReportLockAccountMessage(lockflag, rolename);
}

TimestampTz GetPasswordTimeOfTuple(TimestampTz nowTime, TimestampTz* fromTime, Datum userStatusDatum, HeapTuple tuple,
    TupleDesc pg_user_status_dsc, bool* userStatusIsNull)
{
    Datum fromTimeDatum;
    Interval tspan;
    TimestampTz lockTime = 0;

    /* we transform the u_sess->attr.attr_security.Password_lock_time to days and seconds */
    tspan.month = 0;
    tspan.day = (int)floor(u_sess->attr.attr_security.Password_lock_time);
#ifdef HAVE_INT64_TIMESTAMP
    tspan.time =
        (u_sess->attr.attr_security.Password_lock_time - tspan.day) * HOURS_PER_DAY * SECS_PER_HOUR * USECS_PER_SEC;
#else
    tspan.time = (u_sess->attr.attr_security.Password_lock_time - tspan.day) * HOURS_PER_DAY * SECS_PER_HOUR;
#endif

    /* get the fromTime */
    fromTimeDatum = DirectFunctionCall2(timestamptz_mi_interval, TimestampGetDatum(nowTime), PointerGetDatum(&tspan));
    *fromTime = DatumGetTimestampTz(fromTimeDatum);

    userStatusDatum = heap_getattr(tuple, Anum_pg_user_status_locktime, pg_user_status_dsc, userStatusIsNull);
    /* get the passwordtime of tuple */
    if ((*userStatusIsNull) || (void*)userStatusDatum == NULL) {
        lockTime = 0;
    } else {
        lockTime = DatumGetTimestampTz(userStatusDatum);
    }

    return lockTime;
}

/*
 * Brief			: try to unlock the account
 * Description		: if satisfied unlock conditions, delete the record of the role
 * Notes			:
 */
bool TryUnlockAccount(Oid roleID, bool superunlock, bool isreset)
{
    Relation pg_user_status_rel = NULL;
    TupleDesc pg_user_status_dsc = NULL;
    TimestampTz nowTime;
    TimestampTz fromTime;
    TimestampTz lockTime;
    int16 status = 0;
    Datum userStatusDatum;
    bool userStatusIsNull = false;
    bool result = false;
    bool unlockflag = 0;
    char* rolename = NULL;

    if (!OidIsValid(roleID)) {
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("TryUnlockAccount(): roleid is not valid.")));
    }

#define INITUSER_OID 10
    if (roleID == INITUSER_OID) {
        if (superunlock) {
            ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), errmsg("Permission denied.")));
        } else {
            return true;
        }
    }

    rolename = GetUserNameFromId(roleID);
    /* get the tuple of pg_user_status */
    pg_user_status_rel = RelationIdGetRelation(UserStatusRelationId);

    /* if the relation is valid, get the tuple of roleID */
    if (RelationIsValid(pg_user_status_rel)) {
        LockRelationOid(UserStatusRelationId, RowExclusiveLock);
        pgstat_initstats(pg_user_status_rel);
        pg_user_status_dsc = RelationGetDescr(pg_user_status_rel);

        HeapTuple tuple = SearchSysCache1(USERSTATUSROLEID, PointerGetDatum(roleID));

        /* if the record is not exist, it may be already unlocked by someone else */
        if (!HeapTupleIsValid(tuple)) {
            ereport(WARNING, (errmsg("Invalid roleid in pg_user_status.")));
        } else {
            /* if super user try to unlock, just delete the tuple */
            userStatusDatum = heap_getattr(tuple, Anum_pg_user_status_rolstatus, pg_user_status_dsc, &userStatusIsNull);
            if (!(userStatusIsNull || (void*)userStatusDatum == NULL)) {
                status = DatumGetInt16(userStatusDatum);
            } else {
                status = UNLOCK_STATUS;
            }

            if (superunlock) {
                if (status != UNLOCK_STATUS) {
                    UpdateUnlockAccountTuples(tuple, pg_user_status_rel, pg_user_status_dsc);
                    result = true;
                    unlockflag = 1;
                }
            } else {
                if (status == UNLOCK_STATUS) {
                    if (isreset) {
                        Datum failCountDatum;
                        int failCount = 0;

                        failCountDatum =
                            heap_getattr(tuple, Anum_pg_user_status_failcount, pg_user_status_dsc, &userStatusIsNull);
                        if (userStatusIsNull || (void*)failCountDatum == NULL) {
                            failCount = 0;
                        } else {
                            failCount = DatumGetTimestampTz(failCountDatum);
                        }

                        if (failCount > 0)
                            UpdateUnlockAccountTuples(tuple, pg_user_status_rel, pg_user_status_dsc);
                    }
                    result = true;
                } else if (status == SUPERLOCK_STATUS) {
                    result = false;
                } else {
                    /* get current time */
                    nowTime = GetCurrentTimestamp();
                    lockTime = GetPasswordTimeOfTuple(
                        nowTime, &fromTime, userStatusDatum, tuple, pg_user_status_dsc, &userStatusIsNull);
                    if (lockTime < fromTime) {
                        UpdateUnlockAccountTuples(tuple, pg_user_status_rel, pg_user_status_dsc);
                        result = true;
                        unlockflag = 1;
                    } else {
                        result = false;
                    }
                }
            }
            ReleaseSysCache(tuple);
        }
        AcceptInvalidationMessages();
        (void)GetCurrentCommandId(true);
        CommandCounterIncrement();
        heap_close(pg_user_status_rel, RowExclusiveLock);
        if (unlockflag) {
            pgaudit_lock_or_unlock_user(false, rolename);
        }
    } else {
        ereport(WARNING, (errmsg("the relation pg_user_status is invalid")));
    }

    return result;
}

/*
 * Brief			: try to unlock all the account in pg_user_status
 * Description		: if satisfied unlock conditions, delete the record of the role
 * Notes			:
 */
void TryUnlockAllAccounts(void)
{
    Relation pg_user_status_rel = NULL;
    TupleDesc pg_user_status_dsc = NULL;
    TableScanDesc scan = NULL;
    HeapTuple tuple = NULL;
    TimestampTz nowTime;
    TimestampTz fromTime;
    TimestampTz lockTime;
    int16 status = 0;
    Datum userStatusDatum;
    bool userStatusIsNull = false;
    Datum roleIdDatum;
    bool roleIdIsNull = false;
    char* rolename = NULL;

    if (t_thrd.postmaster_cxt.HaShmData->current_mode == STANDBY_MODE) {
        return;
    }

    /* get the tuple of pg_user_status */
    pg_user_status_rel = RelationIdGetRelation(UserStatusRelationId);

    /* if the relation is valid, get the tuple of roleID */
    if (RelationIsValid(pg_user_status_rel)) {
        LockRelationOid(UserStatusRelationId, RowExclusiveLock);
        pgstat_initstats(pg_user_status_rel);
        pg_user_status_dsc = RelationGetDescr(pg_user_status_rel);
        scan = tableam_scan_begin(pg_user_status_rel, SnapshotNow, 0, NULL);

        /* get current time */
        nowTime = GetCurrentTimestamp();
        while ((tuple = (HeapTuple) tableam_scan_getnexttuple(scan, ForwardScanDirection)) != NULL) {
            userStatusDatum = heap_getattr(tuple, Anum_pg_user_status_rolstatus, pg_user_status_dsc, &userStatusIsNull);
            if (!(userStatusIsNull || (void*)userStatusDatum == NULL)) {
                status = DatumGetInt16(userStatusDatum);
            } else {
                status = UNLOCK_STATUS;
            }
            /* Database Security: Support database audit */
            roleIdDatum = heap_getattr(tuple, Anum_pg_user_status_roloid, pg_user_status_dsc, &roleIdIsNull);
            if (!(roleIdIsNull || (void*)roleIdDatum == NULL)) {
                HeapTuple tupleForSeachCache = NULL;
                tupleForSeachCache = SearchSysCache1(AUTHOID, roleIdDatum);
                /* if user was not found in AUTHOID,we just do nothing.Because */
                if (!HeapTupleIsValid(tupleForSeachCache)) {
                    continue;
                }
                rolename = pstrdup(NameStr(((Form_pg_authid)GETSTRUCT(tupleForSeachCache))->rolname));

                ReleaseSysCache(tupleForSeachCache);
            }

            if (status == LOCK_STATUS) {
                lockTime = GetPasswordTimeOfTuple(
                    nowTime, &fromTime, userStatusDatum, tuple, pg_user_status_dsc, &userStatusIsNull);
                if (lockTime < fromTime) {
                    UpdateUnlockAccountTuples(tuple, pg_user_status_rel, pg_user_status_dsc);
                    pgaudit_lock_or_unlock_user(false, rolename);
                }
            }
        }

        tableam_scan_end(scan);
        AcceptInvalidationMessages();
        (void)GetCurrentCommandId(true);
        CommandCounterIncrement();
        heap_close(pg_user_status_rel, NoLock);
    } else {
        ereport(WARNING, (errmsg("the relation pg_user_status is invalid")));
    }
}

static void UpdateUnlockAccountTuples(HeapTuple tuple, Relation rel, TupleDesc tupledesc)
{
    HeapTuple new_tuple = NULL;
    Datum user_status_record[Natts_pg_user_status];
    bool user_status_record_nulls[Natts_pg_user_status] = {false};
    bool user_status_record_repl[Natts_pg_user_status] = {false};
    
    errno_t rc = memset_s(user_status_record, sizeof(user_status_record), 0, sizeof(user_status_record));
    securec_check(rc, "\0", "\0");
    rc = memset_s(user_status_record_nulls, sizeof(user_status_record_nulls), 0, sizeof(user_status_record_nulls));
    securec_check(rc, "\0", "\0");
    rc = memset_s(user_status_record_repl, sizeof(user_status_record_repl), 0, sizeof(user_status_record_repl));
    securec_check(rc, "\0", "\0");

    user_status_record[Anum_pg_user_status_failcount - 1] = Int32GetDatum(0);
    user_status_record_repl[Anum_pg_user_status_failcount - 1] = true;
    user_status_record[Anum_pg_user_status_rolstatus - 1] = Int16GetDatum(UNLOCK_STATUS);
    user_status_record_repl[Anum_pg_user_status_rolstatus - 1] = true;

    new_tuple =
        (HeapTuple) tableam_tops_modify_tuple(tuple, tupledesc, user_status_record, user_status_record_nulls, user_status_record_repl);
    heap_inplace_update(rel, new_tuple);
    tableam_tops_free_tuple(new_tuple);
}

/*
 * Brief			: whether the account is already been locked
 * Description		:
 * Notes			:
 */
bool IsAccountLocked(Oid roleID)
{
    int16 status = 0;
    Datum userStatusDatum;
    bool userStatusIsNull = false;
    bool result = true;

    if (!OidIsValid(roleID)) {
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("IsAccountLocked(): roleid is not valid.")));
    }
    HeapTuple tuple = SearchSysCache1(USERSTATUSROLEID, PointerGetDatum(roleID));
    if (!HeapTupleIsValid(tuple)) {
        result = false;
    } else {
        userStatusDatum = SysCacheGetAttr(USERSTATUSROLEID, tuple, Anum_pg_user_status_rolstatus, &userStatusIsNull);
        if (!(userStatusIsNull || (void*)userStatusDatum == NULL)) {
            status = DatumGetInt16(userStatusDatum);
        } else {
            status = UNLOCK_STATUS;
        }

        if (status == UNLOCK_STATUS) {
            result = false;
        } else {
            result = true;
        }
        ReleaseSysCache(tuple);
    }

    return result;
}

/* Get the status of account. */
USER_STATUS GetAccountLockedStatus(Oid roleID)
{
    uint16 status = 0;
    Datum userStatusDatum;
    bool userStatusIsNull = false;

    if (!OidIsValid(roleID)) {
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("getAccountLockedStyle: roleid is not valid.")));
    }

    if (g_instance.policy_cxt.account_table != NULL) {
        /* Update user status info from hash table to pg_user_status table. We only update once
         * when the first time user connect to get user lock status after dn became primary. To deal
         * with concurrent scenarios, check hash table not null again after we get hash table lock.
         */
        (void)LWLockAcquire(g_instance.policy_cxt.account_table_lock, LW_EXCLUSIVE);
        if (g_instance.policy_cxt.account_table != NULL) {
            UpdateAccountInfoFromHashTable();
            hash_destroy(g_instance.policy_cxt.account_table);
            g_instance.policy_cxt.account_table = NULL;
        }
        LWLockRelease(g_instance.policy_cxt.account_table_lock);
    }

    HeapTuple tuple = SearchSysCache1(USERSTATUSROLEID, PointerGetDatum(roleID));
    if (!HeapTupleIsValid(tuple)) {
        status = UNLOCK_STATUS;
    } else {
        userStatusDatum = SysCacheGetAttr(USERSTATUSROLEID, tuple, Anum_pg_user_status_rolstatus, &userStatusIsNull);
        if (!(userStatusIsNull || (void*)userStatusDatum == NULL)) {
            status = DatumGetInt16(userStatusDatum);
        } else {
            status = UNLOCK_STATUS;
        }
        ReleaseSysCache(tuple);
    }

    return (USER_STATUS)status;
}

USER_STATUS GetAccountLockedStatusFromHashTable(Oid roleid)
{
    AccountLockHashEntry *account_entry = NULL;
    bool found = false;
    USER_STATUS rolestatus = UNLOCK_STATUS;

    if (g_instance.policy_cxt.account_table == NULL) {
        InitAccountLockHashTable();
    }

    account_entry = (AccountLockHashEntry *)hash_search(g_instance.policy_cxt.account_table, &roleid, HASH_FIND, &found);
    if (found == true) {
        SpinLockAcquire(&account_entry->mutex);
        rolestatus = (USER_STATUS)(account_entry->rolstatus);
        SpinLockRelease(&account_entry->mutex);
    }
    return rolestatus;
}
/* Get the status of account password. */
PASSWORD_STATUS GetAccountPasswordExpired(Oid roleID)
{
    if (roleID == BOOTSTRAP_SUPERUSERID) {
        return UNEXPIRED_STATUS;
    }

    if (!OidIsValid(roleID)) {
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("Invalid roleid in pg_user_status.")));
    }

    int16 status = 0;
    bool userStatusIsNull = false;
    HeapTuple tuple = SearchSysCache1(USERSTATUSROLEID, PointerGetDatum(roleID));
    if (!HeapTupleIsValid(tuple)) {
        return UNEXPIRED_STATUS;
    } else {
        Datum userStatusDatum =
            SysCacheGetAttr((int)USERSTATUSROLEID, tuple, Anum_pg_user_status_passwordexpired, &userStatusIsNull);
        if (!(userStatusIsNull || (void*)userStatusDatum == NULL)) {
            status = DatumGetInt16(userStatusDatum);
        } 
        ReleaseSysCache(tuple);
    }
    return (PASSWORD_STATUS)status;
}

/* set pg_user_status paswordstatus. */
void SetAccountPasswordExpired(Oid roleID, bool expired)
{
    if (roleID == BOOTSTRAP_SUPERUSERID) {
        ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
            errmsg("Forbidden to make password expired of the initial account.")));
    }

    Relation pg_user_status_rel = NULL;
    TupleDesc pg_user_status_dsc = NULL;
    HeapTuple new_tuple = NULL;
    Datum userStatusRecord[Natts_pg_user_status] = {0};
    bool user_status_record_nulls[Natts_pg_user_status] = {false};
    bool user_status_record_repl[Natts_pg_user_status] = {false};

    pg_user_status_rel = RelationIdGetRelation(UserStatusRelationId);
    if (RelationIsValid(pg_user_status_rel)) { 
        LockRelationOid(UserStatusRelationId, RowExclusiveLock);
        pgstat_initstats(pg_user_status_rel);
        pg_user_status_dsc = RelationGetDescr(pg_user_status_rel);
        HeapTuple tuple = SearchSysCache1(USERSTATUSROLEID, PointerGetDatum(roleID));
        if (!HeapTupleIsValid(tuple)) {
            ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("The roleid of pg_user_status not found.")));
        } else { 
            userStatusRecord[Anum_pg_user_status_passwordexpired - 1] =
                Int16GetDatum(expired ? EXPIRED_STATUS : UNEXPIRED_STATUS);
            user_status_record_repl[Anum_pg_user_status_passwordexpired - 1] = true;
            new_tuple =
                heap_modify_tuple(tuple, pg_user_status_dsc, userStatusRecord,
                                  user_status_record_nulls, user_status_record_repl);
            simple_heap_update(pg_user_status_rel, &new_tuple->t_self, new_tuple);
            heap_freetuple_ext(new_tuple);
            ReleaseSysCache(tuple);
        }
        AcceptInvalidationMessages();
        (void)GetCurrentCommandId(true);
        CommandCounterIncrement();
        heap_close(pg_user_status_rel, RowExclusiveLock);
    } else {
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("the relation pg_user_status is invalid")));
    }
}


/*
 * Brief			: delete all the records of roleID in pg_auth_history
 * Description		:
 * Notes			:
 */
void DropUserStatus(Oid roleID)
{
    if (!OidIsValid(roleID)) {
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("DropUserStatus(): roleid is not valid.")));
    }

    /* get the tuple of pg_user_status */
    Relation pg_user_status_rel = RelationIdGetRelation(UserStatusRelationId);

    /* if the relation is valid, then delete the records of the role */
    if (RelationIsValid(pg_user_status_rel)) {
        LockRelationOid(UserStatusRelationId, RowExclusiveLock);
        pgstat_initstats(pg_user_status_rel);

        HeapTuple tuple = SearchSysCache1(USERSTATUSROLEID, PointerGetDatum(roleID));
        if (HeapTupleIsValid(tuple)) {
            simple_heap_delete(pg_user_status_rel, &tuple->t_self);
            ReleaseSysCache(tuple);
        }
        heap_close(pg_user_status_rel, NoLock);
    } else {
        ereport(WARNING, (errmsg("the relation pg_user_status is invalid")));
        return;
    }
}

/*
 * Brief			: Get the roleid through username
 * Description		:
 * Notes			:
 */
Oid GetRoleOid(const char* username)
{
    HeapTuple tuple = SearchSysCache1(AUTHNAME, PointerGetDatum(username));
    if (!HeapTupleIsValid(tuple)) {
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("Invalid username/password,login denied.")));
    }
    Oid roleID = HeapTupleGetOid(tuple);
    ReleaseSysCache(tuple);
    return roleID;
}

/*
 * Brief			: Get the roleid through username
 * Description		:
 * Notes			:
 */
bool IsRoleExist(const char* username)
{
    bool result = false;
    HOLD_INTERRUPTS();
    HeapTuple tuple = SearchSysCache1(AUTHNAME, PointerGetDatum(username));
    RESUME_INTERRUPTS();
    CHECK_FOR_INTERRUPTS();
    if (HeapTupleIsValid(tuple)) {
        ReleaseSysCache(tuple);
        result = true;
    }
    return result;
}

/*
 * Brief			: Get the roleid through username
 * Description		:
 * Notes			:
 */
bool IsAlreadyLoginFailed(Oid roleID)
{
    bool result = false;
    if (!OidIsValid(roleID)) {
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("IsAccountLocked(): roleid is not valid.")));
    }
    HeapTuple tuple = SearchSysCache1(USERSTATUSROLEID, PointerGetDatum(roleID));
    if (HeapTupleIsValid(tuple)) {
        ReleaseSysCache(tuple);
        result = true;
    }
    return result;
}
/* Database Security: Support password complexity */
/*
 * Brief             : reverse_string()
 * Description       : reverse the string
 */
static char* reverse_string(const char* str)
{
    int i;
    int len;
    char* new_string = NULL;
    len = strlen(str);
    new_string = (char*)malloc(len + 1);
    if (new_string == NULL) {
        return NULL;
    }
    for (i = 0; i < len; ++i) {
        new_string[len - i - 1] = str[i];
    }
    new_string[len] = '\0';
    return new_string;
}

/* Get the newest password changing time of user. */
TimestampTz GetUserCurrentPwdtime(Oid roleID)
{
    TupleDesc pg_auth_history_dsc = NULL;
    bool passwordtimeIsNull = false;
    Datum passwordtimeDatum;
    TimestampTz passwordTime = 0;
    ScanKeyData key[1];
    HeapTuple historytupe = NULL;
    SysScanDesc scan;

    /* Open the pg_auth_history catalog. */
    Relation pg_auth_history_rel = heap_open(AuthHistoryRelationId, AccessShareLock);

    /* Scan the pg_auth_history by the roleID. */
    ScanKeyInit(&key[0], Anum_pg_auth_history_roloid, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(roleID));
    scan = systable_beginscan(pg_auth_history_rel, AuthHistoryIndexId, true, SnapshotNow, 1, key);

    /* Get the tuple according to reverse order of the index. */
    while (HeapTupleIsValid(historytupe = systable_getnext_back(scan))) {
        pg_auth_history_dsc = RelationGetDescr(pg_auth_history_rel);

        passwordtimeDatum =
            heap_getattr(historytupe, Anum_pg_auth_history_passwordtime, pg_auth_history_dsc, &passwordtimeIsNull);

        /* Get the passwordtime in the pg_auth_history tuple. */
        if (passwordtimeIsNull || NULL == (void*)passwordtimeDatum) {
            ereport(ERROR, (errcode(ERRCODE_INVALID_PASSWORD), errmsg("User's passwordtime in pg_auth_history is 0.")));
        } else {
            /* Get the lastest password change time. */
            passwordTime = Max(passwordTime, DatumGetTimestampTz(passwordtimeDatum));
        }
    }

    systable_endscan(scan);
    heap_close(pg_auth_history_rel, AccessShareLock);
    return passwordTime;
}

/* Get the left time before password expired. */
Datum gs_password_deadline(PG_FUNCTION_ARGS)
{

    Oid roleid = GetCurrentUserId();
    TimestampTz CurrentPwdtime = GetUserCurrentPwdtime(roleid);
    TimestampTz NowTime = GetCurrentTimestamp();
    Datum FromTimeDatum;
    TimestampTz FromTime;
    Interval TimeSpan;
    Datum DatumLeftSpan;
    Interval* LeftSpan = (Interval*)palloc0(sizeof(Interval));

    /* If u_sess->attr.attr_security.Password_effect_time is zero or password disabled, we return directly. */
    if (u_sess->attr.attr_security.Password_effect_time == 0 || CurrentPwdtime == 0)
        PG_RETURN_INTERVAL_P(LeftSpan);

    /* We transform the u_sess->attr.attr_security.Password_effect_time to interval. */
    TimeSpan.month = 0;
    TimeSpan.day = (int)floor(u_sess->attr.attr_security.Password_effect_time);
#ifdef HAVE_INT64_TIMESTAMP
    TimeSpan.time = (u_sess->attr.attr_security.Password_effect_time - TimeSpan.day) * HOURS_PER_DAY * SECS_PER_HOUR *
                    USECS_PER_SEC;
#else
    TimeSpan.time = (u_sess->attr.attr_security.Password_effect_time - TimeSpan.day) * HOURS_PER_DAY * SECS_PER_HOUR;
#endif

    /* Calculate the latest time should the password changed. */
    FromTimeDatum =
        DirectFunctionCall2(timestamptz_mi_interval, TimestampGetDatum(NowTime), PointerGetDatum(&TimeSpan));
    FromTime = DatumGetTimestampTz(FromTimeDatum);

    /* Calculate the time before password expired. */
    DatumLeftSpan = DirectFunctionCall2(timestamp_mi, CurrentPwdtime, FromTime);
    LeftSpan = DatumGetIntervalP(DatumLeftSpan);

    PG_RETURN_INTERVAL_P(LeftSpan);
}

/* Get the password noticetime which set by user. */
Datum gs_password_notifytime(PG_FUNCTION_ARGS)
{
    PG_RETURN_INT32(u_sess->attr.attr_security.Password_notify_time);
}

/* Clean role's connections on all CNs before drop role operation. */
void PreCleanAndCheckUserConns(const char* username, bool missing_ok)
{
    char query[256] = {0};
    Oid role_id;
    List* childlist = NIL;

    role_id = GetSysCacheOid1(AUTHNAME, CStringGetDatum(username));
    if (!OidIsValid(role_id)) {
        if (!missing_ok) {
            if (!have_createrole_privilege())
                ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), errmsg("Permission denied.")));
            else
                ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("role \"%s\" does not exist", username)));
        } else {
            return;
        }
    }

    if (GetUserChildlistFromCatalog(role_id, &childlist, true)) {
        ListCell* child = NULL;
        foreach (child, childlist) {
            Oid childoid = lfirst_oid(child);
            char childname[NAMEDATALEN] = {0};

            (void)GetRoleName(childoid, childname, NAMEDATALEN);

            if (*childname)
                PreCleanAndCheckUserConns(childname, missing_ok);
        }
    }

    /* 1. clean connections on local pooler */
    DropRoleCleanConnection((char*)username);

    /* 2. clean connections on pooler of remote CNs */
    int rc = sprintf_s(query, sizeof(query), "CLEAN CONNECTION TO ALL TO USER \"%s\";", username);
    securec_check_ss(rc, "\0", "\0");
    ExecUtilityStmtOnNodes(query, NULL, false, true, EXEC_ON_COORDS, false);
}

char* GetRoleName(Oid rolid, char* rolname, size_t size)
{
    HeapTuple tup = SearchSysCache1(AUTHOID, rolid);

    /* if user was not found in AUTHOID,we just do nothing.Because */
    if (!HeapTupleIsValid(tup))
        return NULL;

    errno_t rc = sprintf_s(rolname, size, "%s", NameStr(((Form_pg_authid)GETSTRUCT(tup))->rolname));
    securec_check_ss(rc, "\0", "\0");

    ReleaseSysCache(tup);

    return rolname;
}

/*
 * function name: GetSuperUserName
 * description  : get super user name
 */
char* GetSuperUserName(char* username)
{
    HeapTuple tuple = SearchSysCache1(AUTHOID, ObjectIdGetDatum(BOOTSTRAP_SUPERUSERID));

    if (!HeapTupleIsValid(tuple))
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT),
                errmsg("While get super user, invalid role OID: %u", (uint)BOOTSTRAP_SUPERUSERID)));

    errno_t rc =
        snprintf_s(username, NAMEDATALEN, NAMEDATALEN - 1, "%s", NameStr(((Form_pg_authid)GETSTRUCT(tuple))->rolname));
    securec_check_ss(rc, "\0", "\0");

    ereport(DEBUG1, (errmsg("get super user: %s", username)));

    ReleaseSysCache(tuple);

    return username;
}

Datum calculate_encrypted_combined_password(const char* password, const char* rolname, const char* salt_string)
{
    char encrypted_md5_password[MD5_PASSWD_LEN + 1] = {0};
    char encrypted_sha256_password[SHA256_PASSWD_LEN + 1] = {0};
    char encrypted_combined_password[SHA256_MD5_COMBINED_LEN + 1] = {0};
    char iteration_string[ITERATION_STRING_LEN + 1] = {0};
    Datum datum_value;
    errno_t rc = EOK;

    /* For PG ecological compatibility, we stored both sha256 and md5 password. */
    if (!pg_sha256_encrypt(password,
            salt_string,
            strlen(salt_string),
            encrypted_sha256_password,
            NULL,
            u_sess->attr.attr_security.auth_iteration_count)) {
        rc = memset_s(encrypted_sha256_password, SHA256_PASSWD_LEN + 1, 0, SHA256_PASSWD_LEN + 1);
        securec_check(rc, "\0", "\0");
        ereport(ERROR, (errcode(ERRCODE_INVALID_PASSWORD), errmsg("first stage encryption password failed")));
    }

    if (!pg_md5_encrypt(password, rolname, strlen(rolname), encrypted_md5_password)) {
        rc = memset_s(encrypted_md5_password, MD5_PASSWD_LEN + 1, 0, MD5_PASSWD_LEN + 1);
        securec_check(rc, "\0", "\0");
        rc = memset_s(encrypted_sha256_password, SHA256_PASSWD_LEN + 1, 0, SHA256_PASSWD_LEN + 1);
        securec_check(rc, "\0", "\0");
        ereport(ERROR, (errcode(ERRCODE_INVALID_PASSWORD), errmsg("second stage encryption password failed")));
    }

    encode_iteration(u_sess->attr.attr_security.auth_iteration_count, iteration_string);

    /* Now password contain sha256,md5,iteration and client_server_key with old iteration. */
    rc = snprintf_s(encrypted_combined_password,
        SHA256_MD5_COMBINED_LEN + 1,
        SHA256_MD5_COMBINED_LEN,
        "%s%s%s",
        encrypted_sha256_password,
        encrypted_md5_password,
        iteration_string);
    securec_check_ss(rc, "\0", "\0");
    datum_value = CStringGetTextDatum(encrypted_combined_password);
    ereport(NOTICE, (errmsg("The encrypted password contains MD5 ciphertext, which is not secure.")));

    /* clear the sensitive messages in the stack. */
    rc = memset_s(encrypted_md5_password, MD5_PASSWD_LEN + 1, 0, MD5_PASSWD_LEN + 1);
    securec_check(rc, "\0", "\0");
    rc = memset_s(encrypted_sha256_password, SHA256_PASSWD_LEN + 1, 0, SHA256_PASSWD_LEN + 1);
    securec_check(rc, "\0", "\0");
    rc = memset_s(encrypted_combined_password, SHA256_MD5_COMBINED_LEN + 1, 0, SHA256_MD5_COMBINED_LEN + 1);
    securec_check(rc, "\0", "\0");
    rc = memset_s(iteration_string, ITERATION_STRING_LEN + 1, 0, ITERATION_STRING_LEN + 1);
    securec_check(rc, "\0", "\0");

    return datum_value;
}

Datum calculate_encrypted_sha256_password(const char* password, const char* rolname, const char* salt_string)
{
    char encrypted_sha256_password[SHA256_PASSWD_LEN + 1] = {0};
    char encrypted_sha256_password_complex[SHA256_PASSWD_LEN + ITERATION_STRING_LEN + 1] = {0};
    char iteration_string[ITERATION_STRING_LEN + 1] = {0};
    Datum datum_value;
    errno_t rc = EOK;

    if (!pg_sha256_encrypt(password,
            salt_string,
            strlen(salt_string),
            encrypted_sha256_password,
            NULL,
            u_sess->attr.attr_security.auth_iteration_count)) {
        rc = memset_s(encrypted_sha256_password, SHA256_PASSWD_LEN + 1, 0, SHA256_PASSWD_LEN + 1);
        securec_check(rc, "\0", "\0");
        ereport(ERROR, (errcode(ERRCODE_INVALID_PASSWORD), errmsg("password encryption failed")));
    }

    encode_iteration(u_sess->attr.attr_security.auth_iteration_count, iteration_string);
    rc = snprintf_s(encrypted_sha256_password_complex,
        SHA256_PASSWD_LEN + ITERATION_STRING_LEN + 1,
        SHA256_PASSWD_LEN + ITERATION_STRING_LEN,
        "%s%s",
        encrypted_sha256_password,
        iteration_string);
    securec_check_ss(rc, "\0", "\0");

    datum_value = CStringGetTextDatum(encrypted_sha256_password_complex);
    rc = memset_s(encrypted_sha256_password_complex,
        SHA256_PASSWD_LEN + ITERATION_STRING_LEN + 1,
        0,
        SHA256_PASSWD_LEN + ITERATION_STRING_LEN + 1);
    securec_check(rc, "\0", "\0");
    rc = memset_s(encrypted_sha256_password, SHA256_PASSWD_LEN + 1, 0, SHA256_PASSWD_LEN + 1);
    securec_check(rc, "\0", "\0");
    rc = memset_s(iteration_string, ITERATION_STRING_LEN + 1, 0, ITERATION_STRING_LEN + 1);
    securec_check(rc, "\0", "\0");

    return datum_value;
}

/*
 * @Description: calculate the encrypted password for different conditions.
 * @bool is_encrypted : whether password need encrypted, must be true currently.
 * @char* password : the password need be encrypted.
 * @char* rolname : the role name who own the password.
 * @char* salt_string : the role oid need check.
 * @return : the encrypted password in Datum format.
 */
Datum calculate_encrypted_password(bool is_encrypted, const char* password, const char* rolname, 
                                   const char* salt_string)
{
    errno_t rc = EOK;
    char encrypted_md5_password[MD5_PASSWD_LEN + 1] = {0};
    Datum datum_value;

    if (!is_encrypted || isPWDENCRYPTED(password)) {
        return CStringGetTextDatum(password);
    }

    /*
     * The guc parameter of u_sess.attr.attr_security.Password_encryption_type here may be 0, 1, 2.
     * if Password_encryption_type is 0, the encrypted password is md5.
     * if Password_encryption_type is 1, the encrypted password is sha256 + md5.
     * if Password_encryption_type is 2, the encrypted password is sha256.
     */
    if (u_sess->attr.attr_security.Password_encryption_type == 0) {
        if (!pg_md5_encrypt(password, rolname, strlen(rolname), encrypted_md5_password)) {
            rc = memset_s(encrypted_md5_password, MD5_PASSWD_LEN + 1, 0, MD5_PASSWD_LEN + 1);
            securec_check(rc, "\0", "\0");
            ereport(ERROR, (errcode(ERRCODE_INVALID_PASSWORD), errmsg("password encryption failed")));
        }

        datum_value = CStringGetTextDatum(encrypted_md5_password);
        rc = memset_s(encrypted_md5_password, MD5_PASSWD_LEN + 1, 0, MD5_PASSWD_LEN + 1);
        securec_check(rc, "\0", "\0");
        ereport(NOTICE, (errmsg("The encrypted password contains MD5 ciphertext, which is not secure.")));
    } else if (u_sess->attr.attr_security.Password_encryption_type == 1) {
        datum_value = calculate_encrypted_combined_password(password, rolname, salt_string);
    } else {
        datum_value = calculate_encrypted_sha256_password(password, rolname, salt_string);
    }

    return datum_value;
}

/*
 * Target		:encode int iteration to stable length string.
 * Description	:converted int iteration to string for well store in system table.
 * Input		:auth iteration integer.
 * Output		:auth iteration string after encoded.
 */
void encode_iteration(int auth_count, char* auth_iteration_string)
{
    char base_string[ITERATION_STRING_LEN + 1] = "ecdfdcefade";
    int bit_count = 0;
    const int divided_num = 10;
    for (int i = 0; i < ITERATION_STRING_LEN; i++) {
        bit_count = auth_count % divided_num;
        auth_count = auth_count / divided_num;
        auth_iteration_string[i] = base_string[i] + bit_count;
    }
}

/*
 * Target		:dencode string iteration to int iteration.
 * Description	:converted string iteration to int for deriveKey.
 * Input		:auth iteration string.
 * return		:auth iteration integer.
 */
int decode_iteration(const char* auth_iteration_string)
{
    char base_string[ITERATION_STRING_LEN + 1] = "ecdfdcefade";
    int auth_count = 0;
    int bit_count = 0;

    /* If auth_iteration_string is '\0', mean use default iterstion count. */
    if (*auth_iteration_string != '\0') {
        for (int i = 0; i < ITERATION_STRING_LEN; i++) {

            bit_count = auth_iteration_string[i] - base_string[i];
            auth_count += bit_count * pow(10, i);
        }
    } else {
        auth_count = ITERATION_COUNT;
    }
    return auth_count;
}

/*
 * check weak password dictionary
 */
static bool is_weak_password(const char* password)
{
    HeapTuple tup = NULL;
    Datum datum;
    bool is_null = false;
    char* exist_pwd = NULL;
    bool result = false;

    Relation gs_weak_rel = heap_open(GsGlobalConfigRelationId, AccessShareLock);

    TableScanDesc scan = tableam_scan_begin(gs_weak_rel, SnapshotNow, 0, NULL);
    while ((tup = (HeapTuple) tableam_scan_getnexttuple(scan, ForwardScanDirection)) != NULL) {
        if (strcmp(DatumGetCString(heap_getattr(tup, Anum_gs_global_config_name, RelationGetDescr(gs_weak_rel), &is_null)), 
            "weak_password") == 0) {
            datum = heap_getattr(tup, Anum_gs_global_config_value, RelationGetDescr(gs_weak_rel), &is_null);
            if (is_null) {
                continue;
            }	
            exist_pwd = TextDatumGetCString(datum);
            if (strcmp(password, exist_pwd) == 0) {
                result = true; 
                break;
            }
        }
    }

    tableam_scan_end(scan);
    heap_close(gs_weak_rel, AccessShareLock);
    return result;
}

static void check_weak_password(char *Password)
{
    if (is_weak_password(Password)) {
        str_reset(Password);
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_PASSWORD), errmsg("Password should not be weak password.")));        
    }
}
