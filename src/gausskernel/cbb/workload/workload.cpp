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
 * workload.cpp
 *   functions for workload management
 *
 * IDENTIFICATION
 *    src/gausskernel/cbb/workload/workload.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include <sys/mman.h> /* mmap */

#include "access/tableam.h"
#include "catalog/dependency.h"
#include "catalog/indexing.h"
#include "catalog/namespace.h"
#include "catalog/pg_authid.h"
#include "catalog/pg_shdepend.h"
#include "catalog/pgxc_group.h"
#include "catalog/pg_user_status.h"
#include "commands/dbcommands.h"
#include "commands/defrem.h"
#include "commands/user.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "pgxc/pgxc.h"
#include "securec.h"
#include "storage/lmgr.h"
#include "storage/proc.h"
#include "utils/acl.h"
#include "utils/atomic.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/memprot.h"
#include "utils/syscache.h"
#include "utils/snapmgr.h"
#include "utils/inval.h"
#include "optimizer/nodegroups.h"

#include "workload/commgr.h"
#include "workload/workload.h"

#ifdef ENABLE_UT
#define static
#endif

#define SESSION_TABNAME "gs_wlm_session_query_info_all"
#define OPERATOR_TABNAME "gs_wlm_operator_info"
#define SESSIONUSER_TABNAME "gs_wlm_user_session_info"
#define USER_RESOURCE_HISTORY_TABNAME "gs_wlm_user_resource_history"
#define INSTANCE_RESOURCE_HISTORY_TABNAME "gs_wlm_instance_history"
#define MAX_INFO_MEM 30          /* MB */
#define RESPOOL_MIN_MEMORY 16384 /* 16GB */

#define CHAR_BUF_SIZE 512

extern uint64 parseTableSpaceMaxSize(char* maxSize, bool* unlimited, char** newMaxSize);
extern int gscgroup_get_cgroup_cpuinfo(struct cgroup* cg, int* setcnt, int64* usage);
extern void WLMParseFunc4SessionInfo(StringInfo msg, void* suminfo, int size);
extern void WLMInitMinValue4SessionInfo(WLMGeneralData* gendata);
extern void WLMCheckMinValue4SessionInfo(WLMGeneralData* gendata);

extern int gscgroup_map_node_group(WLMNodeGroupInfo* ng);

/*
 * **************** STATIC FUNCTIONS ************************
 */
/*
 * @Description: check memory limit option
 * @IN isNull: check option whether is null
 * @IN memory_limit: memory limit string in the resource pool
 * @OUT memory: memory string with unit
 * @IN size: memory string size
 * @Return: void
 * @See also:
 */
static void CheckMemoryLimitOption(bool isNull, const char* memory_limit, char* memory, int size)
{
    int memory_limit_size = 0;

    const char* hintmsg = NULL;

    /* option is not null, redundant options */
    if (!isNull) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("redundant options.")));
    }

    /* parse memory limit to 'kB' unit */
    if (!parse_int(memory_limit, &memory_limit_size, GUC_UNIT_KB, &hintmsg)) {
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("invalid value \"%s\" for parameter \"%s\"", memory_limit, "memory_limit"),
                hintmsg ? errhint("%s", _(hintmsg)) : 0));
    }

    if (memory_limit_size <= 0) {
        ereport(ERROR,
            (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
                errmsg("memory_limit size value can't be %d.", memory_limit_size)));
    }

    /* make memory string with unit */
    (void)GetMemorySizeWithUnit(memory, size, memory_limit_size);
}

/*
 * @Description: get user data from htab
 * @IN roleid: user id
 * @IN is_noexcept: if it is not true, we will report an error while
 *                  no data in the hash table, default we will not.
 * @Return: UserData pointer
 * @See also:
 */
UserData* GetUserDataFromHTab(Oid roleid, bool is_noexcept = true)
{
    /* if no exception, only return NULL while htab is not ok */
    Assert(g_instance.wlm_cxt->stat_manager.user_info_hashtbl != NULL);

    UserData* userdata =
        (UserData*)hash_search(g_instance.wlm_cxt->stat_manager.user_info_hashtbl, &roleid, HASH_FIND, NULL);

    /* check the userdata in the htab */
    if (userdata == NULL && !is_noexcept) {
        ereport(ERROR,
            (errcode(ERRCODE_UNDEFINED_OBJECT),
                errmsg("role %u does not exist", roleid),
                errhint("please use \"pgxc_wlm_rebuild_user_respool\" to rebuild info.")));
    }

    return userdata;
}

/*
 * @Description: get resource pool from htab
 * @IN rpoid: resource pool id
 * @IN is_noexcept: if it is not true, we will report an error while
 *                  no data in the hash table, default we will not.
 * @Return: ResourcePool Pointer
 * @See also:
 */
ResourcePool* GetRespoolFromHTab(Oid rpoid, bool is_noexcept)
{
    /* if no exception, only return NULL while htab is not ok */
    Assert(g_instance.wlm_cxt->resource_pool_hashtbl != NULL);

    ResourcePool* respool =
        (ResourcePool*)hash_search(g_instance.wlm_cxt->resource_pool_hashtbl, &rpoid, HASH_FIND, NULL);

    /* check the userdata in the htab */
    if (respool == NULL && !is_noexcept) {
        ereport(ERROR,
            (errcode(ERRCODE_UNDEFINED_OBJECT),
                errmsg("resource pool %u does not exist in htab.", rpoid),
                errhint("please use \"pgxc_wlm_rebuild_user_respool\" to rebuild info.")));
    }

    return respool;
}

/*
 * @Description: check create resource pool with control_group and mem_percent
 * @IN cgroup: resource pool to create with control_group
 * @IN&OUT mempct: for multi_tenant cases, default_memory_percentage is 20, while for normal cases, it is 0
 * @OUT parentid: get parent resource pool id of the newly create resource pool
 * @See also:
 */
void CheckRespoolMultiTenantCreate(WLMNodeGroupInfo* ng, char* cgroup, int* mempct, Oid* parentoid, int* actpct)
{
    bool isparent = false;
    bool is_time_share = false;
    int sumpct = 0;
    bool multi_tenant = false;
    bool find_ng = false;
    Oid parentid = InvalidOid;
    int parentpct = 0;
    HeapTuple tup;
    errno_t rc;
    char* pointer = NULL;
    char* endptr = NULL;
    Datum groupanme_datum;
    bool isNull = false;
    char* groupanme = NULL;

    Relation relation = heap_open(ResourcePoolRelationId, AccessShareLock);
    SysScanDesc scan = systable_beginscan(relation, InvalidOid, false, NULL, 0, NULL);
    while (HeapTupleIsValid((tup = systable_getnext(scan)))) {
        Oid scanrpoid = HeapTupleGetOid(tup);
        char scancgroup[NAMEDATALEN];
        Form_pg_resource_pool pool = (Form_pg_resource_pool)GETSTRUCT(tup);
        int scanmempct = pool->mem_percent;

        groupanme_datum = heap_getattr(tup, Anum_pg_resource_pool_nodegroup, RelationGetDescr(relation), &isNull);
        if (!isNull) {
            groupanme = (char*)pstrdup((const char*)DatumGetCString(groupanme_datum));
        } else {
            groupanme = (char*)pstrdup(DEFAULT_NODE_GROUP);
        }

        int strdiff = strcmp(ng->group_name, groupanme);
        pfree(groupanme);
        if (strdiff != 0) {
            continue;
        }

        find_ng = true;  // only find node group, it can do the following check

        if (StringIsValid(cgroup)) {
            if ((pointer = strchr(cgroup, ':')) == NULL) {
                isparent = true;
            } else if (gscgroup_is_timeshare(++pointer) > 0) {
                is_time_share = true;
            }

            rc = strncpy_s(scancgroup, NAMEDATALEN, NameStr(pool->control_group), NAMEDATALEN - 1);
            securec_check(rc, "\0", "\0");

            endptr = strrchr(scancgroup, ':');

            if (strchr(scancgroup, ':') != NULL && endptr && (strchr(scancgroup, ':') != endptr)) {
                *endptr = '\0';
            }

            /* ------------------------abnormal cases ------------ */
            /* case 1: multi_tenant redundant */
            if (strcmp(cgroup, scancgroup) == 0 && (isparent || (OidIsValid(pool->parentid) && !is_time_share))) {
                systable_endscan(scan);
                heap_close(relation, AccessShareLock);
                ereport(ERROR,
                    (errcode(ERRCODE_DUPLICATE_OBJECT),
                        errmsg("resource pool with control_group %s has been"
                               " existed in the two-layer resource pool list ",
                            cgroup)));
            }
            /* case 2: cgroup is 'class', finding 'class:wd1', but has no parent */
            if (gscgroup_is_child_group(cgroup, scancgroup) && (!OidIsValid(pool->parentid))) {
                systable_endscan(scan);
                heap_close(relation, AccessShareLock);
                ereport(ERROR,
                    (errcode(ERRCODE_DUPLICATE_OBJECT),
                        errmsg("cannot create resource pool with class \"%s\", it has been "
                               "existed in the normal resource pool list",
                            cgroup)));
            }

            /* -----------------------normal cases --------------- */
            /* multi_tenant parent resource pool */
            if (isparent) {
                if (!multi_tenant) {
                    multi_tenant = true;
                }
                if (strchr(scancgroup, ':') == NULL) {
                    sumpct += pool->mem_percent;
                }
            /* multi_tenant child resource pool  */
            } else {
                /* finding exsiting 'class' level */
                if (gscgroup_is_child_group(scancgroup, cgroup)) {
                    if (!multi_tenant) {
                        multi_tenant = true;
                    }
                    parentid = scanrpoid;
                    parentpct = scanmempct;
                }
                /* cannot judge whether it is multi_tenant now,
                 * if not, sumpct will be redundant */
                if (gscgroup_is_brother_group(cgroup, scancgroup)) {
                    sumpct += pool->mem_percent;
                }
            }
        }
    }

    systable_endscan(scan);

    heap_close(relation, AccessShareLock);

    if (find_ng == false && StringIsValid(cgroup) && strchr(cgroup, ':') == NULL) {
        multi_tenant = true;
    }

    if (multi_tenant) {
        if (*mempct == 0) {
            ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                    errmsg("mem_percent of two-layer resource pools cannot be 0")));
        }

        /* create resource pool with mem_percent not specified */
        if (*mempct == -1) {
            *mempct = DEFAULT_MULTI_TENANT_MEMPCT;
        }
    /* normal cases */
    } else if (*mempct == -1) {
        *mempct = DEFAULT_MEMORY_PERCENTAGE;
    }

    /* total mempercent of parent resource pool has to be lower than 100% */
    if (multi_tenant && ((sumpct += *mempct) > FULL_PERCENT)) {
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("memory percent value is beyond the available range.")));
    }

    if (parentoid != NULL) {
        *parentoid = parentid;
        *actpct = (parentpct * (*mempct)) / 100;
    } else {
        *actpct = *mempct;
    }
}

/*
 * @Description: check resource pool to alter control_group and mem_percent
 * @IN pool_name: pool name of the resource pool to be altered
 * @IN cgroup: alter resource pool control_group='cgroup'
 * @IN mempct: mem_percent to be altered
 * @See also:
 */
void CheckRespoolMultiTenantAlter(WLMNodeGroupInfo* ng, const char* pool_name, char* cgroup, int mempct, int* actpct)
{
    FormData_pg_resource_pool_real oldrpdata;

    bool alter_from_multi_tenant = false;
    bool rp_isparent = false;

    Oid rpoid = InvalidOid;

    int sumpct = 0;
    int parentpct = 0;
    char scancgroup[NAMEDATALEN] = {0};

    HeapTuple tup;

    /* get information of the pool */
    rpoid = get_resource_pool_oid(pool_name);

    errno_t rc =
        memset_s(&oldrpdata, sizeof(FormData_pg_resource_pool_real), 0, sizeof(FormData_pg_resource_pool_real));
    securec_check(rc, "\0", "\0");

    /* rpoid must be valid, or it has been reported in the beginning. */
    if (OidIsValid(rpoid) && (!get_resource_pool_param(rpoid, &oldrpdata))) {
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("resource pool information of \"%s\" is missing.", pool_name)));
    }
    /* no mempct is specified, get the old mempct */
    if (mempct == -1) {
        mempct = oldrpdata.mem_percent;
    }

    /* the resource pool to be altered is parent resource pool */
    if (strchr(NameStr(oldrpdata.control_group), ':') == NULL) {
        rp_isparent = true;
    }

    /* multi_tenant cannot altered between different groups */
    if (rp_isparent || OidIsValid(oldrpdata.parentid)) {
        alter_from_multi_tenant = true;
    }

    if (StringIsValid(cgroup) && alter_from_multi_tenant &&
        !gscgroup_is_brother_group(cgroup, NameStr(oldrpdata.control_group))) {
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("cannot alter control_group between different groups "
                       "or alter to a different layer. ")));
    }

    /* scan system table of all the resource pools */
    Relation relation = heap_open(ResourcePoolRelationId, AccessShareLock);
    SysScanDesc scan = systable_beginscan(relation, InvalidOid, false, NULL, 0, NULL);
    Datum groupanme_datum;
    bool isNull = false;
    char* groupanme = NULL;
    while (HeapTupleIsValid((tup = systable_getnext(scan)))) {
        Oid scanrpoid = HeapTupleGetOid(tup);
        Form_pg_resource_pool pool = (Form_pg_resource_pool)GETSTRUCT(tup);
        bool scanrp_isparent = false;
        char* pointer = NULL;
        char* endptr = NULL;
        bool scancgroup_is_timeshare = false;

        groupanme_datum = heap_getattr(tup, Anum_pg_resource_pool_nodegroup, RelationGetDescr(relation), &isNull);
        if (!isNull) {
            groupanme = (char*)pstrdup((const char*)DatumGetCString(groupanme_datum));
        } else {
            groupanme = (char*)pstrdup(DEFAULT_NODE_GROUP);
        }

        int strdiff = strcmp(ng->group_name, groupanme);
        pfree(groupanme);
        if (strdiff != 0) {
            continue;
        }

        /* check of resource pool relations skips its own */
        if (alter_from_multi_tenant && (scanrpoid == rpoid)) {
            sumpct += mempct;
            continue;
        }

        /* get cgroup of scanning resource pool */
        rc = strncpy_s(scancgroup, NAMEDATALEN, NameStr(pool->control_group), NAMEDATALEN - 1);
        securec_check(rc, "\0", "\0");

        if ((pointer = strchr(scancgroup, ':')) == NULL) {
            scanrp_isparent = true;
            if (OidIsValid(oldrpdata.parentid) && oldrpdata.parentid == scanrpoid) {
                parentpct = pool->mem_percent;
            }
        } else {
            if ((endptr = strrchr(scancgroup, ':')) != NULL && (endptr != pointer)) {
                *endptr = '\0';
            }

            if (gscgroup_is_timeshare(++pointer) > 0) {
                scancgroup_is_timeshare = true;
            }
        }

        /* check mempct of parent resource pool */
        if (rp_isparent != false) {
            if (scanrp_isparent) {
                sumpct += pool->mem_percent;
            }
        /* check cgroup and mempct of child resource pool and normal resource pool  */
        } else {
            if (StringIsValid(cgroup)) {
                /* multi_tenant resource pool to alter control_group */
                if (alter_from_multi_tenant && !scancgroup_is_timeshare && (strcmp(scancgroup, cgroup) == 0)) {
                    ereport(ERROR,
                        (errcode(ERRCODE_DUPLICATE_OBJECT),
                            errmsg("resource pool with control group \"%s\" already exists", cgroup)));
                }

                /* non-multi_tenant resource pool to alter control_group
                 * strchr(cgroup,':') == NULL ----alter to 'class'
                 * (scanrp_isparent
                 * && gscgroup_is_child_group(scanscgroup, cgroup))
                 * ----scanning parent resource pool
                 * and cgroup is child resource pool of the parent scancgroup.
                 */
                if (!alter_from_multi_tenant &&
                    (strchr(cgroup, ':') == NULL ||
                    (scanrp_isparent && gscgroup_is_child_group(scancgroup, cgroup)))) {
                    ereport(ERROR,
                        (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                            errmsg("cannot alter normal resource pool to the two-layer resource pool list.")));
                }
            }

            if (alter_from_multi_tenant && (oldrpdata.parentid == pool->parentid)) {
                sumpct += pool->mem_percent;
            }
        }
    }  
    
    if (sumpct > FULL_PERCENT) {
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("memory percent value is beyond the available range.")));
    }

    systable_endscan(scan);
    
    heap_close(relation, AccessShareLock);

    /* mempct cannot be altered to 0 */
    if (alter_from_multi_tenant && mempct == 0) {
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("mem_percent of two-layer resource pool cannot be altered to 0.")));
    }

    /* used for foreign respool */
    if (parentpct) {
        *actpct = parentpct * mempct / 100;
    } else {
        *actpct = mempct;
    }
}

/*
 * function name: CheckIoPriIsValid
 * description  : check io_priority is valid or not
 */
bool CheckIoPriIsValid(const char* io_pri)
{
    if (io_pri == NULL) {
        return false;
    }

    if (strcmp(io_pri, "High") != 0 && strcmp(io_pri, "Medium") != 0 && strcmp(io_pri, "Low") != 0 &&
        strcmp(io_pri, "None") != 0) {
        return false;
    }

    return true;
}
/*
 * function name: CheckResourcePoolOptions
 * description  : check the options of resource pool
 */
static void CheckResourcePoolOptions(const char* pool_name, List* options, bool is_create, Datum* values, bool* nulls,
    bool* repl, Oid* parentid, int* actpct, int* outpct)
{
    ListCell* option = NULL;
    int i;
    int rpname_cnt = Anum_pg_resource_pool_rpname;
    int cg_cnt = Anum_pg_resource_pool_control_group;
    int memp_cnt = Anum_pg_resource_pool_mem_percentage;
    int cpua_cnt = Anum_pg_resource_pool_cpu_affinity;
    int acts_cnt = Anum_pg_resource_pool_active_statements;
    int dop_cnt = Anum_pg_resource_pool_max_dop;
    int memlimit_cnt = Anum_pg_resource_pool_memory_limit;
    int parent_cnt = Anum_pg_resource_pool_parent;
    int iops_limits = Anum_pg_resource_pool_iops_limits;
    int io_priority = Anum_pg_resource_pool_io_priority;
    int nodegroup = Anum_pg_resource_pool_nodegroup;
    int foreign_users = Anum_pg_resource_pool_is_foreign;
    int max_worker = Anum_pg_resource_pool_max_worker;

    int mempct = -1;
    char memory[256] = {0};
    char* cgname = NULL;
    char cgroup[NAMEDATALEN] = {0};
    int is_parent = 0;
    errno_t rc = EOK;
    bool cgflag = false;
    const char* group_name = NULL;
    Oid group_oid = InvalidOid;
    char group_kind;
    bool is_foreign = false;
    bool is_foreign_changed = false;
    WLMNodeGroupInfo* ng = NULL;

    if (options == NULL && !is_create) {
        ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("No options specified")));
    }

    /* set the default value */
    for (i = 0; i < Natts_pg_resource_pool; ++i) {
        nulls[i] = true;
        repl[i] = false;
    }

    values[rpname_cnt - 1] = DirectFunctionCall1(namein, CStringGetDatum(pool_name));
    nulls[rpname_cnt - 1] = false;

    /* Filter options */
    foreach (option, options) {
        DefElem* defel = (DefElem*)lfirst(option);

        /* mem_percent checking */
        if (strcmp(defel->defname, "mem_percent") == 0) {
            if (!nulls[memp_cnt - 1]) {
                ereport(ERROR,
                    (errcode(ERRCODE_NULL_VALUE_NO_INDICATOR_PARAMETER), errmsg("redundant options: \"mem_percent\"")));
            }

            mempct = (int)defGetInt64(defel);

            /* check memory percent value */
            if ((mempct > ULIMITED_MEMORY_PERCENTAGE) || (mempct < DEFAULT_MEMORY_PERCENTAGE)) {
                ereport(ERROR,
                    (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
                        errmsg("mem_percent has to be in the range of 0-100.")));
            }

            values[memp_cnt - 1] = Int32GetDatum(mempct);
            nulls[memp_cnt - 1] = false;
            repl[memp_cnt - 1] = true;
        } else if (strcmp(defel->defname, "cpu_affinity") == 0) { /* cpu affinity checking */
            ereport(
                ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("cpu_affinity is not supported currently!")));
        } else if (strcmp(defel->defname, "active_statements") == 0) { /* active statements checking */
            if (!nulls[acts_cnt - 1]) {
                ereport(ERROR,
                    (errcode(ERRCODE_WITH_CHECK_OPTION_VIOLATION), errmsg("redundant options: \"active_statements\"")));
            }

            int act_statements = (int)defGetInt64(defel);

            if (act_statements < ULIMITED_ACT_STATEMENTS) {
                ereport(ERROR,
                    (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
                        errmsg("active_statements value can't be %d.", act_statements)));
            }

            values[acts_cnt - 1] = Int32GetDatum(act_statements);
            nulls[acts_cnt - 1] = false;
            repl[acts_cnt - 1] = true;
        } else if (strcmp(defel->defname, "control_group") == 0) { /* check the control_group option */
            if (!g_instance.wlm_cxt->gscgroup_config_parsed) {
                ereport(ERROR,
                    (errcode(ERRCODE_SYSTEM_ERROR),
                        errmsg("Failed to initialize Cgroup. "
                               "Please check Workload manager is enabled "
                               "and Cgroups have been created!")));
            }

            cgname = defGetString(defel);

            cgflag = true;                                 /* do the process later */
        } else if (strcmp(defel->defname, "max_dop") == 0) { /* max dop checking */
            int64 max_dop;

            if (!nulls[dop_cnt - 1]) {
                ereport(ERROR,
                    (errcode(ERRCODE_NULL_VALUE_NO_INDICATOR_PARAMETER), errmsg("redundant options: \"max_dop\"")));
            }

            max_dop = (int)defGetInt64(defel);

            if (max_dop < DEFAULT_DOP || max_dop > DEFAULT_MAX_DOP) {
                ereport(ERROR,
                    (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE), errmsg("max_dop value can't be %ld.", max_dop)));
            }

            values[dop_cnt - 1] = Int64GetDatum(max_dop);
            nulls[dop_cnt - 1] = false;
            repl[dop_cnt - 1] = true;
        } else if (strcmp(defel->defname, "memory_limit") == 0) { /* memory limit checking */
            ereport(WARNING, (errmsg("memory_limit is not available currently!")));

            if (!nulls[memlimit_cnt - 1]) {
                ereport(ERROR,
                    (errcode(ERRCODE_NULL_VALUE_NO_INDICATOR_PARAMETER),
                        errmsg("redundant options: \"memory_limit\"")));
            }

            CheckMemoryLimitOption(nulls[memlimit_cnt - 1], defGetString(defel), memory, sizeof(memory));

            values[memlimit_cnt - 1] = DirectFunctionCall1(namein, CStringGetDatum(memory));
            nulls[memlimit_cnt - 1] = false;
            repl[memlimit_cnt - 1] = true;
        } else if (strcmp(defel->defname, "parent") == 0) {
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("parent is not supported currently!")));
        } else if (strcmp(defel->defname, "io_limits") == 0) {
            if (!nulls[iops_limits - 1]) {
                ereport(ERROR,
                    (errcode(ERRCODE_NULL_VALUE_NO_INDICATOR_PARAMETER), errmsg("redundant options: \"io_limits\"")));
            }

            int iops_limit_value = (int)defGetInt64(defel);

            if (iops_limit_value < DEFAULT_IOPS_LIMITS) {
                ereport(ERROR,
                    (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE), errmsg("io_limits can't be %d.", iops_limit_value)));
            }

            values[iops_limits - 1] = Int32GetDatum(iops_limit_value);
            nulls[iops_limits - 1] = false;
            repl[iops_limits - 1] = true;
        } else if (strcmp(defel->defname, "io_priority") == 0) {
            if (!nulls[io_priority - 1]) {
                ereport(ERROR,
                    (errcode(ERRCODE_NULL_VALUE_NO_INDICATOR_PARAMETER), errmsg("redundant options: \"io_priority\"")));
            }

            char* io_pri = defGetString(defel);

            if (!CheckIoPriIsValid(io_pri)) {
                ereport(ERROR,
                    (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("io_priority can only be named as "
                               "\"High\", \"Low\", \"Medium\" and \"None\".")));
            }

            values[io_priority - 1] = DirectFunctionCall1(namein, CStringGetDatum(io_pri));
            nulls[io_priority - 1] = false;
            repl[io_priority - 1] = true;
        } else if (strcmp(defel->defname, "nodegroup") == 0) { /* node group option */
            if (!nulls[nodegroup - 1]) {
                ereport(ERROR,
                    (errcode(ERRCODE_NULL_VALUE_NO_INDICATOR_PARAMETER), errmsg("redundant options: \"nodegroup\"")));
            }

            group_name = defGetString(defel);
            if (group_name == NULL) {
                ereport(ERROR, (errcode(ERRCODE_INVALID_NAME), errmsg("The node group name is invalid")));
            }

            if (IS_PGXC_COORDINATOR) {
                bool to_installation = false;
                if (!in_logic_cluster()) {
                    ereport(ERROR,
                        (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                            errmsg("Do not support to create resource pool in non-logic cluster mode.")));
                }

                if (strcmp(group_name, CNG_OPTION_INSTALLATION) != 0) {
                    group_oid = get_pgxc_groupoid(group_name);
                    if (!OidIsValid(group_oid)) {
                        ereport(ERROR,
                            (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("The node group %s is not exist.", group_name)));
                    }
                } else {
                    to_installation = true;
                    group_oid = ng_get_installation_group_oid();
                }

                if (!superuser()) {
                    /* I am administrator of my logic cluster, the group oid should be myself logic cluster. */
                    if (get_current_lcgroup_oid() != group_oid) {
                        ereport(ERROR,
                            (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                                errmsg("Only create resource pool in self logic cluster.")));
                    }
                }
                group_kind = get_pgxc_groupkind(group_oid);
                if (group_kind != PGXC_GROUPKIND_LCGROUP && !to_installation) {
                    ereport(ERROR,
                        (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                            errmsg("The node group %s is not a logic cluster.", group_name)));
                }
            }

            values[nodegroup - 1] = DirectFunctionCall1(namein, CStringGetDatum(group_name));
            nulls[nodegroup - 1] = false;
            repl[nodegroup - 1] = true;

        } else if (strcmp(defel->defname, "is_foreign") == 0) { /* foreign users option */
            if (!nulls[foreign_users - 1]) {
                ereport(ERROR,
                    (errcode(ERRCODE_NULL_VALUE_NO_INDICATOR_PARAMETER),
                        errmsg("redundant options: \"is_foreign\"")));
            }

            is_foreign = defGetBoolean(defel);
            values[foreign_users - 1] = BoolGetDatum(is_foreign);
            nulls[foreign_users - 1] = false;
            repl[foreign_users - 1] = true;
            is_foreign_changed = true;
        } else if (strcmp(defel->defname, "max_worker") == 0){
            if (!nulls[max_worker - 1]) {
                ereport(ERROR, (errcode(ERRCODE_NULL_VALUE_NO_INDICATOR_PARAMETER),
                    errmsg("redundant options: \"max_worker\"")));
            }
        
            int max_workers = (int)defGetInt64(defel);
        
            if (max_workers < DEFAULT_WORKER || max_workers > DEFAULT_MAX_WORKER) {
                ereport(ERROR, (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
                    errmsg("max_worker can't be %d.", max_workers)));
            }
        
            values[max_worker - 1] = Int32GetDatum(max_workers);
            nulls[max_worker - 1] = false;
            repl[max_worker - 1] = true;
        } else {
            ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("incorrect option: %s", defel->defname)));
        }
    }

    /* must specified node group when in logic cluster mode */
    if (IS_PGXC_COORDINATOR && is_create && is_lcgroup_admin()) {
        if (group_name == NULL) {
            group_name = get_current_lcgroup_name();
            if (group_name != NULL) {
                values[nodegroup - 1] = DirectFunctionCall1(namein, CStringGetDatum(group_name));
                nulls[nodegroup - 1] = false;
                repl[nodegroup - 1] = true;
            }
        }
        if (group_name == NULL) {
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("Must specify nodegroup option when creating resource pool in logic cluster mode.")));
        }
    }

    if (IS_PGXC_COORDINATOR && is_create && in_logic_cluster() && group_oid == InvalidOid && group_name == NULL) {
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("Must specify nodegroup option when creating resource pool in logic cluster mode.")));
    }

    if (IS_PGXC_COORDINATOR && is_create && is_foreign && !in_logic_cluster()) {
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("Must specify is_foreign option when creating resource pool in logic cluster mode.")));
    }

    /* get the node group name when altering resource pool */
    if (!is_create) {
        Oid poolid = get_resource_pool_oid(pool_name);
        if (!OidIsValid(poolid)) {
            ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("Resource Pool \"%s\": object not defined", pool_name)));
        }

        char* alter_group_name = get_resource_pool_ngname(poolid);

        /* only alter between installation and vcgroup */
        if (group_name != NULL && *group_name && strcmp(group_name, alter_group_name) != 0 &&
            strcmp(group_name, CNG_OPTION_INSTALLATION) != 0 &&
            strcmp(alter_group_name, CNG_OPTION_INSTALLATION) != 0) {
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("Can't alter node group option when altering resource pool between different logical "
                           "clusters.")));
        }

        if (group_name == NULL) {
            group_name = alter_group_name;
        }

        bool alter_is_foreign = is_resource_pool_foreign(poolid);
        if (is_foreign_changed && (is_foreign != alter_is_foreign)) {
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("Can't modify is_foreign option when altering resource pool.")));
        }
    }

    if (IS_PGXC_COORDINATOR) {
        /* get the node group information */
        ng = WLMGetNodeGroupFromHTAB(group_name);
        if (NULL == ng) {
            ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_OBJECT),
                    errmsg("Can't get the %s logic cluster information by hash table.", group_name)));
        }

        if (is_foreign && ng->foreignrp) {
            ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                    errmsg("Only one resource pool with is_foreign option is permitted for one logical cluster. "
                           "The resource pool '%u' has been created for foreign users.",
                        ng->foreignrp->rpoid)));
        }
    } else {
        /* get the node group information */
        ng = WLMMustGetNodeGroupFromHTAB(group_name);
        if (NULL == ng) {
            ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_OBJECT),
                    errmsg("Can't get the %s logic cluster information by hash table.", group_name)));
        }
    }

    u_sess->wlm_cxt->respool_node_group = ng;

    /* Check if the cgroup name is valid */
    if (true == cgflag) {
        /* check if the cgroup name is valid */
        if ((is_parent = gscgroup_check_group_name(ng, cgname)) == -1) {
            ereport(ERROR, (errcode(ERRCODE_INVALID_NAME), errmsg("invalid control group: %s", cgname)));
        }

        if (is_parent && is_foreign) {
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("Can't create parent resource pool used for foreign users.")));
        }

        if (is_parent) {
            gscgroup_update_hashtbl(ng, cgname);
        } else {
            gscgroup_update_hashtbl(ng, u_sess->wlm_cxt->group_keyname);
        }

        rc = strncpy_s(cgroup, NAMEDATALEN, cgname, NAMEDATALEN - 1);
        securec_check(rc, "\0", "\0");
        if (is_parent) {
            rc = strncpy_s(cgroup, NAMEDATALEN, cgname, NAMEDATALEN - 1);
            securec_check(rc, "\0", "\0");
        } else {
            rc = strncpy_s(cgroup, NAMEDATALEN, u_sess->wlm_cxt->group_keyname, NAMEDATALEN - 1);
            securec_check(rc, "\0", "\0");
        }

        values[cg_cnt - 1] = DirectFunctionCall1(namein, CStringGetDatum(cgroup));
        nulls[cg_cnt - 1] = false;
        repl[cg_cnt - 1] = true;
    }

    /*
     * check whether resource pool is normal or multi_tenant,
     * give the restrictions of the newly created resource pool
     * with control_group and mem_percent,
     * and get the default mem_percent here
     */
    if (is_create) {
        CheckRespoolMultiTenantCreate(ng, cgroup, &mempct, parentid, actpct);
    /* check whether alter resource pool with control_group and mem_percent is allowed. */
    } else {
        CheckRespoolMultiTenantAlter(ng, pool_name, cgroup, mempct, actpct);
    }
    /* parentid is got from the multi_tenant check process */
    if (OidIsValid(*parentid)) {
        values[parent_cnt - 1] = ObjectIdGetDatum(*parentid);
        nulls[parent_cnt - 1] = false;
        repl[parent_cnt - 1] = true;
    }
    if (mempct > 0 && *memory) {
        ereport(NOTICE,
            (errmsg("Only mem_percent is valid when specify both "
                    "mem_percent and memory_limit. ")));
    }

    *outpct = mempct;

    if (is_create) {
        if (nulls[memp_cnt - 1]) {
            values[memp_cnt - 1] = Int32GetDatum(mempct);
            nulls[memp_cnt - 1] = false;
        }

        if (nulls[cpua_cnt - 1]) {
            values[cpua_cnt - 1] = Int64GetDatum(ULIMITED_CPU_AFFINITY);
            nulls[cpua_cnt - 1] = false;
        }

        if (nulls[acts_cnt - 1]) {
            values[acts_cnt - 1] = Int32GetDatum(DEFAULT_ACT_STATEMENTS);
            nulls[acts_cnt - 1] = false;
        }

        if (nulls[dop_cnt - 1]) {
            values[dop_cnt - 1] = Int32GetDatum(DEFAULT_DOP);
            nulls[dop_cnt - 1] = false;
        }

        if (nulls[cg_cnt - 1]) {
            values[cg_cnt - 1] = DirectFunctionCall1(namein, CStringGetDatum(DEFAULT_CONTROL_GROUP));
            nulls[cg_cnt - 1] = false;
        }

        if (nulls[memlimit_cnt - 1]) {
            values[memlimit_cnt - 1] = DirectFunctionCall1(namein, CStringGetDatum(DEFAULT_MEMORY_LIMIT));
            nulls[memlimit_cnt - 1] = false;
        }

        if (nulls[parent_cnt - 1]) {
            values[parent_cnt - 1] = ObjectIdGetDatum(InvalidOid);
            nulls[parent_cnt - 1] = false;
        }

        /* iops_limits is not in control by default */
        if (nulls[iops_limits - 1]) {
            values[iops_limits - 1] = Int32GetDatum(DEFAULT_IOPS_LIMITS);
            nulls[iops_limits - 1] = false;
        }

        if (nulls[io_priority - 1]) {
            values[io_priority - 1] = DirectFunctionCall1(namein, CStringGetDatum(DEFAULT_IO_PRIORITY));
            nulls[io_priority - 1] = false;
        }

        if (nulls[nodegroup - 1]) {
            values[nodegroup - 1] = DirectFunctionCall1(namein, CStringGetDatum(DEFAULT_NODE_GROUP));
            nulls[nodegroup - 1] = false;
        }

        if (nulls[foreign_users - 1]) {
            values[foreign_users - 1] = BoolGetDatum(is_foreign);
            nulls[foreign_users - 1] = false;
        }

        if (nulls[max_worker - 1]) {
            values[max_worker - 1] = Int32GetDatum(DEFAULT_WORKER);
            nulls[max_worker - 1] = false;
        }
    }
}

/*
 * function name: CheckWorkloadGroupOptions
 * description  : check the options of workload group
 */
static void CheckWorkloadGroupOptions(const char* group_name, const char* pool_name, List* options, bool is_create,
    Datum* values, bool* nulls, bool* repl)
{
    ListCell* option = NULL;
    int i;
    int wgname_cnt = Anum_pg_workload_group_wgname;
    int rgoid_cnt = Anum_pg_workload_group_rgoid;
    int act_cnt = Anum_pg_workload_group_acstatements;

    /* initialize arrrys */
    for (i = 0; i < Natts_pg_workload_group; ++i) {
        nulls[i] = true;
        repl[i] = false;
    }

    /* set group name feild.this is useful for create cmd only. */
    nulls[wgname_cnt - 1] = false;
    values[wgname_cnt - 1] = DirectFunctionCall1(namein, CStringGetDatum(group_name));

    /* check resource pool setting */
    if (pool_name != NULL) {
        Oid pool_oid = get_resource_pool_oid(pool_name);

        if (!OidIsValid(pool_oid)) {
            ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("Resource Pool \"%s\": object not defined", pool_name)));
        }

        values[rgoid_cnt - 1] = ObjectIdGetDatum(pool_oid);
        nulls[rgoid_cnt - 1] = false;
        repl[rgoid_cnt - 1] = true;
    } else {
        if (is_create) {
            nulls[rgoid_cnt - 1] = false;
            values[rgoid_cnt - 1] = ObjectIdGetDatum(DEFAULT_RESOURCE_POOL);
        } else {
            repl[rgoid_cnt - 1] = false;
        }
        ereport(DEBUG1, (errmsg("check create/alter workload group.")));
    }

    /* check with options */
    /* alter cmd must set with options if the resource pool is not set. */
    if (options == NULL && !is_create && nulls[rgoid_cnt - 1]) {
        ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("No options specified")));
    }

    foreach (option, options) {
        DefElem* defel = (DefElem*)lfirst(option);

        if (strcmp(defel->defname, "act_statements") == 0) {
            int active_statements;

            if (!nulls[act_cnt - 1]) {
                ereport(ERROR, (errcode(ERRCODE_NULL_VALUE_NO_INDICATOR_PARAMETER), errmsg("redundant options")));
            }

            active_statements = (int)defGetInt64(defel);
            if (active_statements < 0) {
                ereport(
                    ERROR, (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE), errmsg("act_statements must be positive")));
            }

            values[act_cnt - 1] = Int32GetDatum(active_statements);
            nulls[act_cnt - 1] = false;
            repl[act_cnt - 1] = true;
        } else {
            ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("incorrect option: %s", defel->defname)));
        }
    }

    if (nulls[act_cnt - 1]) {
        if (is_create) {
            nulls[act_cnt - 1] = false;
            values[act_cnt - 1] = Int32GetDatum(DEFAULT_ACTIVE_STATEMENTS);
        } else {
            repl[act_cnt - 1] = false;
        }
    }
}

/*
 * function name: CheckAppWorkloadGroupMappingOptions
 * description  : check the options of application mapping
 */
static void CheckAppWorkloadGroupMappingOptions(
    const char* app_name, List* options, bool is_create, Datum* values, bool* nulls, bool* repl)
{
    ListCell* option = NULL;
    int i;
    int app_cnt = Anum_pg_app_workloadgroup_mapping_appname;
    int wg_cnt = Anum_pg_app_workloadgroup_mapping_wgname;

    if (options == NULL && !is_create) {
        ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("No options specified")));
    }

    for (i = 0; i < Natts_pg_app_workloadgroup_mapping; ++i) {
        nulls[i] = true;
        repl[i] = false;
    }

    values[app_cnt - 1] = DirectFunctionCall1(namein, CStringGetDatum(app_name));
    nulls[app_cnt - 1] = false;

    /* Filter options */
    foreach (option, options) {
        DefElem* defel = (DefElem*)lfirst(option);

        if (strcmp(defel->defname, "workload_gpname") == 0) {
            char* wg_name = NULL;
            Oid wg_oid;

            if (!nulls[wg_cnt - 1]) {
                ereport(ERROR, (errcode(ERRCODE_NULL_VALUE_NO_INDICATOR_PARAMETER), errmsg("redundant options")));
            }

            wg_name = defGetString(defel);

            if (NULL == wg_name) {
                ereport(ERROR, (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED), errmsg("read workload_gpname failed.")));
            }

            wg_oid = get_workload_group_oid(wg_name);

            if (!OidIsValid(wg_oid)) {
                ereport(ERROR,
                    (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("Workload Group \"%s\": object not defined", wg_name)));
            }

            values[wg_cnt - 1] = DirectFunctionCall1(namein, CStringGetDatum(wg_name));
            nulls[wg_cnt - 1] = false;
            repl[wg_cnt - 1] = true;
        } else {
            ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("incorrect option: %s", defel->defname)));
        }
    }

    if (nulls[wg_cnt - 1]) {
        if (is_create) {
            values[wg_cnt - 1] = DirectFunctionCall1(namein, CStringGetDatum(DEFAULT_WORKLOAD_GROUP));
            nulls[wg_cnt - 1] = false;
        } else {
            repl[wg_cnt - 1] = false;
        }
    }
}

/*
 * function name: GetResourcePoolActPts
 * description  : get resource pool active points
 * input        : resource pool data
 * output       : active points
 */
void GetResourcePoolActPts(int memsize, RespoolData* rpdata, int* result)
{
    Assert(result != NULL);
    result[0] = memsize / KBYTES;
    if (rpdata->max_pts <= 0 || rpdata->mem_size == 0 || memsize <= 0) {
        result[1] = FULL_PERCENT;
        return;
    }

    if (memsize > rpdata->mem_size) {
        /* use 90% to protect system */
        result[0] = (int)((rpdata->mem_size * 0.9) / KBYTES);
        result[1] = (int)(rpdata->max_pts * 0.9 + 1);
        return;
    }

    /* statement_mem is set, we compute how many points the query will use */
    int pts = ((long)memsize * rpdata->max_pts / rpdata->mem_size + 1);

    /* we must make sure the points query used is not less than basic value: 100 */
    if (pts <= 0) {
        pts = 1;
    }
    result[1] = pts;
}

/*
 * *************** EXTERNAL FUNCTION ********************************
 */
/*
 * function name: InitializeWorkloadManager
 * description  : initialize the memory context, Cgroup configuration and
 *                workload group hash table etc.
 */
void InitializeWorkloadManager(void)
{
    errno_t rc;

    g_instance.wlm_cxt->workload_manager_mcxt = AllocSetContextCreate((MemoryContext)g_instance.instance_context,
        "Workload manager memory context",
        ALLOCSET_DEFAULT_MINSIZE,
        ALLOCSET_DEFAULT_INITSIZE,
        ALLOCSET_DEFAULT_MAXSIZE,
        SHARED_CONTEXT);
    g_instance.wlm_cxt->query_resource_track_mcxt =
        AllocSetContextCreate((MemoryContext)g_instance.wlm_cxt->workload_manager_mcxt,
            "Query resource track memory context",
            ALLOCSET_DEFAULT_MINSIZE,
            ALLOCSET_DEFAULT_INITSIZE,
            ALLOCSET_DEFAULT_MAXSIZE,
            SHARED_CONTEXT);
    g_instance.wlm_cxt->oper_resource_track_mcxt =
        AllocSetContextCreate((MemoryContext)g_instance.wlm_cxt->workload_manager_mcxt,
            "Operator resource track memory context",
            ALLOCSET_DEFAULT_MINSIZE,
            ALLOCSET_DEFAULT_INITSIZE,
            ALLOCSET_DEFAULT_MAXSIZE,
            SHARED_CONTEXT);

    USE_MEMORY_CONTEXT(g_instance.wlm_cxt->workload_manager_mcxt);

    /* initialize user resource pool hash table */
    InitializeUserResourcePoolHashTable();

    WLMInitializeStatInfo();

    errno_t errval = strncpy_s(g_instance.wlm_cxt->MyDefaultNodeGroup.group_name,
        sizeof(g_instance.wlm_cxt->MyDefaultNodeGroup.group_name),
        DEFAULT_NODE_GROUP,
        sizeof(g_instance.wlm_cxt->MyDefaultNodeGroup.group_name) - 1);
    securec_check_errval(errval, , LOG);

    WLMInitNodeGroupInfo(&g_instance.wlm_cxt->MyDefaultNodeGroup);

    gscgroup_init();

    /* Init the variable */
    rc = memset_s(u_sess->wlm_cxt->control_group,
        sizeof(u_sess->wlm_cxt->control_group),
        0,
        sizeof(u_sess->wlm_cxt->control_group));
    securec_check_errno(rc, , );

    InitOperStatProfile();

    /* workload manager is off, need not do set control group */
    if (!ENABLE_WORKLOAD_CONTROL) {
        return;
    }

    if (g_instance.attr.attr_resource.enable_dynamic_workload) {
        if (t_thrd.utils_cxt.gs_mp_inited) {
            g_instance.wlm_cxt->dynamic_workload_inited = true;
        } else {
            ereport(WARNING, (errmsg("disable dynamic workload, due to memory limit is not set")));
        }
    }

    /* if cgroup is OK, attach postmaster to top group */
    if (g_instance.wlm_cxt->gscgroup_init_done > 0 && AmPostmasterProcess()) {
        (void)gscgroup_attach_backend_task(GSCGROUP_TOP_DATABASE, false);
    }
}

/*
 * @Description: get memory size from the resource pool
 * @IN pct: the memory percent in resource pool
 * @IN parentid: parent resource pool id
 * @Return: memory size, the value is:
 *          memory_size = total_memory * parent_percent * user_percent
 * @See also:
 */
unsigned int GetRPMemorySize(int pct, Oid parentid)
{
    /* get current max chunks per process, convert it to 'kB' */
    unsigned int memsize = (t_thrd.wlm_cxt.thread_srvmgr->freesize_update > 0) ? t_thrd.wlm_cxt.thread_srvmgr->totalsize
                                                                             : RESPOOL_MIN_MEMORY;

    /* compute parent memory size while parent is valid */
    if (OidIsValid(parentid)) {
        FormData_pg_resource_pool_real rpdata;

        if (get_resource_pool_param(parentid, &rpdata) == NULL) {
            ereport(ERROR,
                (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                    errmsg("cannot get resource pool information of %u", parentid)));
        }

        memsize = (rpdata.mem_percent * memsize) / FULL_PERCENT;
    }

    /* compute user memory size */
    memsize = (memsize * pct) / FULL_PERCENT;

    return memsize << BITS_IN_KB;
}

/*
 * function name: GetIoPriority
 * description  : transfer io_priority from string to IO percentage
 */
int GetIoPriority(const char* io_pri)
{
    if (strcmp(io_pri, "None") == 0) {
        return IOPRIORITY_NONE;
    }

    if (strcmp(io_pri, "Low") == 0) {
        return IOPRIORITY_LOW;
    }

    if (strcmp(io_pri, "Medium") == 0) {
        return IOPRIORITY_MEDIUM;
    }

    if (strcmp(io_pri, "High") == 0) {
        return IOPRIORITY_HIGH;
    }

    return IOPRIORITY_NONE;
}

/*
 * @Description: set resource pool info for rpoid
 * @IN roid    : resource poopl id
 * @Return     : void
 * @See also:
 */
void WLMSetRespoolInfo(Oid rpoid, const char* cgroup_old)
{
    errno_t rc;
    WLMGeneralParam* g_wlm_params = &u_sess->wlm_cxt->wlm_params;

    g_wlm_params->rpdata.cgroup = DEFAULT_CONTROL_GROUP;

    rc = memset_s(
        g_wlm_params->rpdata.rpname, sizeof(g_wlm_params->rpdata.rpname), 0, sizeof(g_wlm_params->rpdata.rpname));
    securec_check_errval(rc, , LOG);

    FormData_pg_resource_pool_real rpdata;

    CGSwitchState cgroup_state_reserved = u_sess->wlm_cxt->cgroup_state;

    /*
     * if the resource pool or the control group in the
     * resource pool is invalid, we will use the default cgroup.
     */
    if (OidIsValid(rpoid) && get_resource_pool_param(rpoid, &rpdata)) {
        g_wlm_params->rpdata.rpoid = rpoid;
        g_wlm_params->rpdata.max_pts = rpdata.active_statements * FULL_PERCENT;
        // max statements for simple query is derived from dop, short for degree of parallel
        g_wlm_params->rpdata.max_stmt_simple = rpdata.max_dop;
        g_wlm_params->rpdata.max_dop = rpdata.max_dop;

        errno_t errval = strncpy_s(g_wlm_params->rpdata.rpname,
            sizeof(g_wlm_params->rpdata.rpname),
            NameStr(rpdata.respool_name),
            sizeof(g_wlm_params->rpdata.rpname) - 1);
        securec_check_errval(errval, , LOG);

        /*
         * We have got the control group from
         * the resource pool and it's valid, it's OK to use.
         */
        if (gscgroup_check_group_name(t_thrd.wlm_cxt.thread_node_group, NameStr(rpdata.control_group)) != -1) {
            g_wlm_params->rpdata.cgroup = u_sess->wlm_cxt->group_keyname;
        }

        // The control group in the resource pool is invalid or has changed?
        if (g_instance.wlm_cxt->gscgroup_init_done && strcmp(cgroup_old, g_wlm_params->rpdata.cgroup) != 0) {
            g_wlm_params->rpdata.cgchange = true;
        } else {
            g_wlm_params->rpdata.cgchange = false;
        }

        rc = snprintf_s(g_wlm_params->cgroup,
            sizeof(g_wlm_params->cgroup),
            sizeof(g_wlm_params->cgroup) - 1,
            "%s",
            g_wlm_params->rpdata.cgroup);
        securec_check_ss(rc, "\0", "\0");

        /*
         * If the control group is invalid, we will use
         * the default group and make a notice only once.
         */
        bool isCgroupValid = (g_instance.wlm_cxt->gscgroup_init_done && g_wlm_params->rpdata.cgchange &&
            !CGroupIsValid(u_sess->wlm_cxt->group_keyname));
        if (isCgroupValid) {
            ereport(NOTICE,
                (errmsg("Cgroup \"%s\" in the resource pool is invalid, "
                        "Cgroup \"%s\" will be used.",
                    NameStr(rpdata.control_group),
                    g_wlm_params->rpdata.cgroup)));
        }

        /* if memory percent is valid, we will use this as memory limit size */
        if (maxChunksPerProcess > 0) {
            g_wlm_params->rpdata.mem_size = GetRPMemorySize(rpdata.mem_percent, rpdata.parentid);
        }

        /* get the memsize and its act_pts */
        int result[2] = {0};
        GetResourcePoolActPts(u_sess->attr.attr_sql.statement_mem, &g_wlm_params->rpdata, result);

        g_wlm_params->memsize = result[0];
        g_wlm_params->rpdata.act_pts = result[1];

        if (u_sess->wlm_cxt->session_respool[0] != '\0' &&
            strcmp(u_sess->wlm_cxt->session_respool, INVALID_POOL_NAME) != 0) {
            u_sess->wlm_cxt->cgroup_state = CG_RESPOOL;

            g_wlm_params->rpdata.iops_limits = rpdata.iops_limits;

            if (StringIsValid(NameStr(rpdata.io_priority))) {
                g_wlm_params->rpdata.io_priority = GetIoPriority(NameStr(rpdata.io_priority));
            }
        }

        errval = strncpy_s(g_wlm_params->rpdata.rpname,
            sizeof(g_wlm_params->rpdata.rpname) - 1,
            NameStr(rpdata.respool_name),
            sizeof(g_wlm_params->rpdata.rpname) - 1);
        securec_check_errval(errval, , LOG);

    } else {
        u_sess->wlm_cxt->cgroup_state = cgroup_state_reserved;

        ereport(ERROR,
            (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("cache lookup failed for resource pool id %u, it is removed?", rpoid)));
    }

    if (g_instance.wlm_cxt->gscgroup_init_done) {
        /*
         * If we do not set control group, and control group in
         * the resource pool is changed, we must use the new group
         */
        bool isUseNewGroup = ((u_sess->wlm_cxt->cgroup_state == CG_USING ||
            u_sess->wlm_cxt->cgroup_state == CG_RESPOOL) && g_wlm_params->rpdata.cgchange);
        if (isUseNewGroup) {
            WLMSetControlGroup(g_wlm_params->rpdata.cgroup);
        }

        // If do not set any control group, we will use the control group in resource pool.
        if (u_sess->wlm_cxt->cgroup_state == CG_ORIGINAL) {
            SetConfigOption("cgroup_name", g_wlm_params->rpdata.cgroup, PGC_USERSET, PGC_S_CLIENT);
        }
    } else {
        // Cgroup does not init, we will set the control group to invalid group.
        if (u_sess->wlm_cxt->cgroup_state == CG_ORIGINAL) {
            WLMSetControlGroup(GSCGROUP_INVALID_GROUP);
        } else {
            u_sess->wlm_cxt->cgroup_state = CG_USING;
        }
    }

    if (u_sess->wlm_cxt->cgroup_state == CG_RESPOOL) {
        u_sess->wlm_cxt->cgroup_state = cgroup_state_reserved;
    }

    // set the value of dop between different params
    g_wlm_params->dopvalue = g_wlm_params->rpdata.max_dop;
}

/*
 * @Description: initialize the node group information
 * @IN info    : node group information
 * @Return     : void
 * @See also:
 */
void WLMInitNodeGroupInfo(WLMNodeGroupInfo* info)
{
    Assert(info != NULL);

    info->used_memory = 0;
    info->total_memory = 0;
    info->estimate_memory = 0;
    info->min_freesize = 0;
    info->used = 1;

    dywlm_client_init(info);

    dywlm_server_init(info);

    WLMParctlInit(info);

    errno_t errval = memset_s(info->vaddr, sizeof(info->vaddr), 0, sizeof(info->vaddr));
    securec_check_errval(errval, , LOG);

    info->cgroups_htab = NULL;

    info->node_list = NULL;

    info->is_dirty = false;
}

/*
 * @Description: check and get the node group information
 * @IN group_name : node group name
 * @Return     : void
 * @See also:
 */
WLMNodeGroupInfo* WLMCheckAndGetNodeGroup(const char* group_name)
{
    if (NodeGroupIsDefault(group_name)) {
        return &g_instance.wlm_cxt->MyDefaultNodeGroup;
    }

    /* get the node group from hash table */
    WLMNodeGroupInfo* ng = WLMGetNodeGroupFromHTAB(group_name);
    WLMGeneralParam* g_wlm_params = &u_sess->wlm_cxt->wlm_params;

    Oid groupid = get_pgxc_groupoid(group_name);

    if (OidIsValid(groupid)) {
        if (ng == NULL || !ng->used) {
            if (ng == NULL) {
                USE_AUTO_LWLOCK(WorkloadNodeGroupLock, LW_EXCLUSIVE);

                ng = (WLMNodeGroupInfo*)hash_search(
                    g_instance.wlm_cxt->stat_manager.node_group_hashtbl, g_wlm_params->ngroup, HASH_ENTER_NULL, NULL);

                if (ng == NULL) {
                    ereport(ERROR,
                        (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                            errmsg("alloc memory for node group in htab failed")));
                }
            }

            errno_t errval =
                strncpy_s(ng->group_name, sizeof(ng->group_name), g_wlm_params->ngroup, sizeof(ng->group_name) - 1);
            securec_check_errval(errval, , LOG);

            WLMInitNodeGroupInfo(ng);

            if (pg_strcasecmp(VNG_OPTION_ELASTIC_GROUP, group_name) != 0 && -1 == gscgroup_map_node_group(ng)) {
                ereport(ERROR,
                    (errcode(ERRCODE_UNDEFINED_OBJECT),
                        errmsg("failed to map the control group file for logic cluster %s during checking. "
                               "Please check if it has been created.",
                            ng->group_name)));
            }

            gs_lock_test_and_set((int*)&ng->used, 1);
        }

        return ng;
    } else { /* group id is not valid */
        if (ng != NULL && ng->used) {
            gs_lock_test_and_set((int*)&ng->used, 0);
        }

        ereport(ERROR,
            (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("node group %s is removed", g_wlm_params->ngroup)));
    }

    return ng;
}

/*
 * function name: WLMSetUserInfo
 * description  : set user info before query executing
 */
void WLMSetUserInfo(void)
{
    bool issuper = false;
    Oid rpoid = InvalidOid;
    Oid user_rpoid = InvalidOid;
    WLMGeneralParam* g_wlm_params = &u_sess->wlm_cxt->wlm_params;

    /* set the default node group */
    t_thrd.wlm_cxt.thread_node_group = &g_instance.wlm_cxt->MyDefaultNodeGroup;

    Assert((IS_PGXC_COORDINATOR && !IsConnFromCoord()) || IS_SINGLE_NODE);

    /* we need not set user info for special query */
    if (t_thrd.wlm_cxt.parctl_state.special) {
        return;
    }

    char cgroup_old[NAMEDATALEN] = {0};

    errno_t errval = memcpy_s(cgroup_old, NAMEDATALEN, g_wlm_params->cgroup, NAMEDATALEN);
    securec_check_errval(errval, , LOG);

    GetUserDataFromCatalog(
        GetUserId(), &user_rpoid, NULL, &issuper, NULL, NULL, NULL, g_wlm_params->ngroup, NAMEDATALEN);

    if ('\0' == *g_wlm_params->ngroup) {
        errno_t errval = strncpy_s(g_wlm_params->ngroup, NAMEDATALEN, CNG_OPTION_INSTALLATION, NAMEDATALEN - 1);
        securec_check_errval(errval, , LOG);
    }
    t_thrd.wlm_cxt.thread_node_group = WLMCheckAndGetNodeGroup(g_wlm_params->ngroup);

    if (t_thrd.wlm_cxt.thread_node_group == NULL) {
        ereport(ERROR,
            (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                errmsg("get node group failed for group name %s", g_wlm_params->ngroup)));
    }

    t_thrd.wlm_cxt.thread_climgr = &t_thrd.wlm_cxt.thread_node_group->climgr;
    t_thrd.wlm_cxt.thread_srvmgr = &t_thrd.wlm_cxt.thread_node_group->srvmgr;

    g_wlm_params->rpdata.superuser = issuper;

    if (u_sess->wlm_cxt->session_respool[0] != '\0' &&
        strcmp(u_sess->wlm_cxt->session_respool, INVALID_POOL_NAME) != 0) {
        rpoid = get_resource_pool_oid(u_sess->wlm_cxt->session_respool);

        if (OidIsValid(rpoid)) {
            WLMSetRespoolInfo(rpoid, cgroup_old);
        } else {
            ereport(LOG, (errmsg("session_respool \"%s\" does not exist ",
                            u_sess->wlm_cxt->session_respool)));
        }
    }

    if (!OidIsValid(rpoid)) {
        WLMSetRespoolInfo(user_rpoid, cgroup_old);
    }
}

/*
 * function name: WLMIsSpecialCommand
 * description  : Check command is special command,
 *                it's used by switching cgroup.
 */
gscgroup_stmt_t WLMIsSpecialCommand(const Node* parsetree, const Portal portal)
{
    if (!u_sess->attr.attr_resource.enable_cgroup_switch || parsetree == NULL) {
        return GSCGROUP_NORMAL_STMT;
    }

    gscgroup_stmt_t stmt = GSCGROUP_TOP_STMT;

    if (nodeTag(parsetree) == T_VacuumStmt && (((VacuumStmt*)parsetree)->options & VACOPT_VACUUM)) {
        stmt = GSCGROUP_VACUUM_STMT;
    }

    return stmt;
}

/*
 * function name: WLMIsSpecialQuery
 * description  : Check query whether is special query.
 */
bool WLMIsSpecialQuery(const char* sqltext)
{
    Index i = 0;

    char* whitelist[] = {"select name, setting from pg_settings where name in ('connection_info')",
        "SELECT VERSION()",
        "SELECT intervaltonum(gs_password_deadline())",
        "SELECT gs_password_notifytime()",
        "SET connection_info ="};

    if (IsConnFromGTMTool() || IsConnFromInternalTool() || u_sess->proc_cxt.IsWLMWhiteList) {
        return true;
    }

    /*
     * If the application is 'PGXCNodeName' or the query is from
     * the internal tool(such as gs_clean) or the query is in
     * a transaction block, it will be treated as a special query.
     */
    if (!IsConnFromApp() && StringIsValid(g_instance.attr.attr_common.PGXCNodeName) &&
        strcmp(u_sess->attr.attr_common.application_name, g_instance.attr.attr_common.PGXCNodeName) == 0) {
        return true;
    }

    // if the query is in the white list, it will be treated as a special query
    if (IsUnderPostmaster && IsConnFromApp()) {
        for (i = 0; i < lengthof(whitelist) - 1; ++i) {
            if ( (strcasecmp(sqltext, whitelist[i]) == 0)) {
                return true;
            }
        }
        if ((strlen(whitelist[i]) <= strlen(sqltext)) && 
            (strncasecmp(sqltext, whitelist[i], strlen(whitelist[i])) == 0)) {
            return true;
        }
    }

    return false;
}

/*
 * function name: WLMCheckToAttachCgroup
 * description  : Check if it can be attached to cgroup
 */
unsigned char WLMCheckToAttachCgroup(const QueryDesc* queryDesc)
{
    if (CGroupIsDefault(u_sess->wlm_cxt->control_group)) {
        return 0;
    }

    if (u_sess->attr.attr_resource.parctl_min_cost == 0) {
        return 1;
    }

    if (queryDesc != NULL && queryDesc->planstate != NULL && queryDesc->planstate->plan != NULL &&
        queryDesc->planstate->plan->total_cost >= (Cost)u_sess->attr.attr_resource.parctl_min_cost) {
        return 1;
    }

    return 0;
}

/*
 * function name: InitGlobalResourceParam
 * description  : Set global parameters which will used in parctl according to the value from optimizer
 */
void InitGlobalResourceParam(const QueryDesc* queryDesc, bool isQueryDesc, bool isdywlm)
{
    int result[2] = {0, 0};
    WLMGeneralParam* g_wlm_params = &u_sess->wlm_cxt->wlm_params;

    /* set memsize of query used in parctl. In dynamic case,
     * we will set max_memsize and min_memsize too.
     */
    GetResourcePoolActPts(WLMGetQueryMem(queryDesc, isQueryDesc), &g_wlm_params->rpdata, result);
    g_wlm_params->memsize = result[0];
    g_wlm_params->rpdata.act_pts = result[1];

    /* static case won't use max and min plan. */
    if (!isdywlm) {
        return;
    }
    if (isQueryDesc && queryDesc != NULL && queryDesc->plannedstmt != NULL) {
        g_wlm_params->use_planA = queryDesc->plannedstmt->ng_use_planA;
    } else {
        g_wlm_params->use_planA = false;
    }
    if (isQueryDesc != false && queryDesc != NULL && queryDesc->plannedstmt != NULL && g_wlm_params->use_planA) {
        PlannedStmt* plannedstmt = queryDesc->plannedstmt;

        Oid my_group_oid = get_pgxc_logic_groupoid(GetCurrentUserId());

        for (int i = 0; i < plannedstmt->ng_num; i++) {
            /* find myself nodegroup */
            if (my_group_oid == plannedstmt->ng_queryMem[i].ng_oid) {
                /* reset the value memsize and act_pts */
                GetResourcePoolActPts(plannedstmt->ng_queryMem[i].query_mem[0], &g_wlm_params->rpdata, result);
                g_wlm_params->memsize = result[0];  // in MB
                g_wlm_params->rpdata.act_pts = result[1];

                /* Init memsize of max plan with memsize. */
                g_wlm_params->max_memsize = g_wlm_params->memsize;
                g_wlm_params->rpdata.max_act_pts = g_wlm_params->rpdata.act_pts;

                /* if estimated memsize of min plan is zero or lager than max plan, it means that we have max plan only.
                 */
                if (plannedstmt->ng_queryMem[i].query_mem[1] == 0) {
                    g_wlm_params->min_memsize = g_wlm_params->memsize;
                    g_wlm_params->rpdata.min_act_pts = g_wlm_params->rpdata.act_pts;
                /* Init memsize of min plan. */
                } else {
                    GetResourcePoolActPts(plannedstmt->ng_queryMem[i].query_mem[1], &g_wlm_params->rpdata, result);
                    g_wlm_params->min_memsize = result[0];
                    g_wlm_params->rpdata.min_act_pts = result[1];
                }
                g_wlm_params->min_memsize = Min(g_wlm_params->min_memsize, g_wlm_params->max_memsize);
                g_wlm_params->rpdata.min_act_pts =
                    Min(g_wlm_params->rpdata.min_act_pts, g_wlm_params->rpdata.max_act_pts);

                ereport(DEBUG3,
                    (errmsg("----DYWLM---- InitGlobalResourceParam current query [max_mem: %d, min_mem: %d, max_act: "
                            "%d, min_act: %d]",
                        g_wlm_params->max_memsize,
                        g_wlm_params->min_memsize,
                        g_wlm_params->rpdata.max_act_pts,
                        g_wlm_params->rpdata.min_act_pts)));
                return;
            }
        }
    }

    /* Init memsize of max plan with memsize. */
    g_wlm_params->max_memsize = g_wlm_params->memsize;
    g_wlm_params->rpdata.max_act_pts = g_wlm_params->rpdata.act_pts;

    /* if estimated memsize of min plan is zero or lager than max plan, it means that we have max plan only. */
    if (WLMGetQueryMem(queryDesc, isQueryDesc, false) == 0) {
        g_wlm_params->min_memsize = g_wlm_params->memsize;
        g_wlm_params->rpdata.min_act_pts = g_wlm_params->rpdata.act_pts;
    /* Init memsize of min plan. */
    } else {
        GetResourcePoolActPts(WLMGetQueryMem(queryDesc, isQueryDesc, false), &g_wlm_params->rpdata, result);
        g_wlm_params->min_memsize = result[0];
        g_wlm_params->rpdata.min_act_pts = result[1];
    }
    g_wlm_params->min_memsize = Min(g_wlm_params->min_memsize, g_wlm_params->max_memsize);
    g_wlm_params->rpdata.min_act_pts = Min(g_wlm_params->rpdata.min_act_pts, g_wlm_params->rpdata.max_act_pts);

    ereport(DEBUG3,
        (errmsg("----DYWLM---- InitGlobalResourceParam current query no PlanB[max_mem: %d, min_mem: %d, max_act: %d, "
                "min_act: %d], Plan[%d,%d]",
            g_wlm_params->max_memsize,
            g_wlm_params->min_memsize,
            g_wlm_params->rpdata.max_act_pts,
            g_wlm_params->rpdata.min_act_pts,
            WLMGetQueryMem(queryDesc, isQueryDesc),
            WLMGetQueryMem(queryDesc, isQueryDesc, false))));
}

/*
 * function name: WLMIsSimpleQuery
 * description  : Check query whether is simple query.
 */
bool WLMIsSimpleQuery(const QueryDesc* queryDesc, bool force_control, bool isQueryDesc)
{
    WLMGeneralParam* g_wlm_params = &u_sess->wlm_cxt->wlm_params;

    g_wlm_params->use_planA = false;

    if (COORDINATOR_NOT_SINGLE || (IS_SERVICE_NODE && IsConnFromCoord()) ||
        u_sess->attr.attr_resource.parctl_min_cost < 0) {
        return true;
    }

    if (!COMP_ACC_CLUSTER) {
        /* if cgroup is not used, we will set the statement to normal statement. */
        if (g_instance.wlm_cxt->gscgroup_init_done <= 0 && u_sess->wlm_cxt->cgroup_stmt == GSCGROUP_NONE_STMT) {
            u_sess->wlm_cxt->cgroup_stmt = GSCGROUP_NORMAL_STMT;
        }

        /*
         * The sql is always treated as simple query while
         * application name is 'pgxc' or it's from internal tool
         * or parctl min cost is -1.
         */
        if (u_sess->wlm_cxt->cgroup_stmt == GSCGROUP_TOP_STMT || t_thrd.wlm_cxt.parctl_state.special) {
            return true;
        }

        /* dynamic parctl only use memory to judge a query whether be simple or not. */
        if (!g_instance.wlm_cxt->dynamic_workload_inited && WLMGetTotalCost(queryDesc, isQueryDesc) < 10) {
            /* We always need to init parameters since current query has a plan. */
            if (queryDesc != NULL) {
                int result[2] = {0, 0};
                GetResourcePoolActPts(WLMGetQueryMem(queryDesc, isQueryDesc), &g_wlm_params->rpdata, result);
                g_wlm_params->memsize = result[0];
                g_wlm_params->rpdata.act_pts = result[1];
            }
            return true;
        }
    } else if (t_thrd.wlm_cxt.parctl_state.special) {
        return true;
    }

    /* dynamic workload use query_mem to check query whether is special query */
    if (g_instance.wlm_cxt->dynamic_workload_inited && !COMP_ACC_CLUSTER) {
        if (queryDesc != NULL) {
            InitGlobalResourceParam(queryDesc, isQueryDesc, true);

            if (WLMGetQueryMem(queryDesc, isQueryDesc) < SIMPLE_THRESHOLD) { /* 32 MB */
                if (force_control && WLMGetQueryMem(queryDesc, isQueryDesc)) {
                    return false;
                }

                return true;
            }

            return false;
        }
    } else {
        /* acc cluster is always complicate */
        if (COMP_ACC_CLUSTER) {
            return false;
        }

        if (queryDesc != NULL) {
            InitGlobalResourceParam(queryDesc, isQueryDesc, false);

            if (force_control && WLMGetQueryMem(queryDesc, isQueryDesc)) {
                return false;
            }

            return (WLMGetTotalCost(queryDesc, isQueryDesc) < (Cost)u_sess->attr.attr_resource.parctl_min_cost) ? true
                                                                                                              : false;
        } else if (u_sess->attr.attr_resource.parctl_min_cost == 0) { /* no plan query, such as create table */
            return false;
        }
    }

    return true;
}

/*
 * function name: WLMNeedTrackResource
 * description  : Check query whether need track resource info.
 */
bool WLMNeedTrackResource(const QueryDesc* queryDesc)
{
    if (!IS_SERVICE_NODE || (IS_PGXC_COORDINATOR && IsConnFromCoord()) ||
        t_thrd.wlm_cxt.collect_info->sdetail.statement == NULL || !u_sess->attr.attr_resource.enable_resource_track ||
        u_sess->attr.attr_resource.resource_track_cost < 0 || u_sess->attr.attr_sql.enable_cluster_resize ||
        t_thrd.wlm_cxt.parctl_state.special) {
        return false;
    }

    /* always return false in the compute pool. */
    if (queryDesc != NULL && queryDesc->plannedstmt != NULL && queryDesc->plannedstmt->planTree &&
        false == queryDesc->plannedstmt->in_compute_pool) {
        if (queryDesc->plannedstmt->planTree->total_cost < (Cost)u_sess->attr.attr_resource.resource_track_cost) {
            return false;
        } else {
            return true;
        }
    }

    return false;
}

/*
 * function name: WLMSetControlGroup
 * description  : Set control group name "control_group" and
 *                the flag "cgroup_state"
 */
void WLMSetControlGroup(const char* cgname)
{
    errno_t rc = 0;

    if (cgname != NULL &&
        (*u_sess->wlm_cxt->control_group == '\0' || strcmp(u_sess->wlm_cxt->control_group, cgname) != 0)) {
        if (*u_sess->wlm_cxt->control_group) {
            ereport(DEBUG1, (errmsg("cgroup is changed from %s to %s", u_sess->wlm_cxt->control_group, cgname)));
        } else {
            ereport(DEBUG1, (errmsg("cgroup is set to %s", cgname)));
        }

        rc = snprintf_s(u_sess->wlm_cxt->control_group,
            sizeof(u_sess->wlm_cxt->control_group),
            sizeof(u_sess->wlm_cxt->control_group) - 1,
            "%s",
            cgname);
        securec_check_ss(rc, "\0", "\0");

        u_sess->wlm_cxt->cgroup_state = CG_USERSET;
    }
}

/*
 * @Description: insert resource pool into htab
 * @IN rpoid: resource pool id
 * @IN iops_limits: io_limits to be updated
 * @IN io_prio: io_priority to be updated
 * @Return: true: success
 *          false: failed
 * @See also:
 */
bool RespoolHashTabInsert(
    Oid rpoid, int32 iops_limits, const char* io_prio, const char* controlgroup, const char* nodegroup, bool is_foreign)
{
    bool found = false;

    USE_AUTO_LWLOCK(ResourcePoolHashLock, LW_EXCLUSIVE);

    ResourcePool* respool =
        (ResourcePool*)hash_search(g_instance.wlm_cxt->resource_pool_hashtbl, &rpoid, HASH_ENTER_NULL, &found);
    if (respool == NULL) {
        ereport(LOG,
            (errmsg("failed to allocate memory "
                    "when inserting element into respool hash table")));
        return false;
    }

    if (!found) {
        errno_t errval = memset_s(respool, sizeof(ResourcePool), 0, sizeof(ResourcePool));
        securec_check_errval(errval, , LOG);

        pthread_mutex_init(&respool->mutex, NULL);

        respool->rpoid = rpoid;
        respool->is_foreign = false;
        respool->node_group = NULL;
        respool->foreignrp = NULL;
        respool->parentrp = NULL;
        respool->parentoid = 0;
        ereport(LOG, (errmsg("insert resource pool %u into htab", rpoid)));
    }

    if (u_sess->wlm_cxt->respool_node_group != NULL) {
        respool->node_group = (void*)u_sess->wlm_cxt->respool_node_group;
    }

    if (iops_limits >= 0) {
        respool->iops_limits = iops_limits;
    }

    if (io_prio != NULL && *io_prio) {
        respool->io_priority = GetIoPriority(io_prio);
    }

    if (controlgroup != NULL && *controlgroup) {
        errno_t errval = strncpy_s(respool->cgroup, sizeof(respool->cgroup), controlgroup, sizeof(respool->cgroup) - 1);
        securec_check_errval(errval, , LOG);
    }

    if (nodegroup != NULL && *nodegroup) {
        errno_t errval = strncpy_s(respool->ngroup, sizeof(respool->ngroup), nodegroup, sizeof(respool->ngroup) - 1);
        securec_check_errval(errval, , LOG);
    }
    respool->mempct = u_sess->wlm_cxt->respool_foreign_mempct;
    respool->actpct = u_sess->wlm_cxt->respool_foreign_actpct;
    respool->parentoid = u_sess->wlm_cxt->respool_parentid;

    if (is_foreign) {
        respool->is_foreign = is_foreign;

        /* find the parent rp */
        if (u_sess->wlm_cxt->respool_parentid) {
            ResourcePool* parentrp = (ResourcePool*)hash_search(
                g_instance.wlm_cxt->resource_pool_hashtbl, &u_sess->wlm_cxt->respool_parentid, HASH_FIND, NULL);
            if (respool == NULL) {
                ereport(LOG,
                    (errmsg(
                        "find the parent resource pool of '%u' from hash table.", u_sess->wlm_cxt->respool_parentid)));
            } else {
                respool->parentrp = parentrp;
                parentrp->foreignrp = respool;
            }
        }

        /* Update the resource pool info in node group */
        if (u_sess->wlm_cxt->respool_node_group) {
            u_sess->wlm_cxt->respool_node_group->foreignrp = respool;
        }
    }

    /* update foreign resource pool if it exists */
    if (respool->foreignrp != NULL) {
        respool->foreignrp->actpct = respool->foreignrp->mempct * u_sess->wlm_cxt->respool_foreign_mempct / 100;
    }

    return true;
}

/*
 * @Description: remove resource pool from htab
 * @IN droplist: remove resource pool in the droplist
 * @Return: void
 * @See also:
 */
List* RespoolHashTabRemove(List* droplist)
{
    USE_AUTO_LWLOCK(ResourcePoolHashLock, LW_EXCLUSIVE);
    /* remove resource pool */
    ListCell* dropcell = list_head(droplist);
    while (dropcell != NULL) {
        Oid rpoid = lfirst_oid(dropcell);
        dropcell = lnext(dropcell);

        ResourcePool* respool =
            (ResourcePool*)hash_search(g_instance.wlm_cxt->resource_pool_hashtbl, &rpoid, HASH_FIND, NULL);
        if (respool != NULL && respool->is_foreign) {
            /* update the parent foreign rp if it exists */
            if (respool->parentrp) {
                respool->parentrp->foreignrp = NULL;
                respool->parentrp = NULL;
            }

            /* update the node group if it exists */
            WLMNodeGroupInfo* ng = (WLMNodeGroupInfo*)respool->node_group;
            if (ng != NULL) {
                ng->foreignrp = NULL;
            }
        }

        hash_search(g_instance.wlm_cxt->resource_pool_hashtbl, &rpoid, HASH_REMOVE, NULL);
        /* save the resource pool id to remove to the log */
        ereport(LOG, (errmsg("remove resource pool %u from htab", rpoid)));
    }
    list_free(droplist);
    droplist = NIL;

    return droplist;
}

/*
 * function name: GenerateResourcePoolStmt
 * description  : Generate Create Resource Pool statement.
 * The statement is used for executing on datanode in logic cluster mode. We need to
 * add logic cluster information in the statement.
 */
char* GenerateResourcePoolStmt(CreateResourcePoolStmt* stmt, const char* origin_query)
{
    ListCell* option = NULL;
    char* query_string = NULL;
    char* buf = NULL;
    int total_size;
    int buf_len;
    int len;

    if (!in_logic_cluster() || get_current_lcgroup_name() == NULL) {
        return NULL;
    }

    /* Calculate needed string length. */
    total_size = strlen(origin_query) + CHAR_BUF_SIZE;

    query_string = (char*)palloc0(total_size);

    len = snprintf_s(query_string, total_size, total_size - 1, "CREATE RESOURCE POOL \"%s\" WITH (", stmt->pool_name);
    securec_check_ss(len, "\0", "\0");

    buf = query_string + len;
    buf_len = total_size - len;

    foreach (option, stmt->options) {
        DefElem* defel = (DefElem*)lfirst(option);

        /* mem_percent checking */
        if (strcmp(defel->defname, "mem_percent") == 0) {
            len = snprintf_s(buf, buf_len, buf_len - 1, "mem_percent=%d,", (int)defGetInt64(defel));
        } else if (strcmp(defel->defname, "cpu_affinity") == 0) { /* cpu affinity checking */
            pfree(query_string);
            ereport(
                ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("cpu_affinity is not supported currently!")));
        } else if (strcmp(defel->defname, "active_statements") == 0) { /* active statements checking */
            len = snprintf_s(buf, buf_len, buf_len - 1, "active_statements=%d,", (int)defGetInt64(defel));
        } else if (strcmp(defel->defname, "control_group") == 0) { /* check the control_group option */
            len = snprintf_s(buf, buf_len, buf_len - 1, "control_group='%s',", defGetString(defel));
        } else if (strcmp(defel->defname, "max_dop") == 0) { /* max dop checking */
            len = snprintf_s(buf, buf_len, buf_len - 1, "max_dop=%d,", (int)defGetInt64(defel));

        } else if (strcmp(defel->defname, "memory_limit") == 0) { /* memory limit checking */
            len = snprintf_s(buf, buf_len, buf_len - 1, "memory_limit='%s',", defGetString(defel));
        } else if (strcmp(defel->defname, "parent") == 0) {
            pfree(query_string);
            ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("parent is not supported currently!")));
        } else if (strcmp(defel->defname, "io_limits") == 0) {
            len = snprintf_s(buf, buf_len, buf_len - 1, "io_limits=%d,", (int)defGetInt64(defel));
        } else if (strcmp(defel->defname, "io_priority") == 0) {
            len = snprintf_s(buf, buf_len, buf_len - 1, "io_priority='%s',", defGetString(defel));
        } else if (strcmp(defel->defname, "is_foreign") == 0) {
            len = snprintf_s(buf, buf_len, buf_len - 1, "is_foreign=%d,", defGetBoolean(defel) ? 1 : 0);
        } else if (strcmp(defel->defname, "nodegroup") == 0) { /* node group option */
            errno_t rc = memset_s(query_string, total_size, 0, total_size);
            securec_check(rc, "\0", "\0");
            rc = strcpy_s(query_string, total_size, origin_query);
            securec_check(rc, "\0", "\0");
            return query_string;
        } else {
            pfree(query_string);
            ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("option \"%s\" not recognized", defel->defname)));
        }
        securec_check_ss(len, "\0", "\0");

        buf += len;
        buf_len -= len;

        if (buf_len <= 0) {
            pfree(query_string);
            ereport(ERROR,
                (errcode(ERRCODE_RESOURCE_POOL_ERROR),
                    errmsg("The create resource pool statement buffer is too small(%d).", total_size)));
        }
    }

    len = snprintf_s(buf, buf_len, buf_len - 1, "nodegroup='%s');", get_current_lcgroup_name());
    securec_check_ss(len, "\0", "\0");

    return query_string;
}

/*
 * function name: CreateResourcePool
 * description  : create the resource pool
 */
void CreateResourcePool(CreateResourcePoolStmt* stmt)
{
    char* pool_name = stmt->pool_name;
    Relation relation;
    HeapTuple htup;
    bool nulls[Natts_pg_resource_pool];
    Datum values[Natts_pg_resource_pool];
    bool repl[Natts_pg_resource_pool];
    int actpct = 0;
    int mempct = 0;

    Oid parentid = InvalidOid;
    /* Only a DB administrator can add resource pool */
    if (!superuser() && !is_lcgroup_admin()) {
        ereport(ERROR,
            (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                errmsg("must be system admin or vcadmin to create resource pool")));
    }

    if (strcmp(pool_name, INVALID_POOL_NAME) == 0) {
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_NAME),
                errmsg("default session_respool \"invalid_pool\" is not allowed to set by user")));
    }

    /* Check whether resource pool name is in use */
    if (OidIsValid(get_resource_pool_oid(pool_name))) {
        ereport(ERROR,
            (errcode(ERRCODE_DUPLICATE_OBJECT), errmsg("Resource Pool \"%s\": object already defined", pool_name)));
    }

    if (hash_get_num_entries(g_instance.wlm_cxt->resource_pool_hashtbl) >=
        (int)((MAX_INFO_MEM * MBYTES) / sizeof(ResourcePool))) {
        ereport(ERROR,
            (errcode(ERRCODE_RESOURCE_POOL_ERROR),
                errmsg("The memory of resource pool htab cannot be out of %dMB. please drop the "
                       "unnecessary resource pools",
                    MAX_INFO_MEM)));
    }

    relation = heap_open(ResourcePoolRelationId, ShareUpdateExclusiveLock);

    CheckResourcePoolOptions(pool_name, stmt->options, true, values, nulls, repl, &parentid, &actpct, &mempct);
    htup = (HeapTuple)heap_form_tuple(relation->rd_att, values, nulls);
    /* Insert tuple in catalog */
    (void)simple_heap_insert(relation, htup);

    /*
     * there is lock acquired on the parent tuple
     * in recordDependencyOnRespool
     * if parent is dropped in another thread and committed,
     * an error will be reported
     */
    if (OidIsValid(parentid)) {
        recordDependencyOnRespool(ResourcePoolRelationId, HeapTupleGetOid(htup), parentid);
    }

    u_sess->wlm_cxt->respool_create_oid = HeapTupleGetOid(htup);
    u_sess->wlm_cxt->respool_io_limit_update = DatumGetInt32(values[Anum_pg_resource_pool_iops_limits - 1]);

    char* io_pri_update =
        DatumGetCString(DirectFunctionCall1(nameout, NameGetDatum(values[Anum_pg_resource_pool_io_priority - 1])));
    if (StringIsValid(io_pri_update)) {
        errno_t rc = strcpy_s(u_sess->wlm_cxt->respool_io_pri_update, NAMEDATALEN, io_pri_update);
        securec_check(rc, "\0", "\0");
    }

    char* nodegroup_update =
        DatumGetCString(DirectFunctionCall1(nameout, NameGetDatum(values[Anum_pg_resource_pool_nodegroup - 1])));
    if (StringIsValid(nodegroup_update)) {
        errno_t rc = strcpy_s(u_sess->wlm_cxt->respool_nodegroup, NAMEDATALEN, nodegroup_update);
        securec_check(rc, "\0", "\0");
    }

    char* controlgroup_update =
        DatumGetCString(DirectFunctionCall1(nameout, NameGetDatum(values[Anum_pg_resource_pool_control_group - 1])));
    if (StringIsValid(controlgroup_update)) {
        errno_t rc = strcpy_s(u_sess->wlm_cxt->respool_controlgroup, NAMEDATALEN, controlgroup_update);
        securec_check(rc, "\0", "\0");
    }

    /* used for hashtable update */
    u_sess->wlm_cxt->respool_is_foreign = DatumGetBool(values[Anum_pg_resource_pool_is_foreign - 1]);

    u_sess->wlm_cxt->respool_foreign_mempct = mempct;
    u_sess->wlm_cxt->respool_foreign_actpct = actpct;
    u_sess->wlm_cxt->respool_parentid = parentid;

    CatalogUpdateIndexes(relation, htup);

    heap_close(relation, NoLock);
}

/*
 * function name: AlterResourcePool
 * description  : alter the resource pool
 */
void AlterResourcePool(AlterResourcePoolStmt* stmt)
{
    char* pool_name = stmt->pool_name;
    Oid pool_oid = get_resource_pool_oid(pool_name);

    Relation relation;
    HeapTuple oldtup, newtup;
    bool nulls[Natts_pg_resource_pool];
    Datum values[Natts_pg_resource_pool];
    bool repl[Natts_pg_resource_pool];

    Oid parentid = InvalidOid;
    int actpct = 0;
    int mempct = -1;

    /* Only a DB administrator can alter resource pools */
    if (!superuser() && !is_lcgroup_admin()) {
        ereport(ERROR,
            (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                errmsg("must be system admin or vcadmin to change resource pools")));
    }

    /* Check that resource pool exists */
    if (!OidIsValid(pool_oid)) {
        ereport(
            ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("Resource Pool \"%s\": object not defined", pool_name)));
    }

    /* Check whether it is default_pool */
    if (pool_oid == DEFAULT_POOL_OID) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_NAME), errmsg("Can not alter default_pool")));
    }

    relation = heap_open(ResourcePoolRelationId, ShareUpdateExclusiveLock);

    LockSharedObject(ResourcePoolRelationId, pool_oid, 0, AccessExclusiveLock);

    /* check resource pool option */
    CheckResourcePoolOptions(pool_name, stmt->options, false, values, nulls, repl, &parentid, &actpct, &mempct);

    /* Open old tuple, checks are performed on it and new values */
    oldtup = SearchSysCacheCopy1(PGXCRESOURCEPOOLOID, ObjectIdGetDatum(pool_oid));
    if (!HeapTupleIsValid(oldtup)) {
        ereport(
            ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("cache lookup failed for object %u", pool_oid)));
    }

    /* Update relation */
    newtup = heap_modify_tuple(oldtup, RelationGetDescr(relation), values, nulls, repl);
    simple_heap_update(relation, &oldtup->t_self, newtup);

    if (repl[Anum_pg_resource_pool_iops_limits - 1]) {
        u_sess->wlm_cxt->respool_io_limit_update = DatumGetInt32(values[Anum_pg_resource_pool_iops_limits - 1]);
    }

    if (repl[Anum_pg_resource_pool_io_priority - 1]) {
        char* io_pri_update =
            DatumGetCString(DirectFunctionCall1(nameout, NameGetDatum(values[Anum_pg_resource_pool_io_priority - 1])));
        if (StringIsValid(io_pri_update)) {
            errno_t rc = strcpy_s(u_sess->wlm_cxt->respool_io_pri_update, NAMEDATALEN, io_pri_update);
            securec_check(rc, "\0", "\0");
        }
    }

    /* used for hashtable update */
    u_sess->wlm_cxt->respool_alter_oid = pool_oid;

    u_sess->wlm_cxt->respool_foreign_mempct = mempct;
    u_sess->wlm_cxt->respool_foreign_actpct = actpct;

    /* Update indexes */
    CatalogUpdateIndexes(relation, newtup);
    /* Release lock at Commit */
    heap_close(relation, NoLock);
}
/*
 * function name: RemoveResourcePool
 * description  : remove the resource pools of the catalog
 *                all child resource pools will be dropped
 *                simultaneously
 */
void RemoveResourcePool(Relation relation, Oid pool_oid)
{
    List* childlist = NIL;
    HeapTuple tup;
    MemoryContext oldcontext;

    /*
     * drop child resource pools when drop parent resource pool
     * add childlist in t_thrd.top_mem_cxt to update
     * hash table when commit the transaction.
     */
    oldcontext = MemoryContextSwitchTo(SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_CBB));
    (void)CheckDependencyOnRespool(ResourcePoolRelationId, pool_oid, &childlist, true);
    (void)MemoryContextSwitchTo(oldcontext);

    if (childlist != NULL) {
        ListCell* rpdata = NULL;
        Oid childrpoid = InvalidOid;
        /* Acuire Deletion locks for all child pools first */
        foreach (rpdata, childlist) {
            childrpoid = lfirst_oid(rpdata);

            LockSharedObject(ResourcePoolRelationId, childrpoid, 0, AccessExclusiveLock);
        }
        /* then delete the child pools in the system table */
        foreach (rpdata, childlist) {
            childrpoid = lfirst_oid(rpdata);
            /* delete dependency of the child pools on parent pool */
            deleteSharedDependencyRecordsFor(ResourcePoolRelationId, childrpoid, 0);

            /* delete child pools  */
            tup = SearchSysCache1(PGXCRESOURCEPOOLOID, ObjectIdGetDatum(childrpoid));

            /* if tup is invalid, do nothing */
            if (!HeapTupleIsValid(tup)) {
                ereport(ERROR,
                    (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("cache lookup failed for pg_resource_pool %u", childrpoid)));
            } else {
                simple_heap_delete(relation, &tup->t_self);
                ReleaseSysCache(tup);
            }
        }
        ereport(NOTICE, (errmsg("all child resource pool of %u are dropped simultaneously.", pool_oid)));
    }

    /* add pool_oid and childlist in respool_delete_list for updating hashtbl */
    u_sess->wlm_cxt->respool_delete_list = childlist;

    /* add pool_oid in the deletion list */
    oldcontext = MemoryContextSwitchTo(SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_CBB));
    u_sess->wlm_cxt->respool_delete_list = lappend_oid(u_sess->wlm_cxt->respool_delete_list, pool_oid);
    (void)MemoryContextSwitchTo(oldcontext);

    /* pool_oid might be a child resource pool, delete its dependency. */
    deleteSharedDependencyRecordsFor(ResourcePoolRelationId, pool_oid, 0);

    tup = SearchSysCache1(PGXCRESOURCEPOOLOID, ObjectIdGetDatum(pool_oid));

    if (!HeapTupleIsValid(tup)) {
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("cache lookup failed for pg_resource_pool %u", pool_oid)));
    }

    simple_heap_delete(relation, &tup->t_self);

    ReleaseSysCache(tup);
    heap_close(relation, NoLock);
}
/*
 * function name: DropResourcePool
 * description  : drop the resource pool
 */
void DropResourcePool(DropResourcePoolStmt* stmt)
{
    const char* pool_name = stmt->pool_name;
    Oid pool_oid = get_resource_pool_oid(pool_name);
    bool ret = true;
    StringInfoData detail;
    List* userlist = NIL;

    /* Only a DB administrator can drop resource pools */
    if (!superuser() && !is_lcgroup_admin()) {
        ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), errmsg("Permission denied to drop resource pools.")));
    }

    /* Check whether it is default_pool */
    if (pool_oid == DEFAULT_POOL_OID) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("Can not drop default_pool")));
    }

    /* Check if node is defined */
    if (!OidIsValid(pool_oid)) {
        if (!stmt->missing_ok) {
            ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("Resource Pool \"%s\": object not defined", pool_name)));
        } else {
            ereport(NOTICE, (errmsg("Resource Pool \"%s\" does not exist, skipping", pool_name)));
            return;
        }
    }

    initStringInfo(&detail);

    /* Delete the pg_resource_pool tuple. */
    Relation relation = heap_open(ResourcePoolRelationId, ShareUpdateExclusiveLock);

    /*
     * lock the pool, so no child resource pool or
     * user can add dependencies to her while we drop
     * her.  We keep the lock until the end of transaction.
     */
    LockSharedObject(ResourcePoolRelationId, pool_oid, 0, AccessExclusiveLock);

    /*
     * get users who use the pool in userlist.
     * no need check whether there are
     * users dependent on the child pools.
     * when the parent user doesn't exist,
     * there must be no child users, so there
     * must be no users dependent on the child pools.
     */
    if (CheckDependencyOnRespool(AuthIdRelationId, pool_oid, &userlist, false)) {
        Oid userid = lfirst_oid(list_head(userlist));

        if (OidIsValid(userid)) {
            char rolename[NAMEDATALEN];
            GetRoleName(userid, rolename, NAMEDATALEN);

            if (detail.len != 0) {
                appendStringInfoChar(&detail, '\n');
            }

            appendStringInfo(&detail,
                _("role \"%s\" depends on "
                  "resource pool \"%s\""),
                rolename,
                pool_name);
            ret = false;
        }
    }
    // If we do not use workload manager, we need not check resource pool hash table.
    if (ENABLE_WORKLOAD_CONTROL && !WLMCheckResourcePoolIsIdle(pool_oid)) {
        if (detail.len) {
            appendStringInfoChar(&detail, '\n');
        }

        appendStringInfo(&detail,
            _("active statement depends on "
              "resource pool \"%s\""),
            pool_name);

        ret = false;
    }

    if (!ret) {
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("cannot drop resource pool \"%s\" "
                       "because other objects depend on it",
                    pool_name),
                errdetail("%s", detail.data)));
    }

    pfree(detail.data);

    RemoveResourcePool(relation, pool_oid);
}

/*
 * function name: CreateWorkloadGroup
 * description  : create workload group by DDL
 */
void CreateWorkloadGroup(CreateWorkloadGroupStmt* stmt)
{
    const char* group_name = stmt->group_name;
    const char* pool_name = stmt->pool_name;
    Relation relation;
    HeapTuple htup;
    bool nulls[Natts_pg_workload_group];
    Datum values[Natts_pg_workload_group];
    bool repl[Natts_pg_workload_group];

    /* Only a DB administrator can add workload group */
    if (!superuser()) {
        ereport(
            ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), errmsg("must be system admin to create workload group")));
    }

    /* Check whether workload group name is in use */
    if (OidIsValid(get_workload_group_oid(group_name))) {
        ereport(ERROR,
            (errcode(ERRCODE_DUPLICATE_OBJECT), errmsg("Workload Group \"%s\": object already defined", group_name)));
    }

    CheckWorkloadGroupOptions(group_name, pool_name, stmt->options, true, values, nulls, repl);

    relation = heap_open(WorkloadGroupRelationId, RowExclusiveLock);
    htup = (HeapTuple)heap_form_tuple(relation->rd_att, values, nulls);
    /* Insert tuple in catalog */
    (void)simple_heap_insert(relation, htup);

    CatalogUpdateIndexes(relation, htup);

    heap_close(relation, RowExclusiveLock);
}

/*
 * function name: AlterWorkloadGroup
 * description  : alter workload group by DDL
 */
void AlterWorkloadGroup(AlterWorkloadGroupStmt* stmt)
{
    const char* group_name = stmt->group_name;
    const char* pool_name = stmt->pool_name;
    Oid group_oid = get_workload_group_oid(group_name);
    HeapTuple oldtup, newtup;
    Relation relation;
    Datum values[Natts_pg_workload_group];
    bool nulls[Natts_pg_workload_group];
    bool repl[Natts_pg_workload_group];

    /* Only a DB administrator can alter workload groups */
    if (!superuser()) {
        ereport(
            ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), errmsg("must be system admin to change workload groups")));
    }

    /* Check that workload group exists */
    if (!OidIsValid(group_oid)) {
        ereport(ERROR,
            (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("Workload Group \"%s\": object not defined", group_name)));
    }

    /* Check whether it is default_pool */
    if (group_oid == DEFAULT_GROUP_OID) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("Can not alter default_group")));
    }

    CheckWorkloadGroupOptions(group_name, pool_name, stmt->options, false, values, nulls, repl);

    relation = heap_open(WorkloadGroupRelationId, RowExclusiveLock);

    /* Open new tuple, checks are performed on it and new values */
    oldtup = SearchSysCacheCopy1(PGXCWORKLOADGROUPOID, ObjectIdGetDatum(group_oid));
    if (!HeapTupleIsValid(oldtup)) {
        ereport(
            ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("cache lookup failed for object %u", group_oid)));
    }

    /* Update relation */
    newtup = heap_modify_tuple(oldtup, RelationGetDescr(relation), values, nulls, repl);
    simple_heap_update(relation, &oldtup->t_self, newtup);

    /* Update indexes */
    CatalogUpdateIndexes(relation, newtup);

    /* Release lock at Commit */
    heap_close(relation, NoLock);
}

/*
 * function name: RemoveWorkloadGroup
 * description  : remove workload group by DDL
 */
void RemoveWorkloadGroup(Oid group_oid)
{
    Relation relation;
    HeapTuple tup;

    /* Delete the pg_workload_group tuple. */
    relation = heap_open(WorkloadGroupRelationId, RowExclusiveLock);

    tup = SearchSysCache1(PGXCWORKLOADGROUPOID, ObjectIdGetDatum(group_oid));

    if (!HeapTupleIsValid(tup)) {
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("cache lookup failed for pg_workload_group %u", group_oid)));
    }

    simple_heap_delete(relation, &tup->t_self);

    ReleaseSysCache(tup);

    heap_close(relation, RowExclusiveLock);
}

/*
 * function name: DropWorkloadGroup
 * description  : drop workload group by DDL
 */
void DropWorkloadGroup(DropWorkloadGroupStmt* stmt)
{
    const char* group_name = stmt->group_name;
    Oid group_oid = get_workload_group_oid(group_name);
    bool ret = true;
    Relation relation;
    HeapTuple tup;
    StringInfoData detail;

    SysScanDesc scan;
    Form_pg_app_workloadgroup_mapping app_data;

    /* Only a DB administrator can drop workload groups */
    if (!superuser()) {
        ereport(ERROR,
            (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), errmsg("must be system admin to remove workload groups ")));
    }

    /* Check whether it is default_pool */
    if (group_oid == DEFAULT_GROUP_OID) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("Can not drop default_group")));
    }

    /* Check if node is defined */
    if (!OidIsValid(group_oid)) {
        if (!stmt->missing_ok) {
            ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("Workload Group \"%s\": object not defined", group_name)));
        } else {
            ereport(NOTICE, (errmsg("Workload Group \"%s\" does not exist, skipping", group_name)));
            return;
        }
    }

    initStringInfo(&detail);

    relation = heap_open(AppWorkloadGroupMappingRelationId, AccessShareLock);

    scan = systable_beginscan(relation, InvalidOid, false, NULL, 0, NULL);

    while (HeapTupleIsValid((tup = systable_getnext(scan)))) {
        app_data = (Form_pg_app_workloadgroup_mapping)GETSTRUCT(tup);

        if (strcmp(NameStr(app_data->workload_gpname), group_name) == 0) {
            /* separate entries with a newline */
            if (detail.len != 0) {
                appendStringInfoChar(&detail, '\n');
            }

            appendStringInfo(&detail,
                _("application \"%s\" depends on "
                  "workload group \"%s\""),
                NameStr(app_data->appname),
                group_name);
            ret = false;
        }
    }

    systable_endscan(scan);
    heap_close(relation, AccessShareLock);

    if (!ret) {
        ereport(ERROR,
            (errcode(ERRCODE_OBJECT_IN_USE),
                errmsg("cannot drop workload group \"%s\" "
                       "because other objects depend on it",
                    group_name),
                errdetail("%s", detail.data)));
    }

    RemoveWorkloadGroup(group_oid);

    pfree(detail.data);
}

/*
 * function name: CreateAppWorkloadGroupMapping
 * description  : create the application mapping
 */
void CreateAppWorkloadGroupMapping(CreateAppWorkloadGroupMappingStmt* stmt)
{
    const char* app_name = stmt->app_name;
    Relation relation;
    HeapTuple htup;
    bool nulls[Natts_pg_app_workloadgroup_mapping] = {false};
    Datum values[Natts_pg_app_workloadgroup_mapping];
    bool repl[Natts_pg_app_workloadgroup_mapping];

    /* Only a DB administrator can add app workload group mapping */
    if (!superuser()) {
        ereport(ERROR,
            (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                errmsg("must be system admin to create application "
                       "workload group mapping")));
    }

    if (strcasecmp(app_name, "pgxc") == 0) {
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("Application Workload Group Mapping \"%s\": "
                       "object can not create",
                    app_name)));
    }

    /* Check whether application name is in use */
    if (OidIsValid(get_application_mapping_oid(app_name))) {
        ereport(ERROR,
            (errcode(ERRCODE_DUPLICATE_OBJECT),
                errmsg("Application Workload Group Mapping \"%s\": "
                       "object already defined",
                    app_name)));
    }

    CheckAppWorkloadGroupMappingOptions(app_name, stmt->options, true, values, nulls, repl);

    relation = heap_open(AppWorkloadGroupMappingRelationId, RowExclusiveLock);

    htup = (HeapTuple)heap_form_tuple(relation->rd_att, values, nulls);

    /* Insert tuple in catalog */
    (void)simple_heap_insert(relation, htup);

    CatalogUpdateIndexes(relation, htup);

    heap_close(relation, RowExclusiveLock);
}

/*
 * function name: AlterAppWorkloadGroupMapping
 * description  : alter the application mapping
 */
void AlterAppWorkloadGroupMapping(AlterAppWorkloadGroupMappingStmt* stmt)
{
    const char* app_name = stmt->app_name;
    Oid app_oid = get_application_mapping_oid(app_name);
    HeapTuple oldtup, newtup;
    Relation relation;
    Datum values[Natts_pg_app_workloadgroup_mapping];
    bool nulls[Natts_pg_app_workloadgroup_mapping];
    bool repl[Natts_pg_app_workloadgroup_mapping];

    /* Only a DB administrator can alter app workload group mapping */
    if (!superuser()) {
        ereport(ERROR,
            (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                errmsg("must be system admin to change application "
                       "workload group mapping")));
    }

    /* Check that application name exists */
    if (!OidIsValid(app_oid)) {
        ereport(ERROR,
            (errcode(ERRCODE_UNDEFINED_OBJECT),
                errmsg("Application Workload Group Mapping \"%s\": "
                       "object not defined",
                    app_name)));
    }

    /* Check whether it is default_application */
    if (app_oid == DEFAULT_APP_OID) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("Can not alter default_application")));
    }

    relation = heap_open(AppWorkloadGroupMappingRelationId, RowExclusiveLock);

    /* Open new tuple, checks are performed on it and new values */
    oldtup = SearchSysCacheCopy1(PGXCAPPWGMAPPINGOID, ObjectIdGetDatum(app_oid));
    if (!HeapTupleIsValid(oldtup))
        ereport(
            ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("cache lookup failed for object %u", app_oid)));

    CheckAppWorkloadGroupMappingOptions(app_name, stmt->options, false, values, nulls, repl);

    /* Update relation */
    newtup = heap_modify_tuple(oldtup, RelationGetDescr(relation), values, nulls, repl);
    simple_heap_update(relation, &oldtup->t_self, newtup);

    /* Update indexes */
    CatalogUpdateIndexes(relation, newtup);

    heap_freetuple(newtup);

    /* Release lock at Commit */
    heap_close(relation, NoLock);
}

/*
 * function name: RemoveAppWorkloadGroupMapping
 * description  : remove the application mapping
 */
void RemoveAppWorkloadGroupMapping(Oid app_oid)
{
    Relation relation;
    HeapTuple tup;

    /* Delete the pg_app_workloadgroup_mapping tuple. */
    relation = heap_open(AppWorkloadGroupMappingRelationId, RowExclusiveLock);

    tup = SearchSysCache1(PGXCAPPWGMAPPINGOID, ObjectIdGetDatum(app_oid));

    if (!HeapTupleIsValid(tup)) {
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("cache lookup failed for pg_app_workloadgroup_mapping %u", app_oid)));
    }

    simple_heap_delete(relation, &tup->t_self);

    ReleaseSysCache(tup);

    heap_close(relation, RowExclusiveLock);
}

/*
 * function name: DropAppWorkloadGroupMapping
 * description  : drop the application mapping
 */
void DropAppWorkloadGroupMapping(DropAppWorkloadGroupMappingStmt* stmt)
{
    const char* app_name = stmt->app_name;
    Oid app_oid = get_application_mapping_oid(app_name);

    /* Only a DB administrator can drop app workload group mapping */
    if (!superuser()) {
        ereport(ERROR,
            (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                errmsg("must be system admin to remove "
                       "application workload group mapping.")));
    }

    /* Check whether it is default_application */
    if (app_oid == DEFAULT_APP_OID) {
        ereport(ERROR, (errcode(ERRCODE_WRONG_OBJECT_TYPE), errmsg("Can not drop default_application")));
    }

    /* Check if node is defined */
    if (!OidIsValid(app_oid)) {
        if (!stmt->missing_ok) {
            ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_OBJECT),
                    errmsg("App Workload Group Mapping \"%s\": object not defined", app_name)));
        } else {
            ereport(NOTICE, (errmsg("App Workload Group Mapping \"%s\" does not exist, skipping", app_name)));
            return;
        }
    }

    RemoveAppWorkloadGroupMapping(app_oid);
}

/*
 * function name: GetCurrentCgroup
 * description  : get current cgroup name
 */
void GetCurrentCgroup(char* cgroup, const char* curr_group, int len)
{
    int rc;

    gscgroup_stmt_t curr_stmt = u_sess->wlm_cxt->cgroup_stmt;

    if (!u_sess->attr.attr_resource.enable_cgroup_switch) {
        curr_stmt = GSCGROUP_NORMAL_STMT;
    }

    switch (curr_stmt) {
        case GSCGROUP_NONE_STMT:
        case GSCGROUP_NORMAL_STMT:
            if (StringIsValid(curr_group)) {
                rc = snprintf_s(cgroup, len, len - 1, "%s", curr_group);
                securec_check_ss(rc, "\0", "\0");
            } else {
                rc = snprintf_s(cgroup, len, len - 1, "%s", u_sess->wlm_cxt->control_group);
                securec_check_ss(rc, "\0", "\0");
            }
            break;
        case GSCGROUP_VACUUM_STMT:
            rc = snprintf_s(cgroup, len, len - 1, "%s:%s", GSCGROUP_TOP_BACKEND, GSCGROUP_VACUUM);
            securec_check_ss(rc, "\0", "\0");
            break;
        case GSCGROUP_TOP_STMT:
            rc = snprintf_s(cgroup, len, len - 1, "%s", u_sess->wlm_cxt->control_group);
            securec_check_ss(rc, "\0", "\0");

            (void)gscgroup_get_top_group_name(t_thrd.wlm_cxt.thread_node_group, cgroup, len);
            break;
        default:
            rc = snprintf_s(cgroup, len, len - 1, "%s", u_sess->wlm_cxt->control_group);
            securec_check_ss(rc, "\0", "\0");
            break;
    }
}

/*
 * function name: WLMSwitchCGroup
 * description  : switch current cgroup name
 */
void WLMSwitchCGroup(void)
{
    switch (u_sess->wlm_cxt->cgroup_stmt) {
        case GSCGROUP_NONE_STMT:
        case GSCGROUP_NORMAL_STMT:
            if (u_sess->wlm_cxt->cgroup_state == CG_USERSET) {
                if (!CGroupIsValid(u_sess->wlm_cxt->control_group) || CGroupIsDefault(u_sess->wlm_cxt->control_group)) {
                    return;
                }

                /* simple query will skip the cgroup loading */
                if (IS_SERVICE_NODE && (IsConnFromCoord() || t_thrd.wlm_cxt.parctl_state.simple)) {
                    return;
                }

                if (g_instance.attr.attr_common.enable_thread_pool) {
                    ereport(LOG, (errmsg("Cannot switch control group in thread pool mode.")));
                    return;
                }

                gscgroup_attach_task(t_thrd.wlm_cxt.thread_node_group, u_sess->wlm_cxt->control_group);
                ereport(DEBUG1, (errmsg("Switch cgroup to \"%s\"", u_sess->wlm_cxt->control_group)));
            }
            break;
        case GSCGROUP_TOP_STMT:
            if (u_sess->wlm_cxt->cgroup_state == CG_USERSET ||
                u_sess->wlm_cxt->cgroup_stmt != u_sess->wlm_cxt->cgroup_last_stmt) {
                if (g_instance.attr.attr_common.enable_thread_pool) {
                    ereport(LOG, (errmsg("Cannot switch control group in thread pool mode.")));
                    return;
                }

                gscgroup_switch_topwd(t_thrd.wlm_cxt.thread_node_group);
            }
            break;
        case GSCGROUP_VACUUM_STMT:
            if (u_sess->wlm_cxt->cgroup_stmt != u_sess->wlm_cxt->cgroup_last_stmt) {
                if (g_instance.attr.attr_common.enable_thread_pool) {
                    ereport(LOG, (errmsg("Cannot switch control group in thread pool mode.")));
                    return;
                }

                gscgroup_switch_vacuum();
            }
            break;
        default:
            break;
    }
}

/*
 * function name: GetMemorySizeWithUnit
 * description  : get a memory size with unit
 * arguments    : memory string, string size and memory size
 *                OUT: memory
 * return value : memory string with unit
 */
char* GetMemorySizeWithUnit(char* memory, int size, int memsize)
{
    const char* unit = NULL;

    errno_t rc = memset_s(memory, size, 0, size);
    securec_check(rc, "\0", "\0");

    if (memsize % MBYTES == 0) {
        memsize /= MBYTES;
        unit = "GB"; /* unit is GB */
    } else if (memsize % KBYTES == 0) {
        memsize /= KBYTES;
        unit = "MB"; /* unit is MB */
    } else {
        unit = "kB";
    }

    /* get memory string with unit */
    rc = snprintf_s(memory, size, size - 1, INT64_FORMAT "%s", (int64)memsize, unit);
    securec_check_ss(rc, "\0", "\0");

    return memory;
}

/*
 * function name: CheckWLMSessionInfoTable
 * description  : check session info table
 * arguments    : IN: table name
 * return value : true: valid false: invalid
 */
bool CheckWLMSessionInfoTableValid(const char* tablename)
{
    errno_t rc;

    /* workload manager is not valid, we have to set default database "postgres" here. */
    if (!ENABLE_WORKLOAD_CONTROL) {
        rc = sprintf_s(g_instance.wlm_cxt->stat_manager.database,
            sizeof(g_instance.wlm_cxt->stat_manager.database),
            "%s",
            "postgres");
        securec_check_ss(rc, "\0", "\0");
    }

    if (tablename == NULL || *g_instance.wlm_cxt->stat_manager.database == '\0') {
        return true;
    }

    /*
     * tablename is session info table, but the
     * database is not postgres, it's not valid.
     */
    if ((strcmp(tablename, SESSION_TABNAME) == 0 || strcmp(tablename, SESSIONUSER_TABNAME) == 0 ||
            strcmp(tablename, OPERATOR_TABNAME) == 0 || strcmp(tablename, USER_RESOURCE_HISTORY_TABNAME) == 0 ||
            strcmp(tablename, INSTANCE_RESOURCE_HISTORY_TABNAME) == 0) &&
        strcmp(get_and_check_db_name(u_sess->proc_cxt.MyDatabaseId, true), g_instance.wlm_cxt->stat_manager.database) != 0) {
        return false;
    }

    return true;
}

/*
 * @Description: parse user info configure file
 * @IN void
 * @Return: -1: abnormal 0: normal
 * @See also:
 */
int ParseUserInfoConfigFile(void)
{
    void* vaddr = NULL;
    size_t cglen = GSUSER_ALLNUM * sizeof(WLMUserInfo);
    char* cfgpath = NULL;
    char* exepath = NULL;
    size_t cfgpath_len = 0;
    struct passwd* passwd_user = getpwuid(geteuid());

    int i = 0;

    errno_t rc;

    /* OS user must be valid */
    if (passwd_user == NULL) {
        ereport(WARNING,
            (errmsg("can't get the passwd_user by euid %u."
                    "please check the running user!",
                geteuid())));
        return -1;
    }

    if (t_thrd.proc_cxt.DataDir == NULL) {
        return -1;
    }

    /* configure file will use data directory */
    exepath = t_thrd.proc_cxt.DataDir;

    cfgpath_len = strlen(exepath) + 1 + sizeof(GSWLM_CONF_DIR) + 1 + sizeof(GSWLMCFG_PREFIX) + 1 + sizeof(GSCONF_NAME) +
                  sizeof(GSCFG_SUFFIX) + 1;

    cfgpath = (char*)palloc0_noexcept(cfgpath_len);

    if (cfgpath == NULL) {
        return -1;
    }

    MAKE_SMART_POINTER(cfgpath);

    /* get the configure directory */
    rc = snprintf_s(
        cfgpath, cfgpath_len, cfgpath_len - 1, "%s/%s_%s%s", exepath, GSWLMCFG_PREFIX, GSCONF_NAME, GSCFG_SUFFIX);
    securec_check_ss(rc, "\0", "\0");

    /* if fsize is not valid, the configure does not exist, we will create it */
    long fsize = gsutil_filesize(cfgpath);

    /* configure file doesn't exist or size is not the same */
    vaddr = gsutil_filemap(cfgpath, cglen, (PROT_READ | PROT_WRITE), MAP_SHARED, passwd_user);

    if (NULL == vaddr) {
        ereport(WARNING,
            (errmsg("failed to create and map "
                    "the configure file %s!",
                cfgpath)));
        return -1;
    }

    /* configure file is new, we will initialize it */
    if (fsize == -1) {
        ereport(LOG, (errmsg("user configure file is not found, it will be created.")));
        rc = memset_s(vaddr, cglen, 0, cglen);
        securec_check(rc, "\0", "\0");
    }

    /* set configure file */
    for (i = 0; i < GSUSER_ALLNUM; ++i) {
        g_instance.wlm_cxt->stat_manager.userinfo[i] = (WLMUserInfo*)vaddr + i;
    }

    return 0;
}

/*
 * @Description: scan hash table, assign all the info "is_dirty" true
 * @IN htab: hash table
 * @Return: void
 * @See also:
 */
template <class DataType>
void AssignHTabInfoDirty(HTAB* htab)
{
    DataType* hdata = NULL;

    HASH_SEQ_STATUS hash_seq;
    hash_seq_init(&hash_seq, htab);

    while ((hdata = (DataType*)hash_seq_search(&hash_seq)) != NULL) {
        hdata->is_dirty = true;
    }
}

/*
 * @Description: scan resource pool hash table, and remove the redundant resource pool info
 * @IN void
 * @Return: void
 * @See also:
 */
void CheckResourcePoolHash(void)
{
    HASH_SEQ_STATUS hash_seq;
    hash_seq_init(&hash_seq, g_instance.wlm_cxt->resource_pool_hashtbl);
    ResourcePool* respool = NULL;

    while ((respool = (ResourcePool*)hash_seq_search(&hash_seq)) != NULL) {
        if (!respool->is_dirty) {
            continue;
        }

        hash_search(g_instance.wlm_cxt->resource_pool_hashtbl, &respool->rpoid, HASH_REMOVE, NULL);
    }
}

/*
 * @Description: build hash table for resource pool, including two functions
 *               1, build resource pool when process is restarted,
 *                  rebuild hash table from the very beginning
 *               2, if there is inconsistency between hash table and system tables,
 *                  the altered cgroups or mem_percent is considered, besides,
 *                  removing the redundant data in hash table is needed.
 * @IN lockmode: lock is acquired on the relation.
 * @Return:      true : build success,
 *               false: failed
 * @See also:
 */
bool BuildResourcePoolHash(LOCKMODE lockmode)
{
    bool found = false;
    bool isnull = false;
    errno_t errval = 0;

    List *temp_respool_list = NULL;
    TmpResourcePool *tmpResourcePool = NULL;

    HeapTuple tup;

    Relation relation = heap_open(ResourcePoolRelationId, lockmode);

    TupleDesc pg_respool_dsc = RelationGetDescr(relation);

    SysScanDesc scan = systable_beginscan(relation, InvalidOid, false, NULL, 0, NULL);

    while (HeapTupleIsValid((tup = systable_getnext(scan)))) {
        Oid rpoid = HeapTupleGetOid(tup);

        int io_limits_value = 0;
        int io_priority_value = IOPRIORITY_NONE;

        /* get the control group info */
        Datum ControlGroupDatum = heap_getattr(tup, Anum_pg_resource_pool_control_group, pg_respool_dsc, &isnull);

        /* get the parent oid */
        Datum parentid = heap_getattr(tup, Anum_pg_resource_pool_parent, pg_respool_dsc, &isnull);

        /* get the memory percent */
        Datum mempct = heap_getattr(tup, Anum_pg_resource_pool_mem_percentage, pg_respool_dsc, &isnull);

        tmpResourcePool = (TmpResourcePool *) palloc0_noexcept(sizeof(TmpResourcePool));
        if (tmpResourcePool == NULL) {
            return false;
        }

        /* set user id */
        tmpResourcePool->rpoid = rpoid;
        tmpResourcePool->parentoid = parentid;
        tmpResourcePool->mempct = DatumGetInt32(mempct);

        errval = strncpy_s(tmpResourcePool->cgroup, NAMEDATALEN, DatumGetCString(DirectFunctionCall1(nameout, ControlGroupDatum)), NAMEDATALEN - 1);
        securec_check_errval(errval, , LOG);
        /* get iops_limits for the pool */
        Datum IOLimitsDatum = heap_getattr(tup, Anum_pg_resource_pool_iops_limits, pg_respool_dsc, &isnull);

        if (!isnull) {
            io_limits_value = DatumGetInt32(IOLimitsDatum);
            tmpResourcePool->iops_limits = io_limits_value;
        }

        Datum IOPrioityDatum = heap_getattr(tup, Anum_pg_resource_pool_io_priority, pg_respool_dsc, &isnull);

        if (!isnull) {
            char* io_priority_datum_value = DatumGetCString(DirectFunctionCall1(nameout, IOPrioityDatum));
            io_priority_value = GetIoPriority(io_priority_datum_value);
            tmpResourcePool->io_priority = io_priority_value;
        }

        Datum NodeGroupDatum = heap_getattr(tup, Anum_pg_resource_pool_nodegroup, pg_respool_dsc, &isnull);

        errval = strncpy_s(tmpResourcePool->ngroup, NAMEDATALEN,
            DatumGetCString(DirectFunctionCall1(nameout, NodeGroupDatum)), NAMEDATALEN - 1);
        securec_check_errval(errval, , LOG);

        Datum ForeignUsersDatum = heap_getattr(tup, Anum_pg_resource_pool_is_foreign, pg_respool_dsc, &isnull);
        tmpResourcePool->is_foreign = DatumGetBool(ForeignUsersDatum);

        temp_respool_list = lappend(temp_respool_list, tmpResourcePool);
    }
    systable_endscan(scan);
    heap_close(relation, lockmode);

    /* start update respool hash */
    USE_AUTO_LWLOCK(ResourcePoolHashLock, LW_EXCLUSIVE);
    AssignHTabInfoDirty<ResourcePool>(g_instance.wlm_cxt->resource_pool_hashtbl);

    ListCell *curr = list_head(temp_respool_list);
    ListCell *next = NULL;

    while (curr != NULL) {
        next = lnext(curr);

        tmpResourcePool = (TmpResourcePool *) lfirst(curr);

        /* create resource pool in hash table */
        ResourcePool *rp = (ResourcePool *) hash_search(g_instance.wlm_cxt->resource_pool_hashtbl,
                                                        &tmpResourcePool->rpoid, HASH_ENTER_NULL, &found);

        if (rp == NULL) {
            ereport(LOG, (errmsg("alloc memory failed. Build resource pool failed")));
            list_free_ext(temp_respool_list);
            return false;
        }

        rp->is_dirty = false;

        if (!found) {
            /* init mutex for resource pool */
            pthread_mutex_init(&rp->mutex, NULL);
            rp->rpoid = tmpResourcePool->rpoid;
        }

        errval = strncpy_s(rp->cgroup, NAMEDATALEN, tmpResourcePool->cgroup, NAMEDATALEN - 1);
        securec_check_errval(errval, , LOG);

        rp->parentoid = DatumGetInt32(tmpResourcePool->parentoid);
        rp->mempct = DatumGetInt32(tmpResourcePool->mempct);
        rp->iops_limits = tmpResourcePool->iops_limits;
        rp->io_priority = tmpResourcePool->io_priority;

        errval = memset_s(rp->ngroup, NAMEDATALEN, 0, NAMEDATALEN);
        securec_check_errval(errval, , LOG);

        rp->is_foreign = false;

        errval = strncpy_s(rp->ngroup, NAMEDATALEN, tmpResourcePool->ngroup, NAMEDATALEN - 1);
        securec_check_errval(errval, , LOG);

        rp->is_foreign = tmpResourcePool->is_foreign;

        /* set the foreign resource pool information in node group structure */
        if (rp->is_foreign && 0 != strcmp(rp->ngroup, DEFAULT_NODE_GROUP)) {
            WLMNodeGroupInfo* ng = WLMGetNodeGroupFromHTAB(rp->ngroup);
            if (ng != NULL) {
                ng->foreignrp = rp;
            }
            rp->node_group = (void*)ng;
        }

        curr = next;
    }

    list_free_ext(temp_respool_list);

    /* remove redundant resource pool info */
    CheckResourcePoolHash();
    return true;
}
/*
 * @Description: remove user info from the configure file
 * @IN udata: user data in htab
 * @Return: void
 * @See also:
 */
void RemoveUserConfig(UserData* udata)
{
    if (udata->infoptr == NULL) {
        return;
    }

    /* reset user info in the configure file */
    gs_lock_test_and_set((int32*)&udata->infoptr->userid, InvalidOid);
    gs_lock_test_and_set_64((int64*)&udata->infoptr->space, 0);
    gs_lock_test_and_set(&udata->infoptr->used, 0);
}

/*
 * @Description: scan user hash table, and remove the redundant user info
 * @IN void
 * @Return: void
 * @See also:
 */
void CheckUserInfoHash(void)
{
    UserData* userdata = NULL;

    HASH_SEQ_STATUS hash_seq;
    hash_seq_init(&hash_seq, g_instance.wlm_cxt->stat_manager.user_info_hashtbl);

    while ((userdata = (UserData*)hash_seq_search(&hash_seq)) != NULL) {
        if (!userdata->is_dirty) {
            continue;
        }

        /* user in system_view is invalid, need remove */
        if (userdata->childlist) {
            list_free(userdata->childlist);
            userdata->childlist = NIL;
        }

        if (userdata->parent != NULL) {
            if (userdata->parent->childlist) {
                userdata->parent->childlist = list_delete_ptr(userdata->parent->childlist, (void*)userdata);
            }

            if (userdata->parent->spillSpace > userdata->spillSpace) {
                userdata->parent->spillSpace -= userdata->spillSpace;
            } else {
                userdata->parent->spillSpace = 0;
            }

            if (userdata->parent->globalSpillSpace > userdata->spillSpace) {
                userdata->parent->globalSpillSpace -= userdata->spillSpace;
            } else {
                userdata->parent->globalSpillSpace = 0;
            }

            userdata->parent = NULL;
        }

        hash_search(g_instance.wlm_cxt->stat_manager.user_info_hashtbl, &userdata->userid, HASH_REMOVE, NULL);
    }
}

bool SearchUsedSpace(Oid userID, int64* permSpace, int64* tempSpace)
{
    HeapTuple tuple;
    bool isNull = false;
    bool isFind = false;
    *permSpace = *tempSpace = 0;
    Relation relation = heap_open(UserStatusRelationId, RowExclusiveLock);
    TupleDesc pgUsedSpaceDsc = RelationGetDescr(relation);

    tuple = SearchSysCache1(USERSTATUSROLEID, ObjectIdGetDatum(userID));
    if (HeapTupleIsValid(tuple)) {
        Datum spaceDatum = heap_getattr(tuple, Anum_pg_user_status_permspace, pgUsedSpaceDsc, &isNull);
        *permSpace = DatumGetInt64(spaceDatum);
        Datum tmpSpaceDatum = heap_getattr(tuple, Anum_pg_user_status_tempspace, pgUsedSpaceDsc, &isNull);
        ereport(DEBUG2, (errmodule(MOD_WLM),
            errmsg("[SearchUsedSpace] set temp space from %ld to %ld", *tempSpace, DatumGetInt64(tmpSpaceDatum))));
        *tempSpace = DatumGetInt64(tmpSpaceDatum);
        isFind = true;
        ReleaseSysCache(tuple);
    }

    heap_close(relation, RowExclusiveLock);
    return isFind;
}

bool heap_tuple_size_changed(Relation rel, HeapTuple tup)
{
    Buffer buf;
    Page page;
    OffsetNumber offNum, maxOff;
    ItemId lp = NULL;
    HeapTupleHeader htup;
    uint32 oldLen;
    uint32 newLen;

    buf = ReadBuffer(rel, ItemPointerGetBlockNumber(&(tup->t_self)));
    LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
    page = (Page)BufferGetPage(buf);

    offNum = ItemPointerGetOffsetNumber(&(tup->t_self));
    maxOff = PageGetMaxOffsetNumber(page);
    if (maxOff >= offNum) {
        lp = PageGetItemId(page, offNum);
    }

    if (maxOff < offNum || !ItemIdIsNormal(lp)) {
        UnlockReleaseBuffer(buf);
        return true;
    }

    htup = (HeapTupleHeader)PageGetItem(page, lp);

    oldLen = ItemIdGetLength(lp) - htup->t_hoff;
    newLen = tup->t_len - tup->t_data->t_hoff;
    if (oldLen != newLen || htup->t_hoff != tup->t_data->t_hoff) {
        UnlockReleaseBuffer(buf);
        return true;
    }
    UnlockReleaseBuffer(buf);
    return false;
}

void UpdateUsedSpace(Oid userID, int64 permSpace, int64 tempSpace)
{
    HeapTuple tuple;
    HeapTuple newTuple;
    Datum newRecord[Natts_pg_user_status] = {0};
    bool newRecordNulls[Natts_pg_user_status] = {0};
    bool newRecordRepl[Natts_pg_user_status] = {0};
    Relation relation = heap_open(UserStatusRelationId, RowExclusiveLock);
    tuple = SearchSysCache1(USERSTATUSROLEID, ObjectIdGetDatum(userID));
    if (HeapTupleIsValid(tuple)) {
        TupleDesc pgUsedSpaceDsc = RelationGetDescr(relation);

        newRecord[Anum_pg_user_status_permspace - 1] = Int64GetDatum(permSpace);
        newRecordRepl[Anum_pg_user_status_permspace - 1] = true;

        newRecord[Anum_pg_user_status_tempspace - 1] = Int64GetDatum(tempSpace);
        newRecordRepl[Anum_pg_user_status_tempspace - 1] = true;

        newTuple = heap_modify_tuple(tuple, pgUsedSpaceDsc, newRecord, newRecordNulls, newRecordRepl);
        if (heap_tuple_size_changed(relation, newTuple)) {
            simple_heap_update(relation, &newTuple->t_self, newTuple);
            CatalogUpdateIndexes(relation, newTuple);
        } else {
            heap_inplace_update(relation, newTuple);
        }

        ReleaseSysCache(tuple);
        heap_freetuple_ext(newTuple);
    }
    heap_close(relation, RowExclusiveLock);
}

/*
 * @Description: build hash table for userdata, including two functions
 *               1, build userdata when process is restarted,
 *                  rebuild hash table from the very beginning
 *               2, if there is inconsistency between hash table and system tables,
 *                  the altered resource pool and user group is considered, besides,
 *                  removing the redundant data in hash table is needed.
 *               the difference with building resource pools is that there is possibility
 *               that users alter there relations with each other.
 * @IN lockmode: lock is acquired on the relation.
 * @Return: void
 * @See also:
 */
bool BuildUserInfoHash(LOCKMODE lockmode)
{
    bool found = false;
    bool isNull = false;

    List *temp_userdata_list = NULL;
    TmpUserData *tempUserData = NULL;

    HeapTuple tup;

    Relation relation = heap_open(AuthIdRelationId, lockmode);

    TupleDesc pg_authid_dsc = RelationGetDescr(relation);

    SysScanDesc scan = systable_beginscan(relation, InvalidOid, false, NULL, 0, NULL);

    /* get all user in system table */
    while (HeapTupleIsValid((tup = systable_getnext(scan)))) {
        bool unlimited = false;
        char* sizestr = NULL;
        int64 spacelimit = 0;
        int64 tmpSpaceLimit = 0;
        int64 spillSpaceLimit = 0;
        Oid puid = InvalidOid;

        Datum authidrespoolDatum = heap_getattr(tup, Anum_pg_authid_rolrespool, pg_authid_dsc, &isNull);

        bool is_super = (((Form_pg_authid)GETSTRUCT(tup))->rolsystemadmin);

        /* get user id */
        Oid userid = HeapTupleGetOid(tup);

        Oid rpoid = get_resource_pool_oid(DatumGetPointer(authidrespoolDatum));

        Datum authidparentidDatum = heap_getattr(tup, Anum_pg_authid_rolparentid, pg_authid_dsc, &isNull);

        /* get parent user id */
        puid = DatumGetObjectId(authidparentidDatum);

        Datum authidspaceDatum = heap_getattr(tup, Anum_pg_authid_roltabspace, pg_authid_dsc, &isNull);

        /* we must make sure the space limit is not null to parse it */
        if (!isNull) {
            char* maxSize = DatumGetCString(DirectFunctionCall1(textout, authidspaceDatum));

            spacelimit = (int64)parseTableSpaceMaxSize(maxSize, &unlimited, &sizestr);
            ereport(DEBUG2, (errmsg("user %u spacelimit: %ldK", userid, spacelimit)));
        }

        Datum authidTempSpaceDatum = heap_getattr(tup, Anum_pg_authid_roltempspace, pg_authid_dsc, &isNull);
        /* we must make sure the temp space limit is not null to parse it */
        if (!isNull) {
            char* maxSize = DatumGetCString(DirectFunctionCall1(textout, authidTempSpaceDatum));

            tmpSpaceLimit = (int64)parseTableSpaceMaxSize(maxSize, &unlimited, &sizestr);
            ereport(DEBUG2, (errmsg("user %u tmpSpaceLimit: %ldK",
                userid, (int64)((uint64)tmpSpaceLimit >> BITS_IN_KB))));
        }

        Datum authidSpillSpaceDatum = heap_getattr(tup, Anum_pg_authid_rolspillspace, pg_authid_dsc, &isNull);
        /* we must make sure the spill space limit is not null to parse it */
        if (!isNull) {
            char* maxSize = DatumGetCString(DirectFunctionCall1(textout, authidSpillSpaceDatum));

            spillSpaceLimit = (int64)parseTableSpaceMaxSize(maxSize, &unlimited, &sizestr);
            ereport(DEBUG2, (errmsg("user %u tmpSpaceLimit: %ldK",
                userid, (int64)((uint64)spillSpaceLimit >> BITS_IN_KB))));
        }

        /* create user data in hash table */
        tempUserData = (TmpUserData *)palloc0_noexcept(sizeof(TmpUserData));

        /* set user id */
        tempUserData->userid        = userid;
        tempUserData->puid          = puid;
        tempUserData->spacelimit    = spacelimit;
        tempUserData->tmpSpaceLimit = tmpSpaceLimit;
        tempUserData->spillSpaceLimit = spillSpaceLimit;
        tempUserData->is_super      = is_super;
        tempUserData->is_dirty      = false;
        tempUserData->rpoid         = rpoid;
        tempUserData->respool       = GetRespoolFromHTab(rpoid);

        temp_userdata_list = lappend(temp_userdata_list, tempUserData);
    }

    systable_endscan(scan);
    heap_close(relation, lockmode);
    
    /* start update userdata hash */
    USE_AUTO_LWLOCK(WorkloadUserInfoLock, LW_EXCLUSIVE);
    AssignHTabInfoDirty<UserData>(g_instance.wlm_cxt->stat_manager.user_info_hashtbl);

    ListCell *curr = list_head(temp_userdata_list);
    ListCell *next = NULL;

    while (curr != NULL) {
        next = lnext(curr);

        tempUserData = (TmpUserData *) lfirst(curr);

        Oid userid = tempUserData->userid;
        Oid puid = tempUserData->puid;
        UserData* userdata = (UserData*)hash_search(
            g_instance.wlm_cxt->stat_manager.user_info_hashtbl, &userid, HASH_ENTER_NULL, &found);

        if (userdata == NULL) {
            ereport(LOG, (errmsg("alloc memory failed.")));
            list_free_ext(temp_userdata_list);
            return false;
        }

        /* set user id */
        userdata->userid = userid;
        userdata->spacelimit = tempUserData->spacelimit;
        userdata->tmpSpaceLimit = tempUserData->tmpSpaceLimit;
        userdata->spillSpaceLimit = tempUserData->spillSpaceLimit;
        userdata->is_super = tempUserData->is_super;
        userdata->is_dirty = false;
        userdata->rpoid = tempUserData->rpoid;
        userdata->respool = GetRespoolFromHTab(tempUserData->rpoid);

        if (!found) {
            userdata->entry_list = NULL;
            pthread_mutex_init(&userdata->mutex, NULL);
        }

        if (userdata->userid == BOOTSTRAP_SUPERUSERID) {
            userdata->totalspace = 0;
            userdata->tmpSpace = 0;
            userdata->globalTmpSpace = 0;
            userdata->spillSpace = 0;
            userdata->globalSpillSpace = 0;
            userdata->global_totalspace = 0;

            curr = next;
            continue;
        }

        // when Alter, or Drop Role, do not update user space info from pg_user_status
        if (!u_sess->wlm_cxt->wlmcatalog_update_user) {
            SearchUsedSpace(userid, &userdata->totalspace, &userdata->tmpSpace);
            userdata->global_totalspace = userdata->totalspace;
            userdata->globalTmpSpace = userdata->tmpSpace;
            userdata->spillSpace = userdata->globalSpillSpace = 0;
        } else if (!found) {
            userdata->global_totalspace = userdata->totalspace = 0;
            userdata->globalTmpSpace = userdata->tmpSpace = 0;
            userdata->spillSpace = userdata->globalSpillSpace = 0;
        }

        /* if user has parent user, we must create parent data */
        if (OidIsValid(puid)) {
            /* alter parent user for userdata */
            if (userdata->parent != NULL && userdata->parent->userid != puid && userdata->parent->childlist != NULL) {
                userdata->parent->childlist = list_delete_ptr(userdata->parent->childlist, (void*)userdata);
            }
            /* if user is group user, but altered to be child user */
            if (userdata->childlist != NULL) {
                list_free(userdata->childlist);
                userdata->childlist = NIL;
            }
            /* create parent data in hash table */
            UserData* parentdata = (UserData*)hash_search(
                g_instance.wlm_cxt->stat_manager.user_info_hashtbl, &puid, HASH_ENTER_NULL, NULL);

            if (parentdata == NULL) {
                ereport(LOG, (errmsg("Cannot allocate memory, build user hash table failed.")));
                list_free_ext(temp_userdata_list);
                return false;
            }

            USE_MEMORY_CONTEXT(g_instance.wlm_cxt->workload_manager_mcxt);

            /* append the user data to the parent child list */
            ListCell* node = append_to_list<Oid, UserData, false>(&parentdata->childlist, &userid, userdata);
            if (node == NULL) {
                REVERT_MEMORY_CONTEXT();
                ereport(LOG, (errmsg("Cannot allocate memory, build user hash table failed.")));
                list_free_ext(temp_userdata_list);
                return false;
            }

            userdata->parent = parentdata;
        } else if (userdata->parent != NULL) {
            /* alter user group default */
            if (userdata->parent->childlist) {
                userdata->parent->childlist = list_delete_ptr(userdata->parent->childlist, (void*)userdata);
            }

            userdata->parent = NULL;
        }
        curr = next;
    }

    list_free_ext(temp_userdata_list);
    /* remove redundant user info */
    CheckUserInfoHash();

    return true;
}

/*
 * @Description: remove the node group from hash table
 * @IN data: node group information
 * @Return: void
 * @See also:
 */
void RemoveNodeGroupFromHash(void* data)
{
    WLMNodeGroupInfo* info = (WLMNodeGroupInfo*)data;

    Assert(info != NULL);

    if (info->is_dirty) {
        gs_lock_test_and_set((int*)&info->used, 0);
    }
}

/*
 * @Description: remove the node group from hash table
 * @IN group_name: the node group name
 * @Return: void
 * @See also:
 */
void RemoveNodeGroupInfoInHTAB(const char* group_name)
{
    WLMNodeGroupInfo* info = NULL;
    if (NodeGroupIsDefault(group_name)) {
        return;
    }

    USE_AUTO_LWLOCK(WorkloadNodeGroupLock, LW_EXCLUSIVE);

    info = (WLMNodeGroupInfo*)hash_search(
        g_instance.wlm_cxt->stat_manager.node_group_hashtbl, group_name, HASH_FIND, NULL);

    if (IS_PGXC_DATANODE) { /* reset the node group info */
        // after modify node group reacquire logic cluster for datanode alarm
        SetFlagForGetLCName(true);
        *g_instance.wlm_cxt->local_dn_ngname = '\0';
        g_instance.wlm_cxt->local_dn_nodegroup = NULL;
        ereport(LOG, (errmsg("During removing nodegroup, its original name is '%s'.", group_name)));
    }

    if (info != NULL) {
        gs_compare_and_swap_32((int*)&info->used, 1, 0);
    }
}

/*
 * @Description: create node group information
 * @IN group_name: node group information
 * @Return: void
 * @See also:
 */
WLMNodeGroupInfo* CreateNodeGroupInfoInHTAB(const char* group_name)
{
    bool found = false;

    USE_AUTO_LWLOCK(WorkloadNodeGroupLock, LW_EXCLUSIVE);

    /* create user data in hash table */
    WLMNodeGroupInfo* info = (WLMNodeGroupInfo*)hash_search(
        g_instance.wlm_cxt->stat_manager.node_group_hashtbl, group_name, HASH_ENTER_NULL, &found);

    RELEASE_AUTO_LWLOCK();

    if (info == NULL) {
        ereport(LOG, (errmsg("alloc memory for node group info failed")));
        return NULL;
    }

    info->is_dirty = false;

    if (!found || !info->used) {
        errno_t errval =
            strncpy_s(info->group_name, sizeof(info->group_name), group_name, sizeof(info->group_name) - 1);
        securec_check_errval(errval, , LOG);

        WLMInitNodeGroupInfo(info);

        if (pg_strcasecmp(VNG_OPTION_ELASTIC_GROUP, group_name) != 0) {
            (void)gscgroup_map_node_group(info);
        }
    }

    if (IS_PGXC_DATANODE) { /* set the node group info */
        // after modify node group reacquire logic cluster for datanode alarm
        SetFlagForGetLCName(true);
        if (g_instance.wlm_cxt->local_dn_nodegroup != NULL) {
            ereport(LOG,
                (errmsg("During creating nodegroup, local_dn_ngname: %s has been created, no need to assign "
                        "group_name: %s to it.",
                    g_instance.wlm_cxt->local_dn_ngname,
                    group_name)));
        } else {
            errno_t errval = strncpy_s(g_instance.wlm_cxt->local_dn_ngname,
                sizeof(g_instance.wlm_cxt->local_dn_ngname),
                group_name,
                sizeof(g_instance.wlm_cxt->local_dn_ngname) - 1);
            securec_check_errval(errval, , LOG);

            g_instance.wlm_cxt->local_dn_nodegroup = info;
            ereport(LOG, (errmsg("During creating nodegroup, its name is '%s'.", group_name)));
        }
    }

    gs_compare_and_swap_32((int*)&info->used, 0, 1);

    return info;
}

/*
 * @Description: build hash table for userdata, including two functions
 *               1, build userdata when process is restarted,
 *                  rebuild hash table from the very beginning
 *               2, if there is inconsistency between hash table and system tables,
 *                  the altered resource pool and user group is considered, besides,
 *                  removing the redundant data in hash table is needed.
 *               the difference with building resource pools is that there is possibility
 *               that users alter there relations with each other.
 * @IN lockmode: lock is acquired on the relation.
 * @Return: void
 * @See also:
 */
void BuildNodeGroupInfoHash(LOCKMODE lockmode)
{
    bool isNull = false;
    char group_kind = 'n';

    AssignHTabInfoDirtyWithLock<WLMNodeGroupInfo>(
        g_instance.wlm_cxt->stat_manager.node_group_hashtbl, WorkloadNodeGroupLock);

    HeapTuple tup;

    Relation relation = heap_open(PgxcGroupRelationId, lockmode);

    TupleDesc pg_group_dsc = RelationGetDescr(relation);

    SysScanDesc scan = systable_beginscan(relation, InvalidOid, false, NULL, 0, NULL);

    /* get all user in system table */
    while (HeapTupleIsValid((tup = systable_getnext(scan)))) {
        /* get the group name */
        Datum groupnameDatum = heap_getattr(tup, Anum_pgxc_group_name, pg_group_dsc, &isNull);

        Assert(!isNull);

        char* group_name = DatumGetPointer(groupnameDatum);

        Assert(StringIsValid(group_name));

        Datum groupkindDatum = heap_getattr(tup, Anum_pgxc_group_kind, pg_group_dsc, &isNull);
        if (!isNull) {
            group_kind = DatumGetChar(groupkindDatum);
        }

        /* create user data in hash table */
        if (group_kind == 'v' || group_kind == 'e') {
            (void)CreateNodeGroupInfoInHTAB(group_name);
        }
    }

    systable_endscan(scan);

    heap_close(relation, lockmode);

    ProcessHTabRecordWithLock<WLMNodeGroupInfo>(
        g_instance.wlm_cxt->stat_manager.node_group_hashtbl, WorkloadNodeGroupLock, RemoveNodeGroupFromHash);
}

/*
 * @Description: build hash table for user data and resource pool
 * @IN void
 * @Return: void
 * @See also:
 */
bool BuildUserRPHash(void)
{
    /* build node group firstly and it will be used in resource pool */
    BuildNodeGroupInfoHash(AccessShareLock);

    /* build resource pool hash table */
    if (!BuildResourcePoolHash(AccessShareLock) || !BuildUserInfoHash(AccessShareLock)) {
        return false;
    }

    /* build user data hash table */
    return true;
}

/*
 * @Description: get user's childlist from catalog.
 * @IN userid: get childlist of user with oid "userid"
 * @OUT childlist: childlist of user
 * @IN findall: if true, get all the child users listed in childlist,
 *              Otherwise, it will check if there is one child for the user.
 * @Return: void
 * @See also:
 */
bool GetUserChildlistFromCatalog(Oid userid, List** childlist, bool findall)
{
    bool result = false;
    bool isNULL = false;

    HeapTuple tup;

    Relation relation = heap_open(AuthIdRelationId, AccessShareLock);

    TupleDesc pg_authid_dsc = RelationGetDescr(relation);

    SysScanDesc scan = systable_beginscan(relation, InvalidOid, false, NULL, 0, NULL);

    while (HeapTupleIsValid((tup = systable_getnext(scan)))) {
        Datum authidparentidDatum = heap_getattr(tup, Anum_pg_authid_rolparentid, pg_authid_dsc, &isNULL);

        Oid parentid = DatumGetObjectId(authidparentidDatum);

        Oid roleid = HeapTupleGetOid(tup);

        /* get childlist of userid */
        if (parentid == userid) {
            if (!result) {
                result = true;
            }

            if (childlist != NULL) {
                *childlist = lappend_oid(*childlist, roleid);
            }

            if (!findall) {
                systable_endscan(scan);
                heap_close(relation, AccessShareLock);
                return result;
            }
        }
    }

    systable_endscan(scan);
    heap_close(relation, AccessShareLock);

    return result;
}

/*
 * @Description: get user information from catalog.
 * @IN userid: get information of user with oid "userid"
 * @OUT rpoid: resource pool of userid
 * @OUT parentid: parent oid of userid
 * @OUT issuper: whether the user is super user
 * @Return: void
 * @See also:
 */
void GetUserDataFromCatalog(Oid userid, Oid* rpoid, Oid* parentid, bool* issuper, int64* spacelimit,
    int64* tmpspacelimit, int64* spillspacelimit, char* ngroup, int lenNgroup)
{
    bool isNull = false;

    Oid groupid = InvalidOid;

    Relation relation = heap_open(AuthIdRelationId, AccessShareLock);

    TupleDesc pg_authid_dsc = RelationGetDescr(relation);

    HeapTuple tup = SearchSysCache1(AUTHOID, ObjectIdGetDatum(userid));

    if (!HeapTupleIsValid(tup)) {
        heap_close(relation, AccessShareLock);
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("user %u does not exist", userid)));
    }

    Datum authidrespoolDatum = heap_getattr(tup, Anum_pg_authid_rolrespool, pg_authid_dsc, &isNull);

    /* get resource pool id */
    if (rpoid != NULL) {
        *rpoid = get_resource_pool_oid(DatumGetPointer(authidrespoolDatum));
    }

    if (issuper != NULL) {
        *issuper = (((Form_pg_authid)GETSTRUCT(tup))->rolsystemadmin);
    }

    bool unlimited = false;
    char* sizestr = NULL;
    char* maxSize = NULL;
    Datum authidparentidDatum = heap_getattr(tup, Anum_pg_authid_rolparentid, pg_authid_dsc, &isNull);

    /* get parent user id */
    if (parentid != NULL) {
        *parentid = DatumGetObjectId(authidparentidDatum);
    }

    Datum authidspaceDatum = heap_getattr(tup, Anum_pg_authid_roltabspace, pg_authid_dsc, &isNull);

    /* we must make sure the space limit is not null to parse it */
    if (isNull == false && spacelimit != NULL) {
        maxSize = DatumGetCString(DirectFunctionCall1(textout, authidspaceDatum));
        *spacelimit = (int64)parseTableSpaceMaxSize(maxSize, &unlimited, &sizestr);
        ereport(DEBUG2, (errmsg("user %u spacelimit: %ldK", userid,
            (*(uint64*)spacelimit >> BITS_IN_KB))));
    }

    Datum authidTmpSpaceDatum = heap_getattr(tup, Anum_pg_authid_roltempspace, pg_authid_dsc, &isNull);

    /* we must make sure the temp space limit is not null to parse it */
    if (isNull == false && tmpspacelimit != NULL) {
        maxSize = DatumGetCString(DirectFunctionCall1(textout, authidTmpSpaceDatum));
        *tmpspacelimit = (int64)parseTableSpaceMaxSize(maxSize, &unlimited, &sizestr);
        ereport(DEBUG2, (errmsg("user %u tmpSpaceLimit: %ldK", userid,
            (*(uint64*)tmpspacelimit >> BITS_IN_KB))));
    }

    Datum authidSpillSpaceDatum = heap_getattr(tup, Anum_pg_authid_rolspillspace, pg_authid_dsc, &isNull);

    /* we must make sure the spill space limit is not null to parse it */
    if (isNull == false && spillspacelimit != NULL) {
        maxSize = DatumGetCString(DirectFunctionCall1(textout, authidSpillSpaceDatum));
        *spillspacelimit = (int64)parseTableSpaceMaxSize(maxSize, &unlimited, &sizestr);
        ereport(DEBUG2, (errmsg("user %u spillSpaceLimit: %ldK", userid,
            (*(uint64*)spillspacelimit >> BITS_IN_KB))));
    }

    Datum groupIdDatum = heap_getattr(tup, Anum_pg_authid_rolnodegroup, pg_authid_dsc, &isNull);

    if (!isNull) {
        groupid = DatumGetObjectId(groupIdDatum);
    }

    ReleaseSysCache(tup);

    heap_close(relation, AccessShareLock);

    if (ngroup && OidIsValid(groupid)) {
        errno_t errval = memset_s(ngroup, lenNgroup, 0, lenNgroup);
        securec_check_errval(errval, , LOG);

        if (get_pgxc_groupname(groupid, ngroup) == NULL) {
            ereport(LOG, (errmsg("get group name failed for group oid: %u", groupid)));
        }
    }
}

/*
 * @Description: check user perm space
 * @IN roleid: user id
 * @IN spacelimit: user space limit
 * @Return: void
 * @See also:
 */
void CheckUserPermSpace(Oid roleid, Oid parentid, int64 spacelimit, int64 tmpspacelimit, int64 spillspacelimit)
{
    if (!IS_PGXC_COORDINATOR) {
        return;
    }

    USE_AUTO_LWLOCK(WorkloadUserInfoLock, LW_SHARED);

    UserData* userdata = GetUserDataFromHTab(roleid, true);

    if (userdata == NULL) {
        return;
    }

    if (spacelimit > 0 && userdata->totalspace > spacelimit) {
        RELEASE_AUTO_LWLOCK();
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("user set perm space failed because its used perm space is out of perm space limit.")));
    }

    if (tmpspacelimit > 0 && userdata->tmpSpace > tmpspacelimit) {
        RELEASE_AUTO_LWLOCK();
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("user set temp space failed because its used temp space is out of temp space limit.")));
    }

    if (spillspacelimit > 0 && userdata->spillSpace > spillspacelimit) {
        RELEASE_AUTO_LWLOCK();
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("user set spill space failed because its used spill space is out of spill space limit.")));
    }

    if (OidIsValid(parentid)) {
        UserData* parentdata = GetUserDataFromHTab(parentid, true);

        if (parentdata == NULL) {
            return;
        }

        if (parentdata->spacelimit > 0 && parentdata->totalspace + userdata->totalspace >= parentdata->spacelimit) {
            RELEASE_AUTO_LWLOCK();
            ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                    errmsg("user set group user failed because group user's used perm space will be out of perm space "
                           "limit.")));
        }

        if (parentdata->tmpSpaceLimit > 0 && parentdata->tmpSpace + userdata->tmpSpace >= parentdata->tmpSpaceLimit) {
            RELEASE_AUTO_LWLOCK();
            ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                    errmsg("user set group user failed because group user's used temp space will be out of temp space "
                           "limit.")));
        }

        if (parentdata->spillSpaceLimit > 0 &&
            parentdata->spillSpace + userdata->spillSpace >= parentdata->spillSpaceLimit) {
            RELEASE_AUTO_LWLOCK();
            ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                    errmsg("user set group user failed because group user's used spill space will be out of spill "
                           "space limit.")));
        }
    }
}

inline void ReportGroupSpaceLimit(char* type)
{
    ereport(ERROR,
        (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            errmsg("group user's %s space cannot be less than its child user's %s space limit.", type, type)));
}

inline bool CheckGroupSpaceLimit(int64 child, int64 parent)
{
    return (parent > 0 && child > parent);
}

inline void CheckGroupSpaceLimit(int64 space, int64 tmpspace, int64 spillspace,
     int64 parent_space, int64 parent_tmpspace, int64 parent_spillspace)
{
    if (CheckGroupSpaceLimit(space, parent_space)) {
        ReportGroupSpaceLimit("perm");
    }

    if (CheckGroupSpaceLimit(tmpspace, parent_tmpspace)) {
        ReportGroupSpaceLimit("temp");
    }

    if (CheckGroupSpaceLimit(spillspace, parent_spillspace)) {
        ReportGroupSpaceLimit("spill");
    }
}

inline void CheckChildSpaceLimit(Oid roleid, Oid parentid,
     int64 spacelimit, int64 tmpspacelimit, int64 spillspacelimit)
{
    List* childlist = NIL;
    (void)GetUserChildlistFromCatalog(roleid, &childlist, true);

    if (!OidIsValid(parentid) && childlist != NULL) {
        ListCell* cell = NULL;

        foreach (cell, childlist) {
            int64 child_spacelimit = 0;
            int64 child_tmpspacelimit = 0;
            int64 child_spillspacelimit = 0;
            Oid childoid = lfirst_oid(cell);

            GetUserDataFromCatalog(
                childoid, NULL, NULL, NULL, &child_spacelimit, &child_tmpspacelimit, &child_spillspacelimit, NULL, 0);
            if (CheckGroupSpaceLimit(child_spacelimit, spacelimit)) {
                ReportGroupSpaceLimit("perm");
            }

            if (CheckGroupSpaceLimit(child_tmpspacelimit, tmpspacelimit)) {
                ReportGroupSpaceLimit("temp");
            }

            if (CheckGroupSpaceLimit(child_spillspacelimit, spillspacelimit)) {
                ReportGroupSpaceLimit("spill");
            }
        }
    }
}

/*
 * @Description: check user data space limit in htab
 * @IN roleid: user id
 * @IN parentid: parent user id
 * @IN spacelimit: user space limit
 * @IN is_default: revert the parent user to default(default is nothing)
 * @IN is_changed: space limit is changed
 * @Return: void
 * @See also:
 */
void CheckUserSpaceLimit(Oid roleid, Oid parentid, int64 spacelimit, int64 tmpspacelimit, int64 spillspacelimit,
    bool is_default, bool changed, bool tmpchanged, bool spillchanged)
{
    Oid user_parentid = InvalidOid;
    int64 user_spacelimit = 0;
    int64 user_tmpspacelimit = 0;
    int64 user_spillspacelimit = 0;
    int64 parent_spacelimit = 0;
    int64 parent_tmpspacelimit = 0;
    int64 parent_spillspacelimit = 0;

    if (OidIsValid(roleid)) {
        GetUserDataFromCatalog(
            roleid, NULL, &user_parentid, NULL, &user_spacelimit, &user_tmpspacelimit, &user_spillspacelimit, NULL, 0);
        user_spacelimit = changed ? spacelimit : user_spacelimit;
        user_tmpspacelimit = tmpchanged ? tmpspacelimit : user_tmpspacelimit;
        user_spillspacelimit = spillchanged ? spillspacelimit : user_spillspacelimit;

        if (OidIsValid(parentid)) {
            parent_spacelimit = 0;
            parent_tmpspacelimit = 0;

            GetUserDataFromCatalog(parentid,
                NULL,
                NULL,
                NULL,
                &parent_spacelimit,
                &parent_tmpspacelimit,
                &parent_spillspacelimit,
                NULL,
                0);

            CheckGroupSpaceLimit(user_spacelimit,
                user_tmpspacelimit,
                user_spillspacelimit,
                parent_spacelimit,
                parent_tmpspacelimit,
                parent_spillspacelimit);
        } else if (OidIsValid(user_parentid) && !is_default) {
            GetUserDataFromCatalog(user_parentid,
                NULL,
                NULL,
                NULL,
                &parent_spacelimit,
                &parent_tmpspacelimit,
                &parent_spillspacelimit,
                NULL,
                0);

            CheckGroupSpaceLimit(user_spacelimit,
                user_tmpspacelimit,
                user_spillspacelimit,
                parent_spacelimit,
                parent_tmpspacelimit,
                parent_spillspacelimit);
        } else {
            CheckChildSpaceLimit(
                roleid, user_parentid, user_spacelimit, user_tmpspacelimit, user_spillspacelimit);
        }

        CheckUserPermSpace(roleid, parentid, user_spacelimit, user_tmpspacelimit, user_spillspacelimit);
    } else {
        if (OidIsValid(parentid)) {
            GetUserDataFromCatalog(parentid,
                NULL,
                NULL,
                NULL,
                &parent_spacelimit,
                &parent_tmpspacelimit,
                &parent_spillspacelimit,
                NULL,
                0);

            if (parent_spacelimit > 0 && spacelimit > parent_spacelimit) {
                ReportGroupSpaceLimit("perm");
            }

            if (parent_tmpspacelimit > 0 && tmpspacelimit > parent_tmpspacelimit) {
                ReportGroupSpaceLimit("temp");
            }

            if (parent_spillspacelimit > 0 && spillspacelimit > parent_spillspacelimit) {
                ReportGroupSpaceLimit("spill");
            }
        }
    }
}
/*
 * @Description: check two user whether in one group
 * @IN UserData: user1
 * @IN UserData: user2
 * @Return: bool
 * @See also:
 */
bool UsersInOneGroup(UserData* user1, UserData* user2)
{
    if (user1 == NULL || user2 == NULL) {
        ereport(ERROR, (errmsg("need two valid user.")));
    }

    if (user1->userid == user2->userid) {
        return true;
    }

    if (user1->parent == NULL) {
        if (user2->parent == NULL) {
            return false;
        } else if (user1->userid == user2->parent->userid) {
            return true;
        } else {
            return false;
        }
    }

    if (user2->parent == NULL) {
        if (user1->parent == NULL) {
            return false;
        } else if (user2->userid == user1->parent->userid) {
            return true;
        } else {
            return false;
        }
    }

    if (user1->parent != NULL && user2->parent != NULL) {
        if (user1->parent->userid == user2->parent->userid) {
            return true;
        } else {
            return false;
        }
    }
    return false;
}
/*
 * @Description: check the relation of parent user and user
 * @IN roleid: user id
 * @IN parentid: parent user id
 * @IN rpoid: resource pool id.
 * @Return: void
 * @See also:
 */
void CheckUserRelation(Oid roleid, Oid parentid, Oid rpoid, bool isDefault, int issuper)
{
    Oid parent_rpoid = InvalidOid;

    Oid user_parentid = InvalidOid;
    Oid user_rpoid = InvalidOid;

    bool user_has_child = false;
    bool user_is_super = false;
    bool rp_is_parent = false;

    FormData_pg_resource_pool_real parent_rp_rpdata;
    FormData_pg_resource_pool_real rp_rpdata;

    /* resource pool and parent are all not set, nothing to check */
    if (!isDefault && !OidIsValid(rpoid) && !OidIsValid(parentid) && (issuper <= 0)) {
        return;
    }

    /* parent user is set */
    if (OidIsValid(parentid)) {
        /*
         * acquire lock on parent,
         * so add user group for roleid
         * will block the deletion of the parent user
         */
        LockSharedObject(AuthIdRelationId, parentid, 0, AccessShareLock);

        /* get parent user's rpoid */
        GetUserDataFromCatalog(parentid, &parent_rpoid, NULL, NULL, NULL, NULL, NULL, NULL, 0);

        if (!get_resource_pool_param(parent_rpoid, &parent_rp_rpdata)) {
            ereport(ERROR,
                (errcode(ERRCODE_RESOURCE_POOL_ERROR),
                    errmsg("cannot get resource pool information of user %u", parentid)));
        }

        /* parent users cannot use non-parent resource pool */
        if (strchr(NameStr(parent_rp_rpdata.control_group), ':')) {
            ereport(ERROR,
                (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                    errmsg("user %u cannot be used as user group, for it uses non-parent resource pool %s",
                        parentid,
                        NameStr(parent_rp_rpdata.respool_name))));
        }

        /* cannot specify user group only */
        if (!OidIsValid(rpoid)) {
            ereport(ERROR,
                (errcode(ERRCODE_SYNTAX_ERROR),
                    errmsg("please specify resource pool when create or alter user with "
                           "user group %u",
                        parentid)));
        }
    }

    /* resource pool is set */
    if (OidIsValid(rpoid)) {
        List* userlist = NIL;

        errno_t rc =
            memset_s(&rp_rpdata, sizeof(FormData_pg_resource_pool_real), 0, sizeof(FormData_pg_resource_pool_real));
        securec_check(rc, "\0", "\0");

        if (!get_resource_pool_param(rpoid, &rp_rpdata)) {
            ereport(ERROR,
                (errcode(ERRCODE_RESOURCE_POOL_ERROR),
                    errmsg("cannot get resource pool information of resource pool %u", rpoid)));
        }

        if (strchr(NameStr(rp_rpdata.control_group), ':') == NULL) {
            rp_is_parent = true;
        }

        /* parent user must be unique for resource pool */
        if (rp_is_parent && (CheckDependencyOnRespool(AuthIdRelationId, rpoid, &userlist, false))) {
            ereport(ERROR,
                (errcode(ERRCODE_OBJECT_IN_USE),
                    errmsg("resource pool %u cannot be used, "
                           "for it is used by user %u",
                        rpoid,
                        lfirst_oid(list_head(userlist)))));
        }

        /* set child resource pool only is not allowed */
        if (OidIsValid(rp_rpdata.parentid) && !OidIsValid(parentid)) {
            ereport(ERROR,
                (errcode(ERRCODE_SYNTAX_ERROR),
                    errmsg("please specify user group when create or alter "
                           "user with child resource pool %u.",
                        rpoid)));
        }

        /* set group user and resource pool at the same time */
        if (!OidIsValid(rp_rpdata.parentid) && OidIsValid(parentid)) {
            ereport(ERROR,
                (errcode(ERRCODE_SYNTAX_ERROR),
                    errmsg("non-child resource pool %u cannot be used for child user", rpoid)));
        }

        /* must match */
        if (OidIsValid(rp_rpdata.parentid) && OidIsValid(parentid) && rp_rpdata.parentid != parent_rpoid) {
            ereport(ERROR,
                (errcode(ERRCODE_SYNTAX_ERROR),
                    errmsg("parent user's resource pool(control_group is \"%s\") "
                           "and resource pool specified(control_group is \"%s\") don't match",
                        NameStr(parent_rp_rpdata.control_group),
                        NameStr(rp_rpdata.control_group))));
        }
    }

    if (isDefault) {
        if (OidIsValid(rpoid) && OidIsValid(rp_rpdata.parentid)) {
            ereport(ERROR,
                (errcode(ERRCODE_SYNTAX_ERROR),
                    errmsg("cannot use resource pool %u when alter user with \"user group default\"", rpoid)));
        }
    }

    /* users to alter */
    if (OidIsValid(roleid)) {
        if (GetUserChildlistFromCatalog(roleid, NULL, false)) {
            user_has_child = true;
        }

        GetUserDataFromCatalog(roleid, &user_rpoid, &user_parentid, &user_is_super, NULL, NULL, NULL, NULL, 0);

        if (user_has_child) {
            ereport(ERROR,
                (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                    errmsg("user %u has child users, cannot alter it with any operation", roleid)));
        }

        /* set normal resource pool only is not allowed */
        if (OidIsValid(user_parentid) && OidIsValid(rpoid) && !OidIsValid(rp_rpdata.parentid) && !isDefault) {
            ereport(ERROR,
                (errcode(ERRCODE_SYNTAX_ERROR),
                    errmsg("please specify \"user group default\" when create or alter "
                           "user with non-child resource pool %u.",
                        rpoid)));
        }

        if (isDefault && OidIsValid(user_parentid) && !OidIsValid(rpoid)) {
            ereport(ERROR,
                (errcode(ERRCODE_SYNTAX_ERROR),
                    errmsg("please specify resource pool "
                           "when alter user with \"user group default\"")));
        }
    }
    /* sysadmin cannot use multi-tenant resource pool */
    if (issuper > 0 || (OidIsValid(roleid) && user_is_super)) {
        if ((OidIsValid(rpoid) && (rp_is_parent || OidIsValid(rp_rpdata.parentid))) ||
            (!OidIsValid(rpoid) && (user_has_child || OidIsValid(user_parentid)))) {
            ereport(ERROR,
                (errcode(ERRCODE_SYNTAX_ERROR),
                    errmsg("super user cannot use resource pool in the two-layer resource pool list.")));
        }
    }
}

/*
 * @Description: get the count of user's children
 * @IN roleid: user id
 * @IN stmt: drop role statement
 * @Return: count of user's children
 * @See also:
 */
int UserGetChildRoles(Oid roleid, DropRoleStmt* stmt)
{
    List* childlist = NIL;
    ListCell* child = NULL;

    (void)GetUserChildlistFromCatalog(roleid, &childlist, true);

    /* get all child user name */
    foreach (child, childlist) {
        Oid childoid = lfirst_oid(child);

        char* rolename = (char*)palloc0(NAMEDATALEN);

        /* get role name with user id */
        (void)GetRoleName(childoid, rolename, NAMEDATALEN);

        if (*rolename) {
            stmt->roles = lappend(stmt->roles, makeString(rolename));
        }
    }
    /*
     * for group users, drop child roles recursively,
     * but hash table will not be updated until
     * all the roles are droped in system tables.
     */
    if (list_length(childlist) > 0) {
        ereport(NOTICE,
            (errmsg("all child roles of %u are "
                    "dropped simultaneously.",
                roleid)));
    }
    /* count of children */
    return list_length(childlist);
}

/*
 * @Description: get all user data
 * @OUT num: user data num
 * @Return: user data list
 * @See also:
 */
void* WLMGetAllUserData(int* num)
{
    int idx = 0;

    HASH_SEQ_STATUS hash_seq;
    ListCell* child = NULL;

    USE_AUTO_LWLOCK(WorkloadUserInfoLock, LW_SHARED);

    /* get current session info count, we will do nothing if it's 0 */
    if (g_instance.wlm_cxt->stat_manager.user_info_hashtbl == NULL ||
        (*num = hash_get_num_entries(g_instance.wlm_cxt->stat_manager.user_info_hashtbl)) == 0) {
        return NULL;
    }

    UserData* udata = NULL;
    WLMUserInfo* info = (WLMUserInfo*)palloc0(*num * sizeof(WLMUserInfo));

    hash_seq_init(&hash_seq, g_instance.wlm_cxt->stat_manager.user_info_hashtbl);

    /* Get all user data from the register info hash table. */
    while ((udata = (UserData*)hash_seq_search(&hash_seq)) != NULL) {
        info[idx].userid = udata->userid;

        if (udata->respool != NULL) {
            info[idx].rpoid = udata->respool->rpoid;
        }

        if (udata->parent != NULL) {
            info[idx].parentid = udata->parent->userid;
        }

        info[idx].admin = udata->is_super;
        info[idx].space = udata->totalspace;
        info[idx].spacelimit = udata->spacelimit;

        /* get child count */
        info[idx].childcount = list_length(udata->childlist);

        if (udata->childlist) {
            StringInfoData buf;

            initStringInfo(&buf);

            /* print child oid list */
            foreach (child, udata->childlist) {
                UserData* childata = (UserData*)lfirst(child);
                if (*buf.data) {
                    appendStringInfo(&buf, _(",%u"), childata->userid);
                } else {
                    appendStringInfo(&buf, _("%u"), childata->userid);
                }
            }

            info[idx].children = buf.data;
        }

        ++idx;
    }

    /* user data num */
    *num = idx;

    return info;
}

/*
 * @Description: get user resource data
 * @IN username: user's name
 * @Return: user resource data
 * @See also:
 */
UserResourceData* GetUserResourceData(const char* username)
{
    /* we get user resource data while workload manager is valid */
    if (!ENABLE_WORKLOAD_CONTROL || !StringIsValid(username)) {
        return NULL;
    }

    WLMNodeGroupInfo* ng = NULL;

    Oid userid = get_role_oid(username, false);

    FormData_pg_resource_pool_real rpdata;

    UserResourceData* rsdata = (UserResourceData*)palloc0(sizeof(UserResourceData));

    rsdata->userid = userid;

    /* get the node group info based on user name */
    ng = WLMGetNodeGroupByUserId(userid);
    if (NULL == ng) {
        ereport(
            ERROR, (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED), errmsg("cannot get logic cluster by user %s.", username)));
    }

    WLMAutoLWLock user_lock(WorkloadUserInfoLock, LW_SHARED);

    user_lock.AutoLWLockAcquire();

    UserData* userdata = GetUserDataFromHTab(userid, false);

    if (userdata->respool == NULL || get_resource_pool_param(userdata->respool->rpoid, &rpdata) == NULL) {
        user_lock.AutoLWLockRelease();
        ereport(ERROR,
            (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                errmsg("cannot find resource pool with user: %u", userdata->userid)));
    }

    /* if max dynamic memory is valid, we will use it as max memory */
    if (maxChunksPerProcess > 0) {
        rsdata->total_memory = GetRPMemorySize(rpdata.mem_percent, rpdata.parentid) >> BITS_IN_KB;
    }

    // track io info
    if (userdata->respool != NULL) {
        rsdata->iops_limits = userdata->respool->iops_limits;
        rsdata->io_priority = userdata->respool->io_priority;
    }

    rsdata->curr_iops_limit = userdata->ioinfo.curr_iops_limit;

    rsdata->maxcurr_iops = userdata->ioinfo.currIopsData.max_value;
    rsdata->mincurr_iops = userdata->ioinfo.currIopsData.min_value;
    rsdata->maxpeak_iops = userdata->ioinfo.peakIopsData.max_value;
    rsdata->minpeak_iops = userdata->ioinfo.peakIopsData.min_value;

    /* IO flow data */
    if (IS_PGXC_COORDINATOR) {
        if (userdata->userid != BOOTSTRAP_SUPERUSERID) {
            if (userdata->ioinfo.read_bytes[1] > userdata->ioinfo.read_bytes[0]) {
                rsdata->read_bytes = userdata->ioinfo.read_bytes[1] - userdata->ioinfo.read_bytes[0];
            }
            if (userdata->ioinfo.write_bytes[1] > userdata->ioinfo.write_bytes[0]) {
                rsdata->write_bytes = userdata->ioinfo.write_bytes[1] - userdata->ioinfo.write_bytes[0];
            }
            if (userdata->ioinfo.read_counts[1] > userdata->ioinfo.read_counts[0]) {
                rsdata->read_counts = userdata->ioinfo.read_counts[1] - userdata->ioinfo.read_counts[0];
            }
            if (userdata->ioinfo.write_counts[1] > userdata->ioinfo.write_counts[0]) {
                rsdata->write_counts = userdata->ioinfo.write_counts[1] - userdata->ioinfo.write_counts[0];
            }
        }
    } else {
        if (userdata->userid != BOOTSTRAP_SUPERUSERID) {
            rsdata->read_bytes = userdata->ioinfo.read_bytes[1];
            rsdata->write_bytes = userdata->ioinfo.write_bytes[1];
            rsdata->read_counts = userdata->ioinfo.read_counts[1];
            rsdata->write_counts = userdata->ioinfo.write_counts[1];
        }
    }
    rsdata->read_speed = userdata->ioinfo.read_speed;
    rsdata->write_speed = userdata->ioinfo.write_speed;

    rsdata->total_space = (int64)((uint64)userdata->spacelimit >> BITS_IN_KB);
    rsdata->total_temp_space = (int64)((uint64)userdata->tmpSpaceLimit >> BITS_IN_KB);
    rsdata->total_spill_space = (int64)((uint64)userdata->spillSpaceLimit >> BITS_IN_KB);

    if (IS_PGXC_COORDINATOR) {
        // re-calculate global spill space here
        if (list_length(userdata->childlist) > 0) {
            int64 parentSpillSpace = 0;
            ListCell* cell = NULL;

            /* set parent total space */
            foreach (cell, userdata->childlist) {
                UserData* childdata = (UserData*)lfirst(cell);
                parentSpillSpace += childdata->globalSpillSpace;
            }

            userdata->globalSpillSpace = userdata->spillSpace + parentSpillSpace;
            WLMReAdjustUserSpace(userdata);
        }
    }

    if (rsdata->total_space == 0) {
        rsdata->total_space = -1;
    }
    if (rsdata->total_temp_space == 0) {
        rsdata->total_temp_space = -1;
    }
    if (rsdata->total_spill_space == 0) {
        rsdata->total_spill_space = -1;
    }

    rsdata->used_space = (int64)((uint64)userdata->totalspace >> BITS_IN_KB);
    if (rsdata->total_space != -1 &&
        rsdata->used_space > rsdata->total_space) {
        rsdata->used_space = rsdata->total_space;
    }

    rsdata->used_temp_space = (int64)((uint64)userdata->tmpSpace >> BITS_IN_KB);
    if (rsdata->total_temp_space != -1 &&
        rsdata->used_temp_space > rsdata->total_temp_space) {
        rsdata->used_temp_space = rsdata->total_temp_space;
    }

    if (IS_PGXC_COORDINATOR) {
        rsdata->used_spill_space = (int64)((uint64)userdata->globalSpillSpace >> BITS_IN_KB);
    } else {
        rsdata->used_spill_space = (int64)((uint64)userdata->spillSpace >> BITS_IN_KB);
    }
    if (rsdata->total_spill_space != -1 &&
        rsdata->used_spill_space > rsdata->total_spill_space) {
        rsdata->used_spill_space = rsdata->total_spill_space;
    }

    rsdata->used_memory = userdata->memsize;
    if (rsdata->used_memory > rsdata->total_memory && rsdata->total_memory != 0) {
        rsdata->used_memory = rsdata->total_memory;
    }

    /* set total_memory as max available memory when mem_percent is 0 */
    if (maxChunksPerProcess > 0 && rsdata->total_memory == 0) {
        rsdata->total_memory = GetRPMemorySize(FULL_PERCENT, rpdata.parentid) >> BITS_IN_KB;
    }

    rsdata->total_cpuset = userdata->totalCpuCnt;
    rsdata->used_cpuset = userdata->usedCpuCnt;

    user_lock.AutoLWLockRelease();

    return rsdata;
}
/*
 * @Description: update hash table when commit transaction
 * @See also:
 */
void UpdateWlmCatalogInfoHash(void)
{
    int count = 0;
    /*
     * doesn't need insert into hash table in the starting up process
     */
    while (!WLMIsInfoInit()) {
        pg_usleep(USECS_PER_SEC / 2);

        if (++count > 10) {
            ResetWlmCatalogFlag();
            ereport(LOG, (errmsg("Update user and resource pool Hash table failed, for time is out. ")));

            WLMSetBuildHashStat(0);
            return;
        }
    }

    /* create resource pool */
    if (OidIsValid(u_sess->wlm_cxt->respool_create_oid)) {
        if (!RespoolHashTabInsert(u_sess->wlm_cxt->respool_create_oid,
                u_sess->wlm_cxt->respool_io_limit_update,
                u_sess->wlm_cxt->respool_io_pri_update,
                u_sess->wlm_cxt->respool_controlgroup,
                u_sess->wlm_cxt->respool_nodegroup,
                u_sess->wlm_cxt->respool_is_foreign)) {
            WLMSetBuildHashStat(0);
            return;
        }

        u_sess->wlm_cxt->respool_create_oid = InvalidOid;
        u_sess->wlm_cxt->respool_io_limit_update = -1;
        u_sess->wlm_cxt->respool_is_foreign = false;
        u_sess->wlm_cxt->respool_node_group = NULL;

        if (memset_s(u_sess->wlm_cxt->respool_io_pri_update, NAMEDATALEN, 0, NAMEDATALEN) != EOK ||
            memset_s(u_sess->wlm_cxt->respool_controlgroup, NAMEDATALEN, 0, NAMEDATALEN) != EOK ||
            memset_s(u_sess->wlm_cxt->respool_nodegroup, NAMEDATALEN, 0, NAMEDATALEN) != EOK) {
            WLMSetBuildHashStat(0);
            return;
        }
    }

    if (OidIsValid(u_sess->wlm_cxt->respool_alter_oid)) {
        if (!RespoolHashTabInsert(u_sess->wlm_cxt->respool_alter_oid,
                u_sess->wlm_cxt->respool_io_limit_update,
                u_sess->wlm_cxt->respool_io_pri_update,
                u_sess->wlm_cxt->respool_controlgroup,
                NULL,
                false)) {
            WLMSetBuildHashStat(0);
            return;
        }

        u_sess->wlm_cxt->respool_alter_oid = InvalidOid;
        u_sess->wlm_cxt->respool_io_limit_update = -1;
        u_sess->wlm_cxt->respool_node_group = NULL;
        u_sess->wlm_cxt->respool_foreign_mempct = -1;

        if (memset_s(u_sess->wlm_cxt->respool_io_pri_update, NAMEDATALEN, 0, NAMEDATALEN) != EOK ||
            memset_s(u_sess->wlm_cxt->respool_controlgroup, NAMEDATALEN, 0, NAMEDATALEN) != EOK) {
            WLMSetBuildHashStat(0);
            return;
        }
    }

    /* drop resource pool and its child resource pools */
    if (u_sess->wlm_cxt->respool_delete_list) {
        u_sess->wlm_cxt->respool_delete_list = RespoolHashTabRemove(u_sess->wlm_cxt->respool_delete_list);
    }

    /* update user info */
    if (u_sess->wlm_cxt->wlmcatalog_update_user) {
        if (!BuildUserInfoHash(NoLock)) {
            WLMSetBuildHashStat(0);
            return;
        }

        u_sess->wlm_cxt->wlmcatalog_update_user = false;
    }
}
/*
 * @Description: reset the flags when abort transaction
 * @See also:
 */
void ResetWlmCatalogFlag(void)
{
    u_sess->wlm_cxt->wlmcatalog_update_user = false;
    u_sess->wlm_cxt->respool_create_oid = InvalidOid;
    u_sess->wlm_cxt->respool_alter_oid = InvalidOid;
    u_sess->wlm_cxt->respool_io_limit_update = -1;
    errno_t rc = memset_s(u_sess->wlm_cxt->respool_io_pri_update, NAMEDATALEN, 0, NAMEDATALEN);
    securec_check(rc, "\0", "\0");

    if (u_sess->wlm_cxt->respool_delete_list) {
        if (list_head(u_sess->wlm_cxt->respool_delete_list)) {
            list_free(u_sess->wlm_cxt->respool_delete_list);
        }
        u_sess->wlm_cxt->respool_delete_list = NIL;
    }
}

/*
 * function name: WLMSetSessionRespool
 * description  : Set session_respool name "session_respool"
 */
void WLMSetSessionRespool(const char* respool)
{
    errno_t rc;
    Oid rpoid = InvalidOid;
    FormData_pg_resource_pool_real rpdata;

    if (*respool && strcmp(respool, INVALID_POOL_NAME) == 0) {
        if (*u_sess->wlm_cxt->session_respool == '\0' || strcmp(u_sess->wlm_cxt->session_respool, respool) != 0) {
            rc = snprintf_s(u_sess->wlm_cxt->session_respool,
                sizeof(u_sess->wlm_cxt->session_respool),
                sizeof(u_sess->wlm_cxt->session_respool) - 1,
                "%s",
                respool);
            securec_check_ss(rc, "\0", "\0");

            if (u_sess->wlm_cxt->session_respool_initialize) {
                GetUserDataFromCatalog(GetUserId(), &rpoid, NULL, NULL, NULL, NULL, NULL, NULL, 0);

                if (OidIsValid(rpoid) && get_resource_pool_param(rpoid, &rpdata)) {
                    errno_t errval = strncpy_s(
                        u_sess->wlm_cxt->control_group, NAMEDATALEN, NameStr(rpdata.control_group), NAMEDATALEN - 1);
                    securec_check_errval(errval, , LOG);

                    gscgroup_convert_cgroup(u_sess->wlm_cxt->control_group);

                    u_sess->wlm_cxt->cgroup_state = CG_USERSET;
                }
            } else {
                u_sess->wlm_cxt->session_respool_initialize = true;
            }
        }
        return;
    }

    if (*u_sess->wlm_cxt->session_respool == '\0' || strcmp(u_sess->wlm_cxt->session_respool, respool) != 0) {
        if (*u_sess->wlm_cxt->session_respool) {
            ereport(DEBUG1,
                (errmsg("session_respool is changed from %s to %s", u_sess->wlm_cxt->session_respool, respool)));
        } else {
            ereport(DEBUG1, (errmsg("session_respool is set to %s", respool)));
        }

        rc = snprintf_s(u_sess->wlm_cxt->session_respool,
            sizeof(u_sess->wlm_cxt->session_respool),
            sizeof(u_sess->wlm_cxt->session_respool) - 1,
            "%s",
            respool);
        securec_check_ss(rc, "\0", "\0");
    }

    if (*u_sess->wlm_cxt->session_respool) {
        rpoid = get_resource_pool_oid(u_sess->wlm_cxt->session_respool);

        if (OidIsValid(rpoid) && get_resource_pool_param(rpoid, &rpdata)) {
            errno_t errval =
                strncpy_s(u_sess->wlm_cxt->control_group, NAMEDATALEN, NameStr(rpdata.control_group), NAMEDATALEN - 1);
            securec_check_errval(errval, , LOG);

            gscgroup_convert_cgroup(u_sess->wlm_cxt->control_group);

            u_sess->wlm_cxt->cgroup_state = CG_USERSET;
        }
    }
}

/*
 * function name: WLMCheckSessionRespool
 * description  : check session_respool name "session_respool" is valid or not
 */
void WLMCheckSessionRespool(const char* respool)
{
    if (strcmp(respool, INVALID_POOL_NAME) == 0) {
        return;
    }

    if (!OidIsValid(t_thrd.shemem_ptr_cxt.MyBEEntry->st_userid)) {
        return;
    }

    WLMNodeGroupInfo* ng = WLMGetNodeGroupByUserId(t_thrd.shemem_ptr_cxt.MyBEEntry->st_userid);
    if (NULL == ng) {
        ereport(ERROR,
            (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                errmsg("Failed to get logic cluster information by user(oid %u).",
                    t_thrd.shemem_ptr_cxt.MyBEEntry->st_userid)));
    }

    Oid rpoid = get_resource_pool_oid(respool);
    FormData_pg_resource_pool_real rpdata;

    if (!OidIsValid(rpoid)) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_NAME), errmsg("session_respool name \"%s\" does not exist", respool)));
    }

    // parent pool cannot be used as session_respool
    if (get_resource_pool_param(rpoid, &rpdata)) {
        if (strchr(NameStr(rpdata.control_group), ':') == NULL) {
            ereport(ERROR,
                (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                    errmsg("parent resource pool \"%s\" cannot be used as sesion_respool", respool)));
        }

        if (strcmp(NameStr(rpdata.nodegroup), ng->group_name) != 0) {
            ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                    errmsg("resource pool \"%s\" is not in logical cluster \"%s\".", respool, ng->group_name)));
        }
    }
}

/*
 * @Description: get memory of user
 * @IN userdata: user data
 * @Return: memory size used
 * @See also:
 */
int WLMGetUserMemory(UserData* userdata)
{
    Assert(userdata != NULL);

    int memsize = 0;

    USE_CONTEXT_LOCK(&userdata->mutex);

    foreach_cell(cell, userdata->entry_list)
    {
        SessionLevelMemory* entry = (SessionLevelMemory*)lfirst(cell);

        if (entry != NULL) {
            memsize += entry->queryMemInChunks << (chunkSizeInBits - BITS_IN_MB);
        }
    }

    return memsize;
}

/*
 * @Description: get memory of user
 * @IN userdata: user data
 * @Return: memory size used
 * @See also:
 */
void WLMGetForeignResPoolMemory(ResourcePool* foreign_respool, int* memsize, int* estmsize)
{
    if (NULL == foreign_respool) {
        return;
    }

    USE_CONTEXT_LOCK(&foreign_respool->mutex);

    foreach_cell(cell, foreign_respool->entry_list) {
        SessionLevelMemory* entry = (SessionLevelMemory*)lfirst(cell);

        if (entry != NULL) {
            *memsize += (unsigned int)entry->queryMemInChunks << ((unsigned int)chunkSizeInBits - BITS_IN_MB);
            *estmsize += entry->estimate_memory;
        }
    }
}

/*
 * @Description: Get the node group information based on user oid
 * @IN userid: user oid
 * @IN is_noexcept: true or false
 * @Return: node group information
 * @See also:
 */
WLMNodeGroupInfo* WLMGetNodeGroupByUserId(Oid userid)
{
    char group_name[NAMEDATALEN] = {0};

    GetUserDataFromCatalog(userid, NULL, NULL, NULL, NULL, NULL, NULL, group_name, NAMEDATALEN);

    return WLMGetNodeGroupFromHTAB(group_name);
}

/*
 * @Description: Get the node group information based on group name
 * @IN group_name: group name
 * @IN is_noexcept: true or false
 * @Return: node group information
 * @See also:
 */
WLMNodeGroupInfo* WLMMustGetNodeGroupFromHTAB(const char* group_name)
{
    if (NodeGroupIsDefault(group_name)) {
        return &g_instance.wlm_cxt->MyDefaultNodeGroup;
    }

    USE_AUTO_LWLOCK(WorkloadNodeGroupLock, LW_SHARED);

    WLMNodeGroupInfo* info = (WLMNodeGroupInfo*)hash_search(
        g_instance.wlm_cxt->stat_manager.node_group_hashtbl, group_name, HASH_FIND, NULL);

    if (info == NULL) {
        RELEASE_AUTO_LWLOCK();

        info = CreateNodeGroupInfoInHTAB(group_name);
        if (NULL == info) {
            ereport(ERROR,
                (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                    errmsg("logic cluster %s is not created correctly", group_name)));
        }
    }

    return info;
}

/*
 * @Description: Get the node group information based on group name
 * @IN group_name: group name
 * @IN is_noexcept: true or false
 * @Return: node group information
 * @See also:
 */
WLMNodeGroupInfo* WLMGetNodeGroupFromHTAB(const char* group_name)
{
    if (NodeGroupIsDefault(group_name)) {
        return &g_instance.wlm_cxt->MyDefaultNodeGroup;
    }

    USE_AUTO_LWLOCK(WorkloadNodeGroupLock, LW_SHARED);

    WLMNodeGroupInfo* info = (WLMNodeGroupInfo*)hash_search(
        g_instance.wlm_cxt->stat_manager.node_group_hashtbl, group_name, HASH_FIND, NULL);

    if (pg_strcasecmp(VNG_OPTION_ELASTIC_GROUP, group_name) != 0 && info &&
        (NULL == info->vaddr[0] || NULL == info->cgroups_htab)) {
        ereport(ERROR,
            (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                errmsg("the cgroup file for logic cluster %s is not mapped. "
                       "Please check if it has been created.",
                    info->group_name)));
    }

    if (info == NULL && IS_PGXC_COORDINATOR) {
        Oid group_oid = get_pgxc_groupoid(group_name);
        if (OidIsValid(group_oid)) {
            char group_kind = get_pgxc_groupkind(group_oid);

            if (group_kind == PGXC_GROUPKIND_LCGROUP || group_kind == PGXC_GROUPKING_ELASTIC) {
                ereport(ERROR,
                    (errcode(ERRCODE_CACHE_LOOKUP_FAILED),
                        errmsg("logic cluster %s is not in logic cluster hash table. ", group_name)));
            }
            return &g_instance.wlm_cxt->MyDefaultNodeGroup;
        }
    }

    return info;
}

void WLMReleaseAtThreadExit()
{
    if (u_sess->attr.attr_resource.use_workload_manager && IsUnderPostmaster &&
        (u_sess->wlm_cxt->parctl_state_exit == 0)) {
        u_sess->wlm_cxt->parctl_state_exit = 1;

        if (u_sess->wlm_cxt->parctl_state_control) {
            WLMProcessExiting = true;

            if (g_instance.wlm_cxt->dynamic_workload_inited) {
                if (!COMP_ACC_CLUSTER) {
                    dywlm_client_max_release(&t_thrd.wlm_cxt.parctl_state);
                }

                if (t_thrd.wlm_cxt.parctl_state.rp_reserve) {
                    if (t_thrd.wlm_cxt.parctl_state.simple == 0) {
                        dywlm_client_release(&t_thrd.wlm_cxt.parctl_state);
                    }
                } else if (!WLMIsQueryFinished()) {
                    dywlm_client_clean(&t_thrd.wlm_cxt.parctl_state);
                }

                WLMHandleDywlmSimpleExcept(true);  // handle simple except
                dywlm_client_proc_release();
            } else {
                WLMParctlRelease(&t_thrd.wlm_cxt.parctl_state);
                WLMProcReleaseActiveStatement();
            }

            WLMProcessExiting = false;
        }
    }
}
