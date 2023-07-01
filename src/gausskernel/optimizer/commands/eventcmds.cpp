/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022. All rights reserved.
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
 * eventcmds.cpp
 *        Routines for CREATE/ALTER/DROP EVENT commands.
 * IDENTIFICATION
 *        src/gausskernel/optimizer/commands/eventcmds.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/genam.h"
#include "access/heapam.h"
#include "access/tableam.h"
#include "access/sysattr.h"
#include "catalog/dependency.h"
#include "catalog/indexing.h"
#include "catalog/objectaccess.h"
#include "catalog/pg_object.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_type.h"
#include "catalog/gs_db_privilege.h"
#include "commands/defrem.h"
#include "commands/proclang.h"
#include "executor/executor.h"
#include "knl/knl_thread.h"
#include "miscadmin.h"
#include "optimizer/var.h"
#include "parser/parse_collate.h"
#include "parser/parse_func.h"
#include "storage/tcap.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/guc.h"
#include "utils/inval.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/rel_gs.h"
#include "utils/syscache.h"
#include "utils/snapmgr.h"
#include "utils/plpgsql.h"
#include "storage/lmgr.h"
#include "tcop/utility.h"
#include "catalog/pg_job.h"
#include "tcop/dest.h"
#include "commands/sqladvisor.h"
#include "access/printtup.h"
#include "access/attnum.h"
#include "nodes/makefuncs.h"
#include "postgres_ext.h"
#include "catalog/gs_job_attribute.h"
#include "utils/dbe_scheduler.h"
#include "catalog/pg_job_proc.h"
#include "catalog/pg_authid.h"
#include "commands/user.h"
#include "access/htup.h"
#include "commands/dbcommands.h"

#define SHOW_EVENT_SIZE 10
#define INTERVAL_QUALITY_LENGTH 16

typedef struct field_str_map_st {
    int interval;
    const char *field_str;
} field_str_map;

const field_str_map g_field_str_map[] = {
    {INTERVAL_MASK(YEAR), " year"},
    {INTERVAL_MASK(MONTH), " month"},
    {INTERVAL_MASK(DAY), " day"},
    {INTERVAL_MASK(HOUR), " hour"},
    {INTERVAL_MASK(MINUTE), " minute"},
    {INTERVAL_MASK(SECOND), " second"},
    {INTERVAL_MASK(YEAR) | INTERVAL_MASK(MONTH), " year to month"},
    {INTERVAL_MASK(DAY) | INTERVAL_MASK(HOUR), " day to hour"},
    {INTERVAL_MASK(DAY) | INTERVAL_MASK(HOUR) | INTERVAL_MASK(MINUTE), " day to minute"},
    {INTERVAL_MASK(DAY) | INTERVAL_MASK(HOUR) | INTERVAL_MASK(MINUTE) | INTERVAL_MASK(SECOND), " day to second"},
    {INTERVAL_MASK(HOUR) | INTERVAL_MASK(MINUTE), " hour to minute"},
    {INTERVAL_MASK(HOUR) | INTERVAL_MASK(MINUTE) | INTERVAL_MASK(SECOND), " hour to second"},
    {INTERVAL_MASK(MINUTE) | INTERVAL_MASK(SECOND), " minute to second"},
    {INTERVAL_FULL_RANGE, ""},
};

enum class job_type {
    ARG_JOB_DEFAULT,
    ARG_JOB_ACTION,
    ARG_START_DATE,
    ARG_END_DATE,
    ARG_REPEAT_INTERVAL,
    ARG_JOB_AUTO_DROP,
    ARG_JOB_ENABLED,
    ARG_JOB_COMMENTS,
    ARG_JOB_RENAME,
    ARG_JOB_DEFINER,
    ARG_JOB_DEFINER_DEFAULT
};

typedef struct job_script_map_st {
    job_type act_name;
    const char *act_type;
} job_script;

const job_script g_job_script[] = {{job_type::ARG_JOB_ACTION, "program_action"},
                                   {job_type::ARG_START_DATE, "start_date"},
                                   {job_type::ARG_REPEAT_INTERVAL, "repeat_interval"},
                                   {job_type::ARG_JOB_AUTO_DROP, "auto_drop"},
                                   {job_type::ARG_END_DATE, "end_date"},
                                   {job_type::ARG_JOB_COMMENTS, "comments"},
                                   {job_type::ARG_JOB_ENABLED, "enabled"},
                                   {job_type::ARG_JOB_RENAME, "rename"},
                                   {job_type::ARG_JOB_DEFINER, "owner"},
                                   {job_type::ARG_JOB_DEFINER_DEFAULT, "owner_default"}};

bool CheckEventExists(Datum ev_name, bool miss_ok)
{
    bool is_exists;
    CatCList *list = SearchSysCacheList1(JOBATTRIBUTENAME, ev_name);
    is_exists = (list->n_members > 0) ? true : false;
    ReleaseSysCacheList(list);
    if (!is_exists) {
        return false;
    }
    if (miss_ok) {
        ereport(NOTICE, (errcode(ERRCODE_DUPLICATE_OBJECT),
                         errmsg("event \"%s\" already exists, skipping", TextDatumGetCString(ev_name))));
        return true;
    } else {
        ereport(ERROR, (errcode(ERRCODE_DUPLICATE_OBJECT),
                        errmsg("event \"%s\" already exists", TextDatumGetCString(ev_name))));
    }
    return false;
}

bool CheckEventNotExists(Datum ev_name, bool miss_ok)
{
    bool is_exists;
    CatCList *list = SearchSysCacheList1(JOBATTRIBUTENAME, ev_name);
    is_exists = (list->n_members > 0) ? true : false;
    ReleaseSysCacheList(list);
    if (is_exists) {
        return false;
    }
    if (miss_ok) {
        ereport(NOTICE, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                         errmsg("event \"%s\" is not exists, skipping", TextDatumGetCString(ev_name))));
        return true;
    } else {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("event \"%s\" is not exists", TextDatumGetCString(ev_name))));
    }
    return false;
}

const char *IntervalTypmodParse(A_Const *expr)
{
    long interval_num = expr->val.val.ival;
    size_t size = (sizeof(g_field_str_map) / sizeof(g_field_str_map[0]));
    for (size_t i = 0; i < size; ++i) {
        if (g_field_str_map[i].interval == interval_num) {
            return g_field_str_map[i].field_str;
        }
    }
    return NULL;
}

Datum ParseIntevalExpr(Node *intervalNode)
{
    StringInfoData buf;
    initStringInfo(&buf);
    appendStringInfo(&buf, "interval");
    TypeCast *tc = (TypeCast *)intervalNode;
    A_Const *ac = (A_Const *)tc->arg;
    A_Const *tm = (A_Const *)lfirst(list_head(tc->typname->typmods));
    const char *tm_str = NULL;
    tm_str = IntervalTypmodParse(tm);
    if (IsA(&ac->val, Integer)) {
        char *quantity_str = (char *)palloc(INTERVAL_QUALITY_LENGTH);
        pg_itoa((int)ac->val.val.ival, quantity_str);
        appendStringInfo(&buf, " \'%s\' ", quantity_str);
    } else if (IsA(&ac->val, String) || IsA(&ac->val, Float)) {
        appendStringInfo(&buf, " \'%s\' ", (char *)ac->val.val.str);
    }
    appendStringInfo(&buf, "%s", tm_str);
    return CStringGetTextDatum(buf.data);
}

Datum ExecTimeExpr(Node *node)
{
    /* Check whether the execution result of the time expression is of the timestamp type */
    Oid result_type = exprType(node);
    if (result_type != TIMESTAMPOID && result_type != TIMESTAMPTZOID) {
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_DATETIME_FORMAT), errmsg("the format of the time expression is incorrect.")));
    }

    EState *estate = NULL;
    ExprContext *econtext = NULL;
    Expr *time_expr = NULL;
    ExprState *exprstate = NULL;
    time_expr = (Expr *)node;
    bool is_null = false;
    Datum result;

    estate = CreateExecutorState();
    econtext = GetPerTupleExprContext(estate);
    exprstate = ExecPrepareExpr(time_expr, estate);
    if (!PointerIsValid(exprstate)) {
        ereport(ERROR, (errcode(ERRCODE_DUPLICATE_OBJECT),
                        errmsg("failed when making time expression state for constCompare.")));
    }

    result = ExecEvalExpr(exprstate, econtext, &is_null, NULL);
    if (result_type == TIMESTAMPTZOID) {
        result = DirectFunctionCall1(timestamptz_timestamp, DatumGetTimestampTz(result));
    }
    FreeExecutorState(estate);
    return result;
}

void GetTimeExecResult(CreateEventStmt *stmt, Datum &start_time, Datum &interval_time, Datum &end_time)
{
    /* Parse Interval Expression */
    Node *interval_time_expr = stmt->interval_time;
    interval_time = (Datum)0;
    if (interval_time_expr) {
        interval_time = ParseIntevalExpr(interval_time_expr);
    }

    /* Parsing the time expression */
    Node *end_time_expr = stmt->end_time_expr;
    end_time = get_scheduler_max_timestamptz();
    if (end_time_expr) {
        end_time = ExecTimeExpr(end_time_expr);
    }

    Node *start_time_expr = stmt->start_time_expr;
    start_time = DirectFunctionCall1(timestamptz_timestamp, DatumGetTimestampTz(GetCurrentTimestamp()));
    if (start_time_expr) {
        start_time = ExecTimeExpr(start_time_expr);
    }
}

Datum TranslateArg(char *act_name, Node *act_node)
{
    Datum result = (Datum)0;
    job_type ev_act_type = job_type::ARG_JOB_DEFAULT;
    size_t size = (sizeof(g_job_script) / sizeof(g_job_script[0]));
    for (size_t i = 0; i < size; ++i) {
        if (!strcmp(g_job_script[i].act_type, act_name)) {
            ev_act_type = g_job_script[i].act_name;
            break;
        }
    }
    switch (ev_act_type) {
        case job_type::ARG_JOB_ACTION:
        case job_type::ARG_JOB_COMMENTS: {
            Value *event_arg_node = (Value *)act_node;
            result = CStringGetTextDatum(event_arg_node->val.str);
            break;
        }
        case job_type::ARG_JOB_RENAME: {
            Value *new_name_val = (Value *)act_node;
            result = CStringGetTextDatum(new_name_val->val.str);
            break;
        }
        case job_type::ARG_JOB_DEFINER: {
            Value *ev_definer_node = (Value *)act_node;
            HOLD_INTERRUPTS();
            Oid user_oid = GetSysCacheOid1(AUTHNAME, CStringGetDatum(ev_definer_node->val.str));
            RESUME_INTERRUPTS();
            CHECK_FOR_INTERRUPTS();
            if (!OidIsValid(user_oid))
                ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT),
                                errmsg("role \"%s\" does not exist.", ev_definer_node->val.str)));
            char *name = (char *)palloc(MAX_JOB_NAME_LEN * sizeof(char));
            pg_ltoa((int)user_oid, name);
            result = CStringGetTextDatum(name);
            break;
        }
        case job_type::ARG_JOB_DEFINER_DEFAULT: {
            char *username = get_role_name_str();
            result =  CStringGetTextDatum(username);
            break;
        }
        case job_type::ARG_START_DATE:
        case job_type::ARG_END_DATE: {
            result = ExecTimeExpr(act_node);
            break;
        }
        case job_type::ARG_REPEAT_INTERVAL: {
            result = ParseIntevalExpr(act_node);
            break;
        }
        case job_type::ARG_JOB_ENABLED: {
            Value *ev_status_node = (Value *)act_node;
            EventStatus ev_status = (EventStatus)(ev_status_node->val.ival);
            if (ev_status == EVENT_DISABLE_ON_SLAVE || ev_status == EVENT_DISABLE) {
                result = BoolGetDatum(0);
            } else if (ev_status == EVENT_ENABLE) {
                result = BoolGetDatum(1);
            }
            break;
        }
        case job_type::ARG_JOB_AUTO_DROP: {
            Value *ev_drop_node = (Value *)act_node;
            result = ev_drop_node->val.ival ? CStringGetTextDatum("true") : CStringGetTextDatum("false");
            break;
        }
        default:
            return (Datum)0;
    }
    return result;
}

void CheckDefinerPriviledge(char *user_name)
{
    Oid user_oid = GetSysCacheOid1(AUTHNAME, CStringGetDatum(user_name));
    Oid init_user_id = 10;
    if (user_oid != GetUserId()) {
        if (g_instance.attr.attr_security.enablePrivilegesSeparate) {
            ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                            errmsg("definer_name cannot be specified when PrivilegesSeparate is enabled.")));
        } else if (user_oid == init_user_id) {
            ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                            errmsg("definer_name cannot be specified as the initial user.")));
        } else if (is_role_independent(user_oid) || isMonitoradmin(user_oid) || isOperatoradmin(user_oid)) {
            ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                    errmsg("definer_name cannot be specified as a private user, operator admin, or monitoradmin.")));
        }
    }
}

Datum SetDefinerName(char *def_name, Datum program_name, char** definer_oid)
{
    Datum curuser = get_priv_user(program_name, CharGetDatum(JOB_INTYPE_PLAIN));
    if (def_name) {
        CheckDefinerPriviledge(def_name);
        if (!superuser()) {
            ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                            errmsg("The current user does not have sufficient permissions to specify the definer.")));
        } else {
            HeapTuple oldtuple = SearchSysCache1(AUTHNAME, CStringGetDatum(def_name));
            Value* definer_name = makeString(def_name);
            (*definer_oid) = TextDatumGetCString(TranslateArg("owner", (Node*)definer_name));
            if (!HeapTupleIsValid(oldtuple)) {
                ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("role \"%s\" does not exist.", def_name)));
            } else {
                ReleaseSysCache(oldtuple);
                return CStringGetDatum(def_name);
            }
        }
    }
    if (SECUREC_LIKELY(*definer_oid == NULL)) {
        (*definer_oid) = get_role_name_str();
    }
    return curuser;
}

void PrepareFuncArg(CreateEventStmt *stmt, Datum ev_name, Datum schemaName, FunctionCallInfoData *ev_arg)
{
    /* Creating an Inline Program */
    char* job_definer_oid = NULL;
    Datum definer = SetDefinerName(stmt->def_name, ev_name, &job_definer_oid);
    Datum job_action = CStringGetTextDatum(stmt->event_query_str);
    char *job_type_string = PLSQL_JOB_TYPE;
    text *job_type_text = cstring_to_text_with_len(job_type_string, strlen(job_type_string));
    Datum job_type = CStringGetTextDatum(text_to_cstring(job_type_text));
    char *c_schedule_name = get_inline_schedule_name(ev_name);
    Datum schedule_name = CStringGetTextDatum(c_schedule_name);
    char *c_program_name =
        CreateEventInlineProgram(ev_name, job_type, job_action, CStringGetTextDatum(job_definer_oid));
    Datum program_name = CStringGetTextDatum(c_program_name);

    Datum start_time;
    Datum interval_time;
    Datum end_time;

    /* Parsing the time expression */
    GetTimeExecResult(stmt, start_time, interval_time, end_time);

    ev_arg->arg[ARG_0] = ev_name;
    ev_arg->arg[ARG_1] = program_name;
    ev_arg->arg[ARG_2] = schedule_name;
    ev_arg->arg[ARG_3] = CStringGetTextDatum("DEFAULT_JOB_CLASS");
    ev_arg->arg[ARG_4] = (stmt->event_status == EVENT_ENABLE) ? BoolGetDatum(1) : BoolGetDatum(0);
    ev_arg->arg[ARG_5] = BoolGetDatum(stmt->complete_preserve);
    ev_arg->arg[ARG_6] = (stmt->event_comment_str == NULL) ? (Datum)0 : CStringGetTextDatum(stmt->event_comment_str);
    ev_arg->arg[ARG_7] = CStringGetTextDatum("REGULAR");
    ev_arg->arg[ARG_8] = (Datum)0;
    ev_arg->arg[ARG_9] = (Datum)0;
    ev_arg->arg[ARG_10] = CharGetDatum(JOB_INTYPE_PLAIN);
    ev_arg->arg[ARG_11] = TimeStampToText(start_time);
    ev_arg->arg[ARG_12] = interval_time;
    ev_arg->arg[ARG_13] = TimeStampToText(end_time);
    ev_arg->arg[ARG_14] = job_action;
    ev_arg->arg[ARG_15] = job_type;
    ev_arg->arg[ARG_16] = definer;
    ev_arg->arg[ARG_17] = schemaName;
    ev_arg->arg[ARG_18] = CStringGetTextDatum(job_definer_oid);
}

void CheckEventPrivilege(char* schema_name, char* event_name, AclMode mode, bool is_create_or_alter)
{
    Oid user_oid = GetUserId();

    /* Check whether user have the permission on the specified schema. */
    Oid schema_oid = get_namespace_oid(schema_name, true);
    AclResult acl_result = pg_namespace_aclcheck(schema_oid, user_oid, mode);
    if(acl_result != ACLCHECK_OK) {
        aclcheck_error(acl_result, ACL_KIND_NAMESPACE, schema_name);
    }

    /* Superusers bypass all permission checking. */
    if(superuser_arg(user_oid) || systemDBA_arg(user_oid)) {
        return;
    }
    if(is_create_or_alter) {
        return;
    }

    /* The owner needs to be checked for the alter and drop operation. */
    HeapTuple tup = NULL;
    tup = SearchSysCache2(JOBATTRIBUTENAME, CStringGetTextDatum(event_name), CStringGetTextDatum("owner"));
    if(!HeapTupleIsValid(tup)) {
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT),
                        errmsg("event \"%s\" does not exist", event_name)));
    }

    bool isnull = false;
    Datum owner_datum = SysCacheGetAttr(JOBATTRIBUTENAME, tup, Anum_gs_job_attribute_attribute_value, &isnull);
    char* owner_id_str = TextDatumGetCString(owner_datum);
    Oid owner_id = pg_atoi(owner_id_str, sizeof(int32), '\0');

    if(owner_id != user_oid) {
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT),
                        errmsg("only event's owner have the permission to operate object \"%s\"", event_name)));
    }
    ReleaseSysCache(tup);
}

void CreateEventCommand(CreateEventStmt *stmt)
{
    char *event_name_str = stmt->event_name->relname;
    char *schema_name_str = (stmt->event_name->schemaname) ? stmt->event_name->schemaname : get_real_search_schema();
    CheckEventPrivilege(schema_name_str, event_name_str, ACL_CREATE, true);

    Datum schema_name = DirectFunctionCall1(namein, CStringGetDatum(schema_name_str));
    Datum ev_name = CStringGetTextDatum(event_name_str);
    FunctionCallInfoData ev_arg;
    const short nrgs_job = ARG_19;

    if (CheckEventExists(ev_name, stmt->if_not_exists)) {
        return;
    }

    InitFunctionCallInfoData(ev_arg, NULL, nrgs_job, InvalidOid, NULL, NULL);
    errno_t rc = memset_s(ev_arg.arg, nrgs_job * sizeof(Datum), 0, nrgs_job * sizeof(Datum));
    securec_check(rc, "\0", "\0");
    rc = memset_s(ev_arg.argnull, nrgs_job * sizeof(bool), 0, nrgs_job * sizeof(bool));
    securec_check(rc, "\0", "\0");

    /* Obtains the event parameter. */
    PrepareFuncArg(stmt, ev_name, schema_name, &ev_arg);

    create_job_raw(&ev_arg);
}

Datum GetInlineJobName(Datum ev_name)
{
    errno_t rc;
    char *c_program_name = (char *)palloc(sizeof(char) * MAX_JOB_NAME_LEN);
    rc = strcpy_s(c_program_name, MAX_JOB_NAME_LEN, INLINE_JOB_PROGRAM_PREFIX);
    securec_check(rc, "\0", "\0");
    rc = strcat_s(c_program_name, MAX_JOB_NAME_LEN, TextDatumGetCString(ev_name));
    securec_check(rc, "\0", "\0");
    return CStringGetTextDatum(c_program_name);
}

void UpdateMultiAttribute(Datum ev_name, Datum attValue, Datum new_name, Datum att_name)
{
    Datum values[Natts_gs_job_attribute] = {0};
    bool nulls[Natts_gs_job_attribute] = {0};
    bool replaces[Natts_gs_job_attribute] = {0};

    replaces[Anum_gs_job_attribute_attribute_value - 1] = true;
    values[Anum_gs_job_attribute_attribute_value - 1] = attValue;
    nulls[Anum_gs_job_attribute_attribute_value - 1] = false;
    replaces[Anum_gs_job_attribute_job_name - 1] = true;
    values[Anum_gs_job_attribute_job_name - 1] = new_name;
    nulls[Anum_gs_job_attribute_job_name - 1] = false;
    Relation gs_job_attribute_rel = heap_open(GsJobAttributeRelationId, RowExclusiveLock);
    HeapTuple oldtuple = SearchSysCache2(JOBATTRIBUTENAME, ev_name, att_name);
    if (oldtuple == NULL) {
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("Fail to update attribute."),
                        errdetail("Attribute entry %s not found.", TextDatumGetCString(att_name)),
                        errcause("attribute is not exist"), erraction("Please check object_name")));
    }
    HeapTuple newtuple = heap_modify_tuple(oldtuple, RelationGetDescr(gs_job_attribute_rel), values, nulls, replaces);
    simple_heap_update(gs_job_attribute_rel, &newtuple->t_self, newtuple);
    CatalogUpdateIndexes(gs_job_attribute_rel, newtuple);
    heap_close(gs_job_attribute_rel, NoLock);
    ReleaseSysCache(oldtuple);
    heap_freetuple_ext(newtuple);
}

/* Update the job_name and attribute_value columns in the gs_job_attribute table. */
void UpdateAttributeMultiColum(Datum ev_name, char *attribute_name[], int attr_pos, DefElem *para_def,
                               Datum new_name_result)
{
    attribute_name[attr_pos] = para_def->defname;
    Datum arg_result = TranslateArg(para_def->defname, para_def->arg);
    if (!strcmp(para_def->defname, "owner_default")) {
        UpdateMultiAttribute(ev_name, arg_result, new_name_result, CStringGetTextDatum("owner"));
    } else {
        UpdateMultiAttribute(ev_name, arg_result, new_name_result, CStringGetTextDatum(para_def->defname));
    }
}

/* Update the attribute_value columns in the gs_job_attribute table. */
void UpdateSingleAttribute(Datum ev_name, DefElem *para_def)
{
    Datum arg_result = TranslateArg(para_def->defname, para_def->arg);
    Datum att_name = strcmp(para_def->defname, "owner_default") ? CStringGetTextDatum(para_def->defname)
                                                                : CStringGetTextDatum("owner");
    update_attribute(ev_name, att_name, arg_result);
}

void UpdateMultiRows(AlterEventStmt *stmt, int attr_pos, char *attribute_name[], Datum ev_name)
{
    Datum values[Natts_gs_job_attribute] = {0};
    bool nulls[Natts_gs_job_attribute] = {0};
    bool replaces[Natts_gs_job_attribute] = {0};

    Datum arg_result = TranslateArg(stmt->new_name->defname, (Node *)stmt->new_name->arg);
    replaces[Anum_gs_job_attribute_job_name - 1] = true;
    values[Anum_gs_job_attribute_job_name - 1] = arg_result;
    nulls[Anum_gs_job_attribute_job_name - 1] = false;
    Relation gs_job_attribute_rel = heap_open(GsJobAttributeRelationId, RowExclusiveLock);
    CatCList *list = SearchSysCacheList1(JOBATTRIBUTENAME, ev_name);
    bool is_null = false;
    Datum enattr;
    char *enattname;
    bool is_repeat = false;
    for (int i = 0; i < list->n_members; i++) {
        HeapTuple enum_tup = t_thrd.lsc_cxt.FetchTupleFromCatCList(list, i);
        enattr = heap_getattr(enum_tup, Anum_gs_job_attribute_attribute_name, RelationGetDescr(gs_job_attribute_rel),
                              &is_null);
        enattname = TextDatumGetCString(enattr);
        if (strcmp(enattname, "program_name") == 0) {
            replaces[Anum_gs_job_attribute_attribute_value - 1] = true;
            values[Anum_gs_job_attribute_attribute_value - 1] = GetInlineJobName(arg_result);
            nulls[Anum_gs_job_attribute_attribute_value - 1] = false;
        }
        if (strcmp(enattname, "schedule_name") == 0) {
            replaces[Anum_gs_job_attribute_attribute_value - 1] = true;
            values[Anum_gs_job_attribute_attribute_value - 1] =
                CStringGetTextDatum(get_inline_schedule_name(arg_result));
            nulls[Anum_gs_job_attribute_attribute_value - 1] = false;
        }
        for (int j = 0; j < attr_pos; ++j) {
            if (!strcmp(attribute_name[j], "owner_default")) {
                attribute_name[j] = "owner";
            };
            if (strcmp(enattname, attribute_name[j]) == 0) {
                is_repeat = true;
                break;
            }
        }
        if (is_repeat) {
            is_repeat = false;
            continue;
        }
        HeapTuple old_tup = enum_tup;
        enum_tup = heap_copytuple(old_tup);

        HeapTuple newtuple =
            heap_modify_tuple(enum_tup, RelationGetDescr(gs_job_attribute_rel), values, nulls, replaces);
        if (strcmp(enattname, "program_name") == 0) {
            replaces[Anum_gs_job_attribute_attribute_value - 1] = false;
            values[Anum_gs_job_attribute_attribute_value - 1] = (Datum)0;
            nulls[Anum_gs_job_attribute_attribute_value - 1] = true;
        }
        if (strcmp(enattname, "schedule_name") == 0) {
            replaces[Anum_gs_job_attribute_attribute_value - 1] = false;
            values[Anum_gs_job_attribute_attribute_value - 1] = (Datum)0;
            nulls[Anum_gs_job_attribute_attribute_value - 1] = true;
        }
        simple_heap_update(gs_job_attribute_rel, &newtuple->t_self, newtuple);
        CatalogUpdateIndexes(gs_job_attribute_rel, newtuple);
    }
    ReleaseSysCacheList(list);

    list = SearchSysCacheList1(JOBATTRIBUTENAME, GetInlineJobName(ev_name));
    values[Anum_gs_job_attribute_job_name - 1] = GetInlineJobName(arg_result);

    for (int i = 0; i < list->n_members; i++) {
        HeapTuple enum_tup = t_thrd.lsc_cxt.FetchTupleFromCatCList(list, i);
        enattr = heap_getattr(enum_tup, Anum_gs_job_attribute_attribute_name, RelationGetDescr(gs_job_attribute_rel),
                              &is_null);
        enattname = TextDatumGetCString(enattr);
        for (int j = 0; j < attr_pos; ++j) {
            if (!strcmp(attribute_name[j], "owner_default")) {
                attribute_name[j] = "owner";
            };
            if (strcmp(enattname, attribute_name[j]) == 0 && strcmp(enattname, "comments") != 0) {
                is_repeat = true;
                break;
            }
        }
        if (is_repeat) {
            is_repeat = false;
            continue;
        }
        HeapTuple old_tup = enum_tup;
        enum_tup = heap_copytuple(old_tup);

        HeapTuple newtuple =
            heap_modify_tuple(enum_tup, RelationGetDescr(gs_job_attribute_rel), values, nulls, replaces);
        simple_heap_update(gs_job_attribute_rel, &newtuple->t_self, newtuple);
        CatalogUpdateIndexes(gs_job_attribute_rel, newtuple);
    }
    ReleaseSysCacheList(list);
    heap_close(gs_job_attribute_rel, RowExclusiveLock);
}

void UpdateAttributeParam(AlterEventStmt *stmt, Datum ev_name)
{
    bool need_rename = (stmt->new_name == NULL) ? false : true;
    Datum new_name_result = (Datum)0;
    if (need_rename) {
        new_name_result = TranslateArg(stmt->new_name->defname, stmt->new_name->arg);
    }

    const int attribute_num = ARG_17;
    int attr_pos = 0;
    char *attribute_name[attribute_num] = {0};
    Datum inline_name = GetInlineJobName(ev_name);
    if (stmt->event_comment_str) {
        if (need_rename) {
            UpdateAttributeMultiColum(ev_name, attribute_name, attr_pos++, stmt->event_comment_str, new_name_result);
        } else {
            UpdateSingleAttribute(ev_name, stmt->event_comment_str);
        }
    }
    if (stmt->complete_preserve) {
        if (need_rename) {
            UpdateAttributeMultiColum(ev_name, attribute_name, attr_pos++, stmt->complete_preserve, new_name_result);
        } else {
            UpdateSingleAttribute(ev_name, stmt->complete_preserve);
        }
    }
    if (stmt->def_name) {
        if (need_rename) {
            UpdateAttributeMultiColum(ev_name, attribute_name, attr_pos++, stmt->def_name, new_name_result);
            UpdateAttributeMultiColum(inline_name, attribute_name, attr_pos++, stmt->def_name,
                                      GetInlineJobName(new_name_result));
        } else {
            UpdateSingleAttribute(ev_name, stmt->def_name);
            UpdateSingleAttribute(inline_name, stmt->def_name);
        }
    } else {
        DefElem* defaute_definer_def = makeDefElem("owner_default", NULL);
        if (need_rename) {
            UpdateAttributeMultiColum(ev_name, attribute_name, attr_pos++, defaute_definer_def, new_name_result);
            UpdateAttributeMultiColum(inline_name, attribute_name, attr_pos++, defaute_definer_def,
                                      GetInlineJobName(new_name_result));
        } else {
            UpdateSingleAttribute(ev_name, defaute_definer_def);
            UpdateSingleAttribute(inline_name, defaute_definer_def);
        }
    }
    if (stmt->event_query_str) {
        if (need_rename) {
            UpdateAttributeMultiColum(inline_name, attribute_name, attr_pos++, stmt->event_query_str,
                                      GetInlineJobName(new_name_result));
        } else {
            UpdateSingleAttribute(inline_name, stmt->event_query_str);
        }
    }
    if (need_rename) {
        /* Update other columns that need to be renamed in the job table. */
        UpdateMultiRows(stmt, attr_pos, attribute_name, ev_name);
    }
}

void UpdateMultiProc(Datum attValue, Datum nameValue, Datum jobName)
{
    Datum values[Natts_pg_job_proc] = {0};
    bool nulls[Natts_pg_job_proc] = {0};
    bool replaces[Natts_pg_job_proc] = {0};

    Relation rel = heap_open(PgJobProcRelationId, RowExclusiveLock);

    replaces[Anum_pg_job_proc_what - 1] = true;
    values[Anum_pg_job_proc_what - 1] = attValue;
    nulls[Anum_pg_job_proc_what - 1] = false;
    replaces[Anum_pg_job_proc_job_name - 1] = true;
    values[Anum_pg_job_proc_job_name - 1] = nameValue;
    nulls[Anum_pg_job_proc_job_name - 1] = false;

    HeapTuple oldtuple = search_from_pg_job_proc(rel, jobName);

    HeapTuple newtuple = heap_modify_tuple(oldtuple, RelationGetDescr(rel), values, nulls, replaces);
    simple_heap_update(rel, &newtuple->t_self, newtuple);
    CatalogUpdateIndexes(rel, newtuple);

    heap_close(rel, RowExclusiveLock);
    heap_freetuple_ext(oldtuple);
    heap_freetuple_ext(newtuple);
}

void UpdatePgJobProcName(Datum job_name, DefElem *new_name)
{
    Datum values[Natts_pg_job_proc] = {0};
    bool nulls[Natts_pg_job_proc] = {0};
    bool replaces[Natts_pg_job_proc] = {0};

    Relation rel = heap_open(PgJobProcRelationId, RowExclusiveLock);
    Datum arg_result = TranslateArg(new_name->defname, new_name->arg);
    replaces[Anum_pg_job_proc_job_name - 1] = true;
    values[Anum_pg_job_proc_job_name - 1] = arg_result;
    nulls[Anum_pg_job_proc_job_name - 1] = false;

    HeapTuple oldtuple = search_from_pg_job_proc(rel, job_name);

    HeapTuple newtuple = heap_modify_tuple(oldtuple, RelationGetDescr(rel), values, nulls, replaces);
    simple_heap_update(rel, &newtuple->t_self, newtuple);
    CatalogUpdateIndexes(rel, newtuple);

    heap_close(rel, RowExclusiveLock);
    heap_freetuple_ext(oldtuple);
    heap_freetuple_ext(newtuple);
}

void UpdatePgJobProcParam(AlterEventStmt *stmt, Datum ev_name)
{
    if (stmt->event_query_str) {
        Datum arg_result = TranslateArg(stmt->event_query_str->defname, stmt->event_query_str->arg);
        if (stmt->new_name) {
            Datum ev_new_name = TranslateArg(stmt->new_name->defname, stmt->new_name->arg);
            UpdateMultiProc(arg_result, ev_new_name, ev_name);
        } else {
            dbe_update_pg_job_proc(arg_result, ev_name);
        }
    } else {
        if (stmt->new_name) {
            UpdatePgJobProcName(ev_name, stmt->new_name);
        }
    }
}

void UpdateMultiJob(Datum job_name, Datum *values, bool *nulls, bool *replaces)
{
    Relation pg_job_rel = heap_open(PgJobRelationId, RowExclusiveLock);
    HeapTuple oldtuple = search_from_pg_job(pg_job_rel, job_name);
    if (!HeapTupleIsValid(oldtuple)) {
        heap_close(pg_job_rel, NoLock);
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_DATABASE),
                        errmsg("event \"%s\" does not exist", TextDatumGetCString(job_name))));
    }
    HeapTuple newtuple = heap_modify_tuple(oldtuple, RelationGetDescr(pg_job_rel), values, nulls, replaces);
    simple_heap_update(pg_job_rel, &newtuple->t_self, newtuple);
    CatalogUpdateIndexes(pg_job_rel, newtuple);
    heap_close(pg_job_rel, NoLock);
    heap_freetuple_ext(newtuple);
    heap_freetuple_ext(oldtuple);
}

void UpdatePgJobParam(AlterEventStmt *stmt, Datum ev_name)
{
    Datum arg_result;
    Datum values[Natts_pg_job] = {0};
    bool nulls[Natts_pg_job] = {0};
    bool replaces[Natts_pg_job] = {0};

    Datum definer = DirectFunctionCall1(namein, CStringGetDatum(GetUserNameFromId(GetUserId())));
    if (stmt->def_name) {
        Value *definerVal = (Value *)stmt->def_name->arg;
        definer = DirectFunctionCall1(namein, CStringGetDatum(definerVal->val.str));
    }
    values[Anum_pg_job_log_user - 1] = definer;
    nulls[Anum_pg_job_log_user - 1] = false;
    replaces[Anum_pg_job_log_user - 1] = true;
    values[Anum_pg_job_priv_user - 1] = definer;
    nulls[Anum_pg_job_priv_user - 1] = false;
    replaces[Anum_pg_job_priv_user - 1] = true;
    if (stmt->end_time_expr) {
        arg_result = TranslateArg(stmt->end_time_expr->defname, stmt->end_time_expr->arg);
        values[Anum_pg_job_end_date - 1] = arg_result;
        nulls[Anum_pg_job_end_date - 1] = false;
        replaces[Anum_pg_job_end_date - 1] = true;
    }
    if (stmt->start_time_expr) {
        arg_result = TranslateArg(stmt->start_time_expr->defname, stmt->start_time_expr->arg);
        values[Anum_pg_job_start_date - 1] = arg_result;
        nulls[Anum_pg_job_start_date - 1] = false;
        replaces[Anum_pg_job_start_date - 1] = true;
        values[Anum_pg_job_next_run_date - 1] = arg_result;
        nulls[Anum_pg_job_next_run_date - 1] = false;
        replaces[Anum_pg_job_next_run_date - 1] = true;
    }
    if (stmt->interval_time) {
        arg_result = (stmt->interval_time->arg == NULL)
                         ? CStringGetTextDatum("null")
                         : TranslateArg(stmt->interval_time->defname, stmt->interval_time->arg);
        values[Anum_pg_job_interval - 1] = arg_result;
        nulls[Anum_pg_job_interval - 1] = false;
        replaces[Anum_pg_job_interval - 1] = true;
    }
    if (stmt->event_status) {
        arg_result = TranslateArg(stmt->event_status->defname, stmt->event_status->arg);
        values[Anum_pg_job_enable - 1] = arg_result;
        nulls[Anum_pg_job_enable - 1] = false;
        replaces[Anum_pg_job_enable - 1] = true;
    }
    if (stmt->new_name) {
        arg_result = TranslateArg(stmt->new_name->defname, stmt->new_name->arg);
        values[Anum_pg_job_job_name - 1] = arg_result;
        nulls[Anum_pg_job_job_name - 1] = false;
        replaces[Anum_pg_job_job_name - 1] = true;
    }
    UpdateMultiJob(ev_name, values, nulls, replaces);
}

void AlterEventCommand(AlterEventStmt *stmt)
{
    char *event_name_str = stmt->event_name->relname;
    char *schema_name_str = (stmt->event_name->schemaname) ? stmt->event_name->schemaname : get_real_search_schema();

    Datum ev_name = CStringGetTextDatum(event_name_str);
    CheckEventPrivilege(schema_name_str, event_name_str, ACL_USAGE, false);

    /* Check if object is visible for current user. */
    check_object_is_visible(ev_name, false);
    if (stmt->def_name) {
        Value *ev_definer_node = (Value *)stmt->def_name->arg;
        CheckDefinerPriviledge(ev_definer_node->val.str);
        if (!superuser()) {
            ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                            errmsg("The current user does not have sufficient permissions to specify the definer.")));
        }
    }

    UpdateAttributeParam(stmt, ev_name);
    UpdatePgJobProcParam(stmt, ev_name);
    UpdatePgJobParam(stmt, ev_name);
}

void DropEventCommand(DropEventStmt *stmt)
{
    char *event_name_str = stmt->event_name->relname;
    char *schema_name_str = (stmt->event_name->schemaname) ? stmt->event_name->schemaname : get_real_search_schema();

    Datum ev_name = CStringGetTextDatum(event_name_str);
    if (CheckEventNotExists(ev_name, stmt->missing_ok)) {
        return;
    }
    CheckEventPrivilege(schema_name_str, event_name_str, ACL_USAGE, false);

    FunctionCallInfoData ev_arg;
    const short nrgs_job = ARG_3;
    InitFunctionCallInfoData(ev_arg, NULL, nrgs_job, InvalidOid, NULL, NULL);
    errno_t rc = memset_s(ev_arg.arg, nrgs_job * sizeof(Datum), 0, nrgs_job * sizeof(Datum));
    securec_check(rc, "\0", "\0");
    rc = memset_s(ev_arg.argnull, nrgs_job * sizeof(bool), 0, nrgs_job * sizeof(bool));
    securec_check(rc, "\0", "\0");
    ev_arg.arg[ARG_0] = ev_name;
    ev_arg.arg[ARG_1] = BoolGetDatum(0);
    ev_arg.arg[ARG_2] = BoolGetDatum(0);
    drop_single_job_internal(&ev_arg);
}

StmtResult *SearchEventInfo(ShowEventStmt *stmt)
{
    StringInfoData buf;
    initStringInfo(&buf);
    appendStringInfo(&buf, "SELECT ");
    appendStringInfo(&buf,
                     "job_name,nspname,log_user,priv_user,job_status,start_date,interval,end_date,enable,failure_msg ");
    appendStringInfo(&buf, "FROM PG_JOB ");

    /* Concatenate where clause */
    appendStringInfo(&buf, "WHERE dbname=\'%s\' AND ", get_database_name(u_sess->proc_cxt.MyDatabaseId));
    if (stmt->from_clause) {
        A_Const *fc = (A_Const *)stmt->from_clause;
        appendStringInfo(&buf, "nspname=\'%s\' ", (char *)fc->val.val.str);
    } else {
        char *schema_name = get_real_search_schema();
        appendStringInfo(&buf, "nspname=\'%s\' ", schema_name);
    }
    if (stmt->where_clause) {
        appendStringInfo(&buf, " AND ");
        appendStringInfo(&buf, " %s ", stmt->where_clause);
    }

    return execute_stmt(buf.data, true);
}

void ShowEventCommand(ShowEventStmt *stmt, DestReceiver *dest)
{
    TupOutputState *tstate = NULL;
    TupleDesc tupdesc;
    Datum values[SHOW_EVENT_SIZE] = {0};
    bool isnull[SHOW_EVENT_SIZE] = {false};

    /* need a tuple descriptor representing three TEXT columns */
    tupdesc = CreateTemplateTupleDesc(SHOW_EVENT_SIZE, false, TableAmHeap);
    TupleDescInitEntry(tupdesc, (AttrNumber)ARG_1, "job_name", TEXTOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)ARG_2, "schema_name", TEXTOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)ARG_3, "log_user", TEXTOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)ARG_4, "priv_user", TEXTOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)ARG_5, "job_status", TEXTOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)ARG_6, "start_date", TEXTOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)ARG_7, "interval", TEXTOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)ARG_8, "end_date", TEXTOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)ARG_9, "enable", TEXTOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)ARG_10, "failure_msg", TEXTOID, -1, 0);

    /* prepare for projection of tuples */
    tstate = begin_tup_output_tupdesc(dest, tupdesc);

    /* Create a buffer to store the select statement. */
    StmtResult *res = SearchEventInfo(stmt);

    List *eventList = res->tuples;
    ListCell *eventCell = NULL;

    foreach (eventCell, eventList) {
        List *eventTuple = (List *)eventCell->data.ptr_value;
        ListCell *eventDesc = NULL;
        char *ival;
        int eventPos = 0;
        foreach (eventDesc, eventTuple) {
            ival = (char *)eventDesc->data.ptr_value;
            values[eventPos++] = PointerGetDatum(cstring_to_text(ival));
        }
        do_tup_output(tstate, values, SHOW_EVENT_SIZE, isnull, SHOW_EVENT_SIZE);
        for (int i = 0; i < SHOW_EVENT_SIZE; ++i) {
            pfree(DatumGetPointer(values[i]));
        }
    }
    end_tup_output(tstate);
}

TupleDesc GetEventResultDesc()
{
    TupleDesc tupdesc;
    /* need a tuple descriptor representing ten TEXT columns */
    tupdesc = CreateTemplateTupleDesc(SHOW_EVENT_SIZE, false, TableAmHeap);
    TupleDescInitEntry(tupdesc, (AttrNumber)ARG_1, "job_name", TEXTOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)ARG_2, "schema_name", TEXTOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)ARG_3, "log_user", TEXTOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)ARG_4, "priv_user", TEXTOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)ARG_5, "job_status", TEXTOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)ARG_6, "start_date", TEXTOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)ARG_7, "interval", TEXTOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)ARG_8, "end_date", TEXTOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)ARG_9, "enable", TEXTOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)ARG_10, "failure_msg", TEXTOID, -1, 0);

    return tupdesc;
}
