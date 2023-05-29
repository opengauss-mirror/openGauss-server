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
 * -------------------------------------------------------------------------
 *
 * gs_job_attribute.cpp
 *
 * IDENTIFICATION
 *    src/common/backend/catalog/catalog/gs_job_attribute.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "postgres.h"
#include "knl/knl_variable.h"
#include <limits.h>
#include "access/sysattr.h"
#include "access/xact.h"
#include "catalog/indexing.h"
#include "catalog/namespace.h"
#include "catalog/objectaccess.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_type.h"
#include "commands/alter.h"
#include "commands/comment.h"
#include "commands/dbcommands.h"
#include "commands/extension.h"
#include "commands/schemacmds.h"
#include "commands/user.h"
#include "executor/spi.h"
#include "funcapi.h"
#include "mb/pg_wchar.h"
#include "miscadmin.h"
#include "pgxc/pgxc.h"
#include "tcop/utility.h"
#include "utils/builtins.h"
#include "utils/dbe_scheduler.h"
#include "utils/fmgroids.h"
#include "utils/formatting.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/rel_gs.h"
#include "utils/snapmgr.h"
#include "access/heapam.h"
#include "access/tableam.h"
#include "catalog/pg_job.h"
#include "catalog/pg_job_proc.h"
#include "catalog/pg_authid.h"
#include "catalog/pg_database.h"
#include "catalog/gs_job_argument.h"
#include "catalog/gs_job_attribute.h"
#include "fmgr.h"
#include "utils/syscache.h"
#include "utils/timestamp.h"
#include "pgxc/execRemote.h"

/*
 * @brief get_scheduler_max_timestamp
 *  Get max timestamp 4000-1-1.
 */
Datum get_scheduler_max_timestamp()
{
    return DirectFunctionCall2(to_timestamp,
                               DirectFunctionCall1(textin, CStringGetDatum("4000-1-1")),
                               DirectFunctionCall1(textin, CStringGetDatum("yyyy-mm-dd")));
}

/*
 * @brief get_scheduler_max_timestamptz
 *  Get max timestamp eith time zone 4000-1-1.
 */
Datum get_scheduler_max_timestamptz()
{
    return DirectFunctionCall1(timestamp_timestamptz, get_scheduler_max_timestamp());
}

/*
 * @brief get_role_name_str
 *  Get the string version of role id.
 */
char *get_role_name_str(Oid oid)
{
    oid = (oid != InvalidOid) ? oid : GetUserId();
    char *name = (char *)palloc(MAX_JOB_NAME_LEN * sizeof(char));
    pg_ltoa((int)oid, name);
    return name;
}

/*
 * @brief get_role_datum
 *  Get text datum version of role id.
 */
Datum get_role_datum(Datum name)
{
    char *role_id_str = NULL;
    if (name == (Datum)0) {
        role_id_str = get_role_name_str();
    } else {
        role_id_str = get_role_name_str(get_role_oid(TextDatumGetCString(name), false));
    }
    return CStringGetTextDatum(role_id_str);
}

/*
 * @brief get_raw_role_datum
 *  From text datum role id to role name string.
 */
char *get_raw_role_str(Datum name)
{
    char *name_str = TextDatumGetCString(name);
    return GetUserNameFromId((Oid)atoi(name_str));
}

/*
 * @brief get_default_argument_type
 *  Wrapper for default argument type.
 */
Datum get_default_argument_type()
{
    return DirectFunctionCall1(namein, CStringGetDatum("text"));
}

/*
 * @brief get_pg_job_node_name
 *  Get the pg job node name, can be an empty string.
 * @return Datum node_name datum
 */
static Datum get_pg_job_node_name()
{
    if (!IsConnFromCoord()) {
        return DirectFunctionCall1(namein, CStringGetDatum(g_instance.attr.attr_common.PGXCNodeName));
    }
    return DirectFunctionCall1(namein, CStringGetDatum(""));
}

/*
 * @brief is_inlined_program
 *  Check if name start with INLINE_JOB_PROGRAM_PREFIX.
 */
bool is_inlined_program(Datum program_name)
{
    char *str = TextDatumGetCString(program_name);
    if (strncmp(str, INLINE_JOB_PROGRAM_PREFIX, strlen(INLINE_JOB_PROGRAM_PREFIX)) == 0) {
        return true;
    }
    return false;
}

/*
 * @brief is_inlined_schedule
 *  Check if name start with INLINE_JOB_SCHEDULE_PREFIX.
 */
bool is_inlined_schedule(Datum schedule_name)
{
    char *str = TextDatumGetCString(schedule_name);
    if (strncmp(str, INLINE_JOB_SCHEDULE_PREFIX, strlen(INLINE_JOB_SCHEDULE_PREFIX)) == 0) {
        return true;
    }
    return false;
}

/*
 * @brief IsLegalNameStr
 *  Check if a given name string is legal.
 * @param nameStr   name string
 * @param hasAlpha  alphabet check
 * @return true     legal
 * @return false    too long/contains invalid character
 */
static bool IsLegalNameStr(const char* nameStr, bool *hasAlpha)
{
    size_t NBytes = (unsigned int)strlen(nameStr);
    if (NBytes > (MAX_JOB_NAME_LEN - sizeof(INLINE_JOB_SCHEDULE_PREFIX))) { /* 1024 - 17 */
        return false;
    }

    for (size_t i = 0; i < NBytes; i++) {
        /* check whether the character is correct */
        if (IsIllegalCharacter(nameStr[i])) {
            return false;
        }

        /* NOTE: make sure if hasAlpha is true, the alphabet check is disabled */
        if (!(*hasAlpha) && isalpha(nameStr[i])) {
            *hasAlpha = true;
        }
    }
    return true;
}

/*
 * @brief is_job_running
 *  Is job at running state?
 * @param job_name
 * @return true
 * @return false
 */
bool is_job_running(Datum job_name)
{
    bool is_running = false;
    Relation pg_job_rel = heap_open(PgJobRelationId, AccessShareLock);
    HeapTuple tuple = search_from_pg_job(pg_job_rel, job_name);
    if (!HeapTupleIsValid(tuple)) {
        heap_close(pg_job_rel, AccessShareLock);
        return false;
    }
    Form_pg_job pg_job_value = (Form_pg_job)GETSTRUCT(tuple);
    if (pg_job_value->job_status == PGJOB_RUN_STATUS) {
        is_running = true;
    }
    heap_close(pg_job_rel, AccessShareLock);
    heap_freetuple_ext(tuple);
    return is_running;
}

/*
 * @brief can_disable_job
 *  Check if a job is running. This is a wrapper.
 * @param job_name
 */
static void can_disable_job(Datum job_name)
{
    if (is_job_running(job_name)) {
        ereport(ERROR, (errmodule(MOD_JOB), errcode(ERRCODE_INVALID_STATUS),
                errmsg("disable job_name %s running not allowed.", TextDatumGetCString(job_name)),
                errdetail("N/A"), errcause("disable job running not allowed"),
                erraction("Please check job_name running status")));
    }
}

/*
 * @brief get_attribute_value
 *  Get single attribute Datum.
 */
Datum get_attribute_value(Datum object, const char *attribute, LOCKMODE lockmode, bool miss_ok, bool can_be_null)
{
    bool isnull = false;
    Relation rel = heap_open(GsJobAttributeRelationId, lockmode);
    Datum attribute_name = CStringGetTextDatum(attribute);
    Datum attribute_value = lookup_job_attribute(rel, object, attribute_name, &isnull, miss_ok);
    heap_close(rel, NoLock);
    if (!PointerIsValid(attribute_value) && !can_be_null) {
        ereport(ERROR, (errmodule(MOD_JOB), errcode(ERRCODE_UNDEFINED_OBJECT),
                        errmsg("Attribute %s of object %s can not be empty.", attribute, TextDatumGetCString(object)),
                        errdetail("Corrupted attribute value."), errcause("N/A"),
                        erraction("Please check object name")));
    }
    return attribute_value;
}

/*
 * @brief get_attribute_value_str
 *  Get single attribute value string, wrapper.
 */
char *get_attribute_value_str(Datum object, const char *attribute, LOCKMODE lockmode, bool miss_ok, bool can_be_null)
{
    Datum val = get_attribute_value(object, attribute, lockmode, miss_ok, can_be_null);
    if (val == Datum(0)) {
        return NULL;
    }
    return TextDatumGetCString(val);
}

/*
 * @brief check_object_is_visible
 *  Check if object is visible for current user.
 * @param object_name
 * @param is_readonly   means that if we need to double check EXECUTE_ANY_PROGRAM_PRIVILEGE
 */
void check_object_is_visible(Datum object_name, bool is_readonly)
{
    if(superuser()) {
        return;
    }
    char *cur_user = get_role_name_str();
    char *username = get_attribute_value_str(object_name, "owner", AccessShareLock, false, false);
    if(pg_strcasecmp(cur_user, username) == 0) {
        pfree_ext(username);
        pfree_ext(cur_user);
        return;
    }
    pfree_ext(username);
    char *object_type = get_attribute_value_str(object_name, "object_type", AccessShareLock, false, false);
    if (is_readonly && pg_strcasecmp(object_type, "program") == 0) {
        check_privilege(cur_user, EXECUTE_ANY_PROGRAM_PRIVILEGE);
    } else {
        ereport(ERROR, (errmodule(MOD_JOB), errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                        errmsg("Object %s is not valid for current user.", TextDatumGetCString(object_name)),
                        errdetail("Insufficient privilege given."), errcause("N/A"),
                        erraction("Please contact database administrator.")));
    }
    pfree_ext(cur_user);
    pfree_ext(object_type);
}

/*
 * @brief check_object_type_matched
 *  Check if the name has expected object type.
 * @param object_name
 * @param object_type
 */
void check_object_type_matched(Datum object_name, const char *object_type)
{
    char *attribute_value_str = get_attribute_value_str(object_name, "object_type", RowExclusiveLock, false, false);
    if (pg_strcasecmp(object_type, attribute_value_str) != 0) {
        ereport(ERROR, (errmodule(MOD_JOB), errcode(ERRCODE_DATATYPE_MISMATCH),
                        errmsg("Object %s has type %s.", TextDatumGetCString(object_name), attribute_value_str),
                        errdetail("Object type does not match."), errcause("N/A"),
                        erraction("Please check %s name", object_type)));
    }
    pfree(attribute_value_str);
}

/*
 * @brief check_job_class_valid
 *  Check if job class is valid.
 * @param job_class
 */
void check_job_class_valid(Datum job_class)
{
    if (strcasecmp(TextDatumGetCString(job_class), "DEFAULT_JOB_CLASS") != 0) {
        check_object_type_matched(job_class, "job_class");
    }
}

/*
 * @brief get_job_type
 *  Get the job type string.
 * @param job_name
 * @param miss_ok
 * @return char*
 */
char *get_job_type(Datum job_name, bool miss_ok)
{
    Datum program_name = get_attribute_value(job_name, "program_name", AccessShareLock, miss_ok, miss_ok);
    if (program_name == Datum(0)) {
        Assert(miss_ok);
        return NULL;
    }
    return get_attribute_value_str(program_name, "program_type", AccessShareLock, miss_ok, miss_ok);
}

/*
 * @brief get_program_number_of_arguments
 *  Get the program number of arguments attribute.
 * @param program_name
 * @param miss_ok
 * @return int
 */
int get_program_number_of_arguments(Datum program_name, bool miss_ok)
{
    Datum val = get_attribute_value(program_name, "number_of_arguments", AccessShareLock, miss_ok, true);
    if (val == Datum(0)) {
        return 0;
    }
    return TextToInt32(val);
}

/*
 * @brief insert_gs_job_attribute
 *  Insert into gs_job_attribute.
 * @param gs_job_attribute_rel
 * @param attribute_name
 * @param attribute_value
 * @param values
 * @param nulls
 */
static void insert_gs_job_attribute(const Relation gs_job_attribute_rel, const Datum attribute_name,
                                    const Datum attribute_value, Datum *values, bool *nulls)
{
    values[Anum_gs_job_attribute_attribute_name - 1] = attribute_name;
    if (!PointerIsValid(attribute_value)) {
        Assert(pg_strcasecmp(TextDatumGetCString(attribute_name), "object_type") != 0);
        nulls[Anum_gs_job_attribute_attribute_value - 1] = true;
    } else {
        values[Anum_gs_job_attribute_attribute_value - 1] = attribute_value;
        nulls[Anum_gs_job_attribute_attribute_value - 1] = false;
    }
    HeapTuple tuple = heap_form_tuple(RelationGetDescr(gs_job_attribute_rel), values, nulls);
    (void)simple_heap_insert(gs_job_attribute_rel, tuple);
    CatalogUpdateIndexes(gs_job_attribute_rel, tuple);
    heap_freetuple_ext(tuple);
}

/*
 * @brief insert_attribute
 *  Insert one attribute.
 * @param object_name
 * @param attribute_name
 * @param attribute_value
 */
void insert_attribute(const Datum object_name, const Datum attribute_name, const Datum attribute_value)
{
    Datum values[Natts_gs_job_attribute];
    bool nulls[Natts_gs_job_attribute];
    errno_t rc = memset_s(nulls, sizeof(nulls), 0, sizeof(nulls));
    securec_check(rc, "\0", "\0");
    values[Anum_gs_job_attribute_job_name - 1] = PointerGetDatum(object_name);
    Relation gs_job_attribute_rel = heap_open(GsJobAttributeRelationId, RowExclusiveLock);
    insert_gs_job_attribute(gs_job_attribute_rel, attribute_name, attribute_value, values, nulls);
    heap_close(gs_job_attribute_rel, NoLock);
}

/*
 * @brief multi_insert_attribute
 *  Batch insert attributes.
 * @param object_name
 * @param attribute_name
 * @param attribute_value
 * @param n
 */
void multi_insert_attribute(const Datum object_name, const Datum *attribute_name, const Datum *attribute_value, int n)
{
    Datum values[Natts_gs_job_attribute];
    bool nulls[Natts_gs_job_attribute];
    errno_t rc = memset_s(nulls, sizeof(nulls), 0, sizeof(nulls));
    securec_check(rc, "\0", "\0");
    values[Anum_gs_job_attribute_job_name - 1] = object_name;
    Relation gs_job_attribute_rel = heap_open(GsJobAttributeRelationId, RowExclusiveLock);
    for (int i = 0; i < n; i++) {
        insert_gs_job_attribute(gs_job_attribute_rel, attribute_name[i], attribute_value[i], values, nulls);
    }
    heap_close(gs_job_attribute_rel, NoLock);
}

/*
 * @brief update_attribute
 *  Given a attribute name and value, perform an update.
 * @param object_name
 * @param attribute_name
 * @param attribute_value
 */
void update_attribute(Datum object_name, Datum attribute_name, Datum attribute_value)
{
    Datum values[Natts_gs_job_attribute];
    bool nulls[Natts_gs_job_attribute];
    bool replaces[Natts_gs_job_attribute];
    errno_t rc = memset_s(replaces, sizeof(replaces), 0, sizeof(replaces));
    securec_check(rc, "\0", "\0");
    replaces[Anum_gs_job_attribute_attribute_value - 1] = true;
    values[Anum_gs_job_attribute_attribute_value - 1] = attribute_value;
    nulls[Anum_gs_job_attribute_attribute_value - 1] = false;

    Relation gs_job_attribute_rel = heap_open(GsJobAttributeRelationId, RowExclusiveLock);
    HeapTuple oldtuple = SearchSysCache2(JOBATTRIBUTENAME, object_name, attribute_name);
    if (oldtuple == NULL) {
        ereport(ERROR, (errmodule(MOD_JOB), errcode(ERRCODE_UNDEFINED_OBJECT),
                        errmsg("Fail to update attribute."),
                        errdetail("Attribute entry %s not found.", TextDatumGetCString(attribute_name)),
                        errcause("attribute is not exist"), erraction("Please check object_name")));
    }
    HeapTuple newtuple = heap_modify_tuple(oldtuple, RelationGetDescr(gs_job_attribute_rel), values, nulls, replaces);
    simple_heap_update(gs_job_attribute_rel, &newtuple->t_self, newtuple);
    CatalogUpdateIndexes(gs_job_attribute_rel, newtuple);
    heap_close(gs_job_attribute_rel, NoLock);
    ReleaseSysCache(oldtuple);
    heap_freetuple_ext(newtuple);
}

/*
 * @brief check_privilege
 *  Lookup if user has authorization, report error immediately if doesn't.
 * @param username
 * @param authorization
 */
void check_privilege(char *username, const char *authorization)
{
    if (superuser()) {
        return;
    }
    Datum attribute_name = CStringGetTextDatum(authorization);
    check_authorization_valid(attribute_name);
    bool isnull = false;
    Relation rel = heap_open(GsJobAttributeRelationId, AccessShareLock);
    Datum username_text = CStringGetTextDatum(username);
    Datum attribute_value = lookup_job_attribute(rel, username_text, attribute_name, &isnull, true);
    if (attribute_value == Datum(0)) {
        ereport(ERROR, (errmodule(MOD_JOB), errcode(ERRCODE_UNDEFINED_OBJECT),
                        errmsg("User needs '%s' privilege to perform this operation.", authorization),
                        errdetail("Not enough privileges."), errcause("N/A"),
                        erraction("Please ensure the user have enough privilege.")));
    }
    heap_close(rel, NoLock);
    pfree(DatumGetPointer(attribute_value));
    pfree(DatumGetPointer(attribute_name));
    pfree(DatumGetPointer(username_text));
}

/*
 * @brief check_authorization_valid
 *  Check if authorization is valid.
 * @param privilege
 */
void check_authorization_valid(Datum privilege)
{
    char *privilege_str = TextDatumGetCString(privilege);
    const char *privilege_arr[] = {EXECUTE_ANY_PROGRAM_PRIVILEGE, CREATE_JOB_PRIVILEGE,
                                   CREATE_EXTERNAL_JOB_PRIVILEGE, RUN_EXTERNAL_JOB_PRIVILEGE};
    int len = lengthof(privilege_arr);
    for (int i = 0; i < len; ++i) {
        if (pg_strcasecmp(privilege_str, privilege_arr[i]) == 0) {
            pfree(privilege_str);
            return;
        }
    }
    ereport(ERROR, (errmodule(MOD_JOB), errcode(ERRCODE_INVALID_NAME),
                    errmsg("Invalid privilege %s", privilege_str),
                    errdetail("privilege contains invalid character"), errcause("privilege is invalid"),
                    erraction("Please enter a valid privilege")));
}

/*
 * @brief grant_user_authorization_internal
 *  Grant authorization to user.
 */
void grant_user_authorization_internal(PG_FUNCTION_ARGS)
{
    if (!superuser()) {
        ereport(ERROR, (errmodule(MOD_JOB), errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                        errmsg("Fail to grant authorization."),
                        errdetail("Insufficient privilege to grant authorization."), errcause("N/A"),
                        erraction("Please login in with initial user or contact database administrator.")));
    }
    const Datum username = get_role_datum(PG_GETARG_DATUM(0));
    const Datum privilege = PG_GETARG_DATUM(1);
    check_authorization_valid(privilege);

    Datum attribute_name[] = {privilege};
    int count = lengthof(attribute_name);
    Datum attribute_value[] = {CStringGetTextDatum("granted")};
    multi_insert_attribute(username, attribute_name, attribute_value, count);
    for (int i = 0; i < count; i++) {
        pfree(DatumGetPointer(attribute_name[i]));
    }
    pfree(DatumGetPointer(attribute_value[0]));
}

static char* get_current_username()
{
#ifndef WIN32
    struct passwd* pw = NULL;
    char* pRet = NULL;

    (void)syscalllockAcquire(&getpwuid_lock);
    pw = getpwuid(geteuid());
    if (pw == NULL) {
        (void)syscalllockRelease(&getpwuid_lock);
        return NULL;
    }
    /* Allocate new memory because later getpwuid() calls can overwrite it. */
    pRet = pstrdup(pw->pw_name);
    (void)syscalllockRelease(&getpwuid_lock);
    return pRet;
#else
    return NULL;
#endif
}

/*
 * @brief check_credential_name_valid
 *  Check if a user input string is a valid username.
 * @param file_name
 */
static void check_credential_name_valid(const Datum username)
{
    char *name_str = TextDatumGetCString(username);
    const char *danger_character_list[] = {"|", ";", "&", "$", "<", ">", "`", "\\", "'", "\"", "{",
                                           "}", "(", ")", "[", "]", "~", "*", "?",  "!", "\n", NULL};
    for (int i = 0; danger_character_list[i] != NULL; i++) {
        if (strstr(name_str, danger_character_list[i]) != NULL) {
            ereport(ERROR, (errmodule(MOD_JOB), errcode(ERRCODE_INVALID_NAME),
                            errmsg("Credential username is invalid."),
                            errdetail("Credential username contains invalid character"), errcause("str is invalid"),
                            erraction("Please enter a valid str")));
        }
    }
    char *inital_user = get_current_username();
    Assert(inital_user != NULL);
    if (inital_user == NULL) {
        ereport(ERROR, (errmodule(MOD_JOB), errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("inital_user username is invalid."),
                errdetail("inital_user username is invalid."),
                    errcause("inital_user is invalid"),
                erraction("EXTERNAL_SCRIPT is not supported")));
    }
    if (strcmp(name_str, inital_user) == 0) {
        ereport(ERROR, (errmodule(MOD_JOB), errcode(ERRCODE_INVALID_NAME),
                errmsg("Credential username is invalid."),
                errdetail("Credential username is initialuser, which can not be credential user"),
                    errcause("str is invalid"),
                erraction("Please enter a valid str")));
    }
    pfree(inital_user);
    pfree(name_str);
}

/*
 * @brief create_credential_internal
 *  Create a credential.
 */
void create_credential_internal(PG_FUNCTION_ARGS)
{
    if (!superuser()) {
        ereport(ERROR, (errmodule(MOD_JOB), errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                        errmsg("Fail to create credential."),
                        errdetail("Insufficient privilege to create credential."), errcause("N/A"),
                        erraction("Please login in with initial user or contact database administrator.")));
    }
    const Datum credential_name = PG_GETARG_DATUM(0);
    const Datum username = PG_GETARG_DATUM(1);

    if (!PG_ARGISNULL(2)) {
        char *passwdstr = TextDatumGetCString(PG_GETARG_DATUM(2));
        bool is_valid_passwd = !isStrHasInvalidCharacter(passwdstr);
        str_reset(passwdstr);
        pfree_ext(passwdstr);
        if (!is_valid_passwd) {
            ereport(ERROR, (errcode(ERRCODE_INVALID_PASSWORD),
                errmsg("Password cannot contain characters except numbers, alphabetic characters and "
                       "specified special characters."),
                errcause("Password contain invalid characters."),
                erraction("Use valid characters in password.")));
        }
    }
    const Datum database_role = (Datum)0;
    const Datum windows_domain = (Datum)0;
    if (!PG_ARGISNULL(3) || !PG_ARGISNULL(4)) {
        ereport(ERROR, (errmodule(MOD_JOB), errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("Scheduler does not support remote database job."),
                        errdetail("N/A"), errcause("N/A"),
                        erraction("Please leave database_role, windows_domain blank.")));
    }
    const Datum comments = PG_ARGISNULL(5) ? (Datum)0 : PG_GETARG_DATUM(5);

    /* perform username check */
    check_credential_name_valid(username);

    const char *object_type = "credential";
    Datum attribute_name[] = {CStringGetTextDatum("object_type"), CStringGetTextDatum("username"),
                              CStringGetTextDatum("database_role"), CStringGetTextDatum("windows_domain"),
                              CStringGetTextDatum("comments"), CStringGetTextDatum("owner")};
    int count = lengthof(attribute_name);
    Datum attribute_value[] = {CStringGetTextDatum(object_type), username, database_role, windows_domain, comments,
                               CStringGetTextDatum(get_role_name_str())};
    multi_insert_attribute(credential_name, attribute_name, attribute_value, count);
    for (int i = 0; i < count; i++) {
        pfree(DatumGetPointer(attribute_name[i]));
    }
    pfree(DatumGetPointer(attribute_value[0]));
}

/*
 * @brief check_job_name_valid
 *  Check if a job_name is valid.
 * @param job_name
 */
static void check_schedule_name_valid(Datum schedule_name)
{
    bool hasAlpha = false;
    char *schedule_name_str = TextDatumGetCString(schedule_name);
    if (!IsLegalNameStr(schedule_name_str, &hasAlpha)) {
        ereport(ERROR, (errmodule(MOD_JOB), errcode(ERRCODE_INVALID_NAME),
                        errmsg("Invalid schedule name."), errdetail("Schedule name contains invalid character."),
                        errcause("Schedule name is too long/conatains invalid character."),
                        erraction("Please enter a valid name.")));
    }
    if (!hasAlpha) {
        ereport(ERROR, (errmodule(MOD_JOB), errcode(ERRCODE_INVALID_NAME),
                        errmsg("Invalid schedule name."),
                        errdetail("Schedule name should contain at least one alphabet."),
                        errcause("Invalid object name given."),
                        erraction("Please enter a valid name")));
    }
    pfree(schedule_name_str);
}

/*
 * @brief check_job_creation_privilege
 *  Check if current user can create the job.
 * @param job_type
 */
static void check_schedule_creation_privilege()
{
    char *username = get_role_name_str();
    check_privilege(username, CREATE_JOB_PRIVILEGE);
    pfree(username);
}

/*
 * @brief create_schedule_internal
 *  Create a schedule
 */
void create_schedule_internal(PG_FUNCTION_ARGS)
{
    Datum schedule_name = PG_GETARG_DATUM(0);
    Datum start_date = PG_ARGISNULL(1) ? Datum(0) : TimeStampTzToText(PG_GETARG_DATUM(1));
    Datum repeat_interval = PG_ARGISNULL(2) ? Datum(0) : PG_GETARG_DATUM(2);
    Datum end_date = PG_ARGISNULL(3) ? Datum(0) : TimeStampTzToText(PG_GETARG_DATUM(3));
    Datum comments = PG_ARGISNULL(4) ? Datum(0) : PG_GETARG_DATUM(4);

    /* Various checks */
    check_schedule_creation_privilege();
    check_schedule_name_valid(schedule_name);

    const char *object_type = "schedule";
    char *username = get_role_name_str();
    Datum owner = CStringGetTextDatum(username);
    Datum attribute_name[] = {CStringGetTextDatum("object_type"), CStringGetTextDatum("start_date"),
                              CStringGetTextDatum("repeat_interval"), CStringGetTextDatum("end_date"),
                              CStringGetTextDatum("comments"), CStringGetTextDatum("owner")};
    int count = lengthof(attribute_name);
    Datum attribute_value[] = {CStringGetTextDatum(object_type), start_date, repeat_interval, end_date, comments,
                               owner};
    multi_insert_attribute(schedule_name, attribute_name, attribute_value, count);
    for (int i = 0; i < count; i++) {
        pfree(DatumGetPointer(attribute_name[i]));
    }
    pfree(DatumGetPointer(attribute_value[0]));
}

/*
 * @brief check_program_name_valid
 *  Check if program_name is valid.
 * @param program_name
 * @param is_inline
 */
static void check_program_name_valid(Datum program_name, bool is_inline)
{
    bool hasAlpha = false;
    char *program_name_str = TextDatumGetCString(program_name);
    if (!is_inline && strncmp(program_name_str, INLINE_JOB_PROGRAM_PREFIX, strlen(INLINE_JOB_PROGRAM_PREFIX)) == 0) {
        ereport(ERROR, (errmodule(MOD_JOB), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("Invalid program name"), errdetail("Program name cannot start with inline_program_"),
                        errcause("Program name cannot start with inline_program_"),
                        erraction("Please enter a valid program name")));
    }
    if (!IsLegalNameStr(program_name_str, &hasAlpha)) {
        ereport(ERROR, (errmodule(MOD_JOB), errcode(ERRCODE_INVALID_NAME),
                        errmsg("Invalid program name"), errdetail("Program name contains invalid character"),
                        errcause("Program name is too long/conatains invalid character"),
                        erraction("Please enter a valid name")));
    }
    if (!hasAlpha) {
        ereport(ERROR, (errmodule(MOD_JOB), errcode(ERRCODE_INVALID_NAME),
                        errmsg("Invalid program name."),
                        errdetail("Program name should contain at least one alphabet."),
                        errcause("Invalid object name given."),
                        erraction("Please enter a valid name")));
    }
    pfree_ext(program_name_str);
}

/*
 * @brief check_program_type
 *  Check if program type is valid.
 * @param program_type
 */
static void check_program_type_valid(Datum program_type)
{
    char *type = TextDatumGetCString(program_type);
    if (pg_strcasecmp(type, EXTERNAL_JOB_TYPE) == 0) {
        check_privilege(get_role_name_str(), CREATE_EXTERNAL_JOB_PRIVILEGE);
    }
    const char *valid_types[] = {PLSQL_JOB_TYPE, PRECEDURE_JOB_TYPE, EXTERNAL_JOB_TYPE};
    int count = lengthof(valid_types);
    for (int i = 0; i < count; i++) {
        if (pg_strcasecmp(valid_types[i], type) == 0) {
            pfree(type);
            return;
        }
    }
    ereport(ERROR, (errmodule(MOD_JOB), errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("program_type %s not support.", type),
                    errdetail("Invalid program type."), errcause("N/A"),
                    erraction("Please use PLSQL_BLOCK, STORED_PROCEDURE, EXTERNAL_SCRIPT")));
}

/*
 * @brief check_program_creation_privilege
 *  check if current user are able to create external job.
 * @param program_type
 * @param check_run_external
 */
static void check_program_creation_privilege(Datum program_type)
{
    char *program_type_str = TextDatumGetCString(program_type);
    char *username = get_role_name_str();
    check_privilege(username, CREATE_JOB_PRIVILEGE);

    if (pg_strcasecmp(program_type_str, EXTERNAL_JOB_TYPE) == 0) {
        check_privilege(username, CREATE_EXTERNAL_JOB_PRIVILEGE);
    }
    pfree(program_type_str);
    pfree(username);
}

/*
 * @brief check_program_action
 *  Check if program action contains sensitive info.
 * @param action    program action/job with inlined program action
 * @param type      program type/job with inlined program type
 */
void check_program_action(Datum action)
{
    if (action == (Datum)0) {
        return;
    }

    char *action_str = TextDatumGetCString(action);
    char *masked_str = maskPassword(action_str);
    if (masked_str != NULL) {
        /* free allocated memory at the end of transaction */
        ereport(ERROR, (errmodule(MOD_JOB), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("Job action specified is not allowed."),
                        errdetail("Job action cannot be executed securely."),
                        errcause("Unsecure action specified."),
                        erraction("Please revise your job action.")));
    }
}

/*
 * @brief create_program_internal
 *  Create a program
 */
void create_program_internal(PG_FUNCTION_ARGS, bool is_inline)
{
    Datum program_name = PG_GETARG_DATUM(0);
    Datum program_type = PG_GETARG_DATUM(1);
    Datum program_action = PG_ARGISNULL(2) ? (Datum)0 : PG_GETARG_DATUM(2);
    Datum number_of_arguments = Int32ToText(PG_GETARG_INT32(3));
    Datum enabled = BoolToText(PG_GETARG_DATUM(4));
    Datum comments = PG_ARGISNULL(5) ? (Datum)0 : PG_GETARG_DATUM(5);

    /* perform program check */
    if (!(u_sess->attr.attr_sql.sql_compatibility == B_FORMAT)) {
        check_program_name_valid(program_name, is_inline);
    }
    check_program_type_valid(program_type);
    check_program_creation_privilege(program_type);
    check_program_type_argument(program_type, PG_GETARG_INT32(3));
    check_program_action(program_action);
    if (PG_GETARG_BOOL(4)) {
        check_if_arguments_defined(program_name, PG_GETARG_INT32(3));
    }

    const char *object_type = "program";
    char *username = get_role_name_str();
    Datum owner = CStringGetTextDatum(username);
    Datum attribute_name[] = {CStringGetTextDatum("object_type"), CStringGetTextDatum("program_type"),
                              CStringGetTextDatum("program_action"), CStringGetTextDatum("number_of_arguments"),
                              CStringGetTextDatum("enabled"), CStringGetTextDatum("comments"),
                              CStringGetTextDatum("owner")};
    int count = lengthof(attribute_name);
    Datum attribute_value[] = {CStringGetTextDatum(object_type), program_type, program_action, number_of_arguments,
                               enabled, comments, owner};
    multi_insert_attribute(program_name, attribute_name, attribute_value, count);

    for (int i = 0; i < count; i++) {
        pfree(DatumGetPointer(attribute_name[i]));
    }
    pfree(DatumGetPointer(attribute_value[0]));
}

/*
 * @brief dbe_insert_pg_job
 *  Insert all necessary key values into the pg_job tuple.
 * @param name          Job name
 * @param job_id        Generated job name
 * @param start_date    Job start_date, can be use input or came from existing schedule
 * @param interval      Program interval, always, inlined program or normal program
 * @param end_date      Job end_date, can be use input or came from existing schedule
 * @param enabled       Object enabled? or not
 */
static void dbe_insert_pg_job(Datum name, Datum job_id, Datum start_date, Datum interval, Datum end_date, Datum enabled,
                              Datum priv_user, Datum log_user, Datum schema_name)
{
    errno_t rc = EOK;
    Datum values[Natts_pg_job];
    rc = memset_s(values, sizeof(values), 0, sizeof(values));
    securec_check(rc, "\0", "\0");
    bool nulls[Natts_pg_job];
    rc = memset_s(nulls, sizeof(nulls), false, sizeof(nulls));
    securec_check_c(rc, "\0", "\0");
    Relation pg_job_rel = heap_open(PgJobRelationId, RowExclusiveLock);

    /* Fill lookup values */
    values[Anum_pg_job_interval - 1] = PointerIsValid(interval) ? interval : CStringGetTextDatum("null");
    values[Anum_pg_job_job_id - 1] = job_id;
    values[Anum_pg_job_job_name - 1] = name;
    values[Anum_pg_job_enable - 1] = TextToBool(enabled);
    values[Anum_pg_job_start_date - 1] = DirectFunctionCall1(timestamptz_timestamp, TextToTimeStampTz(start_date));
    values[Anum_pg_job_end_date - 1] = DirectFunctionCall1(timestamptz_timestamp, TextToTimeStampTz(end_date));
    values[Anum_pg_job_log_user - 1] = log_user;
    values[Anum_pg_job_priv_user - 1] = priv_user; /* program's owner */
    values[Anum_pg_job_nspname - 1] = schema_name;

    /* Fill other values */
    const char* db_name = get_and_check_db_name(u_sess->proc_cxt.MyDatabaseId, true);
    values[Anum_pg_job_dbname - 1] = DirectFunctionCall1(namein, CStringGetDatum(db_name));
    values[Anum_pg_job_node_name - 1] = get_pg_job_node_name();
    values[Anum_pg_job_job_status - 1] = CharGetDatum(PGJOB_SUCC_STATUS);
    values[Anum_pg_job_current_postgres_pid - 1] = Int64GetDatum(-1);
    nulls[Anum_pg_job_last_start_date - 1] = true;
    nulls[Anum_pg_job_last_end_date - 1] = true;
    nulls[Anum_pg_job_last_suc_date - 1] = true;
    nulls[Anum_pg_job_this_run_date - 1] = true;
    values[Anum_pg_job_next_run_date - 1] = DirectFunctionCall1(timestamptz_timestamp, TextToTimeStampTz(start_date));
    values[Anum_pg_job_failure_msg - 1] = PointerGetDatum(cstring_to_text(""));

    /* Insert into pg_job */
    HeapTuple tuple = heap_form_tuple(RelationGetDescr(pg_job_rel), values, nulls);
    (void)simple_heap_insert(pg_job_rel, tuple);
    CatalogUpdateIndexes(pg_job_rel, tuple);
    heap_freetuple_ext(tuple);
    heap_close(pg_job_rel, NoLock);
}

/*
 * @brief get_inline_schedule_name
 *  Get the inline schedule name
 * @param job_name
 * @return char*
 */
char *get_inline_schedule_name(Datum job_name)
{
    errno_t rc;
    char *c_job_name = TextDatumGetCString(job_name);

    char *schedule_name = (char *)palloc(sizeof(char) * MAX_JOB_NAME_LEN);
    rc = strcpy_s(schedule_name, MAX_JOB_NAME_LEN, INLINE_JOB_SCHEDULE_PREFIX);
    securec_check(rc, "\0", "\0");
    rc = strcat_s(schedule_name, MAX_JOB_NAME_LEN, c_job_name);
    securec_check(rc, "\0", "\0");
    return schedule_name;
}

/*
 * @brief create_inline_program
 *  Create a inline program object
 */
char *create_inline_program(Datum job_name, Datum job_type, Datum job_action, Datum num_of_args, Datum enabled)
{
    errno_t rc;
    char *c_job_name = TextDatumGetCString(job_name);

    char *program_name = (char *)palloc(sizeof(char) * MAX_JOB_NAME_LEN);
    rc = strcpy_s(program_name, MAX_JOB_NAME_LEN, INLINE_JOB_PROGRAM_PREFIX);
    securec_check(rc, "\0", "\0");
    rc = strcat_s(program_name, MAX_JOB_NAME_LEN, c_job_name);
    securec_check(rc, "\0", "\0");

    static const short nrgs_program = 6;
    FunctionCallInfoData fcinfo_program;
    InitFunctionCallInfoData(fcinfo_program, NULL, nrgs_program, InvalidOid, NULL, NULL);
    rc = memset_s(fcinfo_program.arg, nrgs_program * sizeof(Datum), 0, nrgs_program * sizeof(Datum));
    securec_check(rc, "\0", "\0");
    rc = memset_s(fcinfo_program.argnull, nrgs_program * sizeof(bool), 0, nrgs_program * sizeof(bool));
    securec_check(rc, "\0", "\0");
    fcinfo_program.arg[0] = CStringGetTextDatum(program_name);  /* program_name */
    fcinfo_program.arg[1] = job_type;                           /* program_type */
    fcinfo_program.arg[2] = job_action;                         /* program_action */
    fcinfo_program.arg[3] = num_of_args;                        /* number_of_arguments */
    fcinfo_program.arg[4] = enabled;                            /* enabled */
    fcinfo_program.argnull[5] = true;                           /* comments */
    create_program_internal(&fcinfo_program, true);

    return program_name;
}

char *CreateEventInlineProgram(Datum job_name, Datum job_type, Datum job_action, Datum job_definer)
{
    errno_t rc;
    char *c_job_name = TextDatumGetCString(job_name);

    char *program_name = (char *)palloc(sizeof(char) * MAX_JOB_NAME_LEN);
    rc = strcpy_s(program_name, MAX_JOB_NAME_LEN, INLINE_JOB_PROGRAM_PREFIX);
    securec_check(rc, "\0", "\0");
    rc = strcat_s(program_name, MAX_JOB_NAME_LEN, c_job_name);
    securec_check(rc, "\0", "\0");

    /* perform program check */
    if (!(u_sess->attr.attr_sql.sql_compatibility == B_FORMAT)) {
        check_program_name_valid(CStringGetTextDatum(program_name), true);
    }
    check_program_type_valid(job_type);
    check_program_type_argument(job_type, 0);
    check_program_action(job_action);
    check_if_arguments_defined(CStringGetTextDatum(program_name), 0);

    const char *object_type = "program";
    Datum attribute_name[] = {CStringGetTextDatum("object_type"),    CStringGetTextDatum("program_type"),
                              CStringGetTextDatum("program_action"), CStringGetTextDatum("number_of_arguments"),
                              CStringGetTextDatum("enabled"),        CStringGetTextDatum("comments"),
                              CStringGetTextDatum("owner")};
    int count = lengthof(attribute_name);
    Datum attribute_value[] = {
        CStringGetTextDatum(object_type), job_type, job_action, CStringGetTextDatum("0"), BoolToText(BoolGetDatum(true)), (Datum)0, job_definer};
    multi_insert_attribute(CStringGetTextDatum(program_name), attribute_name, attribute_value, count);

    for (int i = 0; i < count; i++) {
        pfree(DatumGetPointer(attribute_name[i]));
    }
    pfree(DatumGetPointer(attribute_value[0]));

    return program_name;
}

/*
 * @brief get_job_id
 *  Get the unique job id.
 * @return int id
 */
static int get_job_id()
{
    /* pg_job */
    Relation rel = heap_open(PgJobRelationId, RowExclusiveLock);
    uint16 id = 0;
    int ret = jobid_alloc(&id, JOBID_MAX_NUMBER);
    if (ret == JOBID_ALLOC_ERROR) {
        ereport(ERROR, (errmodule(MOD_JOB), errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                        errmsg("Fail to create job"),
                        errdetail("All 32768 jobids have alloc, and there is no free jobid"),
                        errcause("N/A"), erraction("Please drop inactive jobs")));
    }
    heap_close(rel, NoLock);
    return id;
}

/*
 * @brief check_job_creation_privilege
 *  Check if current user can create the job.
 * @param job_type
 */
static void check_job_creation_privilege(Datum job_type)
{
    char *job_type_str = TextDatumGetCString(job_type);
    char *username = get_role_name_str();
    check_privilege(username, CREATE_JOB_PRIVILEGE);

    if (pg_strcasecmp(job_type_str, EXTERNAL_JOB_TYPE) == 0) {
        check_privilege(username, CREATE_EXTERNAL_JOB_PRIVILEGE);
    }
    pfree(job_type_str);
    pfree(username);
}

/*
 * @brief check_job_name_valid
 *  Check if a job_name is valid.
 * @param job_name
 */
static void check_job_name_valid(Datum job_name)
{
    bool hasAlpha = false;
    char *job_name_str = TextDatumGetCString(job_name);
    if (!IsLegalNameStr(job_name_str, &hasAlpha)) {
        ereport(ERROR, (errmodule(MOD_JOB), errcode(ERRCODE_INVALID_NAME),
                        errmsg("Fail to create job."), errdetail("Invalid job name."),
                        errcause("Job name is too long/conatains invalid character."),
                        erraction("Please enter a valid name.")));
    }
    if (!hasAlpha) {
        ereport(ERROR, (errmodule(MOD_JOB), errcode(ERRCODE_INVALID_NAME),
                        errmsg("Invalid job name."),
                        errdetail("Job name should contain at least one alphabet."),
                        errcause("Invalid object name given."),
                        erraction("Please enter a valid name")));
    }
    pfree(job_name_str);
}

/*
 * @brief get_priv_user
 *  Get privilege user base on job_intype.
 */
Datum get_priv_user(Datum program_name, Datum job_intype)
{
    Datum priv_user = 0;
    switch (DatumGetChar(job_intype)) {
        case JOB_INTYPE_PLAIN: /* fall through */
        case JOB_INTYPE_SCHEDULE:
            return DirectFunctionCall1(namein, CStringGetDatum(GetUserNameFromId(GetUserId())));
        case JOB_INTYPE_SCHEDULE_PROGRAM: /* fall through */
        case JOB_INTYPE_PROGRAM: {
            check_object_is_visible(program_name);
            JobAttribute *attributes = (JobAttribute *)palloc(sizeof(JobAttribute));
            attributes[0].object_name = program_name;
            attributes[0].name = CStringGetTextDatum("owner");
            batch_lookup_job_attribute(attributes, 1);
            priv_user = DirectFunctionCall1(namein, CStringGetDatum(get_raw_role_str(attributes[0].value)));
            pfree_ext(attributes);
            break;
        }
        default: {
            ereport(ERROR, (errmodule(MOD_JOB), errcode(ERRCODE_INVALID_NAME),
                    errmsg("Fail to get privilege user."),
                    errdetail("Cannot identify job intype."), errcause("Wrong job intype."),
                    erraction("Please contact engineer to support.")));
        }
    }
    return priv_user;
}

/*
 * @brief create_job_raw
 *  Bare bone function of creating a job.
 */
void create_job_raw(PG_FUNCTION_ARGS)
{
    Datum job_name = PG_GETARG_DATUM(0);
    Datum program_name = PG_GETARG_DATUM(1);
    Datum schedule_name = PG_GETARG_DATUM(2);
    Datum job_class = PG_GETARG_DATUM(3);
    Datum enabled = BoolToText(PG_GETARG_DATUM(4));
    Datum auto_drop = BoolToText(PG_GETARG_DATUM(5));
    Datum comments = PG_GETARG_DATUM(6);
    Datum job_style = PG_GETARG_DATUM(7);
    Datum credential_name = PG_GETARG_DATUM(8);
    Datum destination_name = PG_GETARG_DATUM(9);
    Datum job_intype = PG_GETARG_DATUM(10);
    Datum start_date = PG_GETARG_DATUM(11);
    Datum repeat_interval = PG_GETARG_DATUM(12);
    Datum end_date = PG_GETARG_DATUM(13);
    Datum job_action = PG_GETARG_DATUM(14);
    Datum job_type = PG_GETARG_DATUM(15);
    Datum job_definer = (PG_ARGISNULL(16)) ? Datum(0) : PG_GETARG_DATUM(16);
    Datum job_schemaname = (PG_ARGISNULL(17)) ? Datum(0) : PG_GETARG_DATUM(17);
    Datum job_definer_oid = (PG_ARGISNULL(18)) ? Datum(0) : PG_GETARG_DATUM(18);

    /* Various checks */
    if (PG_ARGISNULL(16)) {
        check_job_name_valid(job_name);
        check_job_creation_privilege(job_type);
    }
    check_job_class_valid(job_class);

    /* gs_job_attribute */
    const char *object_type = "job";
    Datum owner = Datum(0);
    if (PG_ARGISNULL(16)) {
        char *username = get_role_name_str();
        owner = CStringGetTextDatum(username);
    } else {
        owner = job_definer_oid;
    }
    const Datum attribute_name[] = {CStringGetTextDatum("object_type"), CStringGetTextDatum("program_name"),
                                    CStringGetTextDatum("schedule_name"), CStringGetTextDatum("job_class"),
                                    CStringGetTextDatum("auto_drop"), CStringGetTextDatum("comments"),
                                    CStringGetTextDatum("job_style"), CStringGetTextDatum("credential_name"),
                                    CStringGetTextDatum("destination_name"), CStringGetTextDatum("owner")};
    int count = lengthof(attribute_name);
    const Datum attribute_value[] = {CStringGetTextDatum(object_type), program_name, schedule_name, job_class,
                                     auto_drop, comments, job_style, credential_name, destination_name, owner};
    multi_insert_attribute(job_name, attribute_name, attribute_value, count);

    /* pg_job && pg_job_proc */
    Datum job_id = Int16GetDatum(get_job_id());
    Datum priv_user;
    Datum log_user;
    if (PG_ARGISNULL(16)) {
        priv_user = get_priv_user(program_name, job_intype);
        log_user = DirectFunctionCall1(namein, CStringGetDatum(GetUserNameFromId(GetUserId())));
    } else {
        priv_user = DirectFunctionCall1(namein, job_definer);
        log_user = DirectFunctionCall1(namein, job_definer);
    }
    Datum schema_name;
    if (PG_ARGISNULL(17)) {
        schema_name = DirectFunctionCall1(namein, CStringGetDatum(get_real_search_schema()));
    } else {
        schema_name = DirectFunctionCall1(namein, job_schemaname);
    }
    dbe_insert_pg_job(job_name, job_id, start_date, repeat_interval, end_date, enabled, priv_user, log_user,
                      schema_name);
    dbe_insert_pg_job_proc(job_id, job_action, job_name);
    for (int i = 0; i < count; i++) {
        pfree(DatumGetPointer(attribute_name[i]));
    }
    pfree(DatumGetPointer(attribute_value[0]));
}

/*
 * @brief create_job_1_internal
 *  Create a job with inline program and inline schedule.
 */
void create_job_1_internal(PG_FUNCTION_ARGS)
{
    /* inline schedule */
    Datum job_name = PG_GETARG_DATUM(0);
    Datum start_date = PG_ARGISNULL(4) ? GetCurrentTimestamp() : PG_GETARG_DATUM(4);
    Datum repeat_interval = PG_ARGISNULL(5) ? CStringGetTextDatum("null") : PG_GETARG_DATUM(5);
    Datum end_date = PG_ARGISNULL(6) ? get_scheduler_max_timestamptz() : PG_GETARG_DATUM(6);
    char* c_schedule_name = get_inline_schedule_name(job_name);
    Datum schedule_name = CStringGetTextDatum(c_schedule_name);

    /* inline program */
    Datum job_type = PG_GETARG_DATUM(1);
    Datum job_action = PG_GETARG_DATUM(2);
    Datum num_of_args = PG_GETARG_DATUM(3);
    Datum enabled = PG_GETARG_DATUM(8);
    char* c_program_name = create_inline_program(job_name, job_type, job_action, num_of_args, BoolGetDatum(true));
    Datum program_name = CStringGetTextDatum(c_program_name);
    pfree_ext(c_program_name);

    static const short nrgs_job = 19;
    FunctionCallInfoData fcinfo_job;
    InitFunctionCallInfoData(fcinfo_job, NULL, nrgs_job, InvalidOid, NULL, NULL);
    errno_t rc = memset_s(fcinfo_job.arg, nrgs_job * sizeof(Datum), 0, nrgs_job * sizeof(Datum));
    securec_check(rc, "\0", "\0");
    rc = memset_s(fcinfo_job.argnull, nrgs_job * sizeof(bool), 0, nrgs_job * sizeof(bool));
    securec_check(rc, "\0", "\0");

    fcinfo_job.arg[0] = job_name;               /* job_name */
    fcinfo_job.arg[1] = program_name;           /* program_name */
    fcinfo_job.arg[2] = schedule_name;          /* schedule_name */
    fcinfo_job.arg[3] = PG_GETARG_DATUM(7);     /* job_class */
    fcinfo_job.arg[4] = enabled;                /* enabled */
    fcinfo_job.arg[5] = PG_GETARG_DATUM(9);     /* auto_drop */
    fcinfo_job.arg[6] = PG_ARGISNULL(10) ? Datum(0) : PG_GETARG_DATUM(10);    /* comments */
    fcinfo_job.arg[7] = Datum(0);               /* job_style */
    fcinfo_job.arg[8] = PG_ARGISNULL(11) ? Datum(0) : PG_GETARG_DATUM(11);    /* credential_name */
    fcinfo_job.arg[9] = PG_ARGISNULL(12) ? Datum(0) : PG_GETARG_DATUM(12);    /* destination_name */
    fcinfo_job.arg[10] = CharGetDatum(JOB_INTYPE_PLAIN); /* job_intype */
    fcinfo_job.arg[11] = TimeStampTzToText(start_date); /* start_date */
    fcinfo_job.arg[12] = repeat_interval;       /* repeat_interval */
    fcinfo_job.arg[13] = TimeStampTzToText(end_date); /* end_date */
    fcinfo_job.arg[14] = job_action;            /* job action */
    fcinfo_job.arg[15] = job_type;              /* job type */
    fcinfo_job.argnull[16] = true;
    fcinfo_job.argnull[17] = true;
    fcinfo_job.argnull[18] = true;
    create_job_raw(&fcinfo_job);
}

/*
 * @brief get_schedule_info
 *
 * @param schedule_name
 * @param start_date
 * @param repeat_interval
 * @param end_date
 */
static void get_schedule_info(Datum schedule_name, Datum *start_date, Datum *repeat_interval, Datum *end_date)
{
    check_object_is_visible(schedule_name);
    int count = 3;
    JobAttribute *attributes = (JobAttribute *)palloc(sizeof(JobAttribute) * count);
    attributes[0].object_name = schedule_name;
    attributes[0].name = CStringGetTextDatum("start_date");
    attributes[1].object_name = schedule_name;
    attributes[1].name = CStringGetTextDatum("repeat_interval");
    attributes[2].object_name = schedule_name;
    attributes[2].name = CStringGetTextDatum("end_date");
    batch_lookup_job_attribute(attributes, count);

    *start_date = (attributes[0].null) ? TimeStampTzToText(GetCurrentTimestamp()) : attributes[0].value;
    *repeat_interval = (attributes[1].null) ? CStringGetTextDatum("null") : attributes[1].value;
    *end_date = (attributes[2].null) ? TimeStampTzToText(get_scheduler_max_timestamptz()) : attributes[2].value;

    pfree_ext(attributes);
}

/*
 * @brief get_program_info
 *  Get relative program info from system table.
 * @param program_name
 * @param job_type
 * @param job_action
 * @param num_of_args
 * @param enabled
 */
void get_program_info(Datum program_name, Datum *job_type, Datum *job_action, Datum *num_of_args, Datum *enabled)
{
    check_object_is_visible(program_name);
    /* total of 4 attributes: program_type, number_of_arguments, enabled, program_action from program */
    int count = 4;
    JobAttribute *attributes = (JobAttribute *)palloc(sizeof(JobAttribute) * count);
    attributes[0].object_name = program_name;
    attributes[0].name = CStringGetTextDatum("program_type");
    attributes[1].object_name = program_name;
    attributes[1].name = CStringGetTextDatum("number_of_arguments");
    attributes[2].object_name = program_name;
    attributes[2].name = CStringGetTextDatum("enabled");
    attributes[3].object_name = program_name;
    attributes[3].name = CStringGetTextDatum("program_action");
    batch_lookup_job_attribute(attributes, count);

    *job_type = (attributes[0].null) ? Datum(0) : attributes[0].value;
    *num_of_args = (attributes[1].null) ? Datum(0) : attributes[1].value;
    *enabled = (attributes[2].null) ? Datum(0) : attributes[2].value;
    *job_action = (attributes[3].null) ? Datum(0) : attributes[3].value;

    pfree_ext(attributes);
}

/*
 * @brief create_job_2_internal
 *  Create a job with program and schedule.
 */
void create_job_2_internal(PG_FUNCTION_ARGS)
{
    /* schedule */
    Datum schedule_name = PG_GETARG_DATUM(2);
    Datum start_date;
    Datum repeat_interval;
    Datum end_date;
    get_schedule_info(schedule_name, &start_date, &repeat_interval, &end_date);

    /* program */
    Datum program_name = PG_GETARG_DATUM(1);
    Datum job_type;
    Datum job_action;
    Datum num_of_args;
    Datum enabled;
    get_program_info(program_name, &job_type, &job_action, &num_of_args, &enabled);

    static const short nrgs_job = 19;
    FunctionCallInfoData fcinfo_job;
    InitFunctionCallInfoData(fcinfo_job, NULL, nrgs_job, InvalidOid, NULL, NULL);
    errno_t rc = memset_s(fcinfo_job.arg, nrgs_job * sizeof(Datum), 0, nrgs_job * sizeof(Datum));
    securec_check(rc, "\0", "\0");
    rc = memset_s(fcinfo_job.argnull, nrgs_job * sizeof(bool), 0, nrgs_job * sizeof(bool));
    securec_check(rc, "\0", "\0");

    fcinfo_job.arg[0] = PG_GETARG_DATUM(0);     /* job_name */
    fcinfo_job.arg[1] = program_name;           /* program_name */
    fcinfo_job.arg[2] = schedule_name;          /* schedule_name */
    fcinfo_job.arg[3] = PG_GETARG_DATUM(3);     /* job_class */
    fcinfo_job.arg[4] = PG_GETARG_DATUM(4);     /* enabled */
    fcinfo_job.arg[5] = PG_GETARG_DATUM(5);     /* auto_drop */
    fcinfo_job.arg[6] = PG_ARGISNULL(6) ? Datum(0) : PG_GETARG_DATUM(6);     /* comments */
    fcinfo_job.arg[7] = PG_GETARG_DATUM(7);     /* job_style */
    fcinfo_job.arg[8] = PG_ARGISNULL(8) ? Datum(0) : PG_GETARG_DATUM(8);     /* credential_name */
    fcinfo_job.arg[9] = PG_ARGISNULL(9) ? Datum(0) : PG_GETARG_DATUM(9);     /* destination_name */
    fcinfo_job.arg[10] = CharGetDatum(JOB_INTYPE_SCHEDULE_PROGRAM); /* job_intype */

    /* pg_job */
    fcinfo_job.arg[11] = start_date;            /* start_date */
    fcinfo_job.arg[12] = repeat_interval;       /* repeat_interval */
    fcinfo_job.arg[13] = end_date;              /* end_date */
    fcinfo_job.arg[14] = job_action;            /* job action */
    fcinfo_job.arg[15] = job_type;              /* job type */
    fcinfo_job.argnull[16] = true;
    fcinfo_job.argnull[17] = true;
    fcinfo_job.argnull[18] = true;
    create_job_raw(&fcinfo_job);
}

/*
 * @brief create_job_3_internal
 *  Create a job with program and inline schedule.
 */
void create_job_3_internal(PG_FUNCTION_ARGS)
{
    /* inline schedule */
    Datum job_name = PG_GETARG_DATUM(0);
    Datum start_date = PG_ARGISNULL(2) ? GetCurrentTimestamp() : PG_GETARG_DATUM(2);
    Datum repeat_interval = PG_ARGISNULL(3) ? CStringGetTextDatum("null") : PG_GETARG_DATUM(3);
    Datum end_date = PG_ARGISNULL(4) ? get_scheduler_max_timestamptz() : PG_GETARG_DATUM(4);
    char* c_schedule_name = get_inline_schedule_name(job_name);
    Datum schedule_name = CStringGetTextDatum(c_schedule_name);

    /* program */
    Datum program_name = PG_GETARG_DATUM(1);
    Datum job_type;
    Datum job_action;
    Datum num_of_args;
    Datum enabled;
    get_program_info(program_name, &job_type, &job_action, &num_of_args, &enabled);

    static const short nrgs_job = 19;
    FunctionCallInfoData fcinfo_job;
    InitFunctionCallInfoData(fcinfo_job, NULL, nrgs_job, InvalidOid, NULL, NULL);
    errno_t rc = memset_s(fcinfo_job.arg, nrgs_job * sizeof(Datum), 0, nrgs_job * sizeof(Datum));
    securec_check(rc, "\0", "\0");
    rc = memset_s(fcinfo_job.argnull, nrgs_job * sizeof(bool), 0, nrgs_job * sizeof(bool));
    securec_check(rc, "\0", "\0");

    fcinfo_job.arg[0] = job_name;               /* job_name */
    fcinfo_job.arg[1] = program_name;           /* program_name */
    fcinfo_job.arg[2] = schedule_name;          /* schedule_name */
    fcinfo_job.arg[3] = PG_GETARG_DATUM(5);     /* job_class */
    fcinfo_job.arg[4] = PG_GETARG_DATUM(6);     /* enabled */
    fcinfo_job.arg[5] = PG_GETARG_DATUM(7);     /* auto_drop */
    fcinfo_job.arg[6] = PG_ARGISNULL(8) ? Datum(0) : PG_GETARG_DATUM(8);     /* comments */
    fcinfo_job.arg[7] = PG_GETARG_DATUM(9);     /* job_style */
    fcinfo_job.arg[8] = PG_ARGISNULL(10) ? Datum(0) : PG_GETARG_DATUM(10);    /* credential_name */
    fcinfo_job.arg[9] = PG_ARGISNULL(11) ? Datum(0) : PG_GETARG_DATUM(11);    /* destination_name */
    fcinfo_job.arg[10] = CharGetDatum(JOB_INTYPE_PROGRAM); /* job_intype */

    /* pg_job */
    fcinfo_job.arg[11] = TimeStampTzToText(start_date); /* start_date */
    fcinfo_job.arg[12] = repeat_interval;               /* repeat_interval */
    fcinfo_job.arg[13] = TimeStampTzToText(end_date);   /* end_date */
    fcinfo_job.arg[14] = job_action;                    /* job action */
    fcinfo_job.arg[15] = job_type;                      /* job type */
    fcinfo_job.argnull[16] = true;
    fcinfo_job.argnull[17] = true;
    fcinfo_job.argnull[18] = true;
    create_job_raw(&fcinfo_job);
}

/*
 * @brief create_job_4_internal
 *  Create a job with inline program and schedule.
 */
void create_job_4_internal(PG_FUNCTION_ARGS)
{
    /* schedule */
    Datum schedule_name = PG_GETARG_DATUM(1);
    Datum start_date;
    Datum repeat_interval;
    Datum end_date;
    get_schedule_info(schedule_name, &start_date, &repeat_interval, &end_date);

    /* inline program */
    Datum job_name = PG_GETARG_DATUM(0);
    Datum job_type = PG_GETARG_DATUM(2);

    Datum job_action = PG_GETARG_DATUM(3);
    Datum num_of_args = PG_GETARG_DATUM(4);
    Datum enabled = PG_GETARG_DATUM(6);
    char* c_program_name = create_inline_program(job_name, job_type, job_action, num_of_args, BoolGetDatum(true));
    Datum program_name = CStringGetTextDatum(c_program_name);
    pfree_ext(c_program_name);

    static const short nrgs_job = 19;
    FunctionCallInfoData fcinfo_job;
    InitFunctionCallInfoData(fcinfo_job, NULL, nrgs_job, InvalidOid, NULL, NULL);

    fcinfo_job.arg[0] = job_name;               /* job_name */
    fcinfo_job.arg[1] = program_name;           /* program_name */
    fcinfo_job.arg[2] = schedule_name;          /* schedule_name */
    fcinfo_job.arg[3] = PG_GETARG_DATUM(5);     /* job_class */
    fcinfo_job.arg[4] = enabled;                /* enabled */
    fcinfo_job.arg[5] = PG_GETARG_DATUM(7);     /* auto_drop */
    fcinfo_job.arg[6] = PG_ARGISNULL(8) ? Datum(0) : PG_GETARG_DATUM(8);     /* comments */
    fcinfo_job.arg[7] = Datum(0);               /* job_style */
    fcinfo_job.arg[8] = PG_ARGISNULL(9) ? Datum(0) : PG_GETARG_DATUM(9);     /* credential_name */
    fcinfo_job.arg[9] = PG_ARGISNULL(10) ? Datum(0) : PG_GETARG_DATUM(10);    /* destination_name */
    fcinfo_job.arg[10] = CharGetDatum(JOB_INTYPE_SCHEDULE); /* job_intype */

    /* pg_job */
    fcinfo_job.arg[11] = start_date;            /* start_date */
    fcinfo_job.arg[12] = repeat_interval;       /* repeat_interval */
    fcinfo_job.arg[13] = end_date;              /* end_date */
    fcinfo_job.arg[14] = job_action;            /* job action */
    fcinfo_job.arg[15] = job_type;              /* job type */
    fcinfo_job.argnull[16] = true;
    fcinfo_job.argnull[17] = true;
    fcinfo_job.argnull[18] = true;
    create_job_raw(&fcinfo_job);
}

/*
 * @brief check_job_name_valid
 *  Check if a job_name is valid.
 * @param job_name
 */
static void check_job_class_name_valid(Datum job_class_name)
{
    bool hasAlpha = false;
    char *job_class_name_str = TextDatumGetCString(job_class_name);
    if (pg_strcasecmp(job_class_name_str, "DEFAULT_JOB_CLASS") == 0) {
        return;
    }
    if (!IsLegalNameStr(job_class_name_str, &hasAlpha)) {
        ereport(ERROR, (errmodule(MOD_JOB), errcode(ERRCODE_INVALID_NAME),
                        errmsg("Invalid job class name"), errdetail("Job class name contains invalid character"),
                        errcause("Job class name is too long/conatains invalid character"),
                        erraction("Please enter a valid name")));
    }
    if (!hasAlpha) {
        ereport(ERROR, (errmodule(MOD_JOB), errcode(ERRCODE_INVALID_NAME),
                        errmsg("Invalid job class name."),
                        errdetail("Job class name should contain at least one alphabet."),
                        errcause("Invalid object name given."),
                        erraction("Please enter a valid name")));
    }
    pfree(job_class_name_str);
}

/*
 * @brief check_job_creation_privilege
 *  Check if current user can create the job.
 * @param job_type
 */
static void check_job_class_creation_privilege()
{
    char *username = get_role_name_str();
    check_privilege(username, CREATE_JOB_PRIVILEGE);
    pfree(username);
}

/*
 * @brief create_job_class_internal
 *  Create a job class object.
 */
void create_job_class_internal(PG_FUNCTION_ARGS)
{
    Datum job_class_name = PG_GETARG_DATUM(0);
    Datum resource_consumer_group = PG_ARGISNULL(1) ? (Datum)0 : PG_GETARG_DATUM(1);
    Datum service = PG_ARGISNULL(2) ? (Datum)0 : PG_GETARG_DATUM(2);
    Datum logging_level = Int32ToText(PG_GETARG_INT32(3));
    Datum log_history = PG_ARGISNULL(4) ? (Datum)0 : Int32ToText(PG_GETARG_INT32(4));
    Datum comments = PG_ARGISNULL(5) ? (Datum)0 : PG_GETARG_DATUM(5);

    /* Various class */
    check_job_class_name_valid(job_class_name);
    check_job_class_creation_privilege();

    const char *object_type = "job_class";
    char *username = get_role_name_str();
    Datum owner = CStringGetTextDatum(username);
    const Datum attribute_name[] = {CStringGetTextDatum("object_type"), CStringGetTextDatum("resource_consumer_group"),
                                    CStringGetTextDatum("service"),     CStringGetTextDatum("logging_level"),
                                    CStringGetTextDatum("log_history"), CStringGetTextDatum("comments"),
                                    CStringGetTextDatum("owner")};
    int count = lengthof(attribute_name);
    const Datum attribute_value[] = {CStringGetTextDatum(object_type), resource_consumer_group, service,
                                     logging_level, log_history, comments,owner};
    multi_insert_attribute(job_class_name, attribute_name, attribute_value, count);
    for (int i = 0; i < count; i++) {
        pfree(DatumGetPointer(attribute_name[i]));
    }
    pfree(DatumGetPointer(attribute_value[0]));
}

/*
 * @brief set_program_action
 *  Set program attribute.
 * @param program_name
 * @param program_action
 */
static void set_program_action(const Datum program_name, const Datum program_action)
{
    /* check if program action is valid and secure */
    check_program_action(program_action);

    /* update program action and all jobs associated with this program */
    update_attribute(program_name, CStringGetTextDatum("program_action"), program_action);
    Datum attribute_name = CStringGetTextDatum("program_name");
    Datum attribute_value = program_name;
    Relation gs_job_attribute_rel = heap_open(GsJobAttributeRelationId, RowExclusiveLock);
    List *tuples = search_related_attribute(gs_job_attribute_rel, attribute_name, attribute_value);
    ListCell *lc = NULL;
    foreach(lc, tuples) {
        HeapTuple tuple = (HeapTuple)lfirst(lc);
        bool isNull = false;
        Datum related_job_name = heap_getattr(tuple, Anum_gs_job_attribute_job_name,
                                              RelationGetDescr(gs_job_attribute_rel), &isNull);
        Assert(!isNull);
        dbe_update_pg_job_proc(program_action, related_job_name);
    }
    heap_close(gs_job_attribute_rel, NoLock);
    list_free_deep(tuples);
    pfree(DatumGetPointer(attribute_value));
}

/*
 * @brief set_job_class_attribute
 *  Set job class attributes.
 * @param job_class_name
 * @param attribute_name
 * @param attribute_value
 */
static void set_job_class_attribute(const Datum job_class_name, const Datum attribute_name, Datum attribute_value)
{
    if (strcasecmp(TextDatumGetCString(attribute_value), "DEFAULT_JOB_CLASS") != 0) {
        return;
    }
    update_attribute(job_class_name, attribute_name, attribute_value);
}

/*
 * @brief set_program_attribute
 *  Set program attributes.
 * @param program_name
 * @param attribute_name
 * @param attribute_value
 */
static void set_program_attribute(const Datum program_name, const Datum attribute_name, Datum attribute_value)
{
    if (is_inlined_program(program_name)) {
        ereport(ERROR, (errmodule(MOD_JOB), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("Invalid program name"), errdetail("Cannot modify inlined program."),
                        errcause("Try to modify inlined object."), erraction("Please enter a valid program name")));
    }

    /* Get original program values */
    Datum type = (Datum)0;
    Datum action = (Datum)0;
    Datum num_of_args = (Datum)0;
    Datum enabled = (Datum)0;
    get_program_info(program_name, &type, &action, &num_of_args, &enabled);

    /* program action and enabled needs extra checks to perform */
    char *attribute_name_str = TextDatumGetCString(attribute_name);
    if (pg_strcasecmp(attribute_name_str, "program_action") == 0) {
        set_program_action(program_name, attribute_value);
        return;
    }
    if (pg_strcasecmp(attribute_name_str, "enabled") == 0) {
        enable_program(program_name, attribute_value);
        return;
    }

    /* For type, number of arguments, we simply update the attribute, then check if it is valid */
    update_attribute(program_name, attribute_name, attribute_value);

    /* For program type, check privilege && argument needed */
    if (pg_strcasecmp(TextDatumGetCString(attribute_name), "program_type") == 0) {
        check_program_type_valid(attribute_value);
        check_program_type_argument(attribute_value, TextToInt32(num_of_args));
        char *program_type_str = TextDatumGetCString(attribute_value);
        if (pg_strcasecmp(program_type_str, EXTERNAL_JOB_TYPE) == 0) {
            check_privilege(get_role_name_str(), CREATE_EXTERNAL_JOB_PRIVILEGE);
        }
        pfree_ext(program_type_str);
        delete_deprecated_program_arguments(program_name);
    }

    /* For number_of_arguments, delete all deprecated arguments */
    if (pg_strcasecmp(TextDatumGetCString(attribute_name), "number_of_arguments") == 0) {
        check_program_type_argument(type, TextToInt32(attribute_value));
        delete_deprecated_program_arguments(program_name);
    }
}

/*
 * @brief set_schedule_attribute
 *  Note that: set_attribute don't take in NULLs, therefore we don't care if attribute_value is valid(=0).
 * @param schedule_name
 * @param attribute_name
 * @param attribute_value
 */
static void set_schedule_attribute(const Datum schedule_name, const Datum attribute_name, Datum attribute_value)
{
    if (is_inlined_schedule(schedule_name)) {
        ereport(ERROR, (errmodule(MOD_JOB), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("Invalid schedule name"), errdetail("Cannot modify inlined schedule."),
                        errcause("Try to modify inlined object."), erraction("Please enter a valid schedule name")));
    }

    Relation gs_job_attribute_rel = heap_open(GsJobAttributeRelationId, RowExclusiveLock);
    update_attribute(schedule_name, attribute_name, attribute_value);
    char *attribute_name_str = TextDatumGetCString(attribute_name);
    const char *job_related_attr[] = {"start_date", "repeat_interval", "end_date"};
    const int job_related_num[] = {Anum_pg_job_start_date, Anum_pg_job_interval, Anum_pg_job_end_date};

    int count = lengthof(job_related_attr);
    int col_att = -1;
    for (int i = 0; i < count; i++) {
        if (pg_strcasecmp(job_related_attr[i], attribute_name_str) == 0) {
            col_att = i;
            break;
        }
    }
    if (col_att == -1) {
        heap_close(gs_job_attribute_rel, NoLock);
        return;
    }
    if (job_related_num[col_att] == Anum_pg_job_start_date || job_related_num[col_att] == Anum_pg_job_end_date) {
        attribute_value = DirectFunctionCall1(timestamptz_timestamp, TextToTimeStampTz(attribute_value));
    }

    Datum schedule_name_attribute = CStringGetTextDatum("schedule_name");
    List *tuples = search_related_attribute(gs_job_attribute_rel, schedule_name_attribute, schedule_name);
    ListCell *lc = NULL;
    foreach(lc, tuples) {
        HeapTuple tuple = (HeapTuple)lfirst(lc);
        bool isnull = false;
        Datum job_name = heap_getattr(tuple, Anum_gs_job_attribute_job_name, gs_job_attribute_rel->rd_att, &isnull);
        update_pg_job(job_name, job_related_num[col_att], attribute_value);
    }
    heap_close(gs_job_attribute_rel, NoLock);
    list_free_deep(tuples);
    pfree(DatumGetPointer(schedule_name_attribute));
}

/*
 * @brief set_job_inline_schedule_attribute
 *  Set the job inline schedule attributes.
 * @param job_name
 * @param attribute_name
 * @param attribute_value
 * @param attribute_name_str
 * @return true
 * @return false
 */
static bool set_job_inline_schedule_attribute(const Datum job_name, const Datum attribute_name,
                                              const Datum attribute_value, char *attribute_name_str)
{
    /* start_date, repeat_interval, end_date, enabled store in pg_job */
    if (pg_strcasecmp(attribute_name_str, "start_date") == 0) {
        Datum start_date = DirectFunctionCall1(timestamptz_timestamp, TextToTimeStampTz(attribute_value));
        int attribute_numbers[] = {Anum_pg_job_start_date, Anum_pg_job_next_run_date};
        Datum attribute_values[] = {start_date, start_date};
        bool isnull[] = {false, false};
        update_pg_job_multi_columns(job_name, attribute_numbers, attribute_values, isnull, lengthof(attribute_numbers));
        return true;
    }
    if (pg_strcasecmp(attribute_name_str, "end_date") == 0) {
        Datum end_date = DirectFunctionCall1(timestamptz_timestamp, TextToTimeStampTz(attribute_value));
        update_pg_job(job_name, Anum_pg_job_end_date, end_date);
        return true;
    }
    if (pg_strcasecmp(attribute_name_str, "repeat_interval") == 0) {
        Datum repeat_interval = CStringGetTextDatum("null");
        if (PointerIsValid(attribute_value)) {
            repeat_interval = attribute_value;
        }
        update_pg_job(job_name, Anum_pg_job_interval, repeat_interval);
        return true;
    }
    return false;
}

/*
 * @brief set_job_inline_program_attribute
 *  Set job inline program attributes.
 * @param job_name
 * @param attribute_name
 * @param attribute_value
 * @param attribute_name_str
 */
static bool set_job_inline_program_attribute(const Datum job_name, const Datum attribute_name,
                                             const Datum attribute_value, char *attribute_name_str)
{
    /* Get original program values */
    Datum type = (Datum)0;
    Datum action = (Datum)0;
    Datum num_of_args = (Datum)0;
    Datum enabled = (Datum)0;
    Datum program_name = get_attribute_value(job_name, "program_name", AccessShareLock);
    get_program_info(program_name, &type, &action, &num_of_args, &enabled);

    /* Update number of argument */
    if (pg_strcasecmp(attribute_name_str, "number_of_arguments") == 0) {
        check_program_type_argument(type, TextToInt32(attribute_value));
        update_attribute(program_name, attribute_name, attribute_value);
        return true;
    }

    /*
     * Update inline program action. We need to update both job action and program_action.
     * Which means we need to update both pg_job_proc and gs_job_attribute.
     */
    if (pg_strcasecmp(attribute_name_str, "job_action") == 0) {
        check_program_action(attribute_value);
        dbe_update_pg_job_proc(attribute_value, job_name);
        update_attribute(program_name, CStringGetTextDatum("program_action"), attribute_value);
        return true;
    }

    /* Update job type, check valid && if need argument */
    if (pg_strcasecmp(attribute_name_str, "job_type") == 0) {
        check_program_type_valid(attribute_value);
        check_program_type_argument(attribute_value, TextToInt32(num_of_args));
        char *job_type_str = TextDatumGetCString(attribute_value);
        if (pg_strcasecmp(job_type_str, EXTERNAL_JOB_TYPE) == 0) {
            check_privilege(get_role_name_str(), CREATE_EXTERNAL_JOB_PRIVILEGE);
        }
        pfree_ext(job_type_str);
        update_attribute(program_name, CStringGetTextDatum("program_type"), attribute_value);
        return true;
    }
    return false;
}

/*
 * @brief set_job_associated_object_attribute
 *  Set all associated job object attributes.
 * @param job_name
 * @param attribute_name
 * @param attribute_value
 * @param attribute_name_str
 */
static bool set_job_associated_object_attribute(const Datum job_name, const Datum attribute_name,
                                                const Datum attribute_value, char *attribute_name_str)
{
    if (pg_strcasecmp(attribute_name_str, "schedule_name") == 0) {
        update_attribute(job_name, attribute_name, attribute_value);
        Datum start_date;
        Datum repeat_interval;
        Datum end_date;
        get_schedule_info(attribute_value, &start_date, &repeat_interval, &end_date);
        start_date = DirectFunctionCall1(timestamptz_timestamp, TextToTimeStampTz(start_date));
        end_date = DirectFunctionCall1(timestamptz_timestamp, TextToTimeStampTz(end_date));
        Datum job_related_attr[] = {start_date, repeat_interval, end_date};
        int job_related_num[] = {Anum_pg_job_start_date, Anum_pg_job_interval, Anum_pg_job_end_date};
        bool isnull[] = {false, false, false};
        update_pg_job_multi_columns(job_name, (int *)job_related_num, job_related_attr, isnull,
                                    lengthof(job_related_attr));
        return true;
    }
    if (pg_strcasecmp(attribute_name_str, "program_name") == 0) {
        update_attribute(job_name, attribute_name, attribute_value);
        drop_inline_program(job_name);
        Datum job_type;
        Datum job_action;
        Datum num_of_args;
        Datum enabled;
        get_program_info(attribute_value, &job_type, &job_action, &num_of_args, &enabled);
        char *job_type_str = TextDatumGetCString(job_type);
        if (pg_strcasecmp(job_type_str, EXTERNAL_JOB_TYPE) == 0) {
            check_privilege(get_role_name_str(), CREATE_EXTERNAL_JOB_PRIVILEGE);
        }
        pfree_ext(job_type_str);
        dbe_update_pg_job_proc(job_action, job_name);
        delete_from_argument(job_name);
        return true;
    }
    if (pg_strcasecmp(attribute_name_str, "job_class") == 0) {
        check_job_class_valid(attribute_value);
        update_attribute(job_name, attribute_name, attribute_value);
        return true;
    }
    return false;
}

/*
 * @brief set_job_attribute
 *  Set job attribute, given its attribute name, choose the update method.
 * @param job_name
 * @param attribute_name
 * @param attribute_value
 */
void set_job_attribute(const Datum job_name, const Datum attribute_name, const Datum attribute_value)
{
    char *attribute_name_str = TextDatumGetCString(attribute_name);
    if (set_job_inline_schedule_attribute(job_name, attribute_name, attribute_value, attribute_name_str) ||
        set_job_inline_program_attribute(job_name, attribute_name, attribute_value, attribute_name_str) ||
        set_job_associated_object_attribute(job_name, attribute_name, attribute_value, attribute_name_str)) {
        pfree_ext(attribute_name_str);
        return;
    }

    /* Other attributes */
    if (pg_strcasecmp(attribute_name_str, "enabled") == 0) {
        Assert(attribute_value != 0);
        Datum enabled = TextToBool(attribute_value);
        update_pg_job(job_name, Anum_pg_job_enable, enabled);
        return;
    }
    pfree_ext(attribute_name_str);
    update_attribute(job_name, attribute_name, attribute_value);
}

/*
 * @brief set_attribute_with_related_rel
 *  Set attributes and update their related objects.
 * @param object_name
 * @param attribute_name
 * @param attribute_value
 */
void set_attribute_with_related_rel(const Datum object_name, const Datum attribute_name, const Datum attribute_value)
{
    char *object_type = get_attribute_value_str(object_name, "object_type", RowExclusiveLock, false, false);
    if (pg_strcasecmp(object_type, "program") == 0) {
        set_program_attribute(object_name, attribute_name, attribute_value);
    } else if (pg_strcasecmp(object_type, "schedule") == 0) {
        set_schedule_attribute(object_name, attribute_name, attribute_value);
    } else if (pg_strcasecmp(object_type, "job") == 0) {
        set_job_attribute(object_name, attribute_name, attribute_value);
    } else if (pg_strcasecmp(object_type, "job_class") == 0) {
        set_job_class_attribute(object_name, attribute_name, attribute_value);
    } else {
        ereport(ERROR, (errmodule(MOD_JOB), errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("object_type %s not support.", object_type), errdetail("N/A"),
                        errcause("object_type is not exist"), erraction("Please check check object_type")));
    }
    pfree(object_type);
}

/*
 * @brief set_attribute_1_internal
 *  Set attribute with only one value.
 * @param type      set_attribute supported types.
 */
void set_attribute_1_internal(PG_FUNCTION_ARGS, Oid type)
{
    Datum object_name = PG_GETARG_DATUM(0);
    check_object_is_visible(object_name, false);
    Datum attribute_name = PG_GETARG_DATUM(1);
    Datum attribute_value;
    switch (type) {
        case BOOLOID:
            attribute_value = BoolToText(PG_GETARG_DATUM(2));
            break;
        case TIMESTAMPTZOID:
            attribute_value = TimeStampTzToText(PG_GETARG_DATUM(2));
            break;
        case TIMESTAMPOID:
            attribute_value = TimeStampTzToText(DirectFunctionCall1(timestamp_timestamptz, PG_GETARG_DATUM(2)));
            break;
        case DATEOID:
            attribute_value = DateToText(PG_GETARG_DATUM(2));
            break;
        case INTERVALOID:
            attribute_value = IntervalToText(PG_GETARG_DATUM(2));
            break;
        default:
            attribute_value = (Datum)0;
            ereport(ERROR, (errmodule(MOD_JOB), errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                            errmsg("value type %d not support.", type), errdetail("N/A"),
                            errcause("value type is not exist"), erraction("Please check check value type")));
    }
    set_attribute_with_related_rel(object_name, attribute_name, attribute_value);
}

/*
 * @brief prepare_set_attribute
 *  Prepare set_attribute_5 stuff.
 * @param attribute_name    attribute entry name
 * @param name              first attribute name
 * @param value             first attribute value
 * @param extra_name        secondary attribute name for 'event_spec'
 * @param extra_value       secondary attribute value if exists
 */
void prepare_set_attribute(Datum attribute, Datum *name, Datum *value, Datum *extra_name, Datum extra_value)
{
    /* Cannot change owner. */
    char *attribute_str = TextDatumGetCString(attribute);
    if (pg_strcasecmp(attribute_str, "owner") == 0) {
        ereport(ERROR, (errmodule(MOD_JOB), errcode(ERRCODE_INVALID_STATUS),
                        errmsg("Changing the object owner is not allowed."),
                        errdetail("N/A"), errcause("Forbidden operation."),
                        erraction("Please contact database administrator.")));
    }

    /* Only 'event_spec' requires extra attribute entries. */
    if (pg_strcasecmp(attribute_str, "event_spec") == 0) {
        *name = CStringGetTextDatum("event_condition");
        *extra_name = CStringGetTextDatum("queue_spec");
    } else {
        if (extra_value != Datum(0)) {
            ereport(ERROR, (errmodule(MOD_JOB), errcode(ERRCODE_UNDEFINED_OBJECT),
                            errmsg("Can not recognize attribute name %s.", attribute_str),
                            errdetail("Attribute %s does not exists.", attribute_str),
                            errcause("Attribute name is invalid."),
                            erraction("Please check attribute name.")));
        }
        *name = attribute;
        *extra_name = Datum(0);
    }

    /* Lower attribute values */
    if (pg_strcasecmp(attribute_str, "program_action") != 0 && pg_strcasecmp(attribute_str, "job_action") != 0) {
        *value = DirectFunctionCall1Coll(lower, DEFAULT_COLLATION_OID, *value);
    }
    pfree_ext(attribute_str);
}

/*
 * @brief set_attribute_2_internal
 *  Set object attribute with two values.
 */
void set_attribute_2_internal(PG_FUNCTION_ARGS)
{
    const Datum object_name = PG_GETARG_DATUM(0);
    check_object_is_visible(object_name, false);
    const Datum attribute_name = PG_GETARG_DATUM(1);
    Datum attribute_value1 = PG_ARGISNULL(2) ? Datum(0) : PG_GETARG_DATUM(2);
    Datum attribute_value2 = PG_ARGISNULL(3) ? Datum(0) : PG_GETARG_DATUM(3);
    Datum attribute_name1;
    Datum attribute_name2;

    if (PG_ARGISNULL(2)) { /* attribute value cannot be NULL */
        ereport(ERROR, (errmodule(MOD_JOB), errcode(ERRCODE_INVALID_STATUS),
                        errmsg("Fail to set attribute."),
                        errdetail("Attribute value can't be NULL"), errcause("Forbidden operation."),
                        erraction("Please pass the correct value.")));
    }

    prepare_set_attribute(attribute_name, &attribute_name1, &attribute_value1, &attribute_name2, attribute_value2);
    set_attribute_with_related_rel(object_name, attribute_name1, attribute_value1);
    if (PointerIsValid(attribute_value2)) {
        set_attribute_with_related_rel(object_name, attribute_name2, attribute_value2);
    }
}

/*
 * @brief enable_job_by_job_class
 *  Enable/Disable objects of a entire job class.
 * @param job_class_name
 * @param enable_value          Datum (boolean) enable/disable
 * @param force
 */
static void enable_job_by_job_class(const Datum job_class_name, Datum enable_value, bool force)
{
    Relation gs_job_attribute_rel = heap_open(GsJobAttributeRelationId, RowExclusiveLock);
    Datum job_class_attribute_name = CStringGetTextDatum("job_class");
    List *tuples = search_related_attribute(gs_job_attribute_rel, job_class_attribute_name, job_class_name);
    ListCell *lc = NULL;
    foreach(lc, tuples) {
        HeapTuple tuple = (HeapTuple)lfirst(lc);
        bool isnull = false;
        Datum job_name = heap_getattr(tuple, Anum_gs_job_attribute_job_name, gs_job_attribute_rel->rd_att, &isnull);
        if (!force) {
            can_disable_job(job_name);
        }
        update_pg_job(job_name, Anum_pg_job_enable, enable_value);
    }
    heap_close(gs_job_attribute_rel, NoLock);
    list_free_deep(tuples);
}

/*
 * @brief check_enable_program_privilege
 *  check if a program can be enabled.
 * @param rel 
 * @param program_name 
 */
static void check_enable_program_privilege(Relation rel, Datum program_name)
{
    bool isnull = false;
    Datum program_type = lookup_job_attribute(rel, program_name, CStringGetTextDatum("program_type"), &isnull, false);
    char *job_type_str = TextDatumGetCString(program_type);
    bool is_shell_job = (pg_strcasecmp(job_type_str, EXTERNAL_JOB_TYPE) == 0);
    pfree_ext(job_type_str);
    char *cur_user = get_role_name_str();
    if (is_shell_job) {
        check_privilege(cur_user, RUN_EXTERNAL_JOB_PRIVILEGE);
    }
    check_object_is_visible(program_name, false);
    pfree_ext(cur_user);
}

/*
 * @brief check_if_arguments_defined
 */
void check_if_arguments_defined(Datum program_name, int number_of_arguments)
{
    if (is_inlined_program(program_name)) {
        return;
    }

    HeapTuple argument_tuple = NULL;
    for (int i = 1; i <= number_of_arguments; i++) {
        argument_tuple = SearchSysCache2(JOBARGUMENTPOSITION, program_name, Int32GetDatum(i));
        if (!HeapTupleIsValid(argument_tuple)) {
            ereport(ERROR, (errmodule(MOD_JOB), errcode(ERRCODE_UNDEFINED_OBJECT),
                        errmsg("Can not find argument info of object \'%s\'.", TextDatumGetCString(program_name)),
                        errdetail("N/A"), errcause("argument information not found"),
                        erraction("Please check object name")));
        }
        ReleaseSysCache(argument_tuple);
    }
}

/*
 * @brief enable_program
 * @param program_name
 * @param enable_value
 */
void enable_program(Datum program_name, Datum enable_value)
{
    /* check arguments */
    bool isnull = true;
    Relation rel = heap_open(GsJobAttributeRelationId, AccessShareLock);
    check_enable_program_privilege(rel, program_name);
    Datum number_of_arguments = lookup_job_attribute(rel, program_name, CStringGetTextDatum("number_of_arguments"),
                                                     &isnull, false);
    int32_t max_number = DatumGetInt32(TextToInt32(number_of_arguments));
    heap_close(rel, NoLock);

    /* validate enable input */
    char *enable_str = TextDatumGetCString(enable_value);
    if (pg_strcasecmp(enable_str, "true") != 0 && pg_strcasecmp(enable_str, "false") != 0) {
        ereport(ERROR, (errmodule(MOD_JOB), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                            errmsg("enable value must be true or false."), errdetail("N/A"),
                            errcause("enable value must be true or false."),
                            erraction("Please entry true or false for enable param")));
    }
    if (pg_strcasecmp(enable_str, "true") == 0) {
        check_if_arguments_defined(program_name, max_number);
    }
    pfree_ext(enable_str);
    update_attribute(program_name, CStringGetTextDatum("enabled"), enable_value);
}

/*
 * @brief enable_single_force
 *  Enable/Disable one object.
 * @param object_name       A comma-seperated list of objects
 * @param enable_value      enable/disable
 * @param force             if TRUE, objects are disabled even if other objects depend on them
 */
void enable_single_force(Datum object_name, Datum enable_value, bool force)
{
    char *object_type = get_attribute_value_str(object_name, "object_type", RowExclusiveLock, false, false);
    if (!force && pg_strcasecmp(object_type, "job") == 0) {
        can_disable_job(object_name);
    }
    if (pg_strcasecmp(object_type, "job") == 0) {
        update_pg_job(object_name, Anum_pg_job_enable, TextToBool(enable_value));
    } else if (pg_strcasecmp(object_type, "job_class") == 0) {
        enable_job_by_job_class(object_name, TextToBool(enable_value), force);
    } else if (pg_strcasecmp(object_type, "program") == 0) {
        enable_program(object_name, enable_value);
    }
}

/*
 * @brief enable_single_internal
 *  Enable an object.
 */
void enable_single_internal(PG_FUNCTION_ARGS)
{
    /*
     * If a job was disabled and you enable it, the Scheduler begins to automatically run the job according to its
     * schedule. Enabling a disabled job also resets the job RUN_COUNT, FAILURE_COUNT and RETRY_COUNT columns in the
     * *_SCHEDULER_JOBS data dictionary views.
     */
    Datum object_name = PG_GETARG_DATUM(0);
    check_object_is_visible(object_name, false);
    Datum enable_value = BoolToText(true);
    enable_single_force(object_name, enable_value, true);
}

/*
 * @brief disable_single_internal
 *  Disable one object.
 */
void disable_single_internal(PG_FUNCTION_ARGS)
{
    Datum object_name = PG_GETARG_DATUM(0);
    check_object_is_visible(object_name, false);
    bool force = PG_GETARG_BOOL(1);
    Datum enable_value = BoolToText(false);
    enable_single_force(object_name, enable_value, force);
}