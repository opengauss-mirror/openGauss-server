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
 * ---------------------------------------------------------------------------------------
 * 
 * gs_job_attribute.h
 *        definition of the system "gs_job_attribute" relation (gs_job_attribute)
 *        this relation stores additional info for the job presented in pg_job.
 * 
 * IDENTIFICATION
 *        src/include/catalog/gs_job_attribute.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef GS_JOB_ATTRIBUTE_H
#define GS_JOB_ATTRIBUTE_H

#include "postgres.h"
#include "knl/knl_variable.h"
#include "catalog/genbki.h"
#include "fmgr.h"
#include "utils/date.h"
#include "utils/builtins.h"

/* -------------------------------------------------------------------------
 *		gs_job_attribute definition.  cpp turns this into
 *		typedef struct FormData_gs_job_attribute
 * -------------------------------------------------------------------------
 */
#define GsJobAttributeRelationId 9031
#define GsJobAttributeRelation_Rowtype_Id 9031

CATALOG(gs_job_attribute,9031) BKI_SCHEMA_MACRO
{
#ifdef CATALOG_VARLEN			/* variable-length fields start here */
	text		job_name;	                /* Identifier of job. */
    text        attribute_name;             /* name and job_name together are unique */
    text        attribute_value;
#endif
} FormData_gs_job_attribute;

/* -------------------------------------------------------------------------
 *		Form_pg_job corresponds to a pointer to a tuple with
 *		the format of pg_job relation.
 * -------------------------------------------------------------------------
 */
typedef FormData_gs_job_attribute* Form_gs_job_attribute;

/* -------------------------------------------------------------------------
 *		compiler constants for pg_job
 * -------------------------------------------------------------------------
 */
#define Natts_gs_job_attribute                              3
#define Anum_gs_job_attribute_job_name                      1
#define Anum_gs_job_attribute_attribute_name                2
#define Anum_gs_job_attribute_attribute_value               3

#define MAX_JOB_NAME_LEN 1024
#define IsIllegalCharacter(c) ((c) != '/' && !isdigit((c)) && !isalpha((c)) && (c) != '_' && (c) != '-' && \
                               (c) != '.' && (c) != '$')

/* Job in-type */
#define JOB_INTYPE_PLAIN                'n'
#define JOB_INTYPE_SCHEDULE_PROGRAM     'a'
#define JOB_INTYPE_PROGRAM              'p'
#define JOB_INTYPE_SCHEDULE             's'

/*
 * @brief JobArgument
 *  Job program arguments.
 */
typedef struct JobArgument {
    char        *argument_type;
    char        *argument_value;
} JobArgument;

/*
 * @brief JobAttribute
 *  Temporary attribute container.
 */
typedef struct JobAttribute {
    Datum       object_name;
    Datum       name;
    Datum       value;
    bool        null;
} JobAttribute;

/*
 * @brief JobAttributeValue
 *  Data from gs_job_attribute.
 */
typedef struct JobAttributeValue {
    Datum       job_type;
    Datum       program_name;
    Datum       job_class;
    char        *username;
    int         number_of_arguments;
    bool        auto_drop;
    bool        program_enable;
} JobAttributeValue;

/*
 * @brief JobProcValue
 *  Data from pg_job_proc.
 */
typedef struct JobProcValue {
    Datum       action;
    char        *action_str;
} JobProcValue;

/*
 * @brief JobValue
 *  Data from pg_job.
 */
typedef struct JobValue {
    char        *nspname;
    char        *interval;
    Timestamp   end_date;
    int         fail_count;
} JobValue;

/*
 * @brief JobTarget
 *  Execute scheduler job data container.
 */
typedef struct JobTarget {
    Datum               id;
    Datum               job_name;
    JobProcValue        *job_proc_value;
    JobAttributeValue   *job_attribute_value;
    JobValue            *job_value;
    JobArgument         *arguments;
} JobTarget;

/* Proprietary conversion methods */
inline Datum TimeStampTzToText(Datum value)
{
    char *str = DatumGetCString(DirectFunctionCall1(timestamptz_out, value));
    Datum res = CStringGetTextDatum(str);
    pfree_ext(str);
    return res;
}

inline Datum TextToTimeStampTz(Datum value)
{
    if (!PointerIsValid(value)) {
        return GetCurrentTimestamp();
    }
    char *str = TextDatumGetCString(value);
    Datum res = DirectFunctionCall3(timestamptz_in, PointerGetDatum(str), InvalidOid, -1);
    pfree_ext(str);
    return res;
}

inline Datum TextToTimeStamp(Datum value)
{
    if (!PointerIsValid(value)) {
        return DirectFunctionCall1(timestamptz_timestamp, GetCurrentTimestamp());
    }
    short nrgs = 1;
    FunctionCallInfoData fcinfo;
    InitFunctionCallInfoData(fcinfo, NULL, nrgs, InvalidOid, NULL, NULL);
    fcinfo.argnull[0] = false;
    fcinfo.arg[0] = value;
    return text_timestamp(&fcinfo);
}

inline Datum BoolToText(Datum value)
{
    text *res = cstring_to_text(DatumGetBool(value) ? "true" : "false");
    return PointerGetDatum(res);
}

inline Datum TextToBool(Datum value)
{
    char *str = TextDatumGetCString(value);
    if (pg_strcasecmp(str, "true") == 0) {
        pfree_ext(str);
        return BoolGetDatum(1);
    } else {
        Assert(pg_strcasecmp(str, "false") == 0);
        pfree_ext(str);
        return BoolGetDatum(0);
    }
}

inline Datum Int32ToText(int32 value)
{
    char *str = (char *)palloc0(12);
    pg_ltoa(value, str);
    Datum res = CStringGetTextDatum(str);
    pfree_ext(str);
    return res;
}

inline Datum TextToInt32(Datum value)
{
    char *str = TextDatumGetCString(value);
    int res = pg_strtoint32(str);
    pfree_ext(str);
    return Int32GetDatum(res);
}

inline Datum IntervalToText(Datum value)
{
    if (!PointerIsValid(value)) {
        return Datum(0);
    }
    char *str = DatumGetCString(DirectFunctionCall1(interval_out, value));
    Datum res = CStringGetTextDatum(str);
    pfree_ext(str);
    return res;
}

inline Datum DateToText(Datum value)
{
    char *str = DatumGetCString(DirectFunctionCall1(date_out, value));
    Datum res = DirectFunctionCall1(textin, CStringGetDatum(str));
    pfree_ext(str);
    return res;
}

/* Check functions */
extern void check_object_is_visible(Datum object_name, bool is_readonly = true);
extern void check_object_type_matched(Datum object_name, const char *object_type_str);
extern void check_authorization_valid(Datum privilege);
extern void check_program_type_argument(Datum program_type, int number_of_arguments);
extern void check_program_action(Datum action);
extern void check_if_arguments_defined(Datum program_name, int number_of_arguments);
extern void check_privilege(char *username, const char *authorization);
extern bool is_private_scheduler_object(Relation rel, Datum object_name, const char *user_str);
extern bool is_internal_scheduler_object(Relation rel, Datum object_name);
extern bool is_job_running(Datum job_name);
extern bool is_inlined_program(Datum program_name);
extern bool is_inlined_schedule(Datum schedule_name);

/* Delete functions */
extern void delete_from_attribute(const Datum object_name);
extern void delete_from_argument(const Datum object_name);
extern void delete_from_job(const Datum job_name);
extern void delete_from_job_proc(const Datum object_name);
extern void delete_deprecated_program_arguments(Datum program_name, bool delete_self = true);
extern void disable_shared_job_from_owner(const char *user_str);
extern void disable_external_job_from_owner(const char *user_str);
extern void remove_scheduler_objects_from_owner(const char *user_str);
extern void drop_inline_program(Datum job_name);
extern void update_attribute(Datum object_name, Datum attribute_name, Datum attribute_value);
extern void update_pg_job(Datum job_name, int attribute_number, Datum attribute_value, bool isnull = false);
extern void update_pg_job_multi_columns(Datum job_name, const int *attribute_numbers, const Datum *attribute_values,
                                        const bool *isnull, int n);

/* Lookup functions */
extern Datum get_scheduler_max_timestamp();
extern Datum get_scheduler_max_timestamptz();
extern int get_program_number_of_arguments(Datum program_name, bool miss_ok = false);
extern Datum get_attribute_value(Datum object, const char *attribute, LOCKMODE lockmode, bool miss_ok = false,
                                 bool can_be_null = true);
extern char *get_attribute_value_str(Datum object, const char *attribute, LOCKMODE lockmode, bool miss_ok = false,
                                     bool can_be_null = true);
extern char *get_job_type(Datum job_name, bool miss_ok = false);
extern char *get_role_name_str(Oid oid = InvalidOid);
extern Datum get_role_datum(Datum name = (Datum)0);
extern char *get_raw_role_str(Datum name);
extern Datum get_default_argument_type();
extern Datum lookup_job_attribute(Relation rel, Datum job_name, Datum attribute, bool *isnull, bool miss_ok = false);
extern void batch_lookup_job_attribute(JobAttribute *attributes, int n);
extern void batch_lookup_program_argument(Datum job_name, JobArgument *arguments, int num_of_args);
extern List *search_related_attribute(Relation gs_job_attribute_rel, Datum attribute_name, Datum attribute_value);
extern HeapTuple search_from_pg_job(Relation pg_job_rel, Datum job_name);
extern int4 get_job_id_from_pg_job(Datum job_name);
HeapTuple search_from_pg_job_proc_no_exception(Relation rel, Datum job_name);

/* Tools */
extern void enable_single_force(Datum object_name, Datum enable_value, bool force);
extern void enable_program(Datum program_name, Datum enable_value);
extern void set_job_attribute(const Datum job_name, const Datum attribute_name, const Datum attribute_value);
extern bool execute_backend_scheduler_job(Datum job_name, StringInfoData *buf);

/* prefix for inlined object */
#define INLINE_JOB_SCHEDULE_PREFIX "inline_schedule_"
#define INLINE_JOB_PROGRAM_PREFIX "inline_program_"

/* job types */
#define EXTERNAL_JOB_TYPE "EXTERNAL_SCRIPT"
#define PLSQL_JOB_TYPE "PLSQL_BLOCK"
#define PRECEDURE_JOB_TYPE "STORED_PROCEDURE"

/* job privilege */
#define EXECUTE_ANY_PROGRAM_PRIVILEGE "execute any program"
#define CREATE_JOB_PRIVILEGE "create job"
#define CREATE_EXTERNAL_JOB_PRIVILEGE "create external job"
#define RUN_EXTERNAL_JOB_PRIVILEGE "run external job"

/* default credential */
#define DEFAULT_CREDENTIAL_NAME "db_credential"

#endif /* GS_JOB_ATTRIBUTE_H */