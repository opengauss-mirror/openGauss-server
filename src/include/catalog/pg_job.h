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
 * pg_job.h
 *        definition of the system "job" relation (pg_job)
 *        along with the relation's initial contents.
 * 
 * 
 * IDENTIFICATION
 *        src/include/catalog/pg_job.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef PG_JOB_H
#define PG_JOB_H

#include "postgres.h"
#include "knl/knl_variable.h"
#include "catalog/genbki.h"
#include "fmgr.h"
#include "utils/date.h"

#ifdef HAVE_INT64_TIMESTAMP
#define timestamp int64
#else
#define timestamp double
#endif

#define int8 int64

/* -------------------------------------------------------------------------
 *		pg_job definition.  cpp turns this into
 *		typedef struct FormData_pg_job
 * -------------------------------------------------------------------------
 */
#define PgJobRelationId 9022
#define PgJobRelation_Rowtype_Id 11658

CATALOG(pg_job,9022) BKI_SHARED_RELATION BKI_SCHEMA_MACRO
{
	int8		job_id;	                /* Identifier of job. */
	int8		current_postgres_pid;   /* if jobstatus is 'r', this is postgres's pid which is running this job */
	NameData	log_user;               /* Login user when the job was submitted */
	NameData	priv_user;              /* User whose default privileges apply to this job, reserved, unused at present */
	NameData	dbname;                 /* A database name against which to run the script */
	NameData	node_name;              /* Identify the which coordinator the job create and execute */
	char		job_status;             /* Status of job: r=running, s=successfully finished,  f=failed stopping job, 
                                                 * i=ignored failure, d=aborted */
	timestamp	start_date;             /* Date that this job first started executing */
	timestamp	next_run_date;          /* Date that this job will next be executed, this is calculated by 
                                                 * pg_job_schedule.RECURRENCE_EXPR */
                                        /* timestamp when dbe_job, timestamptz when dbe_scheduler */
	int2		failure_count;          /* Number of times the job has started and failed since its last success, 
                                                 * if more than 16, this job will be terminated */
#ifdef CATALOG_VARLEN                           /* Null value constrain */
	text		interval;               /* A date function, evaluated at the start of execution, becomes next NEXT_DATE */
	timestamp	last_start_date;        /* This is when the last execution started. */
	timestamp	last_end_date;          /* This is when the last execution stoped. */
	timestamp	last_suc_date;          /* This is when the last successful execution started. */
	timestamp	this_run_date;          /* Date that this job started executing (usually null if not executing) */
	NameData	nspname;
    text    	job_name;
    timestamp	end_date;
    bool      	enable;
    text        failure_msg;
#endif
} FormData_pg_job;

#undef timestamp
#undef int8

/* -------------------------------------------------------------------------
 *		Form_pg_job corresponds to a pointer to a tuple with
 *		the format of pg_job relation.
 * -------------------------------------------------------------------------
 */
typedef FormData_pg_job* Form_pg_job;

/* -------------------------------------------------------------------------
 *		compiler constants for pg_job
 * -------------------------------------------------------------------------
 */
#define Natts_pg_job						20
#define Anum_pg_job_job_id					1
#define Anum_pg_job_current_postgres_pid	2
#define Anum_pg_job_log_user				3
#define Anum_pg_job_priv_user				4
#define Anum_pg_job_dbname					5
#define Anum_pg_job_node_name				6
#define Anum_pg_job_job_status				7
#define Anum_pg_job_start_date				8
#define Anum_pg_job_next_run_date			9
#define Anum_pg_job_failure_count			10
#define Anum_pg_job_interval				11
#define Anum_pg_job_last_start_date			12
#define Anum_pg_job_last_end_date			13
#define Anum_pg_job_last_suc_date			14
#define Anum_pg_job_this_run_date			15
#define Anum_pg_job_nspname					16
#define Anum_pg_job_job_name				17
#define Anum_pg_job_end_date				18
#define Anum_pg_job_enable					19
#define Anum_pg_job_failure_msg				20




/* Type of pg job command. */
typedef enum
{
	Job_Submit,
	Job_ISubmit,
	Job_ISubmit_Node,
	Job_ISubmit_Node_Internal,
	Job_Remove,
	Job_Update,
	Job_Finish
} Pgjob_Command_Type;

/* Status of pg job. */
typedef enum {
	Pgjob_Run, 
	Pgjob_Succ, 
	Pgjob_Fail,   /* Execute what fail */
} Update_Pgjob_Status;

/* Oid of related for delete pg_job. */
typedef enum {
	DbOid,
	UserOid,
	RelOid
} Delete_Pgjob_Oid;

/* Define job status. */
#define PGJOB_RUN_STATUS    'r'
#define PGJOB_SUCC_STATUS   's'
#define PGJOB_FAIL_STATUS   'f'
#define PGJOB_ABORT_STATUS  'd'

/* job run type */
#define PGJOB_TYPE_ALL_CN   "ALL_CN"
#define PGJOB_TYPE_ALL_DN   "ALL_DN"
#define PGJOB_TYPE_ALL      "ALL_NODE"
#define PGJOB_TYPE_CCN      "CCN"

#define JOBID_MAX_NUMBER  ((uint16)(32767))

extern void update_run_job_to_fail();
extern void remove_job_by_oid(Oid oid, Delete_Pgjob_Oid oidFlag, bool local);
extern void execute_job(int4 job_id);
extern void	get_job_values(int4 job_id, HeapTuple tup, Relation relation, Datum *values, bool *visnull);
extern void RemoveJobById(Oid objectId);
extern void check_job_permission(HeapTuple tuple, bool check_running = true);
extern int jobid_alloc(uint16* pusJobId, int64 job_max_number = JOBID_MAX_NUMBER);
extern void update_pg_job_dbname(Oid jobid, const char* dbname);
extern void update_pg_job_username(Oid jobid, const char* username);
extern char* get_real_search_schema();
extern void check_interval_valid(int4 job_id, Relation rel, Datum interval);
extern void check_job_status(Datum job_status);
extern bool is_scheduler_job_id(Relation relation, int64 job_id);

/* For run_job_internal */
extern void job_finish(PG_FUNCTION_ARGS);
extern Datum job_submit(PG_FUNCTION_ARGS);
extern void create_job_raw(PG_FUNCTION_ARGS);
extern char *create_inline_program(Datum job_name, Datum job_type, Datum job_action, Datum num_of_args, Datum enabled);
extern char *get_inline_schedule_name(Datum job_name);
extern void drop_single_job_internal(PG_FUNCTION_ARGS);
extern void ShowEventCommand(ShowEventStmt *stmt, DestReceiver* dest);
extern TupleDesc GetEventResultDesc();

#define JOBID_ALLOC_OK         0                /* alloc jobid ok */
#define JOBID_ALLOC_ERROR      1                /* alloc jobid error */

#endif /* PG_JOB_H */

