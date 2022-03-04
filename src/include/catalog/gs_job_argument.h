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
 * gs_job_argument.h
 *        definition of the system "gs_job_argument" relation (gs_job_argument)
 *        this relation has nothing to do with other relations but store job/program arguments
 *        for them.
 * 
 * IDENTIFICATION
 *        src/include/catalog/gs_job_argument.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef GS_JOB_ARGUMENT_H
#define GS_JOB_ARGUMENT_H

#include "postgres.h"
#include "knl/knl_variable.h"
#include "catalog/genbki.h"
#include "fmgr.h"
#include "utils/date.h"

/* -------------------------------------------------------------------------
 *		gs_job_argument definition.  cpp turns this into
 *		typedef struct FormData_gs_job_argument
 * -------------------------------------------------------------------------
 */
#define GsJobArgumentRelationId 9036
#define GsJobArgumentRelation_Rowtype_Id 9036

CATALOG(gs_job_argument,9036) BKI_SCHEMA_MACRO
{
    int4            argument_position;          /* position and job_name together are unique */
    NameData        argument_type;
#ifdef CATALOG_VARLEN			/* variable-length fields start here */
    text            job_name;	                /* Identifier of job. */
    text            argument_name;              /* name and job_name together are unique */
    text            argument_value;
    text            default_value;
#endif
} FormData_gs_job_argument;

/* -------------------------------------------------------------------------
 *		Form_pg_job corresponds to a pointer to a tuple with
 *		the format of pg_job relation.
 * -------------------------------------------------------------------------
 */
typedef FormData_gs_job_argument* Form_gs_job_argument;

/* -------------------------------------------------------------------------
 *		compiler constants for pg_job
 * -------------------------------------------------------------------------
 */
#define Natts_gs_job_argument                               6
#define Anum_gs_job_argument_argument_position              1
#define Anum_gs_job_argument_argument_type                  2
#define Anum_gs_job_argument_job_name                       3
#define Anum_gs_job_argument_argument_name                  4
#define Anum_gs_job_argument_argument_value                 5
#define Anum_gs_job_argument_default_value                  6


struct ScanKeyInfo {
    Datum attribute_value;
    AttrNumber attribute_number;
    RegProcedure procedure;
};

List *search_by_sysscan_1(Relation rel, ScanKeyInfo *scan_key_info);

List *search_by_sysscan_2(Relation rel, ScanKeyInfo *scan_key_info1, ScanKeyInfo *scan_key_info2);

#endif /* GS_JOB_ARGUMENT_H */