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
 * pg_app_workloadgroup_mapping.h
 *        define the mapping between application and workload group.
 * 
 * 
 * IDENTIFICATION
 *        src/include/catalog/pg_app_workloadgroup_mapping.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef PG_APP_WORKLOADGROUP_MAPPING_H
#define 	PG_APP_WORKLOADGROUP_MAPPING_H

#include "catalog/genbki.h"
#include "catalog/pg_workload_group.h"


/* define the OID of the table pg_app_workloadgroup_mapping*/
#define AppWorkloadGroupMappingRelationId	3464
#define AppWorkloadGroupMappingRelation_Rowtype_Id	 3468

CATALOG(pg_app_workloadgroup_mapping,3464) BKI_SHARED_RELATION BKI_ROWTYPE_OID(3468) BKI_SCHEMA_MACRO
{
	NameData appname;						/*Name of application*/
	NameData workload_gpname;				/*Name of workload group*/
} FormData_pg_app_workloadgroup_mapping;


/*-------------------------------------------------------------------------
 *		Form_pg_app_workloadgroup_mapping corresponds to a pointer to a tuple with
 *		the format of pg_app_workloadgroup_mapping relation.
 *-------------------------------------------------------------------------
 */
typedef FormData_pg_app_workloadgroup_mapping *Form_pg_app_workloadgroup_mapping;

/*-------------------------------------------------------------------------
 *		compiler constants for pg_app_workloadgroup_mapping
 *-------------------------------------------------------------------------
 */
#define Natts_pg_app_workloadgroup_mapping						2
#define Anum_pg_app_workloadgroup_mapping_appname  			1
#define Anum_pg_app_workloadgroup_mapping_wgname			2

DATA(insert OID = 10 ("default_application" "default_group"));	
#define DEFAULT_APP_OID 10
#define DEFAULT_APP_NAME "default_application"

/*default setting for user defined application*/
#define DEFAULT_WORKLOAD_GROUP DEFAULT_GROUP_NAME
#define DEFAULT_WORKLOAD_GROUP_OID DEFAULT_GROUP_OID

#endif   /* PG_APP_WORKLOADGROUP_MAPPING_H */

