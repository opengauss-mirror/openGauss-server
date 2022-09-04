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
 * pg_workload_group.h
 *        define the system workload group information.
 * 
 * 
 * IDENTIFICATION
 *        src/include/catalog/pg_workload_group.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef PG_WORKLOAD_GROUP_H
#define PG_WORKLOAD_GROUP_H

#include "catalog/genbki.h"
#include "catalog/pg_resource_pool.h"


/* define the OID of the table pg_workload_group */
#define WorkloadGroupRelationId	3451
#define WorkloadGroupRelation_Rowtype_Id	3467

CATALOG(pg_workload_group,3451) BKI_SHARED_RELATION BKI_ROWTYPE_OID(3467) BKI_SCHEMA_MACRO
{
	NameData workload_gpname;   /* Name of worload group */
	Oid respool_oid;            /* The oid of resource pool which the workload group is created under */
	int4 act_statements;        /* The number of active statements */
} FormData_pg_workload_group;


/*-------------------------------------------------------------------------
 *		Form_pg_workload_group corresponds to a pointer to a tuple with
 *		the format of pg_workload_group relation.
 * -------------------------------------------------------------------------
 */
typedef FormData_pg_workload_group *Form_pg_workload_group;

/*-------------------------------------------------------------------------
 *		compiler constants for pg_workload_group
 * -------------------------------------------------------------------------
 */
#define Natts_pg_workload_group						3
#define Anum_pg_workload_group_wgname  			1
#define Anum_pg_workload_group_rgoid				2
#define Anum_pg_workload_group_acstatements		3

DATA(insert OID = 10 ( "default_group" 10 -1));	/* -1 means ulimited */
#define DEFAULT_GROUP_OID 10
#define DEFAULT_GROUP_NAME "default_group"
#define ULIMITED_ACTIVE_STATEMENTS -1

/* default setting for user defined workload group */
#define DEFAULT_RESOURCE_POOL DEFAULT_POOL_OID
#define DEFAULT_ACTIVE_STATEMENTS -1

#endif   /* PG_WORKLOAD_GROUP_H */

