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
 * pg_resource_pool.h
 *        define the system resource pool information.
 * 
 * 
 * IDENTIFICATION
 *        src/include/catalog/pg_resource_pool.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef PG_RESOURCE_POOL_H
#define PG_RESOURCE_POOL_H

#include "catalog/genbki.h"
#include "workload/workload.h"

/* define the OID of the table pg_resource_pool */
#define ResourcePoolRelationId	3450
#define ResourcePoolRelation_Rowtype_Id	3466

CATALOG(pg_resource_pool,3450) BKI_SHARED_RELATION BKI_ROWTYPE_OID(3466) BKI_SCHEMA_MACRO
{
	NameData respool_name;          /* Name of resource pool */
	int4 mem_percent;               /* Memory percentage */
	int8 cpu_affinity;              /* CPU affinity mask */
    NameData control_group;         /* Name of control group */
	int4 active_statements;         /* Active statements */
	int4 max_dop;                   /* Max dop. in redistribution, query_dop on one table whild io_priority is None. */
	NameData memory_limit;          /* Memory limit to use */
	Oid  parentid;                  /* parent resource pool oid */
	int4 io_limits;                 /* iops limit */
	NameData io_priority;           /* percentage of IO resource for DN */
	NameData nodegroup;             /* node group */
	bool is_foreign;				/* flag to indicate the resource pool for foreign users */
    int4 max_worker;                /* in redistribution, create thread num on one table whild io_priority is None. */
} FormData_pg_resource_pool;		


/* -------------------------------------------------------------------------
 *		Form_pg_resource_pool corresponds to a pointer to a tuple with
 *		the format of pg_resource_pool relation.
 *		There is different meaning that "int8" means 8-bits in C codes, while 
 *		it means 64-bits in catalogs.
 * -------------------------------------------------------------------------
 */
typedef struct FormData_pg_resource_pool_real {
	NameData respool_name;           /* Name of resource pool */
	int4     mem_percent;            /* Memory percentage */
	int64    cpu_affinity;           /* CPU affinity mask */
    NameData control_group;          /* Name of control group */
	int4     active_statements;	     /* Active statements */
	int4     max_dop;                /* Max dop. in redistribution, query_dop on one table whild io_priority is None. */
	NameData memory_limit;           /* Memory limit to use */
	Oid      parentid;               /* parent resource pool oid */
	int4     iops_limits;            /* iops limit */
	NameData io_priority;            /* percentage of IO resource for DN */
	NameData nodegroup;              /* node group */
	bool 	 is_foreign;		     /* flag to indicate the resource pool for foreign users */
    int4     max_worker;             /* in redistribution, create thread num on one table whild io_priority is None. */
} FormData_pg_resource_pool_real;

typedef FormData_pg_resource_pool_real *Form_pg_resource_pool;

/* -------------------------------------------------------------------------
 *		compiler constants for pg_resource_pool
 * -------------------------------------------------------------------------
 */
#define Natts_pg_resource_pool                  13
#define Anum_pg_resource_pool_rpname            1
#define Anum_pg_resource_pool_mem_percentage    2
#define Anum_pg_resource_pool_cpu_affinity      3
#define Anum_pg_resource_pool_control_group     4
#define Anum_pg_resource_pool_active_statements 5
#define Anum_pg_resource_pool_max_dop           6
#define Anum_pg_resource_pool_memory_limit      7
#define Anum_pg_resource_pool_parent            8
#define Anum_pg_resource_pool_iops_limits       9
#define Anum_pg_resource_pool_io_priority       10
#define Anum_pg_resource_pool_nodegroup			11
#define Anum_pg_resource_pool_is_foreign		12
#define Anum_pg_resource_pool_max_worker        13

/* -1 means 0xffffffffffffffff */
DATA(insert OID = 10 ("default_pool" 100 -1 "DefaultClass:Medium" -1 1 "8GB" 0 0 "None" "installation" f _null_));

#define DEFAULT_POOL_OID                    10
#define DEFAULT_POOL_NAME                   "default_pool"
#define INVALID_POOL_NAME                   "invalid_pool"
#define ULIMITED_MEMORY_PERCENTAGE          100
#define ULIMITED_CPU_AFFINITY               0xffffffffffffffffULL
#define ULIMITED_ACT_STATEMENTS             -1
#define DEFAULT_CONTROL_GROUP               "DefaultClass:Medium"
#define DEFAULT_DOP                          1
#define DEFAULT_MAX_DOP                      64
#define DEFAULT_MEMORY_LIMIT                "8GB"
#define DEFAULT_IOPS_LIMITS                  0
#define DEFAULT_IO_PRIORITY                 "None"
#define DEFAULT_NODE_GROUP                  "installation" // the same as CNG_OPTION_INSTALLATION
#define DEFAULT_WORKER                       1
#define DEFAULT_MAX_WORKER                   8

/* default setting for user defined resource pool */
#define DEFAULT_MEMORY_PERCENTAGE           0
#define DEFAULT_MULTI_TENANT_MEMPCT         20
#define DEFAULT_ACT_STATEMENTS              10

#endif   /* PG_RESOURCE_POOL_H */

