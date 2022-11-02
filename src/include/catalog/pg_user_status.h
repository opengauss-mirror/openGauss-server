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
 * pg_user_status.h
 *        definition of the system "user status" relation (pg_user_status)
 *        along with the relation's initial contents.
 * 
 * 
 * IDENTIFICATION
 *        src/include/catalog/pg_user_status.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef PG_USER_STATUS_H
#define PG_USER_STATUS_H

#include "catalog/genbki.h"


/* define the timestamptz for the locktime in pg_user_status */
#define timestamptz Datum

/* define the OID of the table pg_user_status */
#define UserStatusRelationId	3460
#define UserStatusRelation_Rowtype_Id	3463

CATALOG(pg_user_status,3460) BKI_SHARED_RELATION BKI_ROWTYPE_OID(3463) BKI_SCHEMA_MACRO
{
	Oid roloid;             /* role OID */
	int4 failcount;         /* failed num of login attampts */
	timestamptz locktime;   /* role lock time */
	int2 rolstatus;         /* role status */
    int8 permspace;             /* perm space */
	int8 tempspace;         /* temp space */
    int2 passwordexpired;   /* password expired status */
} FormData_pg_user_status;

#undef timestamptz

/* -------------------------------------------------------------------------
 *		Form_pg_user_status corresponds to a pointer to a tuple with
 *		the format of pg_user_status relation.
 * -------------------------------------------------------------------------
 */
typedef FormData_pg_user_status *Form_pg_user_status;

/* -------------------------------------------------------------------------
 *		compiler constants for pg_user_status
 * -------------------------------------------------------------------------
 */
#define Natts_pg_user_status                    7
#define Anum_pg_user_status_roloid              1
#define Anum_pg_user_status_failcount           2
#define Anum_pg_user_status_locktime            3
#define Anum_pg_user_status_rolstatus           4
#define Anum_pg_user_status_permspace           5
#define Anum_pg_user_status_tempspace           6
#define Anum_pg_user_status_passwordexpired     7

#endif   /* PG_USER_STATUS_H */

