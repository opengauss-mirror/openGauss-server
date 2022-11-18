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
 * pg_auth_history.h
 *        definition of the system "authorization history" relation (pg_auth_history)
 *        along with the relation's initial contents.
 * 
 * 
 * IDENTIFICATION
 *        src/include/catalog/pg_auth_history.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef PG_AUTH_HISTORY_H
#define PG_AUTH_HISTORY_H

/* define the timestamptz for the passwordtime in pg_auth_history*/
#define timestamptz Datum

/* define the OID of the table pg_auth_history*/
#define AuthHistoryRelationId	3457
#define AuthHistoryRelation_Rowtype_Id 11642

CATALOG(pg_auth_history,3457) BKI_SHARED_RELATION BKI_SCHEMA_MACRO
{
	Oid roloid;		    			/* role OID*/
	timestamptz passwordtime;   	/* the time while create or alter password */
	text rolpassword;				/* role password, md5-encryption, sha256-encryption */
} FormData_pg_auth_history;

#undef timestamptz

/*-------------------------------------------------------------------------
 *		Form_pg_auth_history corresponds to a pointer to a tuple with
 *		the format of pg_auth_history relation.
 *-------------------------------------------------------------------------
 */
typedef FormData_pg_auth_history *Form_pg_auth_history;

/*-------------------------------------------------------------------------
 *		compiler constants for pg_auth_history
 *-------------------------------------------------------------------------
 */
#define Natts_pg_auth_history						3
#define Anum_pg_auth_history_roloid				1
#define Anum_pg_auth_history_passwordtime  		2
#define Anum_pg_auth_history_rolpassword		3

#endif   /* PG_AUTH_HISTORY_H */
