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
 * gs_obsscaninfo.h
 *        definition of the system "obsscaninfo" relation (gs_obsscaninfo)
 *        along with the relation's initial contents.
 * 
 * 
 * IDENTIFICATION
 *        src/include/catalog/gs_obsscaninfo.h
 *
 * NOTES
 *	  the genbki.pl script reads this file and generates .bki
 *	  information from the DATA() statements.
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef GS_OBSSCANINFO_H
#define GS_OBSSCANINFO_H

#include "catalog/genbki.h"

/* ----------------
 *		gs_obsscaninfo definition.cpp turns this into
 *		typedef struct FormData_gs_obsscaninfo
 * ----------------
 */
#define timestamptz Datum
#define GSObsScanInfoRelationId  5680
#define GSObsScanInfoRelation_Rowtype_Id 11661

CATALOG(gs_obsscaninfo,5680) BKI_SHARED_RELATION BKI_WITHOUT_OIDS
{
	/* These fields form the unique key for the entry: */
	int8	    query_id;
#ifdef CATALOG_VARLEN
	text        user_id;
	text        table_name;
	text        file_type;
#endif
	timestamptz	time_stamp;
	float8		actual_time;
	int8		file_scanned;
	float8		data_size;
#ifdef CATALOG_VARLEN
	text		billing_info;
#endif
} FormData_gs_obsscaninfo;

#undef timestamptz

/* ----------------
 *		Form_gs_obsscaninfo corresponds to a pointer to a tuple with
 *		the format of gs_obsscaninfo relation.
 * ----------------
 */
typedef FormData_gs_obsscaninfo *Form_gs_obsscaninfo;

/* ----------------
 *		compiler constants for gs_obsscaninfo
 * ----------------
 */
#define Natts_gs_obsscaninfo                   9
#define Anum_gs_obsscaninfo_query_id           1
#define Anum_gs_obsscaninfo_user_id     	   2
#define Anum_gs_obsscaninfo_table_name         3
#define Anum_gs_obsscaninfo_file_type          4
#define Anum_gs_obsscaninfo_time_stamp         5
#define Anum_gs_obsscaninfo_actual_time        6
#define Anum_gs_obsscaninfo_file_scanned       7
#define Anum_gs_obsscaninfo_data_size          8
#define Anum_gs_obsscaninfo_billing_info       9

#endif   /* GS_OBSSCANINFO_H */
