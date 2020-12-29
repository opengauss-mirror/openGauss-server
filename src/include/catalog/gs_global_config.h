/*-------------------------------------------------------------------------
 *
 * gs_global_config.h
 *       definition of the system global configuration parameters
 *
 *
 * Portions Copyright (c) 1996-2011, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 *
 * IDENTIFICATION 
 *       src/include/catalog/gs_global_config.h
 *
 * NOTES
 *       the genbki.pl script reads this file and generates .bki
 *       information from the DATA() statements.
 *
 *-------------------------------------------------------------------------
 */
#ifndef GS_GLOBAL_CONFIG_H
#define GS_GLOBAL_CONFIG_H

#include "catalog/genbki.h"

#define GsGlobalConfigRelationId  9080
#define GsGlobalConfigRelationId_Rowtype_Id 9081

CATALOG(gs_global_config,9080) BKI_SHARED_RELATION BKI_SCHEMA_MACRO BKI_WITHOUT_OIDS
{
      NameData        name;           /* Configure name */

#ifdef CATALOG_VARLEN
       text    value;          /* Configure value */
#endif
} FormData_gs_global_config;

typedef FormData_gs_global_config *Form_gs_global_config;

#define Natts_gs_global_config       2

#define Anum_gs_global_config_name   1
#define Anum_gs_global_config_value  2

#endif   /* GS_GLOBAL_CONFIG_H */
