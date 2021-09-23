/* -------------------------------------------------------------------------
 *
 * pgxc_group.h
 *	  definition of the system "PGXC group" relation (pgxc_group)
 *
 *
 * Portions Copyright (c) 1996-2011, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 * src/include/catalog/pgxc_group.h
 *
 * NOTES
 *	  the genbki.pl script reads this file and generates .bki
 *	  information from the DATA() statements.
 *
 * -------------------------------------------------------------------------
 */
#ifndef PGXC_GROUP_H
#define PGXC_GROUP_H

#include "nodes/parsenodes.h"
#include "pgxc/groupmgr.h"

#define PgxcGroupRelationId          9014
#define PgxcGroupRelation_Rowtype_Id 11650

CATALOG(pgxc_group,9014) BKI_SHARED_RELATION BKI_SCHEMA_MACRO
{
    NameData          group_name;                  /* Group name */
    char              in_redistribution;           /* flag to determine if node group is under re-dsitribution */

#ifdef CATALOG_VARLEN	
    /* VARIABLE LENGTH FIELDS: */
    oidvector_extend  group_members;               /* Group members */
    text              group_buckets;               /* hash bucket location -- which node */
#endif
    bool              is_installation;             /* flag to indicate if this nodegroup is installation nodegroup */
#ifdef CATALOG_VARLEN
    aclitem           group_acl[1];                /* access permissions */
#endif
    char              group_kind;                  /* Node group kind: 'v' ,'i','e','n' */
    Oid               group_parent;                /* Node group parent OID */
} FormData_pgxc_group;

typedef FormData_pgxc_group *Form_pgxc_group;

#define Natts_pgxc_group                                8

#define Anum_pgxc_group_name                            1
#define Anum_pgxc_group_in_redistribution               2
#define Anum_pgxc_group_members                         3
#define Anum_pgxc_group_buckets                         4
#define Anum_pgxc_group_is_installation                 5
#define Anum_pgxc_group_group_acl                       6
#define Anum_pgxc_group_kind                            7
#define Anum_pgxc_group_parent                          8


#define PGXC_REDISTRIBUTION_SRC_GROUP                  'y'
#define PGXC_REDISTRIBUTION_DST_GROUP                  't'
#define PGXC_NON_REDISTRIBUTION_GROUP                  'n'

#define PGXC_GROUPKIND_INSTALLATION                    'i'
#define PGXC_GROUPKIND_NODEGROUP                       'n'
#define PGXC_GROUPKIND_LCGROUP                         'v'
#define PGXC_GROUPKING_ELASTIC                         'e'

#endif   /* PGXC_GROUP_H */

