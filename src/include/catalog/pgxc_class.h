/* -----------------------------------------------------------
 *
 * Copyright (c) 2010-2013 Postgres-XC Development Group
 *
 * -----------------------------------------------------------
 */
#ifndef PGXC_CLASS_H
#define PGXC_CLASS_H

#include "nodes/parsenodes.h"

#define PgxcClassRelationId  9001
#define PgxcClassRelation_Rowtype_Id 11648

CATALOG(pgxc_class,9001) BKI_WITHOUT_OIDS BKI_SCHEMA_MACRO
{
	Oid		pcrelid;           /* Table Oid */
	char		pclocatortype;     /* Type of distribution */
	int2 		pchashalgorithm;   /* Hashing algorithm */
	int2 		pchashbuckets;     /* Number of buckets */
        NameData        pgroup;            /* Group name */
        char            redistributed;     /* relation is re-distributed */
	int4            redis_order;       /* data redistribution order */

	int2vector		pcattnum;  /* Column number of distribution */

#ifdef CATALOG_VARLEN
	/* VARIABLE LENGTH FIELDS: */
	oidvector_extend	nodeoids;  /* List of nodes used by table */
	text		options;           /* Reserve column */
#endif
} FormData_pgxc_class;

typedef FormData_pgxc_class *Form_pgxc_class;

#define Natts_pgxc_class					10

#define Anum_pgxc_class_pcrelid			1
#define Anum_pgxc_class_pclocatortype		2
#define Anum_pgxc_class_pchashalgorithm		3
#define Anum_pgxc_class_pchashbuckets		4
#define Anum_pgxc_class_pgroup                  5
#define Anum_pgxc_class_redistributed           6
#define Anum_pgxc_class_redis_order             7
#define Anum_pgxc_class_pcattnum		8
#define Anum_pgxc_class_nodes			9
#define Anum_pgxc_class_option			10

typedef enum PgxcClassAlterType {
	PGXC_CLASS_ALTER_DISTRIBUTION,
	PGXC_CLASS_ALTER_NODES,
	PGXC_CLASS_ALTER_ALL
} PgxcClassAlterType;

extern void PgxcClassCreate(Oid pcrelid,
		            char pclocatortype,
		            int2* pcattnum,
		            int pchashalgorithm,
		            int pchashbuckets,
		            int numnodes,
		            Oid *nodes,
                            int distributeNum,
                            const char *groupname);
extern void PgxcClassAlter(Oid pcrelid,
		           char pclocatortype,
		           int2 *pcattnum,
		           int numpcattnum,
		           int pchashalgorithm,
		           int pchashbuckets,
		           int numnodes,
		           Oid *nodes,
		           char ch_redis,
		           PgxcClassAlterType type,
		           const char *groupname);
extern void RemovePgxcClass(Oid pcrelid, bool canmiss = false);
extern bool RelationRedisCheck(Relation rel);
extern void PgxcClassAlterForReloption(Oid pcrelid, const char* reloptionstr);
extern void PgxcClassCreateForReloption(Oid pcrelid, const char* reloptionstr);

#endif   /* PGXC_CLASS_H */

