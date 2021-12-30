/* -------------------------------------------------------------------------
 *
 * toasting.h
 *	  This file provides some definitions to support creation of toast tables
 *
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/catalog/toasting.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef TOASTING_H
#define TOASTING_H

/*
 * toasting.c prototypes
 */
extern void AlterTableCreateToastTable(Oid relOid, Datum reloptions, LOCKMODE partLockMode = AccessExclusiveLock);
extern void BootstrapToastTable(char *relName,
                                Oid toastOid,
                                Oid toastIndexOid);
extern bool createToastTableForPartition(Oid relOid, 
                                         Oid partOid,
                                         Datum reloptions,
                                         LOCKMODE partLockMode);
extern bool CreateToastTableForSubPartition(Relation partRel, Oid subPartOid, Datum reloptions, LOCKMODE partLockMode);

/*
 * This macro is just to keep the C compiler from spitting up on the
 * upcoming commands for genbki.pl.
 */
#define DECLARE_TOAST(name,toastoid, indexoid) extern int no_such_variable

/*
 * What follows are lines processed by genbki.pl to create the statements
 * the bootstrap parser will turn into BootstrapToastTable commands.
 * Each line specifies the system catalog that needs a toast table,
 * the OID to assign to the toast table, and the OID to assign to the
 * toast table's index.  The reason we hard-wire these OIDs is that we
 * need stable OIDs for shared relations, and that includes toast tables
 * of shared relations.
 */

/* normal catalogs */
DECLARE_TOAST(pg_attrdef, 2830, 2831);
DECLARE_TOAST(pg_constraint, 2832, 2833);
DECLARE_TOAST(pg_description, 2834, 2835);
DECLARE_TOAST(pg_proc, 2836, 2837);
DECLARE_TOAST(pg_rewrite, 2838, 2839);
DECLARE_TOAST(pg_rlspolicy, 3173, 3174);
DECLARE_TOAST(pg_seclabel, 3598, 3599);
DECLARE_TOAST(pg_statistic, 2840, 2841);
DECLARE_TOAST(pg_statistic_ext, 3439, 3440);
DECLARE_TOAST(pg_trigger, 2336, 2337);
DECLARE_TOAST(pg_partition, 5502, 5503);
DECLARE_TOAST(pgxc_class, 5506, 5507);
DECLARE_TOAST(pg_hashbucket, 4392, 4393);
DECLARE_TOAST(gs_package, 8002, 8003);
DECLARE_TOAST(gs_global_chain, 5816, 5817);
DECLARE_TOAST(gs_model_warehouse, 3995, 3996);

/* shared catalogs */
DECLARE_TOAST(pg_shdescription, 2846, 2847);
#define PgShdescriptionToastTable 2846
#define PgShdescriptionToastIndex 2847
DECLARE_TOAST(pg_db_role_setting, 2966, 2967);
#define PgDbRoleSettingToastTable 2966
#define PgDbRoleSettingToastIndex 2967
DECLARE_TOAST(pgxc_group, 5504, 5505);
#define PgxcGroupToastTable 5504
#define PgxcGroupToastIndex 5505
#endif   /* TOASTING_H */

