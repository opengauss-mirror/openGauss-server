/* -------------------------------------------------------------------------
 *
 * pg_subscription_rel.h
 *		Local info about tables that come from the publisher of a
 *		subscription (pg_subscription_rel).
 *
 * Portions Copyright (c) 1996-2016, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * -------------------------------------------------------------------------
 */
#ifndef PG_SUBSCRIPTION_REL_H
#define PG_SUBSCRIPTION_REL_H

#include "catalog/genbki.h"

/* ----------------
 *		pg_subscription_rel definition. cpp turns this into
 *		typedef struct FormData_pg_subscription_rel
 * ----------------
 */
#define SubscriptionRelRelationId 6135
#define SubscriptionRelRelation_Rowtype_Id 6139

CATALOG(pg_subscription_rel,6135) BKI_WITHOUT_OIDS BKI_ROWTYPE_OID(6139) BKI_SCHEMA_MACRO
{
    Oid srsubid;     /* Oid of subscription */
    Oid srrelid;     /* Oid of relation */
    char srsubstate; /* state of the relation in subscription */
    int8 srcsn;        /* csn of snapshot used during copy */
#ifdef CATALOG_VARLEN        /* variable-length fields start here */
    text srsublsn;   /* remote lsn of the state change
                      * used for synchronization coordination */
#endif
} FormData_pg_subscription_rel;

typedef FormData_pg_subscription_rel *Form_pg_subscription_rel;

/* ----------------
 *		compiler constants for pg_subscription_rel
 * ----------------
 */
#define Natts_pg_subscription_rel 5
#define Anum_pg_subscription_rel_srsubid 1
#define Anum_pg_subscription_rel_srrelid 2
#define Anum_pg_subscription_rel_srsubstate 3
#define Anum_pg_subscription_rel_srcsn 4
#define Anum_pg_subscription_rel_srsublsn 5

/* ----------------
 *		substate constants
 * ----------------
 */
#define SUBREL_STATE_INIT 'i'     /* initializing (sublsn NULL) */
#define SUBREL_STATE_DATASYNC 'd' /* data is being synchronized (sublsn NULL) */
#define SUBREL_STATE_FINISHEDCOPY 'f' /* tablesync copy phase is completed
                                       * (sublsn NULL) */
#define SUBREL_STATE_SYNCDONE 's' /* synchronization finished infront of apply (sublsn set) */
#define SUBREL_STATE_READY 'r'    /* ready (sublsn set) */

/* These are never stored in the catalog, we only use them for IPC. */
#define SUBREL_STATE_UNKNOWN '\0' /* unknown state */
#define SUBREL_STATE_SYNCWAIT 'w' /* waiting for sync */
#define SUBREL_STATE_CATCHUP 'c'  /* catching up with apply */

typedef struct SubscriptionRelState
{
    Oid relid;
    XLogRecPtr lsn;
    char state;
} SubscriptionRelState;

extern Oid AddSubscriptionRelState(Oid subid, Oid relid, char state);
extern Oid UpdateSubscriptionRelState(Oid subid, Oid relid, char state, XLogRecPtr sublsn,
                                      CommitSeqNo subcsn = InvalidCommitSeqNo);
extern char GetSubscriptionRelState(Oid subid, Oid relid, XLogRecPtr *sublsn, CommitSeqNo *subcsn = NULL);
extern void RemoveSubscriptionRel(Oid subid, Oid relid);

extern List *GetSubscriptionRelations(Oid subid, bool needNotReady);

#endif   /* PG_SUBSCRIPTION_REL_H */