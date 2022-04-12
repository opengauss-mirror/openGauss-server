/* -------------------------------------------------------------------------
 *
 * pg_subscription.h
 *	  definition of the "subscription" system catalog (pg_subscription)
 *
 * Portions Copyright (c) 1996-2020, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/catalog/pg_subscription.h
 *
 * NOTES
 *	  The Catalog.pm module reads this file and derives schema
 *	  information.
 *
 * -------------------------------------------------------------------------
 */
#ifndef PG_SUBSCRIPTION_H
#define PG_SUBSCRIPTION_H

#include "catalog/genbki.h"

/* ----------------
 * 		pg_subscription definition. cpp turns this into
 * 		typedef struct FormData_pg_subscription
 * ----------------
 */

/*
 * Technically, the subscriptions live inside the database, so a shared catalog
 * seems weird, but the replication launcher process needs to access all of
 * them to be able to start the workers, so we have to put them in a shared,
 * nailed catalog.
 *
 * NOTE:  When adding a column, also update system_views.sql.
 */
#define SubscriptionRelationId 6126
#define SubscriptionRelation_Rowtype_Id 6128
CATALOG(pg_subscription,6126) BKI_SHARED_RELATION BKI_ROWTYPE_OID(6128) BKI_SCHEMA_MACRO
{
    Oid subdbid;      /* Database the subscription is in. */
    NameData subname; /* Name of the subscription */

    Oid subowner; /* Owner of the subscription */

    bool subenabled; /* True if the subscription is enabled (the
                      * worker should be running) */

#ifdef CATALOG_VARLEN        /* variable-length fields start here */
    text subconninfo;        /* Connection string to the publisher */
    NameData subslotname;    /* Slot name on publisher */
    text subsynccommit;      /* Synchronous commit setting for worker */
    text subpublications[1]; /* List of publications subscribed to */
    bool subbinary;          /* True if the subscription wants the
                              * publisher to send data in binary */
#endif
}
FormData_pg_subscription;

typedef FormData_pg_subscription *Form_pg_subscription;

#define Natts_pg_subscription 9
#define Anum_pg_subscription_subdbid 1
#define Anum_pg_subscription_subname 2
#define Anum_pg_subscription_subowner 3
#define Anum_pg_subscription_subenabled 4
#define Anum_pg_subscription_subconninfo 5
#define Anum_pg_subscription_subslotname 6
#define Anum_pg_subscription_subsynccommit 7
#define Anum_pg_subscription_subpublications 8
#define Anum_pg_subscription_subbinary 9


typedef struct Subscription {
    Oid oid;            /* Oid of the subscription */
    Oid dbid;           /* Oid of the database which subscription is in */
    char *name;         /* Name of the subscription */
    Oid owner;          /* Oid of the subscription owner */
    bool enabled;       /* Indicates if the subscription is enabled */
    char *conninfo;     /* Connection string to the publisher */
    char *slotname;     /* Name of the replication slot */
    char *synccommit;   /* Synchronous commit setting for worker */
    List *publications; /* List of publication names to subscribe to */
    bool binary;        /* Indicates if the subscription wants data in binary format */
} Subscription;


extern Subscription *GetSubscription(Oid subid, bool missing_ok);
extern void FreeSubscription(Subscription *sub);
extern Oid get_subscription_oid(const char *subname, bool missing_ok);
extern char *get_subscription_name(Oid subid, bool missing_ok);

extern int CountDBSubscriptions(Oid dbid);
extern void ClearListContent(List *list);


#endif /* PG_SUBSCRIPTION_H */
