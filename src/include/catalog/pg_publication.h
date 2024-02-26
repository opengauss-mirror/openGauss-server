/*-------------------------------------------------------------------------
 *
 * pg_publication.h
 *	  definition of the relation sets relation (pg_publication)
  *
 * Portions Copyright (c) 1996-2016, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/catalog/pg_publication.h
 *
 * NOTES
 *	  the genbki.pl script reads this file and generates .bki
 *	  information from the DATA() statements.
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_PUBLICATION_H
#define PG_PUBLICATION_H

#include "catalog/genbki.h"
#include "catalog/objectaddress.h"
#include "nodes/pg_list.h"

/* Publication trigger events */
#define PUB_TRIG_DDL_CMD_END "ddl_command_end"
#define PUB_TRIG_DDL_CMD_START "ddl_command_start"

/* Publication event trigger prefix */
#define PUB_EVENT_TRIG_FORMAT "pg_deparse_trig_%s_%u"
#define PUB_EVENT_TRIG_PREFIX "pg_deparse_trig_"

/* ----------------
 *		pg_publication definition.  cpp turns this into
 *		typedef struct FormData_pg_publication
 *
 * ----------------
 */

#define int8 int64

#define PublicationRelationId 6130
#define PublicationRelation_Rowtype_Id 6141
CATALOG(pg_publication,6130) BKI_ROWTYPE_OID(6141) BKI_SCHEMA_MACRO
{
    NameData pubname; /* name of the publication */

    Oid pubowner; /* publication owner */

    /*
     * indicates that this is special publication which should encompass
     * all tables in the database (except for the unlogged and temp ones)
     */
    bool puballtables;

    /* true if inserts are published */
    bool pubinsert;

    /* true if updates are published */
    bool pubupdate;

    /* true if deletes are published */
    bool pubdelete;

    int8 pubddl;
}
FormData_pg_publication;
#undef int8

/* ----------------
 * 		Form_pg_publication corresponds to a pointer to a tuple with
 * 		the format of pg_publication relation.
 * ----------------
 */
typedef FormData_pg_publication *Form_pg_publication;

/* ----------------
 * 		compiler constants for pg_publication
 * ----------------
 */

#define Natts_pg_publication 7
#define Anum_pg_publication_pubname 1
#define Anum_pg_publication_pubowner 2
#define Anum_pg_publication_puballtables 3
#define Anum_pg_publication_pubinsert 4
#define Anum_pg_publication_pubupdate 5
#define Anum_pg_publication_pubdelete 6
#define Anum_pg_publication_pubddl 7

typedef struct PublicationActions {
    bool pubinsert;
    bool pubupdate;
    bool pubdelete;
    int64 pubddl;
} PublicationActions;

typedef struct Publication {
    Oid oid;
    char *name;
    bool alltables;
    PublicationActions pubactions;
} Publication;

#define PUBDDL_NONE 0
#define PUBDDL_TABLE ((int64)1 << 0)
#define PUBDDL_ALL ((int64)0xFFFFFFFFFFFFFFFF)

#define ENABLE_PUBDDL_TYPE(pub, obj) \
    ((bool)((int64)(pub) & (obj)))

extern Publication *GetPublicationByName(const char *pubname, bool missing_ok);
extern List *GetRelationPublications(Oid relid);
extern List *GetPublicationRelations(Oid pubid);
extern List *GetAllTablesPublications(void);

extern ObjectAddress publication_add_relation(Oid pubid, Relation targetrel, bool if_not_exists);

extern Oid get_publication_oid(const char *pubname, bool missing_ok);
extern char *get_publication_name(Oid pubid, bool missing_ok);

extern Datum pg_get_publication_tables(PG_FUNCTION_ARGS);
extern bool is_publishable_relation(Relation rel);

#endif /* PG_PUBLICATION_H */
