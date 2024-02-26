/*-------------------------------------------------------------------------
 *
 * event_trigger.h
 *   Declarations for command trigger handling.
 *
 * Portions Copyright (c) 1996-2011, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/commands/event_trigger.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef EVENT_TRIGGER_H
#define EVENT_TRIGGER_H

#include "catalog/pg_event_trigger.h"
#include "lib/ilist.h"
#include "nodes/parsenodes.h"
#include "knl/knl_session.h"
#include "catalog/objectaddress.h"
#include "catalog/dependency.h"
#include "tcop/deparse_utility.h"
#include "utils/acl.h"

typedef struct EventTriggerData
{
   NodeTag    type;
   const char *event;              /* event name */
   Node       *parsetree;          /* parse tree */
   const char *tag;                /* command tag */
} EventTriggerData;

typedef struct EventTriggerQueryState
{
   MemoryContext   cxt;
   /* sql_drop */
   slist_head  SQLDropList;
   bool        in_sql_drop;

   /* table_rewrite */
   Oid         table_rewrite_oid;  /* InvalidOid, or set for table_rewrite event */
   int         table_rewrite_reason;   /* AT_REWRITE reason */

   /* Support for command collection */
   bool        commandCollectionInhibited;
   CollectedCommand *currentCommand;
   List       *commandList;        /* list of CollectedCommand; see deparse_utility.h */
   
   struct EventTriggerQueryState *previous;
} EventTriggerQueryState;

#define AT_REWRITE_ALTER_PERSISTENCE   0x01
#define AT_REWRITE_DEFAULT_VAL         0x02
#define AT_REWRITE_COLUMN_REWRITE      0x04
#define AT_REWRITE_ALTER_OID           0x08
#define AT_REWRITE_ALTER_COMPRESSION   0x0B

#define currentEventTriggerState ((EventTriggerQueryState*)(u_sess->exec_cxt.EventTriggerState))

/* Support for dropped objects */
typedef struct SQLDropObject {
   ObjectAddress   address;
   const char     *schemaname;
   const char     *objname;
   const char     *objidentity;
   const char     *objecttype;
   List             *addrnames;
   List             *addrargs;
   bool             original;
   bool             normal;
   bool             istemp;
   slist_node       next;
} SQLDropObject;

/*
 * EventTriggerData is the node type that is passed as fmgr "context" info
 * when a function is called by the event trigger manager.
 */
#define CALLED_AS_EVENT_TRIGGER(fcinfo) \
   ((fcinfo)->context != NULL && IsA((fcinfo)->context, EventTriggerData))

extern Oid CreateEventTrigger(CreateEventTrigStmt *stmt);
extern void RemoveEventTriggerById(Oid ctrigOid);
extern Oid get_event_trigger_oid(const char *trigname, bool missing_ok);

extern Oid AlterEventTrigger(AlterEventTrigStmt *stmt);
extern ObjectAddress AlterEventTriggerOwner(const char *name, Oid newOwnerId);
extern void AlterEventTriggerOwner_oid(Oid, Oid newOwnerId);

extern bool EventTriggerSupportsGrantObjectType(GrantObjectType objtype);
extern bool EventTriggerSupportsObjectType(ObjectType obtype);
extern bool EventTriggerSupportsObjectClass(ObjectClass objclass);
extern void EventTriggerDDLCommandStart(Node *parsetree);
extern void EventTriggerDDLCommandEnd(Node *parsetree);

extern void EventTriggerSQLDrop(Node *parsetree);

extern bool EventTriggerBeginCompleteQuery(void);
extern void EventTriggerEndCompleteQuery(void);
extern bool trackDroppedObjectsNeeded(void);
extern void EventTriggerSQLDropAddObject(const ObjectAddress *object, bool original, bool normal);
extern void EventTriggerTableRewrite(Node *parsetree, Oid tableOid, int reason);

extern void EventTriggerInhibitCommandCollection(void);
extern void EventTriggerUndoInhibitCommandCollection(void);

extern void EventTriggerCollectSimpleCommand(ObjectAddress address,
                                ObjectAddress secondaryObject,
                                Node *parsetree);

extern void EventTriggerAlterTableStart(Node *parsetree);
extern void EventTriggerAlterTableRelid(Oid objectId);
extern void EventTriggerCollectAlterTableSubcmd(Node *subcmd,
                                 ObjectAddress address);
extern void EventTriggerAlterTableEnd(void);

extern void EventTriggerCollectGrant(InternalGrant *istmt);
extern void EventTriggerCollectAlterOpFam(AlterOpFamilyStmt *stmt,
                             Oid opfamoid, List *operators,
                             List *procedures);
extern void EventTriggerCollectCreateOpClass(CreateOpClassStmt *stmt,
                                Oid opcoid, List *operators,
                                List *procedures);
extern void EventTriggerCollectAlterTSConfig(AlterTSConfigurationStmt *stmt,
                                Oid cfgId, Oid *dictIds, int ndicts);
extern void EventTriggerCollectAlterDefPrivs(AlterDefaultPrivilegesStmt *stmt);

#endif   /* EVENT_TRIGGER_H */

