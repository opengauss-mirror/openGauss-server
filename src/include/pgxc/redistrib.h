/* -------------------------------------------------------------------------
 *
 * redistrib.h
 *	  Routines related to online data redistribution
 *
 * Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 *
 * IDENTIFICATION
 *	  src/include/pgxc/redistrib.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef REDISTRIB_H
#define REDISTRIB_H

#include "commands/defrem.h"
#include "nodes/parsenodes.h"
#include "utils/tuplestore.h"
#include "storage/item/itemptr.h"

/*
 * Type of data redistribution operations.
 * Online data redistribution is made of one or more of those operations.
 */
typedef enum RedistribOperation {
    DISTRIB_NONE,          /* Default operation */
    DISTRIB_DELETE_HASH,   /* Perform a DELETE with hash value check */
    DISTRIB_DELETE_MODULO, /* Perform a DELETE with modulo value check */
    DISTRIB_COPY_TO,       /* Perform a COPY TO */
    DISTRIB_COPY_FROM,     /* Perform a COPY FROM */
    DISTRIB_TRUNCATE,      /* Truncate relation */
    DISTRIB_REINDEX        /* Reindex relation */
} RedistribOperation;

/*
 * Determine if operation can be done before or after
 * catalog update on local node.
 */
typedef enum RedistribCatalog {
    CATALOG_UPDATE_NONE,   /* Default state */
    CATALOG_UPDATE_AFTER,  /* After catalog update */
    CATALOG_UPDATE_BEFORE, /* Before catalog update */
    CATALOG_UPDATE_BOTH    /* Before and after catalog update */
} RedistribCatalog;

/*
 * Redistribution command
 * This contains the tools necessary to perform a redistribution operation.
 */
typedef struct RedistribCommand {
    RedistribOperation type;      /* Operation type */
    ExecNodes* execNodes;         /* List of nodes where to perform operation */
    RedistribCatalog updateState; /* Flag to determine if operation can be done before or after catalog update */
} RedistribCommand;

/*
 * Redistribution operation state
 * Maintainer of redistribution state having the list of commands
 * to be performed during redistribution.
 * For the list of commands, we use an array and not a simple list as operations
 * might need to be done in a certain order.
 */
typedef struct RedistribState {
    Oid relid;              /* Oid of relation redistributed */
    List* commands;         /* List of commands */
    Tuplestorestate* store; /* Tuple store used for temporary data storage */
} RedistribState;

extern void PGXCRedistribTable(RedistribState* distribState, RedistribCatalog type);
extern void PGXCRedistribCreateCommandList(RedistribState* distribState, RelationLocInfo* newLocInfo);
extern RedistribCommand* makeRedistribCommand(RedistribOperation type, 
                        RedistribCatalog updateState, ExecNodes* nodes);
extern RedistribState* makeRedistribState(Oid relOid);
extern void FreeRedistribState(RedistribState* state);
extern void FreeRedistribCommand(RedistribCommand* command);

extern List* AlterTableSetRedistribute(Relation rel, RedisRelAction action, char *merge_list);
extern void AlterTableSetPartRelOptions(
    Relation rel, List* defList, AlterTableType operation, LOCKMODE lockmode, char *merge_list, RedisRelAction action);
extern void RelationGetCtids(Relation rel, ItemPointer start_ctid, ItemPointer end_ctid);
extern uint32 RelationGetEndBlock(Relation rel);
extern void get_redis_rel_ctid(
    const char* rel_name, const char* partition_name, RedisCtidType ctid_type, ItemPointer result);
#ifdef ENABLE_MULTIPLE_NODES
extern void CheckRedistributeOption(List* options, Oid* rel_cn_oid, 
                RedisHtlAction* action, char **merge_list, Relation rel);
#else
extern void CheckRedistributeOption(List* options, Oid* rel_cn_oid, 
                RedisHtlAction* action, Relation rel);
#endif


extern bool set_proc_redis(void);
extern bool reset_proc_redis(void);

extern void RemoveRedisRelOptionsFromList(List** reloptions);
extern Node* eval_redis_func_direct(Relation rel, bool is_func_get_start_ctid, int num_of_slice = 0, int slice_index = 0);
extern List* add_ctid_string_to_reloptions(List *def_list, const char *item_name, ItemPointer ctid);
extern ItemPointer eval_redis_func_direct_slice(ItemPointer start_ctid, ItemPointer end_ctid, bool is_func_get_start_ctid, int num_of_slices, int slice_index);
extern void reset_merge_list_on_pgxc_class(Relation rel);

/*
 * ItemPointerGetBlockNumber
 *		Returns the block number of a disk item pointer.
 */
#define RedisCtidGetBlockNumber(pointer) (AssertMacro((pointer) != NULL), BlockIdGetBlockNumber(&(pointer)->ip_blkid))

/*
 * ItemPointerGetOffsetNumber
 *		Returns the offset number of a disk item pointer.
 */
#define RedisCtidGetOffsetNumber(pointer) (AssertMacro((pointer) != NULL), (pointer)->ip_posid)

#endif /* REDISTRIB_H */
