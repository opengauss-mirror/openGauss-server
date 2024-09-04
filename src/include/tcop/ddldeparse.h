/*-------------------------------------------------------------------------
 *
 * ddldeparse.h
 *
 * Portions Copyright (c) 2023, PostgreSQL Global Development Group
 *
 * src/include/tcop/ddldeparse.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef DDL_DEPARSE_H
#define DDL_DEPARSE_H

#include "tcop/deparse_utility.h"

/* Context info needed for deparsing ddl command */
typedef struct
{
    bool        verbose_mode;
    /*
     * include_owner indicates if the owner/role of the command should be
     * included in the deparsed Json output. It is set to false for any commands
     * that don't CREATE database objects (ALTER commands for example), this is
     * to avoid encoding and sending the owner to downstream for replay as it is
     * unnecessary for such commands.
     */
    bool include_owner;

    /* The maximum volatility of functions in expressions of a DDL command. */
    char max_volatility;
}  ddl_deparse_context;

extern Relation table_open(Oid relationId, LOCKMODE lockmode);
extern void table_close(Relation relation, LOCKMODE lockmode);
extern char *deparse_utility_command(CollectedCommand *cmd,
                                     ddl_deparse_context * context);
extern char *deparse_ddl_json_to_string(char *jsonb, char** owner);
extern char *deparse_drop_command(const char *objidentity, const char *objecttype, Node *parsetree);
extern List *deparse_altertable_end(CollectedCommand *cmd);
extern bool relation_support_ddl_replication(Oid relid, bool rewrite = false);

/*
 * Before they are turned into JSONB representation, each command is
 * represented as an object tree, using the structs below.
 */
typedef enum
{
    ObjTypeNull,
    ObjTypeBool,
    ObjTypeString,
    ObjTypeArray,
    ObjTypeInteger,
    ObjTypeFloat,
    ObjTypeObject
} ObjType;

/*
 * Represent the command as an object tree.
 */
typedef struct ObjTree
{
    slist_head  params;         /* Object tree parameters */
    int numParams;      /* Number of parameters in the object tree */
    StringInfo fmtinfo;        /* Format string of the ObjTree */
    bool present; /* Indicates if boolean value should be stored */
} ObjTree;

/*
 * An element of an object tree (ObjTree).
 */
typedef struct ObjElem
{
    char *name;           /* Name of object element */
    ObjType objtype;        /* Object type */

    union {
        bool boolean;
        char *string;
        int64 integer;
        float8 flt;
        ObjTree *object;
        List *array;
    } value;          /* Store the object value based on the object
                                 * type */
    slist_node node;           /* Used in converting back to ObjElem
                                 * structure */
} ObjElem;

ObjTree *new_objtree_VA(const char *fmt, int numobjs, ...);
ObjElem *new_string_object(char *value);
void append_format_string(ObjTree *tree, char *sub_fmt);
void append_object_object(ObjTree *tree, char *sub_fmt, ObjTree *value);
void append_string_object(ObjTree *tree, char *sub_fmt, char *name,
                          const char *value);
void append_array_object(ObjTree *tree, char *sub_fmt, List *array);
typedef enum {
    DEPARSE_SIMPLE_COMMAND,
    ALTER_RELATION_SUBCMD
} collectCmdHookType;
typedef void *(*deparseCollectedCommand)(int type, CollectedCommand *cmd, CollectedATSubcmd *sub,
                                         ddl_deparse_context *context);
#endif /* DDL_DEPARSE_H */
