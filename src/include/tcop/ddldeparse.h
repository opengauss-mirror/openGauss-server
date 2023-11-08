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

extern char *deparse_utility_command(CollectedCommand *cmd,
                                     ddl_deparse_context * context);
extern char *deparse_ddl_json_to_string(char *jsonb, char** owner);
extern char *deparse_drop_command(const char *objidentity, const char *objecttype, Node *parsetree);

#endif /* DDL_DEPARSE_H */
