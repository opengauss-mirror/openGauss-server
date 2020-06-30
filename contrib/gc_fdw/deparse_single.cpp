/*-------------------------------------------------------------------------
 *
 * deparse_single.cpp
 *		  Query deparser for gc_fdw
 *
 * IDENTIFICATION
 *		  contrib/gc_fdw/deparse_single.cpp
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "knl/knl_variable.h"

#include "gc_fdw.h"

#include "access/heapam.h"
#include "access/htup.h"
#include "access/sysattr.h"
#include "catalog/pg_aggregate.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_type.h"
#include "commands/defrem.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "nodes/plannodes.h"
#include "optimizer/clauses.h"
#include "optimizer/prep.h"
#include "optimizer/tlist.h"
#include "optimizer/var.h"
#include "parser/parsetree.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/syscache.h"
#include "utils/typcache.h"


/*
 * the string list of the targetlist of ForeignScan is used in explain command when agg
 * is deparsed to remote sql.
 */
List* get_str_targetlist(List* fdw_private)
{
    List* str_targetlist = (List*)list_nth(fdw_private, FdwScanPrivateStrTargetlist);

    List* rs = NIL;
    ListCell* lc = NULL;
    foreach (lc, str_targetlist) {
        Value* val = (Value*)lfirst(lc);
        rs = lappend(rs, val->val.str);
    }

    return rs;
}

