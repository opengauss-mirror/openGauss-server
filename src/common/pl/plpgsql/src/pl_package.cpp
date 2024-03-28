/* -------------------------------------------------------------------------
 *
 * Portions Copyright (c) 2021, openGauss Contributors

 * IDENTIFICATION
 *    src/common/pl/plpgsql/src/pl_package.cpp
 * -------------------------------------------------------------------------
 */

#include <ctype.h>

#include "utils/plpgsql_domain.h"
#include "catalog/namespace.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_proc_fn.h"
#include "catalog/gs_package.h"
#include "catalog/gs_package_fn.h"
#include "catalog/pg_type.h"
#include "executor/spi.h"
#include "funcapi.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "nodes/nodes.h"
#include "nodes/pg_list.h"
#include "optimizer/clauses.h"
#include "optimizer/subselect.h"
#include "parser/parse_type.h"
#include "pgxc/locator.h"
#include "utils/pl_package.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/globalplancore.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/inval.h"
#include "utils/rel.h"
#include "utils/rel_gs.h"
#include "utils/syscache.h"
#include "utils/acl.h"
#include "miscadmin.h"
#include "parser/scanner.h"
#include "parser/parser.h"
#include "catalog/pg_object.h"
#include "catalog/gs_dependencies_fn.h"

static void plpgsql_pkg_append_dlcell(plpgsql_pkg_HashEnt* entity);

static void plpgsql_pkg_HashTableInsert(PLpgSQL_package* pkg, PLpgSQL_pkg_hashkey* pkg_key);

extern PLpgSQL_package* plpgsql_pkg_HashTableLookup(PLpgSQL_pkg_hashkey* pkg_key);

extern void plpgsql_compile_error_callback(void* arg);

static Node* plpgsql_bind_variable_column_ref(ParseState* pstate, ColumnRef* cref);
static Node* plpgsql_describe_ref(ParseState* pstate, ColumnRef* cref);
static void gsplsql_pkg_set_status(PLpgSQL_package* pkg, bool isSpec, bool isCreate, bool isValid, bool isRecompile,
                                   bool pkg_spec_valid, bool pkg_body_valid);
static void gsplsql_pkg_set_status_in_mem(PLpgSQL_package* pkg, bool is_spec, bool is_valid);
static inline bool gsplsql_pkg_get_valid(PLpgSQL_package* pkg, bool is_spec);
/*
 * plpgsql_parser_setup_bind		set up parser hooks for dynamic parameters
 * only support DBE_SQL.
 */
void plpgsql_parser_setup_bind(struct ParseState* pstate, List** expr)
{
    pstate->p_bind_variable_columnref_hook = plpgsql_bind_variable_column_ref;
    pstate->p_bind_hook_state = (void*)expr;
}

/*
 * plpgsql_bind_variable_column_ref		parser callback after parsing a ColumnRef
 * only support DBE_SQL.
 */
static Node* plpgsql_bind_variable_column_ref(ParseState* pstate, ColumnRef* cref)
{
    List** expr = (List**)pstate->p_bind_hook_state;

    /* get column name */
    Node* field1 = (Node*)linitial(cref->fields);
    AssertEreport(IsA(field1, String), MOD_PLSQL, "string type is required.");
    const char* name1 = NULL;
    name1 = strVal(field1);

    /* get column type */
    int len = 1;
    ListCell* lc_name = NULL;
    ListCell* lc_type = NULL;
    Oid argtypes = 0;
    forboth (lc_type, expr[0], lc_name, expr[1]) {
        if (pg_strcasecmp((char *)lfirst(lc_name), name1) != 0) {
            len++;
            continue;
        }
        argtypes = lfirst_oid(lc_type);
        break;
    }

    if (argtypes == 0) {
        ereport(ERROR, (errmodule(MOD_PLSQL), errcode(ERRCODE_AMBIGUOUS_COLUMN),
                errmsg("argtypes is not valid"), errdetail("Confirm function input parameters."),
                errcause("parameters error."), erraction("Confirm function input parameters.")));
    }

    /* Generate param and Fill by index(len) */
    Param* param = NULL;
    param = makeNode(Param);
    param->paramkind = PARAM_EXTERN;
    param->paramid = len;
    param->paramtype = argtypes;
    param->paramtypmod = -1;
    param->paramcollid = get_typcollation(param->paramtype);
    param->location = cref->location;
    return (Node*)param;
}

/*
 * plpgsql_parser_setup_describe		set up parser hooks for dynamic parameters
 * only support DBE_SQL.
 */
void plpgsql_parser_setup_describe(struct ParseState* pstate, List** expr)
{
    pstate->p_bind_describe_hook = plpgsql_describe_ref;
    pstate->p_describeco_hook_state = (void*)expr;
}

/*
 * plpgsql_describe_ref		parser callback after parsing a ColumnRef
 * only support DBE_SQL.
 */
static Node* plpgsql_describe_ref(ParseState* pstate, ColumnRef* cref)
{
    Assert(pstate);
    /* Generate param and Fill by index(len) */
    Param* param = NULL;
    param = makeNode(Param);
    param->paramkind = PARAM_EXTERN;
    param->paramid = 0;
    param->paramtype = 20;
    param->paramtypmod = -1;
    param->location = cref->location;
    return (Node*)param;
}

static void build_pkg_row_variable(int varno, PLpgSQL_package* pkg, const char* pkgName, const char* nspName);

bool IsOnlyCompilePackage()
{
    if (u_sess->plsql_cxt.curr_compile_context != NULL && 
        u_sess->plsql_cxt.curr_compile_context->plpgsql_curr_compile_package != NULL &&
        u_sess->plsql_cxt.curr_compile_context->plpgsql_curr_compile == NULL) {
            return true;
    } else {
        return false;
    }
    return false;
}


PLpgSQL_datum* plpgsql_pkg_adddatum(const List* wholeName, char** objname, char** pkgname)
{
    char* nspname = NULL;
    PLpgSQL_package* pkg = NULL;
    struct PLpgSQL_nsitem* nse = NULL;
    PLpgSQL_datum* datum = NULL;
    PLpgSQL_pkg_hashkey hashkey;
    Oid pkgOid;
    Oid namespaceId = InvalidOid;
    DeconstructQualifiedName(wholeName, &nspname, objname, pkgname);
    if (nspname != NULL) {
        namespaceId = LookupExplicitNamespace(nspname);
    }
    if (*pkgname == NULL) {
        return NULL;
    }
    /*
     * Lookup the gs_package tuple by Oid; we'll need it in any case
     */
    pkgOid = PackageNameGetOid(*pkgname, namespaceId);
    hashkey.pkgOid = pkgOid;

    pkg = plpgsql_pkg_HashTableLookup(&hashkey);
    
    if (pkg == NULL) {
        pkg = PackageInstantiation(pkgOid);
    }
    /* 
     * find package variable and return package datum,if not found,
     * return NULL
     */
    if (pkg != NULL) {
        nse = plpgsql_ns_lookup(pkg->public_ns, false, *objname, NULL, NULL, NULL);
        if (nse == NULL) {
            return NULL;
        }
    } else {
        return NULL;
    }
    datum = pkg->datums[nse->itemno];
    return datum;
}

/*
 * add package vairable to namespace 
 */
int plpgsql_pkg_adddatum2ns(const List* name)
{
    PLpgSQL_datum* datum = NULL;
    int varno;
    char* objname = NULL;
    char* pkgname = NULL;

    datum = plpgsql_pkg_adddatum(name, &objname, &pkgname);

    if (datum == NULL) {
        return -1;
    } else {
        varno = plpgsql_adddatum(datum);
        switch (datum->dtype)
        {
            case PLPGSQL_DTYPE_VAR:
                plpgsql_ns_additem(PLPGSQL_NSTYPE_VAR, varno, objname, pkgname);
                break;
            case PLPGSQL_DTYPE_ROW:
                plpgsql_ns_additem(PLPGSQL_NSTYPE_ROW, varno, objname, pkgname);
                break;
            case PLPGSQL_DTYPE_RECORD:
                plpgsql_ns_additem(PLPGSQL_NSTYPE_RECORD, varno, objname, pkgname);
                break;
            default:
                ereport(ERROR, (errmodule(MOD_PLSQL), errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                        errmsg("unrecognized type: %d, when build variable in PLSQL, this situation should not occur.",
                            datum->dtype)));
                break;
        }
    }
    return varno;
}

static void build_pkg_cursor_variable(int varno, PLpgSQL_package* pkg,  const char* pkgName, const char* nspName)
{
    int cursorAttrNum = 4;
    int dno = -1;
    char* refname = NULL;
    for (int i = 1; i <= cursorAttrNum; i++) {
        dno = plpgsql_adddatum(pkg->datums[varno + i], false);
        refname = ((PLpgSQL_variable*)(pkg->datums[varno + i]))->refname;
        plpgsql_ns_additem(PLPGSQL_NSTYPE_VAR, dno, refname, pkgName, nspName);
    }
}

static void build_pkg_row_variable(int varno, PLpgSQL_package* pkg, const char* pkgName, const char* nspName)
{
    PLpgSQL_row* row = (PLpgSQL_row*)pkg->datums[varno];
    int dno = -1;
    PLpgSQL_datum* datum = NULL;
    char* refName = NULL;

    for (int i = 0; i < row->nfields; i++) {
        datum = row->pkg->datums[row->varnos[i]];
        if (datum != NULL) {
            refName = ((PLpgSQL_variable*)datum)->refname;
            dno = plpgsql_adddatum(pkg->datums[row->varnos[i]], false);
            if (datum->dtype == PLPGSQL_DTYPE_VAR) {
                plpgsql_ns_additem(PLPGSQL_NSTYPE_VAR, dno, refName, pkgName, nspName);
            } else {
                plpgsql_ns_additem(PLPGSQL_NSTYPE_ROW, dno, refName, pkgName, nspName);
            }
        }
    }
}


int plpgsql_build_pkg_variable(List* name, PLpgSQL_datum* datum, bool isSamePkg)
{
    int varno = 0;
    char* objname = NULL;
    char* pkgname = NULL;
    char* nspname = NULL;
    
    DeconstructQualifiedName(name, &nspname, &objname, &pkgname);

    switch (datum->dtype) {
        case PLPGSQL_DTYPE_VAR: {
            /* Ordinary scalar datatype */
            PLpgSQL_var* var = (PLpgSQL_var*)datum;
            varno = isSamePkg ? var->dno : plpgsql_adddatum(datum, false);
            if (var->addNamespace) {
                plpgsql_ns_additem(PLPGSQL_NSTYPE_VAR, varno, var->refname, pkgname, nspname);
            }
            if (var->datatype->typoid == REFCURSOROID && !isSamePkg) {
                build_pkg_cursor_variable(var->dno, var->pkg, pkgname, nspname);
            }
            return varno;
        }
        case PLPGSQL_DTYPE_ROW: {
            /* Ordinary scalar datatype */
            PLpgSQL_row* row = (PLpgSQL_row*)datum;
            varno = isSamePkg ? row->dno : plpgsql_adddatum(datum, false);
            if (row->addNamespace) {
                plpgsql_ns_additem(PLPGSQL_NSTYPE_ROW, varno, row->refname, pkgname, nspname);
            }
            if (!isSamePkg) {
                build_pkg_row_variable(row->dno, row->pkg, pkgname, nspname);
            }
            return varno;
        }
        case PLPGSQL_DTYPE_RECORD: {
            /* "record" type -- build a record variable */
            PLpgSQL_row* row = (PLpgSQL_row*)datum;

            varno = isSamePkg ? row->dno : plpgsql_adddatum(datum, false);
            if (row->addNamespace) {
                plpgsql_ns_additem(PLPGSQL_NSTYPE_ROW, varno, row->refname, pkgname, nspname);
            }
            if (!isSamePkg) {
                build_pkg_row_variable(row->dno, row->pkg, pkgname, nspname);
            }
            return varno;
        }

    }
    return -1;
}

/*
 * use unknown type when compile a function which has package variable
 * because gauss doesn't support compile multiple function at the same time 
 * so we have a fake compile when compile other functions which has package 
 * variable.we only check if the package is exist,if not exist,return -1,
 * else return the variable number. 
 */

int plpgsql_pkg_add_unknown_var_to_namespace(List* name)
{
#ifdef ENABLE_MULTIPLE_NODES
    return -1;
#endif

    if (list_length(name) >= 4) {
        return -1;
    }

    bool isSamePkg = false;
    PLpgSQL_datum* datum = GetPackageDatum(name, &isSamePkg);
    if (datum != NULL) {
        /*
         * The current memory context is temp context, when this function is called by yylex_inparam etc,
         * so we should swtich to function context.
         * If add package var, plpgsql_ns_additem will swtich to package context.
         */
        MemoryContext oldCxt = MemoryContextSwitchTo(u_sess->plsql_cxt.curr_compile_context->compile_cxt);
        int varno = plpgsql_build_pkg_variable(name, datum, isSamePkg);
        (void)MemoryContextSwitchTo(oldCxt);
        return varno;
    } else {
        return -1;
    }
    return -1;
}

/*
 * @Description : Check co-location for opexpr. we walk through the lists of opexpr
 * to check where they are from new/old with all the distributeion keys of both queries.
 *
 * @in query : Query struct of the query with trigger.
 * @in qry_part_attr_num : list of attr no of the dist keys of triggering query.
 * @in trig_part_attr_num : list of attr no of the dist keys of query in trigger.
 * @in func : trigger function body information.
 * @return : when co-location return true.
 */
extern bool plpgsql_check_opexpr_colocate(
    Query* query, List* qry_part_attr_num, List* trig_part_attr_num, PLpgSQL_function* func, List* opexpr_list)
{
    ListCell* lc1 = NULL;
    ListCell* lc2 = NULL;
    bool is_colocate = true;

    forboth(lc1, qry_part_attr_num, lc2, trig_part_attr_num)
    {
        Expr* expr = NULL;
        Param* param = NULL;
        PLpgSQL_recfield* recfield = NULL;
        PLpgSQL_rec* rec = NULL;
        int fno;
        AttrNumber attnum1 = lfirst_int(lc1);
        AttrNumber attnum2 = lfirst_int(lc2);
        ListCell* opexpr_cell = NULL;

        /* Only all distribute column can colocate, we can ship. */
        if (!is_colocate) {
            return false;
        }

        foreach (opexpr_cell, opexpr_list) {
            Expr* qual_expr = (Expr*)lfirst(opexpr_cell);

            /* Check all opexpr with distribute column */
            expr = pgxc_check_distcol_opexpr(linitial_int(query->resultRelations), attnum2, (OpExpr*)qual_expr);
            if (expr == NULL) {
                is_colocate = false;
                continue;
            }

            /* NEW/OLD is replaced with param by parser */
            if (!IsA(expr, Param)) {
                is_colocate = false;
                continue;
            }
            param = (Param*)expr;

            /* This param should point to datum in func */
            recfield = (PLpgSQL_recfield*)func->datums[param->paramid - 1];

            /* From there we get the new or old rec */
            rec = (PLpgSQL_rec*)func->datums[recfield->recparentno];
            if (strcmp(rec->refname, "new") != 0 && strcmp(rec->refname, "old") != 0) {
                is_colocate = false;
                continue;
            }

            /* We should already set tupdesc at the very beginning */
            if (rec->tupdesc == NULL) {
                is_colocate = false;
                continue;
            }

            /*
             * Find field index of new.a1 and only if it matches to
             * current distribution key of src table, we could call
             * both tables are DML colocated
             */
            fno = SPI_fnumber(rec->tupdesc, recfield->fieldname);
            if (fno != attnum1) {
                is_colocate = false;
                continue;
            }

            is_colocate = true;
            break;
        }
    }

    return is_colocate;
}

/*
 * @Description : Check co-location for update or delete command. To check co-location of the
 * update or delete query in trigger and the triggering query, we walk through the lists of
 * of attribute no of distribution keys of both queries. For each pair, we check
 * if the distribute key of trigger table exist in its where clause and its expression is
 * from new/old and is the distribute key of the table the triggering query work on.
 *
 * For example,
 * tables:
 * create table t1(a1 int, b1 int, c1 int,d1 varchar(100)) distribute by hash(a1,b1);
 * create table t2(a2 int, b2 int, c2 int,d2 varchar(100)) distribute by hash(a2,b2);
 * update in trigger body:
 * update t2 set d2=new.d1 where a2=old.a1 and b2=old.b1;
 * triggering event:
 * an insert or update on t1
 *
 * we walk through two pairs: (a1, a2) and (b1, b2), and check if a2 is in the
 * where clause "a2=old.a1 and b2=old.b1", and if its expression is from new/old
 * and is a1; if b2 is in the where clause too and its expression is from new/old
 * and is b1. If the above two checks are satified, co-location is true.
 *
 * @in query : Query struct of the query with trigger.
 * @in qry_part_attr_num : list of attr no of the dist keys of triggering query.
 * @in trig_part_attr_num : list of attr no of the dist keys of query in trigger.
 * @in func : trigger function body information.
 * @return : when co-location return true.
 */
bool plpgsql_check_updel_colocate(
    Query* query, List* qry_part_attr_num, List* trig_part_attr_num, PLpgSQL_function* func)
{
    Node* whereClause = NULL;
    List* opexpr_list = NIL;
    bool is_colocate = true;

    if (query->jointree == NULL || query->jointree->quals == NULL) {
        return false;
    }

    /* Recursively get a list of opexpr from quals. */
    opexpr_list = pull_opExpr((Node*)query->jointree->quals);
    if (opexpr_list == NIL) {
        return false;
    }
    /* Flatten AND/OR expression for checking or expr. */
    whereClause = eval_const_expressions(NULL, (Node*)query->jointree->quals);

    /* If it is or clause, we break the clause to check colocation of each expr */
    if (or_clause(whereClause)) {
        List* temp_list = NIL;
        ListCell* opexpr_cell = NULL;

        /* For or condtion, we can ship only when all opexpr are colocated. */
        foreach (opexpr_cell, opexpr_list) {
            temp_list = list_make1((Expr*)lfirst(opexpr_cell));
            is_colocate = plpgsql_check_opexpr_colocate(query, qry_part_attr_num, trig_part_attr_num, func, temp_list);

            if (temp_list != NIL) {
                list_free_ext(temp_list);
            }

            if (!is_colocate) {
                break;
            }
        }
        
        if (opexpr_list != NIL) {
            list_free_ext(opexpr_list);
        }
        return is_colocate;
    } else {
        /* For and with no or condition, we can ship when any opexpr is colocated. */
        is_colocate = plpgsql_check_opexpr_colocate(query, qry_part_attr_num, trig_part_attr_num, func, opexpr_list);
    }

    if (opexpr_list != NIL) {
        list_free_ext(opexpr_list);
    }
    return is_colocate;
}

/*
 * @Description : Check co-location for INSERT/UPDATE/DELETE statement and the
 * the statment which it triggered.
 *
 * @in query : Query struct info about the IUD statement.
 * @in rte : range table entry for the insert/update/delete statement.
 * @in plpgsql_func : information for the insert/update/delete trigger function.
 * @return : true when statement and the triggered statement are co-location.
 */
bool plpgsql_check_colocate(Query* query, RangeTblEntry* rte, void* plpgsql_func)
{
    List* query_partAttrNum = NIL;
    List* trig_partAttrNum = NIL;
    Relation qe_relation = NULL; /* triggering query's rel */
    Relation tg_relation = NULL;
    RelationLocInfo* qe_rel_loc_info = NULL; /* triggering query's rel loc info */
    RelationLocInfo* tg_rel_loc_info = NULL; /* trigger body's rel loc info */
    int qe_rel_nodelist_len = 0;
    int tg_rel_nodelist_len = 0;

    Assert(query->commandType == CMD_INSERT || query->commandType == CMD_DELETE || query->commandType == CMD_UPDATE);

    PLpgSQL_function* func = (PLpgSQL_function*)plpgsql_func;

    /* Get event query relation and trigger body's relation. */
    qe_relation = func->tg_relation;
    tg_relation = relation_open(rte->relid, AccessShareLock);

    query_partAttrNum = qe_relation->rd_locator_info->partAttrNum;
    trig_partAttrNum = rte->partAttrNum;

    /* Check if trigger query table and trigger body table are on the same node list. */
    qe_rel_loc_info = qe_relation->rd_locator_info;
    tg_rel_loc_info = tg_relation->rd_locator_info;

    qe_rel_nodelist_len = list_length(qe_rel_loc_info->nodeList);
    tg_rel_nodelist_len = list_length(tg_rel_loc_info->nodeList);

    /* Cannot ship whenever target table is not row type. */
    if (!RelationIsRowFormat(tg_relation)) {
        relation_close(tg_relation, AccessShareLock);
        return false;
    }

    /* The query table and trigger table must in a same group. */
    if (0 != strcmp(qe_rel_loc_info->gname.data, tg_rel_loc_info->gname.data)) {
        relation_close(tg_relation, AccessShareLock);
        return false;
    }
    relation_close(tg_relation, AccessShareLock);

    /* If distribution key list lengths are different they both are not colocated. */
    if (list_length(trig_partAttrNum) != list_length(query_partAttrNum)) {
        return false;
    }

    /*
     * Used difference check function between INSERT and UPDATE/DELETE here because we use
     * targetlist to check INSERT and where clause to check UPDATE/DELETE.
     */
    if (query->commandType == CMD_UPDATE || query->commandType == CMD_DELETE) {
        return plpgsql_check_updel_colocate(query, query_partAttrNum, trig_partAttrNum, func);
    } else {
        return plpgsql_check_insert_colocate(query, query_partAttrNum, trig_partAttrNum, func);
    }
}


bool check_search_path_interface(List *schemas, HeapTuple proc_tup)
{
    if (!SUPPORT_BIND_SEARCHPATH) {
        return true;
    }    
    bool isOidListSame = true;
    Form_pg_proc proc_struct = (Form_pg_proc)GETSTRUCT(proc_tup);
    if (proc_struct->pronamespace != PG_CATALOG_NAMESPACE) {
        /* Get lastest search_path if baseSearchPathValid is false */
        recomputeNamespacePath();

        int len1 = list_length(u_sess->catalog_cxt.baseSearchPath);
        /*   
         * The first element of func->fn_searchpath->schemas is
         * namespace the current function belongs to.
         */
        int len2 = list_length(schemas) - 1;
        Assert(len2 >= 0);

        if (len1 == len2) {
            ListCell* lc1 = NULL;
            ListCell* lc2 = NULL;

            /* Check whether search_path has changed */
            lc1 = list_head(u_sess->catalog_cxt.baseSearchPath);
            lc2 = list_head(schemas);

            /* Check function schema from list second position to list tail */
            lc2 = lnext(lc2);
            for (; lc1 && lc2; lc1 = lnext(lc1), lc2 = lnext(lc2)) {
                if (lfirst_oid(lc1) != lfirst_oid(lc2)) {
                    isOidListSame = false;
                    break;
                }
            }
        } else if (len1 == list_length(schemas)) {
            /* in some case, same length maybe same oid list */
            ListCell* lc1 = NULL;
            foreach(lc1, schemas) {
                if (!list_member_oid(u_sess->catalog_cxt.baseSearchPath, lfirst_oid(lc1))) {
                    isOidListSame = false;
                    break;
                }
            }
        } else {
            /* If length is different, two lists are different. */
            isOidListSame = false;
        }
    }

    return isOidListSame;
}


void plpgsql_pkg_HashTableDelete(PLpgSQL_package* pkg)
{
    plpgsql_pkg_HashEnt* hentry = NULL;
    /* do nothing if not in table */
    if (pkg->pkg_hashkey == NULL) {
        return;
    }
    hentry = (plpgsql_pkg_HashEnt*)hash_search(
        u_sess->plsql_cxt.plpgsql_pkg_HashTable, (void*)pkg->pkg_hashkey, HASH_REMOVE, NULL);
    if (hentry == NULL) {
        elog(WARNING, "trying to delete function that does not exist");
    } else {
        /* delete the cell from the list. */
        u_sess->plsql_cxt.plpgsqlpkg_dlist_objects =
        dlist_delete_cell(u_sess->plsql_cxt.plpgsqlpkg_dlist_objects, hentry->cell, false);
    }
    /* remove back link, which no longer points to allocated storage */
    pkg->pkg_hashkey = NULL;
}

void delete_package(PLpgSQL_package* pkg)
{
    if (pkg->use_count > 0)
        return;
    ListCell* l = NULL;
    foreach(l, pkg->proc_compiled_list) {
        if (((PLpgSQL_function*)lfirst(l))->use_count > 0)
            return;
    }
    foreach(l, pkg->proc_compiled_list) {
        PLpgSQL_function* func = (PLpgSQL_function*)lfirst(l);
        delete_function(func, true);
    }
    /* free package memory,*/
    plpgsql_pkg_HashTableDelete(pkg);
    plpgsql_free_package_memory(pkg);
}

static void plpgsql_pkg_append_dlcell(plpgsql_pkg_HashEnt* entity)
{
    MemoryContext oldctx;
    PLpgSQL_package* pkg = NULL;
    oldctx = MemoryContextSwitchTo(SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_OPTIMIZER));
    u_sess->plsql_cxt.plpgsqlpkg_dlist_objects = dlappend(u_sess->plsql_cxt.plpgsqlpkg_dlist_objects, entity);
    (void)MemoryContextSwitchTo(oldctx);

    entity->cell = u_sess->plsql_cxt.plpgsqlpkg_dlist_objects->tail;
    while (dlength(u_sess->plsql_cxt.plpgsqlpkg_dlist_objects) > g_instance.attr.attr_sql.max_compile_functions) {
        DListCell* headcell = u_sess->plsql_cxt.plpgsqlpkg_dlist_objects->head;
        plpgsql_pkg_HashEnt* head_entity = (plpgsql_pkg_HashEnt*)lfirst(headcell);

        pkg = head_entity->package;

        /* delete from the hash and delete the function's compile */
        CheckCurrCompileDependOnPackage(pkg->pkg_oid);
        delete_package(pkg);
        pfree_ext(pkg);
    }
}


void delete_pkg_in_HashTable(Oid pkgOid)
{
    PLpgSQL_pkg_hashkey hashkey;
    plpgsql_pkg_HashEnt* hentry = NULL;
    hashkey.pkgOid = pkgOid;
    bool found = false;
    hentry = (plpgsql_pkg_HashEnt*)hash_search(u_sess->plsql_cxt.plpgsql_pkg_HashTable, &hashkey, HASH_REMOVE, NULL); 
    if (found) {
        u_sess->plsql_cxt.plpgsqlpkg_dlist_objects =
            dlist_delete_cell(u_sess->plsql_cxt.plpgsqlpkg_dlist_objects, hentry->cell, false);
    }
}

static void plpgsql_pkg_HashTableInsert(PLpgSQL_package* pkg, PLpgSQL_pkg_hashkey* pkg_key)
{
    plpgsql_pkg_HashEnt* hentry = NULL;
    bool found = false;
    hentry = (plpgsql_pkg_HashEnt*)hash_search(u_sess->plsql_cxt.plpgsql_pkg_HashTable, (void*)pkg_key, HASH_ENTER, &found);
    if (found) {
        /* move cell to the tail of the package list. */
        dlist_add_tail_cell(u_sess->plsql_cxt.plpgsqlpkg_dlist_objects, hentry->cell);
        elog(WARNING, "trying to insert a package that already exists");
    } else {
        /* append the current compiling entity to the end of the compile results list. */
        plpgsql_pkg_append_dlcell(hentry);
    }
    hentry->package = pkg;
    /* prepare back link from function to hashtable key */
    pkg->pkg_hashkey = &hentry->key;
}

extern PLpgSQL_package* plpgsql_pkg_HashTableLookup(PLpgSQL_pkg_hashkey* pkg_key)
{
    if (unlikely(u_sess->plsql_cxt.plpgsql_pkg_HashTable == NULL))
        return NULL;
    plpgsql_pkg_HashEnt* hentry = NULL;
    hentry = (plpgsql_pkg_HashEnt*)hash_search(u_sess->plsql_cxt.plpgsql_pkg_HashTable, (void*)pkg_key, HASH_FIND, NULL);
    if (hentry != NULL) {
        /* add cell to the tail of the function list. */
        dlist_add_tail_cell(u_sess->plsql_cxt.plpgsqlpkg_dlist_objects, hentry->cell);
        return hentry->package;
    } else {
        return NULL;
    }
}

static PLpgSQL_package* do_pkg_compile(Oid pkgOid, HeapTuple pkg_tup, PLpgSQL_package* pkg,
    PLpgSQL_pkg_hashkey* hashkey, bool isSpec, bool isCreate)
{
    Form_gs_package pkg_struct = (Form_gs_package)GETSTRUCT(pkg_tup);
    Datum pkgsrcdatum;
    Datum pkginitdatum;
    bool isnull = false;
    char* pkg_source = NULL;
    char* pkg_init_source = NULL;
    int i;
    ErrorContextCallback pl_err_context;
    int parse_rc;
    Oid* saved_pseudo_current_userId = NULL;
    char* signature = NULL;
    List* current_searchpath = NIL;
    char* namespace_name = NULL;
    char context_name[NAMEDATALEN] = {0};
    int rc = 0;
    const int alloc_size = 256;
    Datum namespaceOidDatum;
    Oid namespaceOid = InvalidOid;
    /*
     * Setup the scanner input and error info.    We assume that this function
     * cannot be invoked recursively, so there's no need to save and restore
     * the static variables used here.
     */
    if (isSpec) {
        pkgsrcdatum = SysCacheGetAttr(PACKAGEOID, pkg_tup, Anum_gs_package_pkgspecsrc, &isnull);
    } else {
        pkgsrcdatum = SysCacheGetAttr(PACKAGEOID, pkg_tup, Anum_gs_package_pkgbodydeclsrc, &isnull);
    }
    
    if (isnull) {
        ereport(ERROR, (errmodule(MOD_PLSQL), errcode(ERRCODE_UNDEFINED_OBJECT),
                errmsg("The definition of the package is null"),
                errhint("Check whether the definition of the function is complete in the pg_proc system table.")));
    }
   
    if (!isSpec) {
        pkginitdatum = SysCacheGetAttr(PACKAGEOID, pkg_tup, Anum_gs_package_pkgbodyinitsrc, &isnull);
    } else {
        isnull = true;
    }
    if (isnull) {
        pkg_init_source = "INSTANTIATION DECLARE BEGIN NULL; END";
    } else if (!isnull && !isSpec) {
        pkg_init_source = TextDatumGetCString(pkginitdatum);
    }
    pkg_source = TextDatumGetCString(pkgsrcdatum);
    /*
     * Setup error traceback support for ereport()
     */
    pl_err_context.callback = plpgsql_compile_error_callback;
    pl_err_context.arg = NULL;
    pl_err_context.previous = t_thrd.log_cxt.error_context_stack;
    t_thrd.log_cxt.error_context_stack = &pl_err_context;
    signature = pstrdup(NameStr(pkg_struct->pkgname));
    /*
     * All the permanent output of compilation (e.g. parse tree) is kept in a
     * per-function memory context, so it can be reclaimed easily.
     */
    rc = snprintf_s(
        context_name, NAMEDATALEN, NAMEDATALEN - 1, "%s_%lu", "PL/pgSQL package context", u_sess->debug_query_id);
    securec_check_ss(rc, "", "");
    /*
     * Create the new function struct, if not done already.  The function
     * structs are never thrown away, so keep them in session memory context.
     */
    PLpgSQL_compile_context* curr_compile = createCompileContext(context_name);
    SPI_NESTCOMPILE_LOG(curr_compile->compile_cxt);
    bool pkg_is_null = false;
    if (pkg == NULL) {
        pkg = (PLpgSQL_package*)MemoryContextAllocZero(
            SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_OPTIMIZER), sizeof(PLpgSQL_package));
        pkg->pkg_cxt = curr_compile->compile_cxt;
        pkg->pkg_signature = pstrdup(signature);
        pkg->pkg_owner = pkg_struct->pkgowner;
        pkg->pkg_oid = pkgOid;
        pkg->pkg_tid = pkg_tup->t_self;
        pkg->proc_list = NULL;
        pkg->invalItems = NIL;
        pkg->use_count = 0;
        pkg_is_null = true;
    }
    saved_pseudo_current_userId = u_sess->misc_cxt.Pseudo_CurrentUserId;
    u_sess->misc_cxt.Pseudo_CurrentUserId = &pkg->pkg_owner;
    pkg->is_spec_compiling = isSpec;
    MemoryContext temp = NULL;
    if (u_sess->plsql_cxt.curr_compile_context != NULL) {
        checkCompileMemoryContext(u_sess->plsql_cxt.curr_compile_context->compile_tmp_cxt);
        temp = MemoryContextSwitchTo(u_sess->plsql_cxt.curr_compile_context->compile_tmp_cxt);
    }
    u_sess->plsql_cxt.curr_compile_context = curr_compile;
    pushCompileContext();
    plpgsql_scanner_init(pkg_source);
    curr_compile->plpgsql_error_pkgname = pstrdup(NameStr(pkg_struct->pkgname));

    namespaceOidDatum = SysCacheGetAttr(PACKAGEOID, pkg_tup, Anum_gs_package_pkgnamespace, &isnull);
    if (!isnull) {
       namespaceOid = DatumGetObjectId(namespaceOidDatum);
    }
    if (OidIsValid(namespaceOid)) {
        pkg->namespaceOid = namespaceOid;
    } else {
        pkg->namespaceOid = InvalidOid;
    }
    pkg->is_spec_compiling = isSpec;
    if (isSpec) {
        u_sess->plsql_cxt.plpgsql_IndexErrorVariable = 0;
    }
    /*
     * compile_tmp_cxt is a short temp context that will be detroyed after
     * function compile or execute.
     * func_cxt is a long term context that will stay until thread exit. So
     * malloc on func_cxt should be very careful.
     * signature is stored on a StringInfoData which is 1K byte at least, but
     * most signature will not be so long originally, so we should do a strdup.
     */
    curr_compile->compile_tmp_cxt = MemoryContextSwitchTo(pkg->pkg_cxt);
    if (enable_plpgsql_gsdependency() && isSpec && pkg_is_null) {
        gsplsql_prepare_gs_depend_for_pkg_compile(pkg, isCreate);
    }
    pkg->pkg_signature = pstrdup(signature);
    pkg->pkg_searchpath = (OverrideSearchPath*)palloc0(sizeof(OverrideSearchPath));
    pkg->pkg_searchpath->addCatalog = true;
    pkg->pkg_searchpath->addTemp = true;
    pkg->pkg_xmin = HeapTupleGetRawXmin(pkg_tup);
    pkg->proc_compiled_list = NULL;
    curr_compile->plpgsql_curr_compile_package = pkg;
    if (pkg_struct->pkgnamespace == PG_CATALOG_NAMESPACE) {
        current_searchpath = fetch_search_path(false);
        if (current_searchpath == NIL) {
            ereport(ERROR, (errmodule(MOD_PLSQL), errcode(ERRCODE_UNDEFINED_SCHEMA),
                    errmsg("the search_path is empty while the porc belongs to pg_catalog ")));
        }
        namespace_name = get_namespace_name(linitial_oid(current_searchpath));
        if (namespace_name == NULL) {
            list_free_ext(current_searchpath);
            ereport(ERROR, (errmodule(MOD_PLSQL), errcode(ERRCODE_UNDEFINED_SCHEMA),
                    errmsg("cannot find the namespace according to search_path")));
        }
        pkg->pkg_searchpath->schemas = current_searchpath;
    } else {
        /* Assign namespace of current function to fn_searchpath */
        pkg->pkg_searchpath->schemas = list_make1_oid(pkg_struct->pkgnamespace);
        if (SUPPORT_BIND_SEARCHPATH) {
            /*
             * If SUPPORT_BIND_SEARCHPATH is true,
             * add system's search_path to fn_searchpath.
             * When the relation of other objects cannot be
             * found in the namespace of current function,
             * find them in search_path list.
             * Otherwise, we only find objects in the namespace
             * of current function.
             */
            ListCell* l = NULL;
            /* If u_sess->catalog_cxt.namespaceUser and roleid are not equeal,
             * then u_sess->catalog_cxt.baseSearchPath doesn't
             * contain currentUser schema.currenUser schema will be added in
             * PushOverrideSearchPath.
             * 
             * It can happen executing following statements.
             * 
             * create temp table t1(a int);
             * \d t1                     --(get schema pg_temp_xxx)
             * drop table t1;
             * drop schema pg_temp_xxx cascade;
             * call proc1()              --(proc1 contains create temp table statement)
             */
            Oid roleid = GetUserId();
            if (u_sess->catalog_cxt.namespaceUser != roleid) {
                pkg->pkg_searchpath->addUser = true;
            }
            /* Use baseSearchPath not activeSearchPath. */
            foreach (l, u_sess->catalog_cxt.baseSearchPath) {
                Oid namespaceId = lfirst_oid(l);
                /*
                 * Append namespaceId to fn_searchpath directly.
                 */
                pkg->pkg_searchpath->schemas = lappend_oid(pkg->pkg_searchpath->schemas, namespaceId);
            }
        }
    }
    pfree_ext(signature);
    curr_compile->plpgsql_curr_compile_package->proc_compiled_list = NULL;
    /*
     * Initialize the compiler, particularly the namespace stack.  The
     * outermost namespace contains function parameters and other special
     * variables (such as FOUND), and is named after the function itself.
     */
    curr_compile->datums_pkg_alloc = alloc_size;
    curr_compile->plpgsql_pkg_nDatums = 0;
    /* This is short-lived, so needn't allocate in function's cxt */
    curr_compile->plpgsql_Datums = (PLpgSQL_datum**)MemoryContextAlloc(
        curr_compile->compile_tmp_cxt, sizeof(PLpgSQL_datum*) * curr_compile->datums_pkg_alloc);
    curr_compile->datum_need_free = (bool*)MemoryContextAlloc(
        curr_compile->compile_tmp_cxt, sizeof(bool) * curr_compile->datums_pkg_alloc);
    curr_compile->datums_last = 0;
    PushOverrideSearchPath(pkg->pkg_searchpath);
    plpgsql_ns_init();
    plpgsql_ns_push(NameStr(pkg_struct->pkgname));
    add_pkg_compile();
    curr_compile->datums_last = curr_compile->plpgsql_nDatums;
    curr_compile->plpgsql_pkg_DumpExecTree = false;
    /*
     * Now parse the function's text
     */
    bool saved_flag = u_sess->plsql_cxt.have_error;
    u_sess->plsql_cxt.have_error = false;
    parse_rc = plpgsql_yyparse();
    if (parse_rc != 0) {
        ereport(ERROR,
            (errmodule(MOD_PLSQL),
                errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                errmsg("Syntax parsing error, plpgsql parser returned %d", parse_rc)));
    }
    plpgsql_scanner_finish();
    pfree_ext(pkg_source);
    
    if (pkg_init_source != NULL) {
        plpgsql_scanner_init(pkg_init_source);
        parse_rc = plpgsql_yyparse();
        if (parse_rc != 0) {
            ereport(ERROR,
                (errmodule(MOD_PLSQL),
                    errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                    errmsg("Syntax parsing error, plpgsql parser returned %d", parse_rc)));
        }
        plpgsql_scanner_finish();
    }
    
    PopOverrideSearchPath();
    u_sess->misc_cxt.Pseudo_CurrentUserId = saved_pseudo_current_userId;
#ifndef ENABLE_MULTIPLE_NODES
    if (u_sess->plsql_cxt.have_error && u_sess->attr.attr_common.plsql_show_all_error) {
        u_sess->plsql_cxt.have_error = false;
        ereport(ERROR,
            (errmodule(MOD_PLSQL),
                errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("Debug mod,create procedure has error."),
                errdetail("N/A"),
                errcause("compile procedure error."),
                erraction("check procedure error and redefine procedure")));  
    }
#endif
    u_sess->plsql_cxt.have_error = saved_flag;
    pkg->ndatums = curr_compile->plpgsql_pkg_nDatums;
    pkg->datums = (PLpgSQL_datum**)palloc(sizeof(PLpgSQL_datum*) * curr_compile->plpgsql_pkg_nDatums);
    pkg->datum_need_free = (bool*)palloc(sizeof(bool) * curr_compile->plpgsql_pkg_nDatums);
    pkg->datums_alloc = pkg->ndatums;
    for (i = 0; i < curr_compile->plpgsql_pkg_nDatums; i++) {
        pkg->datums[i] = curr_compile->plpgsql_Datums[i];
        pkg->datum_need_free[i] = curr_compile->datum_need_free[i];
        if (pkg->datums[i]->dtype == PLPGSQL_DTYPE_VAR) {
            PLpgSQL_var* var = reinterpret_cast<PLpgSQL_var*>(pkg->datums[i]);
            if (var->pkg == NULL) {
                var->pkg = pkg;
                var->pkg_name = GetPackageListName(NameStr(pkg_struct->pkgname), namespaceOid);
            }
        } else if (pkg->datums[i]->dtype == PLPGSQL_DTYPE_ROW) {
            PLpgSQL_row* row = (PLpgSQL_row*)pkg->datums[i];
            if (row->pkg == NULL) {
                row->pkg = pkg;
                row->pkg_name = GetPackageListName(NameStr(pkg_struct->pkgname), namespaceOid);
            }
        } else if (pkg->datums[i]->dtype == PLPGSQL_DTYPE_RECORD) {
            PLpgSQL_row* row = (PLpgSQL_row*)pkg->datums[i];
            if (row->pkg == NULL) {
                row->pkg = pkg;
                row->pkg_name = GetPackageListName(NameStr(pkg_struct->pkgname), namespaceOid);
            }
        } else if (pkg->datums[i]->dtype == PLPGSQL_DTYPE_REC) {
            PLpgSQL_rec* rec = (PLpgSQL_rec*)pkg->datums[i];
            if (rec->pkg == NULL) {
                rec->pkg = pkg;
                rec->pkg_name = GetPackageListName(NameStr(pkg_struct->pkgname), namespaceOid);
            }       
        }
    }
    
    if (isSpec) {
        pkg->public_ns = curr_compile->ns_top;
        pkg->is_bodycompiled = false;
        pkg->public_ndatums = curr_compile->plpgsql_pkg_nDatums;
    } else  {
        pkg->private_ns = curr_compile->ns_top;
        pkg->is_bodycompiled = true;
    }
    pkg->proc_list = curr_compile->plpgsql_curr_compile_package->proc_list;
    if (!isSpec) {
        pkg->is_bodycompiled = true;
    } 
    MemoryContext oldcxt = MemoryContextSwitchTo(pkg->pkg_cxt);
    pkg->proc_compiled_list = curr_compile->plpgsql_curr_compile_package->proc_compiled_list;
    if (hashkey && isSpec) {
        plpgsql_pkg_HashTableInsert(pkg, hashkey);
    }
    t_thrd.log_cxt.error_context_stack = pl_err_context.previous;
    curr_compile->plpgsql_error_funcname = NULL;
    curr_compile->plpgsql_check_syntax = false;
    MemoryContextSwitchTo(oldcxt);
    MemoryContextSwitchTo(curr_compile->compile_tmp_cxt);
    curr_compile->compile_tmp_cxt = NULL;
    curr_compile->plpgsql_curr_compile = NULL;
    ereport(DEBUG3, (errmodule(MOD_NEST_COMPILE), errcode(ERRCODE_LOG),
        errmsg("%s finish compile, level: %d", __func__, list_length(u_sess->plsql_cxt.compile_context_list))));
    u_sess->plsql_cxt.curr_compile_context = popCompileContext();
    clearCompileContext(curr_compile);
    if (temp != NULL) {
        MemoryContextSwitchTo(temp);
    }
    return pkg;
}

List* GetPackageListName(const char* pkgName, const Oid nspOid)
{
    StringInfoData nameData;
    List* nameList = NULL;
    initStringInfo(&nameData);
    char* schemaName = get_namespace_name(nspOid);
    if (schemaName == NULL) {
        ereport(ERROR,
            (errmodule(MOD_PLSQL), errcode(ERRCODE_PLPGSQL_ERROR),
             errmsg("failed to find package schema name"),
             errdetail("when compile package \"%s\", failed to find its schema name,"\
                       "the schema maybe dropped by other session", pkgName),
             errcause("excessive concurrency"),
             erraction("reduce concurrency and retry")));
    }
    appendStringInfoString(&nameData, schemaName);
    appendStringInfoString(&nameData, ".");
    appendStringInfoString(&nameData, pkgName);
    nameList = stringToQualifiedNameList(nameData.data);
    pfree_ext(nameData.data);
    return nameList;
}

/*
 * compile and init package by package oid
 */
PLpgSQL_package* plpgsql_pkg_compile(Oid pkgOid, bool for_validator, bool isSpec, bool isCreate, bool isRecompile)
{
#ifdef ENABLE_MULTIPLE_NODES
        ereport(ERROR, (errcode(ERRCODE_INVALID_PACKAGE_DEFINITION),
                    errmsg("not support create package in distributed database")));
#endif
    HeapTuple pkg_tup = NULL;
    Form_gs_package pkg_struct = NULL;
    PLpgSQL_package* pkg = NULL;
    PLpgSQL_pkg_hashkey hashkey;
    bool pkg_valid = false;
    bool pkg_spec_valid = true;
    bool pkg_body_valid = true;
    /*
     * Lookup the gs_package tuple by Oid; we'll need it in any case
     */
    pkg_tup = SearchSysCache1(PACKAGEOID, ObjectIdGetDatum(pkgOid));
    AclResult aclresult = pg_package_aclcheck(pkgOid, GetUserId(), ACL_EXECUTE);
    Form_gs_package pkgForm = (Form_gs_package)GETSTRUCT(pkg_tup);
    NameData pkgname = pkgForm->pkgname;
    if (aclresult != ACLCHECK_OK) {
        aclcheck_error(aclresult, ACL_KIND_PACKAGE, pkgname.data);
    }
    if (!HeapTupleIsValid(pkg_tup)) {
        ereport(ERROR,  (errmodule(MOD_PLSQL),  errcode(ERRCODE_CACHE_LOOKUP_FAILED),
                errmsg("cache lookup failed for package %u, while compile package", pkgOid)));
    }
    pkg_struct = (Form_gs_package)GETSTRUCT(pkg_tup);
    hashkey.pkgOid = pkgOid;
    if (enable_plpgsql_gsdependency_guc()) {
        if (isCreate) {
            u_sess->plsql_cxt.need_create_depend = true;
        } else {
            /**
             * only check for pkg need recompile or not,
             * flag u_sess->plsql_cxt.need_create_depend would be set under ddl
             */
            if (isSpec) {
                pkg_spec_valid = GetPgObjectValid(pkgOid, OBJECT_TYPE_PKGSPEC);
            } else {
                pkg_body_valid = GetPgObjectValid(pkgOid, OBJECT_TYPE_PKGBODY);
            }
            if ((!pkg_body_valid || !pkg_spec_valid) && !u_sess->plsql_cxt.need_create_depend && !isRecompile) {
                gsplsql_do_autonomous_compile(pkgOid, true);
            }
        }
    }
#ifndef ENABLE_MULTIPLE_NODES
    /* Locking is performed before compilation. */
    if (u_sess->SPI_cxt._connected >= 0) {
        gsplsql_lock_func_pkg_dependency_all(pkgOid, PLSQL_PACKAGE_OBJ);
    }
#endif
    pkg = plpgsql_pkg_HashTableLookup(&hashkey);
    if ((!pkg_body_valid || !pkg_spec_valid) && pkg != NULL &&
         !u_sess->plsql_cxt.need_create_depend && u_sess->SPI_cxt._connected >= 0 &&
         !isRecompile && !u_sess->plsql_cxt.during_compile) {
        pkg->is_need_recompile = true;
    }
    bool pkg_status = true;
    if (pkg != NULL) {
        Assert(pkg->pkg_oid == pkgOid);
        pkg_status = pkg->pkg_xmin == HeapTupleGetRawXmin(pkg_tup) &&
            ItemPointerEquals(&pkg->pkg_tid, &pkg_tup->t_self) && !isRecompile && !pkg->is_need_recompile;
        if (pkg_status) {
                pkg_valid = true;
        } else {
            /* need reuse pkg slot in hash table later, we need clear all refcount for this pkg and delete it here */
            delete_package_and_check_invalid_item(pkgOid);
            pkg_valid = false;
        }
    }
    PLpgSQL_compile_context* save_compile_context = u_sess->plsql_cxt.curr_compile_context;
    Oid old_value = saveCallFromPkgOid(pkgOid);
    int oidCompileStatus = getCompileStatus();
    bool save_need_create_depend = u_sess->plsql_cxt.need_create_depend;
    bool save_curr_status = GetCurrCompilePgObjStatus();
    bool save_is_pkg_compile = u_sess->plsql_cxt.is_pkg_compile;
    PG_TRY();
    {
        SetCurrCompilePgObjStatus(true);
        if (!pkg_valid) {
            pkg = NULL;
            pkg = do_pkg_compile(pkgOid, pkg_tup, pkg, &hashkey, true, isCreate);
            gsplsql_pkg_set_status_in_mem(pkg, true, GetCurrCompilePgObjStatus());
#ifndef ENABLE_MULTIPLE_NODES
            PackageInit(pkg, isCreate, isSpec);
#endif
            if (!isSpec && pkg != NULL) {
                gsplsql_pkg_set_status(pkg, true, false, GetCurrCompilePgObjStatus(), false,
                        pkg_spec_valid, pkg_body_valid);
                pkg = do_pkg_compile(pkgOid, pkg_tup, pkg, &hashkey, false, isCreate);
                if (pkg == NULL) {
                    ereport(ERROR,  (errmodule(MOD_PLSQL),  errcode(ERRCODE_CACHE_LOOKUP_FAILED),
                        errmsg("package %u not found", pkgOid)));
                }
                gsplsql_pkg_set_status_in_mem(pkg, false, GetCurrCompilePgObjStatus());
                ReleaseSysCache(pkg_tup);
                pkg_tup = NULL;
#ifndef ENABLE_MULTIPLE_NODES
                PackageInit(pkg, isCreate, isSpec);
#endif
            } else if(!isSpec && pkg == NULL) {
                ereport(ERROR,  (errmodule(MOD_PLSQL),  errcode(ERRCODE_CACHE_LOOKUP_FAILED),
                    errmsg("package spec %u not found", pkgOid)));
            }
        } else {
            if (!pkg->is_bodycompiled && !isSpec) {
                pkg = do_pkg_compile(pkgOid, pkg_tup, pkg, &hashkey, false, isCreate);
                gsplsql_pkg_set_status_in_mem(pkg, false, GetCurrCompilePgObjStatus());
            }  else {
                if (pkg != NULL) {
                    bool temp_pkg_valid = gsplsql_pkg_get_valid(pkg, isSpec);
                    UpdateCurrCompilePgObjStatus(temp_pkg_valid);
                    if (!temp_pkg_valid) {
                        ereport(LOG, (errmodule(MOD_PLSQL), errcode(ERRCODE_UNDEFINED_OBJECT),
                                      errmsg("In nested compilation, package %s is invalid.", pkg->pkg_signature)));
                    }
                }
            }
            /* package must be compiled befor init */
#ifndef ENABLE_MULTIPLE_NODES
            if (pkg != NULL) {
                PackageInit(pkg, isCreate, isSpec);
            } else {
                ereport(ERROR,  (errmodule(MOD_PLSQL),  errcode(ERRCODE_CACHE_LOOKUP_FAILED),
                    errmsg("package spec %u not found", pkgOid))); 
            }
#endif
        }
        u_sess->plsql_cxt.need_create_depend = save_need_create_depend;
        u_sess->plsql_cxt.is_pkg_compile = save_is_pkg_compile;
    }
    PG_CATCH();
    {
        SetCurrCompilePgObjStatus(save_curr_status);
        u_sess->plsql_cxt.need_create_depend = save_need_create_depend;
        u_sess->plsql_cxt.is_pkg_compile = save_is_pkg_compile;
#ifndef ENABLE_MULTIPLE_NODES
        bool insertError = u_sess->attr.attr_common.plsql_show_all_error ||
                                !u_sess->attr.attr_sql.check_function_bodies;
        if (insertError) {
            InsertError(pkgOid);
        }
        popToOldCompileContext(save_compile_context);
        CompileStatusSwtichTo(oidCompileStatus);
#endif
        PG_RE_THROW();
    }
    PG_END_TRY();
    if (enable_plpgsql_gsdependency_guc() && !pkg_valid) {
        gsplsql_complete_gs_depend_for_pkg_compile(pkg, isCreate, isRecompile);
    }
    if (HeapTupleIsValid(pkg_tup)) {
        ReleaseSysCache(pkg_tup);
        pkg_tup = NULL;
    }
    restoreCallFromPkgOid(old_value);
    gsplsql_pkg_set_status(pkg, isSpec, isCreate, GetCurrCompilePgObjStatus(), isRecompile,
                           pkg_spec_valid, pkg_body_valid);
    UpdateCurrCompilePgObjStatus(save_curr_status);
    pkg->is_need_recompile = false;
    /*
     * Finally return the compiled function
     */
    return pkg;
}


/*
 * error context callback to let us supply a call-stack traceback.
 * If we are validating or executing an anonymous code block, the function
 * source text is passed as an argument.
 */
void plpgsql_compile_error_callback(void* arg)
{
    if (arg != NULL) {
        /*
         * Try to convert syntax error position to reference text of original
         * CREATE FUNCTION or DO command.
         */
        if (function_parse_error_transpose((const char*)arg)) {
            return;
        }
        /*
         * Done if a syntax error position was reported; otherwise we have to
         * fall back to a "near line N" report.
         */
    }
    int rc = CompileWhich();
    if (rc == PLPGSQL_COMPILE_PROC) {
        if (u_sess->plsql_cxt.curr_compile_context != NULL &&
            u_sess->plsql_cxt.curr_compile_context->plpgsql_error_funcname) {
            errcontext("compilation of PL/pgSQL function \"%s\" near line %d",
                u_sess->plsql_cxt.curr_compile_context->plpgsql_error_funcname, plpgsql_latest_lineno());
        }
    } else if (rc != PLPGSQL_COMPILE_NULL) {
        errcontext("compilation of PL/pgSQL package near line %d",
            plpgsql_latest_lineno());
    }
}

Oid findPackageParameter(const char* objname) 
{
    Oid toid = InvalidOid;
    if (u_sess->plsql_cxt.curr_compile_context != NULL &&
        u_sess->plsql_cxt.curr_compile_context->plpgsql_curr_compile_package != NULL) {
        PLpgSQL_package* pkg = u_sess->plsql_cxt.curr_compile_context->plpgsql_curr_compile_package;
        PLpgSQL_nsitem* ns = NULL;
        ns = plpgsql_ns_lookup(pkg->public_ns, false, objname, NULL, NULL, NULL);
        if (ns == NULL) {
            ns = plpgsql_ns_lookup(pkg->private_ns, false, objname, NULL, NULL, NULL);
        }
        if (ns != NULL) {
            switch (ns->itemtype) {
                case PLPGSQL_NSTYPE_REFCURSOR:
                    toid = REFCURSOROID;
                    break;
                case PLPGSQL_NSTYPE_RECORD:
                    toid = RECORDOID;
                    break;
                case PLPGSQL_NSTYPE_VAR: {
                    PLpgSQL_var* var = (PLpgSQL_var*)pkg->datums[ns->itemno];
                    if (var->datatype->typoid == REFCURSOROID && OidIsValid(var->datatype->cursorCompositeOid)) {
                        toid = var->datatype->cursorCompositeOid;
                    } else {
                        toid = InvalidOid;
                    }
                    break;
                }
                default:
                    toid = InvalidOid;
            }
        }
    }
    return toid;
}

int GetLineNumber(const char* procedureStr, int loc)
{
#ifdef ENABLE_MULTIPLE_NODES
    return 0;
#endif 
    int lines = 1;
    int jumpWords = 0;
    if (procedureStr == nullptr) {
        return 0;
    }
    if (!strncmp(procedureStr, " DECLARE ", strlen(" DECLARE "))) {
        if (strlen(procedureStr) > strlen(" DECLARE ")) {
            if (procedureStr[strlen(" DECLARE ")] == '\n') {
                jumpWords = strlen(" DECLARE ");
            }
        }
    } else if (!strncmp(procedureStr, " PACKAGE  DECLARE ", strlen(" PACKAGE  DECLARE "))) {
        if (strlen(procedureStr) > strlen(" PACKAGE  DECLARE ")) {
            if (procedureStr[strlen(" PACKAGE  DECLARE ")] == '\n') {
                jumpWords = strlen(" PACKAGE  DECLARE ");
            }
        }
    }
    if (procedureStr == NULL || loc < 0) {
        return 0;
    }
    if (jumpWords > loc) {
        return 0;
    }
    int i = jumpWords;
    while (i-- >= 0) {
        procedureStr++;
    }
    for (int i = jumpWords; i < loc; i++) {
        if (*procedureStr == '\n') {
            lines++;
        }
        procedureStr++;
    }
    return lines;
}

/*
    get the correct line number in package or procedure,it's line number
    start with "Create"
*/
int GetProcedureLineNumberInPackage(const char* procedureStr, int loc) 
{
#ifndef ENABLE_MULTIPLE_NODES
    if (!u_sess->attr.attr_common.plsql_show_all_error) {
        return 0;
    }
#else 
    return 0;
#endif
    int lines = GetLineNumber(procedureStr, loc);
    int rc = CompileWhich();
    if (rc == PLPGSQL_COMPILE_PACKAGE_PROC) {
        lines = u_sess->plsql_cxt.package_first_line + u_sess->plsql_cxt.procedure_start_line + u_sess->plsql_cxt.procedure_first_line + lines - 3;
        return lines > 0 ? lines : 1;
    } else if (rc == PLPGSQL_COMPILE_PACKAGE) {
        if (u_sess->plsql_cxt.procedure_start_line > 0) {
            lines = u_sess->plsql_cxt.procedure_start_line + u_sess->plsql_cxt.package_first_line - 1;
            return lines > 0 ? lines : 1;
        } else {
            if (lines <= 1) {
                lines = u_sess->plsql_cxt.package_first_line; 
            } else {
                lines = u_sess->plsql_cxt.package_first_line + lines - 1; 
            }
            return lines > 0 ? lines : 1;
        }
    }
    else if (rc == PLPGSQL_COMPILE_PROC) {
        lines = u_sess->plsql_cxt.procedure_first_line + lines;
        return lines > 0 ? lines : 1;
    } else {
        ereport(ERROR,
            (errmodule(MOD_PLSQL), errcode(ERRCODE_UNDEFINED_PACKAGE),
                errmsg("not found package or procedure."),
                errdetail("package may has error"),
                errcause("not found package and procedure"),
                erraction("retry")));
    }
    return lines;
}

/*
    insert error line number and message into DBE_PLDEVELOPER.gs_errors
*/
void InsertError(Oid objId) 
{
#ifdef ENABLE_MULTIPLE_NODES
    return;
#else
    if (u_sess->plsql_cxt.errorList == NULL || 
            (!u_sess->attr.attr_common.plsql_show_all_error &&
            u_sess->attr.attr_sql.check_function_bodies)) {
        return;
    } 
    Oid id = InvalidOid;
    Oid nspid = InvalidOid;
    char* name = NULL;
    char* type = NULL;
    Oid userId = (Oid)u_sess->misc_cxt.CurrentUserId;
    int rc = CompileWhich();
    if (rc == PLPGSQL_COMPILE_PROC) {
        id = objId;
        HeapTuple tuple;
        bool isnull = false;
        PLpgSQL_function* func = u_sess->plsql_cxt.curr_compile_context->plpgsql_curr_compile;
        tuple = SearchSysCache1(PROCOID, ObjectIdGetDatum(func->fn_oid));
        Form_pg_proc procStruct = (Form_pg_proc)GETSTRUCT(tuple);
        nspid = procStruct->pronamespace;
        name = NameStr(procStruct->proname);
        Datum prokindDatum = SysCacheGetAttr(PROCOID, tuple, Anum_pg_proc_prokind, &isnull);
        /* prokind maybe null */
        char prokind;
        if (isnull) {
            prokind = 'f';
        } else {
            prokind = CharGetDatum(prokindDatum);
        }
        if (PROC_IS_PRO(prokind)) {
            type = "procedure";
        } else {
            type = "function";
        }
        ReleaseSysCache(tuple);
    } else if ((rc == PLPGSQL_COMPILE_PACKAGE ||
        rc == PLPGSQL_COMPILE_PACKAGE_PROC)) {
        HeapTuple tuple;
        PLpgSQL_package* pkg = u_sess->plsql_cxt.curr_compile_context->plpgsql_curr_compile_package;
        id = pkg->pkg_oid;
        tuple = SearchSysCache1(PACKAGEOID, ObjectIdGetDatum(pkg->pkg_oid));
        if (!HeapTupleIsValid(tuple)) {
            ereport(ERROR,
                (errmodule(MOD_PLSQL), errcode(ERRCODE_UNDEFINED_PACKAGE),
                    errmsg("not found package."),
                    errdetail("package may has error"),
                    errcause("create package may has error"),
                    erraction("please check package")));
        }
        Form_gs_package pkgStruct = (Form_gs_package)GETSTRUCT(tuple);
        nspid = pkgStruct->pkgnamespace;
        name = NameStr(pkgStruct->pkgname);
        if (pkg->is_spec_compiling) {
            type = "package";
        } else {
            type = "package body";
        }
        ReleaseSysCache(tuple);
    }
    StringInfoData ds;  
    char* tmp = EscapeQuotes(name);
    initStringInfo(&ds);
    appendStringInfoString(&ds,
        "declare\n"
        "PRAGMA AUTONOMOUS_TRANSACTION;\n"
        "oldId int:=0;"
        "objId int:=0;"
        "allNum int:=0;\n"
        "begin\n ");
    appendStringInfo(&ds,
        "select count(*) from dbe_pldeveloper.gs_source into allNum where "
        "nspid=%u and name=\'%s\' and type=\'%s\';", nspid, tmp, type);
    appendStringInfo(&ds,
        "if allNum > 0 then "
        "select id from dbe_pldeveloper.gs_source into oldId where "
        "nspid=%u and name=\'%s\' and type=\'%s\';"
        "objId := oldId; "
        "else "
        "objId := %u;"
        "end if;", nspid, tmp, type, objId);
    appendStringInfo(&ds, 
        "delete from DBE_PLDEVELOPER.gs_errors where nspid=%u and name=\'%s\' and type = \'%s\';\n",
        nspid, tmp, type);
    char* errmsg = NULL;
    int line = 0;
    if (rc != PLPGSQL_COMPILE_NULL) {
        ListCell* cell = NULL;
        foreach (cell, u_sess->plsql_cxt.errorList) {
            PLpgSQL_error* item = (PLpgSQL_error*)lfirst(cell);
            errmsg = item->errmsg;
            line = item->line;
            appendStringInfoString(&ds, "insert into DBE_PLDEVELOPER.gs_errors ");
            appendStringInfo(&ds, "values(objId,%u,%u,\'%s\',\'%s\',%d,$gserrors$%s$gserrors$);\n",
                userId, nspid, tmp, type, line, errmsg);
        }
    }
    appendStringInfo(&ds, "end;");
    List* rawParserList = NULL;
    rawParserList = raw_parser(ds.data);
    DoStmt* stmt = (DoStmt *)linitial(rawParserList);
    u_sess->plsql_cxt.insertError = true;
    int save_compile_status = getCompileStatus();
    int save_compile_list_length = list_length(u_sess->plsql_cxt.compile_context_list);
    PLpgSQL_compile_context* save_compile_context = u_sess->plsql_cxt.curr_compile_context;
    MemoryContext temp = NULL;
    if (u_sess->plsql_cxt.curr_compile_context != NULL) {
        temp = MemoryContextSwitchTo(u_sess->plsql_cxt.curr_compile_context->compile_tmp_cxt);
    }
    PG_TRY();
    {
        (void)CompileStatusSwtichTo(NONE_STATUS);
        u_sess->plsql_cxt.curr_compile_context = NULL;
        ExecuteDoStmt(stmt, true);
    }
    PG_CATCH();
    {
        if (temp != NULL) {
            MemoryContextSwitchTo(temp);
        }
        (void)CompileStatusSwtichTo(save_compile_status);
        u_sess->plsql_cxt.curr_compile_context = save_compile_context;
        u_sess->plsql_cxt.isCreateFunction = false;
        PG_RE_THROW();
    }
    PG_END_TRY();
    u_sess->plsql_cxt.curr_compile_context = save_compile_context;
    if (rc != PLPGSQL_COMPILE_PACKAGE_PROC) {
        clearCompileContextList(save_compile_list_length);
    }
    (void)CompileStatusSwtichTo(save_compile_status);
    if (temp != NULL) {
        MemoryContextSwitchTo(temp);
    }
    u_sess->plsql_cxt.insertError = false;
    pfree_ext(tmp);
    pfree_ext(ds.data);
    list_free_deep(u_sess->plsql_cxt.errorList);
    u_sess->plsql_cxt.errorList = NULL;
#endif
}

/*
    insert error line number and message into DBE_PLDEVELOPER.gs_errors
*/
void DropErrorByOid(int objtype, Oid objoid) 
{
    bool notInsert = u_sess->attr.attr_common.upgrade_mode != 0 || SKIP_GS_SOURCE;
    if (notInsert) {
        return;
    }

#ifdef ENABLE_MULTIPLE_NODES
    return;
#else 
    char* name = NULL;
    char* type = NULL;
    Oid nspid = InvalidOid;
    if (objtype == PLPGSQL_PROC) {
        HeapTuple tuple;
        bool isnull = false;
        tuple = SearchSysCache1(PROCOID, ObjectIdGetDatum(objoid));
        Form_pg_proc procStruct = (Form_pg_proc)GETSTRUCT(tuple);
        nspid = procStruct->pronamespace;
        name = NameStr(procStruct->proname);
        Datum prokindDatum = SysCacheGetAttr(PROCOID, tuple, Anum_pg_proc_prokind, &isnull);
        /* prokind maybe null */
        char prokind;
        if (isnull) {
            prokind = 'f';
        } else {
            prokind = CharGetDatum(prokindDatum);
        }
        if (PROC_IS_PRO(prokind)) {
            type = "procedure";
        } else {
            type = "function";
        }
        ReleaseSysCache(tuple);
    } else if ((objtype == PLPGSQL_PACKAGE ||
        objtype == PLPGSQL_PACKAGE_BODY)) {
        HeapTuple tuple = NULL;
        tuple = SearchSysCache1(PACKAGEOID, ObjectIdGetDatum(objoid));
        if (!HeapTupleIsValid(tuple)) {
            ereport(ERROR,
                (errmodule(MOD_PLSQL), errcode(ERRCODE_UNDEFINED_PACKAGE),
                    errmsg("not found package."),
                    errdetail("package may has error"),
                    errcause("create package may has error"),
                    erraction("please check package")));
        }
        Form_gs_package pkgStruct = (Form_gs_package)GETSTRUCT(tuple);
        nspid = pkgStruct->pkgnamespace;
        name = NameStr(pkgStruct->pkgname);
        if (objtype == PLPGSQL_PACKAGE) {
            type = "package";
        } else {
            type = "package body";
        }
        ReleaseSysCache(tuple);
    }
    StringInfoData      ds;  
    char* tmp = EscapeQuotes(name);
    initStringInfo(&ds);
    appendStringInfoString(&ds, " declare begin ");
    if (objtype == PLPGSQL_PACKAGE_BODY) {
        appendStringInfo(&ds, "  delete from DBE_PLDEVELOPER.gs_errors "
                        "where nspid=%u and name = \'%s\' and type = \'%s\';",
                        nspid, tmp, type);
        appendStringInfo(&ds, "  delete from DBE_PLDEVELOPER.gs_source where "
                        "nspid=%u and name = \'%s\' and type = \'%s\';",
                        nspid, tmp, type);
    } else {
        appendStringInfo(&ds, "  delete from DBE_PLDEVELOPER.gs_errors "
                        "where nspid=%u and name = \'%s\' and type = \'%s\';",
                        nspid, tmp, type);
        appendStringInfo(&ds, "  delete from DBE_PLDEVELOPER.gs_errors "
                        "where nspid=%u and name = \'%s\' and type = \'package body\';",
                        nspid, tmp);    
        appendStringInfo(&ds, "  delete from DBE_PLDEVELOPER.gs_source where "
                        "nspid=%u and name = \'%s\' and type = \'%s\';",
                        nspid, tmp, type);
        appendStringInfo(&ds, "  delete from DBE_PLDEVELOPER.gs_source where "
                        "nspid=%u and name = \'%s\' and type = \'package body\';",
                        nspid, tmp);    
    }
    appendStringInfo(&ds, " EXCEPTION WHEN OTHERS THEN NULL; \n");
    appendStringInfo(&ds, " END; ");
    List* rawParserList = raw_parser(ds.data);
    DoStmt* stmt = (DoStmt *)linitial(rawParserList);;
    int save_compile_status = getCompileStatus();
    int save_compile_list_length = list_length(u_sess->plsql_cxt.compile_context_list);
    PLpgSQL_compile_context* save_compile_context = u_sess->plsql_cxt.curr_compile_context;
    MemoryContext temp = NULL;
    if (u_sess->plsql_cxt.curr_compile_context != NULL) {
        temp = MemoryContextSwitchTo(u_sess->plsql_cxt.curr_compile_context->compile_tmp_cxt);
    }
    PG_TRY();
    {
        (void)CompileStatusSwtichTo(NONE_STATUS);
        u_sess->plsql_cxt.curr_compile_context = NULL;
        ExecuteDoStmt(stmt, true);
    }
    PG_CATCH();
    {
        if (temp != NULL) {
            MemoryContextSwitchTo(temp);
        }
        (void)CompileStatusSwtichTo(save_compile_status);
        u_sess->plsql_cxt.curr_compile_context = save_compile_context;
        clearCompileContextList(save_compile_list_length);
        PG_RE_THROW();
    }
    PG_END_TRY();
    u_sess->plsql_cxt.curr_compile_context = save_compile_context;
    (void)CompileStatusSwtichTo(save_compile_status);
    if (temp != NULL) {
        MemoryContextSwitchTo(temp);
    }
    pfree_ext(tmp);
    pfree_ext(ds.data);
#endif
}

int CompileWhich()
{
    if (u_sess->plsql_cxt.curr_compile_context == NULL) {
        return PLPGSQL_COMPILE_NULL;
    }
    
    if (u_sess->plsql_cxt.curr_compile_context->plpgsql_curr_compile_package != NULL && 
        u_sess->plsql_cxt.curr_compile_context->plpgsql_curr_compile != NULL) {
        if (u_sess->plsql_cxt.curr_compile_context->plpgsql_curr_compile->fn_signature == NULL) {
            return PLPGSQL_COMPILE_NULL;
        } else {
            return PLPGSQL_COMPILE_PACKAGE_PROC;
        }
    } else if (u_sess->plsql_cxt.curr_compile_context->plpgsql_curr_compile_package != NULL && 
        u_sess->plsql_cxt.curr_compile_context->plpgsql_curr_compile == NULL) {
        return PLPGSQL_COMPILE_PACKAGE;
    } else if (u_sess->plsql_cxt.curr_compile_context->plpgsql_curr_compile_package == NULL && 
        u_sess->plsql_cxt.curr_compile_context->plpgsql_curr_compile != NULL) {
        if (u_sess->plsql_cxt.curr_compile_context->plpgsql_curr_compile->fn_signature == NULL) {
            return PLPGSQL_COMPILE_NULL;
        } else {
            return PLPGSQL_COMPILE_PROC;
        }
    } else {
        return PLPGSQL_COMPILE_NULL;
    }
    return PLPGSQL_COMPILE_NULL;
}

void InsertErrorMessage(const char* message, int yyloc, bool isQueryString, int lines)
{
#ifdef ENABLE_MULTIPLE_NODES
    return;
#else 
    int rc = CompileWhich();
    if (rc == PLPGSQL_COMPILE_NULL || 
        (!u_sess->attr.attr_common.plsql_show_all_error && u_sess->attr.attr_sql.check_function_bodies)) {
        return;
    }
#endif
    u_sess->plsql_cxt.have_error = true;
    if (!isQueryString && lines == 0) {
        lines = GetProcedureLineNumberInPackage(u_sess->plsql_cxt.curr_compile_context->core_yy->scanbuf, yyloc);
    } else if (lines == 0) {
        lines = GetProcedureLineNumberInPackage(t_thrd.postgres_cxt.debug_query_string, yyloc);
    }
    addErrorList(message, lines);
}

static inline bool gsplsql_pkg_get_valid(PLpgSQL_package* pkg, bool is_spec)
{
    if (is_spec) {
        return (pkg->status & PACKAGE_SPEC_VALID) == PACKAGE_SPEC_VALID;
    }
    return (pkg->status & PACKAGE_BODY_VALID) == PACKAGE_BODY_VALID;;
}

static void gsplsql_pkg_set_status_in_mem(PLpgSQL_package* pkg, bool is_spec, bool is_valid)
{
    if (pkg == NULL) {
        return;
    }
    if (is_spec) {
        if (is_valid) {
            pkg->status |= PACKAGE_SPEC_VALID;
        } else {
            pkg->status &= PACKAGE_SPEC_INVALID;
        }
    } else {
        if (is_valid) {
            pkg->status |= PACKAGE_BODY_VALID;
        } else {
            pkg->status &= PACKAGE_BODY_INVALID;
        }
    }
}

static void gsplsql_pkg_set_status(PLpgSQL_package* pkg, bool isSpec, bool isCreate, bool isValid, bool isRecompile,
                                   bool pkg_spec_valid, bool pkg_body_valid)
{
    gsplsql_pkg_set_status_in_mem(pkg, isSpec, GetCurrCompilePgObjStatus());
    if (!enable_plpgsql_gsdependency_guc() || pkg == NULL) {
        return;
    }
    if (isSpec) {
        if (pkg_spec_valid != isValid) {
            SetPgObjectValid(pkg->pkg_oid, OBJECT_TYPE_PKGSPEC, isValid);
            if (!isValid) {
                SetPgObjectValid(pkg->pkg_oid, OBJECT_TYPE_PKGBODY, isValid);
            }
            HeapTuple tup = SearchSysCache1(PACKAGEOID, ObjectIdGetDatum(pkg->pkg_oid));
            if (!HeapTupleIsValid(tup)) {
                ereport(ERROR, (errmodule(MOD_PLSQL), errcode(ERRCODE_CACHE_LOOKUP_FAILED),
                                errmsg("cache lookup failed for package %u, while compile package", pkg->pkg_oid),
                                errdetail("cache lookup failed"),
                                errcause("System error"),
                                erraction("Drop or rebuild the package")));
            }
            bool is_null;
            (void)SysCacheGetAttr(PACKAGEOID, tup, Anum_gs_package_pkgbodydeclsrc, &is_null);
            if (is_null || !isValid) {
                gsplsql_set_pkg_func_status(pkg->namespaceOid, pkg->pkg_oid, isValid);
            }
            ReleaseSysCache(tup);
        }
    } else {
        if (pkg_body_valid != isValid) {
            bool spec_status = gsplsql_pkg_get_valid(pkg, true);
            if (!spec_status && isValid) {
                spec_status = isValid;
            }
            SetPgObjectValid(pkg->pkg_oid, OBJECT_TYPE_PKGSPEC, spec_status);
            SetPgObjectValid(pkg->pkg_oid, OBJECT_TYPE_PKGBODY, isValid);
            gsplsql_set_pkg_func_status(pkg->namespaceOid, pkg->pkg_oid, isValid);
        }
    }
    if (isValid) {
        return;
    }
    if (isCreate) {
        ereport(WARNING, (errmodule(MOD_PLSQL),
                        errmsg("Package %screated with compilation erors.", isSpec ? "" : "Body ")));
    } else if (isRecompile) {
        ereport(WARNING, (errmodule(MOD_PLSQL),
                        errmsg("Package %s %srecompile with compilation erors.",
                               pkg->pkg_signature, isSpec ? "" : "Body ")));
    }
    return;
}
