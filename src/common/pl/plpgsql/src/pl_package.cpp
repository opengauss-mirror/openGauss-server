#include <ctype.h>

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
#include "miscadmin.h"



static void plpgsql_pkg_append_dlcell(plpgsql_pkg_HashEnt* entity);

static void plpgsql_pkg_HashTableInsert(PLpgSQL_package* pkg, PLpgSQL_pkg_hashkey* pkg_key);

extern PLpgSQL_package* plpgsql_pkg_HashTableLookup(PLpgSQL_pkg_hashkey* pkg_key);

extern bool check_search_path_interface(List *schemas, HeapTuple proc_tup);

extern void plpgsql_compile_error_callback(void* arg);

static bool plpgsql_pkg_check_search_path(PLpgSQL_package* pkg, HeapTuple proc_tup)
{
    /* If SUPPORT_BIND_SEARCHPATH is false, always return true. */
    return check_search_path_interface(pkg->pkg_searchpath->schemas, proc_tup);
}

bool IsOnlyCompilePackage()
{
    if (u_sess->plsql_cxt.plpgsql_curr_compile_package != NULL
        && u_sess->plsql_cxt.plpgsql_curr_compile == NULL) {
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

/*
 * use unknown type when compile a function which has package variable
 * because gauss doesn't support compile multiple function at the same time 
 * so we have a fake compile when compile other functions which has package 
 * variable.we only check if the package is exist,if not exist,return -1,
 * else return the variable number. 
 */

int plpgsql_pkg_add_unknown_var_to_namespace(List* name, int dtype)
{
    char* objname = NULL;
    char* pkgname = NULL;
    char* nspname = NULL;
    DeconstructQualifiedName(name, &nspname, &objname, &pkgname);
    if (IsFunctionInPackage(name) && pkgname!=NULL) {
        return -1;
    } else if (pkgname == NULL) {
        return -1;
    }
    if (PLPGSQL_DTYPE_ROW != dtype) {
        MemoryContext temp = MemoryContextSwitchTo(u_sess->plsql_cxt.plpgsql_cxt);
        PLpgSQL_var* var = NULL;
        var = (PLpgSQL_var*)palloc0(sizeof(PLpgSQL_var));
        var->pkg_name = stringToQualifiedNameList(pkgname);
        var->refname = pstrdup(objname);
        var->dtype = PLPGSQL_DTYPE_UNKNOWN;
        var->dno = plpgsql_adddatum((PLpgSQL_datum *) var);
        plpgsql_ns_additem(PLPGSQL_NSTYPE_UNKNOWN, var->dno, objname, pkgname);
        MemoryContextSwitchTo(temp);
        return var->dno;
    } else {
        Oid type_id = InvalidOid;
        int typmod_p = -1;
        parseTypeString("exception", &type_id, &typmod_p);
        PLpgSQL_type* rowDtype = plpgsql_build_datatype(type_id, typmod_p, 0);
        MemoryContext temp = MemoryContextSwitchTo(u_sess->plsql_cxt.plpgsql_cxt);
        PLpgSQL_row* var = build_row_from_class(rowDtype->typrelid);
        var->pkg_name = stringToQualifiedNameList(pkgname);
        var->refname = pstrdup(objname);
        var->dtype = PLPGSQL_DTYPE_ROW;
        var->dno = plpgsql_adddatum((PLpgSQL_datum *) var);
        var->customErrorCode = 0;
        plpgsql_ns_additem(PLPGSQL_DTYPE_ROW, var->dno, objname, pkgname);
        MemoryContextSwitchTo(temp);
        return var->dno;
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
            expr = pgxc_check_distcol_opexpr(query->resultRelation, attnum2, (OpExpr*)qual_expr);
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
        u_sess->plsql_cxt.plpgsql_dlist_objects =
            dlist_delete_cell(u_sess->plsql_cxt.plpgsql_dlist_objects, hentry->cell, false);
    }
    /* remove back link, which no longer points to allocated storage */
    pkg->pkg_hashkey = NULL;
}

extern void delete_package(PLpgSQL_package* pkg)
{
    ListCell* l = NULL;
    foreach(l, pkg->proc_compiled_list) {
        PLpgSQL_function* func = (PLpgSQL_function*)lfirst(l);
        delete_function(func);
        CacheInvalidateFunction(func->fn_oid);
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
    u_sess->plsql_cxt.plpgsql_dlist_objects = dlappend(u_sess->plsql_cxt.plpgsql_dlist_objects, entity);
    (void)MemoryContextSwitchTo(oldctx);

    entity->cell = u_sess->plsql_cxt.plpgsql_dlist_objects->tail;
    while (dlength(u_sess->plsql_cxt.plpgsql_dlist_objects) > g_instance.attr.attr_sql.max_compile_functions) {
        DListCell* headcell = u_sess->plsql_cxt.plpgsql_dlist_objects->head;
        plpgsql_pkg_HashEnt* head_entity = (plpgsql_pkg_HashEnt*)lfirst(headcell);

        pkg = head_entity->package;

        /* delete from the hash and delete the function's compile */
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
        u_sess->plsql_cxt.plpgsql_dlist_objects =
            dlist_delete_cell(u_sess->plsql_cxt.plpgsql_dlist_objects, hentry->cell, false);
    }
}

static void plpgsql_pkg_HashTableInsert(PLpgSQL_package* pkg, PLpgSQL_pkg_hashkey* pkg_key)
{
    plpgsql_pkg_HashEnt* hentry = NULL;
    bool found = false;
    hentry = (plpgsql_pkg_HashEnt*)hash_search(u_sess->plsql_cxt.plpgsql_pkg_HashTable, (void*)pkg_key, HASH_ENTER, &found);
    if (found) {
        /* move cell to the tail of the package list. */
        dlist_add_tail_cell(u_sess->plsql_cxt.plpgsql_dlist_objects, hentry->cell);
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
    plpgsql_pkg_HashEnt* hentry = NULL;
    hentry = (plpgsql_pkg_HashEnt*)hash_search(u_sess->plsql_cxt.plpgsql_pkg_HashTable, (void*)pkg_key, HASH_FIND, NULL);
    if (hentry != NULL) {
        /* add cell to the tail of the function list. */
        dlist_add_tail_cell(u_sess->plsql_cxt.plpgsql_dlist_objects, hentry->cell);
        return hentry->package;
    } else {
        return NULL;
    }
}



static PLpgSQL_package* do_pkg_compile(Oid pkgOid, HeapTuple pkg_tup, PLpgSQL_package* pkg, PLpgSQL_pkg_hashkey* hashkey, bool isSpec)
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
     * Setup the scanner input and error info.	We assume that this function
     * cannot be invoked recursively, so there's no need to save and restore
     * the static variables used here.
     */
     
    if (isSpec) {
        pkgsrcdatum = SysCacheGetAttr(PACKAGEOID, pkg_tup, Anum_gs_package_pkgspecsrc, &isnull);
    }
    else {
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
    plpgsql_scanner_init(pkg_source);
    u_sess->plsql_cxt.plpgsql_error_pkgname = pstrdup(NameStr(pkg_struct->pkgname));
    /*
     * Setup error traceback support for ereport()
     */
    pl_err_context.callback = plpgsql_compile_error_callback;
    pl_err_context.arg = NULL;
    pl_err_context.previous = t_thrd.log_cxt.error_context_stack;
    t_thrd.log_cxt.error_context_stack = &pl_err_context;
    signature = pstrdup(NameStr(pkg_struct->pkgname));
    /*
     * Create the new function struct, if not done already.  The function
     * structs are never thrown away, so keep them in session memory context.
     */
    if (pkg == NULL) {
        pkg = (PLpgSQL_package*)MemoryContextAllocZero(
            SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_OPTIMIZER), sizeof(PLpgSQL_package));
        pkg->pkg_cxt = AllocSetContextCreate(u_sess->top_mem_cxt,
            context_name,
            ALLOCSET_DEFAULT_MINSIZE,
            ALLOCSET_DEFAULT_INITSIZE,
            ALLOCSET_DEFAULT_MAXSIZE);
        u_sess->plsql_cxt.plpgsql_cxt = pkg->pkg_cxt;
        pkg->pkg_signature = pstrdup(signature);
        pkg->pkg_owner = pkg_struct->pkgowner;
        pkg->pkg_oid = pkgOid;
        pkg->pkg_tid = pkg_tup->t_self;
        pkg->proc_list = NULL;
    }
    namespaceOidDatum = SysCacheGetAttr(PACKAGEOID, pkg_tup, Anum_gs_package_pkgnamespace, &isnull);
    if (!isnull) {
       namespaceOid = DatumGetObjectId(namespaceOidDatum);
    }
    if (OidIsValid(namespaceOid)) {
        pkg->namespaceOid = namespaceOid;
    } else {
        pkg->namespaceOid = InvalidOid;
    }
    u_sess->plsql_cxt.plpgsql_cxt = pkg->pkg_cxt;
    u_sess->plsql_cxt.plpgsql_pkg_cxt = pkg->pkg_cxt;
    pkg->is_spec_compiling = isSpec;
    if (isSpec) {
        u_sess->plsql_cxt.plpgsql_IndexErrorVariable = 0;
    }
    /*
     * All the permanent output of compilation (e.g. parse tree) is kept in a
     * per-function memory context, so it can be reclaimed easily.
     */
    rc = snprintf_s(
        context_name, NAMEDATALEN, NAMEDATALEN - 1, "%s_%lu", "PL/pgSQL package context", u_sess->debug_query_id);
    securec_check_ss(rc, "", "");
    /*
     * compile_tmp_cxt is a short temp context that will be detroyed after
     * function compile or execute.
     * func_cxt is a long term context that will stay until thread exit. So
     * malloc on func_cxt should be very careful.
     * signature is stored on a StringInfoData which is 1K byte at least, but
     * most signature will not be so long originally, so we should do a strdup.
     */
    u_sess->plsql_cxt.compile_tmp_cxt = MemoryContextSwitchTo(pkg->pkg_cxt);
    pkg->pkg_searchpath = (OverrideSearchPath*)palloc0(sizeof(OverrideSearchPath));
    pkg->pkg_searchpath->addCatalog = true;
    pkg->pkg_searchpath->addTemp = true;
    pkg->pkg_xmin = HeapTupleGetRawXmin(pkg_tup);
    pkg->proc_compiled_list = NULL;
    u_sess->plsql_cxt.plpgsql_curr_compile_package = pkg;
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
    u_sess->plsql_cxt.plpgsql_curr_compile_package->proc_compiled_list = NULL;
    /*
     * Initialize the compiler, particularly the namespace stack.  The
     * outermost namespace contains function parameters and other special
     * variables (such as FOUND), and is named after the function itself.
     */
    u_sess->plsql_cxt.datums_pkg_alloc = alloc_size;
    u_sess->plsql_cxt.plpgsql_pkg_nDatums = 0;
    /* This is short-lived, so needn't allocate in function's cxt */
    u_sess->plsql_cxt.plpgsql_Datums = (PLpgSQL_datum**)MemoryContextAlloc(
        u_sess->plsql_cxt.compile_tmp_cxt, sizeof(PLpgSQL_datum*) * u_sess->plsql_cxt.datums_pkg_alloc);
    u_sess->plsql_cxt.datums_last = 0;
    PushOverrideSearchPath(pkg->pkg_searchpath);
    plpgsql_ns_init();
    plpgsql_ns_push(NameStr(pkg_struct->pkgname));
    add_pkg_compile();
    u_sess->plsql_cxt.datums_last = u_sess->plsql_cxt.plpgsql_nDatums;
    u_sess->plsql_cxt.plpgsql_curr_compile_package = pkg;
    u_sess->plsql_cxt.plpgsql_pkg_DumpExecTree = false;
    /*
     * Now parse the function's text
     */
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
    
    pkg->ndatums = u_sess->plsql_cxt.plpgsql_pkg_nDatums;
    pkg->datums = (PLpgSQL_datum**)palloc(sizeof(PLpgSQL_datum*) * u_sess->plsql_cxt.plpgsql_pkg_nDatums);
    for (i = 0; i < u_sess->plsql_cxt.plpgsql_pkg_nDatums; i++) {
        pkg->datums[i] = u_sess->plsql_cxt.plpgsql_Datums[i];
        if (pkg->datums[i]->dtype == PLPGSQL_DTYPE_VAR) {
            PLpgSQL_var* var = reinterpret_cast<PLpgSQL_var*>(pkg->datums[i]);
            var->pkg = pkg;
        }
    }
    
    if (isSpec) {
        pkg->public_ns = u_sess->plsql_cxt.ns_top;
        pkg->is_bodydefined = false;
    } else  {
        pkg->private_ns = u_sess->plsql_cxt.ns_top;
        pkg->is_bodydefined = true;
    }
    pkg->proc_list = u_sess->plsql_cxt.plpgsql_curr_compile_package->proc_list;
    if (!isSpec) {
        pkg->is_bodydefined = true;
    } 
    u_sess->plsql_cxt.compile_tmp_cxt = MemoryContextSwitchTo(pkg->pkg_cxt);
    pkg->proc_compiled_list = u_sess->plsql_cxt.plpgsql_curr_compile_package->proc_compiled_list;
    if (hashkey && isSpec) {
        plpgsql_pkg_HashTableInsert(pkg, hashkey);
    }
    t_thrd.log_cxt.error_context_stack = pl_err_context.previous;
    u_sess->plsql_cxt.plpgsql_error_funcname = NULL;
    u_sess->plsql_cxt.plpgsql_check_syntax = false;
    MemoryContextSwitchTo(u_sess->plsql_cxt.compile_tmp_cxt);
    u_sess->plsql_cxt.compile_tmp_cxt = NULL;
    u_sess->plsql_cxt.plpgsql_curr_compile = NULL;
    return pkg;
}


/*
 * compile and init package by package oid
 */
PLpgSQL_package* plpgsql_pkg_compile(Oid pkgOid, bool for_validator, bool isSpec, bool isCreate)
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
    /*
     * Lookup the gs_package tuple by Oid; we'll need it in any case
     */
    pkg_tup = SearchSysCache1(PACKAGEOID, ObjectIdGetDatum(pkgOid));
    if (!HeapTupleIsValid(pkg_tup)) {
        ereport(ERROR,  (errmodule(MOD_PLSQL),  errcode(ERRCODE_CACHE_LOOKUP_FAILED),
                errmsg("cache lookup failed for package %u, while compile package", pkgOid)));
    }
    pkg_struct = (Form_gs_package)GETSTRUCT(pkg_tup);
    hashkey.pkgOid = pkgOid;
    pkg = plpgsql_pkg_HashTableLookup(&hashkey);

    if (pkg != NULL) {
        if (pkg->pkg_xmin == HeapTupleGetRawXmin(pkg_tup) &&
            ItemPointerEquals(&pkg->pkg_tid, &pkg_tup->t_self) && plpgsql_pkg_check_search_path(pkg, pkg_tup)) {
                pkg_valid = true;
        } else {
            delete_package(pkg);
            pkg_valid = false;
        }
    }
    if (!pkg_valid) {
        pkg = NULL;
        ConnectSPI();
        pkg = do_pkg_compile(pkgOid, pkg_tup, pkg, &hashkey, true);
        DisconnectSPI();
        PackageInit(pkg, isCreate);
        if (!isSpec && pkg != NULL) {
            ConnectSPI();
            pkg = do_pkg_compile(pkgOid, pkg_tup, pkg, &hashkey, false);
            if (pkg == NULL) {
                ereport(ERROR,  (errmodule(MOD_PLSQL),  errcode(ERRCODE_CACHE_LOOKUP_FAILED),
                    errmsg("package %u not found", pkgOid)));
            }
            DisconnectSPI();
            ReleaseSysCache(pkg_tup);
            pkg_tup = NULL;
            PackageInit(pkg, isCreate);
        } else if(!isSpec && pkg == NULL) {
            ereport(ERROR,  (errmodule(MOD_PLSQL),  errcode(ERRCODE_CACHE_LOOKUP_FAILED),
                errmsg("package spec %u not found", pkgOid)));
        }
    } else {
        if (!pkg->is_bodydefined && !isSpec) {
            pkg = do_pkg_compile(pkgOid, pkg_tup, pkg, &hashkey, false);
        }
        u_sess->plsql_cxt.plpgsql_curr_compile_package = pkg;
        /* package must be compiled befor init */
        if (pkg != NULL) {
            PackageInit(pkg, isCreate);
        } else {
            ereport(ERROR,  (errmodule(MOD_PLSQL),  errcode(ERRCODE_CACHE_LOOKUP_FAILED),
                errmsg("package spec %u not found", pkgOid))); 
        }
        u_sess->plsql_cxt.plpgsql_curr_compile_package = NULL;
    }
    if (HeapTupleIsValid(pkg_tup)) {
        ReleaseSysCache(pkg_tup);
        pkg_tup = NULL;
    }
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

    if (u_sess->plsql_cxt.plpgsql_error_funcname) {
        errcontext("compilation of PL/pgSQL function \"%s\" near line %d",
            u_sess->plsql_cxt.plpgsql_error_funcname, plpgsql_latest_lineno());
    }
}

Oid findPackageParameter(const char* objname) 
{
    Oid toid = InvalidOid;
    if (u_sess->plsql_cxt.plpgsql_curr_compile_package != NULL) {
        PLpgSQL_package* pkg = u_sess->plsql_cxt.plpgsql_curr_compile_package;
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
                default:
                    toid = InvalidOid;
            }
        }
    }
    return toid;
}
