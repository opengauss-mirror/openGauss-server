/* Native IndexPath derivation for query-local AStore DiskANN filtering. */
#include "postgres.h"
#include "knl/knl_variable.h"

#include <math.h>
#include "access/datavec/diskann.h"
#include "access/heapam.h"
#include "access/sysattr.h"
#include "access/tuptoaster.h"
#include "access/xact.h"
#include "catalog/namespace.h"
#include "catalog/pg_am.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_type.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/clauses.h"
#include "optimizer/cost.h"
#include "optimizer/diskann_filter_path.h"
#include "optimizer/pathnode.h"
#include "optimizer/planmain.h"
#include "optimizer/paths.h"
#include "optimizer/prep.h"
#include "optimizer/restrictinfo.h"
#include "optimizer/subselect.h"
#include "optimizer/tlist.h"
#include "optimizer/var.h"
#include "parser/parse_func.h"
#include "parser/parse_oper.h"
#include "rewrite/rewriteManip.h"
#include "storage/buf/bufmgr.h"
#include "utils/lsyscache.h"
#include "utils/guc.h"
#include "utils/rel_gs.h"
#include "utils/selfuncs.h"
#include "utils/spccache.h"
#include "utils/syscache.h"

static const Oid ANN_L2_OPERATOR = 8311;
static const Oid ANN_COSINE_OPERATOR = 8313;
static Path* ann_clone_source(Path* path);

bool ann_keep_join_inputs(PlannerInfo* root, RelOptInfo* rel)
{
    return root != NULL && rel != NULL && root->ann_filter_request != NULL &&
        !root->ann_filter_request->deriving &&
        bms_num_members(rel->relids) > 1 &&
        bms_equal(rel->relids, root->ann_filter_request->qualification_relids);
}

void ann_preserve_ordinary_candidate(PlannerInfo* root, Path* path)
{
    if (root == NULL || root->ann_filter_request == NULL || root->ann_filter_request->deriving ||
        path->parent == NULL || path->dop > 1)
        return;
    DiskAnnFilterRequest* request = root->ann_filter_request;
    bool complete = ann_keep_join_inputs(root, path->parent) && path->param_info == NULL;
    bool single_source = bms_num_members(request->qualification_relids) == 1 &&
        path->parent->relid == request->target_rti && path->param_info == NULL &&
        (path->pathtype == T_SeqScan || path->pathtype == T_IndexScan ||
         path->pathtype == T_BitmapHeapScan || path->pathtype == T_TidScan);
    bool business_probe = IsA(path, IndexPath) && !((IndexPath*)path)->isAnnIndex &&
        path->parent->relid != request->target_rti &&
        bms_is_member(path->parent->relid, request->qualification_relids) &&
        path->param_info != NULL && bms_is_subset(PATH_REQ_OUTER(path), request->qualification_relids);
    if (!complete && !business_probe && !single_source)
        return;
    Path* owned = ann_clone_source(path);
    if (owned != NULL) {
        root->ann_filter_request->ordinary_candidates =
            lappend(root->ann_filter_request->ordinary_candidates, owned);
    }
}

static void ann_unsupported(PlannerInfo* root, const char* reason)
{
    int mode = u_sess->datavec_ctx.diskann_filter_mode;
    root->ann_filter_request = NULL;
    if (mode != DISKANN_FILTER_AUTO && mode != DISKANN_FILTER_EXACT)
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("requested DiskANN filter mode cannot be used: %s", reason)));
}

/* Deliberately recognize only OID/type-checked builtin constraint forms. */
static bool ann_nonzero_clause(Node* clause, const Var* var, Oid normfn, Oid greaterop)
{
    if (clause == NULL || !IsA(clause, OpExpr))
        return false;
    OpExpr* op = (OpExpr*)clause;
    if (op->opno != greaterop || list_length(op->args) != 2)
        return false;
    Node* lhs = (Node*)linitial(op->args);
    Node* rhs = (Node*)lsecond(op->args);
    if (!IsA(lhs, FuncExpr) || !IsA(rhs, Const))
        return false;
    FuncExpr* fn = (FuncExpr*)lhs;
    Const* zero = (Const*)rhs;
    return fn->funcid == normfn && list_length(fn->args) == 1 &&
        equal(linitial(fn->args), var) && zero->consttype == FLOAT8OID &&
        !zero->constisnull && DatumGetFloat8(zero->constvalue) == 0.0;
}

static bool ann_vector_domain(PlannerInfo* root, RelOptInfo* rel, Relation table, Var* var, bool cosine)
{
    bool notnull = TupleDescAttr(table->rd_att, var->varattno - 1)->attnotnull;
    bool nonzero = !cosine;
    Oid argtype = VECTOROID;
    Oid normfn = LookupFuncName(list_make2(makeString("pg_catalog"), makeString("vector_norm")),
        1, &argtype, false);
    Oid greaterop = OpernameGetOprid(list_make2(makeString("pg_catalog"), makeString(">")),
        FLOAT8OID, FLOAT8OID);
    ListCell* lc;
    foreach (lc, rel->baserestrictinfo) {
        Node* clause = (Node*)((RestrictInfo*)lfirst(lc))->clause;
        if (IsA(clause, NullTest)) {
            NullTest* test = (NullTest*)clause;
            if (test->nulltesttype == IS_NOT_NULL && !test->argisrow && equal(test->arg, var))
                notnull = true;
        }
        if (ann_nonzero_clause(clause, var, normfn, greaterop)) {
            nonzero = true;
            notnull = true; /* strict WHERE predicate excludes NULL */
        }
    }
    TupleConstr* constraints = table->rd_att->constr;
    if (constraints != NULL && cosine) {
        for (int i = 0; i < constraints->num_check; i++) {
            if (!constraints->check[i].ccvalid)
                continue;
            Node* expr = (Node*)stringToNode(constraints->check[i].ccbin);
            ChangeVarNodes(expr, 1, rel->relid, 0);
            expr = eval_const_expressions(root, expr);
            List* clauses = make_ands_implicit((Expr*)expr);
            foreach (lc, clauses) {
                if (ann_nonzero_clause((Node*)lfirst(lc), var, normfn, greaterop))
                    nonzero = true;
            }
        }
    }
    return notnull && nonzero;
}

/* Use the surviving native join constraints, after outer-join reduction
 * and useless-join removal. The parse jointree can still mention a removed
 * business relation. Uniqueness is not an eligibility requirement. */
static bool ann_target_is_preserved(PlannerInfo* root, Index target_rti)
{
    ListCell* lc;
    foreach (lc, root->join_info_list) {
        SpecialJoinInfo* join = (SpecialJoinInfo*)lfirst(lc);
        bool left = bms_is_member(target_rti, join->syn_lefthand);
        bool right = bms_is_member(target_rti, join->syn_righthand);
        if ((join->jointype == JOIN_FULL && (left || right)) ||
            (join->jointype == JOIN_LEFT && right) ||
            (join->jointype == JOIN_RIGHT && left) ||
            ((join->jointype == JOIN_SEMI || join->jointype == JOIN_ANTI ||
              join->jointype == JOIN_LEFT_ANTI_FULL) && right) ||
            ((join->jointype == JOIN_RIGHT_SEMI || join->jointype == JOIN_RIGHT_ANTI ||
              join->jointype == JOIN_RIGHT_ANTI_FULL) && left))
            return false;
    }
    return true;
}

/* DEBUG2 describes registered metadata, not an execution plan or a claim
 * that a mode has been implemented. Conditions remain in native restrict
 * lists / SpecialJoinInfo at their original ON/WHERE evaluation sites. */
static void ann_log_request(PlannerInfo* root, DiskAnnFilterRequest* request)
{
    List* relations = NIL;
    for (int i = 1; i < root->simple_rel_array_size; i++)
        if (bms_is_member(i, request->qualification_relids))
            relations = lappend_int(relations, i);
    ereport(DEBUG2, (errmsg("DiskANN filter request: target_rti=%u vector_attno=%d "
        "qualification_relids=%s special_joins=%d lateral_dependencies=%s",
        request->target_rti, (int)request->vector_attno, nodeToString(relations),
        list_length(root->join_info_list),
        bms_is_empty(root->simple_rel_array[request->target_rti]->lateral_relids) ? "none" : "native")));
}

void ann_register_filter_request(PlannerInfo* root)
{
    Query* query = root->parse;
    int mode = u_sess->datavec_ctx.diskann_filter_mode;
    if (!u_sess->datavec_ctx.enable_diskann_filter && mode == DISKANN_FILTER_AUTO)
        return;
    if (list_length(query->sortClause) != 1) {
        ListCell* lc;
        foreach (lc, query->sortClause) {
            Node* order = get_sortgroupclause_expr((SortGroupClause*)lfirst(lc), query->targetList);
            if (IsA(order, OpExpr) &&
                (((OpExpr*)order)->opno == ANN_L2_OPERATOR || ((OpExpr*)order)->opno == ANN_COSINE_OPERATOR)) {
                root->ann_filter_controlled = true;
                ann_unsupported(root, "ORDER BY must contain exactly one distance key");
                break;
            }
        }
        return;
    }
    SortGroupClause* sort = (SortGroupClause*)linitial(query->sortClause);
    Node* expr = get_sortgroupclause_expr(sort, query->targetList);
    if (!IsA(expr, OpExpr))
        return;
    OpExpr* distance = (OpExpr*)expr;
    if (distance->opno != ANN_L2_OPERATOR && distance->opno != ANN_COSINE_OPERATOR)
        return;
    root->ann_filter_controlled = true;
    if (mode == DISKANN_FILTER_EXACT)
        return;
    if (!u_sess->datavec_ctx.enable_diskann_filter) {
        ann_unsupported(root, "enable_diskann_filter is off");
        return;
    }
    if (g_instance.role != VSINGLENODE ||
        t_thrd.proc->workingVersionNum < DISKANN_QUERY_FILTER_VERSION_NUM ||
        u_sess->attr.attr_sql.vectorEngineStrategy != OFF_VECTOR_ENGINE) {
        ann_unsupported(root, "only the current single-node row executor is supported");
        return;
    }
    if (query->commandType != CMD_SELECT || query->hasAggs || query->hasWindowFuncs ||
        query->hasTargetSRFs || expression_returns_set((Node*)query->targetList) ||
        contain_subplans((Node*)query->targetList) || contain_subplans((Node*)query->jointree) ||
        query->hasForUpdate || query->hasRowSecurity ||
        query->cteList != NIL || query->distinctClause != NIL || query->groupClause != NIL ||
        query->havingQual != NULL || query->setOperations != NULL ||
        query->limitIsPercent || query->limitWithTies ||
        contain_volatile_functions((Node*)query->targetList) ||
        contain_volatile_functions((Node*)query->jointree) || IsolationIsSerializable() ||
        IS_STREAM_PLAN || u_sess->opt_cxt.query_dop > 1) {
        ann_unsupported(root, "query shape, security, isolation, or execution mode is unsupported");
        return;
    }
    if (query->limitOffset != NULL && IsA(query->limitOffset, Const) &&
        !((Const*)query->limitOffset)->constisnull &&
        DatumGetInt64(((Const*)query->limitOffset)->constvalue) < 0)
        return; /* retain the ordinary executor's SQL error */
    if (query->limitOffset != NULL &&
        (!IsA(query->limitOffset, Const) ||
         (!((Const*)query->limitOffset)->constisnull &&
          DatumGetInt64(((Const*)query->limitOffset)->constvalue) != 0))) {
        ann_unsupported(root, "OFFSET must be zero");
        return;
    }
    if (query->limitCount != NULL && IsA(query->limitCount, Const) &&
        (((Const*)query->limitCount)->constisnull ||
         (((Const*)query->limitCount)->consttype == INT8OID &&
          DatumGetInt64(((Const*)query->limitCount)->constvalue) <= 0)))
        return; /* zero/NULL are legal; negative retains the native SQL error */
    if (query->limitCount == NULL || !IsA(query->limitCount, Const) ||
        ((Const*)query->limitCount)->constisnull ||
        ((Const*)query->limitCount)->consttype != INT8OID ||
        DatumGetInt64(((Const*)query->limitCount)->constvalue) <= 0) {
        ann_unsupported(root, "LIMIT must be known and positive at planning time");
        return;
    }
    Oid lessop = OpernameGetOprid(list_make2(makeString("pg_catalog"), makeString("<")),
        FLOAT8OID, FLOAT8OID);
    if (sort->sortop != lessop || distance->opresulttype != FLOAT8OID ||
        list_length(distance->args) != 2 || !IsA(linitial(distance->args), Var)) {
        ann_unsupported(root, "ORDER BY must be a single builtin vector distance in ascending order");
        return;
    }
    Var* var = (Var*)linitial(distance->args);
    Expr* vector = (Expr*)lsecond(distance->args);
    if (var->varlevelsup != 0 || var->varattno <= 0 || var->vartype != VECTOROID ||
        exprType((Node*)vector) != VECTOROID ||
        (!IsA(vector, Const) && !IsA(vector, Param))) {
        ann_unsupported(root, "vector column or query vector expression is unsupported");
        return;
    }
    if (IsA(vector, Param)) {
        /* A generic vector parameter may later be NULL, zero, or have a
         * different dimension. A custom plan binds and checks its value. */
        ann_unsupported(root, "query vector must be bound in a custom plan");
        return;
    }
    if (IsA(vector, Const)) {
        Const* value = (Const*)vector;
        if (value->constisnull) {
            ann_unsupported(root, "NULL query vector is outside the ANN domain");
            return;
        }
        Vector* v = DatumGetVector(value->constvalue);
        if (var->vartypmod > 0 && v->dim != var->vartypmod) {
            ann_unsupported(root, "query vector dimension does not match the target column");
            return;
        }
        double norm = 0.0;
        for (int i = 0; i < v->dim; i++)
            norm += (double)v->x[i] * v->x[i];
        if (!isfinite(norm) || (distance->opno == ANN_COSINE_OPERATOR && norm == 0.0)) {
            ann_unsupported(root, "query vector must be finite and cosine must be nonzero");
            return;
        }
    }
    Relids active = NULL;
    for (int i = 1; i < root->simple_rel_array_size; i++) {
        RelOptInfo* base = root->simple_rel_array[i];
        if (base != NULL && base->reloptkind == RELOPT_BASEREL)
            active = bms_add_member(active, i);
    }
    RelOptInfo* rel = var->varno < (Index)root->simple_rel_array_size ?
        root->simple_rel_array[var->varno] : NULL;
    if (rel == NULL || rel->rtekind != RTE_RELATION || rel->is_ustore ||
        rel->isPartitionedTable || rel->orientation != REL_ROW_ORIENTED ||
        !bms_is_member(var->varno, active)) {
        ann_unsupported(root, "vector target must be a surviving ordinary AStore relation");
        return;
    }
    if (!ann_target_is_preserved(root, var->varno)) {
        ann_unsupported(root, "join may produce rows without a real output vector target TID");
        return;
    }
    /* A user-written subquery keeps its own ORDER BY/LIMIT boundary.
     * Its external parameters use the native PARAM_EXEC dependency chain;
     * they do not turn this query block into a multi-relation route. */
    RangeTblEntry* rte = root->simple_rte_array[var->varno];
    Relation table = heap_open(rte->relid, NoLock);
    bool safe = table->rd_rel->relkind == RELKIND_RELATION &&
        table->rd_rel->relpersistence == RELPERSISTENCE_PERMANENT &&
        !RELATION_HAS_BUCKET(table) && !RowRelationIsCompressed(table) &&
        rte->securityQuals == NIL && rte->tablesample == NULL &&
        ann_vector_domain(root, rel, table, var, distance->opno == ANN_COSINE_OPERATOR);
    heap_close(table, NoLock);
    if (!safe) {
        ann_unsupported(root, "storage, security, or vector NULL/nonzero domain is unsupported");
        return;
    }
    DiskAnnFilterRequest* request = (DiskAnnFilterRequest*)palloc0(sizeof(DiskAnnFilterRequest));
    request->request_id = 1;
    request->target_rti = var->varno;
    request->vector_attno = var->varattno;
    request->distance_expr = distance;
    request->query_vector = vector;
    request->limit_expr = (Expr*)query->limitCount;
    request->qualification_relids = active;
    request->requested_mode = mode;
    root->ann_filter_request = request;
    ann_log_request(root, request);
    /* Private qualification paths request ctid explicitly. Do not widen
     * the relation target of ordinary competing scans for this identity. */
}

void ann_preserve_filter_candidate(PlannerInfo* root, IndexPath* path)
{
    DiskAnnFilterRequest* request = root->ann_filter_request;
    IndexOptInfo* index = path->indexinfo;
    if (request == NULL || index->relam != DISKANN_AM_OID ||
        index->rel->relid != request->target_rti || index->ncolumns != 1 ||
        index->nkeycolumns != 1 || index->indexkeys[0] != request->vector_attno ||
        index->indexprs != NIL || index->indpred != NIL || path->path.param_info != NULL ||
        list_length(path->indexorderbys) != 1)
        return;
    Relation relation = index_open(index->indexoid, NoLock);
    DiskAnnOptions* options = (DiskAnnOptions*)relation->rd_options;
    bool safe = (options == NULL || !options->enablePQ) && !RELATION_HAS_BUCKET(relation);
    index_close(relation, NoLock);
    if (safe) {
        /* AUTO also submits the native path to add_path, which may free
         * that root during dominance checks. Own the derivation template. */
        IndexPath* owned = (IndexPath*)palloc(sizeof(IndexPath));
        errno_t rc = memcpy_s(owned, sizeof(IndexPath), path, sizeof(IndexPath));
        securec_check(rc, "", "");
        owned->path.pathkeys = list_copy(path->path.pathkeys);
        owned->indexclauses = list_copy(path->indexclauses);
        owned->indexquals = list_copy(path->indexquals);
        owned->indexqualcols = list_copy(path->indexqualcols);
        owned->indexorderbys = list_copy(path->indexorderbys);
        owned->indexorderbycols = list_copy(path->indexorderbycols);
        if (request->requested_mode != DISKANN_FILTER_AUTO && root->parse->hintState != NULL) {
            owned->path.hint_value = 0;
            set_hint_value(index->rel, &owned->path, root->parse->hintState);
        }
        request->ann_candidates = lappend(request->ann_candidates, owned);
    }
}

/* The single-table source is a private root and private bitmap children.
 * No Path is passed to copyObject(), which does not support planner paths. */
static Path* ann_clone_bitmap(Path* path)
{
    Size size = IsA(path, IndexPath) ? sizeof(IndexPath) :
        IsA(path, BitmapAndPath) ? sizeof(BitmapAndPath) :
        IsA(path, BitmapOrPath) ? sizeof(BitmapOrPath) : 0;
    if (size == 0)
        return NULL;
    Path* result = (Path*)palloc(size);
    errno_t rc = memcpy_s(result, size, path, size);
    securec_check(rc, "", "");
    if (!IsA(path, IndexPath)) {
        List* original = IsA(path, BitmapAndPath) ?
            ((BitmapAndPath*)path)->bitmapquals : ((BitmapOrPath*)path)->bitmapquals;
        List* copies = NIL;
        ListCell* lc;
        foreach (lc, original) {
            Path* child = ann_clone_bitmap((Path*)lfirst(lc));
            if (child == NULL)
                return NULL;
            copies = lappend(copies, child);
        }
        if (IsA(path, BitmapAndPath))
            ((BitmapAndPath*)result)->bitmapquals = copies;
        else
            ((BitmapOrPath*)result)->bitmapquals = copies;
    }
    return result;
}

static Path* ann_clone_source(Path* path)
{
    Size size;
    switch (path->pathtype) {
        case T_SeqScan: size = sizeof(Path); break;
        case T_IndexScan: size = sizeof(IndexPath); break;
        case T_IndexOnlyScan: size = sizeof(IndexPath); break;
        case T_BitmapHeapScan: size = sizeof(BitmapHeapPath); break;
        case T_TidScan: size = sizeof(TidPath); break;
        case T_NestLoop: size = sizeof(NestPath); break;
        case T_HashJoin: size = sizeof(HashPath); break;
        case T_MergeJoin: size = sizeof(MergePath); break;
        case T_Material: size = sizeof(MaterialPath); break;
        case T_Unique: size = sizeof(UniquePath); break;
        default: return NULL;
    }
    if (IsA(path, IndexPath) && ((IndexPath*)path)->isAnnIndex)
        return NULL; /* an approximate input is never an exact qualification */
    Path* result = (Path*)palloc(size);
    errno_t rc = memcpy_s(result, size, path, size);
    securec_check(rc, "", "");
    /* Bitmap children are not candidates in the owning relation's list;
     * their immutable clauses may be shared; copy the child root explicitly. */
    if (path->pathtype == T_BitmapHeapScan) {
        Path* child = ann_clone_bitmap(((BitmapHeapPath*)path)->bitmapqual);
        if (child == NULL)
            return NULL;
        ((BitmapHeapPath*)result)->bitmapqual = child;
    }
    if (path->pathtype == T_NestLoop || path->pathtype == T_HashJoin || path->pathtype == T_MergeJoin) {
        JoinPath* join = (JoinPath*)result;
        join->outerjoinpath = ann_clone_source(((JoinPath*)path)->outerjoinpath);
        join->innerjoinpath = ann_clone_source(((JoinPath*)path)->innerjoinpath);
        join->joinrestrictinfo = list_copy(join->joinrestrictinfo);
        if (join->outerjoinpath == NULL || join->innerjoinpath == NULL)
            return NULL;
    } else if (path->pathtype == T_Material || path->pathtype == T_Unique) {
        Path** child = path->pathtype == T_Material ?
            &((MaterialPath*)result)->subpath : &((UniquePath*)result)->subpath;
        *child = ann_clone_source(*child);
        if (*child == NULL)
            return NULL;
    }
    /* Path targets and relation targets are mutable during private plan
     * construction; statistics/index metadata and expressions are borrowed. */
    if (path->parent != NULL) {
        result->parent = (RelOptInfo*)palloc(sizeof(RelOptInfo));
        rc = memcpy_s(result->parent, sizeof(RelOptInfo), path->parent, sizeof(RelOptInfo));
        securec_check(rc, "", "");
        result->parent->reltarget = (PathTarget*)palloc(sizeof(PathTarget));
        rc = memcpy_s(result->parent->reltarget, sizeof(PathTarget), path->parent->reltarget, sizeof(PathTarget));
        securec_check(rc, "", "");
        result->parent->reltarget->exprs = list_copy(path->parent->reltarget->exprs);
        result->parent->unique_for_rels = list_copy(path->parent->unique_for_rels);
        result->parent->non_unique_for_rels = list_copy(path->parent->non_unique_for_rels);
    }
    result->pathtarget = (PathTarget*)palloc(sizeof(PathTarget));
    rc = memcpy_s(result->pathtarget, sizeof(PathTarget), path->pathtarget, sizeof(PathTarget));
    securec_check(rc, "", "");
    result->pathtarget->exprs = list_copy(path->pathtarget->exprs);
    result->pathkeys = list_copy(path->pathkeys);
    return result;
}

static bool ann_is_join(Path* path)
{
    return path->pathtype == T_NestLoop || path->pathtype == T_HashJoin || path->pathtype == T_MergeJoin;
}

/* Propagate the identity through every real input that contains d. A top
 * join cannot project d.ctid unless its child actually supplies it. */
static void ann_supply_tid(PlannerInfo* root, Path* path, Index target)
{
    if (!bms_is_member(target, path->parent->relids))
        return;
    if (ann_is_join(path)) {
        ann_supply_tid(root, ((JoinPath*)path)->outerjoinpath, target);
        ann_supply_tid(root, ((JoinPath*)path)->innerjoinpath, target);
    } else if (path->pathtype == T_Material) {
        ann_supply_tid(root, ((MaterialPath*)path)->subpath, target);
    }
    Expr* tid = (Expr*)makeVar(target, SelfItemPointerAttributeNumber, TIDOID, -1, InvalidOid, 0);
    if (!list_member(path->pathtarget->exprs, tid)) {
        List* tlist = make_tlist_from_pathtarget(path->pathtarget);
        tlist = lappend(tlist, makeTargetEntry(tid, list_length(tlist) + 1, NULL, false));
        path->pathtarget = create_pathtarget(root, tlist);
        path->parent->reltarget = path->pathtarget;
    }
    if (path->parent->reloptkind == RELOPT_BASEREL) {
        RelOptInfo* rel = path->parent;
        Size bytes = sizeof(Relids) * (rel->max_attr - rel->min_attr + 1);
        Relids* needed = (Relids*)palloc(bytes);
        errno_t rc = memcpy_s(needed, bytes, rel->attr_needed, bytes);
        securec_check(rc, "", "");
        needed[SelfItemPointerAttributeNumber - rel->min_attr] = bms_make_singleton(0);
        rel->attr_needed = needed;
    }
}

/* Elision is a projection/multiplicity proof, not a requirement on ANN.
 * For non-unique matches or any business projection the output join stays. */
static bool ann_single_output(PlannerInfo* root, Path* path, Index target)
{
    if (path->pathtype == T_Material)
        return ann_single_output(root, ((MaterialPath*)path)->subpath, target);
    if (!ann_is_join(path))
        return path->parent->relid == target;
    JoinPath* join = (JoinPath*)path;
    bool left = bms_is_member(target, join->outerjoinpath->parent->relids);
    Path* retained = left ? join->outerjoinpath : join->innerjoinpath;
    Path* other = left ? join->innerjoinpath : join->outerjoinpath;
    bool one = left ? join->jointype == JOIN_SEMI || join->jointype == JOIN_ANTI :
        join->jointype == JOIN_RIGHT_SEMI || join->jointype == JOIN_RIGHT_ANTI;
    if (join->jointype == JOIN_INNER)
        one = innerrel_is_unique(root, retained->parent, other->parent, JOIN_INNER, join->joinrestrictinfo);
    return one && ann_single_output(root, retained, target);
}

static bool ann_target_projection(Path* path, Index target)
{
    Relids vars = pull_varnos((Node*)path->pathtarget->exprs);
    return bms_is_subset(vars, bms_make_singleton(target));
}

static void ann_join_extra(PlannerInfo* root, JoinPath* original, Path* outer, Path* inner,
    JoinType type, SpecialJoinInfo* sj, JoinPathExtraData* extra)
{
    errno_t rc = memset_s(sj, sizeof(*sj), 0, sizeof(*sj));
    securec_check(rc, "", "");
    sj->type = T_SpecialJoinInfo;
    sj->jointype = type;
    sj->min_lefthand = sj->syn_lefthand = outer->parent->relids;
    sj->min_righthand = sj->syn_righthand = inner->parent->relids;
    /* Native SpecialJoinInfo retains the original left-preserved logical
     * form even when a physical hash path reverses its inputs. Selectivity
     * support routines do not accept RIGHT_SEMI/RIGHT_ANTI as SJ types. */
    if (type == JOIN_RIGHT || type == JOIN_RIGHT_SEMI || type == JOIN_RIGHT_ANTI ||
        type == JOIN_RIGHT_ANTI_FULL) {
        sj->jointype = type == JOIN_RIGHT ? JOIN_LEFT : type == JOIN_RIGHT_SEMI ? JOIN_SEMI :
            type == JOIN_RIGHT_ANTI ? JOIN_ANTI : JOIN_LEFT_ANTI_FULL;
        sj->min_lefthand = sj->syn_lefthand = inner->parent->relids;
        sj->min_righthand = sj->syn_righthand = outer->parent->relids;
    }
    extra->sjinfo = sj;
    extra->inner_unique = original->inner_unique;
    if (type == JOIN_SEMI || type == JOIN_ANTI)
        compute_semi_anti_join_factors(root, outer->parent, inner->parent, type,
            sj, original->joinrestrictinfo, &extra->semifactors);
    else if (type == JOIN_RIGHT_SEMI || type == JOIN_RIGHT_ANTI) {
        JoinType left_type = type == JOIN_RIGHT_SEMI ? JOIN_SEMI : JOIN_ANTI;
        SemiAntiJoinFactors reverse;
        compute_semi_anti_join_factors(root, outer->parent, inner->parent, left_type,
            sj, original->joinrestrictinfo, &extra->semifactors);
        compute_semi_anti_join_factors(root, inner->parent, outer->parent, left_type,
            sj, original->joinrestrictinfo, &reverse);
        extra->semifactors.match_count = reverse.outer_match_frac;
    }
}

/* Reuse the native constructor and cost functions with the replacement
 * input. No plan-time accounting and no inferred hash/materialize reuse. */
static Path* ann_rebuild_join(PlannerInfo* root, JoinPath* original, Path* outer, Path* inner,
    bool probe, double rows)
{
    RelOptInfo* rel = (RelOptInfo*)palloc(sizeof(RelOptInfo));
    errno_t rc = memcpy_s(rel, sizeof(RelOptInfo), original->path.parent, sizeof(RelOptInfo));
    securec_check(rc, "", "");
    rel->rows = clamp_row_est(rows);
    List* restrictions = list_copy(original->joinrestrictinfo);
    if (!probe && original->path.pathtype == T_NestLoop &&
        original->innerjoinpath->param_info != NULL &&
        !bms_is_subset(PATH_REQ_OUTER(original->innerjoinpath), PATH_REQ_OUTER(inner))) {
        /* Native NL removes clauses enforced by its old parameterized
         * inner index. An independent ANN replacement no longer enforces
         * them: restore those clauses at their original output join scope.
         * Qualification and output still have independent state/ranges. */
        ListCell* lc;
        foreach (lc, original->innerjoinpath->param_info->ppi_clauses) {
            RestrictInfo* clause = (RestrictInfo*)lfirst(lc);
            if (bms_is_subset(clause->required_relids, rel->relids) &&
                !list_member_ptr(restrictions, clause))
                restrictions = lappend(restrictions, clause);
        }
    }
    JoinPath logical = *original;
    logical.joinrestrictinfo = restrictions;
    SpecialJoinInfo sj;
    JoinPathExtraData extra = {};
    JoinCostWorkspace work = {};
    ann_join_extra(root, &logical, outer, inner, original->jointype, &sj, &extra);
    Path* result;
    if (probe || original->path.pathtype == T_NestLoop) {
        Relids required = calc_nestloop_required_outer(outer, inner);
        initial_cost_nestloop(root, &work, original->jointype, outer, inner, &extra, 1);
        result = (Path*)create_nestloop_path(root, rel, original->jointype, &work, &extra,
            outer, inner, list_copy(restrictions), NIL, required, 1);
    } else if (original->path.pathtype == T_HashJoin) {
        List* clauses = ((HashPath*)original)->path_hashclauses;
        Relids required = calc_non_nestloop_required_outer(outer, inner);
        initial_cost_hashjoin(root, &work, original->jointype, clauses, outer, inner, &extra, 1);
        result = (Path*)create_hashjoin_path(root, rel, original->jointype, &work, &extra,
            outer, inner, list_copy(restrictions), required, list_copy(clauses), 1);
    } else {
        MergePath* merge = (MergePath*)original;
        Relids required = calc_non_nestloop_required_outer(outer, inner);
        List* outerkeys = pathkeys_contained_in(merge->outersortkeys, outer->pathkeys) ? NIL : merge->outersortkeys;
        List* innerkeys = pathkeys_contained_in(merge->innersortkeys, inner->pathkeys) ? NIL : merge->innersortkeys;
        /* Replacing an ordered B-tree input with ANN always needs the
         * original merge keys even when its old explicit-sort list was NIL. */
        if (outer->pathkeys == NIL && outerkeys == NIL)
            outerkeys = original->outerjoinpath->pathkeys;
        if (inner->pathkeys == NIL && innerkeys == NIL)
            innerkeys = original->innerjoinpath->pathkeys;
        initial_cost_mergejoin(root, &work, original->jointype, merge->path_mergeclauses,
            outer, inner, outerkeys, innerkeys, &extra);
        result = (Path*)create_mergejoin_path(root, rel, original->jointype, &work, &extra,
            outer, inner, list_copy(restrictions), NIL, required,
            list_copy(merge->path_mergeclauses), list_copy(outerkeys), list_copy(innerkeys));
    }
    result->pathtarget = original->path.pathtarget;
    /* These private roots bypass add_path. Preserve normal join hints and
     * child hint inheritance before comparing complete output routes. */
    set_hint_value(rel, result, root->parse->hintState);
    return result;
}

/* Replace the target scan, preserving every native join condition and
 * ON/WHERE placement. Independent ANN input has no fabricated outer binding. */
static Path* ann_output_route(PlannerInfo* root, Path* source, IndexPath* ann, double qualified)
{
    Index target = ann->ann_target_rti;
    if (!bms_is_member(target, source->parent->relids))
        return ann_clone_source(source);
    if (source->parent->relid == target)
        return (Path*)ann;
    if (source->pathtype == T_Material) {
        Path* child = ann_output_route(root, ((MaterialPath*)source)->subpath, ann, qualified);
        return child == NULL ? NULL : (Path*)create_material_path(child, ((MaterialPath*)source)->materialize_all);
    }
    if (!ann_is_join(source))
        return NULL;
    JoinPath* join = (JoinPath*)source;
    Path* outer = ann_output_route(root, join->outerjoinpath, ann, qualified);
    Path* inner = ann_output_route(root, join->innerjoinpath, ann, qualified);
    if (outer == NULL || inner == NULL)
        return NULL;
    double denominator = ann->ann_filter_mode == ANN_FILTER_POST ?
        Max(ann->indexinfo->rel->rows, 1.0) : Max(qualified, 1.0);
    double rows = ann->path.rows * Max(source->rows, 1.0) / denominator;
    return ann_rebuild_join(root, join, outer, inner, false, rows);
}

/* IN uses an actual target Tid Scan and already-native parameterized
 * business index paths. A missing safe probe is a mode capability failure,
 * never permission to run a complete business scan for every graph node. */
static Path* ann_probe_route(PlannerInfo* root, Path* source, TidPath* tid, Index target)
{
    if (source->parent->relid == target)
        return ann_clone_source((Path*)tid);
    if (!ann_is_join(source))
        return NULL;
    JoinPath* join = (JoinPath*)source;
    JoinPath reversed;
    if (!bms_is_member(target, join->outerjoinpath->parent->relids)) {
        if (join->jointype != JOIN_INNER)
            return NULL;
        /* The output hint may require a -> ANN(d), while an independent
         * candidate EXISTS probe must be Tid(d) -> keyed business access.
         * Only an inner join is commuted here; its original predicates and
         * projection stay unchanged. No external ANN binding is invented. */
        reversed = *join;
        reversed.outerjoinpath = join->innerjoinpath;
        reversed.innerjoinpath = join->outerjoinpath;
        join = &reversed;
    }
    Path* outer = ann_probe_route(root, join->outerjoinpath, tid, target);
    if (outer == NULL || join->innerjoinpath->parent->reloptkind != RELOPT_BASEREL)
        return NULL;
    RelOptInfo* business = root->simple_rel_array[join->innerjoinpath->parent->relid];
    Path* index = NULL;
    ListCell* lc;
    /* Include business probes captured before base add_path dominance.
     * A cheap unparameterized path must not erase the native keyed probe. */
    List* probes = list_concat(list_copy(business->pathlist), list_copy(root->ann_filter_request->ordinary_candidates));
    foreach (lc, probes) {
        Path* candidate = (Path*)lfirst(lc);
        if ((candidate->pathtype == T_IndexScan || candidate->pathtype == T_IndexOnlyScan) &&
            candidate->parent->relid == business->relid &&
            !((IndexPath*)candidate)->isAnnIndex && candidate->dop <= 1 &&
            candidate->param_info != NULL &&
            bms_is_subset(PATH_REQ_OUTER(candidate), outer->parent->relids) &&
            (index == NULL || candidate->total_cost < index->total_cost))
            index = candidate;
    }
    if (index == NULL)
        return NULL;
    IndexPath* heap_index = (IndexPath*)ann_clone_source(index);
    /* The baseline contract uses a heap Index Scan for the business probe.
     * Keep the real node/cost in agreement rather than relabel IndexOnly. */
    heap_index->path.pathtype = T_IndexScan;
    cost_index(heap_index, root, 1.0);
    return ann_rebuild_join(root, join, outer, (Path*)heap_index, true,
        source->rows / Max(root->simple_rel_array[target]->rows, 1.0));
}

/* Use native statistics and cost units. Unknown counts are not empty
 * relations, and intermediate work/cost estimates must remain finite. */
static double ann_count_estimate(double value, double fallback)
{
    if (!isfinite(value) || value < 0)
        value = isfinite(fallback) && fallback >= 0 ? fallback : 1.0;
    return Min(value, 1.0e100);
}

/* Estimate graph work from this index's topology, in native tuple units.
 * A beam limits retained nodes, not visited nodes. Sample bounded existing
 * pages once per index/request; never treat K or selectivity as graph work.
 * This is an estimate, not a runtime visit limit or a quality guarantee. */
static double ann_graph_member_estimate(IndexPath* original, double table_members)
{
    if (original->ann_estimated_members > 0)
        return original->ann_estimated_members;
    double full = Max(table_members, ann_count_estimate(original->indexinfo->tuples, table_members));
    double estimate = full;
    int samples = 0;
    double degrees = 0;
    double members = 0;
    if (full > 1024) {
        Relation index = index_open(original->indexinfo->indexoid, NoLock);
        DiskAnnMetaPageData meta;
        DiskANNGetMetaPageInfo(index, &meta);
        BlockNumber blocks = RelationGetNumberOfBlocks(index);
        bool valid = !meta.enablePQ && meta.nodeSize >= offsetof(DiskAnnNodePageData, pqcode) &&
            meta.nodeSize < BLCKSZ && meta.edgeSize >= sizeof(DiskAnnEdgePageData) &&
            meta.edgeSize < BLCKSZ && blocks > DISKANN_HEAD_BLKNO;
        BlockNumber nodes = valid ? blocks - DISKANN_HEAD_BLKNO : 0;
        int count = Min(nodes, (BlockNumber)32);
        for (int i = 0; valid && i < count; i++) {
            CHECK_FOR_INTERRUPTS();
            BlockNumber block = DISKANN_HEAD_BLKNO +
                (BlockNumber)(((uint64)nodes * i) / count);
            Buffer buffer = ReadBuffer(index, block);
            LockBuffer(buffer, BUFFER_LOCK_SHARE);
            Page page = BufferGetPage(buffer);
            bool usable = DiskAnnPageGetOpaque(page)->pageId == DISKANN_PAGE_ID &&
                PageGetMaxOffsetNumber(page) >= FirstOffsetNumber;
            if (usable) {
                ItemId item = PageGetItemId(page, FirstOffsetNumber);
                usable = ItemIdIsNormal(item) && ItemIdGetLength(item) >= sizeof(IndexTupleData) &&
                    ItemIdGetOffset(item) + ItemIdGetLength(item) <= ((PageHeader)page)->pd_special;
                if (usable) {
                    IndexTuple tuple = DiskAnnPageGetIndexTuple(page);
                    /* Node/edges live in reserved space after the vector
                     * tuple; they are not part of the line pointer length. */
                    usable = IndexTupleSize(tuple) <= ItemIdGetLength(item) &&
                        ItemIdGetOffset(item) + IndexTupleSize(tuple) + meta.nodeSize + meta.edgeSize <=
                            ((PageHeader)page)->pd_special;
                    if (usable) {
                        DiskAnnNodePage node = DiskAnnPageGetNode(tuple);
                        DiskAnnEdgePage edge = (DiskAnnEdgePage)((char*)node + meta.nodeSize);
                        usable = node->heaptidsLength <= DISKANN_HEAPTIDS && edge->count <= DISKANN_MAX_DEGREE;
                        /* Slave topology is owned by its master. A sample
                         * without enough master pages uses the full cost. */
                        if (usable && DiskAnnNodeIsMaster(node->tag)) {
                            samples++;
                            degrees += edge->count;
                            members += Max(node->heaptidsLength, (uint8)1);
                        }
                    }
                }
            }
            UnlockReleaseBuffer(buffer);
            valid = usable;
        }
        if (valid && samples >= count / 2 && samples > 0 && degrees > 0) {
            double degree = Min(Max(degrees / samples, 1.0), (double)DISKANN_MAX_DEGREE);
            double density = Min(Max(members / samples, 1.0), (double)DISKANN_HEAPTIDS);
            double beam = u_sess->datavec_ctx.diskann_probes;
            estimate = Min(full, (FROZEN_POINT_SIZE + beam * degree) * density);
        }
        index_close(index, NoLock);
    }
    original->ann_estimated_members = estimate;
    ereport(DEBUG2, (errmsg("DiskANN graph work estimate: full_members=%.0f samples=%d "
        "mean_degree=%.8f sample_density=%.8f beam=%d estimated_members=%.8f",
        full, samples, samples > 0 ? degrees / samples : 0.0,
        samples > 0 ? members / samples : 0.0, u_sess->datavec_ctx.diskann_probes, estimate)));
    return estimate;
}

static Cost ann_finite_cost(Cost value)
{
    return !isfinite(value) ? 1.0e100 : Min(Max(value, 0.0), 1.0e100);
}

/* Heap width statistics describe the stored datum, not the vector read by
 * the distance function. In particular an EXTERNAL vector can have width
 * 18 while its payload occupies several KB. Native scan/projection costs
 * do not bill that TOAST access. Use the same per-dereference correction
 * for ordinary output, ANN key validation and ANN output projection.
 *
 * EXTERNAL cannot compress: the mean width interpolates between a full
 * inline value and an external pointer. Do not apply that interpolation to
 * EXTENDED/MAIN, where a small datum could instead be compressed inline.
 * Missing statistics or dimensions retain the native estimate. No sampled
 * data pages, forced plan preference, or recall assumption is involved. */
static Cost ann_vector_detoast_cost(PlannerInfo* root)
{
    DiskAnnFilterRequest* request = root->ann_filter_request;
    Var* vector = (Var*)linitial(request->distance_expr->args);
    if (vector->vartypmod <= 0)
        return 0;
    Oid relid = root->simple_rte_array[request->target_rti]->relid;
    HeapTuple tuple = SearchSysCache2(ATTNUM, ObjectIdGetDatum(relid),
        Int16GetDatum(request->vector_attno));
    if (!HeapTupleIsValid(tuple))
        return 0;
    char storage = ((Form_pg_attribute)GETSTRUCT(tuple))->attstorage;
    ReleaseSysCache(tuple);
    if (storage != 'e')
        return 0;
    double width = get_attavgwidth(relid, request->vector_attno, false);
    double payload = offsetof(Vector, x) + (double)vector->vartypmod * sizeof(float);
    /* Small inline varlenas use a one-byte header. Those saved three bytes
     * are not evidence of out-of-line storage (vector(8): 40 -> 37). */
    double inline_width = payload - VARHDRSZ + VARHDRSZ_SHORT <= VARATT_SHORT_MAX ?
        payload - VARHDRSZ + VARHDRSZ_SHORT : payload;
    double pointer = TOAST_POINTER_SIZE;
    if (width <= 0 || inline_width <= pointer || width >= inline_width)
        return 0;
    double fraction = Min(Max((inline_width - width) / (inline_width - pointer), 0.0), 1.0);
    double chunks = ceil((payload - VARHDRSZ) / TOAST_MAX_CHUNK_SIZE);
    double random_cost;
    double sequential_cost;
    get_tablespace_page_costs(root->simple_rel_array[request->target_rti]->reltablespace,
        &random_cost, &sequential_cost);
    /* One leaf lookup plus the payload pages; upper B-tree levels are
     * cached, as in the native generic index cost model. */
    return fraction * ((1.0 + ceil(chunks / TOAST_TUPLES_PER_PAGE)) * random_cost +
        chunks * u_sess->attr.attr_sql.cpu_tuple_cost);
}

static void ann_cost_key_detoast(PlannerInfo* root, IndexPath* path)
{
    double n = ann_count_estimate(path->indexinfo->rel->tuples, 1.0);
    double checks = path->ann_filter_mode == ANN_FILTER_PRE ?
        path->ann_estimated_members * path->annQualSelectivity :
        path->ann_filter_mode == ANN_FILTER_IN ? path->ann_estimated_members :
        path->indexselectivity * n;
    Cost extra = ann_finite_cost(checks * ann_vector_detoast_cost(root));
    path->path.startup_cost = ann_finite_cost(path->path.startup_cost + extra);
    path->path.total_cost = ann_finite_cost(path->path.total_cost + extra);
    ereport(DEBUG2, (errmsg("DiskANN vector key TOAST cost: mode=%d checks=%.8f extra=%.8f",
        (int)path->ann_filter_mode, checks, extra)));
}

/* Only the private source/probe target changes. The relation and its
 * ordinary competing paths retain their original target and costs. */
static void ann_qualification_target(PlannerInfo* root, Path* path, Expr* expr, double rows)
{
    PathTarget* target = create_pathtarget(root,
        list_make1(makeTargetEntry(expr, 1, NULL, false)));
    if (root->parse->is_flt_frame) {
        Cost old_startup = path->pathtarget->cost.startup;
        Cost old_run = path->pathtarget->cost.per_tuple * path->rows;
        path->startup_cost = ann_finite_cost(path->startup_cost - old_startup + target->cost.startup);
        path->total_cost = ann_finite_cost(path->total_cost - old_startup - old_run +
            target->cost.startup + target->cost.per_tuple * rows);
    }
    path->pathtarget = target;
    path->rows = rows;
}

/* Qualification has its own projection contract. Carry only identities and
 * values needed by its native joins/parameterized children, then recost the
 * private tree with those widths. Final business output keeps its own tree. */
static List* ann_required_for(Path* path, List* values)
{
    List* result = NIL;
    ListCell* lc;
    foreach (lc, values) {
        Node* value = (Node*)lfirst(lc);
        Relids relids = pull_varnos(value);
        if (!bms_is_empty(relids) && bms_is_subset(relids, path->parent->relids) &&
            !list_member(result, value))
            result = lappend(result, value);
    }
    if (result == NIL)
        result = list_make1(makeBoolConst(true, false));
    return result;
}

static Path* ann_recost_qualification(PlannerInfo* root, Path* path, List* required)
{
    /* Native Unique needs its complete key target; retain that subtree and
     * its matching costs rather than dropping an unproven uniqueness key. */
    if (path->pathtype == T_Unique)
        return path;
    List* tlist = NIL;
    ListCell* lc;
    foreach (lc, required)
        tlist = lappend(tlist, makeTargetEntry((Expr*)lfirst(lc), list_length(tlist) + 1, NULL, false));
    path->pathtarget = create_pathtarget(root, tlist);
    path->parent->reltarget = path->pathtarget;
    if (ann_is_join(path)) {
        JoinPath* join = (JoinPath*)path;
        List* clauses = get_actual_clauses(join->joinrestrictinfo);
        if (join->innerjoinpath->param_info != NULL)
            clauses = list_concat(clauses, get_actual_clauses(join->innerjoinpath->param_info->ppi_clauses));
        List* needed = pull_var_clause((Node*)required, PVC_RECURSE_AGGREGATES, PVC_INCLUDE_PLACEHOLDERS);
        needed = list_concat(needed,
            pull_var_clause((Node*)clauses, PVC_RECURSE_AGGREGATES, PVC_INCLUDE_PLACEHOLDERS));
        Path* outer = ann_recost_qualification(root, join->outerjoinpath,
            ann_required_for(join->outerjoinpath, needed));
        Path* inner = ann_recost_qualification(root, join->innerjoinpath,
            ann_required_for(join->innerjoinpath, needed));
        return ann_rebuild_join(root, join, outer, inner, false, path->rows);
    }
    if (path->pathtype == T_Material) {
        MaterialPath* material = (MaterialPath*)path;
        Path* child = ann_recost_qualification(root, material->subpath, required);
        return (Path*)create_material_path(child, material->materialize_all);
    }
    double rows = path->rows;
    if (path->pathtype == T_SeqScan)
        cost_seqscan(path, root, path->parent, path->param_info);
    else if (IsA(path, IndexPath))
        cost_index((IndexPath*)path, root, 1.0);
    else if (path->pathtype == T_TidScan)
        cost_tidscan(path, root, path->parent, ((TidPath*)path)->tidquals);
    else if (path->pathtype == T_BitmapHeapScan)
        cost_bitmap_heap_scan(path, root, path->parent, path->param_info,
            ((BitmapHeapPath*)path)->bitmapqual, 1.0);
    path->rows = rows;
    return path;
}

/* Compare a complete route, including its final projection, real native
 * Sort and LIMIT. These temporary cost records are not executor nodes and
 * are never added to the child Path costs (which would bill Sort twice). */
static Cost ann_complete_route_cost(PlannerInfo* root, Path* path)
{
    PathTarget* final_target = create_pathtarget(root, root->parse->targetList);
    Cost paid_startup = root->parse->is_flt_frame ? path->pathtarget->cost.startup : 0;
    Cost paid_tuple = root->parse->is_flt_frame ? path->pathtarget->cost.per_tuple : 0;
    Cost projection_start = Max(final_target->cost.startup - paid_startup, 0.0);
    Cost projection_run = Max(final_target->cost.per_tuple - paid_tuple, 0.0) * path->rows;
    Cost detoast = path->rows * ann_vector_detoast_cost(root);
    projection_run += detoast;
    int64 k = DatumGetInt64(((Const*)root->ann_filter_request->limit_expr)->constvalue);
    Path sorted = {};
    sorted.startup_cost = path->startup_cost + projection_start;
    sorted.total_cost = path->total_cost + projection_start + projection_run;
    bool ordered = pathkeys_contained_in(root->sort_pathkeys, path->pathkeys);
    if (!ordered)
        cost_sort(&sorted, root->sort_pathkeys, sorted.total_cost, path->rows,
            final_target->width, 0, u_sess->attr.attr_memory.work_mem, (double)k, false);
    Plan input = {};
    input.type = ordered ? T_Result : T_Sort;
    input.exec_type = EXEC_ON_COORDS;
    input.startup_cost = sorted.startup_cost;
    input.total_cost = sorted.total_cost;
    input.plan_rows = path->rows;
    Plan limited = input;
    cost_limit(&limited, &input, 0, k);
    Cost cost = ann_finite_cost(limited.total_cost);
    ereport(DEBUG2, (errmsg("DiskANN complete route cost: node=%d rows=%.8f input=%.8f "
        "projection=%.8f sort_limit=%.8f complete=%.8f native_ann=%s vector_toast=%.8f",
        (int)path->pathtype, path->rows, path->total_cost, projection_start + projection_run,
        Max(cost - path->total_cost - projection_start - projection_run, 0.0), cost,
        IsA(path, IndexPath) && ((IndexPath*)path)->isAnnIndex &&
            !((IndexPath*)path)->ann_filter_enabled ? "true" : "false", detoast)));
    return cost;
}

/* A direct single-table LIMIT reaches the legacy ordered executor. That
 * executor finishes each beam, doubles it until K qualifying members, and
 * sorts the admitted heap rows before publication. Under a Join that bound
 * does not reach the scan; keep the original exhaustive native cost there.
 * Use a private POST cost record for the shared graph/heap work, subtract
 * only its new identity/cache operations, and bill the legacy full Sort.
 * This record never becomes a plan and OFF retains its original costs. */
static void ann_recost_native_single(PlannerInfo* root, RelOptInfo* rel, IndexPath* original)
{
    DiskAnnFilterRequest* request = root->ann_filter_request;
    if (bms_num_members(request->qualification_relids) != 1 || original->ann_filter_enabled ||
        !original->isAnnIndex || original->indexinfo->relam != DISKANN_AM_OID ||
        !pathkeys_contained_in(root->sort_pathkeys, original->path.pathkeys))
        return;
    double n = ann_count_estimate(rel->tuples, Max(rel->rows, 1.0));
    double full = Max(n, ann_count_estimate(original->indexinfo->tuples, n));
    double sel = clauselist_selectivity(root, rel->baserestrictinfo, rel->relid, JOIN_INNER, NULL);
    sel = isfinite(sel) ? Min(Max(sel, 0.0), 1.0) : 1.0;
    /* Use the owned sample already taken for the new routes. No second
     * page sample, no K-based reduction in graph work. */
    double members = full;
    ListCell* lc;
    foreach (lc, request->ann_candidates) {
        IndexPath* saved = (IndexPath*)lfirst(lc);
        if (saved->indexinfo == original->indexinfo) {
            members = saved->ann_estimated_members > 0 ? saved->ann_estimated_members : full;
            break;
        }
    }
    IndexPath work = *original;
    work.ann_filter_enabled = true;
    work.ann_filter_mode = ANN_FILTER_POST;
    work.ann_target_rti = request->target_rti;
    work.ann_distance_expr = (Expr*)request->distance_expr;
    work.annQualSelectivity = sel;
    double initial = u_sess->datavec_ctx.diskann_probes;
    int64 k = DatumGetInt64(((Const*)request->limit_expr)->constvalue);
    Cost total = 0;
    double rows = 0;
    int rounds = 0;
    double beam = initial;
    for (; rounds < MAX_SEARCH_ITERATION; rounds++, beam *= SEARCH_DOUBLE) {
        work.ann_initial_search_nodes = (int)Min(beam, (double)INT_MAX);
        work.ann_estimated_members = Min(full, members * (beam / initial));
        cost_index(&work, root, 1.0);
        rows = work.path.rows;
        /* For POST, the native heap model already bills all C visits;
         * only the five new hash/key/delivery operations are additional. */
        double delivery = work.indexselectivity * n;
        /* Legacy ordered scans evaluate distance only after their scalar
         * qual when reuse is off. With reuse enabled each delivered heap
         * key is checked instead. Do not bill the new callback protocol
         * to this native competitor. */
        double dereferences = u_sess->attr.attr_sql.ann_orderby_distance_reuse ==
            ANN_ORDERBY_REUSE_OFF ? rows : delivery;
        total += dereferences * ann_vector_detoast_cost(root);
        total += Max(work.path.total_cost - 5.0 * delivery *
            u_sess->attr.attr_sql.cpu_operator_cost, 0.0);
        if (rows >= (double)k || beam >= full)
            break;
    }
    Path sorted = {};
    cost_sort(&sorted, NIL, total, rows, original->path.pathtarget->width,
        0, u_sess->attr.attr_memory.work_mem, -1, false);
    original->path.startup_cost = sorted.startup_cost;
    original->path.total_cost = sorted.total_cost;
    original->path.rows = rows;
    ereport(DEBUG2, (errmsg("DiskANN native bounded cost: beam=%.0f rounds=%d "
        "members=%.8f rows=%.8f total=%.8f", beam, Min(rounds + 1, MAX_SEARCH_ITERATION),
        members, rows, original->path.total_cost)));
}

/* The existing complete-output gate can run before expensive private
 * qualification/output construction. Keep the final gate as a backstop. */
static bool ann_auto_reject_fixed_rows(PlannerInfo* root, double rows)
{
    if (root->ann_filter_request->requested_mode != DISKANN_FILTER_AUTO)
        return false;
    int64 k = DatumGetInt64(((Const*)root->ann_filter_request->limit_expr)->constvalue);
    if (rows >= (double)k || !isfinite(rows))
        return false;
    ereport(DEBUG2, (errmsg("DiskANN fixed route excluded from AUTO: "
        "estimated_output=%.8f requested_rows=%ld; no feedback expansion", rows, (long)k)));
    return true;
}

/* A complete blocking Sort/Limit cannot cost less than its ANN child's
 * startup. Keep the ordinary route's native hint priority: a lower-priority
 * path is never a bound for a higher-priority ANN replacement. */
static Cost ann_ordinary_complete_bound(PlannerInfo* root, RelOptInfo* rel, int* hint)
{
    Cost best = 1.0e100;
    *hint = INT_MIN;
    ListCell* lc;
    foreach (lc, rel->pathlist) {
        Path* path = (Path*)lfirst(lc);
        if (path->param_info != NULL || path->dop > 1)
            continue;
        if (path->hint_value < *hint)
            continue;
        if (path->hint_value > *hint) {
            *hint = path->hint_value;
            best = 1.0e100;
        }
        best = Min(best, ann_complete_route_cost(root, path));
    }
    return best;
}

static void ann_choose_complete_route(PlannerInfo* root, RelOptInfo* rel, List* candidates)
{
    Path* winner = NULL;
    Cost best = 0;
    List* alternatives = list_copy(candidates);
    if (root->ann_filter_request->requested_mode == DISKANN_FILTER_AUTO)
        alternatives = list_concat(alternatives, list_copy(rel->pathlist));
    ListCell* lc;
    foreach (lc, alternatives) {
        Path* path = (Path*)lfirst(lc);
        if (path->param_info != NULL || path->dop > 1)
            continue;
        /* P1/P2 have a fixed search budget and no feedback expansion.
         * AUTO must not pick a new complete route predicted to underfill
         * the user's output LIMIT. Count complete Join rows, including
         * business multiplicity; never substitute K target identities.
         * Explicit modes remain available for underfill diagnostics. */
        if (list_member_ptr(candidates, path) && ann_auto_reject_fixed_rows(root, path->rows))
            continue;
        if (root->ann_filter_request->requested_mode == DISKANN_FILTER_AUTO && IsA(path, IndexPath))
            ann_recost_native_single(root, rel, (IndexPath*)path);
        Cost cost = ann_complete_route_cost(root, path);
        if (winner == NULL || path->hint_value > winner->hint_value ||
            (path->hint_value == winner->hint_value && cost < best)) {
            winner = path;
            best = cost;
        }
    }
    Assert(winner != NULL);
    /* Publish the missing projection work in the selected native path too,
     * so the later Sort/Limit and EXPLAIN see the same cost as the chooser.
     * Candidate comparison itself never mutates a shared ordinary path. */
    winner->total_cost = ann_finite_cost(winner->total_cost +
        winner->rows * ann_vector_detoast_cost(root));
    /* Each input was privately saved before dominance checks. Only the
     * winner reaches normal plan creation; no runtime alternate survives. */
    rel->pathlist = list_make1(winner);
    set_cheapest(rel);
}

static double ann_join_pass_fraction(PlannerInfo* root, Path* source, RelOptInfo* target)
{
    double base = clauselist_selectivity(root, target->baserestrictinfo, target->relid, JOIN_INNER, NULL);
    double fraction = 0.5; /* native complete correlated SubPlan fallback */
    if (ann_is_join(source)) {
        JoinPath* join = (JoinPath*)source;
        bool left = join->outerjoinpath->parent->relid == target->relid;
        bool right = join->innerjoinpath->parent->relid == target->relid;
        if ((left && (join->jointype == JOIN_INNER || join->jointype == JOIN_SEMI || join->jointype == JOIN_ANTI)) ||
            (right && (join->jointype == JOIN_INNER || join->jointype == JOIN_RIGHT_SEMI || join->jointype == JOIN_RIGHT_ANTI))) {
            SpecialJoinInfo sj;
            JoinPathExtraData extra = {};
            ann_join_extra(root, join, left ? join->outerjoinpath : join->innerjoinpath,
                left ? join->innerjoinpath : join->outerjoinpath, JOIN_SEMI, &sj, &extra);
            fraction = join->jointype == JOIN_ANTI || join->jointype == JOIN_RIGHT_ANTI ? 1.0 - extra.semifactors.outer_match_frac :
                extra.semifactors.outer_match_frac;
        }
    }
    return Min(Max(isfinite(base * fraction) ? base * fraction : 0.5, 0.0), 1.0);
}

static void ann_derive_join_paths(PlannerInfo* root, RelOptInfo* rel)
{
    DiskAnnFilterRequest* request = root->ann_filter_request;
    RelOptInfo* target = root->simple_rel_array[request->target_rti];
    double n = ann_count_estimate(target->tuples, Max(target->rows, 1.0));
    double scalar = clauselist_selectivity(root, target->baserestrictinfo, target->relid, JOIN_INNER, NULL);
    int ordinary_hint = INT_MIN;
    Cost ordinary_bound = request->requested_mode == DISKANN_FILTER_AUTO ?
        ann_ordinary_complete_bound(root, rel, &ordinary_hint) : -1.0;
    HintState* hints = root->parse->hintState;
    /* Native output rebuilding retains the same Join methods and relids.
     * With no scan/stream/parameter hints its hint score equals the source
     * score (or the ANN scan score when the output Join is elided). Unknown
     * hint changes still require the actual private tree and final chooser. */
    bool known_join_hints = hints == NULL || (hints->scan_hint == NIL &&
        hints->stream_hint == NIL && hints->predpush_same_level_hint == NIL);
    List* candidates = NIL;
    ListCell* sc;
    ListCell* ic;
    request->deriving = true;
    foreach (sc, request->ordinary_candidates) {
        Path* source = (Path*)lfirst(sc);
        if (!ann_is_join(source))
            continue;
        double sel = ann_join_pass_fraction(root, source, target);
        double qualified = n * sel;
        bool elide = ann_single_output(root, source, request->target_rti) &&
            ann_target_projection(source, request->target_rti);
        foreach (ic, request->ann_candidates) {
            IndexPath* original = (IndexPath*)lfirst(ic);
            for (int mode = DISKANN_FILTER_PRE; mode <= DISKANN_FILTER_POST; mode++) {
                if (request->requested_mode != DISKANN_FILTER_AUTO && request->requested_mode != mode)
                    continue;
                IndexPath* path = (IndexPath*)palloc(sizeof(IndexPath));
                errno_t rc = memcpy_s(path, sizeof(IndexPath), original, sizeof(IndexPath));
                securec_check(rc, "", "");
                path->ann_filter_enabled = true;
                path->ann_filter_mode = mode == DISKANN_FILTER_PRE ? ANN_FILTER_PRE :
                    mode == DISKANN_FILTER_IN ? ANN_FILTER_IN : ANN_FILTER_POST;
                path->ann_request_id = request->request_id;
                path->ann_target_rti = request->target_rti;
                path->ann_qualification_relids = bms_copy(request->qualification_relids);
                path->ann_query_vector = request->query_vector;
                path->ann_distance_expr = (Expr*)request->distance_expr;
                path->ann_limit_expr = request->limit_expr;
                path->ann_output_join_required = mode == DISKANN_FILTER_POST || !elide;
                path->ann_initial_search_nodes = u_sess->datavec_ctx.diskann_probes;
                path->ann_filter_work_mem = u_sess->datavec_ctx.diskann_filter_work_mem;
                path->ann_candidate_tid_param = -1;
                path->ann_eligibility_path = NULL;
                path->annQuals = path->annQualCols = NIL;
                path->annQualSelectivity = mode == DISKANN_FILTER_POST ? scalar : sel;
                path->ann_estimated_members = ann_graph_member_estimate(original, n);
                path->ann_estimated_probe_calls = mode == DISKANN_FILTER_IN ? path->ann_estimated_members : 0;
                path->path.pathkeys = NIL;
                bool costed = request->requested_mode == DISKANN_FILTER_AUTO;
                if (costed) {
                    cost_index(path, root, 1.0);
                    ann_cost_key_detoast(root, path);
                    double denominator = mode == DISKANN_FILTER_POST ?
                        Max(path->indexinfo->rel->rows, 1.0) : Max(qualified, 1.0);
                    /* Match ann_output_route and ann_rebuild_join rounding,
                     * including LIMIT 1 and nonunique business matches. */
                    double rows = path->ann_output_join_required ?
                        clamp_row_est(path->path.rows * Max(source->rows, 1.0) / denominator) : path->path.rows;
                    if (ann_auto_reject_fixed_rows(root, rows))
                        continue;
                    int output_hint = path->ann_output_join_required ? source->hint_value : path->path.hint_value;
                    if (known_join_hints && ordinary_bound >= 0 && ordinary_hint >= output_hint &&
                        (ordinary_hint > output_hint || path->path.startup_cost > ordinary_bound)) {
                        ereport(DEBUG2, (errmsg("DiskANN fixed route excluded by startup bound: "
                            "mode=%d startup=%.8f ordinary_complete=%.8f hint=%d ordinary_hint=%d; no private route build",
                            (int)path->ann_filter_mode, path->path.startup_cost, ordinary_bound,
                            output_hint, ordinary_hint)));
                        continue;
                    }
                }
                Cost qualification = 0;
                if (mode == DISKANN_FILTER_PRE) {
                    Path* owned = ann_clone_source(source);
                    ann_supply_tid(root, owned, request->target_rti);
                    owned = ann_recost_qualification(root, owned, list_make1(makeVar(request->target_rti,
                        SelfItemPointerAttributeNumber, TIDOID, -1, InvalidOid, 0)));
                    path->ann_eligibility_path = owned;
                    /* Duplicate business rows still cost hash-set insertion attempts. */
                    qualification = owned->total_cost +
                        Max(qualified, source->rows) * u_sess->attr.attr_sql.cpu_tuple_cost;
                } else if (mode == DISKANN_FILTER_IN) {
                    Param* param = makeNode(Param);
                    param->paramkind = PARAM_EXEC;
                    param->paramid = SS_assign_special_param(root);
                    param->paramtype = TIDOID;
                    param->paramtypmod = -1;
                    param->location = -1;
                    Expr* clause = make_opclause(TIDEqualOperator, BOOLOID, false,
                        (Expr*)makeVar(request->target_rti, SelfItemPointerAttributeNumber,
                            TIDOID, -1, InvalidOid, 0), (Expr*)param, InvalidOid, InvalidOid);
                    TidPath* tid = create_tidscan_path(root, target, list_make1(clause));
                    /* One physical target, retaining the native join-key projection. */
                    tid->path.rows = 1;
                    Path* probe = ann_probe_route(root, source, tid, request->target_rti);
                    if (probe == NULL || PATH_REQ_OUTER(probe) != NULL)
                        continue;
                    probe = ann_recost_qualification(root, probe, list_make1(makeBoolConst(true, false)));
                    path->ann_eligibility_path = probe;
                    path->ann_candidate_tid_param = param->paramid;
                    qualification = path->ann_estimated_probe_calls * (probe->startup_cost +
                        Max(probe->total_cost - probe->startup_cost, 0.0) / Max(probe->rows, 1.0));
                }
                if (!costed) {
                    cost_index(path, root, 1.0);
                    ann_cost_key_detoast(root, path);
                }
                path->path.startup_cost = ann_finite_cost(path->path.startup_cost + qualification);
                path->path.total_cost = ann_finite_cost(path->path.total_cost + qualification);
                Path* complete;
                if (mode != DISKANN_FILTER_POST && elide) {
                    /* Physical index ownership stays d; logical output is the
                     * complete relation with a proven one-row target projection. */
                    path->path.parent = rel;
                    path->path.pathtarget = source->pathtarget;
                    complete = (Path*)path;
                } else {
                    complete = ann_output_route(root, source, path, qualified);
                }
                if (complete == NULL || PATH_REQ_OUTER(complete) != NULL)
                    continue;
                candidates = lappend(candidates, complete);
                ereport(DEBUG2, (errmsg("DiskANN join route: mode=%d target_rti=%u output_join=%s "
                    "eligible_targets=%.8f candidate_rows=%.8f output_rows=%.8f qualification=%.8f total=%.8f "
                    "members=%.8f probes=%.8f qualification_rows=%.8f qualification_width=%d",
                    (int)path->ann_filter_mode, request->target_rti, path->ann_output_join_required ? "yes" : "no",
                    qualified, path->path.rows, complete->rows, qualification, complete->total_cost,
                    path->ann_estimated_members, path->ann_estimated_probe_calls,
                    path->ann_eligibility_path == NULL ? 0.0 : path->ann_eligibility_path->rows,
                    path->ann_eligibility_path == NULL ? 0 : path->ann_eligibility_path->pathtarget->width)));
            }
        }
    }
    if (candidates == NIL) {
        request->deriving = false;
        if (request->requested_mode == DISKANN_FILTER_AUTO)
            ann_choose_complete_route(root, rel, NIL);
        else
            ann_unsupported(root, "no complete native join route with a safe qualification probe is available");
        return;
    }
    ann_choose_complete_route(root, rel, candidates);
    request->deriving = false;
}

void ann_derive_filter_paths(PlannerInfo* root, RelOptInfo* rel)
{
    DiskAnnFilterRequest* request = root->ann_filter_request;
    if (request == NULL)
        return;
    if (!bms_equal(rel->relids, request->qualification_relids)) {
        ann_unsupported(root, "qualification relation does not cover the complete request domain");
        return;
    }
    if (request->ann_candidates == NIL) {
        ann_unsupported(root, "no supported DiskANN index matches ORDER BY");
        return;
    }
    if (bms_num_members(request->qualification_relids) > 1) {
        ann_derive_join_paths(root, rel);
        return;
    }
    if (request->ann_candidates == NIL) {
        ann_unsupported(root, "no supported DiskANN index matches ORDER BY");
        return;
    }
    Path* source = NULL;
    ListCell* lc;
    foreach (lc, request->ordinary_candidates) {
        Path* path = (Path*)lfirst(lc);
        if (path->param_info == NULL && path->dop <= 1 &&
            !(IsA(path, IndexPath) && ((IndexPath*)path)->isAnnIndex) &&
            (path->pathtype == T_SeqScan || path->pathtype == T_IndexScan ||
             path->pathtype == T_BitmapHeapScan || path->pathtype == T_TidScan) &&
            (source == NULL || path->hint_value > source->hint_value ||
             (path->hint_value == source->hint_value && path->total_cost < source->total_cost)))
            source = path;
    }
    Path* private_source = source == NULL ? NULL : ann_clone_source(source);
    if (private_source == NULL) {
        ann_unsupported(root, "no owned ordinary qualification source is available");
        return;
    }
    ann_qualification_target(root, private_source,
        (Expr*)makeVar(request->target_rti, SelfItemPointerAttributeNumber,
            TIDOID, -1, InvalidOid, 0), private_source->rows);
    double n = ann_count_estimate(rel->tuples, Max(rel->rows, 1.0));
    double sel = clauselist_selectivity(root, rel->baserestrictinfo, rel->relid, JOIN_INNER, NULL);
    sel = isfinite(sel) ? Min(Max(sel, 0.0), 1.0) : 1.0;
    List* candidates = NIL;
    foreach (lc, request->ann_candidates) {
        IndexPath* original = (IndexPath*)lfirst(lc);
        for (int mode = DISKANN_FILTER_PRE; mode <= DISKANN_FILTER_POST; mode++) {
            if (request->requested_mode != DISKANN_FILTER_AUTO && request->requested_mode != mode)
                continue;
            IndexPath* path = (IndexPath*)palloc(sizeof(IndexPath));
            errno_t rc = memcpy_s(path, sizeof(IndexPath), original, sizeof(IndexPath));
            securec_check(rc, "", "");
            path->ann_filter_enabled = true;
            path->ann_filter_mode = mode == DISKANN_FILTER_PRE ? ANN_FILTER_PRE :
                mode == DISKANN_FILTER_IN ? ANN_FILTER_IN : ANN_FILTER_POST;
            path->ann_request_id = request->request_id;
            path->ann_target_rti = request->target_rti;
            path->ann_qualification_relids = bms_copy(request->qualification_relids);
            path->ann_query_vector = request->query_vector;
            path->ann_distance_expr = (Expr*)request->distance_expr;
            path->ann_limit_expr = request->limit_expr;
            path->ann_initial_search_nodes = u_sess->datavec_ctx.diskann_probes;
            path->ann_filter_work_mem = u_sess->datavec_ctx.diskann_filter_work_mem;
            path->ann_candidate_tid_param = -1;
            path->annQuals = NIL; /* never activate the INCLUDE protocol */
            path->annQualCols = NIL;
            path->annQualSelectivity = sel;
            path->ann_estimated_members = ann_graph_member_estimate(original, n);
            path->ann_estimated_probe_calls = mode == DISKANN_FILTER_IN ?
                path->ann_estimated_members : 0.0; /* no unmeasured cache discount */
            path->path.pathkeys = NIL; /* the real Sort remains above ANN */
            bool costed = request->requested_mode == DISKANN_FILTER_AUTO;
            if (costed) {
                cost_index(path, root, 1.0);
                ann_cost_key_detoast(root, path);
                if (ann_auto_reject_fixed_rows(root, path->path.rows))
                    continue;
            }
            if (mode == DISKANN_FILTER_PRE)
                path->ann_eligibility_path = ann_clone_source(private_source);
            if (mode == DISKANN_FILTER_IN) {
                Param* param = makeNode(Param);
                param->paramkind = PARAM_EXEC;
                param->paramid = SS_assign_special_param(root);
                param->paramtype = TIDOID;
                param->paramtypmod = -1;
                param->location = -1;
                Var* tid = makeVar(request->target_rti, SelfItemPointerAttributeNumber,
                    TIDOID, -1, InvalidOid, 0);
                Expr* qual = make_opclause(TIDEqualOperator, BOOLOID, false,
                    (Expr*)tid, (Expr*)param, InvalidOid, InvalidOid);
                TidPath* probe = create_tidscan_path(root, rel, list_make1(qual));
                ann_qualification_target(root, &probe->path, (Expr*)makeBoolConst(true, false), 1.0);
                path->ann_eligibility_path = (Path*)probe;
                path->ann_candidate_tid_param = param->paramid;
            }
            if (!costed) {
                cost_index(path, root, 1.0);
                ann_cost_key_detoast(root, path);
            }
            Cost qualification = 0;
            if (mode == DISKANN_FILTER_PRE) {
                qualification = private_source->total_cost + n * sel * u_sess->attr.attr_sql.cpu_tuple_cost;
            } else if (mode == DISKANN_FILTER_IN) {
                Path* probe = path->ann_eligibility_path;
                /* The same split as correlated EXISTS in cost_subplan.
                 * Candidate changes require startup again; no one-time
                 * materialization or outer NL multiplier is assumed. */
                Cost per_call = probe->startup_cost +
                    Max(probe->total_cost - probe->startup_cost, 0.0) / Max(probe->rows, 1.0);
                qualification = path->ann_estimated_probe_calls * per_call;
            }
            /* PRE build and IN synchronous probes finish inside the
             * blocking first search, before any candidate is delivered. */
            path->path.startup_cost = ann_finite_cost(path->path.startup_cost + qualification);
            path->path.total_cost = ann_finite_cost(path->path.total_cost + qualification);
            ereport(DEBUG2, (errmsg("DiskANN filter cost: mode=%d members=%.0f probes=%.0f "
                "selectivity=%.8f rows=%.0f qualification=%.8f startup=%.8f total=%.8f",
                (int)path->ann_filter_mode, path->ann_estimated_members, path->ann_estimated_probe_calls,
                sel, path->path.rows, qualification, path->path.startup_cost, path->path.total_cost)));
            candidates = lappend(candidates, path);
        }
    }
    /* Forced mode keeps its actual cost, rather than introducing a negative
     * preference. Ordinary qualification paths remain privately owned. */
    ann_choose_complete_route(root, rel, candidates);
}
