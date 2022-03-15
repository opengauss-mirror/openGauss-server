/*
 * Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 *          http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 *
 * The function generate_unique_queryid() needs to be called when generating the queryid
 * and the function normalized_unique_querystring() is called when generating the query text.
 * -------------------------------------------------------------------------
 *
 * unique_query.cpp
 *   functions for user stat, such as login/logout counter
 *
 * IDENTIFICATION
 *    src/gausskernel/cbb/instruments/utils/unique_query.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"
#include "securec.h"

#include <math.h>
#include <sys/stat.h>

#include "miscadmin.h"
#include "nodes/nodes.h"
#include "nodes/pg_list.h"
#include "nodes/parsenodes.h"
#include "nodes/primnodes.h"

#include "c.h"

#include <unistd.h>

#include "access/hash.h"
#include "parser/scanner.h"
#include "mb/pg_wchar.h"
#include "instruments/unique_query.h"
#include "instruments/instr_slow_query.h"
#include "pgstat.h"

const int JUMBLE_SIZE = 1024; /* query serialization buffer size */
const int CLOCATIONS_BUF_SIZE = 32;

/*
 * Struct for tracking locations/lengths of constants during normalization
 */
typedef struct pgssLocationLen {
    int location; /* start offset in query text */
    int length;   /* length in bytes, or -1 to ignore */
} pgssLocationLen;

/*
 * Working state for computing a query jumble and producing a normalized
 * query string
 */
typedef struct pgssJumbleState {
    /* Jumble of current query tree */
    unsigned char* jumble;

    /* Number of bytes used in jumble[] */
    Size jumble_len;

    /* Array of locations of constants that should be removed */
    pgssLocationLen* clocations;

    /* Allocated length of clocations array */
    int clocations_buf_size;

    /* Current number of valid entries in clocations array */
    int clocations_count;
} pgssJumbleState;

namespace UniqueSql {
/* ---- Function declarations ---- */
void AppendJumble(pgssJumbleState* jstate, const unsigned char* item, Size size);
void JumbleQuery(pgssJumbleState* jstate, Query* query);
void JumbleRangeTable(pgssJumbleState* jstate, List* rtable);
void JumbleExpr(pgssJumbleState* jstate, Node* node);
void RecordConstLocation(pgssJumbleState* jstate, int location);
uint32 pgss_hash_string(const char* str);
char* generate_normalized_query(pgssJumbleState* jstate, const char* query, int* query_len_p, int encoding);
void fill_in_constant_lengths(pgssJumbleState* jstate, const char* query);
int comp_location(const void* a, const void* b);
void generate_jstate(pgssJumbleState* jstate, Query* query);
}  // namespace UniqueSql

typedef struct BuiltinUniqueSQL {
    NodeTag     type;
    const char  *unique_sql;
    uint32      unique_sql_id;
    uint32      unique_sql_len;
} BuiltinUniqueSQL;

/* For sqls in BuiltinUniqueSQLArray, use pre-defined unique sql id/string */
static BuiltinUniqueSQL BuiltinUniqueSQLArray[] = {
    {T_BarrierStmt, "CREATE BARRIER", 0, 0},
    {T_DeallocateStmt, "DEALLOCATE", 0, 0}
};

void init_builtin_unique_sql()
{
    for (uint32 i = 0; i < (sizeof(BuiltinUniqueSQLArray) / sizeof(BuiltinUniqueSQLArray[0])); i++) {
        if (BuiltinUniqueSQLArray[i].unique_sql != NULL) {
            BuiltinUniqueSQLArray[i].unique_sql_len = strlen(BuiltinUniqueSQLArray[i].unique_sql);
            BuiltinUniqueSQLArray[i].unique_sql_id = UniqueSql::pgss_hash_string(BuiltinUniqueSQLArray[i].unique_sql); 
            if (BuiltinUniqueSQLArray[i].unique_sql_id == 0) {
                BuiltinUniqueSQLArray[i].unique_sql_id = 1;
            }
        }
    }
}

static const BuiltinUniqueSQL *find_builtin_unqiue_sql(const Query *query)
{
    if (query == NULL || query->utilityStmt == NULL) {
        return NULL;
    }
    for (uint32 i = 0; i < (sizeof(BuiltinUniqueSQLArray) / sizeof(BuiltinUniqueSQLArray[0])); i++) {
        if (BuiltinUniqueSQLArray[i].type == nodeTag(query->utilityStmt)) {
            if (BuiltinUniqueSQLArray[i].unique_sql != NULL) {
                return (BuiltinUniqueSQLArray + i);
            } else {
                return NULL;
            }
        }
    }
    return NULL;
}

/*
 * create unique queryid
 * query         query tree
 * query_string  query text
 * queryid       queryid
 */
uint32 generate_unique_queryid(Query* query, const char* query_string)
{
    pgssJumbleState jstate;
    uint32 queryid = 0;

    const BuiltinUniqueSQL *builtin_unique_sql = find_builtin_unqiue_sql(query);
    if (builtin_unique_sql != NULL) {
        queryid = builtin_unique_sql->unique_sql_id;
        return queryid;
    }

    errno_t rc;
    rc = memset_s(&jstate, sizeof(jstate), 0, sizeof(jstate));
    securec_check(rc, "\0", "\0");

    /*
     * For utility statements, we just hash the query string directly
     * SELETE/INSERT/UPDATE/DELETE sql call JumbleQuery to generate queryid
     */
    if (query->utilityStmt != NULL) {
        queryid = UniqueSql::pgss_hash_string(query_string);
    } else {
        UniqueSql::generate_jstate(&jstate, query);
        queryid = hash_any(jstate.jumble, jstate.jumble_len);
    }

    if (queryid == 0) {
        queryid = 1;
    }
    return queryid;
}

static void update_multi_sql_location(pgssJumbleState* jstate, int32 multi_sql_offset)
{
    if (multi_sql_offset > 0 && jstate != NULL) {
        for (int i = 0; i < jstate->clocations_count; i++) {
            if (jstate->clocations[i].location >= multi_sql_offset) {
                jstate->clocations[i].location = jstate->clocations[i].location - multi_sql_offset; 
            }
        }
    }
}

/*
 * The caller must apply for a space no smaller than strlen(query_string),
 * and return true if the function is called successfully.
 * input:
 * query        query tree
 * query-string     the query text
 * unique_buf       returned unique query text
 * len          The size of the unique_buf's
 */
bool normalized_unique_querystring(Query* query, const char* query_string, char* unique_buf, int buf_len,
    uint32 multi_sql_offset)
{
    if (query == NULL || query_string == NULL || unique_buf == NULL) {
        return false;
    }

    bool result = true;
    char *norm_query = NULL, *mask_str = NULL;
    int encoding = GetDatabaseEncoding();
    int query_len;
    pgssJumbleState jstate;
    errno_t rc = memset_s(&jstate, sizeof(jstate), 0, sizeof(jstate));
    securec_check(rc, "\0", "\0");

    query_len = strlen(query_string);
    if (query->utilityStmt == NULL) {
        UniqueSql::generate_jstate(&jstate, query);
        if (jstate.clocations_count > 0) {
            update_multi_sql_location(&jstate, multi_sql_offset);
            norm_query = UniqueSql::generate_normalized_query(&jstate, query_string, &query_len, encoding);
            if (norm_query == NULL) {
                result = false;
            }
        }
    } else {
        mask_str = maskPassword(query_string);
        if (mask_str != NULL) {
            query_string = mask_str;
            query_len = strlen(mask_str);
        }
    }

    if (result) {
        if (norm_query != NULL) {
            rc = memcpy_s(unique_buf, buf_len, norm_query, query_len);
            securec_check(rc, "\0", "\0");

            pfree(norm_query);
        } else {
            const BuiltinUniqueSQL *builtin_unique_sql = find_builtin_unqiue_sql(query);
            if (builtin_unique_sql != NULL) {
                query_string = builtin_unique_sql->unique_sql;
                query_len = builtin_unique_sql->unique_sql_len;
            }

            if (query_len > buf_len) {
                query_len = pg_encoding_mbcliplen(encoding, query_string, query_len,
                    g_instance.attr.attr_common.pgstat_track_activity_query_size - 1);
            }

            rc = memcpy_s(unique_buf, buf_len, query_string, query_len);
            securec_check(rc, "\0", "\0");
        }
    }

    pfree_ext(mask_str);
    return result;
}
/*
 * The function generate_jstate() is used to generate jumble for query
 */
void UniqueSql::generate_jstate(pgssJumbleState* jstate, Query* query)
{
    /* Set up workspace for query jumbling */
    jstate->jumble = (unsigned char*)palloc(JUMBLE_SIZE);
    jstate->jumble_len = 0;
    jstate->clocations_buf_size = CLOCATIONS_BUF_SIZE;
    jstate->clocations = (pgssLocationLen*)palloc(jstate->clocations_buf_size * sizeof(pgssLocationLen));
    jstate->clocations_count = 0;

    /* Compute query ID and mark the Query node with it */
    UniqueSql::JumbleQuery(jstate, query);
}
/*
 * Given an arbitrarily long query string, produce a hash for the purposes of
 * identifying the query, without normalizing constants.  Used when hashing
 * utility statements.
 */
uint32 UniqueSql::pgss_hash_string(const char* str)
{
    return hash_any((const unsigned char*)str, strlen(str));
}

/*
 * AppendJumble: Append a value that is substantive in a given query to
 * the current jumble.
 */
void UniqueSql::AppendJumble(pgssJumbleState* jstate, const unsigned char* item, Size size)
{
    unsigned char* jumble = jstate->jumble;
    Size jumble_len = jstate->jumble_len;
    int rc;

    /*
     * Whenever the jumble buffer is full, we hash the current contents and
     * reset the buffer to contain just that hash value, thus relying on the
     * hash to summarize everything so far.
     */
    while (size > 0) {
        Size part_size;

        if (jumble_len >= JUMBLE_SIZE) {
            uint32 start_hash = hash_any(jumble, JUMBLE_SIZE);

            rc = memcpy_s(jumble, JUMBLE_SIZE, &start_hash, sizeof(start_hash));
            securec_check(rc, "\0", "\0");
            jumble_len = sizeof(start_hash);
        }
        part_size = Min(size, JUMBLE_SIZE - jumble_len);
        rc = memcpy_s(jumble + jumble_len, JUMBLE_SIZE - jumble_len, item, part_size);
        securec_check(rc, "\0", "\0");
        jumble_len += part_size;
        item += part_size;
        size -= part_size;
    }
    jstate->jumble_len = jumble_len;
}

/*
 * Wrappers around AppendJumble to encapsulate details of serialization
 * of individual local variable elements.
 */
#define APP_JUMB(item) UniqueSql::AppendJumble(jstate, (const unsigned char*)&(item), sizeof(item))
#define APP_JUMB_STRING(str) UniqueSql::AppendJumble(jstate, (const unsigned char*)(str), strlen(str) + 1)

/*
 * JumbleQuery: Selectively serialize the query tree, appending significant
 * data to the "query jumble" while ignoring nonsignificant data.
 * Rule of thumb for what to include is that we should ignore anything not
 * semantically significant (such as alias names) as well as anything that can
 * be deduced from child nodes (else we'd just be double-hashing that piece
 * of information).
 */
void UniqueSql::JumbleQuery(pgssJumbleState* jstate, Query* query)
{
    Assert(IsA(query, Query));
    Assert(query->utilityStmt == NULL);

    APP_JUMB(query->commandType);
    /* resultRelation is usually predictable from commandType */
    UniqueSql::JumbleExpr(jstate, (Node*)query->cteList);
    UniqueSql::JumbleRangeTable(jstate, query->rtable);
    UniqueSql::JumbleExpr(jstate, (Node*)query->jointree);
    UniqueSql::JumbleExpr(jstate, (Node*)query->targetList);
    UniqueSql::JumbleExpr(jstate, (Node*)query->returningList);
    UniqueSql::JumbleExpr(jstate, (Node*)query->groupClause);
    UniqueSql::JumbleExpr(jstate, (Node*)query->groupingSets);
    UniqueSql::JumbleExpr(jstate, query->havingQual);
    UniqueSql::JumbleExpr(jstate, (Node*)query->windowClause);
    UniqueSql::JumbleExpr(jstate, (Node*)query->distinctClause);
    UniqueSql::JumbleExpr(jstate, (Node*)query->sortClause);
    UniqueSql::JumbleExpr(jstate, query->limitOffset);
    UniqueSql::JumbleExpr(jstate, query->limitCount);
    /* we ignore rowMarks */
    UniqueSql::JumbleExpr(jstate, query->setOperations);
}

/*
 * Jumble a range table
 */
void UniqueSql::JumbleRangeTable(pgssJumbleState* jstate, List* rtable)
{
    ListCell* lc = NULL;

    foreach (lc, rtable) {
        RangeTblEntry* rte = (RangeTblEntry*)lfirst(lc);

        Assert(IsA(rte, RangeTblEntry));
        APP_JUMB(rte->rtekind);
        switch (rte->rtekind) {
            case RTE_RELATION:
                if (rte->ispartrel) {
                    if (rte->isContainPartition && OidIsValid(rte->partitionOid)) {
                        APP_JUMB(rte->partitionOid);
                    } else if (rte->isContainSubPartition && OidIsValid(rte->subpartitionOid)) {
                        APP_JUMB(rte->subpartitionOid);
                    }
                } else {
                    APP_JUMB(rte->relid);
                }
                break;
            case RTE_SUBQUERY:
                UniqueSql::JumbleQuery(jstate, rte->subquery);
                break;
            case RTE_JOIN:
                APP_JUMB(rte->jointype);
                break;
            case RTE_FUNCTION:
                UniqueSql::JumbleExpr(jstate, rte->funcexpr);
                break;
            case RTE_VALUES:
                UniqueSql::JumbleExpr(jstate, (Node*)rte->values_lists);
                break;
            case RTE_CTE:

                /*
                 * Depending on the CTE name here isn't ideal, but it's the
                 * only info we have to identify the referenced WITH item.
                 */
                APP_JUMB_STRING(rte->ctename);
                APP_JUMB(rte->ctelevelsup);
                break;
            case RTE_RESULT:
                break;
            default:
                elog(ERROR, "unrecognized RTE kind: %d", (int)rte->rtekind);
                break;
        }
    }
}

/*
 * Jumble an expression tree
 *
 * In general this function should handle all the same node types that
 * expression_tree_walker() does, and therefore it's coded to be as parallel
 * to that function as possible.  However, since we are only invoked on
 * queries immediately post-parse-analysis, we need not handle node types
 * that only appear in planning.
 *
 * Note: the reason we don't simply use expression_tree_walker() is that the
 * point of that function is to support tree walkers that don't care about
 * most tree node types, but here we care about all types.  We should complain
 * about any unrecognized node type.
 */
void UniqueSql::JumbleExpr(pgssJumbleState* jstate, Node* node)
{
    ListCell* temp = NULL;

    if (node == NULL) {
        return;
    }

    /* Guard against stack overflow due to overly complex expressions */
    check_stack_depth();

    /*
     * We always emit the node's NodeTag, then any additional fields that are
     * considered significant, and then we recurse to any child nodes.
     */
    APP_JUMB(node->type);

    switch (nodeTag(node)) {
        case T_Var: {
            Var* var = (Var*)node;
            APP_JUMB(var->varno);
            APP_JUMB(var->varattno);
            APP_JUMB(var->varlevelsup);
            break;
        }
        case T_Const: {
            Const* c = (Const*)node;
            /* We jumble only the constant's type, not its value */
            APP_JUMB(c->consttype);
            /* Also, record its parse location for query normalization */
            UniqueSql::RecordConstLocation(jstate, c->location);
            break;
        }
        case T_Param: {
            Param* p = (Param*)node;

            APP_JUMB(p->paramkind);
            APP_JUMB(p->paramid);
            APP_JUMB(p->paramtype);

            break;
        }
        case T_Aggref: {
            Aggref* expr = (Aggref*)node;
            APP_JUMB(expr->aggfnoid);
            UniqueSql::JumbleExpr(jstate, (Node*)expr->args);
            UniqueSql::JumbleExpr(jstate, (Node*)expr->aggorder);
            UniqueSql::JumbleExpr(jstate, (Node*)expr->aggdistinct);

            break;
        }
        case T_GroupingFunc: {
            GroupingFunc* grpnode = (GroupingFunc*)node;
            UniqueSql::JumbleExpr(jstate, (Node*)grpnode->refs);
            break;
        }
        case T_WindowFunc: {
            WindowFunc* expr = (WindowFunc*)node;
            APP_JUMB(expr->winfnoid);
            APP_JUMB(expr->winref);
            UniqueSql::JumbleExpr(jstate, (Node*)expr->args);
            break;
        }
        case T_InitList: {
            foreach (temp, (List*)node) {
                APP_JUMB(lfirst_int(temp));
            }
            break;
        }
        case T_ArrayRef: {
            ArrayRef* aref = (ArrayRef*)node;
            UniqueSql::JumbleExpr(jstate, (Node*)aref->refupperindexpr);
            UniqueSql::JumbleExpr(jstate, (Node*)aref->reflowerindexpr);
            UniqueSql::JumbleExpr(jstate, (Node*)aref->refexpr);
            UniqueSql::JumbleExpr(jstate, (Node*)aref->refassgnexpr);

            break;
        }
        case T_FuncExpr: {
            FuncExpr* expr = (FuncExpr*)node;
            APP_JUMB(expr->funcid);
            UniqueSql::JumbleExpr(jstate, (Node*)expr->args);
            break;
        }
        case T_NamedArgExpr: {
            NamedArgExpr* nae = (NamedArgExpr*)node;
            APP_JUMB(nae->argnumber);
            UniqueSql::JumbleExpr(jstate, (Node*)nae->arg);

            break;
        }
        case T_OpExpr:
        case T_DistinctExpr: /* struct-equivalent to OpExpr */
        case T_NullIfExpr: { /* struct-equivalent to OpExpr */
            OpExpr* expr = (OpExpr*)node;
            APP_JUMB(expr->opno);
            UniqueSql::JumbleExpr(jstate, (Node*)expr->args);
            break;
        }
        case T_ScalarArrayOpExpr: {
            ScalarArrayOpExpr* expr = (ScalarArrayOpExpr*)node;
            APP_JUMB(expr->opno);
            APP_JUMB(expr->useOr);
            UniqueSql::JumbleExpr(jstate, (Node*)expr->args);

            break;
        }
        case T_BoolExpr: {
            BoolExpr* expr = (BoolExpr*)node;
            APP_JUMB(expr->boolop);
            UniqueSql::JumbleExpr(jstate, (Node*)expr->args);

            break;
        }
        case T_SubLink: {
            SubLink* sublink = (SubLink*)node;
            APP_JUMB(sublink->subLinkType);
            UniqueSql::JumbleExpr(jstate, (Node*)sublink->testexpr);
            UniqueSql::JumbleQuery(jstate, (Query*)sublink->subselect);

            break;
        }
        case T_FieldSelect: {
            FieldSelect* fs = (FieldSelect*)node;
            APP_JUMB(fs->fieldnum);
            UniqueSql::JumbleExpr(jstate, (Node*)fs->arg);

            break;
        }
        case T_FieldStore: {
            FieldStore* fstore = (FieldStore*)node;
            UniqueSql::JumbleExpr(jstate, (Node*)fstore->arg);
            UniqueSql::JumbleExpr(jstate, (Node*)fstore->newvals);

            break;
        }
        case T_RelabelType: {
            RelabelType* rt = (RelabelType*)node;
            APP_JUMB(rt->resulttype);
            UniqueSql::JumbleExpr(jstate, (Node*)rt->arg);

            break;
        }
        case T_CoerceViaIO: {
            CoerceViaIO* cio = (CoerceViaIO*)node;
            APP_JUMB(cio->resulttype);
            UniqueSql::JumbleExpr(jstate, (Node*)cio->arg);
            break;
        }
        case T_ArrayCoerceExpr: {
            ArrayCoerceExpr* acexpr = (ArrayCoerceExpr*)node;
            APP_JUMB(acexpr->resulttype);
            UniqueSql::JumbleExpr(jstate, (Node*)acexpr->arg);
            break;
        }
        case T_ConvertRowtypeExpr: {
            ConvertRowtypeExpr* crexpr = (ConvertRowtypeExpr*)node;
            APP_JUMB(crexpr->resulttype);
            UniqueSql::JumbleExpr(jstate, (Node*)crexpr->arg);

            break;
        }
        case T_CollateExpr: {
            CollateExpr* ce = (CollateExpr*)node;
            APP_JUMB(ce->collOid);
            UniqueSql::JumbleExpr(jstate, (Node*)ce->arg);
            break;
        }
        case T_CaseExpr: {
            CaseExpr* caseexpr = (CaseExpr*)node;
            UniqueSql::JumbleExpr(jstate, (Node*)caseexpr->arg);
            foreach (temp, caseexpr->args) {
                CaseWhen* when = (CaseWhen*)lfirst(temp);
                Assert(IsA(when, CaseWhen));
                UniqueSql::JumbleExpr(jstate, (Node*)when->expr);
                UniqueSql::JumbleExpr(jstate, (Node*)when->result);
            }
            UniqueSql::JumbleExpr(jstate, (Node*)caseexpr->defresult);
            break;
        }
        case T_CaseTestExpr: {
            CaseTestExpr* ct = (CaseTestExpr*)node;
            APP_JUMB(ct->typeId);
            break;
        }
        case T_ArrayExpr:
            UniqueSql::JumbleExpr(jstate, (Node*)((ArrayExpr*)node)->elements);
            break;
        case T_RowExpr:
            UniqueSql::JumbleExpr(jstate, (Node*)((RowExpr*)node)->args);
            break;
        case T_RowCompareExpr: {
            RowCompareExpr* rcexpr = (RowCompareExpr*)node;
            APP_JUMB(rcexpr->rctype);
            UniqueSql::JumbleExpr(jstate, (Node*)rcexpr->largs);
            UniqueSql::JumbleExpr(jstate, (Node*)rcexpr->rargs);
            break;
        }
        case T_CoalesceExpr:
            UniqueSql::JumbleExpr(jstate, (Node*)((CoalesceExpr*)node)->args);
            break;
        case T_MinMaxExpr: {
            MinMaxExpr* mmexpr = (MinMaxExpr*)node;
            APP_JUMB(mmexpr->op);
            UniqueSql::JumbleExpr(jstate, (Node*)mmexpr->args);
            break;
        }
        case T_XmlExpr: {
            XmlExpr* xexpr = (XmlExpr*)node;
            APP_JUMB(xexpr->op);
            UniqueSql::JumbleExpr(jstate, (Node*)xexpr->named_args);
            UniqueSql::JumbleExpr(jstate, (Node*)xexpr->args);
            break;
        }
        case T_NullTest: {
            NullTest* nt = (NullTest*)node;
            APP_JUMB(nt->nulltesttype);
            UniqueSql::JumbleExpr(jstate, (Node*)nt->arg);

            break;
        }
        case T_BooleanTest: {
            BooleanTest* bt = (BooleanTest*)node;
            APP_JUMB(bt->booltesttype);
            UniqueSql::JumbleExpr(jstate, (Node*)bt->arg);

            break;
        }
        case T_CoerceToDomain: {
            CoerceToDomain* cd = (CoerceToDomain*)node;
            APP_JUMB(cd->resulttype);
            UniqueSql::JumbleExpr(jstate, (Node*)cd->arg);
            break;
        }
        case T_CoerceToDomainValue: {
            CoerceToDomainValue* cdv = (CoerceToDomainValue*)node;
            APP_JUMB(cdv->typeId);
            break;
        }
        case T_SetToDefault: {
            SetToDefault* sd = (SetToDefault*)node;
            APP_JUMB(sd->typeId);
            break;
        }
        case T_CurrentOfExpr: {
            CurrentOfExpr* ce = (CurrentOfExpr*)node;
            APP_JUMB(ce->cvarno);
            if (ce->cursor_name) {
                APP_JUMB_STRING(ce->cursor_name);
            }
            APP_JUMB(ce->cursor_param);

            break;
        }
        case T_TargetEntry: {
            TargetEntry* tle = (TargetEntry*)node;
            APP_JUMB(tle->resno);
            APP_JUMB(tle->ressortgroupref);
            UniqueSql::JumbleExpr(jstate, (Node*)tle->expr);
            break;
        }
        case T_RangeTblRef: {
            RangeTblRef* rtr = (RangeTblRef*)node;
            APP_JUMB(rtr->rtindex);
            break;
        }
        case T_JoinExpr: {
            JoinExpr* join = (JoinExpr*)node;
            APP_JUMB(join->jointype);
            APP_JUMB(join->isNatural);
            APP_JUMB(join->rtindex);
            UniqueSql::JumbleExpr(jstate, join->larg);
            UniqueSql::JumbleExpr(jstate, join->rarg);
            UniqueSql::JumbleExpr(jstate, join->quals);
            break;
        }
        case T_FromExpr: {
            FromExpr* from = (FromExpr*)node;
            UniqueSql::JumbleExpr(jstate, (Node*)from->fromlist);
            UniqueSql::JumbleExpr(jstate, from->quals);
            break;
        }
        case T_List:
            foreach (temp, (List*)node) {
                UniqueSql::JumbleExpr(jstate, (Node*)lfirst(temp));
            }
            break;
        case T_IntList:
            foreach (temp, (List*)node) {
                APP_JUMB(lfirst_int(temp));
            }
            break;
        case T_SortGroupClause: {
            SortGroupClause* sgc = (SortGroupClause*)node;
            APP_JUMB(sgc->tleSortGroupRef);
            APP_JUMB(sgc->eqop);
            APP_JUMB(sgc->sortop);
            APP_JUMB(sgc->nulls_first);

            break;
        }
        case T_GroupingSet: {
            GroupingSet* gsnode = (GroupingSet*)node;
            UniqueSql::JumbleExpr(jstate, (Node*)gsnode->content);
            break;
        }
        case T_WindowClause: {
            WindowClause* wc = (WindowClause*)node;
            APP_JUMB(wc->winref);
            APP_JUMB(wc->frameOptions);
            UniqueSql::JumbleExpr(jstate, (Node*)wc->partitionClause);
            UniqueSql::JumbleExpr(jstate, (Node*)wc->orderClause);
            UniqueSql::JumbleExpr(jstate, wc->startOffset);
            UniqueSql::JumbleExpr(jstate, wc->endOffset);

            break;
        }
        case T_CommonTableExpr: {
            CommonTableExpr* cte = (CommonTableExpr*)node;
            /* we store the string name because RTE_CTE RTEs need it */
            APP_JUMB_STRING(cte->ctename);
            UniqueSql::JumbleQuery(jstate, (Query*)cte->ctequery);
            break;
        }
        case T_SetOperationStmt: {
            SetOperationStmt* setop = (SetOperationStmt*)node;
            APP_JUMB(setop->op);
            APP_JUMB(setop->all);
            UniqueSql::JumbleExpr(jstate, setop->larg);
            UniqueSql::JumbleExpr(jstate, setop->rarg);

            break;
        }
        default:
            /* Only a warning, since we can stumble along anyway */
            elog(DEBUG1, "unrecognized node type: %d", (int)nodeTag(node));
            break;
    }
}

/*
 * Record location of constant within query string of query tree
 * that is currently being walked.
 */
void UniqueSql::RecordConstLocation(pgssJumbleState* jstate, int location)
{
    const int DOUBLE_SIZE = 2;
    /* -1 indicates unknown or undefined location */
    if (location >= 0) {
        /* enlarge array if needed */
        if (jstate->clocations_count >= jstate->clocations_buf_size) {
            jstate->clocations_buf_size *= DOUBLE_SIZE;
            jstate->clocations =
                (pgssLocationLen*)repalloc(jstate->clocations, jstate->clocations_buf_size * sizeof(pgssLocationLen));
        }
        jstate->clocations[jstate->clocations_count].location = location;
        /* initialize lengths to -1 to simplify fill_in_constant_lengths */
        jstate->clocations[jstate->clocations_count].length = -1;
        jstate->clocations_count++;
    }
}

/*
 * Generate a normalized version of the query string that will be used to
 * represent all similar queries.
 *
 * Note that the normalized representation may well vary depending on
 * just which "equivalent" query is used to create the hashtable entry.
 * We assume this is OK.
 *
 * *query_len_p contains the input string length, and is updated with
 * the result string length (which cannot be longer) on exit.
 *
 * Returns a palloc'd string, which is not necessarily null-terminated.
 */
char* UniqueSql::generate_normalized_query(pgssJumbleState* jstate, const char* query, int* query_len_p, int encoding)
{
    char* norm_query = NULL;
    int query_len = *query_len_p;
    int max_output_len, i, rc;
    int len_to_wrt;       /* Length (in bytes) to write */
    int quer_loc = 0;     /* Source query byte location */
    int n_quer_loc = 0;   /* Normalized query byte location */
    int last_off = 0;     /* Offset from start for previous tok */
    int last_tok_len = 0; /* Length (in bytes) of that tok */

    /*
     * Get constants' lengths (core system only gives us locations).  Note
     * this also ensures the items are sorted by location.
     */
    UniqueSql::fill_in_constant_lengths(jstate, query);

    /* Allocate result buffer, ensuring we limit result to allowed size */
    max_output_len = Min(query_len, g_instance.attr.attr_common.pgstat_track_activity_query_size - 1);
    norm_query = (char*)palloc(max_output_len);

    for (i = 0; i < jstate->clocations_count; i++) {
        /*
         * off: Offset from start for cur tok
         * tok_len: Length (in bytes) of that tok
         */
        int off, tok_len;
        off = jstate->clocations[i].location;
        tok_len = jstate->clocations[i].length;

        if (tok_len < 0) {
            continue; /* ignore any duplicates */
        }
        /* Copy next chunk, or as much as will fit */
        len_to_wrt = off - last_off;
        len_to_wrt -= last_tok_len;
        len_to_wrt = Min(len_to_wrt, max_output_len - n_quer_loc);
        /* Should not happen, but for below SQL, Query struct and
         * query string can't be matched(location in Query is bigger
         * than query string)
         *  - delete from plan_table where statement_id='test statement_id',
         *    for sql 'delete plan_table', transformDeleteStmt method will
         *    modify Query member.
         */
        if (len_to_wrt <= 0) {
            break;
        }

        rc = memcpy_s(norm_query + n_quer_loc, max_output_len - n_quer_loc, query + quer_loc, len_to_wrt);
        securec_check(rc, "\0", "\0");
        n_quer_loc += len_to_wrt;

        if (n_quer_loc < max_output_len) {
            norm_query[n_quer_loc++] = '?';
        }

        quer_loc = off + tok_len;
        last_off = off;
        last_tok_len = tok_len;

        /* If we run out of space, might as well stop iterating */
        if (n_quer_loc >= max_output_len) {
            break;
        }
    }

    /*
     * We've copied up until the last ignorable constant.  Copy over the
     * remaining bytes of the original query string, or at least as much as
     * will fit.
     */
    len_to_wrt = query_len - quer_loc;
    len_to_wrt = Min(len_to_wrt, max_output_len - n_quer_loc);
    if (len_to_wrt > 0) {
        rc = memcpy_s(norm_query + n_quer_loc, max_output_len - n_quer_loc, query + quer_loc, len_to_wrt);
        securec_check(rc, "\0", "\0");
        n_quer_loc += len_to_wrt;
    }

    /*
     * If we ran out of space, we need to do an encoding-aware truncation,
     * just to make sure we don't have an incomplete character at the end.
     */
    if (n_quer_loc >= max_output_len) {
        query_len = pg_encoding_mbcliplen(
            encoding, norm_query, n_quer_loc, g_instance.attr.attr_common.pgstat_track_activity_query_size - 1);
    } else {
        query_len = n_quer_loc;
    }

    *query_len_p = query_len;
    return norm_query;
}

/*
 * Given a valid SQL string and an array of constant-location records,
 * fill in the textual lengths of those constants.
 *
 * The constants may use any allowed constant syntax, such as float literals,
 * bit-strings, single-quoted strings and dollar-quoted strings.  This is
 * accomplished by using the public API for the core scanner.
 *
 * It is the caller's job to ensure that the string is a valid SQL statement
 * with constants at the indicated locations.  Since in practice the string
 * has already been parsed, and the locations that the caller provides will
 * have originated from within the authoritative parser, this should not be
 * a problem.
 *
 * Duplicate constant pointers are possible, and will have their lengths
 * marked as '-1', so that they are later ignored.  (Actually, we assume the
 * lengths were initialized as -1 to start with, and don't change them here.)
 *
 * N.B. There is an assumption that a '-' character at a Const location begins
 * a negative numeric constant.  This precludes there ever being another
 * reason for a constant to start with a '-'.
 */
void UniqueSql::fill_in_constant_lengths(pgssJumbleState* jstate, const char* query)
{
    pgssLocationLen* locs = NULL;
    core_yyscan_t yyscanner;
    core_yy_extra_type yyextra;
    core_YYSTYPE yylval;
    YYLTYPE yylloc;
    int last_loc = -1;
    int i;

    /*
     * Sort the records by location so that we can process them in order while
     * scanning the query text.
     */
    if (jstate->clocations_count > 1) {
        qsort(jstate->clocations, jstate->clocations_count, sizeof(pgssLocationLen), UniqueSql::comp_location);
    }
    locs = jstate->clocations;

    /* initialize the flex scanner --- should match raw_parser() */
    yyscanner = scanner_init(query, &yyextra, ScanKeywords, NumScanKeywords);

    /* Search for each constant, in sequence */
    for (i = 0; i < jstate->clocations_count; i++) {
        int loc = locs[i].location;
        int tok;

        Assert(loc >= 0);

        if (loc <= last_loc) {
            continue; /* Duplicate constant, ignore */
        }
        /* Lex tokens until we find the desired constant */
        for (;;) {
            tok = core_yylex(&yylval, &yylloc, yyscanner);
            /* We should not hit end-of-string, but if we do, behave sanely */
            if (tok == 0) {
                break; /* out of inner for-loop */
            }
            /*
             * We should find the token position exactly, but if we somehow
             * run past it, work with that.
             */
            if (yylloc >= loc) {
                if (query[loc] == '-') {
                    /*
                     * It's a negative value - this is the one and only case
                     * where we replace more than a single token.
                     *
                     * Do not compensate for the core system's special-case
                     * adjustment of location to that of the leading '-'
                     * operator in the event of a negative constant.  It is
                     * also useful for our purposes to start from the minus
                     * symbol.  In this way, queries like "select * from foo
                     * where bar = 1" and "select * from foo where bar = -2"
                     * will have identical normalized query strings.
                     */
                    tok = core_yylex(&yylval, &yylloc, yyscanner);
                    if (tok == 0) {
                        break; /* out of inner for-loop */
                    }
                }

                /*
                 * We now rely on the assumption that flex has placed a zero
                 * byte after the text of the current token in scanbuf.
                 */
                locs[i].length = strlen(yyextra.scanbuf + loc);
                break; /* out of inner for-loop */
            }
        }

        /* If we hit end-of-string, give up, leaving remaining lengths -1 */
        if (tok == 0) {
            break;
        }

        last_loc = loc;
    }

    scanner_finish(yyscanner);
}

/*
 * comp_location: comparator for qsorting pgssLocationLen structs by location
 */
int UniqueSql::comp_location(const void* a, const void* b)
{
    int l = ((const pgssLocationLen*)a)->location;
    int r = ((const pgssLocationLen*)b)->location;

    if (l < r) {
        return -1;
    } else if (l > r) {
        return +1;
    } else {
        return 0;
    }
}
