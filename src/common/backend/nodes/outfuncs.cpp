/* -------------------------------------------------------------------------
 *
 * outfuncs.cpp
 *	  Output functions for openGauss tree nodes.
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/common/backend/nodes/outfuncs.cpp
 *
 * NOTES
 *	  Every node type that can appear in stored rules' parsetrees *must*
 *	  have an output function defined here (as well as an input function
 *	  in readfuncs.c).	For use in debugging, we also provide output
 *	  functions for nodes that appear in raw parsetrees, path, and plan trees.
 *	  These nodes however need not have input functions.
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"
#include "miscadmin.h"
#include "bulkload/dist_fdw.h"
#include "foreign/fdwapi.h"
#include "nodes/plannodes.h"
#include "nodes/relation.h"
#include "nodes/nodeFuncs.h"
#include "parser/parse_hint.h"
#include "parser/parser.h"
#include "utils/builtins.h"
#include "utils/datum.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"
#include "catalog/gs_opt_model.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_synonym.h"
#include "catalog/pg_type.h"
#include "optimizer/streamplan.h"
#include "storage/tcap.h"
#include "tcop/utility.h"
#ifdef PGXC
#include "optimizer/dataskew.h"
#include "optimizer/pgxcplan.h"
#include "optimizer/planner.h"
#include "access/transam.h"
#include "pgxc/groupmgr.h"
#include "pgxc/pgxc.h"
#include "pgxc/pgFdwRemote.h"
#endif
#include "db4ai/gd.h"

/*
 * Macros to simplify output of different kinds of fields.	Use these
 * wherever possible to reduce the chance for silly typos.	Note that these
 * hard-wire conventions about the names of the local variables in an Out
 * routine.
 */

/* Write the label for the node type */
#define WRITE_NODE_TYPE(nodelabel) appendStringInfoString(str, nodelabel)

/* Write an integer field (anything written as ":fldname %d") */
#define WRITE_INT_FIELD(fldname) appendStringInfo(str, " :" CppAsString(fldname) " %d", node->fldname)

/* Write an unsigned integer field (anything written as ":fldname %u") */
#define WRITE_UINT_FIELD(fldname) appendStringInfo(str, " :" CppAsString(fldname) " %u", node->fldname)

/* Write an 64bit unsigned integer field (anything written as ":fldname %lu") */
#define WRITE_UINT64_FIELD(fldname) appendStringInfo(str, " :" CppAsString(fldname) " %lu", node->fldname)

/* Write an OID field (don't hard-wire assumption that OID is same as uint) */
#define WRITE_OID_FIELD(fldname) appendStringInfo(str, " :" CppAsString(fldname) " %u", node->fldname)

/* Write a long-integer field */
#define WRITE_LONG_FIELD(fldname) appendStringInfo(str, " :" CppAsString(fldname) " %ld", node->fldname)

/* Write a char field (ie, one ascii character) */
#define WRITE_CHAR_FIELD(fldname) appendStringInfo(str, " :" CppAsString(fldname) " %c", node->fldname)

/* Write an enumerated-type field as an integer code */
#define WRITE_ENUM_FIELD(fldname, enumtype) appendStringInfo(str, " :" CppAsString(fldname) " %d", (int)node->fldname)

/* Write a float field --- caller must give format to define precision */
#define WRITE_FLOAT_FIELD(fldname, format) appendStringInfo(str, " :" CppAsString(fldname) " " format, node->fldname)

/* Write a boolean field */
#define WRITE_BOOL_FIELD(fldname) appendStringInfo(str, " :" CppAsString(fldname) " %s", booltostr(node->fldname))

/* Write a character-string (possibly NULL) field */
#define WRITE_STRING_FIELD(fldname) \
    (appendStringInfo(str, " :" CppAsString(fldname) " "), _outToken(str, node->fldname))

/* Write a parse location field (actually same as INT case) */
#define WRITE_LOCATION_FIELD(fldname) appendStringInfo(str, " :" CppAsString(fldname) " %d", node->fldname)

/* Write a Node field */
#define WRITE_NODE_FIELD(fldname) (appendStringInfo(str, " :" CppAsString(fldname) " "), _outNode(str, node->fldname))

/* Write a Array field */
#define WRITE_ARRAY_FIELD(fldname, fldlen) \
    (appendStringInfo(str, " :" CppAsString(fldname) " "), _outArray(str, (void**)(node->fldname), fldlen))

/* Write a bitmapset field */
#define WRITE_BITMAPSET_FIELD(fldname) \
    (appendStringInfo(str, " :" CppAsString(fldname) " "), _outBitmapset(str, node->fldname))

/* Write a cursor data */
#define WRITE_CURSORDATA_FIELD(fldname) \
    (appendStringInfo(str, " :" CppAsString(fldname) " "), _outCursorData(str, &node->fldname))

#define IsStatisfyUpdateCompatibility(fldname) (!IsInitdb && !(isRestoreMode && (fldname) < FirstNormalObjectId))

/*
 * For the new created type, we have to bind the typname and typnamespace information
 * so that the data node can decode it.
 */
#define WRITE_TYPEINFO_WITHOID(typid)                                                \
    if ((typid) >= FirstBootstrapObjectId && IsStatisfyUpdateCompatibility(typid)) { \
        char* exprtypename = NULL;                                                   \
        char* exprtypenamespace = NULL;                                              \
        appendStringInfo(str, " :exprtypename ");                                    \
        exprtypename = get_typename(typid);                                          \
        _outToken(str, exprtypename);                                                \
        pfree_ext(exprtypename);                                                     \
        appendStringInfo(str, " :exprtypenamespace ");                               \
        exprtypenamespace = get_typenamespace(typid);                                \
        _outToken(str, exprtypenamespace);                                           \
        pfree_ext(exprtypenamespace);                                                \
    }

#define WRITE_TYPEINFO_FIELD(fldname)         \
    do {                                      \
        WRITE_TYPEINFO_WITHOID(node->fldname) \
    } while (0)

#define WRITE_TYPEINFO_LIST(fldname)      \
    do {                                  \
        ListCell* cell = NULL;            \
        foreach (cell, node->fldname) {   \
            Oid typid = lfirst_oid(cell); \
            WRITE_TYPEINFO_WITHOID(typid) \
        }                                 \
    } while (0)

#define WRITE_TYPEINFO_ARRAY(fldname, size)    \
    do {                                       \
        for (int i = 0; i < node->size; i++) { \
            Oid typid = node->fldname[i];      \
            WRITE_TYPEINFO_WITHOID(typid)      \
        }                                      \
    } while (0)

/*
 * Write full-text search configuration's name out of its oid
 *
 * openGauss defines a special type(regconfig) to express full-text search
 * configuration type, and almost all the full-text search functions has a input
 * para of type regconfig, and the value of the para indicates a user-defined
 * full-text search configuration.
 *
 * In order to find the same full-text search configuration in datanode as in
 * coordinator when the function is shipped to datanode from coordinator, we
 * have to write full-text search configuration's name out of its oid
 */
#define WRITE_CFGINFO_FIELD(fldname1, fldname2)               \
    do {                                                      \
        if (REGCONFIGOID == node->fldname1) {                 \
            appendStringInfo(str, " :cfgname ");              \
            _outToken(str, get_cfgname(node->fldname2));      \
            appendStringInfo(str, " :cfgnamespace ");         \
            _outToken(str, get_cfgnamespace(node->fldname2)); \
        }                                                     \
    } while (0)

#define WRITE_EXPRTYPEINFO_FIELD(fldname)                                                    \
    do {                                                                                     \
        if ((fldname) >= FirstBootstrapObjectId && IsStatisfyUpdateCompatibility(fldname)) { \
            appendStringInfo(str, " :exprtypename ");                                        \
            _outToken(str, get_typename(fldname));                                           \
            appendStringInfo(str, " :exprtypenamespace ");                                   \
            _outToken(str, get_typenamespace(fldname));                                      \
        }                                                                                    \
    } while (0)

#define WRITE_FUNCINFO_FIELD(fldname)                                                                  \
    do {                                                                                               \
        if (node->fldname >= FirstBootstrapObjectId && IsStatisfyUpdateCompatibility(node->fldname)) { \
            appendStringInfo(str, " :funcname ");                                                      \
            _outToken(str, get_func_name(node->fldname));                                              \
            appendStringInfo(str, " :funcnamespace ");                                                 \
            _outToken(str, get_namespace_name(get_func_namespace(node->fldname)));                     \
        }                                                                                              \
    } while (0)

#define WRITE_PROCINFO_FIELD(fldname)                                                        \
    do {                                                                                     \
        if ((fldname) >= FirstBootstrapObjectId && IsStatisfyUpdateCompatibility(fldname)) { \
            appendStringInfo(str, " :funcname ");                                            \
            _outToken(str, get_func_name(fldname));                                          \
            appendStringInfo(str, " :funcnamespace ");                                       \
            _outToken(str, get_namespace_name(get_func_namespace(fldname)));                 \
        }                                                                                    \
    } while (0)

#define WRITE_OPINFO_FEILD(fldname)                                                               \
    ;                                                                                             \
    do {                                                                                          \
        if (node->fldname >= FirstNormalObjectId) {                                               \
            char* oprname = NULL;                                                                 \
            char* nspname = NULL;                                                                 \
            Oid oprleft;                                                                          \
            Oid oprright;                                                                         \
            get_oper_name_namespace_oprs(node->fldname, &oprname, &nspname, &oprleft, &oprright); \
            appendStringInfo(str, " :opname ");                                                   \
            _outToken(str, oprname);                                                              \
            appendStringInfo(str, " :opnamespace ");                                              \
            _outToken(str, nspname);                                                              \
            appendStringInfo(str, " :oprleft ");                                                  \
            _outToken(str, get_typename(oprleft));                                                \
            appendStringInfo(str, " :oprright ");                                                 \
            _outToken(str, get_typename(oprright));                                               \
            pfree_ext(oprname);                                                                   \
            pfree_ext(nspname);                                                                   \
        }                                                                                         \
    } while (0)

#define WRITE_GRPOP_FIELD(fldname, size)                                                                 \
    do {                                                                                                 \
        appendStringInfo(str, " :" CppAsString(fldname) " ");                                            \
        appendStringInfoChar(str, '(');                                                                  \
        char* oprname = NULL;                                                                            \
        char* nspname = NULL;                                                                            \
        Oid oprleft;                                                                                     \
        Oid oprright;                                                                                    \
        for (int i = 0; i < node->size; i++) {                                                           \
            appendStringInfoChar(str, '{');                                                              \
            appendStringInfo(str, "OPERATOR ");                                                          \
            appendStringInfo(str, " :opoid ");                                                           \
            appendStringInfo(str, " %u", node->fldname[i]);                                              \
            if (node->fldname[i] >= FirstNormalObjectId) {                                               \
                get_oper_name_namespace_oprs(node->fldname[i], &oprname, &nspname, &oprleft, &oprright); \
                appendStringInfo(str, " :oprname ");                                                     \
                _outToken(str, oprname);                                                                 \
                appendStringInfo(str, " :oprnamespace ");                                                \
                _outToken(str, nspname);                                                                 \
                appendStringInfo(str, " :oprleft ");                                                     \
                _outToken(str, get_typename(oprleft));                                                   \
                appendStringInfo(str, " :oprright ");                                                    \
                _outToken(str, get_typename(oprright));                                                  \
                pfree_ext(oprname);                                                                      \
                pfree_ext(nspname);                                                                      \
            }                                                                                            \
            appendStringInfoChar(str, '}');                                                              \
        }                                                                                                \
        appendStringInfo(str, ") ");                                                                     \
    } while (0)

#define WRITE_SYNINFO_FIELD(fldname)                                      \
    do {                                                                  \
        if (OidIsValid(node->fldname)) {                                  \
            char* synName = NULL;                                         \
            char* synSchema = NULL;                                       \
            GetSynonymAndSchemaName(node->fldname, &synName, &synSchema); \
            if (synName != NULL && synSchema != NULL) {                   \
                appendStringInfo(str, " :synname ");                      \
                _outToken(str, synName);                                  \
                appendStringInfo(str, " :synnamespace ");                 \
                _outToken(str, synSchema);                                \
            }                                                             \
        }                                                                 \
    } while (0)

#define booltostr(x) ((x) ? "true" : "false")

static void _outPathInfo(StringInfo str, Path* node);
static void _outNode(StringInfo str, const void* obj);
static void out_mem_info(StringInfo str, OpMemInfo* node);
static void _outCursorData(StringInfo str, Cursor_Data* node);
static void getNameById(Oid objId, const char* context, char** objNamespace, char** objName);
/*
 * _outToken
 * Convert an ordinary string (eg, an identifier) into a form that
 * will be decoded back to a plain token by read.c's functions.
 *
 * If a null or empty string is given, it is encoded as "<>".
 */
static void _outToken(StringInfo str, const char* s)
{
    if (s == NULL || *s == '\0') {
        appendStringInfo(str, "<>");
        return;
    }

    /*
     * Look for characters or patterns that are treated specially by read.c
     * (either in pg_strtok() or in nodeRead()), and therefore need a
     * protective backslash.
     */
    /* These characters only need to be quoted at the start of the string */
    if (*s == '<' || *s == '\"' || isdigit((unsigned char)*s) ||
        ((*s == '+' || *s == '-') && (isdigit((unsigned char)s[1]) || s[1] == '.'))) {
        appendStringInfoChar(str, '\\');
    }
    while (*s) {
        /* These chars must be backslashed anywhere in the string */
        if (*s == ' ' || *s == '\n' || *s == '\t' || *s == '(' || *s == ')' || *s == '{' || *s == '}' || *s == '\\') {
            appendStringInfoChar(str, '\\');
        }
        appendStringInfoChar(str, *s++);
    }
}

static void _outList(StringInfo str, List* node)
{
    ListCell* lc = NULL;

    appendStringInfoChar(str, '(');

    if (IsA(node, IntList)) {
        appendStringInfoChar(str, 'i');
    } else if (IsA(node, OidList)) {
        appendStringInfoChar(str, 'o');
    }

    foreach (lc, node) {
        /*
         * For the sake of backward compatibility, we emit a slightly
         * different whitespace format for lists of nodes vs. other types of
         * lists. XXX: is this necessary?
         */
        if (IsA(node, List)) {
            _outNode(str, lfirst(lc));
            if (lnext(lc)) {
                appendStringInfoChar(str, ' ');
            }
        } else if (IsA(node, IntList)) {
            appendStringInfo(str, " %d", lfirst_int(lc));
        } else if (IsA(node, OidList)) {
            appendStringInfo(str, " %u", lfirst_oid(lc));
        } else {
            ereport(ERROR,
                (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE), errmsg("unrecognized list node type: %d", (int)node->type)));
        }
    }

    appendStringInfoChar(str, ')');
}

/*
 * _outArray
 *     output an array
 *
 * @param (in) str:
 *     target string to store information
 * @param (in) node:
 *     the array node
 * @param (in) len:
 *     the length of the array
 *
 * @return: void
 */
static void _outArray(StringInfo str, void** node, int len)
{
    appendStringInfoChar(str, '(');

    for (int i = 0; i < len; ++i) {
        Node* item = (Node*)(node[i]);
        if (NULL != item) {
            if (IsA(item, List) || IsA(item, IntList) || IsA(item, OidList)) {
                appendStringInfo(str, "{Item%d", i);
                appendStringInfo(str, " :Content ");
            }
            _outNode(str, item);
            if (IsA(item, List) || IsA(item, IntList) || IsA(item, OidList)) {
                appendStringInfo(str, "}");
            }
        } else {
            appendStringInfo(str, "{NULL");
            appendStringInfo(str, "}");
        }
        if (i + 1 != len) {
            appendStringInfoChar(str, ' ');
        }
    }

    appendStringInfoChar(str, ')');
}

/*
 * _outBitmapset -
 *	   converts a bitmap set of integers
 *
 * Note: the output format is "(b int int ...)", similar to an integer List.
 */
void _outBitmapset(StringInfo str, Bitmapset* bms)
{
    Bitmapset* tmpset = NULL;
    int x;

    appendStringInfoChar(str, '(');
    appendStringInfoChar(str, 'b');
    tmpset = bms_copy(bms);
    while ((x = bms_first_member(tmpset)) >= 0) {
        appendStringInfo(str, " %d", x);   
    }
    bms_free_ext(tmpset);
    appendStringInfoChar(str, ')');
}

/**
 * @Description: converts a array set of integers to string.
 * @in str, store the the (a int int ...) format string in str.
 * @in a, the given uint64 array.
 * @in the length of the array.
 * @return none.
 * Note: the output format is "(a int int ...)", similar to an integer List.
 */
static void _outUint64Array(StringInfo str, uint64* a, int arrayLen)
{
    appendStringInfoChar(str, '(');
    appendStringInfoChar(str, 'a');

    for (int index = 0; index < arrayLen; index++) {
        appendStringInfo(str, " %lu", a[index]);
    }

    appendStringInfoChar(str, ')');
}

/**
 * @Description: converts a array set of integers to string.
 * @in str, store the the (a int int ...) format string in str.
 * @in a, the given uint16 array.
 * @in the length of the array.
 * @return none.
 * Note: the output format is "(a int int ...)", similar to an integer List.
 */
static void _outUint16Array(StringInfo str, uint16* a, int arrayLen)
{
    appendStringInfoChar(str, '(');
    appendStringInfoChar(str, 'a');

    for (int index = 0; index < arrayLen; index++) {
        appendStringInfo(str, " %d", a[index]);
    }

    appendStringInfoChar(str, ')');
}

/*
 * Print the value of a Datum given its type.
 */
static void _outDatum(StringInfo str, Datum value, int typlen, bool typbyval)
{
    Size length, i;
    char* s = NULL;

    length = datumGetSize(value, typbyval, typlen);

    if (typbyval) {
        s = (char*)(&value);
        appendStringInfo(str, "%u [ ", (unsigned int)length);
        for (i = 0; i < (Size)sizeof(Datum); i++) {
            appendStringInfo(str, "%d ", (int)(s[i]));
        }
        appendStringInfo(str, "]");
    } else {
        s = (char*)DatumGetPointer(value);
        if (!PointerIsValid(s)) {
            appendStringInfo(str, "0 [ ]");
        } else {
            appendStringInfo(str, "%u [ ", (unsigned int)length);
            for (i = 0; i < length; i++) {
                appendStringInfo(str, "%d ", (int)(s[i]));
            }
            appendStringInfo(str, "]");
        }
    }
}

/*
 * _outDistribution
 *     output a distribution contain node group information
 *
 * @param (in) str:
 *     the string to store information
 * @param (in) node:
 *     the Distribution node
 *
 * @return: void
 */
static void _outDistribution(StringInfo str, Distribution* node)
{
    WRITE_UINT_FIELD(group_oid);
    WRITE_BITMAPSET_FIELD(bms_data_nodeids);
}

static void _outHDFSTableAnalyze(StringInfo str, HDFSTableAnalyze* node)
{
    WRITE_NODE_TYPE("HDFSTABLEANALYZE");
    WRITE_NODE_FIELD(DnWorkFlow);
    WRITE_INT_FIELD(DnCnt);
    WRITE_BOOL_FIELD(isHdfsStore);
    appendStringInfo(str, " :sampleRate");
    for (int i = 0; i < ANALYZE_MODE_MAX_NUM - 1; i++) {
        appendStringInfo(str, " %.12f", node->sampleRate[i]);
    }
    WRITE_UINT_FIELD(orgCnNodeNo);
    WRITE_BOOL_FIELD(isHdfsForeignTbl);
    WRITE_BOOL_FIELD(sampleTableRequired);
    WRITE_NODE_FIELD(tmpSampleTblNameList);
    WRITE_ENUM_FIELD(disttype, DistributionType);
    WRITE_INT_FIELD(memUsage.work_mem);
    WRITE_INT_FIELD(memUsage.max_mem);
}

/*
 *	Stuff from plannodes.h
 */

static void _outPlannedStmt(StringInfo str, PlannedStmt* node)
{
    WRITE_NODE_TYPE("PLANNEDSTMT");

    WRITE_ENUM_FIELD(commandType, CmdType);
    WRITE_UINT64_FIELD(queryId);
    WRITE_BOOL_FIELD(hasReturning);
    WRITE_BOOL_FIELD(hasModifyingCTE);
    WRITE_BOOL_FIELD(canSetTag);
    WRITE_BOOL_FIELD(transientPlan);
    WRITE_BOOL_FIELD(dependsOnRole);
    WRITE_NODE_FIELD(planTree);
    WRITE_NODE_FIELD(rtable);
    WRITE_NODE_FIELD(resultRelations);
    WRITE_NODE_FIELD(utilityStmt);
    WRITE_NODE_FIELD(subplans);
    WRITE_BITMAPSET_FIELD(rewindPlanIDs);
    WRITE_NODE_FIELD(rowMarks);
    WRITE_NODE_FIELD(relationOids);
    WRITE_NODE_FIELD(invalItems);
    WRITE_INT_FIELD(nParamExec);
    WRITE_INT_FIELD(num_streams);
    WRITE_INT_FIELD(max_push_sql_num);
    WRITE_INT_FIELD(gather_count);
    WRITE_INT_FIELD(num_nodes);

    if (t_thrd.proc->workingVersionNum < 92097 || node->num_streams > 0) {
	    for (int i = 0; i < node->num_nodes; i++) {
	        /* Write the field name only one time and just append the value of each field */
	        appendStringInfo(str, " :nodesDefinition[%d]", i);
	        appendStringInfo(str, " %u", node->nodesDefinition[i].nodeoid);
	        appendStringInfo(str, " %s", node->nodesDefinition[i].nodename.data);
	        appendStringInfo(str, " %s", node->nodesDefinition[i].nodehost.data);
	        appendStringInfo(str, " %d", node->nodesDefinition[i].nodeport);
	        appendStringInfo(str, " %d", node->nodesDefinition[i].nodectlport);
	        appendStringInfo(str, " %d", node->nodesDefinition[i].nodesctpport);
	        appendStringInfo(str, " %s", node->nodesDefinition[i].nodehost1.data);
	        appendStringInfo(str, " %d", node->nodesDefinition[i].nodeport1);
	        appendStringInfo(str, " %d", node->nodesDefinition[i].nodectlport1);
	        appendStringInfo(str, " %d", node->nodesDefinition[i].nodesctpport1);
	        appendStringInfo(str, " %s", booltostr(node->nodesDefinition[i].nodeisprimary));
	        appendStringInfo(str, " %s", booltostr(node->nodesDefinition[i].nodeispreferred));
	    }
    }

    WRITE_INT_FIELD(instrument_option);
    WRITE_INT_FIELD(num_plannodes);
    WRITE_INT_FIELD(query_mem[0]);
    WRITE_INT_FIELD(query_mem[1]);
    WRITE_INT_FIELD(assigned_query_mem[0]);
    WRITE_INT_FIELD(assigned_query_mem[1]);

    WRITE_INT_FIELD(num_bucketmaps);
    for (int j = 0; j < node->num_bucketmaps; j++) {
        if (t_thrd.proc->workingVersionNum >= SEGMENT_PAGE_VERSION_NUM) {
            appendStringInfo(str, " :bucketCnt");
            appendStringInfo(str, " %d", node->bucketCnt[j]);
        }
        appendStringInfo(str, " :bucketMap");
        if (node->bucketMap[j]) {
            for (int i = 0; i < node->bucketCnt[j]; i++) {
                appendStringInfo(str, " %d", node->bucketMap[j][i]);
            }
        } else {
            appendStringInfo(str, " <>");
        }
    }

    WRITE_STRING_FIELD(query_string);
    WRITE_NODE_FIELD(subplan_ids);
    WRITE_NODE_FIELD(initPlan);
    /* data redistribution for DFS table. */
    WRITE_UINT_FIELD(dataDestRelIndex);
    WRITE_INT_FIELD(MaxBloomFilterNum);
    WRITE_INT_FIELD(query_dop);
    WRITE_BOOL_FIELD(in_compute_pool);
    WRITE_BOOL_FIELD(has_obsrel);

    WRITE_INT_FIELD(ng_num);
    for (int i = 0; i < node->ng_num; i++) {
        appendStringInfo(str, " %s", node->ng_queryMem[i].nodegroup);
        appendStringInfo(str, " %d", node->ng_queryMem[i].query_mem[0]);
        appendStringInfo(str, " %d", node->ng_queryMem[i].query_mem[1]);
    }
    WRITE_BOOL_FIELD(isRowTriggerShippable);
    WRITE_BOOL_FIELD(is_stream_plan);
}

/*
 * print the basic stuff of all nodes that inherit from Plan
 */
static void _outPlanInfo(StringInfo str, Plan* node)
{
    WRITE_INT_FIELD(plan_node_id);
    WRITE_INT_FIELD(parent_node_id);
    WRITE_ENUM_FIELD(exec_type, RemoteQueryExecType);
    WRITE_FLOAT_FIELD(startup_cost, "%.2f");
    WRITE_FLOAT_FIELD(total_cost, "%.2f");
    /* When sending rows to dn, transfer to local rows before output */
    appendStringInfo(str, " :plan_rows %.0f", PLAN_LOCAL_ROWS(node));
    WRITE_FLOAT_FIELD(multiple, "%.0f");
    WRITE_INT_FIELD(plan_width);
    WRITE_NODE_FIELD(targetlist);
    WRITE_NODE_FIELD(qual);
    WRITE_NODE_FIELD(lefttree);
    WRITE_NODE_FIELD(righttree);
    WRITE_BOOL_FIELD(ispwj);
    WRITE_INT_FIELD(paramno);
    if (t_thrd.proc->workingVersionNum >= SUBPARTITION_VERSION_NUM) {
        WRITE_INT_FIELD(subparamno);
    }
    WRITE_NODE_FIELD(initPlan);
    WRITE_NODE_FIELD(distributed_keys);
    WRITE_NODE_FIELD(exec_nodes);
    WRITE_BITMAPSET_FIELD(extParam);
    WRITE_BITMAPSET_FIELD(allParam);
    WRITE_BOOL_FIELD(vec_output);
    WRITE_BOOL_FIELD(hasUniqueResults);
    WRITE_BOOL_FIELD(isDeltaTable);
    WRITE_INT_FIELD(operatorMemKB[0]);
    WRITE_INT_FIELD(operatorMemKB[1]);
    WRITE_INT_FIELD(operatorMaxMem);

    WRITE_BOOL_FIELD(parallel_enabled);
    WRITE_BOOL_FIELD(hasHashFilter);
    WRITE_NODE_FIELD(var_list);
    WRITE_NODE_FIELD(filterIndexList);
    WRITE_INT_FIELD(dop);
    WRITE_INT_FIELD(recursive_union_plan_nodeid);
    WRITE_BOOL_FIELD(recursive_union_controller);
    WRITE_INT_FIELD(control_plan_nodeid);
    WRITE_BOOL_FIELD(is_sync_plannode);

    if (t_thrd.proc->workingVersionNum >= ML_OPT_MODEL_VERSION_NUM) {
        WRITE_FLOAT_FIELD(pred_rows, "%.0f");
        WRITE_FLOAT_FIELD(pred_startup_time, "%.0f");
        WRITE_FLOAT_FIELD(pred_total_time, "%.0f");
        WRITE_FLOAT_FIELD(pred_max_memory, "%ld");
    }
}

static void _outPruningResult(StringInfo str, PruningResult* node)
{
    const int num = 92267;
    WRITE_NODE_TYPE("PRUNINGRESULT");

    WRITE_ENUM_FIELD(state, PruningResultState);
    /* skip boundary info */
    WRITE_BITMAPSET_FIELD(bm_rangeSelectedPartitions);
    WRITE_INT_FIELD(intervalOffset);
    WRITE_BITMAPSET_FIELD(intervalSelectedPartitions);
    WRITE_NODE_FIELD(ls_rangeSelectedPartitions);
    if (t_thrd.proc->workingVersionNum >= SUBPARTITION_VERSION_NUM) {
        WRITE_NODE_FIELD(ls_selectedSubPartitions);
    }
    if (t_thrd.proc->workingVersionNum >= num) {
        WRITE_NODE_FIELD(expr);
    }
    if (t_thrd.proc->workingVersionNum >= PBESINGLEPARTITION_VERSION_NUM) {
        WRITE_BOOL_FIELD(isPbeSinlePartition);
    }
}

static void _outSubPartitionPruningResult(StringInfo str, SubPartitionPruningResult* node)
{
    WRITE_NODE_TYPE("SUBPARTITIONPRUNINGRESULT");

    WRITE_INT_FIELD(partSeq);
    WRITE_BITMAPSET_FIELD(bm_selectedSubPartitions);
    WRITE_NODE_FIELD(ls_selectedSubPartitions);
}

/*
 * print the basic stuff of all nodes that inherit from Scan
 */
static void _outScanInfo(StringInfo str, Scan* node)
{
    _outPlanInfo(str, (Plan*)node);

    WRITE_UINT_FIELD(scanrelid);
    WRITE_BOOL_FIELD(isPartTbl);
    WRITE_INT_FIELD(itrs);
    WRITE_NODE_FIELD(pruningInfo);
    if (t_thrd.proc->workingVersionNum >= 92063) {
        WRITE_NODE_FIELD(bucketInfo);
    }
    WRITE_ENUM_FIELD(partScanDirection, ScanDirection);
    WRITE_BOOL_FIELD(scan_qual_optimized);
    WRITE_BOOL_FIELD(predicate_pushdown_optimized);
    WRITE_NODE_FIELD(tablesample);

    out_mem_info(str, &node->mem_info);
    if (t_thrd.proc->workingVersionNum >= SCAN_BATCH_MODE_VERSION_NUM) {
        WRITE_BOOL_FIELD(scanBatchMode);
    }
}

/*
 * print the basic stuff of all nodes that inherit from Join
 */
static void _outJoinPlanInfo(StringInfo str, Join* node)
{
    _outPlanInfo(str, (Plan*)node);

    WRITE_ENUM_FIELD(jointype, JoinType);
    WRITE_NODE_FIELD(joinqual);
    WRITE_BOOL_FIELD(optimizable);
    WRITE_NODE_FIELD(nulleqqual);
    WRITE_UINT_FIELD(skewoptimize);
}

static void _outPlan(StringInfo str, Plan* node)
{
    WRITE_NODE_TYPE("PLAN");

    _outPlanInfo(str, (Plan*)node);
}

static void _outResult(StringInfo str, BaseResult* node)
{
    WRITE_NODE_TYPE("RESULT");

    _outPlanInfo(str, (Plan*)node);

    WRITE_NODE_FIELD(resconstantqual);
}

static void _outModifyTable(StringInfo str, ModifyTable* node)
{
    WRITE_NODE_TYPE("MODIFYTABLE");

    _outPlanInfo(str, (Plan*)node);

    WRITE_ENUM_FIELD(operation, CmdType);
    WRITE_BOOL_FIELD(canSetTag);
    WRITE_NODE_FIELD(resultRelations);
    WRITE_INT_FIELD(resultRelIndex);
    WRITE_NODE_FIELD(plans);
    WRITE_NODE_FIELD(returningLists);
    WRITE_NODE_FIELD(fdwPrivLists);
    WRITE_NODE_FIELD(rowMarks);
    WRITE_INT_FIELD(epqParam);
    WRITE_BOOL_FIELD(partKeyUpdated);
#ifdef PGXC
    WRITE_NODE_FIELD(remote_plans);
    WRITE_NODE_FIELD(remote_insert_plans);
    WRITE_NODE_FIELD(remote_update_plans);
    WRITE_NODE_FIELD(remote_delete_plans);
#endif
    WRITE_BOOL_FIELD(is_dist_insertselect);
    WRITE_NODE_FIELD(cacheEnt);

    WRITE_INT_FIELD(mergeTargetRelation);
    WRITE_NODE_FIELD(mergeSourceTargetList);
    WRITE_NODE_FIELD(mergeActionList);
#ifdef ENABLE_MULTIPLE_NODES
    if (t_thrd.proc->workingVersionNum >= UPSERT_ROW_STORE_VERSION_NUM) {
        WRITE_ENUM_FIELD(upsertAction, UpsertAction);
        WRITE_NODE_FIELD(updateTlist);
        WRITE_NODE_FIELD(exclRelTlist);
        WRITE_INT_FIELD(exclRelRTIndex);
    }
    if (t_thrd.proc->workingVersionNum >= UPSERT_WHERE_VERSION_NUM) {
        WRITE_NODE_FIELD(upsertWhere);
    }
#else
    WRITE_ENUM_FIELD(upsertAction, UpsertAction);
    WRITE_NODE_FIELD(updateTlist);
    WRITE_NODE_FIELD(exclRelTlist);
    WRITE_INT_FIELD(exclRelRTIndex);
    WRITE_NODE_FIELD(upsertWhere);
#endif		
}

static void _outUpsertClause(StringInfo str, const UpsertClause* node)
{
    WRITE_NODE_TYPE("UPSERTCLAUSE");

    WRITE_NODE_FIELD(targetList);
    WRITE_INT_FIELD(location);
    if (t_thrd.proc->workingVersionNum >= UPSERT_WHERE_VERSION_NUM) {
        WRITE_NODE_FIELD(whereClause);
    }
}

static void _outUpsertExpr(StringInfo str, const UpsertExpr* node)
{
    WRITE_NODE_TYPE("UPSERTEXPR");

    WRITE_ENUM_FIELD(upsertAction, UpsertAction);
    WRITE_NODE_FIELD(updateTlist);
    WRITE_NODE_FIELD(exclRelTlist);
    WRITE_INT_FIELD(exclRelIndex);
    WRITE_NODE_FIELD(upsertWhere);
}
static void _outMergeWhenClause(StringInfo str, const MergeWhenClause* node)
{
    WRITE_NODE_TYPE("MERGEWHENCLAUSE");

    WRITE_BOOL_FIELD(matched);
    WRITE_ENUM_FIELD(commandType, CmdType);
    WRITE_NODE_FIELD(condition);
    WRITE_NODE_FIELD(targetList);
    WRITE_NODE_FIELD(cols);
    WRITE_NODE_FIELD(values);
}

static void _outAppend(StringInfo str, Append* node)
{
    WRITE_NODE_TYPE("APPEND");

    _outPlanInfo(str, (Plan*)node);

    WRITE_NODE_FIELD(appendplans);
}

static void _outVecAppend(StringInfo str, Append* node)
{
    WRITE_NODE_TYPE("VECAPPEND");

    _outPlanInfo(str, (Plan*)node);

    WRITE_NODE_FIELD(appendplans);
}

static void _outMergeAppend(StringInfo str, MergeAppend* node)
{
    int i;

    WRITE_NODE_TYPE("MERGEAPPEND");

    _outPlanInfo(str, (Plan*)node);

    WRITE_NODE_FIELD(mergeplans);

    WRITE_INT_FIELD(numCols);

    appendStringInfo(str, " :sortColIdx");
    for (i = 0; i < node->numCols; i++) {
        appendStringInfo(str, " %d", node->sortColIdx[i]);
    }

    WRITE_GRPOP_FIELD(sortOperators, numCols);

    appendStringInfo(str, " :collations");
    for (i = 0; i < node->numCols; i++) {
        appendStringInfo(str, " %u", node->collations[i]);
    }

    // We should use collcation name instead of colloid when
    // node->collOid is user-defined, because DN and CN use different OID
    // though the objection name is the same
    // Note that if this function change, you must check function _readSort()
    //
    for (i = 0; i < node->numCols; i++) {
        if (node->collations[i] >= FirstBootstrapObjectId && IsStatisfyUpdateCompatibility(node->collations[i])) {
            appendStringInfo(str, " :collname ");
            _outToken(str, get_collation_name(node->collations[i]));
        }
    }

    appendStringInfo(str, " :nullsFirst");
    for (i = 0; i < node->numCols; i++) {
        appendStringInfo(str, " %s", booltostr(node->nullsFirst[i]));
    }
}

static void _outStartWithOp(StringInfo str, StartWithOp *node)
{
    WRITE_NODE_TYPE("STARTWITHOP");

    _outPlanInfo(str, (Plan*)node);

    WRITE_NODE_FIELD(cteplan);
    WRITE_NODE_FIELD(ruplan);

    WRITE_NODE_FIELD(keyEntryList);
    WRITE_NODE_FIELD(colEntryList);
    WRITE_NODE_FIELD(internalEntryList);
    WRITE_NODE_FIELD(fullEntryList);

    WRITE_NODE_FIELD(swoptions);
}

static void _outStartWithOptions(StringInfo str, StartWithOptions* node)
{
    WRITE_NODE_TYPE("STARTWITHOPTIONS");

    WRITE_NODE_FIELD(siblings_orderby_clause);
    WRITE_NODE_FIELD(prior_key_index);
    WRITE_ENUM_FIELD(connect_by_type, StartWithConnectByType);
    WRITE_NODE_FIELD(connect_by_level_quals);
    WRITE_NODE_FIELD(connect_by_other_quals);
    WRITE_BOOL_FIELD(nocycle);
}

static void _outRecursiveUnion(StringInfo str, RecursiveUnion* node)
{
    int i;

    WRITE_NODE_TYPE("RECURSIVEUNION");

    _outPlanInfo(str, (Plan*)node);

    WRITE_INT_FIELD(wtParam);
    WRITE_INT_FIELD(numCols);

    appendStringInfo(str, " :dupColIdx");
    for (i = 0; i < node->numCols; i++) {
        appendStringInfo(str, " %d", node->dupColIdx[i]);
    }

    WRITE_GRPOP_FIELD(dupOperators, numCols);
    WRITE_LONG_FIELD(numGroups);
    WRITE_BOOL_FIELD(has_inner_stream);
    WRITE_BOOL_FIELD(has_outer_stream);
    WRITE_BOOL_FIELD(is_used);
    WRITE_BOOL_FIELD(is_correlated);
    if (t_thrd.proc->workingVersionNum >= SWCB_VERSION_NUM) {
        WRITE_NODE_FIELD(internalEntryList);
    }
}

static void _outBitmapAnd(StringInfo str, BitmapAnd* node)
{
    WRITE_NODE_TYPE("BITMAPAND");

    _outPlanInfo(str, (Plan*)node);

    WRITE_NODE_FIELD(bitmapplans);
    if (t_thrd.proc->workingVersionNum >= INPLACE_UPDATE_VERSION_NUM) {
        WRITE_BOOL_FIELD(is_ustore);
    }
}

static void _outBitmapOr(StringInfo str, BitmapOr* node)
{
    WRITE_NODE_TYPE("BITMAPOR");

    _outPlanInfo(str, (Plan*)node);

    WRITE_NODE_FIELD(bitmapplans);
    if (t_thrd.proc->workingVersionNum >= INPLACE_UPDATE_VERSION_NUM) {
        WRITE_BOOL_FIELD(is_ustore);
    }
}
static void _outCStoreIndexOr(StringInfo str, CStoreIndexOr* node)
{
    WRITE_NODE_TYPE("CSTOREINDEXOR");

    _outPlanInfo(str, (Plan*)node);

    WRITE_NODE_FIELD(bitmapplans);
}

static void _outCStoreIndexAnd(StringInfo str, CStoreIndexAnd* node)
{
    WRITE_NODE_TYPE("CSTOREINDEXAND");

    _outPlanInfo(str, (Plan*)node);

    WRITE_NODE_FIELD(bitmapplans);
}

static void _outBucketInfo(StringInfo str, BucketInfo* node)
{
    WRITE_NODE_TYPE("BUCKETINFO");

    WRITE_NODE_FIELD(buckets);
}

static void _outScan(StringInfo str, Scan* node)
{
    WRITE_NODE_TYPE("SCAN");

    _outScanInfo(str, (Scan*)node);
}

static void _outSeqScan(StringInfo str, SeqScan* node)
{
    WRITE_NODE_TYPE("SEQSCAN");

    _outScanInfo(str, (Scan*)node);
}

template <typename T>
static void _outCommonIndexScanPart(StringInfo str, T* node)
{
    _outScanInfo(str, (Scan*)node);
    WRITE_OID_FIELD(indexid);
#ifdef STREAMPLAN
    if (node->indexid >= FirstBootstrapObjectId && IsStatisfyUpdateCompatibility(node->indexid)) {
        appendStringInfo(str, " :indexname ");
        _outToken(str, get_rel_name(node->indexid));
        appendStringInfo(str, " :indexnamespace ");
        _outToken(str, get_namespace_name(get_rel_namespace(node->indexid)));
    }
#endif  // STREAMPLAN
    WRITE_NODE_FIELD(indexqual);
    WRITE_NODE_FIELD(indexqualorig);
    WRITE_NODE_FIELD(indexorderby);
    WRITE_NODE_FIELD(indexorderbyorig);
    WRITE_ENUM_FIELD(indexorderdir, ScanDirection);
}

static void _outIndexScan(StringInfo str, IndexScan* node)
{
    WRITE_NODE_TYPE("INDEXSCAN");
    _outCommonIndexScanPart<IndexScan>(str, node);
    if (t_thrd.proc->workingVersionNum >= INPLACE_UPDATE_VERSION_NUM) {
        WRITE_BOOL_FIELD(is_ustore);
    }
}

static void _outCStoreIndexScan(StringInfo str, CStoreIndexScan* node)
{
    WRITE_NODE_TYPE("CSTOREINDEXSCAN");
    _outCommonIndexScanPart<CStoreIndexScan>(str, node);

    WRITE_NODE_FIELD(baserelcstorequal);
    WRITE_NODE_FIELD(cstorequal);
    WRITE_NODE_FIELD(indextlist);
    WRITE_ENUM_FIELD(relStoreLocation, RelstoreType);
    WRITE_BOOL_FIELD(indexonly);
}

static void _outDfsIndexScan(StringInfo str, DfsIndexScan* node)
{
    WRITE_NODE_TYPE("DFSINDEXSCAN");

    _outScanInfo(str, (Scan*)node);

    WRITE_OID_FIELD(indexid);
#ifdef STREAMPLAN
    if (node->indexid >= FirstBootstrapObjectId && IsStatisfyUpdateCompatibility(node->indexid)) {
        appendStringInfo(str, " :indexname ");
        _outToken(str, get_rel_name(node->indexid));
        appendStringInfo(str, " :indexnamespace ");
        _outToken(str, get_namespace_name(get_rel_namespace(node->indexid)));
    }
#endif  // STREAMPLAN
    WRITE_NODE_FIELD(indextlist);
    WRITE_NODE_FIELD(indexqual);
    WRITE_NODE_FIELD(indexqualorig);
    WRITE_NODE_FIELD(indexorderby);
    WRITE_NODE_FIELD(indexorderbyorig);
    WRITE_ENUM_FIELD(indexorderdir, ScanDirection);
    WRITE_ENUM_FIELD(relStoreLocation, RelstoreType);
    WRITE_NODE_FIELD(cstorequal);
    WRITE_NODE_FIELD(indexScantlist);
    WRITE_NODE_FIELD(dfsScan);
    WRITE_BOOL_FIELD(indexonly);
}

static void _outStream(StringInfo str, Stream* node)
{
    WRITE_NODE_TYPE("STREAM");

    _outScanInfo(str, (Scan*)node);

    WRITE_ENUM_FIELD(type, StreamType);
    WRITE_STRING_FIELD(plan_statement);
    WRITE_NODE_FIELD(consumer_nodes);
    WRITE_NODE_FIELD(distribute_keys);
    WRITE_BOOL_FIELD(is_sorted);
    WRITE_NODE_FIELD(sort);
    WRITE_BOOL_FIELD(is_dummy);
    WRITE_INT_FIELD(smpDesc.consumerDop);
    WRITE_INT_FIELD(smpDesc.producerDop);
    WRITE_ENUM_FIELD(smpDesc.distriType, SmpStreamType);
    WRITE_NODE_FIELD(skew_list);
    WRITE_INT_FIELD(stream_level);
    WRITE_NODE_FIELD(origin_consumer_nodes);
    WRITE_BOOL_FIELD(is_recursive_local);
}

/*
 * _outStreamPath
 *     output a StreamPath node
 *
 * @param (in) str:
 *     the string to store information
 * @param (in) node:
 *     the StreamPath node
 *
 * @return: void
 */
static void _outStreamPath(StringInfo str, StreamPath* node)
{
    WRITE_NODE_TYPE("STREAMPATH");

    _outPathInfo(str, (Path*)node);
    WRITE_ENUM_FIELD(type, StreamType);
    WRITE_NODE_FIELD(subpath);
    _outDistribution(str, &(node->consumer_distribution));
}

#ifdef PGXC

template <typename T>
static void _outCommonRemoteQueryPart(StringInfo str, T* node)
{
    int i;
    _outScanInfo(str, (Scan*)node);

    WRITE_ENUM_FIELD(exec_direct_type, ExecDirectType);
    WRITE_STRING_FIELD(sql_statement);
    WRITE_NODE_FIELD(exec_nodes);
    WRITE_ENUM_FIELD(combine_type, CombineType);
    WRITE_BOOL_FIELD(read_only);
    WRITE_BOOL_FIELD(force_autocommit);
    WRITE_STRING_FIELD(statement);
    WRITE_STRING_FIELD(cursor);
    WRITE_INT_FIELD(rq_num_params);

    if (node->rq_num_params > 0) {
        appendStringInfo(str, " :rq_param_types");
    }
    for (i = 0; i < node->rq_num_params; i++) {
        appendStringInfo(str, " %d", node->rq_param_types[i]);    
    }
    
    WRITE_BOOL_FIELD(rq_params_internal);

    WRITE_ENUM_FIELD(exec_type, RemoteQueryExecType);
    WRITE_BOOL_FIELD(is_temp);

    WRITE_BOOL_FIELD(rq_finalise_aggs);
    WRITE_BOOL_FIELD(rq_sortgroup_colno);
    WRITE_NODE_FIELD(remote_query);
    WRITE_NODE_FIELD(base_tlist);
    WRITE_NODE_FIELD(coord_var_tlist);
    WRITE_NODE_FIELD(query_var_tlist);
    WRITE_BOOL_FIELD(has_row_marks);
    WRITE_BOOL_FIELD(rq_save_command_id);
    WRITE_BOOL_FIELD(is_simple);
    WRITE_BOOL_FIELD(mergesort_required);
    WRITE_BOOL_FIELD(spool_no_data);
    WRITE_BOOL_FIELD(poll_multi_channel);
    WRITE_INT_FIELD(num_stream);
    WRITE_INT_FIELD(num_gather);
    WRITE_NODE_FIELD(sort);
    WRITE_NODE_FIELD(rte_ref);
    WRITE_ENUM_FIELD(position, RemoteQueryType);

    WRITE_TYPEINFO_ARRAY(rq_param_types, rq_num_params);
    if (t_thrd.proc->workingVersionNum >= EXECUTE_DIRECT_ON_MULTI_VERSION_NUM) {
        WRITE_BOOL_FIELD(is_remote_function_query);
    }
    if (t_thrd.proc->workingVersionNum >= FIX_PBE_CUSTOME_PLAN_BUG_VERSION_NUM) {
        WRITE_BOOL_FIELD(isCustomPlan);
        WRITE_BOOL_FIELD(isFQS);
    }
    if (t_thrd.proc->workingVersionNum >= FIX_SQL_ADD_RELATION_REF_COUNT) {
        WRITE_NODE_FIELD(relationOids);
    }  
}
static void _outRemoteQuery(StringInfo str, RemoteQuery* node)
{
    WRITE_NODE_TYPE("REMOTEQUERY");
    _outCommonRemoteQueryPart<RemoteQuery>(str, node);
}

/*
 * _outRemoteQueryPath
 *     output a RemoteQueryPath node's information
 *
 * @param (in) str:
 *     the string to store information
 * @param (in) node:
 *     the RemoteQueryPath node
 *
 * @return: void
 */
static void _outRemoteQueryPath(StringInfo str, RemoteQueryPath* node)
{
    WRITE_NODE_TYPE("REMOTEQUERYPATH");

    _outPathInfo(str, (Path*)node);
    WRITE_NODE_FIELD(rqpath_en);
    WRITE_NODE_FIELD(leftpath);
    WRITE_NODE_FIELD(rightpath);
    WRITE_ENUM_FIELD(jointype, JoinType);
    WRITE_NODE_FIELD(join_restrictlist);
    WRITE_BOOL_FIELD(rqhas_unshippable_qual);
    WRITE_BOOL_FIELD(rqhas_temp_rel);
    WRITE_BOOL_FIELD(rqhas_unshippable_tlist);
}

static void _outSliceBoundary(StringInfo str, SliceBoundary* node)
{
    WRITE_NODE_TYPE("SLICEBOUNDARY");
    WRITE_INT_FIELD(nodeIdx);
    WRITE_INT_FIELD(len);
    WRITE_ARRAY_FIELD(boundary, node->len);
}

static void _outExecBoundary(StringInfo str, ExecBoundary* node)
{
    WRITE_NODE_TYPE("EXECBOUNDARY");
    WRITE_CHAR_FIELD(locatorType);
    WRITE_INT_FIELD(count);
    WRITE_ARRAY_FIELD(eles, node->count);
}

/*
 * _outExecNodes
 *     output a ExecNodes node's information
 *
 * @param (in) str:
 *     the string to store information
 * @param (in) node:
 *     the ExecNodes node
 *
 * @return: void
 */
static void _outExecNodes(StringInfo str, ExecNodes* node)
{
    WRITE_NODE_TYPE("EXEC_NODES");

    WRITE_NODE_FIELD(primarynodelist);
    WRITE_NODE_FIELD(nodeList);
    _outDistribution(str, &node->distribution);
    WRITE_CHAR_FIELD(baselocatortype);
    WRITE_NODE_FIELD(en_expr);
    WRITE_OID_FIELD(en_relid);

    if (node->boundaries != NULL) {
        WRITE_NODE_FIELD(boundaries);
    }

    WRITE_ENUM_FIELD(accesstype, RelationAccessType);
    WRITE_NODE_FIELD(en_dist_vars);
    WRITE_INT_FIELD(bucketmapIdx);
    WRITE_BOOL_FIELD(nodelist_is_nil);
    WRITE_NODE_FIELD(original_nodeList);
    WRITE_NODE_FIELD(dynamic_en_expr);
    if (t_thrd.proc->workingVersionNum >= 92106) {
        WRITE_INT_FIELD(bucketid);
        WRITE_NODE_FIELD(bucketexpr);
        WRITE_OID_FIELD(bucketrelid);
        /*
         * shipping out for dn, relid will be different on dn,
         * so relname and relnamespace string will be must.
         */
        if (node->bucketrelid >= FirstBootstrapObjectId && IsStatisfyUpdateCompatibility(node->bucketrelid)) {
            char* rte_relname = NULL;
            char* rte_relnamespace = NULL;

            getNameById(node->bucketrelid, "RTE", &rte_relnamespace, &rte_relname);

            appendStringInfo(str, " :relname ");
            _outToken(str, rte_relname);
            appendStringInfo(str, " :relnamespace ");
            _outToken(str, rte_relnamespace);
        }
    }
}
#endif

static void _outIndexOnlyScan(StringInfo str, IndexOnlyScan* node)
{
    WRITE_NODE_TYPE("INDEXONLYSCAN");

    _outScanInfo(str, (Scan*)node);

    WRITE_OID_FIELD(indexid);
    if (node->indexid >= FirstBootstrapObjectId && IsStatisfyUpdateCompatibility(node->indexid)) {
        /*
         * For inherit table, the relname will be different
         */
        appendStringInfo(str, " :indexname ");
        _outToken(str, get_rel_name(node->indexid));
        appendStringInfo(str, " :indexnamespace ");
        _outToken(str, get_namespace_name(get_rel_namespace(node->indexid)));
    }
    WRITE_NODE_FIELD(indexqual);
    WRITE_NODE_FIELD(indexorderby);
    WRITE_NODE_FIELD(indextlist);
    WRITE_ENUM_FIELD(indexorderdir, ScanDirection);
}

static void _outBitmapIndexScan(StringInfo str, BitmapIndexScan* node)
{
    WRITE_NODE_TYPE("BITMAPINDEXSCAN");

    _outScanInfo(str, (Scan*)node);

    WRITE_OID_FIELD(indexid);
    WRITE_NODE_FIELD(indexqual);
    WRITE_NODE_FIELD(indexqualorig);
#ifdef STREAMPLAN
    if (node->indexid >= FirstBootstrapObjectId && IsStatisfyUpdateCompatibility(node->indexid)) {
        appendStringInfo(str, " :indexname ");
        _outToken(str, get_rel_name(node->indexid));
        appendStringInfo(str, " :indexnamespace ");
        _outToken(str, get_namespace_name(get_rel_namespace(node->indexid)));
    }
#endif  // STREAMPLAN
    if (t_thrd.proc->workingVersionNum >= INPLACE_UPDATE_VERSION_NUM) {
        WRITE_BOOL_FIELD(is_ustore);
    }
}

static void _outBitmapHeapScan(StringInfo str, BitmapHeapScan* node)
{
    WRITE_NODE_TYPE("BITMAPHEAPSCAN");

    _outScanInfo(str, (Scan*)node);

    WRITE_NODE_FIELD(bitmapqualorig);
}

static void _outCStoreIndexCtidScan(StringInfo str, CStoreIndexCtidScan* node)
{
    WRITE_NODE_TYPE("CSTOREINDEXCTIDSCAN");

    _outScanInfo(str, (Scan*)node);

    WRITE_OID_FIELD(indexid);
    WRITE_NODE_FIELD(indexqual);
    WRITE_NODE_FIELD(cstorequal);
    WRITE_NODE_FIELD(indexqualorig);
#ifdef STREAMPLAN
    if (node->indexid >= FirstBootstrapObjectId && IsStatisfyUpdateCompatibility(node->indexid)) {
        appendStringInfo(str, " :indexname ");
        _outToken(str, get_rel_name(node->indexid));
        appendStringInfo(str, " :indexnamespace ");
        _outToken(str, get_namespace_name(get_rel_namespace(node->indexid)));
    }
#endif  // STREAMPLAN
    WRITE_NODE_FIELD(indextlist);
}

static void _outCStoreIndexHeapScan(StringInfo str, CStoreIndexHeapScan* node)
{
    WRITE_NODE_TYPE("CSTOREINDEXHEAPSCAN");

    _outScanInfo(str, (Scan*)node);

    WRITE_NODE_FIELD(bitmapqualorig);
}

static void _outTidScan(StringInfo str, TidScan* node)
{
    WRITE_NODE_TYPE("TIDSCAN");

    _outScanInfo(str, (Scan*)node);

    WRITE_NODE_FIELD(tidquals);
}

static void _outPartIteratorParam(StringInfo str, PartIteratorParam* node)
{
    WRITE_NODE_TYPE("PARTITERATORPARAM");

    WRITE_INT_FIELD(paramno);
    if (t_thrd.proc->workingVersionNum >= SUBPARTITION_VERSION_NUM) {
        WRITE_INT_FIELD(subPartParamno);
    }
}

static void _outPartIterator(StringInfo str, PartIterator* node)
{
    WRITE_NODE_TYPE("PARTITERATOR");

    _outPlanInfo(str, (Plan*)node);

    WRITE_ENUM_FIELD(partType, PartitionType);
    WRITE_INT_FIELD(itrs);
    WRITE_ENUM_FIELD(direction, ScanDirection);
    WRITE_NODE_FIELD(param);
}

static void _outSubqueryScan(StringInfo str, SubqueryScan* node)
{
    WRITE_NODE_TYPE("SUBQUERYSCAN");

    _outScanInfo(str, (Scan*)node);

    WRITE_NODE_FIELD(subplan);
}

static void _outFunctionScan(StringInfo str, FunctionScan* node)
{
    WRITE_NODE_TYPE("FUNCTIONSCAN");

    _outScanInfo(str, (Scan*)node);

    WRITE_NODE_FIELD(funcexpr);
    WRITE_NODE_FIELD(funccolnames);
    WRITE_NODE_FIELD(funccoltypes);
    WRITE_NODE_FIELD(funccoltypmods);
    WRITE_NODE_FIELD(funccolcollations);

    WRITE_TYPEINFO_LIST(funccoltypes);
}

static void _outValuesScan(StringInfo str, ValuesScan* node)
{
    WRITE_NODE_TYPE("VALUESSCAN");

    _outScanInfo(str, (Scan*)node);

    WRITE_NODE_FIELD(values_lists);
}

static void _outCteScan(StringInfo str, CteScan* node)
{
    WRITE_NODE_TYPE("CTESCAN");

    _outScanInfo(str, (Scan*)node);

    WRITE_INT_FIELD(ctePlanId);
    WRITE_INT_FIELD(cteParam);
    if (t_thrd.proc->workingVersionNum >= SWCB_VERSION_NUM) {
        WRITE_NODE_FIELD(cteRef);
        WRITE_NODE_FIELD(internalEntryList);
    }
}

static void _outWorkTableScan(StringInfo str, WorkTableScan* node)
{
    WRITE_NODE_TYPE("WORKTABLESCAN");

    _outScanInfo(str, (Scan*)node);

    WRITE_INT_FIELD(wtParam);
    if (t_thrd.proc->workingVersionNum >= SWCB_VERSION_NUM) {
        WRITE_BOOL_FIELD(forStartWith);
    }
}

template <typename T>
static void _outCommonForeignScanPart(StringInfo str, T* node)
{
    _outScanInfo(str, (Scan*)node);

    WRITE_OID_FIELD(scan_relid);
    WRITE_NODE_FIELD(fdw_exprs);
    WRITE_NODE_FIELD(fdw_private);
    WRITE_BOOL_FIELD(fsSystemCol);
    WRITE_BOOL_FIELD(needSaveError);
    WRITE_NODE_FIELD(errCache);
    WRITE_NODE_FIELD(prunningResult);
    WRITE_NODE_FIELD(rel);
    WRITE_NODE_FIELD(options);
    WRITE_LONG_FIELD(objectNum);
    WRITE_INT_FIELD(bfNum);

    for (int i = 0; i < node->bfNum; i++) {
        WRITE_NODE_FIELD(bloomFilterSet[i]);
    }
    WRITE_BOOL_FIELD(in_compute_pool);
}
static void _outForeignScan(StringInfo str, ForeignScan* node)
{
    WRITE_NODE_TYPE("FOREIGNSCAN");
    _outCommonForeignScanPart<ForeignScan>(str, node);
}

static void _outForeignPartState(StringInfo str, ForeignPartState* node)
{
    WRITE_NODE_TYPE("FOREIGNPARTSTATE");

    WRITE_NODE_FIELD(partitionKey);
}

static void _outExtensiblePlan(StringInfo str, ExtensiblePlan* node)
{
    WRITE_NODE_TYPE("EXTENSIBLEPLAN");

    _outScanInfo(str, (Scan*)node);

    WRITE_UINT_FIELD(flags);
    WRITE_NODE_FIELD(extensible_plans);
    WRITE_NODE_FIELD(extensible_exprs);
    WRITE_NODE_FIELD(extensible_private);
    WRITE_NODE_FIELD(extensible_plan_tlist);
    WRITE_BITMAPSET_FIELD(extensible_relids);
    /* ExtensibleName is a key to lookup ExtensiblePlanMethods */
    appendStringInfoString(str, " :methods ");
    _outToken(str, node->methods->ExtensibleName);
}

static void _outJoin(StringInfo str, Join* node)
{
    WRITE_NODE_TYPE("JOIN");

    _outJoinPlanInfo(str, (Join*)node);
}

static void _outNestLoop(StringInfo str, NestLoop* node)
{
    WRITE_NODE_TYPE("NESTLOOP");

    _outJoinPlanInfo(str, (Join*)node);

    WRITE_NODE_FIELD(nestParams);
    WRITE_BOOL_FIELD(materialAll);
}

static void _outVecNestLoop(StringInfo str, VecNestLoop* node)
{
    WRITE_NODE_TYPE("VECNESTLOOP");

    _outJoinPlanInfo(str, (Join*)node);

    WRITE_NODE_FIELD(nestParams);
    WRITE_BOOL_FIELD(materialAll);
}

static void _outVecMaterial(StringInfo str, VecMaterial* node)
{
    WRITE_NODE_TYPE("VECMATERIAL");

    _outPlanInfo(str, (Plan*)node);
    WRITE_BOOL_FIELD(materialize_all);
    out_mem_info(str, &node->mem_info);
}

template <typename T>
static void _outCommonJoinPart(StringInfo str, T* node)
{
    int numCols;
    int i;

    _outJoinPlanInfo(str, (Join*)node);

    WRITE_NODE_FIELD(mergeclauses);

    numCols = list_length(node->mergeclauses);

    appendStringInfo(str, " :mergeFamilies");
    for (i = 0; i < numCols; i++) {
        appendStringInfo(str, " %u", node->mergeFamilies[i]);
    }

    appendStringInfo(str, " :mergeCollations");
    for (i = 0; i < numCols; i++) {
        appendStringInfo(str, " %u", node->mergeCollations[i]);
    }

    appendStringInfo(str, " :mergeStrategies");
    for (i = 0; i < numCols; i++) {
        appendStringInfo(str, " %d", node->mergeStrategies[i]);
    }

    appendStringInfo(str, " :mergeNullsFirst");
    for (i = 0; i < numCols; i++) {
        appendStringInfo(str, " %s", booltostr(node->mergeNullsFirst[i]));
    }
}

static void _outMergeJoin(StringInfo str, MergeJoin* node)
{
    WRITE_NODE_TYPE("MERGEJOIN");
    _outCommonJoinPart<MergeJoin>(str, node);
}

static void _outVecMergeJoin(StringInfo str, VecMergeJoin* node)
{
    WRITE_NODE_TYPE("VECMERGEJOIN");
    _outCommonJoinPart<VecMergeJoin>(str, node);
}
static void _outHashJoin(StringInfo str, HashJoin* node)
{
    WRITE_NODE_TYPE("HASHJOIN");

    _outJoinPlanInfo(str, (Join*)node);

    WRITE_NODE_FIELD(hashclauses);
    WRITE_BOOL_FIELD(streamBothSides);
    WRITE_BOOL_FIELD(transferFilterFlag);
    WRITE_BOOL_FIELD(rebuildHashTable);
    WRITE_BOOL_FIELD(isSonicHash);
    out_mem_info(str, &node->mem_info);
}

static void _outVecHashJoin(StringInfo str, VecHashJoin* node)
{
    WRITE_NODE_TYPE("VECHASHJOIN");

    _outJoinPlanInfo(str, (Join*)node);

    WRITE_NODE_FIELD(hashclauses);
    WRITE_BOOL_FIELD(streamBothSides);
    WRITE_BOOL_FIELD(transferFilterFlag);
    WRITE_BOOL_FIELD(rebuildHashTable);
    WRITE_BOOL_FIELD(isSonicHash);
    out_mem_info(str, &node->mem_info);
}

static void _outVecHashAgg(StringInfo str, VecAgg* node)
{
    int i;

    WRITE_NODE_TYPE("VECAGG");

    _outPlanInfo(str, (Plan*)node);

    WRITE_ENUM_FIELD(aggstrategy, AggStrategy);
    WRITE_INT_FIELD(numCols);

    appendStringInfo(str, " :grpColIdx");
    for (i = 0; i < node->numCols; i++) {
        appendStringInfo(str, " %d", node->grpColIdx[i]);
    }

    appendStringInfo(str, " :grpOperators");
    for (i = 0; i < node->numCols; i++) {
        appendStringInfo(str, " %u", node->grpOperators[i]);
    }

    WRITE_LONG_FIELD(numGroups);
    WRITE_NODE_FIELD(groupingSets);
    WRITE_NODE_FIELD(chain);
    WRITE_BOOL_FIELD(is_final);
    WRITE_BOOL_FIELD(single_node);
    WRITE_BITMAPSET_FIELD(aggParams);
    out_mem_info(str, &node->mem_info);
    WRITE_BOOL_FIELD(is_sonichash);
    WRITE_UINT_FIELD(skew_optimize);
    if (t_thrd.proc->workingVersionNum >= SUBLINKPULLUP_VERSION_NUM) {
        WRITE_BOOL_FIELD(unique_check);
    }
}

static void _outAgg(StringInfo str, Agg* node)
{
    int i;
    WRITE_NODE_TYPE("AGG");

    _outPlanInfo(str, (Plan*)node);

    WRITE_ENUM_FIELD(aggstrategy, AggStrategy);
    WRITE_INT_FIELD(numCols);

    appendStringInfo(str, " :grpColIdx");
    for (i = 0; i < node->numCols; i++) {
        appendStringInfo(str, " %d", node->grpColIdx[i]);
    }

    WRITE_GRPOP_FIELD(grpOperators, numCols);

    WRITE_LONG_FIELD(numGroups);
    WRITE_NODE_FIELD(groupingSets);
    WRITE_NODE_FIELD(chain);
    WRITE_BOOL_FIELD(is_final);
    WRITE_BOOL_FIELD(single_node);
    WRITE_BITMAPSET_FIELD(aggParams);
    out_mem_info(str, &node->mem_info);
    WRITE_BOOL_FIELD(is_sonichash);
    WRITE_BOOL_FIELD(is_dummy);
    WRITE_UINT_FIELD(skew_optimize);
    if (t_thrd.proc->workingVersionNum >= SUBLINKPULLUP_VERSION_NUM) {
        WRITE_BOOL_FIELD(unique_check);
    }
}

static void _outWindowAgg(StringInfo str, WindowAgg* node)
{
    int i;

    WRITE_NODE_TYPE("WINDOWAGG");

    _outPlanInfo(str, (Plan*)node);

    WRITE_UINT_FIELD(winref);
    WRITE_INT_FIELD(partNumCols);

    appendStringInfo(str, " :partColIdx");
    for (i = 0; i < node->partNumCols; i++) {
        appendStringInfo(str, " %d", node->partColIdx[i]);
    }

    WRITE_GRPOP_FIELD(partOperators, partNumCols);

    WRITE_INT_FIELD(ordNumCols);

    appendStringInfo(str, " :ordColIdx");
    for (i = 0; i < node->ordNumCols; i++) {
        appendStringInfo(str, " %d", node->ordColIdx[i]);
    }

    WRITE_GRPOP_FIELD(ordOperators, ordNumCols);
    WRITE_INT_FIELD(frameOptions);
    WRITE_NODE_FIELD(startOffset);
    WRITE_NODE_FIELD(endOffset);
    out_mem_info(str, &node->mem_info);
}

static void _outGroup(StringInfo str, Group* node)
{
    int i;

    WRITE_NODE_TYPE("GROUP");

    _outPlanInfo(str, (Plan*)node);

    WRITE_INT_FIELD(numCols);

    appendStringInfo(str, " :grpColIdx");
    for (i = 0; i < node->numCols; i++) {
        appendStringInfo(str, " %d", node->grpColIdx[i]);
    }

    WRITE_GRPOP_FIELD(grpOperators, numCols);
}

static void _outVecGroup(StringInfo str, VecGroup* node)
{
    int i;

    WRITE_NODE_TYPE("VECGROUP");

    _outPlanInfo(str, (Plan*)node);

    WRITE_INT_FIELD(numCols);

    appendStringInfo(str, " :grpColIdx");
    for (i = 0; i < node->numCols; i++) {
        appendStringInfo(str, " %d", node->grpColIdx[i]);
    }

    appendStringInfo(str, " :grpOperators");
    for (i = 0; i < node->numCols; i++) {
        appendStringInfo(str, " %u", node->grpOperators[i]);
    }
}

static void _outMaterial(StringInfo str, Material* node)
{
    WRITE_NODE_TYPE("MATERIAL");

    _outPlanInfo(str, (Plan*)node);
    WRITE_BOOL_FIELD(materialize_all);
    out_mem_info(str, &node->mem_info);
}

static void _outSimpleSort(StringInfo str, SimpleSort* node)
{
    int i;

    WRITE_NODE_TYPE("SIMPLESORT");

    WRITE_INT_FIELD(numCols);

    appendStringInfo(str, " :sortColIdx");
    for (i = 0; i < node->numCols; i++) {
        appendStringInfo(str, " %d", node->sortColIdx[i]);
    }

    WRITE_GRPOP_FIELD(sortOperators, numCols);

    appendStringInfo(str, " :collations");
    for (i = 0; i < node->numCols; i++) {
        appendStringInfo(str, " %u", node->sortCollations[i]);
    }

    /*
     * We should use collcation name instead of colloid when
     * node->collOid is user-defined, because DN and CN use different OID
     * though the objection name is the same
     * Note that if this function change, you must check function _readSimpleSort()
     */
    for (i = 0; i < node->numCols; i++) {
        if (node->sortCollations[i] >= FirstBootstrapObjectId &&
            IsStatisfyUpdateCompatibility(node->sortCollations[i])) {
            appendStringInfo(str, " :collname ");
            _outToken(str, get_collation_name(node->sortCollations[i]));
        }
    }

    appendStringInfo(str, " :nullsFirst");
    for (i = 0; i < node->numCols; i++) {
        appendStringInfo(str, " %s", booltostr(node->nullsFirst[i]));
    }
    WRITE_BOOL_FIELD(sortToStore);
}

static void _outSort(StringInfo str, Sort* node)
{
    int i;

    WRITE_NODE_TYPE("SORT");

    _outPlanInfo(str, (Plan*)node);

    WRITE_INT_FIELD(numCols);

    appendStringInfo(str, " :sortColIdx");
    for (i = 0; i < node->numCols; i++) {
        appendStringInfo(str, " %d", node->sortColIdx[i]);
    }

    WRITE_GRPOP_FIELD(sortOperators, numCols);

    appendStringInfo(str, " :collations");
    for (i = 0; i < node->numCols; i++) {
        appendStringInfo(str, " %u", node->collations[i]);
    }

    /*
     * We should use collcation name instead of colloid when
     * node->collOid is user-defined, because DN and CN use different OID
     * though the objection name is the same
     * Note that if this function change, you must check function _readSort()
     */
    for (i = 0; i < node->numCols; i++) {
        if (node->collations[i] >= FirstBootstrapObjectId && IsStatisfyUpdateCompatibility(node->collations[i])) {
            appendStringInfo(str, " :collname ");
            _outToken(str, get_collation_name(node->collations[i]));
        }
    }

    appendStringInfo(str, " :nullsFirst");
    for (i = 0; i < node->numCols; i++) {
        appendStringInfo(str, " %s", booltostr(node->nullsFirst[i]));
    }
    out_mem_info(str, &node->mem_info);
}

static void _outUnique(StringInfo str, Unique* node)
{
    int i;

    WRITE_NODE_TYPE("UNIQUE");

    _outPlanInfo(str, (Plan*)node);

    WRITE_INT_FIELD(numCols);

    appendStringInfo(str, " :uniqColIdx");
    for (i = 0; i < node->numCols; i++) {
        appendStringInfo(str, " %d", node->uniqColIdx[i]);
    }

    WRITE_GRPOP_FIELD(uniqOperators, numCols);
}

static void _outVecUnique(StringInfo str, VecUnique* node)
{
    int i;

    WRITE_NODE_TYPE("VECUNIQUE");

    _outPlanInfo(str, (Plan*)node);

    WRITE_INT_FIELD(numCols);

    appendStringInfo(str, " :uniqColIdx");
    for (i = 0; i < node->numCols; i++) {
        appendStringInfo(str, " %d", node->uniqColIdx[i]);
    }

    appendStringInfo(str, " :uniqOperators");
    for (i = 0; i < node->numCols; i++) {
        appendStringInfo(str, " %u", node->uniqOperators[i]);
    }
}

static void _outHash(StringInfo str, Hash* node)
{
    WRITE_NODE_TYPE("HASH");

    _outPlanInfo(str, (Plan*)node);

    WRITE_INT_FIELD(skewColumn);
    WRITE_BOOL_FIELD(skewInherit);
    WRITE_OID_FIELD(skewColType);
    WRITE_INT_FIELD(skewColTypmod);
    WRITE_TYPEINFO_FIELD(skewColType);
}

static void _outSetOp(StringInfo str, SetOp* node)
{
    int i;

    WRITE_NODE_TYPE("SETOP");

    _outPlanInfo(str, (Plan*)node);

    WRITE_ENUM_FIELD(cmd, SetOpCmd);
    WRITE_ENUM_FIELD(strategy, SetOpStrategy);
    WRITE_INT_FIELD(numCols);

    appendStringInfo(str, " :dupColIdx");
    for (i = 0; i < node->numCols; i++) {
        appendStringInfo(str, " %d", node->dupColIdx[i]);
    }

    WRITE_GRPOP_FIELD(dupOperators, numCols);

    WRITE_INT_FIELD(flagColIdx);
    WRITE_INT_FIELD(firstFlag);
    WRITE_LONG_FIELD(numGroups);
}

static void _outVecSetOp(StringInfo str, VecSetOp* node)
{
    int i;

    WRITE_NODE_TYPE("VECSETOP");

    _outPlanInfo(str, (Plan*)node);

    WRITE_ENUM_FIELD(cmd, SetOpCmd);
    WRITE_ENUM_FIELD(strategy, SetOpStrategy);
    WRITE_INT_FIELD(numCols);

    appendStringInfo(str, " :dupColIdx");
    for (i = 0; i < node->numCols; i++) {
        appendStringInfo(str, " %d", node->dupColIdx[i]);
    }

    appendStringInfo(str, " :dupOperators");
    for (i = 0; i < node->numCols; i++) {
        appendStringInfo(str, " %u", node->dupOperators[i]);
    }

    WRITE_INT_FIELD(flagColIdx);
    WRITE_INT_FIELD(firstFlag);
    WRITE_LONG_FIELD(numGroups);
}

static void _outLockRows(StringInfo str, LockRows* node)
{
    WRITE_NODE_TYPE("LOCKROWS");

    _outPlanInfo(str, (Plan*)node);

    WRITE_NODE_FIELD(rowMarks);
    WRITE_INT_FIELD(epqParam);
}

static void _outLimit(StringInfo str, Limit* node)
{
    WRITE_NODE_TYPE("LIMIT");

    _outPlanInfo(str, (Plan*)node);

    WRITE_NODE_FIELD(limitOffset);
    WRITE_NODE_FIELD(limitCount);
}

static void _outNestLoopParam(StringInfo str, NestLoopParam* node)
{
    WRITE_NODE_TYPE("NESTLOOPPARAM");

    WRITE_INT_FIELD(paramno);
    WRITE_NODE_FIELD(paramval);
}

static void _outPlanRowMark(StringInfo str, PlanRowMark* node)
{
    WRITE_NODE_TYPE("PLANROWMARK");

    WRITE_UINT_FIELD(rti);
    WRITE_UINT_FIELD(prti);
    WRITE_UINT_FIELD(rowmarkId);
    WRITE_ENUM_FIELD(markType, RowMarkType);
    WRITE_BOOL_FIELD(noWait);
    if (t_thrd.proc->workingVersionNum >= WAIT_N_TUPLE_LOCK_VERSION_NUM) {
        WRITE_INT_FIELD(waitSec);
    }
    WRITE_BOOL_FIELD(isParent);
    WRITE_INT_FIELD(numAttrs);
    WRITE_BITMAPSET_FIELD(bms_nodeids);
}

static void _outPlanInvalItem(StringInfo str, PlanInvalItem* node)
{
    WRITE_NODE_TYPE("PLANINVALITEM");

    WRITE_INT_FIELD(cacheId);
    WRITE_UINT_FIELD(hashValue);
}

/*****************************************************************************
 *
 *	Stuff from primnodes.h.
 *
 *****************************************************************************/

static void _outAlias(StringInfo str, Alias* node)
{
    WRITE_NODE_TYPE("ALIAS");

    WRITE_STRING_FIELD(aliasname);
    WRITE_NODE_FIELD(colnames);
}

static void _outRangeVar(StringInfo str, RangeVar* node)
{
    WRITE_NODE_TYPE("RANGEVAR");

    /*
     * we deliberately ignore catalogname here, since it is presently not
     * semantically meaningful
     */
    WRITE_STRING_FIELD(schemaname);
    WRITE_STRING_FIELD(relname);
    WRITE_STRING_FIELD(partitionname);
    if (t_thrd.proc->workingVersionNum >= SUBPARTITION_VERSION_NUM) {
        WRITE_STRING_FIELD(subpartitionname);
    }
    WRITE_ENUM_FIELD(inhOpt, InhOption);
    WRITE_CHAR_FIELD(relpersistence);
    WRITE_NODE_FIELD(alias);
    WRITE_LOCATION_FIELD(location);
    WRITE_BOOL_FIELD(ispartition);
    if (t_thrd.proc->workingVersionNum >= SUBPARTITION_VERSION_NUM) {
        WRITE_BOOL_FIELD(issubpartition);
    }
    WRITE_NODE_FIELD(partitionKeyValuesList);
    if (t_thrd.proc->workingVersionNum >= 92063) {
        WRITE_BOOL_FIELD(isbucket);
        WRITE_NODE_FIELD(buckets);
    }
    if (TcapFeatureAvail()) {
        WRITE_BOOL_FIELD(withVerExpr);
    }
}

static void _outIntoClause(StringInfo str, IntoClause* node)
{
    WRITE_NODE_TYPE("INTOCLAUSE");

    WRITE_NODE_FIELD(rel);
    WRITE_NODE_FIELD(colNames);
    WRITE_NODE_FIELD(options);
    WRITE_ENUM_FIELD(onCommit, OnCommitAction);
    WRITE_ENUM_FIELD(row_compress, RelCompressType);
    WRITE_STRING_FIELD(tableSpaceName);
    WRITE_BOOL_FIELD(skipData);
    if (t_thrd.proc->workingVersionNum >= MATVIEW_VERSION_NUM) {
        WRITE_BOOL_FIELD(ivm);
    }
    if (t_thrd.proc->workingVersionNum >= MATVIEW_VERSION_NUM) {
        WRITE_CHAR_FIELD(relkind);
    }
}

static void _outVar(StringInfo str, Var* node)
{
    WRITE_NODE_TYPE("VAR");

    WRITE_UINT_FIELD(varno);
    WRITE_INT_FIELD(varattno);
    WRITE_OID_FIELD(vartype);
    WRITE_INT_FIELD(vartypmod);
    WRITE_OID_FIELD(varcollid);
    WRITE_UINT_FIELD(varlevelsup);
    WRITE_UINT_FIELD(varnoold);
    WRITE_INT_FIELD(varoattno);
    WRITE_LOCATION_FIELD(location);

    /*
     * For the new created type, we have to bind the typname and
     * typnamespace information, so that the data node can decode it.
     */
    WRITE_TYPEINFO_FIELD(vartype);
}

static void _outConst(StringInfo str, Const* node)
{
    WRITE_NODE_TYPE("CONST");

    WRITE_OID_FIELD(consttype);
    WRITE_INT_FIELD(consttypmod);
    WRITE_OID_FIELD(constcollid);
    WRITE_INT_FIELD(constlen);
    WRITE_BOOL_FIELD(constbyval);
    WRITE_BOOL_FIELD(constisnull);
    WRITE_BOOL_FIELD(ismaxvalue);
    WRITE_LOCATION_FIELD(location);

    /*
     * just translate type oid to type name, we must do this just for
     * 1. we skip to translate datatype oid to name when const is
     *   maxvalue before this commit, which may lead to incorrect
     *   type oid if the type is a user-defined
     * 2. we must use type oid to  translate data string to Datum,
     *   we have to decode type name to data type oid first, or it
     *   may be an inncorrect type oid if it is user-defined
     */
    WRITE_TYPEINFO_FIELD(consttype);

    appendStringInfo(str, " :constvalue ");
    if (node->ismaxvalue) {
        /* max value wich is used in sql related to partition */
        appendStringInfo(str, "<MAXVALUE>");
    } else if (node->constisnull) {
        /* null value */
        appendStringInfo(str, "<>");
    } else if (node->consttype < FirstBootstrapObjectId) {
        /*
         * For internal bootstrap type
         *
         * Just print the value of the datum since, since
         * data type information is consistent across all instances
         */
        _outDatum(str, node->constvalue, node->constlen, node->constbyval);
    } else {
        Oid typoutput;
        bool typIsVarlena = false;

        /*
         * For user-define type
         *
         * decode datum to string with Output functions identified by
         * datatype since data type oid may be inconsistent on different
         * nodes. the string will be translated to datum with inputfunction
         * when we read the node string
         */
        appendStringInfo(str, " :udftypevalue ");
        getTypeOutputInfo(node->consttype, &typoutput, &typIsVarlena);
        _outToken(str, OidOutputFunctionCall(typoutput, node->constvalue));
    }

    WRITE_CFGINFO_FIELD(consttype, constvalue);
    WRITE_CURSORDATA_FIELD(cursor_data);
}

static void _outParam(StringInfo str, Param* node)
{
    WRITE_NODE_TYPE("PARAM");

    WRITE_ENUM_FIELD(paramkind, ParamKind);
    WRITE_INT_FIELD(paramid);
    WRITE_OID_FIELD(paramtype);
    WRITE_INT_FIELD(paramtypmod);
    WRITE_OID_FIELD(paramcollid);
    WRITE_LOCATION_FIELD(location);
    WRITE_TYPEINFO_FIELD(paramtype);
    
    if (t_thrd.proc->workingVersionNum >= COMMENT_ROWTYPE_TABLEOF_VERSION_NUM)
    {
        WRITE_OID_FIELD(tableOfIndexType);
    }
    if (t_thrd.proc->workingVersionNum >= COMMENT_RECORD_PARAM_VERSION_NUM)
    {
        WRITE_OID_FIELD(recordVarTypOid);
    }

}

static void _outRownum(StringInfo str, const Rownum* node)
{
    WRITE_NODE_TYPE("ROWNUM");
    WRITE_OID_FIELD(rownumcollid);
    WRITE_LOCATION_FIELD(location);
}

static void _outAggref(StringInfo str, Aggref* node)
{
    WRITE_NODE_TYPE("AGGREF");
    WRITE_OID_FIELD(aggfnoid);
#ifdef ENABLE_MULTIPLE_NODES
    if (node->aggfnoid >= FirstBootstrapObjectId && IsStatisfyUpdateCompatibility(node->aggfnoid) &&
        t_thrd.proc->workingVersionNum >= FUNCNAME_PUSHDOWN_VERSION_NUM) {
        // shipping out for dn, oid will be different on dn, so proname and pronamespace string will be must.
        char* proname = NULL;
        char* pronamespace = NULL;
        char* rettypenamespace = NULL;
        char* rettypename = NULL;
        Oid pronamespaceoid = get_func_namespace(node->aggfnoid);
        Oid rettype = 0;
        int nargs = 0;
        Oid* argtypes = NULL;

        proname = get_func_name(node->aggfnoid);
        pronamespace = get_namespace_name(pronamespaceoid);
        rettype = get_func_signature(node->aggfnoid, &argtypes, &nargs);

        appendStringInfo(str, " :pronamespace ");
        _outToken(str, pronamespace);
        appendStringInfo(str, " :proname ");
        _outToken(str, proname);

        rettypenamespace = get_typenamespace(rettype);
        appendStringInfo(str, " :rettypenamespace ");
        _outToken(str, rettypenamespace);
        rettypename = get_typename(rettype);
        appendStringInfo(str, " :rettypename ");
        _outToken(str, rettypename);

        appendStringInfo(str, " :pronargs %d", nargs);
        appendStringInfo(str, " :proargs ");
        for (int index = 0; index < nargs; index++) {
            char* argtypenamespace = NULL;
            char* argtypename = NULL;
            argtypenamespace = get_typenamespace(argtypes[index]);
            _outToken(str, argtypenamespace);
            appendStringInfo(str, " ");
            argtypename = get_typename(argtypes[index]);
            _outToken(str, argtypename);
        }
    }
#endif
    WRITE_OID_FIELD(aggtype);
#ifdef PGXC
    WRITE_OID_FIELD(aggtrantype);
    WRITE_BOOL_FIELD(agghas_collectfn);
    WRITE_INT_FIELD(aggstage);
#endif /* PGXC */
    WRITE_OID_FIELD(aggcollid);
    WRITE_OID_FIELD(inputcollid);
    WRITE_NODE_FIELD(aggdirectargs);
    WRITE_NODE_FIELD(args);
    WRITE_NODE_FIELD(aggorder);
    WRITE_NODE_FIELD(aggdistinct);
    WRITE_BOOL_FIELD(aggstar);
    WRITE_CHAR_FIELD(aggkind);
    WRITE_BOOL_FIELD(aggvariadic);
    WRITE_UINT_FIELD(agglevelsup);
    WRITE_LOCATION_FIELD(location);

    WRITE_TYPEINFO_FIELD(aggtype);
    WRITE_TYPEINFO_FIELD(aggtrantype);
}

static void _outGroupingFunc(StringInfo str, const GroupingFunc* node)
{
    WRITE_NODE_TYPE("GROUPINGFUNC");

    WRITE_NODE_FIELD(args);
    WRITE_NODE_FIELD(refs);
    WRITE_NODE_FIELD(cols);
    WRITE_UINT_FIELD(agglevelsup);
    WRITE_LOCATION_FIELD(location);
}

static void _outGroupingId(StringInfo str, const GroupingId* node)
{
    WRITE_NODE_TYPE("GROUPINGID");
}

static void _outWindowFunc(StringInfo str, WindowFunc* node)
{
    WRITE_NODE_TYPE("WINDOWFUNC");

    WRITE_OID_FIELD(winfnoid);
    WRITE_OID_FIELD(wintype);
    WRITE_OID_FIELD(wincollid);
    WRITE_OID_FIELD(inputcollid);
    WRITE_NODE_FIELD(args);
    WRITE_UINT_FIELD(winref);
    WRITE_BOOL_FIELD(winstar);
    WRITE_BOOL_FIELD(winagg);
    WRITE_LOCATION_FIELD(location);

    WRITE_TYPEINFO_FIELD(wintype);
    WRITE_FUNCINFO_FIELD(winfnoid);
}

static void _outArrayRef(StringInfo str, ArrayRef* node)
{
    WRITE_NODE_TYPE("ARRAYREF");

    WRITE_OID_FIELD(refarraytype);
    WRITE_OID_FIELD(refelemtype);
    WRITE_INT_FIELD(reftypmod);
    WRITE_OID_FIELD(refcollid);
    WRITE_NODE_FIELD(refupperindexpr);
    WRITE_NODE_FIELD(reflowerindexpr);
    WRITE_NODE_FIELD(refexpr);
    WRITE_NODE_FIELD(refassgnexpr);

    WRITE_TYPEINFO_FIELD(refarraytype);
    WRITE_TYPEINFO_FIELD(refelemtype);
}

/*
 * Brief        : get the object name and namespace by id
 * Input        : objId - Object ID
 *              : contex - Object Kind
 * Output       : objNamespace - object namespace
 *              : objName - object name
 * Return Value : None.
 * Notes        : None.
 */
static void getNameById(Oid objId, const char* context, char** objNamespace, char** objName)
{
    *objName = get_rel_name(objId);

    if ((*objName) == NULL) {
        ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("name for %s %u not found", context, objId)));
    }

    Oid namespaceOid = get_rel_namespace(objId);
    if (!OidIsValid(namespaceOid)) {
        ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("namespace for %s %u not found", context, objId)));
    }

    *objNamespace = get_namespace_name(namespaceOid);

    if ((*objNamespace) == NULL) {
        ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("namespace for %s %u not found", context, objId)));
    }
}

static void _outFuncExpr(StringInfo str, FuncExpr* node)
{
    WRITE_NODE_TYPE("FUNCEXPR");
    WRITE_OID_FIELD(funcid);
    WRITE_OID_FIELD(funcresulttype);
    if (t_thrd.proc->workingVersionNum >= CLIENT_ENCRYPTION_PROC_VERSION_NUM) {
        WRITE_INT_FIELD(funcresulttype_orig);
    }
    WRITE_BOOL_FIELD(funcretset);
    WRITE_ENUM_FIELD(funcformat, CoercionForm);
    WRITE_OID_FIELD(funccollid);
    WRITE_OID_FIELD(inputcollid);
    WRITE_NODE_FIELD(args);
    /*
     * If we decide to push the nextval call to DN, we can not
     * pass the sequence id, because they don't share the same id
     * we need to serialize the namespace and sequence name instead of id
     */
    if (node->funcid == NEXTVALFUNCOID) {
        char* seqName = NULL;
        char* seqNameSpace = NULL;
        Const* firstArg = (Const*)linitial(node->args);

        if (firstArg == NULL) {
            ereport(ERROR, (errcode(ERRCODE_UNEXPECTED_NULL_VALUE), errmsg("seqname of nextval() is not found")));
        }

        if (!IsA(firstArg, Const)) {
            ereport(ERROR, (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                            errmsg("argument of nextval() must be plain const value")));
        }

        Oid seqId = DatumGetObjectId(firstArg->constvalue);

        getNameById(seqId, "SEQUENCE", &seqNameSpace, &seqName);

        appendStringInfo(str, " :seqName ");
        _outToken(str, seqName);
        appendStringInfo(str, " :seqNameSpace ");
        _outToken(str, seqNameSpace);
    }

    WRITE_LOCATION_FIELD(location);
    if (t_thrd.proc->workingVersionNum >= SYNONYM_VERSION_NUM) {
        WRITE_OID_FIELD(refSynOid);
    }

    WRITE_TYPEINFO_FIELD(funcresulttype);
    WRITE_FUNCINFO_FIELD(funcid);
    /*
     * Same reason as above,
     * we need to serialize the namespace and synonym name instead of refSynOid
     * when function name is referenced from one synonym,
     */
    if (t_thrd.proc->workingVersionNum >= SYNONYM_VERSION_NUM) {
        WRITE_SYNINFO_FIELD(refSynOid);
    }
}

static void _outNamedArgExpr(StringInfo str, NamedArgExpr* node)
{
    WRITE_NODE_TYPE("NAMEDARGEXPR");

    WRITE_NODE_FIELD(arg);
    WRITE_STRING_FIELD(name);
    WRITE_INT_FIELD(argnumber);
    WRITE_LOCATION_FIELD(location);
}

template <typename T>
static void _outCommonOpExprPart(StringInfo str, T* node)
{
    WRITE_OID_FIELD(opno);
    WRITE_OPINFO_FEILD(opno);
    WRITE_OID_FIELD(opfuncid);
    WRITE_FUNCINFO_FIELD(opfuncid);
    WRITE_OID_FIELD(opresulttype);
    WRITE_BOOL_FIELD(opretset);
    WRITE_OID_FIELD(opcollid);
    WRITE_OID_FIELD(inputcollid);
    WRITE_NODE_FIELD(args);
    WRITE_LOCATION_FIELD(location);

    WRITE_TYPEINFO_FIELD(opresulttype);
}

static void _outOpExpr(StringInfo str, OpExpr* node)
{
    WRITE_NODE_TYPE("OPEXPR");
    _outCommonOpExprPart<OpExpr>(str, node);
}

static void _outDistinctExpr(StringInfo str, DistinctExpr* node)
{
    WRITE_NODE_TYPE("DISTINCTEXPR");
    _outCommonOpExprPart<DistinctExpr>(str, node);
}

static void _outNullIfExpr(StringInfo str, NullIfExpr* node)
{
    WRITE_NODE_TYPE("NULLIFEXPR");
    _outCommonOpExprPart<NullIfExpr>(str, node);
}

static void _outScalarArrayOpExpr(StringInfo str, ScalarArrayOpExpr* node)
{
    WRITE_NODE_TYPE("SCALARARRAYOPEXPR");

    WRITE_OID_FIELD(opno);
    WRITE_OPINFO_FEILD(opno);
    WRITE_OID_FIELD(opfuncid);
    WRITE_FUNCINFO_FIELD(opfuncid);

    WRITE_BOOL_FIELD(useOr);
    WRITE_OID_FIELD(inputcollid);
    WRITE_NODE_FIELD(args);
    WRITE_LOCATION_FIELD(location);
}

static void _outBoolExpr(StringInfo str, BoolExpr* node)
{
    char* opstr = NULL;

    WRITE_NODE_TYPE("BOOLEXPR");

    /* do-it-yourself enum representation */
    switch (node->boolop) {
        case AND_EXPR:
            opstr = "and";
            break;
        case OR_EXPR:
            opstr = "or";
            break;
        case NOT_EXPR:
            opstr = "not";
            break;
        default:
            break;
    }
    appendStringInfo(str, " :boolop ");
    _outToken(str, opstr);

    WRITE_NODE_FIELD(args);
    WRITE_LOCATION_FIELD(location);
}

static void _outSubLink(StringInfo str, SubLink* node)
{
    WRITE_NODE_TYPE("SUBLINK");

    WRITE_ENUM_FIELD(subLinkType, SubLinkType);
    WRITE_NODE_FIELD(testexpr);
    WRITE_NODE_FIELD(operName);
    WRITE_NODE_FIELD(subselect);
    WRITE_LOCATION_FIELD(location);
}

static void _outSubPlan(StringInfo str, SubPlan* node)
{
    WRITE_NODE_TYPE("SUBPLAN");

    WRITE_ENUM_FIELD(subLinkType, SubLinkType);
    WRITE_NODE_FIELD(testexpr);
    WRITE_NODE_FIELD(paramIds);
    WRITE_INT_FIELD(plan_id);
    WRITE_STRING_FIELD(plan_name);
    WRITE_OID_FIELD(firstColType);
    WRITE_INT_FIELD(firstColTypmod);
    WRITE_OID_FIELD(firstColCollation);
    WRITE_BOOL_FIELD(useHashTable);
    WRITE_BOOL_FIELD(unknownEqFalse);
    WRITE_NODE_FIELD(setParam);
    WRITE_NODE_FIELD(parParam);
    WRITE_NODE_FIELD(args);
    WRITE_FLOAT_FIELD(startup_cost, "%.2f");
    WRITE_FLOAT_FIELD(per_call_cost, "%.2f");

    WRITE_TYPEINFO_FIELD(firstColType);
}

static void _outAlternativeSubPlan(StringInfo str, AlternativeSubPlan* node)
{
    WRITE_NODE_TYPE("ALTERNATIVESUBPLAN");

    WRITE_NODE_FIELD(subplans);
}

static void _outFieldSelect(StringInfo str, FieldSelect* node)
{
    WRITE_NODE_TYPE("FIELDSELECT");

    WRITE_NODE_FIELD(arg);
    WRITE_INT_FIELD(fieldnum);
    WRITE_OID_FIELD(resulttype);
    WRITE_INT_FIELD(resulttypmod);
    WRITE_OID_FIELD(resultcollid);

    WRITE_TYPEINFO_FIELD(resulttype);
}

static void _outFieldStore(StringInfo str, FieldStore* node)
{
    WRITE_NODE_TYPE("FIELDSTORE");

    WRITE_NODE_FIELD(arg);
    WRITE_NODE_FIELD(newvals);
    WRITE_NODE_FIELD(fieldnums);
    WRITE_OID_FIELD(resulttype);

    WRITE_TYPEINFO_FIELD(resulttype);
}

static void _outRelabelType(StringInfo str, RelabelType* node)
{
    WRITE_NODE_TYPE("RELABELTYPE");

    WRITE_NODE_FIELD(arg);
    WRITE_OID_FIELD(resulttype);
    WRITE_INT_FIELD(resulttypmod);
    WRITE_OID_FIELD(resultcollid);
    WRITE_ENUM_FIELD(relabelformat, CoercionForm);
    WRITE_LOCATION_FIELD(location);

    WRITE_TYPEINFO_FIELD(resulttype);
}

static void _outCoerceViaIO(StringInfo str, CoerceViaIO* node)
{
    WRITE_NODE_TYPE("COERCEVIAIO");

    WRITE_NODE_FIELD(arg);
    WRITE_OID_FIELD(resulttype);
    WRITE_OID_FIELD(resultcollid);
    WRITE_ENUM_FIELD(coerceformat, CoercionForm);
    WRITE_LOCATION_FIELD(location);

    WRITE_TYPEINFO_FIELD(resulttype);
}

static void _outArrayCoerceExpr(StringInfo str, ArrayCoerceExpr* node)
{
    WRITE_NODE_TYPE("ARRAYCOERCEEXPR");

    WRITE_NODE_FIELD(arg);
    WRITE_OID_FIELD(elemfuncid);
    WRITE_OID_FIELD(resulttype);
    WRITE_INT_FIELD(resulttypmod);
    WRITE_OID_FIELD(resultcollid);
    WRITE_BOOL_FIELD(isExplicit);
    WRITE_ENUM_FIELD(coerceformat, CoercionForm);
    WRITE_LOCATION_FIELD(location);

    WRITE_TYPEINFO_FIELD(resulttype);
    WRITE_FUNCINFO_FIELD(elemfuncid);
}

static void _outConvertRowtypeExpr(StringInfo str, ConvertRowtypeExpr* node)
{
    WRITE_NODE_TYPE("CONVERTROWTYPEEXPR");

    WRITE_NODE_FIELD(arg);
    WRITE_OID_FIELD(resulttype);
    WRITE_ENUM_FIELD(convertformat, CoercionForm);
    WRITE_LOCATION_FIELD(location);

    WRITE_TYPEINFO_FIELD(resulttype);
}

static void _outCollateExpr(StringInfo str, CollateExpr* node)
{
    WRITE_NODE_TYPE("COLLATE");

    WRITE_NODE_FIELD(arg);
    WRITE_OID_FIELD(collOid);
    WRITE_LOCATION_FIELD(location);

    /*
     * We should use collcation name instead of colloid when
     * node->collOid is user-defined, because DN and CN use
     * different OID, though the objection name is the same
     */
    if (node->collOid > FirstBootstrapObjectId && IsStatisfyUpdateCompatibility(node->collOid)) {
        appendStringInfo(str, " :collname ");
        _outToken(str, get_collation_name(node->collOid));
    }
}

static void _outCaseExpr(StringInfo str, CaseExpr* node)
{
    WRITE_NODE_TYPE("CASE");

    WRITE_OID_FIELD(casetype);
    WRITE_OID_FIELD(casecollid);
    WRITE_NODE_FIELD(arg);
    WRITE_NODE_FIELD(args);
    WRITE_NODE_FIELD(defresult);
    WRITE_LOCATION_FIELD(location);

    WRITE_TYPEINFO_FIELD(casetype);
}

static void _outCaseWhen(StringInfo str, CaseWhen* node)
{
    WRITE_NODE_TYPE("WHEN");

    WRITE_NODE_FIELD(expr);
    WRITE_NODE_FIELD(result);
    WRITE_LOCATION_FIELD(location);
}

static void _outCaseTestExpr(StringInfo str, CaseTestExpr* node)
{
    WRITE_NODE_TYPE("CASETESTEXPR");

    WRITE_OID_FIELD(typeId);
    WRITE_INT_FIELD(typeMod);
    WRITE_OID_FIELD(collation);

    WRITE_TYPEINFO_FIELD(typeId);
}

static void _outArrayExpr(StringInfo str, ArrayExpr* node)
{
    WRITE_NODE_TYPE("ARRAY");

    WRITE_OID_FIELD(array_typeid);
    WRITE_OID_FIELD(array_collid);
    WRITE_OID_FIELD(element_typeid);
    WRITE_NODE_FIELD(elements);
    WRITE_BOOL_FIELD(multidims);
    WRITE_LOCATION_FIELD(location);

    WRITE_TYPEINFO_FIELD(array_typeid);
    WRITE_TYPEINFO_FIELD(element_typeid);
}

static void _outRowExpr(StringInfo str, RowExpr* node)
{
    WRITE_NODE_TYPE("ROW");

    WRITE_NODE_FIELD(args);
    WRITE_OID_FIELD(row_typeid);
    WRITE_ENUM_FIELD(row_format, CoercionForm);
    WRITE_NODE_FIELD(colnames);
    WRITE_LOCATION_FIELD(location);

    WRITE_TYPEINFO_FIELD(row_typeid);
}

static void _outRowCompareExpr(StringInfo str, RowCompareExpr* node)
{
    WRITE_NODE_TYPE("ROWCOMPARE");

    WRITE_ENUM_FIELD(rctype, RowCompareType);
    WRITE_NODE_FIELD(opnos);
    WRITE_NODE_FIELD(opfamilies);
    WRITE_NODE_FIELD(inputcollids);
    WRITE_NODE_FIELD(largs);
    WRITE_NODE_FIELD(rargs);
}

static void _outCoalesceExpr(StringInfo str, CoalesceExpr* node)
{
    WRITE_NODE_TYPE("COALESCE");

    WRITE_OID_FIELD(coalescetype);
    WRITE_OID_FIELD(coalescecollid);
    WRITE_NODE_FIELD(args);
    WRITE_LOCATION_FIELD(location);
    WRITE_BOOL_FIELD(isnvl);

    WRITE_TYPEINFO_FIELD(coalescetype);
}

static void _outMinMaxExpr(StringInfo str, MinMaxExpr* node)
{
    WRITE_NODE_TYPE("MINMAX");

    WRITE_OID_FIELD(minmaxtype);
    WRITE_OID_FIELD(minmaxcollid);
    WRITE_OID_FIELD(inputcollid);
    WRITE_ENUM_FIELD(op, MinMaxOp);
    WRITE_NODE_FIELD(args);
    WRITE_LOCATION_FIELD(location);

    WRITE_TYPEINFO_FIELD(minmaxtype);
}

static void _outXmlExpr(StringInfo str, XmlExpr* node)
{
    WRITE_NODE_TYPE("XMLEXPR");

    WRITE_ENUM_FIELD(op, XmlExprOp);
    WRITE_STRING_FIELD(name);
    WRITE_NODE_FIELD(named_args);
    WRITE_NODE_FIELD(arg_names);
    WRITE_NODE_FIELD(args);
    WRITE_ENUM_FIELD(xmloption, XmlOptionType);
    WRITE_OID_FIELD(type);
    WRITE_INT_FIELD(typmod);
    WRITE_LOCATION_FIELD(location);

    WRITE_TYPEINFO_FIELD(type);
}

static void _outNullTest(StringInfo str, NullTest* node)
{
    WRITE_NODE_TYPE("NULLTEST");

    WRITE_NODE_FIELD(arg);
    WRITE_ENUM_FIELD(nulltesttype, NullTestType);
    WRITE_BOOL_FIELD(argisrow);
}

static void _outHashFilter(StringInfo str, HashFilter* node)
{
    WRITE_NODE_TYPE("HASHFILTER");

    WRITE_NODE_FIELD(arg);
    WRITE_NODE_FIELD(typeOids);
    WRITE_NODE_FIELD(nodeList);

    WRITE_TYPEINFO_LIST(typeOids);
}

static void _outBooleanTest(StringInfo str, BooleanTest* node)
{
    WRITE_NODE_TYPE("BOOLEANTEST");

    WRITE_NODE_FIELD(arg);
    WRITE_ENUM_FIELD(booltesttype, BoolTestType);
}

static void _outCoerceToDomain(StringInfo str, CoerceToDomain* node)
{
    WRITE_NODE_TYPE("COERCETODOMAIN");

    WRITE_NODE_FIELD(arg);
    WRITE_OID_FIELD(resulttype);
    WRITE_INT_FIELD(resulttypmod);
    WRITE_OID_FIELD(resultcollid);
    WRITE_ENUM_FIELD(coercionformat, CoercionForm);
    WRITE_LOCATION_FIELD(location);

    WRITE_TYPEINFO_FIELD(resulttype);
}

static void _outCoerceToDomainValue(StringInfo str, CoerceToDomainValue* node)
{
    WRITE_NODE_TYPE("COERCETODOMAINVALUE");

    WRITE_OID_FIELD(typeId);
    WRITE_INT_FIELD(typeMod);
    WRITE_OID_FIELD(collation);
    WRITE_LOCATION_FIELD(location);

    WRITE_TYPEINFO_FIELD(typeId);
}

static void _outSetToDefault(StringInfo str, SetToDefault* node)
{
    WRITE_NODE_TYPE("SETTODEFAULT");

    WRITE_OID_FIELD(typeId);
    WRITE_INT_FIELD(typeMod);
    WRITE_OID_FIELD(collation);
    WRITE_LOCATION_FIELD(location);

    WRITE_TYPEINFO_FIELD(typeId);
}

static void _outCurrentOfExpr(StringInfo str, CurrentOfExpr* node)
{
    WRITE_NODE_TYPE("CURRENTOFEXPR");

    WRITE_UINT_FIELD(cvarno);
    WRITE_STRING_FIELD(cursor_name);
    WRITE_INT_FIELD(cursor_param);
}

static void _outTargetEntry(StringInfo str, TargetEntry* node)
{
    WRITE_NODE_TYPE("TARGETENTRY");

    WRITE_NODE_FIELD(expr);
    WRITE_INT_FIELD(resno);
    WRITE_STRING_FIELD(resname);
    WRITE_UINT_FIELD(ressortgroupref);
    WRITE_OID_FIELD(resorigtbl);
    WRITE_INT_FIELD(resorigcol);
    WRITE_BOOL_FIELD(resjunk);
}

static void _outPseudoTargetEntry(StringInfo str, PseudoTargetEntry* node)
{
    WRITE_NODE_TYPE("PSEUDOTARGETENTRY");

    WRITE_NODE_FIELD(tle);
    WRITE_NODE_FIELD(srctle);
}

static void _outRangeTblRef(StringInfo str, RangeTblRef* node)
{
    WRITE_NODE_TYPE("RANGETBLREF");

    WRITE_INT_FIELD(rtindex);
}

static void _outJoinExpr(StringInfo str, JoinExpr* node)
{
    WRITE_NODE_TYPE("JOINEXPR");

    WRITE_ENUM_FIELD(jointype, JoinType);
    WRITE_BOOL_FIELD(isNatural);
    WRITE_NODE_FIELD(larg);
    WRITE_NODE_FIELD(rarg);
    WRITE_NODE_FIELD(usingClause);
    WRITE_NODE_FIELD(quals);
    WRITE_NODE_FIELD(alias);
    WRITE_INT_FIELD(rtindex);
}

static void _outFromExpr(StringInfo str, FromExpr* node)
{
    WRITE_NODE_TYPE("FROMEXPR");

    WRITE_NODE_FIELD(fromlist);
    WRITE_NODE_FIELD(quals);
}

static void _outMergeAction(StringInfo str, const MergeAction* node)
{
    WRITE_NODE_TYPE("MERGEACTION");

    WRITE_BOOL_FIELD(matched);
    WRITE_NODE_FIELD(qual);
    WRITE_ENUM_FIELD(commandType, CmdType);
    WRITE_NODE_FIELD(targetList);
    WRITE_NODE_FIELD(pulluped_targetList);
}

/*****************************************************************************
 *
 *	Stuff from relation.h.
 *
 *****************************************************************************/

/*
 * print the basic stuff of all nodes that inherit from Path
 *
 * Note we do NOT print the parent, else we'd be in infinite recursion.
 * We can print the parent's relids for identification purposes, though.
 * We also do not print the whole of param_info, since it's printed by
 * _outRelOptInfo; it's sufficient and less cluttering to print just the
 * required outer relids.
 */
static void _outPathInfo(StringInfo str, Path* node)
{
    WRITE_ENUM_FIELD(pathtype, NodeTag);
    appendStringInfo(str, " :parent_relids ");
    _outBitmapset(str, node->parent->relids);
    appendStringInfo(str, " :required_outer ");
    if (node->param_info) {
        _outBitmapset(str, node->param_info->ppi_req_outer);
    } else {
        _outBitmapset(str, NULL);
    }
    WRITE_FLOAT_FIELD(rows, "%.0f");
    WRITE_FLOAT_FIELD(multiple, "%.0f");
    WRITE_FLOAT_FIELD(startup_cost, "%.2f");
    WRITE_FLOAT_FIELD(total_cost, "%.2f");
    WRITE_NODE_FIELD(pathkeys);
    WRITE_NODE_FIELD(distribute_keys);
    if (node->locator_type != '\0') {
        WRITE_CHAR_FIELD(locator_type);
    }
    WRITE_INT_FIELD(dop);
    _outDistribution(str, &(node->distribution));
    WRITE_INT_FIELD(hint_value);
}
/*
 * print the basic stuff of all nodes that inherit from JoinPath
 */
static void _outJoinPathInfo(StringInfo str, JoinPath* node)
{
    _outPathInfo(str, (Path*)node);

    WRITE_ENUM_FIELD(jointype, JoinType);
    WRITE_NODE_FIELD(outerjoinpath);
    WRITE_NODE_FIELD(innerjoinpath);
    WRITE_NODE_FIELD(joinrestrictinfo);
}

static void _outPath(StringInfo str, Path* node)
{
    WRITE_NODE_TYPE("PATH");

    _outPathInfo(str, (Path*)node);
}

static void _outIndexPath(StringInfo str, IndexPath* node)
{
    WRITE_NODE_TYPE("INDEXPATH");

    _outPathInfo(str, (Path*)node);

    WRITE_NODE_FIELD(indexinfo);
    WRITE_NODE_FIELD(indexclauses);
    WRITE_NODE_FIELD(indexquals);
    WRITE_NODE_FIELD(indexqualcols);
    WRITE_NODE_FIELD(indexorderbys);
    WRITE_NODE_FIELD(indexorderbycols);
    WRITE_ENUM_FIELD(indexscandir, ScanDirection);
    WRITE_FLOAT_FIELD(indextotalcost, "%.2f");
    WRITE_FLOAT_FIELD(indexselectivity, "%.4f");
    if (t_thrd.proc->workingVersionNum >= INPLACE_UPDATE_VERSION_NUM) {
        WRITE_BOOL_FIELD(is_ustore);
    }
}

static void _outBitmapHeapPath(StringInfo str, BitmapHeapPath* node)
{
    WRITE_NODE_TYPE("BITMAPHEAPPATH");

    _outPathInfo(str, (Path*)node);

    WRITE_NODE_FIELD(bitmapqual);
}

static void _outBitmapAndPath(StringInfo str, BitmapAndPath* node)
{
    WRITE_NODE_TYPE("BITMAPANDPATH");

    _outPathInfo(str, (Path*)node);

    WRITE_NODE_FIELD(bitmapquals);
    WRITE_FLOAT_FIELD(bitmapselectivity, "%.4f");
    if (t_thrd.proc->workingVersionNum >= INPLACE_UPDATE_VERSION_NUM) {
        WRITE_BOOL_FIELD(is_ustore);
    }
}

static void _outBitmapOrPath(StringInfo str, BitmapOrPath* node)
{
    WRITE_NODE_TYPE("BITMAPORPATH");

    _outPathInfo(str, (Path*)node);

    WRITE_NODE_FIELD(bitmapquals);
    WRITE_FLOAT_FIELD(bitmapselectivity, "%.4f");
    if (t_thrd.proc->workingVersionNum >= INPLACE_UPDATE_VERSION_NUM) {
        WRITE_BOOL_FIELD(is_ustore);
    }
}

static void _outTidPath(StringInfo str, TidPath* node)
{
    WRITE_NODE_TYPE("TIDPATH");

    _outPathInfo(str, (Path*)node);

    WRITE_NODE_FIELD(tidquals);
}

static void _outPartIteratorPath(StringInfo str, PartIteratorPath* node)
{
    WRITE_NODE_TYPE("PARTITERATORPATH");

    _outPathInfo(str, (Path*)node);

    WRITE_INT_FIELD(itrs);
    WRITE_BOOL_FIELD(direction);
    WRITE_ENUM_FIELD(partType, PartitionType);
    WRITE_NODE_FIELD(subPath);
    WRITE_BOOL_FIELD(ispwj);
    WRITE_NODE_FIELD(upperboundary);
    WRITE_NODE_FIELD(lowerboundary);
}

static void _outForeignPath(StringInfo str, ForeignPath* node)
{
    WRITE_NODE_TYPE("FOREIGNPATH");

    _outPathInfo(str, (Path*)node);

    WRITE_NODE_FIELD(fdw_private);
}

static void _outAppendPath(StringInfo str, AppendPath* node)
{
    WRITE_NODE_TYPE("APPENDPATH");

    _outPathInfo(str, (Path*)node);

    WRITE_NODE_FIELD(subpaths);
}

static void _outMergeAppendPath(StringInfo str, MergeAppendPath* node)
{
    WRITE_NODE_TYPE("MERGEAPPENDPATH");

    _outPathInfo(str, (Path*)node);

    WRITE_NODE_FIELD(subpaths);
    WRITE_FLOAT_FIELD(limit_tuples, "%.0f");
}

/*
 * _outResultPath
 *     output a ResultPath node's information
 *
 * @param (in) str:
 *     the string to store information
 * @param (in) node:
 *     the ResultPath node
 *
 * @return: void
 */
static void _outResultPath(StringInfo str, ResultPath* node)
{
    WRITE_NODE_TYPE("RESULTPATH");

    _outPathInfo(str, (Path*)node);

    WRITE_NODE_FIELD(quals);
    WRITE_NODE_FIELD(subpath);
}

/*
 * _outMaterialPath
 *     output a MaterialPath node's information
 *
 * @param (in) str:
 *     the string to store information
 * @param (in) node:
 *     the MaterialPath node
 *
 * @return: void
 */
static void _outMaterialPath(StringInfo str, MaterialPath* node)
{
    WRITE_NODE_TYPE("MATERIALPATH");

    _outPathInfo(str, (Path*)node);

    WRITE_NODE_FIELD(subpath);
    WRITE_BOOL_FIELD(materialize_all);
}

static void _outUniquePath(StringInfo str, UniquePath* node)
{
    WRITE_NODE_TYPE("UNIQUEPATH");

    _outPathInfo(str, (Path*)node);

    WRITE_NODE_FIELD(subpath);
    WRITE_ENUM_FIELD(umethod, UniquePathMethod);
    WRITE_NODE_FIELD(in_operators);
    WRITE_NODE_FIELD(uniq_exprs);
    WRITE_BOOL_FIELD(both_method);
    WRITE_BOOL_FIELD(hold_tlist);
}

static void _outNestPath(StringInfo str, NestPath* node)
{
    WRITE_NODE_TYPE("NESTPATH");

    _outJoinPathInfo(str, (JoinPath*)node);
}

static void _outMergePath(StringInfo str, MergePath* node)
{
    WRITE_NODE_TYPE("MERGEPATH");

    _outJoinPathInfo(str, (JoinPath*)node);

    WRITE_NODE_FIELD(path_mergeclauses);
    WRITE_NODE_FIELD(outersortkeys);
    WRITE_NODE_FIELD(innersortkeys);
    WRITE_BOOL_FIELD(materialize_inner);
}

static void _outHashPath(StringInfo str, HashPath* node)
{
    WRITE_NODE_TYPE("HASHPATH");

    _outJoinPathInfo(str, (JoinPath*)node);

    WRITE_NODE_FIELD(path_hashclauses);
    WRITE_INT_FIELD(num_batches);
}

static void _outPlannerGlobal(StringInfo str, PlannerGlobal* node)
{
    WRITE_NODE_TYPE("PLANNERGLOBAL");

    /* NB: this isn't a complete set of fields */
    WRITE_NODE_FIELD(paramlist);
    WRITE_NODE_FIELD(subplans);

    WRITE_BITMAPSET_FIELD(rewindPlanIDs);
    WRITE_NODE_FIELD(finalrtable);
    WRITE_NODE_FIELD(finalrowmarks);
    WRITE_NODE_FIELD(resultRelations);
    WRITE_NODE_FIELD(relationOids);
    WRITE_NODE_FIELD(invalItems);
    WRITE_INT_FIELD(nParamExec);
    WRITE_UINT_FIELD(lastPHId);
    WRITE_UINT_FIELD(lastRowMarkId);
    WRITE_BOOL_FIELD(transientPlan);
    WRITE_BOOL_FIELD(dependsOnRole);
}

/*
 * _outPlannerInfo
 *     output a PlannerInfo node's information
 *
 * @param (in) str:
 *     the string to store information
 * @param (in) node:
 *     the PlannerInfo node
 *
 * @return: void
 */
static void _outPlannerInfo(StringInfo str, PlannerInfo* node)
{
    WRITE_NODE_TYPE("PLANNERINFO");

    /* NB: this isn't a complete set of fields */
    WRITE_NODE_FIELD(parse);
    WRITE_NODE_FIELD(glob);
    WRITE_UINT_FIELD(query_level);
    WRITE_NODE_FIELD(plan_params);
    WRITE_ARRAY_FIELD(simple_rel_array, node->simple_rel_array_size);
    WRITE_ARRAY_FIELD(simple_rte_array, node->simple_rel_array_size);
    WRITE_BITMAPSET_FIELD(all_baserels);
    WRITE_NODE_FIELD(join_rel_list);
    if (node->join_cur_level >= 1 && node->join_rel_level != NULL) {
        WRITE_ARRAY_FIELD(join_rel_level, (node->join_cur_level + 1));
    }
    WRITE_INT_FIELD(join_cur_level);
    WRITE_NODE_FIELD(init_plans);
    WRITE_NODE_FIELD(cte_plan_ids);
    WRITE_NODE_FIELD(eq_classes);
    WRITE_NODE_FIELD(canon_pathkeys);
    WRITE_NODE_FIELD(left_join_clauses);
    WRITE_NODE_FIELD(right_join_clauses);
    WRITE_NODE_FIELD(full_join_clauses);
    WRITE_NODE_FIELD(join_info_list);
    WRITE_NODE_FIELD(lateral_info_list);
    WRITE_NODE_FIELD(append_rel_list);
    WRITE_NODE_FIELD(rowMarks);
    WRITE_NODE_FIELD(placeholder_list);
    WRITE_NODE_FIELD(query_pathkeys);
    WRITE_NODE_FIELD(group_pathkeys);
    WRITE_NODE_FIELD(window_pathkeys);
    WRITE_NODE_FIELD(distinct_pathkeys);
    WRITE_NODE_FIELD(sort_pathkeys);
    WRITE_NODE_FIELD(minmax_aggs);
    WRITE_FLOAT_FIELD(total_table_pages, "%.0f");
    WRITE_FLOAT_FIELD(tuple_fraction, "%.4f");
    WRITE_FLOAT_FIELD(limit_tuples, "%.0f");
    WRITE_BOOL_FIELD(hasInheritedTarget);
    WRITE_BOOL_FIELD(hasJoinRTEs);
    WRITE_BOOL_FIELD(hasLateralRTEs);
    WRITE_BOOL_FIELD(hasHavingQual);
    WRITE_BOOL_FIELD(hasPseudoConstantQuals);
    WRITE_BOOL_FIELD(hasRecursion);
#ifdef PGXC
    WRITE_INT_FIELD(rs_alias_index);
    WRITE_NODE_FIELD(xc_rowMarks);
#endif
    WRITE_INT_FIELD(wt_param_id);
    WRITE_BITMAPSET_FIELD(curOuterRels);
    WRITE_NODE_FIELD(curOuterParams);
    WRITE_UINT_FIELD(curIteratorParamIndex);
    if (t_thrd.proc->workingVersionNum >= SUBPARTITION_VERSION_NUM) {
        WRITE_UINT_FIELD(curSubPartIteratorParamIndex);
    }
    WRITE_BOOL_FIELD(isPartIteratorPlanning);
    WRITE_INT_FIELD(curItrs);
    WRITE_NODE_FIELD(subqueryRestrictInfo);
}

static void _outRelOptInfo(StringInfo str, RelOptInfo* node)
{
    WRITE_NODE_TYPE("RELOPTINFO");

    /* NB: this isn't a complete set of fields */
    WRITE_ENUM_FIELD(reloptkind, RelOptKind);
    WRITE_BITMAPSET_FIELD(relids);
    WRITE_BOOL_FIELD(isPartitionedTable);
    WRITE_ENUM_FIELD(partflag, PartitionFlag);
    WRITE_FLOAT_FIELD(rows, "%.0f");
    WRITE_INT_FIELD(width);
    WRITE_NODE_FIELD(reltargetlist);
    WRITE_NODE_FIELD(pathlist);
    WRITE_NODE_FIELD(ppilist);
    WRITE_NODE_FIELD(cheapest_startup_path);
    WRITE_NODE_FIELD(cheapest_total_path);
    WRITE_NODE_FIELD(cheapest_unique_path);
    WRITE_NODE_FIELD(cheapest_parameterized_paths);
    WRITE_UINT_FIELD(relid);
    WRITE_UINT_FIELD(reltablespace);
    WRITE_ENUM_FIELD(rtekind, RTEKind);
    WRITE_INT_FIELD(min_attr);
    WRITE_INT_FIELD(max_attr);
    WRITE_NODE_FIELD(lateral_vars);
    WRITE_BITMAPSET_FIELD(lateral_relids);
    WRITE_NODE_FIELD(indexlist);
    WRITE_FLOAT_FIELD(pages, "%.0f");
    WRITE_FLOAT_FIELD(tuples, "%.0f");
    WRITE_FLOAT_FIELD(multiple, "%.0f");
    WRITE_FLOAT_FIELD(allvisfrac, "%.6f");
    WRITE_NODE_FIELD(pruning_result);
    WRITE_INT_FIELD(partItrs);
    WRITE_NODE_FIELD(subplan);
    WRITE_NODE_FIELD(subroot);
    /* we don't try to print fdwroutine or fdw_private */
    WRITE_NODE_FIELD(baserestrictinfo);
    WRITE_UINT_FIELD(baserestrict_min_security);
    WRITE_NODE_FIELD(joininfo);
    WRITE_BOOL_FIELD(has_eclass_joins);
    WRITE_UINT_FIELD(num_data_nodes);
}

static void _outIndexOptInfo(StringInfo str, IndexOptInfo* node)
{
    WRITE_NODE_TYPE("INDEXOPTINFO");

    /* NB: this isn't a complete set of fields */
    WRITE_OID_FIELD(indexoid);
    /* Do NOT print rel field, else infinite recursion */
    WRITE_BOOL_FIELD(ispartitionedindex);
    WRITE_OID_FIELD(partitionindex);
    WRITE_FLOAT_FIELD(pages, "%.0f");
    WRITE_FLOAT_FIELD(tuples, "%.0f");
    WRITE_INT_FIELD(ncolumns);
    WRITE_OID_FIELD(relam);
    WRITE_NODE_FIELD(indexprs);
    WRITE_NODE_FIELD(indpred);
    WRITE_NODE_FIELD(indextlist);
    WRITE_BOOL_FIELD(predOK);
    WRITE_BOOL_FIELD(unique);
    WRITE_BOOL_FIELD(immediate);
    WRITE_BOOL_FIELD(hypothetical);
}

static void _outEquivalenceClass(StringInfo str, EquivalenceClass* node)
{
    /*
     * To simplify reading, we just chase up to the topmost merged EC and
     * print that, without bothering to show the merge-ees separately.
     */
    while (node->ec_merged) {
        node = node->ec_merged;
    }

    WRITE_NODE_TYPE("EQUIVALENCECLASS");

    WRITE_NODE_FIELD(ec_opfamilies);
    WRITE_OID_FIELD(ec_collation);
    WRITE_NODE_FIELD(ec_members);
    WRITE_NODE_FIELD(ec_sources);
    WRITE_NODE_FIELD(ec_derives);
    WRITE_BITMAPSET_FIELD(ec_relids);
    WRITE_BOOL_FIELD(ec_has_const);
    WRITE_BOOL_FIELD(ec_has_volatile);
    WRITE_BOOL_FIELD(ec_below_outer_join);
    WRITE_BOOL_FIELD(ec_group_set);
    WRITE_BOOL_FIELD(ec_broken);
    WRITE_UINT_FIELD(ec_sortref);
    WRITE_UINT_FIELD(ec_min_security);
    WRITE_UINT_FIELD(ec_max_security);
}

static void _outEquivalenceMember(StringInfo str, EquivalenceMember* node)
{
    WRITE_NODE_TYPE("EQUIVALENCEMEMBER");

    WRITE_NODE_FIELD(em_expr);
    WRITE_BITMAPSET_FIELD(em_relids);
    WRITE_BITMAPSET_FIELD(em_nullable_relids);
    WRITE_BOOL_FIELD(em_is_const);
    WRITE_BOOL_FIELD(em_is_child);
    WRITE_OID_FIELD(em_datatype);

    WRITE_TYPEINFO_FIELD(em_datatype);
}

static void _outPathKey(StringInfo str, PathKey* node)
{
    WRITE_NODE_TYPE("PATHKEY");

    WRITE_NODE_FIELD(pk_eclass);
    WRITE_OID_FIELD(pk_opfamily);
    WRITE_INT_FIELD(pk_strategy);
    WRITE_BOOL_FIELD(pk_nulls_first);
}

static void _outParamPathInfo(StringInfo str, const ParamPathInfo* node)
{
    WRITE_NODE_TYPE("PARAMPATHINFO");

    WRITE_BITMAPSET_FIELD(ppi_req_outer);
    WRITE_FLOAT_FIELD(ppi_rows, "%.0f");
    WRITE_NODE_FIELD(ppi_clauses);
    WRITE_BITMAPSET_FIELD(ppi_req_upper);
}

static void _outRestrictInfo(StringInfo str, const RestrictInfo* node)
{
    WRITE_NODE_TYPE("RESTRICTINFO");

    /* NB: this isn't a complete set of fields */
    WRITE_NODE_FIELD(clause);
    WRITE_BOOL_FIELD(is_pushed_down);
    WRITE_BOOL_FIELD(outerjoin_delayed);
    WRITE_BOOL_FIELD(can_join);
    WRITE_BOOL_FIELD(pseudoconstant);
    WRITE_BOOL_FIELD(leakproof);
    WRITE_UINT_FIELD(security_level);
    WRITE_BITMAPSET_FIELD(clause_relids);
    WRITE_BITMAPSET_FIELD(required_relids);
    WRITE_BITMAPSET_FIELD(outer_relids);
    WRITE_BITMAPSET_FIELD(nullable_relids);
    WRITE_BITMAPSET_FIELD(left_relids);
    WRITE_BITMAPSET_FIELD(right_relids);
    WRITE_NODE_FIELD(orclause);
    /* don't write parent_ec, leads to infinite recursion in plan tree dump */
    WRITE_FLOAT_FIELD(norm_selec, "%.4f");
    WRITE_FLOAT_FIELD(outer_selec, "%.4f");
    WRITE_NODE_FIELD(mergeopfamilies);
    /* don't write left_ec, leads to infinite recursion in plan tree dump */
    /* don't write right_ec, leads to infinite recursion in plan tree dump */
    WRITE_NODE_FIELD(left_em);
    WRITE_NODE_FIELD(right_em);
    WRITE_BOOL_FIELD(outer_is_left);
    WRITE_OID_FIELD(hashjoinoperator);
}

static void _outPlaceHolderVar(StringInfo str, PlaceHolderVar* node)
{
    WRITE_NODE_TYPE("PLACEHOLDERVAR");

    WRITE_NODE_FIELD(phexpr);
    WRITE_BITMAPSET_FIELD(phrels);
    WRITE_UINT_FIELD(phid);
    WRITE_UINT_FIELD(phlevelsup);
}

static void _outSpecialJoinInfo(StringInfo str, SpecialJoinInfo* node)
{
    WRITE_NODE_TYPE("SPECIALJOININFO");

    WRITE_BITMAPSET_FIELD(min_lefthand);
    WRITE_BITMAPSET_FIELD(min_righthand);
    WRITE_BITMAPSET_FIELD(syn_lefthand);
    WRITE_BITMAPSET_FIELD(syn_righthand);
    WRITE_ENUM_FIELD(jointype, JoinType);
    WRITE_BOOL_FIELD(lhs_strict);
    WRITE_BOOL_FIELD(delay_upper_joins);
    WRITE_NODE_FIELD(join_quals);
}

static void
_outLateralJoinInfo(StringInfo str, const LateralJoinInfo *node)
{
   WRITE_NODE_TYPE("LATERALJOININFO");

   WRITE_UINT_FIELD(lateral_rhs);
   WRITE_BITMAPSET_FIELD(lateral_lhs);
}

static void _outAppendRelInfo(StringInfo str, AppendRelInfo* node)
{
    WRITE_NODE_TYPE("APPENDRELINFO");

    WRITE_UINT_FIELD(parent_relid);
    WRITE_UINT_FIELD(child_relid);
    WRITE_OID_FIELD(parent_reltype);
    WRITE_OID_FIELD(child_reltype);
    WRITE_NODE_FIELD(translated_vars);
    WRITE_OID_FIELD(parent_reloid);

    WRITE_TYPEINFO_FIELD(parent_reltype);
    WRITE_TYPEINFO_FIELD(child_reltype);
}

static void _outPlaceHolderInfo(StringInfo str, PlaceHolderInfo* node)
{
    WRITE_NODE_TYPE("PLACEHOLDERINFO");

    WRITE_UINT_FIELD(phid);
    WRITE_NODE_FIELD(ph_var);
    WRITE_BITMAPSET_FIELD(ph_eval_at);
    WRITE_BITMAPSET_FIELD(ph_needed);
    WRITE_INT_FIELD(ph_width);
}

static void _outMinMaxAggInfo(StringInfo str, MinMaxAggInfo* node)
{
    WRITE_NODE_TYPE("MINMAXAGGINFO");

    WRITE_OID_FIELD(aggfnoid);
    WRITE_OID_FIELD(aggsortop);
    WRITE_NODE_FIELD(target);
    /* We intentionally omit subroot --- too large, not interesting enough */
    WRITE_NODE_FIELD(path);
    WRITE_FLOAT_FIELD(pathcost, "%.2f");
    WRITE_NODE_FIELD(param);
}

static void _outPlannerParamItem(StringInfo str, PlannerParamItem* node)
{
    WRITE_NODE_TYPE("PLANNERPARAMITEM");

    WRITE_NODE_FIELD(item);
    WRITE_INT_FIELD(paramId);
}

/*****************************************************************************
 *
 *	Stuff from parsenodes.h.
 *
 *****************************************************************************/

/*
 * print the basic stuff of all nodes that inherit from CreateStmt
 */
static void _outCreateStmtInfo(StringInfo str, const CreateStmt* node)
{
    WRITE_NODE_FIELD(relation);
    WRITE_NODE_FIELD(tableElts);
    WRITE_NODE_FIELD(inhRelations);
    WRITE_NODE_FIELD(ofTypename);
    WRITE_NODE_FIELD(constraints);
    WRITE_NODE_FIELD(options);
    WRITE_NODE_FIELD(clusterKeys);
    WRITE_ENUM_FIELD(oncommit, OnCommitAction);
    WRITE_STRING_FIELD(tablespacename);
    WRITE_BOOL_FIELD(if_not_exists);
    if (t_thrd.proc->workingVersionNum >= MATVIEW_VERSION_NUM) {
        WRITE_BOOL_FIELD(ivm);
    }
    WRITE_NODE_FIELD(partTableState);
    WRITE_NODE_FIELD(uuids);
    if (t_thrd.proc->workingVersionNum >= MATVIEW_VERSION_NUM) {
        WRITE_CHAR_FIELD(relkind);
    }
}

static void _outRangePartitionDefState(StringInfo str, RangePartitionDefState* node)
{
    WRITE_NODE_TYPE("RANGEPARTITIONDEFSTATE");

    WRITE_STRING_FIELD(partitionName);
    WRITE_NODE_FIELD(boundary);
    WRITE_STRING_FIELD(tablespacename);
}

static void _outListPartitionDefState(StringInfo str, ListPartitionDefState* node)
{
    WRITE_NODE_TYPE("LISTPARTITIONDEFSTATE");

    WRITE_STRING_FIELD(partitionName);
    WRITE_NODE_FIELD(boundary);
    WRITE_STRING_FIELD(tablespacename);
}

static void _outHashPartitionDefState(StringInfo str, HashPartitionDefState* node)
{
    WRITE_NODE_TYPE("HASHPARTITIONDEFSTATE");

    WRITE_STRING_FIELD(partitionName);
    WRITE_NODE_FIELD(boundary);
    WRITE_STRING_FIELD(tablespacename);
}

static void _outIntervalPartitionDefState(StringInfo str, IntervalPartitionDefState* node)
{
    WRITE_NODE_TYPE("INTERVALPARTITIONDEFSTATE");

    WRITE_NODE_FIELD(partInterval);
    WRITE_NODE_FIELD(intervalTablespaces);
}

static void _outPartitionState(StringInfo str, PartitionState* node)
{
    WRITE_NODE_TYPE("PARTITIONSTATE");

    if (node->partitionStrategy == 0) {
        appendStringInfo(str, " :partitionStrategy 0");
    } else {
        WRITE_CHAR_FIELD(partitionStrategy);
    }

    WRITE_NODE_FIELD(intervalPartDef);
    WRITE_NODE_FIELD(partitionKey);
    WRITE_NODE_FIELD(partitionList);
    WRITE_ENUM_FIELD(rowMovement, RowMovementValue);
    WRITE_NODE_FIELD(subPartitionState);
    WRITE_NODE_FIELD(partitionNameList);
}

static void _outRangePartitionindexDefState(StringInfo str, RangePartitionindexDefState* node)
{
    WRITE_NODE_TYPE("RANGEPARTITIONINDEXDEFSTATE");

    WRITE_STRING_FIELD(name);
    WRITE_STRING_FIELD(tablespace);
    WRITE_NODE_FIELD(sublist);
}

static void _outRangePartitionStartEndDefState(StringInfo str, RangePartitionStartEndDefState* node)
{
    WRITE_NODE_TYPE("RANGEPARTITIONSTARTENDDEFSTATE");

    WRITE_STRING_FIELD(partitionName);
    WRITE_NODE_FIELD(startValue);
    WRITE_NODE_FIELD(endValue);
    WRITE_NODE_FIELD(everyValue);
    WRITE_STRING_FIELD(tableSpaceName);
}

static void _outAddPartitionState(StringInfo str, AddPartitionState* node)
{
    WRITE_NODE_TYPE("ADDPARTITIONSTATE");

    WRITE_NODE_FIELD(partitionList);
    WRITE_BOOL_FIELD(isStartEnd);
}

static void _outAddSubPartitionState(StringInfo str, const AddSubPartitionState* node)
{
    WRITE_NODE_TYPE("ADDSUBPARTITIONSTATE");

    WRITE_STRING_FIELD(partitionName);
    WRITE_NODE_FIELD(subPartitionList);
}


static void _outCreateStmt(StringInfo str, const CreateStmt* node)
{
    WRITE_NODE_TYPE("CREATESTMT");

    _outCreateStmtInfo(str, (const CreateStmt*)node);
}

static void _outCreateForeignTableStmt(StringInfo str, const CreateForeignTableStmt* node)
{
    WRITE_NODE_TYPE("CREATEFOREIGNTABLESTMT");

    _outCreateStmtInfo(str, (const CreateStmt*)node);

    WRITE_STRING_FIELD(servername);
    WRITE_NODE_FIELD(options);
    WRITE_BOOL_FIELD(write_only);
    WRITE_NODE_FIELD(part_state);
}

static void _outIndexStmt(StringInfo str, IndexStmt* node)
{
    WRITE_NODE_TYPE("INDEXSTMT");

    WRITE_STRING_FIELD(schemaname);
    WRITE_STRING_FIELD(idxname);
    WRITE_NODE_FIELD(relation);
    WRITE_STRING_FIELD(accessMethod);
    WRITE_STRING_FIELD(tableSpace);
    WRITE_NODE_FIELD(indexParams);
    if (t_thrd.proc->workingVersionNum >= SUPPORT_GPI_VERSION_NUM) {
        WRITE_NODE_FIELD(indexIncludingParams);
        WRITE_BOOL_FIELD(isGlobal);
    }
    WRITE_NODE_FIELD(options);
    WRITE_NODE_FIELD(whereClause);
    WRITE_NODE_FIELD(excludeOpNames);
    WRITE_STRING_FIELD(idxcomment);
    WRITE_OID_FIELD(indexOid);
    WRITE_OID_FIELD(oldNode);
    WRITE_NODE_FIELD(partClause);
    WRITE_BOOL_FIELD(isPartitioned);
    WRITE_BOOL_FIELD(unique);
    WRITE_BOOL_FIELD(primary);
    WRITE_BOOL_FIELD(isconstraint);
    WRITE_BOOL_FIELD(deferrable);
    WRITE_BOOL_FIELD(initdeferred);
    WRITE_BOOL_FIELD(concurrent);
    WRITE_NODE_FIELD(inforConstraint);
}

static void _outNotifyStmt(StringInfo str, NotifyStmt* node)
{
    WRITE_NODE_TYPE("NOTIFY");

    WRITE_STRING_FIELD(conditionname);
    WRITE_STRING_FIELD(payload);
}

static void _outAlterTableStmt(StringInfo str, AlterTableStmt* node)
{
    WRITE_NODE_TYPE("ALTERTABLE");
    WRITE_NODE_FIELD(relation);
}

static void _outCopyStmt(StringInfo str, CopyStmt* node)
{
    WRITE_NODE_TYPE("COPY");
    WRITE_NODE_FIELD(relation);
}

static void _outDeclareCursorStmt(StringInfo str, DeclareCursorStmt* node)
{
    WRITE_NODE_TYPE("DECLARECURSOR");

    WRITE_STRING_FIELD(portalname);
    WRITE_INT_FIELD(options);
    WRITE_NODE_FIELD(query);
}

static void _outMergeStmt(StringInfo str, MergeStmt* node)
{
    WRITE_NODE_TYPE("MERGEINTO");

    WRITE_NODE_FIELD(relation);
    WRITE_NODE_FIELD(source_relation);
    WRITE_NODE_FIELD(join_condition);
    WRITE_NODE_FIELD(mergeWhenClauses);
    WRITE_BOOL_FIELD(is_insert_update);
    WRITE_NODE_FIELD(insert_stmt);
    WRITE_NODE_FIELD(hintState);
}

static void _outInsertStmt(StringInfo str, InsertStmt* node)
{
    WRITE_NODE_TYPE("INSERT");

    WRITE_NODE_FIELD(relation);
    WRITE_NODE_FIELD(cols);
    WRITE_NODE_FIELD(selectStmt);
    WRITE_NODE_FIELD(returningList);
    WRITE_NODE_FIELD(withClause);
#ifdef ENABLE_MULTIPLE_NODES
    if (t_thrd.proc->workingVersionNum >= UPSERT_ROW_STORE_VERSION_NUM) {
        WRITE_NODE_FIELD(upsertClause);
    }
#else
	WRITE_NODE_FIELD(upsertClause);
#endif	
    WRITE_NODE_FIELD(hintState);
}

static void _outUpdateStmt(StringInfo str, UpdateStmt* node)
{
    WRITE_NODE_TYPE("UPDATE");

    WRITE_NODE_FIELD(relation);
    WRITE_NODE_FIELD(targetList);
    WRITE_NODE_FIELD(whereClause);
    WRITE_NODE_FIELD(fromClause);
    WRITE_NODE_FIELD(returningList);
    WRITE_NODE_FIELD(withClause);
    WRITE_NODE_FIELD(hintState);
}

static void _outSelectStmt(StringInfo str, SelectStmt* node)
{
    WRITE_NODE_TYPE("SELECT");

    WRITE_NODE_FIELD(distinctClause);
    WRITE_NODE_FIELD(intoClause);
    WRITE_NODE_FIELD(targetList);
    WRITE_NODE_FIELD(fromClause);
    if (t_thrd.proc->workingVersionNum >= SWCB_VERSION_NUM) {
        WRITE_NODE_FIELD(startWithClause);
    }
    WRITE_NODE_FIELD(whereClause);
    WRITE_NODE_FIELD(groupClause);
    WRITE_NODE_FIELD(havingClause);
    WRITE_NODE_FIELD(windowClause);
    WRITE_NODE_FIELD(withClause);
    WRITE_NODE_FIELD(valuesLists);
    WRITE_NODE_FIELD(sortClause);
    WRITE_NODE_FIELD(limitOffset);
    WRITE_NODE_FIELD(limitCount);
    WRITE_NODE_FIELD(lockingClause);
    WRITE_ENUM_FIELD(op, SetOperation);
    WRITE_BOOL_FIELD(all);
    WRITE_NODE_FIELD(larg);
    WRITE_NODE_FIELD(rarg);
    WRITE_BOOL_FIELD(hasPlus);
    WRITE_NODE_FIELD(hintState);
}

static void _outFuncCall(StringInfo str, FuncCall* node)
{
    WRITE_NODE_TYPE("FUNCCALL");

    WRITE_NODE_FIELD(funcname);
    WRITE_STRING_FIELD(colname);
    WRITE_NODE_FIELD(args);
    WRITE_NODE_FIELD(agg_order);
    WRITE_BOOL_FIELD(agg_within_group);
    WRITE_BOOL_FIELD(agg_star);
    WRITE_BOOL_FIELD(agg_distinct);
    WRITE_BOOL_FIELD(func_variadic);
    WRITE_NODE_FIELD(over);
    WRITE_LOCATION_FIELD(location);
    WRITE_BOOL_FIELD(call_func);
}

static void _outTableLikeClause(StringInfo str, const TableLikeClause* node)
{
    WRITE_NODE_TYPE("TABLELIKECLAUSE");

    WRITE_NODE_FIELD(relation);
    WRITE_UINT_FIELD(options);
}

static void _outLockingClause(StringInfo str, LockingClause* node)
{
    WRITE_NODE_TYPE("LOCKINGCLAUSE");

    WRITE_NODE_FIELD(lockedRels);
    WRITE_BOOL_FIELD(forUpdate);
    WRITE_BOOL_FIELD(noWait);
    if (t_thrd.proc->workingVersionNum >= ENHANCED_TUPLE_LOCK_VERSION_NUM) {
        WRITE_ENUM_FIELD(strength, LockClauseStrength);
    }
    if (t_thrd.proc->workingVersionNum >= WAIT_N_TUPLE_LOCK_VERSION_NUM) {
        WRITE_BOOL_FIELD(waitSec);
    }
}

static void _outXmlSerialize(StringInfo str, XmlSerialize* node)
{
    WRITE_NODE_TYPE("XMLSERIALIZE");

    WRITE_ENUM_FIELD(xmloption, XmlOptionType);
    WRITE_NODE_FIELD(expr);
    WRITE_NODE_FIELD(typname);
    WRITE_LOCATION_FIELD(location);
}

static void _outColumnDef(StringInfo str, ColumnDef* node)
{
    WRITE_NODE_TYPE("COLUMNDEF");

    WRITE_STRING_FIELD(colname);
    WRITE_NODE_FIELD(typname);
    WRITE_INT_FIELD(kvtype);
    WRITE_INT_FIELD(inhcount);
    WRITE_BOOL_FIELD(is_local);
    WRITE_BOOL_FIELD(is_not_null);
    WRITE_BOOL_FIELD(is_from_type);
    WRITE_BOOL_FIELD(is_serial);

    if (node->storage == 0) {
        appendStringInfo(str, " :storage 0");
    } else {
        WRITE_CHAR_FIELD(storage);
    }

    /* because the ENUM type is not used, so void is passed in */
    WRITE_ENUM_FIELD(cmprs_mode, void);
    WRITE_NODE_FIELD(raw_default);
    WRITE_NODE_FIELD(cooked_default);
    WRITE_NODE_FIELD(collClause);
    WRITE_OID_FIELD(collOid);
    WRITE_NODE_FIELD(constraints);
    WRITE_NODE_FIELD(fdwoptions);
    WRITE_NODE_FIELD(clientLogicColumnRef);
    if (t_thrd.proc->workingVersionNum >= GENERATED_COL_VERSION_NUM) {
        if (node->generatedCol)
            WRITE_CHAR_FIELD(generatedCol);
    }
}

static void _outTypeName(StringInfo str, TypeName* node)
{
    WRITE_NODE_TYPE("TYPENAME");

    WRITE_NODE_FIELD(names);
    WRITE_OID_FIELD(typeOid);
    WRITE_BOOL_FIELD(setof);
    WRITE_BOOL_FIELD(pct_type);
    WRITE_NODE_FIELD(typmods);
    WRITE_INT_FIELD(typemod);
    WRITE_NODE_FIELD(arrayBounds);
    WRITE_LOCATION_FIELD(location);
    if (t_thrd.proc->workingVersionNum >= COMMENT_ROWTYPE_TABLEOF_VERSION_NUM)
    {
        WRITE_BOOL_FIELD(pct_rowtype);
    }
    if (t_thrd.proc->workingVersionNum >= COMMENT_PCT_TYPE_VERSION_NUM)
    {
        WRITE_LOCATION_FIELD(end_location);
    }

    WRITE_TYPEINFO_FIELD(typeOid);
}

static void _outTypeCast(StringInfo str, TypeCast* node)
{
    WRITE_NODE_TYPE("TYPECAST");

    WRITE_NODE_FIELD(arg);
    WRITE_NODE_FIELD(typname);
    WRITE_LOCATION_FIELD(location);
}

static void _outCollateClause(StringInfo str, CollateClause* node)
{
    WRITE_NODE_TYPE("COLLATECLAUSE");

    WRITE_NODE_FIELD(arg);
    WRITE_NODE_FIELD(collname);
    WRITE_LOCATION_FIELD(location);
}

static void _outColumnParam (StringInfo str, ClientLogicColumnParam* node)
{
    WRITE_NODE_TYPE("COLUMNPARAM");
    WRITE_ENUM_FIELD(key, ClientLogicColumnProperty);
    WRITE_STRING_FIELD(value);
    WRITE_UINT_FIELD(len);
    WRITE_LOCATION_FIELD(location);
}
static void _outGlobalParam (StringInfo str, ClientLogicGlobalParam* node)
{
    WRITE_NODE_TYPE("GLOBALPARAM");
    WRITE_ENUM_FIELD(key, ClientLogicGlobalProperty);
    WRITE_STRING_FIELD(value);
    WRITE_UINT_FIELD(len);
    WRITE_LOCATION_FIELD(location);
}
static void _outGlobalSetting (StringInfo str, CreateClientLogicGlobal* node)
{
    WRITE_NODE_TYPE("GLOBALSETTING");
   WRITE_NODE_FIELD(global_key_name);
   WRITE_NODE_FIELD(global_setting_params);
}
static void _outColumnSetting (StringInfo str, CreateClientLogicColumn* node)
{
    WRITE_NODE_TYPE("COLUMNSETTING");
    WRITE_NODE_FIELD(column_key_name);
    WRITE_NODE_FIELD(column_setting_params);
}


static void
_outClientLogicColumnRef(StringInfo str, ClientLogicColumnRef *node)
{
    WRITE_NODE_TYPE("CLIENTLOGICCOLUMNREF");
    WRITE_NODE_FIELD(column_key_name);
    WRITE_NODE_FIELD(orig_typname);
    WRITE_NODE_FIELD(dest_typname);
    WRITE_LOCATION_FIELD(location);
}

static void _outIndexElem(StringInfo str, IndexElem* node)
{
    WRITE_NODE_TYPE("INDEXELEM");

    WRITE_STRING_FIELD(name);
    WRITE_NODE_FIELD(expr);
    WRITE_STRING_FIELD(indexcolname);
    WRITE_NODE_FIELD(collation);
    WRITE_NODE_FIELD(opclass);
    WRITE_ENUM_FIELD(ordering, SortByDir);
    WRITE_ENUM_FIELD(nulls_ordering, SortByNulls);
}
static void _outDefElem(StringInfo str, DefElem* node)
{
    WRITE_NODE_TYPE("DEFELEM");

    WRITE_STRING_FIELD(defnamespace);
    WRITE_STRING_FIELD(defname);
    WRITE_NODE_FIELD(arg);
    WRITE_ENUM_FIELD(defaction, DefElemAction);

    if (t_thrd.proc->workingVersionNum >= COPY_TRANSFORM_VERSION_NUM) {
        WRITE_INT_FIELD(begin_location);
        WRITE_INT_FIELD(end_location);
    }
}

static void _outPLDebug_variable(StringInfo str, PLDebug_variable* node)
{
    WRITE_NODE_TYPE("PLDEBUG_VARIABLE");
    WRITE_STRING_FIELD(name);
    WRITE_STRING_FIELD(var_type);
    WRITE_STRING_FIELD(value);
    WRITE_STRING_FIELD(pkgname);
    WRITE_BOOL_FIELD(isconst);
}

static void _outPLDebug_breakPoint(StringInfo str, PLDebug_breakPoint* node)
{
    WRITE_NODE_TYPE("PLDEBUG_BREAKPOINT");
    WRITE_INT_FIELD(bpIndex);
    WRITE_OID_FIELD(funcoid);
    WRITE_INT_FIELD(lineno);
    WRITE_STRING_FIELD(query);
    WRITE_BOOL_FIELD(active);
}

static void _outPLDebug_frame(StringInfo str, PLDebug_frame* node)
{
    WRITE_NODE_TYPE("PLDEBUG_FRAME");
    WRITE_INT_FIELD(frameno);
    WRITE_STRING_FIELD(funcname);
    WRITE_INT_FIELD(lineno);
    WRITE_STRING_FIELD(query);
    WRITE_INT_FIELD(funcoid);
}

/*
 * @Description: Write hint node to string.
 * @out str: String buf.
 * @in node: Hint struct.
 */
static void _outBaseHint(StringInfo str, Hint* node)
{
    WRITE_NODE_FIELD(relnames);
    WRITE_ENUM_FIELD(hint_keyword, HintKeyword);
    WRITE_ENUM_FIELD(state, HintStatus);
}

/*
 * @Description: No GPC hint node to string.
 * @out str: String buf.
 * @in node: No GPC hint struct.
 */
static void _outNoGPCHint(StringInfo str, const NoGPCHint* node)
{
    WRITE_NODE_TYPE("NOGPCHINT");
    _outBaseHint(str, (Hint*)node);
}

/*
 * @Description: No Expand hint node to string.
 * @out str: String buf.
 * @in node: No Expand hint struct.
 */
static void _outNoExpandHint(StringInfo str, const NoExpandHint* node)
{
    WRITE_NODE_TYPE("NOEXPANDHINT");
    _outBaseHint(str, (Hint*)node);
}

/*
 * @Description: Set hint node to string.
 * @out str: String buf.
 * @in node: Set hint struct.
 */
static void _outSetHint(StringInfo str, const SetHint* node)
{
    WRITE_NODE_TYPE("SETHINT");
    _outBaseHint(str, (Hint*)node);
    WRITE_STRING_FIELD(name);
    WRITE_STRING_FIELD(value);
}

/*
 * @Description: Plancache hint node to string.
 * @out str: String buf.
 * @in node: Plancache hint struct.
 */
static void _outPlanCacheHint(StringInfo str, const PlanCacheHint* node)
{
    WRITE_NODE_TYPE("PLANCACHEHINT");
    _outBaseHint(str, (Hint*)node);
    WRITE_BOOL_FIELD(chooseCustomPlan);
}

/*
 * @Description: Leading hint node to string.
 * @out str: String buf.
 * @in node: Leading hint struct.
 */
static void _outLeadingHint(StringInfo str, LeadingHint* node)
{
    WRITE_NODE_TYPE("LEADINGHINT");
    _outBaseHint(str, (Hint*)node);
    WRITE_BOOL_FIELD(join_order_hint);
}

/*
 * @Description: Predpush hint node to string.
 * @out str: String buf.
 * @in node: Predpush hint struct.
 */
static void _outPredpushHint(StringInfo str, PredpushHint* node)
{
    WRITE_NODE_TYPE("PREDPUSHHINT");
    _outBaseHint(str, (Hint*)node);
    WRITE_BOOL_FIELD(negative);
    WRITE_STRING_FIELD(dest_name);
    WRITE_INT_FIELD(dest_id);
    WRITE_BITMAPSET_FIELD(candidates);
}

/*
 * @Description: Predpush same level hint node to string.
 * @out str: String buf.
 * @in node: Predpush same level hint struct.
 */
static void _outPredpushSameLevelHint(StringInfo str, PredpushSameLevelHint* node)
{
    WRITE_NODE_TYPE("PREDPUSHSAMELEVELHINT");
    _outBaseHint(str, (Hint*)node);
    WRITE_BOOL_FIELD(negative);
    WRITE_STRING_FIELD(dest_name);
    WRITE_INT_FIELD(dest_id);
    WRITE_BITMAPSET_FIELD(candidates);
}

/*
 * @Description: Rewrite hint node to string.
 * @out str: String buf.
 * @in node: Rewrite hint struct.
 */
static void _outRewriteHint(StringInfo str, RewriteHint* node)
{
    WRITE_NODE_TYPE("REWRITEHINT");
    _outBaseHint(str, (Hint*)node);
    WRITE_NODE_FIELD(param_names);
    WRITE_UINT_FIELD(param_bits);
}

/*
 * @Description: Gather hint node to string.
 * @out str: String buf.
 * @in node: Gather hint struct.
 */
static void _outGatherHint(StringInfo str, GatherHint* node)
{
    WRITE_NODE_TYPE("GATHERHINT");
    _outBaseHint(str, (Hint*)node);
    WRITE_ENUM_FIELD(source, GatherSource);
}

/*
 * @Description: Write join hint node to string.
 * @out str: String buf.
 * @in node: Join hint struct.
 */
static void _outJoinHint(StringInfo str, JoinMethodHint* node)
{
    WRITE_NODE_TYPE("JOINHINT");

    _outBaseHint(str, (Hint*)node);
    WRITE_BOOL_FIELD(negative);
    WRITE_BITMAPSET_FIELD(joinrelids);
    WRITE_BITMAPSET_FIELD(inner_joinrelids);
}

/*
 * @Description: Write rows hint node to string.
 * @out str: String buf.
 * @in node: Rows hint struct.
 */
static void _outRowsHint(StringInfo str, RowsHint* node)
{
    WRITE_NODE_TYPE("ROWSHINT");

    _outBaseHint(str, (Hint*)node);
    WRITE_BITMAPSET_FIELD(joinrelids);
    WRITE_STRING_FIELD(rows_str);
    WRITE_ENUM_FIELD(value_type, RowsValueType);
    WRITE_FLOAT_FIELD(rows, "%f");
}

/*
 * @Description: Write stream hint node to string.
 * @out str: String buf.
 * @in node: Stream hint struct.
 */
static void _outStreamHint(StringInfo str, StreamHint* node)
{
    WRITE_NODE_TYPE("STREAMHINT");
    _outBaseHint(str, (Hint*)node);
    WRITE_BOOL_FIELD(negative);
    WRITE_BITMAPSET_FIELD(joinrelids);
    WRITE_ENUM_FIELD(stream_type, StreamType);
}

/*
 * @Description: Write BlockName hint node to string.
 * @out str: String buf.
 * @in node: BlockName hint struct.
 */
static void _outBlockNameHint(StringInfo str, BlockNameHint* node)
{
    WRITE_NODE_TYPE("BLOCKNAMEHINT");
    _outBaseHint(str, (Hint*)node);
}

/*
 * @Description: Write scan hint node to string.
 * @out str: String buf.
 * @in node: Scan hint struct.
 */
static void _outScanMethodHint(StringInfo str, ScanMethodHint* node)
{
    WRITE_NODE_TYPE("SCANHINT");

    _outBaseHint(str, (Hint*)node);
    WRITE_BOOL_FIELD(negative);
    WRITE_BITMAPSET_FIELD(relid);
    WRITE_NODE_FIELD(indexlist);
}

/*
 * @Description: Write gc_fdw remote information node to string.
 * @out str: String buf.
 * @in node: gc_fdw remote informantion.
 */
static void _outPgFdwRemoteInfo(StringInfo str, PgFdwRemoteInfo* node)
{
    WRITE_NODE_TYPE("PGFDWREMOTEINFO");

    WRITE_CHAR_FIELD(reltype);
    WRITE_INT_FIELD(datanodenum);
    WRITE_FLOAT_FIELD(snapsize, "%lu");

    /* Write the field name only one time and just append the value of each field */
    appendStringInfo(str, " :snapshot");

    Assert(node->snapsize % 2 == 0);
    _outUint16Array(str, (uint16*)node->snapshot, node->snapsize / 2);
}

/*
 * @Description: Write skew hint node to string.
 * @out str: String buf.
 * @in node: Skew hint struct.
 */
static void _outSkewHint(StringInfo str, SkewHint* node)
{
    WRITE_NODE_TYPE("SKEWHINT");

    _outBaseHint(str, (Hint*)node);
    WRITE_BITMAPSET_FIELD(relid);
    WRITE_NODE_FIELD(column_list);
    WRITE_NODE_FIELD(value_list);
}

/*
 * @Description: Write skew hint info node to string.
 * @out str: String buf.
 * @in node: SkewRelInfo struct.
 */
static void _outSkewRelInfo(StringInfo str, SkewRelInfo* node)
{
    WRITE_NODE_TYPE("SKEWRELINFO");

    WRITE_STRING_FIELD(relation_name);
    WRITE_OID_FIELD(relation_oid);
    WRITE_NODE_FIELD(rte);
    WRITE_NODE_FIELD(parent_rte);
}

/*
 * @Description: Write skew hint node to string.
 * @out str: String buf.
 * @in node: SkewColumnInfo struct.
 */
static void _outSkewColumnInfo(StringInfo str, SkewColumnInfo* node)
{
    WRITE_NODE_TYPE("SKEWCOLUMNINFO");

    WRITE_OID_FIELD(relation_Oid);
    WRITE_STRING_FIELD(column_name);
    WRITE_UINT_FIELD(attnum);
    WRITE_OID_FIELD(column_typid);
    WRITE_NODE_FIELD(expr);

    WRITE_TYPEINFO_FIELD(column_typid);
}

/*
 * @Description: Write skew hint node to string.
 * @out str: String buf.
 * @in node: SkewValueInfo struct.
 */
static void _outSkewValueInfo(StringInfo str, SkewValueInfo* node)
{
    WRITE_NODE_TYPE("SKEWVALUEINFO");

    WRITE_BOOL_FIELD(support_redis);
    WRITE_NODE_FIELD(const_value);
}

/*
 * @Description: Write skew hint node to string.
 * @out str: String buf.
 * @in node: Skew hint struct.
 */
static void _outSkewHintTransf(StringInfo str, SkewHintTransf* node)
{
    WRITE_NODE_TYPE("SKEWHINTTRANSF");

    WRITE_NODE_FIELD(before);
    WRITE_NODE_FIELD(rel_info_list);
    WRITE_NODE_FIELD(column_info_list);
    WRITE_NODE_FIELD(value_info_list);
}

/*
 * @Description: Write join hint state node to string.
 * @out str: String buf.
 * @in node: Hint state struct.
 */
static void _outHintState(StringInfo str, HintState* node)
{
    WRITE_NODE_TYPE("HINTSTATE");

    WRITE_INT_FIELD(nall_hints);
    WRITE_NODE_FIELD(join_hint);
    WRITE_NODE_FIELD(leading_hint);
    WRITE_NODE_FIELD(row_hint);
    WRITE_NODE_FIELD(stream_hint);
    WRITE_NODE_FIELD(block_name_hint);
    WRITE_NODE_FIELD(scan_hint);
    WRITE_NODE_FIELD(skew_hint);
    if (t_thrd.proc->workingVersionNum >= PREDPUSH_VERSION_NUM) {
        WRITE_NODE_FIELD(predpush_hint);
    }
    if (t_thrd.proc->workingVersionNum >= EXECUTE_DIRECT_ON_MULTI_VERSION_NUM) {
        WRITE_NODE_FIELD(rewrite_hint);
    }
    if (t_thrd.proc->workingVersionNum >= HINT_ENHANCEMENT_VERSION_NUM) {
        WRITE_NODE_FIELD(gather_hint);
        WRITE_NODE_FIELD(no_expand_hint);
        WRITE_NODE_FIELD(set_hint);
        WRITE_NODE_FIELD(cache_plan_hint);
        WRITE_NODE_FIELD(no_gpc_hint);
    }
    if (t_thrd.proc->workingVersionNum >= PREDPUSH_SAME_LEVEL_VERSION_NUM) {
        WRITE_NODE_FIELD(predpush_same_level_hint);
    }
}

static void _outQuery(StringInfo str, Query* node)
{
    WRITE_NODE_TYPE("QUERY");

    WRITE_ENUM_FIELD(commandType, CmdType);
    WRITE_ENUM_FIELD(querySource, QuerySource);
    /* we intentionally do not print the queryId field */
    WRITE_BOOL_FIELD(canSetTag);

    /*
     * Hack to work around missing outfuncs routines for a lot of the
     * utility-statement node types.  (The only one we actually *need* for
     * rules support is NotifyStmt.)  Someday we ought to support 'em all, but
     * for the meantime do this to avoid getting lots of warnings when running
     * with debug_print_parse on.
     */
    if (node->utilityStmt) {
        switch (nodeTag(node->utilityStmt)) {
            case T_CreateStmt:
            case T_IndexStmt:
            case T_NotifyStmt:
            case T_DeclareCursorStmt:
            case T_CopyStmt:
            case T_AlterTableStmt:
                WRITE_NODE_FIELD(utilityStmt);
                break;
            default:
                appendStringInfo(str, " :utilityStmt ?");
                break;
        }
    } else {
        appendStringInfo(str, " :utilityStmt <>");
    }

    WRITE_INT_FIELD(resultRelation);
    WRITE_BOOL_FIELD(hasAggs);
    WRITE_BOOL_FIELD(hasWindowFuncs);
    WRITE_BOOL_FIELD(hasSubLinks);
    WRITE_BOOL_FIELD(hasDistinctOn);
    WRITE_BOOL_FIELD(hasRecursive);
    WRITE_BOOL_FIELD(hasModifyingCTE);
    WRITE_BOOL_FIELD(hasForUpdate);
    WRITE_BOOL_FIELD(hasRowSecurity);
    if (t_thrd.proc->workingVersionNum >= SYNONYM_VERSION_NUM) {
        WRITE_BOOL_FIELD(hasSynonyms);
    }
    WRITE_NODE_FIELD(cteList);
    WRITE_NODE_FIELD(rtable);
    WRITE_NODE_FIELD(jointree);
    WRITE_NODE_FIELD(targetList);
    WRITE_NODE_FIELD(starStart);
    WRITE_NODE_FIELD(starEnd);
    WRITE_NODE_FIELD(starOnly);
    WRITE_NODE_FIELD(returningList);
    WRITE_NODE_FIELD(groupClause);
    WRITE_NODE_FIELD(groupingSets);
    WRITE_NODE_FIELD(havingQual);
    WRITE_NODE_FIELD(windowClause);
    WRITE_NODE_FIELD(distinctClause);
    WRITE_NODE_FIELD(sortClause);
    WRITE_NODE_FIELD(limitOffset);
    WRITE_NODE_FIELD(limitCount);
    WRITE_NODE_FIELD(rowMarks);
    WRITE_NODE_FIELD(setOperations);
    WRITE_NODE_FIELD(constraintDeps);
    WRITE_NODE_FIELD(hintState);
#ifdef PGXC
    WRITE_STRING_FIELD(sql_statement);
    WRITE_BOOL_FIELD(is_local);
    WRITE_BOOL_FIELD(has_to_save_cmd_id);
    WRITE_BOOL_FIELD(vec_output);
    if (t_thrd.proc->workingVersionNum >= TRUNCAST_VERSION_NUM) {
        WRITE_ENUM_FIELD(tdTruncCastStatus, TdTruncCastStatus);
    } else {
        appendStringInfo(str, " :" CppAsString(isTruncationCastAdded) " %s", booltostr(node->tdTruncCastStatus == NOT_CAST_BECAUSEOF_GUC));
    }
    WRITE_NODE_FIELD(equalVars);
#endif
    WRITE_INT_FIELD(mergeTarget_relation);
    WRITE_NODE_FIELD(mergeSourceTargetList);
    WRITE_NODE_FIELD(mergeActionList);
#ifdef ENABLE_MULTIPLE_NODES	
    if (t_thrd.proc->workingVersionNum >= UPSERT_TO_MERGE_VERSION_NUM) {
        WRITE_NODE_FIELD(upsertQuery);
    }
    if (t_thrd.proc->workingVersionNum >= UPSERT_ROW_STORE_VERSION_NUM) {
        WRITE_NODE_FIELD(upsertClause);
    }
#else
        WRITE_NODE_FIELD(upsertQuery);
        WRITE_NODE_FIELD(upsertClause);
#endif	
    WRITE_BOOL_FIELD(isRowTriggerShippable);
    WRITE_BOOL_FIELD(use_star_targets);
    WRITE_BOOL_FIELD(is_from_full_join_rewrite);
    if (t_thrd.proc->workingVersionNum >= PARTIALPUSH_VERSION_NUM) {
        WRITE_BOOL_FIELD(can_push);
    }
    if (t_thrd.proc->workingVersionNum >= SUBLINKPULLUP_VERSION_NUM) {
        WRITE_BOOL_FIELD(unique_check);
    }
}

static void _outSortGroupClause(StringInfo str, SortGroupClause* node)
{
    WRITE_NODE_TYPE("SORTGROUPCLAUSE");

    WRITE_UINT_FIELD(tleSortGroupRef);
    WRITE_OID_FIELD(eqop);
    WRITE_OPINFO_FEILD(eqop);
    WRITE_OID_FIELD(sortop);
    WRITE_OPINFO_FEILD(sortop);

    WRITE_BOOL_FIELD(nulls_first);
    WRITE_BOOL_FIELD(hashable);
    WRITE_BOOL_FIELD(groupSet);
}

static void _outGroupingSet(StringInfo str, const GroupingSet* node)
{
    WRITE_NODE_TYPE("GROUPINGSET");

    WRITE_ENUM_FIELD(kind, GroupingSetKind);
    WRITE_NODE_FIELD(content);
    WRITE_LOCATION_FIELD(location);
}

static void _outWindowClause(StringInfo str, WindowClause* node)
{
    WRITE_NODE_TYPE("WINDOWCLAUSE");

    WRITE_STRING_FIELD(name);
    WRITE_STRING_FIELD(refname);
    WRITE_NODE_FIELD(partitionClause);
    WRITE_NODE_FIELD(orderClause);
    WRITE_INT_FIELD(frameOptions);
    WRITE_NODE_FIELD(startOffset);
    WRITE_NODE_FIELD(endOffset);
    WRITE_UINT_FIELD(winref);
    WRITE_BOOL_FIELD(copiedOrder);
}

static void _outRowMarkClause(StringInfo str, RowMarkClause* node)
{
    WRITE_NODE_TYPE("ROWMARKCLAUSE");

    WRITE_UINT_FIELD(rti);
    WRITE_BOOL_FIELD(forUpdate);
    WRITE_BOOL_FIELD(noWait);
    if (t_thrd.proc->workingVersionNum >= WAIT_N_TUPLE_LOCK_VERSION_NUM) {
        WRITE_INT_FIELD(waitSec);
    }
    WRITE_BOOL_FIELD(pushedDown);
    if (t_thrd.proc->workingVersionNum >= ENHANCED_TUPLE_LOCK_VERSION_NUM) {
        WRITE_ENUM_FIELD(strength, LockClauseStrength);
    }
}

static void _outWithClause(StringInfo str, WithClause* node)
{
    WRITE_NODE_TYPE("WITHCLAUSE");

    WRITE_NODE_FIELD(ctes);
    WRITE_BOOL_FIELD(recursive);
    WRITE_LOCATION_FIELD(location);
    if (t_thrd.proc->workingVersionNum >= SWCB_VERSION_NUM) {
        WRITE_NODE_FIELD(sw_clause);
    }
}

static void _outStartWithClause(StringInfo str, StartWithClause* node)
{
    WRITE_NODE_TYPE("STARTWITHCLAUSE");

    WRITE_NODE_FIELD(startWithExpr);
    WRITE_NODE_FIELD(connectByExpr);
    WRITE_NODE_FIELD(siblingsOrderBy);

    WRITE_BOOL_FIELD(priorDirection);
    WRITE_BOOL_FIELD(nocycle);
    WRITE_BOOL_FIELD(opt);
}

static void _outCommonTableExpr(StringInfo str, CommonTableExpr* node)
{
    WRITE_NODE_TYPE("COMMONTABLEEXPR");

    WRITE_STRING_FIELD(ctename);
    WRITE_NODE_FIELD(aliascolnames);
    if (t_thrd.proc->workingVersionNum >= MATERIALIZED_CTE_NUM) {
        WRITE_ENUM_FIELD(ctematerialized, CTEMaterialize);
    }
    WRITE_NODE_FIELD(ctequery);
    WRITE_LOCATION_FIELD(location);
    WRITE_BOOL_FIELD(cterecursive);
    WRITE_INT_FIELD(cterefcount);
    WRITE_NODE_FIELD(ctecolnames);
    WRITE_NODE_FIELD(ctecoltypes);
    WRITE_NODE_FIELD(ctecoltypmods);
    WRITE_NODE_FIELD(ctecolcollations);
    WRITE_TYPEINFO_LIST(ctecoltypes);
    /* Not write locator_type in accordance with old version. */
    if (t_thrd.proc->workingVersionNum >= MATERIALIZED_CTE_NUM) {
        WRITE_BOOL_FIELD(self_reference);
    }
    if (t_thrd.proc->workingVersionNum >= SWCB_VERSION_NUM) {
        WRITE_NODE_FIELD(swoptions);
    }
    if (t_thrd.proc->workingVersionNum >= DEFAULT_MAT_CTE_NUM) {
        WRITE_BOOL_FIELD(referenced_by_subquery);
    }
}

static void _outStartWithTargetRelInfo(StringInfo str, StartWithTargetRelInfo* node)
{
    WRITE_NODE_TYPE("STARTWITHTARGETRELINFO");

    WRITE_STRING_FIELD(relname);
    WRITE_STRING_FIELD(aliasname);
    WRITE_STRING_FIELD(ctename);
    WRITE_NODE_FIELD(columns);
    WRITE_NODE_FIELD(tblstmt);

    WRITE_ENUM_FIELD(rtekind, RTEKind);
    WRITE_NODE_FIELD(rte);
    WRITE_NODE_FIELD(rtr);
}

static void _outSetOperationStmt(StringInfo str, SetOperationStmt* node)
{
    WRITE_NODE_TYPE("SETOPERATIONSTMT");

    WRITE_ENUM_FIELD(op, SetOperation);
    WRITE_BOOL_FIELD(all);
    WRITE_NODE_FIELD(larg);
    WRITE_NODE_FIELD(rarg);
    WRITE_NODE_FIELD(colTypes);
    WRITE_NODE_FIELD(colTypmods);
    WRITE_NODE_FIELD(colCollations);
    WRITE_NODE_FIELD(groupClauses);

    WRITE_TYPEINFO_LIST(colTypes);
}

static void _outRteRelation(StringInfo str, const RangeTblEntry *node)
{
    WRITE_OID_FIELD(relid);
    WRITE_CHAR_FIELD(relkind);
    if (t_thrd.proc->workingVersionNum >= MATVIEW_VERSION_NUM) {
        WRITE_BOOL_FIELD(isResultRel);
    }
    WRITE_NODE_FIELD(tablesample);
    if (TcapFeatureAvail()) {
        WRITE_NODE_FIELD(timecapsule);
    }
    WRITE_OID_FIELD(partitionOid);
    WRITE_BOOL_FIELD(isContainPartition);
    if (t_thrd.proc->workingVersionNum >= SUBPARTITION_VERSION_NUM) {
        WRITE_OID_FIELD(subpartitionOid);
        WRITE_BOOL_FIELD(isContainSubPartition);
    }
    if (t_thrd.proc->workingVersionNum >= SYNONYM_VERSION_NUM) {
        WRITE_OID_FIELD(refSynOid);
    }
    WRITE_BOOL_FIELD(ispartrel);
    WRITE_BOOL_FIELD(ignoreResetRelid);
    WRITE_NODE_FIELD(pname);
    WRITE_NODE_FIELD(partid_list);
    WRITE_NODE_FIELD(plist);
    if (node->relid >= FirstBootstrapObjectId && IsStatisfyUpdateCompatibility(node->relid) &&
        node->mainRelName == NULL) {
        /*
         * For inherit table, the relname will be different
         */
        // shipping out for dn, relid will be different on dn, so relname and relnamespace string will be must.

        char *rteRelname = NULL;
        char *rteRelnamespace = NULL;

        getNameById(node->relid, "RTE", &rteRelnamespace, &rteRelname);

        appendStringInfo(str, " :relname ");
        _outToken(str, rteRelname);
        appendStringInfo(str, " :relnamespace ");
        _outToken(str, rteRelnamespace);

        /*
         * Same reason as above,
         * we need to serialize the namespace and synonym name instead of refSynOid
         * when relation name is referenced from one synonym,
         */
        if (t_thrd.proc->workingVersionNum >= SYNONYM_VERSION_NUM) {
            WRITE_SYNINFO_FIELD(refSynOid);
        }
    }
}

static void _outRangeTblEntry(StringInfo str, RangeTblEntry* node)
{
    WRITE_NODE_TYPE("RTE");

    /* put alias + eref first to make dump more legible */
    WRITE_NODE_FIELD(alias);
    WRITE_NODE_FIELD(eref);
    WRITE_ENUM_FIELD(rtekind, RTEKind);
#ifdef PGXC
    WRITE_STRING_FIELD(relname);
    WRITE_NODE_FIELD(partAttrNum);
#endif

    if (node->mainRelName != NULL) {
        WRITE_STRING_FIELD(mainRelName);
        WRITE_STRING_FIELD(mainRelNameSpace);
    }

    switch (node->rtekind) {
        case RTE_RELATION:
            _outRteRelation(str, node);
            break;
        case RTE_SUBQUERY:
            WRITE_NODE_FIELD(subquery);
            WRITE_BOOL_FIELD(security_barrier);
            break;
        case RTE_JOIN:
            WRITE_ENUM_FIELD(jointype, JoinType);
            WRITE_NODE_FIELD(joinaliasvars);
            break;
        case RTE_FUNCTION:
            WRITE_NODE_FIELD(funcexpr);
            WRITE_NODE_FIELD(funccoltypes);
            WRITE_NODE_FIELD(funccoltypmods);
            WRITE_NODE_FIELD(funccolcollations);

            WRITE_TYPEINFO_LIST(funccoltypes);
            break;
        case RTE_VALUES:
            WRITE_NODE_FIELD(values_lists);
            WRITE_NODE_FIELD(values_collations);
            break;
        case RTE_CTE:
            WRITE_STRING_FIELD(ctename);
            WRITE_UINT_FIELD(ctelevelsup);
            WRITE_BOOL_FIELD(self_reference);
            if (t_thrd.proc->workingVersionNum >= MATERIALIZED_CTE_NUM) {
                WRITE_BOOL_FIELD(cterecursive);
            }
            WRITE_NODE_FIELD(ctecoltypes);
            WRITE_NODE_FIELD(ctecoltypmods);
            WRITE_NODE_FIELD(ctecolcollations);

            if (t_thrd.proc->workingVersionNum >= SWCB_VERSION_NUM) {
                WRITE_BOOL_FIELD(swConverted);
                WRITE_NODE_FIELD(origin_index);
                WRITE_BOOL_FIELD(swAborted);
                WRITE_BOOL_FIELD(swSubExist);
            }

            WRITE_TYPEINFO_LIST(ctecoltypes);
            break;
#ifdef PGXC
        case RTE_REMOTE_DUMMY:
            /* Everything relevant already copied */
            break;
#endif /* PGXC */
        case RTE_RESULT:
            /* no extra fields */
            break;
        default:
            ereport(ERROR,
                (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE), errmsg("unrecognized RTE kind: %d", (int)node->rtekind)));
            break;
    }

    if (t_thrd.proc->workingVersionNum >= PREDPUSH_VERSION_NUM) {
        WRITE_BOOL_FIELD(lateral);
    }
    WRITE_BOOL_FIELD(inh);
    WRITE_BOOL_FIELD(inFromCl);
    WRITE_UINT_FIELD(requiredPerms);
    WRITE_OID_FIELD(checkAsUser);
    WRITE_BITMAPSET_FIELD(selectedCols);
    WRITE_BITMAPSET_FIELD(modifiedCols);
    WRITE_BITMAPSET_FIELD(insertedCols);
    WRITE_BITMAPSET_FIELD(updatedCols);
    WRITE_ENUM_FIELD(orientation, RelOrientation);
    WRITE_NODE_FIELD(securityQuals);
    WRITE_BOOL_FIELD(subquery_pull_up);
    WRITE_BOOL_FIELD(correlated_with_recursive_cte);
    if (t_thrd.proc->workingVersionNum >= 92063) {
        WRITE_BOOL_FIELD(relhasbucket);
        WRITE_BOOL_FIELD(isbucket);
        if (t_thrd.proc->workingVersionNum >= SEGMENT_PAGE_VERSION_NUM) {
            WRITE_INT_FIELD(bucketmapsize);
        }
        WRITE_NODE_FIELD(buckets);
    }

    if (t_thrd.proc->workingVersionNum >= UPSERT_ROW_STORE_VERSION_NUM) {
        WRITE_BOOL_FIELD(isexcluded);
    }
    if (t_thrd.proc->workingVersionNum >= SUBLINKPULLUP_VERSION_NUM) {
        WRITE_BOOL_FIELD(sublink_pull_up);
    }

    if (t_thrd.proc->workingVersionNum >= INPLACE_UPDATE_VERSION_NUM) {
        WRITE_BOOL_FIELD(is_ustore);
    }

    if (t_thrd.proc->workingVersionNum >= GENERATED_COL_VERSION_NUM) {
        WRITE_BITMAPSET_FIELD(extraUpdatedCols);
    }
}

/*
 * Description: Write TableSampleClause struct to string.
 *
 * Parameters:
 *	@in node: tableSampleClause node.
 * 	@out str: tableSampleClause string.
 *
 * Return: void
 */
static void _outTableSampleClause(StringInfo str, const TableSampleClause* node)
{
    WRITE_NODE_TYPE("TABLESAMPLECLAUSE");

    WRITE_ENUM_FIELD(sampleType, TableSampleType);
    WRITE_NODE_FIELD(args);
    WRITE_NODE_FIELD(repeatable);
}

/*
 * Description: Write TimeCapsuleClause struct to string.
 *
 * Parameters: @in node: TimeCapsuleClause node.
 *             @out str: TimeCapsuleClause string.
 *
 * Return: void
 */
static void OutTimeCapsuleClause(StringInfo str, const TimeCapsuleClause* node)
{
    WRITE_NODE_TYPE("TIMECAPSULECLAUSE");

    WRITE_ENUM_FIELD(tvtype, TvVersionType);
    WRITE_NODE_FIELD(tvver);
}

static void _outAExpr(StringInfo str, A_Expr* node)
{
    WRITE_NODE_TYPE("AEXPR");

    switch (node->kind) {
        case AEXPR_OP:
            appendStringInfo(str, " ");
            WRITE_NODE_FIELD(name);
            break;
        case AEXPR_AND:
            appendStringInfo(str, " AND");
            break;
        case AEXPR_OR:
            appendStringInfo(str, " OR");
            break;
        case AEXPR_NOT:
            appendStringInfo(str, " NOT");
            break;
        case AEXPR_OP_ANY:
            appendStringInfo(str, " ");
            WRITE_NODE_FIELD(name);
            appendStringInfo(str, " ANY ");
            break;
        case AEXPR_OP_ALL:
            appendStringInfo(str, " ");
            WRITE_NODE_FIELD(name);
            appendStringInfo(str, " ALL ");
            break;
        case AEXPR_DISTINCT:
            appendStringInfo(str, " DISTINCT ");
            WRITE_NODE_FIELD(name);
            break;
        case AEXPR_NULLIF:
            appendStringInfo(str, " NULLIF ");
            WRITE_NODE_FIELD(name);
            break;
        case AEXPR_OF:
            appendStringInfo(str, " OF ");
            WRITE_NODE_FIELD(name);
            break;
        case AEXPR_IN:
            appendStringInfo(str, " IN ");
            WRITE_NODE_FIELD(name);
            break;
        default:
            appendStringInfo(str, " ??");
            break;
    }

    WRITE_NODE_FIELD(lexpr);
    WRITE_NODE_FIELD(rexpr);
    WRITE_LOCATION_FIELD(location);
}

static void _outValue(StringInfo str, Value* value)
{
    switch (value->type) {
        case T_Integer:
            appendStringInfo(str, "%ld", value->val.ival);
            break;
        case T_Float:

            /*
             * We assume the value is a valid numeric literal and so does not
             * need quoting.
             */
            appendStringInfoString(str, value->val.str);
            break;
        case T_String:
            appendStringInfoChar(str, '"');
            _outToken(str, value->val.str);
            appendStringInfoChar(str, '"');
            break;
        case T_BitString:
            /* internal representation already has leading 'b' */
            appendStringInfoString(str, value->val.str);
            break;
        case T_Null:
            /* this is seen only within A_Const, not in transformed trees */
            appendStringInfoString(str, "NULL");
            break;
        default:
            ereport(ERROR,
                (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE), errmsg("unrecognized node type: %d", (int)value->type)));
            break;
    }
}

static void _outColumnRef(StringInfo str, ColumnRef* node)
{
    WRITE_NODE_TYPE("COLUMNREF");

    WRITE_NODE_FIELD(fields);
    if (t_thrd.proc->workingVersionNum >= SWCB_VERSION_NUM) {
        WRITE_BOOL_FIELD(prior);
    }
    if (t_thrd.proc->workingVersionNum >= FUNC_PARAM_COL_VERSION_NUM) {
        WRITE_INT_FIELD(indnum);
    }
    WRITE_LOCATION_FIELD(location);
}

static void _outParamRef(StringInfo str, ParamRef* node)
{
    WRITE_NODE_TYPE("PARAMREF");

    WRITE_INT_FIELD(number);
    WRITE_LOCATION_FIELD(location);
}

static void _outAConst(StringInfo str, A_Const* node)
{
    WRITE_NODE_TYPE("A_CONST");

    appendStringInfo(str, " :val ");
    _outValue(str, &(node->val));
    WRITE_LOCATION_FIELD(location);
}

static void _outA_Star(StringInfo str, A_Star* node)
{
    WRITE_NODE_TYPE("A_STAR");
}

static void _outA_Indices(StringInfo str, A_Indices* node)
{
    WRITE_NODE_TYPE("A_INDICES");

    WRITE_NODE_FIELD(lidx);
    WRITE_NODE_FIELD(uidx);
}

static void _outA_Indirection(StringInfo str, A_Indirection* node)
{
    WRITE_NODE_TYPE("A_INDIRECTION");

    WRITE_NODE_FIELD(arg);
    WRITE_NODE_FIELD(indirection);
}

static void _outA_ArrayExpr(StringInfo str, A_ArrayExpr* node)
{
    WRITE_NODE_TYPE("A_ARRAYEXPR");

    WRITE_NODE_FIELD(elements);
    WRITE_LOCATION_FIELD(location);
}

static void _outResTarget(StringInfo str, ResTarget* node)
{
    WRITE_NODE_TYPE("RESTARGET");

    WRITE_STRING_FIELD(name);
    WRITE_NODE_FIELD(indirection);
    WRITE_NODE_FIELD(val);
    WRITE_LOCATION_FIELD(location);
}

static void _outSortBy(StringInfo str, SortBy* node)
{
    WRITE_NODE_TYPE("SORTBY");

    WRITE_NODE_FIELD(node);
    WRITE_ENUM_FIELD(sortby_dir, SortByDir);
    WRITE_ENUM_FIELD(sortby_nulls, SortByNulls);
    WRITE_NODE_FIELD(useOp);
    WRITE_LOCATION_FIELD(location);
}

static void _outWindowDef(StringInfo str, WindowDef* node)
{
    WRITE_NODE_TYPE("WINDOWDEF");

    WRITE_STRING_FIELD(name);
    WRITE_STRING_FIELD(refname);
    WRITE_NODE_FIELD(partitionClause);
    WRITE_NODE_FIELD(orderClause);
    WRITE_INT_FIELD(frameOptions);
    WRITE_NODE_FIELD(startOffset);
    WRITE_NODE_FIELD(endOffset);
    WRITE_LOCATION_FIELD(location);
}

static void _outRangeSubselect(StringInfo str, RangeSubselect* node)
{
    WRITE_NODE_TYPE("RANGESUBSELECT");

    WRITE_BOOL_FIELD(lateral);
    WRITE_NODE_FIELD(subquery);
    WRITE_NODE_FIELD(alias);
}

static void _outRangeFunction(StringInfo str, RangeFunction* node)
{
    WRITE_NODE_TYPE("RANGEFUNCTION");

    WRITE_BOOL_FIELD(lateral);
    WRITE_NODE_FIELD(funccallnode);
    WRITE_NODE_FIELD(alias);
    WRITE_NODE_FIELD(coldeflist);
}

/*
 * Description: Write node RangeTableSample to string.
 *
 * Parameters: @in node: RangeTableSample node.
 *             @out str: RangeTableSample string.
 *
 * Return: void
 */
static void _outRangeTableSample(StringInfo str, RangeTableSample* node)
{
    WRITE_NODE_TYPE("RANGETABLESAMPLE");

    WRITE_NODE_FIELD(relation);
    WRITE_NODE_FIELD(method);
    WRITE_NODE_FIELD(args);
    WRITE_NODE_FIELD(repeatable);
    WRITE_LOCATION_FIELD(location);
}

/*
 * Description: Write node RangeTimeCapsule to string.
 *
 * Parameters:
 *	@in node: RangeTimeCapsule node.
 * 	@out str: RangeTimeCapsule string.
 *
 * Return: void
 */
static void OutRangeTimeCapsule(StringInfo str, RangeTimeCapsule* node)
{
    WRITE_NODE_TYPE("RANGETIMECAPSULE");

    WRITE_NODE_FIELD(relation);
    WRITE_ENUM_FIELD(tvtype, TvVersionType);
    WRITE_NODE_FIELD(tvver);
    WRITE_LOCATION_FIELD(location);
}

static void _outConstraint(StringInfo str, Constraint* node)
{
    WRITE_NODE_TYPE("CONSTRAINT");

    WRITE_STRING_FIELD(conname);
    WRITE_BOOL_FIELD(deferrable);
    WRITE_BOOL_FIELD(initdeferred);
    WRITE_LOCATION_FIELD(location);

    appendStringInfo(str, " :contype ");
    switch (node->contype) {
        case CONSTR_NULL:
            appendStringInfo(str, "NULL");
            break;

        case CONSTR_NOTNULL:
            appendStringInfo(str, "NOT_NULL");
            break;

        case CONSTR_DEFAULT:
            appendStringInfo(str, "DEFAULT");
            WRITE_NODE_FIELD(raw_expr);
            WRITE_STRING_FIELD(cooked_expr);
            break;

        case CONSTR_CHECK:
            appendStringInfo(str, "CHECK");
            WRITE_BOOL_FIELD(is_no_inherit);
            WRITE_NODE_FIELD(raw_expr);
            WRITE_STRING_FIELD(cooked_expr);
            break;

        case CONSTR_PRIMARY:
            appendStringInfo(str, "PRIMARY_KEY");
            WRITE_NODE_FIELD(keys);
            if (t_thrd.proc->workingVersionNum >= SUPPORT_GPI_VERSION_NUM) {
                WRITE_NODE_FIELD(including);
            }
            WRITE_NODE_FIELD(options);
            WRITE_STRING_FIELD(indexname);
            WRITE_STRING_FIELD(indexspace);
            /* access_method and where_clause not currently used */
            break;

        case CONSTR_UNIQUE:
            appendStringInfo(str, "UNIQUE");
            WRITE_NODE_FIELD(keys);
            if (t_thrd.proc->workingVersionNum >= SUPPORT_GPI_VERSION_NUM) {
                WRITE_NODE_FIELD(including);
            }
            WRITE_NODE_FIELD(options);
            WRITE_STRING_FIELD(indexname);
            WRITE_STRING_FIELD(indexspace);
            /* access_method and where_clause not currently used */
            break;

        case CONSTR_EXCLUSION:
            appendStringInfo(str, "EXCLUSION");
            WRITE_NODE_FIELD(exclusions);
            if (t_thrd.proc->workingVersionNum >= SUPPORT_GPI_VERSION_NUM) {
                WRITE_NODE_FIELD(including);
            }
            WRITE_NODE_FIELD(options);
            WRITE_STRING_FIELD(indexname);
            WRITE_STRING_FIELD(indexspace);
            WRITE_STRING_FIELD(access_method);
            WRITE_NODE_FIELD(where_clause);
            break;

        case CONSTR_FOREIGN:
            appendStringInfo(str, "FOREIGN_KEY");
            WRITE_NODE_FIELD(pktable);
            WRITE_NODE_FIELD(fk_attrs);
            WRITE_NODE_FIELD(pk_attrs);
            WRITE_CHAR_FIELD(fk_matchtype);
            WRITE_CHAR_FIELD(fk_upd_action);
            WRITE_CHAR_FIELD(fk_del_action);
            WRITE_NODE_FIELD(old_conpfeqop);
            WRITE_OID_FIELD(old_pktable_oid);
            WRITE_BOOL_FIELD(skip_validation);
            WRITE_BOOL_FIELD(initially_valid);
            break;

        case CONSTR_CLUSTER:
            appendStringInfo(str, "CLUSTER");
            WRITE_NODE_FIELD(keys);
            break;

        case CONSTR_ATTR_DEFERRABLE:
            appendStringInfo(str, "ATTR_DEFERRABLE");
            break;

        case CONSTR_ATTR_NOT_DEFERRABLE:
            appendStringInfo(str, "ATTR_NOT_DEFERRABLE");
            break;

        case CONSTR_ATTR_DEFERRED:
            appendStringInfo(str, "ATTR_DEFERRED");
            break;

        case CONSTR_ATTR_IMMEDIATE:
            appendStringInfo(str, "ATTR_IMMEDIATE");
            break;

        default:
            appendStringInfo(str, "<unrecognized_constraint %d>", (int)node->contype);
            break;
    }
}

static void _outDistFdwDataNodeTask(StringInfo str, DistFdwDataNodeTask* node)
{
    WRITE_NODE_TYPE("DISTFDWDATANODETASK");

    WRITE_STRING_FIELD(dnName);
    WRITE_NODE_FIELD(task);
}

static void _outDistFdwFileSegment(StringInfo str, DistFdwFileSegment* node)
{
    WRITE_NODE_TYPE("DISTFDWFILESEGMENT");

    WRITE_STRING_FIELD(filename);
    WRITE_LONG_FIELD(begin);
    WRITE_LONG_FIELD(end);
    WRITE_LONG_FIELD(ObjectSize);
}

static void _outSplitInfo(StringInfo str, SplitInfo* node)
{
    WRITE_NODE_TYPE("SPLITINFO");

    WRITE_STRING_FIELD(filePath);
    WRITE_STRING_FIELD(fileName);
    WRITE_NODE_FIELD(partContentList);
    WRITE_LONG_FIELD(ObjectSize);
    WRITE_STRING_FIELD(eTag);
    WRITE_INT_FIELD(prefixSlashNum);
}

static void _outSplitMap(StringInfo str, SplitMap* node)
{
    WRITE_NODE_TYPE("SPLITMAP");

    WRITE_INT_FIELD(nodeId);
    WRITE_CHAR_FIELD(locatorType);
    WRITE_LONG_FIELD(totalSize);
    WRITE_INT_FIELD(fileNums);
    WRITE_STRING_FIELD(downDiskFilePath);
    WRITE_NODE_FIELD(lengths);
    WRITE_NODE_FIELD(splits);
}

static void _outErrorCacheEntry(StringInfo str, ErrorCacheEntry* node)
{
    WRITE_NODE_TYPE("ERRORCACHEENTRY");

    WRITE_NODE_FIELD(rte);
    WRITE_STRING_FIELD(filename);
}

/* @hdfs */
static void _outDfsPrivateItem(StringInfo str, DfsPrivateItem* node)
{
    int i;
    WRITE_NODE_TYPE("DFSPRIVATEITEM");

    WRITE_NODE_FIELD(columnList);
    WRITE_NODE_FIELD(targetList);
    WRITE_NODE_FIELD(restrictColList);
    WRITE_NODE_FIELD(partList);
    WRITE_NODE_FIELD(opExpressionList);
    WRITE_NODE_FIELD(dnTask);
    WRITE_NODE_FIELD(hdfsQual);
    WRITE_INT_FIELD(colNum);
    appendStringInfo(str, " :selectivity");
    for (i = 0; i < node->colNum; i++) {
        appendStringInfo(str, " %f", node->selectivity[i]);
    }
}

/*
 * Vector Plan Node
 */

static void _outRowToVec(StringInfo str, RowToVec* node)
{
    WRITE_NODE_TYPE("ROWTOVEC");
    _outPlanInfo(str, (Plan*)node);
}

static void _outVecToRow(StringInfo str, VecToRow* node)
{
    WRITE_NODE_TYPE("VECTOROW");

    _outPlanInfo(str, (Plan*)node);
}

static void _outVecSort(StringInfo str, VecSort* node)
{
    int i;
    WRITE_NODE_TYPE("VECSORT");

    _outPlanInfo(str, (Plan*)node);

    WRITE_INT_FIELD(numCols);

    appendStringInfo(str, " :sortColIdx");
    for (i = 0; i < node->numCols; i++) {
        appendStringInfo(str, " %d", node->sortColIdx[i]);
    }

    appendStringInfo(str, " :sortOperators");
    for (i = 0; i < node->numCols; i++) {
        appendStringInfo(str, " %u", node->sortOperators[i]);
    }

    appendStringInfo(str, " :collations");
    for (i = 0; i < node->numCols; i++) {
        appendStringInfo(str, " %u", node->collations[i]);
    }

    appendStringInfo(str, " :nullsFirst");
    for (i = 0; i < node->numCols; i++) {
        appendStringInfo(str, " %s", booltostr(node->nullsFirst[i]));
    }
    out_mem_info(str, &node->mem_info);
}

static void _outVecResult(StringInfo str, VecResult* node)
{
    WRITE_NODE_TYPE("VECRESULT");

    _outPlanInfo(str, (Plan*)node);

    WRITE_NODE_FIELD(resconstantqual);
}
static void _outCStoreScan(StringInfo str, CStoreScan* node)
{
    WRITE_NODE_TYPE("CSTORESCAN");

    _outScanInfo(str, (Scan*)node);

    WRITE_NODE_FIELD(cstorequal);
    WRITE_NODE_FIELD(minMaxInfo);
    WRITE_ENUM_FIELD(relStoreLocation, RelstoreType);
    WRITE_BOOL_FIELD(is_replica_table);
}

static void _outDfsScan(StringInfo str, DfsScan* node)
{
    WRITE_NODE_TYPE("DFSSCAN");

    _outScanInfo(str, (Scan*)node);

    WRITE_ENUM_FIELD(relStoreLocation, RelstoreType);
    WRITE_NODE_FIELD(privateData);
    WRITE_STRING_FIELD(storeFormat);
}

#ifdef ENABLE_MULTIPLE_NODES
static void
_outTsStoreScan(StringInfo str, TsStoreScan *node)
{
    WRITE_NODE_TYPE("TSSTORESCAN");

    _outScanInfo(str, (Scan*)node);

    WRITE_NODE_FIELD(tsstorequal);
    WRITE_NODE_FIELD(minMaxInfo);
    WRITE_ENUM_FIELD(relStoreLocation, RelstoreType);
    WRITE_BOOL_FIELD(is_replica_table);
    WRITE_INT_FIELD(sort_by_time_colidx);
    WRITE_INT_FIELD(limit);
    WRITE_BOOL_FIELD(is_simple_scan);
    WRITE_BOOL_FIELD(has_sort);
    WRITE_INT_FIELD(series_func_calls);
    WRITE_INT_FIELD(top_key_func_arg);
}
#endif   /* ENABLE_MULTIPLE_NODES */

static void _outVecSubqueryScan(StringInfo str, VecSubqueryScan* node)
{
    WRITE_NODE_TYPE("VECSUBQUERYSCAN");

    _outScanInfo(str, (Scan*)node);

    WRITE_NODE_FIELD(subplan);
}

static void _outVecPartIterator(StringInfo str, VecPartIterator* node)
{
    WRITE_NODE_TYPE("VECPARTITERATOR");

    _outPlanInfo(str, (Plan*)node);

    WRITE_ENUM_FIELD(partType, PartitionType);
    WRITE_INT_FIELD(itrs);
    WRITE_ENUM_FIELD(direction, ScanDirection);
    WRITE_NODE_FIELD(param);
}

static void _outVecLimit(StringInfo str, VecLimit* node)
{
    WRITE_NODE_TYPE("VECLIMIT");

    _outPlanInfo(str, (Plan*)node);

    WRITE_NODE_FIELD(limitOffset);
    WRITE_NODE_FIELD(limitCount);
}

static void _outVecModifyTable(StringInfo str, VecModifyTable* node)
{
    WRITE_NODE_TYPE("VECMODIFYTABLE");

    _outPlanInfo(str, (Plan*)node);
    WRITE_ENUM_FIELD(operation, CmdType);
    WRITE_BOOL_FIELD(canSetTag);
    WRITE_NODE_FIELD(resultRelations);
    WRITE_INT_FIELD(resultRelIndex);
    WRITE_NODE_FIELD(plans);
    WRITE_NODE_FIELD(returningLists);
    WRITE_NODE_FIELD(rowMarks);
    WRITE_INT_FIELD(epqParam);
    WRITE_BOOL_FIELD(partKeyUpdated);
#ifdef PGXC
    WRITE_NODE_FIELD(remote_plans);
    WRITE_NODE_FIELD(remote_insert_plans);
    WRITE_NODE_FIELD(remote_update_plans);
    WRITE_NODE_FIELD(remote_delete_plans);
#endif
    WRITE_NODE_FIELD(cacheEnt);
    WRITE_INT_FIELD(mergeTargetRelation);
    WRITE_NODE_FIELD(mergeSourceTargetList);
    WRITE_NODE_FIELD(mergeActionList);
    out_mem_info(str, &node->mem_info);
}

static void _outVecForeignScan(StringInfo str, VecForeignScan* node)
{
    WRITE_NODE_TYPE("VECFOREIGNSCAN");
    _outCommonForeignScanPart<VecForeignScan>(str, node);
}

static void _outVecStream(StringInfo str, VecStream* node)
{
    WRITE_NODE_TYPE("VECSTREAM");

    _outScanInfo(str, (Scan*)node);

    WRITE_ENUM_FIELD(type, StreamType);
    WRITE_STRING_FIELD(plan_statement);
    WRITE_NODE_FIELD(consumer_nodes);
    WRITE_NODE_FIELD(distribute_keys);
    WRITE_BOOL_FIELD(is_sorted);
    WRITE_NODE_FIELD(sort);
    WRITE_BOOL_FIELD(is_dummy);
    WRITE_INT_FIELD(smpDesc.consumerDop);
    WRITE_INT_FIELD(smpDesc.producerDop);
    WRITE_INT_FIELD(smpDesc.distriType);
    WRITE_NODE_FIELD(skew_list);
    WRITE_INT_FIELD(stream_level);
    WRITE_NODE_FIELD(origin_consumer_nodes);
    WRITE_BOOL_FIELD(is_recursive_local);
}

#ifdef PGXC
static void _outVecRemoteQuery(StringInfo str, VecRemoteQuery* node)
{
    WRITE_NODE_TYPE("VECREMOTEQUERY");
    _outCommonRemoteQueryPart<VecRemoteQuery>(str, node);
}
#endif

static void _outVecWindowAgg(StringInfo str, VecWindowAgg* node)
{
    int i;

    WRITE_NODE_TYPE("VECWINDOWAGG");

    _outPlanInfo(str, (Plan*)node);

    WRITE_UINT_FIELD(winref);
    WRITE_INT_FIELD(partNumCols);

    appendStringInfo(str, " :partColIdx");
    for (i = 0; i < node->partNumCols; i++) {
        appendStringInfo(str, " %d", node->partColIdx[i]);
    }

    appendStringInfo(str, " :partOperations");
    for (i = 0; i < node->partNumCols; i++) {
        appendStringInfo(str, " %u", node->partOperators[i]);
    }

    WRITE_INT_FIELD(ordNumCols);

    appendStringInfo(str, " :ordColIdx");
    for (i = 0; i < node->ordNumCols; i++) {
        appendStringInfo(str, " %d", node->ordColIdx[i]);
    }

    appendStringInfo(str, " :ordOperations");
    for (i = 0; i < node->ordNumCols; i++) {
        appendStringInfo(str, " %u", node->ordOperators[i]);
    }

    WRITE_INT_FIELD(frameOptions);
    WRITE_NODE_FIELD(startOffset);
    WRITE_NODE_FIELD(endOffset);
    out_mem_info(str, &node->mem_info);
}

static void _outInformationalConstraint(StringInfo str, InformationalConstraint* node)
{
    WRITE_NODE_TYPE("SOFTCONSTRAINT");
    WRITE_STRING_FIELD(constrname);
    WRITE_CHAR_FIELD(contype);
    WRITE_BOOL_FIELD(nonforced);
    WRITE_BOOL_FIELD(enableOpt);
}

static void _outAttrMetaData(StringInfo str, AttrMetaData* node)
{
    WRITE_NODE_TYPE("ATTRMETADATA");

    WRITE_STRING_FIELD(attname);
    WRITE_OID_FIELD(atttypid);
    WRITE_OID_FIELD(attlen);
    WRITE_OID_FIELD(attnum);
    WRITE_OID_FIELD(atttypmod);
    WRITE_BOOL_FIELD(attbyval);
    WRITE_CHAR_FIELD(attstorage);
    WRITE_CHAR_FIELD(attalign);
    WRITE_BOOL_FIELD(attnotnull);
    WRITE_BOOL_FIELD(atthasdef);
    WRITE_BOOL_FIELD(attisdropped);
    WRITE_BOOL_FIELD(attislocal);
    WRITE_INT_FIELD(attcmprmode);
    WRITE_INT_FIELD(attinhcount);
    WRITE_OID_FIELD(attcollation);

    WRITE_TYPEINFO_FIELD(atttypid);
}

static void _outRelationMetaData(StringInfo str, RelationMetaData* node)
{
    WRITE_NODE_TYPE("RELATIONMETADATA");

    WRITE_OID_FIELD(rd_id);

    WRITE_OID_FIELD(spcNode);
    WRITE_OID_FIELD(dbNode);
    WRITE_OID_FIELD(relNode);
    if (t_thrd.proc->workingVersionNum >= 92063) {
        WRITE_INT_FIELD(bucketNode);
    }
    WRITE_STRING_FIELD(relname);
    WRITE_CHAR_FIELD(relkind);
    WRITE_CHAR_FIELD(parttype);

    WRITE_INT_FIELD(natts);
    WRITE_NODE_FIELD(attrs);
}

static void _outForeignOptions(StringInfo str, ForeignOptions* node)
{
    WRITE_NODE_TYPE("FOREIGNOPTIONS");

    WRITE_ENUM_FIELD(stype, ServerTypeOption);

    WRITE_NODE_FIELD(fOptions);
}

/**
 * @Description: serialze the BloomFilterSet struct.
 * @in str, store serialized string.
 * @in node, the node to be serialize.
 * @return none.
 */
static void _outBloomFilterSet(StringInfo str, BloomFilterSet* node)
{
    uint64 len = str->len;
    WRITE_NODE_TYPE("BLOOMFILTERSET");

    WRITE_FLOAT_FIELD(length, "%lu");
    _outUint64Array(str, node->data, node->length);
    WRITE_FLOAT_FIELD(numBits, "%lu");
    WRITE_FLOAT_FIELD(numHashFunctions, "%lu");
    WRITE_FLOAT_FIELD(numValues, "%lu");
    WRITE_FLOAT_FIELD(maxNumValues, "%lu");
    WRITE_FLOAT_FIELD(startupEntries, "%lu");
    if (node->startupEntries > 0) {
        for (uint64 cell = 0; cell < node->startupEntries; cell++) {
            _outUint16Array(str, node->valuePositions[cell].position, MAX_HASH_FUNCTIONS);
        }
    }

    WRITE_FLOAT_FIELD(minIntValue, "%lu");
    WRITE_FLOAT_FIELD(minFloatValue, "%f");
    WRITE_STRING_FIELD(minStringValue);
    WRITE_FLOAT_FIELD(maxIntValue, "%lu");
    WRITE_FLOAT_FIELD(maxFloatValue, "%f");
    WRITE_STRING_FIELD(maxStringValue);

    WRITE_BOOL_FIELD(addMinMax);
    WRITE_BOOL_FIELD(hasMM);
    WRITE_ENUM_FIELD(bfType, BloomFilterType);
    WRITE_OID_FIELD(dataType);
    WRITE_INT_FIELD(typeMod);
    WRITE_OID_FIELD(collation);

    WRITE_TYPEINFO_FIELD(dataType);
    elog(DEBUG1, "outfuncs:%s", str->data + len);
}

/*
 * @Description: serialze PurgeStmt
 * @out str: String buf.
 * @in node: PurgeStmt struct.
 */
static void OutPurgeStmt(StringInfo str, PurgeStmt* node)
{
    WRITE_NODE_TYPE("PURGESTMT");
    WRITE_ENUM_FIELD(purtype, PurgeType);
    WRITE_NODE_FIELD(purobj);
}

/*
 * @Description: serialze TimeCapsuleStmt
 * @out str: String buf.
 * @in node: TimeCapsuleStmt struct.
 */
static void OutTimeCapsuleStmt(StringInfo str, TimeCapsuleStmt* node)
{
    WRITE_NODE_TYPE("TIMECAPSULESTME");
    WRITE_ENUM_FIELD(tcaptype, TimeCapsuleType);
    WRITE_NODE_FIELD(relation);
    WRITE_STRING_FIELD(new_relname);

    WRITE_NODE_FIELD(tvver); 
    WRITE_ENUM_FIELD(tvtype, TvVersionType);
}

/*
 * @Description: serialze CommentStmt
 * @out str: String buf.
 * @in node: CommentStmt struct.
 */
static void _outCommentStmt(StringInfo str, CommentStmt* node)
{
    WRITE_NODE_TYPE("COMMENTSTMT");
    WRITE_ENUM_FIELD(objtype, ObjectType);
    WRITE_NODE_FIELD(objname);
    WRITE_NODE_FIELD(objargs);
    WRITE_STRING_FIELD(comment);
}

/**
 * @Description: serialze the TableLikeCtx struct.
 * @in str, store serialized string.
 * @in node, the node to be serialize.
 * @return none.
 */
static void _outTableLikeCtx(StringInfo str, TableLikeCtx* node)
{
    WRITE_NODE_TYPE("TABLELIKECTX");
    WRITE_UINT_FIELD(options);
    WRITE_BOOL_FIELD(temp_table);
    WRITE_BOOL_FIELD(hasoids);
    WRITE_NODE_FIELD(columns);
    WRITE_NODE_FIELD(ckconstraints);
    WRITE_NODE_FIELD(comments);
    WRITE_NODE_FIELD(cluster_keys);
    WRITE_NODE_FIELD(partition);
    WRITE_NODE_FIELD(inh_indexes);
    WRITE_NODE_FIELD(reloptions);
}

static void _outQualSkewInfo(StringInfo str, QualSkewInfo* node)
{
    WRITE_NODE_TYPE("QUALSKEWINFO");
    WRITE_ENUM_FIELD(skew_stream_type, SkewStreamType);
    WRITE_NODE_FIELD(skew_quals);
    WRITE_FLOAT_FIELD(qual_cost.startup, "%.2f");
    WRITE_FLOAT_FIELD(qual_cost.per_tuple, "%.2f");
    WRITE_FLOAT_FIELD(broadcast_ratio, "%.2f");
}

static void _outIndexVar(StringInfo str, IndexVar* node)
{
    WRITE_NODE_TYPE("INDEXVAR");

    WRITE_OID_FIELD(relid);
    WRITE_UINT_FIELD(attno);
    WRITE_STRING_FIELD(relname);
    WRITE_STRING_FIELD(attname);
    WRITE_BOOL_FIELD(indexcol);
    WRITE_NODE_FIELD(indexoids);
    WRITE_BOOL_FIELD(indexpath);
}

static void _outTrainModel(StringInfo str, TrainModel* node)
{
    AlgorithmAPI *api = get_algorithm_api(node->algorithm);
    int num_hyperp;
    const HyperparameterDefinition* definition = api->get_hyperparameters_definitions(api, &num_hyperp);

    if (node->configurations != 1)
        elog(ERROR, "TODO_DB4AI_API: more than one hyperparameter configuration");

    WRITE_NODE_TYPE("TrainModel");
    _outPlanInfo(str, (Plan*)node);
    appendStringInfoString(str, " :algorithm ");
    appendStringInfoString(str, api->name);

    HyperparametersGD *hyperp = (HyperparametersGD *)node->hyperparameters[0];
    while (num_hyperp-- > 0) {
        switch (definition->type) {
            case INT4OID: {
                int32_t *value_addr = (int32_t *)((char *)hyperp + definition->offset);
                appendStringInfo(str, " : %s %d", definition->name, *value_addr);
                break;
            }
            case INT8OID: {
                int64_t *value_addr = (int64_t *)((char *)hyperp + definition->offset);
                appendStringInfo(str, " : %s %ld", definition->name, *value_addr);
                break;
            }
            case FLOAT8OID: {
                double *value_addr = (double *)((char *)hyperp + definition->offset);
                appendStringInfo(str, " : %s %.16g", definition->name, *value_addr);
                break;
            }
            case BOOLOID: {
                bool *value_addr = (bool *)((char *)hyperp + definition->offset);
                appendStringInfo(str, " : %s %s", definition->name, booltostr(*value_addr));
                break;
            }
            case CSTRINGOID: {
                char **value_addr = (char **)((char *)hyperp + definition->offset);
                appendStringInfo(str, " : %s %s", definition->name, *value_addr);
                break;
            }
            case ANYENUMOID: {
                void *value_addr = (void *)((char *)hyperp + definition->offset);
                appendStringInfo(str, " : %s %s", definition->name, definition->validation.enum_getter(value_addr));
                break;
            }
            default:
                break;
        }
        definition++;
    }
}

/*
 * _outNode -
 *	  converts a Node into ascii string and append it to 'str'
 */
static void _outNode(StringInfo str, const void* obj)
{
    /* Guard against stack overflow due to overly complex expressions */
    check_stack_depth();

    if (obj == NULL) {
        appendStringInfo(str, "<>");
    } else if (IsA(obj, List) || IsA(obj, IntList) || IsA(obj, OidList)) {
        _outList(str, (List*)obj);
    } else if (IsA(obj, Integer) || IsA(obj, Float) || IsA(obj, String) || IsA(obj, BitString)) {
        /* nodeRead does not want to see { } around these! */
        _outValue(str, (Value*)obj);
    } else {
        appendStringInfoChar(str, '{');
        switch (nodeTag(obj)) {
            case T_PlannedStmt:
                _outPlannedStmt(str, (PlannedStmt*)obj);
                break;
            case T_HDFSTableAnalyze:
                _outHDFSTableAnalyze(str, (HDFSTableAnalyze*)obj);
                break;
            case T_Plan:
                _outPlan(str, (Plan*)obj);
                break;
            case T_BaseResult:
                _outResult(str, (BaseResult*)obj);
                break;
            case T_ModifyTable:
                _outModifyTable(str, (ModifyTable*)obj);
                break;
            case T_MergeWhenClause:
                _outMergeWhenClause(str, (MergeWhenClause*)obj);
                break;
            case T_Append:
                _outAppend(str, (Append*)obj);
                break;
            case T_MergeAppend:
                _outMergeAppend(str, (MergeAppend*)obj);
                break;
            case T_RecursiveUnion:
                _outRecursiveUnion(str, (RecursiveUnion*)obj);
                break;
            case T_StartWithOptions:
                _outStartWithOptions(str, (StartWithOptions*)obj);
                break;
            case T_StartWithOp:
                _outStartWithOp(str, (StartWithOp*)obj);
                break;
            case T_BitmapAnd:
                _outBitmapAnd(str, (BitmapAnd*)obj);
                break;
            case T_BitmapOr:
                _outBitmapOr(str, (BitmapOr*)obj);
                break;
            case T_Scan:
                _outScan(str, (Scan*)obj);
                break;
            case T_BucketInfo:
                _outBucketInfo(str, (BucketInfo*)obj);
                break;
            case T_SeqScan:
                _outSeqScan(str, (SeqScan*)obj);
                break;
#ifdef PGXC
            case T_RemoteQuery:
                _outRemoteQuery(str, (RemoteQuery*)obj);
                break;
            case T_RemoteQueryPath:
                _outRemoteQueryPath(str, (RemoteQueryPath*)obj);
                break;
#endif
            case T_Stream:
                _outStream(str, (Stream*)obj);
                break;
            case T_StreamPath:
                _outStreamPath(str, (StreamPath*)obj);
                break;
            case T_IndexScan:
                _outIndexScan(str, (IndexScan*)obj);
                break;
            case T_IndexOnlyScan:
                _outIndexOnlyScan(str, (IndexOnlyScan*)obj);
                break;
            case T_CStoreIndexScan:
                _outCStoreIndexScan(str, (CStoreIndexScan*)obj);
                break;
            case T_DfsIndexScan:
                _outDfsIndexScan(str, (DfsIndexScan*)obj);
                break;
            case T_BitmapIndexScan:
                _outBitmapIndexScan(str, (BitmapIndexScan*)obj);
                break;
            case T_BitmapHeapScan:
                _outBitmapHeapScan(str, (BitmapHeapScan*)obj);
                break;
            case T_CStoreIndexCtidScan:
                _outCStoreIndexCtidScan(str, (CStoreIndexCtidScan*)obj);
                break;
            case T_CStoreIndexHeapScan:
                _outCStoreIndexHeapScan(str, (CStoreIndexHeapScan*)obj);
                break;
            case T_CStoreIndexAnd:
                _outCStoreIndexAnd(str, (CStoreIndexAnd*)obj);
                break;
            case T_CStoreIndexOr:
                _outCStoreIndexOr(str, (CStoreIndexOr*)obj);
                break;
            case T_TidScan:
                _outTidScan(str, (TidScan*)obj);
                break;
            case T_PartIteratorParam:
                _outPartIteratorParam(str, (PartIteratorParam*)obj);
                break;
            case T_PartIterator:
                _outPartIterator(str, (PartIterator*)obj);
                break;
            case T_SubqueryScan:
                _outSubqueryScan(str, (SubqueryScan*)obj);
                break;
            case T_FunctionScan:
                _outFunctionScan(str, (FunctionScan*)obj);
                break;
            case T_ValuesScan:
                _outValuesScan(str, (ValuesScan*)obj);
                break;
            case T_CteScan:
                _outCteScan(str, (CteScan*)obj);
                break;
            case T_WorkTableScan:
                _outWorkTableScan(str, (WorkTableScan*)obj);
                break;
            case T_ForeignScan:
                _outForeignScan(str, (ForeignScan*)obj);
                break;
            case T_ForeignPartState:
                _outForeignPartState(str, (ForeignPartState*)obj);
                break;
            case T_ExtensiblePlan:
                _outExtensiblePlan(str, (ExtensiblePlan*)obj);
                break;
            case T_Join:
                _outJoin(str, (Join*)obj);
                break;
            case T_NestLoop:
                _outNestLoop(str, (NestLoop*)obj);
                break;
            case T_MergeJoin:
                _outMergeJoin(str, (MergeJoin*)obj);
                break;
            case T_HashJoin:
                _outHashJoin(str, (HashJoin*)obj);
                break;
            case T_Agg:
                _outAgg(str, (Agg*)obj);
                break;
            case T_WindowAgg:
                _outWindowAgg(str, (WindowAgg*)obj);
                break;
            case T_Group:
                _outGroup(str, (Group*)obj);
                break;
            case T_Material:
                _outMaterial(str, (Material*)obj);
                break;
            case T_Sort:
                _outSort(str, (Sort*)obj);
                break;
            case T_Unique:
                _outUnique(str, (Unique*)obj);
                break;
            case T_Hash:
                _outHash(str, (Hash*)obj);
                break;
            case T_SetOp:
                _outSetOp(str, (SetOp*)obj);
                break;
            case T_LockRows:
                _outLockRows(str, (LockRows*)obj);
                break;
            case T_Limit:
                _outLimit(str, (Limit*)obj);
                break;
            case T_NestLoopParam:
                _outNestLoopParam(str, (NestLoopParam*)obj);
                break;
            case T_PlanRowMark:
                _outPlanRowMark(str, (PlanRowMark*)obj);
                break;
            case T_PlanInvalItem:
                _outPlanInvalItem(str, (PlanInvalItem*)obj);
                break;
            case T_Alias:
                _outAlias(str, (Alias*)obj);
                break;
            case T_RangeVar:
                _outRangeVar(str, (RangeVar*)obj);
                break;
            case T_IntoClause:
                _outIntoClause(str, (IntoClause*)obj);
                break;
            case T_Var:
                _outVar(str, (Var*)obj);
                break;
            case T_Const:
                _outConst(str, (Const*)obj);
                break;
            case T_Param:
                _outParam(str, (Param*)obj);
                break;
            case T_Rownum:
                _outRownum(str, (Rownum*)obj);
                break;
            case T_Aggref:
                _outAggref(str, (Aggref*)obj);
                break;
            case T_GroupingFunc:
                _outGroupingFunc(str, (GroupingFunc*)obj);
                break;
            case T_GroupingId:
                _outGroupingId(str, (GroupingId*)obj);
                break;
            case T_WindowFunc:
                _outWindowFunc(str, (WindowFunc*)obj);
                break;
            case T_ArrayRef:
                _outArrayRef(str, (ArrayRef*)obj);
                break;
            case T_FuncExpr:
                _outFuncExpr(str, (FuncExpr*)obj);
                break;
            case T_NamedArgExpr:
                _outNamedArgExpr(str, (NamedArgExpr*)obj);
                break;
            case T_OpExpr:
                _outOpExpr(str, (OpExpr*)obj);
                break;
            case T_DistinctExpr:
                _outDistinctExpr(str, (DistinctExpr*)obj);
                break;
            case T_NullIfExpr:
                _outNullIfExpr(str, (NullIfExpr*)obj);
                break;
            case T_ScalarArrayOpExpr:
                _outScalarArrayOpExpr(str, (ScalarArrayOpExpr*)obj);
                break;
            case T_BoolExpr:
                _outBoolExpr(str, (BoolExpr*)obj);
                break;
            case T_SubLink:
                _outSubLink(str, (SubLink*)obj);
                break;
            case T_SubPlan:
                _outSubPlan(str, (SubPlan*)obj);
                break;
            case T_AlternativeSubPlan:
                _outAlternativeSubPlan(str, (AlternativeSubPlan*)obj);
                break;
            case T_FieldSelect:
                _outFieldSelect(str, (FieldSelect*)obj);
                break;
            case T_FieldStore:
                _outFieldStore(str, (FieldStore*)obj);
                break;
            case T_RelabelType:
                _outRelabelType(str, (RelabelType*)obj);
                break;
            case T_CoerceViaIO:
                _outCoerceViaIO(str, (CoerceViaIO*)obj);
                break;
            case T_ArrayCoerceExpr:
                _outArrayCoerceExpr(str, (ArrayCoerceExpr*)obj);
                break;
            case T_ConvertRowtypeExpr:
                _outConvertRowtypeExpr(str, (ConvertRowtypeExpr*)obj);
                break;
            case T_CollateExpr:
                _outCollateExpr(str, (CollateExpr*)obj);
                break;
            case T_CaseExpr:
                _outCaseExpr(str, (CaseExpr*)obj);
                break;
            case T_CaseWhen:
                _outCaseWhen(str, (CaseWhen*)obj);
                break;
            case T_CaseTestExpr:
                _outCaseTestExpr(str, (CaseTestExpr*)obj);
                break;
            case T_ArrayExpr:
                _outArrayExpr(str, (ArrayExpr*)obj);
                break;
            case T_RowExpr:
                _outRowExpr(str, (RowExpr*)obj);
                break;
            case T_RowCompareExpr:
                _outRowCompareExpr(str, (RowCompareExpr*)obj);
                break;
            case T_CoalesceExpr:
                _outCoalesceExpr(str, (CoalesceExpr*)obj);
                break;
            case T_MinMaxExpr:
                _outMinMaxExpr(str, (MinMaxExpr*)obj);
                break;
            case T_XmlExpr:
                _outXmlExpr(str, (XmlExpr*)obj);
                break;
            case T_NullTest:
                _outNullTest(str, (NullTest*)obj);
                break;
            case T_HashFilter:
                _outHashFilter(str, (HashFilter*)obj);
                break;
            case T_BooleanTest:
                _outBooleanTest(str, (BooleanTest*)obj);
                break;
            case T_CoerceToDomain:
                _outCoerceToDomain(str, (CoerceToDomain*)obj);
                break;
            case T_CoerceToDomainValue:
                _outCoerceToDomainValue(str, (CoerceToDomainValue*)obj);
                break;
            case T_SetToDefault:
                _outSetToDefault(str, (SetToDefault*)obj);
                break;
            case T_CurrentOfExpr:
                _outCurrentOfExpr(str, (CurrentOfExpr*)obj);
                break;
            case T_TargetEntry:
                _outTargetEntry(str, (TargetEntry*)obj);
                break;
            case T_PseudoTargetEntry:
                _outPseudoTargetEntry(str, (PseudoTargetEntry*)obj);
                break;
            case T_RangeTblRef:
                _outRangeTblRef(str, (RangeTblRef*)obj);
                break;
            case T_JoinExpr:
                _outJoinExpr(str, (JoinExpr*)obj);
                break;
            case T_FromExpr:
                _outFromExpr(str, (FromExpr*)obj);
                break;
            case T_MergeAction:
                _outMergeAction(str, (MergeAction*)obj);
                break;
            case T_UpsertExpr:
                _outUpsertExpr(str, (UpsertExpr*)obj);
                break;
            case T_Path:
                _outPath(str, (Path*)obj);
                break;
            case T_IndexPath:
                _outIndexPath(str, (IndexPath*)obj);
                break;
            case T_BitmapHeapPath:
                _outBitmapHeapPath(str, (BitmapHeapPath*)obj);
                break;
            case T_BitmapAndPath:
                _outBitmapAndPath(str, (BitmapAndPath*)obj);
                break;
            case T_BitmapOrPath:
                _outBitmapOrPath(str, (BitmapOrPath*)obj);
                break;
            case T_TidPath:
                _outTidPath(str, (TidPath*)obj);
                break;
            case T_PartIteratorPath:
                _outPartIteratorPath(str, (PartIteratorPath*)obj);
                break;
            case T_ForeignPath:
                _outForeignPath(str, (ForeignPath*)obj);
                break;
            case T_AppendPath:
                _outAppendPath(str, (AppendPath*)obj);
                break;
            case T_MergeAppendPath:
                _outMergeAppendPath(str, (MergeAppendPath*)obj);
                break;
            case T_ResultPath:
                _outResultPath(str, (ResultPath*)obj);
                break;
            case T_MaterialPath:
                _outMaterialPath(str, (MaterialPath*)obj);
                break;
            case T_UniquePath:
                _outUniquePath(str, (UniquePath*)obj);
                break;
            case T_NestPath:
                _outNestPath(str, (NestPath*)obj);
                break;
            case T_MergePath:
                _outMergePath(str, (MergePath*)obj);
                break;
            case T_HashPath:
                _outHashPath(str, (HashPath*)obj);
                break;
            case T_PlannerGlobal:
                _outPlannerGlobal(str, (PlannerGlobal*)obj);
                break;
            case T_PlannerInfo:
                _outPlannerInfo(str, (PlannerInfo*)obj);
                break;
            case T_RelOptInfo:
                _outRelOptInfo(str, (RelOptInfo*)obj);
                break;
            case T_IndexOptInfo:
                _outIndexOptInfo(str, (IndexOptInfo*)obj);
                break;
            case T_EquivalenceClass:
                _outEquivalenceClass(str, (EquivalenceClass*)obj);
                break;
            case T_EquivalenceMember:
                _outEquivalenceMember(str, (EquivalenceMember*)obj);
                break;
            case T_PathKey:
                _outPathKey(str, (PathKey*)obj);
                break;
            case T_ParamPathInfo:
                _outParamPathInfo(str, (ParamPathInfo*)obj);
                break;
            case T_RestrictInfo:
                _outRestrictInfo(str, (RestrictInfo*)obj);
                break;
            case T_PlaceHolderVar:
                _outPlaceHolderVar(str, (PlaceHolderVar*)obj);
                break;
            case T_SpecialJoinInfo:
                _outSpecialJoinInfo(str, (SpecialJoinInfo*)obj);
                break;
            case T_LateralJoinInfo:
                _outLateralJoinInfo(str, (LateralJoinInfo *)obj);
                break;
            case T_AppendRelInfo:
                _outAppendRelInfo(str, (AppendRelInfo*)obj);
                break;
            case T_PlaceHolderInfo:
                _outPlaceHolderInfo(str, (PlaceHolderInfo*)obj);
                break;
            case T_MinMaxAggInfo:
                _outMinMaxAggInfo(str, (MinMaxAggInfo*)obj);
                break;
            case T_PlannerParamItem:
                _outPlannerParamItem(str, (PlannerParamItem*)obj);
                break;

            case T_CreateStmt:
                _outCreateStmt(str, (CreateStmt*)obj);
                break;
            case T_PartitionState:
                _outPartitionState(str, (PartitionState*)obj);
                break;
            case T_RangePartitionDefState:
                _outRangePartitionDefState(str, (RangePartitionDefState*)obj);
                break;
            case T_ListPartitionDefState:
                _outListPartitionDefState(str, (ListPartitionDefState*)obj);
                break;
            case T_HashPartitionDefState:
                _outHashPartitionDefState(str, (HashPartitionDefState*)obj);
                break;
            case T_IntervalPartitionDefState:
                _outIntervalPartitionDefState(str, (IntervalPartitionDefState*)obj);
                break;
            case T_RangePartitionindexDefState:
                _outRangePartitionindexDefState(str, (RangePartitionindexDefState*)obj);
                break;
            case T_RangePartitionStartEndDefState:
                _outRangePartitionStartEndDefState(str, (RangePartitionStartEndDefState*)obj);
                break;
            case T_AddPartitionState:
                _outAddPartitionState(str, (AddPartitionState*)obj);
                break;
            case T_AddSubPartitionState:
                _outAddSubPartitionState(str, (AddSubPartitionState*)obj);
                break;
            case T_CreateForeignTableStmt:
                _outCreateForeignTableStmt(str, (CreateForeignTableStmt*)obj);
                break;
            case T_IndexStmt:
                _outIndexStmt(str, (IndexStmt*)obj);
                break;
            case T_NotifyStmt:
                _outNotifyStmt(str, (NotifyStmt*)obj);
                break;
            case T_DeclareCursorStmt:
                _outDeclareCursorStmt(str, (DeclareCursorStmt*)obj);
                break;
            case T_CopyStmt:
                _outCopyStmt(str, (CopyStmt*)obj);
                break;
            case T_AlterTableStmt:
                _outAlterTableStmt(str, (AlterTableStmt*)obj);
                break;
            case T_SelectStmt:
                _outSelectStmt(str, (SelectStmt*)obj);
                break;
            case T_MergeStmt:
                _outMergeStmt(str, (MergeStmt*)obj);
                break;
            case T_InsertStmt:
                _outInsertStmt(str, (InsertStmt*)obj);
                break;
            case T_UpdateStmt:
                _outUpdateStmt(str, (UpdateStmt*)obj);
                break;
            case T_ColumnDef:
                _outColumnDef(str, (ColumnDef*)obj);
                break;
            case T_TypeName:
                _outTypeName(str, (TypeName*)obj);
                break;
            case T_TypeCast:
                _outTypeCast(str, (TypeCast*)obj);
                break;
            case T_CollateClause:
                _outCollateClause(str, (CollateClause*)obj);
                break;
            case T_ClientLogicColumnParam:
                _outColumnParam(str, (ClientLogicColumnParam*)obj);
                break;
            case T_ClientLogicGlobalParam:
                _outGlobalParam(str, (ClientLogicGlobalParam*)obj);
                break;
            case T_CreateClientLogicGlobal:
                _outGlobalSetting(str, (CreateClientLogicGlobal*)obj);
                break;
            case T_CreateClientLogicColumn:
                _outColumnSetting(str, (CreateClientLogicColumn*)obj);
                break;
            case T_ClientLogicColumnRef:
                _outClientLogicColumnRef(str, (ClientLogicColumnRef *)obj);
                break;
            case T_IndexElem:
                _outIndexElem(str, (IndexElem*)obj);
                break;
            case T_Query:
                _outQuery(str, (Query*)obj);
                break;
            case T_SortGroupClause:
                _outSortGroupClause(str, (SortGroupClause*)obj);
                break;
            case T_GroupingSet:
                _outGroupingSet(str, (GroupingSet*)obj);
                break;
            case T_WindowClause:
                _outWindowClause(str, (WindowClause*)obj);
                break;
            case T_RowMarkClause:
                _outRowMarkClause(str, (RowMarkClause*)obj);
                break;
            case T_WithClause:
                _outWithClause(str, (WithClause*)obj);
                break;
            case T_StartWithClause:
                _outStartWithClause(str, (StartWithClause*)obj);
                break;
            case T_UpsertClause:
                _outUpsertClause(str, (UpsertClause*)obj);
                break;
            case T_CommonTableExpr:
                _outCommonTableExpr(str, (CommonTableExpr*)obj);
                break;
            case T_StartWithTargetRelInfo:
                _outStartWithTargetRelInfo(str, (StartWithTargetRelInfo*) obj);
                break;
            case T_SetOperationStmt:
                _outSetOperationStmt(str, (SetOperationStmt*)obj);
                break;
            case T_RangeTblEntry:
                _outRangeTblEntry(str, (RangeTblEntry*)obj);
                break;
            case T_TableSampleClause:
                _outTableSampleClause(str, (TableSampleClause*)obj);
                break;
            case T_TimeCapsuleClause:
                OutTimeCapsuleClause(str, (TimeCapsuleClause*)obj);
                break;
            case T_A_Expr:
                _outAExpr(str, (A_Expr*)obj);
                break;
            case T_ColumnRef:
                _outColumnRef(str, (ColumnRef*)obj);
                break;
            case T_ParamRef:
                _outParamRef(str, (ParamRef*)obj);
                break;
            case T_A_Const:
                _outAConst(str, (A_Const*)obj);
                break;
            case T_A_Star:
                _outA_Star(str, (A_Star*)obj);
                break;
            case T_A_Indices:
                _outA_Indices(str, (A_Indices*)obj);
                break;
            case T_A_Indirection:
                _outA_Indirection(str, (A_Indirection*)obj);
                break;
            case T_A_ArrayExpr:
                _outA_ArrayExpr(str, (A_ArrayExpr*)obj);
                break;
            case T_ResTarget:
                _outResTarget(str, (ResTarget*)obj);
                break;
            case T_SortBy:
                _outSortBy(str, (SortBy*)obj);
                break;
            case T_WindowDef:
                _outWindowDef(str, (WindowDef*)obj);
                break;
            case T_RangeSubselect:
                _outRangeSubselect(str, (RangeSubselect*)obj);
                break;
            case T_RangeFunction:
                _outRangeFunction(str, (RangeFunction*)obj);
                break;
            case T_RangeTableSample:
                _outRangeTableSample(str, (RangeTableSample*)obj);
                break;
            case T_RangeTimeCapsule:
                OutRangeTimeCapsule(str, (RangeTimeCapsule*)obj);
                break;
            case T_Constraint:
                _outConstraint(str, (Constraint*)obj);
                break;
            case T_FuncCall:
                _outFuncCall(str, (FuncCall*)obj);
                break;
            case T_DefElem:
                _outDefElem(str, (DefElem*)obj);
                break;
            case T_TableLikeClause:
                _outTableLikeClause(str, (TableLikeClause*)obj);
                break;
            case T_LockingClause:
                _outLockingClause(str, (LockingClause*)obj);
                break;
            case T_XmlSerialize:
                _outXmlSerialize(str, (XmlSerialize*)obj);
                break;

#ifdef PGXC
            case T_ExecNodes:
                _outExecNodes(str, (ExecNodes*)obj);
                break;
            case T_SliceBoundary:
                _outSliceBoundary(str, (SliceBoundary*)obj);
                break;
            case T_ExecBoundary:
                _outExecBoundary(str, (ExecBoundary*)obj);
                break;
            case T_SimpleSort:
                _outSimpleSort(str, (SimpleSort*)obj);
                break;

#endif
            case T_PruningResult:
                _outPruningResult(str, (PruningResult *)obj);
                break;
            case T_SubPartitionPruningResult:
                _outSubPartitionPruningResult(str, (SubPartitionPruningResult *)obj);
                break;
            case T_DistFdwDataNodeTask:
                _outDistFdwDataNodeTask(str, (DistFdwDataNodeTask*)obj);
                break;
            case T_DistFdwFileSegment:
                _outDistFdwFileSegment(str, (DistFdwFileSegment*)obj);
                break;
            case T_SplitInfo:
                _outSplitInfo(str, (SplitInfo*)obj);
                break;
            case T_SplitMap:
                _outSplitMap(str, (SplitMap*)obj);
                break;
            case T_ErrorCacheEntry:
                _outErrorCacheEntry(str, (ErrorCacheEntry*)obj);
                break;
            case T_DfsPrivateItem:
                _outDfsPrivateItem(str, (DfsPrivateItem*)obj);
                break;
            /*
             * Vector Nodes
             */
            case T_RowToVec:
                _outRowToVec(str, (RowToVec*)obj);
                break;

            case T_VecToRow:
                _outVecToRow(str, (VecToRow*)obj);
                break;

            case T_VecSort:
                _outVecSort(str, (VecSort*)obj);
                break;

            case T_VecResult:
                _outVecResult(str, (VecResult*)obj);
                break;

            case T_CStoreScan:
                _outCStoreScan(str, (CStoreScan*)obj);
                break;

            case T_DfsScan:
                _outDfsScan(str, (DfsScan*)obj);
                break;
#ifdef ENABLE_MULTIPLE_NODES
            case T_TsStoreScan:
                _outTsStoreScan(str, (TsStoreScan*)obj);
                break;
#endif   /* ENABLE_MULTIPLE_NODES */
            case T_VecSubqueryScan:
                _outVecSubqueryScan(str, (VecSubqueryScan*)obj);
                break;
            case T_VecPartIterator:
                _outVecPartIterator(str, (VecPartIterator*)obj);
                break;
            case T_VecModifyTable:
                _outVecModifyTable(str, (VecModifyTable*)obj);
                break;
            case T_VecForeignScan:
                _outVecForeignScan(str, (VecForeignScan*)obj);
                break;

            case T_VecHashJoin:
                _outVecHashJoin(str, (VecHashJoin*)obj);
                break;

            case T_VecAgg:
                _outVecHashAgg(str, (VecAgg*)obj);
                break;
            case T_VecAppend:
                _outVecAppend(str, (VecAppend*)obj);
                break;
            case T_VecLimit:
                _outVecLimit(str, (VecLimit*)obj);
                break;

            case T_VecStream:
                _outVecStream(str, (VecStream*)obj);
                break;
#ifdef PGXC
            case T_VecRemoteQuery:
                _outVecRemoteQuery(str, (VecRemoteQuery*)obj);
                break;
#endif
            case T_VecGroup:
                _outVecGroup(str, (VecGroup*)obj);
                break;
            case T_VecUnique:
                _outVecUnique(str, (VecUnique*)obj);
                break;
            case T_VecSetOp:
                _outVecSetOp(str, (VecSetOp*)obj);
                break;
            case T_VecNestLoop:
                _outVecNestLoop(str, (VecNestLoop*)obj);
                break;
            case T_VecMaterial:
                _outVecMaterial(str, (VecMaterial*)obj);
                break;

            case T_VecMergeJoin:
                _outVecMergeJoin(str, (VecMergeJoin*)obj);
                break;

            case T_VecWindowAgg:
                _outVecWindowAgg(str, (VecWindowAgg*)obj);
                break;
            case T_InformationalConstraint:
                _outInformationalConstraint(str, (InformationalConstraint*)obj);
                break;
            case T_AttrMetaData:
                _outAttrMetaData(str, (AttrMetaData*)obj);
                break;
            case T_RelationMetaData:
                _outRelationMetaData(str, (RelationMetaData*)obj);
                break;
            case T_ForeignOptions:
                _outForeignOptions(str, (ForeignOptions*)obj);
                break;
            case T_BloomFilterSet:
                _outBloomFilterSet(str, (BloomFilterSet*)obj);
                break;
            case T_HintState:
                _outHintState(str, (HintState*)obj);
                break;
            case T_JoinMethodHint:
                _outJoinHint(str, (JoinMethodHint*)obj);
                break;
            case T_LeadingHint:
                _outLeadingHint(str, (LeadingHint*)obj);
                break;
            case T_RowsHint:
                _outRowsHint(str, (RowsHint*)obj);
                break;
            case T_StreamHint:
                _outStreamHint(str, (StreamHint*)obj);
                break;
            case T_BlockNameHint:
                _outBlockNameHint(str, (BlockNameHint*)obj);
                break;
            case T_ScanMethodHint:
                _outScanMethodHint(str, (ScanMethodHint*)obj);
                break;
            case T_PgFdwRemoteInfo:
                _outPgFdwRemoteInfo(str, (PgFdwRemoteInfo*)obj);
                break;
            case T_PurgeStmt:
                OutPurgeStmt(str, (PurgeStmt*)obj);
                break;
            case T_TimeCapsuleStmt:
                OutTimeCapsuleStmt(str, (TimeCapsuleStmt*)obj);
                break;
            case T_CommentStmt:
                _outCommentStmt(str, (CommentStmt*)obj);
                break;
            case T_TableLikeCtx:
                _outTableLikeCtx(str, (TableLikeCtx*)obj);
                break;
            case T_SkewHint:
                _outSkewHint(str, (SkewHint*)obj);
                break;
            case T_SkewHintTransf:
                _outSkewHintTransf(str, (SkewHintTransf*)obj);
                break;
            case T_SkewRelInfo:
                _outSkewRelInfo(str, (SkewRelInfo*)obj);
                break;
            case T_SkewColumnInfo:
                _outSkewColumnInfo(str, (SkewColumnInfo*)obj);
                break;
            case T_SkewValueInfo:
                _outSkewValueInfo(str, (SkewValueInfo*)obj);
                break;
            case T_QualSkewInfo:
                _outQualSkewInfo(str, (QualSkewInfo*)obj);
                break;
            case T_IndexVar:
                _outIndexVar(str, (IndexVar*)obj);
                break;
            case T_PredpushHint:
                _outPredpushHint(str, (PredpushHint *)obj);
                break;
            case T_PredpushSameLevelHint:
                _outPredpushSameLevelHint(str, (PredpushSameLevelHint *)obj);
                break;
            case T_RewriteHint:
                _outRewriteHint(str, (RewriteHint *)obj);
                break;
            case T_GatherHint:
                _outGatherHint(str, (GatherHint *)obj);
                break;
            case T_NoExpandHint:
                _outNoExpandHint(str, (NoExpandHint*) obj);
                break;
            case T_SetHint:
                _outSetHint(str, (SetHint*) obj);
                break;
            case T_PlanCacheHint:
                _outPlanCacheHint(str, (PlanCacheHint*) obj);
                break;
            case T_NoGPCHint:
                _outNoGPCHint(str, (NoGPCHint*) obj);
            case T_TrainModel:
                _outTrainModel(str, (TrainModel*)obj);
                break;
            case T_PLDebug_variable:
                _outPLDebug_variable(str, (PLDebug_variable*) obj);
                break;
            case T_PLDebug_breakPoint:
                _outPLDebug_breakPoint(str, (PLDebug_breakPoint*) obj);
                break;
            case T_PLDebug_frame:
                _outPLDebug_frame(str, (PLDebug_frame*) obj);
                break;
            default:

                /*
                 * This should be an ERROR, but it's too useful to be able to
                 * dump structures that _outNode only understands part of.
                 */
                elog(WARNING, "_outNode() could not dump unrecognized node type: %d", (int)nodeTag(obj));
                break;
        }
        appendStringInfoChar(str, '}');
    }
}

/*
 * nodeToString -
 *	   returns the ascii representation of the Node as a palloc'd string
 */
char* nodeToString(const void* obj)
{
    StringInfoData str;

    /* see stringinfo.h for an explanation of this maneuver */
    initStringInfo(&str);
    _outNode(&str, obj);
    return str.data;
}

/*
 * appendBitmapsetToString -
 *	   append the ascii representation of bitmap set to a palloc'd string
 */
void appendBitmapsetToString(void* str, void* bms)
{
    _outBitmapset((StringInfo)str, (Bitmapset*)bms);
}

/*
 * out_mem_info -
 *	   returns the ascii representation of the OpMemInfo structure
 */
static void out_mem_info(StringInfo str, OpMemInfo* node)
{
    WRITE_FLOAT_FIELD(opMem, "%.2f");
    WRITE_FLOAT_FIELD(minMem, "%.2f");
    WRITE_FLOAT_FIELD(maxMem, "%.2f");
    WRITE_FLOAT_FIELD(regressCost, "%.6f");
}

/*
 * _outCursorData -
 *	   returns the ascii representation of the Cursor_Data structure
 */
static void _outCursorData(StringInfo str, Cursor_Data* node)
{
    WRITE_INT_FIELD(row_count);
    WRITE_INT_FIELD(cur_dno);
    WRITE_BOOL_FIELD(is_open);
    WRITE_BOOL_FIELD(found);
    WRITE_BOOL_FIELD(not_found);
    WRITE_BOOL_FIELD(null_open);
    WRITE_BOOL_FIELD(null_fetch);
}
