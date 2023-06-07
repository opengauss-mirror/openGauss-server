/* -------------------------------------------------------------------------
 * ndpoutfuncs.cpp
 *	  Routine to serialize PlanState information
 *
 * Portions Copyright (c) 2022 Huawei Technologies Co.,Ltd.
 *
 * IDENTIFICATION
 *	  contrib/ndpplugin/ndpoutfuncs.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "ndpnodes.h"

enum class NdpStateType {
    // BaseType
    BOOL,
    CHAR,
    INT32,
    UINT32,
    INT64,
    UINT64,

    // NodeType
    RELATION,
    RELFILENODE,
    TUPLEDESC,
    PGATTR,
    XACT,
    AGGSTATE,
    PARAMLIST,
    PARAMDATA,
    SESSIONCONTEXT,
    ILLEGAL
};

static const char* NdpNodeNames[] {
    // BaseType
    "BOOL",
    "CHAR",
    "INT32",
    "UINT32",
    "INT64",
    "UINT64",

    // NodeType
    "RELATION",
    "RELFILENODE",
    "TUPLEDESC",
    "PGATTR",
    "XACT",
    "AGGSTATE",
    "PARAMLIST",
    "PARAMDATA",
    "SESSIONCONTEXT"
};

#define booltostr(x) ((x) ? "true" : "false")

// for performance
#define WRITE_BOOL_FIELD(fldname) appendStringInfo(str, " :" CppAsString(fldname) " %s", booltostr(node->fldname))

/* Write a char field (ie, one ascii character) */
#define WRITE_CHAR_FIELD(fldname) appendStringInfo(str, " :" CppAsString(fldname) " %c", node->fldname)

/* Write an integer field (anything written as ":fldname %d") */
#define WRITE_INT_FIELD(fldname) appendStringInfo(str, " :" CppAsString(fldname) " %d", node->fldname)

/* Write an unsigned integer field (anything written as ":fldname %u") */
#define WRITE_UINT_FIELD(fldname) appendStringInfo(str, " :" CppAsString(fldname) " %u", node->fldname)

/* Write an 64bit unsigned integer field (anything written as ":fldname %lu") */
#define WRITE_UINT64_FIELD(fldname) appendStringInfo(str, " :" CppAsString(fldname) " %lu", node->fldname)

/* Write an OID field (don't hard-wire assumption that OID is same as uint) */
#define WRITE_OID_FIELD(fldname) appendStringInfo(str, " :" CppAsString(fldname) " %u", node->fldname)

#define WRITE_BASE_ARRAY(fldname, fldlen, fldtype) \
    (appendStringInfo(str, " :" CppAsString(fldname) " "), _outBaseArray(str, node->fldname, node->fldlen, fldtype))

#define WRITE_UINT_ARRAY_LEN(fldname, len) \
    (appendStringInfo(str, " :" CppAsString(fldname) " "), \
        _outBaseArray(str, node->fldname, len, NdpStateType::UINT32))

#define WRITE_CHAR_ARRAY(fldname, fldlen) \
    (appendStringInfo(str, " :" CppAsString(fldname) " "), appendBinaryStringInfo(str, node->fldname, node->fldlen))

/* Write a Node field */
#define WRITE_NODE_FIELD(fldname, fldtype) \
    (appendStringInfo(str, " :" CppAsString(fldname) " "), _outNode(str, &node->fldname, fldtype))

#define WRITE_NODE_ARRAY(fldname, fldlen, fldtype) \
    (appendStringInfo(str, " :" CppAsString(fldname) " "), _outNodeArray(str, node->fldname, node->fldlen, fldtype))

static void _outNode(StringInfo str, void* obj, NdpStateType type);
static void _outNodeArray(StringInfo str, void* node, int len, NdpStateType type);

static void _outBaseArray(StringInfo str, void* node, int len, NdpStateType type)
{
    appendStringInfoChar(str, '(');

    for (int i = 0; i < len; ++i) {
        switch (type) {
            case NdpStateType::BOOL:
                appendStringInfo(str, "%s", booltostr(((bool*)node)[i]));
                break;
            case NdpStateType::INT32:
                appendStringInfo(str, "%d", ((int32*)node)[i]);
                break;
            case NdpStateType::UINT32:
                appendStringInfo(str, "%u", ((uint32*)node)[i]);
                break;
            case NdpStateType::INT64:
                appendStringInfo(str, "%ld", ((int64*)node)[i]);
                break;
            case NdpStateType::UINT64:
                appendStringInfo(str, "%lu", ((uint64*)node)[i]);
                break;
            default:
                break;
        }

        if (i + 1 != len) {
            appendStringInfoChar(str, ' ');
        }
    }

    appendStringInfoChar(str, ')');
}

static void _outNdpRelFileNode(StringInfo str, NdpRelFileNode* node)
{
    WRITE_UINT_FIELD(spcNode);
    WRITE_UINT_FIELD(dbNode);
    WRITE_UINT_FIELD(relNode);
    WRITE_UINT_FIELD(bucketNode);
    WRITE_UINT_FIELD(opt);
}

static void _outNdpPGAttr(StringInfo str, NdpPGAttr* node)
{
    WRITE_INT_FIELD(attlen);
    WRITE_BOOL_FIELD(attbyval);
    WRITE_INT_FIELD(attcacheoff);
    WRITE_CHAR_FIELD(attalign);
    WRITE_INT_FIELD(attndims);
    WRITE_CHAR_FIELD(attstorage);
}

static void _outNdpTupleDesc(StringInfo str, NdpTupleDesc* node)
{
    WRITE_INT_FIELD(natts);
    WRITE_NODE_ARRAY(attrs, natts, NdpStateType::PGATTR);
    WRITE_BOOL_FIELD(tdhasoid);
    WRITE_BOOL_FIELD(tdhasuids);
    WRITE_OID_FIELD(tdtypeid);
    WRITE_INT_FIELD(tdtypmod);
}

static void _outNdpRelation(StringInfo str, NdpRelation* node)
{
    WRITE_NODE_FIELD(node, NdpStateType::RELFILENODE);
    WRITE_NODE_FIELD(att, NdpStateType::TUPLEDESC);
}

static void _outNdpSnapshot(StringInfo str, NdpSnapshot* node)
{
    WRITE_UINT_FIELD(satisfies);
    WRITE_UINT64_FIELD(xmin);
    WRITE_UINT64_FIELD(xmax);
    WRITE_UINT64_FIELD(snapshotcsn);
    WRITE_UINT_FIELD(curcid);
}

static void _outNdpXact(StringInfo str, NdpXact* node)
{
    _outNdpSnapshot(str, &node->snapshot);
    WRITE_UINT64_FIELD(transactionId);
    WRITE_INT_FIELD(usedComboCids);
    WRITE_UINT_ARRAY_LEN(comboCids, node->usedComboCids * 2);
    WRITE_UINT64_FIELD(latestCompletedXid);
    WRITE_INT_FIELD(CLogLen);
    WRITE_CHAR_ARRAY(CLogPageBuffer, CLogLen);
    WRITE_INT_FIELD(CSNLogLen);
    WRITE_CHAR_ARRAY(CSNLogPageBuffer, CSNLogLen);
}

static void _outNdpAggState(StringInfo str, NdpAggState* node)
{
    WRITE_NODE_FIELD(aggTd, NdpStateType::TUPLEDESC);
    WRITE_INT_FIELD(aggNum);
    WRITE_NODE_ARRAY(perAggTd, aggNum, NdpStateType::TUPLEDESC);
    WRITE_INT_FIELD(numCols);
    WRITE_BASE_ARRAY(eqFuncOid, numCols, NdpStateType::UINT32);
    WRITE_BASE_ARRAY(hashFuncOid, numCols, NdpStateType::UINT32);
}
static void _outNdpParamList(StringInfo str, NdpParamList* node)
{
    WRITE_INT_FIELD(numParams);
    WRITE_NODE_ARRAY(params, numParams, NdpStateType::PARAMDATA);
}

static void _outNdpSessionContext(StringInfo str, NdpSessionContext* node)
{
    WRITE_INT_FIELD(sql_compatibility);
    WRITE_BOOL_FIELD(behavior_compat_flags);
    WRITE_INT_FIELD(encoding);
}

Size datumGetSize(Datum value, bool typByVal, int typLen)
{
    Size size;

    if (typByVal) {
        /* Pass-by-value types are always fixed-length */
        Assert(typLen > 0 && (unsigned int)(typLen) <= sizeof(Datum));
        size = (Size)typLen;
    } else {
        if (typLen > 0) {
            /* Fixed-length pass-by-ref type */
            size = (Size)typLen;
        } else if (typLen == -1) {
            /* It is a varlena datatype */
            struct varlena* s = (struct varlena*)DatumGetPointer(value);

            if (!PointerIsValid(s))
                ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION), errmsg("invalid Datum pointer")));

            size = (Size)VARSIZE_ANY(s);
        } else if (typLen == -2) {
            /* It is a cstring datatype */
            char* s = (char*)DatumGetPointer(value);

            if (!PointerIsValid(s))
                ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION), errmsg("invalid Datum pointer")));

            size = (Size)(strlen(s) + 1);
        } else {
            ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED), errmsg("invalid typLen: %d", typLen)));
            size = 0; /* keep compiler quiet */
        }
    }

    return size;
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
static void _outNdpParam(StringInfo str, NdpParamData* node)
{
    WRITE_BOOL_FIELD(isnull);
    WRITE_OID_FIELD(ptype);
    WRITE_INT_FIELD(typlen);
    WRITE_BOOL_FIELD(typbyval);
    appendStringInfo(str, " :value ");
    if (node->isnull) {
        /* null value */
        appendStringInfo(str, "<>");
    } else {
        _outDatum(str, node->value, node->typlen, node->typbyval);
    }
}
static void _outNode(StringInfo str, void* obj, NdpStateType type)
{
    if (obj == nullptr) {
        appendStringInfo(str, "<>");
    } else {
        appendStringInfoChar(str, '{');
        if (type > NdpStateType::ILLEGAL) {
            return;
        } else {
            appendStringInfoString(str, NdpNodeNames[static_cast<int>(type)]);
        }
        switch (type) {
            case NdpStateType::RELATION:
                _outNdpRelation(str, reinterpret_cast<NdpRelation*>(obj));
                break;
            case NdpStateType::RELFILENODE:
                _outNdpRelFileNode(str, reinterpret_cast<NdpRelFileNode*>(obj));
                break;
            case NdpStateType::TUPLEDESC:
                _outNdpTupleDesc(str, reinterpret_cast<NdpTupleDesc*>(obj));
                break;
            case NdpStateType::PGATTR:
                _outNdpPGAttr(str, reinterpret_cast<NdpPGAttr*>(obj));
                break;
            case NdpStateType::XACT:
                _outNdpXact(str, reinterpret_cast<NdpXact*>(obj));
                break;
            case NdpStateType::AGGSTATE:
                _outNdpAggState(str, reinterpret_cast<NdpAggState*>(obj));
                break;
            case NdpStateType::PARAMLIST:
                _outNdpParamList(str, reinterpret_cast<NdpParamList*>(obj));
                break;
            case NdpStateType::PARAMDATA:
                _outNdpParam(str, reinterpret_cast<NdpParamData*>(obj));
                break;
            case NdpStateType::SESSIONCONTEXT:
                _outNdpSessionContext(str, reinterpret_cast<NdpSessionContext*>(obj));
            default:
                break;
        }
        appendStringInfoChar(str, '}');
    }
}

static void _outNodeArray(StringInfo str, void* node, int len, NdpStateType type)
{
    appendStringInfoChar(str, '(');

    void* item = node;
    for (int i = 0; i < len; ++i) {
        switch (type) {
            case NdpStateType::RELATION:
                item = reinterpret_cast<NdpRelation*>(node) + i;
                break;
            case NdpStateType::RELFILENODE:
                item = reinterpret_cast<NdpRelFileNode*>(node) + i;
                break;
            case NdpStateType::TUPLEDESC:
                item = reinterpret_cast<NdpTupleDesc*>(node) + i;
                break;
            case NdpStateType::PGATTR:
                item = reinterpret_cast<NdpPGAttr*>(node) + i;
                break;
            case NdpStateType::XACT:
                item = reinterpret_cast<NdpXact*>(node) + i;
                break;
            case NdpStateType::PARAMDATA:
                item = reinterpret_cast<NdpParamData*>(node) + i;
                break;
            default:
                break;
        }
        _outNode(str, item, type);
        if (i + 1 != len) {
            appendStringInfoChar(str, ' ');
        }
    }

    appendStringInfoChar(str, ')');
}

void stateToString(NdpPlanState* node, StringInfo str)
{
    _outNode(str, &node->rel, NdpStateType::RELATION);
    _outNode(str, &node->scanTd, NdpStateType::TUPLEDESC);
    _outNode(str, &node->aggState, NdpStateType::AGGSTATE);
    _outNode(str, &node->paramList, NdpStateType::PARAMLIST);
    _outNode(str, &node->sess, NdpStateType::SESSIONCONTEXT);
}

void queryToString(NdpQuery* node, StringInfo str)
{
    WRITE_INT_FIELD(tableNum);
    _outNode(str, &node->xact, NdpStateType::XACT);
}
