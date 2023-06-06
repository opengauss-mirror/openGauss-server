/* -------------------------------------------------------------------------
 *
 * ndp_nodes.h
 *	  Exports for plan
 *
 * src/include/ndp/ndp_nodes.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef LIBSMARTSCAN_NDP_NODES_H
#define LIBSMARTSCAN_NDP_NODES_H

#ifdef NDP_CLIENT
typedef uintptr_t Datum;
#include "access/attnum.h"
#include "nodes/params.h"
#include "nodes/primnodes.h"
#else
#include "nodes/params.h"
#include "nodes/primnodes.h"
#include "utils/snapshot.h"
#endif

enum init_type { INIT_SUCCESS, INIT_FAIL};

/***************************for NdpScanPage*******************************/
typedef enum NdpScanPageFlag {
    NORMAL_PAGE = 0,
    NDP_FILTERED_PAGE,
    NDP_AGG_PAGE,
    INVALID_PAGE
} NdpScanPageFlag;

/***************************for PlanState*********************************/
typedef struct NdpPGAttr {
    int2 attlen;
    int4 attndims;
    int4 attcacheoff;
    bool attbyval;
    char attstorage;
    char attalign;
} NdpPGAttr;

typedef struct NdpTupleDesc {
    int natts;        /* number of attributes in the tuple */
    NdpPGAttr* attrs;
    Oid tdtypeid;     /* composite type ID for tuple type */
    int32 tdtypmod;   /* typmod for tuple type */
    bool tdhasoid;    /* tuple has oid attribute in its header */
    bool tdhasuids;   /* tuple has uid attribute in its header */
} NdpTupleDesc;

typedef struct NdpRelFileNode {
    uint32 spcNode;
    uint32 dbNode;
    uint32 relNode;
    uint16 bucketNode;
    uint16 opt;
} NdpRelFileNode;

typedef struct NdpRelation {
    NdpRelFileNode node;
    NdpTupleDesc att;
} NdpRelation;

typedef struct NdpSnapshot {
    uint16 satisfies;
    uint64 xmin; /* all XID < xmin are visible to me */
    uint64 xmax; /* all XID >= xmax are invisible to me */
    uint64 snapshotcsn;
    uint32 curcid;
} NdpSnapshot;

typedef struct NdpXact {
    NdpSnapshot snapshot;
    uint64 transactionId; /* my XID, or Invalid if none */
    int usedComboCids; /* number of elements in comboCids */
    uint32 *comboCids; /* An array of cmin,cmax pairs, indexed by combo command id */
    uint64 latestCompletedXid; /* newest XID that has committed or aborted */
    int CLogLen;
    char* CLogPageBuffer;
    int CSNLogLen;
    char* CSNLogPageBuffer;
} NdpXact;

// context for one query
typedef struct NdpQuery {
    uint32 tableNum;
    NdpXact xact;
} NdpQuery;

typedef struct NdpAggState {
    NdpTupleDesc aggTd;
    int aggNum;
    NdpTupleDesc* perAggTd;
    int numCols;
    unsigned int* eqFuncOid;
    unsigned int* hashFuncOid;
} NdpAggState;

typedef struct NdpParamData {
    bool isnull;   /* is it NULL? */
    Oid ptype;     /* parameter's datatype, or 0 */
    int16 typlen;
    bool typbyval;
    Datum value;   /* parameter value */
} NdpParamData;

typedef struct NdpParamList {
    int numParams;
    NdpParamData* params;
} NdpParamList;

typedef struct NdpSessionContext {
    int sql_compatibility; /* belong to knl_session_attr_sql*/
    bool behavior_compat_flags; /* belong to knl_u_utils_context */
    int encoding;
} NdpSessionContext;

typedef struct NdpPlanState {
    NdpRelation rel;
    NdpTupleDesc scanTd;
    NdpAggState aggState;
    NdpParamList paramList;
    NdpSessionContext sess;
} NdpPlanState;

enum FileType {
    INVALIDFILE = -1,
    MDFILE = 0,
    SEGFILE = 1,
};

#endif //LIBSMARTSCAN_NDP_NODES_H
