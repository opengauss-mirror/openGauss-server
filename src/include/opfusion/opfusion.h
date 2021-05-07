/*
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1998-2016, PostgreSQL Global Development Group
 *
 * ---------------------------------------------------------------------------------------
 * 
 * opfusion.h
 *     Operator Fusion's definition for bypass.
 * 
 * IDENTIFICATION
 *        src/include/opfusion/opfusion.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef SRC_INCLUDE_OPFUSION_OPFUSION_H_
#define SRC_INCLUDE_OPFUSION_OPFUSION_H_
#include "auditfuncs.h"
#include "commands/prepare.h"
#include "opfusion/opfusion_util.h"
#include "opfusion/opfusion_scan.h"
#include "lib/stringinfo.h"
#include "nodes/parsenodes.h"
#include "pgxc/pgxcnode.h"
#include "utils/numeric.h"
#include "utils/numeric_gs.h"
#include "utils/plancache.h"
#include "utils/syscache.h"

extern void report_qps_type(CmdType commandType);
extern const char* getBypassReason(FusionType result);
extern void BypassUnsupportedReason(FusionType result);
extern void ExecCheckXactReadOnly(PlannedStmt* plannedstmt);
void InitParamInFusionConstruct(const TupleDesc tupDesc, Datum** values, bool** isNull);
extern FusionType getSelectFusionType(List* stmt_list, ParamListInfo params);
extern FusionType getInsertFusionType(List* stmt_list, ParamListInfo params);
extern FusionType getUpdateFusionType(List* stmt_list, ParamListInfo params);
extern FusionType getDeleteFusionType(List* stmt_list, ParamListInfo params);
extern void tpslot_free_heaptuple(TupleTableSlot* reslot);

typedef struct pnFusionObj {
    char portalname[NAMEDATALEN];
    OpFusion *opfusion;
} pnFusionObj;

#define HASH_TBL_LEN 64

/*
 * The variables in OpFusion is always in two parts: global's variables and local's variables.
 * Global variable means it can be shared in each session.
 * Local variable means it will be change in local session, so it cannot be shared.
 *
 * Global variables be saved into struct OpFusionGlobalVariable, and we access the global variables
 * from pointer m_global, and m_global's mem context is under global cachedplansource.
 * Local variables be saved into struct OpFusionLocaleVariable, and we access the local variables
 * from object m_local, and m_local's context is under session context.
 */
class OpFusion : public BaseObject {
public:
    OpFusion(MemoryContext context, CachedPlanSource* psrc, List* plantree_list);

    virtual ~OpFusion(){};

    static void* FusionFactory(
        FusionType type, MemoryContext context, CachedPlanSource* psrc, List* plantree_list, ParamListInfo params);

    static FusionType getFusionType(CachedPlan* cplan, ParamListInfo params, List* plantree_list);

    static void setCurrentOpFusionObj(OpFusion* obj);

    static bool process(int op, StringInfo msg, char* completionTag, bool isTopLevel, bool* isQueryCompleted);

    static void SaveInGPC(OpFusion* obj);

    static void DropGlobalOpfusion(OpFusion* obj);

    void InitGlobals(MemoryContext context, CachedPlanSource* psrc, List* plantree_list);

    void InitLocals(MemoryContext context);

    void CopyFormats(int16* formats, int numRFormats);

    void updatePreAllocParamter(StringInfo msg);

    void useOuterParameter(ParamListInfo params);

    void describe(StringInfo msg);

    virtual bool execute(long max_rows, char* completionTag)
    {
        Assert(false);
        return false;
    }

    virtual void close()
    {
        Assert(false);
        return;
    }

    void copyGlobalOpfusionVar(OpFusion);
    void setPreparedDestReceiver(DestReceiver* preparedDest);

    Datum CalFuncNodeVal(Oid functionId, List* args, bool* is_null, Datum* values, bool* isNulls);

    Datum EvalSimpleArg(Node* arg, bool* is_null, Datum* values, bool* isNulls);

    static void tearDown(OpFusion* opfusion);

    static void clearForCplan(OpFusion* opfusion, CachedPlanSource* psrc);

    void checkPermission();

    void setReceiver();

    void initParams(ParamListInfo params);

    void executeInit();

    bool executeEnd(const char* portal_name, bool* completionTag);

    void auditRecord();

    void clean();

    static bool isQueryCompleted();

    void bindClearPosition();

    static void initFusionHtab();

    static void ClearInUnexpectSituation();

    static void ClearInSubUnexpectSituation(ResourceOwner owner);

    void storeFusion(const char *portalname);

    static OpFusion *locateFusion(const char *portalname);

    static void removeFusionFromHtab(const char *portalname);

    static void refreshCurFusion(StringInfo msg);

    inline bool IsGlobal()
    {
        pg_memory_barrier();
        return (m_global && m_global->m_is_global);
    }

public:
    struct ParamLoc {
        int paramId;
        int scanKeyIndx;
    };
    struct ConstLoc {
        Datum constValue;
        bool constIsNull;
        int constLoc;
    };
    /*
     * these variables can be shared, mem context on global plancache
     */
    struct OpFusionGlobalVariable {
        CachedPlanSource* m_psrc; /* to get m_cacheplan in PBE */
        
        CachedPlan* m_cacheplan;
        
        PlannedStmt* m_planstmt; /* m_cacheplan->stmt_list in PBE, plantree in non-PBE */
        
        MemoryContext m_context;
        
        bool m_is_pbe_query;
        
        ParamLoc* m_paramLoc; /* location of m_params, include paramId and the location in indexqual */
        
        Oid m_reloid; /* relation oid of range table */
        
        int m_paramNum;

        TableAmType m_table_type;
        
        int16* m_attrno; /* target attribute number, length is m_tupDesc->natts */
        
        bool m_is_bucket_rel;

        FusionType m_type;

        int m_natts;

        volatile bool m_is_global;

        TupleDesc m_tupDesc; /* tuple descriptor */

    };

    OpFusionGlobalVariable *m_global;
    
    /*
     * other variables need change each BE, mem context on session cache context
     */
    struct OpFusionLocaleVariable {
        MemoryContext m_localContext; /* use for local variables */

        MemoryContext m_tmpContext; /* use for tmp memory allocation. */
    
        bool m_isFirst; /* be true if is the fisrt execute in PBE */
    
        ParamListInfo m_outParams; /* use outer side parameter. */

        ParamListInfo m_params;

        TupleTableSlot* m_reslot; /* result slot */
    
        Datum* m_values;
    
        bool* m_isnull;
    
        Datum* m_tmpvals; /* for mapping m_values */
    
        bool* m_tmpisnull; /* for mapping m_isnull */
    
        DestReceiver* m_receiver;
    
        bool m_isInsideRec;
    
        int16* m_rformats;
    
        bool m_isCompleted;
    
        long m_position;
    
        const char *m_portalName;
    
        Snapshot m_snapshot;

        class ScanFusion* m_scan;

        ResourceOwner m_resOwner;
    };

    OpFusionLocaleVariable m_local;
private:
#ifdef ENABLE_MOT
    static FusionType GetMotFusionType(PlannedStmt* plannedStmt);
#endif
};

class SelectFusion : public OpFusion {
public:
    SelectFusion(MemoryContext context, CachedPlanSource* psrc, List* plantree_list, ParamListInfo params);

    ~SelectFusion(){};

    bool execute(long max_rows, char* completionTag);

    void close();

    void InitLocals(ParamListInfo params);

    void InitGlobals();
private:
    struct SelectFusionGlobalVariable {
        int64 m_limitCount;
        int64 m_limitOffset;
    };
    SelectFusionGlobalVariable* m_c_global;
};


class InsertFusion : public OpFusion {
public:
    InsertFusion(MemoryContext context, CachedPlanSource* psrc, List* plantree_list, ParamListInfo params);

    ~InsertFusion(){};

    bool execute(long max_rows, char* completionTag);

    void InitLocals(ParamListInfo params);

    void InitGlobals();
private:
    void refreshParameterIfNecessary();

    struct InsertFusionGlobalVariable {
        /* for func/op expr calculation */
        FuncExprInfo* m_targetFuncNodes;
        
        int m_targetFuncNum;
        
        int m_targetParamNum;

        int m_targetConstNum;

        ConstLoc* m_targetConstLoc;
    };
    InsertFusionGlobalVariable* m_c_global;

    struct InsertFusionLocaleVariable {
        EState* m_estate;
        Datum* m_curVarValue;
        bool* m_curVarIsnull;
    };

    InsertFusionLocaleVariable m_c_local;
};

class UpdateFusion : public OpFusion {
public:
    UpdateFusion(MemoryContext context, CachedPlanSource* psrc, List* plantree_list, ParamListInfo params);

    ~UpdateFusion(){};

    bool execute(long max_rows, char* completionTag);

    void InitLocals(ParamListInfo params);

    void InitGlobals();
private:
    HeapTuple heapModifyTuple(HeapTuple tuple);

    void refreshTargetParameterIfNecessary();


    struct VarLoc {
        int varNo;
        int scanKeyIndx;
    };
    struct UpdateFusionGlobalVariable {
        /* targetlist */
        int m_targetConstNum;
        
        int m_targetParamNum;
        
        VarLoc* m_targetVarLoc;
        
        int m_varNum;
        
        ConstLoc* m_targetConstLoc;
        
        ParamLoc* m_targetParamLoc;
        
        /* for func/op expr calculation */
        FuncExprInfo* m_targetFuncNodes;
        
        int m_targetFuncNum;
    };
    UpdateFusionGlobalVariable* m_c_global;

    struct UpdateFusionLocaleVariable {
        EState* m_estate;
        Datum* m_curVarValue;
        bool* m_curVarIsnull;
    };

    UpdateFusionLocaleVariable m_c_local;
};

class DeleteFusion : public OpFusion {
public:
    DeleteFusion(MemoryContext context, CachedPlanSource* psrc, List* plantree_list, ParamListInfo params);

    ~DeleteFusion(){};

    bool execute(long max_rows, char* completionTag);

    void InitLocals(ParamListInfo params);

    void InitGlobals();
private:
    struct DeleteFusionLocaleVariable {
        EState* m_estate;
    };

    DeleteFusionLocaleVariable m_c_local;
};

#ifdef ENABLE_MOT
class MotJitSelectFusion : public OpFusion {
public:
    MotJitSelectFusion(MemoryContext context, CachedPlanSource *psrc, List *plantree_list, ParamListInfo params);

    ~MotJitSelectFusion() {};

    bool execute(long max_rows, char *completionTag);

    void InitLocals(ParamListInfo params);

    void InitGlobals();
};

class MotJitModifyFusion : public OpFusion {
public:
    MotJitModifyFusion(MemoryContext context, CachedPlanSource *psrc, List *plantree_list, ParamListInfo params);

    ~MotJitModifyFusion() {};

    bool execute(long max_rows, char *completionTag);

    void InitLocals(ParamListInfo params);

    void InitGlobals();
private:
    struct MotJitModifyFusionLocaleVariable {
        CmdType m_cmdType;
        EState* m_estate;
    };
    MotJitModifyFusionLocaleVariable m_c_local;
};
#endif

class SelectForUpdateFusion : public OpFusion {
public:
    SelectForUpdateFusion(MemoryContext context, CachedPlanSource* psrc, List* plantree_list, ParamListInfo params);

    ~SelectForUpdateFusion(){};

    bool execute(long max_rows, char* completionTag);

    void close();

    void InitLocals(ParamListInfo params);

    void InitGlobals();
private:
    struct SelectForUpdateFusionGlobalVariable {
        int64 m_limitCount;
        int64 m_limitOffset;
    };
    SelectForUpdateFusionGlobalVariable* m_c_global;

    struct SelectForUpdateFusionLocaleVariable {
        EState* m_estate;
    };
    SelectForUpdateFusionLocaleVariable m_c_local;
};

class AggFusion : public OpFusion {

public:
    AggFusion(MemoryContext context, CachedPlanSource* psrc, List* plantree_list, ParamListInfo params);

    ~AggFusion(){};

    bool execute(long max_rows, char* completionTag);

    void InitLocals(ParamListInfo params);

    void InitGlobals();

protected:

    typedef void (AggFusion::*aggSumFun)(Datum *transVal, bool transIsNull, Datum *inVal, bool inIsNull);

    /* agg sum function */
    void agg_int2_sum(Datum *transVal, bool transIsNull, Datum *inVal, bool inIsNull);

    void agg_int4_sum(Datum *transVal, bool transIsNull, Datum *inVal, bool inIsNull);

    void agg_int8_sum(Datum *transVal, bool transIsNull, Datum *inVal, bool inIsNull);

    void agg_numeric_sum(Datum *transVal, bool transIsNull, Datum *inVal, bool inIsNull);

    inline void init_var_from_num(Numeric num, NumericVar *dest)
    {
        Assert(!NUMERIC_IS_BI(num));
        dest->ndigits = NUMERIC_NDIGITS(num);
        dest->weight = NUMERIC_WEIGHT(num);
        dest->sign = NUMERIC_SIGN(num);
        dest->dscale = NUMERIC_DSCALE(num);
        dest->digits = NUMERIC_DIGITS(num);
        dest->buf = NULL;       /* digits array is not palloc'd */
    }

    struct AggFusionGlobalVariable {
        aggSumFun m_aggSumFunc;
    };
    AggFusionGlobalVariable* m_c_global;
};

class SortFusion: public OpFusion {

public:

    SortFusion(MemoryContext context, CachedPlanSource* psrc, List* plantree_list, ParamListInfo params);

    ~SortFusion(){};

    bool execute(long max_rows, char *completionTag);

    void InitLocals(ParamListInfo params);

    void InitGlobals();

protected:

    struct SortFusionLocaleVariable {
        TupleDesc  m_scanDesc;
    };

    SortFusionLocaleVariable m_c_local;
};
#endif /* SRC_INCLUDE_OPFUSION_OPFUSION_H_ */
