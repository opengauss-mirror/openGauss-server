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
 * ---------------------------------------------------------------------------------------
 *
 * opfusion.h
 *        Declaration of base class opfusion for bypass executor.
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
#include "gs_ledger/userchain.h"
#include "lib/stringinfo.h"
#include "nodes/parsenodes.h"
#include "opfusion/opfusion_util.h"
#include "pgxc/pgxcnode.h"
#include "utils/numeric.h"
#include "utils/numeric_gs.h"
#include "utils/plancache.h"
#include "utils/syscache.h"

class OpFusion;
typedef unsigned long (OpFusion::*OpFusionExecfuncType)(Relation rel, ResultRelInfo* resultRelInfo);

extern void report_qps_type(CmdType commandType);
extern void ExecCheckXactReadOnly(PlannedStmt* plannedstmt);

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

    void fusionExecute(StringInfo msg, char *completionTag, bool isTopLevel, bool *isQueryCompleted);

    void CheckLogDuration();

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
        OpFusionExecfuncType m_exec_func_ptr;
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

        bool m_ledger_hash_exist;

        uint64 m_ledger_relhash;

        FusionType m_optype;

        ResourceOwner m_resOwner;
    };

    OpFusionLocaleVariable m_local;
private:
#ifdef ENABLE_MOT
    static FusionType GetMotFusionType(PlannedStmt* plannedStmt);
#endif
};

#endif /* SRC_INCLUDE_OPFUSION_OPFUSION_H_ */
