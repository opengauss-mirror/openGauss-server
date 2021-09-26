/*
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 2002-2007, PostgreSQL Global Development Group
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
 * -------------------------------------------------------------------------
 *
 * globalplancache.h
 *
 * IDENTIFICATION
 *        src/include/utils/globalplancache.h
 *
 *---------------------------------------------------------------------------------------
 */
#ifndef GLOBALPLANCACHE_H
#define GLOBALPLANCACHE_H

#include "knl/knl_variable.h"

#include "pgxc/pgxc.h"
#include "storage/sinval.h"
#include "utils/globalplancore.h"
#include "executor/spi.h"

enum  PlansourceInvalidAction
{
    ACTION_RECREATE,
    ACTION_RELOAD,
    ACTION_CN_RETRY
};

class GlobalPlanCache : public BaseObject
{
public:
    GlobalPlanCache();
    ~GlobalPlanCache();
    
    /* global plan cache htab control */
    void Init();
    CachedPlanSource* Fetch(const char *query_string, uint32 query_len, int num_params,
                            Oid* paramTypes, SPISign* spi_sign_ptr);
    void DropInvalid();
    void AddInvalidList(CachedPlanSource* plansource);
    void RemoveEntry(uint32 htblIdx, GPCEntry *entry);
    template<PlansourceInvalidAction action_type>
    void RemovePlanSource(CachedPlanSource* plansource, const char* stmt_name);
    void MoveIntoInvalidPlanList(CachedPlanSource* psrc);
    bool TryStore(CachedPlanSource *plansource,  PreparedStatement *ps);
    Datum PlanClean();
    void CleanUpByTime();
    /* cnretry and pool reload invalid */
    void InvalidPlanSource(CachedPlanSource* plansource, const char* stmt_name, bool clear_dnstmt);

    /* cache invalid */
    void InvalMsg(const SharedInvalidationMessage *msgs, int n);

    /* transaction */
    void RecreateCachePlan(CachedPlanSource* oldsource, const char* stmt_name,
                           PreparedStatement *entry, SPIPlanPtr spiplan, ListCell* spiplanCell, bool hasGetLock);
    void Commit();
    bool CheckRecreateCachePlan(CachedPlanSource* psrc, bool* hasGetLock);

    void CNCommit();
    void DNCommit();

    /* system function */
    void* GetStatus(uint32 *num);

    static bool MsgCheck(const SharedInvalidationMessage *msg);
    static bool NeedDropEntryByLocalMsg(CachedPlanSource* plansource, int tot, const int *idx, const SharedInvalidationMessage *msgs);
    static void GetSchemaName(GPCEnv *env);
    static void EnvFill(GPCEnv *env, bool depends_on_role);
    static void FillClassicEnvSignatures(GPCEnv *env);
    static void FillEnvSignatures(GPCEnv *env);

    /* for spi */
    void SPICommit(CachedPlanSource* plansource);
    void SPITryStore(CachedPlanSource* plansource, SPIPlanPtr spi_plan, int nth);
    bool CheckRecreateSPICachePlan(SPIPlanPtr spi_plan);
    void RecreateSPICachePlan(SPIPlanPtr spi_plan);
    void RemovePlanCacheInSPIPlan(SPIPlanPtr plan);

private:

    GPCHashCtl *m_array;

    DList *m_invalid_list;
};



#endif   /* PLANCACHE_H */
