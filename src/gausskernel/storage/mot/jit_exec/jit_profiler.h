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
 * -------------------------------------------------------------------------
 *
 * jit_profiler.h
 *    JIT LLVM.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/jit_exec/jit_profiler.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef JIT_PROFILER_H
#define JIT_PROFILER_H

#include "storage/mot/jit_def.h"
#include "c.h"
#include "pgstat.h"

#include <list>
#include <vector>
#include <mutex>
#include <condition_variable>
#include <thread>
#include <chrono>
#include <map>

#include "cycles.h"

namespace JitExec {

/** @typedef Vector of sub-query ids. */
using SubQueryIdArray = std::vector<uint32_t>;

/** @struct The meta-data for a single profiled function. */
class JitProfileFunctionMetaData {
public:
    /** @typedef Map of region name by id. */
    using ProfileRegionIdNameMap = std::map<uint32_t, std::string>;

    /** @brief Constructor. */
    JitProfileFunctionMetaData()
        : m_functionOid(InvalidOid),
          m_functionId(MOT_JIT_PROFILE_INVALID_FUNCTION_ID),
          m_parentFunctionId(MOT_JIT_PROFILE_INVALID_FUNCTION_ID),
          m_nameSpace(MOT_JIT_GLOBAL_QUERY_NS)
    {}

    JitProfileFunctionMetaData(Oid functionOid, uint32_t functionId)
        : m_functionOid(functionOid),
          m_functionId(functionId),
          m_parentFunctionId(MOT_JIT_PROFILE_INVALID_FUNCTION_ID),
          m_nameSpace(MOT_JIT_GLOBAL_QUERY_NS)
    {}

    JitProfileFunctionMetaData(Oid functionOid, uint32_t functionId, int parentFunctionId, const char* nameSpace)
        : m_functionOid(functionOid),
          m_functionId(functionId),
          m_parentFunctionId(parentFunctionId),
          m_nameSpace(nameSpace)
    {}

    inline Oid GetFunctionOid() const
    {
        return m_functionOid;
    }

    inline uint32_t GetFunctionId() const
    {
        return m_functionId;
    }

    inline uint32_t GetParentFunctionId() const
    {
        return m_parentFunctionId;
    }

    inline const char* GetNameSpace() const
    {
        return m_nameSpace.c_str();
    }

    inline void AddChildQuery(uint32_t queryId)
    {
        m_subQueryIdArray.push_back(queryId);
    }

    /** @brierf Retrieves profile region identifier by name (allocate on-demand). */
    uint32_t GetProfileRegionDataId(const char* name);

    inline uint32_t GetTotalRegionId() const
    {
        return FindProfileRegionId(MOT_JIT_PROFILE_REGION_TOTAL);
    }

    inline uint32_t GetDefVarsRegionId() const
    {
        return FindProfileRegionId(MOT_JIT_PROFILE_REGION_DEF_VARS);
    }

    inline uint32_t GetInitVarsRegionId() const
    {
        return FindProfileRegionId(MOT_JIT_PROFILE_REGION_INIT_VARS);
    }

    inline uint32_t GetChildCallRegionId() const
    {
        return FindProfileRegionId(MOT_JIT_PROFILE_REGION_CHILD_CALL);
    }

    uint32_t FindProfileRegionId(const char* name) const;

    /** @brief Get reference to sub-query id array. */
    inline const SubQueryIdArray& GetSubQueryIdArray() const
    {
        return m_subQueryIdArray;
    }

    inline const ProfileRegionIdNameMap& GetRegionIdNameMap() const
    {
        return m_profileRegionIdNameMap;
    }

private:
    /** @var The unique identifier of the function in pg_proc. */
    Oid m_functionOid;

    /** @var The profiled function identifier. */
    uint32_t m_functionId;

    /** @var The profile data identifier of the parent function. */
    uint32_t m_parentFunctionId;

    /** @var The name-space of the profiled function/query. */
    std::string m_nameSpace;

    /** @typedef Map of region identifier by name. */
    using ProfileRegionIdMap = std::map<std::string, uint32_t>;

    /** @var The map of region identifier by name. */
    ProfileRegionIdMap m_profileRegionIdMap;

    /** @var The map of region name by identifier. */
    ProfileRegionIdNameMap m_profileRegionIdNameMap;

    /** @var The id array of sub-queries of this function. */
    SubQueryIdArray m_subQueryIdArray;
};

/** @struct The profile data for a single profile region. */
struct JitProfileRegionData {
    JitProfileRegionData() : m_accumulatedTime(0), m_nextStartTime(0), m_sampleCount(0)
    {}

    /** @var Total accumulated time of all samples. */
    uint64_t m_accumulatedTime;

    /** @var Start time of next sample. */
    uint64_t m_nextStartTime;

    /** @var Total number of samples. */
    uint64_t m_sampleCount;
};

/** @class JitProfileFunctionData Profile data for a single function. */
class JitProfileFunctionData {
public:
    /** @brief Constructor. */
    JitProfileFunctionData()
        : m_functionOid(InvalidOid),
          m_functionId(MOT_JIT_PROFILE_INVALID_FUNCTION_ID),
          m_parentFunctionId(MOT_JIT_PROFILE_INVALID_FUNCTION_ID),
          m_nameSpace(MOT_JIT_GLOBAL_QUERY_NS),
          m_childGrossTime(0),
          m_childNetTime(0),
          m_totalRegionId(MOT_JIT_PROFILE_INVALID_REGION_ID)
    {}

    /** @brief Destructor. */
    virtual ~JitProfileFunctionData()
    {}

    inline Oid GetFunctionOid() const
    {
        return m_functionOid;
    }

    inline uint32_t GetFunctionId() const
    {
        return m_functionId;
    }

    inline uint32_t GetParentFunctionId() const
    {
        return m_parentFunctionId;
    }

    inline const char* GetNamespace() const
    {
        return m_nameSpace.c_str();
    }

    inline uint32_t GetTotalSampleCount() const
    {
        uint32_t sampleCount = 0;
        if ((m_totalRegionId != MOT_JIT_PROFILE_INVALID_REGION_ID) &&
            (m_totalRegionId < m_profileRegionDataArray.size())) {
            sampleCount = m_profileRegionDataArray[m_totalRegionId].m_sampleCount;
        }
        return sampleCount;
    }

    void CopySubQueryIdArray(const SubQueryIdArray& rhs);

    /** @var Mark start time of next sample in a profile region. */
    inline void StartProfileRegionSample(uint32_t id)
    {
        if (id >= m_profileRegionDataArray.size()) {
            m_profileRegionDataArray.resize(id + 1);
        }
        m_profileRegionDataArray[id].m_nextStartTime = GetSysClock();
    }

    /** @var Mark end time of next sample in a profile region. */
    inline void EndProfileRegionSample(uint32_t id)
    {
        uint64_t endTime = GetSysClock();
        JitProfileRegionData& pd = m_profileRegionDataArray[id];
        pd.m_accumulatedTime += (endTime - pd.m_nextStartTime);
        pd.m_nextStartTime = 0;
        ++pd.m_sampleCount;
    }

    void CopyMetaData(JitProfileFunctionMetaData* sourceData);

    /** @brief Accumulate profile data into this object. */
    void Accumulate(const JitProfileFunctionData& rhs);

    /** @brief Accumulate profile data of child-query into this object. */
    void AccumulateChild(const JitProfileFunctionData& rhs);

    void Dump();

    /** @brief Issue profile data report for this object. */
    void Report(const char* functionName, const JitProfileFunctionMetaData& profileMetaData);

    /** @brief Issue profile data entry for this object. */
    void Report(MotJitProfile* entry, uint32_t defVarsRegionId, uint32_t initVarsRegionId, uint32_t childCallRegionId);

    /** @brief Get reference to sub-query id array. */
    inline const SubQueryIdArray& GetSubQueryIdArray() const
    {
        return m_subQueryIdArray;
    }

private:
    /** @var The unique identifier of the function in pg_proc. */
    Oid m_functionOid;

    /** @var The profiled function identifier. */
    uint32_t m_functionId;

    /** @var The profile data identifier of the parent function. */
    uint32_t m_parentFunctionId;

    /** @var The name-space of the profiled function/query. */
    std::string m_nameSpace;

    /** @typedef Array of profile data per region. */
    using ProfileRegionDataArray = std::vector<JitProfileRegionData>;

    /** @var The run-time profile data. */
    ProfileRegionDataArray m_profileRegionDataArray;

    /** @var The id array of sub-queries of this function. */
    SubQueryIdArray m_subQueryIdArray;

    /** @var Accumulation of child query profile data. */
    uint64_t m_childGrossTime;

    /** @var Accumulation of child query profile data. */
    uint64_t m_childNetTime;

    /** @var The total region identifier. */
    uint32_t m_totalRegionId;
};

/** @class JitProfiler A run-time profiler for jitted stored procedures. */
class JitProfiler {
public:
    /**
     * @brief Creates the single instance of the profiler.
     * @return True if instance creation succeeded, otherwise false.
     */
    static bool CreateInstance();

    /** @brief Destroys the single instance of the profiler. */
    static void DestroyInstance();

    /** @brief Retrieves a reference to the single instance of the profiler. */
    static JitProfiler* GetInstance();

    /** @brief Retrieves the function identifier by name (allocate on-demand). */
    uint32_t GetProfileFunctionId(const char* functionName, Oid functionOid);

    /** @brief Retrieves the query identifier by name (allocate on-demand). */
    uint32_t GetProfileQueryId(const char* nameSpace, const char* queryString);

    /** @brief Retrieves the profile region identifier by name (allocate on-demand). */
    uint32_t GetProfileRegionId(const char* functionName, const char* regionName);

    /** @brief Retrieves the profile region identifier by name (allocate on-demand). */
    uint32_t GetQueryProfileRegionId(const char* nameSpace, const char* queryString, const char* regionName);

    /** @brief Emit run-time profile data. */
    void EmitProfileData(uint32_t functionId, uint32_t regionId, bool startEvent);

    /** @brief Generate profile view report. */
    MotJitProfile* GetProfileReport(uint32_t* num);

private:
    /** @typedef Map of function/region identifiers by name. */
    using QualifiedName = std::pair<std::string, std::string>;
    using ProfileFunctionMetaDataMap = std::map<QualifiedName, JitProfileFunctionMetaData*>;
    using ProfileFunctionIdNameMap = std::map<uint32_t, QualifiedName>;

    /** @typedef Per-thread array of function profile data (entry per-function). */
    using ThreadProfileFunctionDataArray = std::vector<JitProfileFunctionData>;

    /** @typedef Global array of function profile data (entry per-thread). */
    using GlobalProfileFunctionDataArray = std::vector<ThreadProfileFunctionDataArray*>;

    /** @brief Constructor. */
    JitProfiler();

    /** @brief Destructor. */
    ~JitProfiler();

    /** @brief Terminate all worker threads. */
    void GenerateReport();

    void DumpRawData();

    /** @brief Collect profile data. */
    void CollectReportData(ThreadProfileFunctionDataArray& globalProfileData);

    /** @brief Process profile data. */
    void ProcessReportData(ThreadProfileFunctionDataArray& globalProfileData);

    /** @brief Release all resource. */
    void Cleanup();

    /** @var The singleton instance of the thread pool. */
    static JitProfiler* m_instance;

    /** @var Function identifier map. */
    ProfileFunctionMetaDataMap m_profileFunctionMetaDataMap;

    /** @var Function identifier to name map. */
    ProfileFunctionIdNameMap m_profileFunctionIdNameMap;

    /** @var Global profile data array for all threads. */
    GlobalProfileFunctionDataArray m_profileFunctionDataArray;

    /** @var Lock to synchronize thread pool access. */
    std::mutex m_functionIdLock;

    /** @var The profiled function counter. */
    uint32_t m_nextFunctionId;
};
}  // namespace JitExec

#endif /* JIT_PROFILER_H */
