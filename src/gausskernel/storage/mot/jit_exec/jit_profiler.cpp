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
 * jit_profiler.cpp
 *    JIT LLVM.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/jit_exec/jit_profiler.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "libintl.h"
#include "jit_profiler.h"

#include "debug_utils.h"
#include "utilities.h"
#include "thread_id.h"
#include "mot_configuration.h"

namespace JitExec {
DECLARE_LOGGER(JitProfiler, JitExec)

JitProfiler* JitProfiler::m_instance = nullptr;

uint32_t JitProfileFunctionMetaData::GetProfileRegionDataId(const char* name)
{
    ProfileRegionIdMap::iterator itr = m_profileRegionIdMap.find(name);
    if (itr == m_profileRegionIdMap.end()) {
        uint32_t id = m_profileRegionIdMap.size();
        std::pair<ProfileRegionIdMap::iterator, bool> pairib =
            m_profileRegionIdMap.insert(ProfileRegionIdMap::value_type(name, id));
        MOT_ASSERT(pairib.second);
        m_profileRegionIdNameMap[id] = name;
        return id;
    }
    return itr->second;
}

uint32_t JitProfileFunctionMetaData::FindProfileRegionId(const char* name) const
{
    uint32_t regionId = MOT_JIT_PROFILE_INVALID_REGION_ID;
    ProfileRegionIdMap::const_iterator itr = m_profileRegionIdMap.find(name);
    if (itr != m_profileRegionIdMap.end()) {
        regionId = itr->second;
    }
    return regionId;
}

void JitProfileFunctionData::CopySubQueryIdArray(const SubQueryIdArray& rhs)
{
    m_subQueryIdArray.resize(rhs.size());
    (void)std::copy(rhs.begin(), rhs.end(), m_subQueryIdArray.begin());
}

void JitProfileFunctionData::CopyMetaData(JitProfileFunctionMetaData* sourceData)
{
    m_functionOid = sourceData->GetFunctionOid();
    m_functionId = sourceData->GetFunctionId();
    m_parentFunctionId = sourceData->GetParentFunctionId();
    m_nameSpace = sourceData->GetNameSpace();
    m_totalRegionId = sourceData->GetTotalRegionId();
    CopySubQueryIdArray(sourceData->GetSubQueryIdArray());
}

void JitProfileFunctionData::Accumulate(const JitProfileFunctionData& rhs)
{
    if (m_profileRegionDataArray.size() < rhs.m_profileRegionDataArray.size()) {
        m_profileRegionDataArray.resize(rhs.m_profileRegionDataArray.size());
    }
    uint32_t regionCount = std::min(m_profileRegionDataArray.size(), rhs.m_profileRegionDataArray.size());
    for (uint32_t i = 0; i < regionCount; ++i) {
        m_profileRegionDataArray[i].m_accumulatedTime += rhs.m_profileRegionDataArray[i].m_accumulatedTime;
        m_profileRegionDataArray[i].m_sampleCount += rhs.m_profileRegionDataArray[i].m_sampleCount;
    }
}

void JitProfileFunctionData::AccumulateChild(const JitProfileFunctionData& rhs)
{
    // we would like to accumulate the child's total time into the function's net time
    if (rhs.m_totalRegionId != MOT_JIT_PROFILE_INVALID_REGION_ID &&
        rhs.m_totalRegionId < rhs.m_profileRegionDataArray.size()) {
        const JitProfileRegionData& regionData = rhs.m_profileRegionDataArray[rhs.m_totalRegionId];
        if (regionData.m_sampleCount > 0) {
            // we do not average over child sample count (we have that already in child's report), but rather later on
            // average over parent call count - thus reporting the average sum of all child calls in a single parent
            // call, when we do this for each jittable child call
            m_childNetTime += regionData.m_accumulatedTime;
        }
    }
}

void JitProfileFunctionData::Dump()
{
    for (uint32_t i = 0; i < m_profileRegionDataArray.size(); ++i) {
        JitProfileRegionData& regionData = m_profileRegionDataArray[i];
        if (regionData.m_sampleCount > 0) {
            MOT_LOG_TRACE("      Region %u: sample count %" PRIu64 ", accum %" PRIu64,
                i,
                regionData.m_sampleCount,
                regionData.m_accumulatedTime);
        }
    }
}

void JitProfileFunctionData::Report(const char* functionName, const JitProfileFunctionMetaData& profileMetaData)
{
    // calculate total time, always in entry zero
    if (!m_profileRegionDataArray.empty() && (m_totalRegionId != MOT_JIT_PROFILE_INVALID_FUNCTION_ID) &&
        (m_totalRegionId < m_profileRegionDataArray.size())) {
        JitProfileRegionData& regionData = m_profileRegionDataArray[m_totalRegionId];
        if (regionData.m_sampleCount > 0) {
            uint64_t totalAvgTime = regionData.m_accumulatedTime / regionData.m_sampleCount;
            MOT_LOG_DEBUG("Profile report for jitted function: %s", functionName);
            for (uint32_t i = 0; i < m_profileRegionDataArray.size(); ++i) {
                regionData = m_profileRegionDataArray[i];
                if (regionData.m_sampleCount > 0) {
                    uint64_t avgTime = regionData.m_accumulatedTime / regionData.m_sampleCount;
                    uint64_t nanos = MOT::CpuCyclesLevelTime::CyclesToNanoseconds(avgTime);
                    uint64_t selfPercent = avgTime * 100 / totalAvgTime;
                    JitProfileFunctionMetaData::ProfileRegionIdNameMap::const_iterator itr =
                        profileMetaData.GetRegionIdNameMap().find(i);
                    if (itr != profileMetaData.GetRegionIdNameMap().end()) {
                        MOT_LOG_DEBUG(
                            "Region '%s': %" PRIu64 " nano-seconds (%d%%)", itr->second.c_str(), nanos, selfPercent);
                    }
                }
            }
        }
    }
}

void JitProfileFunctionData::Report(
    MotJitProfile* entry, uint32_t defVarsRegionId, uint32_t initVarsRegionId, uint32_t childCallRegionId)
{
    entry->totalTime = 0;
    entry->selfTime = 0;
    entry->childNetTime = 0;
    entry->childGrossTime = 0;
    entry->defVarsTime = 0;
    entry->initVarsTime = 0;

    // calculate total time, always in entry zero
    if (m_totalRegionId < m_profileRegionDataArray.size()) {
        JitProfileRegionData& totalRegionData = m_profileRegionDataArray[m_totalRegionId];
        if (totalRegionData.m_sampleCount > 0) {
            uint64_t totalCount = totalRegionData.m_sampleCount;
            uint64_t totalAvgTime = totalRegionData.m_accumulatedTime / totalCount;
            entry->totalTime = (int64)MOT::CpuCyclesLevelTime::CyclesToMicroseconds(totalAvgTime);
            entry->selfTime = entry->totalTime;

            uint64_t childNetAvgTime = m_childNetTime / totalCount;
            entry->childNetTime = (int64)MOT::CpuCyclesLevelTime::CyclesToMicroseconds(childNetAvgTime);

            if (childCallRegionId < m_profileRegionDataArray.size()) {
                JitProfileRegionData& childCallRegionData = m_profileRegionDataArray[childCallRegionId];
                if (childCallRegionData.m_sampleCount > 0) {
                    // we do not compute normal average for child calls, but rather average of total number of calls,
                    // this will show the average total time of actual child calls made per function invocation
                    uint64_t childCallAvgTime = childCallRegionData.m_accumulatedTime / totalCount;
                    entry->childGrossTime = (int64)MOT::CpuCyclesLevelTime::CyclesToMicroseconds(childCallAvgTime);
                    entry->selfTime = entry->totalTime - entry->childGrossTime;
                }
            }

            if (defVarsRegionId < m_profileRegionDataArray.size()) {
                JitProfileRegionData& defVarsRegionData = m_profileRegionDataArray[defVarsRegionId];
                if (defVarsRegionData.m_sampleCount > 0) {
                    uint64_t defVarsAvgTime = defVarsRegionData.m_accumulatedTime / defVarsRegionData.m_sampleCount;
                    entry->defVarsTime = (int64)MOT::CpuCyclesLevelTime::CyclesToMicroseconds(defVarsAvgTime);
                }
            }

            if (initVarsRegionId < m_profileRegionDataArray.size()) {
                JitProfileRegionData& initVarsRegionData = m_profileRegionDataArray[initVarsRegionId];
                if (initVarsRegionData.m_sampleCount > 0) {
                    uint64_t initVarsAvgTime = initVarsRegionData.m_accumulatedTime / initVarsRegionData.m_sampleCount;
                    entry->initVarsTime = (int64)MOT::CpuCyclesLevelTime::CyclesToMicroseconds(initVarsAvgTime);
                }
            }
        }
    }
}

bool JitProfiler::CreateInstance()
{
    if (m_instance == nullptr) {
        m_instance = new (std::nothrow) JitProfiler();
    }
    return (m_instance != nullptr);
}

void JitProfiler::DestroyInstance()
{
    if (m_instance != nullptr) {
        delete m_instance;
        m_instance = nullptr;
    }
}

JitProfiler* JitProfiler::GetInstance()
{
    return m_instance;
}

uint32_t JitProfiler::GetProfileFunctionId(const char* functionName, Oid functionOid)
{
    uint32_t functionId = MOT_JIT_PROFILE_INVALID_FUNCTION_ID;
    QualifiedName qname = std::make_pair(MOT_JIT_GLOBAL_QUERY_NS, functionName);
    std::unique_lock<std::mutex> lock(m_functionIdLock);
    ProfileFunctionMetaDataMap::iterator itr = m_profileFunctionMetaDataMap.find(qname);
    if (itr == m_profileFunctionMetaDataMap.end()) {
        JitProfileFunctionMetaData* functionData =
            new (std::nothrow) JitProfileFunctionMetaData(functionOid, m_nextFunctionId);
        if (functionData != nullptr) {
            functionId = m_nextFunctionId++;
            (void)m_profileFunctionMetaDataMap.insert(ProfileFunctionMetaDataMap::value_type(qname, functionData));
            m_profileFunctionIdNameMap[functionId] = qname;
        } else {
            MOT_LOG_TRACE("Failed to allocate profile meta-data object: %s", functionName);
        }
    } else {
        functionId = itr->second->GetFunctionId();
    }
    return functionId;
}

uint32_t JitProfiler::GetProfileQueryId(const char* nameSpace, const char* queryString)
{
    uint32_t functionId = MOT_JIT_PROFILE_INVALID_FUNCTION_ID;
    uint32_t parentFunctionId = MOT_JIT_PROFILE_INVALID_FUNCTION_ID;
    bool expectingParent = (strcmp(nameSpace, MOT_JIT_GLOBAL_QUERY_NS) == 0);

    QualifiedName qname = std::make_pair(nameSpace, queryString);
    QualifiedName pqname = std::make_pair(MOT_JIT_GLOBAL_QUERY_NS, nameSpace);

    std::unique_lock<std::mutex> lock(m_functionIdLock);
    JitProfileFunctionMetaData* parentData = nullptr;
    ProfileFunctionMetaDataMap::iterator pitr = m_profileFunctionMetaDataMap.find(pqname);
    if (pitr != m_profileFunctionMetaDataMap.end()) {
        parentData = pitr->second;
        parentFunctionId = pitr->second->GetFunctionId();
    } else if (expectingParent) {
        // bail out if expected parent not found
        MOT_LOG_TRACE("Parent by name-space %s not found: %s", nameSpace, queryString);
        return MOT_JIT_PROFILE_INVALID_FUNCTION_ID;
    }

    // search for function
    ProfileFunctionMetaDataMap::iterator itr = m_profileFunctionMetaDataMap.find(qname);
    if (itr == m_profileFunctionMetaDataMap.end()) {
        JitProfileFunctionMetaData* functionData =
            new (std::nothrow) JitProfileFunctionMetaData(InvalidOid, m_nextFunctionId, parentFunctionId, nameSpace);
        if (functionData != nullptr) {
            functionId = m_nextFunctionId++;
            (void)m_profileFunctionMetaDataMap.insert(ProfileFunctionMetaDataMap::value_type(qname, functionData));
            (void)m_profileFunctionIdNameMap.insert(ProfileFunctionIdNameMap::value_type(functionId, qname));
            if (parentData != nullptr) {
                parentData->AddChildQuery(functionId);
            }
        } else {
            MOT_LOG_TRACE(
                "Failed to allocate profile meta-data object with name-space [%s]: %s", nameSpace, queryString);
        }
    } else {
        // profiled query already registered
        JitProfileFunctionMetaData* functionData = itr->second;
        functionId = functionData->GetFunctionId();
    }
    return functionId;
}

uint32_t JitProfiler::GetProfileRegionId(const char* functionName, const char* regionName)
{
    uint32_t regionId = MOT_JIT_PROFILE_INVALID_REGION_ID;
    QualifiedName qname = std::make_pair(MOT_JIT_GLOBAL_QUERY_NS, functionName);
    std::unique_lock<std::mutex> lock(m_functionIdLock);
    ProfileFunctionMetaDataMap::iterator itr = m_profileFunctionMetaDataMap.find(qname);
    if (itr != m_profileFunctionMetaDataMap.end()) {
        regionId = itr->second->GetProfileRegionDataId(regionName);
    }
    return regionId;
}

uint32_t JitProfiler::GetQueryProfileRegionId(const char* nameSpace, const char* queryString, const char* regionName)
{
    uint32_t regionId = MOT_JIT_PROFILE_INVALID_REGION_ID;
    QualifiedName qname = std::make_pair(nameSpace, queryString);
    std::unique_lock<std::mutex> lock(m_functionIdLock);
    ProfileFunctionMetaDataMap::iterator itr = m_profileFunctionMetaDataMap.find(qname);
    if (itr != m_profileFunctionMetaDataMap.end()) {
        regionId = itr->second->GetProfileRegionDataId(regionName);
    }
    return regionId;
}

void JitProfiler::EmitProfileData(uint32_t functionId, uint32_t regionId, bool startEvent)
{
    // silently ignore invalid parameters
    if ((functionId == MOT_JIT_PROFILE_INVALID_FUNCTION_ID) || (regionId == MOT_JIT_PROFILE_INVALID_REGION_ID)) {
        return;
    }

    // create on-demand profile data array for current thread
    uint32_t threadId = MOTCurrThreadId;
    if (m_profileFunctionDataArray[threadId] == nullptr) {
        ThreadProfileFunctionDataArray* functionDataArray = new (std::nothrow) ThreadProfileFunctionDataArray();
        if (functionDataArray == nullptr) {
            return;
        }

        std::unique_lock<std::mutex> lock(m_functionIdLock);
        // we don't care function id in each entry is zero...
        functionDataArray->resize(m_profileFunctionMetaDataMap.size());
        m_profileFunctionDataArray[threadId] = functionDataArray;
    }

    // allocate space for profiled function if required (maybe function added after profiling started)
    ThreadProfileFunctionDataArray& pdArray = *m_profileFunctionDataArray[threadId];
    if (pdArray.size() <= functionId) {
        // we don't care function id in each new entry is zero...
        pdArray.resize(functionId + 1);
    }

    //  emit profile data for region
    if (startEvent) {
        pdArray[functionId].StartProfileRegionSample(regionId);
    } else {
        pdArray[functionId].EndProfileRegionSample(regionId);
    }
}

MotJitProfile* JitProfiler::GetProfileReport(uint32_t* num)
{
    if (MOT_CHECK_LOG_LEVEL(MOT::LogLevel::LL_TRACE)) {
        DumpRawData();
    }
    ThreadProfileFunctionDataArray globalProfileData;
    CollectReportData(globalProfileData);
    ProcessReportData(globalProfileData);

    // dump what we just collected
    MOT_LOG_TRACE("JIT Profiler raw PROCESSED data dump:");
    for (uint32_t i = 0; i < globalProfileData.size(); ++i) {
        MOT_LOG_TRACE("  JIT profiler function %u raw data dump:", i);
        MOT_LOG_TRACE("    Function id %u", globalProfileData[i].GetFunctionId());
        MOT_LOG_TRACE("    Parent function id %u", globalProfileData[i].GetParentFunctionId());
        globalProfileData[i].Dump();
    }

    *num = 0;
    uint32_t maxSize = globalProfileData.size();
    MotJitProfile* result = (MotJitProfile*)palloc(maxSize * sizeof(MotJitProfile));
    if (result == nullptr) {
        return nullptr;
    }

    uint32_t count = 0;
    for (uint32_t i = 0; i < maxSize; ++i) {
        JitProfileFunctionData& pfd = globalProfileData[i];
        if (pfd.GetTotalSampleCount() == 0) {
            continue;
        }

        result[count].procOid = pfd.GetFunctionOid();
        result[count].id = pfd.GetFunctionId();
        result[count].parentId = pfd.GetParentFunctionId();
        result[count].query = nullptr;
        result[count].nameSpace = nullptr;
        result[count].weight = (float4)1.0f;
        if (pfd.GetParentFunctionId() != MOT_JIT_PROFILE_INVALID_FUNCTION_ID) {
            JitProfileFunctionData& parentPfd = globalProfileData[pfd.GetParentFunctionId()];
            uint32_t parentCount = parentPfd.GetTotalSampleCount();
            if (parentCount > 0) {
                result[count].weight = ((float4)pfd.GetTotalSampleCount()) / parentCount;
            }
        }

        uint32_t defVarsRegionId = MOT_JIT_PROFILE_INVALID_REGION_ID;
        uint32_t initVarsRegionId = MOT_JIT_PROFILE_INVALID_REGION_ID;
        uint32_t childCallRegionId = MOT_JIT_PROFILE_INVALID_REGION_ID;
        {
            std::unique_lock<std::mutex> lock(m_functionIdLock);
            ProfileFunctionIdNameMap::iterator itr = m_profileFunctionIdNameMap.find(pfd.GetFunctionId());
            if (itr != m_profileFunctionIdNameMap.end()) {
                QualifiedName& qname = itr->second;
                if (pfd.GetFunctionOid() != InvalidOid) {
                    const char* queryString = qname.second.c_str();
                    const char* dotPtr = strchr(queryString, '.');
                    if (dotPtr == nullptr) {
                        result[count].query = pstrdup(queryString);
                    } else {
                        // we shave off the function id
                        size_t dotPos = dotPtr - queryString;
                        result[count].query = pnstrdup(queryString, dotPos);
                    }
                } else {
                    result[count].query = pstrdup(qname.second.c_str());
                }
                result[count].nameSpace = pstrdup(qname.first.c_str());
                ProfileFunctionMetaDataMap::iterator itr2 = m_profileFunctionMetaDataMap.find(qname);
                if (itr2 != m_profileFunctionMetaDataMap.end()) {
                    JitProfileFunctionMetaData* metaData = itr2->second;
                    defVarsRegionId = metaData->GetDefVarsRegionId();
                    initVarsRegionId = metaData->GetInitVarsRegionId();
                    childCallRegionId = metaData->GetChildCallRegionId();
                }
            }
        }

        pfd.Report(&result[count], defVarsRegionId, initVarsRegionId, childCallRegionId);
        ++count;
    }

    *num = count;
    return result;
}

JitProfiler::JitProfiler() : m_nextFunctionId(0)
{
    m_profileFunctionDataArray.resize(MOT::GetGlobalConfiguration().m_maxThreads);
}

JitProfiler::~JitProfiler()
{
    if (MOT_CHECK_TRACE_LOG_LEVEL()) {
        GenerateReport();
    }
    Cleanup();
}

void JitProfiler::GenerateReport()
{
    // accumulate all thread data into global thread data
    ThreadProfileFunctionDataArray globalProfileData;
    CollectReportData(globalProfileData);
    ProcessReportData(globalProfileData);

    std::unique_lock<std::mutex> lock(m_functionIdLock);

    // generate report
    uint32_t functionCount = globalProfileData.size();
    for (uint32_t i = 0; i < functionCount; ++i) {
        JitProfileFunctionData& pfd = globalProfileData[i];
        ProfileFunctionIdNameMap::iterator itr = m_profileFunctionIdNameMap.find(pfd.GetFunctionId());
        if (itr != m_profileFunctionIdNameMap.end()) {
            QualifiedName& qname = itr->second;
            ProfileFunctionMetaDataMap::iterator itr2 = m_profileFunctionMetaDataMap.find(qname);
            if (itr2 != m_profileFunctionMetaDataMap.end()) {
                JitProfileFunctionMetaData* functionData = itr2->second;
                globalProfileData[i].Report(qname.second.c_str(), *functionData);
            }
        }
    }
}

void JitProfiler::DumpRawData()
{
    MOT_LOG_TRACE("JIT Profiler raw data dump:");
    uint32_t maxThreads = MOT::GetGlobalConfiguration().m_maxThreads;
    for (uint32_t i = 0; i < maxThreads; ++i) {
        if (m_profileFunctionDataArray[i] != nullptr) {
            MOT_LOG_TRACE("  JIT profiler thread %u raw data dump:", i);
            ThreadProfileFunctionDataArray& nextArray = *m_profileFunctionDataArray[i];
            for (uint32_t j = 0; j < nextArray.size(); ++j) {
                MOT_LOG_TRACE("    JIT profiler function %u raw data dump:", j);
                nextArray[j].Dump();
            }
        }
    }
}

void JitProfiler::CollectReportData(ThreadProfileFunctionDataArray& globalProfileData)
{
    uint32_t functionCount = 0;
    {
        std::unique_lock<std::mutex> lock(m_functionIdLock);
        functionCount = m_profileFunctionMetaDataMap.size();
    }
    globalProfileData.resize(functionCount);
    uint32_t maxThreads = MOT::GetGlobalConfiguration().m_maxThreads;
    for (uint32_t i = 0; i < maxThreads; ++i) {
        if (m_profileFunctionDataArray[i] != nullptr) {
            ThreadProfileFunctionDataArray& nextArray = *m_profileFunctionDataArray[i];
            uint32_t usedFunctionCount = std::min(functionCount, (uint32_t)nextArray.size());
            for (uint32_t j = 0; j < usedFunctionCount; ++j) {
                globalProfileData[j].Accumulate(nextArray[j]);
            }
        }
    }

    // dump what we just collected
    MOT_LOG_TRACE("JIT Profiler raw COLLECTED data dump:");
    for (uint32_t i = 0; i < globalProfileData.size(); ++i) {
        MOT_LOG_TRACE("  JIT profiler function %u raw data dump:", i);
        globalProfileData[i].Dump();
    }
}

void JitProfiler::ProcessReportData(ThreadProfileFunctionDataArray& globalProfileData)
{
    // first round: add missing meta-data
    {
        std::unique_lock<std::mutex> lock(m_functionIdLock);
        for (uint32_t i = 0; i < globalProfileData.size(); ++i) {
            JitProfileFunctionData& profileData = globalProfileData[i];
            ProfileFunctionIdNameMap::iterator itr = m_profileFunctionIdNameMap.find(i);
            if (itr != m_profileFunctionIdNameMap.end()) {
                QualifiedName& qname = itr->second;
                ProfileFunctionMetaDataMap::iterator itr2 = m_profileFunctionMetaDataMap.find(qname);
                if (itr2 != m_profileFunctionMetaDataMap.end()) {
                    JitProfileFunctionMetaData* sourceData = itr2->second;
                    profileData.CopyMetaData(sourceData);
                }
            }
        }
    }

    // second round: accumulate child-query data into parent
    for (uint32_t i = 0; i < globalProfileData.size(); ++i) {
        JitProfileFunctionData& profileData = globalProfileData[i];
        const SubQueryIdArray& idArray = profileData.GetSubQueryIdArray();
        for (uint32_t j = 0; j < idArray.size(); ++j) {
            uint32_t childIndex = idArray[j];
            profileData.AccumulateChild(globalProfileData[childIndex]);
        }
    }
}

void JitProfiler::Cleanup()
{
    std::unique_lock<std::mutex> lock(m_functionIdLock);

    // clean map
    ProfileFunctionMetaDataMap::iterator itr = m_profileFunctionMetaDataMap.begin();
    while (itr != m_profileFunctionMetaDataMap.end()) {
        delete itr->second;
        ++itr;
    }
    m_profileFunctionMetaDataMap.clear();

    // clean global array
    for (uint32_t i = 0; i < m_profileFunctionDataArray.size(); ++i) {
        if (m_profileFunctionDataArray[i] != nullptr) {
            delete m_profileFunctionDataArray[i];
        }
    }
    m_profileFunctionDataArray.clear();
}
}  // namespace JitExec