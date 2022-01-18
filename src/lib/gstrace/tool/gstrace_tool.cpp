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
 *  gstrace_tool.cpp
 *
 *
 * IDENTIFICATION
 *       src/lib/gstrace/tool/gstrace_tool.cpp
 *
 * ---------------------------------------------------------------------------------------
 */


#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <time.h>
#include <unistd.h>
#include "securec.h"
#include "securec_check.h"

#include "gstrace/gstrace_infra_int.h"
#include "gstrace/gstrace_tool.h"

#ifndef MIN
#define MIN(A, B) ((B) < (A) ? (B) : (A))
#endif

#define TIME_BUF_MAX_SIZE 30

typedef struct trace_data_formatter {
    trace_data_fmt type;

    void (*pFormattingFunc)(char*, size_t size, const void* in_ptr, size_t len, uint32_t flags);
} trace_data_formatter;

ThreadFlow::~ThreadFlow()
{
    if (fpOut != NULL) {
        trace_fclose(fpOut);
        fpOut = NULL;
    }
}

void ThreadFlow::flush()
{
    fflush(fpOut);
}

ThreadFlow::ThreadFlow(pid_t pid, pid_t tid, bool analyze, int stepSize, FILE* fpStepOut)
{
    char outPath[MAX_PATH_LEN] = {0};
    int ret;

    this->m_Pid = pid;
    this->m_Tid = tid;
    this->m_analyze = analyze;
    this->m_stepSize = stepSize;

    ret = snprintf_s(outPath, MAX_PATH_LEN, MAX_PATH_LEN - 1, "tid.%d", m_Tid);
    securec_check_ss_c(ret, "\0", "\0");

    if (!analyze) {
        fpOut = trace_fopen(outPath, "w+");
        if (fpOut == NULL) {
            perror("Failed to open temp merge file.");
            exit(TRACE_COMMON_ERROR);
        }

        // write the header
        fprintf(fpOut, "pid: %d  tid: %d\n\n", pid, tid);
        this->fpStepOut = NULL;
    } else {
        fpOut = NULL;
        if (stepSize > 0) {
            this->fpStepOut = fpStepOut;
        } else {
            this->fpStepOut = NULL;
        }
    }
}

void ThreadFlow::handleData(trace_record* pRec, void* pData, size_t len, uint32_t seqNum)
{
    uint32_t indentation;

    indentation = m_GsFlowStack.size();
    indentation = indentation > 0 ? indentation - 1 : 0;
    if (!this->m_analyze) {
        writeToFile(pRec, 0, indentation, seqNum);
    }
}

void ThreadFlow::handleEntry(trace_record* pRec, uint32_t seqNum)
{
    uint32_t indentation;

    indentation = m_GsFlowStack.size();
    this->m_GsFlowStack.push(*pRec);
    if (!this->m_analyze) {
        writeToFile(pRec, 0, indentation, seqNum);
    }
}

void ThreadFlow::refreshAnalyzeData(trace_record* topRec, trace_record* pRec, func_stat* func_flow,
    func_stat* func_flow_step, uint64_t* stepStartTime, double elapse_time, uint32_t seqNum, uint32_t* stepCounter)
{
    trace_func_stat* func_statInfo = NULL;
    trace_func_stat* func_statInfo_temp = NULL;
    func_stat::iterator it_funcStat;

    /* global statistics for matched function */
    it_funcStat = func_flow->find(topRec->rec_id);
    if (it_funcStat != func_flow->end()) {
        func_statInfo = it_funcStat->second;
        refreshStats(topRec, elapse_time, func_statInfo, seqNum);
    }

    /* step statistics for matched function */
    if (this->m_stepSize > 0) {
        func_stat::iterator it_funcStat_temp = func_flow_step->find(topRec->rec_id);
        if (it_funcStat_temp != func_flow_step->end()) {
            func_statInfo_temp = it_funcStat_temp->second;
            refreshStats(topRec, elapse_time, func_statInfo_temp, seqNum);
        }
    }

    if (this->m_stepSize > 0 && pRec->timestamp - *stepStartTime >= (uint64_t)this->m_stepSize) {
        flushStepStatsToFile(func_flow_step, ++(*stepCounter));
        *stepStartTime = 0;
    }

    return;
}

void ThreadFlow::handleExit(trace_record* pRec, uint32_t seqNum, func_stat* func_flow, func_stat* func_flow_step,
    uint64_t* stepStartTime, uint32_t* stepCounter)
{
    bool is_try_exit = pRec->rec_id == GS_TRC_ID_TRY_CATCH;
    int tryCounter = pRec->user_data_len - sizeof(trace_data_fmt) - sizeof(size_t);
    trace_record topRec = {0};

    if (!is_try_exit || tryCounter > 0) {
        /* pop stack untill all entries without exit are processed */
        do {
            if (m_GsFlowStack.empty()) {
                printf("No entry meets this exit! - 1 -seqnum: %u rec_id: %u tid: %d pid: %d \n",
                    seqNum,
                    pRec->rec_id,
                    pRec->tid,
                    pRec->pid);
                break;
            }

            topRec = m_GsFlowStack.top();
            m_GsFlowStack.pop();

            if (topRec.rec_id != pRec->rec_id && !is_try_exit) {
                // miss ENTRY or EXIT
                printf("overlap exit %u Rec->rec_id: %u tid: %d, pid:%d\n", seqNum, pRec->rec_id, pRec->tid, pRec->pid);
                break;
            }

            if (topRec.timestamp > pRec->timestamp) {
                printf("reverse exit %u Rec->rec_id: %u tid: %d, pid:%d\n", seqNum, pRec->rec_id, pRec->tid, pRec->pid);
                break;
            }

            /* Here use topRec as EXIT since pRec may be TRY_CATCH. */
            topRec.type = TRACE_EXIT;

            double elapse_time = getElapsetime(&topRec, pRec);
            if (*stepStartTime == 0 || *stepStartTime > topRec.timestamp) {
                *stepStartTime = topRec.timestamp;
            }
            if (this->m_analyze) {
                refreshAnalyzeData(
                    &topRec, pRec, func_flow, func_flow_step, stepStartTime, elapse_time, seqNum, stepCounter);
            } else {
                uint32_t indentation = m_GsFlowStack.size();

                /*
                 * Write matched EXIT into file. If no record for mismatch
                 * is expected, uncomment the following check condition.
                 * Should add condition topRec.rec_id == pRec->rec_id
                 */
                writeToFile(&topRec, is_try_exit ? (0 - elapse_time) : elapse_time, indentation, seqNum);
            }
        } while (is_try_exit && (--tryCounter) > 0);
    }

    return;
}

double ThreadFlow::getElapsetime(trace_record* entry_rec, trace_record* exit_rec)
{
    return (double)(exit_rec->timestamp - entry_rec->timestamp);
}

void ThreadFlow::refreshStats(
    trace_record* exit_rec, double elapse_time, trace_func_stat* funcStatInfo, uint32_t seqNum)
{
    if (funcStatInfo->counter == 0) {
        funcStatInfo->max_elapsed_time = elapse_time;
        funcStatInfo->max_pid = exit_rec->pid;
        funcStatInfo->max_tid = exit_rec->tid;
        funcStatInfo->max_seq = seqNum;
        funcStatInfo->min_elapsed_time = elapse_time;
        funcStatInfo->min_seq = seqNum;
        funcStatInfo->min_pid = exit_rec->pid;
        funcStatInfo->min_tid = exit_rec->tid;
        funcStatInfo->average_elapsed_time = elapse_time;
        funcStatInfo->total_time = elapse_time;
        funcStatInfo->counter++;
    } else {
        if (funcStatInfo->max_elapsed_time < elapse_time) {
            funcStatInfo->max_elapsed_time = elapse_time;
            funcStatInfo->max_pid = exit_rec->pid;
            funcStatInfo->max_tid = exit_rec->tid;
            funcStatInfo->max_seq = seqNum;
        }

        if (funcStatInfo->min_elapsed_time > elapse_time) {
            funcStatInfo->min_elapsed_time = elapse_time;
            funcStatInfo->min_pid = exit_rec->pid;
            funcStatInfo->min_tid = exit_rec->tid;
            funcStatInfo->min_seq = seqNum;
        }

        funcStatInfo->total_time = funcStatInfo->total_time + elapse_time;
        funcStatInfo->counter++;
        funcStatInfo->average_elapsed_time = funcStatInfo->total_time / funcStatInfo->counter;
    }
}

void ThreadFlow::writeToFile(trace_record* rec, double elapse_time, uint32_t indentation, uint32_t seqNum)
{
    fprintf(fpOut, "%-5u    ", seqNum);

    for (uint32_t i = 0; i < indentation; ++i) {
        fprintf(fpOut, "| ");
    }

    if (rec->type == TRACE_ENTRY) {
        fprintf(fpOut, "%s %s\n", getTraceFunctionName(rec->rec_id), getTraceTypeString(rec->type));
    } else if (rec->type == TRACE_EXIT) {
        fprintf(fpOut, "%s %s %f\n", getTraceFunctionName(rec->rec_id), getTraceTypeString(rec->type), elapse_time);
    } else {
        fprintf(
            fpOut, "%s %s [probe %u] \n", getTraceFunctionName(rec->rec_id), getTraceTypeString(rec->type), rec->probe);
    }
}

/**
 * flush function stat for current step to file
 * the file handler is managed in DumpFileVisitor
 */
void ThreadFlow::flushStepStatsToFile(func_stat* funcStats_flow, uint32_t stepCounter)
{
    int ret;
    trace_func_stat* func_stat = NULL;
    func_stat::iterator funcStats_it;
    char* buff = (char*)malloc(max_step_analyze_line_size);
    if (buff == NULL) {
        perror("OOM: no enough memory for output statistics.");
        exit(TRACE_COMMON_ERROR);
    }

    funcStats_it = funcStats_flow->begin();
    while (funcStats_it != funcStats_flow->end()) {
        func_stat = funcStats_it->second;

        if (func_stat->counter <= 0) {
            funcStats_it++;
            continue;
        }

        ret = snprintf_s(buff,
            max_step_analyze_line_size,
            max_step_analyze_line_size - 1,
            "%10d %30s %10d %15.0lf %15.0lf %15.0lf %15.0lf\n",
            stepCounter,
            getTraceFunctionName(funcStats_it->first),
            func_stat->counter,
            func_stat->total_time,
            func_stat->average_elapsed_time,
            func_stat->max_elapsed_time,
            func_stat->min_elapsed_time);
        securec_check_ss_c(ret, "\0", "\0");

        funcStats_it++;
        fprintf(fpStepOut, "%s", buff);
        memset_s(buff, max_step_analyze_line_size, 0, max_step_analyze_line_size);
        securec_check_ss_c(ret, "\0", "\0");
    }

    /* reset step function flow stats */
    for (funcStats_it = funcStats_flow->begin(); funcStats_it != funcStats_flow->end(); ++funcStats_it) {
        funcStats_it->second->max_elapsed_time = 0;
        funcStats_it->second->max_pid = 0;
        funcStats_it->second->max_tid = 0;
        funcStats_it->second->max_seq = 0;
        funcStats_it->second->min_elapsed_time = 0;
        funcStats_it->second->min_seq = 0;
        funcStats_it->second->min_pid = 0;
        funcStats_it->second->min_tid = 0;
        funcStats_it->second->average_elapsed_time = 0;
        funcStats_it->second->total_time = 0;
        funcStats_it->second->counter = 0;
    }

    free(buff);
    buff = NULL;
}

// Check the type of the record and call different methods for different types
void DumpFileVisitor::visit(trace_record* pRec, void* pData, size_t len)
{
    switch (pRec->type) {
        case TRACE_DATA:
            visitData(pRec, pData, len);
            break;
        case TRACE_ENTRY:
            visitEntry(pRec);
            break;
        case TRACE_EXIT:
            visitExit(pRec);
            break;
        default:
            printf("Error: unknown trace type");
    }
    m_Counter++;
}

DumpFileFlowVisitor::DumpFileFlowVisitor(bool analyze, int stepSize, const char* outputFile, size_t output_len)
{
    int ret;
    this->m_analyze = analyze;
    this->m_stepSize = stepSize * microsecond;
    this->stepStartTime = 0;
    this->stepCounter = 0;

    if (m_stepSize > 0) {
        ret = snprintf_s(this->m_stepOutputFile, MAX_PATH_LEN, MAX_PATH_LEN - 1, "%s.step", outputFile);
        securec_check_ss_c(ret, "\0", "\0");
        fpStepOut = trace_fopen(this->m_stepOutputFile, "w+");
        if (fpStepOut == NULL) {
            perror("Failed to open step analyze file.");
            exit(TRACE_COMMON_ERROR);
        }
        char* buff = (char*)malloc(max_analyze_line_size);
        if (buff == NULL) {
            perror("OOM: no enough memory for output statistics.");
            exit(TRACE_COMMON_ERROR);
        }
        ret = snprintf_s(buff,
            max_analyze_line_size,
            max_analyze_line_size - 1,
            "%10s %30s %10s %15s %15s %15s %15s\n",
            "SEQUENCE",
            "FUNCTION",
            "#CALL",
            "ELAPSETIME(total)",
            "ELAPSETIME(avg.)",
            "ELAPSETIME(max.)",
            "ELAPSETIME(min.)");
        securec_check_ss_c(ret, "\0", "\0");
        fprintf(fpStepOut, "%s", buff);
        free(buff);
        buff = NULL;
    } else {
        fpStepOut = NULL;
    }
}

DumpFileFlowVisitor::~DumpFileFlowVisitor()
{
    map_flow::iterator it;
    for (it = mapFlows.begin(); it != mapFlows.end(); ++it) {
        delete it->second;
    }

    func_stat::iterator fIt;
    for (fIt = funcFlows.begin(); fIt != funcFlows.end(); ++fIt) {
        free(fIt->second);
    }
    funcFlows.clear();

    for (fIt = funcFlowsStep.begin(); fIt != funcFlowsStep.end(); ++fIt) {
        free(fIt->second);
    }
    funcFlowsStep.clear();

    trace_fclose(fpStepOut);
    fpStepOut = NULL;
}

void DumpFileFlowVisitor::flushThreadFlows()
{
    map_flow::iterator it;

    // iterate over the ThreadFlow map
    for (it = mapFlows.begin(); it != mapFlows.end(); ++it) {
        it->second->flush();
    }
}

void DumpFileFlowVisitor::mergeFiles(const char* outPath, size_t len)
{
    FILE* fpOut = NULL;
    map_flow::iterator it;
    char* buffer = NULL;
    size_t bufSize = 0;
    ssize_t charRead;

    fpOut = trace_fopen(outPath, "w+");
    if (fpOut == NULL) {
        printf("Cannot open file %s\n", outPath);
        goto exit;
    }

    if (!this->m_analyze) {
        // iterate over the ThreadFlow map
        for (it = mapFlows.begin(); it != mapFlows.end(); ++it) {
            char tmpPath[MAX_PATH_LEN] = {0};
            int ret;

            ret = snprintf_s(tmpPath, MAX_PATH_LEN, MAX_PATH_LEN - 1, "tid.%d", it->first);
            securec_check_ss_c(ret, "\0", "\0");

            // Open the file with read mode
            FILE* fpIn = trace_fopen(tmpPath, "r");
            if (NULL == fpIn) {
                printf("Cannot open file %s\n", tmpPath);
                goto exit;
            }

            while ((charRead = getline(&buffer, &bufSize, fpIn)) != -1) {
                fprintf(fpOut, "%s", buffer);
            }

            fprintf(fpOut, "\n");
            (void)trace_fclose(fpIn);

            // Delete the temporary file
            ret = remove(tmpPath);
        }
    } else {
        outputStat(fpOut);
    }

exit:
    free(buffer);
    (void)trace_fclose(fpOut);
}

// If the ThreadFlow for a given tid is not existed, then insert one to it.
void DumpFileFlowVisitor::putIfUnexisted(pid_t pid, pid_t tid)
{
    map_flow::iterator it;

    it = mapFlows.find(tid);
    if (it == mapFlows.end()) {
        // Not existed, insert one for this thread.
        ThreadFlow* pFlow = new ThreadFlow(pid, tid, this->m_analyze, this->m_stepSize, this->fpStepOut);
        mapFlows.insert(std::pair<pid_t, ThreadFlow*>(tid, pFlow));
    }
}

/**
 * if there is no entry in function stats map for specified func_id
 * init and insert one
 * Both global and step function stats call this function
 */
void DumpFileFlowVisitor::putFuncStatIfUnexisted(uint32_t func_id, func_stat* func_stat_map)
{
    func_stat::iterator it;

    it = func_stat_map->find(func_id);
    if (it == func_stat_map->end()) {
        // Not existed, insert one for this thread.
        trace_func_stat* pfuncFlowStat;
        int ret;

        pfuncFlowStat = (trace_func_stat*)malloc(sizeof(trace_func_stat));
        if (pfuncFlowStat == NULL) {
            perror("OOM: no enough memory for traced func statistics");
            exit(TRACE_COMMON_ERROR);
        }
        ret = memset_s(pfuncFlowStat, sizeof(trace_func_stat), 0, sizeof(trace_func_stat));
        securec_check(ret, "\0", "\0");
        func_stat_map->insert(std::pair<uint32_t, trace_func_stat*>(func_id, pfuncFlowStat));
    }
}

void DumpFileFlowVisitor::visitData(trace_record* pRec, void* pData, size_t len)
{
    putIfUnexisted(pRec->pid, pRec->tid);
    map_flow::iterator it;

    it = mapFlows.find(pRec->tid);
    it->second->handleData(pRec, pData, len, m_Counter);
}

void DumpFileFlowVisitor::visitEntry(trace_record* pRec)
{
    putIfUnexisted(pRec->pid, pRec->tid);
    map_flow::iterator it;

    it = mapFlows.find(pRec->tid);
    it->second->handleEntry(pRec, m_Counter);
}

void DumpFileFlowVisitor::visitExit(trace_record* pRec)
{
    putIfUnexisted(pRec->pid, pRec->tid);
    map_flow::iterator it;

    it = mapFlows.find(pRec->tid);
    if (this->m_analyze) {
        putFuncStatIfUnexisted(pRec->rec_id, &funcFlows);
        if (this->m_stepSize > 0) {
            putFuncStatIfUnexisted(pRec->rec_id, &funcFlowsStep);
        }
    }
    it->second->handleExit(pRec, m_Counter, &funcFlows, &funcFlowsStep, &stepStartTime, &stepCounter);
}

void DumpFileFlowVisitor::outputStat(FILE* fp)
{
    int ret;
    trace_func_stat* func_info = NULL;
    func_stat::iterator func_it;
    char* buff = (char*)malloc(max_analyze_line_size);

    if (buff == NULL) {
        perror("OOM: no enough memory for output statistics.");
        exit(TRACE_COMMON_ERROR);
    }

    ret = snprintf_s(buff,
        max_analyze_line_size,
        max_analyze_line_size - 1,
        "%10s %30s %10s %15s %15s %15s %15s %10s %10s\n",
        "MODULE",
        "FUNCTION",
        "#CALL",
        "ELAPSETIME(total)",
        "ELAPSETIME(avg.)",
        "ELAPSETIME(max.)",
        "ELAPSETIME(min.)",
        "SEQUENCE(max.)",
        "SEQUENCE(min.)");
    securec_check_ss_c(ret, "\0", "\0");
    fprintf(fp, "%s", buff);

    func_it = funcFlows.begin();
    while (func_it != funcFlows.end()) {
        func_info = func_it->second;

        ret = snprintf_s(buff,
            max_analyze_line_size,
            max_analyze_line_size - 1,
            "%10s %30s %10d %15.0lf %15.0lf %15.0lf %16.0lf %14d %14d\n",
            getCompNameById(func_it->first),
            getTraceFunctionName(func_it->first),
            func_info->counter,
            func_info->total_time,
            func_info->average_elapsed_time,
            func_info->max_elapsed_time,
            func_info->min_elapsed_time,
            func_info->max_seq,
            func_info->min_seq);
        securec_check_ss_c(ret, "\0", "\0");

        func_it++;
        fprintf(fp, "%s", buff);
        ret = memset_s(buff, max_analyze_line_size, 0, max_analyze_line_size);
        securec_check(ret, "\0", "\0");
    }
    free(buff);
    buff = NULL;
}

DumpFileParser::DumpFileParser(const char* inputFile, size_t len)
{
    fdInput = trace_open_filedesc(inputFile, O_RDONLY, 0);
    if (fdInput == -1) {
        perror("Failed to open trace file.");
        exit(TRACE_COMMON_ERROR);
    }
}

// Dynamically accept a visitor as the logic used for processing records
// during parsing.
void DumpFileParser::acceptVisitor(DumpFileVisitor* visitor)
{
    this->pVisitor = visitor;
}

DumpFileParser::~DumpFileParser()
{
    close(fdInput);
    pVisitor = NULL;
}

trace_msg_code DumpFileParser::parseCfg(trace_config* pCfg)
{
    size_t bytesRead = read(fdInput, (void*)pCfg, sizeof(trace_config));
    if (bytesRead != sizeof(trace_config)) {
        return TRACE_READ_CFG_FROM_FILE_ERR;
    }

    if (pCfg->trc_cfg_magic_no != GS_TRC_CFG_MAGIC_N) {
        return TRACE_MAGIC_FROM_FILE_ERR;
    }

    if (pCfg->version != TRACE_VERSION) {
        return TRACE_VERSION_ERR;
    }

    const char* hash_trace_config_file = getTraceConfigHash();
    if (memcmp(pCfg->hash_trace_config_file, hash_trace_config_file, LENGTH_TRACE_CONFIG_HASH) != 0) {
        return TRACE_VERSION_ERR;
    }

    if ((pCfg->size & (pCfg->size - 1)) != 0) {
        return TRACE_CONFIG_SIZE_ERR;
    }

    return TRACE_OK;
}

trace_msg_code DumpFileParser::parseInfra(trace_infra* pInfra)
{
    size_t bytesRead = read(fdInput, (void*)pInfra, sizeof(trace_infra));
    if (sizeof(trace_infra) != bytesRead) {
        perror("read err");
        return TRACE_READ_INFRA_FROM_FILE_ERR;
    }

    return TRACE_OK;
}

// get header (the first slot)
// so the first slot contains header or the record
trace_msg_code DumpFileParser::parseHeader(void* pSlotHeader, size_t hdr_len)
{
    size_t bytesRead = read(fdInput, (void*)pSlotHeader, hdr_len);
    if (hdr_len != bytesRead) {
        perror("read err");
        return TRACE_READ_SLOT_HEADER_ERR;
    }

    // since slots for one record is contiguous, we can do following check.
    trace_slot_head* pHdr = (trace_slot_head*)pSlotHeader;
    if (pHdr->num_slots_in_area == 0 || pHdr->num_slots_in_area > MAX_TRC_SLOTS) {
        return TRACE_NUM_SLOT_ERR;
    }

    if (pHdr->hdr_magic_number != SLOT_AREAD_HEADER_MAGIC_NO) {
        return TRACE_SLOT_MAGIC_ERR;
    }

    return TRACE_OK;
}

trace_msg_code DumpFileParser::parseData(void* pData, size_t data_len)
{
    size_t bytesRead = read(fdInput, pData, data_len);
    if (data_len != bytesRead) {
        perror("read err");
        return TRACE_READ_SLOT_DATA_ERR;
    }

    return TRACE_OK;
}

trace_msg_code DumpFileParser::parseOneRecord(const char rec_tmp_buf[MAX_TRC_RC_SZ], size_t bufSize, bool hasdata)
{
    int ret;
    trace_record recCopy;
    trace_record* pRec = NULL;
    size_t recOffset;

    recOffset = sizeof(trace_slot_head);
    if (recOffset >= bufSize) {
        return TRACE_BUFFER_SIZE_ERR;
    }

    pRec = (trace_record*)&rec_tmp_buf[recOffset];
    ret = memcpy_s(&recCopy, sizeof(trace_record), pRec, sizeof(trace_record));
    securec_check(ret, "\0", "\0");

    if (this->processForOverlap == true) {
        if (this->firstTimestamp == 0) {
            this->firstTimestamp = recCopy.timestamp;
            return TRACE_OK;
        } else if (this->firstTimestamp < pRec->timestamp) {
            return TRACE_OK;
        } else {
            if (hasdata) {
                this->lenFirstNotOverlaped = pRec->user_data_len + SLOT_SIZE;
            } else {
                this->lenFirstNotOverlaped = SLOT_SIZE;
            }
            return TRACE_OK;
        }
    }

    /* data trace type is the only trace type that can have data
     * for now.
     * We can relax this in the future to include data in entry
     * and exit */
    if (pRec->type == TRACE_DATA) {
        void* pData = (void*)&rec_tmp_buf[sizeof(trace_slot_head) + sizeof(trace_record)];

        /* Process a TRACE_DATA record */
        this->pVisitor->visit(&recCopy, pData, pRec->user_data_len);
    } else {
        /* Process a TRACE_ENTRY/TRACE_EXIT record */
        this->pVisitor->visit(&recCopy, NULL, 0);
    }

    return TRACE_OK;
}

trace_msg_code DumpFileParser::readAndParseOneRecord(void)
{
    uint64_t hdrSeq, tailSeq;
    uint32_t num_slots_in_area;
    trace_slot_head* pHdr = NULL;
    trace_slot_tail* pTail = NULL;
    char rec_tmp_buf[MAX_TRC_RC_SZ] = {'\0'};
    bool hasdata = false;
    trace_msg_code ret;

    ret = parseHeader(&rec_tmp_buf, SLOT_SIZE);
    if (ret != TRACE_OK) {
        return ret;
    }
    pHdr = (trace_slot_head*)(&rec_tmp_buf);
    hdrSeq = pHdr->hdr_sequence;
    num_slots_in_area = pHdr->num_slots_in_area;

    if (num_slots_in_area > 1 && num_slots_in_area <= MAX_TRC_SLOTS) {
        /* read following data slots */
        ret = parseData(&rec_tmp_buf[SLOT_SIZE], (num_slots_in_area - 1) * SLOT_SIZE);
        if (ret != TRACE_OK) {
            return ret;
        }
        hasdata = true;
    }

    /* get tail seq */
    int tail_offset = (num_slots_in_area * SLOT_SIZE) - sizeof(trace_slot_tail);
    if (tail_offset >= MAX_TRC_RC_SZ || tail_offset < 0) {
        return TRACE_TAIL_OFFSET_ERR;
    }
    pTail = (trace_slot_tail*)&rec_tmp_buf[tail_offset];
    tailSeq = pTail->tail_sequence;

    if (hdrSeq == ~tailSeq) {
        return this->parseOneRecord(rec_tmp_buf, MAX_TRC_RC_SZ, hasdata);
    }

    return TRACE_SEQ_ERR;
}

trace_msg_code DumpFileParser::readAndParseDump(uint64_t counter, uint64_t* totNumRecsFormatted)
{
    trace_msg_code parse_result;

    *totNumRecsFormatted = 0;
    for (uint64_t i = 0; i < counter; ++i) {
        parse_result = readAndParseOneRecord();
        if (parse_result != TRACE_OK) {
            break;
        } else {
            *totNumRecsFormatted += 1;
        }
    }

    return parse_result;
}

uint64_t DumpFileParser::findStartSlot(uint64_t maxSeq, uint64_t maxSlots, off_t* firstRecordOffset)
{
    uint64_t i = 0;
    uint64_t startSlot;
    off_t firstNotOverlapRecordWhenDump, firstNotOverlapedRecord;

    this->processForOverlap = true;
    startSlot = maxSeq % maxSlots;
    printf("Trace has wrapped. Dumped and formatted files "
           "will contain only the most recent %lu records\n",
        maxSlots);
    firstNotOverlapRecordWhenDump = (startSlot)*SLOT_SIZE;
    *firstRecordOffset = lseek(this->fdInput, 0, SEEK_CUR);
    if (*firstRecordOffset == -1) {
        perror("lseek current error!");
        return 0;
    }

    do {
        (void)readAndParseOneRecord();
        /* if first record is not valid, just skip it and find next */
        i++;
    } while ((this->firstTimestamp == 0) && (i < maxSlots - startSlot));

    if (lseek(this->fdInput, *firstRecordOffset, SEEK_SET) == -1) {
        perror("lseek first offset error!");
        return 0;
    }
    if (lseek(this->fdInput, firstNotOverlapRecordWhenDump, SEEK_CUR) == -1) {
        perror("lseek first offset error!");
        return 0;
    }

    /* when dump trace, because not stop, there maybe more records after dump finish */
    i = 0;
    do {
        (void)readAndParseOneRecord();
        /* skip invalid record until a valid record is found. */
        i++;
    } while ((this->lenFirstNotOverlaped == 0) && (i < maxSlots - startSlot));

    firstNotOverlapedRecord = lseek(this->fdInput, 0, SEEK_CUR);
    if (firstNotOverlapedRecord == -1) {
        perror("lseek current error!");
        return 0;
    }
    firstNotOverlapedRecord = firstNotOverlapedRecord - this->lenFirstNotOverlaped;
    firstNotOverlapedRecord = lseek(this->fdInput, firstNotOverlapedRecord, SEEK_SET);
    if (firstNotOverlapedRecord == -1) {
        perror("lseek first not overlap error!");
        return 0;
    }
    startSlot = (firstNotOverlapedRecord - *firstRecordOffset) / SLOT_SIZE;
    this->processForOverlap = false;

    return startSlot;
}

// Parse the whole dmp file.
trace_msg_code DumpFileParser::parse()
{
    trace_config trc_cfg;
    trace_infra trc_infra;
    uint64_t maxSeq, maxSlots, slotNumberBeforeReverse;
    uint64_t totNumRecsFormatted = 0;
    uint64_t reversedNumRecsFormatted = 0;
    off_t firstRecordOffset;
    uint64_t startSlot = 0;
    trace_msg_code ret = TRACE_OK;

    ret = parseCfg(&trc_cfg);
    if (ret != TRACE_OK) {
        return ret;
    }

    ret = parseInfra(&trc_infra);
    if (ret != TRACE_OK) {
        return ret;
    }

    maxSeq = (uint64_t)trc_infra.g_slot_count;
    if (maxSeq == 0) {
        return TRACE_NO_RECORDS_ERR;
    }
    maxSlots = trc_cfg.size / SLOT_SIZE;

    /* find first record which not been overlaped */
    if (maxSeq > maxSlots) {
        startSlot = findStartSlot(maxSeq, maxSlots, &firstRecordOffset);
    }

    /* start parse from the first not overlaped record, the number should be min of max -start
     * and max seq, if not overlap, the startSlot is 0 */
    if (startSlot > maxSlots) {
        return TRACE_STATR_SLOT_ERR;
    }
    slotNumberBeforeReverse = maxSlots - startSlot;
    ret = readAndParseDump(MIN(slotNumberBeforeReverse, (uint64_t)trc_infra.g_Counter), &totNumRecsFormatted);
    if (startSlot != 0 && (ret == TRACE_OK)) {
        /* if reverse happen this part will parse first record to the first not overlaped slot */
        /* if totNumRecsFormatted is 0, means header or data is invalid, we just handle records before invalid */
        off_t first = lseek(this->fdInput, firstRecordOffset, SEEK_SET);
        if (first == -1) {
            perror("lseek to first record error!");
        }
        ret = readAndParseDump(startSlot, &reversedNumRecsFormatted);
        totNumRecsFormatted += reversedNumRecsFormatted;
    }
    printf("Found %lu trace records and formatted %lu of them(most recent)\n",
           (uint64_t)trc_infra.g_Counter, totNumRecsFormatted);

    return ret;
}

// out_buf must be at least HEX_DUMP_BUF_SZ
// len is length of in_ptr and must not be greater than HEX_DUMP_PER_LINE
static void gsTrcHexDumpLine(char* out_buf, size_t buf_size, const void* in_ptr, size_t len, uint32_t flags)
{
    int bytes_written = 0;
    size_t offset = 0;
    const unsigned char* pc = (const unsigned char*)in_ptr;

    int ret = memset_s(out_buf, buf_size, '\0', HEX_DUMP_BUF_SZ);
    securec_check(ret, "\0", "\0");

    if (buf_size < HEX_DUMP_BUF_SZ || len > HEX_DUMP_PER_LINE) {
        perror("internal error: illegal parameter for dumpLine");
        exit(TRACE_COMMON_ERROR);
    }

    if (flags & HEX_DUMP_INCLUDE_ADDRESS) {
        bytes_written = snprintf_s(out_buf + offset, buf_size, buf_size - 1, "%p: ", in_ptr);
        securec_check_ss_c(bytes_written, "\0", "\0");
        buf_size -= bytes_written;
        offset += bytes_written;
    }

    // output the memory contents:
    for (size_t j = 0; j < len; j++) {
        if (j && j % 2 == 0) {
            // add space every 4 bytes
            bytes_written = snprintf_s(out_buf + offset, buf_size, buf_size - 1, "%s", " ");
            securec_check_ss_c(bytes_written, "\0", "\0");
            buf_size -= bytes_written;
            offset += bytes_written;
        }
        bytes_written = snprintf_s(out_buf + offset, buf_size, buf_size - 1, "%02X", pc[j]);
        securec_check_ss_c(bytes_written, "\0", "\0");
        buf_size -= bytes_written;
        offset += bytes_written;
    }

    if (flags & HEX_DUMP_INCLUDE_ASCII) {
        // add some space
        bytes_written = snprintf_s(out_buf + offset, buf_size, buf_size - 1, "%s", "  ");
        securec_check_ss_c(bytes_written, "\0", "\0");
        buf_size -= bytes_written;
        offset += bytes_written;

        // output the ascii interpretation of the raw data if possible
        for (size_t j = 0; j < len; j++, offset++) {
            // if a printable character
            out_buf[offset] = (pc[j] >= ' ' && pc[j] <= '~') ? pc[j] : '-';
        }
        buf_size -= len;
    }
    if (buf_size >= 1) {
        bytes_written = snprintf_s(out_buf + offset, buf_size, buf_size - 1, "%s", "\n");
        securec_check_ss_c(bytes_written, "\0", "\0");
    } else {
        exit(TRACE_COMMON_ERROR);
    }
}

// out_buf: buffer to be filled with the hex dump
// size: size of out_buf
// in_ptr: pointer to data to hex dump
// len: length of data
static void gsTrcHexDumpBuffer(char* out_buf, size_t size, const void* in_ptr, size_t len, uint32_t flags)
{
    int numLines = gsAlign(len, HEX_DUMP_PER_LINE) / HEX_DUMP_PER_LINE;
    size_t offset = 0;
    int k;

    if (size < 1) {
        perror("Invalid out buffer size in gsTrcHexDumpBuffer.");
        exit(TRACE_COMMON_ERROR);
    }

    for (k = 0; k < numLines && offset <= size; k++) {
        size_t buf_len = HEX_DUMP_PER_LINE;
        char buf[HEX_DUMP_BUF_SZ] = {0};
        const unsigned char* p = (const unsigned char*)in_ptr + (k * HEX_DUMP_PER_LINE);

        // For the last line, only hexdump a line that contains
        // the remaining bytes.
        if (k == numLines - 1) {
            buf_len = len - (k * HEX_DUMP_PER_LINE);
        }

        gsTrcHexDumpLine(buf, HEX_DUMP_BUF_SZ, (void*)p, buf_len, flags);
        int bytes_written = snprintf_s(out_buf + offset, size - offset, size - offset - 1, "%s", buf);
        securec_check_ss_c(bytes_written, "\0", "\0");
        offset += bytes_written;
    }

    if (k != numLines) {
        perror("Invalid Trace File - length of the trace record is too loog.");
        exit(TRACE_COMMON_ERROR);
    }
}

// given a size and some null-terminated string
// this function will return how many bytes remain
// in the size if we concat the string
static size_t getRemaining(size_t buf_size, const char* buf)
{
    return (buf_size - strlen(buf));
    ;
}

// out_buf: buffer to be filled with the hex dump
// size: size of out_buf
// in_ptr: pointer to data to hex dump
// len: length of data
static void gsTrcFormatUint32(char* out_buf, size_t size, const void* in_ptr, size_t len, uint32_t flags)
{
    char* out_buf_start = out_buf;
    int s = getRemaining(size, out_buf_start);

    int written = snprintf_s(out_buf + (size - s), s, s - 1, "%d", *(uint32_t*)in_ptr);
    securec_check_ss_c(written, "\0", "\0");
}

static trace_data_formatter g_data_formatters[] = {
    {TRC_DATA_FMT_NONE, NULL}, {TRC_DATA_FMT_DFLT, gsTrcHexDumpBuffer}, {TRC_DATA_FMT_UINT32, gsTrcFormatUint32}};

static void gsTrcFormatTracData(const uint32_t rec_no, trace_record* pRec, const char* slot_area, size_t slot_area_len,
    char* out_buf, size_t out_buf_len)
{
    char* pData = (char*)&slot_area[sizeof(trace_slot_head) + sizeof(trace_record)];
    size_t bytes_processed = 0;
    size_t item_size;
    char formatted_buf[MAX_TRC_RC_SZ] = {'\0'};
    char user_data_buf[MAX_TRC_RC_SZ] = {'\0'};
    trace_data_fmt item_type;
    int ret;

    while (bytes_processed < pRec->user_data_len) {
        ret = memcpy_s(&item_type, sizeof(trace_data_fmt), pData, sizeof(trace_data_fmt));
        securec_check(ret, "\0", "\0");
        bytes_processed += sizeof(trace_data_fmt);
        pData += sizeof(trace_data_fmt);

        ret = memcpy_s(&item_size, sizeof(size_t), pData, sizeof(size_t));
        securec_check(ret, "\0", "\0");
        bytes_processed += sizeof(size_t);
        pData += sizeof(size_t);

        if (item_size == 0) {
            perror("invalid probe data size");
            exit(TRACE_COMMON_ERROR);
        }

        ret = memcpy_s(user_data_buf, MAX_TRC_RC_SZ, pData, item_size);
        securec_check(ret, "\0", "\0");
        bytes_processed += item_size;
        pData += item_size;

        g_data_formatters[item_type].pFormattingFunc(formatted_buf,
            sizeof(formatted_buf),
            user_data_buf,
            item_size,
            HEX_DUMP_INCLUDE_ADDRESS | HEX_DUMP_INCLUDE_ASCII);
    }

    char time_buf[TIME_BUF_MAX_SIZE];
    time_t second = pRec->timestamp / USECS_PER_SEC;
    suseconds_t usec = pRec->timestamp % USECS_PER_SEC;
    if (out_buf_len < 1 || ctime_r(&second, time_buf) == NULL) {
        exit(TRACE_COMMON_ERROR);
    }
    ret = snprintf_s(out_buf,
        out_buf_len,
        out_buf_len - 1,
        "%-5u  %s, Pid: %d, Tid: %d, Probe: %u, "
        "Function: %s MicroSecond:%ld %s data:\n%s\n",
        rec_no,
        getTraceTypeString(pRec->type),
        pRec->pid,
        pRec->tid,
        pRec->probe,
        getTraceFunctionName(pRec->rec_id),
        usec,
        time_buf,
        formatted_buf);
    securec_check_ss_c(ret, "\0", "\0");
}

static void gsTrcFormatOneTraceRecord(
    const uint32_t rec_no, char* out_buf, size_t out_buf_len, const char* slot_area, size_t slot_area_len)
{
    trace_record* pRec;
    int ret;

    pRec = (trace_record*)&slot_area[sizeof(trace_slot_head)];

    // Data trace type is the only trace type that can have data for now.
    // We can relax this in the future to include data in entry and exit
    if (pRec->type == TRACE_DATA) {
        gsTrcFormatTracData(rec_no, pRec, slot_area, slot_area_len, out_buf, out_buf_len);
    } else {
        char time_buf[TIME_BUF_MAX_SIZE];
        time_t second = pRec->timestamp / USECS_PER_SEC;
        suseconds_t usec = pRec->timestamp % USECS_PER_SEC;
        if (out_buf_len < 1 || ctime_r(&second, time_buf) == NULL) {
            exit(TRACE_COMMON_ERROR);
        }

        // the last %s is for timestampe which includes a line return
        ret = snprintf_s(out_buf,
            out_buf_len,
            out_buf_len - 1,
            "%-5u  %s, Pid: %d, Tid: %d, Function: %s MicroSecond:%ld %s",
            rec_no,
            getTraceTypeString(pRec->type),
            pRec->pid,
            pRec->tid,
            getTraceFunctionName(pRec->rec_id),
            usec,
            time_buf);
        securec_check_ss_c(ret, "\0", "\0");
    }
}

static trace_msg_code readAndCheckTrcMeta(int fdInput, trace_config* trc_cfg, trace_infra* trc_infra)
{
    size_t bytesRead;

    bytesRead = read(fdInput, (void*)trc_cfg, sizeof(trace_config));
    if (bytesRead != sizeof(trace_config)) {
        perror("read err.");
        return TRACE_READ_CFG_FROM_FILE_ERR;
    }

    if ((trc_cfg->size & (trc_cfg->size - 1)) != 0) {
        return TRACE_BUFFER_SIZE_FROM_FILE_ERR;
    }

    if (trc_cfg->trc_cfg_magic_no != GS_TRC_CFG_MAGIC_N) {
        return TRACE_MAGIC_FROM_FILE_ERR;
    }

    bytesRead = read(fdInput, (void*)trc_infra, sizeof(trace_infra));
    if (bytesRead != sizeof(trace_infra)) {
        perror("read err.");
        return TRACE_READ_INFRA_FROM_FILE_ERR;
    }

    if (trc_infra->g_Counter == 0) {
        return TRACE_NO_RECORDS_ERR;
    }

    return TRACE_OK;
}

static trace_msg_code readAndFormatTrcRec(int fdInput, int fdOutput, uint64_t counter)
{
    uint64_t totalNumRecs = 0;
    trace_msg_code ret = TRACE_OK;
    char rec_tmp_buf[MAX_TRC_RC_SZ] = {'\0'};
    char rec_out_buf[MAX_TRC_RC_SZ] = {'\0'};

    for (uint64_t i = 0; i < counter; i++) {
        // Read the contents of a the header slot
        size_t bytesRead = read(fdInput, (void*)&rec_tmp_buf, SLOT_SIZE);
        if (bytesRead != SLOT_SIZE) {
            perror("Invalid trace file");
            return TRACE_READ_SLOT_HEADER_ERR;
        }
        trace_slot_head* pHdr = (trace_slot_head*)rec_tmp_buf;

        // since slots for one record is contiguous, we can do following check.
        if (pHdr->num_slots_in_area == 0 || pHdr->num_slots_in_area > MAX_TRC_SLOTS) {
            ret = TRACE_NUM_SLOT_ERR;
            break;
        }

        if (pHdr->hdr_magic_number != SLOT_AREAD_HEADER_MAGIC_NO) {
            ret = TRACE_SLOT_MAGIC_ERR;
            break;
        }

        if (pHdr->num_slots_in_area > 1 && pHdr->num_slots_in_area <= MAX_TRC_SLOTS) {
            // read the remainder of the slot area.
            uint64_t size_to_read = (pHdr->num_slots_in_area - 1) * SLOT_SIZE;

            bytesRead = read(fdInput, (void*)&rec_tmp_buf[SLOT_SIZE], size_to_read);
            if (bytesRead != size_to_read) {
                perror("Invalid trace file.");
                return TRACE_READ_SLOT_DATA_ERR;
            }
        }

        // Now we have the data ( one or multiple slots ) in rec_tmp_buf
        // Get the address of the area tail
        int tail_offset = (pHdr->num_slots_in_area * SLOT_SIZE) - sizeof(trace_slot_tail);
        if (tail_offset < 0 || tail_offset >= MAX_TRC_RC_SZ) {
            continue;
        }
        trace_slot_tail* pTail = (trace_slot_tail*)&rec_tmp_buf[tail_offset];

        // slot area entry is valid
        if (pHdr->hdr_sequence == ~pTail->tail_sequence) {
            gsTrcFormatOneTraceRecord(
                (uint32_t)(totalNumRecs + 1), rec_out_buf, MAX_TRC_RC_SZ, rec_tmp_buf, MAX_TRC_RC_SZ);

            // now write the buffer to the file
            size_t bytesWritten = write(fdOutput, rec_out_buf, strlen(rec_out_buf));
            if (bytesWritten != strlen(rec_out_buf)) {
                perror("write error");
                return TRACE_WRITE_FORMATTED_RECORD_ERR;
            }
            totalNumRecs++;
        }
    }

    printf("Found %lu trace records and formatted %lu of them\n", counter, totalNumRecs);
    return ret;
}

// This function will format the trace
// binary file to a text file
//
// No attachment to shared memory is required.
//
// the formatted file will be the same name
// as the binary file with the extension .fmt
// -------------------------------------------
static trace_msg_code formatTrcDumpFile(const char* inputPath, const char* outputPath)
{
    trace_msg_code ret;
    trace_infra trc_infra;
    trace_config trc_cfg;

    /* open input file */
    int fdInput = trace_open_filedesc(inputPath, O_RDONLY, 0);
    if (fdInput == -1) {
        perror("Failed to open input file");
        return TRACE_OPEN_INPUT_FILE_ERR;
    }

    /* Read config header to validate the file and buffer size */
    ret = readAndCheckTrcMeta(fdInput, &trc_cfg, &trc_infra);
    if (ret == TRACE_OK) {
        /* open output file */
        int fdOutput = trace_open_filedesc(outputPath, O_RDWR | O_CREAT | O_TRUNC, S_IRWXU);
        if (fdOutput == -1) {
            close(fdInput);
            perror("Failed to open output file");
            return TRACE_OPEN_OUTPUT_FILE_ERR;
        }

        uint64_t maxSlots = trc_cfg.size / SLOT_SIZE;
        if (trc_infra.g_slot_count > maxSlots) {
            printf("Trace has wrapped.Dumped and formatted files contain only the most recent %lu records\n", maxSlots);
        }

        ret = readAndFormatTrcRec(fdInput, fdOutput, MIN(maxSlots, trc_infra.g_Counter));
        (void)trace_close_filedesc(fdOutput);
    }

    (void)trace_close_filedesc(fdInput);
    return ret;
}

// Format dump file as a control flow
static trace_msg_code formatDumpFileToFlow(const char* inputFile, size_t input_len, const char* outputFile, size_t output_len)
{
    trace_msg_code ret;

    DumpFileParser parser(inputFile, input_len);
    DumpFileFlowVisitor visitor(false);
    parser.acceptVisitor(&visitor);
    ret = parser.parse();
    visitor.flushThreadFlows();
    visitor.mergeFiles(outputFile, output_len);

    return ret;
}

static trace_msg_code anlyzeDumpFile(
    const char* inputFile, size_t input_len, const char* outputFile, size_t output_len, int stepSize)
{
    trace_msg_code ret;

    DumpFileParser parser(inputFile, input_len);
    DumpFileFlowVisitor visitor(true, stepSize, outputFile, output_len);
    parser.acceptVisitor(&visitor);
    ret = parser.parse();
    visitor.mergeFiles(outputFile, output_len);

    return ret;
}

// Get the index of an option in argv array
static int findOption(int argc, char** argv, const char* option, size_t len_option)
{
    for (int i = 0; i < argc; ++i) {
        if (0 == strncmp(argv[i], option, len_option)) {
            return i;
        }
    }

    return -1;
}

// get an optional argument
static char* getCmdOption(int argc, char** argv, const char* option, size_t len_option)
{
    int idx = findOption(argc, argv, option, len_option);
    if (idx != -1 && idx < argc - 1) {
        return argv[idx + 1];
    }

    return NULL;
}

// Find the buffer size if user has set with command line
static int getBufferSize(int argc, char** argv)
{
    const char* option = "-s";
    char* opt = getCmdOption(argc, argv, option, strlen(option) + 1);
    if (opt != NULL && isNumeric(opt)) {
        return atoi(opt);
    } else {
        return DFT_BUF_SIZE;
    }
}

// Find the buffer size if user has set with command line
static int getTracePort(int argc, char** argv)
{
    const char* option = "-p";
    char* opt = getCmdOption(argc, argv, option, strlen(option) + 1);
    if (opt != NULL && isNumeric(opt)) {
        return atoi(opt);
    } else {
        return -1;
    }
}

/**
 * Check if it's analyze step size(unit:second)
 * return -1 if not match "-t"
 */
static int getAnalyzeStepSize(int argc, char** argv)
{
    const char* option = "-t";
    char* opt = getCmdOption(argc, argv, option, strlen(option) + 1);
    if (opt != NULL && isNumeric(opt)) {
        return atoi(opt);
    } else {
        return -1;
    }
}

/**
 * Check if it's tracing to file
 * if trc to file, return the filePath
 * return NULL if not match "-f"
 */
static char* getTraceFile(int argc, char** argv)
{
    const char* option = "-f";
    return getCmdOption(argc, argv, option, strlen(option) + 1);
}

/**
 * Check if it's write output to file
 * if write to file, return the filePath
 * return NULL if not match "-o"
 */
static char* getOutputFile(int argc, char** argv)
{
    const char* option = "-o";
    return getCmdOption(argc, argv, option, strlen(option) + 1);
}

// Print usage of the command line
static void printUsage(int argc, char** argv)
{
    printf("Usage:\n");
    printf("%s start -m <mask> -s <buffer size> -p <port>\n", argv[0]);
    printf("\tStart trace against process.\n");
    printf("\t-p  CN or DN's listening port\n");
    printf("\t-m  [comp1,comp2,…][ALL].[func1,func2,…][ALL].Default is ALL\n");
    printf("\t-s  the size of the shared memory for recording tracing.Default is 1073741824 bytes(1GB)\n");

    printf("%s stop -p <port>\n", argv[0]);
    printf("\tStop trace against process.\n");
    printf("\t-p  CN or DN's listening port\n");

    printf("%s config -p <port>\n", argv[0]);
    printf("\tDisplay the configurations of gstrace.\n");
    printf("\t-p  CN or DN's listening port\n");

    printf("%s dump -p <port> -o <output file>\n", argv[0]);
    printf("\tDump the traced information from the shared memory to a binary file.\n");
    printf("\t-o  binary dump file name\n");

    printf("%s detail -f <dump file> -o <output file>\n", argv[0]);
    printf("\tFormat the gstrace binary file to a human readable text file with detailed diagnostic info.\n");
    printf("\t-f  binary dump file name\n");
    printf("\t-o  text detail file name\n");

    printf("%s codepath -f <dump file> -o <output file>\n", argv[0]);
    printf("\tFormat the gstrace binary file to a human readable code path (function call path).\n");
    printf("\t-f  binary dump file name\n");
    printf("\t-o  text detail file name\n");

    printf("%s analyze -f <dump file> -o <output file>\n", argv[0]);
    printf("\tAnalyze the gstrace binary file to get function statistics.\n");
    printf("\t-f  binary dump file path\n");
    printf("\t-o  text statistics file name\n");

    printf("%s analyze -f <dump file> -o <output file> -t <step size>\n", argv[0]);
    printf("\tAnalyze the gstrace binary file to get function statistics.\n");
    printf("\tBoth global and snapshot for every {step size} seconds.\n");
    printf("\t-f  binary dump file name\n");
    printf("\t-o  global text statistics file name\n");
    printf("\t-t  get statistics for every n seconds, file name will be {global file name}.step\n");
}

/* The order of msg in trace_message should be same with the order of msgcode in trace_msg_code */
trace_msg_t trace_message[] = {
    {TRACE_OK, "Success!"},
    {TRACE_ALREADY_START, "Trace has already been activated."},
    {TRACE_ALREADY_STOP, "Trace has already been deactived."},
    {TRACE_PARAMETER_ERR, "Parameter is not correct."},
    {TRACE_BUFFER_SIZE_ERR, "Invalid share memory buffer size."},
    {TRACE_ATTACH_CFG_SHARE_MEMORY_ERR, "Attach to trace config failed."},
    {TRACE_ATTACH_BUFFER_SHARE_MEMORY_ERR, "Attach to trace buffer failed."},
    {TRACE_OPEN_SHARE_MEMORY_ERR, "Failed to initialize trace buffer."},
    {TRACE_TRUNCATE_ERR, "Failed to set size of trace buffer."},
    {TRACE_MMAP_ERR, "Failed to map memory for trace buffer."},
    {TRACE_MUNMAP_ERR, "Failed to unmap memory for trace buffer."},
    {TRACE_UNLINK_SHARE_MEMORY_ERR, "Failed to delete trace buffer."},
    {TRACE_DISABLE_ERR, "Trace is disable."},
    {TRACE_OPEN_OUTPUT_FILE_ERR, "Failed to open trace output file."},
    {TRACE_OPEN_INPUT_FILE_ERR, "Failed to open trace input file."},
    {TRACE_WRITE_BUFFER_HEADER_ERR, "Failed to write trace buffer header."},
    {TRACE_WRITE_CFG_HEADER_ERR, "Failed to write trace config header."},
    {TRACE_WRITE_BUFFER_ERR, "Failed to write trace buffer."},
    {TRACE_READ_CFG_FROM_FILE_ERR, "Failed to read trace config."},
    {TRACE_BUFFER_SIZE_FROM_FILE_ERR, "Invalid buffer size in trace file."},
    {TRACE_MAGIC_FROM_FILE_ERR, "Invalid magic number in trace file."},
    {TRACE_READ_INFRA_FROM_FILE_ERR, "Failed to read trace infra."},
    {TRACE_NO_RECORDS_ERR, "No trace records was captured."},
    {TRACE_READ_SLOT_HEADER_ERR, "Failed to read trace slot header."},
    {TRACE_NUM_SLOT_ERR, "Concurrent write occurred during dump, some slot header is invalid."},
    {TRACE_SLOT_MAGIC_ERR, "Invalid trace file, magic number is not correct."},
    {TRACE_READ_SLOT_DATA_ERR, "Failed to read trace slot data."},
    {TRACE_WRITE_FORMATTED_RECORD_ERR, "Failed to write formatted trace record."},
    {TRACE_STATR_SLOT_ERR, "Start slot should never big than max slot."},
    {TRACE_TAIL_OFFSET_ERR, "Trace tail offset is invalid."},
    {TRACE_SEQ_ERR, "Trace sequence check failed."},
    {TRACE_VERSION_ERR, "trace version not match."},
    {TRACE_CONFIG_SIZE_ERR, "invalid config size in trace file."},
    {TRACE_PROCESS_NOT_EXIST, "The database process is not exist."},
    {TRACE_MSG_MAX, "Failed!"},
};

static void print_result_message(trace_msg_code rc, int argc, char** argv)
{
    if (rc == TRACE_OK) {
        (void)printf("[GAUSS-TRACE] %s %s\n", argv[1], trace_message[rc].msg_string);
    } else if ((rc > TRACE_OK) && (rc < TRACE_MSG_MAX)) {
        if (rc == trace_message[rc].msg_code) {
            (void)printf("[GAUSS-TRACE] %s\n", trace_message[rc].msg_string);
        } else {
            (void)printf("[GAUSS-TRACE] Trace failed for error %05d.\n", rc);
        }
    } else {
        (void)printf("[GAUSS-TRACE] %s %s\n", argv[1], trace_message[TRACE_MSG_MAX].msg_string);
    }

    if (rc == TRACE_PARAMETER_ERR) {
        printUsage(argc, argv);
    }

    return;
}

int main(int argc, char** argv)
{
    trace_msg_code rc = TRACE_OK;
    int port;
    char* trcFile = NULL;
    char* outputFile = NULL;
    int stepSize;
    const int two_paras = 6;
    const int three_paras = 8;

    if (argc < 2) {
        rc = TRACE_PARAMETER_ERR;
        goto exit;
    }

    port = getTracePort(argc, argv);
    trcFile = getTraceFile(argc, argv);
    outputFile = getOutputFile(argc, argv);
    stepSize = getAnalyzeStepSize(argc, argv);

    if (0 == strcmp(argv[1], "start") && port != -1) {
        trcFile = getTraceFile(argc, argv);
        uint64_t uBufferSize = getBufferSize(argc, argv);
        char* mask = getCmdOption(argc, argv, "-m", sizeof("-m"));
        rc = gstrace_start(port, mask, uBufferSize, trcFile);
    } else if (0 == strcmp(argv[1], "stop") && port != -1) {
        // Stop will disable tracing and delete the shared memory buffer
        rc = gstrace_stop(port);
    } else if (0 == strcmp(argv[1], "config") && port != -1) {
        rc = gstrace_config(port);
    } else if (0 == strcmp(argv[1], "dump") && port != -1 && outputFile != NULL) {
        // dump will dump the contents of the trace buffer to a binary file
        rc = gstrace_dump(port, outputFile);
    } else if (argc == two_paras && (0 == strcmp(argv[1], "detail")) && trcFile != NULL && outputFile != NULL) {
        // detail will format the binary file to a human readable file
        rc = formatTrcDumpFile(trcFile, outputFile);
    } else if (argc == two_paras && (0 == strcmp(argv[1], "codepath")) && trcFile != NULL && outputFile != NULL) {
        rc = formatDumpFileToFlow(trcFile, strlen(trcFile), outputFile, strlen(outputFile));
    } else if (argc == two_paras && (0 == strcmp(argv[1], "analyze")) && trcFile != NULL && outputFile != NULL) {
        rc = anlyzeDumpFile(trcFile, strlen(trcFile), outputFile, strlen(outputFile), 0);
    } else if (argc == three_paras && (0 == strcmp(argv[1], "analyze")) && trcFile != NULL && outputFile != NULL &&
               stepSize != -1) {
        /* step stats file will be {outputFile}.step */
        rc = anlyzeDumpFile(trcFile, strlen(trcFile), outputFile, strlen(outputFile), stepSize);
    } else {
        rc = TRACE_PARAMETER_ERR;
    }

exit:
    print_result_message(rc, argc, argv);
    return (int)rc;
}
