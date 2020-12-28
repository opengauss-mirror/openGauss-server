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
 * gstrace_tool.h
 * 
 * 
 * IDENTIFICATION
 *        src/include/gstrace/gstrace_tool.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef TRACE_TOOL_H_
#define TRACE_TOOL_H_

#include <map>
#include <stack>

#define HEX_DUMP_ADDR_SZ (sizeof("0xffffffffffff") + 2)
#define HEX_DUMP_PER_LINE 16

// 2 bytes for each plus space in between each 2
#define HEX_DUMP_LEN (HEX_DUMP_PER_LINE * 2 + HEX_DUMP_PER_LINE / 2)
#define HEX_DUMP_SPC 2
#define HEX_DUMP_ASCII_START (HEX_DUMP_LEN + HEX_DUMP_SPC)

#define HEX_DUMP_BUF_SZ (HEX_DUMP_ADDR_SZ + HEX_DUMP_ASCII_START + HEX_DUMP_PER_LINE + sizeof("\n"))

#define HEX_DUMP_INCLUDE_ADDRESS (1 << 0)
#define HEX_DUMP_INCLUDE_ASCII (1 << 1)

typedef struct trace_func_stat {
    pid_t max_pid;
    pid_t max_tid;
    uint32_t max_seq;
    pid_t min_pid;
    pid_t min_tid;
    uint32_t min_seq;
    uint32_t counter;
    double total_time;
    double max_elapsed_time;
    double min_elapsed_time;
    double average_elapsed_time;
} trace_func_stat;

typedef std::map<uint32_t, trace_func_stat*> func_stat;

// Class: ThreadFlow
//      Using a stack to construct a control flow for each thread.
// -------------------------------------------------------------------------------------------
class ThreadFlow {
public:
    ThreadFlow(pid_t pid, pid_t tid, bool analyze, int stepSize, FILE* fpStepOut);
    ~ThreadFlow();
    void flush();
    void handleEntry(trace_record* pRec, uint32_t seqNum);
    void handleExit(trace_record* pRec, uint32_t seqNum, func_stat* func_flow, func_stat* func_flow_step,
        uint64_t* stepStartTime, uint32_t* stepCounter);
    void handleData(trace_record* pRec, void* pData, size_t len, uint32_t seqNum);
    void refreshStats(trace_record* entry_rec, double elaps_time, trace_func_stat* funcInfo, uint32_t seqNum);
    void writeToFile(trace_record* rec, double elapse_time, uint32_t indentation, uint32_t seqNum);
    void flushStepStatsToFile(func_stat* func_stats, uint32_t stepCounter);
    double getElapsetime(trace_record* entry_rec, trace_record* exit_rec);
    void refreshAnalyzeData(trace_record* topRec, trace_record* pRec, func_stat* func_flow, func_stat* func_flow_step,
        uint64_t* stepStartTime, double elapse_time, uint32_t seqNum, uint32_t* stepCounter);

private:
    pid_t m_Pid;
    pid_t m_Tid;
    bool m_analyze;
    int m_stepSize;
    std::stack<trace_record> m_GsFlowStack;
    FILE* fpOut;     /* output file */
    FILE* fpStepOut; /* step analyze file */
    const int max_step_analyze_line_size = 2048;
};

typedef std::map<pid_t, ThreadFlow*> map_flow;

// Class: DumpFileVisitor
//      Define the interface for any kind of visitors. To decouple the parser
//      and the logic how you want to use the data parsed from dmp file.
//      e.g. For trace to flow, can create a concrete class named DumpFileFlowVisitor
//           and implement visitEntry, visitExit and visitData respectively.
//
//           For formatting, can create another concrete class named FormatVisitor,
//           and implement each visitXXX method to append each records in order to the fmt file.
// -------------------------------------------------------------------------------------------
class DumpFileVisitor {
public:
    // dataOrRec:
    //       0 - data
    //       1 - rec
    DumpFileVisitor()
    {}
    virtual ~DumpFileVisitor()
    {}

    // Check the type of the record and call different methods for different types
    void visit(trace_record* pRec, void* pData, size_t len);
    virtual void visitEntry(trace_record* pRec){};
    virtual void visitExit(trace_record* pRec){};
    virtual void visitData(trace_record* pRec, void* pData, size_t len){};
    virtual void outputStat(FILE* fp){};

protected:
    uint32_t m_Counter = 1;
    bool m_analyze = false;
    int m_stepSize = 0;
    char m_stepOutputFile[MAX_PATH_LEN] = {0};
    size_t m_stepOutputFileLen = 0;
    FILE* fpStepOut = NULL; /* step analyze file */
};

// Class: DumpFileFlowVisitor
//      Extend the DumpFileVisitor, and implement visitEntry, visitExit and visitData.
//      It's used for taking records in order and create a control flow for each thread.
// -------------------------------------------------------------------------------------------
class DumpFileFlowVisitor : public DumpFileVisitor {
public:
    DumpFileFlowVisitor(bool analyze, int stepSize = 0, const char* outputFile = "", size_t output_len = 0);
    ~DumpFileFlowVisitor();
    void visitEntry(trace_record* pRec) override;
    void visitExit(trace_record* pRec) override;
    void visitData(trace_record* pRec, void* pData, size_t len) override;
    void outputStat(FILE* fp) override;
    void putIfUnexisted(pid_t pid, pid_t tid);
    void putFuncStatIfUnexisted(uint32_t func_id, func_stat* func_stat_map);
    void mergeFiles(const char* outPath, size_t len);
    void flushThreadFlows();

private:
    map_flow mapFlows;
    func_stat funcFlows;
    func_stat funcFlowsStep;
    uint64_t stepStartTime;
    uint32_t stepCounter;
    const int max_analyze_line_size = 2048;
    const int microsecond = 1000000;
};

// Dump File Parser
// -----------------------------------------------------------
class DumpFileParser {
public:
    DumpFileParser(const char* inputFile, size_t len);
    ~DumpFileParser();

    // get header (the first slot)
    // so the first slot contains header or the record
    trace_msg_code parseHeader(void* pSlotHeader, size_t hdr_len);
    trace_msg_code parseData(void* pData, size_t data_len);
    trace_msg_code parseCfg(trace_config* pCfg);
    trace_msg_code parseInfra(trace_infra* pInfra);

    trace_msg_code readAndParseDump(uint64_t counter, uint64_t* totNumRecsFormatted);
    // Parse the whole dmp file.
    trace_msg_code parse();

    // Dynamically accept a visitor as the logic used for processing records
    // during parsing.
    void acceptVisitor(DumpFileVisitor* visitor);
    trace_msg_code parseOneRecord(const char rec_tmp_buf[MAX_TRC_RC_SZ], size_t bufSize, bool hasdata);
    trace_msg_code readAndParseOneRecord(void);
    uint64_t findStartSlot(uint64_t maxSeq, uint64_t maxSlots, off_t* firstRecordOffset);

private:
    int fdInput;
    DumpFileVisitor* pVisitor = NULL;
    uint64_t firstTimestamp = 0;
    uint64_t lenFirstNotOverlaped = 0;
    bool processForOverlap = false;
};

#endif
