/* -------------------------------------------------------------------------
 *
 * instrument.h
 *	  definitions for run-time statistics collection
 *
 *
 * Copyright (c) 2001-2012, PostgreSQL Global Development Group
 *
 * src/include/executor/instrument.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef INSTRUMENT_H
#define INSTRUMENT_H

#include "portability/instr_time.h"
#include "nodes/pg_list.h"
#include "nodes/plannodes.h"
#include "lib/stringinfo.h"
#include "miscadmin.h"
#include "utils/anls_opt.h"
#include <signal.h>
#include "datatype/timestamp.h"
#include "storage/lock/lwlock.h"

class ThreadInstrumentation;
class StreamInstrumentation;

extern THR_LOCAL ThreadInstrumentation* runtimeThreadInstr;

#define trackpoint_number 10
#define DN_NUM_STREAMS_IN_CN(num_streams, gather_count, query_dop) ((num_streams + gather_count) * query_dop)

#define ECQUERYDATALEN 1024

#define TRACK_START(nodeid, id)                            \
    do {                                                   \
        StreamInstrumentation::TrackStartTime(nodeid, id); \
    } while (0);

#define TRACK_END(nodeid, id)                            \
    do {                                                 \
        StreamInstrumentation::TrackEndTime(nodeid, id); \
    } while (0);

typedef struct BufferUsage {
    long shared_blks_hit;      /* # of shared buffer hits */
    long shared_blks_read;     /* # of shared disk blocks read */
    long shared_blks_dirtied;  /* # of shared blocks dirtied */
    long shared_blks_written;  /* # of shared disk blocks written */
    long local_blks_hit;       /* # of local buffer hits */
    long local_blks_read;      /* # of local disk blocks read */
    long local_blks_dirtied;   /* # of shared blocks dirtied */
    long local_blks_written;   /* # of local disk blocks written */
    long temp_blks_read;       /* # of temp blocks read */
    long temp_blks_written;    /* # of temp blocks written */
    instr_time blk_read_time;  /* time spent reading */
    instr_time blk_write_time; /* time spent writing */
} BufferUsage;

typedef struct CPUUsage {
    double m_cycles; /* number of cycles */
} CPUUsage;

/* Flag bits included in InstrAlloc's instrument_options bitmask */
typedef enum InstrumentOption {
    INSTRUMENT_NONE = 0,
    INSTRUMENT_TIMER = 1 << 0,   /* needs timer (and row counts) */
    INSTRUMENT_BUFFERS = 1 << 1, /* needs buffer usage */
    INSTRUMENT_ROWS = 1 << 2,    /* needs row count */
    INSTRUMENT_ALL = 0x7FFFFFFF
} InstrumentOption;

/* Mark the DFS storage system type */
typedef enum DFSType {
    DFS_HDFS = 1, /* Hadoop HDFS storage system              */
    DFS_OBS = 2,  /* OBS key-value storage system			   */
    DFS_DFV = 3   /* DFV p-log based storage system          */
    ,
    TYPE_LOG_FT /* foreign table type, log data */
} DFSType;

#define SORT_IN_DISK 0x00000001   /* space type Disk */
#define SORT_IN_MEMORY 0x00000002 /* space type Memory */

enum sortMethodId {
    /* sort Method */
    HEAPSORT,
    QUICKSORT,
    EXTERNALSORT,
    EXTERNALMERGE,
    STILLINPROGRESS
};

struct sortMessage {
    sortMethodId TypeId;
    char* sortName;
};

typedef struct SortHashInfo {
    int sortMethodId;
    int spaceTypeId;
    long spaceUsed;
    int nbatch;
    int nbatch_original;
    int nbuckets;
    Size spacePeak;
    int64 hash_FileNum;
    int64 hash_innerFileNum; /* number of files to store build side data */
    int64 hash_outerFileNum; /* number of files to store probe side data */
    bool hash_writefile;
    int hash_spillNum;
    int hashtable_expand_times;
    double hashbuild_time;
    double hashagg_time;
    long spill_size;      /* Totoal disk IO */
    long spill_innerSize; /* disk IO of build side data that are spilt to disk */
    long spill_innerSizePartMax;
    long spill_innerSizePartMin;
    long spill_outerSize; /* disk IO of probe side data that are spilt to disk */
    long spill_outerSizePartMax;
    long spill_outerSizePartMin;
    int spill_innerPartNum; /* number of inner partitions that are spilt to disk */
    int spill_outerPartNum; /* number of outer partitions that are spilt to disk */
    int hash_partNum;       /* partition number of either build or probe side */
} SortHashInfo;

typedef struct StreamTime {
    bool need_timer;
    bool running;
    instr_time starttime;
    instr_time counter;
    double firsttuple;
    double tuplecount;
    double startup; /* Total startup time (in seconds) */
    double total;   /* Total total time (in seconds) */
    double ntuples; /* Total tuples produced */
    double nloops;  /* # of run cycles for this node */

} StreamTime;

typedef struct AccumCounters {
    int64 accumCounters[3];
    int64 tempCounters[3];
} AccumCounters;

typedef struct Track {
    int node_id;
    bool is_datanode; /* mark current plan node is datanode or not */
    ThreadId track_threadId;
    int registerTrackId;
    StreamTime track_time;
    AccumCounters accumCounters;
    bool active; /* track.active == true means track is effective */
} Track;

typedef struct RoughCheckInfo {
    uint64 m_CUFull;
    uint64 m_CUNone;
    uint64 m_CUSome;

    inline void IncFullCUNum()
    {
        ++m_CUFull;
    }
    inline void IncNoneCUNum()
    {
        ++m_CUNone;
    }
    inline void IncSomeCUNum()
    {
        ++m_CUSome;
    }

} RCInfo;

typedef struct NetWorkPerfData {
    int64 network_size;
    instr_time start_poll_time;
    instr_time start_deserialize_time;
    instr_time start_copy_time;
    instr_time network_poll_time;
    instr_time network_deserialize_time;
    instr_time network_copy_time;
    double total_poll_time;
    double total_deserialize_time;
    double total_copy_time;
} NetWorkPerfData;

/* For stream thread sending time */
typedef struct StreamSendData {
    instr_time start_send_time;
    instr_time start_wait_quota_time;
    instr_time start_OS_send_time;
    instr_time start_serialize_time;
    instr_time start_copy_time;
    instr_time stream_send_time;
    instr_time stream_wait_quota_time;
    instr_time stream_OS_send_time;
    instr_time stream_serialize_time;
    instr_time stream_copy_time;
    double total_send_time;
    double total_wait_quota_time;
    double total_os_send_time;
    double total_serialize_time;
    double total_copy_time;
    bool loops;
} StreamSendData;

typedef struct MemoryInfo {
    int64 peakOpMemory;        /* Peak Memory usage for per operator */
    int64 peakControlMemory;   /* Peak Control Memory */
    int64 peakNodeMemory;      /* Peak Memory usage for per query */
    int32 operatorMemory;      /* Operator Memory for intensive operator */
    MemoryContext nodeContext; /* node context */
    List* controlContextList;  /* control list of memory contexts */
} MemoryInfo;

/*
 * max non-iteration and iteration times performance can support
 */
#define RECUSIVE_MAX_ITER_NUM 201

typedef struct RecursiveInfo {
    int niters; /* iteration number */
    uint64 iter_ntuples[RECUSIVE_MAX_ITER_NUM];
    bool has_reach_limit;
} RecursiveInfo;

typedef struct Instrumentation {
    /* Parameters set at node creation: */
    bool need_timer;    /* TRUE if we need timer data */
    bool need_bufusage; /* TRUE if we need buffer usage data */
    bool needRCInfo;
    /* Info about current plan cycle: */
    bool running;               /* TRUE if we've completed first tuple */
    instr_time starttime;       /* Start time of current iteration of node */
    instr_time counter;         /* Accumulated runtime for this node */
    double firsttuple;          /* Time for first tuple of this cycle */
    double tuplecount;          /* Tuples emitted so far this cycle */
    BufferUsage bufusage_start; /* Buffer usage at start */
    /* Accumulated statistics across all completed cycles: */
    double startup;                   /* Total startup time (in seconds) */
    double total;                     /* Total total time (in seconds) */
    double ntuples;                   /* Total tuples produced */
    double nloops;                    /* # of run cycles for this node */
    double nfiltered1;                /* # tuples removed by scanqual or joinqual */
    double nfiltered2;                /* # tuples removed by "other" quals OR
                                       * # tuples updated by MERGE */
    BufferUsage bufusage;             /* Total buffer usage */
    CPUUsage cpuusage_start;          /* CPU usage at start */
    CPUUsage cpuusage;                /* Total CPU usage */
    SortHashInfo sorthashinfo;        /* Sort/hash operator perf data*/
    NetWorkPerfData network_perfdata; /* Network performance data */
    StreamSendData stream_senddata;   /* Stream send time */
    RCInfo rcInfo;                    /* Cu filter performance data */
    MemoryInfo memoryinfo;            /* Peak Memory data */
    /* Count up the number of files which are pruned dynamically. */
    uint64 dynamicPrunFiles;
    /* Count up the number of files which are pruned static */
    uint64 staticPruneFiles;
    /* Count up the number of rows which are filtered by the bloom filter. */
    uint64 bloomFilterRows;
    /* Count up the number of strides which are filtered by the bloom filter. */
    uint64 bloomFilterBlocks;
    /* Count up the number of rows which are filtered by the min/max. */
    uint64 minmaxFilterRows;
    double init_time; /*	executor start time	*/
    double end_time;  /*	executor end time	*/
    double run_time;  /*	executor run time	*/
    /* Count up the number of hdfs local block read. */
    double localBlock;
    /* Count up the number of hdfs remote blocks read. */
    double remoteBlock;
    uint32 warning;
    /* Count the number of intersections with dfs namenode/datanode. */
    uint64 nnCalls;
    uint64 dnCalls;
    /* Count up the number of files to access */
    uint64 minmaxCheckFiles;
    /* Count up the number of filtered files */
    uint64 minmaxFilterFiles;
    /* Count up the number of stripe to access */
    uint64 minmaxCheckStripe;
    /* Count up the number of filtered stripe */
    uint64 minmaxFilterStripe;
    /* Count up the number of stride to access */
    uint64 minmaxCheckStride;
    /* Count up the number of filtered stride */
    uint64 minmaxFilterStride;
    /* Count up the number of cache hint of orc data */
    uint64 orcDataCacheBlockCount;
    uint64 orcDataCacheBlockSize;
    /* Count up the number of  load of orc data */
    uint64 orcDataLoadBlockCount;
    uint64 orcDataLoadBlockSize;
    /* Count up the number of  cache hint of orc meta data*/
    uint64 orcMetaCacheBlockCount;
    uint64 orcMetaCacheBlockSize;
    /* Count up the number of oad of orc meta data */
    uint64 orcMetaLoadBlockCount;
    uint64 orcMetaLoadBlockSize;
    bool isLlvmOpt; /* Optimize plan by using llvm. */
    int dfsType;    /* Indicate the storage file system type */
    int width;      /* avg width for the in memory tuples */
    bool sysBusy;   /* if disk spill caused by system busy */
    int spreadNum;  /* auto spread num */
    bool status;
    bool first_time;
    int dop;
    TimestampTz enter_time;

    int ec_operator;                           /* is ec operator */
    int ec_status;                             /* ec task status */
    char ec_execute_datanode[NAMEDATALEN + 1]; /* ec execute datanode name*/
    char ec_dsn[NAMEDATALEN + 1];              /* ec dsn*/
    char ec_username[NAMEDATALEN + 1];         /* ec username*/
    char ec_query[ECQUERYDATALEN + 1];         /* ec execute query*/
    int ec_libodbc_type;                       /* ec execute libodbc_type*/
    int64 ec_fetch_count;                      /* ec fetch count*/
    RecursiveInfo recursiveInfo;
} Instrumentation;

/* instrumentation data */
typedef struct InstrStreamPlanData {
    /* whether the plannode is valid */
    bool isValid;
    bool isExecute;
    bool isSend;
    Instrumentation instruPlanData;
} InstrStreamPlanData;

/* every plannode has a nodeInstr */
struct NodeInstr {
    /* Instrument data */
    InstrStreamPlanData instr;
    int planNodeId;
    int planParentId;
    /* plan type for writing csv */
    int planType;
    /* plantypestridx correspond to plantype for writing csv */
    uint planTypeStrIdx;
    /* plan node name */
    char* name;
    Track* tracks;
    double ntime;
};

/* threadTrack is correspond to trackId in trackdesc */
enum ThreadTrack {
    /* general track */
    RECV_PLAN = 0,
    START_EXEC,
    FINISH_EXEC,
    REPORT_FINISH,
    PASS_WLM,
    STREAMNET_INIT,

    /* node track */
    LOAD_CU_DESC,
    MIN_MAX_CHECK,
    FILL_BATCH,
    GET_CU_DATA,
    UNCOMPRESS_CU,
    CSTORE_PROJECT,
    LLVM_COMPILE_TIME,
    CNNET_INIT,
    CN_CHECK_AND_UPDATE_NODEDEF,
    CN_SERIALIZE_PLAN,
    CN_SEND_QUERYID_WITH_SYNC,
    CN_SEND_BEGIN_COMMAND,
    CN_START_COMMAND,
    DN_STREAM_THREAD_INIT,
    FILL_LATER_BATCH,
    GET_CU_DATA_LATER_READ,
    GET_CU_SIZE,
    GET_CU_DATA_FROM_CACHE,
    FILL_VECTOR_BATCH_BY_TID,
    SCAN_BY_TID,
    PREFETCH_CU_LIST,
    GET_COMPUTE_DATANODE_CONNECTION,
    GET_COMPUTE_POOL_CONNECTION,
    GET_PG_LOG_TUPLES,
    GET_GS_PROFILE_TUPLES,
    TSSTORE_PROJECT,
    TSSTORE_SEARCH,
    TSSTORE_SEARCH_TAGID,

    TRACK_NUM
};

/* planType */
#define HASHJOIN_OP 0x00000001 /* hashjoin */
#define IO_OP 0x00000002       /* scan */
#define JOIN_OP 0x00000004     /* join */
#define SORT_OP 0x00000008     /* sort */
#define NET_OP 0x00000010      /* net */
#define UTILITY_OP 0x00000020  /* utility */
#define HASHAGG_OP 0x00000040  /* hashagg */

/* trackType */
#define TRACK_TIMESTAMP 0x00000001
#define TRACK_TIME 0x00000002
#define TRACK_COUNT 0x00000004
#define TRACK_VALUE 0x00000008
#define TRACK_VALUE_COUNT 0x00000010

/* track information */
struct TrackDesc {
    /* trackId in ThreadTrack */
    ThreadTrack trackId;

    /* trackName correspond to trackId */
    char* trackName;

    /* nodeBind is false means general track, true means node track */
    bool nodeBind;

    /* track type includes TRACK_COUNT, TRACK_TIME ,TRACK_TIMESTAMP */
    int trackType;
};

/*
 * threadInstrInfo is obtained from getstreamInfo.
 * threadInstrInfo in CN larger than threadInstrInfo in DN
 * because of GATHER in CN.
 */
typedef struct ThreadInstrInfo {
    /* head node id. */
    int segmentId;

    /* num of nodes in this stream. */
    int numOfNodes;

    /* base offset by the node instr array. */
    int offset;

} ThreadInstrInfo;

struct Plan;

/*
 * thread instrumentation
 * is allocated in globalstreaminstrumentation according to needs.
 * nodeInstr include instrumentation and nodetrack info in a plannodeid.
 * generaltrack record the generaltrack info in every thread.
 */
class ThreadInstrumentation : public BaseObject {
public:
    /* init in every thread */
    ThreadInstrumentation(uint64 queryId, int segmentId, int nodesInThread, int nodesInSql);

    ~ThreadInstrumentation();

    /* return allocated slot for planNodeId */
    Instrumentation* allocInstrSlot(int planNodeId, int parentPlanId, Plan* plan, struct EState* estate);

    /* thread track time */
    void startTrack(int planId, ThreadTrack instrIdx);

    void endTrack(int planId, ThreadTrack instrIdx);

    int getSegmentId()
    {
        return m_segmentId;
    }

    void setQueryString(char* qstr);

    int* get_instrArrayMap()
    {
        return m_instrArrayMap;
    }

    Track* get_tracks(int planNodeId)
    {
        if (m_instrArrayMap[planNodeId - 1] == -1)
            return NULL;
        else {
            Assert(m_instrArrayMap[planNodeId - 1] != -1);
            return m_instrArray[m_instrArrayMap[planNodeId - 1]].tracks;
        }
    }

    int get_tracknum()
    {
        return m_nodeTrackNum;
    }

    int getgeneraltracknum()
    {
        return m_generalTrackNum;
    }

    int get_generaltracknum()
    {
        return m_generalTrackNum;
    }

    int get_instrArrayLen()
    {
        return m_instrArrayLen;
    }

public:
    uint64 m_queryId;

    /* stream top plan_node_id */
    int m_segmentId;

    /* the allocate instr array. */
    NodeInstr* m_instrArray;

    /* instr array length. */
    int m_instrArrayLen;

    /* node id to the offset of the instr array map. */
    int* m_instrArrayMap;

    /* map array length has all nodes. */
    int m_instrArrayMapLen;

    int m_instrArrayAllocIndx;

    int m_generalTrackNum;

    /* track which do not belong to any plan_node_id */
    Track* m_generalTrackArray;

    int m_nodeTrackNum;

    char* m_queryString;
};

/*
 * GlobalStreamInstrumentation
 * Instrumentation information for distributed query.
 * allocated in topconsumer of DN and CN.
 */
class StreamInstrumentation : public BaseObject {
public:
    /* construnctor */
    StreamInstrumentation(int size, int num_streams, int gather_count, int query_dop, int plan_size, int start_node_id,
        int option, bool trackoption);

    /* deconstructor */
    ~StreamInstrumentation();

    static StreamInstrumentation* InitOnDn(void* desc, int dop);

    static StreamInstrumentation* InitOnCn(void* desc, int dop);

    /* init globalstreaminstrumentation and threadinstrumentation in compute pool */
    static StreamInstrumentation* InitOnCP(void* desc, int dop);

    /* get the toplogy of this stream query. */
    void getStreamInfo(Plan* result_plan, PlannedStmt* planned_stmt, int dop, ThreadInstrInfo* info, int offset);

    void allocateAllThreadInstrOnDN(bool in_compute_pool);

    void allocateAllThreadInstrOnCN();

    /* register thread in compute pool */
    void allocateAllThreadInstrOnCP(int dop);

    /* serialize the specific instrument data to remote */
    void serializeSend();

    /* deserialize the data, and put in the specific slot. -1 means on DWS CN, else on DWS DN */
    void deserialize(int idx, char* msg, size_t len, bool operator_statitics, int cur_smp_id = -1);

    /* serialize the track data to remote. */
    void serializeSendTrack();

    /* deserialize the track data. */
    void deserializeTrack(int idx, char* msg, size_t len);

    /* return the instrumentation related node number. */
    int getInstruNodeNum()
    {
        return m_nodes_num;
    }

    int getInstruPlanSize()
    {
        return m_plannodes_num;
    }

    int getInstruStartId()
    {
        return m_start_node_id;
    }

    int getInstruThreadNum()
    {
        return m_num_streams;
    }

    int get_gather_num()
    {
        return m_gather_count;
    }

    int get_threadInstrArrayLen()
    {
        return m_threadInstrArrayLen;
    }

    /* allocate slot for a node. */
    Instrumentation* allocInstrSlot(int planNodeId);

    /* return a slot in specific idx,plannodeid */
    Instrumentation* getInstrSlot(int idx, int planNodeId);

    /* Return a slot by specific idx, plannodeid and smpid. */
    Instrumentation* getInstrSlot(int idx, int planNodeId, int smpId);

    /* if this plan node run in the data node. */
    bool isFromDataNode(int planNodeId);

    bool isTrack();

    bool needTrack()
    {
        return m_trackoption;
    }

    void set_size(int plan_size)
    {
        m_nodes_num = plan_size;
        m_option = 1;
    }

    void SetStreamSend(int planNodeId, bool send);
    bool IsSend(int idx, int plan_id, int smp_id);

    /* Get dop by plan node id. */
    int getNodeDop(int planNodeId)
    {
        Assert(planNodeId >= 1 && planNodeId <= m_plannodes_num);
        return m_node_dops[planNodeId - 1];
    }

    int get_option()
    {
        return m_option;
    }

    int get_query_dop()
    {
        return m_query_dop;
    }

    MemoryContext getInstrDataContext()
    {
        return m_instrDataContext;
    }

    int* get_planIdOffsetArray()
    {
        return m_planIdOffsetArray;
    }

    // track time
    static void TrackStartTime(int planNodeId, int trackId);
    static void TrackEndTime(int planNodeId, int trackId);

    /* context for CN instrData */
    MemoryContext m_instrDataContext;

    void SetNetWork(int planNodeId, int64 buf_len);
    void SetPeakNodeMemory(int planNodeId, int64 memory_size);

    ThreadInstrumentation* allocThreadInstrumentation(int SegmentId);

    /* just called by the CN of the compute pool */
    void aggregate(int plannode_num);

    /*thread instrumenation array.*/
    ThreadInstrumentation** m_threadInstrArray;

    /* every node has a offset. */
    int* m_planIdOffsetArray;

    /* get ThreadInstrumentation of TOPconsumer	*/
    ThreadInstrumentation* getThreadInstrumentationCN(int idx)
    {
        int dn_num_streams = DN_NUM_STREAMS_IN_CN(m_num_streams, m_gather_count, m_query_dop);
        return m_threadInstrArray[1 + idx * dn_num_streams];
    }

    /* get ThreadInstrumentation in CN */
    ThreadInstrumentation* getThreadInstrumentationCN(int idx, int planNodeId, int smpId)
    {
        Assert(planNodeId >= 1 && planNodeId <= m_plannodes_num);
        int offset =
            (m_planIdOffsetArray[planNodeId - 1] == 0) ? -1 : (m_planIdOffsetArray[planNodeId - 1] - 1) * m_query_dop;
        int dn_num_streams = DN_NUM_STREAMS_IN_CN(m_num_streams, m_gather_count, m_query_dop);

        if (offset == -1)
            return m_threadInstrArray[0];
        else
            return m_threadInstrArray[1 + idx * dn_num_streams + offset + smpId];
    }

    /* get ThreadInstrumentation in DN */
    ThreadInstrumentation* getThreadInstrumentationDN(int planNodeId, int smpId)
    {
        Assert(planNodeId >= 1 && planNodeId <= m_plannodes_num);
        int offset = m_planIdOffsetArray[planNodeId - 1] * m_query_dop;
        return m_threadInstrArray[offset + smpId];
    }

    /* get ThreadInstrumentation */
    ThreadInstrumentation *getThreadInstrumentation(int idx, int planNodeId, int smpId)
    {
        ThreadInstrumentation *threadInstr =
#ifdef ENABLE_MULTIPLE_NODES
            getThreadInstrumentationCN(idx, planNodeId, smpId);
#else
            getThreadInstrumentationDN(planNodeId, smpId);
#endif /* ENABLE_MULTIPLE_NODES */
        return threadInstr;
    }

    /* get threadinstrumentation of current CN */
    ThreadInstrumentation* get_cnthreadinstrumentation(int index)
    {
        Assert(index >= 0 && index <= m_threadInstrArrayLen);

        return m_threadInstrArray[index];
    }

    int get_startnodeid()
    {
        /* get start_node_id of DN*/
        for (int i = 1; i < m_plannodes_num + 1; i++) {
            if (m_planIdOffsetArray[i - 1] == 1) {
                return i;
            }
        }

        return 1;
    }

private:
    /* number of data nodes. */
    int m_nodes_num;

    /* number of stream nodes. */
    int m_num_streams;

    /* gather operator count */
    int m_gather_count;

    /* query dop. */
    int m_query_dop;

    /* number of plan nodes. */
    int m_plannodes_num;

    // the query id of the global instrumentation.
    uint64 m_query_id;

    /* start node id of plan. */
    int m_start_node_id;

    /* dop of each plan nodes. */
    int* m_node_dops;

    /* how many thread instrumentation */
    int m_threadInstrArrayLen;

    /* thread info for this query*/
    ThreadInstrInfo* m_streamInfo;

    int m_streamInfoIdx;

    /* instrumentation flag. */
    int m_option;

    /* track flag */
    bool m_trackoption;
};

// Maximal number of counters support
//
#define MAX_CPU_COUNTERS 3

// Predefined PMU counter groups
//
typedef enum CPUMonGroupID {
    CMON_GENERAL = 0,
} CPUMonGroupID;

// For view of gs_wlm_operator_history
//
typedef enum WlmOperatorstatus {
    Operator_Normal = 0,
    Operator_Pending,
    Operator_Invalid,
} WlmOperatorstatus;

typedef struct CPUMonGroup {
    // group identifier
    //
    CPUMonGroupID m_id;
    // list of counters
    //
    int m_cCounters;
    int m_counters[MAX_CPU_COUNTERS];
} CPUMonGroup;

// Core class implementing PMU counters monitoring
//
class CPUMon {
public:
    static THR_LOCAL bool m_has_perf;
    static THR_LOCAL bool m_has_initialize;

private:
    // Number of counters in the group
    //
    static THR_LOCAL int m_cCounters;

    // File descriptor for each counter
    //
    static THR_LOCAL int m_fd[MAX_CPU_COUNTERS];

private:
    FORCE_INLINE static void validCounter(_in_ int i)
    {
        DBG_ASSERT(i >= 0 && i < m_cCounters);
    }

public:
    static void Initialize(_in_ CPUMonGroupID id);
    static void Shutdown();
    static int64 ReadCounter(_in_ int i);
    static int ReadCounters(_out_ int64* pCounters);
    static int ResetCounters(_out_ int64* pCounters);
    static int RestartCounters(_out_ int64* pCounters, _out_ int64* pAccumCounters);
    static int AccumCounters(__inout int64* pCounters, __inout int64* pAccumCounters);
    static bool get_perf();
};

typedef struct OBSRuntimeInfo {
    NameData relname;
    int32 file_scanned;
    int64 data_size;
    double actual_time;
    int32 format;
} OBSRuntimeInfo;

/* class to collect the OBS runtime info. */
class OBSInstrumentation : public BaseObject {
public:
    // construnctor
    OBSInstrumentation();

    // deconstructor
    ~OBSInstrumentation();

    /* called by HdfsEndForeignScan to collect node data */
    void save(const char* relname, int file_scanned, int64 data_size, double actual_time, int32 format);

    /* send the obs runimte info to uppler node. */
    void serializeSend();

    /* receive data from lower node. */
    void deserialize(char* msg, size_t len);

    /* insert obs runtime data to pg_obsscaninfo. */
    void insertData(uint64 queryid);

    bool* m_p_globalOBSInstrument_valid;

private:
    List* m_rels;
    MemoryContext m_ctx;
};

typedef struct Qpid {
    Oid procId;     /* cn thread id for the statement */
    uint64 queryId; /* debug query id for statement */
    int plannodeid;
} Qpid;

typedef struct OperatorProfileTable {
    int max_realtime_num;    /* max session info num in the hash table */
    int max_collectinfo_num; /* max collect info num in list */

    HTAB* explain_info_hashtbl;   /* collect information hash table */
    HTAB* collected_info_hashtbl; /* collect information hash table */
} OperatorProfileTable;

typedef struct OperatorPlanInfo {
    char*	operation;         /* operator name */
    char*	orientation;       /* row or column */
    char*	strategy;          /* physical attributes of the operator */
    char*	options;           /* logical attributes of the operator */
    char*	condition;         /* predicates */
    char*	projection;        /* projection of operator */
    int		parent_node_id;    /* id of parent node */
    int		left_child_id;     /* id of the left child */
    int		right_child_id;    /* id of the right child */
} OperatorPlanInfo;

typedef struct OperatorInfo {
    int64 total_tuples; /* Total tuples produced */
    int64 peak_memory;
    int64 spill_size;
    TimestampTz enter_time;
    int64 startup_time;
    int64 execute_time;
    uint status;
    uint warning;

    int ec_operator;
    int ec_status;
    char* ec_execute_datanode;
    char* ec_dsn;
    char* ec_username;
    char* ec_query;
    int ec_libodbc_type;
    int64 ec_fetch_count;
    OperatorPlanInfo planinfo;

    char*	datname;
} OperatorInfo;

typedef struct ExplainDNodeInfo {
    Qpid qid;                 /* id for statement on dn */
    Oid userid;               /* user id */
    bool execute_on_datanode; /* execute on datanode */
    bool can_record_to_table; /* flag for whether to record the information table */
    WlmOperatorstatus status; /* the status for this operator */
    char* plan_name;          /* plan node name */
    int query_dop;            /* query dop */
    int64 estimated_rows;     /* plan estimated rows */
    void* explain_entry;      /* session instrumentation entry */
    OperatorInfo geninfo;     /* general info for the dnode collect info */
    TimestampTz exptime;      /* record expire time */
} ExplainDNodeInfo;

typedef struct ExplainGeneralInfo {
    ThreadId tid;             /* thread id for statement */
    uint64 query_id;          /* debug query id */
    int plan_node_id;         /* plan node id */
    char* plan_node_name;     /* plan node name */
    TimestampTz start_time;   /* start execute time for operator */
    double startup_time;      /* the latency of the first output tuple */
    double duration_time;     /* max execute time */
    int query_dop;            /* query dop */
    int64 estimate_rows;      /* plan estimated rows */
    int64 tuple_processed;    /* plan processed rows */
    int32 min_peak_memory;    /* min peak memory for each datanode */
    int32 max_peak_memory;    /* max peak memory for each datanode */
    int32 avg_peak_memory;    /* average peak memory for each datanode */
    int memory_skewed;        /* peak memory skew for each datanode */
    int32 min_spill_size;     /* min spill size for each datanode */
    int32 max_spill_size;     /* max spill size for each datanode */
    int32 avg_spill_size;     /* average spill size for each datanode */
    int i_o_skew;             /* spill size skew for each datanode */
    int64 min_cpu_time;       /* min execute time for each datanode */
    int64 max_cpu_time;       /* max execute time for each datanode */
    int64 total_cpu_time;     /* total execute time for each datanode */
    int cpu_skew;             /* execute time skew for each datanode */
    int warn_prof_info;       /* warning info */
    bool status;              /* execute status */
    bool execute_on_datanode; /* execute on datanode */
    bool remove;              /* remove info from hash table */

    int ec_operator;           /* is ec operator */
    int ec_status;             /* ec task status */
    char* ec_execute_datanode; /* ec execute datanode name*/
    char* ec_dsn;              /* ec dsn*/
    char* ec_username;         /* ec username*/
    char* ec_query;            /* ec execute query*/
    int ec_libodbc_type;       /* ec execute libodbc_type*/
    int64 ec_fetch_count;      /* ec fetch count*/

    char* operation;
    char* orientation;
    char* strategy;
    char* options;
    char* condition;
    char* projection;
    int parent_node_id;
    int left_child_id;
    int right_child_id;
    char* datname;
} ExplainGeneralInfo;

typedef struct size_info {
    int64 total_tuple;
    int64 total_memory;
    int64 total_spill_size;
    int64 total_cpu_time;
    int64 max_peak_memory;
    int64 min_peak_memory;
    int64 max_spill_size;
    int64 min_spill_size;
    int64 min_cpu_time;
    int64 max_cpu_time;
    TimestampTz start_time;
    double startup_time;
    double duration_time; /* max execute time */
    char* plan_node_name;
    uint warn_prof_info;
    uint status;
    bool has_data;
    int dn_count;

    int ec_operator;
    int ec_status;
    char* ec_execute_datanode;
    char* ec_dsn;
    char* ec_username;
    char* ec_query;
    int ec_libodbc_type;
    int64 ec_fetch_count;
} size_info;

typedef struct IterationStats {
    int totalIters;
    struct timeval currentStartTime;
    int levelBuf[SW_LOG_ROWS_FULL];
    int64 rowCountBuf[SW_LOG_ROWS_FULL];
    struct timeval startTimeBuf[SW_LOG_ROWS_FULL];
    struct timeval endTimeBuf[SW_LOG_ROWS_FULL];
} IterationStats;

extern OperatorProfileTable g_operator_table;

extern Instrumentation* InstrAlloc(int n, int instrument_options);
extern void InstrStartNode(Instrumentation* instr);
extern void InstrStopNode(Instrumentation* instr, double nTuples, bool containMemory = true);
extern void InstrEndLoop(Instrumentation* instr);
extern void StreamEndLoop(StreamTime* instr);
extern void AddControlMemoryContext(Instrumentation* instr, MemoryContext context);
extern void CalculateContextSize(MemoryContext ctx, int64* memorySize);
extern void NetWorkTimePollStart(Instrumentation* instr);
extern void NetWorkTimePollEnd(Instrumentation* instr);
extern void NetWorkTimeDeserializeStart(Instrumentation* instr);
extern void NetWorkTimeDeserializeEnd(Instrumentation* instr);
extern void NetWorkTimeCopyStart(Instrumentation* instr);
extern void NetWorkTimeCopyEnd(Instrumentation* instr);
extern void SetInstrNull();

extern void StreamTimeSendStart(Instrumentation* instr);
extern void StreamTimeSendEnd(Instrumentation* instr);
extern void StreamTimeWaitQuotaStart(Instrumentation* instr);
extern void StreamTimeWaitQuotaEnd(Instrumentation* instr);
extern void StreamTimeOSSendStart(Instrumentation* instr);
extern void StreamTimeOSSendEnd(Instrumentation* instr);
extern void StreamTimeSerilizeStart(Instrumentation* instr);
extern void StreamTimeSerilizeEnd(Instrumentation* instr);
extern void StreamTimeCopyStart(Instrumentation* instr);
extern void StreamTimeCopyEnd(Instrumentation* instr);

extern LWLock* LockOperRealTHashPartition(uint32 hashCode, LWLockMode lockMode);
extern LWLock* LockOperHistHashPartition(uint32 hashCode, LWLockMode lockMode);
extern void UnLockOperRealTHashPartition(uint32 hashCode);
extern void UnLockOperHistHashPartition(uint32 hashCode);
extern void ExplainCreateDNodeInfoOnDN(
    Qpid* qid, Instrumentation* instr, bool on_dn, const char* plan_name, int dop, int64 estimated_rows);
extern uint32 GetHashPlanCode(const void* key1, Size keysize);
extern void InitOperStatProfile(void);
extern void* ExplainGetSessionStatistics(int* num);
extern void ExplainSetSessionInfo(int plan_node_id, Instrumentation* instr, bool on_datanode, const char* plan_name,
    int dop, int64 estimated_rows, TimestampTz current_time, OperatorPlanInfo* opt_plan_info);
extern void* ExplainGetSessionInfo(const Qpid* qid, int removed, int* num);
extern void releaseExplainTable();
extern bool IsQpidInvalid(const Qpid* qid);
extern void sendExplainInfo(OperatorInfo* sessionMemory);
extern void ExplainStartToGetStatistics(void);
extern void removeExplainInfo(int plan_node_id);
extern void OperatorStrategyFunc4SessionInfo(StringInfo msg, void* suminfo, int size);
extern void setOperatorInfo(OperatorInfo* operatorMemory, Instrumentation* InstrMemory,
    OperatorPlanInfo* opt_plan_info = NULL);
extern int64 e_rows_convert_to_int64(double plan_rows);
extern void releaseOperatorInfoEC(OperatorInfo* sessionMemory);
#endif /* INSTRUMENT_H */
