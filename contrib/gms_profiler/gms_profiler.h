/*
 *
 * gms_profiler.h
 *     Definition about gms_profiler package.
 *
 *
 * IDENTIFICATION
 *        contrib/gms_profiler/gms_profiler.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef GMS_PROFILER_H
#define GMS_PROFILER_H

#define PROFILER_UNIT_HASH_NUMBER 128
#define PROFILER_DATA_HASH_NUMBER 1024
#define PROFILER_UNIT_STACK_SIZE 1024

#define PROFILER_ERROR_OK 0
#define PROFILER_ERROR_PARAM 1
#define PROFILER_ERROR_IO 2
#define PROFILER_ERROR_VERSION -1

typedef enum
{
    PROFILER_INACTIVE = 0,
    PROFILER_ACTIVE = 1,
    PROFILER_PAUSED = 2
}ProfilerState;

typedef struct ProfilerContext
{
    MemoryContext mem_cxt;
    ProfilerState state;
    // run info
    uint32 runid;
    Oid run_owner;
    TimestampTz run_timestamp;
    char *run_comment;
    char *run_comment1;
    int64 run_total_time;
    bool runinfo_flushed;
    // unit info
    uint32 current_unit_number;
    uint32 max_unit_number;
    uint32 *unit_stack;
    uint32 unit_stack_top;
    uint32 in_stmt;
    HTAB *unit_hash;
    HTAB *data_hash;
}ProfilerContext;

typedef struct ProfilerDataHashKey
{
    uint32 unit_number;
    uint32 lineno;
}ProfilerDataHashKey;

typedef struct ProfilerDataHashEntry
{
    uint32 unit_number;
    uint32 lineno;
    TimestampTz stmt_timestamp;
    uint32 total_occur;
    int64 total_time;
    int64 min_time;
    int64 max_time;
    bool data_flushed;
}ProfilerDataHashEntry;

typedef struct ProfilerUnitHashKey
{
    uint32 unit_oid;
}ProfilerUnitHashKey;

typedef struct ProfilerUnitHashEntry
{
    uint32 unit_oid;
    uint32 unit_number;
    TimestampTz unit_timestamp;
    int64 total_time;
    bool unit_flushed;
}ProfilerUnitHashEntry;

/*
 * External declarations
 */
extern "C" Datum start_profiler(PG_FUNCTION_ARGS);
extern "C" Datum start_profiler_1(PG_FUNCTION_ARGS);
extern "C" Datum start_profiler_ext(PG_FUNCTION_ARGS);
extern "C" Datum start_profiler_ext_1(PG_FUNCTION_ARGS);
extern "C" Datum stop_profiler(PG_FUNCTION_ARGS);
extern "C" Datum flush_data(PG_FUNCTION_ARGS);
extern "C" Datum pause_profiler(PG_FUNCTION_ARGS);
extern "C" Datum resume_profiler(PG_FUNCTION_ARGS);
extern "C" void set_extension_index(uint32 index);
extern "C" void init_session_vars(void);
#endif
