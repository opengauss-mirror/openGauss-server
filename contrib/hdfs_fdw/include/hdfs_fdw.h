#ifndef HDFS_FDW_h
#define HDFS_FDW_h
#include "access/dfs/dfs_am.h"
#include "foreign/foreign.h"
#include "nodes/execnodes.h"
#include "nodes/pg_list.h"

#define HDFS_TUPLE_COST_MULTIPLIER 5

typedef enum OptionType {
    T_INVALID_TYPE = 0x0000,
    T_SERVER_COMMON_OPTION = 0x0001 << 1,
    T_OBS_SERVER_OPTION = 0x0001 << 2,
    T_HDFS_SERVER_OPTION = 0x0001 << 3,
    T_DUMMY_SERVER_OPTION = 0x0001 << 4,
    T_FOREIGN_TABLE_COMMON_OPTION = 0x0001 << 5,
    T_FOREIGN_TABLE_OBS_OPTION = 0x0001 << 6,
    T_FOREIGN_TABLE_HDFS_OPTION = 0x0001 << 7,
    T_SERVER_TYPE_OPTION = 0x0001 << 8,
    T_FOREIGN_TABLE_TEXT_OPTION = 0x0001 << 9,
    T_FOREIGN_TABLE_CSV_OPTION = 0x0001 << 10
} OptionType;

#define OBS_SERVER_OPTION (T_SERVER_COMMON_OPTION | T_OBS_SERVER_OPTION)
#define HDFS_SERVER_OPTION (T_SERVER_COMMON_OPTION | T_HDFS_SERVER_OPTION)
#define DUMMY_SERVER_OPTION (T_SERVER_COMMON_OPTION | T_DUMMY_SERVER_OPTION)
#define OBS_FOREIGN_TABLE_OPTION                                                               \
    (T_FOREIGN_TABLE_COMMON_OPTION | T_FOREIGN_TABLE_OBS_OPTION | T_FOREIGN_TABLE_CSV_OPTION | \
        T_FOREIGN_TABLE_TEXT_OPTION)

#define HDFS_FOREIGN_TABLE_OPTION                                                               \
    (T_FOREIGN_TABLE_COMMON_OPTION | T_FOREIGN_TABLE_HDFS_OPTION | T_FOREIGN_TABLE_CSV_OPTION | \
        T_FOREIGN_TABLE_TEXT_OPTION)

#define SERVER_TYPE_OPTION (T_SERVER_TYPE_OPTION)

#define DFS_OPTION_ARRAY                                                                           \
    (T_SERVER_COMMON_OPTION | T_HDFS_SERVER_OPTION | T_OBS_SERVER_OPTION | T_DUMMY_SERVER_OPTION | \
        HDFS_FOREIGN_TABLE_OPTION | OBS_FOREIGN_TABLE_OPTION)

#define SERVER_TYPE_OPTION_ARRAY (T_SERVER_TYPE_OPTION)

/*
 * HdfsValidOption keeps an option name and a context. When an option is passed
 * into hdfs_fdw objects (server and foreign table), we compare this option's
 * name and context against those of valid options.
 */
typedef struct HdfsValidOption {
    const char* optionName;
    uint32 optType;
} HdfsValidOption;

/*
 * FDW-specific information for RelOptInfo.fdw_private.
 */
typedef struct HdfsFdwPlanState {
    double tuplesCount; /* estimate of number of rows in file */
    HdfsFdwOptions* hdfsFdwOptions;
} HdfsFdwPlanState;

/*
 * HdfsFdwExecState keeps foreign data wrapper specific execution state that we
 * create and hold onto when executing the query.
 */
typedef struct HdfsFdwExecState {
    dfs::reader::ReaderState* readerState;

    dfs::reader::Reader* fileReader;
} HdfsFdwExecState;

/* Function declarations for foreign data wrapper */
extern "C" Datum hdfs_fdw_handler(PG_FUNCTION_ARGS);
extern "C" Datum hdfs_fdw_validator(PG_FUNCTION_ARGS);

#endif /* hdfs_fdw_H */
