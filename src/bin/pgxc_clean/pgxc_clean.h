#ifndef PGXC_CLEAN
#define PGXC_CLEAN

typedef struct database_names {
    struct database_names* next;
    char* database_name;
} database_names;

typedef struct tempschema_info {
    struct tempschema_info* next;
    char* tempschema_name;
} tempschema_info;

typedef struct activebackend_info {
    struct activebackend_info* next;
    int64 sessionID;
    uint32 tempID;
    uint32 timeLineID;
} activebackend_info;

typedef struct sessionid_info {
    struct sessionid_info* next;
    char* session_id;
} sessionid_info;

typedef enum CleanWorkerStatus { WRKR_INIT = 0, WRKR_IDLE, WRKR_WORKING, WRKR_TERMINATED } CleanWorkerStatus;

typedef struct CleanWorkerInfo {
    pthread_t thr_id;
    bool is_main_thread;
    int work_job;
    CleanWorkerStatus worker_status; /* current status of this worker */
    PGconn* conn;
} CleanWorkerInfo;

typedef struct GSCleanGlobalThreads {
    int num_workers;
    CleanWorkerInfo* worker_info;
} GSCleanGlobalThreads;

extern FILE* outf;
extern FILE* errf;

#endif /* PGXC_CLEAN */

