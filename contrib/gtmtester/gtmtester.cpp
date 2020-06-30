#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <stdint.h>
#include <pthread.h>
#include "gtm/libpq-fe.h"
#include "gtm/gtm_c.h"
#include "gtm/gtm_client.h"
#include "gtm/gtm.h"
#include "gtm/memutils.h"
#include "../test/gtmtest/gtm_cycles.h"

#define TransactionIdIsValid(xid) ((xid) != InvalidTransactionId)
#define InvalidTransactionId ((TransactionId)0)

#define MAX_HOST_NAME 256
#define MAX_NUM_CLIENTS 16000

typedef struct gtmtest_opts {
    char host[MAX_HOST_NAME];
    int port;
    int numClients;
    int runTime;
    int targetLoad;
} gtmtest_opts;

class gtmtest_latency_stats {
public:
    gtmtest_latency_stats() : numSamples(0), totalCycles(0), startTime(0)
    {}

    ~gtmtest_latency_stats()
    {}

    void start()
    {
        startTime = GTM_Cycles::rdtsc();
    }

    void finish()
    {
        uint64_t cyclesElapsed = GTM_Cycles::rdtsc() - startTime;
        totalCycles += cyclesElapsed;
        numSamples++;
    }

    void addResults(const gtmtest_latency_stats& other)
    {
        numSamples += other.numSamples;
        totalCycles += other.totalCycles;
    }

    uint64_t getNumSamples()
    {
        return numSamples;
    }

    uint64_t getTotalCycles()
    {
        return totalCycles;
    }

private:
    /* collected statistics */
    uint64_t numSamples;
    uint64_t totalCycles;

    // temporary state
    uint64_t startTime;
};

enum gtmtest_optype {
    gtmtest_op_begintrans = 0,
    gtmtest_op_getgxid,
    gtmtest_op_getsnapshot,
    gtmtest_op_commit,
    gtmtest_op_count,
};

typedef struct gtmtest_thread_arg {
    int idx;
    gtmtest_opts* opts;
    pthread_t thrId;
    uint64_t numXacts;
    uint64_t startTime;
    uint64_t endTime;
    gtmtest_latency_stats opStats[gtmtest_op_count];
} gtmtest_thread_arg;

gtmtest_opts testOpts = {0};
int gtmtest_should_stop = 0;
gtmtest_thread_arg* threadsArg = NULL;
pthread_t TopMostThreadID;
pthread_key_t threadinfo_key;
int tcp_keepalives_idle = 30;
int tcp_keepalives_interval = 30;
int tcp_keepalives_count = 3;
extern int log_min_messages;

void gtm_usleep(long microsec)
{
    if (microsec > 0) {
        struct timeval delay;
        delay.tv_sec = microsec / 1000000L;
        delay.tv_usec = microsec % 1000000L;
        (void)select(0, NULL, NULL, NULL, &delay);
    }
}

static void gtmtest_wait_until(uint64_t timeCycles)
{
    while (true) {
        uint64_t curTime = GTM_Cycles::rdtsc();
        if (curTime >= timeCycles)
            break;
        uint64_t waitUs = GTM_Cycles::toMicroseconds(timeCycles - curTime);
        if (waitUs > 100) {
            gtm_usleep(waitUs);
        }
    }
}

static const char* gtmtest_optype_name(int opType)
{
    switch (opType) {
        case gtmtest_op_begintrans:
            return "BeginTrans";
        case gtmtest_op_getgxid:
            return "GetGXID";
        case gtmtest_op_getsnapshot:
            return "GetSnapshot";
        case gtmtest_op_commit:
            return "Commit";
        default:
            return "Invalid";
    }
}

static void gtmtest_help()
{
    printf("Usage: gtmtester -h <host> -p <port> -c <num clients> -t <run time (secs)> -l <target tpcc load>\n\n");
    printf("HINT: ./gtmtester -h localhost  -p 36666 -c 10 -t 15 -l 100000\n");
}

static void gtmtest_print_test_opts(gtmtest_opts* opts)
{
    if (opts == NULL)
        return;

    printf("Test options:\n"
           "\thostname: %s\n"
           "\tport: %d\n"
           "\tnumClients: %d\n"
           "\trunTime: %d\n"
           "\ttargetLoad: %d\n\n",
        opts->host,
        opts->port,
        opts->numClients,
        opts->runTime,
        opts->targetLoad);
}

static int gtmtest_parse_cli(int argc, char** argv, gtmtest_opts* opts)
{
    int c = 0;
    int rc = 0;
    if (opts != NULL) {
        while ((c = getopt(argc, argv, "h:p:c:t:l:")) != -1)
            switch (c) {
                case 'h':
                    rc = strncpy_s(opts->host, MAX_HOST_NAME, optarg, MAX_HOST_NAME - 1);
                    securec_check(rc, "\0", "\0");
                    break;

                case 'p':
                    opts->port = atoi(optarg);
                    break;

                case 'c':
                    opts->numClients = atoi(optarg);
                    if (opts->numClients > MAX_NUM_CLIENTS) {
                        printf("Max number of clients is too large - try a value under %d\n", MAX_NUM_CLIENTS);
                        return -1;
                    }
                    break;

                case 't':
                    opts->runTime = atoi(optarg);
                    break;

                case 'l':
                    opts->targetLoad = atoi(optarg);
                    break;

                case '?':
                    if (optopt == 'c' || optopt == 'h' || optopt == 'p' || optopt == 't')
                        printf("Option -%c requires an argument.\n", optopt);
                    else if (isprint(optopt))
                        printf("Unknown option `-%c'.\n", optopt);
                    else
                        printf("Unknown option character `\\x%x'.\n", optopt);
                    return -1;
                default:
                    /* could not be here, keep compiler slient */
                    break;
            }

        if (strlen(opts->host) == 0) {
            printf("Please specify the GTM host.\n");
            return -1;
        }

        if (opts->port == 0) {
            printf("Please specify the GTM port.\n");
            return -1;
        }

        if (opts->numClients <= 0) {
            opts->numClients = 10;
        }

        if (opts->runTime <= 0) {
            opts->runTime = 10;
        }

        return 0;
    }

    return -1;
}

static GTM_Conn* gtmtester_connect(char* host, int port)
{
    GTMConnStatusType conn_status;
    GTM_Conn* conn = NULL;
    char conn_str[1024] = {0};

    int rc = snprintf_s(conn_str,
        sizeof(conn_str),
        sizeof(conn_str) - 1,
        "host=%s port=%d node_name=%s connect_timeout=%d",
        host,
        port,
        "coordinator1",
        10);
    securec_check_ss(rc, "\0", "\0");

    conn = PQconnectGTM(conn_str);
    conn_status = GTMPQstatus(conn);
    if (conn_status != CONNECTION_OK) {
        printf("could not connect to GTM, the connection info: %s: %s", conn_str, GTMPQerrorMessage(conn));
        GTMPQfinish(conn);
        conn = NULL;
    }

    return conn;
}

void gtmtester_run_new_order_xact(
    GTM_Conn* conn, gtmtest_thread_arg* thread_arg, uint64_t xactStartTime, uint64_t cyclesPerXact)
{
    GTM_TransactionKey key;
    GTM_Timestamp timestamp;
    GlobalTransactionId gxid;
    GTMClientErrCode err = GTMC_OK;
    int i;
    uint64 commit_csn = 0;

    thread_arg->opStats[gtmtest_op_begintrans].start();
    key = begin_transaction(conn, GTM_ISOLATION_RC, &timestamp);
    thread_arg->opStats[gtmtest_op_begintrans].finish();

    if (key.txnHandle == InvalidTransactionHandle) {
        printf("begin_transaction failed\n");
        exit(1);
    }

    thread_arg->opStats[gtmtest_op_getgxid].start();
    gxid = begin_get_gxid(conn, key, false, &err);
    thread_arg->opStats[gtmtest_op_getgxid].finish();

    if (!TransactionIdIsValid(gxid)) {
        printf("invalid gxid\n");
        exit(1);
    }

    for (i = 0; i < 40; i++) {
        if (cyclesPerXact != 0 && i > 0) {
            gtmtest_wait_until(xactStartTime + (cyclesPerXact / 40 * i));
        }
        thread_arg->opStats[gtmtest_op_getsnapshot].start();
        GTM_Snapshot snapshot = get_snapshot(conn, key, gxid, false, false);
        thread_arg->opStats[gtmtest_op_getsnapshot].finish();
        if (snapshot == NULL) {
            printf("invalid snapshot\n");
            exit(1);
        }
    }

    thread_arg->opStats[gtmtest_op_commit].start();
    int commitStatus = commit_transaction_handle(conn, key, &gxid, false, &commit_csn);
    thread_arg->opStats[gtmtest_op_commit].finish();
    if (commitStatus != 0) {
        printf("failed to commit\n");
        exit(1);
    }
}

void* gtmtest_worker(void* argp)
{
    GTM_ThreadInfo* thrinfo = NULL;
    GTM_Conn* conn = NULL;
    gtmtest_thread_arg* arg = (gtmtest_thread_arg*)argp;
    uint64_t startTime = 0;
    uint64_t endTime = 0;
    uint64_t numXacts = 0;

    thrinfo = (GTM_ThreadInfo*)calloc(1, sizeof(GTM_ThreadInfo));
    if (SetMyThreadInfo(thrinfo)) {
        printf("failed to set thread info\n");
        exit(1);
    }

    thrinfo->thr_thread_context = AllocSetContextCreate(TopMemoryContext,
        "TopMemoryContext",
        ALLOCSET_DEFAULT_MINSIZE,
        ALLOCSET_DEFAULT_INITSIZE,
        ALLOCSET_DEFAULT_MAXSIZE,
        false);

    thrinfo->thr_error_context =
        AllocSetContextCreate(ErrorContext, "ErrorContext", 8 * 1024, 8 * 1024, 8 * 1024, false);

    conn = gtmtester_connect(arg->opts->host, arg->opts->port);
    if (conn == NULL) {
        printf("could not connect to GTM!\n");
        return NULL;
    }

    /*
     * copmpute average time (in cycles) to be spent per transaction
     * to achieve the target load
     */
    uint64_t cyclesPerXact = 0;
    if (arg->opts->targetLoad != 0) {
        double targetPerSec = arg->opts->targetLoad / 0.45 / 60.0 / arg->opts->numClients;
        cyclesPerXact = GTM_Cycles::perSecond() / targetPerSec;
    }

    startTime = GTM_Cycles::rdtsc();
    while (!gtmtest_should_stop) {
        uint64_t xactStartTime = 0;
        if (cyclesPerXact != 0) {
            xactStartTime = startTime + (numXacts * cyclesPerXact);
            gtmtest_wait_until(xactStartTime);
        }
        gtmtester_run_new_order_xact(conn, arg, xactStartTime, cyclesPerXact);
        numXacts++;
    }
    endTime = GTM_Cycles::rdtsc();
    GTMPQfinish(conn);

    arg->numXacts = numXacts;
    arg->startTime = startTime;
    arg->endTime = endTime;
    return NULL;
}

void gtmtest_create_threads(gtmtest_opts* opts)
{
    int err;
    int i;
    pthread_attr_t pThreadAttr;

    printf("Creating threads ");
    threadsArg = (gtmtest_thread_arg*)calloc(opts->numClients, sizeof(gtmtest_thread_arg));
    if (threadsArg == NULL) {
        printf("Out of memory\n");
        exit(1);
    }

    pthread_attr_init(&pThreadAttr);
    pthread_attr_setstacksize(&pThreadAttr, 2048 * 1024L);
    for (i = 0; i < opts->numClients; i++) {
        printf(".");
        fflush(stdout);
        threadsArg[i].idx = i;
        threadsArg[i].opts = opts;
        if ((err = pthread_create(&threadsArg[i].thrId, &pThreadAttr, gtmtest_worker, (void*)&threadsArg[i]))) {
            printf("failed to create thread %d\n", i);
            exit(1);
        }
    }

    printf(" Done\n");
}

int main(int argc, char** argv)
{
    int i = 0;
    uint64_t totalXacts = 0;
    uint64_t totalTime = 0;
    gtmtest_latency_stats totalOpStats[gtmtest_op_count];

    log_min_messages = FATAL; /* disable stderr logging */
    TopMostThreadID = pthread_self();
    pthread_key_create(&threadinfo_key, NULL);

    printf("GtmTester\n");
    if (gtmtest_parse_cli(argc, argv, &testOpts)) {
        gtmtest_help();
        exit(1);
    }

    gtmtest_print_test_opts(&testOpts);

    gtmtest_create_threads(&testOpts);

    printf("\nRunning ");
    for (i = 0; i < testOpts.runTime; i++) {
        printf(".");
        fflush(stdout);
        sleep(1);
    }

    gtmtest_should_stop = 1;
    printf(" Done\n");
    for (i = 0; i < testOpts.numClients; i++) {
        pthread_join(threadsArg[i].thrId, NULL);
    }

    printf("Results:\n");
    for (i = 0; i < testOpts.numClients; i++) {
        printf("[%d] num xacts: %ld, time: %ld\n",
            i,
            threadsArg[i].numXacts,
            threadsArg[i].endTime - threadsArg[i].startTime);
        totalXacts += threadsArg[i].numXacts;
        totalTime += GTM_Cycles::toMicroseconds(threadsArg[i].endTime - threadsArg[i].startTime);
        for (int op = 0; op < gtmtest_op_count; op++) {
            totalOpStats[op].addResults(threadsArg[i].opStats[op]);
        }
    }

    totalTime /= testOpts.numClients; /* avg. run time for all threads */

    uint64_t tps = (uint64_t)(totalXacts / (totalTime / 1000000.0));
    uint64_t totalTpmC = tps * 60;
    uint64_t tpmC = (uint64_t)(totalTpmC * 0.45);

    printf("Total: %lu xacts in %lu micros - %lu tps, %lu total-tpmC, %lu tmpC\n",
        totalXacts,
        totalTime,
        tps,
        totalTpmC,
        tpmC);

    for (int op = 0; op < gtmtest_op_count; op++) {
        if (totalOpStats[op].getNumSamples() != 0) {
            uint64_t avgLatencyUs =
                GTM_Cycles::toMicroseconds(totalOpStats[op].getTotalCycles() / totalOpStats[op].getNumSamples());
            printf("Operation %s avg latency %lu us\n", gtmtest_optype_name(op), avgLatencyUs);
        }
    }

    return 0;
}
