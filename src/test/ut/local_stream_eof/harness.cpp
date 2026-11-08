/* Deterministic protocol test, injecting producer publication at a state load.
 * The receive state machine itself is extracted unchanged from libcomm_memory.
 * No concurrent C++ data race or probabilistic sleep is used in this harness. */
#include <cstdio>
#include <stdexcept>

enum DataStatus { DATA_EMPTY, DATA_PREPARE, DATA_READY, CONN_ERR };
enum { ERROR = 1 };
#define STREAM_SCAN_FINISH 'F'
#define STREAM_SCAN_WAIT 'W'
#define STREAM_SCAN_DATA 'D'
#define ereport(level, details) throw std::runtime_error("connection error")
#define pg_read_barrier() do {} while (0)
#define pg_memory_barrier() do {} while (0)

static void after_load();
struct StatusCell {
    DataStatus value;
    operator DataStatus() const
    {
        DataStatus result = value;
        after_load();
        return result;
    }
};
struct EndCell {
    bool value;
    operator bool() const
    {
        bool result = value;
        after_load();
        return result;
    }
};
struct hash_entry { void _signal() {} };
struct Message { int len; int cursor; char* data; };
typedef Message* StringInfo;
struct Shared {
    StatusCell dataStatus[1][2];
    EndCell is_connect_end[1][2];
    int scanLoc[1];
    StringInfo messages[1][2];
    hash_entry* quota_entrys[1][2];
};
struct StreamState { Shared* sharedContext; int conn_count; };
struct Session { struct { int smp_id; } stream_cxt; } session = {{0}};
static Session* u_sess = &session;
static Shared shared;
static Message messages[2];
static hash_entry entries[2];
static int pending[2], delivered, loads, publish_after, publish_channel, publish_count;
static DataStatus publish_status;

static void publish()
{
    pending[publish_channel] = publish_count;
    shared.dataStatus[0][publish_channel].value = publish_status;
    shared.is_connect_end[0][publish_channel].value = true;
}
static void after_load()
{
    if (++loads == publish_after)
        publish();
}
static bool gs_consume_memory_data(StreamState*, int channel)
{
    if (pending[channel] == 0)
        return false;
    delivered += pending[channel];
    pending[channel] = 0;
    shared.dataStatus[0][channel].value = DATA_EMPTY;
    return true;
}
static void HandleStreamError(StreamState*, char*, int) { throw std::runtime_error("worker error"); }
static void HandleStreamNotice(StreamState*, char*, int) {}
static void handle_end_command(StreamState*, char*, int) {}
static void resetStringInfo(StringInfo message) { message->len = message->cursor = 0; }

/* PRODUCTION_FUNCTION */

static void reset()
{
    shared = Shared{};
    delivered = loads = publish_after = publish_channel = publish_count = 0;
    for (int i = 0; i < 2; ++i) {
        pending[i] = 0;
        messages[i] = Message{};
        shared.messages[0][i] = &messages[i];
        shared.quota_entrys[0][i] = &entries[i];
    }
}

int main()
{
    int checks = 0, failures = 0;
    // Publication before, between, or after the two state loads; partial and
    // full final batches; producer can be either channel of a local stream.
    for (int connections = 1; connections <= 2; ++connections) {
        for (int channel = 0; channel < connections; ++channel) {
            for (int moment = 0; moment <= 2 * connections; ++moment) {
                for (DataStatus status : {DATA_PREPARE, DATA_READY}) {
                    for (int count : {1, 17, 100}) {
                        reset();
                        for (int i = 0; i < connections; ++i)
                            shared.is_connect_end[0][i].value = (i != channel);
                        publish_after = moment;
                        publish_channel = channel;
                        publish_count = count;
                        publish_status = status;
                        if (moment == 0)
                            publish();
                        StreamState node = {&shared, connections};
                        int waiters = -1;
                        bool lost = false, ended = false;
                        for (int poll = 0; poll < 4; ++poll) {
                            char result = gs_find_memory_data(&node, &waiters);
                            if (result == STREAM_SCAN_FINISH) {
                                lost = pending[0] + pending[1] != 0 || delivered != count;
                                ended = true;
                                break;
                            }
                        }
                        ++checks;
                        if (lost || !ended) {
                            ++failures;
                            std::printf("FAIL conns=%d channel=%d after_load=%d status=%d rows=%d delivered=%d\n",
                                connections, channel, moment, status, count, delivered);
                        }
                    }
                }
            }
        }
    }
    // Completed empty streams, live empty streams, and notice/error handling.
    for (int connections = 1; connections <= 2; ++connections) {
        reset();
        StreamState node = {&shared, connections};
        int waiters = -1;
        ++checks;
        if (gs_find_memory_data(&node, &waiters) != STREAM_SCAN_WAIT || waiters != connections)
            ++failures;
        for (int i = 0; i < connections; ++i)
            shared.is_connect_end[0][i].value = true;
        ++checks;
        if (gs_find_memory_data(&node, &waiters) != STREAM_SCAN_FINISH || waiters != 0)
            ++failures;
    }
    for (int kind = 0; kind < 3; ++kind) {
        reset();
        StreamState node = {&shared, 1};
        int waiters = -1;
        shared.is_connect_end[0][0].value = true;
        if (kind == 0)
            shared.dataStatus[0][0].value = CONN_ERR;
        else {
            messages[0].len = 1;
            messages[0].cursor = kind == 1 ? 'E' : 'N';
        }
        ++checks;
        try {
            char result = gs_find_memory_data(&node, &waiters);
            if (kind != 2 || result != STREAM_SCAN_WAIT || messages[0].len != 0)
                ++failures;
        } catch (const std::runtime_error&) {
            if (kind == 2)
                ++failures;
        }
    }
    std::printf("local Stream EOF interleavings: %d checks, %d failures\n", checks, failures);
    return failures ? 1 : 0;
}
