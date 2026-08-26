/* -------------------------------------------------------------------------
 * Copyright (c) 2026 Huawei Technologies Co.,Ltd.
 *
 * Non-blocking polling for coordinated local Stream consumers.  Keep this
 * narrow interface separate from execStream.h so ordinary executor modules
 * do not acquire a high-fanout dependency on HybridRecall support.
 * -------------------------------------------------------------------------
 */
#ifndef LOCAL_STREAM_POLL_H
#define LOCAL_STREAM_POLL_H

#include "executor/exec/execStream.h"

typedef enum StreamPollStatus {
    STREAM_POLL_WAIT = 0,
    STREAM_POLL_DATA,
    STREAM_POLL_FINISH
} StreamPollStatus;

extern StreamPollStatus ExecPollLocalStream(StreamState* node);
extern StreamPollStatus gs_memory_poll(StreamState* node);

#endif /* LOCAL_STREAM_POLL_H */
