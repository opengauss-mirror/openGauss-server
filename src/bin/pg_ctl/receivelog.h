#ifndef SRC_BIN_PG_CTL_RECEIVELOG_H
#define SRC_BIN_PG_CTL_RECEIVELOG_H
#include "access/xlogdefs.h"

/*
 * Called before trying to read more data or when a segment is
 * finished. Return true to stop streaming.
 */
typedef bool (*stream_stop_callback)(XLogRecPtr segendpos, uint32 timeline, bool segment_finished);

extern bool ReceiveXlogStream(PGconn* conn, XLogRecPtr startpos, uint32 timeline, const char* sysidentifier,
    const char* basedir, stream_stop_callback stream_stop, int standby_message_timeout, bool rename_partial);

#endif  // SRC_BIN_PG_CTL_RECEIVELOG_H