#ifndef SRC_BIN_PG_CTL_STREAMUTIL_H
#define SRC_BIN_PG_CTL_STREAMUTIL_H
#include "libpq/libpq-fe.h"

extern const char* progname;

extern char* replication_slot;

/* Connection kept global so we can disconnect easily */
extern PGconn* streamConn;

extern char* xstrdup(const char* s);
extern void* xmalloc0(int size);

#endif  // SRC_BIN_PG_CTL_STREAMUTIL_H