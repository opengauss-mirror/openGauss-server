/* -------------------------------------------------------------------------
 *
 * logicalworker.h
 * 	  Exports for logical replication workers.
 *
 * Portions Copyright (c) 2010-2016, PostgreSQL Global Development Group
 *
 * src/include/replication/logicalworker.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef LOGICALWORKER_H
#define LOGICALWORKER_H

extern void ApplyWorkerMain();
extern bool IsLogicalWorker(void);

#endif /* LOGICALWORKER_H */
