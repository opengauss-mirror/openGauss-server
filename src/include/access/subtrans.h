/*
 * subtrans.h
 *
 * openGauss subtransaction-log manager
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/access/subtrans.h
 */
#ifndef SUBTRANS_H
#define SUBTRANS_H

#include "access/clog.h"

extern void SubTransSetParent(TransactionId xid, TransactionId parent);
extern TransactionId SubTransGetParent(TransactionId xid, CLogXidStatus* status, bool force_wait_parent);
extern TransactionId SubTransGetTopmostTransaction(TransactionId xid);

#endif /* SUBTRANS_H */
