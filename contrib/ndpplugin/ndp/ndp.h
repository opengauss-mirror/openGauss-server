/* -------------------------------------------------------------------------
 *
 * ndp.h
 *	  Exports from ndp/ndp.cpp
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/ndp/ndp.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef NDP_H_
#define NDP_H_

#include "component/rpc/rpc.h"
#include "utils/config.h"

void* NdpMain(void* arg);

init_type NdpWorkerInit();
void NdpWorkerUnInit();

Status SubmitAioReadData(NdpIOTask* task);
#endif /* NDP_H_ */


