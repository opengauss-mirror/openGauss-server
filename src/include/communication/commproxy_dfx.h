/*-------------------------------------------------------------------------
 *
 * com_dfx.h
 *      api for socket programming
 *
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/communication/com_dfx.h
 *
 * NOTES
 *      Libnet or default tcp socket programming
 *
 *-------------------------------------------------------------------------
 */
#ifndef COMMPROXY_DFX_H
#define COMMPROXY_DFX_H

#include "communication/commproxy_interface.h"


class CommDFX {
public:
    static unsigned long int m_global_request_id;

public:
    CommDFX();
    ~CommDFX();

public:
    static void print_debug_detail(SocketRequest* sockreq);
    static unsigned long int GetRequestId();
};

#define SET_REQ_DFX_START(req)                \
    (req).s_req_id = CommDFX::GetRequestId(); \
    (req).s_step = comm_exec_step_start;

#define SET_REQ_DFX_PREPARE(req) (req).s_step = comm_exec_step_prepare;

#define SET_REQ_DFX_PROCESS(req) (req).s_step = comm_exec_step_process;

#define SET_REQ_DFX_READY(req) (req).s_step = comm_exec_step_ready;

#define SET_REQ_DFX_DONE(req) (req).s_step = comm_exec_step_done;(req).printf_req();

#endif /* COMMPROXY_DFX_H */
