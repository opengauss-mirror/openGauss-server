/*
 * Copyright (c) 2021 Huawei Technologies Co.,Ltd.
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 *          http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 * -------------------------------------------------------------------------
 *
 * comm_dfx.cpp
 *        TODO add contents
 *
 *
 * IDENTIFICATION
 *        src/gausskernel/cbb/communication/comm_proxy/comm_dfx.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
#include "stdlib.h"
#include <unistd.h>
#include <iostream>
#include "string.h"
#include "communication/commproxy_dfx.h"
#include "comm_core.h"
#include "comm_proxy.h"
#include "utils/builtins.h"
#include "funcapi.h"

#define MONITORMESSAGEBUFFER 1024

CommDFX::CommDFX()
{}

CommDFX::~CommDFX()
{}

void CommDFX::print_debug_detail(SocketRequest* sockreq)
{
    ereport(DEBUG4, (errmodule(MOD_COMM_PROXY), errmsg("reqid:[%lu], ", sockreq->s_req_id)));
    switch (sockreq->s_sockreq_type) {
        case sockreq_close: {
            CommCloseInvokeParam* param = (CommCloseInvokeParam*)sockreq->s_request_data;
            ereport(DEBUG3, (errmodule(MOD_COMM_PROXY),
                errmsg("reqtype:[sockreq_close], param: fd:[%d]", param->s_fd)));
        } break;
        case sockreq_socket: {
            CommSocketInvokeParam* param = (CommSocketInvokeParam*)sockreq->s_request_data;
            ereport(DEBUG3, (errmodule(MOD_COMM_PROXY),
                errmsg("reqtype:[sockreq_socket], param: domain:[%d], type:[%d], protocol:[%d].",
                param->s_domain,
                param->s_type,
                param->s_protocol)));
        } break;
        case sockreq_accept: {
            CommAcceptInvokeParam* param = (CommAcceptInvokeParam*)sockreq->s_request_data;
            ereport(DEBUG3, (errmodule(MOD_COMM_PROXY),
                errmsg("reqtype:[sockreq_accept], param: server_fd:[%d]", param->s_fd)));
        } break;
        case sockreq_bind: {
            CommBindInvokeParam* param = (CommBindInvokeParam*)sockreq->s_request_data;
            ereport(DEBUG3, (errmodule(MOD_COMM_PROXY),
                errmsg("reqtype:[sockreq_bind], param: server_fd:[%d] addrlen:[%d]",
                param->s_fd,
                param->addrlen)));
        } break;
        case sockreq_listen: {
            CommListenInvokeParam* param = (CommListenInvokeParam*)sockreq->s_request_data;
            ereport(DEBUG3, (errmodule(MOD_COMM_PROXY),
                errmsg("reqtype:[sockreq_listen], param: server_fd:[%d], backlog:[%d]",
                param->s_fd,
                param->backlog)));
        } break;
        case sockreq_setsockopt: {
            CommSetsockoptInvokeParam* param = (CommSetsockoptInvokeParam*)sockreq->s_request_data;
            ereport(DEBUG3, (errmodule(MOD_COMM_PROXY),
                errmsg("reqtype:[sockreq_setsockopt], param: server_fd:[%d], level:[%d], optname:[%d], optlen:[%d]",
                param->s_fd,
                param->level,
                param->optname,
                param->optlen)));
        } break;
        case sockreq_epollctl: {
            CommEpollCtlInvokeParam* param = (CommEpollCtlInvokeParam*)sockreq->s_request_data;
            ereport(DEBUG3, (errmodule(MOD_COMM_PROXY),
                errmsg("reqtype:[sockreq_epollctl], param: epfd:[%d], option:[%d], fd:[%d]",
                param->s_epfd,
                param->s_op,
                param->s_fd)));
        } break;
        case sockreq_poll: {
            CommPollInternalInvokeParam* param = (CommPollInternalInvokeParam*)sockreq->s_request_data;
            ereport(DEBUG3, (errmodule(MOD_COMM_PROXY),
                errmsg("reqtype:[sockreq_poll], param: s_fd:[%d], s_logical_fd:[%d]",
                param->s_fd,
                param->s_logical_fd)));
        } break;
        case sockreq_epollwait:
        default: {
            ereport(ERROR, (errmodule(MOD_COMM_PROXY),
                errcode(ERRCODE_SYSTEM_ERROR),
                errmsg("unknown request."),
                errdetail("s_sockreq_type not find in print_debug_detail()."),
                errcause("System error."),
                erraction("Contact Huawei Engineer.")));
        }
    }
    ereport(LOG, (errmodule(MOD_COMM_PROXY), errmsg("[commdfx] %lu: fd[%d] reqtype[%d].",
        sockreq->s_req_id, sockreq->s_fd, sockreq->s_sockreq_type)));
}

unsigned long int CommDFX::GetRequestId()
{
    const unsigned long int req_max = 0xffffffffffffffff;
    if (m_global_request_id < req_max) {
        m_global_request_id++;
    } else {
        m_global_request_id = 0;
    }
    return m_global_request_id;
}

unsigned long int CommDFX::m_global_request_id = 0;

/*
 ****************************************************************************************
 *  Statistics for clent server
 ****************************************************************************************
 */
volatile unsigned int g_client_tx_packet_count = 0;
volatile unsigned int g_client_rx_packet_count = 0;
volatile unsigned long g_server_tx_packet_count = 0;
volatile unsigned long g_server_rx_packet_count = 0;
volatile unsigned long g_server_rx_packet_nbytes = 0;
volatile unsigned long g_server_rx_packet_count_raw = 0;

void UpdateTxRxStats(int msg_level)
{
    static uint64 last_tx_count = 0;
    static uint64 last_rx_count = 0;
    static uint64 last_rx_nbytes = 0;
    uint64 current_tx_count = 0;
    uint64 current_rx_count = 0;
    uint64 current_rx_nbytes = 0;

    uint64 tx_inc = 0;
    uint64 rx_inc = 0;
    uint64 rx_nbytes_inc = 0;

    /* save current */
    current_tx_count = g_server_tx_packet_count;
    current_rx_count = g_server_rx_packet_count;
    current_rx_nbytes = g_server_rx_packet_nbytes;

    /* calculate incremental */
    tx_inc = current_tx_count - last_tx_count;
    rx_inc = current_rx_count - last_rx_count;
    rx_nbytes_inc = current_rx_nbytes - last_rx_nbytes;

    ereport(msg_level, (errmodule(MOD_COMM_PROXY),
        errmsg("Stats collector TX-pck/s:%.3f(mpps) rx-pck/s:%.3f(mpps) avg-pck:%.2f (Byte) bandwidth:%.2f(MB/s).",
        (float)tx_inc / 1000000,
        (float)rx_inc / 1000000,
        (float)rx_nbytes_inc / rx_inc,
        (float)rx_nbytes_inc / 1000000)));

    /* save last count */
    last_tx_count = current_tx_count;
    last_rx_count = current_rx_count;
    last_rx_nbytes = current_rx_nbytes;
}

int parse_monitor_sock_queue(char* recv_buffer, char* send_buffer, ParseMonitorType type)
{
    int length;
    int startpos = 0;

    if (strncmp("sockqueue", recv_buffer, strlen("sockqueue")) == 0) {
        length = strlen("sockqueue");
        startpos += length;
        startpos += 1; /* split */
        if (strncmp("fd", recv_buffer + startpos, strlen("fd")) == 0) {
            int fd = atoi(recv_buffer + startpos + strlen("fd") + 1); /* split */
            if (fd == 0) {
                return 0;
            }

            if (type == ParseMonitorTypeSet) {
                CommSockDesc* sock_desc = g_comm_controller->FdGetCommSockDesc(fd);
                if (sock_desc == NULL) {
                    snprintf_s(send_buffer, MONITORMESSAGEBUFFER, MONITORMESSAGEBUFFER - 1,
                        "fd:[%d], type:[normal fd], no sock queue", fd);
                }
            }
        }

        return 1;
    }

    return 0;
}

int parse_monitor_fd(char* recv_buffer, char* send_buffer, ParseMonitorType type)
{
    int length;
    int startpos = 0;

    if (strncmp("query", recv_buffer, strlen("query")) == 0) {
        length = strlen("query");
        startpos += length;
        startpos += 1; /* split */
        if (strncmp("fd", recv_buffer + startpos, strlen("fd")) == 0) {
            int fd = atoi(recv_buffer + startpos + strlen("fd") + 1); /* split */
            if (fd == 0) {
                return 0;
            }

            if (type == ParseMonitorTypeSet) {
                CommSockDesc* sock_desc = g_comm_controller->FdGetCommSockDesc(fd);
                if (sock_desc == NULL) {
                    snprintf_s(send_buffer, MONITORMESSAGEBUFFER, MONITORMESSAGEBUFFER - 1,
                        "fd:[%d], type:[normal fd]", fd);
                } else {
                    snprintf_s(send_buffer, MONITORMESSAGEBUFFER, MONITORMESSAGEBUFFER - 1,
                        "fd:[%d], type:[%d]", fd, sock_desc->m_fd_type);
                }
            }
        }

        return 1;
    }

    return 0;
}

int parse_monitor_workernum(char* recv_buffer, char* send_buffer, ParseMonitorType type)
{
    if (strncmp("workernum", recv_buffer, strlen("workernum")) == 0) {
        if (type == ParseMonitorTypeSet) {
            int worker_num = g_comm_controller->m_active_worker_num;
            snprintf_s(send_buffer, MONITORMESSAGEBUFFER, MONITORMESSAGEBUFFER - 1, "%d", worker_num);
        }

        return 1;
    }

    return 0;
}

int parse_monitor_connnum(char* recv_buffer, char* send_buffer, ParseMonitorType type)
{
    if (strncmp("connnum", recv_buffer, strlen("connnum")) == 0) {
        if (type == ParseMonitorTypeSet) {
            snprintf_s(send_buffer, MONITORMESSAGEBUFFER, MONITORMESSAGEBUFFER - 1,
                "current statics:\n"
                "poll        : prepare[%d], ready[%d], end[%d]\n"
                "accept conn : prepare[%d], ready[%d], end[%d]\n"
                "epoll all   : prepare[%d], ready[%d], end[%d]\n"
                "epoll poll  : prepare[%d], ready[%d], end[%d]\n"
                "epoll recv  : prepare[%d], ready[%d], end[%d]\n"
                "epoll listen: prepare[%d], ready[%d], end[%d]\n",
                g_comm_controller->m_poll_static.s_prepare_num,
                g_comm_controller->m_poll_static.s_ready_num,
                g_comm_controller->m_poll_static.s_endnum,
                g_comm_controller->m_accept_static.s_prepare_num,
                g_comm_controller->m_accept_static.s_ready_num,
                g_comm_controller->m_accept_static.s_endnum,
                g_comm_controller->m_epoll_all_static.s_prepare_num,
                g_comm_controller->m_epoll_all_static.s_ready_num,
                g_comm_controller->m_epoll_all_static.s_endnum,
                g_comm_controller->m_poll_static.s_prepare_num,
                g_comm_controller->m_poll_static.s_ready_num,
                g_comm_controller->m_poll_static.s_endnum,
                g_comm_controller->m_epoll_recv_static.s_prepare_num,
                g_comm_controller->m_epoll_recv_static.s_ready_num,
                g_comm_controller->m_epoll_recv_static.s_endnum,
                g_comm_controller->m_epoll_listen_static.s_prepare_num,
                g_comm_controller->m_epoll_listen_static.s_ready_num,
                g_comm_controller->m_epoll_listen_static.s_endnum);
        }

        return 1;
    }

    return 0;
}

ParseMonitorCmd g_parse_monitor_cmd[] = {
    {parse_monitor_sock_queue, NULL, NULL, NULL},
    {parse_monitor_fd, NULL, NULL, NULL},
    {parse_monitor_workernum, NULL, NULL, NULL},
    {parse_monitor_connnum, NULL, NULL, NULL},
    {NULL, NULL, NULL, NULL}};

void ProcessCommMonitorMessage(int fd)
{
    int32 len;
    int length;
    int rc;

    char send_buffer[MONITORMESSAGEBUFFER];
    char recv_buffer[MONITORMESSAGEBUFFER];
    memset_s(send_buffer, sizeof(send_buffer), '\0', sizeof(send_buffer));
    memset_s(recv_buffer, sizeof(recv_buffer), '\0', sizeof(recv_buffer));

    int nbytes = comm_recv(fd, (char*)&len, 4, 0);
    len = ntohl(len);
    if (len < 4) {
        ereport(ERROR, (errmodule(MOD_COMM_PROXY),
            errcode(ERRCODE_SYSTEM_ERROR),
            errmsg("Get msg len error[%d] on fd %d!", len, fd),
            errdetail("N/A"),
            errcause("System error."),
            erraction("Contact Huawei Engineer.")));
        return;
    }

    nbytes = comm_recv(fd, (char*)recv_buffer, len, 0);

    rc = snprintf_s(send_buffer, MONITORMESSAGEBUFFER, MONITORMESSAGEBUFFER - 1, "input error:%s\n", recv_buffer);
    securec_check_ss(rc, "\0", "\0");

    for (int i = 0; g_parse_monitor_cmd[i].func != NULL; i++) {
        if (g_parse_monitor_cmd[i].func(recv_buffer, send_buffer, ParseMonitorTypeSet) > 0) {
            break;
        }
    }

    length = strlen(send_buffer);
    comm_send(fd, (char*)&length, sizeof(int), 0);
    comm_send(fd, send_buffer, length, 0);

    return;
}

unsigned long int g_static_pkt_cnt_by_length[TotalChannel][MAX_PKT_TRANS_CNT];
int g_static_pkt_cnt_idx[TotalChannel];
unsigned long int g_static_walsender_wakeup[WalSenderCnt][MAX_PKT_TRANS_CNT];
int g_static_walsender_cnt_idx[WalSenderCnt];
unsigned long int g_static_walrcvwriter_iostat[WalRcvWriterStat][WalRcvWriterStatMax];
int g_static_walrcvwriter_idx[WalRcvWriterStat];

unsigned long int g_fd_pkt_length_by_loop[MAX_SOCKET_FD_NUM][TotalChannel][MAX_PKT_TRANS_CNT];
int g_fd_pkt_length_idx[MAX_SOCKET_FD_NUM][TotalChannel];
int g_static_fd_map[MAX_SOCKET_FD_NUM] = {0};
int g_static_fd_cnt = 0;
int comm_add_static_fd(int fd)
{
    int id = g_static_fd_cnt;
    if (id >= MAX_SOCKET_FD_NUM) {
        return -1;
    }
    g_static_fd_map[id] = fd;
    g_static_fd_cnt++;
    return id;
}
int comm_convert_fd(int fd)
{
    for (int i = 0; i < g_static_fd_cnt; i++) {
        if (fd == g_static_fd_map[i]) {
            return i;
        }
    }
    return -1;
}
void comm_reset_packet_length_static()
{
    memset_s(g_fd_pkt_length_idx, sizeof(g_fd_pkt_length_idx), 0, sizeof(g_fd_pkt_length_idx));
    memset_s(g_fd_pkt_length_by_loop, sizeof(g_fd_pkt_length_by_loop), 0, sizeof(g_fd_pkt_length_by_loop));
};

void comm_update_packet_length_static(int fd, size_t len, int type)
{
    fd = comm_convert_fd(fd);
    if (fd < 0) {
        return;
    }
    int id = g_fd_pkt_length_idx[fd][type];
    g_fd_pkt_length_by_loop[fd][type][id] = len;
    g_fd_pkt_length_idx[fd][type] = id + 1;

    return;
};
void comm_print_packet_lenght_static()
{
    /* actually, we write to log with all info */
    for (int k = 0; k < MAX_SOCKET_FD_NUM; k++) {
        int max_wake = g_fd_pkt_length_idx[k][ChannelRX];
        if (g_fd_pkt_length_idx[k][ChannelTX] > max_wake) {
            max_wake = g_fd_pkt_length_idx[k][ChannelTX];
        }
        for (int i = 0; i < max_wake; i++) {
            if (g_fd_pkt_length_by_loop[k][ChannelRX][i] != 0 ||
                g_fd_pkt_length_by_loop[k][ChannelTX][i] != 0) {
                printf("packet length: fd:%d, loop:%d, rx:%lu, tx:%lu\n",
                    k,
                    i,
                    g_fd_pkt_length_by_loop[k][ChannelRX][i],
                    g_fd_pkt_length_by_loop[k][ChannelTX][i]);
            }
        }
    }
    fflush(stdout);
}


void comm_update_packet_static(size_t len, int type)
{
    int id = g_static_pkt_cnt_idx[type];
    g_static_pkt_cnt_by_length[type][id] = len;
    g_static_pkt_cnt_idx[type] = id + 1;

};

void comm_print_packet_static()
{
    /* actually, we write to log with all info */
    int max_cnt = g_static_pkt_cnt_idx[ChannelTX];
    if (g_static_pkt_cnt_idx[ChannelRX] > max_cnt) {
        max_cnt = g_static_pkt_cnt_idx[ChannelRX];
    }
    for (int i = 0; i < max_cnt; i++) {
        if (g_static_pkt_cnt_by_length[ChannelTX][i] != 0 ||
            g_static_pkt_cnt_by_length[ChannelRX][i] != 0) {
            printf("pktlen:%d, tx:%lu, rx:%lu\n",
                i,
                g_static_pkt_cnt_by_length[ChannelTX][i],
                g_static_pkt_cnt_by_length[ChannelRX][i]);
        }
    }
    fflush(stdout);

}


void comm_update_walsender_static(int type, int len)
{
    int id = g_static_walsender_cnt_idx[type];
    if (type == WalSenderWakeup) {
        id = g_static_walsender_cnt_idx[WalSenderEnter];
    }
    g_static_walsender_wakeup[type][id] = len;
    g_static_walsender_cnt_idx[type] = id + 1;

};
void comm_print_walsender_static()
{
    /* actually, we write to log with all info */
    int max_wake = g_static_walsender_cnt_idx[WalSenderEnter];
    if (g_static_walsender_cnt_idx[WalSenderWakeup] > max_wake) {
        max_wake = g_static_walsender_cnt_idx[WalSenderWakeup];
    }
    for (int i = 0; i < max_wake; i++) {
        if (g_static_walsender_wakeup[WalSenderEnter][i] != 0 ||
            g_static_walsender_wakeup[WalSenderWakeup][i] != 0) {
            printf("walsender:%d, enter:%lu, wakeup:%lu\n",
                i,
                g_static_walsender_wakeup[WalSenderEnter][i],
                g_static_walsender_wakeup[WalSenderWakeup][i]);
        }
    }
    fflush(stdout);
}


void comm_update_walrcvwriter_static(int type, int len)
{    
    int id = g_static_walrcvwriter_idx[type];
    if (type == WalRcvWriterExpectWrite) {
        id = g_static_walrcvwriter_idx[WalRcvWriterWrite];
    }
    g_static_walrcvwriter_iostat[type][id] = len;
    g_static_walrcvwriter_idx[type] = id + 1;
};

void comm_print_walrcvwriter_static()
{
    /* actually, we write to log with all info */
    int max_id = g_static_walrcvwriter_idx[WalRcvWriterFlush];
    for (int i = 0; i < max_id; i++) {
        if (g_static_walrcvwriter_iostat[WalRcvWriterWrite][i] != 0 ||
            g_static_walrcvwriter_iostat[WalRcvWriterExpectWrite][i] != 0 ||
            g_static_walrcvwriter_iostat[WalRcvWriterFlush][i] != 0) {
            ereport(DEBUG5, (errmodule(MOD_COMM_PROXY),
                errmsg("comm_walrecv:%d, Write:%lu, ExpectWrite:%lu, Flush:%lu.",
                i,
                g_static_walrcvwriter_iostat[WalRcvWriterWrite][i],
                g_static_walrcvwriter_iostat[WalRcvWriterExpectWrite][i],
                g_static_walrcvwriter_iostat[WalRcvWriterFlush][i])));
        }
    }
}

/*
 * gs_comm_proxy_thread_status
 *		Produce a view with connections status between cn and all other active nodes.
 *
 */
Datum gs_comm_proxy_thread_status(PG_FUNCTION_ARGS)
{
    if (!g_comm_proxy_config.s_enable_dfx) {
        ereport(ERROR, (errmodule(MOD_COMM_PROXY),
            errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("unsupported view in mluti_nodes mode or not libnet-set."),
            errdetail("N/A"),
            errcause("System error."),
            erraction("Contact Huawei Engineer.")));
    }

    ReturnSetInfo* rsinfo = (ReturnSetInfo*)fcinfo->resultinfo;
    TupleDesc tupdesc;
    MemoryContext oldcontext;

    oldcontext = MemoryContextSwitchTo(rsinfo->econtext->ecxt_per_query_memory);

    /* need a tuple descriptor representing 9 columns */
    tupdesc = CreateTemplateTupleDesc(7, false);

    TupleDescInitEntry(tupdesc, (AttrNumber) 1, "ProxyThreadId", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) 2, "ProxyCpuAffinity", TEXTOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) 3, "ThreadStartTime", TEXTOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) 4, "RxPkgNums", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) 5, "TxPkgNums", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) 6, "RxPcks", FLOAT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) 7, "TxPcks", FLOAT8OID, -1, 0);

    rsinfo->returnMode = SFRM_Materialize;
    rsinfo->setResult = tuplestore_begin_heap(true, false, u_sess->attr.attr_memory.work_mem);
    rsinfo->setDesc = BlessTupleDesc(tupdesc);

    (void)MemoryContextSwitchTo(oldcontext);

    /* build one tuple and save it in tuplestore. */
    CommController *controller = (CommController *)g_comm_controller;
    int nums = controller->m_communicator_nums[0];
    for (int i = 0; i < nums; i++) {
        ThreadPoolCommunicator *comm = controller->m_communicators[0][i];
        const CommProxyThreadStatus *status = &(comm->m_thread_status);

        Datum values[7] = {0};
        bool  nulls[7] = {false};

        values[0] = Int64GetDatum(status->s_proxy_thread_id);
        values[1] = CStringGetTextDatum(status->s_thread_affinity);
        values[2] = CStringGetTextDatum(timestamptz_to_str(status->s_start_time));
        values[3] = UInt64GetDatum(status->s_recv_packet_num);
        values[4] = UInt64GetDatum(status->s_send_packet_num);
        values[5] = Float8GetDatum(status->s_recv_pps);
        values[6] = Float8GetDatum(status->s_send_pps);

        tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
    }

    /* clean up and return the tuplestore */
    tuplestore_donestoring(rsinfo->setResult);

    return (Datum)0;
}
