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
 * comm_connection.cpp
 *        TODO add contents
 *
 *
 * IDENTIFICATION
 *        src/gausskernel/cbb/communication/comm_proxy/comm_connection.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include <dlfcn.h>
#include <fcntl.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <unistd.h>
#include "comm_core.h"
#include "comm_connection.h"
#include "comm_proxy.h"
#include "postgres.h"
#include "communication/commproxy_interface.h"

/*
 *---------------------------------------------------------------------------------------
 * Global Variables & Functions
 *---------------------------------------------------------------------------------------
 */
extern void CommClearConnOption();
extern void CommClearEpollOption();
extern void CommClearPollOption();

static int comm_socket_check_hook_pre(void *param, CommConnType* type);
static int comm_socket_check_hook_end(void *ret, CommConnType* type);
static int comm_epoll_check_hook_end(void *ret, CommConnType* type);
static int comm_setsockopt_check_hook_pre(void *param, CommConnType* type);
static int comm_connect_check_hook_pre(void *param, CommConnType* type);
static int comm_getsockopt_check_hook_pre(void *param, CommConnType* type);
static int comm_epoll_create_check_hook_pre(void *param, CommConnType* type);
static int comm_send_check_hook_pre(void *param, CommConnType* type);
static int comm_send_check_hook_end(void *param, CommConnType* type);
static int comm_recv_check_hook_pre(void *param, CommConnType* type);
static int comm_recv_check_hook_end(void *param, CommConnType* type);
static int comm_epollctl_check_hook_pre(void *param, CommConnType* type);
static int comm_poll_check_hook_pre(void *param, CommConnType* type);
static int comm_poll_check_hook_end(void *ret, CommConnType* type);

CommApiCheckHookPre g_comm_api_hooks_pre[sockreq_max];
CommApiCheckHookEnd g_comm_api_hooks_end[sockreq_max];


#define INIT_COMM_CONN_KERNEL_API(x) .x##_fn = x
#define INIT_COMM_CONN_PROXY_API(x) . x##_fn = comm_proxy_##x
#ifdef USE_LIBNET
#define INIT_COMM_CONN_LIBNET_API(x) .x##_fn = __wrap_##x
#else
#define INIT_COMM_CONN_LIBNET_API(x) .x##_fn = x
#endif

const gs_api_t g_comm_socket_api[CommConnMax] = {
    {
        /* CommConnKernel */
        INIT_COMM_CONN_KERNEL_API(socket),
        INIT_COMM_CONN_KERNEL_API(accept),
        INIT_COMM_CONN_KERNEL_API(accept4),
        INIT_COMM_CONN_KERNEL_API(bind),
        INIT_COMM_CONN_KERNEL_API(listen),
        INIT_COMM_CONN_KERNEL_API(connect),
        INIT_COMM_CONN_KERNEL_API(getpeername),
        INIT_COMM_CONN_KERNEL_API(getsockname),
        INIT_COMM_CONN_KERNEL_API(setsockopt),
        INIT_COMM_CONN_KERNEL_API(getsockopt),
        INIT_COMM_CONN_KERNEL_API(close),
        INIT_COMM_CONN_KERNEL_API(shutdown),
        INIT_COMM_CONN_KERNEL_API(fork),
        INIT_COMM_CONN_KERNEL_API(read),
        INIT_COMM_CONN_KERNEL_API(write),
        INIT_COMM_CONN_KERNEL_API(fcntl),
        INIT_COMM_CONN_KERNEL_API(epoll_create),
        INIT_COMM_CONN_KERNEL_API(epoll_create1),
        INIT_COMM_CONN_KERNEL_API(epoll_ctl),
        INIT_COMM_CONN_KERNEL_API(epoll_wait),
        INIT_COMM_CONN_KERNEL_API(epoll_pwait),
        INIT_COMM_CONN_KERNEL_API(sigaction),
        INIT_COMM_CONN_KERNEL_API(poll),
        INIT_COMM_CONN_KERNEL_API(send),
        INIT_COMM_CONN_KERNEL_API(recv),
    }, {
        /* CommConnLibnet */
        INIT_COMM_CONN_LIBNET_API(socket),
        INIT_COMM_CONN_LIBNET_API(accept),
        INIT_COMM_CONN_LIBNET_API(accept4),
        INIT_COMM_CONN_LIBNET_API(bind),
        INIT_COMM_CONN_LIBNET_API(listen),
        INIT_COMM_CONN_LIBNET_API(connect),
        INIT_COMM_CONN_LIBNET_API(getpeername),
        INIT_COMM_CONN_LIBNET_API(getsockname),
        INIT_COMM_CONN_LIBNET_API(setsockopt),
        INIT_COMM_CONN_LIBNET_API(getsockopt),
        INIT_COMM_CONN_LIBNET_API(close),
        INIT_COMM_CONN_LIBNET_API(shutdown),
        INIT_COMM_CONN_LIBNET_API(fork),
        INIT_COMM_CONN_LIBNET_API(read),
        INIT_COMM_CONN_LIBNET_API(write),
        INIT_COMM_CONN_LIBNET_API(fcntl),
        INIT_COMM_CONN_LIBNET_API(epoll_create),
        INIT_COMM_CONN_LIBNET_API(epoll_create1),
        INIT_COMM_CONN_LIBNET_API(epoll_ctl),
        INIT_COMM_CONN_LIBNET_API(epoll_wait),
        INIT_COMM_CONN_LIBNET_API(epoll_pwait),
        INIT_COMM_CONN_LIBNET_API(sigaction),
        INIT_COMM_CONN_LIBNET_API(poll),
        INIT_COMM_CONN_LIBNET_API(send),
        INIT_COMM_CONN_LIBNET_API(recv),
#ifdef USE_LIBNET
        INIT_COMM_CONN_LIBNET_API(addr_recv)
#endif
    }, {
        /* CommConnLibnetProxy */
        INIT_COMM_CONN_PROXY_API(socket),
        INIT_COMM_CONN_PROXY_API(accept),
        INIT_COMM_CONN_PROXY_API(accept4),
        INIT_COMM_CONN_PROXY_API(bind),
        INIT_COMM_CONN_PROXY_API(listen),
        INIT_COMM_CONN_PROXY_API(connect),
        INIT_COMM_CONN_PROXY_API(getpeername),
        INIT_COMM_CONN_PROXY_API(getsockname),
        INIT_COMM_CONN_PROXY_API(setsockopt),
        INIT_COMM_CONN_PROXY_API(getsockopt),
        INIT_COMM_CONN_PROXY_API(close),
        INIT_COMM_CONN_PROXY_API(shutdown),
        INIT_COMM_CONN_PROXY_API(fork),
        INIT_COMM_CONN_PROXY_API(read),
        INIT_COMM_CONN_PROXY_API(write),
        INIT_COMM_CONN_PROXY_API(fcntl),
        INIT_COMM_CONN_PROXY_API(epoll_create),
        INIT_COMM_CONN_PROXY_API(epoll_create1),
        INIT_COMM_CONN_PROXY_API(epoll_ctl),
        INIT_COMM_CONN_PROXY_API(epoll_wait),
        INIT_COMM_CONN_PROXY_API(epoll_pwait),
        INIT_COMM_CONN_PROXY_API(sigaction),
        INIT_COMM_CONN_PROXY_API(poll),
        INIT_COMM_CONN_PROXY_API(send),
        INIT_COMM_CONN_PROXY_API(recv),
#ifdef USE_LIBNET
        INIT_COMM_CONN_PROXY_API(addr_recv)
#endif
    }
};

/*
 *---------------------------------------------------------------------------------------
 * Internal functions
 *---------------------------------------------------------------------------------------
 */
int gs_api_init(gs_api_t* gs_api, phy_proto_type_t proto)
{
#ifdef USE_LIBNET
    if (proto == phy_proto_libnet) {
        gs_api->socket_fn = __wrap_socket;
        gs_api->accept_fn = __wrap_accept;
        gs_api->bind_fn = __wrap_bind;
        gs_api->listen_fn = __wrap_listen;
        gs_api->connect_fn = __wrap_connect;
        gs_api->getpeername_fn = __wrap_getpeername;
        gs_api->getsockname_fn = __wrap_getsockname;
        gs_api->setsockopt_fn = __wrap_setsockopt;
        gs_api->getsockopt_fn = __wrap_getsockopt;
        gs_api->close_fn = __wrap_close;
        gs_api->fork_fn = __wrap_fork;
        gs_api->read_fn = __wrap_read;
        gs_api->write_fn = __wrap_write;
        gs_api->fcntl_fn = __wrap_fcntl;
        gs_api->epoll_create_fn = __wrap_epoll_create;
        gs_api->epoll_ctl_fn = __wrap_epoll_ctl;
        gs_api->epoll_wait_fn = __wrap_epoll_wait;
        gs_api->sigaction_fn = __wrap_sigaction;
        gs_api->poll_fn = __wrap_poll;
        gs_api->send_fn = __wrap_send;
        gs_api->recv_fn = __wrap_recv;
        gs_api->shutdown_fn = __wrap_shutdown;
        gs_api->addr_recv_fn = __wrap_addr_recv;
        return 1;
    } else
#endif
    {

    /* the symbol we use here won't be NULL, so we don't need dlerror() to test error */
#define CHECK_DLSYM_RET_RETURN(ret) \
        do {                            \
            if ((ret) == NULL)          \
                goto err_out;           \
        } while (0)

        /* glibc standard api */
        CHECK_DLSYM_RET_RETURN(gs_api->socket_fn = (Sys_socket)dlsym(RTLD_DEFAULT, "socket"));
        CHECK_DLSYM_RET_RETURN(gs_api->accept_fn = (Sys_accept)dlsym(RTLD_DEFAULT, "accept"));
        CHECK_DLSYM_RET_RETURN(gs_api->bind_fn = (Sys_bind)dlsym(RTLD_DEFAULT, "bind"));
        CHECK_DLSYM_RET_RETURN(gs_api->listen_fn = (Sys_listen)dlsym(RTLD_DEFAULT, "listen"));
        CHECK_DLSYM_RET_RETURN(gs_api->connect_fn = (Sys_connect)dlsym(RTLD_DEFAULT, "connect"));
        CHECK_DLSYM_RET_RETURN(gs_api->setsockopt_fn = (Sys_setsockopt)dlsym(RTLD_DEFAULT, "setsockopt"));
        CHECK_DLSYM_RET_RETURN(gs_api->getsockopt_fn = (Sys_getsockopt)dlsym(RTLD_DEFAULT, "getsockopt"));
        CHECK_DLSYM_RET_RETURN(gs_api->getpeername_fn = (Sys_getpeername)dlsym(RTLD_DEFAULT, "getpeername"));
        CHECK_DLSYM_RET_RETURN(gs_api->getsockname_fn = (Sys_getsockname)dlsym(RTLD_DEFAULT, "getsockname"));
        CHECK_DLSYM_RET_RETURN(gs_api->close_fn = (Sys_close)dlsym(RTLD_DEFAULT, "close"));
        CHECK_DLSYM_RET_RETURN(gs_api->fork_fn = (Sys_fork)dlsym(RTLD_DEFAULT, "fork"));
        CHECK_DLSYM_RET_RETURN(gs_api->read_fn = (Sys_read)dlsym(RTLD_DEFAULT, "read"));
        CHECK_DLSYM_RET_RETURN(gs_api->write_fn = (Sys_write)dlsym(RTLD_DEFAULT, "write"));
        CHECK_DLSYM_RET_RETURN(gs_api->fcntl_fn = (Sys_fcntl)dlsym(RTLD_DEFAULT, "fcntl"));
        CHECK_DLSYM_RET_RETURN(gs_api->epoll_create_fn = (Sys_epoll_create)dlsym(RTLD_DEFAULT, "epoll_create"));
        CHECK_DLSYM_RET_RETURN(gs_api->epoll_ctl_fn = (Sys_epoll_ctl)dlsym(RTLD_DEFAULT, "epoll_ctl"));
        CHECK_DLSYM_RET_RETURN(gs_api->epoll_wait_fn = (Sys_epoll_wait)dlsym(RTLD_DEFAULT, "epoll_wait"));
        CHECK_DLSYM_RET_RETURN(gs_api->sigaction_fn = (Sys_sigaction)dlsym(RTLD_DEFAULT, "sigaction"));
        CHECK_DLSYM_RET_RETURN(gs_api->poll_fn = (Sys_poll)dlsym(RTLD_DEFAULT, "poll"));
        CHECK_DLSYM_RET_RETURN(gs_api->send_fn = (Sys_send)dlsym(RTLD_DEFAULT, "send"));
        CHECK_DLSYM_RET_RETURN(gs_api->recv_fn = (Sys_recv)dlsym(RTLD_DEFAULT, "recv"));
        CHECK_DLSYM_RET_RETURN(gs_api->shutdown_fn = (Sys_shutdown)dlsym(RTLD_DEFAULT, "shutdown"));
        CHECK_DLSYM_RET_RETURN(gs_api->addr_recv_fn = (Sys_recv)dlsym(RTLD_DEFAULT, "recv"));

#undef CHECK_DLSYM_RET_RETURN

        return 1;
    }
err_out:
    return 0;
}

void comm_init_api_hook()
{
    /* set all hooks NULL */
    int rc = memset_s(g_comm_api_hooks_pre, sizeof(CommApiCheckHookPre) * sockreq_max,
                    '\0', sizeof(CommApiCheckHookPre) * sockreq_max);
    securec_check(rc, "\0", "\0");
    
    g_comm_api_hooks_pre[sockreq_socket] = comm_socket_check_hook_pre;
    g_comm_api_hooks_pre[sockreq_setsockopt] = comm_setsockopt_check_hook_pre;
    g_comm_api_hooks_pre[sockreq_getsockopt] = comm_getsockopt_check_hook_pre;
    g_comm_api_hooks_pre[sockreq_connect] = comm_connect_check_hook_pre;
    g_comm_api_hooks_pre[sockreq_epoll_create] = comm_epoll_create_check_hook_pre;
    g_comm_api_hooks_pre[sockreq_recv] = comm_recv_check_hook_pre;
    g_comm_api_hooks_pre[sockreq_send] = comm_send_check_hook_pre;
    g_comm_api_hooks_pre[sockreq_epollctl] = comm_epollctl_check_hook_pre;
    g_comm_api_hooks_pre[sockreq_poll] = comm_poll_check_hook_pre;

    memset_s(g_comm_api_hooks_end, sizeof(CommApiCheckHookEnd) * sockreq_max,
           '\0', sizeof(CommApiCheckHookEnd) * sockreq_max);
    securec_check(rc, "\0", "\0");

    g_comm_api_hooks_end[sockreq_socket] = comm_socket_check_hook_end;
    g_comm_api_hooks_end[sockreq_epoll_create] = comm_epoll_check_hook_end;
    g_comm_api_hooks_end[sockreq_poll] = comm_poll_check_hook_end;
    g_comm_api_hooks_end[sockreq_recv] = comm_recv_check_hook_end;
    g_comm_api_hooks_end[sockreq_send] = comm_send_check_hook_end;
}

/*
 *---------------------------------------------------------------------------------------
 * Internal functions
 *---------------------------------------------------------------------------------------
 */
static int comm_socket_check_hook_pre(void *param, CommConnType* type)
{
    CommSocketInvokeParam *api_param = (CommSocketInvokeParam *)param;
    if (api_param->s_domain == AF_UNIX || 
        api_param->s_domain == AF_LOCAL ||
        api_param->s_domain == AF_INET6) {
        *type = CommConnKernel;
    }

    return 0;
}

static int comm_socket_check_hook_end(void *ret, CommConnType* type)
{
    CommClearConnOption();

    return 0;
}

static int comm_epoll_check_hook_end(void *ret, CommConnType* type)
{
    CommClearEpollOption();

    return 0;
}

static int comm_setsockopt_check_hook_pre(void *param, CommConnType* type)
{
    CommSetsockoptInvokeParam *api_param = (CommSetsockoptInvokeParam *)param;

    if (*type != CommConnKernel) {
        if (api_param->level == SOL_SOCKET && api_param->optname == SO_REUSEADDR) {
            api_param->optname |= SO_REUSEPORT;
            return 0;
        }

        if (api_param->level == IPPROTO_IPV6) {
            return -EPROTONOSUPPORT;
        }
    }

    return 0;
}

static int comm_connect_check_hook_pre(void *param, CommConnType* type)
{
    CommConnectParam *api_param = (CommConnectParam *)param;

    /* if conn is pure libnet, we need set noblock mode to avoid lstack block */
    if (*type == CommConnLibnet) {
        /*
         * maybe we need check remote ip to select use libnet or not
         * now, we assume master and slave are not same node, their conn is cross machine
         * so we use libnet
         */
        if (CommSetBlockMode(g_comm_socket_api[*type].fcntl_fn, api_param->s_sockfd, false) == false) {
            ereport(ERROR, (errmodule(MOD_COMM_PROXY),
                errcode(ERRCODE_SYSTEM_ERROR),
                errmsg("set block mode error in comm_connect_check_hook_pre."),
                errdetail("N/A"),
                errcause("System error."),
                erraction("Contact Huawei Engineer.")));
        }
    }

    return 0;
}

static int comm_getsockopt_check_hook_pre(void *param, CommConnType* type)
{
    CommGetsockoptInvokeParam *api_param = (CommGetsockoptInvokeParam *)param;
    if (*type != CommConnKernel) {
        if (api_param->level == SOL_SOCKET && api_param->optname == SO_PEERCRED) {
            *type = CommConnKernel;
        }
    }

    return 0;
}

static int comm_epoll_create_check_hook_pre(void *param, CommConnType* type)
{
    CommEpollCreateParam *api_param = (CommEpollCreateParam *)param;
    api_param->groupid = 0;

    return 0;
}

static int comm_send_check_hook_pre(void *param, CommConnType* type)
{
    CommSendParam *api_param = (CommSendParam *)param;
    if (*type == CommConnProxy) {
        CommSockDesc *comm_sock = g_comm_controller->FdGetCommSockDesc(api_param->s_fd);
        (void)is_null_getcommsockdesc(comm_sock, "comm_send_check_hook_pre", api_param->s_fd);
        /*
         * if a fd is fake libos fd, it must be use kni, and we donot add it to epoll
         * so we use it as kernel fd
         */
        if (comm_sock->m_fd_type == CommSockFakeLibosFd) {
            *type = CommConnKernel;
        }
    }

    return 0;
}

static int comm_send_check_hook_end(void *param, CommConnType* type)
{
    CommSendParam *api_param = (CommSendParam *)param;
    if (api_param->s_ret <= 0) {
        return 0;
    }

    /* we static packet length */
    comm_update_packet_length_static(api_param->s_fd, api_param->s_ret, ChannelTX);

    return 0;
}

static int comm_recv_check_hook_pre(void *param, CommConnType* type)
{
    CommRecvaram *api_param = (CommRecvaram *)param;
    if (*type == CommConnProxy) {
        CommSockDesc *comm_sock = g_comm_controller->FdGetCommSockDesc(api_param->s_fd);
        (void)is_null_getcommsockdesc(comm_sock, "comm_recv_check_hook_pre", api_param->s_fd);
        /*
         * if a fd is fake libos fd, it must be use kni, and we donot add it to epoll
         * so we use it as kernel fd
         */
        if (comm_sock->m_fd_type == CommSockFakeLibosFd) {
            *type = CommConnKernel;
        }
    }

    return 0;
}

static int comm_recv_check_hook_end(void *param, CommConnType* type)
{
    CommRecvaram *api_param = (CommRecvaram *)param;
    if (api_param->s_ret <= 0) {
        return 0;
    }

    /* we static packet length */
    comm_update_packet_length_static(api_param->s_fd, api_param->s_ret, ChannelRX);

    return 0;
}

static int comm_epollctl_check_hook_pre(void *param, CommConnType* type)
{
    CommEpollCtlInvokeParam *api_param = (CommEpollCtlInvokeParam *)param;
    if (*type == CommConnProxy) {
        CommSockDesc *comm_sock = g_comm_controller->FdGetCommSockDesc(api_param->s_fd);
        (void)is_null_getcommsockdesc(comm_sock, "comm_epollctl_check_hook_pre", api_param->s_fd);
        /*
         * if a fd is fake libos fd, it must be use kni, and we donot add it to epoll
         * so we use it as kernel fd
         */
        if (comm_sock->m_fd_type == CommSockFakeLibosFd) {
            *type = CommConnKernel;
        }
    }

    return 0;
}

static int comm_poll_check_hook_pre(void *param, CommConnType* type)
{
    return 0;
}

static int comm_poll_check_hook_end(void *ret, CommConnType* type)
{
    CommClearPollOption();

    return 0;
}
