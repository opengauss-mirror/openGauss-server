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
 * comm_interface.cpp
 *        TODO add contents
 *
 *
 * IDENTIFICATION
 *        src/gausskernel/cbb/communication/comm_proxy/comm_interface.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
#include <stdio.h>
#include <string.h>
#ifndef WIN32
#include <unistd.h>   /* for write() */
#include <sys/mman.h> /* for mmap/munmap */
#else
#include <io.h> /* for _write() */
#include <win32.h>
#endif

#include <sys/ioctl.h>

#include "comm_adapter.h"
#include "communication/commproxy_interface.h"
#include "communication/commproxy_basic.h"
#include "comm_core.h"
#include "comm_proxy.h"
#include "communication/commproxy_dfx.h"
#include "utils/palloc.h"
#include "utils/be_module.h"

#define IS_NULL_STR(str) ((str) == NULL || *str == '\0')

#define DEFAULT_THREAD_POOL_SIZE 16
#define DEFAULT_COMM_THREAD_POOL_GROUPS 4
#define MAX_THREAD_POOL_SIZE 4096
#define MAX_THREAD_POOL_GROUPS 64
#define PLATFORM_INFO_SIZE 1024
#define EULEROSV2R9_ARM "eulerosv2r9.aarch64"

#define write_stderr(str) write(fileno(stderr), str, strlen(str))


static CommConnType CommProxyCreateSocketCheck();
static CommConnType CommProxyCreateEpollCheck();

static bool g_comm_proxy_enabled = false;

static CommConnType CommProxyGetPollType();

ErrorLevel min_debug_level = COMM_LOG;
CoreBindConfig g_core_bind_config = {};
CommProxyConfig g_comm_proxy_config = {};

static bool IsCommProxyEnabled();


/* --------------------Comm Interface function ------------------------------------------ */
CommSocketOption g_default_invalid_sock_opt = {
    .valid = false
};
CommEpollOption g_default_invalid_epoll_opt = {
    .valid = false
};
CommPollOption g_default_invalid_poll_opt = {
    .valid = false
};


/*
 * --------------------------------------------------------------------------------------
 * Export Functions
 * --------------------------------------------------------------------------------------
 */
int comm_socket(int domain, int type, int protocol)
{
    CommSocketInvokeParam param;
    param.s_domain = domain;
    param.s_type = type;
    param.s_protocol = protocol;

    CommConnType conn_type = CommProxyCreateSocketCheck();

    COMM_API_HOOK_PRE(sockreq_socket, param, conn_type);

    int ret = comm_socket_call<CommSocketInvokeParam, Sys_socket>(&param, g_comm_socket_api[conn_type].socket_fn);

    COMM_API_HOOK_END(sockreq_socket, ret, conn_type);

    return ret;
}

int comm_close(int fd)
{
    CommCloseInvokeParam param;
    param.s_fd = fd;

    CommConnType type = CommGetConnType(fd);

    int ret = comm_socket_call<CommCloseInvokeParam, Sys_close>(&param, g_comm_socket_api[type].close_fn);

    return ret;
}

int comm_accept(int sockfd, struct sockaddr* addr, socklen_t* addrlen)
{
    CommAcceptParam param;
    param.s_fd = sockfd;
    param.s_addr = addr;
    param.s_addrlen = addrlen;

    CommConnType type = CommGetConnType(sockfd);

    int ret = comm_socket_call<CommAcceptParam, Sys_accept>(&param, g_comm_socket_api[type].accept_fn);

    return ret;
}

int comm_accept4(int sockfd, struct sockaddr *addr, socklen_t *addrlen, int flags)
{
    CommAccept4Param param;
    param.s_fd = sockfd;
    param.s_addr = addr;
    param.s_addrlen = addrlen;
    param.s_flags = flags;

    CommConnType type = CommGetConnType(sockfd);

    int ret = comm_socket_call<CommAccept4Param, Sys_accept4>(&param, g_comm_socket_api[type].accept4_fn);

    return ret;
}

int comm_bind(int sockfd, const struct sockaddr* addr, socklen_t addrlen)
{
    CommBindInvokeParam param;
    param.s_fd = sockfd;
    param.s_addr = (struct sockaddr*)addr;
    param.addrlen = addrlen;

    CommConnType type = CommGetConnType(sockfd);

    int ret = comm_socket_call<CommBindInvokeParam, Sys_bind>(&param, g_comm_socket_api[type].bind_fn);

    return ret;
}

int comm_listen(int sockfd, int backlog)
{
    CommListenInvokeParam param;
    param.s_fd = sockfd;
    param.backlog = backlog;

    CommConnType type = CommGetConnType(sockfd);
    int ret = comm_socket_call<CommListenInvokeParam, Sys_listen>(&param, g_comm_socket_api[type].listen_fn);

    return ret;
}

ssize_t comm_send(int sockfd, const void* buf, size_t len, int flags)
{
    CommSendParam param;
    param.s_fd = sockfd;
    param.buff = (void *)buf;
    param.len = len;
    param.flags = flags;

    CommConnType type = CommGetConnType(sockfd);

    COMM_API_HOOK_PRE(sockreq_send, param, type);

    int ret = comm_socket_call<CommSendParam, Sys_send>(&param, g_comm_socket_api[type].send_fn);

    COMM_API_HOOK_END(sockreq_send, param, type);

    return ret;
}

ssize_t comm_recv(int sockfd, void* buf, size_t len, int flags)
{
    CommRecvParam param;
    param.s_fd = sockfd;
    param.buff = buf;
    param.len = len;
    param.flags = flags;

    CommConnType type = CommGetConnType(sockfd);

    COMM_API_HOOK_PRE(sockreq_recv, param, type);

    int ret = comm_socket_call<CommRecvParam, Sys_recv>(&param, g_comm_socket_api[type].recv_fn);

    COMM_API_HOOK_END(sockreq_recv, param, type);

    return ret;
}

int comm_setsockopt(int sockfd, int level, int optname, const void* optval, socklen_t optlen)
{
    CommSetsockoptInvokeParam param;
    param.s_fd = sockfd;
    param.level = level;
    param.optname = optname;
    param.optval = optval;
    param.optlen = optlen;

    CommConnType type = CommGetConnType(sockfd);

    COMM_API_HOOK_PRE(sockreq_setsockopt, param, type);

    int ret = comm_socket_call<CommSetsockoptInvokeParam, Sys_setsockopt>(&param, g_comm_socket_api[type].setsockopt_fn);

    COMM_API_HOOK_END(sockreq_setsockopt, ret, type);

    return ret;
}

/*
 * Why is no_proxy retained?
 * poll is a call that has no memory and resets the fds list to be listened on each time.
 * When only timing, pipefd or file fd is used, only the native interface is invoked to ensure better performance.
 * Sometimes poll may be invoked cyclically.
 * If set noproxy as sock_option that transferred through thread variables, we need set them each time.
 * There's no need to waste the expense.
 */
int comm_poll(struct pollfd* fdarray, unsigned long nfds, int timeout)
{
    CommWaitPollParam param;
    param.s_fdarray = fdarray;
    param.s_nfds = nfds;
    param.s_timeout = timeout;

    CommConnType type = CommProxyGetPollType();

    COMM_API_HOOK_PRE(sockreq_poll, param, type);

    int ret = comm_socket_call<CommWaitPollParam, Sys_poll>(&param, g_comm_socket_api[type].poll_fn);

    COMM_API_HOOK_PRE(sockreq_poll, param, type);

    return ret;
}

int comm_epoll_create(int size)
{
    CommEpollCreateParam param;
    param.size = size;
    CommConnType type = CommProxyCreateEpollCheck();

    COMM_API_HOOK_PRE(sockreq_epoll_create, param, type);

    int ret = comm_socket_call<CommEpollCreateParam, Sys_epoll_create>(&param, g_comm_socket_api[type].epoll_create_fn);

    COMM_API_HOOK_END(sockreq_epoll_create, ret, type);

    return ret;
}

int comm_epoll_create1(int flag)
{
    CommEpollCreate1Param param;
    param.flag = flag;
    CommConnType type = CommProxyCreateEpollCheck();

    /* we use same hook with epoll create */
    COMM_API_HOOK_PRE(sockreq_epoll_create, param, type);

    int ret = comm_socket_call<CommEpollCreate1Param, Sys_epoll_create1>(&param, g_comm_socket_api[type].epoll_create1_fn);

    COMM_API_HOOK_END(sockreq_epoll_create, ret, type);

    return ret;
}

int comm_epoll_ctl(int epfd, int op, int fd, struct epoll_event* event)
{
    int ret = -1;
    CommEpollCtlInvokeParam param;
    param.s_epfd = epfd;
    param.s_op = op;
    param.s_fd = fd;
    param.s_event = event;
    CommConnType ep_type = CommGetConnType(epfd);
    CommConnType fd_type = CommGetConnType(fd);

    COMM_API_HOOK_PRE(sockreq_epollctl, param, fd_type);

    EpollInputStatus proxy_status = (EpollInputStatus)(ep_type << CommConnTypeShift | fd_type);
    ereport(DEBUG2, (errmodule(MOD_COMM_PROXY),
        errmsg("comm_epoll_ctl epfd:[%d] op[%d] fd[%d] mode[%d].", epfd, op, fd, proxy_status)));

    switch (proxy_status)
    {
        case epoll_kernel_fd_kernel: {
            ret = g_comm_socket_api[CommConnKernel].epoll_ctl_fn(epfd, op, fd, event);
        } break;

        case epoll_libnet_fd_libnet: {
            ret = g_comm_socket_api[CommConnLibnet].epoll_ctl_fn(epfd, op, fd, event);
        } break;

        case epoll_proxy_fd_kernel: {
            CommSockDesc *comm_sock = g_comm_controller->FdGetCommSockDesc(epfd);
            if (comm_sock == NULL) {
                ereport(ERROR, (errmodule(MOD_COMM_PROXY),
                    errcode(ERRCODE_SYSTEM_ERROR),
                    errmsg("Failed to obtain the comm_sock in comm_epoll_ctl, fd:%d.", epfd),
                    errdetail("N/A"),
                    errcause("System error."),
                    erraction("Contact Huawei Engineer.")));
                return -1;
            }
            int real_epfd = comm_sock->m_epoll_real_fd;
            ret = epoll_ctl(real_epfd, op, fd, event);
            comm_sock->m_need_kernel_call = true;
        } break;

        case epoll_proxy_fd_kernel_proxy: {
            if (g_comm_controller->m_epoll_mode == CommEpollModeWakeup) {
                if (op == EPOLL_CTL_MOD) {
                    ret = comm_proxy_epoll_ctl(epfd, EPOLL_CTL_DEL, fd, NULL);
                    ret = comm_proxy_epoll_ctl(epfd, EPOLL_CTL_ADD, fd, event);
                } else {
                    ret = comm_proxy_epoll_ctl(epfd, op, fd, event);
                }
            } else {
                ret = comm_proxy_epoll_ctl(epfd, op, fd, event);
            }
        } break;

        default: {
            Assert(0);
        }
    }

    return ret;
}

int comm_epoll_wait(int epfd, struct epoll_event* events, int maxevents, int timeout)
{
    CommWaitEpollWaitParam param;
    param.s_epollfd = epfd;
    param.s_events = events;
    param.s_maxevents = maxevents;
    param.s_timeout = timeout;

    CommConnType type = CommGetConnType(epfd);
    int ret = comm_socket_call<CommWaitEpollWaitParam, Sys_epoll_wait>(&param, g_comm_socket_api[type].epoll_wait_fn);

    return ret;
}

int comm_epoll_pwait(int epfd, struct epoll_event* events, int maxevents, int timeout, const sigset_t *sigmask)
{
    CommEpollPWaitParam param;
    param.s_epollfd = epfd;
    param.s_events = events;
    param.s_maxevents = maxevents;
    param.s_timeout = timeout;
    param.s_sigmask = sigmask;

    CommConnType type = CommGetConnType(epfd);

    int ret = comm_socket_call<CommEpollPWaitParam, Sys_epoll_pwait>(&param, g_comm_socket_api[type].epoll_pwait_fn);

    return ret;
}

int comm_fcntl(int fd, int request, long arg)
{
    CommFcntlInvokeParam param;
    param.s_fd = fd;
    param.s_cmd = request;
    param.s_arg = arg;

    CommConnType type = CommGetConnType(fd);
    int ret = comm_socket_call<CommFcntlInvokeParam, Sys_fcntl>(&param, g_comm_socket_api[type].fcntl_fn); 

    return ret;
}

int comm_connect(int sockfd, const struct sockaddr* addr, socklen_t addrlen)
{
    CommConnectParam param;
    param.s_sockfd = sockfd;
    param.s_addr = addr;
    param.s_addrlen = addrlen;

    CommConnType type = CommGetConnType(sockfd);

    COMM_API_HOOK_PRE(sockreq_connect, param, type)

    int ret = comm_socket_call<CommConnectParam, Sys_connect>(&param, g_comm_socket_api[type].connect_fn); 

    return ret;
}

int comm_shutdown(int sockfd, int how)
{
    CommShutdownInvokeParam param;
    param.s_fd = sockfd;
    param.s_how = how;

    CommConnType type = CommGetConnType(sockfd);
    int ret = comm_socket_call<CommShutdownInvokeParam, Sys_shutdown>(&param, g_comm_socket_api[type].shutdown_fn); 

    return ret;
}

int comm_closesocket(int sockfd)
{
    CommCloseInvokeParam param;
    param.s_fd = sockfd;
    CommConnType type = CommGetConnType(sockfd);
    int ret = comm_socket_call<CommCloseInvokeParam, Sys_close>(&param, g_comm_socket_api[type].close_fn); 
    return ret;
}

int comm_getsockopt(int sockfd, int level, int option_name, void* option_value, socklen_t* option_len)
{
    CommGetsockoptInvokeParam param;
    param.s_fd = sockfd;
    param.level = level;
    param.optname = option_name;
    param.optval = option_value;
    param.optlen = option_len;

    CommConnType type = CommGetConnType(sockfd);

    COMM_API_HOOK_PRE(sockreq_getsockopt, param, type)

    int ret = comm_socket_call<CommGetsockoptInvokeParam, Sys_getsockopt>(&param, g_comm_socket_api[type].getsockopt_fn); 
    return ret;
}

int comm_getpeername(int sockfd, struct sockaddr* addr, socklen_t* addrlen)
{
    CommGetpeernameInvokeParam param;
    param.s_fd = sockfd;
    param.s_server_addr = addr;
    param.s_addrlen = addrlen;

    CommConnType type = CommGetConnType(sockfd);
    int ret = comm_socket_call<CommGetpeernameInvokeParam, Sys_getpeername>(&param, g_comm_socket_api[type].getpeername_fn); 

    return ret;
}

int comm_gethostname(char* hostname, size_t size)
{
    return gethostname(hostname, size);
}

int comm_getsockname(int sockfd, struct sockaddr* addr, socklen_t* addrlen)
{
    CommGetsocknameInvokeParam param;
    param.s_fd = sockfd;
    param.s_server_addr = addr;
    param.s_addrlen = addrlen;

    CommConnType type = CommGetConnType(sockfd);

    int ret = comm_socket_call<CommGetsocknameInvokeParam, Sys_getsockname>(&param, g_comm_socket_api[type].getsockname_fn); 

    return ret;
}

int comm_ioctl(int fd, int request, ...)
{
    if (AmINoKernelModeSockfd(fd)) {
        ereport(WARNING, (errmodule(MOD_COMM_PROXY), errmsg("Not support libnet ioctl now.")));
        return -1;
    }

    va_list arg_ptr;
    va_start(arg_ptr, request);
    int ret = ioctl(fd, request, arg_ptr);
    va_end(arg_ptr);
    return ret;
}

ssize_t comm_read(int sockfd, void* buf, size_t count)
{
    if (AmINoKernelModeSockfd(sockfd)) {
        ereport(WARNING, (errmodule(MOD_COMM_PROXY), errmsg("Not support libnet read now.")));
        return -1;
    }

    return read(sockfd, buf, count);
}

ssize_t comm_write(int sockfd, const void* buf, size_t count)
{
    if (AmINoKernelModeSockfd(sockfd)) {
        ereport(WARNING, (errmodule(MOD_COMM_PROXY), errmsg("Not support libnet write now.")));
        return -1;
    }

    return write(sockfd, buf, count);
}

ssize_t comm_sendto(
    int sockfd, const void* buf, size_t len, int flags, const struct sockaddr* dest_addr, socklen_t addrlen)
{
    if (AmINoKernelModeSockfd(sockfd)) {
        ereport(WARNING, (errmodule(MOD_COMM_PROXY), errmsg("Not support libnet sendto now.")));
        return -1;
    }

    return sendto(sockfd, buf, len, flags, dest_addr, addrlen);
}

ssize_t comm_recvfrom(int sockfd, void* buf, size_t len, int flags, struct sockaddr* src_addr, socklen_t* addrlen)
{
    if (AmINoKernelModeSockfd(sockfd)) {
        ereport(WARNING, (errmodule(MOD_COMM_PROXY), errmsg("Not support libnet recvfrom now.")));
        return -1;
    }

    return recvfrom(sockfd, buf, len, flags, src_addr, addrlen);
}

ssize_t comm_sendmsg(int sockfd, const struct msghdr* msg, int flags)
{
    if (AmINoKernelModeSockfd(sockfd)) {
        ereport(WARNING, (errmodule(MOD_COMM_PROXY), errmsg("Not support libnet sendmsg now.")));
        return -1;
    }

    return sendmsg(sockfd, msg, flags);
}

ssize_t comm_recvmsg(int sockfd, struct msghdr* msg, int flags)
{
    if (AmINoKernelModeSockfd(sockfd)) {
        ereport(WARNING, (errmodule(MOD_COMM_PROXY), errmsg("Not support libnet recvmsg now.")));
        return -1;
    }

    return recvmsg(sockfd, msg, flags);
}

int comm_select(int fds, fd_set* readfds, fd_set* writefds, fd_set* exceptfds, struct timeval* timeout)
{
    if (AmINoKernelModeSockfd(fds)) {
        ereport(WARNING, (errmodule(MOD_COMM_PROXY), errmsg("Not support libnet select now.")));
        return -1;
    }

    return select(fds, readfds, writefds, exceptfds, timeout);
}

/*
 *********************************** Helper functions to socket **********************************
 */
/*
 * Socket options get/set/clear
 * for connections
 */
CommSocketOption *CommGetConnOptionForce()
{
    CommSocketOption *option = NULL;
    option = &t_thrd.comm_sock_option;

    return option;
}

void CommSetConnOption(CommConnSocketRole role, CommConnAttr attr, CommConnNumaMap map, int group_id)
{
    CommSocketOption *option = NULL;
    option = &t_thrd.comm_sock_option;

    option->conn_attr = attr;
    option->group_id = group_id;
    option->conn_role = role;
    option->numa_map = map;

    option->valid = true;

    return;
}

CommSocketOption *CommGetConnOption()
{
    CommSocketOption *option = NULL;
    option = &t_thrd.comm_sock_option;

    if (option->valid) {
        return option;
    }

    return NULL;
}

void CommClearConnOption()
{
    CommSocketOption *option = NULL;
    option = &t_thrd.comm_sock_option;

    option->valid = false;

    return;
}

/*
 * Socket options get/set/clear
 * for epoll
 */
void CommSetEpollOption(CommEpollType type)
{
    if (g_comm_controller == NULL) {
        return;
    }

    CommEpollOption *option = NULL;
    option = &t_thrd.comm_epoll_option;

    option->epoll_type = type;

    option->valid = true;

    return;
}
CommEpollOption *CommGetEpollOption()
{
    CommEpollOption *option = NULL;
    option = &t_thrd.comm_epoll_option;

    if (option->valid) {
        return option;
    }

    return NULL;
}

void CommClearEpollOption()
{
    CommEpollOption *option = NULL;
    option = &t_thrd.comm_epoll_option;
    option->valid = false;

    return;
}

/*
 * Socket options get/set/clear
 * for poll
 */
void CommSetPollOption(bool no_proxy, bool ctx_switch, int threshold)
{
    if (g_comm_controller == NULL) {
        return;
    }

    CommPollOption *option = NULL;
    option = &t_thrd.comm_poll_option;

    option->ctx_switch = ctx_switch;
    option->no_proxy = no_proxy;
    option->threshold = threshold;

    option->valid = true;

    return;
}
CommPollOption *CommGetPollOption()
{
    CommPollOption *option = NULL;
    option = &t_thrd.comm_poll_option;

    if (option->valid) {
        return option;
    }

    return NULL;
}

void CommClearPollOption()
{
    CommPollOption *option = NULL;
    option = &t_thrd.comm_poll_option;

    option->valid = false;

    return;
}

/*
 *---------------------------------------------------------------------------------------
 * Internal functions
 *---------------------------------------------------------------------------------------
 */
static bool IsCommProxyEnabled()
{
    /* proxy mode is only supported in single node mode */
    if (g_instance.role != VSINGLENODE) {
        return false;
    }

    if (g_comm_proxy_enabled) {
        return true;
    }

    if (g_instance.attr.attr_common.comm_proxy_attr == NULL ||
        strcmp(g_instance.attr.attr_common.comm_proxy_attr, "none") == 0) {
        return false;
    }

    g_comm_proxy_enabled = true;
    return true;
}

static CommConnType CommProxyCreateSocketCheck()
{
    /*
     * If GUC is not set, we do not take further consideration whether we need apply
     * proxy-mode, so just return false
     */
    if (!IsCommProxyEnabled()) {
        return CommConnKernel;
    }

    CommSocketOption *option = CommGetConnOption();

    if (option == NULL) {
        return CommConnKernel;
    }

    if (AmICommConnProxy(option->conn_attr)) {
        return CommConnProxy;
    }

    if (AmICommConnPrueLibNet(option->conn_attr)) {
        return CommConnLibnet;
    }

    return CommConnKernel;
}

static CommConnType CommProxyCreateEpollCheck()
{
    /*
     * If GUC is not set, we do not take further consideration whether we need apply
     * proxy-mode, so just return false
     */
    if (!IsCommProxyEnabled()) {
        return CommConnKernel;
    }

    CommEpollOption *option = CommGetEpollOption();

    if (option == NULL) {
        return CommConnKernel;
    }

    if (option->epoll_type == CommEpollThreadPoolListener) {
        return CommConnProxy;
    }

    if (option->epoll_type == CommEpollPureConnect) {
        /* the socket will be create next must be pure socket */
        CommSocketOption *sock_option = CommGetConnOption();
        if (sock_option != NULL && AmICommConnPrueLibNet(sock_option->conn_attr)) {
            return CommConnLibnet;
        }
    }

    return CommConnKernel;
}

/*
 * poll is a multi fds socket
 * maybe listen more than one type fd, so we use maximum set
 * libnet api not support poll, we also forbidden pure libnet fd to listen by poll
 */
static CommConnType CommProxyGetPollType()
{
    CommPollOption *option = CommGetPollOption();

    if (option != NULL && option->no_proxy) {
        return CommConnKernel;
    }

    if (AmISupportProxyMode()) {
        return CommConnProxy;
    }

    return CommConnKernel;
}

CommConnType CommGetConnType(int sockfd)
{
    /* Return false if com_proxy is not set */
    if (!IsCommProxyEnabled()) {
        return CommConnKernel;
    }

    if (g_comm_controller == NULL) {
        return CommConnKernel;
    }

    /* If sockfd is not register as CommSock in backend, return false */
    if (CommLibNetIsNoProxyLibosFd(sockfd)) {
        return CommConnLibnet;
    }

    if (g_comm_controller->FdGetCommSockDesc(sockfd) == NULL) {
        return CommConnKernel;
    }

    return CommConnProxy;

}

/*
 * HOOK functions for socket interface
 */
template <typename Req_T, typename Fn_T>
int comm_socket_call(Req_T* param, Fn_T fn)
{
    int ret = param->caller(fn, 0);

    return ret;
}

#define COMM_API_____________________

bool LibnetAddConnectWaitEntry(int epfd, int sock, int forRead, int forWrite, int opt = EPOLL_CTL_ADD)
{
    int rc;
    struct epoll_event ev = {0};
    if (opt != EPOLL_CTL_DEL) {
        ev.events = EPOLLRDHUP | EPOLLPRI | EPOLLERR | EPOLLHUP;;
        if (forRead) {
            ev.events |= EPOLLIN;
        }
        if (forWrite) {
            ev.events |= EPOLLOUT;
        }
    }
    ev.data.fd = sock;
    rc = comm_epoll_ctl(epfd, opt, sock, &ev);

    return true;
}

void init_commsock_recv_delay(CommSockDelay *delay)
{
    delay->cur_delay_time = 0;
    return;
}

void reset_commsock_recv_delay(CommSockDelay *delay)
{
    delay->cur_delay_time = 0;
    return;
}

void perform_commsock_recv_delay(CommSockDelay *delay)
{
    if (delay->cur_delay_time == 0) {
        delay->cur_delay_time = COMM_MIN_DELAY_USEC;
    }
    pg_usleep(delay->cur_delay_time);

    delay->cur_delay_time += (int)(delay->cur_delay_time * 0.8 + 0.5);

    if (delay->cur_delay_time > COMM_MAX_DELAY_USEC) {
        delay->cur_delay_time = COMM_MIN_DELAY_USEC;
    }

    return;
}

CommWaitNextStatus comm_proxy_process_wait(struct timeval st, int timeout, bool ctx_switch, sem_t* s_queue_sem)
{
    struct timeval now;
    struct timespec outtime;
    struct timeval et;

    if (timeout == 0) {
        /* if timeout == 0  means return immediately */
        return CommWaitNextBreak;
    }

    gettimeofday(&now, NULL);
    outtime.tv_sec = now.tv_sec;
    outtime.tv_nsec = now.tv_usec * 1000;

     /*
      * The call will block until either:
      * · a file descriptor becomes ready;
      * · the call is interrupted by a signal handler; or
      * · the timeout expires.
      */
    if (timeout == -1) {
        /* if timeout == -1  means an infinite timeout, we need sleep to release CPU */
        if (s_queue_sem != NULL) {
            sem_timedwait(s_queue_sem, &outtime);
        } else {
            if (ctx_switch) {
                pg_usleep(10);
            }
        }
        return CommWaitNextContinue;
    }

    gettimeofday(&et, NULL);
    double elaps_time = (et.tv_sec - st.tv_sec) * 1000 + (float)(et.tv_usec - st.tv_usec) / 1000;
    if (elaps_time >= timeout) {
        /* The timeout interval of the current invoking expires */
        return CommWaitNextBreak;
    } else {
        /* we need to continue to wait for wakeup or time out */
        if (s_queue_sem != NULL) {
            sem_timedwait(s_queue_sem, &outtime);
        } else {
            if (ctx_switch) {
                pg_usleep(10);
            }
        }

        return CommWaitNextContinue;
    }
}

static bool GetPlatformInfo(char* buffer, size_t buffer_size)
{
    FILE* fp = NULL;
    int rc = 0;

    if (buffer_size < PLATFORM_INFO_SIZE) {
        return false;
    }

    rc = memset_s(buffer, PLATFORM_INFO_SIZE, 0, PLATFORM_INFO_SIZE);
    securec_check_c(rc, "\0", "\0");

    fp = fopen("/proc/version", "r");
    if (fp == NULL) {
        ereport(ERROR, (errmodule(MOD_COMM_PROXY),
            errcode(ERRCODE_UNDEFINED_FILE),
            errmsg("Failed to open file /proc/version."),
            errdetail("N/A"),
            errcause("fopen /proc/version fail."),
            erraction("Please check /proc/version exists.")));
        return false;
    }

    if (fgets(buffer, PLATFORM_INFO_SIZE - 1, fp) == NULL) {
        ereport(ERROR, (errmodule(MOD_COMM_PROXY),
            errcode(ERRCODE_FILE_READ_FAILED),
            errmsg("Failed to read file /proc/version."),
            errdetail("N/A"),
            errcause("read /proc/version fail."),
            erraction("Please check /proc/version.")));
        (void)fclose(fp);
        return false;
    }

    (void)fclose(fp);
    return true;
}

bool IsCommProxyStartUp()
{
    return g_comm_controller != NULL;
}

bool CommProxyNeedSetup()
{
    /* g_comm_controller will not be null if the commproxy is started */
    if (g_comm_controller != NULL) {
        return false;
    }

    char platform_info[PLATFORM_INFO_SIZE] = { 0 };
    if (!GetPlatformInfo(platform_info, sizeof(platform_info))) {
        ereport(ERROR, (errmodule(MOD_COMM_PROXY),
            errcode(ERRCODE_SYSTEM_ERROR),
            errmsg("Failed to get platform info."),
            errdetail("N/A"),
            errcause("System error."),
            erraction("Contact Huawei Engineer.")));
        return false;
    }

    /* proxy mode is only supported in euler2.9 arm */
    if (g_instance.attr.attr_common.comm_proxy_attr != NULL &&
        strcmp(g_instance.attr.attr_common.comm_proxy_attr, "none") != 0 &&
        strstr(platform_info, EULEROSV2R9_ARM) == NULL) {
        ereport(ERROR, (errmodule(MOD_COMM_PROXY),
            errcode(ERRCODE_SYSTEM_ERROR),
            errmsg("comm_proxy mode is only supported in euler2.9 arm."),
            errdetail("N/A"),
            errcause("System error."),
            erraction("Contact Huawei Engineer.")));
        g_comm_proxy_enabled = false;
        return false;
    }

    return IsCommProxyEnabled();
}

void CommProxyStartUp()
{
    module_logging_enable_comm(MOD_COMM_PROXY);

    /* If proxy mode is not required, just return */
    if (g_comm_controller != NULL) {
        ereport(DEBUG2, (errmodule(MOD_COMM_PROXY), errmsg("Skip create proxy controller.")));
        return;
    }

    AutoContextSwitch commContext(g_instance.comm_cxt.comm_global_mem_cxt);
    ereport(DEBUG2, (errmodule(MOD_COMM_PROXY), errmsg("Create comm_proxy.")));

    /*
     * Parse comm-proxy relavant attributes
     *
     * TODO steps
     *      - 1. Parse comm_proxy_attr/thread_pool_attr in postgresql.conf
            - 2. Read m_groups_num & m_workers_num from thread_pool_attr
            - 3. Read Comm Proxy's total comm num, per group comm num, bindcpu info from comm_proxy_attr

        EXAMPLE:
            enable_thread_pool = on
            enable_comm_proxy = on
            thread_pool_attr ='8,4,(cpubind:0-9,12-21,24-33,36-45)'
            comm_proxy_attr = '
            "comm-1":{
                "ltran":{
                    "die0":[30,31],
                    "die1":[],
                    "die2":[],
                    "die3":[]
                },
                "comm":{
                    "die0":[0],
                    "die1":[32],
                    "die2":[],
                    "die3":[]
                }
                "wpc":31
            }'
         **/
    
    /* parse comm_proxy_attr to get ltran/comm numbers and bind cpu */
    if (ParseCommProxyAttr(&g_comm_proxy_config) == false) {
        ereport(ERROR, (errmodule(MOD_COMM_PROXY),
            errcode(ERRCODE_SYSTEM_ERROR),
            errmsg("Parse comm_proxy_attr error."),
            errdetail("Please check comm_proxy_attr."),
            errcause("System error."),
            erraction("Contact Huawei Engineer.")));
        exit(-1);
    }

    /* check libnet dependency enviroment */
    int ret;
    if(g_comm_proxy_config.s_enable_libnet) {
        ret = CommCheckLtranProcess();
        if (ret < 1) {
            ereport(WARNING, (errmodule(MOD_COMM_PROXY), errmsg("Can not get ltran info, we start with tcp mode.")));
            g_comm_proxy_config.s_enable_libnet = false;
        }
    }

    /* for all proxy group that not set prototype by user, we use global default type */
    for (int i = 0; i < g_comm_proxy_config.s_comm_proxy_group_cnt; i++) {
        if (g_comm_proxy_config.s_comm_proxy_groups[i].s_comm_type == (int)phy_proto_invalid) {
            g_comm_proxy_config.s_comm_proxy_groups[i].s_comm_type = (int)phy_proto_tcp;
        }
    }

    /* if currnet enviroment not support libnet, we need reset all proxy group prototype to tcp */
    if (!g_comm_proxy_config.s_enable_libnet) {
        for (int i = 0; i < g_comm_proxy_config.s_comm_proxy_group_cnt; i++) {
            if (g_comm_proxy_config.s_comm_proxy_groups[i].s_comm_type == (int)phy_proto_libnet) {
                g_comm_proxy_config.s_comm_proxy_groups[i].s_comm_type = (int)phy_proto_tcp;
            }
        }
    }


    /* Create global comm controller, also communicators are inited here */
    g_comm_controller = New(CurrentMemoryContext) CommController();
    g_comm_controller->Init();
    for (int i = 0; i < g_comm_proxy_config.s_comm_proxy_group_cnt; i++) {
        g_comm_controller->InitCommProxyGroup(i, &g_comm_proxy_config.s_comm_proxy_groups[i]);
    }

    /* start comm_proxy statistic thread */
    if (g_comm_proxy_config.s_enable_dfx) {
        ereport(DEBUG2, (errmodule(MOD_COMM_PROXY), errmsg("Dfx thread start.")));
        CommStartProxyStatThread(g_comm_controller);
    }
    return;
}

void ParseThreadPoolAttr(ThreadAttr *thread_attr)
{
     thread_attr->s_threads_num = DEFAULT_THREAD_POOL_SIZE;
     thread_attr->s_group_num = DEFAULT_COMM_THREAD_POOL_GROUPS;

    return;
}
