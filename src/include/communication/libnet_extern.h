#ifndef _LIBNET_EXTERN_H_
#define _LIBNET_EXTERN_H_

#ifdef __cplusplus
extern "C" {
#endif

struct pkt_buff_info {
    uint32_t type;
    uint32_t last_offset;
    volatile int len;
    volatile int opt_len;
    void* volatile buff_addr;
};

struct ring_buff_desc {
    struct pkt_buff_info *buff;
    uint32_t begin;
    uint32_t end;
    uint32_t buffer_size;
};

int set_lstack_config_file(const char* path);
void libos_network_init(void);
void libos_network_exit(void);
int eth_dev_poll(void);
int is_libos_fd(int fd);
int get_ip_type(unsigned int ip);
int set_fd_type(int s, int type);
int set_lstack_node_name(const char* nodename);
int set_lstack_local_port_range(unsigned short start, unsigned short end);


int __wrap_poll(struct pollfd* fds, nfds_t nfds, int timeout);
int __wrap_epoll_create(int size);
int __wrap_epoll_ctl(int epfd, int op, int fd, struct epoll_event* event);
int __wrap_epoll_wait(int epfd, struct epoll_event* events, int maxevents, int timeout);
int __wrap_fcntl64(int s, int cmd, ...);
int __wrap_fcntl(int s, int cmd, ...);
int __wrap_accept(int s, struct sockaddr* addr, socklen_t* addrlen);
int __wrap_accept4(int s, struct sockaddr* addr, socklen_t* addrlen, int flags);
int __wrap_bind(int s, const struct sockaddr* name, socklen_t namelen);
int __wrap_connect(int s, const struct sockaddr* name, socklen_t namelen);
int __wrap_listen(int s, int backlog);
int __wrap_getpeername(int s, struct sockaddr* name, socklen_t* namelen);
int __wrap_getsockname(int s, struct sockaddr* name, socklen_t* namelen);
int __wrap_getsockopt(int s, int level, int optname, void* optval, socklen_t* optlen);
int __wrap_setsockopt(int s, int level, int optname, const void* optval, socklen_t optlen);
int __wrap_socket(int domain, int type, int protocol);
ssize_t __wrap_read(int s, void* mem, size_t len);
ssize_t __wrap_write(int s, const void* mem, size_t size);
ssize_t __wrap_recv(int sockfd, void* buf, size_t len, int flags);
ssize_t __wrap_send(int sockfd, const void* buf, size_t len, int flags);
int __wrap_close(int s);
int __wrap_shutdown(int s, int how);
pid_t __wrap_fork(void);
int __wrap_sigaction(int signum, const struct sigaction* act, struct sigaction* oldact);
int __wrap_sendmsg(int s, const struct msghdr* message, int flags);

inline int __wrap_epoll_pwait(int epfd,
    struct epoll_event* events, int maxevents, int timeout, const sigset_t *sigmask)
{
    return __wrap_epoll_wait(epfd, events, maxevents, timeout);
}
inline int __wrap_epoll_create1(int flags)
{
    return __wrap_epoll_create(1);
}


//added by ljt: extern get_current_idx and set_current_idx
extern int get_current_idx(void);
extern void set_current_idx(int idx);
extern int get_numa_info_by_idx(int idx);

extern void set_ipid_start_end(int begin, int end);
extern int get_ipid_start(void);
extern int get_ipid_end(void);


ssize_t __wrap_addr_recv(int sockfd, void *buf, size_t len, int flags);
int lwip_addr_recvfrom(int s, void *mem, size_t len, int flags,
              struct sockaddr *from, socklen_t *fromlen);
void lwip_release_pkt_buf_info(void *mem);
int lwip_get_data_from_buff_addr(void *s, size_t length, void *mem);
int lwip_send_buff_addr(int fd, void *mem, int *used_num);
int lwip_alloc_mbuff(int fd, int num, void *mem);
int lwip_put_data_to_buff(const char *s, size_t len, void *mem);

typedef void RingQueueRef;
RingQueueRef* MpScRingQueueCreate(const char *name, unsigned count, int socket_id);
int MpScRingQueueEnque(RingQueueRef *r, void *const *obj_table);
int MpScRingQueueDeque(RingQueueRef *r, void **obj_table);
unsigned MpScRingQueueCount(RingQueueRef *r);
int MpScRIngQueueDestroy(RingQueueRef *r);

//end of ljt
#ifdef __cplusplus
}
#endif
#endif
