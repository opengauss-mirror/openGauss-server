/*
 * This code implements the functons of gms_tcp
 */

#include "gms_tcp.h"

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(gms_tcp_crlf);
PG_FUNCTION_INFO_V1(gms_tcp_available_real);
PG_FUNCTION_INFO_V1(gms_tcp_close_all_connections);
PG_FUNCTION_INFO_V1(gms_tcp_close_connection);
PG_FUNCTION_INFO_V1(gms_tcp_flush);
PG_FUNCTION_INFO_V1(gms_tcp_get_line_real);
PG_FUNCTION_INFO_V1(gms_tcp_get_raw_real);
PG_FUNCTION_INFO_V1(gms_tcp_get_text_real);
PG_FUNCTION_INFO_V1(gms_tcp_open_connection);
PG_FUNCTION_INFO_V1(gms_tcp_write_line);
PG_FUNCTION_INFO_V1(gms_tcp_write_raw_real);
PG_FUNCTION_INFO_V1(gms_tcp_write_text_real);
PG_FUNCTION_INFO_V1(gms_tcp_connection_in);
PG_FUNCTION_INFO_V1(gms_tcp_connection_out);

static void *gms_tcp_alloc(int len);
static inline List *gms_tcp_lappend(List* list, void* datum);
static inline void gms_tcp_init_data_buffer(GMS_TCP_CONNECTION_BUFFER *c_buffer, int32 buffer_size);
static bool gms_tcp_get_addr_by_hostname(char *remotehost, struct sockaddr_in *saddr);
static bool gms_tcp_change_remote_host(char *remotehost, struct sockaddr_in *saddr);
static int gms_tcp_connect(GMS_TCP_CONNECTION_STATE *c_state);
static void gms_tcp_store_connection(GMS_TCP_CONNECTION_STATE *c_state);
static GMS_TCP_CONNECTION_STATE *gms_tcp_get_connection_state(GMS_TCP_CONNECTION *c);
static void gms_tcp_put_data_to_in_buffer(GMS_TCP_CONNECTION_STATE *c_state, char *data_in, int32 data_len);
static int32 gms_tcp_put_data_to_out_buffer(GMS_TCP_CONNECTION_STATE *c_state, char *data_out, int32 data_len);
static void gms_tcp_release_connection_buffer(GMS_TCP_CONNECTION_BUFFER *buffer);
static void gms_tcp_release_connection_state(std::shared_ptr<void> data);
static void gms_tcp_remove_newline(GMS_TCP_CONNECTION_STATE *c_state, char *data, int32 data_len);
static int32 gms_tcp_get_data_from_in_buffer(GMS_TCP_CONNECTION_STATE *c_state, GMS_TCP_CONNECTION_DATA_OPT *data_opt);
static void gms_tcp_close_connection_by_state(GMS_TCP_CONNECTION_STATE *c_state);
static int32 gms_tcp_get_available_bytes(GMS_TCP_CONNECTION_STATE *c_state);
static inline int gms_tcp_set_connection_rcv_timeout(GMS_TCP_CONNECTION_STATE *c_state, int32 timeout);
static inline int gms_tcp_set_connection_send_timeout(GMS_TCP_CONNECTION_STATE *c_state, int32 timeout);
static void gms_tcp_wait(GMS_TCP_CONNECTION_STATE *c_state, bool report_err);
static void gms_tcp_get_data_from_connection_to_in_buffer(GMS_TCP_CONNECTION_STATE *c_state, GMS_TCP_CONNECTION_DATA_OPT *data_opt);
static bool gms_tcp_check_charset(char *charset);
static inline int32 gms_tcp_get_in_buffer_data_bytes(GMS_TCP_CONNECTION_BUFFER *in_buffer);
static char *gms_tcp_encode_data_by_charset(GMS_TCP_CONNECTION_STATE *c_state, GMS_TCP_CONNECTION_DATA_OPT *data_opt, bool is_write);
static char *gms_tcp_encrypt_data(GMS_TCP_CONNECTION_STATE *c_state, char *data);
static char *gms_tcp_decrypt_data(GMS_TCP_CONNECTION_STATE *c_state, char *data);
static void gms_tcp_get_data(GMS_TCP_CONNECTION_STATE *c_state, GMS_TCP_CONNECTION_DATA_OPT *data_opt);
static int32 gms_tcp_write_data(GMS_TCP_CONNECTION_STATE *c_state, GMS_TCP_CONNECTION_DATA_OPT *data_opt);
static void gms_tcp_check_connection_null(GMS_TCP_CONNECTION_STATE *c_state);

static void gms_tcp_comm_memcopy_str(char *dst, int dst_len, char *src, int *offset);
static void gms_tcp_comm_memcopy_int(char *dst, int dst_len, char *src, int src_len, int *offset);
static int gms_tcp_comm_get_str(text *data_t, char **data, int data_len_max, int *get_data_len, int *total_len);
static bool gms_tcp_comm_cmp_str(const char *str_1, const char *str_2);


static void *
gms_tcp_alloc(int len)
{
    void *result = NULL;

    MemoryContext old_context = MemoryContextSwitchTo(t_thrd.mem_cxt.portal_mem_cxt);
    result = palloc0(len);
    MemoryContextSwitchTo(old_context);

    return result;
}

static inline List *
gms_tcp_lappend(List* list, void* datum)
{
    List *result = NULL;

    MemoryContext old_context = MemoryContextSwitchTo(t_thrd.mem_cxt.portal_mem_cxt);
    result = lappend(list, datum);
    MemoryContextSwitchTo(old_context);

    return result;
}

static inline List *
gms_tcp_lappend_int(List* list, int datum)
{
    List *result = NULL;

    MemoryContext old_context = MemoryContextSwitchTo(t_thrd.mem_cxt.portal_mem_cxt);
    result = lappend_int(list, datum);
    MemoryContextSwitchTo(old_context);

    return result;
}

void
gms_tcp_get_connection_info(GMS_TCP_CONNECTION *c, GMS_TCP_CONNECTION_INFO *c_info)
{
    GMS_TCP_CONNECTION_HEAD *c_h = &c->c_h;
    int offset = 0;

    if (c_h->remote_host_len) {
        c_info->remote_host = (char *)gms_tcp_alloc(c_h->remote_host_len);
        gms_tcp_comm_memcopy_str(c_info->remote_host, c_h->remote_host_len, c->data + offset, &offset);
    }
    if (c_h->remote_port_len) {
        gms_tcp_comm_memcopy_int((char *)&c_info->remote_port, c_h->remote_port_len, c->data + offset, c_h->remote_port_len, &offset);
    }
    if (c_h->local_host_len) {
        c_info->local_host = (char *)gms_tcp_alloc(c_h->local_host_len);
        gms_tcp_comm_memcopy_str(c_info->local_host, c_h->local_host_len, c->data + offset, &offset);
    }
    if (c_h->local_port_len) {
        gms_tcp_comm_memcopy_int((char *)&c_info->local_port, c_h->local_port_len, c->data + offset, c_h->local_port_len, &offset);
    }
    if (c_h->in_buffer_size_len) {
        gms_tcp_comm_memcopy_int((char *)&c_info->in_buffer_size, c_h->in_buffer_size_len, c->data + offset, c_h->in_buffer_size_len, &offset);
    }
    if (c_h->out_buffer_size_len) {
        gms_tcp_comm_memcopy_int((char *)&c_info->out_buffer_size, c_h->out_buffer_size_len, c->data + offset, c_h->out_buffer_size_len, &offset);
    }
    if (c_h->charset_len) {
        c_info->charset = (char *)gms_tcp_alloc(c_h->charset_len);
        gms_tcp_comm_memcopy_str(c_info->charset, c_h->charset_len, c->data + offset, &offset);
    }
    if (c_h->newline_len) {
        c_info->newline = (char *)gms_tcp_alloc(c_h->newline_len);
        gms_tcp_comm_memcopy_str(c_info->newline, c_h->newline_len, c->data + offset, &offset);
    }
    if (c_h->tx_timeout_len) {
        gms_tcp_comm_memcopy_int((char *)&c_info->tx_timeout, c_h->tx_timeout_len, c->data + offset, c_h->tx_timeout_len, &offset);
    }
    if (c_h->fd_len) {
        gms_tcp_comm_memcopy_int((char *)&c_info->fd, c_h->fd_len, c->data + offset, c_h->fd_len, &offset);
    }

    return;
}

void
gms_tcp_get_connection_head_by_info(GMS_TCP_CONNECTION_INFO *c_info, GMS_TCP_CONNECTION_HEAD *c_h)
{
    if (c_info->remote_host) {
        c_h->remote_host_len = strlen(c_info->remote_host) + 1;
        c_h->total_len += c_h->remote_host_len;
    }
    if (c_info->remote_port) {
        c_h->remote_port_len = sizeof(c_info->remote_port);
        c_h->total_len += c_h->remote_port_len;
    }
    if (c_info->local_host) {
        c_h->local_host_len = strlen(c_info->local_host) + 1;
        c_h->total_len += c_h->local_host_len;
    }
    if (c_info->local_port) {
        c_h->local_port_len = sizeof(c_info->local_port);
        c_h->total_len += c_h->local_port_len;
    }
    if (c_info->in_buffer_size) {
        c_h->in_buffer_size_len = sizeof(c_info->in_buffer_size);
        c_h->total_len += c_h->in_buffer_size_len;
    }
    if (c_info->out_buffer_size) {
        c_h->out_buffer_size_len = sizeof(c_info->out_buffer_size);
        c_h->total_len += c_h->out_buffer_size_len;
    }
    if (c_info->charset) {
        c_h->charset_len = strlen(c_info->charset) + 1;
        c_h->total_len += c_h->charset_len;
    }
    if (c_info->newline) {
        c_h->newline_len = strlen(c_info->newline) + 1;
        c_h->total_len += c_h->newline_len;
    }
    if (c_info->tx_timeout) {
        c_h->tx_timeout_len = sizeof(c_info->tx_timeout);
        c_h->total_len += c_h->tx_timeout_len;
    }
    if (c_info->fd) {
        c_h->fd_len = sizeof(c_info->fd);
        c_h->total_len += c_h->fd_len;
    }

    return;
}

static inline void
gms_tcp_init_data_buffer(GMS_TCP_CONNECTION_BUFFER *c_buffer, int32 buffer_size)
{
    errno_t rc = EOK;
    rc = memset_s(c_buffer, GMS_TCP_MAX_IN_BUFFER_SIZE, 0, sizeof(GMS_TCP_CONNECTION_BUFFER));
    securec_check_c(rc, "\0", "\0");
    if (buffer_size) {
        c_buffer->max_buffer_size = buffer_size;
        c_buffer->free_space = buffer_size;
        c_buffer->data = (char *)gms_tcp_alloc(buffer_size);
    }
}

static bool
gms_tcp_get_addr_by_hostname(char *remotehost, struct sockaddr_in *saddr)
{
    struct hostent *hptr = gethostbyname(remotehost);
    if (!hptr) {
        return false;
    }

    if (hptr->h_addrtype == AF_INET) {
        saddr->sin_addr = *((struct in_addr *)hptr->h_addr_list[0]);
        return true;
    }

    return false;
}

static bool
gms_tcp_change_remote_host(char *remotehost, struct sockaddr_in *saddr)
{
    int ret = 0;

    ret = inet_aton(remotehost, &saddr->sin_addr);
    if (!ret) {
        return gms_tcp_get_addr_by_hostname(remotehost, saddr);
    }

    return true;
}

static int
gms_tcp_connect(GMS_TCP_CONNECTION_STATE *c_state)
{
    c_state->fd = socket(AF_INET, SOCK_STREAM, 0);
    struct sockaddr_in saddr;
    int res = 0;
    errno_t rc = EOK;

    rc = memset_s(&saddr, GMS_TCP_MAX_IN_BUFFER_SIZE, 0,sizeof(saddr));
    securec_check_c(rc, "\0", "\0");
    saddr.sin_family = AF_INET;
    saddr.sin_port = htons(c_state->c_info.remote_port);
    saddr.sin_addr = c_state->c_info.saddr.sin_addr;

    res = connect(c_state->fd, (struct sockaddr *)&saddr, sizeof(saddr));
    if (res) {
        c_state->fd = 0;
        return GMS_TCP_CONNECT_FAIL;
    }

    c_state->state = GMS_TCP_CONNECT_OK;

    return GMS_TCP_OK;
}

static void
gms_tcp_store_connection(GMS_TCP_CONNECTION_STATE *c_state)
{
    PortalSpecialData *node;
    node = (PortalSpecialData *)gms_tcp_alloc(sizeof(PortalSpecialData));
    node->data = std::shared_ptr<void>(c_state);
    node->cleanup = gms_tcp_release_connection_state;
    GMS_TCP_FD_BAK_LIST = gms_tcp_lappend(GMS_TCP_FD_BAK_LIST, node);
}

static GMS_TCP_CONNECTION_STATE *
gms_tcp_get_connection_state(GMS_TCP_CONNECTION *c)
{
    ListCell *lc = NULL;
    GMS_TCP_CONNECTION_INFO *c_info = NULL;
    GMS_TCP_CONNECTION_STATE *result = NULL;

    c_info = (GMS_TCP_CONNECTION_INFO *)gms_tcp_alloc(sizeof(GMS_TCP_CONNECTION_INFO));
    gms_tcp_get_connection_info(c, c_info);

    foreach (lc, GMS_TCP_FD_BAK_LIST) {
        PortalSpecialData *node = (PortalSpecialData *)lfirst(lc);
        GMS_TCP_CONNECTION_STATE *c_state = std::static_pointer_cast<GMS_TCP_CONNECTION_STATE>(node->data).get();

        if (c_info->fd == c_state->fd) {
            result = c_state;
            break;
        }
    }

    gms_tcp_release_connection_info(c_info);
    pfree(c_info);

    return result;
}

GMS_TCP_CONNECTION_STATE *
gms_tcp_get_connection_state_by_fd(int fd)
{
    ListCell *lc = NULL;
    GMS_TCP_CONNECTION_STATE *result = NULL;

    foreach (lc, GMS_TCP_FD_BAK_LIST) {
        PortalSpecialData *node = (PortalSpecialData *)lfirst(lc);
        GMS_TCP_CONNECTION_STATE *c_state = std::static_pointer_cast<GMS_TCP_CONNECTION_STATE>(node->data).get();

        if (fd == c_state->fd) {
            result = c_state;
            break;
        }
    }

    return result;
}

static void
gms_tcp_put_data_to_in_buffer(GMS_TCP_CONNECTION_STATE *c_state, char *data_in, int32 data_len)
{
    GMS_TCP_CONNECTION_BUFFER *in_buffer = &c_state->in_buffer;
    int32 left_len = 0;
    error_t errorno = EOK;

    if (in_buffer->free_space <= data_len) {
        gms_tcp_close_connection_by_state(c_state);
        ereport(ERROR,
                (errcode(ERRCODE_BUFFER_TOO_SMALL),
                    errmsg("input bufer is full.")));
    }

    if (in_buffer->end > in_buffer->start) {
        /* |----start    data    end----| */
        left_len = in_buffer->max_buffer_size - in_buffer->end;
        if (left_len > data_len) {
            errorno = memcpy_s(&in_buffer->data[in_buffer->end], left_len, data_in, data_len);
            securec_check_c(errorno, "\0", "\0");
            in_buffer->end += data_len;
        } else {
            errorno = memcpy_s(&in_buffer->data[in_buffer->end], left_len, data_in, left_len);
            securec_check_c(errorno, "\0", "\0");
            errorno = memcpy_s(in_buffer->data, in_buffer->start, &data_in[left_len], (data_len - left_len));
            securec_check_c(errorno, "\0", "\0");
            in_buffer->end = (data_len - left_len);
        }
    } else if (in_buffer->start > in_buffer->end){
        /* |    data    end----start    data    | */
        errorno = memcpy_s(&in_buffer->data[in_buffer->end], in_buffer->start - in_buffer->end, data_in, data_len);
        securec_check_c(errorno, "\0", "\0");
        in_buffer->end += data_len;
    } else {
        /* in buffer is empty, reset it. */
        errorno = memset_s(in_buffer->data, GMS_TCP_MAX_IN_BUFFER_SIZE, 0, in_buffer->max_buffer_size);
        securec_check(errorno,"\0","\0");
        in_buffer->free_space = in_buffer->max_buffer_size;
        in_buffer->start = 0;
        in_buffer->end = 0;
        errorno = memcpy_s(in_buffer->data, in_buffer->max_buffer_size, data_in, data_len);
        securec_check(errorno,"\0","\0");
        in_buffer->end += data_len;
    }

    in_buffer->free_space -= data_len;

    return;
}

static int32
gms_tcp_put_data_to_out_buffer(GMS_TCP_CONNECTION_STATE *c_state, char *data_out, int32 data_len)
{
    GMS_TCP_CONNECTION_BUFFER *out_buffer = &c_state->out_buffer;
    errno_t rc = EOK;

    if (out_buffer->free_space < data_len) {
        gms_tcp_close_connection_by_state(c_state);
        ereport(ERROR,
                (errcode(ERRCODE_BUFFER_TOO_SMALL),
                    errmsg("output bufer is full.")));
    }

    rc = memcpy_s(&out_buffer->data[out_buffer->end], out_buffer->max_buffer_size - out_buffer->end, data_out, data_len);
    securec_check_c(rc, "\0", "\0");
    out_buffer->end += data_len;
    out_buffer->free_space -= data_len;

    return data_len;
}

void
gms_tcp_release_connection_info(GMS_TCP_CONNECTION_INFO *c_info)
{
    if (c_info->remote_host) {
        pfree(c_info->remote_host);
        c_info->remote_host = NULL;
    }

    if (c_info->local_host) {
        pfree(c_info->local_host);
        c_info->local_host = NULL;
    }

    if (c_info->charset) {
        pfree(c_info->charset);
        c_info->charset = NULL;
    }

    if (c_info->newline) {
        pfree(c_info->newline);
        c_info->newline = NULL;
    }

}

static void
gms_tcp_release_connection_buffer(GMS_TCP_CONNECTION_BUFFER *buffer)
{
    if (buffer->data) {
        pfree(buffer->data);
        buffer->data = NULL;
    }

    buffer->free_space = 0;
    buffer->start = 0;
    buffer->end = 0;
}

static void
gms_tcp_release_connection_state(std::shared_ptr<void> data)
{
    GMS_TCP_CONNECTION_STATE *c_state = std::static_pointer_cast<GMS_TCP_CONNECTION_STATE>(data).get();

    if (c_state->fd) {
        close(c_state->fd);
    }

    gms_tcp_release_connection_buffer(&c_state->in_buffer);
    gms_tcp_release_connection_buffer(&c_state->out_buffer);
    gms_tcp_release_connection_info(&c_state->c_info);
    pfree(c_state);
}

static void
gms_tcp_remove_newline(GMS_TCP_CONNECTION_STATE *c_state, char *data, int32 data_len)
{
    int32 i = 0;
    char *newline = NULL;
    int newline_len = 0;
    errno_t rc = EOK;

    Assert(c_state->c_info.newline);
    if (strcmp(c_state->c_info.newline, "CRLF") == 0) {
        newline_len = 2;
        newline = "\r\n";
    } else {
        newline_len = strlen(c_state->c_info.newline);
        newline = c_state->c_info.newline;
    }

    while (i <= data_len - newline_len) {
        if (strncmp(&data[i], newline, newline_len) == 0) {
            rc = memset_s(&data[i], GMS_TCP_MAX_IN_BUFFER_SIZE, 0, newline_len);
            securec_check_c(rc, "\0", "\0");
            i += newline_len;

            continue;
        }
        i++;
    }
}

static int32
gms_tcp_get_data_from_in_buffer(GMS_TCP_CONNECTION_STATE *c_state, GMS_TCP_CONNECTION_DATA_OPT *data_opt)
{
    GMS_TCP_CONNECTION_BUFFER *in_buffer = NULL;
    errno_t rc = EOK;

    in_buffer = &c_state->in_buffer;

    if (in_buffer->end == in_buffer->start) {
        gms_tcp_get_data_from_connection_to_in_buffer(c_state, data_opt);
    }

    /* |----start    data    end----| */
    if (in_buffer->end > in_buffer->start) {
        int32 rcv_len = (data_opt->len && (data_opt->len < (in_buffer->end - in_buffer->start))) ?
                        data_opt->len :
                        (in_buffer->end - in_buffer->start);
        rc = memcpy_s(data_opt->data, GMS_TCP_MAX_IN_BUFFER_SIZE, &in_buffer->data[in_buffer->start], rcv_len);
        securec_check_c(rc, "\0", "\0");
        data_opt->data_len = rcv_len;
        if (!data_opt->peek) {
            rc = memset_s(&in_buffer->data[in_buffer->start], GMS_TCP_MAX_IN_BUFFER_SIZE, 0, rcv_len);
            securec_check_c(rc, "\0", "\0");
            in_buffer->start += rcv_len;
            in_buffer->free_space += rcv_len;
        }
    } else {
        /* |    data    end----start    data    | */
        int after_start = in_buffer->max_buffer_size - in_buffer->start;
        int32 rcv_len = (data_opt->len && (data_opt->len < (in_buffer->max_buffer_size - in_buffer->free_space))) ?
                        data_opt->len :
                        (in_buffer->max_buffer_size - in_buffer->free_space);
        if (rcv_len <= after_start) {
            rc = memcpy_s(data_opt->data, GMS_TCP_MAX_IN_BUFFER_SIZE, &in_buffer->data[in_buffer->start], rcv_len);
            securec_check_c(rc, "\0", "\0");
            if (!data_opt->peek) {
                rc = memset_s(&in_buffer->data[in_buffer->start], GMS_TCP_MAX_IN_BUFFER_SIZE, 0, rcv_len);
                securec_check_c(rc, "\0", "\0");
                in_buffer->start = (in_buffer->start + rcv_len) % in_buffer->max_buffer_size;
                in_buffer->free_space += rcv_len;
            }
        } else {
            int32 left_len = rcv_len - after_start;
            rc = memcpy_s(data_opt->data, GMS_TCP_MAX_IN_BUFFER_SIZE, &in_buffer->data[in_buffer->start], after_start);
            securec_check_c(rc, "\0", "\0");
            data_opt->data_len += after_start;
            rc = memcpy_s(&data_opt->data[after_start], GMS_TCP_MAX_IN_BUFFER_SIZE - after_start, in_buffer->data, left_len);
            securec_check_c(rc, "\0", "\0");
            data_opt->data_len += left_len;
            if (!data_opt->peek) {
                rc = memset_s(&in_buffer->data[in_buffer->start], GMS_TCP_MAX_IN_BUFFER_SIZE, 0, after_start);
                securec_check_c(rc, "\0", "\0");
                rc = memset_s(in_buffer->data, GMS_TCP_MAX_IN_BUFFER_SIZE, 0, left_len);
                securec_check_c(rc, "\0", "\0");
                in_buffer->start = left_len;
                in_buffer->free_space += rcv_len;
            }
        }
    }

    if (data_opt->remove_crlf) {
        gms_tcp_remove_newline(c_state, data_opt->data, data_opt->data_len);
    }

    return data_opt->data_len;
}

static void
gms_tcp_close_connection_by_state(GMS_TCP_CONNECTION_STATE *c_state)
{
    ListCell *lc = NULL;
    PortalSpecialData *find = NULL;

    Assert(c_state);
    Assert(c_state->state == GMS_TCP_CONNECT_OK);

    foreach (lc, GMS_TCP_FD_BAK_LIST) {
        PortalSpecialData *node = (PortalSpecialData *)lfirst(lc);
        if (c_state == std::static_pointer_cast<GMS_TCP_CONNECTION_STATE>(node->data).get()) {
            find = node;
            break;
        }
    }

    if (find) {
        GMS_TCP_FD_BAK_LIST = list_delete_ptr(GMS_TCP_FD_BAK_LIST, find);
        find->cleanup(find->data);
        pfree(find);
    }
}

static int32
gms_tcp_get_available_bytes(GMS_TCP_CONNECTION_STATE *c_state)
{
    int32 bytes_to_read = 0;

    gms_tcp_wait(c_state, false);

    if (ioctl(c_state->fd, FIONREAD, &bytes_to_read) == 0) {
        return bytes_to_read;
    }

    return 0;
}

static inline int
gms_tcp_set_connection_rcv_timeout(GMS_TCP_CONNECTION_STATE *c_state, int32 timeout)
{
    int ret;
    fd_set rd;
    struct timeval timeoutval = {timeout, 0};

    FD_ZERO(&rd);
    FD_SET(c_state->fd, &rd);

    if (timeout < 0) {
        ret = select(c_state->fd + 1, &rd, NULL, NULL, NULL);
    } else {
        ret = select(c_state->fd + 1, &rd, NULL, NULL, &timeoutval);
    }
    return ret;
}

static inline int
gms_tcp_set_connection_send_timeout(GMS_TCP_CONNECTION_STATE *c_state, int32 timeout)
{
    int ret;
    fd_set wd;
    struct timeval timeoutval = {timeout, 0};

    FD_ZERO(&wd);
    FD_SET(c_state->fd, &wd);

    ret = select(c_state->fd + 1, NULL, &wd, NULL, &timeoutval);
    return ret;
}

static void
gms_tcp_wait(GMS_TCP_CONNECTION_STATE *c_state, bool report_err)
{
    int32 timeout = 0;

    if (c_state->c_info.available_wait) {
        /* recv data by available function. */
        if (c_state->c_info.available_timeout) {
            timeout = c_state->c_info.available_timeout;
        }
    } else if (c_state->c_info.get_data_wait) {
        /* recv data by get/read function. */
        if (c_state->c_info.tx_timeout) {
            timeout = c_state->c_info.tx_timeout;
        }
    }

    if (timeout) {
        int ret = gms_tcp_set_connection_rcv_timeout(c_state, timeout);
        if (report_err) {
            if (ret == 0) {
                ereport(ERROR,
                        (errcode(ERRCODE_TRANSFER_TIMEOUT),
                            errmsg("recv data timeout.")));
            } else if (ret < 0) {
                ereport(ERROR,
                        (errcode(ERRCODE_NETWORK_ERROR),
                            errmsg("recv data error.")));
            }
        }
    }
}

static void
gms_tcp_get_data_from_connection_to_in_buffer(GMS_TCP_CONNECTION_STATE *c_state, GMS_TCP_CONNECTION_DATA_OPT *data_opt)
{
    GMS_TCP_CONNECTION_BUFFER *in_buffer = &c_state->in_buffer;
    char *data = NULL;
    int32 recv_len = 0;

    Assert(in_buffer->max_buffer_size && in_buffer->data);

    data = (char *)palloc0(in_buffer->max_buffer_size);

    gms_tcp_wait(c_state, data_opt->report_err);

    recv_len = recv(c_state->fd, data, in_buffer->max_buffer_size, MSG_DONTWAIT);
    if (recv_len > 0) {
        char *decrypt_data = NULL;

        if (!data_opt->len) {
            decrypt_data = gms_tcp_decrypt_data(c_state, data);
        }

        if (decrypt_data) {
            gms_tcp_put_data_to_in_buffer(c_state, decrypt_data, strlen(decrypt_data));
        } else {
            gms_tcp_put_data_to_in_buffer(c_state, data, recv_len);
        }
    } else if (data_opt->report_err) {
        gms_tcp_close_connection_by_state(c_state);
        pfree(data);
        ereport(ERROR,
                    (errcode(ERRCODE_NETWORK_ERROR),
                        errmsg("recv data error.")));
    }

    pfree(data);

    return;
}

static bool
gms_tcp_check_charset(char *charset)
{
    const char *client_encoding_name = pg_get_client_encoding_name();
    iconv_t cd;

    cd = iconv_open(charset, client_encoding_name);
    if (cd == ((iconv_t)(-1))) {
        ereport(WARNING,
                (errmsg("Can not change string from %s to %s", client_encoding_name, charset)));
        return false;
    }

    iconv_close(cd);

    return true;
}

static inline int32
gms_tcp_get_in_buffer_data_bytes(GMS_TCP_CONNECTION_BUFFER *in_buffer)
{
    return in_buffer->max_buffer_size - in_buffer->free_space;
}

static char *
gms_tcp_encode_data_by_charset(GMS_TCP_CONNECTION_STATE *c_state, GMS_TCP_CONNECTION_DATA_OPT *data_opt, bool is_write)
{
    iconv_t cd;
    char *data_out = NULL;
    size_t data_out_len = GMS_TCP_MAX_OUT_BUFFER_SIZE;
    char *data_out_p = NULL;
    char *data_in_p = NULL;
    const char *client_encoding_name = pg_get_client_encoding_name();
    size_t len = data_opt->len ? data_opt->len : strlen(data_opt->data);

    if (gms_tcp_comm_cmp_str(c_state->c_info.charset, client_encoding_name)) {
        return NULL;
    }

    if (is_write) {
        cd = iconv_open(c_state->c_info.charset, client_encoding_name);
    } else {
        cd = iconv_open(client_encoding_name, c_state->c_info.charset);
    }

    if (cd < 0) {
        return NULL;
    }

    data_out = (char *)palloc0(data_out_len);
    data_in_p = data_opt->data;
    data_out_p = data_out;
    if(iconv(cd, &data_in_p, &len, &data_out_p, &data_out_len) < 0){
        iconv_close(cd);
        pfree(data_out);
        return NULL;
    }

    iconv_close(cd);
    return data_out;
}

static char *
gms_tcp_encrypt_data(GMS_TCP_CONNECTION_STATE *c_state, char *data)
{
    /* Do not support encrypt now. */
    return NULL;
}

static char *
gms_tcp_decrypt_data(GMS_TCP_CONNECTION_STATE *c_state, char *data)
{
    /* Do not support decrypt now. */
    return NULL;
}

static void
gms_tcp_get_data(GMS_TCP_CONNECTION_STATE *c_state, GMS_TCP_CONNECTION_DATA_OPT *data_opt)
{
    GMS_TCP_CONNECTION_BUFFER *in_buffer = NULL;
    errno_t rc = EOK;

    in_buffer = &c_state->in_buffer;
    if (in_buffer->max_buffer_size && in_buffer->data) {
        /* if in buffer is used, get data from in buffer. */
        data_opt->data_len = gms_tcp_get_data_from_in_buffer(c_state, data_opt);
        c_state->c_info.get_data_wait = false;
        /* recv data from connection to inbuffer no wait. */
        data_opt->report_err = false;
        gms_tcp_get_data_from_connection_to_in_buffer(c_state, data_opt);
    } else {
        if (c_state->c_info.tx_timeout) {
            int ret = gms_tcp_set_connection_rcv_timeout(c_state, c_state->c_info.tx_timeout);
            if (ret == 0) {
                ereport(ERROR,
                        (errcode(ERRCODE_TRANSFER_TIMEOUT),
                            errmsg("recv data timeout.")));
            } else if (ret < 0) {
                ereport(ERROR,
                        (errcode(ERRCODE_NETWORK_ERROR),
                            errmsg("recv data error.")));
            }
        }

        data_opt->data_len = recv(c_state->fd,
                                 data_opt->data,
                                 data_opt->len ? data_opt->len : GMS_TCP_MAX_IN_BUFFER_SIZE,
                                 MSG_DONTWAIT | (data_opt->peek ? MSG_PEEK : 0));
        if (data_opt->data_len <= 0) {
            gms_tcp_close_connection_by_state(c_state);
            ereport(ERROR,
                        (errcode(ERRCODE_NETWORK_ERROR),
                            errmsg("recv data error.")));
        }

        /* if the data is encrypted, we must get the hold data. */
        if (!data_opt->len) {
            char *decrypt_data = gms_tcp_decrypt_data(c_state, data_opt->data);
            if (decrypt_data) {
                rc = memset_s(data_opt->data, GMS_TCP_MAX_IN_BUFFER_SIZE, 0, GMS_TCP_MAX_IN_BUFFER_SIZE);
                securec_check_c(rc, "\0", "\0");
                rc = memcpy_s(data_opt->data, GMS_TCP_MAX_IN_BUFFER_SIZE, decrypt_data, strlen(decrypt_data));
                securec_check_c(rc, "\0", "\0");
                data_opt->data_len = strlen(decrypt_data);
            }
        }

        if (data_opt->remove_crlf) {
            gms_tcp_remove_newline(c_state, data_opt->data, data_opt->data_len);
        }
    }
}

static int32
gms_tcp_write_data(GMS_TCP_CONNECTION_STATE *c_state, GMS_TCP_CONNECTION_DATA_OPT *data_opt)
{
    char *data = NULL;
    int32 data_len = 0;
    char *encrypt_data = NULL;
    errno_t rc = EOK;

    data_len = data_opt->len ? data_opt->len : strlen(data_opt->data);
    if (c_state->out_buffer.data) {
        bool done = false;
        if (data_opt->ch_charset && c_state->c_info.charset) {
            
            /* change charset, if data is not NULL, send data, or send data_opt->data. */
            data = gms_tcp_encode_data_by_charset(c_state, data_opt, true);
            if (data) {
                /* encrypt the data, if encrypt_data is not NULL, send encrypt_data, or send data. */
                if (data_opt->encrypt) {
                    encrypt_data = gms_tcp_encrypt_data(c_state, data);
                }
                if (encrypt_data) {
                    data_len = gms_tcp_put_data_to_out_buffer(c_state, encrypt_data, strlen(encrypt_data));
                } else {
                    data_len = gms_tcp_put_data_to_out_buffer(c_state, data, strlen(data));
                }
                pfree(data);
                done = true;
            }
        }
        if (!done) {
            /* 
             * the charset is not change, try encrypt the data,
             * if encrypt_data is not NULL, send encrypt_data, or send data_opt->data.
             */
            if (data_opt->encrypt) {
                encrypt_data = gms_tcp_encrypt_data(c_state, data_opt->data);
            }
            if (encrypt_data) {
                data_len = gms_tcp_put_data_to_out_buffer(c_state, encrypt_data, strlen(encrypt_data));
            } else {
                data_len = gms_tcp_put_data_to_out_buffer(c_state, data_opt->data, data_len);
            }
        }
    } else {
        char *data_t = NULL;

        bool done = false;
        if (data_opt->ch_charset && c_state->c_info.charset) {
            /* change charset, if data is not NULL, send data, or send data_opt->data. */
            char *data = gms_tcp_encode_data_by_charset(c_state, data_opt, true);
            if (data) {
                /* encrypt the data, if encrypt_data is not NULL, send encrypt_data, or send data. */
                if (data_opt->encrypt) {
                    encrypt_data = gms_tcp_encrypt_data(c_state, data);
                }
                if (encrypt_data) {
                    data_t = (char *)palloc0(strlen(encrypt_data));
                    rc = memcpy_s(data_t, strlen(encrypt_data), encrypt_data, strlen(encrypt_data));
                    securec_check_c(rc, "\0", "\0");
                    data_len = strlen(encrypt_data);
                } else {
                    data_t = (char *)palloc0(strlen(data));
                    rc = memcpy_s(data_t, strlen(data), data, strlen(data));
                    securec_check_c(rc, "\0", "\0");
                    data_len = strlen(data);
                }
                pfree(data);
                done = true;
            }
        }
        if (!done) {
            /* 
             * the charset is not change, try encrypt the data,
             * if encrypt_data is not NULL, send encrypt_data, or send data_opt->data.
             */
            if (data_opt->encrypt) {
                encrypt_data = gms_tcp_encrypt_data(c_state, data_opt->data);
            }
            if (encrypt_data) {
                data_t = (char *)palloc0(strlen(encrypt_data));
                rc = memcpy_s(data_t, strlen(encrypt_data), encrypt_data, strlen(encrypt_data));
                securec_check_c(rc, "\0", "\0");
                data_len = strlen(encrypt_data);
            } else {
                data_t = (char *)palloc0(strlen(data_opt->data));
                rc = memcpy_s(data_t, strlen(data_opt->data), data_opt->data, strlen(data_opt->data));
                securec_check_c(rc, "\0", "\0");
            }
        }

        if (c_state->c_info.tx_timeout) {
            int ret = gms_tcp_set_connection_send_timeout(c_state, c_state->c_info.tx_timeout);
            if (ret == 0) {
                ereport(ERROR,
                        (errcode(ERRCODE_TRANSFER_TIMEOUT),
                            errmsg("recv data timeout.")));
            } else if (ret < 0) {
                ereport(ERROR,
                        (errcode(ERRCODE_NETWORK_ERROR),
                            errmsg("recv data error.")));
            }
        }
        data_len = send(c_state->fd, data_t, data_len, MSG_DONTWAIT);
        pfree(data_t);
    }

    PG_RETURN_INT32(data_len);
}

static void
gms_tcp_check_connection_null(GMS_TCP_CONNECTION_STATE *c_state)
{
    if (!c_state) {
        ereport(ERROR,
                    (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("get connection fail.")));
    } else {
        Assert(c_state->state == GMS_TCP_CONNECT_OK);
    }
}

Datum
gms_tcp_crlf(PG_FUNCTION_ARGS)
{
    PG_RETURN_TEXT_P(cstring_to_text("\r\n"));
}

Datum
gms_tcp_available_real(PG_FUNCTION_ARGS)
{
    GMS_TCP_CONNECTION *c = (GMS_TCP_CONNECTION *)PG_GETARG_POINTER(0);
    int32 available_timeout = PG_ARGISNULL(1) ? -1 : PG_GETARG_INT32(1);
    GMS_TCP_CONNECTION_STATE *c_state = gms_tcp_get_connection_state(c);
    GMS_TCP_CONNECTION_BUFFER *in_buffer = &c_state->in_buffer;
    GMS_TCP_CONNECTION_DATA_OPT data_opt = {0};
    int32 available = 0;

    gms_tcp_check_connection_null(c_state);

    if (in_buffer->max_buffer_size && in_buffer->data) {
        c_state->c_info.available_wait = true;
        c_state->c_info.available_timeout = 0;
        data_opt.report_err = false;
        gms_tcp_get_data_from_connection_to_in_buffer(c_state, &data_opt);

        available = gms_tcp_get_in_buffer_data_bytes(in_buffer);
        if (!available && available_timeout) {
            c_state->c_info.available_timeout = available_timeout;
            gms_tcp_get_data_from_connection_to_in_buffer(c_state, &data_opt);
            available = gms_tcp_get_in_buffer_data_bytes(in_buffer);
        }
    } else {
        c_state->c_info.available_wait = true;
        c_state->c_info.available_timeout = available_timeout;
        available = gms_tcp_get_available_bytes(c_state);
    }

    c_state->c_info.available_wait = false;
    c_state->c_info.available_timeout = 0;

    PG_RETURN_INT32(available);
}

Datum gms_tcp_flush(PG_FUNCTION_ARGS)
{
    GMS_TCP_CONNECTION *c = (GMS_TCP_CONNECTION *)PG_GETARG_POINTER(0);
    GMS_TCP_CONNECTION_STATE *c_state = gms_tcp_get_connection_state(c);
    GMS_TCP_CONNECTION_BUFFER *out_buffer = NULL;
    errno_t rc = EOK;

    gms_tcp_check_connection_null(c_state);

    out_buffer = &c_state->out_buffer;
    if (!out_buffer->data || !out_buffer->max_buffer_size) {
        PG_RETURN_VOID();
    }

    if (c_state->c_info.tx_timeout) {
        int ret = gms_tcp_set_connection_send_timeout(c_state, c_state->c_info.tx_timeout);
        if (ret == 0) {
            ereport(ERROR,
                    (errcode(ERRCODE_TRANSFER_TIMEOUT),
                        errmsg("recv data timeout.")));
        } else if (ret < 0) {
            ereport(ERROR,
                    (errcode(ERRCODE_NETWORK_ERROR),
                        errmsg("recv data error.")));
        }
    }
    send(c_state->fd, out_buffer->data, out_buffer->end, MSG_DONTWAIT);

    rc = memset_s(out_buffer->data, GMS_TCP_MAX_OUT_BUFFER_SIZE, 0, out_buffer->max_buffer_size);
    securec_check_c(rc, "\0", "\0");
    out_buffer->start = 0;
    out_buffer->end = 0;
    out_buffer->free_space = out_buffer->max_buffer_size;

    PG_RETURN_VOID();
}

Datum
gms_tcp_close_all_connections(PG_FUNCTION_ARGS)
{
    ListCell* lc = NULL;

    if (!GMS_TCP_FD_BAK_LIST) {
        PG_RETURN_VOID();
    }

    foreach (lc, GMS_TCP_FD_BAK_LIST) {
        PortalSpecialData *node = (PortalSpecialData *)lfirst(lc);
        node->cleanup(node->data);
        pfree(node);
    }

    list_free_ext(GMS_TCP_FD_BAK_LIST);

    PG_RETURN_VOID();
}

Datum
gms_tcp_close_connection(PG_FUNCTION_ARGS)
{
    GMS_TCP_CONNECTION *c = (GMS_TCP_CONNECTION *)PG_GETARG_POINTER(0);
    GMS_TCP_CONNECTION_STATE *c_state = gms_tcp_get_connection_state(c);

    gms_tcp_check_connection_null(c_state);

    gms_tcp_close_connection_by_state(c_state);

    PG_RETURN_VOID();
}

Datum
gms_tcp_get_line_real(PG_FUNCTION_ARGS)
{
    GMS_TCP_CONNECTION *c = (GMS_TCP_CONNECTION *)PG_GETARG_POINTER(0);
    bool remove_crlf = PG_GETARG_BOOL(1);
    bool peek = PG_GETARG_BOOL(2);
    bool ch_charset = PG_GETARG_BOOL(3);
    GMS_TCP_CONNECTION_STATE *c_state = gms_tcp_get_connection_state(c);
    GMS_TCP_CONNECTION_DATA_OPT data_opt = {0};

    gms_tcp_check_connection_null(c_state);

    data_opt.data = (char *)palloc0(GMS_TCP_MAX_IN_BUFFER_SIZE);
    data_opt.remove_crlf = remove_crlf;
    data_opt.peek = peek;
    data_opt.report_err = true;

    c_state->c_info.get_data_wait = true;
    gms_tcp_get_data(c_state, &data_opt);
    c_state->c_info.get_data_wait = false;

    if (ch_charset && c_state->c_info.charset) {
        char *data = gms_tcp_encode_data_by_charset(c_state, &data_opt, false);
        if (data) {
            pfree(data_opt.data);
            PG_RETURN_TEXT_P(cstring_to_text(data));
        }
    }

    PG_RETURN_TEXT_P(cstring_to_text(data_opt.data));
}

Datum
gms_tcp_get_raw_real(PG_FUNCTION_ARGS)
{
    GMS_TCP_CONNECTION *c = (GMS_TCP_CONNECTION *)PG_GETARG_POINTER(0);
    int32 len = PG_GETARG_INT32(1);
    bool peek = PG_GETARG_BOOL(2);
    GMS_TCP_CONNECTION_STATE *c_state = gms_tcp_get_connection_state(c);
    GMS_TCP_CONNECTION_DATA_OPT data_opt = {0};
    StringInfoData buf;

    gms_tcp_check_connection_null(c_state);

    data_opt.data = (char *)palloc0(GMS_TCP_MAX_IN_BUFFER_SIZE);
    data_opt.len = len;
    data_opt.peek = peek;
    data_opt.report_err = true;

    c_state->c_info.get_data_wait = true;
    gms_tcp_get_data(c_state, &data_opt);
    c_state->c_info.get_data_wait = false;

    pq_begintypsend(&buf);
    pq_sendbytes(&buf, data_opt.data, data_opt.data_len);
    PG_RETURN_BYTEA_P(pq_endtypsend(&buf));
}

Datum
gms_tcp_get_text_real(PG_FUNCTION_ARGS)
{
    GMS_TCP_CONNECTION *c = (GMS_TCP_CONNECTION *)PG_GETARG_POINTER(0);
    int32 len = PG_GETARG_INT32(1);
    bool peek = PG_GETARG_BOOL(2);
    bool ch_charset = PG_GETARG_BOOL(3);
    GMS_TCP_CONNECTION_STATE *c_state = gms_tcp_get_connection_state(c);
    GMS_TCP_CONNECTION_DATA_OPT data_opt = {0};

    gms_tcp_check_connection_null(c_state);

    data_opt.data = (char *)palloc0(GMS_TCP_MAX_IN_BUFFER_SIZE);
    data_opt.len = len;
    data_opt.peek = peek;
    data_opt.ch_charset = ch_charset;
    data_opt.report_err = true;

    c_state->c_info.get_data_wait = true;
    gms_tcp_get_data(c_state, &data_opt);
    c_state->c_info.get_data_wait = false;

    if (ch_charset && c_state->c_info.charset) {
        char *data = gms_tcp_encode_data_by_charset(c_state, &data_opt, false);
        if (data) {
            pfree(data_opt.data);
            PG_RETURN_TEXT_P(cstring_to_text(data));
        }
    }

    PG_RETURN_TEXT_P(cstring_to_text(data_opt.data));
}

Datum
gms_tcp_open_connection(PG_FUNCTION_ARGS)
{
    text *remote_host_t = PG_GETARG_TEXT_P(0);
    char *remote_host = NULL;
    struct sockaddr_in saddr;
    int remote_port = PG_GETARG_INT32(1);
    text *local_host_t = PG_GETARG_TEXT_P(2);
    char *local_host = NULL;
    int local_port = PG_GETARG_INT32(3);
    int in_buffer_size = PG_GETARG_INT32(4);
    int out_buffer_size = PG_GETARG_INT32(5);
    text *charset_t = PG_GETARG_TEXT_P(6);
    char *charset = NULL;
    text *newline_t = PG_GETARG_TEXT_P(7);
    char *newline = NULL;
    int tx_timeout = PG_GETARG_INT32(8);
    GMS_TCP_CONNECTION_HEAD *c_h = NULL;
    GMS_TCP_CONNECTION *c = NULL;
    GMS_TCP_CONNECTION_STATE *c_state = NULL;
    int offset = 0;

    c = (GMS_TCP_CONNECTION *)palloc0(sizeof(GMS_TCP_CONNECTION));
    c_h = &c->c_h;

    if (ORAFCE_COMM_STRING_TOO_LONG == gms_tcp_comm_get_str(remote_host_t, &remote_host, GMS_TCP_MAX_HOST_LEN, &c_h->remote_host_len, &c_h->total_len)) {
        pfree(c);
        ereport(ERROR,
                (errcode(ERRCODE_BAD_ARGUMENT),
                    errmsg("input remote host too long, max length is %d.", GMS_TCP_MAX_HOST_LEN)));
    }
    if (!remote_host) {
        pfree(c);
        ereport(ERROR,
                (errcode(ERRCODE_BAD_ARGUMENT),
                    errmsg("input error remote host.")));
    }
    if (!gms_tcp_change_remote_host(remote_host, &saddr)) {
        pfree(c);
        ereport(ERROR,
                (errcode(ERRCODE_BAD_ARGUMENT),
                    errmsg("input remote host error.")));
    }

    if (remote_port) {
        c_h->remote_port_len = sizeof(c_h->remote_port_len);
        c_h->total_len += c_h->remote_port_len;
    } else {
        pfree(c);
        ereport(ERROR,
                (errcode(ERRCODE_BAD_ARGUMENT),
                    errmsg("input remote port error.")));
    }

    if (ORAFCE_COMM_STRING_TOO_LONG == gms_tcp_comm_get_str(local_host_t, &local_host, GMS_TCP_MAX_HOST_LEN, &c_h->local_host_len, &c_h->total_len)) {
        pfree(c);
        ereport(ERROR,
                (errcode(ERRCODE_BAD_ARGUMENT),
                    errmsg("input local host too long, max length is %d.", GMS_TCP_MAX_HOST_LEN)));
    }

    if (local_port) {
        c_h->local_port_len = sizeof(c_h->local_port_len);
        c_h->total_len += c_h->local_port_len;
    }

    if (in_buffer_size > GMS_TCP_MAX_IN_BUFFER_SIZE) {
        pfree(c);
        ereport(ERROR,
                (errcode(ERRCODE_BAD_ARGUMENT),
                    errmsg("in buffer size must be limited in %d.", GMS_TCP_MAX_IN_BUFFER_SIZE)));
    } else if (in_buffer_size) {
        c_h->in_buffer_size_len = sizeof(c_h->in_buffer_size_len);
        c_h->total_len += c_h->in_buffer_size_len;
    }

    if (out_buffer_size > GMS_TCP_MAX_OUT_BUFFER_SIZE) {
        pfree(c);
        ereport(ERROR,
                (errcode(ERRCODE_BAD_ARGUMENT),
                    errmsg("in buffer size must be limited in %d.", GMS_TCP_MAX_OUT_BUFFER_SIZE)));
    } else if (out_buffer_size) {
        c_h->out_buffer_size_len = sizeof(c_h->out_buffer_size_len);
        c_h->total_len += c_h->out_buffer_size_len;
    }

    if (ORAFCE_COMM_STRING_TOO_LONG == gms_tcp_comm_get_str(charset_t, &charset, GMS_TCP_MAX_CHARSET_LEN, &c_h->charset_len, &c_h->total_len)) {
        pfree(c);
        ereport(ERROR,
                (errcode(ERRCODE_BAD_ARGUMENT),
                    errmsg("input charset too long, max length is %d.", GMS_TCP_MAX_CHARSET_LEN)));
    }

    if (charset && !gms_tcp_check_charset(charset)) {
        pfree(c);
        ereport(ERROR,
                (errcode(ERRCODE_BAD_ARGUMENT),
                    errmsg("error charset: %s.", charset)));
    }

    if (ORAFCE_COMM_STRING_TOO_LONG == gms_tcp_comm_get_str(newline_t, &newline, 16, &c_h->newline_len, &c_h->total_len)) {
        pfree(c);
        ereport(ERROR,
                (errcode(ERRCODE_BAD_ARGUMENT),
                    errmsg("input newline too long, max length is %d.", GMS_TCP_MAX_NEWLINE_LEN)));
    }

    if (tx_timeout > GMS_TCP_MAX_TX_TIMEOUT || tx_timeout < 0) {
        pfree(c);
        ereport(ERROR,
                (errcode(ERRCODE_BAD_ARGUMENT),
                    errmsg("tx timeout must be limited in %d ~ %d.", 0, GMS_TCP_MAX_TX_TIMEOUT)));
    } else if (tx_timeout) {
        c_h->tx_timeout_len = sizeof(c_h->tx_timeout_len);
        c_h->total_len += c_h->tx_timeout_len;
    }


    Assert(c_h->total_len);

    gms_tcp_comm_memcopy_str(c->data + offset, c_h->remote_host_len, remote_host, &offset);
    gms_tcp_comm_memcopy_int(c->data + offset, c_h->remote_port_len, (char *)&remote_port, c_h->remote_port_len, &offset);
    gms_tcp_comm_memcopy_str(c->data + offset, c_h->local_host_len, local_host, &offset);
    gms_tcp_comm_memcopy_int(c->data + offset, c_h->local_port_len, (char *)&local_port, c_h->local_port_len, &offset);
    gms_tcp_comm_memcopy_int(c->data + offset, c_h->in_buffer_size_len, (char *)&in_buffer_size, c_h->in_buffer_size_len, &offset);
    gms_tcp_comm_memcopy_int(c->data + offset, c_h->out_buffer_size_len, (char *)&out_buffer_size, c_h->out_buffer_size_len, &offset);
    gms_tcp_comm_memcopy_str(c->data + offset, c_h->charset_len, charset, &offset);
    gms_tcp_comm_memcopy_str(c->data + offset, c_h->newline_len, newline, &offset);
    gms_tcp_comm_memcopy_int(c->data + offset, c_h->tx_timeout_len, (char *)&tx_timeout, c_h->tx_timeout_len, &offset);

    c_state = (GMS_TCP_CONNECTION_STATE *)gms_tcp_alloc(sizeof(GMS_TCP_CONNECTION_STATE));
    gms_tcp_get_connection_info(c, &c_state->c_info);
    c_state->c_info.saddr = saddr;
    gms_tcp_init_data_buffer(&c_state->in_buffer, in_buffer_size);
    gms_tcp_init_data_buffer(&c_state->out_buffer, out_buffer_size);
    if (gms_tcp_connect(c_state) != GMS_TCP_OK) {
        std::shared_ptr<void> data_cstate = std::shared_ptr<void>(c_state);
        gms_tcp_release_connection_state(data_cstate);
        pfree(c);
        ereport(ERROR,
                (errcode(ERRCODE_NETWORK_ERROR),
                    errmsg("connect to remote(remote host: %s, remote port: %d) fail.",
                            remote_host, remote_port)));
    }

    c_h->fd_len = sizeof(int);
    gms_tcp_comm_memcopy_int(c->data + offset, c_h->fd_len, (char *)&c_state->fd, c_h->fd_len, &offset);
    c_h->total_len += c_h->fd_len;

    c_state->c_info.fd = c_state->fd;

    gms_tcp_store_connection(c_state);

    PG_RETURN_POINTER(c);
}

Datum
gms_tcp_write_line(PG_FUNCTION_ARGS)
{
    GMS_TCP_CONNECTION *c = (GMS_TCP_CONNECTION *)PG_GETARG_POINTER(0);
    text *data_t = PG_GETARG_TEXT_P(1);
    char *data = text_to_cstring(data_t);
    GMS_TCP_CONNECTION_STATE *c_state = gms_tcp_get_connection_state(c);
    char *data_out = NULL;
    char *newline = NULL;
    int newline_len = 0;
    int32 len = 0;
    GMS_TCP_CONNECTION_DATA_OPT data_opt = {0};
    errno_t rc = EOK;

    gms_tcp_check_connection_null(c_state);

    data_out = (char *)palloc0(GMS_TCP_MAX_OUT_BUFFER_SIZE);

    Assert(c_state->c_info.newline);
    if (strcmp(c_state->c_info.newline, "CRLF") == 0) {
        newline_len = 2;
        newline = "\r\n";
    } else {
        newline_len = 1;
        newline = "\n";
    }

    rc = memcpy_s(data_out, GMS_TCP_MAX_OUT_BUFFER_SIZE, data, strlen(data));
    securec_check_c(rc, "\0", "\0");
    data_opt.data = data_out;
    data_opt.data_len = strlen(data_out);
    rc = memcpy_s(&data_out[data_opt.data_len], GMS_TCP_MAX_OUT_BUFFER_SIZE - data_opt.data_len, newline, newline_len);
    securec_check_c(rc, "\0", "\0");
    data_opt.data_len += newline_len;

    data_opt.ch_charset = true;
    data_opt.encrypt = true;

    len = gms_tcp_write_data(c_state, &data_opt);

    PG_RETURN_INT32(len);
}

Datum
gms_tcp_write_raw_real(PG_FUNCTION_ARGS)
{
    GMS_TCP_CONNECTION *c = (GMS_TCP_CONNECTION *)PG_GETARG_POINTER(0);
    bytea *wbuf = PG_GETARG_BYTEA_P(1);
    int32 write_len = PG_GETARG_INT32(2);
    GMS_TCP_CONNECTION_STATE *c_state = gms_tcp_get_connection_state(c);
    GMS_TCP_CONNECTION_DATA_OPT data_opt = {0};
    int32 len = 0;

    gms_tcp_check_connection_null(c_state);

    data_opt.data = VARDATA(wbuf);
    data_opt.data_len = VARSIZE(wbuf) - VARHDRSZ;
    data_opt.len = write_len;

    len = gms_tcp_write_data(c_state, &data_opt);

    PG_RETURN_INT32(len);
}

Datum
gms_tcp_write_text_real(PG_FUNCTION_ARGS)
{
    GMS_TCP_CONNECTION *c = (GMS_TCP_CONNECTION *)PG_GETARG_POINTER(0);
    text *data_t = PG_GETARG_TEXT_P(1);
    int32 write_len = PG_GETARG_INT32(2);
    char *data = text_to_cstring(data_t);
    GMS_TCP_CONNECTION_STATE *c_state = gms_tcp_get_connection_state(c);
    int32 len = 0;
    GMS_TCP_CONNECTION_DATA_OPT data_opt = {0};

    gms_tcp_check_connection_null(c_state);

    data_opt.data = data;
    data_opt.data_len = strlen(data);
    data_opt.len = write_len;
    data_opt.ch_charset = true;
    data_opt.encrypt = false;

    len = gms_tcp_write_data(c_state, &data_opt);

    PG_RETURN_INT32(len);
}


Datum
gms_tcp_connection_in(PG_FUNCTION_ARGS)
{
    PG_RETURN_NULL();
}

Datum
gms_tcp_connection_out(PG_FUNCTION_ARGS)
{
    GMS_TCP_CONNECTION *c = (GMS_TCP_CONNECTION *)PG_GETARG_POINTER(0);
    GMS_TCP_CONNECTION_INFO *c_info = NULL;
    StringInfoData str;

    if(!c) {
        PG_RETURN_NULL();
    }

    c_info = (GMS_TCP_CONNECTION_INFO *)gms_tcp_alloc(sizeof(GMS_TCP_CONNECTION_INFO));
    gms_tcp_get_connection_info(c, c_info);

    initStringInfo(&str);

    if (c_info->remote_host) {
        appendStringInfo(&str, "remote host:");
        appendStringInfo(&str, "%s", c_info->remote_host);
    }
    if (c_info->remote_port) {
        appendStringInfoChar(&str, ',');
        appendStringInfo(&str, "remote port:");
        appendStringInfo(&str, "%d", c_info->remote_port);
    }
    if (c_info->local_host) {
        appendStringInfo(&str, "local host:");
        appendStringInfo(&str, "%s", c_info->local_host);
    }
    if (c_info->local_port) {
        appendStringInfoChar(&str, ',');
        appendStringInfo(&str, "local port:");
        appendStringInfo(&str, "%d", c_info->local_port);
    }
    if (c_info->in_buffer_size) {
        appendStringInfoChar(&str, ',');
        appendStringInfo(&str, "in buffer size:");
        appendStringInfo(&str, "%d", c_info->in_buffer_size);
    }
    if (c_info->out_buffer_size) {
        appendStringInfoChar(&str, ',');
        appendStringInfo(&str, "out buffer size:");
        appendStringInfo(&str, "%d", c_info->out_buffer_size);
    }
    if (c_info->charset) {
        appendStringInfoChar(&str, ',');
        appendStringInfo(&str, "charset:");
        appendStringInfo(&str, "%s", c_info->charset);
    }
    if (c_info->newline && (strcmp(c_info->newline, "CRLF") != 0)) {
        appendStringInfoChar(&str, ',');
        appendStringInfo(&str, "newline:");
        appendStringInfo(&str, "%s", c_info->newline);
    }
    if (c_info->tx_timeout && (c_info->tx_timeout != GMS_TCP_MAX_TX_TIMEOUT)) {
        appendStringInfoChar(&str, ',');
        appendStringInfo(&str, "tx timeout:");
        appendStringInfo(&str, "%d", c_info->tx_timeout);
    }

    gms_tcp_release_connection_info(c_info);
    pfree(c_info);

    PG_RETURN_CSTRING(str.data);
}

static void gms_tcp_comm_memcopy_str(char *dst, int dst_len, char *src, int *offset)
{
    errno_t rc = EOK;

    if (src) {
        int len = 0;
        int src_len = strlen(src);
        rc = memcpy_s(dst, dst_len, src, src_len);
        securec_check_c(rc, "\0", "\0");
        len += src_len;
        dst[src_len] = '\0';
        len++;

        if (offset) {
            *offset += len;
        }
    }
}

static void gms_tcp_comm_memcopy_int(char *dst, int dst_len, char *src, int src_len, int *offset)
{
    errno_t rc = EOK;

    if (src_len) {
        rc = memcpy_s(dst, dst_len, src, src_len);
        securec_check_c(rc, "\0", "\0");
        if (offset) {
            *offset += src_len;
        }
    }
}

static int gms_tcp_comm_get_str(text *data_t, char **data, int data_len_max, int *get_data_len, int *total_len)
{
    if (data_t) {
        char *p = text_to_cstring(data_t);
        int data_len = strlen(p) + 1;
        if (data_len >= data_len_max) {
            return ORAFCE_COMM_STRING_TOO_LONG;
        }
        if (!(strlen(p) == 1 && p[0] == '0')) {
            *get_data_len = data_len;
            *total_len += data_len;
            *data = p;
        }
    }

    return 0;
}

static bool gms_tcp_comm_cmp_str(const char *str_1, const char *str_2)
{
    if ((str_1 && str_2) && (strlen(str_1) == strlen(str_2)) && (memcmp(str_1, str_2, strlen(str_1)) == 0)) {
        return true;
    } else if (!str_1 && !str_2) {
        return true;
    } else {
        return false;
    }
}
