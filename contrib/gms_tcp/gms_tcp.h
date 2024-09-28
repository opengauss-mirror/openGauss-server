#ifndef __GMS_TCP__
#define __GMS_TCP__

#include <sys/ioctl.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <iconv.h>

#include "postgres.h"
#include "knl/knl_thread.h"
#include "utils/builtins.h"
#include "utils/portal.h"
#include "utils/palloc.h"
#include "libpq/pqformat.h"
#include "utils/elog.h"

#define GMS_TCP_MAX_HOST_LEN 255
#define GMS_TCP_MAX_CHARSET_LEN 32
#define GMS_TCP_MAX_NEWLINE_LEN 16
#define GMS_TCP_MAX_IN_BUFFER_SIZE 32767
#define GMS_TCP_MAX_OUT_BUFFER_SIZE 32767
#define GMS_TCP_MAX_CONNECTION 32
#define GMS_TCP_MAX_PASSWORD_LEN 32
#define GMS_TCP_MAX_PATH_LEN 255
#define GMS_TCP_MAX_TX_TIMEOUT 2147483647

#define GMS_TCP_FD_BAK_LIST *u_sess->exec_cxt.portal_data_list

typedef enum {
    GMS_TCP_CONNECT_OK = 0,
    GMS_TCP_CONNECT_FAIL
} GMS_TCP_CONNECT_STATE;

typedef enum {
    GMS_TCP_OK = 0,
    GMS_TCP_STRING_TOO_LONG
} GMS_TCP_CONNECT_ERROR;

typedef struct {
    int32 total_len;
    int32 remote_host_len;
    int32 remote_port_len;
    int32 local_host_len;
    int32 local_port_len;
    int32 in_buffer_size_len;
    int32 out_buffer_size_len;
    int32 charset_len;
    int32 newline_len;
    int32 tx_timeout_len;
    int32 fd_len;
} GMS_TCP_CONNECTION_HEAD;

#define GMS_TCP_MAX_TYPE_LEN 512
#define GMS_TCP_MAX_TYPE_DATA_LEN (GMS_TCP_MAX_TYPE_LEN - sizeof(GMS_TCP_CONNECTION_HEAD))
typedef struct {
    GMS_TCP_CONNECTION_HEAD c_h;
    char data[GMS_TCP_MAX_TYPE_DATA_LEN];
} GMS_TCP_CONNECTION;

typedef struct {
    char  *remote_host;
    struct sockaddr_in saddr;
    int32 remote_port;
    char  *local_host;
    int32 local_port;
    int32 in_buffer_size;
    int32 out_buffer_size;
    char  *charset;
    char  *newline;
    bool get_data_wait;
    int32 tx_timeout;
    bool available_wait;
    int32 available_timeout;
    int fd;
} GMS_TCP_CONNECTION_INFO;

typedef struct {
    int32 max_buffer_size;
    int32 free_space;
    int32 start;
    int32 end;
    char *data;
} GMS_TCP_CONNECTION_BUFFER;

typedef struct {
    int fd;
    bool secure;
    GMS_TCP_CONNECT_STATE state;
    GMS_TCP_CONNECTION_BUFFER in_buffer;
    GMS_TCP_CONNECTION_BUFFER out_buffer;
    GMS_TCP_CONNECTION_INFO c_info;
} GMS_TCP_CONNECTION_STATE;

typedef struct {
    bool remove_crlf;
    bool peek;
    bool report_err;
    bool ch_charset;
    bool encrypt;
    int32 len;
    char *data;
    int32 data_len;
} GMS_TCP_CONNECTION_DATA_OPT;

typedef enum {
    ORAFCE_COMM_OK = 0,
    ORAFCE_COMM_STRING_TOO_LONG
} ORAFCE_COMM_ERROR;

extern void gms_tcp_release_connection_info(GMS_TCP_CONNECTION_INFO *c_info);
extern void gms_tcp_get_connection_info(GMS_TCP_CONNECTION *c, GMS_TCP_CONNECTION_INFO *c_info);
extern GMS_TCP_CONNECTION_STATE *gms_tcp_get_connection_state_by_fd(int fd);
extern void gms_tcp_get_connection_head_by_info(GMS_TCP_CONNECTION_INFO *c_info, GMS_TCP_CONNECTION_HEAD *c_h);

/*
 * External declarations
 */
extern "C" Datum gms_tcp_crlf(PG_FUNCTION_ARGS);
extern "C" Datum gms_tcp_available_real(PG_FUNCTION_ARGS);
extern "C" Datum gms_tcp_close_all_connections(PG_FUNCTION_ARGS);
extern "C" Datum gms_tcp_close_connection(PG_FUNCTION_ARGS);
extern "C" Datum gms_tcp_flush(PG_FUNCTION_ARGS);
extern "C" Datum gms_tcp_get_line_real(PG_FUNCTION_ARGS);
extern "C" Datum gms_tcp_get_raw_real(PG_FUNCTION_ARGS);
extern "C" Datum gms_tcp_get_text_real(PG_FUNCTION_ARGS);
extern "C" Datum gms_tcp_open_connection(PG_FUNCTION_ARGS);
extern "C" Datum gms_tcp_secure_connection(PG_FUNCTION_ARGS);
extern "C" Datum gms_tcp_no_secure_connection(PG_FUNCTION_ARGS);
extern "C" Datum gms_tcp_write_line(PG_FUNCTION_ARGS);
extern "C" Datum gms_tcp_write_raw_real(PG_FUNCTION_ARGS);
extern "C" Datum gms_tcp_write_text_real(PG_FUNCTION_ARGS);
extern "C" Datum gms_tcp_connection_in(PG_FUNCTION_ARGS);
extern "C" Datum gms_tcp_connection_out(PG_FUNCTION_ARGS);
#endif