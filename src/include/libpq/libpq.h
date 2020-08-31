/* -------------------------------------------------------------------------
 *
 * libpq.h
 *    POSTGRES LIBPQ buffer structure definitions.
 *
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/libpq/libpq.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef LIBPQ_H
#define LIBPQ_H

#include <sys/types.h>
#include <netinet/in.h>

#include "lib/stringinfo.h"
#include "libpq/libpq-be.h"

/* ----------------
 * PQArgBlock
 *      Information (pointer to array of this structure) required
 *      for the PQfn() call.  (This probably ought to go somewhere else...)
 * ----------------
 */
typedef struct {
    int len;
    int isint;
    union {
        int *ptr; /* can't use void (dec compiler barfs)     */
        int integer;
    } u;
} PQArgBlock;

struct PQcommMethods{
    void        (*comm_reset) (void);
    int         (*flush) (void);
    int         (*flush_if_writable) (void);
    bool        (*is_send_pending) (void);
    int         (*putmessage) (char msgtype, const char* s, size_t len);
    int     (*putmessage_noblock) (char msgtype, const char* s, size_t len);
    void        (*startcopyout) (void);
    void        (*endcopyout) (bool errorAbort);
};

#define pq_comm_reset() (t_thrd.msqueue_cxt.PqCommMethods->comm_reset())
#define pq_flush() (t_thrd.msqueue_cxt.PqCommMethods->flush())
#define pq_flush_if_writable() (t_thrd.msqueue_cxt.PqCommMethods->flush_if_writable())
#define pq_is_send_pending() (t_thrd.msqueue_cxt.PqCommMethods->is_send_pending())
#define pq_putmessage(msgtype, s, len) \
    (t_thrd.msqueue_cxt.PqCommMethods->putmessage(msgtype, s, len))
#define pq_putmessage_noblock(msgtype, s, len) \
    (t_thrd.msqueue_cxt.PqCommMethods->putmessage_noblock(msgtype, s, len))
#define pq_startcopyout() (t_thrd.msqueue_cxt.PqCommMethods->startcopyout())
#define pq_endcopyout(errorAbort) (t_thrd.msqueue_cxt.PqCommMethods->endcopyout(errorAbort))

/*
 * External functions.
 */

/*
 * prototypes for functions in pqcomm.c
 */
extern int StreamServerPort(int family, char* hostName, unsigned short portNumber, const char* unixSocketName,
    pgsocket ListenSocket[], pgsocket SctpListenSocket[], int MaxListen, bool add_localaddr_flag,
    bool is_create_psql_sock, bool is_create_libcomm_sock);
extern int StreamConnection(pgsocket server_fd, Port* port);
extern void StreamClose(pgsocket sock);
extern void TouchSocketFile(void);
extern void pq_init(void);
extern int pq_getbytes(char* s, size_t len);
extern int pq_getstring(StringInfo s);
extern int pq_getmessage(StringInfo s, int maxlen);
extern int pq_getbyte(void);
extern int pq_peekbyte(void);
extern int pq_getbyte_if_available(unsigned char* c);
extern int pq_putbytes(const char* s, size_t len);
extern void pq_flush_timedwait(int timeout);
extern bool pq_select(int timeout_ms);
extern void pq_abandon_sendbuffer(void);
extern void pq_abandon_recvbuffer(void);
extern void pq_resize_recvbuffer(int size);
extern void pq_revert_recvbuffer(const char* data, int len);
/*
 * prototypes for functions in be-secure.c
 */
extern const char* ssl_cipher_file;
extern const char* ssl_rand_file;

extern int secure_initialize(void);
extern bool secure_loaded_verify_locations(void);
extern void secure_destroy(void);
extern int secure_open_server(Port* port);
extern void secure_close(Port* port);
extern ssize_t secure_read(Port* port, void* ptr, size_t len);
extern ssize_t secure_write(Port* port, void* ptr, size_t len);

/*
 * interface for flushing sendbuffer to disk
 */

typedef enum TempFileState {
    TEMPFILE_DEFAULT,
    TEMPFILE_CREATED,
    TEMPFILE_FLUSHED,
    TEMPFILE_ON_SENDING,
    TEMPFILE_SENDED,
    TEMPFILE_CLOSED,
    TEMPFILE_ERROR_CLOSE,
    TEMPFILE_ERROR_SEND,
} TempFileState;

extern void pq_disk_reset_tempfile_contextinfo(void);
extern void pq_disk_discard_temp_file(void);
extern bool pq_disk_is_flushed(void);
extern int pq_disk_send_to_frontend(void);
extern void pq_disk_extract_sendbuffer(void);
extern void pq_disk_enable_temp_file(void);
extern void pq_disk_disable_temp_file(void);
extern bool pq_disk_is_temp_file_enabled(void);
extern bool pq_disk_is_temp_file_created(void);

#endif /* LIBPQ_H */
