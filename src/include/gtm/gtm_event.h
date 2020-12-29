/*
 * Copyright (c) 2020 Huawei Technologies Co.,Ltd.
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
 * gtm_event.h
 *    GTM event header file
 *
 * IDENTIFICATION
 *    src/include/gtm/gtm_event.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef GTM_EVENT_H
#define GTM_EVENT_H

#include "gtm/utils/event_engine.h"

/*
 * states of a connection
 */
enum gs_gtm_conn_states {
    GS_GTM_CONN_LISTENING,
    GS_GTM_CONN_ACCEPTED,
    GS_GTM_CONN_WAIT_DATA_RECEIVE,
    GS_GTM_CONN_PROCESS,
    GS_GTM_CONN_CLOSE,
    GS_GTM_CONN_MAX_STATE
};

const char* const gs_gtm_state_name[] = {
    "GS_GTM_CONN_LISTENING",
    "GS_GTM_CONN_ACCEPTED",
    "GS_GTM_CONN_WAIT_DATA_RECEIVE",
    "GS_GTM_CONN_PROCESS",
    "GS_GTM_CONN_CLOSE",
    "GS_GTM_CONN_MAX_STATE"
};

enum gs_gtm_conn_substates {
    GS_GTM_CONN_PROCESS_BEGIN,
    GS_GTM_CONN_PROCESS_PROGRESSING,
    GS_GTM_CONN_PROCESS_END
};

typedef struct gs_gtm_conn gs_gtm_conn_t;
typedef struct gs_gtm_conn_queue gs_gtm_cq_t;
typedef struct gs_gtm_conn_queue_item gs_gtm_cqi_t;
typedef struct gs_gtm_event_thread gs_gtm_event_thread_t;

struct gs_gtm_conn_queue_item {
    int sfd;
    SockAddr sa;
    enum gs_gtm_conn_states init_state;
    unsigned short event_flags;
    struct gs_gtm_conn_queue_item *next;
};

/* A connection queue. */
struct gs_gtm_conn_queue {
    gs_gtm_cqi_t *head;
    gs_gtm_cqi_t *tail;
    pthread_mutex_t lock;
};

struct gs_gtm_event_thread {
    pthread_t thread_id;         /* unique ID of this thread */
    int tid;                     /* thread index in the global thread array */
    struct event_base *base;     /* event engine handle this thread uses */
    struct event notify_event;   /* listen event for notify pipe */
    int notify_receive_fd;       /* receiving end of notify pipe */
    int notify_send_fd;          /* sending end of notify pipe */
    gs_gtm_cq_t *new_conn_queue; /* queue of new connections to handle */
    GTM_ThreadInfo *thrinfo;
    pg_atomic_int32 status;
};

struct gs_gtm_conn {
    int sfd;
    pg_atomic_int32 refcount;
    GTM_ConnectionInfo *gtm_conninfo;
    enum gs_gtm_conn_states state;
    enum gs_gtm_conn_substates conn_process_substate;
    struct event event;
    unsigned short ev_flags;
    unsigned short which;
    bool is_switchover_processing;
    gs_gtm_event_thread_t *thread;
    struct gs_gtm_conn *next;
};

extern int gs_gtm_max_conns_per_event;
extern int gtm_num_threads;
extern int gs_gtm_max_conns;
extern gs_gtm_conn_t **gs_gtm_conns;
extern pthread_t gs_gtm_start_workers_requester;

extern int gs_gtm_conn_pool_init();
extern void gs_gtm_thread_pool_init(int nthreads);
extern int gs_gtm_listen_conns_init(const int listen_sockets[], int nsockets, struct event_base *main_base);
extern void gs_gtm_listen_conns_refresh(const int listen_sockets[], int nsockets, struct event_base *main_base);
extern void gs_gtm_start_workers();
extern void gs_gtm_quit_workers();
extern void gs_gtm_signal_start_workers();
extern void gs_gtm_check_restart_worker();
extern void gs_gtm_set_worker_exit(gs_gtm_event_thread_t *thread);
extern void gs_gtm_conn_closesocket(int sfd);

#endif