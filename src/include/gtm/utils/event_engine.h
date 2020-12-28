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
 * event_engine.h
 *        event engine header file
 *
 * IDENTIFICATION
 *    src/include/gtm/utils/event_engine.h
 *
 * -------------------------------------------------------------------------
 */
 
#ifndef EVENT_ENGINE_H
#define EVENT_ENGINE_H

#include <sys/time.h>
#include "c.h"

/* wait for a socket or FD to become readable */
#define EV_READ     0x02

/* wait for a socket or FD to become writeable */
#define EV_WRITE    0x04

/* persistent event */
#define EV_PERSIST  0x10

/* select edge-triggered behavior, if supported by the backend */
#define EV_ET       0x20

/* connection close event */
#define EV_CLOSED   0x80

typedef struct event_config event_config_t;
typedef struct epoll_info epoll_info_t;
typedef struct event event_t;
typedef struct event_iomap event_iomap_t;
typedef struct event_base event_base_t;
typedef struct event_callback event_callback_t;

struct event_config {
    int max_dispatch_callbacks;
};

struct epoll_info {
    int epfd;
    int nevents;
    struct epoll_event *epevs;
};

struct event {
    event_base_t *ev_base;
    int ev_fd;
    int ev_state;
    unsigned short ev_events;
    unsigned short ev_res; /* result passed to event callback */
    void (*ev_cb)(int, unsigned short, void *);
	void *ev_cb_arg;
};

struct event_iomap {
    event_t **entries;
    int nentries;
};

struct event_base {
    epoll_info_t *epi;
    event_iomap_t iomap;
    /* number of total events added to this event_base */
    int event_count;
    /* number of total active events in this event_base */
	int event_count_active;
    /* set if we should terminate the loop immediately */
    bool event_break;
    bool event_exit;
    bool interruptable;
    bool loop_one_shot;
    /* set to prevent reentrant invocation */
    bool running_loop;
    struct timeval timeout;
    int max_dispatch_callbacks;
};

extern event_config_t *event_config_new();
extern void event_config_free(event_config_t *cfg);
extern event_base_t *event_base_new(const event_config_t *cfg);
extern int event_assign(event_t *ev, event_base_t *base, int fd, unsigned short events,
    void (*callback)(int, unsigned short, void *), void *arg);
extern int event_add(event_t *ev, const struct timeval *tv);
extern int event_del(event_t *ev);
extern int event_base_dispatch(event_base_t *event_base);
extern int event_base_loop(event_base_t *base, int flags);
extern int event_base_loopbreak(event_base_t *event_base);
extern int event_base_loopexit(event_base_t *event_base);
extern void event_base_free(event_base_t *base);
extern int event_get_max();
extern bool event_exceed_limit(const event_base_t *base);
extern int event_base_retrieve_fds(event_base_t *base, int *fds, int len);

#endif
