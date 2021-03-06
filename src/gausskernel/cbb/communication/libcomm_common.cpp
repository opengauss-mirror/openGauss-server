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
 * libcomm_common.cpp
 *
 * IDENTIFICATION
 *    src/gausskernel/cbb/communication/libcomm_common.cpp
 *
 * -------------------------------------------------------------------------
 */
#include <arpa/inet.h>
#include <ctype.h>
#include <errno.h>
#include <fcntl.h>
#include <libcgroup.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <netdb.h>
#include <net/if.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/wait.h>
#include <sys/param.h>
#include <sys/time.h>
#include <unistd.h>

#include "libcomm_common.h"

int binary_semaphore::init() {
    atomic_set(&b_flag, 0);
    atomic_set(&waiting_count, 0);
    atomic_set(&b_destroy, 0);
    atomic_set(&destroy_wait, 0);
    int err = pthread_cond_init(&cond, NULL);
    if (err != 0)
        return err;
    err = pthread_mutex_init(&mutex, NULL);
    if (err != 0) {
        LIBCOMM_PTHREAD_COND_DESTORY(&cond);
        return err;
    }
    return err;
}

int binary_semaphore::destroy(bool do_destroy) {
        const int ret = 0;
        atomic_set(&b_destroy, 1);
        while (destroy_wait != 0) {
            post();
            usleep(100);
        }
        if (do_destroy) {
            LIBCOMM_PTHREAD_COND_DESTORY(&cond);
            LIBCOMM_PTHREAD_MUTEX_DESTORY(&mutex);
        }
        return ret;
}

void binary_semaphore::reset() {
    atomic_set(&b_flag, 0);
    while (destroy_wait != 0) {
        post();
        usleep(100);
    }
}

 void binary_semaphore::post() {
    LIBCOMM_PTHREAD_MUTEX_LOCK(&mutex);
    /* thread will poll up when someone has posted before */
    atomic_set(&b_flag, 1);
    if (waiting_count > 0) {
        LIBCOMM_PTHREAD_COND_SIGNAL(&cond);
    }
    LIBCOMM_PTHREAD_MUTEX_UNLOCK(&mutex);
}

void binary_semaphore::post_all() {
    LIBCOMM_PTHREAD_MUTEX_LOCK(&mutex);
    atomic_set(&b_flag, 1);
    if (waiting_count > 0) {
        LIBCOMM_PTHREAD_COND_BROADCAST(&cond);
    }
    LIBCOMM_PTHREAD_MUTEX_UNLOCK(&mutex);
}

int binary_semaphore::wait() {
    if (b_flag) {
        /* reset b_flag is no one is waitting */
        if (waiting_count == 0)
            atomic_set(&b_flag, 0);
        return 0;
    }

    int ret = 0;
    LIBCOMM_PTHREAD_MUTEX_LOCK(&mutex);
    while (!b_flag) {
        atomic_add(&waiting_count, 1);
        ret = pthread_cond_wait(&cond, &mutex);
        atomic_sub(&waiting_count, 1);
    }

    if (b_destroy)
        ret = -1;
    /* reset b_flag is no one is waitting */
    if (waiting_count == 0)
        atomic_set(&b_flag, 0);
    LIBCOMM_PTHREAD_MUTEX_UNLOCK(&mutex);

    return ret;
}

void binary_semaphore::destroy_wait_add() {
    atomic_add(&destroy_wait, 1);
}

void binary_semaphore::destroy_wait_sub() {
    atomic_sub(&destroy_wait, 1);
}

/** The parameter timeout should be in second, if it is minus or zero, the function
  * is the same as _wait.
  */
int binary_semaphore::timed_wait(int timeout) {
    int ret = -1;

    if (timeout <= 0) {
        ret = wait();
        return ret;
    }

    if (b_flag) {
        /* reset b_flag is no one is waitting */
        if (waiting_count == 0)
            atomic_set(&b_flag, 0);
        return 0;
    }

    LIBCOMM_PTHREAD_MUTEX_LOCK(&mutex);
    if (b_flag) {
        atomic_set(&b_flag, 0);
        LIBCOMM_PTHREAD_MUTEX_UNLOCK(&mutex);
        ret = 0;
        return ret;
    }
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    ts.tv_sec += timeout;
    ts.tv_nsec = 0;
    atomic_add(&waiting_count, 1);

    ret = pthread_cond_timedwait(&cond, &mutex, &ts);

    atomic_sub(&waiting_count, 1);
    if (b_flag) {
        /* reset b_flag is no one is waitting */
        if (waiting_count == 0)
            atomic_set(&b_flag, 0);
        ret = 0;
    }

    if (b_destroy)
        ret = -1;
    LIBCOMM_PTHREAD_MUTEX_UNLOCK(&mutex);

    return ret;
}

int hash_entry::_init() {
    return sem.init();
}

void hash_entry::_destroy() {
    sem.destroy(true);
}

void hash_entry::_signal() {
    sem.post();
}

void hash_entry::_signal_all() {
    sem.post_all();
}

void hash_entry::_wait() {
    sem.wait();
}

void hash_entry::_hold_destroy() {
    sem.destroy_wait_add();
}

void hash_entry::_release_destroy() {
    sem.destroy_wait_sub();
}

int hash_entry::_timewait(int timeout) {
    return sem.timed_wait(timeout);
}

void node_sock::reset_all() {
    ctrl_tcp_sock = -1;
    ctrl_tcp_port = -1;
    ctrl_tcp_sock_id = 0;
    libcomm_reply_sock = -1;
    libcomm_reply_sock_id = -1;
    errno_t ss_rc = 0;
    ss_rc = memset_s(remote_host, HOST_ADDRSTRLEN, 0x0, HOST_ADDRSTRLEN);
    securec_check(ss_rc, "\0", "\0");
    ss_rc = memset_s(remote_nodename, NAMEDATALEN, 0x0, NAMEDATALEN);
    securec_check(ss_rc, "\0", "\0");
    ss_rc = memset_s(&to_ss, sizeof(struct sockaddr_storage), 0x0, sizeof(struct sockaddr_storage));
    securec_check(ss_rc, "\0", "\0");
}

void node_sock::init() {
    reset_all();
    LIBCOMM_PTHREAD_MUTEX_INIT(&_slock, 0);
    LIBCOMM_PTHREAD_MUTEX_INIT(&_tlock, 0);
}

void node_sock::clear() {
    reset_all();
}

void node_sock::destroy() {
    clear();
    LIBCOMM_PTHREAD_MUTEX_DESTORY(&_slock);
    LIBCOMM_PTHREAD_MUTEX_DESTORY(&_tlock);
}

void node_sock::lock() {
    LIBCOMM_PTHREAD_MUTEX_LOCK(&_tlock);
}

void node_sock::unlock() {
    LIBCOMM_PTHREAD_MUTEX_UNLOCK(&_tlock);
}

void node_sock::close_socket(int flag) {
    lock();
    close_socket_nl(flag);
    unlock();
}

void node_sock::close_socket_nl(int flag) { // close without lock
    switch (flag) {
        case CTRL_TCP_SOCK:
            if (ctrl_tcp_sock >= 0) {
                close(ctrl_tcp_sock);
                ctrl_tcp_sock = -1;
                ctrl_tcp_sock_id = -1;
            }
            break;
        default:
            break;
    }
}

void node_sock::set(int val, int flag) {
    lock();
    set_nl(val, flag);
    unlock();
}

void node_sock::set_nl(int val, int flag) { // set without lock
    switch (flag) {
        case CTRL_TCP_SOCK:
            ctrl_tcp_sock = val;
            break;
        case CTRL_TCP_PORT:
            ctrl_tcp_port = val;
            break;
        case CTRL_TCP_SOCK_ID:
            ctrl_tcp_sock_id = val;
            break;
        default:
            break;
    }
}

int node_sock::get(int flag, int* id) {
    int val = -1;
    lock();
    val = get_nl(flag, id);
    unlock();
    return val;
}

int node_sock::get_nl(int flag, int* id) const { // get without lock
    int val = -1;
    switch (flag) {
        case CTRL_TCP_SOCK:
            val = ctrl_tcp_sock;
            if (id != NULL)
                *id = ctrl_tcp_sock_id;
            break;
        case CTRL_TCP_PORT:
            val = ctrl_tcp_port;
            break;
        case CTRL_TCP_SOCK_ID:
            val = ctrl_tcp_sock_id;
            if (id != NULL)
                *id = ctrl_tcp_sock_id;
            break;
        default:
            break;
    }
    return val;
}

