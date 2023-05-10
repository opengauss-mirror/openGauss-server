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
 * ceph.h
 *
 * IDENTIFICATION
 *      src\include\component\ceph\ceph.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef LIBSMARTSCAN_CEPHINTERFACE_H
#define LIBSMARTSCAN_CEPHINTERFACE_H

#include "component/thread/mpmcqueue.h"

#define RbdPath "librbd.so.1"
#define RadosPath "librados.so.2"

typedef void *cephClientCtx;
typedef void *imageHandler;
typedef void *rbd_completion_t;
typedef void (*rbd_callback_t)(rbd_completion_t cb, void *arg);

#ifdef GlobalCache
#define NDP_ASYNC_CEPH
#endif
#define CEPH_MAX_LENGTH 64

typedef enum CephStatus {
    CEPH_ERROR = -1,
    CEPH_OK = 0,
} CephStatus;

typedef enum {
    RBD_LOCK_NONE = 0,
    RBD_LOCK_EXCLUSIVE = 1,
    RBD_LOCK_SHARED = 2,
} RbdLockType;

typedef struct CephObject {
    uint32_t objId;
    uint64_t objOffset;
    char image[CEPH_MAX_LENGTH];
    char pool[CEPH_MAX_LENGTH];
} CephObject;

/*client keepalive deadline, a client should be kicked of*/
#define CEPH_CLIENT_TIMEOUT 30

#define DSS_RBD_COOLIE_LEN 16

#define DSS_RBD_HEADER_LEN 32

/**
 * before pool operation should init operation context
 * ctx	handler of pool operation
 * poolName ceph pool name
 * conf  path pf ceph cluster conf
 * timeout client keepalive timeout
 * return 0 sucess, !0 failed;
 */
CephStatus CephClientCtxInit(cephClientCtx *ctx, char* poolName, const char *conf, uint64_t timeout);

/**
 * finish pool operation should close context
 * ctx handle of pool operation
 * return void
 */
void CephClientCtxClose(cephClientCtx ctx);

/**
 *	open a image
 * ctx	handler of pool operation
 * imageName image name
 * fd imagehandler
 * return 0 sucess, !0 failed
 */
CephStatus CephClientImageOpen(cephClientCtx ctx, char *imageName, imageHandler *fd);

/**
 * 	close image
 * 	return 0 sucess, !0 failed
 */
void CephClientImageClose(imageHandler fd);

/**
 * read data from image
 * fd image operation handler
 * offset read from image offset
 * buf read data buf
 * size read data size
 * return  read size;
 */
int32_t CephClientImageReadUsingOffset(imageHandler fd, uint64_t offset, char *buf, uint64_t size);

/**
 * lock image
 * ctx handle of pool operation
 * fd handle of image operation
 * type LockType exclusive/shared/none
 * return 0 sucess, !0 failed
 */
CephStatus CephClientLock(cephClientCtx ctx, imageHandler fd, RbdLockType type);

/**
 *  unlock image
 *  fd handle of image operation
 *  return 0 sucess, !0 failed
 **/
int CephClientUnLock(imageHandler fd);

/* for GlobalCache */
/**
 *
 **/
int CephClientImageRead(imageHandler fd, CephObject* object, char* buf, size_t len);

int CephClientImageAioRead(imageHandler fd, CephObject* object,
                        char* buf, size_t len, rbd_completion_t c);

int CephClientAioCreateCom(void* cb_arg, rbd_callback_t complete_cb, rbd_completion_t* c);

ssize_t CephClientAioGetRet(rbd_completion_t c);

void CephClientAioRelease(rbd_completion_t c);

/**
 *  get ceph descriptor
 *  ctx	handler of pool operation
 *  poolName ceph pool name
 *  imageName image name
 *  fd imagehandler
 *  return 0 sucess, !0 failed
 **/
CephStatus GetCephDesc(char* poolName, char* imageName, cephClientCtx &ctx, imageHandler &fd);

/**
 *  init ceph map lock
 *  return 0 sucess, !0 failed
 **/
CephStatus CephInit();

/**
 *  Finish Ceph
 **/
void CephUnInit();

typedef struct BufferPool {
    MpmcBoundedQueue<void*>* bufferPool;
    void* memptr;
} BufferPool;
extern BufferPool g_bufferPool;

init_type InitCephBufferPool();
void DestroyCephBufferPool();

// same with front end pool size, since frontend unlikely send request exceed memory pool size 2048
constexpr int BUFFER_POOL_SIZE = 2048;
#endif
