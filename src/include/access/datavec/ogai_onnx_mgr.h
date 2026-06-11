/*
 * Copyright (c) 2025 Huawei Technologies Co.,Ltd.
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
 * ---------------------------------------------------------------------------------------
 *
 * ogai_onnx_mgr.h
 *
 * IDENTIFICATION
 *        src/include/access/datavec/ogai_onnx_mgr.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef OGAI_ONNX_MGR_H
#define OGAI_ONNX_MGR_H

#include "access/datavec/ogai_onnx_runtime.h"
#include "storage/lock/lwlock.h"

#define NAMEDATALEN 64
#define MAXPATH 1024
#define ONNX_MODEL_HASH_CAPACITY 1000
#define ONNX_MODEL_MGR (ONNXModelMgr::GetInstance())

typedef struct ONNXModelDesc {
    OgaiOnnxModelHandle handle;
    int dim;
    pthread_rwlock_t mutex;
} ONNXModelDesc;

typedef struct ONNXModelTag {
    char modelKey[NAMEDATALEN];
    char ownerName[NAMEDATALEN];
} ONNXModelTag;

typedef struct ONNXModelHashEntry {
    ONNXModelTag key;
    ONNXModelDesc* modelDesc;
} ONNXModelHashEntry;

class RWLockGuard {
public:
    explicit RWLockGuard(pthread_rwlock_t& lock, LWLockMode mode = LW_SHARED)
        : m_lock(lock)
        {
        if (mode == LW_SHARED) {
            pthread_rwlock_rdlock(&m_lock);
        } else {
            pthread_rwlock_wrlock(&m_lock);
        }
    }

    ~RWLockGuard()
    {
        pthread_rwlock_unlock(&m_lock);
    }

    RWLockGuard(const RWLockGuard&) = delete;
    RWLockGuard& operator=(const RWLockGuard&) = delete;

private:
    pthread_rwlock_t& m_lock;
};

class ONNXModelMgr : public BaseObject {
public: // static
    static ONNXModelMgr* GetInstance(void);
    static void NewSingletonInstance(void);
    static void DeleteInstance(void);

public:
    bool IsInit();
    ONNXModelDesc* LoadONNXModelByKey(const char* modelKey, const char* ownerName,
                                       const char* modelPath);
    ONNXModelDesc* GetONNXModelDescByKey(const char* modelKey, const char* ownerName);
    ONNXModelDesc* AcquireONNXModelDescByKey(const char* modelKey, const char* ownerName);
    void ReleaseONNXModelDesc(ONNXModelDesc* modelDesc);
    void UnloadONNXModelByKey(const char* modelKey, const char* ownerName);
private:
    ONNXModelMgr()
        : onnxModelMgrCtx(NULL),
          onnxModelHash(NULL),
          onnxEnvHandle(NULL),
          isInit(false)
    {}
    ~ONNXModelMgr()
    {}

    MemoryContext onnxModelMgrCtx;
    HTAB* onnxModelHash;
    pthread_rwlock_t mutex;
    OgaiOnnxEnvHandle onnxEnvHandle;
    bool isInit;

    static ONNXModelMgr* onnxModelMgr;
};

#endif // OGAI_ONNX_MGR_H
