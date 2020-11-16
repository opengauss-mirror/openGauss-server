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
 * jit_llvm.h
 *    JIT LLVM.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/jit_exec/src/jit_llvm.h
 *
 * -------------------------------------------------------------------------
 */

// Be sure to include global.h before postgres.h to avoid conflict between libintl.h (included in global.h)
// and c.h (included in postgres.h).
#include "global.h"

// be careful to include gscodegen.h before anything else to avoid clash with PM definition in datetime.h
#include "codegen/gscodegen.h"
#include "pgxc/pgxc.h"
#include "storage/mot/jit_exec.h"
#include "jit_llvm.h"

// for checking if LLVM_ENABLE_DUMP is defined and for using LLVM_VERSION_STRING
#include "llvm/Config/llvm-config.h"

extern bool GlobalCodeGenEnvironmentSuccess;

namespace JitExec {
DECLARE_LOGGER(JitLlvm, JitExec);

using namespace dorado;

#ifdef __aarch64__
// on ARM platforms we experienced instability during concurrent JIT compilation, so we use a spin-lock
/** @var The global ARM compilation lock. */
static pthread_spinlock_t _arm_compile_lock;

/** @var Denotes whether the global ARM compilation lock initialization was already attempted. */
static int _arm_compile_lock_initialized = 0;

/** @var Denote the global ARM compilation lock is initialized and ready for use. */
static bool _arm_compile_lock_ready = false;

/** @brief Initializes the global ARM compilation lock. */
bool InitArmCompileLock()
{
    bool result = true;
    if (MOT_ATOMIC_CAS(_arm_compile_lock_initialized, 0, 1)) {
        int res = pthread_spin_init(&_arm_compile_lock, 0);
        if (res != 0) {
            MOT_REPORT_SYSTEM_ERROR_CODE(
                res, pthread_spin_init, "Initialize LLVM", "Failed to initialize compile spin-lock for ARM platform");
            // turn off JIT
            DisableMotCodegen();
            result = false;
        } else {
            _arm_compile_lock_ready = true;
        }
    }
    return result;
}

/** @brief Destroys the global ARM compilation lock. */
void DestroyArmCompileLock()
{
    if (_arm_compile_lock_ready) {
        int res = pthread_spin_destroy(&_arm_compile_lock);
        if (res != 0) {
            MOT_REPORT_SYSTEM_ERROR_CODE(
                res, pthread_spin_init, "Initialize LLVM", "Failed to destroy compile spin-lock for ARM platform");
        } else {
            _arm_compile_lock_ready = false;
            MOT_ATOMIC_STORE(_arm_compile_lock_initialized, 0);
        }
    }
}

/** @brief Locks the global ARM compilation lock. */
bool AcquireArmCompileLock()
{
    bool result = false;
    int res = pthread_spin_lock(&_arm_compile_lock);
    if (res != 0) {
        MOT_REPORT_SYSTEM_ERROR_CODE(
            res, pthread_spin_init, "Compile LLVM", "Failed to acquire compile spin-lock for ARM platform");
    } else {
        result = true;
    }
    return result;
}

/** @brief Unlocks the global ARM compilation lock. */
bool ReleaseArmCompileLock()
{
    bool result = false;
    int res = pthread_spin_unlock(&_arm_compile_lock);
    if (res != 0) {
        MOT_REPORT_SYSTEM_ERROR_CODE(
            res, pthread_spin_init, "Compile LLVM", "Failed to release compile spin-lock for ARM platform");
    } else {
        result = true;
    }
    return result;
}
#endif

/** @brief Checks whether the current platforms natively supports LLVM. */
extern bool JitCanInitThreadCodeGen()
{
#ifdef __aarch64__
    return true;  // Using native llvm on ARM
#else
    bool canInit = false;
    if (IS_PGXC_DATANODE && IsMotCodegenEnabled()) {
        canInit = GlobalCodeGenEnvironmentSuccess;
        if (!canInit) {
            // turn off JIT
            DisableMotCodegen();
            MOT_LOG_WARN("SSE4.2 is not supported, disable codegen.");
        }
    }
    return canInit;
#endif
}

/** @brief Prepares for code generation. */
GsCodeGen* SetupCodegenEnv()
{
    // verify that native LLVM is supported
    if (!t_thrd.mot_cxt.init_codegen_once) {
        t_thrd.mot_cxt.init_codegen_once = true;
        if (!JitCanInitThreadCodeGen()) {
            return nullptr;
        }
    }

    // on ARM platform we need to make sure that the compile lock was initialized properly
#ifdef __aarch64__
    if (!_arm_compile_lock_ready) {
        MOT_LOG_TRACE("Previous attempt to initialize compilation lock on ARM failed. Code generation is disabled.");
        // turn off JIT
        DisableMotCodegen();
        return nullptr;
    }
#endif

    // create GsCodeGen object for LLVM code generation
    GsCodeGen* code_gen = New(g_instance.instance_context) GsCodeGen();
    if (code_gen != nullptr) {
        code_gen->initialize();
        code_gen->createNewModule();
    }
    return code_gen;
}

void PrintNativeLlvmStartupInfo()
{
    MOT_LOG_INFO("Using native LLVM version " LLVM_VERSION_STRING);
}

void FreeGsCodeGen(GsCodeGen* code_gen)
{
    // object was allocate on the Process memory context (see SetupCodegenEnv() above, so we can
    // simply call delete operator (which invokes BaseObject::delete class operator defined in palloc.h)
    code_gen->releaseResource();
    delete code_gen;  // invokes parent class BaseObject::delete() operator defined in palloc.h)
}
} // namespace JitExec
