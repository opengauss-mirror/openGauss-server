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
 *    src/gausskernel/storage/mot/jit_exec/jit_llvm.h
 *
 * -------------------------------------------------------------------------
 */

/*
 * ATTENTION:
 * 1. Be sure to include gscodegen.h before anything else to avoid clash with PM definition in datetime.h.
 * 2. Be sure to include libintl.h before gscodegen.h to avoid problem with gettext.
 */
#include "libintl.h"
#include "codegen/gscodegen.h"
#include "pgxc/pgxc.h"
#include "storage/mot/jit_exec.h"
#include "jit_llvm.h"

#ifdef ENABLE_LLVM_COMPILE
// for checking if LLVM_ENABLE_DUMP is defined and for using LLVM_VERSION_STRING
#include "llvm/Config/llvm-config.h"
#include "llvm/Support/Host.h"
#endif

extern bool GlobalCodeGenEnvironmentSuccess;

namespace JitExec {
DECLARE_LOGGER(JitLlvm, JitExec);

using namespace dorado;

/** @brief Checks whether the current platforms natively supports LLVM. */
bool JitCanInitThreadCodeGen()
{
    // Make sure that system type is not generic.
    llvm::StringRef hostCPUName = llvm::sys::getHostCPUName();
    MOT_LOG_INFO("LLVM library detected CPU: %s", hostCPUName.str().c_str());
    if (hostCPUName.empty() || hostCPUName.equals("generic")) {
        MOT_LOG_WARN("LLVM library detected unknown generic system: disabling native LLVM");
        MOT_LOG_WARN("LLVM default target triple is: %s, process triple is: %s",
            llvm::sys::getDefaultTargetTriple().c_str(),
            llvm::sys::getProcessTriple().c_str());
        return false;
    }

#ifdef __aarch64__
    return true;  // Using native llvm on ARM
#else
    bool canInit = false;
    if (IS_PGXC_DATANODE && IsMotCodegenEnabled()) {
        canInit = GlobalCodeGenEnvironmentSuccess;
        if (!canInit) {
            MOT_LOG_WARN("LLVM environment is not initialized, native LLVM will not be used.");
        }
    }
    return canInit;
#endif
}

/** @brief Prepares for code generation. */
GsCodeGen* SetupCodegenEnv()
{
    MOT_ASSERT(g_instance.mot_cxt.jitExecMode == JIT_EXEC_MODE_LLVM);

    // create GsCodeGen object for LLVM code generation
    GsCodeGen* code_gen = New(INSTANCE_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_EXECUTOR)) GsCodeGen();
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
}  // namespace JitExec
