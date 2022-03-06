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
 * gscodegen.cpp
 *	  Gauss LLVM Bridge routine
 *
 * IDENTIFICATION
 *	  src/gausskernel/runtime/codegen/gscodegen.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "codegen/gscodegen.h"
#include <unordered_set>

#ifdef ENABLE_LLVM_COMPILE
#include "llvm/ADT/Triple.h"
#include "llvm/ADT/ArrayRef.h"
#include "llvm/Analysis/InstructionSimplify.h"
#include "llvm/Analysis/Passes.h"
#include "llvm/Analysis/TargetTransformInfo.h"
#include "llvm/Bitcode/BitcodeReader.h"
#include "llvm/ExecutionEngine/ExecutionEngine.h"
#include "llvm/ExecutionEngine/MCJIT.h"
#include "llvm/IR/DataLayout.h"
#include "llvm/IR/Verifier.h"
#include "llvm/Linker/Linker.h"
#include "llvm/IR/PassManager.h"
#include "llvm/Support/DynamicLibrary.h"
#include "llvm/Support/Host.h"
#include "llvm/IR/InstIterator.h"
#include "llvm/IR/NoFolder.h"
#include "llvm/Support/TargetRegistry.h"
#include "llvm/Support/TargetSelect.h"
#include "llvm/Support/raw_ostream.h"
#include "llvm/Analysis/TargetLibraryInfo.h"
#include "llvm/Transforms/IPO.h"
#include "llvm/Transforms/IPO/PassManagerBuilder.h"
#include "llvm/Transforms/Scalar.h"
#include "llvm/Transforms/Utils/BasicBlockUtils.h"
#include "llvm/Transforms/Utils/Cloning.h"
#include "llvm-c/Core.h"
#endif

#include "pgxc/pgxc.h"
#include "utils/memutils.h"
#include "utils/resowner.h"
#include "catalog/pg_type.h"
#include "postmaster/postmaster.h"
#include "optimizer/streamplan.h"

using namespace llvm;
using namespace std;

bool GlobalCodeGenEnvironmentSuccess = false;
static bool gscodegen_initialized = false;

typedef void (*SignalHandlerPtr)(int);
SignalHandlerPtr savedSIGSEGV = (SignalHandlerPtr)(0);
sigjmp_buf SEGSEGVjmp;

void llvm_start_multithreaded()
{
#undef LLVM_ENABLE_THREADS
#define LLVM_ENABLE_THREADS 1
    return;
}
void llvm_stop_multithreaded()
{
#undef LLVM_ENABLE_THREADS
#define LLVM_ENABLE_THREADS 0
    return;
}

extern void lock_codegen_process_sub(int count);
extern void lock_codegen_process_add();

namespace dorado {
void GsCodeGen::initialize()
{
    m_codeGenContext = AllocSetContextCreate(CurrentMemoryContext,
        "codegen memory context",
        ALLOCSET_DEFAULT_MINSIZE,
        ALLOCSET_DEFAULT_INITSIZE,
        ALLOCSET_DEFAULT_MAXSIZE);
}

bool GsCodeGen::InitializeLlvm(bool load_backend)
{
    if (gscodegen_initialized) {
        return true;
    }

    LLVM_TRY()
    {
        /*
         * This allocates a global llvm struct and enables multithreading.
         * There is no real good time to clean this up but we only make it once.
         */
        llvm_start_multithreaded();

        bool result = false;
        /*
         * This can *only* be called once per process and is used to setup
         * dynamically linking jitted code.
         */
        result = llvm::InitializeNativeTarget();
        if (true == result) {
            ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_OBJECT),
                    errmodule(MOD_LLVM),
                    (errmsg("Failed to initialze NativeTarget for LLVM."))));
        }
        /* Add the following initiliaztion for MCJIT */
        result = llvm::InitializeNativeTargetAsmPrinter();
        if (true == result) {
            ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_OBJECT),
                    errmodule(MOD_LLVM),
                    (errmsg("Failed to initialze NativeTargetAsmPrinter for LLVM."))));
        }

        result = llvm::InitializeNativeTargetAsmParser();
        if (true == result) {
            ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_OBJECT),
                    errmodule(MOD_LLVM),
                    (errmsg("Failed to initialze NativeTargetAsmParser for LLVM."))));
        }
    }
    LLVM_CATCH("Failed to initialize LLVM enviroment!");

    gscodegen_initialized = true;

    return gscodegen_initialized;
}

void GsCodeGen::cleanUpLlvm()
{
    LLVM_TRY()
    {
        llvm_stop_multithreaded();
    }
    LLVM_CATCH("Failed to stop llvm thread!");
}

GsCodeGen::GsCodeGen() : m_llvmIRLoaded(false), m_isCorrupt(false)
{
    LLVM_TRY()
    {
        m_llvmContext = new llvm::LLVMContext();
    }
    LLVM_CATCH("Failed to allocate LLVM context!");
    m_currentModule = NULL;
    m_optimizations_enabled = false;
    m_machineCodeJitCompiled = NIL;
    m_currentEngine = NULL;
    m_initialized = false;
    m_moduleCompiled = false;
    m_codeGenContext = NULL;
    m_cfunction_calls = NIL;
}

GsCodeGen::~GsCodeGen()
{
    if (m_llvmContext != NULL) {
        LLVM_TRY()
        {
            delete m_llvmContext;
        }
        LLVM_CATCH("Failed to release LLVM context!");
    }
    m_currentModule = NULL;
    m_llvmContext = NULL;
    m_machineCodeJitCompiled = NULL;
    m_currentEngine = NULL;
    m_codeGenContext = NULL;
    m_cfunction_calls = NULL;
}

void GsCodeGen::enableOptimizations(bool enable)
{
    m_optimizations_enabled = enable;
}

llvm::LLVMContext& GsCodeGen::context()
{
    /*
     * Get the memory context hold by llvm module, since all
     * the llvm values and types are only valid in llvm module.
     */
    return m_currentModule->getContext();
}

bool GsCodeGen::createNewModule()
{
    /* If m_currentModule <> NULL, it indicates we either
     * compiled the module or haven't created the module yet.
     */
    if (m_currentModule != NULL) {
        return true;
    }

    LLVM_TRY()
    {
        m_currentModule = new llvm::Module("LLVM_module01", *m_llvmContext);
    }
    LLVM_CATCH("Failed to create new module!");

    if (!m_initialized) {
        return init();
    }

    return true;
}

llvm::ExecutionEngine* GsCodeGen::createNewEngine(llvm::Module* module)
{
    string errStr;
    llvm::ExecutionEngine* execEngine = NULL;
    SmallVector<std::string, 1> mattrs;
#ifdef __aarch64__
    mattrs.push_back("+crc");
#else
    mattrs.push_back("-avx2");
#endif

    /* Create a new execution engine */
    LLVM_TRY()
    {
        std::unique_ptr<llvm::Module> uni_module(module);
        execEngine = llvm::EngineBuilder(std::move(uni_module))
                         .setErrorStr(&errStr)
                         .setEngineKind(EngineKind::JIT)
                         .setMCPU(sys::getHostCPUName())
                         .setMAttrs(mattrs)
                         .create();
    }
    LLVM_CATCH("Failed to create new execution engine!");

    /* If return null, report error */
    if (execEngine == NULL) {
        ereport(ERROR,
            (errcode(ERRCODE_UNDEFINED_OBJECT),
                errmodule(MOD_LLVM),
                errmsg("Failed to create LLVM state object ExecutionEngine: %s", errStr.c_str())));
    }
    return execEngine;
}

bool GsCodeGen::CheckPreloadModule(llvm::Module* mod)
{
    if (mod == NULL) {
        ereport(LOG, (errmodule(MOD_LLVM), errmsg("Failed to ParseBitcodeFile.")));
        return false;
    }

    const char* triple = mod->getTargetTriple().c_str();
#ifdef __aarch64__
    const char* cpu = "aarch64";
#elif defined (__x86_64__)
    const char* cpu = "x86_64";
#else
    const char* cpu = "generic";
#endif

    char* pos = strstr(const_cast<char*>(triple), cpu);
    if (!pos) {
        ereport(LOG, (errmodule(MOD_LLVM),
                errmsg("Target triple (%s) was not matched on native CPU.", triple)));
        return false;
    }

    return true;
}

bool GsCodeGen::parseIRFile(StringInfo filename)
{
    llvm::Module* m_module = NULL;

    if (m_llvmIRLoaded) {
        return true;
    }

    LLVM_TRY()
    {
        /*
         * Loads an LLVM module. file should be the local path to the LLVM bitcode file.
         */
        ErrorOr<std::unique_ptr<MemoryBuffer>> fileOrErr = MemoryBuffer::getFile(filename->data);
        if (std::error_code fileErr = fileOrErr.getError()) {
            ereport(LOG,
                (errmodule(MOD_LLVM),
                    errmsg("Failed to get file:%s because of %s.",
                        filename->data,
                        fileOrErr.getError().message().c_str())));
            return false;
        }

        t_thrd.codegen_cxt.codegen_IRload_thr_count++;
        lock_codegen_process_add();

        /*
         * Loads an LLVM module, the ir file is a reference to a memory buffer containing
         * LLVM bitcode. To avoid memory leak, we should use to memory alloced at the
         * begenning.
         */
        Assert(m_llvmContext != NULL);
        LWLockAcquire(LLVMParseIRLock, LW_EXCLUSIVE);
        Expected<std::unique_ptr<Module>> moduleOrErr =
            parseBitcodeFile(fileOrErr->get()->getMemBufferRef(), *(m_llvmContext));
        LWLockRelease(LLVMParseIRLock);

        if (Error moduleErr = moduleOrErr.takeError()) {
            ereport(LOG, (errmodule(MOD_LLVM), errmsg("ParseBitcodeFile get error.")));
            return false;
        }
        m_module = moduleOrErr.get().release();
        if (!CheckPreloadModule(m_module))
            return false;
    }
    LLVM_CATCH("Failed to parse IR file.");

    m_currentModule = m_module;
    m_moduleCompiled = false;

    if (!m_llvmIRLoaded) {
        m_llvmIRLoaded = true;
    }
    return init();
}

void GsCodeGen::loadIRFile()
{
    dorado::GsCodeGen* llvmCodeGen = (GsCodeGen*)t_thrd.codegen_cxt.thr_codegen_obj;

    char* exec_path = NULL;
    StringInfo filename = makeStringInfo();
    exec_path = gs_getenv_r("GAUSSHOME");
    if (NULL != exec_path && strcmp(exec_path, "\0") != 0) {
        char* exec_path_r = realpath(exec_path, NULL);
        if (exec_path_r) {
            appendStringInfo(filename, "%s/share/llvmir/GaussDB_expr.ir", exec_path_r);
            check_backend_env(exec_path);
            free(exec_path_r);
        } else {
            ereport(ERROR,
                    (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                     errmodule(MOD_LLVM),
                     errmsg("Got illegal enviroment parameter $GAUSSHOME, please set $GAUSSHOME as your "
                            "installation directory!")));
        }
    } else {
        ereport(ERROR,
            (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                errmodule(MOD_LLVM),
                errmsg("Failed to get enviroment parameter $GAUSSHOME or it is NULL, please set $GAUSSHOME as your "
                       "installation directory!")));
    }
    exec_path = NULL;

    if (!llvmCodeGen->parseIRFile(filename)) {
        pfree_ext(filename->data);
        pfree_ext(filename);
        ereport(
            ERROR, (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED), errmodule(MOD_LLVM), errmsg("Failed to load IR file!")));
    }

    pfree_ext(filename->data);
    pfree_ext(filename);
}

void GsCodeGen::compileCurrentModule(bool enable_jitcache)
{
    /* m_currentModule == NULL when we hit a cache */
    if (m_currentModule == NULL) {
        return;
    }

    MemoryContext oldContext = MemoryContextSwitchTo(m_codeGenContext);
    llvm::Module* module = m_currentModule;

    /* Compile the current module and hang the compiled module over the execution engine */
    llvm::ExecutionEngine* exectorEngine = compileModule(module, enable_jitcache);

    if (NULL == exectorEngine) {
        (void)MemoryContextSwitchTo(oldContext);
        return;
    }

    ListCell* cell = NULL;
    LLVM_TRY()
    {
        foreach (cell, m_machineCodeJitCompiled) {
            Llvm_Map<llvm::Function*, void**>* map = (Llvm_Map<llvm::Function*, void**>*)lfirst(cell);
            llvm::Function* func = map->key;

            /*
             * If the machine code has been generated, do not make it again.
             */
            if (NULL != *map->value) {
                continue;
            }
            void* jittedFunction = exectorEngine->getPointerToFunction(func);
            if (jittedFunction != NULL) {
                *map->value = jittedFunction;
            } else {
                *map->value = NULL;
                ereport(LOG,
                    (errcode(ERRCODE_UNDEFINED_FUNCTION),
                        errmodule(MOD_LLVM),
                        errmsg("Failed to find jitted function \"%s\".", func->getName().data())));
            }
        }
    }
    LLVM_CATCH("Failed to find jitted LLVM function!");

    m_moduleCompiled = true;

    /* So we can read the IR file again when a new statement comes. */
    m_llvmIRLoaded = false;

    (void)MemoryContextSwitchTo(oldContext);
}

llvm::ExecutionEngine* GsCodeGen::compileModule(llvm::Module* module, bool enable_jitcache)
{
    string errStr;
    llvm::ExecutionEngine* newEngine = createNewEngine(module);

    /* set current engine for module optimization */
    if (likely(newEngine != NULL)) {
        m_currentEngine = newEngine;
    }

    /*
     * Optimize the current module, which can greatly reduce the
     * unused IR functions and inline all the IR functions.
     */
    if (m_optimizations_enabled) {
        optimizeModule(module);
    }

    /* compilation */
    LLVM_TRY()
    {
        m_currentEngine->finalizeObject();
    }
    LLVM_CATCH("Failed to compile LLVM module!");

    if (u_sess->attr.attr_sql.enable_codegen_print) {
        ereport(LOG, (errmodule(MOD_LLVM), errmsg("Begin dump all the IR function after optimization!")));
        LWLockAcquire(LLVMDumpIRLock, LW_EXCLUSIVE);
        module->print(llvm::outs(), nullptr);
        LWLockRelease(LLVMDumpIRLock);
        ereport(LOG, (errmodule(MOD_LLVM), errmsg("End of dumping all the IR function!")));
    }

    return newEngine;
}

void GsCodeGen::releaseResource()
{
    /* release codeGenContext, which contains IR function list */
    if (m_codeGenContext) {
        m_codeGenContext = NULL;
    }

    /* reset the m_machineCodeJitCompiled */
    m_machineCodeJitCompiled = NIL;

    /*
     * release llvm execution engine. since module is subordinate to
     * execution engine, module memory can be released when execution
     * enegine memory is released.
     */
    if (NULL != m_currentEngine) {
        LLVM_TRY()
        {
            delete m_currentEngine;
        }
        LLVM_CATCH("Failed to release LLVM engine!");
        m_currentEngine = NULL;
        m_currentModule = NULL;
    }

    if (NULL != m_currentModule) {
        LLVM_TRY()
        {
            delete m_currentModule;
        }
        LLVM_CATCH("Failed to release LLVM module!");
        m_currentModule = NULL;
    }
    lock_codegen_process_sub(t_thrd.codegen_cxt.codegen_IRload_thr_count);
    t_thrd.codegen_cxt.codegen_IRload_thr_count = 0;
}

bool GsCodeGen::init()
{
    m_initialized = true;

    return true;
}

Type* GsCodeGen::getVoidType()
{
    return Type::getVoidTy(context());
}

Type* GsCodeGen::getType(Oid TypeID)
{
    switch (TypeID) {
        case BITOID:
            return Type::getInt1Ty(context());
        case BOOLOID:
        case CHAROID:
            return Type::getInt8Ty(context());
        case INT2OID:
            return Type::getInt16Ty(context());
        case INT4OID:
            return Type::getInt32Ty(context());
        case INT8OID:
            return Type::getInt64Ty(context());
        case FLOAT4OID:
            return Type::getFloatTy(context());
        case FLOAT8OID:
            return Type::getDoubleTy(context());
        default:
            ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                    errmodule(MOD_LLVM),
                    (errmsg("Invalid LLVM type %d", TypeID))));
            return NULL;
    }
}

PointerType* GsCodeGen::getPtrType(Oid TypeID)
{
    return PointerType::get(getType(TypeID), 0);
}

Type* GsCodeGen::getType(const char* name)
{
    Assert(NULL != m_currentModule && NULL != name);
#if LLVM_MAJOR_VERSION == 12
    return StructType::getTypeByName(context(), StringRef(name, strlen(name)));
#else
    return m_currentModule->getTypeByName(StringRef(name, strlen(name)));
#endif
}

PointerType* GsCodeGen::getPtrType(const char* name)
{
    Assert(NULL != name);
    Type* type = getType(name);

    return PointerType::get(type, 0);
}

PointerType* GsCodeGen::getPtrPtrType(const char* name)
{
    Assert(NULL != name);
    Type* type = getType(name);
    Type* typePtr = PointerType::get(type, 0);

    return PointerType::get(typePtr, 0);
}

llvm::Value* GsCodeGen::CastPtrToLlvmPtr(Type* type, const void* ptr)
{
    Constant* const_int = ConstantInt::get(Type::getInt64Ty(context()), (long long)ptr);

    return ConstantExpr::getIntToPtr(const_int, type);
}

bool GsCodeGen::verifyFunction(Function* fn)
{
    if (m_isCorrupt) {
        return false;
    }

    LLVM_TRY()
    {
        /*  Verify the function is valid. Adapted from the pre-verifier function pass. */
        Function::iterator iter;
        for (iter = fn->begin(); iter != fn->end(); ++iter) {
            if (iter->empty() || !iter->back().isTerminator()) {
                m_isCorrupt = true;
                break;
            }
        }
        if (!m_isCorrupt) {
            llvm::raw_fd_ostream rfdos(STDERR_FILENO, false);
            m_isCorrupt = llvm::verifyFunction(*fn, &rfdos);
        }
    }
    LLVM_CATCH("Failed to verify LLVM function!");

    if (m_isCorrupt) {
        return false;
    }
    return true;
}

void GsCodeGen::FinalizeFunction(Function* function, int plan_node_id)
{
    bool is_valid = false;

    /* Make sure if the generated IR function is avaliable or not */
    is_valid = verifyFunction(function);

    if (u_sess->attr.attr_sql.enable_codegen_print) {
        ereport(LOG,
            (errmodule(MOD_LLVM),
                errmsg("The IR function %s generated with plan_node_id %d is:",
                    function->getName().data(),
                    plan_node_id)));
        LWLockAcquire(LLVMDumpIRLock, LW_EXCLUSIVE);
        function->print(llvm::outs());
        LWLockRelease(LLVMDumpIRLock);
    }

    if (!is_valid) {
        ereport(ERROR,
            (errcode(ERRCODE_UNDEFINED_FUNCTION),
                errmodule(MOD_LLVM),
                errmsg("Codegen failed on verifying IR function %s.", function->getName().data())));
    }

    return;
}

PointerType* GsCodeGen::getPtrType(Type* type)
{
    return PointerType::get(type, 0);
}

/*
 * Create a new APInt of numBits width, initialized as val with function
 * APInt(numBits, val, isSigned). When isSigned is true, val is treated
 * as int64_t and the appropriate sign extension to the bit width will be
 * done. Otherwise, no sign extension occurs.
 */
llvm::Value* GsCodeGen::getIntConstant(Oid TypeID, int64_t val)
{
    switch (TypeID) {
        case BITOID:
            return ConstantInt::get(context(), APInt(1, val, true));
        case CHAROID:
            return ConstantInt::get(context(), APInt(8, val, true));
        case INT2OID:
            return ConstantInt::get(context(), APInt(16, val, true));
        case INT4OID:
            return ConstantInt::get(context(), APInt(32, val, true));
        case INT8OID:
            return ConstantInt::get(context(), APInt(64, val, true));
        default:
            ereport(LOG, (errmodule(MOD_LLVM), errmsg("%d is not support yet.", TypeID)));
            return NULL;
    }
}

GsCodeGen::FnPrototype::FnPrototype(GsCodeGen* gen, const char* name, Type* fnReturnType)
    : codeGen(gen), returnType(fnReturnType)
{
    Assert(NULL != name);
    int nameLen = strlen(name);
    if (0 == nameLen) {
        functionName = NULL;
    } else {
        errno_t rc = 0;
        functionName = (char*)palloc0((nameLen + 1) * sizeof(char));
        rc = strncpy_s(functionName, nameLen + 1, name, nameLen);
        securec_check(rc, "\0", "\0");
    }
}

Function* GsCodeGen::FnPrototype::generatePrototype(LlvmBuilder* builder, llvm::Value** params)
{
    Vector<Type*> arguments;
    for (int i = 0; i < functionArgs.size(); ++i) {
        arguments.push_back(functionArgs[i].type);
    }
    FunctionType* prototype =
        FunctionType::get(returnType, llvm::ArrayRef<Type*>(&arguments.front(), arguments.size()), false);

    /* Need to set m_currentModule if not yet */
    codeGen->createNewModule();

    Function* fn = Function::Create(prototype, Function::ExternalLinkage, functionName, codeGen->m_currentModule);

    if (NULL == fn) {
        ereport(ERROR,
            (errcode(ERRCODE_CODEGEN_ERROR), errmodule(MOD_LLVM), errmsg("Failed to create llvm function prototype.")));
    }
    // Name the arguments
    int idx = 0;
    for (Function::arg_iterator iter = fn->arg_begin(); iter != fn->arg_end(); ++iter, ++idx) {
        iter->setName(functionArgs[idx].name);
        if (params != NULL) {
            params[idx] = &*iter;
        }
    }

    if (builder != NULL) {
        BasicBlock* entry_block = BasicBlock::Create(codeGen->context(), "entry", fn);
        builder->SetInsertPoint(entry_block);
    }

    return fn;
}

void GsCodeGen::addFunctionToMCJit(llvm::Function* fn, void** machineCodeFuncPtr)
{

    ListCell* cell = NULL;
    ListCell* prev = NULL;
    Assert(NULL != fn);
    Assert(NULL != machineCodeFuncPtr);
    *machineCodeFuncPtr = NULL;
    /*
     * Allocate memory in codeGenContext during the process of LLVM functionality, since
     * codeGenContext is valid during the whole query, and it is independent of m_llvmContext.
     */
    MemoryContext oldContext = MemoryContextSwitchTo(m_codeGenContext);

    Llvm_Map<llvm::Function*, void**>* map = New(CurrentMemoryContext) Llvm_Map<llvm::Function*, void**>;
    /*
     * If there exitsts one llvm function that is the same as fn, we should first delete it.
     */
    foreach (cell, m_machineCodeJitCompiled) {
        Llvm_Map<llvm::Function*, void**>* tmpMap = (Llvm_Map<llvm::Function*, void**>*)lfirst(cell);

        if (tmpMap->key == fn) {
            m_machineCodeJitCompiled = list_delete_cell(m_machineCodeJitCompiled, cell, prev);
            break;
        }
        prev = cell;
    }

    /* Add the new LLVM function to MCJIT list */
    map->key = fn;
    map->value = machineCodeFuncPtr;
    m_machineCodeJitCompiled = lappend(m_machineCodeJitCompiled, map);

    (void)MemoryContextSwitchTo(oldContext);
}

void GsCodeGen::recordCFunctionCalls(char* name)
{
    m_cfunction_calls = lappend(m_cfunction_calls, name);
}

bool GsCodeGen::hasCFunctionCalls()
{
    return m_cfunction_calls != NIL;
}

void GsCodeGen::clearCFunctionCalls()
{
    list_free_ext(m_cfunction_calls);
    m_cfunction_calls = NIL;
}

void GsCodeGen::dumpCFunctionCalls(const char* location, int plan_id)
{
    if (m_cfunction_calls == NIL)
        return;
    ListCell* cell = NULL;
    StringInfo log_info = makeStringInfo();
    appendStringInfo(
        log_info, "Encounted C functions which are not codegened in the %s of plan %d are: [", location, plan_id);
    foreach (cell, m_cfunction_calls) {
        appendStringInfoString(log_info, (char*)cell->data.ptr_value);
        if (cell->next)
            appendStringInfoString(log_info, ", ");
    }
    appendStringInfoString(log_info, "].");
    ereport(DEBUG1, (errmodule(MOD_LLVM), errmsg("%s", log_info->data)));

    /* free the allocated string */
    pfree_ext(log_info->data);
    pfree_ext(log_info);
}

void GsCodeGen::optimizeModule(llvm::Module* module)
{
    ListCell* cell = NULL;

    LLVM_TRY()
    {
        /*
         * Passmanager will be userd to construct optimizations passed that are 'typical'
         * for c/c++ program. We're We're relying on llvm to pick the best passes for us.
         */
        PassManagerBuilder pass_builder;

        /* optimize level : -O2 */
        pass_builder.OptLevel = 2;

        /* Don't optimize for code size : corresponds to -O2/ -O3 */
        pass_builder.SizeLevel = 0;
        pass_builder.Inliner = createFunctionInliningPass();

        /*
         * Specifying the data layout is necessary for some optimizations
         * e.g. : removing many of the loads/stores produced by structs.
         */
        llvm::TargetIRAnalysis target_analysis = m_currentEngine->getTargetMachine()->getTargetIRAnalysis();

        /*
         * Before running any other optimization passes, run the internalize pass, giving
         * it the names of all functions registered by addFunctionToJIT(), followed by the
         * global dead code elimination pass. This causes all functions not registered to be
         * JIT'd to be marked as internal, and any internal functions that are not used are
         * deleted by DCE pass. This greatly decreases compile time by removing unused code.
         */
        unordered_set<string> exported_fn_names;
        foreach (cell, m_machineCodeJitCompiled) {
            Llvm_Map<llvm::Function*, void**>* tmpMap = (Llvm_Map<llvm::Function*, void**>*)lfirst(cell);
            llvm::Function* func = tmpMap->key;
            exported_fn_names.insert(func->getName().data());
        }

        legacy::PassManager* module_pass_manager(new legacy::PassManager());
        module_pass_manager->add(createTargetTransformInfoWrapperPass(target_analysis));
        module_pass_manager->add(llvm::createInternalizePass([&exported_fn_names](const llvm::GlobalValue& gv) {
            return exported_fn_names.find(gv.getName().str()) != exported_fn_names.end();
        }));

        /* boost:: scoped_ptr<PassManager> module_pass_manager(new PassManager() */
        module_pass_manager->add(createGlobalDCEPass());
        module_pass_manager->run(*module);

        /*
         * Create and run function pass manager:
         * boost::scoped_ptr<FunctionPassManager> fn_pass_manager(new FunctionPassManager(M));
         */
        legacy::FunctionPassManager* fn_pass_manager = new legacy::FunctionPassManager(module);
        fn_pass_manager->add(llvm::createTargetTransformInfoWrapperPass(target_analysis));
        pass_builder.populateFunctionPassManager(*fn_pass_manager);
        fn_pass_manager->doInitialization();
        llvm::Module::iterator it = module->begin();
        llvm::Module::iterator end = module->end();
        while (it != end) {
            if (!it->isDeclaration()) {
                fn_pass_manager->run(*it);
            }
            ++it;
        }
        fn_pass_manager->doFinalization();

        /* Create and run module pass manager */
        delete module_pass_manager;
        module_pass_manager = new legacy::PassManager();
        module_pass_manager->add(llvm::createTargetTransformInfoWrapperPass(target_analysis));
        pass_builder.populateModulePassManager(*module_pass_manager);
        module_pass_manager->run(*module);
        delete module_pass_manager;
        delete fn_pass_manager;
    }
    LLVM_CATCH("Failed to optimize current module!");
}
}  // namespace dorado

static bool getCpuInfo(
    unsigned cpu_info, unsigned* cpu_rEAX, unsigned* cpu_rEBX, unsigned* cpu_rECX, unsigned* cpu_rEDX)
{
#if defined(__GNUC__) || defined(__clang__) || defined(_MSC_VER)
#if defined(__GNUC__) || defined(__clang__)
#if defined(__x86_64__)
    /*
     * gcc doesn't know cpuid would clobber ebx/rbx. Preserve it manually.
     */
    __asm__("movq\t%%rbx, %%rsi\n\t cpuid\n\t xchgq\t%%rbx, %%rsi\n\t"
            : "=a"(*cpu_rEAX), "=S"(*cpu_rEBX), "=c"(*cpu_rECX), "=d"(*cpu_rEDX)
            : "a"(cpu_info));
#elif defined(__i386__)
    __asm__("movl\t%%ebx, %%esi\n\t cpuid\n\t xchgl\t%%ebx, %%esi\n\t"
            : "=a"(*cpu_rEAX), "=S"(*cpu_rEBX), "=c"(*cpu_rECX), "=d"(*cpu_rEDX)
            : "a"(cpu_info));
#else
    Assert(0 && "This method is defined only for x86.");
#endif
#elif defined(_MSC_VER)
#endif
    return false;
#else
    return true;
#endif
}

static bool isCPUFeatureSupportCodegen()
{
#ifdef __aarch64__
    return true;
#endif

    bool isSupportSSE42 = false;
    unsigned EAX = 0;
    unsigned EBX = 0;
    unsigned ECX = 0;
    unsigned EDX = 0;
    unsigned MaxLevel = 0;
    union {
        unsigned u[3];
        char c[12];
    } text;
    errno_t errorno = EOK;
    errorno = memset_s(&text, sizeof(text), 0, sizeof(text));
    securec_check(errorno, "\0", "\0");

    if (getCpuInfo(0, &MaxLevel, text.u + 0, text.u + 2, text.u + 1) || MaxLevel < 1) {
        ereport(LOG, (errmodule(MOD_LLVM), errmsg("Unable to get cpu info")));

    } else {
        /* try to check if the cpu support SSE4.2 */
        (void)getCpuInfo(1, &EAX, &EBX, &ECX, &EDX);
        isSupportSSE42 = (ECX >> 20) & 1;
        if (!isSupportSSE42) {
            ereport(LOG, (errmodule(MOD_LLVM), errmsg("SSE4.2 is not supported, disable codegen.")));
        }
    }

    return isSupportSSE42;
}

bool canInitCodegenInvironment()
{
    if (!IsInitdb && !GlobalCodeGenEnvironmentSuccess) {
        return true;
    } else
        return false;
}

bool canInitThreadCodeGen()
{
    bool canInit = false;

    /* if current dbstate is not normal, disable  enable_codegen */
    if (get_local_dbstate() != NORMAL_STATE) {
        u_sess->attr.attr_sql.enable_codegen = false;
        return canInit;
    }

    if (IS_PGXC_DATANODE && u_sess->attr.attr_sql.enable_codegen &&
        !t_thrd.codegen_cxt.thr_codegen_obj) {
        /* GlobalCodeGenEnvironmentSuccess is true, means CPU feature support codegen */
        canInit = GlobalCodeGenEnvironmentSuccess;
        if (!canInit) {
            u_sess->attr.attr_sql.enable_codegen = false;
        }
    }
    return canInit;
}

/**
 * @Description : Try to initializ the LLVM enviroment.
 */
void CodeGenProcessInitialize()
{
    /* before call InitializeLlvm, check whether CPU feature support codegen */
    if (isCPUFeatureSupportCodegen() && canInitCodegenInvironment()) {
        MemoryContext current_context = CurrentMemoryContext;

        PG_TRY();
        {
            GlobalCodeGenEnvironmentSuccess = dorado::GsCodeGen::InitializeLlvm();
        }
        PG_CATCH();
        {
            (void)MemoryContextSwitchTo(current_context);
            FlushErrorState();
            ereport(LOG, (errmodule(MOD_LLVM), errmsg("Failed to initialze environment for codegen.")));
        }
        PG_END_TRY();
    }
}

/**
 * @Description : Clean up LLVM enviroment resource
 *				  before exit postmaster.
 */
void CodeGenProcessTearDown()
{
    if (GlobalCodeGenEnvironmentSuccess) {
        dorado::GsCodeGen::cleanUpLlvm();
    }
}

/**
 * @Description : Initialize GsCodeGen Obj.
 */
void CodeGenThreadInitialize()
{
    if (t_thrd.codegen_cxt.codegen_IRload_thr_count != 0)
        ereport(LOG, (errmodule(MOD_LLVM), errmsg("Failed to release thread codegen")));
    if (canInitThreadCodeGen()) {
        MemoryContext current_context = CurrentMemoryContext;

        PG_TRY();
        {
            t_thrd.codegen_cxt.thr_codegen_obj = New(CurrentMemoryContext) dorado::GsCodeGen();
        }
        PG_CATCH();
        {
            (void)MemoryContextSwitchTo(current_context);
            FlushErrorState();
            t_thrd.codegen_cxt.thr_codegen_obj = NULL;
            ereport(LOG, (errmodule(MOD_LLVM), errmsg("Failed to initialze thread codegen.")));
        }
        PG_END_TRY();
    }
}

bool CodeGenThreadObjectReady()
{
    return t_thrd.codegen_cxt.thr_codegen_obj != NULL && !t_thrd.codegen_cxt.g_runningInFmgr;
}

/**
 * @Description	: Allocate memory context for codegen.
 */
void CodeGenThreadRuntimeSetup()
{
    if (CodeGenThreadObjectReady()) {
        /* Do some initialization for the codegen Object */
        ((dorado::GsCodeGen*)t_thrd.codegen_cxt.thr_codegen_obj)->initialize();
    }
}

/**
 * @Description	: Compile all the IR function in module to
 *				  get the machine code.
 */
void CodeGenThreadRuntimeCodeGenerate()
{
    ((dorado::GsCodeGen*)t_thrd.codegen_cxt.thr_codegen_obj)->enableOptimizations(true);
    ((dorado::GsCodeGen*)t_thrd.codegen_cxt.thr_codegen_obj)->compileCurrentModule(false);
}

/**
 * @Description : clean up all the thread LLVM resource according
 *				  to the current query. This resource includes
 *				  execution engine/module and GsCodeGen Obj.
 */
void CodeGenThreadTearDown()
{
    if (t_thrd.codegen_cxt.thr_codegen_obj) {
        ((dorado::GsCodeGen*)t_thrd.codegen_cxt.thr_codegen_obj)->releaseResource();
        delete (dorado::GsCodeGen*)t_thrd.codegen_cxt.thr_codegen_obj;
        t_thrd.codegen_cxt.thr_codegen_obj = NULL;
    }
}

/**
 * @Description : reset machine code in subtransaction
 */
void CodeGenThreadReset()
{
    if (t_thrd.codegen_cxt.thr_codegen_obj)
        ((dorado::GsCodeGen*)t_thrd.codegen_cxt.thr_codegen_obj)->resetMCJittedFunc();
}

/**
 * @Description : Make sure if we have a GsCodeGen Obj or not.
 */
/**
 * @Description : Since it takes some time to compile the LLVM IR function to
 *				: machine code, we would not enjoy any benefit if the actual
 *				: execute time is very low. The simplest method is to check
 *				: if we have engough rows.
 *
 * @In rows		: The number of estimated rows in each datanode.
 * @Return		:
 * @Notes		: It is better to compile the estimated compilation time and
 *				: the estimated exectution time, not only consider the rows.
 */
bool CodeGenPassThreshold(double rows, int dn_num, int dop)
{
    if (isIntergratedMachine) {
        if (dn_num <= 0) {
            dn_num = 1;
        }
        Assert(dop > 0);
        rows = rows / dn_num / dop;
    }

    if (rows >= u_sess->attr.attr_sql.codegen_cost_threshold)
        return true;

    return false;
}
