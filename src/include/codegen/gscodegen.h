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
 * ---------------------------------------------------------------------------------------
 *
 * gscodegen.h
 *
 *
 *
 * IDENTIFICATION
 *        src/include/codegen/gscodegen.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef GS_CODEGENH
#define GS_CODEGENH

#ifndef __STDC_LIMIT_MACROS
#define __STDC_LIMIT_MACROS
#endif
#ifndef __STDC_CONSTANT_MACROS
#define __STDC_CONSTANT_MACROS
#endif

#include "pg_config.h"

#ifdef ENABLE_LLVM_COMPILE
#include "llvm/IR/Verifier.h"
#include "llvm/ExecutionEngine/MCJIT.h"
#include "llvm/ExecutionEngine/ObjectCache.h"
#include "llvm/ExecutionEngine/SectionMemoryManager.h"
#include "llvm/IR/DerivedTypes.h"
#include "llvm/IR/IRBuilder.h"
#include "llvm/IR/Intrinsics.h"
#include "llvm/IR/IntrinsicsX86.h"
#include "llvm/IR/IntrinsicsAArch64.h"
#include "llvm/IR/LegacyPassManager.h"
#include "llvm/IR/LLVMContext.h"
#include "llvm/IR/Module.h"
#include "llvm/Support/circular_raw_ostream.h"
#include "llvm/Support/Compiler.h"
#include "llvm/Support/DynamicLibrary.h"
#include "llvm/Support/FileSystem.h"
#include "llvm/Support/MemoryBuffer.h"
#include "llvm/Support/Path.h"
#include "llvm/Support/raw_ostream.h"
#include "llvm/Support/raw_os_ostream.h"
#endif

#undef __STDC_LIMIT_MACROS
#include "c.h"
#include "nodes/pg_list.h"
#include "lib/stringinfo.h"
#include "utils/dfs_vector.h"
#include "postgres.h"
#include "knl/knl_variable.h"

#ifndef BITS
#define BITS 8
#endif

/* The max length of the IR Function's path */
#define MAX_LLVMIR_PATH 1024

/* position of elements that needed in vechashtbl */
#define pos_hashtbl_size 0
#define pos_hashtbl_data 1

/* position of elements that needed in VectorBatch class */
#define pos_batch_mrows 0
#define pos_batch_mcols 1
#define pos_batch_marr 4

/* position of elements that needed in ScalarVector class */
#define pos_scalvec_mrows 0
#define pos_scalvec_flag 3
#define pos_scalvec_vals 5

/* position of elements that needed in hashJoinTbl class */
#define pos_hjointbl_inbatch 12
#define pos_hjointbl_outbatch 13
#define pos_hjointbl_StateLog 23
#define pos_hjointbl_match 26

/* position of elements that needed in hashBasedOperator class */
#define pos_hBOper_htbl 1
#define pos_hBOper_hcxt 2
#define pos_hBOper_cacheLoc 4
#define pos_hBOper_cellCache 5
#define pos_hBOper_keyMatch 6
#define pos_hBOper_cols 18
#define pos_hBOper_mkey 19
#define pos_hBOper_keyIdx 20

/* position of elements that needed in HashAggRunner class */
#define pos_hAggR_hashVal 3
#define pos_hAggR_hSegTbl 12
#define pos_hAggR_hsegmax 14
#define pos_hAggR_hashSize 15

/* position of elements that needed in BaseAggRunner class */
#define pos_bAggR_keyIdxInCell 8
#define pos_bAggR_keySimple 11
#define pos_bAggR_Loc 18
#define pos_bAggR_econtext 22

/* position of elements that needed in ExprContext structure */
#define pos_ecxt_pertuple 5
#define pos_ecxt_scanbatch 17
#define pos_ecxt_innerbatch 18
#define pos_ecxt_outerbatch 19

/* position of elements that needed in hashCell structure */
#define pos_hcell_mval 1

/* position of elements that needed in hashVal structure */
#define pos_hval_val 0
#define pos_hval_flag 1

/* position of elements that needed in StringInfo structure */
#define pos_strinfo_data 0
#define pos_strinfo_len 1

/* position of elements that needed in SonicHashAgg structure */
#define pos_shashagg_ecxt 12

/* position of elements that needed in SonicHash structure */
#define pos_shash_data 1
#define pos_shash_sonichmemctl 14
#define pos_shash_loc 16

/* position of elements that needed in SonicHashMemoryControl structure */
#define pos_sonichmemctl_hcxt 8

/* position of elements that needed in SonicDatumArray structure */
#define pos_sdarray_cxt 1
#define pos_sdarray_atomsize 5
#define pos_sdarray_nbit 7
#define pos_sdarray_arr 8

/* position of elements that needed in atom structure */
#define pos_atom_data 0
#define pos_atom_nullflag 1

#ifdef ENABLE_LLVM_COMPILE
/* The whole intrinsic methods are listed in include/llvm/IR/IntrinsicEnums.inc */
#ifdef LLVM_MAJOR_VERSION

#if (LLVM_MAJOR_VERSION - LLVM_MAJOR_VERSION -1 == 1)
#error LLVM version was defined as empty.
#else
#if (LLVM_MAJOR_VERSION == 10)
const int llvm_prefetch = 217;
const int llvm_sadd_with_overflow = 229;
const int llvm_smul_with_overflow = 236;
const int llvm_ssub_with_overflow = 241;
#elif (LLVM_MAJOR_VERSION == 11)
const int llvm_prefetch = 225;
const int llvm_sadd_with_overflow = 239;
const int llvm_smul_with_overflow = 247;
const int llvm_ssub_with_overflow = 252;
#elif (LLVM_MAJOR_VERSION == 12)
const int llvm_prefetch = 225;
const int llvm_sadd_with_overflow = 240;
const int llvm_smul_with_overflow = 250;
const int llvm_ssub_with_overflow = 256;
#else
#error Un-supported LLVM version.
#endif
#endif

#else
#error LLVM version is not defined.
#endif

/*
 * Declare related LLVM classes to avoid namespace pollution.
 */
namespace llvm {
class AllocaInst;
class BasicBlock;
class ConstantFolder;
class ExecutionEngine;
class Function;
using legacy::FunctionPassManager;
class LLVMContext;
class Module;
class NoFolder;
using legacy::PassManager;
class PointerType;
class StructType;
class TargetData;
class Type;
class Value;
class PassManagerBuilder;

template <typename T, typename I>
class IRBuilder;

class IRBuilderDefaultInserter;
}  // namespace llvm
#endif

namespace dorado {

/*
 * @Description	: Check whether we have the right to init codegen inviroment.
 * @return		: Return true if we can initialize the codgen inviroment.
 */
bool canInitCodegenInvironment();

/*
 * @Description	: Check whether we can initialize the CodeGen Object.
 * @return		: Return true if we can initialize the GsCodeGen Obj.
 */
bool canInitThreadCodeGen();

#ifdef ENABLE_LLVM_COMPILE
class GsCodeGen : public BaseObject {
public:
    void initialize();

    /*
     * @Brief        : Initialize LLVM environment.
     * @Description  : This function must be called once per process before
     *				   any llvm API called. LLVM needs to allocate data
     *				   structures for multi-threading support and to enable
     *				   dynamic linking of jitted code. if 'load_backend' is
     *				   true, load the backend static object for llvm.  This
     *				   is needed when libbackend.so is loaded from java. llvm
     *				   will be default only look in the current object and
     *				   not be able to find the backend symbols.
     * @in load_backend	: Decide to load the backend static object or not.
     *				      The default value is false.
     * @return		: Return true if succeed.
     */
    static bool InitializeLlvm(bool load_backend = false);

    /*
     * @Description	: Clear up the LLVM environment.
     */
    static void cleanUpLlvm();

    /*
     * @Description	: Load IR file and create a module.
     * @in filename	: Finename, a IR file path, like:\user\xxx\xxx.ir.
     * @return		: Return true if succeed..
     */
    bool parseIRFile(StringInfo filename);

    /*
     * @Description	: Find the location of IR file and load it.
     */
    void loadIRFile();

    /*
     * Typedef builder in case we want to change the template arguments later
     */
    typedef llvm::IRBuilder<> LlvmBuilder;

    /*
     * Utility struct that wraps a variable name and llvm type.
     */
    struct NamedVariable {
        char* name;
        llvm::Type* type;

        NamedVariable(char* name = "", llvm::Type* type = NULL)
        {
            this->name = name;
            this->type = type;
        }
    };

    /*
     * define a Llvm_map which is similar to std::map
     * in order to stroe the key-value pairs.
     */
    template <class T_KEY, class T_VALUE>
    class Llvm_Map : public BaseObject {
    public:
        T_KEY key;
        T_VALUE value;
    };

    /*
     * Abstraction over function prototypes.  Contains helpers to build prototypes and
     * generate IR for the types.
     */
    class FnPrototype {
    public:
        /*
         * @Description	: Create a function prototype object
         * @in gen		: A LlvmCodeGen pointer.
         * @in name		: A specified function name.
         * @in ret_type	: The return value of the LLVM function.
         * @Notes       : It is a constructor.
         */
        FnPrototype(GsCodeGen* gen, const char* name, llvm::Type* ret_type);

        /*
         * @Description : Add a argument for the function.
         * @in var		: var, a struct for function argument
         */
        void addArgument(const NamedVariable& var)
        {
            functionArgs.push_back(var);
        }

        /*
         * @Description	: Create a LLVM function prototype.
         * @in builder	: The LLVM object, obtain many api for building LLVM function.
         * @in params	: return argumnets of LLVM function created.
         * @return		: return a created LLVM function pointer.
         * @Notes       : If params is non-null, this function will also
         *				  return the arguments.
         */
        llvm::Function* generatePrototype(LlvmBuilder* builder = NULL, llvm::Value** params = NULL);

    private:
        friend class GsCodeGen;

        GsCodeGen* codeGen;

        char* functionName; /* functions name */
        llvm::Type* returnType;

        Vector<NamedVariable> functionArgs; /* the functions parameters */
    };

    /*
     * @Brief       : Get a PointerType pointer.
     * @Description : Get a LLVM Type pointer point to 'type'.
     * @in type		: a type struct of LLVM namespace.
     * @return		: return a PointerType pointer.
     */
    llvm::PointerType* getPtrType(llvm::Type* type);

    /*
     * @Brief		: Get a Void Type of LLVM namespace
     * @return		: return the void type in LLVM Data Type.
     */
    llvm::Type* getVoidType();

    /*
     * @Description : Get a Type of LLVM namespace according to a type OID.
     * @in type		: A type OID in Gauss200 database.
     * @return		: return a LLVM Data Type .
     */
    llvm::Type* getType(unsigned int type);

    /*
     * @Description : Get a PointerType pointer of LLVM namespace.
     * @in	type	: A type OID in Gauss200 database.
     * @return		: return a PointerType Type .
     */
    llvm::PointerType* getPtrType(unsigned int type);

    /*
     * @Description : Get a Type of LLVM namespace according to a type name.
     *				: This name should be the one recorded in LLVM module.
     * @in name     : name of the struct type/class type.
     * @return		: return a Type .
     */
    llvm::Type* getType(const char* name);

    /*
     * @Description : Get a Type of LLVM namespace according to the 'name' type.
     * @in type		: A type name which can be recognized in Gauss200 database.
     * @return		: return a PointerType Type.
     */
    llvm::PointerType* getPtrType(const char* name);

    llvm::PointerType* getPtrPtrType(const char* name);
    /*
     * @Description : Create a llvm pointer value from 'ptr'. This is used
     *				  to pass pointers between c-code and code-generated IR.
     * @in type		: Data type in LLVM assemble with respect to the actual
     *				  c-type we want to codegen.
     * @in ptr		: Pointer point to the actual c-code data type.
     * @return		: value in 'type' type.
     */
    llvm::Value* CastPtrToLlvmPtr(llvm::Type* type, const void* ptr);

    /*
     * @Description	: Get reference to llvm context object.
     *                Each GsCodeGen has its own context to allow multiple
     *                threads to be calling into llvm at the same time.
     * @return		: Return reference to llvm context object.
     */
    llvm::LLVMContext& context();

    /*
     * @Description : Get a current module.
     * 				  Module looks likes a container, which contains varibles,
     *				  functions and types.
     * @return		: Return the current module in use.
     */
    llvm::Module* module()
    {
        return m_currentModule;
    }

    /*
     * @Description : Adds the IR function to m_machineCodeJitCompiled List
     *				  of GsCodeGen Object. Make a map which is consisted of
     *				  IR function and machine code function pointer. At this
     *				  time, the machine function pointer is a NULL pointer.
     * @in F		: A IR function which will be as a key of map.
     * @in result_fn_ptr : A machine code function pointer which will be as
     *				  a value of map.
     * @return		: void
     */
    void addFunctionToMCJit(llvm::Function* F, void** result_fn_ptr);

    /*
     * @Description : Adds the c-function calls to m_cfunctions_calls List in case of
     *				codegen_strategy is 'full'.
     * 				each element in this List is a pair of funcName and funcOid.
     * @return 	: void
     */
    void recordCFunctionCalls(char* name);

    /*
     * @Description : check whether we have used C-function calls in codegen.
     *				it will check C-function call recording list,
     *				and return whether m_cfunction_calls is empty or not
     * @return	: bool
     */
    bool hasCFunctionCalls();

    /*
     * @Description	: Clearing the C-function calls list m_cfunction_calls.
     * @return		: void
     */
    void clearCFunctionCalls();

    /*
     * @Description	: Dumping out the C-function calls in m_cfunction_calls.
     * @in plan_id		: plan id as extra information to display.
     * @in location		: description string for Filter Codegen or Targetlist Codegen.
     * @return		:void
     */
    void dumpCFunctionCalls(const char* location, int plan_id);

    /**
     * @Description	: Verfies the generated IR function.
     *
     * @in function	: The compiled function.
     * @return		: Return true if the funcion is valid, else retrun false. .
     */
    bool verifyFunction(llvm::Function* function);

    /**
     * @Description	:
     * @in function : LLVM Function we need to check.
     * @in plan_node_id	: The string flag of this IR function in module .
     * @return		: Void
     */
    void FinalizeFunction(llvm::Function* function, int plan_node_id = 0);

    /*
     * @Description	: Returns the constant 'val' of 'type'. Since LLVM could only
     *				  deal with arguments with the same type, we should make a
     *				  difference between these types, even with the same value,
     *				  say, int32 0 and int64 0.
     * @in type		: The corresponding type defined in PG we need to use in LLVM
     * @in val		: the natural value
     * @return		: the actual value defined in LLVM
     */
    llvm::Value* getIntConstant(unsigned int type, int64_t val);

    /*
     * @Description : Loads an LLVM module. 'file' should be the local path to the
     *				  LLVM bitcode (.ll) file. If 'file_size' is not NULL, it will
     *				  be set to the size of 'file'. The caller is responsible for
     *				  cleaning up module.
     * @in codegen  : A GsCodeGen object.
     * @in file     : file, a IR file to be loaded.
     * @in module	: The current module we need to load.
     * @return		: Return true if successed.
     */
    static bool loadModule(GsCodeGen* codegen, char* file, llvm::Module** module);

    /*
     * @Description : Get a new llvm::ExecutionEngine with all the IR function
     *				  be compiled.
     * @in	M		: A llvm::Module pointer.
     * @in  enable_jitcache	: If enable_jitcache is true, we need to use cache,
     *                else we do not need to use cache.
     * @return 		: ExecutionEngien, which includs the module.
     */
    llvm::ExecutionEngine* compileModule(llvm::Module* M, bool enable_jitcache);

    /*
     * @Description : A GsCodeGen constructor.
     *				  Initialize some variables of LlvmCodegen object.
     */
    GsCodeGen();

    /*
     * @Description : release some variables of LlvmCodegen object.
     */
    virtual ~GsCodeGen();

    /*
     * @Description	: Turns on/off optimization passes for current module.
     * @in	        : enable value, used to decide optimize passes or not
     * @out			: flag to optimization or not.
     */
    void enableOptimizations(bool enable);

    /*
     * @Description : Create a new module.
     */
    bool createNewModule();

    /*
     * @Description : Create a new engine.
     */
    llvm::ExecutionEngine* createNewEngine(llvm::Module* module);

    /*
     * @Description : Close the current module.
     */
    void closeCurrentModule()
    {
        m_currentModule = NULL;
    }

    /*
     * @Description	: IR compile to machine code.
     */
    void compileCurrentModule(bool enable_jitcache);

    /*
     * @Description : Release resource.
     *                The resource includes IR function lists and execution engine.
     */
    void releaseResource();

    /*
     * @Description	: Get the pointer to the current module.
     * @return		: Current module in use.
     */
    llvm::Module* getCurrentModule()
    {
        return m_currentModule;
    }

    /* reset m_machineCodeJitCompiled */
    void resetMCJittedFunc()
    {
        m_machineCodeJitCompiled = NIL;
    }

public:
    /* The module in use during this query process */
    llvm::Module* m_currentModule;

private:
    /* Initializes the jitter and execution engine. */
    bool init();

    /*
     * Optimize the module, this includes pruning the module of any unused
     * functions.
     */
    void optimizeModule(llvm::Module* module);

    /*
     * check preload module
     */
    bool CheckPreloadModule(llvm::Module* module);

    /* Flag used to optimize the module or not */
    bool m_optimizations_enabled;

    /* Flag used to mask if we have loaded the ir file or not */
    bool m_llvmIRLoaded;

    /*
     * If true, the module is corrupt and we cannot codegen this query.
     * we could consider just removing the offending function and attempting to
     * codegen the rest of the query.  This requires more testing though to make sure
     * that the error is recoverable.
     */
    bool m_isCorrupt;

    /* Flag used to mask if the GsCodeGen Obj has been initialized or not */
    bool m_initialized;

    llvm::LLVMContext* m_llvmContext;

    /* The map of function to MCJIT compiled object */
    List* m_machineCodeJitCompiled;

    /* Execution Engine in use during this query process */
    llvm::ExecutionEngine* m_currentEngine;

    /*
     * If true, the module has been compiled.  It is not valid to add additional
     * functions after this point.
     */
    bool m_moduleCompiled;

    MemoryContext m_codeGenContext;

    /* Records the c-function calls in codegen IR fucntion of expression tree */
    List* m_cfunction_calls;
};
#endif

/*
 * Macros used to define the variables
 *
 * By following all these regulars, we could easily define
 * consts
 */
#define DEFINE_CGVAR_INT1(name, val) llvm::Value* name = llvmCodeGen->getIntConstant(BITOID, val)
#define DEFINE_CGVAR_INT8(name, val) llvm::Value* name = llvmCodeGen->getIntConstant(CHAROID, val)
#define DEFINE_CGVAR_INT16(name, val) llvm::Value* name = llvmCodeGen->getIntConstant(INT2OID, val)
#define DEFINE_CGVAR_INT32(name, val) llvm::Value* name = llvmCodeGen->getIntConstant(INT4OID, val)
#define DEFINE_CGVAR_INT64(name, val) llvm::Value* name = llvmCodeGen->getIntConstant(INT8OID, val)

/*
 * Macros used to define the data types
 */
#define DEFINE_CG_VOIDTYPE(name) llvm::Type* name = llvmCodeGen->getVoidType()
#define DEFINE_CG_TYPE(name, typoid) llvm::Type* name = llvmCodeGen->getType(typoid)
#define DEFINE_CG_PTRTYPE(name, typoid) llvm::Type* name = llvmCodeGen->getPtrType(typoid)
#define DEFINE_CG_PTRPTRTYPE(name, typoid) llvm::Type* name = llvmCodeGen->getPtrPtrType(typoid)
#define DEFINE_CG_NINTTYP(name, nbit) llvm::Type* name = llvm::IntegerType::getIntNTy(context, nbit)
#define DEFINE_CG_ARRTYPE(name, typoid, num) llvm::Type* name = llvm::ArrayType::get(typoid, num)

/*
 * Define some operation
 */
#define DEFINE_BLOCK(name, insertbfpointer) \
    llvm::BasicBlock* name = llvm::BasicBlock::Create(context, #name, insertbfpointer);

/*
 * Define macros which help to catch and print the exception
 * use SEGSEGVhandler to catch internal coredump occurred in LLVM.
 */
#define LLVM_TRY()                 \
    bool llvm_oper_success = true; \
    try

#define LLVM_CATCH(msg)                                                                     \
    catch (...)                                                                             \
    {                                                                                       \
        llvm_oper_success = false;                                                          \
    }                                                                                       \
    if (!llvm_oper_success) {                                                               \
        ereport(ERROR, (errcode(ERRCODE_CODEGEN_ERROR), errmodule(MOD_LLVM), errmsg(msg))); \
    }

}  // namespace dorado

/*
 * Series of llvm::crc32 function.
 *
 * Contains llvm_crc32_32_8, llvm_crc32_32_16, llvm_crc32_32_32, llvm_crc32_32_64.
 *
 * All functions have three input parameters. The first parameter is the result value,
 * and the next two parameters are input values.
 * Each funtion's parameters are indicated through the function name.
 *
 * For example, llvm_crc32_32_8 indicates the result value is 32bits,
 * the first input parameter is 32bits, and the second input is 8bits.
 */

#ifdef __aarch64__ /* on ARM platform */

/* its function proto is: i32 @llvm.aarch64.crc32cb(i32 %cur, i32 %bits) */
#define llvm_crc32_32_8(res, current, next)                                                              \
    do {                                                                                                 \
        llvm::Intrinsic::ID id = (unsigned)llvm::Intrinsic::aarch64_crc32cb;                             \
        llvm::Function* fn_crc = llvm::Intrinsic::getDeclaration(mod, id);                               \
        if (fn_crc == NULL) {                                                                            \
            ereport(ERROR,                                                                               \
                (errcode(ERRCODE_LOAD_INTRINSIC_FUNCTION_FAILED),                                        \
                    errmodule(MOD_LLVM),                                                                 \
                    errmsg("Failed to get function Intrinsic::aarch64_crc32cb function!\n")));           \
        }                                                                                                \
        llvm::Value* next_ext = builder.CreateZExt(next, int32Type);                                     \
        res = builder.CreateCall(fn_crc, {current, next_ext}, "crc32_8");                                \
    } while (0)

/* its function proto is: i32 @llvm.aarch64.crc32ch(i32 %cur, i32 %bits) */
#define llvm_crc32_32_16(res, current, next)                                                             \
    do {                                                                                                 \
        llvm::Intrinsic::ID id = (unsigned)llvm::Intrinsic::aarch64_crc32ch;                             \
        llvm::Function* fn_crc = llvm::Intrinsic::getDeclaration(mod, id);                               \
        if (fn_crc == NULL) {                                                                            \
            ereport(ERROR,                                                                               \
                (errcode(ERRCODE_LOAD_INTRINSIC_FUNCTION_FAILED),                                        \
                    errmodule(MOD_LLVM),                                                                 \
                    errmsg("Failed to get function Intrinsic::aarch64_crc32ch function!\n")));           \
        }                                                                                                \
        llvm::Value* next_ext = builder.CreateZExt(next, int32Type);                                     \
        res = builder.CreateCall(fn_crc, {current, next_ext}, "crc32_16");                               \
    } while (0)

/* its function proto is: i32 @llvm.aarch64.crc32cw(i32 %cur, i32 %next) */
#define llvm_crc32_32_32(res, current, next)                                                             \
    do {                                                                                                 \
        llvm::Intrinsic::ID id = (unsigned)llvm::Intrinsic::aarch64_crc32cw;                             \
        llvm::Function* fn_crc = llvm::Intrinsic::getDeclaration(mod, id);                               \
        if (fn_crc == NULL) {                                                                            \
            ereport(ERROR,                                                                               \
                (errcode(ERRCODE_LOAD_INTRINSIC_FUNCTION_FAILED),                                        \
                    errmodule(MOD_LLVM),                                                                 \
                    errmsg("Failed to get function Intrinsic::aarch64_crc32cw function!\n")));           \
        }                                                                                                \
        res = builder.CreateCall(fn_crc, {current, next}, "crc32_32");                                   \
    } while (0)

/* its function proto is: i32 @llvm.aarch64.crc32cx(i32 %cur, i64 %next) */
#define llvm_crc32_32_64(res, current, next)                                                             \
    do {                                                                                                 \
        llvm::Intrinsic::ID id = (unsigned)llvm::Intrinsic::aarch64_crc32cx;                             \
        llvm::Function* fn_crc = llvm::Intrinsic::getDeclaration(mod, id);                               \
        if (fn_crc == NULL) {                                                                            \
            ereport(ERROR,                                                                               \
                (errcode(ERRCODE_LOAD_INTRINSIC_FUNCTION_FAILED),                                        \
                    errmodule(MOD_LLVM),                                                                 \
                    errmsg("Failed to get function Intrinsic::aarch64_crc32cx function!\n")));           \
        }                                                                                                \
        res = builder.CreateCall(fn_crc, {current, next}, "crc32_64");                                   \
    } while (0)

#else /* on X86 platform */

/* its function proto is: i32 @llvm.x86.sse42.crc32.32.8(i32 %a, i8 %b) */
#define llvm_crc32_32_8(res, current, next)                                                                   \
    do {                                                                                                      \
        llvm::Intrinsic::ID id = (unsigned)llvm::Intrinsic::x86_sse42_crc32_32_8;                             \
        llvm::Function* fn_crc = llvm::Intrinsic::getDeclaration(mod, id);                                    \
        if (fn_crc == NULL) {                                                                                 \
            ereport(ERROR,                                                                                    \
                (errcode(ERRCODE_LOAD_INTRINSIC_FUNCTION_FAILED),                                             \
                    errmodule(MOD_LLVM),                                                                      \
                    errmsg("Failed to get function Intrinsic::x86_sse42_crc32_32_8 function!\n")));           \
        }                                                                                                     \
        res = builder.CreateCall(fn_crc, {current, next}, "crc32_8");                                         \
    } while (0)

/* its function proto is: i32 @llvm.x86.sse42.crc32.32.16(i32 %a, i16 %b) */
#define llvm_crc32_32_16(res, current, next)                                                                   \
    do {                                                                                                       \
        llvm::Intrinsic::ID id = (unsigned)llvm::Intrinsic::x86_sse42_crc32_32_16;                             \
        llvm::Function* fn_crc = llvm::Intrinsic::getDeclaration(mod, id);                                     \
        if (fn_crc == NULL) {                                                                                  \
            ereport(ERROR,                                                                                     \
                (errcode(ERRCODE_LOAD_INTRINSIC_FUNCTION_FAILED),                                              \
                    errmodule(MOD_LLVM),                                                                       \
                    errmsg("Failed to get function Intrinsic::x86_sse42_crc32_32_16 function!\n")));           \
        }                                                                                                      \
        res = builder.CreateCall(fn_crc, {current, next}, "crc32_16");                                         \
    } while (0)

/* its function proto is: i32 @llvm.x86.sse42.crc32.32.32(i32 %a, i32 %b) */
#define llvm_crc32_32_32(res, current, next)                                                                   \
    do {                                                                                                       \
        llvm::Intrinsic::ID id = (unsigned)llvm::Intrinsic::x86_sse42_crc32_32_32;                             \
        llvm::Function* fn_crc = llvm::Intrinsic::getDeclaration(mod, id);                                     \
        if (fn_crc == NULL) {                                                                                  \
            ereport(ERROR,                                                                                     \
                (errcode(ERRCODE_LOAD_INTRINSIC_FUNCTION_FAILED),                                              \
                    errmodule(MOD_LLVM),                                                                       \
                    errmsg("Failed to get function Intrinsic::x86_sse42_crc32_32_32 function!\n")));           \
        }                                                                                                      \
        res = builder.CreateCall(fn_crc, {current, next}, "crc32_32");                                         \
    } while (0)

/*
 * its function proto is: i64 @llvm.x86.sse42.crc32.64.64(i64 %a0, i64 %a1).
 * x86_sse42_crc32_64_64 gets two 64bit inputs, and returns 64bits.
 * So we have to convert its first input param and the result.
 */
#define llvm_crc32_32_64(res, current, next)                                                                   \
    do {                                                                                                       \
        llvm::Intrinsic::ID id = (unsigned)llvm::Intrinsic::x86_sse42_crc32_64_64;                             \
        llvm::Function* fn_crc = llvm::Intrinsic::getDeclaration(mod, id);                                     \
        if (fn_crc == NULL) {                                                                                  \
            ereport(ERROR,                                                                                     \
                (errcode(ERRCODE_LOAD_INTRINSIC_FUNCTION_FAILED),                                              \
                    errmodule(MOD_LLVM),                                                                       \
                    errmsg("Failed to get function Intrinsic::x86_sse42_crc32_64_64 function!\n")));           \
        }                                                                                                      \
        llvm::Value* current_ext = builder.CreateZExt(current, int64Type);                                     \
        res = builder.CreateCall(fn_crc, {current_ext, next}, "crc32_64");                                     \
        res = builder.CreateTrunc(res, int32Type);                                                             \
    } while (0)

#endif /* end of definition of series of llvm::crc32 function */

#endif
