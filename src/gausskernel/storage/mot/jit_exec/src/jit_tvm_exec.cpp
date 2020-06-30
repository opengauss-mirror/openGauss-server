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
 * jit_tvm_exec.cpp
 *    TVM-jitted query execution.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/jit_exec/src/jit_tvm_exec.cpp
 *
 * -------------------------------------------------------------------------
 */

// be careful to include gscodegen.h before anything else to avoid clash with PM definition in datetime.h
#include "global.h"
#include "codegen/gscodegen.h"
#include "postgres.h"
#include "catalog/pg_operator.h"
#include "utils/fmgroids.h"
#include "nodes/parsenodes.h"
#include "storage/ipc.h"
#include "nodes/pg_list.h"
#include "utils/elog.h"
#include "utils/numeric.h"
#include "utils/numeric_gs.h"
#include "catalog/pg_aggregate.h"

#include "mot_internal.h"
#include "storage/mot/jit_exec.h"
#include "jit_common.h"
#include "jit_tvm_exec.h"
#include "jit_tvm.h"
#include "jit_tvm_util.h"
#include "jit_util.h"
#include "jit_plan.h"

#include "mot_engine.h"
#include "utilities.h"
#include "mot_internal.h"
#include "catalog_column_types.h"
#include "mot_error.h"
#include "utilities.h"
#include "mm_session_api.h"

#include <list>
#include <string>
#include <assert.h>

using namespace tvm;

namespace JitExec {
DECLARE_LOGGER(JitTvm, JitExec)

// forward declarations
struct JitTvmCodeGenContext;
static Expression* ProcessExpr(JitTvmCodeGenContext* ctx, Instruction* row, JitExpr* expr, int* max_arg);
static Expression* ProcessFilterExpr(JitTvmCodeGenContext* ctx, Instruction* row, JitFilter* filter, int* max_arg);

/** @struct Holds instructions that evaluate in runtime to begin and end iterators of a cursor. */
struct JitTvmRuntimeCursor {
    /** @var The iterator pointing to the beginning of the range. */
    Instruction* begin_itr;

    /** @var The iterator pointing to the end of the range. */
    Instruction* end_itr;
};

/** @struct Context used for compiling tvm-jitted functions. */
struct JitTvmCodeGenContext {
    /** @var Main table info. */
    TableInfo _table_info;

    /** @var Inner table info (in JOIN queries). */
    TableInfo m_innerTable_info;

    /** @var The builder used for emitting code. */
    Builder* _builder;

    /** @var The resulting jitted function. */
    Function* m_jittedQuery;
};

/** @var The NULL datum. */
static const Datum _null_datum = PointerGetDatum(nullptr);

/** @brief Initializes a context for compilation. */
static bool InitCodeGenContext(JitTvmCodeGenContext* ctx, Builder* builder, MOT::Table* table, MOT::Index* index,
    MOT::Table* inner_table = nullptr, MOT::Index* inner_index = nullptr)
{
    errno_t erc = memset_s(ctx, sizeof(JitTvmCodeGenContext), 0, sizeof(JitTvmCodeGenContext));
    securec_check(erc, "\0", "\0");
    ctx->_builder = builder;
    if (!InitTableInfo(&ctx->_table_info, table, index)) {
        MOT_REPORT_ERROR(
            MOT_ERROR_OOM, "JIT Compile", "Failed to initialize table information for code-generation context");
        return false;
    }
    if (inner_table && !InitTableInfo(&ctx->m_innerTable_info, inner_table, inner_index)) {
        DestroyTableInfo(&ctx->_table_info);
        MOT_REPORT_ERROR(MOT_ERROR_OOM,
            "JIT Compile",
            "Failed to initialize inner-scan table information for code-generation context");
        return false;
    }

    return true;
}

/** @brief Destroys a compilation context. */
static void DestroyCodeGenContext(JitTvmCodeGenContext* ctx)
{
    if (ctx != nullptr) {
        DestroyTableInfo(&ctx->_table_info);
        DestroyTableInfo(&ctx->m_innerTable_info);
    }
}

/** @brief Gets a key from the execution context. */
static MOT::Key* getExecContextKey(
    ExecContext* exec_context, JitRangeIteratorType range_itr_type, JitRangeScanType range_scan_type)
{
    MOT::Key* key = nullptr;
    if (range_scan_type == JIT_RANGE_SCAN_INNER) {
        if (range_itr_type == JIT_RANGE_ITERATOR_END) {
            key = exec_context->_jit_context->m_innerEndIteratorKey;
        } else {
            key = exec_context->_jit_context->m_innerSearchKey;
        }
    } else {
        if (range_itr_type == JIT_RANGE_ITERATOR_END) {
            key = exec_context->_jit_context->m_endIteratorKey;
        } else {
            key = exec_context->_jit_context->m_searchKey;
        }
    }
    return key;
}

/** @class VarExpression */
class VarExpression : public Expression {
public:
    VarExpression(MOT::Table* table, Instruction* row_inst, int table_colid, int arg_pos)
        : Expression(Expression::CannotFail),
          _table(table),
          _row_inst(row_inst),
          _table_colid(table_colid),
          _arg_pos(arg_pos)
    {}

    ~VarExpression() final
    {
        _table = nullptr;
        _row_inst = nullptr;
    }

    Datum eval(ExecContext* exec_context) final
    {
        exec_context->_expr_rc = MOT_NO_ERROR;
        MOT::Row* row = (MOT::Row*)_row_inst->exec(exec_context);
        return readDatumColumn(_table, row, _table_colid, _arg_pos);
    }

    void dump() final
    {
        (void)fprintf(stderr, "readDatumColumn(%%table, %%row=");
        _row_inst->dump();
        (void)fprintf(stderr, ", table_colid=%d, arg_pos=%d)", _table_colid, _arg_pos);
    }

private:
    MOT::Table* _table;
    Instruction* _row_inst;
    int _table_colid;  // already zero-based
    int _arg_pos;
};

/** @class UnaryOperator */
class UnaryOperator : public Expression {
public:
    Datum eval(ExecContext* exec_context) final
    {
        Datum result = _null_datum;
        Datum arg = _sub_expr->eval(exec_context);
        if (exec_context->_expr_rc == MOT_NO_ERROR) {
            result = evalOperator(exec_context, arg);
        }
        return result;
    }

    void dump() final
    {
        (void)fprintf(stderr, "%s(", _name.c_str());
        _sub_expr->dump();
        (void)fprintf(stderr, ")");
    }

protected:
    UnaryOperator(const char* name, Expression* sub_expr)
        : Expression(Expression::CanFail), _name(name), _sub_expr(sub_expr)
    {}

    ~UnaryOperator() noexcept override
    {
        MOT_LOG_DEBUG("Deleting sub-expr %p in unary operator %s at %p", _sub_expr, _name.c_str(), this);
        delete _sub_expr;
    }

    virtual Datum evalOperator(ExecContext* exec_context, Datum arg) = 0;

private:
    std::string _name;
    Expression* _sub_expr;
};

/** @class UnaryCastOperator */
class UnaryCastOperator : public UnaryOperator {
protected:
    UnaryCastOperator(const char* name, Expression* sub_expr, int arg_pos)
        : UnaryOperator(name, sub_expr), _arg_pos(arg_pos)
    {}

    ~UnaryCastOperator() override
    {}

    virtual Datum evalOperator(ExecContext* exec_context, Datum arg)
    {
        Datum result = _null_datum;
        exec_context->_expr_rc = MOT_NO_ERROR;
        int isnull = getExprArgIsNull(_arg_pos);
        if (!isnull) {
            result = evalCastOperator(exec_context, arg);
        }
        return result;
    }

    virtual Datum evalCastOperator(ExecContext* exec_context, Datum arg) = 0;

private:
    int _arg_pos;
};

/** @class BinaryOperator */
class BinaryOperator : public Expression {
public:
    Datum eval(ExecContext* exec_context) override
    {
        Datum result = _null_datum;
        Datum lhs_arg = _lhs_sub_expr->eval(exec_context);
        if (exec_context->_expr_rc == MOT_NO_ERROR) {
            Datum rhs_arg = _rhs_sub_expr->eval(exec_context);
            if (exec_context->_expr_rc == MOT_NO_ERROR) {
                result = evalOperator(exec_context, lhs_arg, rhs_arg);
            }
        }
        return result;
    }

    void dump() override
    {
        (void)fprintf(stderr, "%s(", _name.c_str());
        _lhs_sub_expr->dump();
        (void)fprintf(stderr, ", ");
        _rhs_sub_expr->dump();
        (void)fprintf(stderr, ")");
    }

protected:
    BinaryOperator(const char* name, Expression* lhs_sub_expr, Expression* rhs_sub_expr)
        : Expression(Expression::CanFail), _name(name), _lhs_sub_expr(lhs_sub_expr), _rhs_sub_expr(rhs_sub_expr)
    {}

    ~BinaryOperator() noexcept override
    {
        MOT_LOG_DEBUG("Deleting lhs-sub-expr %p and rhs-sub-expr %p in binary operator %s at %p",
            _lhs_sub_expr,
            _rhs_sub_expr,
            _name.c_str(),
            this);
        delete _lhs_sub_expr;
        delete _rhs_sub_expr;
    }

    virtual Datum evalOperator(ExecContext* exec_context, Datum lhs_arg, Datum rhs_arg) = 0;

private:
    std::string _name;
    Expression* _lhs_sub_expr;
    Expression* _rhs_sub_expr;
};

/** @class BinaryCastOperator */
class BinaryCastOperator : public BinaryOperator {
protected:
    BinaryCastOperator(const char* name, Expression* lhs_sub_expr, Expression* rhs_sub_expr, int arg_pos)
        : BinaryOperator(name, lhs_sub_expr, rhs_sub_expr), _arg_pos(arg_pos)
    {}

    ~BinaryCastOperator() override
    {}

    Datum evalOperator(ExecContext* exec_context, Datum lhs_arg, Datum rhs_arg) override
    {
        Datum result = _null_datum;
        exec_context->_expr_rc = MOT_NO_ERROR;
        int isnull = getExprArgIsNull(_arg_pos);
        if (!isnull) {
            result = evalCastOperator(exec_context, lhs_arg, rhs_arg);
        }
        return result;
    }

    virtual Datum evalCastOperator(ExecContext* exec_context, Datum lhs_arg, Datum rhs_arg) = 0;

private:
    int _arg_pos;
};

/** @class TernaryOperator */
class TernaryOperator : public Expression {
public:
    Datum eval(ExecContext* exec_context) override
    {
        Datum result = _null_datum;
        exec_context->_expr_rc = MOT_NO_ERROR;
        Datum arg1 = _sub_expr1->eval(exec_context);
        if (exec_context->_expr_rc == MOT_NO_ERROR) {
            Datum arg2 = _sub_expr2->eval(exec_context);
            if (exec_context->_expr_rc == MOT_NO_ERROR) {
                Datum arg3 = _sub_expr3->eval(exec_context);
                if (exec_context->_expr_rc == MOT_NO_ERROR) {
                    result = evalOperator(exec_context, arg1, arg2, arg3);
                }
            }
        }
        return result;
    }

    void dump() override
    {
        (void)fprintf(stderr, "%s(", _name.c_str());
        _sub_expr1->dump();
        (void)fprintf(stderr, ", ");
        _sub_expr2->dump();
        (void)fprintf(stderr, ", ");
        _sub_expr3->dump();
        (void)fprintf(stderr, ")");
    }

protected:
    TernaryOperator(const char* name, Expression* sub_expr1, Expression* sub_expr2, Expression* sub_expr3)
        : Expression(Expression::CanFail),
          _name(name),
          _sub_expr1(sub_expr1),
          _sub_expr2(sub_expr2),
          _sub_expr3(sub_expr3)
    {}

    ~TernaryOperator() noexcept override
    {
        MOT_LOG_DEBUG("Deleting sub-expr1 %p, sub-expr2 %p and sub-expr3 %p in ternary operator %s at %p",
            _sub_expr1,
            _sub_expr2,
            _sub_expr3,
            _name.c_str(),
            this);
        delete _sub_expr1;
        delete _sub_expr2;
        delete _sub_expr3;
    }

    virtual Datum evalOperator(ExecContext* exec_context, Datum arg1, Datum arg2, Datum arg3) = 0;

private:
    std::string _name;
    Expression* _sub_expr1;
    Expression* _sub_expr2;
    Expression* _sub_expr3;
};

/** @class TernaryCastOperator */
class TernaryCastOperator : public TernaryOperator {
protected:
    TernaryCastOperator(
        const char* name, Expression* sub_expr1, Expression* sub_expr2, Expression* sub_expr3, int arg_pos)
        : TernaryOperator(name, sub_expr1, sub_expr2, sub_expr3), _arg_pos(arg_pos)
    {}

    ~TernaryCastOperator() override
    {}

    Datum evalOperator(ExecContext* exec_context, Datum arg1, Datum arg2, Datum arg3) override
    {
        Datum result = _null_datum;
        exec_context->_expr_rc = MOT_NO_ERROR;
        int isnull = getExprArgIsNull(_arg_pos);
        if (!isnull) {
            result = evalCastOperator(exec_context, arg1, arg2, arg3);
        }
        return result;
    }

    virtual Datum evalCastOperator(ExecContext* exec_context, Datum arg1, Datum arg2, Datum arg3) = 0;

private:
    int _arg_pos;
};

/** @define Invoke PG operator with 1 argument. */
#define INVOKE_OPERATOR1(funcid, name, arg)                   \
    PG_TRY();                                                 \
    {                                                         \
        result = DirectFunctionCall1(name, arg);              \
    }                                                         \
    PG_CATCH();                                               \
    {                                                         \
        MOT_REPORT_ERROR(MOT_ERROR_SYSTEM_FAILURE,            \
            "Execute JIT",                                    \
            "Failed to execute PG operator %s (proc-id: %u)", \
            #name,                                            \
            (unsigned)funcid);                                \
        exec_context->_expr_rc = MOT_ERROR_SYSTEM_FAILURE;    \
    }                                                         \
    PG_END_TRY();

/** @define Invoke PG operator with 2 arguments. */
#define INVOKE_OPERATOR2(funcid, name, arg1, arg2)            \
    PG_TRY();                                                 \
    {                                                         \
        result = DirectFunctionCall2(name, arg1, arg2);       \
    }                                                         \
    PG_CATCH();                                               \
    {                                                         \
        MOT_REPORT_ERROR(MOT_ERROR_SYSTEM_FAILURE,            \
            "Execute JIT",                                    \
            "Failed to execute PG operator %s (proc-id: %u)", \
            #name,                                            \
            (unsigned)funcid);                                \
        exec_context->_expr_rc = MOT_ERROR_SYSTEM_FAILURE;    \
    }                                                         \
    PG_END_TRY();

/** @define Invoke PG operator with 3 arguments. */
#define INVOKE_OPERATOR3(funcid, name, arg1, arg2, arg3)      \
    PG_TRY();                                                 \
    {                                                         \
        result = DirectFunctionCall3(name, arg1, arg2, arg3); \
    }                                                         \
    PG_CATCH();                                               \
    {                                                         \
        MOT_REPORT_ERROR(MOT_ERROR_SYSTEM_FAILURE,            \
            "Execute JIT",                                    \
            "Failed to execute PG operator %s (proc-id: %u)", \
            #name,                                            \
            (unsigned)funcid);                                \
        exec_context->_expr_rc = MOT_ERROR_SYSTEM_FAILURE;    \
    }                                                         \
    PG_END_TRY();

#define APPLY_UNARY_OPERATOR(funcid, name)                                    \
    class name##Operator : public UnaryOperator {                             \
    public:                                                                   \
        name##Operator(Expression* sub_expr) : UnaryOperator(#name, sub_expr) \
        {}                                                                    \
        virtual ~name##Operator()                                             \
        {}                                                                    \
                                                                              \
    protected:                                                                \
        virtual Datum evalOperator(ExecContext* exec_context, Datum arg)      \
        {                                                                     \
            Datum result = _null_datum;                                       \
            exec_context->_expr_rc = MOT_NO_ERROR;                            \
            MOT_LOG_DEBUG("Invoking unary operator: " #name);                 \
            INVOKE_OPERATOR1(funcid, name, arg);                              \
            return result;                                                    \
        }                                                                     \
    };

#define APPLY_UNARY_CAST_OPERATOR(funcid, name)                                                         \
    class name##Operator : public UnaryCastOperator {                                                   \
    public:                                                                                             \
        name##Operator(Expression* sub_expr, int arg_pos) : UnaryCastOperator(#name, sub_expr, arg_pos) \
        {}                                                                                              \
        virtual ~name##Operator()                                                                       \
        {}                                                                                              \
                                                                                                        \
    protected:                                                                                          \
        virtual Datum evalCastOperator(ExecContext* exec_context, Datum arg)                            \
        {                                                                                               \
            Datum result = _null_datum;                                                                 \
            exec_context->_expr_rc = MOT_NO_ERROR;                                                      \
            MOT_LOG_DEBUG("Invoking unary cast operator: " #name);                                      \
            INVOKE_OPERATOR1(funcid, name, arg);                                                        \
            return result;                                                                              \
        }                                                                                               \
    };

#define APPLY_BINARY_OPERATOR(funcid, name)                                                 \
    class name##Operator : public BinaryOperator {                                          \
    public:                                                                                 \
        name##Operator(Expression* lhs_sub_expr, Expression* rhs_sub_expr)                  \
            : BinaryOperator(#name, lhs_sub_expr, rhs_sub_expr)                             \
        {}                                                                                  \
        virtual ~name##Operator()                                                           \
        {}                                                                                  \
                                                                                            \
    protected:                                                                              \
        virtual Datum evalOperator(ExecContext* exec_context, Datum lhs_arg, Datum rhs_arg) \
        {                                                                                   \
            Datum result = _null_datum;                                                     \
            exec_context->_expr_rc = MOT_NO_ERROR;                                          \
            MOT_LOG_DEBUG("Invoking binary operator: " #name);                              \
            INVOKE_OPERATOR2(funcid, name, lhs_arg, rhs_arg);                               \
            return result;                                                                  \
        }                                                                                   \
    };

#define APPLY_BINARY_CAST_OPERATOR(funcid, name)                                                \
    class name##Operator : public BinaryCastOperator {                                          \
    public:                                                                                     \
        name##Operator(Expression* lhs_sub_expr, Expression* rhs_sub_expr, int arg_pos)         \
            : BinaryCastOperator(#name, lhs_sub_expr, rhs_sub_expr, arg_pos)                    \
        {}                                                                                      \
        virtual ~name##Operator()                                                               \
        {}                                                                                      \
                                                                                                \
    protected:                                                                                  \
        virtual Datum evalCastOperator(ExecContext* exec_context, Datum lhs_arg, Datum rhs_arg) \
        {                                                                                       \
            Datum result = _null_datum;                                                         \
            exec_context->_expr_rc = MOT_NO_ERROR;                                              \
            MOT_LOG_DEBUG("Invoking binary cast operator: " #name);                             \
            INVOKE_OPERATOR2(funcid, name, lhs_arg, rhs_arg);                                   \
            return result;                                                                      \
        }                                                                                       \
    };

#define APPLY_TERNARY_OPERATOR(funcid, name)                                                      \
    class name##Operator : public TernaryOperator {                                               \
    public:                                                                                       \
        name##Operator(Expression* sub_expr1, Expression* sub_expr2, Expression* sub_expr3)       \
            : TernaryOperator(#name, sub_expr1, sub_expr2, sub_expr3)                             \
        {}                                                                                        \
        virtual ~name##Operator()                                                                 \
        {}                                                                                        \
                                                                                                  \
    protected:                                                                                    \
        virtual Datum evalOperator(ExecContext* exec_context, Datum arg1, Datum arg2, Datum arg3) \
        {                                                                                         \
            Datum result = _null_datum;                                                           \
            exec_context->_expr_rc = MOT_NO_ERROR;                                                \
            MOT_LOG_DEBUG("Invoking ternary operator: " #name);                                   \
            INVOKE_OPERATOR3(funcid, name, arg1, arg2, arg3);                                     \
            return result;                                                                        \
        }                                                                                         \
    };

#define APPLY_TERNARY_CAST_OPERATOR(funcid, name)                                                        \
    class name##Operator : public TernaryCastOperator {                                                  \
    public:                                                                                              \
        name##Operator(Expression* sub_expr1, Expression* sub_expr2, Expression* sub_expr3, int arg_pos) \
            : TernaryCastOperator(#name, sub_expr1, sub_expr2, sub_expr3, arg_pos)                       \
        {}                                                                                               \
        virtual ~name##Operator()                                                                        \
        {}                                                                                               \
                                                                                                         \
    protected:                                                                                           \
        virtual Datum evalCastOperator(ExecContext* exec_context, Datum arg1, Datum arg2, Datum arg3)    \
        {                                                                                                \
            Datum result = _null_datum;                                                                  \
            exec_context->_expr_rc = MOT_NO_ERROR;                                                       \
            MOT_LOG_DEBUG("Invoking ternary cast operator: " #name);                                     \
            INVOKE_OPERATOR3(funcid, name, arg1, arg2, arg3);                                            \
            return result;                                                                               \
        }                                                                                                \
    };

APPLY_OPERATORS()

#undef APPLY_UNARY_OPERATOR
#undef APPLY_BINARY_OPERATOR
#undef APPLY_TERNARY_OPERATOR
#undef APPLY_UNARY_CAST_OPERATOR
#undef APPLY_BINARY_CAST_OPERATOR
#undef APPLY_TERNARY_CAST_OPERATOR

/** @class IsSoftMemoryLimitReachedInstruction */
class IsSoftMemoryLimitReachedInstruction : public Instruction {
public:
    IsSoftMemoryLimitReachedInstruction()
    {}

    ~IsSoftMemoryLimitReachedInstruction() final
    {}

protected:
    uint64_t execImpl(ExecContext* exec_context) final
    {
        return (uint64_t)isSoftMemoryLimitReached();
    }

    void dumpImpl() final
    {
        (void)fprintf(stderr, "isSoftMemoryLimitReached()");
    }
};

/** InitSearchKeyInstruction */
class InitSearchKeyInstruction : public Instruction {
public:
    explicit InitSearchKeyInstruction(JitRangeScanType range_scan_type)
        : Instruction(Instruction::Void), _range_scan_type(range_scan_type)
    {}

    ~InitSearchKeyInstruction() final
    {}

    uint64_t exec(ExecContext* exec_context) final
    {
        if (_range_scan_type == JIT_RANGE_SCAN_INNER) {
            InitKey(exec_context->_jit_context->m_innerSearchKey, exec_context->_jit_context->m_innerIndex);
        } else {
            InitKey(exec_context->_jit_context->m_searchKey, exec_context->_jit_context->m_index);
        }
        return (uint64_t)MOT::RC_OK;
    }

    void dump() final
    {
        if (_range_scan_type == JIT_RANGE_SCAN_INNER) {
            (void)fprintf(stderr, "InitKey(%%inner_search_key, %%inner_index)");
        } else {
            (void)fprintf(stderr, "InitKey(%%search_key, %%index)");
        }
    }

private:
    JitRangeScanType _range_scan_type;
};

/** GetColumnAtInstruction */
class GetColumnAtInstruction : public Instruction {
public:
    GetColumnAtInstruction(int table_colid, JitRangeScanType range_scan_type)
        : _table_colid(table_colid), _range_scan_type(range_scan_type)
    {}

    ~GetColumnAtInstruction() final
    {}

protected:
    uint64_t execImpl(ExecContext* exec_context) final
    {
        MOT::Column* column = nullptr;
        if (_range_scan_type == JIT_RANGE_SCAN_INNER) {
            column = getColumnAt(exec_context->_jit_context->m_innerTable, _table_colid);
        } else {
            column = getColumnAt(exec_context->_jit_context->m_table, _table_colid);
        }
        return (uint64_t)column;
    }

    void dumpImpl() final
    {
        if (_range_scan_type == JIT_RANGE_SCAN_INNER) {
            (void)fprintf(stderr, "%%column = %%inner_table->columns[%d]", _table_colid);
        } else {
            (void)fprintf(stderr, "%%column = %%table->columns[%d]", _table_colid);
        }
    }

private:
    int _table_colid;
    JitRangeScanType _range_scan_type;
};

/** SetExprArgIsNullInstruction */
class SetExprArgIsNullInstruction : public Instruction {
public:
    SetExprArgIsNullInstruction(int arg_pos, int is_null)
        : Instruction(Instruction::Void), _arg_pos(arg_pos), _is_null(is_null)
    {}

    ~SetExprArgIsNullInstruction() final
    {}

    uint64_t exec(ExecContext* exec_context) final
    {
        setExprArgIsNull(_arg_pos, _is_null);
        return (uint64_t)MOT::RC_OK;
    }

    void dump() final
    {
        (void)fprintf(stderr, "setExprArgIsNull(%d, %d)", _arg_pos, _is_null);
    }

private:
    int _arg_pos;
    int _is_null;
};

/** GetExprArgIsNullInstruction */
class GetExprArgIsNullInstruction : public Instruction {
public:
    explicit GetExprArgIsNullInstruction(int arg_pos) : _arg_pos(arg_pos)
    {}

    ~GetExprArgIsNullInstruction() final
    {}

protected:
    uint64_t execImpl(ExecContext* exec_context) final
    {
        return (uint64_t)getExprArgIsNull(_arg_pos);
    }

    void dumpImpl() final
    {
        (void)fprintf(stderr, "getExprArgIsNull(%d)", _arg_pos);
    }

private:
    int _arg_pos;
};

/** WriteDatumColumnInstruction */
class WriteDatumColumnInstruction : public Instruction {
public:
    WriteDatumColumnInstruction(Instruction* row_inst, Instruction* column_inst, Instruction* sub_expr)
        : Instruction(Instruction::Void), _row_inst(row_inst), _column_inst(column_inst), _sub_expr(sub_expr)
    {
        addSubInstruction(row_inst);
        addSubInstruction(column_inst);
        addSubInstruction(sub_expr);
    }

    ~WriteDatumColumnInstruction() final
    {
        _row_inst = nullptr;
        _column_inst = nullptr;
    }

    uint64_t exec(ExecContext* exec_context) final
    {
        Datum arg = (Datum)_sub_expr->exec(exec_context);
        MOT::Row* row = (MOT::Row*)_row_inst->exec(exec_context);
        MOT::Column* column = (MOT::Column*)_column_inst->exec(exec_context);
        writeDatumColumn(row, column, arg);
        return (uint64_t)MOT::RC_OK;
    }

    void dump() final
    {
        (void)fprintf(stderr, "writeDatumColumn(%%row=");
        _row_inst->dump();
        (void)fprintf(stderr, ", %%column=");
        _column_inst->dump();
        (void)fprintf(stderr, ", ");
        _sub_expr->dump();
        (void)fprintf(stderr, ")");
    }

private:
    Instruction* _row_inst;
    Instruction* _column_inst;
    Instruction* _sub_expr;
};

/** BuildDatumKeyInstruction */
class BuildDatumKeyInstruction : public Instruction {
public:
    BuildDatumKeyInstruction(Instruction* column_inst, Instruction* sub_expr, int index_colid, int offset, int size,
        int value_type, JitRangeIteratorType range_itr_type, JitRangeScanType range_scan_type)
        : Instruction(Instruction::Void),
          _column_inst(column_inst),
          _sub_expr(sub_expr),
          _index_colid(index_colid),
          _offset(offset),
          _size(size),
          _value_type(value_type),
          _range_itr_type(range_itr_type),
          _range_scan_type(range_scan_type)
    {
        addSubInstruction(_column_inst);
        addSubInstruction(_sub_expr);
    }

    ~BuildDatumKeyInstruction() final
    {
        _column_inst = nullptr;
        _sub_expr = nullptr;
    }

    uint64_t exec(ExecContext* exec_context) final
    {
        Datum arg = (Datum)_sub_expr->exec(exec_context);
        MOT::Column* column = (MOT::Column*)_column_inst->exec(exec_context);
        MOT::Key* key = getExecContextKey(exec_context, _range_itr_type, _range_scan_type);
        buildDatumKey(column, key, arg, _index_colid, _offset, _size, _value_type);
        return (uint64_t)MOT::RC_OK;
    }

    void dump() final
    {
        (void)fprintf(stderr, "buildDatumKey(%%column=");
        _column_inst->dump();
        if (_range_scan_type == JIT_RANGE_SCAN_INNER) {
            if (_range_itr_type == JIT_RANGE_ITERATOR_END) {
                (void)fprintf(stderr, ", %%innerm_endIteratorKey, ");
            } else {
                (void)fprintf(stderr, ", %%inner_search_key, ");
            }
        } else {
            if (_range_itr_type == JIT_RANGE_ITERATOR_END) {
                (void)fprintf(stderr, ", %%end_iterator_key, ");
            } else {
                (void)fprintf(stderr, ", %%search_key, ");
            }
        }
        _sub_expr->dump();
        (void)fprintf(
            stderr, ", index_colid=%d, offset=%d, size=%d, value_type=%d)", _index_colid, _offset, _size, _value_type);
    }

private:
    Instruction* _column_inst;
    Instruction* _sub_expr;
    int _index_colid;
    int _offset;
    int _size;
    int _value_type;
    JitRangeIteratorType _range_itr_type;
    JitRangeScanType _range_scan_type;
};

/** SetBitInstruction */
class SetBitInstruction : public Instruction {
public:
    explicit SetBitInstruction(int col) : Instruction(Instruction::Void), _col(col)
    {}

    ~SetBitInstruction() final
    {}

    uint64_t exec(ExecContext* exec_context) final
    {
        setBit(exec_context->_jit_context->m_bitmapSet, _col);
        return (uint64_t)MOT::RC_OK;
    }

    void dump() final
    {
        (void)fprintf(stderr, "setBit(%%bitmap_set, col=%d)", _col);
    }

private:
    int _col;
};

/** @class ResetBitmapSetInstruction */
class ResetBitmapSetInstruction : public Instruction {
public:
    ResetBitmapSetInstruction() : Instruction(Instruction::Void)
    {}

    ~ResetBitmapSetInstruction() final
    {}

    uint64_t exec(ExecContext* exec_context) final
    {
        resetBitmapSet(exec_context->_jit_context->m_bitmapSet);
        return (uint64_t)MOT::RC_OK;
    }

    void dump() final
    {
        (void)fprintf(stderr, "resetBitmapSet(%%bitmap_set)");
    }
};

/** @class WriteRowInstruction */
class WriteRowInstruction : public Instruction {
public:
    explicit WriteRowInstruction(Instruction* row_inst) : _row_inst(row_inst)
    {
        addSubInstruction(_row_inst);
    }

    ~WriteRowInstruction() final
    {
        _row_inst = nullptr;
    }

protected:
    uint64_t execImpl(ExecContext* exec_context) final
    {
        MOT::Row* row = (MOT::Row*)_row_inst->exec(exec_context);
        return (uint64_t)writeRow(row, exec_context->_jit_context->m_bitmapSet);
    }

    void dumpImpl() final
    {
        (void)fprintf(stderr, "writeRow(%%row=");
        _row_inst->dump();
        (void)fprintf(stderr, ", %%bitmap_set)");
    }

private:
    Instruction* _row_inst;
};

/** SearchRowInstruction */
class SearchRowInstruction : public Instruction {
public:
    SearchRowInstruction(MOT::AccessType access_mode_value, JitRangeScanType range_scan_type)
        : _access_mode_value(access_mode_value), _range_scan_type(range_scan_type)
    {}

    ~SearchRowInstruction() final
    {}

protected:
    uint64_t execImpl(ExecContext* exec_context) final
    {
        MOT::Row* row = nullptr;
        if (_range_scan_type == JIT_RANGE_SCAN_INNER) {
            row = searchRow(exec_context->_jit_context->m_innerTable,
                exec_context->_jit_context->m_innerSearchKey,
                _access_mode_value);
        } else {
            row = searchRow(
                exec_context->_jit_context->m_table, exec_context->_jit_context->m_searchKey, _access_mode_value);
        }
        return (uint64_t)row;
    }

    void dumpImpl() final
    {
        if (_range_scan_type == JIT_RANGE_SCAN_INNER) {
            (void)fprintf(stderr,
                "%%row = searchRow(%%inner_table, %%inner_search_key, access_mode_value=%d)",
                (int)_access_mode_value);
        } else {
            (void)fprintf(
                stderr, "%%row = searchRow(%%table, %%search_key, access_mode_value=%d)", (int)_access_mode_value);
        }
    }

private:
    MOT::AccessType _access_mode_value;
    JitRangeScanType _range_scan_type;
};

/** @class CreateNewRowInstruction */
class CreateNewRowInstruction : public Instruction {
public:
    CreateNewRowInstruction()
    {}

    ~CreateNewRowInstruction() final
    {}

protected:
    uint64_t execImpl(ExecContext* exec_context) final
    {
        MOT::Row* row = createNewRow(exec_context->_jit_context->m_table);
        return (uint64_t)row;
    }

    void dumpImpl() final
    {
        (void)fprintf(stderr, "%%row = createNewRow(%%table)");
    }
};

/** @class InsertRowInstruction */
class InsertRowInstruction : public Instruction {
public:
    explicit InsertRowInstruction(Instruction* row_inst) : _row_inst(row_inst)
    {
        addSubInstruction(_row_inst);
    }

    ~InsertRowInstruction() final
    {
        _row_inst = nullptr;
    }

protected:
    uint64_t execImpl(ExecContext* exec_context) final
    {
        MOT::Row* row = (MOT::Row*)_row_inst->exec(exec_context);
        return (uint64_t)insertRow(exec_context->_jit_context->m_table, row);
    }

    void dumpImpl() final
    {
        (void)fprintf(stderr, "insertRow(%%table, %%row=");
        _row_inst->dump();
        (void)fprintf(stderr, ")");
    }

private:
    Instruction* _row_inst;
};

/** @class DeleteRowInstruction */
class DeleteRowInstruction : public Instruction {
public:
    DeleteRowInstruction()
    {}
    ~DeleteRowInstruction() final
    {}

protected:
    uint64_t execImpl(ExecContext* exec_context) final
    {
        return (uint64_t)deleteRow();
    }

    void dumpImpl() final
    {
        (void)fprintf(stderr, "deleteRow()");
    }
};

/** @class SetRowNullBitsInstruction */
class SetRowNullBitsInstruction : public Instruction {
public:
    explicit SetRowNullBitsInstruction(Instruction* row_inst) : Instruction(Instruction::Void), _row_inst(row_inst)
    {
        addSubInstruction(_row_inst);
    }

    ~SetRowNullBitsInstruction() final
    {
        _row_inst = nullptr;
    }

    uint64_t exec(ExecContext* exec_context) final
    {
        MOT::Row* row = (MOT::Row*)_row_inst->exec(exec_context);
        setRowNullBits(exec_context->_jit_context->m_table, row);
        return (uint64_t)MOT::RC_OK;
    }

    void dump() final
    {
        (void)fprintf(stderr, "setRowNullBits(%%table, %%row=");
        _row_inst->dump();
        (void)fprintf(stderr, ")");
    }

private:
    Instruction* _row_inst;
};

/** @class SetExprResultNullBitInstruction */
class SetExprResultNullBitInstruction : public Instruction {
public:
    SetExprResultNullBitInstruction(Instruction* row_inst, int table_colid)
        : _row_inst(row_inst), _table_colid(table_colid)
    {
        addSubInstruction(_row_inst);
    }

    ~SetExprResultNullBitInstruction() final
    {
        _row_inst = nullptr;
    }

protected:
    uint64_t execImpl(ExecContext* exec_context) final
    {
        // always performed on main table (there is no inner table in UPDATE command)
        MOT::Row* row = (MOT::Row*)_row_inst->exec(exec_context);
        return (uint64_t)setExprResultNullBit(exec_context->_jit_context->m_table, row, _table_colid);
    }

    void dumpImpl() final
    {
        (void)fprintf(stderr, "setExprResultNullBit(%%table, %%row=");
        _row_inst->dump();
        (void)fprintf(stderr, ", table_colid=%d)", _table_colid);
    }

private:
    Instruction* _row_inst;
    int _table_colid;
};

/** @class ExecClearTupleInstruction */
class ExecClearTupleInstruction : public Instruction {
public:
    ExecClearTupleInstruction() : Instruction(Instruction::Void)
    {}

    ~ExecClearTupleInstruction() final
    {}

    uint64_t exec(ExecContext* exec_context) final
    {
        execClearTuple(exec_context->_slot);
        return (uint64_t)MOT::RC_OK;
    }

    void dump() final
    {
        (void)fprintf(stderr, "execClearTuple(%%slot)");
    }
};

/** @class ExecStoreVirtualTupleInstruction */
class ExecStoreVirtualTupleInstruction : public Instruction {
public:
    ExecStoreVirtualTupleInstruction() : Instruction(Instruction::Void)
    {}

    ~ExecStoreVirtualTupleInstruction() final
    {}

    uint64_t exec(ExecContext* exec_context) final
    {
        execStoreVirtualTuple(exec_context->_slot);
        return (uint64_t)MOT::RC_OK;
    }

    void dump() final
    {
        (void)fprintf(stderr, "execStoreVirtualTuple(%%slot)");
    }
};

/** @class SelectColumnInstruction */
class SelectColumnInstruction : public Instruction {
public:
    SelectColumnInstruction(Instruction* row_inst, int table_colid, int tuple_colid, JitRangeScanType range_scan_type)
        : Instruction(Instruction::Void),
          _row_inst(row_inst),
          _table_colid(table_colid),
          _tuple_colid(tuple_colid),
          _range_scan_type(range_scan_type)
    {
        addSubInstruction(_row_inst);
    }

    ~SelectColumnInstruction() final
    {
        _row_inst = nullptr;
    }

    uint64_t exec(ExecContext* exec_context) final
    {
        MOT::Row* row = (MOT::Row*)_row_inst->exec(exec_context);
        if (_range_scan_type == JIT_RANGE_SCAN_INNER) {
            selectColumn(
                exec_context->_jit_context->m_innerTable, row, exec_context->_slot, _table_colid, _tuple_colid);
        } else {
            selectColumn(exec_context->_jit_context->m_table, row, exec_context->_slot, _table_colid, _tuple_colid);
        }
        return (uint64_t)MOT::RC_OK;
    }

    void dump() final
    {
        if (_range_scan_type == JIT_RANGE_SCAN_INNER) {
            (void)fprintf(stderr, "selectColumn(%%inner_table, %%row=");
        } else {
            (void)fprintf(stderr, "selectColumn(%%table, %%row=");
        }
        _row_inst->dump();
        (void)fprintf(stderr, ", %%slot, table_colid=%d, tuple_colid=%d)", _table_colid, _tuple_colid);
    }

private:
    Instruction* _row_inst;
    int _table_colid;
    int _tuple_colid;
    JitRangeScanType _range_scan_type;
};

/**
 * @class SetTpProcessedInstruction. Sets the number of tuples processed from the number of rows
 * processed. This communicates back to the caller a return value.
 */
class SetTpProcessedInstruction : public Instruction {
public:
    /** @brief Constructor. */
    SetTpProcessedInstruction() : Instruction(Instruction::Void)
    {}

    /** @brief Destructor. */
    ~SetTpProcessedInstruction() final
    {}

    /**
     * @brief Executes the instruction by setting the number of tuples processed.
     * @param exec_context The execution context.
     * @return Not used.
     */
    uint64_t exec(ExecContext* exec_context) final
    {
        setTpProcessed(exec_context->_tp_processed, exec_context->_rows_processed);
        return (uint64_t)MOT::RC_OK;
    }

    /** @brief Dumps the instruction to the standard error stream. */
    void dump() final
    {
        (void)fprintf(stderr, "setTpProcessed(%%tp_processed, %%rows_processed)");
    }
};

/**
 * @class SetScanEndedInstruction. Communicates back to the user whether a range scan has ended
 * (i.e. whether there are no more tuples to report in this query).
 */
class SetScanEndedInstruction : public Instruction {
public:
    /**
     * @brief Constructor.
     * @param result The scan-ended result. Zero means there are more tuples to report. One means the
     * scan ended.
     */
    SetScanEndedInstruction(int result) : Instruction(Instruction::Void), _result(result)
    {}

    /** @brief Destructor. */
    ~SetScanEndedInstruction() final
    {}

    /**
     * @brief Executes the instruction by setting the scan-ended value.
     * @param exec_context The execution context.
     * @return Not used.
     */
    uint64_t exec(ExecContext* exec_context) final
    {
        setScanEnded(exec_context->_scan_ended, _result);
        return (uint64_t)MOT::RC_OK;
    }

    /** @brief Dumps the instruction to the standard error stream. */
    void dump() final
    {
        (void)fprintf(stderr, "setScanEnded(%%scan_ended, %d)", _result);
    }

private:
    /** @var The scan-ended result. Zero means there are more tuples to report. One means the scan ended. */
    int _result;
};

/** @class ResetRowsProcessedInstruction */
class ResetRowsProcessedInstruction : public Instruction {
public:
    ResetRowsProcessedInstruction() : Instruction(Instruction::Void)
    {}

    ~ResetRowsProcessedInstruction() final
    {}

    uint64_t exec(ExecContext* exec_context) final
    {
        exec_context->_rows_processed = 0;
        return (uint64_t)MOT::RC_OK;
    }

    void dump() final
    {
        (void)fprintf(stderr, "%%rows_processed = 0");
    }
};

/** @class IncrementRowsProcessedInstruction */
class IncrementRowsProcessedInstruction : public Instruction {
public:
    IncrementRowsProcessedInstruction() : Instruction(Instruction::Void)
    {}

    ~IncrementRowsProcessedInstruction() final
    {}

    uint64_t exec(ExecContext* exec_context) final
    {
        ++exec_context->_rows_processed;
        return 0;
    }

    void dump() final
    {
        (void)fprintf(stderr, "%%rows_processed += 1");
    }
};

/** @class CopyKeyInstruction */
class CopyKeyInstruction : public Instruction {
public:
    explicit CopyKeyInstruction(JitRangeScanType range_scan_type)
        : Instruction(Instruction::Void), _range_scan_type(range_scan_type)
    {}

    ~CopyKeyInstruction() final
    {}

    uint64_t exec(ExecContext* exec_context) final
    {
        if (_range_scan_type == JIT_RANGE_SCAN_INNER) {
            copyKey(exec_context->_jit_context->m_innerIndex,
                exec_context->_jit_context->m_innerSearchKey,
                exec_context->_jit_context->m_innerEndIteratorKey);
        } else {
            copyKey(exec_context->_jit_context->m_index,
                exec_context->_jit_context->m_searchKey,
                exec_context->_jit_context->m_endIteratorKey);
        }
        return (uint64_t)MOT::RC_OK;
    }

    void dump() final
    {
        if (_range_scan_type == JIT_RANGE_SCAN_INNER) {
            (void)fprintf(stderr, "copyKey(%%inner_index, %%inner_search_key, %%innerm_endIteratorKey)");
        } else {
            (void)fprintf(stderr, "copyKey(%%index, %%search_key, %%end_iterator_key)");
        }
    }

private:
    JitRangeScanType _range_scan_type;
};

/** @class FillKeyPatternInstruction */
class FillKeyPatternInstruction : public Instruction {
public:
    FillKeyPatternInstruction(
        JitRangeIteratorType itr_type, unsigned char pattern, int offset, int size, JitRangeScanType range_scan_type)
        : Instruction(Instruction::Void),
          _itr_type(itr_type),
          _pattern(pattern),
          _offset(offset),
          _size(size),
          _range_scan_type(range_scan_type)
    {}

    ~FillKeyPatternInstruction() final
    {}

    uint64_t exec(ExecContext* exec_context) final
    {
        MOT::Key* key = getExecContextKey(exec_context, _itr_type, _range_scan_type);
        FillKeyPattern(key, _pattern, _offset, _size);
        return (uint64_t)MOT::RC_OK;
    }

    void dump() final
    {
        if (_range_scan_type == JIT_RANGE_SCAN_INNER) {
            if (_itr_type == JIT_RANGE_ITERATOR_END) {
                (void)fprintf(stderr,
                    "FillKeyPattern(%%%s, pattern=0x%X, offset=%d, size=%d)",
                    "innerm_endIteratorKey",
                    (unsigned)_pattern,
                    _offset,
                    _size);
            } else {
                (void)fprintf(stderr,
                    "FillKeyPattern(%%%s, pattern=0x%X, offset=%d, size=%d)",
                    "inner_search_key",
                    (unsigned)_pattern,
                    _offset,
                    _size);
            }
        } else {
            if (_itr_type == JIT_RANGE_ITERATOR_END) {
                (void)fprintf(stderr,
                    "FillKeyPattern(%%%s, pattern=0x%X, offset=%d, size=%d)",
                    "end_iterator_key",
                    (unsigned)_pattern,
                    _offset,
                    _size);
            } else {
                (void)fprintf(stderr,
                    "FillKeyPattern(%%%s, pattern=0x%X, offset=%d, size=%d)",
                    "search_key",
                    (unsigned)_pattern,
                    _offset,
                    _size);
            }
        }
    }

private:
    JitRangeIteratorType _itr_type;
    unsigned char _pattern;
    int _offset;
    int _size;
    JitRangeScanType _range_scan_type;
};

/** @class AdjustKeyInstruction */
class AdjustKeyInstruction : public Instruction {
public:
    AdjustKeyInstruction(JitRangeIteratorType itr_type, unsigned char pattern, JitRangeScanType range_scan_type)
        : Instruction(Instruction::Void), _itr_type(itr_type), _pattern(pattern), _range_scan_type(range_scan_type)
    {}

    ~AdjustKeyInstruction() final
    {}

    uint64_t exec(ExecContext* exec_context) final
    {
        MOT::Key* key = getExecContextKey(exec_context, _itr_type, _range_scan_type);
        MOT::Index* index = (_range_scan_type == JIT_RANGE_SCAN_INNER) ? exec_context->_jit_context->m_innerIndex
                                                                       : exec_context->_jit_context->m_index;
        adjustKey(key, index, _pattern);
        return (uint64_t)MOT::RC_OK;
    }

    void dump() final
    {
        if (_range_scan_type == JIT_RANGE_SCAN_INNER) {
            if (_itr_type == JIT_RANGE_ITERATOR_END) {
                (void)fprintf(stderr,
                    "adjustKey(%%%s, %%inner_index, pattern=0x%X)",
                    "innerm_endIteratorKey",
                    (unsigned)_pattern);
            } else {
                (void)fprintf(
                    stderr, "adjustKey(%%%s, %%inner_index, pattern=0x%X)", "inner_search_key", (unsigned)_pattern);
            }
        } else {
            if (_itr_type == JIT_RANGE_ITERATOR_END) {
                (void)fprintf(stderr, "adjustKey(%%%s, %%index, pattern=0x%X)", "end_iterator_key", (unsigned)_pattern);
            } else {
                (void)fprintf(stderr, "adjustKey(%%%s, %%index, pattern=0x%X)", "search_key", (unsigned)_pattern);
            }
        }
    }

private:
    JitRangeIteratorType _itr_type;
    unsigned char _pattern;
    JitRangeScanType _range_scan_type;
};

/** @class SearchIteratorInstruction */
class SearchIteratorInstruction : public Instruction {
public:
    SearchIteratorInstruction(JitIndexScanDirection index_scan_direction, JitRangeBoundMode range_bound_mode,
        JitRangeScanType range_scan_type)
        : _index_scan_direction(index_scan_direction),
          _range_bound_mode(range_bound_mode),
          _range_scan_type(range_scan_type)
    {}

    ~SearchIteratorInstruction() final
    {}

protected:
    uint64_t execImpl(ExecContext* exec_context) final
    {
        MOT::IndexIterator* iterator = nullptr;
        int forward_scan = (_index_scan_direction == JIT_INDEX_SCAN_FORWARD) ? 1 : 0;
        int include_bound = (_range_bound_mode == JIT_RANGE_BOUND_INCLUDE) ? 1 : 0;
        if (_range_scan_type == JIT_RANGE_SCAN_INNER) {
            iterator = searchIterator(exec_context->_jit_context->m_innerIndex,
                exec_context->_jit_context->m_innerSearchKey,
                forward_scan,
                include_bound);
        } else {
            iterator = searchIterator(exec_context->_jit_context->m_index,
                exec_context->_jit_context->m_searchKey,
                forward_scan,
                include_bound);
        }
        return (uint64_t)iterator;
    }

    void dumpImpl() final
    {
        int forward_scan = (_index_scan_direction == JIT_INDEX_SCAN_FORWARD) ? 1 : 0;
        int include_bound = (_range_bound_mode == JIT_RANGE_BOUND_INCLUDE) ? 1 : 0;
        if (_range_scan_type == JIT_RANGE_SCAN_INNER) {
            (void)fprintf(stderr,
                "searchIterator(%%inner_index, %%inner_search_key, forward_scan=%d, include_bound=%d)",
                forward_scan,
                include_bound);
        } else {
            (void)fprintf(stderr,
                "searchIterator(%%index, %%search_key, forward_scan=%d, include_bound=%d)",
                forward_scan,
                include_bound);
        }
    }

private:
    JitIndexScanDirection _index_scan_direction;
    JitRangeBoundMode _range_bound_mode;
    JitRangeScanType _range_scan_type;
};

/** @class CreateEndIteratorInstruction */
class CreateEndIteratorInstruction : public Instruction {
public:
    CreateEndIteratorInstruction(JitIndexScanDirection index_scan_direction, JitRangeBoundMode range_bound_mode,
        JitRangeScanType range_scan_type)
        : _index_scan_direction(index_scan_direction),
          _range_bound_mode(range_bound_mode),
          _range_scan_type(range_scan_type)
    {}

    ~CreateEndIteratorInstruction() final
    {}

protected:
    uint64_t execImpl(ExecContext* exec_context) final
    {
        MOT::IndexIterator* iterator = nullptr;
        int forward_scan = (_index_scan_direction == JIT_INDEX_SCAN_FORWARD) ? 1 : 0;
        int include_bound = (_range_bound_mode == JIT_RANGE_BOUND_INCLUDE) ? 1 : 0;
        if (_range_scan_type == JIT_RANGE_SCAN_INNER) {
            iterator = createEndIterator(exec_context->_jit_context->m_innerIndex,
                exec_context->_jit_context->m_innerEndIteratorKey,
                forward_scan,
                include_bound);
        } else {
            iterator = createEndIterator(exec_context->_jit_context->m_index,
                exec_context->_jit_context->m_endIteratorKey,
                forward_scan,
                include_bound);
        }
        return (uint64_t)iterator;
    }

    void dumpImpl() final
    {
        int forward_scan = (_index_scan_direction == JIT_INDEX_SCAN_FORWARD) ? 1 : 0;
        int include_bound = (_range_bound_mode == JIT_RANGE_BOUND_INCLUDE) ? 1 : 0;
        if (_range_scan_type == JIT_RANGE_SCAN_INNER) {
            (void)fprintf(stderr,
                "createEndIterator(%%inner_index, %%innerm_endIteratorKey, forward_scan=%d, include_bound=%d)",
                forward_scan,
                include_bound);
        } else {
            (void)fprintf(stderr,
                "createEndIterator(%%index, %%end_iterator_key, forward_scan=%d, include_bound=%d)",
                forward_scan,
                include_bound);
        }
    }

private:
    JitIndexScanDirection _index_scan_direction;
    JitRangeBoundMode _range_bound_mode;
    JitRangeScanType _range_scan_type;
};

/** @class IsScanEndInstruction */
class IsScanEndInstruction : public Instruction {
public:
    IsScanEndInstruction(JitIndexScanDirection index_scan_direction, Instruction* begin_itr_inst,
        Instruction* end_itr_inst, JitRangeScanType range_scan_type)
        : _index_scan_direction(index_scan_direction),
          _begin_itr_inst(begin_itr_inst),
          _end_itr_inst(end_itr_inst),
          _range_scan_type(range_scan_type)
    {
        addSubInstruction(_begin_itr_inst);
        addSubInstruction(_end_itr_inst);
    }

    ~IsScanEndInstruction() final
    {
        _begin_itr_inst = nullptr;
        _end_itr_inst = nullptr;
    }

protected:
    uint64_t execImpl(ExecContext* exec_context) final
    {
        uint64_t result = 0;
        MOT::IndexIterator* begin_itr = (MOT::IndexIterator*)_begin_itr_inst->exec(exec_context);
        MOT::IndexIterator* end_itr = (MOT::IndexIterator*)_end_itr_inst->exec(exec_context);
        int forward_scan = (_index_scan_direction == JIT_INDEX_SCAN_FORWARD) ? 1 : 0;
        if (_range_scan_type == JIT_RANGE_SCAN_INNER) {
            result = isScanEnd(exec_context->_jit_context->m_innerIndex, begin_itr, end_itr, forward_scan);
        } else {
            result = isScanEnd(exec_context->_jit_context->m_index, begin_itr, end_itr, forward_scan);
        }
        return result;
    }

    void dumpImpl() final
    {
        int forward_scan = (_index_scan_direction == JIT_INDEX_SCAN_FORWARD) ? 1 : 0;
        if (_range_scan_type == JIT_RANGE_SCAN_INNER) {
            (void)fprintf(stderr, "isScanEnd(%%inner_index, %%iterator=");
        } else {
            (void)fprintf(stderr, "isScanEnd(%%index, %%iterator=");
        }
        _begin_itr_inst->dump();
        (void)fprintf(stderr, ", %%end_iterator=");
        _end_itr_inst->dump();
        (void)fprintf(stderr, ", forward_scan=%d)", forward_scan);
    }

private:
    JitIndexScanDirection _index_scan_direction;
    Instruction* _begin_itr_inst;
    Instruction* _end_itr_inst;
    JitRangeScanType _range_scan_type;
};

/** @class GetRowFromIteratorInstruction */
class GetRowFromIteratorInstruction : public Instruction {
public:
    GetRowFromIteratorInstruction(MOT::AccessType access_mode, JitIndexScanDirection index_scan_direction,
        Instruction* begin_itr_inst, Instruction* end_itr_inst, JitRangeScanType range_scan_type)
        : _access_mode(access_mode),
          _index_scan_direction(index_scan_direction),
          _begin_itr_inst(begin_itr_inst),
          _end_itr_inst(end_itr_inst),
          _range_scan_type(range_scan_type)
    {
        addSubInstruction(_begin_itr_inst);
        addSubInstruction(_end_itr_inst);
    }

    ~GetRowFromIteratorInstruction() final
    {
        _begin_itr_inst = nullptr;
        _end_itr_inst = nullptr;
    }

protected:
    uint64_t execImpl(ExecContext* exec_context) final
    {
        MOT::Row* row = nullptr;
        MOT::IndexIterator* begin_itr = (MOT::IndexIterator*)_begin_itr_inst->exec(exec_context);
        MOT::IndexIterator* end_itr = (MOT::IndexIterator*)_end_itr_inst->exec(exec_context);
        int forward_scan = (_index_scan_direction == JIT_INDEX_SCAN_FORWARD) ? 1 : 0;
        if (_range_scan_type == JIT_RANGE_SCAN_INNER) {
            row = getRowFromIterator(
                exec_context->_jit_context->m_innerIndex, begin_itr, end_itr, _access_mode, forward_scan);
        } else {
            row =
                getRowFromIterator(exec_context->_jit_context->m_index, begin_itr, end_itr, _access_mode, forward_scan);
        }
        return (uint64_t)row;
    }

    void dumpImpl() final
    {
        int forward_scan = (_index_scan_direction == JIT_INDEX_SCAN_FORWARD) ? 1 : 0;
        if (_range_scan_type == JIT_RANGE_SCAN_INNER) {
            (void)fprintf(stderr, "%%row = getRowFromIterator(%%inner_index, %%iterator=");
        } else {
            (void)fprintf(stderr, "%%row = getRowFromIterator(%%index, %%iterator=");
        }
        _begin_itr_inst->dump();
        (void)fprintf(stderr, ", %%end_iterator=");
        _end_itr_inst->dump();
        (void)fprintf(stderr, ", access_mode=%d, forward_scan=%d)", (int)_access_mode, forward_scan);
    }

private:
    MOT::AccessType _access_mode;
    JitIndexScanDirection _index_scan_direction;
    Instruction* _begin_itr_inst;
    Instruction* _end_itr_inst;
    JitRangeScanType _range_scan_type;
};

/** @class DestroyIteratorInstruction */
class DestroyIteratorInstruction : public Instruction {
public:
    explicit DestroyIteratorInstruction(Instruction* itr_inst) : Instruction(Instruction::Void), _itr_inst(itr_inst)
    {
        addSubInstruction(_itr_inst);
    }

    ~DestroyIteratorInstruction() final
    {
        _itr_inst = nullptr;
    }

    uint64_t exec(ExecContext* exec_context) final
    {
        MOT::IndexIterator* itr = (MOT::IndexIterator*)_itr_inst->exec(exec_context);
        destroyIterator(itr);
        return (uint64_t)MOT::RC_OK;
    }

    void dump() final
    {
        (void)fprintf(stderr, "destroyIterator(%%iterator=");
        _itr_inst->dump();
        (void)fprintf(stderr, ")");
    }

private:
    Instruction* _itr_inst;
};

/** @class GetStateIteratorInstruction */
class SetStateIteratorInstruction : public Instruction {
public:
    SetStateIteratorInstruction(Instruction* itr, JitRangeIteratorType range_itr_type, JitRangeScanType range_scan_type)
        : Instruction(Instruction::Void), _itr(itr), _range_itr_type(range_itr_type), _range_scan_type(range_scan_type)
    {
        addSubInstruction(_itr);
    }

    ~SetStateIteratorInstruction() final
    {
        _itr = nullptr;
    }

    uint64_t exec(ExecContext* exec_context) final
    {
        MOT::IndexIterator* itr = (MOT::IndexIterator*)_itr->exec(exec_context);
        int begin_itr = (_range_itr_type == JIT_RANGE_ITERATOR_START) ? 1 : 0;
        int inner_scan = (_range_scan_type == JIT_RANGE_SCAN_INNER) ? 1 : 0;
        setStateIterator(itr, begin_itr, inner_scan);
        return (uint64_t)MOT::RC_OK;
    }

    void dump() final
    {
        int begin_itr = (_range_itr_type == JIT_RANGE_ITERATOR_START) ? 1 : 0;
        int inner_scan = (_range_scan_type == JIT_RANGE_SCAN_INNER) ? 1 : 0;
        (void)fprintf(stderr, "setStateIterator(");
        _itr->dump();
        (void)fprintf(stderr, ", begin_itr=%d, inner_scan=%d)", begin_itr, inner_scan);
    }

private:
    Instruction* _itr;
    JitRangeIteratorType _range_itr_type;
    JitRangeScanType _range_scan_type;
};

/** @class GetStateIteratorInstruction */
class GetStateIteratorInstruction : public Instruction {
public:
    GetStateIteratorInstruction(JitRangeIteratorType range_itr_type, JitRangeScanType range_scan_type)
        : _range_itr_type(range_itr_type), _range_scan_type(range_scan_type)
    {}

    ~GetStateIteratorInstruction() final
    {}

protected:
    uint64_t execImpl(ExecContext* exec_context) final
    {
        int begin_itr = (_range_itr_type == JIT_RANGE_ITERATOR_START) ? 1 : 0;
        int inner_scan = (_range_scan_type == JIT_RANGE_SCAN_INNER) ? 1 : 0;
        return (uint64_t)getStateIterator(begin_itr, inner_scan);
    }

    void dumpImpl() final
    {
        int begin_itr = (_range_itr_type == JIT_RANGE_ITERATOR_START) ? 1 : 0;
        int inner_scan = (_range_scan_type == JIT_RANGE_SCAN_INNER) ? 1 : 0;
        (void)fprintf(stderr, "getStateIterator(begin_itr=%d, inner_scan=%d)", begin_itr, inner_scan);
    }

private:
    JitRangeIteratorType _range_itr_type;
    JitRangeScanType _range_scan_type;
};

/** @class IsStateIteratorNullInstruction */
class IsStateIteratorNullInstruction : public Instruction {
public:
    IsStateIteratorNullInstruction(JitRangeIteratorType range_itr_type, JitRangeScanType range_scan_type)
        : _range_itr_type(range_itr_type), _range_scan_type(range_scan_type)
    {}

    ~IsStateIteratorNullInstruction() final
    {}

protected:
    uint64_t execImpl(ExecContext* exec_context) final
    {
        int begin_itr = (_range_itr_type == JIT_RANGE_ITERATOR_START) ? 1 : 0;
        int inner_scan = (_range_scan_type == JIT_RANGE_SCAN_INNER) ? 1 : 0;
        return (uint64_t)isStateIteratorNull(begin_itr, inner_scan);
    }

    void dumpImpl() final
    {
        int begin_itr = (_range_itr_type == JIT_RANGE_ITERATOR_START) ? 1 : 0;
        int inner_scan = (_range_scan_type == JIT_RANGE_SCAN_INNER) ? 1 : 0;
        (void)fprintf(stderr, "isStateIteratorNull(begin_itr=%d, inner_scan=%d)", begin_itr, inner_scan);
    }

private:
    JitRangeIteratorType _range_itr_type;
    JitRangeScanType _range_scan_type;
};

/** @class IsStateScanEndInstruction */
class IsStateScanEndInstruction : public Instruction {
public:
    IsStateScanEndInstruction(JitIndexScanDirection index_scan_direction, JitRangeScanType range_scan_type)
        : _index_scan_direction(index_scan_direction), _range_scan_type(range_scan_type)
    {}

    ~IsStateScanEndInstruction() final
    {}

protected:
    uint64_t execImpl(ExecContext* exec_context) final
    {
        int forward_scan = (_index_scan_direction == JIT_INDEX_SCAN_FORWARD) ? 1 : 0;
        int inner_scan = (_range_scan_type == JIT_RANGE_SCAN_INNER) ? 1 : 0;
        return (uint64_t)isStateScanEnd(forward_scan, inner_scan);
    }

    void dumpImpl() final
    {
        int forward_scan = (_index_scan_direction == JIT_INDEX_SCAN_FORWARD) ? 1 : 0;
        int inner_scan = (_range_scan_type == JIT_RANGE_SCAN_INNER) ? 1 : 0;
        (void)fprintf(stderr, "isStateScanEnd(%%index, forward_scan=%d, inner_scan=%d)", forward_scan, inner_scan);
    }

private:
    JitIndexScanDirection _index_scan_direction;
    JitRangeScanType _range_scan_type;
};

/** @class GetRowFromStateIteratorInstruction */
class GetRowFromStateIteratorInstruction : public Instruction {
public:
    GetRowFromStateIteratorInstruction(
        MOT::AccessType access_mode, JitIndexScanDirection index_scan_direction, JitRangeScanType range_scan_type)
        : _access_mode(access_mode), _index_scan_direction(index_scan_direction), _range_scan_type(range_scan_type)
    {}

    ~GetRowFromStateIteratorInstruction() final
    {}

protected:
    uint64_t execImpl(ExecContext* exec_context) final
    {
        int forward_scan = (_index_scan_direction == JIT_INDEX_SCAN_FORWARD) ? 1 : 0;
        int inner_scan = (_range_scan_type == JIT_RANGE_SCAN_INNER) ? 1 : 0;
        MOT::Row* row = getRowFromStateIterator(_access_mode, forward_scan, inner_scan);
        return (uint64_t)row;
    }

    void dumpImpl() final
    {
        int forward_scan = (_index_scan_direction == JIT_INDEX_SCAN_FORWARD) ? 1 : 0;
        int inner_scan = (_range_scan_type == JIT_RANGE_SCAN_INNER) ? 1 : 0;
        (void)fprintf(stderr,
            "%%row = getRowFromStateIterator(access_mode=%d, forward_scan=%d, inner_scan=%d)",
            (int)_access_mode,
            forward_scan,
            inner_scan);
    }

private:
    MOT::AccessType _access_mode;
    JitIndexScanDirection _index_scan_direction;
    JitRangeScanType _range_scan_type;
};

/** @class DestroyStateIteratorsInstruction */
class DestroyStateIteratorsInstruction : public Instruction {
public:
    explicit DestroyStateIteratorsInstruction(JitRangeScanType range_scan_type)
        : Instruction(Instruction::Void), _range_scan_type(range_scan_type)
    {}

    ~DestroyStateIteratorsInstruction() final
    {}

    uint64_t exec(ExecContext* exec_context) final
    {
        int inner_scan = (_range_scan_type == JIT_RANGE_SCAN_INNER) ? 1 : 0;
        destroyStateIterators(inner_scan);
        return (uint64_t)MOT::RC_OK;
    }

    void dump() final
    {
        int inner_scan = (_range_scan_type == JIT_RANGE_SCAN_INNER) ? 1 : 0;
        (void)fprintf(stderr, "destroyStateIterators(inner_scan=%d)", inner_scan);
    }

private:
    JitRangeScanType _range_scan_type;
};

/** @class SetStateScanEndFlagInstruction */
class SetStateScanEndFlagInstruction : public Instruction {
public:
    SetStateScanEndFlagInstruction(int scan_ended, JitRangeScanType range_scan_type)
        : Instruction(Instruction::Void), _scan_ended(scan_ended), _range_scan_type(range_scan_type)
    {}

    ~SetStateScanEndFlagInstruction() final
    {}

    uint64_t exec(ExecContext* exec_context) final
    {
        int inner_scan = (_range_scan_type == JIT_RANGE_SCAN_INNER) ? 1 : 0;
        setStateScanEndFlag(_scan_ended, inner_scan);
        return (uint64_t)MOT::RC_OK;
    }

    void dump() final
    {
        int inner_scan = (_range_scan_type == JIT_RANGE_SCAN_INNER) ? 1 : 0;
        (void)fprintf(stderr, "setStateScanEndFlag(scan_ended=%d, inner_scan=%d)", _scan_ended, inner_scan);
    }

private:
    int _scan_ended;
    JitRangeScanType _range_scan_type;
};

/** @class GetStateScanEndFlagInstruction */
class GetStateScanEndFlagInstruction : public Instruction {
public:
    explicit GetStateScanEndFlagInstruction(JitRangeScanType range_scan_type) : _range_scan_type(range_scan_type)
    {}

    ~GetStateScanEndFlagInstruction()
    {}

protected:
    uint64_t execImpl(ExecContext* exec_context) final
    {
        int inner_scan = (_range_scan_type == JIT_RANGE_SCAN_INNER) ? 1 : 0;
        return (uint64_t)getStateScanEndFlag(inner_scan);
    }

    void dumpImpl() final
    {
        int inner_scan = (_range_scan_type == JIT_RANGE_SCAN_INNER) ? 1 : 0;
        (void)fprintf(stderr, "getStateScanEndFlag(inner_scan=%d)", inner_scan);
    }

private:
    JitRangeScanType _range_scan_type;
};

class ResetStateRowInstruction : public Instruction {
public:
    explicit ResetStateRowInstruction(JitRangeScanType range_scan_type)
        : Instruction(Instruction::Void), _range_scan_type(range_scan_type)
    {}

    ~ResetStateRowInstruction() final
    {}

    uint64_t exec(ExecContext* exec_context) final
    {
        int inner_scan = (_range_scan_type == JIT_RANGE_SCAN_INNER) ? 1 : 0;
        resetStateRow(inner_scan);
        return (uint64_t)MOT::RC_OK;
    }

    void dump() final
    {
        int inner_scan = (_range_scan_type == JIT_RANGE_SCAN_INNER) ? 1 : 0;
        (void)fprintf(stderr, "resetStateRow(inner_scan=%d)", inner_scan);
    }

private:
    JitRangeScanType _range_scan_type;
};

class SetStateRowInstruction : public Instruction {
public:
    SetStateRowInstruction(Instruction* row, JitRangeScanType range_scan_type)
        : Instruction(Instruction::Void), _row(row), _range_scan_type(range_scan_type)
    {
        addSubInstruction(_row);
    }

    ~SetStateRowInstruction() final
    {
        _row = nullptr;
    }

    uint64_t exec(ExecContext* exec_context) final
    {
        int inner_scan = (_range_scan_type == JIT_RANGE_SCAN_INNER) ? 1 : 0;
        MOT::Row* row = (MOT::Row*)_row->exec(exec_context);
        setStateRow(row, inner_scan);
        return (uint64_t)MOT::RC_OK;
    }

    void dump() final
    {
        int inner_scan = (_range_scan_type == JIT_RANGE_SCAN_INNER) ? 1 : 0;
        (void)fprintf(stderr, "setStateRow(row=");
        _row->dump();
        (void)fprintf(stderr, ", inner_scan=%d)", inner_scan);
    }

private:
    Instruction* _row;
    JitRangeScanType _range_scan_type;
};

class GetStateRowInstruction : public Instruction {
public:
    explicit GetStateRowInstruction(JitRangeScanType range_scan_type) : _range_scan_type(range_scan_type)
    {}

    ~GetStateRowInstruction() final
    {}

protected:
    uint64_t execImpl(ExecContext* exec_context) final
    {
        int inner_scan = (_range_scan_type == JIT_RANGE_SCAN_INNER) ? 1 : 0;
        return (uint64_t)getStateRow(inner_scan);
    }

    void dumpImpl() final
    {
        int inner_scan = (_range_scan_type == JIT_RANGE_SCAN_INNER) ? 1 : 0;
        (void)fprintf(stderr, "getStateRow(inner_scan=%d)", inner_scan);
    }

private:
    JitRangeScanType _range_scan_type;
};

class CopyOuterStateRowInstruction : public Instruction {
public:
    CopyOuterStateRowInstruction() : Instruction(Instruction::Void)
    {}

    ~CopyOuterStateRowInstruction() final
    {}

    uint64_t exec(ExecContext* exec_context) final
    {
        copyOuterStateRow();
        return (uint64_t)MOT::RC_OK;
    }

    void dump() final
    {
        (void)fprintf(stderr, "copyOuterStateRow()");
    }
};

class GetOuterStateRowCopyInstruction : public Instruction {
public:
    GetOuterStateRowCopyInstruction()
    {}

    ~GetOuterStateRowCopyInstruction() final
    {}

protected:
    uint64_t execImpl(ExecContext* exec_context) final
    {
        return (uint64_t)getOuterStateRowCopy();
    }

    void dumpImpl() final
    {
        (void)fprintf(stderr, "getOuterStateRowCopy()");
    }
};

class IsStateRowNullInstruction : public Instruction {
public:
    explicit IsStateRowNullInstruction(JitRangeScanType range_scan_type) : _range_scan_type(range_scan_type)
    {}

    ~IsStateRowNullInstruction() final
    {}

protected:
    uint64_t execImpl(ExecContext* exec_context) final
    {
        int inner_scan = (_range_scan_type == JIT_RANGE_SCAN_INNER) ? 1 : 0;
        return (uint64_t)isStateRowNull(inner_scan);
    }

    void dumpImpl() final
    {
        int inner_scan = (_range_scan_type == JIT_RANGE_SCAN_INNER) ? 1 : 0;
        (void)fprintf(stderr, "isStateRowNull(inner_scan=%d)", inner_scan);
    }

private:
    JitRangeScanType _range_scan_type;
};

/** @class ResetStateLimitCounterInstruction */
class ResetStateLimitCounterInstruction : public Instruction {
public:
    ResetStateLimitCounterInstruction() : Instruction(Instruction::Void)
    {}

    ~ResetStateLimitCounterInstruction() final
    {}

    uint64_t exec(ExecContext* exec_context) final
    {
        resetStateLimitCounter();
        return (uint64_t)MOT::RC_OK;
    }

    void dump() final
    {
        (void)fprintf(stderr, "resetStateLimitCounter()");
    }
};

/** @class IncrementStateLimitCounterInstruction */
class IncrementStateLimitCounterInstruction : public Instruction {
public:
    IncrementStateLimitCounterInstruction() : Instruction(Instruction::Void)
    {}

    ~IncrementStateLimitCounterInstruction() final
    {}

    uint64_t exec(ExecContext* exec_context) final
    {
        incrementStateLimitCounter();
        return (uint64_t)MOT::RC_OK;
    }

    void dump() final
    {
        (void)fprintf(stderr, "incrementStateLimitCounter()");
    }
};

/** @class GetStateLimitCounterInstruction */
class GetStateLimitCounterInstruction : public Instruction {
public:
    GetStateLimitCounterInstruction()
    {}

    ~GetStateLimitCounterInstruction() final
    {}

protected:
    uint64_t execImpl(ExecContext* exec_context) final
    {
        uint64_t value = getStateLimitCounter();
        return (uint64_t)value;
    }

    void dumpImpl() final
    {
        (void)fprintf(stderr, "getStateLimitCounter()");
    }
};

/** @class PrepareAvgArrayInstruction */
class PrepareAvgArrayInstruction : public Instruction {
public:
    PrepareAvgArrayInstruction(int element_type, int element_count)
        : Instruction(Instruction::Void), _element_type(element_type), _element_count(element_count)
    {}

    ~PrepareAvgArrayInstruction() final
    {}

    uint64_t exec(ExecContext* exec_context) final
    {
        prepareAvgArray(_element_type, _element_count);
        return (uint64_t)MOT::RC_OK;
    }

    void dump() final
    {
        (void)fprintf(stderr, "prepareAvgArray(element_type=%d, element_count=%d)", _element_type, _element_count);
    }

private:
    int _element_type;
    int _element_count;
};

/** @class LoadAvgArrayExpression */
class LoadAvgArrayExpression : public Expression {
public:
    LoadAvgArrayExpression() : Expression(Expression::CannotFail)
    {}

    ~LoadAvgArrayExpression() final
    {}

    Datum eval(ExecContext* exec_context) final
    {
        return (uint64_t)loadAvgArray();
    }

    void dump() final
    {
        (void)fprintf(stderr, "loadAvgArray()");
    }
};

/** @class SaveAvgArrayInstruction */
class SaveAvgArrayInstruction : public Instruction {
public:
    explicit SaveAvgArrayInstruction(Expression* avg_array) : Instruction(Instruction::Void), _avg_array(avg_array)
    {}

    ~SaveAvgArrayInstruction() final
    {
        _avg_array = nullptr;
    }

    uint64_t exec(ExecContext* exec_context) final
    {
        Datum avg_array = _avg_array->eval(exec_context);
        saveAvgArray(avg_array);
        return (uint64_t)MOT::RC_OK;
    }

    void dump() final
    {
        (void)fprintf(stderr, "saveAvgArray(avg_array=");
        _avg_array->dump();
        (void)fprintf(stderr, ")");
    }

private:
    Expression* _avg_array;
};

/** @class ComputeAvgFromArrayInstruction */
class ComputeAvgFromArrayInstruction : public Instruction {
public:
    explicit ComputeAvgFromArrayInstruction(int element_type) : _element_type(element_type)
    {}

    ~ComputeAvgFromArrayInstruction() final
    {}

protected:
    Datum execImpl(ExecContext* exec_context) final
    {
        return (uint64_t)computeAvgFromArray(_element_type);
    }

    void dumpImpl() final
    {
        (void)fprintf(stderr, "computeAvgFromArray(element_type=%d)", _element_type);
    }

private:
    int _element_type;
};

/** @class ResetAggValueInstruction */
class ResetAggValueInstruction : public Instruction {
public:
    explicit ResetAggValueInstruction(int element_type) : Instruction(Instruction::Void), _element_type(element_type)
    {}

    ~ResetAggValueInstruction() final
    {}

    uint64_t exec(ExecContext* exec_context) final
    {
        resetAggValue(_element_type);
        return (uint64_t)MOT::RC_OK;
    }

    void dump() final
    {
        (void)fprintf(stderr, "resetAggValue(element_type=%d)", _element_type);
    }

private:
    int _element_type;
};

/** @class GetAggValueInstruction */
class GetAggValueExpression : public Expression {
public:
    GetAggValueExpression() : Expression(Expression::CannotFail)
    {}

    ~GetAggValueExpression() final
    {}

    Datum eval(ExecContext* exec_context) final
    {
        return (uint64_t)getAggValue();
    }

    void dump() final
    {
        (void)fprintf(stderr, "getAggValue()");
    }
};

/** @class SetAggValueInstruction */
class SetAggValueInstruction : public Instruction {
public:
    explicit SetAggValueInstruction(Expression* value) : Instruction(Instruction::Void), _value(value)
    {}

    ~SetAggValueInstruction() final
    {
        _value = nullptr;
    }

    Datum exec(ExecContext* exec_context) final
    {
        Datum value = (Datum)_value->eval(exec_context);
        setAggValue(value);
        return (uint64_t)MOT::RC_OK;
    }

    void dump() final
    {
        (void)fprintf(stderr, "setAggValue(value=");
        _value->dump();
        (void)fprintf(stderr, ")");
    }

private:
    Expression* _value;
};

/** @class ResetAggMaxMinNullInstruction */
class ResetAggMaxMinNullInstruction : public Instruction {
public:
    ResetAggMaxMinNullInstruction() : Instruction(Instruction::Void)
    {}

    ~ResetAggMaxMinNullInstruction() final
    {}

    uint64_t exec(ExecContext* exec_context) final
    {
        resetAggMaxMinNull();
        return (uint64_t)MOT::RC_OK;
    }

    void dump() final
    {
        (void)fprintf(stderr, "resetAggMaxMinNull()");
    }
};

/** @class SetAggMaxMinNotNullInstruction */
class SetAggMaxMinNotNullInstruction : public Instruction {
public:
    SetAggMaxMinNotNullInstruction() : Instruction(Instruction::Void)
    {}

    ~SetAggMaxMinNotNullInstruction() final
    {}

    uint64_t exec(ExecContext* exec_context) final
    {
        setAggMaxMinNotNull();
        return (uint64_t)MOT::RC_OK;
    }

    void dump() final
    {
        (void)fprintf(stderr, "setAggMaxMinNotNull()");
    }
};

/** @class GetAggMaxMinIsNullInstruction */
class GetAggMaxMinIsNullInstruction : public Instruction {
public:
    GetAggMaxMinIsNullInstruction()
    {}

    ~GetAggMaxMinIsNullInstruction() final
    {}

protected:
    uint64_t execImpl(ExecContext* exec_context) final
    {
        return (uint64_t)getAggMaxMinIsNull();
    }

    void dumpImpl() final
    {
        (void)fprintf(stderr, "getAggMaxMinIsNull()");
    }
};

/** @class PrepareDistinctSetInstruction */
class PrepareDistinctSetInstruction : public Instruction {
public:
    explicit PrepareDistinctSetInstruction(int element_type)
        : Instruction(Instruction::Void), _element_type(element_type)
    {}

    ~PrepareDistinctSetInstruction() final
    {}

    uint64_t exec(ExecContext* exec_context) final
    {
        prepareDistinctSet(_element_type);
        return (uint64_t)MOT::RC_OK;
    }

    void dump() final
    {
        (void)fprintf(stderr, "prepareDistinctSet(element_type=%d)", _element_type);
    }

private:
    int _element_type;
};

/** @class InsertDistinctItemInstruction */
class InsertDistinctItemInstruction : public Instruction {
public:
    InsertDistinctItemInstruction(int element_type, Expression* value) : _element_type(element_type), _value(value)
    {}

    ~InsertDistinctItemInstruction() final
    {
        _value = nullptr;
    }

protected:
    uint64_t execImpl(ExecContext* exec_context) final
    {
        Datum value = (Datum)_value->eval(exec_context);
        return (uint64_t)insertDistinctItem(_element_type, value);
    }

    void dumpImpl() final
    {
        (void)fprintf(stderr, "insertDistinctItem(element_type=%d, ", _element_type);
        _value->dump();
        (void)fprintf(stderr, ")");
    }

private:
    int _element_type;
    Expression* _value;
};

/** @class DestroyDistinctSetInstruction */
class DestroyDistinctSetInstruction : public Instruction {
public:
    explicit DestroyDistinctSetInstruction(int element_type)
        : Instruction(Instruction::Void), _element_type(element_type)
    {}

    ~DestroyDistinctSetInstruction() final
    {}

    uint64_t exec(ExecContext* exec_context) final
    {
        destroyDistinctSet(_element_type);
        return (uint64_t)MOT::RC_OK;
    }

    void dump() final
    {
        (void)fprintf(stderr, "destroyDistinctSet(element_type=%d)", _element_type);
    }

private:
    int _element_type;
};

/** @class ResetTupleDatumInstruction */
class ResetTupleDatumInstruction : public Instruction {
public:
    ResetTupleDatumInstruction(int tuple_colid, int zero_type)
        : Instruction(Instruction::Void), _tuple_colid(tuple_colid), _zero_type(zero_type)
    {}

    ~ResetTupleDatumInstruction() final
    {}

    uint64_t exec(ExecContext* exec_context) final
    {
        resetTupleDatum(exec_context->_slot, _tuple_colid, _zero_type);
        return (uint64_t)MOT::RC_OK;
    }

    void dump() final
    {
        (void)fprintf(stderr, "resetTupleDatum(%%slot, tuple_colid=%d, zero_type=%d)", _tuple_colid, _zero_type);
    }

private:
    int _tuple_colid;
    int _zero_type;
};

/** @class ReadTupleDatumExpression */
class ReadTupleDatumExpression : public Expression {
public:
    ReadTupleDatumExpression(int tuple_colid, int arg_pos)
        : Expression(Expression::CannotFail), _tuple_colid(tuple_colid), _arg_pos(arg_pos)
    {}

    ~ReadTupleDatumExpression() final
    {}

    Datum eval(ExecContext* exec_context) final
    {
        exec_context->_expr_rc = MOT_NO_ERROR;
        return (uint64_t)readTupleDatum(exec_context->_slot, _tuple_colid, _arg_pos);
    }

    void dump() final
    {
        (void)fprintf(stderr, "readTupleDatum(%%slot, tuple_colid=%d, arg_pos=%d)", _tuple_colid, _arg_pos);
    }

private:
    int _tuple_colid;
    int _arg_pos;
};

/** @class WriteTupleDatumInstruction */
class WriteTupleDatumInstruction : public Instruction {
public:
    WriteTupleDatumInstruction(int tuple_colid, Instruction* value)
        : Instruction(Instruction::Void), _tuple_colid(tuple_colid), _value(value)
    {
        addSubInstruction(_value);
    }

    ~WriteTupleDatumInstruction() final
    {
        _value = nullptr;
    }

    uint64_t exec(ExecContext* exec_context) final
    {
        Datum datum_value = (Datum)_value->exec(exec_context);
        writeTupleDatum(exec_context->_slot, _tuple_colid, datum_value);
        return (uint64_t)MOT::RC_OK;
    }

    void dump() final
    {
        (void)fprintf(stderr, "writeTupleDatum(%%slot, tuple_colid=%d, ", _tuple_colid);
        _value->dump();
        (void)fprintf(stderr, ")");
    }

private:
    int _tuple_colid;
    Instruction* _value;
};

static Instruction* AddIsSoftMemoryLimitReached(JitTvmCodeGenContext* ctx)
{
    return ctx->_builder->addInstruction(new (std::nothrow) IsSoftMemoryLimitReachedInstruction());
}

static void AddInitSearchKey(JitTvmCodeGenContext* ctx, JitRangeScanType range_scan_type)
{
    (void)ctx->_builder->addInstruction(new (std::nothrow) InitSearchKeyInstruction(range_scan_type));
}

static Instruction* AddGetColumnAt(JitTvmCodeGenContext* ctx, int colid, JitRangeScanType range_scan_type)
{
    return ctx->_builder->addInstruction(new (std::nothrow) GetColumnAtInstruction(colid, range_scan_type));
}

static Instruction* AddSetExprArgIsNull(JitTvmCodeGenContext* ctx, int arg_pos, int isnull)
{
    return ctx->_builder->addInstruction(new (std::nothrow) SetExprArgIsNullInstruction(arg_pos, isnull));
}

static Instruction* AddGetExprArgIsNull(JitTvmCodeGenContext* ctx, int arg_pos)
{
    return ctx->_builder->addInstruction(new (std::nothrow) GetExprArgIsNullInstruction(arg_pos));
}

static Expression* AddGetDatumParam(JitTvmCodeGenContext* ctx, int paramid, int arg_pos)
{
    return new (std::nothrow) ParamExpression(paramid, arg_pos);
}

static Expression* AddReadDatumColumn(
    JitTvmCodeGenContext* ctx, MOT::Table* table, Instruction* row_inst, int table_colid, int arg_pos)
{
    return new (std::nothrow) VarExpression(table, row_inst, table_colid, arg_pos);
}

static Instruction* AddWriteDatumColumn(
    JitTvmCodeGenContext* ctx, int table_colid, Instruction* row_inst, Instruction* sub_expr)
{
    // make sure we have a column before issuing the call
    // we always write to a main table row (whether UPDATE or range UPDATE, so inner_scan value below is false)
    Instruction* column = AddGetColumnAt(ctx, table_colid, JIT_RANGE_SCAN_MAIN);
    return ctx->_builder->addInstruction(new (std::nothrow) WriteDatumColumnInstruction(row_inst, column, sub_expr));
}

static void AddBuildDatumKey(JitTvmCodeGenContext* ctx, Instruction* column_inst, int index_colid,
    Instruction* sub_expr, int value_type, JitRangeIteratorType range_itr_type, JitRangeScanType range_scan_type)
{
    int offset = -1;
    int size = -1;
    if (range_scan_type == JIT_RANGE_SCAN_INNER) {
        offset = ctx->m_innerTable_info.m_indexColumnOffsets[index_colid];
        size = ctx->m_innerTable_info.m_index->GetLengthKeyFields()[index_colid];
    } else {
        offset = ctx->_table_info.m_indexColumnOffsets[index_colid];
        size = ctx->_table_info.m_index->GetLengthKeyFields()[index_colid];
    }
    (void)ctx->_builder->addInstruction(new (std::nothrow) BuildDatumKeyInstruction(
        column_inst, sub_expr, index_colid, offset, size, value_type, range_itr_type, range_scan_type));
}

static void AddSetBit(JitTvmCodeGenContext* ctx, int colid)
{
    (void)ctx->_builder->addInstruction(new (std::nothrow) SetBitInstruction(colid));
}

static void AddResetBitmapSet(JitTvmCodeGenContext* ctx)
{
    (void)ctx->_builder->addInstruction(new (std::nothrow) ResetBitmapSetInstruction());
}

static Instruction* AddWriteRow(JitTvmCodeGenContext* ctx, Instruction* row_inst)
{
    return ctx->_builder->addInstruction(new (std::nothrow) WriteRowInstruction(row_inst));
}

static Instruction* AddSearchRow(
    JitTvmCodeGenContext* ctx, MOT::AccessType access_mode_value, JitRangeScanType range_scan_type)
{
    return ctx->_builder->addInstruction(new (std::nothrow) SearchRowInstruction(access_mode_value, range_scan_type));
}

static Instruction* AddCreateNewRow(JitTvmCodeGenContext* ctx)
{
    return ctx->_builder->addInstruction(new (std::nothrow) CreateNewRowInstruction());
}

static Instruction* AddInsertRow(JitTvmCodeGenContext* ctx, Instruction* row_inst)
{
    return ctx->_builder->addInstruction(new (std::nothrow) InsertRowInstruction(row_inst));
}

static Instruction* AddDeleteRow(JitTvmCodeGenContext* ctx)
{
    return ctx->_builder->addInstruction(new (std::nothrow) DeleteRowInstruction());
}

static void AddSetRowNullBits(JitTvmCodeGenContext* ctx, Instruction* row_inst)
{
    (void)ctx->_builder->addInstruction(new (std::nothrow) SetRowNullBitsInstruction(row_inst));
}

static Instruction* AddSetExprResultNullBit(JitTvmCodeGenContext* ctx, Instruction* row_inst, int colid)
{
    return ctx->_builder->addInstruction(new (std::nothrow) SetExprResultNullBitInstruction(row_inst, colid));
}

static void AddExecClearTuple(JitTvmCodeGenContext* ctx)
{
    ctx->_builder->addInstruction(new (std::nothrow) ExecClearTupleInstruction());
}

static void AddExecStoreVirtualTuple(JitTvmCodeGenContext* ctx)
{
    ctx->_builder->addInstruction(new (std::nothrow) ExecStoreVirtualTupleInstruction());
}

static void AddSelectColumn(JitTvmCodeGenContext* ctx, Instruction* row_inst, int table_colid, int tuple_colid,
    JitRangeScanType range_scan_type)
{
    ctx->_builder->addInstruction(
        new (std::nothrow) SelectColumnInstruction(row_inst, table_colid, tuple_colid, range_scan_type));
}

static void AddSetTpProcessed(JitTvmCodeGenContext* ctx)
{
    (void)ctx->_builder->addInstruction(new (std::nothrow) SetTpProcessedInstruction());
}

static void AddSetScanEnded(JitTvmCodeGenContext* ctx, int result)
{
    ctx->_builder->addInstruction(new (std::nothrow) SetScanEndedInstruction(result));
}

static void AddCopyKey(JitTvmCodeGenContext* ctx, JitRangeScanType range_scan_type)
{
    ctx->_builder->addInstruction(new (std::nothrow) CopyKeyInstruction(range_scan_type));
}

static void AddFillKeyPattern(JitTvmCodeGenContext* ctx, unsigned char pattern, int offset, int size,
    JitRangeIteratorType range_itr_type, JitRangeScanType range_scan_type)
{
    ctx->_builder->addInstruction(
        new (std::nothrow) FillKeyPatternInstruction(range_itr_type, pattern, offset, size, range_scan_type));
}

static void AddAdjustKey(JitTvmCodeGenContext* ctx, unsigned char pattern, JitRangeIteratorType range_itr_type,
    JitRangeScanType range_scan_type)
{
    (void)ctx->_builder->addInstruction(
        new (std::nothrow) AdjustKeyInstruction(range_itr_type, pattern, range_scan_type));
}

static Instruction* AddSearchIterator(JitTvmCodeGenContext* ctx, JitIndexScanDirection index_scan_direction,
    JitRangeBoundMode range_bound_mode, JitRangeScanType range_scan_type)
{
    return ctx->_builder->addInstruction(
        new (std::nothrow) SearchIteratorInstruction(index_scan_direction, range_bound_mode, range_scan_type));
}

static Instruction* AddCreateEndIterator(JitTvmCodeGenContext* ctx, JitIndexScanDirection index_scan_direction,
    JitRangeBoundMode range_bound_mode, JitRangeScanType range_scan_type)
{
    return ctx->_builder->addInstruction(
        new (std::nothrow) CreateEndIteratorInstruction(index_scan_direction, range_bound_mode, range_scan_type));
}

static Instruction* AddIsScanEnd(JitTvmCodeGenContext* ctx, JitIndexScanDirection index_scan_direction,
    JitTvmRuntimeCursor* cursor, JitRangeScanType range_scan_type)
{
    return ctx->_builder->addInstruction(new (std::nothrow)
            IsScanEndInstruction(index_scan_direction, cursor->begin_itr, cursor->end_itr, range_scan_type));
}

static Instruction* AddGetRowFromIterator(JitTvmCodeGenContext* ctx, MOT::AccessType access_mode,
    JitIndexScanDirection index_scan_direction, JitTvmRuntimeCursor* cursor, JitRangeScanType range_scan_type)
{
    return ctx->_builder->addInstruction(new (std::nothrow) GetRowFromIteratorInstruction(
        access_mode, index_scan_direction, cursor->begin_itr, cursor->end_itr, range_scan_type));
}

static void AddDestroyIterator(JitTvmCodeGenContext* ctx, Instruction* itr_inst)
{
    (void)ctx->_builder->addInstruction(new (std::nothrow) DestroyIteratorInstruction(itr_inst));
}

static void AddDestroyCursor(JitTvmCodeGenContext* ctx, JitTvmRuntimeCursor* cursor)
{
    AddDestroyIterator(ctx, cursor->begin_itr);
    AddDestroyIterator(ctx, cursor->end_itr);
}

/** @brief Adds a call to setStateIterator(itr, begin_itr). */
static void AddSetStateIterator(
    JitTvmCodeGenContext* ctx, Instruction* itr, JitRangeIteratorType range_itr_type, JitRangeScanType range_scan_type)
{
    (void)ctx->_builder->addInstruction(
        new (std::nothrow) SetStateIteratorInstruction(itr, range_itr_type, range_scan_type));
}

/** @brief Adds a call to isStateIteratorNull(begin_itr). */
static Instruction* AddIsStateIteratorNull(
    JitTvmCodeGenContext* ctx, JitRangeIteratorType range_itr_type, JitRangeScanType range_scan_type)
{
    return ctx->_builder->addInstruction(
        new (std::nothrow) IsStateIteratorNullInstruction(range_itr_type, range_scan_type));
}

/** @brief Adds a call to isStateScanEnd(index, forward_scan). */
static Instruction* AddIsStateScanEnd(
    JitTvmCodeGenContext* ctx, JitIndexScanDirection index_scan_direction, JitRangeScanType range_scan_type)
{
    return ctx->_builder->addInstruction(
        new (std::nothrow) IsStateScanEndInstruction(index_scan_direction, range_scan_type));
}

static Instruction* AddGetRowFromStateIterator(JitTvmCodeGenContext* ctx, MOT::AccessType access_mode,
    JitIndexScanDirection index_scan_direction, JitRangeScanType range_scan_type)
{
    return ctx->_builder->addInstruction(
        new (std::nothrow) GetRowFromStateIteratorInstruction(access_mode, index_scan_direction, range_scan_type));
}

/** @brief Adds a call to setStateScanEndFlag(scan_ended). */
static void AddSetStateScanEndFlag(JitTvmCodeGenContext* ctx, int scan_ended, JitRangeScanType range_scan_type)
{
    (void)ctx->_builder->addInstruction(new (std::nothrow) SetStateScanEndFlagInstruction(scan_ended, range_scan_type));
}

/** @brief Adds a call to getStateScanEndFlag(). */
static Instruction* AddGetStateScanEndFlag(JitTvmCodeGenContext* ctx, JitRangeScanType range_scan_type)
{
    return ctx->_builder->addInstruction(new (std::nothrow) GetStateScanEndFlagInstruction(range_scan_type));
}

static void AddResetStateRow(JitTvmCodeGenContext* ctx, JitRangeScanType range_scan_type)
{
    (void)ctx->_builder->addInstruction(new (std::nothrow) ResetStateRowInstruction(range_scan_type));
}

static void AddSetStateRow(JitTvmCodeGenContext* ctx, Instruction* row, JitRangeScanType range_scan_type)
{
    (void)ctx->_builder->addInstruction(new (std::nothrow) SetStateRowInstruction(row, range_scan_type));
}

static Instruction* AddGetStateRow(JitTvmCodeGenContext* ctx, JitRangeScanType range_scan_type)
{
    return ctx->_builder->addInstruction(new (std::nothrow) GetStateRowInstruction(range_scan_type));
}

static void AddCopyOuterStateRow(JitTvmCodeGenContext* ctx)
{
    (void)ctx->_builder->addInstruction(new (std::nothrow) CopyOuterStateRowInstruction());
}

static Instruction* AddGetOuterStateRowCopy(JitTvmCodeGenContext* ctx)
{
    return ctx->_builder->addInstruction(new (std::nothrow) GetOuterStateRowCopyInstruction());
}

static Instruction* AddIsStateRowNull(JitTvmCodeGenContext* ctx, JitRangeScanType range_scan_type)
{
    return ctx->_builder->addInstruction(new (std::nothrow) IsStateRowNullInstruction(range_scan_type));
}

/** @brief Adds a call to destroyStateIterators(). */
static void AddDestroyStateIterators(JitTvmCodeGenContext* ctx, JitRangeScanType range_scan_type)
{
    ctx->_builder->addInstruction(new (std::nothrow) DestroyStateIteratorsInstruction(range_scan_type));
}

static void AddResetStateLimitCounter(JitTvmCodeGenContext* ctx)
{
    ctx->_builder->addInstruction(new (std::nothrow) ResetStateLimitCounterInstruction());
}

static void AddIncrementStateLimitCounter(JitTvmCodeGenContext* ctx)
{
    ctx->_builder->addInstruction(new (std::nothrow) IncrementStateLimitCounterInstruction());
}

static Instruction* AddGetStateLimitCounter(JitTvmCodeGenContext* ctx)
{
    return ctx->_builder->addInstruction(new (std::nothrow) GetStateLimitCounterInstruction());
}

static void AddPrepareAvgArray(JitTvmCodeGenContext* ctx, int element_type, int element_count)
{
    (void)ctx->_builder->addInstruction(new (std::nothrow) PrepareAvgArrayInstruction(element_type, element_count));
}

static Expression* AddLoadAvgArray(JitTvmCodeGenContext* ctx)
{
    return new (std::nothrow) LoadAvgArrayExpression();
}

static void AddSaveAvgArray(JitTvmCodeGenContext* ctx, Expression* avg_array)
{
    (void)ctx->_builder->addInstruction(new (std::nothrow) SaveAvgArrayInstruction(avg_array));
}

static Instruction* AddComputeAvgFromArray(JitTvmCodeGenContext* ctx, int element_type)
{
    return ctx->_builder->addInstruction(new (std::nothrow) ComputeAvgFromArrayInstruction(element_type));
}

static void AddResetAggValue(JitTvmCodeGenContext* ctx, int element_type)
{
    (void)ctx->_builder->addInstruction(new (std::nothrow) ResetAggValueInstruction(element_type));
}

static Expression* AddGetAggValue(JitTvmCodeGenContext* ctx)
{
    return new (std::nothrow) GetAggValueExpression();
}

static void AddSetAggValue(JitTvmCodeGenContext* ctx, Expression* value)
{
    (void)ctx->_builder->addInstruction(new (std::nothrow) SetAggValueInstruction(value));
}

static void AddResetAggMaxMinNull(JitTvmCodeGenContext* ctx)
{
    (void)ctx->_builder->addInstruction(new (std::nothrow) ResetAggMaxMinNullInstruction());
}

static void AddSetAggMaxMinNotNull(JitTvmCodeGenContext* ctx)
{
    (void)ctx->_builder->addInstruction(new (std::nothrow) SetAggMaxMinNotNullInstruction());
}

static Instruction* AddGetAggMaxMinIsNull(JitTvmCodeGenContext* ctx)
{
    return ctx->_builder->addInstruction(new (std::nothrow) GetAggMaxMinIsNullInstruction());
}

static void AddPrepareDistinctSet(JitTvmCodeGenContext* ctx, int element_type)
{
    (void)ctx->_builder->addInstruction(new (std::nothrow) PrepareDistinctSetInstruction(element_type));
}

static Instruction* AddInsertDistinctItem(JitTvmCodeGenContext* ctx, int element_type, Expression* value)
{
    return ctx->_builder->addInstruction(new (std::nothrow) InsertDistinctItemInstruction(element_type, value));
}

static void AddDestroyDistinctSet(JitTvmCodeGenContext* ctx, int element_type)
{
    (void)ctx->_builder->addInstruction(new (std::nothrow) DestroyDistinctSetInstruction(element_type));
}

static void AddWriteTupleDatum(JitTvmCodeGenContext* ctx, int tuple_colid, Instruction* datum_value)
{
    (void)ctx->_builder->addInstruction(new (std::nothrow) WriteTupleDatumInstruction(tuple_colid, datum_value));
}

#ifdef MOT_JIT_DEBUG
static void IssueDebugLogImpl(JitTvmCodeGenContext* ctx, const char* function, const char* msg)
{
    (void)ctx->_builder->addInstruction(new (std::nothrow) DebugLogInstruction(function, msg));
}
#endif

#ifdef MOT_JIT_DEBUG
#define IssueDebugLog(msg) IssueDebugLogImpl(ctx, __func__, msg)
#else
#define IssueDebugLog(msg)
#endif

static Instruction* buildExpression(JitTvmCodeGenContext* ctx, Expression* expr);

static void CreateJittedFunction(JitTvmCodeGenContext* ctx, const char* function_name, const char* query_string)
{
    ctx->m_jittedQuery = ctx->_builder->createFunction(function_name, query_string);
    IssueDebugLog("Starting execution of jitted function");
}

static bool ProcessJoinOpExpr(
    JitTvmCodeGenContext* ctx, const OpExpr* op_expr, int* column_count, int* column_array, int* max_arg);
static bool ProcessJoinBoolExpr(
    JitTvmCodeGenContext* ctx, const BoolExpr* boolexpr, int* column_count, int* column_array, int* max_arg);

static bool ProcessJoinExpr(JitTvmCodeGenContext* ctx, Expr* expr, int* column_count, int* column_array, int* max_arg)
{
    bool result = false;
    if (expr->type == T_OpExpr) {
        result = ProcessJoinOpExpr(ctx, (OpExpr*)expr, column_count, column_array, max_arg);
    } else if (expr->type == T_BoolExpr) {
        result = ProcessJoinBoolExpr(ctx, (BoolExpr*)expr, column_count, column_array, max_arg);
    } else {
        MOT_LOG_TRACE("Unsupported expression type %d while processing Join Expr", (int)expr->type);
    }
    return result;
}

static Expression* ProcessExpr(
    JitTvmCodeGenContext* ctx, Expr* expr, int& result_type, int arg_pos, int depth, int* max_arg);

static bool ProcessJoinOpExpr(
    JitTvmCodeGenContext* ctx, const OpExpr* op_expr, int* column_count, int* column_array, int* max_arg)
{
    bool result = false;

    // process only point queries
    if (IsWhereOperatorSupported(op_expr->opno)) {
        Instruction* value = nullptr;
        Expression* value_expr = nullptr;
        ListCell* lc1 = nullptr;
        int colid = -1;
        int vartype = -1;
        int result_type = -1;

        foreach (lc1, op_expr->args) {
            Expr* expr = (Expr*)lfirst(lc1);
            if (expr->type ==
                T_RelabelType) {  // sometimes relabel expression hides the inner expression, so we peel it off
                expr = ((RelabelType*)expr)->arg;
            }
            if (expr->type == T_Var) {
                Var* var = (Var*)expr;
                colid = var->varattno;
                vartype = var->vartype;
                if (!IsTypeSupported(vartype)) {
                    MOT_LOG_TRACE("ProcessJoinOpExpr(): Unsupported type %d", vartype);
                    return false;
                }
                // no further processing
            } else {
                value_expr = ProcessExpr(ctx, expr, result_type, 0, 0, max_arg);
                if (value_expr == nullptr) {
                    MOT_LOG_TRACE("Unsupported operand type %d while processing Join OpExpr", (int)expr->type);
                } else if (!IsTypeSupported(result_type)) {
                    MOT_LOG_TRACE("ProcessJoinOpExpr(): Unsupported result type %d", result_type);
                } else {
                    value = buildExpression(ctx, value_expr);
                }
                break;
            }
        }

        if ((colid != -1) && (value != nullptr) && (vartype != -1) && (result_type != -1)) {
            if (result_type != vartype) {
                MOT_LOG_TRACE("ProcessJoinOpExpr(): vartype %d and result-type %d mismatch", vartype, result_type);
                return false;
            }
            // execute: column = getColumnAt(colid)
            Instruction* column = AddGetColumnAt(ctx,
                colid,
                JIT_RANGE_SCAN_MAIN);  // no need to translate to zero-based index (first column is null bits)
            int index_colid = ctx->_table_info.m_columnMap[colid];
            AddBuildDatumKey(ctx, column, index_colid, value, vartype, JIT_RANGE_ITERATOR_START, JIT_RANGE_SCAN_MAIN);

            MOT_LOG_DEBUG("Encountered table column %d, index column %d in where clause", colid, index_colid);
            ++(*column_count);
            column_array[index_colid] = 1;
            result = true;
        } else {
            MOT_LOG_TRACE("ProcessJoinOpExpr(): Invalid expression (colid=%d, value=%p, vartype=%d, result_type=%d)",
                colid,
                value,
                vartype,
                result_type);
        }
    } else {
        MOT_LOG_TRACE("ProcessJoinOpExpr(): Unsupported operator type %u", op_expr->opno);
    }

    return result;
}

static bool ProcessJoinBoolExpr(
    JitTvmCodeGenContext* ctx, const BoolExpr* boolexpr, int* column_count, int* column_array, int* max_arg)
{
    bool result = false;
    if (boolexpr->boolop == AND_EXPR) {
        // now traverse args to get param index to build search key
        ListCell* lc = nullptr;
        foreach (lc, boolexpr->args) {
            // each element is Expr
            Expr* expr = (Expr*)lfirst(lc);
            result = ProcessJoinExpr(ctx, expr, column_count, column_array, max_arg);
            if (!result) {
                MOT_LOG_TRACE("Failed to process operand while processing Join BoolExpr");
                break;
            }
        }
    } else {
        MOT_LOG_TRACE("Unsupported bool operation %d while processing Join BoolExpr", (int)boolexpr->boolop);
    }
    return result;
}

#ifdef DEFINE_BLOCK
#undef DEFINE_BLOCK
#endif
#define DEFINE_BLOCK(block_name, unused) BasicBlock* block_name = ctx->_builder->CreateBlock(#block_name);

static void buildIsSoftMemoryLimitReached(JitTvmCodeGenContext* ctx)
{
    JIT_IF_BEGIN(soft_limit_reached)
    JIT_IF_EVAL_CMP(AddIsSoftMemoryLimitReached(ctx), JIT_CONST(0), JIT_ICMP_NE)
    IssueDebugLog("Soft memory limit reached");
    JIT_RETURN_CONST(MOT::RC_MEMORY_ALLOCATION_ERROR);
    JIT_ELSE()
    IssueDebugLog("Soft memory limit not reached");
    JIT_IF_END()
}

static Instruction* buildExpression(JitTvmCodeGenContext* ctx, Expression* expr)
{
    Instruction* expr_value = ctx->_builder->addExpression(expr);

    // generate code for checking expression evaluation only if the expression is fallible
    if (expr->canFail()) {
        Instruction* expr_rc = ctx->_builder->addGetExpressionRC();

        JIT_IF_BEGIN(check_expression_failed)
        JIT_IF_EVAL_CMP(expr_rc, JIT_CONST(MOT::RC_OK), JIT_ICMP_NE)
        IssueDebugLog("Expression execution failed");
        JIT_RETURN(expr_rc);
        JIT_IF_END()

        IssueDebugLog("Expression execution succeeded");
    }

    return expr_value;
}

static void buildWriteDatumColumn(JitTvmCodeGenContext* ctx, Instruction* row, int colid, Instruction* datum_value)
{
    //   ATTENTION: The datum_value expression-instruction MUST be already evaluated before this code is executed
    //              That is the reason why the expression-instruction was added to the current block and now we
    //              use only a register-ref instruction
    Instruction* set_null_bit_res = AddSetExprResultNullBit(ctx, row, colid);
    IssueDebugLog("Set null bit");

    if (ctx->_table_info.m_table->GetField(colid)->m_isNotNull) {
        JIT_IF_BEGIN(check_null_violation)
        JIT_IF_EVAL_CMP(set_null_bit_res, JIT_CONST(MOT::RC_OK), JIT_ICMP_NE)
        IssueDebugLog("Null constraint violated");
        JIT_RETURN(set_null_bit_res);
        JIT_IF_END()
    }

    // now check if the result is not null, and if so write column datum
    Instruction* is_expr_null = AddGetExprArgIsNull(ctx, 0);
    JIT_IF_BEGIN(check_expr_null)
    JIT_IF_EVAL_NOT(is_expr_null)
    IssueDebugLog("Encountered non-null expression result, writing datum column");
    AddWriteDatumColumn(ctx, colid, row, datum_value);
    JIT_IF_END()
}

static void buildWriteRow(JitTvmCodeGenContext* ctx, Instruction* row, bool isPKey, JitTvmRuntimeCursor* cursor)
{
    IssueDebugLog("Writing row");
    Instruction* write_row_res = AddWriteRow(ctx, row);

    JIT_IF_BEGIN(check_row_written)
    JIT_IF_EVAL_CMP(write_row_res, JIT_CONST(MOT::RC_OK), JIT_ICMP_NE)
    IssueDebugLog("Row not written");
    // need to emit cleanup code
    if (!isPKey) {
        AddDestroyCursor(ctx, cursor);
    }
    JIT_RETURN(write_row_res);
    JIT_IF_END()
}

static void buildResetRowsProcessed(JitTvmCodeGenContext* ctx)
{
    (void)ctx->_builder->addInstruction(new (std::nothrow) ResetRowsProcessedInstruction());
}

static void buildIncrementRowsProcessed(JitTvmCodeGenContext* ctx)
{
    (void)ctx->_builder->addInstruction(new (std::nothrow) IncrementRowsProcessedInstruction());
}

static Instruction* buildCreateNewRow(JitTvmCodeGenContext* ctx)
{
    Instruction* row = AddCreateNewRow(ctx);

    JIT_IF_BEGIN(check_row_created)
    JIT_IF_EVAL_NOT(row)
    IssueDebugLog("Failed to create row");
    JIT_RETURN_CONST(MOT::RC_MEMORY_ALLOCATION_ERROR);
    JIT_IF_END()

    return row;
}

static Instruction* buildSearchRow(
    JitTvmCodeGenContext* ctx, MOT::AccessType access_type, JitRangeScanType range_scan_type)
{
    IssueDebugLog("Searching row");
    Instruction* row = AddSearchRow(ctx, access_type, range_scan_type);

    JIT_IF_BEGIN(check_row_found)
    JIT_IF_EVAL_NOT(row)
    IssueDebugLog("Row not found");
    JIT_RETURN_CONST(MOT::RC_LOCAL_ROW_NOT_FOUND);
    JIT_IF_END()

    IssueDebugLog("Row found");
    return row;
}

static Expression* buildFilter(JitTvmCodeGenContext* ctx, Instruction* row, JitFilter* filter, int* max_arg)
{
    Expression* result = nullptr;
    Expression* lhs_expr = ProcessExpr(ctx, row, filter->_lhs_operand, max_arg);
    if (lhs_expr == nullptr) {
        MOT_LOG_TRACE(
            "buildFilter(): Failed to process LHS expression with type %d", (int)filter->_lhs_operand->_expr_type);
    } else {
        Expression* rhs_expr = ProcessExpr(ctx, row, filter->_rhs_operand, max_arg);
        if (rhs_expr == nullptr) {
            MOT_LOG_TRACE(
                "buildFilter(): Failed to process RHS expression with type %d", (int)filter->_rhs_operand->_expr_type);
            delete lhs_expr;
        } else {
            result = ProcessFilterExpr(ctx, row, filter, max_arg);
        }
    }
    return result;
}

static bool buildFilterRow(
    JitTvmCodeGenContext* ctx, Instruction* row, JitFilterArray* filters, int* max_arg, BasicBlock* next_block)
{
    // 1. for each filter expression we generate the equivalent instructions which should evaluate to true or false
    // 2. We assume that all filters are applied with AND operator between them (imposed during query analysis/plan
    // phase)
    for (int i = 0; i < filters->_filter_count; ++i) {
        Expression* filter_expr = buildFilter(ctx, row, &filters->_scan_filters[i], max_arg);
        if (filter_expr == nullptr) {
            MOT_LOG_TRACE("buildFilterRow(): Failed to process filter expression %d", i);
            return false;
        }

        JIT_IF_BEGIN(filter_expr)
        IssueDebugLog("Checking of row passes filter");
        Instruction* value = buildExpression(ctx, filter_expr);  // this is a boolean datum result
        JIT_IF_EVAL_NOT(value)
        IssueDebugLog("Row did not pass filter");
        if (next_block == nullptr) {
            JIT_RETURN_CONST(MOT::RC_LOCAL_ROW_NOT_FOUND);
        } else {
            ctx->_builder->CreateBr(next_block);
        }
        JIT_ELSE()
        IssueDebugLog("Row passed filter");
        JIT_IF_END()
    }
    return true;
}

static void buildInsertRow(JitTvmCodeGenContext* ctx, Instruction* row)
{
    IssueDebugLog("Inserting row");
    Instruction* insert_row_res = AddInsertRow(ctx, row);

    JIT_IF_BEGIN(check_row_inserted)
    JIT_IF_EVAL_CMP(insert_row_res, JIT_CONST(MOT::RC_OK), JIT_ICMP_NE)
    IssueDebugLog("Row not inserted");
    JIT_RETURN(insert_row_res);
    JIT_IF_END()

    IssueDebugLog("Row inserted");
}

static void buildDeleteRow(JitTvmCodeGenContext* ctx)
{
    IssueDebugLog("Deleting row");
    Instruction* delete_row_res = AddDeleteRow(ctx);

    JIT_IF_BEGIN(check_delete_row)
    JIT_IF_EVAL_CMP(delete_row_res, JIT_CONST(MOT::RC_OK), JIT_ICMP_NE)
    IssueDebugLog("Row not deleted");
    JIT_RETURN(delete_row_res);
    JIT_IF_END()

    IssueDebugLog("Row deleted");
}

static Instruction* buildSearchIterator(JitTvmCodeGenContext* ctx, JitIndexScanDirection index_scan_direction,
    JitRangeBoundMode range_bound_mode, JitRangeScanType range_scan_type)
{
    // search the row, execute: IndexIterator* itr = searchIterator(table, key);
    IssueDebugLog("Searching range start");
    Instruction* itr = AddSearchIterator(ctx, index_scan_direction, range_bound_mode, range_scan_type);

    JIT_IF_BEGIN(check_itr_found)
    JIT_IF_EVAL_NOT(itr)
    IssueDebugLog("Range start not found");
    JIT_RETURN_CONST(MOT::RC_LOCAL_ROW_NOT_FOUND);
    JIT_IF_END()

    IssueDebugLog("Range start found");
    return itr;
}

static Instruction* buildGetRowFromIterator(JitTvmCodeGenContext* ctx, BasicBlock* endLoopBlock,
    MOT::AccessType access_mode, JitIndexScanDirection index_scan_direction, JitTvmRuntimeCursor* cursor,
    JitRangeScanType range_scan_type)
{
    IssueDebugLog("Retrieving row from iterator");
    Instruction* row = AddGetRowFromIterator(ctx, access_mode, index_scan_direction, cursor, range_scan_type);

    JIT_IF_BEGIN(check_itr_row_found)
    JIT_IF_EVAL_NOT(row)
    IssueDebugLog("Iterator row not found");
    JIT_GOTO(endLoopBlock);
    // NOTE: we can actually do here a JIT_WHILE_BREAK() if we have an enclosing while loop
    JIT_IF_END()

    IssueDebugLog("Iterator row found");
    return row;
}

static Expression* ProcessConstExpr(
    JitTvmCodeGenContext* ctx, const Const* const_value, int& result_type, int arg_pos, int depth, int* max_arg)
{
    Expression* result = nullptr;
    MOT_LOG_DEBUG("%*s --> Processing CONST expression", depth, "");
    if (depth > MOT_JIT_MAX_EXPR_DEPTH) {
        MOT_LOG_TRACE("Cannot process expression: Expression exceeds depth limit %d", (int)MOT_JIT_MAX_EXPR_DEPTH);
        return nullptr;
    }

    if (IsTypeSupported(const_value->consttype)) {
        result_type = const_value->consttype;
        result = new (std::nothrow) ConstExpression(const_value->constvalue, arg_pos, (int)(const_value->constisnull));
        if (max_arg && (arg_pos > *max_arg)) {
            *max_arg = arg_pos;
        }
    } else {
        MOT_LOG_TRACE("Failed to process const expression: type %d unsupported", (int)const_value->consttype);
    }

    MOT_LOG_DEBUG("%*s <-- Processing CONST expression result: %p", depth, "", result);
    return result;
}

static Expression* ProcessParamExpr(
    JitTvmCodeGenContext* ctx, const Param* param, int& result_type, int arg_pos, int depth, int* max_arg)
{
    Expression* result = nullptr;
    MOT_LOG_DEBUG("%*s --> Processing PARAM expression", depth, "");
    if (depth > MOT_JIT_MAX_EXPR_DEPTH) {
        MOT_LOG_TRACE("Cannot process expression: Expression exceeds depth limit %d", (int)MOT_JIT_MAX_EXPR_DEPTH);
        return nullptr;
    }

    if (IsTypeSupported(param->paramtype)) {
        result_type = param->paramtype;
        result = AddGetDatumParam(ctx, param->paramid - 1, arg_pos);
        if (max_arg && (arg_pos > *max_arg)) {
            *max_arg = arg_pos;
        }
    } else {
        MOT_LOG_TRACE("Failed to process param expression: type %d unsupported", (int)param->paramtype);
    }

    MOT_LOG_DEBUG("%*s <-- Processing PARAM expression result: %p", depth, "", result);
    return result;
}

static Expression* ProcessRelabelExpr(
    JitTvmCodeGenContext* ctx, RelabelType* relabel_type, int& result_type, int arg_pos, int depth, int* max_arg)
{
    MOT_LOG_DEBUG("Processing RELABEL expression");
    Param* param = (Param*)relabel_type->arg;
    return ProcessParamExpr(ctx, param, result_type, arg_pos, depth, max_arg);
}

static Expression* ProcessVarExpr(
    JitTvmCodeGenContext* ctx, const Var* var, int& result_type, int arg_pos, int depth, int* max_arg)
{
    Expression* result = nullptr;
    MOT_LOG_DEBUG("%*s --> Processing VAR expression", depth, "");
    if (depth > MOT_JIT_MAX_EXPR_DEPTH) {
        MOT_LOG_TRACE("Cannot process expression: Expression exceeds depth limit %d", (int)MOT_JIT_MAX_EXPR_DEPTH);
        return nullptr;
    }

    if (IsTypeSupported(var->vartype)) {
        result_type = var->vartype;
        int table_colid = var->varattno;
        result = AddReadDatumColumn(ctx, ctx->_table_info.m_table, nullptr, table_colid, arg_pos);
        if (max_arg && (arg_pos > *max_arg)) {
            *max_arg = arg_pos;
        }
    } else {
        MOT_LOG_TRACE("Failed to process var expression: type %d unsupported", (int)var->vartype);
    }

    MOT_LOG_DEBUG("%*s <-- Processing VAR expression result: %p", depth, "", result);
    return result;
}

#define APPLY_UNARY_OPERATOR(funcid, name)                   \
    case funcid:                                             \
        MOT_LOG_DEBUG("Adding call to builtin: " #name);     \
        result = new (std::nothrow) name##Operator(args[0]); \
        break;

#define APPLY_BINARY_OPERATOR(funcid, name)                           \
    case funcid:                                                      \
        MOT_LOG_DEBUG("Adding call to builtin: " #name);              \
        result = new (std::nothrow) name##Operator(args[0], args[1]); \
        break;

#define APPLY_TERNARY_OPERATOR(funcid, name)                                   \
    case funcid:                                                               \
        MOT_LOG_DEBUG("Adding call to builtin: " #name);                       \
        result = new (std::nothrow) name##Operator(args[0], args[1], args[2]); \
        break;

#define APPLY_UNARY_CAST_OPERATOR(funcid, name)                       \
    case funcid:                                                      \
        MOT_LOG_DEBUG("Adding call to builtin: " #name);              \
        result = new (std::nothrow) name##Operator(args[0], arg_pos); \
        break;

#define APPLY_BINARY_CAST_OPERATOR(funcid, name)                               \
    case funcid:                                                               \
        MOT_LOG_DEBUG("Adding call to builtin: " #name);                       \
        result = new (std::nothrow) name##Operator(args[0], args[1], arg_pos); \
        break;

#define APPLY_TERNARY_CAST_OPERATOR(funcid, name)                                       \
    case funcid:                                                                        \
        MOT_LOG_DEBUG("Adding call to builtin: " #name);                                \
        result = new (std::nothrow) name##Operator(args[0], args[1], args[2], arg_pos); \
        break;

static Expression* ProcessOpExpr(
    JitTvmCodeGenContext* ctx, const OpExpr* op_expr, int& result_type, int arg_pos, int depth, int* max_arg)
{
    Expression* result = nullptr;
    const int op_args_num = 3;
    MOT_LOG_DEBUG("%*s --> Processing OP %d expression", depth, "", (int)op_expr->opfuncid);
    if (depth > MOT_JIT_MAX_EXPR_DEPTH) {
        MOT_LOG_TRACE("Cannot process expression: Expression exceeds depth limit %d", (int)MOT_JIT_MAX_EXPR_DEPTH);
        return nullptr;
    }

    if (list_length(op_expr->args) > op_args_num) {
        MOT_LOG_TRACE("Unsupported operator %d: too many arguments", op_expr->opno);
        return nullptr;
    }

    Expression* args[op_args_num] = {nullptr, nullptr, nullptr};
    int arg_num = 0;
    int dummy = 0;

    ListCell* lc = nullptr;
    foreach (lc, op_expr->args) {
        Expr* sub_expr = (Expr*)lfirst(lc);
        args[arg_num] = ProcessExpr(ctx, sub_expr, dummy, arg_pos + arg_num, depth + 1, max_arg);
        if (args[arg_num] == nullptr) {
            MOT_LOG_TRACE("Failed to process operator sub-expression %d", arg_num);
            return nullptr;
        }
        if (++arg_num == op_args_num) {
            break;
        }
    }

    result_type = op_expr->opresulttype;
    switch (op_expr->opfuncid) {
        APPLY_OPERATORS()

        default:
            MOT_LOG_TRACE("Unsupported operator function type: %d", op_expr->opfuncid);
            break;
    }

    MOT_LOG_DEBUG("%*s <-- Processing OP %d expression result: %p", depth, "", (int)op_expr->opfuncid, result);
    return result;
}

static Expression* ProcessFuncExpr(
    JitTvmCodeGenContext* ctx, const FuncExpr* func_expr, int& result_type, int arg_pos, int depth, int* max_arg)
{
    Expression* result = nullptr;
    const int func_args_num = 3;
    MOT_LOG_DEBUG("%*s --> Processing FUNC %d expression", depth, "", (int)func_expr->funcid);
    if (depth > MOT_JIT_MAX_EXPR_DEPTH) {
        MOT_LOG_TRACE("Cannot process expression: Expression exceeds depth limit %d", (int)MOT_JIT_MAX_EXPR_DEPTH);
        return nullptr;
    }

    if (list_length(func_expr->args) > func_args_num) {
        MOT_LOG_TRACE("Unsupported function %d: too many arguments", func_expr->funcid);
        return nullptr;
    }

    Expression* args[func_args_num] = {nullptr, nullptr, nullptr};
    int arg_num = 0;
    int dummy = 0;

    ListCell* lc = nullptr;
    foreach (lc, func_expr->args) {
        Expr* sub_expr = (Expr*)lfirst(lc);
        args[arg_num] = ProcessExpr(ctx, sub_expr, dummy, arg_pos + arg_num, depth + 1, max_arg);
        if (args[arg_num] == nullptr) {
            MOT_LOG_TRACE("Failed to process function sub-expression %d", arg_num);
            return nullptr;
        }
        if (++arg_num == func_args_num) {
            break;
        }
    }

    result_type = func_expr->funcresulttype;
    switch (func_expr->funcid) {
        APPLY_OPERATORS()

        default:
            MOT_LOG_TRACE("Unsupported function type: %d", (int)func_expr->funcid);
            break;
    }

    MOT_LOG_DEBUG("%*s <-- Processing FUNC %d expression result: %p", depth, "", (int)func_expr->funcid, result);
    return result;
}

#undef APPLY_UNARY_OPERATOR
#undef APPLY_BINARY_OPERATOR
#undef APPLY_TERNARY_OPERATOR
#undef APPLY_UNARY_CAST_OPERATOR
#undef APPLY_BINARY_CAST_OPERATOR
#undef APPLY_TERNARY_CAST_OPERATOR

static Expression* ProcessExpr(
    JitTvmCodeGenContext* ctx, Expr* expr, int& result_type, int arg_pos, int depth, int* max_arg)
{
    Expression* result = nullptr;
    MOT_LOG_DEBUG("%*s --> Processing expression %d", depth, "", (int)expr->type);
    if (depth > MOT_JIT_MAX_EXPR_DEPTH) {
        MOT_LOG_TRACE("Cannot process expression: Expression exceeds depth limit %d", (int)MOT_JIT_MAX_EXPR_DEPTH);
        return nullptr;
    }

    // case 1: assign from parameter (cases like: s_quantity = $1)
    if (expr->type == T_Const) {
        result = ProcessConstExpr(ctx, (Const*)expr, result_type, arg_pos, depth + 1, max_arg);
    } else if (expr->type == T_Param) {
        result = ProcessParamExpr(ctx, (Param*)expr, result_type, arg_pos, depth + 1, max_arg);
    } else if (expr->type == T_RelabelType) {
        result = ProcessRelabelExpr(ctx, (RelabelType*)expr, result_type, arg_pos, depth + 1, max_arg);
    } else if (expr->type == T_Var) {
        result = ProcessVarExpr(ctx, (Var*)expr, result_type, arg_pos, depth + 1, max_arg);
    } else if (expr->type == T_OpExpr) {
        result = ProcessOpExpr(ctx, (OpExpr*)expr, result_type, arg_pos, depth + 1, max_arg);
    } else if (expr->type == T_FuncExpr) {
        result = ProcessFuncExpr(ctx, (FuncExpr*)expr, result_type, arg_pos, depth + 1, max_arg);
    } else {
        MOT_LOG_TRACE(
            "Failed to generate jitted code for query: unsupported target expression type: %d", (int)expr->type);
    }

    MOT_LOG_DEBUG("%*s <-- Processing expression %d result: %p", depth, "", (int)expr->type, result);
    return result;
}

static Expression* ProcessConstExpr(JitTvmCodeGenContext* ctx, const JitConstExpr* expr, int* max_arg)
{
    AddSetExprArgIsNull(ctx, expr->_arg_pos, (expr->_is_null ? 1 : 0));  // mark expression null status
    Expression* result = new (std::nothrow) ConstExpression(expr->_value, expr->_arg_pos, (int)(expr->_is_null));
    if (max_arg && (expr->_arg_pos > *max_arg)) {
        *max_arg = expr->_arg_pos;
    }
    return result;
}

static Expression* ProcessParamExpr(JitTvmCodeGenContext* ctx, const JitParamExpr* expr, int* max_arg)
{
    Expression* result = AddGetDatumParam(ctx, expr->_param_id, expr->_arg_pos);
    if (max_arg && (expr->_arg_pos > *max_arg)) {
        *max_arg = expr->_arg_pos;
    }
    return result;
}

static Expression* ProcessVarExpr(JitTvmCodeGenContext* ctx, Instruction* row, JitVarExpr* expr, int* max_arg)
{
    Expression* result = nullptr;
    if (row == nullptr) {
        MOT_LOG_TRACE("ProcessVarExpr(): Unexpected VAR expression without a row");
    } else {
        result = AddReadDatumColumn(ctx, expr->_table, row, expr->_column_id, expr->_arg_pos);
        if (max_arg && (expr->_arg_pos > *max_arg)) {
            *max_arg = expr->_arg_pos;
        }
    }
    return result;
}

#define APPLY_UNARY_OPERATOR(funcid, name)                   \
    case funcid:                                             \
        MOT_LOG_DEBUG("Adding call to builtin: " #name);     \
        result = new (std::nothrow) name##Operator(args[0]); \
        break;

#define APPLY_BINARY_OPERATOR(funcid, name)                           \
    case funcid:                                                      \
        MOT_LOG_DEBUG("Adding call to builtin: " #name);              \
        result = new (std::nothrow) name##Operator(args[0], args[1]); \
        break;

#define APPLY_TERNARY_OPERATOR(funcid, name)                                   \
    case funcid:                                                               \
        MOT_LOG_DEBUG("Adding call to builtin: " #name);                       \
        result = new (std::nothrow) name##Operator(args[0], args[1], args[2]); \
        break;

#define APPLY_UNARY_CAST_OPERATOR(funcid, name)                       \
    case funcid:                                                      \
        MOT_LOG_DEBUG("Adding call to builtin: " #name);              \
        result = new (std::nothrow) name##Operator(args[0], arg_pos); \
        break;

#define APPLY_BINARY_CAST_OPERATOR(funcid, name)                               \
    case funcid:                                                               \
        MOT_LOG_DEBUG("Adding call to builtin: " #name);                       \
        result = new (std::nothrow) name##Operator(args[0], args[1], arg_pos); \
        break;

#define APPLY_TERNARY_CAST_OPERATOR(funcid, name)                                       \
    case funcid:                                                                        \
        MOT_LOG_DEBUG("Adding call to builtin: " #name);                                \
        result = new (std::nothrow) name##Operator(args[0], args[1], args[2], arg_pos); \
        break;

static Expression* ProcessOpExpr(JitTvmCodeGenContext* ctx, Instruction* row, JitOpExpr* expr, int* max_arg)
{
    Expression* result = nullptr;
    const int exp_args_num = 3;

    Expression* args[exp_args_num] = {nullptr, nullptr, nullptr};

    for (int i = 0; i < expr->_arg_count; ++i) {
        args[i] = ProcessExpr(ctx, row, expr->_args[i], max_arg);
        if (args[i] == nullptr) {
            MOT_LOG_TRACE("Failed to process operator sub-expression %d", i);
            for (int j = 0; j < i; ++j) {
                delete args[j];  // cleanup after error
            }
            return nullptr;
        }
    }

    int arg_pos = expr->_arg_pos;
    switch (expr->_op_func_id) {
        APPLY_OPERATORS()

        default:
            MOT_LOG_TRACE("Unsupported operator function type: %d", expr->_op_func_id);
            break;
    }

    return result;
}

static Expression* ProcessFuncExpr(JitTvmCodeGenContext* ctx, Instruction* row, JitFuncExpr* expr, int* max_arg)
{
    Expression* result = nullptr;
    const int exp_args_num = 3;

    Expression* args[exp_args_num] = {nullptr, nullptr, nullptr};

    for (int i = 0; i < expr->_arg_count; ++i) {
        args[i] = ProcessExpr(ctx, row, expr->_args[i], max_arg);
        if (args[i] == nullptr) {
            MOT_LOG_TRACE("Failed to process function sub-expression %d", i);
            for (int j = 0; j < i; ++j) {
                delete args[j];  // cleanup after error
            }
            return nullptr;
        }
    }

    int arg_pos = expr->_arg_pos;
    switch (expr->_func_id) {
        APPLY_OPERATORS()

        default:
            MOT_LOG_TRACE("Unsupported function type: %d", expr->_func_id);
            break;
    }

    return result;
}

// we allow only binary operators for filters
#undef APPLY_UNARY_OPERATOR
#undef APPLY_TERNARY_OPERATOR
#undef APPLY_UNARY_CAST_OPERATOR
#undef APPLY_TERNARY_CAST_OPERATOR

#define APPLY_UNARY_OPERATOR(funcid, name)                                              \
    case funcid:                                                                        \
        MOT_LOG_TRACE("Unexpected call in filter expression to unary builtin: " #name); \
        break;

#define APPLY_TERNARY_OPERATOR(funcid, name)                                              \
    case funcid:                                                                          \
        MOT_LOG_TRACE("Unexpected call in filter expression to ternary builtin: " #name); \
        break;

#define APPLY_UNARY_CAST_OPERATOR(funcid, name)                                              \
    case funcid:                                                                             \
        MOT_LOG_TRACE("Unexpected call in filter expression to unary cast builtin: " #name); \
        break;

#define APPLY_TERNARY_CAST_OPERATOR(funcid, name)                                              \
    case funcid:                                                                               \
        MOT_LOG_TRACE("Unexpected call in filter expression to ternary cast builtin: " #name); \
        break;

static Expression* ProcessFilterExpr(JitTvmCodeGenContext* ctx, Instruction* row, JitFilter* filter, int* max_arg)
{
    Expression* result = nullptr;
    const int exp_args_num = 3;

    Expression* args[exp_args_num] = {nullptr, nullptr, nullptr};

    args[0] = ProcessExpr(ctx, row, filter->_lhs_operand, max_arg);
    if (!args[0]) {
        MOT_LOG_TRACE("Failed to process filter LHS expression");
        return nullptr;
    }

    args[1] = ProcessExpr(ctx, row, filter->_rhs_operand, max_arg);
    if (!args[1]) {
        MOT_LOG_TRACE("Failed to process filter RHS expression");
        delete args[0];
        return nullptr;
    }

    int arg_pos = 0;  // always a top-level expression
    switch (filter->_filter_op_funcid) {
        APPLY_OPERATORS()

        default:
            MOT_LOG_TRACE("Unsupported filter function type: %d", filter->_filter_op_funcid);
            break;
    }

    return result;
}

#undef APPLY_UNARY_OPERATOR
#undef APPLY_BINARY_OPERATOR
#undef APPLY_TERNARY_OPERATOR
#undef APPLY_UNARY_CAST_OPERATOR
#undef APPLY_BINARY_CAST_OPERATOR
#undef APPLY_TERNARY_CAST_OPERATOR

static Expression* ProcessExpr(JitTvmCodeGenContext* ctx, Instruction* row, JitExpr* expr, int* max_arg)
{
    Expression* result = nullptr;

    if (expr->_expr_type == JIT_EXPR_TYPE_CONST) {
        result = ProcessConstExpr(ctx, (JitConstExpr*)expr, max_arg);
    } else if (expr->_expr_type == JIT_EXPR_TYPE_PARAM) {
        result = ProcessParamExpr(ctx, (JitParamExpr*)expr, max_arg);
    } else if (expr->_expr_type == JIT_EXPR_TYPE_VAR) {
        result = ProcessVarExpr(ctx, row, (JitVarExpr*)expr, max_arg);
    } else if (expr->_expr_type == JIT_EXPR_TYPE_OP) {
        result = ProcessOpExpr(ctx, row, (JitOpExpr*)expr, max_arg);
    } else if (expr->_expr_type == JIT_EXPR_TYPE_FUNC) {
        result = ProcessFuncExpr(ctx, row, (JitFuncExpr*)expr, max_arg);
    } else {
        MOT_LOG_TRACE(
            "Failed to generate jitted code for query: unsupported target expression type: %d", (int)expr->_expr_type);
    }

    return result;
}

static JitContext* FinalizeCodegen(JitTvmCodeGenContext* ctx, int max_arg, JitCommandType command_type)
{
    // do minimal verification and wrap up
    if (!ctx->m_jittedQuery->finalize()) {
        MOT_LOG_ERROR("Failed to generate jitted code for query: Failed to finalize jit function");
        ctx->m_jittedQuery->dump();
        delete ctx->m_jittedQuery;
        return nullptr;
    }

    // dump if requested
    if (IsMotCodegenPrintEnabled()) {
        ctx->m_jittedQuery->dump();
    }

    // verify function structure
    if (!ctx->m_jittedQuery->verify()) {
        MOT_LOG_TRACE("Failed to generate jitted code for query: Failed to verify jit function");
        delete ctx->m_jittedQuery;
        return nullptr;
    }

    // that's it, we are ready
    JitContext* jit_context = AllocJitContext(JIT_CONTEXT_GLOBAL);
    if (jit_context == nullptr) {
        MOT_LOG_TRACE("Failed to allocate JIT context, aborting code generation");
        delete ctx->m_jittedQuery;
        return nullptr;
    }

    // setup execution details
    jit_context->m_table = ctx->_table_info.m_table;
    jit_context->m_index = ctx->_table_info.m_index;
    jit_context->m_tvmFunction = ctx->m_jittedQuery;
    jit_context->m_argCount = max_arg + 1;
    jit_context->m_innerTable = ctx->m_innerTable_info.m_table;
    jit_context->m_innerIndex = ctx->m_innerTable_info.m_index;
    jit_context->m_commandType = command_type;

    return jit_context;
}

static bool buildScanExpression(JitTvmCodeGenContext* ctx, JitColumnExpr* expr, int* max_arg,
    JitRangeIteratorType range_itr_type, JitRangeScanType range_scan_type, Instruction* outer_row)
{
    Expression* value_expr = ProcessExpr(ctx, outer_row, expr->_expr, max_arg);
    if (value_expr == nullptr) {
        MOT_LOG_TRACE("buildScanExpression(): Failed to process expression with type %d", (int)expr->_expr->_expr_type);
        return false;
    } else {
        Instruction* value = buildExpression(ctx, value_expr);
        Instruction* column = AddGetColumnAt(ctx,
            expr->_table_column_id,
            range_scan_type);  // no need to translate to zero-based index (first column is null bits)
        int index_colid = -1;
        if (range_scan_type == JIT_RANGE_SCAN_INNER) {
            index_colid = ctx->m_innerTable_info.m_columnMap[expr->_table_column_id];
        } else {
            index_colid = ctx->_table_info.m_columnMap[expr->_table_column_id];
        }
        AddBuildDatumKey(ctx, column, index_colid, value, expr->_column_type, range_itr_type, range_scan_type);
    }
    return true;
}

static bool buildPointScan(JitTvmCodeGenContext* ctx, JitColumnExprArray* expr_array, int* max_arg,
    JitRangeScanType range_scan_type, Instruction* outer_row, int expr_count = 0)
{
    if (expr_count == 0) {
        expr_count = expr_array->_count;
    }
    AddInitSearchKey(ctx, range_scan_type);
    for (int i = 0; i < expr_count; ++i) {
        JitColumnExpr* expr = &expr_array->_exprs[i];

        // validate the expression refers to the right table (in search expressions array, all expressions refer to the
        // same table)
        if (range_scan_type == JIT_RANGE_SCAN_INNER) {
            if (expr->_table != ctx->m_innerTable_info.m_table) {
                MOT_REPORT_ERROR(MOT_ERROR_INTERNAL,
                    "Generate TVM JIT Code",
                    "Invalid expression table (expected inner table %s, got %s)",
                    ctx->m_innerTable_info.m_table->GetTableName().c_str(),
                    expr->_table->GetTableName().c_str());
                return false;
            }
        } else {
            if (expr->_table != ctx->_table_info.m_table) {
                MOT_REPORT_ERROR(MOT_ERROR_INTERNAL,
                    "Generate TVM JIT Code",
                    "Invalid expression table (expected main/outer table %s, got %s)",
                    ctx->m_innerTable_info.m_table->GetTableName().c_str(),
                    expr->_table->GetTableName().c_str());
                return false;
            }
        }

        // prepare the sub-expression
        if (!buildScanExpression(ctx, expr, max_arg, JIT_RANGE_ITERATOR_START, range_scan_type, outer_row)) {
            return false;
        }
    }
    return true;
}

static bool writeRowColumns(
    JitTvmCodeGenContext* ctx, Instruction* row, JitColumnExprArray* expr_array, int* max_arg, bool is_update)
{
    for (int i = 0; i < expr_array->_count; ++i) {
        JitColumnExpr* column_expr = &expr_array->_exprs[i];

        Expression* expr = ProcessExpr(ctx, row, column_expr->_expr, max_arg);
        if (expr == nullptr) {
            MOT_LOG_TRACE("ProcessExpr() returned NULL");
            return false;
        }

        // set null bit or copy result data to column
        Instruction* value = buildExpression(ctx, expr);
        buildWriteDatumColumn(ctx, row, column_expr->_table_column_id, value);

        // execute (for incremental redo): setBit(bmp, bit_index);
        if (is_update) {
            AddSetBit(ctx, column_expr->_table_column_id - 1);
        }
    }

    return true;
}

static bool selectRowColumns(JitTvmCodeGenContext* ctx, Instruction* row, JitSelectExprArray* expr_array, int* max_arg,
    JitRangeScanType range_scan_type)
{
    for (int i = 0; i < expr_array->_count; ++i) {
        JitSelectExpr* expr = &expr_array->_exprs[i];
        // we skip expressions that select from other tables
        if (range_scan_type == JIT_RANGE_SCAN_INNER) {
            if (expr->_column_expr->_table != ctx->m_innerTable_info.m_table) {
                continue;
            }
        } else {
            if (expr->_column_expr->_table != ctx->_table_info.m_table) {
                continue;
            }
        }
        AddSelectColumn(ctx, row, expr->_column_expr->_column_id, expr->_tuple_column_id, range_scan_type);
    }

    return true;
}

static bool buildClosedRangeScan(JitTvmCodeGenContext* ctx, JitIndexScan* index_scan, int* max_arg,
    JitRangeScanType range_scan_type, Instruction* outer_row)
{
    // a closed range scan starts just like a point scan (with not enough search expressions) and then adds key patterns
    bool result = buildPointScan(ctx, &index_scan->_search_exprs, max_arg, range_scan_type, outer_row);
    if (result) {
        AddCopyKey(ctx, range_scan_type);

        // now fill each key with the right pattern for the missing pkey columns in the where clause
        bool ascending = (index_scan->_sort_order == JIT_QUERY_SORT_ASCENDING);

        int* index_column_offsets = nullptr;
        const uint16_t* key_length = nullptr;
        int index_column_count = 0;
        if (range_scan_type == JIT_RANGE_SCAN_INNER) {
            index_column_offsets = ctx->m_innerTable_info.m_indexColumnOffsets;
            key_length = ctx->m_innerTable_info.m_index->GetLengthKeyFields();
            index_column_count = ctx->m_innerTable_info.m_index->GetNumFields();
        } else {
            index_column_offsets = ctx->_table_info.m_indexColumnOffsets;
            key_length = ctx->_table_info.m_index->GetLengthKeyFields();
            index_column_count = ctx->_table_info.m_index->GetNumFields();
        }

        int first_zero_column = index_scan->_column_count;
        for (int i = first_zero_column; i < index_column_count; ++i) {
            int offset = index_column_offsets[i];
            int size = key_length[i];
            MOT_LOG_DEBUG(
                "Filling begin/end iterator pattern for missing pkey fields at offset %d, size %d", offset, size);
            AddFillKeyPattern(ctx, ascending ? 0x00 : 0xFF, offset, size, JIT_RANGE_ITERATOR_START, range_scan_type);
            AddFillKeyPattern(ctx, ascending ? 0xFF : 0x00, offset, size, JIT_RANGE_ITERATOR_END, range_scan_type);
        }

        AddAdjustKey(ctx,
            ascending ? 0x00 : 0xFF,
            JIT_RANGE_ITERATOR_START,
            range_scan_type);  // currently this is relevant only for secondary index searches
        AddAdjustKey(ctx,
            ascending ? 0xFF : 0x00,
            JIT_RANGE_ITERATOR_END,
            range_scan_type);  // currently this is relevant only for secondary index searches
    }
    return result;
}

static bool buildSemiOpenRangeScan(JitTvmCodeGenContext* ctx, JitIndexScan* index_scan, int* max_arg,
    JitRangeScanType range_scan_type, JitRangeBoundMode* begin_range_bound, JitRangeBoundMode* end_range_bound,
    Instruction* outer_row)
{
    // an open range scan starts just like a point scan (with not enough search expressions) and then adds key patterns
    // we do not use the last search expression
    bool result = buildPointScan(
        ctx, &index_scan->_search_exprs, max_arg, range_scan_type, outer_row, index_scan->_search_exprs._count - 1);
    if (result) {
        AddCopyKey(ctx, range_scan_type);

        // now fill each key with the right pattern for the missing pkey columns in the where clause
        bool ascending = (index_scan->_sort_order == JIT_QUERY_SORT_ASCENDING);

        int* index_column_offsets = nullptr;
        const uint16_t* key_length = nullptr;
        int index_column_count = 0;
        if (range_scan_type == JIT_RANGE_SCAN_INNER) {
            index_column_offsets = ctx->m_innerTable_info.m_indexColumnOffsets;
            key_length = ctx->m_innerTable_info.m_index->GetLengthKeyFields();
            index_column_count = ctx->m_innerTable_info.m_index->GetNumFields();
        } else {
            index_column_offsets = ctx->_table_info.m_indexColumnOffsets;
            key_length = ctx->_table_info.m_index->GetLengthKeyFields();
            index_column_count = ctx->_table_info.m_index->GetNumFields();
        }

        // prepare offset and size for last column in search
        int last_dim_column = index_scan->_column_count - 1;
        int offset = index_column_offsets[last_dim_column];
        int size = key_length[last_dim_column];

        // now we fill the last dimension (override extra work of point scan above)
        int last_expr_index = index_scan->_search_exprs._count - 1;
        JitColumnExpr* last_expr = &index_scan->_search_exprs._exprs[last_expr_index];
        if (ascending) {
            if ((index_scan->_last_dim_op1 == JIT_WOC_LESS_THAN) ||
                (index_scan->_last_dim_op1 == JIT_WOC_LESS_EQUALS)) {
                // this is an upper bound operator on an ascending semi-open scan so we fill the begin key with zeros,
                // and the end key with the value
                AddFillKeyPattern(ctx, 0x00, offset, size, JIT_RANGE_ITERATOR_START, range_scan_type);
                buildScanExpression(ctx, last_expr, max_arg, JIT_RANGE_ITERATOR_END, range_scan_type, outer_row);
                *begin_range_bound = JIT_RANGE_BOUND_INCLUDE;
                *end_range_bound = (index_scan->_last_dim_op1 == JIT_WOC_LESS_EQUALS) ? JIT_RANGE_BOUND_INCLUDE
                                                                                      : JIT_RANGE_BOUND_EXCLUDE;
            } else {
                // this is a lower bound operator on an ascending semi-open scan so we fill the begin key with the
                // value, and the end key with 0xFF
                buildScanExpression(ctx, last_expr, max_arg, JIT_RANGE_ITERATOR_START, range_scan_type, outer_row);
                AddFillKeyPattern(ctx, 0xFF, offset, size, JIT_RANGE_ITERATOR_END, range_scan_type);
                *begin_range_bound = (index_scan->_last_dim_op1 == JIT_WOC_GREATER_EQUALS) ? JIT_RANGE_BOUND_INCLUDE
                                                                                           : JIT_RANGE_BOUND_EXCLUDE;
                *end_range_bound = JIT_RANGE_BOUND_INCLUDE;
            }
        } else {
            if ((index_scan->_last_dim_op1 == JIT_WOC_LESS_THAN) ||
                (index_scan->_last_dim_op1 == JIT_WOC_LESS_EQUALS)) {
                // this is an upper bound operator on a descending semi-open scan so we fill the begin key with value,
                // and the end key with zeroes
                buildScanExpression(ctx, last_expr, max_arg, JIT_RANGE_ITERATOR_START, range_scan_type, outer_row);
                AddFillKeyPattern(ctx, 0x00, offset, size, JIT_RANGE_ITERATOR_END, range_scan_type);
                *begin_range_bound = (index_scan->_last_dim_op1 == JIT_WOC_LESS_EQUALS) ? JIT_RANGE_BOUND_INCLUDE
                                                                                        : JIT_RANGE_BOUND_EXCLUDE;
                *end_range_bound = JIT_RANGE_BOUND_INCLUDE;
            } else {
                // this is a lower bound operator on a descending semi-open scan so we fill the begin key with 0xFF, and
                // the end key with the value
                AddFillKeyPattern(ctx, 0xFF, offset, size, JIT_RANGE_ITERATOR_START, range_scan_type);
                buildScanExpression(ctx, last_expr, max_arg, JIT_RANGE_ITERATOR_END, range_scan_type, outer_row);
                *begin_range_bound = JIT_RANGE_BOUND_INCLUDE;
                *end_range_bound = (index_scan->_last_dim_op1 == JIT_WOC_GREATER_EQUALS) ? JIT_RANGE_BOUND_INCLUDE
                                                                                         : JIT_RANGE_BOUND_EXCLUDE;
            }
        }

        // now fill the rest as usual
        int first_zero_column = index_scan->_column_count;
        for (int i = first_zero_column; i < index_column_count; ++i) {
            int col_offset = index_column_offsets[i];
            int key_len = key_length[i];
            MOT_LOG_DEBUG("Filling begin/end iterator pattern for missing pkey fields at offset %d, size %d",
                col_offset,
                key_len);
            AddFillKeyPattern(
                ctx, ascending ? 0x00 : 0xFF, col_offset, key_len, JIT_RANGE_ITERATOR_START, range_scan_type);
            AddFillKeyPattern(
                ctx, ascending ? 0xFF : 0x00, col_offset, key_len, JIT_RANGE_ITERATOR_END, range_scan_type);
        }

        AddAdjustKey(ctx,
            ascending ? 0x00 : 0xFF,
            JIT_RANGE_ITERATOR_START,
            range_scan_type);  // currently this is relevant only for secondary index searches
        AddAdjustKey(ctx,
            ascending ? 0xFF : 0x00,
            JIT_RANGE_ITERATOR_END,
            range_scan_type);  // currently this is relevant only for secondary index searches
    }
    return result;
}

static bool buildOpenRangeScan(JitTvmCodeGenContext* ctx, JitIndexScan* index_scan, int* max_arg,
    JitRangeScanType range_scan_type, JitRangeBoundMode* begin_range_bound, JitRangeBoundMode* end_range_bound,
    Instruction* outer_row)
{
    // an open range scan starts just like a point scan (with not enough search expressions) and then adds key patterns
    // we do not use the last two expressions
    bool result = buildPointScan(
        ctx, &index_scan->_search_exprs, max_arg, range_scan_type, outer_row, index_scan->_search_exprs._count - 2);
    if (result) {
        AddCopyKey(ctx, range_scan_type);

        // now fill each key with the right pattern for the missing pkey columns in the where clause
        bool ascending = (index_scan->_sort_order == JIT_QUERY_SORT_ASCENDING);

        int* index_column_offsets = nullptr;
        const uint16_t* key_length = nullptr;
        int index_column_count = 0;
        if (range_scan_type == JIT_RANGE_SCAN_INNER) {
            index_column_offsets = ctx->m_innerTable_info.m_indexColumnOffsets;
            key_length = ctx->m_innerTable_info.m_index->GetLengthKeyFields();
            index_column_count = ctx->m_innerTable_info.m_index->GetNumFields();
        } else {
            index_column_offsets = ctx->_table_info.m_indexColumnOffsets;
            key_length = ctx->_table_info.m_index->GetLengthKeyFields();
            index_column_count = ctx->_table_info.m_index->GetNumFields();
        }

        // now we fill the last dimension (override extra work of point scan above)
        JitWhereOperatorClass before_last_dim_op = index_scan->_last_dim_op1;  // avoid confusion, and give proper names
        JitWhereOperatorClass last_dim_op = index_scan->_last_dim_op2;         // avoid confusion, and give proper names
        int last_expr_index = index_scan->_search_exprs._count - 1;
        JitColumnExpr* last_expr = &index_scan->_search_exprs._exprs[last_expr_index];
        JitColumnExpr* before_last_expr = &index_scan->_search_exprs._exprs[last_expr_index - 1];
        if (ascending) {
            if ((before_last_dim_op == JIT_WOC_LESS_THAN) || (before_last_dim_op == JIT_WOC_LESS_EQUALS)) {
                MOT_ASSERT((last_dim_op == JIT_WOC_GREATER_THAN) || (last_dim_op == JIT_WOC_GREATER_EQUALS));
                // the before-last operator is an upper bound operator on an ascending open scan so we fill the begin
                // key with the last value, and the end key with the before-last value
                buildScanExpression(ctx,
                    last_expr,
                    max_arg,
                    JIT_RANGE_ITERATOR_START,
                    range_scan_type,
                    outer_row);  // lower bound on begin iterator key
                buildScanExpression(ctx,
                    before_last_expr,
                    max_arg,
                    JIT_RANGE_ITERATOR_END,
                    range_scan_type,
                    outer_row);  // upper bound on end iterator key
                *begin_range_bound =
                    (last_dim_op == JIT_WOC_GREATER_EQUALS) ? JIT_RANGE_BOUND_INCLUDE : JIT_RANGE_BOUND_EXCLUDE;
                *end_range_bound =
                    (before_last_dim_op == JIT_WOC_LESS_EQUALS) ? JIT_RANGE_BOUND_INCLUDE : JIT_RANGE_BOUND_EXCLUDE;
            } else {
                MOT_ASSERT((last_dim_op == JIT_WOC_LESS_THAN) || (last_dim_op == JIT_WOC_LESS_EQUALS));
                // the before-last operator is a lower bound operator on an ascending open scan so we fill the begin key
                // with the before-last value, and the end key with the last value
                buildScanExpression(ctx,
                    before_last_expr,
                    max_arg,
                    JIT_RANGE_ITERATOR_START,
                    range_scan_type,
                    outer_row);  // lower bound on begin iterator key
                buildScanExpression(ctx,
                    last_expr,
                    max_arg,
                    JIT_RANGE_ITERATOR_END,
                    range_scan_type,
                    outer_row);  // upper bound on end iterator key
                *begin_range_bound =
                    (before_last_dim_op == JIT_WOC_GREATER_EQUALS) ? JIT_RANGE_BOUND_INCLUDE : JIT_RANGE_BOUND_EXCLUDE;
                *end_range_bound =
                    (last_dim_op == JIT_WOC_LESS_EQUALS) ? JIT_RANGE_BOUND_INCLUDE : JIT_RANGE_BOUND_EXCLUDE;
            }
        } else {
            if ((before_last_dim_op == JIT_WOC_LESS_THAN) || (before_last_dim_op == JIT_WOC_LESS_EQUALS)) {
                MOT_ASSERT((last_dim_op == JIT_WOC_GREATER_THAN) || (last_dim_op == JIT_WOC_GREATER_EQUALS));
                // the before-last operator is an upper bound operator on an descending open scan so we fill the begin
                // key with the last value, and the end key with the before-last value
                buildScanExpression(ctx,
                    before_last_expr,
                    max_arg,
                    JIT_RANGE_ITERATOR_START,
                    range_scan_type,
                    outer_row);  // upper bound on begin iterator key
                buildScanExpression(ctx,
                    last_expr,
                    max_arg,
                    JIT_RANGE_ITERATOR_END,
                    range_scan_type,
                    outer_row);  // lower bound on end iterator key
                *begin_range_bound =
                    (before_last_dim_op == JIT_WOC_LESS_EQUALS) ? JIT_RANGE_BOUND_INCLUDE : JIT_RANGE_BOUND_EXCLUDE;
                *end_range_bound =
                    (last_dim_op == JIT_WOC_GREATER_EQUALS) ? JIT_RANGE_BOUND_INCLUDE : JIT_RANGE_BOUND_EXCLUDE;
            } else {
                MOT_ASSERT((last_dim_op == JIT_WOC_LESS_THAN) || (last_dim_op == JIT_WOC_LESS_EQUALS));
                // the before-last operator is a lower bound operator on an descending open scan so we fill the begin
                // key with the last value, and the end key with the before-last value
                buildScanExpression(ctx,
                    last_expr,
                    max_arg,
                    JIT_RANGE_ITERATOR_START,
                    range_scan_type,
                    outer_row);  // upper bound on begin iterator key
                buildScanExpression(ctx,
                    before_last_expr,
                    max_arg,
                    JIT_RANGE_ITERATOR_END,
                    range_scan_type,
                    outer_row);  // lower bound on end iterator key
                *begin_range_bound =
                    (last_dim_op == JIT_WOC_LESS_EQUALS) ? JIT_RANGE_BOUND_INCLUDE : JIT_RANGE_BOUND_EXCLUDE;
                *end_range_bound =
                    (before_last_dim_op == JIT_WOC_GREATER_EQUALS) ? JIT_RANGE_BOUND_INCLUDE : JIT_RANGE_BOUND_EXCLUDE;
            }
        }

        // now fill the rest as usual
        int first_zero_column = index_scan->_column_count;
        for (int i = first_zero_column; i < index_column_count; ++i) {
            int offset = index_column_offsets[i];
            int size = key_length[i];
            MOT_LOG_DEBUG(
                "Filling begin/end iterator pattern for missing pkey fields at offset %d, size %d", offset, size);
            AddFillKeyPattern(ctx, ascending ? 0x00 : 0xFF, offset, size, JIT_RANGE_ITERATOR_START, range_scan_type);
            AddFillKeyPattern(ctx, ascending ? 0xFF : 0x00, offset, size, JIT_RANGE_ITERATOR_END, range_scan_type);
        }

        AddAdjustKey(ctx,
            ascending ? 0x00 : 0xFF,
            JIT_RANGE_ITERATOR_START,
            range_scan_type);  // currently this is relevant only for secondary index searches
        AddAdjustKey(ctx,
            ascending ? 0xFF : 0x00,
            JIT_RANGE_ITERATOR_END,
            range_scan_type);  // currently this is relevant only for secondary index searches
    }
    return result;
}

static bool buildRangeScan(JitTvmCodeGenContext* ctx, JitIndexScan* index_scan, int* max_arg,
    JitRangeScanType range_scan_type, JitRangeBoundMode* begin_range_bound, JitRangeBoundMode* end_range_bound,
    Instruction* outer_row)
{
    bool result = false;

    // if this is a point scan we generate two identical keys for the iterators
    if (index_scan->_scan_type == JIT_INDEX_SCAN_POINT) {
        result = buildPointScan(ctx, &index_scan->_search_exprs, max_arg, range_scan_type, outer_row);
        if (result) {
            AddCopyKey(ctx, range_scan_type);
            *begin_range_bound = JIT_RANGE_BOUND_INCLUDE;
            *end_range_bound = JIT_RANGE_BOUND_INCLUDE;
        }
    } else if (index_scan->_scan_type == JIT_INDEX_SCAN_CLOSED) {
        result = buildClosedRangeScan(ctx, index_scan, max_arg, range_scan_type, outer_row);
        if (result) {
            *begin_range_bound = JIT_RANGE_BOUND_INCLUDE;
            *end_range_bound = JIT_RANGE_BOUND_INCLUDE;
        }
    } else if (index_scan->_scan_type == JIT_INDEX_SCAN_SEMI_OPEN) {
        result = buildSemiOpenRangeScan(
            ctx, index_scan, max_arg, range_scan_type, begin_range_bound, end_range_bound, outer_row);
    } else if (index_scan->_scan_type == JIT_INDEX_SCAN_OPEN) {
        result = buildOpenRangeScan(
            ctx, index_scan, max_arg, range_scan_type, begin_range_bound, end_range_bound, outer_row);
    }
    return result;
}

static bool buildPrepareStateScan(JitTvmCodeGenContext* ctx, JitIndexScan* index_scan, int* max_arg,
    JitRangeScanType range_scan_type, Instruction* outer_row)
{
    JitRangeBoundMode begin_range_bound = JIT_RANGE_BOUND_NONE;
    JitRangeBoundMode end_range_bound = JIT_RANGE_BOUND_NONE;

    // emit code to check if state iterators are null
    JIT_IF_BEGIN(state_iterators_exist)
    Instruction* is_begin_itr_null = AddIsStateIteratorNull(ctx, JIT_RANGE_ITERATOR_START, range_scan_type);
    JIT_IF_EVAL(is_begin_itr_null)
    // prepare search keys
    if (!buildRangeScan(ctx, index_scan, max_arg, range_scan_type, &begin_range_bound, &end_range_bound, outer_row)) {
        MOT_LOG_TRACE("Failed to generate jitted code for range select query: unsupported WHERE clause type");
        return false;
    }

    // search begin iterator and save it in execution state
    IssueDebugLog("Building search iterator from search key, and saving in execution state");
    Instruction* itr = buildSearchIterator(ctx, index_scan->_scan_direction, begin_range_bound, range_scan_type);
    AddSetStateIterator(ctx, itr, JIT_RANGE_ITERATOR_START, range_scan_type);

    // create end iterator and save it in execution state
    IssueDebugLog("Creating end iterator from end search key, and saving in execution state");
    itr = AddCreateEndIterator(ctx, index_scan->_scan_direction, end_range_bound, range_scan_type);
    AddSetStateIterator(ctx, itr, JIT_RANGE_ITERATOR_END, range_scan_type);
    if (range_scan_type == JIT_RANGE_SCAN_MAIN) {
        AddResetStateLimitCounter(ctx);              // in case there is a limit clause
        AddResetStateRow(ctx, JIT_RANGE_SCAN_MAIN);  // in case this is a join query
        AddSetStateScanEndFlag(
            ctx, 0, JIT_RANGE_SCAN_MAIN);  // reset state flag (only once, and not repeatedly if row filter failed)
    }
    JIT_IF_END()

    return true;
}

static bool buildPrepareStateRow(JitTvmCodeGenContext* ctx, MOT::AccessType access_mode, JitIndexScan* index_scan,
    int* max_arg, JitRangeScanType range_scan_type, BasicBlock* next_block)
{
    MOT_LOG_DEBUG("Generating select code for stateful range select");
    Instruction* row = nullptr;

    // we start a new block so current block must end with terminator
    DEFINE_BLOCK(prepare_state_row_bb, ctx->m_jittedQuery);
    ctx->_builder->CreateBr(prepare_state_row_bb);
    ctx->_builder->SetInsertPoint(prepare_state_row_bb);

    // check if state row is null
    JIT_IF_BEGIN(test_state_row)
    IssueDebugLog("Checking if state row is NULL");
    Instruction* res = AddIsStateRowNull(ctx, range_scan_type);
    JIT_IF_EVAL(res)
    IssueDebugLog("State row is NULL, fetching from state iterators");

    // check if state scan ended
    JIT_IF_BEGIN(test_scan)
    IssueDebugLog("Checking if state scan ended");
    Instruction* res_scan_end = AddIsStateScanEnd(ctx, index_scan->_scan_direction, range_scan_type);
    JIT_IF_EVAL(res_scan_end)
    // fail scan block
    IssueDebugLog("Scan ended, raising internal state scan end flag");
    AddSetStateScanEndFlag(ctx, 1, range_scan_type);
    JIT_ELSE()
    // now get row from iterator (remember current block if filter fails)
    BasicBlock* get_row_from_itr_block = JIT_CURRENT_BLOCK();
    IssueDebugLog("State scan not ended - Retrieving row from iterator");
    row = AddGetRowFromStateIterator(ctx, access_mode, index_scan->_scan_direction, range_scan_type);

    // check if row was found
    JIT_IF_BEGIN(test_row_found)
    JIT_IF_EVAL_NOT(row)
    // row not found branch
    IssueDebugLog("Could not retrieve row from state iterator, raising internal state scan end flag");
    AddSetStateScanEndFlag(ctx, 1, range_scan_type);
    JIT_ELSE()
    // row found, check for additional filters, if not passing filter then go back to execute getRowFromStateIterator()
    if (!buildFilterRow(ctx, row, &index_scan->_filters, max_arg, get_row_from_itr_block)) {
        MOT_LOG_TRACE("Failed to generate jitted code for query: failed to build filter expressions for row");
        return false;
    }
    // row passed all filters, so save it in state outer row
    AddSetStateRow(ctx, row, range_scan_type);
    if (range_scan_type == JIT_RANGE_SCAN_MAIN) {
        AddSetStateScanEndFlag(ctx, 0, range_scan_type);  // reset inner scan flag
    }
    JIT_IF_END()
    JIT_IF_END()
    JIT_IF_END()

    // cleanup state iterators if needed and return/jump to next block
    JIT_IF_BEGIN(state_scan_ended)
    IssueDebugLog("Checking if state scan ended flag was raised");
    Instruction* state_scan_end_flag = AddGetStateScanEndFlag(ctx, range_scan_type);
    JIT_IF_EVAL(state_scan_end_flag)
    IssueDebugLog(" State scan ended flag was raised, cleaning up iterators and reporting to user");
    AddDestroyStateIterators(ctx, range_scan_type);  // cleanup
    if ((range_scan_type == JIT_RANGE_SCAN_MAIN) || (next_block == nullptr)) {
        // either a main scan ended (simple range or outer loop of join),
        // or an inner range ended in an outer point join (so next block is null)
        // in either case let caller know scan ended and return appripriate value
        AddSetScanEnded(ctx, 1);  // no outer row, we are definitely done
        JIT_RETURN_CONST(MOT::RC_LOCAL_ROW_NOT_FOUND);
    } else {
        AddResetStateRow(ctx, JIT_RANGE_SCAN_MAIN);  // make sure a new row is fetched in outer loop
        ctx->_builder->CreateBr(next_block);         // jump back to outer loop
    }
    JIT_IF_END()

    return true;
}

static Instruction* buildPrepareStateScanRow(JitTvmCodeGenContext* ctx, JitIndexScan* index_scan,
    JitRangeScanType range_scan_type, MOT::AccessType access_mode, int* max_arg, Instruction* outer_row,
    BasicBlock* next_block, BasicBlock** loop_block)
{
    // prepare stateful scan if not done so already
    if (!buildPrepareStateScan(ctx, index_scan, max_arg, range_scan_type, outer_row)) {
        MOT_LOG_TRACE("Failed to generate jitted code for range JOIN query: unsupported %s WHERE clause type",
            range_scan_type == JIT_RANGE_SCAN_MAIN ? "outer" : "inner");
        return nullptr;
    }

    // mark position for later jump
    if (loop_block != nullptr) {
        *loop_block = ctx->_builder->CreateBlock("fetch_outer_row_bb");
        ctx->_builder->CreateBr(*loop_block);        // end current block
        ctx->_builder->SetInsertPoint(*loop_block);  // start new block
    }

    // fetch row for read
    if (!buildPrepareStateRow(ctx, access_mode, index_scan, max_arg, range_scan_type, next_block)) {
        MOT_LOG_TRACE("Failed to generate jitted code for range JOIN query: failed to build search outer row block");
        return nullptr;
    }
    Instruction* row = AddGetStateRow(ctx, range_scan_type);
    return row;
}

static JitTvmRuntimeCursor buildRangeCursor(JitTvmCodeGenContext* ctx, JitIndexScan* index_scan, int* max_arg,
    JitRangeScanType range_scan_type, JitIndexScanDirection index_scan_direction, Instruction* outer_row)
{
    JitTvmRuntimeCursor result = {nullptr, nullptr};
    JitRangeBoundMode begin_range_bound = JIT_RANGE_BOUND_NONE;
    JitRangeBoundMode end_range_bound = JIT_RANGE_BOUND_NONE;
    if (!buildRangeScan(ctx, index_scan, max_arg, range_scan_type, &begin_range_bound, &end_range_bound, outer_row)) {
        MOT_LOG_TRACE(
            "Failed to generate jitted code for aggregate range JOIN query: unsupported %s-loop WHERE clause type",
            outer_row ? "inner" : "outer");
        return result;
    }

    // build range iterators
    result.begin_itr = buildSearchIterator(ctx, index_scan_direction, begin_range_bound, range_scan_type);
    result.end_itr = AddCreateEndIterator(ctx, index_scan_direction, end_range_bound, range_scan_type);  // forward scan

    return result;
}

static bool prepareAggregateAvg(JitTvmCodeGenContext* ctx, const JitAggregate* aggregate)
{
    // although we already have this information in the aggregate descriptor, we still check again
    switch (aggregate->_func_id) {
        case INT8AVGFUNCOID:
        case NUMERICAVGFUNCOID:
            // the current_aggregate is a 2 numeric array
            AddPrepareAvgArray(ctx, NUMERICOID, 2);
            break;

        case INT4AVGFUNCOID:
        case INT2AVGFUNCOID:
        case 5537:  // int1 avg
            // the current_aggregate is a 2 int8 array
            AddPrepareAvgArray(ctx, INT8OID, 2);
            break;

        case 2104:  // float4
        case 2105:  // float8
            // the current_aggregate is a 3 float8 array
            AddPrepareAvgArray(ctx, FLOAT8OID, 3);
            break;

        default:
            MOT_LOG_TRACE("Unsupported aggregate AVG() operator function type: %d", aggregate->_func_id);
            return false;
    }

    return true;
}

static bool prepareAggregateSum(JitTvmCodeGenContext* ctx, const JitAggregate* aggregate)
{
    switch (aggregate->_func_id) {
        case INT8SUMFUNCOID:
            // current sum in numeric, and value is int8, both can be null
            AddResetAggValue(ctx, NUMERICOID);
            break;

        case INT4SUMFUNCOID:
        case INT2SUMFUNCOID:
            // current aggregate is a int8, and value is int4, both can be null
            AddResetAggValue(ctx, INT8OID);
            break;

        case 2110:  // float4
            // current aggregate is a float4, and value is float4, both can **NOT** be null
            AddResetAggValue(ctx, FLOAT4OID);
            break;

        case 2111:  // float8
            // current aggregate is a float8, and value is float8, both can **NOT** be null
            AddResetAggValue(ctx, FLOAT8OID);
            break;

        case NUMERICSUMFUNCOID:
            AddResetAggValue(ctx, NUMERICOID);
            // current aggregate is a numeric, and value is numeric, both can **NOT** be null
            break;

        default:
            MOT_LOG_TRACE("Unsupported aggregate SUM() operator function type: %d", aggregate->_func_id);
            return false;
    }
    return true;
}

static bool prepareAggregateMaxMin(JitTvmCodeGenContext* ctx, JitAggregate* aggregate)
{
    AddResetAggMaxMinNull(ctx);
    return true;
}

static bool prepareAggregateCount(JitTvmCodeGenContext* ctx, const JitAggregate* aggregate)
{
    switch (aggregate->_func_id) {
        case 2147:  // int8inc_any
        case 2803:  // int8inc
            // current aggregate is int8, and can **NOT** be null
            AddResetAggValue(ctx, INT8OID);
            break;

        default:
            MOT_LOG_TRACE("Unsupported aggregate COUNT() operator function type: %d", aggregate->_func_id);
            return false;
    }
    return true;
}

static bool prepareDistinctSet(JitTvmCodeGenContext* ctx, const JitAggregate* aggregate)
{
    // we need a hash-set according to the aggregated type (preferably but not necessarily linear-probing hash)
    // we use an opaque datum type, with a tailor-made hash-function and equals function
    AddPrepareDistinctSet(ctx, aggregate->_element_type);
    return true;
}

static bool prepareAggregate(JitTvmCodeGenContext* ctx, JitAggregate* aggregate)
{
    bool result = false;

    switch (aggregate->_aggreaget_op) {
        case JIT_AGGREGATE_AVG:
            result = prepareAggregateAvg(ctx, aggregate);
            break;

        case JIT_AGGREGATE_SUM:
            result = prepareAggregateSum(ctx, aggregate);
            break;

        case JIT_AGGREGATE_MAX:
        case JIT_AGGREGATE_MIN:
            result = prepareAggregateMaxMin(ctx, aggregate);
            break;

        case JIT_AGGREGATE_COUNT:
            result = prepareAggregateCount(ctx, aggregate);
            break;

        default:
            MOT_REPORT_ERROR(MOT_ERROR_INTERNAL,
                "JIT Compile",
                "Cannot prepare for aggregation: invalid aggregate operator %d",
                (int)aggregate->_aggreaget_op);
            break;
    }

    if (result) {
        if (aggregate->_distinct) {
            result = prepareDistinctSet(ctx, aggregate);
        }
    }

    return result;
}

static Expression* buildAggregateAvg(
    JitTvmCodeGenContext* ctx, const JitAggregate* aggregate, Expression* current_aggregate, Expression* var_expr)
{
    Expression* aggregate_expr = nullptr;
    switch (aggregate->_func_id) {
        case INT8AVGFUNCOID:
            // the current_aggregate is a 2 numeric array, and the var expression should evaluate to int8
            aggregate_expr = new (std::nothrow) int8_avg_accumOperator(current_aggregate, var_expr);
            break;

        case INT4AVGFUNCOID:
            // the current_aggregate is a 2 int8 array, and the var expression should evaluate to int4
            // we can save a palloc by ensuring "AggCheckCallContext(fcinfo, NULL)" return true
            aggregate_expr = new (std::nothrow) int4_avg_accumOperator(current_aggregate, var_expr);
            break;

        case INT2AVGFUNCOID:
            // the current_aggregate is a 2 int8 array, and the var expression should evaluate to int2
            // we can save a palloc by ensuring "AggCheckCallContext(fcinfo, NULL)" return true
            aggregate_expr = new (std::nothrow) int2_avg_accumOperator(current_aggregate, var_expr);
            break;

        case 5537:
            // the current_aggregate is a 2 int8 array, and the var expression should evaluate to int1
            // we can save a palloc by ensuring "AggCheckCallContext(fcinfo, NULL)" return true
            aggregate_expr = new (std::nothrow) int1_avg_accumOperator(current_aggregate, var_expr);
            break;

        case 2104:  // float4
            // the current_aggregate is a 3 float8 array, and the var expression should evaluate to float4
            // the function computes 3 values: count, sum, square sum
            // we can save a palloc by ensuring "AggCheckCallContext(fcinfo, NULL)" return true
            aggregate_expr = new (std::nothrow) float4_accumOperator(current_aggregate, var_expr);
            break;

        case 2105:  // float8
            // the current_aggregate is a 3 float8 array, and the var expression should evaluate to float8
            // the function computes 3 values: count, sum, square sum
            // we can save a palloc by ensuring "AggCheckCallContext(fcinfo, NULL)" return true
            aggregate_expr = new (std::nothrow) float8_accumOperator(current_aggregate, var_expr);
            break;

        case NUMERICAVGFUNCOID:
            // the current_aggregate is a 2 numeric array, and the var expression should evaluate to numeric
            aggregate_expr = new (std::nothrow) numeric_avg_accumOperator(current_aggregate, var_expr);
            break;

        default:
            MOT_LOG_TRACE("Unsupported aggregate AVG() operator function type: %d", aggregate->_func_id);
            break;
    }

    return aggregate_expr;
}

static Expression* buildAggregateSum(
    JitTvmCodeGenContext* ctx, const JitAggregate* aggregate, Expression* current_aggregate, Expression* var_expr)
{
    Expression* aggregate_expr = nullptr;
    switch (aggregate->_func_id) {
        case INT8SUMFUNCOID:
            // current sum in numeric, and next value is int8, both can be null
            aggregate_expr = new (std::nothrow) int8_sumOperator(current_aggregate, var_expr);
            break;

        case INT4SUMFUNCOID:
            // current aggregate is a int8, and value is int4, both can be null
            aggregate_expr = new (std::nothrow) int4_sumOperator(current_aggregate, var_expr);
            break;

        case INT2SUMFUNCOID:
            // current aggregate is a int8, and value is int3, both can be null
            aggregate_expr = new (std::nothrow) int2_sumOperator(current_aggregate, var_expr);
            break;

        case 2110:  // float4
            // current aggregate is a float4, and value is float4, both can **NOT** be null
            aggregate_expr = new (std::nothrow) float4plOperator(current_aggregate, var_expr);
            break;

        case 2111:  // float8
            // current aggregate is a float4, and value is float4, both can **NOT** be null
            aggregate_expr = new (std::nothrow) float8plOperator(current_aggregate, var_expr);
            break;

        case NUMERICSUMFUNCOID:
            // current aggregate is a numeric, and value is numeric, both can **NOT** be null
            aggregate_expr = new (std::nothrow) numeric_addOperator(current_aggregate, var_expr);
            break;

        default:
            MOT_LOG_TRACE("Unsupported aggregate SUM() operator function type: %d", aggregate->_func_id);
            break;
    }

    return aggregate_expr;
}

static Expression* buildAggregateMax(
    JitTvmCodeGenContext* ctx, const JitAggregate* aggregate, Expression* current_aggregate, Expression* var_expr)
{
    Expression* aggregate_expr = nullptr;
    switch (aggregate->_func_id) {
        case INT8LARGERFUNCOID:
            // current aggregate is a int8, and value is int8, both can **NOT** be null
            aggregate_expr = new (std::nothrow) int8largerOperator(current_aggregate, var_expr);
            break;

        case INT4LARGERFUNCOID:
            // current aggregate is a int4, and value is int4, both can **NOT** be null
            aggregate_expr = new (std::nothrow) int4largerOperator(current_aggregate, var_expr);
            break;

        case INT2LARGERFUNCOID:
            // current aggregate is a int2, and value is int2, both can **NOT** be null
            aggregate_expr = new (std::nothrow) int2largerOperator(current_aggregate, var_expr);
            break;

        case 5538:
            // current aggregate is a int1, and value is int1, both can **NOT** be null
            aggregate_expr = new (std::nothrow) int1largerOperator(current_aggregate, var_expr);
            break;

        case 2119:  // float4larger
            // current aggregate is a float4, and value is float4, both can **NOT** be null
            aggregate_expr = new (std::nothrow) float4largerOperator(current_aggregate, var_expr);
            break;

        case 2120:  // float8larger
            // current aggregate is a float8, and value is float8, both can **NOT** be null
            aggregate_expr = new (std::nothrow) float8largerOperator(current_aggregate, var_expr);
            break;

        case NUMERICLARGERFUNCOID:
            // current aggregate is a numeric, and value is numeric, both can **NOT** be null
            aggregate_expr = new (std::nothrow) numeric_largerOperator(current_aggregate, var_expr);
            break;

        case 2126:
            // current aggregate is a timestamp, and value is timestamp, both can **NOT** be null
            aggregate_expr = new (std::nothrow) timestamp_largerOperator(current_aggregate, var_expr);
            break;

        case 2122:
            // current aggregate is a date, and value is date, both can **NOT** be null
            aggregate_expr = new (std::nothrow) date_largerOperator(current_aggregate, var_expr);
            break;

        case 2244:
            // current aggregate is a bpchar, and value is bpchar, both can **NOT** be null
            aggregate_expr = new (std::nothrow) bpchar_largerOperator(current_aggregate, var_expr);
            break;

        case 2129:
            // current aggregate is a text, and value is text, both can **NOT** be null
            aggregate_expr = new (std::nothrow) text_largerOperator(current_aggregate, var_expr);
            break;

        default:
            MOT_LOG_TRACE("Unsupported aggregate MAX() operator function type: %d", aggregate->_func_id);
            break;
    }

    return aggregate_expr;
}

static Expression* buildAggregateMin(
    JitTvmCodeGenContext* ctx, const JitAggregate* aggregate, Expression* current_aggregate, Expression* var_expr)
{
    Expression* aggregate_expr = nullptr;
    switch (aggregate->_func_id) {
        case INT8SMALLERFUNCOID:
            // current sum is a int8, and value is int8, both can **NOT** be null
            aggregate_expr = new (std::nothrow) int8smallerOperator(current_aggregate, var_expr);
            break;

        case INT4SMALLERFUNCOID:
            // current aggregate is a int4, and value is int4, both can **NOT** be null
            aggregate_expr = new (std::nothrow) int4smallerOperator(current_aggregate, var_expr);
            break;

        case INT2SMALLERFUNCOID:
            // current aggregate is a int2, and value is int2, both can **NOT** be null
            aggregate_expr = new (std::nothrow) int2smallerOperator(current_aggregate, var_expr);
            break;

        case 2135:  // float4smaller
            // current aggregate is a float4, and value is float4, both can **NOT** be null
            aggregate_expr = new (std::nothrow) float4smallerOperator(current_aggregate, var_expr);
            break;

        case 2120:  // float8smaller
            // current aggregate is a float8, and value is float8, both can **NOT** be null
            aggregate_expr = new (std::nothrow) float8smallerOperator(current_aggregate, var_expr);
            break;

        case NUMERICSMALLERFUNCOID:
            // current aggregate is a numeric, and value is numeric, both can **NOT** be null
            aggregate_expr = new (std::nothrow) numeric_smallerOperator(current_aggregate, var_expr);
            break;

        case 2142:
            // current aggregate is a timestamp, and value is timestamp, both can **NOT** be null
            aggregate_expr = new (std::nothrow) timestamp_smallerOperator(current_aggregate, var_expr);
            break;

        case 2138:
            // current aggregate is a date, and value is date, both can **NOT** be null
            aggregate_expr = new (std::nothrow) date_smallerOperator(current_aggregate, var_expr);
            break;

        case 2245:
            // current aggregate is a bpchar, and value is bpchar, both can **NOT** be null
            aggregate_expr = new (std::nothrow) bpchar_smallerOperator(current_aggregate, var_expr);
            break;

        case 2145:
            // current aggregate is a text, and value is text, both can **NOT** be null
            aggregate_expr = new (std::nothrow) text_smallerOperator(current_aggregate, var_expr);
            break;

        default:
            MOT_LOG_TRACE("Unsupported aggregate MIN() operator function type: %d", aggregate->_func_id);
            break;
    }

    return aggregate_expr;
}

static Expression* buildAggregateCount(
    JitTvmCodeGenContext* ctx, const JitAggregate* aggregate, Expression* count_aggregate)
{
    Expression* aggregate_expr = nullptr;
    switch (aggregate->_func_id) {
        case 2147:  // int8inc_any
            // current aggregate is int8, and can **NOT** be null
            aggregate_expr = new (std::nothrow) int8incOperator(count_aggregate);
            break;

        case 2803:  // int8inc
            // current aggregate is int8, and can **NOT** be null
            aggregate_expr = new (std::nothrow) int8incOperator(count_aggregate);
            break;

        default:
            MOT_LOG_TRACE("Unsupported aggregate COUNT() operator function type: %d", aggregate->_func_id);
            break;
    }

    return aggregate_expr;
}

static bool buildAggregateMaxMin(JitTvmCodeGenContext* ctx, JitAggregate* aggregate, Expression* var_expr)
{
    bool result = true;

    // we first check if the min/max value is null and if so just store the column value
    JIT_IF_BEGIN(test_max_min_value_null)
    Instruction* res = AddGetAggMaxMinIsNull(ctx);
    JIT_IF_EVAL(res)
    AddSetAggValue(ctx, var_expr);
    AddSetAggMaxMinNotNull(ctx);
    JIT_ELSE()
    // get the aggregated value and call the operator
    Expression* current_aggregate = AddGetAggValue(ctx);
    switch (aggregate->_aggreaget_op) {
        case JIT_AGGREGATE_MAX:
            result = buildAggregateMax(ctx, aggregate, current_aggregate, var_expr);
            break;

        case JIT_AGGREGATE_MIN:
            result = buildAggregateMin(ctx, aggregate, current_aggregate, var_expr);
            break;

        default:
            MOT_REPORT_ERROR(
                MOT_ERROR_INTERNAL, "JIT Compile", "Invalid aggregate operator %d", (int)aggregate->_aggreaget_op);
            result = false;
            break;
    }
    JIT_IF_END()

    return result;
}

static bool buildAggregateTuple(JitTvmCodeGenContext* ctx, JitAggregate* aggregate, Expression* var_expr)
{
    bool result = false;

    if ((aggregate->_aggreaget_op == JIT_AGGREGATE_MAX) || (aggregate->_aggreaget_op == JIT_AGGREGATE_MIN)) {
        result = buildAggregateMaxMin(ctx, aggregate, var_expr);
    } else {
        // get the aggregated value
        Expression* current_aggregate = nullptr;
        if (aggregate->_aggreaget_op == JIT_AGGREGATE_AVG) {
            current_aggregate = AddLoadAvgArray(ctx);
        } else {
            current_aggregate = AddGetAggValue(ctx);
        }

        // the operators below take care of null inputs
        Expression* aggregate_expr = nullptr;
        switch (aggregate->_aggreaget_op) {
            case JIT_AGGREGATE_AVG:
                aggregate_expr = buildAggregateAvg(ctx, aggregate, current_aggregate, var_expr);
                break;

            case JIT_AGGREGATE_SUM:
                aggregate_expr = buildAggregateSum(ctx, aggregate, current_aggregate, var_expr);
                break;

            case JIT_AGGREGATE_COUNT:
                aggregate_expr = buildAggregateCount(ctx, aggregate, current_aggregate);
                break;

            default:
                MOT_REPORT_ERROR(
                    MOT_ERROR_INTERNAL, "JIT Compile", "Invalid aggregate operator %d", (int)aggregate->_aggreaget_op);
                break;
        }

        // write back the sum to the aggregated value/array
        if (aggregate_expr != nullptr) {
            if (aggregate->_aggreaget_op == JIT_AGGREGATE_AVG) {
                AddSaveAvgArray(ctx, aggregate_expr);
            } else {
                AddSetAggValue(ctx, aggregate_expr);
            }
            result = true;
        } else {
            // cleanup
            delete current_aggregate;
            delete var_expr;
            MOT_LOG_TRACE("Failed to generate aggregate AVG/SUM/COUNT code");
        }
    }

    return result;
}

static bool buildAggregateRow(
    JitTvmCodeGenContext* ctx, JitAggregate* aggregate, Instruction* row, BasicBlock* next_block)
{
    bool result = false;

    // extract the aggregated column
    Expression* expr = AddReadDatumColumn(ctx, aggregate->_table, row, aggregate->_table_column_id, 0);

    // we first check if we have DISTINCT modifier
    if (aggregate->_distinct) {
        // check row is distinct in state set (managed on the current jit context)
        // emit code to convert column to primitive data type, add value to distinct set and verify
        // if value is not distinct then do not use this row in aggregation
        JIT_IF_BEGIN(test_distinct_value)
        Instruction* res = AddInsertDistinctItem(ctx, aggregate->_element_type, expr);
        JIT_IF_EVAL_NOT(res)
        IssueDebugLog("Value is not distinct, skipping aggregated row");
        ctx->_builder->CreateBr(next_block);
        JIT_IF_END()
    }

    // aggregate row
    IssueDebugLog("Aggregating row into inner value");
    result = buildAggregateTuple(ctx, aggregate, expr);
    if (result) {
        // update number of rows processes
        buildIncrementRowsProcessed(ctx);
    }

    return result;
}

static void buildAggregateResult(JitTvmCodeGenContext* ctx, const JitAggregate* aggregate)
{
    // in case of average we compute it when aggregate loop is done
    if (aggregate->_aggreaget_op == JIT_AGGREGATE_AVG) {
        Instruction* avg_value = AddComputeAvgFromArray(
            ctx, aggregate->_avg_element_type);  // we infer this during agg op analysis, but don't save it...
        AddWriteTupleDatum(ctx, 0, avg_value);   // we alway aggregate to slot tuple 0
    } else {
        Expression* count_expr = AddGetAggValue(ctx);
        Instruction* count_value = buildExpression(ctx, count_expr);
        AddWriteTupleDatum(ctx, 0, count_value);  // we alway aggregate to slot tuple 0
    }

    // we take the opportunity to cleanup as well
    if (aggregate->_distinct) {
        AddDestroyDistinctSet(ctx, aggregate->_element_type);
    }
}

static void buildCheckLimit(JitTvmCodeGenContext* ctx, int limit_count)
{
    // if a limit clause exists, then increment limit counter and check if reached limit
    if (limit_count > 0) {
        AddIncrementStateLimitCounter(ctx);
        JIT_IF_BEGIN(limit_count_reached)
        Instruction* current_limit_count = AddGetStateLimitCounter(ctx);
        Instruction* limit_count_inst = JIT_CONST(limit_count);
        JIT_IF_EVAL_CMP(current_limit_count, limit_count_inst, JIT_ICMP_EQ);
        IssueDebugLog("Reached limit specified in limit clause, raising internal state scan end flag");
        // cleanup and signal scan ended (it is safe to cleanup even if not initialized, so we cleanup everything)
        AddDestroyStateIterators(ctx, JIT_RANGE_SCAN_MAIN);
        AddDestroyStateIterators(ctx, JIT_RANGE_SCAN_INNER);
        AddSetScanEnded(ctx, 1);
        JIT_IF_END()
    }
}

static bool selectJoinRows(
    JitTvmCodeGenContext* ctx, Instruction* outer_row_copy, Instruction* inner_row, JitJoinPlan* plan, int* max_arg)
{
    // select inner and outer row expressions into result tuple (no aggregate because aggregate is not stateful)
    IssueDebugLog("Retrieved row from state iterator, beginning to select columns into result tuple");
    if (!selectRowColumns(ctx, outer_row_copy, &plan->_select_exprs, max_arg, JIT_RANGE_SCAN_MAIN)) {
        MOT_LOG_TRACE(
            "Failed to generate jitted code for Inner Point JOIN query: failed to select outer row expressions");
        return false;
    }
    if (!selectRowColumns(ctx, inner_row, &plan->_select_exprs, max_arg, JIT_RANGE_SCAN_INNER)) {
        MOT_LOG_TRACE(
            "Failed to generate jitted code for Inner Point JOIN query: failed to select outer row expressions");
        return false;
    }
    return true;
}

static JitContext* JitUpdateCodegen(const Query* query, const char* query_string, JitUpdatePlan* plan)
{
    MOT_LOG_DEBUG("Generating code for MOT update at thread %p", (void*)pthread_self());

    Builder builder;

    JitTvmCodeGenContext cg_ctx = {0};
    MOT::Table* table = plan->_query._table;
    if (!InitCodeGenContext(&cg_ctx, &builder, table, table->GetPrimaryIndex())) {
        return nullptr;
    }
    JitTvmCodeGenContext* ctx = &cg_ctx;

    // prepare the jitted function (declare, get arguments into context and define locals)
    CreateJittedFunction(ctx, "MotJittedUpdate", query_string);
    IssueDebugLog("Starting execution of jitted UPDATE");

    // update is not allowed if we reached soft memory limit
    buildIsSoftMemoryLimitReached(ctx);

    // initialize rows_processed local variable
    buildResetRowsProcessed(ctx);

    // begin the WHERE clause (this is a point query
    int max_arg = 0;
    if (!buildPointScan(ctx, &plan->_query._search_exprs, &max_arg, JIT_RANGE_SCAN_MAIN, nullptr)) {
        MOT_LOG_TRACE("Failed to generate jitted code for update query: unsupported WHERE clause type");
        DestroyCodeGenContext(ctx);
        return nullptr;
    }

    // fetch row for writing
    MOT_LOG_DEBUG("Generating update code for point query");
    Instruction* row = buildSearchRow(ctx, MOT::AccessType::WR, JIT_RANGE_SCAN_MAIN);

    // check for additional filters
    if (!buildFilterRow(ctx, row, &plan->_query._filters, &max_arg, nullptr)) {
        MOT_LOG_TRACE("Failed to generate jitted code for update query: unsupported filter");
        DestroyCodeGenContext(ctx);
        return nullptr;
    }

    // prepare a bitmap array
    IssueDebugLog("Resetting bitmap set for incremental redo");
    AddResetBitmapSet(ctx);

    // now begin updating columns
    IssueDebugLog("Updating row columns");
    if (!writeRowColumns(ctx, row, &plan->_update_exprs, &max_arg, true)) {
        MOT_LOG_TRACE("Failed to generate jitted code for update query: failed to process target entry");
        DestroyCodeGenContext(ctx);
        return nullptr;
    }

    // add code:
    // MOT::Rc rc = writeRow(row, bmp);
    // if (rc != MOT::RC_OK)
    //   return rc;
    IssueDebugLog("Writing row");
    buildWriteRow(ctx, row, true, nullptr);

    // the next call will be executed only if the previous call to writeRow succeeded
    buildIncrementRowsProcessed(ctx);

    // execute *tp_processed = rows_processed
    AddSetTpProcessed(ctx);

    // signal to envelope executor scan ended
    AddSetScanEnded(ctx, 1);

    // return success from calling function
    builder.CreateRet(builder.CreateConst((uint64_t)MOT::RC_OK));

    // wrap up
    JitContext* jit_context = FinalizeCodegen(ctx, max_arg, JIT_COMMAND_UPDATE);

    // cleanup
    DestroyCodeGenContext(ctx);

    return jit_context;
}

static JitContext* JitRangeUpdateCodegen(const Query* query, const char* query_string, JitRangeUpdatePlan* plan)
{
    MOT_LOG_DEBUG("Generating code for MOT range update at thread %p", (void*)pthread_self());

    Builder builder;

    JitTvmCodeGenContext cg_ctx = {0};
    MOT::Table* table = plan->_index_scan._table;
    if (!InitCodeGenContext(&cg_ctx, &builder, table, table->GetPrimaryIndex())) {
        return nullptr;
    }
    JitTvmCodeGenContext* ctx = &cg_ctx;

    // prepare the jitted function (declare, get arguments into context and define locals)
    CreateJittedFunction(ctx, "MotJittedRangeUpdate", query_string);
    IssueDebugLog("Starting execution of jitted range UPDATE");

    // update is not allowed if we reached soft memory limit
    buildIsSoftMemoryLimitReached(ctx);

    // initialize rows_processed local variable
    buildResetRowsProcessed(ctx);

    // begin the WHERE clause
    int max_arg = 0;
    MOT_LOG_DEBUG("Generating range cursor for range UPDATE query");
    JitTvmRuntimeCursor cursor =
        buildRangeCursor(ctx, &plan->_index_scan, &max_arg, JIT_RANGE_SCAN_MAIN, JIT_INDEX_SCAN_FORWARD, nullptr);
    if (cursor.begin_itr == nullptr) {
        MOT_LOG_TRACE("Failed to generate jitted code for range UPDATE query: unsupported WHERE clause type");
        DestroyCodeGenContext(ctx);
        return nullptr;
    }

    AddResetBitmapSet(ctx);

    JIT_WHILE_BEGIN(cursor_loop)
    Instruction* res = AddIsScanEnd(ctx, JIT_INDEX_SCAN_FORWARD, &cursor, JIT_RANGE_SCAN_MAIN);
    JIT_WHILE_EVAL_NOT(res)
    Instruction* row = buildGetRowFromIterator(
        ctx, JIT_WHILE_POST_BLOCK(), MOT::AccessType::WR, JIT_INDEX_SCAN_FORWARD, &cursor, JIT_RANGE_SCAN_MAIN);

    // check for additional filters
    if (!buildFilterRow(ctx, row, &plan->_index_scan._filters, &max_arg, JIT_WHILE_COND_BLOCK())) {
        MOT_LOG_TRACE("Failed to generate jitted code for range UPDATE query: unsupported filter");
        DestroyCodeGenContext(ctx);
        return nullptr;
    }

    // now begin updating columns
    IssueDebugLog("Updating row columns");
    if (!writeRowColumns(ctx, row, &plan->_update_exprs, &max_arg, true)) {
        MOT_LOG_TRACE("Failed to generate jitted code for update query: failed to process target entry");
        DestroyCodeGenContext(ctx);
        return nullptr;
    }

    // add code:
    // MOT::Rc rc = writeRow(row, bmp);
    // if (rc != MOT::RC_OK)
    //   return rc;
    IssueDebugLog("Writing row");
    buildWriteRow(ctx, row, false, &cursor);

    // the next call will be executed only if the previous call to writeRow succeeded
    buildIncrementRowsProcessed(ctx);

    // reset bitmap for next loop
    AddResetBitmapSet(ctx);
    JIT_WHILE_END()

    // cleanup
    IssueDebugLog("Reached end of range update loop");
    AddDestroyCursor(ctx, &cursor);

    // execute *tp_processed = rows_processed
    AddSetTpProcessed(ctx);

    // signal to envelope executor scan ended
    AddSetScanEnded(ctx, 1);

    // return success from calling function
    builder.CreateRet(builder.CreateConst((uint64_t)MOT::RC_OK));

    // wrap up
    JitContext* jit_context = FinalizeCodegen(ctx, max_arg, JIT_COMMAND_RANGE_UPDATE);

    // cleanup
    DestroyCodeGenContext(ctx);

    return jit_context;
}

static JitContext* JitInsertCodegen(const Query* query, const char* query_string, JitInsertPlan* plan)
{
    MOT_LOG_DEBUG("Generating code for MOT insert at thread %p", (void*)pthread_self());

    Builder builder;

    JitTvmCodeGenContext cg_ctx = {0};
    MOT::Table* table = plan->_table;
    if (!InitCodeGenContext(&cg_ctx, &builder, table, table->GetPrimaryIndex())) {
        return nullptr;
    }
    JitTvmCodeGenContext* ctx = &cg_ctx;

    // prepare the jitted function (declare, get arguments into context and define locals)
    CreateJittedFunction(ctx, "MotJittedInsert", query_string);
    IssueDebugLog("Starting execution of jitted INSERT");

    // insert is not allowed if we reached soft memory limit
    buildIsSoftMemoryLimitReached(ctx);

    // create new row and bitmap set
    Instruction* row = buildCreateNewRow(ctx);

    // set row null bits
    IssueDebugLog("Setting row null bits before insert");
    AddSetRowNullBits(ctx, row);

    // initialize rows_processed local variable
    buildResetRowsProcessed(ctx);

    IssueDebugLog("Setting row columns");
    int max_arg = 0;
    if (!writeRowColumns(ctx, row, &plan->_insert_exprs, &max_arg, false)) {
        MOT_LOG_TRACE("Failed to generate jitted code for insert query: failed to process target entry");
        DestroyCodeGenContext(ctx);
        return nullptr;
    }

    IssueDebugLog("Inserting row");
    buildInsertRow(ctx, row);

    // the next call will be executed only if the previous call to writeRow succeeded
    buildIncrementRowsProcessed(ctx);

    // execute *tp_processed = rows_processed
    AddSetTpProcessed(ctx);

    // signal to envelope executor scan ended
    AddSetScanEnded(ctx, 1);

    // return success from calling function
    builder.CreateRet(builder.CreateConst((uint64_t)MOT::RC_OK));

    // wrap up
    JitContext* jit_context = FinalizeCodegen(ctx, max_arg, JIT_COMMAND_INSERT);

    // cleanup
    DestroyCodeGenContext(ctx);

    return jit_context;
}

static JitContext* JitDeleteCodegen(const Query* query, const char* query_string, JitDeletePlan* plan)
{
    MOT_LOG_DEBUG("Generating code for MOT delete at thread %p", (void*)pthread_self());

    Builder builder;

    JitTvmCodeGenContext cg_ctx = {0};
    MOT::Table* table = plan->_query._table;
    if (!InitCodeGenContext(&cg_ctx, &builder, table, table->GetPrimaryIndex())) {
        return nullptr;
    }
    JitTvmCodeGenContext* ctx = &cg_ctx;

    // prepare the jitted function (declare, get arguments into context and define locals)
    CreateJittedFunction(ctx, "MotJittedDelete", query_string);
    IssueDebugLog("Starting execution of jitted DELETE");

    // initialize rows_processed local variable
    buildResetRowsProcessed(ctx);

    // begin the WHERE clause
    int max_arg = 0;
    if (!buildPointScan(ctx, &plan->_query._search_exprs, &max_arg, JIT_RANGE_SCAN_MAIN, nullptr)) {
        MOT_LOG_TRACE("Failed to generate jitted code for DELETE query: unsupported WHERE clause type");
        DestroyCodeGenContext(ctx);
        return nullptr;
    }

    // fetch row for delete
    Instruction* row = buildSearchRow(ctx, MOT::AccessType::DEL, JIT_RANGE_SCAN_MAIN);

    // check for additional filters
    if (!buildFilterRow(ctx, row, &plan->_query._filters, &max_arg, nullptr)) {
        MOT_LOG_TRACE("Failed to generate jitted code for DELETE query: unsupported filter");
        DestroyCodeGenContext(ctx);
        return nullptr;
    }

    // add code:
    // MOT::RC rc = deleteRow();
    // if (rc != MOT::RC_OK)
    //   return rc;
    IssueDebugLog("Deleting row");
    buildDeleteRow(
        ctx);  // row is already cached in concurrency control module, so we do not need to provide an argument

    // the next call will be executed only if the previous call to deleteRow succeeded
    buildIncrementRowsProcessed(ctx);

    // execute *tp_processed = rows_processed
    AddSetTpProcessed(ctx);

    // signal to envelope executor scan ended
    AddSetScanEnded(ctx, 1);

    // return success from calling function
    builder.CreateRet(builder.CreateConst((uint64_t)MOT::RC_OK));

    // wrap up
    JitContext* jit_context = FinalizeCodegen(ctx, max_arg, JIT_COMMAND_DELETE);

    // cleanup
    DestroyCodeGenContext(ctx);

    return jit_context;
}

static JitContext* JitSelectCodegen(const Query* query, const char* query_string, JitSelectPlan* plan)
{
    MOT_LOG_DEBUG("Generating code for MOT select at thread %p", (void*)pthread_self());

    Builder builder;

    JitTvmCodeGenContext cg_ctx = {0};
    MOT::Table* table = plan->_query._table;
    if (!InitCodeGenContext(&cg_ctx, &builder, table, table->GetPrimaryIndex())) {
        return nullptr;
    }
    JitTvmCodeGenContext* ctx = &cg_ctx;

    // prepare the jitted function (declare, get arguments into context and define locals)
    CreateJittedFunction(ctx, "MotJittedSelect", query_string);
    IssueDebugLog("Starting execution of jitted SELECT");

    // initialize rows_processed local variable
    buildResetRowsProcessed(ctx);

    // begin the WHERE clause
    int max_arg = 0;
    if (!buildPointScan(ctx, &plan->_query._search_exprs, &max_arg, JIT_RANGE_SCAN_MAIN, nullptr)) {
        MOT_LOG_TRACE("Failed to generate jitted code for SELECT query: unsupported WHERE clause type");
        DestroyCodeGenContext(ctx);
        return nullptr;
    }

    // clear tuple even if row is not found later
    AddExecClearTuple(ctx);

    // fetch row for read
    MOT::AccessType access_mode = query->hasForUpdate ? MOT::AccessType::RD_FOR_UPDATE : MOT::AccessType::RD;
    Instruction* row = buildSearchRow(ctx, access_mode, JIT_RANGE_SCAN_MAIN);

    // check for additional filters
    if (!buildFilterRow(ctx, row, &plan->_query._filters, &max_arg, nullptr)) {
        MOT_LOG_TRACE("Failed to generate jitted code for SELECT query: unsupported filter");
        DestroyCodeGenContext(ctx);
        return nullptr;
    }

    // now begin selecting columns into result
    IssueDebugLog("Selecting columns into result");
    if (!selectRowColumns(ctx, row, &plan->_select_exprs, &max_arg, JIT_RANGE_SCAN_MAIN)) {
        MOT_LOG_TRACE("Failed to generate jitted code for insert query: failed to process target entry");
        DestroyCodeGenContext(ctx);
        return nullptr;
    }

    AddExecStoreVirtualTuple(ctx);

    // update number of rows processed
    buildIncrementRowsProcessed(ctx);

    // execute *tp_processed = rows_processed
    AddSetTpProcessed(ctx);

    // signal to envelope executor scan ended (this is a point query)
    AddSetScanEnded(ctx, 1);

    // return success from calling function
    builder.CreateRet(builder.CreateConst((uint64_t)MOT::RC_OK));

    // wrap up
    JitContext* jit_context = FinalizeCodegen(ctx, max_arg, JIT_COMMAND_SELECT);

    // cleanup
    DestroyCodeGenContext(ctx);

    return jit_context;
}

/** @brief Generates code for range SELECT query with a possible LIMIT clause. */
static JitContext* JitRangeSelectCodegen(const Query* query, const char* query_string, JitRangeSelectPlan* plan)
{
    MOT_LOG_DEBUG("Generating code for MOT select at thread %p", (void*)pthread_self());

    Builder builder;

    JitTvmCodeGenContext cg_ctx = {0};
    MOT::Table* table = plan->_index_scan._table;
    int index_id = plan->_index_scan._index_id;
    if (!InitCodeGenContext(&cg_ctx, &builder, table, table->GetIndex(index_id))) {
        return nullptr;
    }
    JitTvmCodeGenContext* ctx = &cg_ctx;

    // prepare the jitted function (declare, get arguments into context and define locals)
    CreateJittedFunction(ctx, "MotJittedRangeSelect", query_string);
    IssueDebugLog("Starting execution of jitted range SELECT");

    // initialize rows_processed local variable
    buildResetRowsProcessed(ctx);

    // clear tuple even if row is not found later
    AddExecClearTuple(ctx);

    // prepare stateful scan if not done so already, if no row exists then emit code to return from function
    int max_arg = 0;
    MOT::AccessType access_mode = query->hasForUpdate ? MOT::AccessType::RD_FOR_UPDATE : MOT::AccessType::RD;
    Instruction* row = buildPrepareStateScanRow(
        ctx, &plan->_index_scan, JIT_RANGE_SCAN_MAIN, access_mode, &max_arg, nullptr, nullptr, nullptr);
    if (row == nullptr) {
        MOT_LOG_TRACE("Failed to generate jitted code for range select query: unsupported WHERE clause type");
        DestroyCodeGenContext(ctx);
        return nullptr;
    }

    // select inner and outer row expressions into result tuple (no aggregate because aggregate is not stateful)
    IssueDebugLog("Retrieved row from state iterator, beginning to select columns into result tuple");
    if (!selectRowColumns(ctx, row, &plan->_select_exprs, &max_arg, JIT_RANGE_SCAN_MAIN)) {
        MOT_LOG_TRACE("Failed to generate jitted code for range SELECT query: failed to select row expressions");
        DestroyCodeGenContext(ctx);
        return nullptr;
    }
    AddExecStoreVirtualTuple(ctx);
    buildIncrementRowsProcessed(ctx);

    // make sure that next iteration find an empty state row, so scan makes a progress
    AddResetStateRow(ctx, JIT_RANGE_SCAN_MAIN);

    // if a limit clause exists, then increment limit counter and check if reached limit
    buildCheckLimit(ctx, plan->_limit_count);

    // execute *tp_processed = rows_processed
    AddSetTpProcessed(ctx);

    // return success from calling function
    builder.CreateRet(builder.CreateConst((uint64_t)MOT::RC_OK));

    // wrap up
    JitContext* jit_context = FinalizeCodegen(ctx, max_arg, JIT_COMMAND_RANGE_SELECT);

    // cleanup
    DestroyCodeGenContext(ctx);

    return jit_context;
}

/** @brief Generates code for range SELECT query with aggregator. */
static JitContext* JitAggregateRangeSelectCodegen(
    const Query* query, const char* query_string, JitRangeSelectPlan* plan)
{
    MOT_LOG_DEBUG("Generating code for MOT aggregate range select at thread %p", (void*)pthread_self());

    Builder builder;
    MOT::Table* table = plan->_index_scan._table;
    int index_id = plan->_index_scan._index_id;
    JitTvmCodeGenContext cg_ctx = {0};
    if (!InitCodeGenContext(&cg_ctx, &builder, table, table->GetIndex(index_id))) {
        return nullptr;
    }
    JitTvmCodeGenContext* ctx = &cg_ctx;

    // prepare the jitted function (declare, get arguments into context and define locals)
    CreateJittedFunction(ctx, "MotJittedAggregateRangeSelect", query_string);
    IssueDebugLog("Starting execution of jitted aggregate range SELECT");

    // initialize rows_processed local variable
    buildResetRowsProcessed(ctx);

    // clear tuple (we use tuple's resno column as aggregated sum instead of defining local variable)
    AddExecClearTuple(ctx);

    // prepare for aggregation
    prepareAggregate(ctx, &plan->_aggregate);
    AddResetStateLimitCounter(ctx);

    // pay attention: aggregated range scan is not stateful, since we scan all tuples in one call
    MOT::AccessType access_mode = query->hasForUpdate ? MOT::AccessType::RD_FOR_UPDATE : MOT::AccessType::RD;

    // begin the WHERE clause
    int max_arg = 0;
    JitIndexScanDirection index_scan_direction = JIT_INDEX_SCAN_FORWARD;

    // build range iterators
    MOT_LOG_DEBUG("Generating range cursor for range SELECT query");
    JitTvmRuntimeCursor cursor =
        buildRangeCursor(ctx, &plan->_index_scan, &max_arg, JIT_RANGE_SCAN_MAIN, index_scan_direction, nullptr);
    if (cursor.begin_itr == nullptr) {
        MOT_LOG_TRACE("Failed to generate jitted code for aggregate range SELECT query: unsupported WHERE clause type");
        DestroyCodeGenContext(ctx);
        return nullptr;
    }

    JIT_WHILE_BEGIN(cursor_aggregate_loop)
    Instruction* res = AddIsScanEnd(ctx, index_scan_direction, &cursor, JIT_RANGE_SCAN_MAIN);
    JIT_WHILE_EVAL_NOT(res)
    Instruction* row = buildGetRowFromIterator(
        ctx, JIT_WHILE_POST_BLOCK(), access_mode, JIT_INDEX_SCAN_FORWARD, &cursor, JIT_RANGE_SCAN_MAIN);

    // check for additional filters, if not try to fetch next row
    if (!buildFilterRow(ctx, row, &plan->_index_scan._filters, &max_arg, JIT_WHILE_COND_BLOCK())) {
        MOT_LOG_TRACE("Failed to generate jitted code for aggregate range SELECT query: unsupported filter");
        DestroyCodeGenContext(ctx);
        return nullptr;
    }

    // aggregate into tuple (we use tuple's resno column as aggregated sum instead of defining local variable)
    // if row disqualified due to DISTINCT operator then go back to loop test block
    buildAggregateRow(ctx, &plan->_aggregate, row, JIT_WHILE_COND_BLOCK());

    // if a limit clause exists, then increment limit counter and check if reached limit
    if (plan->_limit_count > 0) {
        AddIncrementStateLimitCounter(ctx);
        JIT_IF_BEGIN(limit_count_reached)
        Instruction* current_limit_count = AddGetStateLimitCounter(ctx);
        JIT_IF_EVAL_CMP(current_limit_count, JIT_CONST(plan->_limit_count), JIT_ICMP_EQ);
        IssueDebugLog("Reached limit specified in limit clause, raising internal state scan end flag");
        JIT_WHILE_BREAK()  // break from loop
        JIT_IF_END()
    }
    JIT_WHILE_END()

    // cleanup
    IssueDebugLog("Reached end of aggregate range select loop");
    AddDestroyCursor(ctx, &cursor);

    // wrap up aggregation and write to result tuple
    buildAggregateResult(ctx, &plan->_aggregate);

    // store the result tuple
    AddExecStoreVirtualTuple(ctx);

    // execute *tp_processed = rows_processed
    AddSetTpProcessed(ctx);

    // signal to envelope executor scan ended (this is an aggregate loop)
    AddSetScanEnded(ctx, 1);

    // return success from calling function
    builder.CreateRet(builder.CreateConst((uint64_t)MOT::RC_OK));

    // wrap up
    JitContext* jit_context = FinalizeCodegen(ctx, max_arg, JIT_COMMAND_AGGREGATE_RANGE_SELECT);

    // cleanup
    DestroyCodeGenContext(ctx);

    return jit_context;
}

static JitContext* JitPointJoinCodegen(const Query* query, const char* query_string, JitJoinPlan* plan)
{
    MOT_LOG_DEBUG("Generating code for MOT Point JOIN query at thread %p", (void*)pthread_self());

    Builder builder;

    JitTvmCodeGenContext cg_ctx = {0};
    MOT::Table* outer_table = plan->_outer_scan._table;
    MOT::Index* outer_index = outer_table->GetIndex(plan->_outer_scan._index_id);
    MOT::Table* inner_table = plan->_inner_scan._table;
    MOT::Index* inner_index = inner_table->GetIndex(plan->_inner_scan._index_id);
    if (!InitCodeGenContext(&cg_ctx, &builder, outer_table, outer_index, inner_table, inner_index)) {
        return nullptr;
    }
    JitTvmCodeGenContext* ctx = &cg_ctx;

    // prepare the jitted function (declare, get arguments into context and define locals)
    CreateJittedFunction(ctx, "MotJittedPointJoin", query_string);
    IssueDebugLog("Starting execution of jitted Point JOIN");

    // initialize rows_processed local variable
    buildResetRowsProcessed(ctx);

    // search the outer row
    int max_arg = 0;
    if (!buildPointScan(ctx, &plan->_outer_scan._search_exprs, &max_arg, JIT_RANGE_SCAN_MAIN, nullptr)) {
        MOT_LOG_TRACE("Failed to generate jitted code for Point JOIN query: unsupported outer WHERE clause type");
        DestroyCodeGenContext(ctx);
        return nullptr;
    }

    // clear tuple even if row is not found later
    AddExecClearTuple(ctx);

    // fetch row for read
    MOT::AccessType access_mode = query->hasForUpdate ? MOT::AccessType::RD_FOR_UPDATE : MOT::AccessType::RD;
    Instruction* outer_row = buildSearchRow(ctx, access_mode, JIT_RANGE_SCAN_MAIN);

    // check for additional filters
    if (!buildFilterRow(ctx, outer_row, &plan->_outer_scan._filters, &max_arg, nullptr)) {
        MOT_LOG_TRACE("Failed to generate jitted code for Point JOIN query: unsupported outer scan filter");
        DestroyCodeGenContext(ctx);
        return nullptr;
    }

    // before we move on to inner point scan, we save the outer row in a safe copy (but for that purpose we need to save
    // row in outer scan state)
    AddSetStateRow(ctx, outer_row, JIT_RANGE_SCAN_MAIN);
    AddCopyOuterStateRow(ctx);

    // now search the inner row
    if (!buildPointScan(ctx, &plan->_inner_scan._search_exprs, &max_arg, JIT_RANGE_SCAN_INNER, outer_row)) {
        MOT_LOG_TRACE("Failed to generate jitted code for Point JOIN query: unsupported inner WHERE clause type");
        DestroyCodeGenContext(ctx);
        return nullptr;
    }
    Instruction* inner_row = buildSearchRow(ctx, access_mode, JIT_RANGE_SCAN_INNER);

    // check for additional filters
    if (!buildFilterRow(ctx, inner_row, &plan->_inner_scan._filters, &max_arg, nullptr)) {
        MOT_LOG_TRACE("Failed to generate jitted code for Point JOIN query: unsupported inner scan filter");
        DestroyCodeGenContext(ctx);
        return nullptr;
    }

    // retrieve the safe copy of the outer row
    Instruction* outer_row_copy = AddGetOuterStateRowCopy(ctx);

    // now begin selecting columns into result
    if (!selectJoinRows(ctx, outer_row_copy, inner_row, plan, &max_arg)) {
        MOT_LOG_TRACE(
            "Failed to generate jitted code for Point JOIN query: failed to select row columns into result tuple");
        DestroyCodeGenContext(ctx);
        return nullptr;
    }

    AddExecStoreVirtualTuple(ctx);

    // update number of rows processed
    buildIncrementRowsProcessed(ctx);

    // execute *tp_processed = rows_processed
    AddSetTpProcessed(ctx);

    // signal to envelope executor scan ended (this is a point query)
    AddSetScanEnded(ctx, 1);

    // return success from calling function
    builder.CreateRet(builder.CreateConst((uint64_t)MOT::RC_OK));

    // wrap up
    JitContext* jit_context = FinalizeCodegen(ctx, max_arg, JIT_COMMAND_POINT_JOIN);

    // cleanup
    DestroyCodeGenContext(ctx);

    return jit_context;
}

/** @brief Generates code for range JOIN query with outer point query and a possible LIMIT clause but without
 * aggregation. */
static JitContext* JitPointOuterJoinCodegen(const Query* query, const char* query_string, JitJoinPlan* plan)
{
    MOT_LOG_DEBUG("Generating code for MOT Outer Point JOIN query at thread %p", (void*)pthread_self());

    int max_arg = 0;
    Builder builder;

    JitTvmCodeGenContext cg_ctx = {0};
    MOT::Table* outer_table = plan->_outer_scan._table;
    MOT::Index* outer_index = outer_table->GetIndex(plan->_outer_scan._index_id);
    MOT::Table* inner_table = plan->_inner_scan._table;
    MOT::Index* inner_index = inner_table->GetIndex(plan->_inner_scan._index_id);
    if (!InitCodeGenContext(&cg_ctx, &builder, outer_table, outer_index, inner_table, inner_index)) {
        return nullptr;
    }
    JitTvmCodeGenContext* ctx = &cg_ctx;

    // prepare the jitted function (declare, get arguments into context and define locals)
    CreateJittedFunction(ctx, "MotJittedOuterPointJoin", query_string);
    IssueDebugLog("Starting execution of jitted Outer Point JOIN");

    // initialize rows_processed local variable
    buildResetRowsProcessed(ctx);

    // clear tuple even if row is not found later
    AddExecClearTuple(ctx);

    // we first check if outer state row was already searched
    Instruction* outer_row_copy = AddGetOuterStateRowCopy(ctx);
    JIT_IF_BEGIN(check_outer_row_ready)
    JIT_IF_EVAL_NOT(outer_row_copy)
    // search the outer row
    if (!buildPointScan(ctx, &plan->_outer_scan._search_exprs, &max_arg, JIT_RANGE_SCAN_MAIN, nullptr)) {
        MOT_LOG_TRACE("Failed to generate jitted code for Outer Point JOIN query: unsupported outer WHERE clause type");
        DestroyCodeGenContext(ctx);
        return nullptr;
    }

    // fetch row for read
    MOT::AccessType access_mode = query->hasForUpdate ? MOT::AccessType::RD_FOR_UPDATE : MOT::AccessType::RD;
    Instruction* outer_row = buildSearchRow(ctx, access_mode, JIT_RANGE_SCAN_MAIN);

    // check for additional filters
    if (!buildFilterRow(ctx, outer_row, &plan->_outer_scan._filters, &max_arg, nullptr)) {
        MOT_LOG_TRACE("Failed to generate jitted code for Outer Point JOIN query: unsupported filter");
        DestroyCodeGenContext(ctx);
        return nullptr;
    }

    // before we move on to inner range scan, we save the outer row in a safe copy (but for that purpose we need to save
    // row in outer scan state)
    AddSetStateRow(ctx, outer_row, JIT_RANGE_SCAN_MAIN);
    AddCopyOuterStateRow(ctx);
    outer_row_copy = AddGetOuterStateRowCopy(ctx);  // must get copy again, otherwise it is null
    JIT_IF_END()

    // now prepare inner scan if needed, if no row was found then emit code to return from function (since outer scan is
    // a point query)
    MOT::AccessType access_mode = query->hasForUpdate ? MOT::AccessType::RD_FOR_UPDATE : MOT::AccessType::RD;
    Instruction* inner_row = buildPrepareStateScanRow(
        ctx, &plan->_inner_scan, JIT_RANGE_SCAN_INNER, access_mode, &max_arg, outer_row_copy, nullptr, nullptr);
    if (inner_row == nullptr) {
        MOT_LOG_TRACE("Failed to generate jitted code for Outer Point JOIN query: unsupported WHERE clause type");
        DestroyCodeGenContext(ctx);
        return nullptr;
    }

    // retrieve the safe copy of the outer row
    outer_row_copy = AddGetOuterStateRowCopy(ctx);

    // now begin selecting columns into result
    if (!selectJoinRows(ctx, outer_row_copy, inner_row, plan, &max_arg)) {
        MOT_LOG_TRACE("Failed to generate jitted code for Outer Point JOIN query: failed to select row columns into "
                      "result tuple");
        DestroyCodeGenContext(ctx);
        return nullptr;
    }

    AddExecStoreVirtualTuple(ctx);

    // update number of rows processed
    buildIncrementRowsProcessed(ctx);

    // clear inner row for next iteration
    AddResetStateRow(ctx, JIT_RANGE_SCAN_INNER);

    // if a limit clause exists, then increment limit counter and check if reached limit
    buildCheckLimit(ctx, plan->_limit_count);

    // execute *tp_processed = rows_processed
    AddSetTpProcessed(ctx);

    // return success from calling function
    builder.CreateRet(builder.CreateConst((uint64_t)MOT::RC_OK));

    // wrap up
    JitContext* jit_context = FinalizeCodegen(ctx, max_arg, JIT_COMMAND_RANGE_JOIN);

    // cleanup
    DestroyCodeGenContext(ctx);

    return jit_context;
}

/** @brief Generates code for range JOIN query with inner point query and a possible LIMIT clause but without
 * aggregation. */
static JitContext* JitPointInnerJoinCodegen(const Query* query, const char* query_string, JitJoinPlan* plan)
{
    MOT_LOG_DEBUG("Generating code for MOT Inner Point JOIN at thread %p", (void*)pthread_self());

    Builder builder;

    JitTvmCodeGenContext cg_ctx = {0};
    MOT::Table* outer_table = plan->_outer_scan._table;
    MOT::Index* outer_index = outer_table->GetIndex(plan->_outer_scan._index_id);
    MOT::Table* inner_table = plan->_inner_scan._table;
    MOT::Index* inner_index = inner_table->GetIndex(plan->_inner_scan._index_id);
    if (!InitCodeGenContext(&cg_ctx, &builder, outer_table, outer_index, inner_table, inner_index)) {
        return nullptr;
    }
    JitTvmCodeGenContext* ctx = &cg_ctx;

    // prepare the jitted function (declare, get arguments into context and define locals)
    CreateJittedFunction(ctx, "MotJittedInnerPointJoin", query_string);
    IssueDebugLog("Starting execution of jitted inner point JOIN");

    // initialize rows_processed local variable
    buildResetRowsProcessed(ctx);

    // clear tuple even if row is not found later
    AddExecClearTuple(ctx);

    // prepare stateful scan if not done so already, if row not found then emit code to return from function (since this
    // is an outer scan)
    int max_arg = 0;
    BasicBlock* fetch_outer_row_bb = nullptr;
    MOT::AccessType access_mode = query->hasForUpdate ? MOT::AccessType::RD_FOR_UPDATE : MOT::AccessType::RD;
    Instruction* outer_row = buildPrepareStateScanRow(
        ctx, &plan->_outer_scan, JIT_RANGE_SCAN_MAIN, access_mode, &max_arg, nullptr, nullptr, &fetch_outer_row_bb);
    if (outer_row == nullptr) {
        MOT_LOG_TRACE("Failed to generate jitted code for Inner Point JOIN query: unsupported WHERE clause type");
        DestroyCodeGenContext(ctx);
        return nullptr;
    }

    // before we move on to inner scan, we save the outer row in a safe copy
    AddCopyOuterStateRow(ctx);

    // now search the inner row
    if (!buildPointScan(ctx, &plan->_inner_scan._search_exprs, &max_arg, JIT_RANGE_SCAN_INNER, outer_row)) {
        MOT_LOG_TRACE("Failed to generate jitted code for Inner Point JOIN query: unsupported inner WHERE clause type");
        DestroyCodeGenContext(ctx);
        return nullptr;
    }
    Instruction* inner_row = buildSearchRow(ctx, access_mode, JIT_RANGE_SCAN_INNER);

    // check for additional filters
    if (!buildFilterRow(ctx, inner_row, &plan->_inner_scan._filters, &max_arg, fetch_outer_row_bb)) {
        MOT_LOG_TRACE("Failed to generate jitted code for Inner Point JOIN query: unsupported inner scan filter");
        DestroyCodeGenContext(ctx);
        return nullptr;
    }

    // retrieve the safe copy of the outer row
    Instruction* outer_row_copy = AddGetOuterStateRowCopy(ctx);

    // now begin selecting columns into result
    if (!selectJoinRows(ctx, outer_row_copy, inner_row, plan, &max_arg)) {
        MOT_LOG_TRACE("Failed to generate jitted code for Inner Point JOIN query: failed to select row columns into "
                      "result tuple");
        DestroyCodeGenContext(ctx);
        return nullptr;
    }

    AddExecStoreVirtualTuple(ctx);
    buildIncrementRowsProcessed(ctx);

    // if a limit clause exists, then increment limit counter and check if reached limit
    buildCheckLimit(ctx, plan->_limit_count);

    // execute *tp_processed = rows_processed
    AddSetTpProcessed(ctx);

    // return success from calling function
    builder.CreateRet(builder.CreateConst((uint64_t)MOT::RC_OK));

    // wrap up
    JitContext* jit_context = FinalizeCodegen(ctx, max_arg, JIT_COMMAND_RANGE_JOIN);

    // cleanup
    DestroyCodeGenContext(ctx);

    return jit_context;
}

/** @brief Generates code for range JOIN query with a possible LIMIT clause but without aggregation. */
static JitContext* JitRangeJoinCodegen(const Query* query, const char* query_string, JitJoinPlan* plan)
{
    MOT_LOG_DEBUG("Generating code for MOT range JOIN at thread %p", (void*)pthread_self());

    Builder builder;

    JitTvmCodeGenContext cg_ctx = {0};
    MOT::Table* outer_table = plan->_outer_scan._table;
    MOT::Index* outer_index = outer_table->GetIndex(plan->_outer_scan._index_id);
    MOT::Table* inner_table = plan->_inner_scan._table;
    MOT::Index* inner_index = inner_table->GetIndex(plan->_inner_scan._index_id);
    if (!InitCodeGenContext(&cg_ctx, &builder, outer_table, outer_index, inner_table, inner_index)) {
        return nullptr;
    }
    JitTvmCodeGenContext* ctx = &cg_ctx;

    // prepare the jitted function (declare, get arguments into context and define locals)
    CreateJittedFunction(ctx, "MotJittedRangeJoin", query_string);
    IssueDebugLog("Starting execution of jitted range JOIN");

    // initialize rows_processed local variable
    buildResetRowsProcessed(ctx);

    // clear tuple even if row is not found later
    AddExecClearTuple(ctx);

    // prepare stateful scan if not done so already
    int max_arg = 0;
    BasicBlock* fetch_outer_row_bb = nullptr;
    MOT::AccessType access_mode = query->hasForUpdate ? MOT::AccessType::RD_FOR_UPDATE : MOT::AccessType::RD;
    Instruction* outer_row = buildPrepareStateScanRow(
        ctx, &plan->_outer_scan, JIT_RANGE_SCAN_MAIN, access_mode, &max_arg, nullptr, nullptr, &fetch_outer_row_bb);
    if (outer_row == nullptr) {
        MOT_LOG_TRACE("Failed to generate jitted code for Range JOIN query: unsupported outer WHERE clause type");
        DestroyCodeGenContext(ctx);
        return nullptr;
    }

    // before we move on to inner scan, we save the outer row in a safe copy
    AddCopyOuterStateRow(ctx);

    // now prepare inner scan if needed
    Instruction* inner_row = buildPrepareStateScanRow(
        ctx, &plan->_inner_scan, JIT_RANGE_SCAN_INNER, access_mode, &max_arg, outer_row, fetch_outer_row_bb, nullptr);
    if (inner_row == nullptr) {
        MOT_LOG_TRACE("Failed to generate jitted code for Range JOIN query: unsupported inner WHERE clause type");
        DestroyCodeGenContext(ctx);
        return nullptr;
    }

    // retrieve the safe copy of the outer row
    Instruction* outer_row_copy = AddGetOuterStateRowCopy(ctx);

    // now begin selecting columns into result
    if (!selectJoinRows(ctx, outer_row_copy, inner_row, plan, &max_arg)) {
        MOT_LOG_TRACE(
            "Failed to generate jitted code for Range JOIN query: failed to select row columns into result tuple");
        DestroyCodeGenContext(ctx);
        return nullptr;
    }

    AddExecStoreVirtualTuple(ctx);
    buildIncrementRowsProcessed(ctx);

    // clear inner row for next iteration
    AddResetStateRow(ctx, JIT_RANGE_SCAN_INNER);

    // if a limit clause exists, then increment limit counter and check if reached limit
    buildCheckLimit(ctx, plan->_limit_count);

    // execute *tp_processed = rows_processed
    AddSetTpProcessed(ctx);

    // return success from calling function
    builder.CreateRet(builder.CreateConst((uint64_t)MOT::RC_OK));

    // wrap up
    JitContext* jit_context = FinalizeCodegen(ctx, max_arg, JIT_COMMAND_RANGE_JOIN);

    // cleanup
    DestroyCodeGenContext(ctx);

    return jit_context;
}

/** @brief Generates code for range JOIN query with an aggregator. */
static JitContext* JitAggregateRangeJoinCodegen(const Query* query, const char* query_string, JitJoinPlan* plan)
{
    MOT_LOG_DEBUG("Generating code for MOT aggregate range JOIN at thread %p", (void*)pthread_self());

    Builder builder;

    JitTvmCodeGenContext cg_ctx = {0};
    MOT::Table* outer_table = plan->_outer_scan._table;
    MOT::Index* outer_index = outer_table->GetIndex(plan->_outer_scan._index_id);
    MOT::Table* inner_table = plan->_inner_scan._table;
    MOT::Index* inner_index = inner_table->GetIndex(plan->_inner_scan._index_id);
    if (!InitCodeGenContext(&cg_ctx, &builder, outer_table, outer_index, inner_table, inner_index)) {
        return nullptr;
    }
    JitTvmCodeGenContext* ctx = &cg_ctx;

    // prepare the jitted function (declare, get arguments into context and define locals)
    CreateJittedFunction(ctx, "MotJittedAggregateRangeJoin", query_string);
    IssueDebugLog("Starting execution of jitted aggregate range JOIN");

    // initialize rows_processed local variable
    buildResetRowsProcessed(ctx);

    // clear tuple (we use tuple's resno column as aggregated sum instead of defining local variable)
    AddExecClearTuple(ctx);

    // prepare for aggregation
    prepareAggregate(ctx, &plan->_aggregate);
    AddResetStateLimitCounter(ctx);

    // pay attention: aggregated range scan is not stateful, since we scan all tuples in one call
    MOT::AccessType access_mode = query->hasForUpdate ? MOT::AccessType::RD_FOR_UPDATE : MOT::AccessType::RD;

    // begin the WHERE clause
    int max_arg = 0;

    // build range iterators
    MOT_LOG_DEBUG("Generating outer loop cursor for range JOIN query");
    JitTvmRuntimeCursor outer_cursor =
        buildRangeCursor(ctx, &plan->_outer_scan, &max_arg, JIT_RANGE_SCAN_MAIN, JIT_INDEX_SCAN_FORWARD, nullptr);
    if (outer_cursor.begin_itr == nullptr) {
        MOT_LOG_TRACE(
            "Failed to generate jitted code for aggregate range JOIN query: unsupported outer-loop WHERE clause type");
        DestroyCodeGenContext(ctx);
        return nullptr;
    }

    JIT_WHILE_BEGIN(cursor_aggregate_outer_loop)
    BasicBlock* endOuterLoopBlock = JIT_WHILE_POST_BLOCK();
    Instruction* res = AddIsScanEnd(ctx, JIT_INDEX_SCAN_FORWARD, &outer_cursor, JIT_RANGE_SCAN_MAIN);
    JIT_WHILE_EVAL_NOT(res)
    Instruction* outer_row = buildGetRowFromIterator(
        ctx, JIT_WHILE_POST_BLOCK(), access_mode, JIT_INDEX_SCAN_FORWARD, &outer_cursor, JIT_RANGE_SCAN_MAIN);

    // check for additional filters, if not try to fetch next row
    if (!buildFilterRow(ctx, outer_row, &plan->_outer_scan._filters, &max_arg, JIT_WHILE_COND_BLOCK())) {
        MOT_LOG_TRACE("Failed to generate jitted code for aggregate range JOIN query: unsupported outer-loop filter");
        DestroyCodeGenContext(ctx);
        return nullptr;
    }

    // before we move on to inner scan, we save the outer row in a safe copy (but for that purpose we need to save row
    // in outer scan state)
    AddSetStateRow(ctx, outer_row, JIT_RANGE_SCAN_MAIN);
    AddCopyOuterStateRow(ctx);

    // now build the inner loop
    MOT_LOG_DEBUG("Generating inner loop cursor for range JOIN query");
    JitTvmRuntimeCursor inner_cursor =
        buildRangeCursor(ctx, &plan->_inner_scan, &max_arg, JIT_RANGE_SCAN_INNER, JIT_INDEX_SCAN_FORWARD, outer_row);
    if (inner_cursor.begin_itr == nullptr) {
        MOT_LOG_TRACE(
            "Failed to generate jitted code for aggregate range JOIN query: unsupported inner-loop WHERE clause type");
        DestroyCodeGenContext(ctx);
        return nullptr;
    }

    JIT_WHILE_BEGIN(cursor_aggregate_inner_loop)
    Instruction* res_inner = AddIsScanEnd(ctx, JIT_INDEX_SCAN_FORWARD, &inner_cursor, JIT_RANGE_SCAN_INNER);
    JIT_WHILE_EVAL_NOT(res_inner)
    Instruction* inner_row = buildGetRowFromIterator(
        ctx, JIT_WHILE_POST_BLOCK(), access_mode, JIT_INDEX_SCAN_FORWARD, &inner_cursor, JIT_RANGE_SCAN_INNER);

    // check for additional filters, if not try to fetch next row
    if (!buildFilterRow(ctx, inner_row, &plan->_inner_scan._filters, &max_arg, JIT_WHILE_COND_BLOCK())) {
        MOT_LOG_TRACE("Failed to generate jitted code for aggregate range JOIN query: unsupported inner-loop filter");
        DestroyCodeGenContext(ctx);
        return nullptr;
    }

    // aggregate into tuple (we use tuple's resno column as aggregated sum instead of defining local variable)
    // find out to which table the aggreate expression refers, and aggregate it
    // if row disqualified due to DISTINCT operator then go back to inner loop test block
    if (plan->_aggregate._table == ctx->m_innerTable_info.m_table) {
        buildAggregateRow(ctx, &plan->_aggregate, inner_row, JIT_WHILE_COND_BLOCK());
    } else {
        // retrieve the safe copy of the outer row
        Instruction* outer_row_copy = AddGetOuterStateRowCopy(ctx);
        buildAggregateRow(ctx, &plan->_aggregate, outer_row_copy, JIT_WHILE_COND_BLOCK());
    }

    // if a limit clause exists, then increment limit counter and check if reached limit
    if (plan->_limit_count > 0) {
        AddIncrementStateLimitCounter(ctx);
        JIT_IF_BEGIN(limit_count_reached)
        Instruction* current_limit_count = AddGetStateLimitCounter(ctx);
        JIT_IF_EVAL_CMP(current_limit_count, JIT_CONST(plan->_limit_count), JIT_ICMP_EQ);
        IssueDebugLog("Reached limit specified in limit clause, raising internal state scan end flag");
        AddDestroyCursor(ctx, &outer_cursor);
        AddDestroyCursor(ctx, &inner_cursor);
        ctx->_builder->CreateBr(endOuterLoopBlock);  // break from inner outside of outer loop
        JIT_IF_END()
    }
    JIT_WHILE_END()

    // cleanup
    IssueDebugLog("Reached end of inner loop");
    AddDestroyCursor(ctx, &inner_cursor);

    JIT_WHILE_END()

    // cleanup
    IssueDebugLog("Reached end of outer loop");
    AddDestroyCursor(ctx, &outer_cursor);

    // wrap up aggregation and write to result tuple
    buildAggregateResult(ctx, &plan->_aggregate);

    // store the result tuple
    AddExecStoreVirtualTuple(ctx);

    // execute *tp_processed = rows_processed
    AddSetTpProcessed(ctx);

    // signal to envelope executor scan ended
    AddSetScanEnded(ctx, 1);

    // return success from calling function
    builder.CreateRet(builder.CreateConst((uint64_t)MOT::RC_OK));

    // wrap up
    JitContext* jit_context = FinalizeCodegen(ctx, max_arg, JIT_COMMAND_AGGREGATE_JOIN);

    // cleanup
    DestroyCodeGenContext(ctx);

    return jit_context;
}

static JitContext* JitJoinCodegen(Query* query, const char* query_string, JitJoinPlan* plan)
{
    JitContext* jit_context = nullptr;

    if (plan->_aggregate._aggreaget_op == JIT_AGGREGATE_NONE) {
        switch (plan->_scan_type) {
            case JIT_JOIN_SCAN_POINT:
                // special case: this is really a point query
                jit_context = JitPointJoinCodegen(query, query_string, plan);
                break;

            case JIT_JOIN_SCAN_OUTER_POINT:
                // special case: outer scan is really a point query
                jit_context = JitPointOuterJoinCodegen(query, query_string, plan);
                break;

            case JIT_JOIN_SCAN_INNER_POINT:
                // special case: inner scan is really a point query
                jit_context = JitPointInnerJoinCodegen(query, query_string, plan);
                break;

            case JIT_JOIN_SCAN_RANGE:
                jit_context = JitRangeJoinCodegen(query, query_string, plan);
                break;

            default:
                MOT_LOG_TRACE(
                    "Cannot generate jitteed code for JOIN plan: Invalid JOIN scan type %d", (int)plan->_scan_type);
                break;
        }
    } else {
        jit_context = JitAggregateRangeJoinCodegen(query, query_string, plan);
    }

    return jit_context;
}

static JitContext* JitRangeScanCodegen(const Query* query, const char* query_string, JitRangeScanPlan* plan)
{
    JitContext* jit_context = nullptr;

    switch (plan->_command_type) {
        case JIT_COMMAND_UPDATE:
            jit_context = JitRangeUpdateCodegen(query, query_string, (JitRangeUpdatePlan*)plan);
            break;

        case JIT_COMMAND_SELECT: {
            JitRangeSelectPlan* range_select_plan = (JitRangeSelectPlan*)plan;
            if (range_select_plan->_aggregate._aggreaget_op == JIT_AGGREGATE_NONE) {
                jit_context = JitRangeSelectCodegen(query, query_string, range_select_plan);
            } else {
                jit_context = JitAggregateRangeSelectCodegen(query, query_string, range_select_plan);
            }
        } break;

        default:
            MOT_REPORT_ERROR(MOT_ERROR_INTERNAL,
                "Generate JIT Code",
                "Invalid point query JIT plan command type %d",
                (int)plan->_command_type);
            break;
    }

    return jit_context;
}

static JitContext* JitPointQueryCodegen(const Query* query, const char* query_string, JitPointQueryPlan* plan)
{
    JitContext* jit_context = nullptr;

    switch (plan->_command_type) {
        case JIT_COMMAND_UPDATE:
            jit_context = JitUpdateCodegen(query, query_string, (JitUpdatePlan*)plan);
            break;

        case JIT_COMMAND_DELETE:
            jit_context = JitDeleteCodegen(query, query_string, (JitDeletePlan*)plan);
            break;

        case JIT_COMMAND_SELECT:
            jit_context = JitSelectCodegen(query, query_string, (JitSelectPlan*)plan);
            break;

        default:
            MOT_REPORT_ERROR(MOT_ERROR_INTERNAL,
                "Generate JIT Code",
                "Invalid point query JIT plan command type %d",
                (int)plan->_command_type);
            break;
    }

    return jit_context;
}

extern JitContext* JitCodegenTvmQuery(Query* query, const char* query_string, JitPlan* plan)
{
    JitContext* jit_context = nullptr;

    MOT_LOG_DEBUG("*** Attempting to generate planned TVM-jitted code for query: %s", query_string);

    switch (plan->_plan_type) {
        case JIT_PLAN_INSERT_QUERY:
            jit_context = JitInsertCodegen(query, query_string, (JitInsertPlan*)plan);
            break;

        case JIT_PLAN_POINT_QUERY:
            jit_context = JitPointQueryCodegen(query, query_string, (JitPointQueryPlan*)plan);
            break;

        case JIT_PLAN_RANGE_SCAN:
            jit_context = JitRangeScanCodegen(query, query_string, (JitRangeScanPlan*)plan);
            break;

        case JIT_PLAN_JOIN:
            jit_context = JitJoinCodegen(query, query_string, (JitJoinPlan*)plan);
            break;

        default:
            MOT_REPORT_ERROR(
                MOT_ERROR_INTERNAL, "Generate JIT Code", "Invalid JIT plan type %d", (int)plan->_plan_type);
            break;
    }

    if (jit_context == nullptr) {
        MOT_LOG_TRACE("Failed to generate TVM-jitted code for query: %s", query_string);
    } else {
        MOT_LOG_DEBUG(
            "Got TVM-jitted function %p after compile, for query: %s", jit_context->m_tvmFunction, query_string);
    }

    return jit_context;
}

extern int JitExecTvmQuery(
    JitContext* jit_context, ParamListInfo params, TupleTableSlot* slot, uint64_t* tp_processed, int* scan_ended)
{
    int result = 0;
    ExecContext* exec_context = jit_context->m_execContext;

    // allocate execution context on-demand
    if (exec_context == nullptr) {
        exec_context = allocExecContext(jit_context->m_tvmFunction->getRegisterCount());
        if (exec_context == nullptr) {
            MOT_REPORT_ERROR(MOT_ERROR_OOM, "Execute JIT", "Failed to allocate execution context for TVM-jit function");
            result = MOT::RC_MEMORY_ALLOCATION_ERROR;
        } else {
            // save for later execution
            jit_context->m_execContext = exec_context;
        }
    }

    if (exec_context != nullptr) {
        exec_context->_jit_context = jit_context;
        exec_context->_params = params;
        exec_context->_slot = slot;
        exec_context->_tp_processed = tp_processed;
        exec_context->_scan_ended = scan_ended;

        result = (int)jit_context->m_tvmFunction->exec(exec_context);
    }

    return result;
}
}  // namespace JitExec
