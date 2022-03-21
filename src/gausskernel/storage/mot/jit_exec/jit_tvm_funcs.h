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
 * jit_tvm_funcs.h
 *    TVM-jitted query codegen helpers.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/jit_exec/jit_tvm_funcs.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef JIT_TVM_FUNCS_H
#define JIT_TVM_FUNCS_H

/*
 * ATTENTION: Be sure to include jit_tvm_query.h before anything else because of libintl.h
 * See jit_tvm_query.h for more details.
 */
#include "jit_tvm_query.h"
#include "jit_plan_expr.h"
#include "logger.h"

namespace JitExec {
/** @define The NULL datum. */
#define NULL_DATUM PointerGetDatum(nullptr)

/** @brief Gets a key from the execution context. */
inline MOT::Key* getExecContextKey(tvm::ExecContext* exec_context, JitRangeIteratorType range_itr_type,
    JitRangeScanType range_scan_type, int subQueryIndex)
{
    MOT::Key* key = nullptr;
    if (range_scan_type == JIT_RANGE_SCAN_INNER) {
        if (range_itr_type == JIT_RANGE_ITERATOR_END) {
            key = exec_context->_jit_context->m_innerEndIteratorKey;
        } else {
            key = exec_context->_jit_context->m_innerSearchKey;
        }
    } else if (range_scan_type == JIT_RANGE_SCAN_MAIN) {
        if (range_itr_type == JIT_RANGE_ITERATOR_END) {
            key = exec_context->_jit_context->m_endIteratorKey;
        } else {
            key = exec_context->_jit_context->m_searchKey;
        }
    } else if (range_scan_type == JIT_RANGE_SCAN_SUB_QUERY) {
        if (range_itr_type == JIT_RANGE_ITERATOR_END) {
            key = exec_context->_jit_context->m_subQueryData[subQueryIndex].m_endIteratorKey;
        } else {
            key = exec_context->_jit_context->m_subQueryData[subQueryIndex].m_searchKey;
        }
    }
    return key;
}

/** @class VarExpression */
class VarExpression : public tvm::Expression {
public:
    VarExpression(MOT::Table* table, tvm::Instruction* row_inst, int table_colid, int arg_pos)
        : Expression(tvm::Expression::CannotFail),
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

    Datum eval(tvm::ExecContext* exec_context) final
    {
        exec_context->_expr_rc = MOT_NO_ERROR;
        MOT::Row* row = (MOT::Row*)_row_inst->Exec(exec_context);
        return readDatumColumn(_table, row, _table_colid, _arg_pos);
    }

    void dump() final
    {
        (void)fprintf(stderr, "readDatumColumn(%%table, %%row=");
        _row_inst->Dump();
        (void)fprintf(stderr, ", table_colid=%d, arg_pos=%d)", _table_colid, _arg_pos);
    }

private:
    MOT::Table* _table;
    tvm::Instruction* _row_inst;
    int _table_colid;  // already zero-based
    int _arg_pos;
};

/** @class UnaryOperator */
class UnaryOperator : public tvm::Expression {
public:
    Datum eval(tvm::ExecContext* exec_context) final
    {
        Datum result = NULL_DATUM;
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
    UnaryOperator(const char* name, tvm::Expression* sub_expr)
        : Expression(tvm::Expression::CanFail), _name(name), _sub_expr(sub_expr)
    {}

    ~UnaryOperator() noexcept override
    {
        MOT_LOG_DEBUG("Deleting sub-expr %p in unary operator %s at %p", _sub_expr, _name.c_str(), this);
        delete _sub_expr;
    }

    virtual Datum evalOperator(tvm::ExecContext* exec_context, Datum arg) = 0;

private:
    std::string _name;
    tvm::Expression* _sub_expr;
};

/** @class UnaryCastOperator */
class UnaryCastOperator : public UnaryOperator {
protected:
    UnaryCastOperator(const char* name, tvm::Expression* sub_expr, int arg_pos)
        : UnaryOperator(name, sub_expr), _arg_pos(arg_pos)
    {}

    ~UnaryCastOperator() override
    {}

    Datum evalOperator(tvm::ExecContext* exec_context, Datum arg) final
    {
        Datum result = NULL_DATUM;
        exec_context->_expr_rc = MOT_NO_ERROR;
        int isnull = getExprArgIsNull(_arg_pos);
        if (!isnull) {
            result = evalCastOperator(exec_context, arg);
        }
        return result;
    }

    virtual Datum evalCastOperator(tvm::ExecContext* exec_context, Datum arg) = 0;

private:
    int _arg_pos;
};

/** @class BinaryOperator */
class BinaryOperator : public tvm::Expression {
public:
    Datum eval(tvm::ExecContext* exec_context) final
    {
        Datum result = NULL_DATUM;
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
    BinaryOperator(const char* name, tvm::Expression* lhs_sub_expr, Expression* rhs_sub_expr)
        : Expression(tvm::Expression::CanFail), _name(name), _lhs_sub_expr(lhs_sub_expr), _rhs_sub_expr(rhs_sub_expr)
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

    virtual Datum evalOperator(tvm::ExecContext* exec_context, Datum lhs_arg, Datum rhs_arg) = 0;

private:
    std::string _name;
    Expression* _lhs_sub_expr;
    Expression* _rhs_sub_expr;
};

/** @class BinaryCastOperator */
class BinaryCastOperator : public BinaryOperator {
protected:
    BinaryCastOperator(const char* name, tvm::Expression* lhs_sub_expr, tvm::Expression* rhs_sub_expr, int arg_pos)
        : BinaryOperator(name, lhs_sub_expr, rhs_sub_expr), _arg_pos(arg_pos)
    {}

    ~BinaryCastOperator() override
    {}

    Datum evalOperator(tvm::ExecContext* exec_context, Datum lhs_arg, Datum rhs_arg) final
    {
        Datum result = NULL_DATUM;
        exec_context->_expr_rc = MOT_NO_ERROR;
        int isnull = getExprArgIsNull(_arg_pos);
        if (!isnull) {
            result = evalCastOperator(exec_context, lhs_arg, rhs_arg);
        }
        return result;
    }

    virtual Datum evalCastOperator(tvm::ExecContext* exec_context, Datum lhs_arg, Datum rhs_arg) = 0;

private:
    int _arg_pos;
};

/** @class TernaryOperator */
class TernaryOperator : public tvm::Expression {
public:
    Datum eval(tvm::ExecContext* exec_context) final
    {
        Datum result = NULL_DATUM;
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
    TernaryOperator(
        const char* name, tvm::Expression* sub_expr1, tvm::Expression* sub_expr2, tvm::Expression* sub_expr3)
        : Expression(tvm::Expression::CanFail),
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

    virtual Datum evalOperator(tvm::ExecContext* exec_context, Datum arg1, Datum arg2, Datum arg3) = 0;

private:
    std::string _name;
    Expression* _sub_expr1;
    Expression* _sub_expr2;
    Expression* _sub_expr3;
};

/** @class TernaryCastOperator */
class TernaryCastOperator : public TernaryOperator {
protected:
    TernaryCastOperator(const char* name, tvm::Expression* sub_expr1, tvm::Expression* sub_expr2,
        tvm::Expression* sub_expr3, int arg_pos)
        : TernaryOperator(name, sub_expr1, sub_expr2, sub_expr3), _arg_pos(arg_pos)
    {}

    ~TernaryCastOperator() override
    {}

    Datum evalOperator(tvm::ExecContext* exec_context, Datum arg1, Datum arg2, Datum arg3) final
    {
        Datum result = NULL_DATUM;
        exec_context->_expr_rc = MOT_NO_ERROR;
        int isnull = getExprArgIsNull(_arg_pos);
        if (!isnull) {
            result = evalCastOperator(exec_context, arg1, arg2, arg3);
        }
        return result;
    }

    virtual Datum evalCastOperator(tvm::ExecContext* exec_context, Datum arg1, Datum arg2, Datum arg3) = 0;

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

#define APPLY_UNARY_OPERATOR(funcid, name)                                                  \
    class name##Operator : public UnaryOperator {                                           \
    public:                                                                                 \
        explicit name##Operator(tvm::Expression* sub_expr) : UnaryOperator(#name, sub_expr) \
        {}                                                                                  \
        ~name##Operator() final                                                             \
        {}                                                                                  \
                                                                                            \
    protected:                                                                              \
        Datum evalOperator(tvm::ExecContext* exec_context, Datum arg) final                 \
        {                                                                                   \
            Datum result = NULL_DATUM;                                                      \
            exec_context->_expr_rc = MOT_NO_ERROR;                                          \
            MOT_LOG_DEBUG("Invoking unary operator: " #name);                               \
            INVOKE_OPERATOR1(funcid, name, arg);                                            \
            return result;                                                                  \
        }                                                                                   \
    };

#define APPLY_UNARY_CAST_OPERATOR(funcid, name)                                                              \
    class name##Operator : public UnaryCastOperator {                                                        \
    public:                                                                                                  \
        name##Operator(tvm::Expression* sub_expr, int arg_pos) : UnaryCastOperator(#name, sub_expr, arg_pos) \
        {}                                                                                                   \
        ~name##Operator() final                                                                              \
        {}                                                                                                   \
                                                                                                             \
    protected:                                                                                               \
        Datum evalCastOperator(tvm::ExecContext* exec_context, Datum arg) final                              \
        {                                                                                                    \
            Datum result = NULL_DATUM;                                                                       \
            exec_context->_expr_rc = MOT_NO_ERROR;                                                           \
            MOT_LOG_DEBUG("Invoking unary cast operator: " #name);                                           \
            INVOKE_OPERATOR1(funcid, name, arg);                                                             \
            return result;                                                                                   \
        }                                                                                                    \
    };

#define APPLY_BINARY_OPERATOR(funcid, name)                                                    \
    class name##Operator : public BinaryOperator {                                             \
    public:                                                                                    \
        name##Operator(tvm::Expression* lhs_sub_expr, tvm::Expression* rhs_sub_expr)           \
            : BinaryOperator(#name, lhs_sub_expr, rhs_sub_expr)                                \
        {}                                                                                     \
        ~name##Operator() final                                                                \
        {}                                                                                     \
                                                                                               \
    protected:                                                                                 \
        Datum evalOperator(tvm::ExecContext* exec_context, Datum lhs_arg, Datum rhs_arg) final \
        {                                                                                      \
            Datum result = NULL_DATUM;                                                         \
            exec_context->_expr_rc = MOT_NO_ERROR;                                             \
            MOT_LOG_DEBUG("Invoking binary operator: " #name);                                 \
            INVOKE_OPERATOR2(funcid, name, lhs_arg, rhs_arg);                                  \
            return result;                                                                     \
        }                                                                                      \
    };

#define APPLY_BINARY_CAST_OPERATOR(funcid, name)                                                   \
    class name##Operator : public BinaryCastOperator {                                             \
    public:                                                                                        \
        name##Operator(tvm::Expression* lhs_sub_expr, tvm::Expression* rhs_sub_expr, int arg_pos)  \
            : BinaryCastOperator(#name, lhs_sub_expr, rhs_sub_expr, arg_pos)                       \
        {}                                                                                         \
        ~name##Operator() final                                                                    \
        {}                                                                                         \
                                                                                                   \
    protected:                                                                                     \
        Datum evalCastOperator(tvm::ExecContext* exec_context, Datum lhs_arg, Datum rhs_arg) final \
        {                                                                                          \
            Datum result = NULL_DATUM;                                                             \
            exec_context->_expr_rc = MOT_NO_ERROR;                                                 \
            MOT_LOG_DEBUG("Invoking binary cast operator: " #name);                                \
            INVOKE_OPERATOR2(funcid, name, lhs_arg, rhs_arg);                                      \
            return result;                                                                         \
        }                                                                                          \
    };

#define APPLY_TERNARY_OPERATOR(funcid, name)                                                               \
    class name##Operator : public TernaryOperator {                                                        \
    public:                                                                                                \
        name##Operator(tvm::Expression* sub_expr1, tvm::Expression* sub_expr2, tvm::Expression* sub_expr3) \
            : TernaryOperator(#name, sub_expr1, sub_expr2, sub_expr3)                                      \
        {}                                                                                                 \
        ~name##Operator() final                                                                            \
        {}                                                                                                 \
                                                                                                           \
    protected:                                                                                             \
        Datum evalOperator(tvm::ExecContext* exec_context, Datum arg1, Datum arg2, Datum arg3) final       \
        {                                                                                                  \
            Datum result = NULL_DATUM;                                                                     \
            exec_context->_expr_rc = MOT_NO_ERROR;                                                         \
            MOT_LOG_DEBUG("Invoking ternary operator: " #name);                                            \
            INVOKE_OPERATOR3(funcid, name, arg1, arg2, arg3);                                              \
            return result;                                                                                 \
        }                                                                                                  \
    };

#define APPLY_TERNARY_CAST_OPERATOR(funcid, name)                                                            \
    class name##Operator : public TernaryCastOperator {                                                      \
    public:                                                                                                  \
        name##Operator(                                                                                      \
            tvm::Expression* sub_expr1, tvm::Expression* sub_expr2, tvm::Expression* sub_expr3, int arg_pos) \
            : TernaryCastOperator(#name, sub_expr1, sub_expr2, sub_expr3, arg_pos)                           \
        {}                                                                                                   \
        ~name##Operator() final                                                                              \
        {}                                                                                                   \
                                                                                                             \
    protected:                                                                                               \
        Datum evalCastOperator(tvm::ExecContext* exec_context, Datum arg1, Datum arg2, Datum arg3) final     \
        {                                                                                                    \
            Datum result = NULL_DATUM;                                                                       \
            exec_context->_expr_rc = MOT_NO_ERROR;                                                           \
            MOT_LOG_DEBUG("Invoking ternary cast operator: " #name);                                         \
            INVOKE_OPERATOR3(funcid, name, arg1, arg2, arg3);                                                \
            return result;                                                                                   \
        }                                                                                                    \
    };

APPLY_OPERATORS()

#undef APPLY_UNARY_OPERATOR
#undef APPLY_BINARY_OPERATOR
#undef APPLY_TERNARY_OPERATOR
#undef APPLY_UNARY_CAST_OPERATOR
#undef APPLY_BINARY_CAST_OPERATOR
#undef APPLY_TERNARY_CAST_OPERATOR

/** @class IsSoftMemoryLimitReachedInstruction */
class IsSoftMemoryLimitReachedInstruction : public tvm::Instruction {
public:
    IsSoftMemoryLimitReachedInstruction()
    {}

    ~IsSoftMemoryLimitReachedInstruction() final
    {}

protected:
    uint64_t ExecImpl(tvm::ExecContext* exec_context) final
    {
        return (uint64_t)isSoftMemoryLimitReached();
    }

    void DumpImpl() final
    {
        (void)fprintf(stderr, "isSoftMemoryLimitReached()");
    }
};

/** InitSearchKeyInstruction */
class InitSearchKeyInstruction : public tvm::Instruction {
public:
    explicit InitSearchKeyInstruction(JitRangeScanType range_scan_type, int subQueryIndex)
        : Instruction(tvm::Instruction::Void), _range_scan_type(range_scan_type), m_subQueryIndex(subQueryIndex)
    {}

    ~InitSearchKeyInstruction() final
    {}

    uint64_t Exec(tvm::ExecContext* exec_context) final
    {
        if (_range_scan_type == JIT_RANGE_SCAN_INNER) {
            InitKey(exec_context->_jit_context->m_innerSearchKey, exec_context->_jit_context->m_innerIndex);
        } else if (_range_scan_type == JIT_RANGE_SCAN_MAIN) {
            InitKey(exec_context->_jit_context->m_searchKey, exec_context->_jit_context->m_index);
        } else if (_range_scan_type == JIT_RANGE_SCAN_SUB_QUERY) {
            JitContext::SubQueryData* subQueryData = &exec_context->_jit_context->m_subQueryData[m_subQueryIndex];
            InitKey(subQueryData->m_searchKey, subQueryData->m_index);
        } else {
            return (uint64_t)MOT::RC_ERROR;
        }
        return (uint64_t)MOT::RC_OK;
    }

    void Dump() final
    {
        if (_range_scan_type == JIT_RANGE_SCAN_INNER) {
            (void)fprintf(stderr, "InitKey(%%inner_search_key, %%inner_index)");
        } else if (_range_scan_type == JIT_RANGE_SCAN_MAIN) {
            (void)fprintf(stderr, "InitKey(%%search_key, %%index)");
        } else if (_range_scan_type == JIT_RANGE_SCAN_SUB_QUERY) {
            (void)fprintf(
                stderr, "InitKey(%%sub_query_key[%d], %%sub_query_index[%d])", m_subQueryIndex, m_subQueryIndex);
        }
    }

private:
    JitRangeScanType _range_scan_type;
    int m_subQueryIndex;
};

/** GetColumnAtInstruction */
class GetColumnAtInstruction : public tvm::Instruction {
public:
    GetColumnAtInstruction(int table_colid, JitRangeScanType range_scan_type, int subQueryIndex)
        : _table_colid(table_colid), _range_scan_type(range_scan_type), m_subQueryIndex(subQueryIndex)
    {}

    ~GetColumnAtInstruction() final
    {}

protected:
    uint64_t ExecImpl(tvm::ExecContext* exec_context) final
    {
        MOT::Column* column = nullptr;
        if (_range_scan_type == JIT_RANGE_SCAN_INNER) {
            column = getColumnAt(exec_context->_jit_context->m_innerTable, _table_colid);
        } else if (_range_scan_type == JIT_RANGE_SCAN_MAIN) {
            column = getColumnAt(exec_context->_jit_context->m_table, _table_colid);
        } else if (_range_scan_type == JIT_RANGE_SCAN_SUB_QUERY) {
            column = getColumnAt(exec_context->_jit_context->m_subQueryData[m_subQueryIndex].m_table, _table_colid);
        } else {
            return (uint64_t) nullptr;
        }
        return (uint64_t)column;
    }

    void DumpImpl() final
    {
        if (_range_scan_type == JIT_RANGE_SCAN_INNER) {
            (void)fprintf(stderr, "%%column = %%inner_table->columns[%d]", _table_colid);
        } else if (_range_scan_type == JIT_RANGE_SCAN_MAIN) {
            (void)fprintf(stderr, "%%column = %%table->columns[%d]", _table_colid);
        } else if (_range_scan_type == JIT_RANGE_SCAN_SUB_QUERY) {
            (void)fprintf(stderr, "%%column = %%sub_query[%d].table->columns[%d]", m_subQueryIndex, _table_colid);
        }
    }

private:
    int _table_colid;
    JitRangeScanType _range_scan_type;
    int m_subQueryIndex;
};

/** SetExprArgIsNullInstruction */
class SetExprArgIsNullInstruction : public tvm::Instruction {
public:
    SetExprArgIsNullInstruction(int arg_pos, int is_null)
        : Instruction(tvm::Instruction::Void), _arg_pos(arg_pos), _is_null(is_null)
    {}

    ~SetExprArgIsNullInstruction() final
    {}

    uint64_t Exec(tvm::ExecContext* exec_context) final
    {
        setExprArgIsNull(_arg_pos, _is_null);
        return (uint64_t)MOT::RC_OK;
    }

    void Dump() final
    {
        (void)fprintf(stderr, "setExprArgIsNull(%d, %d)", _arg_pos, _is_null);
    }

private:
    int _arg_pos;
    int _is_null;
};

/** GetExprArgIsNullInstruction */
class GetExprArgIsNullInstruction : public tvm::Instruction {
public:
    explicit GetExprArgIsNullInstruction(int arg_pos) : _arg_pos(arg_pos)
    {}

    ~GetExprArgIsNullInstruction() final
    {}

protected:
    uint64_t ExecImpl(tvm::ExecContext* exec_context) final
    {
        return (uint64_t)getExprArgIsNull(_arg_pos);
    }

    void DumpImpl() final
    {
        (void)fprintf(stderr, "getExprArgIsNull(%d)", _arg_pos);
    }

private:
    int _arg_pos;
};

/** WriteDatumColumnInstruction */
class WriteDatumColumnInstruction : public tvm::Instruction {
public:
    WriteDatumColumnInstruction(Instruction* row_inst, Instruction* column_inst, Instruction* sub_expr)
        : Instruction(tvm::Instruction::Void), _row_inst(row_inst), _column_inst(column_inst), _sub_expr(sub_expr)
    {
        AddSubInstruction(row_inst);
        AddSubInstruction(column_inst);
        AddSubInstruction(sub_expr);
    }

    ~WriteDatumColumnInstruction() final
    {
        _row_inst = nullptr;
        _column_inst = nullptr;
    }

    uint64_t Exec(tvm::ExecContext* exec_context) final
    {
        Datum arg = (Datum)_sub_expr->Exec(exec_context);
        MOT::Row* row = (MOT::Row*)_row_inst->Exec(exec_context);
        MOT::Column* column = (MOT::Column*)_column_inst->Exec(exec_context);
        writeDatumColumn(row, column, arg);
        return (uint64_t)MOT::RC_OK;
    }

    void Dump() final
    {
        (void)fprintf(stderr, "writeDatumColumn(%%row=");
        _row_inst->Dump();
        (void)fprintf(stderr, ", %%column=");
        _column_inst->Dump();
        (void)fprintf(stderr, ", ");
        _sub_expr->Dump();
        (void)fprintf(stderr, ")");
    }

private:
    tvm::Instruction* _row_inst;
    tvm::Instruction* _column_inst;
    tvm::Instruction* _sub_expr;
};

/** BuildDatumKeyInstruction */
class BuildDatumKeyInstruction : public tvm::Instruction {
public:
    BuildDatumKeyInstruction(tvm::Instruction* column_inst, tvm::Instruction* sub_expr, int index_colid, int offset,
        int size, int value_type, JitRangeIteratorType range_itr_type, JitRangeScanType range_scan_type,
        int subQueryIndex)
        : Instruction(tvm::Instruction::Void),
          _column_inst(column_inst),
          _sub_expr(sub_expr),
          _index_colid(index_colid),
          _offset(offset),
          _size(size),
          _value_type(value_type),
          _range_itr_type(range_itr_type),
          _range_scan_type(range_scan_type),
          m_subQueryIndex(subQueryIndex)
    {
        AddSubInstruction(_column_inst);
        AddSubInstruction(_sub_expr);
    }

    ~BuildDatumKeyInstruction() final
    {
        _column_inst = nullptr;
        _sub_expr = nullptr;
    }

    uint64_t Exec(tvm::ExecContext* exec_context) final
    {
        Datum arg = (Datum)_sub_expr->Exec(exec_context);
        MOT::Column* column = (MOT::Column*)_column_inst->Exec(exec_context);
        MOT::Key* key = getExecContextKey(exec_context, _range_itr_type, _range_scan_type, m_subQueryIndex);
        buildDatumKey(column, key, arg, _index_colid, _offset, _size, _value_type);
        return (uint64_t)MOT::RC_OK;
    }

    void Dump() final
    {
        (void)fprintf(stderr, "buildDatumKey(%%column=");
        _column_inst->Dump();
        if (_range_scan_type == JIT_RANGE_SCAN_INNER) {
            if (_range_itr_type == JIT_RANGE_ITERATOR_END) {
                (void)fprintf(stderr, ", %%inner_end_iterator_key, ");
            } else {
                (void)fprintf(stderr, ", %%inner_search_key, ");
            }
        } else if (_range_scan_type == JIT_RANGE_SCAN_MAIN) {
            if (_range_itr_type == JIT_RANGE_ITERATOR_END) {
                (void)fprintf(stderr, ", %%end_iterator_key, ");
            } else {
                (void)fprintf(stderr, ", %%search_key, ");
            }
        } else if (_range_scan_type == JIT_RANGE_SCAN_SUB_QUERY) {
            if (_range_itr_type == JIT_RANGE_ITERATOR_END) {
                (void)fprintf(stderr, ", %%sub_query[%d].end_iterator_key, ", m_subQueryIndex);
            } else {
                (void)fprintf(stderr, ", %%sub_query[%d].search_key, ", m_subQueryIndex);
            }
        }
        _sub_expr->Dump();
        (void)fprintf(
            stderr, ", index_colid=%d, offset=%d, size=%d, value_type=%d)", _index_colid, _offset, _size, _value_type);
    }

private:
    tvm::Instruction* _column_inst;
    tvm::Instruction* _sub_expr;
    int _index_colid;
    int _offset;
    int _size;
    int _value_type;
    JitRangeIteratorType _range_itr_type;
    JitRangeScanType _range_scan_type;
    int m_subQueryIndex;
};

/** SetBitInstruction */
class SetBitInstruction : public tvm::Instruction {
public:
    explicit SetBitInstruction(int col) : Instruction(tvm::Instruction::Void), _col(col)
    {}

    ~SetBitInstruction() final
    {}

    uint64_t Exec(tvm::ExecContext* exec_context) final
    {
        setBit(exec_context->_jit_context->m_bitmapSet, _col);
        return (uint64_t)MOT::RC_OK;
    }

    void Dump() final
    {
        (void)fprintf(stderr, "setBit(%%bitmap_set, col=%d)", _col);
    }

private:
    int _col;
};

/** @class ResetBitmapSetInstruction */
class ResetBitmapSetInstruction : public tvm::Instruction {
public:
    ResetBitmapSetInstruction() : Instruction(tvm::Instruction::Void)
    {}

    ~ResetBitmapSetInstruction() final
    {}

    uint64_t Exec(tvm::ExecContext* exec_context) final
    {
        resetBitmapSet(exec_context->_jit_context->m_bitmapSet);
        return (uint64_t)MOT::RC_OK;
    }

    void Dump() final
    {
        (void)fprintf(stderr, "resetBitmapSet(%%bitmap_set)");
    }
};

/** @class WriteRowInstruction */
class WriteRowInstruction : public tvm::Instruction {
public:
    explicit WriteRowInstruction(tvm::Instruction* row_inst) : _row_inst(row_inst)
    {
        AddSubInstruction(_row_inst);
    }

    ~WriteRowInstruction() final
    {
        _row_inst = nullptr;
    }

protected:
    uint64_t ExecImpl(tvm::ExecContext* exec_context) final
    {
        MOT::Row* row = (MOT::Row*)_row_inst->Exec(exec_context);
        return (uint64_t)writeRow(row, exec_context->_jit_context->m_bitmapSet);
    }

    void DumpImpl() final
    {
        (void)fprintf(stderr, "writeRow(%%row=");
        _row_inst->Dump();
        (void)fprintf(stderr, ", %%bitmap_set)");
    }

private:
    tvm::Instruction* _row_inst;
};

/** SearchRowInstruction */
class SearchRowInstruction : public tvm::Instruction {
public:
    SearchRowInstruction(MOT::AccessType access_mode_value, JitRangeScanType range_scan_type, int subQueryIndex)
        : _access_mode_value(access_mode_value), _range_scan_type(range_scan_type), m_subQueryIndex(subQueryIndex)
    {}

    ~SearchRowInstruction() final
    {}

protected:
    uint64_t ExecImpl(tvm::ExecContext* exec_context) final
    {
        MOT::Row* row = nullptr;
        if (_range_scan_type == JIT_RANGE_SCAN_INNER) {
            row = searchRow(exec_context->_jit_context->m_innerTable,
                exec_context->_jit_context->m_innerSearchKey,
                _access_mode_value);
        } else if (_range_scan_type == JIT_RANGE_SCAN_MAIN) {
            row = searchRow(
                exec_context->_jit_context->m_table, exec_context->_jit_context->m_searchKey, _access_mode_value);
        } else if (_range_scan_type == JIT_RANGE_SCAN_SUB_QUERY) {
            JitContext::SubQueryData* subQueryData = &exec_context->_jit_context->m_subQueryData[m_subQueryIndex];
            row = searchRow(subQueryData->m_table, subQueryData->m_searchKey, _access_mode_value);
        } else {
            return (uint64_t) nullptr;
        }
        return (uint64_t)row;
    }

    void DumpImpl() final
    {
        if (_range_scan_type == JIT_RANGE_SCAN_INNER) {
            (void)fprintf(stderr,
                "%%row = searchRow(%%inner_table, %%inner_search_key, access_mode_value=%d)",
                (int)_access_mode_value);
        } else if (_range_scan_type == JIT_RANGE_SCAN_MAIN) {
            (void)fprintf(
                stderr, "%%row = searchRow(%%table, %%search_key, access_mode_value=%d)", (int)_access_mode_value);
        } else if (_range_scan_type == JIT_RANGE_SCAN_SUB_QUERY) {
            (void)fprintf(stderr,
                "%%row = searchRow(%%sub_query[%d].table, %%sub_query[%d].search_key, access_mode_value=%d)",
                m_subQueryIndex,
                m_subQueryIndex,
                (int)_access_mode_value);
        }
    }

private:
    MOT::AccessType _access_mode_value;
    JitRangeScanType _range_scan_type;
    int m_subQueryIndex;
};

/** @class CreateNewRowInstruction */
class CreateNewRowInstruction : public tvm::Instruction {
public:
    CreateNewRowInstruction()
    {}

    ~CreateNewRowInstruction() final
    {}

protected:
    uint64_t ExecImpl(tvm::ExecContext* exec_context) final
    {
        MOT::Row* row = createNewRow(exec_context->_jit_context->m_table);
        return (uint64_t)row;
    }

    void DumpImpl() final
    {
        (void)fprintf(stderr, "%%row = createNewRow(%%table)");
    }
};

/** @class InsertRowInstruction */
class InsertRowInstruction : public tvm::Instruction {
public:
    explicit InsertRowInstruction(tvm::Instruction* row_inst) : _row_inst(row_inst)
    {
        AddSubInstruction(_row_inst);
    }

    ~InsertRowInstruction() final
    {
        _row_inst = nullptr;
    }

protected:
    uint64_t ExecImpl(tvm::ExecContext* exec_context) final
    {
        MOT::Row* row = (MOT::Row*)_row_inst->Exec(exec_context);
        return (uint64_t)insertRow(exec_context->_jit_context->m_table, row);
    }

    void DumpImpl() final
    {
        (void)fprintf(stderr, "insertRow(%%table, %%row=");
        _row_inst->Dump();
        (void)fprintf(stderr, ")");
    }

private:
    tvm::Instruction* _row_inst;
};

/** @class DeleteRowInstruction */
class DeleteRowInstruction : public tvm::Instruction {
public:
    DeleteRowInstruction()
    {}
    ~DeleteRowInstruction() final
    {}

protected:
    uint64_t ExecImpl(tvm::ExecContext* exec_context) final
    {
        return (uint64_t)deleteRow();
    }

    void DumpImpl() final
    {
        (void)fprintf(stderr, "deleteRow()");
    }
};

/** @class SetRowNullBitsInstruction */
class SetRowNullBitsInstruction : public tvm::Instruction {
public:
    explicit SetRowNullBitsInstruction(tvm::Instruction* row_inst)
        : Instruction(tvm::Instruction::Void), _row_inst(row_inst)
    {
        AddSubInstruction(_row_inst);
    }

    ~SetRowNullBitsInstruction() final
    {
        _row_inst = nullptr;
    }

    uint64_t Exec(tvm::ExecContext* exec_context) final
    {
        MOT::Row* row = (MOT::Row*)_row_inst->Exec(exec_context);
        setRowNullBits(exec_context->_jit_context->m_table, row);
        return (uint64_t)MOT::RC_OK;
    }

    void Dump() final
    {
        (void)fprintf(stderr, "setRowNullBits(%%table, %%row=");
        _row_inst->Dump();
        (void)fprintf(stderr, ")");
    }

private:
    Instruction* _row_inst;
};

/** @class SetExprResultNullBitInstruction */
class SetExprResultNullBitInstruction : public tvm::Instruction {
public:
    SetExprResultNullBitInstruction(tvm::Instruction* row_inst, int table_colid)
        : _row_inst(row_inst), _table_colid(table_colid)
    {
        AddSubInstruction(_row_inst);
    }

    ~SetExprResultNullBitInstruction() final
    {
        _row_inst = nullptr;
    }

protected:
    uint64_t ExecImpl(tvm::ExecContext* exec_context) final
    {
        // always performed on main table (there is no inner table in UPDATE command)
        MOT::Row* row = (MOT::Row*)_row_inst->Exec(exec_context);
        return (uint64_t)setExprResultNullBit(exec_context->_jit_context->m_table, row, _table_colid);
    }

    void DumpImpl() final
    {
        (void)fprintf(stderr, "setExprResultNullBit(%%table, %%row=");
        _row_inst->Dump();
        (void)fprintf(stderr, ", table_colid=%d)", _table_colid);
    }

private:
    Instruction* _row_inst;
    int _table_colid;
};

/** @class ExecClearTupleInstruction */
class ExecClearTupleInstruction : public tvm::Instruction {
public:
    ExecClearTupleInstruction() : Instruction(tvm::Instruction::Void)
    {}

    ~ExecClearTupleInstruction() final
    {}

    uint64_t Exec(tvm::ExecContext* exec_context) final
    {
        execClearTuple(exec_context->_slot);
        return (uint64_t)MOT::RC_OK;
    }

    void Dump() final
    {
        (void)fprintf(stderr, "execClearTuple(%%slot)");
    }
};

/** @class ExecStoreVirtualTupleInstruction */
class ExecStoreVirtualTupleInstruction : public tvm::Instruction {
public:
    ExecStoreVirtualTupleInstruction() : Instruction(tvm::Instruction::Void)
    {}

    ~ExecStoreVirtualTupleInstruction() final
    {}

    uint64_t Exec(tvm::ExecContext* exec_context) final
    {
        execStoreVirtualTuple(exec_context->_slot);
        return (uint64_t)MOT::RC_OK;
    }

    void Dump() final
    {
        (void)fprintf(stderr, "execStoreVirtualTuple(%%slot)");
    }
};

/** @class SelectColumnInstruction */
class SelectColumnInstruction : public tvm::Instruction {
public:
    SelectColumnInstruction(tvm::Instruction* row_inst, int table_colid, int tuple_colid,
        JitRangeScanType range_scan_type, int subQueryIndex)
        : Instruction(tvm::Instruction::Void),
          _row_inst(row_inst),
          _table_colid(table_colid),
          _tuple_colid(tuple_colid),
          _range_scan_type(range_scan_type),
          m_subQueryIndex(subQueryIndex)
    {
        AddSubInstruction(_row_inst);
    }

    ~SelectColumnInstruction() final
    {
        _row_inst = nullptr;
    }

    uint64_t Exec(tvm::ExecContext* exec_context) final
    {
        MOT::Row* row = (MOT::Row*)_row_inst->Exec(exec_context);
        if (_range_scan_type == JIT_RANGE_SCAN_INNER) {
            selectColumn(
                exec_context->_jit_context->m_innerTable, row, exec_context->_slot, _table_colid, _tuple_colid);
        } else if (_range_scan_type == JIT_RANGE_SCAN_MAIN) {
            selectColumn(exec_context->_jit_context->m_table, row, exec_context->_slot, _table_colid, _tuple_colid);
        } else if (_range_scan_type == JIT_RANGE_SCAN_SUB_QUERY) {
            JitContext::SubQueryData* subQueryData = &exec_context->_jit_context->m_subQueryData[m_subQueryIndex];
            selectColumn(subQueryData->m_table, row, subQueryData->m_slot, _table_colid, _tuple_colid);
        } else {
            return (uint64_t)MOT::RC_ERROR;
        }
        return (uint64_t)MOT::RC_OK;
    }

    void Dump() final
    {
        if (_range_scan_type == JIT_RANGE_SCAN_INNER) {
            (void)fprintf(stderr, "selectColumn(%%inner_table, %%row=");
        } else if (_range_scan_type == JIT_RANGE_SCAN_MAIN) {
            (void)fprintf(stderr, "selectColumn(%%table, %%row=");
        } else if (_range_scan_type == JIT_RANGE_SCAN_SUB_QUERY) {
            (void)fprintf(stderr, "selectColumn(%%sub_query[%d].table, %%row=", m_subQueryIndex);
        }
        _row_inst->Dump();
        if (_range_scan_type == JIT_RANGE_SCAN_SUB_QUERY) {
            (void)fprintf(stderr,
                ", %%sub_query[%d].slot, table_colid=%d, tuple_colid=%d)",
                m_subQueryIndex,
                _table_colid,
                _tuple_colid);
        } else {
            (void)fprintf(stderr, ", %%slot, table_colid=%d, tuple_colid=%d)", _table_colid, _tuple_colid);
        }
    }

private:
    tvm::Instruction* _row_inst;
    int _table_colid;
    int _tuple_colid;
    JitRangeScanType _range_scan_type;
    int m_subQueryIndex;
};

/**
 * @class SetTpProcessedInstruction. Sets the number of tuples processed from the number of rows
 * processed. This communicates back to the caller a return value.
 */
class SetTpProcessedInstruction : public tvm::Instruction {
public:
    /** @brief Constructor. */
    SetTpProcessedInstruction() : Instruction(tvm::Instruction::Void)
    {}

    /** @brief Destructor. */
    ~SetTpProcessedInstruction() final
    {}

    /**
     * @brief Executes the instruction by setting the number of tuples processed.
     * @param exec_context The execution context.
     * @return Not used.
     */
    uint64_t Exec(tvm::ExecContext* exec_context) final
    {
        setTpProcessed(exec_context->_tp_processed, exec_context->_rows_processed);
        return (uint64_t)MOT::RC_OK;
    }

    /** @brief Dumps the instruction to the standard error stream. */
    void Dump() final
    {
        (void)fprintf(stderr, "setTpProcessed(%%tp_processed, %%rows_processed)");
    }
};

/**
 * @class SetScanEndedInstruction. Communicates back to the user whether a range scan has ended
 * (i.e. whether there are no more tuples to report in this query).
 */
class SetScanEndedInstruction : public tvm::Instruction {
public:
    /**
     * @brief Constructor.
     * @param result The scan-ended result. Zero means there are more tuples to report. One means the scan ended.
     */
    explicit SetScanEndedInstruction(int result) : Instruction(tvm::Instruction::Void), _result(result)
    {}

    /** @brief Destructor. */
    ~SetScanEndedInstruction() final
    {}

    /**
     * @brief Executes the instruction by setting the scan-ended value.
     * @param exec_context The execution context.
     * @return Not used.
     */
    uint64_t Exec(tvm::ExecContext* exec_context) final
    {
        setScanEnded(exec_context->_scan_ended, _result);
        return (uint64_t)MOT::RC_OK;
    }

    /** @brief Dumps the instruction to the standard error stream. */
    void Dump() final
    {
        (void)fprintf(stderr, "setScanEnded(%%scan_ended, %d)", _result);
    }

private:
    /** @var The scan-ended result. Zero means there are more tuples to report. One means the scan ended. */
    int _result;
};

/** @class ResetRowsProcessedInstruction */
class ResetRowsProcessedInstruction : public tvm::Instruction {
public:
    ResetRowsProcessedInstruction() : Instruction(tvm::Instruction::Void)
    {}

    ~ResetRowsProcessedInstruction() final
    {}

    uint64_t Exec(tvm::ExecContext* exec_context) final
    {
        exec_context->_rows_processed = 0;
        return (uint64_t)MOT::RC_OK;
    }

    void Dump() final
    {
        (void)fprintf(stderr, "%%rows_processed = 0");
    }
};

/** @class IncrementRowsProcessedInstruction */
class IncrementRowsProcessedInstruction : public tvm::Instruction {
public:
    IncrementRowsProcessedInstruction() : Instruction(tvm::Instruction::Void)
    {}

    ~IncrementRowsProcessedInstruction() final
    {}

    uint64_t Exec(tvm::ExecContext* exec_context) final
    {
        ++exec_context->_rows_processed;
        return 0;
    }

    void Dump() final
    {
        (void)fprintf(stderr, "%%rows_processed += 1");
    }
};

/** @class CopyKeyInstruction */
class CopyKeyInstruction : public tvm::Instruction {
public:
    explicit CopyKeyInstruction(JitRangeScanType range_scan_type, int subQueryIndex)
        : Instruction(tvm::Instruction::Void), _range_scan_type(range_scan_type), m_subQueryIndex(subQueryIndex)
    {}

    ~CopyKeyInstruction() final
    {}

    uint64_t Exec(tvm::ExecContext* exec_context) final
    {
        if (_range_scan_type == JIT_RANGE_SCAN_INNER) {
            copyKey(exec_context->_jit_context->m_innerIndex,
                exec_context->_jit_context->m_innerSearchKey,
                exec_context->_jit_context->m_innerEndIteratorKey);
        } else if (_range_scan_type == JIT_RANGE_SCAN_MAIN) {
            copyKey(exec_context->_jit_context->m_index,
                exec_context->_jit_context->m_searchKey,
                exec_context->_jit_context->m_endIteratorKey);
        } else if (_range_scan_type == JIT_RANGE_SCAN_SUB_QUERY) {
            JitContext::SubQueryData* subQueryData = &exec_context->_jit_context->m_subQueryData[m_subQueryIndex];
            copyKey(subQueryData->m_index, subQueryData->m_searchKey, subQueryData->m_endIteratorKey);
        }
        return (uint64_t)MOT::RC_OK;
    }

    void Dump() final
    {
        if (_range_scan_type == JIT_RANGE_SCAN_INNER) {
            (void)fprintf(stderr, "copyKey(%%inner_index, %%inner_search_key, %%inner_end_iterator_key)");
        } else if (_range_scan_type == JIT_RANGE_SCAN_MAIN) {
            (void)fprintf(stderr, "copyKey(%%index, %%search_key, %%end_iterator_key)");
        } else if (_range_scan_type == JIT_RANGE_SCAN_SUB_QUERY) {
            (void)fprintf(stderr,
                "copyKey(%%sub_query[%d].index, %%sub_query[%d].search_key, %%sub_query[%d].end_iterator_key)",
                m_subQueryIndex,
                m_subQueryIndex,
                m_subQueryIndex);
        }
    }

private:
    JitRangeScanType _range_scan_type;
    int m_subQueryIndex;
};

/** @class FillKeyPatternInstruction */
class FillKeyPatternInstruction : public tvm::Instruction {
public:
    FillKeyPatternInstruction(JitRangeIteratorType itr_type, unsigned char pattern, int offset, int size,
        JitRangeScanType range_scan_type, int subQueryIndex)
        : Instruction(tvm::Instruction::Void),
          _itr_type(itr_type),
          _pattern(pattern),
          _offset(offset),
          _size(size),
          _range_scan_type(range_scan_type),
          m_subQueryIndex(subQueryIndex)
    {}

    ~FillKeyPatternInstruction() final
    {}

    uint64_t Exec(tvm::ExecContext* exec_context) final
    {
        MOT::Key* key = getExecContextKey(exec_context, _itr_type, _range_scan_type, m_subQueryIndex);
        FillKeyPattern(key, _pattern, _offset, _size);
        return (uint64_t)MOT::RC_OK;
    }

    void Dump() final
    {
        if (_range_scan_type == JIT_RANGE_SCAN_INNER) {
            if (_itr_type == JIT_RANGE_ITERATOR_END) {
                (void)fprintf(stderr,
                    "FillKeyPattern(%%inner_end_iterator_key, pattern=0x%X, offset=%d, size=%d)",
                    (unsigned)_pattern,
                    _offset,
                    _size);
            } else {
                (void)fprintf(stderr,
                    "FillKeyPattern(%%inner_search_key, pattern=0x%X, offset=%d, size=%d)",
                    (unsigned)_pattern,
                    _offset,
                    _size);
            }
        } else if (_range_scan_type == JIT_RANGE_SCAN_MAIN) {
            if (_itr_type == JIT_RANGE_ITERATOR_END) {
                (void)fprintf(stderr,
                    "FillKeyPattern(%%end_iterator_key, pattern=0x%X, offset=%d, size=%d)",
                    (unsigned)_pattern,
                    _offset,
                    _size);
            } else {
                (void)fprintf(stderr,
                    "FillKeyPattern(%%search_key, pattern=0x%X, offset=%d, size=%d)",
                    (unsigned)_pattern,
                    _offset,
                    _size);
            }
        } else if (_range_scan_type == JIT_RANGE_SCAN_SUB_QUERY) {
            if (_itr_type == JIT_RANGE_ITERATOR_END) {
                (void)fprintf(stderr,
                    "FillKeyPattern(%%sub_query[%d].end_iterator_key, pattern=0x%X, offset=%d, size=%d)",
                    m_subQueryIndex,
                    (unsigned)_pattern,
                    _offset,
                    _size);
            } else {
                (void)fprintf(stderr,
                    "FillKeyPattern(%%sub_query[%d].search_key, pattern=0x%X, offset=%d, size=%d)",
                    m_subQueryIndex,
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
    int m_subQueryIndex;
};

/** @class AdjustKeyInstruction */
class AdjustKeyInstruction : public tvm::Instruction {
public:
    AdjustKeyInstruction(
        JitRangeIteratorType itr_type, unsigned char pattern, JitRangeScanType range_scan_type, int subQueryIndex)
        : Instruction(tvm::Instruction::Void),
          _itr_type(itr_type),
          _pattern(pattern),
          _range_scan_type(range_scan_type),
          m_subQueryIndex(subQueryIndex)
    {}

    ~AdjustKeyInstruction() final
    {}

    uint64_t Exec(tvm::ExecContext* exec_context) final
    {
        MOT::Key* key = getExecContextKey(exec_context, _itr_type, _range_scan_type, m_subQueryIndex);
        MOT::Index* index = nullptr;
        if (_range_scan_type == JIT_RANGE_SCAN_INNER) {
            index = exec_context->_jit_context->m_innerIndex;
        } else if (_range_scan_type == JIT_RANGE_SCAN_MAIN) {
            index = exec_context->_jit_context->m_index;
        } else if (_range_scan_type == JIT_RANGE_SCAN_SUB_QUERY) {
            index = exec_context->_jit_context->m_subQueryData[m_subQueryIndex].m_index;
        }
        adjustKey(key, index, _pattern);
        return (uint64_t)MOT::RC_OK;
    }

    void Dump() final
    {
        if (_range_scan_type == JIT_RANGE_SCAN_INNER) {
            if (_itr_type == JIT_RANGE_ITERATOR_END) {
                (void)fprintf(
                    stderr, "adjustKey(%%inner_end_iterator_key, %%inner_index, pattern=0x%X)", (unsigned)_pattern);
            } else {
                (void)fprintf(stderr, "adjustKey(%%inner_search_key, %%inner_index, pattern=0x%X)", (unsigned)_pattern);
            }
        } else if (_range_scan_type == JIT_RANGE_SCAN_MAIN) {
            if (_itr_type == JIT_RANGE_ITERATOR_END) {
                (void)fprintf(stderr, "adjustKey(%%end_iterator_key, %%index, pattern=0x%X)", (unsigned)_pattern);
            } else {
                (void)fprintf(stderr, "adjustKey(%%search_key, %%index, pattern=0x%X)", (unsigned)_pattern);
            }
        } else if (_range_scan_type == JIT_RANGE_SCAN_SUB_QUERY) {
            if (_itr_type == JIT_RANGE_ITERATOR_END) {
                (void)fprintf(stderr,
                    "adjustKey(%%sub_query[%d].end_iterator_key, %%sub_query[%d].index, pattern=0x%X)",
                    m_subQueryIndex,
                    m_subQueryIndex,
                    (unsigned)_pattern);
            } else {
                (void)fprintf(stderr,
                    "adjustKey(%%sub_query[%d].search_key, %%sub_query[%d].index, pattern=0x%X)",
                    m_subQueryIndex,
                    m_subQueryIndex,
                    (unsigned)_pattern);
            }
        }
    }

private:
    JitRangeIteratorType _itr_type;
    unsigned char _pattern;
    JitRangeScanType _range_scan_type;
    int m_subQueryIndex;
};

/** @class SearchIteratorInstruction */
class SearchIteratorInstruction : public tvm::Instruction {
public:
    SearchIteratorInstruction(JitIndexScanDirection index_scan_direction, JitRangeBoundMode range_bound_mode,
        JitRangeScanType range_scan_type, int subQueryIndex)
        : _index_scan_direction(index_scan_direction),
          _range_bound_mode(range_bound_mode),
          _range_scan_type(range_scan_type),
          m_subQueryIndex(subQueryIndex)
    {}

    ~SearchIteratorInstruction() final
    {}

protected:
    uint64_t ExecImpl(tvm::ExecContext* exec_context) final
    {
        MOT::IndexIterator* iterator = nullptr;
        int forward_scan = (_index_scan_direction == JIT_INDEX_SCAN_FORWARD) ? 1 : 0;
        int include_bound = (_range_bound_mode == JIT_RANGE_BOUND_INCLUDE) ? 1 : 0;
        if (_range_scan_type == JIT_RANGE_SCAN_INNER) {
            iterator = searchIterator(exec_context->_jit_context->m_innerIndex,
                exec_context->_jit_context->m_innerSearchKey,
                forward_scan,
                include_bound);
        } else if (_range_scan_type == JIT_RANGE_SCAN_MAIN) {
            iterator = searchIterator(exec_context->_jit_context->m_index,
                exec_context->_jit_context->m_searchKey,
                forward_scan,
                include_bound);
        } else if (_range_scan_type == JIT_RANGE_SCAN_SUB_QUERY) {
            iterator = searchIterator(exec_context->_jit_context->m_subQueryData[m_subQueryIndex].m_index,
                exec_context->_jit_context->m_subQueryData[m_subQueryIndex].m_searchKey,
                forward_scan,
                include_bound);
        }
        return (uint64_t)iterator;
    }

    void DumpImpl() final
    {
        int forward_scan = (_index_scan_direction == JIT_INDEX_SCAN_FORWARD) ? 1 : 0;
        int include_bound = (_range_bound_mode == JIT_RANGE_BOUND_INCLUDE) ? 1 : 0;
        if (_range_scan_type == JIT_RANGE_SCAN_INNER) {
            (void)fprintf(stderr,
                "searchIterator(%%inner_index, %%inner_search_key, forward_scan=%d, include_bound=%d)",
                forward_scan,
                include_bound);
        } else if (_range_scan_type == JIT_RANGE_SCAN_MAIN) {
            (void)fprintf(stderr,
                "searchIterator(%%index, %%search_key, forward_scan=%d, include_bound=%d)",
                forward_scan,
                include_bound);
        } else if (_range_scan_type == JIT_RANGE_SCAN_SUB_QUERY) {
            (void)fprintf(stderr,
                "searchIterator(%%sub_query[%d].index, %%sub_query[%d].search_key, forward_scan=%d, include_bound=%d)",
                m_subQueryIndex,
                m_subQueryIndex,
                forward_scan,
                include_bound);
        }
    }

private:
    JitIndexScanDirection _index_scan_direction;
    JitRangeBoundMode _range_bound_mode;
    JitRangeScanType _range_scan_type;
    int m_subQueryIndex;
};

/** @class BeginIteratorInstruction */
class BeginIteratorInstruction : public tvm::Instruction {
public:
    BeginIteratorInstruction(JitRangeScanType rangeScanType, int subQueryIndex)
        : m_rangeScanType(rangeScanType), m_subQueryIndex(subQueryIndex)
    {}

    ~BeginIteratorInstruction() final
    {}

protected:
    uint64_t ExecImpl(tvm::ExecContext* exec_context) final
    {
        MOT::IndexIterator* iterator = nullptr;
        if (m_rangeScanType == JIT_RANGE_SCAN_INNER) {
            iterator = beginIterator(exec_context->_jit_context->m_innerIndex);
        } else if (m_rangeScanType == JIT_RANGE_SCAN_MAIN) {
            iterator = beginIterator(exec_context->_jit_context->m_index);
        } else if (m_rangeScanType == JIT_RANGE_SCAN_SUB_QUERY) {
            iterator = beginIterator(exec_context->_jit_context->m_subQueryData[m_subQueryIndex].m_index);
        }
        return (uint64_t)iterator;
    }

    void DumpImpl() final
    {
        if (m_rangeScanType == JIT_RANGE_SCAN_INNER) {
            (void)fprintf(stderr, "beginIterator(%%inner_index)");
        } else if (m_rangeScanType == JIT_RANGE_SCAN_MAIN) {
            (void)fprintf(stderr, "beginIterator(%%index)");
        } else if (m_rangeScanType == JIT_RANGE_SCAN_SUB_QUERY) {
            (void)fprintf(stderr, "beginIterator(%%sub_query[%d].index)", m_subQueryIndex);
        }
    }

private:
    JitRangeScanType m_rangeScanType;
    int m_subQueryIndex;
};

/** @class CreateEndIteratorInstruction */
class CreateEndIteratorInstruction : public tvm::Instruction {
public:
    CreateEndIteratorInstruction(JitIndexScanDirection index_scan_direction, JitRangeBoundMode range_bound_mode,
        JitRangeScanType range_scan_type, int subQueryIndex)
        : _index_scan_direction(index_scan_direction),
          _range_bound_mode(range_bound_mode),
          _range_scan_type(range_scan_type),
          m_subQueryIndex(subQueryIndex)
    {}

    ~CreateEndIteratorInstruction() final
    {}

protected:
    uint64_t ExecImpl(tvm::ExecContext* exec_context) final
    {
        MOT::IndexIterator* iterator = nullptr;
        int forward_scan = (_index_scan_direction == JIT_INDEX_SCAN_FORWARD) ? 1 : 0;
        int include_bound = (_range_bound_mode == JIT_RANGE_BOUND_INCLUDE) ? 1 : 0;
        if (_range_scan_type == JIT_RANGE_SCAN_INNER) {
            iterator = createEndIterator(exec_context->_jit_context->m_innerIndex,
                exec_context->_jit_context->m_innerEndIteratorKey,
                forward_scan,
                include_bound);
        } else if (_range_scan_type == JIT_RANGE_SCAN_MAIN) {
            iterator = createEndIterator(exec_context->_jit_context->m_index,
                exec_context->_jit_context->m_endIteratorKey,
                forward_scan,
                include_bound);
        } else if (_range_scan_type == JIT_RANGE_SCAN_SUB_QUERY) {
            iterator = createEndIterator(exec_context->_jit_context->m_subQueryData[m_subQueryIndex].m_index,
                exec_context->_jit_context->m_subQueryData[m_subQueryIndex].m_endIteratorKey,
                forward_scan,
                include_bound);
        }
        return (uint64_t)iterator;
    }

    void DumpImpl() final
    {
        int forward_scan = (_index_scan_direction == JIT_INDEX_SCAN_FORWARD) ? 1 : 0;
        int include_bound = (_range_bound_mode == JIT_RANGE_BOUND_INCLUDE) ? 1 : 0;
        if (_range_scan_type == JIT_RANGE_SCAN_INNER) {
            (void)fprintf(stderr,
                "createEndIterator(%%inner_index, %%inner_end_iterator_key, forward_scan=%d, include_bound=%d)",
                forward_scan,
                include_bound);
        } else if (_range_scan_type == JIT_RANGE_SCAN_MAIN) {
            (void)fprintf(stderr,
                "createEndIterator(%%index, %%end_iterator_key, forward_scan=%d, include_bound=%d)",
                forward_scan,
                include_bound);
        } else if (_range_scan_type == JIT_RANGE_SCAN_SUB_QUERY) {
            (void)fprintf(stderr,
                "createEndIterator(%%sub_query[%d].index, %%sub_query[%d].end_iterator_key, forward_scan=%d, "
                "include_bound=%d)",
                m_subQueryIndex,
                m_subQueryIndex,
                forward_scan,
                include_bound);
        }
    }

private:
    JitIndexScanDirection _index_scan_direction;
    JitRangeBoundMode _range_bound_mode;
    JitRangeScanType _range_scan_type;
    int m_subQueryIndex;
};

/** @class IsScanEndInstruction */
class IsScanEndInstruction : public tvm::Instruction {
public:
    IsScanEndInstruction(JitIndexScanDirection index_scan_direction, tvm::Instruction* begin_itr_inst,
        tvm::Instruction* end_itr_inst, JitRangeScanType range_scan_type, int subQueryIndex)
        : _index_scan_direction(index_scan_direction),
          _begin_itr_inst(begin_itr_inst),
          _end_itr_inst(end_itr_inst),
          _range_scan_type(range_scan_type),
          m_subQueryIndex(subQueryIndex)
    {
        AddSubInstruction(_begin_itr_inst);
        AddSubInstruction(_end_itr_inst);
    }

    ~IsScanEndInstruction() final
    {
        _begin_itr_inst = nullptr;
        _end_itr_inst = nullptr;
    }

protected:
    uint64_t ExecImpl(tvm::ExecContext* exec_context) final
    {
        uint64_t result = 0;
        MOT::IndexIterator* begin_itr = (MOT::IndexIterator*)_begin_itr_inst->Exec(exec_context);
        MOT::IndexIterator* end_itr = (MOT::IndexIterator*)_end_itr_inst->Exec(exec_context);
        int forward_scan = (_index_scan_direction == JIT_INDEX_SCAN_FORWARD) ? 1 : 0;
        if (_range_scan_type == JIT_RANGE_SCAN_INNER) {
            result = isScanEnd(exec_context->_jit_context->m_innerIndex, begin_itr, end_itr, forward_scan);
        } else if (_range_scan_type == JIT_RANGE_SCAN_MAIN) {
            result = isScanEnd(exec_context->_jit_context->m_index, begin_itr, end_itr, forward_scan);
        } else if (_range_scan_type == JIT_RANGE_SCAN_SUB_QUERY) {
            result = isScanEnd(
                exec_context->_jit_context->m_subQueryData[m_subQueryIndex].m_index, begin_itr, end_itr, forward_scan);
        }
        return result;
    }

    void DumpImpl() final
    {
        int forward_scan = (_index_scan_direction == JIT_INDEX_SCAN_FORWARD) ? 1 : 0;
        if (_range_scan_type == JIT_RANGE_SCAN_INNER) {
            (void)fprintf(stderr, "isScanEnd(%%inner_index, %%iterator=");
        } else if (_range_scan_type == JIT_RANGE_SCAN_MAIN) {
            (void)fprintf(stderr, "isScanEnd(%%index, %%iterator=");
        } else if (_range_scan_type == JIT_RANGE_SCAN_SUB_QUERY) {
            (void)fprintf(stderr, "isScanEnd(%%sub_query[%d].index, %%iterator=", m_subQueryIndex);
        }
        _begin_itr_inst->Dump();
        (void)fprintf(stderr, ", %%end_iterator=");
        _end_itr_inst->Dump();
        (void)fprintf(stderr, ", forward_scan=%d)", forward_scan);
    }

private:
    JitIndexScanDirection _index_scan_direction;
    tvm::Instruction* _begin_itr_inst;
    tvm::Instruction* _end_itr_inst;
    JitRangeScanType _range_scan_type;
    int m_subQueryIndex;
};

/** @class GetRowFromIteratorInstruction */
class GetRowFromIteratorInstruction : public tvm::Instruction {
public:
    GetRowFromIteratorInstruction(MOT::AccessType access_mode, JitIndexScanDirection index_scan_direction,
        tvm::Instruction* begin_itr_inst, Instruction* end_itr_inst, JitRangeScanType range_scan_type,
        int subQueryIndex)
        : _access_mode(access_mode),
          _index_scan_direction(index_scan_direction),
          _begin_itr_inst(begin_itr_inst),
          _end_itr_inst(end_itr_inst),
          _range_scan_type(range_scan_type),
          m_subQueryIndex(subQueryIndex)
    {
        AddSubInstruction(_begin_itr_inst);
        AddSubInstruction(_end_itr_inst);
    }

    ~GetRowFromIteratorInstruction() final
    {
        _begin_itr_inst = nullptr;
        _end_itr_inst = nullptr;
    }

protected:
    uint64_t ExecImpl(tvm::ExecContext* exec_context) final
    {
        MOT::Row* row = nullptr;
        MOT::IndexIterator* begin_itr = (MOT::IndexIterator*)_begin_itr_inst->Exec(exec_context);
        MOT::IndexIterator* end_itr = (MOT::IndexIterator*)_end_itr_inst->Exec(exec_context);
        int forward_scan = (_index_scan_direction == JIT_INDEX_SCAN_FORWARD) ? 1 : 0;
        if (_range_scan_type == JIT_RANGE_SCAN_INNER) {
            row = getRowFromIterator(
                exec_context->_jit_context->m_innerIndex, begin_itr, end_itr, _access_mode, forward_scan);
        } else if (_range_scan_type == JIT_RANGE_SCAN_MAIN) {
            row =
                getRowFromIterator(exec_context->_jit_context->m_index, begin_itr, end_itr, _access_mode, forward_scan);
        } else if (_range_scan_type == JIT_RANGE_SCAN_SUB_QUERY) {
            row = getRowFromIterator(exec_context->_jit_context->m_subQueryData[m_subQueryIndex].m_index,
                begin_itr,
                end_itr,
                _access_mode,
                forward_scan);
        }
        return (uint64_t)row;
    }

    void DumpImpl() final
    {
        int forward_scan = (_index_scan_direction == JIT_INDEX_SCAN_FORWARD) ? 1 : 0;
        if (_range_scan_type == JIT_RANGE_SCAN_INNER) {
            (void)fprintf(stderr, "%%row = getRowFromIterator(%%inner_index, %%iterator=");
        } else if (_range_scan_type == JIT_RANGE_SCAN_MAIN) {
            (void)fprintf(stderr, "%%row = getRowFromIterator(%%index, %%iterator=");
        } else if (_range_scan_type == JIT_RANGE_SCAN_SUB_QUERY) {
            (void)fprintf(stderr, "%%row = getRowFromIterator(%%sub_query[%d].index, %%iterator=", m_subQueryIndex);
        }
        _begin_itr_inst->Dump();
        (void)fprintf(stderr, ", %%end_iterator=");
        _end_itr_inst->Dump();
        (void)fprintf(stderr, ", access_mode=%d, forward_scan=%d)", (int)_access_mode, forward_scan);
    }

private:
    MOT::AccessType _access_mode;
    JitIndexScanDirection _index_scan_direction;
    tvm::Instruction* _begin_itr_inst;
    tvm::Instruction* _end_itr_inst;
    JitRangeScanType _range_scan_type;
    int m_subQueryIndex;
};

/** @class DestroyIteratorInstruction */
class DestroyIteratorInstruction : public tvm::Instruction {
public:
    explicit DestroyIteratorInstruction(Instruction* itr_inst)
        : Instruction(tvm::Instruction::Void), _itr_inst(itr_inst)
    {
        AddSubInstruction(_itr_inst);
    }

    ~DestroyIteratorInstruction() final
    {
        _itr_inst = nullptr;
    }

    uint64_t Exec(tvm::ExecContext* exec_context) final
    {
        MOT::IndexIterator* itr = (MOT::IndexIterator*)_itr_inst->Exec(exec_context);
        destroyIterator(itr);
        return (uint64_t)MOT::RC_OK;
    }

    void Dump() final
    {
        (void)fprintf(stderr, "destroyIterator(%%iterator=");
        _itr_inst->Dump();
        (void)fprintf(stderr, ")");
    }

private:
    Instruction* _itr_inst;
};

class IsNewScanInstruction : public tvm::Instruction {
public:
    IsNewScanInstruction()
    {}

    ~IsNewScanInstruction() final
    {}

protected:
    uint64_t ExecImpl(tvm::ExecContext* exec_context) final
    {
        return (uint64_t)exec_context->m_newScan;
    }

    void DumpImpl() final
    {
        (void)fprintf(stderr, "isNewScan()");
    }
};

/** @class GetStateIteratorInstruction */
class SetStateIteratorInstruction : public tvm::Instruction {
public:
    SetStateIteratorInstruction(
        tvm::Instruction* itr, JitRangeIteratorType range_itr_type, JitRangeScanType range_scan_type)
        : Instruction(tvm::Instruction::Void),
          _itr(itr),
          _range_itr_type(range_itr_type),
          _range_scan_type(range_scan_type)
    {
        AddSubInstruction(_itr);
    }

    ~SetStateIteratorInstruction() final
    {
        _itr = nullptr;
    }

    uint64_t Exec(tvm::ExecContext* exec_context) final
    {
        MOT::IndexIterator* itr = (MOT::IndexIterator*)_itr->Exec(exec_context);
        int begin_itr = (_range_itr_type == JIT_RANGE_ITERATOR_START) ? 1 : 0;
        int inner_scan = (_range_scan_type == JIT_RANGE_SCAN_INNER) ? 1 : 0;
        setStateIterator(itr, begin_itr, inner_scan);
        return (uint64_t)MOT::RC_OK;
    }

    void Dump() final
    {
        int begin_itr = (_range_itr_type == JIT_RANGE_ITERATOR_START) ? 1 : 0;
        int inner_scan = (_range_scan_type == JIT_RANGE_SCAN_INNER) ? 1 : 0;
        (void)fprintf(stderr, "setStateIterator(");
        _itr->Dump();
        (void)fprintf(stderr, ", begin_itr=%d, inner_scan=%d)", begin_itr, inner_scan);
    }

private:
    tvm::Instruction* _itr;
    JitRangeIteratorType _range_itr_type;
    JitRangeScanType _range_scan_type;
};

/** @class GetStateIteratorInstruction */
class GetStateIteratorInstruction : public tvm::Instruction {
public:
    GetStateIteratorInstruction(JitRangeIteratorType range_itr_type, JitRangeScanType range_scan_type)
        : _range_itr_type(range_itr_type), _range_scan_type(range_scan_type)
    {}

    ~GetStateIteratorInstruction() final
    {}

protected:
    uint64_t ExecImpl(tvm::ExecContext* exec_context) final
    {
        int begin_itr = (_range_itr_type == JIT_RANGE_ITERATOR_START) ? 1 : 0;
        int inner_scan = (_range_scan_type == JIT_RANGE_SCAN_INNER) ? 1 : 0;
        return (uint64_t)getStateIterator(begin_itr, inner_scan);
    }

    void DumpImpl() final
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
class IsStateIteratorNullInstruction : public tvm::Instruction {
public:
    IsStateIteratorNullInstruction(JitRangeIteratorType range_itr_type, JitRangeScanType range_scan_type)
        : _range_itr_type(range_itr_type), _range_scan_type(range_scan_type)
    {}

    ~IsStateIteratorNullInstruction() final
    {}

protected:
    uint64_t ExecImpl(tvm::ExecContext* exec_context) final
    {
        int begin_itr = (_range_itr_type == JIT_RANGE_ITERATOR_START) ? 1 : 0;
        int inner_scan = (_range_scan_type == JIT_RANGE_SCAN_INNER) ? 1 : 0;
        return (uint64_t)isStateIteratorNull(begin_itr, inner_scan);
    }

    void DumpImpl() final
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
class IsStateScanEndInstruction : public tvm::Instruction {
public:
    IsStateScanEndInstruction(JitIndexScanDirection index_scan_direction, JitRangeScanType range_scan_type)
        : _index_scan_direction(index_scan_direction), _range_scan_type(range_scan_type)
    {}

    ~IsStateScanEndInstruction() final
    {}

protected:
    uint64_t ExecImpl(tvm::ExecContext* exec_context) final
    {
        int forward_scan = (_index_scan_direction == JIT_INDEX_SCAN_FORWARD) ? 1 : 0;
        int inner_scan = (_range_scan_type == JIT_RANGE_SCAN_INNER) ? 1 : 0;
        return (uint64_t)isStateScanEnd(forward_scan, inner_scan);
    }

    void DumpImpl() final
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
class GetRowFromStateIteratorInstruction : public tvm::Instruction {
public:
    GetRowFromStateIteratorInstruction(
        MOT::AccessType access_mode, JitIndexScanDirection index_scan_direction, JitRangeScanType range_scan_type)
        : _access_mode(access_mode), _index_scan_direction(index_scan_direction), _range_scan_type(range_scan_type)
    {}

    ~GetRowFromStateIteratorInstruction() final
    {}

protected:
    uint64_t ExecImpl(tvm::ExecContext* exec_context) final
    {
        int forward_scan = (_index_scan_direction == JIT_INDEX_SCAN_FORWARD) ? 1 : 0;
        int inner_scan = (_range_scan_type == JIT_RANGE_SCAN_INNER) ? 1 : 0;
        MOT::Row* row = getRowFromStateIterator(_access_mode, forward_scan, inner_scan);
        return (uint64_t)row;
    }

    void DumpImpl() final
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
class DestroyStateIteratorsInstruction : public tvm::Instruction {
public:
    explicit DestroyStateIteratorsInstruction(JitRangeScanType range_scan_type)
        : Instruction(tvm::Instruction::Void), _range_scan_type(range_scan_type)
    {}

    ~DestroyStateIteratorsInstruction() final
    {}

    uint64_t Exec(tvm::ExecContext* exec_context) final
    {
        int inner_scan = (_range_scan_type == JIT_RANGE_SCAN_INNER) ? 1 : 0;
        destroyStateIterators(inner_scan);
        return (uint64_t)MOT::RC_OK;
    }

    void Dump() final
    {
        int inner_scan = (_range_scan_type == JIT_RANGE_SCAN_INNER) ? 1 : 0;
        (void)fprintf(stderr, "destroyStateIterators(inner_scan=%d)", inner_scan);
    }

private:
    JitRangeScanType _range_scan_type;
};

/** @class SetStateScanEndFlagInstruction */
class SetStateScanEndFlagInstruction : public tvm::Instruction {
public:
    SetStateScanEndFlagInstruction(int scan_ended, JitRangeScanType range_scan_type)
        : Instruction(tvm::Instruction::Void), _scan_ended(scan_ended), _range_scan_type(range_scan_type)
    {}

    ~SetStateScanEndFlagInstruction() final
    {}

    uint64_t Exec(tvm::ExecContext* exec_context) final
    {
        int inner_scan = (_range_scan_type == JIT_RANGE_SCAN_INNER) ? 1 : 0;
        setStateScanEndFlag(_scan_ended, inner_scan);
        return (uint64_t)MOT::RC_OK;
    }

    void Dump() final
    {
        int inner_scan = (_range_scan_type == JIT_RANGE_SCAN_INNER) ? 1 : 0;
        (void)fprintf(stderr, "setStateScanEndFlag(scan_ended=%d, inner_scan=%d)", _scan_ended, inner_scan);
    }

private:
    int _scan_ended;
    JitRangeScanType _range_scan_type;
};

/** @class GetStateScanEndFlagInstruction */
class GetStateScanEndFlagInstruction : public tvm::Instruction {
public:
    explicit GetStateScanEndFlagInstruction(JitRangeScanType range_scan_type) : _range_scan_type(range_scan_type)
    {}

    ~GetStateScanEndFlagInstruction() final
    {}

protected:
    uint64_t ExecImpl(tvm::ExecContext* exec_context) final
    {
        int inner_scan = (_range_scan_type == JIT_RANGE_SCAN_INNER) ? 1 : 0;
        return (uint64_t)getStateScanEndFlag(inner_scan);
    }

    void DumpImpl() final
    {
        int inner_scan = (_range_scan_type == JIT_RANGE_SCAN_INNER) ? 1 : 0;
        (void)fprintf(stderr, "getStateScanEndFlag(inner_scan=%d)", inner_scan);
    }

private:
    JitRangeScanType _range_scan_type;
};

class ResetStateRowInstruction : public tvm::Instruction {
public:
    explicit ResetStateRowInstruction(JitRangeScanType range_scan_type)
        : Instruction(tvm::Instruction::Void), _range_scan_type(range_scan_type)
    {}

    ~ResetStateRowInstruction() final
    {}

    uint64_t Exec(tvm::ExecContext* exec_context) final
    {
        int inner_scan = (_range_scan_type == JIT_RANGE_SCAN_INNER) ? 1 : 0;
        resetStateRow(inner_scan);
        return (uint64_t)MOT::RC_OK;
    }

    void Dump() final
    {
        int inner_scan = (_range_scan_type == JIT_RANGE_SCAN_INNER) ? 1 : 0;
        (void)fprintf(stderr, "resetStateRow(inner_scan=%d)", inner_scan);
    }

private:
    JitRangeScanType _range_scan_type;
};

class SetStateRowInstruction : public tvm::Instruction {
public:
    SetStateRowInstruction(Instruction* row, JitRangeScanType range_scan_type)
        : Instruction(tvm::Instruction::Void), _row(row), _range_scan_type(range_scan_type)
    {
        AddSubInstruction(_row);
    }

    ~SetStateRowInstruction() final
    {
        _row = nullptr;
    }

    uint64_t Exec(tvm::ExecContext* exec_context) final
    {
        int inner_scan = (_range_scan_type == JIT_RANGE_SCAN_INNER) ? 1 : 0;
        MOT::Row* row = (MOT::Row*)_row->Exec(exec_context);
        setStateRow(row, inner_scan);
        return (uint64_t)MOT::RC_OK;
    }

    void Dump() final
    {
        int inner_scan = (_range_scan_type == JIT_RANGE_SCAN_INNER) ? 1 : 0;
        (void)fprintf(stderr, "setStateRow(row=");
        _row->Dump();
        (void)fprintf(stderr, ", inner_scan=%d)", inner_scan);
    }

private:
    Instruction* _row;
    JitRangeScanType _range_scan_type;
};

class GetStateRowInstruction : public tvm::Instruction {
public:
    explicit GetStateRowInstruction(JitRangeScanType range_scan_type) : _range_scan_type(range_scan_type)
    {}

    ~GetStateRowInstruction() final
    {}

protected:
    uint64_t ExecImpl(tvm::ExecContext* exec_context) final
    {
        int inner_scan = (_range_scan_type == JIT_RANGE_SCAN_INNER) ? 1 : 0;
        return (uint64_t)getStateRow(inner_scan);
    }

    void DumpImpl() final
    {
        int inner_scan = (_range_scan_type == JIT_RANGE_SCAN_INNER) ? 1 : 0;
        (void)fprintf(stderr, "getStateRow(inner_scan=%d)", inner_scan);
    }

private:
    JitRangeScanType _range_scan_type;
};

class CopyOuterStateRowInstruction : public tvm::Instruction {
public:
    CopyOuterStateRowInstruction() : Instruction(tvm::Instruction::Void)
    {}

    ~CopyOuterStateRowInstruction() final
    {}

    uint64_t Exec(tvm::ExecContext* exec_context) final
    {
        copyOuterStateRow();
        return (uint64_t)MOT::RC_OK;
    }

    void Dump() final
    {
        (void)fprintf(stderr, "copyOuterStateRow()");
    }
};

class GetOuterStateRowCopyInstruction : public tvm::Instruction {
public:
    GetOuterStateRowCopyInstruction()
    {}

    ~GetOuterStateRowCopyInstruction() final
    {}

protected:
    uint64_t ExecImpl(tvm::ExecContext* exec_context) final
    {
        return (uint64_t)getOuterStateRowCopy();
    }

    void DumpImpl() final
    {
        (void)fprintf(stderr, "getOuterStateRowCopy()");
    }
};

class IsStateRowNullInstruction : public tvm::Instruction {
public:
    explicit IsStateRowNullInstruction(JitRangeScanType range_scan_type) : _range_scan_type(range_scan_type)
    {}

    ~IsStateRowNullInstruction() final
    {}

protected:
    uint64_t ExecImpl(tvm::ExecContext* exec_context) final
    {
        int inner_scan = (_range_scan_type == JIT_RANGE_SCAN_INNER) ? 1 : 0;
        return (uint64_t)isStateRowNull(inner_scan);
    }

    void DumpImpl() final
    {
        int inner_scan = (_range_scan_type == JIT_RANGE_SCAN_INNER) ? 1 : 0;
        (void)fprintf(stderr, "isStateRowNull(inner_scan=%d)", inner_scan);
    }

private:
    JitRangeScanType _range_scan_type;
};

/** @class ResetStateLimitCounterInstruction */
class ResetStateLimitCounterInstruction : public tvm::Instruction {
public:
    ResetStateLimitCounterInstruction() : Instruction(tvm::Instruction::Void)
    {}

    ~ResetStateLimitCounterInstruction() final
    {}

    uint64_t Exec(tvm::ExecContext* exec_context) final
    {
        resetStateLimitCounter();
        return (uint64_t)MOT::RC_OK;
    }

    void Dump() final
    {
        (void)fprintf(stderr, "resetStateLimitCounter()");
    }
};

/** @class IncrementStateLimitCounterInstruction */
class IncrementStateLimitCounterInstruction : public tvm::Instruction {
public:
    IncrementStateLimitCounterInstruction() : Instruction(tvm::Instruction::Void)
    {}

    ~IncrementStateLimitCounterInstruction() final
    {}

    uint64_t Exec(tvm::ExecContext* exec_context) final
    {
        incrementStateLimitCounter();
        return (uint64_t)MOT::RC_OK;
    }

    void Dump() final
    {
        (void)fprintf(stderr, "incrementStateLimitCounter()");
    }
};

/** @class GetStateLimitCounterInstruction */
class GetStateLimitCounterInstruction : public tvm::Instruction {
public:
    GetStateLimitCounterInstruction()
    {}

    ~GetStateLimitCounterInstruction() final
    {}

protected:
    uint64_t ExecImpl(tvm::ExecContext* exec_context) final
    {
        uint64_t value = getStateLimitCounter();
        return (uint64_t)value;
    }

    void DumpImpl() final
    {
        (void)fprintf(stderr, "getStateLimitCounter()");
    }
};

/** @class PrepareAvgArrayInstruction */
class PrepareAvgArrayInstruction : public tvm::Instruction {
public:
    PrepareAvgArrayInstruction(int element_type, int element_count)
        : Instruction(tvm::Instruction::Void), _element_type(element_type), _element_count(element_count)
    {}

    ~PrepareAvgArrayInstruction() final
    {}

    uint64_t Exec(tvm::ExecContext* exec_context) final
    {
        prepareAvgArray(_element_type, _element_count);
        return (uint64_t)MOT::RC_OK;
    }

    void Dump() final
    {
        (void)fprintf(stderr, "prepareAvgArray(element_type=%d, element_count=%d)", _element_type, _element_count);
    }

private:
    int _element_type;
    int _element_count;
};

/** @class LoadAvgArrayExpression */
class LoadAvgArrayExpression : public tvm::Expression {
public:
    LoadAvgArrayExpression() : Expression(tvm::Expression::CannotFail)
    {}

    ~LoadAvgArrayExpression() final
    {}

    Datum eval(tvm::ExecContext* exec_context) final
    {
        return (uint64_t)loadAvgArray();
    }

    void dump() final
    {
        (void)fprintf(stderr, "loadAvgArray()");
    }
};

/** @class SaveAvgArrayInstruction */
class SaveAvgArrayInstruction : public tvm::Instruction {
public:
    explicit SaveAvgArrayInstruction(tvm::Expression* avg_array)
        : Instruction(tvm::Instruction::Void), _avg_array(avg_array)
    {}

    ~SaveAvgArrayInstruction() final
    {
        if (_avg_array != nullptr) {
            delete _avg_array;
        }
    }

    uint64_t Exec(tvm::ExecContext* exec_context) final
    {
        Datum avg_array = _avg_array->eval(exec_context);
        saveAvgArray(avg_array);
        return (uint64_t)MOT::RC_OK;
    }

    void Dump() final
    {
        (void)fprintf(stderr, "saveAvgArray(avg_array=");
        _avg_array->dump();
        (void)fprintf(stderr, ")");
    }

private:
    tvm::Expression* _avg_array;
};

/** @class ComputeAvgFromArrayInstruction */
class ComputeAvgFromArrayInstruction : public tvm::Instruction {
public:
    explicit ComputeAvgFromArrayInstruction(int element_type) : _element_type(element_type)
    {}

    ~ComputeAvgFromArrayInstruction() final
    {}

protected:
    Datum ExecImpl(tvm::ExecContext* exec_context) final
    {
        return (uint64_t)computeAvgFromArray(_element_type);
    }

    void DumpImpl() final
    {
        (void)fprintf(stderr, "computeAvgFromArray(element_type=%d)", _element_type);
    }

private:
    int _element_type;
};

/** @class ResetAggValueInstruction */
class ResetAggValueInstruction : public tvm::Instruction {
public:
    explicit ResetAggValueInstruction(int element_type)
        : Instruction(tvm::Instruction::Void), _element_type(element_type)
    {}

    ~ResetAggValueInstruction() final
    {}

    uint64_t Exec(tvm::ExecContext* exec_context) final
    {
        resetAggValue(_element_type);
        return (uint64_t)MOT::RC_OK;
    }

    void Dump() final
    {
        (void)fprintf(stderr, "resetAggValue(element_type=%d)", _element_type);
    }

private:
    int _element_type;
};

/** @class GetAggValueInstruction */
class GetAggValueExpression : public tvm::Expression {
public:
    GetAggValueExpression() : Expression(tvm::Expression::CannotFail)
    {}

    ~GetAggValueExpression() final
    {}

    Datum eval(tvm::ExecContext* exec_context) final
    {
        return (uint64_t)getAggValue();
    }

    void dump() final
    {
        (void)fprintf(stderr, "getAggValue()");
    }
};

/** @class SetAggValueInstruction */
class SetAggValueInstruction : public tvm::Instruction {
public:
    explicit SetAggValueInstruction(tvm::Expression* value) : Instruction(tvm::Instruction::Void), _value(value)
    {}

    ~SetAggValueInstruction() final
    {
        if (_value != nullptr) {
            delete _value;
        }
    }

    Datum Exec(tvm::ExecContext* exec_context) final
    {
        Datum value = (Datum)_value->eval(exec_context);
        setAggValue(value);
        return (uint64_t)MOT::RC_OK;
    }

    void Dump() final
    {
        (void)fprintf(stderr, "setAggValue(value=");
        _value->dump();
        (void)fprintf(stderr, ")");
    }

private:
    tvm::Expression* _value;
};

/** @class ResetAggMaxMinNullInstruction */
class ResetAggMaxMinNullInstruction : public tvm::Instruction {
public:
    ResetAggMaxMinNullInstruction() : Instruction(tvm::Instruction::Void)
    {}

    ~ResetAggMaxMinNullInstruction() final
    {}

    uint64_t Exec(tvm::ExecContext* exec_context) final
    {
        resetAggMaxMinNull();
        return (uint64_t)MOT::RC_OK;
    }

    void Dump() final
    {
        (void)fprintf(stderr, "resetAggMaxMinNull()");
    }
};

/** @class SetAggMaxMinNotNullInstruction */
class SetAggMaxMinNotNullInstruction : public tvm::Instruction {
public:
    SetAggMaxMinNotNullInstruction() : Instruction(tvm::Instruction::Void)
    {}

    ~SetAggMaxMinNotNullInstruction() final
    {}

    uint64_t Exec(tvm::ExecContext* exec_context) final
    {
        setAggMaxMinNotNull();
        return (uint64_t)MOT::RC_OK;
    }

    void Dump() final
    {
        (void)fprintf(stderr, "setAggMaxMinNotNull()");
    }
};

/** @class GetAggMaxMinIsNullInstruction */
class GetAggMaxMinIsNullInstruction : public tvm::Instruction {
public:
    GetAggMaxMinIsNullInstruction()
    {}

    ~GetAggMaxMinIsNullInstruction() final
    {}

protected:
    uint64_t ExecImpl(tvm::ExecContext* exec_context) final
    {
        return (uint64_t)getAggMaxMinIsNull();
    }

    void DumpImpl() final
    {
        (void)fprintf(stderr, "getAggMaxMinIsNull()");
    }
};

/** @class PrepareDistinctSetInstruction */
class PrepareDistinctSetInstruction : public tvm::Instruction {
public:
    explicit PrepareDistinctSetInstruction(int element_type)
        : Instruction(tvm::Instruction::Void), _element_type(element_type)
    {}

    ~PrepareDistinctSetInstruction() final
    {}

    uint64_t Exec(tvm::ExecContext* exec_context) final
    {
        prepareDistinctSet(_element_type);
        return (uint64_t)MOT::RC_OK;
    }

    void Dump() final
    {
        (void)fprintf(stderr, "prepareDistinctSet(element_type=%d)", _element_type);
    }

private:
    int _element_type;
};

/** @class InsertDistinctItemInstruction */
class InsertDistinctItemInstruction : public tvm::Instruction {
public:
    InsertDistinctItemInstruction(int element_type, tvm::Expression* value) : _element_type(element_type), _value(value)
    {}

    ~InsertDistinctItemInstruction() final
    {
        _value = nullptr;
    }

protected:
    uint64_t ExecImpl(tvm::ExecContext* exec_context) final
    {
        Datum value = (Datum)_value->eval(exec_context);
        return (uint64_t)insertDistinctItem(_element_type, value);
    }

    void DumpImpl() final
    {
        (void)fprintf(stderr, "insertDistinctItem(element_type=%d, ", _element_type);
        _value->dump();
        (void)fprintf(stderr, ")");
    }

private:
    int _element_type;
    tvm::Expression* _value;
};

/** @class DestroyDistinctSetInstruction */
class DestroyDistinctSetInstruction : public tvm::Instruction {
public:
    explicit DestroyDistinctSetInstruction(int element_type)
        : Instruction(tvm::Instruction::Void), _element_type(element_type)
    {}

    ~DestroyDistinctSetInstruction() final
    {}

    uint64_t Exec(tvm::ExecContext* exec_context) final
    {
        destroyDistinctSet(_element_type);
        return (uint64_t)MOT::RC_OK;
    }

    void Dump() final
    {
        (void)fprintf(stderr, "destroyDistinctSet(element_type=%d)", _element_type);
    }

private:
    int _element_type;
};

/** @class ResetTupleDatumInstruction */
class ResetTupleDatumInstruction : public tvm::Instruction {
public:
    ResetTupleDatumInstruction(int tuple_colid, int zero_type)
        : Instruction(tvm::Instruction::Void), _tuple_colid(tuple_colid), _zero_type(zero_type)
    {}

    ~ResetTupleDatumInstruction() final
    {}

    uint64_t Exec(tvm::ExecContext* exec_context) final
    {
        resetTupleDatum(exec_context->_slot, _tuple_colid, _zero_type);
        return (uint64_t)MOT::RC_OK;
    }

    void Dump() final
    {
        (void)fprintf(stderr, "resetTupleDatum(%%slot, tuple_colid=%d, zero_type=%d)", _tuple_colid, _zero_type);
    }

private:
    int _tuple_colid;
    int _zero_type;
};

/** @class ReadTupleDatumExpression */
class ReadTupleDatumExpression : public tvm::Expression {
public:
    ReadTupleDatumExpression(int tuple_colid, int arg_pos)
        : Expression(tvm::Expression::CannotFail), _tuple_colid(tuple_colid), _arg_pos(arg_pos)
    {}

    ~ReadTupleDatumExpression() final
    {}

    Datum eval(tvm::ExecContext* exec_context) final
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
class WriteTupleDatumInstruction : public tvm::Instruction {
public:
    WriteTupleDatumInstruction(int tuple_colid, tvm::Instruction* value)
        : Instruction(tvm::Instruction::Void), _tuple_colid(tuple_colid), _value(value)
    {
        AddSubInstruction(_value);
    }

    ~WriteTupleDatumInstruction() final
    {
        _value = nullptr;
    }

    uint64_t Exec(tvm::ExecContext* execContext) final
    {
        Datum datum_value = (Datum)_value->Exec(execContext);
        writeTupleDatum(execContext->_slot, _tuple_colid, datum_value);
        return (uint64_t)MOT::RC_OK;
    }

    void Dump() final
    {
        (void)fprintf(stderr, "writeTupleDatum(%%slot, tuple_colid=%d, ", _tuple_colid);
        _value->Dump();
        (void)fprintf(stderr, ")");
    }

private:
    int _tuple_colid;
    tvm::Instruction* _value;
};

class SelectSubQueryResultExpression : public tvm::Expression {
public:
    explicit SelectSubQueryResultExpression(int subQueryIndex)
        : Expression(tvm::Expression::CannotFail), m_subQueryIndex(subQueryIndex)
    {}

    ~SelectSubQueryResultExpression() final
    {}

    Datum eval(tvm::ExecContext* exec_context) final
    {
        exec_context->_expr_rc = MOT_NO_ERROR;
        return (uint64_t)SelectSubQueryResult(m_subQueryIndex);
    }

    void dump() final
    {
        (void)fprintf(stderr, "SelectSubQueryResult(%%sub_query_index=%d)", m_subQueryIndex);
    }

private:
    int m_subQueryIndex;
};

class CopyAggregateToSubQueryResultInstruction : public tvm::Instruction {
public:
    explicit CopyAggregateToSubQueryResultInstruction(int subQueryIndex)
        : Instruction(tvm::Instruction::Void), m_subQueryIndex(subQueryIndex)
    {}

    ~CopyAggregateToSubQueryResultInstruction() final
    {}

    uint64_t Exec(tvm::ExecContext* exec_context) final
    {
        CopyAggregateToSubQueryResult(m_subQueryIndex);
        return (uint64_t)MOT::RC_OK;
    }

    void Dump() final
    {
        (void)fprintf(stderr, "CopyAggregateToSubQueryResult(%%sub_query_index=%d)", m_subQueryIndex);
    }

private:
    int m_subQueryIndex;
};

/** @class GetConstAtExpression */
class GetConstAtExpression : public tvm::Expression {
public:
    explicit GetConstAtExpression(int constId, int argPos)
        : Expression(tvm::Expression::CanFail), m_constId(constId), m_argPos(argPos)
    {}

    ~GetConstAtExpression() final
    {}

    Datum eval(tvm::ExecContext* exec_context) final
    {
        return (uint64_t)GetConstAt(m_constId, m_argPos);
    }

    void dump() final
    {
        (void)fprintf(stderr, "GetConstAt(constId=%d, argPos=%d)", m_constId, m_argPos);
    }

private:
    int m_constId;
    int m_argPos;
};

inline tvm::Instruction* AddIsSoftMemoryLimitReached(JitTvmCodeGenContext* ctx)
{
    return ctx->_builder->addInstruction(new (std::nothrow) IsSoftMemoryLimitReachedInstruction());
}

inline void AddInitSearchKey(JitTvmCodeGenContext* ctx, JitRangeScanType rangeScanType, int subQueryIndex)
{
    (void)ctx->_builder->addInstruction(new (std::nothrow) InitSearchKeyInstruction(rangeScanType, subQueryIndex));
}

inline tvm::Instruction* AddGetColumnAt(
    JitTvmCodeGenContext* ctx, int colid, JitRangeScanType range_scan_type, int subQueryIndex = -1)
{
    return ctx->_builder->addInstruction(
        new (std::nothrow) GetColumnAtInstruction(colid, range_scan_type, subQueryIndex));
}

inline tvm::Instruction* AddSetExprArgIsNull(JitTvmCodeGenContext* ctx, int arg_pos, int isnull)
{
    return ctx->_builder->addInstruction(new (std::nothrow) SetExprArgIsNullInstruction(arg_pos, isnull));
}

inline tvm::Instruction* AddGetExprArgIsNull(JitTvmCodeGenContext* ctx, int arg_pos)
{
    return ctx->_builder->addInstruction(new (std::nothrow) GetExprArgIsNullInstruction(arg_pos));
}

inline tvm::Expression* AddGetDatumParam(JitTvmCodeGenContext* ctx, int paramid, int arg_pos)
{
    return new (std::nothrow) tvm::ParamExpression(paramid, arg_pos);
}

inline tvm::Expression* AddReadDatumColumn(
    JitTvmCodeGenContext* ctx, MOT::Table* table, tvm::Instruction* row_inst, int table_colid, int arg_pos)
{
    return new (std::nothrow) VarExpression(table, row_inst, table_colid, arg_pos);
}

inline void AddWriteDatumColumn(
    JitTvmCodeGenContext* ctx, int table_colid, tvm::Instruction* row_inst, tvm::Instruction* sub_expr)
{
    // make sure we have a column before issuing the call
    // we always write to a main table row (whether UPDATE or range UPDATE, so inner_scan value below is false)
    tvm::Instruction* column = AddGetColumnAt(ctx, table_colid, JIT_RANGE_SCAN_MAIN);
    ctx->_builder->addInstruction(new (std::nothrow) WriteDatumColumnInstruction(row_inst, column, sub_expr));
}

inline void AddBuildDatumKey(JitTvmCodeGenContext* ctx, tvm::Instruction* column_inst, int index_colid,
    tvm::Instruction* sub_expr, int value_type, JitRangeIteratorType range_itr_type, JitRangeScanType range_scan_type,
    int subQueryIndex = -1)
{
    int offset = -1;
    int size = -1;
    if (range_scan_type == JIT_RANGE_SCAN_INNER) {
        offset = ctx->m_innerTable_info.m_indexColumnOffsets[index_colid];
        size = ctx->m_innerTable_info.m_index->GetLengthKeyFields()[index_colid];
    } else if (range_scan_type == JIT_RANGE_SCAN_MAIN) {
        offset = ctx->_table_info.m_indexColumnOffsets[index_colid];
        size = ctx->_table_info.m_index->GetLengthKeyFields()[index_colid];
    } else if (range_scan_type == JIT_RANGE_SCAN_SUB_QUERY) {
        offset = ctx->m_subQueryTableInfo[subQueryIndex].m_indexColumnOffsets[index_colid];
        size = ctx->m_subQueryTableInfo[subQueryIndex].m_index->GetLengthKeyFields()[index_colid];
    }
    (void)ctx->_builder->addInstruction(new (std::nothrow) BuildDatumKeyInstruction(
        column_inst, sub_expr, index_colid, offset, size, value_type, range_itr_type, range_scan_type, subQueryIndex));
}

inline void AddSetBit(JitTvmCodeGenContext* ctx, int colid)
{
    (void)ctx->_builder->addInstruction(new (std::nothrow) SetBitInstruction(colid));
}

inline void AddResetBitmapSet(JitTvmCodeGenContext* ctx)
{
    (void)ctx->_builder->addInstruction(new (std::nothrow) ResetBitmapSetInstruction());
}

inline tvm::Instruction* AddWriteRow(JitTvmCodeGenContext* ctx, tvm::Instruction* row_inst)
{
    return ctx->_builder->addInstruction(new (std::nothrow) WriteRowInstruction(row_inst));
}

inline tvm::Instruction* AddSearchRow(
    JitTvmCodeGenContext* ctx, MOT::AccessType access_mode_value, JitRangeScanType range_scan_type, int subQueryIndex)
{
    return ctx->_builder->addInstruction(
        new (std::nothrow) SearchRowInstruction(access_mode_value, range_scan_type, subQueryIndex));
}

inline tvm::Instruction* AddCreateNewRow(JitTvmCodeGenContext* ctx)
{
    return ctx->_builder->addInstruction(new (std::nothrow) CreateNewRowInstruction());
}

inline tvm::Instruction* AddInsertRow(JitTvmCodeGenContext* ctx, tvm::Instruction* row_inst)
{
    return ctx->_builder->addInstruction(new (std::nothrow) InsertRowInstruction(row_inst));
}

inline tvm::Instruction* AddDeleteRow(JitTvmCodeGenContext* ctx)
{
    return ctx->_builder->addInstruction(new (std::nothrow) DeleteRowInstruction());
}

inline void AddSetRowNullBits(JitTvmCodeGenContext* ctx, tvm::Instruction* row_inst)
{
    (void)ctx->_builder->addInstruction(new (std::nothrow) SetRowNullBitsInstruction(row_inst));
}

inline tvm::Instruction* AddSetExprResultNullBit(JitTvmCodeGenContext* ctx, tvm::Instruction* row_inst, int colid)
{
    return ctx->_builder->addInstruction(new (std::nothrow) SetExprResultNullBitInstruction(row_inst, colid));
}

inline void AddExecClearTuple(JitTvmCodeGenContext* ctx)
{
    ctx->_builder->addInstruction(new (std::nothrow) ExecClearTupleInstruction());
}

inline void AddExecStoreVirtualTuple(JitTvmCodeGenContext* ctx)
{
    ctx->_builder->addInstruction(new (std::nothrow) ExecStoreVirtualTupleInstruction());
}

inline void AddSelectColumn(JitTvmCodeGenContext* ctx, tvm::Instruction* rowInst, int tableColumnId, int tupleColumnId,
    JitRangeScanType rangeScanType, int subQueryIndex)
{
    ctx->_builder->addInstruction(new (std::nothrow)
            SelectColumnInstruction(rowInst, tableColumnId, tupleColumnId, rangeScanType, subQueryIndex));
}

inline void AddSetTpProcessed(JitTvmCodeGenContext* ctx)
{
    (void)ctx->_builder->addInstruction(new (std::nothrow) SetTpProcessedInstruction());
}

inline void AddSetScanEnded(JitTvmCodeGenContext* ctx, int result)
{
    ctx->_builder->addInstruction(new (std::nothrow) SetScanEndedInstruction(result));
}

inline void AddCopyKey(JitTvmCodeGenContext* ctx, JitRangeScanType range_scan_type, int subQueryIndex)
{
    ctx->_builder->addInstruction(new (std::nothrow) CopyKeyInstruction(range_scan_type, subQueryIndex));
}

inline void AddFillKeyPattern(JitTvmCodeGenContext* ctx, unsigned char pattern, int offset, int size,
    JitRangeIteratorType rangeItrType, JitRangeScanType rangeScanType, int subQueryIndex)
{
    ctx->_builder->addInstruction(new (std::nothrow)
            FillKeyPatternInstruction(rangeItrType, pattern, offset, size, rangeScanType, subQueryIndex));
}

inline void AddAdjustKey(JitTvmCodeGenContext* ctx, unsigned char pattern, JitRangeIteratorType range_itr_type,
    JitRangeScanType range_scan_type, int subQueryIndex)
{
    (void)ctx->_builder->addInstruction(
        new (std::nothrow) AdjustKeyInstruction(range_itr_type, pattern, range_scan_type, subQueryIndex));
}

inline tvm::Instruction* AddSearchIterator(JitTvmCodeGenContext* ctx, JitIndexScanDirection index_scan_direction,
    JitRangeBoundMode range_bound_mode, JitRangeScanType range_scan_type, int subQueryIndex)
{
    return ctx->_builder->addInstruction(new (std::nothrow)
            SearchIteratorInstruction(index_scan_direction, range_bound_mode, range_scan_type, subQueryIndex));
}

inline tvm::Instruction* AddBeginIterator(JitTvmCodeGenContext* ctx, JitRangeScanType rangeScanType, int subQueryIndex)
{
    return ctx->_builder->addInstruction(new (std::nothrow) BeginIteratorInstruction(rangeScanType, subQueryIndex));
}

inline tvm::Instruction* AddCreateEndIterator(JitTvmCodeGenContext* ctx, JitIndexScanDirection index_scan_direction,
    JitRangeBoundMode range_bound_mode, JitRangeScanType range_scan_type, int subQueryIndex = -1)
{
    return ctx->_builder->addInstruction(new (std::nothrow)
            CreateEndIteratorInstruction(index_scan_direction, range_bound_mode, range_scan_type, subQueryIndex));
}

inline tvm::Instruction* AddIsScanEnd(JitTvmCodeGenContext* ctx, JitIndexScanDirection index_scan_direction,
    JitTvmRuntimeCursor* cursor, JitRangeScanType range_scan_type, int subQueryIndex = -1)
{
    return ctx->_builder->addInstruction(new (std::nothrow) IsScanEndInstruction(
        index_scan_direction, cursor->begin_itr, cursor->end_itr, range_scan_type, subQueryIndex));
}

inline tvm::Instruction* AddGetRowFromIterator(JitTvmCodeGenContext* ctx, MOT::AccessType access_mode,
    JitIndexScanDirection index_scan_direction, JitTvmRuntimeCursor* cursor, JitRangeScanType range_scan_type,
    int subQueryIndex)
{
    return ctx->_builder->addInstruction(new (std::nothrow) GetRowFromIteratorInstruction(
        access_mode, index_scan_direction, cursor->begin_itr, cursor->end_itr, range_scan_type, subQueryIndex));
}

inline void AddDestroyIterator(JitTvmCodeGenContext* ctx, tvm::Instruction* itr_inst)
{
    (void)ctx->_builder->addInstruction(new (std::nothrow) DestroyIteratorInstruction(itr_inst));
}

inline void AddDestroyCursor(JitTvmCodeGenContext* ctx, JitTvmRuntimeCursor* cursor)
{
    if (cursor != nullptr) {
        AddDestroyIterator(ctx, cursor->begin_itr);
        AddDestroyIterator(ctx, cursor->end_itr);
    }
}

/** @brief Adds a call to pseudo-function isNewScan(begin_itr). */
inline tvm::Instruction* AddIsNewScan(JitTvmCodeGenContext* ctx)
{
    return ctx->_builder->addInstruction(new (std::nothrow) IsNewScanInstruction());
}

/** @brief Adds a call to setStateIterator(itr, begin_itr). */
inline void AddSetStateIterator(JitTvmCodeGenContext* ctx, tvm::Instruction* itr, JitRangeIteratorType range_itr_type,
    JitRangeScanType range_scan_type)
{
    (void)ctx->_builder->addInstruction(
        new (std::nothrow) SetStateIteratorInstruction(itr, range_itr_type, range_scan_type));
}

/** @brief Adds a call to isStateIteratorNull(begin_itr). */
inline tvm::Instruction* AddIsStateIteratorNull(
    JitTvmCodeGenContext* ctx, JitRangeIteratorType range_itr_type, JitRangeScanType range_scan_type)
{
    return ctx->_builder->addInstruction(
        new (std::nothrow) IsStateIteratorNullInstruction(range_itr_type, range_scan_type));
}

/** @brief Adds a call to isStateScanEnd(index, forward_scan). */
inline tvm::Instruction* AddIsStateScanEnd(
    JitTvmCodeGenContext* ctx, JitIndexScanDirection index_scan_direction, JitRangeScanType range_scan_type)
{
    return ctx->_builder->addInstruction(
        new (std::nothrow) IsStateScanEndInstruction(index_scan_direction, range_scan_type));
}

inline tvm::Instruction* AddGetRowFromStateIterator(JitTvmCodeGenContext* ctx, MOT::AccessType access_mode,
    JitIndexScanDirection index_scan_direction, JitRangeScanType range_scan_type)
{
    return ctx->_builder->addInstruction(
        new (std::nothrow) GetRowFromStateIteratorInstruction(access_mode, index_scan_direction, range_scan_type));
}

/** @brief Adds a call to setStateScanEndFlag(scan_ended). */
inline void AddSetStateScanEndFlag(JitTvmCodeGenContext* ctx, int scan_ended, JitRangeScanType range_scan_type)
{
    (void)ctx->_builder->addInstruction(new (std::nothrow) SetStateScanEndFlagInstruction(scan_ended, range_scan_type));
}

/** @brief Adds a call to getStateScanEndFlag(). */
inline tvm::Instruction* AddGetStateScanEndFlag(JitTvmCodeGenContext* ctx, JitRangeScanType range_scan_type)
{
    return ctx->_builder->addInstruction(new (std::nothrow) GetStateScanEndFlagInstruction(range_scan_type));
}

inline void AddResetStateRow(JitTvmCodeGenContext* ctx, JitRangeScanType range_scan_type)
{
    (void)ctx->_builder->addInstruction(new (std::nothrow) ResetStateRowInstruction(range_scan_type));
}

inline void AddSetStateRow(JitTvmCodeGenContext* ctx, tvm::Instruction* row, JitRangeScanType range_scan_type)
{
    (void)ctx->_builder->addInstruction(new (std::nothrow) SetStateRowInstruction(row, range_scan_type));
}

inline tvm::Instruction* AddGetStateRow(JitTvmCodeGenContext* ctx, JitRangeScanType range_scan_type)
{
    return ctx->_builder->addInstruction(new (std::nothrow) GetStateRowInstruction(range_scan_type));
}

inline void AddCopyOuterStateRow(JitTvmCodeGenContext* ctx)
{
    (void)ctx->_builder->addInstruction(new (std::nothrow) CopyOuterStateRowInstruction());
}

inline tvm::Instruction* AddGetOuterStateRowCopy(JitTvmCodeGenContext* ctx)
{
    return ctx->_builder->addInstruction(new (std::nothrow) GetOuterStateRowCopyInstruction());
}

inline tvm::Instruction* AddIsStateRowNull(JitTvmCodeGenContext* ctx, JitRangeScanType range_scan_type)
{
    return ctx->_builder->addInstruction(new (std::nothrow) IsStateRowNullInstruction(range_scan_type));
}

/** @brief Adds a call to destroyStateIterators(). */
inline void AddDestroyStateIterators(JitTvmCodeGenContext* ctx, JitRangeScanType range_scan_type)
{
    ctx->_builder->addInstruction(new (std::nothrow) DestroyStateIteratorsInstruction(range_scan_type));
}

inline void AddResetStateLimitCounter(JitTvmCodeGenContext* ctx)
{
    ctx->_builder->addInstruction(new (std::nothrow) ResetStateLimitCounterInstruction());
}

inline void AddIncrementStateLimitCounter(JitTvmCodeGenContext* ctx)
{
    ctx->_builder->addInstruction(new (std::nothrow) IncrementStateLimitCounterInstruction());
}

inline tvm::Instruction* AddGetStateLimitCounter(JitTvmCodeGenContext* ctx)
{
    return ctx->_builder->addInstruction(new (std::nothrow) GetStateLimitCounterInstruction());
}

inline void AddPrepareAvgArray(JitTvmCodeGenContext* ctx, int element_type, int element_count)
{
    (void)ctx->_builder->addInstruction(new (std::nothrow) PrepareAvgArrayInstruction(element_type, element_count));
}

inline tvm::Expression* AddLoadAvgArray(JitTvmCodeGenContext* ctx)
{
    return new (std::nothrow) LoadAvgArrayExpression();
}

inline void AddSaveAvgArray(JitTvmCodeGenContext* ctx, tvm::Expression* avg_array)
{
    (void)ctx->_builder->addInstruction(new (std::nothrow) SaveAvgArrayInstruction(avg_array));
}

inline tvm::Instruction* AddComputeAvgFromArray(JitTvmCodeGenContext* ctx, int element_type)
{
    return ctx->_builder->addInstruction(new (std::nothrow) ComputeAvgFromArrayInstruction(element_type));
}

inline void AddResetAggValue(JitTvmCodeGenContext* ctx, int element_type)
{
    (void)ctx->_builder->addInstruction(new (std::nothrow) ResetAggValueInstruction(element_type));
}

inline tvm::Expression* AddGetAggValue(JitTvmCodeGenContext* ctx)
{
    return new (std::nothrow) GetAggValueExpression();
}

inline void AddSetAggValue(JitTvmCodeGenContext* ctx, tvm::Expression* value)
{
    (void)ctx->_builder->addInstruction(new (std::nothrow) SetAggValueInstruction(value));
}

inline void AddResetAggMaxMinNull(JitTvmCodeGenContext* ctx)
{
    (void)ctx->_builder->addInstruction(new (std::nothrow) ResetAggMaxMinNullInstruction());
}

inline void AddSetAggMaxMinNotNull(JitTvmCodeGenContext* ctx)
{
    (void)ctx->_builder->addInstruction(new (std::nothrow) SetAggMaxMinNotNullInstruction());
}

inline tvm::Instruction* AddGetAggMaxMinIsNull(JitTvmCodeGenContext* ctx)
{
    return ctx->_builder->addInstruction(new (std::nothrow) GetAggMaxMinIsNullInstruction());
}

inline void AddPrepareDistinctSet(JitTvmCodeGenContext* ctx, int element_type)
{
    (void)ctx->_builder->addInstruction(new (std::nothrow) PrepareDistinctSetInstruction(element_type));
}

inline tvm::Instruction* AddInsertDistinctItem(JitTvmCodeGenContext* ctx, int element_type, tvm::Expression* value)
{
    return ctx->_builder->addInstruction(new (std::nothrow) InsertDistinctItemInstruction(element_type, value));
}

inline void AddDestroyDistinctSet(JitTvmCodeGenContext* ctx, int element_type)
{
    (void)ctx->_builder->addInstruction(new (std::nothrow) DestroyDistinctSetInstruction(element_type));
}

inline void AddWriteTupleDatum(JitTvmCodeGenContext* ctx, int tuple_colid, tvm::Instruction* datum_value)
{
    (void)ctx->_builder->addInstruction(new (std::nothrow) WriteTupleDatumInstruction(tuple_colid, datum_value));
}

inline tvm::Expression* AddSelectSubQueryResult(JitTvmCodeGenContext* ctx, int subQueryIndex)
{
    return new (std::nothrow) SelectSubQueryResultExpression(subQueryIndex);
}

inline void AddCopyAggregateToSubQueryResult(JitTvmCodeGenContext* ctx, int subQueryIndex)
{
    ctx->_builder->addInstruction(new (std::nothrow) CopyAggregateToSubQueryResultInstruction(subQueryIndex));
}

inline tvm::Expression* AddGetConstAt(JitTvmCodeGenContext* ctx, int constId, int argPos)
{
    return new (std::nothrow) GetConstAtExpression(constId, argPos);
}

#ifdef MOT_JIT_DEBUG
inline void IssueDebugLogImpl(JitTvmCodeGenContext* ctx, const char* function, const char* msg)
{
    (void)ctx->_builder->addInstruction(new (std::nothrow) tvm::DebugLogInstruction(function, msg));
}
#endif

#ifdef MOT_JIT_DEBUG
#define IssueDebugLog(msg) IssueDebugLogImpl(ctx, __func__, msg)
#else
#define IssueDebugLog(msg)
#endif
}  // namespace JitExec

#endif /* JIT_TVM_FUNCS_H */
