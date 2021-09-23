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
 * foreignscancodegen.h
 * 
 * 
 * 
 * IDENTIFICATION
 *        src/include/codegen/foreignscancodegen.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef LLVM_FOREIGN_SCAN_H
#define LLVM_FOREIGN_SCAN_H

#include "codegen/gscodegen.h"
#include "optimizer/clauses.h"
#include "nodes/execnodes.h"

namespace dorado {

/*
 * LlvmForeignScan class implements specific optimization by using LLVM.
 * Just now, pushed down predicate is optimized by using LLVM.
 */
class ForeignScanCodeGen : public BaseObject {
public:
    static bool ScanCodeGen(Expr* expr, PlanState* parent, void** jittedFunc);

    /*
     * Brief        : Check whether operator is legal or not.
     * Description  : Just now, the follwing operator is legal.
     *                "=", "!=", ">", ">=", "<", "<=".
     *                These operator must act on the following data types.
     *                int2, int4, int8, float4, float8.
     * Input        : expr, the node to be checked.
     * Output       : None.
     * Return Value : None.
     * Notes        : None.
     */
    static bool IsJittableExpr(Expr* expr);

    /*
     * Brief        : Build LLVM value by using node.
     * Description  :
     * Input        : node, the node to be used to create IR.
     *                llvm_args, the argument of IR functionto to be created.
     *                builder, a LlvmBuilder object.
     *                fname, function name.
     * Output       : None.
     * Return Value : Retrun llvm::Value.
     * Notes        : None.
     */
    static llvm::Value* buildConstValue(
        Expr* node, llvm::Value* llvm_args[], GsCodeGen::LlvmBuilder& builder, StringInfo fname);
};
}  // namespace dorado
#endif
