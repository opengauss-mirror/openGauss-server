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
 * codegendebuger.h
 * 
 * 
 * 
 * IDENTIFICATION
 *        src/include/codegen/codegendebuger.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef CODEGEN_DEBUGER_H
#define CODEGEN_DEBUGER_H

#include "codegen/gscodegen.h"

namespace dorado {
class DebugerCodeGen : public BaseObject {
public:
    /*
     * Brief        : Wrap an elog function to support gdb functionability.
     * Description  : Support debuging in machine code by calling c-function
     *				  WrapCodeGenDebuger.
     * Input        : ptrbuilder : in which builder we call this function
     *				  value : the actual value when running.
     *				  flag : mask the position to distinct diffrent cases
     * Output       : None.
     * Return Value : None.
     * Notes        : None.
     */
    static void CodeGenDebugInfo(GsCodeGen::LlvmBuilder* ptrbuilder, llvm::Value* value, Datum flag);

    static void CodeGenDebugString(GsCodeGen::LlvmBuilder* ptrbuilder, llvm::Value* value, Datum flag);

    /*
     * Brief        : Wrap an elog function to support gdb functionability.
     * Description  : Support debuging in machine code by calling c-function
     *				  WrapElog.
     * Input        : ptrbuilder : in which builder we call this function
     *				  eleval : reporting level.
     *				  cvalue : the actual error string defined in LLVM function.
     * Output       : None.
     * Return Value : None.
     * Notes        : None.
     */
    static void CodeGenElogInfo(GsCodeGen::LlvmBuilder* ptrbuilder, Datum elevel, const char* cvalue);

    /*
     * Brief        : Wrap an elog function to support gdb functionability.
     * Description  : Wrap the elog function, with which we could easily
     *				  see the actual value when debugging.
     * Input        : value : the actual value when running.
     *				  flag : mask the position to distinct diffrent cases
     * Output       : None.
     * Return Value : None.
     * Notes        : None.
     */
    static void WrapCodeGenDebuger(Datum value, Datum flag);

    static void WrapCodeGenString(char* string, Datum flag);

    /*
     * Brief        : Wrap an elog function with different error level.
     * Description  : Wrap the elog function, which will be call in LLVM
     *				  to support error reporting.
     * Input        : elevel : reporting level.
     *				  strdate : the actual error string.
     * Output       : None.
     * Return Value : None.
     * Notes        : None.
     */
    static void WrapCodeGenElog(Datum elevel, char* strdata);
};
}  // namespace dorado

#endif
