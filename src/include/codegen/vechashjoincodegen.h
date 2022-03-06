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
 * vechashjoincodegen.h
 *        Declarations of code generation for vechashjoin funtions.
 * 
 * Since the most cpu intensive part of hashjoin function locates
 * at build hash table part and join part, we mainly do codegeneration
 * for buildHashTable, innerJoin and so on.
 * 
 * IDENTIFICATION
 *        src/include/codegen/vechashjoincodegen.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef LLVM_VECHASHJOIN_H
#define LLVM_VECHASHJOIN_H
#include "codegen/gscodegen.h"
#include "codegen/vecexprcodegen.h"
#include "vecexecutor/vechashjoin.h"
#include "vecexecutor/vechashtable.h"
#include "nodes/execnodes.h"
#include "vecexecutor/vecnodes.h"

namespace dorado {
/*
 * VecHashJoinCodeGen class implements specific optimization by using LLVM
 */
#ifdef ENABLE_LLVM_COMPILE
class VecHashJoinCodeGen : public BaseObject {
public:
    /*
     * @Brief		: Check if the hash key is simple var or not.
     * @Description	: Since we only support simple hash condition now,
     *				  the hash key expr should be simple var. If this
     *				  var is marked as not_null, we could use fast
     *				  key match path.
     * @in node		: VecHashJoinState, the node to be checked.
     * @in estate	: The expr of hashkey.
     * @in is_simple_var : Flag used to mark if hash cond expr is simple var
     *				  or not.
     * @in enable_fast_keymatch	: Flag used to mark fast codegen path.
     * @return		: Return simple var expression.
     */
    static Var* GetSimpHashCondExpr(
        VecHashJoinState* node, ExprState* estate, bool* is_simple_var, int* enable_fast_keyMatch);

    /*
     * @Brief		: Check the validation of VecHashJoinState.
     * @Description	: Check if the current hash join node could be codegened
     *				  or not. We only support hash inner join. The
     *				  data type of hash keys could only be int4, int8 or bpchar.
     * @in node		: VecHashJoinState, the node to be checked.
     * @return		: Return true if current hash join node could be codegened.
     *				  set VecHashJoinState->enable_fast_keyMatch = 2 if
     * 				  (1) all hash keys are NOT NULL columns from base tables
     *				  (2) all hash key data types are int4 or int8
     *				  else set VecHashJoinState->enable_fast_keyMatch = 1 if
     *				  (1) there is only one hash clause
     *				  else set VecHashJoinState->enable_fast_keyMatch = 0
     */
    static bool JittableHashJoin(VecHashJoinState* node);

    /* @Brief		: Check if we can do codegen on buildHashTable and probeHashTable
     * @Description	: We separate this check from the JittableHashJoin for
     *				  innerJoinT/matchKey since the build/probe function can
     *				  be used for all joins (inner, semi, anti, ...).
     * @in			: VecHashJoinState, the node to be checked.
     * @return		: true if we can do codegen on buildHashTable/probeHashTable
     *				  (1) No any complicate hash clauses
     *				  (2) only support hash keys with int4,int8,bpchar type.
     */
    static bool JittableHashJoin_buildandprobe(VecHashJoinState* node);

    /*
     * @Brief		: Check if we can do codegen on bloom filter function.
     * @Description	: During initialization, we initilized the runtime bloomfilter.
     *				  If the var type is int2, int4 or int8, we chould use
     *				  codegen optimization for bloom filter function.
     * @in			: VecHashJoinState, the node to be checked.
     * @return		: return true if we can do codegen on addLong and includeLong
     *				  function in bloom filter.
     */
    static bool JittableHashJoin_bloomfilter(VecHashJoinState* node);

    /*
     * @Brief		: Code generation for hash join
     * @Description	: Codegen for the hot path in innerJoinT
     *				  match_key, buildHashTable, probeHashTable
     * @in node		: VecHashJoinState, the node to be codegened
     * @return		: true if innerJoinT is generated; otherwise, false.
     */
    static void HashJoinCodeGen(VecHashJoinState* node);
    static llvm::Function* HashJoinCodeGen_normal(VecHashJoinState* node);

    /*
     * @Description	: Generate more efficient code with less memory access
     * 				  and less branches.
     * @in node		: Hash join state node.
     * @return		: LLVM function about specialized hash inner join.
     * @Note		: The condition is one hash clause, hash keys are NOT NULL,
     * 				  and hash keys are integer type.
     */
    static llvm::Function* HashJoinCodeGen_fastpath(VecHashJoinState* node);

    /*
     * @Brief		: Code generation for keyMatch function in hash join
     * @Description	: Codegen for the whole function keyMatch for hash join
     * @in node		: VecHashJoinState, the node to be codegened
     * @return		: The LLVM IR function of keyMatch for hash join
     */
    static llvm::Function* KeyMatchCodeGen(VecHashJoinState* node, Var* variable);

    /*
     * @Brief		: Code generation for buildHashTable function in hash join
     * @Description	: The hash function (hashint4 + hash_uint32) is replaced by
     *				  CRC32 hardware instruction. Need to make sure the
     *				  probeHashTable use the same hash function.
     * @in node		: VecHashJoinState, the node to be codegened
     * @return		: The LLVM IR function of buildHashTable for hash join
     */
    static llvm::Function* HashJoinCodeGen_buildHashTable(VecHashJoinState* node);

    /*
     * @Description	: Similiar as above function except NeedCopy is true.
     * @Note		: NeedCopy is true when we have partition table.
     */
    static llvm::Function* HashJoinCodeGen_buildHashTable_NeedCopy(VecHashJoinState* node);

    /*
     * @Brief		: Code generation for probeHashTable function in hash join
     * @Description	: The hash function (hashint4 + hash_uint32) is replaced by
     *				  CRC32 hardware instruction. Need to make sure the
     *				  buildHashTable use the same hash function.
     * @in node		: VecHashJoinState, the node to be codegened
     * @return		: The LLVM IR function of probeHashTable for hash join
     */
    static llvm::Function* HashJoinCodeGen_probeHashTable(VecHashJoinState* node);

    /*
     * @Brief		: Code generation for addLongInternal in bloom filter
     * @Description	: We use CRC32 hash function to replace the original Thomas
     *				  Wang's hash function. When addLongInternal function is
     *				  codegened, includeLong must use the same hash function.
     * @in	node	: VecHashJoinState, the node to be codegened
     * @return		: The LLVM IR function of addLongInternal for hash bloom filter.
     */
    static llvm::Function* HashJoinCodeGen_bf_addLong(VecHashJoinState* node);

    /*
     * @Brief		: Code generation for includeLong in bloom filter
     * @Description	: We use CRC32 hash function to replace the original Thomas
     *				  Wang's hash function. When includeLong function is
     *				  codegened, addLong must use the same hash function.
     * @in	node	: VecHashJoinState, the node to be codegened
     * @return		: The LLVM IR function of includeLong for hash bloom filter.
     */
    static llvm::Function* HashJoinCodeGen_bf_includeLong(VecHashJoinState* node);
};
#endif
}  // namespace dorado
#endif
