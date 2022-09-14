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
 * fencedudf.h
 *        The header file of implementing to run UDF in fenced mode
 *
 * The core design is to implement a RPC server and client, run udf in RPC server.
 * User defined C function is not safe, because if the UDF has some bugs which can
 * cause coredump or memory leak in gaussdb, So we need provide an fenced mode for udf.
 * When we run fenecd udf, it will run in RPC server. If UDF cause coredump, there is not
 * any impact for gaussdb process.
 * 
 * 
 * IDENTIFICATION
 *        src/include/postmaster/fencedudf.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef FENCEDUDF_H
#define FENCEDUDF_H
#include "access/htup.h"

extern pid_t StartUDFMaster();
extern void FencedUDFMasterMain(int argc, char* argv[]);
extern bool RPCInitFencedUDFIfNeed(Oid functionId, FmgrInfo* finfo, HeapTuple procedureTuple);
template <bool batchMode>
extern Datum RPCFencedUDF(FunctionCallInfo fcinfo);
extern void InitFuncCallUDFInfo(FunctionCallInfoData* finfo, int batchRows, int argN, bool batchMode);

#endif
