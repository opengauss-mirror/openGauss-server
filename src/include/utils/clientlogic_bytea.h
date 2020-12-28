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
 * clientlogic_bytea.h
 *
 * IDENTIFICATION
 *	  src\include\utils\clientlogic_bytea.h
 *
 * -------------------------------------------------------------------------
 */

#pragma once

#include "fmgr.h"

typedef enum {
    ENCRYPTEDCOL_OUTPUT_ESCAPE,
    ENCRYPTEDCOL_OUTPUT_HEX
} EncryptedColOutputType;

extern THR_LOCAL int byteawithoutorderwithequalcol_output; /* ByteaOutputType, but int for GUC enum */

/* functions are in utils/adt/varlena.c */
extern Datum byteawithoutorderwithequalcolin(PG_FUNCTION_ARGS);
extern Datum byteawithoutorderwithequalcolout(PG_FUNCTION_ARGS);
extern Datum byteawithoutorderwithequalcolrecv(PG_FUNCTION_ARGS);
extern Datum byteawithoutorderwithequalcolsend(PG_FUNCTION_ARGS);
extern Datum byteawithoutorderwithequalcoloctetlen(PG_FUNCTION_ARGS);
extern Datum byteawithoutorderwithequalcolGetByte(PG_FUNCTION_ARGS);
extern Datum byteawithoutorderwithequalcolGetBit(PG_FUNCTION_ARGS);
extern Datum byteawithoutorderwithequalcolSetByte(PG_FUNCTION_ARGS);
extern Datum byteawithoutorderwithequalcolSetBit(PG_FUNCTION_ARGS);
extern Datum byteawithoutorderwithequalcoleq(PG_FUNCTION_ARGS);
extern Datum byteawithoutorderwithequalcolne(PG_FUNCTION_ARGS);
extern Datum byteawithoutorderwithequalcollt(PG_FUNCTION_ARGS);
extern Datum byteawithoutorderwithequalcolle(PG_FUNCTION_ARGS);
extern Datum byteawithoutorderwithequalcolgt(PG_FUNCTION_ARGS);
extern Datum byteawithoutorderwithequalcolge(PG_FUNCTION_ARGS);
extern Datum byteawithoutorderwithequalcolcmp(PG_FUNCTION_ARGS);
extern Datum byteawithoutorderwithequalcoleqbytear(PG_FUNCTION_ARGS);
extern Datum byteawithoutorderwithequalcolnebytear(PG_FUNCTION_ARGS);
extern Datum byteawithoutorderwithequalcolltbytear(PG_FUNCTION_ARGS);
extern Datum byteawithoutorderwithequalcollebytear(PG_FUNCTION_ARGS);
extern Datum byteawithoutorderwithequalcolgtbytear(PG_FUNCTION_ARGS);
extern Datum byteawithoutorderwithequalcolgebytear(PG_FUNCTION_ARGS);
extern Datum byteawithoutorderwithequalcolcmpbytear(PG_FUNCTION_ARGS);
extern Datum byteawithoutorderwithequalcoleqbyteal(PG_FUNCTION_ARGS);
extern Datum byteawithoutorderwithequalcolnebyteal(PG_FUNCTION_ARGS);
extern Datum byteawithoutorderwithequalcolltbyteal(PG_FUNCTION_ARGS);
extern Datum byteawithoutorderwithequalcollebyteal(PG_FUNCTION_ARGS);
extern Datum byteawithoutorderwithequalcolgtbyteal(PG_FUNCTION_ARGS);
extern Datum byteawithoutorderwithequalcolgebyteal(PG_FUNCTION_ARGS);
extern Datum byteawithoutorderwithequalcolcmpbyteal(PG_FUNCTION_ARGS);
extern Datum byteawithoutorderwithequalcol_sortsupport(PG_FUNCTION_ARGS);
extern Datum byteawithoutorderwithequalcolpos(PG_FUNCTION_ARGS);
extern Datum byteawithoutorderwithequalcol_substr(PG_FUNCTION_ARGS);
extern Datum byteawithoutorderwithequalcol_substr_no_len(PG_FUNCTION_ARGS);
extern Datum byteawithoutorderwithequalcoloverlay(PG_FUNCTION_ARGS);
extern Datum byteawithoutorderwithequalcoloverlay_no_len(PG_FUNCTION_ARGS);
