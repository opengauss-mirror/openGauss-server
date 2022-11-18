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
 * cstore_compress_copy.cpp
 *    user interface for compression methods, copy comperss values
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/cstore/compression/cstore_compress_copy.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include "access/htup.h"
#include "storage/cstore/cstore_compress.h"

static char* NumericCompressFillHeader(char* ptr, const numerics_statistics_out* out1, const numerics_cmprs_out* out2)
{
    uint16* flags = (uint16*)ptr;
    ptr += sizeof(uint16);

    /// first remember the original total size needed.
    *(int*)ptr = out1->original_size;
    ptr += sizeof(int);

    /// reset all the flags.
    *flags = 0;

    /// handle the part of int64 values
    if (out1->int64_n_values > 0) {
        uint16 modes = out2->int64_out_args.modes;

        *flags |= NUMERIC_FLAG_EXIST_INT64_VAL;
        if (out1->int64_values_are_same) {
            /// we can compute the original size, so that
            /// we don't remember its value here.
            *flags |= NUMERIC_FLAG_INT64_SAME_VAL;
            *(int64*)ptr = out1->int64_minval;
            ptr += sizeof(int64);
        } else if (modes != 0) {
            if (modes & CU_DeltaCompressed) {
                *flags |= NUMERIC_FLAG_INT64_WITH_DELTA;
                /// remember the min value.
                *(int64*)ptr = out1->int64_minval;
                ptr += sizeof(int64);
                /// remember the size of each value.
                /// it's safe to use uint8 data type, because the
                /// the returned value is 0~8.
                *(uint8*)ptr = (uint8)DeltaGetBytesNum(out1->int64_minval, out1->int64_maxval);
                ptr += sizeof(uint8);
            }
            if (modes & CU_RLECompressed) {
                *flags |= NUMERIC_FLAG_INT64_WITH_RLE;
            }

            /// i don't think we should remember the original size,
            /// because we can scan the scalues' map, get the number
            /// of int64 values, and compute this info by multiple.
            *(int32*)ptr = out2->int64_cmpr_size;
            ptr += sizeof(int32);
        }

        if (out1->int64_ascales_are_same) {
            *flags |= NUMERIC_FLAG_INT64_SAME_ASCALE;
            *ptr++ = out1->int64_same_ascale;
        }
    }

    /// handle the part of int32 values
    if (out1->int32_n_values > 0) {
        uint16 modes = out2->int32_out_args.modes;

        *flags |= NUMERIC_FLAG_EXIST_INT32_VAL;
        if (out1->int32_values_are_same) {
            /// we can compute the original size, so that
            /// we don't remember its value here.
            *flags |= NUMERIC_FLAG_INT32_SAME_VAL;
            *(int32*)ptr = out1->int32_minval;
            ptr += sizeof(int32);
        } else if (modes != 0) {
            if (modes & CU_DeltaCompressed) {
                *flags |= NUMERIC_FLAG_INT32_WITH_DELTA;
                /// remember the min value.
                *(int32*)ptr = out1->int32_minval;
                ptr += sizeof(int32);
                /// remember the size of each value.
                /// it's safe to use uint8 data type, because the
                /// the returned value is 0~8.
                *(uint8*)ptr = (uint8)DeltaGetBytesNum(out1->int32_minval, out1->int32_maxval);
                ptr += sizeof(uint8);
            }
            if (modes & CU_RLECompressed) {
                *flags |= NUMERIC_FLAG_INT32_WITH_RLE;
            }

            /// i don't think we should remember the original size,
            /// because we can scan the scalues' map, get the number
            /// of int32 values, and compute this info by multiple.
            *(int32*)ptr = out2->int32_cmpr_size;
            ptr += sizeof(int32);
        }

        if (out1->int32_ascales_are_same) {
            *flags |= NUMERIC_FLAG_INT32_SAME_ASCALE;
            *ptr++ = out1->int32_same_ascale;
        }
    }

    // handle the part of remain numeric values
    if (out1->failed_cnt > 0) {
        *flags |= NUMERIC_FLAG_EXIST_OTHERS;

        if (out2->other_apply_lz4) {
            /// we needn't store the compressed size,
            /// because the header of compressed data has
            /// remembered this info.
            *flags |= NUMERIC_FLAG_OTHER_WITH_LZ4;
        } else {
            *(int32*)ptr = out1->failed_size;
            ptr += sizeof(int32);
        }
    }
    return ptr;
}

/*
 * @Description:  compact 2bits in 1Byte for dscale codes
 *   (2 bits + 2 bits + 2 bits + 2 bits) --> 1Byte
 *   dscale numeric using 2bit flags to  distinguish int32, int64 and numeric
 * @IN/OUT outBuf: dest buffer of store compacted bytes
 * @IN  inValues:  srcource buffer of dscale bits
 * @IN  nInValues: number of dscale in buffer
 * @Return: size of dest buffer used
 */
static int CompactDscaleCodesBy2Bits(uint8* outBuf, const uint8* inValues, int nInValues)
{
    uint8* startPos = outBuf;
    // treat every 4 values as a group
    const uint32 groups = (uint32)nInValues >> 2;
    const uint32 remain = (uint32)nInValues & 0x03;

#ifdef USE_ASSERT_CHECKING
    for (int i = 0; i < nInValues; i++) {
        Assert(inValues[i] < 4);
    }
#endif

    for (uint32 i = 0; i < groups; ++i, inValues += 4) {
        // 2 bits + 2 bits + 2 bits + 2 bits
        *outBuf++ = (inValues[0] << 6) | (inValues[1] << 4) | (inValues[2] << 2) | inValues[3];
    }

    switch (remain) {
        case 3:
            *outBuf++ = (inValues[0] << 6) | (inValues[1] << 4) | (inValues[2] << 2);
            break;
        case 2:
            *outBuf++ = (inValues[0] << 6) | (inValues[1] << 4);
            break;
        case 1:
            *outBuf++ = (inValues[0] << 6);
            break;
        default:
            // nothing to do
            break;
    }

    return outBuf - startPos;
}

/*
 * @Description: compress dscale and store in buffer
 * @IN/OUT ptr:  pointer of buffer to store compressed dscale
 * @IN out1:  statistics result
 * @OUT pFlags: reserved for compression flag
 * @Return: pointer of after store compressed dscale
 */
static char* NumericCompressFillDscales(
    char* ptr, numerics_statistics_out* out1, uint16* /* pFlags */)
{
    // we cannot tolerate this case where either int64 or int32 part exists.
    Assert(out1->n_notnull != out1->failed_cnt);
    Assert(out1->int32_n_values > 0 || out1->int64_n_values > 0);
    Assert(out1->n_notnull > 0);
    Assert(out1->int32_ascales_are_same);
    Assert(out1->int64_ascales_are_same);

    // Case 1: All values are converted to integer
    if (out1->failed_cnt == 0) {
        if (out1->int64_n_values == 0) {
            // only int32 with same scale
            // each value is equal to out1->int32_same_ascale
            // so we needn't store anything.
            return ptr;
        } else if (out1->int32_n_values == 0) {
            // only int64 with same scale
            // each value is equal to out1->int64_same_ascale.
            // so we needn't remember anything.
            return ptr;
        } else {
            // int32 with same scale and int64 with same scale
            // set int32 with 1, int64 with 0 in bitmap
            FillBitmap<char>(ptr, out1->ascale_codes, out1->n_notnull, INT64_DSCALE_ENCODING);
            return ptr + (long)BITMAPLEN(out1->n_notnull);
        }
    } else { // Case 2: Mixed values with integer or numeric
        if (out1->int32_n_values == 0) {
            // int64 with same scale and numeric
            // set numeric with 1, int64 with 0 in bitmap
            FillBitmap<char>(ptr, out1->ascale_codes, out1->n_notnull, INT64_DSCALE_ENCODING);
            return ptr + (long)BITMAPLEN(out1->n_notnull);
        } else if (out1->int64_n_values == 0) {
            // int32 with same scale and numeric
            // set numeric with 1, int32 with 0 in bitmap
            FillBitmap<char>(ptr, out1->ascale_codes, out1->n_notnull, INT32_DSCALE_ENCODING);
            return ptr + (long)BITMAPLEN(out1->n_notnull);
        } else {
            // int32 with same scale,int64 with same scale and numeric
            // encoding NUMERIC_DSCALE_ENCODING,INT32_DSCALE_ENCODING and INT64_DSCALE_ENCODING using 2bit
            return ptr + CompactDscaleCodesBy2Bits((uint8*)ptr, (uint8*)out1->ascale_codes, out1->n_notnull);
        }
    }
}

static char* NumericCompressFillValues(char* ptr, const numerics_statistics_out* out1, const numerics_cmprs_out* out2)
{
    int rc = 0;
    int src_size = 0;

    // step 1: int64 values
    if (out1->int64_n_values > 0 && out1->int64_minval != out1->int64_maxval) {
        if (out2->int64_out_args.modes != 0) {
            rc = memcpy_s(ptr, out2->int64_cmpr_size, out2->int64_out_args.buf, out2->int64_cmpr_size);
            securec_check(rc, "", "");
            ptr += out2->int64_cmpr_size;
        } else {
            src_size = sizeof(int64) * out1->int64_n_values;
            rc = memcpy_s(ptr, src_size, (char*)out1->int64_values, src_size);
            securec_check(rc, "", "");
            ptr += src_size;
        }
    }

    // step 2: int32 values
    if (out1->int32_n_values > 0 && out1->int32_minval != out1->int32_maxval) {
        if (out2->int32_out_args.modes != 0) {
            rc = memcpy_s(ptr, out2->int32_cmpr_size, out2->int32_out_args.buf, out2->int32_cmpr_size);
            securec_check(rc, "", "");
            ptr += out2->int32_cmpr_size;
        } else {
            src_size = sizeof(int32) * out1->int32_n_values;
            rc = memcpy_s(ptr, src_size, (char*)out1->int32_values, src_size);
            securec_check(rc, "", "");
            ptr += src_size;
        }
    }

    // step 3: the other numeric values
    if (out1->failed_cnt > 0) {
        if (out2->other_apply_lz4) {
            rc = memcpy_s(ptr, out2->other_cmpr_size, out2->other_outbuf, out2->other_cmpr_size);
            securec_check(rc, "", "");
            ptr += out2->other_cmpr_size;
        } else {
            rc = memcpy_s(ptr, out1->failed_size, out2->other_srcbuf, out1->failed_size);
            securec_check(rc, "", "");
            ptr += out1->failed_size;
        }
    }
    return ptr;
}

char* NumericCopyCompressedBatchValues(char* ptr, numerics_statistics_out* out1, numerics_cmprs_out* out2)
{
    // step 1: set header flags and its possible metadata.
    uint16* pFlags = (uint16*)ptr;
    ptr = NumericCompressFillHeader(ptr, out1, out2);

    // step 2: fill all the status codes.
    ptr = NumericCompressFillDscales(ptr, out1, pFlags);

    // step 3: fill the real values.
    ptr = NumericCompressFillValues(ptr, out1, out2);

    return ptr;
}
