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
 * vsonicarray.cpp
 *     Routines to handle Sonic datum array.
 *     SonicDatumArray is the base class to store data and flag.
 *
 * IDENTIFICATION
 *     src/backend/vectorsonic/sonicarray/vsonicarray.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "c.h"
#include "catalog/pg_type.h"
#include "vectorsonic/vsonicarray.h"
#include "utils/dynahash.h"
#include "vectorsonic/vsonicfilesource.h"

/*
 * @Description: Get the data length.
 * @in type_oid - type oid
 * @in typeMod - type mode
 * @return - type length that Sonichash concerns
 */
int getDataMinLen(Oid type_oid, int type_mod)
{
    switch (type_oid) {
        case BOOLOID:
        case CHAROID:
        case INT1OID:
            return 1;
        case INT2OID:
        case SMGROID:
            return 2;
        case ABSTIMEOID:
        case ANYELEMENTOID:
        case ANYENUMOID:
        case ANYNONARRAYOID:
        case ANYOID:
        case CIDOID:
        case DATEOID:
        case FDW_HANDLEROID:
        case FLOAT4OID:
        case INT4OID:
        case LANGUAGE_HANDLEROID:
        case OIDOID:
        case OPAQUEOID:
        case REGCLASSOID:
        case REGCONFIGOID:
        case REGDICTIONARYOID:
        case REGOPERATOROID:
        case REGOPEROID:
        case REGPROCEDUREOID:
        case REGPROCOID:
        case REGTYPEOID:
        case RELTIMEOID:
        case TRIGGEROID:
        case VOIDOID:
        case XIDOID:
            return 4;
        case CASHOID:
        case FLOAT8OID:
        case INT8OID:
        case HLL_HASHVAL_OID:
        case INTERNALOID:
        case SMALLDATETIMEOID:
        case TIDOID:
        case TIMEOID:
        case TIMESTAMPOID:
        case TIMESTAMPTZOID:
            return 8;
        case ACLITEMOID:
        case TINTERVALOID:
        case TIMETZOID:
            return 12;
        case INTERVALOID:
        case POINTOID:
            return 16;
        case BPCHAROID:
            if (type_mod != -1) {
                Assert(type_mod > VARHDRSZ);
                return type_mod - VARHDRSZ;
            }
            /* fall through */
        default:
            /* variable length data type */
            return -1;
            break;
    }
    Assert(false);
    /* keep compiler quiet. */
    return 0;
}

/*
 * @Description: get attr desc.
 * 	Now there are 5 types: SONIC_XXX_TYPE defined in vsonicarray.h
 * @in desc - description needed to define.
 * @in type_size - attr length used if attr is NULL.
 * @in attr - Form_pg_attribute info.
 * @in do_not_compress - if true and attr is not intXXX, goto StackEncoding.
 */

void getDataDescTypeId(DatumDesc* desc, int type_size)
{
    switch (type_size) {
        case 1:
            desc->typeId = INT1OID;
            break;
        case 2:
            desc->typeId = INT2OID;
            break;
        case 4:
            desc->typeId = INT4OID;
            break;
        case 8:
            desc->typeId = INT8OID;
            break;
        default:
            Assert(false);
            ereport(ERROR,
                (errmodule(MOD_VEC_EXECUTOR),
                    errcode(ERRCODE_WRONG_OBJECT_TYPE),
                    errmsg("[SonicHash] Unsupport sonic data desc size %d without tuple desc", type_size)));
            /* keep complier quiet */
            break;
    }
}

void getDataDesc(DatumDesc* desc, int type_size, Form_pg_attribute attr, bool do_not_compress)
{
    /* we do not have tuple desc, the only support type is 1,2,4,8 int type. */
    if (attr == NULL) {
        getDataDescTypeId(desc, type_size);
        desc->typeSize = type_size;
        desc->typeMod = -1;
        desc->dataType = SONIC_INT_TYPE;
    } else {
        desc->typeId = attr->atttypid;
        desc->typeMod = attr->atttypmod;
        bool encoded = COL_IS_ENCODE(attr->atttypid);
        int minlen = getDataMinLen(desc->typeId, desc->typeMod);

        if (!encoded) {
            desc->dataType = SONIC_INT_TYPE;
            desc->typeSize = minlen;
            if (desc->typeId == TIDOID)
                desc->typeSize = 8;

            Assert(minlen == 1 || minlen == 2 || minlen == 4 || minlen == 8);
        } else {
            if (minlen == -1) {
                if (desc->typeId == NUMERICOID)
                    desc->dataType = SONIC_NUMERIC_COMPRESS_TYPE;
                else
                    desc->dataType = SONIC_VAR_TYPE;
                desc->typeSize = sizeof(Datum);
            } else if (attr->attlen > 0 && minlen > 8 && minlen <= 16) {
                desc->dataType = SONIC_FIXLEN_TYPE;
                desc->typeSize = minlen;
            } else if (attr->atttypid == BPCHAROID) {
                desc->dataType = SONIC_CHAR_DIC_TYPE;
                desc->typeSize = minlen;
            } else {
                Assert(false);
                ereport(ERROR,
                    (errmodule(MOD_VEC_EXECUTOR),
                        errcode(ERRCODE_WRONG_OBJECT_TYPE),
                        errmsg("[SonicHash] Unrecognize data type %u with tuple desc", attr->atttypid)));
            }
        }

        /*
         * Change to SONIC_VAR_TYPE
         * When the types below are hash keys, store them as pointer.
         */
        if (do_not_compress) {
            if (desc->dataType == SONIC_CHAR_DIC_TYPE || desc->dataType == SONIC_FIXLEN_TYPE ||
                desc->dataType == SONIC_NUMERIC_COMPRESS_TYPE) {
                desc->dataType = SONIC_VAR_TYPE;
                desc->typeSize = sizeof(Datum);
            }
        }
    }
}

/*
 * @Description: constructor.
 * 	Initialize some public variables.
 * @in cxt - memory context.
 * @in capacity - atom size.
 */
SonicDatumArray::SonicDatumArray(MemoryContext cxt, int capacity)
    : m_cxt(cxt), m_arrSize(INIT_ARR_CONTAINER_SIZE), m_atomSize(capacity), m_nbit(my_log2(capacity)), m_atomIdx(-1)
{
    MemoryContext old_cxt = MemoryContextSwitchTo(m_cxt);
    m_arr = (atom**)palloc0(sizeof(atom*) * m_arrSize);

    m_curAtom = NULL;
    m_arrIdx = -1;
    m_atomTypeSize = 0;
    m_nullFlag = false;

    m_curData = NULL;
    m_curFlag = NULL;

    (void)MemoryContextSwitchTo(old_cxt);
}

/*
 * @Description: Generate new atom if there is no space.
 * @in gen_bit_map - If true, allocate space for null flag.
 */
void SonicDatumArray::genNewArray(bool gen_bit_map)
{
    MemoryContext old_cxt = MemoryContextSwitchTo(m_cxt);

    if (unlikely((++m_arrIdx) >= m_arrSize)) {
        /*
         * Here we should guarentee that the m_arrSize is within 2^30,
         * so that after multiple 2 m_arrSize will be less than 2^31.
         * To avoid bad performance influence,
         * we check this condition in loadPartition() when we load all data from temp files
         * and in SaveToMemory() when put original data into memory.
         * Although the maxium m_rows should be m_arrSize * m_atomSize (2^31*2^14 = 2^45)
         * the maxium hash table size (which is going to calculate later) is only 2^32,
         * so the max rows should be less than the number which hash the maximun prime number under 2^32,
         * that is SONIC_MAX_ROWS.
         * Thus, the m_rows should be less than SONIC_MAX_ROWS.
         */
        Assert(m_arrSize <= INT_MAX / 2);
        m_arrSize = m_arrSize * 2;
        m_arr = (atom**)repalloc(m_arr, sizeof(atom*) * m_arrSize);
    }

    atom* at = (atom*)palloc(sizeof(atom));
    at->data = (char*)palloc0(m_atomTypeSize * m_atomSize);
    if (gen_bit_map)
        at->nullFlag = (char*)palloc0(sizeof(bool) * m_atomSize);
    else
        at->nullFlag = NULL;

    m_curAtom = at;
    m_arr[m_arrIdx] = at;
    m_atomIdx = 0;

    /* init flag and curdata in current atom */
    m_curFlag = (uint8*)m_curAtom->nullFlag;
    m_curData = m_curAtom->data;
    (void)MemoryContextSwitchTo(old_cxt);
}

/*
 * @Description: put data and flag into atom.
 * 	If more space is needed, allocate it via genNewArray().
 * @in val - data to put.
 * @in flag - flag to put.
 * @in nrows - the number of data or flag.
 */
void SonicDatumArray::putArray(ScalarValue* vals, uint8* flag, int rows)
{
    int loop_rows;

    bool need_expand = (uint32)rows > (m_atomSize - m_atomIdx);
    loop_rows = need_expand ? (m_atomSize - m_atomIdx) : rows;

    if (loop_rows > 0) {
        /* set with checking flag */
        putDatumArrayWithNullCheck(vals, flag, loop_rows);
        if (m_nullFlag)
            putNullFlagArray(flag, loop_rows);
        m_atomIdx += loop_rows;
    }

    if (need_expand) {
        genNewArray(m_nullFlag);
        putArray(vals + loop_rows, flag + loop_rows, rows - loop_rows);
    }
}

/*
 * @Description: load data and flag into atom by row.
 * 	If more space is needed, allocate it via genNewArray().
 * @in file - data in the file to put.
 * @rowIdx - current rowIdx ,needed in numeric type.
 */
void SonicDatumArray::loadArrayByRow(void* file)
{
    if (unlikely(m_atomIdx % m_atomSize == 0)) {
        genNewArray(m_nullFlag);
        loadDatumFlagArrayByRow(file, m_atomSize);
        m_atomIdx += 1;
    } else {
        loadDatumFlagArrayByRow(file, m_atomSize - m_atomIdx);
        m_atomIdx += 1;
    }
}

/*
 * @Description: load data and flag into atom.
 * 	If more space is needed, allocate it via genNewArray().
 * @in file - data in the file to put.
 * @in nrows - the number of data or flag.
 */
void SonicDatumArray::loadArray(void* file, int64 rows)
{
    CHECK_FOR_INTERRUPTS();

    /* the number of rows left in the current atom */
    int left_rows = m_atomSize - m_atomIdx;

    if (rows > left_rows) {
        /*
         * cannot fill in the current atom
         * first fill the rest of the current atom
         */
        loadDatumFlagArray(file, left_rows);
        m_atomIdx += left_rows;
        rows -= left_rows;

        /* find out how many full atom to put */
        int64 loops = rows / m_atomSize;

        /* find out how many rows to put in the last atom */
        int last_rows = rows % m_atomSize;

        for (int64 loop = 0; loop < loops; ++loop) {
            genNewArray(m_nullFlag);
            loadDatumFlagArray(file, m_atomSize);
            m_atomIdx += m_atomSize;
        }

        if (last_rows > 0) {
            genNewArray(m_nullFlag);
            loadDatumFlagArray(file, last_rows);
            m_atomIdx += last_rows;
        }
    } else {
        loadDatumFlagArray(file, rows);
        m_atomIdx += rows;
    }
}

/*
 * @Description: put flag into single atom.
 * @in flag - flag to put.
 * @in nrows - the number of flag.
 */
inline void SonicDatumArray::putNullFlagArray(uint8* flag, int rows)
{
    char* flag_array = &m_curAtom->nullFlag[m_atomIdx];

    for (int i = 0; i < rows; i++) {
        *flag_array++ = *flag++;
    }
}

/*
 * @Description: return the number of data in current DatumArray.
 */
int64 SonicDatumArray::getRows()
{
    Assert(m_arrIdx >= -1);

    if (m_arrIdx == -1) {
        return 0;
    }

    return ((int64)m_arrIdx) * m_atomSize + m_atomIdx - 1;
}

/*
 * @Description: get nth value and flag from DatumArray.
 * @in nth - the position of value and flag gotten.
 * @in val - output value.
 * @out toflag - output flag.
 */
void SonicDatumArray::getNthDatumFlag(uint32 nth, ScalarValue* val, uint8* to_flag)
{
    int arrIndx;
    int atomIndx;

    arrIndx = (int)getArrayIndx(nth, m_nbit);
    atomIndx = (int)getArrayLoc(nth, m_atomSize - 1);

    Assert(m_nullFlag);
    getDatumFlag(arrIndx, atomIndx, val, to_flag);
}

/*
 * @Description: get nth value from DatumArray.
 * @in nth - the position of value gotten.
 * @return - nth value.
 */
Datum SonicDatumArray::getNthDatum(uint32 nth)
{
    int arrIndx;
    int atomIndx;

    /* Separate nth to arrIndx and atomIndx. */
    arrIndx = (int)getArrayIndx(nth, m_nbit);
    atomIndx = (int)getArrayLoc(nth, m_atomSize - 1);

    return getDatum(arrIndx, atomIndx);
}

/*
 * @Description: Get atom index from input DatumArray location.
 * @in nrows - The size of locs.
 * @in locs - location info in DatumArray.
 * @out array_idx - ouput indexes. The size should be nrows.
 */
void SonicDatumArray::getArrayAtomIdx(int nrows, uint32* locs, ArrayIdx* array_idx)
{
    Assert(nrows <= BatchMaxSize);
    int mask = m_atomSize - 1;
    int i;

    for (i = 0; i < nrows; i++) {
        array_idx->arrIdx = getArrayIndx(*locs, m_nbit);
        array_idx->atomIdx = getArrayLoc(*locs, mask);
        array_idx++;
        locs++;
    }
}

/*
 * @Description:  update data in atom. Notice that if *val* is a temporary variable,
 * free of it after the call of replaceVariable.
 *
 * @in old_val - location  of old_val.
 * @out val - location of updated value.
 */
ScalarValue SonicDatumArray::replaceVariable(Datum old_val, Datum val)
{
    int key_size = VARSIZE_ANY(val);
    int old_key_size = VARSIZE_ANY(old_val);
    errno_t rc;
    /* if we can reuse the space, just fine */
    if (key_size <= old_key_size) {
        /* update value */
        rc = memcpy_s(DatumGetPointer(old_val), key_size, DatumGetPointer(val), key_size);
        securec_check(rc, "\0", "\0");
        return old_val;
    } else {
        AutoContextSwitch memGuard(m_cxt);
        /* free the buffer */
        pfree(DatumGetPointer(old_val));

        char* addr = (char*)palloc(key_size);
        /* update value */
        rc = memcpy_s(addr, key_size, DatumGetPointer(val), key_size);
        securec_check(rc, "\0", "\0");
        return PointerGetDatum(addr);
    }
}
