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
 * bloom_filter.cpp
 *
 * IDENTIFICATION
 *    src/gausskernel/cbb/utils/bloomfilter/bloom_filter.cpp
 *
 * -------------------------------------------------------------------------
 */
#include <math.h>
#include "utils/bloom_filter.h"
#include "catalog/pg_type.h"
#include "utils/builtins.h"
#include "utils/memutils.h"

namespace filter {

#define LONG_BYTE_SIZE (sizeof(uint64))
#define LONG_BIT_SIZE (8 * LONG_BYTE_SIZE)
#define STARTUP_ENTRIES 500
#define MAX_ENTRIES 10000000

BitSet::~BitSet()
{
    pfree_ext(data);
}

BitSet::BitSet(const MemoryContext& ctx, const uint64 bits)
{
    AutoContextSwitch memGuard(ctx);
    // This code is used to achieve rounding up
    int count = (bits % LONG_BIT_SIZE) == 0 ? 0 : 1;
    length = static_cast<uint64>((bits / LONG_BIT_SIZE) + count);
    data = (uint64*)palloc0(sizeof(uint64) * length);
}

BitSet::BitSet(uint64* _data, uint64 _length)
{
    length = _length;
    data = _data;
}

/*
 * Sets the bit at specified index.
 *
 * @param index - position
 */
inline void BitSet::set(uint64 _index)
{
    data[_index >> LSB_IDENTIFY] |= (1L << _index);
}

/*
 * Returns true if the bit is set in the specified index.
 *
 * @param index - position
 * @return - value at the bit position
 */
inline bool BitSet::get(uint64 _index) const
{
    return (data[_index >> LSB_IDENTIFY] & (1L << _index)) != 0;
}

inline uint64 BitSet::getLength() const
{
    return length;
}

inline const uint64* BitSet::getData() const
{
    return data;
}

/*
 * Combines the two BitArrays using bitwise OR.
 */
inline void BitSet::unionAll(const uint64* array)
{
    for (uint64 i = 0; i < length; i++) {
        data[i] |= array[i];
    }
}

/*
 * Combines the two BitArrays using bitwise AND.
 */
inline void BitSet::intersectAll(const uint64* array)
{
    for (uint64 i = 0; i < length; i++) {
        data[i] &= array[i];
    }
}

/*
 * Clear the bit set.
 */
inline void BitSet::clear()
{
    errno_t rc = memset_s(data, LONG_BYTE_SIZE * length, 0, LONG_BYTE_SIZE * length);
    securec_check(rc, "\0", "\0");
}

int64 Murmur3::hash64(const char* data, int32 length)
{
    return hash64(data, length, DEFAULT_SEED);
}

/**
 * Murmur3 64-bit variant. This is essentially MSB 8 bytes of Murmur3 128-bit variant.
 *
 * @param data   - input byte array
 * @param length - length of array
 * @param seed   - seed. (default is 0)
 * @return - hashcode
 */
int64 Murmur3::hash64(const char* data, int32 length, int32 seed)
{
    int64 hash = seed;
    const int32 nblocks = length >> 3;

    // body
    for (int32 i = 0; i < nblocks; i++) {
        const int32 i8 = i << 3;
        int64 k = ((int64)data[i8] & 0xff) | (((int64)data[i8 + 1] & 0xff) << 8) |
                  (((int64)data[i8 + 2] & 0xff) << 16) | (((int64)data[i8 + 3] & 0xff) << 24) |
                  (((int64)data[i8 + 4] & 0xff) << 32) | (((int64)data[i8 + 5] & 0xff) << 40) |
                  (((int64)data[i8 + 6] & 0xff) << 48) | (((int64)data[i8 + 7] & 0xff) << 56);

        // mix functions
        k *= C1;
        k = rotateLeft(k, R1);
        k *= C2;
        hash ^= k;
        hash = rotateLeft(hash, R2) * M + N1;
    }

    // tail
    int64 k1 = 0;
    int tailStart = nblocks << 3;
    switch (length - tailStart) {
        case 7: {
            k1 ^= ((int64)data[tailStart + 6] & 0xff) << 48;
        }
            /* fall through */
        case 6: {
            k1 ^= ((int64)data[tailStart + 5] & 0xff) << 40;
        }
            /* fall through */
        case 5: {
            k1 ^= ((int64)data[tailStart + 4] & 0xff) << 32;
        }
            /* fall through */
        case 4: {
            k1 ^= ((int64)data[tailStart + 3] & 0xff) << 24;
        }
            /* fall through */
        case 3: {
            k1 ^= ((int64)data[tailStart + 2] & 0xff) << 16;
        }
            /* fall through */
        case 2: {
            k1 ^= ((int64)data[tailStart + 1] & 0xff) << 8;
        }
            /* fall through */
        case 1: {
            k1 ^= ((int64)data[tailStart] & 0xff);
            k1 *= C1;
            k1 = rotateLeft(k1, R1);
            k1 *= C2;
            hash ^= k1;
        }
            /* fall through */
        default: {
            break;
        }
    }

    // finalization
    hash ^= length;
    hash = fmix64(hash);

    return hash;
}

inline int64 Murmur3::rotateLeft(int64 n, uint32 i)
{
    return ((0xFFFFFFFFFFFFFFFF & (uint64)n) << i) | ((0xFFFFFFFFFFFFFFFF & (uint64)n) >> (64 - i));
}

inline int64 Murmur3::rotateRight(int64 n, uint32 i)
{
    return ((0xFFFFFFFFFFFFFFFF & (uint64)n) >> i) | ((0xFFFFFFFFFFFFFFFF & (uint64)n) << (64 - i));
}

inline int64 Murmur3::fmix64(int64 v)
{
    uint64 h = (uint64)v;
    h ^= ((0xFFFFFFFFFFFFFFFF & h) >> R3);
    h *= 0xff51afd7ed558ccdL;
    h ^= ((0xFFFFFFFFFFFFFFFF & h) >> R3);
    h *= 0xc4ceb9fe1a85ec53L;
    h ^= ((0xFFFFFFFFFFFFFFFF & h) >> R3);
    return (int64)h;
}

inline const int64 Murmur3::getNullHashCode()
{
    return NULL_HASHCODE;
}

BloomFilter::~BloomFilter()
{}

BloomFilter* createBloomFilter(
    Oid dataType, int32 typeMod, Oid collation, BloomFilterType type, int64 maxNumValues, bool addMinMax)
{
    BloomFilter* ret = NULL;
    switch (dataType) {
        case INT2OID:
        case INT4OID:
        case INT8OID: {
            ret = New(CurrentMemoryContext) BloomFilterImpl<int64>(
                CurrentMemoryContext, dataType, typeMod, collation, type, maxNumValues, DEFAULT_FPP, addMinMax);
            ret->jitted_bf_addLong = NULL;
            ret->jitted_bf_incLong = NULL;
            break;
        }
        case FLOAT4OID:
        case FLOAT8OID: {
            ret = New(CurrentMemoryContext) BloomFilterImpl<float8>(
                CurrentMemoryContext, dataType, typeMod, collation, type, maxNumValues, DEFAULT_FPP, addMinMax);
            break;
        }
        case VARCHAROID:
        case BPCHAROID:
        case TEXTOID:
        case CLOBOID: {
            ret = New(CurrentMemoryContext) BloomFilterImpl<char *>(
                CurrentMemoryContext, dataType, typeMod, collation, type, maxNumValues, DEFAULT_FPP, addMinMax);
            break;
        }
        default: {
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("The data type %u is not supported for bloom filter curently.", dataType)));
        }
    }

    return ret;
}

/*
 * Create bloofilter object by bloomFilterSet struct.
 */
BloomFilter* createBloomFilter(BloomFilterSet* bloomFilterSet)
{
    BloomFilter* ret = NULL;
    switch (bloomFilterSet->dataType) {
        case INT2OID:
        case INT4OID:
        case INT8OID: {
            ret = New(CurrentMemoryContext) BloomFilterImpl<int64>(CurrentMemoryContext, bloomFilterSet);
            ret->jitted_bf_addLong = NULL;
            ret->jitted_bf_incLong = NULL;
            break;
        }
        case FLOAT4OID:
        case FLOAT8OID: {
            ret = New(CurrentMemoryContext) BloomFilterImpl<float8>(CurrentMemoryContext, bloomFilterSet);
            break;
        }
        case VARCHAROID:
        case BPCHAROID:
        case TEXTOID:
        case CLOBOID: {
            ret = New(CurrentMemoryContext) BloomFilterImpl<char *>(CurrentMemoryContext, bloomFilterSet);
            break;
        }
        default: {
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("The data type %u is not supported for bloom filter curently.", bloomFilterSet->dataType)));
        }
    }

    return ret;
}

/*
 * the struct function of BloomFilterImpl.
 * we use the bloomFilterSet to set bloomfilter object.
 */
template <typename baseType>
BloomFilterImpl<baseType>::BloomFilterImpl(MemoryContext& ctx, BloomFilterSet* bloomFilterSet)
{
    context = AllocSetContextCreate(
        ctx, "BloomFilterContext", ALLOCSET_DEFAULT_MINSIZE, ALLOCSET_DEFAULT_INITSIZE, ALLOCSET_DEFAULT_MAXSIZE);
    AutoContextSwitch memGuard(context);

    rebuildBloomFilterValue(bloomFilterSet);
}

template <typename baseType>
BloomFilterImpl<baseType>::BloomFilterImpl(MemoryContext& ctx, Oid _dataType, int32 _typeMod, Oid _collation,
    BloomFilterType _type, int64 expectedEntries, double fpp, bool _addMinMax)
    : minValue((baseType)0),
      maxValue((baseType)0),
      addMinMax(_addMinMax),
      hasMM(false),
      type(_type),
      dataType(_dataType),
      typeMod(_typeMod),
      collation(_collation)
{
    if (expectedEntries <= 0 || expectedEntries >= MAX_ENTRIES) {
        ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION), errmsg("ExpectedEntries should be > 0 and < 10000000.")));
    }

    if (fpp <= 0.0 || fpp >= 1.0) {
        ereport(
            ERROR, (errcode(ERRCODE_DATA_EXCEPTION), errmsg("False positive probability should be > 0.0 & < 1.0.")));
    }

    context = AllocSetContextCreate(
        ctx, "BloomFilterContext", ALLOCSET_DEFAULT_MINSIZE, ALLOCSET_DEFAULT_INITSIZE, ALLOCSET_DEFAULT_MAXSIZE);
    AutoContextSwitch memGuard(context);
    uint64 nb = optimalNumOfBits(expectedEntries, fpp);
    // make 'm' multiple of 64
    numBits = nb + (LONG_BIT_SIZE - (nb % LONG_BIT_SIZE));
    numHashFunctions = optimalNumOfHashFunctions(expectedEntries, numBits);
    Assert(numHashFunctions <= MAX_HASH_FUNCTIONS);
    bitSet = New(context) BitSet(context, numBits);
    numValues = 0;
    maxNumValues = expectedEntries;
    startupEntries = Min(maxNumValues, STARTUP_ENTRIES);
    valuePositions = (ValueBit*)palloc0(startupEntries * sizeof(ValueBit));
}

template <typename baseType>
BloomFilterImpl<baseType>::~BloomFilterImpl()
{
    if (NULL != valuePositions) {
        pfree_ext(valuePositions);
    }

    MemoryContextDelete(context);
}

template <typename baseType>
inline uint64 BloomFilterImpl<baseType>::optimalNumOfHashFunctions(int64 n, int m) const
{
    uint64 numHashFunc = (uint64)round((double)m / n * log(2));
    return Max(1, numHashFunc);
}

template <typename baseType>
inline uint64 BloomFilterImpl<baseType>::optimalNumOfBits(int64 n, double p) const
{
    return (uint64)(-n * log(p) / (log(2) * log(2)));
}

template <>
inline Datum BloomFilterImpl<int64>::valueToDatum(int64 value) const
{
    Datum datum = (Datum)0;
    switch (dataType) {
        case INT2OID: {
            datum = Int16GetDatum((int16)value);
            break;
        }
        case INT4OID: {
            datum = Int32GetDatum((int32)value);
            break;
        }
        case INT8OID: {
            datum = Int64GetDatum(value);
            break;
        }
        default: {
            datum = Int64GetDatum(value);
            break;
        }
    }
    return datum;
}

template <>
inline Datum BloomFilterImpl<float8>::valueToDatum(float8 value) const
{
    if (FLOAT4OID == dataType) {
        return Float4GetDatum(static_cast<float4>(value));
    } else {
        return Float8GetDatum(value);
    }
}

template <>
inline Datum BloomFilterImpl<char *>::valueToDatum(char *value) const
{
    AutoContextSwitch memGuard(context);
    Datum datum = 0;
    switch (dataType) {
        case VARCHAROID: {
            datum = DirectFunctionCall3(varcharin, CStringGetDatum(value), collation, typeMod);
            break;
        }
        case BPCHAROID: {
            datum = DirectFunctionCall3(bpcharin, CStringGetDatum(value), collation, typeMod);
            break;
        }
        case TEXTOID:
        case CLOBOID: {
            datum = CStringGetTextDatum(value);
            break;
        }
        default: {
            break;
        }
    }
    return datum;
}

template <>
inline int64 BloomFilterImpl<int64>::datumToValue(Datum datum) const
{
    int64 value = 0;
    switch (dataType) {
        case INT2OID: {
            value = DatumGetInt16(datum);
            break;
        }
        case INT4OID: {
            value = DatumGetInt32(datum);
            break;
        }
        case INT8OID: {
            value = DatumGetInt64(datum);
            break;
        }
        default: {
            value = DatumGetInt64(datum);
            break;
        }
    }
    return value;
}

template <>
inline float8 BloomFilterImpl<float8>::datumToValue(Datum datum) const
{
    if (FLOAT4OID == dataType) {
        return static_cast<float8>(DatumGetFloat4(datum));
    } else {
        return DatumGetFloat8(datum);
    }
}

template <>
inline char* BloomFilterImpl<char *>::datumToValue(Datum datum) const
{
    AutoContextSwitch memGuard(context);
    char* value = 0;
    switch (dataType) {
        case BPCHAROID: {
            Datum bpchar =
                DirectFunctionCall3(bpcharin, CStringGetDatum(TextDatumGetCString(datum)), collation, typeMod);
            value = TextDatumGetCString(bpchar);
            break;
        }
        case VARCHAROID:
        case TEXTOID:
        case CLOBOID: {
            value = TextDatumGetCString(datum);
            break;
        }
        default: {
            break;
        }
    }
    return value;
}

template <typename baseType>
inline int BloomFilterImpl<baseType>::compareValue(baseType left, baseType right) const
{
    return left > right ? 1 : (left < right ? -1 : 0);
}

template <>
inline int BloomFilterImpl<char *>::compareValue(char* left, char* right) const
{
    return varstr_cmp(left, strlen(left), right, strlen(right), collation);
}

template <>
inline void BloomFilterImpl<int64>::addLong(int64 val)
{
    if (addMinMax) {
        setMinMax(val);
    }

    if (jitted_bf_addLong != NULL) {
        typedef void (*bloomfilter_addLongfunc)(BloomFilterImpl<int64> const* bfi, int64 val);
        ((bloomfilter_addLongfunc)(jitted_bf_addLong))(this, val);
    } else {
        addLongInternal(val);
    }
}

template <>
inline void BloomFilterImpl<double>::addDouble(double val)
{
    if (addMinMax) {
        setMinMax(val);
    }
    addLongInternal(doubleToInt64(val));
}

/* not implemented */
template <>
inline void BloomFilterImpl<char *>::addString(const char *val)
{
    // We use the trick mentioned in "Less Hashing, Same Performance: Building a Better Bloom Filter"
    // by Kirsch et.al. From abstract 'only two hash functions are necessary to effectively
    // implement a Bloom filter without any loss in the asymptotic false positive probability'
    // Lets split up 64-bit hashcode into two 32-bit hash codes and employ the technique mentioned
    // in the above paper
    if (addMinMax && val != NULL) {
        setMinMax(const_cast<char *>(val));
    }
    int64 hash64 = NULL == val ? Murmur3::getNullHashCode() : Murmur3::hash64(val, strlen(val));
    addHash(hash64);
}

template <>
inline void BloomFilterImpl<int64>::addDatum(Datum datumVal)
{
    addLong(datumToValue(datumVal));
}

template <>
inline void BloomFilterImpl<float8>::addDatum(Datum datumVal)
{
    addDouble(datumToValue(datumVal));
}

template <>
inline void BloomFilterImpl<char *>::addDatum(Datum datumVal)
{
    addString(datumToValue(datumVal));
}

// Thomas Wang's integer hash function
template <typename baseType>
int64 BloomFilterImpl<baseType>::getLongHash(int64 key) const
{
    key = (~(uint64_t)key) + (key << 21);
    key = key ^ (key >> 24);
    key = (key + (key << 3)) + (key << 8);
    key = key ^ (key >> 14);
    key = (key + (key << 2)) + (key << 4);
    key = key ^ (key >> 28);
    key = key + (key << 31);
    return key;
}

template <typename baseType>
inline void BloomFilterImpl<baseType>::addLongInternal(int64 val)
{
    addHash(getLongHash(val));
}

template <typename baseType>
void BloomFilterImpl<baseType>::addHash(int64 hash64)
{
    int hash1 = (int)hash64;
    int hash2 = (int)((uint64)hash64 >> 32);

    if (numValues >= maxNumValues) {
        ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION), errmsg("Add too many values to the bloom filter.")));
    } else if (numValues >= startupEntries) {
        AutoContextSwitch memGuard(context);
        startupEntries = Min(startupEntries * 2, maxNumValues);
        valuePositions = (ValueBit*)repalloc(valuePositions, startupEntries * sizeof(ValueBit));
    }

    for (int i = 1; i <= static_cast<int>(numHashFunctions); i++) {
        int combinedHash = hash1 + (i * hash2);
        // hashcode should be positive, flip all the bits if it's negative
        if (combinedHash < 0) {
            combinedHash = ~(uint)combinedHash;
        }
        int pos = combinedHash % numBits;
        bitSet->set(pos);
        valuePositions[numValues].position[i - 1] = pos;
    }
    numValues++;
}

template <typename baseType>
bool BloomFilterImpl<baseType>::minMaxCheck(const BloomFilter& that) const
{
    if (!hasMM || !that.hasMinMax()) {
        return true;
    }

    bool ret = false;
    baseType minV = datumToValue(that.getMin());
    baseType maxV = datumToValue(that.getMax());

    if (numValues == 1) {
        if (compareValue(minValue, minV) >= 0 && compareValue(maxValue, maxV) <= 0) {
            ret = true;
        } else {
            ret = false;
        }
    } else {
        if (compareValue(minValue, minV) > 0 || compareValue(maxValue, maxV) < 0) {
            ret = false;
        } else {
            ret = true;
        }
    }

    return ret;
}

template <>
inline bool BloomFilterImpl<int64>::includeLong(int64 value) const
{
    if (jitted_bf_incLong != NULL) {
        typedef bool (*bloomfilter_incLongfunc)(BloomFilterImpl<int64> const* bfi, int64 value);
        return ((bloomfilter_incLongfunc)(jitted_bf_incLong))(this, value);
    } else {
        if (addMinMax && (1 == numValues)) {
            return (value == minValue);
        }
        return includeHashInternal(getLongHash(value));
    }
}

template <>
inline bool BloomFilterImpl<double>::includeDouble(double value) const
{
    if (addMinMax && (1 == numValues)) {
        return (value == minValue);
    }
    return includeHashInternal(getLongHash(doubleToInt64(value)));
}

template <>
inline bool BloomFilterImpl<char *>::includeString(const char *value) const
{
    if (addMinMax && (1 == numValues)) {
        int len1 = strlen(minValue);
        int len2 = strlen(value);
        if (len1 != len2) {
            return false;
        } else {
            return (0 == memcmp(minValue, value, len1));
        }
    }
    return includeHashInternal(NULL == value ? Murmur3::getNullHashCode() : Murmur3::hash64(value, strlen(value)));
}

template <>
inline bool BloomFilterImpl<int64>::includeDatum(Datum value) const
{
    return includeLong(datumToValue(value));
}

template <>
inline bool BloomFilterImpl<float8>::includeDatum(Datum value) const
{
    return includeDouble(datumToValue(value));
}

template <>
inline bool BloomFilterImpl<char *>::includeDatum(Datum value) const
{
    return includeString(datumToValue(value));
}

template <typename baseType>
bool BloomFilterImpl<baseType>::includeHashInternal(int64 hash64) const
{
    int hash1 = (int)hash64;
    int hash2 = (int)((uint64)hash64 >> 32);
    for (int i = 1; i <= static_cast<int>(numHashFunctions); i++) {
        int combinedHash = hash1 + (i * hash2);
        // hashcode should be positive, flip all the bits if it's negative
        if (combinedHash < 0) {
            combinedHash = ~(uint)combinedHash;
        }
        int pos = combinedHash % numBits;
        if (!bitSet->get(pos)) {
            return false;
        }
    }

    return true;
}

template <typename baseType>
bool BloomFilterImpl<baseType>::includedIn(const BloomFilter& that) const
{
    bool ret = false;

    if (numBits == that.getBitSize() && numHashFunctions == that.getNumHashFunctions()) {
        if (!minMaxCheck(that)) {
            ret = false;
        } else {
            ret = includedIn(that.getBitSet());
        }
    } else {
        ereport(LOG, (errmsg("BloomFilters are not compatible for intersect.")));
        ret = true;
    }

    return ret;
}

template <typename baseType>
inline bool BloomFilterImpl<baseType>::includedIn(const uint64* that) const
{
    bool ret = false;
    if (numValues == 0) {
        ret = false;
    } else if (numValues == 1) {
        ret = singleIncludedIn(that);
    } else {
        ret = multiInCludedIn(that);
    }

    return ret;
}

template <typename baseType>
bool BloomFilterImpl<baseType>::singleIncludedIn(const uint64* that) const
{
    const uint64* bitSetVals = getBitSet();
    for (uint64 i = 0; i < getLength(); i++) {
        if (bitSetVals[i] == 0) {
            continue;
        } else {
            if ((bitSetVals[i] & that[i]) != bitSetVals[i])
                return false;
        }
    }
    return true;
}

template <typename baseType>
bool BloomFilterImpl<baseType>::multiInCludedIn(const uint64* that) const
{
    uint16 pos = 0;

    for (uint64 i = 0; i < numValues; i++) {
        bool match = true;
        for (uint64 j = 0; j < numHashFunctions; j++) {
            pos = valuePositions[i].position[j];
            if ((that[pos >> LSB_IDENTIFY] & (1L << pos)) == 0) {
                match = false;
                break;
            }
        }

        if (match) {
            return true;
        }
    }

    return false;
}

template <typename baseType>
inline uint64 BloomFilterImpl<baseType>::getLength() const
{
    return bitSet->getLength();
}

template <typename baseType>
inline uint64 BloomFilterImpl<baseType>::getBitSize() const
{
    return numBits;
}

template <typename baseType>
inline uint64 BloomFilterImpl<baseType>::getNumHashFunctions() const
{
    return numHashFunctions;
}

template <typename baseType>
inline const uint64* BloomFilterImpl<baseType>::getBitSet() const
{
    return bitSet->getData();
}

template <typename baseType>
inline Oid BloomFilterImpl<baseType>::getDataType() const
{
    return dataType;
}

template <typename baseType>
inline int32 BloomFilterImpl<baseType>::getTypeMod() const
{
    return typeMod;
}

template <typename baseType>
inline BloomFilterType BloomFilterImpl<baseType>::getType() const
{
    return type;
}

template <typename baseType>
inline uint64 BloomFilterImpl<baseType>::getNumValues() const
{
    return numValues;
}

template <typename baseType>
inline Datum BloomFilterImpl<baseType>::getMin() const
{
    return valueToDatum(minValue);
}

template <typename baseType>
inline Datum BloomFilterImpl<baseType>::getMax() const
{
    return valueToDatum(maxValue);
}

template <typename baseType>
inline void BloomFilterImpl<baseType>::setMinMax(baseType val)
{
    if (!hasMM) {
        minValue = val;
        maxValue = val;
        hasMM = true;
    } else {
        if (compareValue(val, minValue) < 0) {
            minValue = val;
        } else if (compareValue(val, maxValue) > 0) {
            maxValue = val;
        }
    }
}

template <typename baseType>
inline bool BloomFilterImpl<baseType>::hasMinMax() const
{
    return hasMM;
}

template <typename baseType>
inline int64 BloomFilterImpl<baseType>::doubleToInt64(double x) const
{
    int64 bits;
    Assert(sizeof(int64) == sizeof(double));
    errno_t rc = memcpy_s(&bits, sizeof(int64), &x, sizeof(double));
    securec_check(rc, "\0", "\0");
    return bits;
}

template <typename baseType>
inline int32 BloomFilterImpl<baseType>::floatToInt32(float x) const
{
    int32 bits;
    Assert(sizeof(int32) == sizeof(float));
    errno_t rc = memcpy_s(&bits, sizeof(int32), &x, sizeof(float));
    securec_check(rc, "\0", "\0");
    return bits;
}

template <typename baseType>
void BloomFilterImpl<baseType>::unionAll(const BloomFilter& that)
{
    if (numBits == that.getBitSize() && numHashFunctions == that.getNumHashFunctions()) {
        bitSet->unionAll(that.getBitSet());
    } else {
        ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION), errmsg("BloomFilters are not compatible for merging.")));
    }
}

template <typename baseType>
void BloomFilterImpl<baseType>::intersectAll(const BloomFilter& that)
{
    if (numBits == that.getBitSize() && numHashFunctions == that.getNumHashFunctions()) {
        if (that.hasMinMax()) {
            if (hasMM) {
                baseType minV = datumToValue(that.getMin());
                baseType maxV = datumToValue(that.getMax());
                if (compareValue(minValue, minV) < 0) {
                    minValue = minV;
                }

                if (compareValue(maxValue, maxV) > 0) {
                    maxValue = maxV;
                }
            } else {
                minValue = datumToValue(that.getMin());
                maxValue = datumToValue(that.getMax());
                hasMM = true;
            }
        }
    }
}

template <typename baseType>
inline void BloomFilterImpl<baseType>::reset()
{
    numValues = 0;
    bitSet->clear();
    hasMM = false;
}

template <>
inline void BloomFilterImpl<int64>::setMinMaxValueByBloomFilterSet(BloomFilterSet* bloomFilterSet)
{
    minValue = bloomFilterSet->minIntValue;
    maxValue = bloomFilterSet->maxIntValue;
}

template <>
inline void BloomFilterImpl<float8>::setMinMaxValueByBloomFilterSet(BloomFilterSet* bloomFilterSet)
{
    minValue = bloomFilterSet->minFloatValue;
    maxValue = bloomFilterSet->maxFloatValue;
}

template <>
inline void BloomFilterImpl<char *>::setMinMaxValueByBloomFilterSet(BloomFilterSet *bloomFilterSet)
{
    minValue = pstrdup(bloomFilterSet->minStringValue);
    maxValue = pstrdup(bloomFilterSet->maxStringValue);
}

template <>
inline void BloomFilterImpl<int64>::setMinMaxValueForBloomFilterSet(BloomFilterSet* bloomFilterSet)
{
    bloomFilterSet->minIntValue = minValue;
    bloomFilterSet->maxIntValue = maxValue;
}

template <>
inline void BloomFilterImpl<float8>::setMinMaxValueForBloomFilterSet(BloomFilterSet* bloomFilterSet)
{
    bloomFilterSet->minFloatValue = minValue;
    bloomFilterSet->maxFloatValue = maxValue;
}

template <>
inline void BloomFilterImpl<char *>::setMinMaxValueForBloomFilterSet(BloomFilterSet *bloomFilterSet)
{
    bloomFilterSet->minStringValue = pstrdup(minValue);
    bloomFilterSet->maxStringValue = pstrdup(maxValue);
}

template <typename baseType>
inline BloomFilterSet* BloomFilterImpl<baseType>::makeBloomFilterSet()
{
    BloomFilterSet* bloomFilterSet = (BloomFilterSet*)makeNode(BloomFilterSet);
    bloomFilterSet->addMinMax = addMinMax;
    bloomFilterSet->collation = collation;
    bloomFilterSet->dataType = dataType;
    bloomFilterSet->hasMM = hasMM;
    bloomFilterSet->length = bitSet->getLength();

    setMinMaxValueForBloomFilterSet(bloomFilterSet);

    bloomFilterSet->maxNumValues = maxNumValues;
    bloomFilterSet->numBits = numBits;
    bloomFilterSet->numHashFunctions = numHashFunctions;
    bloomFilterSet->numValues = numValues;
    bloomFilterSet->startupEntries = startupEntries;
    bloomFilterSet->bfType = type;
    bloomFilterSet->typeMod = typeMod;

    bloomFilterSet->valuePositions = (ValueBit*)palloc0(startupEntries * sizeof(ValueBit));

    for (uint64 entryNum = 0; entryNum < startupEntries; entryNum++) {
        for (int funNum = 0; funNum < MAX_HASH_FUNCTIONS; funNum++) {
            bloomFilterSet->valuePositions[entryNum].position[funNum] = valuePositions[entryNum].position[funNum];
        }
    }

    bloomFilterSet->data = (uint64*)palloc0(sizeof(uint64) * bloomFilterSet->length);
    for (uint64 bitSetNum = 0; bitSetNum < bloomFilterSet->length; bitSetNum++) {
        const uint64* data = bitSet->getData();
        bloomFilterSet->data[bitSetNum] = data[bitSetNum];
    }

    return bloomFilterSet;
}

template <typename baseType>
inline void BloomFilterImpl<baseType>::rebuildBloomFilterValue(BloomFilterSet* bloomFilterSet)
{
    addMinMax = bloomFilterSet->addMinMax;
    collation = bloomFilterSet->collation;
    dataType = bloomFilterSet->dataType;
    hasMM = bloomFilterSet->hasMM;
    bitSet = New(CurrentMemoryContext) BitSet(bloomFilterSet->data, bloomFilterSet->length);
    /* set the MinMax value. */
    setMinMaxValueByBloomFilterSet(bloomFilterSet);

    maxNumValues = bloomFilterSet->maxNumValues;
    numBits = bloomFilterSet->numBits;
    numHashFunctions = bloomFilterSet->numHashFunctions;
    numValues = bloomFilterSet->numValues;
    startupEntries = bloomFilterSet->startupEntries;
    type = bloomFilterSet->bfType;
    typeMod = bloomFilterSet->typeMod;

    StringInfo str = makeStringInfo();

    if (0 != bloomFilterSet->length) {
        elog(DEBUG1, "rebuild bloomfilter:: data");
        const uint64* tempData = bitSet->getData();
        appendStringInfo(str, "\n");
        for (uint64 i = 0; i < bloomFilterSet->length; i++) {
            if (0 != tempData[i]) {
                appendStringInfo(str, "not zore value in position %lu , value %lu,\n", i, tempData[i]);
            }
        }
    }
    elog(DEBUG1, "%s", str->data);

    valuePositions = (ValueBit*)palloc0(startupEntries * sizeof(ValueBit));

    resetStringInfo(str);
    elog(DEBUG1, "rebuild bloomfilter:: valuePositions");
    appendStringInfo(str, "\n");
    for (uint64 entryNum = 0; entryNum < startupEntries; entryNum++) {
        for (int funNum = 0; funNum < MAX_HASH_FUNCTIONS; funNum++) {
            valuePositions[entryNum].position[funNum] = bloomFilterSet->valuePositions[entryNum].position[funNum];
            appendStringInfo(str, "%d, ", valuePositions[entryNum].position[funNum]);
        }
        appendStringInfo(str, "\n");
    }

    elog(DEBUG1, "%s", str->data);
    pfree_ext(str->data);
}

}  // namespace filter
