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
 * bloom_filter.h
 * 
 * 
 * 
 * IDENTIFICATION
 *        src/include/utils/bloom_filter.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef BLOOM_FILTER_H_
#define BLOOM_FILTER_H_

#include "postgres.h"
#include "knl/knl_variable.h"
#include "nodes/nodes.h"

#define SATISFY_BLOOM_FILTER(dataType)                                                                     \
    (dataType == INT2OID || dataType == INT4OID || dataType == INT8OID || dataType == FLOAT4OID ||         \
        dataType == FLOAT8OID || dataType == VARCHAROID || dataType == BPCHAROID || dataType == TEXTOID || \
        dataType == CLOBOID)
#define DEFAULT_ORC_BLOOM_FILTER_ENTRIES 10000
#define MAX_HASH_FUNCTIONS 4
#define LSB_IDENTIFY 6
#define DEFAULT_FPP 0.05  // false positive

typedef enum {
    EQUAL_BLOOM_FILTER,    /* for '=' predicate */
    HASHJOIN_BLOOM_FILTER, /* for hashjoin */
    IN_BLOOM_FILTER        /* for in/any array */
} BloomFilterType;

typedef struct ValueBit {
    uint16 position[MAX_HASH_FUNCTIONS];
} ValueBit;

/*
 * Define the following struct in order to pushdown bloomfilter to computing pool.
 * Thr bloomfilter to be stored type is HASHJOIN_BLOOM_FILTER .
 */
typedef struct BloomFilterSet {
    NodeTag type;
    /* store the BitSet info. */
    uint64* data;
    uint64 length;
    /* store the BloomFilterImpl info */
    uint64 numBits;
    uint64 numHashFunctions;
    uint64 numValues;
    uint64 maxNumValues;
    uint64 startupEntries;
    ValueBit* valuePositions;
    int64 minIntValue;    /* data type to be storeed is one of int2, int4, int8. */
    float8 minFloatValue; /* data type to be storeed is one float4, float8. */
    char* minStringValue; /* data type to be stored is one of varcha, bpchar, text, clob. */
    int64 maxIntValue;    /* data type to be storeed is one of int2, int4, int8. */
    float8 maxFloatValue; /* data type to be storeed is one float4, float8. */
    char* maxStringValue; /* data type to be stored is one of varcha, bpchar, text, clob. */
    bool addMinMax;
    bool hasMM;
    BloomFilterType bfType;
    Oid dataType;
    int32 typeMod;
    Oid collation;
} BloomFilterSet;

namespace filter {
class BloomFilter : public BaseObject {
public:
    virtual ~BloomFilter();

    /* Get the length of the uint64 array. */
    virtual uint64 getLength() const = 0;

    /* Get the number of bits in the inner set. */
    virtual uint64 getBitSize() const = 0;

    /* Get the number of hash functions used in the current bloom filter. */
    virtual uint64 getNumHashFunctions() const = 0;

    /* Get the number of values added into the current bloom filter. */
    virtual uint64 getNumValues() const = 0;

    /* Get the bitset(uint64 array) in the current bloom filter. */
    virtual const uint64* getBitSet() const = 0;

    /* Get the data type of the current bloom filter. */
    virtual Oid getDataType() const = 0;
    virtual int32 getTypeMod() const = 0;

    /*
     * Get the type of the current bloom filter: EQUAL_BLOOM_FILTER,
     * HASHJOIN_BLOOM_FILTER or IN_BLOOM_FILTER.
     */
    virtual BloomFilterType getType() const = 0;

    /*
     * Get the flag which indicates whether the current bloom filter
     * has min/max statistics.
     */
    virtual bool hasMinMax() const = 0;

    /*
     * Get the minimum value of all the values added to the
     * current blomm filter.
     */
    virtual Datum getMin() const = 0;

    /*
     * Get the maximum value of all the values added to the
     * current blomm filter.
     */
    virtual Datum getMax() const = 0;

    /*
     * Add a datum into the current bloom filter.
     * @_in_param val: The datum to be added.
     */
    virtual void addDatum(Datum val) = 0;

    /*
     * Add a value of any supported data type into the current
     * bloom filter.
     * @_in_param value: The value to be added.
     */
    template <typename T>
    void addValue(T value)
    {}

    /*
     * Check if the current bloom filter includes another datum.
     * @_in_param value: The datum to be checked.
     * @return true if the value is included in the bloom filter
     *      or return false.
     */
    virtual bool includeDatum(Datum value) const = 0;

    /*
     * Check if the current bloom filter includes a value of any
     * supported data type.
     * @_in_param value: The value to be checked.
     * @return true if the value is included in the bloom filter
     *      or return false.
     */
    template <typename T>
    bool includeValue(T value) const
    {
        return true;
    }

    /*
     * Check if the inner bloom filter is included in a bit set.
     * If there is only one value stored in the inner bloom filter, call the
     * native singleIncludedIn function. The single value is included in the
     * bit set only when all the four bits of the value are set 1 in the bit
     * set. If there are mulitple values stored in the inner bloom filter,
     * check each of the values to see if there is at least one which is
     * included in the bit set. The inner bloom filter does not refute the bit
     * set unless on one match.
     * @_in_param that: The bit set to be checked.
     * @return true if the inner bloom filter is included in the bit set.
     */
    virtual bool includedIn(const uint64* that) const = 0;

    /*
     * Check if the inner bloom fitler is included in another bloom filter(we
     * name it the outer bloom fitler here for easy).
     * If there is only one value stored in the inner bloom filter, call the
     * native singleIncludedIn function. The single value is included in the
     * outer bloom filter only when all the four bits of the value are set 1 in
     * the outer bloom filter. If there are mulitple values stored in the inner
     * bloom filter, check each of the values to see if there is at least one
     * which is included in the outer bloom fitler. The inner bloom filter does
     * not refute the outer bloom filter unless on one match.
     * @_in_param that: The bloom filter to be checked.
     * @return true if the inner bloom filter is included in the outer one.
     */
    virtual bool includedIn(const BloomFilter& that) const = 0;

    /* Union another bloom filter with the current bloom filter. */
    virtual void unionAll(const BloomFilter& that) = 0;

    /* Intersect another bloom filter with the current one. */
    virtual void intersectAll(const BloomFilter& that) = 0;

    /* Reset the current bloom filter. */
    virtual void reset() = 0;
    /* make one bloomfilterSet struct in order to pushdown bloomfilter inot to computing pool.*/
    virtual BloomFilterSet* makeBloomFilterSet() = 0;
    /* Fill the bloomfilter element by bloomFilterSet. */
    virtual void rebuildBloomFilterValue(BloomFilterSet* bloomFilterSet) = 0;

    /* Function pointer point to jitted_bf_addLong machine code */
    char* jitted_bf_addLong;

    /* Function pointer point to jitted_bf_includeLong machine code */
    char* jitted_bf_incLong;

    /*
     * Check if the current bloom filter includes an int64 value.
     * @_in_param value: The int64 value to be checked.
     * @return true if the value is included in the bloom filter
     *      or return false.
     */
    virtual bool includeLong(int64 value) const = 0;

protected:
    /* Add an int64 value into the inner bloom filter. */
    virtual void addLong(int64 val) = 0;

    /* Add a double value into the inner bloom filter. */
    virtual void addDouble(double val) = 0;

    /* Add a string into the inner bloom filter. */
    virtual void addString(const char* val) = 0;

    /*
     * Check if the current bloom filter includes a double value.
     * @_in_param value: The double value to be checked.
     * @return true if the value is included in the bloom filter
     *      or return false.
     */
    virtual bool includeDouble(double value) const = 0;

    /*
     * Check if the current bloom filter includes a string value.
     * @_in_param value: The string value to be checked.
     * @return true if the value is included in the bloom filter
     *      or return false.
     */
    virtual bool includeString(const char* value) const = 0;
};

/*
 * Bare metal bit set implementation. For performance reasons, this implementation does not check
 * for index bounds nor expand the bit set size if the specified index is greater than the size.
 */
class BitSet : public BaseObject {
public:
    BitSet(const MemoryContext& ctx, const uint64 bits);
    BitSet(uint64* _data, uint64 _length);
    virtual ~BitSet();
    void set(uint64 index);
    bool get(uint64 index) const;
    uint64 getLength() const;
    const uint64* getData() const;
    void unionAll(const uint64* array);
    void intersectAll(const uint64* array);
    void clear();

public:
    uint64* data;
    uint64 length;
};

class Murmur3 {
public:
    static int64 hash64(const char* data, int32 length);

    /**
     * Murmur3 64-bit variant. Essentially, this is the MSB 8 bytes of the Murmur3 128-bit variant.
     * @param data   - input byte array with variable type const char*
     * @param length - length of array with variable type int32
     * @param seed   - seed  with variable type int32. (default is 0)
     * @return - hashcode with variable type int64
     */
    static int64 hash64(const char* data, int32 length, int32 seed);

    static const int64 getNullHashCode();

private:
    static int64 rotateLeft(int64 n, uint32 i);

    static int64 rotateRight(int64 n, uint32 i);

    static int64 fmix64(int64 h);

    // from 64-bit linear congruential generator
    static const int64 NULL_HASHCODE = 2862933555777941757L;

    // Constants for 128 bit variant
    static const int64 C1 = 0x87c37b91114253d5L;
    static const int64 C2 = 0x4cf5ad432745937fL;
    static const int32 R1 = 31;
    static const int32 R2 = 27;
    static const int32 R3 = 33;
    static const int32 M = 5;
    static const int32 N1 = 0x52dce729;
    static const int32 N2 = 0x38495ab5;

    static const int32 DEFAULT_SEED = 104729;
};

template <typename baseType>
class BloomFilterImpl : public BloomFilter {
public:
    BloomFilterImpl(MemoryContext& ctx, Oid dataType, int32 typeMod, Oid collation, BloomFilterType type,
        int64 expectedEntries, double fpp, bool addMinMax);
    BloomFilterImpl(MemoryContext& ctx, BloomFilterSet* bloomFilterSet);
    virtual ~BloomFilterImpl();

private:
    uint64 getLength() const;
    uint64 getBitSize() const;
    uint64 getNumHashFunctions() const;
    uint64 getNumValues() const;
    const uint64* getBitSet() const;
    Oid getDataType() const;
    BloomFilterType getType() const;
    int32 getTypeMod() const;
    bool hasMinMax() const;
    Datum getMin() const;
    Datum getMax() const;
    void addDatum(Datum val)
    {}
    void addLong(int64 val)
    {}
    void addDouble(double val)
    {}
    void addString(const char* val)
    {}
    bool includeDatum(Datum value) const
    {
        return true;
    }
    bool includeLong(int64 value) const
    {
        return true;
    }
    bool includeDouble(double value) const
    {
        return true;
    }
    bool includeString(const char* value) const
    {
        return true;
    }
    bool includedIn(const uint64* that) const;
    bool includedIn(const BloomFilter& that) const;
    void unionAll(const BloomFilter& that);
    void intersectAll(const BloomFilter& that);
    void reset();

    BloomFilterSet* makeBloomFilterSet();
    /* rebuild the element of BloomFilter. */
    void rebuildBloomFilterValue(BloomFilterSet* bloomFilterSet);

    /* set the min/max value by bloomFilterSet in the bloom filter. */
    void setMinMaxValueByBloomFilterSet(BloomFilterSet* bloomFilterSet){};
    /* set the min/max value for bloomFilterSet by bloom filter object. */
    void setMinMaxValueForBloomFilterSet(BloomFilterSet* bloomFilterSet){};

    /* Convert data from float into int32. */
    int32 floatToInt32(float x) const;

    /* Store the min max value in the bloom filter. */
    void setMinMax(baseType val);

    /* Add a value which has been hashed in the bloom filter. */
    void addHash(int64 hash64);

    /*
     * For integer(long) and float(double) data types, they are all converted
     * to long type before been hashed.
     */
    void addLongInternal(int64 val);

    /* Calucate the number of functions for hash. */
    uint64 optimalNumOfHashFunctions(int64 n, int m) const;

    /* Calculate the size of bit set which store the value. */
    uint64 optimalNumOfBits(int64 n, double p) const;

    /* Convert data from double into int64. */
    int64 doubleToInt64(double x) const;

    /* Thomas Wang's integer hash function */
    int64 getLongHash(int64 key) const;

    /* Check if a hashed value is included in the bloom filter. */
    bool includeHashInternal(int64 hash64) const;

    /*
     * Check if the current bloom filter which has only one element
     * is included in the other bitset.
     */
    bool singleIncludedIn(const uint64* that) const;

    /*
     * Check if the current bloom filter which has more than one elements
     * is included in the other bitset.
     */
    bool multiInCludedIn(const uint64* that) const;

    /* Convert raw value into datum. */
    Datum valueToDatum(baseType value) const
    {
        return 0;
    }

    /* Convert datum into raw value. */
    baseType datumToValue(Datum datum) const
    {
        return 0;
    }

    /*
     * Compare two raw values, return 1 if left is bigger than right,
     * return -1 if left is smaller than right, or return 0 if left
     * is equal to right.
     */
    int compareValue(baseType left, baseType right) const;

    /*
     * Use min/max value to check if the current bloom filter
     * is included in the other bloom filter.
     */
    bool minMaxCheck(const BloomFilter& that) const;

public:
    BitSet* bitSet;
    uint64 numBits;
    uint64 numHashFunctions;
    uint64 numValues;
    uint64 maxNumValues;
    uint64 startupEntries;
    ValueBit* valuePositions;
    baseType minValue;
    baseType maxValue;
    bool addMinMax;
    bool hasMM;
    BloomFilterType type;
    Oid dataType;
    int32 typeMod;
    Oid collation;
    MemoryContext context;
};

template <>
inline void BloomFilter::addValue<int64>(int64 value)
{
    addLong(value);
}

template <>
inline void BloomFilter::addValue<double>(double value)
{
    addDouble(value);
}

template <>
inline void BloomFilter::addValue<char*>(char* value)
{
    addString(value);
}

template <>
inline bool BloomFilter::includeValue<int64>(int64 value) const
{
    return includeLong(value);
}

template <>
inline bool BloomFilter::includeValue<double>(double value) const
{
    return includeDouble(value);
}

template <>
inline bool BloomFilter::includeValue<char*>(char* value) const
{
    return includeString(value);
}

/*
 * Create a new bloom filter with special datatype, typemod, collation, max
 * number of values to be added and the flag controlling if min/max statistics
 * need to be added.
 */
BloomFilter* createBloomFilter(
    Oid dataType, int32 typeMod, Oid collation, BloomFilterType type, int64 maxNumValues, bool addMinMax);
/*
 * Create a new bloom filter object with bloomFilterSet struct.
 */
BloomFilter* createBloomFilter(BloomFilterSet* bloomFilterSet);

}  // namespace filter
#endif /* ORC_QUERY_H_ */
