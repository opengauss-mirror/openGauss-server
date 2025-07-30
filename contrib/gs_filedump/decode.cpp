#include "gs_filedump.h"
#include "decode.h"
#include <cstring>
#include <cctype>
#include <cstdio>
#include <lib/stringinfo.h>
#include <access/tupmacs.h>
#include <access/tuptoaster.h>
#include <datatype/timestamp.h>

static bool g_itemIsNull;
static unsigned int g_itemSize;
static int g_ignoreLocation = -1;
static int DelEndZero(int num);

static int ReadStringFromToast(const char *buffer, unsigned int buffSize, unsigned int *outSize,
                               int (*parseValue)(const char *, int));

static int DecodeSmallint(const char *buffer, unsigned int buffSize, unsigned int *outSize);

static int DecodeInt(const char *buffer, unsigned int buffSize, unsigned int *outSize);

static int DecodeUint(const char *buffer, unsigned int buffSize, unsigned int *outSize);

static int DecodeUint64(const char *buffer, unsigned int buffSize, unsigned int *outSize);

static int DecodeBigint(const char *buffer, unsigned int buffSize, unsigned int *outSize);

static int DecodeTime(const char *buffer, unsigned int buffSize, unsigned int *outSize);

static int DecodeTimetz(const char *buffer, unsigned int buffSize, unsigned int *outSize);

static int DecodeDate(const char *buffer, unsigned int buffSize, unsigned int *outSize);

static int DecodeTimestampInternal(const char *buffer, unsigned int buffSize, unsigned int *outSize, bool withTimezone);

static int DecodeTimestamp(const char *buffer, unsigned int buffSize, unsigned int *outSize);

static int DecodeTimestamptz(const char *buffer, unsigned int buffSize, unsigned int *outSize);

static int DecodeFloat4(const char *buffer, unsigned int buffSize, unsigned int *outSize);

static int DecodeFloat8(const char *buffer, unsigned int buffSize, unsigned int *outSize);

static int DecodeBool(const char *buffer, unsigned int buffSize, unsigned int *outSize);

static int DecodeUUID(const char *buffer, unsigned int buffSize, unsigned int *outSize);

static int DecodeMacaddr(const char *buffer, unsigned int buffSize, unsigned int *outSize);

static int DecodeString(const char *buffer, unsigned int buffSize, unsigned int *outSize);

static int DecodeChar(const char *buffer, unsigned int buffSize, unsigned int *outSize);

static int DecodeName(const char *buffer, unsigned int buffSize, unsigned int *outSize);

static int DecodeNumeric(const char *buffer, unsigned int buffSize, unsigned int *outSize);

static int ExtractData(const char *buffer, unsigned int buffSize, unsigned int *outSize,
                       int (*parseValue)(const char *, int));

static int DecodeIgnore(const char *buffer, unsigned int buffSize, unsigned int *outSize);

static int g_ncallbacks = 0;
static DecodeCallbackT g_callbacks[ATTRTYPES_STR_MAX_LEN / 2] = {nullptr};

static ParseCallbackTableItem g_callbackTable[] = {
    {"smallserial", &DecodeSmallint},
    {"smallint", &DecodeSmallint},
    {"int", &DecodeInt},
    {"oid", &DecodeUint},
    {"xid", &DecodeUint64},
    {"serial", &DecodeInt},
    {"bigint", &DecodeBigint},
    {"bigserial", &DecodeBigint},
    {"time", &DecodeTime},
    {"timetz", &DecodeTimetz},
    {"date", &DecodeDate},
    {"timestamp", &DecodeTimestamp},
    {"timestamptz", &DecodeTimestamptz},
    {"real", &DecodeFloat4},
    {"float4", &DecodeFloat4},
    {"float8", &DecodeFloat8},
    {"float", &DecodeFloat8},
    {"bool", &DecodeBool},
    {"uuid", &DecodeUUID},
    {"macaddr", &DecodeMacaddr},
    {"name", &DecodeName},
    {"numeric", &DecodeNumeric},
    {"char", &DecodeChar},
    {"~", &DecodeIgnore},

    /* internally all string types are stored the same way */
    {"charn", &DecodeString},
    {"varchar", &DecodeString},
    {"varcharn", &DecodeString},
    {"text", &DecodeString},
    {"json", &DecodeString},
    {"xml", &DecodeString},
    {NULL, nullptr},
};

static StringInfoData copyString;
static bool g_copyStringInitDone = false;

/* Used by some PostgreSQL macro definitions */
void ExceptionalCondition(const char *conditionName, const char *errorType, const char *fileNameErr, int lineNumber)
{
    printf("Exceptional condition: name = %s, type = %s, fname = %s, line = %d\n",
           conditionName ? conditionName : "(NULL)", errorType ? errorType : "(NULL)",
           fileNameErr ? fileNameErr : "(NULL)", lineNumber);
    exit(1);
}

static int DelEndZero(int num)
{
    int numRes = num;
    while (numRes % DECIMAL_BASE == 0) {
        numRes /= DECIMAL_BASE;
    }
    return numRes;
}

/* Append given string to current COPY line */
static void CopyAppend(const char *str)
{
    if (!g_copyStringInitDone) {
        initStringInfo(&copyString);
        g_copyStringInitDone = true;
    }

    /* Caller probably wanted just to init copyString */
    if (!str) {
        return;
    }

    if (copyString.data[0] != '\0') {
        appendStringInfoString(&copyString, "\t");
    }
    appendStringInfoString(&copyString, str);
}

/*
 * Append given string to current COPY line and encode special symbols
 * like \r, \n, \t and \\.
 */
static int CopyAppendEncode(const char *str, int origLen)
{
    std::string encoded;
    for (int i = 0; i < origLen; ++i, ++str) {
        switch (*str) {
            case '\0': encoded += "\\0"; break;
            case '\r': encoded += "\\r"; break;
            case '\n': encoded += "\\n"; break;
            case '\t': encoded += "\\t"; break;
            case '\\': encoded += "\\\\"; break;
            default:   encoded += *str; break;
        }
    }
    CopyAppend(encoded.c_str());
    return RETURN_SUCCESS;
}

/*
 * Decode a numeric type and append the result to current COPY line
 */

static int HandleSpecialNumeric(struct NumericData *numericData)
{
    if (NUMERIC_IS_NINF(numericData)) {
        CopyAppend("-Infinity");
    } else if (NUMERIC_IS_PINF(numericData)) {
        CopyAppend("Infinity");
    } else if (NUMERIC_IS_NAN(numericData)) {
        CopyAppend("NaN");
    }
    return RETURN_SUCCESS;
}

static void AppendIntegerPart(NumericDigit *digitsArray, int numberOfDigits, int weight, char **currentPosition)
{
    for (int digitIndex = 0; digitIndex <= weight; digitIndex++) {
        NumericDigit digit = (digitIndex < numberOfDigits) ? digitsArray[digitIndex] : 0;

        bool shouldOutputDigit = (digitIndex > 0);
        NumericDigit firstDigit = digit / DECIMAL_THOUSAND;
        digit -= firstDigit * DECIMAL_THOUSAND;
        shouldOutputDigit |= (firstDigit > 0);
        if (shouldOutputDigit) {
            *(*currentPosition)++ = firstDigit + '0';
        }

        firstDigit = digit / DECIMAL_HUNDRED;
        digit -= firstDigit * DECIMAL_HUNDRED;
        shouldOutputDigit |= (firstDigit > 0);
        if (shouldOutputDigit) {
            *(*currentPosition)++ = firstDigit + '0';
        }

        firstDigit = digit / DECIMAL_BASE;
        digit -= firstDigit * DECIMAL_BASE;
        shouldOutputDigit |= (firstDigit > 0);
        if (shouldOutputDigit) {
            *(*currentPosition)++ = firstDigit + '0';
        }

        *(*currentPosition)++ = digit + '0';
    }
}

static void AppendDecimalPart(NumericDigit *digitsArray, int numberOfDigits, int decimalScale, char **currentPosition)
{
    if (decimalScale <= 0) {
        return;
    }

    *(*currentPosition)++ = '.';
    char *endPosition = (*currentPosition) + decimalScale;

    for (int index = 0; index < decimalScale; index += DEC_DIGITS) {
        NumericDigit digit = (index < numberOfDigits) ? digitsArray[index] : 0;
        NumericDigit firstDigit = digit / DECIMAL_THOUSAND;
        digit -= firstDigit * DECIMAL_THOUSAND;
        *(*currentPosition)++ = firstDigit + '0';

        firstDigit = digit / DECIMAL_HUNDRED;
        digit -= firstDigit * DECIMAL_HUNDRED;
        *(*currentPosition)++ = firstDigit + '0';

        firstDigit = digit / DECIMAL_BASE;
        digit -= firstDigit * DECIMAL_BASE;
        *(*currentPosition)++ = firstDigit + '0';

        *(*currentPosition)++ = digit + '0';
    }

    *currentPosition = endPosition;
}

static int BuildNumericString(struct NumericData *numericData, int numericSize)
{
    int sign = NUMERIC_SIGN(numericData);
    int weight = NUMERIC_WEIGHT(numericData);
    int decimalScale = NUMERIC_DSCALE(numericData);
    int numberOfDigits = numericSize / sizeof(NumericDigit);
    NumericDigit *digitsArray = (NumericDigit *)((char *)numericData + NUMERIC_HEADER_SIZE(numericData));

    int startIndex = std::max(1, (weight + 1) * DEC_DIGITS);

    char *stringRepresentation = static_cast<char *>(malloc(startIndex + decimalScale + DEC_DIGITS + 2));
    if (!stringRepresentation) {
        perror("Memory allocation failed for stringRepresentation");
        return MEMORY_ALL_FAILED;
    }

    char *currentPosition = stringRepresentation;

    if (sign == NUMERIC_NEG) {
        *currentPosition++ = '-';
    }

    AppendIntegerPart(digitsArray, numberOfDigits, weight, &currentPosition);
    AppendDecimalPart(digitsArray, numberOfDigits, decimalScale, &currentPosition);

    *currentPosition = '\0';
    CopyAppend(stringRepresentation);
    free(stringRepresentation);

    return RETURN_SUCCESS;
}

static int CopyAppendNumeric(const char *buffer, int numericSize)
{
    if (numericSize == 0 || numericSize > MAXOUTPUTLEN) {
        fprintf(stderr, "Error: invalid allocation size %d\n", numericSize);
        return ATTRIBUTE_SIZE_ERROR;
    }
    struct NumericData *numericData = static_cast<struct NumericData *>(malloc(numericSize));

    if (!numericData) {
        fprintf(stderr, "CopyAppendNumeric: Memory allocation failed\n");
        return MEMORY_ALL_FAILED;
    }

    errno_t rc = memcpy_s((char *)numericData, numericSize, buffer, numericSize);
    securec_check(rc, "\0", "\0");

    int result = RETURN_SUCCESS;

    if (NUMERIC_IS_SPECIAL(numericData)) {
        result = HandleSpecialNumeric(numericData);
    } else if (numericSize == static_cast<int>(NUMERIC_HEADER_SIZE(numericData))) {
        CopyAppendFmt("%d", 0);
    } else {
        result = BuildNumericString(numericData, numericSize);
    }
    free(numericData);
    return RETURN_SUCCESS;
}

/* Discard accumulated COPY line */
static void CopyClear(void)
{
    /* Make sure init is done */
    CopyAppend(nullptr);

    resetStringInfo(&copyString);
}

/* Output and then clear accumulated COPY line */
static void CopyFlush(void)
{
    /* Make sure init is done */
    CopyAppend(nullptr);

    printf("COPY: %s\n", copyString.data);
    CopyClear();
}

/*
 * Add a callback to `g_callbacks` table for given type name
 *
 * Arguments:
 *   type	   - name of a single type, always lowercase
 *
 * Return value is:
 *   RETURN_SUCCESS	        - no error
 *	 ATTRIBUTE_SIZE_ERROR	- invalid type name
 */
static int AddTypeCallback(const char *type)
{
    if (*type == '\0') { /* ignore empty strings */
        return RETURN_SUCCESS;
    }

    for (int idx = 0; g_callbackTable[idx].name != nullptr; idx++) {
        if (strcmp(g_callbackTable[idx].name, type) == 0) {
            g_callbacks[g_ncallbacks] = g_callbackTable[idx].callback;
            if ((strcmp(type, "~") == 0) & (g_ignoreLocation == -1)) {
                g_ignoreLocation = g_ncallbacks;
            }
            g_ncallbacks++;
            return RETURN_SUCCESS;
        }
    }
    fprintf(stderr, "Error: type <%s> doesn't exist or is not currently supported\n", type);
    fprintf(stderr, "Full list of known types: ");

    for (int idx = 0; g_callbackTable[idx].name != nullptr; idx++) {
        fprintf(stderr, "%s ", g_callbackTable[idx].name);
    }
    fprintf(stderr, "\n");
    return ATTRIBUTE_SIZE_ERROR;
}

/*
 * Decode attribute types string like "int,timestamp,bool,uuid"
 *
 * Arguments:
 *   str		- types string
 * Return value is:
 *   RETURN_SUCCESS	        - if string is valid
 *	 ATTRIBUTE_SIZE_ERROR	- if string is invalid
 */
int ParseAttributeTypesString(const char *str)
{
    size_t len = strlen(str);
    if (len > ATTRTYPES_STR_MAX_LEN) {
        fprintf(stderr, "Error: attribute types string is longer than %lu characters!\n", ATTRTYPES_STR_MAX_LEN);
        return ATTRIBUTE_SIZE_ERROR;
    }

    char *attrtypes = static_cast<char *>(malloc(len + 1));
    if (!attrtypes) {
        fprintf(stderr, "Memory allocation failed\n");
        return MEMORY_ALL_FAILED;
    }

    errno_t rc = strcpy_s(attrtypes, len + 1, str);
    securec_check(rc, "", "");
    for (size_t i = 0; i < len; i++) {
        attrtypes[i] = tolower(attrtypes[i]);
    }

    char *token;
    char *saveptr;
    token = strtok_r(attrtypes, ",", &saveptr);

    while (token != nullptr) {
        if (AddTypeCallback(token) < 0) {
            free(attrtypes);
            return ATTRIBUTE_SIZE_ERROR;
        }
        token = strtok_r(nullptr, ",", &saveptr);
    }

    free(attrtypes);
    return RETURN_SUCCESS;
}

/*
 * Convert Julian day number (JDN) to a date.
 * Copy-pasted from src/common/backend/utils/adt/datetime.cpp
 */
void J2date(int jd, int *year, int *month, int *day)
{
    unsigned int julian;
    unsigned int quad;
    unsigned int extra;
    int yearOffset;

    julian = jd + JULIAN_DAY_BASE_ADJUSTMENT;

    quad = julian / DAYS_PER_400_YEARS;
    extra = (julian - quad * DAYS_PER_400_YEARS) * QUAD_YEAR_ADJUSTMENT + EXTRA_YEAR_ADJUSTMENT;
    julian += INITIAL_JULIAN_ADJUSTMENT + quad * EXTRA_YEAR_ADJUSTMENT + extra / DAYS_PER_400_YEARS;

    quad = julian / DAYS_PER_4_YEARS;
    julian -= quad * DAYS_PER_4_YEARS;

    yearOffset = julian * QUAD_YEAR_ADJUSTMENT / DAYS_PER_4_YEARS;
    if (yearOffset != 0) {
        julian = ((julian + DAY_OFFSET_NON_LEAP_YEAR) % DAYS_PER_NORMAL_YEAR) + DAY_ADJUSTMENT_BEFORE_MONTH_CONVERT;
    } else {
        julian = ((julian + DAY_OFFSET_LEAP_YEAR) % DAYS_PER_LEAP_YEAR) + DAY_ADJUSTMENT_BEFORE_MONTH_CONVERT;
    }

    yearOffset += quad * QUAD_YEAR_ADJUSTMENT;
    *year = yearOffset - YEAR_OFFSET_GREGORIAN;

    quad = julian * MONTH_CONVERSION_FACTOR / MONTH_CONVERSION_SCALE;
    *day = julian - DAYS_TO_MONTH_ADJUSTMENT * quad / MONTH_CONVERSION_DIVISOR;
    *month = (quad + MONTH_ADJUSTMENT_OFFSET) % MONTHS_PER_YEAR + 1;
}

template <typename T>
static int DecodeIntegral(const char *buffer, unsigned int buffSize, unsigned int *outSize, const char *FORMAT)
{
    unsigned int delta = 0;
    if (!g_isUHeap) {
        const char *newBuffer = reinterpret_cast<const char *>(LONGALIGN(buffer));

        if (sizeof(T) <= 4) {
            newBuffer = reinterpret_cast<const char *>(INTALIGN(buffer));
        }
        if (sizeof(T) <= 2) {
            newBuffer = reinterpret_cast<const char *>(SHORTALIGN(buffer));
        }
        delta = static_cast<unsigned int>(((uintptr_t)newBuffer - (uintptr_t)buffer));

        CHECK_BUFFER_DELTA_SIZE(buffSize, delta);
        buffSize -= delta;
        buffer = newBuffer;
    }
    CHECK_BUFFER_SIZE(buffSize, sizeof(T));
    if (!g_itemIsNull) {
        CopyAppendFmt(FORMAT, (*reinterpret_cast<const T *>(buffer)));
    }
    *outSize = sizeof(T) + delta;
    return static_cast<int>(DecodeResult::DECODE_SUCCESS);
}
/* Decode a smallint type */
static int DecodeSmallint(const char *buffer, unsigned int buffSize, unsigned int *outSize)
{
    return DecodeIntegral<int16>(buffer, buffSize, outSize, INT16_FORMAT);
}
/* Decode an int type */
static int DecodeInt(const char *buffer, unsigned int buffSize, unsigned int *outSize)
{
    return DecodeIntegral<int32>(buffer, buffSize, outSize, INT32_FORMAT);
}
/* Decode an unsigned int type */
static int DecodeUint(const char *buffer, unsigned int buffSize, unsigned int *outSize)
{
    return DecodeIntegral<uint32>(buffer, buffSize, outSize, UINT32_FORMAT);
}
/* Decode an unsigned int type */
static int DecodeUint64(const char *buffer, unsigned int buffSize, unsigned int *outSize)
{
    return DecodeIntegral<uint64>(buffer, buffSize, outSize, UINT64_FORMAT);
}
/* Decode a bigint type */
static int DecodeBigint(const char *buffer, unsigned int buffSize, unsigned int *outSize)
{
    return DecodeIntegral<int64>(buffer, buffSize, outSize, INT64_FORMAT);
}

static void FormatTime(int64 timestamp_sec, int64 timestamp)
{
    if (timestamp % MICROSECONDS_PER_SECOND != 0) {
        CopyAppendFmt("%02ld:%02ld:%02ld.%ld",
                      timestamp_sec / SECS_PER_MINUTE / MINS_PER_HOUR,
                      (timestamp_sec / SECS_PER_MINUTE) % MINS_PER_HOUR,
                      timestamp_sec % SECS_PER_MINUTE,
                      DelEndZero(timestamp % MICROSECONDS_PER_SECOND));
    } else {
        CopyAppendFmt("%02ld:%02ld:%02ld",
                      timestamp_sec / SECS_PER_MINUTE / MINS_PER_HOUR,
                      (timestamp_sec / SECS_PER_MINUTE) % MINS_PER_HOUR,
                      timestamp_sec % SECS_PER_MINUTE);
    }
}

static void FormatTimeWithTimezone(int64 timestamp_sec, int32 tz_min, int64 timestamp)
{
    if (timestamp % MICROSECONDS_PER_SECOND != 0) {
        if (tz_min % SECS_PER_MINUTE != 0) {
            CopyAppendFmt("%02ld:%02ld:%02ld.%ld%c%02d:%02d",
                          timestamp_sec / SECS_PER_MINUTE / MINS_PER_HOUR,
                          (timestamp_sec / SECS_PER_MINUTE) % MINS_PER_HOUR,
                          timestamp_sec % SECS_PER_MINUTE,
                          DelEndZero(timestamp % MICROSECONDS_PER_SECOND),
                          (tz_min >= 0 ? '+' : '-'),
                          abs(tz_min / MINS_PER_HOUR),
                          abs(tz_min % MINS_PER_HOUR));
        } else {
            CopyAppendFmt("%02ld:%02ld:%02ld.%ld%c%02d",
                          timestamp_sec / SECS_PER_MINUTE / MINS_PER_HOUR,
                          (timestamp_sec / SECS_PER_MINUTE) % MINS_PER_HOUR,
                          timestamp_sec % SECS_PER_MINUTE,
                          DelEndZero(timestamp % MICROSECONDS_PER_SECOND),
                          (tz_min >= 0 ? '+' : '-'),
                          abs(tz_min / MINS_PER_HOUR));
        }
    } else {
        if (tz_min % SECS_PER_MINUTE != 0) {
            CopyAppendFmt("%02ld:%02ld:%02ld%c%02d:%02d",
                          timestamp_sec / SECS_PER_MINUTE / MINS_PER_HOUR,
                          (timestamp_sec / SECS_PER_MINUTE) % MINS_PER_HOUR,
                          timestamp_sec % SECS_PER_MINUTE,
                          (tz_min >= 0 ? '+' : '-'),
                          abs(tz_min / MINS_PER_HOUR),
                          abs(tz_min % MINS_PER_HOUR));
        } else {
            CopyAppendFmt("%02ld:%02ld:%02ld%c%02d",
                          timestamp_sec / SECS_PER_MINUTE / MINS_PER_HOUR,
                          (timestamp_sec / SECS_PER_MINUTE) % MINS_PER_HOUR,
                          timestamp_sec % SECS_PER_MINUTE,
                          (tz_min >= 0 ? '+' : '-'),
                          abs(tz_min / MINS_PER_HOUR));
        }
    }
}

template <bool WITH_TIMEZONE>
static int DecodeTimeTemplate(const char *buffer, unsigned int buffSize, unsigned int *outSize)
{
    if (WITH_TIMEZONE & g_itemIsNull) {
        return static_cast<int>(DecodeResult::DECODE_SUCCESS);
    }
    unsigned int delta = 0;
    int64 timestamp;
    int64 timestamp_sec;
    int32 tz_sec = 0;
    int32 tz_min = 0;

    if (!g_isUHeap) {
        const char *newBuffer = reinterpret_cast<const char *>(LONGALIGN(buffer));
        delta = static_cast<unsigned int>(((uintptr_t)newBuffer - (uintptr_t)buffer));
        CHECK_BUFFER_DELTA_SIZE(buffSize, delta);

        buffSize -= delta;
        buffer = newBuffer;
    }

    if (WITH_TIMEZONE & g_isUHeap) {
        unsigned int newBufferSize = g_itemSize - static_cast<unsigned int>(LONGALIGN(g_itemSize - buffSize));
        delta = static_cast<unsigned int>(buffSize - newBufferSize);

        CHECK_BUFFER_DELTA_SIZE(buffSize, delta);
        buffSize -= delta;
        buffer += delta;
    }

    if (WITH_TIMEZONE) {
        CHECK_BUFFER_SIZE(buffSize, (sizeof(int64) + sizeof(int32)));
        timestamp = *(int64 *)buffer;
        tz_sec = *(int32 *)(buffer + sizeof(int64));
        tz_min = -(tz_sec / 60);
        *outSize = sizeof(int64) + sizeof(int32) + delta;
    } else {
        CHECK_BUFFER_SIZE(buffSize, sizeof(int64));
        timestamp = *(int64 *)buffer;
        *outSize = sizeof(int64) + delta;
    }

    timestamp_sec = timestamp / MICROSECONDS_PER_SECOND;

    if (!g_itemIsNull) {
        if (WITH_TIMEZONE) {
            FormatTimeWithTimezone(timestamp_sec, tz_min, timestamp);
        } else {
            FormatTime(timestamp_sec, timestamp);
        }
    }
    return static_cast<int>(DecodeResult::DECODE_SUCCESS);
}

/* Decode a time type */
static int DecodeTime(const char *buffer, unsigned int buffSize, unsigned int *outSize)
{
    return DecodeTimeTemplate<false>(buffer, buffSize, outSize);
}

/* Decode a timetz type */
static int DecodeTimetz(const char *buffer, unsigned int buffSize, unsigned int *outSize)
{
    return DecodeTimeTemplate<true>(buffer, buffSize, outSize);
}

/* Decode a date type */
static int DecodeDate(const char *buffer, unsigned int buffSize, unsigned int *outSize)
{
    const char *newBuffer = reinterpret_cast<const char *>(INTALIGN(buffer));
    unsigned int delta = static_cast<unsigned int>((uintptr_t)newBuffer - (uintptr_t)buffer);
    int32 dateDelta;
    int32 julianDay;
    int32 year;
    int32 month;
    int32 day;

    CHECK_BUFFER_DELTA_SIZE(buffSize, delta);
    buffSize -= delta;
    buffer = newBuffer;

    CHECK_BUFFER_SIZE(buffSize, sizeof(int32));
    *outSize = sizeof(int32) + delta;

    dateDelta = *(int32 *)buffer;
    if (dateDelta == PG_INT32_MIN) {
        CopyAppend("-infinity");
        return static_cast<int>(DecodeResult::DECODE_SUCCESS);
    }
    if (dateDelta == PG_INT32_MAX) {
        CopyAppend("infinity");
        return static_cast<int>(DecodeResult::DECODE_SUCCESS);
    }

    julianDay = dateDelta + POSTGRES_EPOCH_JDATE;
    J2date(julianDay, &year, &month, &day);

    CopyAppendFmt("%04d-%02d-%02d%s", (year <= 0) ? -year + 1 : year, month, day, (year <= 0) ? " BC" : "");

    return static_cast<int>(DecodeResult::DECODE_SUCCESS);
}

/* Decode a timestamp type */
static int DecodeTimestampInternal(const char *buffer, unsigned int buffSize, unsigned int *outSize, bool withTimezone)
{
    int64 timestamp;
    int64 timestamp_sec;
    int32 julianDay;
    int32 year;
    int32 month;
    int32 day;
    unsigned int delta = 0;

    if (!g_isUHeap) {
        const char *newBuffer = reinterpret_cast<const char *>(LONGALIGN(buffer));
        delta = static_cast<unsigned int>((uintptr_t)newBuffer - (uintptr_t)buffer);

        CHECK_BUFFER_DELTA_SIZE(buffSize, delta);
        buffSize -= delta;
        buffer = newBuffer;
    }

    *outSize = sizeof(int64) + delta;
    timestamp = *(int64 *)buffer;

    if (timestamp == DT_NOBEGIN) {
        CopyAppend("-infinity");
        return static_cast<int>(DecodeResult::DECODE_SUCCESS);
    }
    if (timestamp == DT_NOEND) {
        CopyAppend("infinity");
        return static_cast<int>(DecodeResult::DECODE_SUCCESS);
    }

    julianDay = timestamp / USECS_PER_DAY;
    if (julianDay != 0) {
        timestamp -= julianDay * USECS_PER_DAY;
    }

    if (timestamp < INT64CONST(0)) {
        timestamp += USECS_PER_DAY;
        julianDay -= 1;
    }
    /* add offset to go from J2000 back to standard Julian date */
    julianDay += POSTGRES_EPOCH_JDATE;

    J2date(julianDay, &year, &month, &day);
    timestamp_sec = timestamp / MICROSECONDS_PER_SECOND;

    if (!g_itemIsNull) {
        if (timestamp % MICROSECONDS_PER_SECOND != 0) {
            CopyAppendFmt("%04d-%02d-%02d %02ld:%02ld:%02ld.%ld%s%s",
                (year <= 0) ? -year + 1 : year, month, day, timestamp_sec / SECS_PER_MINUTE / MINS_PER_HOUR,
                (timestamp_sec / SECS_PER_MINUTE) % MINS_PER_HOUR, timestamp_sec % SECS_PER_MINUTE,
                DelEndZero(timestamp % MICROSECONDS_PER_SECOND), withTimezone ? "+00" : "", (year <= 0) ? " BC" : "");
        } else {
            CopyAppendFmt("%04d-%02d-%02d %02ld:%02ld:%02ld%s%s",
                (year <= 0) ? -year + 1 : year, month, day, timestamp_sec / SECS_PER_MINUTE / MINS_PER_HOUR,
                (timestamp_sec / SECS_PER_MINUTE) % MINS_PER_HOUR, timestamp_sec % SECS_PER_MINUTE,
                withTimezone ? "+00" : "", (year <= 0) ? " BC" : "");
        }
    }
    return static_cast<int>(DecodeResult::DECODE_SUCCESS);
}

static int DecodeTimestamp(const char *buffer, unsigned int buffSize, unsigned int *outSize)
{
    return DecodeTimestampInternal(buffer, buffSize, outSize, false);
}

static int DecodeTimestamptz(const char *buffer, unsigned int buffSize, unsigned int *outSize)
{
    return DecodeTimestampInternal(buffer, buffSize, outSize, true);
}

template <typename T>
static int DecodeFloat(const char *buffer, unsigned int buffSize, unsigned int *outSize, const char *typeName)
{
    unsigned int delta = 0;
    if (!g_isUHeap) {
        const char *alignedBuffer = nullptr;
        if (strcmp(typeName, "float4") == 0) {
            alignedBuffer = reinterpret_cast<const char *>(INTALIGN(buffer));
        } else if (strcmp(typeName, "float8") == 0) {
            alignedBuffer = reinterpret_cast<const char *>(LONGALIGN(buffer));
        } else {
            alignedBuffer = buffer;
        }
        delta = static_cast<unsigned int>((uintptr_t)alignedBuffer - (uintptr_t)buffer);

        CHECK_BUFFER_DELTA_SIZE(buffSize, delta);
        buffSize -= delta;
        buffer = alignedBuffer;
    }

    CHECK_BUFFER_SIZE(buffSize, sizeof(T));

    if (!g_itemIsNull) {
        char copyFormatBuff[512];
        snprintf_s(copyFormatBuff, sizeof(copyFormatBuff), sizeof(copyFormatBuff) - 1, "%.*g",
            static_cast<int>(sizeof(T) * 2), *reinterpret_cast<const T*>(buffer));
        if (pg_strncasecmp(copyFormatBuff, "inf", strlen("inf")) == 0) {
            CopyAppend("Infinity");
        } else if (pg_strncasecmp(copyFormatBuff, "-inf", strlen("-inf")) == 0) {
            CopyAppend("-Infinity");
        } else if (pg_strncasecmp(copyFormatBuff, "nan", strlen("nan")) == 0) {
            CopyAppend("NaN");
        } else {
            CopyAppend(copyFormatBuff);
        }
    }
    *outSize = sizeof(T) + delta;
    return static_cast<int>(DecodeResult::DECODE_SUCCESS);
}

/* Decode a float4 type */
static int DecodeFloat4(const char *buffer, unsigned int buffSize, unsigned int *outSize)
{
    return DecodeFloat<float>(buffer, buffSize, outSize, "float4");
}

/* Decode a float8 type */
static int DecodeFloat8(const char *buffer, unsigned int buffSize, unsigned int *outSize)
{
    return DecodeFloat<double>(buffer, buffSize, outSize, "float8");
}

/* Decode an uuid type */
static int DecodeUUID(const char *buffer, unsigned int buffSize, unsigned int *outSize)
{
    if (g_itemIsNull) {
        return static_cast<int>(DecodeResult::DECODE_SUCCESS);
    }
    unsigned char uuid[16];

    CHECK_BUFFER_DELTA_SIZE(buffSize, sizeof(uuid));

    errno_t rc = memcpy_s(uuid, sizeof(uuid), buffer, sizeof(uuid));
    securec_check(rc, "\0", "\0");
    CopyAppendFmt("%02x%02x%02x%02x-%02x%02x-%02x%02x-%02x%02x-%02x%02x%02x%02x%02x%02x", uuid[0], uuid[1], uuid[2],
                  uuid[3], uuid[4], uuid[5], uuid[6], uuid[7], uuid[8], uuid[9], uuid[10], uuid[11], uuid[12], uuid[13],
                  uuid[14], uuid[15]);
    *outSize = sizeof(uuid);
    return static_cast<int>(DecodeResult::DECODE_SUCCESS);
}

/* Decode a macaddr type */
static int DecodeMacaddr(const char *buffer, unsigned int buffSize, unsigned int *outSize)
{
    if (g_itemIsNull) {
        return static_cast<int>(DecodeResult::DECODE_SUCCESS);
    }
    unsigned int delta = 0;
    const char *alignedBuffer = buffer;
    unsigned char macaddr[6];
    if (!g_isUHeap) {
        alignedBuffer = reinterpret_cast<const char *>(INTALIGN(buffer));
        delta = static_cast<unsigned int>((uintptr_t)alignedBuffer - (uintptr_t)buffer);
    } else {
        unsigned int newBufferSize = g_itemSize - static_cast<unsigned int>(INTALIGN(g_itemSize - buffSize));
        delta = static_cast<unsigned int>(buffSize - newBufferSize);
        alignedBuffer = buffer + delta;
    }

    CHECK_BUFFER_DELTA_SIZE(buffSize, delta);
    buffSize -= delta;

    CHECK_BUFFER_SIZE(buffSize, sizeof(macaddr));
    errno_t rc = memcpy_s(macaddr, sizeof(macaddr), alignedBuffer, sizeof(macaddr));
    securec_check(rc, "\0", "\0");
    CopyAppendFmt("%02x:%02x:%02x:%02x:%02x:%02x",
        macaddr[0], macaddr[1], macaddr[2],
        macaddr[3], macaddr[4], macaddr[5]);
    *outSize = sizeof(macaddr) + delta;
    return static_cast<int>(DecodeResult::DECODE_SUCCESS);
}

/* Decode a bool type */
static int DecodeBool(const char *buffer, unsigned int buffSize, unsigned int *outSize)
{
    CHECK_BUFFER_DELTA_SIZE(buffSize, sizeof(bool));

    if (!g_itemIsNull) {
        CopyAppend(*reinterpret_cast<const bool *>(buffer) ? "t" : "f");
    }

    *outSize = sizeof(bool);
    return static_cast<int>(DecodeResult::DECODE_SUCCESS);
}

/* Decode a name type (used mostly in catalog tables) */
static int DecodeName(const char *buffer, unsigned int buffSize, unsigned int *outSize)
{
    if (g_itemIsNull) {
        return static_cast<int>(DecodeResult::DECODE_SUCCESS);
    }
    CHECK_BUFFER_DELTA_SIZE(buffSize, NAMEDATALEN);
    CopyAppendEncode(buffer, strnlen(buffer, NAMEDATALEN));
    *outSize = NAMEDATALEN;
    return static_cast<int>(DecodeResult::DECODE_SUCCESS);
}

/*
 * Decode numeric type.
 */
static int DecodeNumeric(const char *buffer, unsigned int buffSize, unsigned int *outSize)
{
    int result = ExtractData(buffer, buffSize, outSize, &CopyAppendNumeric);
    return result;
}

/* Decode a char type */
static int DecodeChar(const char *buffer, unsigned int buffSize, unsigned int *outSize)
{
    CHECK_BUFFER_SIZE(buffSize, sizeof(char));

    if (!g_itemIsNull) {
        CopyAppendEncode(buffer, 1);
    }
    *outSize = 1;
    return static_cast<int>(DecodeResult::DECODE_SUCCESS);
}

/* Ignore all data left */
static int DecodeIgnore(const char *buffer, unsigned int buffSize, unsigned int *outSize)
{
    *outSize = buffSize;
    return static_cast<int>(DecodeResult::DECODE_SUCCESS);
}

/* Decode char(N), varchar(N), text, json or xml types */
static int DecodeString(const char *buffer, unsigned int buffSize, unsigned int *outSize)
{
    int result = ExtractData(buffer, buffSize, outSize, &CopyAppendEncode);
    return result;
}

/*
 * Align data, parse varlena header, detoast and decompress.
 * Last parameters responds for actual parsing according to type.
 */

/*
* 00000001 1-byte length word, unaligned, TOAST pointer
*/
static int HandleToastPointer(const char *dataStart, unsigned int buffSize, unsigned int *outSize,
                              int (*parseValue)(const char *, int), int padding)
{
    uint32 len = VARSIZE_EXTERNAL(dataStart);

    CHECK_BUFFER_DELTA_SIZE(buffSize, len);

    int result = 0;
    if (g_blockOptions & BLOCK_DECODE_TOAST) {
        result = ReadStringFromToast(dataStart, buffSize, outSize, parseValue);
    } else if (VARATT_IS_EXTERNAL_ONDISK(dataStart)) {
        varatt_external toast_ptr;
        VARATT_EXTERNAL_GET_POINTER(toast_ptr, dataStart);
        if (VARATT_EXTERNAL_IS_COMPRESSED(toast_ptr)) {
            CopyAppend("(TOASTED,pglz)");
        } else {
            CopyAppend("(TOASTED,uncompressed)");
        }
    } else {
        /* If tag is indirect or expanded, it was stored in memory. */
        CopyAppend("(TOASTED IN MEMORY)");
    }

    *outSize = padding + len;
    return result;
}

/*
* xxxxxxx1 1-byte length word, unaligned, uncompressed data (up to
* 126b) xxxxxxx is 1 + string length
*/
static int Handle1ByteInline(const char *dataStart, unsigned int buffSize, unsigned int *outSize,
                             int (*parseValue)(const char *, int), int padding)
{
    uint8 len = VARSIZE_1B(dataStart);
    CHECK_BUFFER_DELTA_SIZE(buffSize, len);

    int result = parseValue(dataStart + 1, len - 1);
    *outSize = padding + len;
    return result;
}

/*
* xxxxxx00 4-byte length word, aligned, uncompressed data (up to 1G)
*/
static int Handle4ByteUncompressed(const char *dataStart, unsigned int buffSize, unsigned int *outSize,
                                   int (*parseValue)(const char *, int), int padding)
{
    uint32 len = VARSIZE_4B(dataStart);
    CHECK_BUFFER_DELTA_SIZE(buffSize, len);

    int result = parseValue(dataStart + 4, len - 4);
    *outSize = padding + len;
    return result;
}

/*
* xxxxxx10 4-byte length word, aligned, *compressed* data (up to 1G)
*/
static int Handle4ByteCompressed(const char *dataStart, unsigned int buffSize, unsigned int *outSize,
                                 int (*parseValue)(const char *, int), int padding)
{
    uint32 len = VARSIZE_4B(dataStart);
    uint32 decompressed_len = VARRAWSIZE_4B_C(dataStart);
    if (decompressed_len == 0 || decompressed_len > MAXOUTPUTLEN) {
        fprintf(stderr, "Error: invalid allocation size %u\n", decompressed_len);
        return ATTRIBUTE_SIZE_ERROR;
    }

    char *decompressTmpBuff = static_cast<char *>(malloc(decompressed_len));
    if (!decompressTmpBuff) {
        perror("Memory allocation failed for decompressTmpBuff");
        return MEMORY_ALL_FAILED;
    }

    uint32 decompress_ret = pglz_decompress(VARDATA_4B_C(dataStart), len - 2 * sizeof(uint32),
                                            decompressTmpBuff, decompressed_len, true);
    if (decompress_ret != decompressed_len || decompress_ret < 0) {
        printf("WARNING: Corrupted toast data, unable to decompress.\n");
        CopyAppend("(inline compressed, corrupted)");
        *outSize = padding + len;
        free(decompressTmpBuff);
        return RETURN_SUCCESS;
    }

    int result = parseValue(decompressTmpBuff, decompressed_len);
    *outSize = padding + len;
    free(decompressTmpBuff);
    return result;
}

static int ExtractData(const char *buffer, unsigned int buffSize, unsigned int *outSize,
                       int (*parseValue)(const char *, int))
{
    if (g_itemIsNull) {
        return static_cast<int>(DecodeResult::DECODE_SUCCESS);
    }

    int padding = 0;
    const char *dataStart = buffer;

    /* Skip padding bytes. */
    while (*dataStart == 0x00) {
        padding++;
        buffSize--;
        dataStart++;
    }

    if (buffSize == 0) {
        return static_cast<int>(DecodeResult::DECODE_BUFF_SIZE_IS_ZERO);
    }

    if (VARATT_IS_1B_E(dataStart)) {
        return HandleToastPointer(dataStart, buffSize, outSize, parseValue, padding);
    }

    if (VARATT_IS_1B(dataStart)) {
        return Handle1ByteInline(dataStart, buffSize, outSize, parseValue, padding);
    }

    if (VARATT_IS_4B_U(dataStart) && buffSize >= 4) {
        return Handle4ByteUncompressed(dataStart, buffSize, outSize, parseValue, padding);
    }

    if (VARATT_IS_4B_C(dataStart) && buffSize >= 8) {
        return Handle4ByteCompressed(dataStart, buffSize, outSize, parseValue, padding);
    }

    return static_cast<int>(DecodeResult::DECODE_FAILURE);
}

/*
 * Try to decode a tuple using a types string provided previously.
 *
 * Arguments:
 *   tupleData   - pointer to the tuple data
 *   tupleSize   - tuple size in bytes
 */

void PrintDecodeError(const char *errorMsg, int currAttr)
{
    printf("Error: unable to decode a tuple, callback #%d returned: %s. Partial data: %s\n",
           currAttr + 1, errorMsg, copyString.data);
}

void HandleDecodeError(int errorCode, int currAttr)
{
    switch (errorCode) {
        case static_cast<int>(DecodeResult::DECODE_BUFF_SIZE_LESS_THAN_DELTA):
            PrintDecodeError("buffSize_LESS_THAN_delta", currAttr);
            break;
        case static_cast<int>(DecodeResult::DECODE_BUFF_SIZE_LESS_THAN_REQUIRED):
            PrintDecodeError("buffSize_LESS_THAN_required", currAttr);
            break;
        case static_cast<int>(DecodeResult::DECODE_BUFF_SIZE_IS_ZERO):
            PrintDecodeError("buffSize_IS_ZERO", currAttr);
            break;
        case static_cast<int>(DecodeResult::DECODE_FAILURE):
            PrintDecodeError("FAILURE", currAttr);
            break;
        default:
            printf("Error: unable to decode a tuple, callback #%d returned %d. Partial data: %s\n",
                   currAttr + 1, errorCode, copyString.data);
            break;
    }
}

void FormatDecode(const char *tupleData, unsigned int tupleSize)
{
    UHeapDiskTuple uheader = (UHeapDiskTuple)tupleData;
    HeapTupleHeader header = (HeapTupleHeader)tupleData;

    const char *data = nullptr;
    unsigned int size = 0;
    g_itemSize = tupleSize;
    CopyClear();

    if (g_isUHeap) {
        data = tupleData + uheader->t_hoff;
        size = tupleSize - uheader->t_hoff;
    } else {
        data = tupleData + header->t_hoff;
        size = tupleSize - header->t_hoff;
    }

    for (int currAttr = 0; currAttr < g_ncallbacks; currAttr++) {
        int ret;
        unsigned int processedSize = 0;

        if (g_isUHeap) {
            if ((uheader->flag & UHEAP_HAS_NULL) && att_isnull(currAttr, uheader->data)) {
                if (currAttr != g_ignoreLocation) {
                    CopyAppend("\\N");
                    g_itemIsNull = true;
                }
            } else if (size < 0) {
                printf("Error: unable to decode a tuple, no more bytes left. Partial data: %s\n", copyString.data);
                return;
            } else {
                g_itemIsNull = false;
            }
        } else {
            if ((header->t_infomask & HEAP_HASNULL) && att_isnull(currAttr, header->t_bits)) {
                if (currAttr != g_ignoreLocation) {
                    CopyAppend("\\N");
                    continue;
                }
            }
            if (size < 0) {
                printf("Error: unable to decode a tuple, no more bytes left. Partial data: %s\n", copyString.data);
                return;
            }
        }
        ret = g_callbacks[currAttr](data, size, &processedSize);
        if (ret < 0) {
            HandleDecodeError(ret, currAttr);
            return;
        }

        size -= processedSize;
        data += processedSize;
    }

    if (size != 0) {
        printf("Error: unable to decode a tuple, %u bytes left, 0 expected. Partial data: %s\n", size, copyString.data);
        return;
    }

    CopyFlush();
}

static int DumpCompressedString(const char *data, int32 compressed_size, int (*parseValue)(const char *, int))
{
    uint32 decompress_ret;
    char *decompressTmpBuff = static_cast<char *>(malloc(TOAST_COMPRESS_RAWSIZE(data)));
    if (!decompressTmpBuff) {
        perror("Memory allocation failed for decompressTmpBuff");
        return MEMORY_ALL_FAILED;
    }
    ToastCompressionId cmid = (ToastCompressionId)TOAST_COMPRESS_RAWMETHOD(data);

    switch (cmid) {
        case ToastCompressionId::TOAST_PGLZ_COMPRESSION_ID:
            decompress_ret = pglz_decompress(TOAST_COMPRESS_RAWDATA(data), compressed_size - TOAST_COMPRESS_HEADER_SIZE,
                                             decompressTmpBuff, TOAST_COMPRESS_RAWSIZE(data), true);
            break;
        case ToastCompressionId::TOAST_LZ4_COMPRESSION_ID:
            printf("Error: compression method lz4 not supported.\n");
            printf("Try to rebuild gs_filedump for PostgreSQL server of version 14+ with --with-lz4 option.\n");
            free(decompressTmpBuff);
            return MEMORY_ALL_FAILED;
        default:
            decompress_ret = -1;
            break;
    }

    if ((decompress_ret != TOAST_COMPRESS_RAWSIZE(data)) || (decompress_ret < 0)) {
        printf("WARNING: Unable to decompress a string. Data is corrupted.\n");
        printf("Returned %d while expected %d.\n", decompress_ret, TOAST_COMPRESS_RAWSIZE(data));
    } else {
        CopyAppendEncode(decompressTmpBuff, decompress_ret);
    }

    free(decompressTmpBuff);

    return decompress_ret;
}

static int ReadStringFromToast(const char *buffer, unsigned int buffSize, unsigned int *outSize,
                               int (*parseValue)(const char *, int))
{
    int result = 0;

    /* If toasted value is on disk, we'll try to restore it. */
    if (VARATT_IS_EXTERNAL_ONDISK(buffer)) {
        varatt_external toast_ptr;
        char *toastData = nullptr;

        FILE *toastRelFp;

        VARATT_EXTERNAL_GET_POINTER(toast_ptr, buffer);

        /* Extract TOASTed value */
        int32 toast_ext_size = toast_ptr.va_extsize;
        int32 num_chunks = (toast_ext_size - 1) / TOAST_MAX_CHUNK_SIZE + 1;
        printf("  TOAST value. Raw size: %8d, external size: %8d, "
               "value id: %6d, toast relation id: %6d, chunks: %6d\n",
               toast_ptr.va_rawsize, toast_ext_size, toast_ptr.va_valueid, toast_ptr.va_toastrelid, num_chunks);

        /* Open TOAST relation file */
        char *toastRelationPath = strdup(g_fileName);
        get_parent_directory(toastRelationPath);

        /* Filename of TOAST relation file */
        char toast_relation_filename[MAXPGPATH];
        errno_t rc;
        if (g_isSegment) {
            rc = sprintf_s(toast_relation_filename, MAXPGPATH, "%s/%d_%s", *toastRelationPath ? toastRelationPath : ".",
                           g_toastRelfilenode, SEGTOASTTAG);
        } else {
            rc = sprintf_s(toast_relation_filename, MAXPGPATH, "%s/%d", *toastRelationPath ? toastRelationPath : ".",
                           toast_ptr.va_toastrelid);
        }

        securec_check(rc, "\0", "\0");
        toastRelFp = fopen(toast_relation_filename, "rb");
        if (!toastRelFp) {
            printf("Cannot open TOAST relation %s\n", toast_relation_filename);
            return static_cast<int>(DecodeResult::DECODE_FAILURE);
        }

        unsigned int toastRelationBlockSize = GetBlockSize(toastRelFp);
        fseek(toastRelFp, 0, SEEK_SET);

        toastData = static_cast<char *>(malloc(toast_ptr.va_rawsize));
        if (!toastData) {
            perror("malloc failed.");
            fclose(toastRelFp);
            return static_cast<int>(DecodeResult::DECODE_FAILURE);
        }
        if (g_isUHeap) {
            result = DumpUHeapFileContents(g_blockOptions, g_controlOptions, toastRelFp, toastRelationBlockSize,
                                           -1,   /* no start block */
                                           -1,   /* no end block */
                                           true, /* is toast relation */
                                           toast_ptr.va_valueid, toast_ext_size, toastData);
        } else {
            result = DumpFileContents(g_blockOptions, g_controlOptions, toastRelFp, toastRelationBlockSize,
                                      -1,   /* no start block */
                                      -1,   /* no end block */
                                      true, /* is toast relation */
                                      toast_ptr.va_valueid, toast_ext_size, toastData);
        }
        if (result != 0) {
            printf("Error in TOAST file.\n");
        } else if (VARATT_EXTERNAL_IS_COMPRESSED(toast_ptr)) {
            result = DumpCompressedString(toastData, toast_ext_size, parseValue);
        } else {
            result = parseValue(toastData, toast_ext_size);
        }

        free(toastData);
        fclose(toastRelFp);
        free(toastRelationPath);
    } else {
        /* If tag is indirect or expanded, it was stored in memory. */
        CopyAppend("(TOASTED IN MEMORY)");
    }

    return result;
}

/* Decode an Oid as int type and pass value out. */
static int DecodeOidBinary(const char *buffer, unsigned int buffSize, unsigned int *processedSize, Oid *result)
{
    unsigned int delta = 0;
    if (!g_isUHeap) {
        const char *newBuffer = reinterpret_cast<const char *>(INTALIGN(buffer));
        delta = static_cast<unsigned int>((uintptr_t)newBuffer - (uintptr_t)buffer);

        CHECK_BUFFER_DELTA_SIZE(buffSize, delta);
        buffSize -= delta;
        buffer = newBuffer;
    }

    CHECK_BUFFER_SIZE(buffSize, sizeof(int32));
    *result = *(Oid *)buffer;
    *processedSize = sizeof(Oid) + delta;

    return static_cast<int>(DecodeResult::DECODE_SUCCESS);
}

/* Decode char(N), varchar(N), text, json or xml types and pass data out. */
static int DecodeBytesBinary(const char *buffer, unsigned int buffSize, unsigned int *processedSize, char *outData,
                             unsigned int *outLength)
{
    if (!VARATT_IS_EXTENDED(buffer)) {
        *outLength = VARSIZE(buffer) - VARHDRSZ;

        *processedSize = VARSIZE(buffer);
        errno_t rc = memcpy_s(outData, *outLength, VARDATA(buffer), *outLength);
        securec_check(rc, "\0", "\0");
    } else {
        printf("Error: unable read TOAST value.\n");
    }

    return static_cast<int>(DecodeResult::DECODE_SUCCESS);
}

/*
 * Decode a TOAST chunk as a tuple (Oid toast_id, Oid chunk_id, text data).
 * If decoded OID is equal toast_oid, copy data into chunkData.
 *
 * Parameters:
 *     tupleData - data of the tuple
 *     tupleSize - length of the tuple
 *     toast_oid - [out] oid of the TOAST value
 *     chunk_id - [out] number of the TOAST chunk stored in the tuple
 *     chunk - [out] extracted chunk data
 *     chunk_size - [out] number of bytes extracted from the chunk
 */
void ToastChunkDecode(const char *tupleData, unsigned int tupleSize, Oid toast_oid, uint32 *chunk_id, char *chunkData,
                      unsigned int *chunkDataSize)
{
    UHeapDiskTuple uheader = (UHeapDiskTuple)tupleData;
    HeapTupleHeader header = (HeapTupleHeader)tupleData;
    const char *data = nullptr;
    unsigned int size = 0;
    if (g_isUHeap) {
        data = tupleData + uheader->t_hoff;
        size = tupleSize - uheader->t_hoff;
    } else {
        data = tupleData + header->t_hoff;
        size = tupleSize - header->t_hoff;
    }

    unsigned int processedSize = 0;
    Oid read_toast_oid = 0;

    /* decode toast_id */
    int ret = DecodeOidBinary(data, size, &processedSize, &read_toast_oid);
    if (ret < 0) {
        printf("Error: unable to decode a TOAST tuple toast_id, "
               "decode function returned %d. Partial data: %s\n",
               ret, copyString.data);
        return;
    }

    size -= processedSize;
    data += processedSize;
    if (size <= 0) {
        printf("Error: unable to decode a TOAST chunk tuple, no more bytes "
               "left. Partial data: %s\n",
               copyString.data);
        return;
    }

    /* It is not what we are looking for */
    if (toast_oid != read_toast_oid) {
        return;
    }

    /* decode chunk_id */
    ret = DecodeOidBinary(data, size, &processedSize, chunk_id);
    if (ret < 0) {
        printf("Error: unable to decode a TOAST tuple chunk_id, decode "
               "function returned %d. Partial data: %s\n",
               ret, copyString.data);
        return;
    }

    size -= processedSize;
    data += processedSize;
    if (g_isUHeap) {
        size -= 1;
        data += 1;
    }

    if (size <= 0) {
        printf("Error: unable to decode a TOAST chunk tuple, no more bytes "
               "left. Partial data: %s\n",
               copyString.data);
        return;
    }

    /* decode data */
    ret = DecodeBytesBinary(data, size, &processedSize, chunkData, chunkDataSize);
    if (ret < 0) {
        printf("Error: unable to decode a TOAST chunk data, decode function "
               "returned %d. Partial data: %s\n",
               ret, copyString.data);
        return;
    }

    size -= processedSize;
    if (size != 0) {
        printf("Error: unable to decode a TOAST chunk tuple, %u bytes left. "
               "Partial data: %s\n",
               size, copyString.data);
        return;
    }
}