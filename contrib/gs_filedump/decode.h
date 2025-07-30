#ifndef GS_FILEDUMP_DECODE_H_
#define GS_FILEDUMP_DECODE_H_

const int DAYS_PER_400_YEARS = 146097;
const int DAYS_PER_4_YEARS = 1461;
const int DAYS_PER_NORMAL_YEAR = 365;
const int DAYS_PER_LEAP_YEAR = 366;
const int DAYS_TO_MONTH_ADJUSTMENT = 7834;
const int MONTH_CONVERSION_FACTOR = 2141;
const int MONTH_CONVERSION_SCALE = 65536;
const int YEAR_OFFSET_GREGORIAN = 4800;
const int JULIAN_DAY_BASE_ADJUSTMENT = 32044;
const int DAY_ADJUSTMENT_BEFORE_MONTH_CONVERT = 123;
const int QUAD_YEAR_ADJUSTMENT = 4;
const int INITIAL_JULIAN_ADJUSTMENT = 60;
const int EXTRA_YEAR_ADJUSTMENT = 3;
const int DAY_OFFSET_NON_LEAP_YEAR = 305;
const int DAY_OFFSET_LEAP_YEAR = 306;
const int MONTH_CONVERSION_DIVISOR = 256;
const int MONTH_ADJUSTMENT_OFFSET = 10;
const int64 MICROSECONDS_PER_SECOND = 1000000;

/* Each special character can be escaped to a maximum of two characters (like '\0' -> "\\0") */
const int MAX_ESCAPE_CHAR_LEN = 2;

const int DECIMAL_BASE = 10;
const int DECIMAL_HUNDRED = 100;
const int DECIMAL_THOUSAND = 1000;

const int NBASE = 10000;
const int HALF_NBASE = 5000;
const int DEC_DIGITS = 4;       /* decimal digits per NBASE digit */
const int MUL_GUARD_DIGITS = 2; /* these are measured in NBASE digits */
const int DIV_GUARD_DIGITS = 4;

using NumericDigit = int16;

int ParseAttributeTypesString(const char *str);

void FormatDecode(const char *tupleData, unsigned int tupleSize);

void ToastChunkDecode(const char *tupleData, unsigned int tupleSize, Oid toast_oid, uint32 *chunk_id, char *chunkData,
                      unsigned int *chunkDataSize);

struct NumericShort {
    uint16 n_header;                            /* Sign + display scale + weight */
    NumericDigit n_data[FLEXIBLE_ARRAY_MEMBER]; /* Digits */
};

struct NumericLong {
    uint16 n_sign_dscale;                       /* Sign + display scale */
    int16 n_weight;                             /* Weight of 1st digit  */
    NumericDigit n_data[FLEXIBLE_ARRAY_MEMBER]; /* Digits */
};

union NumericChoice {
    uint16 n_header;             /* Header word */
    struct NumericLong n_long;   /* Long form (4-byte header) */
    struct NumericShort n_short; /* Short form (2-byte header) */
};

struct NumericData {
    union NumericChoice choice; /* choice of format */
};

/*
 * Interpretation of high bits.
 */

const int NUMERIC_SIGN_MASK = 0xC000;
const int NUMERIC_POS = 0x0000;
const int NUMERIC_NEG = 0x4000;
const int NUMERIC_SHORT = 0x8000;
const int NUMERIC_SPECIAL = 0xC000;

#define NUMERIC_FLAGBITS(n) ((n)->choice.n_header & NUMERIC_SIGN_MASK)
#define NUMERIC_IS_SHORT(n)            (NUMERIC_FLAGBITS(n) == NUMERIC_SHORT)
#define NUMERIC_IS_SPECIAL(n)  (NUMERIC_FLAGBITS(n) == NUMERIC_SPECIAL)

/*
 * If the flag bits are NUMERIC_SHORT or NUMERIC_SPECIAL, we want the short
 * header; otherwise, we want the long one.  Instead of testing against each
 * value, we can just look at the high bit, for a slight efficiency gain.
 */
#define NUMERIC_HEADER_IS_SHORT(n)     (((n)->choice.n_header & 0x8000) != 0)
#define NUMERIC_HEADER_SIZE(n) \
    (sizeof(uint16) + \
    (NUMERIC_HEADER_IS_SHORT(n) ? 0 : sizeof(int16)))

/*
 * Definitions for special values (NaN, positive infinity, negative infinity).
 *
 * The two bits after the NUMERIC_SPECIAL bits are 00 for NaN, 01 for positive
 * infinity, 11 for negative infinity.  (This makes the sign bit match where
 * it is in a short-format value, though we make no use of that at present.)
 * We could mask off the remaining bits before testing the active bits, but
 * currently those bits must be zeroes, so masking would just add cycles.
 */
const int NUMERIC_EXT_SIGN_MASK = 0xF000; /* high bits plus NaN/Inf flag bits */
const int NUMERIC_NAN = 0xC000;
const int NUMERIC_PINF = 0xD000;
const int NUMERIC_NINF = 0xF000;
const int NUMERIC_INF_SIGN_MASK = 0x2000;

#define NUMERIC_EXT_FLAGBITS(n)        ((n)->choice.n_header & NUMERIC_EXT_SIGN_MASK)
#define NUMERIC_IS_NAN(n)              ((n)->choice.n_header == NUMERIC_NAN)
#define NUMERIC_IS_PINF(n)             ((n)->choice.n_header == NUMERIC_PINF)
#define NUMERIC_IS_NINF(n)             ((n)->choice.n_header == NUMERIC_NINF)
#define NUMERIC_IS_INF(n) \
    (((n)->choice.n_header & ~NUMERIC_INF_SIGN_MASK) == NUMERIC_PINF)
       
/*
 * Short format definitions.
 */
const int NUMERIC_SHORT_SIGN_MASK = 0x2000;
const int NUMERIC_SHORT_DSCALE_MASK = 0x1F80;
const int NUMERIC_SHORT_DSCALE_SHIFT = 7;
const int NUMERIC_SHORT_DSCALE_MAX = (NUMERIC_SHORT_DSCALE_MASK >> NUMERIC_SHORT_DSCALE_SHIFT);
const int NUMERIC_SHORT_WEIGHT_SIGN_MASK = 0x0040;
const int NUMERIC_SHORT_WEIGHT_MASK = 0x003F;
const int NUMERIC_SHORT_WEIGHT_MAX = NUMERIC_SHORT_WEIGHT_MASK;
const int NUMERIC_SHORT_WEIGHT_MIN = (-(NUMERIC_SHORT_WEIGHT_MASK + 1));

/*
 * Extract sign, display scale, weight.  These macros extract field values
 * suitable for the NumericVar format from the Numeric (on-disk) format.
 *
 * Note that we don't trouble to ensure that dscale and weight read as zero
 * for an infinity; however, that doesn't matter since we never convert
 * "special" numerics to NumericVar form.  Only the constants defined below
 * (const_nan, etc) ever represent a non-finite value as a NumericVar.
 */

const int NUMERIC_DSCALE_MASK = 0x3FFF;
const int NUMERIC_DSCALE_MAX = NUMERIC_DSCALE_MASK;

#define NUMERIC_SIGN(n) \
    (NUMERIC_IS_SHORT(n) ? \
        (((n)->choice.n_short.n_header & NUMERIC_SHORT_SIGN_MASK) ? \
            NUMERIC_NEG : NUMERIC_POS) : \
            (NUMERIC_IS_SPECIAL(n) ? \
                NUMERIC_EXT_FLAGBITS(n) : NUMERIC_FLAGBITS(n)))
#define NUMERIC_DSCALE(n) \
    (NUMERIC_HEADER_IS_SHORT((n)) ? \
        ((n)->choice.n_short.n_header & NUMERIC_SHORT_DSCALE_MASK) \
        >> NUMERIC_SHORT_DSCALE_SHIFT \
       : ((n)->choice.n_long.n_sign_dscale & NUMERIC_DSCALE_MASK))
#define NUMERIC_WEIGHT(n) \
    (NUMERIC_HEADER_IS_SHORT((n)) ? \
       (((n)->choice.n_short.n_header & NUMERIC_SHORT_WEIGHT_SIGN_MASK ? \
               ~NUMERIC_SHORT_WEIGHT_MASK : 0) \
        | ((n)->choice.n_short.n_header & NUMERIC_SHORT_WEIGHT_MASK)) \
       : ((n)->choice.n_long.n_weight))

#undef TrapMacro
#define TrapMacro(true, condition) (true)

const size_t ATTRTYPES_STR_MAX_LEN = 1024 - 1;

enum class ToastCompressionId {
    TOAST_PGLZ_COMPRESSION_ID = 0,
    TOAST_LZ4_COMPRESSION_ID = 1,
    TOAST_INVALID_COMPRESSION_ID = 2
};

enum class DecodeResult {
    DECODE_SUCCESS = 0,                        // success
    DECODE_BUFF_SIZE_LESS_THAN_DELTA = -1,     // buffSize < delta
    DECODE_BUFF_SIZE_LESS_THAN_REQUIRED = -2,  // buffSize < required_size
    DECODE_BUFF_SIZE_IS_ZERO = -3,             // buffer_size = 0
    DECODE_FAILURE = -9
};

/*
 * Utilities for manipulation of header information for compressed
 * toast entries.
 */
/*
 * These macros define the "saved size" portion of va_extinfo.  Its remaining
 * two high-order bits identify the compression method.
 * Before std14 only pglz compression method existed (with 00 bits).
 */
const int VARLENA_EXTSIZE_BITS = 30;
const unsigned int VARLENA_EXTSIZE_MASK = ((1U << VARLENA_EXTSIZE_BITS) - 1);
#define VARDATA_COMPRESSED_GET_COMPRESS_METHOD(ptr) ((*((uint32 *)(ptr) + 1)) >> VARLENA_EXTSIZE_BITS)

#define TOAST_COMPRESS_RAWSIZE(ptr) ((*(uint32 *)(ptr)) & VARLENA_EXTSIZE_MASK)
#define TOAST_COMPRESS_RAWMETHOD(ptr) ((*(uint32 *)(ptr)) >> VARLENA_EXTSIZE_BITS)
#define TOAST_COMPRESS_RAWDATA(ptr) ((ptr) + sizeof(uint32))
#define TOAST_COMPRESS_HEADER_SIZE (sizeof(uint32))

constexpr const char *INT16_FORMAT = "%d";
constexpr const char *INT32_FORMAT = "%d";
constexpr const char *UINT32_FORMAT = "%u";

#define CHECK_BUFFER_SIZE(buffSize, required_size)      \
    do {                                                \
        if ((buffSize) < (required_size)) {             \
            return static_cast<int>(DecodeResult::DECODE_BUFF_SIZE_LESS_THAN_REQUIRED); \
        }                                               \
    } while (0)

#define CHECK_BUFFER_DELTA_SIZE(buffSize, delta)     \
    do {                                             \
        if ((buffSize) < (delta)) {                  \
            return static_cast<int>(DecodeResult::DECODE_BUFF_SIZE_LESS_THAN_DELTA); \
        }                                            \
    } while (0)

/* CopyAppend version with format string support */
#define CopyAppendFmt(fmt, ...)                                                                             \
    do {                                                                                                    \
        char copyFormatBuff[512];                                                                           \
        snprintf_s(copyFormatBuff, sizeof(copyFormatBuff), sizeof(copyFormatBuff) - 1, fmt, ##__VA_ARGS__); \
        CopyAppend(copyFormatBuff);                                                                         \
    } while (0)

using DecodeCallbackT = int (*)(const char *buffer, unsigned int buffSize, unsigned int *outSize);

using ParseCallbackTableItem = struct {
    char *name;
    DecodeCallbackT callback;
};

/* for ustore begin */
int ParseUHeapAttributeTypesString(const char *str);

void FormatUHeapDecode(const char *tupleData, unsigned int tupleSize);

void ToastUHeapChunkDecode(const char *tupleData, unsigned int tupleSize, Oid toast_oid, uint32 *chunk_id,
                           char *chunkData, unsigned int *chunkDataSize);

/* ----------
 * PGLZ_MAX_OUTPUT -
 *
 *		Macro to compute the buffer size required by pglz_compress().
 *		We allow 4 bytes for overrun before detecting compression failure.
 * ----------
 */
#define PGLZ_MAX_OUTPUT(_dlen) ((_dlen) + 4)

/* ----------
 * PGLZ_Strategy -
 *
 *		Some values that control the compression algorithm.
 *
 *		min_input_size		Minimum input data size to consider compression.
 *
 *		max_input_size		Maximum input data size to consider compression.
 *
 *		min_comp_rate		Minimum compression rate (0-99%) to require.
 *							Regardless of min_comp_rate, the output must be
 *							smaller than the input, else we don't store
 *							compressed.
 *
 *		first_success_by	Abandon compression if we find no compressible
 *							data within the first this-many bytes.
 *
 *		match_size_good		The initial GOOD match size when starting history
 *							lookup. When looking up the history to find a
 *							match that could be expressed as a tag, the
 *							algorithm does not always walk back entirely.
 *							A good match fast is usually better than the
 *							best possible one very late. For each iteration
 *							in the lookup, this value is lowered so the
 *							longer the lookup takes, the smaller matches
 *							are considered good.
 *
 *		match_size_drop		The percentage by which match_size_good is lowered
 *							after each history check. Allowed values are
 *							0 (no change until end) to 100 (only check
 *							latest history entry at all).
 * ----------
 */
using PGLZ_Strategy = struct {
    int32 min_input_size;
    int32 max_input_size;
    int32 min_comp_rate;
    int32 first_success_by;
    int32 match_size_good;
    int32 match_size_drop;
};

/* ----------
 * The standard strategies
 *
 *		PGLZ_strategy_default		Recommended default strategy for TOAST.
 *
 *		PGLZ_strategy_always		Try to compress inputs of any length.
 *									Fallback to uncompressed storage only if
 *									output would be larger than input.
 * ----------
 */
extern const PGLZ_Strategy *const PGLZ_STRATEGY_DEFAULT;
extern const PGLZ_Strategy *const PGLZ_STRATEGY_ALWAYS;

/* ----------
 * Global function declarations
 * ----------
 */
extern int32 pglz_compress(const char *source, int32 slen, char *dest, const PGLZ_Strategy *strategy);
extern int32 pglz_decompress(const char *source, int32 slen, char *dest, int32 rawsize, bool check_complete);

#endif
