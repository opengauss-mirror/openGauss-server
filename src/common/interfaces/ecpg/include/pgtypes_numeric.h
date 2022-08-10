#ifndef PGTYPES_NUMERIC
#define PGTYPES_NUMERIC

#define NUMERIC_POS 0x0000
#define NUMERIC_NEG 0x4000
#define NUMERIC_NAN 0xC000
#define NUMERIC_NULL 0xF000
#define NUMERIC_MAX_PRECISION 1000
#define NUMERIC_MAX_DISPLAY_SCALE NUMERIC_MAX_PRECISION
#define NUMERIC_MIN_DISPLAY_SCALE 0
#define NUMERIC_MIN_SIG_DIGITS 16

#define DECSIZE 30
#define NUMERIC_LOCAL_NDIG  36

typedef unsigned char NumericDigit;
typedef struct {
    int ndigits;          /* number of digits in digits[] - can be 0! */
    int weight;           /* weight of first digit */
    int rscale;           /* result scale */
    int dscale;           /* display scale */
    int sign;             /* NUMERIC_POS, NUMERIC_NEG, or NUMERIC_NAN */
    NumericDigit* buf;    /* start of alloc'd space for digits[] */
    NumericDigit* digits; /* decimal digits */
    NumericDigit local[NUMERIC_LOCAL_NDIG];
} numeric;

typedef struct {
    int ndigits;                  /* number of digits in digits[] - can be 0! */
    int weight;                   /* weight of first digit */
    int rscale;                   /* result scale */
    int dscale;                   /* display scale */
    int sign;                     /* NUMERIC_POS, NUMERIC_NEG, or NUMERIC_NAN */
    NumericDigit digits[DECSIZE]; /* decimal digits */
} decimal;

#define digitbuf_alloc(ndigits) \
        ((NumericDigit*) pgtypes_alloc((ndigits) * sizeof(NumericDigit)))

#define digitbuf_free(v)                  \
        do {                              \
            if (((v)->buf != NULL) && ((v)->buf != (v)->local)) { \
                free((v)->buf);           \
                (v)->buf = (v)->local;    \
            }                             \
        } while (0)

#define init_numeric(v)        \
    do {                       \
        (v)->buf = (v)->local; \
        (v)->ndigits = 0;      \
        (v)->weight = 0;       \
        (v)->sign = 0;         \
        (v)->rscale = 0;       \
        (v)->dscale = 0;       \
    } while (0)

#define init_alloc_numeric(v, n)                \
    do  {                                       \
        (v)->buf = (v)->local;                  \
        (v)->ndigits = (n);                     \
        if ((n) > NUMERIC_LOCAL_NDIG) {         \
            (v)->buf = digitbuf_alloc((n) + 1); \
        }                                       \
        (v)->buf[0] = 0;                        \
        (v)->digits = (v)->buf + 1;             \
    } while (0)

#ifdef __cplusplus
extern "C" {
#endif

numeric* PGTYPESnumeric_new(void);
decimal* PGTYPESdecimal_new(void);
void PGTYPESnumeric_free(numeric*);
void PGTYPESdecimal_free(decimal*);
numeric* PGTYPESnumeric_from_asc(char*, char**);
char* PGTYPESnumeric_to_asc(numeric*, int);
int PGTYPESnumeric_add(numeric*, numeric*, numeric*);
int PGTYPESnumeric_sub(numeric*, numeric*, numeric*);
int PGTYPESnumeric_mul(numeric*, numeric*, numeric*);
int PGTYPESnumeric_div(numeric*, numeric*, numeric*);
int PGTYPESnumeric_cmp(numeric*, numeric*);
int PGTYPESnumeric_from_int(signed int, numeric*);
int PGTYPESnumeric_from_long(signed long int, numeric*);
int PGTYPESnumeric_copy(numeric*, numeric*);
int PGTYPESnumeric_from_double(double, numeric*);
int PGTYPESnumeric_to_double(numeric*, double*);
int PGTYPESnumeric_to_int(numeric*, int*);
int PGTYPESnumeric_to_long(numeric*, long*);
int PGTYPESnumeric_to_decimal(numeric*, decimal*);
int PGTYPESnumeric_from_decimal(decimal*, numeric*);

#ifdef __cplusplus
}
#endif

#endif /* PGTYPES_NUMERIC */
