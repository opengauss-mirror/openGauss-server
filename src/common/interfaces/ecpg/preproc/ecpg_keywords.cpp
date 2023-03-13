/* -------------------------------------------------------------------------
 *
 * ecpg_keywords.c
 *	  lexical token lookup for reserved words in openGauss embedded SQL
 *
 * IDENTIFICATION
 *	  src/interfaces/ecpg/preproc/ecpg_keywords.c
 *
 * -------------------------------------------------------------------------
 */

#include "postgres_fe.h"

#include <ctype.h>

#include "extern.h"
#include "preproc.hpp"

#include "parser/kwlist_d.h"

/* ScanKeywordList lookup data for ECPG keywords */
#include "ecpg_kwlist_d.h"

/* Token codes for ECPG keywords */
#define PG_KEYWORD(kwname, value) value,
static const uint16 ECPGScanKeywordTokens[] = {
#include "ecpg_kwlist.h"
};

#undef PG_KEYWORD

#define PG_KEYWORD(kwname, value, category) value,
static const uint16 SQLScanKeywordTokens[] = {
#include "parser/kwlist.h"
};

#undef PG_KEYWORD

/*
 * ScanECPGKeywordLookup - see if a given word is a keyword
 *
 * Returns the token value of the keyword, or -1 if no match.
 *
 * Keywords are matched using the same case-folding rules as in the backend.
 */
int ScanECPGKeywordLookup(const char *text)
{
   int kwnum;

   /* First check SQL symbols defined by the backend. */
   kwnum = ScanKeywordLookup(text, &ScanKeywords);
   if (kwnum >= 0)
       return SQLScanKeywordTokens[kwnum];

   /* Try ECPG-specific keywords. */
   kwnum = ScanKeywordLookup(text, &ScanECPGKeywords);
   if (kwnum >= 0)
       return ECPGScanKeywordTokens[kwnum];

   return -1;
}
