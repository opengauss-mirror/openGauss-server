#include "c.h"

#include "parser/keywords.h"


/* ScanKeywordList lookup data for SQL keywords */

#include "src/backend_parser/kwlist_d.h"

/* Keyword categories for SQL keywords */

#define PG_KEYWORD(kwname, value, category) category,

const uint8 pgtsql_ScanKeywordCategories[PGTSQL_SCANKEYWORDS_NUM_KEYWORDS] = {
#include "src/backend_parser/kwlist.h"
};

#undef PG_KEYWORD
