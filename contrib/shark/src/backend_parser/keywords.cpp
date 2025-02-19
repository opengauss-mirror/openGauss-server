#include "c.h"

#include "parser/keywords.h"


/* ScanKeywordList lookup data for SQL keywords */

#include "src/backend_parser/kwlist_d.h"

/* Keyword categories for SQL keywords */

#define PG_KEYWORD(kwname, value, category, collabel) category,

const uint8 pgtsql_ScanKeywordCategories[PGTSQL_SCANKEYWORDS_NUM_KEYWORDS] = {
#include "src/backend_parser/kwlist.h"
};

#undef PG_KEYWORD

/* Keyword can-be-direct-label flags for SQL keywords */

#define PG_KEYWORD(kwname, value, category, collabel) collabel,

#define DIRECT_LABEL true
#define AS_LABEL false

const bool	ScanKeywordDirectLabel[PGTSQL_SCANKEYWORDS_NUM_KEYWORDS] = {
#include "src/backend_parser/kwlist.h"
};

#undef PG_KEYWORD
#undef DIRECT_LABEL
#undef AS_LABEL