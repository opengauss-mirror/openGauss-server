#include "postgres.h"
#include "utils/expandeddatum.h"


/*
 * If the Datum represents a R/W expanded object, change it to R/O.
 * Otherwise return the original Datum.
 *
 * Caller must ensure that the datum is a non-null varlena value.  Typically
 * this is invoked via MakeExpandedObjectReadOnly(), which checks that.
 */
Datum
MakeExpandedObjectReadOnlyInternal(Datum d)
{
    return d;
}