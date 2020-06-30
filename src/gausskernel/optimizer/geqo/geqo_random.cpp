/* ------------------------------------------------------------------------
 *
 * geqo_random.cpp
 *	   random number generator
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 * src/gausskernel/optimizer/geqo/geqo_random.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "optimizer/geqo_random.h"

void geqo_set_seed(PlannerInfo* root, double seed)
{
    GeqoPrivateData* priv = (GeqoPrivateData*)root->join_search_private;

    /*
     * XXX. This seeding algorithm could certainly be improved - but it is not
     * critical to do so.
     */
    errno_t rc = EOK;
    rc = memset_s(priv->random_state, sizeof(priv->random_state), 0, sizeof(priv->random_state));
    securec_check(rc, "\0", "\0");
    rc = memcpy_s(priv->random_state, sizeof(priv->random_state), &seed, Min(sizeof(priv->random_state), sizeof(seed)));
    securec_check(rc, "\0", "\0");
}

double geqo_rand(PlannerInfo* root)
{
    GeqoPrivateData* priv = (GeqoPrivateData*)root->join_search_private;

    return erand48(priv->random_state);
}
