/*
 * Copyright (c) 2024 Huawei Technologies Co.,Ltd.
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
 * sampling.h
 *
 * IDENTIFICATION
 *        src/include/access/datavec/sampling.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef SAMPLING_H
#define SAMPLING_H

#include "access/datavec/pg_prng.h"
#include "storage/buf/block.h"

extern void sampler_random_init_state(uint32 seed, pg_prng_state *randstate);
extern double sampler_random_fract(pg_prng_state *randstate);

typedef struct {
    BlockNumber N;           /* number of blocks, known in advance */
    uint32 n;                /* desired sample size */
    BlockNumber t;           /* current block number */
    uint32 m;                /* blocks selected so far */
    pg_prng_state randstate; /* random generator state */
} BlockSamplerData2;

typedef BlockSamplerData2 *BlockSampler2;

extern BlockNumber BlockSampler_Init2(BlockSampler2 bs, BlockNumber nblocks, int samplesize, uint32 randseed);
extern bool BlockSampler_HasMore2(BlockSampler2 bs);
extern BlockNumber BlockSampler_Next2(BlockSampler2 bs);

typedef struct {
    double W;
    pg_prng_state randstate; /* random generator state */
} ReservoirStateData;

typedef ReservoirStateData *ReservoirState;

extern void reservoir_init_selection_state(ReservoirState rs, int n);
extern double reservoir_get_next_S(ReservoirState rs, double t, int n);

#endif /* SAMPLING_H */
