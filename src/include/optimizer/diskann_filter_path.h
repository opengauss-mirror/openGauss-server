#ifndef DISKANN_FILTER_PATH_H
#define DISKANN_FILTER_PATH_H

#include "nodes/relation.h"

extern void ann_register_filter_request(PlannerInfo* root);
extern void ann_preserve_filter_candidate(PlannerInfo* root, IndexPath* path);
extern bool ann_keep_join_inputs(PlannerInfo* root, RelOptInfo* rel);
extern void ann_preserve_ordinary_candidate(PlannerInfo* root, Path* path);
extern void ann_derive_filter_paths(PlannerInfo* root, RelOptInfo* rel);

#endif
