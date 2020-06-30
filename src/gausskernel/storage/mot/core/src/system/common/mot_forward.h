/*
 * Copyright (c) 2020 Huawei Technologies Co.,Ltd.
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
 * mot_forward.h
 *    Forward declarations to reduce include directives.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/src/system/common/mot_forward.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef MOT_FORWARD_H
#define MOT_FORWARD_H

namespace MOT {
// Forward declaration to reduce include directives (keep list sorted)
class Catalog;
class Row;
class TxnManager;
class MOTContext;
class MOTContextNuma;
class EpochManager;
class memory_manager_numa;
}  // namespace MOT

#endif /* MOT_FORWARD_H */
