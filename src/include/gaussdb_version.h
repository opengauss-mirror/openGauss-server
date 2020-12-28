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
 * ---------------------------------------------------------------------------------------
 * 
 * gaussdb_version.h
 *        Definitions for the Gaussdb features check
 * 
 * 
 * IDENTIFICATION
 *        src/include/gaussdb_version.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef GAUSSDB_VERSION_H_INCLUDED
#define GAUSSDB_VERSION_H_INCLUDED

typedef enum {
    MULTI_VALUE_COLUMN = 0,
    JSON,
    XML,
    DATA_STORAGE_FORMAT,
    GTM_FREE,
    DOUBLE_LIVE_DISASTER_RECOVERY_IN_THE_SAME_CITY,
    DISASTER_RECOVERY_IN_TWO_PLACES_AND_THREE_CENTRES,
    GPU_ACCELERATION_IN_MULTIDIMENSIONAL_COLLISION_ANALYSIS,
    FULL_TEXT_INDEX,
    EXTENSION_CONNECTOR,
    SQL_ON_HDFS,
    SQL_ON_OBS,
    EXPRESS_CLUSTER,
    CROSS_DC_COLLABORATION,
    GRAPH_COMPUTING_ENGINE,
    SEQUENTIAL_DATA_ENGINE,
    POSTGIS_DOCKING,
    HA_SINGLE_PRIMARY_MULTI_STANDBY,
    ROW_LEVEL_SECURITY,
    TRANSPARENT_ENCRYPTION,
    PRIVATE_TABLE,
    FEATURE_NAME_MAX_VALUE  // Must be the last one
} feature_name;

/* Product version number is unknown. */
#define PRODUCT_VERSION_UNKNOWN ((uint32)0)
/* GaussDB 200 product version number. */
#define PRODUCT_VERSION_GAUSSDB200 ((uint32)2)
/* GaussDB 300 product version number. */
#define PRODUCT_VERSION_GAUSSDB300 ((uint32)3)

extern void initialize_feature_flags();
extern bool is_feature_disabled(feature_name);
extern void signalReloadLicenseHandler(int sig);
extern uint32 get_product_version();
#endif  // GAUSSDB_VERSION_H_INCLUDED
