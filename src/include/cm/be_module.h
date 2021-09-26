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
 * be_module.h
 * 
 * 
 * 
 * IDENTIFICATION
 *        src/include/cm/be_module.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef SRC_INCLUDE_CM_BE_MODULE_H
#define SRC_INCLUDE_CM_BE_MODULE_H

/*
 * How to add your module id ?
 * 1. add your module id before MOD_MAX in ModuleId;
 * 2. fill up module_map[] about module name, and keep their ordering;
 *
 */
enum ModuleId {
    /* fastpath for all modules on/off */
    MOD_ALL = 0,
    /* add your module id following */
    MOD_CMCTL,
    MOD_CMA,
    MOD_CMS,
    MOD_OMMONITER,
    /* add your module id above */
    MOD_MAX
};

/* 1 bit <--> 1 module, including MOD_MAX. its size is
 *      ((MOD_MAX+1)+7)/8 = MOD_MAX/8 + 1
 */
#define BEMD_BITMAP_SIZE (1 + (MOD_MAX / 8))

#define MODULE_ID_IS_VALID(_id) ((_id) >= MOD_ALL && (_id) < MOD_MAX)
#define ALL_MODULES(_id) (MOD_ALL == (_id))

/* Is it a valid and signle module id ? */
#define VALID_SINGLE_MODULE(_id) ((_id) > MOD_ALL && (_id) < MOD_MAX)

/* max length of module name */
#define MODULE_NAME_MAXLEN (16)

/* delimiter of module name list about GUC parameter */
#define MOD_DELIMITER ','

/* map about module id and its name */
typedef struct module_data {
    ModuleId mod_id;
    const char mod_name[MODULE_NAME_MAXLEN];
} module_data;

/*******************  be-module id <--> name **********************/
extern const module_data module_map[];

/*
 * @Description: find a module's name according to its id.
 * @IN module_id: module id
 * @Return: module name
 */
inline const char* get_valid_module_name(ModuleId module_id)
{
    return module_map[module_id].mod_name;
}

/*******************  be-module logging **********************/

#endif /* SRC_INCLUDE_CM_BE_MODULE_H */
