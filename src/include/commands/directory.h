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
 * directory.h
 *        The declaration of directory operate function. The functions
 *        use to simulate A db's directory.
 * 
 * 
 * IDENTIFICATION
 *        src/include/commands/directory.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef DIRECTORY_H
#define DIRECTORY_H

#include "nodes/parsenodes.h"

/* directory.h */
extern void CreatePgDirectory(CreateDirectoryStmt* stmt);
extern void DropPgDirectory(DropDirectoryStmt* stmt);
extern Oid get_directory_oid(const char* directoryname, bool missing_ok);
extern char* get_directory_name(Oid dir_oid);
extern void RemoveDirectoryById(Oid dirOid);
extern void AlterDirectoryOwner(const char* dirname, Oid newOwnerId);

#endif /* DIRECTORY_H*/
