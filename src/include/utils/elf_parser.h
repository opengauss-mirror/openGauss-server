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
 * fatal_err.h
 *
 * IDENTIFICATION
 *        src/include/utils/fatal_err.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef ELF_PARSER_H
#define ELF_PARSER_H

#include <elf.h>

#define SYMHDR_NUM 2

/*
 * It is an elf parser, which can lookup the function symbol that is the nearest
 * to the given address.
 */
class elf_parser {
public:
    elf_parser();
    ~elf_parser();

    bool reset(const char *filename);
    bool is_same_file(const char *filename);
    bool resolve_addr(uintptr_t addr, char *buf, size_t len, uint *offset);
    inline bool isdyn()
    {
        return dyn;
    }

private:
    const char *fname;  /* file name */
    int fd;
    Elf64_Ehdr ehdr;    /* elf header */
    Elf64_Shdr symhdr[SYMHDR_NUM];  /* there are at most two symbol tables in an elf file */
    int sym_count;      /* number of symbol tables found */
    bool dyn;           /* wether a shared object file is opened */

    void init();
    bool load(const char *name);
    bool match_symbol(uintptr_t addr, uint *strtab_idx, uint *pos, uint *offset);
    bool string_at(uint strtab_idx, uint pos, char *buf, size_t len);
};


#endif
