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
 * elf_parser.cpp
 *
 * IDENTIFICATION
 *        src/common/backend/utils/misc/elf_parser.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
#include <fcntl.h>

#include "postgres.h"
#ifdef ENABLE_MULTIPLE_NODES
#include "pgxc/pgxc.h"
#include "pgxc/execRemote.h"
#endif
#include "utils/elf_parser.h"

elf_parser::elf_parser()
{
    init();
}

elf_parser::~elf_parser()
{
    fname = NULL;
    if (fd != -1) {
        (void)close(fd);
    }
}

bool elf_parser::load(const char *filename)
{
    fd = open(filename, O_RDONLY);
    if (fd == -1) {
        return false;
    }

    fname = filename;

    if (read(fd, &ehdr, sizeof(Elf64_Ehdr)) != sizeof(Elf64_Ehdr)) {
        return false;
    }

    dyn = (ehdr.e_type == ET_DYN) ? true : false;

    const int limit = 1000;
    if (ehdr.e_shnum > limit) {
        return false;
    }

    /* get the section headers representing the symbol talbe */
    Elf64_Shdr shdr;
    for (int i = 0; i < ehdr.e_shnum; i++) {
        off_t off = (off_t)(ehdr.e_shoff + i * ehdr.e_shentsize);

        if (pread(fd, &shdr, sizeof(Elf64_Shdr), off) != sizeof(Elf64_Shdr)) {
            return false;
        }

        if ((shdr.sh_type == SHT_SYMTAB || shdr.sh_type == SHT_DYNSYM) &&
                sym_count < SYMHDR_NUM) {
            if (memcpy_s(symhdr + sym_count, sizeof(Elf64_Shdr), &shdr, sizeof(Elf64_Shdr))) {
                return false;
            }
            sym_count++;
        }
    }

    return true;
}

bool elf_parser::match_symbol(uintptr_t addr, uint *strtab_idx, uint *pos, uint *offset)
{
    for (int ti = 0; ti < sym_count; ti++) {
        off_t off = (off_t)(symhdr[ti].sh_offset);
        if (lseek(fd, off, SEEK_SET) != off) {
            return false;
        }

        Elf64_Sym sym;
        int count = (int)(symhdr[ti].sh_size / symhdr[ti].sh_entsize);

        for (int si = 0; si < count; si++) {
            if (read(fd, &sym, sizeof(Elf64_Sym)) != sizeof(Elf64_Sym)) {
                return false;
            }

            if (ELF64_ST_TYPE(sym.st_info) != STT_FUNC ||
                    addr < sym.st_value ||
                    addr > (sym.st_value + sym.st_size)) {
                continue;
            }

            *strtab_idx = symhdr[ti].sh_link;
            *pos = sym.st_name;
            *offset = (uint)(addr - sym.st_value);

            return true;
        }
    }

    return false;
}

/*
 * Get string at specified position
 */
bool elf_parser::string_at(uint strtab_idx, uint pos, char *buf, size_t len)
{
    off_t off = (off_t)(ehdr.e_shoff + strtab_idx * ehdr.e_shentsize);
    if (lseek(fd, off, SEEK_SET) != off) {
        return false;
    }

    Elf64_Shdr shdr;
    if (read(fd, &shdr, sizeof(Elf64_Shdr)) != sizeof(Elf64_Shdr)) {
        return false;
    }

    off = (off_t)(shdr.sh_offset + pos);
    if (lseek(fd, off, SEEK_SET) != off) {
        return false;
    }

    if (read(fd, buf, len) == -1) {
        return false;
    }

    buf[len - 1] = '\0';

    return true;
}

void elf_parser::init()
{
    fd = -1;
    fname = NULL;
    sym_count = 0;
    dyn = true;
    (void)memset_s(&ehdr, sizeof(Elf64_Ehdr), 0, sizeof(Elf64_Ehdr));
    (void)memset_s(symhdr, sizeof(symhdr), 0, sizeof(symhdr));
}

/*
 * Reset elf parser, in other words, unload the previously opened file and load the new file.
 */
bool elf_parser::reset(const char *filename)
{
    Assert(filename);
    if (fd != -1) {
        (void)close(fd);
    }

    init();

    if (!load(filename)) {
        if (fd != -1) {
            (void)close(fd);
        }
        init();
        return false;
    }

    return true;
}

/*
 * Check whether the specified file is the same as the currently opened file
 */
bool elf_parser::is_same_file(const char *filename)
{
    Assert(filename);
    return fname && !strcmp(filename, fname);
}

/*
 * lookup the function symbol that is nearest to the specified address
 */
bool elf_parser::resolve_addr(uintptr_t addr, char *buf, size_t len, uint *offset)
{
    Assert(fd != -1);
    Assert(buf);
    Assert(offset);

    uint strtab_idx;
    uint pos;

    if (match_symbol(addr, &strtab_idx, &pos, offset)) {
        return string_at(strtab_idx, pos, buf, len);
    }

    return false;
}
