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
 * ----------------------------------------------------------
 * 
 * IDENTIFICATION
 *	  src/gausskernel/cbb/utils/zfiles/gz_file.cpp
 *
 * ----------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"
#include "storage/smgr/fd.h"
#include "utils/palloc.h"
#include "utils/zfiles.h"

void* gz_open(const char* gzfile)
{
    /* open file with vfd */
    opaque_vfd* vfd = (opaque_vfd*)vfd_file_open(gzfile);
    Assert(vfd);
    /* open .gz file */
    gzFile gzf = gzdopen(FileFd(vfd->m_tmp_vfd), "rb");
    if (NULL == gzf) {
        int tmperr = vfd->m_errno;
        /* gzdopen will not close the real fd from OS when failed to open */
        vfd_file_close(vfd);
        ereport(ERROR, (errmsg("gz_open failed: errno %d", tmperr)));
    }

    gz_reader* gz = (gz_reader*)palloc(sizeof(gz_reader));
    gz->m_vfd = vfd;
    gz->m_gzf = gzf;
    return gz;
}

int gz_read(void* obj, char* buf, int bufsize)
{
    gz_reader* gz = (gz_reader*)obj;
    int nread = gzread(gz->m_gzf, buf, bufsize);
    if (nread < 0) {
        int tmperr = errno;
        off_t off = gz->m_vfd->m_cur_off;
        gz_close(gz); /* release fd and memory */
        ereport(ERROR, (errmsg("gz_read failed: offset %ld, errno %d, returned size %d", off, tmperr, nread)));
    }
    /* update file offset */
    gz->m_vfd->m_cur_off += nread;
    return nread;
}

void gz_close(void* obj)
{
    gz_reader* gz = (gz_reader*)obj;
    /* first gzclose_r will close the real fd from OS */
    (void)gzclose_r(gz->m_gzf);
    /* next close the vfd */
    vfd_file_close_with_thief(gz->m_vfd);
    pfree(gz);
}
