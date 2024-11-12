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
 * ---------------------------------------------------------------------------------------
 *
 * imcustorage.cpp
 *      routines to support IMColStore
 *
 * IDENTIFICATION
 *        src/gausskernel/storage/htap/imcustorage.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include "access/htap/imcucache_mgr.h"
#include "access/htap/imcustorage.h"

IMCUStorage::IMCUStorage(const CFileNode& cFileNode) : CUStorage(cFileNode, FILE_INVALID)
{
    InitFileNamePrefix(cFileNode);
}

void IMCUStorage::Destroy()
{
    if (m_fd != FILE_INVALID)
        CloseFile(m_fd);
}

// Get common FileName prefix only once.
//
void IMCUStorage::InitFileNamePrefix(_in_ const CFileNode& cFileNode)
{
    int pathlen;
    char attr_name[32];
    errno_t rc = EOK;

    m_fileNamePrefix[0] = '\0';  // empty string

    rc = sprintf_s(attr_name, sizeof(attr_name), "C%d", cFileNode.m_attid);
    securec_check_ss(rc, "", "");

    Oid spcoid, dboid, reloid;
    spcoid = cFileNode.m_rnode.spcNode;
    dboid = cFileNode.m_rnode.dbNode;
    reloid = cFileNode.m_rnode.relNode;

    if (spcoid == GLOBALTABLESPACE_OID) {
        /* Shared system relations live in {datadir}/global */
        Assert(dboid == 0);
        pathlen = strlen(IMCU_GLOBAL_DIR) + 1 + OIDCHARS + 1 + strlen(attr_name) + 1;
        rc = snprintf_s(m_fileNamePrefix, sizeof(m_fileNamePrefix), pathlen, "%s/%u_%s",
            IMCU_GLOBAL_DIR, reloid, attr_name);
        securec_check_ss(rc, "", "");
    } else if (spcoid == DEFAULTTABLESPACE_OID) {
        /* The default tablespace is {datadir}/base */
        pathlen = strlen(IMCU_DEFAULT_DIR) + 1 + OIDCHARS + 1 + OIDCHARS + 1 + strlen(attr_name) + 1;
        rc = snprintf_s(m_fileNamePrefix, sizeof(m_fileNamePrefix), pathlen, "%s/%u_%s", IMCU_DEFAULT_DIR, reloid,
             attr_name);
        securec_check_ss(rc, "", "");
    } else {
        /* All other tablespaces are accessed via symlinks */
        rc = snprintf_s(m_fileNamePrefix,
                        sizeof(m_fileNamePrefix),
                        sizeof(m_fileNamePrefix) - 1,
                        "%s/%u/%s_%s/%u/%u_%s",
                        TBLSPCDIR,
                        spcoid,
                        TABLESPACE_VERSION_DIRECTORY,
                        g_instance.attr.attr_common.PGXCNodeName,
                        dboid,
                        reloid,
                        attr_name);
        securec_check_ss(rc, "", "");
    }
}

void IMCUStorage::SaveCU(char* writeBuf, _in_ uint32 cuId, _in_ int size)
{
    char tmpFileName[MAXPGPATH] = {0};
    Assert(size > 0);
    GetFileName(tmpFileName, MAXPGPATH, cuId);

    m_fd = OpenFile(tmpFileName, cuId, false);
    Assert(m_fd != FILE_INVALID);
    int writtenBytes = FilePWrite(m_fd, writeBuf, size, 0);
    if (writtenBytes != size) {
        SaveCUReportIOError(tmpFileName, 0, writtenBytes, size, size, size);
    }
    CloseFile(m_fd);
}

void IMCUStorage::Load(_in_ uint32 cuId, _in_ int size, __inout char* outbuf)
{
    char tmpFileName[MAXPGPATH];

    GetFileName(tmpFileName, MAXPGPATH, cuId);
    m_fd = OpenFile(tmpFileName, cuId, false);

    if (m_fd == FILE_INVALID) {
        ereport(ERROR, (errcode_for_file_access(), errmsg("Could not open file \"%s\"", tmpFileName)));
    }

    int nbytes = FilePRead(m_fd, outbuf, size, 0);
    if (nbytes != size) {
        LoadCUReportIOError(tmpFileName, 0, nbytes, size, size);
    }
    CloseFile(m_fd);
    m_fd = FILE_INVALID;
    TryRemoveCUFile(cuId, m_cnode.m_attid);
}

void IMCUStorage::LoadCU(_in_ CU* cuPtr, _in_ uint32 cuId, _in_ int size)
{
    if (size < 0) {
        ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION),
                        errmsg("invalid size(%u) in IMCUStorage::LoadCU", size)));
    }

    if (size == 0) {
        cuPtr->m_compressedBufSize = 0;
        ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION),
                        errmsg("IMCUStorage::LoadCU size = 0")));
        return;
    }

    if (!IsDataFileExist(cuId)) {
        ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION),
                        errmsg("IMCUStorage::do not found cu file in disk.")));
        return;
    }

    // in imcstore, cu always in cache.
    cuPtr->m_compressedBuf = (char*)CStoreMemAlloc::Palloc(size, false);
    Load(cuId, size, cuPtr->m_compressedBuf);
    cuPtr->m_compressedBufSize = size;
    cuPtr->m_cache_compressed = true;
}

void IMCUStorage::TryRemoveCUFile(_in_ uint32 cuId, _in_ int colId)
{
    char tmpFileName[MAXPGPATH];
    m_cnode.m_attid = colId;
    InitFileNamePrefix(m_cnode);
    GetFileName(tmpFileName, MAXPGPATH, cuId);

    if (!IsDataFileExist(cuId)) {
        ereport(LOG, (errcode(ERRCODE_DATA_EXCEPTION),
                        errmsg("IMCUStorage::do not found cu file in disk.")));
        return;
    }
    remove(tmpFileName);
}

/* open file, if file not exists then create it */
File IMCUStorage::WSOpenFile(char* file_name, int fileId, bool direct_flag)
{
    uint32 fileFlags = O_RDWR | PG_BINARY;

    struct stat st;

    /* file may create by other, so  fileFlags without O_EXCL */
    if (lstat((const char*)file_name, &st) == -1) {
        fileFlags = O_RDWR | O_CREAT | PG_BINARY;
    }

    ADIO_RUN()
    {
        if (direct_flag) {
            fileFlags |= O_DIRECT;
        }
    }
    ADIO_END();

    RelFileNodeForkNum filenode;
    filenode.rnode.node = m_cnode.m_rnode;
    filenode.rnode.backend = InvalidBackendId;
    filenode.segno = fileId;
    filenode.storage = COLUMN_STORE;
    if (m_cnode.m_attid < 0) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_ATTRIBUTE),
                        errmsg("validate user defined attribute failed!")));
    }
    filenode.forknumber = (ForkNumber)(m_cnode.m_attid + FirstColForkNum);

    return DataFileIdOpenFile(file_name, filenode, (int)fileFlags, 0600);
}