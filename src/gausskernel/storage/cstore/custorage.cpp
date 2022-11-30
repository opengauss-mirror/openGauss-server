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
 * custorage.cpp
 *      routines to support ColStore
 *
 * IDENTIFICATION
 *        src/gausskernel/storage/cstore/custorage.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include "postgres.h"
#include "knl/knl_variable.h"
#include <fcntl.h>
#include <sys/file.h>
#include "miscadmin.h"
#include "access/cstore_rewrite.h"
#include "access/heapam.h"
#include "access/genam.h"
#include "access/xlog.h"
#include "catalog/pg_tablespace.h"
#include "catalog/catalog.h"
#include "commands/tablespace.h"
#include "service/remote_read_client.h"
#include "storage/cstore/cstorealloc.h"
#include "storage/cu.h"
#include "storage/custorage.h"
#include "storage/lmgr.h"
#include "pgxc/pgxc.h"
#include "postmaster/postmaster.h"
#include "utils/aiomem.h"
#include "utils/plog.h"
#include "securec_check.h"
#include "storage/dss/fio_dss.h"
#include "storage/file/fio_device.h"
#include "storage/vfd.h"

/*
 * The max size for single data file
 */
const uint64 MAX_FILE_SIZE = (uint64)RELSEG_SIZE * BLCKSZ;

CUStorage::CUStorage(const CFileNode& cFileNode, CStoreAllocateStrategy strategy)
    : m_cnode(cFileNode), m_fd(FILE_INVALID), m_strategy(strategy), append_only(false), is_2byte_align(false)
{
    InitFileNamePrefix(cFileNode);
    m_fileName[0] = '\0';  // empty string
    InitCstoreFreeSpace(m_strategy);
}

void CUStorage::Destroy()
{
    if (m_fd != FILE_INVALID)
        CloseFile(m_fd);

    if (m_freespace != NULL) {
        DELETE_EX(m_freespace);
    }
}

CUStorage::~CUStorage()
{
    if (m_freespace != NULL) {
        DELETE_EX(m_freespace);
    }
}

/* create file,  if file exists and isRedo is false then jump elog ERROR,  the caller should table care to close fd */
File CUStorage::CreateFile(char* file_name, int fileId, bool isRedo) const
{
    File fd = -1;
    int fileFlags = O_RDWR | O_CREAT | O_EXCL | PG_BINARY;

    fd = open(file_name, fileFlags, 0600);
    if (fd < 0) {
        int save_errno = errno;

        /*
         * During bootstrap, there are cases where a system relation will be
         * accessed (by internal backend processes) before the bootstrap
         * script nominally creates it.  Therefore, allow the file to exist
         * already, even if isRedo is not set.
         */
        if (isRedo || IsBootstrapProcessingMode()) {
            fileFlags = O_RDWR | PG_BINARY;

            fd = open(file_name, fileFlags, 0600);
        }

        /* be sure to report the error reported by create, not open */
        errno = save_errno;
        if (fd < 0) {
            ereport(ERROR, (errcode_for_file_access(), errmsg("Could not open file \"%s\": %m", file_name)));
        }
    }

    return fd;
}

/* open file, if file not exists then create it */
File CUStorage::OpenFile(char* file_name, int fileId, bool direct_flag)
{
    File fd = WSOpenFile(file_name, fileId, direct_flag);
    if (fd < 0) {
        ereport(ERROR, (errcode_for_file_access(), errmsg("Could not open file \"%s\": %m", file_name)));
    }

    return fd;
}

/* open file, if file not exists then create it */
File CUStorage::WSOpenFile(char* file_name, int fileId, bool direct_flag)
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
    if (m_cnode.m_attid <= 0) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_ATTRIBUTE),
                        errmsg("validate user defined attribute failed!")));
    }
    filenode.forknumber = (ForkNumber)(m_cnode.m_attid + FirstColForkNum);

    return DataFileIdOpenFile(file_name, filenode, (int)fileFlags, 0600);
}

void CUStorage::CloseFile(File fd) const
{
    FileClose(fd);
}

void CUStorage::GetBcmFileName(_out_ char* bcmfile, _in_ int fileId) const
{
    Assert(fileId >= 0);

    // bcm file list: 16385_C1_bcm  16385_C1_bcm.1  16385_C1_bcm.2 ....
    int rc = 0;
    if (fileId > 0) {
        rc =
            snprintf_s(bcmfile, MAXPGPATH, MAXPGPATH - 1, "%s_%s.%d", m_fileNamePrefix, forkNames[BCM_FORKNUM], fileId);
    } else {
        rc = snprintf_s(bcmfile, MAXPGPATH, MAXPGPATH - 1, "%s_%s", m_fileNamePrefix, forkNames[BCM_FORKNUM]);
    }
    securec_check_ss(rc, "", "");
    bcmfile[MAXPGPATH - 1] = '\0';
}

const char* CUStorage::GetColumnFileName() const
{
    // cu file name without segment no. :16385_C1
    return (const char*)m_fileNamePrefix;
}

void CUStorage::GetFileName(_out_ char* fileName, _in_ const size_t capacity, _in_ const int fileId) const
{
    Assert(fileId >= 0);

    // it means a cu file for one column of a reltaion.
    // it's different from bcm file name, its file list:
    //   16385_C1.0 16385_C1.1 16385_C1.2 ...
    int rc = snprintf_s(fileName, capacity, capacity - 1, "%s.%d", m_fileNamePrefix, fileId);
    securec_check_ss(rc, "", "");
    fileName[capacity - 1] = '\0';
}


void CUStorage::InitCstoreFreeSpace(CStoreAllocateStrategy strategy)
{
    if (USING_FREE_SPACE == strategy) {
        m_freespace = New(CurrentMemoryContext) CStoreFreeSpace();
    } else {
        m_freespace = NULL;
    }
}

// Get common FileName prefix only once.
//
void CUStorage::InitFileNamePrefix(_in_ const CFileNode& cFileNode)
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
        pathlen = strlen(GLOTBSDIR) + 1 + OIDCHARS + 1 + strlen(attr_name) + 1;
        rc = snprintf_s(m_fileNamePrefix, sizeof(m_fileNamePrefix), pathlen, "%s/%u_%s", GLOTBSDIR, reloid, attr_name);
        securec_check_ss(rc, "", "");
    } else if (spcoid == DEFAULTTABLESPACE_OID) {
        /* The default tablespace is {datadir}/base */
        pathlen = strlen(DEFTBSDIR) + 1 + OIDCHARS + 1 + OIDCHARS + 1 + strlen(attr_name) + 1;
        rc = snprintf_s(m_fileNamePrefix, sizeof(m_fileNamePrefix), pathlen, "%s/%u/%u_%s", DEFTBSDIR, dboid, reloid,
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

static void SaveCUReportIOError(
    const char* fileName, uint64 writeOffset, int writtenBtyes, int expectedBytes, int expectedTotalBytes, int align_size)
{
    if (writtenBtyes > 0) {
        Assert(align_size > 0);
        if (writtenBtyes % align_size != 0) {
            /* later load will report error about not-matched align */
            ereport(ERROR,
                    (errcode_for_file_access(),
                     errmsg("CU partial write, file \"%s\", offset(%lu), total(%d), expected(%d), actual(%d): %m",
                            fileName,
                            writeOffset,
                            expectedTotalBytes,
                            expectedBytes,
                            writtenBtyes),
                     errhint("CU file will be not page aligned, Check free disk space")));
        } else {
            ereport(ERROR,
                    (errcode_for_file_access(),
                     errmsg("CU partial write, file \"%s\", offset(%lu), total(%d), expected(%d), actual(%d): %m",
                            fileName,
                            writeOffset,
                            expectedTotalBytes,
                            expectedBytes,
                            writtenBtyes),
                     errhint("Check free disk space.")));
        }
    } else {
        ereport(ERROR,
                (errcode_for_file_access(),
                 errmsg("could not extend file \"%s\", offset(%lu), total(%d), expected(%d), actual(%d): %m",
                        fileName,
                        writeOffset,
                        expectedTotalBytes,
                        expectedBytes,
                        writtenBtyes),
                 errhint("Check free disk space.")));
    }
}

static void LoadCUReportIOError(
    const char* fileName, uint64 readOffset, int readBytes, int expectedBytes, int expectedTotalBytes)
{
    if (errno == ENOENT) {
        /* file not exists */
        ereport(ERROR,
                (errcode_for_file_access(),
                 errmsg("read file \"%s\" failed, %m", fileName),
                 errdetail("offset(%lu), load size(%d), expected read(%d), actual read(%d)",
                           readOffset,
                           expectedTotalBytes,
                           expectedBytes,
                           readBytes)));
    } else if (readBytes >= 0) {
        if (readBytes == 0) {
            /* no data is read from */
            ereport(ERROR,
                    (errcode_for_file_access(),
                     errmsg(
                         "no data is read from \"%s\", offset(%lu), load size(%d), expected read(%d), actual read(%d)",
                         fileName,
                         readOffset,
                         expectedTotalBytes,
                         expectedBytes,
                         readBytes)));
        } else {
            /* only some data is read from */
            ereport(ERROR,
                    (errcode_for_file_access(),
                     errmsg("some data are read from \"%s\", offset(%lu), load size(%d), expected read(%d), actual "
                            "read(%d)",
                            fileName,
                            readOffset,
                            expectedTotalBytes,
                            expectedBytes,
                            readBytes),
                     errdetail("maybe CU file is not page aligned, or with not-completed CU data"),
                     errhint(
                         "maybe you should upgrade cstore data files first, or there were CU written IO error before")));
        }
    } else {
        ereport(ERROR,
                (errcode_for_file_access(),
                 errmsg("could not read file \"%s\", offset(%lu), load size(%d), expected read(%d), actual read(%d): %m",
                        fileName,
                        readOffset,
                        expectedTotalBytes,
                        expectedBytes,
                        readBytes)));
    }
}


static inline int min(int a, int b)
{
    return (a > b ? b : a);
}

void CUStorage::SaveCU(char* write_buf, _in_ uint64 offset, _in_ int size, bool direct_flag, bool for_extension)
{
    int writeFileId = offset / MAX_FILE_SIZE;
    uint64 writeOffset = offset % MAX_FILE_SIZE;
    int write_size = min(size, (int)(MAX_FILE_SIZE - writeOffset));
    int left_size = size - write_size;
    Oid tableSpaceOid = m_cnode.m_rnode.spcNode;
    char tmpFileName[MAXPGPATH] = {0};
    errno_t rc = 0;

    if (append_only)
        TableSpaceUsageManager::IsExceedMaxsize(tableSpaceOid, size, false);

    while (write_size > 0) {
        GetFileName(tmpFileName, MAXPGPATH, writeFileId);
        if (strcmp(tmpFileName, m_fileName) != 0) {
            if (m_fd != FILE_INVALID) {
                /*
                 * Flush data if switch data file. We don't do so during
                 * file extension since we'll soon fsync the actual data.
                 */
                if (!for_extension)
                    FlushDataFile();
                FileClose(m_fd);
            }

            m_fd = OpenFile(tmpFileName, writeFileId, direct_flag);
            Assert(m_fd != FILE_INVALID);
            rc = strcpy_s(m_fileName, MAXPGPATH, tmpFileName);
            securec_check_c(rc, "\0", "\0");
        }
        Assert(m_fd != FILE_INVALID);

        /*
         * DSS pwrite does not allow file offset beyond the end of the file,
         * so we need fallocate first. In extend situation, lock is already
         * acquire in previous step.
         */
        vfd *vfdcache = GetVfdCache();
        int fd = vfdcache[m_fd].fd;
        if (is_dss_fd(fd)) {
            off_t fileSize = dss_get_file_size(m_fileName);
            if ((off_t)writeOffset > fileSize) {
                (void)dss_fallocate_file(fd, 0, fileSize, (off_t)writeOffset - fileSize);
            }
        }

        int writtenBytes;
        if (ENABLE_DSS) {
            char *buffer_ori = (char*)palloc(BLCKSZ + write_size);
            char *buffer_ali = (char*)BUFFERALIGN(buffer_ori);
            rc = memcpy_s(buffer_ali, write_size, write_buf, write_size);
            securec_check(rc, "", "");
            writtenBytes = FilePWrite(m_fd, buffer_ali, write_size, (off_t)writeOffset);
            pfree(buffer_ori);
            buffer_ali = NULL;
        } else {
            writtenBytes = FilePWrite(m_fd, write_buf, write_size, (off_t)writeOffset);
        }
        if (writtenBytes != write_size) {
            int align_size = is_2byte_align ? ALIGNOF_TIMESERIES_CUSIZE : ALIGNOF_CUSIZE;
            SaveCUReportIOError(tmpFileName, writeOffset, writtenBytes, write_size, size, align_size);
        }

        ++writeFileId;
        writeOffset = 0;
        write_buf += write_size;
        write_size = (((unsigned int)left_size > MAX_FILE_SIZE) ? MAX_FILE_SIZE : left_size);
        left_size -= write_size;
    }

    if (left_size != 0) {
        ereport(ERROR, (errcode_for_file_access(),
                        errmsg("write file  \"%s\" failed in savecu!", tmpFileName)));
    }
}

// After remote read CU, need overwrite local CU
void CUStorage::OverwriteCU(
    _in_ char* write_buf, _in_ uint64 offset, _in_ int size, bool direct_flag, bool for_extension)
{
    // check offset and size
    int writeFileId = offset / MAX_FILE_SIZE;
    uint64 writeOffset = offset % MAX_FILE_SIZE;
    int write_size = min(size, (int)(MAX_FILE_SIZE - writeOffset));
    int left_size = size - write_size;
    char tmpFileName[MAXPGPATH] = {0};
    errno_t rc = 0;

    // Overwirte CU, do not increase the max size
    while (write_size > 0) {
        GetFileName(tmpFileName, MAXPGPATH, writeFileId);
        if (strcmp(tmpFileName, m_fileName) != 0) {
            if (m_fd != FILE_INVALID) {
                /*
                 * Flush data if switch data file. We don't do so during
                 * file extension since we'll soon fsync the actual data.
                 */
                if (!for_extension)
                    FlushDataFile();
                FileClose(m_fd);
            }

            m_fd = OpenFile(tmpFileName, writeFileId, direct_flag);
            Assert(m_fd != FILE_INVALID);
            rc = strcpy_s(m_fileName, MAXPGPATH, tmpFileName);
            securec_check(rc, "\0", "\0");
        }
        if (m_fd == FILE_INVALID) {
            ereport(ERROR, (errcode_for_file_access(), errmsg("could not open file \"%s\": %m", tmpFileName)));
        }

        int nbytes = 0;
        if (ENABLE_DSS) {
            char *buffer_ori = (char*)palloc(BLCKSZ + write_size);
            char *buffer_ali = (char*)BUFFERALIGN(buffer_ori);
            rc = memcpy_s(buffer_ali, write_size, write_buf, write_size);
            securec_check(rc, "", "");
            nbytes = FilePWrite(m_fd, buffer_ali, write_size, writeOffset);
            pfree(buffer_ori);
            buffer_ali = NULL;
        } else {
            nbytes = FilePWrite(m_fd, write_buf, write_size, writeOffset);
        }
        if (nbytes != write_size) {
            // just warning
            ereport(WARNING,
                    (errcode_for_file_access(),
                     errmsg("Overwrite CU failed. file \"%s\" , offset(%lu), size(%d), expect_write_size(%d), "
                            "acture_write_size(%d): %m",
                            tmpFileName,
                            writeOffset,
                            size,
                            write_size,
                            nbytes),
                     handle_in_client(true)));
        }
        ++writeFileId;
        writeOffset = 0;
        write_buf += write_size;
        write_size = (((unsigned int)left_size > MAX_FILE_SIZE) ? MAX_FILE_SIZE : left_size);
        left_size -= write_size;
    }

    if (left_size != 0) {
        ereport(ERROR, (errcode_for_file_access(),
                        errmsg("write file \"%s\" failed in OverwriteCU!", tmpFileName)));
    }
}

void CUStorage::Load(_in_ uint64 offset, _in_ int size, __inout char* outbuf, bool direct_flag)
{
    int readFileId = CU_FILE_ID(offset);
    uint64 readOffset = CU_FILE_OFFSET(offset);
    int read_size = min(size, (int)(MAX_FILE_SIZE - readOffset));
    int left_size = size - read_size;

    char* read_buf = outbuf;
    char tmpFileName[MAXPGPATH];
    errno_t rc = 0;

    while (read_size > 0) {
        GetFileName(tmpFileName, MAXPGPATH, readFileId);
        if (strcmp(tmpFileName, m_fileName) != 0) {
            if (m_fd != FILE_INVALID)
                FileClose(m_fd);
            m_fd = OpenFile(tmpFileName, readFileId, direct_flag);

            if (m_fd == FILE_INVALID) {
                ereport(ERROR, (errcode_for_file_access(), errmsg("Could not open file \"%s\"", tmpFileName)));
            }

            rc = strcpy_s(m_fileName, MAXPGPATH, tmpFileName);
            securec_check_c(rc, "\0", "\0");
        }

        int nbytes = FilePRead(m_fd, read_buf, read_size, readOffset);
        if (nbytes != read_size) {
            LoadCUReportIOError(tmpFileName, readOffset, nbytes, read_size, size);
        }

        ++readFileId;
        readOffset = 0;
        read_buf += read_size;
        read_size = (((unsigned int)left_size > MAX_FILE_SIZE) ? MAX_FILE_SIZE : left_size);
        left_size -= read_size;
    }
    if (left_size != 0) {
        ereport(ERROR, (errcode_for_file_access(),
                        errmsg("read file \"%s\" failed in load!", tmpFileName)));
    }
}

int CUStorage::WSLoad(_in_ uint64 offset, _in_ int size, __inout char* outbuf, bool direct_flag)
{
    int readFileId = CU_FILE_ID(offset);
    uint64 readOffset = CU_FILE_OFFSET(offset);
    int read_size = min(size, (int)(MAX_FILE_SIZE - readOffset));
    int left_size = size - read_size;
    errno_t rc = 0;

    char* read_buf = outbuf;
    char tmpFileName[MAXPGPATH];
    bool isCUReadSizeValid = false;

    const int CUALIGNSIZE = is_2byte_align ? ALIGNOF_TIMESERIES_CUSIZE : ALIGNOF_CUSIZE;
    isCUReadSizeValid = (read_size > 0 && 0 == read_size % CUALIGNSIZE);

    if (!isCUReadSizeValid) {
        ereport(ERROR,
                (errcode_for_file_access(),
                 errmsg("unexpected cu file read info: offset(%lu), size(%d), readFileId(%d), readOffset(%lu), "
                        "expect_read_size(%d).",
                        offset,
                        size,
                        readFileId,
                        readOffset,
                        read_size)));

        return -1;
    }

    while (read_size > 0) {
        GetFileName(tmpFileName, MAXPGPATH, readFileId);
        if (strcmp(tmpFileName, m_fileName) != 0) {
            if (m_fd != FILE_INVALID)
                FileClose(m_fd);
            m_fd = WSOpenFile(tmpFileName, readFileId, direct_flag);

            if (FILE_INVALID == m_fd)
                return 0;

            rc = strcpy_s(m_fileName, MAXPGPATH, tmpFileName);
            securec_check(rc, "\0", "\0");
        }

        int nbytes = 0;

        /* IO collector and IO scheduler for cstore insert */
        if (ENABLE_WORKLOAD_CONTROL)
            IOSchedulerAndUpdate(IO_TYPE_READ, 1, IO_TYPE_COLUMN);

        if ((nbytes = FilePRead(m_fd, read_buf, read_size, readOffset)) != read_size) {
            if (0 == nbytes) {
                if (u_sess->attr.attr_storage.HaModuleDebug)
                    ereport(NOTICE,
                            (errcode_for_file_access(),
                             errmsg("HA-WSLoad: read file \"%s\" to get 0 byte, please check the according cu file.",
                                    tmpFileName)));
                return 0;
            }

            if (nbytes % CUALIGNSIZE != 0) {
                ereport(ERROR,
                        (errcode_for_file_access(),
                         errmsg("read file \"%s\" failed, offset(%lu), size(%d), expect_read_size(%d), "
                                "acture_read_size(%d), maybe you should upgrade cstore data files first",
                                tmpFileName,
                                offset,
                                size,
                                read_size,
                                nbytes)));
            } else {
                ereport(ERROR,
                        (errcode_for_file_access(),
                         errmsg("could not read file \"%s\", offset(%lu), size(%d), expect_read_size(%d), "
                                "acture_read_size(%d): %m",
                                tmpFileName,
                                offset,
                                size,
                                read_size,
                                nbytes)));
            }
        }

        ++readFileId;
        readOffset = 0;
        read_buf += read_size;
        read_size = (((unsigned int)left_size > MAX_FILE_SIZE) ? MAX_FILE_SIZE : left_size);
        left_size -= read_size;
    }

    if (left_size != 0) {
        ereport(ERROR, (errcode_for_file_access(),
                        errmsg("read file \"%s\" failed in wsload!", tmpFileName)));
    }

    return size;
}

/*
 * @Description: get fd of cu stored in file
 * @Param[IN] offset: cu_pointer
 * @Return: virtual fd
 * @See also:
 */
File CUStorage::GetCUFileFd(uint64 offset)
{
    char tmpFileName[MAXPGPATH];
    File file;

    int readFileId = CU_FILE_ID(offset);
    GetFileName(tmpFileName, MAXPGPATH, readFileId);
    file = OpenFile(tmpFileName, readFileId, true);
    return file;
}

/*
 * @Description: get offset of cu stored in file
 * @Param[IN] offset: cu_pointer
 * @Return: file offset
 * @See also:
 */
uint64 CUStorage::GetCUOffsetInFile(uint64 offset) const
{
    uint64 readOffset = CU_FILE_OFFSET(offset);
    return readOffset;
}

/*
 * @Description:  one cu may store in many data files, check this for adio
 * @Param[IN] offset:cu_pointer
 * @Param[IN] size: cu size
 * @Return: true -- in one file; false -- not in one file
 * @See also:
 */
bool CUStorage::IsCUStoreInOneFile(uint64 offset, int size) const
{
    uint64 readOffset = CU_FILE_OFFSET(offset);
    int left_size = MAX_FILE_SIZE - readOffset;

    if (size > left_size) {
        return false;
    }
    return true;
}

/*
 * @Description: pre allocate file space in adio mode for we need file phy address continue
 * @Param[IN] extend_offset: record extend offset
 * @Param[IN] size: extend size
 * @See also:
 */
void CUStorage::FastExtendFile(uint64 extend_offset, uint32 size, bool keep_size)
{
    File fd;
    uint64 file_offset;
    uint32 left_size;
    uint32 extend_size;

    while (size > 0) {
        file_offset = CU_FILE_OFFSET(extend_offset);
        left_size = MAX_FILE_SIZE - file_offset;
        extend_size = (size < left_size) ? size : left_size;

        fd = GetCUFileFd(extend_offset);
        FileFastExtendFile(fd, file_offset, extend_size, keep_size);
        CloseFile(fd);

        extend_offset += extend_size;
        Assert(size >= extend_size);
        size -= extend_size;
    }
    Assert(size == 0);
    return;
}

/*
 * @Description: get align cu offset, used when upgrade
 * @Param[IN] offset: cu_pointer
 * @Return: cu offset
 * @See also:
 */
uint64 CUStorage::GetAlignCUOffset(uint64 offset) const
{
    const int CUALIGNSIZE = is_2byte_align ? ALIGNOF_TIMESERIES_CUSIZE : ALIGNOF_CUSIZE;
    uint64 readOffset = CU_FILE_OFFSET(offset);
    uint64 remainder = readOffset % CUALIGNSIZE;
    if (remainder != 0) {
        ereport(DEBUG1, (errmodule(MOD_ADIO), errmsg("GetAlignCUOffset: find un align offset(%lu)", offset)));
        if (readOffset > (uint64)CUALIGNSIZE) {
            return offset - remainder;
        } else if (readOffset < (uint64)CUALIGNSIZE) {
            return offset - readOffset;
        } else {
            Assert(0);
        }
    }
    return offset;
}

/*
 * @Description:  get align cu size, used when upgrade
 * @Param[IN] size: cu_pointer
 * @Return: cu size
 * @See also:
 */
int CUStorage::GetAlignCUSize(int size) const
{
    const int CUALIGNSIZE = is_2byte_align ? ALIGNOF_TIMESERIES_CUSIZE : ALIGNOF_CUSIZE;
    int remainder = size % CUALIGNSIZE;
    if (remainder != 0) {
        ereport(DEBUG1, (errmodule(MOD_ADIO), errmsg("GetAlignCUOffset: find un align size(%d)", size)));
        return size + CUALIGNSIZE - remainder;
    }
    return size;
}

/*
 * @Description: load CU data from CU file
 * @Param[IN/OUT] cuPtr: CU object to load data
 * @Param[IN] direct_flag: if ADIO feature is enable, DIO is used.
 * @Param[IN] inCUCache: whether this cuPtr is in CU cache.
 * @Param[IN] offset: CU data logic offset in logic file
 * @Param[IN] size: CU data size
 * @See also:
 */
void CUStorage::LoadCU(_in_ CU* cuPtr, _in_ uint64 offset, _in_ int size, bool direct_flag, bool inCUCache)
{
    if (size < 0 || (uint64)size > MAX_FILE_SIZE) {
        ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION),
                        errmsg("invalid size(%u) in CUStorage::LoadCU", size)));
    }

    if (size == 0) {
        cuPtr->m_compressedBufSize = 0;
        ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION),
                        errmsg("CUStorage::LoadCU size = 0")));
        return;
    }

    uint64 load_offset;
    int load_size;

    load_offset = GetAlignCUOffset(offset);
    cuPtr->m_head_padding_size = offset - load_offset;
    load_size = GetAlignCUSize(cuPtr->m_head_padding_size + size);

    // !!! FUTURE CASE.. We can optimize memory allocation.
    //
    // In order to avoid over-boundary read in the function readData, more 8 byte memory is allocated.
    cuPtr->m_compressedLoadBuf = (char*)CStoreMemAlloc::Palloc(load_size + 8, !inCUCache);
    Load(load_offset, load_size, cuPtr->m_compressedLoadBuf, direct_flag);

    // the complete CU data has been loaded, so set the cu size.
    // we will check this value during decompressing cu data.
    //
    cuPtr->m_compressedBuf = cuPtr->m_compressedLoadBuf + cuPtr->m_head_padding_size;
    cuPtr->SetCUSize(size);
    cuPtr->m_compressedBufSize = size;
    cuPtr->m_cache_compressed = true;
}

/*
 * @Description: load CU data from remote node
 * @Param[IN] cuPtr: CU object to load data
 * @Param[IN] direct_flag: if ADIO feature is enable, DIO is used.
 * @Param[IN] inCUCache: whether this cuPtr is in CU cache.
 * @Param[IN] offset: CU data logic offset in logic file
 * @Param[IN] size: CU data size
 * @See also:
 */
void CUStorage::RemoteLoadCU(_in_ CU* cuPtr, _in_ uint64 offset, _in_ int size, bool direct_flag, bool inCUCache)
{
    /* the caller should alloc the m_compressedLoadBuf */
    Assert(cuPtr->m_compressedLoadBuf != NULL);

    /* get offset and size */
    uint64 load_offset = GetAlignCUOffset(offset);
    cuPtr->m_head_padding_size = offset - load_offset;
    int load_size = GetAlignCUSize(cuPtr->m_head_padding_size + size);

    /* get current xlog insert lsn */
    XLogRecPtr cur_lsn = GetInsertRecPtr();

    /* get remote address */
    char remote_address1[MAXPGPATH] = { 0 }; /* remote_address1[0] = '\0'; */
    char remote_address2[MAXPGPATH] = { 0 }; /* remote_address2[0] = '\0'; */
    GetRemoteReadAddress(remote_address1, remote_address2, MAXPGPATH);

    char* remote_address = remote_address1;
    int retry_times = 0;

retry:
    if (remote_address[0] == '\0' || remote_address[0] == '@')
        ereport(ERROR, (errcode(ERRCODE_IO_ERROR), (errmodule(MOD_REMOTE), errmsg("remote not available"))));

    ereport(LOG,
            (errmodule(MOD_REMOTE),
             errmsg("remote read CU file, %s offset %lu size %d from %s",
                    GetColumnFileName(),
                    offset,
                    size,
                    remote_address)));

    PROFILING_REMOTE_START();

    int ret_code = RemoteGetCU(remote_address,
                                 m_cnode.m_rnode.spcNode,
                                 m_cnode.m_rnode.dbNode,
                                 m_cnode.m_rnode.relNode,
                                 m_cnode.m_attid,
                                 load_offset,
                                 load_size,
                                 cur_lsn,
                                 cuPtr->m_compressedLoadBuf);

    PROFILING_REMOTE_END_READ(size, (ret_code == REMOTE_READ_OK));

    if (ret_code != REMOTE_READ_OK) {
        if (IS_DN_DUMMY_STANDYS_MODE() || retry_times >= 1) {
            ereport(ERROR,
                    (errcode(ERRCODE_IO_ERROR),
                     (errmodule(MOD_REMOTE),
                      errmsg("remote read failed from %s, %s", remote_address, RemoteReadErrMsg(ret_code)))));
        } else

        {
            ereport(WARNING,
                    (errmodule(MOD_REMOTE),
                     errmsg("remote read failed from %s, %s, try another", remote_address, RemoteReadErrMsg(ret_code)),
                     handle_in_client(true)));

            /* Check interrupts */
            CHECK_FOR_INTERRUPTS();

            remote_address = remote_address2;
            ++retry_times;
            goto retry; /* jump out  retry_times >= 1 */
        }
    }

    // the complete CU data has been loaded, so set the cu size.
    // we will check this value during decompressing cu data.
    //
    cuPtr->m_compressedBuf = cuPtr->m_compressedLoadBuf + cuPtr->m_head_padding_size;
    cuPtr->SetCUSize(size);
    cuPtr->m_compressedBufSize = size;
    cuPtr->m_cache_compressed = true;
}

// Flush data to disk
// FileSync respect sync_method variable
//
void CUStorage::FlushDataFile() const
{
    if (m_fd != FILE_INVALID) {
        if (FileSync(m_fd) < 0) {
            ereport(ERROR, (errcode_for_file_access(), errmsg("flush file \"%s\" failed: %m", m_fileName)));
        }
    }
}

uint64 CUStorage::AllocSpace(_in_ int size)
{
    uint64 offset = InvalidCStoreOffset;
    int align_size = is_2byte_align ? ALIGNOF_TIMESERIES_CUSIZE : ALIGNOF_CUSIZE;
    switch (m_strategy) {
        case USING_FREE_SPACE:
            offset = CStoreAllocator::TryAcquireSpaceFromFSM(m_freespace, size, align_size);
            if (offset != InvalidCStoreOffset) {
                append_only = false;
                break;
            }
        /* if no free space fits, use the APPEND_ONLY way */
        case APPEND_ONLY:
            offset = CStoreAllocator::AcquireSpace(m_cnode, size, align_size);
            append_only = true;
            break;
        default:
            Assert(false);
            break;
    }
    return offset;
}

void CUStorage::CreateStorage(int fileId, bool isRedo) const
{
    File fd;
    char filePath[MAXPGPATH];

    GetFileName(filePath, MAXPGPATH, fileId);
    fd = CreateFile(filePath, fileId, isRedo);
    close(fd);
}

bool CUStorage::IsDataFileExist(int fileId) const
{
    char tmpFileName[MAXPGPATH];
    GetFileName(tmpFileName, MAXPGPATH, fileId);

    struct stat st;
    if (lstat((const char*)tmpFileName, &st) == -1)
        return false;

    return true;
}

bool CUStorage::IsBcmFileExist(_in_ int fileId) const
{
    char tmpFileName[MAXPGPATH];
    GetBcmFileName(tmpFileName, fileId);

    struct stat st;
    if (lstat((const char*)tmpFileName, &st) == -1)
        return false;

    return true;
}

/*
 * @Description:  truncate column data files which relation CREATE and TRUNCATE in same XACT block
 */
void CUStorage::TruncateDataFile()
{
    int fileId = 0;
    char tmpFileName[MAXPGPATH];

    while (1) {
        if (!IsDataFileExist(fileId))
            break;

        GetFileName(tmpFileName, MAXPGPATH, fileId);
        File vfd = OpenFile(tmpFileName, fileId, false);
        if (FileTruncate(vfd, 0)) {
            ereport(WARNING, (errmsg("could not ftruncate file \"%s\": %m", tmpFileName)));
        }
        CloseFile(vfd);

        ++fileId;
    }
}

/*
 * @Description:  truncate column bcm files which relation CREATE and TRUNCATE in same XACT block
 */
void CUStorage::TruncateBcmFile()
{
    int fileId = 0;
    char tmpFileName[MAXPGPATH];

    while (1) {
        if (!IsBcmFileExist(fileId))
            break;

        GetBcmFileName(tmpFileName, fileId);
        File vfd = OpenFile(tmpFileName, fileId, false);
        if (FileTruncate(vfd, 0)) {
            ereport(WARNING, (errmsg("could not ftruncate file \"%s\": %m", tmpFileName)));
        }
        CloseFile(vfd);

        ++fileId;
    }
}

void CUStorage::Set2ByteAlign(bool is_2byte_align)
{
    this->is_2byte_align = is_2byte_align;
}

bool CUStorage::Is2ByteAlign()
{
    return is_2byte_align;
}

CUFile::CUFile(const RelFileNode& fileNode, int col)
{
    CFileNode cFileNode(fileNode, col, MAIN_FORKNUM);
    m_colStorage = New(CurrentMemoryContext) CUStorage(cFileNode);

    ADIO_RUN()
    {
        m_buffer = (char*)adio_align_alloc(CUFILE_MAX_BUF_SIZE);
    }
    ADIO_ELSE()
    {
        m_buffer = (char*)palloc(CUFILE_MAX_BUF_SIZE);
    }
    ADIO_END();

    m_maxpos = 0;
    m_maxFileSize = 0;
    m_curFileOffset = 0;
    m_fileId = -1;
}

CUFile::~CUFile()
{
    m_colStorage = NULL;
    m_buffer = NULL;
}

void CUFile::Destroy()
{
    DELETE_EX(m_colStorage);

    ADIO_RUN()
    {
        adio_align_free(m_buffer);
    }
    ADIO_ELSE()
    {
        pfree(m_buffer);
        m_buffer = NULL;
    }
    ADIO_END();
}

char* CUFile::Read(uint64 offset, int wantSize, int* realSize, int align_size)
{
    Assert(align_size > 0);
    Assert(wantSize % align_size == 0);
    Assert(CUFILE_MAX_BUF_SIZE % align_size == 0);

    const bool notInited = (-1 == m_fileId);
    bool switchId = (m_fileId != (int)(CU_FILE_ID(offset)));

    uint64 bufUpBound = MAX_FILE_SIZE * m_fileId + m_curFileOffset;
    uint64 bufDownOffset = bufUpBound - m_maxpos;
    bool notInBuf = (offset < bufDownOffset || offset >= bufUpBound);

    if (notInited || switchId || notInBuf) {
        m_fileId = CU_FILE_ID(offset);
        m_curFileOffset = CU_FILE_OFFSET(offset);
        // we don't call FileClose() because Load() will do the real work
        //
        if (notInited || switchId || (m_curFileOffset >= m_maxFileSize)) {
            char tmpFileName[MAXPGPATH];
            m_colStorage->GetFileName(tmpFileName, MAXPGPATH, m_fileId);

            struct stat st;
            if (lstat((const char*)tmpFileName, &st) == 0) {
                m_maxFileSize = st.st_size;
            } else {
                ereport(ERROR, (errcode_for_file_access(), errmsg("could not read file \"%s\": %m", tmpFileName)));
            }
        }

        // load data into buffer
        //
        m_maxpos = Min((m_maxFileSize - m_curFileOffset), CUFILE_MAX_BUF_SIZE);
        if (m_maxpos == 0) {
            *realSize = 0;
            return NULL;
        } else {
            m_colStorage->Load(offset, m_maxpos, m_buffer, false);
            m_curFileOffset += (uint32)m_maxpos;

            // recompute down and up bound of this buffer after load data
            //
            bufUpBound = MAX_FILE_SIZE * m_fileId + m_curFileOffset;
            bufDownOffset = bufUpBound - m_maxpos;
        }
    }

    // read data from this buffer and its start position is readPos
    //
    int readPos = offset - bufDownOffset;
    char* outdata = m_buffer + readPos;
    *realSize = Min(wantSize, (m_maxpos - readPos));
    return outdata;
}

/* Get the data file size of each column by forloop each segment file of column */
uint64 GetColDataFileSize(Relation rel, int attid)
{
    CFileNode tmpNode(rel->rd_node, attid, MAIN_FORKNUM);
    CUStorage custore(tmpNode);
    char pathname[MAXPGPATH] = "\0";
    uint64 size = 0;

    errno = 0;
    /* segment file id must be continues */
    for (int i = 0;; i++) {
        struct stat fst;
        custore.GetFileName(pathname, MAXPGPATH, i);

        if (stat(pathname, &fst) < 0) {
            /* pathname file is not exist */
            if (FILE_POSSIBLY_DELETED(errno))
                break;
            else
                ereport(ERROR, (errcode_for_file_access(), errmsg("could not stat file \"%s\": %m", pathname)));
        }
        size += fst.st_size;
    }
    custore.Destroy();
    return size;
}
