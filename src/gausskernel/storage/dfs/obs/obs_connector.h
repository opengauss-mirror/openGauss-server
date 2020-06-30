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
 * obs_connector.h
 *
 *
 *
 *
 * IDENTIFICATION
 *        src/gausskernel/storage/dfs/obs/obs_connector.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef OBS_CONNECTOR_H
#define OBS_CONNECTOR_H

#include "access/obs/obs_am.h"
#include "storage/dfs/dfs_connector.h"

namespace dfs {
class OBSConnector : public DFSConnector {
public:
    OBSConnector(MemoryContext ctx, Oid foreignTableId);
    OBSConnector(MemoryContext ctx, ObsOptions *obsOptions);
    virtual ~OBSConnector();
    void Destroy();

private:
    /*
     * Get the OBS server option.
     * @_in param foreignTableId, the given foreign table oid.
     */
    virtual ObsOptions *getOptionFromCache(Oid foreignTableId);
    /*
     * Check if the path in hdfs is a file not directory, log error if the path does not
     * exist.
     * @_in param filePath: the path of the hdfs file/directory.
     * @return Return true: the path is a file; false: the path is not a file but a directory.
     */
    virtual bool isDfsFile(const char *filePath);
    virtual bool isDfsFile(const char *filePath, bool throw_error);

    /* Check if the path is a empty file, log error if the path does not exist. */
    virtual bool isDfsEmptyFile(const char *filePath);

    /*
     * Get the file size of the path. Return -1 if the path does not exist.
     * @_in_param filePath: the path of the hdfs file/directory
     * @return Return the size.
     */
    virtual int64_t getFileSize(const char *filePath);

    /* Get the handler to connect the DFS system. */
    virtual void *getHandler() const;

    /*
     * Get list of files/directories for a given directory-path.
     * hdfsFreeFileInfo is called internally to deallocate memory.
     * Log error if the path does not exist.
     * @_in_param folderPath: The path of the directory.
     * @return Return a list of filepath. Return NULL on error.
     */
    virtual List *listDirectory(char *folderPath);
    virtual List *listDirectory(char *folderPath, bool throw_error);

    virtual List *listObjectsStat(char *path, const char *primitivePrefix = NULL);

    /*
     * Get the block information of the file path.
     * Log error if the path does not exist.
     * @_in_param filePath: The path of the file.
     * @return Return a pointer to DFSBlockInfo.
     */
    virtual DFSBlockInfo *getBlockLocations(char *filePath);

    /*
     * Drop directory. Return 0 if the path does not exist.
     * @_in_param path The path of the directory.
     * @_in_param recursive if path is a directory and set to
     *		non-zero, the directory is deleted else throws an exception. In
     *		case of a file the recursive argument is irrelevant.
     * @return Returns 0 on success, -1 on error.
     */
    virtual int dropDirectory(const char *path, int recursive);

    /*
     * Make a directory using the given path.
     * @_in_param path The path of the directory.
     * @return Returns 0 on success, -1 on error.
     */
    virtual int createDirectory(const char *path);

    /*
     * Make a file using the given path.
     * @_in_param path The path of the file to make.
     * @_in_param flags - an | of bits/fcntl.h file flags - supported flags
     *		are O_RDONLY, O_WRONLY (meaning create or overwrite i.e., implies O_TRUNCAT),
     *		O_WRONLY|O_APPEND and O_SYNC. Other flags are generally ignored other than
     *		(O_RDWR || (O_EXCL & O_CREAT)) which return NULL and set errno equal ENOTSUP.
     * return Returns 0 on success, -1 on error.
     */
    virtual int openFile(const char *path, int flag);

    /* Delete the file using the given file path. Before calling this
     * function, pathExists must be checked.
     * @_in_param path The path of the file.
     * @_in_param recursive if path is a directory and set to
     *		non-zero, the directory is deleted else throws an exception. In
     *		case of a file the recursive argument is irrelevant.
     * @return Returns 0 on success, -1 on error.
     */
    virtual int deleteFile(const char *path, int recursive);

    /*
     * pathExists - Checks if a given path exsits on the filesystem
     * @param path The path to look for
     * @return Returns 0 on success, -1 on error.
     */
    virtual bool pathExists(const char *filePath);

    virtual bool existsFile(const char *path);

    /*
     * check if the current connector has a valid hdfs file handler.
     * @return true if the file handler is valid, false on invalid.
     */
    virtual bool hasValidFile() const;

    /*
     * Write the buffer into the current file according to the length.
     * @_in_param buffer: The content to be write to file.
     * @_in_param length: The length of the byte to write.
     * @return Return the bytes actually write, -1 on error.
     */
    virtual int writeCurrentFile(const char *buffer, int length);

    /*
     * Read fixed size from the offset of the file into the buffer.
     * @_out_param buffer: The buffer to be filled.
     * @_in_param length: The size of bytes expected.
     * @_in_param offset: The offset at which the reading starts.
     * @return the bytes actually read, -1 on error.
     */
    virtual int readCurrentFileFully(char *buffer, int length, int64 offset);

    /*
     * Flush out the data in client's user buffer. After the return of this call,
     * new readers will see the data.
     * @return 0 on success, -1 on error and sets errno
     */
    virtual int flushCurrentFile();
    virtual void closeCurrentFile();

    /*
     * change the file's authority.
     * @_in_param filePath: The absolute path of the file to be set.
     * @_in_param mode: The mode of the authority like 600 or 755.
     * Return 0 if succeed. Return 1 if fail.
     */
    virtual int chmod(const char *filePath, short mode);

    /*
     * Set the label expression on dfs file.
     * @_in_param filePath: The absolute path of the file to be set.
     * @_in_param expression: The label string like "labelA,labelB".
     * Return 0 if succeed. Return 1 if fail.
     */
    virtual int setLabelExpression(const char *filePath, const char *expression);

    /*
     * Get the timestamp of the last modification.
     */
    virtual int64 getLastModifyTime(const char *filePath);

    /*
     * Fetch the configure value from the config file.
     */
    virtual const char *getValue(const char *key, const char *defValue) const;

    /*
     * Get connection type
     */
    virtual int getType();

    OBSReadWriteHandler *searchConnectorCache(Oid foreignTableId);
    OBSReadWriteHandler *createRWHandler(ObsOptions *obsOptions);

private:
    MemoryContext m_memcontext;
    OBSReadWriteHandler *m_handler;
    ServerTypeOption srvType;
};
}  // namespace dfs
#endif
