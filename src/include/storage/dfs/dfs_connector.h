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
 * dfs_connector.h
 * 
 * 
 * 
 * IDENTIFICATION
 *        src/include/storage/dfs/dfs_connector.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef DFS_CONNECTOR_H
#define DFS_CONNECTOR_H

#include <memory>
#include "dfs_config.h"
#include "postgres.h"
#include "knl/knl_variable.h"
#include "foreign/foreign.h"
#include "miscadmin.h"
#include "nodes/pg_list.h"

#define DEFAULT_PERM_MOD (384)

/*
 * DfsSrvOptions holds the option values to be used
 * when connecting external server.
 */
typedef struct DfsSrvOptions {
    char* filesystem;
    char* address;
    char* cfgPath;
    char* storePath;
} DfsSrvOptions;

/*
 *  conn
 */
enum ConnectorType { HDFS_CONNECTOR = 0, OBS_CONNECTOR = 1, UNKNOWN_CONNECTOR };

namespace dfs {
/* Store the block information of the file of dfs system. */
class DFSBlockInfo : public BaseObject {
public:
    virtual ~DFSBlockInfo()
    {}

    /* Get the number of replications of the current file. */
    virtual int getNumOfReplica() const = 0;
    /*
     * Get the string including IP:xferPort for accessing the block in the node.
     * @_in_param blockIdx: The index of the block to search of the file.
     * @_in_param nodeIdx: The index of the node on which to seatch the block.
     * @return the string including IP:xferPort.
     */
    virtual const char* getNames(int blockIdx, int nodeIdx) const = 0;

    /*
     * get whether the location is cached
     * @_in_param blockIdx: The index of the block to search of the file.
     * @_in_param nodeIdx: The index of the node on which to search the block.
     * @return cached-true  uncached - false.
     */
    virtual bool isCached(int blockIdx, int nodeIdx) const = 0;
};

class DFSConnector : public BaseObject {
public:
    virtual ~DFSConnector()
    {}

    /*
     * Check if the path in hdfs is a file not directory, log error if the path does not
     * exist.
     * @_in param filePath: the path of the hdfs file/directory.
     * @return Return true: the path is a file; false: the path is not a file but a directory.
     */
    virtual bool isDfsFile(const char* filePath) = 0;
    virtual bool isDfsFile(const char* filePath, bool throw_error) = 0;

    /* Check if the path is a empty file, log error if the path does not exist. */
    virtual bool isDfsEmptyFile(const char* filePath) = 0;

    /*
     * Get the file size of the path. Return -1 if the path does not exist.
     * @_in_param filePath: the path of the hdfs file/directory
     * @return Return the size.
     */
    virtual int64_t getFileSize(const char* filePath) = 0;

    /* Get the handler to connect the DFS system. */
    virtual void* getHandler() const = 0;

    /*
     * Get list of files/directories for a given directory-path.
     * hdfsFreeFileInfo is called internally to deallocate memory.
     * Log error if the path does not exist.
     * @_in_param folderPath: The path of the directory.
     * @return Return a list of filepath. Return NULL on error.
     */
    virtual List* listDirectory(char* folderPath) = 0;
    virtual List* listDirectory(char* folderPath, bool throw_error) = 0;

    virtual List* listObjectsStat(char* searchPath, const char* prefix = NULL) = 0;

    /*
     * Get the block information of the file path.
     * Log error if the path does not exist.
     * @_in_param filePath: The path of the file.
     * @return Return a pointer to DFSBlockInfo.
     */
    virtual DFSBlockInfo* getBlockLocations(char* filePath) = 0;

    /*
     * Drop directory. Return 0 if the path does not exist.
     * @_in_param path The path of the directory.
     * @_in_param recursive if path is a directory and set to
     *      non-zero, the directory is deleted else throws an exception. In
     *      case of a file the recursive argument is irrelevant.
     * @return Returns 0 on success, -1 on error.
     */
    virtual int dropDirectory(const char* path, int recursive) = 0;

    /*
     * Make a directory using the given path.
     * @_in_param path The path of the directory.
     * @return Returns 0 on success, -1 on error.
     */
    virtual int createDirectory(const char* path) = 0;

    /*
     * Make a file using the given path.
     * @_in_param path The path of the file to make.
     * @_in_param flags - an | of bits/fcntl.h file flags - supported flags
     *      are O_RDONLY, O_WRONLY (meaning create or overwrite i.e., implies O_TRUNCAT),
     *      O_WRONLY|O_APPEND and O_SYNC. Other flags are generally ignored other than
     *      (O_RDWR || (O_EXCL & O_CREAT)) which return NULL and set errno equal ENOTSUP.
     * return Returns 0 on success, -1 on error.
     */
    virtual int openFile(const char* path, int flag) = 0;

    /* Delete the file using the given file path. Before calling this
     * function, pathExists must be checked.
     * @_in_param path The path of the file.
     * @_in_param recursive if path is a directory and set to
     *      non-zero, the directory is deleted else throws an exception. In
     *      case of a file the recursive argument is irrelevant.
     * @return Returns 0 on success, -1 on error.
     */
    virtual int deleteFile(const char* path, int recursive) = 0;

    /*
     * pathExists - Checks if a given path exsits on the filesystem
     * @param path The path to look for
     * @return Returns 0 on success, -1 on error.
     */
    virtual bool pathExists(const char* filePath) = 0;

    virtual bool existsFile(const char* path) = 0;

    /*
     * check if the current connector has a valid hdfs file handler.
     * @return true if the file handler is valid, false on invalid.
     */
    virtual bool hasValidFile() const = 0;

    /*
     * Write the buffer into the current file according to the length.
     * @_in_param buffer: The content to be write to file.
     * @_in_param length: The length of the byte to write.
     * @return Return the bytes actually write, -1 on error.
     */
    virtual int writeCurrentFile(const char* buffer, int length) = 0;

    /*
     * Read fixed size from the offset of the file into the buffer.
     * @_out_param buffer: The buffer to be filled.
     * @_in_param length: The size of bytes expected.
     * @_in_param offset: The offset at which the reading starts.
     * @return the bytes actually read, -1 on error.
     */
    virtual int readCurrentFileFully(char* buffer, int length, int64 offset) = 0;

    /*
     * Flush out the data in client's user buffer. After the return of this call,
     * new readers will see the data.
     * @return 0 on success, -1 on error and sets errno
     */
    virtual int flushCurrentFile() = 0;

    /*
     * Close the current file.
     */
    virtual void closeCurrentFile() = 0;

    /*
     * change the file's authority.
     * @_in_param filePath: The absolute path of the file to be set.
     * @_in_param mode: The mode of the authority like 600 or 755.
     * Return 0 if succeed. Return 1 if fail.
     */
    virtual int chmod(const char* filePath, short mode) = 0;

    /*
     * Set the label expression on dfs file.
     * @_in_param filePath: The absolute path of the file to be set.
     * @_in_param expression: The label string like "labelA,labelB".
     * Return 0 if succeed. Return 1 if fail.
     */
    virtual int setLabelExpression(const char* filePath, const char* expression) = 0;

    /*
     * Get the timestamp of the last modification.
     */
    virtual int64 getLastModifyTime(const char* filePath) = 0;

    /*
     * Fetch the configure value from the config file.
     */
    virtual const char* getValue(const char* key, const char* defValue) const = 0;

    /*
     * Get connection type
     */
    virtual int getType() = 0;
};

/*
 * Construct a connector of DFS which wrappers all the approaches to the DFS.
 * @_in_param ctx: The memory context on which to create the connector(not used for now).
 * @_in_param foreignTableId: The oid of the relation for which we create the connector.
 * @return the constructed connector.
 */
DFSConnector* createConnector(MemoryContext ctx, Oid foreignTableId);
DFSConnector* createTempConnector(MemoryContext ctx, Oid foreignTableId);
DFSConnector* createConnector(MemoryContext ctx, ServerTypeOption srvType, void* options);

/*
 * Construct a connector of DFS which wrappers all the approaches to the DFS.
 * @_in_param ctx: The memory context on which to create the connector(not used for now).
 * @_in_param srvOptions: information of the server option.
 * @_in_param tablespaceOid: tablespace oid.
 * @return the constructed connector.
 */
DFSConnector* createConnector(MemoryContext ctx, DfsSrvOptions* srvOptions, Oid tablespaceOid);
DFSConnector* createTempConnector(MemoryContext ctx, DfsSrvOptions* srvOptions, Oid tablespaceOid);

/* Initialize the global hdfs connector cache hash table. */
void InitHDFSConnectorCacheLock();

/* Remove the connector entry of the hdfs cache according to the server oid. */
void InvalidHDFSConnectorCache(Oid serverOid);

/* Initialize the global obs connector cache hash table. */
void InitOBSConnectorCacheLock();

/*
 * Remove the connector entry of the obs cache according to the server oid.
 * return true if clean it successfully, otherwise return false.
 */
bool InvalidOBSConnectorCache(Oid serverOid);

/* Clean the thread local variables in kerberos. */
void clearKerberosObjs();

}  // namespace dfs

/* check file path should skip */
bool checkFileShouldSkip(char* fileName);

/* check file path should skip */
bool checkPathShouldSkip(char* pathName);

#endif
