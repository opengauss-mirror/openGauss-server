/*
 * Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 2021, openGauss Contributors
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
 * -------------------------------------------------------------------------
 *
 * rowstore.cpp
 *
 * IDENTIFICATION
 *    src/common/backend/utils/sort/rowstore.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "postgres.h"
#include "knl/knl_variable.h"

#include "pgxc/execRemote.h"
#include "catalog/pgxc_node.h"
#include "storage/buf/buffile.h"
#include "lz4.h"
#include "utils/memutils.h"
#include "utils/rowstore.h"

typedef enum RESIDENCE { RES_UNKNOWN = 0, RES_MEM, RES_DISK } RESIDENCE;

/* initialize a new bank */
static Bank* bank_open(size_t max_size);

/* write a data row to bank */
static RESIDENCE bank_write(RowStoreManager rs, Bank* bank, RowStoreCell cell);

/* read a data row from bank */
static bool bank_read(Bank* bank, RowStoreCell data);

/* reset resources in bank*/
static void bank_reset(Bank* bank);

/* release resources and clost a bank */
static void bank_close(Bank* bank);

/* get the data's bank */
static Bank* get_bank_for_data(RowStoreManager rs, RemoteDataRow data);

/* get the msgnode's bank */
static Bank* get_bank_for_oid(RowStoreManager rs, Oid oid);

/* various sanity check */
static void validate_data(RemoteDataRow data, bool is_insert);

/* convert datarow to cell */
static RowStoreCell make_one_cell(RemoteDataRow data);

/* free the cell's memory */
static void free_one_cell(RowStoreCell cell);

/* convert cell to datarow */
static inline void convert_cell_to_datarow(RowStoreCell cell, RemoteDataRow data);

/* compress cell using LZ4 */
static inline int compress_cell(RowStoreCell cell);

/* decompress cell using LZ4 */
static inline void decompress_cell(RowStoreCell cell);

/* convert cell to datarow format */
static inline void convert_cell_to_datarow(RowStoreCell cell, RemoteDataRow data);

/* pack the compressed msg to save meory */
static inline char* pack_msg(char* msg, uint32 len);

/* size of the cell */
static inline size_t get_cell_size(RowStoreCell cell);

/* write to memory or disk? */
static bool should_write_to_memory(Bank* bank, RowStoreCell cell);

/* helpful macro for write an int to file */
#define WRITE_INT(field)                                                                                       \
    do {                                                                                                       \
        if (sizeof(int) != BufFileWrite(bank->file, (void*)&(field), sizeof(int))) {                           \
            ereport(ERROR, (errcode_for_file_access(), errmsg("could not write to row store temp file: %m"))); \
        }                                                                                                      \
    } while (0)

/* helpful macro for write a string to file, the string mem is freed after write */
#define WRITE_STR(field, len)                                                                                  \
    do {                                                                                                       \
        if ((len) != (int)BufFileWrite(bank->file, (void*)(field), (len))) {                                   \
            ereport(ERROR, (errcode_for_file_access(), errmsg("could not write to row store temp file: %m"))); \
        }                                                                                                      \
    } while (0)

/* helpful macro for read an int from file */
#define READ_INT(field)                                                                                         \
    do {                                                                                                        \
        if (sizeof(int) != BufFileRead(bank->file, &(field), sizeof(int))) {                                    \
            ereport(ERROR, (errcode_for_file_access(), errmsg("could not read from row store temp file: %m"))); \
        }                                                                                                       \
    } while (0)

/* helpful macro for read a string from file */
#define READ_STR(field, len)                                                                                    \
    do {                                                                                                        \
        (field) = (char*)palloc(len);                                                                           \
        if ((len) != (int)BufFileRead(bank->file, (void*)(field), (len))) {                                     \
            ereport(ERROR, (errcode_for_file_access(), errmsg("could not read from row store temp file: %m"))); \
        }                                                                                                       \
    } while (0)

/*
 * @Description: initialize a row store
 *
 *     RowStoreAlloc allocate a row store with its memory max size
 *     and a memory context which the row store will allocate memory in.
 *     rely on global variable u_sess->pgxc_cxt.NumDataNodes for the bank_num, does it
 *     change within a query?
 *
 * @param[IN] context:  memory context the row store will be working on
 * @param[IN] resowner: resource owner the row store will attach with
 * @param[IN] max_size: the max size of data can hold in memory
 *                      if more data is coming, they will be written to disk
 * @return RowStore: which hold all the status of row store
 */
RowStoreManager RowStoreAlloc(MemoryContext context, size_t max_size, ResourceOwner resowner)
{
    AutoContextSwitch acs(context);

    /* the rowstore descriptor is allocted under rowstore's memory context */
    RowStoreManager rs = (RowStoreManager)palloc0(sizeof(RowStoreManagerData));

    /* the rs is palloc0, so only need to fill the needed value */
    rs->context = context;
    rs->resowner = (resowner != NULL) ? resowner : t_thrd.utils_cxt.TopTransactionResourceOwner;
    rs->cn_bank_num = u_sess->pgxc_cxt.NumCoords;
    rs->dn_bank_num = u_sess->pgxc_cxt.NumDataNodes;

    /* alloc mem for bank array */
    rs->cn_banks = (Bank**)palloc(sizeof(Bank*) * rs->cn_bank_num);
    rs->dn_banks = (Bank**)palloc(sizeof(Bank*) * rs->dn_bank_num);

    /* initialize the banks */
    for (int i = 0; i < rs->cn_bank_num; i++) {
        rs->cn_banks[i] = bank_open(max_size / rs->cn_bank_num);
    }

    /* initialize the banks */
    for (int i = 0; i < rs->dn_bank_num; i++) {
        rs->dn_banks[i] = bank_open(max_size / rs->dn_bank_num);
    }

    return rs;
}

/* reset mem/file inside rowstore */
void RowStoreReset(RowStoreManager rs)
{
    /* no work to do */
    if (rs == NULL)
        return;

    /* are we missed up the memory? */
    CHECK_MEM(rs);

    if (rs->cn_banks) {
        /* free each bank */
        for (int i = 0; i < rs->cn_bank_num; i++) {
            bank_reset(rs->cn_banks[i]);
        }
    }

    if (rs->dn_banks) {
        /* free each bank */
        for (int i = 0; i < rs->dn_bank_num; i++) {
            bank_reset(rs->dn_banks[i]);
        }
    }

    /* the caller should set rs to NULL */
    return;
}

/*
 * @Description: destory a row store
 *
 *      RowStoreDestory do the oppsite of RowStoreAlloc, it do the cleaning work
 *
 * @param[IN] rs: the row store handle
 * @return void
 */
void RowStoreDestory(RowStoreManager rs)
{
    /* no work to do */
    if (rs == NULL)
        return;

    /* are we missed up the memory? */
    CHECK_MEM(rs);

    if (rs->cn_banks) {
        /* close each bank */
        for (int i = 0; i < rs->cn_bank_num; i++) {
            bank_close(rs->cn_banks[i]);
        }

        /* free the banks pointer array */
        pfree(rs->cn_banks);

        /* be tidy */
        rs->cn_banks = NULL;
        rs->cn_bank_num = 0;
    }

    if (rs->dn_banks) {
        /* close each bank */
        for (int i = 0; i < rs->dn_bank_num; i++) {
            bank_close(rs->dn_banks[i]);
        }

        /* free the banks pointer array */
        pfree(rs->dn_banks);

        /* be tidy */
        rs->dn_banks = NULL;
        rs->dn_bank_num = 0;
    }

    /* including the RowStoreManager itself */
    pfree(rs);

    /* the caller should set rs to NULL */
    return;
}

/*
 * @Description: get the target bank for a remote data row
 *
 *      the data should be put to its own bank based
 *      on the data->msgnode.
 *
 * @param[IN] rs: the row store handle
 * @param[IN] data: the data row to be stored
 * @return the Bank pointer for write
 */
static Bank* get_bank_for_data(RowStoreManager rs, RemoteDataRow data)
{
    return get_bank_for_oid(rs, data->msgnode);
}

/*
 * @Description: get the target bank for a remote data row
 *
 *      the data should be put to its own bank based
 *      on the msgnode's oid
 *
 * @param[IN] rs: the row store handle
 * @param[IN] oid: data rows' msgnode oid
 * @return the Bank pointer for write
 */
static Bank* get_bank_for_oid(RowStoreManager rs, Oid oid)
{
    /* should get one of cn_node_id or dn_node_id */
    int dn_node_id = PGXCNodeGetNodeId(oid, PGXC_NODE_DATANODE);
    int cn_node_id = PGXCNodeGetNodeId(oid, PGXC_NODE_COORDINATOR);

    /* guard against array overflow */
    if (dn_node_id >= rs->dn_bank_num) {
        ereport(ERROR, (errmsg("row store : dn node id(%d) exceeds max bank num(%d)", dn_node_id, rs->dn_bank_num)));
    }

    if (cn_node_id >= rs->cn_bank_num) {
        ereport(ERROR, (errmsg("row store : cn node id(%d) exceeds max bank num(%d)", cn_node_id, rs->cn_bank_num)));
    }

    /* guard against array overflow */
    if (cn_node_id < 0 && dn_node_id < 0) {
        ereport(ERROR, (errmsg("failed to get cn/dn node id for OID %u", oid)));
    }

    /* one of cn_node_id or dn_node_id must be valid */
    if (cn_node_id >= 0) {
        return rs->cn_banks[cn_node_id];
    } else {
        return rs->dn_banks[dn_node_id];
    }
}

/*
 * @Description: insert a value into row store
 *
 *      (1) convert data to cell format(compressed)
 *      (2) insert the cell to bank, if memory is full
 *          the cell will be written to disk and the cell
 *          memory is freed
 *
 * @param[IN] rs: the row store handle
 * @param[IN] data: the data row to be stored
 * @return void
 */
void RowStoreInsert(RowStoreManager rs, RemoteDataRow data)
{
    AutoContextSwitch acs(rs->context);

    validate_data(data, true);

    Bank* bank = get_bank_for_data(rs, data);

    RowStoreCell cell = make_one_cell(data);

    RESIDENCE res = bank_write(rs, bank, cell);
    if (res == RES_DISK) {
        /* the cell will be freed if write to disk */
        free_one_cell(cell);
    } else if (res == RES_MEM) {
        /* no-op, the cell will be kept in mem */
    } else {
        /* oops, someting bad happened */
        ereport(ERROR, (errmsg("row store : cannot write cell into either memory or disk")));
    }
}

/*
 * @Description: fetch data from row store sequentially
 *
 *      fetch a cell from bank, convert it to datarow format
 *      (de-compression is done), and the cell memory is freed.
 *
 *      the caller should always check for the return value
 *      to make sure a valid datarow is fetched from rowstore.
 *
 * @param[IN] rs: the row store handle
 * @param[OUT] datarow: the data is filled to datarow
 * @return true on successful, false on failure.
 */
bool RowStoreFetch(RowStoreManager rs, RemoteDataRow datarow)
{
    AutoContextSwitch acs(rs->context);

    if (RowStoreLen(rs) > 0) {
        RowStoreCellData cell;

        /* fetch from dn bank */
        for (int i = 0; i < rs->dn_bank_num; i++) {
            if (bank_read(rs->dn_banks[i], &cell)) {
                convert_cell_to_datarow(&cell, datarow);

                validate_data(datarow, false);

                return true;
            }
        }

        /* fetch from cn bank */
        for (int i = 0; i < rs->cn_bank_num; i++) {
            if (bank_read(rs->cn_banks[i], &cell)) {
                convert_cell_to_datarow(&cell, datarow);

                validate_data(datarow, false);

                return true;
            }
        }

        /* oops, someting bad happened */
        ereport(ERROR, (errmsg("row store : expect data from row store, but no data read")));
    } else {
        /* oops, the data row is  fill empty data */
        datarow->msg = NULL;
        datarow->msglen = 0;
        datarow->msgnode = 0;

        return false;
    }

    /* should not reach here */
    return false;
}

/*
 * @Description:  fetch data row based on node id
 *
 *      fetch a cell from the node's bank. and convert cell
 *      into datarow format(de-compress is done).
 *
 *      the caller should always check for the return value
 *      to make sure a valid datarow is fetched from rowstore.
 *
 * @param[IN] rs: the row store handle
 * @param[IN] node: the datanode id
 * @param[OUT] data: the data is filled
 * @return true on successful, false on failure.
 */
bool RowStoreFetch(RowStoreManager rs, Oid nodeid, RemoteDataRow data)
{
    AutoContextSwitch acs(rs->context);

    RowStoreCellData cell;

    Bank* bank = get_bank_for_oid(rs, nodeid);

    if (bank_read(bank, &cell)) {
        convert_cell_to_datarow(&cell, data);

        validate_data(data, false);

        /* XXX convert from Oid to int oops */
        if ((int)nodeid != data->msgnode)
            ereport(ERROR, (errmsg("row store : expect to read node_id = %u but got %d", nodeid, data->msgnode)));

        return true;
    }

    return false;
}

/*
 * @Description:  get the length of row store
 *
 *      this function returns how many tuples inside
 *      the banks.
 *
 *      XXX: shall the for loop be optimized
 *      if the number of bank_num is huge
 *
 * @param[IN] rs: the row store handle
 * @return the length of row store(how many tuples inside)
 */
int RowStoreLen(RowStoreManager rs)
{
    int size = 0;

    for (int i = 0; i < rs->dn_bank_num; i++) {
        size += rs->dn_banks[i]->length;
    }

    for (int i = 0; i < rs->cn_bank_num; i++) {
        size += rs->cn_banks[i]->length;
    }

    return size;
}

/*
 * @Description: open a bank for storing cell
 *
 *      this function initalize a new bank with
 *      the max size of memory to store cells.
 *      if the cell size that holds in the bank
 *      exceeds the max_size, the cell will be
 *      written to disk.
 *
 * @param[IN] max_size: the max size of memory for holding
 *                      cells before written to disk.
 * @return the Bank pointer
 */
static Bank* bank_open(size_t max_size)
{
    Bank* bank = (Bank*)palloc0(sizeof(Bank));

    bank->max_size = max_size;

    return bank;
}

/* reset mem/disk inside bank, left bank in open state */
static void bank_reset(Bank* bank)
{
    if (bank->mem) {
        list_free_deep(bank->mem);
        bank->mem = NIL;
    }

    if (bank->file) {
        BufFileClose(bank->file);
        bank->file = NULL;
    }

    bank->current_size = 0;
    bank->write_fileno = 0;
    bank->write_offset = 0;
    bank->read_fileno = 0;
    bank->read_offset = 0;
    bank->length = 0;
}

/*
 * @Description: close a bank
 *
 *      do the cleaning work:
 *         (1) free the memory list
 *         (2) close all the temp files
 *
 * @param[IN] bank: which bank to close?
 * @return void
 */
static void bank_close(Bank* bank)
{
    if (bank->mem) {
        list_free_deep(bank->mem);
        bank->mem = NIL;
    }

    if (bank->file) {
        /* close file, better play safe. */
        BufFileClose(bank->file);
        bank->file = NULL;
    }

    pfree(bank);
}

/*
 * @Description: to see if we should write to memory
 *
 *       we write to memory only if:
 *
 *		 (1) no previous cell has written to disk.
 *       (2) the bank's memory can hold the cell.
 *
 *       why the 1nd condition?
 *           the cell might have abitrary size once
 *           is compressed. so, in order to keep the
 *           FIFO we have to write the cell into disk
 *           once a previous cell has written to disk
 *
 * @param[IN] bank: which bank to write a cell
 * @param[IN] cell: the cell to write to the bank
 * @return bool: true to write to memory false to write to disk
 */
static bool should_write_to_memory(Bank* bank, RowStoreCell cell)
{
    return (bank->file == NULL) && (bank->current_size + get_cell_size(cell) <= bank->max_size);
}

/*
 * @Description: write a cell into the bank
 *
 *      if the bank still have more memory to hold a cell
 *      the cell will be append to bank's mem list.
 *      otherwize of the bank is full, the cell will be
 *      written to disk and the cell memory is freed.
 *
 *      the file will only be created when the memory is full
 *      the file format layout is:
 *
 *            [msgnode][msglen][olen][clen][msg]
 *
 *      then msg string is compressed, therefore the length
 *      of msg is 'clen'. make sure you follow the sequence
 *      when reading and use clen to read the msg.
 *
 * @param[IN] bank: which bank to write a cell
 * @param[IN] cell: the cell to write to the bank
 * @return void
 */
static RESIDENCE bank_write(RowStoreManager rs, Bank* bank, RowStoreCell cell)
{
    if (should_write_to_memory(bank, cell)) {
        /* can hold in memory */
        bank->mem = lappend(bank->mem, cell);

        /* increase bank size */
        bank->current_size += get_cell_size(cell);

        /* increase bank length */
        bank->length++;

        return RES_MEM;
    } else {
        /* alloc file if haven't */
        if (bank->file == NULL) {
            ResourceOwner save;

            save = t_thrd.utils_cxt.CurrentResourceOwner;

            PG_TRY();
            {
                /*
                 * important: wrap the resource owner change in PG_TRY to
                 * restore to CurrentResourceOwner upon error
                 */
                t_thrd.utils_cxt.CurrentResourceOwner = rs->resowner;

                /*
                 * register temp file into RowStore itself's ResourceOwner so that it will
                 * be unregisted at right place when its transaction finishs.
                 */
                bank->file = BufFileCreateTemp(false);

                t_thrd.utils_cxt.CurrentResourceOwner = save;
            }
            PG_CATCH();
            {
                /* oops, error happened, restore it */
                t_thrd.utils_cxt.CurrentResourceOwner = save;
                PG_RE_THROW();
            }
            PG_END_TRY();

            /* record the end of file */
            BufFileTell(bank->file, &(bank->write_fileno), &(bank->write_offset));
        }

        /* seek to the end of file */
        if (0 != BufFileSeek(bank->file, bank->write_fileno, bank->write_offset, SEEK_SET)) {
            ereport(ERROR, (errcode_for_file_access(), errmsg("could not seek store temp file: %m")));
        }

        /* do the writing */
        WRITE_INT(cell->msgnode);
        WRITE_INT(cell->msglen);
        WRITE_INT(cell->clen);
        WRITE_STR(cell->msg, cell->clen);

        /* record the end of file */
        BufFileTell(bank->file, &(bank->write_fileno), &(bank->write_offset));

        /* increase length */
        bank->length++;

        return RES_DISK;
    }

    return RES_UNKNOWN;
}

/*
 * @Description: read a cell from a bank
 *
 *      first try to read from the bank's memory
 *      once the data was read and filled to the
 *      cell, the storecell's memory is freed and
 *      removed from the list.
 *
 *      the data is read following the sequence of
 *      how data is write, refer to bank_write
 *
 * @param[IN] bank: which bank to write a cell
 * @param[OUT] cell: the cell to write to the bank
 * @return true on successful, false on failure
 */
static bool bank_read(Bank* bank, RowStoreCell cell)
{
    /* oops, no more data */
    if (bank->length <= 0)
        return false;

    if (list_length(bank->mem) > 0) {
        /* read from mem */
        RowStoreCell data_read = (RowStoreCell)linitial(bank->mem);

        /* fill the return value */
        *cell = *data_read;

        /* free the cell memory in the list */
        pfree(data_read);

        /* and remove cell from the list */
        bank->mem = list_delete_first(bank->mem);

        /* decrease length */
        bank->length--;

        return true;
    } else {
        if (bank->file == NULL) {
            ereport(ERROR, (errmsg("row store : expect to read from file but no file opened")));
        }

        /* read from disk */
        if (0 != BufFileSeek(bank->file, bank->read_fileno, bank->read_offset, SEEK_SET)) {
            ereport(ERROR, (errcode_for_file_access(), errmsg("could not seek store temp file: %m")));
        }

        /* read data */
        READ_INT(cell->msgnode);
        READ_INT(cell->msglen);
        READ_INT(cell->clen);
        READ_STR(cell->msg, cell->clen);

        /* record the end of file */
        BufFileTell(bank->file, &(bank->read_fileno), &(bank->read_offset));

        /* decrease length */
        bank->length--;

        return true;
    }

    /* should not reach here */
    Assert(0);

    return false;
}

/*
 * @Description: check if the data makes sense
 *
 *      check sanity of data and report an error as early as possible.
 *
 * @param[IN] data: the source data to be checked
 * @return void
 */
static void validate_data(RemoteDataRow data, bool is_insert)
{
    const char* op = is_insert ? "inserting" : "fetching";

    if (data->msglen < 0 && data->msg == NULL) {
        /* emm, seems we can still play ... */
        ereport(LOG, (errmsg("row store : msglen less than zero when %s", op)));

        data->msglen = 0;
    }

    if (data->msglen <= 0 && data->msg != NULL) {
        /* we have msg but the msglen is not right */
        ereport(ERROR, (errmsg("row store : cannot determine msg len when %s", op)));
    }

    if (data->msglen > 0 && data->msg == NULL) {
        /* claims to have msglen but no actual msg provided */
        ereport(ERROR, (errmsg("row store : msg and msglen mismatch when %s", op)));
    }
}

/*
 * @Description: convert RemoteDataRow to RowStoreCell
 *
 *      copy each field and compress the cell
 *      we also do sanity check to the input data
 *
 * @param[IN] data: the source
 * @return cell
 */
static RowStoreCell make_one_cell(RemoteDataRow data)
{
    RowStoreCell cell = (RowStoreCell)palloc(sizeof(RowStoreCellData));

    /* init each field */
    cell->msg = data->msg; /* the data->msg lives in RemoteQueryc context, no need to memcpy */
    cell->msglen = data->msglen;
    cell->msgnode = data->msgnode;
    cell->clen = compress_cell(cell);

    /* data now stored in cell, no longer needed, so free it */
    pfree(data->msg);
    data->msg = NULL;
    data->msglen = 0;
    data->msgnode = 0;

    return cell;
}

/*
 * @Description: free the cell's memory
 *
 *      free the msg and cell
 *
 * @param[IN] data: the cell to be freed
 * @return void
 */
static void free_one_cell(RowStoreCell cell)
{
    if (cell == NULL)
        return;

    if (cell->msg)
        pfree(cell->msg);

    pfree(cell);
}

/*
 * @Description: convert RowStoreCell to RemoteDataRow
 *
 *      decompress the cell and convert to RemoteDataRow
 *
 * @param[IN] cell: the source
 * @param[OUT] data: the destination
 * @return void
 */
static inline void convert_cell_to_datarow(RowStoreCell cell, RemoteDataRow data)
{
    if (cell->clen > 0) {
        decompress_cell(cell);
    }

    data->msg = cell->msg;
    data->msglen = cell->msglen;
    data->msgnode = cell->msgnode;
}

/*
 * @Description: pack the msg tider
 *
 *        the compressed msg len will be shorter than
 *        the palloced size, so we have to pack it
 *        to save more memory. we might use repalloc
 *        but it seems not work(no memory is reduced).
 *        so we have to do it manually, it saves 99%
 *        of memory.
 *
 * @param[IN] msg: the msg to be packed
 * @param[IN] len: the expected packed len
 * @return char * the packed msg
 */
static inline char* pack_msg(char* msg, uint32 len)
{
    char* new_msg = (char*)palloc(len);

    errno_t rc;
    rc = memcpy_s(new_msg, len, msg, len);
    securec_check(rc, "\0", "\0");

    pfree(msg);

    return new_msg;
}

/*
 * @Description: compress the cell using LZ4
 *
 * @param[IN/OUT] cell: the cell to be compressed
 * @return int: the compressed size
 */
static inline int compress_cell(RowStoreCell cell)
{
    /* nothing to do */
    if (cell->msglen <= 0)
        return 0;

    /* ready to compress */
    char* compressed_msg = (char*)palloc(LZ4_COMPRESSBOUND(cell->msglen));

    /* the compression is granted to be successful */
    int clen = LZ4_compress_default(cell->msg, compressed_msg, cell->msglen, LZ4_compressBound(cell->msglen));
    if (clen < 0) {
        /* should never happen, otherwise bugs inside LZ4 */
        ereport(ERROR, (errmsg("row store : LZ4_compress_default failed trying to compress the data")));
    } else if (clen == 0) {
        /* should never happen, otherwise bugs inside LZ4_COMPRESSBOUND */
        ereport(
            ERROR, (errmsg("row store : LZ4_compress_default destination buffer couldn't hold all the information")));
    }

    /* assgin the compressed msg to data */
    cell->msg = pack_msg(compressed_msg, clen);

    return clen;
}

/*
 * @Description: de-compress the cell using LZ4
 *
 * @param[IN/OUT] cell: the cell to be de-compressed
 * @return void
 */
static inline void decompress_cell(RowStoreCell cell)
{
    if (cell->msglen < 0)
        ereport(ERROR, (errmsg("row store : unexpected original len of msg when decompressing data")));

    /* allocate memory to store the de-compressed data */
    char* msg = (char*)palloc0(cell->msglen);

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
    if (cell->msglen != LZ4_decompress_safe(cell->msg, msg, cell->clen, cell->msglen))
#pragma GCC diagnostic pop 
	{
        ereport(ERROR, (errmsg("row store : unexpected compressed len when decompressing data")));
    }

    /* free the compressed msg */
    pfree(cell->msg);

    /* assgin the uncompressed msg to cell */
    cell->msg = msg;
}

/*
 * @Description: get the size of the cell
 *
 *       the cell holds a pointer of size clen(compressed)
 *       and some int/size_t members.
 *
 * @param[IN] cell: the cell which size is expected
 * @return void
 */
static inline size_t get_cell_size(RowStoreCell cell)
{
    size_t total_size = 0;

    total_size += alloc_trunk_size(sizeof(RowStoreCellData));
    total_size += alloc_trunk_size(sizeof(ListCell));
    total_size += alloc_trunk_size((cell->clen > 0) ? cell->clen : cell->msglen);

    return total_size;
}
