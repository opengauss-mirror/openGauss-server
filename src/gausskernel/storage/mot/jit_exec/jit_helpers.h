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
 * -------------------------------------------------------------------------
 *
 * jit_helpers.h
 *    Helper functions for JIT execution.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/jit_exec/jit_helpers.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef JIT_HELPERS_H
#define JIT_HELPERS_H

#include "postgres.h"
#include "nodes/params.h"
#include "mot_engine.h"

/*---------------------------  LLVM Access Helpers ---------------------------*/
extern "C" {
/**
 * @brief Issue debug log message.
 * @param function The function name.
 * @param msg The log message.
 */
void debugLog(const char* function, const char* msg);
void debugLogInt(const char* msg, int arg);
void debugLogString(const char* msg, const char* arg);
void debugLogStringDatum(const char* msg, int64_t arg);
void debugLogDatum(const char* msg, Datum value, int isNull, int type);

/*---------------------------  Engine Access Helpers ---------------------------*/
/**
 * @brief Query whether soft memory limit has been reached.
 * @return Non-zero value whether soft memory limit has been reached, otherwise zero.
 */
int isSoftMemoryLimitReached();

/**
 * @brief Retrieves a reference to the @ref MOTEngine single instance.
 * @return The single @ref MOTEngine instance, or NULL if failed.
 */
MOT::MOTEngine* getMOTEngine();

/**
 * @brief Retrieves The primary index of a table.
 * @param table The table from which the primary index is to be retrieved.
 * @return The primary index of the table.
 */
MOT::Index* getPrimaryIndex(MOT::Table* table);

/**
 * @brief Retrieves a table index by index identifier.
 * @param table The table from which the index is to be retrieved.
 * @param index_id The identifier of the index to retrieve.
 * @return The requested index of the table, or NULL if none such exists.
 */
MOT::Index* getTableIndex(MOT::Table* table, int index_id);

/**
 * @brief Initializes an index search key.
 * @param key The key to initialize.
 * @param index The index from which the key originated.
 */
void InitKey(MOT::Key* key, MOT::Index* index);

/**
 * @brief Retrieves a table column by its ordinal position.
 * @param table The table from which the column is to be retrieved.
 * @param colid The zero-based index of the column to retrieve.
 * @return The requested column, or NULL if non such exists.
 */
MOT::Column* getColumnAt(MOT::Table* table, int table_colid);

/**
 * @brief Marks expression evaluated to null or not.
 * @param isnull Specifies whether the expression evaluated to null.
 */
void SetExprIsNull(int isnull);

/**
 * @brief Queries whether an expression evaluated to null.
 * @return Non-zero value if the expression evaluated to null, otherwise zero.
 */
int GetExprIsNull();

/** @brief Sets last evaluated expression collation. */
void SetExprCollation(int collationId);

/** @brief Retrieves last evaluated expression collation. */
int GetExprCollation();

/**
 * @brief Retrieves a pooled constant by its identifier.
 * @param constId The identifier of the constant value.
 * @return The constant value.
 */
Datum GetConstAt(int constId);

/**
 * @brief Retrieves a datum parameter from parameters array.
 * @param params The parameter array.
 * @param paramid The zero-based index of the parameter to retrieve.
 * @return The parameter value.
 */
Datum getDatumParam(ParamListInfo params, int paramid);

/**
 * @brief Reads a column value from a row.
 * @param table The originating table of the row.
 * @param row The row from which the column value is to be retrieved.
 * @param colid The zero-based index of the column to retrieve.
 * @param innerRow Specified whether datum is read from inner row or not (relevant only for direct row access).
 * @param subQueryIndex Specifies whether datum is read from sub=query result (relevant only for direct row access).
 * @return The column value.
 */
Datum readDatumColumn(MOT::Table* table, MOT::Row* row, int colid, int innerRow, int subQueryIndex);

/**
 * @brief Queries whether the directly-accessed row was concurrently deleted.
 * @return Non-zero value if the row was deleted.
 */
int IsDirectRowDeleted(int innerRow);

/**
 * @brief Writes a column value into a row.
 * @param row The row into which the column value is to be written.
 * @param column The column.
 * @param value The value to write.
 */
void writeDatumColumn(MOT::Row* row, MOT::Column* column, Datum value);

/**
 * @brief Writes a column value into a search key.
 * @param column The column (used for data type information).
 * @param key The key to modify.
 * @param value The value to write.
 * @param index_colid The zero-based index of the column according to the originating index of the key.
 * @param offset The offset in the key from value writing should begin.
 * @param size The number of bytes to write.
 * @param value_type The type of the value to write.
 */
void buildDatumKey(
    MOT::Column* column, MOT::Key* key, Datum value, int index_colid, int offset, int size, int value_type);

/*---------------------------  Invoke PG Operators ---------------------------*/
Datum JitInvokePGFunction0(PGFunction fptr, int collationId);
Datum JitInvokePGFunction1(PGFunction fptr, int collationId, int isStrict, Datum arg, int isnull, Oid argType);
Datum JitInvokePGFunction2(PGFunction fptr, int collationId, int isStrict, Datum arg1, int isnull1, Oid argType1,
    Datum arg2, int isnull2, Oid argType2);
Datum JitInvokePGFunction3(PGFunction fptr, int collationId, int isStrict, Datum arg1, int isnull1, Oid argType1,
    Datum arg2, int isnull2, Oid argType2, Datum arg3, int isnull3, Oid argType3);
Datum JitInvokePGFunctionN(
    PGFunction fptr, int collationId, int isStrict, Datum* args, int* isnull, Oid* argTypes, int argCount);

/*---------------------------  Memory Allocation ---------------------------*/
uint8_t* JitMemSessionAlloc(uint32_t size);
void JitMemSessionFree(uint8_t* ptr);

/*---------------------------  BitmapSet Helpers ---------------------------*/
/**
 * @brief Raises a bit in a bitmap set.
 * @param bmp The bitmap set.
 * @param col The zero-based index of the bit to raise.
 */
void setBit(MOT::BitmapSet* bmp, int col);

/**
 * @brief Resets to zero a bit map set.
 * @param bmp The bitmap set.
 */
void resetBitmapSet(MOT::BitmapSet* bmp);

/*---------------------------  General Helpers ---------------------------*/
/**
 * @brief Retrieves the number of columns in a table.
 * @param table The table to query.
 * @return The number of columns in the table, including the null-bits column in index zero.
 */
int getTableFieldCount(MOT::Table* table);

/**
 * @brief Writes a row back to its table.
 * @param row The row to write.
 * @param bmp The bitmap set denoting the modified columns (for incremental redo).
 * @return Zero if succeeded, otherwise an @ref MOT::RC error code.
 */
int writeRow(MOT::Row* row, MOT::BitmapSet* bmp);

/**
 * @brief Searches a row in a table.
 * @param table The table to search.
 * @param key The key by which the row is to be searched.
 * @param access_mode_value The required row access mode.
 * @param innerRow Specified whether searching for an inner row (relevant only for direct row access).
 * @param subQueryIndex Specifies whether searching for a sub-query row (relevant only for direct row access).
 * @return The requested row, or NULL if the row was not found, or some other failure occurred.
 */
MOT::Row* searchRow(MOT::Table* table, MOT::Key* key, int access_mode_value, int innerRow, int subQueryIndex);

/**
 * @brief Creates a new table row.
 * @param table The table for which the row is to be created.
 * @return The new row, or NULL if failed.
 */
MOT::Row* createNewRow(MOT::Table* table);

/**
 * @brief Inserts a row into a table.
 * @param table The table into which the row is to be inserted.
 * @param row The row to insert.
 * @return Zero if succeeded, otherwise an @ref MOT::RC error code.
 */
int insertRow(MOT::Table* table, MOT::Row* row);

/**
 * @brief Deletes the last selected row.
 * @return Zero if succeeded, otherwise an @ref MOT::RC error code.
 */
int deleteRow();

/**
 * @brief Sets to zero all row null bits.
 * @param table The table to which the row belongs.
 * @param row The row.
 */
void setRowNullBits(MOT::Table* table, MOT::Row* row);

int setConstNullBit(MOT::Table* table, MOT::Row* row, int table_colid, int isnull);

/**
 * @brief Sets the null bit of a row column according to the lastly evaluated expression.
 * @param table The table to which the row belongs.
 * @param row The row.
 * @param table_colid The zero-based index of the column whose null-bit is to be set.
 * @return Zero if succeeded, otherwise an @ref MOT::RC error code (could cause null-violation).
 */
int setExprResultNullBit(MOT::Table* table, MOT::Row* row, int table_colid);

/**
 * @brief Clears a tuple.
 * @param slot The tuple to clear.
 */
void execClearTuple(TupleTableSlot* slot);

/**
 * @brief Stores a virtual tuple.
 * @param slot The tuple to store.
 */
void execStoreVirtualTuple(TupleTableSlot* slot);

/**
 * @brief Selects a row column into a tuple.
 * @param table The table to which the row belongs.
 * @param row The row to store.
 * @param slot The tuple into which data is to be store.
 * @param table_colid The zero-based index of the row column.
 * @param tuple_colid The zero-based index of the tuple column.
 */
void selectColumn(MOT::Table* table, MOT::Row* row, TupleTableSlot* slot, int table_colid, int tuple_colid);

/**
 * @brief Reports to the envelope the number of tuples processed.
 * @param[out] tp_processed The number of tuples processed.
 * @param rows The number of rows processed.
 */
void setTpProcessed(uint64_t* tp_processed, uint64_t rows);

/**
 * @brief Reports to the envelope whether a range scan ended.
 * @param[out] scan_ended The scan ended flag of the envelope.
 * @param result The value to report (non-zero means scan ended).
 */
void setScanEnded(int* scan_ended, int result);

/**
 * @brief Copies a key.
 * @param index The index from which both keys originated.
 * @param src_key The source key.
 * @param dest_key The destination key.
 */
void copyKey(MOT::Index* index, MOT::Key* src_key, MOT::Key* dest_key);

/**
 * @brief Fills a key with a bit-pattern.
 * @param key The key to modify.
 * @param pattern The bit-pattern.
 * @param offset The offset from which to begin filling the pattern.
 * @param size The number of bytes to fill the pattern.
 */
void FillKeyPattern(MOT::Key* key, unsigned char pattern, int offset, int size);

/**
 * @brief Adjusts a key for secondary index scanning.
 * @param key The key to adjust.
 * @param index The index to which the key belongs.
 * @param pattern The bit-pattern used for adjusting the key.
 */
void adjustKey(MOT::Key* key, MOT::Index* index, unsigned char pattern);

/**
 * @brief Searches an iterator for a range scan.
 * @param index The index in which to search the iterator.
 * @param key The search key.
 * @param forward_scan Specifies whether this is a forward scan. If true then a forward iterator is created (such that
 * the an iterator pointing to the value succeeding the lower bound is returned).
 * @return The resulting iterator, or NULL if failed.
 */
MOT::IndexIterator* searchIterator(MOT::Index* index, MOT::Key* key, int forward_scan, int include_bound);

/**
 * @brief Gets the begin iterator for a full-scan.
 * @param index The index in which to search the iterator.
 * @return The resulting iterator, or NULL if failed.
 */
MOT::IndexIterator* beginIterator(MOT::Index* index);

/**
 * @brief Creates an end-iterator for a range scan.
 * @param index The index in which to search the iterator.
 * @param key The search key.
 * @param forward_scan Specifies whether this is a forward scan. If true then a reverse iterator is created (such that
 * the iterator pointing to the value preceding the upper bound is returned).
 * @return The resulting iterator, or NULL if failed.
 */
MOT::IndexIterator* createEndIterator(MOT::Index* index, MOT::Key* key, int forward_scan, int include_bound);

/**
 * @brief Queries whether a range scan ended.
 * @param index The index on which the range scan is performed.
 * @param itr The begin iterator.
 * @param end_itr The end iterator.
 * @param forward_iterator Specifies whether this is a forward scan.
 * @return Non-zero value if the range scan ended, otherwise zero.
 */
int isScanEnd(MOT::Index* index, MOT::IndexIterator* itr, MOT::IndexIterator* end_itr, int forward_scan);

/**
 * @brief Checks whether there is another row during range scan. Cursor is advanced.
 * @param index The index on which the range scan is performed.
 * @param itr The begin iterator.
 * @param endItr The end iterator.
 * @param forwardScan Specifies whether this is a forward scan.
 * @return Non-zero value if a row was found, or zero if end of scan reached, or some other failure occurred.
 */
int CheckRowExistsInIterator(MOT::Index* index, MOT::IndexIterator* itr, MOT::IndexIterator* endItr, int forwardScan);

/**
 * @brief Retrieves a row from an iterator during a range scan. Cursor is advanced.
 * @param index The index on which the range scan is performed.
 * @param itr The begin iterator.
 * @param end_itr The end iterator.
 * @param access_mode The required row access mode.
 * @param forward_iterator Specifies whether this is a forward scan.
 * @param innerRow Specifies whether searching for inner row (relevant only for direct row access).
 * @param subQueryIndex Specifies whether searching for a sub-query row (relevant only for direct row access).
 * @return The resulting row, or NULL if end of scan reached, or some other failure occurred.
 */
MOT::Row* getRowFromIterator(MOT::Index* index, MOT::IndexIterator* itr, MOT::IndexIterator* end_itr, int access_mode,
    int forward_scan, int innerRow, int subQueryIndex);

/**
 * @brief Destroys an iterator.
 * @param itr The iterator to destroy.
 */
void destroyIterator(MOT::IndexIterator* itr);

/*---------------------------  Stateful Execution Helpers ---------------------------*/
/**
 * @brief Installs a stateful iterator (can be accessed in subsequent query execution).
 * @param itr The iterator to install.
 * @param begin_itr Specifies whether this is a begin iterator (non-zero value) or end iterator (zero value).
 * @param inner_scan Specifies whether this is an inner scan iterator in a JOIN query.
 */
void setStateIterator(MOT::IndexIterator* itr, int begin_itr, int inner_scan);

/**
 * @brief Retrieves a stateful iterator.
 * @param begin_itr Specifies whether this is a begin iterator (non-zero value) or end iterator (zero value).
 * @param inner_scan Specifies whether this is an inner scan iterator in a JOIN query.
 * @return The resulting iterator.
 */
MOT::IndexIterator* getStateIterator(int begin_itr, int inner_scan);

/**
 * @brief Queries whether a stateful iterator is null or not.
 * @param begin_itr Specifies whether this is a begin iterator (non-zero value) or end iterator (zero value).
 * @param inner_scan Specifies whether this is an inner scan iterator in a JOIN query.
 * @return Non-zero value if the state iterator is null, otherwise zero.
 */
int isStateIteratorNull(int begin_itr, int inner_scan);

/**
 * @brief Queries whether a stateful range scan ended.
 * @param forward_iterator Specifies whether this is a forward scan.
 * @param inner_scan Specifies whether this is an inner scan iterator in a JOIN query.
 * @return Non-zero value if the range scan ended, otherwise zero.
 */
int isStateScanEnd(int forward_scan, int inner_scan);

/**
 * @brief Retrieves a row from a stateful iterator during a stateful range scan.
 * @param access_mode The required row access mode.
 * @param forward_iterator Specifies whether this is a forward scan.
 * @param inner_scan Specifies whether this is an inner scan iterator in a JOIN query.
 * @return The resulting row, or NULL if end of scan reached, or some other failure occurred.
 */
MOT::Row* getRowFromStateIterator(int access_mode, int forward_scan, int inner_scan);

/**
 * @brief Destroys the stateful range scan iterators.
 * @param inner_scan Specifies whether this is an inner scan iterator in a JOIN query.
 */
void destroyStateIterators(int inner_scan);

/**
 * @brief Sets the stateful range scan ended flag.
 * @param scanEnded Non-zero if the stateful range scan ended.
 * @param innerScan Specifies whether this is an inner scan iterator in a JOIN query.
 */
void setStateScanEndFlag(int scanEnded, int innerScan);

/**
 * @brief Retrieves the value of the stateful range scan ended flag.
 * @param inner_scan Specifies whether this is an inner scan iterator in a JOIN query.
 * @return The value of the stateful range scan ended flag.
 */
int getStateScanEndFlag(int inner_scan);

/**
 * @brief Resets to NULL the outer scan row.
 * @param inner_scan Specifies whether this is an inner scan row in a JOIN query.
 */
void resetStateRow(int inner_scan);

/**
 * @brief Saves the outer scan row.
 * @param row The outer scan row.
 * @param inner_scan Specifies whether this is an inner scan row in a JOIN query.
 */
void setStateRow(MOT::Row* row, int inner_scan);

/**
 * @brief Retrieves the saved outer row.
 * @param inner_scan Specifies whether this is an inner scan row in a JOIN query.
 * @return The saved outer row.
 */
MOT::Row* getStateRow(int inner_scan);

/** @brief Copies the outer state row to a safe buffer. */
void copyOuterStateRow();

/** @brief Retrieves the outer row from a safe copy.  */
MOT::Row* getOuterStateRowCopy();

/**
 * @brief Queries whether saved outer row is null.
 * @param inner_scan Specifies whether this is an inner scan row in a JOIN query.
 * @return Non-zero value if the row is null.
 */
int isStateRowNull(int inner_scan);

/*---------------------------  Limit Clause Helpers ---------------------------*/
/** @brief Resets the limit counter for stateful scan with LIMIT clause. */
void resetStateLimitCounter();

/** @brief Increments the limit counter for stateful scan with LIMIT clause. */
void incrementStateLimitCounter();

/** @brief Retrieves the limit counter for stateful scan with LIMIT clause. */
int getStateLimitCounter();

/*---------------------------  Aggregation Helpers ---------------------------*/
/*---------------------------  Average Helpers ---------------------------*/
/**
 * @brief Prepares an array of numeric zeros for AVG() aggregate operator.
 * @param element_type The Oid of the element type of the array.
 * @param element_count The number of elements in the array.
 */
void prepareAvgArray(int aggIndex, int element_type, int element_count);

/** @brief Saves an intermediate AVG() aggregate operator accumulation. */
Datum loadAvgArray(int aggIndex);

/** @brief Saves an intermediate AVG() aggregate operator accumulation. */
void saveAvgArray(int aggIndex, Datum avg_array);

/** @brief Computes the average value from a final AVG() aggregate operator accumulation. */
Datum computeAvgFromArray(int aggIndex, int element_type);

/*---------------------------  Generic Aggregate Helpers ---------------------------*/
/** @brief Initializes a aggregated value to zero. */
void resetAggValue(int aggIndex, int element_type);

/** @brief Retrieves the value of the aggregated value used to implement SUM/MAX/MIN/COUNT() operators. */
Datum getAggValue(int aggIndex);

/** @brief Retrieves the value of the aggregated value used to implement SUM/MAX/MIN/COUNT() operators. */
void setAggValue(int aggIndex, Datum new_value);

/** @brief Retrieves the flag recording whether SUM/MAX/MIN/COUNT() value was aggregated (or it is still null). */
int getAggValueIsNull(int aggIndex);

/** @brief Sets the flag recording whether SUM/MAX/MIN/COUNT() value was aggregated (or it is still null). */
int setAggValueIsNull(int aggIndex, int isNull);

/*---------------------------  Distinct Aggregate Helpers ---------------------------*/
/** @brief Prepares a set of distinct items for DISTINCT() operator. */
void prepareDistinctSet(int aggIndex, int element_type);

/** @brief Adds an items to the distinct item set. Returns non-zero value if item was added. */
int insertDistinctItem(int aggIndex, int element_type, Datum item);

/** @brief Destroys the set of distinct items for DISTINCT() operator. */
void destroyDistinctSet(int aggIndex, int element_type);

/**
 * @brief Resets a tuple datum to zero value.
 * @param slot The tuple.
 * @param tupleColumnId The zero-based column index of the tuple in which to store the zero value.
 * @param zeroType The Oid of the zero type.
 */
void resetTupleDatum(TupleTableSlot* slot, int tupleColumnId, int zeroType);

/**
 * @brief Reads a tuple datum.
 * @param slot The tuple.
 * @param tuple_colid The zero-based column index of the tuple from which the datum is to be read.
 * @return The datum read from the tuple.
 */
Datum readTupleDatum(TupleTableSlot* slot, int tuple_colid);

/**
 * @brief Writes datum into a tuple.
 * @param slot The tuple.
 * @param tuple_colid The zero-based column index of the tuple into which the datum is to be written.
 * @param datum The datum to be written into the tuple.
 * @param isnull Specifies whether the datum is null.
 */
void writeTupleDatum(TupleTableSlot* slot, int tuple_colid, Datum datum, int isnull);

/**
 * @brief Reads the datum result of a sub-query.
 * @param subQueryIndex The sub-query index.
 * @return The sub-query result.
 */
Datum SelectSubQueryResult(int subQueryIndex);

/**
 * @brief Copies the aggregate result to a sub-query slot.
 * @param subQueryIndex The sub-query index.
 */
void CopyAggregateToSubQueryResult(int subQueryIndex);

/** @brief Retrieves the result slot of a sub-query (LLVM only). */
TupleTableSlot* GetSubQuerySlot(int subQueryIndex);

/** @brief Retrieves the table of a sub-query (LLVM only). */
MOT::Table* GetSubQueryTable(int subQueryIndex);

/** @brief Retrieves the index of a sub-query (LLVM only). */
MOT::Index* GetSubQueryIndex(int subQueryIndex);

/** @brief Retrieves the search-key of a sub-query (LLVM only). */
MOT::Key* GetSubQuerySearchKey(int subQueryIndex);

/** @brief Retrieves the end-iterator key of a sub-query (LLVM only). */
MOT::Key* GetSubQueryEndIteratorKey(int subQueryIndex);

/*---------------------------  LLVM try/catch Helpers for stored procedures ---------------------------*/

/**
 * @brief Called when entering a try-catch block.
 * @return Zero when processing the try-block, a positive value when processing a throws exception, and negative value
 * if an error occurred.
 */
int8_t* LlvmPushExceptionFrame();

/** @brief Retrieves the jump buffer to the deepest exception frame. */
int8_t* LlvmGetCurrentExceptionFrame();

/** @brief Called when leaving a try-catch block. */
int LlvmPopExceptionFrame();

/** @brief Throws an exception value. */
void LlvmThrowException(int exceptionValue);

/**
 * @brief Re-throws the recently thrown exception value (only in catch block).
 */
void LlvmRethrowException();

/** @brief Retrieves the recently thrown exception value (only in catch block). */
int LlvmGetExceptionValue();

/** @brief Retrieves the recently thrown exception status (only in catch block). */
int LlvmGetExceptionStatus();

/** @brief Sets the recently thrown exception status as handled (only in try/catch block). */
void LlvmResetExceptionStatus();

/** @brief Sets the recently thrown exception value to zero. */
void LlvmResetExceptionValue();

/** @brief Called when a try-catch block did not handle the current exception. */
void LlvmUnwindExceptionFrame();

/** @brief Cleans up exception stack due to normal return. */
void LlvmClearExceptionStack();

/** @brief Helper for debugging. */
void LLvmPrintFrame(const char* msg, int8_t* frame);

/*---------------------------  Stored Procedure Helpers ---------------------------*/
/** @brief Abort executed stored procedure with this error code. . */
void JitAbortFunction(int errorCode);

/** @brief Queries whether any of the given parameters is null. */
int JitHasNullParam(ParamListInfo params);

/** @brief Queries the number of parameters stored in the given parameter list. */
int JitGetParamCount(ParamListInfo params);

/** @brief Retrieves the parameter at the specified position from the given parameter list. */
Datum JitGetParamAt(ParamListInfo params, int paramId);

/** @brief Queries whether the parameter in the given index is null. */
int JitIsParamNull(ParamListInfo params, int paramId);

/** @brief Retrieves the parameter at the specified position from the given parameter list. */
Datum* JitGetParamAtRef(ParamListInfo params, int paramId);

/** @brief Queries whether the parameter in the given index is null. */
bool* JitIsParamNullRef(ParamListInfo params, int paramId);

/** @brief Gets the parameter list associated with the current JIT context used for invoking a stored procedure. */
ParamListInfo GetInvokeParamListInfo();

/** @brief Destroys a parameter array. */
void DestroyParamListInfo(ParamListInfo params);

/** @brief Sets a parameter value in the given parameter list. */
void SetParamValue(ParamListInfo params, int paramId, int paramType, Datum datum, int isNull);

/** @brief Sets a parameter value in the given stored procedure sub-query. */
void SetSPSubQueryParamValue(int subQueryId, int id, int type, Datum value, int isNull);

/** @brief Invoke the stored procedure referred by the current invoke context. */
int InvokeStoredProcedure();

/** @brief Queries whether the given slot contains a composite result tuple. */
int IsCompositeResult(TupleTableSlot* slot);

/** @brief Create datum array for result heap tuple (composite return value). */
Datum* CreateResultDatums();

/** @brief Create null array for result heap tuple (composite return value). */
bool* CreateResultNulls();

/** @brief Set result value for result heap tuple (composite return value). */
void SetResultValue(Datum* datums, bool* nulls, int index, Datum value, int isNull);

/** @brief Creates a result heap tuple for the currently executing jitted stored procedure. */
Datum CreateResultHeapTuple(Datum* dvalues, bool* nulls);

/** @brief Sets a datum value in a result slot at the given index. */
void SetSlotValue(TupleTableSlot* slot, int tupleColId, Datum value, int isNull);

/** @brief Retrieves the datum value in a result slot at the given index. */
Datum GetSlotValue(TupleTableSlot* slot, int tupleColId);

/** @brief Retrieves the datum is-null property in a result slot at the given index. */
int GetSlotIsNull(TupleTableSlot* slot, int tupleColId);

/** @brief Retrieves the current sub-transaction id. */
SubTransactionId JitGetCurrentSubTransactionId();

/** @brief Releases all JIT SP active and open sub-transactions. */
void JitReleaseAllSubTransactions(bool rollback, SubTransactionId untilSubXid);

/** @brief Execute required code at beginning of statement block with exceptions. */
void JitBeginBlockWithExceptions();

/** @brief Execute required code at end of statement block with exceptions. */
void JitEndBlockWithExceptions();

/** @brief Execute required code exception handler. */
void JitCleanupBlockAfterException();

/** @brief Sets the current exception origin. */
void JitSetExceptionOrigin(int origin);

/** @brief Retrieves the current exception origin. */
int JitGetExceptionOrigin();

/** @brief Retrieves reference to the current exception origin variable. */
int* JitGetExceptionOriginRef();

/** @brief Do all clean-up required before returning from jitted SP. */
void JitCleanupBeforeReturn(SubTransactionId initSubTxnId);

/** @brief Executes a sub-query invoked from a stored procedure. */
int JitExecSubQuery(int subQueryId, int tcount);

/** @brief Release resources used during execution of non-jittable sub-query. */
void JitReleaseNonJitSubQueryResources(int subQueryId);

/** @brief Retrieves the number of tuples processes by the last sub-query invoked from a stored procedure. */
int JitGetTuplesProcessed(int subQueryId);

/** @brief Retrieves datum value from the result tuple of the last sub-query invoked from a stored procedure. */
Datum JitGetSubQuerySlotValue(int subQueryId, int tupleColId);

/** @brief Retrieves datum is-null from the result tuple of the last sub-query invoked from a stored procedure. */
int JitGetSubQuerySlotIsNull(int subQueryId, int tupleColId);

/** @brief Retrieves the result heap tuple associated with the composite sub-query. */
HeapTuple JitGetSubQueryResultHeapTuple(int subQueryId);

/** @brief Retrieves the datum at the given position in the heap tuple. */
Datum JitGetHeapTupleValue(HeapTuple tuple, int subQueryId, int columnId, int* isNull);

/** @brief Get the execution result of a non-jittable SP query. */
int JitGetSpiResult(int subQueryId);

/** @brief Converts one datum type to another via string. */
Datum JitConvertViaString(Datum value, Oid resultType, Oid targetType, int typeMod);

/** @brief Cast datum value. */
Datum JitCastValue(Datum value, Oid sourceType, Oid targetType, int typeMod, int coercePath, Oid funcId,
    int coercePath2, Oid funcId2, int nargs, int typeByVal);

/** @brief Saves error information of called function without exception block. */
void JitSaveErrorInfo(Datum errorMessage, int sqlState, Datum sqlStateString);

/** @brief Retrieves the error message associated with the last SQL error. */
Datum JitGetErrorMessage();

/** @brief Retrieves the SQL state associated with the last SQL error. */
int JitGetSqlState();

/** @brief Retrieves the SQL state (string-form) associated with the last SQL error. */
Datum JitGetSqlStateString();

/** @brief Converts int value to inverse Boolean datum value. */
Datum JitGetDatumIsNotNull(int isNull);

/** @brief Emit profile data. */
void EmitProfileData(uint32_t functionId, uint32_t regionId, int beginRegion);
}  // extern "C"

#endif
