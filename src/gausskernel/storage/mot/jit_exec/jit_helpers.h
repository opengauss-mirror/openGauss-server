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

#include "jit_pgproc.h"
#include "mot_engine.h"

/*---------------------------  LLVM Access Helpers ---------------------------*/
extern "C" {

/**
 * @brief Issue debug log message.
 * @param function The function name.
 * @param msg The log message.
 */
void debugLog(const char* function, const char* msg);

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
 * @brief Marks expression evaluated to null or not in the specified argument position.
 * @param arg_pos The ordinal position of the expression.
 * @param isnull Specifies whether the expression evaluated to null.
 */
void setExprArgIsNull(int arg_pos, int isnull);

/**
 * @brief Queries whether an expression in the specified argument position evaluated to null.
 * @param arg_pos The ordinal position of the expression.
 * @return Non-zero value if the expression in the specified argument position evaluated to null,
 * otherwise zero.
 */
int getExprArgIsNull(int arg_pos);

/**
 * @brief Retrieves a pooled constant by its identifier.
 * @param constId The identifier of the constant value.
 * @param argPos The ordinal position of the enveloping parameter expression.
 * @return The constant value.
 */
Datum GetConstAt(int constId, int argPos);

/**
 * @brief Retrieves a datum parameter from parameters array.
 * @param params The parameter array.
 * @param paramid The zero-based index of the parameter to retrieve.
 * @param arg_pos The ordinal position of the enveloping parameter expression.
 * @return The parameter value.
 */
Datum getDatumParam(ParamListInfo params, int paramid, int arg_pos);

/**
 * @brief Reads a column value from a row.
 * @param table The originating table of the row.
 * @param row The row from which the column value is to be retrieved.
 * @param colid The zero-based index of the column to retrieve.
 * @param arg_pos The ordinal position of the enveloping VAR expression.
 * @return The column value.
 */
Datum readDatumColumn(MOT::Table* table, MOT::Row* row, int colid, int arg_pos);

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
// cast operators retain first null parameter as null result
// other operator will crash if null is provided
#define APPLY_UNARY_OPERATOR(funcid, name) Datum invoke_##name(Datum arg, int arg_pos);

#define APPLY_UNARY_CAST_OPERATOR(funcid, name) Datum invoke_##name(Datum arg, int arg_pos);

#define APPLY_BINARY_OPERATOR(funcid, name) Datum invoke_##name(Datum lhs, Datum rhs, int arg_pos);

#define APPLY_BINARY_CAST_OPERATOR(funcid, name) Datum invoke_##name(Datum lhs, Datum rhs, int arg_pos);

#define APPLY_TERNARY_OPERATOR(funcid, name) Datum invoke_##name(Datum arg1, Datum arg2, Datum arg3, int arg_pos);

#define APPLY_TERNARY_CAST_OPERATOR(funcid, name) Datum invoke_##name(Datum arg1, Datum arg2, Datum arg3, int arg_pos);

APPLY_OPERATORS()

#undef APPLY_UNARY_OPERATOR
#undef APPLY_BINARY_OPERATOR
#undef APPLY_TERNARY_OPERATOR
#undef APPLY_UNARY_CAST_OPERATOR
#undef APPLY_BINARY_CAST_OPERATOR
#undef APPLY_TERNARY_CAST_OPERATOR

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
 * @return The requested row, or NULL if the row was not found, or some other failure occurred.
 */
MOT::Row* searchRow(MOT::Table* table, MOT::Key* key, int access_mode_value);

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
 * @param forward_scan Specifies whether this is a forward scan. If true then a forward iterator is
 * created (such that the an iterator pointing to the value succeeding the lower bound is returned).
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
 * @param forward_scan Specifies whether this is a forward scan. If true then a reverse iterator is
 * created (such that the an iterator pointing to the value preceeding the upper bound is returned).
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
 * @brief Retrieves a row from an iterator during a range scan.
 * @param index The index on which the range scan is performed.
 * @param itr The begin iterator.
 * @param end_itr The end iterator.
 * @param access_mode The required row access mode.
 * @param forward_iterator Specifies whether this is a forward scan.
 * @return The resulting row, or NULL if end of scan reached, or some other failure occurred.
 */
MOT::Row* getRowFromIterator(
    MOT::Index* index, MOT::IndexIterator* itr, MOT::IndexIterator* end_itr, int access_mode, int forward_scan);

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
void prepareAvgArray(int element_type, int element_count);

/** @brief Saves an intermediate AVG() aggregate operator accumulation. */
Datum loadAvgArray();

/** @brief Saves an intermediate AVG() aggregate operator accumulation. */
void saveAvgArray(Datum avg_array);

/** @brief Computes the average value from a final AVG() aggregate operator accumulation. */
Datum computeAvgFromArray(int element_type);

/*---------------------------  Generic Aggregate Helpers ---------------------------*/
/** @brief Initializes a aggregated value to zero. */
void resetAggValue(int element_type);

/** @brief Retrieves the value of the aggregated value used to implement SUM/MAX/MIN/COUNT() operators. */
Datum getAggValue();

/** @brief Retrieves the value of the aggregated value used to implement SUM/MAX/MIN/COUNT() operators. */
void setAggValue(Datum new_value);

/*---------------------------  Min/Max Aggregate Helpers ---------------------------*/
/** @brief Resets the flag recording whether a MAX/MIN value was aggregated (or it is still null). */
void resetAggMaxMinNull();

/** @brief Records that a MAX/MIN value was aggregated. */
void setAggMaxMinNotNull();

/** @brief Retrieves the flag recording whether a MAX/MIN value was aggregated (or it is still null). */
int getAggMaxMinIsNull();

/*---------------------------  Distinct Aggregate Helpers ---------------------------*/
/** @brief Prepares a set of distinct items for DISTINCT() operator. */
void prepareDistinctSet(int element_type);

/** @brief Adds an items to the distinct item set. Returns non-zero value if item was added. */
int insertDistinctItem(int element_type, Datum item);

/** @brief Destroys the set of distinct items for DISTINCT() operator. */
void destroyDistinctSet(int element_type);

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
 * @param arg_pos The ordinal position of the enveloping expression.
 * @return The datum read from the tuple.
 */
Datum readTupleDatum(TupleTableSlot* slot, int tuple_colid, int arg_pos);

/**
 * @brief Writes datum into a tuple.
 * @param slot The tuple.
 * @param tuple_colid The zero-based column index of the tuple into which the datum is to be written.
 * @param datum The datum to be written into the tuple.
 */
void writeTupleDatum(TupleTableSlot* slot, int tuple_colid, Datum datum);

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
}  // extern "C"

#endif
