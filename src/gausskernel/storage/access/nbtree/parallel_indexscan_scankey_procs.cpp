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
 * ---------------------------------------------------------------------------------------
 *
 * parallel_indexscan_scankey_procs.cpp
 *
 *
 *
 * IDENTIFICATION
 *        src\gausskernel\storage\access\nbtree\parallel_indexscan_scankey_procs.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
#include "postgres.h"
#include "access/nbtree.h"
#include "access/parallel_indexscan_core.h"

/*
 * @brief  _bt_get_locating_start_scankey
 *  Get start scan block of the current thread
 * @param  cur                  current scankey
 * @param  chosen               temporary variable
 * @param  implies_not_null     temporary variable
 * @param  dir                  scan direction
 * @return void
 */
void _bt_get_locating_start_scankey(ScanKey* cur, ScanKey* chosen, ScanKey* implies_not_null, ScanDirection dir)
{
    switch ((*cur)->sk_strategy) {
        case BTEqualStrategyNumber:
            *chosen = *cur;
            break;
        case BTLessStrategyNumber:
        case BTLessEqualStrategyNumber:
            if (*chosen == NULL) {
                if (ScanDirectionIsBackward(dir)) {
                    *chosen = *cur;
                } else {
                    *implies_not_null = *cur;
                }
            }
            break;
        case BTGreaterEqualStrategyNumber:
        case BTGreaterStrategyNumber:
            if (*chosen == NULL) {
                if (ScanDirectionIsForward(dir)) {
                    *chosen = *cur;
                } else {
                    *implies_not_null = *cur;
                }
            }
            break;
        default:
            break;
    }
}

/*
 * @brief  _bt_get_start_keys
 *  Gets the condition used to locate the start scan position
 * @param  scan                 IndexScanDesc
 * @param  dir                  scan direction
 * @param  start_keys           different scanning conditions
 * @param  para_strat_total     startkeys number
 * @return int                  startkeys number
 */
int _bt_get_start_keys(IndexScanDesc scan, ScanDirection dir, ScanKey (&start_keys)[INDEX_MAX_KEYS],
                       StrategyNumber& para_strat_total)
{
    ScanKeyData* notnullkeys = (ScanKeyData*)palloc0(INDEX_MAX_KEYS * sizeof(ScanKeyData));
    int keys_count = 0;
    StrategyNumber strat = InvalidStrategy;
    BTScanOpaque so = (BTScanOpaque)scan->opaque;
    int i = 0;
    if (so->numberOfKeys > 0) {
        AttrNumber para_curattr = 1;
        ScanKey para_chosen = NULL;
        ScanKey para_impies_not_null = NULL;
        ScanKey cur = NULL;
        for (cur = so->keyData, i = 0;; cur++, i++) {
            if (i >= so->numberOfKeys || cur->sk_attno != para_curattr) {
                if (para_chosen == NULL && para_impies_not_null != NULL &&
                    ((para_impies_not_null->sk_flags & SK_BT_NULLS_FIRST) ? ScanDirectionIsForward(dir)
                                                                          : ScanDirectionIsBackward(dir))) {
                    para_chosen = &notnullkeys[keys_count];
                    ScanKeyEntryInitialize(
                        para_chosen,
                        (SK_SEARCHNOTNULL | SK_ISNULL |
                         (para_impies_not_null->sk_flags & (SK_BT_DESC | SK_BT_INDOPTION_SHIFT))),
                        para_curattr,
                        ((para_impies_not_null->sk_flags & SK_BT_NULLS_FIRST) ? BTGreaterStrategyNumber
                                                                              : BTLessStrategyNumber),
                        InvalidOid, InvalidOid, InvalidOid, (Datum)0);
                }
                if (para_chosen == NULL) {
                    break;
                }
                start_keys[keys_count++] = para_chosen;
                strat = para_chosen->sk_strategy;
                if (strat != BTEqualStrategyNumber) {
                    para_strat_total = strat;
                    if (strat == BTGreaterStrategyNumber || strat == BTLessStrategyNumber) {
                        break;
                    }
                }
                if (i >= so->numberOfKeys || cur->sk_attno != para_curattr + 1) {
                    break;
                }
                para_curattr = cur->sk_attno;
                para_chosen = NULL;
                para_impies_not_null = NULL;
            }
            _bt_get_locating_start_scankey(&cur, &para_chosen, &para_impies_not_null, dir);
        }
    }
    return keys_count;
}

/*
 * @brief _bt_get_inskey_scankey_with_rowheader
 *  Gets the condition used to locate the start scan position
 * @param  cur                  current scankey
 * @param  inskey               BTScanInsertData
 * @param  param_strat_total    different scanning conditions
 * @param  keys_count           startkeys number
 * @param  i                    index
 * @param  continue_loop        continue iteration
 * @return bool                 returns true if there is no error, false otherwise
 */
bool _bt_get_inskey_scankey_with_rowheader(ScanKey cur, BTScanInsertData* inskey, StrategyNumber& param_strat_total,
                                           int& keys_count, int i, bool& continue_loop)
{
    ScanKey para_subkey = (ScanKey)DatumGetPointer(cur->sk_argument);
    Assert(para_subkey->sk_flags & SK_ROW_HEADER);
    if (para_subkey->sk_flags & SK_ISNULL) {
        return false;
    }
    inskey->scankeys[i] = *para_subkey;
    if (i == keys_count - 1) {
        bool used_all_subkeys = false;
        Assert(!(para_subkey->sk_flags & SK_ROW_END));
        for (;;) {
            para_subkey++;
            Assert(para_subkey->sk_flags & SK_ROW_MEMBER);
            if (para_subkey->sk_attno != keys_count + 1) {
                break; /* out-of-sequence, can't use it */
            }
            if (para_subkey->sk_strategy != cur->sk_strategy) {
                break; /* wrong direction, cna't use it */
            }
            if (para_subkey->sk_flags & SK_ISNULL) {
                break; /* can't use null keys */
            }
            Assert(keys_count < INDEX_MAX_KEYS);
            inskey->scankeys[keys_count] = *para_subkey;
            keys_count++;
            if (para_subkey->sk_flags & SK_ROW_END) {
                used_all_subkeys = true;
                break;
            }
        }
        if (!used_all_subkeys) {
            switch (param_strat_total) {
                case BTLessStrategyNumber:
                    param_strat_total = BTLessEqualStrategyNumber;
                    break;
                case BTGreaterStrategyNumber:
                    param_strat_total = BTGreaterEqualStrategyNumber;
                    break;
                default:
                    break;
            }
        }
        continue_loop = false;
    }
    return true;
}

/*
 * @brief _bt_get_first_buf_without_scankey
 *  If the number of start conditions is 0, the start buffer and offset are returned.
 * @param  scan     refer to IndexScanDesc(btree & ubtree)
 * @param  dir      Scanning direction
 * @param  offnum   Start offset
 * @return Buffer   Start scanning buffer
 */
Buffer _bt_get_first_buf_without_scankey(IndexScanDesc scan, ScanDirection dir, OffsetNumber* offnum)
{
    Relation rel = scan->indexRelation;
    BlockNumber end_block = scan->btps_end_block;
    Buffer buf = _bt_get_endpoint(rel, 0, ScanDirectionIsBackward(dir), end_block);
    if (BufferIsInvalid(buf)) {
        return InvalidBuffer;
    }
    BTPageOpaqueInternal opaque = (BTPageOpaqueInternal)PageGetSpecialPointer(BufferGetPage(buf));
    if (ScanDirectionIsBackward(dir)) {
        *offnum = PageGetMaxOffsetNumber(BufferGetPage(buf));
    } else if (ScanDirectionIsForward(dir)) {
        /* There could be dead pages to the left, so not this. */
        *offnum = P_FIRSTDATAKEY(opaque);
    }
    return buf;
}