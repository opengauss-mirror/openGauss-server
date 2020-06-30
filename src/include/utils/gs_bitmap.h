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
 * gs_bitmap.h
 *     about null bitmap set/query actions
 *     1.  bitmap_size
 *     2.  bitmap_been_set
 *     3.  bitmap_set
 * 
 * 
 * IDENTIFICATION
 *        src/include/utils/gs_bitmap.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef CODE_SRC_INCLUDE_NULL_BITMAP_H
#define CODE_SRC_INCLUDE_NULL_BITMAP_H

/*
 * bitmap_size
 *      Return the bitmap byte-size of x number.
 */
#define bitmap_size(x) (((x) + 7) / 8)

/*
 * @Description: query the value is null or not.
 * @IN bitmap: null bitmap
 * @IN which: which bit
 * @Return: true, if this bit is set to 1;
 *          false, if this bit is set to 0;
 * @See also: BitmapSize()
 */
static inline bool bitmap_been_set(char* bitmap, int which)
{
    return (bitmap[which / 8] & (1 << (which % 8)));
}

/*
 * @Description: set NULL bit in bitmap.
 *    caller must reset the whole bitmap to 0 first.
 * @IN/OUT bitmap: NULL bitmap
 * @IN which: which value is null
 * @See also:
 */
static inline void bitmap_set(char* bitmap, int which)
{
    bitmap[which / 8] |= (1 << (which % 8));
}

#endif /* CODE_SRC_INCLUDE_NULL_BITMAP_H */
