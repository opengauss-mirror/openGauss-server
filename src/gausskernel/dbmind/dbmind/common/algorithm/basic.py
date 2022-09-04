# Copyright (c) 2020 Huawei Technologies Co.,Ltd.
#
# openGauss is licensed under Mulan PSL v2.
# You can use this software according to the terms and conditions of the Mulan PSL v2.
# You may obtain a copy of Mulan PSL v2 at:
#
#          http://license.coscl.org.cn/MulanPSL2
#
# THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
# EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
# MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
# See the Mulan PSL v2 for more details.


def binary_search(L, target, tolerance=0):
    """A binary search with left-closed and right-opened style.

    :return the index of specific target; if not found, return -1.
    """
    if len(L) == 0:
        return -1
    # [0, length)
    lo, hi = 0, len(L)
    while lo < hi:  # equals to lo == hi
        mid = lo + (hi - lo) // 2
        if abs(L[mid] - target) <= tolerance:
            return mid
        elif L[mid] < target:
            # [mid + 1, hi)
            lo = mid + 1
        elif L[mid] > target:
            # [lo, mid)
            hi = mid
    return -1


def binary_search_leftmost(L, target):
    """The function bases on finding the leftmost element with binary search.

    About Binary Search
    =============

    ..

        Rank queries can be performed with the procedure for finding the leftmost element.
        The number of elements less than the target target is returned by the procedure.

        -- Wikipedia: binary search algorithm


    The pseudocode for finding the leftmost element:
    https://en.wikipedia.org/wiki/Binary_search_algorithm#Procedure_for_finding_the_leftmost_element
    """
    if len(L) == 0:
        return -1
    # [0, length - 1]
    lo, hi = 0, len(L) - 1
    while lo <= hi:  # equals to lo == hi + 1
        mid = lo + (hi - lo) // 2
        if L[mid] == target:
            hi = mid - 1  # shrink right bound
        elif L[mid] < target:
            # [mid + 1, hi]
            lo = mid + 1
        elif L[mid] > target:
            # [lo, mid - 1]
            hi = mid - 1
    return lo


def binary_search_left(L, target):
    """Wrap the function ``how_many_lesser_elements(L, target)`` by adding
    a check for return target.

    :return -1 when not found the target target.
    """
    lo = binary_search_leftmost(L, target)
    return -1 if lo >= len(L) or L[lo] != target else lo


def binary_search_rightmost(L, target):
    """Similar to above function."""
    if len(L) == 0:
        return -1
    # [0, length - 1]
    lo, hi = 0, len(L) - 1
    while lo <= hi:  # equals to lo == hi + 1
        mid = lo + (hi - lo) // 2
        if L[mid] == target:
            lo = mid + 1  # shrink left bound
        elif L[mid] < target:
            # [mid + 1, hi]
            lo = mid + 1
        elif L[mid] > target:
            # [lo, mid - 1]
            hi = mid - 1
    return hi


def binary_search_right(L, target):
    hi = binary_search_rightmost(L, target)
    return -1 if hi < 0 or L[hi] != target else hi


how_many_lesser_elements = binary_search_leftmost


def how_many_larger_elements(L, target):
    right_most = binary_search_right(L, target)
    if right_most >= 0:
        return len(L) - 1 - right_most
    return len(L) - binary_search_leftmost(L, target)


def dek_hash(str_):
    """An algorithm proposed by Donald E. Knuth in The Art Of Computer Programming Volume 3,
     under the topic of sorting and search chapter 6.4.
    """
    length = len(str_)
    hashcode = length

    for char in str_:
        hashcode = ((hashcode << 5) ^ (hashcode >> 27)) ^ ord(char)
        hashcode &= 0xFFFFFFFF
    return hashcode


def djb_hash(str_):
    """An algorithm produced by Professor Daniel J. Bernstein
    and shown first to the world on the usenet newsgroup comp.lang.c.
    It is one of the most efficient hash functions ever published.
    """
    hashcode = 5381

    for char in str_:
        hashcode = ((hashcode << 5) + hashcode) + ord(char)
        hashcode &= 0xFFFFFFFF
    return hashcode
