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
 * mot_string.h
 *    An STL-like string with safe memory management and bounded size.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/infra/containers/mot_string.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef MOT_STRING_H
#define MOT_STRING_H

// for some useful specialization
#include "global.h"
#include "infra.h"
#include "mot_error_codes.h"

#include <string.h>
#include <stdio.h>
#include <stdarg.h>
#include <wchar.h>
#include <ctype.h>

#include <utility>

namespace MOT {
/**
 * @class mot_basic_string
 * @brief An STL-like string with safe memory management, and bounded size. The functionality provided by this class
 * is not as complete as std::string, but sufficient for our needs.
 * @tparam Allocator The allocator used for managing the string buffer.
 */
template <typename Allocator>
class mot_basic_string {
public:
    /**
     * @brief Default constructor.
     * @param size_limit The upper size limit for the string length.
     * @param capacity The initial capacity for the string.
     */
    explicit mot_basic_string(uint32_t sizeLimit = DEFAULT_SIZE_LIMIT, uint32_t capacity = INIT_CAPACITY)
        : _str(nullptr), _size(0), _capacity(0), _size_limit(sizeLimit)
    {
        // we initialize capacity member to zero on purpose because string object has not been allocated yet
        if (capacity > 0) {
            if (this->realloc(capacity)) {
                MOT_INFRA_ASSERT(_str);
                _str[0] = 0;
            }
        }
    }

    /**
     * @brief Copy constructor.
     * @param other The object to copy the string from.
     */
    mot_basic_string(const mot_basic_string& other) : mot_basic_string(other._size_limit, other._capacity)
    {
        if (_str) {
            errno_t erc = strcpy_s(_str, _capacity, other._str);
            securec_check(erc, "\0", "\0");
            _size = other._size;
        }
    }

    /** @brief Move constructor.  */
    mot_basic_string(mot_basic_string&& other)
        : _str(other._str), _size(other._size), _capacity(other._capacity), _size_limit(other._size_limit)
    {
        other._str = nullptr;
        other._size = 0;
        other._capacity = 0;
        other._size_limit = 0;
    }

    /**
     * @brief Constructs a string object from another null-terminated c-string.
     * @param str The string with which the object is to be initialized.
     */
    mot_basic_string(const char* str) : mot_basic_string()  // call default constructor
    {
        assign(str);
    }

    /**
     * @brief Constructs a string object from another c-string.
     * @param str The string with which the object is to be initialized.
     * @param n The number of character to copy from the source string.
     */
    mot_basic_string(const char* str, size_t n) : mot_basic_string()  // call default constructor
    {
        assign(str, n);
    }

    /** @brief Destructor. */
    ~mot_basic_string() noexcept
    {
        if (_str) {
            Allocator::free(_str);
        }
    }

    /** @var Invalid position in the string. */
    static constexpr uint32_t npos = (uint32_t)-1;

    /** @brief Queries whether the string is valid. */
    inline bool is_valid() const
    {
        return _str != nullptr;
    }

    /** @brief Queries whether the string is empty. */
    inline bool empty() const
    {
        return (_size == 0) || (_str == nullptr) || (_str[0] == 0);
    }

    /** @brief Retrieves the underlying string. */
    inline const char* c_str() const
    {
        return _str;
    }

    /** @brief Retrieves the current string length (not including terminating null). */
    inline uint32_t length() const
    {
        return _size;
    }

    /** @brief Retrieves the current string capacity. */
    inline uint32_t capacity() const
    {
        return _capacity;
    }

    /** @brief Retrieves the upper size limit of the string. */
    inline uint32_t size_limit() const
    {
        return _size_limit;
    }

    /** @brief Clears the contents of the string. */
    inline void clear()
    {
        if (_str) {
            _str[0] = 0;
        }
        _size = 0;
    }

    /**
     * @brief Assigns the contents of the string from another string.
     * @param str The string to copy.
     * @param size The length of the string (in characters) to copy (not including terminating null).
     * @return True if copying was successful otherwise false.
     */
    bool assign(const char* str, uint32_t size)
    {
        bool result = true;
        uint32_t newCapacity = size + 1;
        if (newCapacity > _capacity) {
            result = this->realloc(newCapacity);
        }
        if (result) {
            MOT_INFRA_ASSERT(newCapacity <= _capacity);
            errno_t erc = strncpy_s(_str, _capacity, str, size);
            securec_check(erc, "\0", "\0");
            _size = size;
            _str[_size] = 0;
        }
        return result;
    }

    /**
     * @brief Assigns the contents of the string from another string.
     * @param str The string to copy.
     * @return True if copying was successful otherwise false.
     */
    inline bool assign(const char* str)
    {
        return assign(str, (uint32_t)::strlen(str));
    }

    /**
     * @brief Assigns the contents of the string from another string.
     * @param other The string from which to copy.
     * @return True if copying was successful otherwise false.
     */
    inline bool assign(const mot_basic_string& other)
    {
        return assign(other._str, other._size);
    }

    /**
     * @brief Moves another string object into this object. The contents of this object are discarded.
     * @param other The object to move into this object.
     */
    void move(mot_basic_string&& other)
    {
        if (this != &other) {
            // cleanup any existing content
            if (_str) {
                Allocator::free(_str);
            }

            // copy other string members
            _str = other._str;
            _size = other._size;
            _capacity = other._capacity;
            _size_limit = other._size_limit;

            // reset other string members
            other._str = nullptr;
            other._size = 0;
            other._capacity = 0;
            other._size_limit = 0;
        }
    }

    /**
     * @brief Appends a string to this string.
     * @param str The string to append.
     * @param size The length of the string to append (not including terminating null).
     * @return True if appending was successful otherwise false.
     */
    bool append(const char* str, uint32_t size)
    {
        uint32_t newCapacity = _size + size + 1;
        bool result = true;
        if (newCapacity > _capacity) {
            result = this->realloc(newCapacity);
        }
        if (result) {
            MOT_INFRA_ASSERT(newCapacity <= _capacity);
            errno_t erc = strncpy_s(_str + _size, _capacity - _size, str, size);
            securec_check(erc, "\0", "\0");
            _size += size;
            _str[_size] = 0;
        }
        return result;
    }

    /**
     * @brief Appends a string to this string.
     * @param str The string to append.
     * @return True if appending was successful otherwise false.
     */
    inline bool append(const char* str)
    {
        return append(str, strlen(str));
    }

    /**
     * @brief Appends a single character to this string.
     * @param c The character to append.
     * @return True if appending was successful otherwise false.
     */
    inline bool append(char c)
    {
        return append(&c, 1);
    }

    /**
     * @brief Appends a configuration string to this string.
     * @param other The configuration string to append.
     * @return True if appending was successful otherwise false.
     */
    inline bool append(const mot_basic_string& other)
    {
        return append(other._str, other._size);
    }

    /**
     * @brief Trims this string from leading and trailing whitespace characters.
     */
    void trim()
    {
        if (_str) {  // trim of empty string has no effect
            uint32_t leadingWhitespaceCount = count_lead_ws();
            if (leadingWhitespaceCount == _size) {  // trim entire string
                _size = 0;
            } else {
                uint32_t trailingWhitespaceCount = count_trail_ws();
                uint32_t orgSize = _size;
                _size = _size - leadingWhitespaceCount - trailingWhitespaceCount;
                errno_t erc = memmove_s(_str, orgSize, _str + leadingWhitespaceCount, _size);
                securec_check(erc, "\0", "\0");
            }
            _str[_size] = 0;
        }
    }

    /**
     * @brief Formats this string.
     * @param fmt_str The format string.
     * @param ... Additional optional arguments required by the format string.
     * @return True if formatting was successful otherwise false.
     */
    bool format(const char* fmt_str, ...) __attribute__((format(printf, 2, 3)))
    {
        bool result = true;
        va_list args;
        va_start(args, fmt_str);
        uint32_t size = (uint32_t)::vsnprintf(nullptr, 0, fmt_str, args);
        va_end(args);
        uint32_t newCapacity = size + 1;

        if (newCapacity > _capacity) {
            result = this->realloc(newCapacity);
        }
        if (result) {
            MOT_INFRA_ASSERT(newCapacity <= _capacity);
            // format the message
            va_start(args,
                fmt_str);  // must reaffirm after previous call to vsnprintf()! (or otherwise use va_copy() earlier)
            errno_t erc = vsnprintf_s(_str, _capacity, _capacity - 1, fmt_str, args);
            securec_check_ss(erc, "\0", "\0");
            _size = size;
            va_end(args);
        }

        return result;
    }

    /**
     * @brief Copies a substring of this string into another string.
     * @param[out] result A string to receive the resulting substring.
     * @param[opt] start_pos The zero-based start position for forming the substring.
     * @param[opt] length The substring length.
     * @return True if formatting was successful otherwise false.
     */
    inline bool substr(mot_basic_string& result, uint32_t start_pos = 0, uint32_t length = npos) const
    {
        if (length == npos) {
            length = _size - start_pos;  // take as much as there is up to the end
        }
        return result.assign(_str + start_pos, length);
    }

    /**
     * @brief Concatenates another string to this string and puts the result in another string. This
     * object plays the role of the left hand side string.
     * @note Unlike @ref append(), this method does not affect the calling string object.
     * @param rhs The right hand side string to concatenate.
     * @param[out] result The concatenation result.
     * @return True if concatenation was successful otherwise false.
     */
    inline bool concat(const mot_basic_string& rhs, mot_basic_string& result) const
    {
        bool opResult = false;
        if (result.assign(*this)) {
            opResult = result.append(rhs);
        }
        return opResult;
    }

    /**
     * @brief Retrieves the character at the specified position.
     * @param pos The zero-based character position.
     * @return The character or zero if index is out of range.
     */
    inline char at(uint32_t pos) const
    {
        char result = 0;
        MOT_INFRA_ASSERT(pos < _size);
        if (pos < _size) {
            result = _str[pos];
        }
        return result;
    }

    /**
     * @brief Counts the number of occurrences of a given character in the string.
     * @param c The character to count.
     * @return The number of occurrences.
     */
    inline uint32_t count(char c) const
    {
        uint32_t result = 0;
        for (uint32_t i = 0; i < _size; ++i) {
            if (_str[i] == c) {
                ++result;
            }
        }
        return result;
    }

    /**
     * @brief Performs a lexicographical comparison between this string and another string.
     * @param str The other string with which to compare this string.
     * @return Negative value if this string is smaller than the given string, zero if they are equal, or a positive
     * value if this string is larger than the given string.
     */
    inline int compare(const char* str) const
    {
        return ::strcmp(_str, str);
    }

    /**
     * @brief Performs a case-insensitive lexicographical comparison between this string and another string.
     * @param str The other string with which to compare this string.
     * @return Negative value if this string is smaller than the given string, zero if they are equal, or a positive
     * value if this string is larger than the given string.
     */
    inline int compare_no_case(const char* str) const
    {
        return ::strcasecmp(_str, str);
    }

    /**
     * @brief Performs a lexicographical comparison between this string and another string.
     * @param str The other string with which to compare this string.
     * @return Negative value if this string is smaller than the given string, zero if they are equal, or a positive
     * value if this string is larger than the given string.
     */
    inline int compare(const mot_basic_string& str) const
    {
        return ::strcmp(_str, str._str);
    }

    /**
     * @brief Searches for the first occurrence of a character within this string.
     * @param c The character to search.
     * @param[opt] from_pos The start position within this string from which the search is to begin.
     * @return The resulting character position or @ref npos if not found.
     */
    inline uint32_t find(char c, uint32_t from_pos = 0) const
    {
        uint32_t pos = npos;
        char* posStr = ::strchr((char*)(_str + from_pos), c);
        if (posStr != nullptr) {
            pos = posStr - _str;  // return absolute position
        }
        return pos;
    }

    /**
     * @brief Searches for the first occurrence of a string within this string.
     * @param str The string to search.
     * @param[opt] from_pos The start position within this string from which the search is to begin.
     * @return The resulting string position or @ref npos if not found.
     */
    inline uint32_t find(const char* str, uint32_t from_pos = 0) const
    {
        uint32_t pos = npos;
        char* posStr = ::strstr((char*)(_str + from_pos), str);
        if (posStr != nullptr) {
            pos = posStr - _str;  // return absolute position
        }
        return pos;
    }

    /**
     * @brief Finds the last occurrence of a given character within this string.
     * @param c The character to search
     * @param[opt] upto_pos Specifies the maximum character position (not including the maximum position itself) up to
     * which the search is to be made.
     * @return The position of the last occurrence of the searched character or @ref npos.
     */
    inline uint32_t find_last_of(char c, uint32_t upto_pos = npos) const
    {
        if (upto_pos > _size) {
            upto_pos = _size;
        }
        for (uint32_t i = 0; i < upto_pos; ++i) {
            if (_str[upto_pos - i - 1] == c) {
                return upto_pos - i - 1;
            }
        }
        return npos;
    }

    /**
     * @brief Replaces all occurrences of a character within this string.
     * @param old_char The old character to replace.
     * @param new_char The new character to put instead.
     */
    inline void replace(char old_char, char new_char)
    {
        for (uint32_t i = 0; i < _size; ++i) {
            if (_str[i] == old_char) {
                _str[i] = new_char;
            }
        }
    }

    /**
     * @brief Queries whether this string begins with the given prefix.
     * @param str The prefix to check.
     * @return True if this string begins with the given prefix, otherwise false.
     */
    inline bool begins_with(const char* str) const
    {
        bool result = false;
        uint32_t len = (uint32_t)::strlen(str);
        if (len <= _size) {
            result = (uint32_t)::strncmp(_str, str, len) == 0;
        }
        return result;
    }

    /**
     * @brief Replaces this string with a substring of itself (in-place operation).
     * @param[opt] start_pos The zero-based start position for forming the substring.
     * @param[opt] length The substring length.
     */
    void substr_inplace(uint32_t start_pos = 0, uint32_t length = npos)
    {
        if (_str) {                                      // substring of empty string has no effect
            if ((start_pos != 0) || (length != npos)) {  // trivial substring has no effect
                if (length == npos) {
                    length = _size - start_pos;  // take as much as there is up to the end
                }
                errno_t erc = memmove_s(_str, _capacity, _str + start_pos, length);
                securec_check(erc, "\0", "\0");
                _size = length;
                _str[_size] = 0;
            }
        }
    }

    /**
     * @brief Subscript operator.
     * @param pos The zero-based character position.
     * @return The character or zero if index is out of range.
     */
    inline char operator[](uint32_t pos) const
    {
        return at(pos);
    }

    /**
     * @brief Equals operator.
     * @param other The other string with which the comparison is to be made.
     * @return True if the two strings are equal, otherwise false. Null strings are not equal.
     */
    inline bool operator==(const mot_basic_string& other) const
    {
        return (_size == other._size) && (::strcmp(_str, other._str) == 0);
    }

    // disable copy operator as it might fail, and we wish to avoid misuse
    mot_basic_string& operator=(const mot_basic_string& other) = delete;

    /** @brief Move operator. */
    mot_basic_string& operator=(mot_basic_string&& other)
    {
        if (this != &other) {
            this->move(std::move(other));
        }
        return *this;
    }

private:
    /**
     * @brief Helper function for selecting a new capacity based a given capacity demand.
     * @param new_capacity The minimum required new capacity.
     * @return The selected capacity or @ref INVALID_CAPACITY if failed.
     */
    uint32_t select_capacity(uint32_t new_capacity)
    {
        uint32_t selectedCapacity = INVALID_CAPACITY;
        if (_capacity > _size_limit) {
            mot_infra_report_error(MOT_ERROR_RESOURCE_LIMIT,
                "",
                "Cannot increase configuration string capacity to %u bytes: reached size limit %u",
                new_capacity,
                _size_limit);
        } else {
            // align and check with limit
            uint32_t alignedCapacity = (new_capacity + ALIGNMENT - 1) / ALIGNMENT * ALIGNMENT;
            if (alignedCapacity > _size_limit) {
                mot_infra_report_error(MOT_ERROR_RESOURCE_LIMIT,
                    "",
                    "Cannot increase configuration string capacity to %u bytes: aligned size %u reached size limit %u",
                    new_capacity,
                    alignedCapacity,
                    _size_limit);
            } else {
                selectedCapacity = alignedCapacity;
            }
        }
        return selectedCapacity;
    }

    /**
     * @brief Reallocates the string buffer according to new capacity specification.
     * @param new_capacity The new required capacity.
     * @return True if reallocation succeeded, otherwise false.
     * @note In case of failure to reallocate, the object state is left intact.
     */
    bool realloc(uint32_t newCapacity)
    {
        bool result = false;
        uint32_t selectedCapacity = select_capacity(newCapacity);
        if ((selectedCapacity != INVALID_CAPACITY) && (selectedCapacity > _capacity)) {
            void* newPtr = nullptr;
            if (_str) {
                newPtr = Allocator::realloc(_str, _capacity, selectedCapacity);
            } else {
                newPtr = Allocator::allocate(selectedCapacity);
            }
            if (newPtr != NULL) {
                _str = (char*)newPtr;
                _capacity = selectedCapacity;
                result = true;
            } else {
                // if ::relloac() failed _str is left intact (so object state remains valid)
                mot_infra_report_error(MOT_ERROR_OOM,
                    "",
                    "Failed to allocate %u bytes for string while attempting to grow to capacity of %u characters",
                    selectedCapacity,
                    newCapacity);
            }
        }
        return result;
    }

    /** @brief Helper method for counting the number of leading whitespace characters. */
    inline uint32_t count_lead_ws()
    {
        uint32_t count = 0;
        while ((count < _size) && isspace(_str[count])) {
            ++count;
        }
        return count;
    }

    /** @brief Helper method for counting the number of trailing whitespace characters. */
    inline uint32_t count_trail_ws()
    {
        uint32_t count = 0;
        int index = (int)_size - 1;
        while ((index >= 0) && isspace(_str[index])) {
            ++count;
            --index;
        }
        return count;
    }

    /** @var The Actual string. */
    char* _str;

    /** @var The length of the string (not including terminating null) */
    uint32_t _size;

    /** @var The current capacity of the string. */
    uint32_t _capacity;

    /** @var The size limit above which the string cannot grow. */
    uint32_t _size_limit;

    /** @var Initial capacity. */
    static constexpr uint32_t INIT_CAPACITY = 0;

    /** @var String size increases by multiples of 64 bytes each time it needs to grow. */
    static constexpr uint32_t ALIGNMENT = 64;

    /** @var The default size limit if none was specified. */
    static constexpr uint32_t DEFAULT_SIZE_LIMIT = 4096;

    /** @var Constant denoting invalid capacity. */
    static constexpr uint32_t INVALID_CAPACITY = (uint32_t)-1;
};

// define a few commonly used types
/** @typedef mot_string A character string that uses the default system allocator. */
typedef mot_basic_string<mot_default_allocator> mot_string;

/** @typedef mot_string_assigner Assigner for string objects. */
typedef mot_assigner<mot_string> mot_string_assigner;

/** @class Specialization for easily using mot_string in mot containers.   */
template <>
class mot_helper<mot_string> {
public:
    typedef mot_string_assigner assigner;
};
}  // namespace MOT

#endif /* MOT_STRING_H */
