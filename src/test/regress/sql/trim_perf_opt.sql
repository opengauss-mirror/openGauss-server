-- =============================================================================
-- SECTION 1: Basic trim function tests (single_blank = false, custom set)
-- Covers: btrim, ltrim, rtrim with custom character set
-- Template: dotrim<true,true,false>, dotrim<true,false,false>, dotrim<false,true,false>
-- =============================================================================

-- Test 1.1: btrim with custom set, ASCII string (utf8_ascii = true path)
SELECT btrim('zzzytrim', 'xyz');

-- Test 1.2: ltrim with custom set, ASCII string
SELECT ltrim('zzzytrim', 'xyz');

-- Test 1.3: rtrim with custom set, ASCII string
SELECT rtrim('hello...', '.');

-- Test 1.6: ltrim with all chars in set (result empty)
SELECT ltrim('abc', 'abc');

-- Test 1.7: rtrim with all chars in set (result empty)
SELECT rtrim('abc', 'abc');

-- Test 1.8: btrim with all chars in set (result empty)
SELECT btrim('abc', 'abc');

-- Test 1.9: String with no trimmable chars
SELECT btrim('hello', 'xyz');

-- Test 1.12: btrim with single char set (not space)
SELECT btrim('...hello...', '.');

-- =============================================================================
-- SECTION 2: single_blank = true tests (btrim, ltrim, rtrim)
-- Covers: dotrim<true,true,true>, dotrim<true,false,true>, dotrim<false,true,true>
-- =============================================================================

-- Test 2.1: btrim - both sides spaces (single_blank=true, both trim)
SELECT btrim('  hello  ');

-- Test 2.2: btrim - only leading spaces
SELECT btrim('  hello');

-- Test 2.3: btrim - only trailing spaces
SELECT btrim('hello  ');

-- Test 2.4: btrim - no spaces
SELECT btrim('hello');

-- Test 2.5: btrim - all spaces (result empty)
SELECT btrim('     ');

-- Test 2.6: btrim - empty string
SELECT btrim('');

-- Test 2.7: ltrim - leading spaces
SELECT ltrim('  hello');

-- Test 2.8: ltrim - no leading spaces
SELECT ltrim('hello  ');

-- Test 2.9: ltrim - all spaces (result empty)
SELECT ltrim('     ');

-- Test 2.10: ltrim - empty string
SELECT ltrim('');

-- Test 2.11: rtrim - trailing spaces
SELECT rtrim('hello  ');

-- Test 2.12: rtrim - no trailing spaces
SELECT rtrim('  hello');

-- Test 2.13: rtrim - all spaces (result empty)
SELECT rtrim('     ');

-- Test 2.14: rtrim - empty string
SELECT rtrim('');

-- Test 2.15: btrim - single space
SELECT btrim(' ');

-- Test 2.16: ltrim - single space
SELECT ltrim(' ');

-- Test 2.17: rtrim - single space
SELECT rtrim(' ');

-- =============================================================================
-- SECTION 3: Long ASCII strings (NEON path coverage on aarch64)
-- utf8_srting_is_ascii with stringlen > 8 bytes
-- =============================================================================

-- Test 3.1: btrim with long ASCII string (> 8 bytes) - custom set
SELECT btrim('aaaaaaaaaaHello Worldaaaaaaaaaa', 'a');

-- Test 3.2: ltrim with long ASCII string
SELECT ltrim('aaaaaaaaaaHello World', 'a');

-- Test 3.3: rtrim with long ASCII string
SELECT rtrim('Hello Worldaaaaaaaaaa', 'a');

-- Test 3.4: btrim with long ASCII string (> 8 bytes spaces)
SELECT btrim('          Hello World          ');

-- Test 3.5: ltrim with long ASCII string
SELECT ltrim('          Hello World');

-- Test 3.6: rtrim with long ASCII string
SELECT rtrim('Hello World          ');

-- Test 3.7: Long string where first 8 bytes are ASCII,
-- but contains non-ASCII after position 8 (exercises NEON + scalar fallback)
-- We'll test this in Section 5 with UTF8 multibyte characters

-- =============================================================================
-- SECTION 4: Single-byte encoding path (utf8_ascii = true)
-- These tests exercise the optimization: UTF8 + all-ASCII => skip multibyte path
-- =============================================================================

-- Test 4.1: btrim with space as set (single_blank=false, utf8_ascii=true)
SELECT btrim('  hello  ', ' ');

-- Test 4.2: btrim with set containing multiple chars including space
SELECT btrim('  xyz  hello  xyz  ', ' xyz');

-- Test 4.3: ltrim with space set
SELECT ltrim('  hello', ' ');

-- Test 4.4: rtrim with space set
SELECT rtrim('hello  ', ' ');

-- Test 4.5: btrim with set containing non-space chars (multiple)
SELECT btrim('xxxyyyhellozyyyxxx', 'xyz');

-- Test 4.6: ltrim with multi-char set
SELECT ltrim('xxxyyyhello', 'xyz');

-- Test 4.7: rtrim with multi-char set
SELECT rtrim('helloxxxyyy', 'xyz');

-- =============================================================================
-- SECTION 5: UTF8 multibyte character tests (utf8_ascii = false)
-- These tests exercise the multibyte path: non-ASCII chars in UTF8 string
-- =============================================================================

-- Test 5.1: btrim with Chinese characters in set and string
SELECT btrim('你好hello世界', '你好世界');

-- Test 5.2: ltrim with Chinese characters
SELECT ltrim('你好hello', '你好');

-- Test 5.3: rtrim with Chinese characters
SELECT rtrim('hello世界', '世界');

-- Test 5.4: btrim with mixed ASCII + Chinese, trim spaces
-- utf8_ascii=false (Chinese chars present) → enters multibyte branch
SELECT btrim('  你好  ', ' ');

-- Test 5.5: btrim with Chinese string
-- single_blank=true, but utf8_ascii=false → enters multibyte branch (single_blank not used)
SELECT btrim('  你好  ');

-- Test 5.6: ltrim with Chinese string
SELECT ltrim('  你好');

-- Test 5.7: rtrim with Chinese string
SELECT rtrim('你好  ');

-- Test 5.8: String with non-ASCII at position > 8 (NEON processes first 8, scalar catches rest)
-- First 8 chars are ASCII, then Chinese chars follow
SELECT btrim('aaaaaaaa你好', 'a');

-- Test 5.9: Long string with non-ASCII after byte 8
SELECT ltrim('aaaaaaaa你好世界', 'a');

-- Test 5.10: btrim where the set contains multibyte chars only
SELECT btrim('你好hello你好', '你好');

-- Test 5.11: rtrim where the set contains multibyte chars only
SELECT rtrim('hello你好世界', '你好世界');

-- Test 5.12: ltrim where all chars are multibyte
SELECT ltrim('你好你好hello', '你好');

-- =============================================================================
-- SECTION 6: Edge cases - strings and sets of various lengths
-- =============================================================================

-- Test 6.1: Set longer than string
SELECT btrim('abc', 'abcdef');

-- Test 6.2: String with single character, matches set
SELECT btrim('a', 'a');

-- Test 6.3: String with single character, doesn't match set
SELECT btrim('a', 'b');

-- Test 6.4: Set with overlapping characters
SELECT btrim('aaabbbccc', 'abc');

-- Test 6.5: btrim with set of 2 characters
SELECT btrim('abababhello', 'ab');

-- Test 6.6: ltrim with whitespace-only string of different lengths
SELECT ltrim(' ');
SELECT ltrim('  ');
SELECT ltrim('   ');

-- Test 6.7: rtrim with whitespace-only string of different lengths
SELECT rtrim(' ');
SELECT rtrim('  ');
SELECT rtrim('   ');

-- Test 6.8: btrim with tab characters (single_blank only matches space, not tab)
SELECT btrim(E'\thello\t');

-- =============================================================================
-- SECTION 7: A_FORMAT compatibility tests
-- Tests the res_len == 0 path with A_FORMAT + ACCEPT_EMPTY_STR
-- =============================================================================

-- Test 7.1: A_FORMAT mode - btrim returns NULL when result is empty
SET sql_compatibility = 'A';
SELECT btrim('abc', 'abc') IS NULL;

-- Test 7.2: A_FORMAT mode - ltrim returns NULL when result is empty
SELECT ltrim('abc', 'abc') IS NULL;

-- Test 7.3: A_FORMAT mode - rtrim returns NULL when result is empty
SELECT rtrim('abc', 'abc') IS NULL;

-- Test 7.4: A_FORMAT mode - btrim returns NULL when result is empty
SELECT btrim('     ') IS NULL;

-- Test 7.5: A_FORMAT mode - ltrim returns NULL when result is empty
SELECT ltrim('     ') IS NULL;

-- Test 7.6: A_FORMAT mode - rtrim returns NULL when result is empty
SELECT rtrim('     ') IS NULL;

-- Test 7.7: A_FORMAT mode - btrim empty string returns NULL
SELECT btrim('', 'x') IS NULL;

-- Test 7.8: A_FORMAT mode - btrim empty string returns NULL
SELECT btrim('') IS NULL;

-- Test 7.9: A_FORMAT mode - normal result (not empty) works fine
SELECT btrim('  hello  ', ' ');

-- Test 7.10: A_FORMAT mode - btrim normal result
SELECT btrim('  hello  ');

-- Test 7.11: A_FORMAT mode - with Chinese chars, multibyte path
SELECT btrim('你好', '你好') IS NULL;

RESET sql_compatibility;

-- =============================================================================
-- SECTION 8: Mixed scenarios - comprehensive coverage
-- =============================================================================

-- Test 8.1: UTF8 multibyte path with both trim sides and custom set
SELECT btrim('你好abc你好', '你好');

-- Test 8.2: ASCII path (utf8_ascii=true) with both sides, custom set
SELECT btrim('xxhellooxx', 'xo');

-- Test 8.3: ASCII path where stringlen = 0 after trim (exact match)
SELECT btrim('abc', 'abc') = '';

-- Test 8.4: btrim with multi-byte set where only some chars match
SELECT btrim('abc你好def', '你好');

-- Test 8.5: ltrim with all spaces (single_blank=true, result empty)
SELECT ltrim('     ') IS NULL;

-- Test 8.6: rtrim with all spaces (single_blank=true, result empty)
SELECT rtrim('     ') IS NULL;

-- Test 8.7: Verify empty string return (not null) in default compatibility mode
SELECT btrim('abc', 'abc') IS NULL;

-- Test 8.8: Verify ltrim empty return in default mode
SELECT ltrim('abc', 'abc') IS NULL;

-- Test 8.9: Verify rtrim empty return in default mode
SELECT rtrim('abc', 'abc') IS NULL;

-- Test 8.10: btrim with tab + space mix (single_blank only strips spaces)
SELECT btrim(E' \t hello \t ');

-- Test 8.11: ltrim with tab + space mix
SELECT ltrim(E' \t hello');

-- Test 8.12: rtrim with tab + space mix
SELECT rtrim(E'hello \t ');

-- =============================================================================
-- SECTION 9: UTF8 multibyte - non-ASCII in both string and set
-- =============================================================================

-- Test 9.1: Both string and set contain multibyte chars
SELECT btrim('你好世界abc世界你好', '你好世界');

-- Test 9.2: ltrim with only multibyte chars
SELECT ltrim('你好你好你好', '你好');

-- Test 9.3: rtrim with only multibyte chars
SELECT rtrim('你好你好你好', '你好');

-- Test 9.4: btrim with multibyte chars, result is empty
SELECT btrim('你好你好', '你好');

-- Test 9.5: Long multibyte string (>8 bytes total)
SELECT btrim('你好你好你好hello你好你好', '你好');

-- =============================================================================
-- SECTION 10: Additional edge cases for coverage completeness
-- =============================================================================
-- Test 10.2: Verify ltrim behavior with same string and set
SELECT ltrim('aaaaaaaaaa', 'a');

-- Test 10.3: Verify rtrim behavior with same string and set
SELECT rtrim('aaaaaaaaaa', 'a');

-- Test 10.4: btrim with very long string (tests long single_blank path)
SELECT btrim('          a         b         c          ');

-- Test 10.5: btrim with space set (utf8_ascii=true, single_blank=false)
SELECT btrim('          a         b         c          ', ' ');

-- Test 10.6: ltrim with space set, long string
SELECT ltrim('                    hello', ' ');

-- Test 10.7: rtrim with space set, long string
SELECT rtrim('hello                    ', ' ');

-- Test 10.8: btrim with set of one multi-byte char
SELECT btrim('éééhelloééé', 'é');

-- Test 10.9: ltrim with set of one multi-byte char
SELECT ltrim('éééhello', 'é');

-- Test 10.10: rtrim with set of one multi-byte char
SELECT rtrim('helloééé', 'é');

-- =============================================================================
-- SECTION 11: Multi-byte string with > 8 leading/trailing characters
-- These tests exercise the multibyte encoding path with many iterations
-- utf8_ascii=false (non-ASCII chars present) → multibyte branch
-- =============================================================================

-- Test 11.1: btrim with multi-byte string, >8 leading and trailing spaces (custom set)
-- 10 spaces each side, utf8_ascii=false → multibyte branch, many iterations
SELECT btrim('          你好世界          ', ' ');

-- Test 11.2: ltrim with multi-byte string, >8 leading spaces
SELECT ltrim('          你好世界', ' ');

-- Test 11.3: rtrim with multi-byte string, >8 trailing spaces
SELECT rtrim('你好世界          ', ' ');

-- Test 11.4: btrim with multi-byte string, >8 spaces both sides
-- single_blank=true but utf8_ascii=false → multibyte branch (single_blank ignored)
SELECT btrim('          你好世界          ');

-- Test 11.5: ltrim with multi-byte string, >8 leading spaces
SELECT ltrim('          你好世界');

-- Test 11.6: rtrim with multi-byte string, >8 trailing spaces
SELECT rtrim('你好世界          ');

-- Test 11.7: btrim with multi-byte set, >8 multi-byte chars both sides
-- 4 repetitions of '你好' (6 bytes each = 24 bytes each side)
SELECT btrim('你好你好你好你好hello你好你好你好你好', '你好');

-- Test 11.8: ltrim with multi-byte set, >8 multi-byte chars leading
SELECT ltrim('你好你好你好你好你好hello', '你好');

-- Test 11.9: rtrim with multi-byte set, >8 multi-byte chars trailing
SELECT rtrim('hello你好你好你好你好你好', '你好');

-- Test 11.10: btrim with multi-byte set, all chars trimmed (>8 total)
SELECT btrim('你好你好你好你好', '你好');

-- Test 11.11: ltrim with multi-byte set, all chars trimmed
SELECT ltrim('你好你好你好你好', '你好');

-- Test 11.12: rtrim with multi-byte set, all chars trimmed
SELECT rtrim('你好你好你好你好', '你好');

-- Test 11.13: btrim with multi-byte string, >8 leading spaces only
SELECT btrim('          你好', ' ');

-- Test 11.14: btrim with multi-byte string, >8 trailing spaces only
SELECT btrim('你好          ', ' ');

-- Test 11.15: btrim with multi-byte string, >8 spaces only on one side
SELECT btrim('          你好');

-- Test 11.16: btrim with multi-byte string, >8 spaces trailing only
SELECT btrim('你好          ');

-- Test 11.17: Mixed multibyte + ASCII string, >8 leading spaces
SELECT btrim('          你好hello世界', ' ');

-- Test 11.18: btrim with multi-byte string, >8 spaces + multibyte set combined
-- Spaces first trimmed via multibyte branch, then multibyte set chars
SELECT btrim('          你好          hello          你好          ', '你好 ');

SELECT btrim('                    hello', '你好');

-- =============================================================================
-- SECTION 12: A_FORMAT + multi-byte + many characters
-- =============================================================================

-- Test 12.1: A_FORMAT - btrim with multibyte set, all chars trimmed returns NULL
SELECT btrim('你好你好你好你好', '你好') IS NULL;

-- Test 12.2: A_FORMAT - ltrim with multibyte set, all chars trimmed returns NULL
SELECT ltrim('你好你好你好你好', '你好') IS NULL;

-- Test 12.3: A_FORMAT - rtrim with multibyte set, all chars trimmed returns NULL
SELECT rtrim('你好你好你好你好', '你好') IS NULL;

-- Test 12.4: A_FORMAT - btrim with multibyte+spaces, set trims everything returns NULL
-- set contains both space and multibyte chars, utf8_ascii=false, >8 spaces
SELECT btrim('          你好世界          ', ' 你好世界') IS NULL;

-- Test 12.5: A_FORMAT - ltrim with multibyte+spaces, set trims everything returns NULL
SELECT ltrim('          你好世界', ' 你好世界') IS NULL;

-- Test 12.6: A_FORMAT - rtrim with multibyte+spaces, set trims everything returns NULL
SELECT rtrim('你好世界          ', ' 你好世界') IS NULL;

-- Test 12.7: A_FORMAT - btrim with multibyte string, many spaces, result non-empty
SELECT btrim('          你好世界          ', ' ');

-- Test 12.8: A_FORMAT - btrim with multibyte string, many spaces, result non-empty
SELECT btrim('          你好世界          ');
