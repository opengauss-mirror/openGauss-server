/*
 * Copyright (c) 2026 Huawei Technologies Co., Ltd.
 * openGauss-server is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 *
 * ---------------------------------------------------------------------------------------
 *
 * ogai_text_splitter.cpp
 *
 * IDENTIFICATION
 *        src/gausskernel/storage/access/datavec/ogai_text_splitter.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include "postgres.h"

#include <cstring>

#include "utils/elog.h"
#include "access/datavec/ogai_text_splitter.h"

namespace {

const size_t MAX_DOCUMENT_BYTES = 50UL * 1024 * 1024;
static const int ARRAY_DEFAULT_CAPACITY = 64;
static const int MAX_SEMANTIC_LEVEL = 64;
static const int INITIAL_SECTIONS_CAPACITY = 128;
static const int ASCII_CODEPOINT_LIMIT = 128;
static const int ARRAY_GROW_FACTOR = 2;
static const int UTF8_2BYTE_LEN = 2;
static const int UTF8_3BYTE_LEN = 3;
static const int UTF8_4BYTE_LEN = 4;
static const int UTF8_SHIFT_6   = 6;
static const int UTF8_SHIFT_12  = 12;
static const int UTF8_SHIFT_18  = 18;
static const int CRLF_BYTE_LEN = 2;
static const int CHAR_PREFIX_SUM_MAX_BYTES = 10 * 1024 * 1024;
static const int FALLBACK_SCAN_MARGIN = 256;

static inline bool IsAsciiWordChar(int codepoint)
{
    return (codepoint >= 'a' && codepoint <= 'z') || (codepoint >= 'A' && codepoint <= 'Z') ||
           (codepoint >= '0' && codepoint <= '9') || codepoint == '_';
}

static inline bool IsUnicodeWhitespace(int codepoint)
{
    return codepoint == 0x20 || codepoint == 0x09 || codepoint == 0x0A || codepoint == 0x0D ||
           codepoint == 0x0B || codepoint == 0x0C || codepoint == 0x3000;
}

static inline bool IsClosingQuoteOrBracket(int codepoint)
{
    return codepoint == '"' || codepoint == '\'' || codepoint == ')' || codepoint == ']' || codepoint == '}' ||
           codepoint == 0x2019 || codepoint == 0x201D || codepoint == 0x300D || codepoint == 0x300F ||
           codepoint == 0x3011 || codepoint == 0xFF09;
}

static bool DecodeUtf8CodePoint(const char* text, size_t textLen, size_t pos, int* codepoint, size_t* bytes)
{
    const unsigned char* data = reinterpret_cast<const unsigned char*>(text);
    unsigned char c0 = data[pos];

    if (c0 < 0x80) {
        *codepoint = c0;
        *bytes = 1;
        return true;
    }

    if ((c0 & 0xE0) == 0xC0 && pos + 1 < textLen) {
        unsigned char c1 = data[pos + 1];
        if ((c1 & 0xC0) == 0x80) {
            *codepoint = ((c0 & 0x1F) << UTF8_SHIFT_6) | (c1 & 0x3F);
            *bytes = UTF8_2BYTE_LEN;
            return true;
        }
    }

    if ((c0 & 0xF0) == 0xE0 && pos + UTF8_3BYTE_LEN - 1 < textLen) {
        unsigned char c1 = data[pos + 1];
        unsigned char c2 = data[pos + 2];
        if ((c1 & 0xC0) == 0x80 && (c2 & 0xC0) == 0x80) {
            *codepoint = ((c0 & 0x0F) << UTF8_SHIFT_12) | ((c1 & 0x3F) << UTF8_SHIFT_6) | (c2 & 0x3F);
            *bytes = UTF8_3BYTE_LEN;
            return true;
        }
    }

    if ((c0 & 0xF8) == 0xF0 && pos + UTF8_4BYTE_LEN - 1 < textLen) {
        unsigned char c1 = data[pos + 1];
        unsigned char c2 = data[pos + 2];
        unsigned char c3 = data[pos + 3];
        if ((c1 & 0xC0) == 0x80 && (c2 & 0xC0) == 0x80 && (c3 & 0xC0) == 0x80) {
            *codepoint = ((c0 & 0x07) << UTF8_SHIFT_18) | ((c1 & 0x3F) << UTF8_SHIFT_12) |
                    ((c2 & 0x3F) << UTF8_SHIFT_6) | (c3 & 0x3F);
            *bytes = UTF8_4BYTE_LEN;
            return true;
        }
    }

    *codepoint = c0;
    *bytes = 1;
    return false;
}

/* Precompute byte-to-char prefix sum for O(1) char counting.
 * Returns NULL for docs > CHAR_PREFIX_SUM_MAX_BYTES;
 * callers fall back to linear scanning. */
static int* BuildCharPrefixSum(const char* text, int textLen)
{
    if (textLen > CHAR_PREFIX_SUM_MAX_BYTES) {
        return NULL;
    }

    int* sum = (int*)palloc(sizeof(int) * (textLen + 1));
    sum[0] = 0;
    size_t pos = 0;
    int count = 0;
    while (pos < (size_t)textLen) {
        int cp = 0;
        size_t bytes = 0;
        DecodeUtf8CodePoint(text, (size_t)textLen, pos, &cp, &bytes);
        for (size_t b = 1; b < bytes; b++) {
            if (pos + b <= (size_t)textLen) {
                sum[pos + b] = count;
            }
        }
        count++;
        if (pos + bytes <= (size_t)textLen) {
            sum[pos + bytes] = count;
        }
        pos += bytes;
    }
    return sum;
}

typedef struct Section {
    int byteOffset;
    int byteLen;
} Section;

typedef struct SectionArray {
    Section* items;
    int count;
    int capacity;
} SectionArray;

typedef struct ChunkRange {
    int startByte;
    int endByte;
} ChunkRange;

typedef struct SplitContext {
    int cursor;
    int maxChunkChars;
} SplitContext;

typedef struct TextContext {
    const char* text;
    int textLen;
    const int* charSum;
} TextContext;

typedef struct ChunkArray {
    TextSplitChunk* items;
    int count;
    int capacity;
} ChunkArray;

static void InitSectionArray(SectionArray* sa, int initialCap)
{
    sa->capacity = initialCap > 0 ? initialCap : ARRAY_DEFAULT_CAPACITY;
    sa->items = (Section*)palloc(sizeof(Section) * sa->capacity);
    sa->count = 0;
}

static void SectionArrayAdd(SectionArray* sa, int byteOffset, int byteLen)
{
    if (sa->count >= sa->capacity) {
        sa->capacity *= ARRAY_GROW_FACTOR;
        sa->items = (Section*)repalloc(sa->items, sizeof(Section) * sa->capacity);
    }
    sa->items[sa->count].byteOffset = byteOffset;
    sa->items[sa->count].byteLen = byteLen;
    sa->count++;
}

static void FreeSectionArray(SectionArray* sa)
{
    if (sa->items != NULL) {
        pfree(sa->items);
        sa->items = NULL;
    }
    sa->count = 0;
    sa->capacity = 0;
}

static int CountCharsInRange(const char* text, int textLen, int startByte, int endByte)
{
    int count = 0;
    size_t pos = (size_t)startByte;
    size_t limit = (size_t)(endByte < textLen ? endByte : textLen);
    while (pos < limit) {
        int cp = 0;
        size_t bytes = 0;
        DecodeUtf8CodePoint(text, (size_t)textLen, pos, &cp, &bytes);
        count++;
        pos += bytes;
    }
    return count;
}

static int TrimStartBytes(const char* text, int textLen, int start, int end)
{
    int pos = start;
    while (pos < end) {
        int cp = 0;
        size_t bytes = 0;
        DecodeUtf8CodePoint(text, (size_t)textLen, (size_t)pos, &cp, &bytes);
        if (!IsUnicodeWhitespace(cp) && cp != '\n' && cp != '\r') {
            break;
        }
        pos += (int)bytes;
    }
    return pos;
}

static int TrimEndBytes(const char* text, int textLen, int start, int end)
{
    const unsigned char* data = reinterpret_cast<const unsigned char*>(text);
    int pos = end;
    while (pos > start) {
        int prevStart = pos - 1;
        while (prevStart > start && (data[prevStart] & 0xC0) == 0x80) {
            prevStart--;
        }
        int cp = 0;
        size_t bytes = 0;
        DecodeUtf8CodePoint(text, (size_t)textLen, (size_t)prevStart, &cp, &bytes);
        if (!IsUnicodeWhitespace(cp) && cp != '\n' && cp != '\r') {
            return prevStart + (int)bytes;
        }
        pos = prevStart;
    }
    return start;
}

static int CountCharsTrimmed(const TextContext* tctx, int startByte, int endByte)
{
    int ts = TrimStartBytes(tctx->text, tctx->textLen, startByte, endByte);
    int te = TrimEndBytes(tctx->text, tctx->textLen, ts, endByte);
    if (te <= ts) {
        return 0;
    }
    if (tctx->charSum != NULL) {
        return tctx->charSum[te] - tctx->charSum[ts];
    }
    return CountCharsInRange(tctx->text, tctx->textLen, ts, te);
}

static void ParseTextSemanticRuns(const char* text, int textLen, SemanticRunArray* runs)
{
    InitSemanticRunArray(runs, ARRAY_DEFAULT_CAPACITY);
    int pos = 0;
    while (pos < textLen) {
        char ch = text[pos];
        if (ch != '\n' && ch != '\r') {
            pos++;
            continue;
        }

        int runStart = pos;
        int newlineCount = 0;
        while (pos < textLen && (text[pos] == '\n' || text[pos] == '\r')) {
            if (text[pos] == '\r' && pos + 1 < textLen && text[pos + 1] == '\n') {
                newlineCount++;
                pos += CRLF_BYTE_LEN;
            } else {
                newlineCount++;
                pos++;
            }
        }

        AddSemanticRun(runs, newlineCount, runStart, pos);
    }
}

static void ParseSemanticRuns(const char* text, int textLen,
                              TextSplitParserKind kind, SemanticRunArray* runs)
{
    (void)kind;  /* Reserved for future Markdown/Code parsers */
    ParseTextSemanticRuns(text, textLen, runs);
}

static int FindFirstRelevantRun(const SemanticRunArray* runs, int cursorByte)
{
    int low = 0;
    int high = runs->count;
    while (low < high) {
        int mid = low + (high - low) / 2;
        if (runs->items[mid].endByte <= cursorByte) {
            low = mid + 1;
        } else {
            high = mid;
        }
    }
    return low;
}

static void GetDistinctLevels(const SemanticRunArray* runs, int cursorByte,
                               int* levels, int* levelCount, int maxLevels)
{
    bool seen[MAX_SEMANTIC_LEVEL];
    errno_t rc = memset_s(seen, sizeof(seen), 0, sizeof(seen));
    securec_check(rc, "\0", "\0");
    *levelCount = 0;
    int startIdx = FindFirstRelevantRun(runs, cursorByte);
    for (int i = startIdx; i < runs->count; i++) {
        if (runs->items[i].startByte < cursorByte) {
            continue;
        }
        int lv = runs->items[i].level;
        if (lv > 0 && lv < MAX_SEMANTIC_LEVEL && !seen[lv]) {
            seen[lv] = true;
        }
    }

    for (int lv = 1; lv < MAX_SEMANTIC_LEVEL && *levelCount < maxLevels; lv++) {
        if (seen[lv]) {
            levels[(*levelCount)++] = lv;
        }
    }
}

static void SplitBySemantic(const SemanticRunArray* runs, int targetLevel,
    int cursorByte, int scanEndByte, SectionArray* sections)
{
    sections->count = 0;
    int pos = cursorByte;
    int startIdx = FindFirstRelevantRun(runs, cursorByte);
    for (int i = startIdx; i < runs->count; i++) {
        if (runs->items[i].startByte >= scanEndByte) {
            break;
        }
        if (runs->items[i].level < targetLevel) {
            continue;
        }

        int sepStart = runs->items[i].startByte;
        if (sepStart < pos) {
            sepStart = pos;
        }

        if (sepStart > pos) {
            SectionArrayAdd(sections, pos, sepStart - pos);
        }
        SectionArrayAdd(sections, sepStart, runs->items[i].endByte - sepStart);
        pos = runs->items[i].endByte;
    }

    if (pos < scanEndByte) {
        SectionArrayAdd(sections, pos, scanEndByte - pos);
    }
}

static int FirstSectionEnd(const SemanticRunArray* runs, int targetLevel,
    int cursorByte, int textLen)
{
    int startIdx = FindFirstRelevantRun(runs, cursorByte);
    for (int i = startIdx; i < runs->count; i++) {
        if (runs->items[i].level < targetLevel) {
            continue;
        }
        int sepStart = runs->items[i].startByte;
        if (sepStart < cursorByte) {
            sepStart = cursorByte;
        }
        if (sepStart > cursorByte) {
            return sepStart;
        }
        return runs->items[i].endByte;
    }
    return textLen;
}

static size_t SkipClosingCharsAfter(const char* text, size_t textLen, size_t after)
{
    while (after < textLen) {
        int nextCp = 0;
        size_t nextBytes = 0;
        DecodeUtf8CodePoint(text, textLen, after, &nextCp, &nextBytes);
        if (!IsClosingQuoteOrBracket(nextCp)) {
            break;
        }
        after += nextBytes;
    }
    return after;
}

static bool IsAtWordBoundary(const char* text, size_t textLen, size_t after)
{
    if (after >= textLen) {
        return true;
    }
    int afterCp = 0;
    size_t afterBytes = 0;
    DecodeUtf8CodePoint(text, textLen, after, &afterCp, &afterBytes);
    return IsUnicodeWhitespace(afterCp);
}

static void TryAddBoundary(SectionArray* sections, int* pos, size_t boundary)
{
    int b = (int)boundary;
    if (b > *pos) {
        SectionArrayAdd(sections, *pos, b - *pos);
        *pos = b;
    }
}

static void SplitBySentence(const char* text, int textLen, int cursorByte, SectionArray* sections)
{
    sections->count = 0;
    int pos = cursorByte;
    size_t scan = (size_t)cursorByte;
    while (scan < (size_t)textLen) {
        int cp = 0;
        size_t bytes = 0;
        DecodeUtf8CodePoint(text, (size_t)textLen, scan, &cp, &bytes);
        if (cp == '.' || cp == '!' || cp == '?') {
            size_t after = SkipClosingCharsAfter(text, (size_t)textLen, scan + bytes);
            if (IsAtWordBoundary(text, (size_t)textLen, after)) {
                TryAddBoundary(sections, &pos, after);
            }
        } else if (cp == 0x3002 || cp == 0xFF1F || cp == 0xFF01) {
            size_t after = SkipClosingCharsAfter(text, (size_t)textLen, scan + bytes);
            TryAddBoundary(sections, &pos, after);
        }

        scan += bytes;
    }

    if (pos < textLen) {
        SectionArrayAdd(sections, pos, textLen - pos);
    }
}

static void SplitByClause(const char* text, int textLen, int cursorByte, SectionArray* sections)
{
    sections->count = 0;
    int pos = cursorByte;
    size_t scan = (size_t)cursorByte;
    while (scan < (size_t)textLen) {
        int cp = 0;
        size_t bytes = 0;
        DecodeUtf8CodePoint(text, (size_t)textLen, scan, &cp, &bytes);
        if (cp == 0xFF0C || cp == 0xFF1B || cp == 0xFF1A) {
            TryAddBoundary(sections, &pos, scan + bytes);
        } else if (cp == ',' || cp == ';' || cp == ':') {
            size_t after = scan + bytes;
            bool afterIsBoundary = (after >= (size_t)textLen);
            if (!afterIsBoundary) {
                int afterCp = 0;
                size_t afterBytes = 0;
                DecodeUtf8CodePoint(text, (size_t)textLen, after, &afterCp, &afterBytes);
                afterIsBoundary = IsUnicodeWhitespace(afterCp);
            }
            if (afterIsBoundary) {
                TryAddBoundary(sections, &pos, scan + bytes);
            }
        }
        scan += bytes;
    }

    if (pos < textLen) {
        SectionArrayAdd(sections, pos, textLen - pos);
    }
}

static size_t ScanAsciiWordEnd(const char* text, size_t textLen, size_t start)
{
    size_t wordEnd = start;
    while (wordEnd < textLen) {
        int wcp = 0;
        size_t wb = 0;
        DecodeUtf8CodePoint(text, textLen, wordEnd, &wcp, &wb);
        if (wcp < ASCII_CODEPOINT_LIMIT && IsAsciiWordChar(wcp)) {
            wordEnd += wb;
        } else {
            break;
        }
    }
    return wordEnd;
}

static void SplitByWord(const char* text, int textLen, int cursorByte, SectionArray* sections)
{
    sections->count = 0;
    int pos = cursorByte;
    size_t scan = (size_t)cursorByte;
    while (scan < (size_t)textLen) {
        int cp = 0;
        size_t bytes = 0;
        DecodeUtf8CodePoint(text, (size_t)textLen, scan, &cp, &bytes);
        if (cp < ASCII_CODEPOINT_LIMIT && IsAsciiWordChar(cp)) {
            size_t wordEnd = ScanAsciiWordEnd(text, (size_t)textLen, scan + bytes);
            SectionArrayAdd(sections, pos, (int)wordEnd - pos);
            pos = (int)wordEnd;
            scan = wordEnd;
        } else {
            int boundary = (int)(scan + bytes);
            SectionArrayAdd(sections, pos, boundary - pos);
            pos = boundary;
            scan += bytes;
        }
    }
}

static void SplitByChar(const char* text, int textLen, int cursorByte, SectionArray* sections)
{
    sections->count = 0;
    size_t pos = (size_t)cursorByte;
    while (pos < (size_t)textLen) {
        int cp = 0;
        size_t bytes = 0;
        DecodeUtf8CodePoint(text, (size_t)textLen, pos, &cp, &bytes);
        SectionArrayAdd(sections, (int)pos, (int)bytes);
        pos += bytes;
    }
}

typedef enum FallbackLevel {
    FALLBACK_CHAR = 0,
    FALLBACK_WORD,
    FALLBACK_CLAUSE,
    FALLBACK_SENTENCE,
    FALLBACK_COUNT
} FallbackLevel;

static void SplitByFallbackLevel(const char* text, int textLen,
                                  int cursorByte, FallbackLevel level, SectionArray* sections)
{
    switch (level) {
        case FALLBACK_SENTENCE:
            SplitBySentence(text, textLen, cursorByte, sections);
            break;
        case FALLBACK_CLAUSE:
            SplitByClause(text, textLen, cursorByte, sections);
            break;
        case FALLBACK_WORD:
            SplitByWord(text, textLen, cursorByte, sections);
            break;
        case FALLBACK_CHAR:
        default:
            SplitByChar(text, textLen, cursorByte, sections);
            break;
    }
}

static int BinarySearchChunkEnd(const TextContext* tctx, const SectionArray* sections,
    int cursorByte, int maxChunkChars)
{
    if (sections->count == 0) {
        return cursorByte;
    }

    int low = 0;
    int high = sections->count - 1;
    int bestEnd = cursorByte;
    bool foundAny = false;
    while (low <= high) {
        int mid = low + (high - low) / 2;
        int secEnd = sections->items[mid].byteOffset + sections->items[mid].byteLen;
        int trimmedChars = CountCharsTrimmed(tctx, cursorByte, secEnd);
        if (trimmedChars <= maxChunkChars) {
            bestEnd = secEnd;
            foundAny = true;
            low = mid + 1;
        } else {
            if (mid == 0 && !foundAny) {
                bestEnd = secEnd;
                foundAny = true;
            }
            high = mid - 1;
        }
    }

    if (foundAny && bestEnd > cursorByte) {
        bool pastBestEnd = false;
        for (int i = 0; i < sections->count; i++) {
            int se = sections->items[i].byteOffset + sections->items[i].byteLen;
            if (se == bestEnd) {
                pastBestEnd = true;
                continue;
            }
            if (!pastBestEnd) {
                continue;
            }
            int nextChars = CountCharsTrimmed(tctx, cursorByte, se);
            if (nextChars <= maxChunkChars) {
                bestEnd = se;
            } else {
                break;
            }
        }
    }

    return bestEnd;
}

static int ComputeOverlapCursor(const TextContext* tctx, const SectionArray* sections,
    ChunkRange range, int maxOverlapChars)
{
    if (maxOverlapChars <= 0 || sections->count == 0) {
        return range.endByte;
    }

    int endIdx = -1;
    for (int i = sections->count - 1; i >= 0; i--) {
        int se = sections->items[i].byteOffset + sections->items[i].byteLen;
        if (se <= range.endByte) {
            endIdx = i;
            break;
        }
    }
    if (endIdx < 0) {
        return range.endByte;
    }

    int bestStart = range.endByte;
    int low = 0;
    int high = endIdx;
    while (low <= high) {
        int mid = low + (high - low) / 2;
        int offset = sections->items[mid].byteOffset;
        int chars = CountCharsTrimmed(tctx, offset, range.endByte);
        if (chars <= maxOverlapChars && offset < bestStart && offset > range.startByte) {
            bestStart = offset;
        }

        if (chars < maxOverlapChars && mid > 0) {
            high = mid - 1;
        } else {
            low = mid + 1;
        }
    }

    return bestStart;
}

static void EmitChunk(ChunkArray* arr, const char* text, int ts, int te)
{
    if (arr->count >= arr->capacity) {
        arr->capacity *= ARRAY_GROW_FACTOR;
        arr->items = (TextSplitChunk*)repalloc(arr->items, sizeof(TextSplitChunk) * arr->capacity);
    }
    arr->items[arr->count].chunk = pnstrdup(text + ts, te - ts);
    arr->items[arr->count].chunkOffset = ts;
    arr->items[arr->count].chunkLength = te - ts;
    arr->count++;
}

static void EmitTrimmedChunk(const TextContext* tctx, ChunkArray* arr, int startByte, int endByte)
{
    int ts = TrimStartBytes(tctx->text, tctx->textLen, startByte, endByte);
    int te = TrimEndBytes(tctx->text, tctx->textLen, ts, endByte);
    if (te > ts) {
        EmitChunk(arr, tctx->text, ts, te);
    }
}

static int ChooseSemanticLevel(const TextContext* tctx, const SemanticRunArray* runs, int cursor, int maxChunkChars)
{
    int distinctLevels[MAX_SEMANTIC_LEVEL];
    int distinctCount = 0;
    GetDistinctLevels(runs, cursor, distinctLevels, &distinctCount, MAX_SEMANTIC_LEVEL);
    int chosenNlLevel = -1;
    for (int li = 0; li < distinctCount; li++) {
        int firstEnd = FirstSectionEnd(runs, distinctLevels[li], cursor, tctx->textLen);
        int firstChars = CountCharsTrimmed(tctx, cursor, firstEnd);
        if (firstChars > maxChunkChars) {
            break;
        }
        chosenNlLevel = distinctLevels[li];
    }

    return chosenNlLevel;
}

static int ComputeFallbackScanLimit(int cursor, int maxChunkChars, int textLen)
{
    int limit = cursor + maxChunkChars * UTF8_4BYTE_LEN + FALLBACK_SCAN_MARGIN;
    return limit < textLen ? limit : textLen;
}

static void SelectFallbackSections(const TextContext* tctx, int cursor, int maxChunkChars,
    SectionArray* sections)
{
    int scanLimit = ComputeFallbackScanLimit(cursor, maxChunkChars, tctx->textLen);
    FallbackLevel fbOrder[] = { FALLBACK_SENTENCE, FALLBACK_CLAUSE, FALLBACK_WORD, FALLBACK_CHAR };
    for (int fi = 0; fi < (int)FALLBACK_COUNT; fi++) {
        SplitByFallbackLevel(tctx->text, scanLimit, cursor, fbOrder[fi], sections);
        if (sections->count > 0) {
            int firstEnd = sections->items[0].byteOffset + sections->items[0].byteLen;
            int firstChars = CountCharsTrimmed(tctx, cursor, firstEnd);
            if (firstChars <= maxChunkChars) {
                return;
            }
        }
    }
}

static void SelectSections(const TextContext* tctx, const SemanticRunArray* runs,
    SplitContext ctx, SectionArray* sections)
{
    int chosenNlLevel = ChooseSemanticLevel(tctx, runs, ctx.cursor, ctx.maxChunkChars);
    if (chosenNlLevel >= 0) {
        SplitBySemantic(runs, chosenNlLevel, ctx.cursor, tctx->textLen, sections);
    } else {
        SelectFallbackSections(tctx, ctx.cursor, ctx.maxChunkChars, sections);
    }
}

static int AdvanceCursorWithOverlap(const TextContext* tctx, const SectionArray* sections,
    ChunkRange range, int maxOverlapChars)
{
    int nextCursor = ComputeOverlapCursor(tctx, sections, range, maxOverlapChars);
    if (nextCursor <= range.startByte) {
        int cp = 0;
        size_t bytes = 0;
        DecodeUtf8CodePoint(tctx->text, (size_t)tctx->textLen, (size_t)range.startByte, &cp, &bytes);
        nextCursor = range.startByte + (int)bytes;
    }
    return nextCursor;
}

static void BuildChunks(const char* text, int textLen, const TextSplitConfig* config, TextSplitResult* result)
{
    if (textLen <= 0) {
        result->chunks = NULL;
        result->chunkCount = 0;
        return;
    }

    int maxChunkChars = config->maxChunkSize;
    int maxOverlapChars = config->maxChunkOverlap;
    int* charSum = BuildCharPrefixSum(text, textLen);
    TextContext tctx = { text, textLen, charSum };
    SemanticRunArray runs;
    ParseSemanticRuns(text, textLen, config->parserKind, &runs);
    int estimatedChunks = maxChunkChars > 0 ? (textLen / maxChunkChars + 1) : ARRAY_DEFAULT_CAPACITY;
    if (estimatedChunks < ARRAY_DEFAULT_CAPACITY) {
        estimatedChunks = ARRAY_DEFAULT_CAPACITY;
    }

    ChunkArray chunkArr;
    chunkArr.capacity = estimatedChunks;
    chunkArr.items = (TextSplitChunk*)palloc(sizeof(TextSplitChunk) * chunkArr.capacity);
    chunkArr.count = 0;
    SectionArray sections;
    InitSectionArray(&sections, INITIAL_SECTIONS_CAPACITY);

    int cursor = 0;
    while (cursor < textLen) {
        int trimmedChars = CountCharsTrimmed(&tctx, cursor, textLen);
        if (trimmedChars <= maxChunkChars) {
            EmitTrimmedChunk(&tctx, &chunkArr, cursor, textLen);
            break;
        }

        SplitContext ctx = { cursor, maxChunkChars };
        SelectSections(&tctx, &runs, ctx, &sections);
        int chunkEnd = BinarySearchChunkEnd(&tctx, &sections, cursor, maxChunkChars);
        if (chunkEnd <= cursor) {
            chunkEnd = textLen;
        }

        EmitTrimmedChunk(&tctx, &chunkArr, cursor, chunkEnd);
        if (chunkEnd >= textLen) {
            break;
        }
        ChunkRange range = { cursor, chunkEnd };
        cursor = AdvanceCursorWithOverlap(&tctx, &sections, range, maxOverlapChars);
    }

    FreeSectionArray(&sections);
    FreeSemanticRunArray(&runs);
    if (charSum != NULL) {
        pfree(charSum);
    }
    result->chunks = chunkArr.items;
    result->chunkCount = chunkArr.count;
}

static void ValidateTextSplitConfig(const TextSplitConfig* config)
{
    if (config == NULL) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("text split config cannot be null")));
    }

    if (config->maxChunkSize <= 0) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("maxChunkSize must be greater than 0")));
    }

    if (config->maxChunkOverlap < 0 || config->maxChunkOverlap >= config->maxChunkSize) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            errmsg("maxChunkOverlap must be between 0 and maxChunkSize - 1")));
    }
}

}  // namespace

void InitSemanticRunArray(SemanticRunArray* runs, int capacity)
{
    runs->capacity = capacity > 0 ? capacity : ARRAY_DEFAULT_CAPACITY;
    runs->items = (SemanticRun*)palloc(sizeof(SemanticRun) * runs->capacity);
    runs->count = 0;
}

void AddSemanticRun(SemanticRunArray* runs, int level, int startByte, int endByte)
{
    if (runs->count >= runs->capacity) {
        runs->capacity *= ARRAY_GROW_FACTOR;
        runs->items = (SemanticRun*)repalloc(runs->items, sizeof(SemanticRun) * runs->capacity);
    }
    runs->items[runs->count].level = level;
    runs->items[runs->count].startByte = startByte;
    runs->items[runs->count].endByte = endByte;
    runs->count++;
}

void FreeSemanticRunArray(SemanticRunArray* runs)
{
    if (runs->items != NULL) {
        pfree(runs->items);
        runs->items = NULL;
    }
    runs->count = 0;
    runs->capacity = 0;
}

TextSplitConfig BuildDefaultTextSplitConfig(int maxChunkSize, int maxChunkOverlap)
{
    TextSplitConfig config;
    errno_t rc = memset_s(&config, sizeof(TextSplitConfig), 0, sizeof(TextSplitConfig));
    securec_check(rc, "\0", "\0");
    config.maxChunkSize = maxChunkSize;
    config.maxChunkOverlap = maxChunkOverlap;
    config.parserKind = TEXT_SPLIT_PARSER_TEXT;
    return config;
}

void SplitTextByConfig(const char* document, const TextSplitConfig* config, TextSplitResult* result)
{
    if (result == NULL) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("output result container cannot be null")));
    }

    result->chunks = NULL;
    result->chunkCount = 0;
    if (document == NULL || document[0] == '\0') {
        return;
    }

    ValidateTextSplitConfig(config);
    int docLen = (int)strlen(document);
    if ((size_t)docLen > MAX_DOCUMENT_BYTES) {
        ereport(ERROR, (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
            errmsg("document size exceeds maximum allowed %lu bytes", (unsigned long)MAX_DOCUMENT_BYTES)));
    }
    BuildChunks(document, docLen, config, result);
}
