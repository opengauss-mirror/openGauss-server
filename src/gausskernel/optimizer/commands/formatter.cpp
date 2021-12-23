/* -------------------------------------------------------------------------
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2021, openGauss Contributors
 *
 *
 * formatter.cpp
 *
 * IDENTIFICATION
 *	  src/gausskernel/optimizer/commands/formatter.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "c.h"
#include "commands/formatter.h"
#include "mb/pg_wchar.h"
#include "commands/copy.h"
#include "utils/rel.h"
#include "utils/rel_gs.h"

extern int namestrcmp(Name name, const char* str);

/*
 * This space trim operation at preasent is just deployed for FORMAT_FIXED parser.
 * Note: the space at the end of string just can be trimed.
 */
static char* StrnTrimTo(char* dst, uint32* dstlen, const char* src, uint32 srclen)
{
    const char* end = NULL;
    errno_t rc = 0;

    if (src == NULL || srclen == 0)
        return NULL;

    // Skip the space char at the end of sting
    //
    end = src + srclen - 1;

    do {
        if (isspace((int)*end))
            end--;
        else
            break;
    } while (end >= src);

    if (end < src)
        return NULL;

    *dstlen = end - src + 1;
    rc = memcpy_s(dst, *dstlen, src, *dstlen);
    securec_check(rc, "", "");
    return dst;
}

static void ReadAttributeSequence(CopyState cstate)
{
    StringInfo sequence_buf_ptr = &cstate->sequence_buf;
    resetStringInfo(sequence_buf_ptr);

    int numattrs = list_length(cstate->attnumlist);

    for (int i = 0; i < numattrs; i++) {
        if (cstate->sequence != NULL && cstate->sequence[i] != NULL) {
            cstate->raw_fields[i] = &sequence_buf_ptr->data[sequence_buf_ptr->len];
            appendStringInfo(sequence_buf_ptr, "%ld", cstate->sequence[i]->start);
            cstate->sequence[i]->start += cstate->sequence[i]->step;
            cstate->sequence_buf.len++;
        } else if (cstate->constant != NULL && cstate->constant[i] != NULL) {
            cstate->raw_fields[i] = cstate->constant[i]->consVal;
        }
    }
}

static int FixGetColumnListIndex(List *attnamelist, int attnum)
{
    ListCell *col = NULL;
    int index = 0;

    foreach (col, attnamelist) {
        index++;
        if (lfirst_int(col) == attnum) {
            return index;
        }
    }
    ereport(ERROR, (errcode(ERRCODE_INVALID_COLUMN_REFERENCE), errmsg("Attnum %d not find", attnum)));

    /* on failure */
    return InvalidAttrNumber;
}

int ReadAttributesFixedWith(CopyState cstate)
{
    Assert(cstate != NULL);
    Assert(cstate->formatter != NULL);

    char* curPtr = NULL;
    char* outptr = NULL;
    FixFormatter* formatter = (FixFormatter*)(cstate->formatter);
    IllegalCharErrInfo* err_info = NULL;
    ListCell* cur = NULL;

    /*
     * We need a special case for zero-column tables: check that the input
     * line is empty, and return.
     */
    if (cstate->max_fields <= 0) {
        if (cstate->line_buf.len != 0)
            ereport(ERROR, (errcode(ERRCODE_BAD_COPY_FILE_FORMAT), errmsg("extra data after last expected column")));
        return 0;
    }

    resetStringInfo(&cstate->attribute_buf);
    Assert(cstate->raw_fields != NULL);
    errno_t rc =
        memset_s(cstate->raw_fields, sizeof(char*) * cstate->max_fields, 0, sizeof(char*) * cstate->max_fields);
    securec_check(rc, "", "");

    /*
     * The de-escaped attributes will certainly not be longer than the input
     * data line, so we can just force attribute_buf to be large enough and
     * then transfer data without any checks for enough space.	We need to do
     * it this way because enlarging attribute_buf mid-stream would invalidate
     * pointers already stored into cstate->raw_fields[].
     */
    if (cstate->attribute_buf.maxlen <= cstate->line_buf.len)
        enlargeStringInfo(&cstate->attribute_buf, cstate->line_buf.len + formatter->nfield * 2);
    outptr = cstate->attribute_buf.data;

    ReadAttributeSequence(cstate);

    for (int i = 0; i < formatter->nfield; i++) {
        FieldDesc* desc = &(formatter->fieldDesc[i]);
        uint32 inputSize = 0;
        int attnum = FixGetColumnListIndex(cstate->attnumlist, desc->attnum);

        // if the rest length of data < position of current field,
        // we make the field as a NULL field
        //
        curPtr = cstate->line_buf.len > desc->fieldPos ? cstate->line_buf.data + desc->fieldPos : NULL;

        if (curPtr != NULL) {
            StrnTrimTo(outptr, &inputSize, curPtr, Min(desc->fieldSize, cstate->line_buf.len - desc->fieldPos));
            cstate->raw_fields[attnum - 1] = outptr;
            outptr += inputSize;

            /*
             * match each bulkload illegal error to each field.
             */
            if (cstate->illegal_chars_error) {
                foreach (cur, cstate->illegal_chars_error) {
                    err_info = (IllegalCharErrInfo*)lfirst(cur);

                    if (err_info->err_offset > 0) {
                        /*
                         * bulkload illegal error is matched to this field.
                         * for fixed format each field has the value range[desc->fieldPos, desc->fieldPos +
                         * desc->fieldSize);
                         */
                        if (err_info->err_offset < (desc->fieldPos + desc->fieldSize)) {
                            /*
                             * indicate the matched field;
                             */
                            err_info->err_field_no = i;
                            /*
                             * if has been matched set err_offset to -1 to avoid duplicated check.
                             */
                            err_info->err_offset = -1;
                        }
                    }
                }
            }
        }

        *outptr++ = '\0';
    }

    /* Clean up state of attribute_buf */
    outptr--;
    Assert(*outptr == '\0');
    cstate->attribute_buf.len = (outptr - cstate->attribute_buf.data);
    return list_length(cstate->attnumlist);
}

static int MbLen(const char* mbstr, int encoding)
{
    return ((*pg_wchar_table[encoding].mblen)((const unsigned char*)mbstr));
}

static void MbCutString(int encoding, char* str, int limitLen)
{
    int len = strlen(str);
    char* mbstr = str;

    if (len <= limitLen) {
        return;
    }
    len = 0;
    while (1) {
        int mbLen = MbLen(mbstr, encoding);
        if (len + mbLen > limitLen) {
            break;
        }

        len += mbLen;
        mbstr += mbLen;
    }

    str[len] = '\0';
}

template <bool needCutString>
static void AttributeOutFixed(CopyState cstate, char* string, FieldDesc* desc)
{
    char* ptr = NULL;
    int len;
    StringInfo outbuf = cstate->fe_msgbuf;

    if (cstate->need_transcoding)
        ptr = pg_server_to_any(string, strlen(string), cstate->file_encoding);
    else
        ptr = string;

    if (needCutString)
        MbCutString(cstate->file_encoding, ptr, desc->fieldSize);
    else if (outbuf->len > desc->fieldPos)
        ereport(ERROR, (errcode(ERRCODE_BAD_COPY_FILE_FORMAT), errmsg("field position covers previous field")));

    len = strlen(ptr);

    if (outbuf->len < desc->fieldPos) {
        char* outptr = outbuf->data + outbuf->len;
        int neednSpace = desc->fieldPos - outbuf->len;

        errno_t rc = memset_s(outptr, neednSpace, ' ', neednSpace);
        securec_check(rc, "", "");
        outbuf->len += neednSpace;
    }

    // if length of string > length of field, report an formate error,
    // else append space char at the begin/end of field
    //
    if (len <= desc->fieldSize) {
        char* outptr = outbuf->data + outbuf->len;
        int alignSize = desc->fieldPos - outbuf->len + (desc->fieldSize - len);
        errno_t rc = 0;

        switch (desc->align) {
            case ALIGN_LEFT: {
                if (len != 0) {
                    rc = memcpy_s(outptr, len, ptr, len);
                    securec_check(rc, "", "");
                }
                outptr += len;
                rc = memset_s(outptr, alignSize, ' ', alignSize);
                securec_check(rc, "", "");
            } break;
            case ALIGN_RIGHT: {
                rc = memset_s(outptr, alignSize, ' ', alignSize);
                securec_check(rc, "", "");
                outptr += alignSize;
                if (len != 0) {
                    rc = memcpy_s(outptr, len, ptr, len);
                    securec_check(rc, "", "");
                }
            } break;
            default:
                Assert(false);
        }
        outbuf->len += (len + alignSize);
        outbuf->data[outbuf->len] = '\0';
    } else
        ereport(ERROR,
            (errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
                errmsg("length of field \"%s\" longer than limit of \'%d\'", desc->fieldname, desc->fieldSize)));
}

void FixedRowOut(CopyState cstate, Datum* values, const bool* nulls)
{
    FmgrInfo* out_functions = cstate->out_functions;
    FixFormatter* formatter = (FixFormatter*)cstate->formatter;
    FieldDesc* descs = formatter->fieldDesc;
    char* string = NULL;

    enlargeStringInfo(cstate->fe_msgbuf, formatter->lineSize);
    for (int i = 0; i < formatter->nfield; i++) {
        int attnum = formatter->fieldDesc[i].attnum;
        Datum value = values[attnum - 1];
        bool isnull = nulls[attnum - 1];

        if (isnull) {
            AttributeOutFixed<false>(cstate, descs[i].nullString, descs + i);
        } else {
            string = OutputFunctionCall(&out_functions[attnum - 1], value);
            Assert(string != NULL);
            AttributeOutFixed<false>(cstate, string, descs + i);
        }
    }
}

void PrintFixedHeader(CopyState cstate)
{
    FixFormatter* formatter = (FixFormatter*)cstate->formatter;
    FieldDesc* descs = formatter->fieldDesc;
    TupleDesc tupDesc;
    Form_pg_attribute* attr = NULL;
    char* colname = NULL;

    if (cstate->rel)
        tupDesc = RelationGetDescr(cstate->rel);
    else
        tupDesc = cstate->queryDesc->tupDesc;
    attr = tupDesc->attrs;
    enlargeStringInfo(cstate->fe_msgbuf, formatter->lineSize);

    for (int i = 0; i < formatter->nfield; i++) {
        int attnum = formatter->fieldDesc[i].attnum;

        colname = pstrdup(NameStr(attr[attnum - 1]->attname));
        AttributeOutFixed<true>(cstate, colname, descs + i);
        pfree_ext(colname);
    }
}

void VerifyFixedFormatter(TupleDesc tupDesc, FixFormatter* formatter)
{
    List* missFields = NIL;
    ListCell* lc = NULL;
    StringInfo buf = makeStringInfo();

    // Find out which colunm haven't specified Position
    //
    for (int attnum = 0; attnum < tupDesc->natts; attnum++) {
        char* name = NULL;
        bool found = false;

        if (tupDesc->attrs[attnum]->attisdropped)
            continue;
        for (int i = 0; i < formatter->nfield; i++) {
            name = formatter->fieldDesc[i].fieldname;
            if (namestrcmp(&(tupDesc->attrs[attnum]->attname), name) == 0) {
                found = true;
                break;
            }
        }

        if (!found)
            missFields = lappend(missFields, name);
    }

    if (list_length(missFields) > 0) {
        appendStringInfoString(buf, "\"");
        foreach (lc, missFields) {
            char* name = (char*)lfirst(lc);

            if (lc != list_head(missFields))
                appendStringInfoString(buf, ",");

            appendStringInfoString(buf, name);
        }
        appendStringInfo(buf, "\" missing POSITION");
        ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("%s", buf->data)));
    }
}
