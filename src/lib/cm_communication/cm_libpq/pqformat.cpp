/* -------------------------------------------------------------------------
 *
 * pqformat.c
 *		Routines for formatting and parsing frontend/backend messages
 *
 * Outgoing messages are built up in a StringInfo buffer (which is expansible)
 * and then sent in a single call to pq_putmessage.  This module provides data
 * formatting/conversion routines that are needed to produce valid messages.
 * Note in particular the distinction between "raw data" and "text"; raw data
 * is message protocol characters and binary values that are not subject to
 * character set conversion, while text is converted by character encoding
 * rules.
 *
 * Incoming messages are similarly read into a StringInfo buffer, via
 * pq_getmessage, and then parsed and converted from that using the routines
 * in this module.
 *
 * These same routines support reading and writing of external binary formats
 * (typsend/typreceive routines).  The conversion routines for individual
 * data types are exactly the same, only initialization and completion
 * are different.
 *
 *
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 *	$PostgreSQL: pgsql/src/backend/libpq/pqformat.c,v 1.48 2009/01/01 17:23:42 momjian Exp $
 *
 * -------------------------------------------------------------------------
 */
/*
 * INTERFACE ROUTINES
 * Message assembly and output:
 *		pq_beginmessage - initialize StringInfo buffer
 *		pq_sendbyte		- append a raw byte to a StringInfo buffer
 *		pq_sendint		- append a binary integer to a StringInfo buffer
 *		pq_sendint64	- append a binary 8-byte int to a StringInfo buffer
 *		pq_sendfloat4	- append a float4 to a StringInfo buffer
 *		pq_sendfloat8	- append a float8 to a StringInfo buffer
 *		pq_sendbytes	- append raw data to a StringInfo buffer
 *		pq_sendcountedtext - append a counted text string (with character set conversion)
 *		pq_sendtext		- append a text string (with conversion)
 *		pq_sendstring	- append a null-terminated text string (with conversion)
 *		pq_send_ascii_string - append a null-terminated text string (without conversion)
 *		pq_endmessage	- send the completed message to the frontend
 * Note: it is also possible to append data to the StringInfo buffer using
 * the regular StringInfo routines, but this is discouraged since required
 * character set conversion may not occur.
 *
 * typsend support (construct a bytea value containing external binary data):
 *		pq_begintypsend - initialize StringInfo buffer
 *		pq_endtypsend	- return the completed string as a "bytea*"
 *
 * Special-case message output:
 *		pq_puttextmessage - generate a character set-converted message in one step
 *		pq_putemptymessage - convenience routine for message with empty body
 *
 * Message parsing after input:
 *		pq_getmsgbyte	- get a raw byte from a message buffer
 *		pq_getmsgint	- get a binary integer from a message buffer
 *		pq_getmsgint64	- get a binary 8-byte int from a message buffer
 *		pq_getmsgfloat4 - get a float4 from a message buffer
 *		pq_getmsgfloat8 - get a float8 from a message buffer
 *		pq_getmsgbytes	- get raw data from a message buffer
 *		pq_copymsgbytes - copy raw data from a message buffer
 *		pq_getmsgtext	- get a counted text string (with conversion)
 *		pq_getmsgstring - get a null-terminated text string (with conversion)
 *		pq_getmsgend	- verify message fully consumed
 *		pq_getmsgunreadlen - get length of the unread data in the message buffer
 */

/* --------------------------------
 *		pq_getmsgint	- get a binary integer from a message buffer
 *
 *		Values are treated as unsigned.
 * --------------------------------
 */
#include <sys/param.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include "cm/libpq.h"
#include "cm/pqformat.h"
#include "cm/elog.h"

const char* pq_get_msg_type(CM_StringInfo msg, int datalen)
{
    const char* result = NULL;

    if (datalen < 0 || datalen > (msg->len - msg->cursor)) {
        write_runlog(ERROR,
            "pq_get_msg_type: insufficient data left in message, datalen=%d, msg->len=%d, msg->cursor=%d.\n",
            datalen,
            msg->len,
            msg->cursor);
        return NULL;
    }

    result = &msg->data[msg->cursor];
    return result;
}

/* --------------------------------
 *		pq_getmsgbytes	- get raw data from a message buffer
 *
 *		Returns a pointer directly into the message buffer; note this
 *		may not have any particular alignment.
 * --------------------------------
 */
const char* pq_getmsgbytes(CM_StringInfo msg, int datalen)
{
    const char* result = NULL;
    errno_t rc = 0;
    int printMsgLen = 101;
    char dataLog[printMsgLen] = {0};
    if (datalen < 0 || datalen > (msg->len - msg->cursor)) {
        write_runlog(ERROR,
            "pq_getmsgbytes: insufficient data left in message, "
            "datalen=%d, msg->len=%d, msg->maxlen=%d, msg->cursor=%d,"
            " msg->qtype=%d, msg->msglen=%d.\n",
            datalen,
            msg->len,
            msg->maxlen,
            msg->cursor,
            msg->qtype,
            msg->msglen);
        if (msg->len < printMsgLen) {
            rc = memcpy_s(dataLog, printMsgLen, msg->data, msg->len);
            securec_check_errno(rc, );
            write_runlog(ERROR, "pq_getmsgbytes: msg->data=%s.\n", dataLog);
        }
        return NULL;
    }

    result = &msg->data[msg->cursor];
    msg->cursor += datalen;
    return result;
}

const char* pq_getmsgbytes(CM_Result* msg, int datalen)
{
    const char* result = NULL;
    if (datalen < 0 || datalen > msg->gr_msglen) {
        write_runlog(ERROR,
            "pq_getmsgbytes: insufficient data left in message, "
            "datalen=%d, res->gr_msglen=%d.\n",
            datalen,
            msg->gr_msglen);
        return NULL;
    }

    result = (char*)&(msg->gr_resdata);
    return result;
}
