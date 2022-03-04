/* -------------------------------------------------------------------------
 *
 * fe-protocol3.c
 *	  functions that are specific to frontend/backend protocol version 3
 *
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 *
 * IDENTIFICATION
 *	  $PostgreSQL$
 *
 * -------------------------------------------------------------------------
 */
#include "cm/cm_c.h"

#include <ctype.h>
#include <fcntl.h>

#include "cm/libpq-fe.h"
#include "cm/libpq-int.h"

#include <unistd.h>
#include <netinet/in.h>

/*
 * This macro lists the backend message types that could be "long" (more
 * than a couple of kilobytes).
 */
#define VALID_LONG_MESSAGE_TYPE(id) ((id) == 'S' || (id) == 'E')

static void handleSyncLoss(CM_Conn* conn, char id, int msgLength);
static CM_Result* pqParseInput(CM_Conn* conn);
static int cmpqParseSuccess(CM_Conn* conn, CM_Result* result);
/*
 * parseInput: if appropriate, parse input data from backend
 * until input is exhausted or a stopping state is reached.
 * Note that this function will NOT attempt to read more data from the backend.
 */
static CM_Result* pqParseInput(CM_Conn* conn)
{
    char id;
    int msgLength;
    int avail;
    CM_Result* result = NULL;

    if (conn->result == NULL) {
        errno_t rc;

        conn->result = (CM_Result*)malloc(sizeof(CM_Result));
        if (conn->result == NULL)
            return NULL;
        rc = memset_s(conn->result, sizeof(CM_Result), 0, sizeof(CM_Result));
        securec_check_c(rc, (char*)conn->result, "\0");
    }

    result = conn->result;

    /*
     * Try to read a message.  First get the type code and length. Return
     * if not enough data.
     */
    conn->inCursor = conn->inStart;
    if (cmpqGetc(&id, conn))
        return NULL;
    if (cmpqGetInt(&msgLength, 4, conn))
        return NULL;

    /*
     * Try to validate message type/length here.  A length less than 4 is
     * definitely broken.  Large lengths should only be believed for a few
     * message types.
     */
    if (msgLength < 4) {
        handleSyncLoss(conn, id, msgLength);
        return NULL;
    }
    if (msgLength > 30000 && !VALID_LONG_MESSAGE_TYPE(id)) {
        handleSyncLoss(conn, id, msgLength);
        return NULL;
    }

    /*
     * Can't process if message body isn't all here yet.
     */
    conn->result->gr_msglen = msgLength -= 4;
    avail = conn->inEnd - conn->inCursor;
    if (avail < msgLength) {
        /*
         * Before returning, enlarge the input buffer if needed to hold
         * the whole message.  This is better than leaving it to
         * cmpqReadData because we can avoid multiple cycles of realloc()
         * when the message is large; also, we can implement a reasonable
         * recovery strategy if we are unable to make the buffer big
         * enough.
         */
        if (cmpqCheckInBufferSpace((size_t)(conn->inCursor + msgLength), conn)) {
            /*
             * XXX add some better recovery code... plan is to skip over
             * the message using its length, then report an error. For the
             * moment, just treat this like loss of sync (which indeed it
             * might be!)
             */
            handleSyncLoss(conn, id, msgLength);
        }
        return NULL;
    }

    /* switch on protocol character */
    switch (id) {
        case 'S': /* command complete */
            if (cmpqParseSuccess(conn, result))
                return NULL;
            break;

        case 'E': /* error return */
            if (cmpqGetError(conn, result))
                return NULL;
            result->gr_status = CM_RESULT_ERROR;
            break;
        default:
            printfCMPQExpBuffer(
                &conn->errorMessage, "unexpected response from server; first received character was \"%c\"\n", id);
            conn->inCursor += msgLength;
            break;
    }
    /* Successfully consumed this message */
    if (conn->inCursor == conn->inStart + 5 + msgLength) {
        /* Normal case: parsing agrees with specified length */
        conn->inStart = conn->inCursor;
    } else {
        /* Trouble --- report it */
        printfCMPQExpBuffer(
            &conn->errorMessage, "message contents do not agree with length in message type \"%c\"\n", id);
        /* trust the specified message length as what to skip */
        conn->inStart += 5 + msgLength;
    }

    return result;
}

/*
 * handleSyncLoss: clean up after loss of message-boundary sync
 *
 * There isn't really a lot we can do here except abandon the connection.
 */
static void handleSyncLoss(CM_Conn* conn, char id, int msgLength)
{
    printfCMPQExpBuffer(
        &conn->errorMessage, "lost synchronization with server: got message type \"%c\", length %d\n", id, msgLength);
    close(conn->sock);
    conn->sock = -1;
    conn->status = CONNECTION_BAD; /* No more connection to backend */
}

/*
 * Attempt to read an Error or Notice response message.
 * This is possible in several places, so we break it out as a subroutine.
 * Entry: 'E' message type and length have already been consumed.
 * Exit: returns 0 if successfully consumed message.
 *		 returns EOF if not enough data.
 */
int cmpqGetError(CM_Conn* conn, CM_Result* result)
{
    char id;

    /*
     * Read the fields and save into res.
     */
    for (;;) {
        if (cmpqGetc(&id, conn)) {
            goto fail;
        }
        if (id == '\0') {
            break;
        }
        if (cmpqGets(&conn->errorMessage, conn)) {
            goto fail;
        }
    }
    return 0;

fail:
    return EOF;
}

/*
 * CMPQgetResult
 * Get the next CM_Result produced.  Returns NULL if no
 * query work remains or an error has occurred (e.g. out of
 * memory).
 */

CM_Result* cmpqGetResult(CM_Conn* conn)
{
    CM_Result* res = NULL;

    if (conn == NULL)
        return NULL;

    /* Parse any available data, if our state permits. */
    while ((res = pqParseInput(conn)) == NULL) {
        int flushResult;

        /*
         * If data remains unsent, send it.  Else we might be waiting for the
         * result of a command the backend hasn't even got yet.
         */
        while ((flushResult = cmpqFlush(conn)) > 0) {
            if (cmpqWait(false, true, conn)) {
                flushResult = -1;
                break;
            }
        }

        /* Wait for some more data, and load it. */
        if (flushResult ||

            cmpqReadData(conn) <= 0) {
            /*
             * conn->errorMessage has been set by cmpqWait or cmpqReadData.
             */
            return NULL;
        }
    }

    return res;
}

/*
 * return 0 if parsing command is totally completed.
 * return 1 if it needs to be read continuously.
 */
static int cmpqParseSuccess(CM_Conn* conn, CM_Result* result)
{
    errno_t rc;

    result->gr_status = CM_RESULT_OK;
    rc = memcpy_s(&(result->gr_resdata), CM_MSG_MAX_LENGTH, conn->inBuffer + conn->inCursor, result->gr_msglen);
    securec_check_c(rc, "\0", "\0");
    return (result->gr_status);
}

void cmpqResetResultData(CM_Result* result)
{

    if (NULL != result) {
        result->gr_msglen = 0;
        result->gr_status = 0;
        result->gr_type = 0;
    }
}
