/* -------------------------------------------------------------------------
 *
 *	 FILE
 *		fe-misc.c
 *
 *	 DESCRIPTION
 *		 miscellaneous useful functions
 *
 * The communication routines here are analogous to the ones in
 * backend/libpq/pqcomm.c and backend/libpq/pqcomprim.c, but operate
 * in the considerably different environment of the frontend libpq.
 * In particular, we work with a bare nonblock-mode socket, rather than
 * a stdio stream, so that we can avoid unwanted blocking of the application.
 *
 * XXX: MOVE DEBUG PRINTOUT TO HIGHER LEVEL.  As is, block and restart
 * will cause repeat printouts.
 *
 * We must speak the same transmitted data representations as the backend
 * routines.
 *
 *
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 * IDENTIFICATION
 *	  $PostgreSQL: pgsql/src/interfaces/libpq/fe-misc.c,v 1.137 2008/12/11 07:34:09 petere Exp $
 *
 * -------------------------------------------------------------------------
 */

#include "cm/cm_c.h"

#include <signal.h>
#include <time.h>

#include <netinet/in.h>
#include <arpa/inet.h>

#include <unistd.h>
#include <sys/time.h>

#include <poll.h>
#include <sys/poll.h>
#include <sys/select.h>

#include "cm/libpq-fe.h"
#include "cm/libpq-int.h"

static int cmpqPutMsgBytes(const void* buf, size_t len, CM_Conn* conn);
static int cmpqSendSome(CM_Conn* conn, int len);
static int cmpqSocketCheck(CM_Conn* conn, int forRead, int forWrite, time_t end_time);
static int cmpqSocketPoll(int sock, int forRead, int forWrite, time_t end_time);

#define BYTES2 2
#define BYTES4 4

/*
 * cmpqGetc: get 1 character from the connection
 *
 *	All these routines return 0 on success, EOF on error.
 *	Note that for the Get routines, EOF only means there is not enough
 *	data in the buffer, not that there is necessarily a hard error.
 */
int cmpqGetc(char* result, CM_Conn* conn)
{
    if (conn->inCursor >= conn->inEnd) {
        return EOF;
    }

    *result = conn->inBuffer[conn->inCursor++];

    if (conn->Pfdebug != NULL) {
        fprintf(conn->Pfdebug, "From backend> %c\n", *result);
    }

    return 0;
}

/*
 * cmpqGets[_append]:
 * get a null-terminated string from the connection,
 * and store it in an expansible PQExpBuffer.
 * If we run out of memory, all of the string is still read,
 * but the excess characters are silently discarded.
 */
static int cmpqGets_internal(PQExpBuffer buf, CM_Conn* conn, bool resetbuffer)
{
    /* Copy conn data to locals for faster search loop */
    char* inBuffer = conn->inBuffer;
    int inCursor = conn->inCursor;
    int inEnd = conn->inEnd;
    int slen;

    while (inCursor < inEnd && inBuffer[inCursor]) {
        inCursor++;
    }

    if (inCursor >= inEnd) {
        return EOF;
    }

    slen = inCursor - conn->inCursor;

    if (resetbuffer) {
        resetCMPQExpBuffer(buf);
    }

    appendBinaryCMPQExpBuffer(buf, inBuffer + conn->inCursor, slen);

    inCursor++;
    conn->inCursor = inCursor;

    if (conn->Pfdebug != NULL) {
        fprintf(conn->Pfdebug, "From backend> \"%s\"\n", buf->data);
    }

    return 0;
}

int cmpqGets(PQExpBuffer buf, CM_Conn* conn)
{
    return cmpqGets_internal(buf, conn, true);
}

int cmpqGets_append(PQExpBuffer buf, CM_Conn* conn)
{
    return cmpqGets_internal(buf, conn, false);
}

/*
 * cmpqPutnchar:
 *	write exactly len bytes to the current message
 */
int cmpqPutnchar(const char* s, size_t len, CM_Conn* conn)
{
    if (cmpqPutMsgBytes(s, len, conn)) {
        return EOF;
    }

    if (conn->Pfdebug != NULL) {
        fprintf(conn->Pfdebug, "To backend> %.*s\n", (int)len, s);
    }

    return 0;
}

/*
 * cmpqGetnchar:
 * get a string of exactly len bytes in buffer s, no null termination
 */
int cmpqGetnchar(char* s, size_t len, CM_Conn* conn)
{
    errno_t rc = EOK;
    if (len > (size_t)(conn->inEnd - conn->inCursor)) {
        return EOF;
    }

    rc = memcpy_s(s, len, conn->inBuffer + conn->inCursor, len);
    securec_check_c(rc, "", "");
    /* no terminating null */

    conn->inCursor += len;

    if (conn->Pfdebug != NULL) {
        fprintf(conn->Pfdebug, "From backend (%lu)> %.*s\n", (unsigned long)len, (int)len, s);
    }

    return 0;
}

/*
 * cmpqGetInt
 * read a 2 or 4 byte integer and convert from network byte order
 * to local byte order
 */
int cmpqGetInt(int* result, size_t bytes, CM_Conn* conn)
{
    uint16 tmp2;
    uint32 tmp4;
    errno_t rc;

    switch (bytes) {
        case 2:
            if (conn->inCursor + BYTES2 > conn->inEnd) {
                return EOF;
            }
            rc = memcpy_s(&tmp2, BYTES2, conn->inBuffer + conn->inCursor, BYTES2);
            securec_check_c(rc, "\0", "\0");
            conn->inCursor += BYTES2;
            *result = (int)ntohs(tmp2);
            break;
        case 4:
            if (conn->inCursor + BYTES4 > conn->inEnd) {
                return EOF;
            }
            rc = memcpy_s(&tmp4, BYTES4, conn->inBuffer + conn->inCursor, BYTES4);
            securec_check_c(rc, "\0", "\0");
            conn->inCursor += BYTES4;
            *result = (int)ntohl(tmp4);
            break;
        default:
            if (conn->Pfdebug != NULL) {
                fprintf(conn->Pfdebug, "Integer size of (%lu) bytes not supported", bytes);
            }
            return EOF;
    }

    if (conn->Pfdebug != NULL) {
        fprintf(conn->Pfdebug, "From backend (#%lu)> %d\n", (unsigned long)bytes, *result);
    }

    return 0;
}

bool cmConnSetting(CM_Conn* conn, size_t bytes_needed, bool multi)
{
    const int sizeDouble = 2;
    const int size8K = 8192;
    int newsize = conn->outBufSize;
    char* newbuf = NULL;

    if (multi) {
        do {
            newsize *= sizeDouble;
        } while (newsize > 0 && bytes_needed > (size_t)newsize);
    } else {
        do {
            newsize += size8K;
        } while (newsize > 0 && bytes_needed > (size_t)newsize);
    }

    if (newsize > 0 && bytes_needed <= (size_t)newsize) {
        newbuf = (char*)malloc(newsize);
        if (newbuf != NULL) {
            /* realloc succeeded */
            if (conn->outBuffer != NULL) {
                errno_t rc;
                rc = memcpy_s(newbuf, newsize, conn->outBuffer, conn->outBufSize);
                securec_check_c(rc, "\0", "\0");
                FREE_AND_RESET(conn->outBuffer);
            }
            conn->outBuffer = newbuf;
            conn->outBufSize = newsize;
            return true;
        }
    }

    return false;
}

/*
 * Make sure conn's output buffer can hold bytes_needed bytes (caller must
 * include already-stored data into the value!)
 *
 * Returns 0 on success, EOF if failed to enlarge buffer
 */
int cmpqCheckOutBufferSpace(size_t bytes_needed, CM_Conn* conn)
{
    if (bytes_needed <= (size_t)(conn->outBufSize)) {
        return 0;
    }

    /*
     * If we need to enlarge the buffer, we first try to double it in size; if
     * that doesn't work, enlarge in multiples of 8K.  This avoids thrashing
     * the malloc pool by repeated small enlargements.
     *
     * Note: tests for newsize > 0 are to catch integer overflow.
     */
    if (cmConnSetting(conn, bytes_needed, true) || cmConnSetting(conn, bytes_needed, false)) {
        return 0;
    }

    /* realloc failed. Probably out of memory */
    printfCMPQExpBuffer(&conn->errorMessage, "cannot allocate memory for output buffer\n");
    return EOF;
}

/*
 * Make sure conn's input buffer can hold bytes_needed bytes (caller must
 * include already-stored data into the value!)
 *
 * Returns 0 on success, EOF if failed to enlarge buffer
 */
int cmpqCheckInBufferSpace(size_t bytes_needed, CM_Conn* conn)
{
    int newsize = conn->inBufSize;
    char* newbuf = NULL;

    if (bytes_needed <= (size_t)newsize) {
        return 0;
    }

    do {
        newsize *= 2;
    } while (newsize > 0 && bytes_needed > (size_t)newsize);

    if (newsize > 0 && bytes_needed <= (size_t)newsize) {
        newbuf = (char*)malloc(newsize);
        if (newbuf != NULL) {
            /* realloc succeeded */
            if (conn->inBuffer != NULL) {
                errno_t rc;
                rc = memcpy_s(newbuf, newsize, conn->inBuffer, conn->inBufSize);
                securec_check_c(rc, "\0", "\0");
                FREE_AND_RESET(conn->inBuffer);
            }
            conn->inBuffer = newbuf;
            conn->inBufSize = newsize;
            return 0;
        }
    }

    newsize = conn->inBufSize;
    do {
        newsize += 8192;
    } while (newsize > 0 && bytes_needed > (size_t)newsize);

    if (newsize > 0 && bytes_needed <= (size_t)newsize) {
        newbuf = (char*)malloc(newsize);
        if (newbuf != NULL) {
            /* realloc succeeded */
            if (conn->inBuffer != NULL) {
                errno_t rc;
                rc = memcpy_s(newbuf, newsize, conn->inBuffer, conn->inBufSize);
                securec_check_c(rc, "\0", "\0");
                FREE_AND_RESET(conn->inBuffer);
            }
            conn->inBuffer = newbuf;
            conn->inBufSize = newsize;
            return 0;
        }
    }

    /* realloc failed. Probably out of memory */
    printfCMPQExpBuffer(&conn->errorMessage, "cannot allocate memory for input buffer\n");
    return EOF;
}

/*
 * cmpqPutMsgStart: begin construction of a message to the server
 *
 * msg_type is the message type byte, or 0 for a message without type byte
 * (only startup messages have no type byte)
 *
 * force_len forces the message to have a length word; otherwise, we add
 * a length word if protocol 3.
 *
 * Returns 0 on success, EOF on error
 *
 * The idea here is that we construct the message in conn->outBuffer,
 * beginning just past any data already in outBuffer (ie, at
 * outBuffer+outCount).  We enlarge the buffer as needed to hold the message.
 * When the message is complete, we fill in the length word (if needed) and
 * then advance outCount past the message, making it eligible to send.
 *
 * The state variable conn->outMsgStart points to the incomplete message's
 * length word: it is either outCount or outCount+1 depending on whether
 * there is a type byte.  If we are sending a message without length word
 * (pre protocol 3.0 only), then outMsgStart is -1.  The state variable
 * conn->outMsgEnd is the end of the data collected so far.
 */
int cmpqPutMsgStart(char msg_type, bool force_len, CM_Conn* conn)
{
    int lenPos;
    int endPos;

    /* allow room for message type byte */
    if (msg_type) {
        endPos = conn->outCount + 1;
    } else {
        endPos = conn->outCount;
    }

    /* do we want a length word? */
    if (force_len) {
        lenPos = endPos;
        /* allow room for message length */
        endPos += 4;
    } else {
        lenPos = -1;
    }

    /* make sure there is room for message header */
    if (cmpqCheckOutBufferSpace((size_t)endPos, conn)) {
        return EOF;
    }
    /* okay, save the message type byte if any */
    if (msg_type) {
        conn->outBuffer[conn->outCount] = msg_type;
    }
    /* set up the message pointers */
    conn->outMsgStart = lenPos;
    conn->outMsgEnd = endPos;
    /* length word, if needed, will be filled in by cmpqPutMsgEnd */

    if (conn->Pfdebug != NULL) {
        fprintf(conn->Pfdebug, "To backend> Msg %c\n", msg_type ? msg_type : ' ');
    }

    return 0;
}

/*
 * cmpqPutMsgBytes: add bytes to a partially-constructed message
 *
 * Returns 0 on success, EOF on error
 */
static int cmpqPutMsgBytes(const void* buf, size_t len, CM_Conn* conn)
{
    errno_t rc;

    /* make sure there is room for it */
    if (cmpqCheckOutBufferSpace((size_t)conn->outMsgEnd + len, conn)) {
        return EOF;
    }
    /* okay, save the data */
    rc = memcpy_s(conn->outBuffer + conn->outMsgEnd, conn->outBufSize - conn->outMsgEnd, buf, len);
    securec_check_c(rc, "\0", "\0");
    conn->outMsgEnd += len;
    /* no Pfdebug call here, caller should do it */
    return 0;
}

/*
 * cmpqPutMsgEnd: finish constructing a message and possibly send it
 *
 * Returns 0 on success, EOF on error
 *
 * We don't actually send anything here unless we've accumulated at least
 * 8K worth of data (the typical size of a pipe buffer on Unix systems).
 * This avoids sending small partial packets.  The caller must use cmpqFlush
 * when it's important to flush all the data out to the server.
 */
int cmpqPutMsgEnd(CM_Conn* conn)
{
    if (conn->Pfdebug != NULL) {
        fprintf(conn->Pfdebug, "To backend> Msg complete, length %d\n", conn->outMsgEnd - conn->outCount);
    }

    /* Fill in length word if needed */
    if (conn->outMsgStart >= 0) {
        uint32 msgLen = conn->outMsgEnd - conn->outMsgStart;
        errno_t rc;

        msgLen = htonl(msgLen);
        rc = memcpy_s(conn->outBuffer + conn->outMsgStart, conn->outBufSize - conn->outMsgStart, &msgLen, 4);
        securec_check_c(rc, "\0", "\0");
    }

    /* Make message eligible to send */
    conn->outCount = conn->outMsgEnd;

    if (conn->outCount >= 8192) {
        int toSend = conn->outCount - (conn->outCount % 8192);

        if (cmpqSendSome(conn, toSend) < 0) {
            return EOF;
        }
        /* in nonblock mode, don't complain if unable to send it all */
    }

    return 0;
}

/* ----------
 * cmpqReadData: read more data, if any is available
 * Possible return values:
 *	 1: successfully loaded at least one more byte
 *	 0: no data is presently available, but no error detected
 *	-1: error detected (including EOF = connection closure);
 *		conn->errorMessage set
 * NOTE: callers must not assume that pointers or indexes into conn->inBuffer
 * remain valid across this call!
 * ----------
 */
int cmpqReadData(CM_Conn* conn)
{
    int someread = 0;
    int nread;

    if (conn->sock < 0) {
        printfCMPQExpBuffer(&conn->errorMessage, "connection not open\n");
        return TCP_SOCKET_ERROR_EPIPE;
    }

    /* Left-justify any data in the buffer to make room */
    if (conn->inStart < conn->inEnd) {
        if (conn->inStart > 0) {
            errno_t rc;

            rc =
                memmove_s(conn->inBuffer, conn->inBufSize, conn->inBuffer + conn->inStart, conn->inEnd - conn->inStart);
            securec_check_c(rc, "\0", "\0");
            conn->inEnd -= conn->inStart;
            conn->inCursor -= conn->inStart;
            conn->inStart = 0;
        }
    } else {
        /* buffer is logically empty, reset it */
        conn->inStart = conn->inCursor = conn->inEnd = 0;
    }

    /*
     * If the buffer is fairly full, enlarge it. We need to be able to enlarge
     * the buffer in case a single message exceeds the initial buffer size. We
     * enlarge before filling the buffer entirely so as to avoid asking the
     * kernel for a partial packet. The magic constant here should be large
     * enough for a TCP packet or Unix pipe bufferload.  8K is the usual pipe
     * buffer size, so...
     */
    if (conn->inBufSize - conn->inEnd < 8192) {
        if (cmpqCheckInBufferSpace(conn->inEnd + (size_t)8192, conn)) {
            /*
             * We don't insist that the enlarge worked, but we need some room
             */
            if (conn->inBufSize - conn->inEnd < 100)
                return TCP_SOCKET_ERROR_EPIPE; /* errorMessage already set */
        }
    }

    /* OK, try to read some data */
retry3:
    nread = recv(conn->sock, conn->inBuffer + conn->inEnd, conn->inBufSize - conn->inEnd, MSG_DONTWAIT);
    conn->last_call = CM_LastCall_RECV;
    if (nread < 0) {
        conn->last_errno = SOCK_ERRNO;

        if (SOCK_ERRNO == EINTR) {
            goto retry3;
        }
            /* Some systems return EAGAIN/EWOULDBLOCK for no data */
#ifdef EAGAIN
        if (SOCK_ERRNO == EAGAIN) {
            return someread;
        }
#endif
#if defined(EWOULDBLOCK) && (!defined(EAGAIN) || (EWOULDBLOCK != EAGAIN))
        if (SOCK_ERRNO == EWOULDBLOCK) {
            return someread;
        }
#endif
            /* We might get ECONNRESET here if using TCP and backend died */
#ifdef ECONNRESET
        if (SOCK_ERRNO == ECONNRESET) {
            goto definitelyFailed;
        }
#endif
        printfCMPQExpBuffer(&conn->errorMessage, "could not receive data from server:\n");
        return TCP_SOCKET_ERROR_EPIPE;
    } else {
        conn->last_errno = 0;
    }

    if (nread > 0) {
        conn->inEnd += nread;

        /*
         * Hack to deal with the fact that some kernels will only give us back
         * 1 packet per recv() call, even if we asked for more and there is
         * more available.	If it looks like we are reading a long message,
         * loop back to recv() again immediately, until we run out of data or
         * buffer space.  Without this, the block-and-restart behavior of
         * libpq's higher levels leads to O(N^2) performance on long messages.
         *
         * Since we left-justified the data above, conn->inEnd gives the
         * amount of data already read in the current message.	We consider
         * the message "long" once we have acquired 32k ...
         */
#ifdef NOT_USED
        if (conn->inEnd > 32768 && (conn->inBufSize - conn->inEnd) >= 8192) {
            someread = 1;
            goto retry3;
        }
#endif
        return 1;
    }

    if (someread) {
        return 1; /* got a zero read after successful tries */
    }

    /*
     * A return value of 0 could mean just that no data is now available, or
     * it could mean EOF --- that is, the server has closed the connection.
     * Since we have the socket in nonblock mode, the only way to tell the
     * difference is to see if select() is saying that the file is ready.
     * Grumble.  Fortunately, we don't expect this path to be taken much,
     * since in normal practice we should not be trying to read data unless
     * the file selected for reading already.
     *
     * In SSL mode it's even worse: SSL_read() could say WANT_READ and then
     * data could arrive before we make the cmpqReadReady() test.  So we must
     * play dumb and assume there is more data, relying on the SSL layer to
     * detect true EOF.
     */

    switch (cmpqReadReady(conn)) {
        case 0:
            /* definitely no data available */
            return 0;
        case 1:
            /* ready for read */
            break;
        default:
            goto definitelyFailed;
    }

    /*
     * Still not sure that it's EOF, because some data could have just
     * arrived.
     */
retry4:
    nread = recv(conn->sock, conn->inBuffer + conn->inEnd, conn->inBufSize - conn->inEnd, MSG_DONTWAIT);
    conn->last_call = CM_LastCall_RECV;
    if (nread < 0) {
        conn->last_errno = SOCK_ERRNO;
        if (SOCK_ERRNO == EINTR) {
            goto retry4;
        }
            /* Some systems return EAGAIN/EWOULDBLOCK for no data */
#ifdef EAGAIN
        if (SOCK_ERRNO == EAGAIN) {
            return 0;
        }
#endif
#if defined(EWOULDBLOCK) && (!defined(EAGAIN) || (EWOULDBLOCK != EAGAIN))
        if (SOCK_ERRNO == EWOULDBLOCK) {
            return 0;
        }
#endif
            /* We might get ECONNRESET here if using TCP and backend died */
#ifdef ECONNRESET
        if (SOCK_ERRNO == ECONNRESET) {
            goto definitelyFailed;
        }
#endif
        printfCMPQExpBuffer(&conn->errorMessage, "could not receive data from server: \n");
        return -1;
    } else {
        conn->last_errno = 0;
    }
    if (nread > 0) {
        conn->inEnd += nread;
        return 1;
    }

    /*
     * OK, we are getting a zero read even though select() says ready. This
     * means the connection has been closed.  Cope.
     */
definitelyFailed:
    printfCMPQExpBuffer(&conn->errorMessage,
        "server closed the connection unexpectedly\n"
        "\tThis probably means the server terminated abnormally\n"
        "\tbefore or while processing the request.\n");
    conn->status = CONNECTION_BAD; /* No more connection to backend */
    close(conn->sock);
    conn->sock = -1;

    return TCP_SOCKET_ERROR_EPIPE;
}

/*
 * cmpqSendSome: send data waiting in the output buffer.
 *
 * len is how much to try to send (typically equal to outCount, but may
 * be less).
 *
 * Return 0 on success, -1 on failure and 1 when not all data could be sent
 * because the socket would block and the connection is non-blocking.
 */
static int cmpqSendSome(CM_Conn* conn, int len)
{
    char* ptr = conn->outBuffer;
    int remaining = conn->outCount;
    int result = 0;

    if (conn->sock < 0) {
        printfCMPQExpBuffer(&conn->errorMessage, "connection not open\n");
        return TCP_SOCKET_ERROR_EPIPE;
    }

    /* while there's still data to send */
    while (len > 0) {
        int sent;

        sent = send(conn->sock, ptr, len, MSG_DONTWAIT);
        conn->last_call = CM_LastCall_SEND;

        if (sent < 0) {
            conn->last_errno = SOCK_ERRNO;
            /*
             * Anything except EAGAIN/EWOULDBLOCK/EINTR is trouble. If it's
             * EPIPE or ECONNRESET, assume we've lost the backend connection
             * permanently.
             */
            switch (SOCK_ERRNO) {
#ifdef EAGAIN
                case EAGAIN:
                    break;
#endif
#if defined(EWOULDBLOCK) && (!defined(EAGAIN) || (EWOULDBLOCK != EAGAIN))
                case EWOULDBLOCK:
                    break;
#endif
                case EINTR:
                    continue;

                case EPIPE:
#ifdef ECONNRESET
                case ECONNRESET:
#endif
                    printfCMPQExpBuffer(&conn->errorMessage,
                        "server closed the connection unexpectedly\n"
                        "\tThis probably means the server terminated abnormally\n"
                        "\tbefore or while processing the request. errno:%d\n",
                        SOCK_ERRNO);

                    /*
                     * We used to close the socket here, but that's a bad idea
                     * since there might be unread data waiting (typically, a
                     * NOTICE message from the backend telling us it's
                     * committing hara-kiri...).  Leave the socket open until
                     * cmpqReadData finds no more data can be read.  But abandon
                     * attempt to send data.
                     */
                    conn->outCount = 0;
                    return TCP_SOCKET_ERROR_EPIPE;

                default:
                    printfCMPQExpBuffer(&conn->errorMessage, "could not send data to server: \n");
                    /* We don't assume it's a fatal error... */
                    conn->outCount = 0;
                    return TCP_SOCKET_ERROR_EPIPE;
            }
        } else {
            ptr += sent;
            len -= sent;
            remaining -= sent;
            conn->last_errno = 0;
        }

        if (len > 0) {
            /*
             * We didn't send it all, wait till we can send more.
             *
             * If the connection is in non-blocking mode we don't wait, but
             * return 1 to indicate that data is still pending.
             */
            result = 1;
            break;
        }
    }

    /* shift the remaining contents of the buffer */
    if (remaining > 0) {
        errno_t rc;

        rc = memmove_s(conn->outBuffer, conn->outBufSize, ptr, remaining);
        securec_check_c(rc, "\0", "\0");
    }
    conn->outCount = remaining;

    return result;
}

/*
 * cmpqFlush: send any data waiting in the output buffer
 *
 * Return 0 on success, -1 on failure and 1 when not all data could be sent
 * because the socket would block and the connection is non-blocking.
 */
int cmpqFlush(CM_Conn* conn)
{
    if (conn->Pfdebug != NULL) {
        fflush(conn->Pfdebug);
    }

    if (conn->outCount > 0) {
        return cmpqSendSome(conn, conn->outCount);
    }

    return 0;
}
/*
 * cmpqWait: wait until we can read or write the connection socket
 *
 * JAB: If SSL enabled and used and forRead, buffered bytes short-circuit the
 * call to select().
 *
 * We also stop waiting and return if the kernel flags an exception condition
 * on the socket.  The actual error condition will be detected and reported
 * when the caller tries to read or write the socket.
 */
int cmpqWait(int forRead, int forWrite, CM_Conn* conn)
{
    return cmpqWaitTimed(forRead, forWrite, conn, time(NULL) + 5);
}

/*
 * cmpqWaitTimed: wait, but not past finish_time.
 *
 * If finish_time is exceeded then we return failure (EOF).  This is like
 * the response for a kernel exception because we don't want the caller
 * to try to read/write in that case.
 *
 * finish_time = ((time_t) -1) disables the wait limit.
 */
int cmpqWaitTimed(int forRead, int forWrite, CM_Conn* conn, time_t finish_time)
{
    int result;

    result = cmpqSocketCheck(conn, forRead, forWrite, finish_time);

    if (result < 0) {
        return EOF; /* errorMessage is already set */
    }

    if (result == 0) {
        printfCMPQExpBuffer(&conn->errorMessage, "timeout expired\n");
        return EOF;
    }

    return 0;
}

/*
 * cmpqReadReady: is select() saying the file is ready to read?
 * Returns -1 on failure, 0 if not ready, 1 if ready.
 */
int cmpqReadReady(CM_Conn* conn)
{
    return cmpqSocketCheck(conn, 1, 0, (time_t)0);
}

/*
 * Checks a socket, using poll or select, for data to be read, written,
 * or both.  Returns >0 if one or more conditions are met, 0 if it timed
 * out, -1 if an error occurred.
 *
 * If SSL is in use, the SSL buffer is checked prior to checking the socket
 * for read data directly.
 */
static int cmpqSocketCheck(CM_Conn* conn, int forRead, int forWrite, time_t end_time)
{
    int result;

    if (conn == NULL) {
        return TCP_SOCKET_ERROR_EPIPE;
    }
    if (conn->sock < 0) {
        printfCMPQExpBuffer(&conn->errorMessage, "socket not open\n");
        return TCP_SOCKET_ERROR_EPIPE;
    }

    /* We will retry as long as we get EINTR */
    do
        result = cmpqSocketPoll(conn->sock, forRead, forWrite, end_time);
    while (result < 0 && SOCK_ERRNO == EINTR);

    if (result < 0) {
        printfCMPQExpBuffer(&conn->errorMessage, "select() failed: \n");
    }

    return result;
}

/*
 * Check a file descriptor for read and/or write data, possibly waiting.
 * If neither forRead nor forWrite are set, immediately return a timeout
 * condition (without waiting).  Return >0 if condition is met, 0
 * if a timeout occurred, -1 if an error or interrupt occurred.
 *
 * Timeout is infinite if end_time is -1.  Timeout is immediate (no blocking)
 * if end_time is 0 (or indeed, any time before now).
 */
static int cmpqSocketPoll(int sock, int forRead, int forWrite, time_t end_time)
{

#if 1
    /* We use poll(2) if available, otherwise select(2) */
#ifdef HAVE_POLL
    struct pollfd input_fd;
    int timeout_ms;

    if (!forRead && !forWrite) {
        return 0;
    }

    input_fd.fd = sock;
    input_fd.events = POLLERR;
    input_fd.revents = 0;

    if (forRead) {
        input_fd.events |= POLLIN;
    }
    if (forWrite) {
        input_fd.events |= POLLOUT;
    }

    /* Compute appropriate timeout interval */
    if (end_time == ((time_t)-1)) {
        timeout_ms = -1;
    } else {
        time_t now = time(NULL);

        if (end_time > now) {
            timeout_ms = (end_time - now) * 1000;
        } else {
            timeout_ms = 0;
        }
    }

    return poll(&input_fd, 1, timeout_ms);
#else  /* !HAVE_POLL */

    fd_set input_mask;
    fd_set output_mask;
    fd_set except_mask;
    struct timeval timeout;
    struct timeval* ptr_timeout = NULL;

    if (!forRead && !forWrite) {
        return 0;
    }

    FD_ZERO(&input_mask);
    FD_ZERO(&output_mask);
    FD_ZERO(&except_mask);
    if (forRead)
        FD_SET(sock, &input_mask);
    if (forWrite)
        FD_SET(sock, &output_mask);
    FD_SET(sock, &except_mask);

    /* Compute appropriate timeout interval */
    if (end_time == ((time_t)-1)) {
        ptr_timeout = NULL;
    } else {
        time_t now = time(NULL);

        if (end_time > now) {
            timeout.tv_sec = end_time - now;
        } else {
            timeout.tv_sec = 0;
        }
        timeout.tv_usec = 0;
        ptr_timeout = &timeout;
    }

    return select(sock + 1, &input_mask, &output_mask, &except_mask, ptr_timeout);
#endif /* HAVE_POLL */
#endif
}
