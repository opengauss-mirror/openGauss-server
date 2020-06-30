/* src/include/port/irix.h */

#ifndef PORT_IRIX_H
#define PORT_IRIX_H

/*
 * IRIX 6.5.26f and 6.5.22f (at least) have a strtod() that accepts
 * "infinity", but leaves endptr pointing to "inity".
 */
#define HAVE_BUGGY_IRIX_STRTOD

#endif /* PORT_IRIX_H */
