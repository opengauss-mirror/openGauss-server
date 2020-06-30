# contrib/pgcrypto/Makefile

INT_SRCS = md5.cpp sha1.cpp sha2.cpp internal.cpp internal-sha2.cpp blf.cpp rijndael.cpp \
		fortuna.cpp random.cpp pgp-mpi-internal.cpp imath.cpp
INT_TESTS = sha2

OSSL_SRCS = openssl.cpp pgp-mpi-openssl.cpp
OSSL_TESTS = sha2 des 3des cast5

ZLIB_TST = pgp-compression
ZLIB_OFF_TST = pgp-zlib-DISABLED

CF_SRCS = $(if $(subst no,,$(with_openssl)), $(OSSL_SRCS), $(INT_SRCS))
CF_TESTS = $(if $(subst no,,$(with_openssl)), $(OSSL_TESTS), $(INT_TESTS))
CF_PGP_TESTS = $(if $(subst no,,$(with_zlib)), $(ZLIB_TST), $(ZLIB_OFF_TST))

SRCS		= pgcrypto.cpp px.cpp px-hmac.cpp px-crypt.cpp \
		crypt-gensalt.cpp crypt-blowfish.cpp crypt-des.cpp \
		crypt-md5.cpp $(CF_SRCS) \
		mbuf.cpp pgp.cpp pgp-armor.cpp pgp-cfb.cpp pgp-compress.cpp \
		pgp-decrypt.cpp pgp-encrypt.cpp pgp-info.cpp pgp-mpi.cpp \
		pgp-pubdec.cpp pgp-pubenc.cpp pgp-pubkey.cpp pgp-s2k.cpp \
		pgp-pgsql.cpp

MODULE_big	= pgcrypto
OBJS		= $(SRCS:.cpp=.o)

EXTENSION = pgcrypto
DATA = pgcrypto--1.0.sql pgcrypto--unpackaged--1.0.sql

REGRESS = init md5 sha1 hmac-md5 hmac-sha1 blowfish rijndael \
	$(CF_TESTS) \
	crypt-des crypt-md5 crypt-blowfish crypt-xdes \
	pgp-armor pgp-decrypt pgp-encrypt $(CF_PGP_TESTS) \
	pgp-pubkey-decrypt pgp-pubkey-encrypt pgp-info

EXTRA_CLEAN = gen-rtab

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/pgcrypto
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif

# Add libraries that pgcrypto depends (or might depend) on into the
# shared library link.  (The order in which you list them here doesn't
# matter.)
SHLIB_LINK += $(filter -lcrypto -lz, $(LIBS))
ifeq ($(PORTNAME), win32)
SHLIB_LINK += $(filter -leay32, $(LIBS))
# those must be at the end
SHLIB_LINK += -lwsock32 -lws2_32
endif

rijndael.o: rijndael.tbl

rijndael.tbl:
	$(CC) $(CPPFLAGS) $(CFLAGS) -DPRINT_TABS rijndael.cpp -o gen-rtab
	./gen-rtab > rijndael.tbl
