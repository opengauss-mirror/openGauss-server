#----------------------------------------------------------------------------
#
# MPPDB CM makefile
#
#
# distribute/cm_communication/Makefile
#
#-----------------------------------------------------------------------------
PGFILEDESC = "cm - Global Transaction Manager for openGauss"
subdir = src/lib/cm_communication
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

SUBDIRS = cm_feconnect cm_libpq

# Suppress parallel build to avoid dependencies in the subdirectories.
.NOTPARALLEL:

$(recurse)
