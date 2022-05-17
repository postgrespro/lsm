MODULE_big = lsm
OBJS = lsm_fdw.o lsm_client.o lsm_server.o lsm_posix.o lsm_storage.o lsm_util.o
PGFILEDESC = "LSM: log-structured merge-tree"

PG_CPPFLAGS += -Wno-declaration-after-statement
SHLIB_LINK   = -lrocksdb

EXTENSION = lsm
DATA = lsm--0.1.sql

REGRESS = create basic test testddl testcopy testcolumn
REGRESS_OPTS = --temp-config $(top_srcdir)/contrib/lsm/lsm.conf

ifdef USE_PGXS
PG_CONFIG ?= pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/lsm
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
