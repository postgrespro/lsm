CREATE FUNCTION pg_rocksdb_handler()
RETURNS pg_rocksdb_handler
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FOREIGN DATA WRAPPER pg_rocksdb_fdw
  HANDLER pg_rocksdb_handler;

