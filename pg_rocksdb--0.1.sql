CREATE FUNCTION pg_rocksdb_fdw_handler()
-- 底下必须返回的是fdw_handler,而不是其他的
RETURNS fdw_handler
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FOREIGN DATA WRAPPER pg_rocksdb_fdw
  HANDLER pg_rocksdb_fdw_handler;

