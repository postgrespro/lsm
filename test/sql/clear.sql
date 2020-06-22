--
-- Delete the extension and server 
--

\c lsmtest

DROP SERVER lsm_server;  
DROP EXTENSION lsm_fdw;  
  
\c postgres  
DROP DATABASE lsmtest;  
