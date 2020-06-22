--
--Create the extension and server for tests
--


CREATE DATABASE lsmtest;  
\c lsmtest  

CREATE EXTENSION lsm;  
CREATE SERVER lsm_server FOREIGN DATA WRAPPER lsm_fdw;  
