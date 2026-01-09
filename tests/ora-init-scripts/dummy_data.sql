-- CONNECT TO A PDB as SYSDBA
CONNECT sys/ThisIsASecret123@FREEPDB1 AS SYSDBA;
-- CREATE USER

CREATE TABLESPACE users
DATAFILE 'users.dbf' -- Specify the full path and filename
SIZE 100M -- Initial size of the datafile
AUTOEXTEND ON -- Allows the datafile to grow automatically
NEXT 10M -- Specifies the size of each new extent when needed
MAXSIZE UNLIMITED -- Sets no upper limit on the total size
EXTENT MANAGEMENT LOCAL -- Recommended; manages extents using bitmaps within the tablespace
SEGMENT SPACE MANAGEMENT AUTO;

CREATE USER read_user IDENTIFIED BY ThisIsASecret123
DEFAULT TABLESPACE users
TEMPORARY TABLESPACE temp
QUOTA UNLIMITED ON users;
GRANT CREATE SESSION TO read_user;
GRANT CREATE TABLE TO read_user;
GRANT CREATE VIEW TO read_user;
GRANT CREATE SEQUENCE TO read_user;

CONNECT read_user/ThisIsASecret123@FREEPDB1;
-- CREATE TABLE FOR USERS
CREATE TABLE sample_datatypes_tbl (
    id            NUMBER(10) PRIMARY KEY,
    char_col      CHAR(10),
    varchar2_col  VARCHAR2(50),
    nchar_col     NCHAR(10),
    nvarchar2_col NVARCHAR2(50),
    clob_col      CLOB,
    blob_col      BLOB,
    date_col      DATE,
    timestamp_col TIMESTAMP,
    number_col    NUMBER(10, 2),
    float_col     FLOAT(126),
    binary_float_col BINARY_FLOAT,
    binary_double_col BINARY_DOUBLE,
    raw_col       RAW(16)
);

-- Insert a single dummy row of data
INSERT INTO sample_datatypes_tbl VALUES (
    1,
    'FixedChar1',
    'VariableChar',
    'NCharFix01',
    'NVarCharValue',
    'This is a large text object stored in a CLOB column.',
    UTL_RAW.CAST_TO_RAW('This is a blob data, very big by the way.'),
    SYSDATE,
    SYSTIMESTAMP,
    12345.67,
    123.45e6,
    1.23e0,
    1.23e0,
    UTL_RAW.CAST_TO_RAW('raw data')
), (
    2,
    'FixedChar2',
    'AnotherVarChar',
    'NCharFix02',
    'AnotherNVarChar',
    'Second CLOB data entry for testing purposes.',
    UTL_RAW.CAST_TO_RAW('Second blob data entry.'),
    SYSDATE + 1,
    SYSTIMESTAMP + INTERVAL '1' HOUR,
    98765.43,
    54.32e3,
    4.56e1,
    7.89e1,
    UTL_RAW.CAST_TO_RAW('more raw data')
);
