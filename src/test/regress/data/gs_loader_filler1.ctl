-- comments
LOAD DATA
truncate into table sqlldr_tbl
WHEN (2:2) = ',' 
-- internal comments 
fields terminated by ','
trailing nullcols
(
    --id position(1:1) integer external,
    id filler integer external,
    name char(32),
    con "'-' || :name",
    dt date
)
