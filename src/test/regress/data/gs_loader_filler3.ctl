-- comments
LOAD DATA
truncate into table sqlldr_tbl2
WHEN (2:2) = ',' 
-- internal comments 
fields terminated by ','
trailing nullcols
(
    --id position(1:1) integer external,
    id integer external,
    name char(32),
    con ":id || '-' || :name",
    dt filler date
)
