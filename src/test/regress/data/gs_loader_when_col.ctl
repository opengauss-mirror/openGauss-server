-- comments
LOAD DATA
truncate into table sqlldr_tbl
WHEN name = 'OK' 
fields terminated by ','
trailing nullcols
(
    --id position(1:1) integer external,
    id integer external,
    name char(32),
    con ":id || '-' || :name",
    dt date
)
