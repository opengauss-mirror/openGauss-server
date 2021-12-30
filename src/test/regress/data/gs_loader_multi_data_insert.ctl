LOAD DATA
insert into table sqlldr_tbl
WHEN (3:3) = ',' 
fields terminated by ','
trailing nullcols
(
    id integer external,
    name char(32),
    con ":id || '-' || :name",
    dt date
)
