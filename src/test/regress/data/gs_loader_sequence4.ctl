-- comments
LOAD DATA
append into table sqlldr_col_tbl

fields terminated by ',' OPTIONALLY ENCLOSED BY '"'
trailing nullcols
(
    COL_1 sequence(1),
    COL_3 char "trim(:COL_3)",
    COL_2 constant "constant col",
    COL_5  integer external,
    COL_4 "4",
    COL_9 sequence(MAX,2),
    COL_6 char,
    COL_10 sequence(COUNT,3),
    COL_8 "replace(:COL_8, 'a', 'A')"
)
