-- comments
LOAD DATA
truncate into table sqlldr_col_tbl

trailing nullcols
(
    COL_1 sequence(1,1),
    COL_2 constant "constant col",
    COL_3 char "trim(:COL_3)",
    COL_4 "4",
    COL_5 position(1-4) integer external,
    COL_6 position(5-8) char,
    COL_7 position(9-12),
    COL_8 position(13-14) "replace(:COL_8, 'a', 'A')",
    COL_9 sequence(1,2),
    COL_10 sequence(3,3)
)
