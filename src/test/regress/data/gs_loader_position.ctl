-- comments
LOAD DATA
truncate into table sqlldr_col_tbl

trailing nullcols
(
    COL_1  "('123')",
    COL_3 char "trim(:COL_6)",
    COL_4 "4",
    COL_5 position(1-4) integer external,
    COL_6 position(5-8) char,
    COL_7 position(9-12) char "trim(:COL_2)",
    COL_2 constant "constant col constant col constant col constant col constant col constant col constant col constant col constant col constant col",
    COL_8 position(13-14) "replace(:COL_8, ':', '')",
    COL_9 position(15-16) decimal external ":COL_9/10",
    COL_10 position(17-20) decimal external "case when :COL_10<1000 then 0 else :COL_10-1000 end"
)
