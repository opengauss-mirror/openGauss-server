-- comments
LOAD DATA
truncate into table sqlldr_col_tbl

FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"'

trailing nullcols
(
    COL_1 sequence(1),
    COL_5 position(1-4) integer external,
    COL_6 position(5-8) char,
    COL_7 position(9-12) char "trim(:COL_2)",
    COL_2 constant 'constant',
    COL_8 position(13-14) "replace(:COL_8, ':', '')",
    COL_9 position(15-16) decimal external ":COL_9/10",
    COL_10 position(17-20) decimal external "case when :COL_10<1000 then 0 else :COL_10-1000 end"
)
