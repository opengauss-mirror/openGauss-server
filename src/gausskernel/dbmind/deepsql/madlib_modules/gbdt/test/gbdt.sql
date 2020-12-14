DROP TABLE IF EXISTS dt_golf CASCADE;
DROP TABLE IF EXISTS train_output,train_output_summary;
CREATE TABLE dt_golf (
    id integer NOT NULL,
    "OUTLOOK" text,
    temperature double precision,
    humidity double precision,
    "Cont_features" double precision[],
    cat_features text[],
    windy boolean,
    class integer
) m4_ifdef(`__COLUMNS__', `WITH(ORIENTATION=COLUMN)', `');

INSERT INTO dt_golf (id,"OUTLOOK",temperature,humidity,"Cont_features",cat_features, windy,class) VALUES
(1, 'sunny', 85, 85,ARRAY[85, 85], ARRAY['a', 'b'], false, 0),
(2, 'sunny', 80, 90, ARRAY[80, 90], ARRAY['a', 'b'], true, 0),
(3, 'overcast', 83, 78, ARRAY[83, 78], ARRAY['a', 'b'], false, 1),
(4, 'rain', 70, NULL, ARRAY[70, 96], ARRAY['a', 'b'], false, 1),
(5, 'rain', 68, 80, ARRAY[68, 80], ARRAY['a', 'b'], false, 1),
(6, 'rain', NULL, 70, ARRAY[65, 70], ARRAY['a', 'b'], true, 0),
(7, 'overcast', 64, 65, ARRAY[64, 65], ARRAY['c', 'b'], NULL , 1),
(8, 'sunny', 72, 95, ARRAY[72, 95], ARRAY['a', 'b'], false, 0),
(9, 'sunny', 69, 70, ARRAY[69, 70], ARRAY['a', 'b'], false, 1),
(10, 'rain', 75, 80, ARRAY[75, 80], ARRAY['a', 'b'], false, 1),
(11, 'sunny', 75, 70, ARRAY[75, 70], ARRAY['a', 'd'], true, 1),
(12, 'overcast', 72, 90, ARRAY[72, 90], ARRAY['c', 'b'], NULL, 1),
(13, 'overcast', 81, 75, ARRAY[81, 75], ARRAY['a', 'b'], false, 1),
(15, NULL, 81, 75, ARRAY[81, 75], ARRAY['a', 'b'], false, 1),
(16, 'overcast', NULL, 75, ARRAY[81, 75], ARRAY['a', 'd'], false,1),
(14, 'rain', 71, 80, ARRAY[71, 80], ARRAY['c', 'b'], true, 0);

-------------------------------------------------------------------------
-- classification without grouping
 select madlib.gbdt_train('dt_golf',         -- source table
                  'train_output',    -- output model table
                  'id'  ,            -- id column
                  'class',           -- response
                  'windy, "Cont_features"[1]',   -- features
                  NULL,        -- exclude columns
	  NULL,
                  NULL,        -- no grouping
                  30,                -- num of trees
                  2,                 -- num of random features
                  TRUE,    -- importance
                  1,       -- num_permutations
                  10,       -- max depth
                  1,        -- min split
                  1,        -- min bucket
                  8,        -- number of bins per continuous variable
                  'max_surrogates=0',
	              'response',
	              0.001,
                  FALSE,
	              0.5
);

CREATE TABLE dt_golf2 as SELECT * from dt_golf;
DROP TABLE IF EXISTS test_output;
select madlib.gbdt_predict('dt_golf2','train_output','test_output','id');
