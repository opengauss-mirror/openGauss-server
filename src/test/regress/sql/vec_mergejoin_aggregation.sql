/*
 * This file is used to test the function of ExecVecMergeJoin(): part 5,aggregation with null
 */
set enable_hashjoin=off;
set enable_nestloop=off;
----
--- case 6: aggregation with null
----
select vector_mergejoin_engine.VECTOR_MERGEJOIN_TABLE_05.ID,max(vector_mergejoin_engine.VECTOR_MERGEJOIN_TABLE_05.ID),vector_mergejoin_engine.VECTOR_MERGEJOIN_TABLE_05.ID>2,1+2 as RESULT from vector_mergejoin_engine.VECTOR_MERGEJOIN_TABLE_05 INNER join vector_mergejoin_engine.VECTOR_MERGEJOIN_TABLE_06 USING(ID) INNER join vector_mergejoin_engine.VECTOR_MERGEJOIN_TABLE_07 ON vector_mergejoin_engine.VECTOR_MERGEJOIN_TABLE_06.ID=vector_mergejoin_engine.VECTOR_MERGEJOIN_TABLE_07.ID group by vector_mergejoin_engine.VECTOR_MERGEJOIN_TABLE_05.ID order by 1 DESC NULLS LAST fetch FIRST ROW ONLY;

----
--- Clean table and resource
----
drop schema vector_mergejoin_engine cascade;
