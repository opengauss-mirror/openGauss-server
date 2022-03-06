-- join list occurs any error when optimizing multi-columns statistics using eqClass.
CREATE SCHEMA equivalent_class;
SET current_schema = equivalent_class;
CREATE TABLE dim_warehouse_info_t (
    warehouse_id numeric(10,0),
    warehouse_name character varying(60)
)
WITH (orientation=row, compression=no);

CREATE TABLE wms_abnormal_order (
    id bigint,
    abnormal_order_no character varying(384),
    abnormal_type character varying(384),
    warehouse_id numeric(20,0)
)
WITH (orientation=column, compression=middle);

CREATE TABLE wms_stocktaking_merchandise (
    id bigint,
    stocktaking_serno character varying(90),
    warehouse_id numeric(20,0),
    abnormal_order_no character varying(384)
)
WITH (orientation=column, compression=middle);

CREATE TABLE wms_stocktaking_order (
    id bigint,
    stocktaking_serno character varying(90),
    stocktaking_type character varying(384),
    warehouse_id numeric(20,0)
)
WITH (orientation=column, compression=middle);

SET enable_nestloop = off;
SET explain_perf_mode=pretty;
EXPLAIN(costs off) SELECT /* leading((mer (ab (wh ord)))) leading((ab (wh ord)))*/
    mer.abnormal_order_no ,
    ab.abnormal_order_no 
FROM 
    wms_stocktaking_merchandise mer
LEFT JOIN 
    dim_warehouse_info_t wh
ON 
    wh.warehouse_id = mer.warehouse_id
LEFT JOIN 
    wms_stocktaking_order ord
ON 
    ord.warehouse_id = mer.warehouse_id
AND
    ord.stocktaking_serno = mer.stocktaking_serno
LEFT JOIN wms_abnormal_order ab
    ON ab.warehouse_id = mer.warehouse_id
    AND ab.abnormal_order_no = mer.abnormal_order_no
AND ab.abnormal_order_no IN ('AB00000000194178', 'AB00000000194175')
WHERE ord.stocktaking_type = 'AF'
AND mer.abnormal_order_no IS NOT NULL
AND ab.abnormal_type IN ('PICK_ABNORMAL','SORTING_ABNORMAL','PACK_ABNORMAL')
AND wh.warehouse_name ='UKGF Warehouse'
AND mer.abnormal_order_no IN ('AB00000000194178'   ,'AB00000000194175'   )
GROUP BY
mer.abnormal_order_no ,
ab.abnormal_order_no ;

DROP SCHEMA equivalent_class CASCADE;
