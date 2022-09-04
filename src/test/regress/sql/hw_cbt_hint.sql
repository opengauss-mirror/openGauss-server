/*
 * Test caser for hint is not valid for "PartIterator" where a none-unique index in such case is un-hintable
 */
EXPLAIN
SELECT
    /*+ rows(b #189) rows(a #55233) leading((a b)) indexscan(a txn_inv_transactions_t_n6) */
    a.*, b.*
FROM
    txn_inv_transactions_t a INNER JOIN txn_inv_transaction_costs_t b on a.TRANSACTION_ID = b.transaction_id
WHERE
    a.route_group_id = '1031' AND
    a.delete_flag = 'N' AND
    b.route_group_id = '1031' AND
    b.delete_flag = 'N' AND
    b.costed_flag NOT IN ('Y','X' ) AND
    ((a.organization_code = 'MY1' AND a.transaction_date >= '2022-04-01 00:00:00' AND a.transaction_date <= '2022-04-30 23:59:59'))
ORDER BY a.TRANSACTION_ID ASC LIMIT 1000 OFFSET 0;

EXPLAIN
SELECT
    /*+ set(enable_hashjoin off) rows(b #189) rows(a #55233) leading((a b)) indexscan(a txn_inv_transactions_t_n6) */
    a.*, b.*
FROM
    txn_inv_transactions_t a INNER JOIN txn_inv_transaction_costs_t b on a.TRANSACTION_ID = b.transaction_id
WHERE
    a.route_group_id = '1031' AND
    a.delete_flag = 'N' AND
    b.route_group_id = '1031' AND
    b.delete_flag = 'N' AND
    b.costed_flag NOT IN ('Y','X' ) AND
    ((a.organization_code = 'MY1' AND a.transaction_date >= '2022-04-01 00:00:00' AND a.transaction_date <= '2022-04-30 23:59:59'))
ORDER BY a.TRANSACTION_ID ASC LIMIT 1000 OFFSET 0;
