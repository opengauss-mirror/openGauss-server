DROP TABLE IF EXISTS repayments;
DROP TABLE IF EXISTS orders;
DROP TABLE IF EXISTS user_accounts;
DROP TABLE IF EXISTS vip_users;

CREATE TABLE vip_users (
    user_id    BIGINT PRIMARY KEY,
    user_name  VARCHAR(64),
    vip_level  VARCHAR(16)
);

CREATE TABLE user_accounts (
    account_id  BIGINT PRIMARY KEY,
    user_id     BIGINT NOT NULL,
    branch_name VARCHAR(64)
);

CREATE TABLE orders (
    order_id     BIGINT,
    user_id      BIGINT NOT NULL,
    order_amount NUMERIC(12,2)
);

CREATE TABLE repayments (
    repay_id     BIGINT,
    account_id   BIGINT NOT NULL,
    repay_amount NUMERIC(12,2),
    status       VARCHAR(16)
);

CREATE INDEX idx_vip_level      ON vip_users(vip_level);
CREATE INDEX idx_acc_user       ON user_accounts(user_id);
CREATE INDEX idx_orders_userid  ON orders(user_id);
CREATE INDEX idx_rep_acc_status ON repayments(account_id, status);

INSERT INTO vip_users
SELECT g, 'user_' || g,
       CASE WHEN g <= 100 THEN 'DIAMOND' ELSE 'NORMAL' END
FROM generate_series(1, 500) g;

INSERT INTO user_accounts
SELECT g + 1000000, g, 'branch_' || (g % 100)
FROM generate_series(1, 500) g;

INSERT INTO orders
SELECT g * 100 + s, g, (random() * 1000)::numeric(12,2)
FROM generate_series(1, 500) g, generate_series(1, 20) s;

INSERT INTO repayments
SELECT g * 100 + s, g + 1000000, (random() * 500)::numeric(12,2),
       CASE WHEN s % 3 = 0 THEN 'OVERDUE' ELSE 'NORMAL' END
FROM generate_series(1, 500) g, generate_series(1, 10) s;

ANALYZE vip_users;
ANALYZE user_accounts;
ANALYZE orders;
ANALYZE repayments;

EXPLAIN (COSTS OFF)
SELECT /*+ set(rewrite_rule predpushforce) predpush(u a) */
       u.user_id,
       u.user_name,
       a.branch_name,
       vo.total_order_amt,
       vr.total_repay_amt
FROM vip_users u
JOIN user_accounts a
  ON a.user_id = u.user_id
LEFT JOIN (
    SELECT user_id,
           SUM(order_amount) AS total_order_amt
    FROM orders
    GROUP BY user_id
) vo
  ON vo.user_id = u.user_id
LEFT JOIN (
    SELECT account_id,
           SUM(repay_amount) AS total_repay_amt
    FROM repayments
    WHERE status = 'OVERDUE'
    GROUP BY account_id
) vr
  ON vr.account_id = a.account_id
WHERE u.vip_level = 'DIAMOND';

EXPLAIN (COSTS OFF)
SELECT /*+ set(rewrite_rule predpushforce) predpush(a u) */
       u.user_id,
       u.user_name,
       a.branch_name,
       vo.total_order_amt,
       vr.total_repay_amt
FROM vip_users u
JOIN user_accounts a
  ON a.user_id = u.user_id
LEFT JOIN (
    SELECT user_id,
           SUM(order_amount) AS total_order_amt
    FROM orders
    GROUP BY user_id
) vo
  ON vo.user_id = u.user_id
LEFT JOIN (
    SELECT account_id,
           SUM(repay_amount) AS total_repay_amt
    FROM repayments
    WHERE status = 'OVERDUE'
    GROUP BY account_id
) vr
  ON vr.account_id = a.account_id
WHERE u.vip_level = 'DIAMOND';

EXPLAIN (COSTS OFF)
SELECT /*+ set(rewrite_rule predpushforce) predpush(u) */
       u.user_id,
       u.user_name,
       a.branch_name,
       vo.total_order_amt,
       vr.total_repay_amt
FROM vip_users u
JOIN user_accounts a
  ON a.user_id = u.user_id
LEFT JOIN (
    SELECT user_id,
           SUM(order_amount) AS total_order_amt
    FROM orders
    GROUP BY user_id
) vo
  ON vo.user_id = u.user_id
LEFT JOIN (
    SELECT account_id,
           SUM(repay_amount) AS total_repay_amt
    FROM repayments
    WHERE status = 'OVERDUE'
    GROUP BY account_id
) vr
  ON vr.account_id = a.account_id
WHERE u.vip_level = 'DIAMOND';



EXPLAIN (COSTS OFF)
SELECT /*+ set(rewrite_rule predpushforce) predpush(a) */
       u.user_id,
       u.user_name,
       a.branch_name,
       vo.total_order_amt,
       vr.total_repay_amt
FROM vip_users u
JOIN user_accounts a
  ON a.user_id = u.user_id
LEFT JOIN (
    SELECT user_id,
           SUM(order_amount) AS total_order_amt
    FROM orders
    GROUP BY user_id
) vo
  ON vo.user_id = u.user_id
LEFT JOIN (
    SELECT account_id,
           SUM(repay_amount) AS total_repay_amt
    FROM repayments
    WHERE status = 'OVERDUE'
    GROUP BY account_id
) vr
  ON vr.account_id = a.account_id
WHERE u.vip_level = 'DIAMOND';

EXPLAIN (COSTS OFF)
SELECT /*+ set(rewrite_rule predpushforce)*/
       u.user_id,
       u.user_name,
       a.branch_name,
       vo.total_order_amt,
       vr.total_repay_amt
FROM vip_users u
JOIN user_accounts a
  ON a.user_id = u.user_id
LEFT JOIN (
    SELECT user_id,
           SUM(order_amount) AS total_order_amt
    FROM orders
    GROUP BY user_id
) vo
  ON vo.user_id = u.user_id
LEFT JOIN (
    SELECT account_id,
           SUM(repay_amount) AS total_repay_amt
    FROM repayments
    WHERE status = 'OVERDUE'
    GROUP BY account_id
) vr
  ON vr.account_id = a.account_id
WHERE u.vip_level = 'DIAMOND';

EXPLAIN (COSTS OFF)
SELECT /*+ set(rewrite_rule none)*/
       u.user_id,
       u.user_name,
       a.branch_name,
       vo.total_order_amt,
       vr.total_repay_amt
FROM vip_users u
JOIN user_accounts a
  ON a.user_id = u.user_id
LEFT JOIN (
    SELECT user_id,
           SUM(order_amount) AS total_order_amt
    FROM orders
    GROUP BY user_id
) vo
  ON vo.user_id = u.user_id
LEFT JOIN (
    SELECT account_id,
           SUM(repay_amount) AS total_repay_amt
    FROM repayments
    WHERE status = 'OVERDUE'
    GROUP BY account_id
) vr
  ON vr.account_id = a.account_id
WHERE u.vip_level = 'DIAMOND';

DROP TABLE IF EXISTS order_repay;
CREATE TABLE order_repay (
    id         BIGINT,
    user_id    BIGINT NOT NULL,
    account_id BIGINT NOT NULL,
    amount     NUMERIC(12,2)
);
CREATE INDEX idx_or_user_acc ON order_repay(user_id, account_id);

INSERT INTO order_repay
SELECT g * 100 + s, g, g + 1000000, (random() * 1000)::numeric(12,2)
FROM generate_series(1, 500) g, generate_series(1, 20) s;

ANALYZE order_repay;

select count(*) from vip_users;
select count(*) from user_accounts;
select count(*) from order_repay;

EXPLAIN (COSTS OFF)
SELECT /*+ set(rewrite_rule predpushforce) predpush(u a) */
    u.user_id, a.branch_name, v.total_amt
FROM vip_users u
JOIN user_accounts a ON a.user_id = u.user_id
LEFT JOIN (
    SELECT user_id, account_id, SUM(amount) AS total_amt
    FROM order_repay
    GROUP BY user_id, account_id
) v ON v.user_id = u.user_id
   AND v.account_id = a.account_id
WHERE u.vip_level = 'DIAMOND';

EXPLAIN (COSTS OFF)
SELECT /*+ set(rewrite_rule predpushforce) predpush(a u) */
    u.user_id, a.branch_name, v.total_amt
FROM vip_users u
JOIN user_accounts a ON a.user_id = u.user_id
LEFT JOIN (
    SELECT user_id, account_id, SUM(amount) AS total_amt
    FROM order_repay
    GROUP BY user_id, account_id
) v ON v.user_id = u.user_id
   AND v.account_id = a.account_id
WHERE u.vip_level = 'DIAMOND';

EXPLAIN (COSTS OFF)
SELECT /*+ set(rewrite_rule predpushforce) predpush(u) */
    u.user_id, a.branch_name, v.total_amt
FROM vip_users u
JOIN user_accounts a ON a.user_id = u.user_id
LEFT JOIN (
    SELECT user_id, account_id, SUM(amount) AS total_amt
    FROM order_repay
    GROUP BY user_id, account_id
) v ON v.user_id = u.user_id
   AND v.account_id = a.account_id
WHERE u.vip_level = 'DIAMOND';

EXPLAIN (COSTS OFF)
SELECT /*+ set(rewrite_rule predpushforce) predpush(a) */
    u.user_id, a.branch_name, v.total_amt
FROM vip_users u
JOIN user_accounts a ON a.user_id = u.user_id
LEFT JOIN (
    SELECT user_id, account_id, SUM(amount) AS total_amt
    FROM order_repay
    GROUP BY user_id, account_id
) v ON v.user_id = u.user_id
   AND v.account_id = a.account_id
WHERE u.vip_level = 'DIAMOND';

EXPLAIN (COSTS OFF)
SELECT /*+ set(rewrite_rule predpushforce)*/
    u.user_id, a.branch_name, v.total_amt
FROM vip_users u
JOIN user_accounts a ON a.user_id = u.user_id
LEFT JOIN (
    SELECT user_id, account_id, SUM(amount) AS total_amt
    FROM order_repay
    GROUP BY user_id, account_id
) v ON v.user_id = u.user_id
   AND v.account_id = a.account_id
WHERE u.vip_level = 'DIAMOND';

EXPLAIN (COSTS OFF)
SELECT /*+ set(rewrite_rule none)*/
    u.user_id, a.branch_name, v.total_amt
FROM vip_users u
JOIN user_accounts a ON a.user_id = u.user_id
LEFT JOIN (
    SELECT user_id, account_id, SUM(amount) AS total_amt
    FROM order_repay
    GROUP BY user_id, account_id
) v ON v.user_id = u.user_id
   AND v.account_id = a.account_id
WHERE u.vip_level = 'DIAMOND';
