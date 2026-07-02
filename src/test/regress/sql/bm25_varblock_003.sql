-- BM25 VarBlock: stress test offset corruption when freeing interleaved chunks.
--
-- Scenario: rows 1-5000 and 15001-20000 have 4 tokens, rows 5001-15000 have 3 tokens.
-- This creates uneven chain lengths: durian has 10000 postings, others have 20000.
-- Deleting 1-5000 frees durian's first ~half of chain, which shares pages with
-- other tokens' chains. If VarBlockFreeChain corrupts other chains' offsets,
-- subsequent queries after VACUUM will return wrong results or crash.
SET enable_seqscan = off;
SET enable_indexscan = on;

DROP TABLE IF EXISTS bm25_varblock_003;

CREATE TABLE bm25_varblock_003 (
    id int PRIMARY KEY,
    content text
);

-- Insert 1-5000 with 4 tokens
INSERT INTO bm25_varblock_003(id, content)
SELECT g, 'apple banana cherry durian'
FROM generate_series(1, 5000) g;

-- Insert 5001-15000 with 3 tokens (no durian)
INSERT INTO bm25_varblock_003(id, content)
SELECT g, 'apple banana cherry'
FROM generate_series(5001, 15000) g;

-- Insert 15001-20000 with 4 tokens
INSERT INTO bm25_varblock_003(id, content)
SELECT g, 'apple banana cherry durian'
FROM generate_series(15001, 20000) g;

CREATE INDEX bm25_varblock_003_bm25_idx ON bm25_varblock_003 USING bm25(content);

-- Delete 1-5000: frees durian's first 5000 postings (entire initial chunk range)
DELETE FROM bm25_varblock_003 WHERE id BETWEEN 1 AND 5000;

-- Advance transaction horizon so VACUUM can reclaim.
SELECT pg_sleep(2) AS sleep1;
INSERT INTO bm25_varblock_003(id, content) VALUES (-1, 'refresh1');
SELECT pg_sleep(2) AS sleep2;
INSERT INTO bm25_varblock_003(id, content) VALUES (-2, 'refresh2');

-- Query before VACUUM: verify all tokens return correct results.
COPY (SELECT /*+ indexscan(bm25_varblock_003 bm25_varblock_003_bm25_idx) */ id
      FROM bm25_varblock_003
      ORDER BY content <&> 'cherry' DESC
      LIMIT 3) TO STDOUT;
COPY (SELECT /*+ indexscan(bm25_varblock_003 bm25_varblock_003_bm25_idx) */ id
      FROM bm25_varblock_003
      ORDER BY content <&> 'durian' DESC
      LIMIT 3) TO STDOUT;

-- VACUUM — frees chains, potentially corrupting shared page offsets.
VACUUM bm25_varblock_003;

-- After VACUUM: total count should be correct.
SELECT COUNT(*) AS total_rows FROM bm25_varblock_003;

-- After VACUUM: index scan should still return correct results.
COPY (SELECT /*+ indexscan(bm25_varblock_003 bm25_varblock_003_bm25_idx) */ id
      FROM bm25_varblock_003
      ORDER BY content <&> 'cherry' DESC
      LIMIT 3) TO STDOUT;
COPY (SELECT /*+ indexscan(bm25_varblock_003 bm25_varblock_003_bm25_idx) */ id
      FROM bm25_varblock_003
      ORDER BY content <&> 'durian' DESC
      LIMIT 3) TO STDOUT;

DROP TABLE IF EXISTS bm25_varblock_003;