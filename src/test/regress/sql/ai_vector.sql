create database test;
\c test
CREATE TABLE dimens3_scan_l2_100 (
    id serial PRIMARY KEY,
    name text,
    embedding vector(3)  -- 假设向量的维度为512
);
CREATE INDEX ON dimens3_scan_l2_100 USING hnsw (embedding vector_l2_ops);
INSERT INTO dimens3_scan_l2_100 (name, embedding) VALUES ('item1', '[0.211557005, 0.076130312, 0.048887434]');
INSERT INTO dimens3_scan_l2_100 (name, embedding) VALUES ('item2', '[0.822932576, 0.487093015, 0.748730227]');
INSERT INTO dimens3_scan_l2_100 (name, embedding) VALUES ('item3', '[0.564357002, 0.926498683, 0.818280197]');
INSERT INTO dimens3_scan_l2_100 (name, embedding) VALUES ('item4', '[0.791316587, 0.460895557, 0.067207831]');
INSERT INTO dimens3_scan_l2_100 (name, embedding) VALUES ('item5', '[0.974898792, 0.371204436, 0.090201541]');
INSERT INTO dimens3_scan_l2_100 (name, embedding) VALUES ('item6', '[0.972023039, 0.902695732, 0.104805143]');
INSERT INTO dimens3_scan_l2_100 (name, embedding) VALUES ('item7', '[0.340851511, 0.661115819, 0.979039013]');
INSERT INTO dimens3_scan_l2_100 (name, embedding) VALUES ('item8', '[0.492086968, 0.671430133, 0.715509519]');
INSERT INTO dimens3_scan_l2_100 (name, embedding) VALUES ('item9', '[0.815762744, 0.638665527, 0.940596042]');
INSERT INTO dimens3_scan_l2_100 (name, embedding) VALUES ('item10', '[0.598264407, 0.626925064, 0.837062887]');
INSERT INTO dimens3_scan_l2_100 (name, embedding) VALUES ('item11', '[0.605249576, 0.62827664, 0.776847171]');
INSERT INTO dimens3_scan_l2_100 (name, embedding) VALUES ('item12', '[0.608682007, 0.608462876, 0.816056557]');
INSERT INTO dimens3_scan_l2_100 (name, embedding) VALUES ('item13', '[0.361436461, 0.552522446, 0.010793276]');
INSERT INTO dimens3_scan_l2_100 (name, embedding) VALUES ('item14', '[0.856674254, 0.395471228, 0.988986555]');
INSERT INTO dimens3_scan_l2_100 (name, embedding) VALUES ('item15', '[0.694758094, 0.875162429, 0.232631982]');
INSERT INTO dimens3_scan_l2_100 (name, embedding) VALUES ('item16', '[0.021518759, 0.565872631, 0.643064199]');
INSERT INTO dimens3_scan_l2_100 (name, embedding) VALUES ('item17', '[0.766803278, 0.164139074, 0.516602078]');
INSERT INTO dimens3_scan_l2_100 (name, embedding) VALUES ('item18', '[0.764914268, 0.523333259, 0.449667669]');
INSERT INTO dimens3_scan_l2_100 (name, embedding) VALUES ('item19', '[0.212920548, 0.985840962, 0.093416858]');
INSERT INTO dimens3_scan_l2_100 (name, embedding) VALUES ('item20', '[0.789230643, 0.564697507, 0.134702261]');
INSERT INTO dimens3_scan_l2_100 (name, embedding) VALUES ('item21', '[0.717458865, 0.267429064, 0.178979577]');
INSERT INTO dimens3_scan_l2_100 (name, embedding) VALUES ('item22', '[0.509153041, 0.641964123, 0.920561135]');
INSERT INTO dimens3_scan_l2_100 (name, embedding) VALUES ('item23', '[0.431243764, 0.963082203, 0.727807302]');
INSERT INTO dimens3_scan_l2_100 (name, embedding) VALUES ('item24', '[0.827596123, 0.134412498, 0.004161737]');
INSERT INTO dimens3_scan_l2_100 (name, embedding) VALUES ('item25', '[0.258203395, 0.659945327, 0.597459666]');
INSERT INTO dimens3_scan_l2_100 (name, embedding) VALUES ('item26', '[0.944380157, 0.702332123, 0.545305512]');
set hnsw_ef_search=10;
analyze dimens3_scan_l2_100;
SELECT * FROM dimens3_scan_l2_100 ORDER BY embedding <-> '[3,1,2]' LIMIT 5;
SELECT * FROM dimens3_scan_l2_100 ORDER BY embedding <-> '[3,1,2]' LIMIT 20;
SELECT * FROM dimens3_scan_l2_100 ORDER BY embedding <-> '[3,1,2]';
explain (verbose on, costs off) SELECT /*+ indexscan(dimens3_scan_l2_100 dimens3_scan_l2_100_embedding_idx) */ * FROM dimens3_scan_l2_100 ORDER BY embedding <-> '[3,1,2]' LIMIT 5;
SELECT /*+ indexscan(dimens3_scan_l2_100 dimens3_scan_l2_100_embedding_idx) */ * FROM dimens3_scan_l2_100 ORDER BY embedding <-> '[3,1,2]' LIMIT 5;
explain (verbose on, costs off) SELECT /*+ indexscan(dimens3_scan_l2_100 dimens3_scan_l2_100_embedding_idx) */ * FROM dimens3_scan_l2_100 ORDER BY embedding <-> '[3,1,2]';
SELECT /*+ indexscan(dimens3_scan_l2_100 dimens3_scan_l2_100_embedding_idx) */ * FROM dimens3_scan_l2_100 ORDER BY embedding <-> '[3,1,2]';
prepare p_select as SELECT /*+ indexscan(dimens3_scan_l2_100 dimens3_scan_l2_100_embedding_idx) */ * FROM dimens3_scan_l2_100 ORDER BY embedding <-> $1 LIMIT $2;
execute p_select('[3,1,2]', 5);
prepare p_select1 as SELECT /*+ indexscan(dimens3_scan_l2_100 dimens3_scan_l2_100_embedding_idx) */ * FROM dimens3_scan_l2_100 ORDER BY embedding <-> '[3,1,2]' LIMIT $1;
execute p_select1(5);
prepare p_select2 as SELECT /*+ indexscan(dimens3_scan_l2_100 dimens3_scan_l2_100_embedding_idx) */ * FROM dimens3_scan_l2_100 ORDER BY embedding <-> $1 LIMIT 5;
execute p_select2('[3,1,2]');

CREATE TABLE items (id int, embedding vector(3));
INSERT INTO items VALUES (1, '[1,2,3]'), (2, '[4,5,6]'), (3, '[7,8,9]'), (4, '[10,11,12]'), (5, '[13,14,15]');
CREATE INDEX ON items USING ivfflat (embedding vector_l2_ops) WITH (lists = 100);
set enable_seqscan=off;
SELECT /*+ indexscan(embedding, items_embedding_idx) */embedding FROM items ORDER BY embedding <-> '[3,1,2]' LIMIT 5;
set ivfflat_probes = 3;
SELECT /*+ indexscan(embedding, items_embedding_idx) */embedding FROM items ORDER BY embedding <-> '[3,1,2]' LIMIT 5;

CREATE TABLE t1 (val vector(4));
INSERT INTO t1 (val) VALUES ('[0,0,0,0]'), ('[1,2,3,0]'), ('[1,1,1,0]');
INSERT INTO t1 (val) VALUES ('[4,0,0,0]'), ('[4,2,3,0]'), ('[4,1,1,0]');
INSERT INTO t1 (val) VALUES ('[25,20,20,22]'), ('[25,22,23,26]'), ('[25,21,21,21]');
INSERT INTO t1 (val) VALUES ('[24,20,20,20]'), ('[24,22,23,20]'), ('[24,21,21,20]');
CREATE INDEX ON t1 USING ivfflat (val vector_l2_ops) WITH (lists=2);
set enable_seqscan=off;
set ivfflat_probes = 1;
SELECT * FROM t1 ORDER BY val <-> '[1,2,3,4]';
\c regression
drop database test;
