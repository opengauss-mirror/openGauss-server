-- Opt-in standard_v1; legacy indexes must keep their original semantics.
SET client_min_messages = error;
\pset format unaligned
SET enable_seqscan = off;
SET bm25_topk = 100;
CREATE TABLE bm25_standard_docs(id int PRIMARY KEY, content text);
INSERT INTO bm25_standard_docs VALUES
    (1, 'DNA dna DNA the THE a IL-6 TNF-α 2.5 ΣΟΣ İ'),
    (2, 'dna other β ² 中文'), (3, 'other β'), (4, 'rare');
CREATE INDEX bm25_standard_idx ON bm25_standard_docs USING bm25(content)
    WITH (analyzer='standard_v1');
ANALYZE bm25_standard_docs;

CREATE FUNCTION bm25_standard_ids(q text) RETURNS int[] AS $$
    SELECT array_agg(id ORDER BY id) FROM
        (SELECT id FROM bm25_standard_docs ORDER BY content <&> q DESC LIMIT 100) s;
$$ LANGUAGE SQL;
CREATE FUNCTION bm25_standard_error(command text, expected_state text) RETURNS boolean AS $$
BEGIN
    EXECUTE command;
    RETURN false;
EXCEPTION WHEN OTHERS THEN
    RETURN SQLSTATE = expected_state;
END;
$$ LANGUAGE plpgsql;

SELECT bm25_standard_ids('dNa') = ARRAY[1,2] AS case_merged;
SELECT bm25_standard_ids('the') = ARRAY[1] AS stopword_retained;
SELECT bm25_standard_ids('a') = ARRAY[1] AS single_char_retained;
SELECT bm25_standard_ids('6') = ARRAY[1] AS punctuation_split;
SELECT bm25_standard_ids('α') = ARRAY[1] AS greek_retained;
SELECT bm25_standard_ids('Β') = ARRAY[2,3] AS unicode_lowercase;
SELECT bm25_standard_ids('²') = ARRAY[2] AS unicode_numeric;
SELECT bm25_standard_ids('σοσ') = ARRAY[1] AS per_char_sigma;
SELECT bm25_standard_ids('İ') = ARRAY[1] AS expanding_lowercase;
SELECT bm25_standard_ids('中文') = ARRAY[2] AS contiguous_cjk;
SELECT bm25_standard_ids('中') IS NULL AS no_jieba_fallback;
SELECT bm25_standard_ids('!!!') IS NULL AS empty_query;
-- Independent BM25 formula: four documents, lengths 14/5/2/1, avgdl=5.5.
SELECT bool_and(abs(score - ln(1 + (4 - 2 + 0.5)/(2 + 0.5)) * tf * 2.2 /
                   (tf + 1.2 * (0.25 + 0.75 * dl / 5.5))) < 0.000001) AS exact_tf_scores
FROM (SELECT id, content <&> 'dna' AS score FROM bm25_standard_docs
      ORDER BY content <&> 'dna' DESC LIMIT 100) q
JOIN (VALUES (1, 3.0, 14.0), (2, 1.0, 5.0)) oracle(id, tf, dl) USING(id);

SELECT bm25_standard_error('ALTER INDEX bm25_standard_idx SET (analyzer=''jieba'')', '0A000') AS immutable_set;
SELECT bm25_standard_error('ALTER INDEX bm25_standard_idx RESET (analyzer)', '0A000') AS immutable_reset;
SELECT bm25_standard_error('CREATE INDEX bm25_bad_idx ON bm25_standard_docs USING bm25(content) WITH (analyzer=''unknown'')', '22023') AS reject_unknown;
SELECT bm25_standard_error('CREATE INDEX bm25_bad_idx ON bm25_standard_docs USING bm25(content) WITH (analyzer='''')', '22023') AS reject_empty;
SELECT bm25_standard_error('CREATE INDEX bm25_bad_idx ON bm25_standard_docs USING bm25(content) WITH (analyzer=''standard_v1'',dict_path='''')', '22023') AS reject_dictionary;
SELECT bm25_standard_error('INSERT INTO bm25_standard_docs VALUES (10, repeat(''x'',100))', '54000') AS reject_long_ascii;
SELECT bm25_standard_error('INSERT INTO bm25_standard_docs VALUES (10, repeat(''β'',50))', '54000') AS reject_long_utf8;
INSERT INTO bm25_standard_docs VALUES (10, repeat('x',99)), (11, repeat('β',49) || 'a');
SELECT bm25_standard_ids(repeat('x',99)) = ARRAY[10] AS max_ascii;
SELECT bm25_standard_ids(repeat('β',49) || 'a') = ARRAY[11] AS max_utf8;
INSERT INTO bm25_standard_docs VALUES (5, 'DNA DNA DNA');
UPDATE bm25_standard_docs SET content='Β' WHERE id=1;
DELETE FROM bm25_standard_docs WHERE id=3;
VACUUM bm25_standard_docs;
SELECT bm25_standard_ids('dna') = ARRAY[2,5] AS after_update_delete;
SELECT bm25_standard_ids('β') = ARRAY[1,2] AS updated_unicode;
REINDEX INDEX bm25_standard_idx;
SELECT bm25_standard_ids('dna') = ARRAY[2,5] AS after_reindex;
TRUNCATE bm25_standard_docs;
INSERT INTO bm25_standard_docs VALUES (20, 'THE the a');
SELECT bm25_standard_ids('the') = ARRAY[20] AS after_truncate;

CREATE TABLE bm25_legacy_docs(id int PRIMARY KEY, content text);
-- Include a keyword: legacy Jieba falls back to raw cut if extraction is empty.
INSERT INTO bm25_legacy_docs VALUES (1, 'the a apple'), (2, 'banana');
CREATE INDEX bm25_legacy_idx ON bm25_legacy_docs USING bm25(content);
SELECT count(*) = 0 AS legacy_stopwords_unchanged FROM
    (SELECT id FROM bm25_legacy_docs ORDER BY content <&> 'the a' DESC LIMIT 10) q;
SELECT bm25_standard_error('ALTER INDEX bm25_legacy_idx SET (analyzer=''standard_v1'')', '0A000') AS legacy_immutable;
DROP TABLE bm25_legacy_docs;
DROP FUNCTION bm25_standard_error(text, text);
DROP FUNCTION bm25_standard_ids(text);
DROP TABLE bm25_standard_docs;
RESET bm25_topk;
RESET enable_seqscan;
