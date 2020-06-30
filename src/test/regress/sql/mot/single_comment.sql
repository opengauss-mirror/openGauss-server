CREATE FOREIGN TABLE table_with_comments (id SERIAL, num INTEGER);
COMMENT ON FOREIGN TABLE table_with_comments IS 'How can I view this comments?';
COMMENT ON COLUMN table_with_comments.id IS 'This is the table''s ID';
COMMENT ON COLUMN table_with_comments.num IS 'This is the table''s number';
\d+ table_with_comments
DROP FOREIGN TABLE table_with_comments;
