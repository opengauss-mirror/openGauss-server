create extension gms_compress;
create schema gms_compress_test;
set search_path=gms_compress_test;

-- test gms_compress.compress
select gms_compress.lz_compress('123'::raw);
select gms_compress.lz_compress('df'::raw);
select gms_compress.lz_compress('12ab56'::raw);
select gms_compress.lz_compress('123'::raw, 1);
select gms_compress.lz_compress('df'::raw, 6);
select gms_compress.lz_compress('12ab56'::raw, 9);
select gms_compress.lz_compress('123'::blob);
select gms_compress.lz_compress('df'::blob);
select gms_compress.lz_compress('12ab56'::blob);
select gms_compress.lz_compress('123'::blob, 1);
select gms_compress.lz_compress('df'::blob, 6);
select gms_compress.lz_compress('12ab56'::blob, 9);


DECLARE
  content BLOB;
  r_content BLOB;
  v_handle int;
  v_raw raw;
  r_raw raw;
  v_bool boolean;
BEGIN
  content := '123';
  v_raw := '12345';
  r_content := GMS_COMPRESS.LZ_COMPRESS(content);
  r_raw := GMS_COMPRESS.LZ_COMPRESS(v_raw);
  RAISE NOTICE 'r_content=%,r_raw=%', r_content, r_raw;
  r_content := '111';
  GMS_COMPRESS.LZ_COMPRESS(content, r_content);
  RAISE NOTICE 'r_content=%', r_content;
END;
/

DECLARE
  content BLOB;
  r_content BLOB;
  v_handle int;
  v_raw raw;
  r_raw raw;
  v_bool boolean;
BEGIN
  content := 'abc';
  v_raw := 'df';
  r_content := GMS_COMPRESS.LZ_COMPRESS(content, 1);
  r_raw := GMS_COMPRESS.LZ_COMPRESS(v_raw, 9);
  RAISE NOTICE 'r_content=%,r_raw=%', r_content, r_raw;
  r_content := '111';
  GMS_COMPRESS.LZ_COMPRESS(content, r_content, 5);
  RAISE NOTICE 'r_content=%', r_content;
END;
/

-- abnormal scenario
select gms_compress.lz_compress(null::raw);
select gms_compress.lz_compress(''::raw);
select gms_compress.lz_compress('dfg'::raw);
select gms_compress.lz_compress('dfg'::raw, 5);
select gms_compress.lz_compress('123'::raw, 0);
select gms_compress.lz_compress('123'::raw, 10);
select gms_compress.lz_compress(null::blob);
select gms_compress.lz_compress(''::blob);
select gms_compress.lz_compress('dfg'::blob);
select gms_compress.lz_compress('dfg'::blob, 5);
select gms_compress.lz_compress('123'::blob, 0);
select gms_compress.lz_compress('123'::blob, 10);

DECLARE
  content BLOB;
  r_content BLOB;
  v_raw raw;
  r_raw raw;
BEGIN
  content := '';
  v_raw := 'dfg';
  r_content := GMS_COMPRESS.LZ_COMPRESS(content);
  r_raw := GMS_COMPRESS.LZ_COMPRESS(v_raw);
  RAISE NOTICE 'r_content=%,r_raw=%', r_content, r_raw;
  r_content := '111';
  GMS_COMPRESS.LZ_COMPRESS(content, r_content);
  RAISE NOTICE 'r_content=%', r_content;
END;
/

DECLARE
  content BLOB;
  r_content BLOB;
  v_raw raw;
  r_raw raw;
BEGIN
  content := 'abc';
  v_raw := 'df';
  r_content := GMS_COMPRESS.LZ_COMPRESS(content, 0);
  r_raw := GMS_COMPRESS.LZ_COMPRESS(v_raw, 10);
  RAISE NOTICE 'r_content=%,r_raw=%', r_content, r_raw;
  r_content := '111';
  GMS_COMPRESS.LZ_COMPRESS(content, r_content, -1);
  RAISE NOTICE 'r_content=%', r_content;
END;
/

-- test gms_compress.lz_uncompress
select gms_compress.lz_uncompress(gms_compress.lz_compress('123'::raw));
select gms_compress.lz_uncompress(gms_compress.lz_compress('df'::raw));
select gms_compress.lz_uncompress(gms_compress.lz_compress('12ab56'::raw));
select gms_compress.lz_uncompress(gms_compress.lz_compress('123'::raw, 1));
select gms_compress.lz_uncompress(gms_compress.lz_compress('df'::raw, 6));
select gms_compress.lz_uncompress(gms_compress.lz_compress('12ab56'::raw, 9));
select gms_compress.lz_uncompress(gms_compress.lz_compress('123'::blob));
select gms_compress.lz_uncompress(gms_compress.lz_compress('df'::blob));
select gms_compress.lz_uncompress(gms_compress.lz_compress('12ab56'::blob));
select gms_compress.lz_uncompress(gms_compress.lz_compress('123'::blob, 1));
select gms_compress.lz_uncompress(gms_compress.lz_compress('df'::blob, 6));
select gms_compress.lz_uncompress(gms_compress.lz_compress('12ab56'::blob, 9));

DECLARE
  content BLOB;
  r_content BLOB;
  v_content BLOB;
  v_bool boolean;
BEGIN
  content := '123';
  v_content := '123';
  r_content := GMS_COMPRESS.LZ_COMPRESS(content);
  GMS_COMPRESS.LZ_UNCOMPRESS(r_content, v_content);
  RAISE NOTICE 'content=%,r_content=%,v_content=%', content, r_content, v_content;
END;
/

-- abnormal scenario
select gms_compress.lz_uncompress(null::raw);
select gms_compress.lz_uncompress(''::raw);
select gms_compress.lz_uncompress('dfg'::raw);
select gms_compress.lz_uncompress('123'::raw);
select gms_compress.lz_uncompress(null::blob);
select gms_compress.lz_uncompress(''::blob);
select gms_compress.lz_uncompress('dfg'::blob);
select gms_compress.lz_uncompress('123'::blob);

DECLARE
  content BLOB;
  r_content BLOB;
  v_content BLOB;
  v_bool boolean;
BEGIN
  r_content := NULL;
  v_content := '123';
  GMS_COMPRESS.LZ_UNCOMPRESS(r_content, v_content);
  RAISE NOTICE 'content=%,r_content=%,v_content=%', content, r_content, v_content;
END;
/

DECLARE
  content BLOB;
  r_content BLOB;
  v_content BLOB;
  v_bool boolean;
BEGIN
  r_content := '123';
  v_content := '123';
  GMS_COMPRESS.LZ_UNCOMPRESS(r_content, v_content);
  RAISE NOTICE 'content=%,r_content=%,v_content=%', content, r_content, v_content;
END;
/

DECLARE
  content BLOB;
  r_content BLOB;
  v_content BLOB;
  v_bool boolean;
BEGIN
  content := '123';
  r_content := GMS_COMPRESS.LZ_COMPRESS(content);
  v_content := NULL;
  GMS_COMPRESS.LZ_UNCOMPRESS(r_content, v_content);
  RAISE NOTICE 'content=%,r_content=%,v_content=%', content, r_content, v_content;
END;
/

-- test gms_compress.lz_compress_open and ms_compress.lz_compress_close
DECLARE
  content BLOB;
  v_handle int;
BEGIN
	content := '123';
	v_handle := GMS_COMPRESS.LZ_COMPRESS_OPEN(content);
	GMS_COMPRESS.LZ_COMPRESS_CLOSE(v_handle,content);
  RAISE NOTICE 'content=%', content;
END;
/

DECLARE
  content BLOB;
  v_handle int;
BEGIN
	content := '123';
	v_handle := GMS_COMPRESS.LZ_COMPRESS_OPEN(content,5);
	GMS_COMPRESS.LZ_COMPRESS_CLOSE(v_handle,content);
  RAISE NOTICE 'content=%', content;
END;
/

DECLARE
  content BLOB;
  v_handle int;
BEGIN
	content := '123';
	v_handle := GMS_COMPRESS.LZ_COMPRESS_OPEN(content);
  v_handle := GMS_COMPRESS.LZ_COMPRESS_OPEN(content);
  v_handle := GMS_COMPRESS.LZ_COMPRESS_OPEN(content);
  v_handle := GMS_COMPRESS.LZ_COMPRESS_OPEN(content);
  v_handle := GMS_COMPRESS.LZ_COMPRESS_OPEN(content);
  GMS_COMPRESS.LZ_COMPRESS_CLOSE(1,content);
  GMS_COMPRESS.LZ_COMPRESS_CLOSE(2,content);
  GMS_COMPRESS.LZ_COMPRESS_CLOSE(3,content);
  GMS_COMPRESS.LZ_COMPRESS_CLOSE(4,content);
  GMS_COMPRESS.LZ_COMPRESS_CLOSE(5,content);
END;
/

-- abnormal scenario
DECLARE
  content BLOB;
  v_handle int;
BEGIN
	content := '';
	v_handle := GMS_COMPRESS.LZ_COMPRESS_OPEN(content);
	GMS_COMPRESS.LZ_COMPRESS_CLOSE(v_handle,content);
  RAISE NOTICE 'content=%', content;
END;
/

DECLARE
  content BLOB;
  v_handle int;
BEGIN
	content := '123';
	v_handle := GMS_COMPRESS.LZ_COMPRESS_OPEN(content, 0);
	GMS_COMPRESS.LZ_COMPRESS_CLOSE(v_handle,content);
  RAISE NOTICE 'content=%', content;
END;
/

DECLARE
  content BLOB;
  v_handle int;
BEGIN
	content := '123';
	v_handle := GMS_COMPRESS.LZ_COMPRESS_OPEN(content);
	GMS_COMPRESS.LZ_COMPRESS_CLOSE(0,content);
  RAISE NOTICE 'content=%', content;
END;
/
DECLARE
  content BLOB;
BEGIN
	GMS_COMPRESS.LZ_COMPRESS_CLOSE(1,content);
  RAISE NOTICE 'content=%', content;
END;
/

DECLARE
  content BLOB;
  v_handle int;
BEGIN
	content := '123';
	v_handle := GMS_COMPRESS.LZ_COMPRESS_OPEN(content);
	GMS_COMPRESS.LZ_COMPRESS_CLOSE(v_handle+1,content);
  RAISE NOTICE 'content=%', content;
END;
/
DECLARE
  content BLOB;
BEGIN
	GMS_COMPRESS.LZ_COMPRESS_CLOSE(1,content);
  RAISE NOTICE 'content=%', content;
END;
/

DECLARE
  content BLOB;
  v_handle int;
BEGIN
	content := '123';
	v_handle := GMS_COMPRESS.LZ_UNCOMPRESS_OPEN(content);
	GMS_COMPRESS.LZ_COMPRESS_CLOSE(v_handle);
  RAISE NOTICE 'content=%', content;
END;
/
DECLARE
  content BLOB;
BEGIN
	GMS_COMPRESS.LZ_UNCOMPRESS_CLOSE(1);
  RAISE NOTICE 'content=%', content;
END;
/

DECLARE
  content BLOB;
  v_handle int;
BEGIN
	content := '123';
	v_handle := GMS_COMPRESS.LZ_COMPRESS_OPEN(content);
  v_handle := GMS_COMPRESS.LZ_COMPRESS_OPEN(content);
  v_handle := GMS_COMPRESS.LZ_COMPRESS_OPEN(content);
  v_handle := GMS_COMPRESS.LZ_COMPRESS_OPEN(content);
  v_handle := GMS_COMPRESS.LZ_COMPRESS_OPEN(content);
  v_handle := GMS_COMPRESS.LZ_COMPRESS_OPEN(content);
  RAISE NOTICE 'content=%', content;
END;
/

DECLARE
  content BLOB;
  v_handle int;
BEGIN
	content := '123';
	GMS_COMPRESS.LZ_COMPRESS_CLOSE(1,content);
  GMS_COMPRESS.LZ_COMPRESS_CLOSE(2,content);
  GMS_COMPRESS.LZ_COMPRESS_CLOSE(3,content);
  GMS_COMPRESS.LZ_COMPRESS_CLOSE(4,content);
  GMS_COMPRESS.LZ_COMPRESS_CLOSE(5,content);
  GMS_COMPRESS.LZ_COMPRESS_CLOSE(1,content);
  RAISE NOTICE 'content=%', content;
END;
/


-- test gms_compress.lz_compress_add
DECLARE
  content BLOB;
  v_handle int;
  src raw;
BEGIN
	content := '123';
	v_handle := GMS_COMPRESS.LZ_COMPRESS_OPEN(content);
	src := '123';
	GMS_COMPRESS.LZ_COMPRESS_ADD(v_handle,content,src);
	GMS_COMPRESS.LZ_COMPRESS_CLOSE(v_handle,content);
  RAISE NOTICE 'content=%', content;
END;
/

DECLARE
  content BLOB;
  v_handle int;
  src raw;
BEGIN
	content := '123';
	v_handle := GMS_COMPRESS.LZ_COMPRESS_OPEN(content);
	src := '123';
	GMS_COMPRESS.LZ_COMPRESS_ADD(v_handle,content,src);
  GMS_COMPRESS.LZ_COMPRESS_ADD(v_handle,content,src);
	GMS_COMPRESS.LZ_COMPRESS_CLOSE(v_handle,content);
  RAISE NOTICE 'content=%', content;
END;
/

-- abnormal scenario
DECLARE
  content BLOB;
  v_handle int;
  src raw;
BEGIN
	content := '123';
	v_handle := GMS_COMPRESS.LZ_COMPRESS_OPEN(content);
	src := '123';
	GMS_COMPRESS.LZ_COMPRESS_ADD(v_handle,content,NULL);
	GMS_COMPRESS.LZ_COMPRESS_CLOSE(v_handle,content);
  RAISE NOTICE 'content=%', content;
END;
/
DECLARE
  content BLOB;
BEGIN
	GMS_COMPRESS.LZ_COMPRESS_CLOSE(1,content);
  RAISE NOTICE 'content=%', content;
END;
/

DECLARE
  content BLOB;
  v_handle int;
  src raw;
BEGIN
	content := '123';
	v_handle := GMS_COMPRESS.LZ_COMPRESS_OPEN(content);
	src := '123';
	GMS_COMPRESS.LZ_COMPRESS_ADD(0,content,src);
	GMS_COMPRESS.LZ_COMPRESS_CLOSE(v_handle,content);
  RAISE NOTICE 'content=%', content;
END;
/
DECLARE
  content BLOB;
BEGIN
	GMS_COMPRESS.LZ_COMPRESS_CLOSE(1,content);
  RAISE NOTICE 'content=%', content;
END;
/

DECLARE
  content BLOB;
  v_handle int;
  src raw;
BEGIN
	content := '123';
	v_handle := GMS_COMPRESS.LZ_COMPRESS_OPEN(content);
	src := '123';
	GMS_COMPRESS.LZ_COMPRESS_ADD(v_handle+1,content,src);
	GMS_COMPRESS.LZ_COMPRESS_CLOSE(v_handle,content);
  RAISE NOTICE 'content=%', content;
END;
/
DECLARE
  content BLOB;
BEGIN
	GMS_COMPRESS.LZ_COMPRESS_CLOSE(1,content);
  RAISE NOTICE 'content=%', content;
END;
/

-- test gms_compress.lz_uncompress_open and ms_compress.lz_uncompress_close
DECLARE
  content BLOB;
  v_handle int;
BEGIN
	content := '123';
  content := GMS_COMPRESS.LZ_COMPRESS(content);
	v_handle := GMS_COMPRESS.LZ_UNCOMPRESS_OPEN(content);
  RAISE NOTICE 'v_handle=%', v_handle;
	GMS_COMPRESS.LZ_UNCOMPRESS_CLOSE(v_handle);
  RAISE NOTICE 'content=%', content;
END;
/

DECLARE
  content BLOB;
  v_handle int;
BEGIN
	content := '123';
  content := GMS_COMPRESS.LZ_COMPRESS(content);
	v_handle := GMS_COMPRESS.LZ_UNCOMPRESS_OPEN(content);
  RAISE NOTICE 'v_handle=%', v_handle;
  v_handle := GMS_COMPRESS.LZ_UNCOMPRESS_OPEN(content);
  RAISE NOTICE 'v_handle=%', v_handle;
  v_handle := GMS_COMPRESS.LZ_UNCOMPRESS_OPEN(content);
  RAISE NOTICE 'v_handle=%', v_handle;
  v_handle := GMS_COMPRESS.LZ_UNCOMPRESS_OPEN(content);
  RAISE NOTICE 'v_handle=%', v_handle;
  v_handle := GMS_COMPRESS.LZ_UNCOMPRESS_OPEN(content);
  RAISE NOTICE 'v_handle=%', v_handle;
  GMS_COMPRESS.LZ_UNCOMPRESS_CLOSE(1);
  GMS_COMPRESS.LZ_UNCOMPRESS_CLOSE(2);
  GMS_COMPRESS.LZ_UNCOMPRESS_CLOSE(3);
  GMS_COMPRESS.LZ_UNCOMPRESS_CLOSE(4);
  GMS_COMPRESS.LZ_UNCOMPRESS_CLOSE(5);
END;
/

-- abnormal scenario
DECLARE
  content BLOB;
  v_handle int;
BEGIN
	content := '';
	v_handle := GMS_COMPRESS.LZ_UNCOMPRESS_OPEN(content);
  GMS_COMPRESS.LZ_UNCOMPRESS_CLOSE(v_handle);
END;
/

DECLARE
  content BLOB;
  v_handle int;
BEGIN
	content := '123';
	v_handle := GMS_COMPRESS.LZ_UNCOMPRESS_OPEN(content);
  GMS_COMPRESS.LZ_UNCOMPRESS_CLOSE(v_handle);
  GMS_COMPRESS.LZ_UNCOMPRESS_CLOSE(v_handle);
END;
/

DECLARE
  content BLOB;
  v_handle int;
BEGIN
	content := '123';
	v_handle := GMS_COMPRESS.LZ_UNCOMPRESS_OPEN(content);
  GMS_COMPRESS.LZ_UNCOMPRESS_CLOSE(v_handle);
  GMS_COMPRESS.LZ_UNCOMPRESS_CLOSE(0);
END;
/

DECLARE
  content BLOB;
  v_handle int;
BEGIN
	content := '123';
	v_handle := GMS_COMPRESS.LZ_UNCOMPRESS_OPEN(content);
  GMS_COMPRESS.LZ_UNCOMPRESS_CLOSE(v_handle);
  GMS_COMPRESS.LZ_UNCOMPRESS_CLOSE(2);
END;
/

DECLARE
  content BLOB;
  v_handle int;
BEGIN
	content := '123';
	v_handle := GMS_COMPRESS.LZ_COMPRESS_OPEN(content);
  GMS_COMPRESS.LZ_UNCOMPRESS_CLOSE(v_handle);
  GMS_COMPRESS.LZ_UNCOMPRESS_CLOSE(2);
END;
/
DECLARE
  content BLOB;
BEGIN
  GMS_COMPRESS.LZ_COMPRESS_CLOSE(1, content);
END;
/

DECLARE
  content BLOB;
  v_handle int;
BEGIN
	content := '123';
  content := GMS_COMPRESS.LZ_COMPRESS(content);
	v_handle := GMS_COMPRESS.LZ_UNCOMPRESS_OPEN(content);
  RAISE NOTICE 'v_handle=%', v_handle;
  v_handle := GMS_COMPRESS.LZ_UNCOMPRESS_OPEN(content);
  RAISE NOTICE 'v_handle=%', v_handle;
  v_handle := GMS_COMPRESS.LZ_UNCOMPRESS_OPEN(content);
  RAISE NOTICE 'v_handle=%', v_handle;
  v_handle := GMS_COMPRESS.LZ_UNCOMPRESS_OPEN(content);
  RAISE NOTICE 'v_handle=%', v_handle;
  v_handle := GMS_COMPRESS.LZ_UNCOMPRESS_OPEN(content);
  RAISE NOTICE 'v_handle=%', v_handle;
  v_handle := GMS_COMPRESS.LZ_UNCOMPRESS_OPEN(content);
  RAISE NOTICE 'v_handle=%', v_handle;
  GMS_COMPRESS.LZ_UNCOMPRESS_CLOSE(1);
  GMS_COMPRESS.LZ_UNCOMPRESS_CLOSE(2);
  GMS_COMPRESS.LZ_UNCOMPRESS_CLOSE(3);
  GMS_COMPRESS.LZ_UNCOMPRESS_CLOSE(4);
  GMS_COMPRESS.LZ_UNCOMPRESS_CLOSE(5);
END;
/
DECLARE
  content BLOB;
  v_handle int;
BEGIN
  GMS_COMPRESS.LZ_UNCOMPRESS_CLOSE(1);
  GMS_COMPRESS.LZ_UNCOMPRESS_CLOSE(2);
  GMS_COMPRESS.LZ_UNCOMPRESS_CLOSE(3);
  GMS_COMPRESS.LZ_UNCOMPRESS_CLOSE(4);
  GMS_COMPRESS.LZ_UNCOMPRESS_CLOSE(5);
  GMS_COMPRESS.LZ_UNCOMPRESS_CLOSE(6);
END;
/

-- test gms_compress.lz_uncompress_extract
DECLARE
  content BLOB;
  v_handle int;
  v_raw raw;
BEGIN
	content := '123';
  content := GMS_COMPRESS.LZ_COMPRESS(content);
	v_handle := GMS_COMPRESS.LZ_UNCOMPRESS_OPEN(content);
  GMS_COMPRESS.LZ_UNCOMPRESS_EXTRACT(v_handle, v_raw);
	GMS_COMPRESS.LZ_UNCOMPRESS_CLOSE(v_handle);
  RAISE NOTICE 'content=%', content;
  RAISE NOTICE 'v_raw=%', v_raw;
END;
/

-- abnormal scenario
DECLARE
  content BLOB;
  v_handle int;
  v_raw raw;
BEGIN
	content := '123';
  content := GMS_COMPRESS.LZ_COMPRESS(content);
	v_handle := GMS_COMPRESS.LZ_UNCOMPRESS_OPEN(content);
  GMS_COMPRESS.LZ_UNCOMPRESS_EXTRACT(0, v_raw);
	GMS_COMPRESS.LZ_UNCOMPRESS_CLOSE(v_handle);
  RAISE NOTICE 'content=%', content;
  RAISE NOTICE 'v_raw=%', v_raw;
END;
/
DECLARE
  content BLOB;
BEGIN
	GMS_COMPRESS.LZ_UNCOMPRESS_CLOSE(1);
END;
/

DECLARE
  content BLOB;
  v_handle int;
  v_raw raw;
BEGIN
	content := '123';
  content := GMS_COMPRESS.LZ_COMPRESS(content);
  GMS_COMPRESS.LZ_UNCOMPRESS_EXTRACT(1, v_raw);
	GMS_COMPRESS.LZ_UNCOMPRESS_CLOSE(v_handle);
  RAISE NOTICE 'content=%', content;
  RAISE NOTICE 'v_raw=%', v_raw;
END;
/

DECLARE
  content BLOB;
  v_handle int;
  v_raw raw;
BEGIN
	content := '123';
	v_handle := GMS_COMPRESS.LZ_COMPRESS_OPEN(content);
  GMS_COMPRESS.LZ_UNCOMPRESS_EXTRACT(v_handle, v_raw);
	GMS_COMPRESS.LZ_UNCOMPRESS_CLOSE(v_handle);
  RAISE NOTICE 'content=%', content;
  RAISE NOTICE 'v_raw=%', v_raw;
END;
/
DECLARE
  content BLOB;
BEGIN
	GMS_COMPRESS.LZ_COMPRESS_CLOSE(1);
END;
/

DECLARE
  content BLOB;
  v_handle int;
  v_raw raw;
BEGIN
	content := '123';
  content := GMS_COMPRESS.LZ_COMPRESS(content);
	v_handle := GMS_COMPRESS.LZ_UNCOMPRESS_OPEN(content);
  GMS_COMPRESS.LZ_UNCOMPRESS_EXTRACT(v_handle, v_raw);
  GMS_COMPRESS.LZ_UNCOMPRESS_EXTRACT(v_handle, v_raw);
	GMS_COMPRESS.LZ_UNCOMPRESS_CLOSE(v_handle);
  RAISE NOTICE 'content=%', content;
  RAISE NOTICE 'v_raw=%', v_raw;
END;
/
DECLARE
  content BLOB;
BEGIN
	GMS_COMPRESS.LZ_UNCOMPRESS_CLOSE(1);
END;
/

DECLARE
  content BLOB;
  v_handle int;
  v_raw raw;
BEGIN
	content := '123';
	v_handle := GMS_COMPRESS.LZ_UNCOMPRESS_OPEN(content);
  GMS_COMPRESS.LZ_UNCOMPRESS_EXTRACT(v_handle, v_raw);
	GMS_COMPRESS.LZ_UNCOMPRESS_CLOSE(v_handle);
  RAISE NOTICE 'content=%', content;
  RAISE NOTICE 'v_raw=%', v_raw;
END;
/
DECLARE
  content BLOB;
BEGIN
	GMS_COMPRESS.LZ_UNCOMPRESS_CLOSE(1);
END;
/

-- test gms_compress.lz_isopen
DECLARE
  content BLOB;
  v_handle int;
  v_bool boolean;
BEGIN
	content := '123';
  v_bool := false;
	v_handle := GMS_COMPRESS.LZ_COMPRESS_OPEN(content);
  v_bool := GMS_COMPRESS.ISOPEN(v_handle);
  RAISE NOTICE 'v_bool=%', v_bool;
	GMS_COMPRESS.LZ_COMPRESS_CLOSE(v_handle,content);
  v_bool := GMS_COMPRESS.ISOPEN(v_handle);
  RAISE NOTICE 'v_bool=%', v_bool;
END;
/

DECLARE
  content BLOB;
  v_handle int;
  v_bool boolean;
BEGIN
	content := '123';
  v_bool := false;
  content := GMS_COMPRESS.LZ_COMPRESS(content);
	v_handle := GMS_COMPRESS.LZ_UNCOMPRESS_OPEN(content);
  v_bool := GMS_COMPRESS.ISOPEN(v_handle);
  RAISE NOTICE 'v_bool=%', v_bool;
	GMS_COMPRESS.LZ_UNCOMPRESS_CLOSE(v_handle);
  v_bool := GMS_COMPRESS.ISOPEN(v_handle);
  RAISE NOTICE 'v_bool=%', v_bool;
END;
/

-- abnormal scenario
DECLARE
  v_bool boolean;
BEGIN
  v_bool := true;
  v_bool := GMS_COMPRESS.ISOPEN(0);
  RAISE NOTICE 'v_bool=%', v_bool;
END;
/

DECLARE
  v_bool boolean;
BEGIN
  v_bool := true;
  v_bool := GMS_COMPRESS.ISOPEN(1);
  RAISE NOTICE 'v_bool=%', v_bool;
END;
/


reset search_path;
drop schema gms_compress_test cascade;
