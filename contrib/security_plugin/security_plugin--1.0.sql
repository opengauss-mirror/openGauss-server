
create or replace function pg_catalog.creditcardmasking(col text,letter char default 'x') RETURNS text AS $$
declare 
    size INTEGER := 4;
begin
    return CASE WHEN pg_catalog.length(col) >= size THEN
        pg_catalog.REGEXP_REPLACE(pg_catalog.left(col, size*(-1)), '[\d+]', letter, 'g') || pg_catalog.right(col, size)
    ELSE
        col
    end;
end;
$$ LANGUAGE plpgsql;

create or replace function pg_catalog.basicemailmasking(col text, letter char default 'x') RETURNS text AS $$
declare 
    pos INTEGER := position('@' in col);
begin
    return CASE WHEN pos > 1 THEN
    pg_catalog.repeat(letter, pos - 1) || pg_catalog.substring(col, pos, pg_catalog.length(col) - pos +1)
    ELSE
        col
    end;
end;
$$ LANGUAGE plpgsql;

create or replace function pg_catalog.fullemailmasking(col text, letter char default 'x') RETURNS text AS $$
declare 
    pos INTEGER := position('@' in col);
    dot_pos INTEGER := pg_catalog.length(col) - position('.' in pg_catalog.reverse(col)) + 1;
begin
    return CASE WHEN pos > 2 and dot_pos > pos THEN
    pg_catalog.repeat(letter, pos - 1) || '@' || pg_catalog.repeat(letter,  dot_pos - pos - 1) || pg_catalog.substring(col, dot_pos, pg_catalog.length(col) - dot_pos +1)
    ELSE
        col
    end;
end;
$$ LANGUAGE plpgsql;

create or replace function pg_catalog.alldigitsmasking(col text, letter char default '0') RETURNS text AS $$
begin
    return pg_catalog.REGEXP_REPLACE(col, '[\d+]', letter, 'g');
end;
$$ LANGUAGE plpgsql;

create or replace function pg_catalog.shufflemasking(col text) RETURNS text AS $$
declare 
    index INTEGER := 0;
    rd INTEGER;
    size INTEGER := pg_catalog.length(col);
    tmp text := col;
    res text;
begin
    while size > 0 loop 
        rd := pg_catalog.floor(pg_catalog.random() * pg_catalog.length(tmp) + 1);
        res := res || pg_catalog.right(pg_catalog.left(tmp, rd), 1);
        tmp := pg_catalog.left(tmp, rd - 1) || pg_catalog.right(tmp, pg_catalog.length(tmp) - rd);
        size := size - 1;
    END loop;
    return res;
end;
$$ LANGUAGE plpgsql;

create or replace function pg_catalog.randommasking(col text) RETURNS text AS $$	
begin
    return pg_catalog.left(pg_catalog.MD5(pg_catalog.random()::text), pg_catalog.length(col));
end;
$$ LANGUAGE plpgsql;

create or replace function pg_catalog.regexpmasking(col text, reg text, replace_text text, pos INTEGER default 0, reg_len INTEGER default -1) RETURNS text AS $$
declare
	size INTEGER := pg_catalog.length(col);
	endpos INTEGER;
	startpos INTEGER;
	lstr text;
	rstr text;
	ltarget text;
begin
	startpos := pos;
	IF pos < 0 THEN startpos := 0; END IF;
	IF pos >= size THEN startpos := size; END IF;
	endpos := reg_len + startpos - 1;
	IF reg_len < 0 THEN endpos := size - 1; END IF;
	IF reg_len + startpos >= size THEN endpos := size - 1; END IF;
	lstr := pg_catalog.left(col, startpos);
	rstr := pg_catalog.right(col, size - endpos - 1);
	ltarget := pg_catalog.substring(col, startpos+1, endpos - startpos + 1);
    ltarget := pg_catalog.REGEXP_REPLACE(ltarget, reg, replace_text, 'g');
    return lstr || ltarget || rstr;
end;
$$ LANGUAGE plpgsql;
