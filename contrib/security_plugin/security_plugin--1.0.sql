
create or replace function pg_catalog.creditcardmasking(col text,letter char default 'x') RETURNS text AS $$
declare 
    size INTEGER := 4;
begin
    return CASE WHEN length(col) >= size THEN
        REGEXP_REPLACE(left(col, size*(-1)), '[\d+]', letter, 'g') || right(col, size)
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
    repeat(letter, pos - 1) || substring(col, pos, length(col) - pos +1)
    ELSE
        col
    end;
end;
$$ LANGUAGE plpgsql;

create or replace function pg_catalog.fullemailmasking(col text, letter char default 'x') RETURNS text AS $$
declare 
    pos INTEGER := position('@' in col);
    dot_pos INTEGER := length(col) - position('.' in reverse(col)) + 1;
begin
    return CASE WHEN pos > 2 and dot_pos > pos THEN
    repeat(letter, pos - 1) || '@' || repeat(letter,  dot_pos - pos - 1) || substring(col, dot_pos, length(col) - dot_pos +1)
    ELSE
        col
    end;
end;
$$ LANGUAGE plpgsql;

create or replace function pg_catalog.alldigitsmasking(col text, letter char default '0') RETURNS text AS $$
begin
    return REGEXP_REPLACE(col, '[\d+]', letter, 'g');
end;
$$ LANGUAGE plpgsql;

create or replace function pg_catalog.shufflemasking(col text) RETURNS text AS $$
declare 
    index INTEGER := 0;
    rd INTEGER;
    size INTEGER := length(col);
    tmp text := col;
    res text;
begin
    while size > 0 loop 
        rd := floor(random() * length(tmp) + 1);
        res := res || right(left(tmp, rd), 1);
        tmp := left(tmp, rd - 1) || right(tmp, length(tmp) - rd);
        size := size - 1;
    END loop;
    return res;
end;
$$ LANGUAGE plpgsql;

create or replace function pg_catalog.randommasking(col text) RETURNS text AS $$	
begin
    return left(MD5(random()::text), length(col));
end;
$$ LANGUAGE plpgsql;

create or replace function pg_catalog.regexpmasking(col text, reg text, replace_text text, pos INTEGER default 0, reg_len INTEGER default -1) RETURNS text AS $$
declare
	size INTEGER := length(col);
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
	lstr := left(col, startpos);
	rstr := right(col, size - endpos - 1);
	ltarget := substring(col, startpos+1, endpos - startpos + 1);
    ltarget := pg_catalog.REGEXP_REPLACE(ltarget, reg, replace_text, 'g');
    return lstr || ltarget || rstr;
end;
$$ LANGUAGE plpgsql;
