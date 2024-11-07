create extension gms_i18n;
create schema gms_i18n_test;
set search_path=gms_i18n_test;

-- test gms_i18n.raw_to_char
select gms_i18n.raw_to_char(hextoraw('616263646566C2AA'), 'utf8');
select gms_i18n.raw_to_char(hextoraw('e6b58be8af95'), 'utf8');
select gms_i18n.raw_to_char(hextoraw('e6b58be8af95'), '');
select gms_i18n.raw_to_char(hextoraw('e6b58be8af95'));
select gms_i18n.raw_to_char('', 'utf8');
select gms_i18n.raw_to_char('', '');
select gms_i18n.raw_to_char('');
select gms_i18n.raw_to_char(hextoraw('e6b58be8af95'), 'unvalid_charset');
select gms_i18n.raw_to_char(hextoraw('b2e2cad4'), 'gbk');
select gms_i18n.raw_to_char(hextoraw('b2e2cad4'), 'euc_cn');
select gms_i18n.raw_to_char(hextoraw('b4fab8d5'), 'big5');
select gms_i18n.raw_to_char();

-- test gms_i18n.string_to_raw
select gms_i18n.string_to_raw('abcdefª', 'utf8');
select gms_i18n.string_to_raw('测试', 'utf8');
select gms_i18n.string_to_raw('测试', '');
select gms_i18n.string_to_raw('测试');
select gms_i18n.string_to_raw('', 'utf8');
select gms_i18n.string_to_raw('', '');
select gms_i18n.string_to_raw('');
select gms_i18n.string_to_raw('测试', 'unvalid_charset');
select gms_i18n.string_to_raw('测试', 'gbk');
select gms_i18n.string_to_raw('测试', 'euc_cn');
select gms_i18n.string_to_raw('測試', 'big5');
select gms_i18n.string_to_raw();

reset search_path;
drop schema gms_i18n_test cascade;
