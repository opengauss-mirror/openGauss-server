--
-- PGP compression support
--

select pgp_sym_decrypt(dearmor('
-----BEGIN PGP MESSAGE-----

ww0ECQMCsci6AdHnELlh0kQB4jFcVwHMJg0Bulop7m3Mi36s15TAhBo0AnzIrRFrdLVCkKohsS6+
DMcmR53SXfLoDJOv/M8uKj3QSq7oWNIp95pxfA==
=tbSn
-----END PGP MESSAGE-----
'), 'key', 'expect-compress-algo=1');

select pgp_sym_decrypt(
	pgp_sym_encrypt('Secret message', 'key', 'compress-algo=0'),
	'key', 'expect-compress-algo=0');

select pgp_sym_decrypt(
	pgp_sym_encrypt('Secret message', 'key', 'compress-algo=1'),
	'key', 'expect-compress-algo=1');

select pgp_sym_decrypt(
	pgp_sym_encrypt('Secret message', 'key', 'compress-algo=2'),
	'key', 'expect-compress-algo=2');

-- level=0 should turn compression off
select pgp_sym_decrypt(
	pgp_sym_encrypt('Secret message', 'key',
			'compress-algo=2, compress-level=0'),
	'key', 'expect-compress-algo=0');

-- max-decompressed-size option is accepted and limits decompression output
select pgp_sym_decrypt(
	pgp_sym_encrypt('Secret message', 'key', 'compress-algo=1'),
	'key', 'max-decompressed-size=1048576');

-- too small limit should fail
select pgp_sym_decrypt(
	pgp_sym_encrypt('Secret message', 'key', 'compress-algo=1'),
	'key', 'max-decompressed-size=1');

-- max-decompressed-size also limits total decrypted output for non-compressed data
select pgp_sym_decrypt_bytea(
	pgp_sym_encrypt_bytea(repeat('x', 100000), 'key'),
	'key', 'max-decompressed-size=65536');

select length(pgp_sym_decrypt_bytea(
	pgp_sym_encrypt_bytea(repeat('x', 100000), 'key'),
	'key', 'max-decompressed-size=0'));
