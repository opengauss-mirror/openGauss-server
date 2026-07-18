--
-- check static and global data (SD and GD), session 间不共享
--

CREATE FUNCTION global_test_three() returns text
    AS
'if "global_test" not in SD:
	SD["global_test"] = "set by global_test_three"
if "global_test" not in GD:
	GD["global_test"] = "set by global_test_three"
return "SD: " + SD["global_test"] + ", GD: " + GD["global_test"]'
    LANGUAGE plpython3u;

SELECT static_test();
SELECT static_test();
SELECT global_test_three();
SELECT global_test_two();
SELECT global_test_one();
