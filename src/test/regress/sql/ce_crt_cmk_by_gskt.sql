-------------------------------------------------------------------------------------------------------------------------
-- grop     : security
-- module   : client encrypt 
--
-- function : test {sql：CREATE CEK}
--      CREATE CLIENT MASTER KEY $cmk WITH (KEY_STORE = $key_store, KEY_PATH = "$key_id" , ALGORITHM = $algo);
--
-- dependency : 
--      tool  : gs_ktool (sorce code: src/bin/gs_ktool)
-------------------------------------------------------------------------------------------------------------------------

-- prepare | succeed
\! gs_ktool -d all
\! gs_ktool -g && gs_ktool -g && gs_ktool -g && gs_ktool -g && gs_ktool -g

-- create cmk | succeed
CREATE CLIENT MASTER KEY cmk1 WITH (KEY_STORE = gs_ktool, KEY_PATH = "gs_ktool/1" , ALGORITHM = AES_256_CBC);
CREATE CLIENT MASTER KEY cmk2 WITH (KEY_STORE = gs_ktool, KEY_PATH = "gs_ktool/2" , ALGORITHM = SM4);
CREATE CLIENT MASTER KEY cmk5 WITH (KEY_STORE = gs_ktool, KEY_PATH = "gs_ktool/5" , ALGORITHM = AES_256_CBC);

-- drop cmk | succeed
DROP CLIENT MASTER KEY cmk1;
DROP CLIENT MASTER KEY cmk2;

-- create after drop cmk | succeed
CREATE CLIENT MASTER KEY cmk1 WITH (KEY_STORE = gs_ktool, KEY_PATH = "gs_ktool/1", ALGORITHM = AES_256_CBC);
CREATE CLIENT MASTER KEY cmk2 WITH (KEY_STORE = gs_ktool, KEY_PATH = "gs_ktool/2", ALGORITHM = SM4);
DROP CLIENT MASTER KEY cmk1;
CREATE CLIENT MASTER KEY cmk1 WITH (KEY_STORE = gs_ktool, KEY_PATH = "gs_ktool/4", ALGORITHM = AES_256_CBC);
CREATE CLIENT MASTER KEY cmk4 WITH (KEY_STORE = gs_ktool, KEY_PATH = "gs_ktool/1", ALGORITHM = AES_256_CBC);

-- prepare | succeed
\! gs_ktool -d all
DROP CLIENT MASTER KEY cmk1;
DROP CLIENT MASTER KEY cmk2;
DROP CLIENT MASTER KEY cmk4;
DROP CLIENT MASTER KEY cmk5;
\! gs_ktool -g && gs_ktool -g && gs_ktool -g && gs_ktool -g && gs_ktool -g && gs_ktool -g

-- in word "ecmk", 'e' means 'error'
-- create cmk | invalid cmk object name | error
CREATE CLIENT MASTER KEY ecmk 1 WITH (KEY_STORE = gs_ktool, KEY_PATH = "gs_ktool/1" , ALGORITHM = AES_256_CBC);
CREATE CLIENT MASTER KEY ecmk 1 WITH (KEY_STORE = gs_ktool, KEY_PATH = "gs_ktool/1" , ALGORITHM = AES_256_CBC);
CREATE CLIENT MASTER KEY ecmk ecmk WITH (KEY_STORE = gs_ktool, KEY_PATH = "gs_ktool/1" , ALGORITHM = AES_256_CBC);
CREATE CLIENT MASTER KEY "ecmk" ecmk WITH (KEY_STORE = gs_ktool, KEY_PATH = "gs_ktool/1" , ALGORITHM = AES_256_CBC);
CREATE CLIENT MASTER KEY . WITH (KEY_STORE = gs_ktool, KEY_PATH = "gs_ktool/1" , ALGORITHM = AES_256_CBC);
CREATE CLIENT MASTER KEY 你 WITH (KEY_STORE = gs_ktool, KEY_PATH = "gs_ktool/1" , ALGORITHM = AES_256_CBC);

-- create cmk | loss args | error
CREATE CLIENT MASTER KEY ecmk1 WITH (KEY_STORE = gs_ktool);
CREATE CLIENT MASTER KEY ecmk2 WITH (KEY_PATH = "gs_ktool/1");
CREATE CLIENT MASTER KEY ecmk3 WITH (ALGORITHM = AES_256_CBC);
CREATE CLIENT MASTER KEY ecmk4 WITH (KEY_PATH = "gs_ktool/2", ALGORITHM = AES_256_CBC);
CREATE CLIENT MASTER KEY ecmk5 WITH (KEY_STORE = gs_ktool, ALGORITHM = AES_256_CBC);
CREATE CLIENT MASTER KEY ecmk6 WITH (KEY_STORE = gs_ktool, KEY_PATH = "gs_ktool/3");
CREATE CLIENT MASTER KEY ecmk7 WITH (KEY_PATH = "gs_ktool/4", KEY_PATH = "gs_ktool/4", ALGORITHM = AES_256_CBC);
CREATE CLIENT MASTER KEY ecmk8 WITH (KEY_STORE = gs_ktool, KEY_STORE = gs_ktool, ALGORITHM = AES_256_CBC);
CREATE CLIENT MASTER KEY ecmk9 WITH (KEY_STORE = gs_ktool, KEY_PATH = "gs_ktool/5", KEY_PATH = "gs_ktool/5");
CREATE CLIENT MASTER KEY ecmk10 WITH (KEY_STORE = gs_ktool, KEY_PATH = "gs_ktool/5", KEY_PATH = "gs_ktool/6");
CREATE CLIENT MASTER KEY ecmk11 WITH (KEY_STORE = gs_ktool, KEY_STORE = gs_ktool, KEY_STORE = gs_ktool);
CREATE CLIENT MASTER KEY ecmk12 WITH (KEY_STORE = gs_ktool, KEY_STORE = gs_ktool, KEY_STORE = gs_ktool, KEY_STORE = gs_ktool, KEY_STORE = gs_ktool, KEY_STORE = gs_ktool, KEY_STORE = gs_ktool, KEY_STORE = gs_ktool, KEY_STORE = gs_ktool);

-- create cmk | redundant args | error
CREATE CLIENT MASTER KEY ecmk20 WITH (KEY_STORE = gs_ktool, KEY_STORE = gs_ktool, KEY_PATH = "gs_ktool/1", ALGORITHM = AES_256_CBC);
CREATE CLIENT MASTER KEY ecmk21 WITH (KEY_STORE = gs_ktool, KEY_PATH = "gs_ktool/2", KEY_PATH = "gs_ktool/2", ALGORITHM = AES_256_CBC);
CREATE CLIENT MASTER KEY ecmk22 WITH (KEY_STORE = gs_ktool, KEY_PATH = "gs_ktool/3", KEY_PATH = "gs_ktool/4", ALGORITHM = AES_256_CBC);
CREATE CLIENT MASTER KEY ecmk23 WITH (KEY_STORE = gs_ktool, KEY_PATH = "gs_ktool/5", ALGORITHM = AES_256_CBC, ALGORITHM = AES_256_CBC);
CREATE CLIENT MASTER KEY ecmk24 WITH (KEY_STORE = gs_ktool, KEY_PATH = "gs_ktool/5", ALGORITHM = AES_256_CBC, ALGORITHM = AES_256_CBC, ALGORITHM = AES_256_CBC, ALGORITHM = AES_256_CBC);
CREATE CLIENT MASTER KEY ecmk25 WITH (KEY_STORE = gs_ktool, KEY_STORE = gs_ktool, KEY_PATH = "gs_ktool/1", KEY_PATH = "gs_ktool/1", ALGORITHM = AES_256_CBC, ALGORITHM = AES_256_CBC);

-- create cmk | invalid args | error
CREATE CLIENT MASTER KEY ecmk40 WITH (KEY_STORE = , KEY_PATH = "gs_ktool/1", ALGORITHM = AES_256_CBC);
CREATE CLIENT MASTER KEY ecmk41 WITH (KEY_STORE = gs, KEY_PATH = "gs_ktool/2", ALGORITHM = AES_256_CBC);
CREATE CLIENT MASTER KEY ecmk42 WITH (KEY_STORE = gs_ktooll, KEY_PATH = "gs_ktool/3", ALGORITHM = AES_256_CBC);
CREATE CLIENT MASTER KEY ecmk43 WITH (KEY_STORE = gs_ktoal, KEY_PATH = "gs_ktool/4", ALGORITHM = AES_256_CBC);
CREATE CLIENT MASTER KEY ecmk44 WITH (KEY_STORE = "gs_ktoal", KEY_PATH = "gs_ktool/4", ALGORITHM = AES_256_CBC);
CREATE CLIENT MASTER KEY ecmk45 WITH (KEY_STORE = gs_ktoolllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllll1111111111111111111111111111111111, KEY_PATH = "gs_ktool/5", ALGORITHM = AES_256_CBC);
CREATE CLIENT MASTER KEY ecmk46 WITH (KEY_STORE = 很, KEY_PATH = "gs_ktool/1", ALGORITHM = AES_256_CBC);
CREATE CLIENT MASTER KEY ecmk47 WITH (KEY_STORE = ，, KEY_PATH = "gs_ktool/2", ALGORITHM = AES_256_CBC);
-- --
CREATE CLIENT MASTER KEY ecmk60 WITH (KEY_STORE = gs_ktool, KEY_PATH = , ALGORITHM = AES_256_CBC);
CREATE CLIENT MASTER KEY ecmk61 WITH (KEY_STORE = gs_ktool, KEY_PATH = "g", ALGORITHM = );
CREATE CLIENT MASTER KEY ecmk62 WITH (KEY_STORE = gs_ktool, KEY_PATH = "gs_ktoo/1", ALGORITHM = AES_256_CBC);
CREATE CLIENT MASTER KEY ecmk63 WITH (KEY_STORE = gs_ktool, KEY_PATH = "gs_ktool", ALGORITHM = AES_256_CBC);
CREATE CLIENT MASTER KEY ecmk64 WITH (KEY_STORE = gs_ktool, KEY_PATH = "gs_ktool/", ALGORITHM = AES_256_CBC);
CREATE CLIENT MASTER KEY ecmk65 WITH (KEY_STORE = gs_ktool, KEY_PATH = "gs_ktooll/1", ALGORITHM = AES_256_CBC);
CREATE CLIENT MASTER KEY ecmk66 WITH (KEY_STORE = gs_ktool, KEY_PATH = "gs_ktoal/2", ALGORITHM = AES_256_CBC);
CREATE CLIENT MASTER KEY ecmk67 WITH (KEY_STORE = gs_ktool, KEY_PATH = "gs_ktool3", ALGORITHM = AES_256_CBC);
CREATE CLIENT MASTER KEY ecmk68 WITH (KEY_STORE = gs_ktool, KEY_PATH = "gs_ktool//4", ALGORITHM = AES_256_CBC);
CREATE CLIENT MASTER KEY ecmk69 WITH (KEY_STORE = gs_ktool, KEY_PATH = "gs_ktool/\", ALGORITHM = AES_256_CBC);
CREATE CLIENT MASTER KEY ecmk70 WITH (KEY_STORE = gs_ktool, KEY_PATH = "gs_ktool/5.", ALGORITHM = AES_256_CBC);
CREATE CLIENT MASTER KEY ecmk71 WITH (KEY_STORE = gs_ktool, KEY_PATH = "gs_ktool/.", ALGORITHM = AES_256_CBC);
CREATE CLIENT MASTER KEY ecmk72 WITH (KEY_STORE = gs_ktool, KEY_PATH = "gs_ktool/6/", ALGORITHM = AES_256_CBC);
CREATE CLIENT MASTER KEY ecmk73 WITH (KEY_STORE = gs_ktool, KEY_PATH = "gs_ktool/闲", ALGORITHM = AES_256_CBC);
CREATE CLIENT MASTER KEY ecmk74 WITH (KEY_STORE = gs_ktool, KEY_PATH = "gs_ktoolllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllll/1", ALGORITHM = AES_256_CBC);
CREATE CLIENT MASTER KEY ecmk75 WITH (KEY_STORE = gs_ktool, KEY_PATH = "gs_ktool/5555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555", ALGORITHM = AES_256_CBC);
-- --
CREATE CLIENT MASTER KEY ecmk80 WITH (KEY_STORE = gs_ktool, KEY_PATH = "gs_ktool/1", ALGORITHM =);
CREATE CLIENT MASTER KEY ecmk81 WITH (KEY_STORE = gs_ktool, KEY_PATH = "gs_ktool/1", ALGORITHM = AES);
CREATE CLIENT MASTER KEY ecmk82 WITH (KEY_STORE = gs_ktool, KEY_PATH = "gs_ktool/1", ALGORITHM = AES_256);
CREATE CLIENT MASTER KEY ecmk83 WITH (KEY_STORE = gs_ktool, KEY_PATH = "gs_ktool/1", ALGORITHM = AES_256_CFB);
CREATE CLIENT MASTER KEY ecmk84 WITH (KEY_STORE = gs_ktool, KEY_PATH = "gs_ktool/1", ALGORITHM = AES_128_CBC);
CREATE CLIENT MASTER KEY ecmk85 WITH (KEY_STORE = gs_ktool, KEY_PATH = "gs_ktool/1", ALGORITHM = RSA_2048);
CREATE CLIENT MASTER KEY ecmk86 WITH (KEY_STORE = gs_ktool, KEY_PATH = "gs_ktool/1", ALGORITHM = RSA_3072);
CREATE CLIENT MASTER KEY ecmk87 WITH (KEY_STORE = gs_ktool, KEY_PATH = "gs_ktool/1", ALGORITHM = AES_256_CBCB);
CREATE CLIENT MASTER KEY ecmk88 WITH (KEY_STORE = gs_ktool, KEY_PATH = "gs_ktool/1", ALGORITHM = "AES_256_CBC\0");
CREATE CLIENT MASTER KEY ecmk89 WITH (KEY_STORE = gs_ktool, KEY_PATH = "gs_ktool/1", ALGORITHM = .);
CREATE CLIENT MASTER KEY ecmk90 WITH (KEY_STORE = gs_ktool, KEY_PATH = "gs_ktool/1", ALGORITHM = AES_256_CBCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC);
CREATE CLIENT MASTER KEY ecmk91 WITH (KEY_STORE = gs_ktool, KEY_PATH = "gs_ktool/1", ALGORITHM = 的);
-- create cmk | invalid keys | error
CREATE CLIENT MASTER KEY ecmk100 WITH (gs_ktool, KEY_PATH = "gs_ktool/1" , ALGORITHM = AES_256_CBC);
CREATE CLIENT MASTER KEY ecmk101 WITH (KEY_STOR = gs_ktool, KEY_PATH = "gs_ktool/2" , ALGORITHM = AES_256_CBC);
CREATE CLIENT MASTER KEY ecmk102 WITH (KEY_STORE = gs_ktool, KEY = "gs_ktool/3" , ALGORITHM = AES_256_CBC);
CREATE CLIENT MASTER KEY ecmk103 WITH (KEY_STORE = gs_ktool, KEY_PATHH = "gs_ktool/4" , ALGORITHM = AES_256_CBC);
CREATE CLIENT MASTER KEY ecmk104 WITH (KEY_STORE = gs_ktool, KEY_PATH = "gs_ktool/5" , ALGORITHMA = AES_256_CBC);
CREATE CLIENT MASTER KEY ecmk105 WITH (KEY_STORE = gs_ktool, KEY_PATH = "gs_ktool/6" , = AES_256_CBC);
CREATE CLIENT MASTER KEY ecmk106 WITH (KEY_STORE = gs_ktool, 吗 = "gs_ktool/1" , = AES_256_CBC);
CREATE CLIENT MASTER KEY ecmk107 WITH (KEY_STOR = gs_ktool, KEY_STOR = "gs_ktool/2" , ALGORITHM = AES_256_CBC);
CREATE CLIENT MASTER KEY ecmk108 WITH (KEY_STORE = gs_ktool, KEY_PATH = "gs_ktool/1" , ALGORITHM = AES_256_CBC, YES = 1);

-- prepare | succeed
\! gs_ktool -d all
\! gs_ktool -g && gs_ktool -g && gs_ktool -g && gs_ktool -g && gs_ktool -g && gs_ktool -g
CREATE CLIENT MASTER KEY cmk1 WITH (KEY_STORE = gs_ktool, KEY_PATH = "gs_ktool/1" , ALGORITHM = AES_256_CBC);
CREATE CLIENT MASTER KEY cmk2 WITH (KEY_STORE = gs_ktool, KEY_PATH = "gs_ktool/2" , ALGORITHM = AES_256_CBC);
CREATE CLIENT MASTER KEY cmk3 WITH (KEY_STORE = gs_ktool, KEY_PATH = "gs_ktool/3" , ALGORITHM = AES_256_CBC);
CREATE CLIENT MASTER KEY cmk4 WITH (KEY_STORE = gs_ktool, KEY_PATH = "gs_ktool/4" , ALGORITHM = AES_256_CBC);
CREATE CLIENT MASTER KEY cmk5 WITH (KEY_STORE = gs_ktool, KEY_PATH = "gs_ktool/5" , ALGORITHM = AES_256_CBC);

-- create cmk | unserviceable args | error
CREATE CLIENT MASTER KEY cmk1 WITH (KEY_STORE = gs_ktool, KEY_PATH = "gs_ktool/6" , ALGORITHM = AES_256_CBC);
CREATE CLIENT MASTER KEY cmk6 WITH (KEY_STORE = gs_ktool, KEY_PATH = "gs_ktool/1" , ALGORITHM = AES_256_CBC);
CREATE CLIENT MASTER KEY cmk10 WITH (KEY_STORE = gs_ktool, KEY_PATH = "gs_ktool/10" , ALGORITHM = AES_256_CBC);

-- clear | succeed
\! gs_ktool -d all
DROP CLIENT MASTER KEY cmk1;
DROP CLIENT MASTER KEY cmk2;
DROP CLIENT MASTER KEY cmk3;
DROP CLIENT MASTER KEY cmk4;
DROP CLIENT MASTER KEY cmk5;
SELECT * FROM gs_client_global_keys;

