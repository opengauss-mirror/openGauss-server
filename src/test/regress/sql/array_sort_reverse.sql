-- 基本功能
SELECT array_reverse(ARRAY[1,2,3]);
SELECT array_reverse(ARRAY[[1,2],[3,4],[5,6]]);
SELECT array_reverse(ARRAY[3,NULL,1]);

SELECT array_sort(ARRAY[3,1,2]);
SELECT array_sort(ARRAY[[2,4],[2,1],[6,5]]);
SELECT array_sort(ARRAY['b','A','a'] COLLATE "C");

-- 无对应比较操作符，降序排序报错
SELECT array_sort(ARRAY['1'::xid, '2'::xid], true);

-- 参数descending, nulls_first
SELECT array_sort(ARRAY[2,NULL,1]);
SELECT array_sort(ARRAY[2,NULL,1], false);
SELECT array_sort(ARRAY[2,NULL,1], true);
SELECT array_sort(ARRAY[2,NULL,1], false, false);
SELECT array_sort(ARRAY[2,NULL,1], false, true);
SELECT array_sort(ARRAY[2,NULL,1], true, false);
SELECT array_sort(ARRAY[2,NULL,1], true, true);

-- 指定参数名称
SELECT array_sort(ARRAY[3,1,2], descending => false, nulls_first => true);

-- 参数值为NULL
SELECT array_sort(NULL::int[]) IS NULL;
SELECT array_sort(ARRAY[1,2], NULL) IS NULL;
SELECT array_sort(ARRAY[1,2], true, NULL) IS NULL;
SELECT array_reverse(NULL::int[]) IS NULL;

-- 空数组/单元素数组
SELECT array_reverse('{}'::int[]);
SELECT array_sort('{}'::int[]);
SELECT array_sort(ARRAY[42]);
SELECT array_dims(array_reverse('{}'::int[]));

-- 含零长度维度的多维数组
SELECT array_fill(1, ARRAY[2,0]);
SELECT array_sort(array_fill(1, ARRAY[2,0]));
SELECT array_sort(array_fill(1, ARRAY[2,0]), true);
SELECT array_sort(array_fill(1, ARRAY[2,0]), false, true);
SELECT array_sort(array_fill(1, ARRAY[3,0]));
SELECT array_sort(array_fill(1, ARRAY[2,2,0]));
SELECT array_sort(array_fill(1, ARRAY[0]));

-- 保留数组下界
SELECT array_reverse('[0:2]={1,2,3}'::int[]);
SELECT array_sort('[-2:0]={9,8,7}'::int[]);
SELECT array_dims(array_sort('[5:7]={3,1,2}'::int[]));

SELECT pg_typeof(array_sort(ARRAY['b','a']));
