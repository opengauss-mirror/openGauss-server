
-- 测试不同日期格式
SELECT DAY('2023-10-01') AS DayPart; -- 预期结果：1
SELECT DAY('2023-12-31') AS DayPart; -- 预期结果：31
SELECT DAY('2023-02-28') AS DayPart; -- 预期结果：28
SELECT DAY('2024-02-29') AS DayPart; -- 预期结果：29 (闰年)

-- 测试边界值
SELECT DAY('1753-01-01') AS DayPart; -- 预期结果：1 (SQL Server 的最小日期)
SELECT DAY('9999-12-31') AS DayPart; -- 预期结果：31 (SQL Server 的最大日期)

-- 测试 NULL 值
SELECT DAY(NULL) AS DayPart; -- 预期结果：NULL

-- 测试非日期类型
SELECT DAY('2023-10-01'::date) AS DayPart; -- 预期结果：1
SELECT DAY('2023-10-01'::smalldatetime) AS DayPart; -- 预期结果：1
SELECT DAY('2023-10-01'::timestamp) AS DayPart; -- 预期结果：1
SELECT DAY('2023-10-01'::timestamptz) AS DayPart; -- 预期结果：1
SELECT DAY('2023-10-01'::abstime) AS DayPart; -- 预期结果：1
SELECT DAY('2023-10-01'::timestamp(0) with time zone) AS DayPart; -- 预期结果：1

-- 测试时间部分
SELECT DAY('2023-10-01 12:34:56') AS DayPart; -- 预期结果：1
SELECT DAY('2023-10-01 23:59:59.997') AS DayPart; -- 预期结果：1

-- 测试不同的区域设置
SET DateStyle to MDY;
SELECT DAY('10/01/2023') AS DayPart; -- 预期结果：1

SET DateStyle to DMY;
SELECT DAY('01/10/2023') AS DayPart; -- 预期结果：10

SET DateStyle to YMD;
SELECT DAY('2023-10-01') AS DayPart; -- 预期结果：1

-- 测试动态日期
SELECT DAY(CURRENT_DATE) AS DayPart; -- 预期结果：当前日期的天数部分
SELECT DAY(CURRENT_STAMP) AS DayPart; -- 预期结果：当前日期的天数部分


-- 异常测试
SELECT DAY(CURRENT_TIME) AS DayPart;
SELECT DAY('20:12:12') AS DayPart;
SELECT DAY(NULL) AS DayPart;
SELECT DAY(12345) AS DayPart;
SELECT DAY('abc') AS DayPart;

SELECT DAY('2023-13-01') AS DayPart; 
SELECT DAY('2023-02-30') AS DayPart; 
SELECT DAY('2023-04-31') AS DayPart; 
SELECT DAY('2023-00-01') AS DayPart; 
SELECT DAY('2023-01-00') AS DayPart; 


