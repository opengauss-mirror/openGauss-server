create schema clustered_index;
set current_schema = clustered_index;

CREATE TABLE Users (
    UserID INT PRIMARY KEY,
    UserName NVARCHAR(50),
    Email NVARCHAR(100)
);

-- 创建聚集索引
CREATE CLUSTERED INDEX IX_Users_UserID
ON Users (UserID);
DROP INDEX IX_Users_UserID;
CREATE UNIQUE CLUSTERED INDEX IX_Users_UserID
ON Users (UserID);
-- 创建非聚集索引
CREATE NONCLUSTERED INDEX IX_Users_UserName
ON Users (UserName);
DROP INDEX IX_Users_UserName;
CREATE UNIQUE NONCLUSTERED INDEX IX_Users_UserName
ON Users (UserName);

INSERT INTO Users (UserID, UserName, Email) VALUES
(1, 'Alice', 'alice@example.com'),
(2, 'Bob', 'bob@example.com'),
(3, 'Charlie', 'charlie@example.com');

-- 使用聚集索引查询
SELECT * FROM Users WHERE UserID = 1;

-- 使用非聚集索引查询
SELECT * FROM Users WHERE UserName = 'Bob';

CREATE TABLE AM_COMMENT(
    COM_ID int NOT NULL,
 CONSTRAINT PK_AM_COMMENT PRIMARY KEY CLUSTERED(COM_ID)
);

CREATE TABLE AM_COMMENT2(
    COM_ID int NOT NULL,
 CONSTRAINT PK_AM_COMMENT2 UNIQUE CLUSTERED(COM_ID)
);

CREATE TABLE AM_COMMENT3(
    COM_ID int NOT NULL,
 CONSTRAINT PK_AM_COMMENT3 PRIMARY KEY NONCLUSTERED(COM_ID)
);

CREATE TABLE AM_COMMENT4(
    COM_ID int NOT NULL,
 CONSTRAINT PK_AM_COMMENT4 UNIQUE NONCLUSTERED(COM_ID)
);

drop schema clustered_index cascade;