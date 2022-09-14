CREATE TABLE gtest1 (a text, b int GENERATED ALWAYS AS (length(a)) STORED);
insert into gtest1 values ('base');
