create database school;

\c school;

BEGIN;

-- 创建表student
CREATE TABLE student
(
    std_id INT PRIMARY KEY,
    std_name VARCHAR(20) NOT NULL,
    std_sex VARCHAR(6),
    std_birth DATE,
    std_in DATE NOT NULL,
    std_address VARCHAR(100)
);

-- 创建表teacher
CREATE TABLE teacher
(
    tec_id INT PRIMARY KEY,
    tec_name VARCHAR(20) NOT NULL,
    tec_job VARCHAR(15),
    tec_sex VARCHAR(6),
    tec_age INT,
    tec_in DATE NOT NULL
);

-- 创建表class
CREATE TABLE class
(
    cla_id INT PRIMARY KEY,
    cla_name VARCHAR(20) NOT NULL,
    cla_teacher INT NOT NULL
);
-- 给表class添加外键约束
ALTER TABLE class ADD CONSTRAINT fk_tec_id FOREIGN KEY (cla_teacher) REFERENCES teacher(tec_id) ON DELETE CASCADE;

-- 创建表school_department
CREATE TABLE school_department
(
    depart_id INT PRIMARY KEY,
    depart_name VARCHAR(30) NOT NULL,
    depart_teacher INT NOT NULL
);
-- 给表school_department添加外键约束
ALTER TABLE school_department ADD CONSTRAINT fk_depart_tec_id FOREIGN KEY (depart_teacher) REFERENCES teacher(tec_id) ON DELETE CASCADE;

-- 创建表course
CREATE TABLE course
(
    cor_id INT PRIMARY KEY,
    cor_name VARCHAR(30) NOT NULL,
    cor_type VARCHAR(20),
    credit DOUBLE PRECISION
);

-- 插入数据
INSERT INTO student(std_id,std_name,std_sex,std_birth,std_in,std_address) VALUES (1,'张一','男','1993-01-01','2011-09-01','江苏省南京市雨花台区');
INSERT INTO student(std_id,std_name,std_sex,std_birth,std_in,std_address) VALUES (2,'张二','男','1993-01-02','2011-09-01','江苏省南京市雨花台区');
INSERT INTO student(std_id,std_name,std_sex,std_birth,std_in,std_address) VALUES (3,'张三','男','1993-01-03','2011-09-01','江苏省南京市雨花台区');
INSERT INTO student(std_id,std_name,std_sex,std_birth,std_in,std_address) VALUES (4,'张四','男','1993-01-04','2011-09-01','江苏省南京市雨花台区');
INSERT INTO student(std_id,std_name,std_sex,std_birth,std_in,std_address) VALUES (5,'张五','男','1993-01-05','2011-09-01','江苏省南京市雨花台区');
INSERT INTO student(std_id,std_name,std_sex,std_birth,std_in,std_address) VALUES (6,'张六','男','1993-01-06','2011-09-01','江苏省南京市雨花台区');
INSERT INTO student(std_id,std_name,std_sex,std_birth,std_in,std_address) VALUES (7,'张七','男','1993-01-07','2011-09-01','江苏省南京市雨花台区');
INSERT INTO student(std_id,std_name,std_sex,std_birth,std_in,std_address) VALUES (8,'张八','男','1993-01-08','2011-09-01','江苏省南京市雨花台区');
INSERT INTO student(std_id,std_name,std_sex,std_birth,std_in,std_address) VALUES (9,'张九','男','1993-01-09','2011-09-01','江苏省南京市雨花台区');
INSERT INTO student(std_id,std_name,std_sex,std_birth,std_in,std_address) VALUES (10,'李一','男','1993-01-10','2011-09-01','江苏省南京市雨花台区');
INSERT INTO student(std_id,std_name,std_sex,std_birth,std_in,std_address) VALUES (11,'李二','男','1993-01-11','2011-09-01','江苏省南京市雨花台区');
INSERT INTO student(std_id,std_name,std_sex,std_birth,std_in,std_address) VALUES (12,'李三','男','1993-01-12','2011-09-01','江苏省南京市雨花台区');
INSERT INTO student(std_id,std_name,std_sex,std_birth,std_in,std_address) VALUES (13,'李四','男','1993-01-13','2011-09-01','江苏省南京市雨花台区');
INSERT INTO student(std_id,std_name,std_sex,std_birth,std_in,std_address) VALUES (14,'李五','男','1993-01-14','2011-09-01','江苏省南京市雨花台区');
INSERT INTO student(std_id,std_name,std_sex,std_birth,std_in,std_address) VALUES (15,'李六','男','1993-01-15','2011-09-01','江苏省南京市雨花台区');
INSERT INTO student(std_id,std_name,std_sex,std_birth,std_in,std_address) VALUES (16,'李七','男','1993-01-16','2011-09-01','江苏省南京市雨花台区');
INSERT INTO student(std_id,std_name,std_sex,std_birth,std_in,std_address) VALUES (17,'李八','男','1993-01-17','2011-09-01','江苏省南京市雨花台区');
INSERT INTO student(std_id,std_name,std_sex,std_birth,std_in,std_address) VALUES (18,'李九','男','1993-01-18','2011-09-01','江苏省南京市雨花台区');
INSERT INTO student(std_id,std_name,std_sex,std_birth,std_in,std_address) VALUES (19,'王一','男','1993-01-19','2011-09-01','江苏省南京市雨花台区');
INSERT INTO student(std_id,std_name,std_sex,std_birth,std_in,std_address) VALUES (20,'王二','男','1993-01-20','2011-09-01','江苏省南京市雨花台区');
INSERT INTO student(std_id,std_name,std_sex,std_birth,std_in,std_address) VALUES (21,'王三','男','1993-01-21','2011-09-01','江苏省南京市雨花台区');
INSERT INTO student(std_id,std_name,std_sex,std_birth,std_in,std_address) VALUES (22,'王四','男','1993-01-22','2011-09-01','江苏省南京市雨花台区');
INSERT INTO student(std_id,std_name,std_sex,std_birth,std_in,std_address) VALUES (23,'王五','男','1993-01-23','2011-09-01','江苏省南京市雨花台区');
INSERT INTO student(std_id,std_name,std_sex,std_birth,std_in,std_address) VALUES (24,'王六','男','1993-01-24','2011-09-01','江苏省南京市雨花台区');
INSERT INTO student(std_id,std_name,std_sex,std_birth,std_in,std_address) VALUES (25,'王七','男','1993-01-25','2011-09-01','江苏省南京市雨花台区');
INSERT INTO student(std_id,std_name,std_sex,std_birth,std_in,std_address) VALUES (26,'王八','男','1993-01-26','2011-09-01','江苏省南京市雨花台区');
INSERT INTO student(std_id,std_name,std_sex,std_birth,std_in,std_address) VALUES (27,'王九','男','1993-01-27','2011-09-01','江苏省南京市雨花台区');
INSERT INTO student(std_id,std_name,std_sex,std_birth,std_in,std_address) VALUES (28,'钱一','男','1993-01-28','2011-09-01','江苏省南京市雨花台区');
INSERT INTO student(std_id,std_name,std_sex,std_birth,std_in,std_address) VALUES (29,'钱二','男','1993-01-29','2011-09-01','江苏省南京市雨花台区');
INSERT INTO student(std_id,std_name,std_sex,std_birth,std_in,std_address) VALUES (30,'钱三','男','1993-01-30','2011-09-01','江苏省南京市雨花台区');
INSERT INTO student(std_id,std_name,std_sex,std_birth,std_in,std_address) VALUES (31,'钱四','男','1993-02-01','2011-09-01','江苏省南京市雨花台区');
INSERT INTO student(std_id,std_name,std_sex,std_birth,std_in,std_address) VALUES (32,'钱五','男','1993-02-02','2011-09-01','江苏省南京市雨花台区');
INSERT INTO student(std_id,std_name,std_sex,std_birth,std_in,std_address) VALUES (33,'钱六','男','1993-02-03','2011-09-01','江苏省南京市雨花台区');
INSERT INTO student(std_id,std_name,std_sex,std_birth,std_in,std_address) VALUES (34,'钱七','男','1993-02-04','2011-09-01','江苏省南京市雨花台区');
INSERT INTO student(std_id,std_name,std_sex,std_birth,std_in,std_address) VALUES (35,'钱八','男','1993-02-05','2011-09-01','江苏省南京市雨花台区');
INSERT INTO student(std_id,std_name,std_sex,std_birth,std_in,std_address) VALUES (36,'钱九','男','1993-02-06','2011-09-01','江苏省南京市雨花台区');
INSERT INTO student(std_id,std_name,std_sex,std_birth,std_in,std_address) VALUES (37,'吴一','男','1993-02-07','2011-09-01','江苏省南京市雨花台区');
INSERT INTO student(std_id,std_name,std_sex,std_birth,std_in,std_address) VALUES (38,'吴二','男','1993-02-08','2011-09-01','江苏省南京市雨花台区');
INSERT INTO student(std_id,std_name,std_sex,std_birth,std_in,std_address) VALUES (39,'吴三','男','1993-02-09','2011-09-01','江苏省南京市雨花台区');
INSERT INTO student(std_id,std_name,std_sex,std_birth,std_in,std_address) VALUES (40,'吴四','男','1993-02-10','2011-09-01','江苏省南京市雨花台区');
INSERT INTO student(std_id,std_name,std_sex,std_birth,std_in,std_address) VALUES (41,'吴五','男','1993-02-11','2011-09-01','江苏省南京市雨花台区');
INSERT INTO student(std_id,std_name,std_sex,std_birth,std_in,std_address) VALUES (42,'吴六','男','1993-02-12','2011-09-01','江苏省南京市雨花台区');
INSERT INTO student(std_id,std_name,std_sex,std_birth,std_in,std_address) VALUES (43,'吴七','男','1993-02-13','2011-09-01','江苏省南京市雨花台区');
INSERT INTO student(std_id,std_name,std_sex,std_birth,std_in,std_address) VALUES (44,'吴八','男','1993-02-14','2011-09-01','江苏省南京市雨花台区');
INSERT INTO student(std_id,std_name,std_sex,std_birth,std_in,std_address) VALUES (45,'吴九','男','1993-02-15','2011-09-01','江苏省南京市雨花台区');
INSERT INTO student(std_id,std_name,std_sex,std_birth,std_in,std_address) VALUES (46,'柳一','男','1993-02-16','2011-09-01','江苏省南京市雨花台区');
INSERT INTO student(std_id,std_name,std_sex,std_birth,std_in,std_address) VALUES (47,'柳二','男','1993-02-17','2011-09-01','江苏省南京市雨花台区');
INSERT INTO student(std_id,std_name,std_sex,std_birth,std_in,std_address) VALUES (48,'柳三','男','1993-02-18','2011-09-01','江苏省南京市雨花台区');
INSERT INTO student(std_id,std_name,std_sex,std_birth,std_in,std_address) VALUES (49,'柳四','男','1993-02-19','2011-09-01','江苏省南京市雨花台区');
INSERT INTO student(std_id,std_name,std_sex,std_birth,std_in,std_address) VALUES (50,'柳五','男','1993-02-20','2011-09-01','江苏省南京市雨花台区');

INSERT INTO teacher(tec_id,tec_name,tec_job,tec_sex,tec_age,tec_in) VALUES (1,'张一','讲师','男',35,'2009-07-01');
INSERT INTO teacher(tec_id,tec_name,tec_job,tec_sex,tec_age,tec_in) VALUES (2,'张二','讲师','男',35,'2009-07-01');
INSERT INTO teacher(tec_id,tec_name,tec_job,tec_sex,tec_age,tec_in) VALUES (3,'张三','讲师','男',35,'2009-07-01');
INSERT INTO teacher(tec_id,tec_name,tec_job,tec_sex,tec_age,tec_in) VALUES (4,'张四','讲师','男',35,'2009-07-01');
INSERT INTO teacher(tec_id,tec_name,tec_job,tec_sex,tec_age,tec_in) VALUES (5,'张五','讲师','男',35,'2009-07-01');
INSERT INTO teacher(tec_id,tec_name,tec_job,tec_sex,tec_age,tec_in) VALUES (6,'张六','讲师','男',35,'2009-07-01');
INSERT INTO teacher(tec_id,tec_name,tec_job,tec_sex,tec_age,tec_in) VALUES (7,'张七','讲师','男',35,'2009-07-01');
INSERT INTO teacher(tec_id,tec_name,tec_job,tec_sex,tec_age,tec_in) VALUES (8,'张八','讲师','男',35,'2009-07-01');
INSERT INTO teacher(tec_id,tec_name,tec_job,tec_sex,tec_age,tec_in) VALUES (9,'张九','讲师','男',35,'2009-07-01');
INSERT INTO teacher(tec_id,tec_name,tec_job,tec_sex,tec_age,tec_in) VALUES (10,'李一','讲师','男',35,'2009-07-01');
INSERT INTO teacher(tec_id,tec_name,tec_job,tec_sex,tec_age,tec_in) VALUES (11,'李二','讲师','男',35,'2009-07-01');
INSERT INTO teacher(tec_id,tec_name,tec_job,tec_sex,tec_age,tec_in) VALUES (12,'李三','讲师','男',35,'2009-07-01');
INSERT INTO teacher(tec_id,tec_name,tec_job,tec_sex,tec_age,tec_in) VALUES (13,'李四','讲师','男',35,'2009-07-01');
INSERT INTO teacher(tec_id,tec_name,tec_job,tec_sex,tec_age,tec_in) VALUES (14,'李五','讲师','男',35,'2009-07-01');
INSERT INTO teacher(tec_id,tec_name,tec_job,tec_sex,tec_age,tec_in) VALUES (15,'李六','讲师','男',35,'2009-07-01');
INSERT INTO teacher(tec_id,tec_name,tec_job,tec_sex,tec_age,tec_in) VALUES (16,'李七','讲师','男',35,'2009-07-01');
INSERT INTO teacher(tec_id,tec_name,tec_job,tec_sex,tec_age,tec_in) VALUES (17,'李八','讲师','男',35,'2009-07-01');
INSERT INTO teacher(tec_id,tec_name,tec_job,tec_sex,tec_age,tec_in) VALUES (18,'李九','讲师','男',35,'2009-07-01');
INSERT INTO teacher(tec_id,tec_name,tec_job,tec_sex,tec_age,tec_in) VALUES (19,'王一','讲师','男',35,'2009-07-01');
INSERT INTO teacher(tec_id,tec_name,tec_job,tec_sex,tec_age,tec_in) VALUES (20,'王二','讲师','男',35,'2009-07-01');
INSERT INTO teacher(tec_id,tec_name,tec_job,tec_sex,tec_age,tec_in) VALUES (21,'王三','讲师','男',35,'2009-07-01');
INSERT INTO teacher(tec_id,tec_name,tec_job,tec_sex,tec_age,tec_in) VALUES (22,'王四','讲师','男',35,'2009-07-01');
INSERT INTO teacher(tec_id,tec_name,tec_job,tec_sex,tec_age,tec_in) VALUES (23,'王五','讲师','男',35,'2009-07-01');
INSERT INTO teacher(tec_id,tec_name,tec_job,tec_sex,tec_age,tec_in) VALUES (24,'王六','讲师','男',35,'2009-07-01');
INSERT INTO teacher(tec_id,tec_name,tec_job,tec_sex,tec_age,tec_in) VALUES (25,'王七','讲师','男',35,'2009-07-01');
INSERT INTO teacher(tec_id,tec_name,tec_job,tec_sex,tec_age,tec_in) VALUES (26,'王八','讲师','男',35,'2009-07-01');
INSERT INTO teacher(tec_id,tec_name,tec_job,tec_sex,tec_age,tec_in) VALUES (27,'王九','讲师','男',35,'2009-07-01');
INSERT INTO teacher(tec_id,tec_name,tec_job,tec_sex,tec_age,tec_in) VALUES (28,'钱一','讲师','男',35,'2009-07-01');
INSERT INTO teacher(tec_id,tec_name,tec_job,tec_sex,tec_age,tec_in) VALUES (29,'钱二','讲师','男',35,'2009-07-01');
INSERT INTO teacher(tec_id,tec_name,tec_job,tec_sex,tec_age,tec_in) VALUES (30,'钱三','讲师','男',35,'2009-07-01');
INSERT INTO teacher(tec_id,tec_name,tec_job,tec_sex,tec_age,tec_in) VALUES (31,'钱四','讲师','男',35,'2009-07-01');
INSERT INTO teacher(tec_id,tec_name,tec_job,tec_sex,tec_age,tec_in) VALUES (32,'钱五','讲师','男',35,'2009-07-01');
INSERT INTO teacher(tec_id,tec_name,tec_job,tec_sex,tec_age,tec_in) VALUES (33,'钱六','讲师','男',35,'2009-07-01');
INSERT INTO teacher(tec_id,tec_name,tec_job,tec_sex,tec_age,tec_in) VALUES (34,'钱七','讲师','男',35,'2009-07-01');
INSERT INTO teacher(tec_id,tec_name,tec_job,tec_sex,tec_age,tec_in) VALUES (35,'钱八','讲师','男',35,'2009-07-01');
INSERT INTO teacher(tec_id,tec_name,tec_job,tec_sex,tec_age,tec_in) VALUES (36,'钱九','讲师','男',35,'2009-07-01');
INSERT INTO teacher(tec_id,tec_name,tec_job,tec_sex,tec_age,tec_in) VALUES (37,'吴一','讲师','男',35,'2009-07-01');
INSERT INTO teacher(tec_id,tec_name,tec_job,tec_sex,tec_age,tec_in) VALUES (38,'吴二','讲师','男',35,'2009-07-01');
INSERT INTO teacher(tec_id,tec_name,tec_job,tec_sex,tec_age,tec_in) VALUES (39,'吴三','讲师','男',35,'2009-07-01');
INSERT INTO teacher(tec_id,tec_name,tec_job,tec_sex,tec_age,tec_in) VALUES (40,'吴四','讲师','男',35,'2009-07-01');
INSERT INTO teacher(tec_id,tec_name,tec_job,tec_sex,tec_age,tec_in) VALUES (41,'吴五','讲师','男',35,'2009-07-01');
INSERT INTO teacher(tec_id,tec_name,tec_job,tec_sex,tec_age,tec_in) VALUES (42,'吴六','讲师','男',35,'2009-07-01');
INSERT INTO teacher(tec_id,tec_name,tec_job,tec_sex,tec_age,tec_in) VALUES (43,'吴七','讲师','男',35,'2009-07-01');
INSERT INTO teacher(tec_id,tec_name,tec_job,tec_sex,tec_age,tec_in) VALUES (44,'吴八','讲师','男',35,'2009-07-01');
INSERT INTO teacher(tec_id,tec_name,tec_job,tec_sex,tec_age,tec_in) VALUES (45,'吴九','讲师','男',35,'2009-07-01');
INSERT INTO teacher(tec_id,tec_name,tec_job,tec_sex,tec_age,tec_in) VALUES (46,'柳一','讲师','男',35,'2009-07-01');
INSERT INTO teacher(tec_id,tec_name,tec_job,tec_sex,tec_age,tec_in) VALUES (47,'柳二','讲师','男',35,'2009-07-01');
INSERT INTO teacher(tec_id,tec_name,tec_job,tec_sex,tec_age,tec_in) VALUES (48,'柳三','讲师','男',35,'2009-07-01');
INSERT INTO teacher(tec_id,tec_name,tec_job,tec_sex,tec_age,tec_in) VALUES (49,'柳四','讲师','男',35,'2009-07-01');
INSERT INTO teacher(tec_id,tec_name,tec_job,tec_sex,tec_age,tec_in) VALUES (50,'柳五','讲师','男',35,'2009-07-01');

INSERT INTO class(cla_id,cla_name,cla_teacher) VALUES (1,'计算机',1);
INSERT INTO class(cla_id,cla_name,cla_teacher) VALUES (2,'自动化',3);
INSERT INTO class(cla_id,cla_name,cla_teacher) VALUES (3,'飞行器设计',5);
INSERT INTO class(cla_id,cla_name,cla_teacher) VALUES (4,'大学物理',7);
INSERT INTO class(cla_id,cla_name,cla_teacher) VALUES (5,'高等数学',9);
INSERT INTO class(cla_id,cla_name,cla_teacher) VALUES (6,'大学化学',12);
INSERT INTO class(cla_id,cla_name,cla_teacher) VALUES (7,'表演',14);
INSERT INTO class(cla_id,cla_name,cla_teacher) VALUES (8,'服装设计',16);
INSERT INTO class(cla_id,cla_name,cla_teacher) VALUES (9,'工业设计',18);
INSERT INTO class(cla_id,cla_name,cla_teacher) VALUES (10,'金融学',21);
INSERT INTO class(cla_id,cla_name,cla_teacher) VALUES (11,'医学',23);
INSERT INTO class(cla_id,cla_name,cla_teacher) VALUES (12,'土木工程',25);
INSERT INTO class(cla_id,cla_name,cla_teacher) VALUES (13,'机械',27);
INSERT INTO class(cla_id,cla_name,cla_teacher) VALUES (14,'建筑学',29);
INSERT INTO class(cla_id,cla_name,cla_teacher) VALUES (15,'经济学',32);
INSERT INTO class(cla_id,cla_name,cla_teacher) VALUES (16,'财务管理',34);
INSERT INTO class(cla_id,cla_name,cla_teacher) VALUES (17,'人力资源',36);
INSERT INTO class(cla_id,cla_name,cla_teacher) VALUES (18,'力学',38);
INSERT INTO class(cla_id,cla_name,cla_teacher) VALUES (19,'人工智能',41);
INSERT INTO class(cla_id,cla_name,cla_teacher) VALUES (20,'会计',45);

INSERT INTO school_department(depart_id,depart_name,depart_teacher) VALUES (1,'计算机学院',2);
INSERT INTO school_department(depart_id,depart_name,depart_teacher) VALUES (2,'自动化学院',4);
INSERT INTO school_department(depart_id,depart_name,depart_teacher) VALUES (3,'航空宇航学院',6);
INSERT INTO school_department(depart_id,depart_name,depart_teacher) VALUES (4,'艺术学院',8);
INSERT INTO school_department(depart_id,depart_name,depart_teacher) VALUES (5,'理学院',11);
INSERT INTO school_department(depart_id,depart_name,depart_teacher) VALUES (6,'人工智能学院',13);
INSERT INTO school_department(depart_id,depart_name,depart_teacher) VALUES (7,'工学院',15);
INSERT INTO school_department(depart_id,depart_name,depart_teacher) VALUES (8,'管理学院',17);
INSERT INTO school_department(depart_id,depart_name,depart_teacher) VALUES (9,'农学院',22);
INSERT INTO school_department(depart_id,depart_name,depart_teacher) VALUES (10,'医学院',28);

INSERT INTO course(cor_id,cor_name,cor_type,credit) VALUES (1,'数据库系统概论','必修',3);
INSERT INTO course(cor_id,cor_name,cor_type,credit) VALUES (2,'艺术设计概论','选修',1);
INSERT INTO course(cor_id,cor_name,cor_type,credit) VALUES (3,'力学制图','必修',4);
INSERT INTO course(cor_id,cor_name,cor_type,credit) VALUES (4,'飞行器设计历史','选修',1);
INSERT INTO course(cor_id,cor_name,cor_type,credit) VALUES (5,'马克思主义','必修',2);
INSERT INTO course(cor_id,cor_name,cor_type,credit) VALUES (6,'大学历史','必修',2);
INSERT INTO course(cor_id,cor_name,cor_type,credit) VALUES (7,'人力资源管理理论','必修',2.5);
INSERT INTO course(cor_id,cor_name,cor_type,credit) VALUES (8,'线性代数','必修',4);
INSERT INTO course(cor_id,cor_name,cor_type,credit) VALUES (9,'JAVA程序设计','必修',3);
INSERT INTO course(cor_id,cor_name,cor_type,credit) VALUES (10,'操作系统','必修',4);
INSERT INTO course(cor_id,cor_name,cor_type,credit) VALUES (11,'计算机组成原理','必修',3);
INSERT INTO course(cor_id,cor_name,cor_type,credit) VALUES (12,'自动化设计理论','必修',2);
INSERT INTO course(cor_id,cor_name,cor_type,credit) VALUES (13,'情绪表演','必修',2.5);
INSERT INTO course(cor_id,cor_name,cor_type,credit) VALUES (14,'茶学历史','选修',1);
INSERT INTO course(cor_id,cor_name,cor_type,credit) VALUES (15,'艺术论','必修',1.5);
INSERT INTO course(cor_id,cor_name,cor_type,credit) VALUES (16,'机器学习','必修',3);
INSERT INTO course(cor_id,cor_name,cor_type,credit) VALUES (17,'数据挖掘','选修',2);
INSERT INTO course(cor_id,cor_name,cor_type,credit) VALUES (18,'图像识别','必修',3);
INSERT INTO course(cor_id,cor_name,cor_type,credit) VALUES (19,'解剖学','必修',4);
INSERT INTO course(cor_id,cor_name,cor_type,credit) VALUES (20,'3D max','选修',2);

COMMIT;