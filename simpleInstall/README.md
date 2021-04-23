## 1 概述

本章节主要介绍采用openGauss极简安装脚本（以下简称安装脚本），一键式安装openGauss数据库所必须的系统环境及安装步骤。

## 2 安装环境要求

### 2.1 openGauss环境要求

安装openGauss的具体环境要求，请参考《openGauss安装指南》中的“软硬件环境要求”章节。

### 2.2 安装脚本环境要求

#### 硬件环境要求

安装脚本对安装环境的操作系统及对应处理器架构进行了限制，目前支持的环境如表1所示。

**表1** 硬件环境要求

| 操作系统  | 处理器架构 |
| --------- | ---------- |
| openEuler | aarch64    |
| openEuler | x86_64     |
| CentOS    | x86_64     |


## 3 安装openGauss


### 执行安装

使用如下命令执行单节点安装脚本。

```shell
sh install.sh -w password
```

使用如下命令执行一主一备安装脚本。

```shell
sh install.sh -w password --multinode
```
#### 参数说明

- [-w password] gs_initdb password, this argument is necessary for installation
- [-p port] port of openGauss single node, or master node; default value is 5432
- [--multinode] install one master and one slave openGauss node

以上参数的详细信息，请参考《openGauss安装指南》.


## 4 导入展示数据库

### 4.1 学校数据模型

假设A市B学校为了加强对学校的管理，引入了openGauss数据库。在B学校里，主要涉及的对象有学生、教师、班级、院系和课程。本实验假设在B学校数据库中，教师会教授课程，学生会选修课程，院系会聘请教师，班级会组成院系，学生会组成班级。因此，根据此关系，本文给出了相应的关系模式如下。在运行安装脚本时，会根据用户选择安装该展示模型。

#### 关系模式

对于B校中的5个对象，分别建立属于每个对象的属性集合，具体属性描述如下：

- 学生（学号，姓名，性别，出生日期，入学日期，家庭住址）
- 教师（教师编号，教师姓名，职称，性别，年龄，入职日期）
- 班级（班级编号，班级名称，班主任）
- 院系（系编号，系名称，系主任）
- 课程（课程编号，课程名称，课程类型，学分）

上述属性对应的编号为：

- student（std_id，std_name，std_sex，std_birth，std_in，std_address）
- teacher（tec_id，tec_name，tec_job，tec_sex，tec_age，tec_in）
- class（cla_id，cla_name，cla_teacher）
- school_department（depart_id，depart_name，depart_teacher）
- course（cor_id，cor_name，cor_type，credit）

对象之间的关系：

- 一位学生可以选择多门课程，一门课程可被多名学生选择
- 一位老师可以选择多门课程，一门课程可被多名老师教授
- 一个院系可由多个班级组成
- 一个院系可聘请多名老师
- 一个班级可由多名学生组成

### 4.2 金融数据模型

假设A市C银行为了方便对银行数据的管理和操作，引入了openGauss数据库。针对C银行的业务，本实验主要将对象分为客户、银行卡、理财产品、保险、基金和资产。因此，针对这些数据库对象，本实验假设C银行的金融数据库存在着以下关系：客户可以办理银行卡，同时客户可以购买不用的银行产品，如资产，理财产品，基金和保险。那么，根据C银行的对象关系，本文给出了相应的关系模式如下。在运行安装脚本时，会根据用户选择安装该展示模型。

#### 关系模式

对于C银行中的6个对象，分别建立属于每个对象的属性集合，具体属性描述如下：

- 客户（客户编号、客户名称、客户邮箱，客户身份证，客户手机号，客户登录密码）
- 银行卡（银行卡号，银行卡类型，所属客户编号）
- 理财产品（产品名称，产品编号，产品描述，购买金额，理财年限）
- 保险（保险名称，保险编号，保险金额，适用人群，保险年限，保障项目）
- 基金（基金名称，基金编号，基金类型，基金金额，风险等级，基金管理者）
- 资产（客户编号，商品编号，商品状态，商品数量，商品收益，购买时间）

上述属性对应的编号为：

- client（c_id，c_name，c_mail，c_id_card，c_phone，c_password）
- bank_card（b_number，b_type，b_c_id）
- finances_product（p_name，p_id，p_description，p_amount，p_year）
- insurance（i_name，i_id，i_amount，i_person，i_year，i_project）
- fund（f_name，f_id，f_type，f_amount，risk_level，f_manager）
- property（pro_c_id，pro_id，pro_status，pro_quantity，pro_income，pro_purchase_time）

对象之间的关系：

- 一个客户可以办理多张银行卡
- 一个客户可有多笔资产
- 一个客户可以购买多个理财产品，同一类理财产品可由多个客户购买
- 一个客户可以购买多个基金，同一类基金可由多个客户购买
- 一个客户可以购买多个保险，同一类保险可由多个客户购买
