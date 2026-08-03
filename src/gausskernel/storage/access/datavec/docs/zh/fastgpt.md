# 使用 openGauss 部署 FastGPT

FastGPT 是一个基于大型语言模型（LLMs）的知识型平台，提供了一系列全面的即开即用功能，如数据处理、检索增强生成（RAG）检索和视觉人工智能（AI）工作流编排，让您无需进行大量设置或配置即可轻松开发和部署复杂的问题回答系统。

FastGPT 从 [v4.14.11](https://github.com/labring/FastGPT/releases/tag/v4.14.11) 版本开始支持将 openGauss 作为向量数据库。本文着重介绍如何基于 openGauss 向量数据库完成 FastGPT 的部署，搭建 RAG 引擎语料库。

## 前置条件

确保已经安装 Docker 和 [Docker Compose](https://docs.docker.com/compose/) 插件。

## 下载 docker-compose.yml

执行以下命令下载 docker-compose.yml 文件。

```shell
# 创建目录
$ mkdir fastgpt

# 进入创建的目录
$ cd fastgpt

# 下载 docker-compose.yml
$ curl -o docker-compose.yml https://raw.githubusercontent.com/labring/FastGPT/refs/heads/main/deploy/docker/cn/docker-compose.opengauss.yml

# 下载配置文件
$ curl -O https://raw.githubusercontent.com/labring/FastGPT/main/projects/app/data/config.json
```

## 修改配置参数

### 修改镜像版本

修改以下镜像的版本号，需 v4.14.11 及以上版本：

- fastgpt-app：如 registry.cn-hangzhou.aliyuncs.com/fastgpt/fastgpt:v4.14.11
- fastgpt-code-sandbox：如 registry.cn-hangzhou.aliyuncs.com/fastgpt/fastgpt-code-sandbox:v4.14.11
- fastgpt-mcp-server：如 registry.cn-hangzhou.aliyuncs.com/fastgpt/fastgpt-mcp_server:v4.14.11

### 修改数据库连接参数

按需修改 docker-compose.yml 中 openGauss 相关的环境变量。

- `GS_USERNAME`：创建 openGauss 数据库用户，默认为 gaussdb

- `GS_PASSWORD`：设置用户密码（必须包含大写、小写、数字和特殊字符，且长度不少于8位）

- `GS_DB`：创建数据库（默认会创建 postgres 库）

- `OPENGAUSS_URL`：连接数据库的 URL，用户名、密码、数据库根据实际配置填写，格式如下：

  ```shell
  postgresql://${GS_USERNAME}:${GS_PASSWORD}@fastgpt-vector:5432/${GS_DB}
  ```

### 修改访问地址

- 修改存储桶访问地址，环境变量为 `STORAGE_EXTERNAL_ENDPOINT`。可以是固定的宿主机 IP 或者域名，注意不要填写成 127.0.0.1 或者 localhost 等本地回环地址，例如 http://192.168.230.241:9000

- 修改 FastGPT 前端访问地址，环境变量为 `FE_DOMAIN`。可以是固定的宿主机 IP 或者域名，注意不要填写成 127.0.0.1 或者 localhost 等本地回环地址，例如 http://192.168.230.241:3000

  

其他镜像版本、参数按照文件或 FastGPT 官方说明配置即可。

## 启动服务

在与 docker-compose.yml 文件相同的目录下执行。

```shell
# 启动服务
$ docker compose up -d
```

容器启动后执行 `docker compose ps` 命令确保服务都正常运行，如下图所示：

![image-20260420101559681](./figures/fastgpt_container.png)

## 访问 FastGPT

访问 http://YOUR_SERVER_IP:3000 进入 FastGPT 主页。登录用户名为 root，密码为 docker-compose.yml 文件中环境变量 `DEFAULT_ROOT_PSW` 的值。

添加知识库进行写入、检索测试：

![image-20260420102514353](./figures/fastgpt_test.png)

## 停止服务

在与 docker-compose.yml 文件相同的目录下执行。

```shell
# 停止服务
$ docker compose down
```



如果您想进一步了解如何使用 FastGPT，请参阅[官方文档](https://doc.fastgpt.io/zh-CN/docs/introduction)。


