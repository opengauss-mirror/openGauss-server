# 部署基于openGauss向量数据库的RAGFlow

本文主要介绍如何部署RAGFlow，并使用openGauss DataVec向量数据库作为RAG引擎语料库。

## RAGFlow部署

### 硬件要求

- CPU >= 4 核
- RAM >= 16 GB
- Disk >= 50 GB

### 操作系统和软件要求

- OS : openEuler 22.03 LTS SP3 for Arm / BClinux for Euler 21.10U4 / CTyunOS3 / CULinux3.0
- Docker : >= 28.0.1 for Arm
- Docker Compose : >= 2.33.1 for Arm

### 快速启动

#### （1）openGauss与RAGFlow部署在同一节点的情况

1. 拉取KunpengRAG项目仓库并进入RAGFlow的docker-compose部署目录

    ```bash
    git clone https://gitee.com/kunpeng_compute/KunpengRAG.git
    cd KunpengRAG/deployment/docker-compose/ragflow

```

2. 启动 RAGFlow 服务器的最简单方法是运行我们的 [docker-compose.yml](https://gitee.com/kunpeng_compute/KunpengRAG/blob/master/deployment/docker-compose/ragflow/docker-compose.yml) 文件。

```bash
docker compose up -d
```

#### （2）openGauss与RAGFlow部署在不同节点的情况

首先需要在一台节点上完成openGauss服务的部署。

1. 拉取KunpengRAG项目仓库并进入RAGFlow的docker-compose部署目录

    ```bash
    git clone https://gitee.com/kunpeng_compute/KunpengRAG.git
    cd KunpengRAG/deployment/docker-compose/ragflow
    ```

2. 在.env文件中配置openGauss的环境变量，包括openGauss用户名、端口、密码（不配置则按默认值）
3. 通过docker-compose指定单独启动openGauss服务：

```bash
docker-compose up -d opengauss

openGauss部署成功后，到另一台节点完成RAGFlow的部署:
1. 进入对应目录，修改.env环境变量配置文件，修改.env中COMPOSE_PROFILE=none，禁止docker-compose自动拉起openGauss服务。
2. 修改.env中的openGauss配置的环境变量，填写为单独部署的openGauss连接信息:

- OPENGAUSS_HOST：openGauss服务所在服务器IP
- OPENGAUSS_PORT：openGauss服务监听端口，按上述部署默认为5432
- OPENGAUSS_USER：openGauss服务普通用户，按上述部署默认为opengauss_user
- OPENGAUSS_PASS：openGauss服务普通用户密码，按上述部署为xxxxxx
- OPENGAUSS_DATABASE：openGauss服务数据库名称，按上述部署默认为ragflow

3. 启动RAGFlow服务部署命令

```bash
docker-compose up -d
```

运行后，可以在浏览器上访问 [http://Your_server_ip/install](http://localhost/install) 进入 RAGFlow 控制台并开始初始化安装操作。

具体操作请参考[openGauss + RAGFlow 从部署到集成](opengauss_ragflow.md)
