# Deploying RAGFlow Based on the openGauss Vector Database

This section describes how to deploy RAGFlow and use the openGauss DataVec vector database as the corpus for the RAG engine.

## Deploying RAGFlow

### Hardware Requirements

- CPU >= 4 cores
- RAM >= 16 GB
- Disk >= 50 GB

### Operating System and Software Requirements

- OS: openEuler 22.03 LTS SP3 for Arm / BClinux for Euler 21.10U4 / CTyunOS3 / CULinux3.0
- Docker: >= 28.0.1 for Arm
- Docker Compose: >= 2.33.1 for Arm

### Quick Start

#### (1) Deploying openGauss and RAGFlow on the Same Node

1. Clone the KunpengRAG repository and go to the RAGFlow Docker Compose deployment directory.

    ```bash
    git clone https://gitee.com/kunpeng_compute/KunpengRAG.git
    cd KunpengRAG/deployment/docker-compose/ragflow

    ```

2. Use the [docker-compose.yml](https://gitee.com/kunpeng_compute/KunpengRAG/blob/master/deployment/docker-compose/ragflow/docker-compose.yml) file to start the RAGFlow server.

    ```bash
    docker compose up -d
    ```

#### (2) Deploying openGauss and RAGFlow on Different Nodes

First, deploy the openGauss service on one node.

1. Clone the KunpengRAG repository and go to the RAGFlow Docker Compose deployment directory.

    ```bash
    git clone https://gitee.com/kunpeng_compute/KunpengRAG.git
    cd KunpengRAG/deployment/docker-compose/ragflow
    ```

2. Configure the openGauss environment variables in the `.env` file, including the openGauss username, port, and password. If these variables are not configured, the default values are used.
3. Start only the openGauss service using Docker Compose.

   ```bash
   docker compose up -d opengauss
   ```

After openGauss is deployed successfully, deploy RAGFlow on another node.
1. Go to the corresponding directory and modify the `.env` environment variable configuration file. Set `COMPOSE_PROFILE=none` in the `.env` file to prevent Docker Compose from automatically starting the openGauss service.
2. Modify the openGauss environment variables in the `.env` file and enter the connection information for the separately deployed openGauss service:

- `OPENGAUSS_HOST`: IP address of the server where the openGauss service is running.
- `OPENGAUSS_PORT`: port on which the openGauss service listens. In the deployment described above, the default is `5432`.
- `OPENGAUSS_USER`: regular user of the openGauss service. In the deployment described above, the default is `opengauss_user`.
- `OPENGAUSS_PASS`: password of the regular openGauss service user. In the deployment described above, the password is `xxxxxx`.
- `OPENGAUSS_DATABASE`: name of the openGauss service database. In the deployment described above, the default is `ragflow`.

3. Run the following command to start the RAGFlow service.

   ```bash
   docker compose up -d
   ```

After the command completes, access `http://<your_server_ip>/install` in a browser to open the RAGFlow console and start the initial setup.

For details, see [openGauss + RAGFlow: From Deployment to Integration](opengauss_ragflow.md).
