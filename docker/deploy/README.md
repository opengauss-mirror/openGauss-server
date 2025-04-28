
## 使用说明

1. 提前创建容器网络
   
    如果主备部署在同一个节点，创建一个容器网络就可以。
    如果部署在不同节点，需要提前配置跨节点容器网络互通。

2. 修改配置文件 og_config.ini
    
    各配置说明：

    | 配置项 | 说明 |
    | -----  |----- |
    | opengauss_image |  opengauss容器镜像名称， 如opengauss:6.0.0 |
    | dockernetwork   |  创建的容器网络名称。                     |
    | node_names      |  主备的所有容器名称信息，逗号分割。       |
    | node_ips        |  划分的主备所有容器ip信息，逗号分割。和node_names需要一一对应。 |
    | server_port_maps|  数据库容器内部端口(5432)映射到宿主机上的端口，逗号分割。和node_names需要一一对应。 |
    | docker_vols     |  数据目录映射到宿主机上的路径，逗号分割。和node_names需要一一对应。 |
    | ogpassword      |  管理用户密码。需要包含大小写字母、数字、特殊字符至少3种，长度在8-16位。 |



3. 启动主机

    ```
    sh og_docker_deploy.sh -m primary -node node1 -f ./og_config.ini 
    ```


    **-m**  指定该实例的角色。 primary 或 standby。 先启动一个主机，在启动多个备机。

    **-node** 指定容器的名称。对应于配置文件里面的node_names。各个节点不同。
    
    **-f** 指定配置文件。

    如果只有一个节点，则启动后是单机模式。


4. 启动备机

    有几个备机就需要拉起几个：

    ```
    ## 备机节点2
    sh og_docker_deploy.sh -m standby -node node2 -f ./og_config.ini

    ## 备机节点3
    sh og_docker_deploy.sh -m standby -node node3 -f ./og_config.ini
    ```

5. 查询
   
   在指定的容器名称里面查询实例状态：
    ```
    docker exec node1 su - omm -c "gs_ctl query -D /var/lib/opengauss/data"
    ```