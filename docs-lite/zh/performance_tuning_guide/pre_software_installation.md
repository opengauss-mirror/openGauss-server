# 前置软件安装<a name="ZH-CN_TOPIC_0289900367"></a>

1. 安装jdk。
    1. 下载并安装jdk。
    2. 配置环境变量，具体如下：

        ```
        export JAVA_HOME=your_path/jdk1.8.0_232 
        export JRE_HOME=${JAVA_HOME}/jre 
        export CLASSPATH=.:${JAVA_HOME}/lib:${JRE_HOME}/jre 
        export PATH=${PATH}:${JAVA_HOME}/bin 
        export JAVA_TOOL_OPTIONS=-Dfile.encoding=UTF8
        ```

2. 安装numactl。

    执行如下命令安装numactl。

    ```
    yum install –y numactl
    ```

3. 安装ant。

    1. 执行如下命令安装ant。

        ```
        yum install –y ant
        ```

    2. 设置环境变量，修改文件/etc/profile，添加下面两行。

        ```
        export ANT_HOME=/usr/share/ant/ 
        export PATH=${PATH}:${ANT_HOME}/bin
        ```

    >[!NOTE]说明
    >
    >可能对于不同的操作系统ANT\_HOME的安装路径不一样，可以通过find命令找打其对应的安装路径。

4. 安装htop工具，具体请参见[https://hisham.hm/htop/](https://hisham.hm/htop/)。
