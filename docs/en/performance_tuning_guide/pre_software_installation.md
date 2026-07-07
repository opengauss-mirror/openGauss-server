# Software Installation<a name="EN-US_TOPIC_0283136952"></a>

1. Install JDK.
    1. Download and install JDK.
    2. Configure the environment variables as follows:

        ```
        export JAVA_HOME=your_path/jdk1.8.0_232 
        export JRE_HOME=${JAVA_HOME}/jre 
        export CLASSPATH=.:${JAVA_HOME}/lib:${JRE_HOME}/jre 
        export PATH=${PATH}:${JAVA_HOME}/bin 
        export JAVA_TOOL_OPTIONS=-Dfile.encoding=UTF8
        ```

2. Install numactl.

    Run the following command to install numactl.

    ```
    yum install –y numactl
    ```

3. Install ANT.

    1. Run the following command to install ANT.

        ```
        yum install –y ant
        ```

    2. Set environment variables and add the following two lines to the  **/etc/profile**  file.

        ```
        export ANT_HOME=/usr/share/ant/ 
        export PATH=${PATH}:${ANT_HOME}/bin
        ```

    >[!NOTE]NOTE 
    >The installation path of  **ANT\_HOME**  may vary according to the OS. You can run the  **find**  command to find the corresponding installation path.

4. Install the htop tool. For details, see  [https://hisham.hm/htop/](https://hisham.hm/htop/).
