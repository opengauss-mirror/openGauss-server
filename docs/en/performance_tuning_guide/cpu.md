# CPU<a name="EN-US_TOPIC_0245374521"></a>

You can run the  **top**  command to check the CPU usage of each node in openGauss and analyze whether performance bottleneck caused by heavy CPU load exists. The **top** command is used to monitor the Linux OS status. It is a common performance analysis tool and can display the resource usage of each process in the system in real time.
 
Description

- **-d**: number of seconds, indicating the interval for updating the page displayed by running the **top** command. The default value is 5 seconds.
- **-b**: executes the **top** command in batches.
- **-n**: This parameter is used together with **-b** to indicate the number of times that the **top** command is executed.
- **-p**: specifies a PID for observation.

## Checking CPU Usage<a name="en-us_topic_0237121486_en-us_topic_0073253546_en-us_topic_0040046498_section5417561019132"></a>

You can query the CPU usage of the server in the following ways:

On each storage node, run the  **top**  command to check the CPU usage. Then, press  **1**  to view the usage of each CPU core.

```
top - 17:05:04 up 32 days, 20:34,  5 users,  load average: 0.02, 0.02, 0.00
Tasks: 124 total,   1 running, 123 sleeping,   0 stopped,   0 zombie
Cpu0  :  0.0%us,  0.3%sy,  0.0%ni, 69.7%id,  0.0%wa,  0.0%hi,  0.0%si,  0.0%st
Cpu1  :  0.3%us,  0.3%sy,  0.0%ni, 69.3%id,  0.0%wa,  0.0%hi,  0.0%si,  0.0%st
Cpu2  :  0.3%us,  0.3%sy,  0.0%ni, 69.3%id,  0.0%wa,  0.0%hi,  0.0%si,  0.0%st
Cpu3  :  0.3%us,  0.3%sy,  0.0%ni, 69.3%id,  0.0%wa,  0.0%hi,  0.0%si,  0.0%st
Mem:   8038844k total,  7165272k used,   873572k free,   530444k buffers
Swap:  4192924k total,     4920k used,  4188004k free,  4742904k cached

   PID USER      PR  NI  VIRT  RES  SHR S %CPU %MEM    TIME+  COMMAND                                                                                                                                                                      
                                                                                                                                                                   
 35184 omm  20   0  822m 421m 128m S    0  5.4   5:28.15 gaussdb                                                                                                                                                                       
     1 root      20   0 13592  820  784 S    0  0.0   1:16.62 init            
```

In the command output, focus on the CPU usage occupied by each process.

**us**  indicates the CPU percentage occupied by the user space,  **sy**  indicates the CPU percentage occupied by the kernel space, and  **id**  indicates the idle CPU percentage. If  **id**  is less than 10%, the CPU load is high. In this case, you can reduce the CPU load by reducing the number of tasks on nodes.

## Analyzing Performance Parameters<a name="en-us_topic_0237121486_en-us_topic_0073253546_en-us_topic_0040046498_section1965795485640"></a>

1. Run the  **top-H**  command to check the CPU usage. The following is displayed:

    ```
        14 root      20   0     0    0    0 S    0  0.0   0:16.41 events/3                  
    top - 14:22:49 up 5 days, 21:51,  2 users,  load average: 0.08, 0.08, 0.06
    Tasks: 312 total,   1 running, 311 sleeping,   0 stopped,   0 zombie
    Cpu(s):  1.3%us,  0.7%sy,  0.0%ni, 95.0%id,  2.4%wa,  0.5%hi,  0.2%si,  0.0%st
    Mem:   8038844k total,  5317668k used,  2721176k free,   180268k buffers
    Swap:  4192924k total,        0k used,  4192924k free,  2886860k cached
    
       PID USER      PR  NI  VIRT  RES  SHR S %CPU %MEM    TIME+  COMMAND                  
                      
      3105 root      20   0 50492  11m 2708 S    3  0.1  22:22.56 acc-snf                   
                    
      4015 gdm       20   0  232m  23m  11m S    0  0.3  11:34.70 gdm-simple-gree           
     51001 omm  20   0 12140 1484  948 R    0  0.0   0:00.94 top                       
                    
     54885 omm  20   0  615m 396m 116m S    0  5.1   0:09.44 gaussdb                   
                      
                  
         1 root      20   0 13592  944  792 S    0  0.0   0:08.54 init          
    ```

2. In the query result for  **Cpu\(s\)**, check whether the system CPU \(**sy**\) or user CPU \(**us**\) usage is high.
    - If the system CPU usage is too high, you need to identify the abnormal system processes and handle them.
    - If the CPU usage of the openGauss process whose  **USER**  is  **omm**  is too high, optimize the service-related SQL statements based on the running services queries. Based on the features of the currently running service, perform the following operations to check whether this process containing infinite loop logics.
        1. Run the  **top -H  -p pid**  command to identify the threads that use much CPU in the process.

            ```
            top -H -p 54952
            ```

            The threads causing high CPU usage are displayed in the  **top**  column of the command output. In this section, thread  **54775**  is used as an example for analyzing the causes of the high CPU usage.

            ```
            top - 14:23:27 up 5 days, 21:52,  2 users,  load average: 0.04, 0.07, 0.05
            Tasks:  13 total,   0 running,  13 sleeping,   0 stopped,   0 zombie
            Cpu(s):  0.9%us,  0.4%sy,  0.0%ni, 97.3%id,  1.1%wa,  0.2%hi,  0.1%si,  0.0%st
            Mem:   8038844k total,  5322180k used,  2716664k free,   180316k buffers
            Swap:  4192924k total,        0k used,  4192924k free,  2889860k cached
            
               PID USER      PR  NI  VIRT  RES  SHR S %CPU %MEM    TIME+  COMMAND                  
             54775 omm  20   0  684m 424m 131m S    0  5.4   0:00.32 gaussdb                   
             54951 omm  20   0  684m 424m 131m S    0  5.4   0:00.84 gaussdb                   
             54732 omm  20   0  684m 424m 131m S    0  5.4   0:00.24 gaussdb                   
             54758 omm  20   0  684m 424m 131m S    0  5.4   0:00.00 gaussdb                   
             54759 omm  20   0  684m 424m 131m S    0  5.4   0:00.02 gaussdb                   
             54773 omm  20   0  684m 424m 131m S    0  5.4   0:02.79 gaussdb                   
             54780 omm  20   0  684m 424m 131m S    0  5.4   0:00.04 gaussdb                   
             54781 omm  20   0  684m 424m 131m S    0  5.4   0:00.21 gaussdb                   
             54782 omm  20   0  684m 424m 131m S    0  5.4   0:00.02 gaussdb                   
             54798 omm  20   0  684m 424m 131m S    0  5.4   0:16.70 gaussdb                   
             54952 omm  20   0  684m 424m 131m S    0  5.4   0:07.51 gaussdb                   
             54953 omm  20   0  684m 424m 131m S    0  5.4   0:00.81 gaussdb                   
             54954 omm  20   0  684m 424m 131m S    0  5.4   0:06.54 gaussdb                   
            ```

        2. Run the following command to view the function invocation stack for each thread in the process. Check the thread number for the ID of the thread that occupies high CPU usage in the last step.

            ```
            gstack  54954
            ```

            The query result is as follows. The thread number for the thread ID  **54775**  is  **10**.

            ```
            192.168.0.11:~ # gstack 54954
            Thread 10 (Thread 0x7f95a5fff710 (LWP 54775)):
            #0  0x00007f95c41d63c6 in poll () from /lib64/libc.so.6
            #1  0x0000000000d3d2d3 in WaitLatchOrSocket(Latch volatile*, int, int, long) ()
            #2  0x000000000095ed25 in XLogPageRead(XLogRecPtr*, int, bool, bool) ()
            #3  0x000000000095f6dd in ReadRecord(XLogRecPtr*, int, bool) ()
            #4  0x000000000096aef0 in StartupXLOG() ()
            #5  0x0000000000d5607a in StartupProcessMain() ()
            #6  0x00000000009e19f9 in AuxiliaryProcessMain(int, char**) ()
            #7  0x0000000000d50135 in SubPostmasterMain(int, char**) ()
            #8  0x0000000000d504ec in MainStarterThreadFunc(void*) ()
            #9  0x00007f95c79b85f0 in start_thread () from /lib64/libpthread.so.0
            #10 0x00007f95c41df84d in clone () from /lib64/libc.so.6
            #11 0x0000000000000000 in ?? ()
            ```
