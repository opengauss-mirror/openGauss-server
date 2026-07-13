# Shared Memory Leakage

## Symptom

The following error information is recorded in logs.

```
This error usually means that PostgreSQL's request for a shared  memory segment 
exceeded available memory or swap space,  or exceeded your kernel's SHMALL parameter.  
You can either  reduce the request size or reconfigure the kernel with larger SHMALL. 
```

## Cause Analysis

Run the `free` command to check the memory usage. It is found that the `shared` memory occupies a large part.

```
# free -g
              total        used        free      shared  buff/cache   available
Mem:             31           1           2          23         27         2
Swap:             3           3           0
```

Run the `ipcs` command to check the usage of the shared memory. It is found that there is a large amount of shared memory that is not used by processes but is not reclaimed. That is, the value of `nattch` is **0**.

```
[root@pekpeuler00671 script]# ipcs -m

------ Shared Memory Segments --------
key        shmid      owner      perms      bytes      nattch     status
0x00000000 65536      gnome-init 777        16384      1          dest
0x00000000 131073     gnome-init 777        16384      1          dest
0x00000000 163842     gnome-init 777        3145728    2          dest
0x00000000 393219     gnome-init 600        524288     2          dest
0x00000000 425988     gnome-init 600        4194304    2          dest
0x00000000 458757     gnome-init 777        3145728    2          dest
0x00f42401 3604486    1001       600        4455342080 0
0x00f42402 14123015   1003       600        4457177088 0
0x00f42403 23592968   1005       600        4457177088 0
0x00f42404 33062921   1007       600        4457177088 0
0x00f42405 42532874   1009       600        4457177088 0
0x00f42406 52002827   1011       600        4457177088 0
0x00f42407 61472780   1013       600        4457177088 0
0x00f42408 70942733   1015       600        4457177088 0
0x00f42409 80412686   1017       600        4457177088 0
0x00f4240a 89882639   1019       600        4457177088 0
0x00f4240b 99352592   1021       600        4457177088 0
0x00f4240c 108822545  1023       600        4457177088 0
0x00f4240d 118292498  1025       600        4457177088 0
0x00f4240e 127762451  1027       600        4457177088 0
0x00f4240f 136904724  1029       600        4455342080 0
0x00f42410 146374677  1031       600        4457177088 0
0x00f42411 155844630  1033       600        4457177088 0
0x00f42412 165314583  1035       600        4457177088 0
0x00f42413 174784536  1037       600        4457177088 0
```

The cause is that the `kill -9` command is run to exit the database process and the `IpcMemoryDelete` function is not invoked to clear the shared memory. As a result, the memory leakage occurs.

## Procedure

Run the `ipcrm` command to release the shared memory without owners. For example, to release the shared memory whose `shmid` is `3604486`, run the following command.

```
ipcrm -m shid3604486
```
