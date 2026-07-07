# File System Configuration<a name="EN-US_TOPIC_0289900419"></a>

To improve the I/O efficiency of the database, NVMe disks are used as data disks. The file system type is XFS and the data block size is 8 KB. The procedure is as follows:

1. <a name="en-us_topic_0283136945_en-us_topic_0263913268_li13131455153313"></a>Check the file system type of the current data disk.
    1. Run the following command to query the attached NVMe disk:

        ```
        df -h | grep nvme
        ```

        The result is as follows:

        ```
        /dev/nvme0n1                3.7T  2.6T  1.2T  69% /data1 
        /dev/nvme1n1                3.7T  1.9T  1.8T  51% /data2 
        /dev/nvme2n1                3.7T  2.2T  1.6T  59% /data3 
        /dev/nvme3n1                3.7T  1.4T  2.3T  39% /data4
        ```

    2. Run the following command to view the NVMe disk information:

        ```
        xfs_info
        ```

        For example, run the  **xfs\_info /data1**  command. As shown in  [Figure 1](#fig790551418911), the block size is 8 KB and does not need to be changed. If the block size does not meet the requirement of 8 KB, you need to format the block again. Before formatting, back up the data.

        **Figure  1**  Viewing the NVMe disk information<a name="fig790551418911"></a>  
        ![](figures/viewing-the-nvme-disk-information.png)

2. Back up the data on the disk to be modified to other disks or servers.
3. Format the disk as an XFS file system.

    Take the disk  **/dev/nvme0n1**  whose loading path is  **/data1**  as an example. Run the following command. Perform this step based on the actual situation.

    ```
    umount /data1 
    mkfs.xfs -b size=8192 /dev/nvme0n1 -f 
    mount /dev/nvme0n1 /data1
    ```

4. Perform  [step 1](#en-us_topic_0283136945_en-us_topic_0263913268_li13131455153313)  and check whether the block size has been changed to 8 KB.
