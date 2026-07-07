# LLVM使用建议

目前LLVM在数据库内核侧已默认打开，用户可结合上述的分析进行配置，总体建议如下：

1. 设置合理的work\_mem，在允许的条件下尽可能设置较大的work\_mem，如果出现大量数据落盘，则建议关闭LLVM动态编译优化（通过设置enable\_codegen=off实现）。
2. 设置合理的codegen\_cost\_threshold（默认值为10000），确保小数据量场景下避免使用LLVM动态编译优化。当codegen\_cost\_threshold的值设定后，因使用LLVM动态编译优化引入性能劣化，则建议增加codegen\_cost\_threshold的取值。
3. 对于表达式计算使用LLVM动态编译优化，如果存在大量的调用C-函数的场景，建议关闭LLVM动态编译优化。
4. 在ARM平台下使用gstack抓取栈时，可能会由于LLVM重复打印同一个栈帧，导致其他线程的栈无法正常打印。此时可以通过修改gstack脚本，限制打印的栈深度来避免该问题。

    gstack脚本的修改方法如下：
    
    a.   在shell中执行sudo vim `which gstack`命令。
    
    b.  修改脚本中所有的backtrace变量，加上栈深度限制，例如限制打印的栈深度为100，则第一个backtrace修改为backtrace="bt 100"，其他backtrace变量的修改方法类似。
    
    gdb打印全部堆栈的时候如果遇到该问题，也可以通过增栈深度的限制来避免。

    >[!NOTE]说明  
    >在资源许可的情况下，数据量越大，可获得的性能提升效果越好。  
