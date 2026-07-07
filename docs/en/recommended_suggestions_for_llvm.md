# Recommended Suggestions for LLVM<a name="EN-US_TOPIC_0245374541"></a>

Currently, the LLVM is enabled by default in the database kernel, and users can perform related configurations on it. The overall suggestions are as follows:

1. Set  **work\_mem**  to an appropriate value and set it to a large value in allowed conditions. If much data is spilled to disks, you are advised to disable the LLVM dynamic compilation and optimization by setting  **enable\_codegen**  to  **off**\).
2. Set  **codegen\_cost\_threshold**  to an appropriate value \(The default value is  **10000**\). Ensure that LLVM dynamic compilation and optimization is not used when the data volume is small. After the value of  **codegen\_cost\_threshold**  is set, the database performance may deteriorate due to the use of LLVM dynamic compilation and optimization. In this case, you are advised to increase the parameter value.
3. If a large number of C functions are called, you are advised not to use the LLVM dynamic compilation and optimization.

    >[!NOTE]NOTE   
    >If resources are robust, the larger the data volume is, the better the performance improvement effect is.  
