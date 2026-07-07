# Other Factors Affecting LLVM Performance<a name="EN-US_TOPIC_0245374540"></a>

The LLVM optimization effect depends on not only operations and computing in the database, but also the selected hardware environment.

- Number of C functions called by expressions

    CodeGen cannot be used in all expressions in an entire expression, that is, some expressions use CodeGen while others invoke original C codes for calculation. In an entire expression, if more expressions invoke original C codes, LLVM dynamic compilation and optimization may reduce the calculation performance. By setting  **log\_min\_message**  to  **DEBUG1**, you can view expressions that directly invoke C codes.

- Memory resources

    One of the key LLVM features is to ensure the locality of data, that is, data should be stored in registers as much as possible. Data loading should be reduced at the same time. Therefore, when using LLVM optimization, value of  **work\_mem**  must be set as large as required to ensure that codes are processed in the memory using corresponding LLVM. Otherwise, performance deteriorates.

- Optimizer cost estimation

    The LLVM feature realizes a simple cost estimation model. You can determine whether to use LLVM dynamic compilation and optimization for the current node based on the tables involved in the node computing. If the optimizer understates or overestimates the actual number of rows involved, the income cannot be obtained.
