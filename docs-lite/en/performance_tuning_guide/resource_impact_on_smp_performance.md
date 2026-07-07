# Resource Impact on SMP Performance<a name="EN-US_TOPIC_0000001085002992"></a>

The SMP architecture uses abundant resources to obtain time. After the plan parallelism is executed, the resource consumption is added, including the CPU, memory, and I/O resources. As the parallelism degree is expanded, the resource consumption increases. If these resources become a bottleneck, the SMP cannot improve the performance and the overall performance of the database instance may be deteriorated. The following information describes the situations that the SMP affects theses resources:

- CPU resources

    In a general customer scenario, the system CPU usage is not high. Using the SMP parallelism architecture will fully use the CPU resources to improve the system performance. If the number of CPU kernels of the database server is too small and the CPU usage is already high, enabling the SMP parallelism may deteriorate the system performance due to resource competition between multiple threads.

- Memory resources

    Query parallel causes memory usage growth, but the memory usage of each operator is still restricted by  **work\_mem**  and other parameters. Assuming that  **work\_mem**  is 4 GB and the degree of parallelism is 2, the memory usage of each thread in parallel is limited to 2 GB. When  **work\_mem**  is small or the system memory is not sufficient, using SMP may flush data to disks. As a result, the query performance deteriorates.

- I/O resources

    A parallel scan increases I/O resource consumption. It can improve scan performance only when I/O resources are sufficient.
