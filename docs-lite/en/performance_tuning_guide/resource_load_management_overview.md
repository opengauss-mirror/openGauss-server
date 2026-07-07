# Overview<a name="EN-US_TOPIC_0000001147994524"></a>

## Function<a name="section19652145945315"></a>

openGauss manages resource load to balance system resource usage for jobs.

## Related Concepts<a name="section1337122974120"></a>

**Resource management**

openGauss manages system resources, including CPU, memory, I/O, and storage resources. It allocates system resources in a proper way to prevent system efficiency deterioration or system running problems.

**Cgroup**

Control groups \(Cgroups\) are a mechanism provided by the Linux kernel to restrict, record, and isolate physical resources \(such as CPU, memory, and I/O resources\) used by process groups. Cgroups have strict restrictions on Linux system resources. If a process is added to a Cgroup, it can use only restricted resources. For details about Cgroup principles, see the product manual corresponding to your OS.

**Resource pool**

Resource pools are a configuration mechanism provided by openGauss to divide host resources \(memory and I/O resources\) and control SQL concurrency. Resource pools are bound to Cgroups. In this way, you can manage the resource loads of jobs in a specific resource pool.
