---
layout: post
title:  "6. Unified Memory Manager (UMM)"
category: Architecture
order: 6
---

```
From Spark 1.6+, Jan 2016
```

Since Spark 1.6.0, a new memory manager is adopted to replace Static Memory Manager to provide Spark with dynamic memory allocation.

 

It allocates a region of memory as a Unified memory container that is shared by storage and execution.

When execution memory is not used, the storage memory can acquire all the available memory and vice versa.

 

If any of the storage or execution memory needs more space, a function called acquireMemory() will expand one of the memory pools and shrink another one.

 

Borrowed storage memory can be evicted at any given time. Borrowed execution memory, however, will not be evicted in the first design due to complexities in implementation.

**Advantages:**

- The boundary between Storage memory and Execution memory is not static and in case of memory pressure, the boundary would be moved i.e. one region would grow by borrowing space from another one.
- When the application has no cache and progating, execution uses all the memory to avoid unnecessary disk overflow.
- When the application has a cache, it will reserve the minimum storage memory, so that the data block is not affected.
- This approach provides reasonable out-of-the-box performance for a variety of workloads without requiring user expertise of how memory is divided internally.

**JVM has two types of memory:**

- On-Heap Memory
- Off-Heap Memory

In addition to the above two JVM Memory types, there is one more segment of memory that is accessed by Spark i.e External Process Memory. This kind of memory mainly used for PySpark and SparkR applications. This is the memory used by the Python/R process which resides outside of the JVM.

### **5.1 On-Heap Memory:**
By default, Spark uses on-heap memory only. The size of the on-heap memory is configured by the `--executor-memory` or `spark.executor.memory` parameter when the Spark Application starts.

The concurrent tasks running inside Executor share JVM's on-heap memory.

 

Two main configurations that control Executor memory allocation:


| Parameter                    | Description                                                                                                                                         |
| ---------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------- |
| `spark.memory.fraction`      | Fraction of the heap space used for execution and storage. The lower this is, the more frequently spills and cached data eviction occur. The purpose of this config is to set aside memory for internal metadata, user data structures, and imprecise size estimation in the case of sparse, unusually large records. Default: 0.6 |
| `spark.memory.storageFraction`| The size of the storage region within the space set aside by `spark.memory.fraction`. Cached data may only be evicted if total storage exceeds this region. Default: 0.5 |
{: .table-responsive-scroll}




**Note:** In `Spark 1.6`, the spark.memory.fraction value will be 0.75 and `spark.memory.storageFraction` value will be 0.5.

**Apache Spark supports three memory regions:**

- Reserved Memory
- User Memory
- Spark Memory

![Spark Architecture diagram](/WhatNextAlgo/spark_architecture/assets/images/Unified_Memory_Manager_Regions.jpg){:width="75%" height="75%"}

### **Reserved Memory:** 
`Reserved Memory` is the memory reserved for system and is used to store Spark's internal objects.

As of `Spark v1.6.0+`, the value is **300MB**. That means 300MB of RAM does not participate in Spark memory region size calculations.

 

Reserved memory’s size is hardcoded and its size cannot be changed in any way without Spark recompilation or setting `spark.testing.reservedMemory`, which is not recommended as it is a testing parameter not intended to be used in production.

**Formula:**
```
RESERVED_SYSTEM_MEMORY_BYTES = 300 * 1024 * 1024 BYTES = 300 MB
```
[RESERVED SYSTEM MEMORY](https://github.com/apache/spark/blob/v2.4.5/core/src/main/scala/org/apache/spark/memory/UnifiedMemoryManager.scala#L196){:target="_blank"}

**Note:** If the **executor memory** is less than **1.5** times of **reserved memory (1.5 * Reserved Memory = 450MB heap)**, then Spark job will fail with the following exception message.

```
spark-shell --executor-memory 300m
```

```
21/06/21 03:55:51 ERROR repl.Main: Failed to initialize Spark session.
java.lang.IllegalArgumentException: Executor memory 314572800 must be at least 471859200. Please increase executor memory using the --executor-memory option or spark.executor.memory in Spark configuration.
        at org.apache.spark.memory.UnifiedMemoryManager$.getMaxMemory(UnifiedMemoryManager.scala:225)
        at org.apache.spark.memory.UnifiedMemoryManager$.apply(UnifiedMemoryManager.scala:199)
```
[spark executor memory](https://github.com/apache/spark/blob/v2.4.5/core/src/main/scala/org/apache/spark/memory/UnifiedMemoryManager.scala#L222){:target="_blank"}


### **User Memory:** 

User Memory is the memory used to store user-defined data structures, Spark internal metadata, any UDFs created by the user, the data needed for RDD conversion operations such as the information for RDD dependency information etc.

 

For example, we can rewrite Spark aggregation by using mapPartitions transformation maintaining hash table for this aggregation to run, which would consume so-called User Memory.

 

This memory segment is not managed by Spark. Spark will not be aware of/maintain this memory segment.

 

**Formula:**

```
(Java Heap — Reserved Memory) * (1.0 — spark.memory.fraction)
```
 

### **Spark Memory (Unified Memory):**

Spark Memory is the memory pool managed by Apache Spark. Spark Memory is responsible for storing intermediate state while doing task execution like joins or storing the broadcast variables.

 

All the cached/persisted data will be stored in this segment, specifically in the storage memory of this segment.

 

**Formula:**

```
(Java Heap — Reserved Memory) * spark.memory.fraction
```

Spark tasks operate in two main memory regions:

- Execution – Used for shuffles, joins, sorts and aggregations.
- Storage – Used to cache partitions of data.

The boundary between them is set by `spark.memory.storageFraction` parameter, which defaults to 0.5 or 50%.


### **Storage Memory:**

Storage Memory is used for storing all of the cached data, broadcast variables, and unroll data etc. “unroll” is essentially a process of deserializing a serialized data.

 

Any persist option that includes MEMORY in it, Spark will store that data in this segment.

 

Spark clears space for new cache requests by removing old cached objects based on Least Recently Used (LRU) mechanism.

 

Once the cached data it is out of storage, it is either written to disk or recomputed based on configuration. Broadcast variables are stored in cache with MEMORY_AND_DISK persistent level. This is where we store cached data and its long-lived.

 

**Formula:**

```
(Java Heap — Reserved Memory) * spark.memory.fraction * spark.memory.storageFraction
```

### **Execution Memory:**

Execution Memory is used for storing the objects required during the execution of Spark tasks.

 

For example, it is used to store shuffle intermediate buffer on the Map side in memory. Also, it is used to store hash table for hash aggregation step.

 

This pool also supports spilling on disk if not enough memory is available, but the blocks from this pool cannot be forcefully evicted by other threads (tasks).

 

Execution memory tends to be more short-lived than storage. It is evicted immediately after each operation, making space for the next ones.

 

**Formula:**

```
(Java Heap — Reserved Memory) * spark.memory.fraction * (1.0 - spark.memory.storageFraction
```

In Spark 1.6+, there is no hard boundary between Execution memory and Storage memory.

Due to the nature of Execution memory, blocks cannot be forcefully evicted from this pool, otherwise, execution will break since the block it refers to won’t be found.

 

But when it comes to Storage memory, blocks can be evicted from memory and written to disk or recomputed (if persistence level is MEMORY_ONLY) as required.

 

**Storage and Execution pool borrowing rules:**

- Storage memory can borrow space from execution memory only if blocks are not used in Execution memory.
- Execution memory can also borrow space from Storage memory if blocks are not used in Storage memory.
- If blocks from Execution memory is used by Storage memory, and Execution needs more memory, it can forcefully evict the excess blocks occupied by Storage Memory
- If blocks from Storage Memory is used by Execution memory and Storage needs more memory, it cannot forcefully evict the excess blocks occupied by Execution Memory; it will end up having less memory area. It will wait until Spark releases the excess blocks stored by Execution memory and then occupies them.

**Calculate the Memory for 5GB executor memory:**

To calculate Reserved memory, User memory, Spark memory, Storage memory, and Execution memory, we will use the following parameters:

```
spark.executor.memory=5g
spark.memory.fraction=0.6
spark.memory.storageFraction=0.5
```

```
Java Heap Memory       = 5 GB
                       = 5 * 1024 MB
                       = 5120 MB

Reserved Memory        = 300 MB

Usable Memory          = (Java Heap Memory — Reserved Memory)
                       = 5120 MB - 300 MB
                       = 4820 MB

User Memory            = Usable Memory * (1.0 — spark.memory.fraction) 
                       = 4820 MB * (1.0 - 0.6) 
                       = 4820 MB * 0.4 
                       = 1928 MB

Spark Memory           = Usable Memory * spark.memory.fraction
                       = 4820 MB * 0.6 
                       = 2892 MB

Spark Storage Memory   = Spark Memory * spark.memory.storageFraction
                       = 2892 MB * 0.5 
                       = 1446 MB

Spark Execution Memory = Spark Memory * (1.0 - spark.memory.storageFraction)
                       = 2892 MB * ( 1 - 0.5) 
                       = 2892 MB * 0.5 
                       = 1446 MB
```


![Spark Architecture diagram](/WhatNextAlgo/spark_architecture/assets/images/Unified_Memory_Manager_5GB.jpg){:width="75%" height="75%"}


```
Reserved Memory —  300 MB 	—	5.85%
User Memory 	— 1928 MB 	— 	37.65%
Spark Memory 	— 2892 MB 	—	56.48%
```


### **Off-Heap Memory (External memory)**

Off Heap memory means allocating memory objects (serialized to byte array) to memory outside the heap of the Java virtual machine(JVM), which is directly managed by the operating system (not the virtual machine), but stored outside the process heap in native memory (therefore, they are not processed by the garbage collector).

 

The result of this is to keep a smaller heap to reduce the impact of garbage collection on the application.

 

Accessing this data is slightly slower than accessing the on-heap storage, but still faster than reading/writing from a disk. The downside is that the user has to manually deal with managing the allocated memory.

 

This model does not apply within the JVM memory, but calls the Java API for the unsafe related language, such as C, inside malloc () directly to the operating system for memory. Since this method is not been to the JVM memory management, so avoid frequent GC. The disadvantage of this application is that memory must write their own logic and memory applications release.

 

Spark 1.6+ began to introduce Off-heap memory [SPARK-11389](https://issues.apache.org/jira/browse/SPARK-11389){:target="_blank"}. Unified Memory Manager can optionally be allocated using off-heap memory.

| Parameter                    | Description                                                                                                              |
| ---------------------------- | ------------------------------------------------------------------------------------------------------------------------ |
| `spark.memory.offHeap.enabled`| Use off-heap memory for certain operations. Default: false                                                                |
| `spark.memory.offHeap.size`   | Total off-heap memory in bytes. No impact on heap memory usage. Ensure it doesn't exceed executor’s total limits. Default: 0 |
{: .table-responsive-scroll}


By default, Off-heap memory is disabled, but we can enable it by the `spark.memory.offHeap.enabled (false by default)` parameter, and set the memory size by `spark.memory.offHeap.size (0 by default)` parameter.

```
spark-shell \
    --conf spark.memory.offHeap.enabled=true \
    --conf spark.memory.offHeap.size=5g
```

**Off-heap memory** supports **OFF_HEAP** persistence level. Compared to the on-heap memory, the model of the off-heap memory is relatively simple, including only Storage Memory and Execution Memory.



![Spark Architecture diagram](/WhatNextAlgo/spark_architecture/assets/images/OffHeap_Memory_Model.jpg){:width="75%" height="75%"}



If the **off-heap memory** is enabled, there will be **both on-heap** and **off-heap memory** in the Executor.


![Spark Architecture diagram](/WhatNextAlgo/spark_architecture/assets/images/offheap_memory_enabled.png){:width="90%" height="90%"}

The **Execution Memory** in the **Executor** is the sum of the **Execution memory inside the heap** and the **Execution memory outside the heap**. The same is true for **Storage Memory**.

 

**Note: Project Techyon** supports in-memory storage for **Spark RDDs** in **off-heap space**.

**Project Tungsten** supports **storing shuffle objects** in **off-heap space**.

**Advantage(s):**

- It can still reduce memory usage, reduce frequent GC, and improve program performance.
- When an executor is killed, all cached data for that executor would be gone but with off-heap memory, the data would still persist. The lifetime of JVM and lifetime of cached data are decoupled.

**Disadvantage(s):**

- By using **OFF_HEAP** does not back up data, nor can it **guarantee high data availability** like alluxio does, and data loss requires recalculation.


**6. Understand the Memory Allocation using Spark UI**

- **6.1 Using On Heap Memory:**
Let's launch the spark shell with 5GB On Heap Memory to understand the Storage Memory in Spark UI.

```
spark-shell \
    --driver-memory 5g \
    --executor-memory 5g
```

Let's see available **Storage Memory** displayed on the **Spark UI Executor** tab is **2.7 GB**, as follows:

![Spark Architecture diagram](/WhatNextAlgo/spark_architecture/assets/images/storage_memory.png){:width="90%" height="90%"}


Based on our **5GB calculation**, we can see the following memory values:

```
Java Heap Memory       = 5 GB
Reserved Memory        = 300 MB
Usable Memory          = 4820 MB
User Memory            = 1928 MB
Spark Memory           = 2892 MB = 2.8242 GB
Spark Storage Memory   = 1446 MB = 1.4121 GB
Spark Execution Memory = 1446 MB = 1.4121 GB
```

From **Spark UI**, the **Storage Memory** value is **2.7 GB** and from our calculation, the **Storage Memory** value is **1.4121 GB**. **Both Storage Memory values** are **not matched** because from **Spark UI Storage Memory** value is **sum of Storage Memory** and **Execution Memory**.

```
Storage Memory = Spark Storage Memory + Spark Execution Memory
               = 1.4121 GB + 1.4121 GB
               = 2.8242 GB
```

We can see still **Spark UI Storage Memory (2.7 GB)** is **not matched** with the above memory calculation **Storage Memory (2.8242 GB)** because we set **--executor-memory** as **5g**. The memory obtained by **Spark's Executor** through **Runtime.getRuntime.maxMemory** is **4772593664 bytes**, so **Java Heap Memory** is only **4772593664 bytes**.


```
Java Heap Memory       = 4772593664 bytes = 4772593664/(1024 * 1024) = 4551 MB 
Reserved Memory        = 300 MB
Usable Memory          = (Java Heap Memory - Reserved Memory) = (4551 - 300) MB = 4251 MB
User Memory            = (Usable Memory * (1 -spark.memory.fraction)) = 1700.4 MB
Spark Memory           = (Usable Memory * spark.memory.fraction) = 2550.6 MB
Spark Storage Memory   = 1275.3 MB
Spark Execution Memory = 1275.3 MB
```

**Spark Memory (2550.6 MB/2.4908 GB)** still does not match what is displayed on the **Spark UI (2.7 GB)** because while converting **Java Heap Memory bytes** into MB we used **1024 * 1024** but in Spark UI converts bytes by dividing by **1000 * 1000**.

```
Java Heap Memory       = 4772593664 bytes = 4772593664/(1000 * 1000) = 4772.593664 MB 
Reserved Memory        = 300 MB
Usable Memory          = (Java Heap Memory - Reserved Memory) = (4472.593664 - 300) MB = 4472.593664 MB
User Memory            = (Usable Memory * (1 -spark.memory.fraction)) = 1789.0374656 MB
Spark Memory           = (Usable Memory * spark.memory.fraction) = 2683.5561984 MB = ~ 2.7 GB
Spark Storage Memory   = 1341.7780992 MB
Spark Execution Memory = 1341.7780992 MB
```

**Logic for converting bytes into GB:**

**Spark 2.X**

```
function formatBytes(bytes, type) {
    if (type !== 'display') return bytes;
    if (bytes == 0) return '0.0 B';
    var k = 1000;
    var dm = 1;
    var sizes = ['B', 'KB', 'MB', 'GB', 'TB', 'PB', 'EB', 'ZB', 'YB'];
    var i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(dm)) + ' ' + sizes[i];
}
```

**Spark 3.X**

```
function formatBytes(bytes, type) {
  if (type !== 'display') return bytes;
  if (bytes <= 0) return '0.0 B';
  var k = 1024;
  var dm = 1;
  var sizes = ['B', 'KiB', 'MiB', 'GiB', 'TiB', 'PiB', 'EiB', 'ZiB', 'YiB'];
  var i = Math.floor(Math.log(bytes) / Math.log(k));
  return parseFloat((bytes / Math.pow(k, i)).toFixed(dm)) + ' ' + sizes[i];
}

```

**6.2 Using On Heap Memory + Off Heap Memory:**

Let's launch the spark shell with **1GB On Heap memory** and **5GB Off Heap memory** to understand the **Storage Memory**.

```
spark-shell \
    --driver-memory 1g \
    --executor-memory 1g \
    --conf spark.memory.offHeap.enabled=true \
    --conf spark.memory.offHeap.size=5g
```

```
Storage Memory = On Heap Memory + Off Heap Memory
```

### **Memory Calculation:**

**On Heap Memory**

```
Java Heap Memory       = 954728448 bytes = 954728448/1000/1000 = 954 MB
Reserved Memory        = 300 MB
Usable Memory          = (Java Heap Memory - Reserved Memory) = (954 - 300) MB = 654 MB
User Memory            = (Usable Memory * (1 -spark.memory.fraction)) = 261.6 MB
Spark Memory           = (Usable Memory * spark.memory.fraction) = 392.4 MB
Spark Storage Memory   = 196.2 MB
Spark Execution Memory = 196.2 MB
```

**Off Heap Memory**

```
spark.memory.offHeap.size = 5 GB = 5 * 1000 MB = 5000 MB
```

**Storage Memory**

```
Storage Memory = On Heap Memory + Off Heap Memory
               = 392.4 MB + 5000 MB
               = 5392.4 MB
               = 5.4 GB
```

This concludes the overview of Spark architecture, delving into the intricacies of memory management in Spark. We appreciate your visit to these pages. If you have any further inquiries or require additional information, please feel free to reach out. Thank you for your time and interest in exploring Spark's architecture and memory management details.

<style>
    /* Add this to your CSS file or style section */
.table-responsive-scroll {
  overflow-x: auto;
  display: block;
  width: 100%;
}

</style>