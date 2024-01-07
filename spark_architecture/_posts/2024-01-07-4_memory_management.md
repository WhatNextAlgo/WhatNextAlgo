---
layout: post
title:  "4. Memory Management"
category: Architecture
order: 4
---

Spark Memory Management is divided into two types:

- Static Memory Manager (Static Memory Management), and
- Unified Memory Manager (Unified memory management)

![Spark Architecture diagram](/WhatNextAlgo/spark_architecture/assets/images/MemoryManagerTypes.jpg){:width="75%" height="75%"}

Since Spark 1.6.0, Unified Memory Manager has been set as the default memory manager for Spark. 

Static Memory Manager has been deprecated because of the lack of flexibility.

 

In both memory managers, a portion of Java Heap is located for processing Spark applications, while the rest of memory is reserved for Java class references and metadata usage.

 

Note: There will only be one MemoryManager per JVM.
```
// Determine whether to use the old memory management mode
val useLegacyMemoryManager = conf.getBoolean("spark.memory.useLegacyMode", false)

val memoryManager: MemoryManager =
if (useLegacyMemoryManager) {
        // The old version uses static memory management
        new StaticMemoryManager(conf, numUsableCores)
} else {
        // The new version uses unified memory management
        UnifiedMemoryManager(conf, numUsableCores)
}
```

To store the information about the memory, both memory managers will use two memory pools i.e.

- ExecutionMemoryPool and
- StorageMemoryPool