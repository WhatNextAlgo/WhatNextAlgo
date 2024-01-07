---
layout: post
title:  "3. Executor Memory"
category: Architecture
order: 3
---

Executor acts as a JVM process launched on a worker node. So, it is important to understand JVM memory management.

 

JVM memory management is categorized into two types:

- On-Heap memory management (In-Heap memory) - Objects are allocated on the JVM Heap and bound by GC.
- Off-Heap memory management (External memory) - Objects are allocated in memory outside the JVM by serialization, managed by the application, and are not bound by GC.

![Spark Architecture diagram](/WhatNextAlgo/spark_architecture/assets/images/jvm_memory_types.jpg){:width="75%" height="75%"}

**In general, the objects' read and write speed is:**
```
on-heap > off-heap > disk
```