---
layout: post
title:  "5. Static Memory Manager"
category: Architecture
order: 5
---

```
From Spark 1.0, May 2014
```
Static Memory Manager (SMM) is the traditional model and simple scheme for memory management.

It divides memory into two fixed partitions statically.

The size of Storage Memory and Execution Memory and other memory is fixed during application processing, but users can configure it before the application starts.

 

**Note:** Static Memory allocation method has been eliminated in Spark 3.0

**Advantage:**

- Static Memory Manager mechanism is simple to implement
**Disadvantage:** 

- Even though space is available with storage memory, we can’t use it, and there is a disk spill since executor memory is full. (vice versa).

In Spark 1.6+, Static Memory Management can be enabled via the spark.memory.useLegacyMode=true parameter.


| Parameter                     | Description                                                                                                           |
| ----------------------------- | --------------------------------------------------------------------------------------------------------------------- |
| `spark.memory.useLegacyMode` (default fasle) | The option to divide heap space into fixed-size regions. Default: false                                                 |
| `spark.shuffle.memoryFraction` (default 0.2)| The fraction of the heap used for aggregation and cogroup during shuffles. Works only if `spark.memory.useLegacyMode=true` |
| `spark.storage.memoryFraction` (default 0.6)| The fraction of the heap used for Spark’s memory cache. Works only if `spark.memory.useLegacyMode=true`                |
| `spark.storage.unrollFraction` (default 0.2) | The fraction of `spark.storage.memoryFraction` used for unrolling blocks in the memory. This is dynamically allocated by dropping existing blocks when there is not enough free storage space to unroll the new block in its entirety. Works only if `spark.memory.useLegacyMode=true`. |
{: .table-responsive-scroll}

```
Static memory management does not support the use of off-heap memory for storage, so all of it is allocated to the execution space.
```
<style>
    /* Add this to your CSS file or style section */
.table-responsive-scroll {
  overflow-x: auto;
  display: block;
  width: 100%;
}

</style>
