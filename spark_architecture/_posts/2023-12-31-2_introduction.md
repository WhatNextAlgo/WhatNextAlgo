---
layout: post
title:  "2. Introduction"
category: Architecture
order: 2
---

### **Introduction**
Spark is an in-memory processing engine where all of the computation that a task does happen in-memory. So, it is important to understand Spark Memory Management. This will help us develop the Spark applications and perform performance tuning.

 

If the memory allocation is too large when committing, it will occupy resources. If the memory allocation is too small, memory overflow and full GC problems will occur easily.

```
Efficient memory use is critical for good performance, but the reverse is also true – inefficient memory use leads to bad performance.
```
### **Spark Architecture:**

![Spark Architecture diagram](/WhatNextAlgo/spark_architecture/assets/images/sparkplug-cluster-architecture.png){:width="75%" height="75%"}


Spark Application includes two JVM processes, Driver and Executor.

- The Driver is the main control process, which is responsible for creating the SparkSession/SparkContext, submitting the Job, converting the Job to Task, and coordinating the Task execution between executors.
- The Executor is mainly responsible for performing specific calculation tasks and returning the results to the Driver.

Driver's memory management is relatively simple, Spark does not make specific plans.

In this article, we can analyze Executor memory management.