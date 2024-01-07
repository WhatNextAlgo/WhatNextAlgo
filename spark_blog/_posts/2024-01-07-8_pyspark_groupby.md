---
layout: post
title:  "8. PySpark Groupby Explained with Example"
category: Spark
order: 8
---

### **1.DataFrame Creation:**
{% highlight ruby %}
from pyspark.sql.functions import *
simpleData = [("James","Sales","NY",90000,34,10000),
    ("Michael","Sales","NY",86000,56,20000),
    ("Robert","Sales","CA",81000,30,23000),
    ("Maria","Finance","CA",90000,24,23000),
    ("Raman","Finance","CA",99000,40,24000),
    ("Scott","Finance","NY",83000,36,19000),
    ("Jen","Finance","NY",79000,53,15000),
    ("Jeff","Marketing","CA",80000,25,18000),
    ("Kumar","Marketing","NY",91000,50,21000)
  ]

schema = ["employee_name","department","state","salary","age","bonus"]
df = spark.createDataFrame(data=simpleData, schema = schema)
df.printSchema()
root
 |-- employee_name: string (nullable = true)
 |-- department: string (nullable = true)
 |-- state: string (nullable = true)
 |-- salary: long (nullable = true)
 |-- age: long (nullable = true)
 |-- bonus: long (nullable = true)

df.show(truncate=False)
+-------------+----------+-----+------+---+-----+
|employee_name|department|state|salary|age|bonus|
+-------------+----------+-----+------+---+-----+
|James        |Sales     |NY   |90000 |34 |10000|
|Michael      |Sales     |NY   |86000 |56 |20000|
|Robert       |Sales     |CA   |81000 |30 |23000|
|Maria        |Finance   |CA   |90000 |24 |23000|
|Raman        |Finance   |CA   |99000 |40 |24000|
|Scott        |Finance   |NY   |83000 |36 |19000|
|Jen          |Finance   |NY   |79000 |53 |15000|
|Jeff         |Marketing |CA   |80000 |25 |18000|
|Kumar        |Marketing |NY   |91000 |50 |21000|
+-------------+----------+-----+------+---+-----+

{% endhighlight %}

### **2. GroupBy Operation Syntax:**
{% highlight ruby %}
df.groupBy("department").sum("salary").show(truncate=False)
+----------+-----------+
|department|sum(salary)|
+----------+-----------+
|Sales     |257000     |
|Finance   |351000     |
|Marketing |171000     |
+----------+-----------+

df.groupBy("department").count().show(truncate=False)
+----------+-----+
|department|count|
+----------+-----+
|Sales     |3    |
|Finance   |4    |
|Marketing |2    |
+----------+-----+

df.groupBy("department").min("salary").show(truncate=False)
+----------+-----------+
|department|min(salary)|
+----------+-----------+
|Sales     |81000      |
|Finance   |79000      |
|Marketing |80000      |
+----------+-----------+

df.groupBy("department").max("salary").show(truncate=False)
+----------+-----------+
|department|max(salary)|
+----------+-----------+
|Sales     |90000      |
|Finance   |99000      |
|Marketing |91000      |
+----------+-----------+

df.groupBy("department").avg( "salary").show(truncate=False)
+----------+-----------------+
|department|avg(salary)      |
+----------+-----------------+
|Sales     |85666.66666666667|
|Finance   |87750.0          |
|Marketing |85500.0          |
+----------+-----------------+

df.groupBy("department").mean( "salary").show(truncate=False)
+----------+-----------------+
|department|avg(salary)      |
+----------+-----------------+
|Sales     |85666.66666666667|
|Finance   |87750.0          |
|Marketing |85500.0          |
+----------+-----------------+
df.groupBy("department","state").sum("salary","bonus").show(truncate=False)
+----------+-----+-----------+----------+
|department|state|sum(salary)|sum(bonus)|
+----------+-----+-----------+----------+
|Sales     |NY   |176000     |30000     |
|Sales     |CA   |81000      |23000     |
|Finance   |CA   |189000     |47000     |
|Finance   |NY   |162000     |34000     |
|Marketing |NY   |91000      |21000     |
|Marketing |CA   |80000      |18000     |
+----------+-----+-----------+----------+

{% endhighlight %}

### **3. GroupBy and Aggregation operation:**

{% highlight ruby %}
from pyspark.sql.functions import sum,avg,max
df.groupBy("department") \
    .agg(sum("salary").alias("sum_salary"), \
         avg("salary").alias("avg_salary"), \
         sum("bonus").alias("sum_bonus"), \
         max("bonus").alias("max_bonus") \
     ) \
    .show(truncate=False)

+----------+----------+-----------------+---------+---------+
|department|sum_salary|avg_salary       |sum_bonus|max_bonus|
+----------+----------+-----------------+---------+---------+
|Sales     |257000    |85666.66666666667|53000    |23000    |
|Finance   |351000    |87750.0          |81000    |24000    |
|Marketing |171000    |85500.0          |39000    |21000    |
+----------+----------+-----------------+---------+---------+
{% endhighlight %}

### **3. GroupBy and Aggregation operation with where clause:**

{% highlight ruby %}
from pyspark.sql.functions import sum,avg,max
(df.groupBy("department")
   .agg(sum("salary").alias("sum_salary"),
       avg("salary").alias("avg_salary"),
       sum("bonus").alias("sum_bonus"),
       max("bonus").alias("max_bonus"))
   .where(col("sum_bonus") >= 50000)
   .show(truncate=False)
)


+----------+----------+-----------------+---------+---------+
|department|sum_salary|avg_salary       |sum_bonus|max_bonus|
+----------+----------+-----------------+---------+---------+
|Sales     |257000    |85666.66666666667|53000    |23000    |
|Finance   |351000    |87750.0          |81000    |24000    |
+----------+----------+-----------------+---------+---------+
{% endhighlight %}
