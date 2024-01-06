---
layout: post
title:  "6. PySpark Where Filter Function | Multiple Conditions"
date:   2024-01-05 21:22:54 +0530
category: Spark
order: 6
---

### **1. PySpark DataFrame filter() Syntax:**
{% highlight ruby %}

from pyspark.sql.types import StructType,StructField 
from pyspark.sql.types import StringType, IntegerType, ArrayType
data = [
    (("James","","Smith"),["Java","Scala","C++"],"OH","M"),
    (("Anna","Rose",""),["Spark","Java","C++"],"NY","F"),
    (("Julia","","Williams"),["CSharp","VB"],"OH","F"),
    (("Maria","Anne","Jones"),["CSharp","VB"],"NY","M"),
    (("Jen","Mary","Brown"),["CSharp","VB"],"NY","M"),
    (("Mike","Mary","Williams"),["Python","VB"],"OH","M")
 ]
        
schema = StructType([
     StructField('name', StructType([
        StructField('firstname', StringType(), True),
        StructField('middlename', StringType(), True),
         StructField('lastname', StringType(), True)
     ])),
     StructField('languages', ArrayType(StringType()), True),
     StructField('state', StringType(), True),
     StructField('gender', StringType(), True)
 ])

df = spark.createDataFrame(data = data, schema = schema)
df.printSchema()
root
 |-- name: struct (nullable = true)
 |    |-- firstname: string (nullable = true)
 |    |-- middlename: string (nullable = true)
 |    |-- lastname: string (nullable = true)
 |-- languages: array (nullable = true)
 |    |-- element: string (containsNull = true)
 |-- state: string (nullable = true)
 |-- gender: string (nullable = true)

df.show(truncate=False)
+----------------------+------------------+-----+------+
|name                  |languages         |state|gender|
+----------------------+------------------+-----+------+
|{James, , Smith}      |[Java, Scala, C++]|OH   |M     |
|{Anna, Rose, }        |[Spark, Java, C++]|NY   |F     |
|{Julia, , Williams}   |[CSharp, VB]      |OH   |F     |
|{Maria, Anne, Jones}  |[CSharp, VB]      |NY   |M     |
|{Jen, Mary, Brown}    |[CSharp, VB]      |NY   |M     |
|{Mike, Mary, Williams}|[Python, VB]      |OH   |M     |
+----------------------+------------------+-----+------+
{% endhighlight %}

### **2. DataFrame filter() with Column Condition:**
{% highlight ruby %}
df.filter(df.state != 'OH').show()
+--------------------+------------------+-----+------+
|                name|         languages|state|gender|
+--------------------+------------------+-----+------+
|      {Anna, Rose, }|[Spark, Java, C++]|   NY|     F|
|{Maria, Anne, Jones}|      [CSharp, VB]|   NY|     M|
|  {Jen, Mary, Brown}|      [CSharp, VB]|   NY|     M|
+--------------------+------------------+-----+------+
df.filter(df.state == 'OH').show()
+--------------------+------------------+-----+------+
|                name|         languages|state|gender|
+--------------------+------------------+-----+------+
|    {James, , Smith}|[Java, Scala, C++]|   OH|     M|
| {Julia, , Williams}|      [CSharp, VB]|   OH|     F|
|{Mike, Mary, Will...|      [Python, VB]|   OH|     M|
+--------------------+------------------+-----+------+

from pyspark.sql.functions import col
df.filter(col("state") == "OH").show(truncate=False)
+----------------------+------------------+-----+------+
|name                  |languages         |state|gender|
+----------------------+------------------+-----+------+
|{James, , Smith}      |[Java, Scala, C++]|OH   |M     |
|{Julia, , Williams}   |[CSharp, VB]      |OH   |F     |
|{Mike, Mary, Williams}|[Python, VB]      |OH   |M     |
+----------------------+------------------+-----+------+

{% endhighlight %}

### **3. DataFrame filter() with SQL Expression:**
{% highlight ruby %}

#Using SQL Expression
df.filter("gender == 'M'").show()
+--------------------+------------------+-----+------+
|                name|         languages|state|gender|
+--------------------+------------------+-----+------+
|    {James, , Smith}|[Java, Scala, C++]|   OH|     M|
|{Maria, Anne, Jones}|      [CSharp, VB]|   NY|     M|
|  {Jen, Mary, Brown}|      [CSharp, VB]|   NY|     M|
|{Mike, Mary, Will...|      [Python, VB]|   OH|     M|
+--------------------+------------------+-----+------+
#For not equal
df.filter("gender != 'M'").show()
+-------------------+------------------+-----+------+
|               name|         languages|state|gender|
+-------------------+------------------+-----+------+
|     {Anna, Rose, }|[Spark, Java, C++]|   NY|     F|
|{Julia, , Williams}|      [CSharp, VB]|   OH|     F|
+-------------------+------------------+-----+------+
df.filter("gender <> 'M'").show()
+-------------------+------------------+-----+------+
|               name|         languages|state|gender|
+-------------------+------------------+-----+------+
|     {Anna, Rose, }|[Spark, Java, C++]|   NY|     F|
|{Julia, , Williams}|      [CSharp, VB]|   OH|     F|
+-------------------+------------------+-----+------+
{% endhighlight %}

### **4. PySpark Filter with Multiple Conditions:**
{% highlight ruby %}
df.filter((df.state == "OH") & (df.gender == "M")).show(truncate=False)
+----------------------+------------------+-----+------+
|name                  |languages         |state|gender|
+----------------------+------------------+-----+------+
|{James, , Smith}      |[Java, Scala, C++]|OH   |M     |
|{Mike, Mary, Williams}|[Python, VB]      |OH   |M     |
+----------------------+------------------+-----+------+
{% endhighlight %}

### **5. Filter Based on List Values:**
{% highlight ruby %}
li=["OH","CA","DE"]
df.filter(df.state.isin(li)).show(truncate=False)
+----------------------+------------------+-----+------+
|name                  |languages         |state|gender|
+----------------------+------------------+-----+------+
|{James, , Smith}      |[Java, Scala, C++]|OH   |M     |
|{Julia, , Williams}   |[CSharp, VB]      |OH   |F     |
|{Mike, Mary, Williams}|[Python, VB]      |OH   |M     |
+----------------------+------------------+-----+------+

df.filter(~df.state.isin(li)).show(truncate=False)
+--------------------+------------------+-----+------+
|name                |languages         |state|gender|
+--------------------+------------------+-----+------+
|{Anna, Rose, }      |[Spark, Java, C++]|NY   |F     |
|{Maria, Anne, Jones}|[CSharp, VB]      |NY   |M     |
|{Jen, Mary, Brown}  |[CSharp, VB]      |NY   |M     |
+--------------------+------------------+-----+------+

df.filter(df.state.isin(li) == False).show(truncate=False)

+--------------------+------------------+-----+------+
|name                |languages         |state|gender|
+--------------------+------------------+-----+------+
|{Anna, Rose, }      |[Spark, Java, C++]|NY   |F     |
|{Maria, Anne, Jones}|[CSharp, VB]      |NY   |M     |
|{Jen, Mary, Brown}  |[CSharp, VB]      |NY   |M     |
+--------------------+------------------+-----+------+

{% endhighlight %}

### **6. Filter Based on Starts With, Ends With, Contains:**
{% highlight ruby %}

# Using startswith
df.filter(df.state.startswith("N")).show()
+--------------------+------------------+-----+------+
|                name|         languages|state|gender|
+--------------------+------------------+-----+------+
|      {Anna, Rose, }|[Spark, Java, C++]|   NY|     F|
|{Maria, Anne, Jones}|      [CSharp, VB]|   NY|     M|
|  {Jen, Mary, Brown}|      [CSharp, VB]|   NY|     M|
+--------------------+------------------+-----+------+

#using endswith
df.filter(df.state.endswith("H")).show()

+--------------------+------------------+-----+------+
|                name|         languages|state|gender|
+--------------------+------------------+-----+------+
|    {James, , Smith}|[Java, Scala, C++]|   OH|     M|
| {Julia, , Williams}|      [CSharp, VB]|   OH|     F|
|{Mike, Mary, Will...|      [Python, VB]|   OH|     M|
+--------------------+------------------+-----+------+

#contains
df.filter(df.state.contains("H")).show()
+--------------------+------------------+-----+------+
|                name|         languages|state|gender|
+--------------------+------------------+-----+------+
|    {James, , Smith}|[Java, Scala, C++]|   OH|     M|
| {Julia, , Williams}|      [CSharp, VB]|   OH|     F|
|{Mike, Mary, Will...|      [Python, VB]|   OH|     M|
+--------------------+------------------+-----+------+
{% endhighlight %}

### **7. PySpark Filter like and rlike:**
{% highlight ruby %}

data2 = [(2,"Michael Rose"),(3,"Robert Williams"),
     (4,"Rames Rose"),(5,"Rames rose")
  ]
df2 = spark.createDataFrame(data = data2, schema = ["id","name"])

# like - SQL LIKE pattern
df2.filter(df2.name.like("%rose%")).show()
+---+----------+
| id|      name|
+---+----------+
|  5|Rames rose|
+---+----------+

# rlike - SQL RLIKE pattern (LIKE with Regex)
#This check case insensitive
df2.filter(df2.name.rlike("(?i)^*rose$")).show()
+---+------------+
| id|        name|
+---+------------+
|  2|Michael Rose|
|  4|  Rames Rose|
|  5|  Rames rose|
+---+------------+

{% endhighlight %}

### **8. Filter on an Array column:**
{% highlight ruby %}
{% endhighlight %}

### **1. PySpark DataFrame filter() Syntax:**
{% highlight ruby %}
from pyspark.sql.functions import array_contains
df.filter(array_contains(df.languages,"Java")).show(truncate=False)
+----------------+------------------+-----+------+
|name            |languages         |state|gender|
+----------------+------------------+-----+------+
|{James, , Smith}|[Java, Scala, C++]|OH   |M     |
|{Anna, Rose, }  |[Spark, Java, C++]|NY   |F     |
+----------------+------------------+-----+------+
{% endhighlight %}

### **9. Filtering on Nested Struct columns:**
{% highlight ruby %}
df.filter(df.name.lastname == "Williams") \
    .show(truncate=False) 
+----------------------+------------+-----+------+
|name                  |languages   |state|gender|
+----------------------+------------+-----+------+
|{Julia, , Williams}   |[CSharp, VB]|OH   |F     |
|{Mike, Mary, Williams}|[Python, VB]|OH   |M     |
+----------------------+------------+-----+------+
{% endhighlight %}



