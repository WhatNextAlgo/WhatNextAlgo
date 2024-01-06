---
layout: post
title:  "2.Convert PySpark RDD to DataFrame"
date:   2024-01-02 17:22:54 +0530
category: Spark
order: 2
---

### Creating RDD and DataFrames in PySpark

#### 1. Creating an RDD and Converting to DataFrame

{% highlight ruby %}

# Define a list of tuples representing department data
dept = [("Finance", 10), ("Marketing", 20), ("Sales", 30), ("IT", 40)]

# Define column names for the DataFrame
deptColumns = ["dept_name", "dept_id"]

# Create an RDD from the department data
rdd = spark.sparkContext.parallelize(dept)
{% endhighlight %}

#### 2. Converting RDD to DataFrame (Method 1)

{% highlight ruby %}

# Convert RDD to DataFrame using the toDF method
df = rdd.toDF(deptColumns)

# Display DataFrame schema and content
df.printSchema()
df.show()
{% endhighlight %}

**Output:**
```
root
 |-- dept_name: string (nullable = true)
 |-- dept_id: long (nullable = true)

+---------+-------+
|dept_name|dept_id|
+---------+-------+
| Finance | 10    |
|Marketing| 20    |
| Sales   | 30    |
| IT      | 40    |
+---------+-------+
```

#### 3. Converting RDD to DataFrame (Method 2)

{% highlight ruby %}
# Create DataFrame using createDataFrame method
deptDF = spark.createDataFrame(rdd, schema=deptColumns)

# Display DataFrame schema and content without truncation
deptDF.printSchema()
deptDF.show(truncate=False)
{% endhighlight %}

**Output:**
```
root
 |-- dept_name: string (nullable = true)
 |-- dept_id: long (nullable = true)

+---------+-------+
|dept_name|dept_id|
+---------+-------+
|Finance  |10     |
|Marketing|20     |
|Sales    |30     |
|IT       |40     |
+---------+-------+
```

#### 4. Converting RDD to DataFrame with Defined Schema

{% highlight ruby %}
# Define a schema using StructType and StructField
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

deptSchema = StructType([
    StructField("dept_name", StringType(), True),
    StructField("dept_id", IntegerType(), True)
])

# Create DataFrame with specified schema
deptDF1 = spark.createDataFrame(rdd, schema=deptSchema)

# Display DataFrame schema and content without truncation
deptDF1.printSchema()
deptDF1.show(truncate=False)
{% endhighlight %}

**Output:**
```
root
 |-- dept_name: string (nullable = true)
 |-- dept_id: integer (nullable = true)

+---------+-------+
|dept_name|dept_id|
+---------+-------+
|Finance  |10     |
|Marketing|20     |
|Sales    |30     |
|IT       |40     |
+---------+-------+
```

In this code, we first create an RDD from a list of tuples representing department data. Then, we convert the RDD into a DataFrame using two different methods, and finally, we demonstrate creating a DataFrame with a predefined schema using `StructType`. The output shows the schema and contents of the resulting DataFrames.





