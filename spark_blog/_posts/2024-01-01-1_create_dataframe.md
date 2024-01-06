---
layout: post
title:  "1.Create an Empty DataFrame"
date:   2024-01-01 17:22:54 +0530
category: Spark
order: 1
---

In PySpark, you can create a Spark session using the `SparkSession` class. Here's an example of how to create a Spark session:

```python
from pyspark.sql import SparkSession

# Create a Spark session
spark = SparkSession.builder \
    .appName("PySpark Tutorial") \  # Replace with your desired application name
    .config("spark.some.config.option", "some-value") \  # Add any additional configuration options
    .getOrCreate()

# Now, you can use the 'spark' object to work with Spark functionality
```

In the code above:

- `appName("YourAppName")`: Set a name for your Spark application. Replace "YourAppName" with a meaningful name for your application.
  
- `.config("spark.some.config.option", "some-value")`: You can add any additional configuration options here based on your requirements. This is optional.

- `getOrCreate()`: This method either gets the existing Spark session or creates a new one if it doesn't exist.

Once you have created the Spark session, you can use the `spark` object to perform various Spark operations, such as reading/writing data, creating DataFrames, and executing Spark SQL queries.


### **1.Create Empty RDD in PySpark:**

There two ways to create an empty Resilient Distributed Dataset (RDD) in PySpark. Both methods result in an RDD with no elements. Here's a brief explanation of each method:

1. **Using `emptyRDD()` method:**
   {% highlight ruby %}
   emptyRDD = spark.sparkContext.emptyRDD()
   print(emptyRDD)
   {% endhighlight %}

   The `emptyRDD()` method from the PySpark `SparkContext` class is used to create an empty RDD.

2. **Using `parallelize([])` method:**
   {% highlight ruby %}
   rdd2 = spark.sparkContext.parallelize([])
   print(rdd2)
   {% endhighlight %}

   Another way to create an empty RDD is by using the `parallelize` method with an empty list (`[]`). This method distributes an empty collection across the Spark cluster, resulting in an empty RDD.

Both approaches are valid and will give you an RDD with zero elements. The choice between them depends on your preference or specific use case.

### **2. Create Empty DataFrame with Schema (StructType):**

{% highlight ruby %}
from pyspark.sql.types import StructType,StructField,StringType,IntegerType,TimestampType

schema = StructType([
  StructField("firstname",StringType(),True),
  StructField("middlename",StringType(),True),
  StructField("lastname",StringType(),True)
])

df = spark.createDataFrame([],schema=schema)
df.printSchema()

{% endhighlight %}

### **3. Convert Empty RDD to DataFrame:**

{% highlight ruby %}
df1 = emptyRDD.toDF(schema)
df1.printSchema()
{% endhighlight %}


### **4. Create Empty DataFrame with Schema:**

{% highlight ruby %}
df2 = spark.createDataFrame([],schema=schema)
df2.printSchema()
{% endhighlight %}


### **5. Create Empty DataFrame without Schema (no columns):**

{% highlight ruby %}
df3 = spark.createDataFrame([],schema = StructType([]))
df3.printSchema()
{% endhighlight %}





