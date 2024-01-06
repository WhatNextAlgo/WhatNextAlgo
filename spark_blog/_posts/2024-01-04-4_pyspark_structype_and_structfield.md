---
layout: post
title:  "4.Exploring PySpark StructType and Schema Operations"
date:   2024-01-04 20:22:54 +0530
category: Spark
order: 4
---

## **Code Explanation:**

### **1. Defining a DataFrame with StructType:**
{% highlight ruby %}
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

structureData = [
    (("James", "", "Smith"), "36636", "M", 3100),
    (("Michael", "Rose", ""), "40288", "M", 4300),
    (("Robert", "", "Williams"), "42114", "M", 1400),
    (("Maria", "Anne", "Jones"), "39192", "F", 5500),
    (("Jen", "Mary", "Brown"), "", "F", -1)
]

structureSchema = StructType([
    StructField('name', StructType([
        StructField('firstname', StringType(), True),
        StructField('middlename', StringType(), True),
        StructField('lastname', StringType(), True)
    ])),
    StructField('id', StringType(), True),
    StructField('gender', StringType(), True),
    StructField('salary', IntegerType(), True)
])

df2 = spark.createDataFrame(data=structureData, schema=structureSchema)
df2.printSchema()
root
 |-- name: struct (nullable = true)
 |    |-- firstname: string (nullable = true)
 |    |-- middlename: string (nullable = true)
 |    |-- lastname: string (nullable = true)
 |-- id: string (nullable = true)
 |-- gender: string (nullable = true)
 |-- salary: integer (nullable = true)

df2.show(truncate=False)
+--------------------+-----+------+------+
|name                |id   |gender|salary|
+--------------------+-----+------+------+
|{James, , Smith}    |36636|M     |3100  |
|{Michael, Rose, }   |40288|M     |4300  |
|{Robert, , Williams}|42114|M     |1400  |
|{Maria, Anne, Jones}|39192|F     |5500  |
|{Jen, Mary, Brown}  |     |F     |-1    |
+--------------------+-----+------+------+
{% endhighlight %}

### **2. Transforming DataFrame and Adding New Column:**
{% highlight ruby %}

from pyspark.sql.functions import col, struct, when

updateDf = (df2.withColumn("otherInfo", struct(
    col("id").alias("identifier"),
    col("gender").alias("gender"),
    col("salary").alias("salary"),
    when(col("salary").cast(IntegerType()) < 2000, "Low")
    .when(col("salary").cast(IntegerType()) < 4000, "Medium")
    .otherwise("High").alias("Salary_Grade")
)).drop("id", "gender", "salary"))

updateDf.printSchema()
root
 |-- name: struct (nullable = true)
 |    |-- firstname: string (nullable = true)
 |    |-- middlename: string (nullable = true)
 |    |-- lastname: string (nullable = true)
 |-- otherInfo: struct (nullable = false)
 |    |-- identifier: string (nullable = true)
 |    |-- gender: string (nullable = true)
 |    |-- salary: integer (nullable = true)
 |    |-- Salary_Grade: string (nullable = false)
updateDf.show(truncate=False)
+--------------------+------------------------+
|name                |otherInfo               |
+--------------------+------------------------+
|{James, , Smith}    |{36636, M, 3100, Medium}|
|{Michael, Rose, }   |{40288, M, 4300, High}  |
|{Robert, , Williams}|{42114, M, 1400, Low}   |
|{Maria, Anne, Jones}|{39192, F, 5500, High}  |
|{Jen, Mary, Brown}  |{, F, -1, Low}          |
+--------------------+------------------------+

{% endhighlight %}

### **3. Creating DataFrame with ArrayType and MapType:**
{% highlight ruby %}
from pyspark.sql.types import ArrayType, MapType

arrayStructSchema = StructType([
    StructField("fullname", StructType([
        StructField("firstname", StringType(), True),
        StructField("middlename", StringType(), True),
        StructField("lastname", StringType(), True)
    ])),
    StructField("hobbies", ArrayType(StringType()), True),
    StructField("properties", MapType(StringType(), StringType()), True)
])

arrayStructDf = spark.createDataFrame([], schema=arrayStructSchema)
arrayStructDf.printSchema()
root
 |-- fullname: struct (nullable = true)
 |    |-- firstname: string (nullable = true)
 |    |-- middlename: string (nullable = true)
 |    |-- lastname: string (nullable = true)
 |-- hobbies: array (nullable = true)
 |    |-- element: string (containsNull = true)
 |-- properties: map (nullable = true)
 |    |-- key: string
 |    |-- value: string (valueContainsNull = true)
{% endhighlight %}

### **4. Extracting Schema as String:**
{% highlight ruby %}
print(df2.schema.simpleString())

output:
struct<name:struct<firstname:string,middlename:string,lastname:string>,id:string,gender:string,salary:int>
{% endhighlight %}

### **5. Creating DataFrame from JSON Schema String:**
{% highlight ruby %}
import json

structureData = """
{
  "type": "struct",
  "fields": [{
    "name": "name",
    "type": {
      "type": "struct",
      "fields": [{
        "name": "firstname",
        "type": "string",
        "nullable": true,
        "metadata": {}
      }, {
        "name": "middlename",
        "type": "string",
        "nullable": true,
        "metadata": {}
      }, {
        "name": "lastname",
        "type": "string",
        "nullable": true,
        "metadata": {}
      }]
    },
    "nullable": true,
    "metadata": {}
  }, {
    "name": "dob",
    "type": "string",
    "nullable": true,
    "metadata": {}
  }, {
    "name": "gender",
    "type": "string",
    "nullable": true,
    "metadata": {}
  }, {
    "name": "salary",
    "type": "integer",
    "nullable": true,
    "metadata": {}
  }]
}
"""

schemaFromJson = StructType.fromJson(json.loads(structureData))
df3 = spark.createDataFrame(spark.sparkContext.parallelize([]), schema=schemaFromJson)
df3.printSchema()

root
 |-- name: struct (nullable = true)
 |    |-- firstname: string (nullable = true)
 |    |-- middlename: string (nullable = true)
 |    |-- lastname: string (nullable = true)
 |-- id: string (nullable = true)
 |-- gender: string (nullable = true)
 |-- salary: integer (nullable = true)
{% endhighlight %}

### **6. Creating DataFrame from DDL String:**
{% highlight ruby %}
ddlSchemaStr = "`fullname` STRUCT<`firstname` STRING,`middlename` STRING,`lastname` STRING>,`age` INT,`gender` STRING"
ddlSchemaStrDf = spark.createDataFrame([], schema=ddlSchemaStr)
ddlSchemaStrDf.printSchema()
root
 |-- fullname: struct (nullable = true)
 |    |-- firstname: string (nullable = true)
 |    |-- middlename: string (nullable = true)
 |    |-- lastname: string (nullable = true)
 |-- age: integer (nullable = true)
 |-- gender: string (nullable = true)
{% endhighlight %}

### **7. Checking if a Field Exists in DataFrame Schema:**
{% highlight ruby %}
print("fullname" in ddlSchemaStrDf.schema.fieldNames())
output:
True
{% endhighlight %}

### **Output Explanation:**
- Each section includes code snippets explaining how to define a DataFrame schema, manipulate data, and create DataFrames from various schema representations.
- The output of each operation is displayed, providing insights into the structure and content of the DataFrames.

Feel free to modify and run the code snippets in a PySpark environment to explore further.




