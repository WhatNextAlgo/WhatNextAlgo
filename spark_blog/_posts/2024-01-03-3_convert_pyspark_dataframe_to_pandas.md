---
layout: post
title:  "3.Convert PySpark DataFrame to Pandas"
date:   2024-01-03 19:22:54 +0530
category: Spark
order: 3
---

# **PySpark DataFrame Conversion and StructType Example**

In this example, we demonstrate the creation of a PySpark DataFrame and its conversion to a Pandas DataFrame. Additionally, we showcase the creation of another PySpark DataFrame with a more complex structure using StructType.

### **1. Creating and Displaying PySpark DataFrame:**

```python
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Define data and schema for the PySpark DataFrame
data = [("James", "", "Smith", "36636", "M", 60000),
        ("Michael", "Rose", "", "40288", "M", 70000),
        ("Robert", "", "Williams", "42114", "", 400000),
        ("Maria", "Anne", "Jones", "39192", "F", 500000),
        ("Jen", "Mary", "Brown", "", "F", 0)]
columns = ["first_name", "middle_name", "last_name", "dob", "gender", "salary"]
pysparkDF = spark.createDataFrame(data=data, schema=columns)

# Display schema and data
pysparkDF.printSchema()
pysparkDF.show(truncate=False)
```

Output:
```
root
 |-- first_name: string (nullable = true)
 |-- middle_name: string (nullable = true)
 |-- last_name: string (nullable = true)
 |-- dob: string (nullable = true)
 |-- gender: string (nullable = true)
 |-- salary: integer (nullable = true)

+----------+-----------+---------+-----+------+------+
|first_name|middle_name|last_name|dob  |gender|salary|
+----------+-----------+---------+-----+------+------+
|James     |           |Smith    |36636|M     |60000 |
|Michael   |Rose       |         |40288|M     |70000 |
|Robert    |           |Williams |42114|      |400000|
|Maria     |Anne       |Jones    |39192|F     |500000|
|Jen       |Mary       |Brown    |     |F     |0     |
+----------+-----------+---------+-----+------+------+
```
### **2. Converting PySpark DataFrame to Pandas DataFrame:**

```python
pandasDf = pysparkDF.toPandas()
print(pandasDf)
```

Output:
```
  first_name middle_name last_name    dob gender  salary
0      James                       Smith  36636      M   60000
1    Michael        Rose           40288      M   70000
2     Robert                 Williams  42114           400000
3      Maria        Anne     Jones  39192      F   500000
4        Jen        Mary     Brown               F       0
```
### **3. Creating PySpark DataFrame with StructType:**

```python
dataStruct = [(("James", "", "Smith"), "36636", "M", "3000"),
              (("Michael", "Rose", ""), "40288", "M", "4000"),
              (("Robert", "", "Williams"), "42114", "M", "4000"),
              (("Maria", "Anne", "Jones"), "39192", "F", "4000"),
              (("Jen", "Mary", "Brown"), "", "F", "-1")]

schemaStruct = StructType([
    StructField("name", StructType([
        StructField("firstname", StringType(), True),
        StructField("middlename", StringType(), True),
        StructField("lastname", StringType(), True)
    ])),
    StructField("dob", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("salary", StringType(), True)
])

df = spark.createDataFrame(data=dataStruct, schema=schemaStruct)

# Display schema
df.printSchema()
```

Output:
```
root
 |-- name: struct (nullable = true)
 |    |-- firstname: string (nullable = true)
 |    |-- middlename: string (nullable = true)
 |    |-- lastname: string (nullable = true)
 |-- dob: string (nullable = true)
 |-- gender: string (nullable = true)
 |-- salary: string (nullable = true)
```

### **4. Converting PySpark DataFrame with StructType to Pandas DataFrame:**

```python
pandasDF2 = df.toPandas()
print(pandasDF2)
```

Output:
```
                   name    dob gender salary
0        (James, , Smith)  36636      M   3000
1       (Michael, Rose, )  40288      M   4000
2  (Robert, , Williams)  42114      M   4000
3  (Maria, Anne, Jones)  39192      F   4000
4      (Jen, Mary, Brown)               F     -1
```

This post provides a detailed explanation of the PySpark code, including the creation of DataFrames, conversion to Pandas, and the use of StructType for more complex data structures.





