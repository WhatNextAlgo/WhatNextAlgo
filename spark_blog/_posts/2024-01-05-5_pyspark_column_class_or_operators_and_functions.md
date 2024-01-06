---
layout: post
title:  "5.PySpark Column Class | Operators & Functions"
date:   2024-01-05 21:22:54 +0530
category: Spark
order: 5
---

### **1. Create Column Class Object:**
{% highlight ruby %}
from pyspark.sql.functions import lit
colobj = lit("sparkpractice")

data = [("James", 23), ("Ann", 40)]
df = spark.createDataFrame(data, schema="name STRING, age INT")
df.printSchema()
"""
root
 |-- name: string (nullable = true)
 |-- age: integer (nullable = true)
"""
# Using DataFrame object df
df.select(df.age).show()
df.select(df["age"]).show()

output:
+---+
|age|
+---+
| 23|
| 40|
+---+

+---+
|age|
+---+
| 23|
| 40|
+---+

# Using SQL col() functions
from pyspark.sql.functions import col

df.select(col("age")).show()
df.select(col("`age`")).show()

output:
+---+
|age|
+---+
| 23|
| 40|
+---+

+---+
|age|
+---+
| 23|
| 40|
+---+

#Create DataFrame with struct using the Row class
from pyspark.sql import Row
data = [Row(name ="James", prop =Row(hair = "black",eye = "blue")),
       Row(name="Ann",prop=Row(hair="grey",eye="black"))]
df = spark.createDataFrame(data)
df.printSchema()
"""
root
 |-- name: string (nullable = true)
 |-- prop: struct (nullable = true)
 |    |-- hair: string (nullable = true)
 |    |-- eye: string (nullable = true)
"""
df.select(df.prop.hair).show()

+---------+
|prop.hair|
+---------+
|    black|
|     grey|
+---------+

df.select(df["prop.hair"]).show()
+-----+
| hair|
+-----+
|black|
| grey|
+-----+

df.select(col("prop.hair")).show()
+-----+
| hair|
+-----+
|black|
| grey|
+-----+

df.select(col("prop.*")).show()
+-----+-----+
| hair|  eye|
+-----+-----+
|black| blue|
| grey|black|
+-----+-----+
{% endhighlight %}

### **2. PySpark Column Operators:**
{% highlight ruby %}
data=[(100,2,1),(200,3,4),(300,4,4)]
df=spark.createDataFrame(data).toDF("col1","col2","col3")

#Arthmetic operations
df.select(df.col1 + df.col2).show()
+-------------+
|(col1 + col2)|
+-------------+
|          102|
|          203|
|          304|
+-------------+
df.select(df.col1 - df.col2).show() 

+-------------+
|(col1 - col2)|
+-------------+
|           98|
|          197|
|          296|
+-------------+
df.select(df.col1 * df.col2).show()
+-------------+
|(col1 * col2)|
+-------------+
|          200|
|          600|
|         1200|
+-------------+
df.select(df.col1 / df.col2).show()
+-----------------+
|    (col1 / col2)|
+-----------------+
|             50.0|
|66.66666666666667|
|             75.0|
+-----------------+
df.select(df.col1 % df.col2).show()

+-------------+
|(col1 % col2)|
+-------------+
|            0|
|            2|
|            0|
+-------------+
df.select(df.col2 > df.col3).show()
+-------------+
|(col2 > col3)|
+-------------+
|         true|
|        false|
|        false|
+-------------+
df.select(df.col2 < df.col3).show()

+-------------+
|(col2 < col3)|
+-------------+
|        false|
|         true|
|        false|
+-------------+

{% endhighlight %}


### **3. PySpark Column Functions Examples:**
{% highlight ruby %}
data=[("James","Bond","100",None),
      ("Ann","Varsa","200",'F'),
      ("Tom Cruise","XXX","400",''),
      ("Tom Brand",None,"400",'M')] 
columns=["fname","lname","id","gender"]
df=spark.createDataFrame(data,columns)
{% endhighlight %}


### **3.1 alias() – Set’s name to Column:**
{% highlight ruby %}
from pyspark.sql.functions import expr
df.select(df.fname.alias("firstname"),df.lname.alias("lastname")).show()
+----------+--------+
| firstname|lastname|
+----------+--------+
|     James|    Bond|
|       Ann|   Varsa|
|Tom Cruise|     XXX|
| Tom Brand|    null|
+----------+--------+
#Another example
df.select(expr("fname || ' ' || lname as fullname")).show()
+--------------+
|      fullname|
+--------------+
|    James Bond|
|     Ann Varsa|
|Tom Cruise XXX|
|          null|
+--------------+
{% endhighlight %}


### **3.2 asc() & desc() – Sort the DataFrame columns by Ascending or Descending order:**
{% highlight ruby %}
df.sort(df.fname.asc()).show()
+----------+-----+---+------+
|     fname|lname| id|gender|
+----------+-----+---+------+
|       Ann|Varsa|200|     F|
|     James| Bond|100|  null|
| Tom Brand| null|400|     M|
|Tom Cruise|  XXX|400|      |
+----------+-----+---+------+

df.sort(df.fname.desc()).show()

+----------+-----+---+------+
|     fname|lname| id|gender|
+----------+-----+---+------+
|Tom Cruise|  XXX|400|      |
| Tom Brand| null|400|     M|
|     James| Bond|100|  null|
|       Ann|Varsa|200|     F|
+----------+-----+---+------+
{% endhighlight %}


### **3.3 cast() & astype() – Used to convert the data Type:**
{% highlight ruby %}
df.select(df.fname,df.id.cast("int")).printSchema()
root
 |-- fname: string (nullable = true)
 |-- id: integer (nullable = true)
{% endhighlight %}


### **3.4 between() – Returns a Boolean expression when a column values in between lower and upper bound:**
{% highlight ruby %}
df.filter(df.id.between(100,300)).show()
+-----+-----+---+------+
|fname|lname| id|gender|
+-----+-----+---+------+
|James| Bond|100|  null|
|  Ann|Varsa|200|     F|
+-----+-----+---+------+
{% endhighlight %}


### **3.5 contains() – Checks if a DataFrame column value contains a a value specified in this function:**
{% highlight ruby %}
df.filter(df.fname.contains("Cruise")).show()
+----------+-----+---+------+
|     fname|lname| id|gender|
+----------+-----+---+------+
|Tom Cruise|  XXX|400|      |
+----------+-----+---+------+
{% endhighlight %}


### **3.6 startswith() & endswith() – Checks if the value of the DataFrame Column starts and ends with a String respectively:**
{% highlight ruby %}
df.filter(df.fname.startswith("T")).show()
+----------+-----+---+------+
|     fname|lname| id|gender|
+----------+-----+---+------+
|Tom Cruise|  XXX|400|      |
| Tom Brand| null|400|     M|
+----------+-----+---+------+
df.filter(df.fname.endswith("Cruise")).show()
+----------+-----+---+------+
|     fname|lname| id|gender|
+----------+-----+---+------+
|Tom Cruise|  XXX|400|      |
+----------+-----+---+------+
{% endhighlight %}


### **3.7 eqNullSafe():**
{% highlight ruby %}
from pyspark.sql import Row
df1 = spark.createDataFrame([
    Row(id=1, value='foo'),
    Row(id=2, value=None)
])
df1.select(
    df1['value'] == 'foo',
    df1['value'].eqNullSafe('foo'),
    df1['value'].eqNullSafe(None)
).show()
+-------------+---------------+----------------+
|(value = foo)|(value <=> foo)|(value <=> NULL)|
+-------------+---------------+----------------+
|         true|           true|           false|
|         null|          false|            true|
+-------------+---------------+----------------+
{% endhighlight %}


### **3.8 isNull & isNotNull() – Checks if the DataFrame column has NULL or non NULL values:**
{% highlight ruby %}
df.filter(df.lname.isNull()).show()
+---------+-----+---+------+
|    fname|lname| id|gender|
+---------+-----+---+------+
|Tom Brand| null|400|     M|
+---------+-----+---+------+
df.filter(df.lname.isNotNull()).show()
+----------+-----+---+------+
|     fname|lname| id|gender|
+----------+-----+---+------+
|     James| Bond|100|  null|
|       Ann|Varsa|200|     F|
|Tom Cruise|  XXX|400|      |
+----------+-----+---+------+
{% endhighlight %}

### **3.9 like() & rlike() – Similar to SQL LIKE expression:**
{% highlight ruby %}

df.select(df.fname,df.lname,df.id) \
  .filter(df.fname.like("_nn")).show()
+-----+-----+---+
|fname|lname| id|
+-----+-----+---+
|  Ann|Varsa|200|
+-----+-----+---+
{% endhighlight %}


### **3.10 substr() – Returns a Column after getting sub string from the Column:**
{% highlight ruby %}
df.select(df.fname.substr(1,2).alias("substr")).show()
+------+
|substr|
+------+
|    Ja|
|    An|
|    To|
|    To|
+------+
{% endhighlight %}

### **3.11 when() & otherwise() – It is similar to SQL Case When, executes sequence of expressions until it matches the condition and returns a value when match.:**
{% highlight ruby %}
from pyspark.sql.functions import when
(df.select(df.fname,df.lname,
           when(df.gender == "M","Male")
          .when(df.gender == "F","Female")
          .when(df.gender == None,"")
          .otherwise(df.gender).alias("new_gender")
          ).show())
+----------+-----+----------+
|     fname|lname|new_gender|
+----------+-----+----------+
|     James| Bond|      null|
|       Ann|Varsa|    Female|
|Tom Cruise|  XXX|          |
| Tom Brand| null|      Male|
+----------+-----+----------+
{% endhighlight %}

### **3.12 isin() – Check if value presents in a List.:**
{% highlight ruby %}
li = [100,200]
(df.select(df.fname,df.lname,df.id)
   .filter(df.id.isin(li)).show())

+-----+-----+---+
|fname|lname| id|
+-----+-----+---+
|James| Bond|100|
|  Ann|Varsa|200|
+-----+-----+---+
{% endhighlight %}

### **3.13 getField() – To get the value by key from MapType column and by stuct child name from StructType column:**
{% highlight ruby %}

#Create DataFrame with struct, array & map
from pyspark.sql.types import StructType,StructField,StringType,ArrayType,MapType
data=[(("James","Bond"),["Java","C#"],{'hair':'black','eye':'brown'}),
      (("Ann","Varsa"),[".NET","Python"],{'hair':'brown','eye':'black'}),
      (("Tom Cruise",""),["Python","Scala"],{'hair':'red','eye':'grey'}),
      (("Tom Brand",None),["Perl","Ruby"],{'hair':'black','eye':'blue'})]

schema = StructType([
        StructField('name', StructType([
            StructField('fname', StringType(), True),
            StructField('lname', StringType(), True)])),
        StructField('languages', ArrayType(StringType()),True),
        StructField('properties', MapType(StringType(),StringType()),True)
     ])
df=spark.createDataFrame(data,schema)
df.printSchema()

root
 |-- name: struct (nullable = true)
 |    |-- fname: string (nullable = true)
 |    |-- lname: string (nullable = true)
 |-- languages: array (nullable = true)
 |    |-- element: string (containsNull = true)
 |-- properties: map (nullable = true)
 |    |-- key: string
 |    |-- value: string (valueContainsNull = true)


#getField from MapType
df.select(df.properties.getField("hair")).show()
+----------------+
|properties[hair]|
+----------------+
|           black|
|           brown|
|             red|
|           black|
+----------------+

#getField from Struct
df.select(df.name.getField("fname")).show()
+----------+
|name.fname|
+----------+
|     James|
|       Ann|
|Tom Cruise|
| Tom Brand|
+----------+
{% endhighlight %}

### **3.14 getItem() – To get the value by index from MapType or ArrayTupe & ny key for MapType column:**
{% highlight ruby %}
#getItem() used with ArrayType
df.select(df.languages.getItem(1)).show()
+------------+
|languages[1]|
+------------+
|          C#|
|      Python|
|       Scala|
|        Ruby|
+------------+

#getItem() used with MapType
df.select(df.properties.getItem("hair")).show()
+----------------+
|properties[hair]|
+----------------+
|           black|
|           brown|
|             red|
|           black|
+----------------+
{% endhighlight %}

### **3.15 dropFields:**
{% highlight ruby %}
from pyspark.sql import Row
from pyspark.sql.functions import col, lit
df = spark.createDataFrame([
    Row(a=Row(b=1, c=2, d=3, e=Row(f=4, g=5, h=6)))])
df.printSchema()
root
 |-- a: struct (nullable = true)
 |    |-- b: long (nullable = true)
 |    |-- c: long (nullable = true)
 |    |-- d: long (nullable = true)
 |    |-- e: struct (nullable = true)
 |    |    |-- f: long (nullable = true)
 |    |    |-- g: long (nullable = true)
 |    |    |-- h: long (nullable = true)

df = df.withColumn('a', df['a'].dropFields('b'))
df.show()
+-----------------+
|                a|
+-----------------+
|{2, 3, {4, 5, 6}}|
+-----------------+
df.printSchema()
root
 |-- a: struct (nullable = true)
 |    |-- c: long (nullable = true)
 |    |-- d: long (nullable = true)
 |    |-- e: struct (nullable = true)
 |    |    |-- f: long (nullable = true)
 |    |    |-- g: long (nullable = true)
 |    |    |-- h: long (nullable = true)

{% endhighlight %}

### **3.16 withField():**
{% highlight ruby %}
from pyspark.sql import Row
from pyspark.sql.functions import lit
df = spark.createDataFrame([Row(a=Row(b=1, c=2))])
df.withColumn('a', df['a'].withField('b', lit(3))).printSchema()
root
 |-- a: struct (nullable = true)
 |    |-- b: integer (nullable = false)
 |    |-- c: long (nullable = true)
df.withColumn('a', df['a'].withField('b', lit(3))).show()
+------+
|     a|
+------+
|{3, 2}|
+------+

df.withColumn('a', df['a'].withField('d', lit(4))).printSchema()
root
 |-- a: struct (nullable = true)
 |    |-- b: long (nullable = true)
 |    |-- c: long (nullable = true)
 |    |-- d: integer (nullable = false)
{% endhighlight %}

### **3.17 over() – Used with Window Functions:**
{% highlight ruby %}
from pyspark.sql import Window
from pyspark.sql.functions import *
tup = [(1, "a"), (1, "a"), (2, "a"), (1, "b"), (2, "b"), (3, "b")]
df = spark.createDataFrame(tup, ["id", "category"])
window = Window.partitionBy("category").orderBy("id").rangeBetween(Window.currentRow,1)
df.withColumn("sum",sum("id").over(window)).select("category","id","sum").show()
+--------+---+---+
|category| id|sum|
+--------+---+---+
|       a|  1|  4|
|       a|  1|  4|
|       a|  2|  2|
|       b|  1|  3|
|       b|  2|  5|
|       b|  3|  3|
+--------+---+---+
{% endhighlight %}