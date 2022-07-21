# [DC_02] Big Data Fundamentals with PySpark

2022/07

[Datacamp course - Big Data Fundamentals with PySpark](https://app.datacamp.com/learn/courses/big-data-fundamentals-with-pyspark)
[my note(hackmd ver.)](https://hackmd.io/lccqNA_xQP6lJRdGe0XX-A?view)


## 1. Introduction to Big Data analysis with Spark
This chapter introduces the exciting world of Big Data, as well as the various concepts and different frameworks for processing Big Data. You will understand why Apache Spark is considered the best framework for BigData.

### --- What is Big Data? ---
*Big data is a term used to refer to the study and applications of data sets that are too complex for traditional data-processing so ware - Wikipedia*


#### The 3 V's of Big Data
Volume, Variety and Velocity
- Volume: Size of the data
- Variety: Different sources and formats
- Velocity: Speed of the data

![](https://i.imgur.com/wT5R3JC.png)


### --- PySpark: Spark with Python ---
- SparkContext is an entry point into the world of Spark
- An entry point is a way of connecting to Spark cluster
- An entry point is like a key to the house
- PySpark has a default SparkContext called sc

### Understanding SparkContext
PySpark automatically creates a `SparkContext` for you in the PySpark shell (so you don't have to create it by yourself) and is exposed via a variable `sc`.

In this simple exercise, you'll find out the attributes of the `SparkContext` in your PySpark shell which you'll be using for the rest of the course.
```python
# Print the version of SparkContext
print("The version of Spark Context in the PySpark shell is", sc.version)

# Print the Python version of SparkContext
print("The Python version of Spark Context in the PySpark shell is", sc.pythonVer)

# Print the master of SparkContext
print("The master of Spark Context in the PySpark shell is", sc.master)
```
>The version of Spark Context in the PySpark shell is 3.2.0
The Python version of Spark Context in the PySpark shell is 3.9
The master of Spark Context in the PySpark shell is local[*]


### Interactive Use of PySpark
The most important thing to understand here is that we are **not creating any SparkContext object** because PySpark **automatically creates** the SparkContext object named `sc`, by default in the PySpark shell.

```python
# Create a Python list of numbers from 1 to 100 
numb = range(1, 100)

# Load the list into PySpark  
spark_data = sc.parallelize(numb)
```


### Loading data in PySpark shell
```python
# Load a local file into PySpark shell
lines = sc.textFile(file_path)
```
### --- Review of functional programming in Python ---

#### Use of Lambda function in Python - map()
- `map()` function takes a function and a list and returns a new list which contains items returned by that function for each item
- General syntax of map()
```python
map(function, list)
```
- Example of map()
```python
items = [1, 2, 3, 4]
list(map(lambda x: x + 2 , items))
#>>>　[3, 4, 5, 6]
```
#### Use of Lambda function in python - filter()
- `filter()` function takes a function and a list and returns a new list for which the function evaluates as true
- General syntax of filter()
```python
filter(function, list)
```
- Example of lter()
```python
items = [1, 2, 3, 4]
list(filter(lambda x: (x%2 != 0), items))
#>>> [1, 3]
```

### Use of lambda() with map()
```python
# Print my_list in the console
print("Input list is", my_list)

# Square all numbers in my_list
squared_list_lambda = list(map(lambda x: x**2, my_list))

# Print the result of the map function
print("The squared numbers are", squared_list_lambda)
```
>Input list is [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
The squared numbers are [1, 4, 9, 16, 25, 36, 49, 64, 81, 100]

### Use of lambda() with filter()
```python
# Print my_list2 in the console
print("Input list is:", my_list2)

# Filter numbers divisible by 10
filtered_list = list(filter(lambda x: (x%10 == 0), my_list2))

# Print the numbers divisible by 10
print("Numbers divisible by 10 are:", filtered_list)
```
>Input list is: [10, 21, 31, 40, 51, 60, 72, 80, 93, 101]
Numbers divisible by 10 are: [10, 40, 60, 80]


## 2. Programming in PySpark RDD’s
The main abstraction Spark provides is a resilient distributed dataset (RDD), which is the fundamental and backbone data type of this engine. This chapter introduces RDDs and shows how RDDs can be created and executed using RDD Transformations and Actions.

### --- Abstracting Data with RDDs ---
![](https://i.imgur.com/YKBo9oP.png)

Resilient Distributed Datasets
- Resilient: Ability to withstand failures
- Distributed: Spanning across multiple machines
- Datasets: Collection of partitioned data e.g, Arrays, Tables, Tuples etc.,

![](https://i.imgur.com/YJlyq5r.png)

parallelize()
textFile()
![](https://i.imgur.com/JPfaGps.png)
minPartitions, getNumPartitions()
![](https://i.imgur.com/HcRAFVG.png)



### RDDs from Parallelized collections
`sc.parallelize(list)`
```python
# Create an RDD from a list of words
RDD = sc.parallelize(["Spark", "is", "a", "framework", "for", "Big Data processing"])

# Print out the type of the created object
print("The type of RDD is", type(RDD))
```
>The type of RDD is <class 'pyspark.rdd.RDD'>

### RDDs from External Datasets
`sc.textFile(file_path)`
PySpark can easily create RDDs from files that are stored in external storage devices such as HDFS (Hadoop Distributed File System), Amazon S3 buckets, etc. However, the most common method of creating RDD's is from files stored in your local file system. 
```python
# Print the file_path
print("The file_path is", file_path)

# Create a fileRDD from file_path
fileRDD = sc.textFile(file_path)

# Check the type of fileRDD
print("The file type of fileRDD is", type(fileRDD))
```
>The file_path is /usr/local/share/datasets/README.md
The file type of fileRDD is <class 'pyspark.rdd.RDD'>


### Partitions in your data
`getNumPartitions(), minPartitions`
Remember, you already have a SparkContext "sc", "file_path" and "fileRDD" available in your workspace.

```python
# Check the number of partitions in fileRDD
print("Number of partitions in fileRDD is", fileRDD.getNumPartitions())

# Create a fileRDD_part from file_path with 5 partitions
fileRDD_part = sc.textFile(file_path, minPartitions = 5)

# Check the number of partitions in fileRDD_part
print("Number of partitions in fileRDD_part is", fileRDD_part.getNumPartitions())
```
>Number of partitions in fileRDD is 1
Number of partitions in fileRDD_part is 5

### --- Basic RDD Transformations and Actions ---

![](https://i.imgur.com/wYmxEIF.png)
==**Transformations**== create new RDDS
==**Actions**== perform computation on the RDDs

#### RDD Transformations
![](https://i.imgur.com/0HISAtw.png)
**map()**
apply a function to all elements in the RDD
![](https://i.imgur.com/RJUDMFF.png)
**filter()**
return a new RDD with only the elements that pass the condition
![](https://i.imgur.com/61Z2JlQ.png)
**flatMap()**
return mutiple values for element in the original RDD
![](https://i.imgur.com/GRgXgu0.png)
**union()**
![](https://i.imgur.com/cYCtBf5.png)

#### RDD Actions

**collect()** and **take()** Actions
- collect() return all the elements of the dataset as an array
- take(N) returns an array with the first N elements of the dataset

```python=
RDD_map.collect()
# [1, 4, 9, 16]
RDD_map.take(2)
# [1, 4]
```
**first()** and **count()** Actions
- first() prints the first element of the RDD
- count() return the number of elements in the RDD
```python=
RDD_map.first()
# [1]
RDD_flatmap.count()
# 5
```

### Map and Collect
```python
# Create map() transformation to cube numbers
cubedRDD = numbRDD.map(lambda x: x**3)

# Collect the results
numbers_all = cubedRDD.collect()

# Print the numbers from numbers_all
for numb in numbers_all:
	print(numb)
```
>1 8 27 64 ... 1000

### Filter and Count
```python
# Filter the fileRDD to select lines with Spark keyword
fileRDD_filter = fileRDD.filter(lambda line: 'Spark' in line.split())

# How many lines are there in fileRDD?
print("The total number of lines with the keyword Spark is", fileRDD_filter.count())

# Print the first four lines of fileRDD
for line in fileRDD_filter.take(4): 
  print(line)

# >>> The total number of lines with the keyword Spark is 5
# >>> Examples for Learning Spark
# >>> Examples for the Learning Spark book. These examples require a number of libraries and as such have long build files. We have also added a stand alone example with minimal dependencies and a small build file
# >>> These examples have been updated to run against Spark 1.3 so they may
# >>> * Spark 1.3
```

### --- Working with Pair RDDs in PySpark ---
#### Introduction to pair RDDs in PySpark
- Real life datasets are usually key/value pairs
- Each row is a key and maps to one or more values
- Pair RDD is a special data structure to work with this kind of datasets
- Pair RDD: Key is the ==identifer== and value is ==data==

#### Creating pair RDDs
- Two common ways to create pair RDDs
From a list of key-value tuple
From a regular RDD

- Get the data into key/value form for paired RDD

```python=
my_tuple = [('Sam', 23), ('Mary', 34), ('Peter', 25)]
pairRDD_tuple = sc.parallelize(my_tuple)

my_list = ['Sam 23', 'Mary 34', 'Peter 25']
regularRDD = sc.parallelize(my_list)
pairRDD_RDD = regularRDD.map(lambda s: (s.split(' ')[0], s.split(' ')[1]))
```
#### Examples of paired RDD Transformations
- `reduceByKey(func)`: Combine values with the same key
- `groupByKey()`: Group values with the same key
- `sortByKey()`: Return an RDD sorted by the key
- `join()`: Join two pair RDDs based on their key

#### reduceByKey() transformation
combines values with the same key
```python=
regularRDD = sc.parallelize([("Messi", 23), ("Ronaldo", 34),
             ("Neymar", 22), ("Messi", 24)])
pairRDD_reducebykey = regularRDD.reduceByKey(lambda x,y : x + y)
pairRDD_reducebykey.collect()
[('Neymar', 22), ('Ronaldo', 34), ('Messi', 47)]
```
#### sortByKey() transformation
operation orders pair RDD by key
```python=
pairRDD_reducebykey_rev = pairRDD_reducebykey.map(lambda x: (x[1], x[0]))
pairRDD_reducebykey_rev.sortByKey(ascending=False).collect()
[(47, 'Messi'), (34, 'Ronaldo'), (22, 'Neymar')]
```

#### groupByKey() transformation
groups all the values with the same key in the pair RDD
```python=
airports = [("US", "JFK"),("UK", "LHR"),("FR", "CDG"),("US", "SFO")]
regularRDD = sc.parallelize(airports)
pairRDD_group = regularRDD.groupByKey().collect()
for cont, air in pairRDD_group:
print(cont, list(air))
FR ['CDG']
US ['JFK', 'SFO']
UK ['LHR']
```
#### join() transformation
join the ==two pair== RDDs based on their key
```python=
RDD1 = sc.parallelize([("Messi", 34),("Ronaldo", 32),("Neymar", 24)])
RDD2 = sc.parallelize([("Ronaldo", 80),("Neymar", 120),("Messi", 100)])
RDD1.join(RDD2).collect()
[('Neymar', (24, 120)), ('Ronaldo', (32, 80)), ('Messi', (34, 100))]
```

### ReduceBykey and Collect
```python=
# Create PairRDD Rdd with key value pairs
Rdd = sc.parallelize([(1,2),(3,4),(3,6),(4,5)])

# Apply reduceByKey() operation on Rdd
Rdd_Reduced = Rdd.reduceByKey(lambda x, y: x+y)

# Iterate over the result and print the output
for num in Rdd_Reduced.collect(): 
    print("Key {} has {} Counts".format(num[0], num[1]))

# >>> Key 1 has 2 Counts
# >>> Key 3 has 10 Counts
# >>> Key 4 has 5 Counts
```


### SortByKey and Collect
```python
# Sort the reduced RDD with the key by descending order
Rdd_Reduced_Sort = Rdd_Reduced.sortByKey(ascending=False)

# Iterate over the result and retrieve all the elements of the RDD
for num in Rdd_Reduced_Sort.collect():
  print("Key {} has {} Counts".format(num[0], num[1]))

# >>> Key 4 has 5 Counts
# >>> Key 3 has 10 Counts
# >>> Key 1 has 2 Counts
```

### --- Advanced RDD Actions ---

#### reduce() action
- `reduce(func)` action is used for aggregating the elements of a regular RDD
- The function should be commutative (changing the order of the operands does not change the result) and associative
```python=
x = [1,3,4,6]
RDD = sc.parallelize(x)
RDD.reduce(lambda x, y : x + y)
# >>> 14
```

#### saveAsTextFile() action
- `saveAsTextFile()` action saves RDD into a text file inside a directory with each partition as a separate file
- `coalesce()` method can be used to save RDD as a single text file
```python=
RDD.saveAsTextFile("tempFile")
RDD.coalesce(1).saveAsTextFile("tempFile")
```

#### countByKey() action
counts the number of elements for each key
```python=
rdd = sc.parallelize([("a", 1), ("b", 1), ("a", 1)])
for kee, val in rdd.countByKey().items():
    print(kee, val)
# >>> ('a', 2)
# >>> ('b', 1)
```

#### collectAsMap() action
return the key-value pairs in the RDD as a dictionary
```python=
sc.parallelize([(1, 2), (3, 4)]).collectAsMap()
# >>> {1: 2, 3: 4}
```

### CountingBykeys
```python
# Count the unique keys
total = Rdd.countByKey()

# What is the type of total?
print("The type of total is", type(total))

# Iterate over the total and print the output
for k, v in total.items(): 
    print("key", k, "has", v, "counts")

# >>> The type of total is <class 'collections.defaultdict'>
# >>> key 1 has 1 counts
# >>> key 3 has 2 counts
# >>> key 4 has 1 counts
```

### Create a base RDD and transform it
```python
# Create a baseRDD from the file path
baseRDD = sc.textFile(file_path)

# Split the lines of baseRDD into words
splitRDD = baseRDD.flatMap(lambda x: x.split())

# Count the total number of words
print("Total number of words in splitRDD:", splitRDD.count())
# >>> Total number of words in splitRDD: 904061

```

### Remove stop words and reduce the dataset

```python
# Convert the words in lower case and remove stop words from the stop_words curated list
splitRDD_no_stop = splitRDD.filter(lambda x: x.lower() not in stop_words)

# Create a tuple of the word and 1 
splitRDD_no_stop_words = splitRDD_no_stop.map(lambda w: (w, 1))

# Count of the number of occurences of each word
resultRDD = splitRDD_no_stop_words.reduceByKey(lambda x, y: x + y)
```

### Print word frequencies
```python
# Display the first 10 words and their frequencies from the input RDD
for word in resultRDD.take(10):
	print(word)

# Swap the keys and values from the input RDD
resultRDD_swap = resultRDD.map(lambda x: (x[1], x[0]))

# Sort the keys in descending order
resultRDD_swap_sort = resultRDD_swap.sortByKey(ascending=False)

# Show the top 10 most frequent words and their frequencies from the sorted RDD
for word in resultRDD_swap_sort.take(10):
	print("{},{}". format(word[1], word[0]))

# ('Project', 85)
# ('Gutenberg', 26)
# ('EBook', 2)
# ('Complete', 4)
# ('Works', 5)
# ('William', 67)
# ('Shakespeare,', 2)
# ('Shakespeare', 45)
# ('eBook', 9)
# ('use', 266)
# thou,4247
# thy,3630
# shall,3018
# good,2046
# would,1974
# Enter,1926
# thee,1780
# I'll,1737
# hath,1614
# like,1452
```

## 3. Programming in PySpark RDD’s
In this chapter, you'll learn about Spark SQL which is a Spark module for structured data processing. It provides a programming abstraction called DataFrames and can also act as a distributed SQL query engine. This chapter shows how Spark SQL allows you to use DataFrames in Python.

### --- Abstracting Data with DataFrames ---

#### What are PySpark DataFrames?
- PySpark SQL is a Spark library for structured data. It provides more information about the structure of data and computation
- PySpark DataFrame is an ==immutable== distributed collection of data with named columns
- Designed for processing both structured (e.g relational database) and semi-structured data (e.g JSON)
- Dataframe API is available in Python, R, Scala, and Java
- DataFrames in PySpark support both SQL queries ( `SELECT * from table `) or expression methods ( `df.select()` )

#### SparkSession - Entry point for DataFrame API
- SparkContext is the main entry point for creating RDDs
- SparkSession provides a single point of entry to interact with Spark DataFrames
- SparkSession is used to create DataFrame, register DataFrames, execute SQL queries
- SparkSession is available in PySpark shell as `spark`

#### Create a DataFrame from RDD

```python
iphones_RDD = sc.parallelize([
("XS", 2018, 5.65, 2.79, 6.24),
("XR", 2018, 5.94, 2.98, 6.84),
("X10", 2017, 5.65, 2.79, 6.13),
("8Plus", 2017, 6.23, 3.07, 7.12)
])

names = ['Model', 'Year', 'Height', 'Width', 'Weight']

iphones_df = spark.createDataFrame(iphones_RDD, schema=names)
type(iphones_df)
# >>> pyspark.sql.dataframe.DataFrame
```
#### Create a DataFrame from reading a CSV/JSON/TXT
```python
df_csv = spark.read.csv("people.csv", header=True, inferSchema=True)
df_json = spark.read.json("people.json", header=True, inferSchema=True)
df_txt = spark.read.txt("people.txt", header=True, inferSchema=True)
```
- Path to the file and two optional parameters
- Two optional parameters:`header=True` , `inferSchema=True`

### RDD to DataFrame
```python
# Create an RDD from the list
rdd = sc.parallelize(sample_list)

# Create a PySpark DataFrame
names_df = spark.createDataFrame(rdd, schema=['Name', 'Age'])

# Check the type of names_df
print("The type of names_df is", type(names_df))

# >>>The type of names_df is <class 'pyspark.sql.dataframe.DataFrame'>
```

### Loading CSV into DataFrame
```python
# Create an DataFrame from file_path
people_df = spark.read.csv(file_path, header=True, inferSchema=True)

# Check the type of people_df
print("The type of people_df is", type(people_df))

# >>> The type of people_df is <class 'pyspark.sql.dataframe.DataFrame'>
```

### --- Operating on DataFrames in PySpark ---
#### DataFrame operators in PySpark
- DataFrame operations: ==Transformations== and ==Actions==
- DataFrame **Transformations**:
select(), lter(), groupby(), orderby(), dropDuplicates() and withColumnRenamed()
- DataFrame **Actions** :
printSchema(), head(), show(), count(), columns and describe()
( Correction: printSchema() is a method for any Spark dataset/dataframe and not an action )

#### select() and show() operations
- `select()` transformation subsets the columns in the DataFrame
- `show()` action prints first 20 rows in the DataFrame
```python
df_id_age = test.select('Age')
df_id_age.show(3)

# +---+
# |Age|
# +---+
# | 17|
# | 17|
# | 17|
# +---+
# only showing top 3 rows
```
#### filter() and show() operations
- `filter()` transformation filters out the rows based on a condition
```python
new_df_age21 = new_df.filter(new_df.Age > 21)
new_df_age21.show(3)
# +-------+------+---+
# |User_ID|Gender|Age|
# +-------+------+---+
# |1000002| M| 55|
# |1000003| M| 26|
# |1000004| M| 46|
# +-------+------+---+
# only showing top 3 rows
```
#### groupby() and count() operations
- `groupby()` operation can be used to group a variable

```python
test_df_age_group = test_df.groupby('Age')
test_df_age_group.count().show(3)
# +---+------+
# |Age| count|
# +---+------+
# | 26|219587|
# | 17|     4|
# | 55| 21504|
# +---+------+
# only showing top 3 rows
```
#### orderby() Transformations
- `orderby()` operation sorts the DataFrame based on one or more columns

```python
test_df_age_group.count().orderBy('Age').show(3)
# +---+-----+
# |Age|count|
# +---+-----+
# |
# 0|15098|
# | 17|    4|
# | 18|99660|
# +---+-----+
# only showing top 3 rows
```
#### dropDuplicates()
- `dropDuplicates()` removes the duplicate rows of a DataFrame
```python
test_df_no_dup = test_df.select('User_ID','Gender', 'Age').dropDuplicates()
test_df_no_dup.count()
# 5892
```

#### withColumnRenamed Transformations
- `withColumnRenamed()` renames a column in the DataFrame
```python
test_df_sex = test_df.withColumnRenamed('Gender', 'Sex')
test_df_sex.show(3)
# +-------+---+---+
# |User_ID|Sex|Age|
# +-------+---+---+
# |1000001| F| 17|
# |1000001| F| 17|
# |1000001| F| 17|
# +-------+---+---+
```

#### printSchema()
- `printSchema()` operation prints the types of columns in the DataFrame
```python
test_df.printSchema()
# |-- User_ID: integer (nullable = true)
# |-- Product_ID: string (nullable = true)
# |-- Gender: string (nullable = true)
# |-- Age: string (nullable = true)
# |-- Occupation: integer (nullable = true)
# |-- Purchase: integer (nullable = true)
```

#### columns actions
- `columns` operator prints the columns of a DataFrame

```python
test_df.columns
# ['User_ID', 'Gender', 'Age']
```
#### describe() actions
- `describe()` operation compute summary statistics of numerical columns in the DataFrame
```python
test_df.describe().show()
# +-------+------------------+------+------------------+
# | summary|     User_ID|  Gender|     Age|
# +-------+------------------+------+------------------+
# |   count|      550068|  550068|  550068|
# |    mean| 1003028.842|    null|  30.382|
# |  stddev|    1727.591|    null|  11.866|
# |     min|     1000001|       F|       0|
# |     max|     1006040|       M|      55|
# +-------+------------------+------+------------------+
```

### Inspecting data in PySpark DataFrame
```python
# Print the first 10 observations 
people_df.show(10)

# Count the number of rows 
print("There are {} rows in the people_df DataFrame.".format(people_df.count()))

# Count the number of columns and their names
print("There are {} columns in the people_df DataFrame and their names are {}".format(len(people_df.columns), people_df.columns))

# +---+---------+----------------+------+-------------+
# |_c0|person_id|            name|   sex|date of birth|
# +---+---------+----------------+------+-------------+
# |  0|      100|  Penelope Lewis|female|   1990-08-31|
# |  1|      101|   David Anthony|  male|   1971-10-14|
# |  2|      102|       Ida Shipp|female|   1962-05-24|
# |  3|      103|    Joanna Moore|female|   2017-03-10|
# |  4|      104|  Lisandra Ortiz|female|   2020-08-05|
# |  5|      105|   David Simmons|  male|   1999-12-30|
# |  6|      106|   Edward Hudson|  male|   1983-05-09|
# |  7|      107|    Albert Jones|  male|   1990-09-13|
# |  8|      108|Leonard Cavender|  male|   1958-08-08|
# |  9|      109|  Everett Vadala|  male|   2005-05-24|
# +---+---------+----------------+------+-------------+
# only showing top 10 rows

# There are 100000 rows in the people_df DataFrame.
# There are 5 columns in the people_df DataFrame and their names are ['_c0', 'person_id', 'name', 'sex', 'date of birth']
```

### PySpark DataFrame subsetting and cleaning
```python
# Select name, sex and date of birth columns
people_df_sub = people_df.select('name', 'sex', 'date of birth')

# Print the first 10 observations from people_df_sub
people_df_sub.show(10)

# Remove duplicate entries from people_df_sub
people_df_sub_nodup = people_df_sub.dropDuplicates()

# Count the number of rows
print("There were {} rows before removing duplicates, and {} rows after removing duplicates".format(people_df_sub.count(), people_df_sub_nodup.count()))


# +----------------+------+-------------+
# |            name|   sex|date of birth|
# +----------------+------+-------------+
# |  Penelope Lewis|female|   1990-08-31|
# |   David Anthony|  male|   1971-10-14|
# |       Ida Shipp|female|   1962-05-24|
# |    Joanna Moore|female|   2017-03-10|
# |  Lisandra Ortiz|female|   2020-08-05|
# |   David Simmons|  male|   1999-12-30|
# |   Edward Hudson|  male|   1983-05-09|
# |    Albert Jones|  male|   1990-09-13|
# |Leonard Cavender|  male|   1958-08-08|
# |  Everett Vadala|  male|   2005-05-24|
# +----------------+------+-------------+
# only showing top 10 rows

# There were 100000 rows before removing duplicates, and 99998 rows after removing duplicates
```

### Filtering your DataFrame
```python
# Filter people_df to select females 
people_df_female = people_df.filter(people_df.sex == "female")

# Filter people_df to select males
people_df_male = people_df.filter(people_df.sex == "male")

# Count the number of rows 
print("There are {} rows in the people_df_female DataFrame and {} rows in the people_df_male DataFrame".format(people_df_female.count(), people_df_male.count()))

#>>> There are 49014 rows in the people_df_female DataFrame and 49066 rows in the people_df_male DataFrame
```

### --- Interacting with DataFrames using PySpark SQL ---

#### Executing SQL Queries
- The SparkSession `sql()` method executes SQL query
- `sql()` method takes a SQL statement as an argument and returns the result as DataFrame
```python
df.createOrReplaceTempView("table1")
df2 = spark.sql("SELECT field1, field2 FROM table1")
df2.collect()

# [Row(f1=1, f2='row1'), Row(f1=2, f2='row2'), 
#  Row(f1=3, f2='row3')]
```

#### SQL query to extract data
```python
test_df.createOrReplaceTempView("test_table")
query = '''SELECT Product_ID FROM test_table'''
test_product_df = spark.sql(query)
test_product_df.show(5)
# +----------+
# |Product_ID|
# +----------+
# | P00069042|
# | P00248942|
# | P00087842|
# | P00085442|
# | P00285442|
# +----------+
```
#### Summarizing and grouping data using SQL queries

```python
test_df.createOrReplaceTempView("test_table")
query = '''SELECT Age, max(Purchase) FROM test_table GROUP BY Age'''
spark.sql(query).show(5)
# +-----+-------------+
# |  Age|max(Purchase)|
# +-----+-------------+
# |18-25| 23958|
# |26-35| 23961|
# | 0-17| 23955|
# |46-50| 23960|
# |51-55| 23960|
# +-----+-------------+
# only showing top 5 rows
```
#### Filtering columns using SQL queries

```python
test_df.createOrReplaceTempView("test_table")
query = '''SELECT Age, Purchase, Gender FROM test_table WHERE Purchase > 20000 AND Gender == "F"'''
spark.sql(query).show(5)
# +-----+--------+------+
# | Age|Purchase|Gender|
# +-----+--------+------+
# |36-45| 23792| F|
# |26-35| 21002| F|
# |26-35| 23595| F|
# |26-35| 23341| F|
# |46-50| 20771| F|
# +-----+--------+------+
# only showing top 5 rows
```
### Running SQL Queries Programmatically

```python
# Create a temporary table "people"
people_df.createOrReplaceTempView("people")

# Construct a query to select the names of the people from the temporary table "people"
query = '''SELECT name FROM people'''

# Assign the result of Spark's query to people_df_names
people_df_names = spark.sql(query)

# Print the top 10 names of the people
people_df_names.show(10)

#     +----------------+
#     |            name|
#     +----------------+
#     |  Penelope Lewis|
#     |   David Anthony|
#     |       Ida Shipp|
#     |    Joanna Moore|
#     |  Lisandra Ortiz|
#     |   David Simmons|
#     |   Edward Hudson|
#     |    Albert Jones|
#     |Leonard Cavender|
#     |  Everett Vadala|
#     +----------------+
#     only showing top 10 rows
    
```
### SQL queries for filtering Table

```python
# Filter the people table to select female sex 
people_female_df = spark.sql('SELECT * FROM people WHERE sex=="female"')

# Filter the people table DataFrame to select male sex
people_male_df = spark.sql('SELECT * FROM people WHERE sex=="male"')

# Count the number of rows in both DataFrames
print("There are {} rows in the people_female_df and {} rows in the people_male_df DataFrames".format(people_female_df.count(), people_male_df.count()))

#>>> There are 49014 rows in the people_female_df and 49066 rows in the people_male_df DataFrames
```

### --- Data Visualization in PySpark using DataFrames ---
Plotting graphs using PySpark DataFrames is done using three methods
- pyspark_dist_explore library
- toPandas()
- HandySpark library

#### Data Visualization using Pyspark_dist_explore
Pyspark_dist_explore library provides quick insights into DataFrames
Currently three functions available – `hist()` , `distplot()` and `pandas_histogram()`
```python
test_df = spark.read.csv("test.csv", header=True, inferSchema=True)
test_df_age = test_df.select('Age')
hist(test_df_age, bins=20, color="red")
```

#### Using Pandas for plotting DataFrames
It's easy to create charts from pandas DataFrames
```python
test_df = spark.read.csv("test.csv", header=True, inferSchema=True)
test_df_sample_pandas = test_df.toPandas()
test_df_sample_pandas.hist('Age')
```
#### Pandas DataFrame vs PySpark DataFrame
- **Pandas DataFrames** are in-memory, single-server based structures 
and operations on **PySpark** run in parallel
- The result is generated as we apply any operation in **Pandas** whereas operations in **PySpark DataFrame** are lazy evaluation
- **Pandas DataFrame** as mutable and **PySpark DataFrames** are immutable
- **Pandas API** support more operations than **PySpark Dataframe API**

#### HandySpark method of visualization
HandySpark is a package designed to improve PySpark user experience
```python
test_df = spark.read.csv('test.csv', header=True, inferSchema=True)
hdf = test_df.toHandy()
hdf.cols["Age"].hist()
```

### PySpark DataFrame visualization
```python
# Check the column names of names_df
print("The column names of names_df are", names_df.columns)

# Convert to Pandas DataFrame  
df_pandas = names_df.toPandas()

# Create a horizontal bar plot
df_pandas.plot(kind='barh', x='Name', y='Age', colormap='winter_r')
plt.show()
```
![](https://i.imgur.com/frFEw9N.png)

### Part 1: Create a DataFrame from CSV file
```python
# Load the Dataframe
fifa_df = spark.read.csv(file_path, header=True, inferSchema=True)

# Check the schema of columns
fifa_df.printSchema()

# Show the first 10 observations
fifa_df.show(10)

# Print the total number of rows
print("There are {} rows in the fifa_df DataFrame".format(fifa_df.count()))
```
```python

root
 |-- _c0: integer (nullable = true)
 |-- Name: string (nullable = true)
 |-- Age: integer (nullable = true)
 |-- Photo: string (nullable = true)
 |-- Nationality: string (nullable = true)
 ...
 |-- RWB: double (nullable = true)
 |-- ST: double (nullable = true)

+---+-----------------+---+--------------------+-----------+--------------------+-------+---------+-------------------+--------------------+------+-----+-------+------------+----------+-------+-------+------------+---------+--------+-----+---------+---------+------------------+---------+-----------+----------+--------------+-----------+----------------+-------------+-------+------------+----------+-------+---------+-----------+---------+-------------+----------+--------------+------------+-------+---------------+--------+------+-------+----+----+----+----+----+------+----+----+----+----+----+----+----+----+----+----+-------------------+----+----+----+----+----+----+----+----+----+----+----+
|_c0|             Name|Age|               Photo|Nationality|                Flag|Overall|Potential|               Club|           Club Logo| Value| Wage|Special|Acceleration|Aggression|Agility|Balance|Ball control|Composure|Crossing|Curve|Dribbling|Finishing|Free kick accuracy|GK diving|GK handling|GK kicking|GK positioning|GK reflexes|Heading accuracy|Interceptions|Jumping|Long passing|Long shots|Marking|Penalties|Positioning|Reactions|Short passing|Shot power|Sliding tackle|Sprint speed|Stamina|Standing tackle|Strength|Vision|Volleys| CAM|  CB| CDM|  CF|  CM|    ID| LAM|  LB| LCB| LCM| LDM|  LF|  LM|  LS|  LW| LWB|Preferred Positions| RAM|  RB| RCB| RCM| RDM|  RF|  RM|  RS|  RW| RWB|  ST|
+---+-----------------+---+--------------------+-----------+--------------------+-------+---------+-------------------+--------------------+------+-----+-------+------------+----------+-------+-------+------------+---------+--------+-----+---------+---------+------------------+---------+-----------+----------+--------------+-----------+----------------+-------------+-------+------------+----------+-------+---------+-----------+---------+-------------+----------+--------------+------------+-------+---------------+--------+------+-------+----+----+----+----+----+------+----+----+----+----+----+----+----+----+----+----+-------------------+----+----+----+----+----+----+----+----+----+----+----+
|  0|Cristiano Ronaldo| 32|https://cdn.sofif...|   Portugal|https://cdn.sofif...|     94|       94|     Real Madrid CF|https://cdn.sofif...|€95.5M|€565K|   2228|          89|        63|     89|     63|          93|       95|      85|   81|       91|       94|                76|        7|         11|        15|            14|         11|              88|           29|     95|          77|        92|     22|       85|         95|       96|           83|        94|            23|          91|     92|             31|      80|    85|     88|89.0|53.0|62.0|91.0|82.0| 20801|89.0|61.0|53.0|82.0|62.0|91.0|89.0|92.0|91.0|66.0|             ST LW |89.0|61.0|53.0|82.0|62.0|91.0|89.0|92.0|91.0|66.0|92.0|
|  1|         L. Messi| 30|https://cdn.sofif...|  Argentina|https://cdn.sofif...|     93|       93|       FC Barcelona|https://cdn.sofif...| €105M|€565K|   2154|          92|        48|     90|     95|          95|       96|      77|   89|       97|       95|                90|        6|         11|        15|            14|          8|              71|           22|     68|          87|        88|     13|       74|         93|       95|           88|        85|            26|          87|     73|             28|      59|    90|     85|92.0|45.0|59.0|92.0|84.0|158023|92.0|57.0|45.0|84.0|59.0|92.0|90.0|88.0|91.0|62.0|                RW |92.0|57.0|45.0|84.0|59.0|92.0|90.0|88.0|91.0|62.0|88.0|
|  2|           Neymar| 25|https://cdn.sofif...|     Brazil|https://cdn.sofif...|     92|       94|Paris Saint-Germain|https://cdn.sofif...| €123M|€280K|   2100|          94|        56|     96|     82|          95|       92|      75|   81|       96|       89|                84|        9|          9|        15|            15|         11|              62|           36|     61|          75|        77|     21|       81|         90|       88|           81|        80|            33|          90|     78|             24|      53|    80|     83|88.0|46.0|59.0|88.0|79.0|190871|88.0|59.0|46.0|79.0|59.0|88.0|87.0|84.0|89.0|64.0|                LW |88.0|59.0|46.0|79.0|59.0|88.0|87.0|84.0|89.0|64.0|84.0|
|  3|        L. Suárez| 30|https://cdn.sofif...|    Uruguay|https://cdn.sofif...|     92|       92|       FC Barcelona|https://cdn.sofif...|  €97M|€510K|   2291|          88|        78|     86|     60|          91|       83|      77|   86|       86|       94|                84|       27|         25|        31|            33|         37|              77|           41|     69|          64|        86|     30|       85|         92|       93|           83|        87|            38|          77|     89|             45|      80|    84|     88|87.0|58.0|65.0|88.0|80.0|176580|87.0|64.0|58.0|80.0|65.0|88.0|85.0|88.0|87.0|68.0|                ST |87.0|64.0|58.0|80.0|65.0|88.0|85.0|88.0|87.0|68.0|88.0|
|  4|         M. Neuer| 31|https://cdn.sofif...|    Germany|https://cdn.sofif...|     92|       92|   FC Bayern Munich|https://cdn.sofif...|  €61M|€230K|   1493|          58|        29|     52|     35|          48|       70|      15|   14|       30|       13|                11|       91|         90|        95|            91|         89|              25|           30|     78|          59|        16|     10|       47|         12|       85|           55|        25|            11|          61|     44|             10|      83|    70|     11|null|null|null|null|null|167495|null|null|null|null|null|null|null|null|null|null|                GK |null|null|null|null|null|null|null|null|null|null|null|
|  5|   R. Lewandowski| 28|https://cdn.sofif...|     Poland|https://cdn.sofif...|     91|       91|   FC Bayern Munich|https://cdn.sofif...|  €92M|€355K|   2143|          79|        80|     78|     80|          89|       87|      62|   77|       85|       91|                84|       15|          6|        12|             8|         10|              85|           39|     84|          65|        83|     25|       81|         91|       91|           83|        88|            19|          83|     79|             42|      84|    78|     87|84.0|57.0|62.0|87.0|78.0|188545|84.0|58.0|57.0|78.0|62.0|87.0|82.0|88.0|84.0|61.0|                ST |84.0|58.0|57.0|78.0|62.0|87.0|82.0|88.0|84.0|61.0|88.0|
|  6|           De Gea| 26|https://cdn.sofif...|      Spain|https://cdn.sofif...|     90|       92|  Manchester United|https://cdn.sofif...|€64.5M|€215K|   1458|          57|        38|     60|     43|          42|       64|      17|   21|       18|       13|                19|       90|         85|        87|            86|         90|              21|           30|     67|          51|        12|     13|       40|         12|       88|           50|        31|            13|          58|     40|             21|      64|    68|     13|null|null|null|null|null|193080|null|null|null|null|null|null|null|null|null|null|                GK |null|null|null|null|null|null|null|null|null|null|null|
|  7|        E. Hazard| 26|https://cdn.sofif...|    Belgium|https://cdn.sofif...|     90|       91|            Chelsea|https://cdn.sofif...|€90.5M|€295K|   2096|          93|        54|     93|     91|          92|       87|      80|   82|       93|       83|                79|       11|         12|         6|             8|          8|              57|           41|     59|          81|        82|     25|       86|         85|       85|           86|        79|            22|          87|     79|             27|      65|    86|     79|88.0|47.0|61.0|87.0|81.0|183277|88.0|59.0|47.0|81.0|61.0|87.0|87.0|82.0|88.0|64.0|                LW |88.0|59.0|47.0|81.0|61.0|87.0|87.0|82.0|88.0|64.0|82.0|
|  8|         T. Kroos| 27|https://cdn.sofif...|    Germany|https://cdn.sofif...|     90|       90|     Real Madrid CF|https://cdn.sofif...|  €79M|€340K|   2165|          60|        60|     71|     69|          89|       85|      85|   85|       79|       76|                84|       10|         11|        13|             7|         10|              54|           85|     32|          93|        90|     63|       73|         79|       86|           90|        87|            69|          52|     77|             82|      74|    88|     82|83.0|72.0|82.0|81.0|87.0|182521|83.0|76.0|72.0|87.0|82.0|81.0|81.0|77.0|80.0|78.0|            CDM CM |83.0|76.0|72.0|87.0|82.0|81.0|81.0|77.0|80.0|78.0|77.0|
|  9|       G. Higuaín| 29|https://cdn.sofif...|  Argentina|https://cdn.sofif...|     90|       90|           Juventus|https://cdn.sofif...|  €77M|€275K|   1961|          78|        50|     75|     69|          85|       86|      68|   74|       84|       91|                62|        5|         12|         7|             5|         10|              86|           20|     79|          59|        82|     12|       70|         92|       88|           75|        88|            18|          80|     72|             22|      85|    70|     88|81.0|46.0|52.0|84.0|71.0|167664|81.0|51.0|46.0|71.0|52.0|84.0|79.0|87.0|82.0|55.0|                ST |81.0|51.0|46.0|71.0|52.0|84.0|79.0|87.0|82.0|55.0|87.0|
+---+-----------------+---+--------------------+-----------+--------------------+-------+---------+-------------------+--------------------+------+-----+-------+------------+----------+-------+-------+------------+---------+--------+-----+---------+---------+------------------+---------+-----------+----------+--------------+-----------+----------------+-------------+-------+------------+----------+-------+---------+-----------+---------+-------------+----------+--------------+------------+-------+---------------+--------+------+-------+----+----+----+----+----+------+----+----+----+----+----+----+----+----+----+----+-------------------+----+----+----+----+----+----+----+----+----+----+----+
only showing top 10 rows

There are 17981 rows in the fifa_df DataFrame
```

### Part 2: SQL Queries on DataFrame
```python
# Create a temporary view of fifa_df
fifa_df.createOrReplaceTempView('fifa_df_table')

# Construct the "query"
query = '''SELECT Age FROM fifa_df_table WHERE Nationality == "Germany"'''

# Apply the SQL "query"
fifa_df_germany_age = spark.sql(query)

# Generate basic statistics
fifa_df_germany_age.describe().show()

# +-------+-----------------+
# |summary|              Age|
# +-------+-----------------+
# |  count|             1140|
# |   mean|24.20263157894737|
# | stddev|4.197096712293756|
# |    min|               16|
# |    max|               36|
# +-------+-----------------+
```

### Part 3: Data visualization
```python
# Convert fifa_df to fifa_df_germany_age_pandas DataFrame
fifa_df_germany_age_pandas = fifa_df_germany_age.toPandas()

# Plot the 'Age' density of Germany Players
fifa_df_germany_age_pandas.plot(kind='density')
plt.show()
```
![](https://i.imgur.com/vPspA59.png)


## 4. Machine Learning with PySpark MLlib
PySpark MLlib is the Apache Spark scalable machine learning library in Python consisting of common learning algorithms and utilities. Throughout this last chapter, you'll learn important Machine Learning algorithms. You will build a movie recommendation engine and a spam filter, and use k-means clustering.


### --- Overview of PySpark MLlib ---

#### PySpark MLlib Algorithms
- **Classification (Binary and Multiclass) and Regression**: Linear SVMs, logistic regression, decision trees, random forests, gradient-boosted trees, naive Bayes, linear least squares, Lasso, ridge regression, isotonic regression
- **Collaborative ltering**: Alternating least squares (ALS)
- **Clustering**: K-means, Gaussian mixture, Bisecting K-means and Streaming K-Means

#### PySpark MLlib imports
- pyspark.mllib.recommendation
`from pyspark.mllib.recommendation import ALS`
- pyspark.mllib.classification
`from pyspark.mllib.classification import LogisticRegressionWithLBFGS`
- pyspark.mllib.clustering
`from pyspark.mllib.clustering import KMeans`


#### PySpark ML libraries
Q: What kind of data structures does pyspark.mllib built-in library support in Spark?
A: RDDS

### PySpark MLlib algorithms
```python
# Import the library for ALS
from pyspark.mllib.recommendation import ALS

# Import the library for Logistic Regression
from pyspark.mllib.classification import LogisticRegressionWithLBFGS 

# Import the library for Kmeans
from pyspark.mllib.clustering import KMeans
```

### --- Collaborative filtering ---
#### What is Collaborative filtering?
- Collaborative filtering is nding users that share common interests
- Collaborative filtering is commonly used for recommender systems
- Collaborative filtering approaches
    - ==User-User Collaborative filtering==: Finds users that are similar to the target user
    - ==Item-Item Collaborative filtering==: Finds and recommends items that are similar to items with the target user

#### Rating class in pyspark.mllib.recommendation submodule
- The Rating class is a wrapper around tuple (user, product and rating)
- Useful for parsing the RDD and creating a tuple of user, product and rating
```python
from pyspark.mllib.recommendation import Rating
r = Rating(user = 1, product = 2, rating = 5.0)
(r[0], r[1], r[2])
# (1, 2, 5.0)
```

#### Splitting the data using randomSplit()
PySpark's randomSplit() method randomly splits with the provided weights and returns multiple RDDs
```python
data = sc.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
training, test=data.randomSplit([0.6, 0.4])
training.collect()
test.collect()
# [1, 2, 5, 6, 9, 10]
# [3, 4, 7, 8]
```


#### Alternating Least Squares (ALS)
- Alternating Least Squares (ALS) algorithm in `spark.mllib` provides collaborative ltering
- `ALS.train(ratings, rank, iterations)`

```python
r1 = Rating(1, 1, 1.0)
r2 = Rating(1, 2, 2.0)
r3 = Rating(2, 1, 2.0)
ratings = sc.parallelize([r1, r2, r3])
ratings.collect()
# [Rating(user=1, product=1, rating=1.0),
# Rating(user=1, product=2, rating=2.0),
# Rating(user=2, product=1, rating=2.0)]

model = ALS.train(ratings, rank=10, iterations=10)
```


#### predictAll() – Returns RDD of Rating Objects
- The ==`predictAll()`== method returns a list of predicted ratings for input user and product pair
- The method takes in an RDD without ratings to generate the ratings
```python
unrated_RDD = sc.parallelize([(1, 2), (1, 1)])
predictions = model.predictAll(unrated_RDD)
predictions.collect()
# [Rating(user=1, product=1, rating=1.0000278574351853),
# Rating(user=1, product=2, rating=1.9890355703778122)]
```

####  Model evaluation using MSE
The MSE is the average value of the square of (actual rating - predicted rating)
```python
rates = ratings.map(lambda x: ((x[0], x[1]), x[2]))
rates.collect()
#>>> [((1, 1), 1.0), ((1, 2), 2.0), ((2, 1), 2.0)]
preds = predictions.map(lambda x: ((x[0], x[1]), x[2]))
preds.collect()
#>>> [((1, 1), 1.0000278574351853), ((1, 2), 1.9890355703778122)]
rates_preds = rates.join(preds)
rates_preds.collect()
#>>> [((1, 2), (2.0, 1.9890355703778122)), ((1, 1), (1.0, 1.0000278574351853))]
MSE = rates_preds.map(lambda r: (r[1][0] - r[1] [1])**2).mean()
```

### Loading Movie Lens dataset into RDDs
```python
# Load the data into RDD
data = sc.textFile(file_path)

# Split the RDD 
ratings = data.map(lambda l: l.split(','))

# Transform the ratings RDD 
ratings_final = ratings.map(lambda line: Rating(int(line[0]), int(line[1]), float(line[2])))

# Split the data into training and test
training_data, test_data = ratings_final.randomSplit([0.8, 0.2])

```

### Model training and predictions
```python
# Create the ALS model on the training data
model = ALS.train(training_data, rank=10, iterations=10)

# Drop the ratings column 
testdata_no_rating = test_data.map(lambda p: (p[0], p[1]))

# Predict the model  
predictions = model.predictAll(testdata_no_rating)

# Return the first 2 rows of the RDD
predictions.take(2)

#>>> [Rating(user=390, product=667, rating=3.6214908808036843),
#>>>  Rating(user=428, product=5618, rating=4.350444001192587)]
```

### Model evaluation using MSE
```python
# Prepare ratings data
rates = ratings_final.map(lambda r: ((r[0], r[1]), r[2]))

# Prepare predictions data
preds = predictions.map(lambda r: ((r[0], r[1]), r[2]))

# Join the ratings data with predictions data
rates_and_preds = rates.join(preds)

# Calculate and print MSE
MSE = rates_and_preds.map(lambda r: (r[1][0] - r[1][1])**2).mean()
print("Mean Squared Error of the model for the test data = {:.2f}".format(MSE))
#>>> Mean Squared Error of the model for the test data = 1.32
```

### --- Classification ---
#### Working with Vectors
- PySpark MLlib contains speci c data types Vectors and LabelledPoint
- Two types of Vectors
    - **==Dense Vector==**: store all their entries in an array of oating point numbers
    - **==Sparse Vector==**: store only the nonzero values and their indices

```python
denseVec = Vectors.dense([1.0, 2.0, 3.0])
#>>> DenseVector([1.0, 2.0, 3.0])
sparseVec = Vectors.sparse(4, {1: 1.0, 3: 5.5})
#>>> SparseVector(4, {1: 1.0, 3: 5.5})
```


#### LabelledPoint() in PySpark MLlib
- A LabeledPoint is a wrapper for input features and predicted value
- For binary classification of Logistic Regression, a label is either 0 (negative) or 1 (positive)
```python
positive = LabeledPoint(1.0, [1.0, 0.0, 3.0])
negative = LabeledPoint(0.0, [2.0, 1.0, 1.0])
print(positive)
print(negative)
#>>> LabeledPoint(1.0, [1.0,0.0,3.0])
#>>> LabeledPoint(0.0, [2.0,1.0,1.0])
```


#### HashingTF() in PySpark MLlib
- `HashingTF()` algorithm is used to map feature value to indices in the feature vector
```python
from pyspark.mllib.feature import HashingTF
sentence = "hello hello world"
words = sentence.split()
tf = HashingTF(10000)
tf.transform(words)
#>>>　SparseVector(10000, {3065: 1.0, 6861: 2.0})
```

#### Logistic Regression using LogisticRegressionWithLBFGS
- Logistic Regression using Pyspark MLlib is achieved using LogisticRegressionWithLBFGS class

`predict` not `predictAll`
```python
data = [
LabeledPoint(0.0, [0.0, 1.0]),
LabeledPoint(1.0, [1.0, 0.0]),
]
RDD = sc.parallelize(data)
lrm = LogisticRegressionWithLBFGS.train(RDD)
lrm.predict([1.0, 0.0])
lrm.predict([0.0, 1.0])
#>>> 1
#>>> 0
```

### Loading spam and non-spam data
```python=
# Load the datasets into RDDs
spam_rdd = sc.textFile(file_path_spam)
non_spam_rdd = sc.textFile(file_path_non_spam)

# Split the email messages into words
spam_words = spam_rdd.flatMap(lambda email: email.split(' '))
non_spam_words = non_spam_rdd.flatMap(lambda email: email.split(' '))

# Print the first element in the split RDD
print("The first element in spam_words is", spam_words.first())
print("The first element in non_spam_words is", non_spam_words.first())
#>>> The first element in spam_words is ['You', 'have', '1', 'new', 'message.', 'Please', 'call', '08712400200.']
#>>> The first element in non_spam_words is ['Rofl.', 'Its', 'true', 'to', 'its', 'name']
```

### Feature hashing and LabelPoint
```python
# Create a HashingTf instance with 200 features
tf = HashingTF(numFeatures=200)

# Map each word to one feature
spam_features = tf.transform(spam_words)
non_spam_features = tf.transform(non_spam_words)

# Label the features: 1 for spam, 0 for non-spam
spam_samples = spam_features.map(lambda features:LabeledPoint(1, features))
non_spam_samples = non_spam_features.map(lambda features:LabeledPoint(0, features))

# Combine the two datasets
samples = spam_samples.join(non_spam_samples)
```

### Logistic Regression model training
```python
# Split the data into training and testing
train_samples,test_samples = samples.randomSplit([0.8, 0.2])

# Train the model
model = LogisticRegressionWithLBFGS.train(train_samples)

# Create a prediction label from the test data
predictions = model.predict(test_samples.map(lambda x: x.features))

# Combine original labels with the predicted labels
labels_and_preds = test_samples.map(lambda x: x.label).zip(predictions)

# Check the accuracy of the model on the test data
accuracy = labels_and_preds.filter(lambda x: x[0] == x[1]).count() / float(test_samples.count())
print("Model accuracy : {:.2f}".format(accuracy))
```

### --- Clustering ---

#### What is Clustering?
- Clustering is the unsupervised learning task to organize a collection of data into groups
- PySpark MLlib library currently supports the following clustering models :
    - K-means
    - Gaussian mixture
    - Power iteration clustering (PIC)
    - Bisecting k-means
    - Streaming k-means

#### K-means with Spark MLLib
```python
RDD = sc.textFile("WineData.csv"). \
map(lambda x: x.split(",")).\
map(lambda x: [float(x[0]), float(x[1])])
RDD.take(5)
# [[14.23, 2.43], [13.2, 2.14], [13.16, 2.67], [14.37, 2.5], [13.24, 2.87]]
```

#### Train a K-means clustering model
Training K-means model is done using `KMeans.train()` method
```python
from pyspark.mllib.clustering import KMeans
model = KMeans.train(RDD, k = 2, maxIterations = 10)
model.clusterCenters
# [array([12.25573171, 2.28939024]), array([13.636875, 2.43239583])]
```

#### Evaluating the K-means Model
```python
from math import sqrt
def error(point):
    center = model.centers[model.predict(point)]
    return sqrt(sum([x**2 for x in (point - center)]))

WSSSE = RDD.map(lambda point: error(point)).reduce(lambda x, y: x + y)

print("Within Set Sum of Squared Error = " + str(WSSSE))

# Within Set Sum of Squared Error = 77.96236420499056
```


#### Visualizing clusters
```python
wine_data_df = spark.createDataFrame(RDD, schema=["col1", "col2"])
wine_data_df_pandas = wine_data_df.toPandas()

cluster_centers_pandas = pd.DataFrame(model.clusterCenters, columns=["col1", "col2"])
cluster_centers_pandas.head()

plt.scatter(wine_data_df_pandas["col1"], wine_data_df_pandas["col2"]);
plt.scatter(cluster_centers_pandas["col1"], cluster_centers_pandas["col2"], color="red", marker="x")
plt.show()
```
![](https://i.imgur.com/oqRTA5h.png)

### Loading and parsing the 5000 points data
```python=
# Load the dataset into an RDD
clusterRDD = sc.textFile(file_path)

# Split the RDD based on tab
rdd_split = clusterRDD.map(lambda x: x.split("\t"))

# Transform the split RDD by creating a list of integers
rdd_split_int = rdd_split.map(lambda x: [int(x[0]), int(x[1])])

# Count the number of rows in RDD 
print("There are {} rows in the rdd_split_int dataset".format(rdd_split_int.count()))

#>>> There are 5000 rows in the rdd_split_int dataset
```

### K-means training
```python
# Train the model with clusters from 13 to 16 and compute WSSSE
for clst in range(13, 17):
    model = KMeans.train(rdd_split_int, clst, seed=1)
    WSSSE = rdd_split_int.map(lambda point: error(point)).reduce(lambda x, y: x + y)
    print("The cluster {} has Within Set Sum of Squared Error {}".format(clst, WSSSE))
    
# Train the model again with the best k
model = KMeans.train(rdd_split_int, k=15, seed=1)

# Get cluster centers
cluster_centers = model.clusterCenters

#>>> The cluster 13 has Within Set Sum of Squared Error 251787626.51713783
#>>> The cluster 14 has Within Set Sum of Squared Error 257469943.64057225
#>>> The cluster 15 has Within Set Sum of Squared Error 215235374.39950493
#>>> The cluster 16 has Within Set Sum of Squared Error 167785881.85891667

```

### Visualizing clusters
```python
# Convert rdd_split_int RDD into Spark DataFrame and then to Pandas DataFrame
rdd_split_int_df_pandas = spark.createDataFrame(rdd_split_int, schema=["col1", "col2"]).toPandas()

# Convert cluster_centers to a pandas DataFrame
cluster_centers_pandas = pd.DataFrame(cluster_centers, columns=["col1", "col2"])

# Create an overlaid scatter plot of clusters and centroids
plt.scatter(rdd_split_int_df_pandas["col1"], rdd_split_int_df_pandas["col2"])
plt.scatter(cluster_centers_pandas["col1"], cluster_centers_pandas["col2"], color="red", marker="x")
plt.show()
```
![](https://i.imgur.com/jTAad36.png)

### --- Congratulations! ---
#### Fundamentals of BigData and Apache Spark
- **Chapter 1**: Fundamentals of BigData and introduction to Spark as a distributed computing framework
    - **Main components**: Spark Core and Spark built-in libraries - Spark SQL, Spark MLlib, Graphx, and Spark Streaming
    - **PySpark**: Apache Spark’s Python API to execute Spark jobs
    - **PySpark shell**: For developing the interactive applications in python
    - **Spark modes**: Local and cluster mode

#### Spark components
- **Chapter 2**: Introduction to RDDs, different features of RDDs, methods of creating RDDs and RDD operations (Transformations and Actions)
- **Chapter 3**: Introduction to Spark SQL, DataFrame abstraction, creating DataFrames, DataFrame operations and visualizing Big Data through DataFrames
- **Chapter 4**: Introduction to Spark MLlib, the three C's of Machine Learning (Collaborative filtering, Classification and Clustering)


