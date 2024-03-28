'''
Your First data Program in Pyspark

Topics to be discussed in this session

 Launching and using the pyspark shell for interactive development
 Reading and ingesting data into a data frame
 Exploring data using the DataFrame structure
 Selecting columns using the select() method
 Reshaping single-nested data into distinct records using explode()
 Applying simple functions to your columns to modify the data they contain
 Filtering columns using the where() method

'''

## The SparkSession entry point

'''
The SparkSession object is the entry point to the Spark API.

'''
'''
PySpark uses a builder pattern through the SparkSession.builder object. For
those familiar with object-oriented programming, a builder pattern provides a set of
methods to create a highly configurable object without having multiple constructors.

'''

## Create a new SparkSession entry point from scratch

'''
The SparkSession object is the entry point to the Spark API.

'''
'''
PySpark uses a builder pattern through the SparkSession.builder object. For
those familiar with object-oriented programming, a builder pattern provides a set of
methods to create a highly configurable object without having multiple constructors.

'''
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Analzying the vocabulary of Price and Prejudice").getOrCreate()

spark.sparkContext

## Mapping Program

'''
“What are
the most popular words used in the English language?” Before we can even hammer
out code in the REPL, we have to start by mapping the major steps our program will
need to perform:

* Read—Read the input data (we’re assuming a plain text file).
* Token—Tokenize each word.
* Clean—Remove any punctuation and/or tokens that aren’t words. Lowercase each word.
* Count—Count the frequency of each word present in the text.
* Answer—Return the top 10 (or 20, 50, 100).

'''

## Ingest and explore: Setting the stage for data transformation

'''
PySpark provides two main structures for storing data when performing manipulations:
 The RDD
 The data frame

RDD is resilient Distributed Data - Like rows

Dataframe is like columns

'''
## The DataFrameReader Object

spark.read

dir(spark.read)

## Reading Jane Austen book in record time


book = spark.read.text("/home/surani/python_projects/DataAnalysisWithPythonAndPySpark/Data/gutenberg_books/1342-0.txt")

book.show(5)

## Printing the schema of our data frame

book.printSchema()

print(book.dtypes)

## Using PySpark’s documentation directly in the REPL


book.show(n=10,truncate=False,vertical=False)

## Simple Column Transformations : Moving from a sentence to list of words

'''
Splitting our lines of text into arrays or words

'''

from pyspark.sql.functions import split

lines = book.select(split(book.value," ").alias("line"))

lines.show()

## Selecting specific columns using select()

## The simplest select statement ever

book.select(book.value)

## Selecting the value column from the book data frame

from pyspark.sql.functions import *

book.select(book.value)
book.select(book['value'])
book.select(book.value).show()

book.select("value").show()


## Transforming columns: Splitting a string into a list of words

'''
PySpark provides a split() function in the pyspark.sql.functions module for
splitting a longer string into a list of shorter strings. The most popular use case for this
function is to split a sentence into words. The split() function takes two or three
parameters:
 A column object containing strings
 A Java regular expression delimiter to split the strings against
 An optional integer about how many times we apply the delimiter 

'''

## Splitting our lines of text into lists of words

from pyspark.sql.functions import split, column, split

lines = book.select(split(col("value"), " "))

lines.show(5)

## Renaming columns : alias and withColumnRenamed

'''
Our data frame before and after the aliasing
'''

book.select(split(col("value"), " ")).printSchema()

book.select(split(col("value"), " ").alias("line")).printSchema()

## Renaming a column, two ways

''' This is cleaner way'''

lines = book.select(split(book.value," ").alias("line"))

lines.show(5)


## Reshaping data: Exploding a list into rows

'''Enter the explode() function. When applied to a column containing a container-
like data structure (such as an array), it’ll take each element and give it its own row.
This is much easier explained visually rather than using words,

'''
from pyspark.sql.functions import explode,col 

words = lines.select(explode(col("line")).alias("word"))

words.show(5)

## Working with words: Changing case and removing punctuation

## Lower the case of words in data frame

from pyspark.sql.functions import lower

words_lower = words.select(lower(col("word")).alias("word_lower"))

words_lower.show(5)

## Using regexp_extract to keep what look like a word

from pyspark.sql.functions import regexp_extract

words_clean = words_lower.select(
    regexp_extract(col("word_lower"),"[a-z]+",0).alias("word")
    
)

words_clean.show(15)

## Filtering rows 

## Filtering rows in your data frame
# 

words_nonull = words_clean.filter(col("word") != "")

words_nonull.show(15)

## Grouping records: Counting word frequencies

## Counting word frequencies using groupby() and count()


groups = words_nonull.groupby(col("word"))

print(groups)


results = words_nonull.groupby(col("word")).count()

print(results)

results.show()

results.orderBy("count",ascending=False).show(10)

results.orderBy(col("count").desc()).show(10)


## Writing our results in multiple CSV files, one per partition

results.write.mode("overwrite").csv('../simple_results.csv')

results.rdd.getNumPartitions()


'''
By default, PySpark will give you one file per partition. This means that our program, as run on my machine, yields 200 partitions at the end. This isn’t the best for
portability. 

To reduce the number of partitions, we apply the coalesce() method with the desired number of partitions. 
The next listing shows the difference when using coalesce(1) on our data frame before writing to disk.

'''

##  Writing our results under a single partition

results.coalesce(1).write.mode("overwrite").csv('../simple_count_single_partition.csv')


## Simplifying dependencies with PySpark

'''
Simplifying our PySpark functions import

'''

import pyspark.sql.functions as F

## Simplifying our program via method chaining

'''
If we look at the transformation methods we applied to our data frames 
(select(),where(), groupBy(), and count()), they all have something in common: they take a
structure as a parameter—the data frame or GroupedData in the case of count()—and
return a structure.

'''

## Removing intermediate variables by chaining transformation methods

## Before transformation

book = spark.read.text('/home/surani/python_projects/DataAnalysisWithPythonAndPySpark/Data/gutenberg_books/1342-0.txt')

lines = book.select(split(book.value," ").alias("line"))

words = lines.select(explode(col("line")).alias('word'))

words_lower = words.select(lower(col("word")).alias("word"))

words_clean = words_lower.select(
    regexp_extract(col("word"),"[a-z]+",0).alias("word")
)

words_nonull = words_clean.filter(col("word") != "")

results =  words_nonull.groupBy("word").count()

## After transformation

import pyspark.sql.functions as F 

results = (
    spark.read.text("/home/surani/python_projects/DataAnalysisWithPythonAndPySpark/Data/gutenberg_books/1342-0.txt")
    .select(F.split(F.col("value"), " ").alias("line"))
    .select(F.explode(F.col("line")).alias('word'))
    .select(F.lower(F.col("word")).alias("word"))
    .select(F.regexp_extract(F.col("word"),"[a-z]+",0).alias("word"))
    .filter(F.col("word") != "")
    .groupBy("word")
    .count()
)

## Scaling our word count program using the glob pattern

### Before scaling

results = spark.read.text("/home/surani/python_projects/DataAnalysisWithPythonAndPySpark/Data/gutenberg_books/1342-0.txt")

### After scaling

results = spark.read.text("/home/surani/python_projects/DataAnalysisWithPythonAndPySpark/Data/gutenberg_books/*.txt")


results = (
    spark.read.text("/home/surani/python_projects/DataAnalysisWithPythonAndPySpark/Data/gutenberg_books/*.txt")
    .select(F.split(F.col("value"), " ").alias("line"))
    .select(F.explode(F.col("line")).alias('word'))
    .select(F.lower(F.col("word")).alias("word"))
    .select(F.regexp_extract(F.col("word"),"[a-z]+",0).alias("word"))
    .filter(F.col("word") != "")
    .groupBy("word")
    .count()
)

results.count()

results.orderBy("count",ascending=False).show(50)