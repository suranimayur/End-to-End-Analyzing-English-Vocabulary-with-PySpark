# End-to-End-Analyzing-English-Vocabulary-with-PySpark
Analyzing English Vocabulary with PySpark

## Project Summary:
This project aims to analyze the vocabulary of the classic English novel "Pride and Prejudice" by Jane Austen using PySpark. It demonstrates an end-to-end data processing pipeline in PySpark, covering various tasks such as data ingestion, exploration, transformation, and analysis.

The project starts with launching and utilizing the PySpark shell for interactive development. It then proceeds to read and ingest the text data into a DataFrame, followed by exploring the data structure and selecting specific columns. The text data is transformed by splitting sentences into words, cleaning the text by removing punctuation and non-word tokens, and filtering out empty words. Word frequencies are then computed by grouping and counting the occurrences of each word.

Furthermore, the project explores techniques to handle data dependencies efficiently, simplifying the code using method chaining and leveraging the glob pattern to scale the word count program across multiple text files.


# Project Closure Report:
The project successfully achieved its objectives of analyzing the English vocabulary of "Pride and Prejudice" using PySpark. By implementing various PySpark DataFrame operations, the project demonstrated how to read, explore, transform, and analyze text data efficiently.

## Key outcomes of the project include:

* Setting up a PySpark environment and utilizing the SparkSession object as an entry point.
* Implementing data transformations such as splitting sentences into words, cleaning text, and filtering out irrelevant data.
* Computing word frequencies using PySpark's DataFrame operations.
* Simplifying code structure by using method chaining and reducing intermediate variables.
* Scaling the word count program across multiple text files using the glob pattern.
* The project provides valuable insights into working with textual data using PySpark and serves as a foundation for further exploration and analysis of large-scale text corpora. Additionally, it highlights the importance of leveraging * * * PySpark's distributed computing capabilities for efficient data processing tasks.

# Below are the key PySpark and Python functions used in the project

## PySpark Functions:

**1.SparkSession.builder:** Used to create a SparkSession object, which serves as the entry point to Spark API.

spark = SparkSession.builder \
    .appName("Analyzing English Vocabulary with PySpark") \
    .getOrCreate()

**2. spark.read.text():** Reads text files and returns a DataFrame.

book = spark.read.text("/path/to/textfile.txt")

**3. DataFrame.select():** Selects columns from a DataFrame.

lines = book.select(split(book.value, " ").alias("line"))

**4.DataFrame.explode():** Explodes an array or map column into separate rows.

words = lines.select(explode(col("line")).alias('word'))

**5.DataFrame.groupBy():**  Groups the DataFrame using the specified columns.

groups = words.groupBy("word")

**6.DataFrame.count():**  Counts the number of rows in a DataFrame.

word_counts = groups.count()

**7.DataFrame.filter():**  Filters rows using the given condition.

words_nonull = words_clean.filter(col("word") != "")

**8.pyspark.sql.functions:**  Module containing various functions for DataFrame operations.

from pyspark.sql.functions import split, explode, lower, regexp_extract


## Python Functions:

**1.glob.glob(): **  Returns a list of file paths matching a specified pattern.

file_paths = glob.glob("/path/to/files/*.txt")

**2.re.sub():  **  Substitutes occurrences of a pattern in a string.

clean_word = re.sub(r'[^a-zA-Z]', '', word.lower())

**3.split(): **    Splits a string into a list of substrings based on a delimiter.

word_list = sentence.split(" ")

**4.lower(): **  Converts a string to lowercase.

lowercase_word = word.lower()

**5.filter(): ** Filters elements of a sequence based on a condition.

filtered_words = filter(lambda x: x != "", word_list)

**Summary**

These functions were used throughout the project to perform various data processing tasks such as reading files, transforming text data, computing word frequencies, and filtering out unwanted data. They played a crucial role in implementing the end-to-end data processing pipeline for analyzing English vocabulary using PySpark.





















