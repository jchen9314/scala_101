// run script:
//     spark-shell 
//     :load spark_null.scala

import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder().getOrCreate()

// header, true: the first row is header
// inferSchema: use schema in the dataframe

val df = spark.read.option("header","true").option("inferSchema","true").csv("../data/ContainsNull.csv")

df.show()

// drop any null values
df.na.drop().show()

// drop rows with less than the minimum number of non null values (in this case: 2)
df.na.drop(2).show()

//fillna, based on the data type of the value that is filled in
df.na.fill(100).show()

//fillna by specifying a column name: fill(value, Array(column_name))
df.na.fill("New Name", Array("Name")).show()