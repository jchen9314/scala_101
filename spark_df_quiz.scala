// Use the Netflix_2011_2016.csv file to Answer and complete
// the commented tasks below!

import org.apache.spark.sql.SparkSession

// Start a simple Spark Session
val spark = SparkSession.builder().getOrCreate()

// Load the Netflix Stock CSV File, have Spark infer the data types.
val df = spark.read.option("header","true").option("inferSchema", "true").csv("../data/Netflix_2011_2016.csv")

// What are the column names?

df.columns

// What does the Schema look like?

df.printSchema()

// Print out the first 5 rows.

df.head(5)

// Use describe() to learn about the DataFrame.

df.describe().show()

// Create a new dataframe with a column called HV Ratio that
// is the ratio of the High Price versus volume of stock traded
// for a day.

val df2 = df.withColumn("HV Ratio", df("High") / df("Volume"))

// What day had the Peak High in Price?

df.orderBy($"High".desc).select(df("High")).show(1)

// What is the mean of the Close column?

df.select(mean("Close")).show()

// What is the max and min of the Volume column?

df.select(max("Volume"), min("Volume")).show()

// For Scala/Spark $ Syntax

// How many days was the Close lower than $ 600?

df.filter($"Close" < 600).count()

// What percentage of the time was the High greater than $500 ?
// number of rows in df: df.count()
// data type: double / int -> double

df.filter($"High" > 500).count() * 1.0 / df.count()

// What is the Pearson correlation between High and Volume?

df.select(corr("High", "Volume")).show()

// What is the max High per year?

val df2 = df.withColumn("Year", year(df("Date")))

df2.groupBy("Year").max("High").orderBy($"Year".asc).show()

// What is the average Close for each Calender Month?

val df3 = df.withColumn("Month", month(df2("Date")))
df3.groupBy("Month").mean("Close").orderBy($"Month".asc).show()
