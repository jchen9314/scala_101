//run script :load spark_df.scala

import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder().getOrCreate()

// header, true: the first row is header
// inferSchema: use schema in the dataframe

val df = spark.read.option("header","true").option("inferSchema","true").csv("../data/CitiGroup2006_2008")

// df.head(5)

// for (row <- df.head(5)){
//     println(row)
// }


// df.columns

//df.describe().show()

//select a column
df.select("Volume").show(5)

//select multiple columns: add $
df.select($"Date", $"Close").show()

//create a new column
val df2 = df.withColumn("HighPlusLow", df("High") + df("Low"))

df2.printSchema()

//rename a column
df2.select(df2("HighPlusLow").as("HPL"), df2("Close")).show()

///////// filter data /////////
import spark.implicits._

df.filter($"Close" > 480).show() // scala notation
df.filter("Close > 480").show() // spark sql syntax

//multiple filters
df.filter($"Close" < 480 && $"High" < 480).show() //scala notation
df.filter("Close < 480 AND High < 480").show() // spark sql syntax

// dataframe.collect() --> array
val CH_low = df.filter("Close < 480 AND High < 480").collect()

// count # of rows
val CH_low = df.filter("Close < 480 AND High < 480").count()

// equal === 
df.filter($"High" === 484.40).show() // scala notation

//Pearson correlation

df.select(corr("High", "Low")).show()


/////// Date and Timestamp //////

df.select(year(df("Date"))).show()

val df2 = df.withColumn("Year", year(df("Date")))

val dfavgs = df2.groupBy("Year").mean()

dfavgs.orderBy($"Year".asc).select($"Year", $"avg(Close)").show()