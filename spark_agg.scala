// run script:
//     spark-shell 
//     :load spark_agg.scala

import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder().getOrCreate()

// header, true: the first row is header
// inferSchema: use schema in the dataframe

val df = spark.read.option("header","true").option("inferSchema","true").csv("../data/Sales.csv")

//groupby agg: mean(), max(), sum(), min(), count()
df.groupBy("Company").mean().show()

//agg: countDistinct, sumDistinct, variance, stddev, collect_set,...
df.select(countDistinct("Sales")).show()
df.select(collect_set("Sales")).show()

//orderBy
df.orderBy($"Sales".desc).show()