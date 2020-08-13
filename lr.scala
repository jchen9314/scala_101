import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.regression.LinearRegression

// see less error info
import org.apache.log4j._
Logger.getLogger("org").setLevel(Level.ERROR)

val spark = SparkSession.builder().getOrCreate()

val data = spark.read.option("header", "true").option("inferSchema", "true").format("csv").load("../data/Clean_USA_Housing.csv")

// A few things we need to do before Spark can accept the data!
// It needs to be in the form of two columns
// ("label","features")

// This will allow us to join multiple feature columns
// into a single column of an array of feautre values
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg.Vectors

val df = data.select(data("Price").as("label"), 
        $"Avg Area Income", $"Avg Area House Age", $"Avg Area Number of Rooms", $"Area Population")

// An assembler converts the input values to a vector
// A vector is what the ML algorithm reads to train a model

// Set the input columns from which we are supposed to read the values
// Set the name of the column where the vector will be stored
val assembler = (new VectorAssembler().setInputCols(Array("Avg Area Income", "Avg Area House Age", 
                "Avg Area Number of Rooms", "Area Population")).setOutputCol("features"))

// If only transform df, then the "features" column will be added at the end of columns
val output = assembler.transform(df).select($"label", $"features")

val lr = new LinearRegression()

val lrModel = lr.fit(output)
val trainingSummary = lrModel.summary

trainingSummary.predictions.show()

trainingSummary.r2

trainingSummary.rootMeanSquaredError