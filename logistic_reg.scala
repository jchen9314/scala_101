import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.sql.SparkSession

import org.apache.log4j._
Logger.getLogger("org").setLevel(Level.ERROR)

val spark = SparkSession.builder().getOrCreate()

val data = spark.read.option("header", "true").option("inferSchema", "true").format("csv").load("../data/titanic.csv")

data.printSchema()

data.show(5)

val logregdataall = (data.select(data("Survived").as("label"), 
                    $"Pclass", $"Sex", $"Age", $"SibSp", $"Parch", $"Fare", $"Embarked")
                    )

val logregdata = logregdataall.na.drop()

// deal with cate columns (before OHE, transform string into numerical values)
import org.apache.spark.ml.feature.{VectorAssembler, StringIndexer, VectorIndexer, OneHotEncoder}
import org.apache.spark.ml.linalg.Vectors

// Converting Strings into numerical values
val genderIndexer = new StringIndexer().setInputCol("Sex").setOutputCol("SexIndex")
val embarkIndexer = new StringIndexer().setInputCol("Embarked").setOutputCol("EmbarkIndex")

// Converting Numerical values into One Hot Encoding 0 or 1
val genderEncoder = new OneHotEncoder().setInputCol("SexIndex").setOutputCol("SexVec")
val embarkEncoder = new OneHotEncoder().setInputCol("EmbarkIndex").setOutputCol("EmbarkVec")

// (label, features)
val assembler = (new VectorAssembler().setInputCols(Array("Pclass", "SexVec", "Age", "SibSp", 
                "Parch", "Fare", "EmbarkVec")).setOutputCol("features"))

val Array(training, test) = logregdata.randomSplit(Array(0.7, 0.3), seed=12345) //train test split

// pipeline
import org.apache.spark.ml.Pipeline

val lr = new LogisticRegression()

val pipeline = new Pipeline().setStages(Array(genderIndexer, embarkIndexer, 
                                              genderEncoder, embarkEncoder, assembler, lr))

val model = pipeline.fit(training)

val results = model.transform(test)

// model evaluation
import org.apache.spark.mllib.evaluation.MulticlassMetrics

val predictionAndLabels = results.select($"prediction", $"label").as[(Double, Double)].rdd

val metrics = new MulticlassMetrics(predictionAndLabels)

println("Confusion Matrix:")
println(metrics.confusionMatrix)