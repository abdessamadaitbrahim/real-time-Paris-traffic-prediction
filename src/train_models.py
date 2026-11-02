"""Trains 3 Spark ML classifiers on data/data.json and saves the best pipeline."""
import os
import sys
from pathlib import Path

os.environ["PYSPARK_PYTHON"] = sys.executable

import pandas as pd  # noqa: E402
from pyspark.sql import SparkSession  # noqa: E402
from pyspark.ml import Pipeline  # noqa: E402
from pyspark.ml.feature import StringIndexer, VectorAssembler  # noqa: E402
from pyspark.ml.classification import (  # noqa: E402
    DecisionTreeClassifier,
    RandomForestClassifier,
    LogisticRegression,
)
from pyspark.ml.evaluation import MulticlassClassificationEvaluator  # noqa: E402
from pyspark.sql.types import StructType, StructField, StringType, DoubleType  # noqa: E402

ROOT = Path(__file__).resolve().parent.parent
DATA_PATH = ROOT / "data" / "data.json"
MODELS_DIR = ROOT / "models"

spark = SparkSession.builder.appName("TrainingTrafficModels").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# Only q, k and etat_trafic are used for training. geo_point_2d is a JSON
# object in the file, so it is read as a string here and never used.
schema = StructType([
    StructField("q", DoubleType(), True),
    StructField("k", DoubleType(), True),
    StructField("etat_trafic", StringType(), True),
    StructField("t_1h", StringType(), True),
    StructField("geo_point_2d", StringType(), True),
])
df = spark.read.schema(schema).json(str(DATA_PATH))

df_clean = df.select("q", "k", "etat_trafic").dropna()
train_data, test_data = df_clean.randomSplit([0.8, 0.2], seed=42)

classifiers = {
    "DecisionTree": DecisionTreeClassifier(labelCol="label", featuresCol="features"),
    "RandomForest": RandomForestClassifier(labelCol="label", featuresCol="features", numTrees=20),
    "LogisticRegression": LogisticRegression(labelCol="label", featuresCol="features", maxIter=10),
}

metrics = {
    "Accuracy": "accuracy",
    "Precision": "weightedPrecision",
    "Recall": "weightedRecall",
    "F1": "f1",
}

results = []
fitted = {}

for name, classifier in classifiers.items():
    print(f"==> Training {name}...")
    indexer = StringIndexer(inputCol="etat_trafic", outputCol="label")
    assembler = VectorAssembler(inputCols=["q", "k"], outputCol="features")
    pipeline = Pipeline(stages=[indexer, assembler, classifier])

    model = pipeline.fit(train_data)
    predictions = model.transform(test_data)

    row = {"Model": name}
    for label, metric_name in metrics.items():
        evaluator = MulticlassClassificationEvaluator(
            labelCol="label", predictionCol="prediction", metricName=metric_name
        )
        row[label] = evaluator.evaluate(predictions)
    results.append(row)
    fitted[name] = model
    print(f"==> {name} accuracy: {row['Accuracy']:.4f}")

df_results = pd.DataFrame(results)
best_name = df_results.sort_values("Accuracy", ascending=False).iloc[0]["Model"]
best_accuracy = df_results.loc[df_results["Model"] == best_name, "Accuracy"].iloc[0]
print(f"\n==> Best model: {best_name} (accuracy {best_accuracy:.4f})")

MODELS_DIR.mkdir(exist_ok=True)
model_path = str(MODELS_DIR / f"best_model_{best_name.lower()}")
fitted[best_name].write().overwrite().save(model_path)
print(f"==> Model saved to: {model_path}")

print("\n==> Performance summary:")
print(df_results.to_string(index=False))
