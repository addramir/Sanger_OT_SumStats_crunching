import pandas as pd
from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.functions as f
from pyspark.sql.types import *
from pyspark.sql.window import Window
import numpy as np

spark = SparkSession.builder.getOrCreate()



study_index=spark.read.parquet("gs://genetics-portal-dev-data/22.09.1/outputs/lut/study-index")
#gene_index=gene_index.toPandas()