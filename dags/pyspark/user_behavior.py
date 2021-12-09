#
# Code to read from staging layer, create two dataframes and use them to
# get the user_behavior dataframe, it contains the data asked in the
# expected output for the capstone project.
# The user_behavior data frame is written in the staging layer.

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StringType,
    IntegerType,
    FloatType,
    TimestampType,
)

spark = SparkSession.builder.getOrCreate()

# Defining schema for user_purchase
schema = StructType()\
    .add("invoice_number",StringType(),True)\
    .add("stock_code",StringType(),True)\
    .add("detail",StringType(),True)\
    .add("quantity",IntegerType(),True)\
    .add("invoice_date",TimestampType(),True)\
    .add("unit_price",FloatType(),True)\
    .add("customer_id",IntegerType(),True)\
    .add("country",StringType(),True)

# Read from staging layer
user_purchase = spark.read.options(header=True).schema(schema)\
                     .csv('gs://wzlnde-staging-0/user_purchase.csv')

# Calculating amount spent on each purchase
spent = user_purchase\
        .withColumn('amount_spent', user_purchase.quantity * user_purchase.unit_price)\
        .select('customer_id', 'amount_spent')

# Calculating total amount spent per client
spent_per_cid = spent.groupBy('customer_id')\
                     .agg(F.sum('amount_spent')\
                     .alias('amount_spent'))

# Read from staging layer
reviews = spark.read.parquet('gs://wzlnde-staging-0/reviews/')

# Calculating the review score and counting reviews per customer
score = reviews.groupBy('cid')\
               .agg(
                   F.sum('positive_review').alias('review_score'),
                   F.count('cid').alias('review_count')
                   )

# Joining data frames to produce the output data
user_behavior = spent_per_cid.join(score,spent_per_cid.customer_id ==  score.cid,"outer")\
                             .select('customer_id','amount_spent','review_count','review_score')\
                             .withColumn('insert_date', F.current_timestamp())

# Saving data frame as parquet
user_behavior.write.parquet('gs://wzlnde-staging-0/user_behavior')