from pyspark.sql import SparkSession
from pyspark.ml.feature import RegexTokenizer
from pyspark.ml.feature import StopWordsRemover
from pyspark.sql.functions import col, udf, when
from pyspark.sql.types import BooleanType, IntegerType

# Code to read and filter a csv file, and write to a parquet
# file with the resulting data frame using a pyspark job on a dataproc cluster

spark = SparkSession.builder.getOrCreate()

# Read from raw layer
df = spark.read.options(header=True).csv('gs://wzlnde-raw-0/movie_review.csv')

regexTokenizer = RegexTokenizer(
    inputCol='review_str',
    outputCol='review_token',
    pattern='\\W'
    )
regexTokenized = regexTokenizer.transform(df)

# Removing stop words from the tokenized data frame
remover = StopWordsRemover(
    inputCol="review_token",
    outputCol="filtered",
    #stopWords=stopwordList
    )

clean_df = remover.transform(regexTokenized).select('cid', 'filtered')

# Filtering positive reviews
findGood = udf(lambda words: 'good' in words, BooleanType())
reviews_bool = clean_df.withColumn('positive_review_bool', findGood(col('filtered')))\
                       .select('cid', 'positive_review_bool')

# Saving data frame as parquet
reviews_bool.write.parquet('gs://wzlnde-staging-0/reviews_bool.parquet')

# Converting "positive_review_bool" column from boolean to integer
reviews = reviews_bool.withColumn("positive_review",\
                       when(reviews_bool.positive_review_bool == True, 1)\
                      .otherwise(0))\
                      .select('cid','positive_review')

# Saving data frame as parquet
reviews.write.parquet('gs://wzlnde-staging-0/reviews.parquet')
