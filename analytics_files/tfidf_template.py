from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StringType, IntegerType
from pyspark.sql.window import Window
import pyspark
import os
import subprocess
import math

private_ip = 'namenode_ip'

private_ip = private_ip.replace('.', '-')

context = pyspark.SparkContext('local[*]')
sesh = SparkSession(context)

#print(df_mongo.printSchema())
schema = StructType().add('id', IntegerType(), True)\
                     .add('asin', StringType(), True)\
                     .add('helpful', StringType(), True)\
                     .add('overall', IntegerType(), True)\
                     .add('reviewText', StringType(), True)\
                     .add('reviewTime', StringType(), True)\
                     .add('reviewerID', StringType(), True)\
                     .add('reviewerName', StringType(), True)\
                     .add('summary', StringType(), True)\
                     .add('unixReviewTime', IntegerType(), True)
df_review = sesh.read.option('header', False).option( 'delimiter', '\t').schema(schema) \
.csv('hdfs://ip-{}.ec2.internal:9000/user/ubuntu/kindle_reviews/part-m-00000'.format(private_ip))

cmd =b'/opt/hadoop-3.3.0/bin/hdfs dfs -ls kindle_reviews'
files = subprocess.check_output(cmd, shell=True).strip().split(b'\n')

for i in range(len(files)-3):
        df_review_i =  sesh.read.option('header', False).option( 'delimiter', '\t').schema(schema) \
                                .csv('hdfs://ip-{}.ec2.internal:9000/user/ubuntu/kindle_reviews/part-m-0000{}'.format(private_ip, (i+1)))
        df_review = df_review.union(df_review_i)
        print(df_review.count())

#print(df_review.printSchema())

#print(df_review.first())

# print("mongo loaded")

df_review.printSchema()
print('before dropped')
print(df_review.first())

df_review_dropped = df_review.drop("helpful") \
                                .drop("overall") \
                                .drop("reviewTime") \
                                .drop("reviewerID") \
                                .drop("reviewerName") \
                                .drop("summary") \
                                .drop("unixReviewTime")\
                                .drop("id")

print('after dropped')
print(df_review_dropped.first())

# getting wordcount
document_words = df_review_dropped.select('asin', explode(split('reviewText', '\s+')).alias('word'))
word_count = document_words.groupBy('word').count()

document_words.show()
word_count.show()

# getting term frequency using the formula (term in document)/(total words in document)
w = Window.partitionBy(document_words['asin'])
term_frequency = document_words.groupBy('asin', 'word').agg(count('*').alias('word_count'), sum(count('*')).over(w).alias('document_count'), (count('*')/sum(count('*')).over(w)).alias('term_frequency'))\
                               .orderBy('word_count', ascending=False)\
                               .drop('word_count')\
                               .drop('docs_count')

term_frequency.show()

# get the idf table using the formula (log((document count)/(total number of document with word)))
document_count = document_words.select('asin').distinct().count()

w = Window.partitionBy('word')

idf_table = document_words.groupBy('word', 'asin').agg(lit(document_count).alias('document_count'), count('*').over(w).alias('document_terms'), log(lit(document_count)/count('*').over(w)).alias('idf_table'))\
                          .orderBy('idf_table', ascending=False)\
                          .drop('document_count')\
                          .drop('document_terms')\

idf_table.show()

tfidf_table = term_frequency.join(idf_table, ['asin', 'word']).withColumn('tfidf', col('idf_table') * col('term_frequency'))

tfidf_table.orderBy('tfidf', ascending=True).show(truncate=12)

print('writing to file....')
tfidf_table.write.format('com.databricks.spark.csv').save('tfidf_output')

print("done! filename : tfidf_output")