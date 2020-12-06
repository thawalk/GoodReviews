from pyspark.sql import SparkSession, SQLContext
import pyspark.sql.functions as fn
from pyspark.sql.functions import col
import pyspark
import os
import subprocess
import math
from pyspark.sql.types import StructType, StringType, IntegerType

private_ip = 'namenode_ip'

private_ip = private_ip.replace('.', '-')

context = pyspark.SparkContext('local[*]')
sesh = SparkSession(context)

df_mongo = sesh.read.json('hdfs://ip-{}.ec2.internal:9000/user/ubuntu/metadata/metadata.json'.format(private_ip))


# drop these columns from metadata
df_mongo = df_mongo.drop('id')\
                   .drop('_id')\
                   .drop('brand')\
                   .drop('categories')\
                   .drop('description')\
                   .drop('related')\
                   .drop('salesRank')\
                   .drop('title')\
                   .drop('imUrl')\
                   .dropna()\
                   .withColumn('price', col('price').cast('float'))

# make sure prices are positive
df_mongo = df_mongo.where(df_mongo.price > 0)


# structure of kindle reviews
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


#  get list of kindle reviews CSVs
cmd =b'/opt/hadoop-3.3.0/bin/hdfs dfs -ls kindle_reviews'
files = subprocess.check_output(cmd, shell=True).strip().split(b'\n')


# combine all csvs
for i in range(len(files)-3):
        df_review_i =  sesh.read.option('header', False).option( 'delimiter', '\t').schema(schema) \
.csv('hdfs://ip-{}.ec2.internal:9000/user/ubuntu/kindle_reviews/part-m-0000{}'.format(private_ip, (i+1)))
        df_review = df_review.union(df_review_i)
 
# drop these columns from kindle reviews
df_review =  df_review.drop('id')\
                      .drop('helpful')\
                      .drop('overall')\
                      .drop('reviewTime')\
                      .drop('reviewerID')\
                      .drop('reviewerName')\
                      .drop('summary')\
                      .drop('unixReviewTime')


# ensure no null values
df_review = df_review.fillna({'reviewText':' '})
df_review=df_review.rdd.map(lambda x: (x.asin, len(x.reviewText.split(" "))))\
                                .toDF().withColumnRenamed("_1","asin2")\
                                .withColumnRenamed("_2", "rev_split")\
                                .groupby("asin2")\
                                .agg(fn.avg("rev_split"))


# combining kindle reviews and metadata using asin values
comb_df = df_mongo.join(df_review, df_mongo.asin == df_review.asin2)
comb_df = comb_df.drop('asin2')


# pearson calculation functions
rdd = comb_df.rdd.map(list)

k = rdd.count()
sumx = rdd.map(lambda x: x[1]).sum()
sumy = rdd.map(lambda x: x[2]).sum()
sumxy = rdd.map(lambda x: x[1]*x[2]).sum()
sumx_sq = rdd.map(lambda x: x[1]**2).sum()
sumy_sq = rdd.map(lambda x: x[2]**2).sum()

num = k * sumxy - sumx*sumy
den = math.sqrt((k*sumx_sq - sumx**2) * (k*sumy_sq - sumy**2))

out = num/den

print('----This is the pearson correlation-----')
print('Pearson Correlation = ', out)