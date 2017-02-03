from operator import itemgetter
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
import pyspark_cassandra
#from cassandra.cluster import Cluster

SPARK_IP = "ip-172-31-2-11.us-west-2.compute.internal"
SPARK_PORT = "7077"

conf = SparkConf() \
       .setMaster("spark://%s:%s" % (SPARK_IP, SPARK_PORT))
       #.set("spark.cassandra.connection.host", "cas-1")

sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)
path = "s3a://gdelt-open-data/events/20130530.export.csv"
rdd = sc.textFile(path)

date = 1
country = 51
e_type = 12
num_mentions = 33
SQLdate = 1
arr_compressed = rdd.map(lambda x: x.split('\t')) \
                    .map(lambda y: ((int(y[date]), y[country], y[e_type]), int(y[num_mentions]))) \
                    .reduceByKey(lambda a,b: a+b)

further_com = arr_compressed.map(lambda t:((t[0][1],t[0][0]),[(t[0][2],t[1])])) \
                            .reduceByKey(lambda a, b: a+b) \
                            .map(lambda v:(v[0],sorted(v[1],key=itemgetter(1), reverse=True))) \
                            .map(lambda f:((f[0],f[1][:3]))) \
                            .map(lambda g:((g[0],tuple(g[1])))) \
                            .filter(lambda x: len(x[1]) == 3) \
                            .map(lambda l: (l[0][0],l[0][1],l[1])) \
                            .map(lambda k: (k[0],k[1],{k[2][0][0]:k[2][0][1],k[2][1][0]:k[2][1][1],k[2][2][0]:k[2][2][1]}))

print("#########################################")
print(further_com.take(6))
print("#########################################")

#table_cass = sc.parallelize(further_com)
further_com.saveToCassandra("gdelt","world_gather")


##############
#CREATE KEYSPACE gdelt WITH REPLICATION = { 'class' : 'SimpleStrategy', 'repli ation_factor' : 3 };
# CREATE TABLE gdelt.world_gather (country varchar, date int, mention map <text,int>, PRIMARY KEY ( country, date ) );
# SELECT * FROM gdelt.world_gather;
