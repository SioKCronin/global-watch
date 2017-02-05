from operator import itemgetter
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
import pyspark_cassandra

SPARK_IP = "ip-172-31-2-11.us-west-2.compute.internal"
SPARK_PORT = "7077"

conf = SparkConf() \
       .setMaster("spark://%s:%s" % (SPARK_IP, SPARK_PORT))

sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)
path = "s3a://gdelt-open-data/events/20130530.export.csv"
rdd = sc.textFile(path)

def rddCleaning(rd):
    date = 1
    country = 51
    event_c1 = 12
    mentions = 33
    event_c2 = 22

    def fillin(tup):
        if tup[2] == "" and tup[3] != "":
            return ((tup[0],tup[1],tup[3],tup[4]))
        else:
            return ((tup[0],tup[1],tup[2],tup[4]))

    rdd_reduce = rd.map(lambda x: x.split('\t')) \
                    .map(lambda y: ((int(y[date]), y[country], y[event_c1], y[event_c2], int(y[mentions])))) \
                    .map(fillin) \
                    .map(lambda f: (((f[0],f[1],f[2]),f[3]))) \
                    .reduceByKey(lambda a,b: a+b)

    rdd_format = rdd_reduce.map(lambda t:((t[0][1],t[0][0]),[(t[0][2],t[1])])) \
                           .filter(lambda c: c[1][0][0] != "") \
                           .reduceByKey(lambda a, b: a+b) \
                           .map(lambda v:(v[0],sorted(v[1],key=itemgetter(1), reverse=True))) \
                           .map(lambda f:((f[0],f[1][:5]))) \
                           .map(lambda s: ((s[0],dict(s[1]))))

    return rdd_format

rdd_cleaned = rddCleaning(rdd)

print("#########################################")
print(rdd_cleaned.take(6))
print("#########################################")

#table_cass = sc.parallelize(rdd_format)
rdd_cleaned.saveToCassandra("gdelt","world_gather")


##############
#CREATE KEYSPACE gdelt WITH REPLICATION = { 'class' : 'SimpleStrategy', 'repli ation_factor' : 3 };
# CREATE TABLE gdelt.world_gather (country varchar, date int, mention map <text,int>, PRIMARY KEY ( country, date ) );
# SELECT * FROM gdelt.world_gather;
