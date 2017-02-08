from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from abbreviations_dict import tofullname, toevent
from operator import itemgetter
# import pyspark_cassandra

SPARK_IP = "ip-172-31-2-11.us-west-2.compute.internal"
SPARK_PORT = "7077"

conf = SparkConf() \
       .setMaster("spark://%s:%s" % (SPARK_IP, SPARK_PORT))

sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)
df = sqlContext.read \
               .parquet("s3a://calvarado-gdelt/EVENTS1.0/part-r-00000-4163306b-eb6d-46de-adbe-9f0b7be5c269.gz.parquet")
            #    .parquet("s3a://calvarado-gdelt/events1.0/*")
rdd = df.rdd

def rddCleaning(rd,timeframe):

    def fillin(tup):
        if tup[2] == "" and tup[3] != "":
            return ((tup[0],tup[1],tup[3],tup[4],tup[5],tup[6]))
        else:
            return ((tup[0],tup[1],tup[2],tup[4],tup[5],tup[6]))

    def popular_avg(tup):
        country =  tup[0]
        lst = tup[1]
        dictionary = tup[2]
        dict_matches = {}
        for tup in lst:
	    event_type = tup[0]
            dict_matches[event_type] = dictionary[event_type]
        return ((country,lst,dict_matches))

    def merge_info(tup):
        main_dict = tup[1]
        info_dict = tup[2]
        for key in info_dict:
            main_dict[key].update(info_dict[key])
        return ((tup[0],main_dict))

    def event_todict(tup):
        lst = tup[1]
        dict_matches = {}
        for event_tup in lst:
            dict_matches[event_tup[0]] = {"ArticleMentions":event_tup[1]}
        return ((tup[0],dict_matches,tup[2]))



    rdd_reduce  = rd.map(lambda y: ((y["ActionGeo_CountryCode"],
                                     int(y[timeframe]),
                                     y["Actor1Type1Code"],
                                     y["Actor2Type1Code"],
                                     int(y["NumArticles"]),
                                     int(float((y["GoldsteinScale"]))),
                                     int(float(y["AvgTone"]))))) \
		    .map(fillin) \
                    .filter(lambda r: r[0] in tofullname and r[2] in toevent and  r[2] != "" and r[0] != "") \
                    .map(lambda t: ((tofullname[t[0]],t[1],toevent[t[2]],t[3],t[4],t[5]))) \
                    .map(lambda f: (((f[0],f[1],f[2]),(f[3],f[4],f[5],1)))) \
		    .reduceByKey(lambda a,b: (a[0]+b[0], a[1]+b[1], a[2]+b[2], a[3]+b[3])) \
		    .map(lambda s: ((s[0],(s[1][0],s[1][1]/s[1][3],s[1][2]/s[1][3]))))


    rdd_format = rdd_reduce.map(lambda t:((t[0][0],t[0][1]),
                                          ([(t[0][2],t[1][0])],
                                          [(t[0][2],{"GoldsteinScaleAvg":t[1][1],
                                                    "ToneAvg":t[1][2]})]))) \
			   .reduceByKey(lambda a, b: (a[0]+b[0], a[1]+b[1])) \
			   .map(lambda v: (v[0],
                                           sorted(v[1][0],key=itemgetter(1),reverse=True),
                                           v[1][1])) \
                           .map(lambda f: ((f[0],f[1][:5],dict(f[2])))) \
                           .map(popular_avg) \
		           .map(event_todict) \
                           .map(merge_info) \
			   .map(lambda d: ((d[0][0],d[0][1],d[1])))

    return rdd_format

rdd_cleaned = rddCleaning(rdd,"SQLDATE")

print("#########################################")
print(rdd_cleaned.take(6))
print("#########################################")

#table_cass = sc.parallelize(rdd_format)
# rdd_cleaned.saveToCassandra("gdelt","world_gather") ####UNCOMMENT THIS LINE


##############
#CREATE KEYSPACE gdelt WITH REPLICATION = { 'class' : 'SimpleStrategy', 'repli ation_factor' : 3 };
# CREATE TABLE gdelt.world_gather (country varchar, date int, mention map <text,int>, PRIMARY KEY ( country, date ) );
# SELECT * FROM gdelt.world_gather;





# def changeabbrev(tupl):
#     abbrev = tupl[0][0]
#     return ((tofullname[abbrev],tupl[0][1]),tupl[1])
# rdd_reduce = rd.map(lambda x: x.split('\t')) \
#                 .map(lambda y: ((int(y[date]), y[country], y[event_c1], y[event_c2], int(y[mentions])))) \

# rdd_reduce  = rd.map(lambda y: ((y["ActionGeo_CountryCode"],
#                                  int(y["SQLDATE"]),
#                                  y["Actor1Type1Code"],
#                                  y["Actor2Type1Code"],
#                                  int(y["NumArticles"])))) \
#                 .map(fillin) \
#                 .map(lambda: t: ((tofullname[t[0]],t[1],toevent[t[2]],t[3])))
#                 .map(lambda f: (((f[0],f[1],f[2]),f[3]))) \
#                 .reduceByKey(lambda a,b: a+b)
#
# rdd_format = rdd_reduce.map(lambda t:((t[0][0],t[0][1]),[(t[0][2],t[1])])) \
#                        .filter(lambda c: c[1][0][0] != "") \
#                        .reduceByKey(lambda a, b: a+b) \
#                        .map(lambda v:(v[0],sorted(v[1],key=itemgetter(1), reverse=True))) \
#                        .map(lambda f:((f[0],f[1][:5]))) \
#                     #    .map(changeabbrev) \
#                        .map(lambda s: ((s[0],dict(s[1])))) \
#                        .map(lambda d: ((d[0][0],d[0][1],d[1])))
