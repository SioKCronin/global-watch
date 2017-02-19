"""
This script reads all of the files in an Amazon bucket called
gdelt-open-data. Once it reads all of the files in turns them
into one rdd and stores it into memory. I am using a master
node and three worker nodes.

The main function here is rddCleaning, which takes in an rdd
and a timeframe and outputs and updated rdd. Here timeframe is
a string that signifies what kind of aggregation needs to be
done on the data. For example, SQLDATE signifies that daily
aggregations need to be made for all each country and day in
the data. The data in the rdd dates back to 1979 to the present.

Args:
    rd: An rdd
    timeframe: A timeframe specified by the user: "SQLDATE"
               (daily aggregation), or "MonthYear" (monthly
               aggregation), or "Year" (yealy aggragation)

Returns:
    A rdd that is an aggregated and simplified version of the
    rdd that was inputted. The rdd returned is a summary of
    all of the news data dating back to 1979.
"""
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from abbreviations_dict import tofullname, toevent
from operator import itemgetter
import pyspark_cassandra

sc = SparkContext()
sqlContext = SQLContext(sc)
rdd = sc.textFile("s3a://gdelt-open-data/events/*")
rdd.cache()

def rddCleaning(rd,timeframe):
    def check_valid(tup):
        #checks that a string can be turned to int/float
        #used to filter out rows that to do meet above
        #criteria
        try:
            int(tup[1])
            int(tup[4])
            float(tup[5])
            float(tup[6])
            return True
        except ValueError:
            return False

    def fillin(tup):
        #fills in values for missing information
        if tup[2] == "" and tup[3] != "":
            return ((tup[0], tup[1], tup[3], tup[4], tup[5], tup[6]))
        else:
            return ((tup[0], tup[1], tup[2], tup[4], tup[5], tup[6]))

    def popular_avg(curr_tup):
        #finds the most populat averages in dictionary of events
        lst = curr_tup[1]
        dictionary = curr_tup[2]
        dict_matches = {}
        for tup in lst:
	    event_type = tup[0]
            dict_matches[event_type] = dictionary[event_type]
        return ((curr_tup[0], lst, dict_matches, curr_tup[3]))

    def merge_info(tup):
        #updates dictionary in tuple and adds new key and value
        main_dict = tup[1]
        info_dict = tup[2]
        for key in info_dict:
            main_dict[key].update(info_dict[key])
	    main_dict["TotalArticles"] = {"total":tup[3]}
        return ((tup[0], main_dict))

    def event_todict(tup):
        #adding new key value to dicitonary for each event
        #this value represents the total number of times each
        #event was mentioned in other news sources
        lst = tup[1]
        dict_matches = {}
        for event_tup in lst:
            dict_matches[event_tup[0]] = {"ArticleMentions":event_tup[1]}
        return ((tup[0], dict_matches, tup[2], tup[3]))

    def sum_allevents(tup):
        #find the total sum of all the events for a country
        type_lst = tup[1]
        total_mentions = 0
        for event in type_lst:
                total_mentions += event[1]
        return ((tup[0], type_lst, tup[2], total_mentions))

    actionGeo_CountryCode = 51
    time = 0
    actor1Type1Code = 12
    actor2Type1Code = 22
    numArticles = 33
    goldsteinScale = 30
    avgTone = 34

    if timeframe == "SQLDATE":
	time = 1
    elif timeframe == "MonthYear":
	time = 2
    else:
	time = 3

    rdd_start  = rd.map(lambda x: x.split('\t')) \
		           .map(lambda y: ((y[actionGeo_CountryCode],
                                    y[time],
                                    y[actor1Type1Code],
                                    y[actor2Type1Code],
                                    y[numArticles],
                                    y[goldsteinScale],
                                    y[avgTone]))) \
		           .filter(check_valid) \
		           .map(lambda c: ((c[0],int(c[1]), c[2], c[3], int(c[4]),
                                    int(float(c[5])), int(float(c[6]))))) \
		           .map(fillin) \
                   .filter(lambda r: r[0] in tofullname and
                                     r[2] in toevent and
                                     r[2] != "" and
                                     r[0] != "") \
                   .map(lambda t: ((tofullname[t[0]], t[1],
                                         toevent[t[2]], t[3], t[4],
                                         t[5]))) \
                   .map(lambda f: (((f[0], f[1], f[2]),
                                    (f[3],f[4],f[5],1)))) \
		           .reduceByKey(lambda a,b: (a[0]+b[0], a[1]+b[1], a[2]+b[2], a[3]+b[3])) \
		           .map(lambda s: ((s[0], (s[1][0], s[1][1]/s[1][3], s[1][2]/s[1][3]))))


    rdd_fin = rdd_start.map(lambda t:((t[0][0],t[0][1]),
                                      ([(t[0][2],t[1][0])],
                                       [(t[0][2],{"GoldsteinScaleAvg":t[1][1],
                                                  "ToneAvg":t[1][2]})]))) \
			           .reduceByKey(lambda a, b: (a[0]+b[0], a[1]+b[1])) \
			           .map(lambda v: (v[0],
                                       sorted(v[1][0], key=itemgetter(1), reverse=True),
                                       v[1][1])) \
		               .map(sum_allevents) \
                       .map(lambda f: ((f[0], f[1][:5], dict(f[2]), f[3]))) \
                       .map(popular_avg) \
		               .map(event_todict) \
                       .map(merge_info) \
			           .map(lambda d: ((d[0][0],d[0][1],d[1])))
    return rdd_fin

#Writing processed rdds into three Cassandra tables:
daily_rdd = rddCleaning(rdd,"SQLDATE")
daily_rdd.saveToCassandra("gdelt","daily")

monthly_rdd = rddCleaning(rdd,"MonthYear")
monthly_rdd.saveToCassandra("gdelt","monthly")

yearly_rdd = rddCleaning(rdd,"Year")
yearly_rdd.saveToCassandra("gdelt","yearly")
