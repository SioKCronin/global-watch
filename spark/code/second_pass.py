from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark.sql.functions import udf
from pyspark.sql.functions import col
from pyspark.sql.types import StringType, DoubleType, IntegerType
from abbreviations_dict import tofullname, toevent
from operator import itemgetter
from pyspark import StorageLevel
import pyspark_cassandra

sc = SparkContext()
sqlContext = SQLContext(sc)

customSchema =  StructType([
        StructField('GLOBALEVENTID',StringType(),True),
        StructField('SQLDATE',StringType(),True),
        StructField('MonthYear',StringType(),True),
        StructField('Year',StringType(),True),
        StructField('FractionDate',StringType(),True),
        StructField('Actor1Code',StringType(),True),
        StructField('Actor1Name',StringType(),True),
        StructField('Actor1CountryCode',StringType(),True),
        StructField('Actor1KnownGroupCode',StringType(),True),
        StructField('Actor1EthnicCode',StringType(),True),
        StructField('Actor1Religion1Code',StringType(),True),
        StructField('Actor1Religion2Code',StringType(),True),
        StructField('Actor1Type1Code',StringType(),True),
        StructField('Actor1Type2Code',StringType(),True),
        StructField('Actor1Type3Code',StringType(),True),
        StructField('Actor2Code',StringType(),True),
        StructField('Actor2Name',StringType(),True),
        StructField('Actor2CountryCode',StringType(),True),
        StructField('Actor2KnownGroupCode',StringType(),True),
        StructField('Actor2EthnicCode',StringType(),True),
        StructField('Actor2Religion1Code',StringType(),True),
        StructField('Actor2Religion2Code',StringType(),True),
        StructField('Actor2Type1Code',StringType(),True),
        StructField('Actor2Type2Code',StringType(),True),
        StructField('Actor2Type3Code',StringType(),True),
        StructField('IsRootEvent',StringType(),True),
        StructField('EventCode',StringType(),True),
        StructField('EventBaseCode',StringType(),True),
        StructField('EventRootCode',StringType(),True),
        StructField('QuadClass',StringType(),True),
        StructField('GoldsteinScale',StringType(),True),
        StructField('NumMentions',StringType(),True),
        StructField('NumSources',StringType(),True),
        StructField('NumArticles',StringType(),True),
        StructField('AvgTone',StringType(),True),
        StructField('Actor1Geo_Type',StringType(),True),
        StructField('Actor1Geo_FullName',StringType(),True),
        StructField('Actor1Geo_CountryCode',StringType(),True),
        StructField('Actor1Geo_ADM1Code',StringType(),True),
        StructField('Actor1Geo_Lat',StringType(),True),
        StructField('Actor1Geo_Long',StringType(),True),
        StructField('Actor1Geo_FeatureID',StringType(),True),
        StructField('Actor2Geo_Type',StringType(),True),
        StructField('Actor2Geo_FullName',StringType(),True),
        StructField('Actor2Geo_CountryCode',StringType(),True),
        StructField('Actor2Geo_ADM1Code',StringType(),True),
        StructField('Actor2Geo_Lat',StringType(),True),
        StructField('Actor2Geo_Long',StringType(),True),
        StructField('Actor2Geo_FeatureID',StringType(),True),
        StructField('ActionGeo_Type',StringType(),True),
        StructField('ActionGeo_FullName',StringType(),True),
        StructField('ActionGeo_CountryCode',StringType(),True),
        StructField('ActionGeo_ADM1Code',StringType(),True),
        StructField('ActionGeo_Lat',StringType(),True),
        StructField('ActionGeo_Long',StringType(),True),
        StructField('ActionGeo_FeatureID',StringType(),True),
        StructField('DATEADDED',StringType(),True),
        StructField('SOURCEURL',StringType(),True)])

#processing one year at a time
df = sqlContext.read \
    .format('com.databricks.spark.csv') \
    .options(header='false') \
    .options(delimiter="\t") \
    .load('s3a://gdelt-open-data/events/20160201.csv', schema = customSchema)

#defining udfs to process dataframe
def modify_values(r,y):
   if r == '' and y != '':
	return y
   else:
        return r

def country_exists(r):
   if r in tofullname:
        return tofullname[r]
   else:
	return ''

def event_exists(r):
   if r in toevent:
	return toevent[r]
   else:
	return ''

c_exists = udf(country_exists,StringType())
e_exists = udf(event_exists,StringType())
dfsub1 =  df.withColumn("ActionGeo_CountryCode",c_exists(col("ActionGeo_CountryCode"))) \
	        .withColumn("Actor1Type1Code",e_exists(col("Actor1Type1Code")))

sqlContext.registerDataFrameAsTable(dfsub1, 'temp')
df2 = sqlContext.sql("""SELECT ActionGeo_CountryCode,
                               CAST(SQLDATE AS INTEGER), CAST(MonthYear AS INTEGER), CAST(Year AS INTEGER),
                               CASE WHEN Actor1Type1Code = '' AND Actor2Type1Code <> '' THEN Actor2Type1Code
				                     ELSE Actor1Type1Code END AS Actor1Type1Code,
                               CAST(NumArticles AS INTEGER),
                               CAST(GoldsteinScale AS INTEGER),
                               CAST(AvgTone AS INTEGER)
                          FROM temp
                         WHERE ActionGeo_CountryCode <> '' AND ActionGeo_CountryCode IS NOT NULL
                            AND Actor1Type1Code <> '' AND Actor1Type1Code IS NOT NULL
                            AND NumArticles <> '' AND NumArticles IS NOT NULL
                            AND GoldsteinScale <> '' AND GoldsteinScale IS NOT NULL
                            AND AvgTone <> '' AND AvgTone IS NOT NULL""")

sqlContext.dropTempTable('temp')
sqlContext.registerDataFrameAsTable(df2, 'temp3')
sqlContext.cacheTable('temp3')

dfdaily = sqlContext.sql("""SELECT ActionGeo_CountryCode,
				                   SQLDATE,
				                   Actor1Type1Code,
 				                   SUM(NumArticles) AS NumArticles,
                                   ROUND(AVG(GoldsteinScale),0) AS GoldsteinScale,
			                       ROUND(AVG(AvgTone),0) AS AvgTone
            			      FROM temp3
            			     GROUP BY ActionGeo_CountryCode,
            				          SQLDATE,
            				          Actor1Type1Code""")

dfmonthly = sqlContext.sql("""SELECT ActionGeo_CountryCode,
                				     MonthYear,
                				     Actor1Type1Code,
                				     SUM(NumArticles) AS NumArticles,
                				     ROUND(AVG(GoldsteinScale),0) AS GoldsteinScale,
                			      	 ROUND(AVG(AvgTone),0) as AvgTone
                				FROM temp3
            			       GROUP BY ActionGeo_CountryCode,
                    					MonthYear,
                    					Actor1Type1Code""")

dfyearly = sqlContext.sql("""SELECT ActionGeo_CountryCode,
                				    Year,
                				    Actor1Type1Code,
                				    SUM(NumArticles) AS NumArticles,
                				    ROUND(AVG(GoldsteinScale),0) AS GoldsteinScale,
                                    ROUND(AVG(AvgTone),0) as AvgTone
            			       FROM temp3
            			      GROUP BY ActionGeo_CountryCode,
                				       Year,
                				       Actor1Type1Code""")

def rddCleaning(rd,timeframe):

    def popular_avg(curr_tup):
        lst = curr_tup[1]
        dictionary = curr_tup[2]
        dict_matches = {}
        for tup in lst:
	    event_type = tup[0]
            dict_matches[event_type] = dictionary[event_type]
        return ((curr_tup[0],lst,dict_matches,curr_tup[3]))

    def merge_info(tup):
        main_dict = tup[1]
        info_dict = tup[2]
        for key in info_dict:
            main_dict[key].update(info_dict[key])
	main_dict["TotalArticles"] = {"total":tup[3]}
        return ((tup[0],main_dict))

    def event_todict(tup):
        lst = tup[1]
        dict_matches = {}
        for event_tup in lst:
            dict_matches[event_tup[0]] = {"ArticleMentions":event_tup[1]}
        return ((tup[0],dict_matches,tup[2],tup[3]))

    def sum_allevents(tup):
        type_lst = tup[1]
        total_mentions = 0
        for event in type_lst:
                total_mentions += event[1]
        return ((tup[0],type_lst,tup[2],total_mentions))

    rdd_format = rd.map(lambda y: ((y["ActionGeo_CountryCode"],y[timeframe]),
                                   ([(y["Actor1Type1Code"],y["NumArticles"])],
				                    [(y["Actor1Type1Code"],
                                     {"Goldstein":y["GoldsteinScale"],"ToneAvg":y["AvgTone"]})]))) \
        		   .reduceByKey(lambda a, b: (a[0]+b[0], a[1]+b[1])) \
        	       .map(lambda v: (v[0],
                                   sorted(v[1][0],key=itemgetter(1),reverse=True),
                                   dict(v[1][1]))) \
        		   .map(sum_allevents) \
                   .map(popular_avg) \
        		   .map(event_todict) \
                   .map(merge_info) \
        	       .map(lambda d: ((d[0][0],d[0][1],d[1])))

    return rdd_format


daily_rdd = rddCleaning(dfdaily.rdd,"SQLDATE")
print(daily_rdd.take(5))
monthly_rdd = rddCleaning(dfmonthly.rdd,"MonthYear")
print(monthly_rdd.take(5))
yearly_rdd = rddCleaning(dfyearly.rdd,"Year")
print(yearly_rdd.take(5))
