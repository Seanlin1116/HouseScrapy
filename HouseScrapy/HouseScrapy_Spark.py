from pyspark import SparkConf, SparkContext 
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, struct, desc, collect_list
from pyspark.sql.types import BooleanType, StringType

import collections
import json

# user define function -- filter the floor that larger than 13
def filter_13(floor):
    dic ={"一":1,"二":2,"三":3,"四":4,"五":5,"六":6,"七":7,"八":8,"九":9,"十":10,
    "十一":11, "十二":12, "十三":13, "十四":14, "十五":15, "十六":16, "十七":17, "十八":18, "十九":19,
    "二十":20, "三十":30,"四十":40,"五十":50,"六十":60,"七十":70,"八十":80,"九十":90,}
    if len(floor) == 3:
        return dic[floor[:2]] >= 13
    elif len(floor) == 4:
        return(dic[floor[:2]] + dic[floor[2:3]]) >= 13
    else:
        return False

# user define function -- transfer the date from Taiwan date to AD date
def covert_date(d):
    adDate = str(int(d) + 19110000)
    year = adDate[0:4]
    month = adDate[4:6]
    day = adDate[6:8]
    return year + "-" + month + "-" + day

# init spark/ with 2 partitions made
spark = SparkSession.builder.master("local").appName("HouseScrape").config("spark.sql.shuffle.partitions", 2).config('spark.sql.warehouse.dir', 'file:///C:/path/to/my/').getOrCreate()

# read data from certain csv files
lands = spark.read.format('csv').option('header', 'true').load('Downloads/*')
# drop the empty data
lands.na.drop()

# filter the data
lands = lands.filter((lands["主要用途"] == "住家用") & (lands["建物型態"].contains("住宅大樓")))
# filter the data with udf
filter_udf = udf(filter_13, BooleanType())
lands = lands.filter(filter_udf(lands["總樓層數"]))

# create a new column for city name

lands = lands.withColumn("縣市名稱",(lands["土地區段位置建物區段門牌"]).substr(1,3))
# select the columns that matches requirement
lands = lands.select(lands["縣市名稱"], lands['交易年月日'], lands['鄉鎮市區'], lands['建物型態'])
# rename the column for further requirement
lands = lands.withColumnRenamed('縣市名稱','city').withColumnRenamed('交易年月日','date').withColumnRenamed('鄉鎮市區','district').withColumnRenamed('建物型態','building_state')

# transfer the date from Taiwan date to AD date via udf
converter_udf = udf(covert_date, StringType())
lands = lands.withColumn("date", converter_udf("date"))

# desc order by date
lands = lands.sort(desc("date"))

# groupBy date and cities
lands = lands.groupBy("city", "date").agg(collect_list(struct("district", "building_state")).alias('event'))
result = lands.groupBy("city").agg(collect_list(struct("date", "event")).alias('time_slots'))

# output a json file
result.write.format('json').mode('overwrite').save('result_json')
