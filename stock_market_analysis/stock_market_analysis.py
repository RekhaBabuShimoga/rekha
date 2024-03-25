from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

#create spark session object
spark = SparkSession.builder.appName("stock_market_analysis").getOrCreate()

# read the text data from s3 bucket
read_text_nyse_data = spark.read.text("s3://mkinput1/stocks/NYSE_daily_File.txt")
text_data_values = read_text_nyse_data.rdd.map(lambda x: x['value'])
split_data = text_data_values.map(lambda x : x.split("\t"))
stock_data = split_data.map(lambda x : (x[0].strip(),
                                        x[1].strip(),
                                        x[2].strip(),
                                        float(x[3].strip()),
                                        float(x[4].strip()),
                                        float(x[5].strip()),
                                        float(x[6].strip()),
                                        int(x[7].strip()),
                                        float(x[8].strip())))

#create schema
schema = StructType([
    StructField("stock exchange",StringType(),True),
    StructField("company symbol",StringType(),True),
    StructField("date",StringType(),True),
    StructField("open price of the day",FloatType(),True),
    StructField("high of the day",FloatType(),True),
    StructField("low of the day",FloatType(),True),
    StructField("close of the day",FloatType(),True),
    StructField("volume",IntegerType(),True),
    StructField("adjusted close price",FloatType(),True)

])

#create dataframe
stock_dataframe = spark.createDataFrame(stock_data,schema)

#filtering open price of the day and high close is  greater than 0 
stock_analysis1 = stock_dataframe.filter((stock_dataframe["open price of the day"] > 0) & (stock_dataframe["high of the day"] > 0) & (stock_dataframe["close of the day"] > 0) )

#save filtered data to table based on date since the date column having unique date bucketBy is prefered 
stock_analysis1.write.format("orc").bucketBy(4,"date").option("path","s3://rekha-babu/rekha_NYSE_Partition").saveAsTable('mukesh.rekha_NYSE_Partition')

#create table structure view
stock_table_view = stock_analysis1.createOrReplaceTempView("stock_data")

#finding the  average of (close price - open price) based on company symbol 
stock_analysis_return = spark.sql("select `company symbol`, round(avg((`close of the day`)-(`open price of the day`)),5) as avg_stock_price from stock_data group by `company symbol` ")

#highest loss
stock_analysis2_loss = spark.sql("select `company symbol`,round(avg(((`open price of the day`)-(`close of the day`))/(`open price of the day`)),5) as highest_losses from stock_data group by `company symbol`")

#save average data to table 
stock_analysis_return.write.format("orc").option("path","s3://rekha-babu/stock_price_variance").saveAsTable('mukesh.rekha_stock_price_variance')

#save average data to table 
stock_analysis2_loss.write.format("orc").option("path","s3://rekha-babu/loss_stock_price_variance").saveAsTable('mukesh.loss_rekha_stock_price_variance')