# job1

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

#create spark session object
spark = SparkSession.builder.appName("rekha_Hr_data_analysis_job1").getOrCreate()

#reading the text data from s3 bucket
read_text_data = spark.read.text("s3://mkinput1/employees/Consultentdata.txt")
rowrdd = read_text_data.rdd.map(lambda x : x['value'])
split_rdd = rowrdd.map(lambda x : x.split('|'))
employee_details = split_rdd.map(lambda x : (int(x[0]),int(x[1]),x[2],x[3],x[4]))


# create schema
schema = StructType([
    StructField("id",IntegerType(),True),
    StructField("age",IntegerType(),True),
    StructField("gender",StringType(),True),
    StructField("role",StringType(),True),
    StructField("salary",StringType(),True),
])

#create dataframe
employee_df = spark.createDataFrame(employee_details,schema)

#Droping the null values records if present
cleaned_employee_details = employee_df.dropna()

#The below example creates a new Boolean column 'value', it holds true for the numeric value and false for non-numeric. In order to do this, 
#I have done a column cast from string column to int and check the result of cast is null. 
#cast() function return null when it unable to cast to a specific type.
# cast() used to change the data type of column  supoose if it is not able to convert then it returns null
#checking the salary column contains non-numeric values are not if present replace column value with 10000
Final_employee_details = cleaned_employee_details.withColumn("salary",when(col("salary").cast("int").isNotNull(),col('salary')).otherwise("10000"))
print(Final_employee_details.show(10))

#save the Final_employee_details in Consultant_Table_Bucket folder in s3 bucket
s3_target_path = "s3://rekha-babu/Consultant_Table_Bucket"
Final_employee_details.write.format("orc").bucketBy(4, "Salary").option("path", s3_target_path).saveAsTable("mukesh.rekha_Consultant_Table_Bucket")


#job2    rekha_highsalaryEmployees_job2

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import * 


#create spark session object
spark = SparkSession.builder.appName("rekha_highsalaryEmployees_job2").getOrCreate()

#read the data from s3 Consultant_Table_Bucket
emp_data = spark.read.format("orc").load("s3://rekha-babu/Consultant_Table_Bucket")


#replacing the consultant role to DataEngineer
replaced_data = emp_data.withColumn("role",regexp_replace("role","consultant",'DataEngineer'))

# filtering  the salary which is greater than 50000 
filtered_employee = replaced_data.filter(replaced_data['salary']>'50000')
print("filtered employee details",filtered_employee.show(10))

#save the filtered_employee in High_salary_employee_details folder in s3 bucket
#saveAsTable creates table structure in datacatalog table so that in athena we can run query 
#saveAsTable eliminates the use of crawlers to create table structure in data catalog
s3_target_path = "s3://rekha-babu/High_salary_employee_details"
filtered_employee.write.format("orc").option("path", s3_target_path).saveAsTable("mukesh.rekha_High_salary_employee_details")

spark.stop()