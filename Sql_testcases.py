from pyspark.sql import SQLContext, Row
from pyspark import SparkContext
from pyspark import HiveContext
import unittest
import os

sc = SparkContext("local", "Test sql queries from pyspark")

#Change this variable to point to your spak 1.5 example resources
examplefiles_path="/Users/gayathrimurali/spark-1.5.1/examples/src/main/resources/"


#Test 1: Sqlcontext and Hivecontext are created
sqlContext = SQLContext(sc)
hivecontext = HiveContext(sc)
   
#Test 2: Read from a parquet file using sql and hive context into a dataframe. Display and do some filter operations on the dataframe
df_sql=sqlContext.read.load(examplefiles_path + "users.parquet")
df_hive=hivecontext.read.load(examplefiles_path + "users.parquet")

df_sql.show()
df_hive.show()

df_hive.printSchema()

df_hive.filter(df_hive['favorite_color']=='red').show()

#Test 3: Write selected columns from dataframe into a parquet file

if not os.path.exists(examplefiles_path + "nameAndFavColors.parquet"):
   df_hive.select("name","favorite_color").write.save(examplefiles_path + "nameAndFavColors.parquet")

#Test 4: Save dataframe as persistent hive table using hivecontext
hivecontext.sql("DROP TABLE IF EXISTS users")
df_hive.write.saveAsTable("users")


#Test 5: Read from the hive table into a parquet file
colorRed=hivecontext.sql("SELECT * FROM users WHERE favorite_color=='red' ")
colorRed.show()
if not os.path.exists(examplefiles_path + "red.parquet"):
   colorRed.write.parquet(examplefiles_path + "red.parquet")

#Test 6: Create Parquet hive table. Read data from a parquet file and store it in the table
filepath=examplefiles_path + "users.parquet"
load_query="LOAD DATA LOCAL INPATH '"+filepath+"'OVERWRITE INTO TABLE parquetTests"
hivecontext.sql("CREATE TABLE IF NOT EXISTS  parquetTests(name STRING,favorite_color STRING,favorite_numbers ARRAY<INT>) STORED AS PARQUET")
hivecontext.sql(load_query)
results=hivecontext.sql("SELECT * FROM parquetTests")
results.show()

#Test 7 : Create a table from the avro example file. Select all the data from the avro table and write it to the parquet table. All in hive context
filepath=examplefiles_path + "users.avro"
load_query="LOAD DATA LOCAL INPATH '"+filepath+"'OVERWRITE INTO TABLE avroTests"

hivecontext.sql("CREATE TABLE IF NOT EXISTS avroTests(name STRING,favorite_color STRING,favorite_numbers ARRAY<INT>) STORED AS AVRO")
hivecontext.sql(load_query)
hivecontext.sql("INSERT INTO TABLE parquetTests SELECT * FROM avroTests")
results=hivecontext.sql("SELECT * FROM parquetTests")
results.show()

#Test 8 : Add custom SerDe

#hivecontext.sql("ADD JAR '/Users/gayathrimurali/.m2/repository/org/apache/hadoop/hive/serde2/TestSerDe/1.0/TestSerDe-1.0.jar'")
hivecontext.sql("CREATE TABLE IF NOT EXISTS INPUT16_CC(KEY STRING,VALUE STRING) ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.TestSerDe'  with serdeproperties ('testserde.default.serialization.format'='\003', 'dummy.prop.not.used'='dummyy.val') STORED AS TEXTFILE")
hivecontext.sql("LOAD DATA LOCAL INPATH 'TestSerde/kv1_cc.txt' INTO TABLE INPUT16_CC")
results=hivecontext.sql("SELECT * FROM INPUT16_CC")
results.show()

#Adding hive UDFs
hivecontext.sql("CREATE TEMPORARY FUNCTION TOUPPER AS 'com.example.hive.udf.ToUpper'")
results=hivecontext.sql("SELECT TOUPPER(name) FROM users")
results.show()


#Performing different table joins
#Create and insert values into 2 tables
filepath_1=examplefiles_path + "states.txt"
filepath_2=examplefiles_path + "gdp.txt"
load_query_1="LOAD DATA LOCAL INPATH '"+filepath_1+"'OVERWRITE INTO TABLE Table1"
load_query_2="LOAD DATA LOCAL INPATH '"+filepath_2+"'OVERWRITE INTO TABLE Table2"
hivecontext.sql("DROP TABLE IF EXISTS Table1")
hivecontext.sql("DROP TABLE IF EXISTS Table2")
hivecontext.sql("CREATE TABLE IF NOT EXISTS Table1(k1 STRING,v1 STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE")
hivecontext.sql(load_query_1)
hivecontext.sql("CREATE TABLE IF NOT EXISTS Table2(k1 STRING,v1 STRING)ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE")
hivecontext.sql(load_query_2)
results_1=hivecontext.sql("SELECT * FROM Table1")
results_1.show()
results_2=hivecontext.sql("SELECT * FROM Table2")
results_2.show()

#hivecontext.sql("INSERT INTO TABLE Table1 VALUES ('1234','Alabama'),('3245','California'),('4567','Texas'),('7686','Utah'),('9872','Alaska')")


#Equality Join
results=hivecontext.sql("SELECT Table1.* FROM Table1 JOIN Table2 ON (Table1.v1=Table2.k1)")
results.show()

#Left Outer Join
results=hivecontext.sql("SELECT Table1.k1,Table1.v1,Table2.k1,Table2.v1 FROM Table1 LEFT OUTER JOIN Table2 ON (Table1.v1=Table2.k1)") 
results.show()

#Right Outer Join
results=hivecontext.sql("SELECT Table1.k1,Table1.v1,Table2.k1,Table2.v1 FROM Table1 RIGHT OUTER JOIN Table2 ON (Table1.v1=Table2.k1)")
results.show()

#Full Outer Join
results=hivecontext.sql("SELECT Table1.k1,Table1.v1,Table2.k1,Table2.v1 FROM Table1 FULL OUTER JOIN Table2 ON (Table1.v1=Table2.k1)")
results.show()



##############Scraped code#################################################
#Test : Use Regex serde to store text file

#hivecontext.sql("CREATE TABLE IF NOT EXISTS serde_regex(host STRING,identity STRING,user STRING,time STRING,request STRING,status STRING,size STRING,referer STRING,agent STRING) ROW FORMAT SERDE 'org.apache.hadoop.hive.contrib.serde2.RegexSerDe' WITH SERDEPROPERTIES ('input.regex' = '([^]*) ([^]*) ([^]*) (-|\\[^\\]*\\) ([^ \"]*|\"[^\"]*\") (-|[0-9]*) (-|[0-9]*)(?: ([^ \"]*|\".*\") ([^ \"]*|\".*\"))?', 'output.format.string' = '%1$s %2$s %3$s %4$s %5$s %6$s %7$s %8$s %9$s') STORED AS TEXTFILE") 

#hivecontext.sql("LOAD DATA LOCAL INPATH '/Users/gayathrimurali/spark-1.5.1/sql/hive/src/test/resources/data/files/apache.access.log' OVERWRITE INTO TABLE serde_regex")

#results=hivecontext.sql("SELECT * FROM serde_regex")
#results.show()


