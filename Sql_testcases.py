from pyspark.sql import SQLContext, Row
from pyspark import SparkContext
from pyspark import HiveContext
import unittest
import os

sc = SparkContext("local", "Test sql queries from pyspark")

#Test: if sqlcontext and hivecontext are created
sqlContext = SQLContext(sc)
hivecontext = HiveContext(sc)
   
#Test: Read from a parquet file using sql and hive context
df_sql=sqlContext.read.load("/Users/gayathrimurali/spark-1.5.1/examples/src/main/resources/users.parquet")
df_hive=hivecontext.read.load("/Users/gayathrimurali/spark-1.5.1/examples/src/main/resources/users.parquet")
df_sql.show()
df_hive.show()
df_hive.printSchema()
df_hive.filter(df_hive['favorite_color']=='red').show()

#Test: Write selected columns from dataframe into a parquet file
if not os.path.exists("/Users/gayathrimurali/sparksql_example/nameAndFavColors.parquet"):
   df_hive.select("name","favorite_color").write.save("/Users/gayathrimurali/sparksql_example/nameAndFavColors.parquet")

#Test : save dataframe as persistent hive table using hivecontext
#if not hivecontext.sql("SHOW TABLES LIKE '" + users + "'").collect().length == 1:
   df_hive.write.saveAsTable("users")

#Test : read from the hive table into a parquet file
colorRed=hivecontext.sql("SELECT * FROM users WHERE favorite_color=='red' ")
colorRed.show()
if not os.path.exists("/Users/gayathrimurali/sparksql_example/red.parquet"):
   colorRed.write.parquet("red.parquet")

#Test : Create and read from a parquet hive table using hivecontext
hivecontext.setConf("spark.sql.hive.convertMetastoreParquet", "false")
hivecontext.sql("CREATE TABLE IF NOT EXISTS  parquetTests(name STRING,favorite_color STRING,favorite_numbers ARRAY<INT>) ROW FORMAT SERDE 'parquet.hive.serde.ParquetHiveSerDe' STORED AS INPUTFORMAT 'parquet.hive.DeprecatedParquetInputFormat' OUTPUTFORMAT 'parquet.hive.DeprecatedParquetOutputFormat'")
hivecontext.sql("LOAD DATA LOCAL INPATH '/Users/gayathrimurali/spark-1.5.1/examples/src/main/resources/users.parquet' INTO TABLE parquetTests")
results=hivecontext.sql("SELECT * FROM parquetTests")
results.show()
#results.filter(results['favorite_color']=='red').show()
#for i in results:
#   print(i)

#Test : Write into a parquet hive table
#hivecontext.sql("INSERT overwrite parquetTests select * from users")


#############py.test code##################


#def test_context():
#   assert func()
  
#Test Read from a parquet file using sql and hive context
#def func():
#   df_sql=sqlContext.read.load("/Users/gayathrimurali/spark-1.5.1/examples/src/main/resources/users.parquet")
#   df_hive=hivecontext.read.load("/Users/gayathrimurali/spark-1.5.1/examples/src/main/resources/users.parquet")
#   if(df_hive.select("name","favorite_color").write.save("/Users/gayathrimurali/sparksql_example/nameAndFavColors.parquet")):
#	print("write successful")
#	return 1
#   else:
#	return 0
#   df_sql.select("name","favorite_color")
     
#def test_read():
#   assert func()==1

