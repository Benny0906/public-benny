# Databricks notebook source
# MAGIC %md #### workshop for ETL

# COMMAND ----------

df_laptimes = spark.read.csv('s3://columbia-gr5069-main/raw/lap_times.csv', header=True)

# COMMAND ----------

display(df_laptimes)

# COMMAND ----------

df_driver = spark.read.csv('s3://columbia-gr5069-main/raw/drivers.csv', header=True)
df_driver.count()

# COMMAND ----------

display(df_driver)

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_date, datediff, lit

# Assuming your Spark session is already underway
spark = SparkSession.builder.appName("CalculateDriverAges").getOrCreate()

# Reading your data from the S3 bucket, as previously mentioned
df_driver = spark.read.csv('s3://columbia-gr5069-main/raw/drivers.csv', header=True)

# Given the 'dob' column format matches 'yyyy-MM-dd' as shown in your scroll, we convert it directly
df_driver = df_driver.withColumn('dob', col('dob').cast('date'))

# Calculating the age now
df_driver = df_driver.withColumn('age', (datediff(current_date(), col('dob')) / 365).cast('int'))

# Display the enchanted table to confirm the age calculation
df_driver.show()



# COMMAND ----------

from pyspark.sql.types import IntegerType
from pyspark.sql.functions import datediff, current_date, avg

# COMMAND ----------

df_driver = df_driver.withColumn('age', df_driver['age'].cast(IntegerType()))

# COMMAND ----------

display(df_driver)

# COMMAND ----------

df_lap_drivers = df_driver.select(['driverID','nationality','age','forename','surname','url']).join(df_laptimes, on=['driverId'])
display(df_lap_drivers)

# COMMAND ----------

# MAGIC %md
# MAGIC Aggregate by Age

# COMMAND ----------

df_lap_drivers = df_lap_drivers.groupby('nationality','age').agg(avg('milliseconds'))
display(df_lap_drivers)

# COMMAND ----------

# MAGIC %md
# MAGIC storing data in s3

# COMMAND ----------

df_lap_drivers.write.csv('s3://bh2896-gr5069/processed/in_class_workshop/laptimes_by_drivers.csv')
