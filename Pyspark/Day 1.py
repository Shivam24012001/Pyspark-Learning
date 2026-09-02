# Databricks notebook source
# MAGIC %md
# MAGIC Create a Spark session:

# COMMAND ----------

from pyspark.sql import SparkSession
spark=SparkSession.builder.appName("Day 1 Pyspark").getOrCreate()
print(spark)

# COMMAND ----------

# MAGIC %md
# MAGIC Create this DataFrame

# COMMAND ----------

data = [
    (1, "Shivam", "Data Engineering", 80000),
    (2, "Rahul", "Analytics", 70000),
    (3, "Priya", "Data Engineering", 90000),
    (4, "Amit", "Analytics", 65000),
    (5, "Neha", "Data Engineering", 85000)
]

column=["id","name","Department","Salary"]

df=spark.createDataFrame(data, column)

# COMMAND ----------

df.show()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.count()

# COMMAND ----------

df.columns

# COMMAND ----------

df.dtypes

# COMMAND ----------

# MAGIC %md
# MAGIC ### FIRST REAL DATA ENGINEER TASK

# COMMAND ----------

# MAGIC %md
# MAGIC We have an employee dataset. Before building the pipeline, inspect the data and tell me its structure, number of records, columns and data types."

# COMMAND ----------

# MAGIC %md
# MAGIC No of records
# MAGIC

# COMMAND ----------

df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC Column Name

# COMMAND ----------

df.columns

# COMMAND ----------

# MAGIC %md
# MAGIC Data types

# COMMAND ----------

df.dtypes

# COMMAND ----------

# MAGIC %md
# MAGIC Schema

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC Preview

# COMMAND ----------

df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Q1.
# MAGIC
# MAGIC What is Apache Spark?
# MAGIC
# MAGIC Apache spark is distributed compute engine which process large dataset parallel across mutiple machines.
# MAGIC
# MAGIC Q2.
# MAGIC
# MAGIC What is PySpark?
# MAGIC
# MAGIC PySpark is the Python API for Apache Spark that allows us to use Spark's distributed data-processing capabilities through Python.
# MAGIC
# MAGIC Q3.
# MAGIC
# MAGIC What is a SparkSession?
# MAGIC
# MAGIC SparkSession is the main entry point for interacting with Spark's DataFrame and SQL APIs in a PySpark application.
# MAGIC
# MAGIC Q4.
# MAGIC
# MAGIC What is a DataFrame in PySpark?
# MAGIC This is pyspark object, that is very similar to the pandas dataFrame but this is distributed dataFrame.
# MAGIC
# MAGIC A Spark DataFrame has:
# MAGIC
# MAGIC - rows
# MAGIC - named columns
# MAGIC - schema
# MAGIC - distributed partitions
# MAGIC
# MAGIC Q5.
# MAGIC
# MAGIC What is the difference between Pandas DataFrame and Spark DataFrame?
# MAGIC | Pandas                           | Spark                                 |
# MAGIC | -------------------------------- | ------------------------------------- |
# MAGIC | Usually single machine           | Distributed                           |
# MAGIC | Data generally in machine memory | Data distributed across executors     |
# MAGIC | Great for smaller datasets       | Designed for large-scale datasets     |
# MAGIC | Operations execute locally       | Operations can execute across cluster |
# MAGIC
# MAGIC Pandas is primarily single-machine, whereas Spark is designed for distributed processing across multiple machines.
# MAGIC
# MAGIC Q6.
# MAGIC
# MAGIC What is the Driver?
# MAGIC
# MAGIC Consider driver as coordinator,that will create an execution plan , coordinate works and commuticate with excutors.
# MAGIC
# MAGIC               DRIVER
# MAGIC                 │
# MAGIC         "What needs to happen?"
# MAGIC                 │
# MAGIC         Execution planning
# MAGIC                 │
# MAGIC        ┌────────┼────────┐
# MAGIC        ↓        ↓        ↓
# MAGIC    Executor Executor Executor
# MAGIC
# MAGIC Q7.
# MAGIC
# MAGIC What is an Executor?
# MAGIC Executors run tasks assigned by the Driver and can also cache data in memory/disk depending on the application.
# MAGIC
# MAGIC Q8.
# MAGIC
# MAGIC What is a Job?
# MAGIC
# MAGIC A Spark Job is created when an action is executed. The Job represents the work required to produce the requested result.
# MAGIC
# MAGIC Q9.
# MAGIC
# MAGIC What is a Stage?
# MAGIC
# MAGIC A Stage is a set of tasks that can be executed together without requiring another shuffle boundary.
# MAGIC Just remember:
# MAGIC
# MAGIC Shuffle → Stage boundary
# MAGIC
# MAGIC Q10.
# MAGIC
# MAGIC What is a Task?
# MAGIC
# MAGIC One task processes one partition for a particular stage.
# MAGIC
# MAGIC Q11.
# MAGIC
# MAGIC What is lazy evaluation?
# MAGIC
# MAGIC When we run any transformation then spark will not immedialty run the transformation it will first create the logical plan for the transformation then we hit the excute the task then it will trasform the data.
# MAGIC
# MAGIC Q12.
# MAGIC
# MAGIC Why doesn't Spark immediately execute:
# MAGIC df.filter(df.salary > 50000)
# MAGIC
# MAGIC Firstly it will create a plan then when we run command like show then it will tranform the data for me.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Now your first production-style problem.
# MAGIC
# MAGIC Imagine you're working at an e-commerce company.
# MAGIC
# MAGIC You receive:
# MAGIC
# MAGIC orders.csv
# MAGIC
# MAGIC `order_id
# MAGIC customer_id
# MAGIC product_id
# MAGIC order_date
# MAGIC quantity
# MAGIC amount`
# MAGIC
# MAGIC The file contains:
# MAGIC
# MAGIC 100 million records.
# MAGIC
# MAGIC Your manager says:
# MAGIC
# MAGIC "Load this into Spark and give me the schema and record count."
# MAGIC
# MAGIC Question:
# MAGIC
# MAGIC Would you use:
# MAGIC
# MAGIC df.collect()
# MAGIC to inspect the data?
# MAGIC
# MAGIC

# COMMAND ----------

from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    DateType,
    DoubleType
)

schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("order_date", DateType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("amount", DoubleType(), True)
])

df = spark.read.csv(
    "orders.csv",
    header=True,
    schema=schema
)

# COMMAND ----------

# MAGIC %md
# MAGIC Load 100M orders and give me schema and record count.
# MAGIC

# COMMAND ----------

df = spark.read.csv(
    "orders.csv",
    header=True,
    schema=schema
)

df.printSchema()

df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC No. I wouldn't use collect() because it brings all records back to the driver and can cause driver memory issues or OOM. I'd let Spark process the input in distributed partitions. I'd read the file using spark.read.csv(), preferably with an explicit schema, use printSchema() to inspect the structure, and use count() if an exact record count is required. For sampling data, I'd use show() or limit() rather than collect()

# COMMAND ----------

# MAGIC %md
# MAGIC This is more important than the previous questions.
# MAGIC
# MAGIC Imagine this code:

# COMMAND ----------

df = spark.read.csv("orders.csv")

df2 = df.filter(col("amount") > 1000)

df3 = df2.select(
    "order_id",
    "customer_id",
    "amount"
)

df3.count()

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Which lines are transformations?
# MAGIC There are two  transformation:
# MAGIC df.filter()
# MAGIC df.select()
# MAGIC
# MAGIC 2. Which line is the action?
# MAGIC
# MAGIC df3.count()
# MAGIC is action and trigger excution 
# MAGIC
# MAGIC 3. When does Spark actually start executing?
# MAGIC df.filter()
# MAGIC        ↓
# MAGIC df.select()
# MAGIC        ↓
# MAGIC NO actual processing yet
# MAGIC        ↓
# MAGIC df.count()
# MAGIC        ↓
# MAGIC Spark executes the required plan
# MAGIC
# MAGIC
# MAGIC 4. Does df.filter() immediately create a Spark Job?
# MAGIC
# MAGIC No, it creates plan and when we hit execute then it will create Spark job.
# MAGIC
# MAGIC 5. Does count() return all 100M records to the Driver?
# MAGIC
# MAGIC No, it just returns number of records.
# MAGIC 100,000,000 records
# MAGIC         ↓
# MAGIC count()
# MAGIC         ↓
# MAGIC 100,000,000
# MAGIC Only one number comes back to the driver.
# MAGIC count() can still require Spark to scan/process a huge amount of data.
# MAGIC Low result size ≠ low computation cost.
# MAGIC
# MAGIC
# MAGIC 6. Who actually processes the data?
# MAGIC
# MAGIC Executors execute tasks on partitions of data.
# MAGIC
# MAGIC 7. Where does the result of count() go?
# MAGIC
# MAGIC The result is returned to the Driver, because your application requested the result.
# MAGIC                   DRIVER
# MAGIC                     ↑
# MAGIC                     │
# MAGIC                 100000000
# MAGIC                     │
# MAGIC             ┌───────┴───────┐
# MAGIC             │               │
# MAGIC         Executor        Executor
# MAGIC             │               │
# MAGIC          partition       partition
# MAGIC
# MAGIC The executors do the distributed work.
# MAGIC
# MAGIC The final count is communicated back to the Driver.
# MAGIC
# MAGIC It may then be displayed in your notebook/console.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC                     PYSPARK CODE
# MAGIC                          │
# MAGIC                          ▼
# MAGIC                       DRIVER
# MAGIC                          │
# MAGIC                   builds execution plan
# MAGIC                          │
# MAGIC               Transformation operations
# MAGIC                          │
# MAGIC               filter / select / etc.
# MAGIC                          │
# MAGIC                     Lazy...
# MAGIC                          │
# MAGIC                     ACTION
# MAGIC                          │
# MAGIC                     count()
# MAGIC                          │
# MAGIC                         JOB
# MAGIC                          │
# MAGIC                  ┌───────┴───────┐
# MAGIC                  ▼               ▼
# MAGIC               STAGE            STAGE
# MAGIC                  │               │
# MAGIC              TASK TASK        TASK TASK
# MAGIC                  │               │
# MAGIC              EXECUTORS        EXECUTORS
# MAGIC                  │               │
# MAGIC               PARTITIONS       PARTITIONS
# MAGIC                          │
# MAGIC                          ▼
# MAGIC                        RESULT
# MAGIC                          │
# MAGIC                          ▼
# MAGIC                        DRIVER

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🚀 DAY 1 PRACTICAL ASSIGNMENT

# COMMAND ----------

# MAGIC %md
# MAGIC - We receive e-commerce orders every day. Build the initial PySpark ingestion step and perform basic data profiling before the transformation pipeline starts.

# COMMAND ----------

from pyspark.sql import SparkSession

# COMMAND ----------

# MAGIC %md
# MAGIC creating a DataFrame

# COMMAND ----------



data =[
    (101,"C001","P001","2026-08-01",2,1500),
    (102,"C002","P003","2026-08-01",1,800),
    (103,"C001","P002","2026-08-02",3,2200),
    (104,"C003","P001","2026-08-02",1,1500),
    (105,"C002","P004","2026-08-03",5,500)
]

column=["order_id","customer_id","product_id","order_date","quantity","amount"]

df=spark.createDataFrame(data, column)

# COMMAND ----------

# MAGIC %md
# MAGIC display data

# COMMAND ----------

df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Check Schema

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC Count Record

# COMMAND ----------

df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC Get Column Name

# COMMAND ----------

df.columns

# COMMAND ----------

# MAGIC %md
# MAGIC Get data types

# COMMAND ----------

df.dtypes

# COMMAND ----------

# MAGIC %md
# MAGIC Q1
# MAGIC
# MAGIC Your `amount` column is currently:
# MAGIC
# MAGIC `string`
# MAGIC
# MAGIC but business expects it to be numeric.
# MAGIC
# MAGIC What should you do?

# COMMAND ----------

from pyspark.sql.functions import col
df=df.withColumn("amount",col("amount").cast("int"))
df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC Q2
# MAGIC
# MAGIC Your `order_date` is coming as:
# MAGIC
# MAGIC `string`
# MAGIC
# MAGIC What should you eventually convert it to?

# COMMAND ----------

df

# COMMAND ----------

from pyspark.sql.functions import col, to_date
df=df.withColumn("date",to_date(col("order_date"),"yyyy-MM-dd"))
df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC Q3
# MAGIC
# MAGIC There are 100M records in production.
# MAGIC
# MAGIC Would you use:
# MAGIC
# MAGIC `df.show()`
# MAGIC
# MAGIC or
# MAGIC
# MAGIC `df.collect()`
# MAGIC
# MAGIC for debugging?
# MAGIC
# MAGIC Why?
# MAGIC
# MAGIC i will use the df.show() because it will not load all 100M data to driver directly and when i will use the df.collect() it will load all the 100M data to driver directly that will cause driver memory issues.

# COMMAND ----------

# MAGIC %md
# MAGIC Q4
# MAGIC
# MAGIC If the manager asks:
# MAGIC
# MAGIC "How many orders are there?"
# MAGIC
# MAGIC What would you use?
# MAGIC

# COMMAND ----------

df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC Suppose the dataset has:
# MAGIC
# MAGIC 100 million records
# MAGIC
# MAGIC but you only need:
# MAGIC
# MAGIC 20 records
# MAGIC
# MAGIC for checking whether the pipeline is working.
# MAGIC
# MAGIC Would you run:
# MAGIC
# MAGIC `df.count()
# MAGIC df.show(20)`
# MAGIC
# MAGIC or just:
# MAGIC
# MAGIC `df.show(20)`
# MAGIC
# MAGIC Why?

# COMMAND ----------

## i will have to use this one 
df.count()## to check how many orders currenty 
df.show(20)## to check the metadata and many more

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

df.show(1)

# COMMAND ----------

# MAGIC %md
# MAGIC