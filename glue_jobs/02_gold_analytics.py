import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from delta.tables import DeltaTable

## @params: [JOB_NAME]
# --------------------
# Glue bootstrap
# --------------------
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# --------------------
# Paths
# --------------------
source_path="s3://vinay-callcenter-etl-dev/silver_delta"
target_path1="s3://vinay-callcenter-etl-dev/gold/gold_callcenter_analytics/gold_call_daily_summary/"

# --------------------
# Read Silver Delta
# --------------------

prcsd_date=F.current_date()
df = spark.read.format("delta").load(source_path)

# ---- column pruning -----
df1=df.select("call_id","call_start_time","call_end_time","call_status").withColumn("call_date",prcsd_date)
df2=df.select("call_id","agent_id","call_start_time","call_end_time","call_status").withColumn("call_date",prcsd_date)
df3=df.select("call_id","call_start_time","call_end_time","call_status") \
    .withColumn("call_date",prcsd_date) \
    .withColumn("duration_sec",
                F.unix_timestamp(F.col("call_end_time"),"HH:mm:ss") - 
                F.unix_timestamp(F.col("call_start_time"),"HH:mm:ss"))
df3 = df3.filter(F.col("duration_sec") >= 0)

# --------------------
# Aggregations and analytics transformations
# --------------------

# ---- daily calls summary transformation -------------

result_df1 = df1.groupBy("call_date").agg(F.count("*").alias("total_calls"),

                                        # ---- completed calls aggregation -----
                                        
                                        F.sum(
                                            F.when(
                                                F.col("call_status")=='COMPLETED',1).otherwise(0)
                                                ).alias("completed_calls"),
                                                
                                        # ------ dropped calls aggregation -------
                                        
                                        F.sum(
                                            F.when(
                                                F.col("call_status")=='DROPPED',1).otherwise(0)
                                                ).alias("dropped_calls"),
                                                
                                        # ------ FAILED calls aggregation -------
                                        
                                        F.sum(
                                            F.when(
                                                F.col("call_status")=='FAILED',1).otherwise(0)
                                                ).alias("failed_calls")
                                        )
                                        
# ------  completion rate aggregation -------    
                                        
result_df1=result_df1.withColumn("completion_rate",
                            F.when(F.col("total_calls")>0,
                            F.round((F.col("completed_calls")/F.col("total_calls"))*100,2)).otherwise(0.0))


# ---- agent performance metrics tranformation -------------
result_df2 = df2.groupBy("agent_id","call_date").agg(F.count("*").alias("total_calls"),

                                        # ---- completed calls aggregation -----
                                        
                                        F.sum(
                                            F.when(
                                                F.col("call_status")=='COMPLETED',1).otherwise(0)
                                                ).alias("completed_calls"),
                                                
                                        # ------ dropped calls aggregation -------
                                        
                                        F.sum(
                                            F.when(
                                                F.col("call_status")=='DROPPED',1).otherwise(0)
                                                ).alias("dropped_calls"),
                                                
                                        # ------ FAILED calls aggregation -------
                                        
                                        F.sum(
                                            F.when(
                                                F.col("call_status")=='FAILED',1).otherwise(0)
                                                ).alias("failed_calls")
                                        )
                                        
# ------  completion rate aggregation -------    

result_df2=result_df2.withColumn("completion_rate",
                            F.when(F.col("total_calls")>0,
                            F.round((F.col("completed_calls")/F.col("total_calls"))*100,2)).otherwise(0.0))
 

# _____ call duration metrics transformations -------------

result_df3 = df3.groupBy("call_date").agg(

                                        # ---- Average call duration per day -----
                                        
                                        F.round(F.avg("duration_sec"),2).alias("avg_call_duration_seconds"),
                                                
                                        # ------ dropped calls aggregation -------
                                        
                                        F.min("duration_sec").alias("min_call_duration_seconds"),
                                                
                                        # ------ FAILED calls aggregation -------
                                        
                                        F.max("duration_sec").alias("max_call_duration_seconds")
                                        )
                                        

# --------------------
# INITIAL LOAD vs UPSERT
# --------------------

# ---- check if delta exists for call daily summary metrics and merge to gold -----


result_df1=result_df1.withColumns({
    "year": F.year(prcsd_date),
    "month": F.month(prcsd_date)
})




if DeltaTable.isDeltaTable(spark,target_path1):
    dt1=DeltaTable.forPath(spark, target_path1)
    dt1.alias("t").merge(source=result_df1.alias("s"),
                        condition="t.call_date=s.call_date") \
                        .whenMatchedUpdateAll() \
                        .whenNotMatchedInsertAll() \
                        .execute()
else:
    result_df1.write \
        .format("delta") \
        .mode("overwrite") \
        .partitionBy("year","month") \
        .save(target_path1)


# ---- check if delta exists for agent performance metrics and merge to gold -----


result_df2=result_df2.withColumns({
    "year": F.year(prcsd_date),
    "month": F.month(prcsd_date)
})

target_path2="s3://vinay-callcenter-etl-dev/gold/gold_callcenter_analytics/gold_agent_performance/"

if DeltaTable.isDeltaTable(spark,target_path2):
    dt2=DeltaTable.forPath(spark, target_path2)
    dt2.alias("t").merge(source=result_df2.alias("s"),
                        condition="t.call_date=s.call_date AND t.agent_id = s.agent_id") \
                        .whenMatchedUpdateAll() \
                        .whenNotMatchedInsertAll() \
                        .execute()
else:
    result_df2.write \
        .format("delta") \
        .mode("overwrite") \
        .partitionBy("year","month") \
        .save(target_path2)



# ---- check if delta exists for call duration metrics and merge to gold -----

result_df3=result_df3.withColumns({
    "year": F.year(prcsd_date),
    "month": F.month(prcsd_date)
})

target_path3="s3://vinay-callcenter-etl-dev/gold/gold_callcenter_analytics/gold_call_duration_metrics/"

if DeltaTable.isDeltaTable(spark,target_path3):
    dt3=DeltaTable.forPath(spark,target_path3)
    dt3.alias("t").merge(source=result_df3.alias("s"),
                        condition="t.call_date=s.call_date") \
                        .whenMatchedUpdateAll() \
                        .whenNotMatchedInsertAll() \
                        .execute()
else:
    result_df3.write \
        .format("delta") \
        .mode("overwrite") \
        .partitionBy("year","month") \
        .save(target_path3)
        
job.commit()