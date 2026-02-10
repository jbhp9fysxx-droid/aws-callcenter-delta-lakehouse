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

# Enable dynamic partition overwrite safety
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

# --------------------
# Paths
# --------------------
file_path = "s3://vinay-callcenter-etl-dev/source/call_center_raw.csv"
silver_delta_path = "s3://vinay-callcenter-etl-dev/silver_delta"
exception_path = "s3://vinay-callcenter-etl-dev/exception"

# --------------------
# Read Raw Data
# --------------------

schema = "call_id INT,caller_id INT,agent_id INT,call_start_time STRING,call_end_time STRING,call_status STRING"
df=spark.read.schema(schema).csv(file_path,header=True)
# df.show()
# df.printSchema()


# --------------------
# Applying data quality checks and transforming data
# --------------------

time_pattern = "^[0-9]{2}:[0-9]{2}:[0-9]{2}$"
valid_statuses = ["COMPLETED", "DROPPED", "FAILED"]
all_errors = F.concat_ws(" | ",
     # --- check call_id ----
    F.when(F.col('call_id').isNull(),"call_id must not be NULL")
    .when(~F.col("call_id").rlike("^[0-9]+$"),"call_id must be numeric"),

    # ---- check_caller_id ----
    F.when(F.col('caller_id').isNull(),"caller_id must not be NULL")
    .when(~F.col("caller_id").rlike("^[0-9]+$"),"caller_id must be numeric"),

        # ---- check agent_id ----
    F.when(F.col('agent_id').isNull(),"agent_id must not be NULL")
    .when(~F.col("agent_id").rlike("^[0-9]+$"),"agent_id must be numeric"),

        # ---- check call_start_time ----
    F.when(F.col('call_start_time').isNull(),"call_start_time must not be NULL")
    .when(~F.col("call_start_time").rlike(time_pattern),"invalid call_start_time"),

        # ---- check call_end_time ----
    F.when(F.col('call_end_time').isNull(),"call_end_time must not be NULL")
    .when(~F.col("call_end_time").rlike(time_pattern),"invalid call_end_time"),

        # ---- check call_status ----
    F.when(F.col('call_status').isNull(),"call_status must not be NULL")
    .when(~F.upper(F.col("call_status")).isin(valid_statuses),"invalid call_status values")
)


validated_df = df.withColumn("Reject_reason",
                             F.when(all_errors == "",None).otherwise(all_errors))



# Add processing partitions
validated_df=validated_df.withColumns({"year":F.year(F.current_date()),
                                    "month":F.month(F.current_date())})


# --------------------
# Split valid / invalid
# --------------------

valid_df = validated_df.filter(F.col("Reject_reason").isNull())
invalid_df = validated_df.filter(F.col("Reject_reason").isNotNull())

# --------------------
# INITIAL LOAD vs UPSERT
# --------------------
clean_valid_df = valid_df.drop("Reject_reason")

if DeltaTable.isDeltaTable(spark, silver_delta_path):

    delta_table = DeltaTable.forPath(spark, silver_delta_path)

    (
        delta_table.alias("t")
        .merge(
            source=clean_valid_df.alias("s"),
            condition="t.call_id = s.call_id"
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

else:
    (
        clean_valid_df.write
        .format("delta")
        .mode("overwrite")
        .partitionBy("year", "month")
        .save(silver_delta_path)
    )

# ----- Write invalid data to exception path ------

invalid_df.write \
        .format("parquet") \
        .mode("append") \
        .partitionBy("year","month") \
        .save(exception_path)

job.commit()