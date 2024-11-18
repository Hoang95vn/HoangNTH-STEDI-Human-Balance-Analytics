import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1731918126978 = glueContext.create_dynamic_frame.from_catalog(database="hoangnth", table_name="step_trainer_trusted", transformation_ctx="AWSGlueDataCatalog_node1731918126978")

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1731918128322 = glueContext.create_dynamic_frame.from_catalog(database="hoangnth", table_name="accelerometer_trusted", transformation_ctx="AWSGlueDataCatalog_node1731918128322")

# Script generated for node Join
Join_node1731918163563 = Join.apply(frame1=AWSGlueDataCatalog_node1731918126978, frame2=AWSGlueDataCatalog_node1731918128322, keys1=["sensorreadingtime"], keys2=["timestamp"], transformation_ctx="Join_node1731918163563")

# Script generated for node Amazon S3
AmazonS3_node1731918196634 = glueContext.getSink(path="s3://stedi-human-balance-bucket/step_trainer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1731918196634")
AmazonS3_node1731918196634.setCatalogInfo(catalogDatabase="hoangnth",catalogTableName="machine_learning_curated")
AmazonS3_node1731918196634.setFormat("json")
AmazonS3_node1731918196634.writeFrame(Join_node1731918163563)
job.commit()