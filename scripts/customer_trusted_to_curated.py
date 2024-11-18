import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1731916162731 = glueContext.create_dynamic_frame.from_catalog(database="hoangnth", table_name="customer_trusted", transformation_ctx="AWSGlueDataCatalog_node1731916162731")

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1731916164228 = glueContext.create_dynamic_frame.from_catalog(database="hoangnth", table_name="accelerometer_trusted", transformation_ctx="AWSGlueDataCatalog_node1731916164228")

# Script generated for node Join
Join_node1731916189354 = Join.apply(frame1=AWSGlueDataCatalog_node1731916162731, frame2=AWSGlueDataCatalog_node1731916164228, keys1=["email"], keys2=["user"], transformation_ctx="Join_node1731916189354")

# Script generated for node SQL Query
SqlQuery0 = '''
SELECT DISTINCT
    customername,
    email,
    phone,
    birthday,
    serialnumber,
    registrationdate,
    lastupdatedate,
    sharewithresearchasofdate,
    sharewithpublicasofdate,
    sharewithfriendsasofdate
FROM myDataSource
'''
SQLQuery_node1731916257203 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"myDataSource":Join_node1731916189354}, transformation_ctx = "SQLQuery_node1731916257203")

# Script generated for node Amazon S3
AmazonS3_node1731916381939 = glueContext.getSink(path="s3://stedi-human-balance-bucket/customer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1731916381939")
AmazonS3_node1731916381939.setCatalogInfo(catalogDatabase="hoangnth",catalogTableName="customer_curated")
AmazonS3_node1731916381939.setFormat("json")
AmazonS3_node1731916381939.writeFrame(SQLQuery_node1731916257203)
job.commit()