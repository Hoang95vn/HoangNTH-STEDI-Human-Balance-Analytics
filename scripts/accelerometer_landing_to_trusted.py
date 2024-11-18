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

# Script generated for node Customer Trusted
CustomerTrusted_node1731914275356 = glueContext.create_dynamic_frame.from_catalog(database="hoangnth", table_name="customer_trusted", transformation_ctx="CustomerTrusted_node1731914275356")

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1731914334483 = glueContext.create_dynamic_frame.from_catalog(database="hoangnth", table_name="accelerometer_landing", transformation_ctx="AccelerometerLanding_node1731914334483")

# Script generated for node Join
Join_node1731914352451 = Join.apply(frame1=CustomerTrusted_node1731914275356, frame2=AccelerometerLanding_node1731914334483, keys1=["email"], keys2=["user"], transformation_ctx="Join_node1731914352451")

# Script generated for node SQL Query
SqlQuery0 = '''
select user,timestamp,x,y,z from myDataSource
'''
SQLQuery_node1731914917760 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"myDataSource":Join_node1731914352451}, transformation_ctx = "SQLQuery_node1731914917760")

# Script generated for node Amazon S3
AmazonS3_node1731914581954 = glueContext.getSink(path="s3://stedi-human-balance-bucket/accelerometer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1731914581954")
AmazonS3_node1731914581954.setCatalogInfo(catalogDatabase="hoangnth",catalogTableName="accelerometer_trusted")
AmazonS3_node1731914581954.setFormat("json")
AmazonS3_node1731914581954.writeFrame(SQLQuery_node1731914917760)
job.commit()