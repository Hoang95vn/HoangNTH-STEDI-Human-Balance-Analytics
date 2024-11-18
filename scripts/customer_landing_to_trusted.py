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

# Script generated for node Amazon S3
AmazonS3_node1731910054517 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-human-balance-bucket/customer/landing"], "recurse": True}, transformation_ctx="AmazonS3_node1731910054517")

# Script generated for node SQL Query
SqlQuery0 = '''
select * from myDataSource where WHERE sharewithresearchasofdate IS NOT NULL
'''
SQLQuery_node1731913660813 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"myDataSource":AmazonS3_node1731910054517}, transformation_ctx = "SQLQuery_node1731913660813")

# Script generated for node Trusted Customer Zone
TrustedCustomerZone_node1731910828762 = glueContext.getSink(path="s3://stedi-human-balance-bucket/customer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="TrustedCustomerZone_node1731910828762")
TrustedCustomerZone_node1731910828762.setCatalogInfo(catalogDatabase="hoangnth",catalogTableName="customer_trusted")
TrustedCustomerZone_node1731910828762.setFormat("json")
TrustedCustomerZone_node1731910828762.writeFrame(SQLQuery_node1731913660813)
job.commit()