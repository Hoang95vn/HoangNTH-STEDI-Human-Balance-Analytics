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
AWSGlueDataCatalog_node1731917067952 = glueContext.create_dynamic_frame.from_catalog(database="hoangnth", table_name="customer_curated", transformation_ctx="AWSGlueDataCatalog_node1731917067952")

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1731917069480 = glueContext.create_dynamic_frame.from_catalog(database="hoangnth", table_name="step_trainer_landing", transformation_ctx="AWSGlueDataCatalog_node1731917069480")

# Script generated for node Renamed keys for Join
RenamedkeysforJoin_node1731917445655 = ApplyMapping.apply(frame=AWSGlueDataCatalog_node1731917069480, mappings=[("sensorreadingtime", "string", "sensorreadingtime", "long"), ("serialnumber", "string", "right_serialnumber", "string"), ("distancefromobject", "long", "distancefromobject", "int")], transformation_ctx="RenamedkeysforJoin_node1731917445655")

# Script generated for node Join
Join_node1731917093094 = Join.apply(frame1=AWSGlueDataCatalog_node1731917067952, frame2=RenamedkeysforJoin_node1731917445655, keys1=["serialnumber"], keys2=["right_serialnumber"], transformation_ctx="Join_node1731917093094")

# Script generated for node Amazon S3
AmazonS3_node1731917534685 = glueContext.getSink(path="s3://stedi-human-balance-bucket/step_trainer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1731917534685")
AmazonS3_node1731917534685.setCatalogInfo(catalogDatabase="hoangnth",catalogTableName="step_trainer_trusted")
AmazonS3_node1731917534685.setFormat("json")
AmazonS3_node1731917534685.writeFrame(Join_node1731917093094)
job.commit()