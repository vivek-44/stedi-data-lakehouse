import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1779687659233 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_trusted", transformation_ctx="AWSGlueDataCatalog_node1779687659233")

# Script generated for node Amazon S3
AmazonS3_node1779709700782 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-project-vivek/accelerometer/landing/"], "recurse": True}, transformation_ctx="AmazonS3_node1779709700782")

# Script generated for node Join
Join_node1779687716826 = Join.apply(frame1=AWSGlueDataCatalog_node1779687659233, frame2=AmazonS3_node1779709700782, keys1=["email"], keys2=["user"], transformation_ctx="Join_node1779687716826")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=Join_node1779687716826, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1779687628216", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1779687774040 = glueContext.getSink(path="s3://stedi-project-vivek/accelerometer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1779687774040")
AmazonS3_node1779687774040.setCatalogInfo(catalogDatabase="stedi",catalogTableName="accelerometer_trusted")
AmazonS3_node1779687774040.setFormat("glueparquet", compression="snappy")
AmazonS3_node1779687774040.writeFrame(Join_node1779687716826)
job.commit()
