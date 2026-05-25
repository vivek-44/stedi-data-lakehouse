import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
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

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1779688995781 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_trusted", transformation_ctx="AWSGlueDataCatalog_node1779688995781")

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1779689019630 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="step_trainer_trusted", transformation_ctx="AWSGlueDataCatalog_node1779689019630")

# Script generated for node SQL Query
SqlQuery0 = '''
select a.*,s.distancefromobject
FROM a
INNER JOIN s
ON a.timestamp=s.sensorreadingtime;
'''
SQLQuery_node1779689117597 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"a":AWSGlueDataCatalog_node1779688995781, "s":AWSGlueDataCatalog_node1779689019630}, transformation_ctx = "SQLQuery_node1779689117597")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SQLQuery_node1779689117597, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1779688183642", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1779689309618 = glueContext.getSink(path="s3://stedi-project-vivek/machine_learning_curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1779689309618")
AmazonS3_node1779689309618.setCatalogInfo(catalogDatabase="stedi",catalogTableName="machine_learning_curated")
AmazonS3_node1779689309618.setFormat("glueparquet", compression="snappy")
AmazonS3_node1779689309618.writeFrame(SQLQuery_node1779689117597)
job.commit()
