import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
import re

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

# Script generated for node Amazon S3
AmazonS3_node1733426692647 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://proj3-uda/customer/landing/"], "recurse": True}, transformation_ctx="AmazonS3_node1733426692647")

# Script generated for node PrivacyFilter
PrivacyFilter_node1733427403384 = Filter.apply(frame=AmazonS3_node1733426692647, f=lambda row: (not(row["shareWithResearchAsOfDate"] == 0)), transformation_ctx="PrivacyFilter_node1733427403384")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=PrivacyFilter_node1733427403384, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1733426532160", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1733428252898 = glueContext.write_dynamic_frame.from_options(frame=PrivacyFilter_node1733427403384, connection_type="s3", format="json", connection_options={"path": "s3://proj3-uda/customer/trusted/", "compression": "gzip", "partitionKeys": []}, transformation_ctx="AmazonS3_node1733428252898")

job.commit()