import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import re

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Amazon S3
AmazonS3_node1711711111010 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiLine": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://project3parent/customer/landing/"],
        "recurse": True,
    },
    transformation_ctx="AmazonS3_node1711711111010",
)

# Script generated for node PrivacyFilter
PrivacyFilter_node1711711117602 = Filter.apply(
    frame=AmazonS3_node1711711111010,
    f=lambda row: (not (row["shareWithResearchAsOfDate"] == 0)),
    transformation_ctx="PrivacyFilter_node1711711117602",
)

# Script generated for node Amazon S3
AmazonS3_node1711711120819 = glueContext.getSink(
    path="s3://project3parent/customer/trusted/",
    connection_type="s3",
    updateBehavior="LOG",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="AmazonS3_node1711711120819",
)
AmazonS3_node1711711120819.setCatalogInfo(
    catalogDatabase="project3", catalogTableName="customer_trusted"
)
AmazonS3_node1711711120819.setFormat("json")
AmazonS3_node1711711120819.writeFrame(PrivacyFilter_node1711711117602)
job.commit()
