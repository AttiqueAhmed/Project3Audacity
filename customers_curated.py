import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as SqlFuncs

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node accelerometerlanding
accelerometerlanding_node1711742903037 = glueContext.create_dynamic_frame.from_catalog(
    database="project3",
    table_name="accelerometer_landing",
    transformation_ctx="accelerometerlanding_node1711742903037",
)

# Script generated for node customertrusted
customertrusted_node1711742909797 = glueContext.create_dynamic_frame.from_catalog(
    database="project3",
    table_name="customer_trusted",
    transformation_ctx="customertrusted_node1711742909797",
)

# Script generated for node Join
Join_node1711742914055 = Join.apply(
    frame1=customertrusted_node1711742909797,
    frame2=accelerometerlanding_node1711742903037,
    keys1=["email"],
    keys2=["user"],
    transformation_ctx="Join_node1711742914055",
)

# Script generated for node Drop Duplicates
DropDuplicates_node1711788521828 = DynamicFrame.fromDF(
    Join_node1711742914055.toDF().dropDuplicates(["email"]),
    glueContext,
    "DropDuplicates_node1711788521828",
)

# Script generated for node Drop Fields
DropFields_node1711743223495 = DropFields.apply(
    frame=DropDuplicates_node1711788521828,
    paths=["y", "x", "timestamp", "z", "user"],
    transformation_ctx="DropFields_node1711743223495",
)

# Script generated for node customercurated
customercurated_node1711742920998 = glueContext.getSink(
    path="s3://project3parent/customer/curated/",
    connection_type="s3",
    updateBehavior="LOG",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="customercurated_node1711742920998",
)
customercurated_node1711742920998.setCatalogInfo(
    catalogDatabase="project3", catalogTableName="customer_curated"
)
customercurated_node1711742920998.setFormat("json")
customercurated_node1711742920998.writeFrame(DropFields_node1711743223495)
job.commit()
