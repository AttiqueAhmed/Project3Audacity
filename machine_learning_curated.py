import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node step_trainer_trusted
step_trainer_trusted_node1711811948517 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiLine": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://project3parent/step_trainer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="step_trainer_trusted_node1711811948517",
)

# Script generated for node accelerometer_trusted
accelerometer_trusted_node1711811944167 = glueContext.create_dynamic_frame.from_catalog(
    database="project3",
    table_name="accelerometer_trusted",
    transformation_ctx="accelerometer_trusted_node1711811944167",
)

# Script generated for node Join
Join_node1711811951634 = Join.apply(
    frame1=step_trainer_trusted_node1711811948517,
    frame2=accelerometer_trusted_node1711811944167,
    keys1=["sensorReadingTime"],
    keys2=["timestamp"],
    transformation_ctx="Join_node1711811951634",
)

# Script generated for node Drop Fields
DropFields_node1711813089863 = DropFields.apply(
    frame=Join_node1711811951634,
    paths=["timestamp"],
    transformation_ctx="DropFields_node1711813089863",
)

# Script generated for node machine_learning_curated
machine_learning_curated_node1711812761837 = glueContext.getSink(
    path="s3://project3parent/step_trainer/curated/",
    connection_type="s3",
    updateBehavior="LOG",
    partitionKeys=[],
    compression="snappy",
    enableUpdateCatalog=True,
    transformation_ctx="machine_learning_curated_node1711812761837",
)
machine_learning_curated_node1711812761837.setCatalogInfo(
    catalogDatabase="project3", catalogTableName="machine_learning_curated"
)
machine_learning_curated_node1711812761837.setFormat("json")
machine_learning_curated_node1711812761837.writeFrame(DropFields_node1711813089863)
job.commit()
