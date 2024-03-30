import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node customercurated
customercurated_node1711740753430 = glueContext.create_dynamic_frame.from_catalog(database="project3", table_name="customers_curated", transformation_ctx="customercurated_node1711740753430")

# Script generated for node step_trainer_landing
step_trainer_landing_node1711740758637 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://project3parent/step_trainer/landing/"], "recurse": True}, transformation_ctx="step_trainer_landing_node1711740758637")

# Script generated for node Join
step_trainer_landing_node1711740758637DF = step_trainer_landing_node1711740758637.toDF()
customercurated_node1711740753430DF = customercurated_node1711740753430.toDF()
Join_node1711740761429 = DynamicFrame.fromDF(step_trainer_landing_node1711740758637DF.join(customercurated_node1711740753430DF, (step_trainer_landing_node1711740758637DF['serialnumber'] == customercurated_node1711740753430DF['serialnumber']), "leftsemi"), glueContext, "Join_node1711740761429")

# Script generated for node Drop Fields
DropFields_node1711741237338 = DropFields.apply(frame=Join_node1711740761429, paths=[], transformation_ctx="DropFields_node1711741237338")

# Script generated for node step_trainer_trusted
step_trainer_trusted_node1711741077684 = glueContext.write_dynamic_frame.from_options(frame=DropFields_node1711741237338, connection_type="s3", format="json", connection_options={"path": "s3://project3parent/step_trainer/trusted/", "partitionKeys": []}, transformation_ctx="step_trainer_trusted_node1711741077684")

job.commit()