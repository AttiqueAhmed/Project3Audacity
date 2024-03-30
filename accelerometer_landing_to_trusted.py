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

# Script generated for node accelerometerlanding
accelerometerlanding_node1711711111010 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://project3parent/accelerometer/landing/"], "recurse": True}, transformation_ctx="accelerometerlanding_node1711711111010")

# Script generated for node customertrusted
customertrusted_node1711740224848 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://project3parent/customer/trusted/"], "recurse": True}, transformation_ctx="customertrusted_node1711740224848")

# Script generated for node Join
Join_node1711740357341 = Join.apply(frame1=accelerometerlanding_node1711711111010, frame2=customertrusted_node1711740224848, keys1=["user"], keys2=["email"], transformation_ctx="Join_node1711740357341")

# Script generated for node Drop Fields
DropFields_node1711741430382 = DropFields.apply(frame=Join_node1711740357341, paths=["serialNumber", "shareWithPublicAsOfDate", "birthDay", "registrationDate", "shareWithResearchAsOfDate", "customerName", "shareWithFriendsAsOfDate", "email", "lastUpdateDate", "phone"], transformation_ctx="DropFields_node1711741430382")

# Script generated for node accelerometertrusted
accelerometertrusted_node1711740420594 = glueContext.write_dynamic_frame.from_options(frame=DropFields_node1711741430382, connection_type="s3", format="json", connection_options={"path": "s3://project3parent/accelerometer/trusted/", "partitionKeys": []}, transformation_ctx="accelerometertrusted_node1711740420594")

job.commit()