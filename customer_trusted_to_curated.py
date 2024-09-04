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

# Script generated for node accelerometer_trusted
accelerometer_trusted_node1725466977501 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_trusted", transformation_ctx="accelerometer_trusted_node1725466977501")

# Script generated for node customer_trusted
customer_trusted_node1725466974125 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_trusted", transformation_ctx="customer_trusted_node1725466974125")

# Script generated for node SQL Query
SqlQuery3946 = '''
select distinct myDataSource.* from myDataSource
inner join accelerometer_trusted
on myDataSource.email = accelerometer_trusted.user
'''
SQLQuery_node1725467035312 = sparkSqlQuery(glueContext, query = SqlQuery3946, mapping = {"myDataSource":customer_trusted_node1725466974125, "accelerometer_trusted":accelerometer_trusted_node1725466977501}, transformation_ctx = "SQLQuery_node1725467035312")

# Script generated for node customer_curated
customer_curated_node1725467140579 = glueContext.getSink(path="s3://de-fpt-quangnb3/customer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="customer_curated_node1725467140579")
customer_curated_node1725467140579.setCatalogInfo(catalogDatabase="stedi",catalogTableName="customer_curated")
customer_curated_node1725467140579.setFormat("json")
customer_curated_node1725467140579.writeFrame(SQLQuery_node1725467035312)
job.commit()