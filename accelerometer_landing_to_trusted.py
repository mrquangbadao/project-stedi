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

# Script generated for node accelerometer_landing
accelerometer_landing_node1725466190689 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://de-fpt-quangnb3/accelerometer/landing/"], "recurse": True}, transformation_ctx="accelerometer_landing_node1725466190689")

# Script generated for node customer_trusted
customer_trusted_node1725466193021 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_trusted", transformation_ctx="customer_trusted_node1725466193021")

# Script generated for node SQL Query
SqlQuery2866 = '''
select distinct myDataSource.* from myDataSource
join customerTrusted 
on myDataSource.user = customerTrusted.email
'''
SQLQuery_node1725466322251 = sparkSqlQuery(glueContext, query = SqlQuery2866, mapping = {"myDataSource":accelerometer_landing_node1725466190689, "customerTrusted":customer_trusted_node1725466193021}, transformation_ctx = "SQLQuery_node1725466322251")

# Script generated for node accelerometer_trusted
accelerometer_trusted_node1725466531769 = glueContext.getSink(path="s3://de-fpt-quangnb3/accelerometer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="accelerometer_trusted_node1725466531769")
accelerometer_trusted_node1725466531769.setCatalogInfo(catalogDatabase="stedi",catalogTableName="accelerometer_trusted")
accelerometer_trusted_node1725466531769.setFormat("json")
accelerometer_trusted_node1725466531769.writeFrame(SQLQuery_node1725466322251)
job.commit()