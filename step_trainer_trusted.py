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

# Script generated for node customer_curated
customer_curated_node1725467502913 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_curated", transformation_ctx="customer_curated_node1725467502913")

# Script generated for node step_trainer_landing
step_trainer_landing_node1725467469248 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://de-fpt-quangnb3/step_trainer/landing/"], "recurse": True}, transformation_ctx="step_trainer_landing_node1725467469248")

# Script generated for node SQL Query
SqlQuery3522 = '''
SELECT distinct myDataSource.*
FROM myDataSource
WHERE myDataSource.serialnumber IN (
    SELECT distinct customer_curated.serialnumber
    FROM customer_curated
);
'''
SQLQuery_node1725467523386 = sparkSqlQuery(glueContext, query = SqlQuery3522, mapping = {"myDataSource":step_trainer_landing_node1725467469248, "customer_curated":customer_curated_node1725467502913}, transformation_ctx = "SQLQuery_node1725467523386")

# Script generated for node step_trainer_trusted
step_trainer_trusted_node1725467745525 = glueContext.getSink(path="s3://de-fpt-quangnb3/step_trainer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="step_trainer_trusted_node1725467745525")
step_trainer_trusted_node1725467745525.setCatalogInfo(catalogDatabase="stedi",catalogTableName="strep_trainer_trusted")
step_trainer_trusted_node1725467745525.setFormat("json")
step_trainer_trusted_node1725467745525.writeFrame(SQLQuery_node1725467523386)
job.commit()