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

# Script generated for node step_trainer_trusted
step_trainer_trusted_node1725468219598 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="strep_trainer_trusted", transformation_ctx="step_trainer_trusted_node1725468219598")

# Script generated for node accelerometer_trusted
accelerometer_trusted_node1725468247269 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_trusted", transformation_ctx="accelerometer_trusted_node1725468247269")

# Script generated for node SQL Query
SqlQuery3866 = '''
select distinct * from myDataSource
inner join accelerometer_trusted 
on myDataSource.sensorreadingtime = accelerometer_trusted.timestamp
'''
SQLQuery_node1725468292952 = sparkSqlQuery(glueContext, query = SqlQuery3866, mapping = {"myDataSource":step_trainer_trusted_node1725468219598, "accelerometer_trusted":accelerometer_trusted_node1725468247269}, transformation_ctx = "SQLQuery_node1725468292952")

# Script generated for node machine_learning_curated
machine_learning_curated_node1725468455434 = glueContext.getSink(path="s3://de-fpt-quangnb3/step_trainer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], compression="snappy", enableUpdateCatalog=True, transformation_ctx="machine_learning_curated_node1725468455434")
machine_learning_curated_node1725468455434.setCatalogInfo(catalogDatabase="stedi",catalogTableName="machine_learning_curated")
machine_learning_curated_node1725468455434.setFormat("json")
machine_learning_curated_node1725468455434.writeFrame(SQLQuery_node1725468292952)
job.commit()