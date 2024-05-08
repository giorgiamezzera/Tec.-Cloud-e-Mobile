import sys
import json
import pyspark
from pyspark.sql.functions import col, collect_list, array_join, collect_set

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame



#homework 2: aggiungere e trattare il dataset related_videos
related_videos_dataset_path="s3://test-unigm/related_videos.csv"

#leggo i parametri
args=getResolvedOptions(sys.argv,['JOB_NAME'])

#start job
sc=SparkContext()
gluecontext=GlueContext()
spark=gluecontext.spark_session

job=Job(gluecontext)
job.init(args['JOB_NAME'],args)

#leggo il file in input e creo dataset
related_videos_dataset=spark.read \
        .option("header","true") \
        .option("quote", "\"") \
        .option("escape", "\"") \
        .csv(related_videos_dataset_path)

related_videos_dataset.printSchema()

#filtro gli items con chiave nulla
count_items=related_videos_dataset.count()
count_items_null=related_videos_dataset.filter("id is not null").count()

#stampo controllo
print(f"numero di items dal raw data {count_items}")
print(f"numero di items con chiave NOT NULL {count_items_null}")

#drop colonne diverse dall'id - seleziono solo quelle che mi interessano
related_videos_dataset=related_videos_dataset.select("id","related_id")

#aggrego gli items con lo stesso id
related_videos_dataset_agg = related_videos_dataset.groupBy(col("id").alias("id_ref")).agg(collect_set("related_id").alias("related_videos"))
related_videos_dataset_agg.printSchema()

#leggo tedx dataset da mongodb
read_mongo_options = {
    "connectionName": "TEDX2024",
    "database": "unibg_tedx_2024",
    "collection": "tedx_data",
    "ssl": "true",
    "ssl.domain_match": "false"}

tedx_dataset=gluecontext.create_dynamic_frame.from_options(connection_type="mongodb",connection_options=read_mongo_options).toDF()
tedx_dataset.printSchema()

#creo il modello aggregato: aggiungo related_videos a tedx_dataset
tedx_dataset_agg=tedx_dataset.join(related_videos_dataset_agg,tedx_dataset._id==related_videos_dataset_agg.id_ref,"left") \
                    .drop("id_ref") 

tedx_dataset_agg.printSchema()

#scrivo il risultato in mongodb
write_mongo_options = {
    "connectionName": "TEDX2024",
    "database": "unibg_tedx_2024",
    "collection": "tedx_data",
    "ssl": "true",
    "ssl.domain_match": "false"}

tedx_dataset_dynamic_frame=DynamicFrame.fromDF(tedx_dataset_agg,gluecontext,"nested")
gluecontext.write_dynamic_frame.from_options(tedx_dataset_dynamic_frame,connection_type="mongodb",connection_options=write_mongo_options)
